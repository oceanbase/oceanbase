/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_row_reader.h"
#include "share/object/ob_obj_cast.h"
#include "ob_column_map.h"
#include "ob_sparse_cell_reader.h"

namespace oceanbase {
using namespace storage;
using namespace common;
namespace blocksstable {

#define SET_ROW_BASIC_INFO(row_type, row)                        \
  {                                                              \
    row.is_sparse_row_ = SPARSE_ROW_STORE == row_type;           \
    row.flag_ = row_header_->get_row_flag();                     \
    row.set_dml_val(row_header_->get_row_dml());                 \
    row.row_type_flag_.flag_ = row_header_->get_row_type_flag(); \
  }

#define ALLOC_COL_ID_ARRAY(allocator, row)                                                      \
  {                                                                                             \
    void* buf = nullptr;                                                                        \
    if (OB_ISNULL(buf = allocator.alloc(sizeof(uint16_t) * row_header_->get_column_count()))) { \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                          \
      STORAGE_LOG(WARN, "fail to allocate memory for ObLobData", K(ret));                       \
    } else {                                                                                    \
      row.column_ids_ = reinterpret_cast<uint16_t*>(buf);                                       \
    }                                                                                           \
  }

#define DESERIALIZE_TRANS_ID(trans_id_ptr)                                         \
  {                                                                                \
    if (ObRowHeader::RHV_NO_TRANS_ID == row_header_->get_version()) {              \
      STORAGE_LOG(DEBUG, "without trans id", K(ret), K(pos_));                     \
    } else if (ObRowHeader::RHV_WITH_TRANS_ID == row_header_->get_version()) {     \
      if (OB_FAIL(deserialize_trans_id(buf_, row_end_pos_, pos_, trans_id_ptr))) { \
        STORAGE_LOG(WARN, "failed to deserialize trans id", K(ret));               \
      }                                                                            \
    }                                                                              \
  }

//----------------------ObIRowReader-----------------------------------

ObIRowReader::ObIRowReader()
    : buf_(NULL),
      row_end_pos_(0),
      start_pos_(0),
      pos_(0),
      row_header_(NULL),
      trans_id_ptr_(NULL),
      store_column_indexs_(NULL),
      column_ids_(NULL),
      allocator_(ObModIds::OB_CS_ROW_READER),
      is_setuped_(false)
{}

ObIRowReader::~ObIRowReader()
{
  trans_id_ptr_ = NULL;
  reset();
}

void ObIRowReader::reset()
{
  buf_ = NULL;
  row_end_pos_ = 0;
  start_pos_ = 0;
  pos_ = 0;
  row_header_ = NULL;
  store_column_indexs_ = NULL;
  column_ids_ = NULL;
  is_setuped_ = false;
}

int ObIRowReader::get_row_header(const ObRowHeader*& row_header) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_header_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row_header is null");
  } else {
    row_header = row_header_;
  }
  return ret;
}

int deserialize_trans_id(const char* buf, const int64_t row_end_pos, int64_t& pos, transaction::ObTransID* trans_id_ptr)
{
  int ret = OB_SUCCESS;
  transaction::ObTransID not_use_trans_id;  // don't set trans id from outside
  if (OB_ISNULL(trans_id_ptr)) {
    trans_id_ptr = &not_use_trans_id;
    STORAGE_LOG(DEBUG, "TransID ptr is null", K(ret), K(buf), K(row_end_pos), K(pos));
  }
  if (OB_FAIL(trans_id_ptr->deserialize(buf, row_end_pos, pos))) {
    STORAGE_LOG(WARN, "Failed to deserialize TransID", K(ret), K(buf), K(row_end_pos), K(pos));
  } else {
    STORAGE_LOG(DEBUG, "Deserialize TransId success", K(*trans_id_ptr));
  }
  return ret;
}

int ObIRowReader::cast_obj(const common::ObObjMeta& src_meta, common::ObIAllocator& allocator, common::ObObj& obj)
{
  int ret = OB_SUCCESS;
  if (ObNullType == obj.get_type() || ObExtendType == obj.get_type()) {
    // ignore src_meta type, do nothing, just return obj
  } else {
    const ObObjTypeClass type_class = src_meta.get_type_class();
    bool need_cast = true;
    bool empty_str = false;
    if (type_class == obj.get_type_class()) {
      switch (type_class) {
        case ObIntTC: {
          need_cast = src_meta.get_type() < obj.get_type();
          break;
        }
        case ObUIntTC: {
          need_cast = (obj.get_int() < 0 || static_cast<uint64_t>(obj.get_int()) > UINT_MAX_VAL[src_meta.get_type()]);
          break;
        }
        case ObFloatTC: {
          need_cast = (obj.get_float() < 0.0);
          break;
        }
        case ObDoubleTC: {
          need_cast = (obj.get_double() < 0.0);
          break;
        }
        case ObNumberTC: {
          need_cast = obj.is_negative_number();
          break;
        }
        case ObStringTC: {
          need_cast = (obj.get_collation_type() != src_meta.get_collation_type());
          empty_str = 0 == obj.get_val_len();
          break;
        }
        case ObTextTC: {
          need_cast = obj.is_lob_inrow() && (obj.get_collation_type() != src_meta.get_collation_type());
          empty_str = 0 == obj.get_val_len();
          break;
        }
        default: {
          need_cast = false;
        }
      }
    }

    if ((obj.get_type() == src_meta.get_type() && !need_cast) || empty_str) {
      // just change the type
      obj.set_type(src_meta.get_type());
      if (empty_str && ObTextTC == type_class) {
        obj.set_lob_inrow();
      }
    } else {
      // not support data alteration
      ObObj ori_obj = obj;
      int64_t cm_mode = CM_NONE;
      if (ObIntTC == ori_obj.get_type_class() && ObUIntTC == type_class) {
        obj.set_uint(src_meta.get_type(), static_cast<uint64_t>(ori_obj.get_int()));
      } else if (ObIntTC == ori_obj.get_type_class() && ObBitTC == type_class) {
        obj.set_bit(static_cast<uint64_t>(ori_obj.get_int()));
      } else {
        ObCastCtx cast_ctx(&allocator, NULL, cm_mode, src_meta.get_collation_type());
        if (OB_FAIL(ObObjCaster::to_type(src_meta.get_type(), cast_ctx, ori_obj, obj))) {
          STORAGE_LOG(WARN,
              "fail to cast obj",
              K(ret),
              K(ori_obj),
              K(obj),
              K(ori_obj.get_type()),
              K(ob_obj_type_str(ori_obj.get_type())),
              K(src_meta.get_type()),
              K(ob_obj_type_str(src_meta.get_type())));
        }
      }
    }
  }
  return ret;
}

int ObIRowReader::read_text_store(const ObStoreMeta& store_meta, common::ObIAllocator& allocator, ObObj& obj)
{
  int ret = OB_SUCCESS;
  if (STORE_WITHOUT_COLLATION == store_meta.attr_) {
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  } else if (STORE_WITH_COLLATION == store_meta.attr_) {
    obj.set_collation_type(static_cast<ObCollationType>(*read<uint8_t>(buf_, pos_)));
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "row reader only suport utf8 character set", K(ret), K(store_meta.attr_));
  }
  if (OB_SUCC(ret)) {
    const uint8_t* type = read<uint8_t>(buf_, pos_);
    const uint8_t* scale = read<uint8_t>(buf_, pos_);
    const uint8_t* version = read<uint8_t>(buf_, pos_);
    const ObObjType store_type = static_cast<ObObjType>(*type);
    ObLobScale lob_scale(*scale);
    if (STORE_TEXT_STORE_VERSION != *version) {
      STORAGE_LOG(WARN, "[LOB] unexpected text store version", K(*version), K(ret));
    }
    if (lob_scale.is_in_row()) {
      const uint32_t* len = read<uint32_t>(buf_, pos_);
      if (pos_ + *len > row_end_pos_) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buf is not large", K(ret), K(pos_), K(*len), K(row_end_pos_));
      } else {
        obj.set_lob_value(store_type, (char*)(buf_ + pos_), *len);
        pos_ += *len;
      }
    } else if (lob_scale.is_out_row()) {
      void* buf = NULL;
      ObLobData* value = NULL;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObLobData)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to allocate memory for ObLobData", K(ret));
      } else {
        value = new (buf) ObLobData();
        if (OB_FAIL(value->deserialize(buf_, row_end_pos_, pos_))) {
          STORAGE_LOG(WARN, "fail to deserialize lob data", K(ret));
        } else {
          obj.set_lob_value(store_type, value, value->get_handle_size());
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "error unexpected, invalid obj type", K(ret), K(obj));
    }
  }

  return ret;
}

//----------------------ObFlatRowReader-----------------------------------

ObFlatRowReader::ObFlatRowReader()
{}
/*
 * FlatRow : ObRowHeader | Column Array | Column Index Array
 * */
inline int ObFlatRowReader::setup_row(const char* buf, const int64_t row_end_pos, const int64_t pos,
    const int64_t column_index_count, transaction::ObTransID* trans_id_ptr /* = nullptr*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || row_end_pos <= 0 || column_index_count < -1 || pos < 0 || pos >= row_end_pos)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row reader argument", K(ret), K(OB_P(buf)), K(row_end_pos), K(column_index_count));
  } else {
    // set all position
    buf_ = buf;
    row_end_pos_ = row_end_pos;
    start_pos_ = pos;
    pos_ = pos;
    if (column_index_count >= 0) {
      if (OB_FAIL(analyze_row_header(column_index_count,
              trans_id_ptr))) {  // analyze row header
        STORAGE_LOG(WARN, "invalid row reader argument.", K(ret), K(column_index_count));
      } else if (column_index_count > 0) {  // get column index array
        store_column_indexs_ = buf_ + row_end_pos_ - row_header_->get_column_index_bytes() * column_index_count;
      } else {  // 0 == column_index_count : ignore index array
        store_column_indexs_ = NULL;
      }
    } else {  // -1 == column_index_count : no RowHeader and index array
      row_header_ = NULL;
      store_column_indexs_ = NULL;
    }
    if (OB_SUCC(ret)) {
      is_setuped_ = true;
    }
  }
  return ret;
}

/*
 * Row : ObRowHeader | (ObTransID) | Column Array | Column Index Array
 * */
OB_INLINE int ObFlatRowReader::analyze_row_header(const int64_t input_column_cnt, transaction::ObTransID* trans_id_ptr)
{
  int ret = OB_SUCCESS;
  int64_t row_header_size = ObRowHeader::get_serialized_size();
  row_header_ = reinterpret_cast<const ObRowHeader*>(buf_ + pos_);  // get RowHeader
  if (OB_UNLIKELY(input_column_cnt < 0 ||
                  pos_ + row_header_size + row_header_->get_column_index_bytes() * input_column_cnt >= row_end_pos_)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "invalid row reader argument", K(ret), K(pos_), K(row_end_pos_), KPC(row_header_));
    row_header_ = NULL;
  } else {
    pos_ += row_header_size;  // move forward
    DESERIALIZE_TRANS_ID(trans_id_ptr);
    STORAGE_LOG(DEBUG, "success to deserialize trans id", K(ret), K(*trans_id_ptr), K(row_header_->get_version()));
  }
  return ret;
}

// row is store in flat type (cells array | index array)
int ObFlatRowReader::read_row(const char* row_buf, const int64_t row_end_pos, int64_t pos,
    const ObColumnMap& column_map, ObIAllocator& allocator, storage::ObStoreRow& row,
    const ObRowStoreType out_type /* = FLAT_ROW_STORE*/)
{
  int ret = OB_SUCCESS;
  // set position and get row header
  if (OB_FAIL(setup_row(row_buf, row_end_pos, pos, column_map.get_store_count(), row.trans_id_ptr_))) {
    STORAGE_LOG(WARN, "Fail to set up row", K(ret), K(pos), K(row_end_pos));
  } else if (OB_LIKELY(FLAT_ROW_STORE == out_type)) {  // read into flat row type
    if (OB_FAIL(read_flat_row_from_flat_storage(column_map, allocator, row))) {
      STORAGE_LOG(WARN, "Fail to read flat row from flat storage", K(ret), K(column_map));
    }
  } else if (SPARSE_ROW_STORE == out_type) {  // read into sparse row type
    if (OB_FAIL(read_sparse_row_from_flat_storage(column_map, allocator, row))) {
      STORAGE_LOG(WARN, "Fail to read flat row from flat storage", K(ret), K(column_map));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not support out type", K(ret), K(out_type));
  }
  reset();
  return ret;
}

int ObFlatRowReader::read_flat_row_from_flat_storage(
    const ObColumnMap& column_map, ObIAllocator& allocator, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == column_map.get_store_count() || 0 == column_map.get_request_count() ||
                  NULL == column_map.get_column_indexs() || column_map.get_request_count() > row.row_val_.count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "Invalid argument, ",
        K(ret),
        K(column_map.get_store_count()),
        K(column_map.get_request_count()),
        K(column_map.get_column_indexs()),
        K(row));
  } else if (OB_UNLIKELY(NULL == row_header_ || NULL == store_column_indexs_ || NULL == buf_ || pos_ >= row_end_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(
        WARN, "unexpected error", K(ret), K(row_header_), K(store_column_indexs_), K(buf_), K(pos_), K(row_end_pos_));
  } else {
    SET_ROW_BASIC_INFO(FLAT_ROW_STORE, row);  // set row_type/flag/dml/row_type_flag
    const int64_t column_cnt = column_map.get_request_count();
    row.row_val_.count_ = column_cnt;  // set row count
    const ObColumnIndexItem* column_idx = column_map.get_column_indexs();
    const int8_t column_index_bytes = row_header_->get_column_index_bytes();
    int64_t cur_read_idx = 0;
    int64_t column_index_offset = 0;
    int i = 0;
    if (column_map.get_seq_read_column_count() > 0) {
      if (OB_FAIL(sequence_read_flat_column(column_map, allocator, row))) {
        STORAGE_LOG(WARN, "seuqence read flat column failed", K(column_map));
      } else {
        i += column_map.get_seq_read_column_count();
      }
    }
    for (; OB_SUCC(ret) && i < column_cnt; ++i) {  // loop the ColumnIndex array
      if (column_idx[i].store_index_ < 0) {        // not exists
        row.row_val_.cells_[i].set_nop_value();
      } else {
        if (cur_read_idx == column_idx[i].store_index_) {  // sequence read at pos
          if (OB_FAIL(read_obj(column_idx[i].request_column_type_, allocator, row.row_val_.cells_[i]))) {
            STORAGE_LOG(WARN, "Fail to read column, ", K(ret), K(i), K_(pos), K(column_idx[i]));
            for (int j = 0; j < i; ++j) {
              STORAGE_LOG(WARN, "Fail to read column", K(ret), K(j), K(row.row_val_.cells_[j]), K(column_idx[j]));
            }
          } else {
            cur_read_idx++;
          }
        } else {                          // need project
          if (2 == column_index_bytes) {  // get offset
            column_index_offset = reinterpret_cast<const int16_t*>(store_column_indexs_)[column_idx[i].store_index_];
          } else if (1 == column_index_bytes) {
            column_index_offset = reinterpret_cast<const int8_t*>(store_column_indexs_)[column_idx[i].store_index_];
          } else if (4 == column_index_bytes) {
            column_index_offset = reinterpret_cast<const int32_t*>(store_column_indexs_)[column_idx[i].store_index_];
          } else {
            ret = OB_INVALID_DATA;
            STORAGE_LOG(WARN, "Invalid column index bytes, ", K(ret), K(column_index_bytes));
          }
          if (OB_SUCC(ret)) {  // read an obj
            pos_ = start_pos_ + column_index_offset;
            if (OB_FAIL(read_obj(column_idx[i].request_column_type_, allocator, row.row_val_.cells_[i]))) {
              STORAGE_LOG(WARN, "Fail to read column, ", K(ret), K(i), K_(pos), K(column_idx[i]));
              for (int j = 0; j < i; ++j) {
                STORAGE_LOG(WARN, "Fail to read column, ", K(ret), K(j), K(row.row_val_.cells_[j]), K(column_idx[j]));
              }
            } else {  // set sequence read pos
              cur_read_idx = column_idx[i].store_index_ + 1;
            }
          }
        }
      }
    }  // end for
  }
  return ret;
}

int ObFlatRowReader::read_sparse_row_from_flat_storage(
    const ObColumnMap& column_map, ObIAllocator& allocator, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == column_map.get_store_count() || 0 == column_map.get_request_count() ||
                  NULL == column_map.get_column_indexs())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "Invalid argument, ",
        K(ret),
        K(column_map.get_store_count()),
        K(column_map.get_request_count()),
        K(column_map.get_column_indexs()));
  } else if (OB_UNLIKELY(NULL == row_header_ || NULL == store_column_indexs_ || NULL == buf_ || pos_ >= row_end_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(
        WARN, "unexpected error", K(ret), K(row_header_), K(store_column_indexs_), K(buf_), K(pos_), K(row_end_pos_));
  } else {
    SET_ROW_BASIC_INFO(SPARSE_ROW_STORE, row);  // set row_type/flag/dml/row_type_flag
    // read columns
    const int64_t column_cnt = column_map.get_request_count();
    const ObColumnIndexItem* column_idx = column_map.get_column_indexs();
    const int8_t column_index_bytes = row_header_->get_column_index_bytes();
    int64_t cur_read_idx = 0;
    int sparse_col_index = 0;  // write index
    int64_t column_index_offset = 0;
    if (OB_ISNULL(row.column_ids_)) {  // need alloc column id array
      ALLOC_COL_ID_ARRAY(allocator, row);
    }
    // loop the ColumnIndex array in ObColumnMap
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      if (column_idx[i].store_index_ < 0) {
        // do nothing
      } else if (sparse_col_index > row.capacity_) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "input row cell array is not enough", K(row.capacity_));
      } else {
        if (cur_read_idx == column_idx[i].store_index_) {
          // sequence read
          if (OB_FAIL(read_obj(column_idx[i].request_column_type_, allocator, row.row_val_.cells_[sparse_col_index]))) {
            STORAGE_LOG(WARN, "Fail to read column, ", K(ret), K(i), K_(pos), K(column_idx[i]));
          } else {
            cur_read_idx++;
            if (!row.row_val_.cells_[sparse_col_index].is_nop_value()) {     // not nop value
              row.column_ids_[sparse_col_index] = column_idx[i].column_id_;  // record column_id
              sparse_col_index++;                                            // move forward
            }
          }
        } else {  // need project
          if (2 == column_index_bytes) {
            column_index_offset = reinterpret_cast<const int16_t*>(store_column_indexs_)[column_idx[i].store_index_];
          } else if (1 == column_index_bytes) {
            column_index_offset = reinterpret_cast<const int8_t*>(store_column_indexs_)[column_idx[i].store_index_];
          } else if (4 == column_index_bytes) {
            column_index_offset = reinterpret_cast<const int32_t*>(store_column_indexs_)[column_idx[i].store_index_];
          } else {
            ret = OB_INVALID_DATA;
            STORAGE_LOG(WARN, "Invalid column index bytes, ", K(ret), K(column_index_bytes));
          }

          if (OB_SUCC(ret)) {
            pos_ = start_pos_ + column_index_offset;
            if (OB_FAIL(
                    read_obj(column_idx[i].request_column_type_, allocator, row.row_val_.cells_[sparse_col_index]))) {
              STORAGE_LOG(WARN, "Fail to read column, ", K(ret), K(i), K_(pos), K(column_idx[i]));
            } else {
              cur_read_idx = column_idx[i].store_index_ + 1;
              if (!row.row_val_.cells_[sparse_col_index].is_nop_value()) {
                row.column_ids_[sparse_col_index] = column_idx[i].column_id_;  // record column_id
                sparse_col_index++;
              }
            }
          }
        }
      }
    }                    // end for
    if (OB_SUCC(ret)) {  // set count to sparse column count
      row.row_val_.count_ = sparse_col_index;
    }
  }
  STORAGE_LOG(DEBUG, "read sparse row", K(row));
  return ret;
}

int ObFlatRowReader::read_full_row(const char* row_buf, const int64_t row_len, int64_t pos,
    common::ObObjMeta* column_type_array, ObIAllocator& allocator, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  const int64_t column_cnt = row.row_val_.count_;
  if (OB_FAIL(setup_row(row_buf, row_len, pos, column_cnt, row.trans_id_ptr_))) {
    STORAGE_LOG(WARN, "setup flat row failed", K(ret));
  } else if (OB_UNLIKELY(NULL == row_header_ || NULL == store_column_indexs_ || NULL == buf_ || pos_ >= row_end_pos_ ||
                         NULL == column_type_array)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(
        WARN, "unexpected error", K(ret), K(row_header_), K(store_column_indexs_), K(buf_), K(pos_), K(row_end_pos_));
  } else {
    SET_ROW_BASIC_INFO(FLAT_ROW_STORE, row);                    // set row_type/flag/dml/row_type_flag
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {  // read column array
      if (OB_FAIL(read_obj(column_type_array[i], allocator, row.row_val_.cells_[i]))) {
        STORAGE_LOG(WARN, "Fail to read column", K(ret), K(i));
      }
    }
  }
  reset();
  return ret;
}

int ObFlatRowReader::sequence_read_flat_column(
    const ObColumnMap& column_map, ObIAllocator& allocator, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_setuped_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should call setup func first", K(ret));
  } else if (OB_UNLIKELY(column_map.get_seq_read_column_count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(column_map.get_seq_read_column_count()));
  } else if (OB_UNLIKELY(NULL == row_header_ || NULL == store_column_indexs_ || NULL == buf_ || pos_ >= row_end_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(
        WARN, "unexpected error", K(ret), K(row_header_), K(store_column_indexs_), K(buf_), K(pos_), K(row_end_pos_));
  } else {
    const ObColumnIndexItem* column_idx = column_map.get_column_indexs();
    for (int i = 0; OB_SUCC(ret) && i < column_map.get_seq_read_column_count(); ++i) {
      if (OB_FAIL(read_obj(column_idx[i].request_column_type_, allocator, row.row_val_.cells_[i]))) {
        STORAGE_LOG(WARN, "Fail to read column, ", K(ret), K(i), K_(pos), K(column_idx[i]));
      }
    }
  }
  return ret;
}

int ObFlatRowReader::read_column(
    const common::ObObjMeta& src_meta, ObIAllocator& allocator, const int64_t col_index, ObObj& obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_setuped_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should call setup func first", K(ret), K(col_index));
  } else if (OB_UNLIKELY(col_index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(col_index));
  } else if (OB_UNLIKELY(NULL == row_header_ || NULL == store_column_indexs_ || NULL == buf_ || pos_ >= row_end_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(
        WARN, "unexpected error", K(ret), K(row_header_), K(store_column_indexs_), K(buf_), K(pos_), K(row_end_pos_));
  } else {
    const int8_t store_column_index_bytes = row_header_->get_column_index_bytes();
    int64_t column_index_offset = 0;
    if (2 == store_column_index_bytes) {
      column_index_offset = reinterpret_cast<const int16_t*>(store_column_indexs_)[col_index];
    } else if (1 == store_column_index_bytes) {
      column_index_offset = reinterpret_cast<const int8_t*>(store_column_indexs_)[col_index];
    } else if (4 == store_column_index_bytes) {
      column_index_offset = reinterpret_cast<const int32_t*>(store_column_indexs_)[col_index];
    } else {
      ret = OB_INVALID_DATA;
      STORAGE_LOG(WARN, "Invalid column index bytes, ", K(ret), K(store_column_index_bytes));
    }
    pos_ = start_pos_ + column_index_offset;  // set position
    if (OB_SUCC(ret) && OB_FAIL(read_obj(src_meta, allocator, obj))) {
      STORAGE_LOG(WARN, "read column failed", K(ret), K(src_meta));
    }
  }
  return ret;
}

int ObFlatRowReader::read_compact_rowkey(const ObObjMeta* column_types, const int64_t column_count, const char* buf,
    const int64_t row_end_pos, int64_t& pos, ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == column_types || column_count <= 0 || NULL == buf || pos < 0 || row_end_pos <= 0 ||
                  pos >= row_end_pos)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(column_types), K(column_count), K(buf), K(row_end_pos), K(pos));
  } else if (OB_FAIL(setup_row(buf, row_end_pos, pos, -1))) {  // no RowHeader
    STORAGE_LOG(WARN, "row reader fail to setup", K(ret), K(OB_P(buf)), K(row_end_pos), K(pos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      row.cells_[i].set_meta_type(column_types[i]);
      if (OB_FAIL(read_obj_no_meta(column_types[i], allocator_, row.cells_[i]))) {
        STORAGE_LOG(WARN, "row reader fail to read column", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      pos = pos_;
      row.count_ = column_count;
    }
  }
  reset();
  return ret;
}

int ObFlatRowReader::compare_meta_rowkey(const common::ObStoreRowkey& rhs, const ObColumnMap* column_map,
    const int64_t compare_column_count, const char* buf, const int64_t row_end_pos, const int64_t pos,
    int32_t& cmp_result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rhs.is_valid() || NULL == column_map || !column_map->is_valid() || compare_column_count <= 0 ||
                  NULL == buf || pos >= row_end_pos || pos < 0 || row_end_pos <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid row header argument.",
        K(ret),
        K(column_map),
        K(compare_column_count),
        K(rhs),
        K(buf),
        K(row_end_pos),
        K(pos));
  } else {
    const int64_t schema_rowkey_count = column_map->get_rowkey_store_count();
    const int64_t extra_multi_version_col_cnt = column_map->get_multi_version_rowkey_cnt();
    const int64_t store_rowkey_count = schema_rowkey_count + extra_multi_version_col_cnt;
    const int64_t version_column_index = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
        schema_rowkey_count, extra_multi_version_col_cnt);
    const int64_t sql_sequence_index =
        ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(schema_rowkey_count, extra_multi_version_col_cnt);
    if (OB_UNLIKELY(compare_column_count > store_rowkey_count || compare_column_count > rhs.get_obj_cnt())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN,
          "invalid compare column count",
          K(ret),
          K(compare_column_count),
          K(store_rowkey_count),
          K(rhs.get_obj_cnt()));
    } else if (OB_FAIL(setup_row(buf, row_end_pos, pos, 0 /*ignore column_index_array*/))) {
      STORAGE_LOG(WARN, "row reader fail to setup.", K(ret), K(OB_P(buf)), K(row_end_pos), K(pos));
    } else {
      cmp_result = 0;
      common::ObObj obj;
      const ObColumnIndexItem* items = column_map->get_column_indexs();
      for (int64_t i = 0; 0 == cmp_result && OB_SUCC(ret) && i < compare_column_count; ++i) {
        ObObjMeta obj_meta;
        if (OB_UNLIKELY(version_column_index == i || sql_sequence_index == i)) {
          obj_meta.set_int();  // trans version column OR sql sequence column
        } else {
          obj_meta = items[i].get_obj_meta();
        }
        obj.set_meta_type(obj_meta);
        if (OB_FAIL(read_obj_no_meta(obj_meta, allocator_, obj))) {
          STORAGE_LOG(WARN, "row reader fail to read column.", K(ret), K(i));
        } else {
          cmp_result = obj.compare(rhs.get_obj_ptr()[i], common::CS_TYPE_INVALID);
        }
      }  // end for
    }
  }
  reset();
  return ret;
}

int ObFlatRowReader::read_obj(const ObObjMeta& src_meta, ObIAllocator& allocator, ObObj& obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_setuped_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should call setup func first", K(ret));
  } else if (OB_UNLIKELY(NULL == buf_ || pos_ >= row_end_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error", K(ret), K(buf_), K(pos_), K(row_end_pos_));
  } else {
    const ObObj* obj_ptr = NULL;
    const ObObjTypeClass type_class = src_meta.get_type_class();
    const ObObjType column_type = src_meta.get_type();
    bool need_cast = true;
    bool empty_str = false;
    const ObStoreMeta* meta = read<ObStoreMeta>(buf_, pos_);
    obj.set_meta_type(src_meta);
    switch (meta->type_) {
      case ObNullStoreType: {
        obj.set_null();
        break;
      }
      case ObIntStoreType: {
        switch (meta->attr_) {
          case 0:  // value is 1 byte
            obj.set_tinyint(*read<int8_t>(buf_, pos_));
            break;
          case 1:  // value is 2 bytes
            obj.set_smallint(*read<int16_t>(buf_, pos_));
            break;
          case 2:  // value is 4 bytes
            obj.set_int32(*read<int32_t>(buf_, pos_));
            break;
          case 3:  // value is 8 bytes
            obj.set_int(*read<int64_t>(buf_, pos_));
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            STORAGE_LOG(WARN, "not support attr, ", K(ret), K(meta->attr_));
        }
        if (ObIntTC == type_class && src_meta.get_type() > obj.get_type()) {
          need_cast = false;
        } else if (ObUIntTC == type_class && obj.get_int() >= 0 &&
                   static_cast<uint64_t>(obj.get_int()) <= UINT_MAX_VAL[src_meta.get_type()]) {
          need_cast = false;
        }
        break;
      }
      case ObFloatStoreType: {
        obj.set_float(*read<float>(buf_, pos_));
        if (ObFloatTC == type_class && obj.get_float() >= 0.0) {
          need_cast = false;
        }
        break;
      }
      case ObDoubleStoreType: {
        obj.set_double(*read<double>(buf_, pos_));
        if (ObDoubleTC == type_class && obj.get_double() >= 0.0) {
          need_cast = false;
        }
        break;
      }
      case ObNumberStoreType: {
        number::ObNumber::Desc tmp_desc;
        tmp_desc.desc_ = *read<uint32_t>(buf_, pos_);
        tmp_desc.cap_ = tmp_desc.len_;
        obj.set_number(tmp_desc, 0 == tmp_desc.len_ ? NULL : (uint32_t*)(buf_ + pos_));
        pos_ += sizeof(uint32_t) * tmp_desc.len_;
        if (ObNumberTC == type_class && !obj.is_negative_number()) {
          need_cast = false;
        }
        break;
      }
      case ObTimestampStoreType: {
        switch (meta->attr_) {
          case 0:  // ObDateType
            obj.set_date(*read<int32_t>(buf_, pos_));
            break;
          case 1:  // ObDateTimeType
            obj.set_datetime(*read<int64_t>(buf_, pos_));
            break;
          case 2:  // ObTimestampType
            obj.set_timestamp(*read<int64_t>(buf_, pos_));
            break;
          case 3:  // ObTimeType
            obj.set_time(*read<int64_t>(buf_, pos_));
            break;
          case 4:  // ObYearType
            obj.set_year(*read<uint8_t>(buf_, pos_));
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            STORAGE_LOG(WARN, "not support attr, ", K(ret), K(meta->attr_));
        }
        break;
      }
      case ObTimestampTZStoreType: {
        const int64_t* time_us = NULL;
        const uint32_t* time_ctx_desc = NULL;
        const uint16_t* time_desc = NULL;
        const int64_t length = ObObj::get_otimestamp_store_size(common::OTMAT_TIMESTAMP_TZ == meta->attr_);
        if (OB_UNLIKELY(pos_ + length > row_end_pos_)) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "buf is not large", K(ret), K(pos_), K(length), K(row_end_pos_));
        } else if (OB_ISNULL((time_us = read<int64_t>(buf_, pos_)))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "time_us is not expected", K(ret), K(pos_));
        } else if (common::OTMAT_TIMESTAMP_TZ == meta->attr_ &&
                   OB_ISNULL((time_ctx_desc = read<uint32_t>(buf_, pos_)))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "time_ctx_desc is not expected", K(ret), K(pos_));
        } else if (common::OTMAT_TIMESTAMP_TZ != meta->attr_ && OB_ISNULL((time_desc = read<uint16_t>(buf_, pos_)))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "time_desc is not expected", K(ret), K(pos_));
        } else {
          switch (meta->attr_) {
            case common::OTMAT_TIMESTAMP_TZ:  // ObTimestampTZType
              obj.set_timestamp_tz(*time_us, *time_ctx_desc);
              break;
            case common::OTMAT_TIMESTAMP_LTZ:  // ObTimestampLTZType
              obj.set_timestamp_ltz(*time_us, *time_desc);
              break;
            case common::OTMAT_TIMESTAMP_NANO:  // ObTimestampNanoType
              obj.set_timestamp_nano(*time_us, *time_desc);
              break;
            default:
              ret = OB_NOT_SUPPORTED;
              STORAGE_LOG(WARN, "not support attr, ", K(ret), K(meta->attr_));
          }
        }
        break;
      }
      case ObCharStoreType: {
        ObString value;
        if (0 == meta->attr_) {
          obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        } else if (1 == meta->attr_) {
          obj.set_collation_type(static_cast<ObCollationType>(*read<uint8_t>(buf_, pos_)));
        } else {
          ret = OB_NOT_SUPPORTED;
          STORAGE_LOG(WARN, "row reader only suport utf8 character set.", K(ret), K(meta->attr_));
        }
        if (OB_SUCC(ret)) {
          const uint32_t* len = read<uint32_t>(buf_, pos_);
          if (OB_UNLIKELY(pos_ + *len > row_end_pos_)) {
            ret = OB_BUF_NOT_ENOUGH;
            STORAGE_LOG(WARN, "buf is not large", K(ret), K(pos_), K(*len), K_(row_end_pos));
          } else {
            value.assign_ptr((char*)(buf_ + pos_), *len);
            pos_ += *len;
            obj.set_char(value);
          }
        }
        empty_str = 0 == obj.get_val_len();
        if ((ob_is_string_tc(src_meta.get_type()) || (src_meta.is_raw())) &&
            obj.get_collation_type() == src_meta.get_collation_type()) {
          need_cast = false;
        }
        break;
      }
      case ObTextStoreType: {
        if (OB_FAIL(read_text_store(*meta, allocator, obj))) {
          STORAGE_LOG(WARN, "fail to read text store", K(ret));
        } else if (src_meta.is_lob_outrow() || obj.get_collation_type() == src_meta.get_collation_type()) {
          need_cast = false;
        }
        empty_str = 0 == obj.get_val_len();
        break;
      }
      case ObHexStoreType: {
        ObString value;
        const uint32_t* len = read<uint32_t>(buf_, pos_);
        if (OB_UNLIKELY(pos_ + *len > row_end_pos_)) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "buf not enough, ", K(ret), K(pos_), K(*len), K(row_end_pos_));
        } else {
          value.assign_ptr((char*)(buf_ + pos_), *len);
          pos_ += *len;
          obj.set_hex_string(value);
        }
        break;
      }
      case ObExtendStoreType: {
        if (0 == meta->attr_) {
          obj.set_nop_value();
        } else {
          ret = OB_NOT_SUPPORTED;
          STORAGE_LOG(WARN, "the attr type is not supported.", K(ret), K(meta->attr_));
        }
        break;
      }
      case ObIntervalYMStoreType: {
        if (OB_UNLIKELY(pos_ + ObIntervalYMValue::get_store_size() > row_end_pos_)) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "buf not enough, ", K(ret), K(pos_), K(row_end_pos_));
        } else {
          const int64_t year_value = *read<int64_t>(buf_, pos_);
          obj.set_interval_ym(ObIntervalYMValue(year_value));
        }
        break;
      }
      case ObIntervalDSStoreType: {
        if (OB_UNLIKELY(pos_ + ObIntervalDSValue::get_store_size() > row_end_pos_)) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "buf not enough, ", K(ret), K(pos_), K(row_end_pos_));
        } else {
          const int64_t second_value = *read<int64_t>(buf_, pos_);
          const int32_t fs_value = *read<int32_t>(buf_, pos_);
          obj.set_interval_ds(ObIntervalDSValue(second_value, fs_value));
        }
        break;
      }
      case ObRowIDStoreType: {
        const uint32_t* len = read<uint32_t>(buf_, pos_);
        if (OB_UNLIKELY(pos_ + *len > row_end_pos_)) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "buf not enough, ", K(ret), K(pos_), K(*len), K(row_end_pos_));
        } else {
          obj.set_urowid((char*)(buf_ + pos_), *len);
          pos_ += *len;
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "the attr type is not supported.", K(ret), K(meta), K(src_meta));
      } break;
    }

    if (OB_SUCC(ret)) {
      if (ObNullType == obj.get_type() || ObExtendType == obj.get_type()) {
        // ignore src_meta type, do nothing, just return obj
      } else if (((obj.get_type() == src_meta.get_type() || src_meta.is_raw()) && !need_cast) ||
                 empty_str) {  // just change the type
        // extra bypaas path for raw, or data will be wrong
        obj.set_type(src_meta.get_type());
        if (empty_str && ObTextTC == type_class) {
          obj.set_lob_inrow();
        }
      } else {
        // not support data alteration
        ObObj ori_obj = obj;
        int64_t cm_mode = CM_NONE;
        ObObjTypeClass ori_type_class = ori_obj.get_type_class();
        if (ObIntTC == ori_type_class && ObUIntTC == type_class) {
          obj.set_uint(src_meta.get_type(), static_cast<uint64_t>(ori_obj.get_int()));
        } else if (ObIntTC == ori_type_class && ObBitTC == type_class) {
          obj.set_bit(static_cast<uint64_t>(ori_obj.get_int()));
        } else if (ObIntTC == ori_type_class && ObEnumType == column_type) {
          obj.set_enum(static_cast<uint64_t>(ori_obj.get_int()));
        } else if (ObIntTC == ori_type_class && ObSetType == column_type) {
          obj.set_set(static_cast<uint64_t>(ori_obj.get_int()));
        } else {
          ObCastCtx cast_ctx(&allocator, NULL, cm_mode, src_meta.get_collation_type());
          if (OB_FAIL(ObObjCaster::to_type(src_meta.get_type(), cast_ctx, ori_obj, obj))) {
            STORAGE_LOG(WARN,
                "fail to cast obj",
                K(ret),
                K(ori_obj),
                K(obj),
                K(obj_ptr),
                K(ori_obj.get_type()),
                K(ob_obj_type_str(ori_obj.get_type())),
                K(src_meta.get_type()),
                K(ob_obj_type_str(src_meta.get_type())));
          }
        }
      }
    }
  }
  return ret;
}

int ObFlatRowReader::read_obj_no_meta(
    const common::ObObjMeta& src_meta, common::ObIAllocator& allocator, common::ObObj& obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_setuped_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should call setup func first", K(ret));
  } else if (OB_UNLIKELY(NULL == buf_ || pos_ >= row_end_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error", K(ret), K(buf_), K(pos_), K(row_end_pos_));
  } else {
    const ObStoreMeta* meta = read<ObStoreMeta>(buf_, pos_);
    if (obj.is_null() || obj.is_nop_value()) {
      STORAGE_LOG(ERROR, "obj can not be null or nop here!!!", K(obj), K(src_meta));
      ret = OB_ERR_UNEXPECTED;
    } else {
      switch (meta->type_) {
        case ObNullStoreType: {
          obj.set_null();
          break;
        }
        case ObIntStoreType: {
          switch (meta->attr_) {
            case 0:  // value is 1 byte
              obj.set_tinyint_value(*read<int8_t>(buf_, pos_));
              break;
            case 1:  // value is 2 bytes
              obj.set_smallint_value(*read<int16_t>(buf_, pos_));
              break;
            case 2:  // value is 4 bytes
              obj.set_int32_value(*read<int32_t>(buf_, pos_));
              break;
            case 3:  // value is 8 bytes
              obj.set_int_value(*read<int64_t>(buf_, pos_));
              break;
            default:
              ret = OB_NOT_SUPPORTED;
              STORAGE_LOG(WARN, "not support attr, ", K(ret), K(meta->attr_));
          }
          break;
        }
        case ObFloatStoreType: {
          obj.set_float_value(*read<float>(buf_, pos_));
          break;
        }
        case ObDoubleStoreType: {
          obj.set_double_value(*read<double>(buf_, pos_));
          break;
        }
        case ObNumberStoreType: {
          number::ObNumber::Desc tmp_desc;
          tmp_desc.desc_ = *read<uint32_t>(buf_, pos_);
          tmp_desc.cap_ = tmp_desc.len_;
          obj.set_number_value(tmp_desc, 0 == tmp_desc.len_ ? NULL : (uint32_t*)(buf_ + pos_));
          pos_ += sizeof(uint32_t) * tmp_desc.len_;
          break;
        }
        case ObTimestampStoreType: {
          switch (meta->attr_) {
            case 0:  // ObDateType
              obj.set_date_value(*read<int32_t>(buf_, pos_));
              break;
            case 1:  // ObDateTimeType
              obj.set_datetime_value(*read<int64_t>(buf_, pos_));
              break;
            case 2:  // ObTimestampType
              obj.set_timestamp_value(*read<int64_t>(buf_, pos_));
              break;
            case 3:  // ObTimeType
              obj.set_time_value(*read<int64_t>(buf_, pos_));
              break;
            case 4:  // ObYearType
              obj.set_year_value(*read<uint8_t>(buf_, pos_));
              break;
            default:
              ret = OB_NOT_SUPPORTED;
              STORAGE_LOG(WARN, "not support attr, ", K(ret), K(meta->attr_));
          }
          break;
        }
        case ObTimestampTZStoreType: {
          const int64_t* time_us = NULL;
          const uint32_t* time_ctx_desc = NULL;
          const uint16_t* time_desc = NULL;
          const int64_t length = ObObj::get_otimestamp_store_size(common::OTMAT_TIMESTAMP_TZ == meta->attr_);
          if (OB_UNLIKELY(pos_ + length > row_end_pos_)) {
            ret = OB_BUF_NOT_ENOUGH;
            STORAGE_LOG(WARN, "buf is not large", K(ret), K(pos_), K(length), K(row_end_pos_));
          } else if (OB_ISNULL((time_us = read<int64_t>(buf_, pos_)))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "time_us is not expected", K(ret), K(pos_));
          } else if (common::OTMAT_TIMESTAMP_TZ == meta->attr_ &&
                     OB_ISNULL((time_ctx_desc = read<uint32_t>(buf_, pos_)))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "time_ctx_desc is not expected", K(ret), K(pos_));
          } else if (common::OTMAT_TIMESTAMP_TZ != meta->attr_ && OB_ISNULL((time_desc = read<uint16_t>(buf_, pos_)))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "time_desc is not expected", K(ret), K(pos_));
          } else {
            switch (meta->attr_) {
              case common::OTMAT_TIMESTAMP_TZ:  // ObTimestampTZType
                obj.set_timestamp_tz(*time_us, *time_ctx_desc);
                break;
              case common::OTMAT_TIMESTAMP_LTZ:  // ObTimestampLTZType
                obj.set_timestamp_ltz(*time_us, *time_desc);
                break;
              case common::OTMAT_TIMESTAMP_NANO:  // ObTimestampNanoType
                obj.set_timestamp_nano(*time_us, *time_desc);
                break;
              default:
                ret = OB_NOT_SUPPORTED;
                STORAGE_LOG(WARN, "not support attr, ", K(ret), K(meta->attr_));
            }
          }
          break;
        }
        case ObCharStoreType: {
          ObString value;
          if (0 == meta->attr_) {
            obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          } else if (1 == meta->attr_) {
            obj.set_collation_type(static_cast<ObCollationType>(*read<uint8_t>(buf_, pos_)));
          } else {
            ret = OB_NOT_SUPPORTED;
            STORAGE_LOG(WARN, "row reader only suport utf8 character set.", K(ret), K(meta->attr_));
          }
          if (OB_SUCC(ret)) {
            const uint32_t* len = read<uint32_t>(buf_, pos_);
            if (OB_UNLIKELY(pos_ + *len > row_end_pos_)) {
              ret = OB_BUF_NOT_ENOUGH;
              STORAGE_LOG(WARN, "buf is not large", K(ret), K(pos_), K(*len), K(row_end_pos_));
            } else {
              value.assign_ptr((char*)(buf_ + pos_), *len);
              pos_ += *len;
              obj.set_char(value);
            }
          }
          if ((src_meta.is_string_type() || src_meta.is_raw()) &&
              obj.get_collation_type() == src_meta.get_collation_type()) {
            obj.set_type(src_meta.get_type());
            // for Compatibility with 1470
            if (src_meta.is_lob()) {
              obj.set_lob_inrow();
            }
          }
          break;
        }
        case ObTextStoreType: {
          if (OB_FAIL(read_text_store(*meta, allocator, obj))) {
            STORAGE_LOG(WARN, "fail to read text store", K(ret));
          } else if (src_meta.is_lob() && obj.get_collation_type() == src_meta.get_collation_type()) {
            obj.set_type(src_meta.get_type());
          }
          break;
        }
        case ObHexStoreType: {
          ObString value;
          const int32_t* len = read<int32_t>(buf_, pos_);
          if (OB_UNLIKELY(pos_ + *len > row_end_pos_)) {
            ret = OB_BUF_NOT_ENOUGH;
            STORAGE_LOG(WARN, "buf not enough, ", K(ret), K(pos_), K(*len), K(row_end_pos_));
          } else {
            value.assign_ptr((char*)(buf_ + pos_), *len);
            pos_ += *len;
            obj.set_hex_string_value(value);
          }
          break;
        }
        case ObExtendStoreType: {
          if (0 == meta->attr_) {
            obj.set_nop_value();
          } else {
            ret = OB_NOT_SUPPORTED;
            STORAGE_LOG(WARN, "the attr type is not supported.", K(ret), K(meta->attr_));
          }
          // obj.set_type(src_meta.get_type());
          break;
        }
        case ObIntervalYMStoreType: {
          if (pos_ + ObIntervalYMValue::get_store_size() > row_end_pos_) {
            ret = OB_BUF_NOT_ENOUGH;
            STORAGE_LOG(WARN, "buf not enough, ", K(ret), K(pos_), K(row_end_pos_));
          } else {
            const int64_t year_value = *read<int64_t>(buf_, pos_);
            obj.set_interval_ym(ObIntervalYMValue(year_value));
          }
          break;
        }
        case ObIntervalDSStoreType: {
          if (pos_ + ObIntervalDSValue::get_store_size() > row_end_pos_) {
            ret = OB_BUF_NOT_ENOUGH;
            STORAGE_LOG(WARN, "buf not enough, ", K(ret), K(pos_), K(row_end_pos_));
          } else {
            const int64_t second_value = *read<int64_t>(buf_, pos_);
            const int32_t fs_value = *read<int32_t>(buf_, pos_);
            obj.set_interval_ds(ObIntervalDSValue(second_value, fs_value));
          }
          break;
        }
        case ObRowIDStoreType: {
          const uint32_t* len = read<uint32_t>(buf_, pos_);
          if (OB_UNLIKELY(pos_ + *len > row_end_pos_)) {
            ret = OB_BUF_NOT_ENOUGH;
            STORAGE_LOG(WARN, "buf is not large", K(ret), K(pos_), K(*len), K(row_end_pos_));
          } else {
            obj.set_urowid((char*)(buf_ + pos_), *len);
            pos_ += *len;
          }
          break;
        }
        default:
          ret = OB_NOT_SUPPORTED;
          STORAGE_LOG(
              ERROR, "row reader don't support this data type.", K(ret), K(src_meta.get_type()), K(meta->type_));
      }  // switch end
    }
    STORAGE_LOG(DEBUG, "row reader", K(ret), K(src_meta.get_type()), K(meta->type_), K(pos_), K(obj));
  }
  return ret;
}

/***********************************ObSparseRowReader*******************************************/
ObSparseRowReader::ObSparseRowReader()
{}
/*
 * SparseRow : ObRowHeader | Column Array | Column Index Array | Column id Array
 * */
inline int ObSparseRowReader::setup_row(const char* buf, const int64_t row_end_pos, const int64_t pos,
    const int64_t column_index_count /* = INT_MAX*/, transaction::ObTransID* trans_id_ptr /* = nullptr*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || row_end_pos <= 0 || pos < 0 || pos >= row_end_pos)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row reader argument.", K(ret), K(OB_P(buf)), K(row_end_pos));
  } else {
    // set all position
    buf_ = buf;
    row_end_pos_ = row_end_pos;
    start_pos_ = pos;
    pos_ = start_pos_;
    if (column_index_count >= 0) {
      if (OB_FAIL(analyze_row_header(trans_id_ptr))) {  // analyze row header
        STORAGE_LOG(WARN, "invalid row reader argument.", K(ret));
      } else if (column_index_count > 0) {                     // get index array and column id array
        int16_t column_cnt = row_header_->get_column_count();  // use column_count in RowHeader
        const char* column_id_pos = buf_ + row_end_pos_ - sizeof(uint16_t) * column_cnt;
        column_ids_ = reinterpret_cast<const uint16_t*>(column_id_pos);
        store_column_indexs_ = column_id_pos - row_header_->get_column_index_bytes() * column_cnt;
      } else {  // 0 == column_index_count : ignore column index array & column id array
        store_column_indexs_ = NULL;
        column_ids_ = NULL;
      }
    } else {  // -1 == column_index_count : no RowHeader & column index array & column id array
      row_header_ = NULL;
      store_column_indexs_ = NULL;
      column_ids_ = NULL;
    }
    if (OB_SUCC(ret)) {
      is_setuped_ = true;
    }
  }
  return ret;
}

/*
 * Row : ObRowHeader | (ObTransID) | Column Array | Column Index Array | Column id Array
 * */
OB_INLINE int ObSparseRowReader::analyze_row_header(transaction::ObTransID* trans_id_ptr)
{
  int ret = OB_SUCCESS;
  int64_t row_header_size = ObRowHeader::get_serialized_size();
  row_header_ = reinterpret_cast<const ObRowHeader*>(buf_ + pos_);  // get RowHeader
  // sparse row use the column_count_ in RowHeader
  int64_t column_count = row_header_->get_column_count();
  if (OB_UNLIKELY(column_count < 0 || pos_ + row_header_size +
                                              sizeof(uint16_t) * column_count  // column id array length
                                              + row_header_->get_column_index_bytes() * column_count >=
                                          row_end_pos_)) {  // column index array length
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "invalid row reader argument", K(ret), K(pos_), K(row_end_pos_), KPC(row_header_));
    row_header_ = NULL;
  } else {
    pos_ += row_header_size;  // move forward
    DESERIALIZE_TRANS_ID(trans_id_ptr);
    STORAGE_LOG(DEBUG, "success to deserialize trans id", K(ret), KPC(trans_id_ptr), K(row_header_->get_version()));
  }
  return ret;
}

int ObSparseRowReader::read_row(const char* row_buf, const int64_t row_end_pos, int64_t pos,
    const ObColumnMap& column_map, ObIAllocator& allocator, storage::ObStoreRow& row,
    const ObRowStoreType out_type /* = FLAT_ROW_STORE*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(setup_row(row_buf, row_end_pos, pos, INT64_MAX, row.trans_id_ptr_))) {
    STORAGE_LOG(WARN, "Fail to set up sparse row", K(ret), K(row_end_pos), K(pos));
  } else if (FLAT_ROW_STORE == out_type) {
    if (OB_FAIL(read_flat_row_from_sparse_storage(column_map, allocator, row))) {
      STORAGE_LOG(WARN, "Fail to read flat row from sparse storage", K(column_map));
    }
  } else if (SPARSE_ROW_STORE == out_type) {
    if (OB_FAIL(read_sparse_row_from_sparse_storage(column_map, allocator, row))) {
      STORAGE_LOG(WARN, "Fail to read flat row from sparse storage", K(column_map));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not support out type", K(ret), K(out_type));
  }
  reset();
  return ret;
}

int ObSparseRowReader::read_flat_row_from_sparse_storage(
    const ObColumnMap& column_map, ObIAllocator& allocator, storage::ObStoreRow& row)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == column_map.get_cols_map() || NULL == column_map.get_column_indexs() ||
                  0 == column_map.get_request_count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(ret),
        K(column_map.get_cols_map()),
        K(column_map.get_column_indexs()),
        K(column_map.get_request_count()));
  } else if (OB_ISNULL(row_header_) || OB_ISNULL(store_column_indexs_) || OB_ISNULL(buf_) ||
             OB_UNLIKELY(pos_ >= row_end_pos_) || OB_ISNULL(column_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "unexpected error",
        K(ret),
        K(row_header_),
        K(store_column_indexs_),
        K(buf_),
        K(pos_),
        K(row_end_pos_),
        K(column_ids_));
  } else {
    SET_ROW_BASIC_INFO(FLAT_ROW_STORE, row);  // set row_type/flag/dml/row_type_flag
    row.row_val_.count_ = column_map.get_request_count();
    int column_index_bytes = row_header_->get_column_index_bytes();
    for (int i = 0; i < row.row_val_.count_; ++i) {  // set nop value to all column
      row.row_val_.cells_[i].set_nop_value();
    }
    int64_t column_index_offset = 0;
    int proj_pos = -1;
    const share::schema::ColumnMap* cols_id_map_ptr = column_map.get_cols_map();
    const ObColumnIndexItem* column_index_ptr = column_map.get_column_indexs();
    ObSparseCellReader cell_reader;  // Use CellReader to read column
    if (OB_FAIL(cell_reader.init(buf_, row_end_pos_, &allocator))) {
      STORAGE_LOG(WARN, "Failed to init CellReader", K(ret), K(row_end_pos_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_header_->get_column_count(); ++i) {
      if (OB_FAIL(cols_id_map_ptr->get(column_ids_[i], proj_pos))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;  // not found is not an error
        } else {
          STORAGE_LOG(WARN, "get projector from ColumnMap failed", K(ret), K(i), "column_id", column_ids_[i]);
        }
      } else if (0 > proj_pos) {  // not needed column
        STORAGE_LOG(DEBUG, "not needed column", K(column_ids_[i]), K(row.row_val_.count_), K(*cols_id_map_ptr));
      } else if (proj_pos >= row.row_val_.count_) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "input row cell array is not enough", K(row.row_val_.count_));
      } else {
        if (2 == column_index_bytes) {
          column_index_offset = reinterpret_cast<const int16_t*>(store_column_indexs_)[i];
        } else if (1 == column_index_bytes) {
          column_index_offset = reinterpret_cast<const int8_t*>(store_column_indexs_)[i];
        } else if (4 == column_index_bytes) {
          column_index_offset = reinterpret_cast<const int32_t*>(store_column_indexs_)[i];
        } else {
          ret = OB_INVALID_DATA;
          STORAGE_LOG(WARN, "Invalid column index bytes, ", K(ret), K(column_index_bytes));
        }
        pos_ = start_pos_ + column_index_offset;
        cell_reader.set_pos(pos_);  // set CellReader position
        if (OB_SUCC(ret)) {
          if (OB_FAIL(cell_reader.read_cell(row.row_val_.cells_[proj_pos]))) {
            STORAGE_LOG(WARN, "read object failed", K(ret), "column_id", column_ids_[i], K(proj_pos));
          } else if (!row.row_val_.cells_[proj_pos].is_nop_value() && !row.row_val_.cells_[proj_pos].is_null() &&
                     column_index_ptr[i].request_column_type_ != row.row_val_.cells_[proj_pos].get_meta()) {
            if (OB_FAIL(cast_obj(
                    column_index_ptr[proj_pos].request_column_type_, allocator, row.row_val_.cells_[proj_pos]))) {
              STORAGE_LOG(WARN,
                  "failed to cast",
                  K(ret),
                  K(i),
                  K(proj_pos),
                  K(column_index_ptr[proj_pos].request_column_type_),
                  K(row.row_val_.cells_[proj_pos]));
            }
          }
        }
      }
    }  // end for
  }
  return ret;
}

int ObSparseRowReader::read_sparse_row_from_sparse_storage(
    const ObColumnMap& column_map, ObIAllocator& allocator, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == column_map.get_cols_map() || NULL == column_map.get_column_indexs() ||
                  0 == column_map.get_request_count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(ret),
        K(column_map.get_cols_map()),
        K(column_map.get_column_indexs()),
        K(column_map.get_request_count()));
  } else if (OB_UNLIKELY(NULL == row_header_ || NULL == store_column_indexs_ || NULL == buf_ || pos_ >= row_end_pos_ ||
                         NULL == column_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "unexpected error",
        K(ret),
        K(row_header_),
        K(store_column_indexs_),
        K(buf_),
        K(pos_),
        K(row_end_pos_),
        K(column_ids_));
  } else {
    SET_ROW_BASIC_INFO(SPARSE_ROW_STORE, row);  // set row_type/flag/dml/row_type_flag
    if (OB_ISNULL(row.column_ids_)) {           // need allocate column id array
      ALLOC_COL_ID_ARRAY(allocator, row);
    }
    if (OB_SUCC(ret)) {
      int proj_pos = -1;
      int64_t column_index_offset = 0;
      int sparse_col_index = 0;
      int column_index_bytes = row_header_->get_column_index_bytes();
      const share::schema::ColumnMap* cols_id_map_ptr = column_map.get_cols_map();
      ObSparseCellReader cell_reader;
      if (OB_FAIL(cell_reader.init(buf_, row_end_pos_, &allocator))) {  // init cell reader
        STORAGE_LOG(WARN, "Failed to init CellReader", K(ret), K(row_end_pos_));
      }
      // loop all sparse column
      for (int64_t i = 0; OB_SUCC(ret) && i < row_header_->get_column_count(); ++i) {
        if (OB_FAIL(cols_id_map_ptr->get(column_ids_[i], proj_pos))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;  // not found is not an error
          } else {
            STORAGE_LOG(WARN, "get projector from ColumnMap failed", K(ret), "column_id", column_ids_[i]);
          }
        } else if (0 > proj_pos) {  // not needed column
          STORAGE_LOG(DEBUG, "not needed column", K(ret), "column_id", column_ids_[i]);
        } else if (sparse_col_index >= row.capacity_ && sparse_col_index >= row.row_val_.count_) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "input row cell array is not enough", K(row.capacity_), K(row.row_val_.count_));
        } else {
          if (2 == column_index_bytes) {
            column_index_offset = reinterpret_cast<const int16_t*>(store_column_indexs_)[i];
          } else if (1 == column_index_bytes) {
            column_index_offset = reinterpret_cast<const int8_t*>(store_column_indexs_)[i];
          } else if (4 == column_index_bytes) {
            column_index_offset = reinterpret_cast<const int32_t*>(store_column_indexs_)[i];
          } else {
            ret = OB_INVALID_DATA;
            STORAGE_LOG(WARN, "Invalid column index bytes, ", K(ret), K(column_index_bytes));
          }
          if (OB_SUCC(ret)) {
            pos_ = start_pos_ + column_index_offset;
            cell_reader.set_pos(pos_);  // set position of CellReader
            if (OB_FAIL(cell_reader.read_cell(row.row_val_.cells_[sparse_col_index]))) {
              STORAGE_LOG(WARN, "read object failed", K(ret), "column_id", column_ids_[i]);
            } else {  // sparse row need to set column_ids_ array
              row.column_ids_[sparse_col_index] = column_ids_[i];
              ++sparse_col_index;
            }
          }
        }
      }                    // end for
      if (OB_SUCC(ret)) {  // set count to sparse column count
        row.row_val_.count_ = sparse_col_index;
      }
    }
  }
  return ret;
}

int ObSparseRowReader::read_full_row(const char* row_buf, const int64_t row_len, int64_t pos,
    common::ObObjMeta* column_type_array, ObIAllocator& allocator, storage::ObStoreRow& row)
{
  UNUSEDx(column_type_array, allocator);
  int ret = OB_SUCCESS;
  if (OB_FAIL(setup_row(row_buf, row_len, pos, INT64_MAX, row.trans_id_ptr_))) {
    STORAGE_LOG(WARN, "setup sparse row failed", K(ret));
  } else if (OB_UNLIKELY(NULL == row_header_ || NULL == store_column_indexs_ || NULL == column_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(
        WARN, "unexpected error", K(ret), K(row_header_), K(store_column_indexs_), K(buf_), K(pos_), K(row_end_pos_));
  } else {
    SET_ROW_BASIC_INFO(SPARSE_ROW_STORE, row);  // set row_type/flag/dml/row_type_flag
    if (OB_SUCC(ret)) {
      int index = 0;
      ObSparseCellReader cell_reader;
      if (OB_FAIL(cell_reader.init(buf_ + pos_, row_end_pos_, &allocator))) {
        STORAGE_LOG(WARN, "Failed to init CellReader", K(ret), K(pos_), K(row_end_pos_));
      }
      for (; OB_SUCC(ret) && index < row_header_->get_column_count(); ++index) {
        if (index >= row.capacity_) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "input row cell array is not enough", K(row.row_val_.count_), K(row.capacity_));
        } else if (OB_FAIL(cell_reader.read_cell(row.row_val_.cells_[index]))) {
          STORAGE_LOG(WARN, "read cell failed", K(ret), "column_id", column_ids_[index]);
        }
      }
      if (OB_SUCC(ret)) {                  // set count to sparse column count
        if (OB_ISNULL(row.column_ids_)) {  // use column id array
          ALLOC_COL_ID_ARRAY(allocator, row);
        }
        if (OB_SUCC(ret)) {
          MEMCPY(row.column_ids_, column_ids_, sizeof(uint16_t) * index);
          pos_ += cell_reader.get_pos();
          row.row_val_.count_ = index;
        }
      }
    }
  }
  reset();
  return ret;
}

int ObSparseRowReader::read_column(
    const common::ObObjMeta& src_meta, ObIAllocator& allocator, const int64_t col_index, ObObj& obj)
{
  UNUSED(src_meta);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_setuped_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should call setup func first", K(ret), K(col_index));
  } else if (OB_UNLIKELY(NULL == row_header_ || NULL == store_column_indexs_ || NULL == buf_ || pos_ >= row_end_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(
        WARN, "unexpected error", K(ret), K(row_header_), K(store_column_indexs_), K(buf_), K(pos_), K(row_end_pos_));
  } else if (OB_UNLIKELY(col_index >= row_header_->get_column_count() || col_index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(col_index));
  } else {
    const int8_t store_column_index_bytes = row_header_->get_column_index_bytes();
    int64_t column_index_offset = 0;
    ObSparseCellReader cell_reader;
    if (2 == store_column_index_bytes) {
      column_index_offset = reinterpret_cast<const int16_t*>(store_column_indexs_)[col_index];
    } else if (1 == store_column_index_bytes) {
      column_index_offset = reinterpret_cast<const int8_t*>(store_column_indexs_)[col_index];
    } else if (4 == store_column_index_bytes) {
      column_index_offset = reinterpret_cast<const int32_t*>(store_column_indexs_)[col_index];
    } else {
      ret = OB_INVALID_DATA;
      STORAGE_LOG(WARN, "Invalid column index bytes, ", K(ret), K(store_column_index_bytes));
    }
    if (OB_SUCC(ret)) {
      pos_ = start_pos_ + column_index_offset;
      if (OB_FAIL(cell_reader.init(buf_ + pos_, row_end_pos_, &allocator))) {
        STORAGE_LOG(WARN, "Failed to init CellReader", K(ret), K(pos_), K(row_end_pos_));
      } else if (OB_FAIL(cell_reader.read_cell(obj))) {
        STORAGE_LOG(WARN, "Failed to read cell", K(ret), K(pos_), K(row_end_pos_));
      }
    }
  }
  return ret;
}

int ObSparseRowReader::read_compact_rowkey(const ObObjMeta* column_types, const int64_t column_count, const char* buf,
    const int64_t row_end_pos, int64_t& pos, ObNewRow& row)
{
  UNUSED(column_types);
  int ret = OB_SUCCESS;
  if (OB_FAIL(setup_row(buf, row_end_pos, pos, -1))) {  // -1: no RowHeader & index/col_id array
    STORAGE_LOG(WARN, "row reader fail to setup", K(ret), K(OB_P(buf)), K(row_end_pos), K(pos));
  } else {
    ObSparseCellReader cell_reader;
    if (OB_FAIL(cell_reader.init(buf_ + pos_, row_end_pos_, &allocator_))) {
      STORAGE_LOG(WARN, "Failed to init CellReader", K(ret), K(pos_), K(row_end_pos_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {  // read column array
      if (OB_FAIL(cell_reader.read_cell(row.cells_[i]))) {
        STORAGE_LOG(WARN, "cell reader fail to read column", K(ret), K(i), K(column_types[i]));
      }
    }
    if (OB_SUCC(ret)) {
      pos = pos_ + cell_reader.get_pos();
      row.count_ = column_count;
    }
  }
  reset();
  return ret;
}

int ObSparseRowReader::compare_meta_rowkey(const common::ObStoreRowkey& rhs,
    const ObColumnMap* column_map,  // ObColumnMap can be null
    const int64_t compare_column_count, const char* buf, const int64_t row_end_pos, const int64_t pos,
    int32_t& cmp_result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rhs.is_valid() ||
                  (NULL != column_map && (!column_map->is_valid() ||
                                             compare_column_count > column_map->get_rowkey_store_count() +
                                                                        column_map->get_multi_version_rowkey_cnt())) ||
                  compare_column_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid row header argument.",
        K(ret),
        K(column_map),
        K(compare_column_count),
        K(rhs),
        K(buf),
        K(row_end_pos),
        K(pos));
  } else {
    if (OB_UNLIKELY(compare_column_count > rhs.get_obj_cnt())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid compare column count", K(ret), K(compare_column_count), K(rhs.get_obj_cnt()));
    } else if (OB_FAIL(setup_row(buf, row_end_pos, pos, 0))) {  // 0: ignore index/col_id array
      STORAGE_LOG(WARN, "row reader fail to setup.", K(ret), K(OB_P(buf)), K(row_end_pos), K(pos));
    } else {
      cmp_result = 0;
      common::ObObj obj;
      ObSparseCellReader cell_reader;
      if (OB_FAIL(cell_reader.init(buf_ + pos_, row_end_pos_, &allocator_))) {  // init cell reader
        STORAGE_LOG(WARN, "Failed to init CellReader", K(ret), K(row_end_pos_));
      }
      for (int64_t i = 0; 0 == cmp_result && OB_SUCC(ret) && i < compare_column_count; ++i) {
        ObObjMeta obj_meta;
        if (OB_FAIL(cell_reader.read_cell(obj))) {
          STORAGE_LOG(WARN, "row reader fail to read column.", K(ret), K(i));
        } else {
          cmp_result = obj.compare(rhs.get_obj_ptr()[i], common::CS_TYPE_INVALID);
        }
      }
    }
  }
  reset();
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
