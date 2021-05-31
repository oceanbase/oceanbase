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
#include "ob_row_writer.h"
#include "common/row/ob_row.h"
#include "common/cell/ob_cell_writer.h"
#include "ob_block_sstable_struct.h"
#include "storage/ob_i_store.h"
#include "ob_sparse_cell_writer.h"

using namespace oceanbase;
using namespace common;
using namespace storage;
using namespace blocksstable;
#define WRITE_META(meta_type, meta_attr)                               \
  {                                                                    \
    if (pos_ + sizeof(ObStoreMeta) > buf_size_) {                      \
      ret = OB_BUF_NOT_ENOUGH;                                         \
    } else {                                                           \
      ObStoreMeta* meta = reinterpret_cast<ObStoreMeta*>(buf_ + pos_); \
      meta->type_ = static_cast<uint8_t>(meta_type);                   \
      meta->attr_ = static_cast<uint8_t>(meta_attr);                   \
      pos_ += sizeof(ObStoreMeta);                                     \
    }                                                                  \
  }

#define WRITE_COMMON(meta_type, meta_attr, store_type, value)          \
  {                                                                    \
    if (pos_ + sizeof(ObStoreMeta) + sizeof(store_type) > buf_size_) { \
      ret = OB_BUF_NOT_ENOUGH;                                         \
    } else {                                                           \
      ObStoreMeta* meta = reinterpret_cast<ObStoreMeta*>(buf_ + pos_); \
      meta->type_ = meta_type;                                         \
      meta->attr_ = meta_attr;                                         \
      pos_ += sizeof(ObStoreMeta);                                     \
      *(reinterpret_cast<store_type*>(buf_ + pos_)) = value;           \
      pos_ += sizeof(store_type);                                      \
    }                                                                  \
  }

#define WRITE_NUMBER(TYPE)                    \
  if (OB_FAIL(obj.get_##TYPE(tmp_number_))) { \
  } else {                                    \
    ret = write_number(tmp_number_);          \
  }

int ObRowWriter::write_int(const int64_t value)
{
  int ret = OB_SUCCESS;
  int64_t bytes = 0;

  if (OB_FAIL(get_int_byte(value, bytes))) {
    LOG_WARN("failed to get int byte", K(ret));
  } else if (pos_ + sizeof(ObStoreMeta) + bytes > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    ObStoreMeta* meta = reinterpret_cast<ObStoreMeta*>(buf_ + pos_);
    switch (bytes) {
      case 1: {
        meta->type_ = ObIntStoreType;
        meta->attr_ = 0;
        pos_ += sizeof(ObStoreMeta);
        *(reinterpret_cast<int8_t*>(buf_ + pos_)) = static_cast<int8_t>(value);
        pos_ += bytes;
        break;
      }
      case 2: {
        meta->type_ = ObIntStoreType;
        meta->attr_ = 1;
        pos_ += sizeof(ObStoreMeta);
        *(reinterpret_cast<int16_t*>(buf_ + pos_)) = static_cast<int16_t>(value);
        pos_ += bytes;
        break;
      }
      case 4: {
        meta->type_ = ObIntStoreType;
        meta->attr_ = 2;
        pos_ += sizeof(ObStoreMeta);
        *(reinterpret_cast<int32_t*>(buf_ + pos_)) = static_cast<int32_t>(value);
        pos_ += bytes;
        break;
      }
      case 8: {
        meta->type_ = ObIntStoreType;
        meta->attr_ = 3;
        pos_ += sizeof(ObStoreMeta);
        *(reinterpret_cast<int64_t*>(buf_ + pos_)) = static_cast<int64_t>(value);
        pos_ += bytes;
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("not supported int", K(ret), K(value));
      }
    }
  }

  return ret;
}

int ObRowWriter::write_number(const number::ObNumber& number)
{
  int ret = OB_SUCCESS;

  WRITE_META(ObNumberStoreType, 0);

  if (OB_SUCC(ret)) {
    ret = number.encode(buf_, buf_size_, pos_);
  }
  return ret;
}

int ObRowWriter::write_char(
    const ObObj& obj, const ObDataStoreType& meta_type, const ObString& char_value, const int64_t max_length)
{
  int ret = OB_SUCCESS;
  const uint32_t char_value_len = char_value.length();
  int64_t need_len = sizeof(ObStoreMeta) + sizeof(uint32_t) + char_value_len;
  uint8_t attr = 0;
  if (CS_TYPE_UTF8MB4_GENERAL_CI == obj.get_collation_type()) {
    attr = 0;
  } else {
    attr = 1;
    need_len += sizeof(uint8_t);  // obj.get_collation_type()
  }

  if (OB_SUCC(ret)) {
    if (char_value_len > max_length) {
      ret = OB_SIZE_OVERFLOW;
      STORAGE_LOG(ERROR, "varchar value is overflow.", K(ret), "str_len", char_value_len);
    } else if (pos_ + need_len > buf_size_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      ObStoreMeta* meta = reinterpret_cast<ObStoreMeta*>(buf_ + pos_);
      meta->attr_ = attr;
      meta->type_ = meta_type;
      pos_ += sizeof(ObStoreMeta);

      if (1 == attr) {
        *(reinterpret_cast<uint8_t*>(buf_ + pos_)) = static_cast<uint8_t>(obj.get_collation_type());
        pos_ += sizeof(uint8_t);
      }

      *(reinterpret_cast<uint32_t*>(buf_ + pos_)) = char_value_len;
      pos_ += sizeof(uint32_t);
      MEMCPY(buf_ + pos_, char_value.ptr(), char_value_len);
      pos_ += char_value_len;
    }
  }
  return ret;
}

#define WRITE_EXTEND()                                                                     \
  {                                                                                        \
    if (ObActionFlag::OP_NOP == obj.get_ext()) {                                           \
      WRITE_META(ObExtendStoreType, 0);                                                    \
    } else {                                                                               \
      ret = OB_NOT_SUPPORTED;                                                              \
      STORAGE_LOG(WARN, "unsupported extend type.", "extend_type", obj.get_ext(), K(ret)); \
    }                                                                                      \
  }

ObRowWriter::ObRowWriter() : buf_(NULL), buf_size_(0), start_pos_(0), pos_(0), row_header_(NULL), column_index_count_(0)
{}

ObRowWriter::~ObRowWriter()
{}

ObRowWriter::NumberAllocator::NumberAllocator(char* buf, const int64_t buf_size, int64_t& pos)
    : buf_(buf), buf_size_(buf_size), pos_(pos)
{}

int ObRowWriter::write_oracle_timestamp(const ObOTimestampData& ot_data, const common::ObOTimestampMetaAttrType otmat)
{
  int ret = OB_SUCCESS;
  ObStoreMeta meta;
  meta.type_ = (static_cast<uint8_t>(ObTimestampTZStoreType) & 0x1F);
  meta.attr_ = (static_cast<uint8_t>(otmat) & 0x07);
  if (OB_FAIL(append<ObStoreMeta>(meta))) {
    STORAGE_LOG(WARN, "row writer fail to append ObStoreMeta.", K(ret));
  } else if (OB_FAIL(append<int64_t>(ot_data.time_us_))) {
  } else {
    if (common::OTMAT_TIMESTAMP_TZ == otmat) {
      ret = append<uint32_t>(ot_data.time_ctx_.desc_);
    } else {
      ret = append<uint16_t>(ot_data.time_ctx_.time_desc_);
    }
  }
  return ret;
}

int ObRowWriter::write(
    const ObNewRow& row, char* buf, const int64_t buf_size, const ObRowStoreType row_store_type, int64_t& pos)
{
  int ret = OB_SUCCESS;

  if (row.is_invalid() || NULL == buf || buf_size <= 0 || row_store_type >= MAX_ROW_STORE) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row writer input argument.", K(buf), K(buf_size), K(row), K(row_store_type), K(ret));
  } else if (OB_FAIL(init_common(buf, buf_size, pos))) {
    STORAGE_LOG(WARN, "row writer fail to init common.", K(ret), K(OB_P(buf)), K(buf_size), K(pos));
  } else {
    if (SPARSE_ROW_STORE == row_store_type) {
      if (OB_FAIL(append_sparse_new_row(row))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          STORAGE_LOG(WARN, "row writer fail to append sparse new row.", K(ret), K(row));
        }
      }
    } else if (OB_FAIL(append_new_row(row))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        STORAGE_LOG(WARN, "row writer fail to append new row.", K(ret), K(row));
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos = pos_;
  }
  return ret;
}

int ObRowWriter::write(const int64_t rowkey_column_count, const ObStoreRow& row, char* buf, const int64_t buf_size,
    int64_t& pos, int64_t& rowkey_start_pos, int64_t& rowkey_length, const bool only_row_key)
{
  int ret = OB_SUCCESS;
  if (row.is_sparse_row_) {  // write sparse row
    if (OB_FAIL(write_sparse_row(
            rowkey_column_count, row, buf, buf_size, pos, rowkey_start_pos, rowkey_length, only_row_key))) {
      STORAGE_LOG(WARN, "write sparse row failed", K(ret), K(row), K(OB_P(buf)), K(buf_size));
    }
  } else if (OB_FAIL(write_flat_row(rowkey_column_count,
                 row,
                 buf,
                 buf_size,
                 pos,
                 rowkey_start_pos,
                 rowkey_length,
                 only_row_key))) {  // write flat row
    STORAGE_LOG(WARN, "write flat row failed", K(ret), K(row), K(OB_P(buf)), K(buf_size));
  }
  return ret;
}

int ObRowWriter::write_flat_row(const int64_t rowkey_column_count, const ObStoreRow& row, char* buf,
    const int64_t buf_size, int64_t& pos, int64_t& rowkey_start_pos, int64_t& rowkey_length, const bool only_row_key)
{
  int ret = OB_SUCCESS;
  int64_t tmp_rowkey_start_pos = 0;
  int64_t tmp_rowkey_length = 0;
  if (!row.is_valid() || rowkey_column_count <= 0 || rowkey_column_count > row.row_val_.count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid input argument.", K(buf), K(buf_size), K(row), K(rowkey_column_count), K(ret));
  } else if (OB_FAIL(init_common(buf, buf_size, pos))) {  // will check input param
    STORAGE_LOG(WARN, "row writer fail to init common.", K(ret), K(row), K(OB_P(buf)), K(buf_size), K(pos));
  } else if (OB_FAIL(init_store_row(row, rowkey_column_count))) {
    STORAGE_LOG(WARN, "row writer fail to init store row.", K(ret), K(rowkey_column_count));
  } else if (OB_FAIL(append_row_header(row))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      STORAGE_LOG(WARN, "row writer fail to append row header.", K(ret));
    }
  } else if (OB_FAIL(
                 append_store_row(rowkey_column_count, row, only_row_key, tmp_rowkey_start_pos, tmp_rowkey_length))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      STORAGE_LOG(WARN, "row writer fail to append store row.", K(ret), K(rowkey_column_count));
    }
  } else if (OB_FAIL(append_column_index())) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      STORAGE_LOG(WARN, "row writer fail to append column index.", K(ret));
    }
  } else {
    pos = pos_;
    rowkey_start_pos = tmp_rowkey_start_pos;
    rowkey_length = tmp_rowkey_length;
  }
  return ret;
}

int ObRowWriter::write_sparse_row(const int64_t rowkey_column_count, const ObStoreRow& row, char* buf,
    const int64_t buf_size, int64_t& pos, int64_t& rowkey_start_pos, int64_t& rowkey_length, const bool only_row_key)
{
  int ret = OB_SUCCESS;
  int64_t tmp_rowkey_start_pos = 0;
  int64_t tmp_rowkey_length = 0;
  if (!row.is_valid() || rowkey_column_count <= 0 || rowkey_column_count > row.row_val_.count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid input argument.", KP(buf), K(buf_size), K(row), K(rowkey_column_count), K(ret));
  } else if (OB_FAIL(init_common(buf, buf_size, pos))) {  // will check input param
    STORAGE_LOG(WARN, "row writer fail to init common.", K(ret), K(row), K(OB_P(buf)), K(buf_size), K(pos));
  } else if (OB_FAIL(init_store_row(row, rowkey_column_count))) {
    STORAGE_LOG(WARN, "row writer fail to init store row.", K(ret), K(rowkey_column_count));
  } else if (OB_FAIL(append_row_header(row))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      STORAGE_LOG(WARN, "row writer fail to append row header.", K(ret));
    }
  } else if (OB_FAIL(append_sparse_store_row(
                 rowkey_column_count, row, only_row_key, tmp_rowkey_start_pos, tmp_rowkey_length))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      STORAGE_LOG(WARN, "row writer fail to append store row.", K(ret), K(rowkey_column_count));
    }
  } else if (OB_FAIL(append_column_index())) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      STORAGE_LOG(WARN, "row writer fail to append column index.", K(ret));
    }
  } else if (OB_FAIL(append_column_ids(row))) {  // write column id array
    if (OB_BUF_NOT_ENOUGH != ret) {
      STORAGE_LOG(WARN, "row writer fail to append column id array.", K(ret));
    }
  } else {
    pos = pos_;
    rowkey_start_pos = tmp_rowkey_start_pos;
    rowkey_length = tmp_rowkey_length;
  }
  return ret;
}

int ObRowWriter::write_text_store(const ObObj& obj)
{
  int ret = OB_SUCCESS;

  ObLobScale lob_scale(obj.get_scale());
  ObStoreMeta meta;
  meta.type_ = static_cast<uint8_t>(ObTextStoreType);
  if (CS_TYPE_UTF8MB4_GENERAL_CI == obj.get_collation_type()) {
    meta.attr_ = STORE_WITHOUT_COLLATION;
  } else {
    meta.attr_ = STORE_WITH_COLLATION;
  }
  // defend code, fix the scale
  if (!lob_scale.is_valid()) {
    lob_scale.set_in_row();
    STORAGE_LOG(WARN, "[LOB] unexpected error, invalid lob obj scale", K(ret), K(obj), K(obj.get_scale()));
  }
  if (OB_FAIL(append<ObStoreMeta>(meta))) {
    STORAGE_LOG(WARN, "fail to append meta", K(ret));
  } else if (STORE_WITH_COLLATION == meta.attr_ &&
             OB_FAIL(append<uint8_t>(static_cast<uint8_t>(obj.get_collation_type())))) {
    STORAGE_LOG(WARN, "row writer fail to append collation type.", K(ret));
  } else if (OB_FAIL(append<uint8_t>(static_cast<uint8_t>(obj.get_type())))) {
    STORAGE_LOG(WARN, "fail to append obj type", K(ret));
  } else if (OB_FAIL(append<uint8_t>(static_cast<uint8_t>(lob_scale.get_scale())))) {
    STORAGE_LOG(WARN, "fail to append obj scale", K(ret));
  } else if (OB_FAIL(append<uint8_t>(static_cast<uint8_t>(STORE_TEXT_STORE_VERSION)))) {
    STORAGE_LOG(WARN, "fail to append text store version", K(ret));
  } else if (lob_scale.is_in_row()) {
    ObString char_value;
    if (OB_FAIL(obj.get_string(char_value))) {
      STORAGE_LOG(WARN, "fail to get inrow lob obj value", K(obj), K(ret));
    } else if (char_value.length() > ObAccuracy::MAX_ACCURACY[obj.get_type()].get_length()) {
      ret = OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "text value is overflow.", "length", char_value.length(), K(ret));
    } else if (OB_FAIL(append<uint32_t>(static_cast<uint32_t>(char_value.length())))) {
    } else if (pos_ + char_value.length() >= buf_size_) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buffer is not enough to write lob obj value", K(obj), K(ret));
    } else {
      MEMCPY(buf_ + pos_, char_value.ptr(), char_value.length());
      pos_ += char_value.length();
    }
  } else if (lob_scale.is_out_row()) {
    const ObLobData* value = NULL;
    if (OB_FAIL(obj.get_lob_value(value))) {
      STORAGE_LOG(WARN, "fail to get inrow lob obj value", K(obj), K(ret));
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "error unexpected, lob data must not be NULL", K(ret));
    } else if (value->get_serialize_size() > OB_MAX_LOB_HANDLE_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "lob handle size overflow", K(ret), "value_length", value->get_serialize_size());
    } else if (OB_FAIL(value->serialize(buf_, buf_size_, pos_))) {
      STORAGE_LOG(WARN, "fail to serialize value", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "[LOB] unexpected error, invalid lob obj scale", K(ret), K(obj), K(lob_scale));
  }
  return ret;
}

int ObRowWriter::init_common(char* buf, const int64_t buf_size, const int64_t pos)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_size <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row writer input argument.", K(buf), K(buf_size), K(pos), K(ret));
  } else if (pos >= buf_size) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    buf_ = buf;
    buf_size_ = buf_size;
    start_pos_ = pos;
    pos_ = pos;
    row_header_ = NULL;
    // column_indexes_
    column_index_count_ = 0;
  }
  return ret;
}

int ObRowWriter::init_store_row(const ObStoreRow& row, const int64_t rowkey_column_count)
{
  int ret = OB_SUCCESS;
  if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row writer input argument.", K(row), K(ret));
  } else if (rowkey_column_count <= 0 || rowkey_column_count > row.row_val_.count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row writer input argument.", K(rowkey_column_count), K(row.row_val_.count_), K(ret));
  }
  return ret;
}

int ObRowWriter::append_row_header(const ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  int64_t row_header_size = ObRowHeader::get_serialized_size();
  if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row input argument", K(ret), K(row));
  } else if (pos_ + row_header_size > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "buf is not enough", K(ret), K(pos_), K(buf_size_));
  } else {
    row_header_ = reinterpret_cast<ObRowHeader*>(buf_ + pos_);
    row_header_->set_row_flag(static_cast<int8_t>(row.flag_));
    // row_header_.column_index_bytes_
    row_header_->set_row_dml(row.get_dml_val());
    row_header_->set_reserved8(0);
    row_header_->set_column_count(row.row_val_.count_);
    row_header_->set_row_type_flag(row.row_type_flag_.flag_);
    pos_ += row_header_size;  // move forward
    // TransID
    if (OB_ISNULL(row.trans_id_ptr_)) {
      row_header_->set_version(ObRowHeader::RHV_NO_TRANS_ID);
      STORAGE_LOG(DEBUG, "row don't have TransID", K(row.trans_id_ptr_));
    } else {
      row_header_->set_version(ObRowHeader::RHV_WITH_TRANS_ID);
      if (OB_FAIL(row.trans_id_ptr_->serialize(buf_, buf_size_, pos_))) {  // serialize
        STORAGE_LOG(WARN, "Failed to serialize TransID", K(ret), K(*row.trans_id_ptr_), K(buf_), K(buf_size_), K(pos_));
      } else {
        STORAGE_LOG(DEBUG, "Serialize TransID success", K(*row.trans_id_ptr_), K(pos_));
      }
    }
  }
  return ret;
}

int ObRowWriter::append_sparse_store_row(const int64_t rowkey_column_count, const ObStoreRow& row,
    const bool only_row_key, int64_t& rowkey_start_pos, int64_t& rowkey_length)
{
  int ret = OB_SUCCESS;
  const int64_t end_index = only_row_key ? rowkey_column_count : row.row_val_.count_;
  rowkey_start_pos = pos_;  // record rowkey start position
  ObSparseCellWriter cell_writer;
  if (!row.is_valid() || rowkey_column_count <= 0 || rowkey_column_count > row.row_val_.count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row input argument.", K(row), K(rowkey_column_count), K(ret));
  } else {
    if (OB_FAIL(cell_writer.init(buf_ + pos_, buf_size_, DENSE))) {  // init CellWriter
      STORAGE_LOG(WARN, "Failed to init CellWriter", K(buf_), K(pos_), K(buf_size_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < end_index; ++i) {
      const int64_t column_index = cell_writer.size() + pos_ - start_pos_;  // record offset
      column_indexs_8_[i] = static_cast<int8_t>(column_index);
      column_indexs_16_[i] = static_cast<int16_t>(column_index);
      column_indexs_32_[i] = static_cast<int32_t>(column_index);
      if (OB_FAIL(cell_writer.append(row.row_val_.cells_[i]))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          STORAGE_LOG(WARN, "cell writer fail to append column.", K(ret), K(i), K(row.row_val_.cells_[i]));
        }
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos_ += cell_writer.size();
    column_index_count_ += end_index;
    if (rowkey_column_count < end_index) {
      rowkey_length = column_indexs_32_[rowkey_column_count] + start_pos_ - rowkey_start_pos;
    } else {
      rowkey_length = pos_ - rowkey_start_pos;
    }
    STORAGE_LOG(DEBUG, "success to append row.", K(ret), K(row), K(pos_));
  }
  return ret;
}

int ObRowWriter::append_store_row(const int64_t rowkey_column_count, const ObStoreRow& row, const bool only_row_key,
    int64_t& rowkey_start_pos, int64_t& rowkey_length)
{
  int ret = OB_SUCCESS;
  const int64_t end_index = only_row_key ? rowkey_column_count : row.row_val_.count_;
  rowkey_start_pos = pos_;
  if (!row.is_valid() || rowkey_column_count <= 0 || rowkey_column_count > row.row_val_.count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row input argument.", K(row), K(rowkey_column_count), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < end_index; ++i) {
      const int64_t column_index = pos_ - start_pos_;
      column_indexs_8_[i] = static_cast<int8_t>(column_index);
      column_indexs_16_[i] = static_cast<int16_t>(column_index);
      column_indexs_32_[i] = static_cast<int32_t>(column_index);
      if (OB_FAIL(append_column(row.row_val_.cells_[i]))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          STORAGE_LOG(WARN, "row writer fail to append column.", K(ret), K(i), K(row.row_val_.cells_[i]));
        }
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    column_index_count_ += end_index;
    if (rowkey_column_count < end_index) {
      rowkey_length = column_indexs_32_[rowkey_column_count] + start_pos_ - rowkey_start_pos;
    } else {
      rowkey_length = pos_ - rowkey_start_pos;
    }
  }
  return ret;
}

int ObRowWriter::append_new_row(const ObNewRow& row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < row.count_; ++i) {
    if (OB_FAIL(append_column(row.cells_[i]))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        STORAGE_LOG(WARN, "row writer fail to append column.", K(ret), K(i), K(row.cells_[i]));
      }
    }
  }
  return ret;
}

int ObRowWriter::append_sparse_new_row(const ObNewRow& row)
{
  int ret = OB_SUCCESS;
  ObSparseCellWriter cell_writer;
  if (OB_FAIL(cell_writer.init(buf_ + pos_, buf_size_, DENSE))) {
    STORAGE_LOG(WARN, "Failed to init CellWriter", K(buf_), K(pos_), K(buf_size_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row.count_; ++i) {
    if (OB_FAIL(cell_writer.append(row.cells_[i]))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        STORAGE_LOG(WARN, "cell writer fail to append column.", K(ret), K(i), K(row.cells_[i]));
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos_ += cell_writer.size();
  }
  return ret;
}

int ObRowWriter::append_column_index()
{
  int ret = OB_SUCCESS;
  int64_t column_index_bytes = 0;
  if (OB_FAIL(get_int_byte(column_indexs_32_[column_index_count_ - 1], column_index_bytes))) {
    STORAGE_LOG(WARN,
        "fail to get column_index_bytes, ",
        K(ret),
        "column_index_value",
        column_indexs_32_[column_index_count_ - 1]);
  } else if (1 == column_index_bytes) {
    const int64_t copy_size = sizeof(int8_t) * column_index_count_;
    if (pos_ + copy_size > buf_size_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf_ + pos_, column_indexs_8_, copy_size);
      pos_ += copy_size;
    }
  } else if (2 == column_index_bytes) {
    const int64_t copy_size = sizeof(int16_t) * column_index_count_;
    if (pos_ + copy_size > buf_size_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf_ + pos_, column_indexs_16_, copy_size);
      pos_ += copy_size;
    }
  } else if (4 == column_index_bytes) {
    const int64_t copy_size = sizeof(int32_t) * column_index_count_;
    if (pos_ + copy_size > buf_size_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf_ + pos_, column_indexs_32_, copy_size);
      pos_ += copy_size;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unsupported column index bytes.", K(ret), K(column_index_bytes));
  }

  row_header_->set_column_index_bytes(static_cast<int8_t>(column_index_bytes));
  return ret;
}

int ObRowWriter::append_column_ids(const storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  const int64_t copy_size = sizeof(uint16_t) * column_index_count_;
  if (pos_ + copy_size > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    MEMCPY(buf_ + pos_, row.column_ids_, copy_size);
    pos_ += copy_size;
  }
  return ret;
}

template <class T>
int ObRowWriter::append(const T& value)
{
  int ret = OB_SUCCESS;
  if (pos_ + static_cast<int64_t>(sizeof(T)) > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *(reinterpret_cast<T*>(buf_ + pos_)) = value;
    pos_ += sizeof(T);
  }
  return ret;
}

inline int ObRowWriter::get_int_byte(const int64_t int_value, int64_t& bytes) const
{
  int ret = OB_SUCCESS;
  if (int_value >= -128 && int_value <= 127) {
    bytes = 1;
  } else if (int_value >= static_cast<int16_t>(0x8000) && int_value <= static_cast<int16_t>(0x7FFF)) {
    bytes = 2;
  } else if (int_value >= static_cast<int32_t>(0x80000000) && int_value <= static_cast<int32_t>(0x7FFFFFFF)) {
    bytes = 4;
  } else {
    bytes = 8;
  }

  return ret;
}

int ObRowWriter::append_column(const ObObj& obj)
{
  int ret = OB_SUCCESS;
  switch (obj.get_type()) {
    case ObNullType:
      WRITE_META(ObNullStoreType, 0);
      break;
    case ObTinyIntType:
      ret = write_int(obj.get_tinyint());
      break;
    case ObSmallIntType:
      ret = write_int(obj.get_smallint());
      break;
    case ObMediumIntType:
      ret = write_int(obj.get_mediumint());
      break;
    case ObInt32Type:
      ret = write_int(obj.get_int32());
      break;
    case ObIntType:
      ret = write_int(obj.get_int());
      break;
    case ObUTinyIntType:
      ret = write_int(obj.get_utinyint());
      break;
    case ObUSmallIntType:
      ret = write_int(obj.get_usmallint());
      break;
    case ObUMediumIntType:
      ret = write_int(obj.get_umediumint());
      break;
    case ObUInt32Type:
      ret = write_int(obj.get_uint32());
      break;
    case ObUInt64Type:
      ret = write_int(obj.get_uint64());
      break;
    case ObFloatType:
      WRITE_COMMON(ObFloatStoreType, 0, float, obj.get_float());
      break;
    case ObDoubleType:
      WRITE_COMMON(ObDoubleStoreType, 0, double, obj.get_double());
      break;
    case ObUFloatType:
      WRITE_COMMON(ObFloatStoreType, 0, float, obj.get_ufloat());
      break;
    case ObUDoubleType:
      WRITE_COMMON(ObDoubleStoreType, 0, double, obj.get_udouble());
      break;
    case ObNumberType:
      WRITE_NUMBER(number);
      break;
    case ObUNumberType:
      WRITE_NUMBER(unumber);
      break;
    case ObDateType:
      WRITE_COMMON(ObTimestampStoreType, 0, int32_t, obj.get_date());
      break;
    case ObDateTimeType:
      WRITE_COMMON(ObTimestampStoreType, 1, int64_t, obj.get_datetime());
      break;
    case ObTimestampType:
      WRITE_COMMON(ObTimestampStoreType, 2, int64_t, obj.get_timestamp());
      break;
    case ObTimeType:
      WRITE_COMMON(ObTimestampStoreType, 3, int64_t, obj.get_time());
      break;
    case ObYearType:
      WRITE_COMMON(ObTimestampStoreType, 4, uint8_t, obj.get_year());
      break;
    case ObVarcharType:
      ret = write_char(obj, ObCharStoreType, obj.get_varchar(), OB_MAX_VARCHAR_LENGTH);
      break;
    case ObCharType:
      ret = write_char(obj, ObCharStoreType, obj.get_string(), OB_MAX_VARCHAR_LENGTH);
      break;
    case ObRawType:
      ret = write_char(obj, ObCharStoreType, obj.get_raw(), OB_MAX_VARCHAR_LENGTH);
      break;
    case ObNVarchar2Type:
      ret = write_char(obj, ObCharStoreType, obj.get_nvarchar2(), OB_MAX_VARCHAR_LENGTH);
      break;
    case ObNCharType:
      ret = write_char(obj, ObCharStoreType, obj.get_nchar(), OB_MAX_VARCHAR_LENGTH);
      break;
    case ObHexStringType: {
      ObStoreMeta meta;
      meta.type_ = static_cast<uint8_t>(ObHexStoreType);
      meta.attr_ = 0;
      ObString char_value;
      if (OB_FAIL(obj.get_hex_string(char_value))) {
        STORAGE_LOG(WARN, "failed to get hex string.", K(ret));
      } else if (char_value.length() > OB_MAX_VARCHAR_LENGTH) {
        ret = OB_SIZE_OVERFLOW;
        STORAGE_LOG(WARN, "varchar value is overflow.", "length", char_value.length(), K(ret));
      } else if (OB_FAIL(append<ObStoreMeta>(meta))) {
      } else if (OB_FAIL(append<uint32_t>(static_cast<uint32_t>(char_value.length())))) {
      } else if (pos_ + char_value.length() >= buf_size_) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        MEMCPY(buf_ + pos_, char_value.ptr(), char_value.length());
        pos_ += char_value.length();
      }
      break;
    }
    case ObExtendType:
      WRITE_EXTEND();
      break;
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1470) {
        ret = write_char(
            obj, ObCharStoreType, obj.get_string(), ObAccuracy::MAX_ACCURACY_OLD[obj.get_type()].get_length());
      } else {
        ret = write_text_store(obj);
      }
      break;
    }
    case ObBitType:
      ret = write_int(obj.get_bit());
      break;
    case ObEnumType:
      ret = write_int(obj.get_enum());
      break;
    case ObSetType:
      ret = write_int(obj.get_set());
      break;
    case ObTimestampTZType:
      ret = write_oracle_timestamp(obj.get_otimestamp_value(), common::OTMAT_TIMESTAMP_TZ);
      break;
    case ObTimestampLTZType:
      ret = write_oracle_timestamp(obj.get_otimestamp_value(), common::OTMAT_TIMESTAMP_LTZ);
      break;
    case ObTimestampNanoType:
      ret = write_oracle_timestamp(obj.get_otimestamp_value(), common::OTMAT_TIMESTAMP_NANO);
      break;
    case ObIntervalYMType: {
      ObStoreMeta meta;
      ObIntervalYMValue value = obj.get_interval_ym();
      WRITE_META(static_cast<uint8_t>(ObIntervalYMStoreType), 0);
      if (OB_SUCC(ret)) {
        ret = append<int64_t>(value.nmonth_);
      }
      break;
    }
    case ObIntervalDSType: {
      ObStoreMeta meta;
      ObIntervalDSValue value = obj.get_interval_ds();
      WRITE_META(static_cast<uint8_t>(ObIntervalDSStoreType), 0);
      if (OB_SUCC(ret)) {
        ret = append<int64_t>(value.nsecond_);
      }
      if (OB_SUCC(ret)) {
        ret = append<int32_t>(value.fractional_second_);
      }
      break;
    }
    case ObURowIDType: {
      ObStoreMeta meta;
      WRITE_META(static_cast<uint8_t>(ObRowIDStoreType), 0);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(append<uint32_t>(static_cast<uint32_t>(obj.get_string_len())))) {
          STORAGE_LOG(WARN, "failed to append str len", K(ret));
        } else {
          MEMCPY(buf_ + pos_, obj.get_string_ptr(), obj.get_string_len());
          pos_ += obj.get_string_len();
        }
      }
      break;
    }
    case ObNumberFloatType: {
      WRITE_NUMBER(number_float);
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "row writer don't support the data type.", K(obj.get_type()), K(ret));
  }
  return ret;
}
