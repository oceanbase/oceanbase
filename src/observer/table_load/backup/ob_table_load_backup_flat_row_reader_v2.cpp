/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "observer/table_load/backup/ob_table_load_backup_flat_row_reader_v2.h"
#include "share/object/ob_obj_cast.h"
#include "storage/ob_storage_util.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;

ObFlatRowReaderV2::ObFlatRowReaderV2()
  : buf_(NULL),
    row_end_pos_(0),
    start_pos_(0),
    pos_(0),
    row_header_(NULL),
    store_column_indexs_(NULL),
    column_ids_(NULL),
    is_setuped_(false)
{
}
/*
 * FlatRow : ObRowHeaderV2 | Column Array | Column Index Array
 * */


// row is store in flat type (cells array | index array)
int ObFlatRowReaderV2::read_row(
    const char *row_buf,
    const int64_t row_end_pos,
    int64_t pos,
    const ObTableLoadBackupVersion &backup_version,
    const ObIColumnMap *column_map,
    ObIAllocator &allocator,
    ObNewRow &row)
{
  UNUSED(backup_version);
  int ret = OB_SUCCESS;
  // set position and get row header
  if (OB_UNLIKELY(nullptr == row_buf || nullptr == column_map)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(row_buf), KP(column_map));
  } else if (OB_FAIL(setup_row(row_buf, row_end_pos, pos, column_map->get_store_count()))) {
    LOG_WARN("Fail to set up row", KR(ret), K(pos), K(row_end_pos));
  } else if (OB_FAIL(read_flat_row_from_flat_storage(column_map, allocator, row))) {
    LOG_WARN("Fail to read flat row from flat storage", KR(ret), K(column_map));
  }
  return ret;
}

inline int ObFlatRowReaderV2::setup_row(
    const char *buf,
    const int64_t row_end_pos,
    const int64_t pos,
    const int64_t column_index_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_end_pos <= 0 || column_index_count < -1 || pos < 0 || pos >= row_end_pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row reader argument", KR(ret), K(row_end_pos), K(column_index_count));
  } else {
    // set all position
    buf_ = buf;
    row_end_pos_ = row_end_pos;
    start_pos_ = pos;
    pos_ = pos;
    if (column_index_count >= 0) {
      if (OB_FAIL(analyze_row_header(column_index_count))) { // analyze row header
        LOG_WARN("invalid row reader argument.", KR(ret), K(column_index_count));
      } else if (column_index_count > 0) { // get column index array
        store_column_indexs_ = buf_ + row_end_pos_ - row_header_->get_column_index_bytes() * column_index_count;
      } else { // 0 == column_index_count : ignore index array
        store_column_indexs_ = NULL;
      }
    } else { // -1 == column_index_count : no RowHeader and index array
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
 * Row : ObRowHeaderV2 | (ObTransID) | Column Array | Column Index Array
 * */
OB_INLINE int ObFlatRowReaderV2::analyze_row_header(
    const int64_t input_column_cnt)
{
  int ret = OB_SUCCESS;
  int64_t row_header_size = ObRowHeaderV2::get_serialized_size();
  row_header_ = reinterpret_cast<const ObRowHeaderV2*>(buf_ + pos_); // get RowHeader
  if (OB_UNLIKELY(
      input_column_cnt < 0 ||
      pos_ + row_header_size + row_header_->get_column_index_bytes() * input_column_cnt >= row_end_pos_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("invalid row reader argument", KR(ret), K(pos_), K(row_end_pos_), KPC(row_header_));
    row_header_ = NULL;
  } else {
    pos_ += row_header_size; // move forward
  }
  return ret;
}

int ObFlatRowReaderV2::read_flat_row_from_flat_storage(
    const ObIColumnMap *column_map,
    ObIAllocator &allocator,
    ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
      0 == column_map->get_store_count() ||
      0 == column_map->get_request_count() ||
      column_map->get_request_count() > row.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", KR(ret), KPC(column_map), K(row));
  } else if (OB_UNLIKELY(
      NULL == row_header_ ||
      NULL == store_column_indexs_ ||
      NULL == buf_ ||
      pos_ >= row_end_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K(row_header_), K(store_column_indexs_), KP(buf_), K(pos_), K(row_end_pos_));
  } else {
    const int64_t column_cnt = column_map->get_request_count();
    row.count_ = column_cnt; // set row count
    const int8_t column_index_bytes = row_header_->get_column_index_bytes();
    int64_t cur_read_idx = 0;
    int64_t column_index_offset = 0;
    int i = 0;
    if (column_map->get_seq_read_column_count() > 0) {
      if (OB_FAIL(sequence_read_flat_column(column_map, allocator, row))) {
        LOG_WARN("seuqence read flat column failed", KR(ret), K(column_map));
      } else {
        i += column_map->get_seq_read_column_count();
      }
    }
    for ( ; OB_SUCC(ret) && i < column_cnt; ++i) { // loop the ColumnIndex array
      const ObIColumnIndexItem *column_index_item = column_map->get_column_index(i);
      if (column_index_item->get_store_index() < 0) { // not exists
        row.cells_[i].set_nop_value();
      } else {
        if (cur_read_idx == column_index_item->get_store_index()) { //sequence read at pos
          if (OB_FAIL(read_obj(column_index_item->get_request_column_type(), allocator, row.cells_[i]))) {
            LOG_WARN("Fail to read column, ", KR(ret), K(i), K_(pos));
            for (int j = 0; j < i; ++j) {
              LOG_WARN("Fail to read column", KR(ret), K(j), K(row.cells_[j]));
            }
          } else {
            cur_read_idx++;
          }
        } else { //need project
          if (2 == column_index_bytes) { // get offset
            column_index_offset =
                reinterpret_cast<const int16_t *>(store_column_indexs_)[column_index_item->get_store_index()];
          } else if (1 == column_index_bytes) {
            column_index_offset =
                reinterpret_cast<const int8_t *>(store_column_indexs_)[column_index_item->get_store_index()];
          } else if (4 == column_index_bytes) {
            column_index_offset =
                reinterpret_cast<const int32_t *>(store_column_indexs_)[column_index_item->get_store_index()];
          } else {
            ret = OB_INVALID_DATA;
            LOG_WARN("Invalid column index bytes, ", KR(ret), K(column_index_bytes));
          }
          if (OB_SUCC(ret)) { // read an obj
            pos_ = start_pos_ + column_index_offset;
            if (OB_FAIL(read_obj(column_index_item->get_request_column_type(), allocator, row.cells_[i]))) {
              LOG_WARN("Fail to read column, ", KR(ret), K(i), K_(pos));
              for (int j = 0; j < i; ++j) {
                LOG_WARN("Fail to read column, ", KR(ret), K(j), K(row.cells_[j]));
              }
            } else { // set sequence read pos
              cur_read_idx = column_index_item->get_store_index() + 1;
            }
          }
        }
      }
    } // end for
  }
  return ret;
}

int ObFlatRowReaderV2::sequence_read_flat_column(
    const ObIColumnMap *column_map,
    ObIAllocator &allocator,
    ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_setuped_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("should call setup func first", KR(ret));
  } else if (OB_UNLIKELY(column_map->get_seq_read_column_count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(column_map->get_seq_read_column_count()));
  } else if (OB_UNLIKELY(NULL == row_header_ || NULL == store_column_indexs_ || NULL == buf_ || pos_ >= row_end_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K(row_header_), K(store_column_indexs_), K(buf_), K(pos_), K(row_end_pos_));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < column_map->get_seq_read_column_count(); ++i) {
      const ObIColumnIndexItem *column_index_item = column_map->get_column_index(i);
      if (OB_FAIL(read_obj(column_index_item->get_request_column_type(), allocator, row.cells_[i]))) {
        LOG_WARN("Fail to read column, ", KR(ret), K(i), K_(pos));
      }
    }
  }
  return ret;
}

int ObFlatRowReaderV2::read_obj(
    const ObObjMeta &src_meta,
    ObIAllocator &allocator,
    ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_setuped_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("should call setup func first", KR(ret));
  } else if (OB_UNLIKELY(NULL == buf_ || pos_ >= row_end_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K(buf_), K(pos_), K(row_end_pos_));
  } else {
    const ObObj *obj_ptr = NULL;
    const ObObjTypeClass type_class = src_meta.get_type_class();
    const ObObjType column_type = src_meta.get_type();
    bool need_cast = true;
    bool empty_str = false;
    const ObStoreMeta *meta = read<ObStoreMeta>(buf_, pos_);
    obj.set_meta_type(src_meta);
    switch (meta->type_) {
    case ObNullStoreType: {
      obj.set_null();
      break;
    }
    case ObIntStoreType: {
      switch (meta->attr_) {
      case 0:    //value is 1 byte
        obj.set_tinyint(*read<int8_t>(buf_, pos_));
        break;
      case 1:    //value is 2 bytes
        obj.set_smallint(*read<int16_t>(buf_, pos_));
        break;
      case 2:    //value is 4 bytes
        obj.set_int32(*read<int32_t>(buf_, pos_));
        break;
      case 3:    //value is 8 bytes
        obj.set_int(*read<int64_t>(buf_, pos_));
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support attr, ", KR(ret), K(meta->attr_));
      }
      if (ObIntTC == type_class && src_meta.get_type() >= obj.get_type()) {
        need_cast = false;
      } else if (ObUIntTC == type_class && obj.get_int() >= 0
          && static_cast<uint64_t>(obj.get_int()) <= UINT_MAX_VAL[src_meta.get_type()]) {
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
      obj.set_number(tmp_desc, 0 == tmp_desc.len_ ? NULL : (uint32_t*) (buf_ + pos_));
      pos_ += sizeof(uint32_t) * tmp_desc.len_;
      if (ObNumberTC == type_class && !obj.is_negative_number()) {
        need_cast = false;
      }
      break;
    }
    case ObTimestampStoreType: {
      switch (meta->attr_) {
      case 0:    //ObDateType
        obj.set_date(*read<int32_t>(buf_, pos_));
        break;
      case 1:    //ObDateTimeType
        obj.set_datetime(*read<int64_t>(buf_, pos_));
        break;
      case 2:    //ObTimestampType
        obj.set_timestamp(*read<int64_t>(buf_, pos_));
        break;
      case 3:    //ObTimeType
        obj.set_time(*read<int64_t>(buf_, pos_));
        break;
      case 4:    //ObYearType
        obj.set_year(*read<uint8_t>(buf_, pos_));
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support attr, ", KR(ret), K(meta->attr_));
      }
      break;
    }
    case ObTimestampTZStoreType: {
      const int64_t *time_us = NULL;
      const uint32_t *time_ctx_desc = NULL;
      const uint16_t *time_desc = NULL;
      const int64_t length = ObObj::get_otimestamp_store_size(
          common::OTMAT_TIMESTAMP_TZ == meta->attr_);
      if (OB_UNLIKELY(pos_ + length > row_end_pos_)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buf is not large", KR(ret), K(pos_), K(length), K(row_end_pos_));
      } else if (OB_ISNULL((time_us = read<int64_t>(buf_, pos_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("time_us is not expected", KR(ret), K(pos_));
      } else if (common::OTMAT_TIMESTAMP_TZ == meta->attr_ && OB_ISNULL((time_ctx_desc = read<uint32_t>(buf_, pos_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("time_ctx_desc is not expected", KR(ret), K(pos_));
      } else if (common::OTMAT_TIMESTAMP_TZ != meta->attr_ && OB_ISNULL((time_desc = read<uint16_t>(buf_, pos_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("time_desc is not expected", KR(ret), K(pos_));
      } else {
        switch (meta->attr_) {
        case common::OTMAT_TIMESTAMP_TZ:    //ObTimestampTZType
          obj.set_timestamp_tz(*time_us, *time_ctx_desc);
          break;
        case common::OTMAT_TIMESTAMP_LTZ:    //ObTimestampLTZType
          obj.set_timestamp_ltz(*time_us, *time_desc);
          break;
        case common::OTMAT_TIMESTAMP_NANO:    //ObTimestampNanoType
          obj.set_timestamp_nano(*time_us, *time_desc);
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support attr, ", KR(ret), K(meta->attr_));
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
        LOG_WARN("row reader only suport utf8 character set.", KR(ret), K(meta->attr_));
      }
      if (OB_SUCC(ret)) {
        const uint32_t *len = read<uint32_t>(buf_, pos_);
        if (OB_UNLIKELY(pos_ + *len > row_end_pos_)) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("buf is not large", KR(ret), K(pos_), K(*len), K_(row_end_pos));
        } else {
          value.assign_ptr((char*) (buf_ + pos_), *len);
          pos_ += *len;
          obj.set_char(value);
        }
      }
      empty_str = 0 == obj.get_val_len();
      if ((ob_is_string_tc(src_meta.get_type()) || (src_meta.is_raw())) && obj.get_collation_type() == src_meta.get_collation_type()) {
        need_cast = false;
      }
      break;
    }
    case ObTextStoreType: {
      if (OB_FAIL(read_text_store(*meta, allocator, obj))) {
        LOG_WARN("fail to read text store", KR(ret));
      } else if (src_meta.is_outrow() || obj.get_collation_type() == src_meta.get_collation_type()) {
        need_cast = false;
      }
      empty_str = 0 == obj.get_val_len();
      break;
    }
    case ObJsonStoreType: {
      if (OB_FAIL(read_json_store(*meta, allocator, obj))) {
        LOG_WARN("fail to read json store", KR(ret));
      } else if (src_meta.is_json_outrow()) {
        need_cast = false;
      }
      empty_str = 0 == obj.get_val_len();
      break;
    }
    case ObGeometryStoreType: {
      if (OB_FAIL(read_text_store(*meta, allocator, obj))) {
        LOG_WARN("fail to read geometry store", KR(ret));
      } else if (src_meta.is_geometry_outrow()) {
        need_cast = false;
      }
      empty_str = 0 == obj.get_val_len();
      break;
    }
    case ObHexStoreType: {
      ObString value;
      const uint32_t *len = read<uint32_t>(buf_, pos_);
      if (OB_UNLIKELY(pos_ + *len > row_end_pos_)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buf not enough, ", KR(ret), K(pos_), K(*len), K(row_end_pos_));
      } else {
        value.assign_ptr((char*) (buf_ + pos_), *len);
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
        LOG_WARN("the attr type is not supported.", KR(ret), K(meta->attr_));
      }
      break;
    }
    case ObIntervalYMStoreType: {
      if (OB_UNLIKELY(pos_ + ObIntervalYMValue::get_store_size() > row_end_pos_)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buf not enough, ", KR(ret), K(pos_), K(row_end_pos_));
      } else {
        const int64_t year_value = *read<int64_t>(buf_, pos_);
        obj.set_interval_ym(ObIntervalYMValue(year_value));
      }
      break;
    }
    case ObIntervalDSStoreType: {
      if (OB_UNLIKELY(pos_ + ObIntervalDSValue::get_store_size() > row_end_pos_)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buf not enough, ", KR(ret), K(pos_), K(row_end_pos_));
      } else {
        const int64_t second_value = *read<int64_t>(buf_, pos_);
        const int32_t fs_value = *read<int32_t>(buf_, pos_);
        obj.set_interval_ds(ObIntervalDSValue(second_value, fs_value));
      }
      break;
    }
    case ObRowIDStoreType: {
      const uint32_t *len = read<uint32_t>(buf_, pos_);
      if (OB_UNLIKELY(pos_ + *len > row_end_pos_)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buf not enough, ", KR(ret), K(pos_), K(*len), K(row_end_pos_));
      } else {
        obj.set_urowid((char*)(buf_ + pos_), *len);
        pos_ += *len;
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("the attr type is not supported.", KR(ret), K(meta), K(src_meta));
    }
    break;
    }

    if (OB_SUCC(ret)) {
      if (ObNullType == obj.get_type() || ObExtendType == obj.get_type()) {
        //ignore src_meta type, do nothing, just return obj
      } else if (((obj.get_type() == src_meta.get_type() || src_meta.is_raw()) && !need_cast) || empty_str) {    //just change the type
        // extra bypaas path for raw, or data will be wrong
        obj.set_type(src_meta.get_type());
        if (empty_str && (ObTextTC == type_class || ObJsonTC == type_class || ObGeometryTC == type_class)) {
          obj.set_inrow();
        }
      } else {
        //not support data alteration
        ObObj ori_obj = obj;
        int64_t cm_mode = CM_NONE;
        ObObjTypeClass ori_type_calss = ori_obj.get_type_class();
        //int to uint not check range bug:
        if (ObIntTC == ori_type_calss && ObUIntTC == type_class) {
          obj.set_uint(src_meta.get_type(), static_cast<uint64_t>(ori_obj.get_int()));
        } else if (ObIntTC == ori_type_calss && ObBitTC == type_class) {
          obj.set_bit(static_cast<uint64_t>(ori_obj.get_int()));
        } else if (ObIntTC == ori_type_calss && ObEnumType == column_type) {
          obj.set_enum(static_cast<uint64_t>(ori_obj.get_int()));
        } else if (ObIntTC == ori_type_calss && ObSetType == column_type) {
          obj.set_set(static_cast<uint64_t>(ori_obj.get_int()));
        } else {
          ObCastCtx cast_ctx(&allocator, NULL, cm_mode, src_meta.get_collation_type());
          if (OB_FAIL(ObObjCaster::to_type(src_meta.get_type(), cast_ctx, ori_obj, obj))) {
            LOG_WARN("fail to cast obj", KR(ret), K(ori_obj), K(obj), K(obj_ptr), K(ori_obj.get_type()),
                K(ob_obj_type_str(ori_obj.get_type())), K(src_meta.get_type()), K(ob_obj_type_str(src_meta.get_type())));
          }
        }
      }
    }
  }
  return ret;
}

int ObFlatRowReaderV2::read_text_store(
    const ObStoreMeta &store_meta,
    common::ObIAllocator &allocator,
    ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (STORE_WITHOUT_COLLATION == store_meta.attr_) {
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  } else if (STORE_WITH_COLLATION == store_meta.attr_) {
    obj.set_collation_type(static_cast<ObCollationType>(*read<uint8_t>(buf_, pos_)));
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("row reader only suport utf8 character set", KR(ret), K(store_meta.attr_));
  }
  if (OB_SUCC(ret)) {
    const uint8_t *type = read<uint8_t>(buf_, pos_);
    const uint8_t *scale = read<uint8_t>(buf_, pos_);
    const uint8_t *version = read<uint8_t>(buf_, pos_);
    const ObObjType store_type = static_cast<ObObjType>(*type);
    ObBackupLobScale lob_scale(*scale);
    if (OB_UNLIKELY(STORE_TEXT_STORE_VERSION != *version)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LOB] unexpected text store version", KR(ret), K(*version));
    } else {
      if (lob_scale.is_in_row()) {
        const uint32_t *len = read<uint32_t>(buf_, pos_);
        if(pos_ + *len > row_end_pos_){
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("buf is not large", KR(ret), K(pos_), K(*len), K(row_end_pos_));
        } else {
          obj.set_lob_value(store_type, (char*)(buf_ + pos_), *len);
          pos_ += *len;
        }
      } else if (lob_scale.is_out_row()) {
        int64_t tmp_pos = pos_;
        void *buf = NULL;
        ObBackupLobData *value = NULL;
        if (OB_ISNULL(buf = allocator.alloc(sizeof(ObBackupLobData)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for ObBackupLobData", KR(ret));
        } else {
          value = new (buf) ObBackupLobData();
          if (OB_FAIL(value->deserialize(buf_, row_end_pos_, pos_))) {
            LOG_WARN("fail to deserialize lob data", KR(ret));
          } else {
            obj.set_lob_value(store_type, static_cast<const char *>(buf), sizeof(ObBackupLobData));
            obj.set_outrow();
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, invalid obj type", KR(ret), K(obj));
      }
    }
  }
  return ret;
}

int ObFlatRowReaderV2::read_json_store(
    const ObStoreMeta &store_meta,
    common::ObIAllocator &allocator,
    ObObj &obj) {
  return read_text_store(store_meta, allocator, obj);
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
