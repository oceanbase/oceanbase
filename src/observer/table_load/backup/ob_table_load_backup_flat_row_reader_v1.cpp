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
#include "observer/table_load/backup/ob_table_load_backup_flat_row_reader_v1.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;

#define READ_STORE(origin_obj, obj_set_fun, medium_type) \
  { \
    origin_obj.obj_set_fun(*read<medium_type>()); \
  }

template<class T>
inline const T *ObFlatRowReaderV1::read()
{
  const T *ptr = reinterpret_cast<const T*>(buf_ + pos_);
  pos_ += sizeof(T);
  return ptr;
}

int ObFlatRowReaderV1::setup_row(
    const char *buf,
    const int64_t row_end_pos,
    const int64_t pos,
    const ObTableLoadBackupVersion &backup_version,
    const int64_t column_index_count)
{
  int ret = OB_SUCCESS;
  int64_t header_size = 0;
  int64_t column_index_bytes = 0;
  if (backup_version == ObTableLoadBackupVersion::V_1_4) {
    header_size = sizeof(ObRowHeaderV1Dummy);
  } else {
    header_size = sizeof(ObRowHeaderV2);
  }
  if (OB_UNLIKELY(row_end_pos <= 0 || column_index_count < -1 || pos < 0 || (column_index_count >= 0 && pos + header_size >= row_end_pos))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(OB_P(buf)), K(row_end_pos), K(column_index_count), K(pos), K(header_size), K(row_end_pos));
  } else {
    backup_version_ = backup_version;
    buf_ = buf;
    row_end_pos_ = row_end_pos;
    start_pos_ = pos;

    if (column_index_count >= 0) {
      if (backup_version == ObTableLoadBackupVersion::V_1_4) {
        row_header_v1_ = reinterpret_cast<const ObRowHeaderV1*>(buf_ + pos);
        column_index_bytes_ = row_header_v1_->get_column_index_bytes();
        if (row_header_v1_->get_version() > 0) {
          header_size = sizeof(ObRowHeaderV1);
        }
      } else {
        row_header_v2_ = reinterpret_cast<const ObRowHeaderV2*>(buf_ + pos);
        column_index_bytes_ = row_header_v2_->get_column_index_bytes();
      }
      pos_ = pos + header_size;
      if (OB_UNLIKELY(pos + header_size + column_index_bytes_ * column_index_count >= row_end_pos)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("invalid argument", KR(ret), K(pos), K(header_size), K(column_index_bytes_ * column_index_count), K(row_end_pos));
      } else if (column_index_count > 0) {
        store_column_indexs_ = buf_ + row_end_pos_ - column_index_bytes_ * column_index_count;
      } else {
        store_column_indexs_ = NULL;
      }
    } else {
      pos_ = pos;
      store_column_indexs_ = NULL;
    }
  }
  return ret;
}

int ObFlatRowReaderV1::read_text_store(
    const ObStoreMeta &store_meta,
    common::ObIAllocator &allocator,
    ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (STORE_WITHOUT_COLLATION == store_meta.attr_) {
    obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  } else if (STORE_WITH_COLLATION == store_meta.attr_) {
    obj.set_collation_type(static_cast<ObCollationType>(*read<uint8_t>()));
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("row reader only suport utf8 character set", KR(ret), K(store_meta.attr_));
  }
  if (OB_SUCC(ret)) {
    const uint8_t *type = read<uint8_t>();
    const uint8_t *scale = read<uint8_t>();
    const uint8_t *version = read<uint8_t>();
    const ObObjType store_type = static_cast<ObObjType>(*type);
    ObBackupLobScale lob_scale(*scale);
    if (OB_UNLIKELY(STORE_TEXT_STORE_VERSION != *version)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LOB] unexpected text store version", KR(ret), K(*version));
    } else {
      if (lob_scale.is_in_row()) {
        const uint32_t *len = read<uint32_t>();
        if(pos_ + *len > row_end_pos_){
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("buf is not large", KR(ret), K(pos_), K(*len), K(row_end_pos_));
        } else {
          obj.set_lob_value(store_type, (char*)(buf_ + pos_), *len);
          pos_ += *len;
        }
      } else if (lob_scale.is_out_row()) {
        if (backup_version_ == ObTableLoadBackupVersion::V_1_4) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected lob_scale", KR(ret), K(lob_scale));
        } else {
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
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, invalid obj type", KR(ret), K(obj));
      }
    }
  }
  return ret;
}

int ObFlatRowReaderV1::read_column_no_meta(
    const ObObjMeta &src_meta,
    ObIAllocator &allocator,
    ObObj &obj)
{
  int ret = OB_SUCCESS;
  const ObStoreMeta *meta = read<ObStoreMeta>();
  if (OB_UNLIKELY(obj.is_null() || obj.is_nop_value())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("obj can not be null or nop here!!!", KR(ret), K(obj), K(src_meta));
  } else {
    switch (meta->type_) {
      case ObNullStoreType: {
        obj.set_null();
        break;
      }
      case ObIntStoreType: {
        switch (meta->attr_) {
          case 0://value is 1 byte
            READ_STORE(obj, set_tinyint_value, int8_t);
            break;
          case 1://value is 2 bytes
            READ_STORE(obj, set_smallint_value, int16_t);
            break;
          case 2://value is 4 bytes
            READ_STORE(obj, set_int32_value, int32_t);
            break;
          case 3://value is 8 bytes
            READ_STORE(obj, set_int_value, int64_t);
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support attr", KR(ret), K(meta->attr_));
        }
        break;
      }
      case ObFloatStoreType: {
        READ_STORE(obj, set_float_value, float);
        break;
      }
      case ObDoubleStoreType: {
        READ_STORE(obj, set_double_value, double);
        break;
      }
      case ObNumberStoreType: {
        number::ObNumber::Desc tmp_desc;
        tmp_desc.desc_ = *read<uint32_t>();
        tmp_desc.reserved_ = tmp_desc.len_;
        obj.set_number_value(tmp_desc, 0 == tmp_desc.len_ ? NULL : (uint32_t*)(buf_ + pos_));
        pos_ += sizeof(uint32_t) * tmp_desc.len_;
        break;
      }
      case ObTimestampStoreType: {
        switch (meta->attr_) {
          case 0://ObDateType
            READ_STORE(obj, set_date_value, int32_t);
            break;
          case 1://ObDateTimeType
            READ_STORE(obj, set_datetime_value, int64_t);
            break;
          case 2://ObTimestampType
            READ_STORE(obj, set_timestamp_value, int64_t);
            break;
          case 3://ObTimeType
            READ_STORE(obj, set_time_value, int64_t);
            break;
          case 4://ObYearType
            READ_STORE(obj, set_year_value, uint8_t);
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support attr", KR(ret), K(meta->attr_));
        }
        break;
      }
      case ObTimestampTZStoreType: {
        const int64_t *time_us = NULL;
        const uint32_t *time_ctx_desc = NULL;
        const uint16_t *time_desc = NULL;
        const int64_t length = ObObj::get_otimestamp_store_size(common::OTMAT_TIMESTAMP_TZ == meta->attr_);
        if(OB_UNLIKELY(pos_+ length > row_end_pos_)) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "buf is not large", K(ret), K(pos_), K(length), K(row_end_pos_));
        } else if (OB_ISNULL((time_us = read<int64_t>()))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "time_us is not expected", K(ret), K(pos_));
        } else if (common::OTMAT_TIMESTAMP_TZ == meta->attr_ && OB_ISNULL((time_ctx_desc = read<uint32_t>()))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "time_ctx_desc is not expected", K(ret), K(pos_));
        } else if (common::OTMAT_TIMESTAMP_TZ != meta->attr_ && OB_ISNULL((time_desc = read<uint16_t>()))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "time_desc is not expected", K(ret), K(pos_));
        } else {
          switch (meta->attr_) {
            case common::OTMAT_TIMESTAMP_TZ://ObTimestampTZType
              obj.set_timestamp_tz(*time_us, *time_ctx_desc);
              break;
            case common::OTMAT_TIMESTAMP_LTZ://ObTimestampLTZType
              obj.set_timestamp_ltz(*time_us, *time_desc);
              break;
            case common::OTMAT_TIMESTAMP_NANO://ObTimestampNanoType
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
          obj.set_collation_type(static_cast<ObCollationType>(*read<uint8_t>()));
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only suport utf8 character set", KR(ret), K(meta->attr_));
        }
        if (OB_SUCC(ret)) {
          const uint32_t *len = read<uint32_t>();
          if(OB_UNLIKELY(pos_ + *len > row_end_pos_)){
            ret = OB_BUF_NOT_ENOUGH;
            LOG_WARN("buf is not large", KR(ret), K(pos_), K(*len), K(row_end_pos_));
          } else {
            value.assign_ptr((char*)(buf_ + pos_), *len);
            pos_ += *len;
            obj.set_char(value);
          }
        }
        if((src_meta.is_string_type() || src_meta.is_raw()) && obj.get_collation_type() == src_meta.get_collation_type()){
          obj.set_type(src_meta.get_type());
        }
        break;
      }
      case ObTextStoreType : {
        if (OB_FAIL(read_text_store(*meta, allocator, obj))) {
          LOG_WARN("fail to read text store", KR(ret));
        }
        break;
      }
      case ObHexStoreType: {
        ObString value;
        const int32_t *len = read<int32_t>();
        if (OB_UNLIKELY(pos_ + *len > row_end_pos_)) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN( "buf not enough", KR(ret), K(pos_), K(*len), K(row_end_pos_));
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
          LOG_WARN("the attr type is not supported.", K(ret), K(meta->attr_));
        }
        // 这里的处理方式应该与NULL value雷同
        // obj.set_type(src_meta.get_type());
        break;
      }
      case ObIntervalYMStoreType: {
        if (pos_ + ObIntervalYMValue::get_store_size() > row_end_pos_) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "buf not enough, ", K(ret), K(pos_), K(row_end_pos_));
        } else {
          const int64_t year_value = *read<int64_t>();
          obj.set_interval_ym(ObIntervalYMValue(year_value));
        }
        break;
      }
      case ObIntervalDSStoreType: {
        if (pos_ + ObIntervalDSValue::get_store_size() > row_end_pos_) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "buf not enough, ", K(ret), K(pos_), K(row_end_pos_));
        } else {
          const int64_t second_value = *read<int64_t>();
          const int32_t fs_value = *read<int32_t>();
          obj.set_interval_ds(ObIntervalDSValue(second_value, fs_value));
        }
        break;
      }
      case ObRowIDStoreType: {
        const uint32_t *len = read<uint32_t>();
        if (OB_UNLIKELY(pos_ + *len) > row_end_pos_) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "buf not enough", K(ret), K(pos_), K(*len), K(row_end_pos_));
        } else {
          obj.set_urowid(buf_ + pos_, *len);
          pos_ += *len;
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("don't support this data type", KR(ret), K(src_meta.get_type()),
                  K(meta->type_), K(meta->attr_));
    }//switch end
  }
  return ret;
}

int ObFlatRowReaderV1::read_sequence_columns(
    const ObIColumnMap *column_map,
    const int64_t column_count,
    ObIAllocator &allocator,
    common::ObNewRow &row)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    const ObIColumnIndexItem *column_index_item = column_map->get_column_index(i);
    row.cells_[i].set_meta_type(column_index_item->get_request_column_type());
    if (OB_FAIL(read_column_no_meta(column_index_item->get_request_column_type(), allocator, row.cells_[i]))) {
      LOG_WARN("fail to read column", KR(ret), K(i), K(column_index_item->get_request_column_type()), K(row.cells_[i]));
    }
  }
  if (OB_SUCC(ret)) {
    force_read_meta();
  }
  return ret;
}

int ObFlatRowReaderV1::read_column(
    const ObObjMeta &src_meta,
    ObIAllocator &allocator,
    ObObj &obj)
{
  int ret = OB_SUCCESS;
  const ObObj *obj_ptr = NULL;
  const ObObjTypeClass type_class = src_meta.get_type_class();
  const ObObjType column_type = src_meta.get_type();
  bool need_cast = true;
  bool empty_str = false;
  const ObStoreMeta *meta = read<ObStoreMeta>();

  switch (meta->type_) {
    case ObNullStoreType: {
      obj.set_null();
      break;
    }
    case ObIntStoreType: {
      switch (meta->attr_) {
        case 0://value is 1 byte
          READ_STORE(obj, set_tinyint, int8_t);
          break;
        case 1://value is 2 bytes
          READ_STORE(obj, set_smallint, int16_t);
          break;
        case 2://value is 4 bytes
          READ_STORE(obj, set_int32, int32_t);
          break;
        case 3://value is 8 bytes
          READ_STORE(obj, set_int, int64_t);
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support attr", KR(ret), K(meta->attr_));
      }
      if(ObIntTC == type_class && src_meta.get_type() > obj.get_type()){
        need_cast = false;
      } else if(ObUIntTC == type_class && obj.get_int() >= 0
                && static_cast<uint64_t>(obj.get_int()) <= UINT_MAX_VAL[src_meta.get_type()]){
        need_cast = false;
      }
      break;
    }
    case ObFloatStoreType: {
      READ_STORE(obj, set_float, float);
      if(ObFloatTC == type_class && obj.get_float() >= 0.0){
        need_cast = false;
      }
      break;
    }
    case ObDoubleStoreType: {
      READ_STORE(obj, set_double, double);
      if(ObDoubleTC == type_class && obj.get_double() >= 0.0){
        need_cast = false;
      }
      break;
    }
    case ObNumberStoreType: {
      number::ObNumber::Desc tmp_desc;
      tmp_desc.desc_ = *read<uint32_t>();
      tmp_desc.reserved_ = tmp_desc.len_;
      obj.set_number(tmp_desc, 0 == tmp_desc.len_ ? NULL : (uint32_t*)(buf_ + pos_));
      pos_ += sizeof(uint32_t) * tmp_desc.len_;
      if(ObNumberTC == type_class && !obj.get_number().is_negative()){
        need_cast = false;
      }
      break;
    }
    case ObTimestampStoreType: {
      switch (meta->attr_) {
        case 0://ObDateType
          READ_STORE(obj, set_date, int32_t);
          break;
        case 1://ObDateTimeType
          READ_STORE(obj, set_datetime, int64_t);
          break;
        case 2://ObTimestampType
          READ_STORE(obj, set_timestamp, int64_t);
          break;
        case 3://ObTimeType
          READ_STORE(obj, set_time, int64_t);
          break;
        case 4://ObYearType
          READ_STORE(obj, set_year, uint8_t);
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support attr", KR(ret), K(meta->attr_));
      }
      break;
    }
    case ObTimestampTZStoreType: {
      const int64_t *time_us = NULL;
      const uint32_t *time_ctx_desc = NULL;
      const uint16_t *time_desc = NULL;
      const int64_t length = ObObj::get_otimestamp_store_size(common::OTMAT_TIMESTAMP_TZ == meta->attr_);
      if(OB_UNLIKELY(pos_+ length > row_end_pos_)) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buf is not large", K(ret), K(pos_), K(length), K(row_end_pos_));
      } else if (OB_ISNULL((time_us = read<int64_t>()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "time_us is not expected", K(ret), K(pos_));
      } else if (common::OTMAT_TIMESTAMP_TZ == meta->attr_ && OB_ISNULL((time_ctx_desc = read<uint32_t>()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "time_ctx_desc is not expected", K(ret), K(pos_));
      } else if (common::OTMAT_TIMESTAMP_TZ != meta->attr_ && OB_ISNULL((time_desc = read<uint16_t>()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "time_desc is not expected", K(ret), K(pos_));
      } else {
        switch (meta->attr_) {
          case common::OTMAT_TIMESTAMP_TZ://ObTimestampTZType
            obj.set_timestamp_tz(*time_us, *time_ctx_desc);
            break;
          case common::OTMAT_TIMESTAMP_LTZ://ObTimestampLTZType
            obj.set_timestamp_ltz(*time_us, *time_desc);
            break;
          case common::OTMAT_TIMESTAMP_NANO://ObTimestampNanoType
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
        obj.set_collation_type(static_cast<ObCollationType>(*read<uint8_t>()));
      } else {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "row reader only suport utf8 character set.",
                    K(ret), K(meta->attr_));
      }
      if (OB_SUCC(ret)) {
        const uint32_t *len = read<uint32_t>();
        if(pos_ + *len > row_end_pos_){
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "buf is not large",
                      K(ret), K(pos_), K(*len), K(row_end_pos_));
        } else {
          value.assign_ptr((char*)(buf_ + pos_), *len);
          pos_ += *len;
          obj.set_char(value);
        }
      }
      empty_str = 0 == obj.get_val_len();
      if((ob_is_string_tc(src_meta.get_type()) || (src_meta.is_raw())) && obj.get_collation_type() == src_meta.get_collation_type()){
        need_cast = false;
      }
      break;
    }
    case ObTextStoreType : {
      if (OB_FAIL(read_text_store(*meta, allocator, obj))) {
        LOG_WARN("fail to read text store", KR(ret));
      } else if(src_meta.is_lob() && obj.get_collation_type() == src_meta.get_collation_type()) {
        need_cast = false;
      }
      empty_str = 0 == obj.get_val_len();
      break;
    }
    case ObHexStoreType: {
      ObString value;
      const uint32_t *len = read<uint32_t>();
      if (pos_ + *len > row_end_pos_) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buf not enough", KR(ret), K(pos_), K(*len), K(row_end_pos_));
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
        STORAGE_LOG(WARN, "the attr type is not supported.",
                    K(ret), K(meta->attr_));
      }
      break;
    }
    case ObIntervalYMStoreType: {
      if (pos_ + ObIntervalYMValue::get_store_size() > row_end_pos_) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buf not enough, ", K(ret), K(pos_), K(row_end_pos_));
      } else {
        const int64_t year_value = *read<int64_t>();
        obj.set_interval_ym(ObIntervalYMValue(year_value));
      }
      break;
    }
    case ObIntervalDSStoreType: {
      if (pos_ + ObIntervalDSValue::get_store_size() > row_end_pos_) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buf not enough, ", K(ret), K(pos_), K(row_end_pos_));
      } else {
        const int64_t second_value = *read<int64_t>();
        const int32_t fs_value = *read<int32_t>();
        obj.set_interval_ds(ObIntervalDSValue(second_value, fs_value));
      }
      break;
    }
    case ObRowIDStoreType: {
      const uint32_t str_len = *read<uint32_t>();
      if (OB_UNLIKELY(pos_ + str_len > row_end_pos_)) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "buffer not enough", K(ret), K(str_len), K(pos_), K(row_end_pos_));
      } else {
        obj.set_urowid((char*)(buf_ + pos_), str_len);
        pos_ += str_len;
      }
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("don't support this data type", KR(ret), K(src_meta.get_type()),
                K(meta->type_), K(meta->attr_));
  }//switch end

  if(OB_SUCC(ret)){
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
        if(OB_FAIL(ObObjCaster::to_type(src_meta.get_type(), cast_ctx, ori_obj, obj))) {
          LOG_WARN("fail to cast obj", KR(ret), K(ori_obj), K(obj), K(obj_ptr), K(ori_obj.get_type()),
              K(ob_obj_type_str(ori_obj.get_type())), K(src_meta.get_type()), K(ob_obj_type_str(src_meta.get_type())));
        }
      }
    }
  }
  return ret;
}

int ObFlatRowReaderV1::read_columns(
    const int64_t start_column_index,
    const bool check_null_value,
    const bool read_no_meta,
    const ObIColumnMap *column_map,
    ObIAllocator &allocator,
    common::ObNewRow &row,
    bool &has_null_value)
{
  int ret = OB_SUCCESS;
  ObObjMeta request_type;
  int64_t store_index = 0;
  int64_t column_index_offset = 0;
  int64_t request_count = column_map->get_request_count();
  for (int64_t i = start_column_index; OB_SUCC(ret) && i < request_count; ++i) {
    const ObIColumnIndexItem *column_index_item = column_map->get_column_index(i);
    store_index = column_index_item->get_store_index();
    request_type = column_index_item->get_request_column_type();
    if (OB_INVALID_INDEX != store_index) {
      if (2 == column_index_bytes_) {
        column_index_offset = reinterpret_cast<const int16_t *>(store_column_indexs_)[store_index];
      } else if (1 == column_index_bytes_) {
        column_index_offset = reinterpret_cast<const int8_t *>(store_column_indexs_)[store_index];
      } else if (4 == column_index_bytes_) {
        column_index_offset = reinterpret_cast<const int32_t *>(store_column_indexs_)[store_index];
      }
      pos_ = start_pos_ + column_index_offset;

      if (read_no_meta && column_index_item->get_is_column_type_matched()) {
        // Essentiall there is no need to set the meta in the ObObj each time if it's not
        // going to change anyway.
        if (OB_FAIL(read_column_no_meta(request_type, allocator, row.cells_[i]))) {
          LOG_WARN("fail to read column", KR(ret), K(request_type), K(i),
                    K(row.cells_[i]), K(*column_map));
        }
      } else if (OB_FAIL(read_column(request_type, allocator, row.cells_[i]))) {
        // We need to set the mata on fetching the first row and if the column meta stored
        // on macro does not match the one from schema, we need to perform the cast so we
        // have to resolve to the original 'type-aware' call.
        LOG_WARN("fail to read column", KR(ret), K(request_type), K(i),
                  K(row.cells_[i]), K(*column_map));
      }
    } else {
      row.cells_[i].set_nop_value();
    }

    if (OB_SUCC(ret) && check_null_value) {
      if (row.cells_[i].is_null() || row.cells_[i].is_nop_value()) {
        has_null_value = true;
      }
    }
  }
  return ret;
}

int ObFlatRowReaderV1::read_columns(
    const ObIColumnMap *column_map,
    ObIAllocator &allocator,
    ObNewRow &row)
{
  int ret = OB_SUCCESS;
  bool has_null_value = false;
  int64_t seq_read_column_count = column_map->get_seq_read_column_count();
  if (OB_UNLIKELY(row.count_ < column_map->get_request_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(row.count_), K(column_map->get_request_count()));
  } else if (seq_read_column_count > 0 &&
      OB_FAIL(read_sequence_columns(column_map, seq_read_column_count, allocator, row))) {
    LOG_WARN("sequence read columns in row failed", KR(ret), K_(is_first_row), K(*column_map));
  } else if (seq_read_column_count < column_map->get_request_count()) {
    if (OB_FAIL(read_columns(seq_read_column_count,
                             true, /* need check null value for next read row */
                             !is_first_row_ /* if not first row, next time we do not need read meta*/,
                             column_map,
                             allocator,
                             row,
                             has_null_value))) {
      LOG_WARN("read_columns failed", KR(ret), K_(is_first_row), K(has_null_value), K(*column_map), K(row));
    } else {
      is_first_row_ = true;
    }
  }
  return ret;
}

int ObFlatRowReaderV1::read_compact_rowkey(
    const ObObjMeta *column_types,
    const int64_t column_count,
    const ObTableLoadBackupVersion &backup_version,
    ObIAllocator &allocator,
    const char *buf,
    const int64_t row_end_pos,
    int64_t &pos,
    ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == column_types || column_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(column_types), K(column_count));
  } else if (OB_FAIL(setup_row(buf, row_end_pos, pos, backup_version, -1))) {
    LOG_WARN("fail to setup", KR(ret), K(OB_P(buf)), K(row_end_pos), K(pos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      row.cells_[i].set_meta_type(column_types[i]);
      if (OB_FAIL(read_column_no_meta(column_types[i], allocator, row.cells_[i]))) {
        LOG_WARN("fail to read column", KR(ret), K(i), K(column_types[i]), K(row.cells_[i]));
      }
    }
    if (OB_SUCC(ret)) {
      pos = pos_;
      row.count_ = column_count;
    }
  }
  return ret;
}

int ObFlatRowReaderV1::read_row(
    const char *row_buf,
    const int64_t row_end_pos,
    int64_t pos,
    const ObTableLoadBackupVersion &backup_version,
    const ObIColumnMap *column_map,
    ObIAllocator &allocator,
    common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == row_buf || nullptr == column_map)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(row_buf), KP(column_map));
  } else if (OB_FAIL(setup_row(row_buf, row_end_pos, pos, backup_version, column_map->get_store_count()))) {
    LOG_WARN("fail to setup", KR(ret), K(OB_P(row_buf)), K(row_end_pos), K(pos));
  } else if (OB_FAIL(read_columns(column_map, allocator, row))) {
    LOG_WARN("fail to read columns.", KR(ret), K(*column_map));
  }
  return ret;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
