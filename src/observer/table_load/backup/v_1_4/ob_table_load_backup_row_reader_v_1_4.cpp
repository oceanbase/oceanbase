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

#include "observer/table_load/backup/v_1_4/ob_table_load_backup_row_reader_v_1_4.h"

namespace oceanbase
{
namespace observer
{

#define READ_STORE(origin_obj, obj_set_fun, medium_type) \
  { \
    origin_obj.obj_set_fun(*read<medium_type>()); \
  }


/**
 * ObTableLoadBackupColumnMap_V_1_4
 */
int ObTableLoadBackupColumnMap_V_1_4::init(const ObTableLoadBackupMacroBlockMeta_V_1_4 *meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("column map already init", KR(ret));
  } else if (OB_UNLIKELY(meta == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_UNLIKELY(meta->column_number_ <= 0) ||
             OB_UNLIKELY(meta->column_number_ > OB_TABLE_LOAD_PRE_ROW_MAX_COLUMNS_COUNT) ||
             OB_UNLIKELY(meta->column_number_ > meta->column_number_) ||
             OB_UNLIKELY(meta->column_index_scale_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(meta->column_number_), K(meta->column_number_),
              K(meta->column_index_scale_));
  } else if (OB_UNLIKELY(meta->column_index_scale_ > 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not support step number greater than 1", KR(ret), K(meta->column_index_scale_));
  } else {
    request_count_ = 0;
    store_count_ = meta->column_number_;
    rowkey_store_count_ = meta->rowkey_column_number_;
    for (int64_t i = 0; i < store_count_; i++, request_count_++) {
      column_indexs_[request_count_].request_column_type_ = meta->column_type_array_[i];
      column_indexs_[request_count_].store_index_ = static_cast<int16_t>(i);
      column_indexs_[request_count_].is_column_type_matched_ = true;
    }
    seq_read_column_count_ = request_count_;
    read_full_rowkey_ = true;
    is_inited_ = true;
  }

  return ret;
}

void ObTableLoadBackupColumnMap_V_1_4::reuse()
{
  request_count_ = 0;
  store_count_ = 0;
  rowkey_store_count_ = 0;
  seq_read_column_count_ = 0;
  is_inited_ = false;
  read_full_rowkey_ = false;
}


/**
 * ObTableLoadBackupRowReader_V_1_4
 */
template<class T>
inline const T *ObTableLoadBackupRowReader_V_1_4::read()
{
  const T *ptr = reinterpret_cast<const T*>(buf_ + pos_);
  pos_ += sizeof(T);
  return ptr;
}

int ObTableLoadBackupRowReader_V_1_4::setup_row(const char *buf,
                                                const int64_t row_end_pos,
                                                const int64_t pos,
                                                const int64_t column_index_count)
{
  int ret = OB_SUCCESS;
  int64_t header_size = sizeof(ObTableLoadBackupRowHeaderDummy_V_1_4);
  if (OB_UNLIKELY(buf == nullptr || row_end_pos <= 0 || column_index_count < -1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(OB_P(buf)), K(row_end_pos), K(column_index_count));
  } else if (OB_UNLIKELY(pos < 0 || (column_index_count >= 0 && pos + header_size >= row_end_pos))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("invalid argument", KR(ret), K(pos), K(header_size), K(row_end_pos));
  } else {
    buf_ = buf;
    row_end_pos_ = row_end_pos;
    start_pos_ = pos;

    if (column_index_count >= 0) {
      row_header_ = reinterpret_cast<const ObTableLoadBackupRowHeader_V_1_4*>(buf_ + pos);
      if (row_header_->get_version() > 0) {
        header_size = sizeof(ObTableLoadBackupRowHeader_V_1_4);
      }
      pos_ = pos + header_size;
      if (OB_UNLIKELY(pos + header_size + row_header_->get_column_index_bytes() * column_index_count >= row_end_pos)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("invalid argument", KR(ret), K(pos), K(header_size),
                 K(row_header_->get_column_index_bytes() * column_index_count), K(row_end_pos));
      } else if (column_index_count > 0) {
        store_column_indexs_ = buf_ + row_end_pos_ - row_header_->get_column_index_bytes() * column_index_count;
      } else {
        store_column_indexs_ = NULL;
      }
    } else {
      pos_ = pos;
      row_header_ = NULL;
      store_column_indexs_ = NULL;
    }
  }

  return ret;
}

int ObTableLoadBackupRowReader_V_1_4::read_column_no_meta(const ObObjMeta &src_meta, ObObj &obj)
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
        if(src_meta.is_string_type() && obj.get_collation_type() == src_meta.get_collation_type()){
          obj.set_type(src_meta.get_type());
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
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("don't support this data type", KR(ret), K(src_meta.get_type()),
                  K(meta->type_), K(meta->attr_));
    }//switch end
  }

  return ret;
}

template<class T>
int ObTableLoadBackupRowReader_V_1_4::read_sequence_columns(const ObIArray<int64_t> &column_ids,
                                                            const T *items,
                                                            const int64_t column_count,
                                                            common::ObNewRow &row)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    int64_t column_id = column_ids.at(i);
    if (column_id == -1) continue;
    row.cells_[column_id].set_meta_type(items[i].get_obj_meta());
    if (OB_FAIL(read_column_no_meta(items[i].get_obj_meta(), row.cells_[column_id]))) {
      LOG_WARN("fail to read column", KR(ret), K(i), K(items[i].get_obj_meta()), K(row.cells_[column_id]));
    }
  }
  if (OB_SUCC(ret)) {
    force_read_meta();
  }

  return ret;
}

int ObTableLoadBackupRowReader_V_1_4::read_column(const ObObjMeta &src_meta, ObObj &obj)
{
  int ret = OB_SUCCESS;
  const ObObj *obj_ptr = NULL;
  const ObObjTypeClass type_class = src_meta.get_type_class();
  bool need_cast = true;
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
        if(pos_ + *len > row_end_pos_){
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("buf is not large", KR(ret), K(pos_), K(*len), K(row_end_pos_));
        } else {
          value.assign_ptr((char*)(buf_ + pos_), *len);
          pos_ += *len;
          obj.set_char(value);
        }
      }

      if(src_meta.is_string_type() && obj.get_collation_type() == src_meta.get_collation_type()){
        need_cast = false;
      }
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
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("don't support this data type", KR(ret), K(src_meta.get_type()),
                K(meta->type_), K(meta->attr_));
  }//switch end

  if(OB_SUCC(ret)){
    if(ObNullType == obj.get_type() || ObExtendType == obj.get_type()){
    //ignore src_meta type, do nothing, just return obj
    } else if (obj.get_type() == src_meta.get_type() || !need_cast){//just change the type
      obj.set_type(src_meta.get_type());
    } else {
      //not support data alteration
      ObObj ori_obj = obj;
      int64_t cm_mode = CM_NONE;
      if(ObIntTC == ori_obj.get_type_class() && ObUIntTC == type_class) {
        obj.set_uint(src_meta.get_type(), static_cast<uint64_t>(ori_obj.get_int()));
      } else if (ObIntTC == ori_obj.get_type_class() && ObBitTC == type_class) {
        obj.set_bit(static_cast<uint64_t>(ori_obj.get_int()));
      } else {
        ObCastCtx cast_ctx(&allocator_, NULL, cm_mode, src_meta.get_collation_type());
        if(OB_FAIL(ObObjCaster::to_type(src_meta.get_type(), cast_ctx, ori_obj, obj))) {
          LOG_WARN("fail to cast obj", KR(ret), K(ori_obj), K(obj), K(obj_ptr), K(ori_obj.get_type()),
                   K(ob_obj_type_str(ori_obj.get_type())), K(src_meta.get_type()), K(ob_obj_type_str(src_meta.get_type())));
        }
      }
    }
  }

  return ret;
}

int ObTableLoadBackupRowReader_V_1_4::read_columns(const ObIArray<int64_t> &column_ids,
                                                   const int64_t start_column_index,
                                                   const bool check_null_value,
                                                   const bool read_no_meta,
                                                   const ObTableLoadBackupColumnMap_V_1_4 *column_map,
                                                   common::ObNewRow &row,
                                                   bool &has_null_value)
{
  int ret = OB_SUCCESS;
  ObObjMeta request_type;
  int64_t store_index = 0;
  int64_t column_index_offset = 0;
  int64_t request_count = column_map->get_request_count();
  const ObTableLoadBackupColumnIndexItem_V_1_4 *column_indexs = column_map->get_column_indexs();
  const int8_t store_column_index_bytes = row_header_->get_column_index_bytes();
  for (int64_t i = start_column_index; OB_SUCC(ret) && i < request_count; ++i) {
    int64_t column_id = column_ids.at(i);
    if (column_id == -1) continue;
    store_index = column_indexs[i].store_index_;
    request_type = column_indexs[i].request_column_type_;
    if (OB_INVALID_INDEX != store_index) {
      if (2 == store_column_index_bytes) {
        column_index_offset = reinterpret_cast<const int16_t *>(store_column_indexs_)[store_index];
      } else if (1 == store_column_index_bytes) {
        column_index_offset = reinterpret_cast<const int8_t *>(store_column_indexs_)[store_index];
      } else if (4 == store_column_index_bytes) {
        column_index_offset = reinterpret_cast<const int32_t *>(store_column_indexs_)[store_index];
      }
      pos_ = start_pos_ + column_index_offset;

      if (read_no_meta && column_indexs[i].is_column_type_matched_) {
        // Essentiall there is no need to set the meta in the ObObj each time if it's not
        // going to change anyway.
        if (OB_FAIL(read_column_no_meta(request_type, row.cells_[column_id]))) {
          LOG_WARN("fail to read column", KR(ret), K(request_type), K(i),K(column_indexs[i]),
                    K(row.cells_[column_id]), K(*column_map));
        }
      } else if (OB_FAIL(read_column(request_type, row.cells_[column_id]))) {
        // We need to set the mata on fetching the first row and if the column meta stored
        // on macro does not match the one from schema, we need to perform the cast so we
        // have to resolve to the original 'type-aware' call.
        LOG_WARN("fail to read column", KR(ret), K(request_type), K(i), K(column_indexs[i]),
                  K(row.cells_[column_id]), K(*column_map));
      }
    } else {
      row.cells_[column_id].set_nop_value();
    }

    if (OB_SUCC(ret) && check_null_value) {
      if (row.cells_[column_id].is_null() || row.cells_[column_id].is_nop_value()) {
        has_null_value = true;
      }
    }
  }

  return ret;
}

int ObTableLoadBackupRowReader_V_1_4::read_columns(const ObIArray<int64_t> &column_ids,
                                                  const ObTableLoadBackupColumnMap_V_1_4 *column_map,
                                                  common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  bool has_null_value = false;
  int64_t seq_read_column_count = column_map->get_seq_read_column_count();
  if (OB_UNLIKELY(column_ids.count() < column_map->get_request_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(column_ids.count()), K(column_map->get_request_count()));
  } else if (seq_read_column_count > 0 &&
      OB_FAIL(read_sequence_columns(column_ids, column_map->get_column_indexs(), seq_read_column_count, row))) {
    LOG_WARN("sequence read columns in row failed", KR(ret), K_(is_first_row), K(*column_map));
  } else if (seq_read_column_count < column_map->get_request_count()) {
    if (OB_FAIL(read_columns(column_ids,
                             seq_read_column_count,
                             true, /* need check null value for next read row */
                             !is_first_row_ /* if not first row, next time we do not need read meta*/,
                             column_map,
                             row,
                             has_null_value))) {
      LOG_WARN("read_columns failed", KR(ret), K_(is_first_row), K(has_null_value), K(*column_map), K(row));
    } else {
      is_first_row_ = has_null_value;
    }
  }

  return ret;
}

void ObTableLoadBackupRowReader_V_1_4::reset()
{
  allocator_.reset();
}

int ObTableLoadBackupRowReader_V_1_4::read_compact_rowkey(const ObObjMeta *column_types,
                                                          const int64_t column_count,
                                                          const char *buf,
                                                          const int64_t row_end_pos,
                                                          int64_t &pos,
                                                          ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_types) || OB_UNLIKELY(column_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(column_types), K(column_count));
  } else if (OB_FAIL(setup_row(buf, row_end_pos, pos, -1))) {
    LOG_WARN("fail to setup", KR(ret), K(OB_P(buf)), K(row_end_pos), K(pos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      row.cells_[i].set_meta_type(column_types[i]);
      if (OB_FAIL(read_column_no_meta(column_types[i], row.cells_[i]))) {
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

int ObTableLoadBackupRowReader_V_1_4::read_meta_row(const ObIArray<int64_t> &column_ids,
                                                    const ObTableLoadBackupColumnMap_V_1_4 *column_map,
                                                    const char *buf,
                                                    const int64_t row_end_pos,
                                                    int64_t pos,
                                                    common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_map == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column_map is nullptr", KR(ret));
  } else if (OB_FAIL(setup_row(buf, row_end_pos, pos, column_map->get_store_count()))) {
    LOG_WARN("fail to setup", KR(ret), K(OB_P(buf)), K(row_end_pos), K(pos));
  } else if (OB_FAIL(read_columns(column_ids, column_map, row))) {
    LOG_WARN("fail to read columns.", KR(ret), K(*column_map));
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase
