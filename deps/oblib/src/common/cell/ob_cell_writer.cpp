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

#include "common/cell/ob_cell_writer.h"
#include "common/object/ob_object.h"
#include "lib/timezone/ob_time_convert.h"
namespace oceanbase
{
namespace common
{

#define WRITE_META(meta_type, meta_attr) \
  { \
    if (pos_ + sizeof(ObCellWriter::CellMeta) > buf_size_) { \
      ret = OB_BUF_NOT_ENOUGH; \
    } else { \
      ObCellWriter::CellMeta *cell_meta = reinterpret_cast<ObCellWriter::CellMeta*> (buf_ + pos_); \
      cell_meta->type_ = meta_type; \
      cell_meta->attr_ = meta_attr; \
      pos_ += sizeof(ObCellWriter::CellMeta); \
    } \
  }
#define WRITE_DATA(meta_type, meta_attr, store_type, value) \
{ \
  if (pos_ + sizeof(ObCellWriter::CellMeta) + sizeof(store_type) > buf_size_) { \
    ret = OB_BUF_NOT_ENOUGH; \
  } else { \
    ObCellWriter::CellMeta *cell_meta = reinterpret_cast<ObCellWriter::CellMeta*> (buf_ + pos_); \
    cell_meta->type_ = meta_type; \
    cell_meta->attr_ = meta_attr; \
    pos_ += sizeof(ObCellWriter::CellMeta); \
    *(reinterpret_cast<store_type*> (buf_ + pos_)) = value; \
    pos_ += sizeof(store_type); \
  } \
}

int ObCellWriter::write_int(const ObObj &obj, const enum ObObjType meta_type, const int64_t value)
{
  UNUSED(obj);
  int ret = OB_SUCCESS;
  int bytes = get_int_byte(value);
  if (pos_ + sizeof(ObCellWriter::CellMeta) + bytes > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    ObCellWriter::CellMeta *cell_meta = reinterpret_cast<ObCellWriter::CellMeta*> (buf_ + pos_);
    switch (bytes) {
      case 1:
        cell_meta->type_ = meta_type;
        cell_meta->attr_ = 0;
        pos_ += sizeof(ObCellWriter::CellMeta);
        *(reinterpret_cast<int8_t*> (buf_ + pos_)) = static_cast<int8_t> (value);
        pos_ += bytes;
        break;
      case 2:
        cell_meta->type_ = meta_type;
        cell_meta->attr_ = 1;
        pos_ += sizeof(ObCellWriter::CellMeta);
        *(reinterpret_cast<int16_t*> (buf_ + pos_)) = static_cast<int16_t> (value);
        pos_ += bytes;
        break;
      case 4:
        cell_meta->type_ = meta_type;
        cell_meta->attr_ = 2;
        pos_ += sizeof(ObCellWriter::CellMeta);
        *(reinterpret_cast<int32_t*> (buf_ + pos_)) = static_cast<int32_t> (value);
        pos_ += bytes;
        break;
      case 8:
        cell_meta->type_ = meta_type;
        cell_meta->attr_ = 3;
        pos_ += sizeof(ObCellWriter::CellMeta);
        *(reinterpret_cast<int64_t*> (buf_ + pos_)) = value;
        pos_ += bytes;
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "not supported type, ", K(ret), K(meta_type), K(value));
    }
  }
  return ret;
}

int ObCellWriter::write_number(const enum ObObjType meta_type, const number::ObNumber &number, ObObj *clone_obj)
{
  int ret = OB_SUCCESS;
  WRITE_META(meta_type, 0);
  if (OB_SUCC(ret)) {
    if (NULL != clone_obj) {
      number::ObNumber clone_number;
      ret = number.encode(buf_, buf_size_, pos_, &clone_number);
      clone_obj->set_number_value(clone_number);
    } else {
      ret = number.encode(buf_, buf_size_, pos_);
    }
  }
  return ret;
}

int ObCellWriter::write_char(const ObObj &obj, const enum ObObjType meta_type, const ObString &str, ObObj *clone_obj)
{
  int ret = OB_SUCCESS;
  uint32_t str_len = static_cast<uint32_t> (str.length());
  int64_t len = sizeof(ObCellWriter::CellMeta) + sizeof(uint32_t) + str_len;
  ObCellWriter::CellMeta meta;

  meta.type_ = static_cast<uint8_t>(meta_type);
  if (CS_TYPE_UTF8MB4_GENERAL_CI == obj.get_collation_type()) {
    meta.attr_ = 0;
  } else {
    meta.attr_ = 1;
    len += sizeof(uint8_t);
  }

  if (pos_ + len > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *(reinterpret_cast<ObCellWriter::CellMeta*> (buf_ + pos_)) = meta;
    pos_ += sizeof(ObCellWriter::CellMeta);

    if (1 == meta.attr_) {
      *(reinterpret_cast<uint8_t*> (buf_ + pos_)) = static_cast<uint8_t>(obj.get_collation_type());
      pos_ += sizeof(uint8_t);
    }
    *(reinterpret_cast<uint32_t*> (buf_ + pos_)) = str_len;
    pos_ += sizeof(uint32_t);

    MEMCPY(buf_ + pos_, str.ptr(), str_len);
    if (OB_UNLIKELY(NULL != clone_obj)) {
      if (0 != str_len) {
        clone_obj->set_string(meta_type, buf_ + pos_, str_len);
      } else {
        clone_obj->set_string(meta_type, NULL, str_len);
      }
    }
    pos_ += str_len;
  }
  return ret;
}

int ObCellWriter::write_text(const ObObj &obj, const enum ObObjType store_type, const ObString &char_value, ObObj *clone_obj)
{
  int ret = OB_SUCCESS;
  CellMeta meta;
  ObLength max_length = 0;
  if (old_text_format()) {
    max_length = ObAccuracy::MAX_ACCURACY_OLD[obj.get_type()].get_length();
  } else {
    max_length = ObAccuracy::MAX_ACCURACY[obj.get_type()].get_length();
  }

  meta.type_ = CellMeta::SF_MASK_TYPE & static_cast<uint8_t>(store_type);
  if (CS_TYPE_UTF8MB4_GENERAL_CI == obj.get_collation_type()) {
    meta.attr_ = old_text_format() ? TEXT_VARCHAR_NO_COLL : TEXT_SCALE_NO_COLL;
  } else {
    meta.attr_ = old_text_format() ? TEXT_VARCHAR_COLL : TEXT_SCALE_COLL;
  }

  if (OB_SUCC(ret)) {
    if (char_value.length() > (max_length)) {
      ret = OB_SIZE_OVERFLOW;
      COMMON_LOG(WARN, "lob value is overflow", K(ret), "length", char_value.length(), K(max_length));
    } else if (OB_FAIL(append<CellMeta>(meta))) {
    } else if (meta.need_collation()) {
      if (OB_FAIL(append<uint8_t>(static_cast<uint8_t>(obj.get_collation_type())))) {
      }
    }
  }
  if (OB_SUCC(ret) && !old_text_format()) {
    ObLobScale lob_scale(obj.get_scale());
    if (obj.has_lob_header()) {
      lob_scale.set_has_lob_header();
    } else if (!lob_scale.is_in_row()) {
      lob_scale.set_in_row();
    }
    if (OB_FAIL(append<uint8_t>(static_cast<uint8_t>(lob_scale.get_scale())))) {
      COMMON_LOG(WARN, "fail to append scale, ",
         K(ret), K(lob_scale));
    } else if (OB_FAIL(append<uint8_t>(TEXT_CELL_META_VERSION))) {
      COMMON_LOG(WARN, "fail to append lob version", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append<uint32_t>(static_cast<uint32_t>(char_value.length())))) {
    } else if (pos_ + char_value.length() >= buf_size_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf_ + pos_, char_value.ptr(), char_value.length());
      if (NULL != clone_obj) {
        if (char_value.empty()) {
          clone_obj->set_string((store_type), NULL, 0);
        } else {
          clone_obj->set_string((store_type), buf_ + pos_, char_value.length());
        }
      }
      pos_ += char_value.length();
    }
  }
  return ret;
}

int ObCellWriter::write_binary(const enum ObObjType meta_type, const ObString &str, ObObj *clone_obj)
{
  int ret = OB_SUCCESS;
  uint32_t str_len = static_cast<uint32_t> (str.length());
  if (pos_ + sizeof(CellMeta) + sizeof(uint32_t) + str_len > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    CellMeta *meta = reinterpret_cast<CellMeta*> (buf_ + pos_);
    meta->type_ = meta_type;
    meta->attr_ = 0;
    pos_ += sizeof(CellMeta);

    *(reinterpret_cast<uint32_t*> (buf_ + pos_)) = str_len;
    pos_ += sizeof(uint32_t);

    MEMCPY(buf_ + pos_, str.ptr(), str_len);
    if (OB_UNLIKELY(NULL != clone_obj)) {
      if (str_len != 0) {
        clone_obj->set_string(meta_type, buf_ + pos_, str_len);
      } else {
        clone_obj->set_string(meta_type, NULL, str_len);
      }
    }
    pos_ += str_len;
  }
  return ret;
}

int ObCellWriter::write_oracle_timestamp(const ObObj &obj, const common::ObOTimestampMetaAttrType otmat)
{
  int ret = OB_SUCCESS;
  CellMeta meta;
  meta.type_ = (static_cast<uint8_t>(obj.get_type()) & 0x3F);
  meta.attr_ = (static_cast<uint8_t>(otmat) & 0x03);
  const ObOTimestampData &ot_data = obj.get_otimestamp_value();
  if (OB_FAIL(append<CellMeta>(meta))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      COMMON_LOG(WARN, "fail to append CellMeta, ", K(ret), "type", meta.type_, "attr", meta.attr_);
    }
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

int ObCellWriter::write_interval_ds(const ObObj &obj)
{
  int ret = OB_SUCCESS;
  CellMeta meta;
  meta.type_ = (static_cast<uint8_t>(obj.get_type()) & 0x3F);
  meta.attr_ = 0;
  ObIntervalDSValue value = obj.get_interval_ds();
  if (OB_FAIL(append<CellMeta>(meta))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      COMMON_LOG(WARN, "fail to append CellMeta, ", K(ret), "type", meta.type_, "attr", meta.attr_);
    }
  } else if (OB_FAIL(append<int64_t>(value.nsecond_))) {
  } else if (OB_FAIL(append<int32_t>(value.fractional_second_))) {
  }
  return ret;
}

int ObCellWriter::write_interval_ym(const ObObj &obj)
{
  int ret = OB_SUCCESS;
  CellMeta meta;
  meta.type_ = (static_cast<uint8_t>(obj.get_type()) & 0x3F);
  meta.attr_ = 0;
  ObIntervalYMValue value = obj.get_interval_ym();
  if (OB_FAIL(append<CellMeta>(meta))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      COMMON_LOG(WARN, "fail to append CellMeta, ", K(ret), "type", meta.type_, "attr", meta.attr_);
    }
  } else if (OB_FAIL(append<int64_t>(value.nmonth_))) {
  }
  return ret;
}

int ObCellWriter::write_ext(const int64_t ext_value)
{
  int ret = OB_SUCCESS;
  switch(ext_value) {
    case ObActionFlag::OP_END_FLAG:
      WRITE_META(ObExtendType, EA_END_FLAG);
      break;
    case ObActionFlag::OP_NOP:
      WRITE_DATA(ObExtendType, EA_OTHER, int8_t, EV_NOP_ROW);
      break;
    case ObActionFlag::OP_DEL_ROW:
      WRITE_DATA(ObExtendType, EA_OTHER, int8_t, EV_DEL_ROW);
      break;
    case ObActionFlag::OP_LOCK_ROW:
      WRITE_DATA(ObExtendType, EA_OTHER, int8_t, EV_LOCK_ROW);
      break;
    case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
      WRITE_DATA(ObExtendType, EA_OTHER, int8_t, EV_NOT_EXIST_ROW);
      break;
    case ObObj::MIN_OBJECT_VALUE:
      WRITE_DATA(ObExtendType, EA_OTHER, int8_t, EV_MIN_CELL);
      break;
    case ObObj::MAX_OBJECT_VALUE:
      WRITE_DATA(ObExtendType, EA_OTHER, int8_t, EV_MAX_CELL);
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "invalid extend type.", K(ret), K(ext_value));
   }
  return ret;
}

int ObCellWriter::write_urowid(const ObObj &obj, ObObj *clone_obj)
{
  int ret = OB_SUCCESS;
  uint32_t str_len = static_cast<uint32_t>(obj.get_string_len());
  int64_t len = sizeof(ObCellWriter::CellMeta) + sizeof(uint32_t) + str_len;
  ObCellWriter::CellMeta meta;

  meta.type_ = static_cast<uint8_t>(obj.get_type());
  if (OB_UNLIKELY(pos_ + len > buf_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    *(reinterpret_cast<ObCellWriter::CellMeta *>(buf_ + pos_)) = meta;
    pos_ += sizeof(ObCellWriter::CellMeta);

    *(reinterpret_cast<uint32_t *>(buf_ + pos_)) = str_len;
    pos_ += sizeof(uint32_t);

    MEMCPY(buf_ + pos_, obj.get_string_ptr(), str_len);
    if (OB_LIKELY(NULL != clone_obj)) {
      if (0 != str_len) {
        clone_obj->set_urowid(buf_ + pos_, str_len);
      } else {
        clone_obj->set_urowid(NULL, str_len);
      }
    }
    pos_ += str_len;
  }
  return ret;
}

ObCellWriter::ObCellWriter()
    :buf_(NULL),
     buf_size_(0),
     pos_(0),
     last_append_pos_(0),
     cell_cnt_(0),
     store_type_(SPARSE),
     old_text_format_(false),
     is_inited_(false)
{
}

int ObCellWriter::init(char *buf, int64_t size, const ObCompactStoreType store_type, const bool old_text_format)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "ObCellWriter has be inited, ", K(ret));
  } else if (NULL == buf || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid cell writer input argument.", K(ret), KP(buf), K(size));
  } else if (DENSE != store_type && SPARSE != store_type) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid cell writer input argument.", K(ret), K(store_type));
  } else {
    buf_ = buf;
    buf_size_ = size;
    pos_ = 0;
    cell_cnt_ = 0;
    store_type_ = store_type;
    old_text_format_ = old_text_format;
    is_inited_ = true;
  }
  return ret;
}

//This interface is to expand memory, cell_writer has been init before
int ObCellWriter::extend_buf(char *buf, int64_t size)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObCellWriter not inited", K(ret));
  } else if (NULL == buf || size <= 0 || pos_ >= size) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid cell writer input argument.", K(ret), KP(buf), K_(pos), K(size));
  } else {
    //Now back up the data in buf_ to buf, and then modify the pointer position, the size of pos_ does not need to be modified
    MEMCPY(buf, buf_, pos_);
    buf_ = buf;
    buf_size_ = size;
  }

  return ret;
}

//In order to restore the initial position of the buf pointer, the interface does not need to copy data during the revert process
int ObCellWriter::revert_buf(char *buf, int64_t size)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObCellWriter not inited", K(ret));
  } else if (NULL == buf || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid cell writer input argument.", K(ret), KP(buf), K_(pos), K(size));
  } else {
    buf_ = buf;
    buf_size_ = size;
  }

  return ret;
}

int ObCellWriter::append(uint64_t column_id, const ObObj &obj, ObObj *clone_obj)
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = obj.get_type();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObCellWriter should be inited first, ", K(ret));
  } else if (OB_UNLIKELY(!obj.is_valid_type())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument, ", K(ret), "type", obj.get_type());
  } else {
    if (NULL != clone_obj) {
      *clone_obj = obj;
    }

    switch (obj_type) {
      case ObNullType:
        WRITE_META(ObNullType, 0);
        break;
      case ObTinyIntType:
        ret = write_int(obj, ObTinyIntType, obj.get_tinyint());
        break;
      case ObSmallIntType:
        ret = write_int(obj, ObSmallIntType, obj.get_smallint());
        break;
      case ObMediumIntType:
        ret = write_int(obj, ObMediumIntType, obj.get_mediumint());
        break;
      case ObInt32Type:
        ret = write_int(obj, ObInt32Type, obj.get_int32());
        break;
      case ObIntType:
        ret = write_int(obj, ObIntType, obj.get_int());
        break;
      case ObUTinyIntType:
        ret = write_int(obj, ObUTinyIntType, obj.get_utinyint());
        break;
      case ObUSmallIntType:
        ret = write_int(obj, ObUSmallIntType, obj.get_usmallint());
        break;
      case ObUMediumIntType:
        ret = write_int(obj, ObUMediumIntType, obj.get_umediumint());
        break;
      case ObUInt32Type:
        ret = write_int(obj, ObUInt32Type, obj.get_uint32());
        break;
      case ObUInt64Type:
        ret = write_int(obj, ObUInt64Type, obj.get_uint64());
        break;
      case ObFloatType:
        WRITE_DATA(ObFloatType, 0, float, obj.get_float());
        break;
      case ObDoubleType:
        WRITE_DATA(ObDoubleType, 0, double, obj.get_double());
        break;
      case ObUFloatType:
        WRITE_DATA(ObUFloatType, 0, float, obj.get_ufloat());
        break;
      case ObUDoubleType:
        WRITE_DATA(ObUDoubleType, 0, double, obj.get_udouble());
        break;
      case ObNumberType:
        ret = write_number(ObNumberType, obj.get_number(), clone_obj);
        break;
      case ObUNumberType:
        ret = write_number(ObUNumberType, obj.get_unumber(), clone_obj);
        break;
      case ObDateType:
        WRITE_DATA(ObDateType, 0, int32_t, obj.get_date());
        break;
      case ObDateTimeType:
        WRITE_DATA(ObDateTimeType, 0, int64_t, obj.get_datetime());
        break;
      case ObTimestampType:
        WRITE_DATA(ObTimestampType, 0, int64_t, obj.get_timestamp());
        break;
      case ObTimeType: {
        const int64_t ob_time_max_value = TIME_MAX_VAL;
        assert(obj.get_time() <= ob_time_max_value && obj.get_time() >= -ob_time_max_value);
        WRITE_DATA(ObTimeType, 0, int64_t, obj.get_time());
        break;
      }
      case ObYearType:
        WRITE_DATA(ObYearType, 0, uint8_t, obj.get_year());
        break;
      case ObVarcharType:
        ret = write_char(obj, ObVarcharType, obj.get_string(), clone_obj);
        break;
      case ObCharType:
        ret = write_char(obj, ObCharType, obj.get_string(), clone_obj);
        break;
      case ObHexStringType:
        ret = write_binary(ObHexStringType, obj.get_hex_string(), clone_obj);
        break;
      case ObExtendType:
        ret = write_ext(obj.get_ext());
        break;
      //TODO@hanhui texttc share with varchar temporarily
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType: 
      case ObJsonType:
      case ObGeometryType: {
        ret = write_text(obj, obj.get_type(), obj.get_string(), clone_obj);
        break;
      }
      case ObBitType:
        ret = write_int(obj, ObBitType, obj.get_bit());
        break;
      case ObEnumType:
        ret = write_int(obj, ObEnumType, obj.get_enum());
        break;
      case ObSetType:
        ret = write_int(obj, ObSetType, obj.get_set());
        break;
      case ObTimestampTZType:
        ret = write_oracle_timestamp(obj, common::OTMAT_TIMESTAMP_TZ);
        break;
      case ObTimestampLTZType:
        ret = write_oracle_timestamp(obj, common::OTMAT_TIMESTAMP_LTZ);
        break;
      case ObTimestampNanoType:
        ret = write_oracle_timestamp(obj, common::OTMAT_TIMESTAMP_NANO);
        break;
      case ObRawType:
        ret = write_char(obj, ObRawType, obj.get_raw(), clone_obj);
        break;
      case ObIntervalYMType:
        ret = write_interval_ym(obj);
        break;
      case ObIntervalDSType:
        ret = write_interval_ds(obj);
        break;
      case ObNumberFloatType:
        ret = write_number(ObNumberFloatType, obj.get_number_float(), clone_obj);
        break;
      case ObNVarchar2Type:
        ret = write_char(obj, ObNVarchar2Type, obj.get_nvarchar2(), clone_obj);
        break;
      case ObNCharType:
        ret = write_char(obj, ObNCharType, obj.get_nchar(), clone_obj);
        break;
      case ObURowIDType:
        ret = write_urowid(obj, clone_obj);
        break;
      case ObLobType: {
        ret = ! allow_lob_locator() ? OB_NOT_SUPPORTED :
              write_char(obj, ObLobType,
                ObString(obj.get_val_len(), reinterpret_cast<const char *>(obj.get_lob_locator())),
                clone_obj);
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "cell writer don't support the data type.",
            K(ret), "type", obj.get_type());
    }
    if (OB_SUCCESS == ret && OB_INVALID_ID != column_id && SPARSE == store_type_) {
      if (ObExtendType != obj_type
          || obj.is_min_value()
          || obj.is_max_value()) {
        if (OB_FAIL(append<uint32_t>(static_cast<uint32_t>(column_id)))) {
          if (OB_BUF_NOT_ENOUGH != ret) {
            COMMON_LOG(WARN, "fail to append column_id, ", K(ret), K(column_id));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ++cell_cnt_;
      //Save the last_append_pos site for easy column append retry
      last_append_pos_ = pos_;
    } else {
      //Roll back the pos position modified by the column append to facilitate subsequent retry
      pos_ = last_append_pos_;
    }
  }
  return ret;
}

int ObCellWriter::append(const ObObj &obj)
{
  return append(OB_INVALID_ID, obj);
}

void ObCellWriter::reset()
{
  buf_ = NULL;
  buf_size_ = 0;
  pos_ = 0;
  last_append_pos_ = 0;
  cell_cnt_ = 0;
  store_type_ = SPARSE;
  old_text_format_ = false;
  is_inited_ = false;
}

int ObCellWriter::row_nop()
{
  int ret = OB_SUCCESS;
  CellMeta meta;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObCellWriter should be inited firsrt, ", K(ret));
  } else {
    WRITE_DATA(ObExtendType, EA_OTHER, int8_t, EV_NOP_ROW);
  }
  return ret;
}

int ObCellWriter::row_delete()
{
  int ret = OB_SUCCESS;
  CellMeta meta;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObCellWriter should be inited first, ", K(ret));
  } else {
    WRITE_DATA(ObExtendType, EA_OTHER, int8_t, EV_DEL_ROW);
  }
  return ret;
}

int ObCellWriter::row_not_exist()
{
  int ret = OB_SUCCESS;
  CellMeta meta;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObCellWriter should be inited first, ", K(ret));
  } else {
    WRITE_DATA(ObExtendType, EA_OTHER, int8_t, EV_NOT_EXIST_ROW);
  }
  return ret;
}

int ObCellWriter::row_finish()
{
  int ret = OB_SUCCESS;
  CellMeta meta;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObCellWriter should be inited first, ", K(ret));
  } else {
    WRITE_META(ObExtendType, ObCellWriter::EA_END_FLAG);
  }
  return ret;
}

template<class T>
int ObCellWriter::append(const T &value)
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
inline int ObCellWriter::get_int_byte(int64_t int_value)
{
  int bytes = 0;
  if (int_value >= -128 && int_value <= 127) {
    bytes = 1;
  } else if (int_value >= static_cast<int16_t>(0x8000)
             && int_value <= static_cast<int16_t>(0x7FFF)){
    bytes = 2;
  } else if (int_value >= static_cast<int32_t>(0x80000000)
             && int_value <= static_cast<int32_t>(0x7FFFFFFF)) {
    bytes = 4;
  } else {
    bytes = 8;
  }
  return bytes;
}
}//end namespace common
}//end namespace oceanbase
