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

#include "ob_sparse_cell_writer.h"
#include "common/object/ob_object.h"
#include "common/cell/ob_cell_writer.h"
#include "storage/ob_i_store.h"

using namespace oceanbase;
using namespace common;
using namespace blocksstable;

#define SPARSE_WRITE_META(meta_type, meta_attr)                                                   \
  {                                                                                               \
    if (pos_ + sizeof(ObCellWriter::CellMeta) > buf_size_) {                                      \
      ret = OB_BUF_NOT_ENOUGH;                                                                    \
    } else {                                                                                      \
      ObCellWriter::CellMeta* cell_meta = reinterpret_cast<ObCellWriter::CellMeta*>(buf_ + pos_); \
      cell_meta->type_ = meta_type;                                                               \
      cell_meta->attr_ = meta_attr;                                                               \
      pos_ += sizeof(ObCellWriter::CellMeta);                                                     \
    }                                                                                             \
  }

#define SPARSE_WRITE_DATA(meta_type, meta_attr, store_type, value)                                \
  {                                                                                               \
    if (pos_ + sizeof(ObCellWriter::CellMeta) + sizeof(store_type) > buf_size_) {                 \
      ret = OB_BUF_NOT_ENOUGH;                                                                    \
    } else {                                                                                      \
      ObCellWriter::CellMeta* cell_meta = reinterpret_cast<ObCellWriter::CellMeta*>(buf_ + pos_); \
      cell_meta->type_ = meta_type;                                                               \
      cell_meta->attr_ = meta_attr;                                                               \
      pos_ += sizeof(ObCellWriter::CellMeta);                                                     \
      *(reinterpret_cast<store_type*>(buf_ + pos_)) = value;                                      \
      pos_ += sizeof(store_type);                                                                 \
    }                                                                                             \
  }

#define WRITE_NUMBER(TYPE)                    \
  if (OB_FAIL(obj.get_##TYPE(tmp_number_))) { \
  } else {                                    \
    ret = write_number(tmp_number_);          \
  }

int ObSparseCellWriter::write_int(const enum common::ObObjType meta_type, const int64_t value)
{
  int ret = OB_SUCCESS;
  int bytes = get_int_byte(value);
  if (pos_ + sizeof(ObCellWriter::CellMeta) + bytes > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    ObCellWriter::CellMeta* cell_meta = reinterpret_cast<ObCellWriter::CellMeta*>(buf_ + pos_);
    switch (bytes) {
      case 1:
        cell_meta->type_ = meta_type;
        cell_meta->attr_ = 0;
        pos_ += sizeof(ObCellWriter::CellMeta);
        *(reinterpret_cast<int8_t*>(buf_ + pos_)) = static_cast<int8_t>(value);
        pos_ += bytes;
        break;
      case 2:
        cell_meta->type_ = meta_type;
        cell_meta->attr_ = 1;
        pos_ += sizeof(ObCellWriter::CellMeta);
        *(reinterpret_cast<int16_t*>(buf_ + pos_)) = static_cast<int16_t>(value);
        pos_ += bytes;
        break;
      case 4:
        cell_meta->type_ = meta_type;
        cell_meta->attr_ = 2;
        pos_ += sizeof(ObCellWriter::CellMeta);
        *(reinterpret_cast<int32_t*>(buf_ + pos_)) = static_cast<int32_t>(value);
        pos_ += bytes;
        break;
      case 8:
        cell_meta->type_ = meta_type;
        cell_meta->attr_ = 3;
        pos_ += sizeof(ObCellWriter::CellMeta);
        *(reinterpret_cast<int64_t*>(buf_ + pos_)) = value;
        pos_ += bytes;
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "not supported type, ", K(ret), K(meta_type), K(value));
    }
  }
  return ret;
}

int ObSparseCellWriter::write_number(const enum common::ObObjType meta_type, const common::number::ObNumber& number)
{
  int ret = OB_SUCCESS;
  SPARSE_WRITE_META(meta_type, 0);
  if (OB_SUCC(ret)) {
    ret = number.encode(buf_, buf_size_, pos_);
  }
  return ret;
}

int ObSparseCellWriter::write_char(
    const ObObj& obj, const enum common::ObObjType meta_type, const common::ObString& str)
{
  int ret = OB_SUCCESS;
  uint32_t str_len = static_cast<uint32_t>(str.length());
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
    *(reinterpret_cast<ObCellWriter::CellMeta*>(buf_ + pos_)) = meta;
    pos_ += sizeof(ObCellWriter::CellMeta);

    if (1 == meta.attr_) {
      *(reinterpret_cast<uint8_t*>(buf_ + pos_)) = static_cast<uint8_t>(obj.get_collation_type());
      pos_ += sizeof(uint8_t);
    }
    *(reinterpret_cast<uint32_t*>(buf_ + pos_)) = str_len;
    pos_ += sizeof(uint32_t);

    MEMCPY(buf_ + pos_, str.ptr(), str_len);
    pos_ += str_len;
  }
  return ret;
}

int ObSparseCellWriter::write_text_store(const ObObj& obj)
{
  int ret = OB_SUCCESS;

  ObLobScale lob_scale(obj.get_scale());
  ObCellWriter::CellMeta meta;
  meta.type_ = ObCellWriter::CellMeta::SF_MASK_TYPE & static_cast<uint8_t>(obj.get_type());

  // defend code, fix the scale
  if (!lob_scale.is_valid()) {
    lob_scale.set_in_row();
    STORAGE_LOG(WARN, "[LOB] unexpected error, invalid lob obj scale", K(ret), K(obj), K(obj.get_scale()));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append<ObCellWriter::CellMeta>(meta))) {
      STORAGE_LOG(WARN, "fail to append cell meta", K(ret));
    } else if (OB_FAIL(append<uint8_t>(static_cast<uint8_t>(obj.get_collation_type())))) {
      STORAGE_LOG(WARN, "fail to append collation", K(ret));
    } else if (OB_FAIL(append<uint8_t>(static_cast<uint8_t>(lob_scale.get_scale())))) {
      STORAGE_LOG(WARN, "fail to append obj scale", K(ret));
    } else if (OB_FAIL(append<uint8_t>(static_cast<uint8_t>(storage::STORE_TEXT_STORE_VERSION)))) {
      STORAGE_LOG(WARN, "fail to append text store version", K(ret));
    } else if (lob_scale.is_in_row()) {
      ObString char_value;
      if (OB_FAIL(obj.get_string(char_value))) {
        STORAGE_LOG(WARN, "fail to get inrow lob obj value", K(obj), K(ret));
      } else if (char_value.length() > ObAccuracy::MAX_ACCURACY[obj.get_type()].get_length()) {
        ret = OB_SIZE_OVERFLOW;
        STORAGE_LOG(WARN, "text value is overflow.", "length", char_value.length(), K(ret));
      } else if (OB_FAIL(append<uint32_t>(static_cast<uint32_t>(char_value.length())))) {
        STORAGE_LOG(WARN, "failed to append length", "length", char_value.length(), K(ret));
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
  }
  return ret;
}

int ObSparseCellWriter::write_binary(const enum ObObjType meta_type, const ObString& str)
{
  int ret = OB_SUCCESS;
  uint32_t str_len = static_cast<uint32_t>(str.length());
  if (pos_ + sizeof(ObCellWriter::CellMeta) + sizeof(uint32_t) + str_len > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    ObCellWriter::CellMeta* meta = reinterpret_cast<ObCellWriter::CellMeta*>(buf_ + pos_);
    meta->type_ = meta_type;

    meta->attr_ = 0;
    pos_ += sizeof(ObCellWriter::CellMeta);

    *(reinterpret_cast<uint32_t*>(buf_ + pos_)) = str_len;
    pos_ += sizeof(uint32_t);

    MEMCPY(buf_ + pos_, str.ptr(), str_len);
    pos_ += str_len;
  }
  return ret;
}

int ObSparseCellWriter::write_oracle_timestamp(const ObObj& obj, const common::ObOTimestampMetaAttrType otmat)
{
  int ret = OB_SUCCESS;
  ObCellWriter::CellMeta meta;
  meta.type_ = (static_cast<uint8_t>(obj.get_type()) & 0x3F);
  meta.attr_ = (static_cast<uint8_t>(otmat) & 0x03);
  const ObOTimestampData& ot_data = obj.get_otimestamp_value();
  if (OB_FAIL(append<ObCellWriter::CellMeta>(meta))) {
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

int ObSparseCellWriter::write_interval_ds(const ObObj& obj)
{
  int ret = OB_SUCCESS;
  ObCellWriter::CellMeta meta;
  meta.type_ = (static_cast<uint8_t>(obj.get_type()) & 0x3F);
  meta.attr_ = 0;
  ObIntervalDSValue value = obj.get_interval_ds();
  if (OB_FAIL(append<ObCellWriter::CellMeta>(meta))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      COMMON_LOG(WARN, "fail to append CellMeta, ", K(ret), "type", meta.type_, "attr", meta.attr_);
    }
  } else if (OB_FAIL(append<int64_t>(value.nsecond_))) {
  } else if (OB_FAIL(append<int32_t>(value.fractional_second_))) {
  }
  return ret;
}

int ObSparseCellWriter::write_interval_ym(const ObObj& obj)
{
  int ret = OB_SUCCESS;
  ObCellWriter::CellMeta meta;
  meta.type_ = (static_cast<uint8_t>(obj.get_type()) & 0x3F);
  meta.attr_ = 0;
  ObIntervalYMValue value = obj.get_interval_ym();
  if (OB_FAIL(append<ObCellWriter::CellMeta>(meta))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      COMMON_LOG(WARN, "fail to append CellMeta, ", K(ret), "type", meta.type_, "attr", meta.attr_);
    }
  } else if (OB_FAIL(append<int64_t>(value.nmonth_))) {
  }
  return ret;
}

int ObSparseCellWriter::write_ext(const int64_t ext_value)
{
  int ret = OB_SUCCESS;
  switch (ext_value) {
    case ObActionFlag::OP_END_FLAG:
      SPARSE_WRITE_META(ObExtendType, ObCellWriter::EA_END_FLAG);
      break;
    case ObActionFlag::OP_NOP:
      SPARSE_WRITE_DATA(ObExtendType, ObCellWriter::EA_OTHER, int8_t, ObCellWriter::EV_NOP_ROW);
      break;
    case ObActionFlag::OP_DEL_ROW:
      SPARSE_WRITE_DATA(ObExtendType, ObCellWriter::EA_OTHER, int8_t, ObCellWriter::EV_DEL_ROW);
      break;
    case ObActionFlag::OP_LOCK_ROW:
      SPARSE_WRITE_DATA(ObExtendType, ObCellWriter::EA_OTHER, int8_t, ObCellWriter::EV_LOCK_ROW);
      break;
    case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
      SPARSE_WRITE_DATA(ObExtendType, ObCellWriter::EA_OTHER, int8_t, ObCellWriter::EV_NOT_EXIST_ROW);
      break;
    case ObObj::MIN_OBJECT_VALUE:
      SPARSE_WRITE_DATA(ObExtendType, ObCellWriter::EA_OTHER, int8_t, ObCellWriter::EV_MIN_CELL);
      break;
    case ObObj::MAX_OBJECT_VALUE:
      SPARSE_WRITE_DATA(ObExtendType, ObCellWriter::EA_OTHER, int8_t, ObCellWriter::EV_MAX_CELL);
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WARN, "invalid extend type.", K(ret), K(ext_value));
  }
  return ret;
}

ObSparseCellWriter::ObSparseCellWriter()
    : buf_(NULL), buf_size_(0), pos_(0), last_append_pos_(0), cell_cnt_(0), old_text_format_(false), is_inited_(false)
{}

int ObSparseCellWriter::init(char* buf, int64_t size, const bool old_text_format)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "ObSparseCellWriter has be inited, ", K(ret));
  } else if (NULL == buf || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid cell writer input argument.", K(ret), KP(buf), K(size));
  } else {
    buf_ = buf;
    buf_size_ = size;
    pos_ = 0;
    cell_cnt_ = 0;
    old_text_format_ = old_text_format;
    is_inited_ = true;
  }
  return ret;
}

int ObSparseCellWriter::append(const ObObj& obj)
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = obj.get_type();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObSparseCellWriter should be inited first, ", K(ret));
  } else if (OB_UNLIKELY(!obj.is_valid_type())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument, ", K(ret), "type", obj.get_type());
  } else {
    switch (obj_type) {
      case ObNullType:
        SPARSE_WRITE_META(ObNullType, 0);
        break;
      case ObTinyIntType:
        ret = write_int(ObTinyIntType, obj.get_tinyint());
        break;
      case ObSmallIntType:
        ret = write_int(ObSmallIntType, obj.get_smallint());
        break;
      case ObMediumIntType:
        ret = write_int(ObMediumIntType, obj.get_mediumint());
        break;
      case ObInt32Type:
        ret = write_int(ObInt32Type, obj.get_int32());
        break;
      case ObIntType:
        ret = write_int(ObIntType, obj.get_int());
        break;
      case ObUTinyIntType:
        ret = write_int(ObUTinyIntType, obj.get_utinyint());
        break;
      case ObUSmallIntType:
        ret = write_int(ObUSmallIntType, obj.get_usmallint());
        break;
      case ObUMediumIntType:
        ret = write_int(ObUMediumIntType, obj.get_umediumint());
        break;
      case ObUInt32Type:
        ret = write_int(ObUInt32Type, obj.get_uint32());
        break;
      case ObUInt64Type:
        ret = write_int(ObUInt64Type, obj.get_uint64());
        break;
      case ObFloatType:
        SPARSE_WRITE_DATA(ObFloatType, 0, float, obj.get_float());
        break;
      case ObDoubleType:
        SPARSE_WRITE_DATA(ObDoubleType, 0, double, obj.get_double());
        break;
      case ObUFloatType:
        SPARSE_WRITE_DATA(ObUFloatType, 0, float, obj.get_ufloat());
        break;
      case ObUDoubleType:
        SPARSE_WRITE_DATA(ObUDoubleType, 0, double, obj.get_udouble());
        break;
      case ObNumberType:
        ret = write_number(ObNumberType, obj.get_number());
        break;
      case ObUNumberType:
        ret = write_number(ObUNumberType, obj.get_unumber());
        break;
      case ObDateType:
        SPARSE_WRITE_DATA(ObDateType, 0, int32_t, obj.get_date());
        break;
      case ObDateTimeType:
        SPARSE_WRITE_DATA(ObDateTimeType, 0, int64_t, obj.get_datetime());
        break;
      case ObTimestampType:
        SPARSE_WRITE_DATA(ObTimestampType, 0, int64_t, obj.get_timestamp());
        break;
      case ObTimeType:
        SPARSE_WRITE_DATA(ObTimeType, 0, int64_t, obj.get_time());
        break;
      case ObYearType:
        SPARSE_WRITE_DATA(ObYearType, 0, uint8_t, obj.get_year());
        break;
      case ObVarcharType:
        ret = write_char(obj, ObVarcharType, obj.get_string());
        break;
      case ObCharType:
        ret = write_char(obj, ObCharType, obj.get_string());
        break;
      case ObRawType:
        ret = write_char(obj, ObRawType, obj.get_raw());
        break;
      case ObNVarchar2Type:
        ret = write_char(obj, ObNVarchar2Type, obj.get_nvarchar2());
        break;
      case ObNCharType:
        ret = write_char(obj, ObNCharType, obj.get_nchar());
        break;
      case ObHexStringType:
        ret = write_binary(ObHexStringType, obj.get_hex_string());
        break;
      case ObNumberFloatType:
        ret = write_number(ObNumberFloatType, obj.get_unumber());
        break;
      case ObExtendType:
        ret = write_ext(obj.get_ext());
        break;
      // TODO texttc share with varchar temporarily
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType: {
        ret = write_text_store(obj);
        break;
      }
      case ObBitType:
        ret = write_int(ObBitType, obj.get_bit());
        break;
      case ObEnumType:
        ret = write_int(ObEnumType, obj.get_enum());
        break;
      case ObSetType:
        ret = write_int(ObSetType, obj.get_set());
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
      case ObIntervalYMType:
        ret = write_interval_ym(obj);
        break;
      case ObIntervalDSType:
        ret = write_interval_ds(obj);
        break;
      case ObURowIDType: {
        ObCellWriter::CellMeta meta;
        meta.type_ = ObURowIDType;
        if (OB_FAIL(append<ObCellWriter::CellMeta>(meta))) {
          if (OB_BUF_NOT_ENOUGH != ret) {
            COMMON_LOG(WARN, "fail to append CellMeta, ", K(ret), "type", meta.type_, "attr", meta.attr_);
          }
        } else if (OB_FAIL(append<uint32_t>(static_cast<uint32_t>(obj.get_string_len())))) {
          STORAGE_LOG(WARN, "failed to append str len", K(ret));
        } else {
          MEMCPY(buf_ + pos_, obj.get_string_ptr(), obj.get_string_len());
          pos_ += obj.get_string_len();
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "cell writer don't support the data type.", K(ret), "type", obj.get_type());
    }
    if (OB_SUCC(ret)) {
      // Save the last_append_pos site for column append retry
      ++cell_cnt_;
      last_append_pos_ = pos_;
    } else {
      // Rollback the pos position modified by the column append to facilitate subsequent retry
      pos_ = last_append_pos_;
    }
  }
  return ret;
}

void ObSparseCellWriter::reset()
{
  buf_ = NULL;
  buf_size_ = 0;
  pos_ = 0;
  last_append_pos_ = 0;
  cell_cnt_ = 0;
  old_text_format_ = false;
  is_inited_ = false;
}

template <class T>
int ObSparseCellWriter::append(const T& value)
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

inline int ObSparseCellWriter::get_int_byte(int64_t int_value)
{
  int bytes = 0;
  if (int_value >= -128 && int_value <= 127) {
    bytes = 1;
  } else if (int_value >= static_cast<int16_t>(0x8000) && int_value <= static_cast<int16_t>(0x7FFF)) {
    bytes = 2;
  } else if (int_value >= static_cast<int32_t>(0x80000000) && int_value <= static_cast<int32_t>(0x7FFFFFFF)) {
    bytes = 4;
  } else {
    bytes = 8;
  }
  return bytes;
}
