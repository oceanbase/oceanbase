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

#include "ob_sparse_cell_reader.h"
#include "common/cell/ob_cell_writer.h"
#include "storage/ob_i_store.h"

namespace oceanbase {
namespace blocksstable {
#define READ_NULL(object) \
  {                       \
    object.set_null();    \
  }
#define READ_COMMON(obj_set_fun, obj_set_type, medium_type, object) \
  {                                                                 \
    const medium_type* value = 0;                                   \
    if (OB_FAIL(read<medium_type>(value))) {                        \
      COMMON_LOG(WARN, "cell reader fail to read value.", K(ret));  \
    } else {                                                        \
      object.obj_set_fun(static_cast<obj_set_type>(*value));        \
    }                                                               \
  }
#define READ_INT(obj_set_fun, obj_set_type, object)                            \
  {                                                                            \
    switch (meta->attr_) {                                                     \
      case 0:                                                                  \
        READ_COMMON(obj_set_fun, obj_set_type, int8_t, object);                \
        break;                                                                 \
      case 1:                                                                  \
        READ_COMMON(obj_set_fun, obj_set_type, int16_t, object);               \
        break;                                                                 \
      case 2:                                                                  \
        READ_COMMON(obj_set_fun, obj_set_type, int32_t, object);               \
        break;                                                                 \
      case 3:                                                                  \
        READ_COMMON(obj_set_fun, obj_set_type, int64_t, object);               \
        break;                                                                 \
      default:                                                                 \
        ret = OB_NOT_SUPPORTED;                                                \
        COMMON_LOG(WARN, "not supported type, ", K(ret), "attr", meta->attr_); \
    }                                                                          \
  }
#define READ_NUMBER(obj_set_fun, obj_set_type, object)  \
  {                                                     \
    number::ObNumber value;                             \
    if (OB_FAIL(value.decode(buf_, buf_size_, pos_))) { \
      COMMON_LOG(WARN, "decode number failed", K(ret)); \
    } else {                                            \
      object.obj_set_fun(value);                        \
    }                                                   \
  }

#define READ_CHAR(obj_set_fun, object)                                                          \
  {                                                                                             \
    const uint32_t* len = NULL;                                                                 \
    ObString value;                                                                             \
    if (0 == meta->attr_) {                                                                     \
      object.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);                                    \
    } else if (1 == meta->attr_) {                                                              \
      const uint8_t* collation_type = NULL;                                                     \
      if (OB_FAIL(read<uint8_t>(collation_type))) {                                             \
        COMMON_LOG(WARN, "row reader fail to read collation_type.", K(ret));                    \
      } else {                                                                                  \
        object.set_collation_type(static_cast<ObCollationType>(*collation_type));               \
      }                                                                                         \
    }                                                                                           \
    if (OB_SUCC(ret)) {                                                                         \
      if (OB_FAIL(read<uint32_t>(len))) {                                                       \
        COMMON_LOG(WARN, "row reader fail to read value.", K(ret));                             \
      } else if (pos_ + *len > buf_size_) {                                                     \
        ret = OB_BUF_NOT_ENOUGH;                                                                \
        COMMON_LOG(WARN, "buffer not enough, ", K(ret), K_(pos), K_(buf_size), "length", *len); \
      } else {                                                                                  \
        value.assign_ptr((char*)(buf_ + pos_), *len);                                           \
        pos_ += *len;                                                                           \
        object.obj_set_fun(value);                                                              \
      }                                                                                         \
    }                                                                                           \
  }

OB_INLINE int ObSparseCellReader::read_interval_ds(ObObj& obj)
{
  int ret = OB_SUCCESS;
  const int64_t* nsecond = NULL;
  const int32_t* fractional_second = NULL;
  const int64_t value_len = ObIntervalDSValue::get_store_size();
  if (OB_UNLIKELY(pos_ + value_len > buf_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WARN, "buffer not enough, ", K(ret), K_(pos), K_(buf_size), K(value_len));
  } else if (OB_FAIL(read<int64_t>(nsecond))) {
    COMMON_LOG(WARN, "cell reader fail to time_us.", K(ret));
  } else if (OB_FAIL(read<int32_t>(fractional_second))) {
    COMMON_LOG(WARN, "cell reader fail to time_us.", K(ret));
  } else {
    obj.set_interval_ds(ObIntervalDSValue(*nsecond, *fractional_second));
  }
  return ret;
}

OB_INLINE int ObSparseCellReader::read_interval_ym(ObObj& obj)
{
  int ret = OB_SUCCESS;
  const int64_t* nmonth = NULL;
  const int64_t value_len = ObIntervalYMValue::get_store_size();
  if (OB_UNLIKELY(pos_ + value_len > buf_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WARN, "buffer not enough, ", K(ret), K_(pos), K_(buf_size), K(value_len));
  } else if (OB_FAIL(read<int64_t>(nmonth))) {
    COMMON_LOG(WARN, "cell reader fail to time_us.", K(ret));
  } else {
    obj.set_interval_ym(ObIntervalYMValue(*nmonth));
  }
  return ret;
}

int ObSparseCellReader::read_oracle_timestamp(
    const ObObjType obj_type, const uint8_t meta_attr, const common::ObOTimestampMetaAttrType otmat, ObObj& obj)
{
  int ret = OB_SUCCESS;
  const int64_t* time_us = NULL;
  const uint32_t* time_ctx_desc = NULL;
  const uint16_t* time_desc = NULL;
  const uint8_t expect_attr = static_cast<uint8_t>(otmat);
  const int64_t length = ObObj::get_otimestamp_store_size(common::OTMAT_TIMESTAMP_TZ == otmat);
  if (OB_UNLIKELY(expect_attr != meta_attr)) {
    ret = OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "not support this attr, ", K(ret), K(expect_attr), K(meta_attr));
  } else if (OB_UNLIKELY(pos_ + length > buf_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WARN, "buffer not enough, ", K(ret), K_(pos), K_(buf_size), K(length));
  } else if (OB_FAIL(read<int64_t>(time_us))) {
    COMMON_LOG(WARN, "cell reader fail to time_us.", K(ret));
  } else if (common::OTMAT_TIMESTAMP_TZ == otmat) {
    if (OB_FAIL(read<uint32_t>(time_ctx_desc))) {
      COMMON_LOG(WARN, "cell reader fail to read time_ctx_desc.", K(ret));
    } else {
      obj.set_otimestamp_value(obj_type, *time_us, *time_ctx_desc);
    }
  } else {
    if (OB_FAIL(read<uint16_t>(time_desc))) {
      COMMON_LOG(WARN, "cell reader fail to read time_desc.", K(ret));
    } else {
      obj.set_otimestamp_value(obj_type, *time_us, *time_desc);
    }
  }
  return ret;
}

#define READ_BINARY(obj_set_fun, object)                                                      \
  {                                                                                           \
    const int32_t* len = NULL;                                                                \
    ObString value;                                                                           \
    if (OB_FAIL(read<int32_t>(len))) {                                                        \
      COMMON_LOG(WARN, "row reader fail to read value.", K(ret));                             \
    } else if (pos_ + *len > buf_size_) {                                                     \
      ret = OB_BUF_NOT_ENOUGH;                                                                \
      COMMON_LOG(WARN, "buffer not enough, ", K(ret), K_(pos), K_(buf_size), "length", *len); \
    } else {                                                                                  \
      value.assign_ptr((char*)(buf_ + pos_), *len);                                           \
      pos_ += *len;                                                                           \
      object.obj_set_fun(value);                                                              \
    }                                                                                         \
  }
#define READ_EXTEND(object)                                                  \
  {                                                                          \
    if (ObCellWriter::EA_END_FLAG == meta->attr_) {                          \
      object.set_ext(ObActionFlag::OP_END_FLAG);                             \
    } else if (ObCellWriter::EA_OTHER == meta->attr_) {                      \
      const int8_t* value = NULL;                                            \
      if (OB_FAIL(read<int8_t>(value))) {                                    \
        COMMON_LOG(WARN, "cell reader fail to read extend value.", K(ret));  \
      } else {                                                               \
        switch (*value) {                                                    \
          case ObCellWriter::EV_NOP_ROW:                                     \
            object.set_ext(ObActionFlag::OP_NOP);                            \
            break;                                                           \
          case ObCellWriter::EV_DEL_ROW:                                     \
            object.set_ext(ObActionFlag::OP_DEL_ROW);                        \
            break;                                                           \
          case ObCellWriter::EV_LOCK_ROW:                                    \
            obj_.set_ext(ObActionFlag::OP_LOCK_ROW);                         \
            break;                                                           \
          case ObCellWriter::EV_NOT_EXIST_ROW:                               \
            object.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);             \
            break;                                                           \
          case ObCellWriter::EV_MIN_CELL:                                    \
            object.set_min_value();                                          \
            break;                                                           \
          case ObCellWriter::EV_MAX_CELL:                                    \
            object.set_max_value();                                          \
            break;                                                           \
          default:                                                           \
            ret = OB_NOT_SUPPORTED;                                          \
            COMMON_LOG(WARN, "invalid extend value.", K(ret), K(value));     \
        }                                                                    \
      }                                                                      \
    } else {                                                                 \
      ret = OB_NOT_SUPPORTED;                                                \
      COMMON_LOG(WARN, "invalid extend attr.", K(ret), "attr", meta->attr_); \
    }                                                                        \
  }

ObSparseCellReader::ObSparseCellReader()
    : buf_(NULL), buf_size_(0), pos_(0), column_id_(0), obj_(), row_start_(0), is_inited_(false), allocator_(NULL)
{}

int ObSparseCellReader::init(const char* buf, int64_t buf_size, common::ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "ObSparseCellReader has inited, ", K(ret));
  } else if (NULL == buf || buf_size <= 0 || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid cell reader input argeument.", K(ret), KP(buf), K(buf_size), K(allocator));
  } else {
    buf_ = buf;
    buf_size_ = buf_size;
    pos_ = 0;
    column_id_ = 0;
    row_start_ = 0;
    allocator_ = allocator;
    is_inited_ = true;
  }
  return ret;
}

void ObSparseCellReader::reset()
{
  buf_ = NULL;
  buf_size_ = 0;
  pos_ = 0;
  column_id_ = 0;
  row_start_ = 0;
  allocator_ = NULL;
  is_inited_ = false;
}

int ObSparseCellReader::read_text_store(const ObCellWriter::CellMeta& cell_meta, ObObj& obj)
{
  int ret = OB_SUCCESS;
  obj.set_collation_type(static_cast<ObCollationType>(*read<uint8_t>(buf_, pos_)));
  const uint8_t* scale = read<uint8_t>(buf_, pos_);
  const uint8_t* version = read<uint8_t>(buf_, pos_);
  const ObObjType store_type = static_cast<ObObjType>(cell_meta.type_);
  ObLobScale lob_scale(*scale);
  if (storage::STORE_TEXT_STORE_VERSION != *version) {
    STORAGE_LOG(WARN, "[LOB] unexpected text store version", K(*version), K(ret));
  }
  if (lob_scale.is_in_row()) {
    const uint32_t* len = read<uint32_t>(buf_, pos_);
    if (pos_ + *len > buf_size_) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buf is not large", K(ret), K(pos_), K(*len), K(buf_size_));
    } else {
      obj.set_lob_value(store_type, (char*)(buf_ + pos_), *len);
      pos_ += *len;
    }
  } else if (lob_scale.is_out_row()) {
    void* buf = NULL;
    ObLobData* value = NULL;
    if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObLobData)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory for ObLobData", K(ret));
    } else {
      value = new (buf) ObLobData();
      if (OB_FAIL(value->deserialize(buf_, buf_size_, pos_))) {
        STORAGE_LOG(WARN, "fail to deserialize lob data", K(ret));
      } else {
        obj.set_lob_value(store_type, value, value->get_handle_size());
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
      allocator_->free(buf);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, invalid obj type", K(ret), K(obj));
  }

  if (OB_SUCC(ret)) {
    if (0 == obj.get_val_len() && ObTextTC == ob_obj_type_class(store_type)) {
      obj.set_lob_inrow();
    }
  }

  return ret;
}

template <class T>
int ObSparseCellReader::read(const T*& ptr)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObSparseCellReader should be inited first, ", K(ret));
  } else if (pos_ + (static_cast<int64_t>(sizeof(T))) > buf_size_) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WARN, "buffer not enough, ", K(ret), K(buf_size_), K(pos_));
  } else {
    ptr = reinterpret_cast<const T*>(buf_ + pos_);
    pos_ += sizeof(T);
  }
  return ret;
}

int ObSparseCellReader::read_cell(common::ObObj& obj)
{
  int ret = OB_SUCCESS;
  const ObCellWriter::CellMeta* meta = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObSparseCellReader should be inited first, ", K(ret));
  } else if (OB_FAIL(read<ObCellWriter::CellMeta>(meta))) {
    COMMON_LOG(WARN, "cell reader fail to read meta, ", K(ret));
  } else {
    obj.meta_.reset();
    switch (meta->type_) {
      case ObNullType:
        READ_NULL(obj);
        break;
      case ObTinyIntType:
        READ_INT(set_tinyint, int8_t, obj);
        break;
      case ObSmallIntType:
        READ_INT(set_smallint, int16_t, obj);
        break;
      case ObMediumIntType:
        READ_INT(set_mediumint, int32_t, obj);
        break;
      case ObInt32Type:
        READ_INT(set_int32, int32_t, obj);
        break;
      case ObIntType:
        READ_INT(set_int, int64_t, obj);
        break;
      case ObUTinyIntType:
        READ_INT(set_utinyint, uint8_t, obj);
        break;
      case ObUSmallIntType:
        READ_INT(set_usmallint, uint16_t, obj);
        break;
      case ObUMediumIntType:
        READ_INT(set_umediumint, uint32_t, obj);
        break;
      case ObUInt32Type:
        READ_INT(set_uint32, uint32_t, obj);
        break;
      case ObUInt64Type:
        READ_INT(set_uint64, uint64_t, obj);
        break;
      case ObFloatType:
        READ_COMMON(set_float, float, float, obj);
        break;
      case ObUFloatType:
        READ_COMMON(set_ufloat, float, float, obj);
        break;
      case ObDoubleType:
        READ_COMMON(set_double, double, double, obj);
        break;
      case ObUDoubleType:
        READ_COMMON(set_udouble, double, double, obj);
        break;
      case ObNumberType:
        READ_NUMBER(set_number, ObNumber, obj);
        break;
      case ObUNumberType:
        READ_NUMBER(set_unumber, ObNumber, obj);
        break;
      case ObDateTimeType:
        READ_COMMON(set_datetime, int64_t, int64_t, obj);
        break;
      case ObTimestampType:
        READ_COMMON(set_timestamp, int64_t, int64_t, obj);
        break;
      case ObDateType:
        READ_COMMON(set_date, int32_t, int32_t, obj);
        break;
      case ObTimeType:
        READ_COMMON(set_time, int64_t, int64_t, obj);
        break;
      case ObYearType:
        READ_COMMON(set_year, uint8_t, uint8_t, obj);
        break;
      case ObVarcharType:
        READ_CHAR(set_varchar, obj);
        break;
      case ObCharType:
        READ_CHAR(set_char, obj);
        break;
      case ObNVarchar2Type:
        READ_CHAR(set_nvarchar2, obj);
        break;
      case ObNCharType:
        READ_CHAR(set_nchar, obj);
        break;
      case ObHexStringType:
        READ_BINARY(set_hex_string, obj);
        break;
      case ObExtendType:
        READ_EXTEND(obj);
        break;
      case ObTinyTextType:
      case ObTextType:
      case ObMediumTextType:
      case ObLongTextType:
        ret = read_text_store(*meta, obj);
        break;
      case ObBitType:
        READ_INT(set_bit, uint64_t, obj);
        break;
      case ObEnumType:
        READ_INT(set_enum, uint64_t, obj);
        break;
      case ObSetType:
        READ_INT(set_set, uint64_t, obj);
        break;
      case ObTimestampTZType:
        ret = read_oracle_timestamp(ObTimestampTZType, meta->attr_, common::OTMAT_TIMESTAMP_TZ, obj);
        break;
      case ObTimestampLTZType:
        ret = read_oracle_timestamp(ObTimestampLTZType, meta->attr_, common::OTMAT_TIMESTAMP_LTZ, obj);
        break;
      case ObTimestampNanoType:
        ret = read_oracle_timestamp(ObTimestampNanoType, meta->attr_, common::OTMAT_TIMESTAMP_NANO, obj);
        break;
      case ObRawType:
        READ_CHAR(set_raw, obj);
        break;
      case ObIntervalYMType:
        ret = read_interval_ym(obj);
        break;
      case ObIntervalDSType:
        ret = read_interval_ds(obj);
        break;
      case ObNumberFloatType:
        READ_NUMBER(set_number_float, ObNumber, obj);
        break;
      case ObURowIDType: {
        const uint32_t* len = read<uint32_t>(buf_, pos_);
        if (OB_UNLIKELY(pos_ + *len > buf_size_)) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(WARN, "buf not enough, ", K(ret), K(pos_), K(*len), K(buf_size_));
        } else {
          obj.set_urowid((char*)(buf_ + pos_), *len);
          pos_ += *len;
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        COMMON_LOG(WARN, "not supported type.", K(ret), "type", meta->type_);
        break;
    }
  }
  return ret;
}

}  // namespace blocksstable
}  // end namespace oceanbase
