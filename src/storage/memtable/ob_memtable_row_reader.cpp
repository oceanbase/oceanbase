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

#include "storage/memtable/ob_memtable_row_reader.h"
#include "share/config/ob_server_config.h"
#include "common/cell/ob_cell_writer.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace memtable {
#define READ_NULL()  \
  {                  \
    obj_.set_null(); \
  }
#define READ_COMMON(obj_set_fun, obj_set_type, medium_type)            \
  {                                                                    \
    obj_.obj_set_fun(static_cast<obj_set_type>(*read<medium_type>())); \
  }
#define READ_INT(obj_set_fun, obj_set_type)                                   \
  {                                                                           \
    switch (meta->attr_) {                                                    \
      case 0:                                                                 \
        READ_COMMON(obj_set_fun, obj_set_type, int8_t);                       \
        break;                                                                \
      case 1:                                                                 \
        READ_COMMON(obj_set_fun, obj_set_type, int16_t);                      \
        break;                                                                \
      case 2:                                                                 \
        READ_COMMON(obj_set_fun, obj_set_type, int32_t);                      \
        break;                                                                \
      case 3:                                                                 \
        READ_COMMON(obj_set_fun, obj_set_type, int64_t);                      \
        break;                                                                \
      default:                                                                \
        ret = OB_NOT_SUPPORTED;                                               \
        TRANS_LOG(WARN, "not supported type, ", K(ret), "attr", meta->attr_); \
    }                                                                         \
  }
#define READ_NUMBER(obj_set_fun)                        \
  {                                                     \
    number::ObNumber value;                             \
    if (OB_FAIL(value.decode(buf_, buf_size_, pos_))) { \
      COMMON_LOG(WARN, "decode number failed", K(ret)); \
    } else {                                            \
      obj_.obj_set_fun(value);                          \
    }                                                   \
  }

#define READ_CHAR(obj_set_fun)                                                                 \
  {                                                                                            \
    if (0 == meta->attr_) {                                                                    \
      obj_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);                                     \
    } else if (1 == meta->attr_) {                                                             \
      const uint8_t* collation_type = read<uint8_t>();                                         \
      obj_.set_collation_type(static_cast<ObCollationType>(*collation_type));                  \
    }                                                                                          \
    if (OB_SUCC(ret)) {                                                                        \
      const uint32_t* len = read<uint32_t>();                                                  \
      if (pos_ + *len > buf_size_) {                                                           \
        ret = OB_BUF_NOT_ENOUGH;                                                               \
        TRANS_LOG(WARN, "buffer not enough, ", K(ret), K_(pos), K_(buf_size), "length", *len); \
      } else {                                                                                 \
        obj_.obj_set_fun((char*)(buf_ + pos_), *len);                                          \
        pos_ += *len;                                                                          \
      }                                                                                        \
    }                                                                                          \
  }
#define READ_CHAR_WITH_META(obj_set_fun)                                                       \
  {                                                                                            \
    if (0 == meta->attr_) {                                                                    \
      obj_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);                                     \
    } else if (1 == meta->attr_) {                                                             \
      const uint8_t* collation_type = read<uint8_t>();                                         \
      obj_.set_collation_type(static_cast<ObCollationType>(*collation_type));                  \
    }                                                                                          \
    if (OB_SUCC(ret)) {                                                                        \
      const uint32_t* len = read<uint32_t>();                                                  \
      if (pos_ + *len > buf_size_) {                                                           \
        ret = OB_BUF_NOT_ENOUGH;                                                               \
        TRANS_LOG(WARN, "buffer not enough, ", K(ret), K_(pos), K_(buf_size), "length", *len); \
      } else {                                                                                 \
        obj_.obj_set_fun(ObString(*len, (char*)(buf_ + pos_)));                                \
        pos_ += *len;                                                                          \
      }                                                                                        \
    }                                                                                          \
  }
#define READ_TEXT(obj_type)                                                                               \
  {                                                                                                       \
    if (!meta->need_collation()) {                                                                        \
      obj_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);                                                \
    } else {                                                                                              \
      const uint8_t* collation_type = read<uint8_t>();                                                    \
      obj_.set_collation_type(static_cast<ObCollationType>(*collation_type));                             \
    }                                                                                                     \
    if (OB_SUCC(ret) && !meta->is_varchar_text()) {                                                       \
      const uint8_t* scale = read<uint8_t>();                                                             \
      const uint8_t* version = read<uint8_t>();                                                           \
      ObLobScale lob_scale(*scale);                                                                       \
      if (!lob_scale.is_in_row()) {                                                                       \
        ret = OB_ERR_UNEXPECTED;                                                                          \
        TRANS_LOG(WARN, "[LOB] Unexpected outrow lob object in memtable", K_(obj), K(lob_scale), K(ret)); \
      }                                                                                                   \
      if (TEXT_CELL_META_VERSION != *version) {                                                           \
        TRANS_LOG(WARN, "Unexpected lob version", K(*version), K(lob_scale), K(ret));                     \
      }                                                                                                   \
    }                                                                                                     \
    if (OB_SUCC(ret)) {                                                                                   \
      const uint32_t* len = read<uint32_t>();                                                             \
      if (pos_ + *len > buf_size_) {                                                                      \
        ret = OB_BUF_NOT_ENOUGH;                                                                          \
        TRANS_LOG(WARN, "buffer not enough", K(ret), K_(pos), K_(buf_size), "length", *len);              \
      } else {                                                                                            \
        obj_.set_lob_value(static_cast<ObObjType>(obj_type), (char*)(buf_ + pos_), *len);                 \
        pos_ += *len;                                                                                     \
      }                                                                                                   \
    }                                                                                                     \
  }
#define READ_BINARY(obj_set_fun)                                                             \
  {                                                                                          \
    const int32_t* len = read<int32_t>();                                                    \
    if (pos_ + *len > buf_size_) {                                                           \
      ret = OB_BUF_NOT_ENOUGH;                                                               \
      TRANS_LOG(WARN, "buffer not enough, ", K(ret), K_(pos), K_(buf_size), "length", *len); \
    } else {                                                                                 \
      obj_.obj_set_fun((char*)(buf_ + pos_), *len);                                          \
      pos_ += *len;                                                                          \
    }                                                                                        \
  }
#define READ_BINARY_WITH_META(obj_set_fun)                                                   \
  {                                                                                          \
    const int32_t* len = read<int32_t>();                                                    \
    if (pos_ + *len > buf_size_) {                                                           \
      ret = OB_BUF_NOT_ENOUGH;                                                               \
      TRANS_LOG(WARN, "buffer not enough, ", K(ret), K_(pos), K_(buf_size), "length", *len); \
    } else {                                                                                 \
      obj_.obj_set_fun(ObString(*len, (char*)(buf_ + pos_)));                                \
      pos_ += *len;                                                                          \
    }                                                                                        \
  }
#define READ_EXTEND()                                                       \
  {                                                                         \
    if (ObCellWriter::EA_END_FLAG == meta->attr_) {                         \
      obj_.set_ext(ObActionFlag::OP_END_FLAG);                              \
    } else if (ObCellWriter::EA_OTHER == meta->attr_) {                     \
      const int8_t* value = read<int8_t>();                                 \
      switch (*value) {                                                     \
        case ObCellWriter::EV_NOP_ROW:                                      \
          obj_.set_ext(ObActionFlag::OP_NOP);                               \
          break;                                                            \
        case ObCellWriter::EV_DEL_ROW:                                      \
          obj_.set_ext(ObActionFlag::OP_DEL_ROW);                           \
          break;                                                            \
        case ObCellWriter::EV_NOT_EXIST_ROW:                                \
          obj_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);                \
          break;                                                            \
        case ObCellWriter::EV_MIN_CELL:                                     \
          obj_.set_min_value();                                             \
          break;                                                            \
        case ObCellWriter::EV_MAX_CELL:                                     \
          obj_.set_max_value();                                             \
          break;                                                            \
        default:                                                            \
          ret = OB_NOT_SUPPORTED;                                           \
          TRANS_LOG(WARN, "invalid extend value.", K(ret), K(value));       \
      }                                                                     \
    } else {                                                                \
      ret = OB_NOT_SUPPORTED;                                               \
      TRANS_LOG(WARN, "invalid extend attr.", K(ret), "attr", meta->attr_); \
    }                                                                       \
  }
#define READ_EXTEND_WITH_META()                                             \
  {                                                                         \
    if (ObCellWriter::EA_END_FLAG == meta->attr_) {                         \
      ret = OB_ITER_END;                                                    \
    } else if (ObCellWriter::EA_OTHER == meta->attr_) {                     \
      const int8_t* value = read<int8_t>();                                 \
      switch (*value) {                                                     \
        case ObCellWriter::EV_DEL_ROW:                                      \
          if (row_empty) {                                                  \
            row.flag_ = ObActionFlag::OP_DEL_ROW;                           \
          }                                                                 \
          loop_flag = false;                                                \
          break;                                                            \
        case ObCellWriter::EV_MIN_CELL:                                     \
          obj_.set_min_value();                                             \
          break;                                                            \
        case ObCellWriter::EV_MAX_CELL:                                     \
          obj_.set_max_value();                                             \
          break;                                                            \
        default:                                                            \
          ret = OB_NOT_SUPPORTED;                                           \
          TRANS_LOG(WARN, "invalid extend value.", K(ret), K(value));       \
      }                                                                     \
    } else {                                                                \
      ret = OB_NOT_SUPPORTED;                                               \
      TRANS_LOG(WARN, "invalid extend attr.", K(ret), "attr", meta->attr_); \
    }                                                                       \
  }

#define READ_EXTEND_NO_META()                                               \
  {                                                                         \
    if (ObCellWriter::EA_END_FLAG == meta->attr_) {                         \
      ret = OB_ITER_END;                                                    \
    } else if (ObCellWriter::EA_OTHER == meta->attr_) {                     \
      const int8_t* value = read<int8_t>();                                 \
      switch (*value) {                                                     \
        case ObCellWriter::EV_DEL_ROW:                                      \
          if (row_empty) {                                                  \
            row.flag_ = ObActionFlag::OP_DEL_ROW;                           \
          }                                                                 \
          filled_column_count = row.row_val_.count_;                        \
          break;                                                            \
        case ObCellWriter::EV_MIN_CELL:                                     \
          obj_.set_min_value();                                             \
          has_null = true;                                                  \
          break;                                                            \
        case ObCellWriter::EV_MAX_CELL:                                     \
          obj_.set_max_value();                                             \
          has_null = true;                                                  \
          break;                                                            \
        default:                                                            \
          ret = OB_NOT_SUPPORTED;                                           \
          TRANS_LOG(WARN, "invalid extend value.", K(ret), K(value));       \
      }                                                                     \
    } else {                                                                \
      ret = OB_NOT_SUPPORTED;                                               \
      TRANS_LOG(WARN, "invalid extend attr.", K(ret), "attr", meta->attr_); \
    }                                                                       \
  }

int ObMemtableRowReader::read_oracle_timestamp(
    const common::ObObjType obj_type, const uint8_t meta_attr, const common::ObOTimestampMetaAttrType otmat)
{
  int ret = OB_SUCCESS;
  const uint8_t expect_attr = static_cast<uint8_t>(otmat);
  const int64_t length = ObObj::get_otimestamp_store_size(common::OTMAT_TIMESTAMP_TZ == otmat);
  if (expect_attr != meta_attr) {
    ret = OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "not support this attr, ", K(ret), K(otmat), K(meta_attr));
  } else if (OB_UNLIKELY(pos_ + length > buf_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WARN, "buffer not enough, ", K(ret), K_(pos), K_(buf_size), K(length));
  } else {
    const int64_t time_us = *read<int64_t>();
    if (common::OTMAT_TIMESTAMP_TZ == otmat) {
      const uint32_t time_ctx_desc = *read<uint32_t>();
      obj_.set_otimestamp_value(obj_type, time_us, time_ctx_desc);
    } else {
      const uint16_t time_desc = *read<uint16_t>();
      obj_.set_otimestamp_value(obj_type, time_us, time_desc);
    }
  }
  return ret;
}

ObMemtableRowReader::ObMemtableRowReader() : buf_(NULL), buf_size_(0), pos_(0), column_id_(0), obj_()
{}
int ObMemtableRowReader::set_buf(const char* buf, int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid cell reader input argeument.", K(ret), KP(buf), K(buf_size));
  } else {
    buf_ = buf;
    buf_size_ = buf_size;
    pos_ = 0;
    column_id_ = 0;
  }
  return ret;
}
void ObMemtableRowReader::reset()
{
  buf_ = NULL;
  buf_size_ = 0;
  pos_ = 0;
  column_id_ = 0;
  // obj_
}

// bit_set record the column_ids have recorded in row
int ObMemtableRowReader::get_memtable_sparse_row(const share::schema::ColumnMap& column_index,
    ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>& bit_set, storage::ObStoreRow& row, bool& loop_flag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row.column_ids_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "column id array of row is null", K(ret), K(row));
  }
  int32_t column_pos = -1;
  int64_t col_index = row.row_val_.count_;
  bool row_empty = bit_set.is_empty();
  while (OB_SUCC(ret) && OB_SUCC(parse_with_meta(row, row_empty, loop_flag))) {
    if (!loop_flag) {  // end loop
      break;
    } else if (OB_UNLIKELY(OB_INVALID_ID == column_id_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "invalid column id", K_(obj), K(ret));
    } else if (!bit_set.has_member(column_id_)) {  // not found
      if (OB_FAIL(column_index.get(column_id_, column_pos))) {
        if (OB_HASH_NOT_EXIST != ret) {
          TRANS_LOG(WARN, "failed to get column pos", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret)) {
        TRANS_LOG(DEBUG, "add col", K(column_id_), K(obj_));
        if (0 > column_pos) {
          // do nothing
        } else if (OB_FAIL(bit_set.add_member(column_id_))) {
          TRANS_LOG(WARN, "add column id into set failed", K(ret), K(obj_), K(column_id_));
        } else if (column_pos < col_index && row.row_val_.cells_[column_pos].is_nop_value()) {  // set rowkey column
          row.row_val_.cells_[column_pos] = obj_;
          row.column_ids_[column_pos] = column_id_;
        } else {  // set cell column
          row.row_val_.cells_[col_index] = obj_;
          row.column_ids_[col_index] = column_id_;
          ++col_index;
        }
      }
    }
  }  // end while
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  if (OB_SUCC(ret)) {
    row.row_val_.count_ = col_index;  // set column count
  }
  return ret;
}

int ObMemtableRowReader::get_memtable_row(bool& row_empty, const share::schema::ColumnMap& column_index,
    const storage::ObColDescIArray& columns, storage::ObStoreRow& row, memtable::ObNopBitMap& bitmap,
    int64_t& filled_column_count, bool& has_null)
{
  int ret = OB_SUCCESS;
  bool inner_has_null = false;
  while (OB_SUCC(ret) && OB_SUCC(parse_no_meta(row, inner_has_null, row_empty, filled_column_count))) {
    int32_t column_pos = -1;
    if (filled_column_count >= row.row_val_.count_) {
      break;
    } else if (OB_UNLIKELY(OB_INVALID_ID == column_id_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "invalid column id", K_(obj), K(ret));
    } else if (OB_FAIL(column_index.get(column_id_, column_pos))) {
      if (OB_HASH_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "failed to get column pos", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      if (0 > column_pos || !bitmap.test(column_pos)) {
        // do nothing
      } else {
        has_null = has_null | inner_has_null;
        row.row_val_.cells_[column_pos].copy_meta_type(columns.at(column_pos).col_type_);
        obj_.copy_value_or_obj(row.row_val_.cells_[column_pos], inner_has_null);
        bitmap.set_false(column_pos);
        if (row.row_val_.cells_[column_pos].is_lob()) {
          row.row_val_.cells_[column_pos].set_lob_inrow();
        }
        ++filled_column_count;
      }
      inner_has_null = false;
    }
  }

  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  return ret;
}

// row_empty is used to marking delete row
OB_INLINE int ObMemtableRowReader::parse_with_meta(ObStoreRow& row, bool& row_empty, bool& loop_flag)
{
  int ret = OB_SUCCESS;
  const ObCellWriter::CellMeta* meta = read<ObCellWriter::CellMeta>();
  switch (meta->type_) {
    case ObNullType:
      READ_NULL();
      break;
    case ObTinyIntType:
      READ_INT(set_tinyint, int8_t);
      break;
    case ObSmallIntType:
      READ_INT(set_smallint, int16_t);
      break;
    case ObMediumIntType:
      READ_INT(set_mediumint, int32_t);
      break;
    case ObInt32Type:
      READ_INT(set_int32, int32_t);
      break;
    case ObIntType:
      READ_INT(set_int, int64_t);
      break;
    case ObUTinyIntType:
      READ_INT(set_utinyint, uint8_t);
      break;
    case ObUSmallIntType:
      READ_INT(set_usmallint, uint16_t);
      break;
    case ObUMediumIntType:
      READ_INT(set_umediumint, uint32_t);
      break;
    case ObUInt32Type:
      READ_INT(set_uint32, uint32_t);
      break;
    case ObUInt64Type:
      READ_INT(set_uint64, uint64_t);
      break;
    case ObFloatType:
      READ_COMMON(set_float, float, float);
      break;
    case ObUFloatType:
      READ_COMMON(set_ufloat, float, float);
      break;
    case ObDoubleType:
      READ_COMMON(set_double, double, double);
      break;
    case ObUDoubleType:
      READ_COMMON(set_udouble, double, double);
      break;
    case ObNumberType:
      READ_NUMBER(set_number);
      break;
    case ObUNumberType:
      READ_NUMBER(set_unumber);
      break;
    case ObDateTimeType:
      READ_COMMON(set_datetime, int64_t, int64_t);
      break;
    case ObTimestampType:
      READ_COMMON(set_timestamp, int64_t, int64_t);
      break;
    case ObDateType:
      READ_COMMON(set_date, int32_t, int32_t);
      break;
    case ObTimeType:
      READ_COMMON(set_time, int64_t, int64_t);
      break;
    case ObYearType:
      READ_COMMON(set_year, uint8_t, uint8_t);
      break;
    // TODO text share with varchar temporarily
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
      READ_TEXT(meta->type_);
      break;
    case ObVarcharType:
      READ_CHAR(set_varchar);
      break;
    case ObCharType:
      READ_CHAR_WITH_META(set_char);
      break;
    case ObHexStringType:
      READ_BINARY_WITH_META(set_hex_string);
      break;
    case ObExtendType:
      READ_EXTEND_WITH_META();
      break;
    case ObBitType:
      READ_INT(set_bit, uint64_t);
      break;
    case ObEnumType:
      READ_INT(set_enum, uint64_t);
      break;
    case ObSetType:
      READ_INT(set_set, uint64_t);
      break;
    case ObTimestampTZType:
      ret = read_oracle_timestamp(ObTimestampTZType, meta->attr_, common::OTMAT_TIMESTAMP_TZ);
      break;
    case ObTimestampLTZType:
      ret = read_oracle_timestamp(ObTimestampLTZType, meta->attr_, common::OTMAT_TIMESTAMP_LTZ);
      break;
    case ObTimestampNanoType:
      ret = read_oracle_timestamp(ObTimestampNanoType, meta->attr_, common::OTMAT_TIMESTAMP_NANO);
      break;
    case ObRawType:
      READ_CHAR(set_raw);
      break;
    case ObIntervalYMType:
      ret = read_interval_ym();
      break;
    case ObIntervalDSType:
      ret = read_interval_ds();
      break;
    case ObNumberFloatType:
      READ_NUMBER(set_number_float);
      break;
    case ObNVarchar2Type:
      READ_CHAR_WITH_META(set_nvarchar2);
      break;
    case ObNCharType:
      READ_CHAR_WITH_META(set_nchar);
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(WARN, "not supported type.", K(ret), "type", meta->type_);
  }
  row_empty = false;
  if (OB_SUCCESS == ret) {
    if (ObExtendType != meta->type_ || obj_.is_min_value() || obj_.is_max_value()) {
      column_id_ = *read<uint32_t>();
    } else {
      column_id_ = OB_INVALID_ID;
    }
  }
  return ret;
}

// does not set objmeta since it's set above
OB_INLINE int ObMemtableRowReader::parse_no_meta(
    ObStoreRow& row, bool& has_null, bool& row_empty, int64_t& filled_column_count)
{
  int ret = OB_SUCCESS;
  const ObCellWriter::CellMeta* meta = read<ObCellWriter::CellMeta>();
  switch (meta->type_) {
    case ObNullType:
      READ_NULL();
      has_null = true;
      break;
    case ObTinyIntType:
      READ_INT(set_tinyint_value, int8_t);
      break;
    case ObSmallIntType:
      READ_INT(set_smallint_value, int16_t);
      break;
    case ObMediumIntType:
      READ_INT(set_mediumint_value, int32_t);
      break;
    case ObInt32Type:
      READ_INT(set_int32_value, int32_t);
      break;
    case ObIntType:
      READ_INT(set_int_value, int64_t);
      break;
    case ObUTinyIntType:
      READ_INT(set_utinyint_value, uint8_t);
      break;
    case ObUSmallIntType:
      READ_INT(set_usmallint_value, uint16_t);
      break;
    case ObUMediumIntType:
      READ_INT(set_umediumint_value, uint32_t);
      break;
    case ObUInt32Type:
      READ_INT(set_uint32_value, uint32_t);
      break;
    case ObUInt64Type:
      READ_INT(set_uint64_value, uint64_t);
      break;
    case ObFloatType:
      READ_COMMON(set_float_value, float, float);
      break;
    case ObUFloatType:
      READ_COMMON(set_ufloat_value, float, float);
      break;
    case ObDoubleType:
      READ_COMMON(set_double_value, double, double);
      break;
    case ObUDoubleType:
      READ_COMMON(set_udouble_value, double, double);
      break;
    case ObNumberType:
      READ_NUMBER(set_number);
      break;
    case ObUNumberType:
      READ_NUMBER(set_unumber);
      break;
    case ObDateTimeType:
      READ_COMMON(set_datetime_value, int64_t, int64_t);
      break;
    case ObTimestampType:
      READ_COMMON(set_timestamp_value, int64_t, int64_t);
      break;
    case ObDateType:
      READ_COMMON(set_date_value, int32_t, int32_t);
      break;
    case ObTimeType:
      READ_COMMON(set_time_value, int64_t, int64_t);
      break;
    case ObYearType:
      READ_COMMON(set_year_value, uint8_t, uint8_t);
      break;
    // TODO text share with varchar temporarily
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
      READ_TEXT(meta->type_);
      break;
    case ObVarcharType:
      READ_CHAR(set_varchar_value);
      break;
    case ObCharType:
      READ_CHAR(set_char_value);
      break;
    case ObNVarchar2Type:
      READ_CHAR(set_nvarchar2_value);
      break;
    case ObNCharType:
      READ_CHAR(set_nchar_value);
      break;
    case ObHexStringType:
      READ_BINARY(set_hex_string_value);
      break;
    case ObExtendType:
      READ_EXTEND_NO_META();
      break;
    case ObBitType:
      READ_INT(set_bit_value, uint64_t);
      break;
    case ObEnumType:
      READ_INT(set_enum_value, uint64_t);
      break;
    case ObSetType:
      READ_INT(set_set_value, uint64_t);
      break;
    case ObTimestampTZType:
      ret = read_oracle_timestamp(ObTimestampTZType, meta->attr_, common::OTMAT_TIMESTAMP_TZ);
      break;
    case ObTimestampLTZType:
      ret = read_oracle_timestamp(ObTimestampLTZType, meta->attr_, common::OTMAT_TIMESTAMP_LTZ);
      break;
    case ObTimestampNanoType:
      ret = read_oracle_timestamp(ObTimestampNanoType, meta->attr_, common::OTMAT_TIMESTAMP_NANO);
      break;
    case ObRawType:
      READ_CHAR(set_raw_value);
      break;
    case ObIntervalYMType:
      ret = read_interval_ym();
      break;
    case ObIntervalDSType:
      ret = read_interval_ds();
      break;
    case ObNumberFloatType:
      READ_NUMBER(set_number_float);
      break;
    case ObURowIDType:
      ret = read_urowid();
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(WARN, "not supported type.", K(ret), "type", meta->type_);
  }
  row_empty = false;
  if (OB_SUCCESS == ret) {
    if (ObExtendType != meta->type_ || obj_.is_min_value() || obj_.is_max_value()) {
      column_id_ = *read<uint32_t>();
    } else {
      column_id_ = OB_INVALID_ID;
    }
  }
  return ret;
}

template <class T>
OB_INLINE const T* ObMemtableRowReader::read()
{
  const T* ptr = reinterpret_cast<const T*>(buf_ + pos_);
  pos_ += sizeof(T);
  return ptr;
}

OB_INLINE int ObMemtableRowReader::read_interval_ds()
{
  int ret = OB_SUCCESS;
  const int64_t value_len = ObIntervalDSValue::get_store_size();
  if (OB_UNLIKELY(pos_ + value_len > buf_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WARN, "buffer not enough, ", K(ret), K_(pos), K_(buf_size), K(value_len));
  } else {
    const int64_t nsecond = *read<int64_t>();
    const int32_t fractional_second = *read<int32_t>();
    obj_.set_interval_ds(ObIntervalDSValue(nsecond, fractional_second));
  }
  return ret;
}

OB_INLINE int ObMemtableRowReader::read_interval_ym()
{
  int ret = OB_SUCCESS;
  const int64_t value_len = ObIntervalYMValue::get_store_size();
  if (OB_UNLIKELY(pos_ + value_len > buf_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WARN, "buffer not enough, ", K(ret), K_(pos), K_(buf_size), K(value_len));
  } else {
    obj_.set_interval_ym(ObIntervalYMValue(*read<int64_t>()));
  }
  return ret;
}

OB_INLINE int ObMemtableRowReader::read_urowid()
{
  int ret = OB_SUCCESS;
  const uint32_t str_len = *read<uint32_t>();
  if (OB_UNLIKELY(pos_ + str_len > buf_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WARN, "buffer not enough", K(ret), K(pos_), K(buf_size_), K(str_len));
  } else {
    obj_.set_urowid(buf_ + pos_, str_len);
    pos_ += str_len;
  }
  return ret;
}

ObMemtableIterRowReader::ObMemtableIterRowReader()
    : is_inited_(false),
      loop_flag_(true),
      row_empty_(true),
      has_null_(false),
      column_cnt_(0),
      filled_column_count_(0),
      cols_map_(nullptr),
      bitmap_(nullptr),
      columns_ptr_(nullptr),
      bit_set_(nullptr)
{}

ObMemtableIterRowReader::~ObMemtableIterRowReader()
{
  destory();
}

void ObMemtableIterRowReader::destory()
{
  loop_flag_ = true;
  row_empty_ = true;
  has_null_ = false;
  column_cnt_ = 0;
  filled_column_count_ = 0;
  reader_.reset();
  cols_map_ = nullptr;
  bitmap_ = nullptr;
  if (OB_NOT_NULL(bit_set_)) {
    bit_set_->~ObFixedBitSet();
    bit_set_ = nullptr;
  }
  is_inited_ = false;
}

void ObMemtableIterRowReader::reset()
{
  loop_flag_ = true;
  row_empty_ = true;
  has_null_ = false;
  filled_column_count_ = 0;
  reader_.reset();
  bitmap_->reset();  // clear bitmap
  if (OB_NOT_NULL(bit_set_)) {
    bit_set_->clear_all();
  }
}

int ObMemtableIterRowReader::init(common::ObArenaAllocator* allocator, const share::schema::ColumnMap* cols_map,
    ObNopBitMap* bitmap, const storage::ObColDescArray& columns)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    destory();
  }
  cols_map_ = cols_map;
  bitmap_ = bitmap;
  column_cnt_ = columns.count();
  columns_ptr_ = &columns;
  is_inited_ = true;
  if (GCONF._enable_sparse_row) {
    void* buf = allocator->alloc(sizeof(ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "alloc failed", K(ret), KPC(cols_map_));
    } else {
      bit_set_ = new (buf) ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>();
    }
  }
  if (OB_FAIL(ret)) {
    destory();
  }
  return ret;
}

bool ObMemtableIterRowReader::is_iter_end()
{
  bool bret = false;
  if (GCONF._enable_sparse_row) {
    bret = !loop_flag_;
  } else {
    bret = (filled_column_count_ >= column_cnt_);
  }
  return bret;
}

int ObMemtableIterRowReader::get_memtable_row(storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "is not init", K(ret), KPC(cols_map_));
  } else if (OB_ISNULL(cols_map_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "col map is null", K(ret), K(cols_map_));
  } else if (GCONF._enable_sparse_row) {  // iter sparse row
    if (OB_ISNULL(bit_set_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "bit set is null", K(ret), K(bit_set_));
    } else if (OB_FAIL(reader_.get_memtable_sparse_row(*cols_map_, *bit_set_, row, loop_flag_))) {
      COMMON_LOG(WARN, "iter sparse row fail", K(ret), K(reader_));
    }
  } else if (OB_ISNULL(bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "bitmap is null", K(ret), K(bitmap_));
  } else {
    if (OB_FAIL(reader_.get_memtable_row(
            row_empty_, *cols_map_, *columns_ptr_, row, *bitmap_, filled_column_count_, has_null_))) {
      TRANS_LOG(WARN, "iter flat row fail", K(ret), K(reader_));
    }
  }
  return ret;
}

int ObMemtableIterRowReader::set_buf(const char* buf, int64_t buf_size)
{
  reader_.reset();
  return reader_.set_buf(buf, buf_size);
}

int ObMemtableIterRowReader::set_nop_pos(storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "is not init", K(ret), KPC(cols_map_));
  } else if (GCONF._enable_sparse_row) {
    // do nothing
  } else {
    if (OB_ISNULL(bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "bitmap is null", K(ret), K(bitmap_));
    } else if (!bitmap_->is_empty()) {
      if (OB_FAIL(bitmap_->set_nop_obj(row.row_val_.cells_))) {
        COMMON_LOG(WARN, "failed to set nop obj for row", K(ret), KP(bitmap_));
      }
    }
  }
  return ret;
}

}  // end namespace memtable
}  // end namespace oceanbase
