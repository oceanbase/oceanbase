/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_arrow_data_loader.h"

#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/string/ob_sql_string.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/geo/ob_geo_utils.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace sql
{

using namespace common;
using arrow::Array;
using arrow::BaseBinaryArray;
using arrow::BinaryType;
using arrow::BooleanArray;
using arrow::DataType;
using arrow::Date32Array;
using arrow::Date32Type;
using arrow::Decimal128;
using arrow::Decimal256;
using arrow::DecimalType;
using arrow::DoubleType;
using arrow::FixedSizeBinaryArray;
using arrow::FloatType;
using arrow::Int8Type;
using arrow::Int16Type;
using arrow::Int32Type;
using arrow::Int64Type;
using arrow::LargeBinaryType;
using arrow::LargeStringType;
using arrow::NumericArray;
using arrow::StringType;
using arrow::Time32Type;
using arrow::Time64Type;
using arrow::TimestampType;
using arrow::TimeUnit;
using arrow::Type;
using arrow::UInt8Type;
using arrow::UInt16Type;
using arrow::UInt32Type;
using arrow::UInt64Type;

int ObArrowDataLoaderFactory::select_loader(ObIAllocator &allocator,
                                            const DataType &arrow_type,
                                            const ObDatumMeta &datum_type,
                                            ObArrowDataLoader *&loader)
{
  int ret = OB_SUCCESS;
  loader = nullptr;
  const ObObjType out_type = datum_type.get_type();
  switch (arrow_type.id()) {
    case Type::UINT64: {
      if (ObUInt64Type == out_type) {
        loader = OB_NEWx(ObCopyableArrowDataLoader<UInt64Type>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::UINT32: {
      if (out_type >= ObUTinyIntType && out_type <= ObUInt64Type) {
        loader = OB_NEWx(ObIntToInt64ArrowDataLoader<UInt32Type>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::UINT16: {
      if (out_type >= ObUTinyIntType && out_type <= ObUInt64Type) {
        loader = OB_NEWx(ObIntToInt64ArrowDataLoader<UInt16Type>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::UINT8: {
      if (out_type >= ObUTinyIntType && out_type <= ObUInt64Type) {
        loader = OB_NEWx(ObIntToInt64ArrowDataLoader<UInt8Type>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::INT64: {
      if ((out_type >= ObTinyIntType && out_type <= ObUInt64Type) || out_type == ObBitType) {
        loader = OB_NEWx(ObCopyableArrowDataLoader<Int64Type>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::INT32: {
      if (out_type >= ObTinyIntType && out_type <= ObUInt64Type) {
        loader = OB_NEWx(ObIntToInt64ArrowDataLoader<Int32Type>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::INT16: {
      if (out_type >= ObTinyIntType && out_type <= ObUInt64Type) {
        loader = OB_NEWx(ObIntToInt64ArrowDataLoader<Int16Type>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::INT8: {
      if (out_type >= ObTinyIntType && out_type <= ObUInt64Type) {
        loader = OB_NEWx(ObIntToInt64ArrowDataLoader<Int8Type>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::DOUBLE: {
      if (ObDoubleType == out_type) {
        loader = OB_NEWx(ObCopyableArrowDataLoader<DoubleType>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::FLOAT: {
      if (ObFloatType == out_type) {
        loader = OB_NEWx(ObCopyableArrowDataLoader<FloatType>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::BOOL: {
      if (is_mysql_mode() && out_type >= ObTinyIntType && out_type <= ObIntType) {
        loader = OB_NEWx(ObBoolToIntArrowDataLoader, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::STRING: {
      if (ob_is_string_or_lob_type(out_type) || ob_is_json(out_type)) {
        loader = OB_NEWx(ObStringToStringArrowDataLoader<StringType>, &allocator);
      } else if (ObTimeType == out_type) {
        loader = OB_NEWx(ObStringToTimeArrowDataLoader<StringType>, &allocator);
      } else if (ObMySQLDateTimeType == out_type || ObDateTimeType == out_type) {
        loader = OB_NEWx(ObStringToDateTimeArrowDataLoader<StringType>, &allocator);
      } else if (ObMySQLDateType == out_type) {
        loader = OB_NEWx(ObStringToMysqlDateArrowDataLoader<StringType>, &allocator);
      } else if (ObDateType == out_type) {
        loader = OB_NEWx(ObStringToDateArrowDataLoader<StringType>, &allocator);
      } else if (ObYearType == out_type) {
        loader = OB_NEWx(ObStringToYearArrowDataLoader<StringType>, &allocator);
      } else if (ObTimestampType == out_type) {
        loader = OB_NEWx(ObStringToTimestampArrowDataLoader<StringType>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::LARGE_STRING: {
      if (ob_is_string_or_lob_type(out_type) || ob_is_json(out_type)) {
        loader = OB_NEWx(ObStringToStringArrowDataLoader<LargeStringType>, &allocator);
      } else if (ObTimeType == out_type) {
        loader = OB_NEWx(ObStringToTimeArrowDataLoader<LargeStringType>, &allocator);
      } else if (ObMySQLDateTimeType == out_type || ObDateTimeType == out_type) {
        loader = OB_NEWx(ObStringToDateTimeArrowDataLoader<LargeStringType>, &allocator);
      } else if (ObMySQLDateType == out_type) {
        loader = OB_NEWx(ObStringToMysqlDateArrowDataLoader<LargeStringType>, &allocator);
      } else if (ObDateType == out_type) {
        loader = OB_NEWx(ObStringToDateArrowDataLoader<LargeStringType>, &allocator);
      } else if (ObYearType == out_type) {
        loader = OB_NEWx(ObStringToYearArrowDataLoader<LargeStringType>, &allocator);
      } else if (ObTimestampType == out_type) {
        loader = OB_NEWx(ObStringToTimestampArrowDataLoader<LargeStringType>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::BINARY: {
      if (ob_is_geometry(out_type)) {
        loader = OB_NEWx(ObBinaryToGisArrowDataLoader<BinaryType>, &allocator);
      } else if (ob_is_string_or_lob_type(out_type)) {
        loader = OB_NEWx(ObStringToStringArrowDataLoader<BinaryType>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::LARGE_BINARY: {
      if (ob_is_geometry(out_type)) {
        loader = OB_NEWx(ObBinaryToGisArrowDataLoader<LargeBinaryType>, &allocator);
      } else if (ob_is_string_or_lob_type(out_type)) {
        loader = OB_NEWx(ObStringToStringArrowDataLoader<LargeBinaryType>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::DECIMAL128:
    case Type::DECIMAL256: {
      if (ObDecimalIntType == out_type) {
        loader = OB_NEWx(ObDecimalArrowDataLoader, &allocator);
      } else if (out_type >= ObTinyIntType && out_type <= ObUInt64Type) {
        loader = OB_NEWx(ObDecimalToIntArrowDataLoader, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::DATE32: {
      if (ObMySQLDateType == out_type || ObYearType == out_type) {
        loader = OB_NEWx(ObDate32ToMysqlDateArrowDataLoader, &allocator);
      } else if (ObDateType == out_type) {
        loader = OB_NEWx(ObCopyableArrowDataLoader<Date32Type>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::TIME32: {
      if (ObTimeType == out_type) {
        loader = OB_NEWx(ObTimeArrowDataLoader<Time32Type>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::TIME64: {
      if (ObTimeType == out_type) {
        loader = OB_NEWx(ObTimeArrowDataLoader<Time64Type>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    case Type::TIMESTAMP: {
      if (ObTimestampType == out_type || ObMySQLDateTimeType == out_type
          || ObDateTimeType == out_type) {
        loader = OB_NEWx(ObTimeArrowDataLoader<TimestampType>, &allocator);
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
    }
  }

  if (OB_NOT_SUPPORTED == ret) {
    ObSqlString message;
    ObCStringHelper helper;
    message.assign_fmt("Convert from type '%s' to type '%s' ",
                       arrow_type.ToString().c_str(),
                       helper.convert(datum_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, message.ptr());
    LOG_WARN("convert from arrow type to oceanbase data type is not supported yet",
             K(arrow_type.ToString().c_str()),
             K(datum_type));
  } else if (OB_SUCC(ret) && OB_ISNULL(loader)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate arrow data loader", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(loader->init(arrow_type, datum_type))) {
    LOG_WARN("failed to init data loader", K(ret));
    loader->destroy();
    OB_DELETEx(ObArrowDataLoader, &allocator, loader);
    loader = nullptr;
  }
  return ret;
}

template <typename ArrowType>
int ObCopyableArrowDataLoader<ArrowType>::load(const Array &arrow_array, ObEvalCtx &eval_ctx, ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObFixedLengthBase *out_vec = static_cast<ObFixedLengthBase *>(expr->get_vector(eval_ctx));
  ObBitVector *nulls = nullptr;
  const NumericArray<ArrowType> &in_array
      = static_cast<const NumericArray<ArrowType> &>(arrow_array);
  // Arrow guarantees length >= 0 and 0 <= null_count <= length.
  const int64_t length = in_array.length();
  const int64_t null_count = in_array.null_count();
  const int64_t BYTE_WIDTH = sizeof(typename ArrowType::c_type);
  const typename ArrowType::c_type *in_values = in_array.raw_values();
  char *out_data = OB_ISNULL(out_vec) ? nullptr : out_vec->get_data();
  if (OB_ISNULL(out_vec) || OB_ISNULL(nulls = out_vec->get_nulls())
      || VEC_FIXED != out_vec->get_format() || length > out_vec->get_max_row_cnt()
      || BYTE_WIDTH != out_vec->get_length() || (length > 0 && OB_ISNULL(out_data))
      || (length > null_count && OB_ISNULL(in_values))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid arrow array or oceanbase vector in expr",
        K(ret),
        KP(out_vec),
        KP(nulls),
        KP(in_values),
        K(length),
        K(null_count),
        K(BYTE_WIDTH));
  } else if (0 == null_count) {
    if (length > 0) {
      MEMCPY(out_data, in_values, BYTE_WIDTH * length);
      nulls->unset_all(0, length);
    }
    out_vec->reset_has_null();
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && i < length; ++i) {
      if (in_array.IsNull(i)) {
        nulls->set(i);
      } else {
        const typename ArrowType::c_type value = in_array.Value(i);
        nulls->unset(i);
        MEMCPY(out_data + BYTE_WIDTH * i, &value, BYTE_WIDTH);
      }
    }
    out_vec->set_has_null();
  }
  return ret;
}

template <typename ArrowType>
int ObIntToInt64ArrowDataLoader<ArrowType>::load(const Array &arrow_array, ObEvalCtx &eval_ctx, ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  const NumericArray<ArrowType> &in_array = static_cast<const NumericArray<ArrowType> &>(arrow_array);
  if (OB_ISNULL(out_vec) || VEC_FIXED != out_vec->get_format()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid int vector in expr", KP(out_vec));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < in_array.length(); ++i) {
      if (in_array.IsNull(i)) {
        out_vec->set_null(i);
      } else {
        out_vec->set_int(i, in_array.Value(i));
      }
    }
  }
  return ret;
}

static int set_datum_plain(ObExpr *expr, ObEvalCtx &eval_ctx, const ObString &in_str, ObDatum &datum)
{
  UNUSED(expr);
  UNUSED(eval_ctx);
  datum.set_string(in_str);
  return OB_SUCCESS;
}

static int set_datum_string(ObExpr *expr, ObEvalCtx &eval_ctx, const ObString &in_str, ObDatum &datum)
{
  bool has_set_res = false;
  return ObOdpsDataTypeCastUtil::common_string_string_wrap(
      *expr, expr->obj_meta_.get_type(), CS_TYPE_UTF8MB4_BIN,
      expr->obj_meta_.get_type(), expr->datum_meta_.cs_type_,
      in_str, eval_ctx, datum, has_set_res);
}

static int set_datum_text_utf8(ObExpr *expr, ObEvalCtx &eval_ctx, const ObString &in_str, ObDatum &datum)
{
  return ObOdpsDataTypeCastUtil::common_string_text_wrap(
      *expr, in_str, eval_ctx, nullptr, datum, ObVarcharType, CS_TYPE_UTF8MB4_BIN);
}

static int set_datum_text_binary(ObExpr *expr, ObEvalCtx &eval_ctx, const ObString &in_str, ObDatum &datum)
{
  return ObOdpsDataTypeCastUtil::common_string_text_wrap(
      *expr, in_str, eval_ctx, nullptr, datum, ObVarcharType, CS_TYPE_BINARY);
}

static int set_datum_raw(ObExpr *expr, ObEvalCtx &eval_ctx, const ObString &in_str, ObDatum &datum)
{
  bool has_set_res = false;
  return ObDatumHexUtils::hextoraw_string(*expr, in_str, eval_ctx, datum, has_set_res);
}

static int set_datum_json(ObExpr *expr, ObEvalCtx &eval_ctx, const ObString &in_str, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard alloc_guard(eval_ctx);
  ObJsonNode *json_tree = nullptr;
  ObJsonBinSerializer serializer(&eval_ctx.get_expr_res_alloc());
  ObString json_bin_string;
  if (OB_FAIL(ObJsonParser::get_tree(&alloc_guard.get_allocator(), in_str, json_tree))) {
    LOG_WARN("failed to parse json string", K(ret), K(in_str));
  } else if (OB_ISNULL(json_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse json success but got null", K(ret));
  } else if (OB_FAIL(serializer.serialize(json_tree, json_bin_string))) {
    LOG_WARN("failed to serialize json tree", K(ret));
  } else if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_text_wrap(
                 *expr, json_bin_string, eval_ctx, nullptr, datum,
                 ObVarcharType, CS_TYPE_UTF8MB4_BIN))) {
    LOG_WARN("failed to set json datum", K(ret));
  }
  return ret;
}

template <typename ArrowType>
int ObStringToStringArrowDataLoader<ArrowType>::init(const DataType &arrow_type, const ObDatumMeta &ob_type)
{
  int ret = OB_SUCCESS;
  const ObObjType out_type = ob_type.get_type();
  const ObCharsetType out_charset = ObCharset::charset_type_by_coll(ob_type.cs_type_);
  if (ObCharType == out_type || ObVarcharType == out_type) {
    datum_setter_ = (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset)
                    ? set_datum_plain : set_datum_string;
  } else if (out_type >= ObTinyTextType && out_type <= ObLongTextType) {
    datum_setter_ = ArrowType::is_utf8 ? set_datum_text_utf8 : set_datum_text_binary;
  } else if (ObRawType == out_type) {
    datum_setter_ = set_datum_raw;
  } else if (ObJsonType == out_type) {
    datum_setter_ = set_datum_json;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("convert arrow type to ob type not supported", K(ret),
             K(arrow_type.ToString().c_str()), K(ob_type));
  }
  return ret;
}

template <typename ArrowType>
int ObStringToStringArrowDataLoader<ArrowType>::load(const Array &arrow_array, ObEvalCtx &eval_ctx,
                                                     ObExpr *expr)
{
  int ret = OB_SUCCESS;
  const BaseBinaryArray<ArrowType> &binary_array =
      static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  ObDatum datum;
  ObString in_str;
  const bool is_byte_length = !ArrowType::is_utf8 ||
      is_oracle_byte_length(lib::is_oracle_mode(), expr->datum_meta_.length_semantics_);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
  if (OB_ISNULL(datum_setter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("arrow string loader is not inited", K(ret));
  } else if (OB_ISNULL(out_vec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr vector is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < binary_array.length(); ++i) {
    batch_info_guard.set_batch_idx(i);
    if (binary_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (FALSE_IT(item_bytes = binary_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || (item_length == 0 && lib::is_oracle_mode())) {
      out_vec->set_null(i);
    } else if (item_length > expr->max_length_ &&
               (is_byte_length ||
                ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN,
                                       reinterpret_cast<const char *>(item_bytes),
                                       item_length) > expr->max_length_)) {
      ret = OB_ERR_DATA_TOO_LONG;
      LOG_WARN("value is too long", K(ret), K(item_length), K(expr->max_length_));
    } else if (FALSE_IT(in_str.assign_ptr(reinterpret_cast<const char *>(item_bytes), item_length))) {
    } else if (OB_FAIL(datum_setter_(expr, eval_ctx, in_str, datum))) {
      LOG_WARN("failed to set datum value", K(ret));
    } else {
      out_vec->set_string(i, datum.get_string());
    }
  }
  return ret;
}

template <typename ArrowType>
int ObStringToTimeArrowDataLoader<ArrowType>::init(const DataType &arrow_type, const ObDatumMeta &ob_type)
{
  UNUSED(arrow_type);
  return ob_type.get_type() == ObTimeType ? OB_SUCCESS : OB_INVALID_ARGUMENT;
}

template <typename ArrowType>
int ObStringToTimeArrowDataLoader<ArrowType>::load(const Array &arrow_array, ObEvalCtx &eval_ctx,
                                                   ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &string_array =
      static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  int64_t time_value = 0;
  ObString str_value;
  for (int64_t i = 0; OB_SUCC(ret) && i < string_array.length(); ++i) {
    if (string_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (FALSE_IT(item_bytes = string_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vec->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr(reinterpret_cast<const char *>(item_bytes), item_length))) {
    } else if (OB_FAIL(ObTimeConverter::str_to_time(str_value, time_value))) {
      LOG_WARN("failed to convert string to time", K(ret), K(str_value));
    } else {
      out_vec->set_time(i, time_value);
    }
  }
  return ret;
}

static int string_mysql_datetime_handler(const ObString &str_value, ObIVector *out_vec, int64_t index)
{
  int ret = OB_SUCCESS;
  ObTimeConvertCtx time_convert_ctx(nullptr, false);
  ObMySQLDateTime datetime_value = 0;
  if (OB_FAIL(ObTimeConverter::str_to_mdatetime(str_value, time_convert_ctx, datetime_value))) {
    LOG_WARN("failed to convert string to mysql datetime", K(ret), K(str_value));
  } else {
    out_vec->set_mysql_datetime(index, datetime_value);
  }
  return ret;
}

static int string_datetime_handler(const ObString &str_value, ObIVector *out_vec, int64_t index)
{
  int ret = OB_SUCCESS;
  ObTimeConvertCtx time_convert_ctx(nullptr, false);
  int64_t datetime_value = 0;
  if (OB_FAIL(ObTimeConverter::str_to_datetime(str_value, time_convert_ctx, datetime_value))) {
    LOG_WARN("failed to convert string to datetime", K(ret), K(str_value));
  } else {
    out_vec->set_datetime(index, datetime_value);
  }
  return ret;
}

template <typename ArrowType>
int ObStringToDateTimeArrowDataLoader<ArrowType>::init(const DataType &arrow_type,
                                                       const ObDatumMeta &ob_type)
{
  UNUSED(arrow_type);
  int ret = OB_SUCCESS;
  if (ObMySQLDateTimeType == ob_type.get_type()) {
    datetime_handler_ = string_mysql_datetime_handler;
  } else if (ObDateTimeType == ob_type.get_type()) {
    datetime_handler_ = string_datetime_handler;
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

template <typename ArrowType>
int ObStringToDateTimeArrowDataLoader<ArrowType>::load(const Array &arrow_array, ObEvalCtx &eval_ctx,
                                                       ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &string_array =
      static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  ObString str_value;
  if (OB_ISNULL(datetime_handler_)) {
    ret = OB_NOT_INIT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < string_array.length(); ++i) {
    if (string_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (FALSE_IT(item_bytes = string_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vec->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr(reinterpret_cast<const char *>(item_bytes), item_length))) {
    } else if (OB_FAIL(datetime_handler_(str_value, out_vec, i))) {
      LOG_WARN("failed to convert string to datetime", K(ret), K(str_value));
    }
  }
  return ret;
}

template <typename ArrowType>
int ObStringToMysqlDateArrowDataLoader<ArrowType>::init(const DataType &arrow_type,
                                                        const ObDatumMeta &ob_type)
{
  UNUSED(arrow_type);
  return ob_type.get_type() == ObMySQLDateType ? OB_SUCCESS : OB_INVALID_ARGUMENT;
}

template <typename ArrowType>
int ObStringToMysqlDateArrowDataLoader<ArrowType>::load(const Array &arrow_array, ObEvalCtx &eval_ctx,
                                                        ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &string_array =
      static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  ObMySQLDate date_value = 0;
  ObString str_value;
  for (int64_t i = 0; OB_SUCC(ret) && i < string_array.length(); ++i) {
    if (string_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (FALSE_IT(item_bytes = string_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vec->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr(reinterpret_cast<const char *>(item_bytes), item_length))) {
    } else if (OB_FAIL(ObTimeConverter::str_to_mdate(str_value, date_value))) {
      LOG_WARN("failed to convert string to mysql date", K(ret), K(str_value));
    } else {
      out_vec->set_mysql_date(i, date_value);
    }
  }
  return ret;
}

template <typename ArrowType>
int ObStringToDateArrowDataLoader<ArrowType>::init(const DataType &arrow_type, const ObDatumMeta &ob_type)
{
  UNUSED(arrow_type);
  return ob_type.get_type() == ObDateType ? OB_SUCCESS : OB_INVALID_ARGUMENT;
}

template <typename ArrowType>
int ObStringToDateArrowDataLoader<ArrowType>::load(const Array &arrow_array, ObEvalCtx &eval_ctx,
                                                   ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &string_array =
      static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  int32_t date_value = 0;
  ObString str_value;
  for (int64_t i = 0; OB_SUCC(ret) && i < string_array.length(); ++i) {
    if (string_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (FALSE_IT(item_bytes = string_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vec->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr(reinterpret_cast<const char *>(item_bytes), item_length))) {
    } else if (OB_FAIL(ObTimeConverter::str_to_date(str_value, date_value))) {
      LOG_WARN("failed to convert string to date", K(ret), K(str_value));
    } else {
      out_vec->set_date(i, date_value);
    }
  }
  return ret;
}

template <typename ArrowType>
int ObStringToYearArrowDataLoader<ArrowType>::init(const DataType &arrow_type, const ObDatumMeta &ob_type)
{
  UNUSED(arrow_type);
  return ob_type.get_type() == ObYearType ? OB_SUCCESS : OB_INVALID_ARGUMENT;
}

template <typename ArrowType>
int ObStringToYearArrowDataLoader<ArrowType>::load(const Array &arrow_array, ObEvalCtx &eval_ctx,
                                                   ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &string_array =
      static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  uint8_t year_value = 0;
  ObString str_value;
  for (int64_t i = 0; OB_SUCC(ret) && i < string_array.length(); ++i) {
    if (string_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (FALSE_IT(item_bytes = string_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vec->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr(reinterpret_cast<const char *>(item_bytes), item_length))) {
    } else if (OB_FAIL(ObTimeConverter::str_to_year(str_value, year_value))) {
      LOG_WARN("failed to convert string to year", K(ret), K(str_value));
    } else {
      out_vec->set_year(i, year_value);
    }
  }
  return ret;
}

template <typename ArrowType>
int ObStringToTimestampArrowDataLoader<ArrowType>::init(const DataType &arrow_type,
                                                        const ObDatumMeta &ob_type)
{
  UNUSED(arrow_type);
  return ob_type.get_type() == ObTimestampType ? OB_SUCCESS : OB_INVALID_ARGUMENT;
}

template <typename ArrowType>
int ObStringToTimestampArrowDataLoader<ArrowType>::load(const Array &arrow_array, ObEvalCtx &eval_ctx,
                                                        ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &string_array =
      static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  ObTimeConvertCtx time_convert_ctx(nullptr, false);
  int64_t datetime_value = 0;
  ObString str_value;
  for (int64_t i = 0; OB_SUCC(ret) && i < string_array.length(); ++i) {
    if (string_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (FALSE_IT(item_bytes = string_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vec->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr(reinterpret_cast<const char *>(item_bytes), item_length))) {
    } else if (OB_FAIL(ObTimeConverter::str_to_datetime(str_value, time_convert_ctx, datetime_value))) {
      LOG_WARN("failed to convert string to timestamp", K(ret), K(str_value));
    } else {
      out_vec->set_timestamp(i, datetime_value);
    }
  }
  return ret;
}

template <typename ArrowType>
int ObBinaryToGisArrowDataLoader<ArrowType>::load(const Array &arrow_array, ObEvalCtx &eval_ctx,
                                                  ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_guard(eval_ctx);
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &binary_array =
      static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  ObString str_value;
  ObString gis_value;
  ObDatum gis_datum;
  for (int64_t i = 0; OB_SUCC(ret) && i < binary_array.length(); ++i) {
    if (binary_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (FALSE_IT(item_bytes = binary_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vec->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr(reinterpret_cast<const char *>(item_bytes), item_length))) {
    } else if (OB_FAIL(ObGeoTypeUtil::add_geo_version(tmp_alloc_guard.get_allocator(),
                                                       str_value, gis_value))) {
      LOG_WARN("failed to add geo version", K(ret));
    } else if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_text_wrap(
                   *expr, gis_value, eval_ctx, nullptr, gis_datum,
                   ObVarcharType, CS_TYPE_UTF8MB4_BIN))) {
      LOG_WARN("failed to set geo value", K(ret));
    } else {
      out_vec->set_payload_shallow(i, gis_datum.ptr().ptr_, gis_datum.get_int_bytes());
    }
  }
  return ret;
}

int ObBoolToIntArrowDataLoader::load(const Array &arrow_array, ObEvalCtx &eval_ctx, ObExpr *expr)
{
  int ret = OB_SUCCESS;
  const BooleanArray &bool_array = static_cast<const BooleanArray &>(arrow_array);
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  for (int64_t i = 0; OB_SUCC(ret) && i < bool_array.length(); ++i) {
    if (bool_array.IsNull(i)) {
      out_vec->set_null(i);
    } else {
      out_vec->set_bool(i, bool_array.Value(i));
    }
  }
  return ret;
}

int ObDecimalArrowDataLoader::load(const Array &arrow_array, ObEvalCtx &eval_ctx, ObExpr *expr)
{
  int ret = OB_SUCCESS;
  const FixedSizeBinaryArray &decimal_array = static_cast<const FixedSizeBinaryArray &>(arrow_array);
  const DecimalType &in_decimal_type = static_cast<const DecimalType &>(*arrow_array.type());
  const int32_t in_scale = in_decimal_type.scale();
  const ObScale out_scale = expr->datum_meta_.scale_;
  const ObPrecision out_precision = expr->datum_meta_.precision_;
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  const int32_t in_bytes = in_decimal_type.byte_width();
  ObDecimalIntBuilder decimal_builder;
  for (int64_t i = 0; OB_SUCC(ret) && i < arrow_array.length(); ++i) {
    if (arrow_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (OB_FAIL(ObDatumCast::common_scale_decimalint(
                   reinterpret_cast<ObDecimalInt *>(const_cast<uint8_t *>(decimal_array.Value(i))),
                   in_bytes, in_scale, out_scale, out_precision, expr->extra_,
                   decimal_builder, eval_ctx.exec_ctx_.get_user_logging_ctx()))) {
      LOG_WARN("scale decimal int failed", K(ret));
    } else {
      out_vec->set_decimal_int(i, decimal_builder.get_decimal_int(),
                               decimal_builder.get_int_bytes());
    }
  }
  return ret;
}

static int get_int64_decimal128(const std::shared_ptr<DataType> &data_type,
                                const uint8_t *data, int64_t &int_value)
{
  UNUSED(data_type);
  Decimal128 decimal(data);
  int_value = static_cast<int64_t>(decimal.low_bits());
  return OB_SUCCESS;
}

static int get_int64_decimal256(const std::shared_ptr<DataType> &data_type,
                                const uint8_t *data, int64_t &int_value)
{
  UNUSED(data_type);
  Decimal256 decimal(data);
  int_value = static_cast<int64_t>(decimal.low_bits());
  return OB_SUCCESS;
}

int ObDecimalToIntArrowDataLoader::init(const DataType &arrow_type, const ObDatumMeta &ob_type)
{
  UNUSED(ob_type);
  int ret = OB_SUCCESS;
  switch (arrow_type.id()) {
    case Type::DECIMAL256: {
      get_int64_func_ = get_int64_decimal256;
      break;
    }
    case Type::DECIMAL128: {
      get_int64_func_ = get_int64_decimal128;
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(get_int64_func_)) {
    const DecimalType &decimal_type = static_cast<const DecimalType &>(arrow_type);
    if (decimal_type.precision() > 20 || decimal_type.scale() != 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("convert decimal to integer would lose data", K(ret),
               K(decimal_type.ToString().c_str()));
    }
  }
  return ret;
}

int ObDecimalToIntArrowDataLoader::load(const Array &arrow_array, ObEvalCtx &eval_ctx, ObExpr *expr)
{
  int ret = OB_SUCCESS;
  const FixedSizeBinaryArray &decimal_array = static_cast<const FixedSizeBinaryArray &>(arrow_array);
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  int64_t int_value = 0;
  if (OB_ISNULL(get_int64_func_)) {
    ret = OB_NOT_INIT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < arrow_array.length(); ++i) {
    if (arrow_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (OB_FAIL(get_int64_func_(arrow_array.type(), decimal_array.GetValue(i), int_value))) {
      LOG_WARN("failed to convert decimal to int64", K(ret));
    } else {
      out_vec->set_int(i, int_value);
    }
  }
  return ret;
}

int ObDate32ToMysqlDateArrowDataLoader::load(const Array &arrow_array, ObEvalCtx &eval_ctx,
                                             ObExpr *expr)
{
  int ret = OB_SUCCESS;
  const Date32Array &date_array = static_cast<const Date32Array &>(arrow_array);
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  int64_t item_value = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < date_array.length(); ++i) {
    if (date_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (FALSE_IT(item_value = date_array.Value(i))) {
    } else if (ObMySQLDateType == expr->datum_meta_.get_type()) {
      ObMySQLDate mysql_date = 0;
      if (OB_FAIL(ObTimeConverter::date_to_mdate(item_value, mysql_date))) {
        LOG_WARN("failed to convert arrow date to mysql date", K(ret));
      } else {
        out_vec->set_mysql_date(i, mysql_date);
      }
    } else if (ObYearType == expr->datum_meta_.get_type()) {
      uint8_t year = 0;
      if (OB_FAIL(ObTimeConverter::date_to_year(item_value, year))) {
        LOG_WARN("failed to convert arrow date to year", K(ret));
      } else {
        out_vec->set_year(i, year);
      }
    } else {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

template <typename ArrowType>
int ObTimeArrowDataLoader<ArrowType>::init(const DataType &arrow_type, const ObDatumMeta &ob_type)
{
  UNUSED(ob_type);
  int ret = OB_SUCCESS;
  const ArrowType &time_type = static_cast<const ArrowType &>(arrow_type);
  switch (time_type.unit()) {
    case TimeUnit::SECOND: {
      muliples_ = USECS_PER_SEC;
      break;
    }
    case TimeUnit::MILLI: {
      muliples_ = USECS_PER_MSEC;
      break;
    }
    case TimeUnit::MICRO: {
      muliples_ = 1;
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

template <typename ArrowType>
int ObTimeArrowDataLoader<ArrowType>::load(const Array &arrow_array, ObEvalCtx &eval_ctx, ObExpr *expr)
{
  int ret = OB_SUCCESS;
  const NumericArray<ArrowType> &time_array = static_cast<const NumericArray<ArrowType> &>(arrow_array);
  ObIVector *out_vec = expr->get_vector(eval_ctx);
  int64_t arrow_value = 0;
  int64_t timestamp = 0;
  ObMySQLDateTime mysql_time;
  if (OB_UNLIKELY(muliples_ <= 0)) {
    ret = OB_NOT_INIT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < time_array.length(); ++i) {
    if (time_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (FALSE_IT(arrow_value = time_array.Value(i))) {
    } else if (FALSE_IT(timestamp = arrow_value * muliples_)) {
    } else {
      timestamp = timestamp < DATETIME_MIN_VAL ? ObTimeConverter::ZERO_DATETIME : timestamp;
      if (ObMySQLDateTimeType == expr->datum_meta_.get_type()) {
        if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(timestamp, mysql_time))) {
          LOG_WARN("failed to convert arrow timestamp to mysql datetime", K(ret));
        } else {
          out_vec->set_mysql_datetime(i, mysql_time);
        }
      } else if (ObDateTimeType == expr->datum_meta_.get_type()) {
        out_vec->set_datetime(i, timestamp); // the same as set_time()
      } else if (ObTimestampType == expr->datum_meta_.get_type()) {
        out_vec->set_timestamp(i, timestamp); // the same as set_time()
      } else {
        out_vec->set_time(i, timestamp);
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
