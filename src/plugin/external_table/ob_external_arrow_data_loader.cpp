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

#define USING_LOG_PREFIX SHARE

#include "plugin/external_table/ob_external_arrow_data_loader.h"

#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/string/ob_sql_string.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/geo/ob_geo_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "plugin/sys/ob_plugin_utils.h"

namespace oceanbase {

using namespace common;
using namespace sql;

namespace plugin {
namespace external {

using namespace arrow::util;

////////////////////////////////////////////////////////////////////////////////
// ObArrowDataLoaderFactory

int ObArrowDataLoaderFactory::select_loader(ObIAllocator &allocator,
                                            const DataType &arrow_type,
                                            const ObDatumMeta &datum_type,
                                            ObArrowDataLoader *&loader)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), OB_PLUGIN_MEMORY_LABEL);

  const ObObjType out_type = datum_type.get_type();
  // NOTE 由于ADBC没有识别JDBC中的unsigned 数字，所以所有的整数类型数据，从JDBC拉取到ADBC后，都使用int64
  // 表示，并且bigint转换到OB时效率更高。而bigint
  // 由于没有更大的整数类型，使用 ArrowType.Decimal(20, 0)表示。
  switch (arrow_type.id()) {
    case Type::UINT64: {
      if (ObUInt64Type == out_type) {
        ObCopyableArrowDataLoader<UInt64Type> *int64_loader =
            OB_NEWx(ObCopyableArrowDataLoader<UInt64Type>, &allocator);
        if (OB_ISNULL(int64_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*int64_loader)), K(ret));
        }
        loader = int64_loader;
      }
    } break;
    case Type::UINT32: {
      if (out_type >= ObUTinyIntType && out_type <= ObUInt64Type) {
        ObIntToInt64ArrowDataLoader<UInt32Type> *int_loader =
            OB_NEWx(ObIntToInt64ArrowDataLoader<UInt32Type>, &allocator);
        if (OB_ISNULL(int_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*int_loader)), K(ret));
        } else {
          loader = int_loader;
        }
      }
    } break;
    case Type::UINT16: {
      if (out_type >= ObUTinyIntType && out_type <= ObUInt64Type) {
        ObIntToInt64ArrowDataLoader<UInt16Type> *int_loader =
            OB_NEWx(ObIntToInt64ArrowDataLoader<UInt16Type>, &allocator);
        if (OB_ISNULL(int_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*int_loader)), K(ret));
        } else {
          loader = int_loader;
        }
      }
    } break;
    case Type::UINT8: {
      if (out_type >= ObUTinyIntType && out_type <= ObUInt64Type) {
        ObIntToInt64ArrowDataLoader<UInt8Type> *int_loader =
            OB_NEWx(ObIntToInt64ArrowDataLoader<UInt8Type>, &allocator);
        if (OB_ISNULL(int_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*int_loader)), K(ret));
        } else {
          loader = int_loader;
        }
      }
    } break;

    case Type::INT64: {
      if ((out_type >= ObTinyIntType && out_type <= ObUInt64Type) || out_type == ObBitType) {
        ObCopyableArrowDataLoader<Int64Type> *int64_loader =
            OB_NEWx(ObCopyableArrowDataLoader<Int64Type>, &allocator);
        if (OB_ISNULL(int64_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*int64_loader)), K(ret));
        } else {
          loader = int64_loader;
        }
      }
    } break;
    case Type::INT32: {
      if (out_type >= ObTinyIntType && out_type <= ObUInt64Type) {
        ObIntToInt64ArrowDataLoader<Int32Type> *int_loader =
            OB_NEWx(ObIntToInt64ArrowDataLoader<Int32Type>, &allocator);
        if (OB_ISNULL(int_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*int_loader)), K(ret));
        } else {
          loader = int_loader;
        }
      }
    } break;
    case Type::INT16: {
      if (out_type >= ObTinyIntType && out_type <= ObUInt64Type) {
        ObIntToInt64ArrowDataLoader<Int16Type> *int_loader =
            OB_NEWx(ObIntToInt64ArrowDataLoader<Int16Type>, &allocator);
        if (OB_ISNULL(int_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*int_loader)), K(ret));
        } else {
          loader = int_loader;
        }
      }
    } break;
    case Type::INT8: {
      if (out_type >= ObTinyIntType && out_type <= ObUInt64Type) {
        ObIntToInt64ArrowDataLoader<Int8Type> *int_loader =
            OB_NEWx(ObIntToInt64ArrowDataLoader<Int8Type>, &allocator);
        if (OB_ISNULL(int_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*int_loader)), K(ret));
        } else {
          loader = int_loader;
        }
      }
    } break;

    case Type::DOUBLE: {
      if (ObDoubleType == datum_type.get_type()) {
        ObCopyableArrowDataLoader<DoubleType> *double_loader =
            OB_NEWx(ObCopyableArrowDataLoader<DoubleType>, &allocator);
        if (OB_ISNULL(double_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*double_loader)), K(ret));
        } else {
          loader = double_loader;
        }
      }
    } break;

    case Type::FLOAT: {
      if (ObFloatType == datum_type.get_type()) {
        ObCopyableArrowDataLoader<FloatType> *float_loader =
            OB_NEWx(ObCopyableArrowDataLoader<FloatType>, &allocator);
        if (OB_ISNULL(float_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*float_loader)), K(ret));
        } else {
          loader = float_loader;
        }
      }
    } break;

    case Type::BOOL: {
      ObObjType out_type = datum_type.get_type();
      if (is_mysql_mode() && out_type >= ObTinyIntType && out_type <= ObIntType) {
        ObBoolToIntArrowDataLoader *bool_loader =
            OB_NEWx(ObBoolToIntArrowDataLoader, &allocator);
        if (OB_ISNULL(bool_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*bool_loader)), K(ret));
        } else {
          loader = bool_loader;
        }
      }
    } break;

    case Type::STRING: {
      if (ob_is_string_or_lob_type(out_type) || ob_is_json(out_type)) {
        ObStringToStringArrowDataLoader<StringType> *string_loader =
            OB_NEWx(ObStringToStringArrowDataLoader<StringType>, &allocator);
        if (OB_ISNULL(string_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*string_loader)), K(ret));
        } else {
          loader = string_loader;
        }
      } else if (ObTimeType == out_type) {
        loader = OB_NEWx(ObStringToTimeArrowDataLoader<StringType>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObStringToTimeArrowDataLoader<StringType>)), K(ret));
        }
      } else if (ObMySQLDateTimeType == out_type) {
        loader = OB_NEWx(ObStringToDateTimeArrowDataLoader<StringType>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObStringToDateTimeArrowDataLoader<StringType>)), K(ret));
        }
      } else if (ObMySQLDateType == out_type) {
        loader = OB_NEWx(ObStringToMysqlDateArrowDataLoader<StringType>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObStringToMysqlDateArrowDataLoader<StringType>)), K(ret));
        }
      } else if (ObYearType == out_type) {
        loader = OB_NEWx(ObStringToYearArrowDataLoader<StringType>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObStringToYearArrowDataLoader<StringType>)), K(ret));
        }
      } else if (ObTimestampType == out_type) {
        loader = OB_NEWx(ObStringToTimestampArrowDataLoader<StringType>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObStringToTimestampArrowDataLoader<StringType>)), K(ret));
        }
      }
    } break;
    case Type::LARGE_STRING: {
      if (ob_is_string_or_lob_type(out_type) || ob_is_json(out_type)) {
        ObStringToStringArrowDataLoader<LargeStringType> *string_loader =
            OB_NEWx(ObStringToStringArrowDataLoader<LargeStringType>, &allocator);
        if (OB_ISNULL(string_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*string_loader)), K(ret));
        } else {
          loader = string_loader;
        }
      } else if (ObTimeType == out_type) {
        loader = OB_NEWx(ObStringToTimeArrowDataLoader<LargeStringType>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObStringToTimeArrowDataLoader<LargeStringType>)), K(ret));
        }
      } else if (ObMySQLDateTimeType == out_type) {
        loader = OB_NEWx(ObStringToDateTimeArrowDataLoader<LargeStringType>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObStringToDateTimeArrowDataLoader<LargeStringType>)), K(ret));
        }
      } else if (ObMySQLDateType == out_type) {
        loader = OB_NEWx(ObStringToMysqlDateArrowDataLoader<LargeStringType>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObStringToMysqlDateArrowDataLoader<LargeStringType>)), K(ret));
        }
      } else if (ObYearType == out_type) {
        loader = OB_NEWx(ObStringToYearArrowDataLoader<LargeStringType>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObStringToYearArrowDataLoader<LargeStringType>)), K(ret));
        }
      } else if (ObTimestampType == out_type) {
        loader = OB_NEWx(ObStringToTimestampArrowDataLoader<LargeStringType>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObStringToTimestampArrowDataLoader<LargeStringType>)), K(ret));
        }
      }
    } break;
    case Type::BINARY: {
      if (ob_is_geometry(out_type)) {
        ObBinaryToGisArrowDataLoader<BinaryType> *binary_loader =
            OB_NEWx(ObBinaryToGisArrowDataLoader<BinaryType>, &allocator);
        if (OB_ISNULL(binary_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(*binary_loader));
        } else {
          loader = binary_loader;
        }
      } else if (ob_is_string_or_lob_type(out_type)) {
        ObStringToStringArrowDataLoader<BinaryType> *string_loader =
            OB_NEWx(ObStringToStringArrowDataLoader<BinaryType>, &allocator);
        if (OB_ISNULL(string_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*string_loader)), K(ret));
        } else {
          loader = string_loader;
        }
      }
    } break;
    case Type::LARGE_BINARY: {
      if (ob_is_geometry(out_type)) {
        ObBinaryToGisArrowDataLoader<LargeBinaryType> *binary_loader =
            OB_NEWx(ObBinaryToGisArrowDataLoader<LargeBinaryType>, &allocator);
        if (OB_ISNULL(binary_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(*binary_loader));
        } else {
          loader = binary_loader;
        }
      } else if (ob_is_string_or_lob_type(out_type)) {
        ObStringToStringArrowDataLoader<LargeBinaryType> *string_loader =
            OB_NEWx(ObStringToStringArrowDataLoader<LargeBinaryType>, &allocator);
        if (OB_ISNULL(string_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*string_loader)), K(ret));
        } else {
          loader = string_loader;
        }
      }
    } break;

    case Type::DECIMAL128:
    case Type::DECIMAL256:
      //case Type::DECIMAL32:
      //case Type::DECIMAL64:
    {
      if (ObDecimalIntType == out_type) {
        ObDecimalArrowDataLoader *decimal_loader = OB_NEWx(ObDecimalArrowDataLoader, &allocator);
        if (OB_ISNULL(decimal_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*decimal_loader)), K(ret));
        } else {
          loader = decimal_loader;
        }
      } else if (out_type >= ObTinyIntType && out_type <= ObUInt64Type) {
        // all integer types
        ObDecimalToIntArrowDataLoader *decimal_loader = OB_NEWx(ObDecimalToIntArrowDataLoader, &allocator);
        if (OB_ISNULL(decimal_loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(*decimal_loader)), K(ret));
        } else {
          loader = decimal_loader;
        }
      }
    } break;

    case Type::DATE32: {
      if (ObMySQLDateType == out_type || ObYearType == out_type) {
        loader = OB_NEWx(ObDate32ToMysqlDateArrowDataLoader, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObDate32ToMysqlDateArrowDataLoader)), K(ret));
        }
      } else if (ObDateType == datum_type.get_type()) {
        loader = OB_NEWx(ObCopyableArrowDataLoader<Date32Type>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObCopyableArrowDataLoader<Date32Type>)), K(ret));
        }
      }
    } break;

    case Type::TIME32: {
      if (ObTimeType == datum_type.get_type()) {
        loader = OB_NEWx(ObTimeArrowDataLoader<Time32Type>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObTimeArrowDataLoader<Time32Type>)), K(ret));
        }
      }
    } break;

    case Type::TIMESTAMP: {
      if (ObTimestampType == datum_type.get_type() || ObMySQLDateTimeType == datum_type.get_type()) {
        loader = OB_NEWx(ObTimeArrowDataLoader<TimestampType>, &allocator);
        if (OB_ISNULL(loader)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(sizeof(ObTimeArrowDataLoader<TimestampType>)), K(ret));
        }
      }
    } break;

    default: {
      LOG_WARN("not supported arrow data type to oceanbase type", K(arrow_type.ToString().c_str()), K(datum_type));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(loader) && OB_FAIL(loader->init(arrow_type, datum_type))) {
    LOG_WARN("failed to init data loader", K(ret));
    loader->destroy();
    OB_DELETEx(ObArrowDataLoader, &allocator, loader);
    loader = nullptr;
  }

  if (OB_SUCC(ret) && OB_ISNULL(loader)) {
    ret = OB_NOT_SUPPORTED;
    ObSqlString message;
    ObCStringHelper helper;
    message.assign_fmt("Convert from type '%s' to type '%s' ",
                          arrow_type.ToString().c_str(), helper.convert(datum_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, message.ptr());
    LOG_WARN("convert from arrow type to oceanbase data type is not supported yet",
             K(arrow_type.ToString().c_str()), K(datum_type));
  }
  LOG_TRACE("select loader done", K(ret), KP(loader), K(arrow_type.ToString().c_str()), K(datum_type));
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObCopyableArrowDataLoader
template <typename ArrowType>
int ObCopyableArrowDataLoader<ArrowType>::load(const Array &arrow_array, sql::ObEvalCtx& eval_ctx, sql::ObExpr * expr)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_guard(eval_ctx);
  ObFixedLengthBase *out_vec = static_cast<ObFixedLengthBase *>(expr->get_vector(eval_ctx));
  ObBitVector *nulls = nullptr;
  const typename ArrowType::c_type *in_values = nullptr;
  const NumericArray<ArrowType> &in_array = static_cast<const NumericArray<ArrowType> &>(arrow_array);
  const uint8_t *arrow_null_data = arrow_array.null_bitmap_data();

  if (OB_ISNULL(in_values = in_array.raw_values())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid array: no raw value", KP(in_values));
  } else if (OB_ISNULL(out_vec)
             || OB_ISNULL(nulls = out_vec->get_nulls())
             || VEC_FIXED != out_vec->get_format()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid oceanbase vector in expr", KP(out_vec));
  } else {
    LOG_DEBUG("got a column", KCSTRING(arrow_array.ToString().c_str()));
    for (int64_t i = 0; IS_LOG_ENABLED(DEBUG) && i < arrow_array.length(); i++) {
      LOG_DEBUG("column value", K(i), K(in_values[i]));
    }
    MEMCPY(out_vec->get_data(), in_values, arrow_array.type()->byte_width() * arrow_array.length());
    if (OB_NOT_NULL(arrow_null_data)) {
      // the bit 1 means `valid` or `not null` in arrow
      // but it means `is null` in ObBitVector
      nulls->bit_not(*to_bit_vector(arrow_null_data), arrow_array.length());
      out_vec->set_has_null();
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObIntToInt64ArrowDataLoader
template <typename ArrowType>
int ObIntToInt64ArrowDataLoader<ArrowType>::load(const Array &arrow_array, sql::ObEvalCtx& eval_ctx, sql::ObExpr * expr)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_guard(eval_ctx);
  ObIVector *int64_vec = static_cast<ObIVector *>(expr->get_vector(eval_ctx));
  ObBitVector *nulls = nullptr;
  const NumericArray<ArrowType> &int_array = static_cast<const NumericArray<ArrowType> &>(arrow_array);

  if (OB_ISNULL(int64_vec)
      || VEC_FIXED != int64_vec->get_format()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid int64 vector in expr", KP(int64_vec));
  } else {
    LOG_DEBUG("got a column", KCSTRING(arrow_array.ToString().c_str()));
    for (int i = 0; i < int_array.length(); i++) {
      LOG_DEBUG("column value", K(i), K(int_array.Value(i)));
      if (!int_array.IsNull(i)) {
        int64_vec->set_int(i, int_array.Value(i));
      } else {
        int64_vec->set_null(i);
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObStringToStringArrowDataLoader
int set_datum_plain(sql::ObExpr *expr, sql::ObEvalCtx &eval_ctx, const ObString &in_str, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  datum.set_string(in_str);
  return ret;
}
int set_datum_string(sql::ObExpr *expr, sql::ObEvalCtx &eval_ctx, const ObString &in_str, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  bool has_set_res = false;
  ret = ObOdpsDataTypeCastUtil::common_string_string_wrap(
            *expr, expr->obj_meta_.get_type()/*in_type*/, CS_TYPE_UTF8MB4_BIN/*in_cs_type*/,
            expr->obj_meta_.get_type()/*out_type*/, expr->datum_meta_.cs_type_/*out_cs_type*/,
            in_str, eval_ctx, datum, has_set_res);
  return ret;
}

int set_datum_text_utf8(sql::ObExpr *expr, sql::ObEvalCtx &eval_ctx, const ObString &in_str, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ret = ObOdpsDataTypeCastUtil::common_string_text_wrap(
      *expr, in_str, eval_ctx, nullptr, datum, ObVarcharType/*in_type*/, CS_TYPE_UTF8MB4_BIN);
  return ret;
}

int set_datum_text_binary(sql::ObExpr *expr, sql::ObEvalCtx &eval_ctx, const ObString &in_str, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ret = ObOdpsDataTypeCastUtil::common_string_text_wrap(
      *expr, in_str, eval_ctx, nullptr, datum, ObVarcharType/*in_type*/, CS_TYPE_BINARY);
  return ret;
}

int set_datum_raw(sql::ObExpr *expr, sql::ObEvalCtx &eval_ctx, const ObString &in_str, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  bool has_set_res = false;
  ret = ObDatumHexUtils::hextoraw_string(*expr, in_str, eval_ctx, datum, has_set_res);
  return ret;
}

int set_datum_json(sql::ObExpr *expr, sql::ObEvalCtx &eval_ctx, const ObString &in_str, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard alloc_guard(eval_ctx);
  ObJsonNode *json_tree = nullptr;
  ObJsonBinSerializer serializer(&eval_ctx.get_expr_res_alloc());
  ObString json_bin_string;
  if (OB_FAIL(ObJsonParser::get_tree(&alloc_guard.get_allocator(), in_str, json_tree))) {
    LOG_WARN("failed to parse json string", K(in_str));
  } else if (OB_ISNULL(json_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse json success but got null", KP(json_tree));
  } else if (OB_FAIL(serializer.serialize(json_tree, json_bin_string))) {
    LOG_WARN("failed to serialize json tree to json bin string", K(ret));
  } else if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_text_wrap(
      *expr, json_bin_string, eval_ctx, nullptr, datum, ObVarcharType/*in_type*/, CS_TYPE_UTF8MB4_BIN))) {
    LOG_WARN("failed to set string", K(ret));
  }

  return ret;
}

template <typename ArrowType>
int ObStringToStringArrowDataLoader<ArrowType>::init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type)
{
  int ret = OB_SUCCESS;
  ObObjType out_type = ob_type.get_type();
  ObCharsetType out_charset = ObCharset::charset_type_by_coll(ob_type.cs_type_);
  DatumSetter datum_setter = nullptr;
  if (OB_NOT_NULL(datum_setter)) {
    ret = OB_INIT_TWICE;
  } else if (ObCharType == out_type || ObVarcharType == out_type) {
    if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
      datum_setter_ = set_datum_plain;
    } else {
      datum_setter_ = set_datum_string; // need charset convertion
    }
  } else if (out_type >= ObTinyTextType && out_type <= ObLongTextType) {
    // text field is treat as lob
    if (ArrowType::is_utf8) {
      datum_setter_ = set_datum_text_utf8;
    } else {
      datum_setter_ = set_datum_text_binary;
    }
  } else if (ObRawType == out_type) {
    datum_setter_ = set_datum_raw;
  } else if (ObJsonType == out_type) {
    datum_setter_ = set_datum_json;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("convert arrow type to ob type not supported", K(arrow_type.ToString().c_str()), K(ob_type));
  }

  return ret;
}

template <typename ArrowType>
int ObStringToStringArrowDataLoader<ArrowType>::load(
    const Array &arrow_array, sql::ObEvalCtx &eval_ctx, sql::ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_guard(eval_ctx);
  const BaseBinaryArray<ArrowType> &binary_array = static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  ObIVector *out_vector = static_cast<ObIVector *>(expr->get_vector(eval_ctx));
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;

  if (OB_ISNULL(datum_setter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStringToStringArrowDataLoader: loader is not set");
  }

  // 字符串的长度应该按照字节来计算，还是字符个数来计算
  bool is_byte_length = !ArrowType::is_utf8 ||
                        is_oracle_byte_length(lib::is_oracle_mode(), expr->datum_meta_.length_semantics_);

  ObDatum datum;
  ObString in_str;
  // use BatchInfoScopeGuard to control the batch index or row index.
  // ObExpr::get_str_res_mem return the same memory buffer for the same row.
  // ObTextStringVectorResult::init which is used by ObOdpsDataTypeCastUtil::common_string_text_wrap
  // uses get_str_res_mem to allocate memory.
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
  for (int64_t i = 0; OB_SUCC(ret) && i < binary_array.length(); i++) {
    batch_info_guard.set_batch_idx(i);

    if (binary_array.IsNull(i)) {
      out_vector->set_null(i);
    } else if (FALSE_IT(item_bytes = binary_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || (item_length == 0 && lib::is_oracle_mode())) {
      out_vector->set_null(i);
    } else if (item_length > expr->max_length_ &&
               (is_byte_length ||
                ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN, (const char *)item_bytes, item_length) > expr->max_length_)
               ) {
      ret = OB_ERR_DATA_TOO_LONG;
      LOG_WARN("value is too long", K(item_length), K(expr->max_length_), K(ret));
    } else if (FALSE_IT(in_str.assign_ptr((const char *)item_bytes, item_length))) {
    } else if (OB_FAIL(datum_setter_(expr, eval_ctx, in_str, datum))) {
      LOG_WARN("failed to set datum value", K(ret));
    } else {
      out_vector->set_string(i, datum.get_string());
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////////////////////
// ObStringToTimeArrowDataLoader
template <typename ArrowType>
int ObStringToTimeArrowDataLoader<ArrowType>::init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type)
{
  int ret = OB_SUCCESS;
  if (ob_type.get_type() != ObTimeType) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only time type is supported by this loader", KCSTRING(arrow_type.ToString().c_str()), K(ob_type));
  }
  return ret;
}

template <typename ArrowType>
int ObStringToTimeArrowDataLoader<ArrowType>::load(const Array &arrow_array, sql::ObEvalCtx &eval_ctx, sql::ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObIVector *out_vector = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &string_array = static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  int64_t time_value = 0;
  ObString str_value;
  for (int64_t i = 0; OB_SUCC(ret) && i < string_array.length(); i++) {
    if (string_array.IsNull(i)) {
      out_vector->set_null(i);
    } else if (FALSE_IT(item_bytes = string_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vector->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr((const char *)item_bytes, item_length))) {
    } else if (OB_FAIL(ObTimeConverter::str_to_time(str_value, time_value))) {
      LOG_WARN("failed to convert string to time", K(ret), K(str_value));
    } else {
      LOG_TRACE("set item time value", K(str_value), K(time_value));
      out_vector->set_time(i, time_value);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObStringToDateTimeArrowDataLoader
template <typename ArrowType>
int ObStringToDateTimeArrowDataLoader<ArrowType>::init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type)
{
  int ret = OB_SUCCESS;
  if (ob_type.get_type() != ObMySQLDateTimeType) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only mysql datetime type is supported by this loader", KCSTRING(arrow_type.ToString().c_str()), K(ob_type));
  }
  return ret;
}

template <typename ArrowType>
int ObStringToDateTimeArrowDataLoader<ArrowType>::load(const Array &arrow_array, sql::ObEvalCtx &eval_ctx, sql::ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObIVector *out_vector = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &string_array = static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  ObTimeConvertCtx time_convert_ctx(nullptr/*tz_info*/, false/*is_timestamp*/);
  ObMySQLDateTime datetime_value = 0;
  ObString str_value;
  for (int64_t i = 0; OB_SUCC(ret) && i < string_array.length(); i++) {
    if (string_array.IsNull(i)) {
      out_vector->set_null(i);
    } else if (FALSE_IT(item_bytes = string_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vector->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr((const char *)item_bytes, item_length))) {
    } else if (OB_FAIL(ObTimeConverter::str_to_mdatetime(str_value, time_convert_ctx, datetime_value))) {
      LOG_WARN("failed to convert string to mysql datetime", K(ret), K(str_value));
    } else {
      LOG_TRACE("set item mysql date time value", K(str_value), K(datetime_value));
      out_vector->set_mysql_datetime(i, datetime_value);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObStringToMysqlDateArrowDataLoader
template <typename ArrowType>
int ObStringToMysqlDateArrowDataLoader<ArrowType>::init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type)
{
  int ret = OB_SUCCESS;
  if (ob_type.get_type() != ObMySQLDateType) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only mysql date type is supported by this loader", KCSTRING(arrow_type.ToString().c_str()), K(ob_type));
  }
  return ret;
}

template <typename ArrowType>
int ObStringToMysqlDateArrowDataLoader<ArrowType>::load(const Array &arrow_array, sql::ObEvalCtx &eval_ctx, sql::ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObIVector *out_vector = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &string_array = static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  ObMySQLDate date_value = 0;
  ObString str_value;
  for (int64_t i = 0; OB_SUCC(ret) && i < string_array.length(); i++) {
    if (string_array.IsNull(i)) {
      out_vector->set_null(i);
    } else if (FALSE_IT(item_bytes = string_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vector->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr((const char *)item_bytes, item_length))) {
    } else if (OB_FAIL(ObTimeConverter::str_to_mdate(str_value, date_value))) {
      LOG_WARN("failed to convert string to mysql date", K(ret), K(str_value));
    } else {
      LOG_TRACE("set item mysql date value", K(str_value), K(date_value));
      out_vector->set_mysql_date(i, date_value);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObStringToYearArrowDataLoader
template <typename ArrowType>
int ObStringToYearArrowDataLoader<ArrowType>::init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type)
{
  int ret = OB_SUCCESS;
  if (ob_type.get_type() != ObYearType) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only YEAR type is supported by this loader", KCSTRING(arrow_type.ToString().c_str()), K(ob_type));
  }
  return ret;
}

template <typename ArrowType>
int ObStringToYearArrowDataLoader<ArrowType>::load(const Array &arrow_array, sql::ObEvalCtx &eval_ctx, sql::ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObIVector *out_vector = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &string_array = static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  uint8_t year_value = 0;
  ObString str_value;
  for (int64_t i = 0; OB_SUCC(ret) && i < string_array.length(); i++) {
    if (string_array.IsNull(i)) {
      out_vector->set_null(i);
    } else if (FALSE_IT(item_bytes = string_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vector->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr((const char *)item_bytes, item_length))) {
    } else if (OB_FAIL(ObTimeConverter::str_to_year(str_value, year_value))) {
      LOG_WARN("failed to convert string to year", K(ret), K(str_value));
    } else {
      LOG_TRACE("set item year value", K(str_value), K(year_value));
      out_vector->set_year(i, year_value);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObStringToTimestampArrowDataLoader
template <typename ArrowType>
int ObStringToTimestampArrowDataLoader<ArrowType>::init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type)
{
  int ret = OB_SUCCESS;
  if (ob_type.get_type() != ObTimestampType) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only timestamp type is supported by this loader", KCSTRING(arrow_type.ToString().c_str()), K(ob_type));
  }
  return ret;
}

template <typename ArrowType>
int ObStringToTimestampArrowDataLoader<ArrowType>::load(const Array &arrow_array, sql::ObEvalCtx &eval_ctx, sql::ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObIVector *out_vector = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &string_array = static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  ObTimeConvertCtx time_convert_ctx(nullptr/*tz_info*/, false/*is_timestamp*/);
  int64_t datetime_value = 0;
  ObString str_value;
  for (int64_t i = 0; OB_SUCC(ret) && i < string_array.length(); i++) {
    if (string_array.IsNull(i)) {
      out_vector->set_null(i);
    } else if (FALSE_IT(item_bytes = string_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vector->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr((const char *)item_bytes, item_length))) {
    } else if (OB_FAIL(ObTimeConverter::str_to_datetime(str_value, time_convert_ctx, datetime_value))) {
      LOG_WARN("failed to convert string to timestamp", K(ret), K(str_value));
    } else {
      LOG_TRACE("set item mysql timestamp value", K(str_value), K(datetime_value));
      out_vector->set_timestamp(i, datetime_value);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObBinaryToGisArrowDataLoader
template <typename ArrowType>
int ObBinaryToGisArrowDataLoader<ArrowType>::load(const Array &arrow_array, sql::ObEvalCtx &eval_ctx, sql::ObExpr *expr)
{
  int ret = OB_SUCCESS;

  ObEvalCtx::TempAllocGuard tmp_alloc_guard(eval_ctx);
  ObIVector *out_vector = expr->get_vector(eval_ctx);
  const BaseBinaryArray<ArrowType> &binary_array = static_cast<const BaseBinaryArray<ArrowType> &>(arrow_array);
  typename ArrowType::offset_type item_length = 0;
  const uint8_t *item_bytes = nullptr;
  ObString str_value; // gis without version
  ObString gis_value;
  ObDatum  gis_datum;
  for (int64_t i = 0; OB_SUCC(ret) && i < binary_array.length(); i++) {
    if (binary_array.IsNull(i)) {
      out_vector->set_null(i);
    } else if (FALSE_IT(item_bytes = binary_array.GetValue(i, &item_length))) {
    } else if (OB_ISNULL(item_bytes) || 0 == item_length) {
      out_vector->set_null(i);
    } else if (FALSE_IT(str_value.assign_ptr((const char *)item_bytes, item_length))) {
    } else if (OB_FAIL(ObGeoTypeUtil::add_geo_version(tmp_alloc_guard.get_allocator(), str_value, gis_value))) {
      LOG_WARN("failed to add geo version", K(ret), K(str_value.length()));
    } else if (OB_FAIL(ret = ObOdpsDataTypeCastUtil::common_string_text_wrap(
        *expr, gis_value, eval_ctx, nullptr, gis_datum, ObVarcharType/*in_type*/, CS_TYPE_UTF8MB4_BIN))) {
      LOG_WARN("failed to set geo value", K(ret));
    } else {
      out_vector->set_payload_shallow(i, static_cast<const void *>(gis_datum.ptr().ptr_), gis_datum.get_int_bytes());
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////////////////////
// ObBoolToIntArrowDataLoader
int ObBoolToIntArrowDataLoader::load(const Array &arrow_array, sql::ObEvalCtx &eval_ctx, sql::ObExpr *expr)
{
  int ret = OB_SUCCESS;
  const BooleanArray &bool_array = static_cast<const BooleanArray &>(arrow_array);
  ObIVector *out_vector = expr->get_vector(eval_ctx);
  bool item_value = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < bool_array.length(); i++) {
    if (bool_array.IsNull(i)) {
      out_vector->set_null(i);
    } else if (FALSE_IT(item_value = bool_array.Value(i))) {
    } else {
      out_vector->set_bool(i, item_value);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObDecimalArrowDataLoader
int ObDecimalArrowDataLoader::load(const Array &arrow_array, sql::ObEvalCtx &eval_ctx, sql::ObExpr *expr)
{
  int ret = OB_SUCCESS;
  const FixedSizeBinaryArray &decimal_array = static_cast<const FixedSizeBinaryArray &>(arrow_array);
  const DecimalType &in_decimal_type = static_cast<const DecimalType &>(*arrow_array.type());
  int32_t in_precision = in_decimal_type.precision();
  int32_t in_scale = in_decimal_type.scale();
  ObScale out_scale = expr->datum_meta_.scale_;
  ObPrecision out_precision = expr->datum_meta_.precision_;
  ObIVector *out_vector = expr->get_vector(eval_ctx);
  ObDecimalInt *in_decimal = nullptr;
  const int32_t in_bytes = in_decimal_type.byte_width();
  ObDecimalIntBuilder decimal_builder;
  for (int64_t i = 0; OB_SUCC(ret) && i < arrow_array.length(); i++) {
    if (arrow_array.IsNull(i)) {
      out_vector->set_null(i);
    } else if (OB_FAIL(ObDatumCast::common_scale_decimalint((ObDecimalInt *)decimal_array.Value(i),
                                                            in_bytes, in_scale, out_scale, out_precision,
                                                            expr->extra_/*cast mode*/,
                                                            decimal_builder,
                                                            eval_ctx.exec_ctx_.get_user_logging_ctx()))) {
        LOG_WARN("scale decimal int failed", K(ret));
    } else {
      out_vector->set_decimal_int(i, decimal_builder.get_decimal_int(), decimal_builder.get_int_bytes());
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObDecimalToIntArrowDataLoader
int get_int64_decimal128(const shared_ptr<DataType> &data_type, const uint8_t *data, int64_t &int_value)
{
  int ret = OB_SUCCESS;
  Decimal128 decimal(data);
  int_value = static_cast<int64_t>(decimal.low_bits());
  return ret;
}

int get_int64_decimal256(const shared_ptr<DataType> &data_type, const uint8_t *data, int64_t &int_value)
{
  int ret = OB_SUCCESS;
  Decimal256 decimal(data);
  int_value = static_cast<int64_t>(decimal.low_bits());
  return ret;
}

int ObDecimalToIntArrowDataLoader::init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type)
{
  int ret = OB_SUCCESS;
  switch (arrow_type.id()) {
    case Type::DECIMAL256: {
      get_int64_func_ = get_int64_decimal256;
    } break;
    case Type::DECIMAL128: {
      get_int64_func_ = get_int64_decimal128;
    } break;
      //case Type::DECIMAL64:
      //case Type::DECIMAL32:

    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("arrow data type must be decimal", KCSTRING(arrow_type.ToString().c_str()), K(ret));
    } break;
  }

  if (OB_NOT_NULL(get_int64_func_)) {
    const DecimalType &decimal_type = static_cast<const DecimalType &>(arrow_type);
    if (decimal_type.precision() > 20 || decimal_type.scale() != 0) {
      // UINT64 needs precision 20, INT64 needs precision 19
      LOG_WARN("convert decimal with precision bigger than 20 or with non-zero scale will loss data",
               KCSTRING(decimal_type.ToString().c_str()));
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

int ObDecimalToIntArrowDataLoader::load(const Array &arrow_array, sql::ObEvalCtx &eval_ctx, sql::ObExpr *expr)
{
  int ret = OB_SUCCESS;
  const FixedSizeBinaryArray &decimal_array = static_cast<const FixedSizeBinaryArray &>(arrow_array);
  ObIVector *out_vector = expr->get_vector(eval_ctx);

  if (OB_ISNULL(get_int64_func_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }

  int64_t int_value = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < arrow_array.length(); i++) {
    if (arrow_array.IsNull(i)) {
      out_vector->set_null(i);
    } else if (OB_FAIL(get_int64_func_(arrow_array.type(), decimal_array.GetValue(i), int_value))) {
      LOG_WARN("failed to convert decimal to int64", K(ret));
    } else {
      out_vector->set_int(i, int_value);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObDate32ToMysqlDateArrowDataLoader
int ObDate32ToMysqlDateArrowDataLoader::load(const Array &arrow_array, sql::ObEvalCtx &eval_ctx, sql::ObExpr *expr)
{
  int ret = OB_SUCCESS;
  const Date32Array &date_array = static_cast<const Date32Array &>(arrow_array);
  ObIVector *out_vector = expr->get_vector(eval_ctx);
  int64_t item_value = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < date_array.length(); i++) {
    if (date_array.IsNull(i)) {
      out_vector->set_null(i);
    } else if (FALSE_IT(item_value = date_array.Value(i))) {
    } else if (ObMySQLDateType == expr->datum_meta_.get_type()) {
      ObMySQLDate mysql_date = 0;
      if (OB_FAIL(ObTimeConverter::date_to_mdate(item_value, mysql_date))) {
        LOG_WARN("failed to convert arrow date to mysql date", K(ret), K(item_value));
      } else {
        LOG_TRACE("got mysql date", K(item_value), K(mysql_date));

        ObMySQLDate min_mysql_date; // TODO get static minimum date value
        min_mysql_date.year_ = 1000;
        min_mysql_date.month_ = 1;
        min_mysql_date.day_ = 1;
        if (lib::is_mysql_mode() && mysql_date < min_mysql_date) {
          mysql_date = ObTimeConverter::MYSQL_ZERO_DATE;
          LOG_TRACE("date is too small, convert to zero_date", K(mysql_date), K(min_mysql_date));
        }
        out_vector->set_mysql_date(i, mysql_date);
      }
    } else if (ObYearType == expr->datum_meta_.get_type()) {
      uint8_t year = 0;
      if (OB_FAIL(ObTimeConverter::date_to_year(item_value, year))) {
        LOG_WARN("failed to convert arrow date to year", K(ret), K(item_value));
      } else {
        LOG_TRACE("got mysql year", K(item_value), K(year));
        out_vector->set_year(i, year);
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only ObMySQLDateType and ObYearType supported", K(expr->datum_meta_));
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObTimeArrowDataLoader
template <typename ArrowType>
int ObTimeArrowDataLoader<ArrowType>::init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type)
{
  int ret = OB_SUCCESS;
  const ArrowType &time_type = static_cast<const ArrowType &>(arrow_type);
  switch (time_type.unit()) {
    case TimeUnit::SECOND: {
      muliples_ = USECS_PER_SEC;
    } break;
    case TimeUnit::MILLI: {
      muliples_ = USECS_PER_MSEC;
    } break;
    case TimeUnit::MICRO: {
      muliples_ = 1;
    } break;
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("load time not supported", KCSTRING(arrow_type.ToString().c_str()));
    }
  }
  return ret;
}

/// NOTE timestamp from arrow always use UTC and we ignore the timezone in the DataType.
template <typename ArrowType>
int ObTimeArrowDataLoader<ArrowType>::load(const Array &arrow_array, sql::ObEvalCtx &eval_ctx, sql::ObExpr *expr)
{
  int ret = OB_SUCCESS;
  const NumericArray<ArrowType> &time_array = static_cast<const NumericArray<ArrowType> &>(arrow_array);
  ObIVector *out_vec = expr->get_vector(eval_ctx);

  if (OB_UNLIKELY(muliples_ <= 0)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(muliples_));
  }

  int64_t arrow_value = 0;
  int64_t timestamp = 0;
  ObMySQLDateTime mysql_time;
  for (int64_t i = 0; OB_SUCC(ret) && i < time_array.length(); i++) {
    if (time_array.IsNull(i)) {
      out_vec->set_null(i);
    } else if (FALSE_IT(arrow_value = time_array.Value(i))) {
    } else if (FALSE_IT(timestamp = arrow_value * muliples_)) {
    } else {
      timestamp = timestamp < DATETIME_MIN_VAL ? ObTimeConverter::ZERO_DATETIME : timestamp;
      if (ObMySQLDateTimeType == expr->datum_meta_.get_type()) {
        // ObTimeConverter::timestamp_to_mdatetime should use timezone but I don't care the timezone
        if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(timestamp, mysql_time))) {
          LOG_WARN("failed to convert arrow timestamp to mysql date time", K(ret), K(timestamp));
        } else {
          ObMySQLDateTime min_datetime;
          min_datetime.microseconds_ = 0;
          min_datetime.second_ = 0;
          min_datetime.minute_ = 0;
          min_datetime.hour_ = 0;

          out_vec->set_mysql_datetime(i, mysql_time);
        }
      } else { // TIME or TIMESTAMP
        // We should call set_time for TIME field and set_timestamp for TIMESTAMP field.
        // But the final effect is the same so I just call `set_time` for both types.
        out_vec->set_time(i, timestamp);
      }
    }
  }
  return ret;
}

} // namespace external
} // namespace plugin
} // namespace oceanbase
