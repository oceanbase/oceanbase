/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "plugin/external_table/ob_external_arrow_object.h"
#include "plugin/sys/ob_plugin_utils.h"

#include "common/object/ob_object.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase {

using namespace common;

namespace plugin {
namespace external {

int ObArrowObject::get_array(const ObObj &obj, shared_ptr<Array> &array_value_ret)
{
  int ret = OB_SUCCESS;
  ObArrowStatus obstatus(ret);
  array_value_ret.reset();
  shared_ptr<Scalar> scalar_value;
  unique_ptr<ArrayBuilder> array_builder;
  if (OB_FAIL(get_scalar_value(obj, scalar_value))) {
    LOG_WARN("failed to build scalar value form obj", K(ret), K(obj));
  } else if (!scalar_value) {
    LOG_TRACE("cant't build scalar from obobj");
  } else if (OBARROW_FAIL(MakeBuilder(scalar_value->type).Value(&array_builder))) {
    LOG_WARN("failed to create array builder", K(obstatus));
  } else if (OBARROW_FAIL(array_builder->AppendScalar(*scalar_value))) {
    LOG_WARN("failed to append scalar", K(obstatus));
  } else if (OBARROW_FAIL(array_builder->Finish().Value(&array_value_ret))) {
    LOG_WARN("failed to build array", K(obstatus));
  }
  return ret;
}
int ObArrowObject::get_scalar_value(const ObObj &value, shared_ptr<Scalar> &value_scalar_ret)
{
  int ret = OB_SUCCESS;
  value_scalar_ret.reset();

  ObArrowStatus obstatus(ret);
  shared_ptr<Scalar> value_scalar;
  const ObObjType value_type = value.get_type();

  OB_TRY_BEGIN;
  if (value.is_invalid_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("value is invalid", K(value));
  } else if (value.is_null()) {
    value_scalar = MakeNullScalar(arrow::int64()); // null type cannot be append to array
  } else if (value.is_unsigned_integer()) {
    uint64_t uint_value;
    if (FALSE_IT(uint_value = value.get_uint64())) {
    } else if (OBARROW_FAIL(arrow::MakeScalar(arrow::uint64(), uint_value).Value(&value_scalar))) {
      LOG_WARN("failed to create uint64 scalar", K(obstatus));
    }
  } else if (value.is_signed_integer()) {
    int64_t int_value;
    if (FALSE_IT(int_value = value.get_int())) {
    } else if (OBARROW_FAIL(arrow::MakeScalar(arrow::int64(), int_value).Value(&value_scalar))) {
      LOG_WARN("failed to create int64 scalar", K(obstatus));
    }
  } else if (ob_is_real_type(value.get_type())) {
    double double_value = 0.0;
    if (ob_is_float_type(value.get_type())) {
      double_value = value.get_float();
    } else if (ob_is_double_type(value.get_type())) {
      double_value = value.get_double();
    }
    if (OBARROW_FAIL(arrow::MakeScalar(arrow::float64(), double_value).Value(&value_scalar))) {
      LOG_WARN("failed to create float64 scalar", K(obstatus));
    }
  } else if (ob_is_string_or_lob_type(value.get_type())) {
    ObString string_value;
    ObString converted_string_value;
    ObCollationType ob_collation_type;
    ObMalloc malloc(ObMemAttr(MTL_ID(), OB_PLUGIN_MEMORY_LABEL));
    if (OB_FAIL(value.get_string(string_value))) {
      LOG_WARN("failed to get_string from object", K(value));
    } else if (FALSE_IT(ob_collation_type = value.get_collation_type())) {
    } else if (CHARSET_BINARY == ObCharset::charset_type_by_coll(ob_collation_type)) {
      if (OBARROW_FAIL(arrow::MakeScalar(arrow::binary(), std::string(string_value.ptr(), string_value.length()))
                       .Value(&value_scalar))) {
        LOG_WARN("failed to create binary scalar", K(obstatus), K(string_value));
      }
    } else if (OB_FAIL(ObCharset::charset_convert(
        malloc, string_value, ob_collation_type, CS_TYPE_UTF8MB4_BIN/*dst_cs_type*/, converted_string_value))) {
      LOG_WARN("failed to convert string to destination charset", K(ob_collation_type), K(ret));
    } else if (OBARROW_FAIL(arrow::MakeScalar(arrow::utf8(),
                                              std::string(converted_string_value.ptr(), converted_string_value.length()))
                            .Value(&value_scalar))) {
      LOG_WARN("failed to create arrow string scalar", K(obstatus), K(converted_string_value));
    }

    if (OB_NOT_NULL(converted_string_value.ptr()) && converted_string_value.ptr() != string_value.ptr()) {
      malloc.free(converted_string_value.ptr());
    }
  } else if (ob_is_decimal_int(value.get_type())) {
    const ObDecimalInt *decimal_value = value.get_decimal_int();
    shared_ptr<DataType> decimal_type;
    ObDecimalIntBuilder decimal_builder;
    if (OB_ISNULL(decimal_value)) {
      value_scalar = MakeNullScalar(decimal_type);
    } else if (value.get_int_bytes() > Decimal256Type::kByteWidth) {
      // not supported
      LOG_INFO("decimal bytes larger than Decimal256Type::kByteWidth not supported",
          K(value.get_int_bytes()), K(Decimal256Type::kByteWidth));
    } else if (FALSE_IT(decimal_builder.from(decimal_value, value.get_int_bytes()))) {
    } else if (FALSE_IT(decimal_builder.extend(Decimal256Type::kByteWidth))) {
    } else if (OBARROW_FAIL(Decimal256Type::Make(Decimal256Type::kMaxPrecision, value.get_scale())
                            .Value(&decimal_type))) {
      LOG_WARN("create decimal type failed", K(obstatus), K(value.meta_));
    } else if (OBARROW_FAIL(arrow::MakeScalar(decimal_type, Decimal256((uint8_t*)decimal_builder.get_buffer()))
                            .Value(&value_scalar))) {
      LOG_WARN("failed to create decimal scalar", K(value), K(obstatus));
    }
  } else if (ObMySQLDateType == value.get_type() ||
             ObMySQLDateTimeType == value.get_type() ||
             ObTimestampType == value.get_type() ||
             ObTimeType == value.get_type()) {
    const int64_t buf_len = 64;
    char buff[64];
    int64_t pos = 0;
    value.print_plain_str_literal(buff, buf_len, pos);
    value_scalar = arrow::MakeScalar(std::string(buff, pos));
  } else {
    // do nothing
    // other types not supported yet
    LOG_TRACE("value type not supported yet", K(value.get_type()));
  }

  if (!value_scalar) {
    // ret = OB_NOT_SUPPORTED;
  } else {
    value_scalar_ret = value_scalar;
    LOG_DEBUG("convert from ob object to arrow scalar", K(value), KCSTRING(value_scalar->ToString().c_str()));
  }
  OB_TRY_END;
  return ret;
}

} // namespace external
} // namespace plugin
} // namespace oceanbase
