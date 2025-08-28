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
#define USING_LOG_PREFIX SQL

#include "sql/table_format/iceberg/scan/conversions.h"

#include "common/object/ob_obj_type.h"
#include "lib/wide_integer/ob_wide_integer_helper.h"
#include "share/schema/ob_column_schema.h"
#include "sql/engine/table/ob_external_table_pushdown_filter.h"

#include <boost/endian/arithmetic.hpp>

namespace oceanbase
{

namespace sql
{

namespace iceberg
{

int Conversions::convert_statistics_binary_to_ob_obj(
    ObIAllocator &allocator,
    const ObString &binary,
    const ObObjType ob_obj_type,
    const std::optional<ObCollationType> collation_type,
    const std::optional<int16_t> ob_type_precision,
    const std::optional<int16_t> ob_type_scale,
    ObObj &ob_obj)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    LOG_WARN("oracle is not supported", K(ret));
    ret = OB_NOT_SUPPORTED;
  }

  if (OB_SUCC(ret)) {
    switch (ob_obj_type) {
      case ObTinyIntType: {
        // aka boolean
        ob_obj.set_bool(*binary.ptr() != 0x00);
        break;
      }
      case ObInt32Type: {
        // aka int
        int32_t val
            = boost::endian::endian_load<int32_t, sizeof(int32_t), boost::endian::order::little>(
                reinterpret_cast<unsigned char const *>(binary.ptr()));
        ob_obj.set_int32(val);
        break;
      }
      case ObIntType: {
        // aka long
        int64_t val
            = boost::endian::endian_load<int64_t, sizeof(int64_t), boost::endian::order::little>(
                reinterpret_cast<unsigned char const *>(binary.ptr()));
        ob_obj.set_int(val);
        break;
      }
      case ObFloatType: {
        // aka float
        float val = boost::endian::endian_load<float, sizeof(float), boost::endian::order::little>(
            reinterpret_cast<unsigned char const *>(binary.ptr()));
        ob_obj.set_float(val);
        break;
      }
      case ObDoubleType: {
        // aka double
        double val
            = boost::endian::endian_load<double, sizeof(double), boost::endian::order::little>(
                reinterpret_cast<unsigned char const *>(binary.ptr()));
        ob_obj.set_double(val);
        break;
      }
      case ObDecimalIntType: {
        // aka decimal
        if (!ob_type_precision.has_value() || !ob_type_scale.has_value()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("DecimalInt must has valid precision and scale", K(ret));
        } else {
          int32_t buffer_size
              = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(ob_type_precision.value());
          char *buf = NULL;
          if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buffer_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret));
          } else {
            // fill 1 when the input value is negative, otherwise fill 0
            memset(buf, (binary.ptr()[0] >> 8), buffer_size);
            int32_t write_index = 0;
            for (int32_t i = binary.length() - 1; i >= 0; i--) {
              buf[write_index] = binary.ptr()[i];
              write_index++;
            }
            ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
            ob_obj.set_decimal_int(buffer_size, ob_type_scale.value(), decint);
          }
        }
        break;
      }
      case ObDateType: {
        // aka date
        int32_t val
            = boost::endian::endian_load<int32_t, sizeof(int32_t), boost::endian::order::little>(
                reinterpret_cast<unsigned char const *>(binary.ptr()));
        ob_obj.set_date(val);
        break;
      }
      case ObTimeType: {
        // aka time
        int64_t val
            = boost::endian::endian_load<int64_t, sizeof(int64_t), boost::endian::order::little>(
                reinterpret_cast<unsigned char const *>(binary.ptr()));
        ob_obj.set_time(val);
        break;
      }
      case ObDateTimeType: {
        // aka timestamp
        int64_t val
            = boost::endian::endian_load<int64_t, sizeof(int64_t), boost::endian::order::little>(
                reinterpret_cast<unsigned char const *>(binary.ptr()));
        ob_obj.set_datetime(val);
        break;
      }
      case ObTimestampType: {
        // aka timestamp_tz
        int64_t val
            = boost::endian::endian_load<int64_t, sizeof(int64_t), boost::endian::order::little>(
                reinterpret_cast<unsigned char const *>(binary.ptr()));
        ob_obj.set_timestamp(val);
        break;
      }
      case ObVarcharType: {
        // aka string / binary
        ObString tmp;
        if (!collation_type.has_value()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("collation_type must not be null", K(ret));
        } else if (OB_FAIL(ob_write_string(allocator, binary, tmp))) {
          LOG_WARN("failed to copy string", K(tmp), K(ret));
        } else {
          ob_obj.set_varchar(tmp);
          ob_obj.set_collation_type(collation_type.value());
        }
        break;
      }
      default:
        LOG_WARN("unsupported ob type", K(ob_obj_type), K(ret));
        ret = OB_NOT_SUPPORTED;
    }
  }

  return ret;
}

int Conversions::convert_statistics_binary_to_ob_obj(
    ObIAllocator &allocator,
    const ObString &binary,
    const share::schema::ObColumnSchemaV2 &column_schema,
    ObObj &ob_obj)
{
  int ret = OB_SUCCESS;
  ObObjType type = column_schema.get_data_type();
  if (ObObjType::ObDecimalIntType == type) {
    if (OB_FAIL(convert_statistics_binary_to_ob_obj(allocator,
                                                    binary,
                                                    type,
                                                    std::nullopt,
                                                    column_schema.get_data_precision(),
                                                    column_schema.get_data_scale(),
                                                    ob_obj))) {
      LOG_WARN("failed to convert statistics binary to ob object", K(ret));
    }
  } else if (ObObjType::ObVarcharType == type) {
    if (OB_FAIL(convert_statistics_binary_to_ob_obj(allocator,
                                                    binary,
                                                    type,
                                                    column_schema.get_collation_type(),
                                                    std::nullopt,
                                                    std::nullopt,
                                                    ob_obj))) {
      LOG_WARN("failed to convert statistics binary to ob object", K(ret));
    }
  } else {
    if (OB_FAIL(convert_statistics_binary_to_ob_obj(allocator,
                                                    binary,
                                                    type,
                                                    std::nullopt,
                                                    std::nullopt,
                                                    std::nullopt,
                                                    ob_obj))) {
      LOG_WARN("failed to convert statistics binary to ob object", K(ret));
    }
  }
  return ret;
}

int Conversions::convert_statistics_binary_to_ob_obj(ObIAllocator &allocator,
                                                     const ObString &binary,
                                                     const ObColumnMeta &column_meta,
                                                     ObObj &ob_obj)
{
  int ret = OB_SUCCESS;
  ObObjType type = column_meta.type_;
  if (ObObjType::ObDecimalIntType == type) {
    if (OB_FAIL(convert_statistics_binary_to_ob_obj(allocator,
                                                    binary,
                                                    type,
                                                    std::nullopt,
                                                    column_meta.precision_,
                                                    column_meta.scale_,
                                                    ob_obj))) {
      LOG_WARN("failed to convert statistics binary to ob object", K(ret));
    }
  } else if (ObObjType::ObVarcharType == type) {
    if (OB_FAIL(convert_statistics_binary_to_ob_obj(allocator,
                                                    binary,
                                                    type,
                                                    column_meta.cs_type_,
                                                    std::nullopt,
                                                    std::nullopt,
                                                    ob_obj))) {
      LOG_WARN("failed to convert statistics binary to ob object", K(ret));
    }
  } else {
    if (OB_FAIL(convert_statistics_binary_to_ob_obj(allocator,
                                                    binary,
                                                    type,
                                                    std::nullopt,
                                                    std::nullopt,
                                                    std::nullopt,
                                                    ob_obj))) {
      LOG_WARN("failed to convert statistics binary to ob object", K(ret));
    }
  }
  return ret;
}

// 数组 index 数 precision 0 <= precision < 40
// value 是所需 bytes
static const int32_t DECIMAL_REQUIRED_BYTES[40]
    = {0, 1, 1,  2,  2,  3,  3,  4,  4,  4,  5,  5,  6,  6,  6,  7,  7,  8,  8,  9,
       9, 9, 10, 10, 11, 11, 11, 12, 12, 13, 13, 13, 14, 14, 15, 15, 16, 16, 16, 17};

int Conversions::decimal_required_bytes(int32_t precision, int32_t &required_bytes)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(precision < 0 || precision >= 40)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid decimal precision", K(ret), K(precision));
  } else {
    required_bytes = DECIMAL_REQUIRED_BYTES[precision];
  }
  return ret;
}

} // namespace iceberg

} // namespace sql

} // namespace oceanbase