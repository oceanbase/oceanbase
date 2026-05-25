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

#define USING_LOG_PREFIX PL

#include "pl/external_routine/ob_py_udf_arrow.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/ipc/reader.h"
#include "arrow/builder.h"
#include "lib/string/ob_string.h"
#include "lib/number/ob_number_v2.h"
#include "lib/timezone/ob_time_convert.h"

namespace oceanbase
{
namespace pl
{

static std::shared_ptr<arrow::DataType> ob_type_to_arrow(const common::ObObjMeta &meta)
{
  std::shared_ptr<arrow::DataType> result = nullptr;
  switch (meta.get_type()) {
    case common::ObTinyIntType:
    case common::ObSmallIntType:
    case common::ObMediumIntType:
    case common::ObInt32Type:
    case common::ObIntType:
    case common::ObYearType:
      result = arrow::int64();
      break;
    case common::ObUTinyIntType:
    case common::ObUSmallIntType:
    case common::ObUMediumIntType:
    case common::ObUInt32Type:
    case common::ObUInt64Type:
      result = arrow::uint64();
      break;
    case common::ObFloatType:
      result = arrow::float32();
      break;
    case common::ObDoubleType:
      result = arrow::float64();
      break;
    case common::ObVarcharType:
    case common::ObCharType:
    case common::ObNVarchar2Type:
      result = arrow::binary();
      break;
    case common::ObRawType:
    case common::ObHexStringType:
      result = arrow::binary();
      break;
    case common::ObNumberType:
    case common::ObDecimalIntType:
      // Use utf8 string instead of Arrow Decimal128/256 because:
      // 1. OB supports DECIMAL(65,30) with up to 65-digit precision,
      //    but Arrow Decimal128 only supports up to 38 digits,
      //    and PyArrow has poor support for Decimal256.
      // 2. String representation avoids precision loss from
      //    ObNumber -> int128 conversion (scale alignment, rounding).
      // 3. Python side uses Decimal(str) for lossless construction.
      result = arrow::utf8();
      break;
    case common::ObDateTimeType:
    case common::ObTimestampType:
      result = arrow::timestamp(arrow::TimeUnit::MICRO);
      break;
    case common::ObMySQLDateTimeType:
    case common::ObDateType:
    case common::ObMySQLDateType:
      result = arrow::binary();  // pass as string to avoid internal format issues
      break;
    default:
      break;  // caller checks nullptr and returns OB_NOT_SUPPORTED
  }
  return result;
}

static int append_obj_to_builder(const common::ObObj &obj,
                                 const common::ObObjMeta &meta,
                                 arrow::ArrayBuilder *builder)
{
  int ret = common::OB_SUCCESS;
  arrow::Status st;

  if (obj.is_null()) {
    st = builder->AppendNull();
    if (!st.ok()) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("arrow AppendNull failed", K(ret));
    }
  } else {
    switch (meta.get_type()) {
      case common::ObTinyIntType:
      case common::ObSmallIntType:
      case common::ObMediumIntType:
      case common::ObInt32Type:
      case common::ObIntType: {
        arrow::Int64Builder *b = static_cast<arrow::Int64Builder *>(builder);
        st = b->Append(obj.get_int());
        break;
      }
      case common::ObYearType: {
        arrow::Int64Builder *b = static_cast<arrow::Int64Builder *>(builder);
        // YEAR stores offset from 1900; pass actual year to Python
        st = b->Append(static_cast<int64_t>(obj.get_year()) + 1900);
        break;
      }
      case common::ObUTinyIntType:
      case common::ObUSmallIntType:
      case common::ObUMediumIntType:
      case common::ObUInt32Type:
      case common::ObUInt64Type: {
        arrow::UInt64Builder *b = static_cast<arrow::UInt64Builder *>(builder);
        st = b->Append(obj.get_uint64());
        break;
      }
      case common::ObFloatType: {
        arrow::FloatBuilder *b = static_cast<arrow::FloatBuilder *>(builder);
        st = b->Append(obj.get_float());
        break;
      }
      case common::ObDoubleType: {
        arrow::DoubleBuilder *b = static_cast<arrow::DoubleBuilder *>(builder);
        st = b->Append(obj.get_double());
        break;
      }
      case common::ObVarcharType:
      case common::ObCharType:
      case common::ObNVarchar2Type: {
        arrow::BinaryBuilder *b = static_cast<arrow::BinaryBuilder *>(builder);
        common::ObString s = obj.get_string();
        st = b->Append(reinterpret_cast<const uint8_t *>(s.ptr()), s.length());
        break;
      }
      case common::ObRawType:
      case common::ObHexStringType: {
        arrow::BinaryBuilder *b = static_cast<arrow::BinaryBuilder *>(builder);
        common::ObString s = obj.get_string();
        st = b->Append(s.ptr(), s.length());
        break;
      }
      case common::ObNumberType:
      case common::ObDecimalIntType: {
        // Serialize number as string for Python decimal.Decimal (see utf8 rationale above)
        arrow::StringBuilder *b = static_cast<arrow::StringBuilder *>(builder);
        char buf[common::number::ObNumber::MAX_PRINTABLE_SIZE];
        int64_t pos = 0;
        if (OB_FAIL(obj.get_number().format(buf, sizeof(buf), pos, -1))) {
          LOG_WARN("failed to format number", K(ret));
        } else {
          st = b->Append(buf, static_cast<int32_t>(pos));
        }
        break;
      }
      case common::ObDateTimeType:
      case common::ObTimestampType: {
        arrow::TimestampBuilder *b = static_cast<arrow::TimestampBuilder *>(builder);
        st = b->Append(obj.get_datetime());  // already in microseconds since epoch
        break;
      }
      case common::ObMySQLDateTimeType: {
        // ObMySQLDateTimeType uses packed bit-field format, convert to string
        arrow::BinaryBuilder *b = static_cast<arrow::BinaryBuilder *>(builder);
        common::ObMySQLDateTime mdt;
        mdt.datetime_ = obj.get_datetime();
        char buf[64] = {0};
        int64_t pos = 0;
        if (OB_FAIL(common::ObTimeConverter::mdatetime_to_str(
                mdt, nullptr, common::ObString(), 6, buf, sizeof(buf), pos))) {
          LOG_WARN("failed to format mysql datetime", K(ret));
        } else {
          st = b->Append(reinterpret_cast<const uint8_t *>(buf), static_cast<int32_t>(pos));
        }
        break;
      }
      case common::ObDateType: {
        arrow::BinaryBuilder *b = static_cast<arrow::BinaryBuilder *>(builder);
        char buf[16] = {0};
        int64_t pos = 0;
        if (OB_FAIL(common::ObTimeConverter::date_to_str(
                obj.get_date(), buf, sizeof(buf), pos))) {
          LOG_WARN("failed to format date", K(ret));
        } else {
          st = b->Append(reinterpret_cast<const uint8_t *>(buf), static_cast<int32_t>(pos));
        }
        break;
      }
      case common::ObMySQLDateType: {
        arrow::BinaryBuilder *b = static_cast<arrow::BinaryBuilder *>(builder);
        char buf[16] = {0};
        int64_t pos = 0;
        common::ObMySQLDate mdate = obj.get_mysql_date();
        if (OB_FAIL(common::ObTimeConverter::mdate_to_str(mdate, buf, sizeof(buf), pos))) {
          LOG_WARN("failed to format mysql date", K(ret));
        } else {
          st = b->Append(reinterpret_cast<const uint8_t *>(buf), static_cast<int32_t>(pos));
        }
        break;
      }
      default: {
        ret = common::OB_NOT_SUPPORTED;
        LOG_WARN("unsupported obj type for arrow conversion", K(ret), K(meta.get_type()));
        break;
      }
    } // switch
  } // else (!is_null)

  if (OB_SUCC(ret) && !st.ok()) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("arrow append failed", K(ret), K(meta.get_type()));
  }

  return ret;
}

int ob_udf_args_to_arrow(
    int64_t udf_id,
    const common::ObString &mode,
    const common::ObIArray<common::ObObjMeta> &arg_types,
    const common::ObIArray<common::ObIArray<common::ObObj>*> &args,
    int64_t batch_size,
    const common::ObObjMeta &result_meta,
    sql::ObArrowMemPool &pool,
    std::shared_ptr<arrow::Buffer> &out_buf)
{
  int ret = common::OB_SUCCESS;
  try {
    int64_t num_cols = arg_types.count();

    // Build schema fields
    arrow::FieldVector fields;
    fields.reserve(num_cols);
    for (int64_t col = 0; OB_SUCC(ret) && col < num_cols; ++col) {
      std::shared_ptr<arrow::DataType> arrow_type = ob_type_to_arrow(arg_types.at(col));
      if (arrow_type == nullptr) {
        ret = common::OB_NOT_SUPPORTED;
        LOG_WARN("unsupported OB type for arrow", K(ret), K(col), K(arg_types.at(col).get_type()));
      } else {
        fields.push_back(arrow::field(std::to_string(col), arrow_type));
      }
    }

    // Create builders and fill data
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(num_cols);

    for (int64_t col = 0; OB_SUCC(ret) && col < num_cols; ++col) {
      std::shared_ptr<arrow::DataType> arrow_type = ob_type_to_arrow(arg_types.at(col));
      arrow::Result<std::unique_ptr<arrow::ArrayBuilder>> builder_result = arrow::MakeBuilder(arrow_type, &pool);
      if (!builder_result.ok()) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create arrow builder", K(ret), K(col));
        break;
      }
      std::unique_ptr<arrow::ArrayBuilder> builder = std::move(*builder_result);

      const common::ObIArray<common::ObObj> *col_data = args.at(col);
      if (OB_ISNULL(col_data)) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("args column data is null", K(ret), K(col));
        break;
      }
      for (int64_t row = 0; OB_SUCC(ret) && row < batch_size; ++row) {
        if (OB_FAIL(append_obj_to_builder(col_data->at(row), arg_types.at(col), builder.get()))) {
          LOG_WARN("failed to append obj to arrow builder", K(ret), K(col), K(row));
        }
      }

      if (OB_SUCC(ret)) {
        arrow::Result<std::shared_ptr<arrow::Array>> finish_result = builder->Finish();
        if (!finish_result.ok()) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("failed to finish arrow array", K(ret), K(col));
        } else {
          arrays.push_back(std::move(*finish_result));
        }
      }
    }

    if (OB_SUCC(ret)) {
      // Add metadata (including return arrow type for Python-side array construction)
      char udf_id_str[32];
      snprintf(udf_id_str, sizeof(udf_id_str), "%ld", udf_id);
      std::shared_ptr<arrow::DataType> ret_arrow_type = ob_type_to_arrow(result_meta);
      std::string ret_type_str = ret_arrow_type ? ret_arrow_type->ToString() : "";
      std::shared_ptr<arrow::KeyValueMetadata> metadata = arrow::KeyValueMetadata::Make(
          {"ob_udf_id", "ob_mode", "ob_ret_type"},
          {std::string(udf_id_str), std::string(mode.ptr(), mode.length()), ret_type_str});

      std::shared_ptr<arrow::Schema> schema = arrow::schema(fields, metadata);
      std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(schema, batch_size, arrays);

      // Serialize to IPC stream
      arrow::Result<std::shared_ptr<arrow::io::BufferOutputStream>> sink_result =
          arrow::io::BufferOutputStream::Create(4096, &pool);
      if (!sink_result.ok()) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create buffer output stream", K(ret));
      } else {
        std::shared_ptr<arrow::io::BufferOutputStream> sink = std::move(*sink_result);
        arrow::ipc::IpcWriteOptions write_opts = arrow::ipc::IpcWriteOptions::Defaults();
        write_opts.memory_pool = &pool;
        arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>> writer_result =
            arrow::ipc::MakeStreamWriter(sink, schema, write_opts);
        if (!writer_result.ok()) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("failed to create arrow stream writer", K(ret));
        } else {
          std::shared_ptr<arrow::ipc::RecordBatchWriter> writer = std::move(*writer_result);
          arrow::Status st = writer->WriteRecordBatch(*batch);
          if (!st.ok()) {
            ret = common::OB_ERR_UNEXPECTED;
            LOG_WARN("failed to write record batch", K(ret));
          } else {
            st = writer->Close();
            if (!st.ok()) {
              ret = common::OB_ERR_UNEXPECTED;
              LOG_WARN("failed to close arrow writer", K(ret));
            } else {
              arrow::Result<std::shared_ptr<arrow::Buffer>> buf_result = sink->Finish();
              if (!buf_result.ok()) {
                ret = common::OB_ERR_UNEXPECTED;
                LOG_WARN("failed to finish buffer output stream", K(ret));
              } else {
                out_buf = std::move(*buf_result);
              }
            }
          }
        }
      }
    }
  } catch (const sql::ObErrorCodeException &ob_error) {
    if (OB_SUCC(ret)) {
      ret = ob_error.get_error_code();
      LOG_WARN("arrow operation failed with OB error", K(ret));
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected exception in arrow serialization", K(ret));
    }
  }

  return ret;
}

static int64_t arrow_scalar_to_int64(const std::shared_ptr<arrow::Scalar> &scalar,
                                     bool &valid)
{
  int64_t result = 0;
  valid = true;
  switch (scalar->type->id()) {
    case arrow::Type::BOOL:
      result = std::static_pointer_cast<arrow::BooleanScalar>(scalar)->value ? 1 : 0;
      break;
    case arrow::Type::INT8:
      result = std::static_pointer_cast<arrow::Int8Scalar>(scalar)->value;
      break;
    case arrow::Type::INT16:
      result = std::static_pointer_cast<arrow::Int16Scalar>(scalar)->value;
      break;
    case arrow::Type::INT32:
      result = std::static_pointer_cast<arrow::Int32Scalar>(scalar)->value;
      break;
    case arrow::Type::INT64:
      result = std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value;
      break;
    case arrow::Type::DOUBLE:
      result = static_cast<int64_t>(std::static_pointer_cast<arrow::DoubleScalar>(scalar)->value);
      break;
    case arrow::Type::FLOAT:
      result = static_cast<int64_t>(std::static_pointer_cast<arrow::FloatScalar>(scalar)->value);
      break;
    default:
      valid = false;
      LOG_WARN_RET(common::OB_ERR_UNEXPECTED, "unexpected arrow type for int64 conversion",
               K(scalar->type->id()));
      break;
  }
  return result;
}

static uint64_t arrow_scalar_to_uint64(const std::shared_ptr<arrow::Scalar> &scalar,
                                       bool &valid)
{
  uint64_t result = 0;
  valid = true;
  switch (scalar->type->id()) {
    case arrow::Type::BOOL:
      result = std::static_pointer_cast<arrow::BooleanScalar>(scalar)->value ? 1 : 0;
      break;
    case arrow::Type::UINT8:
      result = std::static_pointer_cast<arrow::UInt8Scalar>(scalar)->value;
      break;
    case arrow::Type::UINT16:
      result = std::static_pointer_cast<arrow::UInt16Scalar>(scalar)->value;
      break;
    case arrow::Type::UINT32:
      result = std::static_pointer_cast<arrow::UInt32Scalar>(scalar)->value;
      break;
    case arrow::Type::UINT64:
      result = std::static_pointer_cast<arrow::UInt64Scalar>(scalar)->value;
      break;
    case arrow::Type::INT64:
      result = static_cast<uint64_t>(std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value);
      break;
    default:
      valid = false;
      LOG_WARN_RET(common::OB_ERR_UNEXPECTED, "unexpected arrow type for uint64 conversion",
               K(scalar->type->id()));
      break;
  }
  return result;
}

static int arrow_scalar_to_obj(const std::shared_ptr<arrow::Scalar> &scalar,
                               const common::ObObjMeta &result_meta,
                               common::ObIAllocator &allocator,
                               common::ObObj &obj)
{
  int ret = common::OB_SUCCESS;

  if (!scalar->is_valid) {
    obj.set_null();
  } else {
    bool convert_valid = true;
    switch (result_meta.get_type()) {
      case common::ObTinyIntType: {
        int64_t v = arrow_scalar_to_int64(scalar, convert_valid);
        if (!convert_valid) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar type conversion failed", K(ret), K(scalar->type->id()), K(result_meta.get_type()));
        } else if (OB_UNLIKELY(v < INT8_MIN || v > INT8_MAX)) {
          ret = common::OB_DATA_OUT_OF_RANGE;
          LOG_WARN("python udf returned value out of tinyint range", K(ret), K(v));
        } else {
          obj.set_tinyint(static_cast<int8_t>(v));
        }
        break;
      }
      case common::ObSmallIntType: {
        int64_t v = arrow_scalar_to_int64(scalar, convert_valid);
        if (!convert_valid) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar type conversion failed", K(ret), K(scalar->type->id()), K(result_meta.get_type()));
        } else if (OB_UNLIKELY(v < INT16_MIN || v > INT16_MAX)) {
          ret = common::OB_DATA_OUT_OF_RANGE;
          LOG_WARN("python udf returned value out of smallint range", K(ret), K(v));
        } else {
          obj.set_smallint(static_cast<int16_t>(v));
        }
        break;
      }
      case common::ObMediumIntType: {
        int64_t v = arrow_scalar_to_int64(scalar, convert_valid);
        if (!convert_valid) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar type conversion failed", K(ret), K(scalar->type->id()), K(result_meta.get_type()));
        } else if (OB_UNLIKELY(v < -8388608LL || v > 8388607LL)) {
          ret = common::OB_DATA_OUT_OF_RANGE;
          LOG_WARN("python udf returned value out of mediumint range", K(ret), K(v));
        } else {
          obj.set_mediumint(static_cast<int32_t>(v));
        }
        break;
      }
      case common::ObInt32Type: {
        int64_t v = arrow_scalar_to_int64(scalar, convert_valid);
        if (!convert_valid) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar type conversion failed", K(ret), K(scalar->type->id()), K(result_meta.get_type()));
        } else if (OB_UNLIKELY(v < INT32_MIN || v > INT32_MAX)) {
          ret = common::OB_DATA_OUT_OF_RANGE;
          LOG_WARN("python udf returned value out of int32 range", K(ret), K(v));
        } else {
          obj.set_int32(static_cast<int32_t>(v));
        }
        break;
      }
      case common::ObIntType: {
        int64_t v = arrow_scalar_to_int64(scalar, convert_valid);
        if (!convert_valid) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar type conversion failed", K(ret), K(scalar->type->id()), K(result_meta.get_type()));
        } else {
          obj.set_int(v);
        }
        break;
      }
      case common::ObYearType: {
        int64_t v = arrow_scalar_to_int64(scalar, convert_valid);
        if (!convert_valid) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar type conversion failed", K(ret), K(scalar->type->id()), K(result_meta.get_type()));
        } else if (OB_UNLIKELY(v < 1901 || v > 2155)) {
          ret = common::OB_DATA_OUT_OF_RANGE;
          LOG_WARN("python udf returned value out of year range", K(ret), K(v));
        } else {
          obj.set_year(static_cast<uint8_t>(v - 1900));
        }
        break;
      }
      case common::ObUTinyIntType: {
        uint64_t v = arrow_scalar_to_uint64(scalar, convert_valid);
        if (!convert_valid) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar type conversion failed", K(ret), K(scalar->type->id()), K(result_meta.get_type()));
        } else if (OB_UNLIKELY(v > UINT8_MAX)) {
          ret = common::OB_DATA_OUT_OF_RANGE;
          LOG_WARN("python udf returned value out of utinyint range", K(ret), K(v));
        } else {
          obj.set_utinyint(static_cast<uint8_t>(v));
        }
        break;
      }
      case common::ObUSmallIntType: {
        uint64_t v = arrow_scalar_to_uint64(scalar, convert_valid);
        if (!convert_valid) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar type conversion failed", K(ret), K(scalar->type->id()), K(result_meta.get_type()));
        } else if (OB_UNLIKELY(v > UINT16_MAX)) {
          ret = common::OB_DATA_OUT_OF_RANGE;
          LOG_WARN("python udf returned value out of usmallint range", K(ret), K(v));
        } else {
          obj.set_usmallint(static_cast<uint16_t>(v));
        }
        break;
      }
      case common::ObUMediumIntType: {
        uint64_t v = arrow_scalar_to_uint64(scalar, convert_valid);
        if (!convert_valid) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar type conversion failed", K(ret), K(scalar->type->id()), K(result_meta.get_type()));
        } else if (OB_UNLIKELY(v > 16777215ULL)) {
          ret = common::OB_DATA_OUT_OF_RANGE;
          LOG_WARN("python udf returned value out of umediumint range", K(ret), K(v));
        } else {
          obj.set_umediumint(static_cast<uint32_t>(v));
        }
        break;
      }
      case common::ObUInt32Type: {
        uint64_t v = arrow_scalar_to_uint64(scalar, convert_valid);
        if (!convert_valid) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar type conversion failed", K(ret), K(scalar->type->id()), K(result_meta.get_type()));
        } else if (OB_UNLIKELY(v > UINT32_MAX)) {
          ret = common::OB_DATA_OUT_OF_RANGE;
          LOG_WARN("python udf returned value out of uint32 range", K(ret), K(v));
        } else {
          obj.set_uint32(static_cast<uint32_t>(v));
        }
        break;
      }
      case common::ObUInt64Type: {
        uint64_t v = arrow_scalar_to_uint64(scalar, convert_valid);
        if (!convert_valid) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar type conversion failed", K(ret), K(scalar->type->id()), K(result_meta.get_type()));
        } else {
          obj.set_uint64(v);
        }
        break;
      }
      case common::ObFloatType: {
        // Python float is always 64-bit; PyArrow may return DoubleScalar
        if (scalar->type->id() == arrow::Type::DOUBLE) {
          std::shared_ptr<arrow::DoubleScalar> typed = std::static_pointer_cast<arrow::DoubleScalar>(scalar);
          obj.set_float(static_cast<float>(typed->value));
        } else if (scalar->type->id() == arrow::Type::FLOAT) {
          std::shared_ptr<arrow::FloatScalar> typed = std::static_pointer_cast<arrow::FloatScalar>(scalar);
          obj.set_float(typed->value);
        } else {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected arrow type for float conversion", K(ret), K(scalar->type->id()));
        }
        break;
      }
      case common::ObDoubleType: {
        // Python float is always 64-bit, but handle float32 just in case
        if (scalar->type->id() == arrow::Type::FLOAT) {
          std::shared_ptr<arrow::FloatScalar> typed = std::static_pointer_cast<arrow::FloatScalar>(scalar);
          obj.set_double(static_cast<double>(typed->value));
        } else if (scalar->type->id() == arrow::Type::DOUBLE) {
          std::shared_ptr<arrow::DoubleScalar> typed = std::static_pointer_cast<arrow::DoubleScalar>(scalar);
          obj.set_double(typed->value);
        } else {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected arrow type for double conversion", K(ret), K(scalar->type->id()));
        }
        break;
      }
      case common::ObVarcharType:
      case common::ObCharType:
      case common::ObNVarchar2Type: {
        // Python may return str (StringScalar/utf8) or bytes (BinaryScalar)
        std::shared_ptr<arrow::Buffer> buf;
        if (scalar->type->id() == arrow::Type::BINARY || scalar->type->id() == arrow::Type::LARGE_BINARY) {
          buf = std::static_pointer_cast<arrow::BinaryScalar>(scalar)->value;
        } else if (scalar->type->id() == arrow::Type::STRING || scalar->type->id() == arrow::Type::LARGE_STRING) {
          buf = std::static_pointer_cast<arrow::StringScalar>(scalar)->value;
        } else {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected arrow type for varchar/char conversion", K(ret), K(scalar->type->id()));
        }
        if (OB_ISNULL(buf)) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar buffer is null for varchar/char", K(ret));
        }
        int32_t len = 0;
        char *copy = nullptr;
        if (OB_SUCC(ret)) {
          len = static_cast<int32_t>(buf->size());
          if (len == 0) {
            obj.set_varchar(nullptr, 0);
            obj.set_meta_type(result_meta);
          } else {
            copy = static_cast<char *>(allocator.alloc(len));
            if (OB_ISNULL(copy)) {
              ret = common::OB_ALLOCATE_MEMORY_FAILED;
            } else {
              MEMCPY(copy, buf->data(), len);
              obj.set_varchar(copy, len);
              obj.set_meta_type(result_meta);
            }
          }
        }
        break;
      }
      case common::ObRawType:
      case common::ObHexStringType: {
        if (OB_UNLIKELY(scalar->type->id() != arrow::Type::BINARY
                        && scalar->type->id() != arrow::Type::LARGE_BINARY)) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected arrow type for raw/hex conversion", K(ret), K(scalar->type->id()));
          break;
        }
        std::shared_ptr<arrow::BinaryScalar> typed = std::static_pointer_cast<arrow::BinaryScalar>(scalar);
        std::shared_ptr<arrow::Buffer> buf = typed->value;
        if (OB_ISNULL(buf)) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("arrow scalar buffer is null for raw/hex", K(ret));
        }
        int32_t len = 0;
        char *copy = nullptr;
        if (OB_SUCC(ret)) {
          len = static_cast<int32_t>(buf->size());
          if (len == 0) {
            obj.set_raw(nullptr, 0);
          } else {
            copy = static_cast<char *>(allocator.alloc(len));
            if (OB_ISNULL(copy)) {
              ret = common::OB_ALLOCATE_MEMORY_FAILED;
            } else {
              MEMCPY(copy, buf->data(), len);
              obj.set_raw(copy, len);
            }
          }
        }
        break;
      }
      case common::ObNumberType:
      case common::ObDecimalIntType: {
        common::number::ObNumber num;
        if (scalar->type->id() == arrow::Type::DECIMAL128 ||
            scalar->type->id() == arrow::Type::DECIMAL256) {
          // Python decimal.Decimal values are serialized as Arrow Decimal128/256
          std::string str = scalar->ToString();
          if (OB_FAIL(num.from(str.c_str(), static_cast<int64_t>(str.size()), allocator))) {
            LOG_WARN("failed to parse number from arrow decimal scalar", K(ret));
          } else {
            obj.set_number(num);
          }
        } else {
          // Fallback: string representation (e.g. from pa.array([str(d)]))
          std::shared_ptr<arrow::StringScalar> typed = std::static_pointer_cast<arrow::StringScalar>(scalar);
          std::shared_ptr<arrow::Buffer> buf = typed->value;
          if (OB_ISNULL(buf)) {
            ret = common::OB_ERR_UNEXPECTED;
            LOG_WARN("arrow scalar buffer is null for number string", K(ret));
          } else if (OB_FAIL(num.from(reinterpret_cast<const char*>(buf->data()),
                               static_cast<int64_t>(buf->size()), allocator))) {
            LOG_WARN("failed to parse number from arrow string", K(ret));
          } else {
            obj.set_number(num);
          }
        }
        break;
      }
      case common::ObDateTimeType:
      case common::ObTimestampType: {
        if (OB_UNLIKELY(scalar->type->id() != arrow::Type::TIMESTAMP)) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected arrow type for datetime/timestamp conversion", K(ret),
                   K(scalar->type->id()));
        } else {
          std::shared_ptr<arrow::TimestampScalar> typed = std::static_pointer_cast<arrow::TimestampScalar>(scalar);
          obj.set_datetime(typed->value);
        }
        break;
      }
      case common::ObMySQLDateTimeType: {
        // Python returns datetime as string or Arrow timestamp; parse back to packed format
        std::shared_ptr<arrow::Buffer> buf;
        char str_buf[64] = {0};
        int32_t str_len = 0;
        if (scalar->type->id() == arrow::Type::TIMESTAMP) {
          // Python returned a datetime.datetime → Arrow TimestampScalar
          // Convert to string first, then parse to ObMySQLDateTime
          std::shared_ptr<arrow::TimestampScalar> typed = std::static_pointer_cast<arrow::TimestampScalar>(scalar);
          std::string py_str = typed->ToString();  // "YYYY-MM-DD HH:MM:SS.ffffff"
          str_len = std::min(static_cast<int32_t>(py_str.size()),
                             static_cast<int32_t>(sizeof(str_buf) - 1));
          MEMCPY(str_buf, py_str.c_str(), str_len);
        } else {
          // String/binary representation
          if (scalar->type->id() == arrow::Type::BINARY || scalar->type->id() == arrow::Type::LARGE_BINARY) {
            buf = std::static_pointer_cast<arrow::BinaryScalar>(scalar)->value;
          } else if (scalar->type->id() == arrow::Type::STRING || scalar->type->id() == arrow::Type::LARGE_STRING) {
            buf = std::static_pointer_cast<arrow::StringScalar>(scalar)->value;
          } else {
            ret = common::OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected arrow type for mysql datetime string", K(ret), K(scalar->type->id()));
          }
          if (OB_ISNULL(buf)) {
            ret = common::OB_ERR_UNEXPECTED;
            LOG_WARN("arrow scalar buffer is null for mysql datetime", K(ret));
          } else {
            str_len = std::min(static_cast<int32_t>(buf->size()),
                               static_cast<int32_t>(sizeof(str_buf) - 1));
            MEMCPY(str_buf, buf->data(), str_len);
          }
        }
        common::ObMySQLDateTime mdt;
        common::ObString str(str_len, str_buf);
        common::ObTimeConvertCtx cvrt_ctx(nullptr, false);
        if (OB_FAIL(ret)) {
          // skip
        } else if (OB_FAIL(common::ObTimeConverter::str_to_mdatetime(str, cvrt_ctx, mdt))) {
          LOG_WARN("failed to parse datetime string from python", K(ret), K(str));
        } else {
          obj.set_datetime(mdt.datetime_);
          obj.set_meta_type(result_meta);
        }
        break;
      }
      case common::ObDateType: {
        char str_buf[16] = {0};
        int32_t str_len = 0;
        if (scalar->type->id() == arrow::Type::DATE32) {
          std::shared_ptr<arrow::Date32Scalar> typed = std::static_pointer_cast<arrow::Date32Scalar>(scalar);
          int64_t days = typed->value;
          time_t epoch_sec = days * 86400LL;
          struct tm tm_val;
          gmtime_r(&epoch_sec, &tm_val);
          str_len = snprintf(str_buf, sizeof(str_buf), "%04d-%02d-%02d",
                             tm_val.tm_year + 1900, tm_val.tm_mon + 1, tm_val.tm_mday);
        } else {
          std::shared_ptr<arrow::Buffer> buf;
          if (scalar->type->id() == arrow::Type::BINARY || scalar->type->id() == arrow::Type::LARGE_BINARY) {
            buf = std::static_pointer_cast<arrow::BinaryScalar>(scalar)->value;
          } else if (scalar->type->id() == arrow::Type::STRING || scalar->type->id() == arrow::Type::LARGE_STRING) {
            buf = std::static_pointer_cast<arrow::StringScalar>(scalar)->value;
          } else {
            ret = common::OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected arrow type for date string", K(ret), K(scalar->type->id()));
          }
          if (OB_ISNULL(buf)) {
            ret = common::OB_ERR_UNEXPECTED;
            LOG_WARN("arrow scalar buffer is null for date", K(ret));
          } else {
            str_len = std::min(static_cast<int32_t>(buf->size()),
                               static_cast<int32_t>(sizeof(str_buf) - 1));
            MEMCPY(str_buf, buf->data(), str_len);
          }
        }
        int32_t d_value = 0;
        common::ObString str(str_len, str_buf);
        if (OB_FAIL(ret)) {
          // skip
        } else if (OB_FAIL(common::ObTimeConverter::str_to_date(str, d_value))) {
          LOG_WARN("failed to parse date string from python", K(ret), K(str));
        } else {
          obj.set_date(d_value);
        }
        break;
      }
      case common::ObMySQLDateType: {
        char str_buf[16] = {0};
        int32_t str_len = 0;
        if (scalar->type->id() == arrow::Type::DATE32) {
          std::shared_ptr<arrow::Date32Scalar> typed = std::static_pointer_cast<arrow::Date32Scalar>(scalar);
          int64_t days = typed->value;
          time_t epoch_sec = days * 86400LL;
          struct tm tm_val;
          gmtime_r(&epoch_sec, &tm_val);
          str_len = snprintf(str_buf, sizeof(str_buf), "%04d-%02d-%02d",
                             tm_val.tm_year + 1900, tm_val.tm_mon + 1, tm_val.tm_mday);
        } else {
          std::shared_ptr<arrow::Buffer> buf;
          if (scalar->type->id() == arrow::Type::BINARY || scalar->type->id() == arrow::Type::LARGE_BINARY) {
            buf = std::static_pointer_cast<arrow::BinaryScalar>(scalar)->value;
          } else if (scalar->type->id() == arrow::Type::STRING || scalar->type->id() == arrow::Type::LARGE_STRING) {
            buf = std::static_pointer_cast<arrow::StringScalar>(scalar)->value;
          } else {
            ret = common::OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected arrow type for mysql date string", K(ret), K(scalar->type->id()));
          }
          if (OB_ISNULL(buf)) {
            ret = common::OB_ERR_UNEXPECTED;
            LOG_WARN("arrow scalar buffer is null for mysql date", K(ret));
          } else {
            str_len = std::min(static_cast<int32_t>(buf->size()),
                               static_cast<int32_t>(sizeof(str_buf) - 1));
            MEMCPY(str_buf, buf->data(), str_len);
          }
        }
        common::ObMySQLDate mdate;
        common::ObString str(str_len, str_buf);
        if (OB_FAIL(ret)) {
          // skip
        } else if (OB_FAIL(common::ObTimeConverter::str_to_mdate(str, mdate))) {
          LOG_WARN("failed to parse mysql date string from python", K(ret), K(str));
        } else {
          obj.set_mysql_date(mdate);
        }
        break;
      }
      default: {
        ret = common::OB_NOT_SUPPORTED;
        LOG_WARN("unsupported result type from arrow", K(ret), K(result_meta.get_type()));
        break;
      }
    } // switch
  } // else (scalar is valid)

  return ret;
}

int ob_udf_result_from_arrow(
    const uint8_t *ipc_bytes,
    int64_t byte_len,
    const common::ObObjMeta &result_meta,
    sql::ObArrowMemPool &pool,
    common::ObIAllocator &allocator,
    common::ObIArray<common::ObObj> &results)
{
  int ret = common::OB_SUCCESS;
  try {
    // Wrap raw bytes in a non-owning arrow::Buffer, then create BufferReader
    std::shared_ptr<arrow::Buffer> arrow_buf = std::make_shared<arrow::Buffer>(ipc_bytes, byte_len);
    std::shared_ptr<arrow::io::BufferReader> buf_reader = std::make_shared<arrow::io::BufferReader>(arrow_buf);
    arrow::ipc::IpcReadOptions read_opts = arrow::ipc::IpcReadOptions::Defaults();
    read_opts.memory_pool = &pool;
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>> reader_result =
        arrow::ipc::RecordBatchStreamReader::Open(buf_reader, read_opts);
    std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader;
    if (!reader_result.ok()) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("failed to open arrow ipc reader", K(ret));
    } else {
      reader = std::move(*reader_result);
    }

    std::shared_ptr<arrow::RecordBatch> batch;
    if (OB_SUCC(ret)) {
      arrow::Status st = reader->ReadNext(&batch);
      if (!st.ok() || batch == nullptr) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("failed to read arrow record batch", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (batch->num_columns() < 1) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("arrow result batch has no columns", K(ret));
      }
    }

    std::shared_ptr<arrow::Array> col;
    if (OB_SUCC(ret)) {
      col = batch->column(0);
      if (OB_ISNULL(col)) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("arrow result column is null", K(ret));
      } else {
        LOG_TRACE("ob_udf_result_from_arrow", K(result_meta.get_type()),
                 "arrow_col_type", col->type()->ToString().c_str());
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < col->length(); ++i) {
      arrow::Result<std::shared_ptr<arrow::Scalar>> scalar_result = col->GetScalar(i);
      if (!scalar_result.ok()) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get scalar from arrow column", K(ret), K(i));
      } else {
        common::ObObj obj;
        if (OB_FAIL(arrow_scalar_to_obj(*scalar_result, result_meta, allocator, obj))) {
          LOG_WARN("failed to convert arrow scalar to ObObj", K(ret), K(i));
        } else if (OB_FAIL(results.push_back(obj))) {
          LOG_WARN("failed to push back result", K(ret), K(i));
        }
      }
    }
  } catch (const sql::ObErrorCodeException &ob_error) {
    if (OB_SUCC(ret)) {
      ret = ob_error.get_error_code();
      LOG_WARN("arrow operation failed with OB error", K(ret));
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected exception in arrow deserialization", K(ret));
    }
  }

  return ret;
}

} // namespace pl
} // namespace oceanbase
