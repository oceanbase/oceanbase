/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_data_source.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr.h"
#include "share/vector/ob_vector_base.h"
#include "share/vector/ob_fixed_length_base.h"
#include "share/vector/ob_vector_define.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/ob_bit_vector.h"
#include "share/vector/expr_cmp_func.h"  // for NullSafeRowCmpFunc, VectorCmpExprFuncsHelper
#include "common/object/ob_obj_type.h"
#include "lib/number/ob_number_v2.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/geo/ob_wkt_parser.h"
#include "lib/geo/ob_geo.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include <numeric>    // std::iota
#include <algorithm>  // std::stable_sort
#include <vector>
#include <sstream>    // std::istringstream

namespace oceanbase
{
namespace sql
{

int MockDataSourceOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();

  // Check if LIMIT reached
  if (has_limit_ && output_cnt_ >= limit_) {
    brs_.size_ = 0;
    brs_.end_ = true;
    return OB_SUCCESS;
  }

  // Calculate remaining rows based on mode
  int64_t total_remain = 0;
  if (data_source_mode_ == MockDataSourceMode::ARRAY) {
    total_remain = static_cast<int64_t>(rows_.size()) - cur_idx_;
  } else if (data_source_mode_ == MockDataSourceMode::GENERATOR) {
    total_remain = generator_total_rows_ - cur_idx_;
  } else if (data_source_mode_ == MockDataSourceMode::STREAM_CSV) {
    // For STREAM_CSV, we don't know total upfront, use a large value
    // The end will be determined by EOF
    total_remain = max_row_cnt;
  }

  int64_t remain = total_remain;

  // Apply LIMIT: only output up to limit_ rows
  if (has_limit_ && limit_ > 0) {
    const int64_t limit_remain = limit_ - output_cnt_;
    remain = std::min(remain, limit_remain);
  }

  const int64_t cnt = std::min(max_row_cnt, remain);

  if (cnt == 0) {
    brs_.size_ = 0;
    brs_.end_ = true;
    return OB_SUCCESS;
  }

  // Get output expressions from spec
  const ExprFixedArray &output_exprs = spec_.output_;
  const int64_t col_count = output_exprs.count();

  // Materialize each logical row once per batch. Column-major filling used to call
  // row_generator_(idx) once per (column, row), which invoked ColumnGenerators (e.g.
  // random_int) col_count times per row and desynchronized RNG state from verify_ordered
  // and single-row semantics. See: MergeJoinOpTest.LargeDataInnerJoin first mismatch
  // at batch boundary (default batch_size 256).
  std::vector<TestRow> batch_rows;
  batch_rows.reserve(cnt);
  for (int64_t row = 0; row < cnt && OB_SUCC(ret); ++row) {
    if (data_source_mode_ == MockDataSourceMode::ARRAY) {
      if (cur_idx_ + row >= static_cast<int64_t>(rows_.size())) {
        break;
      }
      batch_rows.push_back(rows_[cur_idx_ + row]);
    } else if (data_source_mode_ == MockDataSourceMode::GENERATOR) {
      if (cur_idx_ + row >= generator_total_rows_) {
        break;
      }
      TestRow test_row = row_generator_(cur_idx_ + row);
      if (test_row.empty()) {
        break;
      }
      batch_rows.push_back(std::move(test_row));
    } else if (data_source_mode_ == MockDataSourceMode::STREAM_CSV) {
      if (csv_file_ == nullptr || !csv_file_->is_open() || csv_file_->eof()) {
        break;
      }
      std::string line;
      if (!std::getline(*csv_file_, line)) {
        break;
      }
      csv_current_line_++;
      batch_rows.push_back(parse_csv_line(line));
    }
  }

  const int64_t filled_rows = static_cast<int64_t>(batch_rows.size());
  if (filled_rows == 0) {
    brs_.size_ = 0;
    brs_.end_ = (data_source_mode_ == MockDataSourceMode::STREAM_CSV);
    return ret;
  }

  if (filled_rows < cnt) {
    brs_.size_ = filled_rows;
    brs_.end_ = (data_source_mode_ == MockDataSourceMode::STREAM_CSV) ? true : false;
  }

  const int64_t fill_cnt = filled_rows;

  // Initialize vectors for each column and fill data from batch_rows
  for (int64_t col = 0; col < col_count && OB_SUCC(ret); ++col) {
    ObExpr *expr = output_exprs.at(col);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(col), K(ret));
      break;
    }

    // Determine vector format based on use_rich_format_ setting and data type
    VectorFormat fmt;
    if (!spec_.use_rich_format_) {
      fmt = VEC_UNIFORM;
    } else if (expr->is_fixed_length_data_) {
      fmt = VEC_FIXED;
    } else if (vector_format_ == VEC_CONTINUOUS) {
      fmt = VEC_CONTINUOUS;
    } else {
      fmt = VEC_DISCRETE;
    }
    expr->init_vector(eval_ctx_, fmt, fill_cnt, true);

    // For VEC_CONTINUOUS, initialize offsets[0] = 0 before filling data
    if (fmt == VEC_CONTINUOUS) {
      uint32_t *offsets = expr->get_continuous_vector_offsets(eval_ctx_);
      offsets[0] = 0;
    }

    for (int64_t row = 0; row < fill_cnt && OB_SUCC(ret); ++row) {
      const TestRow &test_row = batch_rows[static_cast<size_t>(row)];
      if (col >= static_cast<int64_t>(test_row.size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column index out of range", K(col), K(test_row.size()), K(ret));
        break;
      }
      const TestValue &val = test_row[col];
      ret = fill_value_directly(expr, row, val, fmt);
    }

    // Mark the expression as evaluated and projected
    if (OB_SUCC(ret)) {
      sql::ObBitVector &nulls = expr->get_nulls(eval_ctx_);
      ObDatum *datums = expr->locate_batch_datums(eval_ctx_);
      int64_t row_count = (brs_.size_ > 0) ? brs_.size_ : fill_cnt;
      for (int64_t row = 0; row < row_count; ++row) {
        // For VEC_UNIFORM, null is stored in datum->null_ (set by vec->set_null() in
        // fill_value_directly). The separate null bitmap (get_nulls) is NOT updated for
        // VEC_UNIFORM, so checking nulls.at(row) would miss nulls and set_none() would
        // clear the null flag. Use datum->is_null() directly for VEC_UNIFORM.
        bool is_null = (fmt == VEC_UNIFORM || fmt == VEC_UNIFORM_CONST)
                       ? datums[row].is_null()
                       : nulls.at(row);
        if (is_null) {
          datums[row].set_null();
        } else {
          datums[row].set_none();
        }
      }

      expr->set_evaluated_projected(eval_ctx_);
      if (expr->is_batch_result()) {
        sql::ObBitVector &eval_flags = expr->get_evaluated_flags(eval_ctx_);
        for (int64_t row = 0; row < row_count; ++row) {
          eval_flags.set(row);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t row_count = (brs_.size_ > 0) ? brs_.size_ : fill_cnt;
    if (data_source_mode_ == MockDataSourceMode::ARRAY) {
      brs_.end_ = (cur_idx_ + row_count >= static_cast<int64_t>(rows_.size()));
    } else if (data_source_mode_ == MockDataSourceMode::GENERATOR) {
      brs_.end_ = (cur_idx_ + row_count >= generator_total_rows_);
    }
    // For STREAM_CSV, brs_.end_ is already set above

    brs_.size_ = row_count;
    cur_idx_ += row_count;
    output_cnt_ += row_count;
  }

  // In 1.0 mode (no rich format), cast each output expr vector to VEC_UNIFORM
  // so downstream 1.0 operators can access data via datum ptr/len/null correctly.
  if (OB_SUCC(ret) && !spec_.use_rich_format_) {
    for (int64_t col = 0; col < col_count && OB_SUCC(ret); ++col) {
      ObExpr *expr = output_exprs.at(col);
      if (OB_NOT_NULL(expr)) {
        if (OB_FAIL(expr->cast_to_uniform(brs_.size_, eval_ctx_))) {
          LOG_WARN("cast to uniform failed", K(ret), K(col));
        }
      }
    }
  }

  return ret;
}

// Helper function to parse CSV line
TestRow MockDataSourceOp::parse_csv_line(const std::string &line)
{
  TestRow row;
  std::string cell;
  bool in_quotes = false;

  for (size_t i = 0; i < line.size(); ++i) {
    char c = line[i];
    if (c == '"') {
      in_quotes = !in_quotes;
    } else if (c == ',' && !in_quotes) {
      row.push_back(TestValue(cell));
      cell.clear();
    } else {
      cell += c;
    }
  }
  // Don't forget the last cell
  row.push_back(TestValue(cell));

  return row;
}

// ===== fill_value_directly helper functions =====

namespace {

/**
 * Validate JSON text and convert to JSON binary format.
 * Uses ObJsonParser to parse text, then ObJsonBaseFactory to convert to ObJsonBin.
 * The binary output is stored in the test vector with ObLobCommon header.
 */
static int validate_and_convert_json(common::ObIAllocator &allocator,
                                     const common::ObString &json_text,
                                     common::ObString &json_bin)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *j_base = nullptr;
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, json_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_base))) {
    LOG_WARN("failed to parse JSON text to binary", K(ret), K(json_text));
  } else {
    // Use get_raw_binary() to get the serialized JSON binary
    ObJsonBin *j_bin = static_cast<ObJsonBin *>(j_base);
    common::ObString raw_bin;
    if (OB_FAIL(j_bin->get_raw_binary(raw_bin, &allocator))) {
      LOG_WARN("failed to get JSON raw binary", K(ret));
    } else if (raw_bin.length() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("JSON binary output is empty", K(ret));
    } else {
      // Deep copy since raw_bin points to internal memory
      char *buf = static_cast<char *>(allocator.alloc(raw_bin.length()));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc json bin buf failed", K(ret), K(raw_bin.length()));
      } else {
        MEMCPY(buf, raw_bin.ptr(), raw_bin.length());
        json_bin.assign_ptr(buf, static_cast<ObString::obstr_size_t>(raw_bin.length()));
      }
    }
  }
  return ret;
}

/**
 * Validate Geometry WKT text and convert to WKB format.
 * Uses ObWktParser to parse WKT and produce WKB binary.
 * The WKB output is stored in the test vector with ObLobCommon header.
 */
static int validate_and_convert_geo(common::ObIAllocator &allocator,
                                    const common::ObString &wkt_text,
                                    common::ObString &geo_bin)
{
  int ret = OB_SUCCESS;
  ObGeometry *geo = nullptr;
  if (OB_FAIL(ObWktParser::parse_wkt(allocator, wkt_text, geo, true /* to_wkb */,
                                      false /* is_geographical */))) {
    LOG_WARN("failed to parse WKT text", K(ret), K(wkt_text));
  } else if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("geo is null after parse", K(ret));
  } else {
    ObString wkb = geo->to_wkb();
    if (wkb.length() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("WKB output is empty", K(ret));
    } else {
      char *buf = static_cast<char *>(allocator.alloc(wkb.length()));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc geo wkb buf failed", K(ret), K(wkb.length()));
      } else {
        MEMCPY(buf, wkb.ptr(), wkb.length());
        geo_bin.assign_ptr(buf, static_cast<ObString::obstr_size_t>(wkb.length()));
      }
    }
  }
  return ret;
}

/**
 * Validate RoaringBitmap comma-separated integer list and convert to serialized binary format.
 * Parses "1,5,10" style text, creates ObRoaringBitmap, serializes to binary.
 * The binary output is stored in the test vector with ObLobCommon header.
 */
static int validate_and_convert_roaringbitmap(common::ObIAllocator &allocator,
                                              const common::ObString &rb_text,
                                              common::ObString &rb_bin)
{
  int ret = OB_SUCCESS;
  ObRoaringBitmap rb(&allocator);
  // Parse comma-separated integers from the input text
  std::string input(rb_text.ptr(), rb_text.length());
  std::istringstream iss(input);
  std::string token;
  bool has_value = false;
  while (std::getline(iss, token, ',')) {
    // Trim whitespace
    size_t start = token.find_first_not_of(" \t\r\n");
    size_t end = token.find_last_not_of(" \t\r\n");
    if (start == std::string::npos) continue;
    std::string trimmed = token.substr(start, end - start + 1);
    if (trimmed.empty()) continue;
    char *endptr = nullptr;
    errno = 0;
    uint64_t val = strtoull(trimmed.c_str(), &endptr, 10);
    if (endptr == trimmed.c_str() || errno != 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid roaring bitmap value", K(ret), K(trimmed.c_str()));
      break;
    }
    if (OB_FAIL(rb.value_add(val))) {
      LOG_WARN("failed to add value to roaring bitmap", K(ret), K(val));
      break;
    }
    has_value = true;
  }
  if (OB_FAIL(ret)) {
    // already logged
  } else if (!has_value) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("roaring bitmap text contains no valid values", K(ret), K(rb_text));
  } else {
    ObStringBuffer res_buf(&allocator);
    if (OB_FAIL(rb.serialize(res_buf))) {
      LOG_WARN("failed to serialize roaring bitmap", K(ret));
    } else {
      rb_bin.assign_ptr(res_buf.ptr(), static_cast<ObString::obstr_size_t>(res_buf.length()));
    }
  }
  return ret;
}

/**
 * Check that variable-length data does not exceed the per-row reserved buffer size.
 * OB allocates OBJ_DATUM_MAX_RES_SIZE (128) bytes per row for variable-length data
 * in VEC_DISCRETE (ptrs[]) and VEC_UNIFORM (datum.ptr_) formats. Writing more than
 * this overwrites adjacent rows' buffers and causes memory corruption.
 * LOB types include the ObLobCommon header in the total, so the limit applies to
 * sizeof(ObLobCommon) + payload_len.
 * For VEC_CONTINUOUS the buffer is contiguous (no per-row limit), so this check
 * is not needed there.
 */
static int check_var_len_limit(const ObExpr *expr, int64_t data_len)
{
  int ret = OB_SUCCESS;
  const uint32_t res_buf_len = expr->res_buf_len_;
  if (OB_UNLIKELY(data_len > res_buf_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("variable-length data exceeds per-row buffer limit (128 bytes), "
             "reduce string/LOB payload size", K(ret), K(data_len), K(res_buf_len),
             K(expr->datum_meta_.get_type()));
  }
  return ret;
}

/**
 * SortKeyDatum stores the raw binary data of a sort key evaluation result.
 * Used by sort_data_by_exprs() for type-safe comparison via NullSafeRowCmpFunc,
 * avoiding the limited and semantically inaccurate datum_to_test_value() conversion.
 */
struct SortKeyDatum {
  bool is_null_;
  const void *ptr_;    // points to deep-copied datum payload
  int32_t len_;

  SortKeyDatum() : is_null_(true), ptr_(nullptr), len_(0) {}
  SortKeyDatum(bool is_null, const void *ptr, int32_t len)
    : is_null_(is_null), ptr_(ptr), len_(len) {}
};

}  // anonymous namespace

int MockDataSourceOp::fill_value_directly(ObExpr *expr, int64_t row_idx,
                                            const TestValue &value, VectorFormat fmt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", KP(expr), K(ret));
    return ret;
  }

  // Get vector object to properly set null with has_null_ flag
  common::ObIVector *vec = expr->get_vector(eval_ctx_);

  // Handle NULL value
  if (value.is_null()) {
    // Use vector's set_null() to set both bitmap AND has_null_ flag
    vec->set_null(row_idx);
    // For VEC_CONTINUOUS, must update offsets even for NULL values
    // so that next row's offset is correct
    if (fmt == VEC_CONTINUOUS) {
      uint32_t *offsets = expr->get_continuous_vector_offsets(eval_ctx_);
      offsets[row_idx + 1] = offsets[row_idx];  // NULL has zero length
    }
    return OB_SUCCESS;
  } else {
    // Use vector's unset_null() to clear null flag
    vec->unset_null(row_idx);
  }

  const ObObjType obj_type = expr->datum_meta_.get_type();

  // For VEC_FIXED format, directly write to res_buf
  if (fmt == VEC_FIXED) {
    char *data = expr->get_res_buf(eval_ctx_);
    const VecValueTypeClass vec_tc = get_vec_value_tc(obj_type,
        expr->datum_meta_.scale_, expr->datum_meta_.precision_);

    switch (vec_tc) {
    // RTCType<VEC_TC_INTEGER> = int64_t
    // Covers: ObTinyIntType, ObSmallIntType, ObMediumIntType, ObInt32Type, ObIntType
    case VEC_TC_INTEGER: {
      if (!value.is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value type mismatch, expected int", K(obj_type), K(ret));
      } else {
        int64_t *int_data = reinterpret_cast<int64_t *>(data);
        int_data[row_idx] = value.get_int();
      }
      break;
    }
    // RTCType<VEC_TC_UINTEGER> = uint64_t
    // Covers: ObUTinyIntType, ObUSmallIntType, ObUMediumIntType, ObUInt32Type, ObUInt64Type
    case VEC_TC_UINTEGER: {
      if (!value.is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value type mismatch, expected uint", K(obj_type), K(ret));
      } else {
        uint64_t *uint_data = reinterpret_cast<uint64_t *>(data);
        uint_data[row_idx] = static_cast<uint64_t>(value.get_int());
      }
      break;
    }
    // RTCType<VEC_TC_FLOAT> = float
    // Covers: ObFloatType, ObUFloatType
    case VEC_TC_FLOAT: {
      float fval = value.is_double() ? static_cast<float>(value.get_double()) :
                   (value.is_int() ? static_cast<float>(value.get_int()) : 0.0f);
      float *float_data = reinterpret_cast<float *>(data);
      float_data[row_idx] = fval;
      break;
    }
    // RTCType<VEC_TC_DOUBLE> = double, RTCType<VEC_TC_FIXED_DOUBLE> = double
    // Covers: ObDoubleType, ObUDoubleType
    case VEC_TC_DOUBLE:
    case VEC_TC_FIXED_DOUBLE: {
      double dval = value.is_double() ? value.get_double() :
                    (value.is_int() ? static_cast<double>(value.get_int()) : 0.0);
      double *double_data = reinterpret_cast<double *>(data);
      double_data[row_idx] = dval;
      break;
    }
    // RTCType<VEC_TC_DATETIME> = int64_t
    // Covers: ObDateTimeType, ObTimestampType
    case VEC_TC_DATETIME: {
      int64_t val = value.is_int() ? value.get_int() :
                    (value.is_double() ? static_cast<int64_t>(value.get_double()) : 0);
      int64_t *dt_data = reinterpret_cast<int64_t *>(data);
      dt_data[row_idx] = val;
      break;
    }
    // RTCType<VEC_TC_DATE> = int32_t
    // Covers: ObDateType
    case VEC_TC_DATE: {
      int32_t val = value.is_int() ? static_cast<int32_t>(value.get_int()) :
                    (value.is_double() ? static_cast<int32_t>(value.get_double()) : 0);
      int32_t *date_data = reinterpret_cast<int32_t *>(data);
      date_data[row_idx] = val;
      break;
    }
    // RTCType<VEC_TC_TIME> = int64_t
    case VEC_TC_TIME: {
      int64_t val = value.is_int() ? value.get_int() :
                    (value.is_double() ? static_cast<int64_t>(value.get_double()) : 0);
      int64_t *time_data = reinterpret_cast<int64_t *>(data);
      time_data[row_idx] = val;
      break;
    }
    // RTCType<VEC_TC_YEAR> = uint8_t
    case VEC_TC_YEAR: {
      uint8_t val = value.is_int() ? static_cast<uint8_t>(value.get_int()) : 0;
      uint8_t *year_data = reinterpret_cast<uint8_t *>(data);
      year_data[row_idx] = val;
      break;
    }
    // RTCType<VEC_TC_BIT> = uint64_t
    case VEC_TC_BIT:
    // RTCType<VEC_TC_ENUM_SET> = uint64_t
    case VEC_TC_ENUM_SET: {
      uint64_t val = value.is_int() ? static_cast<uint64_t>(value.get_int()) : 0;
      uint64_t *u64_data = reinterpret_cast<uint64_t *>(data);
      u64_data[row_idx] = val;
      break;
    }
    // RTCType<VEC_TC_TIMESTAMP_TZ> = ObOTimestampData (12 bytes, packed)
    case VEC_TC_TIMESTAMP_TZ: {
      ObOTimestampData *tz_data = reinterpret_cast<ObOTimestampData *>(
          data + row_idx * sizeof(ObOTimestampData));
      tz_data->time_us_ = value.is_int() ? value.get_int() : 0;
      tz_data->time_ctx_.desc_ = 0;
      break;
    }
    // RTCType<VEC_TC_TIMESTAMP_TINY> = ObOTimestampTinyData (10 bytes, packed)
    // Covers: ObTimestampLTZType, ObTimestampNanoType
    case VEC_TC_TIMESTAMP_TINY: {
      ObOTimestampTinyData *tiny_data = reinterpret_cast<ObOTimestampTinyData *>(
          data + row_idx * sizeof(ObOTimestampTinyData));
      tiny_data->time_us_ = value.is_int() ? value.get_int() : 0;
      tiny_data->desc_ = 0;
      break;
    }
    // RTCType<VEC_TC_INTERVAL_YM> = int64_t
    case VEC_TC_INTERVAL_YM: {
      int64_t val = value.is_int() ? value.get_int() : 0;
      int64_t *ym_data = reinterpret_cast<int64_t *>(data);
      ym_data[row_idx] = val;
      break;
    }
    // RTCType<VEC_TC_INTERVAL_DS> = ObIntervalDSValue (12 bytes, packed)
    case VEC_TC_INTERVAL_DS: {
      ObIntervalDSValue *ds_data = reinterpret_cast<ObIntervalDSValue *>(
          data + row_idx * sizeof(ObIntervalDSValue));
      ds_data->nsecond_ = value.is_int() ? value.get_int() : 0;
      ds_data->fractional_second_ = 0;
      break;
    }
    // RTCType<VEC_TC_MYSQL_DATETIME> = int64_t
    case VEC_TC_MYSQL_DATETIME: {
      int64_t val = value.is_int() ? value.get_int() : 0;
      int64_t *mdt_data = reinterpret_cast<int64_t *>(data);
      mdt_data[row_idx] = val;
      break;
    }
    // RTCType<VEC_TC_MYSQL_DATE> = int32_t
    case VEC_TC_MYSQL_DATE: {
      int32_t val = value.is_int() ? static_cast<int32_t>(value.get_int()) : 0;
      int32_t *mdate_data = reinterpret_cast<int32_t *>(data);
      mdate_data[row_idx] = val;
      break;
    }
    // DecimalInt types
    case VEC_TC_DEC_INT32:
    case VEC_TC_DEC_INT64:
    case VEC_TC_DEC_INT128:
    case VEC_TC_DEC_INT256:
    case VEC_TC_DEC_INT512: {
      // Convert value to string first
      std::string str_val;
      if (value.is_string()) {
        str_val = value.get_string();
      } else if (value.is_int()) {
        str_val = std::to_string(value.get_int());
      } else if (value.is_double()) {
        str_val = std::to_string(value.get_double());
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsupported value type for decimal int", K(ret));
        break;
      }
      // Get precision and calculate int bytes
      const int16_t precision = expr->datum_meta_.precision_;
      const int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
      // Parse string to ObDecimalInt
      ObDecimalInt *decint = nullptr;
      int16_t calc_scale = 0, calc_precision = 0;
      int32_t val_len = 0;
      common::ObArenaAllocator arena;
      if (OB_FAIL(wide::from_string(str_val.c_str(), str_val.length(), arena,
                                     calc_scale, calc_precision, val_len, decint))) {
        LOG_WARN("failed to parse decimal int from string", K(str_val.c_str()), K(ret));
        break;
      }
      // Copy to vector buffer with sign extension.
      char *dst = data + row_idx * int_bytes;
      ObDecimalIntBuilder builder;
      builder.from(decint, val_len);
      builder.extend(int_bytes);
      MEMCPY(dst, builder.get_decimal_int(), int_bytes);
      break;
    }
    // RTCType<VEC_TC_UNKNOWN> = int64_t
    case VEC_TC_UNKNOWN: {
      int64_t val = value.is_int() ? value.get_int() : 0;
      int64_t *int_data = reinterpret_cast<int64_t *>(data);
      int_data[row_idx] = val;
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported vec_tc for VEC_FIXED", K(vec_tc), K(obj_type), K(ret));
      break;
    }
  } else if (fmt == VEC_DISCRETE) {
    // For string types in discrete format, we need to handle ptrs and lens
    // VEC_DISCRETE uses separate ptrs and lens arrays
    char **ptrs = expr->get_discrete_vector_ptrs(eval_ctx_);
    int32_t *lens = expr->get_discrete_vector_lens(eval_ctx_);
    const VecValueTypeClass vec_tc = get_vec_value_tc(obj_type,
        expr->datum_meta_.scale_, expr->datum_meta_.precision_);

    switch (vec_tc) {
    // Normal string types -- no LobLocator header needed
    // Covers: VEC_TC_STRING (Varchar, Char, HexString, TinyText, NVarchar2, NChar),
    //         VEC_TC_RAW, VEC_TC_ROWID, VEC_TC_ENUM_SET_INNER
    case VEC_TC_STRING:
    case VEC_TC_RAW:
    case VEC_TC_ROWID:
    case VEC_TC_ENUM_SET_INNER: {
      const std::string &str = value.is_string() ? value.get_string() : value.to_string();
      if (OB_FAIL(check_var_len_limit(expr, static_cast<int64_t>(str.size())))) { break; }
      char *str_buf = ptrs[row_idx];
      MEMCPY(str_buf, str.data(), str.size());
      lens[row_idx] = static_cast<int32_t>(str.size());
      break;
    }
    // LOB storage types -- need ObLobCommon header
    // Covers: VEC_TC_LOB (Text, MediumText, LongText, Lob),
    //         VEC_TC_UDT, VEC_TC_COLLECTION
    case VEC_TC_LOB:
    case VEC_TC_UDT:
    case VEC_TC_COLLECTION: {
      const std::string &str = value.is_string() ? value.get_string() : value.to_string();
      int64_t total_len = sizeof(ObLobCommon) + str.size();
      if (OB_FAIL(check_var_len_limit(expr, total_len))) { break; }
      ObLobCommon *lob_data = new (ptrs[row_idx]) ObLobCommon();
      MEMCPY(lob_data->buffer_, str.data(), str.size());
      lens[row_idx] = static_cast<int32_t>(total_len);
      break;
    }
    case VEC_TC_JSON: {
      const std::string &str = value.is_string() ? value.get_string() : value.to_string();
      common::ObString json_text(static_cast<int32_t>(str.size()), str.data());
      common::ObString json_bin;
      common::ObArenaAllocator arena;
      if (OB_FAIL(validate_and_convert_json(arena, json_text, json_bin))) {
        LOG_WARN("validate_and_convert_json failed", K(ret));
        break;
      }
      int64_t total_len = sizeof(ObLobCommon) + json_bin.length();
      if (OB_FAIL(check_var_len_limit(expr, total_len))) { break; }
      ObLobCommon *lob_data = new (ptrs[row_idx]) ObLobCommon();
      MEMCPY(lob_data->buffer_, json_bin.ptr(), json_bin.length());
      lens[row_idx] = static_cast<int32_t>(total_len);
      break;
    }
    case VEC_TC_GEO: {
      const std::string &str = value.is_string() ? value.get_string() : value.to_string();
      common::ObString wkt_text(static_cast<int32_t>(str.size()), str.data());
      common::ObString geo_bin;
      common::ObArenaAllocator arena;
      if (OB_FAIL(validate_and_convert_geo(arena, wkt_text, geo_bin))) {
        LOG_WARN("validate_and_convert_geo failed", K(ret));
        break;
      }
      int64_t total_len = sizeof(ObLobCommon) + geo_bin.length();
      if (OB_FAIL(check_var_len_limit(expr, total_len))) { break; }
      ObLobCommon *lob_data = new (ptrs[row_idx]) ObLobCommon();
      MEMCPY(lob_data->buffer_, geo_bin.ptr(), geo_bin.length());
      lens[row_idx] = static_cast<int32_t>(total_len);
      break;
    }
    case VEC_TC_ROARINGBITMAP: {
      const std::string &str = value.is_string() ? value.get_string() : value.to_string();
      common::ObString rb_text(static_cast<int32_t>(str.size()), str.data());
      common::ObString rb_bin;
      common::ObArenaAllocator arena;
      if (OB_FAIL(validate_and_convert_roaringbitmap(arena, rb_text, rb_bin))) {
        LOG_WARN("validate_and_convert_roaringbitmap failed", K(ret));
        break;
      }
      int64_t total_len = sizeof(ObLobCommon) + rb_bin.length();
      if (OB_FAIL(check_var_len_limit(expr, total_len))) { break; }
      ObLobCommon *lob_data = new (ptrs[row_idx]) ObLobCommon();
      MEMCPY(lob_data->buffer_, rb_bin.ptr(), rb_bin.length());
      lens[row_idx] = static_cast<int32_t>(total_len);
      break;
    }
    // VEC_TC_NUMBER
    case VEC_TC_NUMBER: {
      number::ObNumber num;
      common::ObArenaAllocator arena;
      if (value.is_int()) {
        ret = num.from(value.get_int(), arena);
      } else if (value.is_double()) {
        ret = num.from(std::to_string(value.get_double()).c_str(), arena);
      } else if (value.is_string()) {
        ret = num.from(value.get_string().c_str(), arena);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsupported value type for number", K(ret));
        break;
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to convert value to ObNumber", K(ret));
        break;
      }
      const int32_t num_len = sizeof(ObNumberDesc) + num.d_.len_ * sizeof(uint32_t);
      if (OB_FAIL(check_var_len_limit(expr, static_cast<int64_t>(num_len)))) { break; }
      char *num_buf = ptrs[row_idx];
      number::ObCompactNumber *cnum = reinterpret_cast<number::ObCompactNumber *>(num_buf);
      cnum->desc_ = num.d_;
      if (num.d_.len_ > 0) {
        const uint32_t *src_digits = num.get_digits();
        MEMCPY(cnum->digits_, src_digits, num.d_.len_ * sizeof(uint32_t));
      }
      lens[row_idx] = num_len;
      break;
    }
    default:
      // Fallback: try as string
      if (value.is_string()) {
        const std::string &str = value.get_string();
        if (OB_FAIL(check_var_len_limit(expr, static_cast<int64_t>(str.size())))) { break; }
        char *str_buf = ptrs[row_idx];
        MEMCPY(str_buf, str.data(), str.size());
        lens[row_idx] = static_cast<int32_t>(str.size());
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported vec_tc for VEC_DISCRETE", K(vec_tc), K(obj_type), K(fmt), K(ret));
      }
      break;
    }
  } else if (fmt == VEC_CONTINUOUS) {
    // Continuous format: offsets[] + contiguous data buffer
    // offsets[row_idx] points to the start of row_idx's data
    // offsets[row_idx + 1] - offsets[row_idx] is the length
    char *cont_data = expr->get_continuous_vector_data(eval_ctx_);
    uint32_t *offsets = expr->get_continuous_vector_offsets(eval_ctx_);
    const ObObjType obj_type = expr->datum_meta_.get_type();
    const VecValueTypeClass vec_tc = get_vec_value_tc(obj_type,
        expr->datum_meta_.scale_, expr->datum_meta_.precision_);

    int32_t src_len = 0;
    std::string str_val;  // temporary for string conversion

    switch (vec_tc) {
    // Number type: encode as ObCompactNumber binary format
    case VEC_TC_NUMBER: {
      number::ObNumber num;
      common::ObArenaAllocator arena;
      if (value.is_int()) {
        ret = num.from(value.get_int(), arena);
      } else if (value.is_double()) {
        ret = num.from(std::to_string(value.get_double()).c_str(), arena);
      } else if (value.is_string()) {
        ret = num.from(value.get_string().c_str(), arena);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsupported value type for number", K(ret));
        break;
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to convert value to ObNumber", K(ret));
        break;
      }
      src_len = sizeof(ObNumberDesc) + num.d_.len_ * sizeof(uint32_t);
      char *dst = cont_data + offsets[row_idx];
      number::ObCompactNumber *cnum = reinterpret_cast<number::ObCompactNumber *>(dst);
      cnum->desc_ = num.d_;
      if (num.d_.len_ > 0) {
        const uint32_t *src_digits = num.get_digits();
        MEMCPY(cnum->digits_, src_digits, num.d_.len_ * sizeof(uint32_t));
      }
      offsets[row_idx + 1] = offsets[row_idx] + src_len;
      break;
    }
    // String types: write string data directly
    case VEC_TC_STRING:
    case VEC_TC_RAW:
    case VEC_TC_ROWID:
    case VEC_TC_ENUM_SET_INNER: {
      if (value.is_string()) {
        str_val = value.get_string();
      } else {
        str_val = value.to_string();
      }
      src_len = static_cast<int32_t>(str_val.size());
      MEMCPY(cont_data + offsets[row_idx], str_val.data(), src_len);
      offsets[row_idx + 1] = offsets[row_idx] + src_len;
      break;
    }
    // LOB storage types: with ObLobCommon header
    case VEC_TC_LOB:
    case VEC_TC_UDT:
    case VEC_TC_COLLECTION: {
      if (value.is_string()) {
        str_val = value.get_string();
      } else {
        str_val = value.to_string();
      }
      int64_t total_len = sizeof(ObLobCommon) + str_val.size();
      char *dst = cont_data + offsets[row_idx];
      ObLobCommon *lob_data = new (dst) ObLobCommon();
      MEMCPY(lob_data->buffer_, str_val.data(), str_val.size());
      src_len = static_cast<int32_t>(total_len);
      offsets[row_idx + 1] = offsets[row_idx] + src_len;
      break;
    }
    case VEC_TC_JSON: {
      if (value.is_string()) {
        str_val = value.get_string();
      } else {
        str_val = value.to_string();
      }
      common::ObString json_text(static_cast<int32_t>(str_val.size()), str_val.data());
      common::ObString json_bin;
      common::ObArenaAllocator arena;
      if (OB_FAIL(validate_and_convert_json(arena, json_text, json_bin))) {
        LOG_WARN("validate_and_convert_json failed", K(ret));
        break;
      }
      int64_t total_len = sizeof(ObLobCommon) + json_bin.length();
      char *dst = cont_data + offsets[row_idx];
      ObLobCommon *lob_data = new (dst) ObLobCommon();
      MEMCPY(lob_data->buffer_, json_bin.ptr(), json_bin.length());
      src_len = static_cast<int32_t>(total_len);
      offsets[row_idx + 1] = offsets[row_idx] + src_len;
      break;
    }
    case VEC_TC_GEO: {
      if (value.is_string()) {
        str_val = value.get_string();
      } else {
        str_val = value.to_string();
      }
      common::ObString wkt_text(static_cast<int32_t>(str_val.size()), str_val.data());
      common::ObString geo_bin;
      common::ObArenaAllocator arena;
      if (OB_FAIL(validate_and_convert_geo(arena, wkt_text, geo_bin))) {
        LOG_WARN("validate_and_convert_geo failed", K(ret));
        break;
      }
      int64_t total_len = sizeof(ObLobCommon) + geo_bin.length();
      char *dst = cont_data + offsets[row_idx];
      ObLobCommon *lob_data = new (dst) ObLobCommon();
      MEMCPY(lob_data->buffer_, geo_bin.ptr(), geo_bin.length());
      src_len = static_cast<int32_t>(total_len);
      offsets[row_idx + 1] = offsets[row_idx] + src_len;
      break;
    }
    case VEC_TC_ROARINGBITMAP: {
      if (value.is_string()) {
        str_val = value.get_string();
      } else {
        str_val = value.to_string();
      }
      common::ObString rb_text(static_cast<int32_t>(str_val.size()), str_val.data());
      common::ObString rb_bin;
      common::ObArenaAllocator arena;
      if (OB_FAIL(validate_and_convert_roaringbitmap(arena, rb_text, rb_bin))) {
        LOG_WARN("validate_and_convert_roaringbitmap failed", K(ret));
        break;
      }
      int64_t total_len = sizeof(ObLobCommon) + rb_bin.length();
      char *dst = cont_data + offsets[row_idx];
      ObLobCommon *lob_data = new (dst) ObLobCommon();
      MEMCPY(lob_data->buffer_, rb_bin.ptr(), rb_bin.length());
      src_len = static_cast<int32_t>(total_len);
      offsets[row_idx + 1] = offsets[row_idx] + src_len;
      break;
    }
    // Other types: fallback to string representation
    default: {
      if (value.is_string()) {
        str_val = value.get_string();
      } else if (value.is_int()) {
        str_val = std::to_string(value.get_int());
      } else if (value.is_double()) {
        str_val = std::to_string(value.get_double());
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported value type for continuous vector", K(ret));
        break;
      }
      src_len = static_cast<int32_t>(str_val.size());
      MEMCPY(cont_data + offsets[row_idx], str_val.data(), src_len);
      offsets[row_idx + 1] = offsets[row_idx] + src_len;
      break;
    }
    }
  } else if (fmt == VEC_UNIFORM) {
    // For VEC_UNIFORM format, write to ObDatum array
    ObDatum *datums = expr->locate_batch_datums(eval_ctx_);
    const VecValueTypeClass vec_tc = get_vec_value_tc(obj_type,
        expr->datum_meta_.scale_, expr->datum_meta_.precision_);

    switch (vec_tc) {
    case VEC_TC_INTEGER: {
      datums[row_idx].set_int(value.is_int() ? value.get_int() :
                              (value.is_double() ? static_cast<int64_t>(value.get_double()) : 0));
      break;
    }
    case VEC_TC_UINTEGER: {
      datums[row_idx].set_uint(value.is_int() ? static_cast<uint64_t>(value.get_int()) : 0);
      break;
    }
    case VEC_TC_FLOAT: {
      float fval = value.is_double() ? static_cast<float>(value.get_double()) :
                   (value.is_int() ? static_cast<float>(value.get_int()) : 0.0f);
      datums[row_idx].set_float(fval);
      break;
    }
    case VEC_TC_DOUBLE:
    case VEC_TC_FIXED_DOUBLE: {
      double dval = value.is_double() ? value.get_double() :
                    (value.is_int() ? static_cast<double>(value.get_int()) : 0.0);
      datums[row_idx].set_double(dval);
      break;
    }
    case VEC_TC_DATETIME:
    case VEC_TC_TIME:
    case VEC_TC_INTERVAL_YM:
    case VEC_TC_MYSQL_DATETIME: {
      datums[row_idx].set_int(value.is_int() ? value.get_int() : 0);
      break;
    }
    case VEC_TC_DATE:
    case VEC_TC_MYSQL_DATE: {
      datums[row_idx].set_int(value.is_int() ? value.get_int() : 0);
      break;
    }
    case VEC_TC_YEAR: {
      datums[row_idx].set_int(value.is_int() ? value.get_int() : 0);
      break;
    }
    case VEC_TC_BIT:
    case VEC_TC_ENUM_SET: {
      datums[row_idx].set_uint(value.is_int() ? static_cast<uint64_t>(value.get_int()) : 0);
      break;
    }
    case VEC_TC_TIMESTAMP_TZ: {
      // ObOTimestampData is 12 bytes, packed; store raw via set_int for time_us_
      datums[row_idx].set_int(value.is_int() ? value.get_int() : 0);
      break;
    }
    case VEC_TC_TIMESTAMP_TINY: {
      datums[row_idx].set_int(value.is_int() ? value.get_int() : 0);
      break;
    }
    case VEC_TC_INTERVAL_DS: {
      // ObIntervalDSValue is 12 bytes, packed
      datums[row_idx].set_int(value.is_int() ? value.get_int() : 0);
      break;
    }
    // String types without LOB header
    case VEC_TC_STRING:
    case VEC_TC_RAW:
    case VEC_TC_ROWID:
    case VEC_TC_ENUM_SET_INNER: {
      const std::string &str = value.is_string() ? value.get_string() : value.to_string();
      if (OB_FAIL(check_var_len_limit(expr, static_cast<int64_t>(str.size())))) { break; }
      datums[row_idx].set_string(str.data(), static_cast<uint32_t>(str.size()));
      break;
    }
    // LOB storage types -- need ObLobCommon header
    case VEC_TC_LOB:
    case VEC_TC_UDT:
    case VEC_TC_COLLECTION: {
      const std::string &str = value.is_string() ? value.get_string() : value.to_string();
      int64_t total_len = sizeof(ObLobCommon) + str.size();
      if (OB_FAIL(check_var_len_limit(expr, total_len))) { break; }
      // Reset ptr to frame memory for this datum
      expr->reset_ptr_in_datum(eval_ctx_, row_idx);
      // Construct ObLobCommon header + data in the datum's buffer
      ObLobCommon *lob_data = new (const_cast<char *>(datums[row_idx].ptr_)) ObLobCommon();
      MEMCPY(lob_data->buffer_, str.data(), str.size());
      datums[row_idx].pack_ = static_cast<uint32_t>(total_len);
      break;
    }
    case VEC_TC_JSON: {
      const std::string &str = value.is_string() ? value.get_string() : value.to_string();
      common::ObString json_text(static_cast<int32_t>(str.size()), str.data());
      common::ObString json_bin;
      common::ObArenaAllocator arena;
      if (OB_FAIL(validate_and_convert_json(arena, json_text, json_bin))) {
        LOG_WARN("validate_and_convert_json failed", K(ret));
        break;
      }
      int64_t total_len = sizeof(ObLobCommon) + json_bin.length();
      if (OB_FAIL(check_var_len_limit(expr, total_len))) { break; }
      expr->reset_ptr_in_datum(eval_ctx_, row_idx);
      ObLobCommon *lob_data = new (const_cast<char *>(datums[row_idx].ptr_)) ObLobCommon();
      MEMCPY(lob_data->buffer_, json_bin.ptr(), json_bin.length());
      datums[row_idx].pack_ = static_cast<uint32_t>(total_len);
      break;
    }
    case VEC_TC_GEO: {
      const std::string &str = value.is_string() ? value.get_string() : value.to_string();
      common::ObString wkt_text(static_cast<int32_t>(str.size()), str.data());
      common::ObString geo_bin;
      common::ObArenaAllocator arena;
      if (OB_FAIL(validate_and_convert_geo(arena, wkt_text, geo_bin))) {
        LOG_WARN("validate_and_convert_geo failed", K(ret));
        break;
      }
      int64_t total_len = sizeof(ObLobCommon) + geo_bin.length();
      if (OB_FAIL(check_var_len_limit(expr, total_len))) { break; }
      expr->reset_ptr_in_datum(eval_ctx_, row_idx);
      ObLobCommon *lob_data = new (const_cast<char *>(datums[row_idx].ptr_)) ObLobCommon();
      MEMCPY(lob_data->buffer_, geo_bin.ptr(), geo_bin.length());
      datums[row_idx].pack_ = static_cast<uint32_t>(total_len);
      break;
    }
    case VEC_TC_ROARINGBITMAP: {
      const std::string &str = value.is_string() ? value.get_string() : value.to_string();
      common::ObString rb_text(static_cast<int32_t>(str.size()), str.data());
      common::ObString rb_bin;
      common::ObArenaAllocator arena;
      if (OB_FAIL(validate_and_convert_roaringbitmap(arena, rb_text, rb_bin))) {
        LOG_WARN("validate_and_convert_roaringbitmap failed", K(ret));
        break;
      }
      int64_t total_len = sizeof(ObLobCommon) + rb_bin.length();
      if (OB_FAIL(check_var_len_limit(expr, total_len))) { break; }
      expr->reset_ptr_in_datum(eval_ctx_, row_idx);
      ObLobCommon *lob_data = new (const_cast<char *>(datums[row_idx].ptr_)) ObLobCommon();
      MEMCPY(lob_data->buffer_, rb_bin.ptr(), rb_bin.length());
      datums[row_idx].pack_ = static_cast<uint32_t>(total_len);
      break;
    }
    case VEC_TC_NUMBER: {
      number::ObNumber num;
      common::ObArenaAllocator arena;
      if (value.is_int()) {
        ret = num.from(value.get_int(), arena);
      } else if (value.is_double()) {
        ret = num.from(std::to_string(value.get_double()).c_str(), arena);
      } else if (value.is_string()) {
        ret = num.from(value.get_string().c_str(), arena);
      } else {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      if (OB_FAIL(ret)) break;
      const int32_t num_len = sizeof(ObNumberDesc) + num.d_.len_ * sizeof(uint32_t);
      if (OB_FAIL(check_var_len_limit(expr, static_cast<int64_t>(num_len)))) { break; }
      expr->reset_ptr_in_datum(eval_ctx_, row_idx);
      MEMSET(const_cast<char *>(datums[row_idx].ptr_), 0, num_len);
      number::ObCompactNumber *cnum = reinterpret_cast<number::ObCompactNumber *>(
          const_cast<char *>(datums[row_idx].ptr_));
      cnum->desc_ = num.d_;
      if (num.d_.len_ > 0) {
        MEMCPY(cnum->digits_, num.get_digits(), num.d_.len_ * sizeof(uint32_t));
      }
      datums[row_idx].pack_ = static_cast<uint32_t>(num_len);
      break;
    }
    case VEC_TC_DEC_INT32:
    case VEC_TC_DEC_INT64:
    case VEC_TC_DEC_INT128:
    case VEC_TC_DEC_INT256:
    case VEC_TC_DEC_INT512: {
      std::string str_val;
      if (value.is_string()) str_val = value.get_string();
      else if (value.is_int()) str_val = std::to_string(value.get_int());
      else if (value.is_double()) str_val = std::to_string(value.get_double());
      else { ret = OB_ERR_UNEXPECTED; break; }
      const int16_t precision = expr->datum_meta_.precision_;
      const int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
      if (OB_FAIL(check_var_len_limit(expr, static_cast<int64_t>(int_bytes)))) { break; }
      ObDecimalInt *decint = nullptr;
      int16_t calc_scale = 0, calc_precision = 0;
      int32_t val_len = 0;
      common::ObArenaAllocator arena;
      if (OB_FAIL(wide::from_string(str_val.c_str(), str_val.length(), arena,
                                     calc_scale, calc_precision, val_len, decint))) break;
      // reset ptr_ to point to the expression's res_buf before writing
      expr->reset_ptr_in_datum(eval_ctx_, row_idx);
      // zero the full int_bytes slot first (val_len may be < int_bytes for small values)
      MEMSET(const_cast<char *>(datums[row_idx].ptr_), 0, int_bytes);
      MEMCPY(const_cast<char *>(datums[row_idx].ptr_), decint, val_len);
      datums[row_idx].pack_ = static_cast<uint32_t>(int_bytes);
      break;
    }
    case VEC_TC_UNKNOWN: {
      datums[row_idx].set_int(value.is_int() ? value.get_int() : 0);
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported vec_tc for VEC_UNIFORM", K(vec_tc), K(obj_type), K(fmt), K(ret));
      break;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported vector format", K(fmt), K(ret));
  }

  return ret;
}

// ===== sort_data_by_exprs helpers =====

namespace {

/**
 * Fill an ObDatum from a TestValue for row-mode sort key evaluation.
 * String pointer points into the TestValue's std::string, which must remain alive.
 */
static void fill_datum_from_test_value(ObDatum &datum, const TestValue &val)
{
  if (val.is_null()) {
    datum.set_null();
  } else if (val.is_int()) {
    datum.set_int(val.get_int());
  } else if (val.is_double()) {
    datum.set_double(val.get_double());
  } else if (val.is_string()) {
    const std::string &s = val.get_string();
    datum.set_string(ObString(static_cast<int32_t>(s.length()), s.c_str()));
  } else {
    datum.set_null();
  }
}

/**
 * Convert an evaluated ObDatum to TestValue for sort comparison.
 * @deprecated No longer used by sort_data_by_exprs(). Kept for reference only.
 *             Sort now uses SortKeyDatum + NullSafeRowCmpFunc for full type coverage.
 */
static TestValue datum_to_test_value(const ObDatum &datum, const ObObjType type)
{
  if (datum.is_null()) return TestValue::null();
  if (ob_is_int_tc(type)) {
    return TestValue(datum.get_int());
  } else if (ob_is_uint_tc(type)) {
    return TestValue(static_cast<int64_t>(datum.get_uint64()));
  } else if (type == ObFloatType) {
    return TestValue(static_cast<double>(datum.get_float()));
  } else if (type == ObDoubleType) {
    return TestValue(datum.get_double());
  } else if (ob_is_string_type(type)) {
    ObString str = datum.get_string();
    return TestValue(std::string(str.ptr(), str.length()));
  }
  return TestValue::null();
}

/**
 * Recursively clear evaluated flag for sort key expr and all sub-exprs.
 * Called before evaluating each row so stale cached values are not reused.
 */
static void clear_sort_key_eval_flag(ObExpr *expr, ObEvalCtx &eval_ctx)
{
  if (OB_ISNULL(expr)) return;
  expr->get_eval_info(eval_ctx).clear_evaluated_flag();
  for (uint32_t i = 0; i < expr->arg_cnt_; ++i) {
    clear_sort_key_eval_flag(expr->args_[i], eval_ctx);
  }
}

/**
 * Recursively reset vector header format to VEC_INVALID for expr and all sub-exprs.
 * This prevents cast_to_uniform() from overwriting datums with stale vector data
 * left over from a previous execution (e.g., 2.0 run in dual-format check mode).
 */
static void reset_vector_header_recursive(ObExpr *expr, ObEvalCtx &eval_ctx)
{
  if (OB_ISNULL(expr)) return;
  if (UINT32_MAX != expr->vector_header_off_) {
    expr->get_vector_header(eval_ctx).format_ = VEC_INVALID;
  }
  for (uint32_t i = 0; i < expr->arg_cnt_; ++i) {
    reset_vector_header_recursive(expr->args_[i], eval_ctx);
  }
}

}  // anonymous namespace

int MockDataSourceOp::sort_data_by_exprs()
{
  int ret = OB_SUCCESS;
  if (sort_key_specs_.empty() || rows_.empty()) return ret;

  const ExprFixedArray &col_exprs = get_spec().output_;
  const int64_t n = static_cast<int64_t>(rows_.size());
  const int64_t col_count = col_exprs.count();
  common::ObIAllocator &allocator = ctx_.get_allocator();

  // Reset vector header formats to VEC_INVALID for all column exprs and sort key sub-exprs.
  // This prevents cast_to_uniform() in ObExpr::eval() from overwriting freshly filled datums
  // with stale vector data from a previous execution (e.g., 2.0 run in dual-format check).
  for (int64_t j = 0; j < col_count; ++j) {
    ObExpr *col_expr = col_exprs.at(j);
    if (UINT32_MAX != col_expr->vector_header_off_) {
      col_expr->get_vector_header(eval_ctx_).format_ = VEC_INVALID;
    }
  }
  for (const SortKeySpec &spec : sort_key_specs_) {
    reset_vector_header_recursive(spec.key_expr, eval_ctx_);
  }

  // Pre-compute comparison functions for each sort key using NullSafeRowCmpFunc.
  // This provides type-correct comparison for all 40+ OB types, replacing the
  // limited datum_to_test_value() which only supported int/uint/float/double/string.
  std::vector<NullSafeRowCmpFunc> cmp_funcs;
  cmp_funcs.reserve(sort_key_specs_.size());
  for (const SortKeySpec &spec : sort_key_specs_) {
    NullSafeRowCmpFunc null_first_cmp = nullptr, null_last_cmp = nullptr;
    // Try basic_funcs_ first (set during CG, available at sort time)
    if (OB_NOT_NULL(spec.key_expr->basic_funcs_)) {
      null_first_cmp = spec.key_expr->basic_funcs_->row_null_first_cmp_;
      null_last_cmp = spec.key_expr->basic_funcs_->row_null_last_cmp_;
    }
    // Fallback: use VectorCmpExprFuncsHelper if basic_funcs_ not set
    if (OB_ISNULL(null_first_cmp) || OB_ISNULL(null_last_cmp)) {
      VectorCmpExprFuncsHelper::get_cmp_set(
          spec.key_expr->datum_meta_, spec.key_expr->datum_meta_,
          null_first_cmp, null_last_cmp);
    }
    NullSafeRowCmpFunc cmp_func = spec.nulls_first ? null_first_cmp : null_last_cmp;
    cmp_funcs.push_back(cmp_func);
  }

  // Collect sort key datum payloads for each row (deep-copied, type-agnostic binary)
  std::vector<std::vector<SortKeyDatum>> key_datums(n);

  for (int64_t row_i = 0; row_i < n && OB_SUCC(ret); ++row_i) {
    const TestRow &row = rows_[row_i];

    // 1. Clear all eval flags (columns + sort key sub-exprs) for this row
    for (int64_t j = 0; j < col_count; ++j) {
      col_exprs.at(j)->get_eval_info(eval_ctx_).clear_evaluated_flag();
    }
    for (const SortKeySpec &spec : sort_key_specs_) {
      clear_sort_key_eval_flag(spec.key_expr, eval_ctx_);
    }

    // 2. Fill column datums from TestRow and mark evaluated
    for (int64_t j = 0; j < col_count && OB_SUCC(ret); ++j) {
      if (j >= static_cast<int64_t>(row.size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row has fewer columns than expected", K(j), K(row.size()), K(ret));
        break;
      }
      ObExpr *col_expr = col_exprs.at(j);
      ObDatum &datum = col_expr->locate_datum_for_write(eval_ctx_);
      fill_datum_from_test_value(datum, row[j]);
      col_expr->set_evaluated_flag(eval_ctx_);
    }
    if (OB_FAIL(ret)) break;

    // 3. Evaluate each sort key expr and deep-copy the raw datum payload
    key_datums[row_i].reserve(sort_key_specs_.size());
    for (const SortKeySpec &spec : sort_key_specs_) {
      ObDatum *result = nullptr;
      if (OB_FAIL(spec.key_expr->eval(eval_ctx_, result))) {
        LOG_WARN("sort key eval failed", K(ret));
        break;
      }
      if (OB_ISNULL(result) || result->is_null()) {
        key_datums[row_i].emplace_back(true, nullptr, 0);
      } else {
        // Deep copy datum payload so it survives across row evaluations
        char *buf = static_cast<char *>(allocator.alloc(result->len_));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc sort key datum buf failed", K(ret));
          break;
        }
        MEMCPY(buf, result->ptr_, result->len_);
        key_datums[row_i].emplace_back(false, buf, static_cast<int32_t>(result->len_));
      }
    }
  }

  if (OB_FAIL(ret)) return ret;

  // 4. Stable-sort rows by collected key datums using NullSafeRowCmpFunc
  std::vector<int64_t> idx(n);
  std::iota(idx.begin(), idx.end(), 0);
  std::stable_sort(idx.begin(), idx.end(),
    [&](int64_t a, int64_t b) -> bool {
      for (size_t k = 0; k < sort_key_specs_.size(); ++k) {
        const SortKeyDatum &da = key_datums[a][k];
        const SortKeyDatum &db = key_datums[b][k];

        if (cmp_funcs[k] != nullptr) {
          // Use NullSafeRowCmpFunc for type-correct comparison
          int cmp_ret = 0;
          const ObObjMeta &meta = sort_key_specs_[k].key_expr->obj_meta_;
          cmp_funcs[k](meta, meta,
                       da.ptr_, da.len_, da.is_null_,
                       db.ptr_, db.len_, db.is_null_,
                       cmp_ret);
          if (cmp_ret != 0) {
            return sort_key_specs_[k].ascending ? (cmp_ret < 0) : (cmp_ret > 0);
          }
        } else {
          // Fallback: manual comparison for types without cmp func
          if (da.is_null_ && db.is_null_) continue;
          const bool asc = sort_key_specs_[k].ascending;
          const bool nf  = sort_key_specs_[k].nulls_first;
          if (da.is_null_) return nf ? asc : !asc;
          if (db.is_null_) return nf ? !asc : asc;
          // Compare by raw bytes (not ideal but safe fallback)
          int32_t min_len = std::min(da.len_, db.len_);
          int cmp = MEMCMP(da.ptr_, db.ptr_, min_len);
          if (cmp == 0) cmp = da.len_ - db.len_;
          if (cmp != 0) return asc ? (cmp < 0) : (cmp > 0);
        }
      }
      return false;  // equal
    });

  std::vector<TestRow> sorted;
  sorted.reserve(n);
  for (int64_t i : idx) sorted.push_back(std::move(rows_[i]));
  rows_ = std::move(sorted);

  return ret;
}

}  // namespace sql
}  // namespace oceanbase