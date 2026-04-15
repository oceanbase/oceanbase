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
#include "share/datum/ob_datum.h"
#include "sql/engine/ob_bit_vector.h"
#include "common/object/ob_obj_type.h"
#include "lib/number/ob_number_v2.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"
#include <numeric>    // std::iota
#include <algorithm>  // std::stable_sort

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

  // Initialize vectors for each column and fill data
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
    expr->init_vector(eval_ctx_, fmt, cnt, true);

    // For VEC_CONTINUOUS, initialize offsets[0] = 0 before filling data
    if (fmt == VEC_CONTINUOUS) {
      uint32_t *offsets = expr->get_continuous_vector_offsets(eval_ctx_);
      offsets[0] = 0;
    }

    // Fill values for this column
    int64_t actual_rows = 0;
    for (int64_t row = 0; row < cnt && OB_SUCC(ret); ++row) {
      TestRow test_row;

      // Get row based on mode
      if (data_source_mode_ == MockDataSourceMode::ARRAY) {
        if (cur_idx_ + row >= static_cast<int64_t>(rows_.size())) {
          break;
        }
        test_row = rows_[cur_idx_ + row];
        actual_rows++;
      } else if (data_source_mode_ == MockDataSourceMode::GENERATOR) {
        if (cur_idx_ + row >= generator_total_rows_) {
          break;
        }
        test_row = row_generator_(cur_idx_ + row);
        if (test_row.empty()) {
          // Generator signaled end
          break;
        }
        actual_rows++;
      } else if (data_source_mode_ == MockDataSourceMode::STREAM_CSV) {
        // Read next line from CSV
        if (csv_file_ == nullptr || !csv_file_->is_open() || csv_file_->eof()) {
          break;
        }
        std::string line;
        if (!std::getline(*csv_file_, line)) {
          break;
        }
        csv_current_line_++;
        // Parse CSV line (simple comma-separated)
        test_row = parse_csv_line(line);
        actual_rows++;
      }

      if (col >= static_cast<int64_t>(test_row.size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column index out of range", K(col), K(test_row.size()), K(ret));
        break;
      }
      const TestValue &val = test_row[col];
      ret = fill_value_directly(expr, row, val, fmt);
    }

    // Update actual row count (may be less than cnt for STREAM_CSV at EOF)
    if (col == 0 && actual_rows < cnt) {
      brs_.size_ = actual_rows;
      brs_.end_ = (data_source_mode_ == MockDataSourceMode::STREAM_CSV) ? true : false;
    }

    // Mark the expression as evaluated and projected
    if (OB_SUCC(ret)) {
      sql::ObBitVector &nulls = expr->get_nulls(eval_ctx_);
      ObDatum *datums = expr->locate_batch_datums(eval_ctx_);
      int64_t row_count = (brs_.size_ > 0) ? brs_.size_ : cnt;
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
    int64_t row_count = (brs_.size_ > 0) ? brs_.size_ : cnt;
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

    switch (obj_type) {
    case ObIntType:
    case ObInt32Type: {
      if (!value.is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value type mismatch, expected int", K(obj_type), K(ret));
      } else {
        int64_t *int_data = reinterpret_cast<int64_t *>(data);
        int_data[row_idx] = value.get_int();
      }
      break;
    }

    case ObUInt64Type:
    case ObUInt32Type: {
      if (!value.is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value type mismatch, expected uint", K(obj_type), K(ret));
      } else {
        uint64_t *uint_data = reinterpret_cast<uint64_t *>(data);
        uint_data[row_idx] = static_cast<uint64_t>(value.get_int());
      }
      break;
    }

    case ObDoubleType: {
      double dval = value.is_double() ? value.get_double() :
                    (value.is_int() ? static_cast<double>(value.get_int()) : 0.0);
      double *double_data = reinterpret_cast<double *>(data);
      double_data[row_idx] = dval;
      break;
    }

    case ObFloatType: {
      float fval = value.is_double() ? static_cast<float>(value.get_double()) :
                   (value.is_int() ? static_cast<float>(value.get_int()) : 0.0f);
      float *float_data = reinterpret_cast<float *>(data);
      float_data[row_idx] = fval;
      break;
    }
    case ObDecimalIntType: {
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
      // val_len may be smaller than int_bytes when the string's parsed precision
      // is less than the expression's declared precision (e.g. "0.50" has
      // precision=3 → val_len=4, but decimal(10,2) needs int_bytes=8).
      // Use ObDecimalIntBuilder::extend() so negative values are sign-extended
      // correctly (fills 0xFF for negative, 0x00 for positive).
      char *dst = data + row_idx * int_bytes;
      ObDecimalIntBuilder builder;
      builder.from(decint, val_len);
      builder.extend(int_bytes);
      MEMCPY(dst, builder.get_decimal_int(), int_bytes);
      break;
    }

    default:
      // For other fixed types, try to treat as int64
      if (value.is_int()) {
        int64_t *int_data = reinterpret_cast<int64_t *>(data);
        int_data[row_idx] = value.get_int();
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported type for direct fill", K(obj_type), K(fmt), K(ret));
      }
      break;
    }
  } else if (fmt == VEC_DISCRETE) {
    // For string types in discrete format, we need to handle ptrs and lens
    // VEC_DISCRETE uses separate ptrs and lens arrays
    char **ptrs = expr->get_discrete_vector_ptrs(eval_ctx_);
    int32_t *lens = expr->get_discrete_vector_lens(eval_ctx_);

    switch (obj_type) {
    case ObVarcharType:
    case ObCharType:
    case ObTextType:
    case ObLongTextType: {
      if (!value.is_string()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value type mismatch, expected string", K(obj_type), K(ret));
      } else {
        const std::string &str = value.get_string();
        // Use the pre-set ptrs[row_idx] which is already correctly initialized by init_vector
        // via reset_discretes_ptr to point to frame + res_buf_off_ + row_idx * res_buf_len_
        char *str_buf = ptrs[row_idx];
        MEMCPY(str_buf, str.data(), str.size());
        lens[row_idx] = static_cast<int32_t>(str.size());
      }
      break;
    }

    case ObNumberType: {
      // For number type, convert to ObCompactNumber format
      // ObNumber internal format: desc_ (4 bytes) + digits_[] (uint32_t array)
      // Use public from() template method which internally creates TAllocator
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

      // Calculate total size: desc_ (4 bytes) + digits (len_ * 4 bytes)
      const int32_t num_len = sizeof(ObNumberDesc) + num.d_.len_ * sizeof(uint32_t);
      // Use the pre-set ptrs[row_idx] which is already correctly initialized by init_vector
      char *num_buf = ptrs[row_idx];
      // Copy desc_
      number::ObCompactNumber *cnum = reinterpret_cast<number::ObCompactNumber *>(num_buf);
      cnum->desc_ = num.d_;
      // Copy digits_
      if (num.d_.len_ > 0) {
        const uint32_t *src_digits = num.get_digits();
        MEMCPY(cnum->digits_, src_digits, num.d_.len_ * sizeof(uint32_t));
      }
      lens[row_idx] = num_len;
      break;
    }

    default:
      // For other types, try string representation
      if (value.is_string()) {
        const std::string &str = value.get_string();
        // Use the pre-set ptrs[row_idx] which is already correctly initialized by init_vector
        char *str_buf = ptrs[row_idx];
        MEMCPY(str_buf, str.data(), str.size());
        lens[row_idx] = static_cast<int32_t>(str.size());
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported type for discrete vector", K(obj_type), K(fmt), K(ret));
      }
      break;
    }
  } else if (fmt == VEC_CONTINUOUS) {
    // Continuous format: offsets[] + contiguous data buffer
    // offsets[row_idx] points to the start of row_idx's data
    // offsets[row_idx + 1] - offsets[row_idx] is the length
    char *cont_data = expr->get_continuous_vector_data(eval_ctx_);
    uint32_t *offsets = expr->get_continuous_vector_offsets(eval_ctx_);

    // Get source data
    const char *src = nullptr;
    int32_t src_len = 0;
    std::string str_val;

    if (value.is_string()) {
      str_val = value.get_string();
      src = str_val.c_str();
      src_len = static_cast<int32_t>(str_val.length());
    } else if (value.is_int()) {
      str_val = std::to_string(value.get_int());
      src = str_val.c_str();
      src_len = static_cast<int32_t>(str_val.length());
    } else if (value.is_double()) {
      str_val = std::to_string(value.get_double());
      src = str_val.c_str();
      src_len = static_cast<int32_t>(str_val.length());
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported value type for continuous vector", K(ret));
    }

    if (OB_SUCC(ret)) {
      // Copy data to contiguous buffer
      MEMCPY(cont_data + offsets[row_idx], src, src_len);
      // Set next offset
      offsets[row_idx + 1] = offsets[row_idx] + src_len;
    }
  } else if (fmt == VEC_UNIFORM) {
    // For VEC_UNIFORM format, write to ObDatum array
    ObDatum *datums = expr->locate_batch_datums(eval_ctx_);

    switch (obj_type) {
    case ObIntType:
    case ObInt32Type: {
      if (!value.is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value type mismatch, expected int", K(obj_type), K(ret));
      } else {
        datums[row_idx].set_int(value.get_int());
      }
      break;
    }

    case ObUInt64Type:
    case ObUInt32Type: {
      if (!value.is_int()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value type mismatch, expected uint", K(obj_type), K(ret));
      } else {
        datums[row_idx].set_uint(static_cast<uint64_t>(value.get_int()));
      }
      break;
    }

    case ObDoubleType: {
      double dval = value.is_double() ? value.get_double() :
                    (value.is_int() ? static_cast<double>(value.get_int()) : 0.0);
      datums[row_idx].set_double(dval);
      break;
    }

    case ObFloatType: {
      float fval = value.is_double() ? static_cast<float>(value.get_double()) :
                   (value.is_int() ? static_cast<float>(value.get_int()) : 0.0f);
      datums[row_idx].set_float(fval);
      break;
    }

    case ObDecimalIntType: {
      std::string str_val;
      if (value.is_string()) {
        str_val = value.get_string();
      } else if (value.is_int()) {
        str_val = std::to_string(value.get_int());
      } else if (value.is_double()) {
        str_val = std::to_string(value.get_double());
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsupported value type for decimal int (uniform)", K(ret));
        break;
      }
      const int16_t precision = expr->datum_meta_.precision_;
      const int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
      ObDecimalInt *decint = nullptr;
      int16_t calc_scale = 0, calc_precision = 0;
      int32_t val_len = 0;
      common::ObArenaAllocator arena;
      if (OB_FAIL(wide::from_string(str_val.c_str(), str_val.length(), arena,
                                     calc_scale, calc_precision, val_len, decint))) {
        LOG_WARN("failed to parse decimal int from string (uniform)", K(str_val.c_str()), K(ret));
        break;
      }
      // reset ptr_ to point to the expression's res_buf before writing:
      // set_decimal_int does memcpy(ptr_, ...) so ptr_ must point to valid frame memory
      expr->reset_ptr_in_datum(eval_ctx_, row_idx);
      // zero the full int_bytes slot first (val_len may be < int_bytes for small values)
      MEMSET(const_cast<char *>(datums[row_idx].ptr_), 0, int_bytes);
      MEMCPY(const_cast<char *>(datums[row_idx].ptr_), decint, val_len);
      datums[row_idx].pack_ = static_cast<uint32_t>(int_bytes);
      break;
    }

    case ObVarcharType:
    case ObCharType:
    case ObTextType:
    case ObLongTextType: {
      if (!value.is_string()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("value type mismatch, expected string", K(obj_type), K(ret));
      } else {
        const std::string &str = value.get_string();
        datums[row_idx].set_string(str.data(), static_cast<uint32_t>(str.size()));
      }
      break;
    }

    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported type for uniform vector", K(obj_type), K(fmt), K(ret));
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

  // Collect sort key values for each row
  std::vector<std::vector<TestValue>> key_values(n);

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

    // 3. Evaluate each sort key expr and record result
    key_values[row_i].reserve(sort_key_specs_.size());
    for (const SortKeySpec &spec : sort_key_specs_) {
      ObDatum *result = nullptr;
      if (OB_FAIL(spec.key_expr->eval(eval_ctx_, result))) {
        LOG_WARN("sort key eval failed", K(ret));
        break;
      }
      if (OB_ISNULL(result)) {
        key_values[row_i].push_back(TestValue::null());
      } else {
        key_values[row_i].push_back(
            datum_to_test_value(*result, spec.key_expr->datum_meta_.type_));
      }
    }
  }

  if (OB_FAIL(ret)) return ret;

  // 4. Stable-sort rows by collected key values
  std::vector<int64_t> idx(n);
  std::iota(idx.begin(), idx.end(), 0);
  std::stable_sort(idx.begin(), idx.end(),
    [&](int64_t a, int64_t b) -> bool {
      for (size_t k = 0; k < sort_key_specs_.size(); ++k) {
        const TestValue &va = key_values[a][k];
        const TestValue &vb = key_values[b][k];
        if (va.is_null() && vb.is_null()) continue;
        const bool asc = sort_key_specs_[k].ascending;
        const bool nf  = sort_key_specs_[k].nulls_first;
        if (va.is_null()) return nf ? asc : !asc;
        if (vb.is_null()) return nf ? !asc : asc;
        if (va < vb) return asc;
        if (vb < va) return !asc;
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