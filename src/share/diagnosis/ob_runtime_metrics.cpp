/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/diagnosis/ob_runtime_metrics.h"

#define USING_LOG_PREFIX COMMON

using namespace oceanbase::sql;

namespace oceanbase
{
namespace common
{
int value_print_help(char *buf, const int64_t buf_len, int64_t &pos, uint64_t value,
                     metric::Unit unit)
{
  static constexpr uint64_t kilo = 1000UL;
  static constexpr uint64_t mega = 1000UL * 1000UL;
  static constexpr uint64_t giga = 1000UL * 1000UL * 1000UL;
  static constexpr uint64_t kilo_byte = 1024UL;
  static constexpr uint64_t mega_byte = 1024UL * 1024UL;
  static constexpr uint64_t giga_byte = 1024UL * 1024UL * 1024UL;
  int ret = OB_SUCCESS;

#define UNIT_PRINT(kilo_base, mega_base, giga_base, unit, kilo_unit, mega_unit, giga_unit, value)  \
  OZ(J_QUOTE());                                                                                   \
  if (value < kilo_base) {                                                                         \
    OZ(BUF_PRINTF("%lu", value));                                                                  \
    OZ(J_NAME(unit));                                                                              \
  } else if (value < mega_base) {                                                                  \
    double double_val = double(value) / kilo_base;                                                 \
    OZ(BUF_PRINTF("%.3lf", double_val));                                                           \
    OZ(J_NAME(kilo_unit));                                                                         \
  } else if (value < giga_base) {                                                                  \
    double double_val = double(value) / mega_base;                                                 \
    OZ(BUF_PRINTF("%.3lf", double_val));                                                           \
    OZ(J_NAME(mega_unit));                                                                         \
  } else {                                                                                         \
    double double_val = double(value) / giga_base;                                                 \
    OZ(BUF_PRINTF("%.3lf", double_val));                                                           \
    OZ(J_NAME(giga_unit));                                                                         \
  }                                                                                                \
  OZ(J_QUOTE());

  switch (unit) {
  case metric::Unit::INT: {
    OZ(BUF_PRINTF("%lu", value));
    break;
  }
  case metric::Unit::BYTES: {
    UNIT_PRINT(kilo_byte, mega_byte, giga_byte, "B", "KB", "MB", "GB", value)
    break;
  }
  case metric::Unit::TIME_NS: {
    UNIT_PRINT(kilo, mega, giga, "NS", "US", "MS", "S", value)
    break;
  }
  case metric::Unit::TIMESTAMP: {
    uint64_t usec = value / 1000000;
    uint64_t nsec = value % 1000000;
    if (!ObTimeConverter::is_valid_datetime(usec)) {
      ret = OB_DATETIME_FUNCTION_OVERFLOW;
      LOG_WARN("datetime overflow", K(value));
    } else {
      time_t t = static_cast<time_t>(usec);
      struct tm tm;
      localtime_r(&t, &tm);
      pos += strftime(buf + pos, buf_len - pos, "%Y-%m-%d %H:%M:%S", &tm);
      OZ(BUF_PRINTF(".%lu", nsec));
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected unit", K(static_cast<int>(unit)));
    break;
  }
  }
#undef UNIT_PRINT
  return ret;
}

int key_value_print_help(char *buf, const int64_t buf_len, int64_t &pos,
  ObMergeMetric &merge_counter, metric::Unit unit)
{
#define PRINT_MAIN_AGG_NAME(agg_name, agg_function_name)                                       \
  OZ(J_QUOTE());                                                                               \
  OZ(J_NAME(#agg_name));                                                                       \
  OZ(J_QUOTE());                                                                               \
  OZ(J_COLON());                                                                               \
  OZ(value_print_help(buf, buf_len, pos, merge_counter.get_##agg_function_name##_value(),      \
  unit));

  int ret = OB_SUCCESS;
  int agg_type = get_metric_agg_type(merge_counter.get_metric_id());
  switch (agg_type & 0x3) {
    case M_FIRST_VAL: {
      PRINT_MAIN_AGG_NAME(first_val, first);
      break;
    }
    case M_SUM: {
      PRINT_MAIN_AGG_NAME(sum, sum);
      break;
    }
    case M_AVG: {
      PRINT_MAIN_AGG_NAME(avg, avg);
      break;
    }
    default: {
      break;
    }
  }

#define PRINT_EXTRA_AGG_NAME(agg_name, agg_function_name) \
  OZ(J_COMMA());                                          \
  OZ(J_QUOTE());                                          \
  OZ(J_NAME(#agg_name));                                  \
  OZ(J_QUOTE());                                          \
  OZ(J_COLON());                                          \
  OZ(value_print_help(buf, buf_len, pos, merge_counter.get_##agg_name##_value(), unit));

  if (agg_type & E_MIN) {
    PRINT_EXTRA_AGG_NAME(min, min);
  }
  if (agg_type & E_MAX) {
    PRINT_EXTRA_AGG_NAME(max, max);
  }
  if (agg_type & E_VARIANCE) {
    PRINT_EXTRA_AGG_NAME(variance, variance);
  }
#undef PRINT_MAIN_AGG_NAME
#undef PRINT_EXTRA_AGG_NAME
  return ret;
}
int64_t ObMetric::get_format_size(bool with_braces)
{
  /*
    format example: "IO_READ_BYTES":"100MB"
    fixed symbols: {"":""}\0  , totally 8 bytes
    name length: static_assert to max 40 bytes
    value length: UINT64_MAX = 18446744073709551615 or 18446744073709.552MB, at most 20 bytes
                  or timestamp format: 2021-05-03 00:00:00.123456, 26 bytes
  */
  static constexpr int64_t fixed_symbol_cnt = 8;
  static constexpr int64_t fixed_symbol_cnt_no_braces = 6; // remove the {} for profile print
  static constexpr int64_t max_value_len = 20;
  static constexpr int64_t timestamp_value_len = 26;

  bool is_timestamp = get_metric_unit(id_) == metric::Unit::TIMESTAMP;
  int64_t fixed_len = with_braces ? fixed_symbol_cnt : fixed_symbol_cnt_no_braces;
  int64_t name_len = strlen(get_metric_name(id_));
  int64_t value_len = is_timestamp ? timestamp_value_len : max_value_len;
  int64_t buf_len = fixed_len + name_len + value_len;
  return buf_len;
}

int ObMetric::to_format_json(ObIAllocator *alloc, const char *&result, bool with_braces)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = nullptr;
  int64_t buf_len = get_format_size(with_braces);
  if (OB_ISNULL(buf = static_cast<char *>(alloc->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate format string buf", K(buf_len));
  } else if (OB_FAIL(to_format_json(buf, buf_len, pos, with_braces))) {
    LOG_WARN("failed to convert to format json");
  } else {
    result = buf;
  }
  return ret;
}

int ObMetric::to_format_json(char *buf, const int64_t buf_len, int64_t &pos, bool with_braces)
{
  // format example: {"IO_READ_BYTES": "100MB"}
  int ret = OB_SUCCESS;
  if (with_braces) {
    OZ(J_OBJ_START());
  }
  OZ(J_QUOTE());
  OZ(J_NAME(get_metric_name(id_)));
  OZ(J_QUOTE());
  OZ(J_COLON());
  OZ(value_print_help(buf, buf_len, pos, value(), get_metric_unit(id_)));
  if (with_braces) {
    OZ(J_OBJ_END());
  }
  return ret;
}

/*
  format example:
                    |<-main_agg->|<----------------extra_agg-------------------->|
  {"IO_READ_BYTES":{"avg":"100MB","min":"100MB","max":"100MB","variance":"100MB"}}
  fixed symbols: {"":{}}\0  , totally 8 bytes
  metric_name_length is variable, example: IO_READ_BYTES, at most 33 bytes
  "":""  for a metric, fixed length is 5 bytes, total length is 5 * N
  agg_name_length is variable, example: avg, sum, first_val
  value length: UINT64_MAX = 18446744073709551615 or 18446744073709.552MB, at most 20 bytes
  N value append N - 1 ', ' which needs 2(N - 1) bytes if N >= 2
  so, the length is 8 + 13 + (5 * 4) + (3 + 3 + 3 + 8) + 20 * 4 + 2 * 3
*/
int64_t ObMergeMetric::get_format_size(bool with_braces)
{
  static constexpr int64_t fixed_symbol_cnt = 8;
  static constexpr int64_t fixed_symbol_cnt_no_braces = 6; // remove the {} for profile print
  int64_t name_len = strlen(get_metric_name(id_));
  int64_t agg_name_len = 0;
  int64_t agg_count = 0;
  int agg_type = get_metric_agg_type(id_);
#define AGG_CASE(main_agg, agg_name_len_, agg_count_)                                              \
  case main_agg: {                                                                                 \
    agg_name_len += agg_name_len_;                                                                 \
    agg_count += agg_count_;                                                                       \
    break;                                                                                         \
  }
  switch (agg_type & 0x3) {
    AGG_CASE(M_FIRST_VAL, 11, 1)
    AGG_CASE(M_SUM, 3, 1)
    AGG_CASE(M_AVG, 3, 1)
    default: {
      break;
    }
  }
#undef AGG_CASE
  if (agg_type & E_MIN) {
    agg_name_len += 3;
    agg_count++;
  }
  if (agg_type & E_MAX) {
    agg_name_len += 3;
    agg_count++;
  }
  if (agg_type & E_VARIANCE) {
    agg_name_len += 8;
    agg_count++;
  }
  int64_t buf_len = 0;
  int64_t fixed_len = with_braces ? fixed_symbol_cnt : fixed_symbol_cnt_no_braces;
  if (agg_count > 1) {
    buf_len = fixed_symbol_cnt + name_len + 5 * agg_count + agg_name_len + 20 * agg_count
              + 2 * (agg_count - 1);
  } else {
    buf_len = buf_len = fixed_symbol_cnt + name_len + 5 * agg_count + agg_name_len + 20 * agg_count;
  }
  return buf_len;
}

int ObMergeMetric::to_format_json(char *buf, const int64_t buf_len, int64_t &pos, bool with_braces)
{
  int ret = OB_SUCCESS;
  if (with_braces) {
    OZ(J_OBJ_START());
  }
  OZ(J_QUOTE());
  OZ(J_NAME(get_metric_name(id_)));
  OZ(J_QUOTE());
  OZ(J_COLON());
  OZ(J_OBJ_START());
  OZ(key_value_print_help(buf, buf_len, pos, *this, get_metric_unit(id_)));
  OZ(J_OBJ_END());
  if (with_braces) {
    OZ(J_OBJ_END());
  }
  return ret;
}

void ObMergeMetric::update(uint64_t value)
{
  int agg_type = get_metric_agg_type(id_);
  switch (agg_type & 0x3) {
    case M_FIRST_VAL: {
      if (first_value_ == 0) {
        first_value_ = value;
      }
      break;
    }
    case M_SUM:
    case M_AVG: {
      sum_value_ += value;
      count_++;
      break;
    }
    default: {
      break;
    }
  }
  if (agg_type & E_MIN) {
    if (min_value_ == 0 || min_value_ > value) {
      min_value_ = value;
    }
  }
  if (agg_type & E_MAX) {
    if (max_value_ == 0 || max_value_ < value) {
      max_value_ = value;
    }
  }
  if (agg_type & E_VARIANCE) {
    if (count_ > 1) {
      uint64_t cur_avg = sum_value_ / count_;
      uint64_t last_avg = (sum_value_ - value) / (count_ - 1);
      uint64_t last_variance2 = variance_value_ * variance_value_;
      variance_value_ =
          sqrt(last_variance2 + ((value - cur_avg) * (value - last_avg) - last_variance2) / count_);
    }
  }
}

uint64_t ObMergeMetric::value() const
{
  int agg_type = get_metric_agg_type(id_);
  uint64_t value = 0;
  switch (agg_type & 0x3) {
    case M_FIRST_VAL: {
      value = get_first_value();
      break;
    }
    case M_SUM: {
      value = get_sum_value();
      break;
    }
    case M_AVG: {
      value = get_avg_value();
      break;
    }
    default: {
      break;
    }
  }
  return value;
}

} // namespace common
} // namespace oceanbase
