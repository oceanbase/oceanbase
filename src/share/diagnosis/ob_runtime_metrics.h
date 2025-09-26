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

#pragma once
#include <stdint.h>
#include "share/diagnosis/ob_sql_monitor_statname.h"
#include "lib/atomic/ob_atomic.h"
#include "src/sql/ob_sql_define.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace metric
{

#define M_FIRST_VAL         (0ULL)
#define M_SUM               (1ULL)
#define M_AVG               (1ULL << 1)
#define E_MIN               (1ULL << 2)
#define E_MAX               (1ULL << 3)
#define E_VARIANCE          (1ULL << 4)

enum class Unit
{
  INVALID,
  INT, // numerical, will not scale print format
  BYTES,
  TIMESTAMP,
  TIME_NS,
};

enum class Level
{
  CRITICAL = 0, // core metrics, essential for real-time monitoring
  STANDARD,     // standard metrics, commonly used for monitoring but are not urgent
  AD_HOC,       // used for debugging or deep-dive analysis, not display default
};

} // namespace metric

namespace common
{
using ObMetricId = sql::ObSqlMonitorStatIds::ObSqlMonitorStatEnum;
inline const char *get_metric_name(ObMetricId id)
{
  if (OB_UNLIKELY(id >= ObMetricId::MONITOR_STATNAME_END
                  || id <= ObMetricId::MONITOR_STATNAME_BEGIN)) {
    return "INVALID";
  } else {
    return sql::OB_MONITOR_STATS[id].name_;
  }
}

inline const char *get_metric_description(ObMetricId id)
{
  if (OB_UNLIKELY(id >= ObMetricId::MONITOR_STATNAME_END
                  || id <= ObMetricId::MONITOR_STATNAME_BEGIN)) {
    return "INVALID";
  } else {
    return sql::OB_MONITOR_STATS[id].description_;
  }
}

inline metric::Unit get_metric_unit(ObMetricId id)
{
  if (OB_UNLIKELY(id >= ObMetricId::MONITOR_STATNAME_END
                  || id <= ObMetricId::MONITOR_STATNAME_BEGIN)) {
    return metric::Unit::INVALID;
  } else {
    return static_cast<metric::Unit>(sql::OB_MONITOR_STATS[id].unit_);
  }
}

inline int get_metric_agg_type(ObMetricId id)
{
  if (OB_UNLIKELY(id >= ObMetricId::MONITOR_STATNAME_END
                  || id <= ObMetricId::MONITOR_STATNAME_BEGIN)) {
    return M_FIRST_VAL;
  } else {
    return sql::OB_MONITOR_STATS[id].agg_type_;
  }
}

inline metric::Level get_metric_level(ObMetricId id)
{
  if (OB_UNLIKELY(id >= ObMetricId::MONITOR_STATNAME_END
                  || id <= ObMetricId::MONITOR_STATNAME_BEGIN)) {
    return metric::Level::AD_HOC;
  } else {
    return static_cast<metric::Level>(sql::OB_MONITOR_STATS[id].level_);
  }
}

template<typename MetricType>
class ObOpProfile;

struct ObMetric
{
public:
  OB_INLINE uint64_t value() const
  {
    return ATOMIC_LOAD_RLX(&value_);
  }

  // for counter
  OB_INLINE void inc(uint64_t value) {
    ATOMIC_AAF(&value_, value);
  }

  // for gauge
  OB_INLINE void set(uint64_t value) {
    ATOMIC_STORE_RLX(&value_, value);
  }

  int64_t get_format_size(bool with_braces = true);
  int to_format_json(ObIAllocator *alloc, const char *&result, bool with_braces = true);
  int to_format_json(char *buf, const int64_t buf_len, int64_t &pos, bool with_braces = true);
  int32_t get_metric_id() { return id_; }
  TO_STRING_KV(K(id_), K(value_));

private:
  ObMetricId id_;
  uint64_t value_;
  friend ObOpProfile<ObMetric>;
};

struct ObMergeMetric
{
public:
  void update(uint64_t value);
  int64_t get_format_size(bool with_braces = true);
  int to_format_json(char *buf, const int64_t buf_len, int64_t &pos, bool with_braces);
  ObMetricId get_metric_id() { return id_; }
  uint64_t value() const;
  uint64_t get_sum_value() const { return sum_value_; }
  uint64_t get_avg_value() const {
    if (count_ > 0) {
      return sum_value_ / count_;
    } else {
      return 0;
    }
  }
  uint64_t get_min_value() const { return min_value_; }
  uint64_t get_max_value() const { return max_value_; }
  uint64_t get_first_value() const { return first_value_; }
  uint64_t get_variance_value() const { return variance_value_; }
  TO_STRING_KV(K(id_), K(count_), K(sum_value_), K(min_value_), K(max_value_), K(first_value_), K(variance_value_));

private:
  ObMetricId id_;
  uint64_t count_{0};
  uint64_t sum_value_{0};
  uint64_t min_value_{0};
  uint64_t max_value_{0};
  uint64_t first_value_{0};
  uint64_t variance_value_{0};
  friend ObOpProfile<ObMergeMetric>;
};

} // namespace common
} // namespace oceanbase
