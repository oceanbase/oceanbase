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
#include "share/diagnosis/ob_runtime_metrics.h"
#include "share/diagnosis/ob_profile_name_def.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace common
{
#define REGISTER_METRIC(metric_id, metric)                                                         \
  if (OB_SUCC(ret)) {                                                                              \
    ObOpProfile<ObMetric> *profile = get_current_profile();                                        \
    if (nullptr == profile) {                                                                      \
    } else if (OB_FAIL(profile->get_or_register_metric(metric_id, metric))) {                      \
      COMMON_LOG(WARN, "failed to register metric", K(metric_id));                                 \
    }                                                                                              \
  }

#define INC_METRIC_VAL(metric_id, value)                                                           \
  if (OB_SUCC(ret)) {                                                                              \
    ObOpProfile<ObMetric> *profile = get_current_profile();                                        \
    if (nullptr != profile) {                                                                      \
      ObMetric *metric = nullptr;                                                                  \
      if (OB_FAIL(profile->get_or_register_metric(metric_id, metric))) {                           \
        COMMON_LOG(WARN, "failed to register metric", K(metric_id));                               \
      } else {                                                                                     \
        metric->inc(value);                                                                        \
      }                                                                                            \
    }                                                                                              \
  }

#define SET_METRIC_VAL(metric_id, value)                                                           \
  if (OB_SUCC(ret)) {                                                                              \
    ObOpProfile<ObMetric> *profile = get_current_profile();                                        \
    if (nullptr != profile) {                                                                      \
      ObMetric *metric = nullptr;                                                                  \
      if (OB_FAIL(profile->get_or_register_metric(metric_id, metric))) {                           \
        COMMON_LOG(WARN, "failed to register metric", K(metric_id));                               \
      } else {                                                                                     \
        metric->set(value);                                                                        \
      }                                                                                            \
    }                                                                                              \
  }

struct ObProfileHead
{
  ObProfileHead()
  {
    reset();
  }
  ~ObProfileHead() = default;
  union
  {
    int32_t name_id_;
    struct
    {
      bool enable_rich_format_ : 1;
      ObProfileId id_ : 31;
    };
  };
  int32_t parent_idx_;
  int64_t offset_;
  int64_t length_;
  void reset()
  {
    name_id_ = 0;
    parent_idx_ = 0;
    offset_ = 0;
    length_ = 0;
  }
};

struct ObProfileHeads
{
  int64_t head_count_;
  int64_t metric_count_;
  int64_t head_offset_;
};

template<typename MetricType = ObMetric>
class ObOpProfile
{
public:
  static constexpr int MAX_METRIC_SLOT_CNT =
      ObMetricId::MONITOR_STATNAME_END - ObMetricId::MONITOR_STATNAME_BEGIN;
  static constexpr int METRICS_ID_MAP_SLOT{sizeof(uint8_t) * ObMetricId::MONITOR_STATNAME_END};
  static constexpr uint8_t LOCAL_METRIC_CNT = 8;

public:

  template <typename T>
  struct ObLockFreeLinkWrap
  {
    TO_STRING_KV(K(next_));
    ObLockFreeLinkWrap<T> *next_;
    T elem_;
  };

  typedef ObLockFreeLinkWrap<MetricType> MetricWrap;
  typedef ObLockFreeLinkWrap<ObOpProfile<MetricType> *> ProfileWrap;

public:
  explicit ObOpProfile(ObProfileId id, ObIAllocator *alloc, bool enable_rich_format = false)
      : enable_rich_format_(enable_rich_format), id_(id), alloc_(alloc),
        non_local_metrics_(OB_MALLOC_NORMAL_BLOCK_SIZE, *alloc),
        child_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, *alloc)
  {
    MEMSET(metrics_id_map_, UINT8_MAX, METRICS_ID_MAP_SLOT);
    MEMSET(local_metrics_, 0, LOCAL_METRIC_CNT * sizeof(MetricWrap));
  }

  TO_STRING_KV(K(id_));
  int64_t get_format_size();
  int to_format_json(ObIAllocator *alloc, const char *&result, bool with_outside_label = true,
                     metric::Level display_level = metric::Level::STANDARD);
  int pretty_print(ObIAllocator *alloc, const char *&result, const ObString &prefix,
                   const ObString &child_prefix,
                   metric::Level display_level = metric::Level::STANDARD) const;
  ObProfileId get_id() const { return id_; }
  bool enable_rich_format() { return enable_rich_format_; }
  inline const char *get_name_str() const
  {
    return ObProfileNameSet::get_profile_name(id_, enable_rich_format_);
  }
  inline ObOpProfile<MetricType> *get_parent() { return parent_; }
  inline void set_parent(ObOpProfile<MetricType> *parent) {  parent_ = parent; }
  MetricWrap *get_metric_head() const { return metric_head_; }
  ProfileWrap *get_child_head() const { return child_head_; }

  // get metric value if exists
  OB_INLINE void get_metric_value(ObMetricId metric_id, bool &exist, uint64_t &value) const
  {
    uint8_t arr_idx = metrics_id_map_[metric_id];
    if (arr_idx != UINT8_MAX) {
      exist = true;
      if (arr_idx < LOCAL_METRIC_CNT) {
        value = local_metrics_[arr_idx].elem_.value();
      } else {
        value = non_local_metrics_.at(arr_idx - LOCAL_METRIC_CNT)->elem_.value();
      }
    } else {
      exist = false;
    }
  }

  // get metric if exists, return null if not exists
  const MetricType *get_metric(ObMetricId metric_id) const
  {
    const MetricType *metric = nullptr;
    uint8_t arr_idx = metrics_id_map_[metric_id];
    if (arr_idx != UINT8_MAX) {
      if (arr_idx < LOCAL_METRIC_CNT) {
        metric = &local_metrics_[arr_idx].elem_;
      } else {
        metric = &non_local_metrics_.at(arr_idx - LOCAL_METRIC_CNT)->elem_;
      }
    }
    return metric;
  }

  // get metric if exists, register metric if not exists
  OB_INLINE int get_or_register_metric(ObMetricId metric_id, MetricType *&metric)
  {
    int ret = OB_SUCCESS;
    uint8_t arr_idx = metrics_id_map_[metric_id];
    if (arr_idx != UINT8_MAX) {
      if (arr_idx < LOCAL_METRIC_CNT) {
        metric = &local_metrics_[arr_idx].elem_;
      } else {
        metric = &non_local_metrics_.at(arr_idx - LOCAL_METRIC_CNT)->elem_;
      }
    } else if (OB_FAIL(register_metric(metric_id, metric))) {
      COMMON_LOG(WARN, "failed to register metric");
    }
    return ret;
  }

  int get_or_register_child(ObProfileId id, ObOpProfile<MetricType> *&child);

  int get_all_count(int64_t &metric_count, int64_t &profile_cnt);
  int to_persist_profile(const char *&persist_profile, int64_t &persist_profile_size,
                         ObIAllocator *alloc);
  int convert_current_profile_to_persist(char *buf, int64_t &buf_pos, const int64_t buf_len,
                                         const int64_t max_head_count, ObProfileHead *profile_head,
                                         int32_t &id, int32_t parent_idx);
private:
  int to_format_json_(char *buf, const int64_t buf_len, int64_t &pos,
                      metric::Level display_level) const;
  int pretty_print_(char *buf, const int64_t buf_len, int64_t &pos, const ObString &prefix,
                    const ObString &child_prefix, metric::Level display_level) const;
  int64_t get_persist_profile_size(int64_t metric_count, int64_t child_cnt);

  int register_metric(ObMetricId metric_id, MetricType *&metric);

private:
  union
  {
    int32_t name_id_{0};
    struct
    {
      bool enable_rich_format_ : 1;
      ObProfileId id_ : 31;
    };
  };
  ObIAllocator *alloc_{nullptr};

  int64_t metric_count_{0};
  MetricWrap *metric_head_{nullptr}; // access only by seek profile thread
  MetricWrap *metric_tail_{nullptr}; // access only by query thread
  uint8_t metrics_id_map_[MAX_METRIC_SLOT_CNT];
  MetricWrap local_metrics_[LOCAL_METRIC_CNT];
  ObSEArray<MetricWrap *, 16, ObIAllocator&> non_local_metrics_;
  ObOpProfile<MetricType> *parent_{nullptr};
  ProfileWrap *child_head_{nullptr}; // access only by seek profile thread
  ProfileWrap *child_tail_{nullptr}; // access only by query thread
  ObSEArray<ObOpProfile<MetricType> *, 4, ObIAllocator&> child_array_;

  // control print as key value pair "label":{value} or only value {value}
  // for the operator profile, we only print value in sql plan monitor
  // when query is running, this variables will not be accessed
  bool with_label_{true};
};

int convert_persist_profile_to_realtime(const char *persist_profile, const int64_t persist_len,
                                        ObOpProfile<ObMetric> *&profile, ObIAllocator *alloc);

template<typename MetricType = ObMetric>
inline ObOpProfile<MetricType> *&get_current_profile()
{
  thread_local ObOpProfile<MetricType> *current_profile = nullptr;
  return current_profile;
}

class ObProfileSwitcher
{
public:
  explicit ObProfileSwitcher(ObProfileId id);

  explicit ObProfileSwitcher(ObOpProfile<ObMetric> *new_profile)
  {
    old_profile_ = get_current_profile();
    get_current_profile() = new_profile;
  }

  ~ObProfileSwitcher()
  {
    get_current_profile() = old_profile_;
  }

private:
  ObOpProfile<ObMetric> *old_profile_{nullptr};
};

} // namespace common
} // namespace oceanbase
