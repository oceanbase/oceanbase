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

#include "share/diagnosis/ob_runtime_profile.h"
#include "lib/container/ob_array_iterator.h"

#define USING_LOG_PREFIX COMMON

namespace oceanbase
{
namespace common
{

template<typename MetricType>
int64_t ObOpProfile<MetricType>::get_format_size()
{
  /*
    format example:
    "PHY_JOIN_FILTER":{
        "filtered row count":60,
        "total row count":100,
        "XXX_PROFILE_NAME":{
        }
    }
    fixed symbols: "":{}\0    totally 6 bytes
    name: static_assert at most 38 bytes
    N metrics size
    M childs size
    append (N + M - 1) ', ' to separate items
    6 + name + 2 * (N + M - 1)
  */
  static constexpr int64_t fixed_symbol_cnt = 6;
  int64_t name_len = strlen(get_name_str());
  int64_t buf_len = fixed_symbol_cnt + name_len;

  int64_t metric_cnt = 0;
  MetricWrap *cur_metric = ATOMIC_LOAD(&metric_head_);
  while (nullptr != cur_metric) {
    metric_cnt++;
    buf_len += cur_metric->elem_.get_format_size(false);
    cur_metric = ATOMIC_LOAD(&cur_metric->next_);
  }

  int64_t child_cnt = 0;
  ProfileWrap *cur_child = ATOMIC_LOAD(&child_head_);
  while (nullptr != cur_child) {
    child_cnt++;
    buf_len += cur_child->elem_->get_format_size();
    cur_child = ATOMIC_LOAD(&cur_child->next_);
  }

  if (metric_cnt + child_cnt >= 2) {
    // ", " between metrics and sub_profiles
    buf_len += 2 * (metric_cnt + child_cnt - 1);
  }
  return buf_len;
}

template <typename MetricType>
int ObOpProfile<MetricType>::to_format_json(ObIAllocator *alloc, const char *&result,
                                            bool with_outside_label, metric::Level display_level)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = get_format_size();
  if (with_outside_label) {
    // add out side {}
    buf_len += 2;
  } else {
    /*
      remove the outside label

        "PHY_JOIN_FILTER":{                         {
          "filtered row count":60,                    "filtered row count":60,
          "total row count":100,     ====>            "total row count":100,
          "XXX_PROFILE_NAME":{                        "XXX_PROFILE_NAME":{
          }                                           }
        }                                           }
      remove name and  "":
    */
    buf_len -= strlen(get_name_str());
    buf_len -= 3;
    with_label_ = false;
  }
  char *buf = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char *>(alloc->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate format string buf", K(buf_len));
  } else if (with_outside_label && OB_FAIL(J_OBJ_START())) {
    LOG_WARN("failed to pro data buf print");
  } else if (OB_FAIL(to_format_json_(
                 buf,
                 with_outside_label ? buf_len - 1 : buf_len, // buf_len - 1 to preserve last '}'
                 pos, display_level))) {
    LOG_WARN("failed to convert to format json", K(pos), K(buf_len));
  } else if (with_outside_label && OB_FAIL(J_OBJ_END())) {
  } else {
    result = buf;
  }
  with_label_ = true;
  return ret;
}

template <typename MetricType>
int ObOpProfile<MetricType>::to_format_json_(char *buf, const int64_t buf_len, int64_t &pos,
                                             metric::Level display_level) const
{
  /*
    format example:
    "PHY_JOIN_FILTER":{
        "filtered row count":60,
        "total row count":100,
        "XXX_PROFILE_NAME":{
        }
    }
  */
  int ret = OB_SUCCESS;
  if (with_label_) {
    OZ(J_QUOTE());              //   "
    OZ(J_NAME(get_name_str())); //   PHY_JOIN_FILTER
    OZ(J_QUOTE());              //   "
    OZ(J_COLON());              //   :
  }
  OZ(J_OBJ_START());            //   {
  MetricWrap *cur_metric = ATOMIC_LOAD(&metric_head_);
  ProfileWrap *cur_child = ATOMIC_LOAD(&child_head_);
  bool has_metric = false;
  bool has_child = (cur_child != nullptr);
  if (OB_SUCC(ret) && pos < buf_len) {
    while (nullptr != cur_metric && OB_SUCC(ret)) {
      if (get_metric_level(cur_metric->elem_.id_) > display_level) {
        // skip metric whose level > display level
        cur_metric = ATOMIC_LOAD(&cur_metric->next_);
        continue;
      }
      if (pos + cur_metric->elem_.get_format_size() > buf_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("pos overflow", K(pos), K(buf_len), K(cur_metric->elem_.id_),
                 K(cur_metric->elem_.get_format_size()));
      } else if (has_metric && OB_FAIL(J_COMMA())) {// if already print a metric, add ", "
        LOG_WARN("failed to print comma", K(pos), K(buf_len));
      } else if (OB_FAIL(cur_metric->elem_.to_format_json(buf, buf_len, pos, false))) {
        LOG_WARN("failed to format metric", K(pos), K(buf_len), K(cur_metric->elem_.id_));
      } else {
        has_metric = true;
        cur_metric = ATOMIC_LOAD(&cur_metric->next_);
      }
    }
  }
  if (has_metric && has_child) {
    OZ(J_COMMA());
  }
  if (OB_SUCC(ret) && pos < buf_len) {
    while (nullptr != cur_child && OB_SUCC(ret)) {
      if (OB_FAIL(cur_child->elem_->to_format_json_(buf, buf_len, pos, display_level))) {
        LOG_WARN("failed to format child profile", K(pos), K(buf_len));
      } else {
        cur_child = ATOMIC_LOAD(&cur_child->next_);
        if (nullptr != cur_child) {
          OZ(J_COMMA());
        }
      }
    }
  }
  OZ(J_OBJ_END());              //   }
  return ret;
}

template <typename MetricType>
int ObOpProfile<MetricType>::pretty_print(ObIAllocator *alloc, const char *&result,
                                          const ObString &prefix,
                                          const ObString &child_prefix,
                                          metric::Level display_level) const

{
  int ret = OB_SUCCESS;
  int64_t buf_len = 8 * 1024;
  int64_t pos = 0;
  char *buf = static_cast<char *>(alloc->alloc(buf_len));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf");
  } else if (OB_FAIL(pretty_print_(buf, buf_len, pos, prefix, child_prefix, display_level))) {
    LOG_WARN("failed to pretty print");
  } else {
    result = buf;
  }
  return ret;
}

template <typename MetricType>
int ObOpProfile<MetricType>::pretty_print_(char *buf, const int64_t buf_len, int64_t &pos,
                                           const ObString &prefix,
                                           const ObString &child_prefix,
                                           metric::Level display_level) const

{
  int ret = OB_SUCCESS;
  MetricWrap *cur_metric = ATOMIC_LOAD(&metric_head_);
  ProfileWrap *cur_child = ATOMIC_LOAD(&child_head_);
  bool has_metric = false;
  bool has_child = (cur_child != nullptr);
  if (prefix.length() > 0) {
    OZ(BUF_PRINTF("%s", prefix.ptr()));
  }
  OZ(BUF_PRINTF("%s \n", get_name_str()));
  if (OB_SUCC(ret) && pos < buf_len) {
    while (nullptr != cur_metric && OB_SUCC(ret)) {
      if (get_metric_level(cur_metric->elem_.id_) > display_level) {
        // skip metric whose level > display level
        cur_metric = ATOMIC_LOAD(&cur_metric->next_);
        continue;
      }
      if (has_metric && OB_FAIL(BUF_PRINTF("\n"))) {
        LOG_WARN("failed to buf print");
      } else if (OB_FAIL(cur_metric->elem_.pretty_print(buf, buf_len, pos, child_prefix))) {
        LOG_WARN("failed to print metric", K(pos), K(buf_len), K(cur_metric->elem_.id_));
      } else {
        has_metric = true;
        cur_metric = ATOMIC_LOAD(&cur_metric->next_);
      }
    }
  }
  if (has_metric && has_child) {
    OB_FAIL(BUF_PRINTF("\n"));
  }
  if (OB_SUCC(ret) && pos < buf_len) {
    char temp_buf[1024] = "\0";
    int64_t temp_buf_len = sizeof(temp_buf);
    int64_t temp_pos = 0;
    OZ(oceanbase::common::databuff_printf(temp_buf, temp_buf_len, temp_pos,
                                          "%s  ", child_prefix.ptr()));
    ObString sub_child_prefix(temp_pos, temp_buf);
    while (nullptr != cur_child && OB_SUCC(ret)) {
      if (OB_FAIL(cur_child->elem_->pretty_print_(
              buf, buf_len, pos, child_prefix, sub_child_prefix, display_level))) {
        LOG_WARN("failed to print child profile", K(pos), K(buf_len));
      } else {
        cur_child = ATOMIC_LOAD(&cur_child->next_);
        if (NULL != cur_child) {
          OZ(BUF_PRINTF("\n"));
        }
      }
    }
  }
  return ret;
}

template<typename MetricType>
int ObOpProfile<MetricType>::register_metric(ObMetricId metric_id, MetricType *&metric)
{
  int ret = OB_SUCCESS;
  MetricWrap *new_metric = nullptr;
  int64_t metric_count = ATOMIC_LOAD_RLX(&metric_count_);
  if (metric_count < LOCAL_METRIC_CNT) {
    new_metric = &local_metrics_[metric_count];
    new_metric->elem_.id_ = metric_id;
  } else {
    char *buf = nullptr;
    if (OB_ISNULL(buf = (char *)alloc_->alloc(sizeof(MetricWrap)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc memory");
    } else {
      MEMSET(buf, 0, sizeof(MetricWrap));
      new_metric = reinterpret_cast<MetricWrap*>(buf);
      new_metric->elem_.id_ = metric_id;
      if (OB_FAIL(non_local_metrics_.push_back(new_metric))) {
        LOG_WARN("failed to push back metric");
      }
    }
  }

  if (OB_SUCC(ret)) {
    metrics_id_map_[metric_id] = metric_count;
    metric = &new_metric->elem_;
    if (0 == metric_count) {
      // first metric
      ATOMIC_STORE_RLX(&metric_head_, new_metric);
    } else {
      ATOMIC_STORE_RLX(&(metric_tail_->next_), new_metric);
    }
    // tail only access by query thread
    metric_tail_ = new_metric;
    // add metric count finally
    __sync_fetch_and_add(&metric_count_, 1, __ATOMIC_RELAXED);
  }
  return ret;
}

template<typename MetricType>
int ObOpProfile<MetricType>::get_or_register_child(ObProfileId id, ObOpProfile<MetricType> *&child)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (uint64_t i = 0; i < child_array_.count(); ++i) {
    if (child_array_.at(i)->id_ == id) {
      found = true;
      child = child_array_.at(i);
      break;
    }
  }
  if (!found) {
    char *buf = nullptr;
    size_t alloc_size = sizeof(ProfileWrap) + sizeof(ObOpProfile<MetricType>);
    if (OB_ISNULL(buf = (char *)alloc_->alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory");
    } else {
      MEMSET(buf, 0, alloc_size);
      ProfileWrap *profile_wrap = reinterpret_cast<ProfileWrap *>(buf);
      ObOpProfile<MetricType> *child_profile =
          new (buf + sizeof(ProfileWrap)) ObOpProfile<MetricType>(id, alloc_);
      profile_wrap->elem_ = child_profile;
      if (OB_FAIL(child_array_.push_back(child_profile))) {
        LOG_WARN("failed to push_back child profile");
      } else {
        child_profile->parent_ = this;
        child = child_profile;
        if (nullptr == ATOMIC_LOAD(&child_head_)) {
          // first child
          ATOMIC_STORE_RLX(&child_head_, profile_wrap);
        } else {
          ATOMIC_STORE_RLX(&(child_tail_->next_), profile_wrap);
        }
        // tail only access by query thread
        child_tail_ = profile_wrap;
      }
    }
  }
  return ret;
}

template<typename MetricType>
int ObOpProfile<MetricType>::get_all_count(int64_t &metric_count, int64_t &profile_cnt)
{
  int ret = OB_SUCCESS;
  metric_count += ATOMIC_LOAD(&metric_count_);
  ProfileWrap *cur_child = ATOMIC_LOAD(&child_head_);
  while (nullptr != cur_child) {
    profile_cnt += 1;
    cur_child->elem_->get_all_count(metric_count, profile_cnt);
    cur_child = ATOMIC_LOAD(&cur_child->next_);
  }
  return ret;
}

// profile_cnt include itself
template<typename MetricType>
int64_t ObOpProfile<MetricType>::get_persist_profile_size(int64_t metric_count, int64_t profile_cnt)
{
  return sizeof(ObProfileHeads) + sizeof(ObProfileHead) * profile_cnt
         + 2 * sizeof(uint64_t) * metric_count;
}

template<>
int ObOpProfile<ObMetric>::convert_current_profile_to_persist(char *buf, int64_t &buf_pos,
                                                    const int64_t buf_len,
                                                    const int64_t max_head_count,
                                                    ObProfileHead *profile_head, int32_t &id,
                                                    int32_t parent_idx)
{
  int ret = OB_SUCCESS;
  static constexpr uint64_t step = sizeof(uint64_t) * 2;
  int32_t cur_id = id;
  if (OB_ISNULL(buf) || OB_ISNULL(profile_head)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("profile_head or buf is null", K(profile_head), K(buf));
  } else {
    profile_head[id].name_id_ = name_id_;
    profile_head[id].parent_idx_ = parent_idx;
    profile_head[id].offset_ = buf_pos;
    MetricWrap *cur_metric = ATOMIC_LOAD(&metric_head_);
    uint64_t *cur_buf = reinterpret_cast<uint64_t *>(buf + buf_pos);
    while (nullptr != cur_metric && buf_pos + step <= buf_len) {
      uint64_t metric_id = cur_metric->elem_.get_metric_id();
      uint64_t metric_value = cur_metric->elem_.value();
      *cur_buf++ = metric_id;
      *cur_buf++ = metric_value;
      buf_pos += step;
      cur_metric = ATOMIC_LOAD(&cur_metric->next_);
    }
    profile_head[id].length_ = buf_pos - profile_head[id].offset_;
    ProfileWrap *cur_child = ATOMIC_LOAD(&child_head_);
    while (nullptr != cur_child && (id < max_head_count - 1)) {
      cur_child->elem_->convert_current_profile_to_persist(buf, buf_pos, buf_len, max_head_count,
                                                           profile_head, ++id, cur_id);
      cur_child = ATOMIC_LOAD(&cur_child->next_);
    }
  }
  return ret;
}

template<>
int ObOpProfile<ObMergeMetric>::convert_current_profile_to_persist(char *buf, int64_t &buf_pos,
                                                    const int64_t buf_len,
                                                    const int64_t max_head_count,
                                                    ObProfileHead *profile_head, int32_t &id,
                                                    int32_t parent_id)
{
  return OB_NOT_IMPLEMENT;
}
/*
profile structure
          A
        /   \
       B     C    -----------------------------------
      / \   / \   |                                 |
     D   E F   G  |                                 |
  |head_count profile_head A profile_head B ……|profile A| profile B| ……
  |<------      ObProfileHeads        ------->|

  |metric_id1 value1 metric_id2 value2  …… |
  |<------        profile A        ------->|
  one metric has id and value, so it use 16 bytes

  total_size = sizeof(ObProfileHeads) + sizeof(ObProfileHead) * profile_cnt
              + 2 * 8 * metric_cnt
*/
template<typename MetricType>
int ObOpProfile<MetricType>::to_persist_profile(const char *&persist_profile, int64_t &persist_profile_size,
                                    ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  int64_t metric_count = 0;
  int64_t profile_cnt = 1; // include itself
  char *buf = nullptr;
  if (OB_FAIL(get_all_count(metric_count, profile_cnt))) {
    LOG_WARN("failed to get persist size");
  } else if (FALSE_IT(persist_profile_size = get_persist_profile_size(metric_count, profile_cnt))) {
    // do nothing
  } else if (OB_ISNULL(buf = static_cast<char *>(alloc->alloc(persist_profile_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory");
  } else {
    MEMSET(buf, 0, persist_profile_size);
    ObProfileHeads *profile_heads = new (buf) ObProfileHeads();
    profile_heads->head_count_ = profile_cnt;
    profile_heads->metric_count_ = metric_count;
    profile_heads->head_offset_ = sizeof(ObProfileHeads);
    for (int64_t i = 0; i < profile_cnt; ++i) {
      new (buf + sizeof(ObProfileHeads) + sizeof(ObProfileHead) * i) ObProfileHead();
    }
    ObProfileHead *profile_head = reinterpret_cast<ObProfileHead *>(buf + sizeof(ObProfileHeads));
    int64_t buf_pos = sizeof(ObProfileHeads) + sizeof(ObProfileHead) * profile_cnt;
    int32_t id = 0;
    if (OB_FAIL(convert_current_profile_to_persist(buf, buf_pos, persist_profile_size,
                                                   profile_heads->head_count_,
                                                   profile_head, id, -1))) {
      LOG_WARN("failed to convert profile to persist");
    } else {
      persist_profile = buf;
    }
  }
  return ret;
}

// only select thread or merge thread will call convert_persist_profile_to_realtime.
// select thread use its own thread_local allocator and release all profile in the end
// merge thread get read_time profile, and then use another allocator to convert it to persist
// profile
int convert_persist_profile_to_realtime(const char *persist_profile, const int64_t persist_len,
                                        ObOpProfile<ObMetric> *&profile, ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  static constexpr uint64_t step = sizeof(uint64_t) * 2;
  int64_t buf_pos = 0;
  const ObProfileHeads *profile_heads = reinterpret_cast<const ObProfileHeads *>(persist_profile);
  int64_t profile_cnt = profile_heads->head_count_;
  int64_t metric_cnt = profile_heads->metric_count_;
  const ObProfileHead *profile_head =
      reinterpret_cast<const ObProfileHead *>(persist_profile + profile_heads->head_offset_);
  ObOpProfile<ObMetric> **profiles_array = nullptr;
  if (OB_UNLIKELY(profile_heads->head_offset_ > persist_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("unexpected persist profile size", K(profile_heads->head_offset_), K(persist_len));
  } else if (OB_ISNULL(profiles_array = static_cast<ObOpProfile<ObMetric> **>(
                           alloc->alloc(sizeof(ObOpProfile<ObMetric> *) * profile_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory");
  } else {
    MEMSET(profiles_array, 0, sizeof(ObOpProfile<ObMetric> *) * profile_cnt);
    const uint64_t *cur_metric_and_value_ptr = nullptr;
    uint64_t cur_metric_id = 0;
    uint64_t cur_metric_value;
    ObOpProfile<ObMetric> *new_profile = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < profile_cnt; ++i) {
      if (profile_head[i].parent_idx_ != -1 && profile_head[i].parent_idx_ < profile_cnt
          && OB_NOT_NULL(profiles_array[profile_head[i].parent_idx_])) {
        profiles_array[profile_head[i].parent_idx_]->get_or_register_child(profile_head[i].id_,
                                                                           new_profile);
      } else {
        new_profile = OB_NEWx(ObOpProfile<ObMetric>, alloc, profile_head[i].id_, alloc,
                              profile_head[i].enable_rich_format_);
      }
      if (OB_NOT_NULL(new_profile)) {
        profiles_array[i] = new_profile;
        cur_metric_and_value_ptr =
            reinterpret_cast<const uint64_t *>(persist_profile + profile_head[i].offset_);
        int64_t cur_metric_cnt = profile_head[i].length_ / sizeof(uint64_t) / 2;
        int64_t pos = profile_head[i].offset_;
        for (int64_t j = 0; j < cur_metric_cnt && pos + step <= persist_len && OB_SUCC(ret); j++) {
          cur_metric_id = *cur_metric_and_value_ptr++;
          cur_metric_value = *cur_metric_and_value_ptr++;
          pos += step;
          ObMetric *metric = nullptr;
          if (OB_FAIL(new_profile->get_or_register_metric(
                  static_cast<ObMetricId>(cur_metric_id), metric))) {
            LOG_WARN("failed to register metric");
          } else if (OB_NOT_NULL(metric)) {
            metric->set(cur_metric_value);
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    profile = profiles_array[0];
  }
  return ret;
}

ObProfileSwitcher::ObProfileSwitcher(ObProfileId name)
{
  int ret = OB_SUCCESS;
  old_profile_ = get_current_profile();
  ObOpProfile<ObMetric> *new_profile = nullptr;
  if (nullptr == old_profile_) {
    // disabled
  } else if (OB_FAIL(old_profile_->get_or_register_child(name, new_profile))) {
    LOG_WARN("failed to get or add child", K(name));
  } else {
    get_current_profile() = new_profile;
  }
}

template class ObOpProfile<ObMetric>;
template class ObOpProfile<ObMergeMetric>;
} // namespace common
} // namespace oceanbase