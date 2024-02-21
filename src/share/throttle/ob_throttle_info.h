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

#ifndef OCEABASE_SHARE_THROTTLE_INFO_H
#define OCEABASE_SHARE_THROTTLE_INFO_H

#include "share/ob_light_hashmap.h"
#include "lib/objectpool/ob_concurrency_objpool.h"

namespace oceanbase {
namespace share {

class ThrottleID
{
  OB_UNIS_VERSION(1);

public:
  ThrottleID(const int64_t thread_idx)
      : thread_idx_(thread_idx), hash_(murmurhash(&thread_idx, sizeof(thread_idx), 0)) {}
  ThrottleID() = delete;
  ThrottleID(const ThrottleID &) = delete;
  ThrottleID &operator= (const ThrottleID &) = delete;
  ~ThrottleID() {}

  // hash() and is_valid() is necessary function for light hash map
  uint64_t hash() const
  {
    return hash_;
  }
  bool is_valid() const { return thread_idx_ > 0; }
  bool operator<(const ThrottleID &rhs) {
    bool bool_ret = false;
    if (thread_idx_ < rhs.thread_idx_) {
      bool_ret = true;
    }
    return bool_ret;
  }
  bool operator>(const ThrottleID &rhs) {
    bool bool_ret = false;
    if (thread_idx_ > rhs.thread_idx_) {
      bool_ret = true;
    }
    return bool_ret;
  }
  bool operator==(const ThrottleID &other) const
  { return thread_idx_ == other.thread_idx_; }
  bool operator!=(const ThrottleID &other) const
  { return thread_idx_ != other.thread_idx_; }

  TO_STRING_KV(K(thread_idx_), K(hash_));

private:
  const int64_t thread_idx_;
  const uint64_t hash_;
};

struct ObThrottleInfo : public ObLightHashLink<ObThrottleInfo>
{
  ThrottleID throttle_id_;
  bool need_throttle_;
  int64_t sequence_;
  int64_t allocated_size_;
  ObThrottleInfo(int64_t thread_idx) : throttle_id_(thread_idx), need_throttle_(false), sequence_(0), allocated_size_(0) {}
  void reset()
  {
    need_throttle_ = false;
    sequence_ = 0;
    allocated_size_ = 0;
  }
  bool is_throttling()
  {
    return need_throttle_;
  }
  bool contain(const ThrottleID &throttle_id) const  { return this->throttle_id_ == throttle_id; }

  TO_STRING_KV(K(throttle_id_), K(need_throttle_), K(sequence_), K(allocated_size_));
};

struct ObThrottleInfoAllocHandle
{
  ObThrottleInfo *alloc_value()
  {
    return op_alloc_args(ObThrottleInfo, common::get_itid());
  }

  void free_value(ObThrottleInfo *val)
  {
    if (NULL != val) {
      op_free(val);
    }
  }
};

using ObThrottleInfoHashMap =
    ObLightHashMap<ThrottleID, ObThrottleInfo, ObThrottleInfoAllocHandle, common::SpinRWLock, 1 << 10 /* BUCKETS_CNT */>;

struct ObThrottleInfoGuard
{
  ObThrottleInfo *throttle_info_;
  ObThrottleInfoHashMap *throttle_info_map_;

  ObThrottleInfoGuard() : throttle_info_(nullptr), throttle_info_map_(nullptr) {}
  ~ObThrottleInfoGuard() { reset(); }

  bool is_valid() { return OB_NOT_NULL(throttle_info_) && OB_NOT_NULL(throttle_info_map_); }

  void reset()
  {
    if (is_valid()) {
      throttle_info_map_->revert(throttle_info_);
      throttle_info_ = nullptr;
      throttle_info_map_ = nullptr;
    }
  }

  void init(ObThrottleInfo *throttle_info, ObThrottleInfoHashMap *throttle_info_map)
  {
    reset();
    throttle_info_ = throttle_info;
    throttle_info_map_ = throttle_info_map;
  }

  ObThrottleInfo *throttle_info() { return throttle_info_; }
  const ObThrottleInfo *throttle_info() const { return throttle_info_; }

  TO_STRING_KV(KP(throttle_info_), KP(throttle_info_map_));
};
}  // namespace share
}  // namespace oceanbase


#endif