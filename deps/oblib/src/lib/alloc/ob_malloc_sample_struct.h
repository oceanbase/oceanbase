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

#ifndef OCEANBASE_MALLOC_SAMPLE_STRUCT_H_
#define OCEANBASE_MALLOC_SAMPLE_STRUCT_H_

#include "lib/alloc/alloc_struct.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace lib
{
static const int32_t MAX_MALLOC_SAMPLER_NUM = (1<<15) - 1;

class ObMallocSampleLimiter
{
public:
  ObMallocSampleLimiter();
  bool try_acquire(int64_t alloc_bytes);
  static bool malloc_sample_allowed(const int64_t size, const ObMemAttr &attr);
  static void set_interval(int32_t max_ratio, int32_t min_ratio);
private:
  static int32_t min_malloc_sample_interval;
  static int32_t max_malloc_sample_interval;
  static const int32_t INTERVAL_UPPER_LIMIT = 10000;
  static const int32_t MUST_SAMPLE_SIZE = 16<<20;
  static const int32_t CUMULATIVE_SAMPLE_SIZE = 4<<20;
  int64_t count_;
  int64_t hold_;
};

static ObMallocSampleLimiter rate_limiters[MAX_MALLOC_SAMPLER_NUM + 1];

struct ObMallocSampleKey
{
  ObMallocSampleKey()
  {}
  int64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool operator==(const ObMallocSampleKey &other) const;
  int64_t tenant_id_;
  int64_t ctx_id_;
  char label_[lib::AOBJECT_LABEL_SIZE + 1];
  void *bt_[AOBJECT_BACKTRACE_COUNT];
};

struct ObMallocSampleValue
{
  ObMallocSampleValue()
  {}
  ObMallocSampleValue(int64_t alloc_count, int64_t alloc_bytes)
    : alloc_count_(alloc_count), alloc_bytes_(alloc_bytes)
  {}
  int64_t alloc_count_;
  int64_t alloc_bytes_;
};

typedef hash::ObHashMap<ObMallocSampleKey, ObMallocSampleValue,
                        hash::NoPthreadDefendMode> ObMallocSampleMap;
typedef hash::HashMapPair<ObMallocSampleKey, ObMallocSampleValue> ObMallocSamplePair;
typedef ObMallocSampleMap::iterator ObMallocSampleIter;
struct ObMallocSamplePairCmp
{
  bool operator()(const ObMallocSamplePair *left, const ObMallocSamplePair *right)
  {
    bool bret = true;
    if (left->first.tenant_id_ != right->first.tenant_id_) {
      bret = left->first.tenant_id_ < right->first.tenant_id_;
    } else if (left->first.ctx_id_ != right->first.ctx_id_) {
      bret = left->first.ctx_id_ < right->first.ctx_id_;
    } else if (0 != STRCMP(left->first.label_, right->first.label_)) {
      bret = STRCMP(left->first.label_, right->first.label_) < 0;
    } else if (left->second.alloc_bytes_ != right->second.alloc_bytes_) {
      bret = left->second.alloc_bytes_ > right->second.alloc_bytes_;
    }
    return bret;
  }
};

inline uint64_t ob_malloc_sample_hash(const char* data)
{
  return (uint64_t)data * 0xdeece66d + 0xb;
}

inline ObMallocSampleLimiter::ObMallocSampleLimiter()
  : count_(0), hold_(0)
{}

inline bool ObMallocSampleLimiter::try_acquire(int64_t alloc_bytes)
{
  bool ret = false;
  // Condition sample: controlled by sampler interval and Cumulative hold.
  hold_ += alloc_bytes;
  count_ += 1;
  if (min_malloc_sample_interval <= count_) {
    if (hold_ >= CUMULATIVE_SAMPLE_SIZE || max_malloc_sample_interval <= count_) {
      count_ = 0;
      hold_ = 0;
      ret = true;
    }
  }
  return ret;
}

inline bool ObMallocSampleLimiter::malloc_sample_allowed(const int64_t size, const ObMemAttr &attr)
{
  bool ret = false;
  if (ObLightBacktraceGuard::is_enabled()) {
    // light_backtrace can sample all.
    ret = true;
  } else if (OB_UNLIKELY(INTERVAL_UPPER_LIMIT == min_malloc_sample_interval)) {
    // Zero sample mode.
  } else if (OB_UNLIKELY(MUST_SAMPLE_SIZE <= size)) {
    // Full sample when size is bigger than 16M.
    ret = true;
  } else {
    uint64_t hash_val = ob_malloc_sample_hash(attr.label_.str_);
    if (rate_limiters[hash_val & MAX_MALLOC_SAMPLER_NUM].try_acquire(size)) {
      ret = true;
    }
  }
  return ret;
}

inline void ObMallocSampleLimiter::set_interval(int32_t max_interval, int32_t min_interval)
{
#if defined(__x86_64__)
  if (min_interval < 1 || max_interval > INTERVAL_UPPER_LIMIT
      || max_interval < min_interval) {
    _OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "set the min or max malloc times between two samples unexpected,"
                "max_interval=%d, min_interval=%d", max_interval, min_interval);
  } else {
    min_malloc_sample_interval = min_interval;
    max_malloc_sample_interval = max_interval;
    _OB_LOG_RET(INFO, common::OB_SUCCESS, "set the min or max malloc times between two samples succeed,"
                "max_interval=%d, min_interval=%d", max_interval, min_interval);
  }
#else
  UNUSEDx(max_interval, min_interval);
#endif
}

inline int64_t ObMallocSampleKey::hash() const
{
  int64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&ctx_id_, sizeof(ctx_id_), hash_val);
  hash_val = murmurhash(label_, sizeof(label_), hash_val);
  hash_val = murmurhash(bt_, sizeof(bt_), hash_val);
  return hash_val;
}

inline int ObMallocSampleKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

inline bool ObMallocSampleKey::operator==(const ObMallocSampleKey &other) const
{
  bool ret = true;
  if (tenant_id_ != other.tenant_id_ || ctx_id_ != other.ctx_id_
      || 0 != STRNCMP(label_, other.label_, sizeof(label_))
      || 0 != MEMCMP((char*)bt_, (char*)other.bt_, sizeof(bt_))) {
    ret = false;
  }
  return ret;
}

} // end of namespace lib
} // end of namespace oceanbase

#endif
