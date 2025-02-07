/**
 * Copyright (c) 2024 OceanBase
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

#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"

namespace oceanbase{
namespace sql {
/**
 *  @brief A simple hash container composed of unique uint64_t keys implemented with opened
 *  addressing. The capacity of the ObSmallHashSet is 2^n.
 *  @tparam  _Accurate  Whether need to seek whole hashset when meeting conflict.
 *  If _Accurate = true, it works as a normal hashset.
 *  If _Accurate = false, it only MAX_SEEK_TIMES when meeting hash confilct. That is to say,
 *  when testing if an element is in the ObSmallHashSet, false positives are possible. It will either
 *  say that an element is definitely not in the set or that it is possible the element is in the
 *  set.
 *  @
 */

template <bool _Accurate>
class ObSmallHashSet
{
  OB_UNIS_VERSION(1);
public:
  using bucket_t = uint64_t;
  ~ObSmallHashSet() {}

  int init(uint64_t capacity, int64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    if (inited_) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "init twice");
    } else {
      alloc_.set_tenant_id(tenant_id);
      alloc_.set_label("ObSmallHashSet");
      if (OB_FAIL(expand(capacity))) {
        COMMON_LOG(WARN, "failed to expand when init");
      } else {
        inited_ = true;
      }
    }
    return ret;
  }

  void clear()
  {
    memset(buckets_, 0, sizeof(bucket_t) * capacity_);
    size_ = 0;
  }

  inline void destroy() {
    inited_ = false;
    buckets_ = nullptr;
    bucket_mask_ = 0;
    capacity_ = 0;
    size_ = 0;
    alloc_.reset();
  }

  inline bool inited() const
  {
    return inited_;
  }

  inline uint64_t size() const
  {
    return size_;
  }

  inline int insert_hash_batch(uint64_t* hashs, uint64_t batch_size)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!inited_)) {
      ret = OB_NOT_INIT;
      COMMON_LOG(ERROR, "not inited");
    }
    for (int64_t i = 0; i < batch_size && OB_SUCC(ret); ++i) {
      ret = insert_hash(hashs[i]);
    }
    return ret;
  }

  inline int insert_hash(uint64_t hash)
  {
    int ret = OB_SUCCESS;
    hash |= KEY_MASK;
    uint64_t offset = hash & bucket_mask_;
    while ((buckets_[offset] != EMPTY_KEY) && (buckets_[offset] != hash)) {
      offset = (++offset) & bucket_mask_;
    }
    if (buckets_[offset] != hash) {
      buckets_[offset] = hash;
      size_++;
      if (size_ * 2 > capacity_ && OB_FAIL(expand(capacity_))) {
        COMMON_LOG(WARN, "failed to expand", K(capacity_));
      }
    }
    return ret;
  }

  int merge(const ObSmallHashSet<_Accurate> &other)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; i < other.capacity_ && OB_SUCC(ret); ++i) {
      if (EMPTY_KEY == other.buckets_[i]) {
      } else {
        ret = insert_hash(other.buckets_[i]);
      }
    }
    return ret;
  }

  inline bool test_hash(uint64_t hash) const
  {
    bool find = false;
    hash |= KEY_MASK;
    uint64_t offset = hash & bucket_mask_;
    uint64_t i = 0;
    for (i = 0; i < capacity_; ++i) {
      if (EMPTY_KEY == buckets_[offset]) {
        break;
      } else if (buckets_[offset] == hash) {
        find = true;
        break;
      } else if (!_Accurate) {
        if (i > MAX_SEEK_TIMES) {
          // no seek more, return true
          find = true;
          break;
        }
      }
      offset = (++offset) & bucket_mask_;
    }
#ifdef unittest
  seek_total_times_ += i;
#endif
    return find;
  }

TO_STRING_KV(K(inited_), K(capacity_), K(size_));

private:
  uint64_t normalize_capacity(uint64_t n)
  {
    return max(MIN_BUCKET_SIZE, next_pow2(2 * n));
  }

  int expand(uint64_t capacity) {
    int ret = OB_SUCCESS;
    uint64_t new_capacity = normalize_capacity(capacity);
    void *buf = nullptr;
    if (OB_ISNULL(buf = alloc_.alloc_aligned(sizeof(bucket_t) * new_capacity, CACHE_LINE_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to allocate bucket memory", K(new_capacity));
    } else {
      bucket_t *old_buckets = buckets_;
      buckets_= static_cast<bucket_t *>(buf);
      uint64_t old_capacity = capacity_;
      capacity_ = new_capacity;
      bucket_mask_ = capacity_ - 1;
      // init new bucket
      memset(buckets_, 0, sizeof(bucket_t) * new_capacity);
      // move data
      for (uint64_t i = 0; i < old_capacity; ++i) {
        uint64_t &hash = old_buckets[i];
        if (hash == EMPTY_KEY) {
          continue;
        }
        uint64_t offset = hash & bucket_mask_;
        while ((buckets_[offset] != EMPTY_KEY) && (buckets_[offset] != hash)) {
          offset = (++offset) & bucket_mask_;
        }
        buckets_[offset] = hash;
      }
    }
    COMMON_LOG(DEBUG, "expand capacity to ", K(capacity_));
    return ret;
  }

private:
  static constexpr uint64_t EMPTY_KEY = 0UL;
  static constexpr uint64_t KEY_MASK = 1UL << 63;
  static constexpr int64_t MIN_BUCKET_SIZE = 128;
  static constexpr int64_t CACHE_LINE_SIZE = 64;
  static constexpr int64_t MAX_SEEK_TIMES = 8;

private:
  bool inited_{false};
  bucket_t *buckets_{nullptr};
  uint64_t bucket_mask_{0};
  uint64_t capacity_{0};
  uint64_t size_{0};
  common::ObArenaAllocator alloc_;
#ifdef unittest
  uint64_t seek_total_times_{0};
#endif
};

template <bool _Accurate>
int ObSmallHashSet<_Accurate>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(UNIS_VERSION);
  if (OB_SUCC(ret)) {
    int64_t size_nbytes = NS_::OB_SERIALIZE_SIZE_NEED_BYTES;
    int64_t pos_bak = (pos += size_nbytes);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialize_(buf, buf_len, pos))) {
        RPC_WARN("serialize fail", K(ret));
      }
    }
    int64_t serial_size = pos - pos_bak;
    int64_t tmp_pos = 0;
    if (OB_SUCC(ret)) {
      CHECK_SERIALIZE_SIZE(CLS, serial_size);
      ret = NS_::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes, size_nbytes, tmp_pos,
                                        serial_size);
    }
  }
  return ret;
}

template <bool _Accurate>
int ObSmallHashSet<_Accurate>::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(capacity_);
  OB_UNIS_ENCODE(size_);
  for (int64_t i = 0; i < capacity_ && OB_SUCC(ret); ++i) {
    if (EMPTY_KEY == buckets_[i]) {
    } else {
      OB_UNIS_ENCODE(buckets_[i]);
    }
  }
  return ret;
}

template <bool _Accurate>
int ObSmallHashSet<_Accurate>::deserialize(const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(version);
    OB_UNIS_DECODE(len);
    CHECK_VERSION_LENGTH(CLS, version, len);
  }
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos))) {
      RPC_WARN("deserialize_ fail", "slen", len, K(pos), K(ret));
    }
    pos = pos_orig + len;
  }
  return ret;
}

template <bool _Accurate>
int ObSmallHashSet<_Accurate>::deserialize_(const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  uint64_t capacity = 0;
  uint64_t size = 0;
  OB_UNIS_DECODE(capacity);
  OB_UNIS_DECODE(size);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init(capacity, ob_get_tenant_id()))) {
    RPC_WARN("failed to init");
  }
  uint64_t hash = 0;
  for (int64_t i = 0; i < size && OB_SUCC(ret); ++i) {
    OB_UNIS_DECODE(hash);
    if (OB_FAIL(insert_hash(hash))) {
      RPC_WARN("failed to insert_hash");
    }
  }
  return ret;
}

template <bool _Accurate>
int64_t ObSmallHashSet<_Accurate>::get_serialize_size() const
{
  int64_t len = get_serialize_size_();
  OB_UNIS_ADD_LEN(UNIS_VERSION);
  len += NS_::OB_SERIALIZE_SIZE_NEED_BYTES;
  return len;
}

template <bool _Accurate>
int64_t ObSmallHashSet<_Accurate>::get_serialize_size_() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(capacity_);
  OB_UNIS_ADD_LEN(size_);
  for (int64_t i = 0; i < capacity_; ++i) {
    if (EMPTY_KEY == buckets_[i]) {
    } else {
      OB_UNIS_ADD_LEN(buckets_[i]);
    }
  }
  return len;
}

} // namespace sql
} // namespace oceanbases
