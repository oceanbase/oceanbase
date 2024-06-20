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

#ifndef OCEANBASE_SHARE_AGGREGATE_HASHMAP_H_
#define OCEANBASE_SHARE_AGGREGATE_HASHMAP_H_

#include <utility>

#include "lib/hash/ob_hashutils.h"
#include "lib/container/ob_fixed_array.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
using namespace common;

template<typename Key, typename Value, typename hasher = hash::hash_func<Key>>
struct HashMap
{
  struct Bucket
  {
    static const uint64_t OCCUPIED_MASK = (1ULL << 63);
    Bucket(): hash_(0), key_(), value_() {}

    bool is_occupied() { return  (hash_ & OCCUPIED_MASK) != 0; }

    void set_hash(uint64_t hash) { hash_ = (hash | OCCUPIED_MASK); }
    void reset() { hash_ = 0; }
    uint64_t hash_;
    Key key_;
    Value value_;
    TO_STRING_KV(K_(hash), K_(key), K_(value), K(is_occupied()));
  };
  HashMap(): buckets_(nullptr), cnt_(0), cap_(0), allocator_(nullptr) {}
  ~HashMap() { destroy(); }
  inline int init(ObIAllocator &allocator, int64_t cap)
  {
    int ret = OB_SUCCESS;
    void *buf = nullptr;
    int64_t buf_size = cap * sizeof(Bucket);
    if (OB_UNLIKELY(cap <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected cap size", K(ret), K(cap));
    } else if (OB_ISNULL(buf = allocator.alloc(buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      MEMSET(buf, 0, buf_size);
      buckets_ = reinterpret_cast<Bucket *>(buf);
      cap_ = cap;
      cnt_ = 0;
      allocator_ = &allocator;
    }
    return ret;
  }
  inline int get(const Key &key, Value *&value)
  {
    int ret = OB_SUCCESS;
    uint64_t hash_val = 0;
    OB_ASSERT(buckets_ != NULL);
    if (OB_UNLIKELY(need_extend())) {
      SQL_LOG(INFO, "extending hashmap", K(cap_));
      if (OB_FAIL(extend())) {
        SQL_LOG(WARN, "extend hash table failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(hasher()(key, hash_val))) {
      SQL_LOG(WARN, "hahs calculation failed", K(ret));
    } else {
      int64_t probe_cnt = 0;
      Bucket *bucket = nullptr;

      if (OB_NOT_NULL(bucket = inner_get(hash_val, key))) {
        if (bucket->is_occupied()) {
          value = &bucket->value_;
        } else {
          bucket->set_hash(hash_val);
          bucket->key_ = key;
          bucket->value_ = Value();
          value = &bucket->value_;
          cnt_++;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected null bucket", K(ret));
      }
    }
    return ret;
  }

  int erase(const Key &key)
  {
    int ret = OB_SUCCESS;
    uint64_t hash_val = 0;
    OB_ASSERT(buckets_ != NULL);
    if (OB_FAIL(hasher()(key, hash_val))) {
      SQL_LOG(WARN, "hash calculation failed", K(ret));
    } else {
      Bucket *bucket = nullptr;
      if (OB_NOT_NULL(bucket = inner_get(hash_val, key))) {
        bucket->reset();
        cnt_--;
      }
    }
    return ret;
  }

  void reuse()
  {
    MEMSET(buckets_, 0, cap_ * sizeof(Bucket));
    cnt_ = 0;
  }

  void destroy()
  {
    buckets_ = nullptr;
    cap_ = 0;
    cnt_ = 0;
    allocator_ = nullptr;
  }
private:
  inline bool need_extend() const { return cnt_ * 2 >= cap_; }
  inline int64_t bucket_idx(const uint64_t hash_val) const
  {
    return (hash_val ^ Bucket::OCCUPIED_MASK) % cap_;
  }
  inline Bucket *inner_get(const uint64_t hash_val, const Key &key)
  {
    int64_t pos = bucket_idx(hash_val);
    int64_t probe_cnt = 0;
    Bucket *ret_bucket = nullptr;
    while (probe_cnt < cap_ && ret_bucket == NULL) {
      Bucket &bucket = buckets_[pos];
      if (bucket.is_occupied() && bucket.key_ == key) {
        ret_bucket = &bucket;
        break;
      } else if (bucket.is_occupied()) {
        pos = (pos + 1) % cap_;
        probe_cnt++;
      } else {
        // find empty slot
        break;
      }
    }
    if (ret_bucket != nullptr) {
      // do nothing
    } else if (!buckets_[pos].is_occupied()) {
      ret_bucket = &buckets_[pos];
    }
    return ret_bucket;
  }

  inline int extend()
  {
    int ret = OB_SUCCESS;
    int64_t new_cap = cap_ * 2, old_cap = cap_;
    void *buf = nullptr;
    int64_t buf_size = new_cap * sizeof(Bucket);
    Bucket *old_buckets = buckets_;
    if (OB_UNLIKELY(new_cap <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid cap size", K(ret), K(new_cap));
    } else if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected null allocator", K(ret));
    } else if (OB_ISNULL(buf = allocator_->alloc(buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      MEMSET(buf, 0, buf_size);
      buckets_ = reinterpret_cast<Bucket *>(buf);
      cap_ = new_cap;
      Bucket *new_bucket = nullptr;
      for (int i = 0; OB_SUCC(ret) && i < old_cap; i++) {
        Bucket &old = old_buckets[i];
        if (!old.is_occupied()) {
          continue;
        } else if (OB_ISNULL(new_bucket = inner_get(old.hash_, old.key_))) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "invalid null new bucket", K(ret));
        } else if (OB_UNLIKELY(new_bucket->is_occupied())) {
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "duplicated element", K(ret));
        } else {
          new_bucket->set_hash(old.hash_);
          new_bucket->key_ = old.key_;
          new_bucket->value_ = old.value_;
        }
      }
    }
    if (OB_FAIL(ret) && buf != nullptr && allocator_ != nullptr) {
      allocator_->free(buf);
      buckets_ = old_buckets;
      cap_ = old_cap;
    }
    return ret;
  }

private:
  // ObFixedArray<Bucket, ObIAllocator> *buckets_;
  Bucket *buckets_;
  int64_t cnt_;
  int64_t cap_;
  ObIAllocator *allocator_;

};
} // aggregate
} // share
} // oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_HASHMAP_H_