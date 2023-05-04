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

#ifndef OB_CUCKOO_HASHMAP_H_
#define OB_CUCKOO_HASHMAP_H_

#include "lib/hash/ob_hashutils.h"

namespace oceanbase
{
namespace common
{

namespace hash
{

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
class ObCuckooHashMap;

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
class ObCuckooHashMapIterator
{
private:
  typedef ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal> HashMap;
  typedef ObCuckooHashMapIterator<_key_type, _value_type, _hashfunc, _equal> HashMapIterator;
  typedef HashMapPair<_key_type, _value_type> pair_type;
  typedef pair_type &reference;
  typedef pair_type *pointer;
public:
  ObCuckooHashMapIterator(const HashMap &map, const int64_t bucket_pos, const int64_t slot_pos, const int64_t slot_count)
    : map_(&map), bucket_pos_(bucket_pos), slot_pos_(slot_pos), slot_count_(slot_count)
  {
  }
  ObCuckooHashMapIterator()
    : map_(nullptr), bucket_pos_(0), slot_pos_(0), slot_count_(0)
  {
  }
  reference operator *() const
  {
    return bucket_pos_ < map_->bucket_num_ ? map_->buckets_[bucket_pos_].slots_[slot_pos_] : map_->overflow_array_[slot_pos_];
  }
  pointer operator ->() const
  {
    return bucket_pos_ < map_->bucket_num_ ? &(map_->buckets_[bucket_pos_].slots_[slot_pos_]) : &(map_->overflow_array_[slot_pos_]);
  }
  bool operator ==(const HashMapIterator &iter) const
  {
    return map_ == iter.map_ && bucket_pos_ == iter.bucket_pos_ && slot_pos_ == iter.slot_pos_;
  }
  bool operator !=(const HashMapIterator &iter) const
  {
    return !operator==(iter);
  }
  HashMapIterator &operator ++()
  {
    if (OB_ISNULL(map_)) {
      OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "hash map must not be NULL", K(lbt()));
    } else if (bucket_pos_ == map_->bucket_num_ && slot_pos_ >= map_->overflow_count_) {
      // do nothing
    } else {
      bool found = false;
      if (bucket_pos_ < map_->bucket_num_) {
        ++slot_pos_;
        for ( ; bucket_pos_ < map_->bucket_num_; ++bucket_pos_) {
          for ( ; slot_pos_ < slot_count_; ++slot_pos_) {
            if (map_->buckets_[bucket_pos_].occupied_[slot_pos_]) {
              found = true;
              break;
            }
          }
          if (!found) {
            slot_pos_ = 0;
          } else {
            break;
          }
        }
      } else if (bucket_pos_ == map_->bucket_num_) {
        if (slot_pos_ < map_->overflow_count_) {
          ++slot_pos_;
        }
      }
    }
    return *this;
  }
  HashMapIterator operator ++(int)
  {
    HashMapIterator iter = *this;
    ++*this;
    return iter;
  }
private:
  const HashMap *map_;
  int64_t bucket_pos_;
  int64_t slot_pos_;
  int64_t slot_count_;
};

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
class ObCuckooHashMapConstIterator
{

private:
  typedef ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal> HashMap;
  typedef ObCuckooHashMapIterator<_key_type, _value_type, _hashfunc, _equal> HashMapIterator;
  typedef ObCuckooHashMapConstIterator<_key_type, _value_type, _hashfunc, _equal> HashMapConstIterator;
  typedef HashMapPair<_key_type, _value_type> pair_type;
  //typedef pair_type &reference;
  //typedef pair_type *pointer;
  typedef const pair_type &const_reference;
  typedef const pair_type *const_pointer;

public:
  ObCuckooHashMapConstIterator(const HashMap &map, const int64_t bucket_pos, const int64_t slot_pos, const int64_t slot_count)
    : map_(&map), bucket_pos_(bucket_pos), slot_pos_(slot_pos), slot_count_(slot_count)
  {
  }
  ObCuckooHashMapConstIterator()
    : map_(nullptr), bucket_pos_(0), slot_pos_(0), slot_count_(0)
  {
  }
  const_reference operator *() const
  {
    return bucket_pos_ < map_->bucket_num_ ? map_->buckets_[bucket_pos_].slots_[slot_pos_] : map_->overflow_array_[slot_pos_];
  }
  const_pointer operator ->() const
  {
    return bucket_pos_ < map_->bucket_num_ ? &(map_->buckets_[bucket_pos_].slots_[slot_pos_]) : &(map_->overflow_array_[slot_pos_]);
  }
  bool operator ==(const HashMapConstIterator &iter) const
  {
    return map_ == iter.map_ && bucket_pos_ == iter.bucket_pos_ && slot_pos_ == iter.slot_pos_;
  }
  bool operator !=(const HashMapConstIterator &iter) const
  {
    return !operator==(iter);
  }
  HashMapConstIterator &operator ++()
  {
    if (OB_ISNULL(map_)) {
      OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "hash map must not be NULL", K(lbt()));
    } else if (bucket_pos_ == map_->bucket_num_ && slot_pos_ >= map_->overflow_count_) {
      // do nothing
    } else {
      bool found = false;
      if (bucket_pos_ < map_->bucket_num_) {
        ++slot_pos_;
        for ( ; bucket_pos_ < map_->bucket_num_; ++bucket_pos_) {
          for ( ; slot_pos_ < slot_count_; ++slot_pos_) {
            if (map_->buckets_[bucket_pos_].occupied_[slot_pos_]) {
              found = true;
              break;
            }
          }
          if (!found) {
            slot_pos_ = 0;
          } else {
            break;
          }
        }
      } else if (bucket_pos_ == map_->bucket_num_) {
        if (slot_pos_ < map_->overflow_count_) {
          ++slot_pos_;
        }
      }
    }
    return *this;
  }
  HashMapConstIterator operator ++(int)
  {
    HashMapConstIterator iter = *this;
    ++*this;
    return iter;
  }
private:
  const HashMap *map_;
  int64_t bucket_pos_;
  int64_t slot_pos_;
  int64_t slot_count_;
};

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
class ObCuckooHashMapIterator;

template <typename _key_type,
          typename _value_type,
          typename _hashfunc = hash_func<_key_type>,
          typename _equal = equal_to<_key_type>>
class ObCuckooHashMap
{
public:
  typedef ObCuckooHashMapIterator<_key_type, _value_type, _hashfunc, _equal> iterator;
  typedef ObCuckooHashMapConstIterator<_key_type, _value_type, _hashfunc, _equal> const_iterator;
  ObCuckooHashMap();
  ~ObCuckooHashMap();
  int create(const int64_t bucket_num, ObIAllocator *allocator);
  int get(const _key_type &key, _value_type &value) const;
  int set(const _key_type &key, const _value_type &value, const bool is_overwrite = false);
  int erase(const _key_type &key, _value_type *value = nullptr);
  void destroy();
  void clear();
  int64_t size() const;
  iterator begin();
  iterator end();
  bool is_inited() const { return is_inited_; }
  const_iterator begin() const;
  const_iterator end() const;
private:
  friend class ObCuckooHashMapIterator<_key_type, _value_type, _hashfunc, _equal>;
  friend class ObCuckooHashMapConstIterator<_key_type, _value_type, _hashfunc, _equal>;
  static const int64_t MAX_CUCKOO_INSERT_DEPTH = 40;
  static const int64_t BUCKET_SLOT_COUNT = 4;
  static const int64_t OVERFLOW_EXPAND_COUNT = 10;
  static const int64_t MIN_REQUIRED_LOAD_FACTOR = 2;
  typedef ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal> self_t;
  typedef HashMapPair<_key_type, _value_type> pair_type;
  struct Bucket{
    Bucket()
    {
      memset(slots_, 0, sizeof(pair_type) * BUCKET_SLOT_COUNT);
      memset(occupied_, 0, sizeof(bool) * BUCKET_SLOT_COUNT);
    }
    pair_type slots_[BUCKET_SLOT_COUNT];
    bool occupied_[BUCKET_SLOT_COUNT];
  };
  struct SlotPos {
    SlotPos()
      : bucket_pos_(-1), slot_pos_(-1)
    {
    }
    bool is_valid() const { return bucket_pos_ >= 0 && slot_pos_ >= 0; }
    void reset()
    {
      bucket_pos_ = -1;
      slot_pos_ = -1;
    }
    int64_t bucket_pos_;
    int64_t slot_pos_;
  };
private:
  uint64_t hash1(const _key_type &key) const;
  uint64_t hash2(const uint64_t hash_val) const;
  int get_impl(const uint64_t hash_val, const bool is_first_hash, const _key_type &key, SlotPos &pos) const;
  int get_slot_pos(const _key_type &key, SlotPos &slot_p) const;
  int get_overflow_slot_pos(const _key_type &key, SlotPos &slot_p) const;
  int copy_pair(const _key_type &key, const _value_type &value, pair_type &pair);
  int erase_impl(const uint64_t hash_val, const bool is_first_hash, const _key_type &key, bool &found, _value_type *value = nullptr);
  int set_impl(const uint64_t hash_val, const bool is_first_hash, const _key_type *pkey, const _value_type *pvalue, const bool is_overwrite, bool &done);
  int quick_set(
      const _key_type *pkey, const _value_type *pvalue, const bool is_overwrite, bool &done);
  int cuckoo_set_impl(
      const uint64_t hash_val, const bool is_first_hash, const bool is_overwrite, const _key_type *pkey, const _value_type *pvalue, pair_type &old_pair, bool &finished);
  int cuckoo_set(
    const _key_type *pkey, const _value_type *pvalue, const bool is_overwrite, pair_type &old_pair, bool &done);
  int overflow_set(const _key_type &key, const _value_type &value, bool &done);
  int rehash();
  int64_t advised_bucket_num(const int64_t elem_count);
private:
  bool is_inited_;
  Bucket *buckets_;
  int64_t bucket_num_;
  int64_t occupied_count_;
  ObRandom random_;
  common::ObIAllocator *allocator_;
  pair_type *overflow_array_;
  int64_t overflow_capacity_;
  int64_t overflow_count_;
  mutable _hashfunc hashfunc_;
  mutable _equal equal_;
};

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::ObCuckooHashMap()
  : is_inited_(false), buckets_(nullptr), bucket_num_(0), occupied_count_(0), random_(), allocator_(nullptr),
    overflow_array_(nullptr), overflow_capacity_(0), overflow_count_(0), hashfunc_(), equal_()
{
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::~ObCuckooHashMap()
{
  destroy();
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
uint64_t ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::hash1(const _key_type &key) const
{
  uint64_t hash_val = 0;
  hashfunc_(key, hash_val);
  return hash_val;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
uint64_t ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::hash2(const uint64_t hash_val1) const
{
  uint32_t hash_val_tmp = static_cast<uint32_t>(hash_val1);
  uint32_t delta = (hash_val_tmp >> 17) | (hash_val_tmp << 15);
  return hash_val1 + delta;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::create(
    const int64_t bucket_num, common::ObIAllocator *allocator)
{
  int ret = common::OB_SUCCESS;
  char *buf = nullptr;
  const int64_t tmp_advised_bucket_num = advised_bucket_num(bucket_num);
  bucket_num_ = std::max(tmp_advised_bucket_num / BUCKET_SLOT_COUNT + 1, 2L);
  bucket_num_ = (bucket_num_ % 2 == 0) ? bucket_num_ : bucket_num_ + 1;
  if (bucket_num_ <= 0 || nullptr == allocator) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(bucket_num), KP(allocator));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(bucket_num_ * (sizeof(Bucket)))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to allocate memory", K(ret));
  } else {
    buckets_ = new (buf) Bucket[bucket_num_];
    allocator_ = allocator;
    is_inited_ = true;
    OB_LOG(DEBUG, "create hash map", K(bucket_num));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != buf) {
      allocator->free(buf);
      buf = nullptr;
      buckets_ = nullptr;
    }
    bucket_num_ = 0;
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
void ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::clear()
{
  if (nullptr != allocator_) {
    if (nullptr != buckets_) {
      for (int64_t i = 0; i < bucket_num_; ++i) {
        Bucket &b = buckets_[i];
        for (int64_t j = 0; j < BUCKET_SLOT_COUNT; ++j) {
          pair_type &pair = b.slots_[i];
          pair.~pair_type();
          b.occupied_[j] = false;
        }
      }
      occupied_count_ = 0;
    }
    if (nullptr != overflow_array_) {
      for (int64_t i = 0; i < overflow_capacity_; ++i) {
        pair_type &pair = overflow_array_[i];
        pair.~pair_type();
      }
      allocator_->free(overflow_array_);
      overflow_array_ = nullptr;
      overflow_capacity_ = 0;
      overflow_count_ = 0;
    }
  }
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
void ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::destroy()
{
  if (nullptr != allocator_) {
    if (nullptr != buckets_) {
      for (int64_t i = 0; i < bucket_num_; ++i) {
        Bucket &b = buckets_[i];
        for (int64_t j = 0; j < BUCKET_SLOT_COUNT; ++j) {
          pair_type &pair = b.slots_[i];
          pair.~pair_type();
        }
      }
      allocator_->free(buckets_);
      buckets_ = nullptr;
      bucket_num_ = 0;
      occupied_count_ = 0;
    }
    if (nullptr != overflow_array_) {
      for (int64_t i = 0; i < overflow_capacity_; ++i) {
        pair_type &pair = overflow_array_[i];
        pair.~pair_type();
      }
      allocator_->free(overflow_array_);
      overflow_array_ = nullptr;
      overflow_capacity_ = 0;
      overflow_count_ = 0;
    }
    allocator_ = nullptr;
    is_inited_ = false;
  }
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::get_impl(
    const uint64_t hash_val, const bool is_first_hash, const _key_type &key, SlotPos &slot_p) const
{
  int ret = common::OB_SUCCESS;
  slot_p.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObCuckooHashMap has not been inited", K(ret));
  } else {
    const int64_t bucket_num = bucket_num_;
    const int64_t bucket_pos = hash_val % (bucket_num / 2) + (is_first_hash ? 0 : bucket_num / 2);
    Bucket &b = buckets_[bucket_pos];
    for (int64_t i = 0; OB_SUCC(ret) && i < BUCKET_SLOT_COUNT; ++i) {
      bool occupied = b.occupied_[i];
      if (occupied) {
        pair_type &pair = b.slots_[i];
        if (equal_(pair.first, key)) {
          slot_p.bucket_pos_ = bucket_pos;
          slot_p.slot_pos_ = i;
          break;
        }
      }
    }
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::erase_impl(
    const uint64_t hash_val, const bool is_first_hash, const _key_type &key, bool &found, _value_type *value)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObCuckooHashMap has not been inited", K(ret));
  } else {
    const int64_t bucket_num = bucket_num_;
    const int64_t bucket_pos = hash_val % (bucket_num / 2) + (is_first_hash ? 0 : bucket_num / 2);
    found = false;
    Bucket &b = buckets_[bucket_pos];
    for (int64_t i = 0; OB_SUCC(ret) && i < BUCKET_SLOT_COUNT && !found; ++i) {
      bool occupied = b.occupied_[i];
      if (occupied) {
        pair_type &pair = b.slots_[i];
        found = equal_(pair.first, key);
        if (found) {
          if (nullptr != value) {
            if (OB_FAIL(copy_assign(*value, pair.second))) {
              OB_LOG(WARN, "fail to copy data", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            OB_LOG(DEBUG, "erase element", "occupied_count", occupied_count_ - 1, K(overflow_count_), K(pair));
            pair.~pair_type();
            new (&pair) pair_type();
            b.occupied_[i] = false;
            --occupied_count_;
          }
        }
      }
    }
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::get_slot_pos(
  const _key_type &key, SlotPos &slot_p) const
{
  int ret = common::OB_SUCCESS;
  int64_t hash_val1 = hash1(key);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObCuckooHashMap has not been inited", K(ret));
  } else if (OB_FAIL(get_impl(hash_val1, true/*first hash*/, key, slot_p))) {
    OB_LOG(WARN, "fail to get value", K(ret));
  } else if (!slot_p.is_valid()) {
    if (OB_FAIL(get_impl(hash2(hash_val1), false/*not first hash*/, key, slot_p))) {
      OB_LOG(WARN, "fail to get value", K(ret));
    }
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::get_overflow_slot_pos(
  const _key_type &key, SlotPos &slot_p) const
{
  int ret = common::OB_SUCCESS;
  slot_p.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < overflow_count_; ++i) {
    if(equal_(overflow_array_[i].first, key)) {
      slot_p.bucket_pos_ = bucket_num_;
      slot_p.slot_pos_ = i;
      break;
    }
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::get(
    const _key_type &key, _value_type &value) const
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObCuckooHashMap has not been inited", K(ret));
  } else {
    SlotPos slot_p;
    if (OB_FAIL(get_slot_pos(key, slot_p))) {
      OB_LOG(WARN, "fail to get slot pos", K(ret));
    } else if (slot_p.is_valid()) {
      Bucket &b = buckets_[slot_p.bucket_pos_];
      pair_type &pair = b.slots_[slot_p.slot_pos_];
      if (OB_FAIL(copy_assign(value, pair.second))) {
        OB_LOG(WARN, "fail to copy assign value", K(ret));
      }
    } else {
      if (OB_FAIL(get_overflow_slot_pos(key, slot_p))) {
        OB_LOG(WARN, "fail to get overflow slot pos", K(ret));
      } else if (slot_p.is_valid()) {
        if (OB_FAIL(copy_assign(value, overflow_array_[slot_p.slot_pos_].second))) {
          OB_LOG(WARN, "fail to copy data", K(ret));
        }
      } else {
        ret = common::OB_HASH_NOT_EXIST;
      }
    }
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::erase(
    const _key_type &key, _value_type *value)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObCuckooHashMap has not been inited", K(ret));
  } else {
    bool found = false;
    uint64_t hash_val1 = hash1(key);
    if (OB_FAIL(erase_impl(hash_val1, true/*first hash*/, key, found, value))) {
      OB_LOG(WARN, "fail to erase", K(ret));
    } else if (!found) {
      uint64_t hash_val2 = hash2(hash_val1);
      if (OB_FAIL(erase_impl(hash_val2, false/*second hash*/, key, found ,value))) {
        OB_LOG(WARN, "fail to erase", K(ret));
      } else if (!found) {
        for (int64_t i = 0; OB_SUCC(ret) && i < overflow_count_ && !found; ++i) {
          pair_type &pair = overflow_array_[i];
          found = equal_(pair.first, key);
          if (found) {
            if (nullptr != value) {
              if (OB_FAIL(copy_assign(*value, pair.second))) {
                OB_LOG(WARN, "fail to copy data", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              pair.~pair_type();
              new (&pair) pair_type();
              OB_LOG(DEBUG, "swap element", K(overflow_array_[overflow_count_ - 1]), K(i));
              if (i != overflow_count_ - 1) {
                if (OB_FAIL(copy_assign(pair, overflow_array_[overflow_count_ - 1]))) {
                  OB_LOG(WARN, "fail to copy assign", K(ret));
                } else {
                  pair_type &tmp = overflow_array_[overflow_count_ - 1];
                  tmp.~pair_type();
                  new (&tmp) pair_type();
                }
              }
              --overflow_count_;
              OB_LOG(DEBUG, "erase element", K(occupied_count_), K(overflow_count_), K(key), K(i));
            }
          }
        }
        if (OB_SUCC(ret) && !found) {
          ret = common::OB_HASH_NOT_EXIST;
        }
      }
    }
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::cuckoo_set_impl(
    const uint64_t hash_val, const bool is_first_hash, const bool is_overwrite, const _key_type *pkey, const _value_type *pvalue, pair_type &old_pair,
    bool &finished)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObCuckooHashMap has not been inited", K(ret));
  } else {
    const int64_t bucket_pos = hash_val % (bucket_num_ / 2) + (is_first_hash ? 0 : bucket_num_ / 2);
    int64_t slot_idx = -1;
    finished = false;
    Bucket &b = buckets_[bucket_pos];
    OB_LOG(DEBUG, "cuckoo set impl", K(*pkey));
    for (int64_t i = 0; OB_SUCC(ret) && i < BUCKET_SLOT_COUNT; ++i) {
      bool occupied = b.occupied_[i];
      if (occupied) {
        if (is_overwrite) {
          pair_type &pair = b.slots_[i];
          bool is_equal = equal_(pair.first, *pkey);
          if (is_equal) {
            slot_idx = i;
            break;
          }
        }
      } else {
        pair_type &pair = b.slots_[i];
        if (OB_FAIL(copy_assign(pair.first, *pkey))) {
          OB_LOG(WARN, "fail to copy data", K(ret));
        } else if (OB_FAIL(copy_assign(pair.second, *pvalue))) {
          OB_LOG(WARN, "fail to copy data", K(ret));
        } else {
          b.occupied_[i] = true;
          finished = true;
          ++occupied_count_;
          OB_LOG(DEBUG, "cuckoo set end", K(pair.first), K(bucket_pos), K(is_first_hash), "slot_idx", i);
          break;
        }
      }
    }
    // all slots are occupied or overwrite is requested
    if (OB_SUCC(ret) && !finished) {
      slot_idx = -1 == slot_idx ? random_.get(0, BUCKET_SLOT_COUNT - 1) : slot_idx;
      pair_type &pair = b.slots_[slot_idx];
      pair_type old_pair_tmp;
      if (OB_FAIL(copy_assign(old_pair_tmp.first, pair.first))) {
        OB_LOG(WARN, "fail to copy data", K(ret));
      } else if (OB_FAIL(copy_assign(old_pair_tmp.second, pair.second))) {
        OB_LOG(WARN, "fail to copy data", K(ret));
      } else {
        pair.~pair_type();
        new (&pair) pair_type();
        if (OB_FAIL(copy_assign(pair.first, *pkey))) {
          OB_LOG(WARN, "fail to copy data", K(ret));
        } else if (OB_FAIL(copy_assign(pair.second, *pvalue))) {
          OB_LOG(WARN, "fail to copy data", K(ret));
        } else if (OB_FAIL(copy_assign(old_pair.first, old_pair_tmp.first))) {
          OB_LOG(WARN, "fail to copy data", K(ret));
        } else if (OB_FAIL(copy_assign(old_pair.second, old_pair_tmp.second))) {
          OB_LOG(WARN, "fail to copy data", K(ret));
        } else {
          b.occupied_[slot_idx] = true;
          OB_LOG(DEBUG, "cuckoo set next key", K(*pkey), K(bucket_pos), K(old_pair.first), K(slot_idx));
        }
      }
    }
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::cuckoo_set(
    const _key_type *pkey, const _value_type *pvalue, const bool is_overwrite, pair_type &old_pair, bool &done)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObCuckooHashMap has not been inited", K(ret));
  } else {
    int64_t cuckoo_step = 0;
    const _key_type *pkey_tmp = pkey;
    const _value_type *pvalue_tmp = pvalue;
    bool is_first_hash = true;
    done = false;
    OB_LOG(DEBUG, "begin cuckoo set", K(*pkey));
    while (OB_SUCC(ret) && !done && cuckoo_step < MAX_CUCKOO_INSERT_DEPTH) {
      int64_t hash_val1 = hash1(*pkey_tmp);
      int64_t hash_val = is_first_hash ? hash_val1 : hash2(hash_val1);
      if (OB_FAIL(cuckoo_set_impl(hash_val, is_first_hash, is_overwrite, pkey_tmp, pvalue_tmp, old_pair, done))) {
        OB_LOG(WARN, "fail to cuckoo set", K(ret));
      } else if (!done) {
        ++cuckoo_step;
        is_first_hash = !is_first_hash;
        pkey_tmp = &old_pair.first;
        pvalue_tmp = &old_pair.second;
        OB_LOG(DEBUG, "cuckset key for next round", K(*pkey_tmp), K(cuckoo_step));
      }
    }
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::set_impl(
    const uint64_t hash_val, const bool is_first_hash, const _key_type *pkey, const _value_type *pvalue, const bool is_overwrite,
    bool &done)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObCuckooHashMap has not been inited", K(ret));
  } else {
    const int64_t bucket_num = bucket_num_;
    const int64_t bucket_pos = hash_val % (bucket_num / 2) + (is_first_hash ? 0 : bucket_num / 2);
    done = false;
    Bucket &b = buckets_[bucket_pos];
    for (int64_t i = 0; OB_SUCC(ret) && i < BUCKET_SLOT_COUNT && !done; ++i) {
      bool occupied = b.occupied_[i];
      if (occupied) {
        pair_type &pair = b.slots_[i];
        STORAGE_LOG(DEBUG, "check key is equal", K(pair.first), K(*pkey));
        bool is_equal = equal_(pair.first, *pkey);
        if (is_equal) {
          if (is_overwrite) {
            pair.second.~_value_type();
            new (&pair.second) _value_type();
            if (OB_FAIL(copy_assign(pair.second, *pvalue))) {
              OB_LOG(WARN, "fail to copy data", K(ret));
            } else {
              done = true;
            }
          } else {
            ret = common::OB_HASH_EXIST;
            done = true;
          }
        }
      } else {
        pair_type &pair = b.slots_[i];
        if (OB_FAIL(copy_assign(pair.first, *pkey))) {
          OB_LOG(WARN, "fail to copy data", K(ret));
        } else if (OB_FAIL(copy_assign(pair.second, *pvalue))) {
          OB_LOG(WARN, "fail to copy data", K(ret));
        } else {
          b.occupied_[i] = true;
          done = true;
          ++occupied_count_;
          OB_LOG(DEBUG, "success to set", K(*pkey), K(*pvalue), K(bucket_pos), K(bucket_num), K(hash_val));
        }
      }
    }
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::quick_set(
    const _key_type *pkey, const _value_type *pvalue, const bool is_overwrite, bool &done)
{
  int ret = OB_SUCCESS;
  done = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObCuckooHashMap has not been inited", K(ret));
  } else {
    uint64_t hash_val1 = hash1(*pkey);
    if (OB_FAIL(set_impl(hash_val1, true/*first hash*/, pkey, pvalue, is_overwrite, done))) {
      OB_LOG(WARN, "fail to set", K(ret));
    } else if (!done) {
      uint64_t hash_val2 = hash2(hash_val1);
      if (OB_FAIL(set_impl(hash_val2, false/*second hash*/, pkey, pvalue, is_overwrite, done))) {
        OB_LOG(WARN, "fail to set", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < overflow_count_ && !done; ++i) {
      done = equal_(*pkey, overflow_array_[i].first);
      if (done) {
        if (is_overwrite) {
          overflow_array_[i].~pair_type();
          new (overflow_array_ + i) pair_type();
          if (OB_FAIL(copy_assign(overflow_array_[i].first, *pkey))) {
            OB_LOG(WARN, "fail to copy data", K(ret));
          } else if (OB_FAIL(copy_assign(overflow_array_[i].second, *pvalue))) {
            OB_LOG(WARN, "fail to copy data", K(ret));
          }
        } else {
          ret = common::OB_HASH_EXIST;
        }
      }
    }
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::rehash()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObCuckooHashMap has not been inited", K(ret));
  } else {
    const int64_t bucket_num = bucket_num_;
    const int64_t new_bucket_num = std::max(11 * bucket_num_ / 10, bucket_num_ + 1) * BUCKET_SLOT_COUNT;
    self_t new_hash_map;
    OB_LOG(DEBUG, "begin rehash", K(occupied_count_), K(new_bucket_num));
    if (OB_FAIL(new_hash_map.create(new_bucket_num, allocator_))) {
      OB_LOG(WARN, "fail to create new hash map", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_num; ++i) {
      Bucket &b = buckets_[i];
      for (int64_t j = 0; OB_SUCC(ret) && j < BUCKET_SLOT_COUNT; ++j) {
        const bool occupied = b.occupied_[j];
        pair_type &pair = b.slots_[j];
        if (occupied) {
          if (OB_FAIL(new_hash_map.set(pair.first, pair.second))) {
            OB_LOG(WARN, "fail to set to new hash map", K(ret));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < overflow_count_; ++i) {
      pair_type &pair = overflow_array_[i];
      if (OB_FAIL(new_hash_map.set(pair.first, pair.second))) {
        OB_LOG(WARN, "fail to set to new hash map", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      std::swap(buckets_, new_hash_map.buckets_);
      std::swap(bucket_num_, new_hash_map.bucket_num_);
      std::swap(occupied_count_, new_hash_map.occupied_count_);
      std::swap(overflow_array_, new_hash_map.overflow_array_);
      std::swap(overflow_capacity_, new_hash_map.overflow_capacity_);
      std::swap(overflow_count_, new_hash_map.overflow_count_);
    }
    OB_LOG(DEBUG, "end rehash", K(new_bucket_num), K(ret), K(occupied_count_), K(overflow_count_));
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::overflow_set(
    const _key_type &key, const _value_type &value, bool &done)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObCuckooHashMap has not been inited", K(ret));
  } else {
    if (overflow_count_ == overflow_capacity_) {
      pair_type *new_overflow_array = nullptr;
      if (OB_ISNULL(new_overflow_array = static_cast<pair_type *>(allocator_->alloc(sizeof(pair_type) * (overflow_capacity_ + OVERFLOW_EXPAND_COUNT))))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to allocate memory", K(ret));
      } else {
        new (new_overflow_array) pair_type[overflow_capacity_ + OVERFLOW_EXPAND_COUNT];
        if (nullptr != overflow_array_) {
          for (int64_t i = 0; OB_SUCC(ret) && i < overflow_count_; ++i) {
            if (OB_FAIL(copy_assign(new_overflow_array[i], overflow_array_[i]))) {
              OB_LOG(WARN, "fail to copy data", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(copy_assign(new_overflow_array[overflow_count_].first, key))) {
            OB_LOG(WARN, "fail to copy data", K(ret));
          } else if (OB_FAIL(copy_assign(new_overflow_array[overflow_count_].second, value))) {
            OB_LOG(WARN, "fail to copy data", K(ret));
          } else {
            ++overflow_count_;
            OB_LOG(DEBUG, "overflow set", K(key), K(overflow_count_), K(occupied_count_));
          }
        }

        if (OB_SUCC(ret) && nullptr != overflow_array_) {
          for (int64_t i = 0; OB_SUCC(ret) && i < overflow_capacity_; ++i) {
            overflow_array_[i].~pair_type();
          }
          allocator_->free(overflow_array_);
        }

        if (OB_SUCC(ret)) {
          overflow_array_ = new_overflow_array;
          overflow_capacity_ = overflow_capacity_ + OVERFLOW_EXPAND_COUNT;
          done = true;
        }
      }
    } else {
      if (OB_FAIL(copy_assign(overflow_array_[overflow_count_].first, key))) {
        OB_LOG(WARN, "fail to copy data", K(ret));
      } else if (OB_FAIL(copy_assign(overflow_array_[overflow_count_].second, value))) {
        OB_LOG(WARN, "fail to copy data", K(ret));
      } else {
        ++overflow_count_;
        OB_LOG(DEBUG, "overflow set", K(key), K(overflow_count_), K(occupied_count_));
        done = true;
      }
    }
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::copy_pair(
    const _key_type &key, const _value_type &value, pair_type &pair)
{
  int ret = common::OB_SUCCESS;
  pair.~pair_type();
  new (&pair) pair_type();
  if (OB_FAIL(copy_assign(pair.first, key))) {
    OB_LOG(WARN, "fail to copy data", K(ret));
  } else if (OB_FAIL(copy_assign(pair.second, value))) {
    OB_LOG(WARN, "fail to copy data", K(ret));
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::set(
    const _key_type &key, const _value_type &value, const bool is_overwrite)
{
  int ret = common::OB_SUCCESS;
  SlotPos slot_p;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObCuckooHashMap has not been inited", K(ret));
  } else if (OB_FAIL(get_slot_pos(key, slot_p))) {
    OB_LOG(WARN, "fail to get slot pos", K(ret));
  } else if (slot_p.is_valid()) {
    if (is_overwrite) {
      Bucket &b = buckets_[slot_p.bucket_pos_];
      pair_type &pair = b.slots_[slot_p.slot_pos_];
      if (OB_FAIL(copy_pair(key, value, pair))) {
        OB_LOG(WARN, "fail to copy pair", K(ret));
      }
    } else {
      ret = common::OB_HASH_EXIST;
    }
  } else {
    if (OB_FAIL(get_overflow_slot_pos(key, slot_p))) {
      OB_LOG(WARN, "fail to get overflow slot pos", K(ret));
    } else {
      if (slot_p.is_valid()) {
        if (is_overwrite) {
          pair_type &pair = overflow_array_[slot_p.slot_pos_];
          if (OB_FAIL(copy_pair(key, value, pair))) {
            OB_LOG(WARN, "fail to copy pair", K(ret));
          }
        } else {
          ret = common::OB_HASH_EXIST;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !slot_p.is_valid()) {
    bool done = false;
    const _key_type *pkey = &key;
    const _value_type *pvalue = &value;
    pair_type old_pair;
    while (OB_SUCC(ret) && !done) {
      if (OB_FAIL(quick_set(pkey, pvalue, is_overwrite, done))) {
        OB_LOG(WARN, "fail to quick set", K(ret));
      } else if (!done) {
        if (OB_FAIL(cuckoo_set(pkey, pvalue, is_overwrite, old_pair, done))) {
          OB_LOG(WARN, "fail to cuckoo set", K(ret));
        } else if (!done) {
          pkey = &old_pair.first;
          pvalue = &old_pair.second;
          if (occupied_count_ > 0 && bucket_num_ * BUCKET_SLOT_COUNT / occupied_count_ >= MIN_REQUIRED_LOAD_FACTOR) {
            OB_LOG(DEBUG, "do not need rehash", K(bucket_num_), K(occupied_count_), K(bucket_num_ * BUCKET_SLOT_COUNT / occupied_count_));
            break;
          } else if (OB_FAIL(rehash())) {
            OB_LOG(WARN, "fail to rehash", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret) && !done) {
      if (OB_FAIL(overflow_set(*pkey, *pvalue, done))) {
        OB_LOG(WARN, "fail to overflow set", K(ret));
      } else if (!done) {
        ret = common::OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "error unexpected, overflow set must be success", K(ret));
      }
    }
  }
  return ret;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int64_t ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::advised_bucket_num(const int64_t elem_count)
{
  return elem_count * 12 / 10;  // advised load factor 0.8
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
int64_t ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::size() const
{
  return occupied_count_ + overflow_count_;
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
ObCuckooHashMapIterator<_key_type, _value_type, _hashfunc, _equal> ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::begin()
{
  int64_t bucket_pos = 0;
  int64_t slot_pos = 0;
  int64_t slot_count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    // do nothing
  } else {
    bool found = false;
    slot_count = BUCKET_SLOT_COUNT;
    for ( ; bucket_pos < bucket_num_; ++bucket_pos) {
      for (slot_pos = 0; slot_pos < slot_count; ++slot_pos) {
        if (buckets_[bucket_pos].occupied_[slot_pos]) {
          found = true;
          break;
        }
      }
      if (found) {
        break;
      }
    }
    if (bucket_pos == bucket_num_) {
      slot_pos = 0;
    }
  }
  return iterator(*this, bucket_pos, slot_pos, slot_count);
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
ObCuckooHashMapIterator<_key_type, _value_type, _hashfunc, _equal> ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::end()
{
  return iterator(*this, bucket_num_, overflow_count_, BUCKET_SLOT_COUNT);
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
ObCuckooHashMapConstIterator<_key_type, _value_type, _hashfunc, _equal> ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::begin() const
{
  int64_t bucket_pos = 0;
  int64_t slot_pos = 0;
  int64_t slot_count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    // do nothing
  } else {
    bool found = false;
    slot_count = BUCKET_SLOT_COUNT;
    for ( ; bucket_pos < bucket_num_; ++bucket_pos) {
      for (slot_pos = 0; slot_pos < slot_count; ++slot_pos) {
        if (buckets_[bucket_pos].occupied_[slot_pos]) {
          found = true;
          break;
        }
      }
      if (found) {
        break;
      }
    }
    if (bucket_pos == bucket_num_) {
      slot_pos = 0;
    }
  }
  return const_iterator(*this, bucket_pos, slot_pos, slot_count);
}

template <typename _key_type,
          typename _value_type,
          typename _hashfunc,
          typename _equal>
ObCuckooHashMapConstIterator<_key_type, _value_type, _hashfunc, _equal> ObCuckooHashMap<_key_type, _value_type, _hashfunc, _equal>::end() const
{
  return const_iterator(*this, bucket_num_, overflow_count_, BUCKET_SLOT_COUNT);
}


}  // end namespace hash
}  // end namespace common
}  // end namespace ocanbase

#endif  // OB_CUCKOO_HASHMAP_H_
