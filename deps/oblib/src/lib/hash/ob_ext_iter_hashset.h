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

#ifndef OCEANBASE_LIB_HASH_OB_EXT_ITER_HASHSET_H__
#define OCEANBASE_LIB_HASH_OB_EXT_ITER_HASHSET_H__

#include "lib/hash/ob_iteratable_hashset.h"  // ObIteratableHashSet
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace common {
namespace hash {
template <class K, uint64_t N, class Allocator>
class ObExtIterHashSet;

///////////////////////////////////// ObExtIterHashSet Iterator /////////////////////////////////////
template <class HashSet>
class ObExtIterHashSetConstIterator {
public:
  typedef typename HashSet::const_key_ref_t const_key_ref_t;
  typedef ObExtIterHashSetConstIterator<HashSet> SelfType;
  typedef typename HashSet::HashSetBucket BucketType;
  typedef typename HashSet::InnerHashSetIter InnerIter;

public:
  ObExtIterHashSetConstIterator(const HashSet* set, const BucketType* bucket, const InnerIter& iter)
      : hash_set_(set), bucket_(bucket), iter_(iter)
  {}

  ObExtIterHashSetConstIterator(const SelfType& other)
      : hash_set_(other.hash_set_), bucket_(other.bucket_), iter_(other.iter_)
  {}

  ~ObExtIterHashSetConstIterator()
  {}

public:
  ObExtIterHashSetConstIterator& operator=(const SelfType& other)
  {
    if (this != &other) {
      hash_set_ = other.hash_set_;
      bucket_ = other.bucket_;
      iter_ = other.iter_;
    }

    return *this;
  }
  ObExtIterHashSetConstIterator& operator=(SelfType&& other) = default;

  bool operator==(const SelfType& other) const
  {
    return other.hash_set_ == hash_set_ && other.bucket_ == bucket_ && other.iter_ == iter_;
  }

  bool operator!=(const SelfType& other) const
  {
    return other.hash_set_ != hash_set_ || other.bucket_ != bucket_ || other.iter_ != iter_;
  }

  SelfType& operator++()
  {
    if (NULL == hash_set_ || NULL == bucket_) {
      LIB_LOG(ERROR, "err hash set, iter or bucket", K(hash_set_), K(bucket_));
    } else {
      // if current bucket is not null, fetch next one.
      if (iter_ != bucket_->hash_set_.end()) {
        ++iter_;
      }

      // if reach end of current bucket, step to the next bucket.
      if (iter_ == bucket_->hash_set_.end() && NULL != bucket_->next_) {
        bucket_ = bucket_->next_;
        iter_ = bucket_->hash_set_.begin();
      }
    }

    return *this;
  }

  const_key_ref_t operator*() const
  {
    if (NULL == hash_set_ || NULL == bucket_) {
      LIB_LOG(ERROR, "err hash set, iter or bucket", K(hash_set_), K(bucket_));
    }
    return *iter_;
  }

private:
  const HashSet* hash_set_;
  const BucketType* bucket_;
  InnerIter iter_;
};

///////////////////////////////////// ObExtIterHashSet /////////////////////////////////////

template <class K, uint64_t N = 1031, class Allocator = ObIAllocator>
class ObExtIterHashSet {
public:
  typedef const K& const_key_ref_t;
  typedef ObIteratableHashSet<K, N> BaseHashSet;
  typedef ObExtIterHashSet<K, N, Allocator> SelfType;
  typedef ObExtIterHashSetConstIterator<SelfType> const_iterator_t;
  typedef typename BaseHashSet::const_iterator_t InnerHashSetIter;

  struct HashSetBucket {
    BaseHashSet hash_set_;
    HashSetBucket* next_;

    HashSetBucket() : hash_set_(), next_(NULL)
    {}
    ~HashSetBucket()
    {}
    void reset()
    {
      hash_set_.reset();
      next_ = NULL;
    }
  };

public:
  explicit ObExtIterHashSet(Allocator& allocator);
  virtual ~ObExtIterHashSet();

public:
  /**
   * @retval OB_SUCCESS       insert successfully
   * @retval OB_HASH_EXIST    key exists
   * @retval other ret        failed
   */
  int set_refactored(const K& key);
  /**
   * @retval OB_HASH_EXIST     key exists
   * @retval OB_HASH_NOT_EXIST key not exists
   */
  int exist_refactored(const K& key) const
  {
    return is_exist_(key) ? OB_HASH_EXIST : OB_HASH_NOT_EXIST;
  }

  void reset();
  void clear()
  {
    reset();
  }

  const_iterator_t begin() const
  {
    return const_iterator_t(this, &buckets_, buckets_.hash_set_.begin());
  }

  const_iterator_t end() const
  {
    return const_iterator_t(this, buckets_tail_, buckets_tail_->hash_set_.end());
  }

  int64_t count() const
  {
    return count_;
  }

public:
  DECLARE_TO_STRING;

private:
  bool is_exist_(const K& key) const;
  bool is_full_() const
  {
    return count_ >= bucket_num_ * static_cast<int64_t>(N);
  }
  int add_bucket_();

private:
  int64_t count_;                // count of object
  int64_t bucket_num_;           // count of bucket
  Allocator& allocator_;         // allocator
  HashSetBucket buckets_;        // bucket lists
  HashSetBucket* buckets_tail_;  // bucket lists tail

private:
  template <class HashSet>
  friend class ObExtIterHashSetConstIterator;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExtIterHashSet);
};

template <class K, uint64_t N, class Allocator>
ObExtIterHashSet<K, N, Allocator>::ObExtIterHashSet(Allocator& allocator)
    : count_(0), bucket_num_(1), allocator_(allocator), buckets_(), buckets_tail_(&buckets_)
{}

template <class K, uint64_t N, class Allocator>
ObExtIterHashSet<K, N, Allocator>::~ObExtIterHashSet()
{
  reset();
}

template <class K, uint64_t N, class Allocator>
int ObExtIterHashSet<K, N, Allocator>::set_refactored(const K& key)
{
  int ret = OB_SUCCESS;
  bool exist = is_exist_(key);

  if (exist) {
    ret = OB_HASH_EXIST;
  } else if (is_full_() && OB_FAIL(add_bucket_())) {
    LIB_LOG(WARN, "add_bucket_ fail", K(ret));
  } else {
    // keep inserting successful
    if (NULL == buckets_tail_) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "err bucket tail", K(ret));
    } else if (OB_FAIL(buckets_tail_->hash_set_.set_refactored(key))) {
      LIB_LOG(WARN, "set key into hash set fail", K(ret));
    } else {
      count_++;
    }
  }

  return ret;
}

template <class K, uint64_t N, class Allocator>
bool ObExtIterHashSet<K, N, Allocator>::is_exist_(const K& key) const
{
  bool exist = false;
  const HashSetBucket* item = &buckets_;

  while (!exist && NULL != item) {
    if (OB_HASH_EXIST == item->hash_set_.exist_refactored(key)) {
      exist = true;
      break;
    } else {
      item = item->next_;
    }
  }

  return exist;
}

template <class K, uint64_t N, class Allocator>
int ObExtIterHashSet<K, N, Allocator>::add_bucket_()
{
  int ret = OB_SUCCESS;
  HashSetBucket* bucket = (HashSetBucket*)allocator_.alloc(sizeof(HashSetBucket));
  if (NULL == bucket) {
    LIB_LOG(WARN, "allocate memory for bucket fail", "bucket_size", sizeof(HashSetBucket));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    new (bucket) HashSetBucket();

    bucket->next_ = NULL;
    buckets_tail_->next_ = bucket;
    buckets_tail_ = bucket;

    bucket_num_++;

    LIB_LOG(DEBUG, "add bucket", K(bucket_num_), K(count_), K(bucket));
  }
  return ret;
}

template <class K, uint64_t N, class Allocator>
void ObExtIterHashSet<K, N, Allocator>::reset()
{
  HashSetBucket* bucket = buckets_.next_;

  while (NULL != bucket) {
    HashSetBucket* next = bucket->next_;
    bucket->~HashSetBucket();
    allocator_.free((void*)bucket);

    bucket = next;
  }
  bucket = NULL;

  buckets_.reset();
  buckets_tail_ = &buckets_;
  bucket_num_ = 1;
  count_ = 0;
}

template <class K, uint64_t N, class Allocator>
int64_t ObExtIterHashSet<K, N, Allocator>::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_ARRAY_START();
  const_iterator_t beg = begin();
  for (const_iterator_t it = beg; it != end(); ++it) {
    if (it != beg) {
      J_COMMA();
    }
    BUF_PRINTO(*it);
  }
  J_ARRAY_END();
  return pos;
}
}  // namespace hash
}  // namespace common
}  // namespace oceanbase
#endif
