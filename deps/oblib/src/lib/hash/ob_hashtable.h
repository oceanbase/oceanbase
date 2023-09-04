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

#ifndef  OCEANBASE_COMMON_HASH_HASHTABLE_
#define  OCEANBASE_COMMON_HASH_HASHTABLE_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include "lib/atomic/ob_atomic.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/hash/ob_serialization.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/utility.h"
#include "lib/hash/mprotect.h"
namespace oceanbase
{
namespace common
{
namespace hash
{

template <class _key_type,
          class _value_type,
          class _hashfunc,
          class _equal,
          class _getkey,
          class _allocer,
          class _defendmode,
          template <class> class _bucket_array,
          class _bucket_allocer,
          int64_t EXTEND_RATIO>
class ObHashTable;

template <class _key_type,
          class _value_type,
          class _hashfunc,
          class _equal,
          class _getkey,
          class _allocer,
          class _defendmode,
          template <class> class _bucket_array,
          class _bucket_allocer,
          int64_t EXTEND_RATIO>
class ObHashTableIterator;

template <class _key_type,
          class _value_type,
          class _hashfunc,
          class _equal,
          class _getkey,
          class _allocer,
          class _defendmode,
          template <class> class _bucket_array,
          class _bucket_allocer,
          int64_t EXTEND_RATIO>
class ObHashTableConstIterator;

template <class _value_type,
          class _lock_type,
          class _cond_type>
class ObHashTableBucketNodeConstIterator;

template<class _value_type>
struct ObHashTableNode
{
  static constexpr uint64_t MAGIC = 0x486173684e6f6465; // hex of "HashNode"
  bool check_magic_code() { return MAGIC == magic_; }
  uint64_t magic_;
  ObHashTableNode *next;
  _value_type data;
  volatile bool is_fake;
};

template <class _value_type,
          class _lock_type,
          class _cond_type>
class ObHashTableBucketNodeIterator
{
private:
  typedef ObHashTableNode<_value_type> hashnode;
  typedef ObHashTableBucketNodeIterator<_value_type,
                                        _lock_type, _cond_type> iterator;
  typedef ObHashTableBucketNodeConstIterator<_value_type,
                                             _lock_type, _cond_type> const_iterator;
  typedef _value_type &reference;
  typedef _value_type *pointer;
  friend class ObHashTableBucketNodeConstIterator<_value_type, _lock_type, _cond_type>;
public:
  ObHashTableBucketNodeIterator() : node_(0)
  {
  }

  ObHashTableBucketNodeIterator(const iterator &other) :
      node_(other.node_)
  {
  }

  ObHashTableBucketNodeIterator(
                      hashnode *node) :
      node_(node)
  {
  }

  reference operator *() const
  {
    return node_->data;
  }

  pointer operator ->() const
  {
    _value_type *p = NULL;
    if (OB_ISNULL(node_)) {
      HASH_WRITE_LOG_RET(HASH_FATAL, OB_ERR_UNEXPECTED, "node is null, backtrace=%s", lbt());
    } else {
      p = &(node_->data);
    }
    return p;
  }

  bool operator ==(const iterator &iter) const
  {
    return node_ == iter.node_;
  }

  bool operator !=(const iterator &iter) const
  {
    return node_ != iter.node_;
  }

  iterator &operator ++()
  {
    if (NULL != node_) {
      node_ = node_->next;
    }
    return *this;
  }

  iterator operator ++(int)
  {
    iterator iter = *this;
    ++*this;
    return iter;
  }

private:
  hashnode *node_;
};

template <class _value_type,
          class _lock_type,
          class _cond_type>
class ObHashTableBucketNodeConstIterator
{
private:
  typedef ObHashTableNode<_value_type> hashnode;
  typedef ObHashTableBucketNodeIterator<_value_type,
                                        _lock_type, _cond_type> iterator;
  typedef ObHashTableBucketNodeConstIterator<_value_type,
                                        _lock_type, _cond_type> const_iterator;
  typedef const _value_type &const_reference;
  typedef const _value_type *const_pointer;
  friend class ObHashTableBucketNodeIterator<_value_type, _lock_type, _cond_type>;
public:
  ObHashTableBucketNodeConstIterator() : node_(0)
  {
  }

  ObHashTableBucketNodeConstIterator(const const_iterator &other) :
      node_(other.node_)
  {
  }

  ObHashTableBucketNodeConstIterator(const iterator &other) :
      node_(other.node_)
  {
  }

  ObHashTableBucketNodeConstIterator(
                      hashnode *node) :
      node_(node)
  {
  }

  const_reference operator *() const
  {
    return node_->data;
  }

  const_pointer operator ->() const
  {
    _value_type *p = NULL;
    if (OB_ISNULL(node_)) {
      HASH_WRITE_LOG_RET(HASH_FATAL, OB_ERR_UNEXPECTED, "node is null, backtrace=%s", lbt());
    } else {
      p = &(node_->data);
    }
    return p;
  }

  bool operator ==(const const_iterator &iter) const
  {
    return node_ == iter.node_;
  }

  bool operator !=(const const_iterator &iter) const
  {
    return node_ != iter.node_;
  }

  const_iterator &operator ++()
  {
    if (NULL != node_) {
      node_ = node_->next;
    }
    return *this;
  }

  const_iterator operator ++(int)
  {
    const_iterator iter = *this;
    ++*this;
    return iter;
  }

private:
  hashnode *node_;
};

template<class _value_type, class _lock_type, class _cond_type = NCond>
struct ObHashTableBucket : public MProtect
{
  mutable _lock_type lock;
  mutable _cond_type cond;
  ObHashTableNode<_value_type> *node;
  typedef ObHashTableBucketNodeIterator<_value_type, _lock_type, _cond_type> iterator;
  typedef ObHashTableBucketNodeConstIterator<_value_type, _lock_type, _cond_type> const_iterator;
  iterator node_begin()
  {
    return iterator(node);
  }
  const_iterator node_begin() const
  {
    return const_iterator(node);
  }
  iterator node_end()
  {
    return iterator(nullptr);
  }
  const_iterator node_end() const
  {
    return const_iterator(nullptr);
  }
};

template<class _value_type>
struct ObHashTableBucket<_value_type,
                         NoPthreadDefendMode::lock_type,
                         NoPthreadDefendMode::cond_type> : public MProtect
{
  ObHashTableNode<_value_type> *node;
};

template<class _value_type, class _defendmode>
struct ObBucketLockCond
{
  typedef ObHashTableBucket<_value_type,
                            typename _defendmode::lock_type,
                            typename _defendmode::cond_type> bucket_type;
  explicit ObBucketLockCond(bucket_type &bucket)
    : bucket_(bucket)
  {
  }
  typename _defendmode::lock_type &lock()
  {
    return bucket_.lock;
  }
  typename _defendmode::cond_type &cond()
  {
    return bucket_.cond;
  }
private:
  bucket_type &bucket_;
};

template<class _value_type>
struct ObBucketLockCond<_value_type, NoPthreadDefendMode>
{
  typedef ObHashTableBucket<_value_type,
                            NoPthreadDefendMode::lock_type,
                            NoPthreadDefendMode::cond_type> bucket_type;
  explicit ObBucketLockCond(bucket_type &bucket)
  {
    UNUSED(bucket);
  }
  NoPthreadDefendMode::lock_type &lock()
  {
    return lock_;
  }
  NoPthreadDefendMode::cond_type &cond()
  {
    return cond_;
  }
private:
  NoPthreadDefendMode::lock_type lock_;
  NoPthreadDefendMode::cond_type cond_;
};

template <class _key_type,
          class _value_type,
          class _hashfunc,
          class _equal,
          class _getkey,
          class _allocer,
          class _defendmode,
          template <class> class _bucket_array,
          class _bucket_allocer,
          int64_t EXTEND_RATIO>
class ObHashTableBucketIterator
{
private:
  typedef ObHashTable<_key_type,
                      _value_type,
                      _hashfunc,
                      _equal,
                      _getkey,
                      _allocer,
                      _defendmode,
                      _bucket_array,
                      _bucket_allocer,
                      EXTEND_RATIO> hashtable;
  typedef ObHashTableBucketIterator<_key_type,
                              _value_type,
                              _hashfunc,
                              _equal,
                              _getkey,
                              _allocer,
                              _defendmode,
                              _bucket_array,
                              _bucket_allocer,
                              EXTEND_RATIO> iterator;
  typedef typename hashtable::hashbucket &reference;
  typedef typename hashtable::hashbucket *pointer;
public:
  ObHashTableBucketIterator() : ht_(NULL), bucket_pos_(-1), bucket_(NULL)
  {
  }

  ObHashTableBucketIterator(const iterator &other) :
    ht_(other.ht_), bucket_pos_(other.bucket_pos_), bucket_(other.bucket_)
  {
  }

  ObHashTableBucketIterator(const hashtable *ht, int64_t bucket_pos, pointer bucket) :
    ht_(ht), bucket_pos_(bucket_pos), bucket_(bucket)
  {
  }

  reference operator *() const
  {
    return *bucket_;
  }

  pointer operator ->() const
  {
    return bucket_;
  }

  bool operator ==(const iterator &iter) const
  {
    return bucket_ == iter.bucket_;
  }

  bool operator !=(const iterator &iter) const
  {
    return bucket_ != iter.bucket_;
  }

  iterator &operator ++()
  {
    if (OB_ISNULL(ht_)) {
      HASH_WRITE_LOG_RET(HASH_FATAL, OB_ERR_UNEXPECTED, "node is null, backtrace=%s", lbt());
    } else if (++bucket_pos_ >= ht_->bucket_num_) {
      bucket_ = nullptr;
    } else {
      bucket_ = &ht_->buckets_[bucket_pos_];
    }
    return *this;
  }

  iterator operator ++(int)
  {
    iterator iter = *this;
    ++*this;
    return iter;
  }

private:
  const hashtable *ht_;
  int64_t bucket_pos_;
  pointer bucket_;
};

template <class _key_type,
          class _value_type,
          class _hashfunc,
          class _equal,
          class _getkey,
          class _allocer,
          class _defendmode,
          template <class> class _bucket_array,
          class _bucket_allocer,
          int64_t EXTEND_RATIO>
class ObHashTableIterator
{
private:
  typedef ObHashTableNode<_value_type> hashnode;
  typedef ObHashTable<_key_type,
                      _value_type,
                      _hashfunc,
                      _equal,
                      _getkey,
                      _allocer,
                      _defendmode,
                      _bucket_array,
                      _bucket_allocer,
                      EXTEND_RATIO> hashtable;
  typedef ObHashTableIterator<_key_type,
                              _value_type,
                              _hashfunc,
                              _equal,
                              _getkey,
                              _allocer,
                              _defendmode,
                              _bucket_array,
                              _bucket_allocer,
                              EXTEND_RATIO> iterator;
  typedef ObHashTableConstIterator<_key_type,
                                   _value_type,
                                   _hashfunc,
                                   _equal,
                                   _getkey,
                                   _allocer,
                                   _defendmode,
                                   _bucket_array,
                                   _bucket_allocer,
                                   EXTEND_RATIO> const_iterator;
  typedef _value_type &reference;
  typedef _value_type *pointer;
  friend class ObHashTableConstIterator<_key_type,
                                        _value_type,
                                        _hashfunc,
                                        _equal,
                                        _getkey,
                                        _allocer,
                                        _defendmode,
                                        _bucket_array,
                                        _bucket_allocer,
                                        EXTEND_RATIO>;
public:
  ObHashTableIterator() : ht_(NULL), bucket_pos_(0), node_(0)
  {
  }

  ObHashTableIterator(const iterator &other) :
      ht_(other.ht_), bucket_pos_(other.bucket_pos_), node_(other.node_)
  {
  }

  ObHashTableIterator(const hashtable *ht, int64_t bucket_pos,
                      hashnode *node) :
      ht_(ht), bucket_pos_(bucket_pos), node_(node)
  {
  }

  reference operator *() const
  {
    return node_->data;
  }

  pointer operator ->() const
  {
    _value_type *p = NULL;
    if (OB_ISNULL(node_)) {
      HASH_WRITE_LOG_RET(HASH_FATAL, OB_ERR_UNEXPECTED, "node is null, backtrace=%s", lbt());
    } else {
      p = &(node_->data);
    }
    return p;
  }

  bool operator ==(const iterator &iter) const
  {
    return node_ == iter.node_;
  }

  bool operator !=(const iterator &iter) const
  {
    return node_ != iter.node_;
  }

  iterator &operator ++()
  {
    if (OB_ISNULL(ht_)) {
      HASH_WRITE_LOG_RET(HASH_FATAL, OB_ERR_UNEXPECTED, "node is null, backtrace=%s", lbt());
    } else if (NULL != node_ && NULL != (node_ = node_->next)) {
      // do nothing
    } else {
      for (int64_t i = bucket_pos_ + 1; i < ht_->bucket_num_; i++) {
        if (NULL != (node_ = ht_->buckets_[i].node)) {
          bucket_pos_ = i;
          break;
        }
      }
      if (NULL == node_) {
        bucket_pos_ = ht_->bucket_num_;
      }
    }
    return *this;
  }

  iterator operator ++(int)
  {
    iterator iter = *this;
    ++*this;
    return iter;
  }

private:
  const hashtable *ht_;
  int64_t bucket_pos_;
  hashnode *node_;
};

template <class _key_type,
          class _value_type,
          class _hashfunc,
          class _equal,
          class _getkey,
          class _allocer,
          class _defendmode,
          template <class> class _bucket_array,
          class _bucket_allocer,
          int64_t EXTEND_RATIO>
class ObHashTableConstIterator
{
private:
  typedef ObHashTableNode<_value_type> hashnode;
  typedef ObHashTable<_key_type,
                      _value_type,
                      _hashfunc,
                      _equal,
                      _getkey,
                      _allocer,
                      _defendmode,
                      _bucket_array,
                      _bucket_allocer,
                      EXTEND_RATIO> hashtable;
  typedef ObHashTableIterator<_key_type,
                              _value_type,
                              _hashfunc,
                              _equal,
                              _getkey,
                              _allocer,
                              _defendmode,
                              _bucket_array,
                              _bucket_allocer,
                              EXTEND_RATIO> iterator;
  typedef ObHashTableConstIterator<_key_type,
                                   _value_type,
                                   _hashfunc,
                                   _equal,
                                   _getkey,
                                   _allocer,
                                   _defendmode,
                                   _bucket_array,
                                   _bucket_allocer,
                                   EXTEND_RATIO> const_iterator;
  typedef const _value_type &const_reference;
  typedef const _value_type *const_pointer;
  friend class ObHashTableIterator<_key_type,
                                   _value_type,
                                   _hashfunc,
                                   _equal,
                                   _getkey,
                                   _allocer,
                                   _defendmode,
                                   _bucket_array,
                                   _bucket_allocer,
                                   EXTEND_RATIO>;
public:
  ObHashTableConstIterator() :
      ht_(NULL), bucket_pos_(0), node_(0)
  {
  }

  ObHashTableConstIterator(const const_iterator &other) :
      ht_(other.ht_), bucket_pos_(other.bucket_pos_), node_(other.node_)
  {
  }

  ObHashTableConstIterator(const iterator &other) :
      ht_(other.ht_), bucket_pos_(other.bucket_pos_), node_(other.node_)
  {
  }

  ObHashTableConstIterator(const hashtable *ht, int64_t bucket_pos,
                           hashnode *node) :
      ht_(ht), bucket_pos_(bucket_pos), node_(node)
  {
  }

  const_reference operator *() const
  {
    return node_->data;
  }

  const_pointer operator ->() const
  {
    return &(node_->data);
  }

  bool operator ==(const const_iterator &iter) const
  {
    return node_ == iter.node_;
  }

  bool operator !=(const const_iterator &iter) const
  {
    return node_ != iter.node_;
  }

  const_iterator &operator ++()
  {
    if (OB_ISNULL(ht_)) {
      HASH_WRITE_LOG_RET(HASH_FATAL, OB_ERR_UNEXPECTED, "ht_ is null, backtrace=%s", lbt());
    } else if (NULL != node_ && NULL != (node_ = node_->next)) {
      // do nothing
    } else {
      for (int64_t i = bucket_pos_ + 1; i < ht_->bucket_num_; i++) {
        if (NULL != (node_ = ht_->buckets_[i].node)) {
          bucket_pos_ = i;
          break;
        }
      }
      if (NULL == node_) {
        bucket_pos_ = ht_->bucket_num_;
      }
    }
    return *this;
  }

  const_iterator operator ++(int)
  {
    const_iterator iter = *this;
    ++*this;
    return iter;
  }

private:
  const hashtable *ht_;
  int64_t bucket_pos_;
  hashnode *node_;
};

template<class _value_type>
struct HashTableTypes
{
  typedef ObHashTableNode<_value_type> AllocType;
};

template <class _key_type,
          class _value_type,
          class _hashfunc,
          class _equal,
          class _getkey,     // function of getting key with value
          class _allocer,
          class _defendmode, // multi-thread protection mode
          template <class> class _bucket_array,
          class _bucket_allocer = oceanbase::common::ObMalloc,
          int64_t EXTEND_RATIO = 1>
class ObHashTable
{
public:
  typedef ObHashTableBucketIterator<_key_type,
                              _value_type,
                              _hashfunc,
                              _equal,
                              _getkey,
                              _allocer,
                              _defendmode,
                              _bucket_array,
                              _bucket_allocer,
                              EXTEND_RATIO> bucket_iterator;
  typedef ObHashTableIterator<_key_type,
                              _value_type,
                              _hashfunc,
                              _equal,
                              _getkey,
                              _allocer,
                              _defendmode,
                              _bucket_array,
                              _bucket_allocer,
                              EXTEND_RATIO> iterator;
  typedef ObHashTableConstIterator<_key_type,
                                   _value_type,
                                   _hashfunc,
                                   _equal,
                                   _getkey,
                                   _allocer,
                                   _defendmode,
                                   _bucket_array,
                                   _bucket_allocer,
                                   EXTEND_RATIO> const_iterator;
  typedef typename _defendmode::lock_type lock_type;
  typedef typename _defendmode::cond_type cond_type;
  typedef ObHashTableBucket<_value_type,
                            lock_type,
                            cond_type> hashbucket;
  typedef ObHashTableNode<_value_type> hashnode;
  typedef ObBucketLockCond<_value_type, _defendmode> bucket_lock_cond;
  typedef typename _defendmode::readlocker readlocker;
  typedef typename _defendmode::writelocker writelocker;
private:
  // ARRAY_SIZE * hashbucket is approximately equal to 1M, must be 2 << N.
  static const int64_t ARRAY_SIZE = 1024 * 16;

  typedef typename _defendmode::cond_waiter cond_waiter;
  typedef typename _defendmode::cond_broadcaster cond_broadcaster;
  typedef ObHashTable<_key_type,
                      _value_type,
                      _hashfunc,
                      _equal,
                      _getkey,
                      _allocer,
                      _defendmode,
                      _bucket_array,
                      _bucket_allocer,
                      EXTEND_RATIO> hashtable;
  typedef pre_proc<_value_type> preproc;
  typedef typename _bucket_array<hashbucket>::array_type bucket_array;
  friend class ObHashTableBucketIterator<_key_type,
                                   _value_type,
                                   _hashfunc,
                                   _equal,
                                   _getkey,
                                   _allocer,
                                   _defendmode,
                                   _bucket_array,
                                   _bucket_allocer,
                                   EXTEND_RATIO>;
  friend class ObHashTableIterator<_key_type,
                                   _value_type,
                                   _hashfunc,
                                   _equal,
                                   _getkey,
                                   _allocer,
                                   _defendmode,
                                   _bucket_array,
                                   _bucket_allocer,
                                   EXTEND_RATIO>;
  friend class ObHashTableConstIterator<_key_type,
                                        _value_type,
                                        _hashfunc,
                                        _equal,
                                        _getkey,
                                        _allocer,
                                        _defendmode,
                                        _bucket_array,
                                        _bucket_allocer,
                                        EXTEND_RATIO>;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHashTable);

public:
  ObHashTable() : default_bucket_allocer_(ObModIds::OB_HASH_BUCKET),
                  allocer_(NULL),
                  bucket_allocer_(&default_bucket_allocer_),
                  bucket_num_(0),
                  size_(0)
  {
    static_assert(((EXTEND_RATIO <= 1)
                   || (EXTEND_RATIO > 1 && std::is_same<_defendmode, NoPthreadDefendMode>::value)),
                  "only NoPthreadDefendMode can use extendable hashtable");
    construct(buckets_);
  }

  ~ObHashTable()
  {
    if (inited(buckets_) && NULL != allocer_) {
      destroy();
    }
  }
public:
  inline bool created() const
  {
    return inited(buckets_);
  }
  int create(int64_t bucket_num, _allocer *allocer, _bucket_allocer *bucket_allocer)
  {
    int ret = OB_SUCCESS;;
    if (OB_UNLIKELY(0 >= bucket_num) || OB_UNLIKELY(NULL == allocer)) {
      HASH_WRITE_LOG(HASH_WARNING, "invalid param bucket_num=%ld allocer=%p", bucket_num, allocer);
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_UNLIKELY(inited(buckets_))) {
      HASH_WRITE_LOG(HASH_WARNING, "hashtable has already been created allocer=%p bucket_num=%ld",
                     allocer_, bucket_num_);
      ret = OB_INIT_TWICE;
    } else if (OB_UNLIKELY(0 != (ret = hash::create(buckets_, bucket_num, ARRAY_SIZE,
                                                    sizeof(hashbucket), *bucket_allocer)))) {
      HASH_WRITE_LOG(HASH_WARNING, "create buckets fail, ret=%d", ret);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      //memset(buckets_, 0, sizeof(hashbucket) * bucket_num);
      bucket_num_ = bucket_num;
      allocer_ = allocer;
      bucket_allocer_ = bucket_allocer;
    }
    return ret;
  }

  int destroy()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG(HASH_DEBUG, "hashtable is empty");
    } else {
      for (int64_t i = 0; i < bucket_num_; i++) {
        hashnode *cur_node = NULL;
        if (NULL != (cur_node = buckets_[i].node)) {
          while (NULL != cur_node) {
            abort_unless(cur_node->check_magic_code());
            hashnode *tmp_node = cur_node->next;
            allocer_->free(cur_node);
            cur_node = tmp_node;
          }
          buckets_[i].node = NULL;
        }
      }
      allocer_ = NULL;
      //delete[] buckets_;
      //buckets_ = NULL;
      hash::destroy(buckets_, *bucket_allocer_);
      bucket_num_ = 0;
      size_ = 0;
    }
    return ret;
  }

  int clear()
  {
    int ret = OB_SUCCESS;;
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      ret = OB_NOT_INIT;
    } else {
      for (int64_t i = 0; i < bucket_num_; i++) {
        bucket_lock_cond blc(buckets_[i]);
        writelocker locker(blc.lock());
        hashnode *cur_node = NULL;
        if (NULL != (cur_node = buckets_[i].node)) {
          while (NULL != cur_node) {
            abort_unless(cur_node->check_magic_code());
            hashnode *tmp_node = cur_node->next;
            allocer_->free(cur_node);
            cur_node = tmp_node;
          }
        }
        buckets_[i].node = NULL;
      }
      size_ = 0;
    }
    return ret;
  }

  int reuse()
  {
   return clear();
  }

  bucket_iterator bucket_begin()
  {
    hashbucket *bucket = NULL;
    int64_t bucket_pos = 0;
    //if (NULL == buckets_ || NULL == allocer_)
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_NOT_INIT, "hashtable not init, backtrace=%s", lbt());
    } else {
      bucket = &buckets_[bucket_pos];
    }
    return bucket_iterator(this, bucket_pos, bucket);
  }

  bucket_iterator bucket_end()
  {
    return bucket_iterator(this, bucket_num_, NULL);
  }

  iterator begin()
  {
    hashnode *node = NULL;
    int64_t bucket_pos = 0;
    //if (NULL == buckets_ || NULL == allocer_)
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_NOT_INIT, "hashtable not init, backtrace=%s", lbt());
    } else {
      while (NULL == node && bucket_pos < bucket_num_) {
        node = buckets_[bucket_pos].node;
        if (NULL == node) {
          ++bucket_pos;
        }
      }
    }
    return iterator(this, bucket_pos, node);
  }

  iterator end()
  {
    return iterator(this, bucket_num_, NULL);
  }

  const_iterator begin() const
  {
    hashnode *node = NULL;
    int64_t bucket_pos = 0;
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG_RET(HASH_WARNING, OB_NOT_INIT, "hashtable not init, backtrace=%s", lbt());
    } else {
      while (NULL == node && bucket_pos < bucket_num_) {
        node = buckets_[bucket_pos].node;
        if (NULL == node) {
          ++bucket_pos;
        }
      }
    }
    return const_iterator(this, bucket_pos, node);
  }

  const_iterator end() const
  {
    return const_iterator(this, bucket_num_, NULL);
  }

private:
  inline int internal_get(const hashbucket &bucket,
                          const _key_type &key,
                          _value_type &value,
                          bool &is_fake) const
  {
    int ret = OB_SUCCESS;
    const _value_type *tmp_value = NULL;
    if (OB_SUCC(internal_get(bucket, key, tmp_value, is_fake))) {
      if (OB_ISNULL(tmp_value)) {
        ret = OB_ERR_UNEXPECTED;
        HASH_WRITE_LOG(HASH_FATAL, "hashtable internal get null value, backtrace=%s", lbt());
      } else if (OB_FAIL(copy_assign(value, *tmp_value))) { // it is ok since this function is always called under lock protection
        HASH_WRITE_LOG(HASH_FATAL, "failed to copy data, ret = %d", ret);
      }
    }
    return ret;
  }

  inline int internal_get(const hashbucket &bucket,
                          const _key_type &key,
                          const _value_type *&value,
                          bool &is_fake) const
  {
    int ret = OB_HASH_NOT_EXIST;
    hashnode *node = bucket.node;
    is_fake = false;
    while (NULL != node) {
      abort_unless(node->check_magic_code());
      if (equal_(getkey_(node->data), key)) {
        value = &(node->data);
        is_fake = node->is_fake;
        ret = OB_SUCCESS;
        break;
      } else {
        node = node->next;
      }
    }
    return ret;
  }

  inline int internal_set(hashbucket &bucket,
                          const _value_type &value,
                          const bool is_fake)
  {
    int ret = OB_SUCCESS;
    hashnode *node = NULL;
    node = (hashnode *)(allocer_->alloc());
    if (NULL == node) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      HASH_WRITE_LOG(HASH_FATAL, "alloc hash node failed ret = %d", ret);
    } else {
      node->magic_ = hashnode::MAGIC;
      if (OB_FAIL(copy_assign(node->data, value))) {
        HASH_WRITE_LOG(HASH_FATAL, "failed to copy data, ret = %d", ret);
      } else {
        node->is_fake = is_fake;
        node->next = bucket.node;
        bucket.node = node;
        ATOMIC_INC((uint64_t *) &size_);
      }
    }
    return ret;
  }

  inline int internal_erase(hashbucket &bucket,
                            const _key_type &key)
  {
    int ret = OB_HASH_NOT_EXIST;
    hashnode *node = bucket.node;
    hashnode *prev = nullptr;
    while (nullptr != node) {
      if (equal_(getkey_(node->data), key)) {
        ret = OB_SUCCESS;
        if (nullptr == prev) {
          bucket.node = node->next;
        } else {
          prev->next = node->next;
        }
        allocer_->free(node);
        node = nullptr;
        ATOMIC_DEC((uint64_t *) &size_);
        break;
      } else {
        prev = node;
        node = node->next;
      }
    }
    return ret;
  }

  // This function is unsafe in that the pointer it returns might be invalid when
  // the user uses it. The multi-thread safeness is left to the caller.
  int internal_get_with_timeout_unsafe(hashbucket &bucket,
                                       const _key_type &key,
                                       const _value_type *&value,
                                       const int64_t timeout_us = 0)
  {
    bool is_fake = false;
    int ret = internal_get(bucket, key, value, is_fake);
    if (timeout_us > 0) {
      if (OB_SUCC(ret) && is_fake) {
        struct timespec ts;
        int64_t abs_timeout_us = get_cur_microseconds_time() + timeout_us;
        ts = microseconds_to_ts(abs_timeout_us);
        do {
          bucket_lock_cond blc(bucket);
          if (get_cur_microseconds_time() > abs_timeout_us
              || ETIMEDOUT == cond_waiter()(blc.cond(), blc.lock(), ts)) {
            HASH_WRITE_LOG(HASH_WARNING, "wait fake node become normal node timeout");
            ret = OB_HASH_GET_TIMEOUT;
            break;
          }
          ret = internal_get(bucket, key, value, is_fake);
          if (OB_HASH_NOT_EXIST == ret) {
            HASH_WRITE_LOG(HASH_WARNING, "after wake up, fake node is non-existent or deleted");
            ret = OB_ERROR;
            break;
          }
        } while (OB_SUCC(ret) && is_fake);
      }
      if (OB_HASH_NOT_EXIST == ret) {
        //add a fake value
        if (OB_FAIL(internal_set(bucket, _value_type(), true))) {
          // set error, do nothing
        }
      }
    } else {
      if (OB_SUCC(ret) && is_fake) {
        ret = OB_HASH_NOT_EXIST;
      }
    }
    return ret;
  }
public:
  // 4 functions below are thread-safe.
  //
  // if fake node is existent, also return OB_HASH_NOT_EXIST
  // if timeout_us is not zero, check fake node, if no fake node, add
  // fake node, else wait fake node become normal node
  int get_refactored(const _key_type &key,
          _value_type &value,
          const int64_t timeout_us = 0)
  {
    int ret = OB_HASH_NOT_EXIST;
    uint64_t hash_value = 0;
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG(HASH_WARNING, "hashtable not init");
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(hashfunc_(key, hash_value))) {
      HASH_WRITE_LOG(HASH_WARNING, "hash key failed, ret = %d", ret);
    } else {
      int64_t bucket_pos = hash_value % bucket_num_;
      hashbucket &bucket = buckets_[bucket_pos];
      MProtectGuard guard(bucket);
      bucket_lock_cond blc(bucket);
      readlocker locker(blc.lock());

      /* critical section */
      const _value_type *tmp_value = NULL;
      if (OB_SUCC(internal_get_with_timeout_unsafe(bucket, key, tmp_value, timeout_us))
          && NULL != tmp_value) {
        // it's ok since we're holding the read lock
        if (OB_FAIL(copy_assign(value, *tmp_value))) {
          HASH_WRITE_LOG(HASH_FATAL, "failed to copy data, ret = %d", ret);
        }
      } else {
        // do nothing
      }
      /* end of critical section */
    }
    return ret;
  }


  int get_refactored(const _key_type &key,
          const _value_type *&value,
          const int64_t timeout_us = 0)
  {
    int ret = OB_SUCCESS;
    uint64_t hash_value = 0;
    if (!inited(buckets_) || NULL == allocer_) {
      HASH_WRITE_LOG(HASH_WARNING, "hashtable not init");
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(hashfunc_(key, hash_value))) {
        HASH_WRITE_LOG(HASH_WARNING, "hash key failed, ret = %d", ret);
    } else {
      int64_t bucket_pos = hash_value % bucket_num_;
      hashbucket &bucket = buckets_[bucket_pos];
      bucket_lock_cond blc(bucket);
      MProtectGuard guard(bucket);
      readlocker locker(blc.lock());
      /* critical section */
      ret = internal_get_with_timeout_unsafe(bucket, key, value, timeout_us);
      /* end of critical section */
    }
    return ret;
  }

  template <typename _callback>
  int callback_helper(const _value_type &value, _callback *callback)
  {
    return (*callback)(value);
  }

  template <>
  int callback_helper<void>(const _value_type &value, void *callback)
  {
    return OB_SUCCESS;
  }

  // if key not exist, set node and execute callback
  // if callback failed, erase the node
  // parameters:
  //   flag: 0 shows that not cover existing object.
  //   callback: MUST with a int operater()
  // return value:
  //   OB_SUCCESS for success, the node is set
  //   OB_HASH_EXIST for node already exist
  //   others for error
  template <typename _callback = void, bool with_callback = !std::is_void<_callback>::value>
  int set_refactored(const _key_type &key, const _value_type &value, int flag = 0,
      int broadcast = 0, int overwrite_key = 0, _callback *callback = nullptr)
  {
    int ret = OB_SUCCESS;
    uint64_t hash_value = 0;
    UNUSED(overwrite_key);
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG(HASH_WARNING, "hashtable not init");
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(hashfunc_(key, hash_value))) {
      HASH_WRITE_LOG(HASH_WARNING, "hash key failed, ret = %d", ret);
    } else if (1 < EXTEND_RATIO && size_ >= bucket_num_) {
      if (OB_FAIL(extend())) {
        HASH_WRITE_LOG(HASH_WARNING, "failed to extend, ret = %d, bucket_num = %ld, size = %ld",
                                      ret, bucket_num_, size_);
      }
    }
    if (OB_SUCC(ret)) {
      int64_t bucket_pos = hash_value % bucket_num_;
      hashbucket &bucket = buckets_[bucket_pos];
      MProtectGuard guard(bucket);
      bucket_lock_cond blc(bucket);
      writelocker locker(blc.lock());
      hashnode *node = bucket.node;
      hashnode *prev = NULL;
      while (NULL != node && OB_SUCC(ret)) {
        abort_unless(node->check_magic_code());
        if (equal_(getkey_(node->data), key)) {
          if (0 == flag) {
            ret = OB_HASH_EXIST;
          } else {
            // write on a temp node, then replace prev node
            // if copy assign node failed, keep prev node
            hashnode *tmp_node = NULL;
            tmp_node = (hashnode *)(allocer_->alloc());
            if (OB_ISNULL(tmp_node)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              HASH_WRITE_LOG(HASH_FATAL, "alloc hash node failed, ret = %d", ret);
            } else if (FALSE_IT(tmp_node->magic_ = hashnode::MAGIC)) {
            } else if (OB_FAIL(hash::copy(tmp_node->data, value, hash::NormalPairTag()))) {
              HASH_WRITE_LOG(HASH_FATAL, "copy data failed, ret = %d", ret);
              allocer_->free(tmp_node);
              tmp_node = NULL;
            } else {
              tmp_node->next = node->next;
              if (NULL == prev) {
                bucket.node = tmp_node;
              } else {
                prev->next = tmp_node;
              }
              allocer_->free(node);
              tmp_node->is_fake = false;
            }
            if (broadcast != 0) {
              bucket_lock_cond blc(bucket);
              cond_broadcaster()(blc.cond());
            }
          }
          break;
        } else {
          prev = node;
          node = node->next;
        }
      }
      if (OB_SUCC(ret) && NULL == node) {
        ret = internal_set(bucket, value, false);
        if (with_callback) {
          if (OB_SUCC(ret)) {
            int tmp_ret = callback_helper(value, callback);
            if (OB_SUCCESS != tmp_ret) {
              HASH_WRITE_LOG(HASH_WARNING, "hashtable executes callback failed, tmp_ret=%d", tmp_ret);
              ret = OB_ERR_UNEXPECTED;
              // never fail because internal_set succeed
              (void) internal_erase(bucket, key);
            }
          }
        }
      }
    }
    return ret;
  }

  int extend()
  {
    int ret = OB_SUCCESS;
    bucket_array old_buckets = buckets_;
    int64_t old_bucket_num = bucket_num_;
    bucket_num_ = old_bucket_num * EXTEND_RATIO;
    if (OB_ISNULL(bucket_allocer_)) {
      ret = OB_INVALID_ARGUMENT;
      HASH_WRITE_LOG(HASH_WARNING, "bucket_allocer is null, ret=%d", ret);
    } else if (OB_UNLIKELY(0 != (ret = hash::create(buckets_,
                                             bucket_num_,
                                             ARRAY_SIZE,
                                             sizeof(hashbucket),
                                             *bucket_allocer_)))) {
      HASH_WRITE_LOG(HASH_WARNING, "create buckets fail, ret=%d", ret);
    } else {
      for (int64_t i = 0; i < old_bucket_num && OB_SUCC(ret); i++) {
        hashbucket &old_bucket = old_buckets[i];
        hashnode *cur_node = old_bucket.node;
        hashnode *next_node = NULL;
        while (NULL != cur_node && OB_SUCC(ret)) {
          abort_unless(cur_node->check_magic_code());
          uint64_t hash_value = 0;
          if (OB_FAIL(hashfunc_(getkey_(cur_node->data), hash_value))) {
            HASH_WRITE_LOG(HASH_WARNING, "hash key failed, ret = %d", ret);
          } else {
            int64_t bucket_pos = hash_value % bucket_num_;
            hashbucket &new_bucket = buckets_[bucket_pos];
            // record next node
            next_node = cur_node->next;
            // insert into new bucket
            cur_node->next = new_bucket.node;
            new_bucket.node = cur_node;
            //set cur node
            cur_node = next_node;
          }
        }
      }
      hash::destroy(old_buckets, *bucket_allocer_);
    }

    return ret;
  }

  // notice that it's to update a value atomicly in bucket lock,
  // so there must not be any call of hashtable function in callback.
  template<class _callback>
  int atomic_refactored(const _key_type &key, _callback &callback)
  {
    return atomic(key, callback, preproc_);
  }
  // this operation will rdlock on bucket, if there is writing in callback, keep atomic by yourself.
  template<class _callback>
  int read_atomic(const _key_type &key, _callback &callback)
  {
    return read_atomic(key, callback, preproc_);
  }

  // 该原子操作在bucket上添加的写锁,
  // 如果节点存在，调用 callback 进行修改，如果节点不存在，插入该节点
  //
  // 返回值：
  //   OB_SUCCESS 表示成功
  //   其它 表示出错
  template<class _callback>
  int set_or_update(const _key_type &key, const _value_type &value,
                    _callback &callback)
  {
    return set_or_update(key, value, callback, preproc_);
  }

  int erase_refactored(const _key_type &key, _value_type *value = NULL)
  {
    int ret = OB_SUCCESS;
    uint64_t hash_value = 0;
    //if (NULL == buckets_ || NULL == allocer_)
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG(HASH_WARNING, "hashtable not init");
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(hashfunc_(key, hash_value))) {
      HASH_WRITE_LOG(HASH_WARNING, "hash key failed, ret = %d", ret);
    } else {
      int64_t bucket_pos = hash_value % bucket_num_;
      hashbucket &bucket = buckets_[bucket_pos];
      bucket_lock_cond blc(bucket);
      writelocker locker(blc.lock());
      hashnode *node = bucket.node;
      hashnode *prev = NULL;
      ret = OB_HASH_NOT_EXIST;
      while (NULL != node && OB_HASH_NOT_EXIST == ret) {
        abort_unless(node->check_magic_code());
        if (equal_(getkey_(node->data), key)) {
          ret = OB_SUCCESS;
          if (NULL != value) {
            if (OB_FAIL(copy_assign(*value, node->data))) {
              HASH_WRITE_LOG(HASH_FATAL, "failed to copy data, ret = %d", ret);
            }
          }
          // if copy assign failed, won't erase, keep prev node
          if (OB_SUCC(ret)) {
            if (NULL == prev) {
              bucket.node = node->next;
            } else {
              prev->next = node->next;
            }
            allocer_->free(node);
            node = NULL;
            ATOMIC_DEC((uint64_t *) &size_);
            bucket_lock_cond blc(bucket);
            cond_broadcaster()(blc.cond());
          }
          break;
        } else {
          prev = node;
          node = node->next;
        }
      }
    }
    return ret;
  }

  // erase key value pair if pred is met
  // thread safe erase, will add write lock to the bucket
  // return value:
  //   OB_SUCCESS for success
  //   OB_HASH_NOT_EXIST for node not exists
  //   others for error
  template<class _pred>
  int erase_if(const _key_type &key, _pred &pred, bool &is_erased, _value_type *value = NULL)
  {
    return erase_if(key, pred, preproc_, is_erased, value);
  }

  // erase key value pair if pred is met
  // thread safe erase, will add write lock to the bucket
  template<class _pred, class _preproc>
  int erase_if(const _key_type &key, _pred &pred, _preproc &preproc,
               bool &is_erased, _value_type *value = NULL)
  {
    int ret = OB_SUCCESS;
    uint64_t hash_value = 0;
    is_erased = false;
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG(HASH_WARNING, "hashtable not init");
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(hashfunc_(key, hash_value))) {
      HASH_WRITE_LOG(HASH_WARNING, "hash key failed, ret = %d", ret);
    } else {
      int64_t bucket_pos = hash_value % bucket_num_;
      hashbucket &bucket = buckets_[bucket_pos];
      bucket_lock_cond blc(bucket);
      writelocker locker(blc.lock());
      hashnode *node = bucket.node;
      hashnode *prev = NULL;
      ret = OB_HASH_NOT_EXIST;
      while (NULL != node) {
        if (equal_(getkey_(node->data), key)) {
          ret = OB_SUCCESS;
          if (pred(preproc(node->data))) {
            if (NULL != value) {
              if (OB_FAIL(copy_assign(*value, node->data))) {
                HASH_WRITE_LOG(HASH_FATAL, "failed to copy data, ret = %d", ret);
              }
            }
            if (OB_SUCC(ret)) {
              if (NULL == prev) {
                bucket.node = node->next;
              } else {
                prev->next = node->next;
              }
              allocer_->free(node);
              node = NULL;
              ATOMIC_DEC((uint64_t *) &size_);
              bucket_lock_cond blc(bucket);
              cond_broadcaster()(blc.cond());
              is_erased = true;
            }
          }
          break;
        } else {
          prev = node;
          node = node->next;
        }
      }
    }
    return ret;
  }

  // thread safe scan, will add read lock to the bucket,
  // the modification to the value is forbidden
  // @param callback
  // @return  OB_SUCCESS for success, other for errors.
  template<class _callback>
  int foreach_refactored(_callback &callback) const
  {
    int ret = OB_SUCCESS;
    static_assert(std::is_same<decltype(callback(*(_value_type*)0)), int>::value,
        "hash table foreach callback format error");
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG(HASH_WARNING, "hashtable not init");
      ret = OB_NOT_INIT;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < bucket_num_; i++) {
        hashbucket &bucket = buckets_[i];
        bucket_lock_cond blc(bucket);
        readlocker locker(blc.lock());
        hashnode *node = bucket.node;
        while (OB_SUCC(ret) && NULL != node) {
          abort_unless(node->check_magic_code());
          if (OB_FAIL(callback(node->data))) {
            HASH_WRITE_LOG(HASH_WARNING, "fail to do call back, ret=%d", ret);
          } else {
            node = node->next;
          }
        }
      }
    }
    return ret;
  }

public:
  int64_t size() const
  {
    return size_;
  }

  int64_t get_bucket_count() const { return bucket_num_; }

public:
  template<class _archive>
  int serialization(_archive &archive)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG(HASH_WARNING, "hashtable not init");
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(hash::serialization(archive, bucket_num_))) {
      HASH_WRITE_LOG(HASH_WARNING,
                     "serialize hash bucket_num fail bucket_num=%ld, ret=%d", bucket_num_, ret);
    } else if (OB_FAIL(hash::serialization(archive, size_))) {
      HASH_WRITE_LOG(HASH_WARNING, "serialize hash size fail size=%ld, ret=%d", size_, ret);
    } else if (size_ > 0) {
      for (iterator iter = begin(); OB_SUCC(ret) && iter != end(); iter++) {
        if (OB_FAIL(hash::serialization(archive, *iter))) {
          HASH_WRITE_LOG(HASH_WARNING, "serialize item fail value_pointer=%p, ret=%d",
              &(*iter), ret);
        }
      }
    }
    return ret;
  }

  template<class _archive>
  int deserialization(_archive &archive, _allocer *allocer)
  {
    int ret = OB_SUCCESS;
    int64_t bucket_num = 0;
    int64_t size = 0;
    if (OB_UNLIKELY(NULL == allocer)) {
      HASH_WRITE_LOG(HASH_WARNING, "invalid param allocer null pointer");
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(hash::deserialization(archive, bucket_num))
               || OB_UNLIKELY(0 >= bucket_num)) {
      HASH_WRITE_LOG(HASH_WARNING, "deserialize bucket_num fail, ret=%d, bucket_num=%ld",
          ret, bucket_num);
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else if (OB_FAIL(hash::deserialization(archive, size))) {
      HASH_WRITE_LOG(HASH_WARNING, "deserialize size fail, ret=%d", ret);
    } else {
      if (OB_UNLIKELY(inited(buckets_)) && OB_UNLIKELY(NULL != allocer_)) {
        destroy();
      }
      if (OB_SUCC(create(bucket_num, allocer, bucket_allocer_))) {
        _value_type value;
        for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
          if (OB_FAIL(hash::deserialization(archive, value))) {
            HASH_WRITE_LOG(HASH_WARNING,
                           "deserialize item fail num=%ld, ret=%d", i, ret);
          } else if (OB_FAIL(set_refactored(getkey_(value), value))) {
            HASH_WRITE_LOG(HASH_WARNING, "insert item fail num=%ld, ret=%d", i, ret);
          } else {
            // do nothing
          }
        }
      }
    }
    return ret;
  }

  // this operation will rdlock on bucket, if there is writing in callback, keep atomic by yourself.
  template<class _callback, class _preproc>
  int read_atomic(const _key_type &key, _callback &callback,
             _preproc &preproc)
  {
    int ret = OB_SUCCESS;
    uint64_t hash_value = 0;
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG(HASH_WARNING, "hashtable not init");
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(hashfunc_(key, hash_value))) {
      HASH_WRITE_LOG(HASH_WARNING, "hash key failed, ret = %d", ret);
    } else {
      int64_t bucket_pos = hash_value % bucket_num_;
      hashbucket &bucket = buckets_[bucket_pos];
      bucket_lock_cond blc(bucket);
      readlocker locker(blc.lock());
      hashnode *node = bucket.node;
      ret = OB_HASH_NOT_EXIST;
      while (NULL != node) {
        abort_unless(node->check_magic_code());
        if (equal_(getkey_(node->data), key)) {
          callback(preproc(node->data));
          ret = OB_SUCCESS;
          break;
        } else {
          node = node->next;
        }
      }
    }
    return ret;
  }

  // not use as interface, this function wrlock on bucket.
  template<class _callback, class _preproc>
  int atomic(const _key_type &key, _callback &callback,
             _preproc &preproc)
  {
    int ret = OB_SUCCESS;
    uint64_t hash_value = 0;
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG(HASH_WARNING, "hashtable not init");
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(hashfunc_(key, hash_value))) {
        HASH_WRITE_LOG(HASH_WARNING, "hash key failed, ret = %d", ret);
    } else {
      int64_t bucket_pos = hash_value % bucket_num_;
      hashbucket &bucket = buckets_[bucket_pos];
      bucket_lock_cond blc(bucket);
      writelocker locker(blc.lock());
      hashnode *node = bucket.node;
      ret = OB_HASH_NOT_EXIST;
      while (NULL != node) {
        abort_unless(node->check_magic_code());
        if (equal_(getkey_(node->data), key)) {
          callback(preproc(node->data));
          ret = OB_SUCCESS;
          break;
        } else {
          node = node->next;
        }
      }
    }
    return ret;
  }

  // 不存在就插入，存在就调用 callback 修改
  // 该原子操作在bucket上添加的写锁
  template<class _callback, class _preproc>
  int set_or_update(const _key_type &key, const _value_type &value,
                    _callback &callback, _preproc &preproc)
  {
    int ret = OB_SUCCESS;
    uint64_t hash_value;
    if (OB_UNLIKELY(!inited(buckets_)) || OB_UNLIKELY(NULL == allocer_)) {
      HASH_WRITE_LOG(HASH_WARNING, "hashtable not init");
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(hashfunc_(key, hash_value))) {
      HASH_WRITE_LOG(HASH_WARNING, "hash key failed, ret = %d", ret);
    } else {
      int64_t bucket_pos = hash_value % bucket_num_;
      hashbucket &bucket = buckets_[bucket_pos];
      bucket_lock_cond blc(bucket);
      writelocker locker(blc.lock());
      hashnode *node = bucket.node;
      while (NULL != node) {
        if (equal_(getkey_(node->data), key)) {
          callback(preproc(node->data));
          break;
        } else {
          node = node->next;
        }
      }
      if (NULL == node) {
        ret = internal_set(bucket, value, false);
      }
    }

    return ret;
  }
private:
  _bucket_allocer default_bucket_allocer_;
  _allocer *allocer_;
  _bucket_allocer *bucket_allocer_;
  //hashbucket *buckets_;
  bucket_array buckets_;
  int64_t bucket_num_;
  int64_t size_;

  mutable preproc preproc_;
  mutable _hashfunc hashfunc_;
  mutable _equal equal_;
  mutable _getkey getkey_;
};
}
}
}

#endif //OCEANBASE_COMMON_HASH_HASHTABLE_H_
