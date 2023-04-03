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

#ifndef OCEANBASE_STORAGE_OB_TRANS_HASHMAP_
#define OCEANBASE_STORAGE_OB_TRANS_HASHMAP_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"
/*
 * For Example
 * 
 * 1. Define the hash value class
 *
 *   class ObTransCtx : public ObTransHashLink<ObTransCtx>
 *   {
 *   public:
 *     // hash key compare method
 *     bool contain(const ObTransID &trans_id) const {  return trans_id == trans_id_; }
 *   private:
 *     // hash key
 *     ObTransID trans_id_;
 *   };
 *
 * 2. define the allocator of hash value
 *   class ObTransCtxAlloc
 *   {
 *   public:
 *     Value *alloc_value() const {
 *       // for example
 *       return op_alloc(Value);
 *     }
 *     void free_value(Value * val) {
 *       // for example
 *       if (NULL != val) {
 *         op_free(val);
 *       }
 *     }
 *     xxxxx
 *   };
 *
 * 3. define the HashMap
 *   ObTransHashMap <ObTransID, ObTransCtx, ObTransCtxAlloc> CtxMap;
 *
 * 4. Ref 
 *   insert_and_get // create the hash value , ref = ref + 2;
 *   get()          // ref++
 *   revert         // ref --; 
 *
 * 5. More Attentions are as followed:
 *
 * 1) 'Key -> Value' must be 1:1，otherwise you should not use such hashmap;
 * 2) 'Key -> Value' must be 1:1，otherwise you should not use such hashmap;
 * 3) 'Key -> Value' must be 1:1，otherwise you should not use such hashmap;
 *
 */

namespace oceanbase
{
namespace transaction
{
template<typename Value>
class ObTransHashLink
{
public:
  ObTransHashLink() : ref_(0), prev_(NULL), next_(NULL) {}
  ~ObTransHashLink()
  {
    ref_ = 0;
    prev_ = NULL;
    next_ = NULL;
  }
  inline int inc_ref(int32_t x)
  {
    if (ref_ < 0) {
      TRANS_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "unexpected ref when inc ref", K(ref_));
    }
    return ATOMIC_FAA(&ref_, x);
  }
  inline int32_t dec_ref(int32_t x)
  {
    int32_t ref = ATOMIC_SAF(&ref_, x);
    if (ref < 0) {
      TRANS_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "unexpected error", K(ref_));
    }
    return ref;
  }
  int32_t get_ref() const { return ref_; }
  int32_t ref_;
  Value *prev_;
  Value *next_;
};

template<typename Key, typename Value, typename AllocHandle, typename LockType, int64_t BUCKETS_CNT = 64>
class ObTransHashMap
{
 typedef common::ObSEArray<Value *, 32> ValueArray;
public:
  ObTransHashMap() : is_inited_(false), total_cnt_(0)
  {
    OB_ASSERT(BUCKETS_CNT > 0);
  }
  ~ObTransHashMap() { destroy(); }
  int64_t count() const { return ATOMIC_LOAD(&total_cnt_); }
  int64_t alloc_cnt() const { return alloc_handle_.get_alloc_cnt(); }
  void reset()
  {
    if (is_inited_) {
      // del all value from hash backet
      Value *curr = nullptr;
      Value *next = nullptr;
      for (int64_t i = 0; i < BUCKETS_CNT; ++i) {
        {
          BucketWLockGuard guard(buckets_[i].lock_, get_itid());

          curr = buckets_[i].next_;
          while (OB_NOT_NULL(curr)) {
            next = curr->next_;
            del_from_bucket_(i, curr);
            // dec ref and free curr value
            revert(curr);
            curr = next;
          }
        }
        // reset bucket
        buckets_[i].reset();
      }
      total_cnt_ = 0;
      is_inited_ = false;
    }
  }

  void destroy() { reset(); }

  int init(const lib::ObMemAttr &mem_attr)
  {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      TRANS_LOG(WARN, "ObTransHashMap init twice", K(ret));
    } else {
      // init bucket, init lock in bucket
      for (int64_t i = 0 ; OB_SUCC(ret) && i < BUCKETS_CNT; ++i) {
        if (OB_FAIL(buckets_[i].init(mem_attr))) {
          TRANS_LOG(WARN, "ObTransHashMap bucket init fail", K(ret));
          for (int64_t j = 0 ; j <= i; ++j) {
            buckets_[j].destroy();
          }
        }
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
    return ret;
  }

  int insert_and_get(const Key &key, Value *value, Value **old_value)
  { return insert__(key, value, 2, old_value); }
  int insert(const Key &key, Value *value)
  { return insert__(key, value, 1, 0); }
  int insert__(const Key &key, Value *value, int ref, Value **old_value)
  {
    int ret = OB_SUCCESS;

    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObTransHashMap not init", K(ret), KP(value));
    } else if (!key.is_valid() || OB_ISNULL(value)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(key), KP(value));
    } else {
      int64_t pos = key.hash() % BUCKETS_CNT;
      BucketWLockGuard guard(buckets_[pos].lock_, get_itid());
      Value *curr = buckets_[pos].next_;

      while (OB_NOT_NULL(curr)) {
        if (curr->contain(key)) {
          break;
        } else {
          curr = curr->next_;
        }
      }
      if (OB_ISNULL(curr)) {
        // inc ref when value in hashmap
        value->inc_ref(ref);
        if (NULL != buckets_[pos].next_) {
          buckets_[pos].next_->prev_ = value;
        }
        value->next_ = buckets_[pos].next_;
        value->prev_ = NULL;
        buckets_[pos].next_ = value;
        ATOMIC_INC(&total_cnt_);
      } else {
        ret = OB_ENTRY_EXIST;
        if (old_value) {
          curr->inc_ref(1);
          *old_value = curr;
        }
      }
    }
    return ret;
  }

  int del(const Key &key, Value *value)
  {
    int ret = OB_SUCCESS;

    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObTransHashMap not init", K(ret), KP(value));
    } else if (!key.is_valid() || OB_ISNULL(value)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(ERROR, "invalid argument", K(key), KP(value));
    } else {
      int64_t pos = key.hash() % BUCKETS_CNT;
      BucketWLockGuard guard(buckets_[pos].lock_, get_itid());
      if (buckets_[pos].next_ != value &&
          (NULL == value->prev_ && NULL == value->next_)) {
        // do nothing
      } else {
        del_from_bucket_(pos, value);
        revert(value);
      }
    }
    return ret;
  }

  void del_from_bucket_(const int64_t pos, Value *curr)
  {
    if (curr == buckets_[pos].next_) {
      if (NULL == curr->next_) {
        buckets_[pos].next_ = NULL;
      } else {
        buckets_[pos].next_ = curr->next_;
        curr->next_->prev_ = curr->prev_;
      }
    } else {
      curr->prev_->next_ = curr->next_;
      if (NULL != curr->next_) {
        curr->next_->prev_ = curr->prev_;
      }
    }
    curr->prev_ = NULL;
    curr->next_ = NULL;
    ATOMIC_DEC(&total_cnt_);
  }

  int get(const Key &key, Value *&value)
  {
    int ret = OB_SUCCESS;

    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "ObTransHashMap not init", K(ret), K(key));
    } else if (!key.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(key));
    } else {
      Value *tmp_value = NULL;
      int64_t pos = key.hash() % BUCKETS_CNT;

      BucketRLockGuard guard(buckets_[pos].lock_, get_itid());

      tmp_value = buckets_[pos].next_;
      while (OB_NOT_NULL(tmp_value)) {
        if (tmp_value->contain(key)) {
          value = tmp_value;
          break;
        } else {
          tmp_value = tmp_value->next_;
        }
      }

      if (OB_ISNULL(tmp_value)) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        // inc ref when get value
        value->inc_ref(1);
      }
    }
    return ret;
  }

  void revert(Value *value)
  {
    if (OB_NOT_NULL(value)) {
      if (0 == value->dec_ref(1)) {
        alloc_handle_.free_value(value);
      }
    }
  }

  template <typename Function> int for_each(Function &fn)
  {
    int ret = common::OB_SUCCESS;
    for (int64_t pos = 0 ; OB_SUCC(ret) && pos < BUCKETS_CNT; ++pos) {
      ret = for_each_in_one_bucket(fn, pos);
    }
    return ret;
  }

  template <typename Function> int for_each_in_one_bucket(Function& fn, int64_t bucket_pos)
  {
    int ret = common::OB_SUCCESS;
    if (bucket_pos < 0 || bucket_pos >= BUCKETS_CNT) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ValueArray array;
      if (OB_FAIL(generate_value_arr_(bucket_pos, array))) {
        TRANS_LOG(WARN, "generate value array error", K(ret));
      } else {
        const int64_t cnt = array.count();
        for (int64_t i = 0; i < cnt; ++i) {
          if (OB_SUCC(ret) && !fn(array.at(i))) {
            ret = OB_EAGAIN;
          }
          if (0 == array.at(i)->dec_ref(1)) {
            alloc_handle_.free_value(array.at(i));
          }

        }
      }
    }
    return ret;
  }

  template <typename Function> int remove_if(Function &fn)
  {
    int ret = common::OB_SUCCESS;

    ValueArray array;
    for (int64_t pos = 0 ; pos < BUCKETS_CNT; ++pos) {
      array.reset();
      if (OB_FAIL(generate_value_arr_(pos, array))) {
        TRANS_LOG(WARN, "generate value array error", K(ret));
      } else {
        const int64_t cnt = array.count();
        for (int64_t i = 0; i < cnt; ++i) {
          if (fn(array.at(i))) {
            BucketWLockGuard guard(buckets_[pos].lock_, get_itid());
            if (buckets_[pos].next_ != array.at(i)
                && (NULL == array.at(i)->prev_ && NULL == array.at(i)->next_)) {
              // do nothing
            } else {
              del_from_bucket_(pos, array.at(i));
            }
          }
          if (0 == array.at(i)->dec_ref(1)) {
            alloc_handle_.free_value(array.at(i));
          }
        }
      }
    }
    return ret;
  }

  int generate_value_arr_(const int64_t bucket_pos, ValueArray &arr)
  {
    int ret = common::OB_SUCCESS;
    // read lock
    BucketRLockGuard guard(buckets_[bucket_pos].lock_, get_itid());
    Value *val = buckets_[bucket_pos].next_;

    while (OB_SUCC(ret) && OB_NOT_NULL(val)) {
      val->inc_ref(1);
      if (OB_FAIL(arr.push_back(val))) {
        TRANS_LOG(WARN, "value array push back error", K(ret));
        val->dec_ref(1);
      }
      val = val->next_;
    }

    if (OB_FAIL(ret)) {
      const int64_t cnt = arr.count();
      for (int64_t i = 0; i < cnt; ++i) {
        arr.at(i)->dec_ref(1);
      }
    }
    return ret;
  }

  int alloc_value(Value *&value)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == (value = alloc_handle_.alloc_value())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    return ret;
  }

  void free_value(Value *value)
  {
    if (OB_NOT_NULL(value)) {
      alloc_handle_.free_value(value);
    }
  }

  int64_t get_total_cnt() {
    return ATOMIC_LOAD(&total_cnt_);
  }

  static int64_t get_buckets_cnt() {
    return BUCKETS_CNT;
  }
private:
  struct ObTransHashHeader
  {
    Value *next_;
    Value *hot_cache_val_;
    LockType lock_;

    ObTransHashHeader() : next_(NULL), hot_cache_val_(NULL) {}
    ~ObTransHashHeader() { destroy(); }
    int init(const lib::ObMemAttr &mem_attr)
    {
      return lock_.init(mem_attr);
    }
    void reset()
    {
      next_ = NULL;
      hot_cache_val_ = NULL;
      lock_.destroy();
    }
    void destroy()
    {
      reset();
    }
  };

  // thread local node
  class Node
  {
  public:
    Node() : thread_id_(-1) {}
    void reset() { thread_id_ = -1; }
    void set_thread_id(const int64_t thread_id) { thread_id_ = thread_id; }
    uint64_t get_thread_id() const { return thread_id_; }
  private:
    uint64_t thread_id_;
  };

  class BucketRLockGuard
  {
  public:
    explicit BucketRLockGuard(const LockType &lock, const uint64_t thread_id)
        : lock_(const_cast<LockType &>(lock)), ret_(OB_SUCCESS)
    {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.rdlock()))) {
        COMMON_LOG_RET(WARN, ret_, "Fail to read lock, ", K_(ret));
      } else {
        ObTransHashMap::get_thread_node().set_thread_id(thread_id);
      }
    }
    ~BucketRLockGuard()
    {
      if (OB_LIKELY(OB_SUCCESS == ret_)) {
        lock_.rdunlock();
        ObTransHashMap::get_thread_node().reset();
      }
    }
    inline int get_ret() const { return ret_; }
  private:
    LockType &lock_;
    int ret_;
  private:
    DISALLOW_COPY_AND_ASSIGN(BucketRLockGuard);
  };

  class BucketWLockGuard
  {
  public:
    explicit BucketWLockGuard(const LockType &lock, const uint64_t thread_id)
        : lock_(const_cast<LockType &>(lock)), ret_(OB_SUCCESS), locked_(false)
    {
      // no need to lock
      if (get_itid() == get_thread_node().get_thread_id()) {
        locked_ = false;
        TRANS_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "unexpected thread status", K(thread_id));
      } else {
        if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrlock()))) {
          COMMON_LOG_RET(WARN, ret_, "Fail to write lock, ", K_(ret));
        } else {
          locked_ = true;
          ObTransHashMap::get_thread_node().set_thread_id(thread_id);
        }
      }
    }
    ~BucketWLockGuard()
    {
      if (locked_ && OB_LIKELY(OB_SUCCESS == ret_)) {
        lock_.wrunlock();
        ObTransHashMap::get_thread_node().reset();
      }
    }
    inline int get_ret() const { return ret_; }
  private:
    LockType &lock_;
    int ret_;
    bool locked_;
  private:
    DISALLOW_COPY_AND_ASSIGN(BucketWLockGuard);
  };

  static Node &get_thread_node()
  {
    RLOCAL_INLINE(Node, node);
    return node;
  }

private:
  // sizeof(ObTransHashMap) = BUCKETS_CNT * sizeof(LockType);
  // sizeof(SpinRWLock) = 20B;
  // sizeof(QsyncLock) = 4K;
  bool is_inited_;
  ObTransHashHeader buckets_[BUCKETS_CNT];
  int64_t total_cnt_;
#ifndef NDEBUG
public:
#endif
  AllocHandle alloc_handle_;
};

}
}
#endif // OCEANBASE_STORAGE_OB_TRANS_HASHMAP_
