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

#ifndef OCEANBASE_LIB_HASH_OB_CONCURRENT_HASH_MAP_WITH_HAZARD_VALUE_
#define OCEANBASE_LIB_HASH_OB_CONCURRENT_HASH_MAP_WITH_HAZARD_VALUE_

#include <lib/hash/ob_hash.h>
#include <lib/allocator/ob_small_allocator.h>
#include <lib/hash/ob_hazard_pointer.h>
#include "lib/hash/ob_concurrent_hash_map.h"       // HashAlloc

namespace oceanbase
{
namespace common
{
/*
struct HashAlloc: public IAlloc
{
public:
  HashAlloc(): is_inited_(false), size_(0) {}
  virtual ~HashAlloc() {}
  inline void *alloc(int64_t sz);
  inline void free(void *p);
private:
  bool is_inited_;
  int64_t size_;
  ObSmallAllocator alloc_;
};

struct ArrayAlloc: public IAlloc
{
public:
  ArrayAlloc() {}
  virtual ~ArrayAlloc() {}
  inline void *alloc(int64_t sz);
  inline void free(void *p);
};
*/

template<typename Key, typename Value>
class ObConcurrentHashMapWithHazardValue {};

template<typename Key, typename Value>
class ObConcurrentHashMapWithHazardValue<Key, Value *>
{
public:
  class IValueAlloc
  {
  public:
    virtual Value *alloc() = 0;
    virtual void   free(Value *value) = 0;
  };
  class IValueReclaimCallback
  {
  public:
    virtual void reclaim_value(Value *value) = 0;
  };
public:
  ObConcurrentHashMapWithHazardValue();
  ObConcurrentHashMapWithHazardValue(IHashAlloc &hash_alloc, IArrayAlloc &array_alloc);
  int init();
  int init(IValueAlloc *value_alloc);

  /**
   * @param value_alloc to control the alloc and gc of Value
   * @param value_reclaim_callback callback of Value gc
   */
  int init(IValueAlloc *value_alloc, IValueReclaimCallback *value_reclaim_callback);

  // return OB_SUCCESS for success, OB_ENTRY_EXIST for key exist, other for errors.
  int put_refactored(const Key &key, Value *value);
  // return OB_SUCCESS for success, OB_ENTRY_NOT_EXIST for key not exist, other for errors.
  int remove_refactored(const Key &key);
  // return OB_SUCCESS for success, OB_ENTRY_EXIST for key exist, other for errors.
  int create_refactored(const Key &key, Value *&value);
  // return OB_SUCCESS for success, OB_ENTRY_NOT_EXIST for key not exist, other for errors.
  int get_refactored(const Key &key, Value *&value);
  int revert_value(const Value *value);
  // return OB_ENTRY_EXIST for key exist, OB_ENTRY_NOT_EXIST for key not exist, other for errors.
  int contains_key(const Key &key) const;
  // return OB_SUCCESS for success, other for errors.
  template <typename Function> int for_each(Function &fn);
  // return OB_SUCCESS for success, other for errors.
  int get_count(int64_t &cnt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObConcurrentHashMapWithHazardValue);
private:
  typedef HashRoot                  SHashRoot;
  typedef HashBase<Key, Value *>    SHash;
  typedef typename SHash::Iterator  SScanIterator;
  typedef typename SHash::Handle    SScanHandle;
  typedef typename SHash::Handle    SGetHandle;
  typedef typename SHash::Handle    SPutHandle;
protected:
  /// this class is for ObConcurrentHashMapWithHazardValue
  class DefaultValueAlloc : public IValueAlloc
  {
  public:
    Value *alloc() { return op_alloc(Value); }
    void   free(Value *value) { op_free(value); value = NULL; }
  };
  /// this class is for ObConcurrentHashMapWithHazardValue
  class DefaultValueReclaimCallback : public IValueReclaimCallback
  {
  public:
    DefaultValueReclaimCallback() : alloc_(NULL) {}
    int init(IValueAlloc *alloc);
    virtual void reclaim_value(Value *value);
  private:
    DISALLOW_COPY_AND_ASSIGN(DefaultValueReclaimCallback);
  private:
    IValueAlloc *alloc_;
  };
  /// this class is for ObHazardPointer
  class HazardPtrReclaimCallback : public ObHazardPointer::ReclaimCallback
  {
  public:
    HazardPtrReclaimCallback() : value_reclaim_callback_(NULL) {}
    int init(IValueReclaimCallback *value_reclaim_callback);
    virtual void reclaim_ptr(uintptr_t ptr);
  private:
    DISALLOW_COPY_AND_ASSIGN(HazardPtrReclaimCallback);
  private:
    IValueReclaimCallback *value_reclaim_callback_;
  };
  /// this class is for HashBase <ob_hash.h>
  class HashReclaimCallback : public SHash::IKVRCallback
  {
  public:
    HashReclaimCallback() : hazard_ptr_(NULL) {}
    int init(ObHazardPointer *hazard_ptr);
    virtual void reclaim_key_value(Key &key, Value *&value);
  private:
    DISALLOW_COPY_AND_ASSIGN(HashReclaimCallback);
  private:
    ObHazardPointer *hazard_ptr_;
  };
private:
  int err_code_map(int err) const;
  int check_init() const;
private:
  ObHazardPointer               hazard_pointer_;
  HazardPtrReclaimCallback      hazard_ptr_reclaim_callback_;
  IValueAlloc *                 value_alloc_;
  IValueReclaimCallback *       value_reclaim_callback_;
  DefaultValueAlloc             default_value_alloc_;
  DefaultValueReclaimCallback   default_value_reclaim_callback_;
  HashAlloc                     hash_alloc_;
  ArrayAlloc                    array_alloc_;
  HashReclaimCallback           reclaim_callback_;
  SHash                         hash_;
  SHashRoot                     hash_root_;
  bool                          is_inited_;
};

/*
void* HashAlloc::alloc(int64_t sz)
{
  void* p = NULL;
  int ret = OB_SUCCESS;
  while(!ATOMIC_LOAD(&is_inited_)) {
    if (ATOMIC_BCAS(&size_, 0, sz)) {
      if (0 != (ret = alloc_.init(sz))) {
        ATOMIC_STORE(&size_, 0);
        STORAGE_LOG(WARN, "init small_allocator fail", K(sz));
        break;
      } else {
        ATOMIC_STORE(&is_inited_, true);
      }
    }
  }
  if (OB_SUCC(ret)) {
    p = alloc_.alloc();
  }
  return p;
}

void HashAlloc::free(void *p)
{
  alloc_.free(p);
}

void *ArrayAlloc::alloc(int64_t sz)
{
  return ob_malloc(sz);
}

void ArrayAlloc::free(void *p)
{
  ob_free(p);
}
*/

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::DefaultValueReclaimCallback::init(IValueAlloc *alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc)) {
    COMMON_LOG(ERROR, "invalid argument", KP(alloc));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    alloc_ = alloc;
  }
  return ret;
}

template<typename Key, typename Value>
void ObConcurrentHashMapWithHazardValue<Key, Value *>::DefaultValueReclaimCallback::reclaim_value(Value *value)
{
  if (OB_ISNULL(alloc_)) {
    COMMON_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "DefaultValueReclaimCallback wrong status", KP(alloc_));
  } else {
    alloc_->free(value);
    value = NULL;
  }
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::HazardPtrReclaimCallback::init(IValueReclaimCallback *value_reclaim_callback)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value_reclaim_callback)) {
    COMMON_LOG(ERROR, "invalid argument", K(value_reclaim_callback));
    ret = OB_INVALID_ARGUMENT;
  } else {
    value_reclaim_callback_ = value_reclaim_callback;
  }
  return ret;
}

template<typename Key, typename Value>
void ObConcurrentHashMapWithHazardValue<Key, Value *>::HazardPtrReclaimCallback::reclaim_ptr(uintptr_t ptr)
{
  if (OB_ISNULL(value_reclaim_callback_)) {
    COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "HazardPtrReclaimCallback wrong status", K(value_reclaim_callback_));
  } else {
    value_reclaim_callback_->reclaim_value(reinterpret_cast<Value *>(ptr));
  }
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::HashReclaimCallback::init(ObHazardPointer *hazard_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hazard_ptr)) {
    COMMON_LOG(ERROR, "invalid argument", KP(hazard_ptr));
    ret = OB_INVALID_ARGUMENT;
  } else {
    hazard_ptr_ = hazard_ptr;
  }
  return ret;
}

template<typename Key, typename Value>
void ObConcurrentHashMapWithHazardValue<Key, Value *>::HashReclaimCallback::reclaim_key_value(Key &key,
    Value *&value)
{
  UNUSED(key);
  if (OB_ISNULL(hazard_ptr_)) {
    COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "HashReclaimCallback status error", KP(hazard_ptr_));
  } else {
    int ret = hazard_ptr_->retire(reinterpret_cast<uintptr_t>(value));
    if (OB_SUCCESS != ret) {
      COMMON_LOG(ERROR, "retire ptr error", K(value), K(ret));
    }
  }
}

template<typename Key, typename Value>
ObConcurrentHashMapWithHazardValue<Key, Value *>::ObConcurrentHashMapWithHazardValue()
  :
    hazard_pointer_(),
    hazard_ptr_reclaim_callback_(),
    value_alloc_(NULL),
    value_reclaim_callback_(NULL),
    default_value_alloc_(),
    default_value_reclaim_callback_(),
    hash_alloc_(),
    array_alloc_(),
    reclaim_callback_(),
    hash_(hash_alloc_, array_alloc_, reclaim_callback_, 1<<16),
    hash_root_(),
    is_inited_(false)
{
}

template<typename Key, typename Value>
ObConcurrentHashMapWithHazardValue<Key, Value *>::ObConcurrentHashMapWithHazardValue(IHashAlloc &hash_alloc,
    IArrayAlloc &array_alloc)
  :
    hazard_pointer_(),
    hazard_ptr_reclaim_callback_(),
    value_alloc_(NULL),
    value_reclaim_callback_(NULL),
    default_value_alloc_(),
    hash_alloc_(),
    array_alloc_(),
    reclaim_callback_(),
    hash_(hash_alloc, array_alloc, reclaim_callback_, 1<<16),
    hash_root_(),
    default_value_reclaim_callback_(),
    is_inited_(false)
{
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::init()
{
  return init(&default_value_alloc_, &default_value_reclaim_callback_);
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::init(IValueAlloc *value_alloc)
{
  return init(value_alloc, &default_value_reclaim_callback_);
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::init(IValueAlloc *value_alloc, IValueReclaimCallback *value_reclaim_callback)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    COMMON_LOG(ERROR, "ObConcurrentHashMapWithHazardValue has inited_", K(this));
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(value_alloc) || OB_ISNULL(value_reclaim_callback)) {
    COMMON_LOG(ERROR, "invalid arguments", KP(value_alloc), KP(value_reclaim_callback));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(default_value_reclaim_callback_.init(value_alloc))) {
    COMMON_LOG(ERROR, "default_value_reclaim_callback_ init error", K(ret), K(value_alloc));
  } else if (OB_FAIL(hazard_ptr_reclaim_callback_.init(value_reclaim_callback))) {
    COMMON_LOG(ERROR, "hazard_ptr_reclaim_callback_ init error", K(ret), K(value_reclaim_callback));
  } else if (OB_FAIL(hazard_pointer_.init(&hazard_ptr_reclaim_callback_))) {
    COMMON_LOG(ERROR, "hazard_pointer_ init error", K(ret), K(&hazard_ptr_reclaim_callback_));
  } else if (OB_FAIL(reclaim_callback_.init(&hazard_pointer_))) {
    COMMON_LOG(ERROR, "reclaim_callback_ init error", K(&hazard_pointer_));
  } else {
    value_alloc_ = value_alloc;
    value_reclaim_callback_ = value_reclaim_callback;
    is_inited_ = true;
  }
  return ret;
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::put_refactored(const Key &key, Value *value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init())) {
  } else {
    int hash_ret = 0;
    SPutHandle handle(hash_, hash_root_);
    while (-EAGAIN == (hash_ret = hash_.insert(handle, key, value))) {
      // empty
    }
    ret = err_code_map(hash_ret);
  }
  return ret;
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::remove_refactored(const Key &key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init())) {
  } else {
    int hash_ret = 0;
    Value *v = NULL;
    SPutHandle handle(hash_, hash_root_);
    while (-EAGAIN == (hash_ret = hash_.del(handle, key, v))) {
      // empty
    }
    ret = err_code_map(hash_ret);
  }
  return ret;
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::create_refactored(const Key &key, Value *&value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init())) {
  } else {
    int hash_ret = 0;
    Value *v = value_alloc_->alloc();
    if (OB_ISNULL(v)) {
      COMMON_LOG(ERROR, "alloc value error");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      SPutHandle handle(hash_, hash_root_);
      while (-EAGAIN == (hash_ret = hash_.insert(handle, key, v))) {
        // empty
      }
      ret = err_code_map(hash_ret);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(hazard_pointer_.protect(reinterpret_cast<uintptr_t>(v)))) {
          COMMON_LOG(ERROR, "hazard_pointer protect error", K(ret), K(v));
          Value *tmpv = NULL;
          while (-EAGAIN == (hash_.del(handle, key, tmpv))) {
            // empty
          }
        } else {
          value = v;
        }
      }
      if (OB_FAIL(ret)) {
        value_alloc_->free(v);
        v = NULL;
      }
    }
  }
  return ret;
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::get_refactored(const Key &key, Value *&value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init())) {
  } else {
    Value *v = NULL;
    int hash_ret = 0;
    SGetHandle handle(hash_, hash_root_);
    while (-EAGAIN == (hash_ret = hash_.get(handle, key, v))) {
      // empty
    }
    ret = err_code_map(hash_ret);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(hazard_pointer_.protect(reinterpret_cast<uintptr_t>(v)))) {
        COMMON_LOG(ERROR, "hazard_pointer protect error", K(ret), K(value));
      } else {
        value = v;
      }
    }
  }
  return ret;
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::revert_value(const Value *value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init())) {
  } else if (OB_ISNULL(value)) {
    COMMON_LOG(ERROR, "invalid argument", K(value));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(hazard_pointer_.release(reinterpret_cast<uintptr_t>(value)))) {
    COMMON_LOG(ERROR, "hazard_pointer_ release error", K(ret), K(value));
  } else {
    // empty
  }
  return ret;
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::contains_key(const Key &key) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init())) {
  } else {
    int hash_ret = 0;
    Value *v = NULL;
    SGetHandle handle(const_cast<SHash&>(hash_), const_cast<SHashRoot&>(hash_root_));
    while (-EAGAIN == (hash_ret = const_cast<SHash&>(hash_).get(handle, key, v))) {
      // empty
    }
    hash_ret = 0 == hash_ret ? -EEXIST : hash_ret;
    ret = err_code_map(hash_ret);
  }
  return ret;
}

template <typename Key, typename Value>
template <typename Function>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::for_each(Function &fn)
{
  int ret = OB_SUCCESS;
  int hash_ret = 0;
  SScanHandle handle(hash_, hash_root_);
  SScanIterator iter;
  while (-EAGAIN == (hash_ret = hash_.get_iter(handle, iter))) {
    // empty
  }
  if (0 != hash_ret) {
  } else {
    Key k;
    Value *v = NULL;
    while (OB_SUCCESS == (hash_ret = iter.next(k, v))) {
      fn(k, v);
    }
    if (-ENOENT != hash_ret) {
      ret = err_code_map(hash_ret);
    }
  }
  return ret;
}

template <typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::get_count(int64_t &count)
{
  int ret = OB_SUCCESS;
  int hash_ret = 0;
  SScanHandle handle(hash_, hash_root_);
  SScanIterator iter;
  count = 0;
  while (-EAGAIN == (hash_ret = hash_.get_iter(handle, iter))) {
    // empty
  }
  if (0 != hash_ret) {
  } else {
    Key k;
    Value *v = NULL;
    while (OB_SUCCESS == (hash_ret = iter.next(k, v))) {
      count++;
    }
    if (-ENOENT != hash_ret) {
      ret = err_code_map(hash_ret);
    }
  }
  return ret;
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::err_code_map(int err) const
{
  int ret = OB_SUCCESS;
  switch (err) {
  case 0:           ret = OB_SUCCESS; break;
  case -ENOENT:     ret = OB_ENTRY_NOT_EXIST; break;
  case -EEXIST:     ret = OB_ENTRY_EXIST; break;
  case -ENOMEM:     ret = OB_ALLOCATE_MEMORY_FAILED; break;
  case -EOVERFLOW:  ret = OB_SIZE_OVERFLOW; break;
  default:          ret = OB_ERROR;
  }
  return ret;
}

template<typename Key, typename Value>
int ObConcurrentHashMapWithHazardValue<Key, Value *>::check_init() const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    COMMON_LOG(ERROR, "ObConcurrentHashMapWithHazardValue status error", K(this));
    ret = OB_NOT_INIT;
  }
  return ret;
}

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_HASH_OB_CONCURRENT_HASH_MAP_WITH_HAZARD_VALUE_
