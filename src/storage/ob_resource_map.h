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

#ifndef OCEANBASE_STORAGE_OB_RESOURCE_MAP_H_
#define OCEANBASE_STORAGE_OB_RESOURCE_MAP_H_

#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/lock/ob_bucket_lock.h"
#include "src/share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace common
{
class SpinRWLock;
class SpinRLockGuard;
class SpinWLockGuard;
}
namespace storage
{

template <typename Key, typename Value>
class ObResourceMap;

template <typename Value>
class ObResourceValueStore
{
public:
  ObResourceValueStore();
  virtual ~ObResourceValueStore();
  virtual int inc_ref_cnt();
  virtual int dec_ref_cnt(int64_t &ref_cnt);
  virtual int64_t get_ref_cnt() const;
  Value *get_value_ptr() const { return ptr_; }
  void set_value_ptr(Value *ptr) { ptr_ = ptr; }

  TO_STRING_KV(KPC_(ptr), K_(ref_cnt));

private:
  Value *ptr_;
  int64_t ref_cnt_;
};

template <typename Value>
ObResourceValueStore<Value>::ObResourceValueStore()
  : ptr_(NULL), ref_cnt_(0)
{
}

template <typename Value>
ObResourceValueStore<Value>::~ObResourceValueStore()
{
}

template <typename Value>
int ObResourceValueStore<Value>::inc_ref_cnt()
{
  int ret = common::OB_SUCCESS;
  ATOMIC_INC(&ref_cnt_);
  return ret;
}

template <typename Value>
int ObResourceValueStore<Value>::dec_ref_cnt(int64_t &ref_cnt)
{
  int ret = common::OB_SUCCESS;
  bool finished = false;
  int64_t old_val = ATOMIC_LOAD(&ref_cnt_);
  int64_t new_val = 0;
  while (!finished) {
    new_val = old_val - 1;
    ref_cnt = new_val;
    finished = (old_val == (new_val = ATOMIC_VCAS(&ref_cnt_, old_val, new_val)));
    old_val = new_val;
  }
  return ret;
}

template <typename Value>
int64_t ObResourceValueStore<Value>::get_ref_cnt() const
{
  return ATOMIC_LOAD(&ref_cnt_);
}

template <typename Value>
class ObResourceHandle
{
  template <typename, typename>
  friend class oceanbase::storage::ObResourceMap;
public:
  ObResourceHandle()
    : ptr_(NULL)
  {}
  virtual ~ObResourceHandle() {}
  Value *get_resource_ptr() const { return NULL == ptr_ ? NULL : ptr_->get_value_ptr(); }
  int64_t get_ref_cnt() const { return NULL == ptr_ ? 0 : ptr_->get_ref_cnt(); }
  virtual void reset() = 0;
protected:
  DISALLOW_COPY_AND_ASSIGN(ObResourceHandle);
  ObResourceValueStore<Value> *ptr_;
};

template <typename Key, typename Value>
class ObResourceDefaultCallback
{
public:
  ObResourceDefaultCallback() {};
  virtual ~ObResourceDefaultCallback() {}
  int operator()(common::hash::HashMapPair<Key, Value*> &pair) { UNUSED(pair); return common::OB_SUCCESS; }
};

template <typename Key, typename Value, typename Callback>
class ObForeachCallbackAdaptor
{
public:
  ObForeachCallbackAdaptor(Callback &callback);
  virtual ~ObForeachCallbackAdaptor();
  int operator()(common::hash::HashMapPair<Key, ObResourceValueStore<Value>*> &pair);
private:
  Callback &callback_;
};

template <typename Key, typename Value, typename Callback>
ObForeachCallbackAdaptor<Key, Value, Callback>::ObForeachCallbackAdaptor(Callback &callback)
  : callback_(callback)
{
}

template <typename Key, typename Value, typename Callback>
ObForeachCallbackAdaptor<Key, Value, Callback>::~ObForeachCallbackAdaptor()
{
}

template <typename Key, typename Value, typename Callback>
int ObForeachCallbackAdaptor<Key, Value, Callback>::operator()(
    common::hash::HashMapPair<Key, ObResourceValueStore<Value> *> &pair)
{
  common::hash::HashMapPair<Key, Value *> adapter_pair;
  adapter_pair.first = pair.first;
  adapter_pair.second = pair.second->get_value_ptr();
  static_assert(std::is_same<decltype(callback_(*( common::hash::HashMapPair<Key, Value *>*)0)), int>::value,
      "hash table foreach callback format error");
  return callback_(adapter_pair);
}

template <typename Key, typename Value>
class ObResourceMap
{
public:
  ObResourceMap();
  virtual ~ObResourceMap();
  int init(const int64_t bucket_num, const ObMemAttr &attr,
      const int64_t total_limit, const int64_t hold_limit, const int64_t page_size);
  int init(const int64_t bucket_num, const ObMemAttr &attr, common::ObIAllocator &allocator);
  int get(const Key &key, ObResourceHandle<Value> &handle);
  template <typename Callback = ObResourceDefaultCallback<Key, Value>>
  int set(const Key &key, Value &value, Callback callback = ObResourceDefaultCallback<Key, Value>());
  template <typename Callback = ObResourceDefaultCallback<Key,Value>>
  int erase(const Key &key, Callback callback = ObResourceDefaultCallback<Key, Value>());
  template <typename Callback = ObResourceDefaultCallback<Key, Value>>
  int foreach(Callback &callback);
  void destroy();
  int dec_handle_ref(ObResourceValueStore<Value> *ptr);
  common::ObIAllocator &get_allocator() { return *allocator_; }
  int inc_handle_ref(ObResourceValueStore<Value> *ptr);
protected:
  int get_without_lock(
      const Key &key,
      ObResourceHandle<Value> &handle);
  void free_resource(ObResourceValueStore<Value> *ptr);
protected:
  typedef ObResourceValueStore<Value> ValueStore;
  typedef common::hash::ObHashMap<Key, ValueStore *, common::hash::NoPthreadDefendMode> MAP;
  MAP map_;
  common::ObConcurrentFIFOAllocator default_allocator_;
  common::ObIAllocator *allocator_;
  common::ObBucketLock bucket_lock_;
  common::hash::hash_func<Key> hash_func_;
  bool is_inited_;
};

template <typename Key, typename Value>
ObResourceMap<Key, Value>::ObResourceMap()
  : map_(), default_allocator_(), allocator_(&default_allocator_), bucket_lock_(), is_inited_(false)
{
}

template <typename Key, typename Value>
ObResourceMap<Key, Value>::~ObResourceMap()
{
}

template <typename Key, typename Value>
int ObResourceMap<Key, Value>::init(
    const int64_t bucket_num,
    const ObMemAttr &attr,
    const int64_t total_limit,
    const int64_t hold_limit,
    const int64_t page_size)
{
  int ret = common::OB_SUCCESS;
  const uint64_t tenant_id = attr.tenant_id_;
  const int64_t bkt_num = common::hash::cal_next_prime(bucket_num);
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObResourceMap has already been inited", K(ret));
  } else if (OB_UNLIKELY(bucket_num <= 0 || total_limit <= 0 || hold_limit <= 0 || page_size <= 0
      || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(bucket_num), K(total_limit), K(hold_limit),
        K(page_size), K(tenant_id));
  } else if (OB_FAIL(bucket_lock_.init(bkt_num, ObLatchIds::DEFAULT_BUCKET_LOCK, ObMemAttr(tenant_id, "ResourMapLock")))) {
    STORAGE_LOG(WARN, "fail to init bucket lock", K(ret), K(bkt_num));
  } else if (OB_FAIL(map_.create(bkt_num, attr, attr))) {
    STORAGE_LOG(WARN, "fail to create map", K(ret));
  } else if (OB_UNLIKELY(bkt_num != map_.bucket_count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "lock buckets isn't equal to map buckets, which could cause concurrency issues", K(ret),
        K(bkt_num), K(map_.bucket_count()));
  } else if (OB_FAIL(default_allocator_.init(page_size, "ResourceMap", tenant_id, total_limit))) {
    STORAGE_LOG(WARN, "fail to init allocator", K(ret));
  } else {
    default_allocator_.set_attr(attr);
    is_inited_ = true;
    STORAGE_LOG(INFO, "init resource map success", K(ret), K(attr), K(bkt_num));
  }
  return ret;
}

template <typename Key, typename Value>
int ObResourceMap<Key, Value>::init(const int64_t bucket_num, const ObMemAttr &attr, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = attr.tenant_id_;
  const int64_t bkt_num = common::hash::cal_next_prime(bucket_num);
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObResourceMap has already been inited", K(ret));
  } else if (OB_UNLIKELY(bucket_num <= 0 || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(bucket_num), K(tenant_id));
  } else if (OB_FAIL(bucket_lock_.init(bkt_num, ObLatchIds::DEFAULT_BUCKET_LOCK, ObMemAttr(tenant_id, "ResourMapLock")))) {
    STORAGE_LOG(WARN, "fail to init bucket lock", K(ret), K(bkt_num));
  } else if (OB_FAIL(map_.create(bkt_num, attr, attr))) {
    STORAGE_LOG(WARN, "fail to create map", K(ret));
  } else if (OB_UNLIKELY(bkt_num != map_.bucket_count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "lock buckets isn't equal to map buckets, which could cause concurrency issues", K(ret),
        K(bkt_num), K(map_.bucket_count()));
  } else {
    allocator_ = &allocator;
    is_inited_ = true;
    STORAGE_LOG(INFO, "init resource map success", K(ret), K(attr), K(bkt_num));
  }
  return ret;
}

template <typename Key, typename Value>
int ObResourceMap<Key, Value>::get(const Key &key, ObResourceHandle<Value> &handle)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  if (OB_FAIL(hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to do hash", K(ret));
  } else {
    common::ObBucketHashRLockGuard guard(bucket_lock_, hash_val);
    handle.reset();
    ret = get_without_lock(key, handle);
  }
  return ret;
}

template <typename Key,
          typename Value>
int ObResourceMap<Key, Value>::get_without_lock(
    const Key &key,
    ObResourceHandle<Value> &handle)
{
  int ret = common::OB_SUCCESS;
  ValueStore *ptr = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObResourceMap has not been inited", K(ret));
  } else if (OB_FAIL(map_.get_refactored(key, ptr))) {
    if (common::OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get from map", K(ret), K(key));
    } else {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
  } else if (OB_FAIL(ptr->inc_ref_cnt())) {
    STORAGE_LOG(WARN, "fail to increase ref count", K(ret));
  } else {
    handle.ptr_ = ptr;
  }
  return ret;
}

template <typename Key, typename Value>
template <typename Callback>
int ObResourceMap<Key, Value>::set(const Key &key, Value &value, Callback callback)
{
  int ret = common::OB_SUCCESS;
  Value *ptr = NULL;
  ValueStore *value_store = NULL;
  const int64_t buf_size = value.get_deep_copy_size() + sizeof(ValueStore);
  char *buf = NULL;
  uint64_t hash_val = 0;
  if (OB_FAIL(hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to do hash", K(ret));
  } else {
    lib::ObMemAttr attr(MTL_ID(), "ResourceMapSet");
    common::ObBucketHashWLockGuard guard(bucket_lock_, hash_val);
    if (OB_UNLIKELY(!is_inited_)) {
      ret = common::OB_NOT_INIT;
      STORAGE_LOG(WARN, "ObResourceMap has not been inited", K(ret));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(buf_size, attr)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret), K(buf_size));
    } else {
      value_store = new (buf) ValueStore();
      if (OB_FAIL(value.deep_copy(buf + sizeof(ValueStore), buf_size - sizeof(ValueStore), ptr))) {
        STORAGE_LOG(WARN, "fail to deep copy value", K(ret));
      } else {
        value_store->set_value_ptr(ptr);
        common::hash::HashMapPair<Key, Value *> pair;
        pair.first = key;
        pair.second = value_store->get_value_ptr();
        if (OB_FAIL(callback(pair))) {
          STORAGE_LOG(WARN, "fail to callback", K(ret));
        } else if (OB_FAIL(inc_handle_ref(value_store))) {
          STORAGE_LOG(WARN, "fail to inc handle reference count", K(ret));
        } else if (OB_FAIL(map_.set_refactored(key, value_store))) {
          STORAGE_LOG(WARN, "fail to set to map", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret) && NULL != buf) {
    free_resource(value_store);
  }
  return ret;
}

template <typename Key, typename Value>
template <typename Callback>
int ObResourceMap<Key, Value>::erase(const Key &key, Callback callback)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  if (OB_FAIL(hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to do hash", K(ret));
  } else {
    common::ObBucketHashWLockGuard guard(bucket_lock_, hash_val);
    ValueStore *ptr = NULL;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = common::OB_NOT_INIT;
      STORAGE_LOG(WARN, "ObResourceMap has not been inited", K(ret));
    } else if (OB_FAIL(map_.get_refactored(key, ptr))) {
      STORAGE_LOG(WARN, "fail to get from map", K(ret));
    } else if (OB_FAIL(map_.erase_refactored(key))) {
      STORAGE_LOG(WARN, "fail to erase from map", K(ret));
    } else {
      common::hash::HashMapPair<Key, Value *> pair;
      pair.first = key;
      pair.second = ptr->get_value_ptr();
      if (OB_FAIL(callback(pair))) {
        STORAGE_LOG(WARN, "fail to callback", K(ret));
      } else if (OB_FAIL(dec_handle_ref(ptr))) {
        STORAGE_LOG(WARN, "fail to dec handle ref", K(ret));
      }
    }
  }
  return ret;
}

template <typename Key, typename Value>
template <typename Callback>
int ObResourceMap<Key, Value>::foreach(Callback &callback)
{
  int ret = common::OB_SUCCESS;
  ObForeachCallbackAdaptor<Key, Value, Callback> callback_adpator(callback);
  common::ObBucketWLockAllGuard guard(bucket_lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObResourceMap has not been inited", K(ret));
  } else if (OB_FAIL(map_.foreach_refactored(callback_adpator))) {
    STORAGE_LOG(WARN, "fail to foreach refactored", K(ret));
  }
  return ret;
}

template <typename Key, typename Value>
void ObResourceMap<Key, Value>::free_resource(ValueStore *value_store)
{
  if (NULL != value_store) {
    char *buf = reinterpret_cast<char *>(value_store);
    Value *ptr = value_store->get_value_ptr();
    value_store->~ValueStore();
    value_store = NULL;
    if (NULL != ptr) {
      ptr->~Value();
      ptr = NULL;
    }
    allocator_->free(buf);
    buf = NULL;
  }
}

template <typename Key, typename Value>
void ObResourceMap<Key, Value>::destroy()
{
  {
    common::ObBucketWLockAllGuard guard(bucket_lock_);
    typename MAP::iterator iter;
    for (iter = map_.begin(); iter != map_.end(); iter++) {
      ValueStore *value_store = iter->second;
      if (OB_NOT_NULL(value_store)) {
        STORAGE_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "exception: this value should be erased before.",
                K(*value_store->get_value_ptr()));
        free_resource(value_store);
      }
    }
  }
  bucket_lock_.destroy();
  map_.destroy();
  default_allocator_.destroy();
  allocator_ = &default_allocator_;
  is_inited_ = false;
}

template <typename Key, typename Value>
int ObResourceMap<Key, Value>::inc_handle_ref(ValueStore *ptr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObResourceMap has not been inited", K(ret));
  } else if (OB_ISNULL(ptr)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(ptr));
  } else if (OB_FAIL(ptr->inc_ref_cnt())) {
    STORAGE_LOG(WARN, "fail to increase ref count", K(ret));
  }
  return ret;
}

template <typename Key, typename Value>
int ObResourceMap<Key, Value>::dec_handle_ref(ValueStore *ptr)
{
  int ret = common::OB_SUCCESS;
  int64_t ref_cnt = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObResourceMap has not been inited", K(ret));
  } else if (OB_ISNULL(ptr)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(ptr));
  } else if (OB_FAIL(ptr->dec_ref_cnt(ref_cnt))) {
    STORAGE_LOG(WARN, "fail to decrease ref count", K(ret));
  } else if (0 == ref_cnt) {
    free_resource(ptr);
  }
  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_RESOURCE_MAP_H_
