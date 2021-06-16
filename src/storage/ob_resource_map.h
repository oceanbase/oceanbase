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

#include "lib/lock/ob_spin_rwlock.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase {
namespace common {
class SpinRWLock;
class SpinRLockGuard;
class SpinWLockGuard;
}  // namespace common
namespace storage {

template <typename Key, typename Value>
class ObResourceMap;

template <typename Value>
class ObResourceValueStore {
public:
  ObResourceValueStore();
  virtual ~ObResourceValueStore();
  virtual int inc_ref_cnt();
  virtual int dec_ref_cnt(int64_t& ref_cnt);
  virtual int64_t get_ref_cnt() const;
  Value* get_value_ptr() const
  {
    return ptr_;
  }
  void set_value_ptr(Value* ptr)
  {
    ptr_ = ptr;
  }

private:
  Value* ptr_;
  int64_t ref_cnt_;
};

template <typename Value>
ObResourceValueStore<Value>::ObResourceValueStore() : ptr_(NULL), ref_cnt_(0)
{}

template <typename Value>
ObResourceValueStore<Value>::~ObResourceValueStore()
{}

template <typename Value>
int ObResourceValueStore<Value>::inc_ref_cnt()
{
  int ret = common::OB_SUCCESS;
  ATOMIC_INC(&ref_cnt_);
  return ret;
}

template <typename Value>
int ObResourceValueStore<Value>::dec_ref_cnt(int64_t& ref_cnt)
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
class ObResourceHandle {
  template <typename, typename>
  friend class oceanbase::storage::ObResourceMap;

public:
  ObResourceHandle() : ptr_(NULL)
  {}
  virtual ~ObResourceHandle()
  {}
  Value* get_resource_ptr() const
  {
    return NULL == ptr_ ? NULL : ptr_->get_value_ptr();
  }
  virtual void reset() = 0;

protected:
  DISALLOW_COPY_AND_ASSIGN(ObResourceHandle);
  ObResourceValueStore<Value>* ptr_;
};

template <typename Key, typename Value>
class ObResourceDefaultCallback {
public:
  ObResourceDefaultCallback(){};
  virtual ~ObResourceDefaultCallback()
  {}
  int operator()(common::hash::HashMapPair<Key, Value*>& pair)
  {
    UNUSED(pair);
    return common::OB_SUCCESS;
  }
};

template <typename Key, typename Value, typename Callback>
class ObForeachCallbackAdaptor {
public:
  ObForeachCallbackAdaptor(Callback& callback);
  virtual ~ObForeachCallbackAdaptor();
  int operator()(common::hash::HashMapPair<Key, ObResourceValueStore<Value>*>& pair);

private:
  Callback& callback_;
};

template <typename Key, typename Value, typename Callback>
ObForeachCallbackAdaptor<Key, Value, Callback>::ObForeachCallbackAdaptor(Callback& callback) : callback_(callback)
{}

template <typename Key, typename Value, typename Callback>
ObForeachCallbackAdaptor<Key, Value, Callback>::~ObForeachCallbackAdaptor()
{}

template <typename Key, typename Value, typename Callback>
int ObForeachCallbackAdaptor<Key, Value, Callback>::operator()(
    common::hash::HashMapPair<Key, ObResourceValueStore<Value>*>& pair)
{
  common::hash::HashMapPair<Key, Value*> adapter_pair;
  adapter_pair.first = pair.first;
  adapter_pair.second = pair.second->get_value_ptr();
  return callback_(adapter_pair);
}

template <typename Key, typename Value>
class ObResourceMap {
public:
  explicit ObResourceMap();
  virtual ~ObResourceMap();
  int init(const int64_t bucket_num, const char* label, const int64_t total_limit, const int64_t hold_limit,
      const int64_t page_size);
  template <typename Callback = ObResourceDefaultCallback<Key, Value>>
  int get(const Key& key, ObResourceHandle<Value>& handle, Callback callback = ObResourceDefaultCallback<Key, Value>());
  template <typename Callback = ObResourceDefaultCallback<Key, Value>>
  int set(const Key& key, Value& value, Callback callback = ObResourceDefaultCallback<Key, Value>());
  template <typename Callback = ObResourceDefaultCallback<Key, Value>>
  int erase(const Key& key, Callback callback = ObResourceDefaultCallback<Key, Value>());
  template <typename Callback = ObResourceDefaultCallback<Key, Value>>
  int foreach (Callback& callback);
  void destroy();
  int dec_handle_ref(ObResourceValueStore<Value>* ptr);
  common::ObIAllocator& get_allocator()
  {
    return allocator_;
  }

private:
  int inc_handle_ref(ObResourceValueStore<Value>* ptr);
  void free_resource(ObResourceValueStore<Value>* ptr);

private:
  typedef ObResourceValueStore<Value> ValueStore;
  typedef common::hash::ObHashMap<Key, ValueStore*, common::hash::NoPthreadDefendMode> MAP;
  MAP map_;
  common::ObConcurrentFIFOAllocator allocator_;
  common::SpinRWLock lock_;
  bool is_inited_;
};

template <typename Key, typename Value>
ObResourceMap<Key, Value>::ObResourceMap() : map_(), allocator_(), lock_(), is_inited_(false)
{}

template <typename Key, typename Value>
ObResourceMap<Key, Value>::~ObResourceMap()
{}

template <typename Key, typename Value>
int ObResourceMap<Key, Value>::init(const int64_t bucket_num, const char* label, const int64_t total_limit,
    const int64_t hold_limit, const int64_t page_size)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObResourceMap has already been inited", K(ret));
  } else if (OB_UNLIKELY(bucket_num <= 0 || total_limit <= 0 || hold_limit <= 0 || page_size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(bucket_num), K(total_limit), K(hold_limit), K(page_size));
  } else if (OB_FAIL(map_.create(bucket_num, label))) {
    STORAGE_LOG(WARN, "fail to create map", K(ret));
  } else if (OB_FAIL(allocator_.init(total_limit, hold_limit, page_size))) {
    STORAGE_LOG(WARN, "fail to init allocator", K(ret));
  } else {
    allocator_.set_label(label);
    is_inited_ = true;
  }
  return ret;
}

template <typename Key, typename Value>
template <typename Callback>
int ObResourceMap<Key, Value>::get(const Key& key, ObResourceHandle<Value>& handle, Callback callback)
{
  int ret = common::OB_SUCCESS;
  common::SpinRLockGuard guard(lock_);
  ValueStore* ptr = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObResourceMap has not been inited", K(ret));
  } else if (OB_FAIL(map_.get_refactored(key, ptr))) {
    if (common::OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get from map", K(ret), K(key));
    } else {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
  } else {
    common::hash::HashMapPair<Key, Value*> pair;
    pair.first = key;
    pair.second = ptr->get_value_ptr();
    if (OB_FAIL(callback(pair))) {
      STORAGE_LOG(WARN, "fail to callback", K(ret));
    } else if (OB_FAIL(inc_handle_ref(ptr))) {
      STORAGE_LOG(WARN, "fail to inc handle ref count", K(ret));
    } else {
      handle.ptr_ = ptr;
    }
  }
  return ret;
}

template <typename Key, typename Value>
template <typename Callback>
int ObResourceMap<Key, Value>::set(const Key& key, Value& value, Callback callback)
{
  int ret = common::OB_SUCCESS;
  Value* ptr = NULL;
  ValueStore* value_store = NULL;
  const int64_t buf_size = value.get_deep_copy_size() + sizeof(ValueStore);
  char* buf = NULL;
  common::SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObResourceMap has not been inited", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(buf_size)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret), K(buf_size));
  } else {
    value_store = new (buf) ValueStore();
    if (OB_FAIL(value.deep_copy(buf + sizeof(ValueStore), buf_size - sizeof(ValueStore), ptr))) {
      STORAGE_LOG(WARN, "fail to deep copy value", K(ret));
    } else {
      value_store->set_value_ptr(ptr);
      common::hash::HashMapPair<Key, Value*> pair;
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

  if (OB_FAIL(ret) && NULL != buf) {
    free_resource(value_store);
  }
  return ret;
}

template <typename Key, typename Value>
template <typename Callback>
int ObResourceMap<Key, Value>::erase(const Key& key, Callback callback)
{
  int ret = common::OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  ValueStore* ptr = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObResourceMap has not been inited", K(ret));
  } else if (OB_FAIL(map_.get_refactored(key, ptr))) {
    STORAGE_LOG(WARN, "fail to get from map", K(ret));
  } else if (OB_FAIL(map_.erase_refactored(key))) {
    STORAGE_LOG(WARN, "fail to erase from map", K(ret));
  } else {
    common::hash::HashMapPair<Key, Value*> pair;
    pair.first = key;
    pair.second = ptr->get_value_ptr();
    if (OB_FAIL(callback(pair))) {
      STORAGE_LOG(WARN, "fail to callback", K(ret));
    } else if (OB_FAIL(dec_handle_ref(ptr))) {
      STORAGE_LOG(WARN, "fail to dec handle ref", K(ret));
    }
  }
  return ret;
}

template <typename Key, typename Value>
template <typename Callback>
int ObResourceMap<Key, Value>::foreach (Callback& callback)
{
  int ret = common::OB_SUCCESS;
  ObForeachCallbackAdaptor<Key, Value, Callback> callback_adpator(callback);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObResrouceMap has not been inited", K(ret));
  } else if (OB_FAIL(map_.foreach_refactored(callback_adpator))) {
    STORAGE_LOG(WARN, "fail to foreach refactored", K(ret));
  }
  return ret;
}

template <typename Key, typename Value>
void ObResourceMap<Key, Value>::free_resource(ValueStore* value_store)
{
  if (NULL != value_store) {
    char* buf = reinterpret_cast<char*>(value_store);
    Value* ptr = value_store->get_value_ptr();
    value_store->~ValueStore();
    value_store = NULL;
    if (NULL != ptr) {
      ptr->~Value();
      ptr = NULL;
    }
    allocator_.free(buf);
    buf = NULL;
  }
}

template <typename Key, typename Value>
void ObResourceMap<Key, Value>::destroy()
{
  common::SpinWLockGuard guard(lock_);
  typename MAP::iterator iter;
  for (iter = map_.begin(); iter != map_.end(); iter++) {
    ValueStore* value_store = iter->second;
    if (OB_NOT_NULL(value_store)) {
      STORAGE_LOG(WARN, "exception: this value should be erased before.", K(*value_store->get_value_ptr()));
      free_resource(value_store);
    }
  }
  map_.destroy();
  allocator_.destroy();
  is_inited_ = false;
}

template <typename Key, typename Value>
int ObResourceMap<Key, Value>::inc_handle_ref(ValueStore* ptr)
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
int ObResourceMap<Key, Value>::dec_handle_ref(ValueStore* ptr)
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
