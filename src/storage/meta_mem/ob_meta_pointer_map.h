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

#ifndef OCEANBASE_STORAGE_OB_META_POINTER_MAP_H_
#define OCEANBASE_STORAGE_OB_META_POINTER_MAP_H_

#include "lib/allocator/page_arena.h"
#include "lib/stat/ob_diagnose_info.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_meta_pointer.h"
#include "storage/ob_resource_map.h"

namespace oceanbase
{
namespace storage
{
template <typename Key, typename T>
class ObMetaPointerHandle;

template <typename Key, typename T>
class ObMetaPointerMap : public ObResourceMap<Key, ObMetaPointer<T>>
{
public:
  typedef ObResourceMap<Key, ObMetaPointer<T>> ResourceMap;
  int erase(const Key &key, ObMetaObjGuard<T> &guard);
  int exist(const Key &key, bool &is_exist);
  int get_meta_obj(const Key &key, ObMetaObjGuard<T> &guard);
  int get_meta_obj_with_external_memory(
      const Key &key,
      common::ObArenaAllocator &allocator,
      ObMetaObjGuard<T> &guard,
      const bool force_alloc_new = false);
  int try_get_in_memory_meta_obj(const Key &key, bool &success, ObMetaObjGuard<T> &guard);
  int try_get_in_memory_meta_obj_and_addr(
      const Key &key,
      ObMetaDiskAddr &addr,
      ObMetaObjGuard<T> &guard);
  int get_meta_addr(const Key &key, ObMetaDiskAddr &addr);
  int set_meta_obj(const Key &key, ObMetaObjGuard<T> &guard);
  int set_attr_for_obj(const Key &key, ObMetaObjGuard<T> &guard);
  int compare_and_swap_addr_and_object(
      const Key &key,
      const ObMetaDiskAddr &new_addr,
      const ObMetaObjGuard<T> &old_guard,
      ObMetaObjGuard<T> &new_guard);
  // TIPS:
  //  - only compare and swap pure address, but no reset object.
  // only used for replay and compat, others mustn't call this func
  int compare_and_swap_address_without_object(
      const Key &key,
      const ObMetaDiskAddr &old_addr,
      const ObMetaDiskAddr &new_addr,
      const bool set_pool /* whether to set pool */,
      ObITenantMetaObjPool *pool);
  template <typename Operator> int for_each_value_store(Operator &op);
  int wash_meta_obj(const Key &key, ObMetaObjGuard<ObTablet> &guard, void *&free_obj);
  int64_t count() const { return ResourceMap::map_.size(); }

private:
  // used when tablet object and memory is hold by external allocator
  int load_meta_obj(
      const Key &key,
      ObMetaPointer<T> *meta_pointer,
      common::ObArenaAllocator &allocator,
      ObMetaDiskAddr &load_addr,
      T *t);
  // used when tablet object and memory is hold by t3m
  int load_meta_obj(
      const Key &key,
      ObMetaPointer<T> *meta_pointer,
      ObMetaDiskAddr &load_addr,
      T *&t);
  int load_and_hook_meta_obj(const Key &key, ObMetaPointerHandle<Key, T> &ptr_hdl, ObMetaObjGuard<T> &guard);
  int try_get_in_memory_meta_obj(
      const Key &key,
      ObMetaPointerHandle<Key, T> &ptr_hdl,
      ObMetaObjGuard<T> &guard,
      bool &is_in_memory);
  int inner_erase(const Key &key);

public:
  using ObResourceMap<Key, ObMetaPointer<T>>::ObResourceMap;
};

template <typename Key, typename T>
class ObMetaPointerHandle : public ObResourceHandle<ObMetaPointer<T>>
{
public:
  ObMetaPointerHandle();
  explicit ObMetaPointerHandle(ObMetaPointerMap<Key, T> &map);
  ObMetaPointerHandle(
      ObResourceValueStore<ObMetaPointer<T>> *ptr,
      ObMetaPointerMap<Key, T> *map);
  virtual ~ObMetaPointerHandle();

public:
  virtual void reset() override;
  bool is_valid() const;
  int assign(const ObMetaPointerHandle<Key, T> &other);

  TO_STRING_KV("ptr", ObResourceHandle<ObMetaPointer<T>>::ptr_, KP_(map));
private:
  int set(
      ObResourceValueStore<ObMetaPointer<T>> *ptr,
      ObMetaPointerMap<Key, T> *map);

private:
  ObMetaPointerMap<Key, T> *map_;

  DISALLOW_COPY_AND_ASSIGN(ObMetaPointerHandle);
};

template <typename Key, typename T>
ObMetaPointerHandle<Key, T>::ObMetaPointerHandle()
  : ObResourceHandle<ObMetaPointer<T>>::ObResourceHandle(),
    map_(nullptr)
{
}

template <typename Key, typename T>
ObMetaPointerHandle<Key, T>::ObMetaPointerHandle(ObMetaPointerMap<Key, T> &map)
    : ObResourceHandle<ObMetaPointer<T>>::ObResourceHandle(),
      map_(&map)
{
}

template <typename Key, typename T>
ObMetaPointerHandle<Key, T>::ObMetaPointerHandle(
    ObResourceValueStore<ObMetaPointer<T>> *ptr,
    ObMetaPointerMap<Key, T> *map)
    : ObResourceHandle<ObMetaPointer<T>>::ObResourceHandle(),
      map_(map)
{
  abort_unless(common::OB_SUCCESS == set(ptr, map));
}

template <typename Key, typename T>
ObMetaPointerHandle<Key, T>::~ObMetaPointerHandle()
{
  reset();
}

template <typename Key, typename T>
void ObMetaPointerHandle<Key, T>::reset()
{
  int ret = common::OB_SUCCESS;
  if (nullptr != ObResourceHandle<ObMetaPointer<T>>::ptr_) {
    if (nullptr == map_) {
      STORAGE_LOG(ERROR, "map is null", K(ret), KP_(map));
    } else if (OB_FAIL(map_->dec_handle_ref(ObResourceHandle<ObMetaPointer<T>>::ptr_))) {
      STORAGE_LOG(WARN, "fail to decrease handle reference count", K(ret));
    } else {
      ObResourceHandle<ObMetaPointer<T>>::ptr_ = nullptr;
    }
  }
}

template <typename Key, typename T>
bool ObMetaPointerHandle<Key, T>::is_valid() const
{
  return nullptr != ObResourceHandle<ObMetaPointer<T>>::ptr_
      && nullptr != ObResourceHandle<ObMetaPointer<T>>::ptr_->get_value_ptr()
      && nullptr != map_;
}

template <typename Key, typename T>
int ObMetaPointerHandle<Key, T>::assign(const ObMetaPointerHandle<Key, T> &other)
{
  int ret = common::OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(set(other.ptr_, other.map_))) {
      STORAGE_LOG(WARN, "failed to set member", K(ret), K(other));
    }
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerHandle<Key, T>::set(
    ObResourceValueStore<ObMetaPointer<T>> *ptr,
    ObMetaPointerMap<Key, T> *map)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(ptr) || OB_ISNULL(map)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(ptr), KP(map));
  } else {
    reset();
    if (OB_FAIL(map->inc_handle_ref(ptr))) {
      STORAGE_LOG(WARN, "fail to inc tablet poiner", K(ret), KP(ptr), KP(map));
    } else {
      ObResourceHandle<ObMetaPointer<T>>::ptr_ = ptr;
      map_ = map;
    }
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::erase(const Key &key, ObMetaObjGuard<T> &guard)
{
  int ret = common::OB_SUCCESS;
  bool need_erase = false;
  if (OB_UNLIKELY(!ResourceMap::is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObResourceMap has not been inited", K(ret));
  } else if (OB_SUCC(get_meta_obj(key, guard))) { // make sure load object to memory to release any reference count.
    need_erase = true;
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;  // ignore ret error, may be creating failure or has been erased.
  } else if (OB_ITEM_NOT_SETTED == ret) {
    need_erase = true;
    ret = OB_SUCCESS;  // ignore ret error, may be creating failure.
  } else {
    STORAGE_LOG(WARN, "fail to get meta obj", K(ret), K(key));
  }
  if (OB_SUCC(ret) && need_erase) {
    if (OB_FAIL(inner_erase(key))) {
      STORAGE_LOG(WARN, "fail to erase meta pointer", K(ret), K(key));
    }
  }
  STORAGE_LOG(DEBUG, "erase", K(ret), K(need_erase), K(guard), KPC(guard.get_obj()));
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::inner_erase(const Key &key)
{
  int ret = common::OB_SUCCESS;
  ObResourceValueStore<ObMetaPointer<T>> *ptr = NULL;
  uint64_t hash_val = 0;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else {
    common::ObBucketHashWLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::map_.get_refactored(key, ptr))) {
      STORAGE_LOG(WARN, "fail to get from map", K(ret));
    } else if (OB_FAIL(ResourceMap::map_.erase_refactored(key))) {
      STORAGE_LOG(WARN, "fail to erase from map", K(ret));
    } else {
      ObMetaPointer<T> *value = ptr->get_value_ptr();
      value->reset_obj();
      if (OB_FAIL(ResourceMap::dec_handle_ref(ptr))) {
        STORAGE_LOG(WARN, "fail to dec handle ref", K(ret));
      }
    }
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::exist(const Key &key, bool &is_exist)
{
  int ret = common::OB_SUCCESS;
  ObMetaPointerHandle<Key, T> ptr_hdl(*this);
  ObMetaPointer<T> *t_ptr = nullptr;
  is_exist = false;
  uint64_t hash_val = 0;
  if (OB_UNLIKELY(!ResourceMap::is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObResourceMap has not been inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else {
    common::ObBucketHashRLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = common::OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "fail to get pointer handle", K(ret));
      }
    } else if (OB_ISNULL(t_ptr = ptr_hdl.get_resource_ptr())) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get meta pointer", K(ret), KP(t_ptr));
    } else {
      is_exist = true;
    }
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::try_get_in_memory_meta_obj_and_addr(
    const Key &key,
    ObMetaDiskAddr &addr,
    ObMetaObjGuard<T> &guard)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObMetaPointerHandle<Key, T> ptr_hdl(*this);
  ObMetaPointer<T> *t_ptr = nullptr;
  guard.reset();
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else { // read lock
    common::ObBucketHashRLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      if (common::OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get pointer handle", K(ret), K(key));
      }
    } else if (OB_ISNULL(t_ptr = ptr_hdl.get_resource_ptr())) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get meta pointer", K(ret), KP(t_ptr), K(key));
    } else if (t_ptr->is_in_memory() && OB_FAIL(t_ptr->get_in_memory_obj(guard))) {
      STORAGE_LOG(WARN, "fail to get meta object", K(ret), KP(t_ptr), K(key));
    } else {
      addr = t_ptr->get_addr();
    }
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::try_get_in_memory_meta_obj(
    const Key &key,
    bool &success,
    ObMetaObjGuard<T> &guard)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObMetaPointerHandle<Key, T> ptr_hdl(*this);
  ObMetaPointer<T> *t_ptr = nullptr;
  guard.reset();
  success = false;

  if (OB_UNLIKELY(!key.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else { // read lock
    common::ObBucketHashRLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      if (common::OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get pointer handle", K(ret), K(key));
      }
    } else if (OB_ISNULL(t_ptr = ptr_hdl.get_resource_ptr())) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get meta pointer", K(ret), KP(t_ptr), K(key));
    } else if (t_ptr->is_in_memory()) {
      if (OB_FAIL(t_ptr->get_in_memory_obj(guard))) {
        STORAGE_LOG(WARN, "fail to get meta object", K(ret), KP(t_ptr), K(key));
      } else {
        success = true;
      }
    }
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::try_get_in_memory_meta_obj(
    const Key &key,
    ObMetaPointerHandle<Key, T> &ptr_hdl,
    ObMetaObjGuard<T> &guard,
    bool &is_in_memory)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  ObMetaPointer<T> *t_ptr = nullptr;
  is_in_memory = false;
  if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else {
    common::ObBucketHashRLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      if (common::OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get pointer handle", K(ret));
      }
    } else if (OB_ISNULL(t_ptr = ptr_hdl.get_resource_ptr())) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get meta pointer", K(ret), KP(t_ptr), K(key));
    } else if (OB_UNLIKELY(t_ptr->get_addr().is_none())) {
      ret = OB_ITEM_NOT_SETTED;
      STORAGE_LOG(DEBUG, "pointer addr is none, no object to be got", K(ret), K(key), KPC(t_ptr));
    } else if (t_ptr->is_in_memory()) {
      t_ptr->get_obj(guard);
      is_in_memory = true;
    }
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::get_meta_obj(
    const Key &key,
    ObMetaObjGuard<T> &guard)
{
  int ret = common::OB_SUCCESS;
  ObMetaPointerHandle<Key, T> ptr_hdl(*this);
  bool is_in_memory = false;
  guard.reset();
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else if (OB_FAIL(try_get_in_memory_meta_obj(key, ptr_hdl, guard, is_in_memory))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_ITEM_NOT_SETTED == ret) {
      STORAGE_LOG(DEBUG, "meta obj does not exist", K(ret), K(key));
    } else {
      STORAGE_LOG(WARN, "fail to try get in memory meta obj", K(ret), K(key));
    }
  } else if (OB_UNLIKELY(!is_in_memory)) {
    if (OB_FAIL(load_and_hook_meta_obj(key, ptr_hdl, guard))) {
      STORAGE_LOG(WARN, "fail to load and hook meta obj", K(ret), K(key));
    } else {
      EVENT_INC(ObStatEventIds::TABLET_CACHE_MISS);
    }
  } else {
    EVENT_INC(ObStatEventIds::TABLET_CACHE_HIT);
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::load_and_hook_meta_obj(
    const Key &key,
    ObMetaPointerHandle<Key, T> &ptr_hdl,
    ObMetaObjGuard<T> &guard)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  ObMetaDiskAddr disk_addr;
  ObMetaPointer<T> *meta_pointer = ptr_hdl.get_resource_ptr();
  do {
    bool need_free_obj = false;
    T *t = nullptr;
    // Move load obj from disk out of the bucket lock, because
    // wash obj may acquire the bucket lock again, which cause dead lock.
    if (OB_FAIL(load_meta_obj(key, meta_pointer, disk_addr, t))) {
      STORAGE_LOG(WARN, "load obj from disk fail", K(ret), K(key), KPC(meta_pointer), K(lbt()));
    } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
      STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
    } else {
      ObMetaPointerHandle<Key, T> tmp_ptr_hdl(*this);
      {
        common::ObBucketHashWLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
        if (OB_FAIL(ResourceMap::get_without_lock(key, tmp_ptr_hdl))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            STORAGE_LOG(WARN, "fail to get pointer handle", K(ret));
          }
          need_free_obj = true;
        } else if (meta_pointer->is_in_memory()) {  // some other thread finish loading
          need_free_obj = true;
          if (OB_FAIL(meta_pointer->get_in_memory_obj(guard))) {
            STORAGE_LOG(WARN, "fail to get meta object", K(ret), KP(meta_pointer));
          }
        } else if (OB_UNLIKELY(disk_addr != meta_pointer->get_addr()
            || meta_pointer != tmp_ptr_hdl.get_resource_ptr()
            || meta_pointer->get_addr() != tmp_ptr_hdl.get_resource_ptr()->get_addr())) {
          ret = OB_ITEM_NOT_MATCH;
          need_free_obj = true;
          if (REACH_TIME_INTERVAL(1000000)) {
            STORAGE_LOG(WARN, "disk address or pointer change", K(ret), K(disk_addr), KPC(meta_pointer),
                KPC(tmp_ptr_hdl.get_resource_ptr()));
          }
        } else {
          if (OB_FAIL(meta_pointer->hook_obj(t, guard))) {
            STORAGE_LOG(WARN, "fail to hook object", K(ret), KP(meta_pointer));
          } else if (OB_FAIL(guard.get_obj()->assign_pointer_handle(ptr_hdl))) {
            STORAGE_LOG(WARN, "fail to assign pointer handle", K(ret));
          }
        }
      } // write lock end
      if (need_free_obj) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(meta_pointer->release_obj(t))) {
          STORAGE_LOG(ERROR, "fail to release object", K(ret), K(tmp_ret), KP(meta_pointer));
        } else if (meta_pointer != tmp_ptr_hdl.get_resource_ptr()) {
          meta_pointer = tmp_ptr_hdl.get_resource_ptr();
          if (OB_TMP_FAIL(ptr_hdl.assign(tmp_ptr_hdl))) {
            STORAGE_LOG(WARN, "fail to assign pointer handle", K(ret), K(tmp_ret), K(ptr_hdl), K(tmp_ptr_hdl));
          }
        }
      }
    }
  } while (OB_ITEM_NOT_MATCH == ret);
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::load_meta_obj(
    const Key &key,
    ObMetaPointer<T> *meta_pointer,
    common::ObArenaAllocator &allocator,
    ObMetaDiskAddr &load_addr,
    T *t)
{
  int ret = common::OB_SUCCESS;
  uint64_t  hash_val = 0;
  if (OB_UNLIKELY(!key.is_valid()) || OB_ISNULL(meta_pointer)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key), KP(meta_pointer));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else {
    common::ObArenaAllocator arena_allocator(common::ObMemAttr(MTL_ID(), "LoadMetaObj"));
    char *buf = nullptr;
    int64_t buf_len = 0;
    {
      common::ObBucketHashRLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
      ObMetaPointerHandle<Key, T> tmp_ptr_hdl(*this);
      // check whether the tablet has been deleted
      if (OB_FAIL(ResourceMap::get_without_lock(key, tmp_ptr_hdl))) {
        if (common::OB_ENTRY_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "fail to get pointer handle", K(ret), K(key));
        } else {
          STORAGE_LOG(INFO, "the tablet has been deleted", K(ret), K(key));
        }
      } else if (OB_FAIL(meta_pointer->read_from_disk(arena_allocator, buf, buf_len, load_addr))) {
        STORAGE_LOG(WARN, "fail to read from disk", K(ret), KPC(meta_pointer));
      } else {
        t->tablet_addr_ = load_addr;
        if (OB_FAIL(meta_pointer->deserialize(allocator, buf, buf_len, t))) {
          STORAGE_LOG(WARN, "fail to deserialize object", K(ret), K(key), KPC(meta_pointer));
        }
      }
    }
  }

  // this load_meta_obj is called when tablet memory hold by external allocator
  // let caller tackle failure, recycle object and memory,
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::load_meta_obj(
    const Key &key,
    ObMetaPointer<T> *meta_pointer,
    ObMetaDiskAddr &load_addr,
    T *&t)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  if (OB_UNLIKELY(!key.is_valid()) || OB_ISNULL(meta_pointer)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key), KP(meta_pointer));
  } else if (OB_FAIL(meta_pointer->acquire_obj(t))) {
    STORAGE_LOG(WARN, "fail to acquire object", K(ret), K(key), KPC(meta_pointer));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else {
    common::ObArenaAllocator arena_allocator(common::ObMemAttr(MTL_ID(), "LoadMetaObj"));
    char *buf = nullptr;
    int64_t buf_len = 0;
    {
      common::ObBucketHashRLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
      ObMetaPointerHandle<Key, T> tmp_ptr_hdl(*this);
      // check whether the tablet has been deleted
      if (OB_FAIL(ResourceMap::get_without_lock(key, tmp_ptr_hdl))) {
        if (common::OB_ENTRY_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "fail to get pointer handle", K(ret), K(key));
        } else {
          STORAGE_LOG(INFO, "the tablet has been deleted", K(ret), K(key));
        }
      } else if (OB_FAIL(meta_pointer->read_from_disk(arena_allocator, buf, buf_len, load_addr))) {
        STORAGE_LOG(WARN, "fail to read from disk", K(ret), KPC(meta_pointer));
      } else {
        t->tablet_addr_ = load_addr;
        if (OB_FAIL(meta_pointer->deserialize(buf, buf_len, t))) {
          STORAGE_LOG(WARN, "fail to deserialize object", K(ret), K(key), KPC(meta_pointer));
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(t)) {
    meta_pointer->release_obj(t);
    t = nullptr;
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::get_meta_obj_with_external_memory(
    const Key &key,
    common::ObArenaAllocator &allocator,
    ObMetaObjGuard<T> &guard,
    const bool force_alloc_new)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObMetaPointerHandle<Key, T> ptr_hdl(*this);
  ObMetaPointer<T> *t_ptr = nullptr;
  bool is_in_memory = false;
  guard.reset();
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else if (force_alloc_new) {
    common::ObBucketHashRLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      if (common::OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get pointer handle", K(ret));
      }
    }
  } else if (OB_FAIL(try_get_in_memory_meta_obj(key, ptr_hdl, guard, is_in_memory))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      STORAGE_LOG(DEBUG, "meta obj does not exist", K(ret), K(key));
    } else {
      STORAGE_LOG(WARN, "fail to try get in memory meta obj", K(ret), K(key));
    }
  } else if (is_in_memory) {
    EVENT_INC(ObStatEventIds::TABLET_CACHE_HIT);
  }
  if (OB_SUCC(ret) && !is_in_memory) {
    t_ptr = ptr_hdl.get_resource_ptr();
    ObMetaDiskAddr disk_addr;
    void *buf = allocator.alloc(sizeof(T));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret), KP(buf), "size of", sizeof(T));
    } else {
      bool need_free_obj = false;
      T *t = new (buf) T();
      do {
        t->reset();
        if (OB_FAIL(load_meta_obj(key, t_ptr, allocator, disk_addr, t))) {
          STORAGE_LOG(WARN, "load obj from disk fail", K(ret), K(key), KPC(t_ptr), K(lbt()));
        } else {
          ObMetaPointerHandle<Key, T> tmp_ptr_hdl(*this);
          common::ObBucketHashWLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
          // some other thread finish loading
          if (OB_FAIL(ResourceMap::get_without_lock(key, tmp_ptr_hdl))) {
            if (OB_ENTRY_NOT_EXIST != ret) {
              STORAGE_LOG(WARN, "fail to get pointer handle", K(ret));
            }
          } else if (!force_alloc_new && t_ptr->is_in_memory()) {
            if (OB_FAIL(t_ptr->get_in_memory_obj(guard))) {
              STORAGE_LOG(WARN, "fail to get meta object", K(ret), KP(t_ptr));
            } else {
              need_free_obj = true;
            }
          } else if (OB_UNLIKELY(disk_addr != t_ptr->get_addr()
              || t_ptr != tmp_ptr_hdl.get_resource_ptr()
              || t_ptr->get_addr() != tmp_ptr_hdl.get_resource_ptr()->get_addr())) {
            ret = OB_ITEM_NOT_MATCH;
            if (t_ptr != tmp_ptr_hdl.get_resource_ptr()) {
              t_ptr = tmp_ptr_hdl.get_resource_ptr();
              int tmp_ret = OB_SUCCESS;
              if (OB_TMP_FAIL(ptr_hdl.assign(tmp_ptr_hdl))) {
                STORAGE_LOG(WARN, "fail to assign pointer handle", K(ret), K(tmp_ret), K(ptr_hdl), K(tmp_ptr_hdl));
              }
            }
            if (REACH_TIME_INTERVAL(1000000)) {
              STORAGE_LOG(WARN, "disk address change", K(ret), K(disk_addr), KPC(t_ptr));
            }
          } else if (OB_FAIL(t->deserialize_post_work(allocator))) {
            STORAGE_LOG(WARN, "fail to deserialize post work", K(ret), KP(t));
          } else if (OB_FAIL(t->assign_pointer_handle(ptr_hdl))) {
            STORAGE_LOG(WARN, "fail to assign pointer handle", K(ret), KP(t));
          } else {
            ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
            guard.set_obj(t, &allocator, t3m);
          }
        }  // write lock end
        if ((OB_FAIL(ret) && OB_NOT_NULL(t)) || need_free_obj) {
          t->dec_macro_ref_cnt();
        }
      } while (OB_ITEM_NOT_MATCH == ret);
      if ((OB_FAIL(ret) && OB_NOT_NULL(t)) || need_free_obj) {
        t->~T();
        allocator.free(t);
        t = nullptr;
      }
    }
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::get_meta_addr(const Key &key, ObMetaDiskAddr &addr)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObMetaPointerHandle<Key, T> ptr_hdl(*this);
  ObMetaPointer<T> *t_ptr = nullptr;

  if (OB_UNLIKELY(!key.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else { // read lock
    common::ObBucketHashRLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      STORAGE_LOG(WARN, "fail to get pointer handle", K(ret), K(key));
    } else if (OB_ISNULL(t_ptr = ptr_hdl.get_resource_ptr())) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get meta pointer", K(ret), KP(t_ptr));
    } else {
      addr = t_ptr->get_addr();
    }
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::set_meta_obj(const Key &key, ObMetaObjGuard<T> &guard)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObMetaPointerHandle<Key, T> ptr_hdl(*this);
  ObMetaPointer<T> *t_ptr = nullptr;
  if (OB_UNLIKELY(!key.is_valid() || !guard.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key), K(guard));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else {
    common::ObBucketHashWLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      STORAGE_LOG(WARN, "fail to get pointer handle", K(ret));
    } else if (OB_ISNULL(t_ptr = ptr_hdl.get_resource_ptr())) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get meta pointer", K(ret), KP(t_ptr));
    } else {
      t_ptr->set_obj(guard);
    }
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::set_attr_for_obj(const Key &key, ObMetaObjGuard<T> &guard)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObMetaPointerHandle<Key, T> ptr_hdl(*this);
  ObMetaPointer<T> *t_ptr = nullptr;
  if (OB_UNLIKELY(!key.is_valid() || !guard.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key), K(guard));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else {
    common::ObBucketHashWLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      STORAGE_LOG(WARN, "fail to get pointer handle", K(ret));
    } else if (OB_ISNULL(t_ptr = ptr_hdl.get_resource_ptr())) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get meta pointer", K(ret), KP(t_ptr));
    } else {
      t_ptr->set_attr_for_obj(guard.get_obj());
    }
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::compare_and_swap_addr_and_object(
    const Key &key,
    const ObMetaDiskAddr &new_addr,
    const ObMetaObjGuard<T> &old_guard,
    ObMetaObjGuard<T> &new_guard)
{
  int ret = common::OB_SUCCESS;
  ObMetaPointerHandle<Key, T> ptr_hdl(*this);
  ObMetaPointer<T> *t_ptr = nullptr;
  ObMetaObjGuard<T> ptr_guard;
  uint64_t hash_val = 0;

  if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else {
    common::ObBucketHashWLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      if (common::OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "fail to get pointer handle", K(ret), K(key));
      }
    } else if (OB_ISNULL(t_ptr = ptr_hdl.get_resource_ptr())) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get meta pointer", K(ret), KP(t_ptr));
    } else if (OB_UNLIKELY(!t_ptr->is_in_memory())) {
      if (t_ptr->get_addr().is_disked()) {
        ret = common::OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "old object has changed, which is not allowed", K(ret), KP(t_ptr));
      } else {
        // first time CAS
        if (!t_ptr->get_addr().is_none() || old_guard.get_obj() != new_guard.get_obj()) {
          ret = common::OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "this is not the first time cas", K(ret),
            K(t_ptr->get_addr()), KP(old_guard.get_obj()), KP(new_guard.get_obj()));
        } else {
          ret = common::OB_SUCCESS;
        }
      }
    } else if (OB_FAIL(t_ptr->get_in_memory_obj(ptr_guard))) {
      STORAGE_LOG(WARN, "fail to get object", K(ret), KP(t_ptr));
    } else if (OB_UNLIKELY(ptr_guard.get_obj() != old_guard.get_obj())) {
      ret = common::OB_NOT_THE_OBJECT;
      STORAGE_LOG(WARN, "old object has changed", K(ret), KP(t_ptr));
    }

    if (OB_SUCC(ret)) {
      t_ptr->set_addr_with_reset_obj(new_addr);
      t_ptr->set_obj(new_guard);
    }
  }

  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::compare_and_swap_address_without_object(
    const Key &key,
    const ObMetaDiskAddr &old_addr,
    const ObMetaDiskAddr &new_addr,
    const bool set_pool /* whether to set pool */,
    ObITenantMetaObjPool *pool)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObMetaPointerHandle<Key, T> ptr_hdl(*this);
  ObMetaPointer<T> *t_ptr = nullptr;
  if (OB_UNLIKELY(!key.is_valid()
               || !old_addr.is_valid()
               || !new_addr.is_valid()
               || new_addr.is_none()
               || (set_pool && nullptr == pool))) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key), K(old_addr), K(new_addr), K(set_pool), KP(pool));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else {
    common::ObBucketHashWLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      STORAGE_LOG(WARN, "fail to get pointer handle", K(ret));
    } else if (OB_ISNULL(t_ptr = ptr_hdl.get_resource_ptr())) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get meta pointer", K(ret), KP(t_ptr));
    } else if (OB_UNLIKELY(t_ptr->get_addr() != old_addr)) {
      ret = common::OB_NOT_THE_OBJECT;
      STORAGE_LOG(WARN, "old address has changed, need to get again", K(ret), KPC(t_ptr), K(old_addr));
    } else {
      t_ptr->set_addr_with_reset_obj(new_addr);
      if (set_pool) {
        t_ptr->set_obj_pool(*pool);
      }
    }
  }
  return ret;
}

// ATTENTION: operator should be read-only operations
template <typename Key, typename T>
template <typename Operator>
int ObMetaPointerMap<Key, T>::for_each_value_store(Operator &op)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!ResourceMap::is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMetaPointerMap has not been inited", K(ret));
  } else {
    bool locked = false;
    while (OB_SUCC(ret) && !locked) {
      common::ObBucketTryRLockAllGuard lock_guard(ResourceMap::bucket_lock_);
      if (OB_FAIL(lock_guard.get_ret()) && OB_EAGAIN != ret) {
        STORAGE_LOG(WARN, "fail to lock all tablet id set", K(ret));
      } else if (OB_EAGAIN == ret) {
        // try again after 1ms sleep.
        ob_usleep(1000);
        ret = common::OB_SUCCESS;
      } else {
        locked = true;
        if (OB_FAIL(ResourceMap::map_.foreach_refactored(op))) {
          STORAGE_LOG(WARN, "fail to foreach refactored", K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename Key, typename T>
int ObMetaPointerMap<Key, T>::wash_meta_obj(const Key &key, ObMetaObjGuard<ObTablet> &guard, void *&free_obj)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObMetaPointerHandle<Key, T> ptr_hdl(*this);
  ObMetaPointer<T> *t_ptr = nullptr;

  if (OB_UNLIKELY(!ResourceMap::is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMetaPointerMap has not been inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
    STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
  } else {
    common::ObBucketHashWLockGuard lock_guard(ResourceMap::bucket_lock_, hash_val);
    if (OB_FAIL(ResourceMap::get_without_lock(key, ptr_hdl))) {
      if (common::OB_ENTRY_NOT_EXIST == ret) {
        ret = common::OB_SUCCESS;
        STORAGE_LOG(WARN, "tablet maybe already gc-ed", K(ret), K(key));
      } else {
        STORAGE_LOG(WARN, "fail to get pointer handle", K(ret), K(key));
      }
    } else if (OB_ISNULL(t_ptr = ptr_hdl.get_resource_ptr())) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get meta pointer", K(ret), KP(t_ptr), K(key));
    } else if (OB_FAIL(t_ptr->dump_meta_obj(guard, free_obj))) {
      STORAGE_LOG(WARN, "fail to dump meta obj", K(ret), K(key));
    }
  }
  return ret;
}
}  // end namespace storage
}  // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_META_POINTER_MAP_H_ */
