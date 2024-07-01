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

#include "storage/meta_mem/ob_tablet_pointer_map.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{

ObTabletPointerMap::ObTabletPointerMap()
    : ObResourceMap<ObTabletMapKey, ObTabletPointer>(), max_count_(0)
{
}

int ObTabletPointerMap::set(const ObTabletMapKey &key, ObTabletPointer &ptr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResourceMap::set(key, ptr))) {
    STORAGE_LOG(WARN, "fail to set into ResourceMap", K(ret), K(key));
  } else if (count() > ATOMIC_LOAD(&max_count_)) {
    ATOMIC_INC(&max_count_);
  }
  return ret;
}

int ObTabletPointerMap::erase(const ObTabletMapKey &key, ObMetaObjGuard<ObTablet> &guard)
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

int ObTabletPointerMap::inner_erase(const ObTabletMapKey &key)
{
  int ret = common::OB_SUCCESS;
  ObResourceValueStore<ObTabletPointer> *ptr = NULL;
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
      ObTabletPointer *value = ptr->get_value_ptr();
      value->reset_obj();
      if (OB_FAIL(ResourceMap::dec_handle_ref(ptr))) {
        STORAGE_LOG(WARN, "fail to dec handle ref", K(ret));
      }
    }
  }
  return ret;
}

int ObTabletPointerMap::exist(const ObTabletMapKey &key, bool &is_exist)
{
  int ret = common::OB_SUCCESS;
  ObTabletPointerHandle ptr_hdl(*this);
  ObTabletPointer *t_ptr = nullptr;
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

int ObTabletPointerMap::try_get_in_memory_meta_obj_and_addr(
    const ObTabletMapKey &key,
    ObMetaDiskAddr &addr,
    ObMetaObjGuard<ObTablet> &guard)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObTabletPointerHandle ptr_hdl(*this);
  ObTabletPointer *t_ptr = nullptr;
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

int ObTabletPointerMap::try_get_in_memory_meta_obj(
    const ObTabletMapKey &key,
    bool &success,
    ObMetaObjGuard<ObTablet> &guard)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObTabletPointerHandle ptr_hdl(*this);
  ObTabletPointer *t_ptr = nullptr;
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

int ObTabletPointerMap::try_get_in_memory_meta_obj(
    const ObTabletMapKey &key,
    ObTabletPointerHandle &ptr_hdl,
    ObMetaObjGuard<ObTablet> &guard,
    bool &is_in_memory)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  ObTabletPointer *t_ptr = nullptr;
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

int ObTabletPointerMap::try_get_in_memory_meta_obj_with_filter(
    const ObTabletMapKey &key,
    ObITabletFilterOp &op,
    ObTabletPointerHandle &ptr_hdl,
    ObMetaObjGuard<ObTablet> &guard,
    bool &is_in_memory)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  ObTabletPointer *t_ptr = nullptr;
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
      if (t_ptr->is_attr_valid()) { // try skip tablet with attr
        ObTabletResidentInfo info(key, *t_ptr);
        bool is_skipped = false;
        if (OB_FAIL(op(info, is_skipped))) {
          STORAGE_LOG(WARN, "fail to skip tablet", K(ret), KP(t_ptr), K(key), K(info));
        } else if (is_skipped) {
          ret = OB_NOT_THE_OBJECT;
        }
      } else {
        op.inc_invalid_attr_cnt();
      }
      if (OB_SUCC(ret)) {
        t_ptr->get_obj(guard);
        is_in_memory = true;
      }
    } else {
      op.inc_not_in_memory_cnt();
    }
  }
  return ret;
}

int ObTabletPointerMap::get_meta_obj(
    const ObTabletMapKey &key,
    ObMetaObjGuard<ObTablet> &guard)
{
  int ret = common::OB_SUCCESS;
  ObTabletPointerHandle ptr_hdl(*this);
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

int ObTabletPointerMap::get_meta_obj_with_filter(
    const ObTabletMapKey &key,
    ObITabletFilterOp &op,
    ObMetaObjGuard<ObTablet> &guard)
{
  int ret = common::OB_SUCCESS;
  ObTabletPointerHandle ptr_hdl(*this);
  bool is_in_memory = false;
  guard.reset();
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key));
  } else if (OB_FAIL(try_get_in_memory_meta_obj_with_filter(key, op, ptr_hdl, guard, is_in_memory))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_ITEM_NOT_SETTED == ret) {
      STORAGE_LOG(DEBUG, "meta obj does not exist", K(ret), K(key));
    } else if (OB_NOT_THE_OBJECT == ret) {
      STORAGE_LOG(DEBUG, "this tablet has been skipped", K(ret), K(key));
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


int ObTabletPointerMap::load_and_hook_meta_obj(
    const ObTabletMapKey &key,
    ObTabletPointerHandle &ptr_hdl,
    ObMetaObjGuard<ObTablet> &guard)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  ObUpdateTabletPointerParam update_pointer_param;
  ObTabletPointer *meta_pointer = ptr_hdl.get_resource_ptr();
  do {
    bool need_free_obj = false;
    ObTablet *t = nullptr;
    // Move load obj from disk out of the bucket lock, because
    // wash obj may acquire the bucket lock again, which cause dead lock.
    if (OB_FAIL(load_meta_obj(key, meta_pointer, update_pointer_param, t))) {
      STORAGE_LOG(WARN, "load obj from disk fail", K(ret), K(key), KPC(meta_pointer), K(lbt()));
    } else if (OB_FAIL(ResourceMap::hash_func_(key, hash_val))) {
      STORAGE_LOG(WARN, "fail to calc hash", K(ret), K(key));
    } else {
      ObTabletPointerHandle tmp_ptr_hdl(*this);
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
        } else if (OB_UNLIKELY(update_pointer_param.tablet_addr_ != meta_pointer->get_addr()
            || meta_pointer != tmp_ptr_hdl.get_resource_ptr()
            || meta_pointer->get_addr() != tmp_ptr_hdl.get_resource_ptr()->get_addr())) {
          ret = OB_ITEM_NOT_MATCH;
          need_free_obj = true;
          if (REACH_TIME_INTERVAL(1000000)) {
            STORAGE_LOG(WARN, "disk address or pointer change", K(ret), K(update_pointer_param), KPC(meta_pointer),
                KPC(tmp_ptr_hdl.get_resource_ptr()));
          }
        } else {
          if (OB_FAIL(meta_pointer->hook_obj(update_pointer_param.tablet_attr_, t, guard))) {
            STORAGE_LOG(WARN, "fail to hook object", K(ret), K(update_pointer_param), KP(meta_pointer));
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

int ObTabletPointerMap::load_meta_obj(
    const ObTabletMapKey &key,
    ObTabletPointer *meta_pointer,
    common::ObArenaAllocator &allocator,
    ObMetaDiskAddr &load_addr,
    ObTablet *t)
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
      ObTabletPointerHandle tmp_ptr_hdl(*this);
      // check whether the tablet has been deleted
      if (OB_FAIL(ResourceMap::get_without_lock(key, tmp_ptr_hdl))) {
        if (common::OB_ENTRY_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "fail to get pointer handle", K(ret), K(key));
        } else {
          STORAGE_LOG(INFO, "the tablet has been deleted", K(ret), K(key));
        }
      } else if (OB_FAIL(meta_pointer->read_from_disk(true/*is_full_load*/, arena_allocator, buf, buf_len, load_addr))) {
        STORAGE_LOG(WARN, "fail to read from disk", K(ret), KPC(meta_pointer));
      } else if (OB_FAIL(t->assign_pointer_handle(tmp_ptr_hdl))) {
        STORAGE_LOG(WARN, "fail to assign pointer handle", K(ret), K(tmp_ptr_hdl));
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

int ObTabletPointerMap::load_meta_obj(
    const ObTabletMapKey &key,
    ObTabletPointer *meta_pointer,
    ObUpdateTabletPointerParam &update_pointer_param,
    ObTablet *&t)
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
      ObTabletPointerHandle tmp_ptr_hdl(*this);
      ObMetaDiskAddr load_addr;
      // check whether the tablet has been deleted
      if (OB_FAIL(ResourceMap::get_without_lock(key, tmp_ptr_hdl))) {
        if (common::OB_ENTRY_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "fail to get pointer handle", K(ret), K(key));
        } else {
          STORAGE_LOG(INFO, "the tablet has been deleted", K(ret), K(key));
        }
      } else if (OB_FAIL(meta_pointer->read_from_disk(false/*is_full_load*/, arena_allocator, buf, buf_len, load_addr))) {
        STORAGE_LOG(WARN, "fail to read from disk", K(ret), KPC(meta_pointer));
      } else if (OB_FAIL(t->assign_pointer_handle(tmp_ptr_hdl))) {
        STORAGE_LOG(WARN, "fail to assign pointer handle", K(ret), K(tmp_ptr_hdl));
      } else {
        t->tablet_addr_ = load_addr;
        if (OB_FAIL(meta_pointer->deserialize(buf, buf_len, t))) {
          STORAGE_LOG(WARN, "fail to deserialize object", K(ret), K(key), KPC(meta_pointer));
        } else if (OB_FAIL(t->get_updating_tablet_pointer_param(update_pointer_param))) {
          STORAGE_LOG(WARN, "fail to get updating tablet pointer parameters", K(ret), KPC(t));
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

int ObTabletPointerMap::get_meta_obj_with_external_memory(
    const ObTabletMapKey &key,
    common::ObArenaAllocator &allocator,
    ObMetaObjGuard<ObTablet> &guard,
    const bool force_alloc_new,
    ObITabletFilterOp *op)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObTabletPointerHandle ptr_hdl(*this);
  ObTabletPointer *t_ptr = nullptr;
  bool is_in_memory = false;
  guard.reset();
  if (OB_UNLIKELY(!key.is_valid() || (force_alloc_new && nullptr != op))) { /*only support filter when not force new*/
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
    } else if (!ptr_hdl.get_resource_ptr()->get_addr().is_disked()) {
      ret = OB_EAGAIN; // For non-disked addr tablet, please wait for persist.
    }
  } else if ((nullptr == op && OB_FAIL(try_get_in_memory_meta_obj(key, ptr_hdl, guard, is_in_memory)))
      || (nullptr != op && OB_FAIL(try_get_in_memory_meta_obj_with_filter(key, *op, ptr_hdl, guard, is_in_memory)))) {
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
    void *buf = allocator.alloc(sizeof(ObTablet));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret), KP(buf), "size of", sizeof(ObTablet));
    } else {
      bool need_free_obj = false;
      ObTablet *t = new (buf) ObTablet();
      do {
        t->reset();
        if (OB_FAIL(load_meta_obj(key, t_ptr, allocator, disk_addr, t))) {
          STORAGE_LOG(WARN, "load obj from disk fail", K(ret), K(key), KPC(t_ptr), K(lbt()));
        } else {
          ObTabletPointerHandle tmp_ptr_hdl(*this);
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
        t->~ObTablet();
        allocator.free(t);
        t = nullptr;
      }
    }
  }
  return ret;
}

int ObTabletPointerMap::get_meta_addr(const ObTabletMapKey &key, ObMetaDiskAddr &addr)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObTabletPointerHandle ptr_hdl(*this);
  ObTabletPointer *t_ptr = nullptr;

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

int ObTabletPointerMap::set_meta_obj(const ObTabletMapKey &key, ObMetaObjGuard<ObTablet> &guard)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObTabletPointerHandle ptr_hdl(*this);
  ObTabletPointer *t_ptr = nullptr;
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

int ObTabletPointerMap::get_attr_for_obj(const ObTabletMapKey &key, ObMetaObjGuard<ObTablet> &guard)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObTabletPointerHandle ptr_hdl(*this);
  ObTabletPointer *t_ptr = nullptr;
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
      t_ptr->get_attr_for_obj(guard.get_obj());
    }
  }
  return ret;
}

int ObTabletPointerMap::compare_and_swap_addr_and_object(
    const ObTabletMapKey &key,
    const ObMetaObjGuard<ObTablet> &old_guard,
    const ObMetaObjGuard<ObTablet> &new_guard,
    const ObUpdateTabletPointerParam &update_pointer_param)
{
  int ret = common::OB_SUCCESS;
  ObTabletPointerHandle ptr_hdl(*this);
  ObTabletPointer *t_ptr = nullptr;
  ObMetaObjGuard<ObTablet> ptr_guard;
  const ObMetaDiskAddr &new_addr = update_pointer_param.tablet_addr_;
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
      if (t_ptr->get_addr().is_equal_for_persistence(new_addr)) {
        // no need to update tablet attr, including creating memtables or updating the same tablets
        STORAGE_LOG(DEBUG, "no need to update tablet attr", K(ret), K(new_addr), K(t_ptr->get_addr()), K(new_guard), K(old_guard));
      } else if (OB_FAIL(t_ptr->set_tablet_attr(update_pointer_param.tablet_attr_))) {
        STORAGE_LOG(WARN, "failed to update tablet attr", K(ret), K(key), KPC(t_ptr), K(update_pointer_param));
      }

      if (OB_SUCC(ret)) {
        t_ptr->set_addr_with_reset_obj(new_addr);
        t_ptr->set_obj(new_guard);
      }
    }
  }

  return ret;
}

int ObTabletPointerMap::compare_and_swap_address_without_object(
    const ObTabletMapKey &key,
    const ObMetaDiskAddr &old_addr,
    const ObMetaDiskAddr &new_addr,
    const bool set_pool /* whether to set pool */,
    ObITenantMetaObjPool *pool)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObTabletPointerHandle ptr_hdl(*this);
  ObTabletPointer *t_ptr = nullptr;
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

int ObTabletPointerMap::wash_meta_obj(const ObTabletMapKey &key, ObMetaObjGuard<ObTablet> &guard, void *&free_obj)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  ObTabletPointerHandle ptr_hdl(*this);
  ObTabletPointer *t_ptr = nullptr;

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
} // end namespace storage
} // end namespace oceanbase
