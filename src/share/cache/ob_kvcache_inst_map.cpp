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

#include "ob_kvcache_inst_map.h"

namespace oceanbase {
using namespace lib;
namespace common {
int ObTenantMBList::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_FAIL(ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id, resource_mgr_))) {
    COMMON_LOG(WARN, "get_tenant_resource_mgr failed", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    head_.reset();
    head_.prev_ = &head_;
    head_.next_ = &head_;
    ref_cnt_ = 0;
    inited_ = true;
  }
  return ret;
}

ObTenantMBListHandle::ObTenantMBListHandle() : map_(NULL), list_(NULL)
{}

ObTenantMBListHandle::~ObTenantMBListHandle()
{
  reset();
}

int ObTenantMBListHandle::init(ObKVCacheInstMap* map, ObTenantMBList* list)
{
  int ret = OB_SUCCESS;
  if (NULL != map_ || NULL != list_) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(WARN, "init twice", K(ret), KP_(map), KP_(list));
  } else if (NULL == map || NULL == list) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments", K(ret), KP(map), KP(list));
  } else {
    map_ = map;
    list_ = list;
    list_->inc_ref();
  }
  return ret;
}

void ObTenantMBListHandle::reset()
{
  if (NULL != map_ && NULL != list_) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(map_->dec_mb_list_ref(list_))) {
      SHARE_LOG(ERROR, "dec_mb_list_ref failed", K(ret), KP_(list));
    }
  }
  map_ = NULL;
  list_ = NULL;
}

ObKVMemBlockHandle* ObTenantMBListHandle::get_head()
{
  ObKVMemBlockHandle* head = NULL;
  if (NULL != list_) {
    head = &list_->head_;
  }
  return head;
}

ObTenantResourceMgrHandle* ObTenantMBListHandle::get_resource_handle()
{
  ObTenantResourceMgrHandle* resource_handle = NULL;
  if (NULL != list_) {
    resource_handle = &list_->resource_mgr_;
  }
  return resource_handle;
}
/**
 * ---------------------------------------------------------ObKVCacheInstHandle-----------------------------------------------------
 */
ObKVCacheInstHandle::ObKVCacheInstHandle() : map_(NULL), inst_(NULL)
{}

ObKVCacheInstHandle::~ObKVCacheInstHandle()
{
  reset();
}

void ObKVCacheInstHandle::reset()
{
  if (NULL != map_ && NULL != inst_) {
    map_->de_inst_ref(inst_);
  }
  map_ = NULL;
  inst_ = NULL;
}

ObKVCacheInstHandle::ObKVCacheInstHandle(const ObKVCacheInstHandle& other)
{
  map_ = other.map_;
  inst_ = other.inst_;
  if (NULL != map_ && NULL != inst_) {
    map_->add_inst_ref(inst_);
  }
}

ObKVCacheInstHandle& ObKVCacheInstHandle::operator=(const ObKVCacheInstHandle& other)
{
  if (map_ == other.map_ && inst_ == other.inst_) {  // do nothing
  } else {
    reset();
    map_ = other.map_;
    inst_ = other.inst_;
    if (NULL != map_ && NULL != inst_) {
      map_->add_inst_ref(inst_);
    }
  }
  return *this;
}

ObKVCacheInstMap::ObKVCacheInstMap()
    : lock_(),
      inst_map_(),
      inst_pool_(),
      list_lock_(),
      list_map_(),
      list_pool_(),
      configs_(NULL),
      allocator_(ObNewModIds::OB_KVSTORE_CACHE),
      inst_keys_(),
      tenant_mgr_(NULL),
      is_inited_(false)
{}

ObKVCacheInstMap::~ObKVCacheInstMap()
{
  destroy();
}

int ObKVCacheInstMap::init(const int64_t max_entry_cnt, const ObKVCacheConfig* configs, const ObITenantMgr& tenant_mgr)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObKVCacheInstMap has been inited, ", K(ret));
  } else if (max_entry_cnt <= 0 || NULL == configs) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(max_entry_cnt), KP(configs), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(inst_map_.create(max_entry_cnt, ObNewModIds::OB_KVSTORE_CACHE, ObNewModIds::OB_KVSTORE_CACHE))) {
      COMMON_LOG(WARN, "Fail to create inst map, ", K(ret));
    } else if (NULL ==
               (buf = (char*)allocator_.alloc((sizeof(ObKVCacheInst) + sizeof(ObKVCacheInst*)) * max_entry_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "Fail to allocate memory, ", K(max_entry_cnt), K(ret));
    } else if (OB_FAIL(inst_pool_.init(max_entry_cnt, buf + sizeof(ObKVCacheInst) * max_entry_cnt))) {
      COMMON_LOG(WARN, "Fail to init inst pool, ", K(ret));
    } else {
      ObKVCacheInst* inst = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < max_entry_cnt; ++i) {
        inst = new (buf + sizeof(ObKVCacheInst) * i) ObKVCacheInst();
        if (OB_FAIL(inst_pool_.push(inst))) {
          COMMON_LOG(WARN, "Fail to push inst to pool, ", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t tenant_num = MAX_TENANT_NUM_PER_SERVER;
    if (OB_FAIL(list_map_.init(tenant_num, tenant_num, ObNewModIds::OB_KVSTORE_CACHE))) {
      COMMON_LOG(WARN, "init mb list map failed", K(ret), K(tenant_num));
    } else if (NULL ==
               (buf = (char*)allocator_.alloc((sizeof(ObTenantMBList) + sizeof(ObTenantMBList*)) * tenant_num))) {
      COMMON_LOG(WARN, "alloc memory failed", K(ret), K(tenant_num));
    } else if (OB_FAIL(list_pool_.init(tenant_num, buf + sizeof(ObTenantMBList) * tenant_num))) {
      COMMON_LOG(WARN, "init mb list pool failed", K(ret), K(tenant_num));
    } else {
      ObTenantMBList* list = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_num; ++i) {
        list = new (buf + sizeof(ObTenantMBList) * i) ObTenantMBList();
        if (OB_FAIL(list_pool_.push(list))) {
          COMMON_LOG(WARN, "push mb list to pool failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    const int key_cnt = MAX_TENANT_NUM_PER_SERVER * MAX_CACHE_NUM;
    if (OB_FAIL(inst_keys_.init(key_cnt, ObModIds::OB_KVSTORE_CACHE_WASH_STRUCT))) {
      COMMON_LOG(WARN, "inst_keys_ init failed", K(ret), K(key_cnt));
    }
  }

  if (OB_SUCC(ret)) {
    configs_ = configs;
    tenant_mgr_ = &tenant_mgr;
    is_inited_ = true;
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

void ObKVCacheInstMap::destroy()
{
  inst_map_.destroy();
  inst_pool_.destroy();
  list_map_.destroy();
  list_pool_.destroy();
  allocator_.reset();
  configs_ = NULL;
  inst_keys_.destroy();
  is_inited_ = false;
}

int ObKVCacheInstMap::get_cache_inst(const ObKVCacheInstKey& inst_key, ObKVCacheInstHandle& inst_handle)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheInstMap has not been inited, ", K(ret));
  } else if (!inst_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "The inst_key is not valid, ", K(ret));
  } else {
    inst_handle.reset();
    ObKVCacheInst* inst = NULL;
    // try get store tenant handle
    {
      DRWLock::RDLockGuard rd_guard(lock_);
      if (OB_SUCC(inst_map_.get_refactored(inst_key, inst))) {
        // success to get st_handle, add ref to return outside
        add_inst_ref(inst);
      }
    }

    if (OB_HASH_NOT_EXIST == ret) {
      DRWLock::WRLockGuard wr_guard(lock_);
      if (OB_SUCC(inst_map_.get_refactored(inst_key, inst))) {
        // double check, success to get inst, add ref to return outside
        add_inst_ref(inst);
      } else if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(inst_pool_.pop(inst))) {
          COMMON_LOG(WARN, "Fail to alloc cache inst, ", K(ret));
        } else if (OB_FAIL(get_mb_list(inst_key.tenant_id_, inst->mb_list_handle_))) {
          COMMON_LOG(WARN, "get mb list failed", K(ret), "tenant_id", inst_key.tenant_id_);
        } else if (OB_FAIL(inst->node_allocator_.init(
                       OB_MALLOC_BIG_BLOCK_SIZE, ObNewModIds::OB_KVSTORE_CACHE, inst_key.tenant_id_, 1))) {
          COMMON_LOG(WARN, "Fail to init node allocator, ", K(ret));
        } else if (OB_FAIL(inst_map_.set_refactored(inst_key, inst))) {
          COMMON_LOG(WARN, "Fail to set inst to inst map, ", K(ret));
        } else {
          inst->cache_id_ = inst_key.cache_id_;
          inst->tenant_id_ = inst_key.tenant_id_;
          inst->status_.config_ = &configs_[inst_key.cache_id_];
          // the first ref is kept by inst_map_
          add_inst_ref(inst);
          // the second ref is return outside
          add_inst_ref(inst);
        }

        if (OB_FAIL(ret) && NULL != inst) {
          inst->reset();
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = inst_pool_.push(inst))) {
            COMMON_LOG(ERROR, "Fail to push inst to pool, ", K(tmp_ret));
          }
          if (OB_SUCCESS != (tmp_ret = inst_map_.erase_refactored(inst_key))) {
            if (OB_HASH_NOT_EXIST != tmp_ret) {
              COMMON_LOG(ERROR, "Fail to erase inst key, ", K(ret));
            }
          }
          inst = NULL;
        }
      }
    }

    if (OB_SUCC(ret) && NULL != inst) {
      inst_handle.map_ = this;
      inst_handle.inst_ = inst;
    }
  }
  return ret;
}

int ObKVCacheInstMap::clean_garbage_inst()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheInstMap has not been inited, ", K(ret));
  } else {
    ObKVCacheInst* inst = NULL;
    inst_keys_.reuse();
    // collect garbage instance keys
    {
      DRWLock::RDLockGuard rd_guard(lock_);
      for (KVCacheInstMap::iterator iter = inst_map_.begin(); OB_SUCC(ret) && iter != inst_map_.end(); ++iter) {
        if (!tenant_mgr_->has_tenant(iter->first.tenant_id_)) {
          inst = iter->second;
          if (inst->can_destroy()) {
            if (OB_FAIL(inst_keys_.push_back(iter->first))) {
              COMMON_LOG(WARN, "Fail to push garbage instance key to array, ", K(ret));
            }
          } else {
            ObKVMemBlockHandle* mb_handle = NULL;
            for (int64_t i = 0; i < MAX_POLICY; ++i) {
              if (NULL != (mb_handle = inst->handles_[i])) {
                if (ATOMIC_BCAS((uint64_t*)(&inst->handles_[i]), (uint64_t)mb_handle, NULL)) {
                  (void)ATOMIC_BCAS((uint32_t*)(&mb_handle->status_), USING, FULL);
                }
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && inst_keys_.count() > 0) {
      DRWLock::WRLockGuard wr_guard(lock_);
      for (int64_t i = 0; OB_SUCC(ret) && i < inst_keys_.count(); ++i) {
        if (!tenant_mgr_->has_tenant(inst_keys_.at(i).tenant_id_)) {
          if (OB_SUCC(inst_map_.get_refactored(inst_keys_.at(i), inst))) {
            if (inst->can_destroy()) {
              inst->reset();
              if (OB_FAIL(inst_map_.erase_refactored(inst_keys_.at(i)))) {
                COMMON_LOG(ERROR, "Fail to erase key from inst map, ", K(ret));
              } else if (OB_FAIL(inst_pool_.push(inst))) {
                COMMON_LOG(ERROR, "Fail to push inst to inst pool, ", K(ret));
              }
            }
          } else {
            if (OB_HASH_NOT_EXIST != ret) {
              COMMON_LOG(WARN, "Fail to get inst from map, ", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObKVCacheInstMap::refresh_score()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheInstMap has not been inited, ", K(ret));
  } else {
    int64_t mb_cnt = 0;
    double avg_hit = 0;
    ObKVCacheInst* inst = NULL;
    int64_t total_hit_cnt = 0;
    DRWLock::RDLockGuard rd_guard(lock_);
    for (KVCacheInstMap::iterator iter = inst_map_.begin(); OB_SUCC(ret) && iter != inst_map_.end(); ++iter) {
      inst = iter->second;
      mb_cnt = ATOMIC_LOAD(&inst->status_.lru_mb_cnt_) + ATOMIC_LOAD(&inst->status_.lfu_mb_cnt_);
      avg_hit = 0;
      total_hit_cnt = inst->status_.total_hit_cnt_.value();
      if (mb_cnt > 0) {
        avg_hit = double(total_hit_cnt - inst->status_.last_hit_cnt_) / (double)mb_cnt;
      }
      inst->status_.last_hit_cnt_ = total_hit_cnt;
      inst->status_.base_mb_score_ = inst->status_.base_mb_score_ * CACHE_SCORE_DECAY_FACTOR +
                                     avg_hit * (double)(inst->status_.config_->priority_);
    }
  }
  return ret;
}

int ObKVCacheInstMap::set_priority(const int64_t cache_id, const int64_t old_priority, const int64_t new_priority)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheInstMap has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(cache_id < 0) || OB_UNLIKELY(old_priority <= 0) || OB_UNLIKELY(new_priority <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(cache_id), K(old_priority), K(new_priority), K(ret));
  } else {
    DRWLock::RDLockGuard rd_guard(lock_);
    for (KVCacheInstMap::iterator iter = inst_map_.begin(); OB_SUCC(ret) && iter != inst_map_.end(); ++iter) {
      if (iter->first.cache_id_ == cache_id) {
        iter->second->status_.base_mb_score_ =
            iter->second->status_.base_mb_score_ * double(new_priority) / double(old_priority);
      }
    }
  }
  return ret;
}

int ObKVCacheInstMap::get_tenant_cache_info(const uint64_t tenant_id, ObIArray<ObKVCacheInstHandle>& inst_handles)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheInstMap has not been inited, ", K(ret));
  } else if (0 == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(tenant_id), K(ret));
  } else {
    DRWLock::RDLockGuard rd_guard(lock_);
    for (KVCacheInstMap::iterator iter = inst_map_.begin(); OB_SUCC(ret) && iter != inst_map_.end(); ++iter) {
      if (iter->first.tenant_id_ == tenant_id) {
        ObKVCacheInstHandle handle;
        handle.inst_ = iter->second;
        handle.map_ = this;
        handle.inst_->status_.map_size_ = iter->second->node_allocator_.allocated();
        add_inst_ref(handle.inst_);
        if (OB_FAIL(inst_handles.push_back(handle))) {
          COMMON_LOG(WARN, "Fail to push info to array, ", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObKVCacheInstMap::get_all_cache_info(ObIArray<ObKVCacheInstHandle>& inst_handles)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheInstMap has not been inited, ", K(ret));
  } else {
    DRWLock::RDLockGuard rd_guard(lock_);
    for (KVCacheInstMap::iterator iter = inst_map_.begin(); OB_SUCC(ret) && iter != inst_map_.end(); ++iter) {
      ObKVCacheInstHandle handle;
      handle.inst_ = iter->second;
      handle.map_ = this;
      handle.inst_->status_.map_size_ = iter->second->node_allocator_.allocated();
      add_inst_ref(handle.inst_);
      if (OB_FAIL(inst_handles.push_back(handle))) {
        COMMON_LOG(WARN, "Fail to push info to array, ", K(ret));
      }
    }
  }
  return ret;
}

int ObKVCacheInstMap::set_hold_size(const uint64_t tenant_id, const char* cache_name, const int64_t hold_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || NULL == cache_name || hold_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), KP(cache_name), K(hold_size));
  } else {
    DRWLock::RDLockGuard rd_guard(lock_);
    bool find = false;
    for (KVCacheInstMap::iterator iter = inst_map_.begin(); !find && OB_SUCC(ret) && iter != inst_map_.end(); ++iter) {
      if (iter->first.tenant_id_ == tenant_id) {
        const int64_t cache_id = iter->second->cache_id_;
        if (0 == STRNCMP(configs_[cache_id].cache_name_, cache_name, MAX_CACHE_NAME_LENGTH)) {
          iter->second->status_.set_hold_size(hold_size);
          find = true;
        }
      }
    }

    if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
      // we are sure that cache_name not null
      COMMON_LOG(WARN, "cache not exist", K(ret), K(tenant_id), K(cache_name));
    }
  }
  return ret;
}

int ObKVCacheInstMap::get_hold_size(const uint64_t tenant_id, const char* cache_name, int64_t& hold_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || NULL == cache_name) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", K(ret), K(tenant_id), KP(cache_name), K(hold_size));
  } else {
    DRWLock::RDLockGuard rd_guard(lock_);
    bool find = false;
    for (KVCacheInstMap::iterator iter = inst_map_.begin(); !find && OB_SUCC(ret) && iter != inst_map_.end(); ++iter) {
      if (iter->first.tenant_id_ == tenant_id) {
        const int64_t cache_id = iter->second->cache_id_;
        if (0 == STRNCMP(configs_[cache_id].cache_name_, cache_name, MAX_CACHE_NAME_LENGTH)) {
          hold_size = iter->second->status_.get_hold_size();
          find = true;
        }
      }
    }

    if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObKVCacheInstMap::get_mb_list(const uint64_t tenant_id, ObTenantMBListHandle& list_handle)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    ObTenantMBList* list = NULL;
    bool need_create = false;
    {
      DRWLock::RDLockGuard rd_guard(list_lock_);
      if (OB_FAIL(list_map_.get(tenant_id, list))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          need_create = true;
        } else {
          COMMON_LOG(WARN, "get failed", K(ret), K(tenant_id));
        }
      } else if (OB_FAIL(list_handle.init(this, list))) {
        COMMON_LOG(WARN, "init list_handle failed", K(ret));
      }
    }

    if (need_create && OB_SUCC(ret) && !list_handle.is_valid()) {
      DRWLock::WRLockGuard wr_guard(list_lock_);
      if (OB_FAIL(list_map_.get(tenant_id, list))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          COMMON_LOG(WARN, "get failed", K(ret), K(tenant_id));
        } else if (OB_FAIL(list_pool_.pop(list))) {
          COMMON_LOG(WARN, "list_pool pop failed", K(ret));
        } else {
          list->reset();
          if (OB_FAIL(list->init(tenant_id))) {
            COMMON_LOG(WARN, "init list failed", K(ret), K(tenant_id));
          } else if (OB_FAIL(list_map_.set(tenant_id, list))) {
            COMMON_LOG(WARN, "list_map set failed", K(ret));
          }

          if (OB_FAIL(ret)) {
            int temp_ret = OB_SUCCESS;
            list->reset();
            if (OB_SUCCESS != (temp_ret = list_pool_.push(list))) {
              COMMON_LOG(ERROR, "list_pool push failed", K(temp_ret));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(list_handle.init(this, list))) {
          COMMON_LOG(WARN, "init list_handle failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObKVCacheInstMap::dec_mb_list_ref(ObTenantMBList* list)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (NULL == list) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(list));
  } else {
    const int64_t ref_cnt = list->dec_ref();
    if (ref_cnt < 0) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "negative ref count", K(ret), K(ref_cnt));
    } else if (0 == ref_cnt) {
      DRWLock::WRLockGuard wr_guard(list_lock_);
      if (0 == list->get_ref()) {
        if (OB_FAIL(list_map_.erase(list->tenant_id_))) {
          COMMON_LOG(WARN, "erase failed", K(ret), "tenant_id", list->tenant_id_);
        } else {
          list->reset();
          if (OB_FAIL(list_pool_.push(list))) {
            COMMON_LOG(WARN, "list_pool push failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

void ObKVCacheInstMap::add_inst_ref(ObKVCacheInst* inst)
{
  if (OB_UNLIKELY(NULL != inst)) {
    (void)ATOMIC_AAF(&inst->ref_cnt_, 1);
  }
}

void ObKVCacheInstMap::de_inst_ref(ObKVCacheInst* inst)
{
  if (OB_UNLIKELY(NULL != inst)) {
    (void)ATOMIC_SAF(&inst->ref_cnt_, 1);
  }
}

}  // end namespace common
}  // end namespace oceanbase
