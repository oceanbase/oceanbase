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


namespace oceanbase
{
using namespace lib;
namespace common
{
int ObTenantMBList::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_FAIL(ObResourceMgr::get_instance().get_tenant_resource_mgr(
      tenant_id, resource_mgr_))) {
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

ObTenantMBListHandle::ObTenantMBListHandle()
  : map_(NULL), list_(NULL)
{
}

ObTenantMBListHandle::~ObTenantMBListHandle()
{
  reset();
}

int ObTenantMBListHandle::init(ObKVCacheInstMap *map, ObTenantMBList *list)
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

ObKVMemBlockHandle *ObTenantMBListHandle::get_head()
{
  ObKVMemBlockHandle *head = NULL;
  if (NULL != list_) {
    head = &list_->head_;
  }
  return head;
}

ObTenantResourceMgrHandle *ObTenantMBListHandle::get_resource_handle()
{
  ObTenantResourceMgrHandle *resource_handle = NULL;
  if (NULL != list_) {
    resource_handle = &list_->resource_mgr_;
  }
  return resource_handle;
}


/**
 * ---------------------------------------------------------ObKVCacheInst-----------------------------------------------------
 */
bool ObKVCacheInst::can_destroy() const
{
  return is_delete_
      && 0 == ATOMIC_LOAD(&ref_cnt_)
      && 0 == status_.kv_cnt_
      && 0 == status_.store_size_
      && 0 == status_.lru_mb_cnt_
      && 0 == status_.lfu_mb_cnt_;
}

void ObKVCacheInst::try_mark_delete()
{
  if (!is_delete_) {
    is_delete_ = true;
    ATOMIC_DEC(&ref_cnt_);
    for (int i = 0 ; i < MAX_POLICY ; ++i) {
      if (nullptr != handles_[i]) {
        handles_[i]->status_ = FULL;
        handles_[i] = nullptr;
      }
    }
  }
}

/**
 * ---------------------------------------------------------ObKVCacheInstHandle-----------------------------------------------------
 */
ObKVCacheInstHandle::ObKVCacheInstHandle()
  : map_(NULL), inst_(NULL)
{
}

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

bool ObKVCacheInstHandle::is_valid() const
{
  return (nullptr != map_) && (nullptr != inst_);
}

ObKVCacheInstHandle::ObKVCacheInstHandle(const ObKVCacheInstHandle &other)
{
  map_ = other.map_;
  inst_ = other.inst_;
  if (NULL != map_ && NULL != inst_) {
    map_->add_inst_ref(inst_);
  }
}

ObKVCacheInstHandle& ObKVCacheInstHandle::operator = (const ObKVCacheInstHandle& other)
{
  if (map_ == other.map_ && inst_ == other.inst_) { // do nothing
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
  : lock_(common::ObLatchIds::KV_CACHE_INST_LOCK),
    inst_map_(),
    list_lock_(common::ObLatchIds::KV_CACHE_LIST_LOCK),
    list_map_(),
    list_pool_(),
    configs_(NULL),
    allocator_("TenantMBList"),
    inst_keys_(),
    mem_limit_getter_(NULL),
    is_inited_(false)
{
}

ObKVCacheInstMap::~ObKVCacheInstMap()
{
  destroy();
}

int ObKVCacheInstMap::init(const int64_t max_entry_cnt, const ObKVCacheConfig *configs,
                           const ObITenantMemLimitGetter &mem_limit_getter)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObKVCacheInstMap has been inited, ", K(ret));
  } else if (max_entry_cnt <= 0 || NULL == configs) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(max_entry_cnt), KP(configs), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(inst_map_.create(max_entry_cnt, "CACHE_INST_MAP", "CACHE_INST_MAP"))) {
      COMMON_LOG(WARN, "Fail to create inst map, ", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t tenant_num = MAX_TENANT_NUM_PER_SERVER;
    if (OB_FAIL(list_map_.init(tenant_num, tenant_num, "CACHE_TNT_LST"))) {
      COMMON_LOG(WARN, "init mb list map failed", K(ret), K(tenant_num));
    } else if (NULL == (buf = (char*)allocator_.alloc(
        (sizeof(ObTenantMBList) + sizeof(ObTenantMBList*)) * tenant_num))) {
      COMMON_LOG(WARN, "alloc memory failed", K(ret), K(tenant_num));
    } else if (OB_FAIL(list_pool_.init(tenant_num, buf + sizeof(ObTenantMBList) * tenant_num))) {
      COMMON_LOG(WARN, "init mb list pool failed", K(ret), K(tenant_num));
    } else if (OB_FAIL(tenant_set_.create(tenant_num))) {
      COMMON_LOG(WARN, "init tenant set failed", K(ret), K(tenant_num));
    } else {
      ObTenantMBList *list = NULL;
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
    if (OB_FAIL(inst_keys_.init(key_cnt, "CACHE_INST_MAP"))) {
      COMMON_LOG(WARN, "inst_keys_ init failed", K(ret), K(key_cnt));
    }
  }

  if (OB_SUCC(ret)) {
    configs_ = configs;
    mem_limit_getter_ = &mem_limit_getter;
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
  tenant_set_.destroy();
  list_map_.destroy();
  list_pool_.destroy();
  allocator_.reset();
  configs_ = NULL;
  inst_keys_.destroy();
  is_inited_ = false;
}

int ObKVCacheInstMap::get_cache_inst(
    const ObKVCacheInstKey &inst_key,
    ObKVCacheInstHandle &inst_handle)
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
    ObKVCacheInst *inst = NULL;
    //try get store tenant handle
    {
      DRWLock::RDLockGuard rd_guard(lock_);
      if (OB_SUCC(inst_map_.get_refactored(inst_key, inst))) {
        //success to get st_handle, add ref to return outside
        add_inst_ref(inst);
      }
    }

    if (OB_HASH_NOT_EXIST == ret) {
      DRWLock::WRLockGuard wr_guard(lock_);
      if (OB_SUCC(inst_map_.get_refactored(inst_key, inst))) {
        //double check, success to get inst, add ref to return outside
        add_inst_ref(inst);
      } else if (OB_HASH_NOT_EXIST == ret) {
        lib::ObMemAttr attr(inst_key.tenant_id_, "CACHE_MAP_NODE");
        SET_USE_500(attr);
        inst = OB_NEW(ObKVCacheInst, SET_IGNORE_MEM_VERSION(ObMemAttr(inst_key.tenant_id_, "CACHE_INST")));
        if (OB_ISNULL(inst)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN, "Fail to alloc cache inst, ", K(ret));
        } else if (OB_FAIL(get_mb_list(inst_key.tenant_id_, inst->mb_list_handle_))) {
          COMMON_LOG(WARN, "get mb list failed", K(ret), "tenant_id", inst_key.tenant_id_);
        } else if (OB_FAIL(inst->node_allocator_.init(OB_MALLOC_BIG_BLOCK_SIZE, attr, 1))) {
          COMMON_LOG(WARN, "Fail to init node allocator, ", K(ret));
        } else if (OB_FAIL(inst_map_.set_refactored(inst_key, inst))) {
          COMMON_LOG(WARN, "Fail to set inst to inst map, ", K(ret));
        } else {
          inst->cache_id_ = inst_key.cache_id_;
          inst->tenant_id_ = inst_key.tenant_id_;
          inst->status_.config_ = &configs_[inst_key.cache_id_];
          //the first ref is kept by inst_map_
          add_inst_ref(inst);
          //the second ref is return outside
          add_inst_ref(inst);
        }

        if (OB_FAIL(ret) && NULL != inst) {
          inst->reset();
          int tmp_ret = OB_SUCCESS;
          ob_delete(inst);
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

int ObKVCacheInstMap::mark_tenant_delete(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheInstMap has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(tenant_id));
  } else {
    ObKVCacheInst *inst = nullptr;
    DRWLock::WRLockGuard wr_guard(lock_);
    for (KVCacheInstMap::iterator iter = inst_map_.begin() ; OB_SUCC(ret) && iter != inst_map_.end() ; ++iter) {
      if (tenant_id != iter->first.tenant_id_) {
      } else if (OB_ISNULL(iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "Unexpected null cache inst", K(ret));
      } else {
        iter->second->try_mark_delete();
      }
    }
    COMMON_LOG(INFO, "mark delete details", K(ret), K(tenant_id));
  }

  return ret;
}

int ObKVCacheInstMap::erase_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheInstMap has not been inited", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(tenant_id));
  } else {
    ObSEArray<ObKVCacheInstKey, MAX_CACHE_NUM> erase_key_list;
    ObSEArray<ObKVCacheInst *, MAX_CACHE_NUM> erase_inst_list;
    DRWLock::WRLockGuard wr_guard(lock_);
    ObKVCacheInst *inst = nullptr;
    for (KVCacheInstMap::iterator iter = inst_map_.begin() ; OB_SUCC(ret) && iter != inst_map_.end() ; ++iter) {
      inst = iter->second;
      if (tenant_id != iter->first.tenant_id_) {
      } else if (OB_ISNULL(inst)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "Unexpected null cache inst", K(ret));
      } else if (!inst->can_destroy()) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "Still can not destroy cache inst", K(ret), KPC(inst), K(inst->status_.store_size_),
                   K(inst->status_.kv_cnt_), K(inst->status_.lfu_mb_cnt_), K(inst->status_.lru_mb_cnt_));
      } else if (OB_FAIL(erase_key_list.push_back(iter->first))) {
        COMMON_LOG(WARN, "Fail to push back erase inst key", K(ret));
      } else if (OB_FAIL(erase_inst_list.push_back(inst))) {
        COMMON_LOG(WARN, "Fail to push back erase inst key", K(ret));
      }
    }
    for (int i = 0 ; OB_SUCC(ret) && i < erase_key_list.count() ; ++i) {
      ObKVCacheInstKey tmp_key = erase_key_list.at(i);
      inst = erase_inst_list.at(i);
      if (OB_FAIL(inst_map_.erase_refactored(tmp_key))) {
        COMMON_LOG(WARN, "Fail to erase cache inst from inst map", K(ret));
      } else if (FALSE_IT(inst->reset())) {
      } else {
        ob_delete(inst);
      }
    }
  }
  COMMON_LOG(INFO, "erase tenant cache inst details", K(ret), K(tenant_id));

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
    ObKVCacheInst *inst = NULL;
    int64_t total_hit_cnt = 0;
    DRWLock::RDLockGuard rd_guard(lock_);
    for (KVCacheInstMap::iterator iter = inst_map_.begin(); OB_SUCC(ret) && iter != inst_map_.end(); ++iter) {
      inst = iter->second;
      mb_cnt = ATOMIC_LOAD(&inst->status_.lru_mb_cnt_) + ATOMIC_LOAD(&inst->status_.lfu_mb_cnt_);
      avg_hit = 0;
      total_hit_cnt = inst->status_.total_hit_cnt_.value();
      if (mb_cnt > 0) {
        avg_hit = double (total_hit_cnt - inst->status_.last_hit_cnt_) / (double) mb_cnt;
      }
      inst->status_.last_hit_cnt_ = total_hit_cnt;
      inst->status_.base_mb_score_ = inst->status_.base_mb_score_ * CACHE_SCORE_DECAY_FACTOR
          + avg_hit * (double) (inst->status_.config_->priority_);
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
        iter->second->status_.base_mb_score_ = iter->second->status_.base_mb_score_
            * double(new_priority) / double(old_priority);
      }
    }
  }
  return ret;
}

int ObKVCacheInstMap::get_cache_info(const uint64_t tenant_id, ObIArray<ObKVCacheInstHandle> &inst_handles)
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
      if (iter->first.tenant_id_ != tenant_id && OB_SYS_TENANT_ID != tenant_id) {
      } else if (OB_ISNULL(iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "Unexpected null cache inst", K(ret));
      } else if (iter->second->is_mark_delete()) {
      } else if (OB_FAIL(inner_push_inst_handle(iter, inst_handles))) {
        COMMON_LOG(WARN, "Fail to inner push cache inst", K(ret));
      }
    }
  }
  return ret;
}

void ObKVCacheInstMap::print_tenant_cache_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(is_inited_)) {
    ContextParam param;
    param.set_mem_attr(common::OB_SERVER_TENANT_ID, ObModIds::OB_TEMP_VARIABLES);
    CREATE_WITH_TEMP_CONTEXT(param) {
      static const int64_t BUFLEN = 1 << 17;
      char *buf = (char *)ctxalp(BUFLEN);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(ERROR, "no memory", K(ret));
      } else {
        int64_t ctx_pos = 0;
        {
          DRWLock::RDLockGuard rd_guard(lock_);
          for (KVCacheInstMap::iterator iter = inst_map_.begin(); iter != inst_map_.end(); ++iter) {
            if (iter->second->tenant_id_ == tenant_id) {
              ret = databuff_printf(buf, BUFLEN, ctx_pos,
              "[CACHE] tenant_id=%8ld | cache_name=%30s | cache_size=%12ld | cache_store_size=%12ld | cache_map_size=%12ld | kv_cnt=%8ld | hold_size=%12ld\n",
              iter->second->tenant_id_,
              iter->second->status_.config_->cache_name_,
              iter->second->status_.store_size_ + iter->second->node_allocator_.allocated(),
              iter->second->status_.store_size_,
              iter->second->node_allocator_.allocated(),
              iter->second->status_.kv_cnt_,
              iter->second->status_.hold_size_);
            }
          }
        }
        _OB_LOG(INFO, "[CACHE] tenant_id=%8ld cache memory info: \n%s", tenant_id, buf);
      }
    }
  }
}

void ObKVCacheInstMap::print_all_cache_info()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_inited_)) {
    tenant_set_.clear();
    {
      DRWLock::RDLockGuard rd_guard(lock_);
      for (KVCacheInstMap::iterator iter = inst_map_.begin(); OB_SUCC(ret) && iter != inst_map_.end(); ++iter) {
        if (OB_HASH_NOT_EXIST == (ret = tenant_set_.exist_refactored(iter->second->tenant_id_))) {
          // Expected result, tenant_id is not existed in tenant_set_.
          ret = tenant_set_.set_refactored(iter->second->tenant_id_);
        } else if (OB_HASH_EXIST == ret) {
          // Expected result, tenant_id is existed in tenant_set_.
          ret = OB_SUCCESS;
        } else {
          // Unexpected, exit cycle.
          COMMON_LOG(WARN, "Get tenant id fail", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // set tenant_id succeeded
      for (hash::ObHashSet<uint64_t>::iterator iter = tenant_set_.begin(); iter != tenant_set_.end(); ++iter) {
        print_tenant_cache_info(iter->first);
      }
    } else {
      // set tenant_id failed
      COMMON_LOG(WARN, "Set tenant id fail", K(ret));
    }
  }
}

int ObKVCacheInstMap::set_hold_size(const uint64_t tenant_id, const char *cache_name,
                                    const int64_t hold_size)
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
    for (KVCacheInstMap::iterator iter = inst_map_.begin();
         !find && OB_SUCC(ret) && iter != inst_map_.end(); ++iter) {
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

int ObKVCacheInstMap::get_hold_size(const uint64_t tenant_id, const char *cache_name,
                                    int64_t &hold_size)
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
    for (KVCacheInstMap::iterator iter = inst_map_.begin();
         !find && OB_SUCC(ret) && iter != inst_map_.end(); ++iter) {
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

int ObKVCacheInstMap::get_mb_list(const uint64_t tenant_id, ObTenantMBListHandle &list_handle, const bool create_list)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    ObTenantMBList *list = NULL;
    bool need_create = false;
    {
      DRWLock::RDLockGuard rd_guard(list_lock_);
      if (OB_FAIL(list_map_.get(tenant_id, list))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          if (create_list) {
            ret = OB_SUCCESS;
            need_create = true;
          }
          // If the parameter "create_list" is false, OB_ENTRY_NOT_EXIST should be treated
          // as excepted return rather than error. Therefore, do nothing.
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

int ObKVCacheInstMap::dec_mb_list_ref(ObTenantMBList *list)
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

void ObKVCacheInstMap::add_inst_ref(ObKVCacheInst *inst)
{
  if (OB_UNLIKELY(NULL != inst)) {
    (void) ATOMIC_AAF(&inst->ref_cnt_, 1);
  }
}

void ObKVCacheInstMap::de_inst_ref(ObKVCacheInst *inst)
{
  if (OB_UNLIKELY(NULL != inst)) {
    (void) ATOMIC_SAF(&inst->ref_cnt_, 1);
  }
}

int ObKVCacheInstMap::inner_push_inst_handle(const KVCacheInstMap::iterator &iter, ObIArray<ObKVCacheInstHandle> &inst_handles)
{
  INIT_SUCC(ret);

  ObKVCacheInstHandle handle;
  handle.inst_ = iter->second;
  handle.map_ = this;
  handle.inst_->status_.map_size_ = iter->second->node_allocator_.allocated();
  add_inst_ref(handle.inst_);
  if (OB_FAIL(inst_handles.push_back(handle))) {
    COMMON_LOG(WARN, "Fail to push back inst handle to array", K(ret));
  }

  return ret;
}


}//end namespace common
}//end namespace oceanbase
