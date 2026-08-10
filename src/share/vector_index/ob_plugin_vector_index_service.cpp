/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SERVER
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "storage/ob_storage_rpc.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/ob_get_compat_mode.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/ob_vec_index_builder_util.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/vector_index/ob_ai_access_service.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace share
{

class ObHoldAdaptorRefCallback
{
public:
  ObHoldAdaptorRefCallback() : adapter_(nullptr) {}
  void operator()(hash::HashMapPair<common::ObTabletID, ObPluginVectorIndexAdaptor*> &pair)
  {
    ObPluginVectorIndexAdaptor *adapter = pair.second;
    if (OB_NOT_NULL(adapter)) {
      adapter->inc_ref();
      adapter_ = adapter;
    }
  }
  ObPluginVectorIndexAdaptor *get_adapter() const { return adapter_; }
private:
  ObPluginVectorIndexAdaptor *adapter_;
};

class ObCollectAdaptorTabletIdCallback
{
public:
  explicit ObCollectAdaptorTabletIdCallback(ObIArray<ObTabletID> &tablet_ids)
    : tablet_ids_(tablet_ids)
  {}

  int operator()(const hash::HashMapPair<common::ObTabletID, ObPluginVectorIndexAdaptor*> &entry)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(tablet_ids_.push_back(entry.first))) {
      LOG_WARN("failed to collect adaptor tablet id", KR(ret), K(entry.first));
    }
    return ret;
  }

private:
  ObIArray<ObTabletID> &tablet_ids_;
};

int ObAdapterMapFunc::operator()(const hash::HashMapPair<common::ObTabletID, ObPluginVectorIndexAdaptor*> &entry)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdaptor* adapter(entry.second);
  ObTabletID tablet_id(entry.first);
  if (OB_FAIL(array_.push_back(ObAdapterMapKeyValue(tablet_id, adapter)))) {
    LOG_WARN("failed to push back adapter", K(ret), K(array_), K(adapter));
  }
  return ret;
}

ObPluginVectorIndexMgr::~ObPluginVectorIndexMgr()
{
  destroy();
}

void ObPluginVectorIndexMgr::destroy()
{
  if (IS_INIT) {
    LOG_INFO("LS Vector Index Mgr destroy", K(ls_id_));
    is_inited_ = false;
    need_check_ = false;
    ls_id_.reset();
    release_all_adapters();
    vec_adaptor_map_.destroy();
    ivf_index_helper_map_.destroy();
    ivf_cache_mgr_map_.destroy();
    mem_sync_info_.destroy();
    async_task_opt_.destroy();
  }
}

void ObPluginVectorIndexMgr::release_all_adapters()
{
  int ret = OB_SUCCESS;
  WLockGuard lock_guard(adapter_map_rwlock_);
  FOREACH(iter, vec_adaptor_map_) {
    const ObTabletID &tablet_id = iter->first;
    ObPluginVectorIndexAdaptor *adapter = iter->second;
    if (OB_NOT_NULL(adapter)) {
      adapter->dec_ls_ref();
    }
    if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(adapter))) {
      LOG_ERROR("fail to release vector index adapter", K(tablet_id), KR(ret));
      ret = OB_SUCCESS; // continue release
    }
  }
  FOREACH(iter, ivf_index_helper_map_) {
    const ObIvfHelperKey &key = iter->first;
    ObIvfBuildHelper *helper = iter->second;
    if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_build_helper(helper))) {
      LOG_ERROR("fail to release vector index adapter", K(key), KR(ret));
      ret = OB_SUCCESS; // continue release
    }
  }
  FOREACH(iter, ivf_cache_mgr_map_) {
    const ObTabletID &tablet_id = iter->first;
    ObIvfCacheMgr *mgr = iter->second;
    if (OB_FAIL(ObPluginVectorIndexUtils::release_ivf_cache_mgr(mgr))) {
      LOG_ERROR("fail to release vector index ivf cache mgr", K(tablet_id), KR(ret), KPC(mgr));
      ret = OB_SUCCESS; // continue release
    }
  }
}

int ObPluginVectorIndexMgr::init(uint64_t tenant_id,
                                 ObLSID ls_id,
                                 lib::MemoryContext &memory_context,
                                 uint64_t *all_vsag_use_mem)
{
  int ret = OB_SUCCESS;
  int64_t hash_capacity = common::hash::cal_next_prime(DEFAULT_ADAPTER_HASH_SIZE);
  DISABLE_SQL_MEMLEAK_GUARD;
  if (OB_FAIL(vec_adaptor_map_.create(hash_capacity, "VecIdxAdptMap", "VecIdxAdptMap", tenant_id))) {
    LOG_WARN("fail to create full index adapter map", KR(ret), K(ls_id));
  } else if (OB_FAIL(ivf_index_helper_map_.create(hash_capacity, "IvfIdxHpMap", "IvfIdxHpMap", tenant_id))) {
    LOG_WARN("fail to create ivf index build helper map", KR(ret), K(ls_id));
  } else if (OB_FAIL(ivf_cache_mgr_map_.create(hash_capacity, "IvfMgrMap", "IvfMgrMap", tenant_id))) {
    LOG_WARN("fail to create ivf mgr map", KR(ret), K(ls_id));
  } else if (OB_FAIL(mem_sync_info_.init(hash_capacity, tenant_id, ls_id))) {
    LOG_WARN("fail to create first mem sync set", K(ls_id), KR(ret));
  } else if (OB_FAIL(async_task_opt_.init(hash_capacity, tenant_id, ls_id))) {
    LOG_WARN("fail to create async task option", KR(ret), K(ls_id));
  } else {
    ls_tablet_task_ctx_.task_id_ = 0;
    ls_tablet_task_ctx_.non_memdata_task_cycle_ = 0;
    ls_tablet_task_ctx_.need_memdata_sync_ = false;
    ls_tablet_task_ctx_.state_ = OB_TTL_TASK_PREPARE;
    need_check_ = false;
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    memory_context_ = memory_context;
    all_vsag_use_mem_ = all_vsag_use_mem;
    identity_ts_ = ObTimeUtility::current_time();
    ATOMIC_STORE(&is_ls_destroying_, false);
    ATOMIC_STORE(&vec_idx_async_bind_stopped_, false);
    is_inited_ = true;
  }
  return ret;
}

int ObPluginVectorIndexMgr::try_reuse_adaptor_from_tenant_map(
    ObTabletID inc_tablet_id, bool &reused)
{
  int ret = OB_SUCCESS;
  reused = false;
  if (OB_UNLIKELY(!inc_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant adapter reuse argument", KR(ret), K(inc_tablet_id));
  } else if (OB_ISNULL(vector_index_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index service is null", KR(ret), K(ls_id_), K(inc_tablet_id));
  } else {
    ObPluginVectorIndexAdapterGuard ls_adaptor_guard;
    ObPluginVectorIndexAdapterGuard tenant_adaptor_guard;
    if (OB_FAIL(get_adapter_inst_guard(inc_tablet_id, ls_adaptor_guard))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("adaptor detached from ls_map during try_reuse", K(ret),K(inc_tablet_id));
      } else {
        LOG_WARN("failed to get adapter instance guard", KR(ret), K(inc_tablet_id));
      }
    } else if (OB_FAIL(vector_index_service_->get_tenant_adapter_inst_guard(
        inc_tablet_id, tenant_adaptor_guard))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ObPluginVectorIndexAdaptor *ls_adaptor = ls_adaptor_guard.get_adatper();
        if (OB_ISNULL(ls_adaptor)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls adaptor is null", KR(ret), K(inc_tablet_id));
        } else if (OB_FAIL(vector_index_service_->set_tenant_vec_adaptor(
            inc_tablet_id, ls_adaptor))) {
          LOG_WARN("failed to insert to tenant map", KR(ret), K(inc_tablet_id));
        } else {
          LOG_INFO("insert ls adaptor to tenant map",
              K(inc_tablet_id), KPC(ls_adaptor));
        }
      } else {
        LOG_WARN("failed to get from tenant map", KR(ret), K(inc_tablet_id));
      }
    } else if (OB_NOT_NULL(tenant_adaptor_guard.get_adatper())) {
      ObPluginVectorIndexAdaptor *tenant_adaptor = tenant_adaptor_guard.get_adatper();
      ObAdaptorMigState tenant_state = tenant_adaptor->get_mig_state();
      bool detached = false;
      bool attached = false;
      if (ObAdaptorMigState::MIG_INVALID != tenant_state) {
        if (OB_FAIL(detach_adapter(inc_tablet_id, detached))) {
          LOG_WARN("failed to detach old ls adapter", KR(ret), K(inc_tablet_id));
        } else if (OB_FAIL(attach_adapter(tenant_adaptor, attached))) {
          LOG_WARN("failed to attach tenant adaptor to ls map",
              KR(ret), K(inc_tablet_id));
        }
        if (OB_SUCC(ret)) {
          reused = true;
          LOG_INFO("reused migrating tenant adaptor, skip fetch",
              K(inc_tablet_id), K(tenant_state), KPC(tenant_adaptor));
        }
      } else {
        ObPluginVectorIndexAdaptor *ls_adaptor = ls_adaptor_guard.get_adatper();
        if (OB_ISNULL(ls_adaptor)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls adaptor is null", KR(ret), K(inc_tablet_id));
        } else if (OB_FAIL(vector_index_service_->erase_tenant_vec_adaptor(inc_tablet_id))) {
          LOG_WARN("failed to erase old tenant adaptor", KR(ret), K(inc_tablet_id));
        } else if (OB_FAIL(vector_index_service_->set_tenant_vec_adaptor(
            inc_tablet_id, ls_adaptor))) {
          LOG_WARN("failed to set ls adaptor to tenant map", KR(ret), K(inc_tablet_id));
        } else {
          LOG_INFO("replaced MIG_INVALID tenant adaptor with ls shell",
              K(inc_tablet_id), KPC(ls_adaptor));
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::set_vec_adaptor_(ObTabletID tablet_id,
                                             ObPluginVectorIndexAdaptor *adapter_inst,
                                             int overwrite)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vec_adaptor_map_.set_refactored(tablet_id, adapter_inst, overwrite))) {
    LOG_WARN("failed to set vector index adaptor", K(tablet_id), KR(ret));
  } else {
    adapter_inst->inc_ref();
  }
  return ret;
}

int ObPluginVectorIndexMgr::attach_adapter(ObPluginVectorIndexAdaptor *adapter_inst,
                                           bool &attached,
                                           int overwrite)
{
  int ret = OB_SUCCESS;
  attached = false;
  if (OB_ISNULL(adapter_inst)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adapter is null", KR(ret));
  } else {
    WLockGuard lock_guard(adapter_map_rwlock_);
    ret = attach_adapter_in_lock_(adapter_inst, attached, overwrite);
  }
  return ret;
}

int ObPluginVectorIndexMgr::attach_adapter_in_lock_(ObPluginVectorIndexAdaptor *adapter_inst,
                                                    bool &attached,
                                                    int overwrite)
{
  int ret = OB_SUCCESS;
  attached = false;
  if (OB_FAIL(set_vec_adaptor_(adapter_inst->get_inc_tablet_id(), adapter_inst, overwrite))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to attach adapter into ls map", KR(ret), K(ls_id_), K(adapter_inst->get_inc_tablet_id()));
    }
  } else {
    adapter_inst->inc_ls_ref();
    adapter_inst->reset_zero_ref_time();
    attached = true;
  }
  return ret;
}

int ObPluginVectorIndexMgr::erase_vec_adaptor(ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  bool detached = false;
  if (OB_FAIL(detach_adapter_in_lock_(tablet_id, detached))) {
    LOG_WARN("failed to erase vector index adaptor", K(tablet_id), KR(ret));
  }
  return ret;
}

int ObPluginVectorIndexMgr::detach_adapter(ObTabletID tablet_id,
                                           bool &detached)
{
  int ret = OB_SUCCESS;
  WLockGuard lock_guard(adapter_map_rwlock_);
  ret = detach_adapter_in_lock_(tablet_id, detached);
  return ret;
}

int ObPluginVectorIndexMgr::detach_adapter_in_lock_(ObTabletID tablet_id,
                                                    bool &detached)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdaptor *adapter_inst = nullptr;
  detached = false;
  if (OB_FAIL(vec_adaptor_map_.erase_refactored(tablet_id, &adapter_inst))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to detach vector index adaptor", K(tablet_id), KR(ret));
    }
  } else if (OB_ISNULL(adapter_inst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("adapter inst is null", K(tablet_id), KR(ret));
  } else {
    adapter_inst->dec_ls_ref();
    detached = true;
    if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(adapter_inst))) {
      LOG_WARN("fail to release vector index adapter", K(tablet_id), KR(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::erase_ivf_build_helper(const ObIvfHelperKey &key, bool *fully_cleared)
{
  int ret = OB_SUCCESS;
  ObIvfBuildHelper *helper_inst = nullptr;
  if (OB_FAIL(ivf_index_helper_map_.erase_refactored(key, &helper_inst))) {
    LOG_WARN("failed to erase ivf build helper", K(key), KR(ret));
  } else if (OB_ISNULL(helper_inst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("helper inst is null", K(key), KR(ret));
  } else {
    bool fully_released = false;
    if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_build_helper(helper_inst, &fully_released))) {
      LOG_WARN("fail to release ivf build helper", K(key), KR(ret));
    } else if (nullptr != fully_cleared) {
      *fully_cleared = fully_released;
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_adapter_inst_guard(ObTabletID tablet_id, ObPluginVectorIndexAdapterGuard &adpt_guard)
{
  int ret = OB_SUCCESS;
  RLockGuard lock_guard(adapter_map_rwlock_);

  ObPluginVectorIndexAdaptor *index_inst = nullptr;
  if (OB_FAIL(get_adapter_inst_(tablet_id, index_inst))) {
    LOG_WARN("failed to get adapter inst", K(tablet_id), KR(ret));
  } else if (OB_FAIL(adpt_guard.set_adapter(index_inst))) {
    LOG_WARN("failed to set adapter", K(tablet_id), KR(ret));
  } else {
    int64_t mgr_ts = get_identity_ts();
    if (index_inst->need_identity_refresh(mgr_ts)) {
      index_inst->do_identity_refresh(mgr_ts);
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_adapter_inst_guard_in_lock(ObTabletID tablet_id, ObPluginVectorIndexAdapterGuard &adpt_guard)
{
  int ret = OB_SUCCESS;

  ObPluginVectorIndexAdaptor *index_inst = nullptr;
  if (OB_FAIL(get_adapter_inst_(tablet_id, index_inst))) {
    LOG_WARN("failed to get adapter inst", K(tablet_id), KR(ret));
  } else if (OB_FAIL(adpt_guard.set_adapter(index_inst))) {
    LOG_WARN("failed to set adapter", K(tablet_id), KR(ret));
  } else {
    int64_t mgr_ts = get_identity_ts();
    if (index_inst->need_identity_refresh(mgr_ts)) {
      index_inst->do_identity_refresh(mgr_ts);
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_inc_tablet_id_by_vbitmap(
    const int64_t vbitmap_table_id,
    const common::ObTabletID &vbitmap_tablet_id,
    common::ObTabletID &inc_tablet_id)
{
  int ret = OB_HASH_NOT_EXIST;
  RLockGuard lock_guard(adapter_map_rwlock_);
  FOREACH_X(iter, vec_adaptor_map_, OB_HASH_NOT_EXIST == ret) {
    ObPluginVectorIndexAdaptor *adapter = iter->second;
    if (OB_NOT_NULL(adapter)
        && adapter->get_vbitmap_table_id() == vbitmap_table_id
        && adapter->get_vbitmap_tablet_id() == vbitmap_tablet_id
        && adapter->is_inc_tablet_valid()) {
      inc_tablet_id = adapter->get_inc_tablet_id();
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::collect_adaptor_tablet_ids(ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObCollectAdaptorTabletIdCallback callback(tablet_ids);
  if (OB_FAIL(vec_adaptor_map_.foreach_refactored(callback))) {
    LOG_WARN("failed to foreach adaptor map", KR(ret), K(ls_id_));
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_adapter_inst_(ObTabletID tablet_id, ObPluginVectorIndexAdaptor *&index_inst)
{
  int ret = OB_SUCCESS;
  index_inst = nullptr;

  if (OB_FAIL(vec_adaptor_map_.get_refactored(tablet_id, index_inst))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index inst", K(tablet_id), KR(ret));
    }
  } else if (OB_ISNULL(index_inst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index inst", K(tablet_id), KR(ret));
  }

  return ret;
}

int ObPluginVectorIndexMgr::get_build_helper_inst_(const ObIvfHelperKey &key, ObIvfBuildHelper *&helper_inst)
{
  int ret = OB_SUCCESS;
  helper_inst = nullptr;

  if (OB_FAIL(ivf_index_helper_map_.get_refactored(key, helper_inst))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get ivf index build helper inst", K(key), KR(ret));
    }
  } else if (OB_ISNULL(helper_inst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ivf index build helper inst", K(key), KR(ret));
  }

  return ret;
}

int ObPluginVectorIndexMgr::get_build_helper_inst_guard(const ObIvfHelperKey &key, ObIvfBuildHelperGuard &helper_guard)
{
  int ret = OB_SUCCESS;
  ObIvfBuildHelper *helper_inst = nullptr;
  if (OB_FAIL(get_build_helper_inst_(key, helper_inst))) {
    LOG_WARN("failed to get adapter inst", K(key), KR(ret));
  } else if (OB_FAIL(helper_guard.set_helper(helper_inst))) {
    LOG_WARN("failed to set adapter", K(key), KR(ret));
  }
  return ret;
}

int ObPluginVectorIndexMgr::create_ivf_build_helper(
    const ObIvfHelperKey &key,
    ObIndexType type,
    ObString &vec_index_param,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObIvfBuildHelper *tmp_ivf_build_helper = nullptr;
  void *helper_buff = nullptr;
  if (INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL == type) {
    if (OB_ISNULL(helper_buff = allocator.alloc(sizeof(ObIvfFlatBuildHelper)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ivf index build helper", KR(ret));
    } else {
      tmp_ivf_build_helper = new(helper_buff)ObIvfFlatBuildHelper(&allocator, tenant_id_);
      if (OB_FAIL(tmp_ivf_build_helper->init(vec_index_param, memory_context_, all_vsag_use_mem_))) {
        LOG_WARN("failed to init ivf build helper", K(ret));
      }
    }
  } else if (INDEX_TYPE_VEC_IVFSQ8_META_LOCAL == type) {
    if (OB_ISNULL(helper_buff = allocator.alloc(sizeof(ObIvfSq8BuildHelper)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ivf index build helper", KR(ret));
    } else {
      tmp_ivf_build_helper = new(helper_buff)ObIvfSq8BuildHelper(&allocator, tenant_id_);
      if (OB_FAIL(tmp_ivf_build_helper->init(vec_index_param, memory_context_, all_vsag_use_mem_))) {
        LOG_WARN("failed to init ivf build helper", K(ret), K(vec_index_param));
      }
    }
  } else if (INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL == type) {
    if (OB_ISNULL(helper_buff = allocator.alloc(sizeof(ObIvfPqBuildHelper)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ivf index build helper", KR(ret));
    } else {
      tmp_ivf_build_helper = new(helper_buff)ObIvfPqBuildHelper(&allocator, tenant_id_);
      if (OB_FAIL(tmp_ivf_build_helper->init(vec_index_param, memory_context_, all_vsag_use_mem_))) {
        LOG_WARN("failed to init ivf build helper", K(ret), K(vec_index_param));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported index type", K(ret), K(type));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_ivf_build_helper_(key, tmp_ivf_build_helper))) {
      LOG_WARN("set ivf index build helper faild", K(key), KR(ret));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_ivf_build_helper)) {
    tmp_ivf_build_helper->~ObIvfBuildHelper();
    allocator.free(helper_buff);
    tmp_ivf_build_helper = nullptr;
    helper_buff = nullptr;
  }
  return ret;
}

int ObPluginVectorIndexMgr::set_ivf_build_helper_(const ObIvfHelperKey &key, ObIvfBuildHelper *helper_inst)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ivf_index_helper_map_.set_refactored(key, helper_inst))) {
    LOG_WARN("failed to set ivf build helper", K(key), KR(ret));
  } else {
    helper_inst->inc_ref();
  }
  return ret;
}

int ObPluginVectorIndexMgr::acquire_adapter_by_ctx(ObVectorIndexAcquireCtx &ctx,
                                                   ObIAllocator &allocator,
                                                   ObPluginVectorIndexAdapterGuard &adapter_guard,
                                                   ObString *vec_index_param,
                                                   int64_t dim)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdaptor *adapter = nullptr;
  bool became_complete = false;
  if (OB_UNLIKELY(!ctx.is_inc_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ctx, inc_tablet_id is not valid", K(ret), K(ctx));
  } else {
    RLockGuard lock_guard(adapter_map_rwlock_);
    if (OB_FAIL(vec_adaptor_map_.get_refactored(ctx.inc_tablet_id_, adapter))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        adapter = nullptr;
      } else {
        LOG_WARN("failed to get adapter from complete map", K(ret), K(ctx.inc_tablet_id_));
      }
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_NOT_NULL(adapter)) {
    // adapter already exists, try fill tablet info and return
    if (OB_FAIL(adapter->try_fill_tablet_info(ctx, became_complete))) {
      LOG_WARN("failed to try fill tablet info", K(ret), K(ctx), KPC(adapter));
    } else if (OB_FAIL(adapter_guard.set_adapter(adapter))) {
      LOG_WARN("failed to set adapter", K(ret), K(adapter_guard));
    }
  } else {
    // create new adapter with inc_tablet_id as key, then fill remaining info from ctx
    void *adpt_buff = allocator.alloc(sizeof(ObPluginVectorIndexAdaptor));
    if (OB_ISNULL(adpt_buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for vector index adapter", K(ret));
    } else {
      ObPluginVectorIndexAdaptor *new_adapter =
          new(adpt_buff)ObPluginVectorIndexAdaptor(&allocator, memory_context_, tenant_id_);
      if ((OB_ISNULL(vec_index_param) || vec_index_param->empty())
          && OB_FAIL(new_adapter->init(memory_context_, all_vsag_use_mem_))) {
        LOG_WARN("failed to init adaptor", K(ret));
      } else if ((OB_NOT_NULL(vec_index_param) && !vec_index_param->empty())
          && OB_FAIL(new_adapter->init(*vec_index_param, dim, memory_context_, all_vsag_use_mem_))) {
        LOG_WARN("failed to init adaptor", K(ret), KPC(vec_index_param), K(dim));
      } else if (OB_FAIL(new_adapter->set_tablet_id(VIRT_INC, ctx.inc_tablet_id_))) {
        LOG_WARN("failed to set inc tablet id", K(ret), K(ctx));
      } else if (OB_FAIL(new_adapter->try_fill_tablet_info(ctx, became_complete))) {
        LOG_WARN("failed to fill tablet info from ctx", K(ret), K(ctx));
      } else {
        WLockGuard lock_guard(adapter_map_rwlock_);
        if (OB_FAIL(set_vec_adaptor_(ctx.inc_tablet_id_, new_adapter))) {
          if (OB_HASH_EXIST == ret) {
            // another thread created it, try get again
            ret = OB_SUCCESS;
            ObPluginVectorIndexAdaptor *existing = nullptr;
            if (OB_FAIL(vec_adaptor_map_.get_refactored(ctx.inc_tablet_id_, existing))) {
              LOG_WARN("failed to get existing adapter", K(ret), K(ctx.inc_tablet_id_));
            } else {
              if (OB_FAIL(existing->try_fill_tablet_info(ctx, became_complete))) {
                LOG_WARN("failed to try fill tablet info", K(ret), K(ctx));
              } else if (OB_FAIL(adapter_guard.set_adapter(existing))) {
                LOG_WARN("failed to set adapter", K(ret), K(adapter_guard));
              }
            }
            new_adapter->~ObPluginVectorIndexAdaptor();
            allocator.free(adpt_buff);
            new_adapter = nullptr;
            adpt_buff = nullptr;
          } else {
            LOG_WARN("failed to set vector adaptor", K(ret), K(ctx.inc_tablet_id_));
          }
        } else {
          // set_vec_adaptor_ succeeded: this adaptor is now structurally attached
          // to the LS map, so bump ls_ref to stay consistent with
          // attach_adapter_in_lock_ (detach_adapter_in_lock_ and
          // release_all_adapters always dec_ls_ref on removal).
          new_adapter->inc_ls_ref();
          if (OB_FAIL(adapter_guard.set_adapter(new_adapter))) {
            LOG_WARN("failed to set adapter", K(ret), K(adapter_guard));
            // new_adapter is already in the LS map; detach it (erase + dec_ls_ref
            // + release) instead of destructing it directly in the fallback below,
            // otherwise the map would keep a dangling pointer to freed memory.
            bool detached = false;
            const int tmp_ret = detach_adapter_in_lock_(ctx.inc_tablet_id_, detached);
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("failed to detach adapter after set guard failed",
                  K(tmp_ret), K(ctx.inc_tablet_id_));
            }
            // detach released new_adapter (ref_cnt dropped to 0); clear local
            // pointers so the fallback cleanup below does not double free.
            new_adapter = nullptr;
            adpt_buff = nullptr;
          } else {
            new_adapter->set_identity_ts(get_identity_ts());
            FLOG_INFO("[VECTOR INDEX ADAPTOR] create adaptor by ctx success",
                KP(new_adapter), K(ctx), K(new_adapter->get_ready_state()), K(lbt()));
          }
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(new_adapter)) {
        new_adapter->~ObPluginVectorIndexAdaptor();
        allocator.free(adpt_buff);
        new_adapter = nullptr;
        adpt_buff = nullptr;
      }
    }
  }
  if (OB_SUCC(ret) && became_complete) {
    on_adapter_complete(adapter_guard.get_adatper());
  }
  return ret;
}

int ObPluginVectorIndexService::get_tenant_adapter_inst_guard(
    ObTabletID tablet_id,
    ObPluginVectorIndexAdapterGuard &adpt_guard)
{
  int ret = OB_SUCCESS;
  ObHoldAdaptorRefCallback callback;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tenant_adaptor_map_.read_atomic(tablet_id, callback))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to read tenant adaptor map", KR(ret), K(tablet_id));
    }
  } else if (OB_ISNULL(callback.get_adapter())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant adaptor callback returned null adapter", KR(ret), K(tablet_id));
  } else if (OB_FAIL(adpt_guard.set_adapter_with_ref_hold(callback.get_adapter()))) {
    ObPluginVectorIndexAdaptor *adapter = callback.get_adapter();
    int tmp_ret = ObPluginVectorIndexUtils::release_vector_index_adapter(adapter);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to release tenant adaptor after guard adopt failure", K(tmp_ret), K(tablet_id));
    }
    LOG_WARN("failed to adopt tenant adaptor", KR(ret), K(tablet_id));
  }
  return ret;
}

int ObPluginVectorIndexService::set_tenant_vec_adaptor(ObTabletID tablet_id,
                                                       ObPluginVectorIndexAdaptor *adapter_inst,
                                                       int overwrite)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(adapter_inst) || OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant adaptor set arguments", KR(ret), K(tablet_id), KP(adapter_inst));
  } else if (OB_FAIL(tenant_adaptor_map_.set_refactored(tablet_id, adapter_inst, overwrite))) {
    LOG_WARN("failed to set tenant adaptor", KR(ret), K(tablet_id));
  } else {
    adapter_inst->inc_ref();
  }
  return ret;
}

int ObPluginVectorIndexService::erase_tenant_vec_adaptor(ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdaptor *adapter_inst = nullptr;
  if (OB_FAIL(tenant_adaptor_map_.erase_refactored(tablet_id, &adapter_inst))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to erase tenant adaptor", KR(ret), K(tablet_id));
    }
  } else if (OB_ISNULL(adapter_inst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant adaptor is null", KR(ret), K(tablet_id));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(adapter_inst))) {
    LOG_WARN("failed to release tenant adaptor", KR(ret), K(tablet_id));
  }
  return ret;
}

int ObPluginVectorIndexMgr::check_need_mem_data_sync_task(bool &need_sync,
                                                          bool can_release_processing_ctx)
{
  int ret = OB_SUCCESS;
  need_sync = false;
  common::ObSpinLockGuard guard(task_ctx_lock_);
  if (OB_FAIL(mem_sync_info_.promote_due_deferred_tasks())) {
    LOG_WARN("fail to promote deferred mem sync tasks", KR(ret), K(ls_id_));
  } else {
    mem_sync_info_.check_and_switch_if_needed(
        need_sync, ls_tablet_task_ctx_.all_finished_, can_release_processing_ctx);
  }
  if (need_sync) {
    LOG_INFO("memdata sync check", K(ls_id_), K(need_sync), K(ls_tablet_task_ctx_));
  } else {
    LOG_TRACE("memdata sync check", K(ls_id_), K(need_sync), K(ls_tablet_task_ctx_));
  }
  // both map empty, do nothing
  return ret;
}

int ObPluginVectorIndexService::acquire_adapter_guard(ObLSID ls_id,
                                                      ObVectorIndexAcquireCtx &ctx,
                                                      ObPluginVectorIndexAdapterGuard &adapter_guard,
                                                      ObString *vec_index_param,
                                                      int64_t dim)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  bool became_complete = false;
  bool adapter_attached_to_ls = false;
  if (OB_FAIL(acquire_vector_index_mgr(ls_id, ls_index_mgr))) {
    LOG_WARN("failed to acquire vector index mgr", KR(ret), K(ls_id));
  } else if (OB_FAIL(get_tenant_adapter_inst_guard(ctx.inc_tablet_id_, adapter_guard))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to get tenant adaptor", K(ls_id), K(ctx), KR(ret));
    } else {
      ret = OB_SUCCESS;
      void *adpt_buff = adaptor_allocator_.alloc(sizeof(ObPluginVectorIndexAdaptor));
      ObPluginVectorIndexAdaptor *new_adapter = nullptr;
      if (OB_ISNULL(adpt_buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for vector index adapter", K(ret));
      } else {
        new_adapter = new(adpt_buff)ObPluginVectorIndexAdaptor(&adaptor_allocator_, memory_context_, tenant_id_);
        if ((OB_ISNULL(vec_index_param) || vec_index_param->empty())
            && OB_FAIL(new_adapter->init(memory_context_, all_vsag_use_mem_))) {
          LOG_WARN("failed to init adaptor", K(ret));
        } else if ((OB_NOT_NULL(vec_index_param) && !vec_index_param->empty())
            && OB_FAIL(new_adapter->init(*vec_index_param, dim, memory_context_, all_vsag_use_mem_))) {
          LOG_WARN("failed to init adaptor", K(ret), KPC(vec_index_param), K(dim));
        } else if (OB_FAIL(new_adapter->set_tablet_id(VIRT_INC, ctx.inc_tablet_id_))) {
          LOG_WARN("failed to set inc tablet id", K(ret), K(ctx));
        } else if (OB_FAIL(new_adapter->try_fill_tablet_info(ctx, became_complete))) {
          LOG_WARN("failed to fill tablet info from ctx", K(ret), K(ctx));
        } else if (FALSE_IT(new_adapter->set_identity_ts(ls_index_mgr->get_identity_ts()))) {
        } else if (OB_FAIL(set_tenant_vec_adaptor(ctx.inc_tablet_id_, new_adapter))) {
          if (ret == OB_HASH_EXIST) {
            ret = OB_SUCCESS;
            new_adapter->~ObPluginVectorIndexAdaptor();
            adaptor_allocator_.free(adpt_buff);
            new_adapter = nullptr;
            adpt_buff = nullptr;
            if (OB_FAIL(get_tenant_adapter_inst_guard(ctx.inc_tablet_id_, adapter_guard))) {
              LOG_WARN("failed to get existing tenant adaptor", KR(ret), K(ctx.inc_tablet_id_));
            } else {
              if (OB_FAIL(adapter_guard.get_adatper()->try_fill_tablet_info(ctx, became_complete))) {
                LOG_WARN("failed to fill existing tenant adaptor", KR(ret), K(ctx));
              }
            }
          } else {
            LOG_WARN("failed to set tenant adaptor", KR(ret), K(ctx.inc_tablet_id_));
          }
        } else if (OB_FAIL(adapter_guard.set_adapter(new_adapter))) {
          int tmp_ret = erase_tenant_vec_adaptor(ctx.inc_tablet_id_);
          new_adapter = nullptr;
          adpt_buff = nullptr;
          if (OB_SUCCESS != tmp_ret && OB_HASH_NOT_EXIST != tmp_ret) {
            LOG_WARN("failed to rollback tenant adaptor after guard set failed", KR(tmp_ret), K(ctx.inc_tablet_id_));
          }
          LOG_WARN("failed to set adapter guard", KR(ret), K(ctx));
        } else {
          FLOG_INFO("[VECTOR INDEX ADAPTOR] create tenant adaptor by ctx success",
              KP(new_adapter), K(ctx), K(new_adapter->get_ready_state()), K(lbt()));
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(new_adapter)) {
          new_adapter->~ObPluginVectorIndexAdaptor();
          adaptor_allocator_.free(adpt_buff);
          new_adapter = nullptr;
          adpt_buff = nullptr;
        }
      }
    }
  } else {
    if (OB_FAIL(adapter_guard.get_adatper()->try_fill_tablet_info(ctx, became_complete))) {
      LOG_WARN("failed to fill tenant adaptor info", K(ls_id), K(ctx), KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObPluginVectorIndexAdapterGuard local_guard;
    bool attached = false;
    if (OB_FAIL(ls_index_mgr->get_adapter_inst_guard(ctx.inc_tablet_id_, local_guard))) {
      if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
        if (OB_FAIL(ls_index_mgr->attach_adapter(adapter_guard.get_adatper(), attached))) {
          ls_index_mgr->set_last_transfer_scn(share::SCN::invalid_scn());
          LOG_WARN("failed to attach adaptor into ls map", KR(ret), K(ls_id), K(ctx.inc_tablet_id_));
        } else {
          adapter_attached_to_ls = attached;
        }
      } else {
        LOG_WARN("failed to check local adaptor", KR(ret), K(ls_id), K(ctx.inc_tablet_id_));
      }
    } else if (local_guard.get_adatper() != adapter_guard.get_adatper()) {
      ls_index_mgr->set_last_transfer_scn(share::SCN::invalid_scn());
    } else {
      adapter_attached_to_ls = true;
    }
  }
  if (OB_SUCC(ret) && became_complete && adapter_attached_to_ls) {
    ls_index_mgr->on_adapter_complete(adapter_guard.get_adatper());
  }

  return ret;
}

int ObPluginVectorIndexService::acquire_ivf_build_helper_guard(
    ObLSID ls_id,
    const ObIvfHelperKey &key,
    ObIndexType type,
    ObIvfBuildHelperGuard &helper_guard,
    ObString &vec_index_param)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  if (OB_FAIL(get_ls_index_mgr_map().get_refactored(ls_id, ls_index_mgr))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index mgr for ls", K(ret), K(ls_id));
    } else { // create new ls index mgr if not exist
      ret = OB_SUCCESS;
      if (OB_FAIL(create_ivf_build_helper(ls_id, key, type, vec_index_param))) {
        LOG_WARN("failed to create tmp vector index instance", K(ret), K(ls_id), K(key), K(type), K(vec_index_param));
      }
      if (OB_FAIL(ret) && (OB_HASH_EXIST != ret)) {
      } else if (OB_FAIL(get_build_helper_inst_guard(ls_id, key, helper_guard))) {
        LOG_WARN("failed to get tmp ivf build helper instance", K(ret), K(ls_id), K(key), K(type));
      } else {
        LOG_INFO("create ivf build helper success", K(ret), K(ls_id), KPC(helper_guard.get_helper()));
      }
    }
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_index_mgr->get_build_helper_inst_guard(key, helper_guard))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get ivf build helper", KR(ret), K(ls_id), K(key));
    } else { // not exist create new
      if (OB_FAIL(ls_index_mgr->create_ivf_build_helper(key, type, vec_index_param, ivf_allocator_))) {
        LOG_WARN("failed to create tmp vector index instance with ls", KR(ret), K(ls_id), K(key), K(type));
      }
      if (OB_FAIL(ret) && (OB_HASH_EXIST != ret)) {
      } else if (OB_FAIL(ls_index_mgr->get_build_helper_inst_guard(key, helper_guard))) {
        LOG_WARN("failed to get tmp vector index instance with ls", KR(ret), K(ls_id), K(key), K(type));
      } else {
        LOG_INFO("create partial index adapter success", K(ret), K(ls_id), K(key), KPC(helper_guard.get_helper()));
      }
    }
  } else {
    // get from existed ls index mgr
  }

  return ret;
}

int ObPluginVectorIndexService::acquire_adapter_guard(ObLSID ls_id,
                                                      ObTabletID tablet_id,
                                                      ObIndexType type,
                                                      ObPluginVectorIndexAdapterGuard &adapter_guard,
                                                      ObString *vec_index_param,
                                                      int64_t dim)
{
  int ret = OB_SUCCESS;
  UNUSED(type);
  UNUSED(vec_index_param);
  UNUSED(dim);
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  if (OB_FAIL(get_ls_index_mgr_map().get_refactored(ls_id, ls_index_mgr))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index mgr for ls", KR(ret), K(ls_id));
    }
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_index_mgr->get_adapter_inst_guard(tablet_id, adapter_guard))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index adapter", K(ls_id), K(tablet_id), KR(ret));
    } else {
      ret = get_tenant_adapter_inst_guard(tablet_id, adapter_guard);
    }
  }

  return ret;
}

int ObPluginVectorIndexService::get_adapter_inst_guard(ObLSID ls_id,
                                                       ObTabletID tablet_id,
                                                       ObPluginVectorIndexAdapterGuard &adpt_guard)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  if (OB_FAIL(get_ls_index_mgr_map().get_refactored(ls_id, ls_index_mgr))) {
    LOG_WARN("failed to get vector index mgr for ls", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_index_mgr->get_adapter_inst_guard(tablet_id, adpt_guard))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = get_tenant_adapter_inst_guard(tablet_id, adpt_guard);
    }
    if (ret != OB_SUCCESS && ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to get vector index adapter", K(ls_id), K(tablet_id), KR(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexService::get_build_helper_inst_guard(
    ObLSID ls_id,
    const ObIvfHelperKey &key,
    ObIvfBuildHelperGuard &helper_guard)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  if (OB_FAIL(get_ls_index_mgr_map().get_refactored(ls_id, ls_index_mgr))) {
    LOG_WARN("failed to get vector index mgr for ls", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_index_mgr->get_build_helper_inst_guard(key, helper_guard))) {
    LOG_WARN("failed to get ivf build helper guard", KR(ret), K(ls_id), K(key));
  }
  return ret;
}


int ObPluginVectorIndexService::release_vector_index_tmp_info(const int64_t task_id)
{
  int ret = OB_SUCCESS;
  ObVectorIndexTmpInfo *tmp_info = nullptr;
  if (task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id));
  } else if (OB_FAIL(get_vector_index_tmp_info_map().erase_refactored(task_id, &tmp_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to erase vector index tmp info", K(ret), K(task_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(task_id), K(tmp_info));
  } else {
    tmp_info->reset();
    tmp_info->~ObVectorIndexTmpInfo();
    tmp_info_allocator_.free(tmp_info);
    tmp_info = nullptr;
  }
  return ret;
}

int ObPluginVectorIndexService::get_vector_index_tmp_info(const int64_t task_id, ObVectorIndexTmpInfo *&tmp_info, const bool get_from_exist)
{
  int ret = OB_SUCCESS;
  tmp_info = nullptr;
  if (task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id));
  } else if (OB_FAIL(get_vector_index_tmp_info_map().get_refactored(task_id, tmp_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (get_from_exist) {
        LOG_WARN("vector tmp_info hash not exist", K(ret), K(task_id));
      } else {
        void *tmp_info_buff = tmp_info_allocator_.alloc(sizeof(ObVectorIndexTmpInfo));
        if (OB_ISNULL(tmp_info_buff)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memeory for new vector index tmp info", K(ret), K(task_id));
        } else {
          ObVectorIndexTmpInfo *new_tmp_info = new(tmp_info_buff)ObVectorIndexTmpInfo();
          if (OB_FAIL(get_vector_index_tmp_info_map().set_refactored(task_id, new_tmp_info))) {
            LOG_WARN("set vector index tmp info faild", K(ret), K(task_id));
          } else {
            tmp_info = new_tmp_info;
          }
          if (OB_FAIL(ret)) {
            new_tmp_info->~ObVectorIndexTmpInfo();
            tmp_info_allocator_.free(tmp_info_buff);
            tmp_info_buff = nullptr;
          }
        }
      }
    }
  } else if (OB_ISNULL(tmp_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(tmp_info));
  }

  return ret;
}

int ObPluginVectorIndexService::remove_ls_index_mgr(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  {
    // Called after dag_ref_cnt_ reaches zero, so no Local-LS task holds this mgr.
    // WLock serializes erase with RLock-protected cross-LS scans of
    // index_ls_mgr_map_ (snapshot/cache collection, transfer maintenance checks,
    // async task memory gating and tenant-task cancellation), whose readers may
    // keep mgr-internal pointers valid only while holding RLock.
    // Lock scope covers only erase; destroy()+free() happen outside to avoid
    // holding WLock across slow mgr teardown.
    TCRWLock::WLockGuard lock_guard(ls_mgr_map_rwlock_);
    if (OB_FAIL(index_ls_mgr_map_.erase_refactored(ls_id, &ls_index_mgr))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to erase ls index mgr", KR(ret), K(ls_id));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(ls_index_mgr)) {
    ls_index_mgr->destroy();
    index_mgr_allocator_.free(ls_index_mgr);
  }
  return ret;
}

int ObPluginVectorIndexService::attach_tenant_adaptors_to_ls_map_for_destroy(
    const share::ObLSID &ls_id,
    const common::ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  bool is_oracle_mode = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index service is not inited", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(ls_id));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to check oracle mode", KR(ret), K_(tenant_id), K(ls_id));
  } else if (is_oracle_mode) {
    // Oracle tenants never run vector index maintenance (see
    // ObPluginVectorIndexLoadScheduler::check_can_do_work), so no tenant adaptor
    // should exist. Skip to avoid accidentally creating an ls_mgr that would
    // never advance last_transfer_scn.
  } else if (OB_FAIL(get_ls_index_mgr_map().get_refactored(ls_id, ls_index_mgr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("[VEC_INDEX][LS_DESTROY] ls vector index mgr not exist before tablet destroy",
          K(ls_id), "tablet_count", tablet_ids.count());
    } else {
      LOG_WARN("failed to get vector index mgr before tablet destroy", KR(ret), K(ls_id));
    }
  }


  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ls_index_mgr)) {
    LOG_INFO("get null vector index mgr before tablet destroy", KR(ret), K(ls_id));
  } else {
    LOG_INFO("[VEC_INDEX][LS_DESTROY] prepare tenant adaptors before tablet destroy",
        K(ls_id), KP(ls_index_mgr), "tablet_count", tablet_ids.count(), K(tablet_ids));
    lib::ObMutexGuard guard(ls_index_mgr->get_maintenance_lock());
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      ObPluginVectorIndexAdapterGuard local_guard;
      ObPluginVectorIndexAdapterGuard tenant_guard;
      if (OB_FAIL(get_tenant_adapter_inst_guard(tablet_id, tenant_guard))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[VEC_INDEX][LS_DESTROY] failed to get tenant adaptor before tablet destroy",
              KR(ret), K(ls_id), K(tablet_id));
        }
      } else if (OB_ISNULL(tenant_guard.get_adatper())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[VEC_INDEX][LS_DESTROY] tenant adaptor is null before tablet destroy",
            KR(ret), K(ls_id), K(tablet_id));
      } else {
        bool need_attach = false;
        if (OB_FAIL(ls_index_mgr->get_adapter_inst_guard(tablet_id, local_guard))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            need_attach = true;
          } else {
            LOG_WARN("[VEC_INDEX][LS_DESTROY] failed to get local adaptor before tablet destroy",
                KR(ret), K(ls_id), K(tablet_id));
          }
        }
        if (OB_SUCC(ret) && need_attach) {
          bool attached = false;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ls_index_mgr->attach_adapter(tenant_guard.get_adatper(), attached))) {
            LOG_WARN("[VEC_INDEX][LS_DESTROY] failed to attach tenant adaptor before tablet destroy",
                KR(ret), K(ls_id), K(tablet_id), KPC(tenant_guard.get_adatper()));
          } else {
            LOG_INFO("[VEC_INDEX][LS_DESTROY] attached tenant adaptor before tablet destroy",
                K(ls_id), K(tablet_id), K(attached), KPC(tenant_guard.get_adatper()),
                "ls_ref", tenant_guard.get_adatper()->get_ls_ref());
          }
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexService::acquire_vector_index_mgr(ObLSID ls_id,
                                                       ObPluginVectorIndexMgr *&mgr,
                                                       const bool check_ls_exist)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_index_mgr_map().get_refactored(ls_id, mgr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      // Before creating a new mgr, verify the LS is still in ls_map. Skip when the caller
      // already holds a live ObLS* (e.g. LoadScheduler::init during register_to_service_,
      // before add_ls_to_map_).
      if (check_ls_exist) {
        storage::ObLSService *ls_service = MTL(storage::ObLSService *);
        storage::ObLSHandle ls_handle;
        if (OB_ISNULL(ls_service)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[VEC_INDEX] ls service is null before creating vec index mgr",
                   K(ls_id), KR(ret));
        } else if (OB_FAIL(ls_service->get_ls(
                       ls_id, ls_handle, storage::ObLSGetMod::SHARE_MOD))) {
          LOG_WARN("[VEC_INDEX] failed to get ls before creating vec index mgr",
                   K(ls_id), KR(ret));
        } else if (OB_ISNULL(ls_handle.get_ls())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[VEC_INDEX] ls is null before creating vec index mgr",
                   K(ls_id), KR(ret));
        } else if (ls_handle.get_ls()->is_logonly_replica()) {
          ret = OB_REPLICA_NOT_READABLE;
          LOG_INFO("[VEC_INDEX] skip creating vec index mgr for logonly replica",
                   K(ls_id), KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        void *mgr_buff = index_mgr_allocator_.alloc(sizeof(ObPluginVectorIndexMgr));
        if (OB_ISNULL(mgr_buff)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memeory for new vector index mgr", KR(ret));
        } else {
          ObPluginVectorIndexMgr *new_ls_index_mgr = new(mgr_buff)ObPluginVectorIndexMgr(memory_context_, tenant_id_);
          if (OB_FAIL(new_ls_index_mgr->init(tenant_id_, ls_id, memory_context_, all_vsag_use_mem_))) {
            LOG_WARN("failed to init ls vector index mgr", KR(ret), K(ls_id));
          } else {
            new_ls_index_mgr->set_vector_index_service(this);
            if (OB_FAIL(get_ls_index_mgr_map().set_refactored(ls_id, new_ls_index_mgr))) {
              if (ret != OB_HASH_EXIST) {
                LOG_WARN("set vector index mgr map faild", KR(ret), K(ls_id));
              }
            } else {
              LOG_INFO("[VEC_INDEX] created vector index mgr for ls",
                       K(tenant_id_), K(ls_id), KP(new_ls_index_mgr), K(lbt()));
            }
          }
          if (OB_FAIL(ret)) {
            new_ls_index_mgr->~ObPluginVectorIndexMgr();
            index_mgr_allocator_.free(mgr_buff);
            new_ls_index_mgr = nullptr;
            mgr_buff = nullptr;
          }
          if (OB_FAIL(ret) && (OB_HASH_EXIST != ret)) {
          } else if (OB_FAIL(get_ls_index_mgr_map().get_refactored(ls_id, mgr))) {
            LOG_WARN("failed to get vector index mgr for ls", KR(ret), K(ls_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexService::create_ivf_build_helper(
    ObLSID ls_id,
    const ObIvfHelperKey &key,
    ObIndexType type,
    ObString &vec_index_param)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  ObIvfBuildHelper *tmp_ivf_build_helper = nullptr;

  if (OB_FAIL(acquire_vector_index_mgr(ls_id, ls_index_mgr))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index mgr for ls", KR(ret), K(ls_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_index_mgr->create_ivf_build_helper(key,
                                                           type,
                                                           vec_index_param,
                                                           ivf_allocator_))) {
    LOG_WARN("failed to create ivf build helper", KR(ret), K(ls_id), K(key), K(type), K(vec_index_param));
  }

  return ret;
}

int ObPluginVectorIndexService::erase_ivf_build_helper(ObLSID ls_id, const ObIvfHelperKey &key, bool *fully_cleared)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;

  if (OB_FAIL(acquire_vector_index_mgr(ls_id, ls_index_mgr))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index mgr for ls", KR(ret), K(ls_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_index_mgr->erase_ivf_build_helper(key, fully_cleared))) {
    LOG_WARN("failed to erase ivf build helper", KR(ret), K(ls_id), K(key));
  }

  return ret;
}

ObPluginVectorIndexService::~ObPluginVectorIndexService()
{
  destroy();
}

void ObPluginVectorIndexService::destroy()
{
  if (IS_INIT) {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    FLOG_INFO("destroy vector index service", K_(tenant_id));

    // Stop creating/acquiring new vector objects before reclaiming allocators.
    stop();
    wait();

    // TODO: destory shared tg_id
    if (kmeans_tg_id_ != OB_INVALID_TG_ID) {
      TG_DESTROY(kmeans_tg_id_);
      kmeans_tg_id_ = OB_INVALID_TG_ID;
    }
    if (embedding_tg_id_ != OB_INVALID_TG_ID) {
      TG_DESTROY(embedding_tg_id_);
      embedding_tg_id_ = OB_INVALID_TG_ID;
    }
    // destroy kmeans build task handler
    kmeans_build_task_handler_.destroy();
    // destroy async task handler
    vec_async_task_handle_.destroy();
    // destroy ai execution service before adaptor_allocator_ reset
    ai_execution_service_.destroy();

    FOREACH(iter, vec_idx_tmp_map_) {
      ObVectorIndexTmpInfo *tmp_info = iter->second;
      if (OB_NOT_NULL(tmp_info)) {
        tmp_info->~ObVectorIndexTmpInfo();
        tmp_info_allocator_.free(tmp_info);
        tmp_info = nullptr;
      }
    }
    vec_idx_tmp_map_.destroy();

    vec_idx_async_task_sched_.destroy();
    vec_index_priority_queue_manager_.destroy();

    FOREACH(iter, index_ls_mgr_map_) {
      const ObLSID &ls_id = iter->first;
      ObPluginVectorIndexMgr *ls_index_mgr = iter->second;
      if (OB_NOT_NULL(ls_index_mgr)) {
        ls_index_mgr->destroy();
        index_mgr_allocator_.free(ls_index_mgr);
        ls_index_mgr = nullptr;
      }
    }
    index_ls_mgr_map_.destroy();

    FOREACH(iter, tenant_adaptor_map_) {
      ObPluginVectorIndexAdaptor *adapter = iter->second;
      if (OB_NOT_NULL(adapter)) {
        tmp_ret = ObPluginVectorIndexUtils::release_vector_index_adapter(adapter);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to release tenant vector index adapter", K(tmp_ret), K(iter->first));
          tmp_ret = OB_SUCCESS;
        }
      }
    }
    tenant_adaptor_map_.destroy();

    index_mgr_allocator_.reset();
    ivf_allocator_.reset();
    tmp_info_allocator_.reset();
    adaptor_allocator_.reset();

    is_inited_ = false;
    has_start_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    is_ls_or_tablet_changed_ = false;
    schema_service_ = NULL;
    ls_service_ = NULL;
    sql_proxy_ = NULL;
  }
}

int ObPluginVectorIndexService::init(const uint64_t tenant_id,
                                     schema::ObMultiVersionSchemaService *schema_service,
                                     ObLSService *ls_service)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr(tenant_id, "VecIdxSrv");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(schema_service)
      || OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to init ObPluginVectorIndexService", KR(ret), K(tenant_id));
  } else if (OB_FAIL(index_ls_mgr_map_.create(common::hash::cal_next_prime(DEFAULT_LS_HASH_SIZE),
                                              "VecIdxLSMgr",
                                              "VecIdxLSMgr",
                                              tenant_id))) {
    LOG_WARN("create ls mgr ", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_adaptor_map_.create(common::hash::cal_next_prime(DEFAULT_TENANT_ADAPTER_HASH_SIZE),
                                                "VecIdxTntAdp",
                                                "VecIdxTntAdp",
                                                tenant_id))) {
    LOG_WARN("create tenant adaptor map failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(vec_idx_tmp_map_.create(common::hash::cal_next_prime(DEFAULT_LS_HASH_SIZE),
                                              "AsyncTaskMap",
                                              "AsyncTaskMap",
                                              tenant_id))) {
    LOG_WARN("create ls mgr ", KR(ret), K(tenant_id));
  } else if (OB_FAIL(index_mgr_allocator_.init(nullptr, OB_MALLOC_MIDDLE_BLOCK_SIZE,
      lib::ObMemAttr(tenant_id, "VecIdxLSMgr")))) {
    LOG_WARN("VecIdxLSMgr allocator init failed.", K(ret));
  } else if (OB_FAIL(tmp_info_allocator_.init(nullptr, OB_MALLOC_MIDDLE_BLOCK_SIZE,
      lib::ObMemAttr(tenant_id, "VecIdxTmpInfo")))) {
    LOG_WARN("VecIdxTmpInfo allocator init failed.", K(ret));
  } else if (OB_FAIL(adaptor_allocator_.init(nullptr, OB_MALLOC_MIDDLE_BLOCK_SIZE,
      lib::ObMemAttr(tenant_id, "VecIdxAdaptor")))) {
    LOG_WARN("VecIdxAdaptor allocator init failed.", K(ret));
  } else if (OB_FAIL(ivf_allocator_.init(nullptr, OB_MALLOC_MIDDLE_BLOCK_SIZE,
      lib::ObMemAttr(tenant_id, "VecIdxIVF")))) {
    LOG_WARN("VecIdxIVF allocator init failed.", K(ret));
  } else {
    ObSharedMemAllocMgr *shared_mem_mgr = MTL(ObSharedMemAllocMgr*);
    memory_context_ = shared_mem_mgr->vector_allocator().get_mem_context();
    all_vsag_use_mem_ = shared_mem_mgr->vector_allocator().get_used_mem_ptr();
    tenant_id_ = tenant_id;
    schema_service_ = schema_service;
    ls_service_ = ls_service;
    sql_proxy_ = GCTX.sql_proxy_;
    is_inited_ = true;
    LOG_INFO("plugin vector index service: init", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObPluginVectorIndexService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  int64_t start_time_us = ObTimeUtility::current_time();
  FLOG_INFO("ObPluginVectorIndexService: start to switch_to_leader", K(tenant_id_), K(start_time_us));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPluginVectorIndexService is not inited", K(ret), K(tenant_id_));
#ifndef OB_BUILD_SYS_VEC_IDX
  } else if (!is_user_tenant(tenant_id_)) { // skip not user tenant
#endif
  } else if (is_oracle_mode()) { // skip oracle mode
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("ObPluginVectorIndexService: finish switch_to_leader", KR(ret), K(tenant_id_), K(cost_us));
  return ret;
}

int ObPluginVectorIndexService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  inner_switch_to_follower();
  return ret;
}

void ObPluginVectorIndexService::switch_to_follower_forcedly()
{
  inner_switch_to_follower();
}

void ObPluginVectorIndexService::inner_switch_to_follower()
{
  FLOG_INFO("ObPluginVectorIndexService: switch_to_follower", K_(tenant_id));
}

int ObPluginVectorIndexService::mtl_init(ObPluginVectorIndexService *&service)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = &GSCHEMASERVICE;
  ObLSService *ls_service = MTL(ObLSService*);

  if (OB_FAIL(service->init(MTL_ID(), schema_service, ls_service))) {
    LOG_WARN("fail to init plugin vector index service service", KR(ret));
  }
  return ret;
}

int ObPluginVectorIndexService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPluginVectorIndexService is not inited", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(vec_index_priority_queue_manager_.init(tenant_id_))) {
    LOG_WARN("fail to init vec index priority queue manager", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(vec_idx_async_task_sched_.init(tenant_id_, this))) {
    LOG_WARN("fail to init vec idx async task scheduler", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(vec_idx_async_task_sched_.start())) {
    LOG_WARN("fail to start vec idx async task scheduler", KR(ret), K_(tenant_id));
  }
  return ret;
}

void ObPluginVectorIndexService::stop()
{
  if (IS_INIT) {
    LOG_INFO("stop vector index service", K_(tenant_id), K_(is_inited));
    vec_idx_async_task_sched_.stop();
    get_vec_async_task_handle().stop();
    kmeans_build_task_handler_.stop();
    embedding_task_handler_.stop();
  }
}

void ObPluginVectorIndexService::wait()
{
  if (IS_INIT) {
    LOG_INFO("wait vector index service", K_(tenant_id));
    get_vec_async_task_handle().wait();
    kmeans_build_task_handler_.wait();
    embedding_task_handler_.wait();
  }
}

// for debug
void ObPluginVectorIndexMgr::dump_all_inst()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 64> tablet_ids;
  // print_adapter_info outputs incr/vbitmap/snap/frozen sections, need larger buffer than OB_VECTOR_INDEX_SYNC_INFO_SIZE(1024)
  char adaptor_info_str_[ObVectorIndexInfo::OB_VECTOR_INDEX_STATISTICS_SIZE];
  if (OB_FAIL(collect_adaptor_tablet_ids(tablet_ids))) {
    LOG_WARN("[VEC_INDEX][DUMP] failed to collect tablet ids", KR(ret), K(ls_id_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
    const ObTabletID &tablet_id = tablet_ids.at(i);
    ObPluginVectorIndexAdapterGuard adapter_guard;
    if (OB_FAIL(get_adapter_inst_guard(tablet_id, adapter_guard))) {
      if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
        continue;
      }
      LOG_WARN("[VEC_INDEX][DUMP] failed to get adapter guard", KR(ret), K(ls_id_), K(tablet_id));
      continue;
    }
    ObPluginVectorIndexAdaptor *adapter = adapter_guard.get_adatper();
    ObVectorIndexParam *hnsw_param = (adapter == nullptr)? nullptr : (ObVectorIndexParam *)(adapter->get_algo_data());
    if (OB_NOT_NULL(adapter)) {
      // complete_map is keyed by inc_tablet_id only
      memset(adaptor_info_str_, 0, ObVectorIndexInfo::OB_VECTOR_INDEX_STATISTICS_SIZE);
      int64_t pos = 0;
      if (OB_TMP_FAIL(adapter->print_adapter_info(adaptor_info_str_,
                                                  ObVectorIndexInfo::OB_VECTOR_INDEX_STATISTICS_SIZE, pos))) {
        LOG_WARN("[VEC_INDEX][DUMP] failed to print adapter info", K(tmp_ret));
      }
      LOG_INFO("[VEC_INDEX][DUMP] dump complete index adapter inc_tablet_id", K(ls_id_), K(tablet_id), KPC(hnsw_param), KP(adapter), KPC(adapter), K(adaptor_info_str_));
      adapter->reset_dump_info();
    } else {
      LOG_WARN("[VEC_INDEX][DUMP] dump complete index adapter is null", K(ls_id_), K(tablet_id));
    }
  }
  UNUSED(ret);
}

int ObPluginVectorIndexMgr::get_cache_tablet_ids(ObLSID &ls_id, ObIArray<ObLSTabletPair> &cache_tablet_ids)
{
  int ret = OB_SUCCESS;
  ObLSTabletPair pair;
  FOREACH_X(iter, ivf_cache_mgr_map_, OB_SUCC(ret))
  {
    pair.ls_id_ = ls_id;
    pair.tablet_id_ = iter->first;
    if (OB_FAIL(cache_tablet_ids.push_back(pair))) {
      LOG_WARN("failed to push array", K(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_snapshot_tablet_ids(
    ObIArray<ObLSTabletPair> &complete_tablet_ids,
    ObIArray<ObLSTabletPair> &partial_tablet_ids)
{
  int ret = OB_SUCCESS;
  ObLSTabletPair pair;
  ObSEArray<ObTabletID, 64> tablet_ids;
  // partial_tablet_ids is no longer populated (partial map removed)
  UNUSED(partial_tablet_ids);
  if (OB_FAIL(collect_adaptor_tablet_ids(tablet_ids))) {
    LOG_WARN("failed to collect adaptor tablet ids", KR(ret), K(ls_id_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
    const ObTabletID &tablet_id = tablet_ids.at(i);
    pair.ls_id_ = ls_id_;
    pair.tablet_id_ = tablet_id;
    if (OB_FAIL(complete_tablet_ids.push_back(pair))) {
      LOG_WARN("failed to push array", K(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_migration_adaptor_list(
    common::ObIArray<storage::ObMigrationVectorIndexAdaptorMeta> &adaptor_metas)
{
  int ret = OB_SUCCESS;
  RLockGuard lock_guard(adapter_map_rwlock_);
  FOREACH_X(iter, vec_adaptor_map_, OB_SUCC(ret)) {
    ObPluginVectorIndexAdaptor *adaptor = iter->second;
    if (OB_ISNULL(adaptor)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null adaptor in vec_adaptor_map_", K(ret), K(iter->first));
    } else if (!adaptor->is_ready_complete() || !adaptor->get_snap_has_complete()) {
      LOG_INFO("adaptor is not ready complete or snap has not completed, skip", KPC(adaptor));
    } else {
      storage::ObMigrationVectorIndexAdaptorMeta meta;
      if (OB_FAIL(adaptor->get_adaptor_meta(meta))) {
        LOG_WARN("failed to get adaptor meta", K(ret), K(iter->first));
      } else if (OB_FAIL(adaptor_metas.push_back(meta))) {
        LOG_WARN("failed to push adaptor meta", K(ret), K(meta));
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::batch_create_adaptor_shells(
    common::ObIAllocator &allocator,
    const common::ObIArray<storage::ObMigrationVectorIndexAdaptorMeta> &adaptor_metas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < adaptor_metas.count(); ++i) {
    const storage::ObMigrationVectorIndexAdaptorMeta &meta = adaptor_metas.at(i);
    if (!meta.is_valid()) {
      LOG_WARN("adaptor meta is invalid, skip", K(meta));
      continue;
    }

    void *adpt_buff = nullptr;
    ObPluginVectorIndexAdaptor *adaptor = nullptr;

    if (OB_ISNULL(adpt_buff = allocator.alloc(sizeof(ObPluginVectorIndexAdaptor)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for adaptor", K(ret), K(meta));
    } else {
      adaptor = new(adpt_buff) ObPluginVectorIndexAdaptor(&allocator, memory_context_, tenant_id_);
      bool attached = false;
      if (OB_FAIL(adaptor->copy_meta_info(meta))) {
        LOG_WARN("failed to copy meta info from migration meta", K(ret), K(meta));
      } else if (OB_FAIL(adaptor->init(memory_context_, all_vsag_use_mem_))) {
        LOG_WARN("failed to init adaptor", K(ret), K(meta));
      } else if (OB_FAIL(attach_adapter(adaptor, attached))) {
        LOG_WARN("failed to insert vector index adaptor", K(ret), K(meta));
      } else if (!attached) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to attach adaptor", K(ret), K(meta));
      } else {
        adaptor->set_identity_ts(get_identity_ts());
        adaptor->set_mig_state(ObAdaptorMigState::MIG_INIT);
        LOG_INFO("adaptor shell created", KPC(adaptor), K(meta));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(adaptor)) {
        adaptor->~ObPluginVectorIndexAdaptor();
        allocator.free(adpt_buff);
        adaptor = nullptr;
        adpt_buff = nullptr;
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::enqueue_mem_sync_task(
    common::ObTabletID inc_tablet_id, const int64_t inc_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mem_sync_info_.add_task_to_waiting_map(inc_tablet_id, inc_table_id))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("add_task_to_waiting_map failed", K(ret), K(inc_tablet_id), K(inc_table_id));
    }
  } else {
    LOG_INFO("enqueued memdata sync task", K(inc_tablet_id), K(inc_table_id));
  }
  return ret;
}

void ObPluginVectorIndexMgr::on_adapter_complete(
    ObPluginVectorIndexAdaptor *adapter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(adapter) || !adapter->is_ready_complete()) {
    LOG_WARN("invalid completed adapter", KP(adapter));
  } else if (get_ls_leader()) {
    // leader re-evaluates need_sync every scheduler tick, no re-arm needed
  } else if (!adapter->get_inc_tablet_id().is_valid()
             || OB_INVALID_ID == adapter->get_inc_table_id()) {
    // should not happen for a complete adapter, be defensive
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    if (!tenant_config.is_valid() || !tenant_config->load_vector_index_on_follower) {
      // follower load disabled, skip
    } else if (OB_FAIL(enqueue_mem_sync_task(adapter->get_inc_tablet_id(),
                                             adapter->get_inc_table_id()))) {
      // best-effort: the follower falls back to the next leader sync log on failure
      LOG_WARN("fail to enqueue follower mem sync after adapter complete",
          K(ret), K_(ls_id), "inc_tablet_id", adapter->get_inc_tablet_id());
    } else {
      LOG_INFO("[VECTOR INDEX ADAPTOR] follower adapter became complete, schedule mem load",
          K_(ls_id), "inc_tablet_id", adapter->get_inc_tablet_id(),
          "inc_table_id", adapter->get_inc_table_id());
    }
  }
}

int ObPluginVectorIndexService::get_snapshot_ids(
    ObIArray<ObLSTabletPair> &complete_tablet_ids,
    ObIArray<ObLSTabletPair> &partial_tablet_ids)
{
  int ret = OB_SUCCESS;
  TCRWLock::RLockGuard lock_guard(ls_mgr_map_rwlock_);
  FOREACH_X(iter, index_ls_mgr_map_, OB_SUCC(ret)) {
    const ObLSID &ls_id = iter->first;
    ObPluginVectorIndexMgr *index_ls_mgr = iter->second;
    if (OB_FAIL(index_ls_mgr->get_snapshot_tablet_ids(complete_tablet_ids, partial_tablet_ids))) {
      LOG_WARN("failed to get snapshot tablet ids", K(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexService::get_cache_ids(ObIArray<ObLSTabletPair> &cache_tablet_ids)
{
  int ret = OB_SUCCESS;
  TCRWLock::RLockGuard lock_guard(ls_mgr_map_rwlock_);
  FOREACH_X(iter, index_ls_mgr_map_, OB_SUCC(ret))
  {
    ObLSID &ls_id = iter->first;
    ObPluginVectorIndexMgr *index_ls_mgr = iter->second;
    if (OB_FAIL(index_ls_mgr->get_cache_tablet_ids(ls_id, cache_tablet_ids))) {
      LOG_WARN("failed to get cache tablet ids", K(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::replace_old_adapter(ObPluginVectorIndexAdaptor *new_adapter)
{
  int ret = 0;
  if (OB_ISNULL(new_adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null adapter", KR(ret));
  } else if (OB_ISNULL(vector_index_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index service is null", KR(ret), K(ls_id_));
  } else {
    ObPluginVectorIndexAdaptor *old_adapter = nullptr;
    if (OB_SUCCESS == get_adapter_inst_(new_adapter->get_inc_tablet_id(), old_adapter)
        && OB_NOT_NULL(old_adapter)
        && old_adapter->get_last_empty_scan_scn().is_valid()) {
      new_adapter->set_last_empty_scan_scn(old_adapter->get_last_empty_scan_scn());
      LOG_INFO("empty scan scn guard watermark carried over on adapter replace",
               K(new_adapter->get_inc_tablet_id()), "last_empty_scan_scn", old_adapter->get_last_empty_scan_scn());
    }

    bool detached = false;
    if (OB_FAIL(vector_index_service_->erase_tenant_vec_adaptor(new_adapter->get_inc_tablet_id()))) {
      if (ret != OB_HASH_NOT_EXIST) {
        LOG_WARN("failed to erase old tenant adapter", K(new_adapter->get_inc_tablet_id()), KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(vector_index_service_->set_tenant_vec_adaptor(new_adapter->get_inc_tablet_id(), new_adapter))) {
      LOG_WARN("failed to set new tenant adapter", K(new_adapter->get_inc_tablet_id()), KR(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(detach_adapter_in_lock_(new_adapter->get_inc_tablet_id(), detached))) {
      LOG_WARN("failed to detach old ls adapter", K(new_adapter->get_inc_tablet_id()), KR(ret));
    }
    if (OB_SUCC(ret)) {
      bool attached = false;
      if (OB_FAIL(attach_adapter_in_lock_(new_adapter, attached))) {
        set_last_transfer_scn(share::SCN::invalid_scn());
        LOG_WARN("failed to attach new adapter into ls map", K(new_adapter->get_inc_tablet_id()), KR(ret));
      }
    }
  }
  LOG_INFO("replace old adapter", K(ret), KP(new_adapter), K(*new_adapter));
  return ret;
}

int ObPluginVectorIndexMgr::replace_old_adapter_with_scn_check(ObPluginVectorIndexAdaptor *new_adapter, bool &has_replace)
{
  int ret = OB_SUCCESS;
  has_replace = false;
  bool need_replace = true;
  if (OB_ISNULL(new_adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null adapter", KR(ret));
  } else {
    // Optimistic lock: compare and swap
    ObPluginVectorIndexAdaptor *cur_adapter = nullptr;
    if (OB_FAIL(vec_adaptor_map_.get_refactored(new_adapter->get_inc_tablet_id(), cur_adapter))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // No old adapter exists, can replace, may not go there
        ret = OB_SUCCESS;
        LOG_INFO("No old adapter exists", KPC(new_adapter));
      } else {
        LOG_WARN("failed to get old adapter for replace scn comparison", K(*new_adapter), KR(ret));
      }
    } else if (OB_NOT_NULL(cur_adapter)) {
      share::SCN cur_adapter_replace_scn = cur_adapter->get_replace_scn();
      share::SCN new_adapter_replace_scn = new_adapter->get_replace_scn();
      if (!cur_adapter_replace_scn.is_valid()) {
        new_adapter->update_can_skip(cur_adapter->get_can_skip());
        LOG_INFO("cur may create by ddl, can replace",
                  K(new_adapter_replace_scn), K(cur_adapter_replace_scn), K(*new_adapter));
      } else if (cur_adapter_replace_scn >= new_adapter_replace_scn) {
        need_replace = false;
        LOG_INFO("scn check failed, skip replace",
                  K(new_adapter_replace_scn), K(cur_adapter_replace_scn), K(*new_adapter));
      } else {
        new_adapter->update_can_skip(cur_adapter->get_can_skip());
        LOG_INFO("scn check pass, will replace cur adapter",
                  K(new_adapter_replace_scn), K(cur_adapter_replace_scn), KP(new_adapter), K(*new_adapter));
      }
    }

    if (OB_SUCC(ret)) {
      if (need_replace && OB_FAIL(replace_old_adapter(new_adapter))) {
        LOG_WARN("failed to replace old adapter", KR(ret));
      } else {
        has_replace = need_replace;
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::set_ivf_cache_mgr(const ObIvfCacheMgrKey &tablet_id,
                                              ObIvfCacheMgr *cache_mgr,
                                              int overwrite)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ivf_cache_mgr_map_.set_refactored(tablet_id, cache_mgr, overwrite))) {
    LOG_WARN("failed to set vector index ivf cache mgr", K(tablet_id), KR(ret));
  } else {
    cache_mgr->inc_ref();
  }
  return ret;
}


int ObPluginVectorIndexMgr::erase_ivf_cache_mgr(const ObIvfCacheMgrKey &cachr_mgr_key)
{
  int ret = OB_SUCCESS;
  ObIvfCacheMgr *cache_mgr = nullptr;
  if (OB_FAIL(ivf_cache_mgr_map_.erase_refactored(cachr_mgr_key, &cache_mgr))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to erase partial vector index ivf cache mgr", K(cachr_mgr_key), KR(ret));
    }
  } else if (OB_ISNULL(cache_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ivf cache mgr inst is null", K(cachr_mgr_key), KR(ret));
  } else {
    if (OB_FAIL(ObPluginVectorIndexUtils::release_ivf_cache_mgr(cache_mgr))) {
      LOG_WARN("fail to release vector index ivf cache mgr", K(cachr_mgr_key), KR(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_ivf_cache_mgr(const ObIvfCacheMgrKey& cachr_mgr_key, ObIvfCacheMgr *&cache_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ivf_cache_mgr_map_.get_refactored(cachr_mgr_key, cache_mgr))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("fail to get cache mgr", K(ret), K(cachr_mgr_key));
    } else {
      LOG_INFO("cache mgr not exist", K(ret), K(cachr_mgr_key));
    }
  } else if (OB_ISNULL(cache_mgr)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("ivf cache mgr inst is null", KR(ret), K(cachr_mgr_key));
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_ivf_cache_mgr_guard(const ObIvfCacheMgrKey& cachr_mgr_key, ObIvfCacheMgrGuard &cache_mgr_guard)
{
  int ret = OB_SUCCESS;

  ObIvfCacheMgr *cache_mgr = nullptr;
  if (OB_FAIL(get_ivf_cache_mgr(cachr_mgr_key, cache_mgr))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to get ivf cache mgr inst", KR(ret), K(cachr_mgr_key));
    }
  } else if (OB_FAIL(cache_mgr_guard.set_cache_mgr(cache_mgr))) {
    LOG_WARN("failed to set ivf cache mgr", KR(ret), K(cachr_mgr_key));
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_or_create_ivf_cache_mgr_guard(ObIAllocator &allocator,
                                                              const ObIvfCacheMgrKey &key,
                                                              const ObVectorIndexParam &vec_index_param,
                                                              int64_t dim,
                                                              int64_t table_id,
                                                              ObIvfCacheMgrGuard &cache_mgr_guard)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ivf_cache_mgr_guard(key, cache_mgr_guard))) {
    if (ret == OB_HASH_NOT_EXIST) {
      if (OB_FAIL(create_ivf_cache_mgr(allocator, key, vec_index_param, dim, table_id))) {
        LOG_WARN("failed to create ivf cache mgr", K(ret), K(key), K(vec_index_param));
      } else if (OB_FAIL(get_ivf_cache_mgr_guard(key, cache_mgr_guard))) {
        LOG_WARN("failed to get ivf cache mgr guard", K(ret), K(key));
      }
    } else {
      LOG_WARN("fail to get ivf cache mgr guard", K(ret), K(key));
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::create_ivf_cache_mgr(ObIAllocator &allocator,
                                                 const ObIvfCacheMgrKey &key,
                                                 const ObVectorIndexParam &vec_index_param,
                                                 int64_t dim,
                                                 int64_t table_id)
{
  int ret = OB_SUCCESS;
  ObIvfCacheMgr *tmp_ivf_cache_mgr = nullptr;

  void *mgr_buff = nullptr;
  if (OB_ISNULL(mgr_buff = allocator.alloc(sizeof(ObIvfCacheMgr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for vector index ivf cache mgr", KR(ret));
  } else {
    tmp_ivf_cache_mgr = new(mgr_buff)ObIvfCacheMgr(allocator, tenant_id_);
    if (OB_FAIL(tmp_ivf_cache_mgr->init(memory_context_, vec_index_param, key, dim, table_id, all_vsag_use_mem_))) {
      LOG_WARN("failed to init cache mgr.", K(ret));
    } else {
      if (OB_FAIL(set_ivf_cache_mgr(key, tmp_ivf_cache_mgr))) {
        if (ret == OB_HASH_EXIST) {
          LOG_INFO("vector index ivf cache mgr may created by other threads");
          // Release the created but unused cache_mgr, as there is already one in the map.
          if (OB_NOT_NULL(tmp_ivf_cache_mgr)) {
            tmp_ivf_cache_mgr->~ObIvfCacheMgr();
            allocator.free(mgr_buff);
            tmp_ivf_cache_mgr = nullptr;
            mgr_buff = nullptr;
          }
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("set vector index ivf cache mgr faild", KR(ret), K(key));
        }
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(tmp_ivf_cache_mgr)) {
      tmp_ivf_cache_mgr->~ObIvfCacheMgr();
      allocator.free(mgr_buff);
      tmp_ivf_cache_mgr = nullptr;
      mgr_buff = nullptr;
    }
  }

  return ret;
}

int ObPluginVectorIndexService::process_pq_centroid_cache(ObIvfCentCache *cent_cache,
                                                        ObIArray<float*> &aux_info,
                                                        ObExprVecIvfCenterIdCache *expr_cache,
                                                        int64_t m)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(cent_cache)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("cent_cache is null", K(ret));
  } else {
    int64_t pq_m = m;

    if (pq_m <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid m value for PQ cache", K(ret), K(pq_m));
    } else {
      uint64_t capacity = cent_cache->get_count();
      LOG_DEBUG("Using m value for PQ centroid cache", K(pq_m), K(capacity), K(cent_cache->get_cent_vec_dim()));

      uint64_t count_per_m = capacity / pq_m;

      for (int64_t m_idx = 1; m_idx <= pq_m && OB_SUCC(ret); ++m_idx) {
        for (uint64_t i = 1; i <= count_per_m && OB_SUCC(ret); ++i) {
          float *centroid_vec = nullptr;
          if (OB_FAIL(cent_cache->read_pq_centroid(m_idx, i, centroid_vec))) {
            LOG_WARN("fail to read pq centroid", K(ret), K(m_idx), K(i));
          } else if (OB_ISNULL(centroid_vec)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("read pq centroid returned null pointer", K(m_idx), K(i));
          } else {
            if (OB_FAIL(aux_info.push_back(centroid_vec))) {
              LOG_WARN("failed to add pq centroid to aux_info array", K(ret), K(m_idx), K(i));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexService::process_centroid_cache(ObIvfCentCache *cent_cache,
                                                     ObIArray<float*> &aux_info,
                                                     ObExprVecIvfCenterIdCache *expr_cache,
                                                     ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(cent_cache)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("cent_cache is null", K(ret));
  } else {
    uint64_t capacity = cent_cache->get_count();
    // If expr_cache is provided, use its allocator (same as SQL query table logic)
    ObIAllocator *used_allocator = OB_NOT_NULL(expr_cache) ? &expr_cache->get_allocator() : &allocator;

    for (uint64_t i = 1; i <= capacity && OB_SUCC(ret); ++i) {
      float *centroid_vec = nullptr;
      if (cent_cache->has_hgraph_index()) {
        if (OB_FAIL(cent_cache->read_centroid(i, centroid_vec, true /*deep_copy*/, used_allocator))) {
          LOG_WARN("fail to get centroids from hgraph index", K(ret));
        }
      } else {
        if (OB_FAIL(cent_cache->read_centroid(i, centroid_vec))) {
          LOG_WARN("fail to read centroid", K(ret), K(i));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(centroid_vec)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read centroid returned null pointer", K(i));
      } else {
        if (OB_FAIL(aux_info.push_back(centroid_vec))) {
          LOG_WARN("failed to add centroid to aux_info array", K(ret), K(i));
        }
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexService::get_ivf_aux_info_from_cache(
  const uint64_t table_id,
  const ObTabletID tablet_id,
  const IvfCacheType cache_type,
  ObIAllocator &allocator,
  ObIArray<float*> &aux_info,
  ObExprVecIvfCenterIdCache *expr_cache,
  const ObTabletID cache_tablet_id,
  int64_t m)
{

  int ret = OB_SUCCESS;
  bool cache_hit = false;
  ObIvfCacheMgrGuard cache_guard;
  ObIvfCacheMgr *cache_mgr = nullptr;
  // Get ls_id through LocationService
  ObLSID ls_id;
  bool location_cache_hit = false;
  if (OB_FAIL(GCTX.location_service_->get(tenant_id_, cache_tablet_id, INT64_MAX, location_cache_hit, ls_id))) {
    LOG_WARN("Failed to get ls_id by cache_tablet_id, will try original logic", K(ret), K(cache_tablet_id));
  } else {

    ObIvfCacheMgrKey cache_key(cache_tablet_id);

    if (OB_FAIL(acquire_ivf_cache_mgr_guard(ls_id, cache_key, cache_guard))) {
      ret = OB_CACHE_NOT_HIT;
      LOG_WARN("Failed to acquire cache mgr guard, will try original logic", K(ret), K(ls_id), K(cache_key));
    } else if (OB_ISNULL(cache_mgr = cache_guard.get_ivf_cache_mgr())) {
      ret = OB_CACHE_NOT_HIT;
      LOG_WARN("Cache mgr is null, will try original logic", K(ret));
    } else {
      LOG_DEBUG("Successfully acquired cache mgr, checking cache status", K(cache_tablet_id), K(ls_id),
               "cache_mgr_key", cache_mgr->get_cache_mgr_key(), "cache_type", cache_type);

      ObIvfCentCache *cent_cache = nullptr;
      IvfCacheKey ivf_cache_key(cache_type);

      if (OB_FAIL(cache_mgr->get_cache_node(ivf_cache_key, cent_cache))) {
        // Failed to get cache node (including non-existence), fallback to original logic
        if (ret == OB_HASH_NOT_EXIST) {
          LOG_DEBUG("Cache node does not exist, will try original logic", K(table_id), K(cache_tablet_id), K(cache_type));
        } else {
          LOG_WARN("Failed to get cache node, will fallback to original logic", K(ret), K(table_id), K(cache_tablet_id), K(cache_type));
        }
        ret = OB_CACHE_NOT_HIT;
      } else if (OB_ISNULL(cent_cache)) {
        // Cache node is null, this is an abnormal situation
        ret = OB_CACHE_NOT_HIT;
        LOG_WARN("Cache node is null, will fallback to original logic", K(table_id), K(cache_tablet_id), K(cache_type));
      } else {
        if (cent_cache->is_completed()) {

          RWLock::RLockGuard guard(cent_cache->get_lock());
          uint64_t capacity = cent_cache->get_count();

          switch (cache_type) {
            case IvfCacheType::IVF_PQ_CENTROID_CACHE: {
              if (OB_FAIL(process_pq_centroid_cache(cent_cache, aux_info, expr_cache, m))) {
                LOG_WARN("failed to process PQ centroid cache", K(ret));
              }
              break;
            }
            case IvfCacheType::IVF_CENTROID_CACHE: {
              if (OB_FAIL(process_centroid_cache(cent_cache, aux_info, expr_cache, allocator))) {
                LOG_WARN("failed to process centroid cache", K(ret));
              }
              break;
            }
            default: {
              ret = OB_CACHE_NOT_HIT;
              LOG_WARN("unsupported cache type", K(ret), K(cache_type));
              break;
            }
          }

          if (OB_SUCC(ret)) {
            if (aux_info.count() > 0) {
              cache_hit = true;
              LOG_DEBUG("[IVF_CACHE_HIT] Successfully read centers from system cache", K(table_id), K(cache_tablet_id),
                       K(aux_info.count()), "cache_type", cache_type,
                       "cache_type_name", cache_type == IvfCacheType::IVF_CENTROID_CACHE ? "IVF_CENTROID" : "PQ_CENTROID");
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("[IVF_CACHE_ERROR] Cache is completed but contains no data, possible corruption",
                       K(ret), K(table_id), K(cache_tablet_id), K(cache_type), K(capacity),
                       "cache_type_name", cache_type == IvfCacheType::IVF_CENTROID_CACHE ? "IVF_CENTROID" : "PQ_CENTROID");
            }
          }
        } else {
          ret = OB_CACHE_NOT_HIT;
          LOG_WARN("Cache not completed, will try original logic", K(ret), K(cache_type));
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexService::get_ivf_aux_info(
  const uint64_t table_id,
  const ObTabletID tablet_id,
  ObIAllocator &allocator,
  ObIArray<float*> &aux_info)
{
  int ret = OB_SUCCESS;
  bool is_hidden_table = false;
  ObSqlString sql_string;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPluginVectorIndexService is not inited", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(generate_get_aux_info_sql(table_id, tablet_id, is_hidden_table, sql_string))) {
    LOG_WARN("failed to generate sql", K(ret), K(table_id));
  } else {
    ObSessionParam session_param;
    session_param.sql_mode_ = nullptr;
    session_param.tz_info_wrap_ = nullptr;
    session_param.consumer_group_id_ = GET_GROUP_ID();
    InnerDDLInfo ddl_info;
    ddl_info.set_is_dummy_ddl_for_inner_visibility(true);
    ddl_info.set_source_table_hidden(is_hidden_table);
    ddl_info.set_dest_table_hidden(false);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(session_param.ddl_info_.init(ddl_info, 0 /*session id*/))) {
        LOG_WARN("fail to init ddl info", KR(ret), K(ddl_info));
      } else if (OB_FAIL(sql_proxy_->read(res, tenant_id_, sql_string.ptr(), &session_param))) {
        LOG_WARN("failed to execute sql", K(ret), K(sql_string));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret), K(sql_string));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          const int64_t col_idx = 0;
          ObObj vec_obj;
          ObString blob_data;
          if (OB_FAIL(result->get_obj(col_idx, vec_obj))) {
            LOG_WARN("failed to get vid", K(ret));
          } else if (FALSE_IT(blob_data = vec_obj.get_string())) {
          } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator,
                                                                        ObLongTextType,
                                                                        CS_TYPE_BINARY,
                                                                        true,
                                                                        blob_data))) {
            LOG_WARN("fail to get real data.", K(ret), K(blob_data));
          } else {
            int64_t dim = blob_data.length() / sizeof(float);
            float *data = nullptr;
            if (OB_ISNULL(data = static_cast<float*>(allocator.alloc(sizeof(float) * dim)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to alloc memory", K(ret));
            } else if (FALSE_IT(MEMCPY(data, reinterpret_cast<float*>(blob_data.ptr()), sizeof(float) * dim))) {
            } else if (OB_FAIL(aux_info.push_back(data))) {
              LOG_WARN("failed to push back array", K(ret));
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexService::get_ivf_aux_info(
  const uint64_t table_id,
  const ObTabletID tablet_id,
  const int64_t dim,
  ObIAllocator &allocator,
  float* &aux_info,
  int64_t &count)
{
  int ret = OB_SUCCESS;
  ObSEArray<float*, 64> temp_aux_info;
  aux_info = nullptr;
  count = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPluginVectorIndexService is not inited", KR(ret), K_(tenant_id));
  } else if (dim <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dimension", K(ret), K(dim));
  } else {
    ObArenaAllocator tmp_allocator("GetIVFAuxInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);

    // First, get data from table using the old interface with temporary allocator
    if (OB_FAIL(get_ivf_aux_info(table_id, tablet_id, tmp_allocator, temp_aux_info))) {
      LOG_WARN("failed to get aux info from table", K(ret), K(table_id), K(tablet_id));
    } else if (temp_aux_info.empty()) {
      // No data found, return empty result
      aux_info = nullptr;
      count = 0;
    } else {
      // Copy data from pointer array to continuous memory
      count = temp_aux_info.count();
      int64_t alloc_size = sizeof(float) * dim * count;
      if (OB_ISNULL(aux_info = static_cast<float*>(allocator.alloc(alloc_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc continuous memory", K(ret), K(alloc_size));
      } else {
        float *current_pos = aux_info;
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          float *vec_data = temp_aux_info.at(i);
          if (OB_ISNULL(vec_data)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null vector data", K(ret), K(i));
          } else {
            MEMCPY(current_pos, vec_data, sizeof(float) * dim);
            current_pos += dim;
          }
        }
      }
    }
  }
  return ret;
}

// need partition key
int ObPluginVectorIndexService::generate_get_aux_info_sql(
    const uint64_t table_id,
    const ObTabletID tablet_id,
    bool &is_hidden_table,
    ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_ || OB_INVALID_ID == table_id || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(tenant_id), K(table_id));
  } else {
    const ObTableSchema *table_schema = nullptr;
    const ObTableSchema *data_table_schema = nullptr;
    ObString database_name;
    schema::ObSchemaGetterGuard schema_guard;
    if (OB_ISNULL(schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is nullptr", K(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("failed to get tenant schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("failed to get table schema", K(ret), K(table_id));
    } else if (!table_schema->is_vec_ivf_centroid_index() &&
               !table_schema->is_vec_ivfsq8_meta_index() &&
               !table_schema->is_vec_ivfpq_pq_centroid_index()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid table type", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_schema->get_data_table_id(), data_table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(table_schema->get_data_table_id()));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("failed to get table schema", K(ret), K(table_schema->get_data_table_id()));
    } else {
      const uint64_t database_id = table_schema->get_database_id();
      const ObDatabaseSchema *db_schema = nullptr;
      is_hidden_table = table_schema->is_user_hidden_table();
      if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, database_id, db_schema))) {
        LOG_WARN("fail to get database schema", K(ret), K_(tenant_id), K(database_id));
      } else if (OB_ISNULL(db_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, database schema must not be nullptr", K(ret));
      } else {
        database_name = db_schema->get_database_name_str();
      }
    }
    const char* query_col = "";
    const char* filter_col = "";
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count(); ++i) {
      const ObColumnSchemaV2 *data_col_schema = nullptr;
      const ObColumnSchemaV2 *col_schema = nullptr;
      if (OB_ISNULL(col_schema = table_schema->get_column_schema_by_idx(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(i), K(table_schema));
      } else if (OB_ISNULL(data_col_schema = data_table_schema->get_column_schema(col_schema->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (data_col_schema->is_vec_ivf_center_id_column()
                  || data_col_schema->is_vec_ivf_meta_id_column()
                  || data_col_schema->is_vec_ivf_pq_center_id_column()) {
        filter_col = col_schema->get_column_name();
      } else if (data_col_schema->is_vec_ivf_center_vector_column()
                || data_col_schema->is_vec_ivf_meta_vector_column()) {
        query_col = col_schema->get_column_name();
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == strlen(query_col) || 0 == strlen(filter_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null col name", K(ret), K(query_col), K(filter_col));
      } else {
        uint64_t min_center_id = 0;
        uint64_t max_center_id = UINT64_MAX;
        const ObString &table_name = table_schema->get_table_name_str();
        if (OB_FAIL(sql_string.assign_fmt("SELECT %.*s FROM `%.*s`.`%.*s` WHERE %.*s >= X'%016lx%016lx' and %.*s <= X'%016lx%016lx'",
            static_cast<int>(strlen(query_col)), query_col,
            static_cast<int>(database_name.length()), database_name.ptr(),
            static_cast<int>(table_name.length()), table_name.ptr(),
            static_cast<int>(strlen(filter_col)), filter_col,
            tablet_id.id(), min_center_id,
            static_cast<int>(strlen(filter_col)), filter_col,
            tablet_id.id(), max_center_id))) {
          LOG_WARN("failed to assign sql string", K(ret));
        } else {
          LOG_DEBUG("success to generate sql string", K(ret), K(sql_string), K(table_id), K(tablet_id));
        }
      }
    }

  }
  return ret;
}
int ObPluginVectorIndexService::acquire_ivf_cache_mgr_guard(
    ObLSID ls_id, const ObIvfCacheMgrKey &key, ObIvfCacheMgrGuard &cache_mgr_guard)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;

  if (OB_FAIL(acquire_vector_index_mgr(ls_id, ls_index_mgr))) {
    LOG_WARN("failed to acquire vector index mgr", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_index_mgr->get_ivf_cache_mgr_guard(key, cache_mgr_guard))) {
    LOG_WARN("fail to get ivf cache mgr guard", K(ret), K(key));
  }
  return ret;
}

int ObPluginVectorIndexService::acquire_ivf_cache_mgr_guard(ObLSID ls_id,
                                                            const ObIvfCacheMgrKey &key,
                                                            const ObVectorIndexParam &vec_index_param,
                                                            int64_t dim,
                                                            int64_t table_id,
                                                            ObIvfCacheMgrGuard &cache_mgr_guard)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPluginVectorIndexService is not inited", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(acquire_vector_index_mgr(ls_id, ls_index_mgr))) {
    LOG_WARN("failed to acquire vector index mgr", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_index_mgr->get_or_create_ivf_cache_mgr_guard(ivf_allocator_, key, vec_index_param, dim, table_id, cache_mgr_guard))) {
    LOG_WARN("failed to get or create ivf cache mgr guard", K(ls_id), KR(ret));
  } else {
    ObIvfCacheMgr* cache_mgr = cache_mgr_guard.get_ivf_cache_mgr();
    if (OB_ISNULL(cache_mgr)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null cache mgr", K(ret));
    }
  }

  return ret;
}

int ObPluginVectorIndexService::start_kmeans_tg()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::VectorTaskPool, kmeans_tg_id_))) {
    LOG_WARN("TG_CREATE_TENANT failed for kmeans thread pool", KR(ret));
  } else if (OB_FAIL(TG_START(kmeans_tg_id_))) {
    LOG_WARN("TG_START failed for kmeans thread pool", KR(ret));
  }
  return ret;
}

int ObPluginVectorIndexService::get_embedding_task_handler(ObEmbeddingTaskHandler *&handler)
{
  int ret = OB_SUCCESS;
  if (!embedding_task_handler_.is_inited() && OB_FAIL(embedding_task_handler_.init())) {
    LOG_WARN("failed to init embedding task handler", KR(ret));
  } else {
    handler = &embedding_task_handler_;
  }
  return ret;
}

int ObPluginVectorIndexService::get_ai_execution_service(oceanbase::vector_index::ObAiAccessService *&service)
{
  int ret = OB_SUCCESS;
  if (!ai_execution_service_.is_inited()
      || !ai_execution_service_.is_running()
      || !ai_execution_service_.is_table_poller_running()) {
    common::ObSpinLockGuard lock_guard(ai_service_init_lock_);
    if (!ai_execution_service_.is_inited() && OB_FAIL(ai_execution_service_.init(adaptor_allocator_, tenant_id_))) {
      LOG_WARN("failed to init ai execution service", KR(ret), K(tenant_id_));
    } else if (!ai_execution_service_.is_running() && OB_FAIL(ai_execution_service_.start())) {
      LOG_WARN("failed to start ai execution service", KR(ret), K(tenant_id_));
    } else if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy_ is null", K(ret));
    } else {
      ai_execution_service_.set_sql_client(sql_proxy_);
      if (!ai_execution_service_.is_table_poller_running()) {
        if (OB_FAIL(ai_execution_service_.register_table_poller_task(*sql_proxy_))) {
          LOG_WARN("failed to register table poller", KR(ret), K(tenant_id_));
        } else {
          LOG_INFO("table poller started for ai execution service", K(tenant_id_));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    service = &ai_execution_service_;
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
