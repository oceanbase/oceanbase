/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SERVER
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/ob_vec_index_builder_util.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"

namespace oceanbase
{
namespace share
{

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
    partial_index_adpt_map_.destroy();
    complete_index_adpt_map_.destroy();
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
  FOREACH(iter, partial_index_adpt_map_) {
    const ObTabletID &tablet_id = iter->first;
    ObPluginVectorIndexAdaptor *adapter = iter->second;
    if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(adapter))) {
      LOG_ERROR("fail to release vector index adapter", K(tablet_id), KR(ret));
      ret = OB_SUCCESS; // continue release
    }
  }
  FOREACH(iter, complete_index_adpt_map_) {
    const ObTabletID &tablet_id = iter->first;
    ObPluginVectorIndexAdaptor *adapter = iter->second;
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
  if (OB_FAIL(complete_index_adpt_map_.create(hash_capacity, "VecIdxAdptMap", "VecIdxAdptMap", tenant_id))) {
    LOG_WARN("fail to create full index adapter map", KR(ret), K(ls_id));
  } else if (OB_FAIL(partial_index_adpt_map_.create(hash_capacity, "VecIdxAdptMap", "VecIdxAdptMap", tenant_id))) {
    LOG_WARN("fail to create partial index adapter map", KR(ret), K(ls_id));
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
    is_inited_ = true;
  }
  return ret;
}

int ObPluginVectorIndexMgr::set_complete_adapter_(ObTabletID tablet_id,
                                                  ObPluginVectorIndexAdaptor *adapter_inst,
                                                  int overwrite)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(complete_index_adpt_map_.set_refactored(tablet_id, adapter_inst, overwrite))) {
    LOG_WARN("failed to set complete vector index adapter", K(tablet_id), KR(ret));
  } else {
    adapter_inst->inc_ref();
  }
  return ret;
}

int ObPluginVectorIndexMgr::erase_complete_adapter(ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdaptor *adapter_inst = nullptr;
  if (OB_FAIL(complete_index_adpt_map_.erase_refactored(tablet_id, &adapter_inst))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to erase partial vector index adapter", K(tablet_id), KR(ret));
    }
  } else if (OB_ISNULL(adapter_inst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("adapter inst is null", K(tablet_id), KR(ret));
  } else {
    if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(adapter_inst))) {
      LOG_WARN("fail to release vector index adapter", K(tablet_id), KR(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::set_partial_adapter_(ObTabletID tablet_id,
                                                 ObPluginVectorIndexAdaptor *adapter_inst,
                                                 int overwrite)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partial_index_adpt_map_.set_refactored(tablet_id, adapter_inst, overwrite))) {
    LOG_WARN("failed to set partial vector index adapter", K(tablet_id), KR(ret));
  } else {
    adapter_inst->inc_ref();
  }
  return ret;
}

int ObPluginVectorIndexMgr::erase_partial_adapter_(ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdaptor *adapter_inst = nullptr;
  if (OB_FAIL(partial_index_adpt_map_.erase_refactored(tablet_id, &adapter_inst))) {
    LOG_WARN("failed to erase partial vector index adapter", K(tablet_id), KR(ret));
  } else if (OB_ISNULL(adapter_inst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("adapter inst is null", K(tablet_id), KR(ret));
  } else {
    if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(adapter_inst))) {
      LOG_WARN("fail to release vector index adapter", K(tablet_id), KR(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::erase_partial_adapter(ObTabletID tablet_id)
{
  return erase_partial_adapter_(tablet_id);
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
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_adapter_inst_(ObTabletID tablet_id, ObPluginVectorIndexAdaptor *&index_inst)
{
  int ret = OB_SUCCESS;
  index_inst = nullptr;

  if (OB_FAIL(partial_index_adpt_map_.get_refactored(tablet_id, index_inst))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get temp vector index inst", K(tablet_id), KR(ret));
    } else {
      ret = OB_SUCCESS; // not in partial adapter, try to get complete adapter
    }
  } else if (OB_ISNULL(index_inst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null temp vector index inst", K(tablet_id), KR(ret));
  }

  if (OB_FAIL(ret) || OB_NOT_NULL(index_inst)) {
  } else if (OB_FAIL(complete_index_adpt_map_.get_refactored(tablet_id, index_inst))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get full vector index inst", K(tablet_id), KR(ret));
    } else {
      // ret is OB_HASH_NOT_EXIST not found,
    }
  } else if (OB_ISNULL(index_inst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null full vector index inst", K(tablet_id), KR(ret));
  }

  return ret;
}

int ObPluginVectorIndexMgr::create_partial_adapter(ObTabletID idx_tablet_id,
                                                   ObTabletID data_tablet_id,
                                                   ObIndexType type,
                                                   ObIAllocator &allocator,
                                                   int64_t index_table_id,
                                                   ObString *vec_index_param,
                                                   int64_t dim)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdaptor *tmp_vec_idx_adpt = nullptr;

  void *adpt_buff = allocator.alloc(sizeof(ObPluginVectorIndexAdaptor));
  if (OB_ISNULL(adpt_buff)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for vector index adapter", KR(ret));
  } else {
    tmp_vec_idx_adpt = new(adpt_buff)ObPluginVectorIndexAdaptor(&allocator, memory_context_, tenant_id_);
    ObVectorIndexRecordType record_type = ObPluginVectorIndexUtils::index_type_to_record_type(type);
    if (record_type >= VIRT_MAX) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index type", K(type), KR(ret));
      // always init after construct
    } else if ((OB_ISNULL(vec_index_param) || vec_index_param->empty())
               && OB_FAIL(tmp_vec_idx_adpt->init(memory_context_, all_vsag_use_mem_))) {
      LOG_WARN("failed to init adpt.", K(ret));
      // need to handle dim and type
    } else if ((OB_NOT_NULL(vec_index_param) && !vec_index_param->empty())
               && OB_FAIL(tmp_vec_idx_adpt->init(*vec_index_param, dim, memory_context_, all_vsag_use_mem_))) {
      LOG_WARN("failed to init adpt.", K(ret), K(*vec_index_param), K(dim));
    } else if (OB_FAIL(tmp_vec_idx_adpt->set_tablet_id(record_type, idx_tablet_id))) {
      LOG_WARN("failed to set tablet id", K(idx_tablet_id), K(type), KR(ret));
    } else if (data_tablet_id.is_valid() // tmp adapter may not have data_tablet id
               && OB_FAIL(tmp_vec_idx_adpt->set_tablet_id(VIRT_DATA, data_tablet_id))) {
      LOG_WARN("failed to set data tablet id", K(idx_tablet_id), K(type), K(data_tablet_id), KR(ret));
    } else if (OB_FAIL(tmp_vec_idx_adpt->set_table_id(record_type, index_table_id))) {
      LOG_WARN("failed to set index table id", K(idx_tablet_id), K(type), K(index_table_id), KR(ret));
    } else {
      tmp_vec_idx_adpt->set_create_type(ObPluginVectorIndexUtils::index_type_to_create_type(type));
    }
    if (OB_SUCC(ret)) {
      WLockGuard lock_guard(adapter_map_rwlock_);
      if (OB_FAIL(set_partial_adapter_(idx_tablet_id, tmp_vec_idx_adpt))) {
        LOG_WARN("set vector index adapter faild", K(idx_tablet_id), KR(ret));
      } // other thread set already, need get again ?
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(tmp_vec_idx_adpt)) {
      tmp_vec_idx_adpt->~ObPluginVectorIndexAdaptor();
      allocator.free(adpt_buff);
      tmp_vec_idx_adpt = nullptr;
      adpt_buff = nullptr;
    }
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

int ObPluginVectorIndexMgr::get_or_create_partial_adapter_(ObTabletID tablet_id,
                                                           ObIndexType type,
                                                           ObPluginVectorIndexAdapterGuard &adapter_guard,
                                                           ObString *vec_index_param,
                                                           int64_t dim,
                                                           ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_adapter_inst_guard(tablet_id, adapter_guard))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index adapter", K(tablet_id), KR(ret));
    } else { // not exist create new
      if (OB_FAIL(create_partial_adapter(tablet_id, ObTabletID(), type, allocator, OB_INVALID_ID, vec_index_param, dim))) {
        LOG_WARN("failed to create tmp vector index instance with ls", K(tablet_id), K(type), KR(ret));
      }
      if (OB_FAIL(ret) && (OB_HASH_EXIST != ret)) {
      } else if (OB_FAIL(get_adapter_inst_guard(tablet_id, adapter_guard))) {
        LOG_WARN("failed to get tmp vector index instance with ls", K(tablet_id), K(type), KR(ret));
      } else {
        LOG_INFO("create partial index adapter success", K(ret), KPC(adapter_guard.get_adatper()));
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_adapter_inst_by_ctx(ObVectorIndexAcquireCtx &ctx,
                                                    bool &need_merge,
                                                    ObIAllocator &allocator,
                                                    ObPluginVectorIndexAdapterGuard &adapter_guard,
                                                    ObVectorIndexAdapterCandiate &candidate,
                                                    ObString *vec_index_param,
                                                    int64_t dim)
{
  int ret = OB_SUCCESS;
  need_merge = true;

  if (!ctx.inc_tablet_id_.is_valid()
      || !ctx.snapshot_tablet_id_.is_valid()
      || !ctx.vbitmap_tablet_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ctx), KR(ret));
  } else {
    ObPluginVectorIndexAdaptor *adapter = nullptr;
    bool is_hybrid = ctx.embedded_tablet_id_.is_valid();
    // fast return if get complete adapter
    if (OB_FAIL(get_or_create_partial_adapter_(ctx.inc_tablet_id_,
                                               is_hybrid ? INDEX_TYPE_HYBRID_INDEX_LOG_LOCAL : INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL,
                                               candidate.inc_adatper_guard_,
                                               vec_index_param,
                                               dim,
                                               allocator))) {
      LOG_WARN("failed to get vector index adapter", K(ctx.inc_tablet_id_), KR(ret));
    } else if (FALSE_IT(adapter = candidate.inc_adatper_guard_.get_adatper())) {
    } else if (adapter->get_create_type() == CreateTypeFullPartial
               || adapter->get_create_type() == CreateTypeComplete) {
      if (OB_FAIL(adapter_guard.set_adapter(adapter))) {
        LOG_WARN("failed to set adapter", K(adapter_guard), KR(ret));
      } else {
        need_merge = false;
      }
    }

    if (OB_FAIL(ret) || need_merge == false) {
      // do nothing
    } else if (OB_FAIL(get_or_create_partial_adapter_(ctx.vbitmap_tablet_id_,
                                                      INDEX_TYPE_VEC_INDEX_ID_LOCAL,
                                                      candidate.bitmp_adatper_guard_,
                                                      vec_index_param,
                                                      dim,
                                                      allocator))) {
      LOG_WARN("failed to get vector index adapter", K(ctx.vbitmap_tablet_id_), KR(ret));
    } else if (FALSE_IT(adapter = candidate.bitmp_adatper_guard_.get_adatper())) {
    } else if (adapter->get_create_type() == CreateTypeFullPartial
               || adapter->get_create_type() == CreateTypeComplete) {
      if (OB_FAIL(adapter_guard.set_adapter(adapter))) {
        LOG_WARN("failed to set adapter", K(adapter_guard), KR(ret));
      } else {
        need_merge = false;
      }
    }

    if (OB_FAIL(ret) || need_merge == false) {
      // do nothing
    } else if (OB_FAIL(get_or_create_partial_adapter_(ctx.snapshot_tablet_id_,
                                                      INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                                      candidate.sn_adatper_guard_,
                                                      vec_index_param,
                                                      dim,
                                                      allocator))) {
      LOG_WARN("failed to get vector index adapter", K(ctx.snapshot_tablet_id_), KR(ret));
    } else if (FALSE_IT(adapter = candidate.sn_adatper_guard_.get_adatper())) {
    } else if (adapter->get_create_type() == CreateTypeFullPartial
               || adapter->get_create_type() == CreateTypeComplete) {
      if (OB_FAIL(adapter_guard.set_adapter(adapter))) {
        LOG_WARN("failed to set adapter", K(adapter_guard), KR(ret));
      } else {
        need_merge = false;
      }
    }

    if ((OB_FAIL(ret) || need_merge == false)) {
      // do nothing
    } else if (!is_hybrid) {
      // do nothing
    } else if (OB_FAIL(get_or_create_partial_adapter_(ctx.embedded_tablet_id_,
                                                      INDEX_TYPE_HYBRID_INDEX_EMBEDDED_LOCAL,
                                                      candidate.embedded_adatper_guard_,
                                                      vec_index_param,
                                                      dim,
                                                      allocator))) {
      LOG_WARN("failed to get vector index adapter", K(ctx.embedded_tablet_id_), KR(ret));
    } else if (FALSE_IT(adapter = candidate.embedded_adatper_guard_.get_adatper())) {
    } else if (adapter->get_create_type() == CreateTypeFullPartial
               || adapter->get_create_type() == CreateTypeComplete) {
      if (OB_FAIL(adapter_guard.set_adapter(adapter))) {
        LOG_WARN("failed to set adapter", K(adapter_guard), KR(ret));
      } else {
        need_merge = false;
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_and_merge_adapter(ObVectorIndexAcquireCtx &ctx,
                                                  ObIAllocator &allocator,
                                                  ObPluginVectorIndexAdapterGuard &adapter_guard,
                                                  ObString *vec_index_param,
                                                  int64_t dim)
{
  int ret = OB_SUCCESS;
  bool need_merge = false;
  ObVectorIndexAdapterCandiate candidate;
  if (OB_FAIL(get_adapter_inst_by_ctx(ctx, need_merge, allocator, adapter_guard,
                                      candidate, vec_index_param, dim))) {
    LOG_WARN("failed to get and merge adapter", K(ls_id_), K(ctx), KR(ret));
  }
  if (OB_SUCC(ret)
      && need_merge
      && OB_FAIL(replace_with_full_partial_adapter(ctx, allocator, adapter_guard,
                                                   vec_index_param, dim, &candidate))) {
    LOG_WARN("failed to replace with full partial adapter", K(ctx), KR(ret));
  }

  return ret;
}

int ObPluginVectorIndexMgr::check_need_mem_data_sync_task(bool &need_sync)
{
  need_sync = false;
  mem_sync_info_.check_and_switch_if_needed(need_sync, ls_tablet_task_ctx_.all_finished_);
  LOG_INFO("memdata sync check", K(ls_id_), K(need_sync), K(ls_tablet_task_ctx_));
  // both map empty, do nothing
  return OB_SUCCESS;
}

int ObPluginVectorIndexService::acquire_adapter_guard(ObLSID ls_id,
                                                      ObVectorIndexAcquireCtx &ctx,
                                                      ObPluginVectorIndexAdapterGuard &adapter_guard,
                                                      ObString *vec_index_param,
                                                      int64_t dim)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;

  if (OB_FAIL(acquire_vector_index_mgr(ls_id, ls_index_mgr))) {
    LOG_WARN("failed to acquire vector index mgr", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_index_mgr->get_and_merge_adapter(ctx, allocator_, adapter_guard,
                                                         vec_index_param, dim))) {
    LOG_WARN("failed to get and merge adapter", K(ls_id), K(ctx), KR(ret));
  } else {
    share::ObPluginVectorIndexAdaptor* adaptor = adapter_guard.get_adatper();
    if (OB_NOT_NULL(adaptor) && !adaptor->validate_tablet_ids(ctx)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("validate tablet ids failed", K(ret), K(ctx), K(adaptor));
    }
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
      if (OB_FAIL(ls_index_mgr->create_ivf_build_helper(key, type, vec_index_param, allocator_))) {
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
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  if (OB_FAIL(get_ls_index_mgr_map().get_refactored(ls_id, ls_index_mgr))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index mgr for ls", KR(ret), K(ls_id));
    } else { // create new ls index mgr if not exist
      ret = OB_SUCCESS;
      if (OB_FAIL(create_partial_adapter(ls_id, tablet_id, ObTabletID(), type, OB_INVALID_ID, vec_index_param, dim))) {
        LOG_WARN("failed to create tmp vector index instance", K(ls_id), K(tablet_id), K(type), KR(ret));
      }
      if (OB_FAIL(ret) && (OB_HASH_EXIST != ret)) {
      } else if (OB_FAIL(get_adapter_inst_guard(ls_id, tablet_id, adapter_guard))) {
        LOG_WARN("failed to get tmp vector index instance", K(ls_id), K(tablet_id), K(type), KR(ret));
      } else {
        LOG_INFO("create partial index adapter success", K(ret), K(ls_id), KPC(adapter_guard.get_adatper()));
      }
    }
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_index_mgr->get_adapter_inst_guard(tablet_id, adapter_guard))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index adapter", K(ls_id), K(tablet_id), KR(ret));
    } else { // not exist create new
      if (OB_FAIL(ls_index_mgr->create_partial_adapter(tablet_id, ObTabletID(), type, allocator_, OB_INVALID_ID, vec_index_param, dim))) {
        LOG_WARN("failed to create tmp vector index instance with ls", K(ls_id), K(tablet_id), K(type), KR(ret));
      }
      if (OB_FAIL(ret) && (OB_HASH_EXIST != ret)) {
      } else if (OB_FAIL(ls_index_mgr->get_adapter_inst_guard(tablet_id, adapter_guard))) {
        LOG_WARN("failed to get tmp vector index instance with ls", K(ls_id), K(tablet_id), K(type), KR(ret));
      } else {
        LOG_INFO("create partial index adapter success", K(ret), K(ls_id), KP(adapter_guard.get_adatper()), KPC(adapter_guard.get_adatper()));
      }
    }
  } else {
    // get from existed ls index mgr
  }
  if (OB_SUCC(ret)
      && OB_NOT_NULL(adapter_guard.get_adatper())
      && adapter_guard.get_adatper()->get_index_type() >= ObVectorIndexAlgorithmType::VIAT_MAX) {
    // check index param, if it is emtpy, may get partial adapter during maintenance
    if (OB_NOT_NULL(vec_index_param)
        && !vec_index_param->empty()
        && OB_FAIL(adapter_guard.get_adatper()->set_param(*vec_index_param, dim))) {
      LOG_WARN("failed to set param", K(ret), K(ls_id), K(tablet_id), K(type), KPC(vec_index_param), K(dim));
    }
    LOG_INFO("may get get partial adapter during maintenance", KPC(adapter_guard.get_adatper()));
  }

  return ret;
}

int ObPluginVectorIndexMgr::check_and_merge_partial_inner(ObVecIdxSharedTableInfoMap &info_map,
                                                          ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  typedef common::hash::ObHashMap<ObPluginVectorIndexIdentity, ObVectorIndexAdapterCandiate*> VectorIndexIdentityMap;
  VectorIndexIdentityMap data_tablet_id_map;
  ObArenaAllocator tmp_allocator("VectorAdptCandi", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  if (OB_FAIL(data_tablet_id_map.create(DEFAULT_CANDIDATE_ADAPTER_HASH_SIZE, "VecIdxDataTID"))) {
    LOG_WARN("fail to create hash map for data tablet id to vec index adapter", KR(ret));
  } else {
    // build candidate and save to data_tablet_id_map
    // query process may merge adapters and delete partial adapters from hashmap,
    // use lock here to avoid merge race condition for simple
    RLockGuard lock_guard(adapter_map_rwlock_);

    FOREACH_X(adpt_lt, get_partial_adapter_map(), OB_SUCC(ret)) {
      ObTabletID index_tablet_id = adpt_lt->first;
      ObPluginVectorIndexAdaptor *partial_adpt = adpt_lt->second;
      ObTabletID data_tablet_id = partial_adpt->get_data_tablet_id();
      ObVectorIndexAdapterCandiate *candidate = nullptr;
      char *buff = nullptr;
      ObPluginVectorIndexIdentity index_identity(data_tablet_id, partial_adpt->get_index_identity());
      if (!index_identity.is_valid()) {
        // skip, wait for next round
      } else {
        if (OB_FAIL(data_tablet_id_map.get_refactored(index_identity, candidate))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("failed to get candidate index adapter", K(index_identity), KR(ret));
          } else {
            buff = static_cast<char *>(tmp_allocator.alloc(sizeof(ObVectorIndexAdapterCandiate)));
            if (OB_ISNULL(buff)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate memory for vector index adapter", KR(ret));
            } else {
              candidate = new(buff)ObVectorIndexAdapterCandiate();
              if (OB_FAIL(data_tablet_id_map.set_refactored(index_identity, candidate))) {
                LOG_WARN("failed to set candidate index adapter", K(index_identity), KR(ret));
              }
            }
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(candidate)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid candidate index adapter", KR(ret));
        } else {
          if (index_tablet_id == partial_adpt->get_inc_tablet_id()) {
            if (candidate->inc_adatper_guard_.is_valid()) { // conflict maybe during rebuild
              candidate->is_valid_ = false;
            } else {
              candidate->inc_adatper_guard_.set_adapter(partial_adpt);
            }
          } else if (index_tablet_id == partial_adpt->get_vbitmap_tablet_id()) {
            if (candidate->bitmp_adatper_guard_.is_valid()) { // conflict maybe during rebuild
              candidate->is_valid_ = false;
            } else {
              candidate->bitmp_adatper_guard_.set_adapter(partial_adpt);
            }
          } else if (index_tablet_id == partial_adpt->get_snap_tablet_id()) {
            if (candidate->sn_adatper_guard_.is_valid()) { // conflict maybe during rebuild
              candidate->is_valid_ = false;
            } else {
              candidate->sn_adatper_guard_.set_adapter(partial_adpt);
            }
          } else if (index_tablet_id == partial_adpt->get_embedded_tablet_id()) {
            candidate->is_hybrid_ = true;
            if (candidate->embedded_adatper_guard_.is_valid()) { // conflict maybe during rebuild
              candidate->is_valid_ = false;
            } else {
              candidate->embedded_adatper_guard_.set_adapter(partial_adpt);
            }
          }
        }
      }
    }
  }

  ret = OB_SUCCESS; // continue handle valid candidates
  FOREACH_X(candidate_adpt_lt, data_tablet_id_map, OB_SUCC(ret)) {
    ObVectorIndexAdapterCandiate *candidate = candidate_adpt_lt->second;
    if (OB_ISNULL(candidate)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid candidate index adapter", KR(ret), K(candidate_adpt_lt->first));
    } else if (candidate->is_valid_ == false || (!candidate->is_complete())) {
      // do nothing
    } else if (OB_FAIL(replace_with_complete_adapter(candidate, info_map, allocator))) {
      LOG_WARN("failed to replace adapter", KR(ret), K(candidate_adpt_lt->first));
    }
  }
  // do clean up
  FOREACH(candidate_adpt_lt, data_tablet_id_map) {
    ObVectorIndexAdapterCandiate *candidate = candidate_adpt_lt->second;
    if (OB_ISNULL(candidate)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid candidate index adapter", KR(ret));
    } else {
      candidate->~ObVectorIndexAdapterCandiate();
    }
  }

  data_tablet_id_map.reuse();
  tmp_allocator.reset();

  return ret;
}

int ObPluginVectorIndexService::check_and_merge_adapter(ObLSID ls_id, ObVecIdxSharedTableInfoMap &info_map)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  if (OB_FAIL(get_ls_index_mgr_map().get_refactored(ls_id, index_ls_mgr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get vector index ls mgr", KR(ret), K(tenant_id_), K(ls_id));
    }
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_id));
  } else if (!index_ls_mgr->get_partial_adapter_map().empty()) {
    if (OB_FAIL(index_ls_mgr->check_and_merge_partial_inner(info_map, allocator_))) {
      LOG_WARN("failed to check and merge partial adapter", KR(ret));
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
    LOG_WARN("failed to get vector index adapter", K(ls_id), K(tablet_id), KR(ret));
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
    allocator_.free(tmp_info);
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
        void *tmp_info_buff = allocator_.alloc(sizeof(ObVectorIndexTmpInfo));
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
            allocator_.free(tmp_info_buff);
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

int ObPluginVectorIndexService::acquire_vector_index_mgr(ObLSID ls_id, ObPluginVectorIndexMgr *&mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_ls_index_mgr_map().get_refactored(ls_id, mgr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      void *mgr_buff = allocator_.alloc(sizeof(ObPluginVectorIndexMgr));
      if (OB_ISNULL(mgr_buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memeory for new vector index mgr", KR(ret));
      } else {
        ObPluginVectorIndexMgr *new_ls_index_mgr = new(mgr_buff)ObPluginVectorIndexMgr(memory_context_, tenant_id_);
        if (OB_FAIL(new_ls_index_mgr->init(tenant_id_, ls_id, memory_context_, all_vsag_use_mem_))) {
          LOG_WARN("failed to init ls vector index mgr", KR(ret), K(ls_id));
        } else if (OB_FAIL(get_ls_index_mgr_map().set_refactored(ls_id, new_ls_index_mgr))) {
          if (ret != OB_HASH_EXIST) {
            LOG_WARN("set vector index mgr map faild", KR(ret), K(ls_id));
          }
        }
        if (OB_FAIL(ret)) {
          new_ls_index_mgr->~ObPluginVectorIndexMgr();
          allocator_.free(mgr_buff);
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
  return ret;
}

int ObPluginVectorIndexService::create_partial_adapter(ObLSID ls_id,
                                                       ObTabletID idx_tablet_id,
                                                       ObTabletID data_tablet_id,
                                                       ObIndexType type,
                                                       int64_t index_table_id,
                                                       ObString *vec_index_param,
                                                       int64_t dim)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  ObPluginVectorIndexMgr *new_ls_index_mgr = nullptr;
  ObPluginVectorIndexAdaptor *tmp_vec_idx_adpt = nullptr;

  if (OB_FAIL(acquire_vector_index_mgr(ls_id, ls_index_mgr))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index mgr for ls", KR(ret), K(ls_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_index_mgr->create_partial_adapter(idx_tablet_id,
                                                          data_tablet_id,
                                                          type,
                                                          allocator_,
                                                          index_table_id,
                                                          vec_index_param,
                                                          dim))) {
    LOG_WARN("set vector index adapter faild", K(ls_id), K(idx_tablet_id), KR(ret));
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
                                                           allocator_))) {
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
    FLOG_INFO("destroy vector index service", K_(tenant_id));
    is_inited_ = false;
    has_start_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    is_ls_or_tablet_changed_ = false;
    schema_service_ = NULL;
    ls_service_ = NULL;
    sql_proxy_ = NULL;

    FOREACH(iter, vec_idx_tmp_map_) {
      ObVectorIndexTmpInfo *tmp_info = iter->second;
      if (OB_NOT_NULL(tmp_info)) {
        tmp_info->~ObVectorIndexTmpInfo();
        allocator_.free(tmp_info);
        tmp_info = nullptr;
      }
    }
    vec_idx_tmp_map_.destroy();

    FOREACH(iter, index_ls_mgr_map_) {
      const ObLSID &ls_id = iter->first;
      ObPluginVectorIndexMgr *ls_index_mgr = iter->second;
      if (OB_NOT_NULL(ls_index_mgr)) {
        ls_index_mgr->destroy();
        allocator_.free(ls_index_mgr);
        ls_index_mgr = nullptr;
      }
    }
    index_ls_mgr_map_.destroy();

    allocator_.reset();
    alloc_.reset();

    // destroy vec async task
    if (OB_NOT_NULL(tenant_vec_async_task_sched_)) {
      tenant_vec_async_task_sched_->destroy();  // destroy tg_id_
      ob_free(tenant_vec_async_task_sched_);
      tenant_vec_async_task_sched_ = nullptr;
    }

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
  } else if (OB_FAIL(vec_idx_tmp_map_.create(common::hash::cal_next_prime(DEFAULT_LS_HASH_SIZE),
                                              "AsyncTaskMap",
                                              "AsyncTaskMap",
                                              tenant_id))) {
    LOG_WARN("create ls mgr ", KR(ret), K(tenant_id));
  } else if (FALSE_IT(alloc_.set_tenant_id(tenant_id))) {
  } else if (OB_FAIL(allocator_.init(&alloc_, OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
    LOG_WARN("ObTenantSrs allocator init failed.", K(ret));
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
  } else {
    if (OB_ISNULL(tenant_vec_async_task_sched_)) {
      if (OB_FAIL(alloc_tenant_vec_async_task_sched())) {
        LOG_WARN("fail to alloc tenant_vec_async_task_sched_", K(ret), K(tenant_id_));
      }
    }
    if (OB_SUCC(ret)) {
      if (!is_vec_async_task_started_) {
        if (OB_FAIL(tenant_vec_async_task_sched_->start())) {
          LOG_WARN("fail to start tenant_vec_async_task_sched_", K(ret), K(tenant_id_));
        } else {
          is_vec_async_task_started_ = true;
        }
      } else {
        tenant_vec_async_task_sched_->resume();
      }
    }
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("ObPluginVectorIndexService: finish switch_to_leader", KR(ret), K(tenant_id_), K(cost_us), KP(tenant_vec_async_task_sched_));
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
  const int64_t start_time_us = ObTimeUtility::current_time();
  if (OB_NOT_NULL(tenant_vec_async_task_sched_)) {
    tenant_vec_async_task_sched_->pause();
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
  FLOG_INFO("ObPluginVectorIndexService: switch_to_follower", K(tenant_id_), K(cost_us), KP(tenant_vec_async_task_sched_));
}

int ObPluginVectorIndexService::alloc_tenant_vec_async_task_sched()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  int64_t len = sizeof(ObTenantVecAsyncTaskScheduler);
  ObMemAttr attr(MTL_ID(), "VecIdxAsyncTask");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPluginVectorIndexService is not inited", K(ret), K(tenant_id_));
  } else if (OB_NOT_NULL(tenant_vec_async_task_sched_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_vec_async_task_sched_ is not null", K_(tenant_id), KR(ret), KP(tenant_vec_async_task_sched_));
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObTenantVecAsyncTaskScheduler), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K_(tenant_id), K(len));
  } else if (FALSE_IT(tenant_vec_async_task_sched_ = new(buf) ObTenantVecAsyncTaskScheduler())) {
  } else if (OB_FAIL(tenant_vec_async_task_sched_->init(tenant_id_, *GCTX.sql_proxy_))) {
    LOG_WARN("fail to init tenant_vec_async_task_sched_", K(ret), K(tenant_id_));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(tenant_vec_async_task_sched_)) {
      tenant_vec_async_task_sched_->destroy();  // destroy tg_id_
      ob_free(tenant_vec_async_task_sched_);
      tenant_vec_async_task_sched_ = nullptr;
    }
  }
  LOG_DEBUG("finish alloc_tenant_vec_async_task_sched", K(ret), K(tenant_id_));
  return ret;
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
  }
  return ret;
}

void ObPluginVectorIndexService::stop()
{
  if (IS_INIT) {
    LOG_INFO("stop vector index service", K_(tenant_id), K_(is_inited));
    if (OB_NOT_NULL(tenant_vec_async_task_sched_)) {
      tenant_vec_async_task_sched_->stop();
    }
    get_vec_async_task_handle().stop();
    kmeans_build_task_handler_.stop();
    embedding_task_handler_.stop();
  }
}

void ObPluginVectorIndexService::wait()
{
  if (IS_INIT) {
    LOG_INFO("wait vector index service", K_(tenant_id));
    if (OB_NOT_NULL(tenant_vec_async_task_sched_)) {
      tenant_vec_async_task_sched_->wait();
    }
    kmeans_build_task_handler_.wait();
    embedding_task_handler_.wait();
  }
}

// for debug
void ObPluginVectorIndexMgr::dump_all_inst()
{
  int ret = OB_SUCCESS;
  RLockGuard lock_guard(adapter_map_rwlock_);
  FOREACH(iter, partial_index_adpt_map_) {
    const ObTabletID &tablet_id = iter->first;
    ObPluginVectorIndexAdaptor *adapter = iter->second;
    ObVectorIndexParam *hnsw_param = (adapter == nullptr)? nullptr : (ObVectorIndexParam *)(adapter->get_algo_data());
    LOG_INFO("dump partial index adapter", K(ls_id_), K(tablet_id), KP(adapter), KPC(adapter), KPC(hnsw_param));
  }
  FOREACH(iter, complete_index_adpt_map_) {
    const ObTabletID &tablet_id = iter->first;
    ObPluginVectorIndexAdaptor *adapter = iter->second;
    ObVectorIndexParam *hnsw_param = (adapter == nullptr)? nullptr : (ObVectorIndexParam *)(adapter->get_algo_data());
    LOG_INFO("dump complete index adapter", K(ls_id_), K(tablet_id), KP(adapter), KPC(adapter), KPC(hnsw_param));
  }
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
  RLockGuard lock_guard(adapter_map_rwlock_);
  FOREACH_X(iter, partial_index_adpt_map_, OB_SUCC(ret)) {
    const ObTabletID &tablet_id = iter->first;
    pair.ls_id_ = ls_id_;
    pair.tablet_id_ = tablet_id;
    if (OB_FAIL(partial_tablet_ids.push_back(pair))) {
      LOG_WARN("failed to push array", K(ret));
    }
  }
  FOREACH_X(iter, complete_index_adpt_map_, OB_SUCC(ret)) {
    const ObTabletID &tablet_id = iter->first;
    pair.ls_id_ = ls_id_;
    pair.tablet_id_ = tablet_id;
    if (OB_FAIL(complete_tablet_ids.push_back(pair))) {
      LOG_WARN("failed to push array", K(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexService::get_snapshot_ids(
    ObIArray<ObLSTabletPair> &complete_tablet_ids,
    ObIArray<ObLSTabletPair> &partial_tablet_ids)
{
  int ret = OB_SUCCESS;
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

// for complete
int ObPluginVectorIndexMgr::replace_with_complete_adapter(ObVectorIndexAdapterCandiate *candidate,
                                                          ObVecIdxSharedTableInfoMap &info_map,
                                                          ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdapterGuard &inc_adapter_guard = candidate->inc_adatper_guard_;
  ObPluginVectorIndexAdapterGuard &bitmap_adapter_guard = candidate->bitmp_adatper_guard_;
  ObPluginVectorIndexAdapterGuard &sn_adapter_guard = candidate->sn_adatper_guard_;
  ObPluginVectorIndexAdapterGuard &embedded_adapter_guard = candidate->embedded_adatper_guard_;
  bool is_hybrid = embedded_adapter_guard.is_valid();
  // create new adapter
  ObPluginVectorIndexAdaptor *new_adapter = nullptr;
  bool set_success = false;
  void *adpt_buff = allocator.alloc(sizeof(ObPluginVectorIndexAdaptor));
  if (OB_ISNULL(adpt_buff)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for vector index adapter", KR(ret));
  } else {
    new_adapter = new(adpt_buff)ObPluginVectorIndexAdaptor(&allocator, memory_context_, tenant_id_);
    new_adapter->set_create_type(CreateTypeComplete);
    if (OB_FAIL(new_adapter->merge_parital_index_adapter(inc_adapter_guard.get_adatper()))) {
      LOG_WARN("failed to merge inc index adapter", KPC(inc_adapter_guard.get_adatper()), KR(ret));
    } else if (OB_FAIL(new_adapter->merge_parital_index_adapter(bitmap_adapter_guard.get_adatper()))) {
      LOG_WARN("failed to merge bitmap index adapter", KPC(bitmap_adapter_guard.get_adatper()), KR(ret));
    } else if (OB_FAIL(new_adapter->merge_parital_index_adapter(sn_adapter_guard.get_adatper()))) {
      LOG_WARN("failed to merge snapshot index adapter", KPC(sn_adapter_guard.get_adatper()), KR(ret));
    } else if (is_hybrid && OB_FAIL(new_adapter->merge_parital_index_adapter(embedded_adapter_guard.get_adatper()))) {
      LOG_WARN("failed to merge embedded adapter", KPC(embedded_adapter_guard.get_adatper()), KR(ret));
      // still call init to avoid not all 3 part of partial adapter called before merge
    } else if (OB_FAIL(new_adapter->init(memory_context_, all_vsag_use_mem_))) {
      LOG_WARN("failed to init adpt.", K(ret));
    } else if (!new_adapter->is_vid_rowkey_info_valid()) {
      ObVectorIndexSharedTableInfo info;
      if (OB_FAIL(info_map.get_refactored(new_adapter->get_data_tablet_id(), info))) {
        LOG_WARN("failed to get vector index shared table info",
          K(new_adapter->get_data_tablet_id()), KR(ret));
      } else {
        if (info.rowkey_vid_table_id_ != OB_INVALID_ID) {
          new_adapter->set_is_need_vid(true);
          new_adapter->set_vid_rowkey_info(info);
        } else {
          new_adapter->set_is_need_vid(false);
          new_adapter->set_data_table_id(info);
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      WLockGuard lock_guard(adapter_map_rwlock_);
      int overwrite = 0;
      // should not fail in followring process
      if (OB_FAIL(set_complete_adapter_(new_adapter->get_inc_tablet_id(), new_adapter, overwrite))) {
        LOG_WARN("failed to set new complete partial adapter", K(new_adapter->get_inc_tablet_id()), KR(ret));
      } else if (OB_FAIL(set_complete_adapter_(new_adapter->get_vbitmap_tablet_id(), new_adapter, overwrite))) {
        LOG_WARN("failed to set new complete partial adapter", K(new_adapter->get_vbitmap_tablet_id()), KR(ret));
        if (OB_FAIL(erase_complete_adapter(new_adapter->get_inc_tablet_id()))) {
          LOG_WARN("fail to release complete index adapter", K(new_adapter->get_inc_tablet_id()), KR(ret));
        } else {
          new_adapter = nullptr;
        }
      } else if (OB_FAIL(set_complete_adapter_(new_adapter->get_snap_tablet_id(), new_adapter, overwrite))) {
        LOG_WARN("failed to set new full partial adapter", K(new_adapter->get_snap_tablet_id()), KR(ret));
        if (OB_FAIL(erase_complete_adapter(new_adapter->get_inc_tablet_id()))) {
          LOG_WARN("fail to release complete index adapter", K(new_adapter->get_inc_tablet_id()), KR(ret));
        } else if (OB_FAIL(erase_complete_adapter(new_adapter->get_vbitmap_tablet_id()))) {
          LOG_WARN("fail to release complete index adapter", K(new_adapter->get_vbitmap_tablet_id()), KR(ret));
        } else {
          new_adapter = nullptr;
        }
      } else if (is_hybrid && OB_FAIL(set_complete_adapter_(new_adapter->get_embedded_tablet_id(), new_adapter, overwrite))) {
          LOG_WARN("failed to set new full partial adapter", K(new_adapter->get_embedded_tablet_id()), KR(ret));
          if (OB_FAIL(erase_complete_adapter(new_adapter->get_inc_tablet_id()))) {
          LOG_WARN("fail to release complete index adapter", K(new_adapter->get_inc_tablet_id()), KR(ret));
          } else if (OB_FAIL(erase_complete_adapter(new_adapter->get_vbitmap_tablet_id()))) {
            LOG_WARN("fail to release complete index adapter", K(new_adapter->get_vbitmap_tablet_id()), KR(ret));
          } else if (OB_FAIL(erase_complete_adapter(new_adapter->get_snap_tablet_id()))) {
            LOG_WARN("fail to release complete index adapter", K(new_adapter->get_snap_tablet_id()), KR(ret));
          } else {
            new_adapter = nullptr;
          }
      } else {
        set_success = true;
        if (OB_FAIL(erase_partial_adapter_(new_adapter->get_inc_tablet_id()))) {
          LOG_WARN("fail to release partial index adapter", K(new_adapter->get_inc_tablet_id()), KR(ret));
        } else if (OB_FAIL(erase_partial_adapter_(new_adapter->get_vbitmap_tablet_id()))) {
          LOG_WARN("fail to release partial index adapter", K(new_adapter->get_vbitmap_tablet_id()), KR(ret));
        } else if (OB_FAIL(erase_partial_adapter_(new_adapter->get_snap_tablet_id()))) {
          LOG_WARN("fail to release partial index adapter", K(new_adapter->get_snap_tablet_id()), KR(ret));
        } else if (is_hybrid && OB_FAIL(erase_partial_adapter_(new_adapter->get_embedded_tablet_id()))) {
          LOG_WARN("fail to release partial index adapter", K(new_adapter->get_embedded_tablet_id()), KR(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(new_adapter) && set_success == false) {
    new_adapter->~ObPluginVectorIndexAdaptor();
    allocator.free(adpt_buff);
    new_adapter = nullptr;
    adpt_buff = nullptr;
  }
  return ret;
}

int ObPluginVectorIndexMgr::replace_old_adapter(ObPluginVectorIndexAdaptor *new_adapter)
{
  int ret = 0;
  if (OB_ISNULL(new_adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null adapter", KR(ret));
  } else {
    int overwrite = 0;
    // should not fail in following process
    if (OB_FAIL(erase_complete_adapter(new_adapter->get_inc_tablet_id()))) {
      LOG_WARN("failed to erase new complete partial adapter", K(new_adapter->get_inc_tablet_id()), KR(ret));
    } else if (OB_FAIL(erase_complete_adapter(new_adapter->get_vbitmap_tablet_id()))) {
      LOG_WARN("failed to erase new complete partial adapter", K(new_adapter->get_vbitmap_tablet_id()), KR(ret));
    } else if (OB_FAIL(erase_complete_adapter(new_adapter->get_snap_tablet_id()))) {
      LOG_WARN("failed to erase new complete partial adapter", K(new_adapter->get_snap_tablet_id()), KR(ret));
    } else if (OB_FAIL(set_complete_adapter_(new_adapter->get_inc_tablet_id(), new_adapter, overwrite))) {
      LOG_WARN("failed to set new complete partial adapter", K(new_adapter->get_inc_tablet_id()), KR(ret));
    } else if (OB_FAIL(set_complete_adapter_(new_adapter->get_vbitmap_tablet_id(), new_adapter, overwrite))) {
      LOG_WARN("failed to set new complete partial adapter", K(new_adapter->get_vbitmap_tablet_id()), KR(ret));
    } else if (OB_FAIL(set_complete_adapter_(new_adapter->get_snap_tablet_id(), new_adapter, overwrite))) {
      LOG_WARN("failed to set new complete partial adapter", K(new_adapter->get_snap_tablet_id()), KR(ret));
    }

    if (OB_SUCC(ret) && new_adapter->is_embedded_tablet_valid()) {
      if (OB_FAIL(erase_complete_adapter(new_adapter->get_embedded_tablet_id()))) {
        LOG_WARN("failed to erase new complete partial adapter", K(new_adapter->get_embedded_tablet_id()), KR(ret));
      } else if (OB_FAIL(set_complete_adapter_(new_adapter->get_embedded_tablet_id(), new_adapter, overwrite))) {
        LOG_WARN("failed to set new complete partial adapter", K(new_adapter->get_embedded_tablet_id()), KR(ret));
      }
    }
  }
  return ret;
}

// for full partial
int ObPluginVectorIndexMgr::replace_with_full_partial_adapter(ObVectorIndexAcquireCtx &ctx,
                                                              ObIAllocator &allocator,
                                                              ObPluginVectorIndexAdapterGuard &adapter_guard,
                                                              ObString *vec_index_param,
                                                              int64_t dim,
                                                              ObVectorIndexAdapterCandiate *candidate)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdapterGuard &inc_adapter_guard = candidate->inc_adatper_guard_;
  ObPluginVectorIndexAdapterGuard &bitmap_adapter_guard = candidate->bitmp_adatper_guard_;
  ObPluginVectorIndexAdapterGuard &sn_adapter_guard = candidate->sn_adatper_guard_;
  ObPluginVectorIndexAdapterGuard &emdedded_adapter_guard = candidate->embedded_adatper_guard_;
  // create new adapter
  ObPluginVectorIndexAdaptor *new_adapter = nullptr;
  bool set_success = false;
  void *adpt_buff = allocator.alloc(sizeof(ObPluginVectorIndexAdaptor));
  if (OB_ISNULL(adpt_buff)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for vector index adapter", KR(ret));
  } else {
    new_adapter = new(adpt_buff)ObPluginVectorIndexAdaptor(&allocator, memory_context_, tenant_id_);
    new_adapter->set_create_type(CreateTypeFullPartial);
    if (OB_FAIL(new_adapter->set_tablet_id(VIRT_INC, ctx.inc_tablet_id_))) {
      LOG_WARN("failed to set inc tablet id", K(ctx), KR(ret));
    } else if (OB_FAIL(new_adapter->set_tablet_id(VIRT_BITMAP, ctx.vbitmap_tablet_id_))) {
      LOG_WARN("failed to set snapshot bitmap tablet id", K(ctx), KR(ret));
    } else if (OB_FAIL(new_adapter->set_tablet_id(VIRT_SNAP, ctx.snapshot_tablet_id_))) {
      LOG_WARN("failed to set snapshot index tablet id", K(ctx), KR(ret));
    } else if (OB_FAIL(new_adapter->set_tablet_id(VIRT_DATA, ctx.data_tablet_id_))) {
      LOG_WARN("failed to set data tablet id", K(ctx), KR(ret));
    } else if (ctx.embedded_tablet_id_.is_valid() && OB_FAIL(new_adapter->set_tablet_id(VIRT_EMBEDDED, ctx.embedded_tablet_id_))) {
      LOG_WARN("failed to set embedded tablet id", K(ctx), KR(ret));
    } else if (OB_FAIL(new_adapter->merge_parital_index_adapter(inc_adapter_guard.get_adatper()))) {
      LOG_WARN("failed to merge inc index adapter",
        K(ctx), KPC(inc_adapter_guard.get_adatper()), KR(ret));
    } else if (OB_FAIL(new_adapter->merge_parital_index_adapter(bitmap_adapter_guard.get_adatper()))) {
      LOG_WARN("failed to merge bitmap index adapter",
        K(ctx), KPC(bitmap_adapter_guard.get_adatper()), KR(ret));
    } else if (OB_FAIL(new_adapter->merge_parital_index_adapter(sn_adapter_guard.get_adatper()))) {
      LOG_WARN("failed to merge snapshot index adapter",
        K(ctx), KPC(sn_adapter_guard.get_adatper()), KR(ret));
    } else if (ctx.embedded_tablet_id_.is_valid() && OB_FAIL(new_adapter->merge_parital_index_adapter(emdedded_adapter_guard.get_adatper()))) {
      LOG_WARN("failed to merge emdedded adapter",
        K(ctx), KPC(emdedded_adapter_guard.get_adatper()), KR(ret));
      // still call init to avoid not all 3 part of partial adapter called before merge
    } else if ((OB_NOT_NULL(vec_index_param) && !vec_index_param->empty())
                && OB_FAIL(new_adapter->init(*vec_index_param, dim, memory_context_, all_vsag_use_mem_))) {
      LOG_WARN("failed to init adpt.", K(ret), K(*vec_index_param), K(dim));
    } else {
      ObPluginVectorIndexAdapterGuard old_inc_adapter_guard;
      ObPluginVectorIndexAdapterGuard old_bitmap_adapter_guard;
      ObPluginVectorIndexAdapterGuard old_sn_adapter_guard;
      ObPluginVectorIndexAdapterGuard old_emdedded_adapter_guard;
      WLockGuard lock_guard(adapter_map_rwlock_);
      int overwrite = 1;
      // should not fail in followring process
      if (OB_FAIL(get_adapter_inst_guard_in_lock(ctx.inc_tablet_id_, old_inc_adapter_guard))) {
        LOG_WARN("failed to get adapter", K(ret), K(ctx.inc_tablet_id_));
      } else if (OB_FAIL(get_adapter_inst_guard_in_lock(ctx.vbitmap_tablet_id_, old_bitmap_adapter_guard))) {
        LOG_WARN("failed to get adapter", K(ret), K(ctx.vbitmap_tablet_id_));
      } else if (OB_FAIL(get_adapter_inst_guard_in_lock(ctx.snapshot_tablet_id_, old_sn_adapter_guard))) {
        LOG_WARN("failed to get adapter", K(ret), K(ctx.snapshot_tablet_id_));
      } else if (ctx.embedded_tablet_id_.is_valid() && OB_FAIL(get_adapter_inst_guard_in_lock(ctx.embedded_tablet_id_, old_emdedded_adapter_guard))) {
        LOG_WARN("failed to get adapter", K(ret), K(ctx.embedded_tablet_id_));
      } else if (OB_FAIL(set_partial_adapter_(ctx.inc_tablet_id_, new_adapter, overwrite))) {
        LOG_WARN("failed to set new full partial adapter", K(ctx.inc_tablet_id_), KR(ret));
      } else if (OB_FAIL(set_partial_adapter_(ctx.vbitmap_tablet_id_, new_adapter, overwrite))) {
        LOG_WARN("failed to set new full partial adapter", K(ctx.vbitmap_tablet_id_), KR(ret));
      } else if (OB_FAIL(set_partial_adapter_(ctx.snapshot_tablet_id_, new_adapter, overwrite))) {
        LOG_WARN("failed to set new full partial adapter", K(ctx.snapshot_tablet_id_), KR(ret));
      } else if (OB_FAIL(ctx.embedded_tablet_id_.is_valid() && set_partial_adapter_(ctx.embedded_tablet_id_, new_adapter, overwrite))) {
        LOG_WARN("failed to set new full partial adapter", K(ctx.embedded_tablet_id_), KR(ret));
      } else if (OB_FAIL(adapter_guard.set_adapter(new_adapter))) {
        LOG_WARN("failed to set adapter", K(ctx), KR(ret));
      } else {
        bool set_success = false;
        // release because they are removed from hashmap
        ObPluginVectorIndexAdaptor *inc_adapter = old_inc_adapter_guard.get_adatper();
        ObPluginVectorIndexAdaptor *bitmap_adapter = old_bitmap_adapter_guard.get_adatper();
        ObPluginVectorIndexAdaptor *sn_adapter = old_sn_adapter_guard.get_adatper();

        if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(inc_adapter))) {
          LOG_WARN("fail to release vector index adapter",
            K(ctx.inc_tablet_id_), KPC(inc_adapter), KR(ret));
        } else if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(bitmap_adapter))) {
          LOG_WARN("fail to release vector index adapter",
            K(ctx.vbitmap_tablet_id_), KPC(bitmap_adapter), KR(ret));
        } else if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(sn_adapter))) {
          LOG_WARN("fail to release vector index adapter",
            K(ctx.snapshot_tablet_id_), KPC(sn_adapter), KR(ret));
        } else if (ctx.embedded_tablet_id_.is_valid()) {
          ObPluginVectorIndexAdaptor *emdedded_adapter = old_emdedded_adapter_guard.get_adatper();
          if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(emdedded_adapter))) {
            LOG_WARN("fail to release vector index adapter",
              K(ctx.embedded_tablet_id_), KPC(emdedded_adapter), KR(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret) && set_success == false && OB_NOT_NULL(new_adapter)) {
    new_adapter->~ObPluginVectorIndexAdaptor();
    allocator.free(adpt_buff);
    adpt_buff = nullptr;
    new_adapter = nullptr;
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
            } else if (OB_NOT_NULL(expr_cache)) {
              if (OB_FAIL(expr_cache->append_center(centroid_vec))) {
                LOG_WARN("failed to add pq centroid to expression cache", K(ret), K(m_idx), K(i));
              }
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
                                                     ObExprVecIvfCenterIdCache *expr_cache)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(cent_cache)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("cent_cache is null", K(ret));
  } else {
    uint64_t capacity = cent_cache->get_count();

    for (uint64_t i = 1; i <= capacity && OB_SUCC(ret); ++i) {
      float *centroid_vec = nullptr;
      if (OB_FAIL(cent_cache->read_centroid(i, centroid_vec))) {
        LOG_WARN("fail to read centroid", K(ret), K(i));
      } else if (OB_ISNULL(centroid_vec)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read centroid returned null pointer", K(i));
      } else {
        if (OB_FAIL(aux_info.push_back(centroid_vec))) {
          LOG_WARN("failed to add centroid to aux_info array", K(ret), K(i));
        } else if (OB_NOT_NULL(expr_cache)) {
          if (OB_FAIL(expr_cache->append_center(centroid_vec))) {
            LOG_WARN("failed to add centroid to expression cache", K(ret), K(i));
          }
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
    LOG_DEBUG("Failed to get ls_id by cache_tablet_id, will try original logic", K(ret), K(cache_tablet_id));
  } else {

    ObIvfCacheMgrKey cache_key(cache_tablet_id);

    if (OB_FAIL(acquire_ivf_cache_mgr_guard(ls_id, cache_key, cache_guard))) {
      ret = OB_CACHE_NOT_HIT;
      LOG_DEBUG("Failed to acquire cache mgr guard, will try original logic", K(ret), K(ls_id), K(cache_key));
    } else if (OB_ISNULL(cache_mgr = cache_guard.get_ivf_cache_mgr())) {
      ret = OB_CACHE_NOT_HIT;
      LOG_DEBUG("Cache mgr is null, will try original logic", K(ret));
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

          LOG_DEBUG("Found completed cache, reading data", K(capacity), K(cache_type));

          switch (cache_type) {
            case IvfCacheType::IVF_PQ_CENTROID_CACHE: {
              if (OB_FAIL(process_pq_centroid_cache(cent_cache, aux_info, expr_cache, m))) {
                LOG_WARN("failed to process PQ centroid cache", K(ret));
              }
              break;
            }
            case IvfCacheType::IVF_CENTROID_CACHE: {
              if (OB_FAIL(process_centroid_cache(cent_cache, aux_info, expr_cache))) {
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

              if (OB_NOT_NULL(expr_cache)) {
                expr_cache->set_cache_key(table_id, tablet_id);
                LOG_DEBUG("System cache hit, expression cache updated with shallow copy", K(table_id), K(tablet_id), K(aux_info.count()));
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("[IVF_CACHE_ERROR] Cache is completed but contains no data, possible corruption",
                       K(ret), K(table_id), K(cache_tablet_id), K(cache_type), K(capacity),
                       "cache_type_name", cache_type == IvfCacheType::IVF_CENTROID_CACHE ? "IVF_CENTROID" : "PQ_CENTROID");
            }
          } else {
            ret = OB_CACHE_NOT_HIT;
            LOG_DEBUG("[IVF_CACHE_MISS] Cache processing failed, will try original logic", K(ret), K(table_id), K(cache_tablet_id));
          }
        } else {
          ret = OB_CACHE_NOT_HIT;
          LOG_DEBUG("Cache not completed, will try original logic", K(ret), K(cache_type));
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
    session_param.ddl_info_.set_is_dummy_ddl_for_inner_visibility(true);
    session_param.ddl_info_.set_source_table_hidden(is_hidden_table);
    session_param.ddl_info_.set_dest_table_hidden(false);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy_->read(res, tenant_id_, sql_string.ptr(), &session_param))) {
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
  } else if (OB_FAIL(ls_index_mgr->get_or_create_ivf_cache_mgr_guard(allocator_, key, vec_index_param, dim, table_id, cache_mgr_guard))) {
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

}
}
