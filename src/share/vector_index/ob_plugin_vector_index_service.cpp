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

namespace oceanbase
{
namespace share
{

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
    mem_sync_info_.destroy();
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
    const ObTabletID &tablet_id = iter->first;
    ObIvfBuildHelper *helper = iter->second;
    if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_build_helper(helper))) {
      LOG_ERROR("fail to release vector index adapter", K(tablet_id), KR(ret));
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
  if (OB_FAIL(complete_index_adpt_map_.create(hash_capacity, "VecIdxAdptMap", "VecIdxAdptMap", tenant_id))) {
    LOG_WARN("fail to create full index adapter map", K(ls_id), KR(ret));
  } else if (OB_FAIL(partial_index_adpt_map_.create(hash_capacity, "VecIdxAdptMap", "VecIdxAdptMap", tenant_id))) {
    LOG_WARN("fail to create partial index adapter map", K(ls_id), KR(ret));
  } else if (OB_FAIL(ivf_index_helper_map_.create(hash_capacity, "IvfIdxHpMap", "IvfIdxHpMap", tenant_id))) {
    LOG_WARN("fail to create ivf index build helper map", K(ls_id), KR(ret));
  } else if (OB_FAIL(mem_sync_info_.init(hash_capacity, tenant_id, ls_id))) {
    LOG_WARN("fail to create first mem sync set", K(ls_id), KR(ret));
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

int ObPluginVectorIndexMgr::erase_ivf_build_helper(ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  ObIvfBuildHelper *helper_inst = nullptr;
  if (OB_FAIL(ivf_index_helper_map_.erase_refactored(tablet_id, &helper_inst))) {
    LOG_WARN("failed to erase ivf build helper", K(tablet_id), KR(ret));
  } else if (OB_ISNULL(helper_inst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("helper inst is null", K(tablet_id), KR(ret));
  } else {
    if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_build_helper(helper_inst))) {
      LOG_WARN("fail to release ivf build helper", K(tablet_id), KR(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexMgr::get_adapter_inst_guard(ObTabletID tablet_id,  ObPluginVectorIndexAdapterGuard &adpt_guard)
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

int ObPluginVectorIndexMgr::get_build_helper_inst_(ObTabletID tablet_id, ObIvfBuildHelper *&helper_inst)
{
  int ret = OB_SUCCESS;
  helper_inst = nullptr;

  if (OB_FAIL(ivf_index_helper_map_.get_refactored(tablet_id, helper_inst))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get ivf index build helper inst", K(tablet_id), KR(ret));
    }
  } else if (OB_ISNULL(helper_inst)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ivf index build helper inst", K(tablet_id), KR(ret));
  }

  return ret;
}

int ObPluginVectorIndexMgr::get_build_helper_inst_guard(ObTabletID tablet_id, ObIvfBuildHelperGuard &helper_guard)
{
  int ret = OB_SUCCESS;
  ObIvfBuildHelper *helper_inst = nullptr;
  if (OB_FAIL(get_build_helper_inst_(tablet_id, helper_inst))) {
    LOG_WARN("failed to get adapter inst", K(tablet_id), KR(ret));
  } else if (OB_FAIL(helper_guard.set_helper(helper_inst))) {
    LOG_WARN("failed to set adapter", K(tablet_id), KR(ret));
  }
  return ret;
}

int ObPluginVectorIndexMgr::create_ivf_build_helper(
    ObTabletID idx_tablet_id,
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
      if (OB_FAIL(tmp_ivf_build_helper->init(vec_index_param))) {
        LOG_WARN("failed to init ivf build helper", K(ret));
      }
    }
  } else if (INDEX_TYPE_VEC_IVFSQ8_META_LOCAL == type) {
    if (OB_ISNULL(helper_buff = allocator.alloc(sizeof(ObIvfSq8BuildHelper)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ivf index build helper", KR(ret));
    } else {
      tmp_ivf_build_helper = new(helper_buff)ObIvfSq8BuildHelper(&allocator, tenant_id_);
      if (OB_FAIL(tmp_ivf_build_helper->init(vec_index_param))) {
        LOG_WARN("failed to init ivf build helper", K(ret), K(vec_index_param));
      }
    }
  } else if (INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL == type) {
    if (OB_ISNULL(helper_buff = allocator.alloc(sizeof(ObIvfPqBuildHelper)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ivf index build helper", KR(ret));
    } else {
      tmp_ivf_build_helper = new(helper_buff)ObIvfPqBuildHelper(&allocator, tenant_id_);
      if (OB_FAIL(tmp_ivf_build_helper->init(vec_index_param))) {
        LOG_WARN("failed to init ivf build helper", K(ret), K(vec_index_param));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported index type", K(ret), K(type));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_ivf_build_helper_(idx_tablet_id, tmp_ivf_build_helper))) {
      LOG_WARN("set ivf index build helper faild", K(idx_tablet_id), KR(ret));
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

int ObPluginVectorIndexMgr::set_ivf_build_helper_(ObTabletID tablet_id, ObIvfBuildHelper *helper_inst)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ivf_index_helper_map_.set_refactored(tablet_id, helper_inst))) {
    LOG_WARN("failed to set ivf build helper", K(tablet_id), KR(ret));
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
    // fast return if get complete adapter
    if (OB_FAIL(get_or_create_partial_adapter_(ctx.inc_tablet_id_,
                                               INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL,
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
    LOG_WARN("failed to acquire vector index mgr", K(ls_id), KR(ret));
  } else if (OB_FAIL(ls_index_mgr->get_and_merge_adapter(ctx, allocator_, adapter_guard,
                                                         vec_index_param, dim))) {
    LOG_WARN("failed to get and merge adapter", K(ls_id), K(ctx), KR(ret));
  }

  return ret;
}

int ObPluginVectorIndexService::acquire_ivf_build_helper_guard(
    ObLSID ls_id,
    ObTabletID tablet_id,
    ObIndexType type,
    ObIvfBuildHelperGuard &helper_guard,
    ObString &vec_index_param)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  if (OB_FAIL(get_ls_index_mgr_map().get_refactored(ls_id, ls_index_mgr))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index mgr for ls", K(ls_id), KR(ret));
    } else { // create new ls index mgr if not exist
      ret = OB_SUCCESS;
      if (OB_FAIL(create_ivf_build_helper(ls_id, tablet_id, type, vec_index_param))) {
        LOG_WARN("failed to create tmp vector index instance", K(ls_id), K(tablet_id), K(type), KR(ret), K(vec_index_param));
      }
      if (OB_FAIL(ret) && (OB_HASH_EXIST != ret)) {
      } else if (OB_FAIL(get_build_helper_inst_guard(ls_id, tablet_id, helper_guard))) {
        LOG_WARN("failed to get tmp ivf build helper instance", K(ls_id), K(tablet_id), K(type), KR(ret));
      } else {
        LOG_INFO("create ivf build helper success", K(ret), K(ls_id), KPC(helper_guard.get_helper()));
      }
    }
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", K(ls_id), KR(ret));
  } else if (OB_FAIL(ls_index_mgr->get_build_helper_inst_guard(tablet_id, helper_guard))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get ivf build helper", K(ls_id), K(tablet_id), KR(ret));
    } else { // not exist create new
      if (OB_FAIL(ls_index_mgr->create_ivf_build_helper(tablet_id, type, vec_index_param, allocator_))) {
        LOG_WARN("failed to create tmp vector index instance with ls", K(ls_id), K(tablet_id), K(type), KR(ret));
      }
      if (OB_FAIL(ret) && (OB_HASH_EXIST != ret)) {
      } else if (OB_FAIL(ls_index_mgr->get_build_helper_inst_guard(tablet_id, helper_guard))) {
        LOG_WARN("failed to get tmp vector index instance with ls", K(ls_id), K(tablet_id), K(type), KR(ret));
      } else {
        LOG_INFO("create partial index adapter success", K(ret), K(ls_id), K(tablet_id), KPC(helper_guard.get_helper()));
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
      LOG_WARN("failed to get vector index mgr for ls", K(ls_id), KR(ret));
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
    LOG_WARN("get null vector index mgr for ls", K(ls_id), KR(ret));
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
        LOG_INFO("create partial index adapter success", K(ret), K(ls_id), KPC(adapter_guard.get_adatper()));
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
    LOG_WARN("failed to get vector index mgr for ls", K(ls_id), KR(ret));
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", K(ls_id), KR(ret));
  } else if (OB_FAIL(ls_index_mgr->get_adapter_inst_guard(tablet_id, adpt_guard))) {
    LOG_WARN("failed to get vector index adapter", K(ls_id), K(tablet_id), KR(ret));
  }
  return ret;
}

int ObPluginVectorIndexService::get_build_helper_inst_guard(
    ObLSID ls_id,
    ObTabletID tablet_id,
    ObIvfBuildHelperGuard &helper_guard)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  if (OB_FAIL(get_ls_index_mgr_map().get_refactored(ls_id, ls_index_mgr))) {
    LOG_WARN("failed to get vector index mgr for ls", K(ls_id), KR(ret));
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", K(ls_id), KR(ret));
  } else if (OB_FAIL(ls_index_mgr->get_build_helper_inst_guard(tablet_id, helper_guard))) {
    LOG_WARN("failed to get ivf build helper guard", K(ls_id), K(tablet_id), KR(ret));
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
        if (OB_FAIL(new_ls_index_mgr->init(tenant_id_, ls_id, memory_context_, &all_vsag_use_mem_))) {
          LOG_WARN("failed to init ls vector index mgr", K(ls_id), KR(ret));
        } else if (OB_FAIL(get_ls_index_mgr_map().set_refactored(ls_id, new_ls_index_mgr))) {
          if (ret != OB_HASH_EXIST) {
            LOG_WARN("set vector index mgr map faild", K(ls_id), KR(ret));
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
          LOG_WARN("failed to get vector index mgr for ls", K(ls_id), KR(ret));
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
      LOG_WARN("failed to get vector index mgr for ls", K(ls_id), KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", K(ls_id), KR(ret));
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
    ObTabletID idx_tablet_id,
    ObIndexType type,
    ObString &vec_index_param)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  ObIvfBuildHelper *tmp_ivf_build_helper = nullptr;

  if (OB_FAIL(acquire_vector_index_mgr(ls_id, ls_index_mgr))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index mgr for ls", K(ls_id), KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", K(ls_id), KR(ret));
  } else if (OB_FAIL(ls_index_mgr->create_ivf_build_helper(idx_tablet_id,
                                                           type,
                                                           vec_index_param,
                                                           allocator_))) {
    LOG_WARN("failed to create ivf build helper", K(ls_id), K(idx_tablet_id), K(type), KR(ret), K(vec_index_param));
  }

  return ret;
}

int ObPluginVectorIndexService::erase_ivf_build_helper(ObLSID ls_id, ObTabletID idx_tablet_id)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;

  if (OB_FAIL(acquire_vector_index_mgr(ls_id, ls_index_mgr))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get vector index mgr for ls", K(ls_id), KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ls_index_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null vector index mgr for ls", K(ls_id), KR(ret));
  } else if (OB_FAIL(ls_index_mgr->erase_ivf_build_helper(idx_tablet_id))) {
    LOG_WARN("failed to create ivf build helper", K(ls_id), K(idx_tablet_id), KR(ret));
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
    if (memory_context_ != nullptr) {
      DESTROY_CONTEXT(memory_context_);
      memory_context_ = nullptr;
    }
    alloc_.reset();
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
  } else if (FALSE_IT(alloc_.set_tenant_id(tenant_id))) {
  } else if (OB_FAIL(allocator_.init(&alloc_, OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
    LOG_WARN("ObTenantSrs allocator init failed.", K(ret));
  } else {
    lib::ContextParam param;
    param.set_mem_attr(tenant_id)
      .set_properties(lib::ADD_CHILD_THREAD_SAFE | lib::ALLOC_THREAD_SAFE | lib::RETURN_MALLOC_DEFAULT)
      .set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE)
      .set_label("VectorIndexVsag")
      .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
    if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(memory_context_, param))) {
      LOG_WARN("create memory entity failed", K(ret));
    } else {
      tenant_id_ = tenant_id;
      schema_service_ = schema_service;
      ls_service_ = ls_service;
      sql_proxy_ = GCTX.sql_proxy_;
      is_inited_ = true;
      LOG_INFO("plugin vector index service: init", KR(ret), K_(tenant_id));
    }
  }
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
  }
}

void ObPluginVectorIndexService::wait()
{
  if (IS_INIT) {
    LOG_INFO("wait vector index service", K_(tenant_id));
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

// for complete
int ObPluginVectorIndexMgr::replace_with_complete_adapter(ObVectorIndexAdapterCandiate *candidate,
                                                          ObVecIdxSharedTableInfoMap &info_map,
                                                          ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdapterGuard &inc_adapter_guard = candidate->inc_adatper_guard_;
  ObPluginVectorIndexAdapterGuard &bitmap_adapter_guard = candidate->bitmp_adatper_guard_;
  ObPluginVectorIndexAdapterGuard &sn_adapter_guard = candidate->sn_adatper_guard_;
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
      // still call init to avoid not all 3 part of partial adapter called before merge
    } else if (OB_FAIL(new_adapter->init(memory_context_, all_vsag_use_mem_))) {
      LOG_WARN("failed to init adpt.", K(ret));
    } else if (!new_adapter->is_vid_rowkey_info_valid()) {
      ObVectorIndexSharedTableInfo info;
      if (OB_FAIL(info_map.get_refactored(new_adapter->get_data_tablet_id(), info))) {
        LOG_WARN("failed to get vector index shared table info",
          K(new_adapter->get_data_tablet_id()), KR(ret));
      } else {
        new_adapter->set_vid_rowkey_info(info);
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
      } else {
        set_success = true;
        if (OB_FAIL(erase_partial_adapter_(new_adapter->get_inc_tablet_id()))) {
          LOG_WARN("fail to release partial index adapter", K(new_adapter->get_inc_tablet_id()), KR(ret));
        } else if (OB_FAIL(erase_partial_adapter_(new_adapter->get_vbitmap_tablet_id()))) {
          LOG_WARN("fail to release partial index adapter", K(new_adapter->get_vbitmap_tablet_id()), KR(ret));
        } else if (OB_FAIL(erase_partial_adapter_(new_adapter->get_snap_tablet_id()))) {
          LOG_WARN("fail to release partial index adapter", K(new_adapter->get_snap_tablet_id()), KR(ret));
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
    } else if (OB_FAIL(new_adapter->merge_parital_index_adapter(inc_adapter_guard.get_adatper()))) {
      LOG_WARN("failed to merge inc index adapter",
        K(ctx), KPC(inc_adapter_guard.get_adatper()), KR(ret));
    } else if (OB_FAIL(new_adapter->merge_parital_index_adapter(bitmap_adapter_guard.get_adatper()))) {
      LOG_WARN("failed to merge bitmap index adapter",
        K(ctx), KPC(bitmap_adapter_guard.get_adatper()), KR(ret));
    } else if (OB_FAIL(new_adapter->merge_parital_index_adapter(sn_adapter_guard.get_adatper()))) {
      LOG_WARN("failed to merge snapshot index adapter",
        K(ctx), KPC(sn_adapter_guard.get_adatper()), KR(ret));
      // still call init to avoid not all 3 part of partial adapter called before merge
    } else if ((OB_NOT_NULL(vec_index_param) && !vec_index_param->empty())
                && OB_FAIL(new_adapter->init(*vec_index_param, dim, memory_context_, all_vsag_use_mem_))) {
      LOG_WARN("failed to init adpt.", K(ret), K(*vec_index_param), K(dim));
    } else {
      WLockGuard lock_guard(adapter_map_rwlock_);
      int overwrite = 1;
      // should not fail in followring process
      if (OB_FAIL(set_partial_adapter_(ctx.inc_tablet_id_, new_adapter, overwrite))) {
        LOG_WARN("failed to set new full partial adapter", K(ctx.inc_tablet_id_), KR(ret));
      } else if (OB_FAIL(set_partial_adapter_(ctx.vbitmap_tablet_id_, new_adapter, overwrite))) {
        LOG_WARN("failed to set new full partial adapter", K(ctx.vbitmap_tablet_id_), KR(ret));
      } else if (OB_FAIL(set_partial_adapter_(ctx.snapshot_tablet_id_, new_adapter, overwrite))) {
        LOG_WARN("failed to set new full partial adapter", K(ctx.snapshot_tablet_id_), KR(ret));
      } else if (OB_FAIL(adapter_guard.set_adapter(new_adapter))) {
        LOG_WARN("failed to set adapter", K(ctx), KR(ret));
      } else {
        bool set_success = false;
        // release because they are removed from hashmap
        ObPluginVectorIndexAdaptor *inc_adapter = inc_adapter_guard.get_adatper();
        ObPluginVectorIndexAdaptor *bitmap_adapter = bitmap_adapter_guard.get_adatper();
        ObPluginVectorIndexAdaptor *sn_adapter = sn_adapter_guard.get_adatper();

        if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(inc_adapter))) {
          LOG_WARN("fail to release vector index adapter",
            K(ctx.inc_tablet_id_), KPC(inc_adapter), KR(ret));
        } else if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(bitmap_adapter))) {
          LOG_WARN("fail to release vector index adapter",
            K(ctx.vbitmap_tablet_id_), KPC(bitmap_adapter), KR(ret));
        } else if (OB_FAIL(ObPluginVectorIndexUtils::release_vector_index_adapter(sn_adapter))) {
          LOG_WARN("fail to release vector index adapter",
            K(ctx.snapshot_tablet_id_), KPC(sn_adapter), KR(ret));
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

int ObPluginVectorIndexService::get_ivf_aux_info(
    const uint64_t table_id,
    const ObTabletID tablet_id,
    ObIAllocator &allocator,
    ObIArray<float*> &aux_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPluginVectorIndexService is not inited", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(generate_get_aux_info_sql(table_id, tablet_id, sql_string))) {
    LOG_WARN("failed to generate sql", K(ret), K(table_id));
  } else {
    ObSessionParam session_param;
    session_param.sql_mode_ = nullptr;
    session_param.tz_info_wrap_ = nullptr;
    session_param.ddl_info_.set_is_dummy_ddl_for_inner_visibility(true);
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
          LOG_INFO("success to generate sql string", K(ret), K(sql_string), K(table_id), K(tablet_id));
        }
      }
    }

  }
  return ret;
}

}
}
