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
#include "ob_ivf_async_task.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/ob_ls_id.h"
#include "share/vector_index/ob_vector_index_ivf_cache_util.h"

namespace oceanbase
{
namespace share
{
int ObIvfAsyncTask::delete_deprecated_cache(ObPluginVectorIndexService &vector_index_service)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  if (OB_FAIL(vector_index_service.get_ls_index_mgr_map().get_refactored(ls_id_, ls_index_mgr))) {
    if (ret == OB_HASH_NOT_EXIST) {
      // do not need delete
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get vector index mgr for ls", KR(ret), K(ls_id_));
    }
  } else if (OB_FAIL(ls_index_mgr->erase_ivf_cache_mgr(ctx_->task_status_.tablet_id_))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("failed to erase vector index ivf cache mgr",
               K(ls_id_),
               K(ctx_->task_status_.tablet_id_),
               KR(ret));
    } else {  // already removed
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObIvfAsyncTask::write_cache(ObPluginVectorIndexService &vector_index_service)
{
  int ret = OB_SUCCESS;
  ObIvfCacheMgrGuard cache_guard;
  ObIvfCacheMgr *cache_mgr = nullptr;
  ObVectorIndexParam vec_param;
  ObIvfCentCache *cent_cache = nullptr;
  ObIvfAuxTableInfo *aux_table_info = nullptr;
  ObSchemaGetterGuard schema_guard;

  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null ctx_", K(ret), KP(ctx_));
  } else if (OB_ISNULL(aux_table_info = reinterpret_cast<ObIvfAuxTableInfo *>(ctx_->extra_data_))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null aux_table_info", K(ret), KP(ctx_->extra_data_));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_param_with_dim(
                 schema_guard,
                 tenant_id_,
                 ctx_->task_status_.table_id_,
                 aux_table_info->data_table_id_,
                 ObVectorIndexType::VIT_IVF_INDEX,
                 vec_param))) {
    LOG_WARN("fail to get vector index param with dim",
             K(ret),
             K(tenant_id_),
             K(ctx_->task_status_.table_id_),
             KPC(aux_table_info));
  } else if (OB_FAIL(vector_index_service.acquire_ivf_cache_mgr_guard(ls_id_,
                                                                      ctx_->task_status_.tablet_id_,
                                                                      vec_param,
                                                                      vec_param.dim_,
                                                                      ctx_->task_status_.table_id_,
                                                                      cache_guard))) {
    LOG_WARN("fail to acquire ivf cache mgr with vec param",
             K(ret),
             K(ls_id_),
             K(ctx_->task_status_),
             K(vec_param));
  } else if (OB_ISNULL(cache_mgr = cache_guard.get_ivf_cache_mgr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null cache mgr", K(ret));
  } else if (!aux_table_info->is_ivf_centroid_table_valid()) {
    // IVF centroid table is not valid, skip
  } else if (OB_FAIL(cache_mgr->get_or_create_cache_node(IvfCacheType::IVF_CENTROID_CACHE,
                                                         cent_cache))) {
    LOG_WARN("fail to get or create cache node", K(ret));
  } else if (OB_FAIL(ObIvfCacheUtil::scan_and_write_ivf_cent_cache(
                 vector_index_service,
                 aux_table_info->centroid_table_id_,
                 aux_table_info->centroid_tablet_ids_[0],
                 *cent_cache))) {
    LOG_WARN("fail to scan and write ivf cent cache", K(ret), KPC(aux_table_info));
  } else if (aux_table_info->type_ == VIAT_IVF_PQ
             && aux_table_info->is_ivf_pq_centroid_table_valid()) {
    ObIvfCentCache *pq_cent_cache = nullptr;
    if (OB_FAIL(cache_mgr->get_or_create_cache_node(IvfCacheType::IVF_PQ_CENTROID_CACHE,
                                                    pq_cent_cache))) {
      LOG_WARN("fail to get or create cache node", K(ret));
    } else if (OB_FAIL(ObIvfCacheUtil::scan_and_write_ivf_cent_cache(
                   vector_index_service,
                   aux_table_info->pq_centroid_table_id_,
                   aux_table_info->pq_centroid_tablet_ids_[0],
                   *pq_cent_cache))) {
      LOG_WARN("fail to scan and write ivf cent cache", K(ret), K(aux_table_info));
    }
  }
  return ret;
}

int ObIvfAsyncTask::do_work()
{
  int ret = OB_SUCCESS;
  bool is_deprecated = false;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  DEBUG_SYNC(HANDLE_VECTOR_INDEX_ASYNC_TASK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVecIndexAsyncTask is not init", KR(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->ls_) || OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected nullptr", K(ret), KP(ctx_), KP(vector_index_service));
  } else if (OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (ctx_->task_status_.task_type_ == OB_VECTOR_ASYNC_INDEX_IVF_CLEAN) {
    if (OB_FAIL(delete_deprecated_cache(*vector_index_service))) {
      LOG_WARN("fail to delete deprecated cache", K(ret));
    }
  } else if (ctx_->task_status_.task_type_ == OB_VECTOR_ASYNC_INDEX_IVF_LOAD) {
    if (OB_FAIL(write_cache(*vector_index_service))) {
      LOG_WARN("fail to write cache", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task type", K(ret), KPC(ctx_));
  }

  if (OB_NOT_NULL(ctx_)) {
    common::ObSpinLockGuard ctx_guard(ctx_->lock_);
    ctx_->task_status_.ret_code_ = ret;
    ctx_->in_thread_pool_ = false;
  }
  LOG_INFO("end ivf do_work", K(ret), K(ctx_->task_status_.tablet_id_));
  return ret;
}
}  // namespace share
}  // namespace oceanbase