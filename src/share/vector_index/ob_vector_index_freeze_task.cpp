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
#include "ob_vector_index_freeze_task.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/allocator/ob_tenant_vector_allocator.h"
#include "storage/ls/ob_ls.h"
#include "storage/vector_index/ob_vector_index_refresh.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/access/ob_table_scan_iterator.h"

namespace oceanbase
{
namespace share
{

bool ObVecIdxFreezeTaskExecutor::check_operation_allow()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  bool bret = true;
  const bool is_not_support = ! ObPluginVectorIndexHelper::enable_persist_vector_index_incremental(tenant_id_);
  if (is_not_support) {
    bret = false;
    LOG_DEBUG("skip this round, not support async task.");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    bret = false;
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_5_1_0) {
    bret = false;
    LOG_DEBUG("vector index async task can not work with data version less than 4_5_0_0", K(tenant_data_version));
  } else if (OB_FAIL(ObPluginVectorIndexHelper::get_active_segment_max_size(tenant_id_, max_active_segment_size_))) {
    LOG_WARN("get active segment max size fail", K(ret), K(tenant_id_));
  }
  return bret;
}

int ObVecIdxFreezeTaskExecutor::load_task(uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  ObArray<ObVecIndexAsyncTaskCtx*> task_ctx_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector async task not init", KR(ret));
  } else if (!check_operation_allow()) { // skip
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) { // skip
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else {
    ObSharedMemAllocMgr *shared_mem_mgr = MTL(ObSharedMemAllocMgr*);
    ObTenantVectorAllocator& vector_allocator = shared_mem_mgr->vector_allocator();
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    ObIAllocator *allocator = task_opt.get_allocator();
    const int64_t current_task_cnt = ObVecIndexAsyncTaskUtil::get_processing_task_cnt(task_opt);
    RWLock::RLockGuard lock_guard(index_ls_mgr->get_adapter_map_lock());

    // 冻结持久化不会引入额外内存消耗, 这里主要是根据租户剩余内存, 判断是否需要提前冻结，避免合并时内存不足
    // 对与HNSW主要是避免Segment过大, 导致写入变慢
    int64_t avail_merge_mem = 1 * 1024 * 1024 * 1024L;  // default 1GB
    int64_t freeze_segment_size = 128 * 1024 * 1024; // default 128MB
    int64_t total_pre_alloc_size = 0;
    int64_t freeze_threshold = 0;
    FOREACH_X(iter, index_ls_mgr->get_complete_adapter_map(),
        OB_SUCC(ret) && (task_ctx_array.count() + current_task_cnt <= MAX_ASYNC_TASK_PROCESSING_COUNT)) {
      ObTabletID tablet_id = iter->first;
      ObPluginVectorIndexAdaptor *adapter = iter->second;
      if (OB_ISNULL(adapter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else {
        int64_t new_task_id = OB_INVALID_ID;
        int64_t index_table_id = OB_INVALID_ID;
        bool inc_new_task = false;
        common::ObCurTraceId::TraceId new_trace_id;

        char *task_ctx_buf = nullptr;
        ObVecIndexAsyncTaskCtx* task_ctx = nullptr;
        bool need_freeze = false;
        int64_t inc_mem_size = 0;
        if (! adapter->is_complete()) {
          LOG_TRACE("adapter not complete, no need freeze", K(ret), KPC(adapter));
        } else if (adapter->get_create_type() != CreateTypeComplete) {
          LOG_INFO("adapter is not complete, no need freeze", K(ret), KPC(adapter));
        } else if (adapter->is_hybrid_index()) {
          LOG_TRACE("adapter is not hybrid index, no need merge", K(ret), KPC(adapter));
        } else if (adapter->is_sparse_vector_index_type()) {
          LOG_TRACE("adapter is sparse vector index, no need merge", K(ret), KPC(adapter));
        } else if (OB_FALSE_IT(inc_mem_size = adapter->get_incr_vsag_mem_used())) {
        } else if (inc_mem_size <= 0) {
          LOG_TRACE("incr vsag mem used is 0, no need to freeze", K(ret), K(inc_mem_size));
        } else if (OB_FAIL(calc_freeze_threshold(vector_allocator, inc_mem_size, total_pre_alloc_size, freeze_threshold))) {
          if (OB_BUF_NOT_ENOUGH != ret) {
            LOG_WARN("fail to calc freeze threshold", K(ret));
          } else {
            // TODO zhichen
            ret = OB_SUCCESS;
            LOG_INFO("left memory may be not enough, let it go...", K(vector_allocator.get_left_pre_alloc_size()));
          }
        } else if (OB_FAIL(adapter->check_need_freeze(freeze_threshold, need_freeze))) {
          LOG_WARN("failed to check need freeze", K(ret), KPC(adapter));
        } else if (! need_freeze && ! adapter->has_frozen()) {
          LOG_TRACE("active segment is not need to freeze", K(ret), KPC(adapter));
        } else if (adapter->is_frozen_finish()) {
          LOG_INFO("frozen data is already finished, no need to freeze", K(ret), KPC(adapter));
        } else if (OB_ISNULL(task_ctx_buf = static_cast<char *>(allocator->alloc(sizeof(ObVecIndexAsyncTaskCtx))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("async task ctx is null", K(ret));
        } else if (FALSE_IT(task_ctx = new(task_ctx_buf) ObVecIndexAsyncTaskCtx())) {
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(tenant_id_, new_task_id))) {
          LOG_WARN("fail to fetch new task id", K(ret), K(tenant_id_));
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::get_table_id_from_adapter(adapter, tablet_id, index_table_id))) { // only get table 3 table_id to generate new task
          LOG_WARN("fail to get table id from adapter", K(ret), K(tablet_id));
        } else if (OB_INVALID_ID == index_table_id) {
          LOG_DEBUG("index table id is invalid, skip", K(ret)); // skip to next
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_trace_id(++task_trace_base_num, allocator, new_trace_id))) {
          LOG_WARN("fail to fetch new trace id", K(ret), K(tablet_id));
        } else {
          LOG_DEBUG("start load task", K(ret), K(tablet_id), K(tenant_id_), K(task_trace_base_num), K(ls_->get_ls_id()));
          // 1. update task_ctx to async task map
          task_ctx->tenant_id_ = tenant_id_;
          task_ctx->ls_ = ls_;
          task_ctx->task_status_.tablet_id_ = tablet_id.id();
          task_ctx->task_status_.tenant_id_ = tenant_id_;
          task_ctx->task_status_.table_id_ = index_table_id;
          task_ctx->task_status_.task_id_ = new_task_id;
          task_ctx->task_status_.task_type_ = ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_FREEZE;
          task_ctx->task_status_.trigger_type_ = ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_AUTO;
          task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
          task_ctx->task_status_.trace_id_ = new_trace_id;
          task_ctx->allocator_.set_tenant_id(tenant_id_);
          if (OB_FAIL(index_ls_mgr->get_async_task_opt().add_task_ctx(tablet_id, task_ctx, inc_new_task))) { // not overwrite
            LOG_WARN("fail to add task ctx", K(ret));
          } else if (inc_new_task && OB_FAIL(task_ctx_array.push_back(task_ctx))) {
            LOG_WARN("fail to push back task status", K(ret), K(task_ctx));
          }
        }
        if (OB_FAIL(ret) || !inc_new_task) { // release memory when fail
          if (OB_NOT_NULL(task_ctx)) {
            task_ctx->~ObVecIndexAsyncTaskCtx();
            allocator->free(task_ctx); // arena need free
            task_ctx = nullptr;
          }
        }
      }
    }
    LOG_INFO("finish load async task", K(ret), K(ls_->get_ls_id()), K(task_ctx_array.count()), K(current_task_cnt));
    vector_allocator.pre_free(total_pre_alloc_size);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(insert_new_task(task_ctx_array))) {
    LOG_WARN("fail to insert new tasks", K(ret), K(tenant_id_), K(ls_->get_ls_id()));
  }
  // clear on fail
  if (OB_FAIL(ret) && !task_ctx_array.empty()) {
    if (OB_FAIL(clear_task_ctxs(index_ls_mgr->get_async_task_opt(), task_ctx_array))) {
      LOG_WARN("fail to clear task ctx", K(ret));
    }
  }
  return ret;
}

int ObVecIdxFreezeTaskExecutor::calc_freeze_threshold(
    ObTenantVectorAllocator& vector_allocator,
    const int64_t inc_mem_size,
    int64_t &total_pre_alloc_size,
    int64_t &freeze_threshold)
{
  int ret = OB_SUCCESS;
  const int64_t left_size = vector_allocator.get_left_pre_alloc_size();
  if (inc_mem_size >= left_size) {
    freeze_threshold = inc_mem_size;
  } else {
    freeze_threshold = max_active_segment_size_;
  }
  if (OB_FAIL(vector_allocator.pre_alloc(inc_mem_size))) {
    LOG_TRACE("fail to pre alloc", K(ret), K(inc_mem_size));
  } else {
    total_pre_alloc_size += inc_mem_size;
    LOG_TRACE("calc freeze threshold success", K(ret), K(inc_mem_size), K(freeze_threshold), K(left_size), K(total_pre_alloc_size));
  }
  return ret;
}

int ObVecIdxFreezeTask::do_work()
{
  int ret = OB_SUCCESS;
  ObTraceIdGuard trace_guard(ctx_->task_status_.trace_id_);
  LOG_TRACE("[VECTOR INDEX FREEZE] start freeze task", K(ret), K(ctx_->task_status_.tablet_id_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVecIndexAsyncTask is not init", KR(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->ls_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected nullptr", K(ret), KP(ctx_));
  } else if (OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (ctx_->task_status_.task_type_ == OB_VECTOR_ASYNC_INDEX_FREEZE) {
    if (OB_FAIL(process_freeze())) {
      LOG_WARN("fail to process freeze", K(ret), KPC(ctx_));
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
  LOG_TRACE("[VECTOR INDEX FREEZE] end freeze task", K(ret), K(ctx_->task_status_.tablet_id_));
  return ret;
}

int ObVecIdxFreezeTask::process_freeze()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdapterGuard adpt_guard;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  LOG_TRACE("[VECTOR INDEX FREEZE] start process_freeze", K(ret), K(ctx_->task_status_), K(ls_id_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVecIndexAsyncTask is not init", KR(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (OB_FAIL(vec_idx_mgr_->get_adapter_inst_guard(ctx_->task_status_.tablet_id_, adpt_guard))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
      LOG_INFO("can not get adapter, need wait", K(ret), KPC(ctx_));
    } else {
      LOG_WARN("fail to get adapter instance", KR(ret), KPC(ctx_));
    }
  } else if (OB_ISNULL(adpt_guard.get_adatper())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get vector index adapter", KR(ret), KPC(ctx_));
  } else if (!adpt_guard.get_adatper()->is_complete()) {
    ret = OB_EAGAIN;
    LOG_INFO("adapter not complete, need wait", K(ret), KP(adpt_guard.get_adatper()));
  } else if (adpt_guard.get_adatper()->get_create_type() != CreateTypeComplete) {
    ret = OB_EAGAIN;
    LOG_INFO("adapter is not complete, no need freeze", K(ret), KPC(adpt_guard.get_adatper()));
  } else if (!check_snapshot_table_available(*adpt_guard.get_adatper())) {
    ret = OB_EAGAIN; // will retry
    LOG_INFO("skip to do async task due to snapshot table not available", KR(ret), KPC(ctx_));
  } else if (FALSE_IT(set_old_adapter(adpt_guard.get_adatper()))) {
  } else if (OB_FAIL(check_and_freeze(adpt_guard, ls_id_))) {
    LOG_WARN("[VECTOR INDEX FREEZE] fail to check and freeze", K(ret), KP(adpt_guard.get_adatper()));
  } else {
    LOG_TRACE("[VECTOR INDEX FREEZE] check freeze success", K(ret), KP(adpt_guard.get_adatper()));
  }

  if (OB_NOT_NULL(ctx_)) {
    common::ObSpinLockGuard ctx_guard(ctx_->lock_);
    ctx_->task_status_.ret_code_ = ret;
  }
  LOG_TRACE("[VECTOR INDEX FREEZE] end process_freeze", K(ret), K(ctx_->task_status_));
  return ret;
}

int ObVecIdxFreezeTask::check_and_freeze(ObPluginVectorIndexAdapterGuard &adpt_guard, const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdaptor* adaptor = adpt_guard.get_adatper();
  bool can_freeze = false;
  ObVecIdxFrozenDataHandle &frozen_data = adaptor->get_frozen_data();
  if (ObVecIdxFrozenData::State::NO_FROZEN != frozen_data->state_) {
    can_freeze = true;
  } else if (OB_FAIL(adaptor->check_can_freeze(ls_id, 0 /* allways in execution */, can_freeze))) {
    LOG_WARN("failed to check need freeze", K(ret));
  } else if (! can_freeze) {
    ret = OB_EAGAIN;
    LOG_INFO("active segment actually can not freeze in task execution", K(ret), KPC(adaptor));
  } else if (OB_FAIL(adaptor->freeze_active_segment(ls_id))) {
    LOG_WARN("failed to freeze active segment", K(ret));
  }

  if (OB_SUCC(ret) && can_freeze) {
    ObArenaAllocator tmp_allocator("VecIdxFrez", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    const share::SCN &frozen_scn = frozen_data->frozen_scn_;
    while (OB_SUCC(ret) &&
        (ObVecIdxFrozenData::State::NO_FROZEN != frozen_data->state_ && ObVecIdxFrozenData::State::FINISH != frozen_data->state_)) {
      switch (frozen_data->state_) {
      case ObVecIdxFrozenData::State::FROZEN: {
        if (OB_FAIL(check_and_wait_write(frozen_data))) {
          LOG_WARN("check and wait write finished fail", K(ret));
        } else {
          frozen_data->state_ = ObVecIdxFrozenData::State::WAIT_WRITE_FINISHED;
        }
        break;
      }
      case ObVecIdxFrozenData::State::WAIT_WRITE_FINISHED: {
        if (! adaptor->is_hybrid_index() && OB_FAIL(ObVectorIndexRefresher::tablet_fast_refresh(adaptor, frozen_scn))) {
          if (OB_INVALID_QUERY_TIMESTAMP == ret || OB_TABLE_DEFINITION_CHANGED == ret) {
            LOG_INFO("snapshot version is too old, refresh to latest", K(ret), KP(adaptor), K(frozen_scn));
            SCN new_refresh_scn;
            if (OB_FAIL(get_current_scn(new_refresh_scn))) {
              LOG_WARN("fail to get scn", KR(ret));
            } else if (OB_FAIL(ObVectorIndexRefresher::tablet_fast_refresh(adaptor, new_refresh_scn))) {
              LOG_WARN("tablet_fast_refresh fail again", K(ret), K(new_refresh_scn), K(frozen_scn));
            } else {
              frozen_data->state_ = ObVecIdxFrozenData::State::REFRESH_VBITMAP;
              LOG_INFO("tablet fast refresh success after use current scn", KP(adaptor), K(new_refresh_scn));
            }
          } else {
            LOG_WARN("tablet_fast_refresh fail", K(ret), KP(adaptor), K(frozen_scn));
          }
        } else {
          frozen_data->state_ = ObVecIdxFrozenData::State::REFRESH_VBITMAP;
        }
        break;
      }
      case ObVecIdxFrozenData::State::REFRESH_VBITMAP: {
        if (OB_FAIL(adaptor->update_vbitmap_memdata(ls_id, frozen_scn, tmp_allocator))) {
          LOG_WARN("update_vbitmap_memdata fail", K(ret));
        } else {
          frozen_data->state_ = ObVecIdxFrozenData::State::PERSIST;
        }
        break;
      }
      case ObVecIdxFrozenData::State::PERSIST: {
        if (OB_FAIL(adaptor->persist_incr_segment(ls_id))) {
          LOG_WARN("persist frozen segment fail", K(ret), K(ls_id));
        } else {
          frozen_data->state_ = ObVecIdxFrozenData::State::REFRESH_SNAP;
        }
        break;
      }
      case ObVecIdxFrozenData::State::REFRESH_SNAP: {
        if (OB_FAIL(refresh_adaptor(adpt_guard, tmp_allocator, ls_id))) {
          LOG_WARN("replace new meta fail", K(ret));
        } else {
          frozen_data->set_frozen_finish();
          LOG_INFO("active segment freeze and persist success");
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid state", K(ret), K(frozen_data->state_));
        break;
      }
      }
    }
    if (OB_FAIL(ret)) {
      frozen_data->ret_code_ = ret;
      frozen_data->retry_cnt_++;
      if (frozen_data->retry_too_many()) {
        // ignore ret
        LOG_ERROR("NOTICE!!!: frozen and persist vector index fail too many times", KPC(adaptor));
      }
    }
  }
  return ret;
}

int ObVecIdxFreezeTask::check_and_wait_write(const ObVecIdxFrozenDataHandle &frozen_data)
{
  int ret = OB_SUCCESS;
  const static int64_t WAIT_US = 100 * 1000; // 100ms
  const static int64_t MAX_WAIT_CNT = 100;
  int64_t check_cnt = 0;
  while(check_cnt < MAX_WAIT_CNT && frozen_data->segment_handle_->get_write_ref() > 0) {
    LOG_INFO("there are some write thread still run, so need wait", K(frozen_data), K(check_cnt));
    ob_usleep(WAIT_US);
    ++check_cnt;
  }
  if (check_cnt >= MAX_WAIT_CNT || frozen_data->segment_handle_->get_write_ref() > 0) {
    ret = OB_EAGAIN;
    LOG_WARN("reach max wait cnt, still has write thread", K(ret), K(check_cnt), K(frozen_data));
  }
  return ret;
}

int ObVecIdxFreezeTask::refresh_adaptor(ObPluginVectorIndexAdapterGuard &adpt_guard, ObIAllocator &allocator, const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  share::SCN current_scn;
  schema::ObTableParam snap_table_param(allocator);
  storage::ObTableScanParam snap_scan_param;
  common::ObNewRowIterator *snap_data_iter = nullptr;
  ObTableScanIterator *table_scan_iter = nullptr;
  blocksstable::ObDatumRow *datum_row = nullptr;
  ObPluginVectorIndexAdaptor* new_adapter = nullptr;
  if (OB_FAIL(create_new_adapter(vector_index_service, adpt_guard, new_adapter))) {
    LOG_WARN("fail to get or create new adapter", K(ret), K(ctx_));
  } else if (OB_FAIL(get_current_scn(current_scn))) {
    LOG_WARN("fail to get scn", KR(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(const_cast<ObLSID&>(ls_id),
                                new_adapter,
                                current_scn,
                                INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                allocator,
                                allocator,
                                snap_scan_param,
                                snap_table_param,
                                snap_data_iter))) {
    LOG_WARN("read_local_tablet fail", K(ret));
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan iter is null", K(ret), KP(snap_data_iter));
  } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed.", K(ret));
    } else {
      LOG_INFO("there is no vector index meta row");
    }
  } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row invalid.", K(ret));
  } else if (datum_row->get_column_count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
  } else if (! datum_row->storage_datums_[0].get_string().suffix_match("_meta_data")) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first row of vector index is not meta row", K(ret), KPC(datum_row));
  } else {
    ObString meta_data = datum_row->storage_datums_[1].get_string();
    int64_t meta_scn = 0;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator, ObLongTextType, true, meta_data, nullptr))) {
      LOG_WARN("read real string data fail", K(ret));
    } else if (OB_FAIL(ObVectorIndexMeta::get_meta_scn(meta_data, meta_scn))) {
      LOG_WARN("get meta scn fail", K(ret));
    } else if (OB_FAIL(new_adapter->deserialize_snap_meta(meta_data))) {
      LOG_WARN("deserialize snap meta fail", K(ret), K(meta_scn), K(meta_data.length()));
    } else if (OB_FAIL(try_reuse_segments_from_old_adapter())) {
      LOG_WARN("try reuse segments from old adapter fail", K(ret));
    } else if (OB_FAIL(new_adapter->get_snap_data()->load_persist_segments(new_adapter, allocator_, ls_id_, current_scn))) {
      LOG_WARN("load persist segments fail", K(ret));
    } else if (OB_FALSE_IT(new_adapter->get_snap_data()->set_inited())) {
    } else if (OB_FALSE_IT(new_adapter->close_snap_data_rb_flag())) {
    } else {
      LOG_INFO("build new adaptor success", KPC(new_adapter), KPC(adpt_guard.get_adatper()));
    }
  }

  if (OB_SUCC(ret)) {
    ObPluginVectorIndexAdapterGuard latest_adpt_guard;
    RWLock::WLockGuard lock_guard(vec_idx_mgr_->get_adapter_map_lock());
    // TODO handle snapshot scn is old
    if (OB_FAIL(vec_idx_mgr_->get_adapter_inst_guard_in_lock(ctx_->task_status_.tablet_id_, latest_adpt_guard))) {
      LOG_WARN("fail to get adapter instance", K(ret), KPC(ctx_));
    } else if (OB_FAIL(new_adapter->merge_incr_data(latest_adpt_guard.get_adatper(), false))){
      LOG_WARN("partial vector index adapter not valid", K(ret), KP(latest_adpt_guard.get_adatper()), KPC(this));
    } else if (OB_FAIL(new_adapter->set_replace_scn(current_scn))) {
      LOG_WARN("failed to set replace scn", K(ret), K(current_scn));
    } else if (OB_FAIL(vec_idx_mgr_->replace_old_adapter(new_adapter))) {
      LOG_WARN("failed to replace old adapter", K(ret));
    } else {
      LOG_INFO("relpace old adaptor success", KP(new_adapter), KP(adpt_guard.get_adatper()), KP(latest_adpt_guard.get_adatper()));
    }
  }

  // clean tmp info
  {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(vector_index_service->release_vector_index_tmp_info(ctx_->task_status_.task_id_))) {
      LOG_WARN("fail to release vector index tmp info", K(ret));
    }
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(new_adapter)) {
    LOG_INFO("release new adapter memory in failure", K(ret));
    new_adapter->~ObPluginVectorIndexAdaptor();
    vector_index_service->get_adaptor_allocator().free(new_adapter);
    new_adapter = nullptr;
  }

  if (OB_NOT_NULL(snap_data_iter)) {
    int tmp_ret = MTL(ObAccessService*)->revert_scan_iter(snap_data_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert snap_data_iter failed", K(ret));
    }
    snap_data_iter = nullptr;
  }

  if (OB_SUCC(ret)) {
    // just trigger, so don't afftect return
    int tmp_ret = OB_SUCCESS;
    ObSEArray<uint64_t, 2> tablet_ids;
    if (OB_TMP_FAIL(tablet_ids.push_back(new_adapter->get_inc_tablet_id().id()))) {
      LOG_WARN("failed to store tablet id", K(tmp_ret));
    } else if (OB_TMP_FAIL(tablet_ids.push_back(new_adapter->get_vbitmap_tablet_id().id()))) {
      LOG_WARN("failed to store tablet id", K(tmp_ret));
    } else if (OB_TMP_FAIL(ObVectorIndexRefresher::trigger_major_freeze(tenant_id_, tablet_ids))) {
      LOG_WARN("failed to trigger major freeze", K(tmp_ret));
    }
  }

  return ret;
}

}
}
