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
#include "ob_vector_index_merge_task.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_vector_index_aux_table_handler.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/allocator/ob_tenant_vector_allocator.h"
#include "share/ob_ddl_common.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
namespace share
{

bool ObVecIdxMergeTaskExecutor::check_operation_allow()
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
  } else if (OB_FAIL(ObPluginVectorIndexHelper::get_merge_base_percentage(tenant_id_, merge_base_percentage_))) {
    LOG_WARN("failed to get merge base percentage", K(ret), K(tenant_id_));
  }
  return bret;
}

int ObVecIdxMergeTaskExecutor::load_task(uint64_t &task_trace_base_num)
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
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    ObIAllocator *allocator = task_opt.get_allocator();
    const int64_t current_task_cnt = ObVecIndexAsyncTaskUtil::get_processing_task_cnt(task_opt);

    RWLock::RLockGuard lock_guard(index_ls_mgr->get_adapter_map_lock());
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
        bool need_merge = false;
        if (! adapter->is_complete()) {
          LOG_TRACE("adapter not complete, no need merge", K(ret), KPC(adapter));
        } else if (adapter->get_create_type() != CreateTypeComplete) {
          LOG_TRACE("adapter is not complete, no need merge", K(ret), KPC(adapter));
        } else if (adapter->is_hybrid_index()) {
          LOG_TRACE("adapter is not hybrid index, no need merge", K(ret), KPC(adapter));
        } else if (adapter->is_sparse_vector_index_type()) {
          LOG_TRACE("adapter is sparse vector index, no need merge", K(ret), KPC(adapter));
        } else if (OB_FAIL(adapter->check_need_merge(merge_base_percentage_, need_merge))) {
          LOG_WARN("failed to check need merge", K(ret), KPC(adapter));
        } else if (! need_merge) {
          LOG_TRACE("vector index is not need to merge", K(ret), KPC(adapter));
        } else if (OB_ISNULL(task_ctx_buf = static_cast<char *>(allocator->alloc(sizeof(ObVecIndexAsyncTaskCtx))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("async task ctx is null", K(ret));
        } else if (FALSE_IT(task_ctx = new(task_ctx_buf) ObVecIndexAsyncTaskCtx())) {
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(tenant_id_, new_task_id))) {
          LOG_WARN("fail to fetch new task id", K(ret), K(tenant_id_));
        } else if (! adapter->is_vbitmap_tablet_valid() || tablet_id != adapter->get_vbitmap_tablet_id()) {
          LOG_DEBUG("index table id is invalid, skip", K(ret)); // skip to next
        } else if (OB_FALSE_IT(index_table_id = adapter->get_vbitmap_table_id())) {
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
          task_ctx->task_status_.task_type_ = ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_MERGE;
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

int ObVecIdxMergeTask::do_work()
{
  int ret = OB_SUCCESS;
  ObTraceIdGuard trace_guard(ctx_->task_status_.trace_id_);
  LOG_TRACE("[VECTOR INDEX MERGE] start merge task", K(ret), K(ctx_->task_status_.tablet_id_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVecIdxMergeTask is not init", KR(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->ls_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected nullptr", K(ret), KP(ctx_));
  } else if (OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (ctx_->task_status_.task_type_ == OB_VECTOR_ASYNC_INDEX_MERGE) {
    if (OB_FAIL(process_merge())) {
      LOG_WARN("fail to process merge", K(ret), KPC(ctx_));
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
  LOG_TRACE("[VECTOR INDEX MERGE] end merge task", K(ret), K(ctx_->task_status_.tablet_id_));
  return ret;
}

struct ObVIMergeSortItem {
  ObVIMergeSortItem():
    vid_bound_(), vec_cnt_(0), mem_size_(0), scn_(0), seg_meta_(nullptr), has_quantization_(false)
  {}

  TO_STRING_KV(K_(vid_bound), K_(vec_cnt), K_(mem_size), K_(scn), KP_(seg_meta), K_(has_quantization))

  ObVidBound vid_bound_;
  int64_t vec_cnt_;
  int64_t mem_size_;
  int64_t scn_;
  const ObVectorIndexSegmentMeta *seg_meta_;
  bool has_quantization_;  // whether this segment has quantization
};

struct ObVIMergeSortItemCompartor
{
  bool operator()(const ObVIMergeSortItem &lhs, const ObVIMergeSortItem &rhs) const
  {
    bool res = false;
    if (lhs.vid_bound_.min_vid_ < rhs.vid_bound_.min_vid_) {
      res = true;
    } else if (lhs.vid_bound_.min_vid_ > rhs.vid_bound_.min_vid_) {
      res = false;
    } else {
      res = lhs.scn_ < rhs.scn_;
    }
    return res;
  }
};

int extract_segment_scn(const ObVectorIndexSegmentMeta &seg_meta, ObString &scn_str)
{
  int ret = OB_SUCCESS;
  ObString key = seg_meta.start_key_;
  const ObString tablet_id_str = key.split_on('_');
  scn_str = key.split_on('_');
  if (scn_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scn is empty", K(ret), K(seg_meta));
  }
  return ret;
}

int ObVecIdxMergeTask::prepare_merge_segment(const ObVecIdxSnapshotDataHandle& old_snap_data)
{
  int ret = OB_SUCCESS;
  ObSharedMemAllocMgr *shared_mem_mgr = MTL(ObSharedMemAllocMgr*);
  ObTenantVectorAllocator& vector_allocator = shared_mem_mgr->vector_allocator();
  const int64_t base_count = old_snap_data->meta_.bases_.count();
  const int64_t incr_count = old_snap_data->meta_.incrs_.count();
  int64_t total_incr_mem = 0;
  int64_t total_base_mem = 0;
  int64_t total_incr_vec_cnt = 0;
  int64_t total_base_vec_cnt = 0;
  bool start_from_base = false;
  int64_t merge_start_idx = -1;
  int64_t merge_end_idx = -1;
  int64_t merge_seg_cnt = 0;
  int64_t merge_mem = 0;
  int64_t merge_vec_cnt = 0;
  int64_t avail_merge_mem = vector_allocator.get_left_pre_alloc_size();
  ObSEArray<ObVIMergeSortItem, 10> items;
  for (int64_t i = 0; OB_SUCC(ret) && i < incr_count; ++i) {
    ObVIMergeSortItem item;
    const ObVectorIndexSegmentMeta &seg_meta = old_snap_data->meta_.incrs_.at(i);
    item.seg_meta_ = &seg_meta;
    item.has_quantization_ = seg_meta.has_quantization();
    item.scn_ = seg_meta.scn_;
    if (! seg_meta.segment_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("segment is invalid", K(ret), K(i), K(seg_meta), K(old_snap_data));
    } else if (OB_FAIL(seg_meta.segment_handle_->get_vid_bound(item.vid_bound_.min_vid_, item.vid_bound_.max_vid_))) {
      LOG_WARN("get vid bound fail", K(ret), K(seg_meta));
    } else if (OB_FAIL(seg_meta.segment_handle_->get_index_number(item.vec_cnt_))) {
      LOG_WARN("get index number fail", K(ret), K(seg_meta));
    } else if (OB_FALSE_IT(item.mem_size_ = seg_meta.segment_handle_->get_mem_hold())) {
    } else if (OB_FAIL(items.push_back(item))) {
      LOG_WARN("push back item fail", K(ret), K(item), K(seg_meta));
    } else {
      total_incr_mem += item.mem_size_;
      total_incr_vec_cnt += item.vec_cnt_;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < base_count; ++i) {
    const ObVectorIndexSegmentMeta &seg_meta = old_snap_data->meta_.bases_.at(i);
    int64_t vec_cnt = 0;
    if (! seg_meta.segment_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("segment is invalid", K(ret), K(i), K(seg_meta), K(old_snap_data));
    } else if (OB_FAIL(seg_meta.segment_handle_->get_index_number(vec_cnt))) {
      LOG_WARN("get index number fail", K(ret), K(seg_meta));
    } else {
      total_base_mem += seg_meta.segment_handle_->get_mem_hold();
      total_base_vec_cnt += vec_cnt;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (incr_count != items.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incr count is not equal to items count", K(ret), K(incr_count), K(items.count()));
  } else if (incr_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not incr segment", K(ret), K(old_snap_data), K(incr_count));
  } else if (1 == incr_count) {
    // merge base + incr when only one incr segment
    const ObVectorIndexSegmentMeta &seg_meta = old_snap_data->meta_.incrs_.at(0);
    const ObVectorIndexSegmentHandle &handle = seg_meta.segment_handle_;
    if (! handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("segment handle is invalid", K(ret), K(seg_meta));
    } else if (old_snap_data->check_incr_can_merge_base()) {
      merge_from_base_ = true;
      LOG_INFO("incr bitmap is complete, merge from base", K(ret), K(old_snap_data));
    } else {
      ret = OB_EAGAIN;
      LOG_WARN("incr bitmap is not complete, need to wait", K(ret), K(old_snap_data));
    }
    if (OB_SUCC(ret)) {
      merge_start_idx = 0;
      merge_end_idx = 1;
      merge_seg_cnt = base_count + 1;
      merge_vec_cnt = total_base_vec_cnt + total_incr_vec_cnt;
      merge_mem = total_base_mem + total_incr_mem;
      start_from_base = true;
    }
    LOG_INFO("merge one incr segment", K(ret), K(incr_count), K(base_count), K(merge_seg_cnt),
        K(merge_vec_cnt), K(total_base_vec_cnt), K(total_incr_vec_cnt),
        K(merge_mem), K(total_base_mem), K(total_incr_mem),
        K(start_from_base), K(merge_start_idx), K(merge_end_idx));
  } else {
    // merge all incr when multiple incr segments
    lib::ob_sort(items.begin(), items.end(), ObVIMergeSortItemCompartor());
    LOG_INFO("sort items", K(items.count()), K(base_count), K(incr_count));
    merge_start_idx = 0;
    merge_end_idx = incr_count;
    merge_seg_cnt = incr_count;
    merge_vec_cnt = total_incr_vec_cnt;
    merge_mem = total_incr_mem;
    LOG_INFO("merge one incr segment", K(incr_count), K(base_count), K(merge_seg_cnt),
        K(merge_vec_cnt), K(total_base_vec_cnt), K(total_incr_vec_cnt),
        K(merge_mem), K(total_base_mem), K(total_incr_mem),
        K(start_from_base), K(merge_start_idx), K(merge_end_idx));
  }
  LOG_INFO("prepare merge info",
      K(ret), K(avail_merge_mem), K(incr_count), K(base_count), K(merge_seg_cnt),
      K(merge_vec_cnt), K(total_base_vec_cnt), K(total_incr_vec_cnt),
      K(merge_mem), K(total_base_mem), K(total_incr_mem),
      K(start_from_base), K(merge_start_idx), K(merge_end_idx));

  if (OB_FAIL(ret)) {
  } else if (merge_start_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find merge starting point", K(ret), K(items.count()), K(merge_start_idx));
  } else if (merge_seg_cnt < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no enough segmetn for merge", K(ret), K(items.count()), K(merge_seg_cnt));
  } else if (OB_FAIL(vector_allocator.pre_alloc(merge_mem))) {
    LOG_WARN("fail to pre alloc", K(ret), K(merge_mem));
  } else if (OB_FALSE_IT(pre_alloc_size_ = merge_mem)) {
  } else {
    if (start_from_base && merge_start_idx != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not merge from incr start", K(ret), K(start_from_base), K(merge_start_idx));
    } else if (start_from_base) {
      merge_from_base_ = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < base_count; ++i) {
        const ObVectorIndexSegmentMeta &seg_meta = old_snap_data->meta_.bases_.at(i);
        if (OB_FAIL(merge_segments_.push_back(&seg_meta))) {
          LOG_WARN("push segment fail", K(ret), K(i), K(seg_meta));
        }
      }
    }
    for (int64_t i = merge_start_idx; OB_SUCC(ret) && i < merge_end_idx; ++i) {
      const ObVIMergeSortItem &cur_item = items.at(i);
      if (OB_FAIL(merge_segments_.push_back(cur_item.seg_meta_))) {
        LOG_WARN("push segment fail", K(ret), K(i), K(cur_item));
      }
    }
    if (OB_SUCC(ret) && 0 == base_count && ! merge_from_base_) {
      roaring::api::roaring64_bitmap_t *merged_vbitmap = nullptr;
      roaring::api::roaring64_bitmap_t *merged_ibitmap = nullptr;
      {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIMergeBmap"));
        ROARING_TRY_CATCH(merged_vbitmap = roaring::api::roaring64_bitmap_create());
        ROARING_TRY_CATCH(merged_ibitmap = roaring::api::roaring64_bitmap_create());
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(merged_vbitmap) || OB_ISNULL(merged_ibitmap)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create merged bitmap", K(ret), K(tenant_id_), KP(merged_vbitmap), KP(merged_ibitmap));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < merge_segments_.count(); ++i) {
          const ObVectorIndexSegmentMeta *seg_meta = merge_segments_.at(i);
          const ObVectorIndexSegmentHandle &handle = seg_meta->segment_handle_;
          if (! handle.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("segment handle is invalid", K(ret), K(i), KPC(seg_meta));
          } else if (OB_NOT_NULL(handle->vbitmap_)) {
            if (OB_NOT_NULL(handle->vbitmap_->insert_bitmap_)) {
              ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(merged_vbitmap, handle->vbitmap_->insert_bitmap_));
            }
          }
          if (OB_SUCC(ret) && OB_NOT_NULL(handle->ibitmap_)) {
            if (OB_NOT_NULL(handle->ibitmap_->insert_bitmap_)) {
              ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(merged_ibitmap, handle->ibitmap_->insert_bitmap_));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        // delete semantic is already applied to ibitmap.insert_bitmap_,
        // so diff only needs to compare merged insert sets.
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIMergeBmap"));
        uint64_t diff_cnt = roaring64_bitmap_xor_cardinality(merged_ibitmap, merged_vbitmap);
        if (0 == diff_cnt) {
          merge_from_base_ = true;
          LOG_INFO("diff is empty, merge from base", K(ret), K(diff_cnt));
        } else {
          LOG_INFO("diff is not empty, merge from incr", K(ret), K(diff_cnt));
        }
      }

      if (OB_NOT_NULL(merged_vbitmap)) {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIMergeBmap"));
        roaring64_bitmap_free(merged_vbitmap);
        merged_vbitmap = nullptr;
      }
      if (OB_NOT_NULL(merged_ibitmap)) {
        lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIMergeBmap"));
        roaring64_bitmap_free(merged_ibitmap);
        merged_ibitmap = nullptr;
      }
    }
  }
  LOG_INFO("merge segments prepared", K(ret), K(avail_merge_mem), K(merge_from_base_), K(merge_segments_.count()), K(merge_segments_));
  return ret;
}

int ObVecIdxMergeTask::upadte_task_merge_segments_info() {
  int ret = OB_SUCCESS;
  ctx_->task_status_.task_info_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_segments_.count(); ++i) {
    ObVecIndexTaskSegInfo seg_info;
    if (OB_FAIL(ObVecIndexAsyncTaskUtil::seg_meta_to_task_seg_info(*merge_segments_.at(i), ctx_->allocator_, seg_info))) {
      LOG_WARN("seg_meta_to_task_seg_info fail", K(ret), K(i), KPC(merge_segments_.at(i)));
    } else if (OB_FAIL(ctx_->task_status_.task_info_.merge_segs_.push_back(seg_info))) {
      LOG_WARN("push_back merge_segments fail", K(ret), K(seg_info));
    }
  }
  return ret;
}

int ObVecIdxMergeTask::upadte_task_result_segments_info(const ObVectorIndexSegmentMeta *new_meta) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new_meta is null", K(ret));
  } else {
    const ObVecIdxSnapshotDataHandle& new_snap = new_adapter_->get_snap_data();
    if (new_snap->meta_.bases_.count() == 1) {
      ObVecIndexTaskSegInfo res_seg;
      if (OB_FAIL(ObVecIndexAsyncTaskUtil::seg_meta_to_task_seg_info(*new_meta, ctx_->allocator_, res_seg))) {
        LOG_WARN("seg_meta_to_task_seg_info res_seg fail", K(ret), K(new_snap));
      }
      if (OB_SUCC(ret) && res_seg.vector_cnt_ < 0) {
        // use last_res_seg_info_ from build_finished() when segment_handle_ may be invalid in same build_finished()
        res_seg.vector_cnt_ = new_snap->last_res_seg_info_.vector_cnt_;
        res_seg.mem_used_ = new_snap->last_res_seg_info_.mem_used_;
        res_seg.min_vid_ = new_snap->last_res_seg_info_.min_vid_;
        res_seg.max_vid_ = new_snap->last_res_seg_info_.max_vid_;
        if (OB_FAIL(ctx_->task_status_.task_info_.res_segs_.push_back(res_seg))) {
          LOG_WARN("push_back result_segments fail", K(ret), K(res_seg));
        }
      }
    }
  }
  return ret;
}

int ObVecIdxMergeTask::build_filter_clause(const ObTableSchema &data_table_schema, const ObTableSchema &snapshot_table_schema)
{
  int ret = OB_SUCCESS;
  ObString vid_col_name;
  int64_t pos = 0;
  ObSEArray<uint64_t, 4> all_column_ids;
  if (merge_segments_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_segments is empty", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_segments_.count(); ++i) {
    const ObVectorIndexSegmentMeta *seg_meta = merge_segments_.at(i);
    int64_t seg_min_vid = INT64_MAX;
    int64_t seg_max_vid = 0;
    if (OB_FAIL(seg_meta->segment_handle_->get_vid_bound(seg_min_vid, seg_max_vid))) {
      LOG_WARN("get_vid_bound fail", K(ret), K(i), K(seg_meta));
    } else {
      min_vid_ = OB_MIN(min_vid_, seg_min_vid);
      max_vid_ = OB_MAX(max_vid_, seg_max_vid);
    }
  }
  if (min_vid_ >= max_vid_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vid bound is invalid", K(ret), K(min_vid_), K(max_vid_));
  } else if (OB_FAIL(snapshot_table_schema.get_all_column_ids(all_column_ids))) {
    LOG_WARN("fail to get all column ids", K(ret), K(data_table_schema));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < all_column_ids.count(); i++) {
    const ObColumnSchemaV2 *column_schema;
    if (OB_ISNULL(column_schema = data_table_schema.get_column_schema(all_column_ids.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get column schema", K(ret), K(all_column_ids.at(i)));
    } else if (column_schema->is_vec_hnsw_vid_column()) {
      vid_col_name = column_schema->get_column_name_str();
      LOG_INFO("[VECTOR INDEX MERGE] vid column name", K(vid_col_name));
      break;
    } else if (column_schema->is_hidden_pk_column_id(all_column_ids.at(i))) {
      vid_col_name = column_schema->get_column_name_str();
      LOG_INFO("[VECTOR INDEX MERGE] vid column name", K(vid_col_name));
      break;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (merge_from_base_) {
    int64_t real_min_vid = min_vid_;
    min_vid_ = 0;
    if (OB_FAIL(filter_sql_str_.assign_fmt("where %.*s <= %ld ",
      static_cast<int>(vid_col_name.length()), vid_col_name.ptr(), max_vid_))) {
      LOG_WARN("append fmt fail", K(ret), K(vid_col_name));
    } else {
      LOG_INFO("build_filter_clause from base success", K(vid_col_name), K(min_vid_), K(max_vid_), K(real_min_vid), K(filter_sql_str_));
    }
  } else if (OB_FAIL(filter_sql_str_.assign_fmt("where %.*s >= %ld and %.*s <= %ld ",
      static_cast<int>(vid_col_name.length()), vid_col_name.ptr(), min_vid_,
      static_cast<int>(vid_col_name.length()), vid_col_name.ptr(), max_vid_))) {
    LOG_WARN("append fmt fail", K(ret), K(vid_col_name));
  } else {
    LOG_INFO("build_filter_clause success", K(vid_col_name), K(min_vid_), K(max_vid_), K(filter_sql_str_));
  }

  if (OB_SUCC(ret) && ! data_table_schema.is_heap_organized_table()) {
    filter_sql_str_.reset();
    LOG_INFO("table is not heap_organized_table, so reset filter", K(data_table_schema));
  }
  return ret;
}

int ObVecIdxMergeTask::merge_bitmap(ObPluginVectorIndexAdaptor *adaptor)
{
  int ret = OB_SUCCESS;
  ObVectorIndexSegmentBuilder* segment_builder = nullptr;
  ObVectorIndexRoaringBitMap *ibitmap = nullptr;
  ObVectorIndexRoaringBitMap *vbitmap = nullptr;
  if (OB_ISNULL(adaptor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor is null", K(ret));
  } else if (merge_segments_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_segments is empty", K(ret));
  } else if (OB_FAIL(adaptor->init_snap_data_for_build(! merge_from_base_))) {
    LOG_WARN("init segment builder fail", K(ret), KPC(adaptor));
  } else if (OB_ISNULL(segment_builder = adaptor->get_snap_data()->builder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("segment_builder is null", K(ret));
  } else if (min_vid_ >= max_vid_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vid bound is invalid", K(ret), K(min_vid_), K(max_vid_));
  } else if (OB_FALSE_IT(segment_builder->vid_bound_.set_vid_bound(max_vid_, min_vid_))) {
  } else if (OB_FALSE_IT(segment_builder->need_vid_check_ = true)) {
  } else if (merge_from_base_) {
    LOG_INFO("is merge from base, so no need to merge bitmap", KPC(segment_builder));
  } else if (OB_FALSE_IT(segment_builder->seg_type_ = ObVectorIndexSegmentType::INCR_MERGE)) {
  } else if (OB_ISNULL(ibitmap = segment_builder->ibitmap_) || OB_ISNULL(vbitmap = segment_builder->vbitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ibitmap or vbitmap is null", K(ret), KP(ibitmap), KP(vbitmap));
  } else if (OB_FAIL(ibitmap->init(true, false))) {
    LOG_WARN("init ibitmap fail", K(ret));
  } else if (OB_FAIL(vbitmap->init(true, true))) {
    LOG_WARN("init vbitmap fail", K(ret));
  } else {
    int64_t merge_vbitmap_insert = 0;
    int64_t merge_vbitmap_delete = 0;
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmap"));
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_segments_.count(); ++i) {
      const ObVectorIndexSegmentMeta *seg_meta = merge_segments_.at(i);
      const ObVectorIndexSegmentHandle &handle = seg_meta->segment_handle_;
      if (! handle.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("segment handle is invalid", K(ret), K(i), KPC(seg_meta));
      } else if (OB_SUCC(ret) && OB_NOT_NULL(handle->vbitmap_)) {
        if (OB_NOT_NULL(handle->vbitmap_->insert_bitmap_)) {
          ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(vbitmap->insert_bitmap_, handle->vbitmap_->insert_bitmap_));
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(handle->vbitmap_->delete_bitmap_)) {
          ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(vbitmap->delete_bitmap_, handle->vbitmap_->delete_bitmap_));
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(vbitmap->insert_bitmap_)) {
      if (OB_NOT_NULL(vbitmap->delete_bitmap_)) {
        ROARING_TRY_CATCH(roaring64_bitmap_andnot_inplace(vbitmap->insert_bitmap_, vbitmap->delete_bitmap_));
      }
      if (OB_SUCC(ret)) {
        merge_vbitmap_insert = roaring64_bitmap_get_cardinality(vbitmap->insert_bitmap_);
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(vbitmap->delete_bitmap_)) {
      merge_vbitmap_delete = roaring64_bitmap_get_cardinality(vbitmap->delete_bitmap_);
    }
    LOG_INFO("init segment builder sucess", K(ret),KPC(segment_builder), K(merge_vbitmap_insert), K(merge_vbitmap_delete));
  }
  return ret;
}

int ObVecIdxMergeTask::process_merge()
{
  int ret = OB_SUCCESS;
  bool need_merge = false;
  ObPluginVectorIndexAdapterGuard adpt_guard;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  LOG_TRACE("[VECTOR INDEX MERGE] start process_merge", K(ret), K(ctx_->task_status_), K(ls_id_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVecIdxMergeTask is not init", KR(ret));
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
  } else if (! adpt_guard.get_adatper()->is_complete()) {
    ret = OB_EAGAIN;
    LOG_INFO("adapter not complete, need wait", K(ret), KP(adpt_guard.get_adatper()));
  } else if (adpt_guard.get_adatper()->get_create_type() != CreateTypeComplete) {
    ret = OB_EAGAIN;
    LOG_INFO("adapter is not complete, no need merge", K(ret), KPC(adpt_guard.get_adatper()));
  } else if (!check_snapshot_table_available(*adpt_guard.get_adatper())) {
    ret = OB_EAGAIN; // will retry
    LOG_INFO("skip to do async task due to snapshot table not available", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(check_and_merge(adpt_guard))) {
    LOG_WARN("[VECTOR INDEX MERGE] fail to check and merge", K(ret), KP(adpt_guard.get_adatper()));
  } else {
    LOG_INFO("[VECTOR INDEX MERGE] process merge success", K(ret), KP(adpt_guard.get_adatper()));
  }

  return ret;
}

int ObVecIdxMergeTask::check_and_merge(ObPluginVectorIndexAdapterGuard &adpt_guard)
{
  int ret = OB_SUCCESS;
   ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexAdapterGuard new_adpt_guard;
  ObPluginVectorIndexAdaptor *new_adapter = nullptr;
  bool need_merge = false;

  // prepare adaptor
  set_old_adapter(adpt_guard.get_adatper());
  if (OB_FAIL(create_new_adapter(vector_index_service, adpt_guard, new_adapter))) {
    LOG_WARN("fail to get or create new adapter", K(ret), K(ctx_));
  } else if (OB_FAIL(new_adpt_guard.set_adapter(new_adapter))) { // inc_ref + 1
    LOG_WARN("fail to set new adpater to guard", K(ret));
  } else if (! ctx_->task_status_.target_scn_.is_valid() && OB_FAIL(get_current_scn(ctx_->task_status_.target_scn_))) {
    LOG_WARN("fail to get scn", KR(ret));
  } else if (OB_FAIL(check_and_wait_write())) {
    LOG_WARN("fail to check and wait write", KR(ret));
  }

  // merge
  if (OB_SUCC(ret) && ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING) {
    if (OB_FAIL(adpt_guard.get_adatper()->check_need_merge(1/*merge_base_percentage*/, need_merge))) {
      LOG_WARN("check need merge", K(ret), KPC(adpt_guard.get_adatper()));
    } else if (! need_merge) {
      ret = OB_EAGAIN;
      LOG_WARN("snapshot data actually is not need to merge in task execution, so retry", K(ret), KPC(adpt_guard.get_adatper()));
    } else if (OB_FAIL(execute_merge())) {
      LOG_WARN("merge incr fail", K(ret), KPC(ctx_));
    } else {
      common::ObSpinLockGuard ctx_guard(ctx_->lock_);
      ctx_->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE;
      if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(ctx_))) {
        LOG_WARN("fail to update task status to inner table", K(ret), K(tenant_id_), KPC(ctx_));
      }
    }
  }

  // exchange
  if (OB_SUCC(ret) && ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_EXCHANGE) {
    if (OB_FAIL(execute_exchange())) {
      LOG_WARN("execute exchange fail", K(ret), KPC(ctx_));
    } else {
      common::ObSpinLockGuard ctx_guard(ctx_->lock_);
      ctx_->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN;
      if (OB_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code(ctx_))) {
        LOG_WARN("fail to update task status to inner table", K(ret), K(tenant_id_), KPC(ctx_));
      }
    }
  }

  // clean.
  // delete table 5 un-visible data when fail or finish
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(ret) || ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CLEAN) {
    if (OB_TMP_FAIL(execute_clean())) {
      LOG_WARN("fail to execute clean", K(ret), K(tmp_ret), KPC(ctx_));
    }
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }

  // clean tmp info
  {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(vector_index_service->release_vector_index_tmp_info(ctx_->task_status_.task_id_))) {
      LOG_WARN("fail to release vector index tmp info", K(ret));
    }
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }
  // release pre alloc size
  if (pre_alloc_size_ > 0) {
    ObSharedMemAllocMgr *shared_mem_mgr = MTL(ObSharedMemAllocMgr*);
    ObTenantVectorAllocator& vector_allocator = shared_mem_mgr->vector_allocator();
    vector_allocator.pre_free(pre_alloc_size_);
    pre_alloc_size_ = 0;
  }

  LOG_TRACE("[VECTOR INDEX MERGE] end process_merge", K(ret), K(ctx_->task_status_));
  return ret;
}

int ObVecIdxMergeTask::exchange_snap_index_rows(
    const ObVectorIndexSegmentMeta& seg_meta,
    const ObTableSchema &data_table_schema,
    const ObTableSchema &snapshot_table_schema,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const uint64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObAccessService *oas = MTL(ObAccessService*);
  ObVecIndexATaskUpdIterator row_iter;
  ObDMLBaseParam dml_param;
  ObTableScanIterator *table_scan_iter = nullptr;
  storage::ObStoreCtxGuard store_ctx_guard;
  storage::ObTableScanParam snap_scan_param;
  schema::ObTableParam snap_table_param(allocator_);
  share::schema::ObTableDMLParam table_dml_param(allocator_);
  common::ObNewRowIterator *snap_data_iter = nullptr;
  common::ObCollationType cs_type = CS_TYPE_INVALID;
  const uint64_t schema_version = data_table_schema.get_schema_version();
  ObSEArray<uint64_t, 4> all_column_ids;
  ObSEArray<uint64_t, 4> dml_column_ids;
  ObSEArray<uint64_t, 1> upd_column_ids;
  ObSEArray<uint64_t, 4> extra_column_idxs;

  LOG_INFO("exchange seg meta info", K(seg_meta));
  if (OB_ISNULL(ctx_) || OB_ISNULL(tx_desc) || OB_ISNULL(new_adapter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(tx_desc), KP(ctx_));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                            new_adapter_,
                                            snapshot.version(), //ctx_->task_status_.target_scn_
                                            INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                            allocator_,
                                            allocator_,
                                            snap_scan_param,
                                            snap_table_param,
                                            snap_data_iter))) {
    LOG_WARN("fail to read data table local tablet.", K(ret));
  } else if (OB_FAIL(get_snap_index_column_info(data_table_schema, snapshot_table_schema, all_column_ids, dml_column_ids, extra_column_idxs, cs_type))) {
    LOG_WARN("fail to get snap index column info", K(ret));
  } else if (vector_visible_col_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector visible col idx", K(ret), K(vector_visible_col_idx_));
  } else if (OB_FAIL(upd_column_ids.push_back(all_column_ids.at(vector_visible_col_idx_)))) {
    LOG_WARN("fail to push back update column id", K(ret), K(vector_visible_col_idx_), K(all_column_ids));
  } else if (OB_FAIL(table_dml_param.convert(&snapshot_table_schema, snapshot_table_schema.get_schema_version(), dml_column_ids))) { // need this?
    LOG_WARN("fail to convert table dml param.", K(ret));
  } else if (OB_FALSE_IT(table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter))) {
  } else if (OB_FAIL(rescan(seg_meta, snap_scan_param, timeout_us, table_scan_iter))) {
    LOG_WARN("rescan fail", K(ret));
  } else if (OB_FAIL(prepare_dml_udp_row_iter(table_scan_iter, extra_column_idxs, row_iter))) {
    LOG_WARN("fail to prepare dml iter", K(ret));
  } else if (OB_FAIL(prepare_dml_param(dml_param, table_dml_param, store_ctx_guard, tx_desc, snapshot, schema_version, timeout_us))) {
    LOG_WARN("fail to prepare lob meta dml", K(ret));
  } else if (OB_FAIL(oas->update_rows(ls_id_, new_adapter_->get_snap_tablet_id(),
      *tx_desc, dml_param, dml_column_ids, upd_column_ids, &row_iter, affected_rows))) {
    LOG_WARN("fail to update_rows", K(ret), K(ctx_), K(dml_column_ids), K(upd_column_ids));
  } else {
    LOG_INFO("print update rows count", K(affected_rows), K(seg_meta));
  }
  if (OB_NOT_NULL(oas)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(snap_data_iter)) {
      tmp_ret = oas->revert_scan_iter(snap_data_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    snap_data_iter = nullptr;
  }
  return ret;
}

int ObVecIdxMergeTask::execute_merge()
{
  int ret = OB_SUCCESS;
  transaction::ObTxReadSnapshot snapshot;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  const int64_t data_table_id = new_adapter_->get_data_table_id();
  const int64_t snapshot_table_id = new_adapter_->get_snapshot_table_id();
  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *snapshot_schema = nullptr;
  SCN current_scn = ctx_->task_status_.target_scn_;
  const ObVecIdxSnapshotDataHandle& old_snap_data = old_adapter_->get_snap_data();
  if (OB_FAIL(prepare_merge_segment(old_snap_data))) {
    LOG_WARN("prepare merge segment fail", K(ret), K(old_snap_data));
  } else if (OB_FAIL(upadte_task_merge_segments_info())) {
    LOG_WARN("prepare task segment info fail", K(ret));
  } else if (OB_FAIL(prepare_schema_and_snapshot(data_schema, snapshot_schema, data_table_id, snapshot_table_id, ctx_->task_status_.target_scn_.get_val_for_sql(), snapshot))) {
    LOG_WARN("fail to prepare schema and snapshot", K(ret), K(ctx_));
  } else if (OB_FAIL(build_filter_clause(*data_schema, *snapshot_schema))) {
    LOG_WARN("fail to build_filter_clause", K(ret), KPC(ctx_));
  } else if (OB_FAIL(merge_bitmap(new_adapter_))) {
    LOG_WARN("merge bitmap fail", K(ret));
  } else if (OB_FAIL(execute_insert(data_schema))) {
    LOG_WARN("fail to execute insert", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObVecIdxMergeTask::execute_exchange()
{
  int ret = OB_SUCCESS;
  transaction::ObTxDesc *tx_desc = nullptr;
  transaction::ObTxReadSnapshot snapshot;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  const int64_t data_table_id = new_adapter_->get_data_table_id();
  const int64_t snapshot_table_id = new_adapter_->get_snapshot_table_id();
  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *snapshot_schema = nullptr;
  if (OB_FAIL(prepare_schema_and_snapshot(data_schema, snapshot_schema, data_table_id, snapshot_table_id, ctx_->task_status_.target_scn_.get_val_for_sql(), snapshot))) {
    LOG_WARN("fail to prepare schema and snapshot", K(ret), K(ctx_));
  } else if (OB_FAIL(ObInsertLobColumnHelper::start_trans(ls_id_, false/*is_for_read*/, timeout_us, tx_desc))) {
    LOG_WARN("fail to get tx_desc", K(ret));
  } else if (OB_ISNULL(tx_desc) || OB_ISNULL(txs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
  } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id_, timeout_us, snapshot))) { // get new snapshot version to exchange table 5 rows
    LOG_WARN("fail to get snapshot", K(ret));
  } else {
    ObVecIdxSnapshotDataHandle& new_snap = new_adapter_->get_snap_data();
    LOG_INFO("new snap info", K(new_snap));
    if (new_snap->meta_.bases_.count() > 1 || new_snap->meta_.incrs_.count() != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new snap is incorrect", K(ret), K(new_snap));
    } else if (new_snap->meta_.bases_.count() > 0 && OB_FAIL(exchange_snap_index_rows(new_snap->meta_.bases_.at(0), *data_schema, *snapshot_schema, tx_desc, snapshot, timeout_us))) {
      LOG_WARN("fail to exchange snap index rows", K(ret), K(new_snap));
    } else if (OB_FAIL(clean_snap_index_rows(*data_schema, *snapshot_schema, tx_desc, snapshot, timeout_us))) {
      LOG_WARN("fail to delete inc index rows", K(ret), K(ctx_));
    } else if (OB_FAIL(update_meta(new_snap, tx_desc, snapshot, timeout_us))) {
      LOG_WARN("update meta fail", K(ret), K(new_snap));
    }
  }
  int tmp_ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_VECTOR_INDEX_MERGE_COMMIT);
  if (nullptr != tx_desc && OB_SUCCESS != (tmp_ret = ObInsertLobColumnHelper::end_trans(tx_desc, OB_SUCCESS != ret, timeout_us))) {
    ret = tmp_ret;
    LOG_WARN("fail to end trans", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(refresh_adaptor())) {
    LOG_WARN("refresh adaptor fail", K(ret));
  }
  return ret;
}

int ObVecIdxMergeTask::execute_insert(const ObTableSchema *data_schema)
{
  int ret = OB_SUCCESS;
  int64_t parallelism;
  int64_t data_table_id;
  int64_t dest_table_id;
  ObTabletID dest_tablet_id;
  ObString partition_names("");
  ObArenaAllocator allocator("atask_built");

  if (OB_ISNULL(ctx_) || OB_ISNULL(new_adapter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(ctx_), K(new_adapter_));
  } else if (OB_FALSE_IT(data_table_id = new_adapter_->get_data_table_id())) {
  } else if (OB_FALSE_IT(dest_table_id = new_adapter_->get_snapshot_table_id())) {
  } else if (OB_FALSE_IT(dest_tablet_id = new_adapter_->get_snap_tablet_id())) {
  } else if (OB_FAIL(get_task_paralellism(parallelism))) {
    LOG_WARN("fail to get task parallelism", K(ret), K(ctx_));
  } else if (OB_FAIL(get_partition_name(*data_schema, data_table_id, dest_table_id, dest_tablet_id, allocator, partition_names))) {
    LOG_WARN("fail to get partition name", K(ret), K(ctx_));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::set_inner_sql_snapshot_version(ctx_->task_status_.task_id_, ctx_->task_status_.target_scn_.get_val_for_sql()))) {
    LOG_WARN("fail to set inner sql snapshot", K(ret), K(ctx_));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::set_inner_sql_schema_version(ctx_->task_status_.task_id_, data_schema->get_schema_version()))) {
    LOG_WARN("fail to set inner sql snapshot", K(ret), K(ctx_));
  } else if (OB_FAIL(execute_inner_sql(*data_schema, data_table_id, dest_table_id, ctx_->task_status_.task_id_,
      parallelism, partition_names, ctx_->task_status_.target_scn_))) {
    LOG_WARN("fail to execute inner sql", K(ret), K(partition_names), K(ctx_));
  }
  return ret;
}

int ObVecIdxMergeTask::clean_snap_index_rows(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &snapshot_table_schema,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const uint64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  ObDMLBaseParam dml_param;
  ObTableScanIterator *table_scan_iter = nullptr;
  storage::ObTableScanParam snap_scan_param;
  schema::ObTableParam snap_table_param(allocator_);
  share::schema::ObTableDMLParam table_dml_param(allocator_);
  common::ObNewRowIterator *snap_data_iter = nullptr;
  const uint64_t schema_version = data_table_schema.get_schema_version();
  common::ObCollationType cs_type = CS_TYPE_INVALID;
  ObSEArray<uint64_t, 4> all_column_ids;
  ObSEArray<uint64_t, 4> dml_column_ids;
  ObSEArray<uint64_t, 4> extra_column_idxs;

  if (OB_ISNULL(ctx_) || OB_ISNULL(tx_desc) || OB_ISNULL(new_adapter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(tx_desc), KP(ctx_));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                            new_adapter_,
                                            snapshot.version(), //ctx_->task_status_.target_scn_
                                            INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                            allocator_,
                                            allocator_,
                                            snap_scan_param,
                                            snap_table_param,
                                            snap_data_iter))) {
    LOG_WARN("fail to read data table local tablet.", K(ret));
  } else if (OB_FAIL(get_snap_index_column_info(data_table_schema, snapshot_table_schema, all_column_ids, dml_column_ids, extra_column_idxs, cs_type))) {
    LOG_WARN("fail to get snap index column info", K(ret));
  } else if (OB_FAIL(table_dml_param.convert(&snapshot_table_schema, snapshot_table_schema.get_schema_version(), dml_column_ids))) { // need this?
    LOG_WARN("fail to convert table dml param.", K(ret));
  } else {
    table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter);
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_segments_.count(); ++i) {
      const ObVectorIndexSegmentMeta *seg_meta = merge_segments_.at(i);
      LOG_INFO("delete segment info", K(i), KPC(seg_meta));
      if (OB_FAIL(delete_segment_rows(*seg_meta, snap_scan_param, timeout_us, table_scan_iter,
          dml_column_ids, extra_column_idxs, tx_desc, snapshot,
          table_dml_param, new_adapter_->get_snap_tablet_id(), schema_version))) {
        LOG_WARN("delete incr segment row fail", K(ret), K(i), KPC(seg_meta));
      }
    }
  }
  if (OB_NOT_NULL(oas)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(snap_data_iter)) {
      tmp_ret = oas->revert_scan_iter(snap_data_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    snap_data_iter = nullptr;
  }
  return ret;
}

int ObVecIdxMergeTask::rescan(
    const ObVectorIndexSegmentMeta& seg_meta, storage::ObTableScanParam &scan_param,
    const uint64_t timeout, ObTableScanIterator *table_scan_iter)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  ObNewRange range;
  scan_param.key_ranges_.reuse();
  // update timeout
  scan_param.timeout_ = timeout;
  scan_param.for_update_wait_timeout_ = scan_param.timeout_;
  int64_t rowkey_cnt = scan_param.table_param_->get_read_info().get_schema_rowkey_count();
  if (rowkey_objs_.count() < rowkey_cnt && OB_FAIL(rowkey_objs_.prepare_allocate(rowkey_cnt*2))) {
    LOG_WARN("prepare rowkey array fail", K(ret), K(rowkey_cnt));
  } else if (OB_FAIL(build_rowkey_range(seg_meta, range))) {
    LOG_WARN("build_range fail", K(ret));
  } else if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
    LOG_WARN("push key range fail", K(ret), K(scan_param), K(range));
  } else if (OB_FAIL(oas->reuse_scan_iter(false/*tablet id same*/, table_scan_iter))) {
    LOG_WARN("reuse scan iter fail", K(ret));
  } else if (OB_FAIL(oas->table_rescan(scan_param, table_scan_iter))) {
    LOG_WARN("do table rescan fail", K(ret));
  }
  return ret;
}

int ObVecIdxMergeTask::build_rowkey_range(const ObVectorIndexSegmentMeta& seg_meta, ObNewRange &range)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = rowkey_objs_.count() / 2;
  for (int64_t i = 0; i < rowkey_cnt; ++i) {
    if (vector_key_col_idx_ == i) {
      rowkey_objs_[i].reset();
      rowkey_objs_[i].set_varchar(seg_meta.start_key_);
      rowkey_objs_[i].set_collation_type(key_col_cs_type_);
    } else {
      rowkey_objs_[i] = ObObj::make_min_obj();
    }
  }
  ObRowkey min_row_key(rowkey_objs_.get_data(), rowkey_cnt);
  for (int64_t i = rowkey_cnt; i < rowkey_objs_.count(); ++i) {
    if (i - rowkey_cnt == vector_key_col_idx_) {
      rowkey_objs_[i].reset();
      rowkey_objs_[i].set_varchar(seg_meta.end_key_);
      rowkey_objs_[i].set_collation_type(key_col_cs_type_);
    } else {
      rowkey_objs_[i] = ObObj::make_max_obj();
    }
  }
  ObRowkey max_row_key(rowkey_objs_.get_data() + rowkey_cnt, rowkey_cnt);

  range.table_id_ = 0; // TODO make sure this is correct
  range.start_key_ = min_row_key;
  range.end_key_ = max_row_key;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  return ret;
}

int ObVecIdxMergeTask::delete_segment_rows(
    const ObVectorIndexSegmentMeta& seg_meta, storage::ObTableScanParam &scan_param,
    const uint64_t timeout, ObTableScanIterator *table_scan_iter,
    ObIArray<uint64_t> &dml_column_ids, ObIArray<uint64_t> &extra_column_idxs,
    transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
    share::schema::ObTableDMLParam &table_dml_param, const ObTabletID &tablet_id,
    const uint64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  int64_t affected_rows = 0;
  ObDMLBaseParam dml_param;
  storage::ObValueRowIterator row_iter;
  storage::ObStoreCtxGuard store_ctx_guard;
  common::ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_FAIL(rescan(seg_meta, scan_param, timeout, table_scan_iter))) {
    LOG_WARN("rescan fail", K(ret));
  } else if (OB_FAIL(prepare_dml_del_row_iter(tx_desc, cs_type, table_scan_iter, extra_column_idxs, row_iter, snapshot))) {
    LOG_WARN("fail to prepare dml iter", K(ret));
  } else if (OB_FAIL(prepare_dml_param(dml_param, table_dml_param, store_ctx_guard, tx_desc, snapshot, schema_version, timeout))) {
    LOG_WARN("fail to prepare lob meta dml", K(ret));
  } else if (OB_FAIL(oas->delete_rows(ls_id_, tablet_id, *tx_desc, dml_param, dml_column_ids, &row_iter, affected_rows))) {
    LOG_WARN("failed to delete rows from snapshot table", K(ret));
  }
  return ret;
}

int ObVecIdxMergeTask::update_meta(
    const ObVecIdxSnapshotDataHandle& new_snap, transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot, const uint64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObVecIdxSnapTableSegMergeOp snap_table_handler(new_adapter_->get_tenant_id());
  const int64_t base_count = new_snap->meta_.bases_.count();
  ObVectorIndexSegmentMeta *new_seg_meta = base_count > 0 ? &new_snap->meta_.bases_.at(0) : nullptr;
  if (OB_FAIL(snap_table_handler.init(ls_id_, new_adapter_->get_data_table_id(), new_adapter_->get_snapshot_table_id(), new_adapter_->get_snap_tablet_id()))) {
    LOG_WARN("init snap table handler fail", K(ret));
  } else if (OB_FAIL(snap_table_handler.preprea_meta(new_seg_meta, merge_segments_))) {
    LOG_WARN("prepare meta fail", K(ret));
  } else if (OB_FAIL(snap_table_handler.insertup_meta_row(new_adapter_, tx_desc, timeout_us))) {
    LOG_WARN("insertup_meta_row fail", K(ret));
  } else if (OB_FAIL(upadte_task_result_segments_info(new_seg_meta))) {
    // The observability should not alter the execution flow. So update task info here
    LOG_WARN("upadte task result segments info fail", K(ret));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObVecIdxMergeTask::prepare_new_meta(
    ObVectorIndexMeta &new_meta,
    ObVectorIndexSegmentMeta &new_seg_meta,
    const ObVectorIndexMeta &old_meta)
{
  int ret = OB_SUCCESS;
  if (merge_from_base_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < old_meta.bases_.count(); ++i) {
      const ObVectorIndexSegmentMeta &old_seg_meta = old_meta.bases_.at(i);
      bool is_merge_segment = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < merge_segments_.count() && ! is_merge_segment; ++j) {
        const ObVectorIndexSegmentMeta *merge_seg_meta = merge_segments_.at(j);
        if (0 == merge_seg_meta->start_key_.compare(old_seg_meta.start_key_)) {
          is_merge_segment = true;
        }
      }
      if (! is_merge_segment && OB_FAIL(new_meta.bases_.push_back(old_seg_meta))) {
        LOG_WARN("push back seg meta to bases fail", K(ret), K(old_seg_meta), K(new_meta));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(new_meta.bases_.push_back(new_seg_meta))) {
      LOG_WARN("push back seg meta to bases fail", K(ret), K(new_seg_meta), K(new_meta));
    }
  } else if (OB_FAIL(new_meta.bases_.assign(old_meta.bases_))) {
    LOG_WARN("assign base fail", K(ret));
  } else if (OB_FAIL(new_meta.incrs_.push_back(new_seg_meta))) {
    LOG_WARN("push back seg meta to incrs fail", K(ret), K(new_meta));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < old_meta.incrs_.count(); ++i) {
    const ObVectorIndexSegmentMeta &old_seg_meta = old_meta.incrs_.at(i);
    bool is_merge_segment = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < merge_segments_.count() && ! is_merge_segment; ++j) {
      const ObVectorIndexSegmentMeta *merge_seg_meta = merge_segments_.at(j);
      if (0 == merge_seg_meta->start_key_.compare(old_seg_meta.start_key_)) {
        is_merge_segment = true;
      }
    }
    if (! is_merge_segment && OB_FAIL(new_meta.incrs_.push_back(old_seg_meta))) {
      LOG_WARN("push back seg meta to incrs fail", K(ret), K(old_seg_meta), K(new_meta));
    }
  }
  LOG_INFO("prepare new meta finished", K(ret), K(new_meta), K(old_meta));
  return ret;
}

int ObVecIdxMergeTask::refresh_adaptor()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  share::SCN current_scn;
  schema::ObTableParam snap_table_param(allocator_);
  storage::ObTableScanParam snap_scan_param;
  common::ObNewRowIterator *snap_data_iter = nullptr;
  ObTableScanIterator *table_scan_iter = nullptr;
  blocksstable::ObDatumRow *datum_row = nullptr;
  if (OB_FAIL(get_current_scn(current_scn))) {
    LOG_WARN("fail to get scn", KR(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                new_adapter_,
                                current_scn,
                                INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                allocator_,
                                allocator_,
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
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator_, ObLongTextType, true, meta_data, nullptr))) {
      LOG_WARN("read real string data fail", K(ret));
    } else if (OB_FAIL(ObVectorIndexMeta::get_meta_scn(meta_data, meta_scn))) {
      LOG_WARN("get meta scn fail", K(ret));
    } else if (meta_scn <= old_adapter_->get_snap_data()->meta_.header_.scn_) {
      no_need_replace_adaptor_ = true;
      LOG_WARN("no need refresh adaptor", K(meta_scn), KPC(old_adapter_));
      ObPluginVectorIndexAdapterGuard latest_adpt_guard;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(vec_idx_mgr_->get_adapter_inst_guard(ctx_->task_status_.tablet_id_, latest_adpt_guard))) {
        LOG_WARN("fail to get adapter instance", K(tmp_ret), KPC(ctx_));
      } else if (latest_adpt_guard.get_adatper() != old_adapter_) {
        LOG_INFO("old adaptor is changeed", KPC(old_adapter_), KPC(latest_adpt_guard.get_adatper()), KPC(new_adapter_));
      } else {
        int64_t pos = 0;
        ObVectorIndexMeta tmp_meta;
        if (OB_TMP_FAIL(tmp_meta.deserialize(meta_data.ptr(), meta_data.length(), pos))) {
          LOG_WARN("deserialize fail", K(ret), K(meta_data.length()));
        }
        ret = OB_ERR_UNEXPECTED;
        LOG_INFO("old adaptor is not changeed, but scn is small", K(tmp_meta), KPC(old_adapter_), KPC(new_adapter_));
      }
    } else if (OB_FALSE_IT(new_adapter_->get_snap_data()->free_memdata_resource(new_adapter_->get_allocator(), new_adapter_->get_tenant_id()))) {
    } else if (OB_FAIL(new_adapter_->deserialize_snap_meta(meta_data))) {
      LOG_WARN("deserialize snap meta fail", K(ret), K(meta_scn), K(meta_data.length()));
    } else if (OB_FAIL(try_reuse_segments_from_old_adapter())) {
      LOG_WARN("try reuse segments from old adapter fail", K(ret));
    } else if (OB_FAIL(new_adapter_->get_snap_data()->load_persist_segments(new_adapter_, allocator_, ls_id_, current_scn))) {
      LOG_WARN("load persist segments fail", K(ret));
    } else if (OB_FALSE_IT(new_adapter_->get_snap_data()->set_inited())) {
    } else if (OB_FALSE_IT(new_adapter_->close_snap_data_rb_flag())) {
    } else {
      LOG_INFO("build new adaptor success", KPC(new_adapter_), KPC(old_adapter_));
      ObPluginVectorIndexAdapterGuard latest_adpt_guard;
      RWLock::WLockGuard lock_guard(vec_idx_mgr_->get_adapter_map_lock());
      if (OB_FAIL(vec_idx_mgr_->get_adapter_inst_guard_in_lock(ctx_->task_status_.tablet_id_, latest_adpt_guard))) {
        LOG_WARN("fail to get adapter instance", K(ret), KPC(ctx_));
      } else if (latest_adpt_guard.get_adatper()->get_snapshot_scn() > new_adapter_->get_snapshot_scn()) {
        LOG_INFO("snap data is load by other, so no need to refresh", KPC(new_adapter_), KPC(latest_adpt_guard.get_adatper()));
      } else if (OB_FAIL(new_adapter_->merge_incr_data(latest_adpt_guard.get_adatper()))){
        LOG_WARN("partial vector index adapter not valid", K(ret), KP(latest_adpt_guard.get_adatper()), KPC(this));
      } else if (OB_FAIL(new_adapter_->set_replace_scn(current_scn))) {
        LOG_WARN("failed to set replace scn", K(ret), K(current_scn));
      } else if (OB_FAIL(vec_idx_mgr_->replace_old_adapter(new_adapter_))) {
        LOG_WARN("failed to replace old adapter", K(ret));
      } else {
        has_replace_old_adapter_ = true;
        LOG_INFO("relpace old adaptor success", KP(new_adapter_), KP(old_adapter_), KP(latest_adpt_guard.get_adatper()));
      }
    }
  }

  if (OB_NOT_NULL(snap_data_iter)) {
    int tmp_ret = MTL(ObAccessService*)->revert_scan_iter(snap_data_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert snap_data_iter failed", K(ret));
    }
    snap_data_iter = nullptr;
  }

  return ret;
}

int ObVecIdxMergeTask::check_and_wait_write()
{
  int ret = OB_SUCCESS;
  const static int64_t WAIT_US = 1000 * 1000; // 100ms
  const static int64_t MAX_WAIT_CNT = 100;
  int64_t check_cnt = 0;
  const ObTabletID data_tablet_id = new_adapter_->get_data_tablet_id();
  const int64_t ts = ctx_->task_status_.target_scn_.get_val_for_sql();
  const ObLSID &ls_id = ls_id_;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  ObLSService *ls_service = MTL(ObLSService *);
  SCN snapshot_version;
  transaction::ObTransID tx_id;
  ObLSHandle ls_handle;
  while(OB_SUCC(ret) && check_cnt < MAX_WAIT_CNT && ! snapshot_version.is_valid()) {
    if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("get ls failed", K(ls_id));
    } else if (OB_FAIL(ls_handle.get_ls()->check_modify_time_elapsed(data_tablet_id, ts, tx_id))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("check modify time elapsed failed", K(ret), K(ts), K(tx_id), K(data_tablet_id), K(ls_id));
      } else {
        ret = OB_SUCCESS;
        ob_usleep(WAIT_US);
        LOG_INFO("there are some write trans donot commit, so need wait", K(ls_id), K(data_tablet_id), K(ts));
      }
    } else if (OB_FAIL(txs->get_max_commit_version(snapshot_version))) {
      LOG_WARN("fail to get max commit version", K(ret));
    } else {
      ctx_->task_status_.target_scn_ = snapshot_version;
      LOG_INFO("check success", K(snapshot_version), K(data_tablet_id), K(ts), K(tx_id));
    }
    ++check_cnt;
  }
  return ret;
}


}
}
