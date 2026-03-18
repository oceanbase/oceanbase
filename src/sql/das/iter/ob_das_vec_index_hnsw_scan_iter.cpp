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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_vec_index_hnsw_scan_iter.h"
#include "src/storage/access/ob_table_scan_iterator.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace transaction;
using namespace share;

namespace sql
{

int ObDASVecIndexHNSWScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ObDASIterType::DAS_ITER_HNSW_SCAN != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for hnsw scan iter", K(ret), K(param));
  } else {
    ObDASVecIndexHNSWScanIterParam &hnsw_scan_param = static_cast<ObDASVecIndexHNSWScanIterParam &>(param);
    eval_ctx_ = hnsw_scan_param.eval_ctx_;
    ls_id_ = hnsw_scan_param.ls_id_;
    tx_desc_ = hnsw_scan_param.tx_desc_;
    snapshot_ = hnsw_scan_param.snapshot_;

    delta_buf_iter_ = hnsw_scan_param.delta_buf_iter_;
    index_id_iter_ = hnsw_scan_param.index_id_iter_;
    snapshot_iter_ = hnsw_scan_param.snapshot_iter_;
    com_aux_vec_iter_ = hnsw_scan_param.com_aux_vec_iter_;

    vec_index_scan_ctdef_ = hnsw_scan_param.vec_index_scan_ctdef_;
    vec_index_scan_rtdef_ = hnsw_scan_param.vec_index_scan_rtdef_;
    sort_expr_ = hnsw_scan_param.sort_expr_;
    search_ctx_ = hnsw_scan_param.search_ctx_;


    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "HNSW", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create vector hnsw memory context", K(ret));
      } else {
        ada_ctx_.set_temp_allocator(&mem_context_->get_arena_allocator());
      }
    }


    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_sort())) {
      LOG_WARN("failed to init sort", K(ret));
    } else if (OB_FAIL(ObVectorIndexParam::build_search_param(vec_index_scan_ctdef_->vector_index_param_, vec_index_scan_ctdef_->query_param_, search_param_))) {
      LOG_WARN("build search param fail", K(vec_index_scan_ctdef_->vector_index_param_), K(vec_index_scan_ctdef_->query_param_));
    } else {
      if (search_param_.similarity_threshold_ != 0) {
        if (OB_FAIL(ObDasVecScanUtils::get_distance_threshold_hnsw(*sort_expr_, search_param_.similarity_threshold_, distance_threshold_))) {
          LOG_WARN("get distance threshold fail", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDASVecIndexHNSWScanIter::init_sort()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sort_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort expr is null", K(ret));
  } else if (sort_expr_->is_vector_sort_expr()) {
    distance_calc_ = sort_expr_;
    if (sort_expr_->arg_cnt_ != 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg num", K(ret), K(sort_expr_->arg_cnt_));
    } else if (sort_expr_->args_[0]->is_const_expr()) {
      search_vec_ = sort_expr_->args_[0];
    } else if (sort_expr_->args_[1]->is_const_expr()) {
      search_vec_ = sort_expr_->args_[1];
    }
  }
  return ret;
}

int ObDASVecIndexHNSWScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!com_aux_vec_iter_first_scan_ && OB_FAIL(reuse_com_aux_vec_iter())) {
    LOG_WARN("failed to reuse com aux vec iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }

  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  query_cond_ = nullptr;
  first_post_filter_search_ = true;

  // memory reset
  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
  ada_ctx_.~ObVectorQueryAdaptorResultContext();
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }
  vec_op_alloc_.reset();

  // must be after memory reset
  new (&ada_ctx_) ObVectorQueryAdaptorResultContext(MTL_ID(), 0, &vec_op_alloc_, nullptr);
  ada_ctx_.set_temp_allocator(&mem_context_->get_arena_allocator());

  return ret;
}

int ObDASVecIndexHNSWScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_NOT_NULL(delta_buf_iter_) && OB_FAIL(delta_buf_iter_->release())) {
    LOG_WARN("failed to release delta buf iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(index_id_iter_) && OB_FAIL(index_id_iter_->release())) {
    LOG_WARN("failed to release index id iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(snapshot_iter_) && OB_FAIL(snapshot_iter_->release())) {
    LOG_WARN("failed to release snapshot iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(com_aux_vec_iter_) && OB_FAIL(com_aux_vec_iter_->release())) {
    LOG_WARN("failed to release com aux vec iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  delta_buf_iter_ = nullptr;
  index_id_iter_ = nullptr;
  snapshot_iter_ = nullptr;
  com_aux_vec_iter_ = nullptr;
  tx_desc_ = nullptr;
  snapshot_ = nullptr;
  vec_index_scan_ctdef_ = nullptr;
  vec_index_scan_rtdef_ = nullptr;
  distance_calc_ = nullptr;
  query_cond_ = nullptr;

  ObDasVecScanUtils::release_scan_param(delta_buf_scan_param_);
  ObDasVecScanUtils::release_scan_param(index_id_scan_param_);
  ObDasVecScanUtils::release_scan_param(snapshot_scan_param_);
  ObDasVecScanUtils::release_scan_param(com_aux_vec_scan_param_);

  // memory reset
  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
  ada_ctx_.~ObVectorQueryAdaptorResultContext();
  if (nullptr != mem_context_)  {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  vec_op_alloc_.reset();

  return ret;
}

uint64_t ObDASVecIndexHNSWScanIter::adjust_batch_count(bool is_vectored, uint64_t batch_count)
{
  uint64_t ret_count = batch_count;
  if (need_save_distance_result()) {
    if (!is_vectored) {
      ret_count = 1;
    } else if (batch_count > MAX_OPTIMIZE_BATCH_COUNT) {
      ret_count = MAX_OPTIMIZE_BATCH_COUNT;
    }
  }
  return ret_count;
}

int ObDASVecIndexHNSWScanIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  // todo
  return ret;
}

void ObDASVecIndexHNSWScanIter::reset_adaptor_vid_iter_for_next_iteration()
{
  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
}

int ObDASVecIndexHNSWScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  reset_adaptor_vid_iter_for_next_iteration();
  if (OB_FAIL(process_adaptor_state(true))) {
    LOG_WARN("failed to process adaptor state", K(ret));
  }
  return ret;
}

bool ObDASVecIndexHNSWScanIter::is_parallel_with_block_granule()
{
  int64_t expected_worker_cnt = 0;
  bool is_block_granule_type = false;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  if (OB_NOT_NULL(exec_ctx_)) {
    task_exec_ctx = exec_ctx_->get_task_executor_ctx();
    is_block_granule_type = exec_ctx_->is_block_granule_type();
    if (OB_NOT_NULL(task_exec_ctx)) {
      expected_worker_cnt = task_exec_ctx->get_expected_worker_cnt();
    }
  }
  LOG_DEBUG("print worker cnt and granule type", K(expected_worker_cnt), K(is_block_granule_type));
  return expected_worker_cnt > 1 && is_block_granule_type;
}

int ObDASVecIndexHNSWScanIter::process_adaptor_state(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  bool ls_leader = true;
  if (!query_cond_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to set query condition.", K(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::get_ls_leader_flag(ls_id_, ls_leader))) {
    LOG_WARN("fail to get ls leader flag", K(ret), K(ls_id_));
  } else if (OB_ISNULL(snapshot_) || ! snapshot_->core_.version_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("snapshot is null", K(ret), KPC(snapshot_));
  } else if (OB_FALSE_IT(ada_ctx_.set_scn(snapshot_->core_.version_))) {
  } else if (OB_FALSE_IT(ada_ctx_.set_ls_leader(ls_leader))) {
  } else if (!ls_leader) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (!tenant_config.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail get tenant_config", KR(ret), K(MTL_ID()));
    } else if (!tenant_config->load_vector_index_on_follower) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "when load_vector_index_on_follower is false, weak read using vector index on follower is");
      LOG_WARN("when load_vector_index_on_follower is false, weak read using vector index on follower is not supported", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    RWLock::RLockGuard lock_guard(adaptor_->get_query_lock());
    if (is_pre_filter()) {
      if (OB_FAIL(process_adaptor_state_pre_filter(&ada_ctx_, adaptor_, is_vectorized))) {
        LOG_WARN("hnsw pre filter failed to query result.", K(ret));
      }
    } else if (OB_FAIL(process_adaptor_state_post_filter(&ada_ctx_, adaptor_, is_vectorized))) {
      LOG_WARN("hnsw post filter failed to query result.", K(ret));
    }
  }

  return ret;
}

int ObDASVecIndexHNSWScanIter::process_adaptor_state_pre_filter_brute_force(
                        ObVectorQueryAdaptorResultContext *ada_ctx,
                        ObPluginVectorIndexAdaptor* adaptor,
                        int64_t *&brute_vids,
                        int64_t& brute_cnt,
                        bool& need_complete_data,
                        bool check_need_complete_data)
{
  INIT_SUCC(ret);
  if (is_hnsw_bq()) {
    if (OB_FAIL(process_adaptor_state_pre_filter_brute_force_bq(ada_ctx, adaptor, brute_vids, brute_cnt, need_complete_data, check_need_complete_data))) {
      LOG_WARN("hnsw pre filter(brute force) failed to query result.", K(ret));
    }
  } else {
    if (OB_FAIL(process_adaptor_state_pre_filter_brute_force_not_bq(ada_ctx, adaptor, brute_vids, brute_cnt, need_complete_data, check_need_complete_data))) {
      LOG_WARN("hnsw pre filter(brute force) failed to query result.", K(ret));
    }
  }
  return ret;
}

int ObDASVecIndexHNSWScanIter::process_adaptor_state_pre_filter_brute_force_not_bq(
                        ObVectorQueryAdaptorResultContext *ada_ctx,
                        ObPluginVectorIndexAdaptor* adaptor,
                        int64_t *&brute_vids,
                        int64_t& brute_cnt,
                        bool& need_complete_data,
                        bool check_need_complete_data)
{
  INIT_SUCC(ret);
  ObString search_vec = query_cond_->query_vector_;
  need_complete_data = false;
  ObVecIdxQueryResult dist_result;

  uint64_t limit = limit_;
  uint64_t capacity = limit > brute_cnt ? brute_cnt : limit;
  ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
  ObSimpleMaxHeap max_heap(&allocator, capacity);
  if (capacity == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(adaptor)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null.", K(ret));
  } else if (OB_FAIL(max_heap.init())) {
    LOG_WARN("failed to init max heap.", K(ret));
  } else if (OB_FAIL(query_brute_force_distances(adaptor, search_vec, brute_vids, brute_cnt, dist_result))) {
    LOG_WARN("failed to query vids.", K(ret), K(brute_cnt));
  } else if (dist_result.distances_.count() != dist_result.segments_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("distances_list count is not equal segment_list count", K(ret),
      K(dist_result.distances_.count()), K(dist_result.segments_.count()));
  } else if (dist_result.distances_.count() == 0) {
    need_complete_data = check_need_complete_data ? true : false;
  } else if (dist_result.distances_.count() == 1) {
    const float* distances = dist_result.distances_.at(0);
    ObVectorIndexSegmentHandle &segment = dist_result.segments_.at(0);
    for (int64_t i = 0; i < brute_cnt && OB_SUCC(ret) && !need_complete_data; ++i) {
      double distance = distances[i];
      if (distance != -1 && distance <= distance_threshold_) {
        max_heap.push(brute_vids[i], distance, segment.get());
      } else {
        need_complete_data = check_need_complete_data ? true : false;
      }
    }
  } else {
    const int64_t segment_cnt = dist_result.segments_.count();
    for (int64_t i = 0; i < brute_cnt && OB_SUCC(ret) && !need_complete_data; ++i) {
      double distance = -1;
      int64_t k = 0;
      for (; k < segment_cnt && (distance = dist_result.distances_.at(k)[i]) == -1; ++k);

      if (distance != -1 && distance <= distance_threshold_) {
        max_heap.push(brute_vids[i], distance, dist_result.segments_.at(k).get());
      } else {
        need_complete_data = check_need_complete_data ? true : false;
      }
    }
  }

  // sort
  if (OB_SUCC(ret) && !need_complete_data) {
    max_heap.max_heap_sort();
  }

  // check heap size
  if (OB_FAIL(ret)) {
  } else if (need_complete_data) {
    // do nothing
  } else if (max_heap.get_size() == 0) {
    ret = OB_ITER_END;
  } else {
    uint64_t heap_size = max_heap.get_size();
    int64_t *vids = nullptr;
    float *distances = nullptr;

    if (OB_ISNULL(vids = static_cast<int64_t *>(vec_op_alloc_.alloc(sizeof(int64_t) * heap_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc vids", K(ret));
    } else if (OB_ISNULL(distances = static_cast<float *>(vec_op_alloc_.alloc(sizeof(float) * heap_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc distances", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else {
      int64_t heap_idx = 0;
      int64_t vid_idx = 0;
      while (OB_SUCC(ret) && heap_idx < heap_size) {
        int res_vid = max_heap.at(heap_idx);
        float res_dis = max_heap.value_at(heap_idx);
        vids[heap_idx] = res_vid;
        distances[heap_idx] = res_dis;
        heap_idx++;
      }

      void *iter_buff = nullptr;
      int64_t extra_info_actual_size = 0;
      int64_t extra_column_count = adaptor->get_extra_column_count();
      ObVecExtraInfoPtr extra_info_ptr;

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryVidIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc adaptor vid iter.", K(ret));
      } else if (OB_FALSE_IT(adaptor_vid_iter_ = new (iter_buff)
                             ObVectorQueryVidIterator(extra_column_count, extra_info_actual_size, query_cond_->rel_count_, query_cond_->rel_map_ptr_))) {
      } else if (OB_FAIL(adaptor_vid_iter_->init(heap_size, vids, distances, extra_info_ptr, &vec_op_alloc_))) {
        LOG_WARN("iter init failed.", K(ret));
      }
    }
  }
  adaptor->free_result(dist_result);
  return ret;
}

int64_t ObDASVecIndexHNSWScanIter::get_reorder_count_for_brute_force(const int64_t ef_search, const int64_t topK, const ObVectorIndexParam& param)
{
  const float refine_k = param.refine_k_;
  int64_t refine_cnt = refine_k * topK < MIN_BQ_REORDER_SIZE_FOR_BRUTE_FORCE ? MIN_BQ_REORDER_SIZE_FOR_BRUTE_FORCE : refine_k * topK;
  LOG_TRACE("reorder count info", K(ef_search), K(topK), K(refine_k), K(refine_cnt));
  return OB_MIN(OB_MAX(topK, OB_MAX(refine_cnt, ef_search)), MAX_VSAG_QUERY_RES_SIZE);
}

int ObDASVecIndexHNSWScanIter::init_brute_force_params(ObVectorQueryAdaptorResultContext *ada_ctx,
                                              ObPluginVectorIndexAdaptor* adaptor,
                                              BruteForceContext& ctx)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(ada_ctx) || OB_ISNULL(adaptor)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("ada_ctx or adaptor is null", K(ret), KP(ada_ctx), KP(adaptor));
  } else {
    ctx.limit = get_reorder_count_for_brute_force(query_cond_->ef_search_, limit_, search_param_);
    ctx.search_vec = query_cond_->query_vector_;
  }

  return ret;
}

int ObDASVecIndexHNSWScanIter::query_brute_force_distances(ObPluginVectorIndexAdaptor* adaptor,
                                                  const ObString& search_vec,
                                                  int64_t* brute_vids,
                                                  int64_t brute_cnt,
                                                  ObVecIdxQueryResult& dist_result)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(adaptor) || OB_ISNULL(brute_vids) || brute_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(adaptor), KP(brute_vids), K(brute_cnt));
  } else {
    dist_result.brute_cnt = brute_cnt;
    if (OB_FAIL(adaptor->vsag_query_vids(reinterpret_cast<float *>(const_cast<char*>(search_vec.ptr())),
                                         brute_vids, brute_cnt, dist_result, search_vec.length()))) {
      LOG_WARN("failed to brute force query", K(ret), K(brute_cnt));
    }
  }

  return ret;
}

int ObDASVecIndexHNSWScanIter::merge_and_sort_brute_force_results_bq(const ObVecIdxQueryResult& dist_result,
                                                            int64_t* brute_vids,
                                                            int64_t brute_cnt,
                                                            ObSimpleMaxHeap& snap_heap,
                                                            ObSimpleMaxHeap& incr_heap,
                                                            bool& need_complete_data,
                                                            bool check_need_complete_data)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(brute_vids) || brute_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(brute_vids), K(brute_cnt));
  } else {
    if (dist_result.distances_.count() == 0) {
      need_complete_data = check_need_complete_data ? true : false;
    } else {
      const int64_t segment_cnt = dist_result.segments_.count();
      for (int64_t i = 0; i < brute_cnt && OB_SUCC(ret) && !need_complete_data; ++i) {
        double distance = -1;
        int64_t k = 0;
        for (; k < segment_cnt && (distance = dist_result.distances_.at(k)[i]) == -1; ++k);

        if (distance == -1) {
          need_complete_data = check_need_complete_data ? true : false;
        } else if (segment_cnt > 1) {
          if (distance <= distance_threshold_ && ! dist_result.segments_.at(k)->is_base()) {
            incr_heap.push(brute_vids[i], distance, dist_result.segments_.at(k).get());
          }
          if (distance <= distance_threshold_ && dist_result.segments_.at(k)->is_base()) {
            snap_heap.push(brute_vids[i], distance, dist_result.segments_.at(k).get());
          }
        } else if (! dist_result.segments_.at(k)->is_base() && distance <= distance_threshold_) {
          incr_heap.push(brute_vids[i], distance, dist_result.segments_.at(k).get());
        } else if (dist_result.segments_.at(k)->is_base() && distance <= distance_threshold_) {
          snap_heap.push(brute_vids[i], distance, dist_result.segments_.at(k).get());
        }
      }
    }
  }

  return ret;
}

int ObDASVecIndexHNSWScanIter::build_brute_force_result_iterator_bq(ObPluginVectorIndexAdaptor* adaptor,
                                                            const ObSimpleMaxHeap& snap_heap,
                                                            const ObSimpleMaxHeap& incr_heap,
                                                            ObVecIdxQueryResult &dist_result,
                                                            ObVectorQueryVidIterator*& result_iter)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(adaptor)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("adaptor is null", K(ret));
  } else {
    uint64_t snap_size = snap_heap.get_size();
    uint64_t incr_size = incr_heap.get_size();
    uint64_t total_size = snap_size + incr_size;

    if (total_size == 0) {
      ret = OB_ITER_END;
    } else {
      int64_t *vids = nullptr;
      float *distances = nullptr;
      ObVectorIndexSegment** segment_ptrs = nullptr;

      if (OB_ISNULL(vids = static_cast<int64_t *>(vec_op_alloc_.alloc(sizeof(int64_t) * total_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate vids", K(ret));
      } else if (OB_ISNULL(distances = static_cast<float *>(vec_op_alloc_.alloc(sizeof(float) * total_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate distances", K(ret));
      } else if (OB_ISNULL(segment_ptrs = static_cast<ObVectorIndexSegment**>(vec_op_alloc_.alloc(sizeof(ObVectorIndexSegment*) * total_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate segment_ptrs", K(ret));
      }

      if (OB_SUCC(ret)) {
        int64_t idx = 0;
        hash::ObHashMap<int64_t, bool> added_vids;
        if (OB_FAIL(added_vids.create(total_size, "BQResultDedup"))) {
          LOG_WARN("failed to create added vids hash map", K(ret));
        } else {
          for (int64_t i = 0; i < incr_size && OB_SUCC(ret); ++i) {
            int64_t vid = incr_heap.at(i);
            vids[idx] = vid;
            distances[idx] = static_cast<float>(incr_heap.value_at(i));
            segment_ptrs[idx] = incr_heap.segment_at(i);
            idx++;

            if (OB_FAIL(added_vids.set_refactored(vid, true))) {
              LOG_WARN("failed to mark vid as added", K(ret), K(vid));
            }
          }

          for (int64_t i = 0; i < snap_size && OB_SUCC(ret); ++i) {
            int64_t vid = snap_heap.at(i);
            bool exists = false;
            if (OB_FAIL(added_vids.get_refactored(vid, exists))) {
              if (OB_HASH_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
                exists = false;
              } else {
                LOG_WARN("failed to check vid existence", K(ret), K(vid));
              }
            }

            if (OB_SUCC(ret) && !exists) {
              vids[idx] = vid;
              distances[idx] = static_cast<float>(snap_heap.value_at(i));
              segment_ptrs[idx] = snap_heap.segment_at(i);
              idx++;
            }
          }
          total_size = idx;
        }

        if (OB_SUCC(ret)) {
          int64_t extra_info_actual_size = 0;
          ObVecExtraInfoPtr extra_info_ptr;
          int64_t extra_column_count = adaptor->get_extra_column_count();

          void *iter_buff = nullptr;
          if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryVidIterator)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate adaptor vid iter", K(ret));
          } else if (OB_FALSE_IT(result_iter = new (iter_buff)
                                ObVectorQueryVidIterator(extra_column_count, extra_info_actual_size, query_cond_->rel_count_, query_cond_->rel_map_ptr_))) {
          } else if (OB_FAIL(result_iter->init(total_size, vids, distances, extra_info_ptr, &vec_op_alloc_))) {
            LOG_WARN("iter init failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASVecIndexHNSWScanIter::process_adaptor_state_pre_filter_brute_force_bq(
                        ObVectorQueryAdaptorResultContext *ada_ctx,
                        ObPluginVectorIndexAdaptor* adaptor,
                        int64_t *&brute_vids,
                        int64_t& brute_cnt,
                        bool& need_complete_data,
                        bool check_need_complete_data)
{
  INIT_SUCC(ret);

  BruteForceContext ctx;
  if (OB_FAIL(init_brute_force_params(ada_ctx, adaptor, ctx))) {
    LOG_WARN("failed to init brute force params", K(ret));
  } else {
    uint64_t capacity = ctx.limit > brute_cnt ? brute_cnt : ctx.limit;
    if (capacity == 0) {
      ret = OB_ITER_END;
    } else {
      ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
      ObVecIdxQueryResult dist_result;
      ObSimpleMaxHeap snap_heap(&allocator, capacity);
      ObSimpleMaxHeap incr_heap(&allocator, capacity);

      if (OB_FAIL(snap_heap.init())) {
        LOG_WARN("failed to init snap heap", K(ret));
      } else if (OB_FAIL(incr_heap.init())) {
        LOG_WARN("failed to init incr heap", K(ret));
      } else if (OB_FAIL(query_brute_force_distances(adaptor, ctx.search_vec, brute_vids, brute_cnt, dist_result))) {
        LOG_WARN("failed to query brute force distances", K(ret));
      } else if (OB_FAIL(merge_and_sort_brute_force_results_bq(dist_result, brute_vids, brute_cnt,
                                                          snap_heap, incr_heap, need_complete_data, check_need_complete_data))) {
        LOG_WARN("failed to merge and sort bq results", K(ret));
      } else if (OB_SUCC(ret) && !need_complete_data) {
        if (OB_FAIL(build_brute_force_result_iterator_bq(adaptor, snap_heap, incr_heap, dist_result, adaptor_vid_iter_))) {
          LOG_WARN("failed to build bq result iterator", K(ret));
        }
      }
      adaptor->free_result(dist_result);
    }
  }

  return ret;
}


int ObDASVecIndexHNSWScanIter::process_adaptor_state_pre_filter(
    ObVectorQueryAdaptorResultContext *ada_ctx,
    ObPluginVectorIndexAdaptor* adaptor,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;

  if (go_brute_force_ && bitmap_->type_ == ObVecIndexBitmap::VIDS) {
    int64_t *brute_vids = bitmap_->get_vids();
    int64_t brute_cnt = bitmap_->get_valid_cnt();

    bool need_complete_data = false;
    if (OB_FAIL(process_adaptor_state_pre_filter_brute_force(ada_ctx, adaptor, brute_vids, brute_cnt, need_complete_data, true))) {
      LOG_WARN("hnsw pre filter(brute force) failed to query result.", K(ret));
    } else if (need_complete_data) {
      query_cond_->only_complete_data_ = true;
      if (OB_FAIL(process_adaptor_state_post_filter(ada_ctx, adaptor, is_vectorized))) {
        LOG_WARN("failed to process adaptor state post filter", K(ret));
      } else if (OB_FAIL(process_adaptor_state_pre_filter_brute_force(ada_ctx, adaptor, brute_vids, brute_cnt, need_complete_data, false))) {
        LOG_WARN("hnsw pre filter(brute force) failed to query result.", K(ret));
      }
    }
  } else {
    if (OB_FAIL(init_pre_filter(ada_ctx))) {
      LOG_WARN("failed to init prefilter bitmap", K(ret));
    } else if (adaptor->check_if_complete_data(ada_ctx)) {
      if (OB_FAIL(process_adaptor_state_post_filter_once(ada_ctx, adaptor))) {
        LOG_WARN("failed to process adaptor state post filter", K(ret));
      }
    } else {
      if (OB_FAIL(adaptor->set_adaptor_ctx_flag(ada_ctx))) {
        LOG_WARN("failed to set adaptor ctx flag", K(ret));
      } else if (PVQP_SECOND == ada_ctx->get_flag() || (!ada_ctx->get_ls_leader())) {
        if (OB_FAIL(do_snapshot_table_scan())) {
          LOG_WARN("failed to do snapshot table scan.", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(snapshot_iter_)) {
        query_cond_->row_iter_ = snapshot_iter_->get_output_result_iter();
        query_cond_->query_scn_ = snapshot_scan_param_.snapshot_.core_.version_;
        query_cond_->scan_param_ = &snapshot_scan_param_;
      }

      if (FAILEDx(adaptor->query_result(ls_id_, ada_ctx, query_cond_, adaptor_vid_iter_))) {
        LOG_WARN("failed to query result.", K(ret));
      } else if (PVQ_REFRESH == ada_ctx->get_status()) {
        if (OB_FAIL(ObPluginVectorIndexUtils::query_need_refresh_memdata(adaptor, ls_id_, ada_ctx->get_ls_leader()))) {
          if (ret != OB_SCHEMA_EAGAIN) {
            LOG_WARN("fail to refresh memdata in query", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObDASVecIndexHNSWScanIter::init_pre_filter(ObVectorQueryAdaptorResultContext *ada_ctx)
{
  int ret = OB_SUCCESS;


  if (OB_ISNULL(bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is null", K(ret));
  } else if (bitmap_->type_ == ObVecIndexBitmap::VIDS) {
    if (OB_FAIL(bitmap_->upgrade_to_byte_array())) {
      LOG_WARN("failed to upgrade to byte array", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (bitmap_->type_ == ObVecIndexBitmap::BYTE_ARRAY) {
    // Directly assign bitmap pointer
    if (bitmap_->valid_cnt_ == 0 || OB_ISNULL(bitmap_->bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bitmap_ is null", K(ret));
    } else {
      uint64_t capacity = (bitmap_->max_vid_ - bitmap_->min_vid_ + 7) / 8 * 8;
      if (OB_FAIL(ada_ctx->init_prefilter(bitmap_->bitmap_, bitmap_->min_vid_, bitmap_->max_vid_, capacity, bitmap_->valid_cnt_))) {
        LOG_WARN("init prefilter with byte array bitmap failed", K(ret));
      }
    }
  } else if (bitmap_->type_ == ObVecIndexBitmap::ROARING_BITMAP) {
    // Directly assign roaring bitmap pointer
    if (OB_ISNULL(bitmap_->roaring_bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("roaring_bitmap_ is null", K(ret));
    } else if (OB_FAIL(ada_ctx->init_prefilter(bitmap_->roaring_bitmap_))) {
      LOG_WARN("init prefilter with roaring bitmap failed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid bitmap type", K(ret), K(bitmap_->type_));
  }

  return ret;
}

int ObDASVecIndexHNSWScanIter::process_adaptor_state_post_filter(
    ObVectorQueryAdaptorResultContext *ada_ctx,
    ObPluginVectorIndexAdaptor* adaptor,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  int64_t iter_scan_total_num = 0;
  if (first_post_filter_search_ && OB_FAIL(process_adaptor_state_post_filter_once(ada_ctx, adaptor))) {
    LOG_WARN("failed to process adaptor state post filter once.", K(ret), K(vec_index_type_), K(vec_idx_try_path_));
  } else if (!first_post_filter_search_ && OB_FAIL(adaptor->query_next_result(ada_ctx, query_cond_, adaptor_vid_iter_))) {
  } else if (first_post_filter_search_ && OB_FALSE_IT(first_post_filter_search_ = false)) {
  }
  return ret;
}

int ObDASVecIndexHNSWScanIter::process_adaptor_state_post_filter_once(
    ObVectorQueryAdaptorResultContext *ada_ctx,
    ObPluginVectorIndexAdaptor* adaptor)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ada_ctx) || OB_ISNULL(adaptor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shouldn't be null.", K(ret), K(ada_ctx), K(adaptor));
  } else {
    ObVidAdaLookupStatus last_state = ObVidAdaLookupStatus::STATES_ERROR;
    ObVidAdaLookupStatus cur_state = ObVidAdaLookupStatus::STATES_INIT;

    if (adaptor->get_can_skip() == SKIP && ada_ctx->get_ls_leader()) {
      if (OB_FAIL(ada_ctx->init_bitmaps())) {
        LOG_WARN("failed to init bitmaps", K(ret));
      } else {
        if (!adaptor->get_snap_has_complete()) {
          ada_ctx->set_status(PVQ_LACK_SCN);
          ada_ctx->set_flag(PVQP_SECOND);
          cur_state = ObVidAdaLookupStatus::QUERY_SNAPSHOT_TBL;
        } else {
          ada_ctx->set_status(PVQ_OK);
          ada_ctx->set_flag(PVQP_FIRST);
          cur_state = ObVidAdaLookupStatus::STATES_SET_RESULT;
        }
      }
    }

    while (OB_SUCC(ret) && ObVidAdaLookupStatus::STATES_ERROR != cur_state && ObVidAdaLookupStatus::STATES_FINISH != cur_state) {
      if ((last_state != cur_state || cur_state == ObVidAdaLookupStatus::QUERY_ROWKEY_VEC) && OB_FAIL(prepare_state(cur_state, *ada_ctx))) {
        LOG_WARN("failed to prepare state", K(ret));
      } else if (OB_FAIL(call_pva_interface(cur_state, *ada_ctx, *adaptor))) {
        LOG_WARN("failed to call pva interface", K(ret));
      } else if (OB_FALSE_IT(last_state = cur_state)) {
      } else if (OB_FAIL(next_state(cur_state, *ada_ctx))) {
        LOG_WARN("failed to get next status.", K(cur_state), K(ada_ctx->get_status()), K(ret));
      }
    }
  }

  return ret;
}

int ObDASVecIndexHNSWScanIter::prepare_state(const ObVidAdaLookupStatus& cur_state, ObVectorQueryAdaptorResultContext &ada_ctx)
{
  int ret = OB_SUCCESS;

  switch(cur_state) {
    case ObVidAdaLookupStatus::STATES_INIT: {
      if (OB_FAIL(do_delta_buf_table_scan())) {
        LOG_WARN("failed to do delta buf table scan.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_ROWKEY_VEC: {
      if (OB_FAIL(prepare_complete_vector_data(ada_ctx))) {
        LOG_WARN("failed to prepare complete vector data.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_INDEX_ID_TBL: {
      if (OB_FAIL(do_index_id_table_scan())) {
        LOG_WARN("failed to do index id table scan.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_SNAPSHOT_TBL: {
      if (OB_FAIL(do_snapshot_table_scan())) {
        LOG_WARN("failed to do snapshot table scan.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::STATES_REFRESH:
    case ObVidAdaLookupStatus::STATES_SET_RESULT: {
      // do nothing
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status.", K(ret));
      break;
    }
  }
  return ret;
}

int ObDASVecIndexHNSWScanIter::call_pva_interface(const ObVidAdaLookupStatus& cur_state,
                                          ObVectorQueryAdaptorResultContext& ada_ctx,
                                          ObPluginVectorIndexAdaptor &adaptor)
{
  int ret = OB_SUCCESS;
  switch(cur_state) {
    case ObVidAdaLookupStatus::STATES_INIT: {
      ObNewRowIterator *real_delta_buf_iter = delta_buf_iter_->get_output_result_iter();
      if (OB_FAIL(adaptor.check_delta_buffer_table_readnext_status(&ada_ctx, real_delta_buf_iter, delta_buf_scan_param_.snapshot_.core_.version_))) {
        LOG_WARN("failed to check delta buffer table readnext status.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_ROWKEY_VEC: {
      if (OB_FAIL(adaptor.complete_delta_buffer_table_data(&ada_ctx))) {
        LOG_WARN("failed to complete delta buffer table data.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_INDEX_ID_TBL: {
      ObNewRowIterator *real_index_id_iter = index_id_iter_->get_output_result_iter();
      if (!index_id_scan_param_.snapshot_.is_valid() || !index_id_scan_param_.snapshot_.core_.version_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get index id scan param invalid.", K(ret));
      } else if (OB_FAIL(adaptor.check_index_id_table_readnext_status(&ada_ctx, real_index_id_iter, index_id_scan_param_.snapshot_.core_.version_))) {
        LOG_WARN("failed to check index id table readnext status.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_SNAPSHOT_TBL: {
      if (OB_FAIL(adaptor.check_snapshot_table_wait_status(&ada_ctx))) {
        LOG_WARN("failed to check snapshot table wait status.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::STATES_SET_RESULT: {
      if (OB_NOT_NULL(snapshot_iter_)) {
        query_cond_->row_iter_ = snapshot_iter_->get_output_result_iter();
        query_cond_->query_scn_ = snapshot_scan_param_.snapshot_.core_.version_;
        query_cond_->scan_param_ = &snapshot_scan_param_;
      }
      if (!ada_ctx.get_ls_leader() && OB_FAIL(prepare_follower_query_cond(*query_cond_))) {
        LOG_WARN("fail to prepare query cond of follower", K(ret));
      } else if (OB_FAIL(adaptor.query_result(ls_id_, &ada_ctx, query_cond_, adaptor_vid_iter_))) {
        LOG_WARN("failed to query result.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::STATES_REFRESH: {
      if (OB_FAIL(ObPluginVectorIndexUtils::query_need_refresh_memdata(&adaptor, ls_id_, ada_ctx.get_ls_leader()))) {
        if (ret != OB_SCHEMA_EAGAIN) {
          LOG_WARN("fail to refresh memdata in query", K(ret));
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status.", K(ret));
      break;
    }
  }
  return ret;
}

int ObDASVecIndexHNSWScanIter::next_state(ObVidAdaLookupStatus& cur_state, ObVectorQueryAdaptorResultContext& ada_ctx)
{
  int ret = OB_SUCCESS;
  switch(cur_state) {
    case ObVidAdaLookupStatus::STATES_INIT: {
      if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_WAIT) {
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_LACK_SCN) {
        cur_state = ObVidAdaLookupStatus::QUERY_INDEX_ID_TBL;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_OK) {
        cur_state = ObVidAdaLookupStatus::STATES_SET_RESULT;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_INVALID_SCN) {
        cur_state = ObVidAdaLookupStatus::STATES_ERROR;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status.", K(ada_ctx.get_status()), K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_ROWKEY_VEC: {
      if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_LACK_SCN) {
        cur_state = ObVidAdaLookupStatus::QUERY_SNAPSHOT_TBL;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_INVALID_SCN) {
        cur_state = ObVidAdaLookupStatus::STATES_ERROR;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_COM_DATA) {
        cur_state = ObVidAdaLookupStatus::QUERY_ROWKEY_VEC;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status.", K(ada_ctx.get_status()), K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_INDEX_ID_TBL: {
      if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_WAIT) {
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_COM_DATA) {
        cur_state = ObVidAdaLookupStatus::QUERY_ROWKEY_VEC;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_LACK_SCN) {
        cur_state = ObVidAdaLookupStatus::QUERY_SNAPSHOT_TBL;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_OK) {
        cur_state = ObVidAdaLookupStatus::STATES_SET_RESULT;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_INVALID_SCN) {
        cur_state = ObVidAdaLookupStatus::STATES_ERROR;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status.", K(ada_ctx.get_status()), K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::QUERY_SNAPSHOT_TBL: {
      if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_WAIT) {
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_OK) {
        cur_state = ObVidAdaLookupStatus::STATES_SET_RESULT;
      } else if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_INVALID_SCN) {
        cur_state = ObVidAdaLookupStatus::STATES_ERROR;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status.", K(ada_ctx.get_status()), K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::STATES_SET_RESULT: {
      if (ada_ctx.get_status() == PluginVectorQueryResStatus::PVQ_REFRESH) {
        cur_state = ObVidAdaLookupStatus::STATES_REFRESH;
      } else {
        cur_state = ObVidAdaLookupStatus::STATES_FINISH;
      }
      break;
    }
    case ObVidAdaLookupStatus::STATES_REFRESH: {
      cur_state = ObVidAdaLookupStatus::STATES_FINISH;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status.", K(ada_ctx.get_status()), K(ret));
      break;
    }
  }
  return ret;
}

int ObDASVecIndexHNSWScanIter::get_ob_hnsw_ef_search(uint64_t &ob_hnsw_ef_search)
{
  int ret = OB_SUCCESS;
  const uint64_t OB_HNSW_EF_SEARCH_DEFAULT = 64;  // same as SYS_VAR_OB_HNSW_EF_SEARCH

  ObSQLSessionInfo *session = nullptr;
  if (OB_NOT_NULL(vec_index_scan_ctdef_) && vec_index_scan_ctdef_->query_param_.is_set_ef_search_) {
    ob_hnsw_ef_search =  vec_index_scan_ctdef_->query_param_.ef_search_;
    LOG_TRACE("use stmt ef_search", K(ob_hnsw_ef_search));
  } else if (OB_ISNULL(session = exec_ctx_->get_my_session())) {
    ob_hnsw_ef_search = OB_HNSW_EF_SEARCH_DEFAULT;
    LOG_WARN("session is null", K(ret), KP(exec_ctx_));
  } else if (OB_FAIL(session->get_ob_hnsw_ef_search(ob_hnsw_ef_search))) {
    LOG_WARN("failed to get ob hnsw ef search", K(ret));
  }

  return ret;
}

int64_t ObDASVecIndexHNSWScanIter::get_reorder_count(const int64_t ef_search, const int64_t topK, const ObVectorIndexParam& param)
{
  // max is ef_search
  const float refine_k = param.refine_k_;
  int64_t refine_cnt = refine_k * topK;
  if (refine_cnt < topK) refine_cnt = topK;
  LOG_TRACE("reorder count info", K(ef_search), K(topK), K(refine_k), K(refine_cnt));
  int64_t reorder_count = 0;
  if (is_hnsw_bq()) {
    reorder_count = OB_MIN(OB_MAX(topK, OB_MIN(refine_cnt, ef_search)), MAX_VSAG_QUERY_RES_SIZE);
  }
  return reorder_count;
}

int ObDASVecIndexHNSWScanIter::prepare_follower_query_cond(ObVectorQueryConditions &query_cond)
{
  int ret = OB_SUCCESS;
  /* snapshot_scan_param_.tablet_id_ will update in do_snapshot_table_scan
  * so snapshot_scan_param_.tablet_id_ != snapshot_tablet_id_ means current query_cond_.row_iter_ is not current snapshot_tablet_id_ iter,
  * that we should refresh */
  if ((query_cond_->row_iter_ != nullptr && snapshot_scan_param_.tablet_id_ != snapshot_tablet_id_)
      || OB_ISNULL(query_cond_->row_iter_)) {
    if (OB_FAIL(do_snapshot_table_scan())) {
      LOG_WARN("fail to do snapshot table scan", K(ret));
    } else if (OB_NOT_NULL(snapshot_iter_)) {
      query_cond_->row_iter_ = snapshot_iter_->get_output_result_iter();
    }
  }
  return ret;
}

int ObDASVecIndexHNSWScanIter::prepare_complete_vector_data(ObVectorQueryAdaptorResultContext& ada_ctx)
{
  int ret = OB_SUCCESS;

  ObObj *vids = nullptr;
  int64_t vec_cnt = ada_ctx.get_vec_cnt();

  if (OB_ISNULL(vids = ada_ctx.get_vids())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get vectors.", K(ret));
  }

  for (int i = 0; OB_SUCC(ret) && i < vec_cnt; i++) {
    ObRowkey vid(&(vids[i + ada_ctx.get_curr_idx()]), 1);
    ObRowkey *rowkey;
    ObString vector;
    vid.get_obj_ptr()->meta_.set_uint64();
    rowkey = &vid;

    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_vector_from_com_aux_vec_table(mem_context_->get_arena_allocator(), rowkey, vector))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get vector from com aux vec table.", K(ret));
        }
      }
    }

    if (ret == OB_ITER_END) {
      ada_ctx.set_vector(i, nullptr, 0);
      ret = OB_SUCCESS;
    } else if (OB_SUCC(ret)) {
      ada_ctx.set_vector(i, vector.ptr(), vector.length());
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObDASVecIndexHNSWScanIter::get_vector_from_com_aux_vec_table(ObIAllocator &allocator, ObRowkey *rowkey, ObString &vector)
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *com_aux_vec_ctdef = vec_index_scan_ctdef_->get_com_aux_vec_table_ctdef();

  if (!com_aux_vec_iter_first_scan_ && OB_FAIL(reuse_com_aux_vec_iter())) {
    LOG_WARN("failed to reuse com aux vec iter.", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(*rowkey, com_aux_vec_scan_param_, com_aux_vec_ctdef->ref_table_id_))) {
    LOG_WARN("failed to set lookup key", K(ret));
  } else if (OB_FAIL(do_com_aux_vec_table_scan())) {
    LOG_WARN("failed to do com aux vec table scan", K(ret));
  } else if (OB_FAIL(get_vector_from_com_aux_vec_table(allocator, vector))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to get vector from com aux vec table", K(ret));
  }

  return ret;
}

int ObDASVecIndexHNSWScanIter::get_vector_from_com_aux_vec_table(ObIAllocator &allocator, ObString &vector)
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *com_aux_vec_ctdef = vec_index_scan_ctdef_->get_com_aux_vec_table_ctdef();
  storage::ObTableScanIterator *table_scan_iter = dynamic_cast<storage::ObTableScanIterator *>(com_aux_vec_iter_->get_output_result_iter());
  blocksstable::ObDatumRow *datum_row = nullptr;

  com_aux_vec_iter_->clear_evaluated_flag();

  const int64_t INVALID_COLUMN_ID = -1;
  int64_t vec_col_idx = INVALID_COLUMN_ID;
  int output_row_cnt = com_aux_vec_ctdef->pd_expr_spec_.access_exprs_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < output_row_cnt; i++) {
    ObExpr *expr = com_aux_vec_ctdef->pd_expr_spec_.access_exprs_.at(i);
    if (T_REF_COLUMN == expr->type_) {
      if (vec_col_idx == INVALID_COLUMN_ID) {
        vec_col_idx = i;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("already get vec col idx.", K(ret), K(vec_col_idx), K(i));
      }
    }
  }

  if (vec_col_idx == INVALID_COLUMN_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get vec col idx.", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to scan com aux vec iter", K(ret));
    }
  } else if (datum_row->get_column_count() != output_row_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
  } else if (OB_FALSE_IT(vector = datum_row->storage_datums_[vec_col_idx].get_string())) {
    LOG_WARN("failed to get vid.", K(ret));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator,
                                                                ObLongTextType,
                                                                CS_TYPE_BINARY,
                                                                com_aux_vec_ctdef->result_output_.at(0)->obj_meta_.has_lob_header(),
                                                                vector))) {
    LOG_WARN("failed to get real data.", K(ret));
  }

  return ret;
}

int ObDASVecIndexHNSWScanIter::do_delta_buf_table_scan()
{
  const ObDASScanCtDef *delta_buf_ctdef = vec_index_scan_ctdef_->get_delta_buf_table_ctdef();
  ObDASScanRtDef *delta_buf_rtdef = vec_index_scan_rtdef_->get_delta_buf_table_rtdef();

  return do_aux_table_scan_need_reuse(delta_buf_iter_first_scan_,
                                      delta_buf_scan_param_,
                                      delta_buf_ctdef,
                                      delta_buf_rtdef,
                                      delta_buf_iter_,
                                      delta_buf_tablet_id_);
}

int ObDASVecIndexHNSWScanIter::do_index_id_table_scan()
{
  const ObDASScanCtDef *index_id_ctdef = vec_index_scan_ctdef_->get_index_id_table_ctdef();
  ObDASScanRtDef *index_id_rtdef = vec_index_scan_rtdef_->get_index_id_table_rtdef();

  return do_aux_table_scan_need_reuse(index_id_iter_first_scan_,
                                      index_id_scan_param_,
                                      index_id_ctdef,
                                      index_id_rtdef,
                                      index_id_iter_,
                                      index_id_tablet_id_);
}

int ObDASVecIndexHNSWScanIter::do_snapshot_table_scan()
{
  const ObDASScanCtDef *snapshot_ctdef = vec_index_scan_ctdef_->get_snapshot_table_ctdef();
  ObDASScanRtDef *snapshot_rtdef = vec_index_scan_rtdef_->get_snapshot_table_rtdef();

  return do_snapshot_table_scan_need_reuse(snapshot_iter_first_scan_,
                                          snapshot_scan_param_,
                                          snapshot_ctdef,
                                          snapshot_rtdef,
                                          snapshot_iter_,
                                          snapshot_tablet_id_);
}

int ObDASVecIndexHNSWScanIter::do_com_aux_vec_table_scan()
{
  int ret = OB_SUCCESS;

  if (com_aux_vec_iter_first_scan_) {
    const ObDASScanCtDef *com_aux_tbl_ctdef = vec_index_scan_ctdef_->get_com_aux_vec_table_ctdef();
    ObDASScanRtDef *com_aux_tbl_rtdef = vec_index_scan_rtdef_->get_com_aux_vec_table_rtdef();
    if (OB_FAIL(ObDasVecScanUtils::init_vec_aux_scan_param(ls_id_,
                                                          com_aux_vec_tablet_id_,
                                                          com_aux_tbl_ctdef,
                                                          com_aux_tbl_rtdef,
                                                          tx_desc_,
                                                          snapshot_,
                                                          com_aux_vec_scan_param_,
                                                          true/*is_get*/))) {
      LOG_WARN("failed to init com aux vec scan param", K(ret));
    } else if (OB_FALSE_IT(com_aux_vec_iter_->set_scan_param(com_aux_vec_scan_param_))) {
    } else if (OB_FAIL(com_aux_vec_iter_->do_table_scan())) {
      LOG_WARN("failed to do com aux vec iter scan", K(ret));
    } else {
      com_aux_vec_iter_first_scan_ = false;
    }
  } else {
    if (OB_FAIL(com_aux_vec_iter_->rescan())) {
      LOG_WARN("failed to rescan com aux vec table scan iterator.", K(ret));
    }
  }
  return ret;
}

int ObDASVecIndexHNSWScanIter::do_snapshot_table_scan_need_reuse(bool &first_scan,
                                                        ObTableScanParam &scan_param,
                                                        const ObDASScanCtDef *ctdef,
                                                        ObDASScanRtDef *rtdef,
                                                        ObDASScanIter *iter,
                                                        ObTabletID &tablet_id,
                                                        bool is_get)
{
  int ret = OB_SUCCESS;
  bool need_reverse = false;
  blocksstable::ObDatumRow *unused_row = nullptr;
  if (OB_FAIL(do_aux_table_scan_need_reuse(first_scan, scan_param, ctdef, rtdef, iter, tablet_id, is_get))) {
    LOG_WARN("fail to do aux table scan", K(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::check_snapshot_iter_need_rescan(iter->get_output_result_iter(), need_reverse, unused_row))) {
    LOG_WARN("fail to check snapshot iter need rescan", K(ret));
  } else if (need_reverse) {
    LOG_WARN("do snapshot table scan need rescan", K(scan_param.scan_flag_.scan_order_));
    if (OB_FAIL(ObPluginVectorIndexUtils::release_row_scan_iter(iter->get_output_result_iter()))) {
      LOG_WARN("fail to release row scan iter", K(ret));
    } else if (FALSE_IT(first_scan = true)) {
    } else if (OB_FAIL(do_aux_table_scan_need_reuse(first_scan, scan_param, ctdef, rtdef, iter, tablet_id, is_get, need_reverse))) {
      LOG_WARN("fail to do aux table scan", K(ret));
    }
  } else if (OB_FAIL(do_aux_table_scan_need_reuse(first_scan, scan_param, ctdef, rtdef, iter, tablet_id, is_get, need_reverse))) {
    LOG_WARN("fail to do aux table scan", K(ret));
  }
  return ret;
}

int ObDASVecIndexHNSWScanIter::do_aux_table_scan_need_reuse(bool &first_scan,
                                                    ObTableScanParam &scan_param,
                                                    const ObDASScanCtDef *ctdef,
                                                    ObDASScanRtDef *rtdef,
                                                    ObDASScanIter *iter,
                                                    ObTabletID &tablet_id,
                                                    bool is_get,
                                                    bool need_reverse)
{
  int ret = OB_SUCCESS;

  ObNewRange scan_range;
  if (first_scan) {
    if (OB_FAIL(ObDasVecScanUtils::init_vec_aux_scan_param(ls_id_, tablet_id, ctdef, rtdef,tx_desc_, snapshot_, scan_param, is_get))) {
      LOG_WARN("failed to init scan param", K(ret));
    } else if (OB_FALSE_IT(ObDasVecScanUtils::set_whole_range(scan_range, ctdef->ref_table_id_))) {
      LOG_WARN("failed to generate init scan range", K(ret));
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to append scan range", K(ret));
    } else if (need_reverse && FALSE_IT(scan_param.scan_flag_.scan_order_ = ObQueryFlag::Reverse)) {
    } else if (OB_FALSE_IT(iter->set_scan_param(scan_param))) {
    } else if (OB_FAIL(iter->do_table_scan())) {
      LOG_WARN("failed to do scan", K(ret));
    } else {
      first_scan = false;
    }
  } else {
    if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, iter, scan_param, tablet_id))) {
      LOG_WARN("failed to reuse scan iterator", K(ret));
    } else if (OB_FALSE_IT(ObDasVecScanUtils::set_whole_range(scan_range, ctdef->ref_table_id_))) {
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to append scan range", K(ret));
    } else if (OB_FAIL(iter->rescan())) {
      LOG_WARN("failed to rescan scan iterator.", K(ret));
    }
  }
  return ret;
}

int ObDASVecIndexHNSWScanIter::do_aux_table_scan(bool &first_scan,
                                        ObTableScanParam &scan_param,
                                        const ObDASScanCtDef *ctdef,
                                        ObDASScanRtDef *rtdef,
                                        ObDASScanIter *iter,
                                        ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  if (first_scan) {
    scan_param.need_switch_param_ = false;
    if (OB_FAIL(
            ObDasVecScanUtils::init_scan_param(ls_id_, tablet_id, ctdef, rtdef, tx_desc_, snapshot_, scan_param))) {
      LOG_WARN("failed to init scan param", K(ret));
    } else if (OB_FALSE_IT(iter->set_scan_param(scan_param))) {
    } else if (OB_FAIL(iter->do_table_scan())) {
      LOG_WARN("failed to do scan", K(ret));
    } else {
      first_scan = false;
    }
  } else {
    if (OB_FAIL(iter->rescan())) {
      LOG_WARN("failed to rescan scan iterator.", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
