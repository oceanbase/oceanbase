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
#include "sql/das/iter/ob_das_hnsw_scan_iter.h"
#include "sql/das/ob_das_scan_op.h"
#include "storage/tx_storage/ob_access_service.h"
#include "src/storage/access/ob_table_scan_iterator.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace transaction;
using namespace share;

namespace sql
{

int ObDASHNSWScanIter::do_table_scan()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(inv_idx_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inv idx scan iter is null", K(ret));
  } else if (OB_FAIL(inv_idx_scan_iter_->do_table_scan())) {
    LOG_WARN("failed to do inv idx table scan.", K(ret));
  }

  return ret;
}

int ObDASHNSWScanIter::rescan()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->rescan())) {
    LOG_WARN("failed to rescan inv idx scan iter", K(ret));
  }

  return ret;
}

void ObDASHNSWScanIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(inv_idx_scan_iter_)) {
    inv_idx_scan_iter_->clear_evaluated_flag();
  }
}

int ObDASHNSWScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ObDASIterType::DAS_ITER_HNSW_SCAN != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for hnsw scan iter", K(ret), K(param));
  } else {
    ObDASHNSWScanIterParam &hnsw_scan_param = static_cast<ObDASHNSWScanIterParam &>(param);
    ls_id_ = hnsw_scan_param.ls_id_;
    tx_desc_ = hnsw_scan_param.tx_desc_;
    snapshot_ = hnsw_scan_param.snapshot_;

    inv_idx_scan_iter_ = hnsw_scan_param.inv_idx_scan_iter_;
    delta_buf_iter_ = hnsw_scan_param.delta_buf_iter_;
    index_id_iter_ = hnsw_scan_param.index_id_iter_;
    snapshot_iter_ = hnsw_scan_param.snapshot_iter_;
    vid_rowkey_iter_ = hnsw_scan_param.vid_rowkey_iter_;
    com_aux_vec_iter_ = hnsw_scan_param.com_aux_vec_iter_;
    rowkey_vid_iter_ = hnsw_scan_param.rowkey_vid_iter_;

    vec_aux_ctdef_ = hnsw_scan_param.vec_aux_ctdef_;
    vec_aux_rtdef_ = hnsw_scan_param.vec_aux_rtdef_;
    vid_rowkey_ctdef_ = hnsw_scan_param.vid_rowkey_ctdef_;
    vid_rowkey_rtdef_ = hnsw_scan_param.vid_rowkey_rtdef_;
    sort_ctdef_ = hnsw_scan_param.sort_ctdef_;
    sort_rtdef_ = hnsw_scan_param.sort_rtdef_;

    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "HNSW", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create vector hnsw memory context", K(ret));
      }
    }

    if (OB_NOT_NULL(sort_ctdef_) && OB_NOT_NULL(sort_rtdef_)) {
      if (OB_FAIL(ObDasVecScanUtils::init_limit(vec_aux_ctdef_, vec_aux_rtdef_, sort_ctdef_, sort_rtdef_, limit_param_))) {
        LOG_WARN("failed to init limit", K(ret), KPC(vec_aux_ctdef_), KPC(vec_aux_rtdef_));
      } else if (limit_param_.offset_ + limit_param_.limit_ > MAX_VSAG_QUERY_RES_SIZE) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_WARN(OB_NOT_SUPPORTED, "query size (limit + offset) is more than 16384");
      } else if (OB_FAIL(ObDasVecScanUtils::init_sort(
                     vec_aux_ctdef_, vec_aux_rtdef_, sort_ctdef_, sort_rtdef_, limit_param_, search_vec_, distance_calc_))) {
        LOG_WARN("failed to init sort", K(ret), KPC(vec_aux_ctdef_), KPC(vec_aux_rtdef_));
      } else if (OB_FAIL(set_vec_index_param(vec_aux_ctdef_->vec_index_param_))) {
        LOG_WARN("failed to set vec index param", K(ret));
      }
    }

    dim_ = vec_aux_ctdef_->dim_;
    selectivity_ = vec_aux_ctdef_->selectivity_;
  }

  return ret;
}

int ObDASHNSWScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->reuse())) {
    LOG_WARN("failed to reuse inv idx scan iter", K(ret));
  } else if (!com_aux_vec_iter_first_scan_ && OB_FAIL(reuse_com_aux_vec_iter())) {
    LOG_WARN("failed to reuse com aux vec iter", K(ret));
  } else if (!rowkey_vid_iter_first_scan_ && OB_FAIL(reuse_rowkey_vid_iter())) {
    LOG_WARN("failed to reuse rowkey vid iter", K(ret));
  } else if (!vid_rowkey_iter_first_scan_ && OB_FAIL(reuse_vid_rowkey_iter())) {
    LOG_WARN("failed to reuse vid rowkey iter", K(ret));
  }

  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }
  vec_op_alloc_.reset();
  saved_rowkeys_.reset();

  if (OB_SUCC(ret) && OB_FAIL(set_vec_index_param(vec_aux_ctdef_->vec_index_param_))) {
    LOG_WARN("failed to set vec index param", K(ret));
  }

  return ret;
}

int ObDASHNSWScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->release())) {
    LOG_WARN("failed to release inv idx scan iter", K(ret));
    tmp_ret = ret;
    ret = OB_SUCCESS;
  }
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
  if (OB_NOT_NULL(vid_rowkey_iter_) && OB_FAIL(vid_rowkey_iter_->release())) {
    LOG_WARN("failed to release vid rowkey iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(com_aux_vec_iter_) && OB_FAIL(com_aux_vec_iter_->release())) {
    LOG_WARN("failed to release com aux vec iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(rowkey_vid_iter_) && OB_FAIL(rowkey_vid_iter_->release())) {
    LOG_WARN("failed to release rowkey vid iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }

  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  inv_idx_scan_iter_ = nullptr;
  delta_buf_iter_ = nullptr;
  index_id_iter_ = nullptr;
  snapshot_iter_ = nullptr;
  vid_rowkey_iter_ = nullptr;
  com_aux_vec_iter_ = nullptr;
  rowkey_vid_iter_ = nullptr;

  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }

  saved_rowkeys_.reset();
  tx_desc_ = nullptr;
  snapshot_ = nullptr;

  delta_buf_scan_param_.destroy_schema_guard();
  delta_buf_scan_param_.snapshot_.reset();
  delta_buf_scan_param_.destroy();
  index_id_scan_param_.destroy_schema_guard();
  index_id_scan_param_.snapshot_.reset();
  index_id_scan_param_.destroy();
  snapshot_scan_param_.destroy_schema_guard();
  snapshot_scan_param_.snapshot_.reset();
  snapshot_scan_param_.destroy();
  vid_rowkey_scan_param_.destroy_schema_guard();
  vid_rowkey_scan_param_.snapshot_.reset();
  vid_rowkey_scan_param_.destroy();
  com_aux_vec_scan_param_.destroy_schema_guard();
  com_aux_vec_scan_param_.snapshot_.reset();
  com_aux_vec_scan_param_.destroy();
  rowkey_vid_scan_param_.destroy_schema_guard();
  rowkey_vid_scan_param_.snapshot_.reset();
  rowkey_vid_scan_param_.destroy();

  vec_aux_ctdef_ = nullptr;
  vec_aux_rtdef_ = nullptr;
  vid_rowkey_ctdef_ = nullptr;
  vid_rowkey_rtdef_ = nullptr;
  sort_ctdef_ = nullptr;
  sort_rtdef_ = nullptr;
  search_vec_ = nullptr;
  distance_calc_ = nullptr;

  if (nullptr != mem_context_)  {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  vec_op_alloc_.reset();

  return ret;
}

uint64_t ObDASHNSWScanIter::adjust_batch_count(bool is_vectored, uint64_t batch_count)
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

int ObDASHNSWScanIter::save_distance_expr_result(const ObObj& dist_obj)
{
  int ret = OB_SUCCESS;
  if (need_save_distance_result()) {
    ObDatum &distance = distance_calc_->locate_datum_for_write(*sort_rtdef_->eval_ctx_);
    ObEvalInfo &eval_info = distance_calc_->get_eval_info(*sort_rtdef_->eval_ctx_);

    distance.set_double(dist_obj.get_float());
    eval_info.evaluated_ = true;
    eval_info.projected_ = true;
  }
  return ret;
}

int ObDASHNSWScanIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;

  if (limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(adaptor_vid_iter_)) {
    if (OB_FAIL(process_adaptor_state(false))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to process adaptor state", K(ret));
      }
    }
  }

  ObNewRow *row = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(adaptor_vid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get adaptor vid iter", K(ret));
  } else if (OB_FAIL(adaptor_vid_iter_->get_next_row(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next next row from adaptor vid iter", K(ret));
    }
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be null", K(ret));
  } else if (row->get_count() != 1 && row->get_count() != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be one row", K(row->get_count()), K(ret));
  } else {
    ObExpr *vid_expr = vec_aux_ctdef_->inv_scan_vec_id_col_;
    ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
    guard.set_batch_idx(0);
    ObDatum &vid_datum = vid_expr->locate_datum_for_write(*vec_aux_rtdef_->eval_ctx_);
    if (OB_FAIL(vid_datum.from_obj(row->get_cell(0)))) {
      LOG_WARN("failed to from obj", K(ret));
    // for distance calc optimize, needn't calc again
    } else if (need_save_distance_result()
      && OB_FAIL(save_distance_expr_result(row->get_cell(1)))) {
      LOG_WARN("failed to set distance", K(ret));
    }
  }

  return ret;
}

int ObDASHNSWScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(adaptor_vid_iter_)) {
    if (OB_FAIL(process_adaptor_state(true))) {
      LOG_WARN("failed to process adaptor state", K(ret));
    }
  }

  ObNewRow *row = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(adaptor_vid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get adaptor vid iter", K(ret));
  } else if (OB_FALSE_IT(adaptor_vid_iter_->set_batch_size(capacity))) {
  } else if (OB_FAIL(adaptor_vid_iter_->get_next_rows(row, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next next row from adaptor vid iter", K(ret));
    }
  } else if (count > 0) {
    ObExpr *vid_expr = vec_aux_ctdef_->inv_scan_vec_id_col_;
    ObDatum *vid_datum = nullptr;
    ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
    guard.set_batch_size(count);
    if (OB_ISNULL(vid_datum = vid_expr->locate_datums_for_update(*vec_aux_rtdef_->eval_ctx_, count))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, datums is nullptr", K(ret), KPC(vid_expr));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        guard.set_batch_idx(i);
        vid_datum[i].set_int(row->get_cell(i * 2).get_int());
      }
      if (OB_SUCC(ret)) {
        vid_expr->set_evaluated_projected(*vec_aux_rtdef_->eval_ctx_);
      }
    }

    // for distance calc optimize, needn't calc again
    if (OB_SUCC(ret) && OB_FAIL(save_distance_expr_result(row, count))) {
      LOG_WARN("failed to set distance expr result.", K(ret));
    }
  }

  return ret;
}

int ObDASHNSWScanIter::save_distance_expr_result(ObNewRow *row, int64_t size)
{
  int ret = OB_SUCCESS;

  if (need_save_distance_result()) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*sort_rtdef_->eval_ctx_);
    batch_info_guard.set_batch_size(size);

    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      ObObj& dist_obj = row->get_cell(i * 2 + 1);
      batch_info_guard.set_batch_idx(i);

      ObDatum& wr_datum = distance_calc_->locate_datum_for_write(*sort_rtdef_->eval_ctx_);
      wr_datum.set_double(dist_obj.get_float());

      ObEvalInfo &eval_info = distance_calc_->get_eval_info(*sort_rtdef_->eval_ctx_);
      eval_info.evaluated_ = true;
      eval_info.projected_ = true;
    }
  }

  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
  if (vec_aux_ctdef_->is_post_filter()) {
    if (OB_FAIL(process_adaptor_state_hnsw(allocator, is_vectorized))) {
      LOG_WARN("failed to process adaptor state hnsw", K(ret));
    }
  } else if (vec_aux_ctdef_->is_pre_filter()) {
    uint64_t rowkey_count = 0;
    while (OB_SUCC(ret) && rowkey_count < MAX_BRUTE_FORCE_SIZE) {
      ObRowkey rowkey;
      if (OB_FAIL(get_next_single_row(is_vectorized))) {
      } else if (OB_FALSE_IT(rowkey_count++)) {
      } else if (OB_FAIL(get_rowkey(allocator, rowkey))) {
        LOG_WARN("failed to get rowkey", K(ret));
      } else if (OB_FAIL(saved_rowkeys_.push_back(rowkey))) {
        LOG_WARN("store lookup key range failed", K(ret), K(com_aux_vec_scan_param_));
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next rowkey", K(ret));
      } else if (rowkey_count == 0) {
      } else {
        ret = OB_SUCCESS;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (rowkey_count < MAX_BRUTE_FORCE_SIZE) {
      if (OB_FAIL(process_adaptor_state_brute_force(allocator, is_vectorized))) {
        LOG_WARN("failed to process adaptor state brute force", K(ret));
      }
    } else {
      if (OB_FAIL(process_adaptor_state_hnsw(allocator, is_vectorized))) {
        LOG_WARN("failed to process adaptor state hnsw", K(ret));
      }
    }

    if (nullptr != mem_context_) {
      mem_context_->reset_remain_one_page();
    }
  }

  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state_brute_force(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;

  ObString search_vec;
  if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(mem_context_->get_arena_allocator(), sort_rtdef_, search_vec_, search_vec))) {
    LOG_WARN("failed to get real search vec", K(ret));
  }

  ObExprVectorDistance::ObVecDisType dis_type = ObExprVectorDistance::ObVecDisType::MAX_TYPE;
  if (OB_SUCC(ret) && ObDasVecScanUtils::get_distance_expr_type(*sort_ctdef_->sort_exprs_[0], *sort_rtdef_->eval_ctx_, dis_type)) {
    LOG_WARN("failed to get distance type.", K(ret));
  }

  // init heap
  uint64_t limit = limit_param_.limit_ + limit_param_.offset_;
  uint64_t saved_rowkey_count = saved_rowkeys_.count();
  uint64_t capaticy = limit > saved_rowkey_count ? saved_rowkey_count : limit;
  ObSimpleMaxHeap max_heap(&allocator, capaticy);
  if (OB_SUCC(ret) && OB_FAIL(max_heap.init())) {
    LOG_WARN("failed to init max heap.", K(ret));
  }

  if (OB_SUCC(ret)) {
    // get vector by rowkey, calc distance and push to min heap
    const ObDASScanCtDef *com_aux_tbl_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_com_aux_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
    for (int64_t i = 0; OB_SUCC(ret) && i < saved_rowkey_count; ++i) {
      ObString vector;
      if (OB_FAIL(get_vector_from_com_aux_vec_table(allocator, saved_rowkeys_[i], vector))) {
        LOG_WARN("failed to get vector from com aux vec table.", K(ret), K(i));
      } else if (OB_ISNULL(vector.ptr())) {
        // compatible with VSAG, ignoring null vector.
      } else {
        double distance = 0.0;
        const float *data_l = reinterpret_cast<const float*>(search_vec.ptr());
        const float *data_r = reinterpret_cast<const float*>(vector.ptr());
        if (OB_FAIL(ObExprVectorDistance::distance_funcs[dis_type](data_l, data_r, dim_, distance))) {
          LOG_WARN("failed to get distance type", K(ret));
        } else {
          distance = sort_ctdef_->sort_exprs_[0]->type_ == T_FUN_SYS_NEGATIVE_INNER_PRODUCT ? -1 * distance : distance;
          max_heap.push(saved_rowkeys_[i], distance);
        }
      }
    }

    if (OB_SUCC(ret)) {
      max_heap.max_heap_sort();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (max_heap.get_size() == 0) {
    ret = OB_ITER_END;
  }

  uint64_t vid_count = max_heap.get_size();
  int64_t *vids = nullptr;
  float *distances = nullptr;
  if (OB_SUCC(ret)) {
    // get vid by top k rowkey from rowkey-vid table.
    if (OB_ISNULL(vids = static_cast<int64_t *>(vec_op_alloc_.alloc(sizeof(int64_t) * vid_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator vids", K(ret), K(vid_count));
    } else if (OB_ISNULL(distances = static_cast<float *>(vec_op_alloc_.alloc(sizeof(float) * vid_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator distances", K(ret), K(vid_count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < vid_count; ++i) {
        ObRowkey *rowkey;
        int64_t vid = 0;
        distances[i] = max_heap.value_at(i);
        if (OB_ISNULL(rowkey = max_heap.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get rowkey from max heap.", K(ret));
        } else if (OB_FAIL(get_vid_from_rowkey_vid_table(*rowkey, vid))) {
          LOG_WARN("failed to get vid from rowkey vid table.", K(ret));
        } else {
          vids[i] = vid;
        }
      }

      if (OB_ITER_END == ret){
        ret = OB_SUCCESS;
      }
    }
  }

  // init adaptor_vid_iter_ by vids.
  if (OB_SUCC(ret)) {
    void *iter_buff = nullptr;
    if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryVidIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator adaptor vid iter.", K(ret));
    } else if (OB_FALSE_IT(adaptor_vid_iter_ = new(iter_buff) ObVectorQueryVidIterator())) {
    } else if (OB_FAIL(adaptor_vid_iter_->init(vid_count, vids, distances, &vec_op_alloc_))) {
      LOG_WARN("iter init failed.", K(ret));
    }
  }

  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state_hnsw(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;

  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  ObVectorQueryAdaptorResultContext ada_ctx(MTL_ID(), &vec_op_alloc_, &allocator);
  share::ObVectorIndexAcquireCtx index_ctx;
  ObPluginVectorIndexAdapterGuard adaptor_guard;
  index_ctx.inc_tablet_id_ = delta_buf_tablet_id_;
  index_ctx.vbitmap_tablet_id_ = index_id_tablet_id_;
  index_ctx.snapshot_tablet_id_ = snapshot_tablet_id_;
  index_ctx.data_tablet_id_ = com_aux_vec_tablet_id_;

  if (OB_FAIL(vec_index_service->acquire_adapter_guard(ls_id_, index_ctx, adaptor_guard, &vec_index_param_, dim_))) {
    LOG_WARN("failed to get ObMockPluginVectorIndexAdapter", K(ret));
  } else {
    share::ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    if (OB_ISNULL(adaptor)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("shouldn't be null.", K(ret));
    } else if (vec_aux_ctdef_->is_pre_filter() && OB_FAIL(process_adaptor_state_pre_filter(&ada_ctx, adaptor, is_vectorized))) {
      LOG_WARN("hnsw pre filter failed to query result.", K(ret));
    } else if (vec_aux_ctdef_->is_post_filter() && OB_FAIL(process_adaptor_state_post_filter(&ada_ctx, adaptor))) {
      LOG_WARN("hnsw post filter failed to query result.", K(ret));
    }
  }

  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state_pre_filter(
    ObVectorQueryAdaptorResultContext *ada_ctx,
    ObPluginVectorIndexAdaptor* adaptor,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ada_ctx) || OB_ISNULL(adaptor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shouldn't be null.", K(ret), K(ada_ctx), K(adaptor));
  } else {
    if (!ada_ctx->is_bitmaps_valid(true/*is_extra*/) && OB_FAIL(ada_ctx->init_bitmaps(true/*is_extra*/))) {
        LOG_WARN("init bitmaps failed.", K(ret));
    } else {
      uint64_t saved_rowkey_cnt = saved_rowkeys_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < saved_rowkey_cnt; ++i) {
        int64_t vid = 0;
        if (OB_FAIL(get_vid_from_rowkey_vid_table(saved_rowkeys_[i], vid))) {
          LOG_WARN("failed to get vid from rowkey vid table.", K(ret));
        } else if (OB_FAIL(adaptor->add_extra_valid_vid(ada_ctx, vid))) {
          LOG_WARN("failed to add valid vid", K(ret));
        }
      }

      while (OB_SUCC(ret)) {
        if (OB_FAIL(get_next_single_row(is_vectorized))) {
        } else {
          ObRowkey rowkey;
          int64_t vid = 0;
          if (OB_FAIL(get_rowkey(*ada_ctx->get_tmp_allocator(), rowkey))) {
            LOG_WARN("failed to get rowkey", K(ret));
          } else if (OB_FAIL(get_vid_from_rowkey_vid_table(rowkey, vid))) {
            LOG_WARN("failed to get vid from rowkey vid table.", K(ret));
          } else if (OB_FAIL(adaptor->add_extra_valid_vid(ada_ctx, vid))) {
            LOG_WARN("failed to add valid vid", K(ret));
          }
        }
      }
    }
  }

  if (OB_ITER_END != ret) {
    LOG_WARN("get next row failed.", K(ret));
  } else {
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(ret)) {
  } else if (adaptor->check_if_complete_data(ada_ctx)) {
    if (OB_FAIL(process_adaptor_state_post_filter(ada_ctx, adaptor))) {
      LOG_WARN("failed to process adaptor state post filter", K(ret));
    }
  } else {
    if (OB_FAIL(adaptor->set_adaptor_ctx_flag(ada_ctx))) {
      LOG_WARN("failed to set adaptor ctx flag", K(ret));
    } else if (PVQP_SECOND == ada_ctx->get_flag()) {
      if (OB_FAIL(do_snapshot_table_scan())) {
        LOG_WARN("failed to do snapshot table scan.", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObVectorQueryConditions query_cond;
      if (OB_FAIL(set_vector_query_condition(query_cond))) {
        LOG_WARN("failed to set query condition.", K(ret));
      } else if (OB_FAIL(adaptor->query_result(ada_ctx, &query_cond, adaptor_vid_iter_))) {
        LOG_WARN("failed to query result.", K(ret));
      }
    }
  }

  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state_post_filter(
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

int ObDASHNSWScanIter::get_next_single_row(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  inv_idx_scan_iter_->clear_evaluated_flag();
  if (is_vectorized) {
    int64_t scan_row_cnt = 0;
    ret = inv_idx_scan_iter_->get_next_rows(scan_row_cnt, 1);
  } else {
    ret = inv_idx_scan_iter_->get_next_row();
  }

  return ret;
}

int ObDASHNSWScanIter::prepare_state(const ObVidAdaLookupStatus& cur_state, ObVectorQueryAdaptorResultContext &ada_ctx)
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

int ObDASHNSWScanIter::call_pva_interface(const ObVidAdaLookupStatus& cur_state,
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
      ObVectorQueryConditions query_cond;
      if (OB_FAIL(set_vector_query_condition(query_cond))) {
        LOG_WARN("failed to set query condition.", K(ret));
      } else if (OB_FAIL(adaptor.query_result(&ada_ctx, &query_cond, adaptor_vid_iter_))) {
        LOG_WARN("failed to query result.", K(ret));
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

int ObDASHNSWScanIter::next_state(ObVidAdaLookupStatus& cur_state, ObVectorQueryAdaptorResultContext& ada_ctx)
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

int ObDASHNSWScanIter::get_ob_hnsw_ef_search(uint64_t &ob_hnsw_ef_search)
{
  int ret = OB_SUCCESS;
  const uint64_t OB_HNSW_EF_SEARCH_DEFAULT = 64;  // same as SYS_VAR_OB_HNSW_EF_SEARCH

  ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(session = exec_ctx_->get_my_session())) {
    ob_hnsw_ef_search = OB_HNSW_EF_SEARCH_DEFAULT;
    LOG_WARN("session is null", K(ret), KP(exec_ctx_));
  } else if (OB_FAIL(session->get_ob_hnsw_ef_search(ob_hnsw_ef_search))) {
    LOG_WARN("failed to get ob hnsw ef search", K(ret));
  }

  return ret;
}

int ObDASHNSWScanIter::set_vector_query_condition(ObVectorQueryConditions &query_cond)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(search_vec_) || OB_ISNULL(sort_rtdef_) || OB_ISNULL(sort_rtdef_->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null.", K(ret));
  } else {
    query_cond.query_order_ = true;
    query_cond.row_iter_ = snapshot_iter_->get_output_result_iter();
    query_cond.query_scn_ = snapshot_scan_param_.snapshot_.core_.version_;

    uint64_t ob_hnsw_ef_search = 0;
    if (OB_FAIL(get_ob_hnsw_ef_search(ob_hnsw_ef_search))) {
      LOG_WARN("failed to get ob hnsw ef search", K(ret), K(ob_hnsw_ef_search));
    } else if (OB_FALSE_IT(query_cond.ef_search_ = ob_hnsw_ef_search)) {
    } else {
      uint64_t real_limit = limit_param_.limit_ + limit_param_.offset_;
      // if selectivity_ == 1 means there is no filter
      if (vec_aux_ctdef_->is_post_filter() && (selectivity_ != 1 && selectivity_ != 0)) {
        real_limit = real_limit * 2 > ob_hnsw_ef_search ? real_limit * 2 : ob_hnsw_ef_search;
        real_limit = real_limit > MAX_VSAG_QUERY_RES_SIZE ? MAX_VSAG_QUERY_RES_SIZE : real_limit;
      }
      query_cond.query_limit_ = real_limit;
    }

    ObDatum *vec_datum = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(OB_FAIL(search_vec_->eval(*(sort_rtdef_->eval_ctx_), vec_datum)))) {
      LOG_WARN("eval vec arg failed", K(ret));
    } else if (OB_FALSE_IT(query_cond.query_vector_ = vec_datum->get_string())) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&mem_context_->get_arena_allocator(),
                                                                 ObLongTextType,
                                                                 CS_TYPE_BINARY,
                                                                 search_vec_->obj_meta_.has_lob_header(),
                                                                 query_cond.query_vector_))) {
      LOG_WARN("failed to get real data.", K(ret));
    }
  }
  return ret;
}

int ObDASHNSWScanIter::prepare_complete_vector_data(ObVectorQueryAdaptorResultContext& ada_ctx)
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
    ObRowkey rowkey;
    ObString vector;
    if (OB_FAIL(get_rowkey_from_vid_rowkey_table(mem_context_->get_arena_allocator(), vid, rowkey))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get rowkey.", K(ret));
      } else {
        ada_ctx.set_vector(i, nullptr, 0);
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(get_vector_from_com_aux_vec_table(mem_context_->get_arena_allocator(), rowkey, vector))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get vector from com aux vec table.", K(ret));
      } else {
        ada_ctx.set_vector(i, nullptr, 0);
        ret = OB_SUCCESS;
      }
    } else {
      ada_ctx.set_vector(i, vector.ptr(), vector.length());
    }
  }

  LOG_INFO("SYCN_DELTA_query_data", K(ada_ctx.get_vec_cnt()), K(ada_ctx.get_curr_idx()));

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObDASHNSWScanIter::get_vid_from_rowkey_vid_table(ObRowkey& rowkey, int64_t &vid)
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *rowkey_vid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
  ObDASScanRtDef *rowkey_vid_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx());

  if (OB_ISNULL(rowkey_vid_ctdef) || OB_ISNULL(rowkey_vid_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey_vid_ctdef or rowkey_vid_rtdef is null.", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(rowkey, rowkey_vid_scan_param_, rowkey_vid_ctdef->ref_table_id_))) {
    LOG_WARN("failed to set rowkey.", K(ret));
  } else if (OB_FAIL(do_aux_table_scan(rowkey_vid_iter_first_scan_, rowkey_vid_scan_param_, rowkey_vid_ctdef, rowkey_vid_rtdef, rowkey_vid_iter_, rowkey_vid_tablet_id_))) {
    LOG_WARN("do do aux table scan failed", K(ret));
  } else if (OB_FALSE_IT(rowkey_vid_iter_->clear_evaluated_flag())) {
  } else if (OB_FAIL(rowkey_vid_iter_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to scan rowkey vid iter", K(ret));
    }
  } else {
    ObExpr *vid_expr = vec_aux_ctdef_->inv_scan_vec_id_col_;
    ObDatum &vid_datum = vid_expr->locate_expr_datum(*rowkey_vid_rtdef->eval_ctx_);
    vid = vid_datum.get_int();
  }

  int tmp_ret = ret;
  if (OB_FAIL(reuse_rowkey_vid_iter())) {
    LOG_WARN("failed to reuse rowkey vid iter.", K(ret));
  } else {
    ret = tmp_ret;
  }

  return ret;
}

int ObDASHNSWScanIter::get_rowkey_from_vid_rowkey_table(ObIAllocator &allocator, ObRowkey& vid, ObRowkey& rowkey)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(vid, vid_rowkey_scan_param_, vid_rowkey_ctdef_->ref_table_id_))) {
    LOG_WARN("failed to set vid.", K(ret));
  } else if (OB_FAIL(do_aux_table_scan(vid_rowkey_iter_first_scan_, vid_rowkey_scan_param_, vid_rowkey_ctdef_, vid_rowkey_rtdef_, vid_rowkey_iter_, vid_rowkey_tablet_id_))) {
    LOG_WARN("failed to do vid rowkey scan", K(ret));
  } else if (OB_FALSE_IT(vid_rowkey_iter_->clear_evaluated_flag())) {
  } else if (OB_FAIL(vid_rowkey_iter_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to scan vid rowkey iter", K(vid), K(ret));
    }
  } else if (OB_FAIL(get_rowkey(allocator, rowkey))) {
    LOG_WARN("faild to get rowkey");
  }

  int tmp_ret = ret;
  if (OB_FAIL(reuse_vid_rowkey_iter())) {
    LOG_WARN("failed to reuse vid rowkey iter.", K(ret));
  } else {
    ret = tmp_ret;
  }

  return ret;
}

int ObDASHNSWScanIter::get_vector_from_com_aux_vec_table(ObIAllocator &allocator, ObRowkey& rowkey, ObString &vector)
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *com_aux_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_com_aux_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);

  if (!com_aux_vec_iter_first_scan_ && OB_FAIL(reuse_com_aux_vec_iter())) {
    LOG_WARN("failed to reuse com aux vec iter.", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(rowkey, com_aux_vec_scan_param_, com_aux_vec_ctdef->ref_table_id_))) {
    LOG_WARN("failed to set lookup key", K(ret));
  } else if (OB_FAIL(do_com_aux_vec_table_scan())) {
    LOG_WARN("failed to do com aux vec table scan", K(ret));
  } else if (OB_FALSE_IT(com_aux_vec_iter_->clear_evaluated_flag())) {
  } else {
    storage::ObTableScanIterator *table_scan_iter = dynamic_cast<storage::ObTableScanIterator *>(com_aux_vec_iter_->get_output_result_iter());
    blocksstable::ObDatumRow *datum_row = nullptr;

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
  }

  return ret;
}

int ObDASHNSWScanIter::do_delta_buf_table_scan()
{
  const ObDASScanCtDef *delta_buf_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_delta_tbl_idx(), ObTSCIRScanType::OB_VEC_DELTA_BUF_SCAN);
  ObDASScanRtDef *delta_buf_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_delta_tbl_idx());

  return do_aux_table_scan_need_reuse(delta_buf_iter_first_scan_,
                                      delta_buf_scan_param_,
                                      delta_buf_ctdef,
                                      delta_buf_rtdef,
                                      delta_buf_iter_,
                                      delta_buf_tablet_id_);
}

int ObDASHNSWScanIter::do_index_id_table_scan()
{
  const ObDASScanCtDef *index_id_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_index_id_tbl_idx(), ObTSCIRScanType::OB_VEC_IDX_ID_SCAN);
  ObDASScanRtDef *index_id_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_index_id_tbl_idx());

  return do_aux_table_scan_need_reuse(index_id_iter_first_scan_,
                                      index_id_scan_param_,
                                      index_id_ctdef,
                                      index_id_rtdef,
                                      index_id_iter_,
                                      index_id_tablet_id_);
}

int ObDASHNSWScanIter::do_snapshot_table_scan()
{
  const ObDASScanCtDef *snapshot_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_snapshot_tbl_idx(), ObTSCIRScanType::OB_VEC_SNAPSHOT_SCAN);
  ObDASScanRtDef *snapshot_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_snapshot_tbl_idx());

  return do_aux_table_scan_need_reuse(snapshot_iter_first_scan_,
                                      snapshot_scan_param_,
                                      snapshot_ctdef,
                                      snapshot_rtdef,
                                      snapshot_iter_,
                                      snapshot_tablet_id_);
}

int ObDASHNSWScanIter::do_com_aux_vec_table_scan()
{
  int ret = OB_SUCCESS;

  if (com_aux_vec_iter_first_scan_) {
    const ObDASScanCtDef *com_aux_tbl_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
        vec_aux_ctdef_->get_com_aux_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
    ObDASScanRtDef *com_aux_tbl_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_com_aux_tbl_idx());
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

int ObDASHNSWScanIter::do_aux_table_scan_need_reuse(bool &first_scan,
                                                    ObTableScanParam &scan_param,
                                                    const ObDASScanCtDef *ctdef,
                                                    ObDASScanRtDef *rtdef,
                                                    ObDASScanIter *iter,
                                                    ObTabletID &tablet_id,
                                                    bool is_get)
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
    } else if (OB_FALSE_IT(iter->set_scan_param(scan_param))) {
    } else if (OB_FAIL(iter->do_table_scan())) {
      LOG_WARN("failed to do scan", K(ret));
    } else {
      first_scan = false;
    }
  } else {
    if (OB_FAIL(reuse_iter(scan_param, tablet_id, iter))) {
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

int ObDASHNSWScanIter::do_aux_table_scan(bool &first_scan,
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

int ObDASHNSWScanIter::reuse_iter(ObTableScanParam &scan_param, ObTabletID &tablet_id, ObDASScanIter *iter)
{
   int ret = OB_SUCCESS;

  const ObTabletID &scan_tablet_id = scan_param.tablet_id_;
  scan_param.need_switch_param_ = scan_param.need_switch_param_
                                  || (scan_tablet_id.is_valid() && scan_tablet_id != tablet_id);
  scan_param.tablet_id_ = tablet_id;
  scan_param.ls_id_ = ls_id_;

  if (OB_NOT_NULL(iter) && OB_FAIL(iter->reuse())) {
    LOG_WARN("reuse scan iter failed", K(ret));
  }

  return ret;
}

int ObDASHNSWScanIter::get_rowkey(ObIAllocator &allocator, ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;

  int64_t rowkey_cnt = 0;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;

  const ObDASScanCtDef * ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
  ObDASScanRtDef *rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx());

  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(ctdef), KP(rtdef));
  } else if (OB_FALSE_IT(rowkey_cnt = ctdef->rowkey_exprs_.count())) {
  } else if (OB_UNLIKELY(rowkey_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(rowkey_cnt));
  } else if (OB_FALSE_IT(obj_ptr = new (buf) ObObj[rowkey_cnt])) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
      ObObj tmp_obj;
      ObExpr *expr = ctdef->rowkey_exprs_.at(i);
      ObDatum &datum = expr->locate_expr_datum(*rtdef->eval_ctx_);
      if (OB_ISNULL(datum.ptr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get col datum null", K(ret));
      } else if (OB_FAIL(datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      } else if (OB_FAIL(ob_write_obj(allocator, tmp_obj, obj_ptr[i]))) {
        LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
      }
    }

    if (OB_SUCC(ret)) {
      rowkey.assign(obj_ptr, rowkey_cnt);
    }
  }

  return ret;
}

int ObSimpleMaxHeap::init()
{
  int ret = OB_SUCCESS;

  if (init_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_ISNULL(heap_ = static_cast<ObSortItem *>(allocator_->alloc(sizeof(ObSortItem) * capacity_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    init_ = true;
  }

  return ret;
}

int ObSimpleMaxHeap::release()
{
  int ret = OB_SUCCESS;

  if (!init_) {
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_NOT_NULL(heap_)) {
    allocator_->free(heap_);
    heap_ = nullptr;
  }

  return ret;
}

void ObSimpleMaxHeap::push(ObRowkey &rowkey, double distiance)
{
  ObSortItem item(rowkey, distiance);

  if (size_ < capacity_) {
    heap_[size_] = item;
    size_++;
    heapify_up(size_ - 1);
  } else if (item < heap_[0]) {
    heap_[0] = item;
    heapify_down(0);
  }
}

void ObSimpleMaxHeap::max_heap_sort()
{
  lib::ob_sort(heap_, heap_ + size_);
}

ObRowkey* ObSimpleMaxHeap::at(uint64_t idx)
{
  ObRowkey *rowkey = nullptr;
  if (idx < size_) {
    rowkey = &heap_[idx].rowkey_;
  }
  return rowkey;
}

double ObSimpleMaxHeap::value_at(uint64_t idx)
{
  double value = 0.0;
  if (idx < size_) {
    value = heap_[idx].distance_;
  }
  return value;
}

void ObSimpleMaxHeap::heapify_up(int idx) {
  while (idx > 0) {
    int parent_idx = (idx - 1) / 2;
    if (heap_[parent_idx] < heap_[idx]) {
      std::swap(heap_[parent_idx], heap_[idx]);
      idx = parent_idx;
    } else {
      break;
    }
  }
}

void ObSimpleMaxHeap::heapify_down(int idx) {
  while (true) {
    int left_idx = 2 * idx + 1;
    int right_idx = 2 * idx + 2;
    int largest_idx = idx;

    if (left_idx < size_ && heap_[left_idx] > heap_[largest_idx]) {
        largest_idx = left_idx;
    }

    if (right_idx < size_ && heap_[right_idx] > heap_[largest_idx]) {
        largest_idx = right_idx;
    }

    if (largest_idx != idx) {
        std::swap(heap_[idx], heap_[largest_idx]);
        idx = largest_idx;
    } else {
        break;
    }
  }
}


}  // namespace sql
}  // namespace oceanbase
