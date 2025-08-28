/**
 * Copyright (c) 2024 OceanBase
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
#include "ob_das_spiv_merge_iter.h"
#include "sql/das/ob_das_scan_op.h"
#include "storage/tx_storage/ob_access_service.h"
#include "src/storage/access/ob_table_scan_iterator.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "storage/access/ob_table_scan_iterator.h"
#include <time.h>

namespace oceanbase
{
namespace sql
{

int ObDASSPIVMergeIter::get_ob_sparse_drop_ratio_search(uint64_t &drop_ratio)
{
  int ret = OB_SUCCESS; 
  const uint64_t OB_SPARSE_DROP_RATIO_SEARCH_DEFAULT = 0;

  ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(session = exec_ctx_->get_my_session())) {
    drop_ratio = OB_SPARSE_DROP_RATIO_SEARCH_DEFAULT;
    LOG_WARN("session is null", K(ret), KP(exec_ctx_));
  } else if (OB_FAIL(session->get_ob_sparse_drop_ratio_search(drop_ratio))) {
    LOG_WARN("failed to get ob spiv drop ratio search", K(ret));
  }

  return ret;
}

int ObDASSPIVMergeIter::init_query_vector(const ObDASVecAuxScanCtDef *ir_ctdef,
                                          ObDASVecAuxScanRtDef *ir_rtdef,
                                          const ObDASSortCtDef *sort_ctdef,
                                          ObDASSortRtDef *sort_rtdef,
                                          const common::ObLimitParam &limit_param,
                                          ObExpr *&search_vec,
                                          ObExpr *&distance_calc)
{
  int ret = OB_SUCCESS;

  ObDatum *qvec_datum;
  ObString qvec_data;
  uint64_t drop_ratio;
  if (OB_FAIL(get_ob_sparse_drop_ratio_search(drop_ratio))) {
      LOG_WARN("failed to get sparse drop ratio search", K(ret));  
  } else if (drop_ratio == 100) {
  } else if (OB_FAIL(ObDasVecScanUtils::init_sort(vec_aux_ctdef_, vec_aux_rtdef_, sort_ctdef_, 
                                          sort_rtdef_, limit_param_, qvec_expr_, distance_calc_))) {
    LOG_WARN("failed to init sort", K(ret));
  } else if (OB_ISNULL(qvec_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("qvec expr is null", K(ret));
  } else if (OB_FAIL(qvec_expr_->eval(*(sort_rtdef_->eval_ctx_), qvec_datum))) {
    LOG_WARN("eval qvec arg failed", K(ret));
  } else if (qvec_datum->is_null()){
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("qvec datum null", K(ret));
  } else {
    const uint16_t subschema_id = qvec_expr_->obj_meta_.get_subschema_id();

    ObIArrayType *qvec_ptr = nullptr;
    if (OB_FAIL(ObArrayExprUtils::get_array_obj(allocator_, *(sort_rtdef_->eval_ctx_), subschema_id, qvec_datum->get_string(), qvec_ptr))) {
      LOG_WARN("get qvec failed", K(ret));
    } else if (OB_ISNULL(qvec_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("qvec is null", K(ret));    
    } else if (OB_FALSE_IT(qvec_ = static_cast<ObMapType *>(qvec_ptr))) {
    } else {
      int size = qvec_->cardinality();
      int drop_count = size * drop_ratio / 100;
      if (drop_count != 0) {
        ObArrayFixedSize<uint32_t> *keys_arr = dynamic_cast<ObArrayFixedSize<uint32_t> *>(qvec_->get_key_array());
        ObArrayFixedSize<float> *values_arr = dynamic_cast<ObArrayFixedSize<float> *>(qvec_->get_value_array());
        if (OB_ISNULL(keys_arr) || OB_ISNULL(values_arr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to cast key", K(ret));
        } else {
          uint32_t *keys = reinterpret_cast<uint32_t *>(keys_arr->get_data());
          float *values = reinterpret_cast<float *>(values_arr->get_data());
          sort_by_value(keys, values, 0, size - 1);
          float threshold = values[drop_count];
          int real_drop_count = drop_count;
          for(; real_drop_count >= 1; real_drop_count--) {
            if (values[real_drop_count - 1] != threshold) {
              break;
            }
          }
          uint32_t *new_keys = keys + real_drop_count;
          float *new_values = values + real_drop_count;
          sort_by_key(new_keys, new_values, 0, size - 1 - real_drop_count);
          keys_arr->set_data(new_keys, size - real_drop_count);
          values_arr->set_data(new_values, size - real_drop_count);
        }
      }
    }
  }
  return ret;
}

void ObDASSPIVMergeIter::sort_by_key(uint32_t *keys, float *values, int l, int r) 
{
  if (l < r) {
    int pos = random_partition_by_key(keys, values, l, r);
    sort_by_key(keys, values, l, pos - 1);
    sort_by_key(keys, values, pos + 1, r);
  }
}

int ObDASSPIVMergeIter::random_partition_by_value(uint32_t *keys, float *values, int l, int r) 
{
  int i = l, j = r;
  srand(time(0));
  int pos = rand() % (r - l + 1) + l;
  uint32_t pivot_key = keys[pos];
  float pivot_value = values[pos];
  std::swap(keys[l], keys[pos]);
  std::swap(values[l], values[pos]);
  while (i < j) {
    while (i < j && values[j] >= pivot_value) {
      j--;
    }
    keys[i] = keys[j];
    values[i] = values[j];
    while (i < j && values[i] <= pivot_value) {
      i++;
    }
    keys[j] = keys[i];
    values[j] = values[i];
  }
  keys[i] = pivot_key;
  values[i] = pivot_value;

  return i;
}

int ObDASSPIVMergeIter::random_partition_by_key(uint32_t *keys, float *values, int l, int r) 
{
  int i = l, j = r;
  srand(time(0));
  int pos = rand() % (r - l + 1) + l;
  uint32_t pivot_key = keys[pos];
  float pivot_value = values[pos];
  std::swap(keys[l], keys[pos]);
  std::swap(values[l], values[pos]);
  while (i < j) {
    while (i < j && keys[j] >= pivot_key) j--;
    keys[i] = keys[j];
    values[i] = values[j];
    while (i < j && keys[i] <= pivot_key) i++;
    keys[j] = keys[i];
    values[j] = values[i];
  }
  keys[i] = pivot_key;
  values[i] = pivot_value;

  return i;
}

void ObDASSPIVMergeIter::sort_by_value(uint32_t *keys, float *values, int l, int r)
{
  if (l < r) {
    int pos = random_partition_by_value(keys, values, l, r);
    sort_by_value(keys, values, l, pos - 1);
    sort_by_value(keys, values, pos + 1, r);
  }
}

void ObDASSPIVMergeIter::set_algo()
{
  algo_ = SPIVAlgo::DAAT_NAIVE;

}

void ObDASSPIVMergeIter::clear_evaluated_flag()
{
  if (vec_aux_ctdef_->is_pre_filter()) {
    if (OB_NOT_NULL(inv_idx_scan_iter_)) {
      inv_idx_scan_iter_->clear_evaluated_flag();
    } else if (OB_NOT_NULL(rowkey_docid_iter_)) {
      rowkey_docid_iter_->clear_evaluated_flag();
    } else if (OB_NOT_NULL(aux_data_iter_)) {
      aux_data_iter_->clear_evaluated_flag();
    }
  } 
}

int ObDASSPIVMergeIter::rescan()
{
  int ret = OB_SUCCESS;
  if (vec_aux_ctdef_->is_pre_filter()) {
    if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->rescan())) {
      LOG_WARN("failed to do inv idx table rescan", K(ret));
    } 
  } 
  
  return ret;
}

int ObDASSPIVMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (vec_aux_ctdef_->is_pre_filter()) {
    if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->do_table_scan())) {
      LOG_WARN("failed to do inv idx table scan", K(ret));
    }
  }
  return ret;
}

int ObDASSPIVMergeIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(ObDASIterType::DAS_ITER_SPIV_MERGE != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for spiv merge iter", K(ret), K(param));
  } else {
    ObDASSPIVMergeIterParam spiv_merge_param = static_cast<ObDASSPIVMergeIterParam &>(param);

    ls_id_ = spiv_merge_param.ls_id_;
    tx_desc_ = spiv_merge_param.tx_desc_;
    snapshot_ = spiv_merge_param.snapshot_;

    rowkey_docid_iter_ = spiv_merge_param.rowkey_docid_iter_;
    aux_data_iter_ = spiv_merge_param.aux_data_iter_;
    inv_idx_scan_iter_ = spiv_merge_param.inv_idx_scan_iter_;

    vec_aux_ctdef_ = spiv_merge_param.vec_aux_ctdef_;
    vec_aux_rtdef_ = spiv_merge_param.vec_aux_rtdef_;
    sort_ctdef_ = spiv_merge_param.sort_ctdef_;
    sort_rtdef_ = spiv_merge_param.sort_rtdef_;
    spiv_scan_ctdef_ = spiv_merge_param.spiv_scan_ctdef_;
    spiv_scan_rtdef_ = spiv_merge_param.spiv_scan_rtdef_;
    selectivity_ = vec_aux_ctdef_->selectivity_;

    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "SPIV_MERGE", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create vector spiv_merge memory context", K(ret));
      }
    }
    
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDasVecScanUtils::init_limit(vec_aux_ctdef_, vec_aux_rtdef_, sort_ctdef_, sort_rtdef_, limit_param_))) {
      LOG_WARN("failed to init limit", K(ret), KPC(vec_aux_ctdef_), KPC(vec_aux_rtdef_));
    } else if (OB_FAIL(init_query_vector(vec_aux_ctdef_, vec_aux_rtdef_, sort_ctdef_, sort_rtdef_, limit_param_, qvec_expr_, distance_calc_))) {
      LOG_WARN("failed to init query vector", K(ret)); 
    } else if (OB_ISNULL(qvec_)) {
    } else if (OB_FAIL(ObDasVecScanUtils::get_distance_expr_type(*sort_ctdef_->sort_exprs_[0], *sort_rtdef_->eval_ctx_, dis_type_))) {
        LOG_WARN("failed to get distance type.", K(ret));
    } else if (dis_type_ != ObExprVectorDistance::ObVecDisType::DOT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("distance type not support yet", K(ret), K(dis_type_));
    } else if (OB_FALSE_IT(set_algo())) {
    } else if (vec_aux_ctdef_->is_pre_filter()){
      if (OB_FAIL(valid_docid_set_.create(16))) {
        LOG_WARN("failed to create docid set", K(ret));
      }
    }

    is_inited_ = true;
  }
  return ret;
}

int ObDASSPIVMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  
  if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->reuse())) {
    LOG_WARN("failed to reuse inv idx scan iter", K(ret));
  } else if (!aux_data_table_first_scan_ && OB_FAIL(reuse_aux_data_iter())) {
    LOG_WARN("failed to reuse com aux vec iter", K(ret));
  } else if (!rowkey_docid_table_first_scan_ && OB_FAIL(reuse_rowkey_docid_iter())) {
    LOG_WARN("failed to reuse rowkey vid iter", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < iters_.count(); i++) {
      if (OB_ISNULL(iters_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter is null");
      } else if (OB_FAIL(iters_.at(i)->reuse())) {
        LOG_WARN("failed to reuse iter", K(ret), K(i));
      }
    }
  }
  
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }
  saved_rowkeys_.reset();
  valid_docid_set_.clear();
  result_docids_.reset();
  result_docids_curr_iter_ = OB_INVALID_INDEX_INT64;
 
  return ret;
}

int ObDASSPIVMergeIter::inner_release()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  
  if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->release())) {
    LOG_WARN("failed to release inv idx scan iter", K(ret));
    tmp_ret = ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(aux_data_iter_) && OB_FAIL(aux_data_iter_->release())) {
    LOG_WARN("failed to release aux data iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(rowkey_docid_iter_) && OB_FAIL(rowkey_docid_iter_->release())) {
    LOG_WARN("failed to release rowkey docid iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  for (int i = 0; OB_SUCC(ret) && i < iters_.count(); i++) {
    if (OB_ISNULL(iters_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is null");
    } else if (OB_FAIL(iters_.at(i)->release())) {
      LOG_WARN("failed to release spiv scan iter", K(ret));
      tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
      ret = OB_SUCCESS;
    }
  }
  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }
  
  inv_idx_scan_iter_ = nullptr;
  aux_data_iter_ = nullptr;
  rowkey_docid_iter_ = nullptr;
  iters_.reset();

  tx_desc_ = nullptr;
  snapshot_ = nullptr;
  vec_aux_ctdef_ = nullptr;
  vec_aux_rtdef_ = nullptr;
  sort_ctdef_ = nullptr;
  sort_rtdef_ = nullptr;
  qvec_ = nullptr;
  qvec_expr_ = nullptr;
  distance_calc_ = nullptr;

  cursors_.reset();
  saved_rowkeys_.reset();
  valid_docid_set_.destroy();
  result_docids_.reset();
  result_docids_curr_iter_ = OB_INVALID_INDEX_INT64;

  ObDasVecScanUtils::release_scan_param(aux_data_scan_param_);
  ObDasVecScanUtils::release_scan_param(rowkey_docid_scan_param_);

  if (nullptr != mem_context_)  {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  allocator_.reset();
  
  return ret;
}

int ObDASSPIVMergeIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(qvec_) || limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_INVALID_INDEX_INT64 == result_docids_curr_iter_) {
    if (OB_FAIL(process(false))) {
      LOG_WARN("failed to process", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_INDEX_INT64 == result_docids_curr_iter_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get heap iter", K(ret));
  } else if (result_docids_curr_iter_ == result_docids_.count()){
    ret = OB_ITER_END;
  } else {
    ObExpr *docid_expr = vec_aux_ctdef_->spiv_scan_docid_col_;
    ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
    guard.set_batch_idx(0);
    ObDatum &docid_datum = docid_expr->locate_datum_for_write(*vec_aux_rtdef_->eval_ctx_);
    docid_datum.set_string(result_docids_.at(result_docids_curr_iter_++));
  }

  return ret;
}

int ObDASSPIVMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(qvec_) || limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_INVALID_INDEX_INT64 == result_docids_curr_iter_) {
    if (OB_FAIL(process(true))) {
      LOG_WARN("failed to process", K(ret));
    }
  } 
  
  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_INDEX_INT64 == result_docids_curr_iter_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get heap iter", K(ret));
  } else if (result_docids_curr_iter_ == result_docids_.count()) {
    ret = OB_ITER_END;
  } else {
    count = OB_MIN(result_docids_.count() - result_docids_curr_iter_, capacity);
    ObExpr *docid_expr = vec_aux_ctdef_->spiv_scan_docid_col_;
    ObDatum *docid_datum = nullptr;
    ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
    guard.set_batch_size(count);
    if (OB_ISNULL(docid_datum = docid_expr->locate_datums_for_update(*vec_aux_rtdef_->eval_ctx_, count))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, datums is nullptr", K(ret), KPC(docid_expr));
    } else {
      for (int64_t i = 0; i < count; ++i) {  
        guard.set_batch_idx(i);
        docid_datum[i].set_string(result_docids_.at(result_docids_curr_iter_++));
      }
      docid_expr->set_evaluated_projected(*vec_aux_rtdef_->eval_ctx_);
    }
  }

  return ret;
}

int ObDASSPIVMergeIter::process(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
  if (vec_aux_ctdef_->is_pre_filter()) {
    if (OB_FAIL(process_pre_filter(allocator, is_vectorized))) {
      LOG_WARN("failed to process pre filter", K(ret));
    }
  } else if (vec_aux_ctdef_->is_post_filter()) {
    if (OB_FAIL(process_post_filter(allocator, is_vectorized))) {
      LOG_WARN("failed to process post filter", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    result_docids_curr_iter_ = 0;
  }
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }
  return ret;
}

int ObDASSPIVMergeIter::get_rowkey_pre_filter(ObIAllocator &allocator, bool is_vectorized, int64_t batch_count){
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_spiv_rowkey_docid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
  ObDASScanRtDef *rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_spiv_rowkey_docid_tbl_idx());

  bool is_iter_end = false;
  while (OB_SUCC(ret) && saved_rowkeys_.count() < MAX_SPIV_BRUTE_FORCE_SIZE && !is_iter_end) {
    if (!is_vectorized) {
      ObRowkey *rowkey;
      inv_idx_scan_iter_->clear_evaluated_flag();
      if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
          is_iter_end = true;
        } else {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (OB_FAIL(ObDasVecScanUtils::get_rowkey(allocator, ctdef, rtdef, rowkey))) {
        LOG_WARN("failed to get rowkey", K(ret));
      } else if (OB_FAIL(saved_rowkeys_.push_back(rowkey))) {
        LOG_WARN("failed to push rowkey", K(ret));
      }
    } else {
      int64_t scan_row_cnt = 0;
      int64_t curr_batch_count = OB_MIN(batch_count, MAX_SPIV_BRUTE_FORCE_SIZE - saved_rowkeys_.count());
      if (OB_FAIL(inv_idx_scan_iter_->get_next_rows(scan_row_cnt, curr_batch_count))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row from inv_idx_scan_iter_", K(ret));
        } else { 
          ret = OB_SUCCESS;        
          if (scan_row_cnt == 0) {
            is_iter_end = true;
          }
        }
      }

      if (OB_SUCC(ret) && !is_iter_end) {
        ObEvalCtx::BatchInfoScopeGuard guard(*rtdef->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        for (int i = 0; OB_SUCC(ret) && i < scan_row_cnt; i++) {
          guard.set_batch_idx(i);
          ObRowkey *rowkey;
          if (OB_FAIL(ObDasVecScanUtils::get_rowkey(allocator, ctdef, rtdef, rowkey))) {
            LOG_WARN("failed to add rowkey", K(ret), K(i));
          } else if (OB_FAIL(saved_rowkeys_.push_back(rowkey))) {
            LOG_WARN("failed to push rowkeys", K(ret));           
          }
        }
      }
    }
  }

  return ret;
}

int ObDASSPIVMergeIter::get_rowkey_and_set_docids(ObIAllocator &allocator, bool is_vectorized, int64_t batch_count){
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_spiv_rowkey_docid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
  ObDASScanRtDef *rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_spiv_rowkey_docid_tbl_idx());

  bool is_iter_end = false;
  while (OB_SUCC(ret) && !is_iter_end) {
    if (!is_vectorized) {
      for (int i = 0; OB_SUCC(ret) && i < batch_count && !is_iter_end; i++) {
        ObRowkey *rowkey;
        inv_idx_scan_iter_->clear_evaluated_flag();
        if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
          if (ret == OB_ITER_END) {
            ret = OB_SUCCESS;
            is_iter_end = true;
          } else {
            LOG_WARN("failed to get next row", K(ret));
          }
        } else if (OB_FAIL(ObDasVecScanUtils::get_rowkey(allocator, ctdef, rtdef, rowkey))) {
          LOG_WARN("failed to get rowkey", K(ret));
        } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(*rowkey, rowkey_docid_scan_param_, ctdef->ref_table_id_))) {
          LOG_WARN("failed to set rowkey.", K(ret));
        }
      }
    } else {
      int64_t scan_row_cnt = 0;
      if (OB_FAIL(inv_idx_scan_iter_->get_next_rows(scan_row_cnt, batch_count))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row from inv_idx_scan_iter_", K(ret));
        } else {  
          ret = OB_SUCCESS;       
          if (scan_row_cnt == 0) {
            is_iter_end = true;
          }
        }
      }
      if (OB_SUCC(ret) && !is_iter_end) {
        ObEvalCtx::BatchInfoScopeGuard guard(*rtdef->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        for (int i = 0; OB_SUCC(ret) && i < scan_row_cnt; i++) {
          guard.set_batch_idx(i);
          ObRowkey *rowkey;
          if (OB_FAIL(ObDasVecScanUtils::get_rowkey(allocator, ctdef, rtdef, rowkey))) {
            LOG_WARN("failed to add rowkey", K(ret), K(i));
          } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(*rowkey, rowkey_docid_scan_param_, ctdef->ref_table_id_))) {
            LOG_WARN("failed to set rowkey.", K(ret));
          }
        }
      }
    }
    
    if (OB_SUCC(ret) && rowkey_docid_scan_param_.key_ranges_.count() != 0) {
      if (OB_FAIL(do_rowkey_docid_table_scan())) { 
        LOG_WARN("failed to do rowkey docid table scan", K(ret));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < batch_count; ++i) {
        ObString docid;
        if (OB_FAIL(get_docid_from_rowkey_docid_table(docid))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to get docid from rowkey docid table.", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(valid_docid_set_.set_refactored(docid))){
          LOG_WARN("failed to insert valid docid set", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(reuse_rowkey_docid_iter())) {
        LOG_WARN("failed to reuse rowkey docid iter", K(ret));
      }
    }
  }

  return ret;
}

int ObDASSPIVMergeIter::do_brute_force(ObIAllocator &allocator, bool is_vectorized, int64_t batch_count)
{
  int ret = OB_SUCCESS;

  uint64_t limit = limit_param_.limit_ + limit_param_.offset_;
  uint64_t saved_rowkey_count = saved_rowkeys_.count();
  uint64_t capacity = limit > saved_rowkey_count ? saved_rowkey_count : limit;
  
  ObSPIVFixedSizeHeap<ObRowkeyScoreItem, ObRowkeyScoreItemCmp> max_heap(capacity, allocator, rowkey_score_cmp_);
  
  const ObDASScanCtDef *aux_data_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_spiv_aux_data_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
  const ObDASScanRtDef *aux_data_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_spiv_aux_data_tbl_idx());

  // get vector by rowkey, calc distance and push to heap
  int64_t cur_idx = 0;
  while (OB_SUCC(ret) && cur_idx < saved_rowkey_count) {
    int64_t start_idx = cur_idx;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_count && cur_idx < saved_rowkey_count; ++i) {
      if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(*saved_rowkeys_[cur_idx++], aux_data_scan_param_, aux_data_ctdef->ref_table_id_))) {
        LOG_WARN("failed to set lookup key", K(ret));
      } 
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_aux_data_table_scan())) {
      LOG_WARN("failed to do aux data table scan", K(ret));
    } else {
      for (; OB_SUCC(ret) && start_idx < cur_idx; ++start_idx) {
        ObString vector;
        if (OB_FAIL(get_vector_from_aux_data_table(vector))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to get vector from aux data table.", K(ret), K(start_idx));
          }
        } else if (OB_ISNULL(vector.ptr())) {
        } else {
          double score;
          ObIArrayType *arr = nullptr;
          ObMapType *vec = nullptr;
          if (OB_FAIL(ObArrayTypeObjFactory::construct(allocator, *qvec_->get_array_type(), arr, true))) {
            LOG_WARN("failed to construct arr", K(ret));
          } else if (OB_FAIL(arr->init(vector))){
            LOG_WARN("failed to init sparse vector", K(ret));
          } else if (OB_FALSE_IT(vec = dynamic_cast<ObMapType *>(arr))) {
          } else if (OB_ISNULL(vec)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("arr cast failed", K(ret));
          } else if (OB_FAIL(ObExprVectorDistance::SparseVectorDisFunc::spiv_distance_funcs[dis_type_](qvec_, vec, score))) {
            LOG_WARN("failed to get score", K(ret));
          } else if (score == 0) {
          } else {
            ObRowkey *rowkey = saved_rowkeys_[start_idx];
            ObRowkeyScoreItem item{rowkey, -score};
            max_heap.push(item);
          } 
        }
      }
      int tmp_ret = ret;
      ret = OB_SUCCESS;
      if (OB_FAIL(reuse_aux_data_iter())) {
        LOG_WARN("failed to reuse aux data table iter.", K(ret));
      } else {
        ret = tmp_ret;
      }
      ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
    }
  }

  uint64_t heap_size = max_heap.count();
  if (OB_SUCC(ret)) {
    const ObDASScanCtDef *rowkey_docid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_spiv_rowkey_docid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
    while (OB_SUCC(ret) && !max_heap.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_count && !max_heap.empty(); ++i) {
        ObRowkey *rowkey = max_heap.top().rowkey_;
        if (OB_FAIL(max_heap.pop())) {
          LOG_WARN("failed to pop rowkey from heap", K(ret));  
        } else if (OB_ISNULL(rowkey)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get rowkey from max heap.", K(ret));  
        } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(*rowkey, rowkey_docid_scan_param_, rowkey_docid_ctdef->ref_table_id_))) {
          LOG_WARN("failed to set rowkey.", K(ret));
        }      
      }

      if (OB_SUCC(ret) && OB_FAIL(do_rowkey_docid_table_scan())) { 
        LOG_WARN("failed to do rowkey docid table scan", K(ret));
      }

      ObString cur_docid;
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_count; ++i) {
        if (OB_FAIL(get_docid_from_rowkey_docid_table(cur_docid))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to get docid from rowkey docid table.", K(ret), K(i));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(result_docids_.push_back(cur_docid))) {
          LOG_WARN("failed to push back docid", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(reuse_rowkey_docid_iter())) {
        LOG_WARN("failed to reuse rowkey docid iter", K(ret));
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < heap_size / 2; i++) {
      std::swap(result_docids_.at(i), result_docids_.at(heap_size - 1 - i));
    }
  }

  return ret;
}

int ObDASSPIVMergeIter::set_valid_docids_with_rowkeys(ObIAllocator &allocator, int64_t batch_count)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *rowkey_docid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_spiv_rowkey_docid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);

  int64_t rowkey_idx = 0;
  int rowkeys_size = saved_rowkeys_.count();
  
  while (OB_SUCC(ret) && rowkey_idx < rowkeys_size) {
    int batch_size = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_count && rowkey_idx < rowkeys_size; ++i) {
      ObRowkey *rowkey;
      if (OB_ISNULL(rowkey = saved_rowkeys_.at(rowkey_idx++))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get rowkey from saved rowkeys.", K(ret));  
      } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(*rowkey, rowkey_docid_scan_param_, rowkey_docid_ctdef->ref_table_id_))) {
        LOG_WARN("failed to set rowkey.", K(ret));
      }
      batch_size++;
    }
    if (OB_SUCC(ret) && OB_FAIL(do_rowkey_docid_table_scan())) { 
      LOG_WARN("failed to do rowkey docid table scan", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      ObString docid;
      if (OB_FAIL(get_docid_from_rowkey_docid_table(docid))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get docid from rowkey docid table.", K(ret));
        }
      } else if (OB_FAIL(valid_docid_set_.set_refactored(docid))){
        LOG_WARN("failed to insert valid docid set", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(reuse_rowkey_docid_iter())) {
      LOG_WARN("failed to reuse rowkey docid iter", K(ret));
    }
  }

  return ret;
}

int ObDASSPIVMergeIter::process_pre_filter(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  int64_t batch_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE; 
  if (OB_FAIL(get_rowkey_pre_filter(allocator, is_vectorized, batch_count))) {
    LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
  } else if (saved_rowkeys_.count() < MAX_SPIV_BRUTE_FORCE_SIZE) {
    if (OB_FAIL(do_brute_force(allocator, is_vectorized, batch_count))) {
      LOG_WARN("failed to do brute force", K(ret));
    }
  } else if (OB_FAIL(set_valid_docids_with_rowkeys(allocator, batch_count))) {
    LOG_WARN("failed to set valid docids", K(ret));
  } else if (OB_FAIL(get_rowkey_and_set_docids(allocator, is_vectorized, batch_count))){
    LOG_WARN("failed to get rowkeys and set valid docids", K(ret));
  } else if (OB_FAIL(process_algo(allocator, true, is_vectorized))) {
    LOG_WARN("failed to process algo", K(ret));
  }

  return ret;
}

int ObDASSPIVMergeIter::process_post_filter(ObIAllocator &allocator, bool is_vectorized)
{
  return process_algo(allocator, false, is_vectorized);
}

int ObDASSPIVMergeIter::process_algo(ObIAllocator &allocator, bool is_pre_filter, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  if (algo_ != SPIVAlgo::DAAT_NAIVE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SPIVAlgo not supported", K(ret));
  } else {
    int64_t limit = limit_param_.limit_ + limit_param_.offset_;
    int64_t capacity = limit;
    if (!is_pre_filter && selectivity_ != 1) {
      capacity = limit * 2;
    }
    if (OB_FAIL(daat_naive(allocator, is_pre_filter, is_vectorized, capacity))) {
      LOG_WARN("failed to do daat_naive", K(ret));
    }
  }
  return ret;
}

int ObDASSPIVMergeIter::daat_naive(ObIAllocator &allocator, bool is_pre_filter, bool is_vectorized, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObString curr_min_docid;
  float *value;
  
  ObSPIVFixedSizeHeap<ObDocidScoreItem, ObDocidScoreItemCmp> heap(capacity, allocator, docid_score_cmp_); 

  if (OB_ISNULL(qvec_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("qvec is null", K(ret));
  } else if (qvec_->size() == 0) {
    ret = OB_ITER_END;
  } else {
    float *values = reinterpret_cast<float *>(qvec_->get_value_array()->get_data());
    if (OB_FAIL(make_cursors(allocator, is_pre_filter, is_vectorized))) {
      LOG_WARN("failed to make cursors", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        // find first valid docid_
        int j = 0;
        for (; j < cursors_.count(); j++) {
          if (OB_NOT_NULL(cursors_[j])) {
            curr_min_docid = cursors_[j]->docid_;
            break;
          }
        }
        int pos = j;

        // all docids are invalid
        if (j >= cursors_.count()) {
          break;
        }
        
        // select min docid
        for (; j < cursors_.count(); j++) {
          if (OB_ISNULL(cursors_[j])) {
            continue;
          }
          if (cursors_[j]->docid_ < curr_min_docid) {
            curr_min_docid = cursors_[j]->docid_;
          }
        }

        double score = 0;
        for (int i = pos; OB_SUCC(ret) && i < cursors_.count(); i++) {
          if (OB_ISNULL(cursors_[i]) || cursors_[i]->docid_ > curr_min_docid) {
            continue;
          }
          // iters 创建的时候是按照 key 的顺序，所以这里可以直接索引 value
          int64_t idx = cursors_[i]->iter_idx_;
          score += cursors_[i]->value_ * values[idx];
          
          bool is_iter_end = false;

          if (OB_FAIL(ret)) {
          } else if (is_pre_filter) {
            bool exist = false;
            while (OB_SUCC(ret) && !is_iter_end && !exist) {
              if (OB_FAIL(single_iter_get_next_row(allocator, idx, cursors_[i], is_iter_end, is_vectorized))) {
                LOG_WARN("failed to get next row", K(ret));
              } else if (is_iter_end) {
                cursors_[i] = nullptr;
                ret = OB_SUCCESS;
              } else {
                int hash_ret = valid_docid_set_.exist_refactored(cursors_[i]->docid_);
                if (OB_HASH_EXIST == hash_ret) {
                  exist = true;
                }
              }
            }
          } else { 
            if (OB_FAIL(single_iter_get_next_row(allocator, idx, cursors_[i], is_iter_end, is_vectorized))) {
              LOG_WARN("failed to get next row", K(ret));
            } else if (is_iter_end) {
              cursors_[i] = nullptr;
            }
          }
        }
        ObDocidScoreItem item{curr_min_docid, -score};
        heap.push(item);
      }    
    
      int heap_size = heap.count();
      if (heap_size == 0) {
        ret= OB_ITER_END;
      } else {
        ObString res_docid;
        for (int i = 0; OB_SUCC(ret) && i < heap_size; i++) {
          if(OB_FAIL(ob_write_string(allocator_, heap.top().docid_, res_docid))) {
            LOG_WARN("failed to deep copy res_docid", K(ret));
          } else if (OB_FAIL(result_docids_.push_back(res_docid))) {
            LOG_WARN("failed to push back docid", K(ret));
          } else if (OB_FAIL(heap.pop())) {
            LOG_WARN("failed to pop docid", K(ret));
          }
        }
        for (int i = 0; OB_SUCC(ret) && i < heap_size / 2; i++) {
          std::swap(result_docids_.at(i), result_docids_.at(heap_size - 1 - i));
        }
      }
    } 
  }

  return ret;
}

int ObDASSPIVMergeIter::make_cursors(ObIAllocator &allocator, bool is_pre_filter, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  bool is_iter_end;
  ObSPIVItem *buff = nullptr;
  if (OB_ISNULL(buff = static_cast<ObSPIVItem *>(allocator.alloc(sizeof(ObSPIVItem) * iters_.count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator cursor.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
      if (OB_ISNULL(cursors_[i])) {
        cursors_[i] = buff + i;
      } 

      if (OB_FAIL(iters_.at(i)->do_table_scan())) {
        LOG_WARN("failed to do table scan", K(ret), K(i), K(iters_.count()));
      }
      
      if (OB_FAIL(ret)) {
      } else if (is_pre_filter) {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(single_iter_get_next_row(allocator, i, cursors_[i], is_iter_end, is_vectorized))) {
            LOG_WARN("failed to get single iter next row", K(ret), K(i), K(iters_.count()));
          } else if (is_iter_end) {
            cursors_[i] = nullptr;
            break;
          } else {
            int hash_ret = valid_docid_set_.exist_refactored(cursors_[i]->docid_);
            if (OB_HASH_EXIST == hash_ret) {
              break;
            }
          }
        }
      } else {
        if (OB_FAIL(single_iter_get_next_row(allocator, i, cursors_[i], is_iter_end, is_vectorized))) {
          LOG_WARN("failed to get next row", K(ret));
        } else if (is_iter_end) {
          cursors_[i] = nullptr; 
        } 
      }
    }
  }
  return ret;
}

int ObDASSPIVMergeIter::single_iter_get_next_row(ObIAllocator &allocator, const int64_t iter_idx, ObSPIVItem *&item, bool &is_iter_end, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  is_iter_end = false;
  ObDASSPIVScanIter *iter = nullptr;
  if (OB_ISNULL(iter = iters_.at(iter_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null spiv scan iter ptr", K(ret), K(iter_idx), K(iters_.count()));
  } else { 
    int64_t scan_row_cnt = 0;
    if (is_vectorized) {
      if (OB_FAIL(iter->get_next_rows(scan_row_cnt, 1))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row from iterator", K(ret));
        } else {
          is_iter_end = true;
          ret = OB_SUCCESS;
        }
      }
    } else {
      if (OB_FAIL(iter->get_next_row())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row from iterator", K(ret));
        } else {
          is_iter_end = true;
          ret = OB_SUCCESS;
        }
      } else {
        scan_row_cnt = 1;
      }
    }
    
    if (OB_FAIL(ret)) {
    } else if (scan_row_cnt == 0) {
    } else if (OB_FAIL(fill_spiv_item(allocator, *iter, iter_idx, item))) {
      LOG_WARN("fail to fill spiv item", K(ret));
    } 
  } 

  return ret;
}

int ObDASSPIVMergeIter::fill_spiv_item(ObIAllocator &allocator, ObDASSPIVScanIter &iter, const int64_t iter_idx, ObSPIVItem *&item)
{
  int ret = OB_SUCCESS;
  
  if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fill spiv item null", K(ret));
  } else {
    item->iter_idx_ = iter_idx;
    ObExpr *docid_expr = spiv_scan_ctdef_->result_output_[0];
    const ObDatum &docid_datum = docid_expr->locate_expr_datum(*spiv_scan_rtdef_->eval_ctx_);

    if (OB_FAIL(ob_write_string(allocator, docid_datum.get_string(), item->docid_))) {
      LOG_WARN("failed to write docid string", K(ret), K(docid_datum), KPC(docid_expr));
    } else {
      ObExpr *value_expr = vec_aux_ctdef_->spiv_scan_value_col_;
      const ObDatum &value_datum = value_expr->locate_expr_datum(*spiv_scan_rtdef_->eval_ctx_);
      item->value_ = value_datum.get_float();
    }
  }
  return ret;
}

int ObDASSPIVMergeIter::do_aux_data_table_scan()
{
  int ret = OB_SUCCESS;

  if (aux_data_table_first_scan_) {
    const ObDASScanCtDef *aux_data_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
        vec_aux_ctdef_->get_spiv_aux_data_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
    ObDASScanRtDef *aux_data_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_spiv_aux_data_tbl_idx());
    if (OB_FAIL(ObDasVecScanUtils::init_vec_aux_scan_param(ls_id_,
                                                           aux_data_tablet_id_,
                                                           aux_data_ctdef,
                                                           aux_data_rtdef,
                                                           tx_desc_,
                                                           snapshot_,
                                                           aux_data_scan_param_,
                                                           true/*is_get*/))) {
      LOG_WARN("failed to init aux data scan param", K(ret));
    } else if (OB_FALSE_IT(aux_data_iter_->set_scan_param(aux_data_scan_param_))) {
    } else if (OB_FAIL(aux_data_iter_->do_table_scan())) {
      LOG_WARN("failed to do aux data iter scan", K(ret));
    } else {
      aux_data_table_first_scan_ = false;
    }
  } else {
    if (OB_FAIL(aux_data_iter_->rescan())) {
      LOG_WARN("failed to rescan aux data table scan iterator.", K(ret));
    }
  }
  return ret;
}

int ObDASSPIVMergeIter::do_rowkey_docid_table_scan()
{
  int ret = OB_SUCCESS;

  if (rowkey_docid_table_first_scan_) {
    rowkey_docid_scan_param_.need_switch_param_ = false;
    const ObDASScanCtDef *rowkey_docid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
        vec_aux_ctdef_->get_spiv_rowkey_docid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN); 
    ObDASScanRtDef *rowkey_docid_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_spiv_rowkey_docid_tbl_idx());
    if (OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_,
                                                   rowkey_docid_tablet_id_,
                                                   rowkey_docid_ctdef,
                                                   rowkey_docid_rtdef,
                                                   tx_desc_,
                                                   snapshot_,
                                                   rowkey_docid_scan_param_))) {
      LOG_WARN("failed to init rowkey docid scan param", K(ret));
    } else if (OB_FALSE_IT(rowkey_docid_iter_->set_scan_param(rowkey_docid_scan_param_))) {
    } else if (OB_FAIL(rowkey_docid_iter_->do_table_scan())) {
      LOG_WARN("failed to do rowkey docid iter scan", K(ret));
    } else {
      rowkey_docid_table_first_scan_ = false;
    }
  } else {
    if (OB_FAIL(rowkey_docid_iter_->rescan())) {
      LOG_WARN("failed to rescan rowkey docid table scan iterator.", K(ret));
    }
  }
  return ret;
}

int ObDASSPIVMergeIter::get_vector_from_aux_data_table(ObString &vector)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
  const ObDASScanCtDef *aux_data_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_spiv_aux_data_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
  storage::ObTableScanIterator *table_scan_iter = dynamic_cast<storage::ObTableScanIterator *>(aux_data_iter_->get_output_result_iter());
  blocksstable::ObDatumRow *datum_row = nullptr;
  
  aux_data_iter_->clear_evaluated_flag();

  const int64_t INVALID_COLUMN_ID = -1;
  int64_t vec_col_idx = INVALID_COLUMN_ID;
  int output_row_cnt = aux_data_ctdef->pd_expr_spec_.access_exprs_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < output_row_cnt; i++) {
    ObExpr *expr = aux_data_ctdef->pd_expr_spec_.access_exprs_.at(i);
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
      LOG_WARN("failed to scan aux data iter", K(ret));
    }
  } else if (datum_row->get_column_count() != output_row_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
  } else if (OB_FALSE_IT(vector = datum_row->storage_datums_[vec_col_idx].get_string())) {
    LOG_WARN("failed to get vid.", K(ret));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator, 
                                                                ObLongTextType, 
                                                                CS_TYPE_BINARY, 
                                                                aux_data_ctdef->result_output_.at(0)->obj_meta_.has_lob_header(), 
                                                                vector))) {
    LOG_WARN("failed to get real data.", K(ret));
  }

  return ret;
}

int ObDASSPIVMergeIter::get_docid_from_rowkey_docid_table(ObString &docid)
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *rowkey_docid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_spiv_rowkey_docid_tbl_idx(),ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
  ObDASScanRtDef *rowkey_docid_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_spiv_rowkey_docid_tbl_idx());

  rowkey_docid_iter_->clear_evaluated_flag();
  if (OB_FAIL(rowkey_docid_iter_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to scan rowkey docid iter", K(ret));
    }
  } else {
    ObExpr *docid_expr = vec_aux_ctdef_->spiv_scan_docid_col_;
    ObDatum &docid_datum = docid_expr->locate_expr_datum(*rowkey_docid_rtdef->eval_ctx_);
    if (OB_FAIL(ob_write_string(allocator_, docid_datum.get_string(), docid))) {
      LOG_WARN("invalid docid string.", K(ret));
    } 
  }

  return ret;
}


} // namespace sql
} // namespace oceanbase
