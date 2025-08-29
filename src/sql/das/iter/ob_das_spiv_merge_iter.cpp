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

// TODO: set algo dynamic
void ObDASSPIVMergeIter::set_algo()
{
  algo_ = SPIVAlgo::BLOCK_MAX_WAND;
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
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_inv_scan_range_key())) {
    LOG_WARN("failed to set inv scan range key for rescan", K(ret));
  }
  for (int i = 0; i < inv_dim_scan_iters_.count() && OB_SUCC(ret); ++i) {
    if (OB_NOT_NULL(inv_dim_scan_iters_[i]) && OB_FAIL(inv_dim_scan_iters_[i]->rescan())) {
      LOG_WARN("failed to rescan inv dim scan iter", K(ret));
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
  if(OB_SUCC(ret)) {
    if (OB_FAIL(create_dim_iters())) {
      LOG_WARN("failed to create dim iters", K(ret));
    } else if (OB_FAIL(create_spiv_merge_iter())) {
      LOG_WARN("failed to create sr merge iter", K(ret));
    } else if (OB_FAIL(set_inv_scan_range_key())) {
      LOG_WARN("failed to set inv scan range key", K(ret));
    } else {
      for (int i = 0; i < inv_dim_scan_iters_.count() && OB_SUCC(ret); ++i) {
        if (OB_NOT_NULL(inv_dim_scan_iters_[i]) && OB_FAIL(inv_dim_scan_iters_[i]->do_table_scan())) {
          LOG_WARN("failed to do table scan", K(ret));
        }
      }
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
    block_max_scan_ctdef_ = spiv_merge_param.block_max_scan_ctdef_;
    block_max_scan_rtdef_ = spiv_merge_param.block_max_scan_rtdef_;
    selectivity_ = vec_aux_ctdef_->selectivity_;

    if (is_use_docid()) {
      set_datum_func_ = set_datum_shallow;
      docid_lt_func_ = docid_lt_string;
      docid_gt_func_ = docid_gt_string;
    } else {
      set_datum_func_ = set_datum_int;
      docid_lt_func_ = docid_lt_int;
      docid_gt_func_ = docid_gt_int;
    }

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
      if (OB_FAIL(valid_docid_set_.create(16, ObMemAttr(MTL_ID(), "ValidDocidSet")))) {
        LOG_WARN("failed to create docid set", K(ret));
      }
    }

    is_inited_ = true;
    is_pre_processed_ = false;
  }
  return ret;
}

int ObDASSPIVMergeIter::build_inv_scan_range(ObNewRange &range, uint64_t table_id, uint32_t dim)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
  ObObj *start_key_ptr = nullptr;
  ObObj *end_key_ptr = nullptr;
  ObRowkey start_key;
  ObRowkey end_key;
  if (OB_ISNULL(start_key_ptr = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * INV_IDX_ROWKEY_COL_CNT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_ISNULL(end_key_ptr = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * INV_IDX_ROWKEY_COL_CNT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    start_key_ptr[0].set_uint32(dim);
    start_key_ptr[1].set_min_value();
    start_key.assign(start_key_ptr, INV_IDX_ROWKEY_COL_CNT);

    end_key_ptr[0].set_uint32(dim);
    end_key_ptr[1].set_max_value();
    end_key.assign(end_key_ptr, INV_IDX_ROWKEY_COL_CNT);

    range.table_id_ = table_id;
    range.start_key_ = start_key;
    range.end_key_ = end_key;
  }
  return ret;
}

int ObDASSPIVMergeIter::set_inv_scan_range_key()
{
  int ret = OB_SUCCESS;
  uint32_t *dims = nullptr;
  int size = 0;
  if (OB_NOT_NULL(qvec_)) {
    dims = reinterpret_cast<uint32_t *>(qvec_->get_key_array()->get_data());
    size = qvec_->cardinality();
  }
  for (int i = 0; i < size && OB_SUCC(ret); ++i) {
    ObNewRange range;
    if (OB_FAIL(build_inv_scan_range(range, spiv_scan_ctdef_->ref_table_id_, dims[i]))) {
      LOG_WARN("failed to build inv scan range", K(ret), K(dims[i]));
    } else if (OB_FAIL(inv_scan_params_[i]->key_ranges_.push_back(range))) {
      LOG_WARN("failed to push inv scan range", K(ret), K(dims[i]));
    } else if (algo_ == BLOCK_MAX_WAND && OB_FAIL(block_max_scan_params_[i]->key_ranges_.push_back(range))) {
      LOG_WARN("failed to push block max scan range", K(ret), K(dims[i]));
    }
  }
  return ret;
}

int ObDASSPIVMergeIter::init_dim_iter_param(ObSPIVDimIterParam &dim_param, int64_t idx)
{
  int ret = OB_SUCCESS;
  dim_param.mem_context_ = mem_context_;
  dim_param.allocator_ = &allocator_;
  dim_param.eval_ctx_ = vec_aux_rtdef_->eval_ctx_;
  dim_param.inv_idx_scan_param_ = nullptr;
  dim_param.inv_idx_agg_iter_ = nullptr;
  dim_param.inv_idx_agg_expr_ = nullptr;
  dim_param.inv_idx_agg_param_ = nullptr;
  dim_param.inv_scan_domain_id_expr_ = spiv_scan_ctdef_->result_output_[0];
  dim_param.inv_scan_score_expr_ = vec_aux_ctdef_->spiv_scan_value_col_;
  dim_param.inv_idx_scan_ctdef_ = spiv_scan_ctdef_;
  dim_param.inv_idx_scan_rtdef_ = spiv_scan_rtdef_;

  uint32_t *keys = nullptr;
  float *values = nullptr;
  int size = 0;
  if (OB_NOT_NULL(qvec_)) {
    keys = reinterpret_cast<uint32_t *>(qvec_->get_key_array()->get_data());
    values = reinterpret_cast<float *>(qvec_->get_value_array()->get_data());
    size = qvec_->cardinality();
  }
  if (idx >= size || idx >= inv_dim_scan_iters_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index out of range", K(ret), K(idx), K(size));
  } else if (OB_FALSE_IT(dim_param.inv_idx_scan_iter_ = inv_dim_scan_iters_[idx])) {
  } else if (OB_FALSE_IT(dim_param.query_value_ = values[idx])) {
  } else if (OB_FALSE_IT(dim_param.dim_ = keys[idx])) {
  } else if (OB_ISNULL(dim_param.inv_idx_scan_param_ = OB_NEWx(ObTableScanParam, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for inv scan param", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_,
                 dim_docid_value_tablet_id_,
                 spiv_scan_ctdef_,
                 spiv_scan_rtdef_,
                 tx_desc_,
                 snapshot_,
                 *dim_param.inv_idx_scan_param_,
                 false))) {
    LOG_WARN("failed to init scan param", K(ret));
  }
  return ret;
}

int ObDASSPIVMergeIter::create_dim_iters()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(qvec_)) {
    int size = qvec_->cardinality();
    if (FALSE_IT(inv_scan_params_.set_allocator(&allocator_))) {
    } else if (OB_FAIL(inv_scan_params_.init(size))) {
      LOG_WARN("failed to init inv scan params array", K(ret));
    } else if (OB_FAIL(inv_scan_params_.prepare_allocate(size))) {
      LOG_WARN("failed to prepare allocate inv scan params array", K(ret));
    } else if (algo_ != SPIVAlgo::BLOCK_MAX_WAND) {
    } else if (FALSE_IT(block_max_scan_params_.set_allocator(&allocator_))) {
    } else if (OB_FAIL(block_max_scan_params_.init(size))) {
      LOG_WARN("failed to init inv scan params array", K(ret));
    } else if (OB_FAIL(block_max_scan_params_.prepare_allocate(size))) {
      LOG_WARN("failed to prepare allocate inv scan params array", K(ret));
    } else if(OB_FAIL(block_max_iter_param_.init(*vec_aux_ctdef_, allocator_))) {
      LOG_WARN("failed to init block max iter param", K(ret));
    }
    for (int i = 0; i < size && OB_SUCC(ret); i++) {
      ObSPIVDimIterParam dim_param;
      ObISRDaaTDimIter *dim_iter = nullptr;
      if (OB_FAIL(init_dim_iter_param(dim_param, i))) {
        LOG_WARN("failed to init dim iter param");
      }
      inv_scan_params_[i] = dim_param.inv_idx_scan_param_;
      if (OB_FAIL(ret)){
      } else if(algo_ == SPIVAlgo::BLOCK_MAX_WAND) {
        ObSPIVBlockMaxDimIter *block_max_iter = nullptr;
        if (OB_ISNULL(block_max_scan_params_[i] = OB_NEWx(ObTableScanParam, &allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for block max scan param", K(ret));
        } else if (OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_,
                       dim_docid_value_tablet_id_,
                       block_max_scan_ctdef_,
                       block_max_scan_rtdef_,
                       tx_desc_,
                       snapshot_,
                       *block_max_scan_params_[i],
                       false))) {
          LOG_WARN("failed to init scan param", K(ret));
        } else if (OB_ISNULL(block_max_iter = OB_NEWx(ObSPIVBlockMaxDimIter, &allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for spiv block max iter", K(ret));
        } else if (OB_FAIL(block_max_iter->init(dim_param, block_max_iter_param_, *block_max_scan_params_[i]))) {
          LOG_WARN("failed to init spiv block max iter", K(ret));
        } else {
          dim_iter = block_max_iter;
        }
      } else if (algo_ == SPIVAlgo::DAAT_NAIVE) {
        ObSPIVDaaTDimIter *iter = nullptr;
        if (OB_ISNULL(iter = OB_NEWx(ObSPIVDaaTDimIter, &allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory for dim iter");
        } else if (OB_FAIL(iter->init(dim_param))) {
          LOG_WARN("failed to init dim iter");
        } else {
          dim_iter = iter;
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported sparse vector query algo", K(ret), K_(algo));
      }
      if (OB_SUCC(ret) && OB_FAIL(dim_iters_.push_back(dim_iter))) {
        LOG_WARN("failed to push back to dim iters");
      }
    }
  }

  return ret;
}

int ObDASSPIVMergeIter::init_spiv_merge_param(ObSPIVDaaTParam &iter_param)
{
  int ret = OB_SUCCESS;
  if(OB_ISNULL(vec_aux_ctdef_) || OB_ISNULL(vec_aux_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vec_aux_ctdef is NULL", K_(vec_aux_ctdef), K_(vec_aux_rtdef));
  } else {
    iter_param.allocator_ = &allocator_;
    iter_param.dim_iters_ = &dim_iters_;
    iter_param.is_pre_filter_ = vec_aux_ctdef_->is_pre_filter();
    iter_param.is_use_docid_ = is_use_docid();
    base_param_.dim_weights_ = nullptr;
    base_param_.limit_param_ = &limit_param_;
    base_param_.eval_ctx_ = vec_aux_rtdef_->eval_ctx_;
    base_param_.id_proj_expr_ = vec_aux_ctdef_->spiv_scan_docid_col_;
    base_param_.topk_limit_ = limit_param_.limit_ + limit_param_.offset_;
    if (!vec_aux_ctdef_->is_pre_filter() && selectivity_ < 1.0) {
      base_param_.topk_limit_ = base_param_.topk_limit_ * 2;
    }
    base_param_.relevance_proj_expr_ = nullptr;
    base_param_.filter_expr_ = nullptr;
    iter_param.base_param_ = &base_param_;
    ObSRDaaTInnerProductRelevanceCollector *inner_product_relevance_collector = nullptr;
    if (OB_ISNULL(
            inner_product_relevance_collector = OB_NEWx(ObSRDaaTInnerProductRelevanceCollector, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for inner product relevance collector", K(ret));
    } else if (OB_FAIL(inner_product_relevance_collector->init())) {
      LOG_WARN("failed to init boolean relevance collector", K(ret));
    } else {
      iter_param.relevance_collector_ = inner_product_relevance_collector;
    }
  }
  return ret;
}

int ObDASSPIVMergeIter::create_spiv_merge_iter()
{
  int ret = OB_SUCCESS;
  ObSPIVDaaTParam param;
  switch (algo_) {
    case SPIVAlgo::DAAT_NAIVE:{
      ObSPIVDaaTNaiveIter *iter = nullptr;
      if (OB_FAIL(init_spiv_merge_param(param))) {
        LOG_WARN("failed to init merge param");
      } else if (OB_ISNULL(iter = OB_NEWx(ObSPIVDaaTNaiveIter, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for daat iter");
      } else if (OB_FAIL(iter->init(param))) {
          LOG_WARN("failed to init daat naive iter");
      } else {
        spiv_iter_ = iter;
      }
      break;
    }
    case SPIVAlgo::BLOCK_MAX_WAND: {
      ObSPIVBMWIter *iter = nullptr;
      if (OB_FAIL(init_spiv_merge_param(param))) {
        LOG_WARN("failed to init merge param");
      } else if (OB_ISNULL(iter = OB_NEWx(ObSPIVBMWIter, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for daat iter");
      } else if (OB_FAIL(iter->init(param))) {
          LOG_WARN("failed to init daat naive iter");
      } else {
        spiv_iter_ = iter;
      }
      break;
    }
    case SPIVAlgo::DAAT_MAX_SCORE:
    case SPIVAlgo::WAND:
    case SPIVAlgo::BLOCK_MAX_MAX_SCORE:
    case SPIVAlgo::TAAT_NAIVE: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported sparse vector query algorithm", K(ret), K_(algo));
      break;
    }
  }
  return ret;
}

int ObDASSPIVMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;

  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }

  if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->reuse())) {
    LOG_WARN("failed to reuse inv idx scan iter", K(ret));
  } else if (!aux_data_table_first_scan_ && OB_FAIL(reuse_aux_data_iter())) {
    LOG_WARN("failed to reuse com aux vec iter", K(ret));
  } else if (!rowkey_docid_table_first_scan_ && OB_FAIL(reuse_rowkey_docid_iter())) {
    LOG_WARN("failed to reuse rowkey vid iter", K(ret));
  } else {
    for (int i = 0; i < inv_dim_scan_iters_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(ObDasVecScanUtils::reuse_iter(
              ls_id_, inv_dim_scan_iters_[i], *inv_scan_params_[i], dim_docid_value_tablet_id_))) {
        LOG_WARN("failed to reuse inv dim scan iter", K(ret));
      }
    }
    if (OB_ISNULL(spiv_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("spiv iter is null", K(ret));
    } else {
      spiv_iter_->reuse();
    }
  }
  saved_rowkeys_.reset();
  valid_docid_set_.clear();
  result_docids_.reset();
  result_docids_curr_iter_ = OB_INVALID_INDEX_INT64;
  is_pre_processed_ = false;

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
  for (int i = 0; i < inv_dim_scan_iters_.count() && OB_SUCC(ret); ++i) {
    if (OB_NOT_NULL(inv_dim_scan_iters_[i]) && OB_FAIL(inv_dim_scan_iters_[i]->release())) {
      LOG_WARN("failed to release dim scan iter", K(ret));
      tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
      ret = OB_SUCCESS;
    }
  }
  inv_dim_scan_iters_.reset();
  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  inv_idx_scan_iter_ = nullptr;
  aux_data_iter_ = nullptr;
  rowkey_docid_iter_ = nullptr;

  tx_desc_ = nullptr;
  snapshot_ = nullptr;
  vec_aux_ctdef_ = nullptr;
  vec_aux_rtdef_ = nullptr;
  sort_ctdef_ = nullptr;
  sort_rtdef_ = nullptr;
  qvec_ = nullptr;
  qvec_expr_ = nullptr;
  distance_calc_ = nullptr;

  saved_rowkeys_.reset();
  valid_docid_set_.destroy();
  result_docids_.reset();
  result_docids_curr_iter_ = OB_INVALID_INDEX_INT64;
  dim_iters_.reset();
  if (OB_NOT_NULL(spiv_iter_)) {
    spiv_iter_->~ObISparseRetrievalMergeIter();
    spiv_iter_ = nullptr;
  }
  is_pre_processed_ = false;

  ObDasVecScanUtils::release_scan_param(aux_data_scan_param_);
  ObDasVecScanUtils::release_scan_param(rowkey_docid_scan_param_);
  for(int64_t i = 0; i < inv_scan_params_.count(); i++) {
    if (OB_NOT_NULL(inv_scan_params_[i])) {
      ObDasVecScanUtils::release_scan_param(*inv_scan_params_[i]);
    }
  }
  inv_scan_params_.reset();
  for(int64_t i = 0; i < block_max_scan_params_.count(); i++) {
    if (OB_NOT_NULL(block_max_scan_params_[i])) {
      ObDasVecScanUtils::release_scan_param(*block_max_scan_params_[i]);
    }
  }
  block_max_scan_params_.reset();

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
  int64_t count = 0;
  int ret = inner_get_next_rows(count, 1);
  return ret;
}

int ObDASSPIVMergeIter::project_brute_result(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(qvec_) || limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
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
        set_datum_func_(docid_datum[i], result_docids_.at(result_docids_curr_iter_++));
      }
      docid_expr->set_evaluated_projected(*vec_aux_rtdef_->eval_ctx_);
    }
  }

  return ret;
}

int ObDASSPIVMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool is_vectorized = capacity > 1 ? true : false;
  if (OB_ISNULL(qvec_) || limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (vec_aux_ctdef_->is_pre_filter() && !is_pre_processed_) {
    if(OB_FAIL(pre_process(is_vectorized))) {
      LOG_WARN("failed to pre process", K(ret));
    }
  }
  if(OB_FAIL(ret)) {
  } else if (result_docids_.count() != 0) {
    if(OB_FAIL(project_brute_result(count, capacity))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to project brute result", K(ret));
      }
    }
  } else {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("not inited", K(ret));
    } else if (OB_ISNULL(spiv_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("spiv iter is null", K(ret));
    } else if (OB_UNLIKELY(0 == capacity)) {
      count = 0;
    } else {
      if(OB_FAIL(spiv_iter_->get_next_rows(capacity, count))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next rows", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDASSPIVMergeIter::get_ctdef_with_rowkey_exprs(const ObDASScanCtDef *&ctdef, ObDASScanRtDef *&rtdef)
{
  int ret = OB_SUCCESS;
  ctdef = nullptr;
  rtdef = nullptr;

  if (!is_use_docid()) {
    int idx = get_aux_data_tbl_idx();
    ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(idx, ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
    rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(idx);
  } else {
    ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_spiv_rowkey_docid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
    rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_spiv_rowkey_docid_tbl_idx());
  }

  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(ctdef), KP(rtdef));
  }

  return ret;
}

int ObDASSPIVMergeIter::get_rowkey_pre_filter(ObIAllocator &allocator, bool is_vectorized, int64_t batch_count){
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *ctdef = nullptr;
  ObDASScanRtDef *rtdef = nullptr;
  if (OB_FAIL(get_ctdef_with_rowkey_exprs(ctdef, rtdef))) {
    LOG_WARN("failed to get ctdef with rowkey exprs", K(ret));
  }

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

  bool is_iter_end = false;
  const ObDASScanCtDef *ctdef = nullptr;
  ObDASScanRtDef *rtdef = nullptr;
  if (OB_FAIL(get_ctdef_with_rowkey_exprs(ctdef, rtdef))) {
    LOG_WARN("failed to get ctdef with rowkey exprs", K(ret));
  }

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
        } else if (is_use_docid() && OB_FAIL(ObDasVecScanUtils::set_lookup_key(*rowkey, rowkey_docid_scan_param_, ctdef->ref_table_id_))) {
          LOG_WARN("failed to set rowkey.", K(ret));
        } else if (!is_use_docid()) {
          ObDocIdExt docid;
          if (OB_FAIL(rowkey2docid(*rowkey, docid))) {
            LOG_WARN("failed to get docid", K(ret));
          } else if (OB_FAIL(valid_docid_set_.set_refactored(docid))){
            LOG_WARN("failed to insert valid docid set", K(ret));
          }
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
          } else if (is_use_docid() && OB_FAIL(ObDasVecScanUtils::set_lookup_key(*rowkey, rowkey_docid_scan_param_, ctdef->ref_table_id_))) {
            LOG_WARN("failed to set rowkey.", K(ret));
          } else if (!is_use_docid()) {
            ObDocIdExt docid;
            if (OB_FAIL(rowkey2docid(*rowkey, docid))) {
              LOG_WARN("failed to get docid", K(ret));
            } else if (OB_FAIL(valid_docid_set_.set_refactored(docid))){
              LOG_WARN("failed to insert valid docid set", K(ret));
            }
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!is_use_docid()) {
      // do nothing
    } else if (rowkey_docid_scan_param_.key_ranges_.count() != 0) {
      if (OB_FAIL(do_rowkey_docid_table_scan())) {
        LOG_WARN("failed to do rowkey docid table scan", K(ret));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < batch_count; ++i) {
        ObDocIdExt docid;
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

int ObDASSPIVMergeIter::rowkey2docid(ObRowkey &rowkey, ObDocIdExt &docid)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_use_docid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not use rowkey as docid when use docid", K(ret));
  } else if (OB_UNLIKELY(rowkey.length() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey length", K(ret), K(rowkey.length()));
  } else if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey.length()));
  } else if (OB_FAIL(docid.from_obj(*rowkey.ptr()))){
    LOG_WARN("failed to get docid from rowkey", K(ret));
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

  int idx = get_aux_data_tbl_idx();
  const ObDASScanCtDef *aux_data_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(idx, ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
  const ObDASScanRtDef *aux_data_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(idx);

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

  if (OB_FAIL(ret)) {
  } else if (is_use_docid()) {
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

      ObDocIdExt cur_docid;
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
  } else {
    while (OB_SUCC(ret) && !max_heap.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_count && !max_heap.empty(); ++i) {
        ObRowkey *rowkey = max_heap.top().rowkey_;
        if (OB_FAIL(max_heap.pop())) {
          LOG_WARN("failed to pop rowkey from heap", K(ret));
        } else if (OB_ISNULL(rowkey)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get rowkey from max heap.", K(ret));
        } else {
          ObDocIdExt cur_docid;
          if (OB_FAIL(rowkey2docid(*rowkey, cur_docid))) {
            LOG_INFO("failed to get docid from rowkey", K(ret));
          } else if (OB_FAIL(result_docids_.push_back(cur_docid))) {
            LOG_WARN("failed to push back docid", K(ret));
          }
        }
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

  if (is_use_docid()) {
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
        ObDocIdExt docid;
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
  } else {
    int64_t rowkey_idx = 0;
    int rowkeys_size = saved_rowkeys_.count();

    while (OB_SUCC(ret) && rowkey_idx < rowkeys_size) {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_count && rowkey_idx < rowkeys_size; ++i) {
        ObRowkey *rowkey;
        ObDocIdExt docid;
        if (OB_ISNULL(rowkey = saved_rowkeys_.at(rowkey_idx++))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get rowkey from saved rowkeys.", K(ret));
        } else if (OB_FAIL(rowkey2docid(*rowkey, docid))) {
          LOG_WARN("failed to set docid.", K(ret));
        } else if (OB_FAIL(valid_docid_set_.set_refactored(docid))){
          LOG_WARN("failed to insert valid docid set", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDASSPIVMergeIter::pre_process(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
  int64_t batch_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  if (OB_FAIL(get_rowkey_pre_filter(allocator, is_vectorized, batch_count))) {
    LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
  } else if (saved_rowkeys_.count() < MAX_SPIV_BRUTE_FORCE_SIZE) {
    if (OB_FAIL(do_brute_force(allocator, is_vectorized, batch_count))) {
      LOG_WARN("failed to do brute force", K(ret));
    } else if (result_docids_.count() != 0) {
      result_docids_curr_iter_ = 0;
    }
  } else if (OB_FAIL(set_valid_docids_with_rowkeys(allocator, batch_count))) {
    LOG_WARN("failed to set valid docids", K(ret));
  } else if (OB_FAIL(get_rowkey_and_set_docids(allocator, is_vectorized, batch_count))) {
    LOG_WARN("failed to get rowkeys and set valid docids", K(ret));
  } else {
    if (algo_ == SPIVAlgo::DAAT_NAIVE) {
      ObSPIVDaaTIter *iter = static_cast<ObSPIVDaaTIter *>(spiv_iter_);
      if(OB_FAIL(iter->set_valid_docid_set(valid_docid_set_))) {
        LOG_WARN("failed to set valid docid set", K(ret));
      }
    } else if (algo_ == SPIVAlgo::BLOCK_MAX_WAND) {
       ObSPIVBMWIter *iter = static_cast<ObSPIVBMWIter *>(spiv_iter_);
      if(OB_FAIL(iter->set_valid_docid_set(valid_docid_set_))) {
        LOG_WARN("failed to set valid docid set", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported sparse vector query algorithm", K(ret), K_(algo));
    }
  }
  if(OB_SUCC(ret)) {
    is_pre_processed_ = true;
  }
  return ret;
}

int ObDASSPIVMergeIter::do_aux_data_table_scan()
{
  int ret = OB_SUCCESS;

  if (aux_data_table_first_scan_) {
    int idx = get_aux_data_tbl_idx();
    const ObDASScanCtDef *aux_data_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(idx, ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
    ObDASScanRtDef *aux_data_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(idx);
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
  int idx =get_aux_data_tbl_idx();
  const ObDASScanCtDef *aux_data_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(idx, ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
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

int ObDASSPIVMergeIter::get_docid_from_rowkey_docid_table(ObDocIdExt &docid)
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
    if (OB_FAIL(docid.from_datum(docid_datum))) {
      LOG_WARN("invalid docid.", K(ret));
    }
  }

  return ret;
}


} // namespace sql
} // namespace oceanbase
