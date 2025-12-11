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
#include "sql/engine/expr/ob_expr_vector.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "sql/das/iter/ob_das_functional_lookup_iter.h"
#include "sql/das/ob_das_utils.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/das/iter/ob_das_func_data_iter.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace transaction;
using namespace share;

namespace sql
{


int ObDASHNSWScanIter::reuse_pre_filter_by_type()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pre_scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pre-filter scan_param is null", K(ret));
  } else {
    pre_scan_param_->need_switch_param_ = false;
  }
  return ret;
}

int ObDASHNSWScanIter::do_table_scan()
{
  int ret = OB_SUCCESS;

  if (!is_primary_pre_with_rowkey_with_filter_) {
    if (is_pre_filter() || is_in_filter()) {
      if (OB_ISNULL(inv_idx_scan_iter_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inv idx scan iter is null", K(ret));
      } else if (OB_FAIL(reuse_pre_filter_by_type())) {
        LOG_WARN("failed to reuse pre filter by type", K(ret));
      } else if (OB_FAIL(inv_idx_scan_iter_->do_table_scan())) {
        LOG_WARN("failed to do inv idx table scan.", K(ret));
      } else {
        idx_iter_first_scan_ = false;
      }
    }
  } else {
    if (!is_pre_filter() && !is_in_filter()) {
    } else if (OB_ISNULL(rowkey_vid_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey vid iter is null", K(ret));
    } else if (OB_FAIL(build_rowkey_vid_range())) {
      LOG_WARN("fail to build rowkey vid range", K(ret));
    } else if (OB_FAIL(do_rowkey_vid_table_scan())) {
      LOG_WARN("fail to do rowkey vid table scan.", K(ret));
    }
  }

  return ret;
}

int ObDASHNSWScanIter::build_rowkey_vid_range()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inv_idx_scan_iter_) || OB_ISNULL(vec_aux_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpeted error, data table iter or ctdef is nullptr", K(ret));
  } else {
    const ObDASScanCtDef *rowkey_vid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
    ObDASScanIter *inv_idx_scan_iter = static_cast<ObDASScanIter *>(inv_idx_scan_iter_);

    const common::ObIArray<common::ObNewRange> &key_ranges = inv_idx_scan_iter->get_scan_param().key_ranges_;
    const common::ObIArray<common::ObNewRange> &ss_key_ranges = inv_idx_scan_iter->get_scan_param().ss_key_ranges_;
    if (OB_ISNULL(rowkey_vid_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey vid ctdef is null", K(ret));
    } else {
      rowkey_vid_scan_param_.enable_new_false_range_ = inv_idx_scan_iter->get_scan_param().enable_new_false_range_;
      for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
        ObNewRange key_range = key_ranges.at(i);
        key_range.table_id_ = rowkey_vid_ctdef->ref_table_id_;
        if (OB_FAIL(rowkey_vid_scan_param_.key_ranges_.push_back(key_range))) {
          LOG_WARN("fail to push back key range for rowkey vid scan param", K(ret), K(key_range));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < ss_key_ranges.count(); ++i) {
        ObNewRange ss_key_range = ss_key_ranges.at(i);
        ss_key_range.table_id_ = rowkey_vid_ctdef->ref_table_id_;
        if (OB_FAIL(rowkey_vid_scan_param_.ss_key_ranges_.push_back(ss_key_range))) {
          LOG_WARN("fail to push back ss key range for rowkey vid scan param", K(ret), K(ss_key_range));
        }
      }
    }
  }

  return ret;
}

int ObDASHNSWScanIter::rescan()
{
  int ret = OB_SUCCESS;
  if (!is_primary_pre_with_rowkey_with_filter_) {
    if (is_pre_filter() || is_in_filter()) {
      if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->rescan())) {
        LOG_WARN("failed to rescan inv idx scan iter", K(ret));
      }
    }
  } else {
    if (is_pre_filter() || is_in_filter()) {
      if (OB_ISNULL(rowkey_vid_iter_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey vid iter is null", K(ret));
      } else if (OB_FAIL(build_rowkey_vid_range())) {
        LOG_WARN("fail to build rowkey vid range", K(ret));
      } else if (OB_FAIL(do_rowkey_vid_table_scan())) {
        LOG_WARN("fail to do rowkey vid table scan.", K(ret));
      }
    }
  }

  return ret;
}

void ObDASHNSWScanIter::clear_evaluated_flag()
{
  if (!is_primary_pre_with_rowkey_with_filter_) {
    if (OB_NOT_NULL(inv_idx_scan_iter_)) {
      inv_idx_scan_iter_->clear_evaluated_flag();
    }
  } else {
    if (OB_NOT_NULL(rowkey_vid_iter_)) {
      rowkey_vid_iter_->clear_evaluated_flag();
    }
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
    use_vid_ = hnsw_scan_param.use_vid_;

    inv_idx_scan_iter_ = hnsw_scan_param.inv_idx_scan_iter_;
    delta_buf_iter_ = hnsw_scan_param.delta_buf_iter_;
    index_id_iter_ = hnsw_scan_param.index_id_iter_;
    snapshot_iter_ = hnsw_scan_param.snapshot_iter_;
    vid_rowkey_iter_ = hnsw_scan_param.vid_rowkey_iter_;
    com_aux_vec_iter_ = hnsw_scan_param.com_aux_vec_iter_;
    rowkey_vid_iter_ = hnsw_scan_param.rowkey_vid_iter_;
    data_filter_iter_ = hnsw_scan_param.data_filter_iter_;

    vec_aux_ctdef_ = hnsw_scan_param.vec_aux_ctdef_;
    vec_aux_rtdef_ = hnsw_scan_param.vec_aux_rtdef_;
    vid_rowkey_ctdef_ = hnsw_scan_param.vid_rowkey_ctdef_;
    vid_rowkey_rtdef_ = hnsw_scan_param.vid_rowkey_rtdef_;
    data_filter_ctdef_ = hnsw_scan_param.data_filter_ctdef_;
    data_filter_rtdef_ = hnsw_scan_param.data_filter_rtdef_;
    sort_ctdef_ = hnsw_scan_param.sort_ctdef_;
    sort_rtdef_ = hnsw_scan_param.sort_rtdef_;
    vec_index_type_ = hnsw_scan_param.vec_index_type_;
    vec_idx_try_path_ = hnsw_scan_param.vec_idx_try_path_;
    dim_ = vec_aux_ctdef_->dim_;
    extra_column_count_ = vec_aux_ctdef_->extra_column_count_;
    is_primary_pre_with_rowkey_with_filter_ = vec_aux_ctdef_->can_use_vec_pri_opt();
    pre_scan_param_ = hnsw_scan_param.pre_scan_param_;
    adaptive_ctx_.selectivity_ = vec_aux_ctdef_->selectivity_;
    adaptive_ctx_.row_count_ = vec_aux_ctdef_->row_count_;
    adaptive_ctx_.can_extract_range_ = hnsw_scan_param.can_extract_range_;
    adaptive_ctx_.is_primary_index_ = hnsw_scan_param.is_primary_index_;
    adaptive_ctx_.with_extra_info_ = extra_column_count_ > 0;
    adaptive_ctx_.only_rowkey_filter_ = is_primary_pre_with_rowkey_with_filter_;
    func_lookup_ctdef_ = hnsw_scan_param.func_lookup_ctdef_;
    func_lookup_rtdef_ = hnsw_scan_param.func_lookup_rtdef_;
    func_lookup_iter_ = hnsw_scan_param.func_lookup_iter_;
    pre_filter_iter_ = hnsw_scan_param.pre_filter_iter_;

    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "HNSW", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create vector hnsw memory context", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(sort_ctdef_) && OB_NOT_NULL(sort_rtdef_)) {
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

    if (OB_SUCC(ret) && is_adaptive_filter()) {
      if (vec_aux_ctdef_->relevance_col_cnt_ > 0) {
        //vec_idx_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER;
        vec_idx_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER;
        if (pre_filter_iter_ == nullptr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pre filter iter is null", K(ret));
        }
      } else {
        ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(*exec_ctx_);
        ObVecIdxAdaTryPath cur_path = ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN;
        if (OB_ISNULL(plan_ctx->get_phy_plan())) {
          // remote scan, phy plan is null, do nothing, just use try path in ctdef
          LOG_WARN("plan ctx is null", K(ret), KP(plan_ctx));
        } else if (OB_FALSE_IT(cur_path = static_cast<ObVecIdxAdaTryPath>(plan_ctx->get_phy_plan()->stat_.vec_index_exec_ctx_.cur_path_))) {
        } else if (cur_path != vec_idx_try_path_ &&
                  cur_path > ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN &&
                  cur_path < ObVecIdxAdaTryPath::VEC_PATH_MAX) {
          vec_idx_try_path_ = cur_path;
        }
      }
    }

    if (OB_SUCC(ret) && (is_adaptive_filter() || is_iter_filter())) {
      if ((OB_ISNULL(data_filter_ctdef_) || OB_ISNULL(data_filter_rtdef_)
         || (vec_aux_ctdef_->relevance_col_cnt_ > 0 && OB_ISNULL(func_lookup_iter_)))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid das iter param type for hnsw scan iter", K(ret), K(param));
      }
    }

    if (OB_SUCC(ret) && extra_column_count_ > 0 && OB_FAIL(get_extra_idx_in_outexprs(extra_in_rowkey_idxs_))) {
      LOG_WARN("failed to get extra idx in outexprs", K(ret), K(extra_column_count_));
    } else if (OB_NOT_NULL(rowkey_vid_iter_)) {
      rowkey_vid_iter_->set_scan_param(rowkey_vid_scan_param_);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObVectorIndexParam::build_search_param(vec_aux_ctdef_->vector_index_param_, vec_aux_ctdef_->vec_query_param_, search_param_))) {
        LOG_WARN("build search param fail", K(vec_aux_ctdef_->vector_index_param_), K(vec_aux_ctdef_->vec_query_param_));
      } else {
        LOG_TRACE("search param", K(vec_aux_ctdef_->vector_index_param_), K(vec_aux_ctdef_->vec_query_param_), K(search_param_));
      }
    }
  }

  LOG_TRACE("vector index show basic hnsw search info", K(dim_), K(extra_column_count_), K(is_primary_pre_with_rowkey_with_filter_),
                                                        K(data_filter_ctdef_), K(vec_index_type_), K(vec_idx_try_path_), K(adaptive_ctx_));
  return ret;
}

int ObDASHNSWScanIter::reuse_func_lookup_iter()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (func_lookup_iter_->get_type() == ObDASIterType::DAS_ITER_FUNC_LOOKUP) {
    ObDASFuncLookupIter* func_iter = static_cast<ObDASFuncLookupIter*>(func_lookup_iter_);
    if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, func_iter->get_index_scan_iter(), func_filter_scan_param_, rowkey_doc_tablet_id_))) {
      LOG_WARN("failed to set lookup key", K(ret));
      tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
      ret = OB_SUCCESS;
    }
  }
  if (FAILEDx(func_lookup_iter_->reuse())) {
    LOG_WARN("failed to reuse func lookup iter", K(ret));
  }

  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }
  return ret;
}

int ObDASHNSWScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!idx_iter_first_scan_ && OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->reuse())) {
    LOG_WARN("failed to reuse inv idx scan iter", K(ret));
    tmp_ret = ret;
    ret = OB_SUCCESS;
  }
  if (!com_aux_vec_iter_first_scan_ && OB_FAIL(reuse_com_aux_vec_iter())) {
    LOG_WARN("failed to reuse com aux vec iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(rowkey_vid_iter_) && !rowkey_vid_iter_first_scan_ && OB_FAIL(reuse_rowkey_vid_iter())) {
    LOG_WARN("failed to reuse rowkey vid iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(vid_rowkey_iter_) && !vid_rowkey_iter_first_scan_ && OB_FAIL(reuse_vid_rowkey_iter())) {
    LOG_WARN("failed to reuse vid rowkey iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }

  if (!data_filter_iter_first_scan_ && OB_FAIL(reuse_filter_data_table_iter())) {
    LOG_WARN("failed to reuse data filter iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }

  if (!func_lookup_first_scan_ && OB_NOT_NULL(func_lookup_iter_) && OB_FAIL(reuse_func_lookup_iter())) {
    LOG_WARN("failed to reuse func lookup iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }

  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
  if (OB_NOT_NULL(tmp_adaptor_vid_iter_)) {
    tmp_adaptor_vid_iter_->reset();
    tmp_adaptor_vid_iter_->~ObVectorQueryVidIterator();
    tmp_adaptor_vid_iter_ = nullptr;
  }
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }
  rel_map_.reuse();
  vec_op_alloc_.reset();
  query_cond_.reset();
  go_brute_force_ = false;
  only_complete_data_ = false;

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
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(data_filter_iter_) && OB_FAIL(data_filter_iter_->release())) {
    LOG_WARN("failed to release data filter iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }
  if (OB_NOT_NULL(func_lookup_iter_) && OB_FAIL(func_lookup_iter_->release())) {
    LOG_WARN("failed to release func lookup filter iter", K(ret));
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
  data_filter_iter_ = nullptr;

  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
  tx_desc_ = nullptr;
  snapshot_ = nullptr;

  ObDasVecScanUtils::release_scan_param(delta_buf_scan_param_);
  ObDasVecScanUtils::release_scan_param(index_id_scan_param_);
  ObDasVecScanUtils::release_scan_param(snapshot_scan_param_);
  ObDasVecScanUtils::release_scan_param(vid_rowkey_scan_param_);
  ObDasVecScanUtils::release_scan_param(com_aux_vec_scan_param_);
  ObDasVecScanUtils::release_scan_param(rowkey_vid_scan_param_);
  ObDasVecScanUtils::release_scan_param(data_filter_scan_param_);
  ObDasVecScanUtils::release_scan_param(func_filter_scan_param_);

  vec_aux_ctdef_ = nullptr;
  vec_aux_rtdef_ = nullptr;
  vid_rowkey_ctdef_ = nullptr;
  vid_rowkey_rtdef_ = nullptr;
  data_filter_ctdef_ = nullptr;
  data_filter_rtdef_ = nullptr;
  sort_ctdef_ = nullptr;
  sort_rtdef_ = nullptr;
  search_vec_ = nullptr;
  distance_calc_ = nullptr;
  query_cond_.reset();

  if (nullptr != mem_context_)  {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  extra_in_rowkey_idxs_.reset();
  vec_op_alloc_.reset();
  adaptive_ctx_.reset();
  rel_map_.destroy();
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
    eval_info.set_evaluated(true);
    eval_info.set_projected(true);
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
  } else if (OB_FAIL(adaptor_vid_iter_->get_next_row(row, vec_aux_ctdef_->result_output_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next next row from adaptor vid iter", K(ret));
    }
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be null", K(ret));
  } else if (row->get_count() != extra_column_count_ + vec_aux_ctdef_->relevance_col_cnt_ + 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be one row", K(row->get_count()), K(extra_column_count_), K(ret));
  } else {
    if (need_save_distance_result()
      && OB_FAIL(save_distance_expr_result(row->get_cell(0)))) {
      LOG_WARN("failed to set distance", K(ret));
    } else {
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_idx(0);
      // res_exprs: vid_column, extra_column...
      const ExprFixedArray& res_exprs = vec_aux_ctdef_->result_output_;
      if (res_exprs.count() != extra_column_count_ + vec_aux_ctdef_->relevance_col_cnt_ + 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("res_exprs count is not equal to extra_column_count_ + rel_col + 2", K(ret), K(res_exprs), K(extra_column_count_), K(vec_aux_ctdef_->relevance_col_cnt_));
      } else {
        // row.cell(0): dis, row.cell(1): vid, row.cell(...): extra_col...
        int row_cell_idx = 1;
        // first_expr is: vid_column, ...is: extra_column...
        for (int64_t i = 0; OB_SUCC(ret) && i < res_exprs.count(); ++i) {
          ObExpr *expr = res_exprs.at(i);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("should not be null", K(ret));
          } else {
            ObDatum &datum = expr->locate_datum_for_write(*vec_aux_rtdef_->eval_ctx_);
            if (OB_FAIL(datum.from_obj(row->get_cell(row_cell_idx++)))) {
              LOG_WARN("failed to from obj", K(ret));
            }
          }
        }
      }
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
  } else if (OB_FAIL(adaptor_vid_iter_->get_next_rows(row, count, vec_aux_ctdef_->result_output_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next next row from adaptor vid iter", K(ret));
    }
  } else if (count > 0) {
    ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
    guard.set_batch_size(count);
    // res_exprs: vid_column, extra_column...
    const ExprFixedArray& res_exprs = vec_aux_ctdef_->result_output_;
    int rel_count = vec_aux_ctdef_->relevance_col_cnt_;
    if (res_exprs.count() != extra_column_count_ + rel_count + 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("res_exprs count is not equal to extra_column_count_ + 1", K(ret), K(res_exprs), K(extra_column_count_), K(rel_count));
    } else {
      int64_t a_batch_obj_cnt = 2 + extra_column_count_ + rel_count;
      for (int64_t idx_exp = 0; OB_SUCC(ret) && idx_exp < res_exprs.count(); ++idx_exp) {
        ObExpr *expr = res_exprs.at(idx_exp);
        ObDatum *datum = nullptr;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret));
        } else if (OB_ISNULL(datum = expr->locate_datums_for_update(*vec_aux_rtdef_->eval_ctx_, count))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, datums is nullptr", K(ret), KPC(expr));
        } else {
          // row.cell(0):               dis, row.cell(1):                   vid
          // row.cell(a_batch_obj_cnt): dis, row.cell(a_batch_obj_cnt + 1): vid
          for (int64_t num = 0; OB_SUCC(ret) && num < count; ++num) {
            guard.set_batch_idx(num);
            if (OB_FAIL(datum[num].from_obj(row->get_cell(num * a_batch_obj_cnt + 1 + idx_exp)))) {
              LOG_WARN("fail to from obj", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            expr->set_evaluated_projected(*vec_aux_rtdef_->eval_ctx_);
          }
        }
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
  int rel_count = vec_aux_ctdef_->relevance_col_cnt_;
  if (need_save_distance_result()) {
    int64_t a_batch_obj_cnt = 2 + extra_column_count_ + rel_count;
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*sort_rtdef_->eval_ctx_);
    batch_info_guard.set_batch_size(size);

    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      ObObj& dist_obj = row->get_cell(i * a_batch_obj_cnt);
      batch_info_guard.set_batch_idx(i);

      ObDatum& wr_datum = distance_calc_->locate_datum_for_write(*sort_rtdef_->eval_ctx_);
      wr_datum.set_double(dist_obj.get_float());

      ObEvalInfo &eval_info = distance_calc_->get_eval_info(*sort_rtdef_->eval_ctx_);
      eval_info.set_evaluated(true);
      eval_info.set_projected(true);
    }
  }

  return ret;
}

// cannot run post filter when parallel with block granule
bool ObDASHNSWScanIter::check_need_force_switch_run_path()
{
  return can_retry_ && (vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER) && is_parallel_with_block_granule();
}

bool ObDASHNSWScanIter::is_parallel_with_block_granule()
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

int ObDASHNSWScanIter::check_iter_filter_need_retry()
{
  int ret = OB_SUCCESS;
  double iter_selectivity = double(adaptive_ctx_.iter_res_row_cnt_) /  double(adaptive_ctx_.iter_filter_row_cnt_);
  double output_row_cnt = iter_selectivity * adaptive_ctx_.row_count_;
  if (adaptive_ctx_.iter_times_ > 2) {
    if (adaptive_ctx_.is_primary_index_) {
      ret = (output_row_cnt <= ObVecIdxExtraInfo::MAX_HNSW_PRE_ROW_CNT_WITH_ROWKEY
            && iter_selectivity < ObVecIdxExtraInfo::DEFAULT_PRE_RATE_FILTER_WITH_ROWKEY) ?
            OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY : OB_SUCCESS;
    } else {
      ret = (output_row_cnt <= ObVecIdxExtraInfo::MAX_HNSW_PRE_ROW_CNT_WITH_IDX
            && iter_selectivity < ObVecIdxExtraInfo::DEFAULT_PRE_RATE_FILTER_WITH_IDX) ?
            OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY : OB_SUCCESS;
    }
  }

  LOG_TRACE("switch path check iter filter need retry:", K(ret), K(adaptive_ctx_), K(iter_selectivity), K(output_row_cnt));
  return ret;
}

int ObDASHNSWScanIter::check_pre_filter_need_retry()
{
  int ret = OB_SUCCESS;
  double pre_selectivity = double(adaptive_ctx_.pre_scan_row_cnt_) / double(adaptive_ctx_.row_count_);
  if (adaptive_ctx_.pre_scan_row_cnt_ <= MAX_HNSW_BRUTE_FORCE_SIZE) {
    /*do nothing*/
  } else if (is_parallel_with_block_granule()) {
    /*do nothing*/
  } else if (!adaptive_ctx_.is_primary_index_ && pre_selectivity > ObVecIdxExtraInfo::DEFAULT_PRE_RATE_FILTER_WITH_IDX) {
    ret = OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY;
  } else if (pre_selectivity > ObVecIdxExtraInfo::DEFAULT_PRE_RATE_FILTER_WITH_ROWKEY) {
    ret = OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY;
  }
  LOG_TRACE("switch path check pre filter need retry:", K(ret), K(adaptive_ctx_), K(pre_selectivity));
  return ret;
}

int ObDASHNSWScanIter::reset_filter_path()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(*exec_ctx_);
  ObPlanStat* plan_stat = nullptr;
  if (OB_ISNULL(plan_ctx->get_phy_plan())) {
    // remote scan, phy plan is null, do nothing, just use try path in ctdef
    LOG_WARN("plan ctx is null", K(ret), KP(plan_ctx));
  } else if (OB_FALSE_IT(plan_stat = const_cast<ObPlanStat*>(&(plan_ctx->get_phy_plan()->stat_)))) {
  } else if (vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER) {
    vec_idx_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER;
  } else if (vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER) {
    double iter_selectivity = double(adaptive_ctx_.iter_res_row_cnt_) / double(adaptive_ctx_.iter_filter_row_cnt_);
    adaptive_ctx_.selectivity_ = iter_selectivity;
    vec_idx_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER;
  }

  if (OB_FAIL(ret) || OB_ISNULL(plan_stat)) {
  } else if (OB_FAIL(updata_vec_exec_ctx(plan_stat))) {
    LOG_WARN("failed to updata vec exec ctx", K(ret), K(vec_idx_try_path_));
  } else {
    query_cond_.reset();
    go_brute_force_ = false;
    only_complete_data_ = false;
    can_retry_ = false;
    if (OB_NOT_NULL(tmp_adaptor_vid_iter_)) {
      tmp_adaptor_vid_iter_->reset();
      tmp_adaptor_vid_iter_->~ObVectorQueryVidIterator();
      tmp_adaptor_vid_iter_ = nullptr;
    }
    if (OB_NOT_NULL(adaptor_vid_iter_)) {
      adaptor_vid_iter_->reset();
      adaptor_vid_iter_->~ObVectorQueryVidIterator();
      adaptor_vid_iter_ = nullptr;
    }
  }
  return ret;
}

int ObDASHNSWScanIter::updata_vec_exec_ctx(ObPlanStat* plan_stat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan stat is null", K(ret), KP(plan_stat));
  } else {
    int32_t record_count = plan_stat->vec_index_exec_ctx_.record_count_ + 1;
    if (record_count < CHANGE_PATH_WINDOW_SIZE) {
      if (vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER) {
        ATOMIC_INC(&(plan_stat->vec_index_exec_ctx_.iter_filter_chosen_times_));
      } else if (vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER) {
        ATOMIC_INC(&(plan_stat->vec_index_exec_ctx_.pre_filter_chosen_times_));
      } else if (vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_IN_FILTER) {
        ATOMIC_INC(&(plan_stat->vec_index_exec_ctx_.in_filter_chosen_times_));
      }
      ATOMIC_INC(&(plan_stat->vec_index_exec_ctx_.record_count_));
    } else {
      double iter_time = plan_stat->vec_index_exec_ctx_.iter_filter_chosen_times_;
      double pre_time = plan_stat->vec_index_exec_ctx_.pre_filter_chosen_times_;
      double in_time = plan_stat->vec_index_exec_ctx_.in_filter_chosen_times_;
      iter_time = std::log(iter_time) * DECAY_FACTOR;
      pre_time = std::log(pre_time) * DECAY_FACTOR;
      in_time = std::log(in_time) * DECAY_FACTOR;
      FLOG_INFO("begin to reset plan stat filter path", K(plan_stat->vec_index_exec_ctx_.record_count_),
      K(plan_stat->vec_index_exec_ctx_.cur_path_), K(vec_idx_try_path_), K(adaptive_ctx_), K(ret));
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.record_count_), 0);
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.iter_filter_chosen_times_), static_cast<int64_t>(iter_time));
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.pre_filter_chosen_times_), static_cast<int64_t>(pre_time));
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.in_filter_chosen_times_), static_cast<int64_t>(in_time));
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.cur_path_), static_cast<uint8_t>(vec_idx_try_path_));
    }
  }
  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  can_retry_ = check_if_can_retry();
  bool need_force_switch_path = check_need_force_switch_run_path();
  if (need_force_switch_path) {
    ret = OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY;
  }
  if (need_force_switch_path || OB_FAIL(inner_process_adaptor_state(is_vectorized))) {
    if (ret == OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY && can_retry_) {
      ret = OB_SUCCESS;
      if (OB_FAIL(reset_filter_path())) {
        LOG_WARN("failed to reset filter path", K(vec_index_type_), K(vec_idx_try_path_), K(adaptive_ctx_), K(ret));
      } else if (!is_pre_filter() && !is_in_filter()) {
        // do not need scan/rescan idx iter
      } else if (idx_iter_first_scan_) {
        if (OB_FAIL(do_table_scan())) {
          LOG_WARN("failed to do table scan", K(vec_index_type_), K(vec_idx_try_path_), K(adaptive_ctx_));
        }
      } else if (!idx_iter_first_scan_) {
        if (OB_FAIL(rescan())) {
          LOG_WARN("failed to do table rescan", K(vec_index_type_), K(vec_idx_try_path_), K(adaptive_ctx_));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(inner_process_adaptor_state(is_vectorized))) {
        LOG_WARN("failed to process adaptor state hnsw", K(extra_column_count_), K(is_primary_pre_with_rowkey_with_filter_),
                                                         K(data_filter_ctdef_), K(vec_index_type_), K(vec_idx_try_path_), K(adaptive_ctx_));
      }
    } else {
      LOG_WARN("failed to process adaptor state hnsw", K(extra_column_count_), K(is_primary_pre_with_rowkey_with_filter_),
                                                       K(data_filter_ctdef_), K(vec_index_type_), K(vec_idx_try_path_), K(adaptive_ctx_));
    }
  }
  return ret;
}

int ObDASHNSWScanIter::inner_process_adaptor_state(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
  if (OB_FAIL(process_adaptor_state_hnsw(allocator, is_vectorized))) {
    LOG_WARN("failed to process adaptor state hnsw", K(ret));
  }

  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }
  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state_hnsw(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;

  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexAdapterGuard adaptor_guard;
  ObVectorQueryAdaptorResultContext ada_ctx(MTL_ID(), extra_column_count_, &vec_op_alloc_, &allocator);
  share::ObVectorIndexAcquireCtx index_ctx;
  index_ctx.inc_tablet_id_ = delta_buf_tablet_id_;
  index_ctx.vbitmap_tablet_id_ = index_id_tablet_id_;
  index_ctx.snapshot_tablet_id_ = snapshot_tablet_id_;
  index_ctx.data_tablet_id_ = com_aux_vec_tablet_id_;
  bool ls_leader = true;

  if (OB_FAIL(vec_index_service->acquire_adapter_guard(ls_id_, index_ctx, adaptor_guard, &vec_index_param_, dim_))) {
    LOG_WARN("failed to get ObPluginVectorIndexAdapter", K(ret), K(ls_id_), K(index_ctx));
  } else {
    share::ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    if (OB_ISNULL(adaptor)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("shouldn't be null.", K(ret));
    } else if (!query_cond_.is_inited() && OB_FAIL(set_vector_query_condition(query_cond_))) {
      LOG_WARN("failed to set query condition.", K(ret));
    } else if (vec_aux_ctdef_->relevance_col_cnt_ > 0  && OB_FAIL(init_rel_map(adaptor))) {
      LOG_WARN("failed to init rel map", K(ret));
    } else if (OB_FAIL(ObPluginVectorIndexUtils::get_ls_leader_flag(ls_id_, ls_leader))) {
      LOG_WARN("fail to get ls leader flag", K(ret), K(ls_id_));
    } else if (OB_FALSE_IT(ada_ctx.set_ls_leader(ls_leader))) {
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
      RWLock::RLockGuard lock_guard(adaptor->get_query_lock());
      if (is_pre_filter() || is_in_filter()) {
        if (OB_FAIL(process_adaptor_state_pre_filter(&ada_ctx, adaptor, is_vectorized))) {
          LOG_WARN("hnsw pre filter failed to query result.", K(ret));
        }
      // for compatibility, do not check by vec_aux_ctdef.is_post_filter, use is_pre_filter_ instead
      // because the vec_type_ is not serialize in the vec_ctdef in version 435, making it impossible to use this flag to check whether it is pre/post
      } else if (OB_FAIL(process_adaptor_state_post_filter(&ada_ctx, adaptor, is_vectorized))) {
        LOG_WARN("hnsw post filter failed to query result.", K(ret));
      }
    }
  }

  return ret;
}

int ObDASHNSWScanIter::do_get_extra_info_by_vids(ObPluginVectorIndexAdaptor *adaptor, const ObSimpleMaxHeap &heap,
                                                 int64_t *vids_idx, int64_t *vids, bool get_snap,
                                                 ObVecExtraInfoPtr &extra_info_ptr)
{
  int ret = OB_SUCCESS;

  int64_t heap_size = heap.get_size();

  if (OB_ISNULL(vids_idx) || OB_ISNULL(vids)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected is null", KP(vids_idx), KP(vids));
  } else if (extra_info_ptr.count_ != heap_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count not = heap_size.", K(extra_info_ptr.count_), K(heap_size));
  } else {
    int64_t vids_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < heap_size; ++i) {
      int64_t vid = heap.at(i);
      bool is_snap = heap.is_snap(i);
      if (is_snap == get_snap) {
        vids[vids_cnt] = vid;
        vids_idx[vids_cnt++] = i;
      }
    }
    char *extra_info_buf = nullptr;
    if (OB_FAIL(ret) || vids_cnt == 0) {
    } else if (OB_ISNULL(extra_info_buf = static_cast<char *>(
                             vec_op_alloc_.alloc(vids_cnt * extra_info_ptr.extra_info_actual_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc extra info buf.", K(ret), K(vids_cnt), K(extra_info_ptr.extra_info_actual_size_));
    } else if (OB_FAIL(adaptor->get_extra_info_by_ids(vids, vids_cnt, extra_info_buf, get_snap))) {
      LOG_WARN("failed to get extra info by ids.", K(ret), KP(vids), K(vids_cnt));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < vids_cnt; ++i) {
        int64_t idx = vids_idx[i];
        if (OB_FAIL(extra_info_ptr.set_no_copy(idx, extra_info_buf + i * extra_info_ptr.extra_info_actual_size_))) {
          LOG_WARN("failed to set extra info.", K(ret), K(idx), K(extra_info_buf));
        }
      }
    }
  }

  return ret;
}

int ObDASHNSWScanIter::get_extra_info_by_vids(ObPluginVectorIndexAdaptor *adaptor, const ObSimpleMaxHeap &heap, ObVecExtraInfoPtr &extra_info_ptr) {
  int ret = OB_SUCCESS;

  int64_t heap_size = heap.get_size();
  if (OB_ISNULL(adaptor)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null.", K(ret));
  } else {
    int64_t *vids_idx = nullptr;
    int64_t *vids = nullptr;
    if (OB_ISNULL(vids_idx = static_cast<int64_t *>(
                      mem_context_->get_arena_allocator().alloc(sizeof(int64_t) * heap_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc snap vids idx.", K(ret));
    } else if (OB_ISNULL(vids = static_cast<int64_t *>(
                             mem_context_->get_arena_allocator().alloc(sizeof(int64_t) * heap_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc snap vids.", K(ret));
    } else if (OB_FAIL(do_get_extra_info_by_vids(adaptor, heap, vids_idx, vids, true, extra_info_ptr))) {
      LOG_WARN("failed to get_extra_info_by_vid in snap.", K(ret));
    } else {
      MEMSET(vids_idx, 0, sizeof(sizeof(int64_t) * heap_size));
      MEMSET(vids, 0, sizeof(sizeof(int64_t) * heap_size));
      if (OB_FAIL(do_get_extra_info_by_vids(adaptor, heap, vids_idx, vids, false, extra_info_ptr))) {
        LOG_WARN("failed to get_extra_info_by_vid in snap.", K(ret));
      }
    }
  }

  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state_pre_filter_brute_force(
                        ObVectorQueryAdaptorResultContext *ada_ctx,
                        ObPluginVectorIndexAdaptor* adaptor,
                        int64_t *&brute_vids,
                        int& brute_cnt,
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

int ObDASHNSWScanIter::process_adaptor_state_pre_filter_brute_force_not_bq(
                        ObVectorQueryAdaptorResultContext *ada_ctx,
                        ObPluginVectorIndexAdaptor* adaptor,
                        int64_t *&brute_vids,
                        int& brute_cnt,
                        bool& need_complete_data,
                        bool check_need_complete_data)
{
  INIT_SUCC(ret);
  ObString search_vec;
  need_complete_data = false;
  const float* distances_inc = nullptr;
  const float* distances_snap = nullptr;
  uint64_t limit = limit_param_.limit_ + limit_param_.offset_;
  uint64_t capaticy = limit > brute_cnt ? brute_cnt : limit;
  ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
  ObExprVectorDistance::ObVecDisType dis_type = ObExprVectorDistance::ObVecDisType::MAX_TYPE;
  ObSimpleMaxHeap max_heap(&allocator, capaticy);
  // query brute distances
  if (capaticy == 0) {
    // limit or vid is null, no need to query
    ret = OB_ITER_END;
  } else if (OB_ISNULL(adaptor)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null.", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(mem_context_->get_arena_allocator(), sort_rtdef_, search_vec_, search_vec))) {
    LOG_WARN("failed to get real search vec", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::get_distance_expr_type(*sort_ctdef_->sort_exprs_[0], *sort_rtdef_->eval_ctx_, dis_type))) {
    LOG_WARN("failed to get distance type.", K(ret));
  } else if (OB_FAIL(max_heap.init())) {
    LOG_WARN("failed to init max heap.", K(ret));
  } else if (OB_FAIL(adaptor->vsag_query_vids(reinterpret_cast<float *>(search_vec.ptr()), brute_vids, brute_cnt, distances_inc, false))) {
    LOG_WARN("failed to query vids.", K(ret), K(brute_cnt));
  } else if (OB_FAIL(adaptor->vsag_query_vids(reinterpret_cast<float *>(search_vec.ptr()), brute_vids, brute_cnt, distances_snap, true))) {
    LOG_WARN("failed to query vids.", K(ret), K(brute_cnt));
  } else if (distances_inc == nullptr && distances_snap == nullptr) {
    need_complete_data = check_need_complete_data ? true : false;
  } else if (distances_inc == nullptr || distances_snap == nullptr) {
    bool is_snap = distances_inc == nullptr;
    for (int i = 0; i < brute_cnt && OB_SUCC(ret) && !need_complete_data; ++i) {
      double distance = distances_inc == nullptr ? distances_snap[i] : distances_inc[i];
      // if distances == -1, means vid not exist
      if (distance != -1) {
        max_heap.push(brute_vids[i], distance, is_snap);
      } else {
        need_complete_data = check_need_complete_data ? true : false;
      }
    }
  } else {
    for (int i = 0; i < brute_cnt && OB_SUCC(ret) && !need_complete_data; ++i) {
      bool is_snap = distances_inc[i] == -1;
      double distance = distances_inc[i] == -1 ? distances_snap[i] : distances_inc[i];
      // if distances == -1, means vid not exist
      if (distance != -1) {
        max_heap.push(brute_vids[i], distance, is_snap);
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
    int64_t extra_info_actual_size = 0;
    ObVecExtraInfoPtr extra_info_ptr;
    // alloc final res
    if (OB_FAIL(adaptor->get_extra_info_actual_size(extra_info_actual_size))) {
      LOG_WARN("failed to get extra info actual size.", K(ret));
    } else if (OB_ISNULL(vids = static_cast<int64_t *>(vec_op_alloc_.alloc(sizeof(int64_t) * heap_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator vids", K(ret));
    } else if (OB_ISNULL(distances = static_cast<float *>(vec_op_alloc_.alloc(sizeof(float) * heap_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator distances", K(ret));
    } else if (extra_column_count_ > 0) {
     if (OB_FAIL(extra_info_ptr.init(&vec_op_alloc_, extra_info_actual_size, heap_size))) {
        LOG_WARN("failed to init merge_extra_info_ptr.", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      int64_t heap_idx = 0;
      int64_t vid_idx = 0;
      // init final res
      while (OB_SUCC(ret) && heap_idx < heap_size) {
        int res_vid = max_heap.at(heap_idx);
        float res_dis = max_heap.value_at(heap_idx);
        vids[heap_idx] = res_vid;
        distances[heap_idx] = res_dis;
        heap_idx++;
      }

      // init adaptor_vid_iter_
      void *iter_buff = nullptr;
      if (OB_FAIL(ret)) {
      } else if (extra_column_count_ > 0 &&
                 OB_FAIL(get_extra_info_by_vids(adaptor, max_heap, extra_info_ptr))) {
        LOG_WARN("failed to get extra info by vids.", K(ret));
      } else if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryVidIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator adaptor vid iter.", K(ret));
      } else if (OB_FALSE_IT(adaptor_vid_iter_ = new (iter_buff)
                                 ObVectorQueryVidIterator(extra_column_count_, extra_info_actual_size, query_cond_.rel_count_, query_cond_.rel_map_ptr_))) {
      } else if (OB_FAIL(adaptor_vid_iter_->init(heap_size, vids, distances, extra_info_ptr, &vec_op_alloc_))) {
        LOG_WARN("iter init failed.", K(ret));
      }
    }
  }

  // release distances
  if (OB_NOT_NULL(distances_inc)) {
    adaptor->get_incr_data()->mem_ctx_->Deallocate((void *)distances_inc);
    distances_inc = nullptr;
  }

  if (OB_NOT_NULL(distances_snap)) {
    adaptor->get_snap_data_()->mem_ctx_->Deallocate((void *)distances_snap);
    distances_snap = nullptr;
  }
  return ret;
}

int64_t ObDASHNSWScanIter::get_reorder_count_for_brute_force(const int64_t ef_search, const int64_t topK, const ObVectorIndexParam& param)
{
  const float refine_k = param.refine_k_;
  int64_t refine_cnt = refine_k * topK < MIN_BQ_REORDER_SIZE_FOR_BRUTE_FORCE ? MIN_BQ_REORDER_SIZE_FOR_BRUTE_FORCE : refine_k * topK;
  LOG_TRACE("reorder count info", K(ef_search), K(topK), K(refine_k), K(refine_cnt));
  return OB_MIN(OB_MAX(topK, OB_MAX(refine_cnt, ef_search)), MAX_VSAG_QUERY_RES_SIZE);
}

int ObDASHNSWScanIter::init_brute_force_params(ObVectorQueryAdaptorResultContext *ada_ctx,
                                               ObPluginVectorIndexAdaptor* adaptor,
                                               BruteForceContext& ctx)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(ada_ctx) || OB_ISNULL(adaptor)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("ada_ctx or adaptor is null", K(ret), KP(ada_ctx), KP(adaptor));
  } else {
    ctx.limit = limit_param_.limit_ + limit_param_.offset_;
    ctx.limit = get_reorder_count_for_brute_force(query_cond_.ef_search_, ctx.limit, search_param_);

    if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(mem_context_->get_arena_allocator(),
                                                       sort_rtdef_, search_vec_, ctx.search_vec))) {
      LOG_WARN("failed to get real search vec", K(ret));
    } else if (OB_FAIL(ObDasVecScanUtils::get_distance_expr_type(*sort_ctdef_->sort_exprs_[0],
                                                                 *sort_rtdef_->eval_ctx_, ctx.dis_type))) {
      LOG_WARN("failed to get distance type", K(ret));
    }
  }

  return ret;
}

int ObDASHNSWScanIter::query_brute_force_distances(ObPluginVectorIndexAdaptor* adaptor,
                                                   const ObString& search_vec,
                                                   int64_t* brute_vids,
                                                   int brute_cnt,
                                                   DistanceResult& dist_result)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(adaptor) || OB_ISNULL(brute_vids) || brute_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(adaptor), KP(brute_vids), K(brute_cnt));
  } else {
    dist_result.brute_cnt = brute_cnt;
    if (OB_FAIL(adaptor->vsag_query_vids(reinterpret_cast<float *>(const_cast<char*>(search_vec.ptr())),
                                         brute_vids, brute_cnt, dist_result.distances_inc, false))) {
      LOG_WARN("failed to query incremental vids", K(ret), K(brute_cnt));
    } else if (OB_FAIL(adaptor->vsag_query_vids(reinterpret_cast<float *>(const_cast<char*>(search_vec.ptr())),
                                           brute_vids, brute_cnt, dist_result.distances_snap, true))) {
      LOG_WARN("failed to query snapshot vids", K(ret), K(brute_cnt));
    }
  }

  return ret;
}

void ObDASHNSWScanIter::release_brute_force_distance_memory(ObPluginVectorIndexAdaptor* adaptor,
                                                           const DistanceResult& dist_result)
{
  if (OB_NOT_NULL(dist_result.distances_inc)) {
    adaptor->get_incr_data()->mem_ctx_->Deallocate((void *)dist_result.distances_inc);
  }

  if (OB_NOT_NULL(dist_result.distances_snap)) {
    adaptor->get_snap_data_()->mem_ctx_->Deallocate((void *)dist_result.distances_snap);
  }
}

int ObDASHNSWScanIter::merge_and_sort_brute_force_results_bq(const DistanceResult& dist_result,
                                                             int64_t* brute_vids,
                                                             int brute_cnt,
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
    if (dist_result.distances_inc == nullptr && dist_result.distances_snap == nullptr) {
      need_complete_data = check_need_complete_data ? true : false;
    } else {
      for (int i = 0; i < brute_cnt && OB_SUCC(ret) && !need_complete_data; ++i) {
        bool has_incr = (dist_result.distances_inc != nullptr && dist_result.distances_inc[i] != -1.0);
        bool has_snap = (dist_result.distances_snap != nullptr && dist_result.distances_snap[i] != -1.0);

        if (!has_incr && !has_snap) {
          need_complete_data = check_need_complete_data ? true : false;
        } else if (has_incr && has_snap) {
          incr_heap.push(brute_vids[i], dist_result.distances_inc[i], false);
          snap_heap.push(brute_vids[i], dist_result.distances_snap[i], true);
        } else if (has_incr) {
          incr_heap.push(brute_vids[i], dist_result.distances_inc[i], false);
        } else if (has_snap) {
          snap_heap.push(brute_vids[i], dist_result.distances_snap[i], true);
        }
      }
    }
  }

  return ret;
}

int ObDASHNSWScanIter::build_brute_force_result_iterator_bq(ObPluginVectorIndexAdaptor* adaptor,
                                                            const ObSimpleMaxHeap& snap_heap,
                                                            const ObSimpleMaxHeap& incr_heap,
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
      int64_t extra_info_actual_size = 0;
      ObVecExtraInfoPtr extra_info_ptr;

      if (OB_FAIL(adaptor->get_extra_info_actual_size(extra_info_actual_size))) {
        LOG_WARN("failed to get extra info actual size", K(ret));
      } else if (OB_ISNULL(vids = static_cast<int64_t *>(vec_op_alloc_.alloc(sizeof(int64_t) * total_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate vids", K(ret));
      } else if (OB_ISNULL(distances = static_cast<float *>(vec_op_alloc_.alloc(sizeof(float) * total_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate distances", K(ret));
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
              idx++;
            }
          }
          total_size = idx;
        }

        if (OB_SUCC(ret) && extra_column_count_ > 0 &&
            OB_FAIL(extra_info_ptr.init(&vec_op_alloc_, extra_info_actual_size, total_size))) {
          LOG_WARN("failed to init extra_info_ptr", K(ret));
        }

        if (OB_SUCC(ret) && extra_column_count_ > 0) {
          ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
          ObSimpleMaxHeap merged_heap(&allocator, total_size);

          if (OB_FAIL(merged_heap.init())) {
            LOG_WARN("failed to init merged heap for extra info", K(ret));
          } else {
            for (int64_t i = 0; i < total_size; ++i) {
              bool is_snap = (i >= incr_size);
              merged_heap.push(vids[i], distances[i], is_snap);
            }

            if (OB_FAIL(get_extra_info_by_vids(adaptor, merged_heap, extra_info_ptr))) {
              LOG_WARN("failed to get extra info by vids", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          void *iter_buff = nullptr;
          if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryVidIterator)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate adaptor vid iter", K(ret));
          } else if (OB_FALSE_IT(result_iter = new (iter_buff)
                                ObVectorQueryVidIterator(extra_column_count_, extra_info_actual_size, query_cond_.rel_count_, query_cond_.rel_map_ptr_))) {
          } else if (OB_FAIL(result_iter->init(total_size, vids, distances, extra_info_ptr, &vec_op_alloc_))) {
            LOG_WARN("iter init failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state_pre_filter_brute_force_bq(
                        ObVectorQueryAdaptorResultContext *ada_ctx,
                        ObPluginVectorIndexAdaptor* adaptor,
                        int64_t *&brute_vids,
                        int& brute_cnt,
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
      DistanceResult dist_result; // initialized by the constructor
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
        if (OB_FAIL(build_brute_force_result_iterator_bq(adaptor, snap_heap, incr_heap, adaptor_vid_iter_))) {
          LOG_WARN("failed to build bq result iterator", K(ret));
        }
      }
      release_brute_force_distance_memory(adaptor, dist_result);
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
  int64_t *brute_vids = nullptr;
  int brute_cnt = 0;
  if (is_primary_pre_with_rowkey_with_filter_) {
    if (OB_FAIL(process_adaptor_state_pre_filter_with_rowkey(ada_ctx, adaptor, brute_vids, brute_cnt, is_vectorized))) {
      LOG_WARN("hnsw pre filter(rowkey vid iter) failed to query result.", K(ret));
    }
  } else if (OB_FAIL(process_adaptor_state_pre_filter_with_idx_filter(ada_ctx, adaptor, brute_vids, brute_cnt, is_vectorized))) {
    LOG_WARN("hnsw pre filter(idx iter) failed to query result.", K(ret));
  }
  LOG_TRACE("vector index show pre-filter query info", K(is_primary_pre_with_rowkey_with_filter_), K(go_brute_force_));
  if (OB_FAIL(ret)) {
  } else if (go_brute_force_) {
    bool need_complete_data = false;
    if (OB_FAIL(process_adaptor_state_pre_filter_brute_force(ada_ctx, adaptor, brute_vids, brute_cnt, need_complete_data, true))) {
      LOG_WARN("hnsw pre filter(brute force) failed to query result.", K(ret));
    } else if (need_complete_data) {
      only_complete_data_ = true;
      if (OB_FAIL(process_adaptor_state_post_filter(ada_ctx, adaptor, is_vectorized))) {
        LOG_WARN("failed to process adaptor state post filter", K(ret));
      } else if (OB_FAIL(process_adaptor_state_pre_filter_brute_force(ada_ctx, adaptor, brute_vids, brute_cnt, need_complete_data, false))) {
        LOG_WARN("hnsw pre filter(brute force) failed to query result.", K(ret));
      }
    }
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
    } else if (OB_NOT_NULL(snapshot_iter_) && OB_FALSE_IT(query_cond_.row_iter_ = snapshot_iter_->get_output_result_iter())) {
    } else if (OB_FAIL(adaptor->query_result(ls_id_, ada_ctx, &query_cond_, adaptor_vid_iter_))) {
      LOG_WARN("failed to query result.", K(ret));
    } else if (PVQ_REFRESH == ada_ctx->get_status()) {
      if (OB_FAIL(ObPluginVectorIndexUtils::query_need_refresh_memdata(adaptor, ls_id_, ada_ctx->get_ls_leader()))) {
        if (ret != OB_SCHEMA_EAGAIN) {
          LOG_WARN("fail to refresh memdata in query", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObDASHNSWScanIter::build_extra_info_rowkey(const ObRowkey &rowkey, ObRowkey &extra_rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *extra_info_objs = nullptr;
  if (OB_FALSE_IT(extra_rowkey.reset())) {
  } else if (OB_FAIL(build_extra_info_obj_from_rowkey(rowkey.get_obj_ptr(), extra_info_objs))) {
    LOG_WARN("failed to build rowkey obj from extra info.", K(ret));
  } else {
    extra_rowkey.assign(extra_info_objs, extra_column_count_);
  }

  return ret;
}

int ObDASHNSWScanIter::build_extra_info_range(const ObNewRange &range, const ObNewRange *&const_extra_range)
{
  int ret = OB_SUCCESS;
  ObNewRange *extra_range = nullptr;
  void *buf = nullptr;
  if (extra_column_count_ <= 1) {
    const_extra_range = &range;
  } else if (OB_ISNULL(buf = mem_context_->get_arena_allocator().alloc(sizeof(ObNewRange)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mem for new range.", K(ret));
  } else if (OB_FALSE_IT(extra_range = new (buf) ObNewRange())) {
  } else if (OB_FALSE_IT(*extra_range = range)) {
  } else if (OB_FAIL(build_extra_info_rowkey(range.start_key_, extra_range->start_key_))) {
    LOG_WARN("failed to build rowkey obj from extra info.", K(ret), K(range));
  } else if (OB_FAIL(build_extra_info_rowkey(range.end_key_, extra_range->end_key_))) {
    LOG_WARN("failed to build rowkey obj from extra info.", K(ret), K(range));
  } else {
    const_extra_range = extra_range;
  }
  LOG_TRACE("build extra info range", K(range.start_key_), K(range.end_key_),
  KPC(range.start_key_.get_obj_ptr()), KPC(range.end_key_.get_obj_ptr()));

  return ret;
}

int ObDASHNSWScanIter::init_pre_filter(ObPluginVectorIndexAdaptor *adaptor, ObVectorQueryAdaptorResultContext *ada_ctx)
{
  int ret = OB_SUCCESS;
  bool init_as_range = false;
  ObArray<const ObNewRange *> rk_range;
  if (is_primary_pre_with_rowkey_with_filter_ && extra_column_count_ > 0) {
    if (rowkey_vid_iter_->get_scan_param().pd_storage_filters_ != nullptr) { // has filter
      // not support
    } else {
      init_as_range = true;
      // check key range
      const ObRangeArray& key_range = rowkey_vid_iter_->get_scan_param().key_ranges_;
      for (int64_t i = 0; i < key_range.count() && OB_SUCC(ret); i++) {
        const ObNewRange *range = &key_range.at(i);
        if (OB_FAIL(rk_range.push_back(range))) {
          LOG_WARN("fail to push back range", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (init_as_range) {
      const ObDASScanCtDef *ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx(),
                                                                          ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
      if (OB_ISNULL(ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey vid ctdef is null.", K(ret));
      } else if (OB_FAIL(ada_ctx->init_prefilter(adaptor, adaptive_ctx_.selectivity_, rk_range, ctdef->rowkey_exprs_, extra_in_rowkey_idxs_))) {
        LOG_WARN("init bitmaps failed.", K(ret));
      }
    } else {
      ObVidBound bound;
      if (OB_FAIL(adaptor->get_vid_bound(bound))) {
        LOG_WARN("fail to get vid bound from adaptor", K(ret));
      } else if (OB_FAIL(ada_ctx->init_prefilter(bound.min_vid_, bound.max_vid_))) {
        LOG_WARN("init bitmaps failed.", K(ret), K(bound.min_vid_), K(bound.max_vid_));
      }
    }
  }
  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state_pre_filter_with_rowkey(
                        ObVectorQueryAdaptorResultContext *ada_ctx,
                        ObPluginVectorIndexAdaptor* adaptor,
                        int64_t *&vids,
                        int& brute_cnt,
                        bool is_vectorized)
{
  int ret = OB_SUCCESS;
  ObVidBound bound;
  if (OB_ISNULL(ada_ctx) || OB_ISNULL(adaptor) || OB_ISNULL(rowkey_vid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shouldn't be null.", K(ret), K(ada_ctx), K(adaptor));
  } else if (!ada_ctx->is_prefilter_valid() && OB_FAIL(init_pre_filter(adaptor, ada_ctx))) {
    LOG_WARN("init bitmaps failed.", K(ret), K(bound.min_vid_), K(bound.max_vid_));
  } else {
    vids = nullptr;
    brute_cnt = 0;
    ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
    if (OB_ISNULL(vids = static_cast<int64_t *>(allocator.alloc(sizeof(int64_t) * MAX_HNSW_BRUTE_FORCE_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator vids", K(ret));
    } else {
      go_brute_force_ = true;
      const ObDASScanCtDef *rowkey_vid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
      int64_t batch_row_count = vec_aux_rtdef_->eval_ctx_->max_batch_size_ > 0 ?
                                min(vec_aux_rtdef_->eval_ctx_->max_batch_size_, ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE) :
                                ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
      bool index_end = false;
      bool is_pre_filter_end = false;
      while (OB_SUCC(ret) && !index_end && !is_pre_filter_end) {
        bool add_brute = (brute_cnt + batch_row_count) < MAX_HNSW_BRUTE_FORCE_SIZE;
        if (!is_vectorized) {
          for (int i = 0; OB_SUCC(ret) && i < batch_row_count && !is_pre_filter_end; ++i) {
            int64_t vid = 0;
            if (OB_FAIL(get_vid_from_rowkey_vid_table(vid))) {
              if (OB_UNLIKELY(OB_ITER_END != ret)) {
                LOG_WARN("failed to get vector from rowkey vid table.", K(ret), K(i));
              }
              index_end = true;
            } else if (go_brute_force_ && add_brute && brute_cnt < MAX_HNSW_BRUTE_FORCE_SIZE) {
              vids[brute_cnt] = vid;
              ++brute_cnt;
            } else if (ada_ctx->is_range_prefilter()) {
              is_pre_filter_end = true;
              go_brute_force_ = false;
            } else {
              // brute_cnt + batch_row_count already > MAX_HNSW_BRUTE_FORCE_SIZE
              // do not choose brue force, just add vids to bitmap
              if (go_brute_force_) {
                go_brute_force_ = false;
                lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(adaptor->get_tenant_id(), "VIBitmapADPI"));
                for (int j = 0; OB_SUCC(ret) && j < brute_cnt; ++j) {
                  if (OB_FAIL(adaptor->add_extra_valid_vid_without_malloc_guard(ada_ctx, vids[j]))) {
                    LOG_WARN("failed to add valid vid", K(ret));
                  }
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(adaptor->add_extra_valid_vid(ada_ctx, vid))) {
                LOG_WARN("failed to add valid vid", K(ret));
              }
            }
          }
          adaptive_ctx_.pre_scan_row_cnt_ += batch_row_count;
        } else {
          rowkey_vid_iter_->clear_evaluated_flag();
          int64_t scan_row_cnt = 0;
          if (OB_FAIL(rowkey_vid_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next row.", K(ret));
            }
            index_end = true;
          }

          if (OB_FAIL(ret) && OB_ITER_END != ret) {
          } else if (scan_row_cnt > 0) {
            adaptive_ctx_.pre_scan_row_cnt_ += scan_row_cnt;
            ret = OB_SUCCESS;
          }

          if (OB_SUCC(ret)) {
            ObExpr *vid_expr = vec_aux_ctdef_->inv_scan_vec_id_col_;
            ObDatum *vid_datum = vid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
            if (go_brute_force_ && add_brute && (brute_cnt + scan_row_cnt < MAX_HNSW_BRUTE_FORCE_SIZE)) {
              for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
                int64_t vid = vid_datum[i].get_int();
                vids[brute_cnt] = vid;
                ++brute_cnt;
              }
            } else if (ada_ctx->is_range_prefilter()) {
              is_pre_filter_end = true;
              go_brute_force_ = false;
            } else {
              lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(adaptor->get_tenant_id(), "VIBitmapADPI"));

              // first, add vids into bitmap
              if (go_brute_force_) {
                go_brute_force_ = false;
                for (int j = 0; OB_SUCC(ret) && j < brute_cnt; ++j) {
                  if (OB_FAIL(adaptor->add_extra_valid_vid(ada_ctx, vids[j]))) {
                    LOG_WARN("failed to add valid vid", K(ret));
                  }
                }
              }
              // then add this batch into bitmap
              for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
                int64_t vid = vid_datum[i].get_int();
                if (OB_FAIL(adaptor->add_extra_valid_vid_without_malloc_guard(ada_ctx, vid))) {
                  LOG_WARN("failed to add valid vid", K(ret));
                }
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (can_retry_ && !go_brute_force_ && OB_FAIL(check_pre_filter_need_retry())) {
          LOG_WARN("ret of check iter filter need retry.", K(ret), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_));
        }
      } // end while
    }
    if (OB_ITER_END != ret && OB_SUCCESS != ret) {
      LOG_WARN("get next row failed.", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObDASHNSWScanIter::get_from_vid_rowkey(ObIAllocator &allocator, ObRowkey *&rowkey)
{
  int ret = OB_SUCCESS;

  int64_t output_cnt = 0;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;
  const ObDASScanCtDef *com_aux_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_com_aux_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
  int64_t main_rowkey_cnt = 0;

  if (OB_ISNULL(vid_rowkey_ctdef_) || OB_ISNULL(vid_rowkey_rtdef_) || OB_ISNULL(com_aux_vec_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(vid_rowkey_ctdef_), KP(vid_rowkey_rtdef_));
  } else if (OB_FALSE_IT(output_cnt = vid_rowkey_ctdef_->result_output_.count())) {
  } else if (OB_FALSE_IT(main_rowkey_cnt = com_aux_vec_ctdef->table_param_.get_read_info().get_schema_rowkey_count())) {
  } else if (OB_UNLIKELY(main_rowkey_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret), K(main_rowkey_cnt));
  } else if (OB_UNLIKELY(output_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid output cnt", K(ret), K(output_cnt));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * main_rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(output_cnt), K(main_rowkey_cnt));
  } else if (OB_FALSE_IT(obj_ptr = new (buf) ObObj[main_rowkey_cnt])) {
  } else if (OB_ISNULL(rowkey = static_cast<ObRowkey *>(allocator.alloc(sizeof(ObRowkey))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    int add_rowkey_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < output_cnt; ++i) {
      ObObj tmp_obj;
      ObExpr *expr = vid_rowkey_ctdef_->result_output_.at(i);
      ObDatum &datum = expr->locate_expr_datum(*vid_rowkey_rtdef_->eval_ctx_);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get col datum null", K(ret));
      } else if (T_PSEUDO_GROUP_ID == expr->type_ || T_PSEUDO_ROW_TRANS_INFO_COLUMN == expr->type_) {
        // nothing to do.
        LOG_TRACE("skip expr", K(i), KPC(expr));
      } else {
        ObDatum &datum = expr->locate_expr_datum(*vid_rowkey_rtdef_->eval_ctx_);
        if (i >= main_rowkey_cnt) {
          ret = ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid rowkey idx", K(ret), K(i), K(main_rowkey_cnt), K(output_cnt), KPC(vid_rowkey_ctdef_), KPC(com_aux_vec_ctdef));
        } else if (OB_FAIL(datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret));
        } else if (OB_FAIL(ob_write_obj(allocator, tmp_obj, obj_ptr[i]))) {
          LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
        } else {
          ++add_rowkey_cnt;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(main_rowkey_cnt != add_rowkey_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid rowkey cnt", K(ret), K(add_rowkey_cnt), K(main_rowkey_cnt), K(output_cnt), KPC(vid_rowkey_ctdef_), KPC(com_aux_vec_ctdef));
    } else {
      rowkey->assign(obj_ptr, main_rowkey_cnt);
    }
  }

  return ret;
}

int ObDASHNSWScanIter::get_ctdef_with_rowkey_exprs(const ObDASScanCtDef *&ctdef, ObDASScanRtDef *&rtdef)
{
  int ret = OB_SUCCESS;
  ctdef = nullptr;
  rtdef = nullptr;

  ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_com_aux_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
  rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_com_aux_tbl_idx());
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
    rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx());
  }

  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(ctdef), KP(rtdef));
  }

  return ret;
}

int ObDASHNSWScanIter::get_rowkey(ObIAllocator &allocator, ObRowkey *&rowkey)
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *ctdef = nullptr;
  ObDASScanRtDef *rtdef = nullptr;
  if (OB_FAIL(get_ctdef_with_rowkey_exprs(ctdef, rtdef))) {
    LOG_WARN("failed to get ctdef with rowkey exprs", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::get_rowkey(allocator, ctdef, rtdef, rowkey))) {
    LOG_WARN("failed to get rowkey", K(ret));
  }

  return ret;
}

int ObDASHNSWScanIter::get_relevance(ObIAllocator &allocator, ObIArray<double*> &rel_record)
{
  int ret = OB_SUCCESS;
  double* rel_array = nullptr;
  if (OB_FAIL(get_one_relevance(allocator, rel_array))) {
    LOG_WARN("failed to get one relevance", K(ret));
  } else if (OB_ISNULL(rel_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rel_array is null", K(ret));
  } else if (OB_FAIL(rel_record.push_back(rel_array))) {
    LOG_WARN("failed to push back relevance", K(ret));
  }
  return ret;
}

int ObDASHNSWScanIter::get_relevances(
    ObIAllocator &allocator,
    ObIArray<double*> &rel_record,
    int64_t row_count)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
  guard.set_batch_size(row_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
    guard.set_batch_idx(i);
    if (OB_FAIL(get_relevance(allocator, rel_record))) {
      LOG_WARN("failed to get relevance", K(ret));
    }
  }
  return ret;
}

int ObDASHNSWScanIter::get_vid_from_idx_filter(
    ObVectorQueryAdaptorResultContext *ada_ctx,
    ObPluginVectorIndexAdaptor* adaptor,
    int64_t *&vids,
    int &brute_cnt,
    bool is_vectorized,
    int64_t batch_row_count)
{
  int ret = OB_SUCCESS;
  go_brute_force_ = true;
  bool index_end = false;
  ObArray<double*> relevance_record;
  const ObDASScanCtDef *rowkey_vid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx(),
                                                                                  ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
  bool if_add_relevance = vec_aux_ctdef_->relevance_col_cnt_ > 0;
  bool rel_filter_res = true;

  // scan pre-filter iter and set rowkey_vid_iter key range
  while (OB_SUCC(ret) && !index_end) {
    relevance_record.reuse();
    if (!is_vectorized) {
      for (int i = 0; OB_SUCC(ret) && i < batch_row_count && !index_end; ++i) {
        ObRowkey *rowkey;
        inv_idx_scan_iter_->clear_evaluated_flag();
        if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
          ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
          index_end = true;
        } else if (OB_FAIL(get_rowkey(*ada_ctx->get_tmp_allocator(), rowkey))) {
          LOG_WARN("failed to get rowkey", K(ret));
        } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(*rowkey, rowkey_vid_scan_param_, rowkey_vid_ctdef->ref_table_id_))) {
          LOG_WARN("failed to set lookup key", K(ret));
        } else if (if_add_relevance && OB_FAIL(get_relevance(vec_op_alloc_, relevance_record))) {
          LOG_WARN("failed to get relevance", K(ret), K(i));
        }
      }
      adaptive_ctx_.pre_scan_row_cnt_ += batch_row_count;
    } else {
      int64_t scan_row_cnt = 0;
      if (OB_FAIL(inv_idx_scan_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row.", K(ret));
        }
        index_end = true;
      }

      if (OB_FAIL(ret) && OB_ITER_END != ret) {
      } else if (scan_row_cnt > 0) {
        adaptive_ctx_.pre_scan_row_cnt_ += scan_row_cnt;
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to get next row.", K(ret));
      } else if (can_retry_ && !go_brute_force_ && OB_FAIL(check_pre_filter_need_retry())) {
        LOG_WARN("ret of check pre filter need retry.", K(ret), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_));
      } else {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        for(int i = 0; OB_SUCC(ret) && i < scan_row_cnt; i++) {
          guard.set_batch_idx(i);
          ObRowkey *rowkey;
          if(OB_FAIL(get_rowkey(*ada_ctx->get_tmp_allocator(), rowkey))) {
            LOG_WARN("failed to add rowkey", K(ret), K(i));
          } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(*rowkey, rowkey_vid_scan_param_, rowkey_vid_ctdef->ref_table_id_))) {
            LOG_WARN("failed to set lookup key", K(ret));
          } else if (if_add_relevance && OB_FAIL(get_relevance(vec_op_alloc_, relevance_record))) {
            LOG_WARN("failed to get relevance", K(ret), K(i));
          }
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(do_rowkey_vid_table_scan())) {
      LOG_WARN("do do aux table scan failed", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (!is_vectorized) {
      for (int i = 0; OB_SUCC(ret) && i < batch_row_count; ++i) {
        int64_t vid = 0;
        if (can_retry_ && OB_FAIL(check_pre_filter_need_retry())) {
          LOG_WARN("ret of check iter filter need retry.", K(ret), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_));
        } else if (OB_FAIL(get_vid_from_rowkey_vid_table(vid))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to get vector from rowkey vid table.", K(ret), K(i));
          }
        } else if (go_brute_force_ && brute_cnt + batch_row_count < MAX_HNSW_BRUTE_FORCE_SIZE) {
          vids[brute_cnt] = vid;
          brute_cnt++;
        } else {
          // brute_cnt + batch_row_count already > MAX_HNSW_BRUTE_FORCE_SIZE
          // do not choose brue force, just add vids to bitmap
          if (go_brute_force_) {
            go_brute_force_ = false;
            lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(adaptor->get_tenant_id(), "VIBitmapADPI"));
            for (int j = 0; OB_SUCC(ret) && j < brute_cnt; ++j) {
              if (OB_FAIL(adaptor->add_extra_valid_vid_without_malloc_guard(ada_ctx, vids[j]))) {
                LOG_WARN("failed to add valid vid", K(ret));
              }
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(adaptor->add_extra_valid_vid(ada_ctx, vid))) {
            LOG_WARN("failed to add valid vid", K(ret));
          }
        }
      } // end for
    } else {
      rowkey_vid_iter_->clear_evaluated_flag();
      int64_t scan_row_cnt = 0;
      if (OB_FAIL(rowkey_vid_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row.", K(ret));
        }
      }

      if (OB_FAIL(ret) && OB_ITER_END != ret) {
      } else if (scan_row_cnt > 0) {
        if (!if_add_relevance || relevance_record.count() == scan_row_cnt) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected vid count", K(vec_aux_ctdef_->relevance_col_cnt_), K(relevance_record.count()), K(scan_row_cnt));
        }
      }

      if (OB_SUCC(ret)) {
        ObExpr *vid_expr = vec_aux_ctdef_->inv_scan_vec_id_col_;
        ObDatum *vid_datum = vid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);

        if (go_brute_force_ && brute_cnt + scan_row_cnt < MAX_HNSW_BRUTE_FORCE_SIZE) {
          for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
            int64_t vid = vid_datum[i].get_int();
            vids[brute_cnt] = vid;
            brute_cnt++;
            if (if_add_relevance && i >= relevance_record.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected vid count", K(vec_aux_ctdef_->relevance_col_cnt_), K(relevance_record.count()), K(scan_row_cnt));
            } else if (if_add_relevance && OB_FAIL(add_one_relevance(vid, relevance_record.at(i)))) {
              LOG_WARN("failed to add relevance", K(ret), K(i));
            }
          }
        } else {
          lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(adaptor->get_tenant_id(), "VIBitmapADPI"));
          // first, add vids into bitmap
          if (go_brute_force_) {
            go_brute_force_ = false;
            for (int j = 0; OB_SUCC(ret) && j < brute_cnt; ++j) {
              if (OB_FAIL(adaptor->add_extra_valid_vid(ada_ctx, vids[j]))) {
                LOG_WARN("failed to add valid vid", K(ret));
              }
            }
          }
          // then add this batch into bitmap
          for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
            int64_t vid = vid_datum[i].get_int();
            if (OB_FAIL(adaptor->add_extra_valid_vid_without_malloc_guard(ada_ctx, vid))) {
              LOG_WARN("failed to add valid vid", K(ret));
            } else if (if_add_relevance && i >= relevance_record.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected vid count", K(vec_aux_ctdef_->relevance_col_cnt_), K(relevance_record.count()), K(scan_row_cnt));
            } else if (if_add_relevance && i >= relevance_record.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected vid count", K(vec_aux_ctdef_->relevance_col_cnt_), K(relevance_record.count()), K(scan_row_cnt));
            } else if (if_add_relevance && OB_FAIL(add_one_relevance(vid, relevance_record.at(i)))) {
              LOG_WARN("failed to add relevance", K(ret), K(i));
            }
          }
        }
      }
    }

    int tmp_ret = ret;
    ret = OB_SUCCESS;
    if (OB_FAIL(reuse_rowkey_vid_iter())) {
      LOG_WARN("failed to reuse rowkey vid iter.", K(ret));
    } else {
      ret = tmp_ret;
    }
  }

  return ret;
}

int ObDASHNSWScanIter::get_pk_increment(int64_t &vid)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *ctdef = nullptr;
  ObDASScanRtDef *rtdef = nullptr;
  if (OB_FAIL(get_ctdef_with_rowkey_exprs(ctdef, rtdef))) {
    LOG_WARN("failed to get ctdef with rowkey exprs", K(ret));
  } else if (OB_UNLIKELY(ctdef->rowkey_exprs_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret), K(ctdef->rowkey_exprs_.count()));
  } else {
    ObExpr *expr = ctdef->rowkey_exprs_.at(0);
    ObDatum &datum = expr->locate_expr_datum(*rtdef->eval_ctx_);
    vid = datum.get_uint64();
  }

  return ret;
}

int ObDASHNSWScanIter::get_pk_increment_from_idx_filter(
    ObVectorQueryAdaptorResultContext *ada_ctx,
    ObPluginVectorIndexAdaptor* adaptor,
    int64_t *&vids,
    int &brute_cnt,
    bool is_vectorized,
    int64_t batch_row_count)
{
  int ret = OB_SUCCESS;
  go_brute_force_ = true;
  bool index_end = false;
  ObArray<double*> relevance_record;
  bool if_add_relevance = vec_aux_ctdef_->relevance_col_cnt_ > 0;
  bool rel_filter_res = true;

  while (OB_SUCC(ret) && !index_end) {
    relevance_record.reuse();
    if (!is_vectorized) {
      for (int i = 0; OB_SUCC(ret) && i < batch_row_count && !index_end; ++i) {
        int64_t vid = 0;
        inv_idx_scan_iter_->clear_evaluated_flag();
        if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
          ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
          index_end = true;
        } else if (if_add_relevance && OB_FAIL(get_relevance(vec_op_alloc_, relevance_record))) {
          LOG_WARN("failed to get relevance", K(ret), K(i));
        } else if (OB_FAIL(get_pk_increment(vid))) {
          LOG_WARN("failed to pk_increment", K(ret));
        } else if (go_brute_force_ && brute_cnt + 1 < MAX_HNSW_BRUTE_FORCE_SIZE) {
          vids[brute_cnt] = vid;
          brute_cnt++;
          if (if_add_relevance && i >= relevance_record.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected vid count", K(vec_aux_ctdef_->relevance_col_cnt_), K(relevance_record.count()));
          } else if (if_add_relevance && OB_FAIL(add_one_relevance(vid, relevance_record.at(i)))) {
            LOG_WARN("failed to add relevance", K(ret), K(i));
          }
        } else {
          // brute_cnt + batch_row_count already > MAX_HNSW_BRUTE_FORCE_SIZE
          // do not choose brue force, just add vids to bitmap
          if (go_brute_force_) {
            go_brute_force_ = false;
            lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(adaptor->get_tenant_id(), "VIBitmapADPI"));
            for (int j = 0; OB_SUCC(ret) && j < brute_cnt; ++j) {
              if (OB_FAIL(adaptor->add_extra_valid_vid_without_malloc_guard(ada_ctx, vids[j]))) {
                LOG_WARN("failed to add valid vid", K(ret));
              }
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(adaptor->add_extra_valid_vid(ada_ctx, vid))) {
            LOG_WARN("failed to add valid vid", K(ret));
          } else if (if_add_relevance && i >= relevance_record.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected vid count", K(vec_aux_ctdef_->relevance_col_cnt_), K(relevance_record.count()));
          } else if (if_add_relevance && OB_FAIL(add_one_relevance(vid, relevance_record.at(i)))) {
            LOG_WARN("failed to add relevance", K(ret), K(i));
          }
        }
      }
    } else {
      int64_t scan_row_cnt = 0;
      if (OB_FAIL(inv_idx_scan_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row.", K(ret));
        }
        index_end = true;
      }

      if (OB_FAIL(ret) && OB_ITER_END != ret) {
      } else if (scan_row_cnt > 0) {
        ret = OB_SUCCESS;
      }

      if (OB_SUCC(ret)) {
        if (if_add_relevance && OB_FAIL(get_relevances(vec_op_alloc_, relevance_record, scan_row_cnt))) {
          LOG_WARN("failed to get relevance", K(ret));
        } else {
          const ObDASScanCtDef *ctdef = nullptr;
          ObDASScanRtDef *rtdef = nullptr;
          if (OB_FAIL(get_ctdef_with_rowkey_exprs(ctdef, rtdef))) {
            LOG_WARN("failed to get ctdef with rowkey exprs", K(ret));
          } else if (OB_UNLIKELY(ctdef->rowkey_exprs_.count() != 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid rowkey cnt", K(ret), K(ctdef->rowkey_exprs_.count()));
          } else {
            ObExpr *expr = ctdef->rowkey_exprs_.at(0);
            ObEvalCtx::BatchInfoScopeGuard guard(*rtdef->eval_ctx_);
            guard.set_batch_size(scan_row_cnt);
            guard.set_batch_idx(0);
            ObDatum *vid_datums = expr->locate_batch_datums(*rtdef->eval_ctx_);
            if (go_brute_force_ && brute_cnt + scan_row_cnt < MAX_HNSW_BRUTE_FORCE_SIZE) {
              for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
                int64_t vid = vid_datums[i].get_uint64();
                vids[brute_cnt] = vid;
                brute_cnt++;
                if (if_add_relevance) {
                  if (OB_UNLIKELY(i >= relevance_record.count())) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("unexpected vid count", K(vec_aux_ctdef_->relevance_col_cnt_), K(relevance_record.count()), K(scan_row_cnt));
                  } else if (OB_FAIL(add_one_relevance(vid, relevance_record.at(i)))) {
                    LOG_WARN("failed to add relevance", K(ret), K(i));
                  }
                }
              }
            } else {
              lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(adaptor->get_tenant_id(), "VIBitmapADPI"));
              // first, add vids into bitmap
              if (go_brute_force_) {
                go_brute_force_ = false;
                for (int j = 0; OB_SUCC(ret) && j < brute_cnt; ++j) {
                  if (OB_FAIL(adaptor->add_extra_valid_vid_without_malloc_guard(ada_ctx, vids[j]))) {
                    LOG_WARN("failed to add valid vid", K(ret));
                  }
                }
              }

              // then add this batch into bitmap
              for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
                int64_t vid = vid_datums[i].get_uint64();
                if (OB_FAIL(adaptor->add_extra_valid_vid_without_malloc_guard(ada_ctx, vid))) {
                  LOG_WARN("failed to add valid vid", K(ret));
                } else if (if_add_relevance) {
                  if (OB_UNLIKELY(i >= relevance_record.count())) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("unexpected vid count", K(vec_aux_ctdef_->relevance_col_cnt_), K(relevance_record.count()), K(scan_row_cnt));
                  } else if (OB_FAIL(add_one_relevance(vid, relevance_record.at(i)))) {
                    LOG_WARN("failed to add relevance", K(ret), K(i));
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDASHNSWScanIter::init_rel_map(ObPluginVectorIndexAdaptor* adaptor)
{
  int ret = OB_SUCCESS;
  if (rel_map_.created()) {
    rel_map_.reuse();
  } else {
    ObMemAttr bucket_attr(adaptor->get_tenant_id(), "HnswRelMap");
    ObMemAttr node_attr(adaptor->get_tenant_id(), "HnswRelMap");
    if (OB_FAIL(rel_map_.create(MAX_HNSW_BRUTE_FORCE_SIZE, bucket_attr, node_attr))) {
      LOG_WARN("failed to create json bucket num", K(ret));
    }
  }
  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state_pre_filter_with_idx_filter(
    ObVectorQueryAdaptorResultContext *ada_ctx,
    ObPluginVectorIndexAdaptor* adaptor,
    int64_t *&vids,
    int &brute_cnt,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ada_ctx) || OB_ISNULL(adaptor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shouldn't be null.", K(ret), KP(ada_ctx), KP(adaptor));
  } else if (use_vid_ && OB_ISNULL(rowkey_vid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey vid iter is null.", K(ret));
  } else {
    vids = nullptr;
    brute_cnt = 0;
    ObArenaAllocator &allocator = mem_context_->get_arena_allocator();
    int64_t batch_row_count = vec_aux_rtdef_->eval_ctx_->max_batch_size_ > 0 ?
                                min(vec_aux_rtdef_->eval_ctx_->max_batch_size_, ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE) :
                                ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
    ObVidBound bound;
    if (OB_FAIL(adaptor->get_vid_bound(bound))) {
      LOG_WARN("fail to get vid bound from adaptor", K(ret));
    } else if (!ada_ctx->is_prefilter_valid() && OB_FAIL(ada_ctx->init_prefilter(bound.min_vid_, bound.max_vid_))) {
      LOG_WARN("init bitmaps failed.", K(ret), K(bound.min_vid_), K(bound.max_vid_));
    } else if (OB_ISNULL(vids = static_cast<int64_t *>(allocator.alloc(sizeof(int64_t) * MAX_HNSW_BRUTE_FORCE_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator vids", K(ret));
    } else if (use_vid_) {
      ret = get_vid_from_idx_filter(ada_ctx, adaptor, vids, brute_cnt, is_vectorized, batch_row_count);
    } else {
      ret = get_pk_increment_from_idx_filter(ada_ctx, adaptor, vids, brute_cnt, is_vectorized, batch_row_count);
    }
  }

  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    LOG_WARN("get next row failed.", K(ret));
  } else {
    ret = OB_SUCCESS;
  }

  return ret;
}

bool ObDASHNSWScanIter::can_be_last_search(int64_t old_ef,
                                           int64_t need_cnt_next,
                                           float select_ratio)
{
  bool ret_bool = false;
  if (select_ratio < ITER_CONSIDER_LAST_SEARCH_SELETIVITY) {
    ret_bool = false;
  } else {
    int64_t expect_get = std::ceil(static_cast<float>(need_cnt_next) / select_ratio);
    ret_bool = expect_get * 2 <= old_ef;
  }
  LOG_TRACE("iteractive filter log: can be last search", K(ret_bool), K(old_ef), K(select_ratio));
  return ret_bool;
}

/* simple_cmp_filter:
1. only one filter (c1 > 1 and c2 < 2 is not supported);
2. rowkey column only one；
3. simple compare, including: >, <, >=, <=, =, !=;
*/
int ObDASHNSWScanIter::check_is_simple_cmp_filter()
{
  int ret = OB_SUCCESS;
  if (!use_vid_ || vec_aux_ctdef_->relevance_col_cnt_ > 0) {
    simple_cmp_info_.inited_ = false;
  } else {
    const ObDASScanCtDef * rowkey_vid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
    if (OB_NOT_NULL(data_filter_ctdef_) && OB_NOT_NULL(data_filter_rtdef_) && OB_NOT_NULL(rowkey_vid_ctdef)
      && extra_column_count_ == 1 && rowkey_vid_ctdef->rowkey_exprs_.count() == 1
      && !data_filter_ctdef_->pd_expr_spec_.pushdown_filters_.empty()
      && data_filter_ctdef_->pd_expr_spec_.pushdown_filters_.count() == 1) {
      ObExpr* filter_expr = data_filter_ctdef_->pd_expr_spec_.pushdown_filters_.at(0);
      if (OB_NOT_NULL(filter_expr) && filter_expr->arg_cnt_ == 2) {
        ObExprOperatorType filter_type = filter_expr->type_;
        if (T_OP_EQ == filter_type || T_OP_NE == filter_type
          || T_OP_GT == filter_type || T_OP_LT == filter_type
          || T_OP_GE == filter_type || T_OP_LE == filter_type) {
          ObExpr *arg1 = filter_expr->args_[0];
          ObExpr *arg2 = filter_expr->args_[1];
          ObExpr* arg_col = (arg1->type_ == T_REF_COLUMN) ? arg1 : arg2;
          ObExpr* arg_num = (arg1->type_ == T_REF_COLUMN) ? arg2 : arg1;
          ObExpr *rowkey_expr = rowkey_vid_ctdef->rowkey_exprs_.at(0);
          simple_cmp_info_.inited_ = false;
          bool is_rowkey_filter = rowkey_expr == arg_col;
          simple_cmp_info_.filter_expr_ = filter_expr;
          ObDatum* filter_datum_ = nullptr;
          if (!is_rowkey_filter) {
            // do nothing
          } else if (OB_UNLIKELY(OB_ISNULL(data_filter_rtdef_->eval_ctx_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("eval ctx is null", K(ret));
          } else if (OB_UNLIKELY(OB_FAIL(arg_num->eval(*(data_filter_rtdef_->eval_ctx_), filter_datum_)))) {
            LOG_WARN("eval filter arg failed", K(ret));
          } else if (OB_FAIL(filter_datum_->to_obj(simple_cmp_info_.filter_arg_, arg_num->obj_meta_))) {
            LOG_WARN("get filter obj failed", K(ret), K(arg_num->obj_meta_));
          } else {
            simple_cmp_info_.inited_ = OB_NOT_NULL(arg_col);
          }
        } // else not simple cmp
      }
    }
  }
  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state_post_filter(
    ObVectorQueryAdaptorResultContext *ada_ctx,
    ObPluginVectorIndexAdaptor* adaptor,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  bool end_search = false;
  bool first_search = true;
  int64_t iter_scan_total_num = 0;
  adaptive_ctx_.iter_times_ = 0;
  if (is_iter_filter()) {
    query_cond_.query_limit_ = std::max(query_cond_.ef_search_, static_cast<int64_t>(std::ceil(query_cond_.query_limit_ * FIXED_MAGNIFICATION_RATIO)));
    query_cond_.ef_search_ = std::max(query_cond_.ef_search_, static_cast<int64_t>(query_cond_.query_limit_));
    query_cond_.ef_search_ = query_cond_.ef_search_ > VSAG_MAX_EF_SEARCH ? VSAG_MAX_EF_SEARCH : query_cond_.ef_search_;
    if (OB_FAIL(check_is_simple_cmp_filter())) {
      LOG_WARN("failed to check can filter in hnsw.", K(ret));
    }
  }
  LOG_TRACE("vector index show post-filter query info", K(vec_index_type_), K(vec_idx_try_path_), K(simple_cmp_info_.inited_), KPC(simple_cmp_info_.filter_expr_),
  K(extra_column_count_), K(query_cond_.query_limit_), K(query_cond_.ef_search_));
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  int64_t hnsw_max_iter_scan_nums = tenant_config->_hnsw_max_scan_vectors;
  while (OB_SUCC(ret) && !end_search) {
    ++adaptive_ctx_.iter_times_;
    if (first_search && OB_FAIL(process_adaptor_state_post_filter_once(ada_ctx, adaptor))) {
      LOG_WARN("failed to process adaptor state post filter once.", K(ret), K(vec_index_type_), K(vec_idx_try_path_), K(query_cond_));
    } else if (!first_search && OB_FAIL(adaptor->query_next_result(ada_ctx, &query_cond_, tmp_adaptor_vid_iter_))) {
    } else if (first_search && OB_FALSE_IT(first_search = false)) {
    } else if (!is_iter_filter()) {
      end_search = true;
    } else if (OB_ISNULL(tmp_adaptor_vid_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shouldn't be null.", K(ret), K(tmp_adaptor_vid_iter_));
    } else if (OB_FAIL(post_query_vid_with_filter(ada_ctx, adaptor, hnsw_max_iter_scan_nums, iter_scan_total_num, is_vectorized))) {
      LOG_WARN("failed to query vid with filter.", K(ret), K(extra_column_count_), K(adaptive_ctx_.iter_times_));
    } else if (query_cond_.query_limit_ == 0) {
      end_search = true;
    }
  }
  LOG_TRACE("print hnsw search times", K(adaptive_ctx_));
  return ret;
}

int ObDASHNSWScanIter::set_rowkey_by_vid(ObNewRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tmp_adaptor_vid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shouldn't be null.", K(ret), KPC(adaptor_vid_iter_), KPC(tmp_adaptor_vid_iter_));
  } else {
    int64_t extra_column_cnt = tmp_adaptor_vid_iter_->get_extra_column_count();
    if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be null", K(ret));
    } else if (row->get_count() < ObAdaptorIterRowIdx::ROWKEY_START_IDX + extra_column_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be one row", K(row->get_count()), K(ret));
    } else if (extra_column_cnt == 0) {
      // if there is no extro info, find vid-rowkey table
      ObRowkey vid_row(&row->get_cell(ObAdaptorIterRowIdx::VID_OBJ_IDX), 1);
      ObRowkey *rowkey;
      if (OB_ISNULL(data_filter_iter_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data scan iter is null", K(ret));
      } else if (!data_filter_iter_first_scan_ && OB_FAIL(reuse_filter_data_table_iter())) {
        LOG_WARN("failed to reuse com aux vec iter.", K(ret));
      } else if (use_vid_) {
        if (OB_FAIL(get_rowkey_from_vid_rowkey_table(mem_context_->get_arena_allocator(), vid_row, rowkey))) {
          // do not overwrite ret code, in case search data_table by wrong rowkey
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get rowkey.", K(ret));
          }
        }
      } else {
        vid_row.get_obj_ptr()->meta_.set_uint64();
        rowkey = &vid_row;
      }

      if (FAILEDx(ObDasVecScanUtils::set_lookup_key(*rowkey, data_filter_scan_param_, data_filter_ctdef_->ref_table_id_))) {
        LOG_WARN("failed to set lookup key", K(ret));
      } else if (vec_aux_ctdef_->relevance_col_cnt_ > 0 && OB_FAIL(set_func_lookup_rowkey(*rowkey))) {
        LOG_WARN("faile to set lookup scan key", K(ret), KPC(rowkey));
      }
    } else {
      // if there is extra info, set rowkey directly
      ObRowkey rowkey;
      ObObj *rowkey_objs = nullptr;
      if (OB_FAIL(build_rowkey_obj_from_extra_info(&row->get_cell(ObAdaptorIterRowIdx::ROWKEY_START_IDX),
                                                   rowkey_objs))) {
        LOG_WARN("failed to build rowkey from extra info obj", K(ret));
      } else if (OB_FALSE_IT(rowkey.assign(rowkey_objs, extra_column_count_))) {
      } else if (!data_filter_iter_first_scan_ && OB_FAIL(reuse_filter_data_table_iter())) {
        LOG_WARN("failed to reuse com aux vec iter.", K(ret));
      } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(rowkey, data_filter_scan_param_,
                                                           data_filter_ctdef_->ref_table_id_))) {
        LOG_WARN("failed to set lookup key", K(ret));
      } else if (vec_aux_ctdef_->relevance_col_cnt_ > 0 && OB_FAIL(set_func_lookup_rowkey(rowkey))) {
        LOG_WARN("faile to set lookup scan key", K(ret), K(rowkey));
      }
    }
  }
  return ret;
}

int ObDASHNSWScanIter::get_simple_cmp_filter_res(ObNewRow *row, bool& res)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tmp_adaptor_vid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shouldn't be null.", K(ret), KPC(adaptor_vid_iter_), KPC(tmp_adaptor_vid_iter_));
  } else {
    int64_t extra_column_cnt = tmp_adaptor_vid_iter_->get_extra_column_count();
    ObExpr* filter_expr = simple_cmp_info_.filter_expr_;
    if (OB_ISNULL(row) || OB_ISNULL(filter_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be null", K(ret), KPC(row), KPC(filter_expr));
    } else if (row->get_count() < ObAdaptorIterRowIdx::ROWKEY_START_IDX + extra_column_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be one row", K(row->get_count()), K(ret));
    } else if (extra_column_cnt <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shouldn't be null.", K(ret), K(extra_column_count_), K(extra_column_cnt));
    } else if (extra_column_cnt > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("simple_cmp_filter extra_column_cnt must 1", K(ret), K(extra_column_count_), K(extra_column_cnt));
    } else {
      int result = 0;
      ObExpr *arg1 = filter_expr->args_[0];
      ObExpr *arg2 = filter_expr->args_[1];
      bool is_first_col = arg1->type_ == T_REF_COLUMN;
      ObExprOperatorType filter_type = filter_expr->type_;
      ObObj* obj1 = is_first_col? &row->get_cell(ObAdaptorIterRowIdx::ROWKEY_START_IDX) : &simple_cmp_info_.filter_arg_;
      ObObj* obj2 = is_first_col? &simple_cmp_info_.filter_arg_ : &row->get_cell(ObAdaptorIterRowIdx::ROWKEY_START_IDX);
      if (obj1->is_string_type() && obj2->is_string_type()) {
        obj1->compare(*obj2, is_first_col? obj1->get_collation_type() : obj2->get_collation_type(), result);
      } else {
        obj1->compare(*obj2, result);
      }
      if (result == 0) {
        res = (filter_type == T_OP_EQ || filter_type == T_OP_GE || filter_type == T_OP_LE);
      } else if (result == 1) {
        res = (filter_type == T_OP_NE || filter_type == T_OP_GT || filter_type == T_OP_GE);
      } else if (result == -1) {
        res = (filter_type == T_OP_NE || filter_type == T_OP_LT || filter_type == T_OP_LE);
      }
    }
  }
  return ret;
}

int ObDASHNSWScanIter::set_func_lookup_rowkey(ObRowkey &rowkey)
{
  INIT_SUCC(ret);
  if (vec_aux_ctdef_->relevance_col_cnt_ <= 0) {
    // do nothing
  } else if (OB_ISNULL(func_lookup_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func lookup iter is null", K(ret), KP(func_lookup_iter_));
  } else if (func_lookup_iter_->get_type() == ObDASIterType::DAS_ITER_FUNC_LOOKUP) {
    ObDASFuncLookupIter* func_iter = static_cast<ObDASFuncLookupIter*>(func_lookup_iter_);
    if (!func_lookup_first_scan_ && OB_FAIL(func_iter->reuse())) {
      LOG_WARN("failed to reuse com aux vec iter.", K(ret));
    } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(rowkey, func_filter_scan_param_,
                                                         static_cast<const ObDASScanCtDef*>(func_iter->index_ctdef_)->ref_table_id_))) {
      LOG_WARN("failed to set lookup key", K(ret));
    }
  } else if (func_lookup_iter_->get_type() == ObDASIterType::DAS_ITER_FUNC_DATA) {
    ObDASFuncDataIter* func_data_iter = static_cast<ObDASFuncDataIter*>(func_lookup_iter_);
    ObDocIdExt doc_id;
    if (rowkey.get_obj_cnt() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey cnt is unexpected", K(ret), K(rowkey.get_obj_cnt()));
    } else if (func_data_iter->has_main_lookup_iter()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("func data should not index back", K(ret));
    } else if (!func_lookup_first_scan_ && OB_FAIL(func_data_iter->reuse())) {
      LOG_WARN("failed to reuse com aux vec iter.", K(ret));
    } else if (OB_FAIL(doc_id.from_obj(*rowkey.ptr()))){
      LOG_WARN("failed to get docid from rowkey", K(ret));
    } else if (OB_FAIL(func_data_iter->add_doc_id(doc_id))) {
      LOG_WARN("failed to add doc id", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func lookup iter type is unexpected", K(ret), K(func_lookup_iter_->get_type()));
  }
  return ret;
}

int ObDASHNSWScanIter::filter_func_lookup_iter(bool is_vectorized,
                                              bool& filter_res)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(func_lookup_iter_) || OB_ISNULL(func_lookup_ctdef_) || OB_ISNULL(func_lookup_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func lookup iter is null", K(ret), KP(func_lookup_iter_), KP(func_lookup_ctdef_), KP(func_lookup_rtdef_));
  } else if (func_lookup_iter_->get_type() == ObDASIterType::DAS_ITER_FUNC_LOOKUP) {
    ObDASFuncLookupIter* func_iter = static_cast<ObDASFuncLookupIter*>(func_lookup_iter_);

    if (func_lookup_first_scan_) {
      func_filter_scan_param_.need_switch_param_ = false;
      if (OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_, rowkey_doc_tablet_id_,
                   static_cast<const ObDASScanCtDef*>(func_iter->index_ctdef_),
                   static_cast<ObDASScanRtDef*>(func_iter->index_rtdef_), tx_desc_, snapshot_, func_filter_scan_param_))) {
        LOG_WARN("failed to init scan param", K(ret));
      } else if (OB_FALSE_IT(func_iter->set_index_scan_param(func_filter_scan_param_))) {
      } else if (OB_FAIL(func_iter->do_table_scan())) {
        LOG_WARN("failed to do scan", K(ret));
      } else {
        func_lookup_first_scan_ = false;
      }
    } else {
      if (OB_FAIL(func_iter->rescan())) {
        LOG_WARN("failed to rescan func lookup iterator.", K(ret));
      }
    }
  } else if (func_lookup_iter_->get_type() == ObDASIterType::DAS_ITER_FUNC_DATA) {
    ObDASFuncDataIter* func_data_iter = static_cast<ObDASFuncDataIter*>(func_lookup_iter_);
    if (func_lookup_first_scan_) {
      func_filter_scan_param_.need_switch_param_ = false;
      if (OB_FAIL(func_data_iter->do_table_scan())) {
        LOG_WARN("failed to do scan", K(ret));
      } else {
        func_lookup_first_scan_ = false;
      }
    } else {
      if (OB_FAIL(func_data_iter->rescan())) {
        LOG_WARN("failed to rescan func lookup iterator.", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func lookup iter type is unexpected", K(ret), K(func_lookup_iter_->get_type()));
  }

  if (OB_FAIL(ret)) {
  } else if (is_vectorized) {
    int64_t scan_row_cnt = 0;
    ret = func_lookup_iter_->get_next_rows(scan_row_cnt, 1);
  } else {
    ret = func_lookup_iter_->get_next_row();
  }

  if (OB_SUCC(ret)) {
    filter_res = true;
  } else if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
    LOG_WARN("iter end of func lookup iter", K(ret));
    filter_res = false;
  }
  return ret;
}

int ObDASHNSWScanIter::add_one_relevance(int64_t vid, double* &rel_record)
{
  INIT_SUCC(ret);
  double* val = nullptr;
  if (OB_ISNULL(rel_record)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(vid));
  } else if (OB_FAIL(rel_map_.get_refactored(vid, val))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      if (OB_FAIL(rel_map_.set_refactored(vid, rel_record))) {
        LOG_WARN("failed to set json pl object into bucket.", K(ret));
      }
    }
  } else {
    ret == OB_ERR_DUPLICATE_KEY;
    LOG_WARN("alreay add vid", K(vid), KP(val));
  }
  return ret;
}

int ObDASHNSWScanIter::get_one_relevance(ObIAllocator &allocator, double* &rel_array)
{
  INIT_SUCC(ret);
  const sql::ExprFixedArray &out_exprs = vec_aux_ctdef_->result_output_;
  // supposed to be: vid + extra_info + relevance
  double *rel_ptr = nullptr;
  void *buf = nullptr;
  int64_t rel_cnt = vec_aux_ctdef_->relevance_col_cnt_;
  if (out_exprs.count() != 1 + extra_column_count_ + rel_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid output cnt", K(ret), K(out_exprs.count()), K(extra_column_count_), K(vec_aux_ctdef_->relevance_col_cnt_));
  } else if (OB_ISNULL(rel_ptr = static_cast<double *>(allocator.alloc(sizeof(double) * rel_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(rel_cnt));
  } else {
    for (int64_t i = extra_column_count_ + 1, idx = 0; OB_SUCC(ret) && i < out_exprs.count(); ++i, ++idx) {
      ObObj tmp_obj;
      ObExpr *expr = out_exprs.at(i);
      ObDatum &datum = expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
      if (OB_ISNULL(datum.ptr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get col datum null", K(ret), K(i), K(idx));
      } else if (OB_FAIL(datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret), K(i), K(idx));
      } else if (tmp_obj.is_double()) {
        rel_ptr[idx] = tmp_obj.get_double();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid obj type", K(ret), K(tmp_obj), K(i), K(idx));
      }
    }

    if (OB_SUCC(ret)) {
      rel_array = rel_ptr;
    }
  }
  return ret;
}

int ObDASHNSWScanIter::filter_by_index_back(int64_t vid,
                                            ObNewRow *row,
                                            bool is_vectorized,
                                            bool& filter_res)
{
  INIT_SUCC(ret);
  double* rel_array = nullptr;
  ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
  guard.set_batch_size(1);
  if (OB_FAIL(set_rowkey_by_vid(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to set rowkey by vid.", K(ret), K(vid));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (vec_aux_ctdef_->relevance_col_cnt_ > 0) {
    if (OB_FAIL(filter_func_lookup_iter(is_vectorized, filter_res))) {
      LOG_WARN("failed to filter func lookup iter.", K(ret), K(vid));
    } else if (OB_FALSE_IT(guard.set_batch_idx(0))) {
    } else if (OB_FAIL(get_one_relevance(vec_op_alloc_, rel_array))) {
      LOG_WARN("failed to get relevance", K(ret), K(vid));
    }
  } else {
    filter_res = true;
  }

  if (OB_FAIL(ret) || !filter_res) {
  } else if (OB_FAIL(do_aux_table_scan(data_filter_iter_first_scan_,
                                      data_filter_scan_param_,
                                      data_filter_ctdef_,
                                      data_filter_rtdef_,
                                      data_filter_iter_,
                                      com_aux_vec_tablet_id_))) {
    LOG_WARN("failed to do data filter table scan.", K(ret), K(data_filter_iter_first_scan_));
  } else if (OB_FAIL(get_single_row_from_data_filter_iter(is_vectorized))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to scan vid rowkey iter", K(ret));
    } else {
      ret = OB_SUCCESS;
      filter_res = false;
    }
  } else {
    filter_res = true;
  }

  if (OB_SUCC(ret) && vec_aux_ctdef_->relevance_col_cnt_ > 0) {
    if (OB_ISNULL(rel_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rel array is null", K(ret));
    } else if (filter_res) {
      if (OB_FAIL(add_one_relevance(vid, rel_array))) {
        LOG_WARN("fail to add rel_array", K(ret), K(vid));
      }
    } else {
      vec_op_alloc_.free(rel_array);
    }
  }
  return ret;
}

int ObDASHNSWScanIter::post_query_vid_with_filter(
    ObVectorQueryAdaptorResultContext *ada_ctx,
    ObPluginVectorIndexAdaptor* adaptor,
    int64_t hnsw_max_iter_scan_nums,
    int64_t& iter_scan_total_num,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ada_ctx) || OB_ISNULL(adaptor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shouldn't be null.", K(ret), K(ada_ctx), K(adaptor));
  } else {
    if (OB_ISNULL(adaptor_vid_iter_)) {
      void *iter_buff = nullptr;
      uint64_t final_res_cnt = limit_param_.limit_ + limit_param_.offset_;
      int64_t extra_info_actual_size = 0;

      if (is_hnsw_bq()) {
        final_res_cnt = get_reorder_count(query_cond_.ef_search_, final_res_cnt, search_param_);
      }

      if (OB_FAIL(adaptor->get_extra_info_actual_size(extra_info_actual_size))) {
        LOG_WARN("failed to get extra info actual size.", K(ret));
      } else if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryVidIterator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocator adaptor vid iter.", K(ret));
      } else if (OB_FALSE_IT(adaptor_vid_iter_ = new(iter_buff) ObVectorQueryVidIterator(extra_column_count_, extra_info_actual_size, query_cond_.rel_count_, query_cond_.rel_map_ptr_))) {
      } else if (OB_FAIL(adaptor_vid_iter_->init(final_res_cnt, &vec_op_alloc_))) {
        LOG_WARN("iter init failed.", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(adaptor_vid_iter_) || OB_ISNULL(tmp_adaptor_vid_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shouldn't be null.", K(ret), KPC(adaptor_vid_iter_), KPC(tmp_adaptor_vid_iter_));
    } else {
      int64_t unfiltered_vid_cnt = tmp_adaptor_vid_iter_->get_total();
      const int64_t* unfiltered_vids = tmp_adaptor_vid_iter_->get_vids();
      const float* unfiltered_distance = tmp_adaptor_vid_iter_->get_distance();
      const ObVecExtraInfoPtr &unfiltered_extra_info = tmp_adaptor_vid_iter_->get_extra_info();
      int64_t total_before_add = adaptor_vid_iter_->get_total();
      int64_t extra_column_cnt = tmp_adaptor_vid_iter_->get_extra_column_count();
      iter_scan_total_num += unfiltered_vid_cnt;
      for (int i = 0; OB_SUCC(ret) && i < unfiltered_vid_cnt && !adaptor_vid_iter_->get_enough(); ++i) {
        ObNewRow *row = nullptr;
        bool filter_res = false;
        if (OB_FAIL(tmp_adaptor_vid_iter_->get_next_row(row, vec_aux_ctdef_->result_output_, true))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to get next next row from adaptor vid iter", K(ret), K(i));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (simple_cmp_info_.inited_) { // use simpel cmp
          if (OB_FAIL(get_simple_cmp_filter_res(row, filter_res))) {
            LOG_WARN("failed to get simple cmp filter res.", K(ret), K(i));
          } else if (filter_res && (OB_FAIL(adaptor_vid_iter_->add_result(
                        unfiltered_vids[i], unfiltered_distance[i],
                        unfiltered_extra_info.is_null() ? nullptr : unfiltered_extra_info[i])))) {
            LOG_WARN("failed to add result", K(ret), K(i));
          }
        } else if (OB_FAIL(filter_by_index_back(unfiltered_vids[i], row, is_vectorized, filter_res))) {
          LOG_WARN("fail to filter by index back", K(extra_column_cnt), K(i), K(unfiltered_vids[i]), K(ret));
        } else if (!filter_res) {
          // do nothing
        } else if (OB_FAIL(adaptor_vid_iter_->add_result(
                        unfiltered_vids[i], unfiltered_distance[i],
                        unfiltered_extra_info.is_null() ? nullptr : unfiltered_extra_info[i]))) {
          LOG_WARN("failed to add result", K(ret), K(i), K(extra_column_cnt));
        }
      } // end for
      if (OB_FAIL(ret)) {
      } else if (tmp_adaptor_vid_iter_->get_total() < query_cond_.query_limit_
      || (iter_scan_total_num > hnsw_max_iter_scan_nums && hnsw_max_iter_scan_nums > 0)) {
        // res is already less than limit, no need to find again
        query_cond_.query_limit_ = 0;
        LOG_TRACE("iteractive filter log:", K(tmp_adaptor_vid_iter_->get_total()), K(query_cond_.query_limit_), K(total_before_add), K(adaptor_vid_iter_->get_total()));
      } else {
        int64_t need_cnt_next = adaptor_vid_iter_->get_alloc_size() - adaptor_vid_iter_->get_total();
        int total_after_add = adaptor_vid_iter_->get_total();
        int added_cnt  = total_after_add - total_before_add;
        float old_limit = static_cast<float>(query_cond_.query_limit_);
        float old_ef = static_cast<float>(query_cond_.ef_search_);
        adaptive_ctx_.iter_res_row_cnt_ += added_cnt;
        adaptive_ctx_.iter_filter_row_cnt_ += unfiltered_vid_cnt;
        if (can_retry_ && OB_FAIL(check_iter_filter_need_retry())) {
          LOG_WARN("ret of check iter filter need retry.", K(ret), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_));
        } else if (need_cnt_next > 0) {
          float need_ratio = static_cast<float>(need_cnt_next) / static_cast<float>(added_cnt);
          float select_ratio = static_cast<float>(added_cnt) / static_cast<float>(unfiltered_vid_cnt);
          int need_res_cnt = select_ratio > 0 ? static_cast<int64_t>(std::ceil(need_cnt_next / select_ratio)) : need_cnt_next;
          uint32_t new_limit = 0;
          int64_t new_ef = old_ef;
          if (added_cnt == 0) {
            // selectivity is 0, amplify directly
            new_limit = old_ef * FIXED_MAGNIFICATION_RATIO;
            new_ef = std::max(query_cond_.ef_search_, static_cast<int64_t>(new_limit));
            new_ef = new_ef > VSAG_MAX_EF_SEARCH ? VSAG_MAX_EF_SEARCH : new_ef;
            query_cond_.is_last_search_ = false;
          } else {
            int need_res_cnt = static_cast<int64_t>(std::ceil(need_cnt_next / select_ratio));
            if (can_be_last_search(old_ef, need_cnt_next, select_ratio)) {
              new_limit = old_ef;
              query_cond_.is_last_search_ = true;
            } else {
              new_limit = need_res_cnt;
              new_ef = std::max(query_cond_.ef_search_, static_cast<int64_t>(new_limit));
              new_ef = new_ef > VSAG_MAX_EF_SEARCH ? VSAG_MAX_EF_SEARCH : new_ef;
            }
          }
          query_cond_.query_limit_ = new_limit;
          // hnsw_bq will reoder by top-N operator, so the limit needs to be increased
          if (is_hnsw_bq()) query_cond_.query_limit_ = get_reorder_count(new_ef, new_limit, search_param_);
          query_cond_.ef_search_ = new_ef;
          LOG_TRACE("iteractive filter arg log:", K(total_after_add), K(total_before_add), K(unfiltered_vid_cnt), K(select_ratio),  K(old_limit), K(new_limit),
                                                  K(old_ef), K(new_ef), K(query_cond_.query_limit_), K(query_cond_.ef_search_));
        } else {
          query_cond_.query_limit_ = 0;
        }
      }

      if (OB_NOT_NULL(tmp_adaptor_vid_iter_)) {
        tmp_adaptor_vid_iter_->reset();
        tmp_adaptor_vid_iter_->~ObVectorQueryVidIterator();
        tmp_adaptor_vid_iter_ = nullptr;
      }
    }
  }
  return ret;
}

int ObDASHNSWScanIter::process_adaptor_state_post_filter_once(
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

    if (adaptor->get_can_skip() == SKIP) {
      if (OB_FAIL(ada_ctx->init_bitmaps())) {
        LOG_WARN("failed to init bitmaps", K(ret));
      } else {
        if (adaptor->get_snap_data_()->rb_flag_) {
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

int ObDASHNSWScanIter::get_single_row_from_data_filter_iter(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data_filter_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data filter iter is null.", K(ret));
  } else {
    data_filter_iter_->clear_evaluated_flag();
    if (is_vectorized) {
      int64_t scan_row_cnt = 0;
      ret = data_filter_iter_->get_next_rows(scan_row_cnt, 1);
    } else {
      ret = data_filter_iter_->get_next_row();
    }
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
      if (OB_NOT_NULL(snapshot_iter_) && OB_FALSE_IT(query_cond_.row_iter_ = snapshot_iter_->get_output_result_iter())) {
      } else if (!ada_ctx.get_ls_leader() && OB_FAIL(prepare_follower_query_cond(query_cond_))) {
        LOG_WARN("fail to prepare query cond of follower", K(ret));
      } else if (OB_FAIL(adaptor.query_result(ls_id_, &ada_ctx, &query_cond_, is_iter_filter() ? tmp_adaptor_vid_iter_ : adaptor_vid_iter_))) {
        LOG_WARN("failed to query result.", K(ret));
      }
      break;
    }
    case ObVidAdaLookupStatus::STATES_REFRESH: {        // refresh
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

int ObDASHNSWScanIter::get_ob_hnsw_ef_search(uint64_t &ob_hnsw_ef_search)
{
  int ret = OB_SUCCESS;
  const uint64_t OB_HNSW_EF_SEARCH_DEFAULT = 64;  // same as SYS_VAR_OB_HNSW_EF_SEARCH

  ObSQLSessionInfo *session = nullptr;
  if (OB_NOT_NULL(vec_aux_ctdef_) && vec_aux_ctdef_->vec_query_param_.is_set_ef_search_) {
    ob_hnsw_ef_search =  vec_aux_ctdef_->vec_query_param_.ef_search_;
    LOG_TRACE("use stmt ef_search", K(ob_hnsw_ef_search));
  } else if (OB_ISNULL(session = exec_ctx_->get_my_session())) {
    ob_hnsw_ef_search = OB_HNSW_EF_SEARCH_DEFAULT;
    LOG_WARN("session is null", K(ret), KP(exec_ctx_));
  } else if (OB_FAIL(session->get_ob_hnsw_ef_search(ob_hnsw_ef_search))) {
    LOG_WARN("failed to get ob hnsw ef search", K(ret));
  }

  return ret;
}

int64_t ObDASHNSWScanIter::get_reorder_count(const int64_t ef_search, const int64_t topK, const ObVectorIndexParam& param)
{
  // max is ef_search
  const float refine_k = param.refine_k_;
  int64_t refine_cnt = refine_k * topK;
  if (refine_cnt < topK) refine_cnt = topK;
  LOG_TRACE("reorder count info", K(ef_search), K(topK), K(refine_k), K(refine_cnt));
  return OB_MIN(OB_MAX(topK, OB_MIN(refine_cnt, ef_search)), MAX_VSAG_QUERY_RES_SIZE);
}

int ObDASHNSWScanIter::set_vector_query_condition(ObVectorQueryConditions &query_cond)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(search_vec_) || OB_ISNULL(sort_rtdef_) || OB_ISNULL(sort_rtdef_->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null.", K(ret));
  } else {
    query_cond.query_order_ = true;
    query_cond.query_scn_ = snapshot_scan_param_.snapshot_.core_.version_;
    query_cond.only_complete_data_ = only_complete_data_; // ture when search brute force
    query_cond.scan_param_ = &snapshot_scan_param_;
    query_cond.rel_count_ = vec_aux_ctdef_->relevance_col_cnt_;
    query_cond.rel_map_ptr_ = &rel_map_;
    query_cond.is_post_with_filter_ = is_iter_filter();

    uint64_t ob_hnsw_ef_search = 0;
    if (OB_FAIL(get_ob_hnsw_ef_search(ob_hnsw_ef_search))) {
      LOG_WARN("failed to get ob hnsw ef search", K(ret), K(ob_hnsw_ef_search));
    } else if (OB_FALSE_IT(query_cond.ef_search_ = ob_hnsw_ef_search)) {
    } else {
      uint64_t real_limit = limit_param_.limit_ + limit_param_.offset_;
      // if selectivity_ == 1 means there is no filter
      if (is_hnsw_bq()) {
        // normally topK(real_limit) should be the same as ef_search for bq
        // but if topK is larger than ef_search, use topK
        real_limit = get_reorder_count(ob_hnsw_ef_search, real_limit, search_param_);
      }
      query_cond.query_limit_ = real_limit;
    }

    ObDatum *vec_datum = NULL;
    LOG_DEBUG("print_vector_query_condition", K(query_cond.query_limit_), K(query_cond.ef_search_));
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
    } else {
      query_cond.extra_column_count_ = extra_column_count_;
    }
    LOG_TRACE("vector index show basic hnsw query cond", K(query_cond.only_complete_data_), K(query_cond.ef_search_), K(query_cond.query_limit_),
                                            K(query_cond.extra_column_count_), K(query_cond.query_vector_));
  }
  return ret;
}

int ObDASHNSWScanIter::get_extra_idx_in_outexprs(ObIArray<int64_t> &extra_in_rowkey_idxs)
{
  int ret = OB_SUCCESS;
  // extra_info column is sorted by column_id
  const ObDASScanCtDef *ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx(),
                                                                      ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
  const sql::ExprFixedArray &rowkey_exprs = ctdef->rowkey_exprs_;
  const sql::ExprFixedArray &out_exprs = vec_aux_ctdef_->result_output_;
  if (out_exprs.count() - (1 + vec_aux_ctdef_->relevance_col_cnt_) != rowkey_exprs.count() || rowkey_exprs.count() != extra_column_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey count is not equal.", K(ret), K(rowkey_exprs.count()), K(out_exprs.count()), K(vec_aux_ctdef_->relevance_col_cnt_));
  }
  // out_exprs extra_info is begin with 1, the 0 is vid
  for (int64_t i = 1; OB_SUCC(ret) && i <= extra_column_count_; ++i) {
    const ObExpr *extra_info_expr = out_exprs.at(i);
    bool find = false;
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < rowkey_exprs.count(); ++j) {
      const ObExpr *rowkey_expr = rowkey_exprs.at(j);
      if (rowkey_expr->frame_idx_ == extra_info_expr->frame_idx_ &&
          rowkey_expr->datum_off_ == extra_info_expr->datum_off_) {
        if (OB_FAIL(extra_in_rowkey_idxs.push_back(j))) {
          LOG_WARN("push failed", K(ret));
        } else {
          find = true;
        }
      }
    }
  }
  return ret;
}


int ObDASHNSWScanIter::prepare_extra_objs(ObIAllocator &allocator, ObObj *&objs)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (extra_column_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra column count is 0.", K(ret), K(extra_column_count_));
  } else if (extra_in_rowkey_idxs_.count() != extra_column_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra info column count is not equal.", K(ret), K(extra_column_count_),
             K(extra_in_rowkey_idxs_.count()));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * extra_column_count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(extra_column_count_));
  } else if (OB_FALSE_IT(objs = new (buf) ObObj[extra_column_count_])) {
  }

  return ret;
}

int ObDASHNSWScanIter::build_extra_info_obj_from_rowkey(const ObObj *rowkey_objs, ObObj *&extra_info_objs)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(rowkey_objs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey_objs is null.", K(ret));
  } else if (OB_FAIL(prepare_extra_objs(mem_context_->get_arena_allocator(), extra_info_objs))) {
    LOG_WARN("prepare extra info obj failed", K(ret));
  } else {
    int64_t rowkey_idx = 0;
    for (int64_t i = 0; i < extra_column_count_ && OB_SUCC(ret); ++i) {
      rowkey_idx = extra_in_rowkey_idxs_.at(i);
      if (OB_UNLIKELY(OB_ISNULL(rowkey_objs + rowkey_idx) || rowkey_idx >= extra_column_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey_obj is null, or rowkey_idx invalid", K(ret), K(i), K(rowkey_idx), K(extra_column_count_));
      } else {
        extra_info_objs[i] = rowkey_objs[rowkey_idx];
      }
    }
  }

  return ret;
}

int ObDASHNSWScanIter::build_rowkey_obj_from_extra_info(ObObj *extra_info_objs, ObObj *&rowkey_objs)
{
  int ret = OB_SUCCESS;

  void *buf = nullptr;
  if (OB_ISNULL(extra_info_objs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("extra info obj is null.", K(ret));
  } else if (extra_column_count_ == 1) {
    rowkey_objs = extra_info_objs;
  } else if (OB_FAIL(prepare_extra_objs(mem_context_->get_arena_allocator(), rowkey_objs))) {
    LOG_WARN("prepare extra info obj failed", K(ret));
  } else {
    int64_t rowkey_idx = 0;
    for (int64_t i = 0; i < extra_column_count_ && OB_SUCC(ret); ++i) {
      rowkey_idx = extra_in_rowkey_idxs_.at(i);
      if (OB_UNLIKELY(OB_ISNULL(extra_info_objs + i) || rowkey_idx >= extra_column_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_info_obj is null, or rowkey_idx invalid", K(ret), K(i), K(rowkey_idx), K(extra_column_count_));
      } else {
        rowkey_objs[rowkey_idx] = extra_info_objs[i];
      }
    }
  }

  return ret;
}

int ObDASHNSWScanIter::prepare_follower_query_cond(ObVectorQueryConditions &query_cond)
{
  int ret = OB_SUCCESS;
  /* snapshot_scan_param_.tablet_id_ will update in do_snapshot_table_scan
   * so snapshot_scan_param_.tablet_id_ != snapshot_tablet_id_ means current query_cond_.row_iter_ is not current snapshot_tablet_id_ iter,
   * that we should refresh */
  if ((query_cond_.row_iter_ != nullptr && snapshot_scan_param_.tablet_id_ != snapshot_tablet_id_)
      || OB_ISNULL(query_cond_.row_iter_)) {
    if (OB_FAIL(do_snapshot_table_scan())) {
      LOG_WARN("fail to do snapshot table scan", K(ret));
    } else if (OB_NOT_NULL(snapshot_iter_)) {
      query_cond_.row_iter_ = snapshot_iter_->get_output_result_iter();
    }
  }
  return ret;
}

int ObDASHNSWScanIter::prepare_complete_vector_data(ObVectorQueryAdaptorResultContext& ada_ctx)
{
  int ret = OB_SUCCESS;

  ObObj *vids = nullptr;
  ObSEArray<int64_t, 4> extra_in_rowkey_idxs;
  int64_t vec_cnt = ada_ctx.get_vec_cnt();

  if (OB_ISNULL(vids = ada_ctx.get_vids())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get vectors.", K(ret));
  } else if (extra_column_count_ > 0 && extra_in_rowkey_idxs_.empty()) {
    // get extra info column index in output_exprs
    LOG_WARN("failed to get extra idx in output exprs", K(ret), K(extra_column_count_));
  }

  for (int i = 0; OB_SUCC(ret) && i < vec_cnt; i++) {
    ObRowkey vid(&(vids[i + ada_ctx.get_curr_idx()]), 1);
    ObRowkey *rowkey;
    ObString vector;
    if (use_vid_) {
      if (OB_FAIL(get_rowkey_from_vid_rowkey_table(mem_context_->get_arena_allocator(), vid, rowkey))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get rowkey.", K(ret));
        }
      }
    } else {
      vid.get_obj_ptr()->meta_.set_uint64();
      rowkey = &vid;
    }

    if (OB_FAIL(ret)) {
      if (OB_ITER_END == ret) {
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
      if (extra_column_count_ > 0) {
        // Note: extra_colunm is rowkey + partition_key, rowkey must must include all partition columns
        if (OB_FAIL(ada_ctx.set_extra_info(i, *rowkey, extra_in_rowkey_idxs_))) {
          LOG_WARN("failed to set_extra_info.", K(ret), K(i), KP(rowkey), K(extra_in_rowkey_idxs_));
        }
      }
    }
  }

  LOG_INFO("SYCN_DELTA_query_data", K(ada_ctx.get_vec_cnt()), K(ada_ctx.get_curr_idx()));

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObDASHNSWScanIter::get_vid_from_rowkey_vid_table(int64_t &vid)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(rowkey_vid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey vid iter is null.", K(ret));
  } else {
    rowkey_vid_iter_->clear_evaluated_flag();
    if (OB_FAIL(rowkey_vid_iter_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to scan rowkey vid iter", K(ret));
      }
    } else {
      ObDASScanRtDef *rowkey_vid_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx());
      ObExpr *vid_expr = vec_aux_ctdef_->inv_scan_vec_id_col_;
      ObDatum &vid_datum = vid_expr->locate_expr_datum(*rowkey_vid_rtdef->eval_ctx_);
      vid = vid_datum.get_int();
    }
  }

  return ret;
}

int ObDASHNSWScanIter::get_rowkey_from_vid_rowkey_table(ObIAllocator &allocator, ObRowkey& vid, ObRowkey *&rowkey)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(vid, vid_rowkey_scan_param_, vid_rowkey_ctdef_->ref_table_id_))) {
    LOG_WARN("failed to set vid.", K(ret));
  } else if (OB_FAIL(do_vid_rowkey_table_scan())) {
    LOG_WARN("failed to do vid rowkey scan", K(ret));
  } else if (OB_FALSE_IT(vid_rowkey_iter_->clear_evaluated_flag())) {
  } else if (OB_FAIL(vid_rowkey_iter_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to scan vid rowkey iter", K(vid), K(ret));
    }
  } else if (OB_FAIL(get_from_vid_rowkey(allocator, rowkey))) {
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

int ObDASHNSWScanIter::get_vector_from_com_aux_vec_table(ObIAllocator &allocator, ObRowkey *rowkey, ObString &vector)
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *com_aux_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_com_aux_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);

  if (!com_aux_vec_iter_first_scan_ && OB_FAIL(reuse_com_aux_vec_iter())) {
    LOG_WARN("failed to reuse com aux vec iter.", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(*rowkey, com_aux_vec_scan_param_, com_aux_vec_ctdef->ref_table_id_))) {
    LOG_WARN("failed to set lookup key", K(ret));
  } else if (OB_FAIL(do_com_aux_vec_table_scan())) {
    LOG_WARN("failed to do com aux vec table scan", K(ret));
  } else if (OB_FAIL(get_vector_from_com_aux_vec_table(allocator, vector))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get vector from com aux vec table", K(ret));
    }
  }

  return ret;
}

int ObDASHNSWScanIter::get_vector_from_com_aux_vec_table(ObIAllocator &allocator, ObString &vector)
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *com_aux_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_com_aux_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
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

int ObDASHNSWScanIter::do_vid_rowkey_table_scan()
{
  return do_aux_table_scan(vid_rowkey_iter_first_scan_,
                           vid_rowkey_scan_param_,
                           vid_rowkey_ctdef_,
                           vid_rowkey_rtdef_,
                           vid_rowkey_iter_,
                           vid_rowkey_tablet_id_);
}

int ObDASHNSWScanIter::do_rowkey_vid_table_scan()
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *rowkey_vid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
  ObDASScanRtDef *rowkey_vid_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_rowkey_vid_tbl_idx());
  if (OB_ISNULL(rowkey_vid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey vid iter is null.", K(ret));
  } else if (rowkey_vid_iter_first_scan_) {
    rowkey_vid_scan_param_.need_switch_param_ = false;
    ObDASScanIter *inv_idx_scan_iter = static_cast<ObDASScanIter *>(inv_idx_scan_iter_);
    if (!is_primary_pre_with_rowkey_with_filter_ &&
        OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_, rowkey_vid_tablet_id_, rowkey_vid_ctdef, rowkey_vid_rtdef,
                                                  tx_desc_, snapshot_, rowkey_vid_scan_param_))) {
      LOG_WARN("failed to init scan param", K(ret));
    } else if (is_primary_pre_with_rowkey_with_filter_ &&
        OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_, rowkey_vid_tablet_id_, rowkey_vid_ctdef, rowkey_vid_rtdef,
                                                  tx_desc_, snapshot_, rowkey_vid_scan_param_,
                                                  inv_idx_scan_iter->get_scan_param().is_get_))) {
      LOG_WARN("failed to init scan param", K(ret));
    } else if (OB_FAIL(rowkey_vid_iter_->do_table_scan())) {
      LOG_WARN("failed to do table scan", K(ret));
    } else {
      rowkey_vid_iter_first_scan_ = false;
    }
  } else {
    if (OB_FAIL(rowkey_vid_iter_->rescan())) {
      LOG_WARN("fail to rescan scan iterator.", K(ret));
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
    LOG_WARN("allocate memory failed", K(ret), K(capacity_));
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

void ObSimpleMaxHeap::push(int64_t vid, double distiance, bool is_snap)
{
  ObSortItem item(vid, distiance, is_snap);

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

int64_t ObSimpleMaxHeap::at(uint64_t idx) const
{
  int64_t vid = 0;
  if (idx < size_) {
    vid = heap_[idx].vid_;
  }
  return vid;
}

double ObSimpleMaxHeap::value_at(uint64_t idx) const
{
  double value = 0.0;
  if (idx < size_) {
    value = heap_[idx].distance_;
  }
  return value;
}

bool ObSimpleMaxHeap::is_snap(uint64_t idx) const
{
  bool is_snap = false;
  if (idx < size_) {
    is_snap = heap_[idx].is_snap_;
  }
  return is_snap;
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
