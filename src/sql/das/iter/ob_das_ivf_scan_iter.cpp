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
#include "sql/das/iter/ob_das_ivf_scan_iter.h"
#include "sql/das/ob_das_scan_op.h"
#include "storage/tx_storage/ob_access_service.h"
#include "src/storage/access/ob_table_scan_iterator.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "sql/engine/expr/ob_expr_vec_ivf_sq8_data_vector.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "share/ob_vec_index_builder_util.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "lib/roaringbitmap/ob_rb_memory_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace transaction;
using namespace share;
using namespace sql;

namespace sql
{

int ObDASIvfBaseScanIter::do_table_scan()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(inv_idx_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inv_idx_scan_iter_ is null", K(ret));
  } else if (OB_FAIL(inv_idx_scan_iter_->do_table_scan())) {
    LOG_WARN("fail to do inv idx table scan.", K(ret));
  }

  return ret;
}

int ObDASIvfBaseScanIter::rescan()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->rescan())) {
    LOG_WARN("failed to rescan inv_idx_scan_iter_", K(ret));
  }

  return ret;
}

void ObDASIvfBaseScanIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(inv_idx_scan_iter_)) {
    inv_idx_scan_iter_->clear_evaluated_flag();
  }
}

int ObDASIvfBaseScanIter::gen_rowkeys_itr()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(saved_rowkeys_itr_) && saved_rowkeys_itr_->is_init()) {
  } else {
    uint64_t rowkey_count = saved_rowkeys_.count();
    void *iter_buff = nullptr;
    if (rowkey_count == 0) {
      ret = OB_ITER_END;
      LOG_WARN("no rowkeys found", K(ret));
    } else if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryRowkeyIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocator rowkey iter.", K(ret));
    } else if (OB_FALSE_IT(saved_rowkeys_itr_ = new (iter_buff) ObVectorQueryRowkeyIterator())) {
    } else if (OB_FAIL(saved_rowkeys_itr_->init(rowkey_count, &saved_rowkeys_))) {
      LOG_WARN("iter init failed.", K(ret));
    }
  }

  return ret;
}

int ObDASIvfBaseScanIter::do_table_full_scan(bool is_vectorized,
                                             const ObDASScanCtDef *ctdef,
                                             ObDASScanRtDef *rtdef,
                                             ObDASScanIter *iter,
                                             int64_t pri_key_cnt,
                                             ObTabletID &tablet_id,
                                             bool &first_scan,
                                             ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  if (first_scan) {
    ObNewRange scan_range;
    if (OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_, tablet_id, ctdef, rtdef, tx_desc_, snapshot_, scan_param,
                                                   false /*is_get*/, &mem_context_->get_arena_allocator()))) {
      LOG_WARN("failed to generate init vec aux scan param", K(ret));
    } else if (OB_FALSE_IT(ObDasVecScanUtils::set_whole_range(scan_range, ctdef->ref_table_id_))) {
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to append scan range", K(ret));
    } else if (OB_FALSE_IT(iter->set_scan_param(scan_param))) {
    } else if (OB_FAIL(iter->do_table_scan())) {
      LOG_WARN("failed to do scan", K(ret));
    } else {
      first_scan = false;
    }
  } else {
    const ObTabletID &scan_tablet_id = scan_param.tablet_id_;
    scan_param.need_switch_param_ =
        scan_param.need_switch_param_ || (scan_tablet_id.is_valid() && (tablet_id != scan_tablet_id));
    scan_param.tablet_id_ = tablet_id;
    scan_param.ls_id_ = ls_id_;

    ObNewRange scan_range;
    if (OB_FAIL(iter->reuse())) {
      LOG_WARN("failed to reuse scan iterator.", K(ret));
    } else if (OB_FALSE_IT(ObDasVecScanUtils::set_whole_range(scan_range, ctdef->ref_table_id_))) {
    } else if (OB_FAIL(scan_param.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to append scan range", K(ret));
    } else if (OB_FAIL(iter->rescan())) {
      LOG_WARN("failed to rescan scan iterator.", K(ret));
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::do_aux_table_scan(bool &first_scan,
                                            ObTableScanParam &scan_param,
                                            const ObDASScanCtDef *ctdef,
                                            ObDASScanRtDef *rtdef,
                                            ObDASScanIter *iter,
                                            ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  if (first_scan) {
    scan_param.need_switch_param_ = false;
    if (OB_FAIL(ObDasVecScanUtils::init_scan_param(
            ls_id_, tablet_id, ctdef, rtdef, tx_desc_, snapshot_, scan_param, false/*is_get*/, &mem_context_->get_arena_allocator()))) {
      LOG_WARN("failed to init scan param", K(ret));
    } else if (OB_FALSE_IT(iter->set_scan_param(scan_param))) {
    } else if (OB_FAIL(iter->do_table_scan())) {
      LOG_WARN("failed to do scan", K(ret));
    } else {
      first_scan = false;
    }
  } else {
    const ObTabletID &scan_tablet_id = scan_param.tablet_id_;
    scan_param.need_switch_param_ =
        scan_param.need_switch_param_ || (scan_tablet_id.is_valid() && (tablet_id != scan_tablet_id));
    scan_param.tablet_id_ = tablet_id;
    scan_param.ls_id_ = ls_id_;
    if (OB_FAIL(iter->rescan())) {
      LOG_WARN("fail to rescan scan iterator.", K(ret));
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->reuse())) {
    LOG_WARN("failed to reuse inv idx scan iter", K(ret));
  } else if (!centroid_iter_first_scan_ && OB_FAIL(ObDasVecScanUtils::reuse_iter(
                                               ls_id_, centroid_iter_, centroid_scan_param_, centroid_tablet_id_))) {
    LOG_WARN("failed to reuse com aux vec iter", K(ret));
  } else if (!cid_vec_iter_first_scan_ &&
             OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_))) {
    LOG_WARN("failed to reuse rowkey vid iter", K(ret));
  } else if (!rowkey_cid_iter_first_scan_ &&
             OB_FAIL(ObDasVecScanUtils::reuse_iter(
                 ls_id_, rowkey_cid_iter_, rowkey_cid_scan_param_, rowkey_cid_tablet_id_))) {
    LOG_WARN("failed to reuse vid rowkey iter", K(ret));
  } else if (!brute_first_scan_ && OB_FAIL(ObDasVecScanUtils::reuse_iter(
                                      ls_id_, brute_iter_, brute_scan_param_, brute_tablet_id_))) {
    LOG_WARN("failed to reuse iter", K(ret));
  } else if (!data_filter_iter_first_scan_ && OB_FAIL(ObDasVecScanUtils::reuse_iter(
                                      ls_id_, data_filter_iter_, data_filter_scan_param_, data_filter_tablet_id_))) {
    LOG_WARN("failed to reuse iter", K(ret));
  } 

  if (OB_NOT_NULL(saved_rowkeys_itr_)) {
    saved_rowkeys_itr_->reset();
    saved_rowkeys_itr_->~ObVectorQueryRowkeyIterator();
    saved_rowkeys_itr_ = nullptr;
  }
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }

  vec_op_alloc_.reset();
  saved_rowkeys_.reset();
  pre_fileter_rowkeys_.reset();
  center_cache_guard_.reset();
  iterative_filter_ctx_.reuse();
  // adaptive_ctx_.reset();
  return ret;
}

int ObDASIvfBaseScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ObDASIterType::DAS_ITER_IVF_SCAN != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for ivf scan iter", K(ret), K(param));
  } else {
    ObDASIvfScanIterParam &ivf_scan_param = static_cast<ObDASIvfScanIterParam &>(param);
    ls_id_ = ivf_scan_param.ls_id_;
    tx_desc_ = ivf_scan_param.tx_desc_;
    snapshot_ = ivf_scan_param.snapshot_;

    inv_idx_scan_iter_ = ivf_scan_param.inv_idx_scan_iter_;
    centroid_iter_ = ivf_scan_param.centroid_iter_;
    cid_vec_iter_ = ivf_scan_param.cid_vec_iter_;
    rowkey_cid_iter_ = ivf_scan_param.rowkey_cid_iter_;
    brute_iter_ = ivf_scan_param.brute_iter_;
    data_filter_iter_ = ivf_scan_param.data_filter_iter_;

    vec_aux_ctdef_ = ivf_scan_param.vec_aux_ctdef_;
    vec_aux_rtdef_ = ivf_scan_param.vec_aux_rtdef_;
    sort_ctdef_ = ivf_scan_param.sort_ctdef_;
    sort_rtdef_ = ivf_scan_param.sort_rtdef_;
    data_filter_ctdef_ = ivf_scan_param.data_filter_ctdef_;
    data_filter_rtdef_ = ivf_scan_param.data_filter_rtdef_;

    vec_index_type_ = ivf_scan_param.vec_index_type_;
    vec_idx_try_path_ = ivf_scan_param.vec_idx_try_path_;

    adaptive_ctx_.selectivity_ = vec_aux_ctdef_->selectivity_;
    adaptive_ctx_.row_count_ = vec_aux_ctdef_->row_count_;
    adaptive_ctx_.is_primary_index_ = ivf_scan_param.is_primary_index_;
    adaptive_ctx_.can_use_vec_pri_opt_ = vec_aux_ctdef_->can_use_vec_pri_opt();

    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "IVF", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create vector ivf memory context", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      if (OB_NOT_NULL(sort_ctdef_) && OB_NOT_NULL(sort_rtdef_)) {
        ObExpr *distance_calc = nullptr;
        if (OB_FAIL(
                ObDasVecScanUtils::init_limit(vec_aux_ctdef_, vec_aux_rtdef_, sort_ctdef_, sort_rtdef_, limit_param_))) {
          LOG_WARN("failed to init limit", K(ret), KPC(vec_aux_ctdef_), KPC(vec_aux_rtdef_));
        } else if (OB_FAIL(ObDasVecScanUtils::init_sort(
                      vec_aux_ctdef_, vec_aux_rtdef_, sort_ctdef_, sort_rtdef_, limit_param_, search_vec_, distance_calc))) {
          LOG_WARN("failed to init sort", K(ret), KPC(vec_aux_ctdef_), KPC(vec_aux_rtdef_));
        } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
                       vec_aux_ctdef_->vec_index_param_, ObVectorIndexType::VIT_IVF_INDEX, vec_index_param_))) {
          LOG_WARN("fail to parse params from string", K(ret), K(vec_aux_ctdef_->vec_index_param_));
        } else if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(persist_alloc_, sort_rtdef_, search_vec_,
                                                                  real_search_vec_))) {
          LOG_WARN("failed to get real search vec", K(ret));
        } else if (OB_FAIL(ObDasVecScanUtils::get_distance_expr_type(*sort_ctdef_->sort_exprs_[0],
                                                                     *sort_rtdef_->eval_ctx_, dis_type_))) {
          LOG_WARN("failed to get distance type.", K(ret));
        } else if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::COSINE) {
          // nomolize serach_vec
          if (OB_FAIL(ObVectorNormalize::L2_normalize_vector(vec_aux_ctdef_->dim_,
                                                             reinterpret_cast<float *>(real_search_vec_.ptr()),
                                                             reinterpret_cast<float *>(real_search_vec_.ptr())))) {
            LOG_WARN("failed to normalize vector", K(ret));
          } else {
            need_norm_ = true;
            dis_type_ = oceanbase::sql::ObExprVectorDistance::ObVecDisType::DOT;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObVectorIndexParam::build_search_param(vec_aux_ctdef_->vector_index_param_, vec_aux_ctdef_->vec_query_param_, search_param_))) {
            LOG_WARN("build search param fail", K(vec_aux_ctdef_->vector_index_param_), K(vec_aux_ctdef_->vec_query_param_));
          } else {
            LOG_TRACE("search param", K(vec_aux_ctdef_->vector_index_param_), K(vec_aux_ctdef_->vec_query_param_), K(search_param_));
            if (search_param_.similarity_threshold_ > 0) {
              if (OB_FAIL(ObDasVecScanUtils::check_ivf_support_similarity_threshold(*sort_ctdef_->sort_exprs_[0]))) {
                LOG_WARN("check support similarity threshold fail", K(ret));
              } else {
                similarity_threshold_ = search_param_.similarity_threshold_;
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else {
          ObSQLSessionInfo *session = nullptr;
          uint64_t ob_ivf_nprobes = 0;
          
          if (OB_NOT_NULL(vec_aux_ctdef_) && vec_aux_ctdef_->vec_query_param_.is_set_ivf_nprobes_) {
            nprobes_ = vec_aux_ctdef_->vec_query_param_.ivf_nprobes_;
            LOG_TRACE("use stmt ivf_nprobes", K(nprobes_));
          } else if (OB_ISNULL(session = sort_rtdef_->eval_ctx_->exec_ctx_.get_my_session())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get session", K(ret), KPC(session));
          } else if (OB_FAIL(session->get_ob_ivf_nprobes(ob_ivf_nprobes))) {
            LOG_WARN("failed to get ob ob_ivf_nprobes", K(ret));
          } else {
            nprobes_ = ob_ivf_nprobes;
          }
        }
      }
    }

    if (OB_SUCC(ret) && is_adaptive_filter()) {
      ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(*exec_ctx_);
      ObVecIdxAdaTryPath cur_path = ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN;
      if (OB_ISNULL(plan_ctx->get_phy_plan())) {
        // remote scan, phy plan is null, do nothing, just use try path in ctdef
        LOG_WARN("plan ctx is null", K(ret), KP(plan_ctx));
      } else if (OB_FALSE_IT(cur_path = static_cast<ObVecIdxAdaTryPath>(plan_ctx->get_phy_plan()->stat_.vec_index_exec_ctx_.cur_path_))) {
      } else if (cur_path != vec_idx_try_path_ && 
                  cur_path > ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN &&
                  cur_path < ObVecIdxAdaTryPath::VEC_PATH_MAX) {
        LOG_INFO("adaptive filter change path", K(cur_path), K(vec_idx_try_path_));
        vec_idx_try_path_ = cur_path;
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(data_filter_ctdef_)) {
      int64_t main_rowkey_cnt = data_filter_ctdef_->table_param_.get_read_info().get_schema_rowkey_count();
      void *ptr = nullptr;
      if (OB_ISNULL(ptr = persist_alloc_.alloc(sizeof(ObObj) * main_rowkey_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(main_rowkey_cnt));
      } else {
        ObObj *obj_ptr = new (ptr) ObObj[main_rowkey_cnt];
        tmp_main_rowkey_.assign(obj_ptr, main_rowkey_cnt);
      }
    }
  }

    dim_ = vec_aux_ctdef_->dim_;
    selectivity_ = vec_aux_ctdef_->selectivity_;

  return ret;
}

int ObDASIvfBaseScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_NOT_NULL(inv_idx_scan_iter_) && OB_FAIL(inv_idx_scan_iter_->release())) {
    LOG_WARN("failed to release inv_idx_scan_iter_", K(ret));
    tmp_ret = ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(centroid_iter_) && OB_FAIL(centroid_iter_->release())) {
    LOG_WARN("failed to release centroid_iter_", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(cid_vec_iter_) && OB_FAIL(cid_vec_iter_->release())) {
    LOG_WARN("failed to release cid_vec_iter_", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(rowkey_cid_iter_) && OB_FAIL(rowkey_cid_iter_->release())) {
    LOG_WARN("failed to release rowkey_cid_iter_", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(brute_iter_) && OB_FAIL(brute_iter_->release())) {
    LOG_WARN("failed to release brute_iter_", K(ret));
  }
  if (OB_NOT_NULL(data_filter_iter_) && OB_FAIL(data_filter_iter_->release())) {
    LOG_WARN("failed to release data_filter_iter_", K(ret));
  }

  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  inv_idx_scan_iter_ = nullptr;
  centroid_iter_ = nullptr;
  cid_vec_iter_ = nullptr;
  rowkey_cid_iter_ = nullptr;
  brute_iter_ = nullptr;
  data_filter_iter_ = nullptr;

  if (OB_NOT_NULL(saved_rowkeys_itr_)) {
    saved_rowkeys_itr_->reset();
    saved_rowkeys_itr_->~ObVectorQueryRowkeyIterator();
    saved_rowkeys_itr_ = nullptr;
  }

  saved_rowkeys_.reset();
  pre_fileter_rowkeys_.reset();
  if (nullptr != mem_context_)  {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  tmp_main_rowkey_.reset();
  vec_op_alloc_.reset();
  persist_alloc_.reset();
  center_cache_guard_.reset();
  iterative_filter_ctx_.reset();
  adaptive_ctx_.reset();
  tx_desc_ = nullptr;
  snapshot_ = nullptr;

  ObDasVecScanUtils::release_scan_param(centroid_scan_param_);
  ObDasVecScanUtils::release_scan_param(cid_vec_scan_param_);
  ObDasVecScanUtils::release_scan_param(rowkey_cid_scan_param_);
  ObDasVecScanUtils::release_scan_param(brute_scan_param_);
  ObDasVecScanUtils::release_scan_param(data_filter_scan_param_);

  vec_aux_ctdef_ = nullptr;
  vec_aux_rtdef_ = nullptr;
  sort_ctdef_ = nullptr;
  sort_rtdef_ = nullptr;
  search_vec_ = nullptr;
  real_search_vec_ = nullptr;
  data_filter_ctdef_ = nullptr;
  data_filter_rtdef_ = nullptr;
  return ret;
}

int ObDASIvfBaseScanIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    if (OB_FAIL(process_ivf_scan(false/*is_vectorized*/))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to process ivf scan state", K(ret));
      }
    }
  }

  ObRowkey *rowkey = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rowkey iter", K(ret));
  } else if (OB_FAIL(get_next_saved_rowkey())) {
    LOG_WARN("failed to get saved rowkey", K(ret));
  }

  return ret;
}

int ObDASIvfBaseScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    if (OB_FAIL(process_ivf_scan(true/*is_vectorized*/))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to process ivf scan state", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rowkey iter", K(ret));
  } else if (OB_FALSE_IT(saved_rowkeys_itr_->set_batch_size(capacity))) {
  } else if (OB_FAIL(get_next_saved_rowkeys(count))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get saved rowkeys", K(ret));
    }
  }

  return ret;
}

int ObDASIvfBaseScanIter::do_ivf_scan(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  if (is_post_filter()) {
    if (OB_FAIL(process_ivf_scan_post(is_vectorized))) {
      LOG_WARN("failed to process ivf_scan post filter", K(ret), K_(pre_fileter_rowkeys), K_(saved_rowkeys));
    }
  } else if (OB_FAIL(process_ivf_scan_pre(mem_context_->get_arena_allocator(), is_vectorized))) {
    LOG_WARN("failed to process ivf_scan pre", K(ret));
  }
  return ret;
}

int ObDASIvfBaseScanIter::process_ivf_scan(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  can_retry_ = check_if_can_retry();
  if (OB_FAIL(do_ivf_scan(is_vectorized))) {
    LOG_WARN("failed to process ivf_scan pre", K(ret));
  }

  if (OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY == ret && can_retry_) {
    LOG_INFO("index adaptive scan need retry", K(vec_index_type_), K(vec_idx_try_path_), K(adaptive_ctx_));
    if (OB_FAIL(reset_filter_path())) {
      LOG_WARN("failed to reset filter path", K(vec_index_type_), K(vec_idx_try_path_), K(adaptive_ctx_), K(ret));
    } else if (OB_FAIL(do_ivf_scan(is_vectorized))) {
      LOG_WARN("failed to process ivf_scan pre", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("ivf scan stat info", K_(vec_index_type), K_(vec_idx_try_path), K(adaptive_ctx_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(gen_rowkeys_itr())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to gen rowkeys itr", K(saved_rowkeys_));
    }
  }
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }

  return ret;
}

int ObDASIvfBaseScanIter::check_iter_filter_need_retry()
{
  int ret = OB_SUCCESS;
  double iter_selectivity = double(adaptive_ctx_.iter_res_row_cnt_) /  double(adaptive_ctx_.iter_filter_row_cnt_);
  double output_row_cnt = iter_selectivity * adaptive_ctx_.row_count_;
  if (adaptive_ctx_.iter_times_ > 2) {
    if (adaptive_ctx_.can_use_vec_pri_opt_) {
      ret = (iter_selectivity <= ObVecIdxExtraInfo::DEFAULT_IVF_PRE_RATE_FILTER_WITH_ROWKEY) ?
            OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY : OB_SUCCESS;
    } else {
      ret = (output_row_cnt < ObVecIdxExtraInfo::MAX_HNSW_PRE_ROW_CNT_WITH_IDX
            && iter_selectivity <= ObVecIdxExtraInfo::DEFAULT_IVF_PRE_RATE_FILTER_WITH_IDX) ?
            OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY : OB_SUCCESS;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("switch path check iter filter need retry", K(ret), K(adaptive_ctx_), K(iter_selectivity), K(output_row_cnt));
  }
  return ret;
}

int ObDASIvfBaseScanIter::check_pre_filter_need_retry()
{
  int ret = OB_SUCCESS;
  double pre_selectivity = double(adaptive_ctx_.pre_scan_row_cnt_) / double(adaptive_ctx_.row_count_);
  if (adaptive_ctx_.can_use_vec_pri_opt_) pre_selectivity =  double(adaptive_ctx_.vec_dist_calc_cnt_) / double(adaptive_ctx_.cid_vec_scan_rows_);
  if (adaptive_ctx_.pre_scan_row_cnt_ < IVF_MAX_BRUTE_FORCE_SIZE) {
    /*do nothing*/
  } else if (adaptive_ctx_.can_use_vec_pri_opt_) {
    ret = pre_selectivity > ObVecIdxExtraInfo::DEFAULT_IVF_PRE_RATE_FILTER_WITH_ROWKEY ?
      OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY : OB_SUCCESS;
  } else if (pre_selectivity > ObVecIdxExtraInfo::DEFAULT_IVF_PRE_RATE_FILTER_WITH_IDX) {
    ret = OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("switch path check pre filter need retry", K(ret), K(adaptive_ctx_), K(pre_selectivity));
  }
  return ret;
}

int ObDASIvfBaseScanIter::reset_filter_path()
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
    can_retry_ = false;
  }
  return ret;
}

int ObDASIvfBaseScanIter::updata_vec_exec_ctx(ObPlanStat* plan_stat)
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
      }
      ATOMIC_INC(&(plan_stat->vec_index_exec_ctx_.record_count_));
    } else {
      double iter_time = plan_stat->vec_index_exec_ctx_.iter_filter_chosen_times_;
      double pre_time = plan_stat->vec_index_exec_ctx_.pre_filter_chosen_times_;
      iter_time = std::log(iter_time) * DECAY_FACTOR;
      pre_time = std::log(pre_time) * DECAY_FACTOR;
      FLOG_INFO("begin to reset plan stat filter path", K(plan_stat->vec_index_exec_ctx_.record_count_), 
      K(plan_stat->vec_index_exec_ctx_.cur_path_), K(vec_idx_try_path_), K(adaptive_ctx_), K(ret));
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.record_count_), 0);
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.iter_filter_chosen_times_), static_cast<int64_t>(iter_time));
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.pre_filter_chosen_times_), static_cast<int64_t>(pre_time));
      ATOMIC_STORE(&(plan_stat->vec_index_exec_ctx_.cur_path_), static_cast<uint8_t>(vec_idx_try_path_));
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::build_cid_vec_query_rowkey(const ObString &cid,
                                                     bool is_min,
                                                     int64_t rowkey_cnt,
                                                     common::ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;

  ObObj *obj_ptr = nullptr;
  if (OB_ISNULL(obj_ptr = static_cast<ObObj *>(mem_context_->get_arena_allocator().alloc(sizeof(ObObj) * rowkey_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    // rowkey: [cid, rowkey]
    obj_ptr[0].set_varbinary(cid);
    for (int64_t i = 1; i < rowkey_cnt; ++i) {
      if (is_min) {
        obj_ptr[i].set_min_value();
      } else {
        obj_ptr[i].set_max_value();
      }
    }

    rowkey.assign(obj_ptr, rowkey_cnt);
  }
  return ret;
}

int ObDASIvfBaseScanIter::build_cid_vec_query_range(const ObString &cid,
                                                    int64_t rowkey_cnt,
                                                    ObNewRange &cid_pri_key_range)
{
  int ret = OB_SUCCESS;
  ObRowkey cid_rowkey_min;
  ObRowkey cid_rowkey_max;
  if (cid.empty()) {
    cid_pri_key_range.set_whole_range();
  } else {
    if (OB_FAIL(build_cid_vec_query_rowkey(cid, true, rowkey_cnt, cid_rowkey_min))) {
      LOG_WARN("failed to build cid vec query rowkey", K(ret));
    } else if (OB_FAIL(build_cid_vec_query_rowkey(cid, false, rowkey_cnt, cid_rowkey_max))) {
      LOG_WARN("failed to build cid vec query rowkey", K(ret));
    } else {
      cid_pri_key_range.start_key_ = cid_rowkey_min;
      cid_pri_key_range.end_key_ = cid_rowkey_max;
    }
  }

  return ret;
}

int ObDASIvfBaseScanIter::do_rowkey_cid_table_scan()
{
  int ret = OB_SUCCESS;

  if (rowkey_cid_iter_first_scan_) {
    const ObDASScanCtDef *rowkey_cid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
        vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
    ObDASScanRtDef *rowkey_cid_rtdef =
        vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx());
    if (OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_,
                                                   rowkey_cid_tablet_id_,
                                                   rowkey_cid_ctdef,
                                                   rowkey_cid_rtdef,
                                                   tx_desc_,
                                                   snapshot_,
                                                   rowkey_cid_scan_param_,
                                                   true,
                                                   &mem_context_->get_arena_allocator()))) {
      LOG_WARN("failed to init rowkey cid vec lookup scan param", K(ret));
    } else if (OB_FALSE_IT(rowkey_cid_iter_->set_scan_param(rowkey_cid_scan_param_))) {
    } else if (OB_FAIL(rowkey_cid_iter_->do_table_scan())) {
      LOG_WARN("fail to do rowkey cid vec table scan.", K(ret));
    } else {
      rowkey_cid_iter_first_scan_ = false;
    }
  } else {
    const ObTabletID &scan_tablet_id = rowkey_cid_scan_param_.tablet_id_;
    rowkey_cid_scan_param_.need_switch_param_ = 
      rowkey_cid_scan_param_.need_switch_param_ || (scan_tablet_id.is_valid() && (rowkey_cid_tablet_id_ != scan_tablet_id));
    rowkey_cid_scan_param_.tablet_id_ = rowkey_cid_tablet_id_;
    rowkey_cid_scan_param_.ls_id_ = ls_id_;

    if (OB_FAIL(rowkey_cid_iter_->rescan())) {
      LOG_WARN("fail to rescan cid vec table scan iterator.", K(ret));
    }
  }

  return ret;
}

void ObDASIvfBaseScanIter::set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids)
{
  centroid_tablet_id_ = related_tablet_ids.centroid_tablet_id_;
  cid_vec_tablet_id_ = related_tablet_ids.cid_vec_tablet_id_;
  rowkey_cid_tablet_id_ = related_tablet_ids.rowkey_cid_tablet_id_;
  sq_meta_tablet_id_ = related_tablet_ids.special_aux_tablet_id_;
  pq_centroid_tablet_id_ = related_tablet_ids.special_aux_tablet_id_;
  brute_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
  data_filter_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
}

int ObDASIvfBaseScanIter::get_next_saved_rowkey()
{
  int ret = OB_SUCCESS;
  if (saved_rowkeys_itr_->is_get_from_scan_iter()) {
    if (OB_FAIL(saved_rowkeys_itr_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row from scan iter", K(ret));
      }
    }
  } else {
    ObRowkey rowkey;
    if (OB_FAIL(saved_rowkeys_itr_->get_next_row(rowkey))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next next row from adaptor vid iter", K(ret));
      }
    } else {
      const ExprFixedArray& ivf_res_exprs = vec_aux_ctdef_->result_output_;
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_idx(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < ivf_res_exprs.count(); ++i) {
        ObExpr *expr = ivf_res_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret));
        } else {
          ObDatum &datum = expr->locate_datum_for_write(*vec_aux_rtdef_->eval_ctx_);
          if (OB_FAIL(datum.from_obj(rowkey.get_obj_ptr()[i]))) {
            LOG_WARN("failed to from obj", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObDASIvfBaseScanIter::get_next_saved_rowkeys(int64_t &count)
{
  int ret = OB_SUCCESS;
  if (saved_rowkeys_itr_->is_get_from_scan_iter()) {
    if (OB_FAIL(saved_rowkeys_itr_->get_next_rows(count))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row from scan iter", K(ret));
      }
    }
  } else {
    ObSEArray<ObRowkey, 8> rowkeys;
    if (OB_FAIL(saved_rowkeys_itr_->get_next_rows(rowkeys, count))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next next row from adaptor iter", K(ret));
      }
    } else if (count > 0) {
      const ExprFixedArray& ivf_res_exprs = vec_aux_ctdef_->result_output_;
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_size(count);
      for (int64_t idx_exp = 0; OB_SUCC(ret) && idx_exp < ivf_res_exprs.count(); ++idx_exp) {
        ObExpr *expr = ivf_res_exprs.at(idx_exp);
        ObDatum *datum = nullptr;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret));
        } else if (OB_ISNULL(datum = expr->locate_datums_for_update(*vec_aux_rtdef_->eval_ctx_, count))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, datums is nullptr", K(ret), KPC(expr));
        } else {
          for (int64_t idx_key = 0; OB_SUCC(ret) && idx_key < count; ++idx_key) {
            guard.set_batch_idx(idx_key);
            if (OB_FAIL(datum[idx_key].from_obj(rowkeys[idx_key].get_obj_ptr()[idx_exp]))) {
              LOG_WARN("fail to from obj", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            expr->set_evaluated_projected(*vec_aux_rtdef_->eval_ctx_);
          }
        }
      }
    }
  }

  return ret;
}

int ObDASIvfBaseScanIter::gen_rowkeys_itr_brute(ObDASIter *scan_iter)
{
  int ret = OB_SUCCESS;

  void *iter_buff = nullptr;
  if (OB_ISNULL(scan_iter) || OB_ISNULL(scan_iter->get_output())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan_iter is null.", K(ret), KP(scan_iter));
  } else if (!scan_iter->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan_iter is not inited.", K(ret));
  } else if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryRowkeyIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator rowkey iter.", K(ret));
  } else if (OB_FALSE_IT(saved_rowkeys_itr_ = new (iter_buff) ObVectorQueryRowkeyIterator())) {
  } else {
    if (OB_FAIL(saved_rowkeys_itr_->init(scan_iter))) {
      LOG_WARN("iter init failed.", K(ret));
    }
  }

  return ret;
}

int64_t ObDASIvfBaseScanIter::get_nprobe(const common::ObLimitParam &limit_param, int64_t enlargement_factor /*= 1*/)
{
  int64_t nprobe = INT64_MAX;
  int64_t sum = limit_param.limit_ + limit_param.offset_;

  if (sum > 0 && sum <= (INT64_MAX / enlargement_factor)) {
    nprobe = sum * enlargement_factor;
  }
  return nprobe;
}

int64_t ObDASIvfBaseScanIter::get_heap_size(const int64_t limit_k, const double select_ratio)
{
  int64_t heap_size = limit_k;
  /**
   * The minimum magnification is 2x and the maximum is 10x.
   * so if select_ratio >= 0.5, use 2x
   * if select_ratio < 0.1, use 10x
   * otherwise use limit_k / select_ratio
  */
  if (select_ratio > 0.0 && select_ratio < 1.0) {
    if (select_ratio >= 0.5) heap_size = 2 * limit_k;
    else if (select_ratio <= 0.1) heap_size = 10 * limit_k;
    else heap_size = std::ceil(limit_k / select_ratio);
  }
  return heap_size;
}

int ObDASIvfBaseScanIter::gen_near_cid_heap_from_cache(ObIvfCentCache &cent_cache,
                                                   share::ObVectorCenterClusterHelper<float, ObCenterId> &nearest_cid_heap,
                                                   bool save_center_vec /*= false*/)
{
  int ret = OB_SUCCESS;
  RWLock::RLockGuard guard(cent_cache.get_lock());
  uint64_t capacity = cent_cache.get_count();
  float *cid_vec = nullptr;
  ObString cid_str;
  ObCenterId center_id;
  center_id.tablet_id_ = centroid_tablet_id_.id();
  CenterSaveMode center_save_mode = CenterSaveMode::SHALLOW_COPY_CENTER_VEC;
  // NOTE(liyao): valid center id start from 1
  for (uint64_t i = 1; i <= capacity && OB_SUCC(ret); ++i) {
    if (OB_FAIL(cent_cache.read_centroid(i, cid_vec))) {
      LOG_WARN("fail to read centroid", K(ret), K(i));
    } else if (FALSE_IT(center_id.center_id_ = i)) {
    } else if (OB_FAIL(nearest_cid_heap.push_center(center_id, cid_vec, dim_, center_save_mode))) {
      LOG_WARN("failed to push center.", K(ret));
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::try_write_centroid_cache(
    ObIvfCentCache &cent_cache,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  RWLock::WLockGuard guard(cent_cache.get_lock());
  if (!cent_cache.is_writing()) {
    LOG_INFO("other threads already writed centroids cache, skip", K(ret));
  } else {
    ObArenaAllocator tmp_allocator;
    ObCenterId cent_id;
    const ObDASScanCtDef *centroid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_centroid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN);
    ObDASScanRtDef *centroid_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_centroid_tbl_idx());
    if (OB_FAIL(do_table_full_scan(is_vectorized,
                                   centroid_ctdef,
                                   centroid_rtdef,
                                   centroid_iter_,
                                   CENTROID_PRI_KEY_CNT,
                                   centroid_tablet_id_,
                                   centroid_iter_first_scan_,
                                   centroid_scan_param_))) {
      LOG_WARN("failed to do centroid table scan", K(ret));
    } else if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(centroid_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        ObExpr *cid_expr = centroid_ctdef->result_output_[CID_IDX];
        ObExpr *cid_vec_expr = centroid_ctdef->result_output_[CID_VECTOR_IDX];
        ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
        ObDatum *cid_vec_datum = cid_vec_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
        bool has_lob_header = centroid_ctdef->result_output_.at(CID_VECTOR_IDX)->obj_meta_.has_lob_header();
        uint64_t center_idx = 0;

        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          ObString cid = cid_datum[i].get_string();
          ObString cid_vec = cid_vec_datum[i].get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                          &tmp_allocator,
                          ObLongTextType,
                          CS_TYPE_BINARY,
                          has_lob_header,
                          cid_vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(cent_id, cid, ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
            LOG_WARN("fail to get center idx from string", K(ret), KPHEX(cid.ptr(), cid.length()));
          } else if (OB_FAIL(cent_cache.write_centroid(cent_id.center_id_, reinterpret_cast<float*>(cid_vec.ptr()), cid_vec.length()))) {
            LOG_WARN("fail to write centroid", K(ret), K(center_idx), KPHEX(cid_vec.ptr(), cid_vec.length()));
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(centroid_iter_, centroid_scan_param_, centroid_tablet_id_)
    } else {
      centroid_iter_->clear_evaluated_flag();
      ObExpr *cid_expr = centroid_ctdef->result_output_[CID_IDX];
      ObExpr *cid_vec_expr = centroid_ctdef->result_output_[CID_VECTOR_IDX];
      ObDatum &cid_datum = cid_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
      ObDatum &cid_vec_datum = cid_vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
      bool has_lob_header = centroid_ctdef->result_output_.at(CID_VECTOR_IDX)->obj_meta_.has_lob_header();

      uint64_t center_idx = 0;
      for (int i = 0; OB_SUCC(ret); ++i) {
        if (OB_FAIL(centroid_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to scan vid rowkey iter", K(ret));
          }
        } else {
          ObString cid = cid_datum.get_string();
          ObString cid_vec = cid_vec_datum.get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator, ObLongTextType, CS_TYPE_BINARY,
                                                                has_lob_header, cid_vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
                         cent_id, cid, ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
            LOG_WARN("fail to get center idx from string", K(ret), KPHEX(cid.ptr(), cid.length()));
          } else if (OB_FAIL(cent_cache.write_centroid(cent_id.center_id_, reinterpret_cast<float *>(cid_vec.ptr()),
                                                       cid_vec.length()))) {
            LOG_WARN("fail to write centroid", K(ret), K(center_idx), KPHEX(cid_vec.ptr(), cid_vec.length()));
          }
        }
      }  // end for
      int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;
      if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, centroid_iter_, centroid_scan_param_, centroid_tablet_id_))) {
        LOG_WARN("failed to reuse rowkey cid iter.", K(ret));
      } else {
        ret = tmp_ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (cent_cache.get_count() > 0) {
        cent_cache.set_completed();
        LOG_DEBUG("success to write centroid table cache", K(centroid_tablet_id_), K(cent_cache.get_count()));
      } else {
        cent_cache.reuse();
        LOG_DEBUG("Empty centroid table, no need to set cache", K(centroid_tablet_id_));
      }
    }
  }

  if (OB_FAIL(ret)) {
    cent_cache.reuse();
  }
  
  return ret;
}

int ObDASIvfBaseScanIter::get_centers_cache(bool is_vectorized, 
                                        bool is_pq_centers, 
                                        ObIvfCacheMgrGuard &cache_guard,
                                        ObIvfCentCache *&cent_cache, 
                                        bool &is_cache_usable)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  ObIvfCacheMgr *cache_mgr = nullptr;
  const ObDASScanCtDef *centroid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_centroid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN);

  // pq/flat both use centroid_tablet_id_
  if (! cache_guard.is_valid() && OB_FAIL(vec_index_service->acquire_ivf_cache_mgr_guard(
        ls_id_, centroid_tablet_id_, vec_index_param_, dim_, centroid_ctdef->ref_table_id_, cache_guard))) {
    LOG_WARN("failed to get ObPluginVectorIndexAdapter", 
      K(ret), K(ls_id_), K(centroid_tablet_id_), K(vec_index_param_));
  } else if (OB_ISNULL(cache_mgr = cache_guard.get_ivf_cache_mgr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null cache mgr", K(ret));
  } else if (OB_FAIL(cache_mgr->get_or_create_cache_node(
      is_pq_centers ? IvfCacheType::IVF_PQ_CENTROID_CACHE : IvfCacheType::IVF_CENTROID_CACHE, cent_cache))) {
    LOG_WARN("fail to get or create cache node", K(ret), K(is_pq_centers));
    if (ret == OB_ALLOCATE_MEMORY_FAILED) {
      is_cache_usable = false;
      ret = OB_SUCCESS;
    }
  } else if (!cent_cache->is_completed()) {
    if (cent_cache->set_writing_if_idle()) {
      // write cache
      int tmp_ret = is_pq_centers ? 
          try_write_pq_centroid_cache(*cent_cache, is_vectorized) : 
          try_write_centroid_cache(*cent_cache, is_vectorized);
      if (OB_TMP_FAIL(tmp_ret)) {
        LOG_WARN("fail to try write centroid cache", K(ret), K(is_vectorized), KPC(cent_cache), K(is_pq_centers));
      } else {
        is_cache_usable = cent_cache->is_completed();
      }
    } else {
      LOG_INFO("other threads already writed centroids cache, skip", K(ret));
    }
  } else {
    // read cache
    is_cache_usable = true;
  }
  return ret;
}

template <typename T>
int ObDASIvfBaseScanIter::generate_nearest_cid_heap(
    bool is_vectorized,
    T &nearest_cid_heap,
    bool save_center_vec /*= false*/)
{
  int ret = OB_SUCCESS;
  ObIvfCentCache *cent_cache = nullptr;
  bool is_cache_usable = false;

  if (OB_FAIL(get_centers_cache(is_vectorized, false/*is_pq_centers*/, center_cache_guard_, cent_cache, is_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(cent_cache));
  } else if (is_cache_usable) {
    if (OB_FAIL(gen_near_cid_heap_from_cache(*cent_cache, nearest_cid_heap, save_center_vec))) {
      LOG_WARN("fail to gen near cid heap from cache", K(ret), K(center_cache_guard_));
    }
  } else {
    if (OB_FAIL(gen_near_cid_heap_from_table(is_vectorized, nearest_cid_heap, save_center_vec))) {
      LOG_WARN("fail to gen near cid heap from table", K(ret), K(center_cache_guard_));
    }
  }

  return ret;
}

int ObDASIvfBaseScanIter::gen_near_cid_heap_from_table(
  bool is_vectorized,
  share::ObVectorCenterClusterHelper<float, ObCenterId> &nearest_cid_heap,
  bool save_center_vec /*= false*/)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *centroid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_centroid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN);
  ObDASScanRtDef *centroid_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_centroid_tbl_idx());
  
  // if no have center table cache, need deep copy center vec, else shallol copy
  // todo(wmj): need fit ivf adaptor cache
  CenterSaveMode center_save_mode = save_center_vec ? CenterSaveMode::DEEP_COPY_CENTER_VEC : CenterSaveMode::NOT_SAVE_CENTER_VEC;
  if (OB_FAIL(do_table_full_scan(is_vectorized,
                                  centroid_ctdef,
                                  centroid_rtdef,
                                  centroid_iter_,
                                  CENTROID_PRI_KEY_CNT,
                                  centroid_tablet_id_,
                                  centroid_iter_first_scan_,
                                  centroid_scan_param_))) {
    LOG_WARN("failed to do centroid table scan", K(ret));
  } else if (is_vectorized) {
    IVF_GET_NEXT_ROWS_BEGIN(centroid_iter_)
    if (OB_SUCC(ret)) {
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_size(scan_row_cnt);
      ObExpr *cid_expr = centroid_ctdef->result_output_[CID_IDX];
      ObExpr *cid_vec_expr = centroid_ctdef->result_output_[CID_VECTOR_IDX];
      ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
      ObDatum *cid_vec_datum = cid_vec_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
      bool has_lob_header = centroid_ctdef->result_output_.at(CID_VECTOR_IDX)->obj_meta_.has_lob_header();
      ObCenterId center_id;

      for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
        guard.set_batch_idx(i);
        ObString cid = cid_datum[i].get_string();
        ObString cid_vec = cid_vec_datum[i].get_string();
        
        if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(center_id, cid))) {
          LOG_WARN("failed to get center id from string", K(ret));
        } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                        &mem_context_->get_arena_allocator(),
                        ObLongTextType,
                        CS_TYPE_BINARY,
                        has_lob_header,
                        cid_vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (OB_FAIL(nearest_cid_heap.push_center(center_id, reinterpret_cast<float *>(cid_vec.ptr()), dim_, center_save_mode))) {
          LOG_WARN("failed to push center.", K(ret));
        }
      }
    }
    IVF_GET_NEXT_ROWS_END(centroid_iter_, centroid_scan_param_, centroid_tablet_id_)
  } else {
    centroid_iter_->clear_evaluated_flag();
    ObExpr *cid_expr = centroid_ctdef->result_output_[CID_IDX];
    ObExpr *cid_vec_expr = centroid_ctdef->result_output_[CID_VECTOR_IDX];
    ObDatum &cid_datum = cid_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
    ObDatum &cid_vec_datum = cid_vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
    bool has_lob_header = centroid_ctdef->result_output_.at(CID_VECTOR_IDX)->obj_meta_.has_lob_header();

    ObCenterId center_id;
    for (int i = 0; OB_SUCC(ret); ++i) {
      if (OB_FAIL(centroid_iter_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to scan vid rowkey iter", K(ret));
        }
      } else {
        ObString cid = cid_datum.get_string();
        ObString cid_vec = cid_vec_datum.get_string();
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&mem_context_->get_arena_allocator(), ObLongTextType,
                                                              CS_TYPE_BINARY, has_lob_header, cid_vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (cid_vec.empty()) {
          // ignoring null vector.
        } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(center_id, cid))) {
          LOG_WARN("failed to get center id from string", K(ret));
        } else if (OB_FAIL(nearest_cid_heap.push_center(center_id, reinterpret_cast<float *>(cid_vec.ptr()), dim_,
                                                        center_save_mode))) {
          LOG_WARN("failed to push center.", K(ret));
        }
      }
    }

    if (ret == OB_ITER_END) {
      if (OB_FAIL(centroid_iter_->reuse())) {
        LOG_WARN("fail to reuse scan iterator.", K(ret));
      }
    }
  }
  
  return ret;
}

int ObDASIvfBaseScanIter::parse_centroid_datum(
  const ObDASScanCtDef *centroid_ctdef,
  ObIAllocator& allocator, 
  blocksstable::ObDatumRow *datum_row,
  ObString &cid, 
  ObString &cid_vec)
{
  int ret = OB_SUCCESS;
  cid.reset();
  cid_vec.reset();
  if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row invalid.", K(ret));
  } else if (datum_row->get_column_count() != CENTROID_ALL_KEY_CNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
  } else if (OB_FALSE_IT(cid = datum_row->storage_datums_[0].get_string())) {
  } else if (OB_FALSE_IT(cid_vec = datum_row->storage_datums_[1].get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  &allocator,
                  ObLongTextType,
                  CS_TYPE_BINARY,
                  centroid_ctdef->result_output_.at(1)->obj_meta_.has_lob_header(),
                  cid_vec))) {
    LOG_WARN("failed to get real data.", K(ret));
  }

  return ret;
}

int ObDASIvfBaseScanIter::prepare_cid_range(
  const ObDASScanCtDef *cid_vec_ctdef, 
  int64_t &cid_vec_column_count,
  int64_t &cid_vec_pri_key_cnt,
  int64_t &rowkey_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cid_vec_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef is null", K(ret), KP(cid_vec_ctdef));
  } else if (OB_FALSE_IT(cid_vec_column_count = cid_vec_ctdef->access_column_ids_.count())) {
  } else if (OB_UNLIKELY(cid_vec_column_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret));
  } else {
    cid_vec_pri_key_cnt = cid_vec_column_count - CID_VEC_COM_KEY_CNT;
    rowkey_cnt = cid_vec_column_count - CID_VEC_COM_KEY_CNT - CID_VEC_FIXED_PRI_KEY_CNT;
  }
  return ret;
}

int ObDASIvfBaseScanIter::scan_cid_range(
  const ObString &cid, 
  int64_t cid_vec_pri_key_cnt, 
  const ObDASScanCtDef *cid_vec_ctdef, 
  ObDASScanRtDef *cid_vec_rtdef,
  storage::ObTableScanIterator *&cid_vec_scan_iter)
{
  int ret = OB_SUCCESS;
  ObNewRange cid_pri_key_range;
  if (OB_ISNULL(cid_vec_ctdef) || OB_ISNULL(cid_vec_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(cid_vec_ctdef), KP(cid_vec_rtdef));
  } else if (OB_FAIL(build_cid_vec_query_range(cid, cid_vec_pri_key_cnt, cid_pri_key_range))) {
    LOG_WARN("failed to build cid vec query rowkey", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_range(cid_pri_key_range, cid_vec_scan_param_, cid_vec_ctdef->ref_table_id_))) {
    LOG_WARN("failed to append scan range", K(ret));
  } else if (OB_FAIL(do_aux_table_scan(cid_vec_iter_first_scan_,
                                        cid_vec_scan_param_,
                                        cid_vec_ctdef,
                                        cid_vec_rtdef,
                                        cid_vec_iter_,
                                        cid_vec_tablet_id_))) {
    LOG_WARN("fail to rescan cid vec table scan iterator.", K(ret));
  } else {
    cid_vec_scan_iter = static_cast<storage::ObTableScanIterator *>(cid_vec_iter_->get_output_result_iter());
    if (OB_ISNULL(cid_vec_scan_iter)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null scan iter", K(ret));
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::get_rowkey_pre_filter(ObIAllocator& allocator, bool is_vectorized, int64_t max_rowkey_count)
{
  int ret = OB_SUCCESS;
  uint64_t rowkey_count = 0;
  while (OB_SUCC(ret) && rowkey_count < max_rowkey_count) {
    inv_idx_scan_iter_->clear_evaluated_flag();
    if (!is_vectorized) {
      ObRowkey *rowkey = nullptr;
      if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next rowkey", K(ret));
        }
      } else if (OB_FALSE_IT(rowkey_count++)) {
      } else if (OB_FAIL(get_rowkey(allocator, rowkey))) {
        // pre_fileter_rowkeys_ need keep rowkey mem, so use vec_op_alloc_
        LOG_WARN("failed to get rowkey", K(ret));
      } else if (OB_FAIL(pre_fileter_rowkeys_.push_back(*rowkey))) {
        LOG_WARN("failed to push rowkey", K(ret));
      }
    } else {
      int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;

      int64_t scan_row_cnt = 0;
      if (OB_FAIL(inv_idx_scan_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next rowkey", K(ret));
        }
      }

      rowkey_count += scan_row_cnt;
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        LOG_WARN("fail to get next row from inv_idx_scan_iter_", K(ret));
      } else if (scan_row_cnt > 0) {
        ret = OB_SUCCESS;
      }

      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        for (int i = 0; OB_SUCC(ret) && i < scan_row_cnt; i++) {
          guard.set_batch_idx(i);
          ObRowkey *rowkey = nullptr;
          // pre_fileter_rowkeys_ need keep rowkey mem, so use vec_op_alloc_
          if (OB_FAIL(get_rowkey(allocator, rowkey))) {
            LOG_WARN("failed to add rowkey", K(ret), K(i));
          } else if (OB_FAIL(pre_fileter_rowkeys_.push_back(*rowkey))) {
            LOG_WARN("store push rowkey", K(ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_ITER_END != ret) {
  } else if (OB_ITER_END == ret && rowkey_count > 0) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObDASIvfBaseScanIter::get_main_rowkey_from_cid_vec_datum(ObIAllocator& allocator,
                                                             const ObDASScanCtDef *cid_vec_ctdef,
                                                             const int64_t rowkey_cnt,
                                                             ObRowkey &main_rowkey,
                                                             bool need_alloc /* true */)
{
  int ret = OB_SUCCESS;

  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;

  if (OB_ISNULL(cid_vec_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(cid_vec_ctdef));
  } else {
    // cid_vec_scan_iter output: [IVF_CID_VEC_CID_COL IVF_CID_VEC_VECTOR_COL ROWKEY]
    // Note: when _enable_defensive_check = 2, cid_vec_out_exprs is [IVF_CID_VEC_CID_COL IVF_CID_VEC_VECTOR_COL ROWKEY
    // DEFENSE_CHECK_COL]
    const ExprFixedArray& cid_vec_out_exprs = cid_vec_ctdef->result_output_;
    if (rowkey_cnt > cid_vec_out_exprs.count() - CID_VEC_COM_KEY_CNT - CID_VEC_FIXED_PRI_KEY_CNT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey_cnt is illegal", K(ret), K(rowkey_cnt), K(cid_vec_out_exprs.count()));
    } else if (need_alloc) {
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(rowkey_cnt));
      } else if (OB_FALSE_IT(obj_ptr = new (buf) ObObj[rowkey_cnt])) {
      }
    } else {
      obj_ptr = main_rowkey.get_obj_ptr();
    }
    if (OB_FAIL(ret)) {
    } else {
      int rowkey_idx = 0;
      for (int64_t i = 2; OB_SUCC(ret) && i < cid_vec_out_exprs.count() && rowkey_idx < rowkey_cnt; ++i) {
        ObObj tmp_obj;
        ObExpr *expr = cid_vec_out_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret));
        } else if (OB_FAIL(expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).to_obj(obj_ptr[rowkey_idx++], expr->obj_meta_, expr->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && need_alloc) {
      main_rowkey.assign(obj_ptr, rowkey_cnt);
    }
  }

  return ret;
}

int ObDASIvfBaseScanIter::get_pre_filter_rowkey_batch(ObIAllocator &allocator,
                                                  bool is_vectorized,
                                                  int64_t batch_row_count,
                                                  bool &index_end)
{
  int ret = OB_SUCCESS;
  index_end = false;
  if (!is_vectorized) {
    for (int i = 0; OB_SUCC(ret) && i < batch_row_count && !index_end; ++i) {
      inv_idx_scan_iter_->clear_evaluated_flag();
      ObRowkey *rowkey = nullptr;
      if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
        index_end = true;
      } else if (OB_FAIL(get_rowkey(allocator, rowkey))) {
        // pre_fileter_rowkeys_ need keep rowkey mem, so use vec_op_alloc_
        LOG_WARN("failed to get rowkey", K(ret));
      } else if (OB_FAIL(pre_fileter_rowkeys_.push_back(*rowkey))) {
        LOG_WARN("failed to save rowkey", K(ret));
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
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_size(scan_row_cnt);
      for (int i = 0; OB_SUCC(ret) && i < scan_row_cnt; i++) {
        guard.set_batch_idx(i);
        ObRowkey *rowkey = nullptr;
        // pre_fileter_rowkeys_ need keep rowkey mem, so use vec_op_alloc_
        if (OB_FAIL(get_rowkey(allocator, rowkey))) {
          LOG_WARN("failed to add rowkey", K(ret), K(i));
        } else if (OB_FAIL(pre_fileter_rowkeys_.push_back(*rowkey))) {
          LOG_WARN("failed to save rowkey", K(ret));
        }
      }
    }
  }

  return ret;
}

template <typename T>
int ObDASIvfBaseScanIter::calc_vec_dis(T *a, T *b, int dim, float &dis, ObExprVectorDistance::ObVecDisType dis_type)
{
  int ret = OB_SUCCESS;
  if (dis_type != oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
    double distance = 0;
    if (OB_FAIL(oceanbase::sql::ObExprVectorDistance::DisFunc<T>::distance_funcs[dis_type](a, b, dim, distance))) {
      LOG_WARN("failed to get distance type", K(ret), KP(a), KP(b), K(dim));
    } else {
      dis = distance;
    }
  } else {
    dis = ObVectorL2Distance<T>::l2_square_flt_func(a, b, dim);
  }

  return ret;
}

int ObDASIvfBaseScanIter::do_post_filter(bool is_vectorized, ObIVFRowkeyDistMap &rowkey_dist_map, ObSEArray<ObIvfRowkeyDistEntry, 16> &matched_rowkeys)
{
  int ret = OB_SUCCESS;
  if (vec_aux_ctdef_->can_use_vec_pri_opt()) {
    if (OB_FAIL(do_simple_post_filter(rowkey_dist_map, matched_rowkeys))) {
      LOG_WARN("do_simple_post_filter fail", K(ret));
    }
  } else if (OB_FAIL(do_table_post_filter(is_vectorized, rowkey_dist_map, matched_rowkeys))) {
    LOG_WARN("do_table_post_filter fail", K(ret));
  }
  return ret;
}

int ObDASIvfBaseScanIter::do_simple_post_filter(ObIVFRowkeyDistMap &rowkey_dist_map, ObSEArray<ObIvfRowkeyDistEntry, 16> &matched_rowkeys)
{
  int ret = OB_SUCCESS;
  ObIVFRowkeyDistMapIterator rowkey_iter = rowkey_dist_map.begin();
  ObIVFRowkeyDistMapIterator rowkey_end = rowkey_dist_map.end();
  ObDASScanIter* inv_iter = (ObDASScanIter*)inv_idx_scan_iter_;
  ObArray<const ObNewRange *> rk_range;
  const ObRangeArray& key_range = inv_iter->get_scan_param().key_ranges_;
  for (int64_t i = 0; i < key_range.count() && OB_SUCC(ret); i++) {
    const ObNewRange *range = &key_range.at(i);
    if (OB_FAIL(rk_range.push_back(range))) {
      LOG_WARN("fail to push back range", K(ret), K(i));
    }
  }
  while (OB_SUCC(ret) && rowkey_iter != rowkey_end) {
    ObRowkey &rowkey = rowkey_iter->first;
    const ObIvfRowkeyDistEntry &entry = rowkey_iter->second;
    bool is_match = false;
    ObNewRange tmp_range;
    if (OB_FAIL(tmp_range.build_range(rk_range.at(0)->table_id_, rowkey))) {
      LOG_WARN("fail to build tmp range", K(ret));
    }
    // do compare
    for (int64_t i = 0; i < rk_range.count() && !is_match && OB_SUCC(ret); i++) {
      if (rk_range.at(i)->compare_with_startkey2(tmp_range) <= 0 && rk_range.at(i)->compare_with_endkey2(tmp_range) >= 0) {
        is_match = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_match && OB_FAIL(matched_rowkeys.push_back(entry))) {
      LOG_WARN("failed to push back.", K(ret), K(rowkey), K(entry));
    } else {
      ++rowkey_iter;
    }
  }

  if (OB_SUCC(ret)) {
    adaptive_ctx_.iter_filter_row_cnt_ += rowkey_dist_map.size();
    adaptive_ctx_.iter_res_row_cnt_ += matched_rowkeys.count();
    if (can_retry_ && OB_FAIL(check_iter_filter_need_retry())) {
      LOG_WARN("ret of check iter filter need retry.", K(ret), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_));
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::do_table_post_filter(bool is_vectorized, ObIVFRowkeyDistMap &rowkey_dist_map, ObSEArray<ObIvfRowkeyDistEntry, 16> &matched_rowkeys)
{
  int ret = OB_SUCCESS;
  int64_t scan_row_cnt = 0;
  int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  int64_t count = 0;
  int64_t matched_count = 0;
  ObIVFRowkeyDistMapIterator rowkey_iter = rowkey_dist_map.begin();
  ObIVFRowkeyDistMapIterator rowkey_end = rowkey_dist_map.end();
  while (OB_SUCC(ret) && rowkey_iter != rowkey_end) {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_row_count && rowkey_iter != rowkey_end; ++i) {
      ObRowkey &rowkey = rowkey_iter->first;
      if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(
              rowkey, data_filter_scan_param_, data_filter_ctdef_->ref_table_id_))) {
        LOG_WARN("failed to set lookup key", K(ret), K(i), K(count));
      } else {
        ++count;
        ++rowkey_iter;
        LOG_DEBUG("add filter rowkey", K(i), K(rowkey), K(count));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_aux_table_scan(data_filter_iter_first_scan_, 
                                      data_filter_scan_param_, 
                                      data_filter_ctdef_, 
                                      data_filter_rtdef_, 
                                      data_filter_iter_, 
                                      data_filter_tablet_id_))) {
      LOG_WARN("failed to do data filter table scan.", K(ret), K(count), K(data_filter_iter_first_scan_));
    } else if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(data_filter_iter_)
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          ObRowkey &main_rowkey = tmp_main_rowkey_;
          ObIvfRowkeyDistEntry* entry = nullptr;
          if (OB_FAIL(get_main_rowkey(data_filter_ctdef_, main_rowkey))) {
            LOG_WARN("fail to get main rowkey", K(ret));
          } else if (OB_ISNULL(entry = rowkey_dist_map.get(main_rowkey))) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("rowkey not exist", K(ret), K(main_rowkey));
          } else if (OB_FAIL(matched_rowkeys.push_back(*entry))) {
            LOG_WARN("failed to push back.", K(ret), KPC(entry));
          } else {
            ++matched_count;
            LOG_DEBUG("match filter rowkey", K(i), K(main_rowkey), K(matched_count));
          }
        }
      IVF_GET_NEXT_ROWS_END(data_filter_iter_, data_filter_scan_param_, data_filter_tablet_id_)
    } else {
      data_filter_iter_->clear_evaluated_flag();
      for (int i = 0; OB_SUCC(ret) && i < batch_row_count; ++i) {
        ObRowkey &main_rowkey = tmp_main_rowkey_;
        ObIvfRowkeyDistEntry* entry = nullptr;
        if (OB_FAIL(data_filter_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to scan vid rowkey iter", K(ret));
          }
        } else if (OB_FAIL(get_main_rowkey(data_filter_ctdef_, main_rowkey))) {
          LOG_WARN("fail to get main rowkey", K(ret));
        } else if (OB_ISNULL(entry = rowkey_dist_map.get(main_rowkey))) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("rowkey not exist", K(ret), K(main_rowkey));
        } else if (OB_FAIL(matched_rowkeys.push_back(*entry))) {
          LOG_WARN("failed to push back.", K(ret), KPC(entry));
        } else {
          ++matched_count;
          LOG_DEBUG("match filter rowkey", K(i), K(main_rowkey), K(matched_count));
        }
      }
      int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;
      if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, data_filter_iter_, data_filter_scan_param_, data_filter_tablet_id_))) { 
        LOG_WARN("failed to reuse data_filter iter.", K(ret));
      } else {
        ret = tmp_ret;
      }
    }
  }
  if (OB_SUCC(ret)) {
    adaptive_ctx_.iter_filter_row_cnt_ += count;
    adaptive_ctx_.iter_res_row_cnt_ += matched_count;
    if (can_retry_ && OB_FAIL(check_iter_filter_need_retry())) {
      LOG_WARN("ret of check iter filter need retry.", K(ret), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_));
    }
  }
  return ret;
}

/*************************** implement ObDASIvfScanIter ****************************/
int ObDASIvfScanIter::inner_init(ObDASIterParam &param)
{
  int ret = ObDASIvfBaseScanIter::inner_init(param);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to init", K(ret));
  } else if (is_post_filter()) {
    if (OB_FAIL(near_cid_.reserve(nprobes_))) {
      LOG_WARN("failed to reserve nearest cid vec", K(ret), K(vec_index_param_.nlist_));
    }
  } else if (OB_FAIL(near_cid_dist_.allocate_array(persist_alloc_, vec_index_param_.nlist_ + 1))) {
    LOG_WARN("failed to reserve nearest cid vec", K(ret), K(vec_index_param_.nlist_));
  } else {
    memset(near_cid_dist_.get_data(), 0, near_cid_dist_.count() * sizeof(bool));
  }
  return ret;
};

int ObDASIvfScanIter::inner_reuse()
{
  near_cid_.reuse();
  memset(near_cid_dist_.get_data(), 0, near_cid_dist_.count() * sizeof(bool));
  return ObDASIvfBaseScanIter::inner_reuse();
}

int ObDASIvfScanIter::inner_release()
{
  near_cid_.reset();
  near_cid_dist_.reset();
  return ObDASIvfBaseScanIter::inner_release();
}

int ObDASIvfScanIter::get_nearest_probe_center_ids(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  share::ObVectorCenterClusterHelper<float, ObCenterId> nearest_cid_heap(
      mem_context_->get_arena_allocator(), reinterpret_cast<const float *>(real_search_vec_.ptr()),
      dis_type_, dim_, nprobes_, 0.0, (is_pre_filter() || is_iter_filter()), &iterative_filter_ctx_.allocator_);
  if (OB_FAIL(generate_nearest_cid_heap(is_vectorized, nearest_cid_heap))) {
    LOG_WARN("failed to generate nearest cid heap", K(ret), K(nprobes_), K(dim_), K(real_search_vec_));
  }
  if (OB_SUCC(ret)) {
    if (nearest_cid_heap.get_center_count() == 0) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (nearest_cid_heap.is_save_all_center()) {
      if (OB_FAIL(nearest_cid_heap.get_all_centroids(iterative_filter_ctx_.centroids_))) {
        LOG_WARN("init_centroids fail", K(ret));
      } else if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_center_ids(nprobes_, near_cid_))) {
        LOG_WARN("failed to get top n", K(ret), K(iterative_filter_ctx_));
      }
    } else if (OB_FAIL(nearest_cid_heap.get_nearest_probe_center_ids(near_cid_))) {
      LOG_WARN("failed to get top n", K(ret));
    }
  }
  return ret;
}

// for flat/sq, con_key is vector(float/uint8)
int ObDASIvfScanIter::parse_cid_vec_datum(
  ObIAllocator& allocator,
  int64_t cid_vec_column_count,
  const ObDASScanCtDef *cid_vec_ctdef,
  const int64_t rowkey_cnt,
  ObRowkey &main_rowkey,
  ObString &com_key)
{
  int ret = OB_SUCCESS;
  ObExpr *vec_expr = cid_vec_ctdef->result_output_[1];
  if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(allocator, cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
    LOG_WARN("failed to get main rowkey from cid vec datum", K(ret));
  } else if (OB_FALSE_IT(com_key = vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  &allocator,
                  ObLongTextType,
                  CS_TYPE_BINARY,
                  cid_vec_ctdef->result_output_.at(1)->obj_meta_.has_lob_header(),
                  com_key))) {
    LOG_WARN("failed to get real data.", K(ret));
  }
  return ret;
}

template <typename T>
int ObDASIvfScanIter::get_rowkeys_to_heap(const ObString &cid_str, int64_t cid_vec_pri_key_cnt,
                                          int64_t cid_vec_column_count, int64_t rowkey_cnt, bool is_vectorized,
                                          ObVectorCenterClusterHelper<T, ObRowkey> &nearest_rowkey_heap,
                                          bool &is_first_vec, bool &cid_vec_need_norm, ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN);
  ObDASScanRtDef *cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx());
  storage::ObTableScanIterator *cid_vec_scan_iter = nullptr;
  if (OB_FAIL(scan_cid_range(cid_str, cid_vec_pri_key_cnt, cid_vec_ctdef, cid_vec_rtdef, cid_vec_scan_iter))) {
    LOG_WARN("fail to scan cid range", K(ret), K(cid_str), K(cid_vec_pri_key_cnt));
  } else if (is_vectorized) {
    IVF_GET_NEXT_ROWS_BEGIN(cid_vec_iter_)
    if (OB_SUCC(ret)) {
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_size(scan_row_cnt);
      bool has_lob_header = cid_vec_ctdef->result_output_.at(CID_VECTOR_IDX)->obj_meta_.has_lob_header();
      ObExpr *cid_expr = cid_vec_ctdef->result_output_[CID_VECTOR_IDX];
      ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
      adaptive_ctx_.cid_vec_scan_rows_ += scan_row_cnt;
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
        guard.set_batch_idx(i);
        ObRowkey main_rowkey;
        ObString vec = cid_datum[i].get_string();
        bool skip = false;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                        mem_context_->get_arena_allocator(),
                        cid_datum[i],
                        cid_expr->datum_meta_,
                        has_lob_header,
                        vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (OB_ISNULL(vec.ptr())) {
          // ignoring null vector.
        } else if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
          LOG_WARN("fail to get main rowkey", K(ret));
        } else if (prefilter != nullptr && !prefilter->test(main_rowkey)) {
          // has been filter, do nothing
          skip = true;
        } else if (std::is_same<T, float>::value && need_norm_) {
          // If the first vec needs do_norm, it means that the vec in the cid_vector table is not normalized.
          if (is_first_vec) {
            if (OB_FAIL(ObVectorNormalize::L2_normalize_vector(
                    vec_aux_ctdef_->dim_, reinterpret_cast<float *>(vec.ptr()), reinterpret_cast<float *>(vec.ptr()),
                    &cid_vec_need_norm))) {
              LOG_WARN("failed to normalize vector.", K(ret));
            } else {
              is_first_vec = false;
            }
          } else if (cid_vec_need_norm && OB_FAIL(ObVectorNormalize::L2_normalize_vector(
                                              vec_aux_ctdef_->dim_, reinterpret_cast<float *>(vec.ptr()),
                                              reinterpret_cast<float *>(vec.ptr())))) {
            LOG_WARN("failed to normalize vector.", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get rowkey", K(ret));
        } else if (skip) {
        } else if (OB_NOT_NULL(vec.ptr()) && OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, reinterpret_cast<T *>(vec.ptr()), dim_))) {
          LOG_WARN("failed to push center.", K(ret));
        } else {
          adaptive_ctx_.vec_dist_calc_cnt_ ++;
        }
      }
    }
    IVF_GET_NEXT_ROWS_END(cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_)
  } else {
    while (OB_SUCC(ret)) {
      ObRowkey main_rowkey;
      ObString vec;
      bool skip = false;
      adaptive_ctx_.cid_vec_scan_rows_ ++;
      // cid_vec_scan_iter output: [IVF_CID_VEC_CID_COL IVF_CID_VEC_VECTOR_COL ROWKEY]
      if (OB_FAIL(cid_vec_scan_iter->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to scan vid rowkey iter", K(ret));
        }
      } else if (OB_FAIL(parse_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_column_count, cid_vec_ctdef, rowkey_cnt, main_rowkey, vec))) {
        LOG_WARN("fail to parse cid vec datum", K(ret), K(cid_vec_column_count), K(rowkey_cnt));
      } else if (prefilter != nullptr && !prefilter->test(main_rowkey)) {
        // has been filter, do nothing
        skip = true;
      } else if (OB_ISNULL(vec.ptr())) {
        // ignoring null vector.
      } else if (std::is_same<T, float>::value && need_norm_) {
        // If the first vec needs do_norm, it means that the vec in the cid_vector table is not normalized.
        if (is_first_vec) {
          if (OB_FAIL(ObVectorNormalize::L2_normalize_vector(
                  vec_aux_ctdef_->dim_, reinterpret_cast<float *>(vec.ptr()), reinterpret_cast<float *>(vec.ptr()),
                  &cid_vec_need_norm))) {
            LOG_WARN("failed to normalize vector.", K(ret));
          } else {
            is_first_vec = false;
          }
        } else if (cid_vec_need_norm && OB_FAIL(ObVectorNormalize::L2_normalize_vector(
                                            vec_aux_ctdef_->dim_, reinterpret_cast<float *>(vec.ptr()),
                                            reinterpret_cast<float *>(vec.ptr())))) {
          LOG_WARN("failed to normalize vector.", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to get rowkey", K(ret));
      } else if (skip) {
      } else if (OB_NOT_NULL(vec.ptr()) &&
                 OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, reinterpret_cast<T *>(vec.ptr()), dim_))) {
        LOG_WARN("failed to push center.", K(ret));
      } else {
        adaptive_ctx_.vec_dist_calc_cnt_ ++;
      }
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (OB_FAIL(cid_vec_iter_->reuse())) {
        LOG_WARN("fail to reuse scan iterator.", K(ret));
      }
    }
  }

  return ret;
}

template <typename T>
int ObDASIvfScanIter::get_nearest_limit_rowkeys_in_cids(
    bool is_vectorized,
    T *search_vec,
    ObSEArray<ObRowkey, 16> &saved_rowkeys,
    ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;
  // only scan without filter can reach here
  share::ObVectorCenterClusterHelper<T, ObRowkey> nearest_rowkey_heap(
      vec_op_alloc_, search_vec, dis_type_, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
  if (OB_FAIL(get_nearest_limit_rowkeys_in_cids<T>(is_vectorized, search_vec, nearest_rowkey_heap, prefilter))) {
    LOG_WARN("calc_nearest_limit_rowkeys_in_cids fail", K(ret));
  } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys))) {
    LOG_WARN("failed to get top n", K(ret));
  }
  return ret;
}

template <typename T>
int ObDASIvfScanIter::get_nearest_limit_rowkeys_in_cids(
    bool is_vectorized,
    T *serch_vec,
    share::ObVectorCenterClusterHelper<T, ObRowkey> &nearest_rowkey_heap,
    ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN);
  ObDASScanRtDef *cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx());
  int64_t cid_vec_column_count = 0;
  int64_t cid_vec_pri_key_cnt = 0;
  int64_t rowkey_cnt = 0;
  int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
  char *buf = nullptr;
  ObString cid_str;

  if (OB_FAIL(prepare_cid_range(cid_vec_ctdef, cid_vec_column_count, cid_vec_pri_key_cnt, rowkey_cnt))) {
    LOG_WARN("fail to prepare cid range", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(mem_context_->get_arena_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc cid", K(ret));
  } else {
    cid_str.assign_buffer(buf, buf_len);
  } 
  // 3. Obtain nprobes * k rowkeys
  if (near_cid_.count() == 0) {
    // The situation of index creation with table creation
    ObString empty_cid;
    bool is_first_vec = true;
    bool cid_vec_need_norm = true;
    if (OB_FAIL(get_rowkeys_to_heap(empty_cid, cid_vec_pri_key_cnt, cid_vec_column_count, rowkey_cnt, is_vectorized,
                                    nearest_rowkey_heap, is_first_vec, cid_vec_need_norm, prefilter))) {
      LOG_WARN("failed to get rowkeys to heap, when near_cid is empty", K(ret));
    }
  } else {
    // for adaptor old version(< 4353), which cid_vec is not normlized in cosine dis
    bool is_first_vec = true;
    bool cid_vec_need_norm = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < near_cid_.count(); ++i) {
      const ObCenterId &cur_cid = near_cid_.at(i);
      if (OB_FALSE_IT(cid_str.assign_buffer(buf, buf_len))) {
      } else if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(cur_cid, cid_str))) {
        LOG_WARN("failed to set center_id to string", K(ret), K(cur_cid), K(cid_str));
      } else if (OB_FAIL(get_rowkeys_to_heap(cid_str, cid_vec_pri_key_cnt, cid_vec_column_count, rowkey_cnt,
                                             is_vectorized, nearest_rowkey_heap, is_first_vec, cid_vec_need_norm, prefilter))) {
        LOG_WARN("failed to get rowkeys to heap", K(ret), K(cur_cid));
      }
    }
  }
  return ret;
}

int ObDASIvfScanIter::process_ivf_scan_post(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_ivf_scan_post<float>(is_vectorized, reinterpret_cast<float *>(real_search_vec_.ptr())))) {
    LOG_WARN("failed to do post filter", K(ret), K(is_vectorized));
  }
  return ret;
}

template <typename T>
int ObDASIvfScanIter::do_ivf_scan_post(bool is_vectorized, T *search_vec)
{
  int ret = OB_SUCCESS;
  const int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  if (OB_FAIL(get_nearest_probe_center_ids(is_vectorized))) {
    // 1. Scan the centroid table, range: [min] ~ [max]; sort by l2_distance(c_vec, search_vec_); limit nprobes;
    // return the cid column.
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("failed to get nearest probe center ids", K(ret));
    } else if (is_adaptive_filter()) {
      LOG_INFO("nearest probe center ids is empty", K(ret));
      ret = OB_SUCCESS;
      adaptive_ctx_.is_brute_force_= true;
      float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
      ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      bool index_end = false;
      while (OB_SUCC(ret) && !index_end) {
        if (OB_FAIL(get_pre_filter_rowkey_batch(mem_context_->get_arena_allocator(), is_vectorized, batch_row_count,
                                                index_end))) {
          LOG_WARN("failed to get rowkey batch", K(ret), K(is_vectorized));
        } else if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
          LOG_WARN("failed to get limit rowkey brute", K(ret));
        } else {
          pre_fileter_rowkeys_.reset();
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    } else {
      LOG_INFO("nearest probe center ids is empty", K(ret));
      ret = OB_SUCCESS;
      adaptive_ctx_.is_brute_force_= true;
      float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
      ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
        LOG_WARN("failed to get limit rowkey brute", K(ret));
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    }
  } else if (is_iter_filter()) {
    adaptive_ctx_.iter_times_ = 0;
    ObSEArray<ObIvfRowkeyDistEntry, 16> near_rowkeys;
    int64_t limit_k = limit_param_.limit_ + limit_param_.offset_;
    int64_t iter_cnt = 0;
    int64_t next_nprobe = 0;
    bool iter_end = false;
    bool no_new_near_rowkeys = false;
    int64_t heap_size = get_heap_size(limit_k, selectivity_);
    share::ObVectorCenterClusterHelper<T, ObRowkey> nearest_rowkey_heap(
        vec_op_alloc_, search_vec, dis_type_, dim_, heap_size, similarity_threshold_);
    ObIVFRowkeyDistMap rowkey_dist_map;
    ObIvfRowkeyDistItemCompare head_cmp(dis_type_);
    ObIvfRowkeyDistHeap rowkey_dist_heap(head_cmp);
    if (OB_FAIL(rowkey_dist_map.create(32, lib::ObMemAttr(MTL_ID(), "IVFMap") ))) {
      LOG_WARN("create rowkey dist map fail", K(ret));
    }
    while (OB_SUCC(ret) && ! iter_end && near_rowkeys.count() < limit_k) {
      rowkey_dist_map.reuse();
      ++adaptive_ctx_.iter_times_;
      int32_t start_idx = -1;
      if (OB_FAIL(get_nearest_limit_rowkeys_in_cids<T>(is_vectorized, search_vec, nearest_rowkey_heap, nullptr))) {
        LOG_WARN("failed to get nearest limit rowkeys in cids", K(near_cid_));
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_centers_dist_map(rowkey_dist_map))) {
        LOG_WARN("get dist map fail", K(ret));
      } else if (OB_FALSE_IT(start_idx = near_rowkeys.count())) {
      } else if (OB_FAIL(do_post_filter(is_vectorized, rowkey_dist_map, near_rowkeys))) {
        LOG_WARN("do post filter fail", K(ret), K(near_rowkeys.count()));
      } else if (OB_FALSE_IT(no_new_near_rowkeys = (near_rowkeys.count() == start_idx))) {
        // there are no new rowkeys in near_rowkeys after post filter
      } else {
        for (int64_t i = start_idx; OB_SUCC(ret) && i < near_rowkeys.count(); ++i) {
          const ObIvfRowkeyDistEntry &entry = near_rowkeys.at(i);
          if (OB_FAIL(rowkey_dist_heap.push(ObIvfRowkeyDistItem(i, entry.distance_)))) {
            LOG_WARN("push rowkey dist fail", K(ret), K(i), K(entry));
          } else {
            LOG_TRACE("push", K(entry), K(i), K(limit_k));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (near_rowkeys.count() < limit_k) {
        ++iter_cnt;
        const int64_t left_search = limit_k - near_rowkeys.count();
        next_nprobe = OB_MIN(nprobes_, (nprobes_ * ((double)left_search) / limit_k + 1));
        LOG_INFO("postfilter does not get enough result", K(limit_k), "near_rowkeys_count", near_rowkeys.count(),
            K(selectivity_), K(left_search), K(next_nprobe), K(iter_cnt), K(nprobes_), K(heap_size));
        near_cid_.reuse();
        if (similarity_threshold_ != 0 && no_new_near_rowkeys) {
          iter_end = true;
          LOG_INFO("there are no new rowkeys in near_rowkeys after post filter, stop iterative filter", K(ret), K(no_new_near_rowkeys), K(near_rowkeys.count()), K(start_idx));
        } else if (! iterative_filter_ctx_.has_next_center()) {
          iter_end = true;
        } else if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_center_ids(next_nprobe, near_cid_))) {
          LOG_WARN("get next centers from iterative_filter_ctx fail", K(ret), K(iterative_filter_ctx_),
              K(next_nprobe), K(limit_k), K(left_search), K(iter_cnt), K(nprobes_));
        } else if (0 == near_cid_.count()) {
          iter_end = true;
          LOG_TRACE("there are no centers left to access", K(ret), K(iterative_filter_ctx_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      for(int64_t cnt = 0; OB_SUCC(ret) && cnt < limit_k && ! rowkey_dist_heap.empty(); ++cnt) {
        const ObIvfRowkeyDistItem &item = rowkey_dist_heap.top();
        if (item.rowkey_idx_ > near_rowkeys.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("out of bound of near_rowkeys", K(ret), K(item), K(near_rowkeys.count()));
        } else if (OB_FAIL(saved_rowkeys_.push_back(near_rowkeys.at(item.rowkey_idx_).rowkey_))) {
          LOG_WARN("push back fail", K(ret), K(item));
        } else if (OB_FAIL(rowkey_dist_heap.pop())) {
          LOG_WARN("rowkey_dist_heap pop fail", K(ret), K(saved_rowkeys_.count()), K(item));
        } else {
          LOG_TRACE("result", K(item), "i", cnt, K(limit_k));
        }
      }
    }
  } else if (OB_FAIL(get_nearest_limit_rowkeys_in_cids<T>(is_vectorized,
                                                              search_vec,
                                                              saved_rowkeys_, nullptr))) {
    // 2. get cidx in (top-nprobes 个 cid)
    //    scan cid_vector table, range: [cidx, min] ~ [cidx, max]; sort by l2_distance(vec, search_vec_); limit
    //    (limit+offset); return rowkey column
    LOG_WARN("failed to get nearest limit rowkeys in cids", K(near_cid_));
  }
  return ret;
}

int ObDASIvfScanIter::filter_rowkey_by_cid(bool is_vectorized,
                                           int64_t batch_row_count,
                                           int& push_count)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *rowkey_cid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  ObExpr *cid_expr = rowkey_cid_ctdef->result_output_[0];

  if (!is_vectorized) {
    bool index_end = false;
    bool is_cid_exist = false;
    for (int i = 0; OB_SUCC(ret) && i < batch_row_count && !index_end; ++i) {
      ObString cid;
      if (OB_FAIL(get_cid_from_rowkey_cid_table(cid))) {
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
        index_end = true;
      } else if (OB_FAIL(check_cid_exist(cid, is_cid_exist))) {
        LOG_WARN("failed to check cid exist", K(ret), K(cid));
      } else if (!is_cid_exist) {
        push_count++;
      } else if (OB_FAIL(saved_rowkeys_.push_back(pre_fileter_rowkeys_[push_count++]))) {
        LOG_WARN("failed to add rowkey", K(ret));
      }
    }
    int tmp_ret = ret;
    if (OB_FAIL(
            ObDasVecScanUtils::reuse_iter(ls_id_, rowkey_cid_iter_, rowkey_cid_scan_param_, rowkey_cid_tablet_id_))) {
      LOG_WARN("failed to reuse rowkey cid iter.", K(ret));
    } else {
      ret = tmp_ret;
    }
  } else {
    IVF_GET_NEXT_ROWS_BEGIN(rowkey_cid_iter_)
    if (OB_SUCC(ret)) {
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
      guard.set_batch_size(scan_row_cnt);
      ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
      bool is_cid_exist = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
        guard.set_batch_idx(i);
        ObString cid = cid_datum[i].get_string();
        if (OB_FAIL(check_cid_exist(cid, is_cid_exist))) {
          LOG_WARN("failed to check cid exist", K(ret), K(cid));
        } else if (!is_cid_exist) {
          push_count++;
        } else if (OB_FAIL(saved_rowkeys_.push_back(pre_fileter_rowkeys_[push_count++]))) {
          LOG_WARN("failed to add rowkey", K(ret));
        }
      }
    }
    IVF_GET_NEXT_ROWS_END(rowkey_cid_iter_, rowkey_cid_scan_param_, rowkey_cid_tablet_id_)
  }

  return ret;
}

int ObDASIvfScanIter::filter_pre_rowkey_batch(bool is_vectorized,
                                              int64_t batch_row_count)
{
  int ret = OB_SUCCESS;

  int64_t filted_rowkeys_count = pre_fileter_rowkeys_.count();
  const ObDASScanCtDef *rowkey_cid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  ObExpr *cid_expr = rowkey_cid_ctdef->result_output_[0];

  int count = 0;
  int push_count = 0;

  while (OB_SUCC(ret) && count < filted_rowkeys_count) {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_row_count && count < filted_rowkeys_count; ++i) {
      if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(
              pre_fileter_rowkeys_[count++], rowkey_cid_scan_param_, rowkey_cid_ctdef->ref_table_id_))) {
        LOG_WARN("failed to set lookup key", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_rowkey_cid_table_scan())) {
      LOG_WARN("do rowkey cid table scan failed", K(ret));
    } else if (OB_FAIL(filter_rowkey_by_cid(is_vectorized, batch_row_count, push_count))) {
      LOG_WARN("filter rowkey batch failed", K(ret), K(is_vectorized), K(batch_row_count));
    }
  }

  return ret;
}

int ObDASIvfScanIter::process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_ivf_scan_pre<float>(allocator, is_vectorized, reinterpret_cast<float *>(real_search_vec_.ptr())))) {
    LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
  }
  return ret;
}

template <typename T>
int ObDASIvfScanIter::do_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized, T *search_vec)
{
  int ret = OB_SUCCESS;
  ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
  int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  ObDASScanIter* inv_iter = (ObDASScanIter*)inv_idx_scan_iter_;
  bool is_range_prefilter = vec_aux_ctdef_->can_use_vec_pri_opt();
  ObIvfPreFilter prefilter(MTL_ID());
  if (OB_FAIL(get_rowkey_pre_filter(mem_context_->get_arena_allocator(), is_vectorized, IVF_MAX_BRUTE_FORCE_SIZE))) {
    LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
  } else if (pre_fileter_rowkeys_.count() < IVF_MAX_BRUTE_FORCE_SIZE) {
    // do brute search
    adaptive_ctx_.is_brute_force_= true;
    IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, reinterpret_cast<float *>(real_search_vec_.ptr()), raw_dis_type, dim_,
                                      get_nprobe(limit_param_, 1), similarity_threshold_);
    if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
      LOG_WARN("failed to get limit rowkey brute", K(ret));
    } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
      LOG_WARN("failed to get top n", K(ret));
    }
  } else {
    if (OB_FAIL(get_nearest_probe_center_ids(is_vectorized))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        // cid_center table is empty, just do brute search
        ret = OB_SUCCESS;
        adaptive_ctx_.is_brute_force_= true;
        IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, reinterpret_cast<float *>(real_search_vec_.ptr()) /*unused*/, raw_dis_type, dim_,
                                          get_nprobe(limit_param_, 1), similarity_threshold_);
        bool index_end = false;
        while (OB_SUCC(ret) && !index_end) {
          if (OB_FAIL(get_pre_filter_rowkey_batch(mem_context_->get_arena_allocator(), is_vectorized, batch_row_count,
                                                  index_end))) {
            LOG_WARN("failed to get rowkey batch", K(ret), K(is_vectorized));
          } else if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
            LOG_WARN("failed to get limit rowkey brute", K(ret));
          } else {
            pre_fileter_rowkeys_.reset();
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
          LOG_WARN("failed to get top n", K(ret));
        }
      } else {
        LOG_WARN("failed to get nearest probe center ids", K(ret));
      }
    } else {
      if (is_range_prefilter) { // rowkey range prefilter
        adaptive_ctx_.pre_scan_row_cnt_ += pre_fileter_rowkeys_.count();
        adaptive_ctx_.is_range_prefilter_ = true;
        ObArray<const ObNewRange *> rk_range;
        const ObRangeArray& key_range = inv_iter->get_scan_param().key_ranges_;
        for (int64_t i = 0; i < key_range.count() && OB_SUCC(ret); i++) {
          const ObNewRange *range = &key_range.at(i);
          if (OB_FAIL(rk_range.push_back(range))) {
            LOG_WARN("fail to push back range", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(prefilter.init(rk_range))) {
            LOG_WARN("fail to init prefilter as range filter", K(ret));
          }
        }
      } else if (OB_FAIL(prefilter.init())) { // rowkey hash bitmap prefilter
        LOG_WARN("fail to init prefilter as roaring bitmap", K(ret));
      } else { // add bitmap for bitmap filter
        adaptive_ctx_.pre_scan_row_cnt_ += pre_fileter_rowkeys_.count();
        for (int i = 0; i < pre_fileter_rowkeys_.count() && OB_SUCC(ret); i++) {
          uint64_t hash_val = hash_val_for_rk(pre_fileter_rowkeys_.at(i));
          if (OB_FAIL(prefilter.add(hash_val))) {
            LOG_WARN("fail to add hash val to prefilter", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(build_rowkey_hash_set(prefilter, is_vectorized, batch_row_count))) {
          LOG_WARN("fail to build rk hash set", K(ret));
        }
      }

      // do for loop until saved_rowkeys_.count() >= limitK
      if (OB_SUCC(ret)) {
        int64_t limit_k = limit_param_.limit_ + limit_param_.offset_;
        int64_t iter_cnt = 0;
        int64_t next_nprobe = 0;
        bool iter_end = false;
        bool is_first_scan = true;
        int32_t start_idx = -1;
        bool no_new_near_rowkeys = false;
        share::ObVectorCenterClusterHelper<T, ObRowkey> nearest_rowkey_heap(
            vec_op_alloc_, search_vec, dis_type_, dim_, limit_k, similarity_threshold_);
        while (OB_SUCC(ret) && ! iter_end && nearest_rowkey_heap.count() < limit_k) {
          if (OB_FALSE_IT(start_idx = nearest_rowkey_heap.count())) {
          } else if (OB_FAIL(get_nearest_limit_rowkeys_in_cids<T>(
              is_vectorized,
              search_vec,
              nearest_rowkey_heap,
              &prefilter))) {
            LOG_WARN("fail to calc nearest limit rowkeys in cids", K(ret), K(dim_));
          } else if (OB_FALSE_IT(no_new_near_rowkeys = (nearest_rowkey_heap.count() == start_idx))) {
            // there are no new rowkeys in near_rowkeys after post filter
          }
          if (OB_FAIL(ret)) {
          } else if (nearest_rowkey_heap.count() < limit_k) {
            ++iter_cnt;
            const int64_t left_search = limit_k - nearest_rowkey_heap.count();
            next_nprobe = OB_MIN(nprobes_, (nprobes_ * ((double)left_search) / limit_k + 1));
            LOG_INFO("prefilter does not get enough result", K(limit_k), "nearest_rowkey_heap_count", nearest_rowkey_heap.count(),
                K(selectivity_), K(left_search), K(next_nprobe), K(iter_cnt), K(nprobes_));
            near_cid_.reuse();
            is_first_scan = false;
            if (similarity_threshold_ != 0 && no_new_near_rowkeys) {
              iter_end = true;
              LOG_INFO("there are no new rowkeys in near_rowkeys after post filter, stop iterative filter", K(ret), K(no_new_near_rowkeys), K(nearest_rowkey_heap.count()), K(start_idx), K(limit_k));
            } else if (! iterative_filter_ctx_.has_next_center()) {
              iter_end = true;
            } else if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_center_ids(next_nprobe, near_cid_))) {
              LOG_WARN("get next centers from iterative_filter_ctx fail", K(ret), K(iterative_filter_ctx_),
                  K(next_nprobe), K(limit_k), K(left_search), K(iter_cnt), K(nprobes_));
            } else if (0 == near_cid_.count()) {
              iter_end = true;
              LOG_TRACE("there are no centers left to access", K(ret), K(iterative_filter_ctx_));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
          LOG_WARN("get rowkeys from heap fail", K(ret), K(limit_k));
        } else {
          LOG_TRACE("get rowkeys from heap success", K(limit_k), K(saved_rowkeys_.count()), K(iter_cnt), K(next_nprobe), K(nprobes_));
        }
      }
    }
  }

  return ret;
}

int ObDASIvfScanIter::get_cid_from_rowkey_cid_table(ObString &cid)
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *rowkey_cid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  ObExpr *cid_expr = rowkey_cid_ctdef->result_output_[0];

  rowkey_cid_iter_->clear_evaluated_flag();
  if (OB_FAIL(rowkey_cid_iter_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to scan rowkey cid iter", K(ret));
    }
  } else {
    ObDatum &cid_datum = cid_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
    cid = cid_datum.get_string();
  }

  return ret;
}

int ObDASIvfScanIter::check_cid_exist(const ObString &src_cid, bool &src_cid_exist)
{
  int ret = OB_SUCCESS;
  src_cid_exist = false;
  ObCenterId src_centor_id;
  if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(src_centor_id, src_cid))) {
    LOG_WARN("failed to get center id from string", K(src_cid));
  } else if (OB_UNLIKELY(src_centor_id.center_id_ >= near_cid_dist_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src_cid is not exist", K(ret), K(src_centor_id), K(near_cid_dist_.count()));
  } else if (near_cid_dist_.at(src_centor_id.center_id_)) {
    src_cid_exist = true;
  }

  return ret;
}
/************************************************** ObDASIvfPQScanIter ******************************************************/

int ObDASIvfPQScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam index_param;
  if (OB_FAIL(ObDASIvfBaseScanIter::inner_init(param))) {
    LOG_WARN("fail to do inner init ", K(ret), K(param));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_3) {
    if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(vec_aux_ctdef_->vec_index_param_, ObVectorIndexType::VIT_IVF_INDEX, index_param))) {
      LOG_WARN("fail to parse params from string", K(ret), K(vec_aux_ctdef_->vec_index_param_));
    } 
  } else if (OB_FAIL(index_param.assign(vec_aux_ctdef_->get_vec_index_param()))) {
    LOG_WARN("fail to assign params from vec_aux_ctdef_", K(ret));
  }
  
  if (OB_FAIL(ret)) {
  } else {
    ObDASIvfScanIterParam &ivf_scan_param = static_cast<ObDASIvfScanIterParam &>(param);
    pq_centroid_iter_ = ivf_scan_param.pq_centroid_iter_;
    m_ = vec_index_param_.m_;
    nbits_ = vec_index_param_.nbits_;
    if (dim_ % m_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dim should be able to divide m exactly", K(ret), K(dim_), K(m_));
    } else if (nbits_ < 1 || nbits_ > 24) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nbits should be in range [1,24]", K(ret), K(nbits_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(near_cid_vec_.reserve(nprobes_))) {
    LOG_WARN("failed to reserve nearest cid vec", K(ret), K(vec_index_param_.nlist_));
  } else if (OB_FAIL(near_cid_vec_dis_.reserve(nprobes_))) {
    LOG_WARN("failed to reserve nearest cid vec dis", K(ret), K(vec_index_param_.nlist_));
  } else if (OB_FAIL(near_cid_vec_ptrs_.allocate_array(persist_alloc_, vec_index_param_.nlist_ + 1))) {
    LOG_WARN("failed to reserve nearest cid vec", K(ret), K(vec_index_param_.nlist_));
  } else {
    memset(near_cid_vec_ptrs_.get_data(), 0, near_cid_vec_ptrs_.count() * sizeof(float *));
  }
  return ret;
}

int ObDASIvfPQScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  near_cid_vec_.reuse();
  near_cid_vec_dis_.reuse();
  memset(near_cid_vec_ptrs_.get_data(), 0, near_cid_vec_ptrs_.count() * sizeof(float *));

  if (!pq_centroid_first_scan_ && OB_FAIL(ObDasVecScanUtils::reuse_iter(
                                      ls_id_, pq_centroid_iter_, pq_centroid_scan_param_, pq_centroid_tablet_id_))) {
    LOG_WARN("failed to reuse iter", K(ret));
  } else if (OB_FAIL(ObDASIvfBaseScanIter::inner_reuse())) {
    LOG_WARN("fail to do inner reuse", K(ret));
  }
  return ret;
}

int ObDASIvfPQScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(pq_centroid_iter_) && OB_FAIL(pq_centroid_iter_->release())) {
    LOG_WARN("failed to release pq_centroid_iter_", K(ret));
  }
  pq_centroid_iter_ = nullptr;
  ObDasVecScanUtils::release_scan_param(pq_centroid_scan_param_);
  near_cid_vec_.reset();
  near_cid_vec_dis_.reset();
  near_cid_vec_ptrs_.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDASIvfBaseScanIter::inner_release())) {
    LOG_WARN("fail to do inner release", K(ret));
  }

  return ret;
}

// for pq, con_key is array(vector(float))
int ObDASIvfPQScanIter::parse_pq_ids_vec_datum(
  ObIAllocator &allocator,
  int64_t cid_vec_column_count,
  const ObDASScanCtDef *cid_vec_ctdef,
  const int64_t rowkey_cnt,
  ObRowkey &main_rowkey,
  ObString &com_key)
{
  int ret = OB_SUCCESS;
  ObExpr *pq_ids_expr = cid_vec_ctdef->result_output_[PQ_IDS_IDX];
  
  if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(allocator, cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
    LOG_WARN("failed to get main rowkey from cid vec datum", K(ret));
  } else if (pq_ids_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).is_null()) {
    // do nothing
    com_key.reset();
  } else {
    com_key = pq_ids_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).get_string();
  }
  return ret;
}

int ObDASIvfPQScanIter::calc_distance_between_pq_ids_by_cache(ObIvfCentCache &cent_cache, 
                                                              const ObString &pq_center_ids,
                                                              const ObIArray<float *> &splited_residual,
                                                              float &distance) {
  int ret = OB_SUCCESS;

  RWLock::RLockGuard guard(cent_cache.get_lock());
  float square = 0.0f;
  const uint8_t* pq_ids_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(pq_center_ids.ptr());
  PQDecoderGeneric decoder(pq_ids_ptr, nbits_);
  for (int j = 0; OB_SUCC(ret) && j < m_; ++j) {
    // 3.2.1 pq_center_ids[j] is put into ivf_pq_centroid table to find pq_center_vecs[j]
    float *pq_cid_vec = nullptr;
    if (OB_FAIL(cent_cache.read_pq_centroid(j + 1 /*m*/, decoder.decode() + 1, pq_cid_vec))) {
      LOG_WARN("fail to get pq cid vec from cache", K(ret), K(j + 1));
    } else if (OB_ISNULL(pq_cid_vec)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null pq_cid_vec", 
        K(ret), K(j), KPHEX(pq_center_ids.ptr(), pq_center_ids.length()));
    } else {
      // 3.3.2 Calculate the distance between pq_center_vecs[j] and r(x)[j].
      //       The sum of j = 0 ~ m is the distance from x to rowkey
      float dis = 0.0f;
      if (OB_FAIL(calc_vec_dis<float>(splited_residual.at(j), pq_cid_vec, dim_ / m_, dis, dis_type_))) {
        SHARE_LOG(WARN, "failed to get distance type", K(ret));
      } else {
        square += dis;
      }
    }
  } // end for
  
  if (OB_SUCC(ret)) {
    distance = square;
  }
  return ret;
}

int ObDASIvfPQScanIter::calc_distance_between_pq_ids_by_table(
  bool is_vectorized,
  const ObString &pq_center_ids,
  const ObIArray<float *> &splited_residual,
  int64_t batch_row_count,
  float &distance)
{
  // todo(wmj): need fit ivf adaptor cache，pq_center_ids find pq_center_vec
  int ret = OB_SUCCESS;
  float dis_square = 0.0f;
  const ObDASScanCtDef *pq_cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_pq_id_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN);
  ObDASScanRtDef *pq_cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_pq_id_tbl_idx());
  if (OB_ISNULL(pq_cid_vec_ctdef) || OB_ISNULL(pq_cid_vec_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(pq_cid_vec_ctdef), KP(pq_cid_vec_rtdef));
  } 

  int count = 0;
  int pq_cid_idx = 0;
  int64_t pq_cid_count = m_;
  uint64_t tablet_id = ObVecIVFPQCenterIDS::get_tablet_id(pq_center_ids.ptr());
  const uint8_t* pq_ids_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(pq_center_ids.ptr());
  PQDecoderGeneric decoder(pq_ids_ptr, nbits_);
  while (OB_SUCC(ret) && count < pq_cid_count) {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_row_count && count < pq_cid_count; ++i, ++count) {
      ObRowkey pq_cid_rowkey;
      ObString pq_center_id_str;
      ObPqCenterId pq_center_id(tablet_id, count + 1, decoder.decode() + 1);
      if (OB_FAIL(ObVectorClusterHelper::set_pq_center_id_to_string(pq_center_id, pq_center_id_str,
                                                                    &mem_context_->get_arena_allocator()))) {
        LOG_WARN("fail to set pq center id to string", K(ret), K(pq_center_id));
      } else if (OB_FAIL(build_cid_vec_query_rowkey(pq_center_id_str, true /*is_min*/, CENTROID_PRI_KEY_CNT,
                                             pq_cid_rowkey))) {
        LOG_WARN("failed to build cid vec query rowkey", K(ret));
      } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(pq_cid_rowkey, pq_centroid_scan_param_,
                                                           pq_cid_vec_ctdef->ref_table_id_))) {
        LOG_WARN("failed to set lookup key", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_aux_table_scan(pq_centroid_first_scan_, pq_centroid_scan_param_, pq_cid_vec_ctdef,
                                         pq_cid_vec_rtdef, pq_centroid_iter_, pq_centroid_tablet_id_))) {
      LOG_WARN("fail to rescan cid vec table scan iterator.", K(ret));
    } else if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(pq_centroid_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        bool has_lob_header = pq_cid_vec_ctdef->result_output_.at(PQ_CENTROID_VEC_IDX)->obj_meta_.has_lob_header();
        ObExpr *vec_expr = pq_cid_vec_ctdef->result_output_[PQ_CENTROID_VEC_IDX];
        ObDatum *vec_datum = vec_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);

        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          float cur_dis = 0.0f;
          guard.set_batch_idx(i);
          ObString vec = vec_datum[i].get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                          &mem_context_->get_arena_allocator(),
                          ObLongTextType,
                          CS_TYPE_BINARY,
                          has_lob_header,
                          vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else if (OB_ISNULL(vec.ptr())) {
            // ignoring null vector.
            pq_cid_idx++;
          } else if (OB_FAIL(calc_vec_dis<float>(splited_residual.at(pq_cid_idx), reinterpret_cast<float *>(vec.ptr()),
                                                 dim_ / m_, cur_dis, dis_type_))) {
            SHARE_LOG(WARN, "failed to get distance type", K(ret));
          } else {
            dis_square += cur_dis;
            pq_cid_idx++;
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(pq_centroid_iter_, pq_centroid_scan_param_, pq_centroid_tablet_id_)
    } else {
      pq_centroid_iter_->clear_evaluated_flag();
      bool has_lob_header = pq_cid_vec_ctdef->result_output_.at(PQ_CENTROID_VEC_IDX)->obj_meta_.has_lob_header();
      ObExpr *vec_expr = pq_cid_vec_ctdef->result_output_[PQ_CENTROID_VEC_IDX];
      ObDatum &vec_datum = vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
      for (int i = 0; OB_SUCC(ret) && i < batch_row_count; ++i) {
        float cur_dis = 0.0f;
        if (OB_FAIL(pq_centroid_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to scan vid rowkey iter", K(ret));
          }
        } else {
          ObString vec = vec_datum.get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                          &mem_context_->get_arena_allocator(),
                          ObLongTextType,
                          CS_TYPE_BINARY,
                          has_lob_header,
                          vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else if (OB_ISNULL(vec.ptr())) {
            // ignoring null vector.
            pq_cid_idx++;
          } else if (OB_FAIL(calc_vec_dis<float>(splited_residual.at(pq_cid_idx), reinterpret_cast<float *>(vec.ptr()),
                                                 dim_ / m_, cur_dis, dis_type_))) {
            SHARE_LOG(WARN, "failed to get distance type", K(ret));
          } else {
            dis_square += cur_dis;
            pq_cid_idx++;
          }
        }
      }
      int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;
      if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, pq_centroid_iter_, pq_centroid_scan_param_, pq_centroid_tablet_id_))) { 
        LOG_WARN("failed to reuse rowkey cid iter.", K(ret));
      } else {
        ret = tmp_ret;
      }
    }
  }
  if (pq_cid_idx != pq_cid_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pq cid idx", K(ret), K(pq_cid_idx), K(pq_cid_count));
  } else {
    distance = dis_square;
  }
  return ret;
}

int ObDASIvfPQScanIter::calc_distance_between_pq_ids(
    bool is_vectorized,
    const ObString &pq_center_ids, 
    const ObIArray<float *> &splited_residual, 
    float &distance)
{
  int ret = OB_SUCCESS;
  ObIvfCacheMgrGuard cache_guard;
  ObIvfCentCache *cent_cache = nullptr;
  bool is_cache_usable = false;
  if (OB_FAIL(get_centers_cache(is_vectorized, true/*is_pq_centers*/, cache_guard, cent_cache, is_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(cent_cache));
  } else if (is_cache_usable) {
    if (OB_FAIL(calc_distance_between_pq_ids_by_cache(*cent_cache, pq_center_ids, splited_residual, distance))) {
      LOG_WARN("fail to calc distance between pq ids by cache", K(ret), K(is_vectorized), KPC(cent_cache));
    }
  } else {
    if (OB_FAIL(calc_distance_between_pq_ids_by_table(is_vectorized, 
                                                      pq_center_ids, 
                                                      splited_residual, 
                                                      ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE, 
                                                      distance))) {
      LOG_WARN("fail to calc distance between pq ids by table", K(ret), K(is_vectorized));
    }
  }

  return ret;
}

int ObDASIvfPQScanIter::calc_distance_with_precompute(
    ObEvalCtx::BatchInfoScopeGuard &guard,
    int64_t scan_row_cnt,
    int64_t rowkey_cnt,
    ObRowkey& filter_main_rowkey,
    float *sim_table,
    float dis0,
    IvfRowkeyHeap& nearest_rowkey_heap,
    ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN);
  ObExpr *cid_expr = cid_vec_ctdef->result_output_[PQ_IDS_IDX];
  ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
  int counter = 0;
  size_t saved_j[4] = {0, 0, 0, 0};
  for (int64_t j = 0; OB_SUCC(ret) && j < scan_row_cnt; ++j) {
    bool is_skip = false;
    if (cid_datum[j].is_null()) {
      is_skip = true;
    } else if (prefilter != nullptr) {
      // get rowkey and filter first
      guard.set_batch_idx(j);
      if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, filter_main_rowkey, false))) {
        LOG_WARN("fail to get main rowkey", K(ret));
      } else if (!prefilter->test(filter_main_rowkey)) {
        is_skip = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_skip) {
    } else {
      adaptive_ctx_.vec_dist_calc_cnt_++;
      saved_j[0] = (counter == 0) ? j : saved_j[0];
      saved_j[1] = (counter == 1) ? j : saved_j[1];
      saved_j[2] = (counter == 2) ? j : saved_j[2];
      saved_j[3] = (counter == 3) ? j : saved_j[3];
      counter += 1;
      if (counter == 4) {
        float distance_0 = 0;
        float distance_1 = 0;
        float distance_2 = 0;
        float distance_3 = 0;
        {
          const uint8_t* pq_id_ptr_0 = ObVecIVFPQCenterIDS::get_pq_id_ptr(cid_datum[saved_j[0]].get_string().ptr());
          const uint8_t* pq_id_ptr_1 = ObVecIVFPQCenterIDS::get_pq_id_ptr(cid_datum[saved_j[1]].get_string().ptr());
          const uint8_t* pq_id_ptr_2 = ObVecIVFPQCenterIDS::get_pq_id_ptr(cid_datum[saved_j[2]].get_string().ptr());
          const uint8_t* pq_id_ptr_3 = ObVecIVFPQCenterIDS::get_pq_id_ptr(cid_datum[saved_j[3]].get_string().ptr());
          ObVectorL2Distance<float>::distance_four_codes(
            m_, nbits_, sim_table, pq_id_ptr_0, pq_id_ptr_1, pq_id_ptr_2, pq_id_ptr_3,
            distance_0, distance_1, distance_2, distance_3);
          distance_0 = distance_0 + dis0;
          distance_1 = distance_1 + dis0;
          distance_2 = distance_2 + dis0;
          distance_3 = distance_3 + dis0;
          // 先检查是否需要分配内存，避免不必要的内存分配
          if (nearest_rowkey_heap.should_push_center(distance_0)) {
            ObRowkey main_rowkey;
            {
              guard.set_batch_idx(saved_j[0]);
              {
                if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
                  LOG_WARN("fail to get main rowkey", K(ret));
                } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance_0))) {
                  LOG_WARN("failed to push center.", K(ret));
                }
              }
            }
          }
          if (OB_SUCC(ret) && nearest_rowkey_heap.should_push_center(distance_1)) {
            ObRowkey main_rowkey;
            {
              guard.set_batch_idx(saved_j[1]);
              {
                if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
                  LOG_WARN("fail to get main rowkey", K(ret));
                } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance_1))) {
                  LOG_WARN("failed to push center.", K(ret));
                }
              }
            }
          }
          if (OB_SUCC(ret) && nearest_rowkey_heap.should_push_center(distance_2)) {
            ObRowkey main_rowkey;
            {
              guard.set_batch_idx(saved_j[2]);
              {
                if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
                  LOG_WARN("fail to get main rowkey", K(ret));
                } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance_2))) {
                  LOG_WARN("failed to push center.", K(ret));
                }
              }
            }
          }
          if (OB_SUCC(ret) && nearest_rowkey_heap.should_push_center(distance_3)) {
            ObRowkey main_rowkey;
            {
              guard.set_batch_idx(saved_j[3]);
              {
                if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
                  LOG_WARN("fail to get main rowkey", K(ret));
                } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance_3))) {
                  LOG_WARN("failed to push center.", K(ret));
                }
              }
            }
          }
        }
        counter = 0;
      }
    }
  }
  // process counter left
  for (size_t kk = 0; kk < counter && OB_SUCC(ret); kk++) {
    const uint8_t* pq_id_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(cid_datum[saved_j[kk]].get_string().ptr());
    float dis = dis0 + ObVectorL2Distance<float>::distance_one_code(m_, nbits_, sim_table, pq_id_ptr);
    if (nearest_rowkey_heap.should_push_center(dis)) {
      guard.set_batch_idx(saved_j[kk]);
      ObRowkey main_rowkey;
      {
        if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
          LOG_WARN("fail to get main rowkey", K(ret));
        } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, dis))) {
          LOG_WARN("failed to push center.", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::check_can_pre_compute(
    bool is_vectorized,
    ObIvfCacheMgrGuard &pre_cache_guard,
    ObIvfCentCache *&pre_cent_cache,
    bool &pre_compute_table)
{
  int ret = OB_SUCCESS;
  int64_t ksub = 1L << nbits_;
  pre_compute_table = false;
  if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
    if (OB_FAIL(get_pq_precomputetable_cache(is_vectorized, pre_cache_guard, pre_cent_cache, pre_compute_table))) {
      LOG_WARN("fail to get pq precompute table cache", K(ret), K(is_vectorized), KPC(pre_cent_cache));
    }
  } else if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::DOT) {
    // check pq center cache is ok
    ObIvfCacheMgrGuard pq_center_guard;
    ObIvfCentCache *pq_cent_cache = nullptr;
    bool is_pq_cent_cache_usable = false;
    if (OB_FAIL(get_centers_cache(is_vectorized, true/*is_pq_centers*/, pq_center_guard, pq_cent_cache, is_pq_cent_cache_usable))) {
      LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(pq_cent_cache));
    } else if (is_pq_cent_cache_usable) {
      pre_compute_table = true;
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::calc_nearest_limit_rowkeys_in_cids(
    bool is_vectorized,
    float *search_vec,
    ObSEArray<ObRowkey, 16> &saved_rowkeys,
    ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;
  int64_t sub_dim = dim_ / m_;
  // only scan without filter can reach here
  IvfRowkeyHeap nearest_rowkey_heap(
      vec_op_alloc_, search_vec, dis_type_, sub_dim, get_nprobe(limit_param_, 1), similarity_threshold_);
  if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(is_vectorized, search_vec, nearest_rowkey_heap, prefilter))) {
    LOG_WARN("calc_nearest_limit_rowkeys_in_cids fail", K(ret));
  } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys))) {
    LOG_WARN("failed to get top n", K(ret));
  }
  return ret;
}

int ObDASIvfPQScanIter::calc_nearest_limit_rowkeys_in_cids(
    bool is_vectorized,
    float *search_vec,
    IvfRowkeyHeap &nearest_rowkey_heap,
    ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;

  int64_t sub_dim = dim_ / m_;
  int64_t ksub = 1L << nbits_;
  const ObDASScanCtDef *cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN);
  ObDASScanRtDef *cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx());
  int64_t cid_vec_column_count = 0;
  int64_t cid_vec_pri_key_cnt = 0;
  int64_t rowkey_cnt = 0;
  ObArray<float *> splited_residual;
  int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
  char *buf = nullptr;
  ObString cid_str;
  float *residual = nullptr;
  bool pre_compute_table = false;
  ObIvfCacheMgrGuard pre_cache_guard;
  ObIvfCentCache *pre_cent_cache = nullptr;
  float *sim_table = nullptr;
  float *sim_table_2 = nullptr;
  const float* sim_table_ptrs = nullptr;
  ObRowkey filter_main_rowkey;
  bool is_l2 = (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN);
  if (OB_FAIL(prepare_cid_range(cid_vec_ctdef, cid_vec_column_count, cid_vec_pri_key_cnt, rowkey_cnt))) {
    LOG_WARN("fail to prepare cid range", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(mem_context_->get_arena_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc cid", K(ret));
  } else if (OB_FALSE_IT(cid_str.assign_buffer(buf, buf_len))) {
  } else if (OB_FAIL(check_can_pre_compute(is_vectorized, pre_cache_guard, pre_cent_cache, pre_compute_table))) {
    LOG_WARN("fail to check can use precomputetable", K(ret));
  } else if (!pre_compute_table) {
    char *residual_buf = nullptr;
    if (OB_FAIL(splited_residual.reserve(m_))) {
      LOG_WARN("fail to init splited residual array", K(ret), K(m_));
    } else if (is_l2) { // only L2 need residual
      if (OB_ISNULL(residual_buf = static_cast<char*>(mem_context_->get_arena_allocator().alloc(dim_ * sizeof(float))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc residual buf", K(ret));
      } else if (OB_FALSE_IT(residual = new(residual_buf) float[dim_])) {
      }
    }
  } else { // scan by precompute table cache
    ObObj *obj_ptr = nullptr;
    sim_table = (float*)mem_context_->get_arena_allocator().alloc(sizeof(float) * (ksub * m_) * 2);
    obj_ptr = (ObObj*)mem_context_->get_arena_allocator().alloc(sizeof(ObObj) * rowkey_cnt);
    if (OB_ISNULL(sim_table) || OB_ISNULL(obj_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc residual buf", K(ret), K(ksub), K(m_));
    } else {
      filter_main_rowkey.assign(obj_ptr, rowkey_cnt);
      sim_table_2 = sim_table + (ksub * m_);
      if (is_l2 && OB_FAIL(pre_compute_inner_prod_table(search_vec, sim_table_2, is_vectorized))) {
        LOG_WARN("fail to pre compute inner prod table", K(ret));
      } else if (!is_l2 && OB_FAIL(pre_compute_inner_prod_table(search_vec, sim_table, is_vectorized))) {
        LOG_WARN("fail to pre compute inner prod table", K(ret));
      }
    }
  }
  // 1. for every (cid, cid_vec),
  for (int i = 0; OB_SUCC(ret) && i < near_cid_vec_.count(); ++i) {
    const ObCenterId &cur_cid = near_cid_vec_.at(i).first;
    float *cur_cid_vec = near_cid_vec_.at(i).second;
    float dis0 = 0.0f;
    if (pre_compute_table) {
      if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
        sim_table_ptrs = pre_cent_cache->get_centroids() + (cur_cid.center_id_ - 1) * ksub * m_;
        ObVectorL2Distance<float>::fvec_madd(m_ * ksub,
                                             sim_table_ptrs,
                                             -2.0,
                                             sim_table_2,
                                             sim_table);
      }
      dis0 = near_cid_vec_dis_.at(i);
    } else {
      // 1.1 Calculate the residual r(x) = x - cid_vec
      //     split r(x) into m parts, the jth part is called r(x)[j]
      splited_residual.reuse();
      if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
        if (OB_FAIL(ObVectorIndexUtil::calc_residual_vector(dim_, search_vec, cur_cid_vec, residual))) {
          LOG_WARN("fail to calc residual vector", K(ret), K(dim_));
        } 
      } else {
        dis0 = near_cid_vec_dis_.at(i);
        residual = search_vec; // ip dis = dis0 + search_vec · pq_vec
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to calc residual vector", K(ret), K(dim_));
      } else if (OB_FAIL(ObVectorIndexUtil::split_vector(m_, dim_, residual, splited_residual))) {
        LOG_WARN("fail to split vector", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      // 1.2 cid put the query in the ivf_pq_code table to find (rowkey, pq_center_ids)
      storage::ObTableScanIterator *cid_vec_scan_iter = nullptr;
      if (OB_FALSE_IT(cid_str.assign_buffer(buf, buf_len))) {
      } else if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(cur_cid, cid_str))) {
        LOG_WARN("failed to set center_id to string", K(ret), K(cur_cid), K(cid_str));
      } else if (OB_FAIL(scan_cid_range(cid_str, cid_vec_pri_key_cnt, cid_vec_ctdef, cid_vec_rtdef, cid_vec_scan_iter))) {
        LOG_WARN("fail to scan cid range", K(ret), K(cur_cid), K(cid_vec_pri_key_cnt));
      } else if (is_vectorized) {
        IVF_GET_NEXT_ROWS_BEGIN(cid_vec_iter_)
        if (OB_SUCC(ret)) {
          ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
          guard.set_batch_size(scan_row_cnt);
          ObExpr *cid_expr = cid_vec_ctdef->result_output_[PQ_IDS_IDX];
          ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
          adaptive_ctx_.cid_vec_scan_rows_ += scan_row_cnt;

          if (pre_compute_table) {
            if (OB_FAIL(calc_distance_with_precompute(guard, scan_row_cnt, rowkey_cnt, filter_main_rowkey,
                                                      sim_table, dis0, nearest_rowkey_heap, prefilter))) {
              LOG_WARN("fail to calc distance with pre compute table", K(ret));
            }
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < scan_row_cnt; ++j) {
              bool is_skip = false;
              guard.set_batch_idx(j);
              if (cid_datum[j].is_null() || cid_datum[j].get_string().empty()) {
                // do nothing
              } else {
                ObRowkey main_rowkey;
                ObString pq_center_ids = cid_datum[j].get_string();
                float distance = 0;
                if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
                  LOG_WARN("fail to get main rowkey", K(ret));
                } else if (prefilter != nullptr && !prefilter->test(main_rowkey)) {
                  // has been filter, do nothing
                } else if (OB_FAIL(calc_distance_between_pq_ids(is_vectorized, pq_center_ids, splited_residual, distance))) {
                  LOG_WARN("fail to calc distance between pq ids", K(ret));
                } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance + dis0))) {
                  LOG_WARN("failed to push center.", K(ret));
                } else {
                  adaptive_ctx_.vec_dist_calc_cnt_++;
                }
              }
            }
          }
        }
        IVF_GET_NEXT_ROWS_END(cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_)
      } else {
        while (OB_SUCC(ret)) {
          ObRowkey main_rowkey;
          ObString vec_arr_str;
          ObString pq_center_ids;
          float distance = 0.0f;
          adaptive_ctx_.cid_vec_scan_rows_++;
          // cid_vec_scan_iter output: [IVF_CID_VEC_CID_COL IVF_CID_VEC_VECTOR_COL ROWKEY]
          if (OB_FAIL(cid_vec_scan_iter->get_next_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to scan vid rowkey iter", K(ret));
            }
          } else if (OB_FAIL(parse_pq_ids_vec_datum(
              mem_context_->get_arena_allocator(), 
              cid_vec_column_count, 
              cid_vec_ctdef, 
              rowkey_cnt,
              main_rowkey, 
              pq_center_ids))) {
            LOG_WARN("fail to parse cid vec datum", K(ret), K(cid_vec_column_count), K(rowkey_cnt));
          } else if (pq_center_ids.empty()) {
            // ignore null arr
          } else if (prefilter != nullptr && !prefilter->test(main_rowkey)) {
            // has been filter, do nothing
          } else {
            if (pre_compute_table) {
              const uint8_t* pq_id_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(pq_center_ids.ptr());
              distance = ObVectorL2Distance<float>::distance_one_code(m_, nbits_, sim_table, pq_id_ptr);
            } else if (OB_FAIL(calc_distance_between_pq_ids(is_vectorized, pq_center_ids, splited_residual, distance))) {
              LOG_WARN("fail to calc distance between pq ids", K(ret));
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance + dis0))) {
              LOG_WARN("failed to push center.", K(ret));
            } else {
              adaptive_ctx_.vec_dist_calc_cnt_++;
            }
          }
        } // end while

        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
          if (OB_FAIL(cid_vec_iter_->reuse())) {
            LOG_WARN("fail to reuse scan iterator.", K(ret));
          }
        }
      }
    }
  } // end for i
  return ret;
}

int ObDASIvfPQScanIter::get_nearest_probe_centers(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  //precompute table use euclidean_squared, so we need to convert to euclidean_squared
  //L2_squared = dis0^2 + precomcute.result
  ObExprVectorDistance::ObVecDisType cur_dis_type = dis_type_;
  if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
    cur_dis_type = oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED;
  }
  share::ObVectorCenterClusterHelper<float, ObCenterId> nearest_cid_heap(
      mem_context_->get_arena_allocator(), reinterpret_cast<const float *>(real_search_vec_.ptr()),
      cur_dis_type, dim_, nprobes_, 0.0, (is_pre_filter() || is_iter_filter()), &iterative_filter_ctx_.allocator_);
  if (OB_FAIL(generate_nearest_cid_heap(is_vectorized, nearest_cid_heap, true/*save_center_vec*/))) {
    LOG_WARN("failed to generate nearest cid heap", K(ret), K(nprobes_), K(dim_), K(real_search_vec_));
  } else {
    if (nearest_cid_heap.get_center_count() == 0) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (nearest_cid_heap.is_save_all_center()) {
      if (OB_FAIL(nearest_cid_heap.get_all_centroids(iterative_filter_ctx_.centroids_))) {
        LOG_WARN("init_centroids fail", K(ret));
      } else if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_centers_vec_dist(nprobes_, near_cid_vec_, near_cid_vec_dis_))) {
        LOG_WARN("failed to get top n", K(ret), K(iterative_filter_ctx_));
      }
    } else { // whatever pre or post, go here
      if (OB_FAIL(nearest_cid_heap.get_nearest_probe_centers_vec_dist(near_cid_vec_, near_cid_vec_dis_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::get_rowkey_brute_post(bool is_vectorized, IvfRowkeyHeap& nearest_rowkey_heap)
{
  int ret = OB_SUCCESS;

  float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
  ObString raw_search_vec;
  const ObDASScanCtDef *brute_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_brute_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
  ObDASScanRtDef *brute_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_brute_tbl_idx());
  int64_t main_rowkey_cnt = 0;
  // prepare norm info
  ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
  ObVectorNormalizeInfo norm_info;
  if (OB_ISNULL(brute_ctdef) || OB_ISNULL(brute_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(brute_ctdef), KP(brute_rtdef));
  } else if (OB_FALSE_IT(main_rowkey_cnt = brute_ctdef->table_param_.get_read_info().get_schema_rowkey_count())) {
  } else if (OB_UNLIKELY(main_rowkey_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret));
  } else if (need_norm_) {
    if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(mem_context_->get_arena_allocator(), sort_rtdef_, search_vec_,
                                                       raw_search_vec))) {
      LOG_WARN("failed to get real search vec", K(ret));
    } else if (OB_FALSE_IT(search_vec = reinterpret_cast<float *>(raw_search_vec.ptr()))) {
    }
  }
  int count = 0;
  int batch_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  int search_count = (pre_fileter_rowkeys_.count() > 0) ? pre_fileter_rowkeys_.count() : 1;
  while (OB_SUCC(ret) && count < search_count) {
    if (pre_fileter_rowkeys_.count() > 0) {
      // setup lookup key range
      brute_scan_param_.key_ranges_.reset(); // clean key range array
      for (int i = 0; OB_SUCC(ret) && i < batch_count && count < search_count; i++, count++) {
        ObNewRange rk_range;
        if (OB_FAIL(rk_range.build_range(brute_ctdef->ref_table_id_, pre_fileter_rowkeys_.at(count)))) {
          LOG_WARN("fail to build key range", K(ret));
        } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_range(rk_range, brute_scan_param_, brute_ctdef->ref_table_id_))) {
          LOG_WARN("failed to append scan range", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(do_aux_table_scan(brute_first_scan_,
                                          brute_scan_param_,
                                          brute_ctdef,
                                          brute_rtdef,
                                          brute_iter_,
                                          brute_tablet_id_))) {
        LOG_WARN("fail to rescan brute table scan iterator.", K(ret));
      }
    } else {
      count = search_count;
      if (OB_FAIL(do_table_full_scan(is_vectorized,
                                     brute_ctdef,
                                     brute_rtdef,
                                     brute_iter_,
                                     0, /* not used */
                                     brute_tablet_id_,
                                     brute_first_scan_,
                                     brute_scan_param_))) {
        LOG_WARN("failed to do centroid table scan", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(brute_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        ObExpr *vec_expr = brute_ctdef->result_output_[DATA_VECTOR_IDX];
        ObDatum *vec_datum = vec_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);

        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          ObString c_vec = vec_datum[i].get_string();
          if (vec_datum[i].is_null()) {
            // do nothing for null vector
          } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                             &mem_context_->get_arena_allocator(),
                             ObLongTextType,
                             CS_TYPE_BINARY,
                             vec_expr->obj_meta_.has_lob_header(),
                             c_vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else { // dis for l2
            float distance = 0.0f;
            ObRowkey main_rowkey;
            if (OB_FAIL(calc_vec_dis<float>(reinterpret_cast<float *>(c_vec.ptr()), search_vec, dim_, distance, raw_dis_type))) {
              LOG_WARN("fail to calc vec dis", K(ret), K(dim_));
            } else if (OB_FAIL(get_main_rowkey_brute(mem_context_->get_arena_allocator(), brute_ctdef, main_rowkey_cnt, main_rowkey))) {
              LOG_WARN("fail to get main rowkey", K(ret));
            } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance))) {
              LOG_WARN("failed to push center.", K(ret));
            }
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(brute_iter_, brute_scan_param_, brute_tablet_id_)
    } else {
      brute_iter_->clear_evaluated_flag();
      ObExpr *vec_expr = brute_ctdef->result_output_[DATA_VECTOR_IDX];
      ObDatum &vec_datum = vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
      while (OB_SUCC(ret)) {
        if (OB_FAIL(brute_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row failed.", K(ret));
          }
        } else if (vec_datum.is_null()) {
          // do nothing
        } else {
          ObString c_vec = vec_datum.get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                      &mem_context_->get_arena_allocator(),
                      ObLongTextType,
                      CS_TYPE_BINARY,
                      vec_expr->obj_meta_.has_lob_header(),
                      c_vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else {
            float distance = 0.0f;
            ObRowkey main_rowkey;
            if (OB_FAIL(calc_vec_dis<float>(reinterpret_cast<float *>(c_vec.ptr()), search_vec, dim_, distance, raw_dis_type))) {
              LOG_WARN("fail to calc vec dis", K(ret), K(dim_));
            } else if (OB_FAIL(get_main_rowkey_brute(mem_context_->get_arena_allocator(), brute_ctdef, main_rowkey_cnt, main_rowkey))) {
              LOG_WARN("fail to get main rowkey", K(ret));
            } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance))) {
              LOG_WARN("failed to push center.", K(ret));
            }
          }
        }
      } // end while
      int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;
      if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, brute_iter_, brute_scan_param_, brute_tablet_id_))) { 
        LOG_WARN("failed to reuse rowkey cid iter.", K(ret));
      } else {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObDASIvfBaseScanIter::get_main_rowkey_brute(
  ObIAllocator &allocator,
  const ObDASScanCtDef *brute_ctdef,
  const int64_t rowkey_cnt,
  ObRowkey &main_rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;

  if (OB_ISNULL(brute_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(brute_ctdef));
  } else {
    // brute_iter output: [DATA_VECTOR ROWKEY]
    // Note: when _enable_defensive_check = 2, cid_vec_out_exprs is [DATA_VECTOR ROWKEY DEFENSE_CHECK_COL]
    const ExprFixedArray& brute_out_exprs = brute_ctdef->result_output_;
    if (rowkey_cnt > brute_out_exprs.count() - 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey_cnt is illegal", K(ret), K(rowkey_cnt), K(brute_out_exprs.count()));
    } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(rowkey_cnt));
    } else if (OB_FALSE_IT(obj_ptr = new (buf) ObObj[rowkey_cnt])) {
    } else {
      int rowkey_idx = 0;
      for (int64_t i = 1; OB_SUCC(ret) && i < brute_out_exprs.count() && rowkey_idx < rowkey_cnt; ++i) {
        ObObj tmp_obj;
        ObExpr *expr = brute_out_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret));
        } else if (OB_FAIL(expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret));
        } else if (OB_FALSE_IT(obj_ptr[rowkey_idx++] = tmp_obj)) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      main_rowkey.assign(obj_ptr, rowkey_cnt);
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::process_ivf_scan_post(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  const int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  // 1. Scan the ivf_centroid table, calculate the distance between vec_x and cid_vec, 
  //    and get the nearest cluster center (cid 1, cid_vec 1)... (cid n, cid_vec n)
  if (OB_FAIL(get_nearest_probe_centers(is_vectorized))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("failed to get nearest probe center ids", K(ret));
    } else if (is_adaptive_filter()) {
      LOG_INFO("nearest probe center ids is empty", K(ret));
      ret = OB_SUCCESS;
      adaptive_ctx_.is_brute_force_= true;
      float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
      ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      bool index_end = false;
      while (OB_SUCC(ret) && !index_end) {
        if (OB_FAIL(get_pre_filter_rowkey_batch(mem_context_->get_arena_allocator(), is_vectorized, batch_row_count,
                                                index_end))) {
          LOG_WARN("failed to get rowkey batch", K(ret), K(is_vectorized));
        } else if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
          LOG_WARN("failed to get limit rowkey brute", K(ret));
        } else {
          pre_fileter_rowkeys_.reset();
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    } else {
      LOG_INFO("nearest probe center ids is empty", K(ret));
      ret = OB_SUCCESS;
      adaptive_ctx_.is_brute_force_= true;
      float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
      ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
        LOG_WARN("failed to get limit rowkey brute", K(ret));
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    }
  } else if (is_iter_filter()) {
    adaptive_ctx_.iter_times_ = 0;
    ObSEArray<ObIvfRowkeyDistEntry, 16> near_rowkeys;
    int64_t limit_k = limit_param_.limit_ + limit_param_.offset_;
    int64_t iter_cnt = 0;
    int64_t next_nprobe = 0;
    bool iter_end = false;
    bool no_new_near_rowkeys = false;
    float* search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
    const int64_t sub_dim = dim_ / m_;
    int64_t enlargement_factor = (selectivity_ != 0 && selectivity_ != 1 && is_iter_filter()) ? POST_ENLARGEMENT_FACTOR : 1;
    int64_t heap_size = get_heap_size(limit_k, selectivity_);
    ObExprVectorDistance::ObVecDisType cur_dis_type = dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN ? oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED : dis_type_;
    IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, cur_dis_type, sub_dim, heap_size, similarity_threshold_);
    ObIVFRowkeyDistMap rowkey_dist_map;
    ObIvfRowkeyDistItemCompare head_cmp(cur_dis_type);
    ObIvfRowkeyDistHeap rowkey_dist_heap(head_cmp);
    if (OB_FAIL(rowkey_dist_map.create(32, lib::ObMemAttr(MTL_ID(), "IVFMap") ))) {
      LOG_WARN("create rowkey dist map fail", K(ret));
    }
    while (OB_SUCC(ret) && ! iter_end && near_rowkeys.count() < limit_k) {
      rowkey_dist_map.reuse();
      ++adaptive_ctx_.iter_times_;
      int32_t start_idx = -1;
      if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(
          is_vectorized,
          search_vec,
          nearest_rowkey_heap,
          nullptr))) {
        LOG_WARN("fail to calc nearest limit rowkeys in cids", K(ret), K(dim_));
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_centers_dist_map(rowkey_dist_map))) {
        LOG_WARN("get dist map fail", K(ret));
      } else if (OB_FALSE_IT(start_idx = near_rowkeys.count())) {
      } else if (OB_FAIL(do_post_filter(is_vectorized, rowkey_dist_map, near_rowkeys))) {
        LOG_WARN("do post filter fail", K(ret), K(near_rowkeys.count()));
      } else if (OB_FALSE_IT(no_new_near_rowkeys = (near_rowkeys.count() == start_idx))) {
        // there are no new rowkeys in near_rowkeys after post filter
      } else {
        for (int64_t i = start_idx; OB_SUCC(ret) && i < near_rowkeys.count(); ++i) {
          const ObIvfRowkeyDistEntry &entry = near_rowkeys.at(i);
          if (OB_FAIL(rowkey_dist_heap.push(ObIvfRowkeyDistItem(i, entry.distance_)))) {
            LOG_WARN("push rowkey dist fail", K(ret), K(i), K(entry));
          } else {
            LOG_TRACE("push", K(entry), K(i), K(limit_k));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (near_rowkeys.count() < limit_k) {
        ++iter_cnt;
        const int64_t left_search = limit_k - near_rowkeys.count();
        next_nprobe = OB_MIN(nprobes_, (nprobes_ * ((double)left_search) / limit_k + 1));
        LOG_INFO("postfilter does not get enough result", K(limit_k), "near_rowkeys_count", near_rowkeys.count(),
            K(selectivity_), K(left_search), K(next_nprobe), K(iter_cnt), K(nprobes_), K(heap_size));
        near_cid_vec_.reuse();
        near_cid_vec_dis_.reuse();
        if (similarity_threshold_ != 0 && no_new_near_rowkeys) {
          iter_end = true;
          LOG_INFO("there are no new rowkeys in near_rowkeys after post filter, stop iterative filter", K(ret), K(no_new_near_rowkeys), K(near_rowkeys.count()), K(start_idx));
        } else if (! iterative_filter_ctx_.has_next_center()) {
          iter_end = true;
        } else if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_centers_vec_dist(next_nprobe, near_cid_vec_, near_cid_vec_dis_))) {
          LOG_WARN("get next centers from iterative_filter_ctx fail", K(ret), K(iterative_filter_ctx_),
              K(next_nprobe), K(limit_k), K(left_search), K(iter_cnt), K(nprobes_));
        } else if (near_cid_vec_.count() != near_cid_vec_dis_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("near_cid_vec count is not equal to near_cid_vec_dis count", K(ret),
              "near_cid_vec_count", near_cid_vec_.count(), "near_cid_vec_dis_count", near_cid_vec_dis_.count());
        } else if (0 == near_cid_vec_dis_.count()) {
          iter_end = true;
          LOG_TRACE("there are no centers left to access", K(ret), K(iterative_filter_ctx_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      for(int64_t cnt = 0; OB_SUCC(ret) && cnt < limit_k && ! rowkey_dist_heap.empty(); ++cnt) {
        const ObIvfRowkeyDistItem &item = rowkey_dist_heap.top();
        if (item.rowkey_idx_ > near_rowkeys.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("out of bound of near_rowkeys", K(ret), K(item), K(near_rowkeys.count()));
        } else if (OB_FAIL(saved_rowkeys_.push_back(near_rowkeys.at(item.rowkey_idx_).rowkey_))) {
          LOG_WARN("push back fail", K(ret), K(item));
        } else if (OB_FAIL(rowkey_dist_heap.pop())) {
          LOG_WARN("rowkey_dist_heap pop fail", K(ret), K(saved_rowkeys_.count()), K(item));
        } else {
          LOG_TRACE("result", K(item), "i", cnt, K(limit_k));
        }
      }
    }
  } else if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(
      is_vectorized,
      reinterpret_cast<float *>(real_search_vec_.ptr()),
      saved_rowkeys_,
      nullptr))) {
    // 2. search nearest rowkeys
    LOG_WARN("fail to calc nearest limit rowkeys in cids", K(ret), K(dim_));
  }

  return ret;
}

// NOTICE: shadow copy from expr memory
int ObDASIvfBaseScanIter::get_main_rowkey(
    const ObDASScanCtDef *ctdef,
    ObRowkey &main_rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;

  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(ctdef));
  } else {
    const int64_t rowkey_cnt = main_rowkey.get_obj_cnt();
    const ExprFixedArray& rowkey_exprs = ctdef->rowkey_exprs_;
    if (rowkey_exprs.count() != rowkey_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("out expr is not enough for rowkey", K(ret), K(rowkey_cnt), K(rowkey_exprs.count()));
    } else {
      ObObj *obj_ptr = main_rowkey.get_obj_ptr();
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
        ObExpr *expr = rowkey_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret), K(i));
        } else if (OB_FAIL(expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).to_obj(obj_ptr[i], expr->obj_meta_, expr->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret), K(i), KPC(expr));
        }
      }
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::get_cid_from_pq_rowkey_cid_table(ObIAllocator &allocator, ObString &cid, ObString &pq_cids)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *rowkey_cid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  ObExpr *cid_expr = rowkey_cid_ctdef->result_output_[0];
  ObExpr *pq_cids_expr = rowkey_cid_ctdef->result_output_[1];

  rowkey_cid_iter_->clear_evaluated_flag();
  if (OB_FAIL(rowkey_cid_iter_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to scan rowkey cid iter", K(ret));
    }
  } else {
    pq_cids = pq_cids_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).get_string();
    ObDatum &cid_datum = cid_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
    cid = cid_datum.get_string();
  }
  return ret;
}

// if cid not exist, center_vec is nullptr
int ObDASIvfPQScanIter::check_cid_exist(
    const ObString &src_cid,
    float *&center_vec,
    bool &src_cid_exist)
{
  int ret = OB_SUCCESS;
  src_cid_exist = false;
  ObCenterId src_centor_id;
  center_vec = nullptr;
  if (OB_UNLIKELY(near_cid_vec_ptrs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("near_cid_vec_ptrs_ is empty", K(ret));
  } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(src_centor_id, src_cid))) {
    LOG_WARN("failed to get center id from string", K(src_cid));
  } else if (OB_UNLIKELY(src_centor_id.center_id_ >= near_cid_vec_ptrs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src_cid is not exist", K(ret), K(src_centor_id), K(near_cid_vec_ptrs_.count()));
  } else if (OB_NOT_NULL(near_cid_vec_ptrs_.at(src_centor_id.center_id_))) {
    src_cid_exist = true;
    center_vec = near_cid_vec_ptrs_.at(src_centor_id.center_id_);
  }

  return ret;
}

int ObDASIvfPQScanIter::calc_adc_distance(
    bool is_vectorized,
    const ObString &cid,
    const ObString &pq_center_ids,
    IvfRowkeyHeap &rowkey_heap,
    ObArray<float *> &splited_residual,
    float *residual,
    int &push_count)
{
  int ret = OB_SUCCESS;

  float *cur_cid_vec = nullptr;
  bool is_cid_exist = false;
  float distance = 0.0f;
  float dis0 = 0.0f;
  if (OB_FALSE_IT(splited_residual.reuse())) {
    LOG_WARN("fail to init splited residual array", K(ret), K(m_));
  } else if (OB_FAIL(check_cid_exist(cid, cur_cid_vec, is_cid_exist))) {
    // 2. Compare the cid and output the equivalent (rowkey, cid, cid_vec, pq_center_ids).
    LOG_WARN("fail to check cid exist", K(ret), K(cid));
  } else if (!is_cid_exist) {
    push_count++;
  }
  // 3. For each (rowkey, cid, cid_vec, pq_center_ids) :
  // 3.1 Calculate the residual r(x) = x - cid_vec
  //     split r(x) into m parts, the jth part is called r(x)[j]
  else {
    if (dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
      if (OB_FAIL(ObVectorIndexUtil::calc_residual_vector(
              dim_, reinterpret_cast<const float *>(real_search_vec_.ptr()), cur_cid_vec, residual))) {
        LOG_WARN("fail to calc residual vector", K(ret), K(dim_));
      }
    } else {
      if (OB_FAIL(calc_vec_dis<float>(reinterpret_cast<float *>(real_search_vec_.ptr()), cur_cid_vec, dim_, dis0, dis_type_))) {
        LOG_WARN("fail to calc vec dis", K(ret), K(dim_));
      } else {
        residual = reinterpret_cast<float *>(real_search_vec_.ptr());
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObVectorIndexUtil::split_vector(m_, dim_, residual, splited_residual))) {
      LOG_WARN("fail to split vector", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calc_distance_between_pq_ids(is_vectorized, pq_center_ids, splited_residual, distance))) {
    LOG_WARN("fail to calc distance between pq ids", K(ret));
  } else if (OB_FALSE_IT(distance += dis0)) {
  } else if (OB_FAIL(rowkey_heap.push_center(pre_fileter_rowkeys_[push_count++], distance))) {
    LOG_WARN("failed to push center.", K(ret));
  }
  return ret;
}

int ObDASIvfPQScanIter::filter_rowkey_by_cid(bool is_vectorized, int64_t batch_row_count, IvfRowkeyHeap &rowkey_heap,
                                             int &push_count)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *rowkey_cid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);

  ObArray<float *> splited_residual;
  float *residual = nullptr;
  if (OB_FAIL(splited_residual.reserve(m_))) {
    LOG_WARN("fail to init splited residual array", K(ret), K(m_));
  } else {
    char *residual_buf = nullptr;
    if (OB_ISNULL(residual_buf = static_cast<char*>(mem_context_->get_arena_allocator().alloc(dim_ * sizeof(float))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc residual buf", K(ret));
    } else if (OB_FALSE_IT(residual = new(residual_buf) float[dim_])) {
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    if (!is_vectorized) {
      bool index_end = false;
      for (int i = 0; OB_SUCC(ret) && i < batch_row_count && !index_end; ++i) {
        ObString cid;
        ObString pq_center_ids;
        // 1. ivf_pq_rowkey_cid table: Querying the (cid, pq_center_ids) corresponding to the rowkey in the primary table
        if (OB_FAIL(get_cid_from_pq_rowkey_cid_table(mem_context_->get_arena_allocator(), cid, pq_center_ids))) {
          ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
          index_end = true;
        } else if (OB_FAIL(calc_adc_distance(is_vectorized, cid, pq_center_ids, rowkey_heap, splited_residual, residual, push_count))) {
          LOG_WARN("fail to calc adc distance", K(ret), K(cid), K(i));
        }
      } // end for
      int tmp_ret = ret;
      if (OB_FAIL(
              ObDasVecScanUtils::reuse_iter(ls_id_, rowkey_cid_iter_, rowkey_cid_scan_param_, rowkey_cid_tablet_id_))) {
        LOG_WARN("failed to reuse rowkey cid iter.", K(ret));
      } else {
        ret = tmp_ret;
      }
    } else {
      IVF_GET_NEXT_ROWS_BEGIN(rowkey_cid_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        ObExpr *cid_expr = rowkey_cid_ctdef->result_output_[CIDS_IDX];
        ObExpr *pq_cids_expr = rowkey_cid_ctdef->result_output_[PQ_IDS_IDX];
        ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
        ObDatum *pq_cid_datum = pq_cids_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
  
        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          ObString cid = cid_datum[i].get_string();
          ObString pq_center_ids = pq_cid_datum[i].get_string();
          if (OB_FAIL(calc_adc_distance(is_vectorized, cid, pq_center_ids, rowkey_heap, splited_residual, residual, push_count))) {
            LOG_WARN("fail to calc adc distance", K(ret), K(cid), K(i));
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(rowkey_cid_iter_, rowkey_cid_scan_param_, rowkey_cid_tablet_id_)
    }
  }

  return ret;
}

int ObDASIvfPQScanIter::filter_pre_rowkey_batch(bool is_vectorized, int64_t batch_row_count,
                                                IvfRowkeyHeap &rowkey_heap)
{
  int ret = OB_SUCCESS;

  int64_t filted_rowkeys_count = pre_fileter_rowkeys_.count();
  const ObDASScanCtDef *rowkey_cid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  ObExpr *cid_expr = rowkey_cid_ctdef->result_output_[0];

  int count = 0;
  int push_count = 0;

  while (OB_SUCC(ret) && count < filted_rowkeys_count) {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_row_count && count < filted_rowkeys_count; ++i) {
      if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(
              pre_fileter_rowkeys_[count++], rowkey_cid_scan_param_, rowkey_cid_ctdef->ref_table_id_))) {
        LOG_WARN("failed to set lookup key", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_rowkey_cid_table_scan())) {
      LOG_WARN("do rowkey cid table scan failed", K(ret));
    } else if (OB_FAIL(filter_rowkey_by_cid(is_vectorized, batch_row_count, rowkey_heap, push_count))) {
      LOG_WARN("filter rowkey batch failed", K(ret), K(is_vectorized), K(batch_row_count));
    }
  }

  return ret;
}

int ObDASIvfPQScanIter::process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
  ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;

  ObDASScanIter* inv_iter = (ObDASScanIter*)inv_idx_scan_iter_;
  bool is_range_prefilter = vec_aux_ctdef_->can_use_vec_pri_opt();
  ObIvfPreFilter prefilter(MTL_ID());
  // 1. Scan the ivf_centroid table, calculate the distance between vec_x and cid_vec, 
  //    and get the nearest cluster center (cid 1, cid_vec 1)... (cid n, cid_vec n)
  if (OB_FAIL(get_nearest_probe_centers(is_vectorized))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      // cid_center table is empty, just do brute search
      ret = OB_SUCCESS;
      adaptive_ctx_.is_brute_force_= true;
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      bool index_end = false;
      while (OB_SUCC(ret) && !index_end) {
        if (OB_FAIL(get_pre_filter_rowkey_batch(mem_context_->get_arena_allocator(), is_vectorized, batch_row_count,
                                                index_end))) {
          LOG_WARN("failed to get rowkey batch", K(ret), K(is_vectorized));
        } else if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
          LOG_WARN("failed to get limit rowkey brute", K(ret));
        } else {
          pre_fileter_rowkeys_.reset();
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    } else {
      LOG_WARN("failed to get nearest probe center ids", K(ret));
    }
  } else {
    if (OB_FAIL(get_rowkey_pre_filter(mem_context_->get_arena_allocator(), is_vectorized, IVF_MAX_BRUTE_FORCE_SIZE))) {
      LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
    } else if (pre_fileter_rowkeys_.count() < IVF_MAX_BRUTE_FORCE_SIZE) {
      // do brute search
      adaptive_ctx_.is_brute_force_= true;
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
        LOG_WARN("failed to get limit rowkey brute", K(ret));
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    } else { // search with prefilter
      if (is_range_prefilter) { // rowkey range prefilter
        adaptive_ctx_.pre_scan_row_cnt_ += pre_fileter_rowkeys_.count();
        adaptive_ctx_.is_range_prefilter_ = true;
        ObArray<const ObNewRange *> rk_range;
        const ObRangeArray& key_range = inv_iter->get_scan_param().key_ranges_;
        for (int64_t i = 0; i < key_range.count() && OB_SUCC(ret); i++) {
          const ObNewRange *range = &key_range.at(i);
          if (OB_FAIL(rk_range.push_back(range))) {
            LOG_WARN("fail to push back range", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(prefilter.init(rk_range))) {
            LOG_WARN("fail to init prefilter as range filter", K(ret));
          }
        }
      } else if (OB_FAIL(prefilter.init())) { // rowkey hash bitmap prefilter
        LOG_WARN("fail to init prefilter as roaring bitmap", K(ret));
      } else { // add bitmap for bitmap filter
        adaptive_ctx_.pre_scan_row_cnt_ += pre_fileter_rowkeys_.count();
        for (int i = 0; i < pre_fileter_rowkeys_.count() && OB_SUCC(ret); i++) {
          uint64_t hash_val = hash_val_for_rk(pre_fileter_rowkeys_.at(i));
          if (OB_FAIL(prefilter.add(hash_val))) {
            LOG_WARN("fail to add hash val to prefilter", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(build_rowkey_hash_set(prefilter, is_vectorized, batch_row_count))) {
          LOG_WARN("fail to build rk hash set", K(ret));
        }
      }
      // do for loop until saved_rowkeys_.count() >= limitK
      if (OB_SUCC(ret)) {
        int64_t limit_k = limit_param_.limit_ + limit_param_.offset_;
        int64_t iter_cnt = 0;
        int64_t next_nprobe = 0;
        bool iter_end = false;
        bool is_first_scan = true;
        const int64_t sub_dim = dim_ / m_;
        int32_t start_idx = -1;
        bool no_new_near_rowkeys = false;
        ObExprVectorDistance::ObVecDisType cur_dis_type = dis_type_ == oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN ? oceanbase::sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED : dis_type_;
        IvfRowkeyHeap nearest_rowkey_heap(
            vec_op_alloc_, reinterpret_cast<float *>(real_search_vec_.ptr()), cur_dis_type, sub_dim, limit_k, similarity_threshold_);
        while (OB_SUCC(ret) && ! iter_end && nearest_rowkey_heap.count() < limit_k) {
          if (OB_FALSE_IT(start_idx = nearest_rowkey_heap.count())) {
          } else if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(
              is_vectorized,
              reinterpret_cast<float *>(real_search_vec_.ptr()),
              nearest_rowkey_heap,
              &prefilter))) {
            LOG_WARN("fail to calc nearest limit rowkeys in cids", K(ret), K(dim_));
          } else if (OB_FALSE_IT(no_new_near_rowkeys = (nearest_rowkey_heap.count() == start_idx))) {
            // there are no new rowkeys in near_rowkeys after post filter
          }
          if (OB_FAIL(ret)) {
          } else if (nearest_rowkey_heap.count() < limit_k) {
            ++iter_cnt;
            const int64_t left_search = limit_k - nearest_rowkey_heap.count();
            next_nprobe = OB_MIN(nprobes_, (nprobes_ * ((double)left_search) / limit_k + 1));
            LOG_INFO("prefilter does not get enough result", K(limit_k), "nearest_rowkey_heap_count", nearest_rowkey_heap.count(),
                K(selectivity_), K(left_search), K(next_nprobe), K(iter_cnt), K(nprobes_));
            near_cid_vec_.reuse();
            near_cid_vec_dis_.reuse();
            is_first_scan = false;
            if (similarity_threshold_ != 0 && no_new_near_rowkeys) {
              iter_end = true;
              LOG_INFO("there are no new rowkeys in near_rowkeys after post filter, stop iterative filter", K(ret), K(no_new_near_rowkeys), K(nearest_rowkey_heap.count()), K(start_idx), K(limit_k));
            } else if (! iterative_filter_ctx_.has_next_center()) {
              iter_end = true;
            } else if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_centers_vec_dist(next_nprobe, near_cid_vec_, near_cid_vec_dis_))) {
              LOG_WARN("get next centers from iterative_filter_ctx fail", K(ret), K(iterative_filter_ctx_),
                  K(next_nprobe), K(limit_k), K(left_search), K(iter_cnt), K(nprobes_));
            } else if (near_cid_vec_.count() != near_cid_vec_dis_.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("near_cid_vec count is not equal to near_cid_vec_dis count", K(ret),
                  "near_cid_vec_count", near_cid_vec_.count(), "near_cid_vec_dis_count", near_cid_vec_dis_.count());
            } else if (0 == near_cid_vec_dis_.count()) {
              iter_end = true;
              LOG_TRACE("there are no centers left to access", K(ret), K(iterative_filter_ctx_));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
          LOG_WARN("get rowkeys from heap fail", K(ret), K(limit_k));
        } else {
          LOG_TRACE("get rowkeys from heap success", K(limit_k), K(saved_rowkeys_.count()), K(iter_cnt), K(next_nprobe), K(nprobes_));
        }
      }
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::try_write_pq_centroid_cache(
    ObIvfCentCache &cent_cache,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  RWLock::WLockGuard guard(cent_cache.get_lock());
  if (!cent_cache.is_writing()) {
    LOG_INFO("other threads already writed centroids cache, skip", K(ret));
  } else {
    ObArenaAllocator tmp_allocator;
    const ObDASScanCtDef *pq_cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_pq_id_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN);
    ObDASScanRtDef *pq_cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_pq_id_tbl_idx());
    ObPqCenterId pq_cent_id;
    if (OB_ISNULL(pq_cid_vec_ctdef) || OB_ISNULL(pq_cid_vec_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctdef or rtdef is null", K(ret), KP(pq_cid_vec_ctdef), KP(pq_cid_vec_rtdef));
    } else if (OB_FAIL(do_table_full_scan(is_vectorized,
                                    pq_cid_vec_ctdef,
                                    pq_cid_vec_rtdef,
                                    pq_centroid_iter_,
                                    CENTROID_PRI_KEY_CNT,
                                    pq_centroid_tablet_id_,
                                    pq_centroid_first_scan_,
                                    pq_centroid_scan_param_))) {
      LOG_WARN("failed to do centroid table scan", K(ret));
    } else if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(pq_centroid_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        ObExpr *cid_expr = pq_cid_vec_ctdef->result_output_[CID_IDX];
        ObExpr *cid_vec_expr = pq_cid_vec_ctdef->result_output_[CID_VECTOR_IDX];
        ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
        ObDatum *cid_vec_datum = cid_vec_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
        bool has_lob_header = pq_cid_vec_ctdef->result_output_.at(CID_VECTOR_IDX)->obj_meta_.has_lob_header();
        uint64_t center_idx = 0;

        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          ObString cid = cid_datum[i].get_string();
          ObString cid_vec = cid_vec_datum[i].get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                          &tmp_allocator,
                          ObLongTextType,
                          CS_TYPE_BINARY,
                          has_lob_header,
                          cid_vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else if (OB_FAIL(ObVectorClusterHelper::get_pq_center_id_from_string(pq_cent_id, cid,
            ObVectorClusterHelper::IVF_PARSE_M_ID | ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
            LOG_WARN("fail to get center idx from string", K(ret), KPHEX(cid.ptr(), cid.length()));
          } else if (OB_FAIL(cent_cache.write_pq_centroid(
                pq_cent_id.m_id_, pq_cent_id.center_id_, reinterpret_cast<float*>(cid_vec.ptr()), cid_vec.length()))) {
            LOG_WARN("fail to write centroid", K(ret), K(pq_cent_id), KPHEX(cid_vec.ptr(), cid_vec.length()));
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(pq_centroid_iter_, pq_centroid_scan_param_, pq_centroid_tablet_id_)
    } else {
      pq_centroid_iter_->clear_evaluated_flag();
      ObExpr *cid_expr = pq_cid_vec_ctdef->result_output_[CID_IDX];
      ObDatum &cid_datum = cid_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
      bool has_lob_header = pq_cid_vec_ctdef->result_output_.at(CID_VECTOR_IDX)->obj_meta_.has_lob_header();
      ObExpr *vec_expr = pq_cid_vec_ctdef->result_output_[CID_VECTOR_IDX];
      ObDatum &vec_datum = vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
      for (int i = 0; OB_SUCC(ret); ++i) {
        if (OB_FAIL(pq_centroid_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to scan vid rowkey iter", K(ret));
          }
        } else {
          ObString cid_vec = vec_datum.get_string();
          ObString cid = cid_datum.get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                          &mem_context_->get_arena_allocator(),
                          ObLongTextType,
                          CS_TYPE_BINARY,
                          has_lob_header,
                          cid_vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else if (OB_FAIL(ObVectorClusterHelper::get_pq_center_id_from_string(pq_cent_id, cid,
            ObVectorClusterHelper::IVF_PARSE_M_ID | ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
            LOG_WARN("fail to get center idx from string", K(ret), KPHEX(cid.ptr(), cid.length()));
          } else if (OB_FAIL(cent_cache.write_pq_centroid(
                pq_cent_id.m_id_, pq_cent_id.center_id_, reinterpret_cast<float*>(cid_vec.ptr()), cid_vec.length()))) {
            LOG_WARN("fail to write centroid", K(ret), K(pq_cent_id), KPHEX(cid_vec.ptr(), cid_vec.length()));
          }
        }
      }
      int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;
      if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, pq_centroid_iter_, pq_centroid_scan_param_, pq_centroid_tablet_id_))) { 
        LOG_WARN("failed to reuse rowkey cid iter.", K(ret));
      } else {
        ret = tmp_ret;
      }
    }

    if (OB_SUCC(ret)) {
      if (cent_cache.get_count() > 0) {
        cent_cache.set_completed();
        LOG_DEBUG("success to write centroid table cache", K(pq_centroid_tablet_id_), K(cent_cache.get_count()));
      } else {
        cent_cache.reuse();
        LOG_DEBUG("Empty centroid table, no need to set cache", K(pq_centroid_tablet_id_));
      }
      
    }
  }

  if (OB_FAIL(ret)) {
    cent_cache.reuse();
  }
  
  return ret;
}

int ObDASIvfPQScanIter::get_pq_precomputetable_cache(
    bool is_vectorized, 
    ObIvfCacheMgrGuard &cache_guard,
    ObIvfCentCache *&cent_cache, 
    bool &is_cache_usable)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  ObIvfCacheMgr *cache_mgr = nullptr;
  const ObDASScanCtDef *centroid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_centroid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN);
  // pq/flat both use centroid_tablet_id_
  if (OB_FAIL(vec_index_service->acquire_ivf_cache_mgr_guard(
        ls_id_, centroid_tablet_id_, vec_index_param_, dim_, centroid_ctdef->ref_table_id_, cache_guard))) {
    LOG_WARN("failed to get ObPluginVectorIndexAdapter", 
      K(ret), K(ls_id_), K(centroid_tablet_id_), K(vec_index_param_));
  } else if (OB_ISNULL(cache_mgr = cache_guard.get_ivf_cache_mgr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null cache mgr", K(ret));
  } else if (OB_FAIL(cache_mgr->get_or_create_cache_node(IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE, cent_cache))) {
    LOG_WARN("fail to get or create cache node", K(ret));
    if (ret == OB_ALLOCATE_MEMORY_FAILED) {
      is_cache_usable = false;
      ret = OB_SUCCESS;
    }
  } else if (!cent_cache->is_completed()) {
    if (cent_cache->set_writing_if_idle()) {
      int tmp_ret = try_write_pq_precompute_table_cache(*cent_cache, is_vectorized);
      if (OB_TMP_FAIL(tmp_ret)) {
        if (tmp_ret != OB_EAGAIN) {
          LOG_WARN("fail to try write centroid cache", K(tmp_ret), K(is_vectorized), KPC(cent_cache));
        } else {
          is_cache_usable = false;
        }
      } else {
        is_cache_usable = cent_cache->is_completed();
      }
    } else {
      LOG_INFO("other threads already writed pq precompute table cache, skip", K(ret));
    }
  } else {
    // read cache
    is_cache_usable = true;
  }
  return ret;
}

int ObDASIvfPQScanIter::try_write_pq_precompute_table_cache(
    ObIvfCentCache &cent_cache,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  // write cache
  ObIvfCacheMgrGuard pq_center_guard;
  ObIvfCentCache *pq_cent_cache = nullptr;
  bool is_pq_cent_cache_usable = false;
  ObIvfCacheMgrGuard center_guard;
  ObIvfCentCache *ivf_cent_cache = nullptr;
  bool is_ivf_cent_cache_usable = false;
  if (OB_FAIL(get_centers_cache(is_vectorized, true/*is_pq_centers*/, pq_center_guard, pq_cent_cache, is_pq_cent_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(pq_cent_cache));
  } else if (OB_FAIL(get_centers_cache(is_vectorized, false/*is_pq_centers*/, center_guard, ivf_cent_cache, is_ivf_cent_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(ivf_cent_cache));
  } else {
    RWLock::WLockGuard guard(cent_cache.get_lock());
    if (!cent_cache.is_writing()) {
      LOG_INFO("other threads already writed centroids cache, skip", K(ret));
    } else {
      if (is_pq_cent_cache_usable && is_ivf_cent_cache_usable) {
        
          ObArenaAllocator tmp_allocator;
          int64_t ksub = 1L << nbits_;
          int64_t sub_dim = dim_ / m_;
          float *ivf_centers = ivf_cent_cache->get_centroids();
          float *pq_center = pq_cent_cache->get_centroids(); // m * ksub * sub_dim
          float *precompute_table = cent_cache.get_centroids();
          float *r_norms = (float*)tmp_allocator.alloc(sizeof(float) * m_ * ksub);
          if (OB_ISNULL(r_norms)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc norms", K(ret), K(m_), K(ksub));
          } else {
            // compute norms
            for (int i = 0; i < m_; i++) {
              for (int j = 0; j < ksub; j++) {
                float* pq_cid_vec = pq_center + (i * ksub + j) * sub_dim;
                r_norms[i * ksub + j] = ObVectorL2Distance<float>::l2_norm_square(pq_cid_vec, sub_dim);
              }
            }
            // nlist
            for (int i = 0; i < ivf_cent_cache->get_capacity(); i++) {
              float* ivf_center = ivf_centers + i * dim_;
              float* tab = &precompute_table[i * m_ * ksub];
              for (int j = 0; j < m_; j++) {
                ObVectorIpDistance<float>::fvec_inner_products_ny(tab + j * ksub,
                                                                  ivf_center + j * sub_dim,
                                                                  pq_center + j * ksub * sub_dim,
                                                                  sub_dim,
                                                                  ksub);
              }
              ObVectorL2Distance<float>::fvec_madd(m_ * ksub, r_norms, 2.0, tab, tab);
            }
          }
        if (OB_SUCC(ret)) {
          cent_cache.set_completed();
        }
        if (OB_FAIL(ret)) {
          cent_cache.reuse();
        }
      } else {
        ret = OB_EAGAIN;
        cent_cache.reuse();
      }
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::pre_compute_inner_prod_table(
    const float* search_vec,
    float* dis_table,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  int64_t ksub = 1L << nbits_;
  int64_t sub_dim = dim_ / m_;
  // write cache
  ObIvfCacheMgrGuard pq_center_guard;
  ObIvfCentCache *pq_cent_cache = nullptr;
  bool is_pq_cent_cache_usable = false;
  if (OB_FAIL(get_centers_cache(is_vectorized, true/*is_pq_centers*/, pq_center_guard, pq_cent_cache, is_pq_cent_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(pq_cent_cache));
  } else if (is_pq_cent_cache_usable) {
    float *pq_center = pq_cent_cache->get_centroids();
    for (int i = 0; i < m_; i++) {
      ObVectorIpDistance<float>::fvec_inner_products_ny(dis_table + i * ksub,
                                                        search_vec + i * sub_dim,
                                                        pq_center + i * ksub * sub_dim,
                                                        sub_dim,
                                                        ksub);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get pq center cache", K(ret));
  }
  return ret;
}

uint64_t ObDASIvfBaseScanIter::hash_val_for_rk(const common::ObRowkey& rk)
{
  uint64_t hash_val = 0;
  if (rk.get_obj_cnt() == 1 && ob_is_int_uint_tc(rk.get_obj_ptr()[0].get_type())) {
    hash_val = rk.get_obj_ptr()[0].get_uint64();
  } else {
    for (int i = 0; i < rk.get_obj_cnt(); i++) {
      (void)rk.get_obj_ptr()[i].hash(hash_val, hash_val);
    }
  }
  return hash_val;
}

int ObDASIvfBaseScanIter::build_rowkey_hash_set(
  ObIvfPreFilter &prefilter,
  bool is_vectorized,
  int64_t batch_row_count)
{
  int ret = OB_SUCCESS;
  bool index_end = false;
  // alloc one RowKey
  ObArenaAllocator& allocator = mem_context_->get_arena_allocator();
  int64_t rowkey_cnt = 0;
  const ObDASScanCtDef *ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(),
                                                                        ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  ObDASScanRtDef *rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx());
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(ctdef), KP(rtdef));
  } else if (OB_FALSE_IT(rowkey_cnt = ctdef->rowkey_exprs_.count())) {
  } else if (OB_UNLIKELY(rowkey_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret));
  } else {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "IVFRBTMP"));
    const bool is_one_int_rowkey = rowkey_cnt == 1 && ob_is_int_uint_tc(ctdef->rowkey_exprs_.at(0)->obj_meta_.get_type());
    if (!is_vectorized) {
      while (OB_SUCC(ret) && !index_end) {
        inv_idx_scan_iter_->clear_evaluated_flag();
        if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
          ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
          index_end = true;
        } else {
          uint64_t hash_val = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
            ObObj tmp_obj;
            ObExpr *expr = ctdef->rowkey_exprs_.at(i);
            ObDatum &datum = expr->locate_expr_datum(*rtdef->eval_ctx_);
            if (OB_ISNULL(datum.ptr_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get col datum null", K(ret));
            } else if (is_one_int_rowkey) {
              hash_val = datum.get_uint64();
            } else if (OB_FAIL(datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
              LOG_WARN("convert datum to obj failed", K(ret));
            } else if (OB_FAIL(tmp_obj.hash(hash_val, hash_val))) {
              LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(prefilter.add(hash_val))) {
              LOG_WARN("fail to add hash val to prefilter", K(ret));
            } else {
              adaptive_ctx_.pre_scan_row_cnt_ ++;
            }
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    } else {
      while (OB_SUCC(ret) && !index_end) {
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
          ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
          guard.set_batch_size(scan_row_cnt);
          adaptive_ctx_.pre_scan_row_cnt_ += scan_row_cnt;
          for (int i = 0; OB_SUCC(ret) && i < scan_row_cnt; i++) {
            guard.set_batch_idx(i);
            // pre_fileter_rowkeys_ need keep rowkey mem, so use vec_op_alloc_
            uint64_t hash_val = 0;
            for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_cnt; ++j) {
              ObObj tmp_obj;
              ObExpr *expr = ctdef->rowkey_exprs_.at(j);
              ObDatum &datum = expr->locate_expr_datum(*rtdef->eval_ctx_);
              if (OB_ISNULL(datum.ptr_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get col datum null", K(ret));
              } else if (is_one_int_rowkey) {
                hash_val = datum.get_uint64();
              } else if (OB_FAIL(datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
                LOG_WARN("convert datum to obj failed", K(ret));
              } else if (OB_FAIL(tmp_obj.hash(hash_val, hash_val))) {
                LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(prefilter.add(hash_val))) {
                LOG_WARN("fail to add hash val to prefilter", K(ret));
              }
            }
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
  }

  if (OB_SUCC(ret) && can_retry_ && OB_FAIL(check_pre_filter_need_retry())) {
    LOG_WARN("ret of check pre filter need retry.", K(ret), K(can_retry_), K(adaptive_ctx_), K(vec_index_type_), K(vec_idx_try_path_));
  }
  return ret;
}

/********************************************************************************************************/

int ObDASIvfSQ8ScanIter::inner_init(ObDASIterParam &param)
{
  int ret = ObDASIvfScanIter::inner_init(param);
  if (OB_SUCC(ret)) {
    ObDASIvfScanIterParam &ivf_scan_param = static_cast<ObDASIvfScanIterParam &>(param);
    sq_meta_iter_ = ivf_scan_param.sq_meta_iter_;
  }

  return ret;
}

int ObDASIvfSQ8ScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(sq_meta_iter_) && OB_FAIL(sq_meta_iter_->release())) {
    LOG_WARN("failed to release inv_idx_scan_iter_", K(ret));
  }
  sq_meta_iter_ = nullptr;
  ObDasVecScanUtils::release_scan_param(sq_meta_scan_param_);
  if (OB_SUCC(ret) && OB_FAIL(ObDASIvfScanIter::inner_release())) {
    LOG_WARN("fail to do ObDASIvfScanIter::inner_release", K(ret));
  }

  return ret;
}

int ObDASIvfSQ8ScanIter::get_real_search_vec_u8(bool is_vectorized, ObString &real_search_vec_u8)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *sq_meta_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_sq_meta_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN);
  ObDASScanRtDef *sq_meta_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_sq_meta_tbl_idx());
  ObString min_vec;
  ObString step_vec;
  if (OB_FAIL(do_table_full_scan(is_vectorized,
                                 sq_meta_ctdef,
                                 sq_meta_rtdef,
                                 sq_meta_iter_,
                                 SQ_MEAT_PRI_KEY_CNT,
                                 sq_meta_tablet_id_,
                                 sq_meta_iter_first_scan_,
                                 sq_meta_scan_param_))) {
    LOG_WARN("failed to do table scan sq_meta", K(ret));
  } else if (is_vectorized) {
    IVF_GET_NEXT_ROWS_BEGIN(sq_meta_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_aux_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        bool has_lob_header = sq_meta_ctdef->result_output_.at(META_VECTOR_IDX)->obj_meta_.has_lob_header();
        ObExpr *meta_vec_expr = sq_meta_ctdef->result_output_[META_VECTOR_IDX];
        ObDatum *meta_vec_datum = meta_vec_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);

        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          if (i == ObIvfConstant::SQ8_META_MIN_IDX || i == ObIvfConstant::SQ8_META_STEP_IDX) {
            ObString c_vec = meta_vec_datum[i].get_string();
            if (OB_FAIL(ObTextStringHelper::read_real_string_data_with_copy(
                    mem_context_->get_arena_allocator(),
                    meta_vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_),
                    sq_meta_ctdef->result_output_.at(META_VECTOR_IDX)->datum_meta_,
                    has_lob_header,
                    c_vec))) {
              LOG_WARN("failed to get real data.", K(ret));
            } else if (i == ObIvfConstant::SQ8_META_MIN_IDX) {
              // if not has lob, need deepcopy, because datum_row whill reuse
              min_vec = c_vec;
            } else if (i == ObIvfConstant::SQ8_META_STEP_IDX) {
              step_vec = c_vec;
            }
          }
        }
      }
    IVF_GET_NEXT_ROWS_END(sq_meta_iter_, sq_meta_scan_param_, sq_meta_tablet_id_)
  } else {
    // get min_vec max_vec step_vec in sq_meta table
    sq_meta_iter_->clear_evaluated_flag();
    bool has_lob_header = sq_meta_ctdef->result_output_.at(META_VECTOR_IDX)->obj_meta_.has_lob_header();
    ObExpr *meta_vec_expr = sq_meta_ctdef->result_output_[META_VECTOR_IDX];
    ObDatum &meta_vec_datum = meta_vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
    int row_index = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(sq_meta_iter_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else {
        ObString c_vec = meta_vec_datum.get_string();
        if (OB_FAIL(ObTextStringHelper::read_real_string_data_with_copy(
                mem_context_->get_arena_allocator(),
                meta_vec_datum,
                sq_meta_ctdef->result_output_.at(META_VECTOR_IDX)->datum_meta_,
                has_lob_header,
                c_vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (row_index == ObIvfConstant::SQ8_META_MIN_IDX) {
          // if not has lob, need deepcopy, because datum_row whill reuse
          min_vec = c_vec;
        } else if (row_index == ObIvfConstant::SQ8_META_STEP_IDX) {
          step_vec = c_vec;
        }
      }
      row_index++;
    }
    if (ret == OB_ITER_END) {
      if (OB_FAIL(sq_meta_iter_->reuse())) {
        LOG_WARN("fail to reuse scan iterator.", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    uint8_t *res_vec = nullptr;
    if (OB_ISNULL(min_vec.ptr()) || OB_ISNULL(step_vec.ptr())) {
      if (OB_ISNULL(res_vec = reinterpret_cast<uint8_t *>(mem_context_->get_arena_allocator().alloc(sizeof(uint8_t) * dim_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(sizeof(uint8_t) * dim_));
      } else {
        MEMSET(res_vec, 0, sizeof(uint8_t) * dim_);
      }
    } else if (OB_FAIL(
                    ObExprVecIVFSQ8DataVector::cal_u8_data_vector(mem_context_->get_arena_allocator(),
                                                                  dim_,
                                                                  reinterpret_cast<float *>(min_vec.ptr()),
                                                                  reinterpret_cast<float *>(step_vec.ptr()),
                                                                  reinterpret_cast<float *>(real_search_vec_.ptr()),
                                                                  res_vec))) {
      LOG_WARN("fail to cal u8 data vector", K(ret), K(dim_));
    }
    if (OB_SUCC(ret)) {
      real_search_vec_u8.assign_ptr(reinterpret_cast<char *>(res_vec), dim_ * sizeof(uint8_t));
    }
  }
  return ret;
}

int ObDASIvfSQ8ScanIter::process_ivf_scan_post(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  ObString real_search_vec_u8;
  if (OB_FAIL(get_real_search_vec_u8(is_vectorized, real_search_vec_u8))) {
    LOG_WARN("failed to get real search vec u8", K(ret));
  } else if (OB_FAIL(do_ivf_scan_post<uint8_t>(is_vectorized, reinterpret_cast<uint8_t *>(real_search_vec_u8.ptr())))) {
    LOG_WARN("failed to do post filter", K(ret), K(is_vectorized));
  }
  return ret;
}

int ObDASIvfSQ8ScanIter::process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  ObString real_search_vec_u8;
  if (OB_FAIL(get_real_search_vec_u8(is_vectorized, real_search_vec_u8))) {
    LOG_WARN("failed to get real search vec u8", K(ret));
  } else if (OB_FAIL(do_ivf_scan_pre<uint8_t>(allocator, is_vectorized, reinterpret_cast<uint8_t*>(real_search_vec_u8.ptr())))) {
    LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
  }
  return ret;
}

// ObIvfPreFilter
void ObIvfPreFilter::reset()
{
  // release memory
  if (OB_NOT_NULL(roaring_bitmap_)) {
    if (type_ == FilterType::ROARING_BITMAP) {
      roaring::api::roaring64_bitmap_free(roaring_bitmap_);
    }
  }
  // reset members
  type_ = FilterType::ROARING_BITMAP;
  roaring_bitmap_ = nullptr;
  rk_range_.reset();
}

int ObIvfPreFilter::init()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(roaring_bitmap_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    ROARING_TRY_CATCH(roaring_bitmap_ = roaring::api::roaring64_bitmap_create());
    if (OB_SUCC(ret)) {
      type_ = FilterType::ROARING_BITMAP;
    }
  }
  return ret;
}

int ObIvfPreFilter::init(const ObIArray<const ObNewRange*> &range)
{
  int ret = OB_SUCCESS;
  if (rk_range_.count() != 0) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(rk_range_));
  } else if (range.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rk range", K(ret), K(range));
  } else if (OB_FAIL(rk_range_.assign(range))) {
    LOG_WARN("fail to assign rt st", K(ret));
  } else {
    type_ = FilterType::SIMPLE_RANGE;
  }
  return ret;
}

int ObIvfPreFilter::add(int64_t id)
{
  int ret = OB_SUCCESS;
  if (type_ == FilterType::ROARING_BITMAP) {
    ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_add(roaring_bitmap_, id));
  } else if (type_ == FilterType::SIMPLE_RANGE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("simple range not support add", K(ret));
  }
  return ret;
}

bool ObIvfPreFilter::test(const ObRowkey& main_rowkey)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (type_ == FilterType::ROARING_BITMAP) {
    uint64_t hash_val = 0;
    if (main_rowkey.get_obj_cnt() == 1 && ob_is_int_uint_tc(main_rowkey.get_obj_ptr()[0].get_type())) {
      hash_val = main_rowkey.get_obj_ptr()[0].get_uint64();
    } else {
      for (int i = 0; i < main_rowkey.get_obj_cnt(); i++) {
        (void)main_rowkey.get_obj_ptr()[i].hash(hash_val, hash_val);
      }
    }
    bret = roaring::api::roaring64_bitmap_contains(roaring_bitmap_, hash_val);
  } else if (type_ == FilterType::SIMPLE_RANGE) {
    ObNewRange tmp_range;
    if (OB_FAIL(tmp_range.build_range(rk_range_.at(0)->table_id_, main_rowkey))) {
      LOG_WARN("fail to build tmp range", K(ret));
    }
    // do compare
    for (int64_t i = 0; i < rk_range_.count() && !bret && OB_SUCC(ret); i++) {
      if (rk_range_.at(i)->compare_with_startkey2(tmp_range) <= 0 && rk_range_.at(i)->compare_with_endkey2(tmp_range) >= 0) {
        bret = true;
      }
    }
  }
  return bret;
}

void ObIvfIterativeFilterContext::reset()
{
  visited_center_cnt_ = 0;
  centroids_.reset();
  allocator_.reset();
}

void ObIvfIterativeFilterContext::reuse()
{
  visited_center_cnt_ = 0;
  centroids_.reuse();
  allocator_.reuse();
}

int ObIvfIterativeFilterContext::get_next_nearest_probe_center_ids(const int64_t nprobe, ObIArray<ObCenterId> &center_ids)
{
  int ret = OB_SUCCESS;
  if (visited_center_cnt_ >= centroids_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("all centers are visited", K(ret), K(visited_center_cnt_), "centroids_count", centroids_.count());
  } else {
    int64_t end = OB_MIN(visited_center_cnt_ + nprobe, centroids_.count());
    for (int64_t i = visited_center_cnt_; OB_SUCC(ret) && i < end; ++i) {
      const ObCentroidQueryInfo<float, ObCenterId> &cur = centroids_.at(i);
      if (OB_FAIL(center_ids.push_back(cur.id_))) {
        LOG_WARN("failed to push center id", K(ret), K(i), K(cur));
      }
    }
    if (OB_SUCC(ret)) {
      visited_center_cnt_ = end;
    }
  }
  return ret;
}

int ObIvfIterativeFilterContext::get_next_nearest_probe_center_ids_dist(const int64_t nprobe, ObArrayWrap<bool> &nearest_cid_dist)
{
  int ret = OB_SUCCESS;
  if (visited_center_cnt_ >= centroids_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("all centers are visited", K(ret), K(visited_center_cnt_), "centroids_count", centroids_.count());
  } else {
    int64_t end = OB_MIN(visited_center_cnt_ + nprobe, centroids_.count());
    for (int64_t i = visited_center_cnt_; OB_SUCC(ret) && i < end; ++i) {
      const ObCentroidQueryInfo<float, ObCenterId> &cur = centroids_.at(i);
      const ObCenterId &center_id = cur.id_;
      if (OB_UNLIKELY(center_id.center_id_ >= nearest_cid_dist.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("center_id is not less than nearest_cid_dist", K(ret), K(center_id), K(nearest_cid_dist.count()));
      } else {
        nearest_cid_dist.at(center_id.center_id_) = true;
      }
    }
    if (OB_SUCC(ret)) {
      visited_center_cnt_ = end;
    }
  }
  return ret;
}

int ObIvfIterativeFilterContext::get_next_nearest_probe_centers_vec_dist(const int64_t nprobe, ObIArray<std::pair<ObCenterId, float *>> &center_ids, ObIArray<float> &distances)
{
  int ret = OB_SUCCESS;
  if (visited_center_cnt_ >= centroids_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("all centers are visited", K(ret), K(visited_center_cnt_), "centroids_count", centroids_.count());
  } else {
    int64_t end = OB_MIN(visited_center_cnt_ + nprobe, centroids_.count());
    for (int64_t i = visited_center_cnt_; OB_SUCC(ret) && i < end; ++i) {
      const ObCentroidQueryInfo<float, ObCenterId> &cur = centroids_.at(i);
      if (OB_FAIL(center_ids.push_back(std::make_pair(cur.id_, (float *)cur.vec_)))) {
        LOG_WARN("failed to push center id", K(ret), K(i), K(cur));
      } else if (OB_FAIL(distances.push_back(cur.distance_))) {
        LOG_WARN("failed to push distance", K(ret), K(i), K(cur));
      }
    }
    if (OB_SUCC(ret)) {
      visited_center_cnt_ = end;
    }
  }
  return ret;
}

bool ObIvfIterativeFilterContext::has_next_center() const
{
  LOG_TRACE("next info", K_(visited_center_cnt), "center_count", centroids_.count());
  return visited_center_cnt_ < centroids_.count();
}

}  // namespace sql
}  // namespace oceanbase
