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
#include "sql/das/iter/ob_das_vec_index_driver_iter.h"
#include "sql/das/iter/ob_das_profile_iter.h"
#include "sql/das/iter/ob_das_vec_index_hnsw_scan_iter.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/das/iter/ob_das_search_driver_iter.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/utility/ob_sort.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "sql/das/iter/ob_das_scan_iter.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

OB_SERIALIZE_MEMBER((ObDASVecIndexDriverCtDef, ObDASAttachCtDef), sort_expr_, limit_expr_, offset_expr_, vec_index_param_, query_param_, vector_index_param_, algorithm_type_, vec_type_, row_count_, dim_);
OB_SERIALIZE_MEMBER((ObDASVecIndexDriverRtDef, ObDASAttachRtDef));

int ObDASVecIndexDriverIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *my_profile = nullptr;
  if (OB_FAIL(ObDASProfileIter::init_runtime_profile(
          common::ObProfileId::HYBRID_SEARCH_VEC_ITER, my_profile))) {
    LOG_WARN("failed to init runtime profile", KR(ret));
  } else if (OB_ISNULL(vec_index_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vec index scan iter or filter iter is null", K(ret));
  } else {
    common::ObProfileSwitcher switcher(my_profile);
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_INDEX_TYPE, vec_index_type_);
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_FILTER_MODE, static_cast<uint64_t>(filter_mode_));
    if (OB_FAIL(vec_index_scan_iter_->do_table_scan())) {
      LOG_WARN("failed to do table scan", K(ret));
    } else if (OB_NOT_NULL(filter_iter_) && OB_FAIL(filter_iter_->do_table_scan())) {
      LOG_WARN("failed to do table scan", K(ret));
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::rescan()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *my_profile = nullptr;

  if (OB_FAIL(ObDASProfileIter::init_runtime_profile(
          common::ObProfileId::HYBRID_SEARCH_VEC_ITER, my_profile))) {
    LOG_WARN("failed to init runtime profile", KR(ret));
  } else if (OB_ISNULL(vec_index_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vec index scan iter or filter iter is null", K(ret));
  } else {
    common::ObProfileSwitcher switcher(my_profile);
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_INDEX_TYPE, vec_index_type_);
    SET_METRIC_VAL(common::ObMetricId::HS_VEC_FILTER_MODE, static_cast<uint64_t>(filter_mode_));
    if (OB_FAIL(vec_index_scan_iter_->rescan())) {
      LOG_WARN("failed to rescan vec index scan iter", K(ret));
    } else if (OB_NOT_NULL(filter_iter_) && OB_FAIL(filter_iter_->rescan())) {
      LOG_WARN("failed to rescan filter iter", K(ret));
    }
  }

  return ret;
}

void ObDASVecIndexDriverIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(vec_index_scan_iter_)) {
    vec_index_scan_iter_->clear_evaluated_flag();
  }
  if (OB_NOT_NULL(filter_iter_)) {
    filter_iter_->clear_evaluated_flag();
  }
}

int ObDASVecIndexDriverIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ObDASIterType::DAS_ITER_VEC_INDEX_DRIVER != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for vec index driver iter", K(ret), K(param));
  } else {
    ObDASVecIndexDriverIterParam &vec_index_driver_param = static_cast<ObDASVecIndexDriverIterParam &>(param);
    ls_id_ = vec_index_driver_param.ls_id_;
    tx_desc_ = vec_index_driver_param.tx_desc_;
    snapshot_ = vec_index_driver_param.snapshot_;
    vec_index_scan_iter_ = vec_index_driver_param.vec_index_scan_iter_;
    filter_iter_ = vec_index_driver_param.filter_iter_;
    vec_index_driver_ctdef_ = vec_index_driver_param.vec_index_driver_ctdef_;
    vec_index_driver_rtdef_ = vec_index_driver_param.vec_index_driver_rtdef_;
    vec_index_type_ = vec_index_driver_param.vec_index_type_;
    vec_idx_try_path_ = vec_index_driver_param.vec_idx_try_path_;
    sort_expr_ = vec_index_driver_param.sort_expr_;
    limit_expr_ = vec_index_driver_param.limit_expr_;
    offset_expr_ = vec_index_driver_param.offset_expr_;
    dim_ = vec_index_driver_param.dim_;
    search_ctx_ = vec_index_driver_param.search_ctx_;
    score_expr_ = vec_index_driver_param.score_expr_;
    filter_mode_ = vec_index_driver_param.filter_mode_;
    scalar_scan_ctdef_ = vec_index_driver_param.scalar_scan_ctdef_;
    scalar_scan_rtdef_ = vec_index_driver_param.scalar_scan_rtdef_;

    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "VECSEARCH", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("failed to create vector search memory context", K(ret));
      } else {
        bitmap_.set_allocator(&mem_context_->get_arena_allocator());
      }
    }

    if (FAILEDx(init_limit_param())) {
      LOG_WARN("failed to init limit param", K(ret));
    } else if (OB_FAIL(init_sort())) {
      LOG_WARN("failed to init sort", K(ret));
    } else if (OB_FAIL(set_vec_index_param(vec_index_driver_ctdef_->vec_index_param_))) {
      LOG_WARN("failed to set vec index param", K(ret));
    } else if (OB_FAIL(ObVectorIndexParam::build_search_param(vec_index_driver_ctdef_->vector_index_param_, vec_index_driver_ctdef_->query_param_, search_param_))) {
      LOG_WARN("build search param fail", K(vec_index_driver_ctdef_->vector_index_param_), K(vec_index_driver_ctdef_->query_param_));
    } else if (search_param_.similarity_threshold_ != 0) {
      if (OB_FAIL(ObDasVecScanUtils::get_distance_threshold_hnsw(*sort_expr_, search_param_.similarity_threshold_, distance_threshold_))) {
        LOG_WARN("get distance threshold fail", K(ret));
      }
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::init_sort()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sort_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort expr is null", K(ret));
  } else if (sort_expr_->is_vector_sort_expr()) {
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

int ObDASVecIndexDriverIter::init_limit_param()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(limit_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit expr or offset expr is null", K(ret));
  } else {
    bool is_null = false;
    ObDatum *limit_datum = nullptr;
    ObDatum *offset_datum = nullptr;
    if (OB_FAIL(limit_expr_->eval(*vec_index_driver_rtdef_->eval_ctx_, limit_datum))) {
      LOG_WARN("failed to eval limit expr", K(ret));
    } else if (limit_datum->is_null()) {
      is_null = true;
      limit_param_.limit_ = 0;
    } else {
      limit_param_.limit_ = limit_datum->get_int() < 0 ? 0 : limit_datum->get_int();
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(offset_expr_)) {
      if (!is_null && OB_FAIL(offset_expr_->eval(*vec_index_driver_rtdef_->eval_ctx_, offset_datum))) {
        LOG_WARN("failed to eval offset expr", K(ret));
      } else if (offset_datum->is_null()) {
        limit_param_.offset_ = 0;
      } else {
        limit_param_.offset_ = offset_datum->get_int() < 0 ? 0 : offset_datum->get_int();
      }
    }

    if (OB_SUCC(ret)) {
      if (limit_param_.offset_ + limit_param_.limit_ > ObDASVecIndexHNSWScanIter::MAX_VSAG_QUERY_RES_SIZE) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_WARN(OB_NOT_SUPPORTED, "query size (limit + offset) is more than 16384", K(limit_param_));
      } else {
        vec_index_scan_iter_->set_limit(limit_param_.limit_+limit_param_.offset_);
      }
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::inner_reuse()
{
  int ret = OB_SUCCESS;

  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(vec_index_scan_iter_) && OB_FAIL(vec_index_scan_iter_->reuse())) {
    LOG_WARN("failed to reuse vec index scan iter", K(ret));
    tmp_ret = ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(filter_iter_) && OB_FAIL(filter_iter_->reuse())) {
    LOG_WARN("failed to reuse filter iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  // memory reset
  if (vid_to_distance_.created()) {
    vid_to_distance_.destroy();
  }

  query_cond_.reset();
  bitmap_.reset();
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }

  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
  vec_op_alloc_.reset();

  // reset iterative filter state
  iter_unfiltered_vid_cnt_ = 0;
  iter_added_cnt_ = 0;
  iter_scan_total_num_ = 0;
  ready_to_output_ = false;

  // must be after memory reset
  if (OB_SUCC(ret) && OB_FAIL(set_vec_index_param(vec_index_driver_ctdef_->vec_index_param_))) {
    LOG_WARN("failed to set vec index param", K(ret));
  }

  return ret;
}

int ObDASVecIndexDriverIter::inner_release()
{
  int ret = OB_SUCCESS;

  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(vec_index_scan_iter_) && OB_FAIL(vec_index_scan_iter_->release())) {
    LOG_WARN("failed to release vec index scan iter", K(ret));
    tmp_ret = ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(filter_iter_) && OB_FAIL(filter_iter_->release())) {
    LOG_WARN("failed to release filter iter", K(ret));
    tmp_ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  vec_index_scan_iter_ = nullptr;
  filter_iter_ = nullptr;

  // memory reset
  if (vid_to_distance_.created()) {
    vid_to_distance_.destroy();
  }

  query_cond_.reset();
  bitmap_.reset();
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }

  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
  vec_op_alloc_.reset();

  return ret;
}

int ObDASVecIndexDriverIter::inner_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObDASVecIndexDriverIter::set_vector_query_condition(ObVectorQueryConditions &query_cond)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(search_vec_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("search vec is null", K(ret));
  } else {
    query_cond.query_order_ = true;
    query_cond.query_scn_ = snapshot_->core_.version_;
    query_cond.is_post_with_filter_ = is_iter_filter();
    query_cond.distance_threshold_ = distance_threshold_;
    query_cond.ob_sparse_drop_ratio_search_ = search_param_.ob_sparse_drop_ratio_search_;

    uint64_t ob_hnsw_ef_search = 0;
    if (OB_FAIL(get_ob_hnsw_ef_search(ob_hnsw_ef_search))) {
      LOG_WARN("failed to get ob hnsw ef search", K(ret), K(ob_hnsw_ef_search));
    } else if (OB_FALSE_IT(query_cond.ef_search_ = ob_hnsw_ef_search)) {
    } else {
      uint64_t real_limit = limit_param_.limit_+limit_param_.offset_;
      // if selectivity_ == 1 means there is no filter
      if (is_hnsw_bq()) {
        // normally topK(real_limit) should be the same as ef_search for bq
        // but if topK is larger than ef_search, use topK
        real_limit = get_reorder_count(ob_hnsw_ef_search, real_limit, search_param_);
      }
      query_cond.query_limit_ = real_limit;
    }

    if (OB_FAIL(ret)) {
    } else if (!OB_ISNULL(search_vec_)) {
      if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(mem_context_->get_arena_allocator(), vec_index_driver_rtdef_->eval_ctx_,
                                                        search_vec_, query_cond.query_vector_))) {
        LOG_WARN("failed to get real search vec", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    vec_index_scan_iter_->set_vector_query_condition(&query_cond);
  }

  return ret;
}

int ObDASVecIndexDriverIter::get_reorder_count(const int64_t ef_search, const int64_t topK, const ObVectorIndexParam& param)
{
  // max is ef_search
  const float refine_k = param.refine_k_;
  int64_t refine_cnt = refine_k * topK;
  if (refine_cnt < topK) refine_cnt = topK;
  LOG_TRACE("reorder count info", K(ef_search), K(topK), K(refine_k), K(refine_cnt));
  int64_t reorder_count = 0;
  if (is_hnsw_bq()) {
    reorder_count = OB_MIN(OB_MAX(topK, OB_MIN(refine_cnt, ef_search)), ObDASVecIndexHNSWScanIter::MAX_VSAG_QUERY_RES_SIZE);
  }
  return reorder_count;
}

int ObDASVecIndexDriverIter::get_ob_hnsw_ef_search(uint64_t &ob_hnsw_ef_search)
{
  int ret = OB_SUCCESS;
  const uint64_t OB_HNSW_EF_SEARCH_DEFAULT = 64;  // same as SYS_VAR_OB_HNSW_EF_SEARCH
  ObExecContext *exec_ctx = &vec_index_driver_rtdef_->eval_ctx_->exec_ctx_;

  ObSQLSessionInfo *session = nullptr;
  if (OB_NOT_NULL(vec_index_driver_ctdef_) && vec_index_driver_ctdef_->query_param_.is_set_ef_search_) {
    ob_hnsw_ef_search =  vec_index_driver_ctdef_->query_param_.ef_search_;
    LOG_TRACE("use stmt ef_search", K(ob_hnsw_ef_search));
  } else if (OB_ISNULL(session = exec_ctx->get_my_session())) {
    ob_hnsw_ef_search = OB_HNSW_EF_SEARCH_DEFAULT;
    LOG_WARN("session is null", K(ret), KP(exec_ctx));
  } else if (OB_FAIL(session->get_ob_hnsw_ef_search(ob_hnsw_ef_search))) {
    LOG_WARN("failed to get ob hnsw ef search", K(ret));
  }

  return ret;
}

int ObDASVecIndexDriverIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (!ready_to_output_) {
    if (limit_param_.limit_ + limit_param_.offset_ == 0) {
      ret = OB_ITER_END;
    } else if (!query_cond_.is_inited() && OB_FAIL(set_vector_query_condition(query_cond_))) {
      LOG_WARN("failed to set vector query condition", K(ret));
    } else {
      ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
      ObPluginVectorIndexAdapterGuard adaptor_guard;
      share::ObVectorIndexAcquireCtx index_ctx;
      index_ctx.inc_tablet_id_ = delta_buf_tablet_id_;
      index_ctx.vbitmap_tablet_id_ = index_id_tablet_id_;
      index_ctx.snapshot_tablet_id_ = snapshot_tablet_id_;
      index_ctx.data_tablet_id_ = com_aux_vec_tablet_id_;
      if (OB_FAIL(vec_index_service->acquire_adapter_guard(ls_id_, index_ctx, adaptor_guard, &vec_index_param_, dim_))) {
        LOG_WARN("failed to get ObPluginVectorIndexAdapter", K(ret), K(ls_id_), K(index_ctx));
      } else {
        share::ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
        if (OB_ISNULL(adaptor)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("adaptor is null", K(ret));
        } else {
          vec_index_scan_iter_->set_adaptor(adaptor);
          if (is_pre_filter()) {
            if (OB_FAIL(process_pre_filter_mode(adaptor, count, capacity))) {
              LOG_WARN("failed to process pre filter mode", K(ret));
            }
          } else if (is_iter_filter()) {
            if (OB_FAIL(process_iterative_filter_mode(adaptor, count, capacity))) {
              LOG_WARN("failed to process iterative filter mode", K(ret));
            }
          } else if (is_post_filter()) {
            if (OB_FAIL(process_post_filter_mode(adaptor, count, capacity))) {
              LOG_WARN("failed to process post filter mode", K(ret));
            }
          }
        }
      }
    }
    ready_to_output_ = true;
  }

  if (OB_SUCC(ret)) {
    if (is_pre_filter()) {
      share::ObVectorQueryVidIterator *adaptor_vid_iter = vec_index_scan_iter_->get_adaptor_vid_iter();
      if (OB_FAIL(fill_results_to_eval_ctx(adaptor_vid_iter, count, capacity))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to fill results to eval ctx", K(ret));
      }
    } else if (is_iter_filter()) {
      if (OB_FAIL(fill_results_to_eval_ctx(adaptor_vid_iter_, count, capacity))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to fill results to eval ctx", K(ret));
      }
    } else if (is_post_filter()) {
      ObVectorQueryVidIterator *adaptor_vid_iter = filter_iter_ != nullptr ? adaptor_vid_iter_ : vec_index_scan_iter_->get_adaptor_vid_iter();
      if (OB_FAIL(fill_results_to_eval_ctx(adaptor_vid_iter, count, capacity))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to fill results to eval ctx", K(ret));
      }
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::build_bitmap_from_filter_iter(share::ObPluginVectorIndexAdaptor* adaptor)
{
  int ret = OB_SUCCESS;

  ObVidBound bound;
  const uint64_t MAX_HNSW_BRUTE_FORCE_SIZE = 20000;
  if (OB_FAIL(adaptor->get_vid_bound(bound))) {
    LOG_WARN("failed to get vid bound", K(ret));
  } else if (OB_FAIL(bitmap_.init(bound.min_vid_, bound.max_vid_, MAX_HNSW_BRUTE_FORCE_SIZE))) {
    LOG_WARN("failed to init bitmap", K(ret));
  }

  ObEvalCtx *eval_ctx = vec_index_driver_rtdef_->eval_ctx_;
  int64_t batch_row_count = eval_ctx->max_batch_size_ > 0 ?
                            min(eval_ctx->max_batch_size_, ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE) :
                            ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;

  bool index_end = false;
  while (OB_SUCC(ret) && !index_end) {
    int64_t scan_row_cnt = 0;
    filter_iter_->clear_evaluated_flag();
    if (OB_FAIL(filter_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row.", K(ret));
      }
      index_end = true;
    }

    if (OB_FAIL(ret) && OB_ITER_END != ret) {
    } else if (scan_row_cnt > 0) {
      ret = OB_SUCCESS;
    }

    ObExpr *vid_expr = nullptr;
    if (OB_FAIL(ret)) {
    } else if (scan_row_cnt == 0) {
    } else if (OB_ISNULL(search_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search ctx is null", K(ret));
    } else if (OB_UNLIKELY(search_ctx_->get_rowid_exprs().count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowid exprs is empty", K(ret));
    } else if (OB_ISNULL(vid_expr = search_ctx_->get_rowid_exprs().at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowid expr is null", K(ret));
    } else if (vid_expr->enable_rich_format() && is_valid_format(vid_expr->get_format(*eval_ctx))) {
      ObIVector *vec = vid_expr->get_vector(*eval_ctx);
      if (OB_ISNULL(vec)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vector is null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          int64_t vid = vec->get_uint64(i);
          if (OB_FAIL(bitmap_.add_vid(vid))) {
            LOG_WARN("failed to add vid to bitmap", K(ret), K(vid), K(i));
          }
        }
      }
    } else {
      ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
      guard.set_batch_size(scan_row_cnt);
      ObDatum *vid_datums = vid_expr->locate_batch_datums(*eval_ctx);
      if (OB_ISNULL(vid_datums)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vid datums is null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          int64_t vid = vid_datums[i].get_uint64();
          if (OB_FAIL(bitmap_.add_vid(vid))) {
            LOG_WARN("failed to add vid to bitmap", K(ret), K(vid), K(i));
          }
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObDASVecIndexDriverIter::process_pre_filter_mode(share::ObPluginVectorIndexAdaptor* adaptor, int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(build_bitmap_from_filter_iter(adaptor))) {
    LOG_WARN("failed to build bitmap from filter iter", K(ret));
  } else if (bitmap_.get_valid_cnt() == 0) {
    ret = OB_ITER_END;
  } else {
    vec_index_scan_iter_->set_bitmap(&bitmap_);
    vec_index_scan_iter_->clear_evaluated_flag();
    int64_t dummy_count = 0;
    if (OB_FAIL(vec_index_scan_iter_->get_next_rows(dummy_count, capacity))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to query with prefilter", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::set_bitmap_to_filter_iter()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(filter_iter_)) {
    ObDASSearchDriverIter *search_driver_iter = static_cast<ObDASSearchDriverIter*>(filter_iter_);
    search_driver_iter->set_bitmap(&bitmap_);
  }
  return ret;
}

int ObDASVecIndexDriverIter::process_iterative_filter_mode(share::ObPluginVectorIndexAdaptor* adaptor, int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  bool end_search = false;
  bool first_search = true;
  if (OB_FAIL(adjust_vector_query_condition(first_search))) {
    LOG_WARN("failed to adjust vector query condition", K(ret));
  } else {
    while (OB_SUCC(ret) && !end_search) {
      if (OB_FAIL(fetch_vids_from_vec_index(adaptor, count, capacity))) {
        LOG_WARN("failed to fetch vids from vec index", K(ret));
      } else if (filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER) {
        if (OB_FAIL(post_query_vid_with_expr_filter())) {
          LOG_WARN("failed to post query vid with expr filter", K(ret));
        }
      } else {
        if (!first_search && OB_FAIL(filter_iter_->reuse())) {
          LOG_WARN("failed to reuse filter iter", K(ret));
        } else if (!first_search && OB_FAIL(filter_iter_->rescan())) {
          LOG_WARN("failed to rescan filter iter", K(ret));
        } else if (OB_FAIL(set_bitmap_to_filter_iter())) {
          LOG_WARN("failed to set bitmap to filter iter", K(ret));
        } else if (OB_FAIL(post_query_vid_with_filter())) {
          LOG_WARN("failed to post query vid with filter", K(ret));
        }
      }

      first_search = false;
      if (FAILEDx(adjust_vector_query_condition(first_search))) {
        LOG_WARN("failed to adjust vector query condition", K(ret));
      } else if (query_cond_.query_limit_ == 0) {
        end_search = true;
      }
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::fill_results_to_eval_ctx(share::ObVectorQueryVidIterator *adaptor_vid_iter, int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(adaptor_vid_iter)) {
    ret = OB_ITER_END;
  } else if (!adaptor_vid_iter->is_init()) {
    ret = OB_ITER_END;
  } else {
    ObNewRow *row = nullptr;
    if (OB_FALSE_IT(adaptor_vid_iter->set_batch_size(capacity))) {
    } else if (OB_FAIL(adaptor_vid_iter->get_next_rows(row, count, vec_index_driver_ctdef_->result_output_))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rows from adaptor vid iter", K(ret));
    } else if (count > 0) {
      ObEvalCtx *eval_ctx = vec_index_driver_rtdef_->eval_ctx_;
      const ExprFixedArray& res_exprs = vec_index_driver_ctdef_->result_output_;
      // res_exprs size should be 1, which is vid_column
      if (res_exprs.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("res_exprs count is not equal to 1", K(ret), K(res_exprs));
      } else {
        int64_t a_batch_obj_cnt = 2;
        ObExpr *vid_expr = res_exprs.at(0);
        if (OB_ISNULL(vid_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret));
        } else if (vid_expr->enable_rich_format()) {
          ObIVector *vec = nullptr;
          if (OB_FAIL(vid_expr->init_vector_for_write(*eval_ctx, vid_expr->get_default_res_format(), count))) {
            LOG_WARN("failed to init vector for write", K(ret));
          } else if (OB_ISNULL(vec = vid_expr->get_vector(*eval_ctx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("vector is null", K(ret));
          } else {
            for (int64_t num = 0; OB_SUCC(ret) && num < count; ++num) {
              vec->set_uint(num, row->get_cell(num * a_batch_obj_cnt + 1).get_int());
            }
          }
        } else {
          ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
          guard.set_batch_size(count);
          ObDatum *vid_datum = nullptr;
          if (OB_ISNULL(vid_datum = vid_expr->locate_datums_for_update(*eval_ctx, count))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, datums is nullptr", K(ret), KPC(vid_expr));
          } else {
            for (int64_t num = 0; OB_SUCC(ret) && num < count; ++num) {
              guard.set_batch_idx(num);
              if (OB_FAIL(vid_datum[num].from_obj(row->get_cell(num * a_batch_obj_cnt + 1)))) {
                LOG_WARN("fail to from obj", K(ret));
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          vid_expr->set_evaluated_projected(*eval_ctx);
        }
      }

      if (FAILEDx(save_vec_socre_expr_result(row, count))) {
        LOG_WARN("failed to save vec score expr result", K(ret));
      }
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::fetch_vids_from_vec_index(share::ObPluginVectorIndexAdaptor* adaptor, int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(vec_index_scan_iter_) || OB_ISNULL(adaptor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vec_index_scan_iter or adaptor is null", K(ret));
  } else {
    share::ObVectorQueryVidIterator *adaptor_vid_iter = nullptr;
    int64_t dummy_count = 0;
    vec_index_scan_iter_->clear_evaluated_flag();
    if (OB_FAIL(vec_index_scan_iter_->get_next_rows(dummy_count, capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next rows from vec index scan iter to trigger init", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
    adaptor_vid_iter = vec_index_scan_iter_->get_adaptor_vid_iter();

    if (OB_SUCC(ret) && OB_NOT_NULL(adaptor_vid_iter)) {
      int64_t total_count = adaptor_vid_iter->get_total();
      iter_unfiltered_vid_cnt_ = total_count;
      iter_scan_total_num_ += total_count;

      bool need_bitmap = filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_SEARCH_DRIVER_FILTER
                         && nullptr != filter_iter_;
      if (need_bitmap && total_count > 0) {
        ObVidBound bound;
        const uint64_t MAX_HNSW_BRUTE_FORCE_SIZE = 20000;
        if (OB_FAIL(adaptor->get_vid_bound(bound))) {
          LOG_WARN("failed to get vid bound", K(ret));
        } else if (OB_FAIL(bitmap_.init(bound.min_vid_, bound.max_vid_, MAX_HNSW_BRUTE_FORCE_SIZE))) {
          LOG_WARN("failed to init bitmap", K(ret));
        } else {
          const int64_t *vids = adaptor_vid_iter->get_vids();
          const float *distances = adaptor_vid_iter->get_distance();

          if (OB_ISNULL(vids) || OB_ISNULL(distances)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("vids or distances is null", K(ret), KP(vids), KP(distances), K(total_count));
          } else if (OB_FAIL(vid_to_distance_.create(total_count, "VidToDistMap"))) {
            LOG_WARN("failed to create vid to distance hash map", K(ret), K(total_count));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < total_count; ++i) {
              int64_t vid = vids[i];
              float distance = distances[i];

              if (OB_FAIL(bitmap_.add_vid(vid))) {
                LOG_WARN("failed to add vid to bitmap", K(ret), K(vid), K(i));
              } else if (OB_FAIL(vid_to_distance_.set_refactored(vid, distance))) {
                LOG_WARN("failed to set vid to distance mapping", K(ret), K(vid), K(distance));
              }
            }
          }
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::init_adaptor_vid_iter_if_null()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(adaptor_vid_iter_)) {
    void *iter_buff = nullptr;
    uint64_t final_res_cnt = limit_param_.limit_ + limit_param_.offset_;
    int64_t extra_info_actual_size = 0;
    int64_t extra_column_count = 0;

    if (is_hnsw_bq()) {
      final_res_cnt = get_reorder_count(query_cond_.ef_search_, final_res_cnt, search_param_);
    }

    if (OB_ISNULL(iter_buff = vec_op_alloc_.alloc(sizeof(ObVectorQueryVidIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate adaptor vid iter", K(ret));
    } else if (OB_FALSE_IT(adaptor_vid_iter_ = new(iter_buff)
                            ObVectorQueryVidIterator(extra_column_count, extra_info_actual_size,
                                                     query_cond_.rel_count_, query_cond_.rel_map_ptr_))) {
    } else if (OB_FAIL(adaptor_vid_iter_->init(final_res_cnt, &vec_op_alloc_))) {
      LOG_WARN("adaptor vid iter init failed", K(ret));
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::post_query_vid_with_filter()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(filter_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter_iter or adaptor is null", K(ret));
  } else if (OB_FAIL(init_adaptor_vid_iter_if_null())) {
    LOG_WARN("failed to init adaptor vid iter", K(ret));
  } else if (iter_unfiltered_vid_cnt_ > 0) {
    iter_added_cnt_ = 0;

    ObEvalCtx *eval_ctx = vec_index_driver_rtdef_->eval_ctx_;
    int64_t batch_row_count = eval_ctx->max_batch_size_ > 0 ?
                              min(eval_ctx->max_batch_size_, ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE) :
                              ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;

    struct VidDistancePair {
      int64_t vid;
      float distance;
      static bool compare(const VidDistancePair &a, const VidDistancePair &b)
      {
        return a.distance < b.distance;
      }
    };
    VidDistancePair *all_pairs = nullptr;
    int64_t valid_total_cnt = 0;
    int64_t allocated_capacity = iter_unfiltered_vid_cnt_;

    if (OB_ISNULL(all_pairs = static_cast<VidDistancePair *>(
                  mem_context_->get_arena_allocator().alloc(sizeof(VidDistancePair) * allocated_capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate all pairs", K(ret), K(allocated_capacity));
    } else {
      bool filter_end = false;
      while (OB_SUCC(ret) && !filter_end) {
        int64_t scan_row_cnt = 0;
        filter_iter_->clear_evaluated_flag();

        if (OB_FAIL(filter_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to get next rows from filter iter", K(ret));
          } else {
            filter_end = true;
            ret = OB_SUCCESS;
          }
        }

        if (OB_SUCC(ret) && scan_row_cnt > 0) {
          ObExpr *vid_expr = nullptr;
          if (OB_ISNULL(search_ctx_) || OB_UNLIKELY(search_ctx_->get_rowid_exprs().count() <= 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("search ctx or rowid exprs is invalid", K(ret));
          } else if (OB_ISNULL(vid_expr = search_ctx_->get_rowid_exprs().at(0))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("vid expr is null", K(ret));
          } else {
            ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
            guard.set_batch_size(scan_row_cnt);

            int64_t filtered_vid = 0;
            bool use_rich_format = vid_expr->enable_rich_format() && is_valid_format(vid_expr->get_format(*eval_ctx));
            ObIVector *vec = nullptr;
            ObDatum *vid_datums = nullptr;

            if (use_rich_format) {
              if (OB_ISNULL(vec = vid_expr->get_vector(*eval_ctx))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("vector is null", K(ret));
              }
            } else {
              if (OB_ISNULL(vid_datums = vid_expr->locate_batch_datums(*eval_ctx))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("vid datums is null", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
                if (use_rich_format) {
                  filtered_vid = vec->get_uint64(i);
                } else {
                  guard.set_batch_idx(i);
                  filtered_vid = vid_datums[i].get_uint64();
                }

                float distance = 0.0f;
                if (OB_FAIL(vid_to_distance_.get_refactored(filtered_vid, distance))) {
                  LOG_WARN("failed to get distance from hash map", K(ret), K(filtered_vid));
                } else if (OB_UNLIKELY(valid_total_cnt >= allocated_capacity)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("valid total cnt is greater than allocated capacity", K(ret), K(valid_total_cnt), K(allocated_capacity));
                } else {
                  all_pairs[valid_total_cnt].vid = filtered_vid;
                  all_pairs[valid_total_cnt].distance = distance;
                  valid_total_cnt++;
                }
              }
            }
          }
        }
      }

      if (OB_SUCC(ret) && valid_total_cnt > 0) {
        lib::ob_sort(all_pairs, all_pairs + valid_total_cnt, VidDistancePair::compare);

        for (int64_t i = 0; OB_SUCC(ret) && i < valid_total_cnt && !adaptor_vid_iter_->get_enough(); ++i) {
          if (OB_FAIL(adaptor_vid_iter_->add_result(all_pairs[i].vid, all_pairs[i].distance, nullptr))) {
            LOG_WARN("failed to add result to adaptor vid iter", K(ret), K(all_pairs[i].vid), K(all_pairs[i].distance));
          } else {
            iter_added_cnt_++;
          }
        }
      }

      bitmap_.reset();
      if (vid_to_distance_.created()) {
        vid_to_distance_.destroy();
      }
    }
  }

  return ret;
}

bool ObDASVecIndexDriverIter::can_be_last_search(int64_t old_ef,
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
  LOG_TRACE("iteractive filter log: can be last search", K(ret_bool), K(old_ef), K(need_cnt_next), K(select_ratio));
  return ret_bool;
}

int ObDASVecIndexDriverIter::adjust_vector_query_condition(bool first_search)
{
  int ret = OB_SUCCESS;
  if (first_search) {
    query_cond_.query_limit_ = std::max(query_cond_.ef_search_,
                                        static_cast<int64_t>(std::ceil(query_cond_.query_limit_ * FIXED_MAGNIFICATION_RATIO)));
    query_cond_.ef_search_ = std::max(query_cond_.ef_search_, static_cast<int64_t>(query_cond_.query_limit_));
    query_cond_.ef_search_ = query_cond_.ef_search_ > VSAG_MAX_EF_SEARCH ? VSAG_MAX_EF_SEARCH : query_cond_.ef_search_;
    LOG_TRACE("iteractive filter first search: adjust query condition",
              K(query_cond_.query_limit_), K(query_cond_.ef_search_));
  } else {
    if (OB_ISNULL(adaptor_vid_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("adaptor vid iter is null", K(ret));
    } else {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      int64_t hnsw_max_iter_scan_nums = 0;
      if (tenant_config.is_valid()) {
        hnsw_max_iter_scan_nums = tenant_config->_hnsw_max_scan_vectors;
      }

      if (iter_unfiltered_vid_cnt_ < query_cond_.query_limit_
          || (hnsw_max_iter_scan_nums > 0 && iter_scan_total_num_ > hnsw_max_iter_scan_nums)) {
        LOG_TRACE("iteractive filter: stop search",
                  K(iter_unfiltered_vid_cnt_), K(query_cond_.query_limit_),
                  K(adaptor_vid_iter_->get_total()),
                  K(iter_scan_total_num_), K(hnsw_max_iter_scan_nums));
        query_cond_.query_limit_ = 0;
      } else {
        int64_t need_cnt_next = adaptor_vid_iter_->get_alloc_size() - adaptor_vid_iter_->get_total();
        int64_t total_after_add = adaptor_vid_iter_->get_total();
        int64_t added_cnt = iter_added_cnt_;
        float old_limit = static_cast<float>(query_cond_.query_limit_);
        float old_ef = static_cast<float>(query_cond_.ef_search_);

        if (need_cnt_next > 0) {
          float select_ratio = iter_unfiltered_vid_cnt_ > 0 ?
                                static_cast<float>(added_cnt) / static_cast<float>(iter_unfiltered_vid_cnt_) : 0.0f;

          uint32_t new_limit = 0;
          int64_t new_ef = old_ef;

          if (added_cnt == 0) {
            new_limit = static_cast<int64_t>(old_ef * FIXED_MAGNIFICATION_RATIO);
            new_ef = std::max(query_cond_.ef_search_, static_cast<int64_t>(new_limit));
            new_ef = new_ef > VSAG_MAX_EF_SEARCH ? VSAG_MAX_EF_SEARCH : new_ef;
            query_cond_.is_last_search_ = false;
          } else {
            int64_t need_res_cnt = OB_MIN(static_cast<int64_t>(std::ceil(need_cnt_next / select_ratio)),
                                   query_cond_.query_limit_ * FIXED_MAGNIFICATION_RATIO_EACH_ITERATIVE);
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
          if (is_hnsw_bq()) {
            query_cond_.query_limit_ = get_reorder_count(new_ef, new_limit, search_param_);
          }
          query_cond_.ef_search_ = new_ef;

          LOG_TRACE("iteractive filter: adjust query condition",
                    K(total_after_add), K(iter_unfiltered_vid_cnt_),
                    K(select_ratio), K(old_limit), K(new_limit), K(old_ef), K(new_ef),
                    K(query_cond_.query_limit_), K(query_cond_.ef_search_), K(added_cnt), K(need_cnt_next));
        } else {
          query_cond_.query_limit_ = 0;
          LOG_TRACE("iteractive filter: enough results", K(total_after_add), K(adaptor_vid_iter_->get_alloc_size()));
        }
      }
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::process_post_filter_mode(share::ObPluginVectorIndexAdaptor* adaptor, int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  bool first_search = true;
  if (OB_FAIL(adjust_vector_query_condition(first_search))) {
    LOG_WARN("failed to adjust vector query condition", K(ret));
  } else if (OB_FAIL(fetch_vids_from_vec_index(adaptor, count, capacity))) {
    LOG_WARN("failed to fetch vids from vec index", K(ret));
  } else if (OB_FAIL(set_bitmap_to_filter_iter())) {
    LOG_WARN("failed to set bitmap to filter iter", K(ret));
  } else if (OB_NOT_NULL(filter_iter_)) {
    if (filter_mode_ == ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER) {
      if (OB_FAIL(post_query_vid_with_expr_filter())) {
        LOG_WARN("failed to post query vid with expr filter", K(ret));
      }
    } else {
      if (OB_FAIL(post_query_vid_with_filter())) {
        LOG_WARN("failed to post query vid with filter", K(ret));
      }
    }
  } else if (OB_ISNULL(filter_iter_)) {
    ObVectorQueryVidIterator *adaptor_vid_iter = vec_index_scan_iter_->get_adaptor_vid_iter();
    if (OB_ISNULL(adaptor_vid_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("adaptor vid iter is null", K(ret));
    } else {
      adaptor_vid_iter->set_total(limit_param_.limit_ + limit_param_.offset_);
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObDASVecIndexDriverIter::save_vec_socre_expr_result(ObNewRow *row, int64_t size)
{
  int ret = OB_SUCCESS;

  ObEvalCtx *eval_ctx = vec_index_driver_rtdef_->eval_ctx_;
  int64_t a_batch_obj_cnt = 2;
  if (OB_ISNULL(score_expr_) || OB_ISNULL(row) || OB_ISNULL(eval_ctx) || OB_ISNULL(sort_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score expr or row or eval ctx or sort expr is null", K(ret), K(score_expr_), K(row), K(eval_ctx), K(sort_expr_));
  } else if (score_expr_->enable_rich_format()) {
    if (OB_FAIL(score_expr_->init_vector_for_write(*eval_ctx, score_expr_->get_default_res_format(), size))) {
      LOG_WARN("failed to init vector for write", K(ret));
    } else {
      ObIVector *vec = score_expr_->get_vector(*eval_ctx);
      for (int64_t num = 0; OB_SUCC(ret) && num < size; ++num) {
        double distance = static_cast<double>(row->get_cell(num * a_batch_obj_cnt).get_float());
        double score = 0;
        if (OB_FAIL(distance_to_score(distance, score))) {
          LOG_WARN("failed to convert distance to score", K(ret));
        } else {
          vec->set_double(num, score);
        }
      }
    }
  } else {
    ObDatum *score_datum = nullptr;
    ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
    guard.set_batch_size(size);
    if (OB_ISNULL(score_datum = score_expr_->locate_datums_for_update(*eval_ctx, size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, datums is nullptr", K(ret), KPC(score_expr_));
    } else {
      for (int64_t num = 0; OB_SUCC(ret) && num < size; ++num) {
        guard.set_batch_idx(num);
        double distance = static_cast<double>(row->get_cell(num * a_batch_obj_cnt).get_float());
        double score = 0;
        if (OB_FAIL(distance_to_score(distance, score))) {
          LOG_WARN("failed to convert distance to score", K(ret));
        } else {
          score_datum[num].set_double(score);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    score_expr_->set_evaluated_projected(*eval_ctx);
  }

  return ret;
}

int ObDASVecIndexDriverIter::distance_to_score(double distance, double &score)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sort_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort expr is null", K(ret));
  } else if (sort_expr_->type_ == T_FUN_SYS_L2_DISTANCE) {
    score = 1.0 / (1.0 + distance);
  } else if (sort_expr_->type_ == T_FUN_SYS_COSINE_DISTANCE) {
    score = 1.0 - (distance / 2.0);
  } else if (sort_expr_->type_ == T_FUN_SYS_NEGATIVE_INNER_PRODUCT) {
    if (distance > 0.0) {
      score = 1.0 / (1.0 + distance);
    } else {
      score = 1.0 - distance;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support sort expr type", K(ret), K(sort_expr_->type_));
  }

  return ret;
}

int ObDASVecIndexDriverIter::post_query_vid_with_expr_filter()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(filter_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter iter or adaptor is null", K(ret));
  } else if (OB_FAIL(init_adaptor_vid_iter_if_null())) {
    LOG_WARN("failed to init adaptor vid iter", K(ret));
  } else {
    iter_added_cnt_ = 0;
    int64_t total_before_add = adaptor_vid_iter_->get_total();

    share::ObVectorQueryVidIterator *adaptor_vid_iter = vec_index_scan_iter_->get_adaptor_vid_iter();
    if (OB_ISNULL(adaptor_vid_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("adaptor vid iter is null", K(ret));
    } else {
      int64_t total_count = adaptor_vid_iter->get_total();
      const int64_t *vids = adaptor_vid_iter->get_vids();
      const float *distances = adaptor_vid_iter->get_distance();

      for (int64_t i = 0; OB_SUCC(ret) && i < total_count && !adaptor_vid_iter_->get_enough(); ++i) {
        bool filter_res = false;
        int64_t vid = vids[i];
        float distance = distances[i];

        if (OB_FAIL(filter_by_index_back(vid, distance, filter_res))) {
          LOG_WARN("failed to filter by index back", K(ret), K(vid));
        } else if (!filter_res) {
          // do nothing
        } else if (OB_FAIL(adaptor_vid_iter_->add_result(vid, distance, nullptr))) {
          LOG_WARN("failed to add result to adaptor vid iter", K(ret), K(vid), K(distance));
        } else {
          iter_added_cnt_++;
        }
      }
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::filter_by_index_back(int64_t vid, float distance, bool &filter_res)
{
  int ret = OB_SUCCESS;
  filter_res = false;
  UNUSED(distance);

  if (filter_mode_ != ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter_by_index_back should only be called in expr filter mode",
             K(ret), K(filter_mode_));
  } else if (OB_ISNULL(filter_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter iter is null", K(ret));
  } else if (OB_FAIL(filter_iter_->reuse())) {
    LOG_WARN("failed to reuse filter iter", K(ret));
  } else if (OB_FAIL(set_rowkey_by_vid(vid))) {
    LOG_WARN("failed to set rowkey by vid", K(ret), K(vid));
  } else if (OB_FAIL(filter_iter_->rescan())) {
    LOG_WARN("failed to rescan filter iter", K(ret));
  } else {
    bool is_vectorized = vec_index_driver_rtdef_->eval_ctx_->is_vectorized();
    if (OB_FAIL(get_single_row_from_filter_iter(is_vectorized))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get single row from data filter iter", K(ret), K(vid));
      } else {
        ret = OB_SUCCESS;
        filter_res = false;
      }
    } else {
      filter_res = true;
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::set_rowkey_by_vid(int64_t vid)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(scalar_scan_ctdef_) || OB_ISNULL(scalar_scan_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar_scan_ctdef or scalar_scan_rtdef is null", K(ret), K(scalar_scan_ctdef_), K(scalar_scan_rtdef_));
  } else {
    vid_obj_for_lookup_.set_uint64(vid);
    ObRowkey vid_rowkey(&vid_obj_for_lookup_, 1);
    ObNewRange look_range;
    scalar_scan_rtdef_->key_ranges_.reset();
    if (OB_FAIL(look_range.build_range(scalar_scan_ctdef_->ref_table_id_, vid_rowkey))) {
      LOG_WARN("build lookup range failed", K(ret));
    } else if (OB_FAIL(scalar_scan_rtdef_->key_ranges_.push_back(look_range))) {
      LOG_WARN("failed to push back lookup range", K(ret));
    }
  }

  return ret;
}

int ObDASVecIndexDriverIter::get_single_row_from_filter_iter(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  filter_iter_->clear_evaluated_flag();
  if (is_vectorized) {
    int64_t scan_row_cnt = 0;
    ret = filter_iter_->get_next_rows(scan_row_cnt, 1);
  } else {
    ret = filter_iter_->get_next_row();
  }

  return ret;
}


} // namespace sql
} // namespace oceanbase
