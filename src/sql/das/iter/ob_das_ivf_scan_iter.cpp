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

    vec_aux_ctdef_ = ivf_scan_param.vec_aux_ctdef_;
    vec_aux_rtdef_ = ivf_scan_param.vec_aux_rtdef_;
    sort_ctdef_ = ivf_scan_param.sort_ctdef_;
    sort_rtdef_ = ivf_scan_param.sort_rtdef_;

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
            if (OB_FAIL(ObDasVecScanUtils::check_ivf_support_similarity_threshold(*sort_ctdef_->sort_exprs_[0]))) {
              LOG_WARN("check support similarity threshold fail", K(ret));
            } else {
              similarity_threshold_ = search_param_.similarity_threshold_;
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

  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  inv_idx_scan_iter_ = nullptr;
  centroid_iter_ = nullptr;
  cid_vec_iter_ = nullptr;
  rowkey_cid_iter_ = nullptr;
  brute_iter_ = nullptr;

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
  vec_op_alloc_.reset();
  persist_alloc_.reset();
  tx_desc_ = nullptr;
  snapshot_ = nullptr;

  ObDasVecScanUtils::release_scan_param(centroid_scan_param_);
  ObDasVecScanUtils::release_scan_param(cid_vec_scan_param_);
  ObDasVecScanUtils::release_scan_param(rowkey_cid_scan_param_);
  ObDasVecScanUtils::release_scan_param(brute_scan_param_);

  vec_aux_ctdef_ = nullptr;
  vec_aux_rtdef_ = nullptr;
  sort_ctdef_ = nullptr;
  sort_rtdef_ = nullptr;
  search_vec_ = nullptr;
  real_search_vec_ = nullptr;

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
    LOG_WARN("failed to get saved rowkeys", K(ret));
  }

  return ret;
}

int ObDASIvfBaseScanIter::process_ivf_scan(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  if (vec_aux_ctdef_->is_post_filter()) {
    if (OB_FAIL(process_ivf_scan_post(is_vectorized))) {
      LOG_WARN("failed to process ivf_scan post filter", K(ret), K_(pre_fileter_rowkeys), K_(saved_rowkeys));
    }
  } else if (OB_FAIL(process_ivf_scan_pre(mem_context_->get_arena_allocator(), is_vectorized))) {
    LOG_WARN("failed to process ivf_scan pre", K(ret));
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
  CenterSaveMode center_save_mode = save_center_vec ? CenterSaveMode::DEEP_COPY_CENTER_VEC : CenterSaveMode::NOT_SAVE_CENTER_VEC;
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
  if (OB_FAIL(vec_index_service->acquire_ivf_cache_mgr_guard(
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

int ObDASIvfBaseScanIter::generate_nearest_cid_heap(
    bool is_vectorized,
    share::ObVectorCenterClusterHelper<float, ObCenterId> &nearest_cid_heap,
    bool save_center_vec /*= false*/)
{
  int ret = OB_SUCCESS;
  ObIvfCacheMgrGuard cache_guard;
  ObIvfCentCache *cent_cache = nullptr;
  bool is_cache_usable = false;

  if (OB_FAIL(get_centers_cache(is_vectorized, false/*is_pq_centers*/, cache_guard, cent_cache, is_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(cent_cache));
  } else if (is_cache_usable) {
    if (OB_FAIL(gen_near_cid_heap_from_cache(*cent_cache, nearest_cid_heap, save_center_vec))) {
      LOG_WARN("fail to gen near cid heap from cache", K(ret), K(cache_guard));
    }
  } else {
    if (OB_FAIL(gen_near_cid_heap_from_table(is_vectorized, nearest_cid_heap, save_center_vec))) {
      LOG_WARN("fail to gen near cid heap from table", K(ret), K(cache_guard));
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
/*************************** implement ObDASIvfScanIter ****************************/
int ObDASIvfScanIter::inner_init(ObDASIterParam &param)
{
  int ret = ObDASIvfBaseScanIter::inner_init(param);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to init", K(ret));
  } else if (vec_aux_ctdef_->is_post_filter()) {
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
  if (vec_aux_ctdef_->is_post_filter()) {
    near_cid_.reuse();
  } else {
    memset(near_cid_dist_.get_data(), 0, near_cid_dist_.count() * sizeof(bool));
  }
  return ObDASIvfBaseScanIter::inner_reuse();
}

int ObDASIvfScanIter::inner_release()
{
  if (vec_aux_ctdef_->is_post_filter()) {
    near_cid_.reset();
  } else {
    near_cid_dist_.reset();
  }
  return ObDASIvfBaseScanIter::inner_release();
}

int ObDASIvfScanIter::get_nearest_probe_center_ids(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  share::ObVectorCenterClusterHelper<float, ObCenterId> nearest_cid_heap(
      mem_context_->get_arena_allocator(), reinterpret_cast<const float *>(real_search_vec_.ptr()), dis_type_, dim_, nprobes_, 0.0);
  if (OB_FAIL(generate_nearest_cid_heap(is_vectorized, nearest_cid_heap))) {
    LOG_WARN("failed to generate nearest cid heap", K(ret), K(nprobes_), K(dim_), K(real_search_vec_));
  }
  if (OB_SUCC(ret)) {
    if (nearest_cid_heap.get_center_count() == 0) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (vec_aux_ctdef_->is_post_filter()) {
      if (OB_FAIL(nearest_cid_heap.get_nearest_probe_center_ids(near_cid_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    } else if (OB_FAIL(nearest_cid_heap.get_nearest_probe_center_ids_dist(near_cid_dist_))) {
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
                                          bool &is_first_vec, bool &cid_vec_need_norm)
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

      for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
        guard.set_batch_idx(i);
        ObRowkey main_rowkey;
        ObString vec = cid_datum[i].get_string();
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                        &mem_context_->get_arena_allocator(),
                        ObLongTextType,
                        CS_TYPE_BINARY,
                        has_lob_header,
                        vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (OB_ISNULL(vec.ptr())) {
          // ignoring null vector.
        } else if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
          LOG_WARN("fail to get main rowkey", K(ret));
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
        } else if (OB_NOT_NULL(vec.ptr()) && OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, reinterpret_cast<T *>(vec.ptr()), dim_))) {
          LOG_WARN("failed to push center.", K(ret));
        }
      }
    }
    IVF_GET_NEXT_ROWS_END(cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_)
  } else {
    while (OB_SUCC(ret)) {
      ObRowkey main_rowkey;
      ObString vec;
      // cid_vec_scan_iter output: [IVF_CID_VEC_CID_COL IVF_CID_VEC_VECTOR_COL ROWKEY]
      if (OB_FAIL(cid_vec_scan_iter->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to scan vid rowkey iter", K(ret));
        }
      } else if (OB_FAIL(parse_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_column_count, cid_vec_ctdef, rowkey_cnt, main_rowkey, vec))) {
        LOG_WARN("fail to parse cid vec datum", K(ret), K(cid_vec_column_count), K(rowkey_cnt));
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
      } else if (OB_NOT_NULL(vec.ptr()) &&
                 OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, reinterpret_cast<T *>(vec.ptr()), dim_))) {
        LOG_WARN("failed to push center.", K(ret));
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
int ObDASIvfScanIter::get_nearest_limit_rowkeys_in_cids(bool is_vectorized, T *serch_vec)
{
  int ret = OB_SUCCESS;

  int64_t enlargement_factor = (selectivity_ != 0 && selectivity_ != 1) ? POST_ENLARGEMENT_FACTOR : 1;
  share::ObVectorCenterClusterHelper<T, ObRowkey> nearest_rowkey_heap(
      vec_op_alloc_, serch_vec, dis_type_, dim_, get_nprobe(limit_param_, enlargement_factor), similarity_threshold_);

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
                                    nearest_rowkey_heap, is_first_vec, cid_vec_need_norm))) {
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
                                             is_vectorized, nearest_rowkey_heap, is_first_vec, cid_vec_need_norm))) {
        LOG_WARN("failed to get rowkeys to heap", K(ret), K(cur_cid));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
      LOG_WARN("failed to get top n", K(ret));
    }
  }

  return ret;
}

int ObDASIvfScanIter::process_ivf_scan_post(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_nearest_probe_center_ids(is_vectorized))) {
    // 1. Scan the centroid table, range: [min] ~ [max]; sort by l2_distance(c_vec, search_vec_); limit nprobes;
    // return the cid column.
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("failed to get nearest probe center ids", K(ret));
    } else {
      LOG_INFO("nearest probe center ids is empty", K(ret));
      ret = OB_SUCCESS;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to process_ivf_scan_post", K(ret));
  } else if (OB_FAIL(get_nearest_limit_rowkeys_in_cids<float>(is_vectorized,
                                                              reinterpret_cast<float *>(real_search_vec_.ptr())))) {
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
  float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
  ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;

  int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  // Firstly, scan whether it is an brute search.
  if (OB_FAIL(get_rowkey_pre_filter(vec_op_alloc_, is_vectorized, IVF_MAX_BRUTE_FORCE_SIZE))) {
    LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
  } else if (pre_fileter_rowkeys_.count() < IVF_MAX_BRUTE_FORCE_SIZE) {
    // do brute search
    IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec, raw_dis_type, dim_,
                                      get_nprobe(limit_param_, 1), similarity_threshold_);
    if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
      LOG_WARN("failed to get limit rowkey brute", K(ret));
    } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
      LOG_WARN("failed to get top n", K(ret));
    }
  } else {
    bool go_brute = false;
    if (OB_FAIL(get_nearest_probe_center_ids(is_vectorized))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        // cid_center table is empty, just do brute search
        ret = OB_SUCCESS;
        IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec /*unused*/, raw_dis_type, dim_,
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
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
          LOG_WARN("failed to get top n", K(ret));
        } else {
          go_brute = true;
        }
      } else {
        LOG_WARN("failed to get nearest probe center ids", K(ret));
      }
    } else if (OB_FAIL(filter_pre_rowkey_batch(is_vectorized, batch_row_count))) {
      LOG_WARN("failed to filter pre rowkey batch", K(ret));
    } else {
      pre_fileter_rowkeys_.reset();
    }

    bool index_end = false;
    while (!go_brute && OB_SUCC(ret) && !index_end) {
      if (OB_FAIL(get_pre_filter_rowkey_batch(vec_op_alloc_, is_vectorized, batch_row_count, index_end))) {
        // Take a batch of prefilter rowkeys and put them in pre_fileter_rowkeys_
        LOG_WARN("failed to get rowkey batch", K(ret), K(is_vectorized));
      } else if (OB_FAIL(filter_pre_rowkey_batch(is_vectorized, batch_row_count))) {
        // Filter the rowkeys and put the saved rowkeys
        LOG_WARN("failed to filter pre rowkey batch", K(ret));
      } else {
        pre_fileter_rowkeys_.reset();
      }
    }

    if (OB_SUCCESS != ret && OB_ITER_END != ret) {
      LOG_WARN("get next row failed.", K(ret));
    } else {
      ret = OB_SUCCESS;
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
  uint64_t tenant_cluster_version = GET_MIN_CLUSTER_VERSION();
  if (OB_FAIL(ObDASIvfBaseScanIter::inner_init(param))) {
    LOG_WARN("fail to do inner init ", K(ret), K(param));
  } else if (!((tenant_cluster_version >= MOCK_CLUSTER_VERSION_4_3_5_3 &&
                tenant_cluster_version < CLUSTER_VERSION_4_4_0_0) ||
               tenant_cluster_version >= CLUSTER_VERSION_4_4_1_0)) {
    if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(vec_aux_ctdef_->vec_index_param_,
                                                             ObVectorIndexType::VIT_IVF_INDEX, index_param))) {
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
  } else if (vec_aux_ctdef_->is_post_filter()) {
    if (OB_FAIL(near_cid_vec_.reserve(nprobes_))) {
      LOG_WARN("failed to reserve nearest cid vec", K(ret), K(vec_index_param_.nlist_));
    } else if (OB_FAIL(near_cid_vec_dis_.reserve(nprobes_))) {
      LOG_WARN("failed to reserve nearest cid vec dis", K(ret), K(vec_index_param_.nlist_));
    }
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
  if (vec_aux_ctdef_->is_post_filter()) {
    near_cid_vec_.reuse();
    near_cid_vec_dis_.reuse();
  } else {
    memset(near_cid_vec_ptrs_.get_data(), 0, near_cid_vec_ptrs_.count() * sizeof(float *));
  }
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
          if (OB_SUCC(ret)) {
            guard.set_batch_idx(saved_j[1]);
            {
              if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
                LOG_WARN("fail to get main rowkey", K(ret));
              } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance_1))) {
                LOG_WARN("failed to push center.", K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
            guard.set_batch_idx(saved_j[2]);
            {
              if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(mem_context_->get_arena_allocator(), cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
                LOG_WARN("fail to get main rowkey", K(ret));
              } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance_2))) {
                LOG_WARN("failed to push center.", K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
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
        counter = 0;
      }
    }
  }
  // process counter left
  for (size_t kk = 0; kk < counter && OB_SUCC(ret); kk++) {
    const uint8_t* pq_id_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(cid_datum[saved_j[kk]].get_string().ptr());
    float dis = dis0 + ObVectorL2Distance<float>::distance_one_code(m_, nbits_, sim_table, pq_id_ptr);
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
    ObIvfPreFilter *prefilter)
{
  int ret = OB_SUCCESS;

  int64_t sub_dim = dim_ / m_;
  int64_t ksub = 1L << nbits_;
  IvfRowkeyHeap nearest_rowkey_heap(
      vec_op_alloc_, search_vec/*unused*/, dis_type_, sub_dim,
      get_nprobe(limit_param_, 1), similarity_threshold_); // pq do not need to
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

  // 2. get nearest probe
  if (OB_SUCC(ret)) {
    if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
      LOG_WARN("failed to get top n", K(ret));
    }

  }
  return ret;
}

int ObDASIvfPQScanIter::get_nearest_probe_centers(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  share::ObVectorCenterClusterHelper<float, ObCenterId> nearest_cid_heap(
      mem_context_->get_arena_allocator(), reinterpret_cast<const float *>(real_search_vec_.ptr()), dis_type_, dim_, nprobes_, 0.0);
  if (OB_FAIL(generate_nearest_cid_heap(is_vectorized, nearest_cid_heap, true/*save_center_vec*/))) {
    LOG_WARN("failed to generate nearest cid heap", K(ret), K(nprobes_), K(dim_), K(real_search_vec_));
  }
  if (OB_SUCC(ret)) {
    if (nearest_cid_heap.get_center_count() == 0) {
      ret = OB_ENTRY_NOT_EXIST;
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
  // 1. Scan the ivf_centroid table, calculate the distance between vec_x and cid_vec,
  //    and get the nearest cluster center (cid 1, cid_vec 1)... (cid n, cid_vec n)
  if (OB_FAIL(get_nearest_probe_centers(is_vectorized))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
      ObExprVectorDistance::ObVecDisType raw_dis_type = !need_norm_ ? dis_type_ : ObExprVectorDistance::ObVecDisType::COSINE;
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
        LOG_WARN("failed to get limit rowkey brute", K(ret));
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    } else {
      LOG_WARN("failed to get nearest probe center ids", K(ret));
    }
  } else if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(
      is_vectorized,
      reinterpret_cast<float *>(real_search_vec_.ptr()),
      nullptr))) {
    // 2. search nearest rowkeys
    LOG_WARN("fail to calc nearest limit rowkeys in cids", K(ret), K(dim_));
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

  bool is_rk_opt = vec_aux_ctdef_->can_use_vec_pri_opt();
  ObDASScanIter* inv_iter = (ObDASScanIter*)inv_idx_scan_iter_;
  bool is_range_prefilter = is_rk_opt && (inv_iter->get_scan_param().pd_storage_filters_ == nullptr);
  ObIvfPreFilter prefilter(MTL_ID());
  // 1. Scan the ivf_centroid table, calculate the distance between vec_x and cid_vec,
  //    and get the nearest cluster center (cid 1, cid_vec 1)... (cid n, cid_vec n)
  if (OB_FAIL(get_nearest_probe_centers(is_vectorized))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      // cid_center table is empty, just do brute search
      ret = OB_SUCCESS;
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
      IvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec/*unused*/, raw_dis_type, dim_, get_nprobe(limit_param_, 1), similarity_threshold_);
      if (OB_FAIL(get_rowkey_brute_post(is_vectorized, nearest_rowkey_heap))) {
        LOG_WARN("failed to get limit rowkey brute", K(ret));
      } else if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    } else { // search with prefilter
      if (is_range_prefilter) { // rowkey range prefilter
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
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(
        is_vectorized,
        reinterpret_cast<float *>(real_search_vec_.ptr()),
        &prefilter))) {
        // 2. search nearest rowkeys
        LOG_WARN("fail to calc nearest limit rowkeys in cids", K(ret), K(dim_));
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

uint64_t ObDASIvfPQScanIter::hash_val_for_rk(const common::ObRowkey& rk)
{
  uint64_t hash_val = 0;
  for (int i = 0; i < rk.get_obj_cnt(); i++) {
    (void)rk.get_obj_ptr()[i].hash(hash_val, hash_val);
  }
  return hash_val;
}

int ObDASIvfPQScanIter::build_rowkey_hash_set(
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
  // post scan:
  //   1. Obtain nproc cids
  //   2. real_search_vec_ to real_search_vec_unit8
  //   3. Query ivf_cid_vector, retrieve all (rowkey, data_vec) pairs under each cid, calculate the distance between
  //   data_vec and vec_x_uint8, and select the top K.
  ObString real_search_vec_u8;
  if (OB_FAIL(get_nearest_probe_center_ids(is_vectorized))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("failed to get nearest probe center ids", K(ret));
    } else {
      LOG_INFO("nearest probe center ids is empty", K(ret));
      ret = OB_SUCCESS;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to process_ivf_scan_post", K(ret));
  } else if (OB_FAIL(get_real_search_vec_u8(is_vectorized, real_search_vec_u8))) {
    // real_search_vec to real_search_vec_u8 with min_vec max_vec step_vec
    LOG_WARN("failed to get real search vec u8", K(ret));
  } else if (OB_FAIL(get_nearest_limit_rowkeys_in_cids<uint8_t>(
                 is_vectorized, reinterpret_cast<uint8_t *>(real_search_vec_u8.ptr())))) {
    LOG_WARN("failed to get nearest limit rowkeys in cids", K(near_cid_));
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
    for (int i = 0; i < main_rowkey.get_obj_cnt(); i++) {
      (void)main_rowkey.get_obj_ptr()[i].hash(hash_val, hash_val);
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


}  // namespace sql
}  // namespace oceanbase
