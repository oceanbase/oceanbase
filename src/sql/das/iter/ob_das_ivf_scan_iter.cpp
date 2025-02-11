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

int ObDASIvfBaseScanIter::do_table_full_scan(bool &first_scan,
                                             ObTableScanParam &scan_param,
                                             const ObDASScanCtDef *ctdef,
                                             ObDASScanRtDef *rtdef,
                                             ObDASScanIter *iter,
                                             int64_t pri_key_cnt,
                                             ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  if (first_scan) {
    ObNewRange scan_range;
    if (OB_FAIL(ObDasVecScanUtils::init_vec_aux_scan_param(
            ls_id_, tablet_id, ctdef, rtdef, tx_desc_, snapshot_, scan_param))) {
      LOG_WARN("failed to init scan param", K(ret));
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
            ls_id_, tablet_id, ctdef, rtdef, tx_desc_, snapshot_, scan_param, false))) {
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
  }

  if (OB_NOT_NULL(saved_rowkeys_itr_)) {
    saved_rowkeys_itr_->reset();
    saved_rowkeys_itr_->~ObVectorQueryRowkeyIterator();
    saved_rowkeys_itr_ = nullptr;
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

    vec_aux_ctdef_ = ivf_scan_param.vec_aux_ctdef_;
    vec_aux_rtdef_ = ivf_scan_param.vec_aux_rtdef_;
    sort_ctdef_ = ivf_scan_param.sort_ctdef_;
    sort_rtdef_ = ivf_scan_param.sort_rtdef_;

    if (OB_NOT_NULL(sort_ctdef_) && OB_NOT_NULL(sort_rtdef_)) {
      ObExpr *distance_calc = nullptr;
      if (OB_FAIL(
              ObDasVecScanUtils::init_limit(vec_aux_ctdef_, vec_aux_rtdef_, sort_ctdef_, sort_rtdef_, limit_param_))) {
        LOG_WARN("failed to init limit", K(ret), KPC(vec_aux_ctdef_), KPC(vec_aux_rtdef_));
      } else if (OB_FAIL(ObDasVecScanUtils::init_sort(
                     vec_aux_ctdef_, vec_aux_rtdef_, sort_ctdef_, sort_rtdef_, limit_param_, search_vec_, distance_calc))) {
        LOG_WARN("failed to init sort", K(ret), KPC(vec_aux_ctdef_), KPC(vec_aux_rtdef_));
      } else if (OB_FAIL(ob_write_string(vec_op_alloc_, vec_aux_ctdef_->vec_index_param_, vec_index_param_))) {
        LOG_WARN("failed to set vec index param", K(ret));
      } else if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(vec_op_alloc_, sort_rtdef_, search_vec_, real_search_vec_))) {
        LOG_WARN("failed to get real search vec", K(ret));
      } else if (OB_FAIL(ObDasVecScanUtils::get_distance_expr_type(
                     *sort_ctdef_->sort_exprs_[0], *sort_rtdef_->eval_ctx_, dis_type_))) {
        LOG_WARN("failed to get distance type.", K(ret));
      } else {
        ObSQLSessionInfo *session = nullptr;
        uint64_t ob_ivf_nprobes = 0;
        if (OB_ISNULL(session = sort_rtdef_->eval_ctx_->exec_ctx_.get_my_session())) {
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

    dim_ = vec_aux_ctdef_->dim_;

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

  // return first error code
  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }

  inv_idx_scan_iter_ = nullptr;
  centroid_iter_ = nullptr;
  cid_vec_iter_ = nullptr;
  rowkey_cid_iter_ = nullptr;

  if (OB_NOT_NULL(saved_rowkeys_itr_)) {
    saved_rowkeys_itr_->reset();
    saved_rowkeys_itr_->~ObVectorQueryRowkeyIterator();
    saved_rowkeys_itr_ = nullptr;
  }

  saved_rowkeys_.reset();
  pre_fileter_rowkeys_.reset();
  vec_op_alloc_.reset();
  tx_desc_ = nullptr;
  snapshot_ = nullptr;

  ObDasVecScanUtils::release_scan_param(centroid_scan_param_);
  ObDasVecScanUtils::release_scan_param(cid_vec_scan_param_);
  ObDasVecScanUtils::release_scan_param(rowkey_cid_scan_param_);

  vec_aux_ctdef_ = nullptr;
  vec_aux_rtdef_ = nullptr;
  sort_ctdef_ = nullptr;
  sort_rtdef_ = nullptr;
  search_vec_ = nullptr;
  real_search_vec_ = nullptr;

  return ret;
}

int ObDASIvfBaseScanIter::build_cid_vec_query_rowkey(const ObString &cid,
                                                     bool is_min,
                                                     int64_t rowkey_cnt,
                                                     common::ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;

  ObObj *obj_ptr = nullptr;
  if (OB_ISNULL(obj_ptr = static_cast<ObObj *>(vec_op_alloc_.alloc(sizeof(ObObj) * rowkey_cnt)))) {
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
                                                   rowkey_cid_scan_param_))) {
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
    ObRowkey *rowkey = nullptr;
    if (OB_FAIL(saved_rowkeys_itr_->get_next_row(rowkey))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next next row from adaptor vid iter", K(ret));
      }
    } else if (OB_ISNULL(rowkey)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be null", K(ret));
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
          if (OB_FAIL(datum.from_obj(rowkey->get_obj_ptr()[i]))) {
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
    ObSEArray<ObRowkey *, 8> rowkeys;
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
            if (OB_FAIL(datum[idx_key].from_obj(rowkeys[idx_key]->get_obj_ptr()[idx_exp]))) {
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
/*************************** implement ObDASIvfScanIter ****************************/

int ObDASIvfScanIter::parse_centroid_datum_with_deep_copy(
    const ObDASScanCtDef *centroid_ctdef,
    ObIAllocator& allocator,
    blocksstable::ObDatumRow *datum_row,
    ObString &cid,
    ObString &cid_vec)
{
  int ret = OB_SUCCESS;
  cid.reset();
  cid_vec.reset();
  ObString tmp_cid;
  ObString tmp_cid_vec;
  if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row invalid.", K(ret));
  } else if (datum_row->get_column_count() != CENTROID_ALL_KEY_CNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
  } else if (OB_FALSE_IT(tmp_cid = datum_row->storage_datums_[0].get_string())) {
  } else if (OB_FALSE_IT(tmp_cid_vec = datum_row->storage_datums_[1].get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  &allocator,
                  ObLongTextType,
                  CS_TYPE_BINARY,
                  centroid_ctdef->result_output_.at(1)->obj_meta_.has_lob_header(),
                  tmp_cid_vec))) {
    LOG_WARN("failed to get real data.", K(ret));
  } else if (OB_ISNULL(tmp_cid_vec.ptr())) {
    // ignoring null vector.
  } else if (OB_FAIL(ob_write_string(allocator, tmp_cid, cid))) {
    LOG_WARN("failed to write string", K(ret), K(tmp_cid));
  } else if (OB_FAIL(ob_write_string(allocator, tmp_cid_vec, cid_vec))) {
    LOG_WARN("failed to write string", K(ret), K(tmp_cid_vec));
  }
  return ret;
}

int ObDASIvfScanIter::generate_nearear_cid_heap(
  share::ObVectorCentorClusterHelper<float, ObString> &nearest_cid_heap,
  bool save_center_vec /*= false*/)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *centroid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_centroid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN);
  ObDASScanRtDef *centroid_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_centroid_tbl_idx());
  if (OB_FAIL(do_table_full_scan(centroid_iter_first_scan_,
                                        centroid_scan_param_,
                                        centroid_ctdef,
                                        centroid_rtdef,
                                        centroid_iter_,
                                        CENTROID_PRI_KEY_CNT,
                                        centroid_tablet_id_))) {
    LOG_WARN("failed to do centroid table scan", K(ret));
  } else {
    storage::ObTableScanIterator *centroid_scan_iter =
        static_cast<storage::ObTableScanIterator *>(centroid_iter_->get_output_result_iter());
    if (OB_ISNULL(centroid_scan_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("real centroid iter is null", K(ret));
    } else {
      // get vec in centroid_iter and get l2_distance(c_vec, search_vec_)
      // push in max_cid_heap
      const ObDASScanCtDef *centroid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
          vec_aux_ctdef_->get_ivf_centroid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN);

      ObString c_vec;
      ObString c_id;
      while (OB_SUCC(ret)) {
        blocksstable::ObDatumRow *datum_row = nullptr;
        if (OB_FAIL(centroid_scan_iter->get_next_row(datum_row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row failed.", K(ret));
          }
        } else if (OB_FAIL(parse_centroid_datum_with_deep_copy(centroid_ctdef, vec_op_alloc_, datum_row, c_id, c_vec))) {
          LOG_WARN("fail to deep copy centroid datum", K(ret));
        } else if (c_vec.empty()) {
          // ignoring null vector.
        } else if (OB_FAIL(nearest_cid_heap.push_center(c_id, reinterpret_cast<float *>(c_vec.ptr()), dim_, save_center_vec))) {
          LOG_WARN("failed to push center.", K(ret));
        }
      } // end while

      if (ret == OB_ITER_END) {
        if (OB_FAIL(centroid_iter_->reuse())) {
          LOG_WARN("fail to reuse scan iterator.", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDASIvfScanIter::get_nearest_probe_center_ids(ObIArray<ObString> &nearest_cids)
{
  int ret = OB_SUCCESS;
  share::ObVectorCentorClusterHelper<float, ObString> nearest_cid_heap(
      vec_op_alloc_, reinterpret_cast<const float *>(real_search_vec_.ptr()), dim_, nprobes_);
  if (OB_FAIL(nearest_cids.reserve(nprobes_))) {
    LOG_WARN("failed to reserve nearest_cids", K(ret));
  } else if (OB_FAIL(generate_nearear_cid_heap(nearest_cid_heap))) {
    LOG_WARN("failed to generate nearest cid heap", K(ret), K(nprobes_), K(dim_), K(real_search_vec_));
  }
  if (OB_SUCC(ret) || ret == OB_ITER_END) {
    if (OB_FAIL(nearest_cid_heap.get_nearest_probe_center_ids(nearest_cids))) {
      LOG_WARN("failed to get top n", K(ret));
    } else if (nearest_cids.count() == 0) {
      // push an empty ObString, for get all cid in cid_vec_table
      if (OB_FAIL(nearest_cids.push_back(ObString()))) {
        LOG_WARN("failed to push back empty ObString", K(ret));
      }
    }
  }
  return ret;
}

int ObDASIvfScanIter::get_main_rowkey_from_cid_vec_datum(const ObDASScanCtDef *cid_vec_ctdef,
                                                         const int64_t rowkey_cnt,
                                                         ObRowkey *&main_rowkey)
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
    } else if (OB_ISNULL(buf = vec_op_alloc_.alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(rowkey_cnt));
    } else if (OB_FALSE_IT(obj_ptr = new (buf) ObObj[rowkey_cnt])) {
    } else if (OB_ISNULL(main_rowkey = static_cast<ObRowkey *>(vec_op_alloc_.alloc(sizeof(ObRowkey))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for ObObj", K(ret));
    } else {
      int rowkey_idx = 0;
      for (int64_t i = 2; OB_SUCC(ret) && i < cid_vec_out_exprs.count() && rowkey_idx < rowkey_cnt; ++i) {
        ObObj tmp_obj;
        ObExpr *expr = cid_vec_out_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should not be null", K(ret));
        } else if (OB_FAIL(expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret));
        } else if (OB_FAIL(ob_write_obj(vec_op_alloc_, tmp_obj, obj_ptr[rowkey_idx++]))) {
          LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
        }
      }
    }
    if (OB_SUCC(ret)) {
      main_rowkey->assign(obj_ptr, rowkey_cnt);
    }
  }

  return ret;
}

int ObDASIvfScanIter::prepare_cid_range(
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

int ObDASIvfScanIter::scan_cid_range(
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

// for flat/sq, con_key is vector(float/uint8)
int ObDASIvfScanIter::parse_cid_vec_datum(
  int64_t cid_vec_column_count,
  const ObDASScanCtDef *cid_vec_ctdef,
  const int64_t rowkey_cnt,
  ObRowkey *&main_rowkey,
  ObString &com_key)
{
  int ret = OB_SUCCESS;
  ObExpr *vec_expr = cid_vec_ctdef->result_output_[1];
  if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
    LOG_WARN("failed to get main rowkey from cid vec datum", K(ret));
  } else if (OB_FALSE_IT(com_key = vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  &vec_op_alloc_,
                  ObLongTextType,
                  CS_TYPE_BINARY,
                  cid_vec_ctdef->result_output_.at(1)->obj_meta_.has_lob_header(),
                  com_key))) {
    LOG_WARN("failed to get real data.", K(ret));
  }
  return ret;
}

template <typename T>
int ObDASIvfScanIter::get_nearest_limit_rowkeys_in_cids(const ObIArray<ObString> &nearest_cids, T *serch_vec)
{
  int ret = OB_SUCCESS;

  share::ObVectorCentorClusterHelper<T, ObRowkey *> nearest_rowkey_heap(
      vec_op_alloc_, serch_vec, dim_, get_nprobe(limit_param_));
  const ObDASScanCtDef *cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN);
  ObDASScanRtDef *cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx());
  int64_t cid_vec_column_count = 0;
  int64_t cid_vec_pri_key_cnt = 0;
  int64_t rowkey_cnt = 0;

  if (OB_FAIL(prepare_cid_range(cid_vec_ctdef, cid_vec_column_count, cid_vec_pri_key_cnt, rowkey_cnt))) {
    LOG_WARN("fail to prepare cid range", K(ret));
  } else {
    // 3. Obtain nprobes * k rowkeys
    for (int64_t i = 0; OB_SUCC(ret) && i < nearest_cids.count(); ++i) {
      ObString cid = nearest_cids.at(i);
      storage::ObTableScanIterator *cid_vec_scan_iter = nullptr;
      if (OB_FAIL(scan_cid_range(cid, cid_vec_pri_key_cnt, cid_vec_ctdef, cid_vec_rtdef, cid_vec_scan_iter))) {
        LOG_WARN("fail to scan cid range", K(ret), K(cid), K(cid_vec_pri_key_cnt));
      } else {
        while (OB_SUCC(ret)) {
          ObRowkey *main_rowkey;
          ObString vec;
          // cid_vec_scan_iter output: [IVF_CID_VEC_CID_COL IVF_CID_VEC_VECTOR_COL ROWKEY]
          if (OB_FAIL(cid_vec_scan_iter->get_next_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to scan vid rowkey iter", K(ret));
            }
          } else if (OB_FAIL(parse_cid_vec_datum(cid_vec_column_count, cid_vec_ctdef, rowkey_cnt, main_rowkey, vec))) {
            LOG_WARN("fail to parse cid vec datum", K(ret), K(cid_vec_column_count), K(rowkey_cnt));
          } else if (OB_ISNULL(vec.ptr())) {
            // ignoring null vector.
          } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, reinterpret_cast<T *>(vec.ptr()), dim_))) {
            LOG_WARN("failed to push center.", K(ret));
          }
        }
      }

      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        if (OB_FAIL(cid_vec_iter_->reuse())) {
          LOG_WARN("fail to reuse scan iterator.", K(ret));
        }
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

int ObDASIvfScanIter::get_pre_filter_rowkey_batch(ObIAllocator &allocator,
                                                  bool is_vectorized,
                                                  int64_t batch_row_count,
                                                  bool &index_end)
{
  int ret = OB_SUCCESS;
  index_end = false;
  if (!is_vectorized) {
    for (int i = 0; OB_SUCC(ret) && i < batch_row_count && !index_end; ++i) {
      inv_idx_scan_iter_->clear_evaluated_flag();
      ObRowkey *rowkey;
      if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
        index_end = true;
      } else if (OB_FAIL(get_rowkey(allocator, rowkey))) {
        LOG_WARN("failed to get rowkey", K(ret));
      } else if (OB_FAIL(pre_fileter_rowkeys_.push_back(rowkey))) {
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
        ObRowkey *rowkey;
        if (OB_FAIL(get_rowkey(allocator, rowkey))) {
          LOG_WARN("failed to add rowkey", K(ret), K(i));
        } else if (OB_FAIL(pre_fileter_rowkeys_.push_back(rowkey))) {
          LOG_WARN("failed to save rowkey", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDASIvfScanIter::get_rowkey_pre_filter(bool is_vectorized, uint64_t &rowkey_count)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && rowkey_count < MAX_BRUTE_FORCE_SIZE) {
    inv_idx_scan_iter_->clear_evaluated_flag();
    if (!is_vectorized) {
      ObRowkey *rowkey;
      if (OB_FAIL(inv_idx_scan_iter_->get_next_row())) {
      } else if (OB_FALSE_IT(rowkey_count++)) {
      } else if (OB_FAIL(get_rowkey(vec_op_alloc_, rowkey))) {
        LOG_WARN("failed to get rowkey", K(ret));
      } else if (OB_FAIL(pre_fileter_rowkeys_.push_back(rowkey))) {
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
          ObRowkey *rowkey;
          if (OB_FAIL(get_rowkey(vec_op_alloc_, rowkey))) {
            LOG_WARN("failed to add rowkey", K(ret), K(i));
          } else if (OB_FAIL(pre_fileter_rowkeys_.push_back(rowkey))) {
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

int ObDASIvfScanIter::process_ivf_scan_brute()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(saved_rowkeys_.assign(pre_fileter_rowkeys_))) {
    LOG_WARN("failed to assign saved_rowkeys", K(pre_fileter_rowkeys_));
  }
  return ret;
}

int ObDASIvfScanIter::process_ivf_scan_post()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 8> nearest_cids;
  nearest_cids.set_attr(ObMemAttr(MTL_ID(), "VecIdxNearCid"));
  if (OB_FAIL(get_nearest_probe_center_ids(nearest_cids))) {
    // 1. Scan the centroid table, range: [min] ~ [max]; sort by l2_distance(c_vec, search_vec_); limit nprobes;
    // return the cid column.
    LOG_WARN("failed to get nearest probe center ids", K(ret));
  } else if (OB_FAIL(get_nearest_limit_rowkeys_in_cids<float>(nearest_cids,
                                                              reinterpret_cast<float *>(real_search_vec_.ptr())))) {
    // 2. get cidx in (top-nprobes 个 cid)
    //    scan cid_vector table, range: [cidx, min] ~ [cidx, max]; sort by l2_distance(vec, search_vec_); limit
    //    (limit+offset); return rowkey column
    LOG_WARN("failed to get nearest limit rowkeys in cids", K(nearest_cids));
  }
  return ret;
}

int ObDASIvfScanIter::process_ivf_scan(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  if (vec_aux_ctdef_->is_post_filter()) {
    if (OB_FAIL(process_ivf_scan_post())) {
      LOG_WARN("failed to process ivf_scan post filter", K(ret), K_(pre_fileter_rowkeys), K_(saved_rowkeys));
    }
  } else if (OB_FAIL(process_ivf_scan_pre(vec_op_alloc_, is_vectorized))) {
    LOG_WARN("failed to process ivf_scan pre", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(gen_rowkeys_itr())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to gen rowkeys itr", K(saved_rowkeys_));
    }
  }

  return ret;
}

int ObDASIvfScanIter::filter_rowkey_by_cid(const ObIArray<ObString> &nearest_cids,
                                           bool is_vectorized,
                                           int64_t batch_row_count,
                                           bool &index_end)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *rowkey_cid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  ObExpr *cid_expr = rowkey_cid_ctdef->result_output_[0];

  int push_count = 0;
  if (!is_vectorized) {
    for (int i = 0; OB_SUCC(ret) && i < batch_row_count && !index_end; ++i) {
      ObString cid;
      if (OB_FAIL(get_cid_from_rowkey_cid_table(cid))) {
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
        index_end = true;
      } else if (!check_cid_exist(nearest_cids, cid)) {
        push_count++;
      } else if (OB_FAIL(saved_rowkeys_.push_back(pre_fileter_rowkeys_[push_count++]))) {
        LOG_WARN("failed to add rowkey", K(ret));
      }
    }
  } else {
    rowkey_cid_iter_->clear_evaluated_flag();
    int64_t scan_row_cnt = 0;
    if (OB_FAIL(rowkey_cid_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
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
      ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);

      for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
        ObString cid = cid_datum[i].get_string();
        if (!check_cid_exist(nearest_cids, cid)) {
          push_count++;
        } else if (OB_FAIL(saved_rowkeys_.push_back(pre_fileter_rowkeys_[push_count++]))) {
          LOG_WARN("failed to add rowkey", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDASIvfScanIter::filter_pre_rowkey_batch(const ObIArray<ObString> &nearest_cids,
                                              bool is_vectorized,
                                              int64_t batch_row_count)
{
  int ret = OB_SUCCESS;

  int64_t filted_rowkeys_count = pre_fileter_rowkeys_.count();
  const ObDASScanCtDef *rowkey_cid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  ObExpr *cid_expr = rowkey_cid_ctdef->result_output_[0];

  int count = 0;
  bool index_end = false;
  while (OB_SUCC(ret) && count < filted_rowkeys_count && !index_end) {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_row_count && count < filted_rowkeys_count; ++i) {
      if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(
              *pre_fileter_rowkeys_[count++], rowkey_cid_scan_param_, rowkey_cid_ctdef->ref_table_id_))) {
        LOG_WARN("failed to set lookup key", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_rowkey_cid_table_scan())) {
      LOG_WARN("do rowkey cid table scan failed", K(ret));
    } else if (OB_FAIL(filter_rowkey_by_cid(nearest_cids, is_vectorized, batch_row_count, index_end))) {
      LOG_WARN("filter rowkey batch failed", K(ret), K(nearest_cids), K(is_vectorized), K(batch_row_count));
    } else {
      int tmp_ret = ret;
      if (OB_FAIL(ObDasVecScanUtils::reuse_iter(
              ls_id_, rowkey_cid_iter_, rowkey_cid_scan_param_, rowkey_cid_tablet_id_))) {
        LOG_WARN("failed to reuse rowkey cid iter.", K(ret));
      } else {
        ret = tmp_ret;
      }
    }
  }

  return ret;
}

int ObDASIvfScanIter::process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;

  int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  ObSEArray<ObString, 8> nearest_cids;
  nearest_cids.set_attr(ObMemAttr(MTL_ID(), "VecIdxNearCid"));
  if (OB_FAIL(get_nearest_probe_center_ids(nearest_cids))) {
    LOG_WARN("failed to get nearest probe center ids", K(ret));
  } else if (nearest_cids.count() == 1 && nearest_cids.at(0).empty()) {
    // create an index on the table, No 1 table is empty
    if (OB_FAIL(gen_rowkeys_itr_brute(inv_idx_scan_iter_))) {
      LOG_WARN("failed to gen rowkeys itr brute pre", K(ret));
    }
  } else {
    uint64_t rowkey_count = 0;
    if (OB_FAIL(get_rowkey_pre_filter(is_vectorized, rowkey_count))) {
      LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
    } else if (rowkey_count < MAX_BRUTE_FORCE_SIZE) {
      // brute_force: directly return rowkey
      if (OB_FAIL(process_ivf_scan_brute())) {
        LOG_WARN("failed to process ivf_scan brute filter", K(ret), K_(pre_fileter_rowkeys), K_(saved_rowkeys));
      }
    } else if (OB_FAIL(filter_pre_rowkey_batch(nearest_cids, is_vectorized, batch_row_count))) {
      LOG_WARN("failed to filter pre rowkey batch", K(ret));
    } else {
      pre_fileter_rowkeys_.reset();
    }

    bool index_end = false;
    while (OB_SUCC(ret) && !index_end) {
      if (OB_FAIL(get_pre_filter_rowkey_batch(allocator, is_vectorized, batch_row_count, index_end))) {
        // Take a batch of prefilter rowkeys and put them in pre_fileter_rowkeys_
        LOG_WARN("failed to get rowkey batch", K(ret), K(is_vectorized));
      } else if (OB_FAIL(filter_pre_rowkey_batch(nearest_cids, is_vectorized, batch_row_count))) {
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

bool ObDASIvfScanIter::check_cid_exist(const ObIArray<ObString> &dst_cids, const ObString &src_cid)
{
  int ret = OB_SUCCESS;
  bool src_cid_exist = false;
  ObCenterId src_centor_id;
  if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(src_centor_id, src_cid))) {
    LOG_WARN("failed to get center id from string", K(src_cid));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dst_cids.count() && !src_cid_exist; i++) {
      ObCenterId dst_centor_id;
      if (dst_cids.at(i).empty()) {
        // fit create an index with the table
        src_cid_exist = true;
      } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(dst_centor_id, dst_cids.at(i)))) {
        LOG_WARN("failed to get center id from string", K(dst_cids), K(i));
      } else if (src_centor_id == dst_centor_id) {
        src_cid_exist = true;
      }
    }
  }

  return src_cid_exist;
}

int ObDASIvfScanIter::get_rowkey(ObIAllocator &allocator, ObRowkey *&rowkey)
{
  int ret = OB_SUCCESS;

  int64_t rowkey_cnt = 0;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;

  const ObDASScanCtDef *ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(),
                                                                      ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  ObDASScanRtDef *rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx());

  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(ctdef), KP(rtdef));
  } else if (OB_FALSE_IT(rowkey_cnt = ctdef->rowkey_exprs_.count())) {
  } else if (OB_UNLIKELY(rowkey_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret), K(rowkey_cnt));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(rowkey_cnt));
  } else if (OB_FALSE_IT(obj_ptr = new (buf) ObObj[rowkey_cnt])) {
  } else if (OB_ISNULL(rowkey = static_cast<ObRowkey *>(vec_op_alloc_.alloc(sizeof(ObRowkey))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
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

    LOG_INFO("ObDASIVFScanIter get rowkey", K(obj_ptr), K(rowkey_cnt));
    if (OB_SUCC(ret)) {
      rowkey->assign(obj_ptr, rowkey_cnt);
    }
  }

  return ret;
}

int ObDASIvfScanIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    if (OB_FAIL(process_ivf_scan(false))) {
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

int ObDASIvfScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    if (OB_FAIL(process_ivf_scan(true))) {
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
/********************************************************************************************************/

int ObDASIvfPQScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam index_param;
  if (OB_FAIL(ObDASIvfScanIter::inner_init(param))) {
    LOG_WARN("fail to do inner init ", K(ret), K(param));
  } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(vec_aux_ctdef_->vec_index_param_, ObVectorIndexType::VIT_IVF_INDEX, index_param))) {
    LOG_WARN("fail to parse params from string", K(ret), K(vec_aux_ctdef_->vec_index_param_));
  } else {
    ObDASIvfScanIterParam &ivf_scan_param = static_cast<ObDASIvfScanIterParam &>(param);
    pq_centroid_iter_ = ivf_scan_param.pq_centroid_iter_;
    m_ = index_param.m_;
    if (dim_ % m_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dim should be able to divide m exactly", K(ret), K(dim_), K(m_));
    } else {
      ObSqlCollectionInfo arr_binary_info(persist_alloc_);
      ObCollectionArrayType *arr_type = nullptr;
      arr_binary_info.name_def_ = ObVecIndexBuilderUtil::IVF_PQ_CENTER_IDS_COL_TYPE_NAME;
      arr_binary_info.name_len_ = strlen(ObVecIndexBuilderUtil::IVF_PQ_CENTER_IDS_COL_TYPE_NAME);
      if (OB_FAIL(arr_binary_info.parse_type_info())) {
        LOG_WARN("fail to parse type info", K(ret));
      } else if (OB_ISNULL(pq_ids_type_ = static_cast<ObCollectionArrayType *>(arr_binary_info.collection_meta_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("source array collection array type is null", K(ret), K(arr_binary_info));
      }
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(pq_centroid_iter_) && OB_FAIL(pq_centroid_iter_->release())) {
    LOG_WARN("failed to release pq_centroid_iter_", K(ret));
  } else if (OB_NOT_NULL(pq_ids_type_)) {
    pq_ids_type_->~ObCollectionArrayType();
    pq_ids_type_ = nullptr;
  }
  persist_alloc_.reset();
  pq_centroid_iter_ = nullptr;
  ObDasVecScanUtils::release_scan_param(pq_centroid_scan_param_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDASIvfScanIter::inner_release())) {
    LOG_WARN("fail to do inner release", K(ret));
  }
  return ret;
}

int ObDASIvfPQScanIter::get_pq_cids_from_datum(
    ObIAllocator &allocator,
    const ObString& pq_cids_str,
    ObArrayBinary *&pq_cids)
{
  int ret = OB_SUCCESS;
  ObIArrayType *src_arr = nullptr;
  ObString data_str = pq_cids_str;
  if (OB_ISNULL(pq_ids_type_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("pq_ids_type_ should not be null after inner init", K(ret));
  } else if (OB_FAIL(ObArrayTypeObjFactory::construct(allocator, *pq_ids_type_, src_arr, true/*read_only*/))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator,
                                                              ObLongTextType,
                                                              CS_TYPE_BINARY,
                                                              true/*has_lob_header*/,
                                                              data_str))) {
    LOG_WARN("fail to get real data.", K(ret), K(data_str));
  } else if (OB_FAIL(src_arr->init(data_str))) {
    LOG_WARN("failed to init array", K(ret));
  } else if (OB_ISNULL(pq_cids = reinterpret_cast<ObArrayBinary *>(src_arr))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("type is not ObArrayBinary", K(ret));
  } else if (pq_cids->size() != m_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("size of pq cid array should be equal to pq m param", K(ret), K(m_), K(pq_cids->size()));
  }
  return ret;
}

// for pq, con_key is array(vector(float))
int ObDASIvfPQScanIter::parse_cid_vec_datum(
  ObIAllocator &allocator,
  int64_t cid_vec_column_count,
  const ObDASScanCtDef *cid_vec_ctdef,
  const int64_t rowkey_cnt,
  ObRowkey *&main_rowkey,
  ObArrayBinary *&com_key)
{
  int ret = OB_SUCCESS;
  ObExpr *pq_ids_expr = cid_vec_ctdef->result_output_[PQ_IDS_IDX];

  if (OB_FAIL(get_main_rowkey_from_cid_vec_datum(cid_vec_ctdef, rowkey_cnt, main_rowkey))) {
    LOG_WARN("failed to get main rowkey from cid vec datum", K(ret));
  } else if (pq_ids_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).is_null()) {
    // do nothing
  } else if (OB_FAIL(get_pq_cids_from_datum(allocator, pq_ids_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).get_string(), com_key))) {
    LOG_WARN("fail to get pq cids from datum", K(ret));
  }
  return ret;
}

int ObDASIvfPQScanIter::get_pq_cid_vec_by_pq_cid(const ObString &pq_cid, float *&pq_cid_vec)
{
  int ret = OB_SUCCESS;
  ObRowkey pq_cid_rowkey;
  const ObDASScanCtDef *pq_cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_pq_id_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN);
  ObDASScanRtDef *pq_cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_pq_id_tbl_idx());
  if (OB_ISNULL(pq_cid_vec_ctdef) || OB_ISNULL(pq_cid_vec_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(pq_cid_vec_ctdef), KP(pq_cid_vec_rtdef));
  } else if (OB_FAIL(build_cid_vec_query_rowkey(pq_cid, true/*is_min*/, CENTROID_PRI_KEY_CNT, pq_cid_rowkey))) {
    LOG_WARN("failed to build cid vec query rowkey", K(ret));
  } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(pq_cid_rowkey, pq_centroid_scan_param_, pq_cid_vec_ctdef->ref_table_id_))) {
    LOG_WARN("failed to append scan range", K(ret));
  } else if (OB_FAIL(do_aux_table_scan(pq_centroid_first_scan_,
                                        pq_centroid_scan_param_,
                                        pq_cid_vec_ctdef,
                                        pq_cid_vec_rtdef,
                                        pq_centroid_iter_,
                                        pq_centroid_tablet_id_))) {
    LOG_WARN("fail to rescan cid vec table scan iterator.", K(ret));
  } else {
    storage::ObTableScanIterator *cid_vec_scan_iter =
        static_cast<storage::ObTableScanIterator *>(pq_centroid_iter_->get_output_result_iter());
    if (OB_ISNULL(cid_vec_scan_iter)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null scan iter", K(ret));
    } else {
      int row_cnt = 0;
      while (OB_SUCC(ret)) {
        ObExpr *pq_center_vec_expr = pq_cid_vec_ctdef->result_output_[PQ_CENTROID_VEC_IDX];
        ObRowkey *main_rowkey;
        ObString vec;
        // cid_vec_scan_iter output: [IVF_CID_VEC_CID_COL IVF_CID_VEC_VECTOR_COL ROWKEY]
        if (OB_FAIL(cid_vec_scan_iter->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to scan vid rowkey iter", K(ret));
          }
        } else if (FALSE_IT(++row_cnt)) {
        } else if (OB_FALSE_IT(vec = pq_center_vec_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).get_string())) {
        } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                        &vec_op_alloc_,
                        ObLongTextType,
                        CS_TYPE_BINARY,
                        pq_cid_vec_ctdef->result_output_.at(PQ_CENTROID_VEC_IDX)->obj_meta_.has_lob_header(),
                        vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (OB_ISNULL(vec.ptr())) {
          // ignoring null vector.
        } else {
          pq_cid_vec = reinterpret_cast<float *>(vec.ptr());
        }
      }

      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        if (OB_FAIL(pq_centroid_iter_->reuse())) {
          LOG_WARN("fail to reuse scan iterator.", K(ret));
        } else if (row_cnt != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected only 1 row", K(ret), K(row_cnt), K(pq_cid));
        }
      }
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::calc_distance_between_pq_ids(
    const ObArrayBinary &pq_center_ids,
    const ObIArray<float *> &splited_residual,
    double &distance)
{
  int ret = OB_SUCCESS;
  distance = 0.0;
  for (int j = 0; OB_SUCC(ret) && j < m_; ++j) {
    // 3.2.1 pq_center_ids[j] is put into ivf_pq_centroid table to find pq_center_vecs[j]
    float *pq_cid_vec = nullptr;
    if (OB_FAIL(get_pq_cid_vec_by_pq_cid(pq_center_ids[j], pq_cid_vec))) {
      LOG_WARN("fail to get pq cid vec by pq cid", K(ret));
    } else {
      // 3.3.2 Calculate the distance between pq_center_vecs[j] and r(x)[j].
      //       The sum of j = 0 ~ m is the distance from x to rowkey
      double cur_distance = DBL_MAX;
      if (OB_FAIL(ObVectorL2Distance::l2_distance_func(splited_residual.at(j), pq_cid_vec, dim_ / m_, cur_distance))) {
        LOG_WARN("failed to calc l2 distance", K(ret));
      } else {
        distance += cur_distance;
      }
    }
  } // end for
  return ret;
}

int ObDASIvfPQScanIter::calc_nearest_limit_rowkeys_in_cids(
    const ObIArray<IvfCidVecPair> &nearest_centers,
    float *search_vec)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator(ObMemAttr(MTL_ID(), "DasIvfPQ"));
  int64_t sub_dim = dim_ / m_;
  IvfRowkeyHeap nearest_rowkey_heap(
      vec_op_alloc_, search_vec/*unused*/, sub_dim,
      get_nprobe(limit_param_, PQ_ID_ENLARGEMENT_FACTOR));
  const ObDASScanCtDef *cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN);
  ObDASScanRtDef *cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx());
  int64_t cid_vec_column_count = 0;
  int64_t cid_vec_pri_key_cnt = 0;
  int64_t rowkey_cnt = 0;
  ObArray<float *> splited_residual;
  if (OB_FAIL(splited_residual.reserve(m_))) {
    LOG_WARN("fail to init splited residual array", K(ret), K(m_));
  } else if (OB_FAIL(prepare_cid_range(cid_vec_ctdef, cid_vec_column_count, cid_vec_pri_key_cnt, rowkey_cnt))) {
    LOG_WARN("fail to prepare cid range", K(ret));
  }
  // 1. for every (cid, cid_vec),
  IvfCidVecPair cur_pair;
  for (int i = 0; OB_SUCC(ret) && i < nearest_centers.count(); ++i) {
    const ObString &cur_cid = nearest_centers.at(i).first;
    float *cur_cid_vec = nearest_centers.at(i).second;
    // 1.1 Calculate the residual r(x) = x - cid_vec
    //     split r(x) into m parts, the jth part is called r(x)[j]
    float *residual = nullptr;
    if (OB_FAIL(ObVectorIndexUtil::calc_residual_vector(tmp_allocator, dim_, search_vec, cur_cid_vec, residual))) {
      LOG_WARN("fail to calc residual vector", K(ret), K(dim_));
    } else if (OB_FAIL(ObVectorIndexUtil::split_vector(tmp_allocator, m_, dim_, residual, splited_residual))) {
      LOG_WARN("fail to split vector", K(ret));
    } else {
      // 1.2 cid put the query in the ivf_pq_code table to find (rowkey, pq_center_ids)
      storage::ObTableScanIterator *cid_vec_scan_iter = nullptr;
      if (OB_FAIL(scan_cid_range(cur_cid, cid_vec_pri_key_cnt, cid_vec_ctdef, cid_vec_rtdef, cid_vec_scan_iter))) {
        LOG_WARN("fail to scan cid range", K(ret), K(cur_cid), K(cid_vec_pri_key_cnt));
      }
      while (OB_SUCC(ret)) {
        ObRowkey *main_rowkey;
        ObString vec_arr_str;
        ObArrayBinary *pq_center_ids = NULL;
        double distance = 0.0;
        // cid_vec_scan_iter output: [IVF_CID_VEC_CID_COL IVF_CID_VEC_VECTOR_COL ROWKEY]
        if (OB_FAIL(cid_vec_scan_iter->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to scan vid rowkey iter", K(ret));
          }
        } else if (OB_FAIL(parse_cid_vec_datum(
            tmp_allocator,
            cid_vec_column_count,
            cid_vec_ctdef,
            rowkey_cnt,
            main_rowkey,
            pq_center_ids))) {
          LOG_WARN("fail to parse cid vec datum", K(ret), K(cid_vec_column_count), K(rowkey_cnt));
        } else if (OB_ISNULL(pq_center_ids)) {
          // ignore null arr
        } else if (OB_FAIL(calc_distance_between_pq_ids(*pq_center_ids, splited_residual, distance))) {
          LOG_WARN("fail to calc distance between pq ids", K(ret));
        } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance))) {
          LOG_WARN("failed to push center.", K(ret));
        }
      } // end while

      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        if (OB_FAIL(cid_vec_iter_->reuse())) {
          LOG_WARN("fail to reuse scan iterator.", K(ret));
        } else {
          splited_residual.reuse();
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

int ObDASIvfPQScanIter::get_nearest_probe_centers(ObIArray<IvfCidVecPair> &nearest_centers)
{
  int ret = OB_SUCCESS;
  share::ObVectorCentorClusterHelper<float, ObString> nearest_cid_heap(
      vec_op_alloc_, reinterpret_cast<const float *>(real_search_vec_.ptr()), dim_, nprobes_);
  if (OB_FAIL(nearest_centers.reserve(nprobes_))) {
    LOG_WARN("failed to reserve nearest_centers", K(ret));
  } else if (OB_FAIL(generate_nearear_cid_heap(nearest_cid_heap, true/*save_center_vec*/))) {
    LOG_WARN("failed to generate nearest cid heap", K(ret), K(nprobes_), K(dim_), K(real_search_vec_));
  }
  if (OB_SUCC(ret) || ret == OB_ITER_END) {
    if (OB_FAIL(nearest_cid_heap.get_nearest_probe_centers(nearest_centers))) {
      LOG_WARN("failed to get top n", K(ret));
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::get_rowkey_brute_post()
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *cid_vec_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN);
  ObDASScanRtDef *cid_vec_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_cid_vec_tbl_idx());
  int64_t cid_vec_column_count = 0;
  int64_t cid_vec_pri_key_cnt = 0;
  int64_t rowkey_cnt = 0;
  if (OB_FAIL(prepare_cid_range(cid_vec_ctdef, cid_vec_column_count, cid_vec_pri_key_cnt, rowkey_cnt))) {
    LOG_WARN("fail to prepare cid range", K(ret));
  } else {
    storage::ObTableScanIterator *cid_vec_scan_iter = nullptr;
    // empty cid means whole range
    ObString empty_cid;
    if (OB_FAIL(scan_cid_range(empty_cid, cid_vec_pri_key_cnt, cid_vec_ctdef, cid_vec_rtdef, cid_vec_scan_iter))) {
      LOG_WARN("fail to scan cid range", K(ret), K(empty_cid), K(cid_vec_pri_key_cnt));
    } else if (OB_FAIL(gen_rowkeys_itr_brute(cid_vec_iter_))) {
      LOG_WARN("fail to gen rowkeys iterator", K(ret));
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::process_ivf_scan_post()
{
  int ret = OB_SUCCESS;
  ObSEArray<IvfCidVecPair, 8> nearest_centers;
  nearest_centers.set_attr(ObMemAttr(MTL_ID(), "VecIdxNearCid"));
  if (OB_FAIL(get_nearest_probe_centers(nearest_centers))) {
    // 1. Scan the ivf_centroid table, calculate the distance between vec_x and cid_vec,
    //    and get the nearest cluster center (cid 1, cid_vec 1)... (cid n, cid_vec n)
    LOG_WARN("failed to get nearest probe center ids", K(ret));
  } else if (nearest_centers.empty()) {
    if (OB_FAIL(get_rowkey_brute_post())) {
      LOG_WARN("failed to get limit rowkey brute", K(ret));
    }
  } else if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(
      nearest_centers,
      reinterpret_cast<float *>(real_search_vec_.ptr())))) {
    // 2. search nearest rowkeys
    LOG_WARN("fail to calc nearest limit rowkeys in cids", K(ret), K(dim_));

  }

  return ret;
}

int ObDASIvfPQScanIter::get_cid_from_pq_rowkey_cid_table(ObIAllocator &allocator, ObString &cid, ObArrayBinary *&pq_cids)
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
  } else if (OB_FAIL(get_pq_cids_from_datum(allocator, pq_cids_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_).get_string(), pq_cids))) {
    LOG_WARN("fail to get pq cids from datum", K(ret));
  } else {
    ObDatum &cid_datum = cid_expr->locate_expr_datum(*vec_aux_rtdef_->eval_ctx_);
    cid = cid_datum.get_string();
  }
  return ret;
}

// if cid not exist, center_vec is nullptr
int ObDASIvfPQScanIter::check_cid_exist(
    const ObIArray<IvfCidVecPair> &dst_cids,
    const ObString &src_cid,
    float *&center_vec,
    bool &src_cid_exist)
{
  int ret = OB_SUCCESS;
  src_cid_exist = false;
  ObCenterId src_centor_id;
  center_vec = nullptr;
  if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(src_centor_id, src_cid))) {
    LOG_WARN("failed to get center id from string", K(src_cid));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dst_cids.count() && !src_cid_exist; i++) {
      ObCenterId dst_centor_id;
      if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(dst_centor_id, dst_cids.at(i).first))) {
        LOG_WARN("failed to get center id from string", K(dst_cids), K(i));
      } else if (src_centor_id == dst_centor_id) {
        src_cid_exist = true;
        if (OB_ISNULL(center_vec = dst_cids.at(i).second)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("invalid null center_vec", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDASIvfPQScanIter::calc_adc_distance(
    const ObString &cid,
    const ObArrayBinary &pq_center_ids,
    const ObIArray<IvfCidVecPair> &nearest_cids,
    IvfRowkeyHeap &rowkey_heap,
    int &push_count)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator(ObMemAttr(MTL_ID(), "DasIvfPQ"));
  float *cur_cid_vec = nullptr;
  bool is_cid_exist = false;
  float *residual = nullptr;
  ObArray<float *> splited_residual;
  double distance = 0.0;
  if (OB_FAIL(splited_residual.reserve(m_))) {
    LOG_WARN("fail to init splited residual array", K(ret), K(m_));
  } else if (OB_FAIL(check_cid_exist(nearest_cids, cid, cur_cid_vec, is_cid_exist))) {
    // 2. Compare the cid and output the equivalent (rowkey, cid, cid_vec, pq_center_ids).
    LOG_WARN("fail to check cid exist", K(ret), K(cid));
  } else if (!is_cid_exist) {
    push_count++;
  }
  // 3. For each (rowkey, cid, cid_vec, pq_center_ids) :
  // 3.1 Calculate the residual r(x) = x - cid_vec
  //     split r(x) into m parts, the jth part is called r(x)[j]
  else if (OB_FAIL(ObVectorIndexUtil::calc_residual_vector(
      tmp_allocator,
      dim_,
      reinterpret_cast<const float *>(real_search_vec_.ptr()),
      cur_cid_vec,
      residual))) {
    LOG_WARN("fail to calc residual vector", K(ret), K(dim_));
  } else if (OB_FAIL(ObVectorIndexUtil::split_vector(tmp_allocator, m_, dim_, residual, splited_residual))) {
    LOG_WARN("fail to split vector", K(ret));
  } else if (OB_FAIL(calc_distance_between_pq_ids(pq_center_ids, splited_residual, distance))) {
    LOG_WARN("fail to calc distance between pq ids", K(ret));
  } else if (OB_FAIL(rowkey_heap.push_center(pre_fileter_rowkeys_[push_count++], distance))) {
    LOG_WARN("failed to push center.", K(ret));
  }
  return ret;
}

int ObDASIvfPQScanIter::filter_rowkey_by_cid(const ObIArray<IvfCidVecPair> &nearest_cids,
                                            bool is_vectorized,
                                            int64_t batch_row_count,
                                            IvfRowkeyHeap &rowkey_heap,
                                            bool &index_end)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *rowkey_cid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  int push_count = 0;
  ObArenaAllocator tmp_allocator(ObMemAttr(MTL_ID(), "DasIvfPQ"));
  if (!is_vectorized) {
    for (int i = 0; OB_SUCC(ret) && i < batch_row_count && !index_end; ++i) {
      ObString cid;
      ObArrayBinary *pq_center_ids = nullptr;
      // 1. ivf_pq_rowkey_cid table: Querying the (cid, pq_center_ids) corresponding to the rowkey in the primary table
      if (OB_FAIL(get_cid_from_pq_rowkey_cid_table(tmp_allocator, cid, pq_center_ids))) {
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
        index_end = true;
      } else if (OB_FAIL(calc_adc_distance(cid, *pq_center_ids, nearest_cids, rowkey_heap, push_count))) {
        LOG_WARN("fail to calc adc distance", K(ret), K(cid), K(i));
      }
    } // end for
  } else {
    rowkey_cid_iter_->clear_evaluated_flag();
    int64_t scan_row_cnt = 0;
    if (OB_FAIL(rowkey_cid_iter_->get_next_rows(scan_row_cnt, batch_row_count))) {
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
      ObExpr *cid_expr = rowkey_cid_ctdef->result_output_[CIDS_IDX];
      ObExpr *pq_cids_expr = rowkey_cid_ctdef->result_output_[PQ_IDS_IDX];
      ObDatum *cid_datum = cid_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);
      ObDatum *pq_cid_datum = pq_cids_expr->locate_batch_datums(*vec_aux_rtdef_->eval_ctx_);

      for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
        ObString cid = cid_datum[i].get_string();
        ObArrayBinary *pq_center_ids = nullptr;
        if (OB_FAIL(get_pq_cids_from_datum(tmp_allocator, pq_cid_datum[i].get_string(), pq_center_ids))) {
          LOG_WARN("fail to get pq cids from datum", K(ret));
        } else if (OB_FAIL(calc_adc_distance(cid, *pq_center_ids, nearest_cids, rowkey_heap, push_count))) {
          LOG_WARN("fail to calc adc distance", K(ret), K(cid), K(i));
        }
      }
    }
  }

  return ret;
}

int ObDASIvfPQScanIter::filter_pre_rowkey_batch(const ObIArray<IvfCidVecPair> &nearest_cids,
                                                bool is_vectorized,
                                                int64_t batch_row_count,
                                                IvfRowkeyHeap &rowkey_heap)
{
  int ret = OB_SUCCESS;

  int64_t filted_rowkeys_count = pre_fileter_rowkeys_.count();
  const ObDASScanCtDef *rowkey_cid_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_rowkey_cid_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  ObExpr *cid_expr = rowkey_cid_ctdef->result_output_[0];

  int count = 0;
  bool index_end = false;
  while (OB_SUCC(ret) && count < filted_rowkeys_count && !index_end) {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_row_count && count < filted_rowkeys_count; ++i) {
      if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(
              *pre_fileter_rowkeys_[count++], rowkey_cid_scan_param_, rowkey_cid_ctdef->ref_table_id_))) {
        LOG_WARN("failed to set lookup key", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_rowkey_cid_table_scan())) {
      LOG_WARN("do rowkey cid table scan failed", K(ret));
    } else if (OB_FAIL(filter_rowkey_by_cid(nearest_cids, is_vectorized, batch_row_count, rowkey_heap, index_end))) {
      LOG_WARN("filter rowkey batch failed", K(ret), K(nearest_cids), K(is_vectorized), K(batch_row_count));
    } else {
      int tmp_ret = ret;
      if (OB_FAIL(ObDasVecScanUtils::reuse_iter(
              ls_id_, rowkey_cid_iter_, rowkey_cid_scan_param_, rowkey_cid_tablet_id_))) {
        LOG_WARN("failed to reuse rowkey cid iter.", K(ret));
      } else {
        ret = tmp_ret;
      }
    }
  }

  return ret;
}

int ObDASIvfPQScanIter::process_ivf_scan_pre(ObIAllocator &allocator, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  ObSEArray<IvfCidVecPair, 8> nearest_centers;
  nearest_centers.set_attr(ObMemAttr(MTL_ID(), "VecIdxNearCid"));
  IvfRowkeyHeap nearest_rowkey_heap(
      vec_op_alloc_, reinterpret_cast<const float *>(real_search_vec_.ptr())/*unused*/,
      dim_ / m_, get_nprobe(limit_param_, PQ_ID_ENLARGEMENT_FACTOR));
  if (OB_FAIL(get_nearest_probe_centers(nearest_centers))) {
    // 1. Scan the ivf_centroid table, calculate the distance between vec_x and cid_vec,
    //    and get the nearest cluster center (cid 1, cid_vec 1)... (cid n, cid_vec n)
    LOG_WARN("failed to get nearest probe center ids", K(ret));
  } else if (nearest_centers.count() == 0) {
    if (OB_FAIL(gen_rowkeys_itr_brute(inv_idx_scan_iter_))) {
      LOG_WARN("failed to get limit rowkey brute pre", K(ret));
    }
  } else {
    uint64_t rowkey_count = 0;
    if (OB_FAIL(get_rowkey_pre_filter(is_vectorized, rowkey_count))) {
      LOG_WARN("failed to get rowkey pre filter", K(ret), K(is_vectorized));
    } else if (rowkey_count < MAX_BRUTE_FORCE_SIZE) {
      // brute_force: directly return rowkey
      if (OB_FAIL(process_ivf_scan_brute())) {
        LOG_WARN("failed to process adaptorstate brute filter", K(ret), K_(pre_fileter_rowkeys), K_(saved_rowkeys));
      }
    } else if (OB_FAIL(
                   filter_pre_rowkey_batch(nearest_centers, is_vectorized, batch_row_count, nearest_rowkey_heap))) {
      LOG_WARN("failed to filter pre rowkey batch", K(ret));
    } else {
      pre_fileter_rowkeys_.reset();
    }

    bool index_end = false;
    while (OB_SUCC(ret) && !index_end) {
      if (OB_FAIL(get_pre_filter_rowkey_batch(allocator, is_vectorized, batch_row_count, index_end))) {
        LOG_WARN("failed to get rowkey batch", K(ret), K(is_vectorized));
      } else if (OB_FAIL(
                     filter_pre_rowkey_batch(nearest_centers, is_vectorized, batch_row_count, nearest_rowkey_heap))) {
        LOG_WARN("failed to filter pre rowkey batch", K(ret));
      } else {
        pre_fileter_rowkeys_.reset();
      }
    }

    if (OB_FAIL(ret) && OB_ITER_END != ret) {
      LOG_WARN("get next row failed.", K(ret));
    } else {
      ret = OB_SUCCESS; // overwrite OB_ITER_END
      // output from nearest_rowkey_heap
      if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_center_ids(saved_rowkeys_))) {
        LOG_WARN("failed to get top n", K(ret));
      }
    }
  }
  return ret;
}

int ObDASIvfPQScanIter::process_ivf_scan(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  if (vec_aux_ctdef_->is_post_filter()) {
    if (OB_FAIL(process_ivf_scan_post())) {
      LOG_WARN("failed to process adaptorstate post filter", K(ret), K_(pre_fileter_rowkeys), K_(saved_rowkeys));
    }
  } else if (OB_FAIL(process_ivf_scan_pre(vec_op_alloc_, is_vectorized))) {
    LOG_WARN("failed to process adaptor state hnsw", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(gen_rowkeys_itr())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to gen adapter rowkeys itr", K(saved_rowkeys_));
    }
  }
  return ret;
}

/********************************************************************************************************/
int ObDASIvfSQ8ScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    if (OB_FAIL(process_ivf_scan_sq(true))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to process adaptor state", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get adaptor vid iter", K(ret));
  } else if (OB_FALSE_IT(saved_rowkeys_itr_->set_batch_size(capacity))) {
  } else if (OB_FAIL(get_next_saved_rowkeys(count))) {
    LOG_WARN("failed to get saved rowkeys", K(ret));
  }
  return ret;
}

int ObDASIvfSQ8ScanIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (limit_param_.limit_ + limit_param_.offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    if (OB_FAIL(process_ivf_scan_sq(false))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to process adaptor state", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(saved_rowkeys_itr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get adaptor iter", K(ret));
  } else if (OB_FAIL(get_next_saved_rowkey())) {
    LOG_WARN("failed to get saved rowkey", K(ret));
  }
  return ret;
}


int ObDASIvfSQ8ScanIter::process_ivf_scan_sq(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  if (vec_aux_ctdef_->is_post_filter()) {
    if (OB_FAIL(process_ivf_scan_post_sq())) {
      LOG_WARN("failed to process adaptorstate post filter", K(ret), K_(pre_fileter_rowkeys), K_(saved_rowkeys));
    }
  } else if (OB_FAIL(process_ivf_scan_pre(vec_op_alloc_, is_vectorized))) {
    LOG_WARN("failed to process adaptor state hnsw", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(gen_rowkeys_itr())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to gen rowkeys itr", K(saved_rowkeys_));
    }
  }
  return ret;
}

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

int ObDASIvfSQ8ScanIter::get_real_search_vec_u8(ObString &real_search_vec_u8)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *sq_meta_ctdef = vec_aux_ctdef_->get_vec_aux_tbl_ctdef(
      vec_aux_ctdef_->get_ivf_sq_meta_tbl_idx(), ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN);
  ObDASScanRtDef *sq_meta_rtdef = vec_aux_rtdef_->get_vec_aux_tbl_rtdef(vec_aux_ctdef_->get_ivf_sq_meta_tbl_idx());
  if (OB_FAIL(do_table_full_scan(sq_meta_iter_first_scan_,
                                 sq_meta_scan_param_,
                                 sq_meta_ctdef,
                                 sq_meta_rtdef,
                                 sq_meta_iter_,
                                 SQ_MEAT_PRI_KEY_CNT,
                                 sq_meta_tablet_id_))) {
    LOG_WARN("failed to do table scan sq_meta", K(ret));
  } else {
    // get min_vec max_vec step_vec in sq_meta table
    storage::ObTableScanIterator *sq_meta_scan_iter =
        static_cast<storage::ObTableScanIterator *>(sq_meta_iter_->get_output_result_iter());
    if (OB_ISNULL(sq_meta_scan_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("real sql meta iter is null", K(ret));
    } else {
      ObString min_vec;
      ObString step_vec;
      int row_index = 0;
      while (OB_SUCC(ret)) {
        ObString c_vec;
        blocksstable::ObDatumRow *datum_row = nullptr;
        if (OB_FAIL(sq_meta_scan_iter->get_next_row(datum_row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row failed.", K(ret));
          }
        } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get row invalid.", K(ret));
        } else if (datum_row->get_column_count() != SQ_MEAT_ALL_KEY_CNT) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
        } else if (row_index == ObIvfConstant::SQ8_META_MIN_IDX || row_index == ObIvfConstant::SQ8_META_STEP_IDX) {
          c_vec = datum_row->storage_datums_[1].get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  &vec_op_alloc_,
                  ObLongTextType,
                  CS_TYPE_BINARY,
                  sq_meta_ctdef->result_output_.at(1)->obj_meta_.has_lob_header(),
                  c_vec))) {
            LOG_WARN("failed to get real data.", K(ret));
          } else if (row_index == ObIvfConstant::SQ8_META_MIN_IDX) {
            min_vec = c_vec;
          } else if (row_index == ObIvfConstant::SQ8_META_STEP_IDX) {
            step_vec = c_vec;
          }
        }
        row_index++;
      }

      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        uint8_t *res_vec = nullptr;
        if (OB_ISNULL(min_vec.ptr()) || OB_ISNULL(step_vec.ptr())) {
          if (OB_ISNULL(res_vec = reinterpret_cast<uint8_t *>(vec_op_alloc_.alloc(sizeof(uint8_t) * dim_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret), K(sizeof(uint8_t) * dim_));
          } else {
            MEMSET(res_vec, 0, sizeof(uint8_t) * dim_);
          }
        } else if (OB_FAIL(
                       ObExprVecIVFSQ8DataVector::cal_u8_data_vector(vec_op_alloc_,
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
    }
  }
  return ret;
}

int ObDASIvfSQ8ScanIter::process_ivf_scan_post_sq()
{
  int ret = OB_SUCCESS;
  // post scan:
  //   1. Obtain nproc cids
  //   2. real_search_vec_ to real_search_vec_unit8
  //   3. Query ivf_cid_vector, retrieve all (rowkey, data_vec) pairs under each cid, calculate the distance between
  //   data_vec and vec_x_uint8, and select the top K.
  ObSEArray<ObString, 8> nearest_cids;
  nearest_cids.set_attr(ObMemAttr(MTL_ID(), "VecIdxNearCid"));
  ObString real_search_vec_u8;
  if (OB_FAIL(get_nearest_probe_center_ids(nearest_cids))) {
    LOG_WARN("failed to get nearest probe center ids", K(ret));
  } else if (OB_FAIL(get_real_search_vec_u8(real_search_vec_u8))) {
    // real_search_vec to real_search_vec_u8 with min_vec max_vec step_vec
    LOG_WARN("failed to get real search vec u8", K(ret));
  } else if (OB_FAIL(get_nearest_limit_rowkeys_in_cids<uint8_t>(
                 nearest_cids, reinterpret_cast<uint8_t *>(real_search_vec_u8.ptr())))) {
    LOG_WARN("failed to get nearest limit rowkeys in cids", K(nearest_cids));
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
