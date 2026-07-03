/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_scalar_ror_op.h"

namespace oceanbase
{
namespace sql
{

int ObDASScalarROROp::do_open()
{
  int ret = OB_SUCCESS;
  tsc_service_ = MTL(storage::ObAccessService *);
  if (OB_ISNULL(tsc_service_) || OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(scalar_ctdef_), K(scalar_rtdef_), K(tsc_service_));
  } else if (OB_FAIL(get_related_tablet_id(scalar_ctdef_, tablet_id_))) {
    LOG_WARN("failed to get related tablet id", K(ret));
  } else if (OB_FAIL(search_ctx_.init_scan_param(tablet_id_, scalar_ctdef_, scalar_rtdef_, scan_param_))) {
    LOG_WARN("failed to init scan param", K(ret), K(tablet_id_));
  } else if (FALSE_IT(scan_param_.is_get_ = false)) {
  } else if (OB_FAIL(narrow_scan_ranges_by_docid_range())) {
    LOG_WARN("failed to narrow scan ranges by docid range", K(ret));
  } else if (OB_FAIL(tsc_service_->table_scan(scan_param_, result_))) {
    LOG_WARN("failed to do table scan", K(scan_param_));
  } else if (OB_FAIL(search_ctx_.create_rowid_store(max_batch_size(), rowid_store_))) {
    LOG_WARN("failed to create rowid store", K(ret), K(max_batch_size()));
  } else if (OB_ISNULL(rowid_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store", K(ret));
  } else {
    rowid_store_iter_.init(rowid_store_);
    SET_METRIC_VAL(common::ObMetricId::HS_TABLET_ID, tablet_id_.id());
  }
  return ret;
}

int ObDASScalarROROp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_) || OB_ISNULL(tsc_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(scalar_ctdef_), K(scalar_rtdef_), K(tsc_service_));
  } else if (OB_FAIL(get_related_tablet_id(scalar_ctdef_, tablet_id_))) {
    LOG_WARN("failed to get related tablet id", K(ret));
  } else if (FALSE_IT(ObIDASSearchOp::switch_tablet_id(search_ctx_.get_ls_id(), tablet_id_, scan_param_))) {
  } else if (OB_FAIL(tsc_service_->reuse_scan_iter(scan_param_.need_switch_param_, result_))) {
    LOG_WARN("failed to reuse scan iter", K(ret));
  } else if (OB_FAIL(prepare_scan_ranges(scalar_rtdef_))) {
    LOG_WARN("failed to prepare scan ranges", K(ret));
  } else if (OB_FAIL(narrow_scan_ranges_by_docid_range())) {
    LOG_WARN("failed to narrow scan ranges by docid range", K(ret));
  } else if (OB_FAIL(tsc_service_->table_rescan(scan_param_, result_))) {
    LOG_WARN("failed to rescan table", K(ret), K(scan_param_));
  } else {
    if (OB_NOT_NULL(rowid_store_)) {
      rowid_store_->reuse();
      rowid_store_iter_.reset();
      rowid_store_iter_.init(rowid_store_);
    }
    scan_param_.need_switch_param_ = false;
    SET_METRIC_VAL(common::ObMetricId::HS_TABLET_ID, tablet_id_.id());
  }
  return ret;
}

int ObDASScalarROROp::do_close()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(result_) && OB_NOT_NULL(tsc_service_)) {
    if (OB_FAIL(tsc_service_->revert_scan_iter(result_))) {
      LOG_WARN("failed to revert scan iter", K(ret));
    }
    result_ = nullptr;
  }
  scan_param_.destroy_schema_guard();
  scan_param_.snapshot_.reset();
  scan_param_.destroy();
  if (OB_NOT_NULL(rowid_store_)) {
    rowid_store_->reset();
    rowid_store_iter_.reset();
    rowid_store_ = nullptr;
  }
  return ret;
}

int ObDASScalarROROp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  score = 0.0;

  if(OB_ISNULL(rowid_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store", K(ret));
  } else {
    ret = rowid_store_iter_.lower_bound(target);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rowid_store_iter_.get_cur_rowid(curr_id))) {
      LOG_WARN("failed to get rowid", K(ret));
    }
  } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
    LOG_WARN("falied to find lower bound in store", K(ret));
  }
  // else if (OB_FAIL(advance_skip_scan(target))) {
  //   LOG_WARN("failed to advance skip scan", K(ret));
  // }
  else {
    bool reached = false;
    int64_t storage_count = 0;
    ret = OB_SUCCESS;
    while (!reached && OB_SUCC(ret)) {
      if (OB_FAIL(result_->get_next_rows(storage_count, max_batch_size()))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next rows", K(ret));
        } else if (storage_count > 0) {
          ret = OB_SUCCESS;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(search_ctx_.lower_bound_in_frame(target, &scalar_ctdef_->rowkey_exprs_, storage_count, idx))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("falied to find lower bound", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        rowid_store_->reuse();
        if (OB_FAIL(rowid_store_->fill(idx, storage_count, scalar_ctdef_->rowkey_exprs_))) {
          LOG_WARN("failed to fill rowid store", K(ret));
        } else if (rowid_store_->count() == 0) {
          // do nothing
        } else if (FALSE_IT(rowid_store_iter_.reuse())) {
        } else if (OB_FAIL(rowid_store_iter_.get_cur_rowid(curr_id))) {
          LOG_WARN("failed to get rowid", K(ret));
        } else {
          reached = true;
        }
      }
    }
  }

  return ret;
}

int ObDASScalarROROp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (OB_ISNULL(rowid_store_) || OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store or result", K(ret));
  } else {
    rowid_store_iter_.next_idx();
  }

  if (OB_FAIL(ret)) {
  } else if (!rowid_store_iter_.is_empty()) {
    if (OB_FAIL(rowid_store_iter_.get_cur_rowid(next_id))) {
      LOG_WARN("failed to get next rowid", K(ret));
    }
  } else {
    // there is no more rowid in store, fetch next batch
    rowid_store_->reuse();
    int64_t storage_count = 0;
    if (OB_FAIL(result_->get_next_rows(storage_count, max_batch_size()))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next rows", K(ret));
      } else if (storage_count > 0) {
        ret = OB_SUCCESS;
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      if (OB_FAIL(rowid_store_->fill(0, storage_count, scalar_ctdef_->rowkey_exprs_))) {
        LOG_WARN("failed to fill rowid store", K(ret));
      } else if (rowid_store_->count() == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty rowid store", K(ret));
      } else if (FALSE_IT(rowid_store_iter_.reuse())) {
      } else if (OB_FAIL(rowid_store_iter_.get_cur_rowid(next_id))) {
        LOG_WARN("failed to get next rowid", K(ret));
      }
    }
  }

  return ret;
}

int ObDASScalarROROp::prepare_scan_ranges(const ObDASScalarScanRtDef *rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan rtdef", K(ret));
  } else if (OB_FAIL(scan_param_.key_ranges_.assign(rtdef->key_ranges_))) {
    LOG_WARN("failed to assign key ranges", K(ret));
  }
  return ret;
}

int ObDASScalarROROp::narrow_scan_ranges_by_docid_range()
{
  int ret = OB_SUCCESS;
  if (!search_ctx_.has_docid_range() || OB_ISNULL(scalar_ctdef_)) {
    // no narrowing needed
  } else {
    const common::ObObj &lo = search_ctx_.get_docid_range_lo();
    const common::ObObj &hi = search_ctx_.get_docid_range_hi();
    // Range parallel only supports single-column uint64 docid as pk.
    const int64_t rowkey_cnt = scalar_ctdef_->rowkey_exprs_.count();
    if (OB_UNLIKELY(search_ctx_.get_rowid_type() != DAS_ROWID_TYPE_UINT64)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("docid range parallel requires uint64 rowid type",
          KR(ret), K(search_ctx_.get_rowid_type()));
    } else if (OB_UNLIKELY(rowkey_cnt != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("docid range narrowing expects single-column uint64 pk",
          KR(ret), K(rowkey_cnt));
    }
    ObRangeArray &key_ranges = scan_param_.key_ranges_;

    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
      ObNewRange &range = key_ranges.at(i);
      const int64_t obj_cnt = range.start_key_.get_obj_cnt();
      if (OB_UNLIKELY(obj_cnt < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected key range obj count for docid range narrowing",
            KR(ret), K(i), K(obj_cnt), K(range));
      } else {
        // docid (single pk column) is at the tail of the composite key
        const int64_t pk_idx = obj_cnt - 1;
        ObObj *start_objs = range.start_key_.get_obj_ptr();
        ObObj *end_objs = range.end_key_.get_obj_ptr();
        if (OB_ISNULL(start_objs) || OB_ISNULL(end_objs)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null key range obj ptr for docid range narrowing",
              KR(ret), K(i), KP(start_objs), KP(end_objs), K(range));
        } else {
          // Narrow start key: tighten docid column to docid_range_lo
          if (!lo.is_min_value()) {
            ObObj &start_pk = start_objs[pk_idx];
            if (start_pk.is_min_value()
                || (!start_pk.is_max_value()
                    && start_pk.get_uint64() < lo.get_uint64())) {
              start_pk = lo;
              range.border_flag_.set_inclusive_start();
            }
          }

          // Narrow end key: tighten docid column to docid_range_hi
          if (!hi.is_max_value()) {
            ObObj &end_pk = end_objs[pk_idx];
            if (end_pk.is_max_value()
                || (!end_pk.is_min_value()
                    && end_pk.get_uint64() > hi.get_uint64())) {
              end_pk = hi;
              range.border_flag_.set_inclusive_end();
            }
          }
        }
      }
    }

    LOG_TRACE("[FUSION_TRACE] scalar ror narrow scan ranges by docid range",
        K(lo), K(hi), K(key_ranges));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase