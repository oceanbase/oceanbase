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
  } else if (FALSE_IT(ObIDASSearchOp::switch_tablet_id(tablet_id_, scan_param_))) {
  } else if (OB_FAIL(tsc_service_->reuse_scan_iter(scan_param_.need_switch_param_, result_))) {
    LOG_WARN("failed to reuse scan iter", K(ret));
  } else if (OB_FAIL(prepare_scan_ranges(scalar_rtdef_))) {
    LOG_WARN("failed to prepare scan ranges", K(ret));
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

} // namespace sql
} // namespace oceanbase