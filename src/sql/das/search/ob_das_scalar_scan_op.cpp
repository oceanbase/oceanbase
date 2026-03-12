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
#include "sql/das/search/ob_das_scalar_scan_op.h"

namespace oceanbase
{
namespace sql
{

int ObDASScalarScanOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  // leaf node, no children
  return OB_SUCCESS;
}

int ObDASScalarScanOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASScalarScanOpParam &scalar_op_param = static_cast<const ObDASScalarScanOpParam &>(op_param);
  if (OB_ISNULL(scalar_op_param.get_scan_ctdef()) || OB_ISNULL(scalar_op_param.get_scan_rtdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    scalar_ctdef_ = scalar_op_param.get_scan_ctdef();
    scalar_rtdef_ = scalar_op_param.get_scan_rtdef();
  }
  return ret;
}

int ObDASScalarScanOp::do_open()
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
  } else if (OB_FAIL(tsc_service_->table_scan(scan_param_, result_))) {
    LOG_WARN("failed to do table scan", K(scan_param_));
  }
  SET_METRIC_VAL(common::ObMetricId::HS_TABLET_ID, tablet_id_.id());
  LOG_TRACE("[hybrid search] open scalar scan op", K(ret), K(tablet_id_), K(scan_param_));
  return ret;
}

int ObDASScalarScanOp::do_rescan()
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
    scan_param_.need_switch_param_ = false;
    SET_METRIC_VAL(common::ObMetricId::HS_TABLET_ID, tablet_id_.id());
  }
  return ret;
}

int ObDASScalarScanOp::do_close()
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
  return ret;
}

int ObDASScalarScanOp::prepare_scan_ranges(const ObDASScalarScanRtDef *rtdef)
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

int ObDASScalarScanOp::get_next_rows(int64_t &count, const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(profile_);
  common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
  count = 0;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("result is not initialized", K(ret));
  } else if (OB_FAIL(result_->get_next_rows(count, max_batch_size))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next rows", K(ret));
    } else if (count > 0) {
      ret = OB_SUCCESS;
    }
  }
  INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, count);
  return ret;
}

} // namespace sql
} // namespace oceanbase