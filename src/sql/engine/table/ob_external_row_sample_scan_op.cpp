/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT

#include "sql/engine/table/ob_external_row_sample_scan_op.h"

using namespace oceanbase;
using namespace common;
using namespace storage;
using namespace sql;

int ObExternalRowSampleScanOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScanOp::inner_open())) {
    LOG_WARN("fail to inner open", K(ret));
  } else if (MY_SPEC.use_dist_das()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "External Table Row Sample Scan with dist DAS");
  } else {
    need_sample_ = !MY_SPEC.get_sample_info().is_no_sample();
    tsc_rtdef_.scan_rtdef_.sample_info_ = need_sample_ ? &(MY_SPEC.get_sample_info()) : nullptr;
    LOG_INFO("OptStat: ObExternalRowSampleScanOp set sample_info",
             "need_sample",
             need_sample_,
             "sample_info_ptr",
             OB_NOT_NULL(tsc_rtdef_.scan_rtdef_.sample_info_) ? "NOT_NULL" : "NULL",
             "is_row_sample",
             OB_NOT_NULL(tsc_rtdef_.scan_rtdef_.sample_info_) ? tsc_rtdef_.scan_rtdef_.sample_info_->is_row_sample() : false,
             "sample_percent",
             OB_NOT_NULL(tsc_rtdef_.scan_rtdef_.sample_info_) ? tsc_rtdef_.scan_rtdef_.sample_info_->percent_ : 0.0);
  }
  return ret;
}

int ObExternalRowSampleScanOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScanOp::inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret), "op", op_name());
    }
  }
  LOG_DEBUG("static engine external table row sample scan get row", K(ret));
  return ret;
}

int ObExternalRowSampleScanOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScanOp::inner_get_next_batch(max_row_cnt))) {
    LOG_WARN("get next batch failed", K(ret), "op", op_name());
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObExternalRowSampleScanSpec, ObTableScanSpec), sample_info_);
