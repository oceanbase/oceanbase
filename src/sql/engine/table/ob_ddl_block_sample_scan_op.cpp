/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT

#include "sql/engine/table/ob_ddl_block_sample_scan_op.h"

using namespace oceanbase;
using namespace common;
using namespace storage;
using namespace sql;

int ObDDLBlockSampleScanOp::inner_open()
{
  int ret = OB_SUCCESS;
  set_need_sample(!MY_SPEC.get_sample_info().is_no_sample());
  if (OB_FAIL(ObTableScanOp::inner_open())) {
    LOG_WARN("fail to inner open", K(ret));
  } else if (MY_SPEC.use_dist_das()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ddl block sample scan with dist das");
  }
  return ret;
}

int ObDDLBlockSampleScanOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScanOp::inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret), "op", op_name());
    }
  }
  LOG_DEBUG("static engine block sample scan get row", K(ret));
  return ret;
}

int ObDDLBlockSampleScanOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScanOp::inner_get_next_batch(max_row_cnt))) {
    LOG_WARN("get next batch failed", K(ret), "op", op_name());
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDDLBlockSampleScanSpec, ObTableScanSpec), sample_info_);
