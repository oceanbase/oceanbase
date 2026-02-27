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
  if (MY_SPEC.is_fts_ddl_ && need_sample_) {
    if (OB_FAIL(ObTableScanOp::inner_get_next_fts_index_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next fts sample row failed", K(ret), "op", op_name());
      }
    }
  } else if (OB_FAIL(ObTableScanOp::inner_get_next_row())) {
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
