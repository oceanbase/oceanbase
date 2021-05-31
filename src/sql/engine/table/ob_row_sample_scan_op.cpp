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

#include "sql/engine/table/ob_row_sample_scan_op.h"

using namespace oceanbase;
using namespace common;
using namespace storage;
using namespace sql;

int ObRowSampleScanOp::prepare_scan_param()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScanOp::prepare_scan_param())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("prepare scan param failed", K(ret));
    }
  } else {
    scan_param_.sample_info_ = MY_SPEC.get_sample_info();
    scan_param_.op_filters_ = NULL;
    scan_param_.op_filters_before_index_back_ = NULL;
  }
  return ret;
}

int ObRowSampleScanOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScanOp::inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret), "op", op_name());
    }
  }
  LOG_DEBUG("static engine row sample scan get row", K(ret));
  return ret;
}

//
OB_SERIALIZE_MEMBER((ObRowSampleScanSpec, ObTableScanSpec), sample_info_);
