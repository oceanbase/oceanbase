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

#include "sql/engine/table/ob_block_sample_scan.h"

using namespace oceanbase;
using namespace common;
using namespace storage;
using namespace sql;

ObBlockSampleScan::ObBlockSampleScan(common::ObIAllocator& allocator) : ObTableScan(allocator)
{}

int ObBlockSampleScan::prepare_scan_param(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScan::prepare_scan_param(ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("prepare scan param failed", K(ret));
    }
  } else {
    ObTableScanCtx* scan_ctx = NULL;
    if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get op ctx", K(ret));
    } else {
      scan_ctx->scan_param_.sample_info_ = sample_info_;
      scan_ctx->scan_param_.filters_before_index_back_.reset();
      scan_ctx->scan_param_.filters_.reset();
    }
  }
  return ret;
}

inline int ObBlockSampleScan::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObTableScanInput* scan_input = NULL;
  if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    if (OB_FAIL(CREATE_PHY_OP_INPUT(ObTableScanInput, ctx, get_id(), get_type(), scan_input))) {
      LOG_WARN("fail to create table scan input", K(ret), "op_id", get_id(), "op_type", get_type());
    } else {
      scan_input->set_location_idx(0);
    }
  }
  if (OB_FAIL((ret))) {
    // do nothing
  } else if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObBlockSampleScanCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret), K(get_type()));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator context is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, false))) {
    LOG_WARN("init current row failed", K(ret));
  } else if (OB_FAIL(static_cast<ObTableScanCtx*>(op_ctx)->init_table_allocator(ctx))) {
    LOG_WARN("fail to init table allocator", K(ret));
  }
  return ret;
}

int ObBlockSampleScan::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScan::inner_get_next_row(ctx, row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret), K_(type), "op", ob_phy_operator_type_str(get_type()));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObBlockSampleScan, ObTableScan), sample_info_);
