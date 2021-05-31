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
#include "sql/engine/table/ob_mv_table_scan.h"
using namespace oceanbase;
using namespace common;
using namespace storage;
using namespace sql;

ObPhyOperatorType ObMVTableScanInput::get_phy_op_type() const
{
  return PHY_MV_TABLE_SCAN;
}

ObMVTableScan::ObMVTableScan(common::ObIAllocator& allocator)
    : ObTableScan(allocator), right_table_param_(allocator), right_table_location_key_(OB_INVALID_ID)
{}

inline int ObMVTableScan::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;

  ObTableScanInput* scan_input = NULL;
  if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    if (OB_FAIL(CREATE_PHY_OP_INPUT(ObMVTableScanInput, ctx, get_id(), get_type(), scan_input))) {
      LOG_WARN("fail to create table scan input", K(ret), "op_id", get_id(), "op_type", get_type());
    } else {
      scan_input->set_location_idx(0);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMVTableScanCtx, ctx, get_id(), get_type(), op_ctx))) {
      LOG_WARN("create physical operator context failed", K(ret), K(get_type()));
    } else if (OB_ISNULL(op_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator context is null");
    } else if (OB_FAIL(init_cur_row(*op_ctx, false))) {
      LOG_WARN("init current row failed", K(ret));
    }
  }
  return ret;
}

int ObMVTableScan::prepare_scan_param(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = NULL;
  ObMVTableScanCtx* scan_ctx = NULL;
  if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObMVTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(ObTableScan::prepare_scan_param(ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to do prepare_scan_param", K(ret));
    }
  } else {
    scan_ctx->right_scan_param_.trans_desc_ = &my_session->get_trans_desc();
    const ObPhyTableLocation* table_loc = NULL;
    ObTaskExecutorCtx* executor_ctx = NULL;
    if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get task executor ctx", K(ret));
    } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                   *executor_ctx, right_table_location_key_, right_table_location_key_, table_loc))) {
      LOG_WARN("fail to get mv right table location", K(ret), K(right_table_location_key_));
    } else if (OB_ISNULL(table_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table location is NULL", K(ret));
    } else {
      const share::ObPartitionReplicaLocation& part_loc = table_loc->get_partition_location_list().at(0);
      if (OB_FAIL(part_loc.get_partition_key(scan_ctx->right_scan_param_.pkey_))) {
        LOG_WARN("get partition key fail", K(ret), K(part_loc));
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObMVTableScan, ObTableScan), right_table_param_, right_table_location_key_);

void ObMVTableScan::reset()
{
  ObTableScan::reset();
  right_table_param_.reset();
  right_table_location_key_ = OB_INVALID_ID;
}

void ObMVTableScan::reuse()
{
  ObTableScan::reuse();
  right_table_param_.reset();
  right_table_location_key_ = OB_INVALID_ID;
}

int ObMVTableScan::do_table_scan(ObExecContext& ctx, bool is_rescan) const
{
  int ret = OB_SUCCESS;
  ObTableScanInput* scan_input = NULL;
  ObMVTableScanCtx* scan_ctx = NULL;
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  ObSQLSessionInfo* my_session = NULL;
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op input", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObMVTableScanCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get op ctx", K(ret));
  } else if (OB_FAIL(prepare(ctx, is_rescan))) {  // prepare scan input param
    LOG_WARN("fail to prepare scan param", K(ret));
  }
  /* step 1 */
  if (OB_SUCC(ret)) {
    scan_ctx->output_row_count_ = 0;
    scan_ctx->scan_param_.key_ranges_.reset();
    scan_ctx->scan_param_.scan_flag_.join_type_ = INNER_JOIN;
    scan_ctx->scan_param_.table_param_ = &table_param_;
    scan_ctx->right_scan_param_.table_param_ = &right_table_param_;
    scan_ctx->right_scan_param_.scan_flag_.join_type_ = INNER_JOIN;

    scan_ctx->right_scan_param_.timeout_ = scan_ctx->scan_param_.timeout_;
    scan_ctx->right_scan_param_.schema_version_ = scan_ctx->scan_param_.schema_version_;
    scan_ctx->right_scan_param_.index_id_ = right_table_param_.get_table_id();
    scan_ctx->right_scan_param_.reserved_cell_count_ = output_column_ids_.count();
    scan_ctx->right_scan_param_.column_ids_.reset();
    scan_ctx->right_scan_param_.column_ids_.assign(output_column_ids_);

    for (int64_t i = 0; OB_SUCC(ret) && i < scan_input->key_ranges_.count(); ++i) {
      if (OB_FAIL(scan_ctx->scan_param_.key_ranges_.push_back(scan_input->key_ranges_.at(i)))) {
        LOG_WARN("fail to push back key range", K(ret), K(i));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObIDataAccessService* das = NULL;
    if (OB_FAIL(get_partition_service(task_exec_ctx, das))) {
      LOG_WARN("fail to get partition service", K(ret));
    } else if (OB_FAIL(das->join_mv_scan(scan_ctx->scan_param_, scan_ctx->right_scan_param_, scan_ctx->result_))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to scan table", K(scan_ctx->scan_param_), K(ret));
      }
    } else {
      LOG_DEBUG("table_scan begin", K(scan_ctx->scan_param_), K(*scan_ctx->result_), "op_id", get_id());
    }
  }

  if (OB_SUCC(ret)) {
    ret = sim_err(my_session);
  }
  return ret;
}
