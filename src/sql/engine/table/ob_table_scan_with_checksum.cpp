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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_table_scan_with_checksum.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_index_checksum.h"
#include "share/ob_sstable_checksum_operator.h"
#include "storage/ob_freeze_info_snapshot_mgr.h"
#include "storage/ob_partition_service.h"
#include "observer/ob_server_struct.h"
#include "share/scheduler/ob_dag_scheduler.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

OB_SERIALIZE_MEMBER((ObTableScanWithChecksumInput, ObTableScanInput), task_id_);

ObTableScanWithChecksumInput::ObTableScanWithChecksumInput() : ObTableScanInput()
{}

ObTableScanWithChecksumInput::~ObTableScanWithChecksumInput()
{}

void ObTableScanWithChecksumInput::reset()
{
  ObTableScanInput::reset();
  task_id_.reset();
}

int ObTableScanWithChecksumInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScanInput::init(ctx, task_info, op))) {
    LOG_WARN("fail to do table scan input init", K(ret));
  } else {
    task_id_ = task_info.get_task_location().get_ob_task_id();
  }
  return ret;
}

ObTableScanWithChecksum::ObTableScanWithChecksumCtx::ObTableScanWithChecksumCtx(ObExecContext& ctx)
    : ObTableScanCtx(ctx), checksum_(nullptr), col_ids_(), task_id_(0)
{}

ObTableScanWithChecksum::ObTableScanWithChecksumCtx::~ObTableScanWithChecksumCtx()
{
  destroy();
}

void ObTableScanWithChecksum::ObTableScanWithChecksumCtx::destroy()
{
  ObTableScan::ObTableScanCtx::destroy();
  if (nullptr != checksum_) {
    if (nullptr != table_allocator_) {
      table_allocator_->free(checksum_);
      checksum_ = nullptr;
    }
  }
  col_ids_.reset();
  task_id_ = 0;
}

int ObTableScanWithChecksum::ObTableScanWithChecksumCtx::get_output_col_ids(const ObTableParam& table_param)
{
  int ret = OB_SUCCESS;
  const ObIArray<int32_t>& output_projector = table_param.get_output_projector();
  ObArray<int32_t> out_cols;
  ObColumnParam* col_param = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_param.get_columns().count(); ++i) {
    if (OB_FAIL(table_param.get_columns().at(i, col_param))) {
      LOG_WARN("fail to get ith column", K(ret), K(i));
    } else if (OB_FAIL(out_cols.push_back(static_cast<int32_t>(col_param->get_column_id())))) {
      LOG_WARN("fail to push back column id", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < output_projector.count(); ++i) {
    const int64_t idx = output_projector.at(i);
    if (idx >= out_cols.count() || idx < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid output projector index", K(ret), K(idx));
    } else if (OB_FAIL(col_ids_.push_back(out_cols.at(idx)))) {
      LOG_WARN("fail to push back column id", K(ret));
    }
  }
  return ret;
}

int ObTableScanWithChecksum::ObTableScanWithChecksumCtx::allocate_checksum_memory()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_allocator_));
  } else {
    void* checksum_buf = NULL;
    ObIAllocator& allocator = *table_allocator_;
    const int64_t column_cnt = col_ids_.count();
    if (column_cnt <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(col_ids_));
    } else if (OB_ISNULL(checksum_buf = allocator.alloc(column_cnt * sizeof(int64_t)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for checksum", K(ret));
    } else {
      checksum_ = static_cast<int64_t*>(checksum_buf);
      MEMSET(checksum_, 0, column_cnt * sizeof(int64_t));
    }
  }
  return ret;
}

int ObTableScanWithChecksum::ObTableScanWithChecksumCtx::add_row_checksum(const common::ObNewRow* row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(row));
  } else if (col_ids_.count() != row->count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column count is not equal",
        K(ret),
        "checksum column count",
        col_ids_.count(),
        "get_next_row column count",
        row->count_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row->count_; ++i) {
      checksum_[i] += row->cells_[i].checksum_v2(0);
    }
  }
  return ret;
}

int ObTableScanWithChecksum::ObTableScanWithChecksumCtx::report_checksum(const int64_t execution_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObIndexChecksumItem> checksum_items;
  const ObPartitionKey& pkey = scan_param_.pkey_;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_ids_.count(); ++i) {
    ObIndexChecksumItem item;
    item.execution_id_ = execution_id;
    item.tenant_id_ = extract_tenant_id(pkey.get_table_id());
    item.table_id_ = pkey.get_table_id();
    item.partition_id_ = pkey.get_partition_id();
    item.column_id_ = col_ids_.at(i);
    item.task_id_ = task_id_;
    item.checksum_ = checksum_[i];
    item.checksum_method_ = blocksstable::CCM_VALUE_ONLY;
    if (OB_FAIL(checksum_items.push_back(item))) {
      LOG_WARN("fail to push back item", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObIndexChecksumOperator::update_checksum(checksum_items, *GCTX.sql_proxy_))) {
      LOG_WARN("fail to update checksum", K(ret));
    }
  }
  return ret;
}

ObTableScanWithChecksum::ObTableScanWithChecksum(common::ObIAllocator& allocator) : ObTableScan(allocator)
{}

ObTableScanWithChecksum::~ObTableScanWithChecksum()
{}

int ObTableScanWithChecksum::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableScan::inner_open(ctx))) {
    LOG_WARN("fail to do ObTableScan inner open", K(ret));
  } else {
    ObTableScanWithChecksumCtx* scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanWithChecksumCtx, ctx, get_id());
    ObTaskExecutorCtx* executor_ctx = NULL;
    storage::ObPartitionService* part_service = NULL;
    if (OB_ISNULL(scan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, scan ctx must not be NULL", K(ret));
    } else {
      const ObTableParam& table_param = *scan_ctx->scan_param_.table_param_;
      if (OB_ISNULL(scan_ctx->scan_param_.table_param_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the table params is null", K(ret), K(scan_ctx->scan_param_.table_param_));
      } else if (OB_FAIL(scan_ctx->get_output_col_ids(table_param))) {
        LOG_WARN("fail to get output col ids", K(ret));
      } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get task executor ctx", K(ret));
      } else if (OB_ISNULL(part_service = executor_ctx->get_partition_service())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, partition service must not be NULL", K(ret));
      } else {
        ObTableScanWithChecksumInput* scan_input = NULL;
        if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanWithChecksumInput, ctx, get_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get op input", K(ret));
        } else {
          scan_ctx->task_id_ = scan_input->get_ob_task_id().get_task_id();
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(scan_ctx->allocate_checksum_memory())) {
          LOG_WARN("fail to allocate checksum memory", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableScanWithChecksum::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableScanWithChecksumCtx* scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanWithChecksumCtx, ctx, get_id());
  ObTableScanWithChecksumInput* scan_input = GET_PHY_OP_INPUT(ObTableScanWithChecksumInput, ctx, get_id());
  if (OB_ISNULL(scan_ctx) || OB_ISNULL(scan_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, scan ctx and scan input must not be null", K(ret), KP(scan_ctx), KP(scan_input));
  } else if (OB_FAIL(try_check_status(ctx))) {
    LOG_WARN("failed to check status", K(ret));
  } else if (OB_FAIL(ObTableScan::inner_get_next_row(ctx, row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to do inner get next row", K(ret));
    } else {
      // report checksum info
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = scan_ctx->report_checksum(scan_input->get_ob_task_id().get_execution_id()))) {
        LOG_WARN("fail to report checksum", K(tmp_ret));
        ret = tmp_ret;
      }
    }
  } else {
    if (OB_FAIL(scan_ctx->add_row_checksum(row))) {
      LOG_WARN("fail to add row checksum", K(ret));
    }
  }
  return ret;
}

int ObTableScanWithChecksum::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;

  ObTableScanWithChecksumInput* scan_input = NULL;
  if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanWithChecksumInput, ctx, get_id()))) {
    if (OB_FAIL(CREATE_PHY_OP_INPUT(ObTableScanWithChecksumInput, ctx, get_id(), get_type(), scan_input))) {
      LOG_WARN("fail to create table scan input", K(ret), "op_id", get_id(), "op_type", get_type());
    } else {
      scan_input->set_location_idx(0);
    }
  }

  if (OB_SUCC(ret)) {
    ObSQLSessionInfo* my_session = NULL;
    if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTableScanWithChecksumCtx, ctx, get_id(), get_type(), op_ctx))) {
      LOG_WARN("create physical operator context failed", K(ret), K(get_type()));
    } else if (OB_ISNULL(op_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator context is null");
    } else if (OB_FAIL(init_cur_row(*op_ctx, false))) {
      LOG_WARN("init current row failed", K(ret));
    } else if (OB_FAIL(static_cast<ObTableScanCtx*>(op_ctx)->init_table_allocator(ctx))) {
      LOG_WARN("fail to init table allocator", K(ret));
    } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get my session", K(ret));
    }
  }
  return ret;
}
