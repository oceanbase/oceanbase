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

#include "sql/engine/dml/ob_table_delete_op.h"
#include "storage/ob_partition_service.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_task_executor_ctx.h"

namespace oceanbase {
namespace sql {

OB_SERIALIZE_MEMBER((ObTableDeleteOpInput, ObTableModifyOpInput));

OB_SERIALIZE_MEMBER((ObTableDeleteSpec, ObTableModifySpec));

int ObTableDeleteOp::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::rescan())) {
    LOG_WARN("table modify rescan failed", K(ret));
  } else {
    part_infos_.reset();
    if (NULL != rowkey_dist_ctx_) {
      rowkey_dist_ctx_->clear();
    }
    if (OB_FAIL(get_gi_task())) {
      LOG_WARN("get granule iterator task failed", K(ret));
    } else if (OB_FAIL(do_table_delete())) {
      LOG_WARN("do table delete failed", K(ret));
    }
  }
  return ret;
}

int ObTableDeleteOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  storage::ObPartitionService* partition_service = NULL;
  const ObPhysicalPlan* phy_plan = NULL;
  int64_t schema_version = 0;
  int64_t binlog_row_image = share::ObBinlogRowImage::FULL;

  if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL(session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get session", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_ISNULL(phy_plan = plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get phy_plan", K(ret));
  } else if (OB_FAIL(phy_plan->get_base_table_version(MY_SPEC.index_tid_, schema_version))) {
    LOG_WARN("failed to get base table version", K(ret));
  } else if (OB_FAIL(session->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    dml_param_.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    dml_param_.schema_version_ = schema_version;
    dml_param_.is_total_quantity_log_ = (share::ObBinlogRowImage::FULL == binlog_row_image);
    dml_param_.sql_mode_ = session->get_sql_mode();
    dml_param_.tz_info_ = TZ_INFO(session);
    dml_param_.table_param_ = &MY_SPEC.table_param_;
    dml_param_.tenant_schema_version_ = plan_ctx->get_tenant_schema_version();
    NG_TRACE(delete_start_delete);
    if (MY_SPEC.gi_above_) {
      if (OB_FAIL(get_gi_task())) {
        LOG_WARN("get granule iterator task failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_table_delete())) {
        LOG_WARN("do table delete failed", K(ret));
      }
    }
  }
  NG_TRACE(delete_iter_end);
  return ret;
}

int ObTableDeleteOp::do_table_delete()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = ctx_.get_physical_plan_ctx();
  int64_t affected_rows = 0;
  if (iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(iter_end_), K(MY_SPEC.id_));
  } else if (OB_FAIL(get_part_location(part_infos_))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (MY_SPEC.is_returning_) {
    // do nothing, delete rows in returning.
  } else if (OB_FAIL(delete_rows(affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("delete wor to partition storage failed", K(ret));
    }
  } else if (!MY_SPEC.from_multi_table_dml()) {
    plan_ctx->add_affected_rows(affected_rows);
  }
  return ret;
}

int ObTableDeleteOp::delete_rows(int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = ctx_.get_my_session();
  storage::ObPartitionService* partition_service = NULL;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  DMLRowIterator dml_row_iter(ctx_, *this);
  if (OB_ISNULL(session) || OB_ISNULL(executor_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session or exeutor_ctx is NULL", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_UNLIKELY(part_infos_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part info is empty", K(ret));
  } else if (OB_FAIL(dml_row_iter.init())) {
    LOG_WARN("init dml row iter failed", K(ret));
  } else if (OB_LIKELY(part_infos_.count() == 1)) {
    if (OB_FAIL(partition_service->delete_rows(session->get_trans_desc(),
            dml_param_,
            part_infos_.at(0).partition_key_,
            MY_SPEC.column_ids_,
            &dml_row_iter,
            affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("delete row to partition storage failed", K(ret));
      }
    }
  } else {
    // multi partition
    for (int64_t i = 0; OB_SUCC(ret) && i < part_infos_.count(); ++i) {
      const ObPartitionKey& part_key = part_infos_.at(i).partition_key_;
      part_row_cnt_ = part_infos_.at(i).part_row_cnt_;
      ObNewRow* row = NULL;
      while (OB_SUCC(ret) && OB_SUCC(dml_row_iter.get_next_row(row))) {
        if (OB_FAIL(partition_service->delete_row(
                session->get_trans_desc(), dml_param_, part_key, MY_SPEC.column_ids_, *row))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            LOG_WARN("delete row to partition storage failed", K(ret));
          }
        } else {
          affected_rows += 1;
          if (part_row_cnt_ <= 0) {
            break;
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("process delete row failed", K(ret));
    }
  }
  return ret;
}

int ObTableDeleteOp::prepare_next_storage_row(const ObExprPtrIArray*& output)
{
  output = &MY_SPEC.storage_row_output_;
  return inner_get_next_row();
}

int ObTableDeleteOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(child_));
  OX(try_check_status());
  if (OB_SUCC(ret) && iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(MY_SPEC.id_), K(iter_end_));
    ret = OB_ITER_END;
  }
  if (OB_SUCC(ret)) {
    if (MY_SPEC.from_multi_table_dml()) {
      --part_row_cnt_;
    }
    bool get_next_row = false;
    do {
      get_next_row = false;
      bool is_null = false;
      if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from child op failed", K(ret));
        }
      } else {
        clear_evaluated_flag();
        // see comment in CG, optimizer guarantee that:
        // first rowkey count of child output is rowkey.
        if (MY_SPEC.need_filter_null_row_ && !MY_SPEC.from_multi_table_dml()) {
          if (OB_FAIL(check_rowkey_is_null(child_->get_spec().output_, MY_SPEC.primary_key_ids_.count(), is_null))) {
            LOG_WARN("check rowkey is null failed", K(ret));
          } else {
            get_next_row = is_null;
          }
        }
        if (OB_SUCC(ret) && !get_next_row && !MY_SPEC.from_multi_table_dml()) {
          bool is_distinct = false;
          if (OB_FAIL(check_rowkey_whether_distinct(child_->get_spec().output_,
                  MY_SPEC.primary_key_ids_.count(),
                  MY_SPEC.distinct_algo_,
                  rowkey_dist_ctx_,
                  is_distinct))) {
            LOG_WARN("check rowkey whether distinct failed", K(ret));
          } else {
            get_next_row = !is_distinct;
          }
        }
      }
    } while (OB_SUCC(ret) && get_next_row);
  }
  // TODO: laster for engien 3.0
  if (OB_SUCC(ret) && !MY_SPEC.from_multi_table_dml()) {
    OZ(ForeignKeyHandle::do_handle_old_row(*this, MY_SPEC.get_fk_args(), MY_SPEC.storage_row_output_));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
