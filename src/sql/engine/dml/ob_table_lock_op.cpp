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
#include "ob_table_lock_op.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;
namespace sql {

OB_SERIALIZE_MEMBER((ObTableLockOpInput, ObTableModifyOpInput));

OB_SERIALIZE_MEMBER((ObTableLockSpec, ObTableModifySpec), for_update_wait_us_, skip_locked_);

ObTableLockSpec::ObTableLockSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObTableModifySpec(alloc, type), for_update_wait_us_(-1), skip_locked_(false)
{}

ObTableLockSpec::~ObTableLockSpec()
{}

ObTableLockOp::ObTableLockOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObTableModifyOp(exec_ctx, spec, input), lock_row_(), dml_param_(), part_key_(), part_infos_()
{}

int ObTableLockOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  const ObTableLockSpec& spec = MY_SPEC;
  int64_t schema_version = 0;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(spec.plan_) || OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid operator", K(ret), K(spec.plan_), K(child_));
  } else if (OB_FAIL(spec.plan_->get_base_table_version(spec.index_tid_, schema_version))) {
    LOG_WARN("failed to get base table version", K(ret));
  } else if (OB_FAIL(my_session->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    dml_param_.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    dml_param_.schema_version_ = schema_version;
    dml_param_.is_total_quantity_log_ = (ObBinlogRowImage::FULL == binlog_row_image);
    dml_param_.tz_info_ = TZ_INFO(my_session);
    dml_param_.sql_mode_ = my_session->get_sql_mode();
    dml_param_.table_param_ = &spec.table_param_;
    dml_param_.tenant_schema_version_ = plan_ctx->get_tenant_schema_version();
    for_update_wait_timeout_ = spec.for_update_wait_us_ > 0
                                   ? spec.for_update_wait_us_ + my_session->get_query_start_time()
                                   : spec.for_update_wait_us_;
    if (MY_SPEC.gi_above_ && OB_FAIL(get_gi_task())) {
      LOG_WARN("get granule iterator task failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_table_lock())) {
      LOG_WARN("do table lock failed", K(ret));
    }
  }
  return ret;
}

int ObTableLockOp::do_table_lock()
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(MY_SPEC.id_));
  } else if (OB_FAIL(get_part_location(part_infos_))) {
    LOG_WARN("failed to get part locations", K(ret));
  } else if (OB_UNLIKELY(part_infos_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition infos are empty", K(ret));
  } else if (MY_SPEC.from_multi_table_dml()) {
    if (OB_FAIL(lock_multi_part())) {
      if (OB_TRANSACTION_SET_VIOLATION != ret && OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("failed to lock multi part", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLockOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  const ObTableLockSpec& spec = MY_SPEC;
  if (OB_UNLIKELY(spec.from_multi_table_dml())) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(lock_single_part())) {
    if (OB_TRANSACTION_SET_VIOLATION != ret && OB_TRY_LOCK_ROW_CONFLICT != ret && OB_ITER_END != ret) {
      LOG_WARN("failed to lock next row", K(ret));
    }
  }
  return ret;
}

int ObTableLockOp::lock_single_part()
{
  int ret = OB_SUCCESS;
  const ObTableLockSpec& spec = MY_SPEC;
  ObPartitionService* partition_service = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  bool is_null = false;
  bool got_row = false;
  if (iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(spec.id_));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_)) ||
             OB_ISNULL(partition_service = executor_ctx->get_partition_service()) ||
             OB_ISNULL(my_session = GET_MY_SESSION(ctx_)) || OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor context is invalid", K(ret), K(executor_ctx), K(partition_service), K(my_session));
  } else {
    while (OB_SUCC(ret) && !got_row) {
      clear_evaluated_flag();
      if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (spec.need_filter_null_row_ &&
                 OB_FAIL(check_rowkey_is_null(spec.storage_row_output_, spec.storage_row_output_.count(), is_null))) {
        LOG_WARN("failed to check rowkey is null", K(ret));
      } else if (is_null) {
        // no need to lock
        got_row = true;
      } else if (OB_FAIL(project_row(spec.storage_row_output_, lock_row_))) {
        LOG_WARN("failed to project row", K(ret));
      } else if (OB_FAIL(partition_service->lock_rows(my_session->get_trans_desc(),
                     dml_param_,
                     for_update_wait_timeout_,
                     part_infos_.at(0).partition_key_,
                     lock_row_,
                     LF_NONE))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret &&
            OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret) {
          LOG_WARN("failed to lock row", K(ret));
        } else if (spec.is_skip_locked()) {
          ret = OB_SUCCESS;
        } else if (spec.is_nowait() && OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT;
        }
      } else {
        got_row = true;
        plan_ctx->add_affected_rows(1LL);
      }
    }
  }
  return ret;
}

int ObTableLockOp::lock_multi_part()
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* partition_service = NULL;
  ObSQLSessionInfo* my_session = NULL;
  const ObTableLockSpec& spec = MY_SPEC;
  if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_)) ||
      OB_ISNULL(partition_service = executor_ctx->get_partition_service()) ||
      OB_ISNULL(my_session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("execution context is invalid", K(ret), K(executor_ctx), K(partition_service), K(my_session));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_infos_.count(); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < part_infos_.at(i).part_row_cnt_; ++j) {
      clear_evaluated_flag();
      if (OB_FAIL(child_->get_next_row())) {
        LOG_WARN("failed to get next row", K(ret));
      } else if (OB_FAIL(project_row(spec.storage_row_output_, lock_row_))) {
        LOG_WARN("failed to get next row", K(ret));
      } else if (OB_FAIL(partition_service->lock_rows(my_session->get_trans_desc(),
                     dml_param_,
                     for_update_wait_timeout_,
                     part_infos_.at(i).partition_key_,
                     lock_row_,
                     LF_NONE))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret &&
            OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret) {
          LOG_WARN("failed to lock row", K(ret));
        } else if (spec.is_nowait() && OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT;
        }
      }
    }
  }
  return ret;
}

int ObTableLockOp::inner_close()
{
  return ObTableModifyOp::inner_close();
}

int ObTableLockOp::rescan()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.gi_above_ || MY_SPEC.from_multi_table_dml()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table select for update rescan not supported", K(ret));
  } else if (OB_FAIL(ObTableModifyOp::rescan())) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else {
    part_infos_.reset();
    part_key_.reset();
    if (nullptr != rowkey_dist_ctx_) {
      rowkey_dist_ctx_->clear();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_gi_task())) {
      LOG_WARN("get granule iterator task failed", K(ret));
    } else if (OB_FAIL(get_part_location(part_infos_))) {
      LOG_WARN("failed to get part locations", K(ret));
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
