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

#include "sql/engine/table/ob_table_lookup_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_table_lookup.h"
#include "sql/engine/table/ob_lookup_task_builder.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/executor/ob_task.h"
#include "sql/executor/ob_mini_task_executor.h"

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER((ObTableLookupSpec, ObOpSpec), lookup_info_, calc_part_id_expr_);

ObTableLookupOp::ObTableLookupOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(exec_ctx, spec, input),
      allocator_(),
      result_(),
      result_iter_(),
      state_(INDEX_SCAN),
      end_(false),
      lookup_task_executor_(exec_ctx.get_allocator()),
      task_builder_(exec_ctx.get_allocator()),
      partitions_ranges_(),
      partition_cnt_(0),
      schema_guard_(nullptr)
{}

int ObTableLookupOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSqlCtx* sql_ctx = NULL;
  ObExecutorRpcImpl* exec_rpc = NULL;
  ObTaskExecutorCtx* task_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  if (OB_ISNULL(sql_ctx = ctx_.get_sql_ctx()) || OB_ISNULL(sql_ctx->schema_guard_) ||
      OB_ISNULL(MY_SPEC.calc_part_id_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(sql_ctx), KP(MY_SPEC.calc_part_id_expr_));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx_, exec_rpc))) {
    LOG_WARN("get task executor rpc failed", K(ret));
  } else if (OB_ISNULL(MY_SPEC.remote_tsc_spec_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("remote_table_scan is null", K(ret));
  } else {
    // Initialize the plan builder and plan executor
    ObCurTraceId::TraceId execution_id;
    ObJobID ob_job_id;
    execution_id.init(ObCurTraceId::get_addr());
    ob_job_id.set_mini_task_type();
    ob_job_id.set_server(execution_id.get_addr());
    ob_job_id.set_execution_id(execution_id.get_seq());
    ob_job_id.set_job_id(MY_SPEC.remote_tsc_spec_->id_);
    if (OB_FAIL(task_builder_.init(
            MY_SPEC.lookup_info_, MY_SPEC.get_phy_plan(), ob_job_id, &ctx_, NULL, MY_SPEC.remote_tsc_spec_))) {
      LOG_WARN("init task builder failed", K(ret));
    } else if (OB_FAIL(lookup_task_executor_.init(*my_session, exec_rpc))) {
      LOG_WARN("init executor failed", K(ret));
    } else if (OB_FAIL(set_partition_cnt(MY_SPEC.lookup_info_.partition_num_))) {
      LOG_WARN("the partition cnt is invalid", K(ret));
    } else {
      ObMemAttr mem_attr;
      mem_attr.tenant_id_ = my_session->get_effective_tenant_id();
      mem_attr.label_ = ObModIds::OB_SQL_TABLE_LOOKUP;
      // The ObScanner will not be transmitted over the network, and it contains local back-table query result data, so
      // there is no 64M limit.
      result_.get_task_result().set_mem_size_limit(INT64_MAX);
      allocator_.set_attr(mem_attr);
      partitions_ranges_.set_mem_attr(mem_attr);
      result_.set_tenant_id(my_session->get_effective_tenant_id());
      schema_guard_ = sql_ctx->schema_guard_;
      if (OB_FAIL(result_.init())) {
        LOG_WARN("fail to init task result", K(ret));
      } else if (OB_FAIL((result_.get_task_result().get_datum_store().begin(result_iter_)))) {
        LOG_WARN("fail to get datum store iter", K(ret));
      }
    }
  }

  return ret;
}

int ObTableLookupOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  do {
    switch (state_) {
      case INDEX_SCAN: {
        int64_t count = 0;
        int64_t part_row_cnt = 0;
        while (count < DEFAULT_BATCH_ROW_COUNT && part_row_cnt < DEFAULT_PARTITION_BATCH_ROW_COUNT && OB_SUCC(ret)) {
          ++count;
          if (OB_FAIL(child_->get_next_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next row from child failed", K(ret));
            }
          } else {
            clear_evaluated_flag();
            if (OB_FAIL(process_row(part_row_cnt))) {
              LOG_WARN("store the row failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) || OB_ITER_END == ret) {
          state_ = DISTRIBUTED_LOOKUP;
          set_end(OB_ITER_END == ret);
          ret = OB_SUCCESS;
        }
        break;
      }
      case DISTRIBUTED_LOOKUP: {
        if (OB_FAIL(execute())) {
          LOG_WARN("remote lookup failed", K(ret));
        } else {
          state_ = OUTPUT_ROWS;
        }
        break;
      }
      case OUTPUT_ROWS: {
        if (OB_FAIL(get_store_next_row())) {
          if (OB_ITER_END == ret) {
            if (!end()) {
              state_ = INDEX_SCAN;
              ret = OB_SUCCESS;
            } else {
              state_ = EXECUTION_FINISHED;
            }
          } else {
            LOG_WARN("look up get next row failed", K(ret));
          }
        } else {
          got_next_row = true;
          LOG_DEBUG("got next row from table lookup", K(MY_SPEC.output_));
        }
        break;
      }
      case EXECUTION_FINISHED: {
        if (OB_SUCC(ret) || OB_ITER_END == ret) {
          ret = OB_ITER_END;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state", K(state_));
      }
    }
  } while (!(got_next_row || OB_FAIL(ret)));

  return ret;
}

int ObTableLookupOp::process_row(int64_t& part_row_cnt)
{
  int ret = OB_SUCCESS;
  share::schema::ObMultiVersionSchemaService* schema_service = NULL;
  ObTaskExecutorCtx* task_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObSEArray<int64_t, 1> partition_ids;
  ObDatum* partition_id_datum = NULL;
  if (OB_FAIL(MY_SPEC.calc_part_id_expr_->eval(eval_ctx_, partition_id_datum))) {
    LOG_WARN("fail to calc part id", K(ret), K(*MY_SPEC.calc_part_id_expr_));
  } else if (ObExprCalcPartitionId::NONE_PARTITION_ID == partition_id_datum->get_int()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no partition matched", K(ret), K(child_->get_spec().output_));
  } else if (OB_FAIL(store_row(partition_id_datum->get_int(),
                 child_->get_spec().output_,
                 MY_SPEC.lookup_info_.is_old_no_pk_table_,
                 part_row_cnt))) {
    LOG_WARN("the lookup ctx store row failed", K(ret));
  } else {
    LOG_DEBUG("table lookup process row",
        "child output",
        ROWEXPR2STR(eval_ctx_, child_->get_spec().output_),
        K(*partition_id_datum),
        K(MY_SPEC.lookup_info_));
  }

  return ret;
}

// Extract the row from the lower layer and materialize it in the ctx of the table lookup.
int ObTableLookupOp::store_row(
    int64_t part_id, const ObIArray<ObExpr*>& row, const bool table_has_hidden_pk, int64_t& part_row_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partitions_ranges_.add_range(
          eval_ctx_, part_id, MY_SPEC.lookup_info_.ref_table_id_, row, table_has_hidden_pk, part_row_cnt))) {
    LOG_WARN("Failed to add range", K(ret));
  }

  return ret;
}

// The executor will send the data to the remote back to the meter.
int ObTableLookupOp::execute()
{
  int ret = OB_SUCCESS;
  ObMiniTaskRetryInfo retry_info;
  if (OB_FAIL(task_builder_.build_lookup_tasks(
          ctx_, partitions_ranges_, MY_SPEC.lookup_info_.table_id_, MY_SPEC.lookup_info_.ref_table_id_))) {
    LOG_WARN("Failed to build lookup tasks", K(ret));
  } else if (OB_FAIL(lookup_task_executor_.execute(ctx_,
                 task_builder_.get_lookup_task_list(),
                 task_builder_.get_lookup_taskinfo_list(),
                 retry_info,
                 result_))) {
    LOG_WARN("Failed to remote execute table scan", K(ret));
  }
  LOG_TRACE("Lookup get some result",
      K(result_.get_task_result().get_datum_store()),
      K(retry_info.need_retry()),
      K(partitions_ranges_.count()));
  retry_info.do_retry_execution();
  while (OB_SUCC(ret) && retry_info.need_retry()) {
    retry_info.add_retry_times();
    LOG_TRACE("Table lookup retry remote partition", K(retry_info.get_retry_times()));
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("Check physical plan status failed", K(ret));
    } else if (OB_FAIL(task_builder_.rebuild_overflow_task(retry_info))) {
      LOG_WARN("Failed to rebuild overflow task", K(ret));
    } else if (OB_FAIL(lookup_task_executor_.retry_overflow_task(ctx_,
                   task_builder_.get_lookup_task_list(),
                   task_builder_.get_lookup_taskinfo_list(),
                   retry_info,
                   result_))) {
      LOG_WARN("Failed to retry overflow task", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(clean_mem())) {
      LOG_WARN("Failed to clean range mem", K(ret));
    }
  }
  return ret;
}

// The cells of each row before the lower layer are materialized in the ranges,
// and the ranges have been serialized and sent to the remote end in execute,
// and this part of the memory is reclaimed here. Otherwise, all rows of cells
// will be materialized in this operator, which consumes memory.
int ObTableLookupOp::clean_mem()
{
  int ret = OB_SUCCESS;
  partitions_ranges_.release();
  task_builder_.reset();
  return ret;
}

int ObTableLookupOp::set_partition_cnt(int64_t partition_cnt)
{
  int ret = OB_SUCCESS;
  if (partition_cnt <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the partition cnt is invalid", K(ret), K(partition_cnt));
  } else {
    partition_cnt_ = partition_cnt;
  }
  return ret;
}

void ObTableLookupOp::destroy()
{
  result_.reset();
  result_iter_.reset();
  state_ = EXECUTION_FINISHED;
  task_builder_.reset();
  end_ = true;
  lookup_task_executor_.destroy();
  partitions_ranges_.release();
  allocator_.~ObArenaAllocator();
  ObOperator::destroy();
}

void ObTableLookupOp::reset()
{
  result_.reset();
  result_iter_.reset();
  state_ = INDEX_SCAN;
  task_builder_.reset();
  allocator_.reset();
  end_ = false;
}

// After returning to the table from the remote end, take out the data CTX
// temporary storage and spit it out upwards.
int ObTableLookupOp::get_store_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(result_iter_.get_next_row_skip_const(eval_ctx_, MY_SPEC.remote_tsc_spec_->output_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      // We get a ob_iter_end, reuse the scanner.
      result_.get_task_result().reuse();
      int tmp_ret = result_.get_task_result().get_datum_store().begin(result_iter_);
      if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("fail to set datum store iter begin", K(ret));
      }
    }
  }
  return ret;
}

int ObTableLookupOp::wait_all_task(ObPhysicalPlanCtx* plan_ctx)
{
  int ret = OB_SUCCESS;
  /**
   * wait_all_task() will be called in close() of multi table dml operators, and close()
   * will be called if open() has been called, no matter open() return OB_SUCCESS or not.
   * so if we get a NULL multi_dml_ctx or plan_ctx here, just ignore it.
   */
  if (OB_NOT_NULL(plan_ctx) && OB_FAIL(lookup_task_executor_.wait_all_task(plan_ctx->get_timeout_timestamp()))) {
    LOG_WARN("wait all task failed", K(ret));
  }
  return ret;
}

int ObTableLookupOp::inner_close()
{
  int ret = OB_SUCCESS;
  int wait_ret = wait_all_task(ctx_.get_physical_plan_ctx());
  if (OB_SUCCESS != wait_ret) {
    LOG_WARN("wait all task failed", K(wait_ret));
  }
  if (OB_FAIL(clean_mem())) {
    LOG_WARN("failed to clean ranges mem", K(ret));
  } else {
    ret = wait_ret;
  }

  return ret;
}

int ObTableLookupOp::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clean_mem())) {
    LOG_WARN("failed to clean ranges mem", K(ret));
  } else if (OB_FAIL(child_->rescan())) {
    LOG_WARN("rescan operator failed", K(ret));
  } else {
    reset();
  }

  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
