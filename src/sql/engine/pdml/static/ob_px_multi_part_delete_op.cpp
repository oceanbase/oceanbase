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

#include "ob_px_multi_part_delete_op.h"
#include "storage/ob_dml_param.h"
#include "storage/ob_partition_service.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/px/ob_px_sqc_handler.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::common::serialization;

OB_SERIALIZE_MEMBER((ObPxMultiPartDeleteOpInput, ObPxMultiPartModifyOpInput));

OB_SERIALIZE_MEMBER(
    (ObPxMultiPartDeleteSpec, ObTableModifySpec), row_desc_, table_desc_, delete_row_exprs_, with_barrier_);

//////////////////////ObPxMultiPartDeleteSpec///////////////////
int ObPxMultiPartDeleteSpec::register_to_datahub(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (with_barrier_) {
    if (OB_ISNULL(ctx.get_sqc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else {
      ObIAllocator& allocator = ctx.get_sqc_handler()->get_safe_allocator();
      void* buf = ctx.get_allocator().alloc(sizeof(ObBarrierWholeMsg::WholeMsgProvider));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObBarrierWholeMsg::WholeMsgProvider* provider = new (buf) ObBarrierWholeMsg::WholeMsgProvider();
        ObSqcCtx& sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
        if (OB_FAIL(sqc_ctx.add_whole_msg_provider(get_id(), *provider))) {
          LOG_WARN("fail add whole msg provider", K(ret));
        }
      }
    }
  }
  return ret;
}

//////////////////////ObPxMultiPartDeleteOp///////////////////
int ObPxMultiPartDeleteOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("failed to inner open", K(ret));
  } else if (!(MY_SPEC.table_desc_.is_valid()) || !(MY_SPEC.row_desc_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table or row desc is invalid", K(ret), K(MY_SPEC.table_desc_), K(MY_SPEC.row_desc_));
  } else if (OB_FAIL(row_iter_wrapper_.init(&MY_SPEC.delete_row_exprs_, &child_->get_spec().output_))) {
    LOG_WARN("alloc delete row failed", K(ret));
  } else if (OB_FAIL(data_driver_.init(ctx_.get_allocator(), MY_SPEC.table_desc_, this, this))) {
    LOG_WARN("failed to init data driver", K(ret));
  } else if (MY_SPEC.with_barrier_) {
    if (OB_ISNULL(input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the input is null", K(ret));
    } else if (OB_FAIL(data_driver_.set_with_barrier(
                   MY_SPEC.get_id(), static_cast<const ObPxMultiPartModifyOpInput*>(input_)))) {
      LOG_WARN("faile to set barrier", K(ret));
    }
  }
  LOG_TRACE("pdml static delete op", K(ret), K_(MY_SPEC.table_desc), K_(MY_SPEC.row_desc));
  return ret;
}

int ObPxMultiPartDeleteOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the child op is null", K(ret));
  } else if (MY_SPEC.is_returning_) {
    if (OB_FAIL(data_driver_.get_next_row(ctx_, child_->get_spec().output_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed get next row from data driver", K(ret));
      } else {
        LOG_TRACE("data driver has been iterated to end");
      }
    } else {
      clear_evaluated_flag();
      LOG_DEBUG("get one row for returning", "row", ROWEXPR2STR(*ctx_.get_eval_ctx(), MY_SPEC.output_));
    }
  } else {
    do {
      if (OB_FAIL(data_driver_.get_next_row(ctx_, child_->get_spec().output_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed get next row from data driver", K(ret));
        } else {
          LOG_TRACE("data driver has been iterated to end");
        }
      } else {
        clear_evaluated_flag();
        LOG_DEBUG("get one row for delete loop", "row", ROWEXPR2STR(*ctx_.get_eval_ctx(), child_->get_spec().output_));
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

int ObPxMultiPartDeleteOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_close())) {
    LOG_WARN("failed to inner close table modify", K(ret));
  } else {
    data_driver_.destroy();
    row_iter_wrapper_.reset();
  }
  return ret;
}

//////////// pdml data interface implementation: reader & writer ////////////
int ObPxMultiPartDeleteOp::read_row(ObExecContext& ctx, const ObExprPtrIArray*& row, int64_t& part_id)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null", K(ret));
  } else if (OB_FAIL(child_->get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get next row from child", K(ret));
    }
  } else {
    clear_evaluated_flag();
    const int64_t part_id_idx = MY_SPEC.row_desc_.get_part_id_index();
    row = &child_->get_spec().output_;
    if (NO_PARTITION_ID_FLAG == part_id_idx) {
      part_id = 0;
    } else if (child_->get_spec().output_.count() > part_id_idx) {
      ObExpr* expr = child_->get_spec().output_.at(part_id_idx);
      ObDatum& expr_datum = expr->locate_expr_datum(*ctx_.get_eval_ctx());
      part_id = expr_datum.get_int();
      LOG_DEBUG("get the part id", K(ret), K(expr_datum));
    }
  }
  return ret;
}

int ObPxMultiPartDeleteOp::write_rows(ObExecContext& ctx, ObPartitionKey& pkey, ObPDMLOpRowIterator& dml_row_iter)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  storage::ObDMLBaseParam dml_param;
  ObSQLSessionInfo* my_session = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* ps = NULL;
  const ObPhysicalPlan* phy_plan = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;

  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(ps = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL(phy_plan = plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get phy_plan", K(ret));
  } else if (OB_FAIL(
                 fill_dml_base_param(MY_SPEC.table_desc_.index_tid_, *my_session, *phy_plan, *plan_ctx, dml_param))) {
    LOG_WARN("failed to fill dml base param", K(ret));
  } else {
    row_iter_wrapper_.set_iterator(dml_row_iter);
    int64_t affected_rows = 0;
    if (OB_FAIL(ps->delete_rows(
            my_session->get_trans_desc(), dml_param, pkey, MY_SPEC.column_ids_, &row_iter_wrapper_, affected_rows))) {
      LOG_WARN(
          "failed to write rows to storage layer", K(ret), K(MY_SPEC.is_returning_), K(MY_SPEC.index_tid_), K(pkey));
    } else {
      if (!(MY_SPEC.is_pdml_index_maintain_)) {
        plan_ctx->add_affected_rows(affected_rows);
        plan_ctx->add_row_deleted_count(affected_rows);
      }
      LOG_TRACE("pdml delete ok", K(pkey), K(MY_SPEC.is_pdml_index_maintain_), K(affected_rows));
    }
  }
  return ret;
}

int ObPxMultiPartDeleteOp::fill_dml_base_param(uint64_t index_tid, ObSQLSessionInfo& my_session,
    const ObPhysicalPlan& my_phy_plan, const ObPhysicalPlanCtx& my_plan_ctx, storage::ObDMLBaseParam& dml_param) const
{
  int ret = OB_SUCCESS;
  int64_t schema_version = 0;
  int64_t binlog_row_image = share::ObBinlogRowImage::FULL;
  if (OB_FAIL(my_phy_plan.get_base_table_version(index_tid, schema_version))) {
    LOG_WARN("failed to get base table version", K(ret));
  } else if (OB_FAIL(my_session.get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    dml_param.schema_version_ = schema_version;
    dml_param.is_total_quantity_log_ = (share::ObBinlogRowImage::FULL == binlog_row_image);
    dml_param.timeout_ = my_plan_ctx.get_ps_timeout_timestamp();
    dml_param.sql_mode_ = my_session.get_sql_mode();
    dml_param.tz_info_ = TZ_INFO(&my_session);
    dml_param.tenant_schema_version_ = my_plan_ctx.get_tenant_schema_version();
  }
  return ret;
}

/**ObPDMLRowIteratorWrapper**/

int ObPxMultiPartDeleteOp::ObPDMLOpRowIteratorWrapper::init(
    const ExprFixedArray* delete_exprs, const ExprFixedArray* read_row_from_iter)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (OB_ISNULL(delete_exprs) || OB_ISNULL(read_row_from_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the delete row is null", K(ret));
  } else if (delete_exprs->count() < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("alloc delete row failed", K(ret), K(delete_exprs->count()));
  } else {
    delete_row_exprs_ = delete_exprs;
    read_row_from_iter_ = read_row_from_iter;
  }
  return ret;
}

int ObPxMultiPartDeleteOp::ObPDMLOpRowIteratorWrapper::get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is null", K(ret));
  } else if (OB_FAIL(iter_->get_next_row(*read_row_from_iter_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get next row from child", K(ret));
    }
  } else {
    if (OB_FAIL(op_->project_row(*delete_row_exprs_, delete_row_))) {
      LOG_WARN("failed to project row for delete iter", K(ret));
    } else {
      op_->clear_evaluated_flag();
    }
    row = &delete_row_;
    LOG_DEBUG("iter one delete row", K(ret), K(*row));
  }
  return ret;
}
