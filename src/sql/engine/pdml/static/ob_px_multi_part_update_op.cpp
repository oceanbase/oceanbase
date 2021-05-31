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
#include "ob_px_multi_part_update_op.h"
#include "storage/ob_dml_param.h"
#include "storage/ob_partition_service.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::common::serialization;

OB_SERIALIZE_MEMBER((ObPxMultiPartUpdateOpInput, ObPxMultiPartModifyOpInput));

OB_SERIALIZE_MEMBER((ObPxMultiPartUpdateSpec, ObTableModifySpec), row_desc_, table_desc_, updated_column_ids_,
    updated_column_infos_, old_row_exprs_, new_row_exprs_);

int ObPxMultiPartUpdateSpec::set_updated_column_info(
    int64_t array_index, uint64_t column_id, uint64_t project_index, bool auto_filled_timestamp)
{
  int ret = OB_SUCCESS;
  ColumnContent column;
  column.projector_index_ = project_index;
  column.auto_filled_timestamp_ = auto_filled_timestamp;
  CK(array_index >= 0 && array_index < updated_column_ids_.count());
  CK(array_index >= 0 && array_index < updated_column_infos_.count());
  if (OB_SUCC(ret)) {
    updated_column_ids_.at(array_index) = column_id;
    updated_column_infos_.at(array_index) = column;
  }
  return ret;
}

int ObPxMultiPartUpdateSpec::init_updated_column_count(common::ObIAllocator& allocator, int64_t count)
{
  UNUSED(allocator);
  int ret = common::OB_SUCCESS;
  OZ(updated_column_infos_.prepare_allocate(count));
  OZ(updated_column_ids_.prepare_allocate(count));

  return ret;
}

//////////////////////ObPxMultiPartInsertOp///////////////////
int ObPxMultiPartUpdateOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("failed to inner open", K(ret));
  } else if (!(MY_SPEC.table_desc_.is_valid()) || !(MY_SPEC.row_desc_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table or row desc is invalid", K(ret), K(MY_SPEC.table_desc_), K(MY_SPEC.row_desc_));
  } else if (OB_FAIL(data_driver_.init(ctx_.get_allocator(), MY_SPEC.table_desc_, this, this))) {
    LOG_WARN("failed to init data driver", K(ret));
  }
  LOG_TRACE("pdml static update op", K(ret), K_(MY_SPEC.table_desc), K_(MY_SPEC.row_desc));
  return ret;
}

int ObPxMultiPartUpdateOp::inner_get_next_row()
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
        LOG_DEBUG("get one row for update loop", "row", ROWEXPR2STR(*ctx_.get_eval_ctx(), child_->get_spec().output_));
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

int ObPxMultiPartUpdateOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_close())) {
    LOG_WARN("failed to inner close table modify", K(ret));
  } else {
    data_driver_.destroy();
  }
  return ret;
}

int ObPxMultiPartUpdateOp::process_row()
{
  int ret = OB_SUCCESS;
  bool is_filtered = false;
  OZ(check_row_null(MY_SPEC.new_row_exprs_, MY_SPEC.column_infos_));
  OZ(filter_row_for_check_cst(MY_SPEC.check_constraint_exprs_, is_filtered));
  OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
  return ret;
}

//////////// pdml data interface implementation: reader & writer ////////////
int ObPxMultiPartUpdateOp::read_row(ObExecContext& ctx, const ObExprPtrIArray*& row, int64_t& part_id)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null", K(ret));
  } else if (OB_FAIL(child_->get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get next row from child", K(ret));
    }
  } else {
    clear_evaluated_flag();
    // Obtain the partition corresponding to the corresponding row through partition id expr
    const int64_t part_id_idx = MY_SPEC.row_desc_.get_part_id_index();
    row = &child_->get_spec().output_;
    if (NO_PARTITION_ID_FLAG == part_id_idx) {
      // default partition id 0
      part_id = 0;
    } else if (child_->get_spec().output_.count() > part_id_idx) {
      ObExpr* expr = child_->get_spec().output_.at(part_id_idx);
      ObDatum& expr_datum = expr->locate_expr_datum(*ctx_.get_eval_ctx());
      part_id = expr_datum.get_int();
      LOG_DEBUG("get the part id", K(ret), K(expr_datum));
    }
  }

  if (!MY_SPEC.is_pdml_index_maintain_ && OB_SUCC(ret)) {
    // only main table needs to check constraint
    if (OB_FAIL(process_row())) {
      LOG_WARN("fail process row", K(ret));
    }
  }
  return ret;
}

int ObPxMultiPartUpdateOp::write_rows(ObExecContext& ctx, ObPartitionKey& pkey, ObPDMLOpRowIterator& dml_row_iter)
{
  int ret = OB_SUCCESS;
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
    ObPDMLOpRowIteratorWrapper row_iter_wrapper(pkey, dml_param, dml_row_iter, *this);
    int64_t affected_rows = 0;
    if (OB_FAIL(ps->update_rows(my_session->get_trans_desc(),
            dml_param,
            pkey,
            MY_SPEC.column_ids_,
            MY_SPEC.updated_column_ids_,
            &row_iter_wrapper,
            affected_rows))) {
      LOG_WARN(
          "failed to write rows to storage layer", K(ret), K(MY_SPEC.is_returning_), K(MY_SPEC.index_tid_), K(pkey));
    } else {
      if (!(MY_SPEC.is_pdml_index_maintain_)) {
        plan_ctx->add_row_matched_count(found_rows_);
        plan_ctx->add_row_duplicated_count(changed_rows_);
        plan_ctx->add_affected_rows(
            my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS ? found_rows_ : affected_rows_);
        LOG_TRACE("pdml update ok",
            K(pkey),
            K(MY_SPEC.is_pdml_index_maintain_),
            K(affected_rows),
            K(affected_rows_),
            K(found_rows_),
            K(changed_rows_));
        found_rows_ = 0;
        changed_rows_ = 0;
        affected_rows_ = 0;
      }
    }
  }
  return ret;
}

int ObPxMultiPartUpdateOp::fill_dml_base_param(uint64_t index_tid, ObSQLSessionInfo& my_session,
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

int ObPxMultiPartUpdateOp::ObPDMLOpRowIteratorWrapper::get_next_row(common::ObNewRow*& row)
{
  return op_.get_next_row(pkey_, dml_param_, iter_, row);
}

// for row-multiplex: one_row => old_row + new_row
int ObPxMultiPartUpdateOp::get_next_row(
    ObPartitionKey& pkey, storage::ObDMLBaseParam& dml_param, ObPDMLOpRowIterator& iter, common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (has_got_old_row_) {
    row = &new_row_;
    has_got_old_row_ = false;
    LOG_DEBUG("iter one update new row", K(ret), K(*row));
  } else {
    bool need_update = false;
    do {
      if (OB_FAIL(iter.get_next_row(child_->get_spec().output_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail get next row from child", K(ret));
        }
      } else {
        if (OB_FAIL(project_row(MY_SPEC.old_row_exprs_, old_row_))) {
          LOG_WARN("failed to project row for update iter", K(ret));
        } else if (OB_FAIL(project_row(MY_SPEC.new_row_exprs_, new_row_))) {
          LOG_WARN("failed to project row for update iter", K(ret));
        } else if (OB_FAIL(check_updated_value(*this,
                       MY_SPEC.get_assign_columns(),
                       MY_SPEC.old_row_exprs_,
                       MY_SPEC.new_row_exprs_,
                       need_update))) {
          LOG_WARN("fail check updated value", K_(old_row), K_(new_row), K(ret));
        } else if (!need_update && OB_FAIL(lock_row(MY_SPEC.old_row_exprs_, dml_param, pkey))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            LOG_WARN("fail lock row", K_(old_row), K(pkey), K(ret));
          }
        } else if (need_update) {
          row = &old_row_;
          has_got_old_row_ = true;
          LOG_DEBUG("iter one update old row", K(ret), K(*row));
        }
        clear_evaluated_flag();
      }
    } while (!need_update && OB_SUCC(ret));
  }
  return ret;
}
