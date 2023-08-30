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
#include "ob_px_multi_part_insert_op.h"
#include "storage/access/ob_dml_param.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/engine/cmd/ob_table_direct_insert_service.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::common::serialization;
using namespace oceanbase::observer;

OB_SERIALIZE_MEMBER((ObPxMultiPartInsertOpInput, ObPxMultiPartModifyOpInput));

OB_SERIALIZE_MEMBER((ObPxMultiPartInsertSpec, ObTableModifySpec),
                    row_desc_,
                    ins_ctdef_);


//////////////////////ObPxMultiPartInsertOp///////////////////
int ObPxMultiPartInsertOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("failed to inner open", K(ret));
  } else if (OB_FAIL(ObDMLService::init_ins_rtdef(dml_rtctx_,
                                                  ins_rtdef_,
                                                  MY_SPEC.ins_ctdef_,
                                                  trigger_clear_exprs_,
                                                  fk_checkers_))) {
    LOG_WARN("init insert rtdef failed", K(ret));
  } else if (!(MY_SPEC.row_desc_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table or row desc is invalid", K(ret), K(MY_SPEC.row_desc_));
  } else if (OB_FAIL(data_driver_.init(get_spec(), ctx_.get_allocator(), ins_rtdef_, this, this,
                                       nullptr, MY_SPEC.ins_ctdef_.is_heap_table_))) {
    LOG_WARN("failed to init data driver", K(ret));
  }
  if (OB_SUCC(ret)) {
    const ObPhysicalPlan *plan = GET_PHY_PLAN_CTX(ctx_)->get_phy_plan();
    if (ObTableDirectInsertService::is_direct_insert(*plan)) {
      int64_t task_id = ctx_.get_px_task_id() + 1;
      if (OB_FAIL(ObTableDirectInsertService::open_task(plan->get_append_table_id(), task_id, table_ctx_))) {
        LOG_WARN("failed to open table direct insert task", KR(ret),
            K(plan->get_append_table_id()), K(task_id));
      } else {
        ins_rtdef_.das_rtdef_.direct_insert_task_id_ = task_id;
      }
    }
  }
  LOG_TRACE("pdml static insert op", K(ret), K_(MY_SPEC.row_desc), K_(MY_SPEC.ins_ctdef));
  return ret;
}

int ObPxMultiPartInsertOp::inner_get_next_row()
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
      LOG_DEBUG("get one row for returning",
        "row", ROWEXPR2STR(get_eval_ctx(), MY_SPEC.output_));
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
        LOG_DEBUG("get one row for insert loop",
          "row", ROWEXPR2STR(get_eval_ctx(), child_->get_spec().output_));
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

int ObPxMultiPartInsertOp::inner_close()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObPhysicalPlan *plan = GET_PHY_PLAN_CTX(ctx_)->get_phy_plan();
  if (ObTableDirectInsertService::is_direct_insert(*plan)) {
    int64_t task_id = ctx_.get_px_task_id() + 1;
    int error_code = (static_cast<const ObPxMultiPartInsertOpInput *>(input_))->get_error_code();
    if (OB_TMP_FAIL(ObTableDirectInsertService::close_task(plan->get_append_table_id(),
                                                           task_id,
                                                           table_ctx_,
                                                           error_code))) {
      LOG_WARN("failed to close table direct insert task", KR(tmp_ret),
          K(plan->get_append_table_id()), K(task_id), K(error_code));
    }
  }
  if (OB_FAIL(ObTableModifyOp::inner_close())) {
    LOG_WARN("failed to inner close table modify", K(ret));
  } else {
    data_driver_.destroy();
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  return ret;
}

int ObPxMultiPartInsertOp::process_row()
{
  int ret = OB_SUCCESS;
  bool is_filtered = false;
  OZ (ObDMLService::check_row_null(MY_SPEC.ins_ctdef_.new_row_,
                                   eval_ctx_,
                                   ins_rtdef_.cur_row_num_,
                                   MY_SPEC.ins_ctdef_.column_infos_,
                                   MY_SPEC.ins_ctdef_.das_ctdef_,
                                   MY_SPEC.ins_ctdef_.is_single_value_,
                                   *this));
  OZ(ObDMLService::filter_row_for_view_check(MY_SPEC.ins_ctdef_.view_check_exprs_, eval_ctx_, is_filtered));
  OV(!is_filtered, OB_ERR_CHECK_OPTION_VIOLATED);
  OZ(ObDMLService::filter_row_for_check_cst(MY_SPEC.ins_ctdef_.check_cst_exprs_, eval_ctx_, is_filtered));
  OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
  return ret;
}

//////////// pdml data interface implementation: reader & writer ////////////
int ObPxMultiPartInsertOp::read_row(ObExecContext &ctx,
                                    const ObExprPtrIArray *&row,
                                    common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  // 从child中读取数据，数据存储在child的output exprs中
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null", K(ret));
  } else if (OB_FAIL(child_->get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail get next row from child", K(ret));
    }
  } else {
    // 每一次从child节点获得新的数据都需要进行清除计算标记
    clear_evaluated_flag();
    // 通过partition id expr获得对应行对应的分区
    const int64_t part_id_idx = MY_SPEC.row_desc_.get_part_id_index();
    // 返回的值是child的output exprs
    row = &child_->get_spec().output_;
    if (NO_PARTITION_ID_FLAG == part_id_idx) {
      ObDASTableLoc *table_loc = ins_rtdef_.das_rtdef_.table_loc_;
      if (OB_ISNULL(table_loc) || table_loc->get_tablet_locs().size() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert table location is invalid", K(ret), KPC(table_loc));
      } else {
        tablet_id = table_loc->get_first_tablet_loc()->tablet_id_;
      }
    } else if (child_->get_spec().output_.count() > part_id_idx) {
      ObExpr *expr = child_->get_spec().output_.at(part_id_idx);
      ObDatum &expr_datum = expr->locate_expr_datum(get_eval_ctx());
      tablet_id = expr_datum.get_int();
      LOG_DEBUG("get the part id", K(ret), K(expr_datum));
    }
  }
  if (!MY_SPEC.is_pdml_index_maintain_ && OB_SUCC(ret)) {
    if (OB_FAIL(process_row())) {
      LOG_WARN("fail process row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("read row from pdml cache", "read_row", ROWEXPR2STR(eval_ctx_, *row), K(tablet_id));
  }
  return ret;
}

int ObPxMultiPartInsertOp::write_rows(ObExecContext &ctx,
                                      const ObDASTabletLoc *tablet_loc,
                                      ObPDMLOpRowIterator &dml_row_iter)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObPhysicalPlanCtx *plan_ctx = NULL;
  // 向存储层写入数据
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObChunkDatumStore::StoredRow* stored_row = nullptr;
      clear_evaluated_flag();
      if (OB_FAIL(try_check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(dml_row_iter.get_next_row(child_->get_spec().output_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        } else {
          iter_end_ = true;
        }
      } else if (OB_FAIL(ObDMLService::insert_row(MY_SPEC.ins_ctdef_, ins_rtdef_, tablet_loc, dml_rtctx_, stored_row))) {
        LOG_WARN("insert row to das failed", K(ret));
      } else if (OB_FAIL(discharge_das_write_buffer())) {
        LOG_WARN("failed to submit all dml task when the buffer of das op is full", K(ret));
      }
    }

    if (OB_ITER_END == ret) {
      if (OB_FAIL(submit_all_dml_task())) {
        LOG_WARN("do insert rows post process failed", K(ret));
      }
    }
    if (!(MY_SPEC.is_pdml_index_maintain_)) {
      plan_ctx->add_row_matched_count(ins_rtdef_.das_rtdef_.affected_rows_);
      plan_ctx->add_affected_rows(ins_rtdef_.das_rtdef_.affected_rows_);
    }
    LOG_TRACE("pdml insert ok", K(MY_SPEC.is_pdml_index_maintain_),
              K(ins_rtdef_.das_rtdef_.affected_rows_));
    ins_rtdef_.das_rtdef_.affected_rows_ = 0;
  }
  return ret;
}
