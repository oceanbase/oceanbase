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
#include "storage/access/ob_dml_param.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/dml/ob_dml_service.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::common::serialization;

OB_SERIALIZE_MEMBER((ObPxMultiPartDeleteOpInput, ObPxMultiPartModifyOpInput));

OB_SERIALIZE_MEMBER((ObPxMultiPartDeleteSpec, ObTableModifySpec),
                    row_desc_,
                    del_ctdef_,
                    with_barrier_);

//////////////////////ObPxMultiPartDeleteSpec///////////////////
int ObPxMultiPartDeleteSpec::register_to_datahub(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  if (with_barrier_) {
    if (OB_ISNULL(ctx.get_sqc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else {
      void *buf = ctx.get_allocator().alloc(sizeof(ObBarrierWholeMsg::WholeMsgProvider));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObBarrierWholeMsg::WholeMsgProvider *provider =
          new (buf)ObBarrierWholeMsg::WholeMsgProvider();
        ObSqcCtx &sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
        if (OB_FAIL(sqc_ctx.add_whole_msg_provider(get_id(), dtl::DH_BARRIER_WHOLE_MSG, *provider))) {
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
  } else if (OB_FAIL(ObDMLService::init_del_rtdef(dml_rtctx_, del_rtdef_, MY_SPEC.del_ctdef_))) {
    LOG_WARN("init delete rtdef failed", K(ret));
  } else if (OB_FAIL(data_driver_.init(get_spec(), ctx_.get_allocator(), del_rtdef_, this, this, false, MY_SPEC.with_barrier_))) {
    LOG_WARN("failed to init data driver", K(ret));
  } else if (MY_SPEC.with_barrier_) {
    if (OB_ISNULL(input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the input is null", K(ret));
    } else if (OB_FAIL(data_driver_.set_dh_barrier_param(
                MY_SPEC.get_id(),
                static_cast<const ObPxMultiPartModifyOpInput *>(input_)))) {
      LOG_WARN("faile to set barrier", K(ret));
    }
  }
  LOG_TRACE("pdml static delete op", K(ret), K_(MY_SPEC.row_desc), K(MY_SPEC.del_ctdef_));
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
        LOG_DEBUG("get one row for delete loop",
          "row", ROWEXPR2STR(get_eval_ctx(), child_->get_spec().output_));
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
  }
  return ret;
}

//////////// pdml data interface implementation: reader & writer ////////////
int ObPxMultiPartDeleteOp::read_row(ObExecContext &ctx,
                                    const ObExprPtrIArray *&row,
                                    ObTabletID &tablet_id,
                                    bool &is_skipped)
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
    if (OB_FAIL(ObDMLService::process_delete_row(MY_SPEC.del_ctdef_, del_rtdef_, is_skipped, *this))) {
      LOG_WARN("process delete row failed", K(ret));
    } else if (!is_skipped) {
      // 通过partition id expr获得对应行对应的分区
      const int64_t part_id_idx = MY_SPEC.row_desc_.get_part_id_index();
      // 返回的值是child的output exprs
      row = &child_->get_spec().output_;
      if (NO_PARTITION_ID_FLAG == part_id_idx) {
        // 如果row中没有partition id expr对应的cell，默认partition id为0
        ObDASTableLoc *table_loc = del_rtdef_.das_rtdef_.table_loc_;
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
  }
  return ret;
}



int ObPxMultiPartDeleteOp::write_rows(ObExecContext &ctx,
                                      const ObDASTabletLoc *tablet_loc,
                                      ObPDMLOpRowIterator &dml_row_iter)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObPhysicalPlanCtx *plan_ctx = NULL;
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      ObChunkDatumStore::StoredRow* stored_row = nullptr;
      if (OB_FAIL(try_check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(dml_row_iter.get_next_row(child_->get_spec().output_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        } else {
          iter_end_ = true;
        }
      } else if (OB_FAIL(ObDMLService::delete_row(MY_SPEC.del_ctdef_, del_rtdef_, tablet_loc, dml_rtctx_, stored_row))) {
        LOG_WARN("delete row to das failed", K(ret));
      } else if (OB_FAIL(discharge_das_write_buffer())) {
        LOG_WARN("failed to submit all dml task when the buffer of das op is full", K(ret));
      }
    }

    if (OB_ITER_END == ret) {
      if (OB_FAIL(submit_all_dml_task())) {
        LOG_WARN("do delete rows post process failed", K(ret));
      }
    }
    if (!(MY_SPEC.is_pdml_index_maintain_) && !(MY_SPEC.is_pdml_update_split_)) {
      plan_ctx->add_affected_rows(del_rtdef_.das_rtdef_.affected_rows_);
      plan_ctx->add_row_deleted_count(del_rtdef_.das_rtdef_.affected_rows_);
    }

    if (MY_SPEC.is_pdml_update_split_) {
      plan_ctx->add_row_duplicated_count(del_rtdef_.das_rtdef_.affected_rows_);
    }
    
    LOG_TRACE("pdml delete ok", K(MY_SPEC.is_pdml_index_maintain_),
              K(del_rtdef_.das_rtdef_.affected_rows_));
    del_rtdef_.das_rtdef_.affected_rows_ = 0;
  }
  return ret;
}
