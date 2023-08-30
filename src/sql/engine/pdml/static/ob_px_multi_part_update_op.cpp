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
#include "storage/access/ob_dml_param.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/engine/dml/ob_dml_service.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::common::serialization;

OB_SERIALIZE_MEMBER((ObPxMultiPartUpdateOpInput, ObPxMultiPartModifyOpInput));

OB_SERIALIZE_MEMBER((ObPxMultiPartUpdateSpec, ObTableModifySpec),
                    row_desc_,
                    upd_ctdef_);

//////////////////////ObPxMultiPartInsertOp///////////////////
int ObPxMultiPartUpdateOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("failed to inner open", K(ret));
  } else if (!(MY_SPEC.row_desc_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table or row desc is invalid", K(ret), K(MY_SPEC.row_desc_));
  } else if (OB_FAIL(data_driver_.init(get_spec(), ctx_.get_allocator(), upd_rtdef_, this, this,
                                       nullptr, false))) {
    LOG_WARN("failed to init data driver", K(ret));
  } else if (OB_FAIL(ObDMLService::init_upd_rtdef(dml_rtctx_,
                                                  upd_rtdef_,
                                                  MY_SPEC.upd_ctdef_,
                                                  trigger_clear_exprs_,
                                                  fk_checkers_))) {
    LOG_WARN("init update rtdef failed", K(ret));
  }
  LOG_TRACE("pdml static update op", K(ret), K_(MY_SPEC.row_desc));
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
        LOG_DEBUG("get one row for update loop",
          "row", ROWEXPR2STR(get_eval_ctx(), child_->get_spec().output_));
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

int ObPxMultiPartUpdateOp::update_row_to_das(const ObDASTabletLoc *tablet_loc)
{
  int ret = OB_SUCCESS;
  bool is_skipped = false;
  ObChunkDatumStore::StoredRow* stored_row = nullptr;
  ++upd_rtdef_.cur_row_num_;
  if (OB_FAIL(ObDMLService::process_update_row(MY_SPEC.upd_ctdef_, upd_rtdef_, is_skipped, *this))) {
    LOG_WARN("process update row failed", K(ret));
  } else if (is_skipped) {
    //do nothing
  } else if (OB_FAIL(ObDMLService::update_row(MY_SPEC.upd_ctdef_, upd_rtdef_, tablet_loc, tablet_loc, dml_rtctx_, stored_row, stored_row, stored_row))) {
    LOG_WARN("insert row with das failed", K(ret));
  } else {
    ++upd_rtdef_.found_rows_;
  }
  return ret;
}

//////////// pdml data interface implementation: reader & writer ////////////
int ObPxMultiPartUpdateOp::read_row(ObExecContext &ctx,
                                    const ObExprPtrIArray *&row,
                                    common::ObTabletID &tablet_id)
{
  // 从child中读取数据，数据存储在child的output exprs中
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  bool is_update_timestamp = false;
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
    // 每一次从child节点获得新的数据都需要进行清除计算标记
    clear_evaluated_flag();
    // 通过partition id expr获得对应行对应的分区
    const int64_t part_id_idx = MY_SPEC.row_desc_.get_part_id_index();
    // 返回的值是child的output exprs
    row = &child_->get_spec().output_;
    if (NO_PARTITION_ID_FLAG == part_id_idx) {
      ObDASTableLoc *table_loc = upd_rtdef_.dupd_rtdef_.table_loc_;
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

    // 做auto_failed_timestamp的检查
    // 如果assign_column包含了ON UPDATE CURRENT_TIMESTAMP类型的时间戳列
    // 第一步先检查主表是否发生update，如果主表当前行没有发生update，那么吧old_row中的当前列拷贝到new_row中
    // pdml的索引表在检查当前行是否发生update时，不会跳过ON UPDATE CURRENT_TIMESTAMP列，会比较value
    if (MY_SPEC.upd_ctdef_.is_primary_index_
        && lib::is_mysql_mode()
        && is_update_auto_filled_timestamp()) {
      // update the timestamp column, then check whether row is updated
      if (OB_FAIL(ObDMLService::check_row_whether_changed(MY_SPEC.upd_ctdef_, upd_rtdef_, get_eval_ctx()))) {
        LOG_WARN("fail to check_row_whether_changed", K(ret));
      } else if (!upd_rtdef_.is_row_changed_) {
        // copy old_row timestamp column as new_row timestamp column
        const ObExprPtrIArray &old_row = MY_SPEC.upd_ctdef_.old_row_;
        const ObExprPtrIArray &new_row = MY_SPEC.upd_ctdef_.new_row_;
        FOREACH_CNT_X(info, MY_SPEC.upd_ctdef_.assign_columns_, OB_SUCC(ret)) {
          const uint64_t idx = info->projector_index_;
          if (info->auto_filled_timestamp_) {
            ObDatum *old_datum = NULL;
            ObDatum *new_datum = NULL;
            if (OB_FAIL(old_row.at(idx)->eval(get_eval_ctx(), old_datum))
                || OB_FAIL(new_row.at(idx)->eval(get_eval_ctx(), new_datum))) {
              LOG_WARN("evaluate value failed", K(ret));
            } else if (OB_ISNULL(old_datum) || OB_ISNULL(new_datum)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("datum is null, unexpected", K(ret), KPC(old_datum), KPC(new_datum));
            } else {
              new_row.at(idx)->locate_datum_for_write(get_eval_ctx()) = *old_datum;
              new_row.at(idx)->set_evaluated_projected(get_eval_ctx());
            }
            LOG_TRACE("after copy timestamp column to new_row", K(ret), K(idx),
                      "old_row", ROWEXPR2STR(get_eval_ctx(), old_row),
                      "new_row", ROWEXPR2STR(get_eval_ctx(), new_row));
          }
        }
      }
    }

  }
  return ret;
}

bool ObPxMultiPartUpdateOp::is_update_auto_filled_timestamp()
{
  bool updated = false;
  FOREACH_CNT_X(info, MY_SPEC.upd_ctdef_.assign_columns_, !updated) {
    const uint64_t idx = info->projector_index_;
    if (info->auto_filled_timestamp_) {
      updated = true;
    }
  }
  return updated;
}


int ObPxMultiPartUpdateOp::write_rows(ObExecContext &ctx,
                                      const ObDASTabletLoc *tablet_loc,
                                      ObPDMLOpRowIterator &dml_row_iter)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;

  ObSQLSessionInfo *session = ctx_.get_my_session();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed");
  } else {
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      if (OB_FAIL(try_check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (OB_FAIL(dml_row_iter.get_next_row(child_->get_spec().output_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        } else {
          iter_end_ = true;
        }
      } else if (OB_FAIL(update_row_to_das(tablet_loc))) {
        LOG_WARN("update row to das failed", K(ret));
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
      int64_t found_rows = upd_rtdef_.found_rows_;
      int64_t changed_rows = upd_rtdef_.dupd_rtdef_.affected_rows_;
      plan_ctx->add_row_matched_count(found_rows);
      plan_ctx->add_row_duplicated_count(changed_rows);
      plan_ctx->add_affected_rows(session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS ?
                                  found_rows : changed_rows);
    }
    upd_rtdef_.found_rows_ = 0;
    upd_rtdef_.dupd_rtdef_.affected_rows_ = 0;
  }
  return ret;
}
