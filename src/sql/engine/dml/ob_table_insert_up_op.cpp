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

#include "ob_table_insert_up_op.h"
#include "sql/engine/ob_operator_reg.h"
#include "sql/engine/expr/ob_expr_autoinc_nextval.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/das/ob_das_insert_op.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/dml/ob_trigger_handler.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/dml/ob_fk_checker.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
OB_SERIALIZE_MEMBER((ObTableInsertUpOpInput, ObTableModifyOpInput));

OB_DEF_SERIALIZE(ObTableInsertUpSpec)
{
  int ret = OB_SUCCESS;
  int64_t index_cnt = insert_up_ctdefs_.count();
  BASE_SER((ObTableInsertUpSpec, ObTableModifySpec));
  OB_UNIS_ENCODE(index_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; ++i) {
    ObInsertUpCtDef *insert_up_ctdef = insert_up_ctdefs_.at(i);
    if (OB_ISNULL(insert_up_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert_up_ctdef is nullptr", K(ret));
    }
    OB_UNIS_ENCODE(*insert_up_ctdef);
  }
  OB_UNIS_ENCODE(conflict_checker_ctdef_);
  OB_UNIS_ENCODE(all_saved_exprs_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableInsertUpSpec)
{
  int ret = OB_SUCCESS;
  int64_t index_cnt = 0;
  BASE_DESER((ObTableInsertUpSpec, ObTableModifySpec));
  OB_UNIS_DECODE(index_cnt);
  OZ(insert_up_ctdefs_.allocate_array(alloc_, index_cnt));
  ObDMLCtDefAllocator<ObInsertUpCtDef> insert_up_ctdef_allocator(alloc_);
  for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; ++i) {
    insert_up_ctdefs_.at(i) = insert_up_ctdef_allocator.alloc();
    if (OB_ISNULL(insert_up_ctdefs_.at(i))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc insert_up_ctdef failed", K(ret));
    }
    OB_UNIS_DECODE(*insert_up_ctdefs_.at(i));
  }
  OB_UNIS_DECODE(conflict_checker_ctdef_);
  OB_UNIS_DECODE(all_saved_exprs_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableInsertUpSpec)
{
  int64_t len = 0;
  int64_t index_cnt = insert_up_ctdefs_.count();
  BASE_ADD_LEN((ObTableInsertUpSpec, ObTableModifySpec));
  OB_UNIS_ADD_LEN(index_cnt);
  for (int64_t i = 0; i < index_cnt; ++i) {
    ObInsertUpCtDef *insert_up_ctdef = insert_up_ctdefs_.at(i);
    if (insert_up_ctdef != nullptr) {
      OB_UNIS_ADD_LEN(*insert_up_ctdef);
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "insert_up_ctdef is null, unexpected");
    }
  }
  OB_UNIS_ADD_LEN(conflict_checker_ctdef_);
  OB_UNIS_ADD_LEN(all_saved_exprs_);
  return len;
}

int ObTableInsertUpOp::check_need_exec_single_row()
{
  int ret = OB_SUCCESS;
  ret = ObTableModifyOp::check_need_exec_single_row();
  if (OB_SUCC(ret) && !execute_single_row_) {
    ObInsertUpCtDef *insert_up_ctdef = MY_SPEC.insert_up_ctdefs_.at(0);
    const ObInsCtDef *ins_ctdef = insert_up_ctdef->ins_ctdef_;
    const ObUpdCtDef *upd_ctdef = insert_up_ctdef->upd_ctdef_;
    if (OB_NOT_NULL(ins_ctdef) && OB_NOT_NULL(upd_ctdef)) {
      for (int64_t j = 0;
          OB_SUCC(ret) && !execute_single_row_ && j < ins_ctdef->trig_ctdef_.tg_args_.count();
          ++j) {
        const ObTriggerArg &tri_arg = ins_ctdef->trig_ctdef_.tg_args_.at(j);
        execute_single_row_ = tri_arg.is_execute_single_row();
      }
      for (int64_t j = 0;
          OB_SUCC(ret) && !execute_single_row_ && j < upd_ctdef->trig_ctdef_.tg_args_.count();
          ++j) {
        const ObTriggerArg &tri_arg = upd_ctdef->trig_ctdef_.tg_args_.at(j);
        execute_single_row_ = tri_arg.is_execute_single_row();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ins_ctdef or upd_ctdef of primary table is nullptr", K(ret));
    }
  }
  return ret;
}

int ObTableInsertUpOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
  NG_TRACE(replace_open);
  if (OB_FAIL(check_insert_up_ctdefs_valid())) {
    LOG_WARN("check insert up ctdefs valid failed", K(ret));
  } else if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("inner open ObTableModifyOp failed", K(ret));
  } else if (OB_UNLIKELY(MY_SPEC.insert_up_ctdefs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ins ctdef is invalid", K(ret), KP(this));
  } else if (OB_UNLIKELY(iter_end_)) {
    //do nothing
  } else if (OB_FAIL(insert_up_row_store_.init(UINT64_MAX,
                                               my_session->get_effective_tenant_id(),
                                               ObCtxIds::DEFAULT_CTX_ID,
                                               "insert_up_row_store",
                                               false/*enable_dump*/))) {
    LOG_WARN("fail to init replace row store", K(ret));
  } else if (OB_FAIL(inner_open_with_das())) {
    LOG_WARN("inner open with das failed", K(ret));
  } else {
    const ObInsertUpCtDef *insert_up_ctdef = MY_SPEC.insert_up_ctdefs_.at(0);
    const ObDASInsCtDef &das_ins_ctdef = insert_up_ctdef->ins_ctdef_->das_ctdef_;
    is_ignore_ = das_ins_ctdef.is_ignore_;
    conflict_checker_.set_local_tablet_loc(MY_INPUT.get_tablet_loc());
  }
  return ret;
}

int ObTableInsertUpOp::inner_open_with_das()
{
  int ret = OB_SUCCESS;
  const ObExprFrameInfo *expr_frame_info = NULL;
  ObDASTableLoc *table_loc = nullptr;
  expr_frame_info = nullptr != MY_SPEC.expr_frame_info_
                               ? MY_SPEC.expr_frame_info_
                               : &MY_SPEC.plan_->get_expr_frame_info();
  if (OB_FAIL(init_insert_up_rtdef())) {
    LOG_WARN("init insert_up rtdef failed", K(ret), K(MY_SPEC.insert_up_ctdefs_.count()));
  } else if (OB_ISNULL(table_loc = insert_up_rtdefs_.at(0).ins_rtdef_.das_rtdef_.table_loc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table location is nullptr", K(ret));
  } else if (OB_FAIL(conflict_checker_.init_conflict_checker(expr_frame_info, table_loc))) {
    LOG_WARN("init conflict_checker fail", K(ret));
  } else {
     // init update das_ref
     ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
     ObMemAttr mem_attr;
     mem_attr.tenant_id_ = session->get_effective_tenant_id();
     mem_attr.label_ = "SqlInsUpUpd";
     upd_rtctx_.das_ref_.set_expr_frame_info(expr_frame_info);
     upd_rtctx_.das_ref_.set_mem_attr(mem_attr);
     upd_rtctx_.das_ref_.set_execute_directly(!MY_SPEC.use_dist_das_);
  }
  return ret;
}

OB_INLINE int ObTableInsertUpOp::init_insert_up_rtdef()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(insert_up_rtdefs_.allocate_array(ctx_.get_allocator(), MY_SPEC.insert_up_ctdefs_.count()))) {
    LOG_WARN("allocate insert up rtdef failed", K(ret), K(MY_SPEC.insert_up_ctdefs_.count()));
  }
  trigger_clear_exprs_.reset();
  fk_checkers_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_up_rtdefs_.count(); ++i) {
    ObInsertUpCtDef *insert_up_ctdef = MY_SPEC.insert_up_ctdefs_.at(i);
    const ObInsCtDef *ins_ctdef = insert_up_ctdef->ins_ctdef_;
    const ObUpdCtDef *upd_ctdef = insert_up_ctdef->upd_ctdef_;
    ObInsRtDef &ins_rtdef = insert_up_rtdefs_.at(i).ins_rtdef_;
    ObUpdRtDef &upd_rtdef = insert_up_rtdefs_.at(i).upd_rtdef_;
    if (OB_FAIL(ObDMLService::init_ins_rtdef(dml_rtctx_,
                                             ins_rtdef,
                                             *ins_ctdef,
                                             trigger_clear_exprs_,
                                             fk_checkers_))) {
      LOG_WARN("init insert rt_def failed", K(ret), KPC(ins_ctdef));
    } else if (OB_ISNULL(upd_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upd_ctdef is null", K(ret));
    } else if (OB_FAIL(ObDMLService::init_upd_rtdef(dml_rtctx_,
                                                    upd_rtdef,
                                                    *upd_ctdef,
                                                    trigger_clear_exprs_,
                                                    fk_checkers_))) {
      LOG_WARN("init upd_rt_def failed", K(ret), KPC(upd_ctdef));
    } else {
      ins_rtdef.das_rtdef_.table_loc_->is_writing_ = true;
    }
  }
  return ret;
}

int ObTableInsertUpOp::inner_close()
{
  int ret = OB_SUCCESS;
  int close_ret = OB_SUCCESS;
  NG_TRACE(replace_inner_close);
  if (OB_FAIL(conflict_checker_.close())) {
    LOG_WARN("fail to close conflict_checker", K(ret));
  }

  if (upd_rtctx_.das_ref_.has_task()) {
    close_ret = (upd_rtctx_.das_ref_.close_all_task());
    if (OB_SUCCESS == close_ret) {
      upd_rtctx_.das_ref_.reset();
    }
  }
  ret = OB_SUCCESS == ret ? close_ret : ret;
  // close dml das tasks
  close_ret = ObTableModifyOp::inner_close();
  return (OB_SUCCESS == ret) ? close_ret : ret;
}

int ObTableInsertUpOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_rescan())) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else {
    conflict_checker_.set_local_tablet_loc(MY_INPUT.get_tablet_loc());
    if (upd_rtctx_.das_ref_.has_task()) {
      if (OB_FAIL(upd_rtctx_.das_ref_.close_all_task())) {
        LOG_WARN("close all update das task failed", K(ret));
      } else {
        upd_rtctx_.reuse();
      }
    }
  }

  if (OB_SUCC(ret)) {
    insert_up_rtdefs_.release_array();
    if (OB_UNLIKELY(iter_end_)) {
      //do nothing
    } else if (OB_FAIL(init_insert_up_rtdef())) {
      LOG_WARN("init insert up rtdef failed", K(ret));
    } else if (OB_FAIL(reuse())) {
      LOG_WARN("reuse op fail", K(ret));
    }
  }
  return ret;
}

int ObTableInsertUpOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
  if (iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(MY_SPEC.id_), K(iter_end_));
    ret = OB_ITER_END;
  } else {
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("check status failed", K(ret));
    } else if (OB_ISNULL(my_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else if (OB_FAIL(do_insert_up())) {
      LOG_WARN("fail to do insert_up", K(ret));
    } else {
      if (OB_FAIL(plan_ctx->sync_last_value_local())) {
        LOG_WARN("failed to sync value globally", K(ret));
      }
      ObUpdRtDef &upd_rtdef = insert_up_rtdefs_.at(0).upd_rtdef_;
      plan_ctx->set_affected_rows(my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS ?
                                    insert_rows_ + upd_rtdef.found_rows_ :
                                    insert_rows_ + upd_changed_rows_);
      plan_ctx->set_row_duplicated_count(upd_changed_rows_);
      plan_ctx->add_row_matched_count(found_rows_);
    }
    int sync_ret = OB_SUCCESS;
    if (OB_SUCCESS != (sync_ret = plan_ctx->sync_last_value_global())) {
      // sync last user specified value after iter ends(compatible with MySQL)
      LOG_WARN("failed to sync last value", K(sync_ret));
    }
    if (OB_SUCC(ret)) {
      ret = OB_SUCCESS == sync_ret ? OB_ITER_END : sync_ret;
    }
  }
  return ret;
}

int ObTableInsertUpOp::set_heap_table_new_pk(const ObUpdCtDef &upd_ctdef,
                                             ObUpdRtDef &upd_rtdef)
{
  int ret = OB_SUCCESS;
  ObDASTabletLoc *old_tablet_loc = nullptr;
  ObDASTabletLoc *new_tablet_loc = nullptr;
  if (OB_FAIL(calc_update_tablet_loc(upd_ctdef, upd_rtdef, old_tablet_loc, new_tablet_loc))) {
    LOG_WARN("calc update tablet_loc failed", K(ret), K(upd_ctdef));
  } else if (old_tablet_loc != new_tablet_loc) {
    if (OB_FAIL(ObDMLService::set_update_hidden_pk(eval_ctx_,
                                                   upd_ctdef,
                                                   new_tablet_loc->tablet_id_))) {
      LOG_WARN("fail to set hidden_pk", K(ret));
    }
  }
  return ret;
}

// 构建冲突的hash map的时候也使用的是column_ref， 回表也是使用的column_ref expr来读取scan的结果
// 因为constarain_info中使用的column_ref expr，所以此处需要使用table_column_old_exprs (column_ref exprs)
int ObTableInsertUpOp::do_insert_up_cache()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObConflictValue, 1> constraint_values;
  ObChunkDatumStore::Iterator insert_row_iter;
  bool is_skipped = false;
  const ObChunkDatumStore::StoredRow *insert_row = NULL;
  const ObInsertUpCtDef &insert_up_ctdef = *(MY_SPEC.insert_up_ctdefs_.at(0));
  const ObInsCtDef &ins_ctdef = *(insert_up_ctdef.ins_ctdef_);
  const ObUpdCtDef &upd_ctdef = *(insert_up_ctdef.upd_ctdef_);
  ObInsertUpRtDef &insert_up_rtdef = insert_up_rtdefs_.at(0);
  ObInsRtDef &ins_rtdef = insert_up_rtdef.ins_rtdef_;
  ObUpdRtDef &upd_rtdef = insert_up_rtdef.upd_rtdef_;

  NG_TRACE_TIMES(2, insert_up_start_shuff);
  if (OB_FAIL(insert_up_row_store_.begin(insert_row_iter))) {
    LOG_WARN("fail to get insert_up_row_store begin iter", K(ret));
  }
  while (OB_SUCC(ret) && OB_SUCC(insert_row_iter.get_next_row(insert_row))) {
    constraint_values.reuse();
    if (OB_ISNULL(insert_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get insert row is null", K(ret));
    } else if (OB_FAIL(insert_row->to_expr(MY_SPEC.all_saved_exprs_, eval_ctx_))) {
      LOG_WARN("flush to expr failed", K(ret), KPC(insert_row));
    } else if (OB_FAIL(conflict_checker_.check_duplicate_rowkey(insert_row,
                                                                constraint_values,
                                                                true))) {
      LOG_WARN("check duplicated key failed", K(ret), KPC(insert_row));
    } else if (constraint_values.empty()) {
      // do insert
      ObChunkDatumStore::StoredRow *insert_new_row = NULL;
      ObDMLModifyRowNode modify_row(this, &ins_ctdef, &ins_rtdef, ObDmlEventType::DE_INSERTING);
      if (is_ignore_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ignore is unexpected", K(ret), KPC(insert_row));
      } else if (OB_FAIL(ObDMLService::process_insert_row(ins_ctdef, ins_rtdef, *this, is_skipped))) {
        LOG_WARN("fail to do process insert", K(ret), K(ins_ctdef),
                 "insert_row", ROWEXPR2STR(eval_ctx_, get_primary_table_insert_row()));
      // TODO(yikang): fix trigger related for heap table
      } else if (OB_FAIL(conflict_checker_.convert_exprs_to_stored_row(get_primary_table_insert_row(),
                                                                       insert_new_row))) {
        LOG_WARN("convert expr to stored row failed", K(ret),
                 "exprs", get_primary_table_insert_row());
      } else if (OB_FAIL(conflict_checker_.insert_new_row(insert_new_row, ObNewRowSource::FROM_INSERT))) {
        LOG_WARN("fail to insert row", K(ret),
                 "insert_row", ROWEXPR2STR(eval_ctx_, get_primary_table_insert_row()));
      } else {
        modify_row.new_row_ = insert_new_row;
        if (need_after_row_process(ins_ctdef) && OB_FAIL(dml_modify_rows_.push_back(modify_row))) {
          LOG_WARN("failed to push dml modify row to modified row list", K(ret));
        } else {
          insert_rows_++;
        }
      }
    } else {
      // do update
      // 对于update, update c1 = values(c1) + 1, 此时new_row中替换的column
      // 依赖于insert_row_中的值, 并且需要重新计算, 所以这里需要
      // clear_evaluated_flag, 并且将数据重新flush到insert_row中
      ObChunkDatumStore::StoredRow *upd_new_row = NULL;
      const ObChunkDatumStore::StoredRow *upd_old_row = constraint_values.at(0).current_datum_row_;
      ObDMLModifyRowNode modify_row(this, &upd_ctdef, &upd_rtdef, ObDmlEventType::DE_UPDATING);
      clear_evaluated_flag();
      if (OB_FAIL(insert_row->to_expr(MY_SPEC.all_saved_exprs_, eval_ctx_))) {
        LOG_WARN("insert_row to expr failed", K(ret), KPC(insert_row),
                 "exprs", get_primary_table_insert_row());
      } else if (OB_FAIL(upd_old_row->to_expr(get_primary_table_upd_old_row(), eval_ctx_))) {
        LOG_WARN("upd_old_row to expr failed", K(ret), KPC(upd_old_row),
                 "exprs", get_primary_table_upd_old_row());
      } else if (ins_ctdef.is_primary_index_
                 && OB_FAIL(TriggerHandle::init_param_new_row(get_eval_ctx(),
                                                              ins_ctdef.trig_ctdef_,
                                                              ins_rtdef.trig_rtdef_))) {
        LOG_WARN("init_param_new_row failed", K(ret));
        // 冲突的行update这里也需要重新执行before insert row trigger
      } else if (ins_ctdef.is_primary_index_
                 && OB_FAIL(TriggerHandle::do_handle_before_row(*this,
                                                                ins_ctdef.das_base_ctdef_,
                                                                ins_ctdef.trig_ctdef_,
                                                                ins_rtdef.trig_rtdef_))) {
        LOG_WARN("do_handle_before_row failed", K(ret));
      } else if (OB_FAIL(ObDMLService::process_update_row(upd_ctdef, upd_rtdef, is_skipped, *this))) {
        LOG_WARN("process update failed", K(ret), K(upd_ctdef));
      } else if (upd_ctdef.is_heap_table_ &&
          OB_FAIL(set_heap_table_new_pk(upd_ctdef, upd_rtdef))) {
        LOG_WARN("set heap table hidden_pk failed", K(ret), K(upd_ctdef));
      } else if (OB_FAIL(conflict_checker_.convert_exprs_to_stored_row(get_primary_table_upd_new_row(),
                                                                       upd_new_row))) {
        LOG_WARN("convert expr to stored row failed", K(ret), "exprs", get_primary_table_upd_old_row());
      } else if (OB_FAIL(calc_auto_increment(upd_ctdef))) {
        LOG_WARN("calc auto_inc failed", K(ret), K(upd_ctdef));
      } else if (FALSE_IT(upd_rtdef.found_rows_++)) {
        // do nothing
      } else if (is_ignore_) {
        if (OB_FAIL(do_update_with_ignore())) {
          LOG_WARN("do update with ignore failed", K(ret));
        } else if (upd_rtdef.is_row_changed_) {
          insert_rows_++;
          upd_changed_rows_++;
        }
      } else if (upd_rtdef.is_row_changed_) {
        if (OB_FAIL(conflict_checker_.update_row(upd_new_row, upd_old_row))) {
          LOG_WARN("fail to update row in conflict_checker", K(ret),
                   KPC(upd_new_row), KPC(upd_old_row));
        } else {
          modify_row.old_row_ = const_cast<ObChunkDatumStore::StoredRow *>(upd_old_row);
          modify_row.new_row_ = upd_new_row;
          if (need_after_row_process(upd_ctdef) && OB_FAIL(dml_modify_rows_.push_back(modify_row))) {
            LOG_WARN("failed to push dml modify row to modified row list", K(ret));
          } else {
            insert_rows_++;
            upd_changed_rows_++;
          }
        }
      } else {
        // create table t1(c1 int primary key, c2 timestamp default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP);
        // insert into t1(c1) values(1);
        // insert into t1(c1) values(1) on duplicate key update c1=1;
        // 当出现冲突时，做update，但是c2列不应该被更新
        if (OB_FAIL(conflict_checker_.lock_row(upd_old_row))) {
          LOG_WARN("checker lock row failed", K(ret), KPC(upd_old_row));
        } else {
          LOG_TRACE("curr update row is not changed", KPC(upd_new_row), KPC(upd_old_row));
        }
      }
    }
  } // while row store end
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  return ret;
}

int ObTableInsertUpOp::insert_row_to_das(const ObInsCtDef &ins_ctdef,
                                         ObInsRtDef &ins_rtdef,
                                         const ObDASTabletLoc *tablet_loc)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow* stored_row = nullptr;
  if (OB_FAIL(ObDMLService::insert_row(ins_ctdef, ins_rtdef, tablet_loc, dml_rtctx_, stored_row))) {
    LOG_WARN("insert row with das failed", K(ret));
  } else {
    LOG_TRACE("insert one row", KPC(tablet_loc),
              "insert row", ROWEXPR2STR(eval_ctx_, ins_ctdef.new_row_));
  }

  return ret;
}

int ObTableInsertUpOp::insert_row_to_das(const ObInsCtDef &ins_ctdef,
                                         ObInsRtDef &ins_rtdef,
                                         const ObDASTabletLoc *tablet_loc,
                                         ObDMLModifyRowNode &modify_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLService::insert_row(ins_ctdef, ins_rtdef, tablet_loc, dml_rtctx_, modify_row.new_row_))) {
    LOG_WARN("insert row with das failed", K(ret));
  } else {
    LOG_TRACE("insert one row", KPC(tablet_loc),
              "insert row", ROWEXPR2STR(eval_ctx_, ins_ctdef.new_row_));
  }

  return ret;
}

int ObTableInsertUpOp::try_insert_row()
{
  int ret = OB_SUCCESS;
  bool is_skipped = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.insert_up_ctdefs_.count(); ++i) {
    // first time: insert each table with fetched row
    // second time: after do conflict checker, insert row without duplicate key
    const ObInsertUpCtDef &insert_up_ctdef = *(MY_SPEC.insert_up_ctdefs_.at(i));
    const ObInsCtDef &ins_ctdef = *(insert_up_ctdef.ins_ctdef_);
    ObInsertUpRtDef &insert_up_rtdef = insert_up_rtdefs_.at(i);
    ObInsRtDef &ins_rtdef = insert_up_rtdef.ins_rtdef_;
    ObDASTabletLoc *tablet_loc = nullptr;
    ObDMLModifyRowNode modify_row(this, const_cast<ObInsCtDef *>(&ins_ctdef), &ins_rtdef, ObDmlEventType::DE_INSERTING);
    ++ins_rtdef.cur_row_num_;
    if (OB_FAIL(ObDMLService::process_insert_row(ins_ctdef, ins_rtdef, *this, is_skipped))) {
      LOG_WARN("process insert row failed", K(ret));
    } else if (OB_UNLIKELY(is_skipped)) {
      break;
    } else if (OB_FAIL(calc_insert_tablet_loc(ins_ctdef, ins_rtdef, tablet_loc))) {
      LOG_WARN("calc insert partition key failed", K(ret));
    } else if (ins_ctdef.is_heap_table_ &&
        OB_FAIL(ObDMLService::set_heap_table_hidden_pk(ins_ctdef, tablet_loc->tablet_id_, eval_ctx_))) {
      LOG_WARN("set_heap_table_hidden_pk failed", K(ret), KPC(tablet_loc));
    } else if (OB_FAIL(insert_row_to_das(ins_ctdef, ins_rtdef, tablet_loc, modify_row))) {
      LOG_WARN("insert row with das failed", K(ret));
    // TODO(yikang): fix trigger related for heap table
    } else if (need_after_row_process(ins_ctdef) && OB_FAIL(dml_modify_rows_.push_back(modify_row))) {
      LOG_WARN("failed to push dml modify row to modified row list", K(ret));
    }
  }
  return ret;
}

int ObTableInsertUpOp::lock_one_row_to_das(const ObUpdCtDef &upd_ctdef,
                                           ObUpdRtDef &upd_rtdef,
                                           const ObDASTabletLoc *tablet_loc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upd_ctdef.dlock_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddel_ctdef can't be null", K(ret));
  } else if (OB_ISNULL(upd_rtdef.dlock_rtdef_)) {
    ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
    ObIAllocator &allocator = ctx_.get_allocator();
    uint64_t tenant_id = my_session->get_effective_tenant_id();
    void *buffer = nullptr;
    if (OB_FAIL(ObDASTaskFactory::alloc_das_rtdef(DAS_OP_TABLE_LOCK,
                                                  allocator,
                                                  upd_rtdef.dlock_rtdef_))) {
      LOG_WARN("create das lock rtdef failed", K(ret));
    } else if (OB_FAIL(ObDMLService::init_das_dml_rtdef(dml_rtctx_,
                                                        *upd_ctdef.dlock_ctdef_,
                                                        *upd_rtdef.dlock_rtdef_,
                                                        nullptr))) {
      LOG_WARN("init das dml rtdef failed", K(ret), K(upd_ctdef), K(upd_rtdef));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(upd_rtdef.dlock_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dlock_rtdef_ is null", K(ret));
  } else if (OB_FAIL(ObDMLService::check_dml_tablet_validity(dml_rtctx_,
                                                             *tablet_loc,
                                                             upd_ctdef.old_row_,
                                                             upd_ctdef,
                                                             upd_rtdef))) {
    LOG_WARN("check dml tablet validity failed", K(ret));
  } else if (OB_FAIL(ObDMLService::lock_row(*upd_ctdef.dlock_ctdef_,
                                            *upd_rtdef.dlock_rtdef_,
                                            tablet_loc,
                                            upd_rtctx_,
                                            upd_ctdef.old_row_))) {
    LOG_WARN("delete row with das failed", K(ret));
  } else {
    LOG_DEBUG("lock one_row", KPC(tablet_loc), "upd lock_row",
              ROWEXPR2STR(eval_ctx_, upd_ctdef.old_row_));
  }

  return ret;
}

int ObTableInsertUpOp::insert_upd_new_row_to_das()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.insert_up_ctdefs_.count(); ++i) {
    const ObInsertUpCtDef &insert_up_ctdef = *(MY_SPEC.insert_up_ctdefs_.at(i));
    const ObUpdCtDef *upd_ctdef = insert_up_ctdef.upd_ctdef_;
    ObInsertUpRtDef &insert_up_rtdef = insert_up_rtdefs_.at(i);
    ObUpdRtDef &upd_rtdef = insert_up_rtdef.upd_rtdef_;
    ObDASTabletLoc *tablet_loc = nullptr;
    // must do update
    // do insert update new_row
    OZ(calc_upd_new_row_tablet_loc(*upd_ctdef, upd_rtdef, tablet_loc));
    OZ(insert_one_upd_new_row_das(*upd_ctdef, upd_rtdef, tablet_loc));
  }
  return ret;
}

int ObTableInsertUpOp::insert_row_to_das()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.insert_up_ctdefs_.count(); ++i) {
    const ObInsertUpCtDef &insert_up_ctdef = *(MY_SPEC.insert_up_ctdefs_.at(i));
    const ObUpdCtDef *upd_ctdef = insert_up_ctdef.upd_ctdef_;
    const ObInsCtDef *ins_ctdef = insert_up_ctdef.ins_ctdef_;
    ObUpdRtDef &upd_rtdef = insert_up_rtdefs_.at(i).upd_rtdef_;
    ObInsRtDef &ins_rtdef = insert_up_rtdefs_.at(i).ins_rtdef_;
    ObDASTabletLoc *tablet_loc = nullptr;
    // do insert
    OZ(calc_insert_tablet_loc(*ins_ctdef, ins_rtdef, tablet_loc));
    OZ(insert_row_to_das(*ins_ctdef, ins_rtdef, tablet_loc));
  }
  return ret;
}

int ObTableInsertUpOp::delete_upd_old_row_to_das()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.insert_up_ctdefs_.count(); ++i) {
    const ObInsertUpCtDef &insert_up_ctdef = *(MY_SPEC.insert_up_ctdefs_.at(i));
    const ObUpdCtDef *upd_ctdef = insert_up_ctdef.upd_ctdef_;
    ObInsertUpRtDef &insert_up_rtdef = insert_up_rtdefs_.at(i);
    ObUpdRtDef &upd_rtdef = insert_up_rtdef.upd_rtdef_;
    ObDASTabletLoc *tablet_loc = nullptr;
    // must do update
    OZ(calc_upd_old_row_tablet_loc(*upd_ctdef, upd_rtdef, tablet_loc));
    // do delete
    OZ(delete_one_upd_old_row_das(*upd_ctdef, upd_rtdef, tablet_loc));
  }
  return ret;
}

int ObTableInsertUpOp::delete_one_upd_old_row_das(const ObUpdCtDef &upd_ctdef,
                                                  ObUpdRtDef &upd_rtdef,
                                                  const ObDASTabletLoc *tablet_loc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upd_ctdef.ddel_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddel_ctdef can't be null", K(ret));
  } else if (OB_ISNULL(upd_rtdef.ddel_rtdef_)) {
    ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
    ObIAllocator &allocator = ctx_.get_allocator();
    uint64_t tenant_id = my_session->get_effective_tenant_id();
    if (OB_FAIL(ObDMLService::init_das_del_rtdef_for_update(dml_rtctx_, upd_ctdef, upd_rtdef))) {
      LOG_WARN("init das dml rtdef failed", K(ret), K(upd_ctdef), K(upd_rtdef));
    }
  }
  ObChunkDatumStore::StoredRow* stored_row = nullptr;
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_ISNULL(upd_rtdef.ddel_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddel_rtdef is null", K(ret));
  } else if (OB_FAIL(ObDMLService::check_dml_tablet_validity(dml_rtctx_,
                                                             *tablet_loc,
                                                             upd_ctdef.old_row_,
                                                             upd_ctdef,
                                                             upd_rtdef))) {
    LOG_WARN("check dml tablet validity failed", K(ret));
  } else if (OB_FAIL(ObDMLService::delete_row(*upd_ctdef.ddel_ctdef_,
                                              *upd_rtdef.ddel_rtdef_,
                                              tablet_loc,
                                              upd_rtctx_,
                                              upd_ctdef.old_row_,
                                              stored_row))) {
    LOG_WARN("delete row with das failed", K(ret));
  } else {
    LOG_DEBUG("delete upd old_row", KPC(tablet_loc), "upd old_row",
              ROWEXPR2STR(eval_ctx_, upd_ctdef.old_row_));
  }
  return ret;
}

int ObTableInsertUpOp::insert_one_upd_new_row_das(const ObUpdCtDef &upd_ctdef,
                                                  ObUpdRtDef &upd_rtdef,
                                                  const ObDASTabletLoc *tablet_loc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upd_ctdef.dins_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dins_ctdef_ can't be null", K(ret));
  } else if (OB_ISNULL(upd_rtdef.dins_rtdef_)) {
    ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
    ObIAllocator &allocator = ctx_.get_allocator();
    uint64_t tenant_id = my_session->get_effective_tenant_id();
    if (OB_FAIL(ObDMLService::init_das_ins_rtdef_for_update(dml_rtctx_, upd_ctdef, upd_rtdef))) {
      LOG_WARN("init das dml rtdef failed", K(ret), K(upd_ctdef), K(upd_rtdef));
    }
  }
  ObChunkDatumStore::StoredRow* stored_row = nullptr;
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_ISNULL(upd_rtdef.dins_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dins_rtdef is null", K(ret));
  } else if (OB_FAIL(ObDMLService::check_dml_tablet_validity(dml_rtctx_,
                                                             *tablet_loc,
                                                             upd_ctdef.new_row_,
                                                             upd_ctdef,
                                                             upd_rtdef))) {
    LOG_WARN("check insert row tablet validity failed", K(ret));
  } else if (OB_FAIL(ObDMLService::insert_row(*upd_ctdef.dins_ctdef_,
                                              *upd_rtdef.dins_rtdef_,
                                              tablet_loc,
                                              upd_rtctx_,
                                              upd_ctdef.new_row_,
                                              stored_row))) {
    LOG_WARN("insert row with das failed", K(ret));
  } else {
    LOG_DEBUG("ins upd new_row", KPC(tablet_loc), "upd new_row",
              ROWEXPR2STR(eval_ctx_, upd_ctdef.new_row_));
  }
  return ret;
}

int ObTableInsertUpOp::calc_update_multi_tablet_id(const ObUpdCtDef &upd_ctdef,
                                                   ObExpr &part_id_expr,
                                                   ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObObjectID partition_id = OB_INVALID_ID;
  ObDatum *partition_id_datum = NULL;
  if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(&part_id_expr, eval_ctx_, partition_id, tablet_id))) {
    LOG_WARN("calc part and tablet id by expr failed", K(ret));
  } else if (OB_FAIL(deal_hint_part_selection(partition_id))) {
    LOG_WARN("Partition not match", K(ret));
  }
  return ret;
}

int ObTableInsertUpOp::calc_upd_old_row_tablet_loc(const ObUpdCtDef &upd_ctdef,
                                                   ObUpdRtDef &upd_rtdef,
                                                   ObDASTabletLoc *&tablet_loc)
{
  // 只有跨分区的更新时才会有delete task
  int ret = OB_SUCCESS;
  if (MY_SPEC.use_dist_das_) {
    if (upd_ctdef.multi_ctdef_ != nullptr) {
      ObExpr *calc_part_id_old = upd_ctdef.multi_ctdef_->calc_part_id_old_;
      if (calc_part_id_old != nullptr) {
        ObTabletID tablet_id;
        ObDASTableLoc &table_loc = *upd_rtdef.dupd_rtdef_.table_loc_;
        if (OB_FAIL(calc_update_multi_tablet_id(upd_ctdef, *calc_part_id_old, tablet_id))) {
          LOG_WARN("calc multi old part key failed", K(ret));
        } else if (OB_FAIL(DAS_CTX(ctx_).extended_tablet_loc(table_loc, tablet_id, tablet_loc))) {
          LOG_WARN("extended tablet loc failed", K(ret));
        }
      }
    }
  } else {
    //direct write insert row to storage
    tablet_loc = MY_INPUT.get_tablet_loc();
  }
  LOG_DEBUG("get upd del old row pkey", KPC(tablet_loc));
  return ret;
}


int ObTableInsertUpOp::calc_upd_new_row_tablet_loc(const ObUpdCtDef &upd_ctdef,
                                                   ObUpdRtDef &upd_rtdef,
                                                   ObDASTabletLoc *&tablet_loc)
{
  // 只有跨分区的更新时才会有delete task
  int ret = OB_SUCCESS;
  if (MY_SPEC.use_dist_das_) {
    if (upd_ctdef.multi_ctdef_ != nullptr) {
      ObExpr *calc_part_id_new = upd_ctdef.multi_ctdef_->calc_part_id_new_;
      if (calc_part_id_new != nullptr) {
        ObTabletID tablet_id;
        ObDASTableLoc &table_loc = *upd_rtdef.dupd_rtdef_.table_loc_;
        if (OB_FAIL(calc_update_multi_tablet_id(upd_ctdef, *calc_part_id_new, tablet_id))) {
          LOG_WARN("calc multi new part key failed", K(ret));
        } else if (OB_FAIL(DAS_CTX(ctx_).extended_tablet_loc(table_loc, tablet_id, tablet_loc))) {
          LOG_WARN("extended tablet loc failed", K(ret));
        }
      }
    }
  } else {
    //direct write insert row to storage
    tablet_loc = MY_INPUT.get_tablet_loc();
  }
  LOG_DEBUG("get upd ins new row pkey", KPC(tablet_loc));
  return ret;
}

int ObTableInsertUpOp::deal_hint_part_selection(ObObjectID partition_id)
{
  int ret = OB_SUCCESS;
  const ObInsertUpCtDef &insert_up_ctdef = *(MY_SPEC.insert_up_ctdefs_.at(0));
  const ObInsCtDef *ins_ctdef = insert_up_ctdef.ins_ctdef_;
  if (!ins_ctdef->multi_ctdef_->hint_part_ids_.empty()
      && !has_exist_in_array(ins_ctdef->multi_ctdef_->hint_part_ids_, partition_id)) {
    ret = OB_PARTITION_NOT_MATCH;
    LOG_WARN("Partition not match", K(ret),
              K(partition_id), K(ins_ctdef->multi_ctdef_->hint_part_ids_));
  }
  return ret;
}

int ObTableInsertUpOp::calc_insert_tablet_loc(const ObInsCtDef &ins_ctdef,
                                              ObInsRtDef &ins_rtdef,
                                              ObDASTabletLoc *&tablet_loc)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.use_dist_das_) {
    if (ins_ctdef.multi_ctdef_ != nullptr) {
      ObExpr *calc_part_id_expr = ins_ctdef.multi_ctdef_->calc_part_id_expr_;
      ObObjectID partition_id = OB_INVALID_ID;
      ObTabletID tablet_id;
      ObDASTableLoc &table_loc = *ins_rtdef.das_rtdef_.table_loc_;
      if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(calc_part_id_expr, eval_ctx_, partition_id, tablet_id))) {
        LOG_WARN("calc part and tablet id by expr failed", K(ret));
      } else if (OB_FAIL(deal_hint_part_selection(partition_id))) {
        LOG_WARN("Partition not match", K(ret));
      } else if (OB_FAIL(DAS_CTX(ctx_).extended_tablet_loc(table_loc, tablet_id, tablet_loc))) {
        LOG_WARN("extended tablet loc failed", K(ret));
      }
    }
  } else {
    //direct write insert row to storage
    tablet_loc = MY_INPUT.get_tablet_loc();
  }
  return ret;
}

int ObTableInsertUpOp::calc_update_tablet_loc(const ObUpdCtDef &upd_ctdef,
                                              ObUpdRtDef &upd_rtdef,
                                              ObDASTabletLoc *&old_tablet_loc,
                                              ObDASTabletLoc *&new_tablet_loc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_upd_old_row_tablet_loc(upd_ctdef, upd_rtdef, old_tablet_loc))) {
    LOG_WARN("fail to calc update old_row pkey", K(ret), K(upd_ctdef));
  } else if (OB_FAIL(calc_upd_new_row_tablet_loc(upd_ctdef, upd_rtdef, new_tablet_loc))) {
    LOG_WARN("fail to calc update new_row pkey", K(ret), K(upd_ctdef));
  }
  return ret;
}

int ObTableInsertUpOp::get_next_row_from_child()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else {
    found_rows_++;
    LOG_TRACE("child output row", "output row", ROWEXPR2STR(eval_ctx_, child_->get_spec().output_));
  }
  return ret;
}

int ObTableInsertUpOp::do_insert_up()
{
  int ret = OB_SUCCESS;
  bool is_iter_end = false;
  while (OB_SUCC(ret) && !is_iter_end) {
    int64_t insert_rows = 0;
    transaction::ObTxSEQ savepoint_no;
    // must set conflict_row fetch flag
    add_need_conflict_result_flag();
    NG_TRACE_TIMES(2, insert_up_load_all_row);
    if (OB_FAIL(ObSqlTransControl::create_anonymous_savepoint(ctx_, savepoint_no))) {
      LOG_WARN("fail to create save_point", K(ret));
    } else if (OB_FAIL(load_batch_insert_up_rows(is_iter_end, insert_rows))) {
      LOG_WARN("fail to load all row", K(ret));
    } else if (OB_FAIL(post_all_dml_das_task(dml_rtctx_, false))) {
      LOG_WARN("fail to post all das task", K(ret));
    } else if (!check_is_duplicated() && OB_FAIL(ObDMLService::handle_after_row_processing(this, &dml_modify_rows_))) {
      LOG_WARN("try insert is not duplicated, failed to process foreign key handle", K(ret));
    } else if (!check_is_duplicated()) {
      insert_rows_ += insert_rows;
      LOG_TRACE("try insert is not duplicated", K(ret), K(insert_rows_));
    } else if (OB_FAIL(fetch_conflict_rowkey())) {
      LOG_WARN("fail to fetch conflict row", K(ret));
    } else if (OB_FAIL(reset_das_env())) {
      // 这里需要reuse das 相关信息
      LOG_WARN("fail to reset das env", K(ret));
    } else if (OB_FAIL(ObSqlTransControl::rollback_savepoint(ctx_, savepoint_no))) {
      // 本次插入存在冲突, 回滚到save_point
      LOG_WARN("fail to rollback to save_point", K(ret));
    } else if (OB_FAIL(conflict_checker_.do_lookup_and_build_base_map(
        insert_up_row_store_.get_row_cnt()))) {
      LOG_WARN("fail to build conflict map", K(ret));
    } else if (OB_FAIL(do_insert_up_cache())) {
      LOG_WARN("fail to do insert_up in cache", K(ret));
    } else if (!is_ignore_ && OB_FAIL(prepare_final_insert_up_task())) {
      LOG_WARN("fail to prepare final das task", K(ret));
    } else if (OB_FAIL(post_all_dml_das_task(upd_rtctx_, true))) {
      LOG_WARN("do insert rows post process failed", K(ret));
    } else if (OB_FAIL(post_all_dml_das_task(dml_rtctx_, false))) {
      LOG_WARN("do insert rows post process failed", K(ret));
    } else if (OB_FAIL(ObDMLService::handle_after_row_processing(this, &dml_modify_rows_))) {
      LOG_WARN("try insert is duplicated, failed to process foreign key handle", K(ret));
    }

    if (OB_SUCC(ret) && !is_iter_end) {
      // 只有还有下一个batch时才需要做reuse，如果没有下一个batch，close和destroy中会释放内存
      // 前边逻辑执行成功，这一批batch成功完成replace, reuse环境, 准备下一个batch
      if (OB_FAIL(reuse())) {
        LOG_WARN("fail to reuse insert_up op", K(ret));
      }
    }
  }
  return ret;
}

int ObTableInsertUpOp::load_batch_insert_up_rows(bool &is_iter_end, int64_t &insert_rows)
{
  int ret = OB_SUCCESS;
  is_iter_end = false;
  int64_t row_cnt = 0;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_INSERT_UP_BATCH_ROW_COUNT);
  int64_t default_row_batch_cnt = simulate_batch_row_cnt > 0 ?
                                  simulate_batch_row_cnt : INT64_MAX;
  if (is_ignore_ || execute_single_row_) {
    // If it is ignore mode or need excute row by row, degenerate into single line execution
    default_row_batch_cnt = 1;
  }
  LOG_DEBUG("simulate lookup row batch count", K(simulate_batch_row_cnt), K(default_row_batch_cnt));
  while (OB_SUCC(ret) && ++row_cnt <= default_row_batch_cnt) {
    if (OB_FAIL(get_next_row_from_child())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to load next row from child", K(ret));
      } else {
        iter_end_ = true;
      }
    } else if (OB_FAIL(try_insert_row())) {
      LOG_WARN("try insert row to das", K(ret));
    } else if (OB_FAIL(insert_up_row_store_.add_row(MY_SPEC.all_saved_exprs_, &eval_ctx_))) {
      LOG_WARN("add insert_up row to row store failed", K(ret));
    } else {
      plan_ctx->record_last_insert_id_cur_stmt();
      insert_rows++;
      if (insert_up_row_store_.get_mem_used() >= OB_DEFAULT_INSERT_UP_MEMORY_LIMIT) {
        LOG_INFO("insert up rows used memory over limit", K(ret), K(row_cnt), K(insert_rows));
        break;
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    is_iter_end = true;
  }

  return ret;
}

int ObTableInsertUpOp::post_all_dml_das_task(ObDMLRtCtx &dml_rtctx, bool del_task_ahead)
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, insert_up_try_insert);
  if (dml_rtctx.das_ref_.has_task()) {
    if (del_task_ahead) {
      if (OB_FAIL(dml_rtctx.das_ref_.pick_del_task_to_first())) {
        LOG_WARN("remove delete das task first failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml_rtctx.das_ref_.execute_all_task())) {
        LOG_WARN("execute all das task failed", K(ret));
      }
    }
  }
  return ret;
}

bool ObTableInsertUpOp::check_is_duplicated()
{
  int bret = false;
  for (int64_t i = 0; i < insert_up_rtdefs_.count(); ++i) {
    ObInsertUpRtDef &insert_up_rtdef = insert_up_rtdefs_.at(i);
    ObDASInsRtDef &ins_rtdef = insert_up_rtdef.ins_rtdef_.das_rtdef_;
    if (ins_rtdef.is_duplicated_) {
      ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
      plan_ctx->set_last_insert_id_cur_stmt(0);
      bret = true;
    }
  }
  return bret;
}

int ObTableInsertUpOp::get_next_conflict_rowkey(DASTaskIter &task_iter)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  while (OB_SUCC(ret) && !got_row) {
    ObNewRow *dup_row = nullptr;
    ObChunkDatumStore::StoredRow *stored_row = nullptr;
    ObDASWriteBuffer::DmlShadowRow ssr;
    ObDASInsertOp *ins_op = static_cast<ObDASInsertOp*>(*task_iter);
    ObNewRowIterator *conflict_result = ins_op->get_duplicated_result();
    const ObDASInsCtDef *ins_ctdef = static_cast<const ObDASInsCtDef*>(ins_op->get_ctdef());
    // 因为返回的都是主表的主键，主表的主键一定是在存储层有储存的，是不需要再收起来层再做运算的，
    // 所以这里不需要clear eval flag
    // clear_datum_eval_flag();
    if (OB_ISNULL(conflict_result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("duplicted key result is null", K(ret));
    } else if (OB_FAIL(conflict_result->get_next_row(dup_row))) {
      if (OB_ITER_END == ret) {
        ++task_iter;
        if (!task_iter.is_end()) {
          ret = OB_SUCCESS;
        }
      } else {
        LOG_WARN("get next row from das result failed", K(ret));
      }
    } else if (OB_FAIL(ssr.init(dml_rtctx_.get_das_alloc(), ins_ctdef->table_rowkey_types_, false))) {
      LOG_WARN("init shadow stored row failed", K(ret), K(ins_ctdef->table_rowkey_types_));
    } else if (OB_FAIL(ssr.shadow_copy(*dup_row))) {
      LOG_WARN("shadow copy ob new row failed", K(ret));
    } else if (OB_ISNULL(stored_row = ssr.get_store_row())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("stored row is null", K(ret));
    } else if (OB_FAIL(stored_row->to_expr(
            conflict_checker_.checker_ctdef_.data_table_rowkey_expr_,
            conflict_checker_.eval_ctx_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from result iterator failed", K(ret));
      }
    } else {
      got_row = true;
    }
  }
  return ret;
}

int ObTableInsertUpOp::fetch_conflict_rowkey()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  NG_TRACE_TIMES(2, insert_up_start_lookup);
  DASTaskIter task_iter = dml_rtctx_.das_ref_.begin_task_iter();
  while (OB_SUCC(ret) && !task_iter.is_end()) {
    // 不需要clear rowkey表达式的eval_flag，因为主键使用的是column_ref表达式，不存在eval_fun
    if (OB_FAIL(get_next_conflict_rowkey(task_iter))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next conflict rowkey from das_result", K(ret));
      }
    } else if (OB_FAIL(conflict_checker_.build_primary_table_lookup_das_task())) {
      LOG_WARN("fail to build lookup_das_task", K(ret));
    }
  }
  ret = (ret == OB_ITER_END ? OB_SUCCESS : ret);
  return ret;
}

int ObTableInsertUpOp::prepare_final_insert_up_task()
{
  int ret = OB_SUCCESS;
  ObConflictRowMap *primary_map = NULL;
  NG_TRACE_TIMES(2, insert_up_final_shuff);
  OZ(conflict_checker_.get_primary_table_map(primary_map));
  CK(OB_NOT_NULL(primary_map));
  ObConflictRowMap::iterator start_row_iter = primary_map->begin();
  ObConflictRowMap::iterator end_row_iter = primary_map->end();
  for (; OB_SUCC(ret) && start_row_iter != end_row_iter; ++start_row_iter) {
    clear_datum_eval_flag();
    ObConflictValue &constraint_value = start_row_iter->second;
    LOG_DEBUG("get one constraint_value from primary hash map", K(constraint_value));
    if (constraint_value.new_row_source_ == ObNewRowSource::FROM_UPDATE) {
      OZ(do_update(constraint_value));
    } else if (constraint_value.new_row_source_ == ObNewRowSource::NEED_DO_LOCK) {
      OZ(do_lock(constraint_value));
    } else if (constraint_value.new_row_source_ == ObNewRowSource::FROM_INSERT) {
      OZ(do_insert(constraint_value));
    }
  }
  return ret;
}

int ObTableInsertUpOp::do_lock(const ObConflictValue &constraint_value)
{
  int ret = OB_SUCCESS;
  const ObInsertUpCtDef &insert_up_ctdef = *(MY_SPEC.insert_up_ctdefs_.at(0));
  const ObUpdCtDef *pri_upd_ctdef = insert_up_ctdef.upd_ctdef_;
  ObInsertUpRtDef &insert_up_rtdef = insert_up_rtdefs_.at(0);
  ObUpdRtDef &pri_upd_rtdef = insert_up_rtdef.upd_rtdef_;
  ObDASTabletLoc *tablet_loc = nullptr;
  if (constraint_value.new_row_source_ == ObNewRowSource::NEED_DO_LOCK) {
    if (NULL != constraint_value.baseline_datum_row_ &&
        NULL != constraint_value.current_datum_row_) {
      if (constraint_value.baseline_datum_row_ == constraint_value.current_datum_row_) {
        // do lock
        OZ(constraint_value.baseline_datum_row_->to_expr(get_primary_table_upd_old_row(), eval_ctx_));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected constraint_value", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected constraint_value", K(ret));
    }
  }

  // lock 只锁主表
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calc_upd_old_row_tablet_loc(*pri_upd_ctdef, pri_upd_rtdef, tablet_loc))) {
    LOG_WARN("fail to calc pkey for lock", K(ret), KPC(pri_upd_ctdef));
  } else if (OB_FAIL(lock_one_row_to_das(*pri_upd_ctdef, pri_upd_rtdef, tablet_loc))) {
    LOG_WARN("fail to write one row to lock task", K(ret), KPC(tablet_loc), KPC(pri_upd_ctdef));
  }
  return ret;
}

int ObTableInsertUpOp::do_update(const ObConflictValue &constraint_value)
{
  int ret = OB_SUCCESS;
  bool only_do_upd_ins = false;
  bool only_do_lock = false;
  if (constraint_value.new_row_source_ == ObNewRowSource::FROM_UPDATE) {
    // current_datum_row_ 是update的new_row
    if (NULL != constraint_value.baseline_datum_row_ &&
        NULL != constraint_value.current_datum_row_) {
      // base_line 和 curr_row 都存在
      OZ(constraint_value.baseline_datum_row_->to_expr(get_primary_table_upd_old_row(), eval_ctx_));
      OZ(delete_upd_old_row_to_das());
      OZ(constraint_value.current_datum_row_->to_expr(get_primary_table_upd_new_row(), eval_ctx_));
      OZ(insert_upd_new_row_to_das());
    } else if (NULL == constraint_value.baseline_datum_row_ &&
               NULL != constraint_value.current_datum_row_) {
      // base_line不存在 但是 curr_row 存在，说明curr_row是从其他行update来的
      OZ(constraint_value.current_datum_row_->to_expr(get_primary_table_upd_new_row(), eval_ctx_));
      OZ(insert_upd_new_row_to_das());
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected constraint_value", K(ret));
    }
  }
  return ret;
}

int ObTableInsertUpOp::do_update_with_ignore()
{
  int ret = OB_SUCCESS;
  ObInsertUpCtDef *insert_up_ctdef = MY_SPEC.insert_up_ctdefs_.at(0);
  const ObUpdCtDef *upd_ctdef = insert_up_ctdef->upd_ctdef_;
  ObInsertUpRtDef &insert_up_rtdef = insert_up_rtdefs_.at(0);
  ObUpdRtDef &upd_rtdef = insert_up_rtdef.upd_rtdef_;
  ObDASTabletLoc *old_tablet_loc = nullptr;
  ObDASTabletLoc *new_tablet_loc = nullptr;
  ObDMLModifyRowNode modify_row(this, (upd_ctdef), &upd_rtdef, ObDmlEventType::DE_UPDATING);
  if (MY_SPEC.insert_up_ctdefs_.count() > 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support global index with ignore", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Do update about global index with ignore");
  } else if (OB_FAIL(calc_update_tablet_loc(*upd_ctdef, upd_rtdef, old_tablet_loc, new_tablet_loc))) {
    LOG_WARN("fail to calc update_pkey", K(ret), KPC(upd_ctdef));
  } else if (old_tablet_loc != new_tablet_loc) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ignore not supported", K(ret), KPC(old_tablet_loc), KPC(new_tablet_loc));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Do update with ignore under inconsistent tablet loc");
  } else if (OB_FAIL(ObDMLService::update_row(*upd_ctdef, upd_rtdef, old_tablet_loc, new_tablet_loc, dml_rtctx_,
                                             modify_row.old_row_, modify_row.new_row_, modify_row.full_row_))) {
    LOG_WARN("fail to insert update_row to das", K(ret));
  } else if (need_after_row_process(*upd_ctdef) && OB_FAIL(dml_modify_rows_.push_back(modify_row))) {
    LOG_WARN("failed to push dml modify row to modified row list", K(ret));
  }
  return ret;
}

int ObTableInsertUpOp::do_insert(const ObConflictValue &constraint_value)
{
  int ret = OB_SUCCESS;
  bool has_insert = false;
  bool has_delete = false;
  // curr_row来自于insert
  if (NULL != constraint_value.baseline_datum_row_ &&
      NULL != constraint_value.current_datum_row_) {
    // delete + insert
    OZ(constraint_value.baseline_datum_row_->to_expr(get_primary_table_upd_old_row(), eval_ctx_));
    OZ(delete_upd_old_row_to_das());
    OZ(constraint_value.current_datum_row_->to_expr(get_primary_table_insert_row(), eval_ctx_));
    OZ(insert_row_to_das());
  } else if (NULL != constraint_value.baseline_datum_row_ &&
             NULL == constraint_value.current_datum_row_) {
    // only delete
    OZ(constraint_value.baseline_datum_row_->to_expr(get_primary_table_upd_old_row(), eval_ctx_));
    OZ(delete_upd_old_row_to_das());
  } else if (NULL == constraint_value.baseline_datum_row_ &&
             NULL != constraint_value.current_datum_row_) {
    // only insert
    OZ(constraint_value.current_datum_row_->to_expr(get_primary_table_insert_row(), eval_ctx_));
    OZ(insert_row_to_das());
  }
  return ret;
}

int ObTableInsertUpOp::reset_das_env()
{
  int ret = OB_SUCCESS;
  // 释放第一次try insert的das task
  if (OB_FAIL(dml_rtctx_.das_ref_.close_all_task())) {
    LOG_WARN("close all das task failed", K(ret));
  } else {
    dml_rtctx_.das_ref_.reuse();
    dml_modify_rows_.clear();
  }

  // 因为第二次插入不需要fetch conflict result了，如果有conflict
  // 就直接报错
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_up_rtdefs_.count(); ++i) {
    ObInsRtDef &ins_rtdef = insert_up_rtdefs_.at(i).ins_rtdef_;
    ins_rtdef.das_rtdef_.need_fetch_conflict_ = false;
    ins_rtdef.das_rtdef_.is_duplicated_ = false;
  }
  return ret;
}

int ObTableInsertUpOp::reuse()
{
  int ret = OB_SUCCESS;
  if (dml_rtctx_.das_ref_.has_task()) {
    if (OB_FAIL(dml_rtctx_.das_ref_.close_all_task())) {
      LOG_WARN("close all insert das task failed", K(ret));
    } else {
      dml_rtctx_.das_ref_.reuse();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(upd_rtctx_.das_ref_.close_all_task())) {
      LOG_WARN("close all update das task failed", K(ret));
    } else {
      upd_rtctx_.das_ref_.reuse();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(conflict_checker_.reuse())) {
      LOG_WARN("fail to reuse conflict checker", K(ret));
    } else {
      insert_up_row_store_.reset();
      dml_modify_rows_.clear();
    }
  }

  return ret;
}

void ObTableInsertUpOp::add_need_conflict_result_flag()
{
  for (int64_t i = 0; i < insert_up_rtdefs_.count(); ++i) {
    ObInsRtDef &ins_rtdef = insert_up_rtdefs_.at(i).ins_rtdef_;
    ins_rtdef.das_rtdef_.need_fetch_conflict_ = true;
  }
  dml_rtctx_.set_non_sub_full_task();
  upd_rtctx_.set_pick_del_task_first();
  upd_rtctx_.set_non_sub_full_task();
}

int ObTableInsertUpOp::calc_auto_increment(const ObUpdCtDef &upd_ctdef)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  NG_TRACE_TIMES(2, insertup_start_calc_update_row);
  if (OB_SUCC(ret)) {
    // before calc new row, to be compatible with MySQL
    // 1. disable operation to sync user specified value for auto-increment column because duplicate
    //    hidden pk will be placed in first place of row
    //    ATTENTION: suppose two auto-increment column at most here
    // 2. set duplicate flag to reuse auto-generated value
    ObIArray<AutoincParam> &autoinc_params = plan_ctx->get_autoinc_params();
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
      autoinc_params.at(i).sync_flag_ = false;
      if (NULL != autoinc_params.at(i).cache_handle_) {
        autoinc_params.at(i).cache_handle_->last_row_dup_flag_ = false;
        LOG_DEBUG("set autoinc_params", K(i), K(autoinc_params.at(i)));
      }
    }

    if (OB_SUCC(ret)) {
      if (plan_ctx->get_autoinc_id_tmp() == plan_ctx->get_last_insert_id_cur_stmt()) {
        plan_ctx->set_last_insert_id_cur_stmt(0);
      }
      FOREACH_CNT_X(info, upd_ctdef.assign_columns_, OB_SUCC(ret)) {
        const uint64_t idx = info->projector_index_;
        OZ(update_auto_increment(*upd_ctdef.new_row_.at(idx), upd_ctdef.column_ids_.at(idx)));
      }

      // !!!ATTENTION: OB_HIDDEN_PK_INCREMENT_COLUMN_ID is changed to tablet seq hidden pk,
      // you should be careful if you want to restore the following logic
      // 对于 insert on duplicate 场景，不需要支持 MySQL 的行为。
      // 假设我们支持 MySQL 的行为，那么意味着我们努力地尝试让表的自增值全局递增，
      // 而不是仅仅分区内保持递增。对于全局递增的目标，我们理论上就达不到，也没有
      // 去追求。
      // 所以，决定放弃 insert on duplicate 场景下的兼容行为。
      // 但是，依然保持 insert 一个确定值时，全局推高自增值的行为。因为这个有一定
      // 的应用场景：例如，用户希望在某个时间点，全局推高所有分区的自增值到某个值
      // 之后。
      //
      // 基于这个决策，删除下面所有代码。为了便于追溯，仅注释掉代码，保持一定时间。
      //if (OB_SUCC(ret)) {
      //  // to be compatible with MySQL
      //  // some value for auto-increment column may come from update stmt; we should sync it
      //  // for example
      //  //   create table t1(c1 int unique, c2 int auto_increment primary key, c3 int);
      //  //   insert into t1 values (1, 1, 1);
      //  //   update t1 set c2 = 10 where c1 = 1;
      //  //   insert into t1 values (1, 100, 100) on duplicate key update c2 = 10, c1=2;
      //  //   insert into t1 values (3, null, 100);
      //  // the last insert should generate 11 for c2
      //  //如果自增列的值c1=10,不是通过insert语句差进去的，那么这个自增值10不会被sync出去。
      //  //但是，如果这一行又被insert on
      //  //duplicate,并且，更新的不是自增值的话，这个自增列的旧值10将被sync出去；
      //  LOG_INFO("check autoincrement", K(is_auto_col_changed), K(is_row_changed));
      //  if (!is_auto_col_changed && is_row_changed) {
      //    ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
      //    if (OB_ISNULL(plan_ctx)) {
      //      ret = OB_ERR_UNEXPECTED;
      //      LOG_WARN("fail to get physical plan ctx", K(plan_ctx));
      //    } else {
      //      ObIArray<AutoincParam> &autoinc_params = plan_ctx->get_autoinc_params();
      //      for (int64_t z = 0; z < autoinc_params.count() && OB_SUCC(ret); ++z) {
      //        SQL_ENG_LOG(DEBUG, "print autoinc", K(autoinc_params.at(z)), K(z));
      //        // 非隐藏列场景下，将 value sync 出去，使得所有 server 上的下一个值都大于 value
      //        if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID != autoinc_params.at(z).autoinc_col_id_) {
      //          AutoincParam &autoinc_param = autoinc_params.at(z);
      //          const ObObj &val = scan_result_row.get_cell(autoinc_param.autoinc_old_value_index_);
      //          LOG_INFO("check autoinc val", K(autoinc_param), K(scan_result_row), K(val));
      //          uint64_t casted_value = 0;
      //          EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      //          EXPR_GET_UINT64_V2(val, casted_value);
      //          if (OB_FAIL(ret)) {
      //            LOG_WARN("failed to cast value; still go on", K(ret));
      //            ret = OB_SUCCESS;
      //          } else {
      //            autoinc_param.value_to_sync_ = casted_value;
      //            autoinc_param.sync_flag_ = true;
      //          }
      //        }
      //      }
      //    }
      //  }
      //}
    }
  }
  return ret;
}

int ObTableInsertUpOp::update_auto_increment(const ObExpr &expr,
                                             const uint64_t cid)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  ObIArray<AutoincParam> &autoinc_params = plan_ctx->get_autoinc_params();
  AutoincParam *autoinc_param = NULL;
  for (int64_t j = 0; OB_SUCC(ret) && j < autoinc_params.count(); ++j) {
    if (cid == autoinc_params.at(j).autoinc_col_id_) {
      autoinc_param = &autoinc_params.at(j);
      break;
    }
  }
  if (NULL != autoinc_param) {
    bool is_zero = false;
    uint64_t casted_value = 0;
    ObDatum *datum = NULL;
    if (OB_FAIL(expr.eval(eval_ctx_, datum))) {
      LOG_WARN("evaluate expression failed", K(ret));
    } else if (OB_FAIL(ObExprAutoincNextval::get_uint_value(
                expr, datum, is_zero, casted_value))) {
      LOG_WARN("get casted valued failed", K(ret), K(*datum));
    } else {
      CacheHandle *cache_handle = autoinc_param->cache_handle_;
      if (!OB_ISNULL(cache_handle)
          && true == cache_handle->last_row_dup_flag_
          && 0 != cache_handle->last_value_to_confirm_) {
        // auto-increment value has been generated for this row
        if (casted_value == cache_handle->last_value_to_confirm_) {
          // column may be updated, but updated value is the same with old value
          cache_handle->last_row_dup_flag_ = false;
          cache_handle->last_value_to_confirm_ = 0;
        } else if (cache_handle->in_range(casted_value)) {
          // update value in generated range
          ret = OB_ERR_AUTO_INCREMENT_CONFLICT;
          LOG_WARN("update value in auto-generated range", K(ret), K(casted_value), KPC(cache_handle));
        } else {
          autoinc_param->value_to_sync_ = casted_value;
          autoinc_param->sync_flag_ = true;
        }
      } else {
        // no auto-increment value generated; user specify a value
        // mark sync flag to sync update value
        autoinc_param->value_to_sync_ = casted_value;
        autoinc_param->sync_flag_ = true;
      }
    }
  }
  NG_TRACE_TIMES(2, insertup_end_auto_increment);
  return ret;
}

int ObTableInsertUpOp::check_insert_up_ctdefs_valid() const
{
  int ret = OB_SUCCESS;
  CK(MY_SPEC.insert_up_ctdefs_.count() > 0);
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.insert_up_ctdefs_.count(); ++i) {
    const ObInsertUpCtDef *insert_up_ctdef = NULL;
    CK(OB_NOT_NULL(insert_up_ctdef = MY_SPEC.insert_up_ctdefs_.at(i)));
    CK(OB_NOT_NULL(insert_up_ctdef->ins_ctdef_));
    //CK(OB_NOT_NULL(insert_up_ctdef->upd_ctdef_));
  }
  return ret;
}

const ObIArray<ObExpr *> &ObTableInsertUpOp::get_primary_table_insert_row()
{
  return MY_SPEC.insert_up_ctdefs_.at(0)->ins_ctdef_->new_row_;
}

const ObIArray<ObExpr *> &ObTableInsertUpOp::get_primary_table_columns()
{
  return get_primary_table_upd_old_row();
}

const ObIArray<ObExpr *> &ObTableInsertUpOp::get_primary_table_upd_new_row()
{
  return MY_SPEC.insert_up_ctdefs_.at(0)->upd_ctdef_->new_row_;
}

const ObIArray<ObExpr *> &ObTableInsertUpOp::get_primary_table_upd_old_row()
{
  return MY_SPEC.insert_up_ctdefs_.at(0)->upd_ctdef_->old_row_;
}
} // end namespace sql
} // end namespace oceanbase
