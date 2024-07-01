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
#include "sql/engine/dml/ob_table_merge_op.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_autoincrement_service.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/rowkey/ob_rowkey.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/engine/dml/ob_trigger_handler.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/dml/ob_fk_checker.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace sql;

namespace sql
{

OB_SERIALIZE_MEMBER((ObTableMergeOpInput, ObTableModifyOpInput));

OB_DEF_SERIALIZE(ObTableMergeSpec)
{
  int ret = OB_SUCCESS;
  int64_t index_cnt = merge_ctdefs_.count();
  BASE_SER((ObTableMergeSpec, ObTableModifySpec));
  OB_UNIS_ENCODE(has_insert_clause_);
  OB_UNIS_ENCODE(has_update_clause_);

  OB_UNIS_ENCODE(delete_conds_);
  OB_UNIS_ENCODE(update_conds_);
  OB_UNIS_ENCODE(insert_conds_);
  OB_UNIS_ENCODE(distinct_key_exprs_);
  OB_UNIS_ENCODE(index_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; ++i) {
    ObMergeCtDef *merge_ctdef = merge_ctdefs_.at(i);
    if (OB_ISNULL(merge_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge_ctdef is nullptr", K(ret));
    }
    OB_UNIS_ENCODE(*merge_ctdef);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTableMergeSpec)
{
  int ret = OB_SUCCESS;
  int64_t index_cnt = 0;
  BASE_DESER((ObTableMergeSpec, ObTableModifySpec));
  OB_UNIS_DECODE(has_insert_clause_);
  OB_UNIS_DECODE(has_update_clause_);

  OB_UNIS_DECODE(delete_conds_);
  OB_UNIS_DECODE(update_conds_);
  OB_UNIS_DECODE(insert_conds_);
  OB_UNIS_DECODE(distinct_key_exprs_);
  OB_UNIS_DECODE(index_cnt);
  OZ(merge_ctdefs_.init(index_cnt));
  ObDMLCtDefAllocator<ObMergeCtDef> merge_ctdef_allocator(alloc_);
  for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; ++i) {
    ObMergeCtDef *merge_ctdef = merge_ctdef_allocator.alloc();
    if (OB_ISNULL(merge_ctdef)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc merge_ctdef failed", K(ret));
    }
    OB_UNIS_DECODE(*merge_ctdef);
    OZ(merge_ctdefs_.push_back(merge_ctdef));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableMergeSpec)
{
  int64_t len = 0;
  int64_t index_cnt = merge_ctdefs_.count();
  BASE_ADD_LEN((ObTableMergeSpec, ObTableModifySpec));
  OB_UNIS_ADD_LEN(has_insert_clause_);
  OB_UNIS_ADD_LEN(has_update_clause_);

  OB_UNIS_ADD_LEN(delete_conds_);
  OB_UNIS_ADD_LEN(update_conds_);
  OB_UNIS_ADD_LEN(insert_conds_);
  OB_UNIS_ADD_LEN(distinct_key_exprs_);
  OB_UNIS_ADD_LEN(index_cnt);
  for (int64_t i = 0; i < index_cnt; ++i) {
    ObMergeCtDef *merge_ctdef = merge_ctdefs_.at(i);
    if (merge_ctdef != nullptr) {
      OB_UNIS_ADD_LEN(*merge_ctdef);
    }
  }
  return len;
}

ObTableMergeSpec::ObTableMergeSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObTableModifySpec(alloc, type),
    has_insert_clause_(false),
    has_update_clause_(false),
    delete_conds_(alloc),
    update_conds_(alloc),
    insert_conds_(alloc),
    distinct_key_exprs_(alloc),
    merge_ctdefs_(alloc),
    alloc_(alloc)
{
}

OB_INLINE int ObTableMergeSpec::get_single_dml_ctdef(const ObDMLBaseCtDef *&dml_ctdef) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merge_ctdefs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge into does ont only one table reference", K(ret), K(merge_ctdefs_.count()));
  } else if (has_insert_clause_) {
    if (OB_ISNULL(merge_ctdefs_.at(0)->ins_ctdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ins_ctdef_ can not be null", K(ret));
    } else {
      dml_ctdef = merge_ctdefs_.at(0)->ins_ctdef_;
    }
  } else if (has_update_clause_) {
    if (OB_ISNULL(merge_ctdefs_.at(0)->upd_ctdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upd_ctdef_ can not be null", K(ret));
    } else {
      dml_ctdef = merge_ctdefs_.at(0)->upd_ctdef_;
    }
  }
  return ret;
}

ObTableMergeOp::ObTableMergeOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObTableModifyOp(ctx, spec, input),
      affected_rows_(0)
{
}

int ObTableMergeOp::check_need_exec_single_row()
{
  int ret = OB_SUCCESS;
  ret = ObTableModifyOp::check_need_exec_single_row();
  if (OB_SUCC(ret) && !execute_single_row_) {
    ObMergeCtDef *merge_ctdef = MY_SPEC.merge_ctdefs_.at(0);
    if (!execute_single_row_ && OB_NOT_NULL(merge_ctdef->ins_ctdef_)) {
      const ObInsCtDef &ins_ctdef = *merge_ctdef->ins_ctdef_;
      for (int64_t j = 0;
          OB_SUCC(ret) && !execute_single_row_ && j < ins_ctdef.trig_ctdef_.tg_args_.count();
          ++j) {
        const ObTriggerArg &tri_arg = ins_ctdef.trig_ctdef_.tg_args_.at(j);
        execute_single_row_ = tri_arg.is_execute_single_row();
      }
    }

    if (!execute_single_row_ && OB_NOT_NULL(merge_ctdef->upd_ctdef_)) {
      const ObUpdCtDef &upd_ctdef = *merge_ctdef->upd_ctdef_;
      for (int64_t j = 0;
          OB_SUCC(ret) && !execute_single_row_ && j < upd_ctdef.trig_ctdef_.tg_args_.count();
          ++j) {
        const ObTriggerArg &tri_arg = upd_ctdef.trig_ctdef_.tg_args_.at(j);
        execute_single_row_ = tri_arg.is_execute_single_row();
      }
    }

    if (!execute_single_row_ && OB_NOT_NULL(merge_ctdef->del_ctdef_)) {
      const ObDelCtDef &del_ctdef = *merge_ctdef->del_ctdef_;
      for (int64_t j = 0;
          OB_SUCC(ret) && !execute_single_row_ && j < del_ctdef.trig_ctdef_.tg_args_.count();
          ++j) {
        const ObTriggerArg &tri_arg = del_ctdef.trig_ctdef_.tg_args_.at(j);
        execute_single_row_ = tri_arg.is_execute_single_row();
      }
    }
  }
  return ret;
}

int ObTableMergeOp::inner_open_with_das()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(open_table_for_each())) {
    LOG_WARN("open table for each failed", K(ret), K(MY_SPEC.merge_ctdefs_.count()));
  } else {
    dml_rtctx_.set_pick_del_task_first();
  }
  return ret;
}

int ObTableMergeOp::open_table_for_each()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(merge_rtdefs_.allocate_array(ctx_.get_allocator(), MY_SPEC.merge_ctdefs_.count()))) {
    LOG_WARN("allocate merge rtdef failed", K(ret), K(MY_SPEC.merge_ctdefs_.count()));
  } else if (OB_FAIL(ObDMLService::create_rowkey_check_hashset(get_spec().rows_, &ctx_, merge_rtdefs_.at(0).rowkey_dist_ctx_))) {
    LOG_WARN("Failed to create hash set", K(ret));
  } else if (OB_FAIL(ObDMLService::init_ob_rowkey(ctx_.get_allocator(), MY_SPEC.distinct_key_exprs_.count(), merge_rtdefs_.at(0).table_rowkey_))) {
    LOG_WARN("fail to init ObRowkey used for distinct check", K(ret));
  }
  trigger_clear_exprs_.reset();
  fk_checkers_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.merge_ctdefs_.count(); ++i) {
    ObMergeCtDef *merge_ctdef = MY_SPEC.merge_ctdefs_.at(i);
    if (OB_NOT_NULL(merge_ctdef->ins_ctdef_)) {
      // init insert rtdef
      const ObInsCtDef &ins_ctdef = *merge_ctdef->ins_ctdef_;
      ObInsRtDef &ins_rtdef = merge_rtdefs_.at(i).ins_rtdef_;
      if (OB_FAIL(ObDMLService::init_ins_rtdef(dml_rtctx_,
                                               ins_rtdef,
                                               ins_ctdef,
                                               trigger_clear_exprs_,
                                               fk_checkers_))) {
        LOG_WARN("init ins rtdef failed", K(ret));
      } else if (ins_ctdef.is_primary_index_) {
        if (OB_FAIL(ObDMLService::process_before_stmt_trigger(ins_ctdef, ins_rtdef, dml_rtctx_,
                                                              ObDmlEventType::DE_INSERTING))) {
          LOG_WARN("process before stmt trigger failed", K(ret));
        } else {
          ins_rtdef.das_rtdef_.table_loc_->is_writing_ = true;
        }
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(merge_ctdef->upd_ctdef_)) {
      // init update rtdef
      const ObUpdCtDef &upd_ctdef = *merge_ctdef->upd_ctdef_;
      ObUpdRtDef &upd_rtdef = merge_rtdefs_.at(i).upd_rtdef_;
      upd_rtdef.primary_rtdef_ = &merge_rtdefs_.at(0).upd_rtdef_;
      if (OB_FAIL(ObDMLService::init_upd_rtdef(dml_rtctx_,
                                               upd_rtdef,
                                               upd_ctdef,
                                               trigger_clear_exprs_,
                                               fk_checkers_))) {
        LOG_WARN("init upd rtdef failed", K(ret));
      } else if (upd_ctdef.is_primary_index_) {
        if (OB_FAIL(ObDMLService::process_before_stmt_trigger(upd_ctdef, upd_rtdef, dml_rtctx_,
                                                             ObDmlEventType::DE_UPDATING))) {
          LOG_WARN("process after stmt trigger failed", K(ret));
        } else {
          upd_rtdef.dupd_rtdef_.table_loc_->is_writing_ = true;
        }
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(merge_ctdef->del_ctdef_)) {
      // init delete rtdef
      const ObDelCtDef &del_ctdef = *merge_ctdef->del_ctdef_;
      ObDelRtDef &del_rtdef = merge_rtdefs_.at(i).del_rtdef_;
      if (OB_FAIL(ObDMLService::init_del_rtdef(dml_rtctx_, del_rtdef, del_ctdef))) {
        LOG_WARN("init delete rtdef failed", K(ret));
      } else if (del_ctdef.is_primary_index_) {
        if (OB_FAIL(ObDMLService::process_before_stmt_trigger(del_ctdef, del_rtdef, dml_rtctx_,
                                                              ObDmlEventType::DE_DELETING))) {
          LOG_WARN("process before stmt trigger failed", K(ret));
        } else {
          del_rtdef.das_rtdef_.table_loc_->is_writing_ = true;
        }
      }
    }
  }
  return ret;
}

OB_INLINE int ObTableMergeOp::close_table_for_each()
{
  int ret = OB_SUCCESS;
  if (!merge_rtdefs_.empty() && (OB_SUCCESS == ctx_.get_errcode())) {
    //only primary index table need to execute triggered
    const ObMergeCtDef &merge_ctdef = *MY_SPEC.merge_ctdefs_.at(0);
    ObMergeRtDef &merge_rtdef = merge_rtdefs_.at(0);
    if (OB_NOT_NULL(merge_ctdef.ins_ctdef_)) {
      const ObInsCtDef &ins_ctdef = *merge_ctdef.ins_ctdef_;
      ObInsRtDef &ins_rtdef = merge_rtdef.ins_rtdef_;
      if (OB_NOT_NULL(ins_rtdef.das_rtdef_.table_loc_)) {
        ins_rtdef.das_rtdef_.table_loc_->is_writing_ = false;
      }
      if (OB_FAIL(ObDMLService::process_after_stmt_trigger(ins_ctdef, ins_rtdef, dml_rtctx_,
                                                           ObDmlEventType::DE_INSERTING))) {
        LOG_WARN("process after stmt trigger failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(merge_ctdef.upd_ctdef_)) {
      const ObUpdCtDef &upd_ctdef = *merge_ctdef.upd_ctdef_;
      ObUpdRtDef &upd_rtdef = merge_rtdef.upd_rtdef_;
      if (OB_NOT_NULL(upd_rtdef.dupd_rtdef_.table_loc_)) {
        upd_rtdef.dupd_rtdef_.table_loc_->is_writing_ = false;
      }
      if (OB_FAIL(ObDMLService::process_after_stmt_trigger(upd_ctdef, upd_rtdef, dml_rtctx_,
                                                           ObDmlEventType::DE_UPDATING))) {
        LOG_WARN("process after stmt trigger failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(merge_ctdef.del_ctdef_)) {
      const ObDelCtDef &del_ctdef = *merge_ctdef.del_ctdef_;
      ObDelRtDef &del_rtdef = merge_rtdef.del_rtdef_;
      if (OB_NOT_NULL(del_rtdef.das_rtdef_.table_loc_)) {
        del_rtdef.das_rtdef_.table_loc_->is_writing_ = false;
      }
      if (OB_FAIL(ObDMLService::process_after_stmt_trigger(del_ctdef, del_rtdef, dml_rtctx_,
                                                           ObDmlEventType::DE_DELETING))) {
        LOG_WARN("process after stmt trigger failed", K(ret));
      }
    }
  }
  //whether it is successful or not, needs to release rtdef
  merge_rtdefs_.release_array();
  affected_rows_ = 0;
  return ret;
}

int ObTableMergeOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_UNLIKELY(MY_SPEC.merge_ctdefs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("del ctdef is invalid", K(ret), KP(this));
  } else if (OB_UNLIKELY(iter_end_)) {
    //do nothing
  } else if (OB_FAIL(inner_open_with_das())) {
    LOG_WARN("inner open with das failed", K(ret));
  }
  return ret;
}

int ObTableMergeOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.gi_above_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table merge into rescan not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "table merge into rescan");
  } else if (OB_FAIL(ObTableModifyOp::inner_rescan())) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else if (OB_FAIL(close_table_for_each())) {
    LOG_WARN("release table context for each failed", K(ret));
  } else if (OB_UNLIKELY(iter_end_)) {
    //do nothing
  } else if (OB_FAIL(open_table_for_each())) {
    LOG_WARN("open table for each failed", K(ret));
  }
  return ret;
}

int ObTableMergeOp::check_is_match(bool &is_match)
{
  int ret = OB_SUCCESS;
  const ObTableMergeSpec &spec = MY_SPEC;
  is_match = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < spec.distinct_key_exprs_.count(); ++i) {
    ObDatum *datum = NULL;
    const ObExpr *e = spec.distinct_key_exprs_.at(i);
    if (OB_ISNULL(e)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (OB_FAIL(e->eval(eval_ctx_, datum))) {
      LOG_WARN("failed to evaluate expression", K(ret));
    } else {
      is_match = !datum->is_null();
    }
  }
  return ret;
}

int ObTableMergeOp::calc_delete_condition(const ObIArray<ObExpr*> &exprs, bool &is_true_cond)
{
  int ret = OB_SUCCESS;
  if (0 == exprs.count()) {
    is_true_cond = false;
  } else if (OB_FAIL(calc_condition(exprs, is_true_cond))) {
    LOG_WARN("fail to calc delete join condition", K(ret), K(exprs));
  }
  return ret;
}

int ObTableMergeOp::calc_condition(const ObIArray<ObExpr*> &exprs, bool &is_true_cond)
{
  int ret = OB_SUCCESS;
  ObDatum *cmp_res = NULL;
  is_true_cond = true;
  ARRAY_FOREACH(exprs, i) {
    if (OB_FAIL(exprs.at(i)->eval(eval_ctx_, cmp_res))) {
      LOG_WARN("fail to calc other join condition", K(ret), K(*exprs.at(i)));
    } else if (cmp_res->is_null() || 0 == cmp_res->get_int()) {
      is_true_cond = false;
      break;
    }
  }
  return ret;
}

int ObTableMergeOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(close_table_for_each())) {
    LOG_WARN("clost table for each", K(ret));
  }
  int  close_ret = ObTableModifyOp::inner_close();
  return (OB_SUCCESS == ret) ? close_ret : ret;
}

int ObTableMergeOp::write_row_to_das_buffer()
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  if (OB_FAIL(check_is_match(is_match))) {
    LOG_WARN("failed to check is match", K(ret));
  } else if (is_match) {
    // matched then update / delete
    if (MY_SPEC.has_update_clause_ && OB_FAIL(do_update())) {
      LOG_WARN("fail to do_update", K(ret));
    }
  } else {
    // not matched then insert
    if (MY_SPEC.has_insert_clause_ && OB_FAIL(do_insert())) {
      LOG_WARN("fail to do_insert", K(ret));
    }
  }
  return ret;
}

int ObTableMergeOp::write_rows_post_proc(int last_errno)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  //iterator end, if das ref has task, need flush all task data to partition storage
  if (OB_SUCC(last_errno) && iter_end_) {
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan_ctx is null", K(ret));
    } else {
      plan_ctx->add_affected_rows(affected_rows_);
    }
  }
  return ret;
}

int ObTableMergeOp::check_is_distinct(bool &conflict)
{
  int ret = OB_SUCCESS;
  bool is_distinct = true;
  conflict = false;
  // check whether distinct  only use rowkey
  if (OB_FAIL(ObDMLService::check_rowkey_whether_distinct(MY_SPEC.distinct_key_exprs_,
                                                          DistinctType::T_HASH_DISTINCT,
                                                          get_eval_ctx(),
                                                          get_exec_ctx(),
                                                          merge_rtdefs_.at(0).table_rowkey_,
                                                          merge_rtdefs_.at(0).rowkey_dist_ctx_,
                                                          // merge_rtdefs_ length must > 0
                                                          is_distinct))) {
    LOG_WARN("fail to check row whether distinct", K(ret));
  } else if (!is_distinct) {
    conflict = true;
  }
  return ret;
}

int ObTableMergeOp::do_update()
{
  int ret = OB_SUCCESS;
  bool need_update = false;
  bool need_delete = false;
  bool is_conflict = false;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
  OZ (calc_condition(MY_SPEC.update_conds_, need_update));
  if (need_update) {
    OZ (check_is_distinct(is_conflict));
    OZ (calc_delete_condition(MY_SPEC.delete_conds_, need_delete));
    if (is_conflict) {
      ret = need_delete ? OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS : OB_ERR_UPDATE_TWICE;
    } else {
      if (need_delete) {
        // 此处调用dml_service的delete相关的处理接口
        if (OB_SUCC(ret) && OB_FAIL(delete_row_das())) {
          LOG_WARN("fail to update row by das ", K(ret));
        }
      } else {
        // 此处调用dml_service的update相关的处理接口
        if (OB_SUCC(ret) && OB_FAIL(update_row_das())) {
          LOG_WARN("fail to update row by das ", K(ret));
        }
      }
    }
  }
  return ret;
}

// when update with partition key, we will delete row from old partition first,
// then insert new row to new partition
int ObTableMergeOp::update_row_das()
{
  int ret = OB_SUCCESS;
  // 这里只设置dml_event,没有init_column_columns是因为在ObDMLService::init_upd_rtdef里已经设置了
  dml_rtctx_.get_exec_ctx().set_dml_event(ObDmlEventType::DE_UPDATING);
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.merge_ctdefs_.count(); ++i) {
    ObMergeCtDef *merge_ctdef = MY_SPEC.merge_ctdefs_.at(i);
    const ObUpdCtDef *upd_ctdef = NULL;
    bool is_skipped = false;
    ObDASTabletLoc *old_tablet_loc = nullptr;
    ObDASTabletLoc *new_tablet_loc = nullptr;
    ObUpdRtDef &upd_rtdef = merge_rtdefs_.at(i).upd_rtdef_;
    ObDMLModifyRowNode modify_row(this, merge_ctdef->upd_ctdef_, &upd_rtdef, ObDmlEventType::DE_UPDATING);
    ObChunkDatumStore::StoredRow* stored_row = nullptr;
    if (OB_ISNULL(merge_ctdef)) {
      // merge_ctdef can't be NULL
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge_ctdef can't be NULL", K(ret));
    } else if (OB_ISNULL(upd_ctdef = merge_ctdef->upd_ctdef_)) {
      // some global index table maybe not need to update
      LOG_DEBUG("this global index not need to update", K(ret), K(i));
    } else if (OB_FAIL(ObDMLService::process_update_row(
                       *upd_ctdef, upd_rtdef, is_skipped, *this))) {
      LOG_WARN("process update row failed", K(ret));
    } else if (OB_UNLIKELY(is_skipped)) {
      //this row has been skipped, so can not write to DAS buffer(include its global index)
      //so need to break this loop
      break;
    } else if (OB_FAIL(calc_update_tablet_loc(*upd_ctdef, upd_rtdef, old_tablet_loc, new_tablet_loc))) {
      LOG_WARN("calc partition key failed", K(ret));
    } else if (OB_FAIL(ObDMLService::update_row(*upd_ctdef, upd_rtdef, old_tablet_loc, new_tablet_loc, dml_rtctx_,
                                                modify_row.old_row_, modify_row.new_row_, modify_row.full_row_))) {
      LOG_WARN("insert row with das failed", K(ret));
    } else if (need_after_row_process(*upd_ctdef) && OB_FAIL(dml_modify_rows_.push_back(modify_row))) {
        LOG_WARN("failed to push dml modify row to modified row list", K(ret));
    } else {
      // do nothing
    }
  }

  if (OB_SUCC(ret)) {
    affected_rows_++;
  }
  return ret;
}

int ObTableMergeOp::delete_row_das()
{
  int ret = OB_SUCCESS;
  dml_rtctx_.get_exec_ctx().set_dml_event(ObDmlEventType::DE_DELETING);
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.merge_ctdefs_.count(); ++i) {
    ObMergeCtDef *merge_ctdef = MY_SPEC.merge_ctdefs_.at(i);
    const ObDelCtDef *del_ctdef = NULL;
    bool is_skipped = false;
    ObDASTabletLoc *tablet_loc = nullptr;
    ObDelRtDef &del_rtdef = merge_rtdefs_.at(i).del_rtdef_;
    ObDMLModifyRowNode modify_row(this, (merge_ctdef->del_ctdef_), &del_rtdef, ObDmlEventType::DE_DELETING);
    if (OB_ISNULL(merge_ctdef)) {
      // merge_ctdef can't be NULL
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge_ctdef can't be NULL", K(ret));
    } else if (OB_ISNULL(del_ctdef = merge_ctdef->del_ctdef_)) {
      // must delete all data table and index table
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("del_ctdef can't be NULL", K(ret));
    } else if (OB_FAIL(ObDMLService::process_delete_row(*del_ctdef,
                                                        del_rtdef,
                                                        is_skipped,
                                                        *this))) {
      LOG_WARN("fail to process delete row", K(ret));
    } else if (OB_UNLIKELY(is_skipped)) {
      // 这里貌似不应该出现is_skipped == true的场景
    } else if (OB_FAIL(calc_delete_tablet_loc(*del_ctdef, del_rtdef, tablet_loc))) {
      LOG_WARN("calc partition key failed", K(ret));
    } else if (OB_FAIL(ObDMLService::delete_row(*del_ctdef, del_rtdef, tablet_loc, dml_rtctx_, modify_row.old_row_))) {
      LOG_WARN("insert row with das failed", K(ret));
    } else if (need_after_row_process(*del_ctdef) && OB_FAIL(dml_modify_rows_.push_back(modify_row))) {
      LOG_WARN("failed to push dml modify row to modified row list", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    affected_rows_++;
  }
  return ret;
}

int ObTableMergeOp::do_insert()
{
  int ret = OB_SUCCESS;
  bool is_true_cond = false;
  bool is_skipped = false;
  dml_rtctx_.get_exec_ctx().set_dml_event(ObDmlEventType::DE_INSERTING);
  if (!MY_SPEC.has_insert_clause_) {
    //do nothing
    LOG_DEBUG("trace not has insert clause", K(ret));
  } else if (OB_FAIL(calc_condition(MY_SPEC.insert_conds_, is_true_cond))) {
    LOG_WARN("fail to calc condition", K(MY_SPEC.insert_conds_), K(ret));
  } else if (is_true_cond) {
    // 此处调用dml_service的insert相关的处理接口
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.merge_ctdefs_.count(); ++i) {
      ObMergeCtDef *merge_ctdef = MY_SPEC.merge_ctdefs_.at(i);
      ObInsCtDef *ins_ctdef = NULL;
      ObInsRtDef &ins_rtdef = merge_rtdefs_.at(i).ins_rtdef_;
      ObDASTabletLoc *tablet_loc = nullptr;
      ObDMLModifyRowNode modify_row(this, (merge_ctdef->ins_ctdef_), &ins_rtdef, ObDmlEventType::DE_INSERTING);
      if (OB_ISNULL(merge_ctdef)) {
        // merge_ctdef can't be NULL
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("merge_ctdef can't be NULL", K(ret));
      } else if (OB_ISNULL(ins_ctdef = merge_ctdef->ins_ctdef_)) {
        // must insert all table(包括所有全局索引表)
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ins_ctdef can't be NULL", K(ret));
      } else {
        //LOG_TRACE("insert row", "row", ROWEXPR2STR(eval_ctx_, ins_ctdef->new_row_));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObDMLService::init_heap_table_pk_for_ins(*ins_ctdef, eval_ctx_))) {
        LOG_WARN("fail to init heap table pk to null", K(ret));
      } else if (OB_FAIL(ObDMLService::process_insert_row(*ins_ctdef, ins_rtdef, *this, is_skipped))) {
        LOG_WARN("fail to process_insert_row", K(ret));
      } else if (OB_UNLIKELY(is_skipped)) {
        break;
      } else if (OB_FAIL(calc_insert_tablet_loc(*ins_ctdef, ins_rtdef, tablet_loc))) {
        LOG_WARN("calc partition key failed", K(ret));
      } else if (OB_FAIL(ObDMLService::set_heap_table_hidden_pk(*ins_ctdef,
                                                                tablet_loc->tablet_id_,
                                                                eval_ctx_))) {
        LOG_WARN("set_heap_table_hidden_pk failed", K(ret), KPC(tablet_loc), KPC(ins_ctdef));
      } else if (OB_FAIL(ObDMLService::insert_row(*ins_ctdef, ins_rtdef, tablet_loc, dml_rtctx_, modify_row.new_row_))) {
        LOG_WARN("insert row with das failed", K(ret));
      // TODO(yikang): fix trigger related for heap table
      } else if (need_after_row_process(*ins_ctdef) && OB_FAIL(dml_modify_rows_.push_back(modify_row))) {
        LOG_WARN("failed to push dml modify row to modified row list", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      affected_rows_++;
    }
  }
  return ret;
}

int ObTableMergeOp::calc_update_tablet_loc(const ObUpdCtDef &upd_ctdef,
                                           ObUpdRtDef &upd_rtdef,
                                           ObDASTabletLoc *&old_tablet_loc,
                                           ObDASTabletLoc *&new_tablet_loc)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.use_dist_das_) {
    ObTabletID old_tablet_id;
    ObTabletID new_tablet_id;
    if (upd_ctdef.multi_ctdef_ != nullptr) {
      ObExpr *calc_part_id_old = upd_ctdef.multi_ctdef_->calc_part_id_old_;
      ObExpr *calc_part_id_new = upd_ctdef.multi_ctdef_->calc_part_id_new_;
      ObObjectID old_partition_id = OB_INVALID_ID;
      ObObjectID new_partition_id = OB_INVALID_ID;
      if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(calc_part_id_old, eval_ctx_, old_partition_id, old_tablet_id))) {
        LOG_WARN("calc part and tablet id by expr failed", K(ret));
      } else if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(calc_part_id_new, eval_ctx_, new_partition_id, new_tablet_id))) {
        LOG_WARN("calc part and tablet id by expr failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObDASTableLoc &table_loc = *upd_rtdef.dupd_rtdef_.table_loc_;
      if (old_tablet_id == new_tablet_id) {
        if (OB_FAIL(DAS_CTX(ctx_).extended_tablet_loc(table_loc, old_tablet_id, old_tablet_loc))) {
          LOG_WARN("extended old row tablet loc failed", K(ret), K(old_tablet_id));
        } else {
          new_tablet_loc = old_tablet_loc;
        }
      } else if (OB_FAIL(DAS_CTX(ctx_).extended_tablet_loc(table_loc, old_tablet_id, old_tablet_loc))) {
        LOG_WARN("extended old tablet location failed", K(ret), K(old_tablet_id));
      } else if (OB_FAIL(DAS_CTX(ctx_).extended_tablet_loc(table_loc, new_tablet_id, new_tablet_loc))) {
        LOG_WARN("extended new tablet location failed", K(ret), K(new_tablet_id));
      }
    }
  } else {
    //direct write delete row to storage
    old_tablet_loc = MY_INPUT.get_tablet_loc();
    new_tablet_loc = old_tablet_loc;
  }
  return ret;
}

int ObTableMergeOp::calc_insert_tablet_loc(const ObInsCtDef &ins_ctdef,
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
      } else if (!ins_ctdef.multi_ctdef_->hint_part_ids_.empty()
          && !has_exist_in_array(ins_ctdef.multi_ctdef_->hint_part_ids_, partition_id)) {
        ret = OB_PARTITION_NOT_MATCH;
        LOG_DEBUG("Partition not match", K(ret),
                  K(partition_id), K(ins_ctdef.multi_ctdef_->hint_part_ids_));
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

int ObTableMergeOp::calc_delete_tablet_loc(const ObDelCtDef &del_ctdef,
                                           ObDelRtDef &del_rtdef,
                                           ObDASTabletLoc *&tablet_loc)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.use_dist_das_) {
    if (del_ctdef.multi_ctdef_ != nullptr) {
      ObExpr *calc_part_id_expr = del_ctdef.multi_ctdef_->calc_part_id_expr_;
      ObObjectID partition_id = OB_INVALID_ID;
      ObTabletID tablet_id;
      ObDASTableLoc &table_loc = *del_rtdef.das_rtdef_.table_loc_;
      if (OB_FAIL(ObExprCalcPartitionBase::calc_part_and_tablet_id(calc_part_id_expr, eval_ctx_, partition_id, tablet_id))) {
        LOG_WARN("calc part and tablet id by expr failed", K(ret));
      } else if (OB_FAIL(DAS_CTX(ctx_).extended_tablet_loc(table_loc, tablet_id, tablet_loc))) {
        LOG_WARN("extended tablet loc failed", K(ret));
      }
    }
  } else {
    //direct write delete row to storage
    tablet_loc = MY_INPUT.get_tablet_loc();
  }
  return ret;
}
}//sql
}//oceanbase
