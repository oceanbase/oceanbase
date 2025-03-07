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
#include "sql/engine/dml/ob_table_replace_op.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/das/ob_das_insert_op.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;

namespace sql
{
OB_SERIALIZE_MEMBER((ObTableReplaceOpInput, ObTableModifyOpInput));

OB_DEF_SERIALIZE(ObTableReplaceSpec)
{
  int ret = OB_SUCCESS;
  int64_t index_cnt = replace_ctdefs_.count();
  BASE_SER((ObTableReplaceSpec, ObTableModifySpec));
  OB_UNIS_ENCODE(index_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; ++i) {
    ObReplaceCtDef *replace_ctdef = replace_ctdefs_.at(i);
    if (OB_ISNULL(replace_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replace_ctdef is nullptr", K(ret));
    }
    OB_UNIS_ENCODE(*replace_ctdef);
  }
  OB_UNIS_ENCODE(only_one_unique_key_);
  OB_UNIS_ENCODE(conflict_checker_ctdef_);
  OB_UNIS_ENCODE(has_global_unique_index_);
  OB_UNIS_ENCODE(all_saved_exprs_);
  OB_UNIS_ENCODE(doc_id_col_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTableReplaceSpec)
{
  int ret = OB_SUCCESS;
  int64_t index_cnt = 0;
  BASE_DESER((ObTableReplaceSpec, ObTableModifySpec));
  OB_UNIS_DECODE(index_cnt);
  OZ(replace_ctdefs_.allocate_array(alloc_, index_cnt));
  ObDMLCtDefAllocator<ObReplaceCtDef> replace_ctdef_allocator(alloc_);
  for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; ++i) {
    ObReplaceCtDef *replace_ctdef = replace_ctdef_allocator.alloc();
    if (OB_ISNULL(replace_ctdef)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc insert_up_ctdef failed", K(ret));
    }
    OB_UNIS_DECODE(*replace_ctdef);
    replace_ctdefs_.at(i) = replace_ctdef;
  }
  OB_UNIS_DECODE(only_one_unique_key_);
  OB_UNIS_DECODE(conflict_checker_ctdef_);
  OB_UNIS_DECODE(has_global_unique_index_);
  OB_UNIS_DECODE(all_saved_exprs_);
  OB_UNIS_DECODE(doc_id_col_id_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableReplaceSpec)
{
  int64_t len = 0;
  int64_t index_cnt = replace_ctdefs_.count();
  BASE_ADD_LEN((ObTableReplaceSpec, ObTableModifySpec));
  OB_UNIS_ADD_LEN(index_cnt);
  for (int64_t i = 0; i < index_cnt; ++i) {
    ObReplaceCtDef *replace_ctdef = replace_ctdefs_.at(i);
    if (replace_ctdef != nullptr) {
      OB_UNIS_ADD_LEN(*replace_ctdef);
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "replace_ctdef is null unexpected");
    }
  }
  OB_UNIS_ADD_LEN(only_one_unique_key_);
  OB_UNIS_ADD_LEN(conflict_checker_ctdef_);
  OB_UNIS_ADD_LEN(has_global_unique_index_);
  OB_UNIS_ADD_LEN(all_saved_exprs_);
  OB_UNIS_ADD_LEN(doc_id_col_id_);
  return len;
}

ObDasParallelType ObTableReplaceOp::check_das_parallel_type()
{
  return DAS_SERIALIZATION;
}


int ObTableReplaceOp::check_need_exec_single_row()
{
  int ret = OB_SUCCESS;
  ret = ObTableModifyOp::check_need_exec_single_row();
  if (OB_SUCC(ret) && !execute_single_row_) {
    ObReplaceCtDef *replace_ctdef = MY_SPEC.replace_ctdefs_.at(0);
    const ObInsCtDef *ins_ctdef = replace_ctdef->ins_ctdef_;
    const ObDelCtDef *del_ctdef = replace_ctdef->del_ctdef_;
    if (OB_NOT_NULL(ins_ctdef)) {
      for (int64_t j = 0;
          OB_SUCC(ret) && !execute_single_row_ && j < ins_ctdef->trig_ctdef_.tg_args_.count();
          ++j) {
        const ObTriggerArg &tri_arg = ins_ctdef->trig_ctdef_.tg_args_.at(j);
        execute_single_row_ = tri_arg.is_execute_single_row();
      }
    }
    if (OB_NOT_NULL(del_ctdef)) {
      for (int64_t j = 0;
          OB_SUCC(ret) && !execute_single_row_ && j < del_ctdef->trig_ctdef_.tg_args_.count();
          ++j) {
        const ObTriggerArg &tri_arg = del_ctdef->trig_ctdef_.tg_args_.at(j);
        execute_single_row_ = tri_arg.is_execute_single_row();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ins_ctdef or del_ctdef of primary table is nullptr", K(ret));
    }
  }
  return ret;
}

int ObTableReplaceOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
  NG_TRACE(replace_open);
  if (OB_FAIL(check_replace_ctdefs_valid())) {
    LOG_WARN("replace ctdef is invalid", K(ret));
  } else if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("inner open ObTableModifyOp failed", K(ret));
  } else if (OB_UNLIKELY(MY_SPEC.replace_ctdefs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ins ctdef is invalid", K(ret), KP(this));
  } else if (OB_UNLIKELY(iter_end_)) {
    //do nothing
  } else if (OB_FAIL(replace_row_store_.init(UINT64_MAX,
                                             my_session->get_effective_tenant_id(),
                                             ObCtxIds::DEFAULT_CTX_ID,
                                             "replace_row_store",
                                             false/*enable_dump*/))) {
    LOG_WARN("fail to init replace row store", K(ret));
  } else if (OB_FAIL(inner_open_with_das())) {
    LOG_WARN("inner open with das failed", K(ret));
  } else {
    conflict_checker_.set_local_tablet_loc(MY_INPUT.get_tablet_loc());
  }
  return ret;
}

OB_INLINE int ObTableReplaceOp::inner_open_with_das()
{
  int ret = OB_SUCCESS;
  const ObExprFrameInfo *expr_frame_info = NULL;
  ObDASTableLoc *table_loc = nullptr;
  bool use_partition_gts_opt = false;
  expr_frame_info = nullptr != MY_SPEC.expr_frame_info_
                               ? MY_SPEC.expr_frame_info_
                               : &MY_SPEC.plan_->get_expr_frame_info();
  if (ctx_.get_das_ctx().get_use_gts_opt()) {
    gts_state_ = MY_SPEC.has_global_unique_index_ == true ?
        WITH_UNIQUE_GLOBAL_INDEX_STATE : USE_PARTITION_SNAPSHOT_STATE;
    if (gts_state_ == USE_PARTITION_SNAPSHOT_STATE) {
      dml_rtctx_.das_ref_.set_do_gts_opt(true);
      replace_rtctx_.das_ref_.set_do_gts_opt(true);
      use_partition_gts_opt = true;
    }
  }
  if (OB_FAIL(init_replace_rtdef())) {
    LOG_WARN("init replace rtdef failed", K(ret), K(MY_SPEC.replace_ctdefs_.count()));
  } else if (OB_ISNULL(table_loc = replace_rtdefs_.at(0).ins_rtdef_.das_rtdef_.table_loc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table location is invalid", K(ret));
  } else if (OB_FAIL(conflict_checker_.init_conflict_checker(expr_frame_info,
                                                             table_loc,
                                                             use_partition_gts_opt))) {
    LOG_WARN("init conflict_checker fail", K(ret), K(use_partition_gts_opt));
  } else {
    // init update das_ref
    ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
    ObMemAttr mem_attr;
    mem_attr.tenant_id_ = session->get_effective_tenant_id();
    mem_attr.label_ = "SqlReplaceInto";
    replace_rtctx_.das_ref_.set_expr_frame_info(expr_frame_info);
    replace_rtctx_.das_ref_.set_mem_attr(mem_attr);
    replace_rtctx_.das_ref_.set_execute_directly(!MY_SPEC.use_dist_das_);
    ObDasParallelType type = ObTableModifyOp::check_das_parallel_type();
    if (DAS_SERIALIZATION != type) {
      type = DAS_BLOCKING_PARALLEL;
      LOG_TRACE("this sql use das parallel submit for replace", K(check_das_parallel_type()));
    }
    replace_rtctx_.das_ref_.get_das_parallel_ctx().set_das_parallel_type(type);
    replace_rtctx_.das_ref_.get_das_parallel_ctx().set_das_dop(ctx_.get_das_ctx().get_real_das_dop());
  }
  return ret;
}

OB_INLINE int ObTableReplaceOp::init_replace_rtdef()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_rtdefs_.allocate_array(ctx_.get_allocator(), MY_SPEC.replace_ctdefs_.count()))) {
    LOG_WARN("allocate insert rtdef failed", K(ret), K(MY_SPEC.replace_ctdefs_.count()));
  }
  trigger_clear_exprs_.reset();
  fk_checkers_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < replace_rtdefs_.count(); ++i) {
    ObReplaceCtDef *replace_ctdef = MY_SPEC.replace_ctdefs_.at(i);
    const ObInsCtDef *ins_ctdef = replace_ctdef->ins_ctdef_;
    const ObDelCtDef *del_ctdef = replace_ctdef->del_ctdef_;
    ObInsRtDef &ins_rtdef = replace_rtdefs_.at(i).ins_rtdef_;
    ObDelRtDef &del_rtdef = replace_rtdefs_.at(i).del_rtdef_;
    OZ(ObDMLService::init_ins_rtdef(dml_rtctx_, ins_rtdef, *ins_ctdef, trigger_clear_exprs_, fk_checkers_));
    OZ(ObDMLService::init_del_rtdef(dml_rtctx_, del_rtdef, *del_ctdef));
    if (OB_SUCC(ret)) {
      ins_rtdef.das_rtdef_.table_loc_->is_writing_ = !(ins_ctdef->is_single_value_);
    }
  }
  return ret;
}

int ObTableReplaceOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_rescan())) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else {
    conflict_checker_.set_local_tablet_loc(MY_INPUT.get_tablet_loc());
    if (replace_rtctx_.das_ref_.has_task()) {
        if (OB_FAIL(replace_rtctx_.das_ref_.close_all_task())) {
          LOG_WARN("close all update das task failed", K(ret));
        } else {
          replace_rtctx_.reuse();
        }
      }
    }

  if (OB_SUCC(ret)) {
    replace_rtdefs_.release_array();
    if (OB_UNLIKELY(iter_end_)) {
      //do nothing
    } else if (OB_FAIL(init_replace_rtdef())) {
      LOG_WARN("init replace rtdef failed", K(ret));
    } else if (OB_FAIL(reuse())) {
      LOG_WARN("reuse op fail", K(ret));
    }
  }
  return ret;
}

int ObTableReplaceOp::inner_close()
{
  int ret = OB_SUCCESS;
  int close_ret = OB_SUCCESS;
  NG_TRACE(replace_inner_close);
  if (OB_FAIL(conflict_checker_.close())) {
    LOG_WARN("fail to close conflict_checker", K(ret));
  }
  if (replace_rtctx_.das_ref_.has_task()) {
    close_ret = (replace_rtctx_.das_ref_.close_all_task());
    if (OB_SUCCESS == close_ret) {
      replace_rtctx_.das_ref_.reset();
    }
  }
  ret = OB_SUCCESS == ret ? close_ret : ret;
  // close dml das tasks
  close_ret = ObTableModifyOp::inner_close();
  return (OB_SUCCESS == ret) ? close_ret : ret;
}

int ObTableReplaceOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(MY_SPEC.id_), K(iter_end_));
    ret = OB_ITER_END;
  } else {
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("check status failed", K(ret));
    } else if (OB_FAIL(do_replace_into())) {
      LOG_WARN("fail to do replace into", K(ret));
    } else if (OB_FAIL(plan_ctx->sync_last_value_local())) {
      LOG_WARN("failed to sync value globally", K(ret));
    } else {
      plan_ctx->set_row_matched_count(insert_rows_);
      plan_ctx->set_affected_rows(insert_rows_ + delete_rows_);
      plan_ctx->set_row_duplicated_count(delete_rows_);
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

OB_INLINE int ObTableReplaceOp::get_next_row_from_child()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else {
    insert_rows_++;
    LOG_TRACE("child output row", "output row", ROWEXPR2STR(eval_ctx_, child_->get_spec().output_));
  }
  return ret;
}

OB_INLINE int ObTableReplaceOp::load_all_replace_row(bool &is_iter_end)
{
  int ret = OB_SUCCESS;
  is_iter_end = false;
  bool reach_mem_limit = false;
  ObInsCtDef *primary_ins_ctdef = NULL;
  int64_t row_cnt = 0;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  int64_t simulate_batch_count = - EVENT_CALL(EventTable::EN_TABLE_REPLACE_BATCH_ROW_COUNT);
  int64_t default_row_store_mem_limit = OB_DEFAULT_REPLACE_MEMORY_LIMIT;
  if (replace_rtctx_.das_ref_.get_parallel_type() != DAS_SERIALIZATION) {
    default_row_store_mem_limit = OB_DEFAULT_REPLACE_MEMORY_LIMIT * 5; // 10M
  }
  NG_TRACE_TIMES(2, replace_start_load_row);

  while (OB_SUCC(ret) && !reach_mem_limit) {
    // todo @kaizhan.dkz @wangbo.wb 增加行前trigger逻辑在这里
    // 新行的外键检查也在这里做
    bool is_skipped = false;
    if (OB_FAIL(get_next_row_from_child())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to load next row from child", K(ret));
      }
    } else if (OB_FAIL(insert_row_to_das(is_skipped))) {
      LOG_WARN("insert row to das", K(ret));
    } else if (get_all_saved_exprs().empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty", K(ret));
    } else if (OB_FAIL(!is_skipped && replace_row_store_.add_row(get_all_saved_exprs(), &eval_ctx_))) {
      LOG_WARN("add replace row to row store failed", K(ret));
    } else {
      row_cnt++;
      plan_ctx->record_last_insert_id_cur_stmt();
      if (replace_row_store_.get_mem_used() >= default_row_store_mem_limit ||
          execute_single_row_) {
        reach_mem_limit = true;
        LOG_TRACE("replace into rows used memory over limit", K(ret), K(row_cnt),
                      K(default_row_store_mem_limit), K(replace_row_store_.get_mem_used()), K(execute_single_row_));
      } else if (simulate_batch_count != 0 && row_cnt > simulate_batch_count) {
        reach_mem_limit = true;
        LOG_TRACE("replace into rows count reach simulate_batch_count", K(ret),
            K(row_cnt), K(simulate_batch_count), K(replace_row_store_.get_mem_used()));
      }
      // record for insertup batch_dml_optimization
      int64_t insert_row = is_skipped ? 0 : 1;
      if (OB_FAIL(merge_implict_cursor(insert_row, 0, 0, 0))) {
        LOG_WARN("merge implict cursor failed", K(ret));
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    is_iter_end = true;
  }
  return ret;
}

int ObTableReplaceOp::final_insert_row_to_das()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.replace_ctdefs_.count(); ++i) {
    // insert each table with fetched row
    const ObReplaceCtDef &replace_ctdef = *(MY_SPEC.replace_ctdefs_.at(i));
    const ObInsCtDef &ins_ctdef = *(replace_ctdef.ins_ctdef_);
    ObReplaceRtDef &replace_rtdef = replace_rtdefs_.at(i);
    ObInsRtDef &ins_rtdef = replace_rtdef.ins_rtdef_;
    ObDASTabletLoc *tablet_loc = nullptr;
    ObChunkDatumStore::StoredRow *stored_row_ = nullptr;
    ++ins_rtdef.cur_row_num_;
    if (OB_FAIL(calc_insert_tablet_loc(ins_ctdef, ins_rtdef, tablet_loc))) {
      LOG_WARN("calc partition key failed", K(ret));
    } else if (OB_FAIL(ObDMLService::insert_row(ins_ctdef, ins_rtdef, tablet_loc, replace_rtctx_, stored_row_))) {
      LOG_WARN("insert row with das failed", K(ret));
    } else {
      LOG_TRACE("final insert one row", KPC(tablet_loc), "ins row", ROWEXPR2STR(eval_ctx_, ins_ctdef.new_row_));
    }
  }
  return ret;
}

int ObTableReplaceOp::insert_row_to_das(bool &is_skipped)
{
  int ret = OB_SUCCESS;
  is_skipped = false;
  // 尝试写入数据到所有的表
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.replace_ctdefs_.count(); ++i) {
    // insert each table with fetched row
    const ObReplaceCtDef &replace_ctdef = *(MY_SPEC.replace_ctdefs_.at(i));
    const ObInsCtDef &ins_ctdef = *(replace_ctdef.ins_ctdef_);
    ObReplaceRtDef &replace_rtdef = replace_rtdefs_.at(i);
    ObInsRtDef &ins_rtdef = replace_rtdef.ins_rtdef_;
    ObDASTabletLoc *tablet_loc = nullptr;
    ObDMLModifyRowNode modify_row(this, &ins_ctdef, &ins_rtdef, ObDmlEventType::DE_INSERTING);
    ++ins_rtdef.cur_row_num_;
    if (OB_FAIL(ObDMLService::init_heap_table_pk_for_ins(ins_ctdef, eval_ctx_))) {
      LOG_WARN("fail to init heap table pk to null", K(ret));
    } else if (OB_FAIL(ObDMLService::process_insert_row(ins_ctdef, ins_rtdef, *this, is_skipped))) {
      LOG_WARN("process insert row failed", K(ret));
    } else if (OB_UNLIKELY(is_skipped)) {
      break;
    } else if (OB_FAIL(calc_insert_tablet_loc(ins_ctdef, ins_rtdef, tablet_loc))) {
      LOG_WARN("calc partition key failed", K(ret));
    } else if (OB_FAIL(ObDMLService::set_heap_table_hidden_pk(ins_ctdef, tablet_loc->tablet_id_, eval_ctx_))) {
      LOG_WARN("set_heap_table_hidden_pk failed", K(ret), KPC(tablet_loc), K(ins_ctdef));
    } else if (OB_FAIL(ObDMLService::insert_row(ins_ctdef, ins_rtdef, tablet_loc, dml_rtctx_, modify_row.new_row_))) {
      LOG_WARN("insert row with das failed", K(ret));
    // TODO(yikang): fix trigger related for heap table
    } else if (need_after_row_process(ins_ctdef) && OB_FAIL(dml_modify_rows_.push_back(modify_row))) {
        LOG_WARN("failed to push dml modify row to modified row list", K(ret));
    } else {
      LOG_TRACE("insert one row", KPC(tablet_loc), "ins row", ROWEXPR2STR(eval_ctx_, ins_ctdef.new_row_));
    }
  }
  return ret;
}

int ObTableReplaceOp::delete_row_to_das(bool need_do_trigger)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.replace_ctdefs_.count(); ++i) {
    bool is_skipped = false;
    ObDASTabletLoc *tablet_loc = nullptr;
    const ObReplaceCtDef &replace_ctdef = *(MY_SPEC.replace_ctdefs_.at(i));
    const ObDelCtDef &del_ctdef = *(replace_ctdef.del_ctdef_);
    ObReplaceRtDef &replace_rtdef = replace_rtdefs_.at(i);
    ObDelRtDef &del_rtdef = replace_rtdef.del_rtdef_;
    ObChunkDatumStore::StoredRow *stored_row = nullptr;
    if (need_do_trigger &&
        OB_FAIL(ObDMLService::process_delete_row(del_ctdef, del_rtdef, is_skipped, *this))) {
      LOG_WARN("process delete row failed", K(ret));
    } else if (OB_UNLIKELY(is_skipped)) {
      //this row has been skipped, so can not write to DAS buffer(include its global index)
      //so need to break this loop
      break;
    } else if (OB_FAIL(calc_delete_tablet_loc(del_ctdef, del_rtdef, tablet_loc))) {
      LOG_WARN("calc partition key failed", K(ret));
    } else if (OB_FAIL(ObDMLService::delete_row(del_ctdef, del_rtdef, tablet_loc, replace_rtctx_, stored_row))) {
      LOG_WARN("insert row with das failed", K(ret));
    } else {
      LOG_DEBUG("delete one row", KPC(tablet_loc), "del row", ROWEXPR2STR(eval_ctx_, del_ctdef.old_row_));
    }
  }
  return ret;
}

int ObTableReplaceOp::fetch_conflict_rowkey(int64_t replace_row_cnt)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  NG_TRACE_TIMES(2, replace_build_fetch_rowkey);
  DASTaskIter task_iter = dml_rtctx_.das_ref_.begin_task_iter();
  // 暂时拍脑袋定个100行
  if (replace_row_cnt > ObConflictCheckerCtdef::MIN_ROW_COUNT_USE_HASHSET_DO_DISTICT) {
    if (OB_FAIL(conflict_checker_.create_rowkey_check_hashset(replace_row_cnt))) {
      LOG_WARN("fail to create conflict_checker hash_set", K(ret), K(replace_row_cnt));
    }
  }

  if (gts_state_ == USE_PARTITION_SNAPSHOT_STATE) {
    DASTaskIter task_iter = dml_rtctx_.das_ref_.begin_task_iter();
    while (OB_SUCC(ret) && !task_iter.is_end()) {
      ObDASInsertOp *ins_op = static_cast<ObDASInsertOp*>(*task_iter);
      const ObDASInsCtDef *ins_ctdef = static_cast<const ObDASInsCtDef*>(ins_op->get_ctdef());
        transaction::ObTxReadSnapshot *snapshot = ins_op->get_das_gts_opt_info().get_response_snapshot();
        if (OB_ISNULL(snapshot)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (!snapshot->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected invalid snaopshot", K(ret), KPC(snapshot));
        } else if (OB_FAIL(conflict_checker_.collect_all_snapshot(*snapshot, ins_op->get_tablet_loc()))) {
          LOG_WARN("fail to collect snapshot", K(ret), KPC(snapshot), KPC(ins_op));
        }
      ++task_iter;
    }
  }
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

int ObTableReplaceOp::get_next_conflict_rowkey(DASTaskIter &task_iter)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  while (OB_SUCC(ret) && !got_row) {
    ObDatumRow *dup_row = nullptr;
    ObChunkDatumStore::StoredRow *stored_row = nullptr;
    ObDASWriteBuffer::DmlShadowRow ssr;
    ObDASInsertOp *ins_op = static_cast<ObDASInsertOp*>(*task_iter);
    ObDatumRowIterator *conflict_result = ins_op->get_duplicated_result();
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
    } else if (OB_FAIL(stored_row->to_expr(conflict_checker_.checker_ctdef_.data_table_rowkey_expr_,
                                           conflict_checker_.eval_ctx_))) {
      LOG_WARN("get next row from result iterator failed", K(ret));
    } else {
      got_row = true;
    }
  }
  return ret;
}

int ObTableReplaceOp::post_all_try_insert_das_task(ObDMLRtCtx &dml_rtctx)
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, replace_try_insert);
  if (dml_rtctx.das_ref_.has_task()) {
    if (gts_state_ == WITH_UNIQUE_GLOBAL_INDEX_STATE) {
      share::ObLSID ls_id;
      ObSQLSessionInfo *my_session = GET_MY_SESSION(ctx_);
      ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
      if (dml_rtctx.das_ref_.check_tasks_same_ls_and_is_local(ls_id)) {
        if (OB_FAIL(ObSqlTransControl::get_ls_read_snapshot(my_session,
                                                            plan_ctx,
                                                            ls_id,
                                                            ctx_.get_das_ctx().get_snapshot()))) {
          LOG_WARN("fail to get ls read snapshot", K(ret));
        } else {
          LOG_TRACE("all tasks with same ls_id, get ls snapshot", K(ls_id), K(ctx_.get_das_ctx().get_snapshot()));
        }
      } else {
        if (OB_FAIL(ObSqlTransControl::get_read_snapshot(my_session,
                                                         plan_ctx,
                                                         ctx_.get_das_ctx().get_snapshot()))) {
          LOG_WARN("fail to get global read snapshot", K(ret));
        } else {
          gts_state_ = GTE_GTS_STATE;
          LOG_TRACE("task in different ls_id, get gts", K(ctx_.get_das_ctx().get_snapshot()));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml_rtctx.das_ref_.execute_all_task())) {
        LOG_WARN("execute all delete das task failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableReplaceOp::post_all_dml_das_task(ObDMLRtCtx &dml_rtctx)
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, replace_final_write);

  if (dml_rtctx.das_ref_.has_task()) {
    if (gts_state_ == USE_PARTITION_SNAPSHOT_STATE) {
      if (OB_FAIL(conflict_checker_.set_partition_snapshot_for_das_task(dml_rtctx.das_ref_))) {
        LOG_WARN("fail to set partition snapshot", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(dml_rtctx.das_ref_.execute_all_task())) {
      LOG_WARN("execute all delete das task failed", K(ret));
    }
  }
  return ret;
}

int ObTableReplaceOp::rollback_savepoint(const transaction::ObTxSEQ &savepoint_no)
{
  int ret = OB_SUCCESS;
  NG_TRACE_TIMES(2, replace_start_rollback);
  if (OB_FAIL(ObSqlTransControl::rollback_savepoint(ctx_, savepoint_no))) {
    LOG_WARN("fail to rollback to save_point", K(ret), K(savepoint_no));
  }
  return ret;
}

int ObTableReplaceOp::do_replace_into()
{
  int ret = OB_SUCCESS;
  bool is_iter_end = false;
  while (OB_SUCC(ret) && !is_iter_end) {
    transaction::ObTxSEQ savepoint_no;
    int64_t start_time = 0;
    int64_t end_time = 0;
    // must set conflict_row fetch flag
    add_need_conflict_result_flag();
    if (OB_FAIL(ObSqlTransControl::create_anonymous_savepoint(ctx_, savepoint_no))) {
      LOG_WARN("fail to create save_point", K(ret));
    } else if (OB_FAIL(load_all_replace_row(is_iter_end))) {
      LOG_WARN("fail to load all row", K(ret));
    } else if (OB_FAIL(post_all_try_insert_das_task(dml_rtctx_))) {
      LOG_WARN("fail to post all das task", K(ret));
    } else if (!check_is_duplicated() && OB_FAIL(ObDMLService::handle_after_row_processing(this, &dml_modify_rows_))) {
      LOG_WARN("try insert is not duplicated, failed to process foreign key handle", K(ret));
    } else if (!check_is_duplicated()) {
      LOG_DEBUG("try insert is not duplicated", K(ret));
    } else if (OB_FAIL(fetch_conflict_rowkey(replace_row_store_.get_row_cnt()))) {
      LOG_WARN("fail to fetch conflict row", K(ret));
    } else if (OB_FAIL(reset_das_env())) {
      // 这里需要reuse das 相关信息
      LOG_WARN("fail to reset das env", K(ret));
    } else if (OB_FAIL(rollback_savepoint(savepoint_no))) {
      // 本次插入存在冲突, 回滚到save_point
      LOG_WARN("fail to rollback to save_point", K(ret), K(savepoint_no));
    } else if (OB_FAIL(conflict_checker_.do_lookup_and_build_base_map(replace_row_store_.get_row_cnt()))) {
      LOG_WARN("fail to do table lookup", K(ret));
    } else if (OB_FAIL(replace_conflict_row_cache())) {
      LOG_WARN("fail to shuff all replace row", K(ret));
    } else if (OB_FAIL(prepare_final_replace_task())) {
      LOG_WARN("fail to prepare final das task", K(ret));
    } else if (OB_FAIL(post_all_dml_das_task(replace_rtctx_))) {
      LOG_WARN("do insert rows post process failed", K(ret));
    } else if (OB_FAIL(ObDMLService::handle_after_row_processing(this, &dml_modify_rows_))) {
      LOG_WARN("try insert is duplicated, failed to process foreign key handle", K(ret));
    }

    if (OB_SUCC(ret) && !is_iter_end) {
      // 只有还有下一个batch时才需要做reuse，如果没有下一个batch，close和destroy中会释放内存
      // 前边逻辑执行成功，这一批batch成功完成replace, reuse环境, 准备下一个batch
      if (OB_FAIL(reuse())) {
        LOG_WARN("reuse failed", K(ret));
      }
    }
  }
  return ret;
}

bool ObTableReplaceOp::check_is_duplicated()
{
  int bret = false;
  for (int64_t i = 0; i < replace_rtdefs_.count(); ++i) {
    ObReplaceRtDef &replace_rtdef = replace_rtdefs_.at(i);
    ObDASInsRtDef &ins_rtdef = replace_rtdef.ins_rtdef_.das_rtdef_;
    if (ins_rtdef.is_duplicated_) {
      bret = true;
    }
  }
  return bret;
}

int ObTableReplaceOp::replace_conflict_row_cache()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObConflictValue, 2> constraint_values;
  ObChunkDatumStore::Iterator replace_row_iter;
  bool skip_delete = false; // useless, only meet the function entry requirements
  bool is_skipped = false;
  const ObReplaceCtDef &replace_ctdef = *(MY_SPEC.replace_ctdefs_.at(0));
  const ObInsCtDef &ins_ctdef = *(replace_ctdef.ins_ctdef_);
  const ObDelCtDef &del_ctdef = *(replace_ctdef.del_ctdef_);
  ObReplaceRtDef &replace_rtdef = replace_rtdefs_.at(0);
  ObInsRtDef &ins_rtdef = replace_rtdef.ins_rtdef_;
  ObDelRtDef &del_rtdef = replace_rtdef.del_rtdef_;
  const ObChunkDatumStore::StoredRow *stored_row = NULL;

  NG_TRACE_TIMES(2, replace_start_shuff);
  if (OB_FAIL(replace_row_store_.begin(replace_row_iter))) {
    LOG_WARN("begin replace_row_store failed", K(ret), K(replace_row_store_.get_row_cnt()));
  }
  // 构建冲突的hash map的时候也使用的是column_ref， 回表也是使用的column_ref expr来读取scan的结果
  // 因为constarain_info中使用的column_ref expr，所以此处需要使用table_column_old_exprs (column_ref exprs)
  while (OB_SUCC(ret) && OB_SUCC(replace_row_iter.get_next_row(stored_row))) {
    int64_t delete_rows = 0;
    int64_t insert_rows = 0;
    constraint_values.reuse();
    ObChunkDatumStore::StoredRow *insert_new_row = NULL;
    if (OB_ISNULL(stored_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replace_row is null", K(ret));
    } else if (OB_FAIL(stored_row->to_expr(get_all_saved_exprs(), eval_ctx_))) {
      LOG_WARN("flush replace_row to exprs failed", K(ret), KPC(stored_row));
    } else if (OB_FAIL(conflict_checker_.convert_exprs_to_stored_row(get_primary_table_new_row(),
                                                                     insert_new_row))) {
      LOG_WARN("convert exprs to stored_row failed", K(ret), KPC(insert_new_row));
    } else if (OB_FAIL(conflict_checker_.check_duplicate_rowkey(insert_new_row,
                                                                constraint_values,
                                                                false))) {
      LOG_WARN("check rowkey from conflict_checker failed", K(ret), KPC(insert_new_row));
    } else {
      insert_rows++;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < constraint_values.count(); ++i) {
      //delete duplicated row
      const ObChunkDatumStore::StoredRow *delete_row = constraint_values.at(i).current_datum_row_;
      ObDMLModifyRowNode modify_row(this, &del_ctdef, &del_rtdef, ObDmlEventType::DE_DELETING);
      bool same_row = false;
      if (OB_ISNULL(delete_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("delete row failed", K(ret));
      } else if (OB_FAIL(delete_row->to_expr(get_primary_table_old_row(), eval_ctx_))) {
        // dup checker依赖table column exprs
        LOG_WARN("flush delete_row to old_row failed", K(ret), KPC(delete_row), K(get_primary_table_old_row()));
      } else if (OB_FAIL(ObDMLService::process_delete_row(del_ctdef, del_rtdef, skip_delete, *this))) {
        LOG_WARN("process delete row failed", K(ret), KPC(delete_row), K(get_primary_table_old_row()));
      } else if (OB_FAIL(conflict_checker_.delete_old_row(delete_row, ObNewRowSource::FROM_INSERT))) {
        LOG_WARN("delete old_row from conflict checker failed", K(ret), KPC(delete_row));
      } else if (MY_SPEC.only_one_unique_key_) {
        if (OB_FAIL(check_values(same_row, insert_new_row, delete_row))) {
          LOG_WARN("check value failed", K(ret), KPC(insert_new_row), KPC(delete_row));
        }
      }
      if (OB_SUCC(ret)) {
        modify_row.old_row_ = const_cast<ObChunkDatumStore::StoredRow *>(delete_row);
        if (need_after_row_process(del_ctdef) && OB_FAIL(dml_modify_rows_.push_back(modify_row))) {
          LOG_WARN("failed to push dml modify row to modified row list", K(ret));
        }
      }
      if (OB_SUCC(ret) && !same_row) {
        delete_rows++;
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObDMLService::process_insert_row(ins_ctdef, ins_rtdef, *this, is_skipped))) {
      LOG_WARN("convert exprs to stored_row failed", K(ret), KPC(insert_new_row));
    } else if (OB_UNLIKELY(is_skipped)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected skipped", K(ret), KPC(insert_new_row));
    } else if (OB_FAIL(conflict_checker_.insert_new_row(insert_new_row, ObNewRowSource::FROM_INSERT))) {
      LOG_WARN("insert new to conflict_checker failed", K(ret), KPC(insert_new_row));
    }
    if (OB_SUCC(ret)) {
      ObDMLModifyRowNode modify_row(this, &ins_ctdef, &ins_rtdef, ObDmlEventType::DE_INSERTING);
      modify_row.new_row_ = insert_new_row;
      if (need_after_row_process(ins_ctdef) && OB_FAIL(dml_modify_rows_.push_back(modify_row))) {
        LOG_WARN("failed to push dml modify row to modified row list", K(ret));
      } else if (OB_FAIL(replace_implict_cursor(insert_rows + delete_rows, 0, insert_rows, delete_rows))) {
        LOG_WARN("merge implict cursor failed", K(ret));
      }
    }
    delete_rows_ += delete_rows;
  } // while row store end
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  return ret;
}

int ObTableReplaceOp::prepare_final_replace_task()
{
  int ret = OB_SUCCESS;
  ObConflictRowMap *primary_map = NULL;
  OZ(conflict_checker_.get_primary_table_map(primary_map));
  CK(OB_NOT_NULL(primary_map));
  OZ(do_delete(primary_map));
  OZ(do_insert(primary_map));
  return ret;
}

int ObTableReplaceOp::do_delete(ObConflictRowMap *primary_map)
{
  int ret = OB_SUCCESS;
  ObConflictRowMap::iterator start_row_iter = primary_map->begin();
  ObConflictRowMap::iterator end_row_iter = primary_map->end();
  for (; OB_SUCC(ret) && start_row_iter != end_row_iter; ++start_row_iter) {
    clear_datum_eval_flag();
    ObConflictValue &constraint_value = start_row_iter->second;
    LOG_TRACE("get one constraint_value from primary hash map", K(constraint_value));
    if (NULL != constraint_value.baseline_datum_row_) {
      //baseline row is not empty, delete it
      if (OB_FAIL(constraint_value.baseline_datum_row_->to_expr(
          get_primary_table_old_row(), eval_ctx_))) {
        LOG_WARN("stored row to expr faild", K(ret));
      } else if (OB_FAIL(delete_row_to_das(false))) {
        LOG_WARN("shuffle delete row failed", K(ret), K(constraint_value));
      } else {
        LOG_TRACE("delete one row from primary hash map",
                  "real delete row", ROWEXPR2STR(eval_ctx_, get_primary_table_old_row()));
      }
    }
  }
  return ret;
}

int ObTableReplaceOp::do_insert(ObConflictRowMap *primary_map)
{
  int ret = OB_SUCCESS;
  ObConflictRowMap::iterator start_row_iter = primary_map->begin();
  ObConflictRowMap::iterator end_row_iter = primary_map->end();
  for (; OB_SUCC(ret) && start_row_iter != end_row_iter; ++start_row_iter) {
    clear_datum_eval_flag();
    ObConflictValue &constraint_value = start_row_iter->second;
    if (OB_SUCC(ret) && NULL != constraint_value.current_datum_row_) {
      //current row is not empty, insert new row
      if (OB_FAIL(constraint_value.current_datum_row_->to_expr(
          get_primary_table_new_row(), eval_ctx_))) {
        LOG_WARN("stored row to expr faild", K(ret));
      } else if (OB_FAIL(final_insert_row_to_das())) {
        LOG_WARN("shuffle insert row failed", K(ret), K(constraint_value));
      } else {
        LOG_TRACE("insert one row from primary hash map",
                  "real insert row", ROWEXPR2STR(eval_ctx_, get_primary_table_new_row()));
      }
    }
  }
  return ret;
}

int ObTableReplaceOp::calc_insert_tablet_loc(const ObInsCtDef &ins_ctdef,
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

int ObTableReplaceOp::calc_delete_tablet_loc(const ObDelCtDef &del_ctdef,
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


int ObTableReplaceOp::check_replace_ctdefs_valid() const
{
  int ret = OB_SUCCESS;
  CK(MY_SPEC.replace_ctdefs_.count() > 0);
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.replace_ctdefs_.count(); ++i) {
    const ObReplaceCtDef *replace_ctdef = NULL;
    const ObInsCtDef *insert_ctdef = NULL;
    const ObDelCtDef *del_ctdef = NULL;
    CK(OB_NOT_NULL(replace_ctdef = MY_SPEC.replace_ctdefs_.at(i)));
    CK(OB_NOT_NULL(insert_ctdef = replace_ctdef->ins_ctdef_));
    CK(OB_NOT_NULL(del_ctdef = replace_ctdef->del_ctdef_));
  }
  return ret;
}

const ObIArray<ObExpr *> &ObTableReplaceOp::get_all_saved_exprs()
{
  // During the upgrade, all_saved_exprs_ in the previous version is empty.
  // When this node is used as a remote execution node of the plan,
  // all_saved_exprs_ is also empty after the partition_wise plan is deserialized.
  return MY_SPEC.all_saved_exprs_.empty() ?
      MY_SPEC.replace_ctdefs_.at(0)->ins_ctdef_->new_row_ :
      MY_SPEC.all_saved_exprs_;
}

const ObIArray<ObExpr *> &ObTableReplaceOp::get_primary_table_new_row()
{
  return MY_SPEC.replace_ctdefs_.at(0)->ins_ctdef_->new_row_;
}

const ObIArray<ObExpr *> &ObTableReplaceOp::get_primary_table_old_row()
{
  return MY_SPEC.replace_ctdefs_.at(0)->del_ctdef_->old_row_;
}

int ObTableReplaceOp::reset_das_env()
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
  // 就说明replace into的某些逻辑处理有问题
  for (int64_t i = 0; OB_SUCC(ret) && i < replace_rtdefs_.count(); ++i) {
    ObInsRtDef &ins_rtdef = replace_rtdefs_.at(i).ins_rtdef_;
    ins_rtdef.das_rtdef_.need_fetch_conflict_ = false;
    ins_rtdef.das_rtdef_.is_duplicated_ = false;
  }
  return ret;
}


int ObTableReplaceOp::reuse()
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
    if (OB_FAIL(replace_rtctx_.das_ref_.close_all_task())) {
      LOG_WARN("close all update das task failed", K(ret));
    } else {
      replace_rtctx_.das_ref_.reuse();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(conflict_checker_.reuse())) {
      LOG_WARN("fail to reuse conflict checker", K(ret));
    } else {
      dml_modify_rows_.clear();
      replace_row_store_.reset();
    }
  }

  return ret;
}

void ObTableReplaceOp::add_need_conflict_result_flag()
{
  for (int64_t i = 0; i < replace_rtdefs_.count(); ++i) {
    ObInsRtDef &ins_rtdef = replace_rtdefs_.at(i).ins_rtdef_;
    ins_rtdef.das_rtdef_.need_fetch_conflict_ = true;
  }
  dml_rtctx_.set_pick_del_task_first();
  dml_rtctx_.set_non_sub_full_task();
}

int ObTableReplaceOp::check_values(bool &is_equal,
                                   const ObChunkDatumStore::StoredRow *replace_row,
                                   const ObChunkDatumStore::StoredRow *delete_row)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  const ObIArray<ObExpr *> &new_row = get_primary_table_new_row();
  const ObIArray<ObExpr *> &old_row = get_primary_table_old_row();
  int cmp_ret = 0;
  OZ(check_replace_ctdefs_valid());
  CK(OB_NOT_NULL(delete_row));
  CK(OB_NOT_NULL(replace_row));
  CK(replace_row->cnt_ == new_row.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < new_row.count(); ++i) {
    const UIntFixedArray &column_ids = MY_SPEC.replace_ctdefs_.at(0)->ins_ctdef_->column_ids_;
    CK(new_row.at(i)->basic_funcs_->null_first_cmp_ == old_row.at(i)->basic_funcs_->null_first_cmp_);
    if (OB_SUCC(ret)) {
      if (share::schema::ObColumnSchemaV2::is_hidden_pk_column_id(column_ids[i])) {
        //隐藏主键列不处理
      } else if (MY_SPEC.doc_id_col_id_ == column_ids[i]) {
        // skip doc id
      } else {
        const ObDatum &insert_datum = replace_row->cells()[i];
        const ObDatum &del_datum = delete_row->cells()[i];
        if (OB_FAIL(new_row.at(i)->basic_funcs_->null_first_cmp_(insert_datum, del_datum, cmp_ret))) {
          LOG_WARN("compare failed", K(ret));
        } else if (0 != cmp_ret) {
          is_equal = false;
        }
      }
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
