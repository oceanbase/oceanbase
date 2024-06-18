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

#include "sql/engine/dml/ob_table_insert_op.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/engine/dml/ob_trigger_handler.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/basic/ob_expr_values_op.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/ob_autoincrement_service.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/profile/ob_perf_event.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "sql/engine/cmd/ob_table_direct_insert_service.h"
#include "sql/engine/dml/ob_fk_checker.h"


namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
using namespace observer;
namespace sql
{

OB_SERIALIZE_MEMBER((ObTableInsertOpInput, ObTableModifyOpInput));

OB_DEF_SERIALIZE(ObTableInsertSpec)
{
  int ret = OB_SUCCESS;
  int64_t tbl_cnt = ins_ctdefs_.count();
  BASE_SER((ObTableInsertSpec, ObTableModifySpec));
  OB_UNIS_ENCODE(tbl_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < tbl_cnt; ++i) {
    int64_t idx_cnt = ins_ctdefs_.at(i).count();
    OB_UNIS_ENCODE(idx_cnt);
    for (int64_t j = 0; OB_SUCC(ret) && j < idx_cnt; ++j) {
      ObInsCtDef *ins_ctdef = ins_ctdefs_.at(i).at(j);
      if (OB_ISNULL(ins_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ins_ctdef is nullptr", K(ret));
      }
      OB_UNIS_ENCODE(*ins_ctdef);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTableInsertSpec)
{
  int ret = OB_SUCCESS;
  int64_t tbl_cnt = 0;
  BASE_DESER((ObTableInsertSpec, ObTableModifySpec));
  OB_UNIS_DECODE(tbl_cnt);
  if (OB_SUCC(ret) && tbl_cnt > 0) {
    OZ(ins_ctdefs_.allocate_array(alloc_, tbl_cnt));
  }
  ObDMLCtDefAllocator<ObInsCtDef> ins_ctdef_allocator(alloc_);
  for (int64_t i = 0; OB_SUCC(ret) && i < tbl_cnt; ++i) {
    int64_t index_cnt = 0;
    OB_UNIS_DECODE(index_cnt);
    OZ(ins_ctdefs_.at(i).allocate_array(alloc_, index_cnt));
    for (int64_t j = 0; OB_SUCC(ret) && j < index_cnt; ++j) {
      ObInsCtDef *ins_ctdef = ins_ctdef_allocator.alloc();
      if (OB_ISNULL(ins_ctdef)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc ins_ctdef failed", K(ret));
      }
      OB_UNIS_DECODE(*ins_ctdef);
      ins_ctdefs_.at(i).at(j) = ins_ctdef;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableInsertSpec)
{
  int64_t len = 0;
  int64_t tbl_cnt = ins_ctdefs_.count();
  BASE_ADD_LEN((ObTableInsertSpec, ObTableModifySpec));
  OB_UNIS_ADD_LEN(tbl_cnt);
  for (int64_t i = 0; i < tbl_cnt; ++i) {
    int64_t index_cnt = ins_ctdefs_.at(i).count();
    OB_UNIS_ADD_LEN(index_cnt);
    for (int64_t j = 0; j < index_cnt; ++j) {
      ObInsCtDef *ins_ctdef = ins_ctdefs_.at(i).at(j);
      if (ins_ctdef != nullptr) {
        OB_UNIS_ADD_LEN(*ins_ctdef);
      }
    }
  }
  return len;
}

int ObTableInsertOp::check_need_exec_single_row()
{
  int ret = OB_SUCCESS;
  ret = ObTableModifyOp::check_need_exec_single_row();
  if (OB_SUCC(ret) && !execute_single_row_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.ins_ctdefs_.count() && !execute_single_row_; ++i) {
      const ObTableInsertSpec::InsCtDefArray &ctdefs = MY_SPEC.ins_ctdefs_.at(i);
      const ObInsCtDef &ins_ctdef = *ctdefs.at(0);
      for (int64_t j = 0;
          OB_SUCC(ret) && !execute_single_row_ && j < ins_ctdef.trig_ctdef_.tg_args_.count();
          ++j) {
        const ObTriggerArg &tri_arg = ins_ctdef.trig_ctdef_.tg_args_.at(j);
        execute_single_row_ = tri_arg.is_execute_single_row();
      }
    }
  }
  return ret;
}

OB_INLINE int ObTableInsertOp::inner_open_with_das()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(open_table_for_each())) {
    LOG_WARN("open table for each failed", K(ret), K(MY_SPEC.ins_ctdefs_.count()));
  }
  return ret;
}

OB_INLINE int ObTableInsertOp::open_table_for_each()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ins_rtdefs_.allocate_array(ctx_.get_allocator(), MY_SPEC.ins_ctdefs_.count()))) {
    LOG_WARN("allocate insert rtdef failed", K(ret), K(MY_SPEC.ins_ctdefs_.count()));
  }
  trigger_clear_exprs_.reset();
  fk_checkers_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < ins_rtdefs_.count(); ++i) {
    InsRtDefArray &rtdefs = ins_rtdefs_.at(i);
    const ObTableInsertSpec::InsCtDefArray &ctdefs = MY_SPEC.ins_ctdefs_.at(i);
    if (OB_FAIL(rtdefs.allocate_array(ctx_.get_allocator(), ctdefs.count()))) {
      LOG_WARN("allocate update rtdefs failed", K(ret), K(ctdefs.count()));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < rtdefs.count(); ++j) {
      ObInsRtDef &ins_rtdef = rtdefs.at(j);
      const ObInsCtDef &ins_ctdef = *ctdefs.at(j);
      if (OB_FAIL(ObDMLService::init_ins_rtdef(dml_rtctx_, ins_rtdef, ins_ctdef, trigger_clear_exprs_, fk_checkers_))) {
        LOG_WARN("init insert rtdef failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && !rtdefs.empty()) {
      const ObInsCtDef &primary_ins_ctdef = *ctdefs.at(0);
      ObInsRtDef &primary_ins_rtdef = rtdefs.at(0);
      if (primary_ins_ctdef.error_logging_ctdef_.is_error_logging_) {
        is_error_logging_ = true;
      }
      if (OB_FAIL(ObDMLService::process_before_stmt_trigger(primary_ins_ctdef,
                                                            primary_ins_rtdef,
                                                            dml_rtctx_,
                                                            ObDmlEventType::DE_INSERTING))) {
        LOG_WARN("process before stmt trigger failed", K(ret));
      } else {
        //this table is being accessed by dml operator, mark its table location as writing
        //but single value insert in oracle allow the nested sql modify its insert table
        //clear the writing flag in table location before the trigger execution
        //see it:
        primary_ins_rtdef.das_rtdef_.table_loc_->is_writing_ =
            !(primary_ins_ctdef.is_single_value_ && lib::is_oracle_mode());
      }
    }
  }
  return ret;
}

int ObTableInsertOp::calc_tablet_loc(const ObInsCtDef &ins_ctdef,
                                     ObInsRtDef &ins_rtdef,
                                     ObDASTabletLoc *&tablet_loc)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.use_dist_das_) {
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
  } else {
    //direct write insert row to storage
    tablet_loc = MY_INPUT.get_tablet_loc();
  }
  return ret;
}

int ObTableInsertOp::write_row_to_das_buffer()
{
  int ret = OB_SUCCESS;
  ret = insert_row_to_das();
  return ret;
}

void ObTableInsertOp::record_err_for_load_data(int err_ret, int row_num)
{
  UNUSED(err_ret);
  if (OB_NOT_NULL(ctx_.get_my_session()) && ctx_.get_my_session()->is_load_data_exec_session()) {
    //record failed line num in warning buffer for load data
    ObWarningBuffer *buffer = ob_get_tsi_warning_buffer();
    if (OB_NOT_NULL(buffer) && 0 == buffer->get_error_line()) {
      buffer->set_error_line_column(row_num, 0);
    }
    LOG_DEBUG("load data exec log error line", K(err_ret), K(row_num));
  }
}

OB_INLINE int ObTableInsertOp::insert_row_to_das()
{
  int ret = OB_SUCCESS;
  transaction::ObTxSEQ savepoint_no;
  NG_TRACE(insert_start);
  // first get next row from child operator
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  bool is_skipped = false;
  if (is_error_logging_) {
    OZ(ObSqlTransControl::create_anonymous_savepoint(ctx_, savepoint_no));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.ins_ctdefs_.count(); ++i) {
    const ObTableInsertSpec::InsCtDefArray &ctdefs = MY_SPEC.ins_ctdefs_.at(i);
    InsRtDefArray &rtdefs = ins_rtdefs_.at(i);
    is_skipped = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < ctdefs.count(); ++j) {
      // insert each table with fetched row
      const ObInsCtDef &ins_ctdef = *(ctdefs.at(j));
      ObInsRtDef &ins_rtdef = rtdefs.at(j);
      ObDASTabletLoc *tablet_loc = nullptr;
      ObDMLModifyRowNode modify_row(this, &ins_ctdef, &ins_rtdef, ObDmlEventType::DE_INSERTING);
      if (!MY_SPEC.ins_ctdefs_.at(0).at(0)->has_instead_of_trigger_) {
        ++ins_rtdef.cur_row_num_;
      }
      if (OB_FAIL(ObDMLService::init_heap_table_pk_for_ins(ins_ctdef, eval_ctx_))) {
        LOG_WARN("fail to init heap table pk to null", K(ret));
      } else if (OB_FAIL(ObDMLService::process_insert_row(ins_ctdef, ins_rtdef, *this, is_skipped))) {
        if (is_error_logging_ && err_log_rt_def_.first_err_ret_ != OB_SUCCESS) {
          // It means that error logging has caught the error, and the log will not be printed temporarily
        } else {
          LOG_WARN("process insert row failed", K(ret));
        }
      } else if (OB_UNLIKELY(is_skipped)) {
        break;
      } else if (OB_FAIL(calc_tablet_loc(ins_ctdef, ins_rtdef, tablet_loc))) {
        LOG_WARN("calc partition key failed", K(ret));
      } else if (OB_FAIL(ObDMLService::set_heap_table_hidden_pk(ins_ctdef,
                                                                tablet_loc->tablet_id_,
                                                                eval_ctx_))) {
        LOG_WARN("set_heap_table_hidden_pk failed", K(ret), KPC(tablet_loc));
      } else if (OB_FAIL(ObDMLService::insert_row(ins_ctdef, ins_rtdef, tablet_loc, dml_rtctx_, modify_row.new_row_))) {
        LOG_WARN("insert row with das failed", K(ret));
      // TODO(yikang): fix trigger related for heap table
      } else if (need_after_row_process(ins_ctdef) && OB_FAIL(dml_modify_rows_.push_back(modify_row))) {
        LOG_WARN("failed to push dml modify row to modified row list", K(ret));
      }
      if (OB_FAIL(ret)) {
        record_err_for_load_data(ret, ins_rtdef.cur_row_num_);
      }
    } // end for global index ctdef loop
    if (OB_SUCC(ret)) {
      int64_t insert_rows = is_skipped ? 0 : 1;
      if (OB_FAIL(merge_implict_cursor(insert_rows, 0, 0, 0))) {
        LOG_WARN("merge implict cursor failed", K(ret));
      }
    }

  } // end for table ctdef loop

  if (is_error_logging_) {
    int err_ret = ret;
    if (OB_FAIL(ObDMLService::catch_violate_error(err_ret,
                                                  savepoint_no,
                                                  dml_rtctx_,
                                                  err_log_rt_def_,
                                                  MY_SPEC.ins_ctdefs_.at(0).at(0)->error_logging_ctdef_,
                                                  err_log_service_,
                                                  ObDASOpType::DAS_OP_TABLE_INSERT))) {
      LOG_WARN("fail to catch violate error", K(err_ret), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    plan_ctx->record_last_insert_id_cur_stmt();
  }
  NG_TRACE(insert_end);
  return ret;
}

int ObTableInsertOp::write_rows_post_proc(int last_errno)
{
  int ret = last_errno;
  if (iter_end_) {
    ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      plan_ctx->set_last_insert_id_cur_stmt(0);
    }
    if (OB_SUCC(ret)) {
      int64_t changed_rows = 0;
      //for multi table
      for (int64_t i = 0; i < ins_rtdefs_.count(); ++i) {
        changed_rows += ins_rtdefs_.at(i).at(0).das_rtdef_.affected_rows_;
      }
      plan_ctx->add_row_matched_count(changed_rows);
      plan_ctx->add_affected_rows(changed_rows);
      // sync last user specified value after iter ends(compatible with MySQL)
      if (OB_FAIL(plan_ctx->sync_last_value_local())) {
        LOG_WARN("failed to sync last value", K(ret));
      }
    }
    int sync_ret = OB_SUCCESS;
    if (OB_SUCCESS != (sync_ret = plan_ctx->sync_last_value_global())) {
      LOG_WARN("failed to sync value globally", K(sync_ret));
    }
    NG_TRACE(sync_auto_value);
    if (OB_SUCC(ret)) {
      ret = sync_ret;
    }
    if (OB_SUCC(ret) && GCONF.enable_defensive_check() && !is_error_logging_) {
      if (OB_FAIL(check_insert_affected_row())) {
        LOG_WARN("check index insert consistency failed", K(ret));
      }
    }
  }
  return ret;
}

OB_INLINE int ObTableInsertOp::check_insert_affected_row()
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < ins_rtdefs_.count(); ++idx) {
    const ObTableInsertSpec::InsCtDefArray &each_ins_ctdefs = MY_SPEC.ins_ctdefs_.at(idx);
    InsRtDefArray &each_ins_rtdefs = ins_rtdefs_.at(idx);
    const ObInsCtDef &pri_ctdef = *each_ins_ctdefs.at(0);
    ObInsRtDef &pri_rtdef = each_ins_rtdefs.at(0);
    for (int64_t i = 1; OB_SUCC(ret) && i < each_ins_rtdefs.count(); ++i) {
      ObInsCtDef &idx_ctdef = *each_ins_ctdefs.at(i);
      ObInsRtDef &idx_rtdef = each_ins_rtdefs.at(i);
      if (pri_rtdef.das_rtdef_.affected_rows_ != idx_rtdef.das_rtdef_.affected_rows_) {
        ret = OB_ERR_DEFENSIVE_CHECK;
        ObString func_name = ObString::make_string("check_insert_affected_row");
        LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
        LOG_ERROR_RET(OB_ERR_DEFENSIVE_CHECK,
                  "Fatal Error!!! data table insert affected row is not match with index table",
                  K(ret),
                  "primary_affected_rows", pri_rtdef.das_rtdef_.affected_rows_,
                  "index_affected_rows", idx_rtdef.das_rtdef_.affected_rows_,
                  "primary_ins_ctdef", pri_ctdef,
                  "index_ins_ctdef", idx_ctdef,
                  "primary_ins_rtdef", pri_rtdef,
                  "index_ins_rtdef", idx_rtdef);
        LOG_DBA_ERROR_V2(OB_SQL_INSERT_AFFECTED_ROW_FAIL, ret, "Attention!!!", "data table delete affected row is not match with index table "
                  ", data table delete affected_rows is: ", pri_rtdef.das_rtdef_.affected_rows_,
                  ", index table delete affected_rows is: ", idx_rtdef.das_rtdef_.affected_rows_);
      }
    }
    if (OB_SUCC(ret) && !pri_ctdef.das_ctdef_.is_ignore_) {
      if (pri_rtdef.cur_row_num_ != pri_rtdef.das_rtdef_.affected_rows_) {
        ret = OB_ERR_DEFENSIVE_CHECK;
        ObString func_name = ObString::make_string("check_insert_affected_row");
        LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
        LOG_ERROR_RET(OB_ERR_DEFENSIVE_CHECK, "Fatal Error!!! data table insert affected row is not match with found rows", K(ret),
                  "primary_affected_rows", pri_rtdef.das_rtdef_.affected_rows_,
                  "primary_found_rows", pri_rtdef.cur_row_num_,
                  "primary_ins_ctdef", pri_ctdef,
                  "primary_ins_rtdef", pri_rtdef);
       LOG_DBA_ERROR_V2(OB_SQL_INSERT_AFFECTED_ROW_FAIL, ret, "Attention!!!", "data table insert affected row is not match with found rows"
                  ", primary_affected_rows is: ", pri_rtdef.das_rtdef_.affected_rows_,
                  ", primary_get_rows is: ", pri_rtdef.cur_row_num_);
      }
    }
  }
  return ret;
}

int ObTableInsertOp::inner_open()
{
  int ret = OB_SUCCESS;
  NG_TRACE(insert_open);
  //execute insert with das
  //calc partition by table location info
  if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("inner open ObTableModifyOp failed", K(ret));
  } else if (OB_UNLIKELY(MY_SPEC.ins_ctdefs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ins ctdef is invalid", K(ret), KP(this));
  } else if (OB_UNLIKELY(iter_end_)) {
    //do nothing
  } else if (OB_FAIL(inner_open_with_das())) {
    LOG_WARN("inner open with das failed", K(ret));
  }
  return ret;
}

int ObTableInsertOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableModifyOp::inner_rescan())) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else if (OB_UNLIKELY(iter_end_)) {
    //do nothing
  } else if (OB_FAIL(close_table_for_each())) {
    LOG_WARN("close table for each failed", K(ret));
  } else if (OB_FAIL(open_table_for_each())) {
    LOG_WARN("open table for each failed", K(ret));
  }
  return ret;
}

int ObTableInsertOp::inner_close()
{
  NG_TRACE(insert_close);
  int ret = OB_SUCCESS;
  if (OB_FAIL(close_table_for_each())) {
    LOG_WARN("close table for each failed", K(ret));
  }
  int close_ret = ObTableModifyOp::inner_close();
  return (OB_SUCCESS == ret) ? close_ret : ret;
}

OB_INLINE int ObTableInsertOp::close_table_for_each()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ctx_.get_errcode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < ins_rtdefs_.count(); ++i) {
      if (!ins_rtdefs_.at(i).empty()) {
        const ObInsCtDef &primary_ins_ctdef = *MY_SPEC.ins_ctdefs_.at(i).at(0);
        ObInsRtDef &primary_ins_rtdef = ins_rtdefs_.at(i).at(0);
        if (OB_NOT_NULL(primary_ins_rtdef.das_rtdef_.table_loc_)) {
          primary_ins_rtdef.das_rtdef_.table_loc_->is_writing_ = false;
        }
        if (OB_FAIL(ObDMLService::process_after_stmt_trigger(primary_ins_ctdef,
                                                             primary_ins_rtdef,
                                                             dml_rtctx_,
                                                             ObDmlEventType::DE_INSERTING))) {
          LOG_WARN("process after stmt trigger failed", K(ret));
        }
      }
    }
  }
  //whether it is successful or not, needs to release rtdef
  for (int64_t i = 0; i < ins_rtdefs_.count(); ++i) {
    ins_rtdefs_.at(i).release_array();
  }
  return ret;
}


}  // namespace sql
}  // namespace oceanbase
