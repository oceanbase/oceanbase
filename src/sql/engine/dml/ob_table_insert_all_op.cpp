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
#include "sql/engine/dml/ob_table_insert_all_op.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/engine/dml/ob_trigger_handler.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql
{

OB_SERIALIZE_MEMBER(InsertAllTableInfo,
                    match_conds_exprs_,
                    match_conds_idx_);

OB_SERIALIZE_MEMBER((ObTableInsertAllOpInput, ObTableInsertOpInput));

OB_DEF_SERIALIZE(ObTableInsertAllSpec)
{
  int ret = OB_SUCCESS;
  int64_t ins_cnt = insert_table_infos_.count();
  BASE_SER((ObTableInsertAllSpec, ObTableInsertSpec));
  LST_DO_CODE(OB_UNIS_ENCODE,
              is_insert_all_with_conditions_,
              is_insert_all_first_,
              ins_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < ins_cnt; ++i) {
    InsertAllTableInfo *ins_table_info = insert_table_infos_.at(i);
    if (OB_ISNULL(ins_table_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ins_table_info is nullptr", K(ret), K(ins_table_info));
    } else {
      OB_UNIS_ENCODE(*ins_table_info);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTableInsertAllSpec)
{
  int ret = OB_SUCCESS;
  int64_t ins_cnt = 0;
  BASE_DESER((ObTableInsertAllSpec, ObTableInsertSpec));
  LST_DO_CODE(OB_UNIS_DECODE,
              is_insert_all_with_conditions_,
              is_insert_all_first_,
              ins_cnt);
  OZ(insert_table_infos_.init(ins_cnt));
  for (int64_t i = 0; OB_SUCC(ret) && i < ins_cnt; ++i) {
    void *ptr = NULL;
    InsertAllTableInfo *ins_table_info = NULL;
    if (OB_ISNULL(ptr = alloc_.alloc(sizeof(InsertAllTableInfo)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory", K(ret), K(ptr));
    } else {
      ins_table_info = new(ptr)InsertAllTableInfo(alloc_);
      OB_UNIS_DECODE(*ins_table_info);
      OZ(insert_table_infos_.push_back(ins_table_info));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableInsertAllSpec)
{
  int64_t len = 0;
  int64_t ins_cnt = insert_table_infos_.count();
  BASE_ADD_LEN((ObTableInsertAllSpec, ObTableInsertSpec));
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              is_insert_all_with_conditions_,
              is_insert_all_first_,
              ins_cnt);
  for (int64_t i = 0; i < ins_cnt; ++i) {
    InsertAllTableInfo *ins_table_info = insert_table_infos_.at(i);
    if (ins_table_info != nullptr) {
      OB_UNIS_ADD_LEN(*ins_table_info);
    }
  }
  return len;
}

int ObTableInsertAllOp::switch_iterator(ObExecContext &ctx)
{
  UNUSED(ctx);
  return common::OB_ITER_END;
}

// If there are foreign key dependencies between the tables to be inserted, a single row is required
int ObTableInsertAllOp::check_need_exec_single_row()
{
  int ret = OB_SUCCESS;
  ret = ObTableModifyOp::check_need_exec_single_row();
  if (OB_SUCC(ret) && !execute_single_row_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.ins_ctdefs_.count() && !execute_single_row_; ++i) {
      const ObTableInsertSpec::InsCtDefArray &ctdefs = MY_SPEC.ins_ctdefs_.at(i);
      const ObInsCtDef &ins_ctdef = *(ctdefs.at(0));
      const uint64_t table_id = ins_ctdef.das_base_ctdef_.index_tid_;
      const ObForeignKeyArgArray &fk_args = ins_ctdef.fk_args_;
      for (int64_t j = 0;
          OB_SUCC(ret) && !execute_single_row_ && j < ins_ctdef.trig_ctdef_.tg_args_.count();
          ++j) {
        const ObTriggerArg &tri_arg = ins_ctdef.trig_ctdef_.tg_args_.at(j);
        execute_single_row_ = tri_arg.is_execute_single_row();
      }
      for (int j = 0; OB_SUCC(ret) && j < fk_args.count() && !execute_single_row_; j++) {
        const ObForeignKeyArg &fk_arg = fk_args.at(j);
        const uint64_t parent_table_id = fk_arg.table_id_;
        for (int k = 0; k < MY_SPEC.ins_ctdefs_.count() && !execute_single_row_; ++k) {
          const uint64_t tmp_table_id =  MY_SPEC.ins_ctdefs_.at(k).at(0)->das_base_ctdef_.index_tid_;
          if (parent_table_id == tmp_table_id && k != i) {
            execute_single_row_ = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableInsertAllOp::write_row_to_das_buffer()
{
  int ret = OB_SUCCESS;
  //int64_t savepoint_no = 0;
  NG_TRACE(insert_start);
  // first get next row from child operator
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  // erro logging not support, fix it later
  // if (is_error_logging_) {
  //   OZ(ObSqlTransControl::create_anonymous_savepoint(ctx_, savepoint_no));
  // }
  int64_t tbl_idx = 0;
  int64_t pre_match_idx = -1;
  bool is_continued = true;
  bool no_need_insert = false;
  bool have_insert_row = false;
  bool is_skipped = false;
  for (int64_t i = 0; OB_SUCC(ret) && is_continued && i < MY_SPEC.ins_ctdefs_.count(); ++i) {
    const ObTableInsertSpec::InsCtDefArray &ctdefs = MY_SPEC.ins_ctdefs_.at(i);
    InsRtDefArray &rtdefs = ins_rtdefs_.at(i);
    if (OB_FAIL(check_match_conditions(i, have_insert_row, pre_match_idx,
                                       no_need_insert, is_continued))) {
      LOG_WARN("failed to check match conditions", K(ret));
    } else if (no_need_insert) {
      /*do nothing*/
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < ctdefs.count(); ++j) {
        const ObInsCtDef &ins_ctdef = *(ctdefs.at(j));
        ObInsRtDef &ins_rtdef = rtdefs.at(j);
        ObDASTabletLoc *tablet_loc = nullptr;
        ObDMLModifyRowNode modify_row(this, &ins_ctdef, &ins_rtdef, ObDmlEventType::DE_INSERTING);
        ++ins_rtdef.cur_row_num_;
        if (OB_FAIL(ObDMLService::init_heap_table_pk_for_ins(ins_ctdef, eval_ctx_))) {
          LOG_WARN("fail to init heap table pk to null", K(ret));
        } else if (OB_FAIL(ObDMLService::process_insert_row(ins_ctdef, ins_rtdef, *this, is_skipped))) {
        // if (is_error_logging_ && err_log_rt_def_.first_err_ret_ != OB_SUCCESS) {
        //   // It means that error logging has caught the error, and the log will not be printed temporarily
        // } else {
           LOG_WARN("process insert row failed", K(ret));
        //}
        } else if (OB_UNLIKELY(is_skipped)) {
          break;
        } else if (OB_FAIL(ObTableInsertOp::calc_tablet_loc(ins_ctdef, ins_rtdef, tablet_loc))) {
          LOG_WARN("calc partition key failed", K(ret));
        } else if (OB_FAIL(ObDMLService::set_heap_table_hidden_pk(ins_ctdef, tablet_loc->tablet_id_, eval_ctx_))) {
          LOG_WARN("set_heap_table_hidden_pk failed", K(ret), KPC(tablet_loc));
        } else if (OB_FAIL(ObDMLService::insert_row(ins_ctdef, ins_rtdef, tablet_loc, dml_rtctx_, modify_row.new_row_))) {
          LOG_WARN("insert row with das failed", K(ret));
        // TODO(yikang): fix trigger related for heap table
        } else if (need_after_row_process(ins_ctdef) && OB_FAIL(dml_modify_rows_.push_back(modify_row))) {
          LOG_WARN("failed to push dml modify row to modified row list", K(ret));
        } else {
          have_insert_row = true;
        }
      } // end for global index ctdef loop

      // NOTE: for insert all into t1,t2, t1 is the parent table of t2, Single-line execution is required to ensure oracle compatibility
      if (OB_SUCC(ret) && execute_single_row_ && OB_FAIL(submit_all_dml_task())) {
        LOG_WARN("failed to push dml task", K(ret));
      }
    }
  } // end for table ctdef loop
  //erro logging not support, fix it later
  // if (is_error_logging_) {
  //   int err_ret = ret;
  //   if (OB_FAIL(ObDMLService::catch_violate_error(err_ret,
  //                                                 savepoint_no,
  //                                                 das_ctx_,
  //                                                 err_log_rt_def_,
  //                                                 MY_SPEC.ins_ctdefs_.at(0)->error_logging_ctdef_,
  //                                                 err_log_service_,
  //                                                 ObDASOpType::DAS_OP_TABLE_INSERT))) {
  //     LOG_WARN("fail to catch violate error", K(err_ret), K(ret));
  //   }
  // }
  if (OB_SUCC(ret)) {
    plan_ctx->record_last_insert_id_cur_stmt();
  }
  if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
    plan_ctx->set_last_insert_id_cur_stmt(0);
  }
  NG_TRACE(insert_end);
  return ret;
}

int ObTableInsertAllOp::check_match_conditions(const int64_t tbl_idx,
                                               const bool have_insert_row,
                                               int64_t &pre_match_idx,
                                               bool &no_need_insert,
                                               bool &is_continued)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begine to check_match_conditions", K(have_insert_row), K(pre_match_idx), K(tbl_idx),
                                                K(no_need_insert), K(is_continued));
  InsertAllTableInfo *table_info = NULL;
  bool is_match = false;
  //1. common insert all, eg: insert all into t1 select ...
  if (!MY_SPEC.is_insert_all_with_conditions_) {//just insert
    /*do nothing*/
  //2. insert all with contidions, eg: insert all/first when ... into t1 else ... select ...
  } else if (OB_UNLIKELY(tbl_idx >= MY_SPEC.insert_table_infos_.count()) ||
             OB_ISNULL(table_info = MY_SPEC.insert_table_infos_.at(tbl_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(tbl_idx), K(ret), K(table_info),
                                     K(MY_SPEC.insert_table_infos_.count()));
  } else if (table_info->match_conds_idx_ == pre_match_idx) {//belong same conditions
    /*do nothing*/
  //insert first only insert the first fullfill with condition.
  } else if (MY_SPEC.is_insert_all_first_ && have_insert_row) {
    is_continued = false;
    no_need_insert = true;
  } else if (table_info->match_conds_exprs_.count() == 0) {//else branch in insert all
    if (OB_UNLIKELY(table_info->match_conds_idx_ != -1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(table_info->match_conds_idx_));
    } else {
      pre_match_idx = table_info->match_conds_idx_;
      if (have_insert_row) {
        no_need_insert = true;
        is_continued = false;
      } else {
        no_need_insert = false;
        }
      }
  //conditions branch in insert all
  } else if (OB_FAIL(check_row_match_conditions(table_info->match_conds_exprs_, is_match))) {
    LOG_WARN("failed to check row match conditions", K(ret));
  } else if (is_match) {
    no_need_insert = false;
    pre_match_idx = table_info->match_conds_idx_;
  } else {
    no_need_insert = true;
    pre_match_idx = table_info->match_conds_idx_;
  }
  LOG_TRACE("Succeed to check_match_conditions", K(have_insert_row),
                                                 K(pre_match_idx), K(tbl_idx),
                                                 K(no_need_insert), K(is_continued));
  return ret;
}

int ObTableInsertAllOp::check_row_match_conditions(const ExprFixedArray &match_conds_exprs,
                                                   bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_match && i < match_conds_exprs.count(); ++i) {
    ObExpr *expr = match_conds_exprs.at(i);
    ObDatum *datum = nullptr;
    if (OB_FAIL(expr->eval(get_eval_ctx(), datum))) {
      LOG_WARN("eval check constraint expr failed", K(ret));
    } else {
      OB_ASSERT(ob_is_int_tc(expr->datum_meta_.type_));
      if (!datum->is_null() && 1 == datum->get_int()) {
        /*do nothing*/
      } else {
        is_match = false;
      }
    }
  }
  return ret;
}

int ObTableInsertAllOp::inner_open()
{
  return ObTableInsertOp::inner_open();
}

int ObTableInsertAllOp::inner_rescan()
{
  return ObTableInsertOp::inner_rescan();
}

int ObTableInsertAllOp::inner_close()
{
  return ObTableInsertOp::inner_close();
}

}  // namespace sql
}  // namespace oceanbase
