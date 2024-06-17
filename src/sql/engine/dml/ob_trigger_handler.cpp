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

#include "sql/engine/dml/ob_trigger_handler.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/engine/dml/ob_table_insert_op.h"
#include "sql/engine/dml/ob_table_update_op.h"
#include "sql/engine/basic/ob_expr_values_op.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/executor/ob_task_spliter.h"
#include "storage/ob_i_store.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "sql/ob_spi.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace storage;
using namespace share;
using namespace share::schema;
using namespace observer;
namespace sql
{
int TriggerHandle::init_trigger_row(
  ObIAllocator &alloc, int64_t rowtype_col_count, pl::ObPLRecord *&record)
{
  int ret = OB_SUCCESS;
  int64_t init_size = pl::ObRecordType::get_init_size(rowtype_col_count);
  init_size += sizeof(ObObj) * rowtype_col_count; //append hidden columns
  if (OB_ISNULL(record = reinterpret_cast<pl::ObPLRecord*>(alloc.alloc(init_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(init_size));
  } else {
    new (record)pl::ObPLRecord(OB_INVALID_ID, rowtype_col_count);
    ObObj *cells = record->get_element();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowtype_col_count; i++) {
      new (&cells[i]) ObObj();
      cells[i].set_null();
    }
  }
  return ret;
}

/*
  drop table t3;
  create table t3 (
    pk int primary key,
    aa_int int,
    bb_int int
  );
  insert into t3 values (400, 500, 500);
  commit;

  CREATE OR REPLACE TRIGGER seq_trigger_1
    BEFORE UPDATE ON t3
    FOR EACH ROW
  BEGIN
    DBMS_OUTPUT.PUT_LINE('Updating T3, original aa_int is ' || :NEW.aa_int);
    :NEW.aa_int := :NEW.aa_int * 10;
    DBMS_OUTPUT.PUT_LINE('Updating T3, modify aa_int to ' || :NEW.aa_int);
  END;
  /

  CREATE OR REPLACE TRIGGER seq_trigger_2
    BEFORE UPDATE ON t3
    FOR EACH ROW
  BEGIN
    DBMS_OUTPUT.PUT_LINE('Updating T3, original aa_int is ' || :NEW.aa_int);
    :NEW.aa_int := :NEW.aa_int * 10;
    DBMS_OUTPUT.PUT_LINE('Updating T3, modify aa_int to ' || :NEW.aa_int);
  END;
  /

  SQL> update t3 set aa_int = 700 where pk = 400;
  Updating T3, original aa_int is 700
  Updating T3, modify aa_int to 7000
  Updating T3, original aa_int is 7000
  Updating T3, modify aa_int to 70000

  1 row updated.

  * the following trigger which fired after previous trigger can see the modification
  * happened in previous trigger, so we use ObArrayHelper here which can help all triggers
  * to share the same old row and new row.
  */
int TriggerHandle::init_trigger_params(
  ObDMLRtCtx &das_ctx,
  uint64_t trigger_event,
  const ObTrigDMLCtDef &trig_ctdef,
  ObTrigDMLRtDef &trig_rtdef)
{
  UNUSED(trigger_event);
  int ret = OB_SUCCESS;
  void *when_point_params_buf = NULL;
  void *row_point_params_buf = NULL;
  int64_t param_store_size = sizeof(ParamStore);
  // TODO: 这个接口还可以进一步精细化，比如在没有when条件时，tg_when_point_params_相关逻辑都不需要执行的，
  //       或者在insert/delete操作时，tg_init_point_params_也不需要执行的。
  if (OB_ISNULL(when_point_params_buf = das_ctx.get_exec_ctx().get_allocator().alloc(param_store_size)) ||
      OB_ISNULL(row_point_params_buf = das_ctx.get_exec_ctx().get_allocator().alloc(param_store_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    ObIAllocator &allocator = das_ctx.get_exec_ctx().get_allocator();
    trig_rtdef.tg_when_point_params_ = new(when_point_params_buf)ParamStore(ObWrapperAllocator(allocator));
    trig_rtdef.tg_row_point_params_ = new(row_point_params_buf)ParamStore(ObWrapperAllocator(allocator));
    OZ (trig_rtdef.tg_when_point_params_->prepare_allocate(WHEN_POINT_PARAM_COUNT));
    OZ (trig_rtdef.tg_row_point_params_->prepare_allocate(lib::is_oracle_mode() ?
        ROW_POINT_PARAM_COUNT : ROW_POINT_PARAM_COUNT_MYSQL));

    pl::ObPLRecord *old_record = NULL;
    pl::ObPLRecord *new_record = NULL;
    // FIXME: for delete it has no new_rowid_expr, trig_col_info hasn't add rowid
    int64_t rowtype_col_count = trig_ctdef.trig_col_info_.get_rowtype_count();
    int64_t init_size = pl::ObRecordType::get_init_size(rowtype_col_count);
    if (trig_ctdef.all_tm_points_.has_when_condition() || trig_ctdef.all_tm_points_.has_row_point()) {
      OZ (init_trigger_row(das_ctx.get_exec_ctx().get_allocator(), rowtype_col_count, old_record));
      OZ (init_trigger_row(das_ctx.get_exec_ctx().get_allocator(), rowtype_col_count, new_record));
    }
    LOG_DEBUG("trigger init", K(rowtype_col_count), K(ret));

    if (OB_SUCC(ret) && trig_ctdef.all_tm_points_.has_when_condition()) {
      trig_rtdef.tg_when_point_params_->at(0).set_extend(reinterpret_cast<int64_t>(old_record),
                                                        pl::PL_RECORD_TYPE, init_size);
      trig_rtdef.tg_when_point_params_->at(0).set_param_meta();
      trig_rtdef.tg_when_point_params_->at(1).set_extend(reinterpret_cast<int64_t>(new_record),
                                                        pl::PL_RECORD_TYPE, init_size);
      trig_rtdef.tg_when_point_params_->at(1).set_param_meta();
    }
    if (OB_SUCC(ret) && trig_ctdef.all_tm_points_.has_row_point()) {
      trig_rtdef.tg_row_point_params_->at(0).set_extend(reinterpret_cast<int64_t>(old_record),
                                            pl::PL_RECORD_TYPE, init_size);
      trig_rtdef.tg_row_point_params_->at(0).set_param_meta();
      trig_rtdef.tg_row_point_params_->at(1).set_extend(reinterpret_cast<int64_t>(new_record),
                                            pl::PL_RECORD_TYPE, init_size);
      trig_rtdef.tg_row_point_params_->at(1).set_param_meta();
    }
    trig_rtdef.old_record_ = old_record;
    trig_rtdef.new_record_ = new_record;
  }
  return ret;
}

int TriggerHandle::init_param_rows(
  ObEvalCtx &eval_ctx,
  const ObTrigDMLCtDef &trig_ctdef,
  ObTrigDMLRtDef &trig_rtdef)
{
  int ret = OB_SUCCESS;
  OZ (init_param_old_row(eval_ctx, trig_ctdef, trig_rtdef));
  OZ (init_param_new_row(eval_ctx, trig_ctdef, trig_rtdef));
  return ret;
}

int TriggerHandle::init_param_old_row(
  ObEvalCtx &eval_ctx,
  const ObTrigDMLCtDef &trig_ctdef,
  ObTrigDMLRtDef &trig_rtdef)
{
  int ret = OB_SUCCESS;
  if (trig_ctdef.all_tm_points_.has_when_condition() ||
      trig_ctdef.all_tm_points_.has_row_point()) {
    ObObj *cells = nullptr;
    if (OB_ISNULL(trig_rtdef.old_record_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: old record is null", K(ret));
    } else if (OB_ISNULL(cells = trig_rtdef.old_record_->get_element())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cells is NULL", K(ret));
    }
    for (int64_t i = 0; i < trig_ctdef.old_row_exprs_.count() && OB_SUCC(ret); ++i) {
      ObDatum *datum;
      if (OB_FAIL(trig_ctdef.old_row_exprs_.at(i)->eval(eval_ctx, datum))) {
        LOG_WARN("failed to eval rowid expr", K(ret));
      } else if (OB_ISNULL(datum))  {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("datum is NULL", K(ret));
      } else if (OB_FAIL(datum->to_obj(cells[i],
          trig_ctdef.old_row_exprs_.at(i)->obj_meta_))) {
        LOG_WARN("failed to datum to obj", K(ret));
      }
      LOG_DEBUG("debug init param old expr", K(ret), K(i),
        K(ObToStringExpr(eval_ctx, *trig_ctdef.old_row_exprs_.at(i))));
    }
    if (OB_NOT_NULL(trig_ctdef.rowid_old_expr_)) {
      ObDatum *datum;
      if (OB_FAIL(trig_ctdef.rowid_old_expr_->eval(eval_ctx, datum))) {
        LOG_WARN("failed to eval rowid expr", K(ret));
      }
      LOG_DEBUG("debug init param rowid old expr", K(ret),
        K(ObToStringExpr(eval_ctx, *trig_ctdef.rowid_old_expr_)));
    }
  }
  return ret;
}

int TriggerHandle::init_param_new_row(
  ObEvalCtx &eval_ctx,
  const ObTrigDMLCtDef &trig_ctdef,
  ObTrigDMLRtDef &trig_rtdef)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("debug init param new row", K(ret));
  if (trig_ctdef.all_tm_points_.has_when_condition() ||
      trig_ctdef.all_tm_points_.has_row_point()) {
    ObObj *cells = nullptr;
    if (OB_ISNULL(trig_rtdef.new_record_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: new record is null", K(ret));
    } else if (OB_ISNULL(cells = trig_rtdef.new_record_->get_element())){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cells is NULL", K(ret));
    }
    for (int64_t i = 0; i < trig_ctdef.new_row_exprs_.count() && OB_SUCC(ret); ++i) {
      ObDatum *datum;
      if (OB_FAIL(trig_ctdef.new_row_exprs_.at(i)->eval(eval_ctx, datum))) {
        LOG_WARN("failed to eval rowid expr", K(ret));
      } else if (OB_ISNULL(datum))  {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("datum is NULL", K(ret));
      } else if (OB_FAIL(datum->to_obj(cells[i],
          trig_ctdef.new_row_exprs_.at(i)->obj_meta_))) {
        LOG_WARN("failed to datum to obj", K(ret));
      }
      if (OB_SUCC(ret) && !trig_ctdef.trig_col_info_.get_flags()[i].is_rowid_
          && (ObCharType == cells[i].get_type() || ObNCharType == cells[i].get_type())) {
        // pad space for char type
        const common::ObObjType &col_type = trig_ctdef.new_row_exprs_.at(i)->obj_meta_.get_type();
        common::ObAccuracy accuracy;
        accuracy.length_ =  trig_ctdef.new_row_exprs_.at(i)->max_length_;
        accuracy.length_semantics_ = trig_ctdef.new_row_exprs_.at(i)->datum_meta_.length_semantics_;
        if (OB_FAIL(ObSPIService::spi_pad_char_or_varchar(eval_ctx.exec_ctx_.get_my_session(),
                                                          col_type,
                                                          accuracy,
                                                          &eval_ctx.exec_ctx_.get_allocator(),
                                                          &cells[i]))) {
          LOG_WARN("failed to pad space", K(col_type), K(accuracy), K(ret));
        }
      }
      LOG_DEBUG("debug init param new expr", K(ret), K(i),
        K(ObToStringExpr(eval_ctx, *trig_ctdef.new_row_exprs_.at(i))));
    }
    if (OB_NOT_NULL(trig_ctdef.rowid_new_expr_)) {
      ObDatum *datum;
      if (OB_FAIL(trig_ctdef.rowid_new_expr_->eval(eval_ctx, datum))) {
        LOG_WARN("failed to eval rowid expr", K(ret));
      }
      LOG_DEBUG("debug init param rowid new expr", K(ret),
        K(ObToStringExpr(eval_ctx, *trig_ctdef.rowid_new_expr_)));
    }
  }
  return ret;
}

int TriggerHandle::set_rowid_into_row(
  const ObTriggerColumnsInfo &cols,
  const ObObj &rowid_val,
  ObObj* cells)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(cols.get_flags()));
  // We can't distinguish urowid column and trigger rowid column in ObTriggerColumns,
  // but we can ensure that the trigger rowid column must behind the urowid column if exist,
  // because the trigger rowid column mock in optimizer
  for (int64_t i = cols.get_count() - 1; OB_SUCC(ret) && i >= 0; ++i) {
    if (cols.get_flags()[i].is_rowid_) {
      cells[i] = rowid_val;
      LOG_DEBUG("set_rowid_into_row done", K(ret), K(cells[i]));
      break;
    }
  }
  LOG_DEBUG("set_rowid_into_row done", K(ret));
  return ret;
}

int TriggerHandle::set_rowid_into_row(
  const ObTriggerColumnsInfo &cols,
  ObEvalCtx &eval_ctx,
  ObExpr *src_expr,
  ObObj* cells)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(cols.get_flags()));
  // We can't distinguish urowid column and trigger rowid column in ObTriggerColumns,
  // but we can ensure that the trigger rowid column must behind the urowid column if exist,
  // because the trigger rowid column mock in optimizer
  for (int64_t i = cols.get_count() - 1; OB_SUCC(ret) && i >= 0; ++i) {
    if (cols.get_flags()[i].is_rowid_) {
      ObDatum *datum;
      if (OB_FAIL(src_expr->eval(eval_ctx, datum))) {
        LOG_WARN("failed to eval expr", K(ret));
      } else if (OB_ISNULL(datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("datum is NULL", K(ret));
      } else if (OB_FAIL(datum->to_obj(cells[i], src_expr->obj_meta_))) {
        LOG_WARN("failed to datum to obj", K(ret));
      } else {
        LOG_DEBUG("set_rowid_into_row done", K(ret), K(ObToStringExpr(eval_ctx, *src_expr)));
      }
      break;
    }
  }
  LOG_DEBUG("set_rowid_into_row done", K(ret));
  return ret;
}

// calculate rowid and set into new_record that is for pl
int TriggerHandle::do_handle_rowid_before_row(
  ObTableModifyOp &dml_op,
  const ObTrigDMLCtDef &trig_ctdef,
  ObTrigDMLRtDef &trig_rtdef,
  uint64_t tg_event)
{
  int ret = OB_SUCCESS;
  if (NULL != trig_ctdef.rowid_new_expr_ && ObTriggerEvents::is_insert_event(tg_event)) {
    // we set a invalid rowid obj into old_row/new_row
    ObObj *rowid_val = NULL;
    if (OB_FAIL(ObURowIDData::build_invalid_rowid_obj(dml_op.get_exec_ctx().get_allocator(), rowid_val))) {
      LOG_WARN("failed to build urowid", K(ret));
    } else if (OB_ISNULL(rowid_val)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: rowid is null", K(ret));
    } else if (OB_FAIL(TriggerHandle::set_rowid_into_row(trig_ctdef.trig_col_info_,
                                          *rowid_val,
                                          trig_rtdef.old_record_->get_element()))) {
      LOG_WARN("failed to set rowid to old record", K(ret));
    } else if (OB_FAIL(TriggerHandle::set_rowid_into_row(trig_ctdef.trig_col_info_,
                                          *rowid_val,
                                          trig_rtdef.new_record_->get_element()))) {
      LOG_WARN("failed to set rowid to old record", K(ret));
    }
    LOG_DEBUG("handle rowid before insert success", K(tg_event), K(*rowid_val));
  } else if (NULL != trig_ctdef.rowid_old_expr_ && ObTriggerEvents::is_delete_event(tg_event)) {
    // new.rowid should be same with old.rowid
    OZ (TriggerHandle::set_rowid_into_row(trig_ctdef.trig_col_info_,
                                          dml_op.get_eval_ctx(),
                                          trig_ctdef.rowid_old_expr_,
                                          trig_rtdef.new_record_->get_element()));
    LOG_DEBUG("handle rowid before delete success", K(tg_event));
  }
  return ret;
}

int TriggerHandle::calc_when_condition(
  ObTableModifyOp &dml_op,
  ObTrigDMLRtDef &trig_rtdef,
  uint64_t trigger_id,
  bool &need_fire)
{
  int ret = OB_SUCCESS;
  ObObj result;
  if (OB_ISNULL(trig_rtdef.tg_when_point_params_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(trig_rtdef.tg_when_point_params_));
  } else if (OB_FAIL(calc_trigger_routine(dml_op.get_exec_ctx(),
                           trigger_id, ROUTINE_IDX_CALC_WHEN,
                           *trig_rtdef.tg_when_point_params_, result))) {
    LOG_WARN("failed to cacl trigger routine", K(ret));
  } else {
    need_fire = result.is_true();
    LOG_DEBUG("TRIGGER", K(result), K(need_fire));
  }
  return ret;
}

int TriggerHandle::calc_trigger_routine(
  ObExecContext &exec_ctx,
  uint64_t trigger_id,
  uint64_t routine_id,
  ParamStore &params)
{
  int ret = OB_SUCCESS;
  ObObj result;
  OZ (calc_trigger_routine(exec_ctx, trigger_id, routine_id, params, result));
  return ret;
}

int TriggerHandle::calc_trigger_routine(
  ObExecContext &exec_ctx,
  uint64_t trigger_id,
  uint64_t routine_id,
  ParamStore &params,
  ObObj &result)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> path;
  ObArray<int64_t> nocopy_params;
  trigger_id = ObTriggerInfo::get_trigger_spec_package_id(trigger_id);
  bool old_flag = false;
  common::ObArenaAllocator tmp_allocator(common::ObMemAttr(MTL_ID(), "TriggerExec"));
  CK (OB_NOT_NULL(exec_ctx.get_my_session()));
  OX (old_flag = exec_ctx.get_my_session()->is_for_trigger_package());
  OX (exec_ctx.get_my_session()->set_for_trigger_package(true));
  OV (OB_NOT_NULL(exec_ctx.get_pl_engine()));
  OZ (exec_ctx.get_pl_engine()->execute(
    exec_ctx, tmp_allocator, trigger_id, routine_id, path, params, nocopy_params, result),
      trigger_id, routine_id, params);
  CK (OB_NOT_NULL(exec_ctx.get_my_session()));
  OZ (exec_ctx.get_my_session()->reset_all_package_state_by_dbms_session(true));
  if (exec_ctx.get_my_session()->is_for_trigger_package()) {
    // whether `ret == OB_SUCCESS`, need to restore flag
    exec_ctx.get_my_session()->set_for_trigger_package(old_flag);
  }
  if (OB_SUCC(ret)) {
    bool copy_out_param = lib::is_oracle_mode() ?
                          (ROUTINE_IDX_BEFORE_ROW == routine_id) : (ROUTINE_IDX_BEFORE_ROW_MYSQL == routine_id);
    if (copy_out_param) {
      CK (2 == params.count());
      if (OB_SUCC(ret)) {
        pl::ObPLRecord *new_record = reinterpret_cast<pl::ObPLRecord *>(params.at(1).get_ext());
        CK (OB_NOT_NULL(new_record));
        OZ (new_record->deep_copy(*new_record, exec_ctx.get_allocator()));
      }
    }
  }
  return ret;
}

int TriggerHandle::check_and_update_new_row(
  ObTableModifyOp *self_op,
  const ObTriggerColumnsInfo &columns,
  ObEvalCtx &eval_ctx,
  const ObIArray<ObExpr *> &new_row_exprs,
  pl::ObPLRecord *new_record,
  bool check)
{
  int ret = OB_SUCCESS;
  // plus 1 is for rowid
  if (OB_ISNULL(new_record)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: old or new record is null", K(ret), K(new_record));
  } else {
    bool updated = false;
    int64_t op_row_idx = 0;
    ObObj *new_cells = new_record->get_element();
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.get_count(); i++) {
      if (columns.get_flags()[i].is_rowid_) {
      } else if (check) {
        ObDatum *datum;
        ObObj new_obj;
        if (OB_FAIL(new_row_exprs.at(i)->eval(eval_ctx, datum))) {
          LOG_WARN("failed to eval expr", K(ret));
        } else if (OB_ISNULL(datum))  {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("datum is NULL", K(ret));
        } else if (OB_FAIL(datum->to_obj(new_obj, new_row_exprs.at(i)->obj_meta_))) {
          LOG_WARN("failed to to obj", K(ret));
        } else {
          bool is_strict_equal = false;
          if (new_obj.is_lob_storage()) {
            common::ObArenaAllocator lob_allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
            ObObj cmp_obj;
            ObObj other_obj;
            if (new_obj.is_delta_tmp_lob() || new_cells[i].is_delta_tmp_lob()) {
              is_strict_equal = false;
            } else if (OB_FAIL(ObTextStringIter::convert_outrow_lob_to_inrow_templob(new_obj,
                                                                              cmp_obj,
                                                                              NULL,
                                                                              &lob_allocator))) {
              LOG_WARN("failed to convert lob", K(ret), K(new_obj));
            } else if (OB_FAIL(ObTextStringIter::convert_outrow_lob_to_inrow_templob(new_cells[i],
                                                                              other_obj,
                                                                              NULL,
                                                                              &lob_allocator))) {
              LOG_WARN("failed to convert lob", K(ret), K(i), K(new_cells[i]));
            } else {
              is_strict_equal = cmp_obj.strict_equal(other_obj);
            }
          } else {
            is_strict_equal = new_obj.strict_equal(new_cells[i]);
          }
          if (OB_FAIL(ret)) {
          } else if(!is_strict_equal) {
            if (lib::is_oracle_mode() && !columns.get_flags()[i].is_update_) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "modify column not in update column list in before update row trigger");
            } else {
              updated = true;
            }
          }
        }
      }
    }
    LOG_DEBUG("debug update row", K(updated), K(lbt()));
    // updated
    if (OB_SUCC(ret) && (updated || !check)) {
      self_op->clear_dml_evaluated_flag();
      LOG_DEBUG("debug update row", K(updated), K(check));
    }
    // case: create table t11( c2 generated always as (c1 + 1), c1 int);
    // The generated column c2 is before normal column c1
    // so we need calculate normal column value firstly, then generate column value
    // Firstly calculate the basic columns
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.get_count(); i++) {
      if (!columns.get_flags()[i].is_gen_col_) {
        ObExpr *expr = new_row_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is NULL", K(ret));
        } else {
          ObDatum &write_datum = expr->locate_datum_for_write(eval_ctx);
          if (OB_FAIL(write_datum.from_obj(new_cells[i]))) {
            LOG_WARN("failed to from obj", K(ret));
          } else if (is_lob_storage(new_cells[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(new_cells[i], expr->obj_meta_,
                                                 eval_ctx.exec_ctx_.get_allocator(), write_datum))) {
          LOG_WARN("adjust lob datum failed", K(ret), K(new_cells[i].get_meta()), K(expr->obj_meta_));
        } else {
            expr->set_evaluated_flag(eval_ctx);
            LOG_DEBUG("trigger write new datum", K(new_cells[i]), K(i),
              K(ObToStringExpr(eval_ctx, *new_row_exprs.at(i))));
          }
        }
      }
    }
    // Secondly calculate the generated column
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.get_count(); i++) {
      if (columns.get_flags()[i].is_gen_col_) {
        // for generated column, the dependent column is already evaluated
        // so the generated column can evalate by eval_function
        ObDatum *datum;
        if (OB_FAIL(new_row_exprs.at(i)->eval(eval_ctx, datum))) {
          LOG_WARN("failed to eval expr", K(ret));
        } else {
          LOG_DEBUG("trigger write new datum", K(new_cells[i]), K(i),
            K(ObToStringExpr(eval_ctx, *new_row_exprs.at(i))));
        }
      }
    }
  }
  return ret;
}

int TriggerHandle::do_handle_before_row(
  ObTableModifyOp &dml_op,
  ObDASDMLBaseCtDef &das_base_ctdef,
  const ObTrigDMLCtDef &trig_ctdef,
  ObTrigDMLRtDef &trig_rtdef)
{
  UNUSED(das_base_ctdef);
  int ret = OB_SUCCESS;
  if (trig_ctdef.all_tm_points_.has_before_row()) {
    LOG_DEBUG("debug handle before row");
    uint64_t tg_event = trig_ctdef.tg_event_;
    if (OB_FAIL(do_handle_rowid_before_row(dml_op, trig_ctdef, trig_rtdef, tg_event))) {
      LOG_WARN("do handle rowid before row failed", K(ret), K(tg_event));
    } else {
      ObSQLSessionInfo *my_session = dml_op.get_exec_ctx().get_my_session();
      stmt::StmtType saved_stmt_type = stmt::T_NONE;
      bool need_fire = true;
      LOG_DEBUG("TRIGGER", K(trig_ctdef.tg_args_),
        K(trig_ctdef.all_tm_points_.has_before_row()),
        K(trig_ctdef.all_tm_points_.has_after_row()),
        K(trig_ctdef.all_tm_points_.has_before_point()),
        K(trig_ctdef.all_tm_points_.has_after_point()),
        K(trig_ctdef.all_tm_points_.has_instead_row()));
      OV (OB_NOT_NULL(my_session));
      for (int64_t i = 0; OB_SUCC(ret) && i < trig_ctdef.tg_args_.count(); i++) {
        const ObTriggerArg &tg_arg = trig_ctdef.tg_args_.at(i);
        if (!tg_arg.has_before_row_point() || !tg_arg.has_trigger_events(tg_event)) {
          need_fire = false;
        } else if (tg_arg.has_when_condition()) {
          OZ (calc_when_condition(dml_op, trig_rtdef, tg_arg.get_trigger_id(), need_fire));
        } else {
          need_fire = true;
        }
        LOG_DEBUG("TRIGGER handle before row", K(need_fire), K(i), K(lbt()));
        if (need_fire) {
          if (OB_ISNULL(trig_rtdef.tg_row_point_params_)) {
            ret = OB_NOT_INIT;
            LOG_WARN("trigger row point params is not init", K(ret));
          } else {
            const ObTableModifySpec &modify_spec = static_cast<const ObTableModifySpec&>(dml_op.get_spec());
            if (OB_FAIL(calc_before_row(dml_op, trig_rtdef, tg_arg.get_trigger_id()))) {
              LOG_WARN("failed to calc before row", K(ret));
            } else if ((ObTriggerEvents::is_update_event(tg_event) ||
                  ObTriggerEvents::is_insert_event(tg_event))) {
                if (!trig_ctdef.all_tm_points_.has_instead_row() &&
                    OB_FAIL(check_and_update_new_row(&dml_op,
                                              trig_ctdef.trig_col_info_,
                                              dml_op.get_eval_ctx(),
                                              trig_ctdef.new_row_exprs_,
                                              trig_rtdef.new_record_,
                                              ObTriggerEvents::is_update_event(tg_event)))) {
                  LOG_WARN("failed to check updated new row", K(ret));
              }
            }
            if (OB_SUCC(ret) && trig_ctdef.all_tm_points_.has_instead_row()) {
              GET_PHY_PLAN_CTX(dml_op.get_exec_ctx())->add_affected_rows(1);
              if (ObTriggerEvents::is_update_event(tg_event)) {
                GET_PHY_PLAN_CTX(dml_op.get_exec_ctx())->add_row_matched_count(1);
                GET_PHY_PLAN_CTX(dml_op.get_exec_ctx())->add_row_duplicated_count(1);
              }
            }
            LOG_DEBUG("TRIGGER calc before row", K(need_fire), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int TriggerHandle::calc_after_row(
  ObTableModifyOp &dml_op, ObTrigDMLRtDef &trig_rtdef, uint64_t trigger_id)
{
  int ret = OB_SUCCESS;
  uint64_t idx = lib::is_oracle_mode() ? ROUTINE_IDX_AFTER_ROW : ROUTINE_IDX_AFTER_ROW_MYSQL;
  if (OB_ISNULL(trig_rtdef.tg_row_point_params_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguement", K(ret));
  } else if (OB_FAIL(calc_trigger_routine(dml_op.get_exec_ctx(),
                                          trigger_id, idx,
                                          *trig_rtdef.tg_row_point_params_))) {
    LOG_WARN("failed to calc trigger routine", K(ret));
  }
  return ret;
}

int TriggerHandle::calc_before_row(
  ObTableModifyOp &dml_op, ObTrigDMLRtDef &trig_rtdef, uint64_t trigger_id)
{
  int ret = OB_SUCCESS;
  uint64_t idx = lib::is_oracle_mode() ? ROUTINE_IDX_BEFORE_ROW : ROUTINE_IDX_BEFORE_ROW_MYSQL;
  if (OB_ISNULL(trig_rtdef.tg_row_point_params_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(trig_rtdef.tg_row_point_params_));
  } else if (OB_FAIL(calc_trigger_routine(dml_op.get_exec_ctx(),
                                          trigger_id, idx,
                                          *trig_rtdef.tg_row_point_params_))) {
    LOG_WARN("failed to calc trigger routine", K(ret));
  }
  return ret;
}

int TriggerHandle::calc_before_stmt(
  ObTableModifyOp &dml_op,
  ObTrigDMLRtDef &trig_rtdef,
  uint64_t trigger_id)
{
  UNUSED(trig_rtdef);
  int ret = OB_SUCCESS;
  ParamStore params;
  if (OB_FAIL(calc_trigger_routine(dml_op.get_exec_ctx(),
                                   trigger_id, ROUTINE_IDX_BEFORE_STMT, params))) {
    LOG_WARN("failed to calc trigger routine", K(ret));
  }
  return ret;
}

int TriggerHandle::do_handle_after_stmt(
  ObTableModifyOp &dml_op,
  const ObTrigDMLCtDef &trig_ctdef,
  ObTrigDMLRtDef &trig_rtdef,
  uint64_t tg_event)
{
  int ret = OB_SUCCESS;
  if (trig_ctdef.all_tm_points_.has_after_stmt()) {
    LOG_DEBUG("TRIGGER", K(trig_ctdef.tg_args_));
    for (int64_t i = 0; OB_SUCC(ret) && i < trig_ctdef.tg_args_.count(); i++) {
      const ObTriggerArg &tg_arg = trig_ctdef.tg_args_.at(i);
      if (tg_arg.has_after_stmt_point() && tg_arg.has_trigger_events(tg_event)) {
        OZ (calc_after_stmt(dml_op, trig_rtdef, tg_arg.get_trigger_id()), ret);
      }
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = destroy_compound_trigger_state(dml_op.get_exec_ctx(), trig_ctdef))) {
    LOG_WARN("destroy compound trigger state failed", K(tmp_ret), K(ret));
  }
  return ret;
}

int TriggerHandle::calc_after_stmt(
  ObTableModifyOp &dml_op, ObTrigDMLRtDef &trig_rtdef, uint64_t trigger_id)
{
  UNUSED(trig_rtdef);
  int ret = OB_SUCCESS;
  ParamStore params;
  OZ (calc_trigger_routine(dml_op.get_exec_ctx(),
                           trigger_id, ROUTINE_IDX_AFTER_STMT, params));
  return ret;
}

// only the first trigger need run statement trigger
int TriggerHandle::do_handle_before_stmt(
  ObTableModifyOp &dml_op,
  const ObTrigDMLCtDef &trig_ctdef,
  ObTrigDMLRtDef &trig_rtdef,
  uint64_t tg_event)
{
  int ret = OB_SUCCESS;
  if (trig_ctdef.all_tm_points_.has_before_stmt()) {
    LOG_DEBUG("TRIGGER", K(trig_ctdef.tg_args_));
    for (int64_t i = 0; OB_SUCC(ret) && i < trig_ctdef.tg_args_.count(); i++) {
      const ObTriggerArg &tg_arg = trig_ctdef.tg_args_.at(i);
      if (tg_arg.has_before_stmt_point() && tg_arg.has_trigger_events(tg_event)) {
        if (OB_FAIL(calc_before_stmt(dml_op, trig_rtdef, tg_arg.get_trigger_id()))) {
          LOG_WARN("failed to calc befeore stmt", K(ret));
        }
      }
    }
  }
  return ret;
}

// to compatible with oralce
int TriggerHandle::do_handle_rowid_after_row(
  ObTableModifyOp &dml_op,
  const ObTrigDMLCtDef &trig_ctdef,
  ObTrigDMLRtDef &trig_rtdef,
  uint64_t tg_event)
{
  int ret = OB_SUCCESS;
  if (NULL != trig_ctdef.rowid_new_expr_ && ObTriggerEvents::is_insert_event(tg_event)) {
    // old.rowid should be same with new.rowid for insert
    trig_ctdef.rowid_new_expr_->get_eval_info(dml_op.get_eval_ctx()).clear_evaluated_flag();
    OZ (TriggerHandle::set_rowid_into_row(trig_ctdef.trig_col_info_,
                                          dml_op.get_eval_ctx(),
                                          trig_ctdef.rowid_new_expr_,
                                          trig_rtdef.old_record_->get_element()));
    OZ (TriggerHandle::set_rowid_into_row(trig_ctdef.trig_col_info_,
                                          dml_op.get_eval_ctx(),
                                          trig_ctdef.rowid_new_expr_,
                                          trig_rtdef.new_record_->get_element()));
    LOG_DEBUG("handle rowid after insert success", K(tg_event));
  } else if (NULL != trig_ctdef.rowid_old_expr_ && ObTriggerEvents::is_delete_event(tg_event)) {
      // new.rowid should be same with old.rowid
    OZ (TriggerHandle::set_rowid_into_row(trig_ctdef.trig_col_info_,
                                          dml_op.get_eval_ctx(),
                                          trig_ctdef.rowid_old_expr_,
                                          trig_rtdef.new_record_->get_element()));
    LOG_DEBUG("handle rowid after delete success", K(tg_event));
  }
  return ret;
}

int TriggerHandle::do_handle_after_row(
  ObTableModifyOp &dml_op,
  const ObTrigDMLCtDef &trig_ctdef,
  ObTrigDMLRtDef &trig_rtdef,
  uint64_t tg_event)
{
  int ret = OB_SUCCESS;
  if (trig_ctdef.all_tm_points_.has_after_row()) {
    if (OB_FAIL(do_handle_rowid_after_row(dml_op, trig_ctdef, trig_rtdef, tg_event))) {
      LOG_WARN("do handle rowid after row failed", K(ret), K(tg_event));
    } else {
      bool need_fire = false;
      LOG_DEBUG("TRIGGER", K(trig_ctdef.tg_args_));
      for (int64_t i = 0; OB_SUCC(ret) && i < trig_ctdef.tg_args_.count(); i++) {
        const ObTriggerArg &tg_arg = trig_ctdef.tg_args_.at(i);
        if (!tg_arg.has_after_row_point() || !tg_arg.has_trigger_events(tg_event)) {
          need_fire = false;
        } else if (tg_arg.has_when_condition()) {
          OZ (calc_when_condition(dml_op, trig_rtdef, tg_arg.get_trigger_id(), need_fire));
        } else {
          need_fire = true;
        }
        LOG_DEBUG("TRIGGER", K(need_fire));
        if (need_fire) {
          OZ (calc_after_row(dml_op, trig_rtdef, tg_arg.get_trigger_id()));
        }
      }
    }
  }
  return ret;
}

int TriggerHandle::destroy_compound_trigger_state(ObExecContext &exec_ctx, const ObTrigDMLCtDef &trig_ctdef)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = exec_ctx.get_my_session();
  ObSchemaGetterGuard *schema_guard = exec_ctx.get_sql_ctx()->schema_guard_;
  OV (OB_NOT_NULL(session_info) && OB_NOT_NULL(schema_guard));
  for (int64_t i = 0; OB_SUCC(ret) && i < trig_ctdef.tg_args_.count(); i++) {
    uint64_t trg_id = trig_ctdef.tg_args_.at(i).get_trigger_id();
    const ObTriggerInfo *trg_info = NULL;
    OZ (schema_guard->get_trigger_info(session_info->get_effective_tenant_id(), trg_id, trg_info), trg_id);
    OV (OB_NOT_NULL(trg_info));
    if (OB_SUCC(ret) && trg_info->is_compound_dml_type()) {
      OZ (pl::ObPLPackageManager::destory_package_state(*session_info,
                                                        ObTriggerInfo::get_trigger_body_package_id(trg_id)));
      LOG_DEBUG("destroy trigger state", K(trg_id), K(ret));
    }
  }
  return ret;
}

int64_t TriggerHandle::get_routine_param_count(const uint64_t routine_id)
{
  int64_t count = OB_INVALID_COUNT;
  if (lib::is_oracle_mode()
      && (ROUTINE_IDX_BEFORE_STMT == routine_id
          || ROUTINE_IDX_AFTER_STMT == routine_id)) {
    count = STMT_POINT_PARAM_COUNT;
  } else {
    count = ROW_POINT_PARAM_COUNT;
  }
  return count;
}

}  // namespace sql
}  // namespace oceanbase
