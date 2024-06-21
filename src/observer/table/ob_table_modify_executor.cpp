/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_modify_executor.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/das/ob_das_insert_op.h"
#include "ob_htable_utils.h"
#include "ob_table_cg_service.h"

using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{

int ObTableApiModifyExecutor::check_row_null(const ObExprPtrIArray &row, const ColContentIArray &column_infos)
{
  int ret = OB_SUCCESS;

  if (row.count() < column_infos.count()) { // column_infos count less than row count when do update
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid row count", K(ret), K(row), K(column_infos));
  }

  for (int i = 0; OB_SUCC(ret) && i < column_infos.count(); i++) {
    ObDatum *datum = NULL;
    const bool is_nullable = column_infos.at(i).is_nullable_;
    uint64_t col_idx = column_infos.at(i).projector_index_;
    if (OB_FAIL(row.at(col_idx)->eval(eval_ctx_, datum))) {
      LOG_WARN("fail to eval datum", K(ret), K(row), K(column_infos), K(col_idx));
    } else if (!is_nullable && datum->is_null()) {
      const ObString &column_name = column_infos.at(i).column_name_;
      ret = OB_BAD_NULL_ERROR;
      LOG_USER_ERROR(OB_BAD_NULL_ERROR, column_name.length(), column_name.ptr());
      LOG_WARN("bad null error", K(ret), K(row), K(column_name));
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_das_ref())) {
    LOG_WARN("fail to init das dml ctx", K(ret));
  } else {
    is_opened_ = true;
  }
  return ret;
}

OB_INLINE int ObTableApiModifyExecutor::init_das_ref()
{
  int ret = OB_SUCCESS;
  ObDASRef &das_ref = dml_rtctx_.das_ref_;
  ObSQLSessionInfo *session = GET_MY_SESSION(exec_ctx_);

  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session", K(ret));
  } else {
    // todo@dazhi: use different labels for each dml type
    const char *label = "DmlDASCtx";
    const bool use_dist_das = false;
    ObMemAttr memattr(session->get_effective_tenant_id(), label, ObCtxIds::EXECUTE_CTX_ID);
    // das_ref.set_expr_frame_info(expr_frame_info_);
    das_ref.set_mem_attr(memattr);
    das_ref.set_execute_directly(!use_dist_das);
  }

  return OB_SUCCESS;
}

int ObTableApiModifyExecutor::submit_all_dml_task()
{
  int ret = OB_SUCCESS;
  ObDASRef &das_ref = dml_rtctx_.das_ref_;

  if (das_ref.has_task()) {
    if (OB_FAIL(das_ref.execute_all_task())) {
      LOG_WARN("fail to execute all dml das tasks", K(ret));
    } else if (OB_FAIL(das_ref.close_all_task())) {
      LOG_WARN("fail to close all dml tasks", K(ret));
    } else {
      das_ref.reuse();
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::close()
{
  int ret = OB_SUCCESS;

  if (!is_opened_) {
    // do nothing
  } else {
    ObDASRef &das_ref = dml_rtctx_.das_ref_;
    ObPhysicalPlanCtx *plan_ctx = tb_ctx_.get_exec_ctx().get_physical_plan_ctx();
    if (OB_NOT_NULL(plan_ctx)) {
      share::ObAutoincrementService &auto_service = share::ObAutoincrementService::get_instance();
      ObIArray<AutoincParam> &auto_params = plan_ctx->get_autoinc_params();
      for (int64_t i = 0; i < auto_params.count(); ++i) {
        if (OB_NOT_NULL(auto_params.at(i).cache_handle_)) {
          auto_service.release_handle(auto_params.at(i).cache_handle_);
        }
      }
    }

    if (das_ref.has_task()) {
      if (OB_FAIL(das_ref.close_all_task())) {
        LOG_WARN("fail to close all insert das task", K(ret));
      } else {
        das_ref.reset();
      }
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::init_das_dml_rtdef(const ObDASDMLBaseCtDef &das_ctdef,
                                                 ObDASDMLBaseRtDef &das_rtdef,
                                                 const ObDASTableLocMeta *loc_meta)
{
  int ret = OB_SUCCESS;

  ObDASRef &das_ref = dml_rtctx_.das_ref_;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(das_ref.get_exec_ctx());
  ObDASCtx &das_ctx = das_ref.get_exec_ctx().get_das_ctx();
  uint64_t table_loc_id = das_ctdef.table_id_;
  uint64_t ref_table_id = das_ctdef.index_tid_;
  das_rtdef.timeout_ts_ = tb_ctx_.get_timeout_ts();
  das_rtdef.prelock_ = my_session->get_prelock();
  das_rtdef.tenant_schema_version_ = tb_ctx_.get_tenant_schema_version();
  das_rtdef.sql_mode_ = my_session->get_sql_mode();
  das_rtdef.table_loc_ = das_ctx.get_table_loc_by_id(table_loc_id, ref_table_id);
  if (OB_ISNULL(das_rtdef.table_loc_)) {
    if (OB_ISNULL(loc_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("loc meta is null", K(ret), K(table_loc_id),
          K(ref_table_id), K(das_ctx.get_table_loc_list()));
    } else if (OB_FAIL(das_ctx.extended_table_loc(*loc_meta, das_rtdef.table_loc_))) {
      LOG_WARN("extended table location failed", K(ret), KPC(loc_meta));
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::init_related_das_rtdef(const DASDMLCtDefArray &das_ctdefs,
                                                     DASDMLRtDefArray &das_rtdefs)
{
  int ret = OB_SUCCESS;
  ObDASRef &das_ref = dml_rtctx_.das_ref_;
  int64_t ct_count = das_ctdefs.count();

  if (!das_ctdefs.empty()) {
    ObIAllocator &allocator = das_ref.get_exec_ctx().get_allocator();
    if (OB_FAIL(das_rtdefs.allocate_array(allocator, ct_count))) {
      SQL_DAS_LOG(WARN, "fail to create das insert rtdef array", K(ret), K(ct_count));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ct_count; ++i) {
    ObDASTaskFactory &das_factory = das_ref.get_exec_ctx().get_das_ctx().get_das_factory();
    ObDASBaseRtDef *das_rtdef = nullptr;
    if (OB_FAIL(das_factory.create_das_rtdef(das_ctdefs.at(i)->op_type_, das_rtdef))) {
      SQL_DAS_LOG(WARN, "fail to create das insert rtdef", K(ret));
    } else if (OB_FAIL(init_das_dml_rtdef(*das_ctdefs.at(i),
                                          static_cast<ObDASDMLBaseRtDef&>(*das_rtdef),
                                          nullptr))) {
      SQL_DAS_LOG(WARN, "fail to init das dml rtdef", K(ret));
    } else {
      das_rtdefs.at(i) = static_cast<ObDASDMLBaseRtDef*>(das_rtdef);
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::calc_tablet_loc(ObDASTabletLoc *&tablet_loc)
{
  int ret = OB_SUCCESS;
  ObTableID table_loc_id = get_table_ctx().get_ref_table_id();
  ObTableID ref_table_id = get_table_ctx().get_ref_table_id();;
  ObDASCtx &das_ctx = exec_ctx_.get_das_ctx();
  ObDASTableLoc *table_loc = nullptr;

  if (OB_ISNULL(table_loc = das_ctx.get_table_loc_by_id(table_loc_id, ref_table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get table location by table id failed", K(ret),
              K(table_loc_id), K(ref_table_id), K(das_ctx.get_table_loc_list()));
  } else {
    tablet_loc = table_loc->get_first_tablet_loc();
  }

  return ret;
}

int ObTableApiModifyExecutor::generate_ins_rtdef(const ObTableInsCtDef &ins_ctdef,
                                                 ObTableInsRtDef &ins_rtdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_das_dml_rtdef(ins_ctdef.das_ctdef_,
                                 ins_rtdef.das_rtdef_,
                                 nullptr))) {
    LOG_WARN("fail to init das dml rtdef", K(ret));
  } else if (OB_FAIL(init_related_das_rtdef(ins_ctdef.related_ctdefs_,
                                            ins_rtdef.related_rtdefs_))) {
    LOG_WARN("fail to init related das ctdef", K(ret));
  } else {
    ins_rtdef.das_rtdef_.related_ctdefs_ = &ins_ctdef.related_ctdefs_;
    ins_rtdef.das_rtdef_.related_rtdefs_ = &ins_rtdef.related_rtdefs_;
  }

  return ret;
}

int ObTableApiModifyExecutor::generate_del_rtdef(const ObTableDelCtDef &del_ctdef,
                                                 ObTableDelRtDef &del_rtdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_das_dml_rtdef(del_ctdef.das_ctdef_,
                                 del_rtdef.das_rtdef_,
                                 nullptr))) {
    LOG_WARN("fail to init das dml rtdef", K(ret));
  } else if (OB_FAIL(init_related_das_rtdef(del_ctdef.related_ctdefs_,
                                            del_rtdef.related_rtdefs_))) {
    LOG_WARN("fail to init related das ctdef", K(ret));
  } else {
    del_rtdef.das_rtdef_.related_ctdefs_ = &del_ctdef.related_ctdefs_;
    del_rtdef.das_rtdef_.related_rtdefs_ = &del_rtdef.related_rtdefs_;
  }

  return ret;
}

int ObTableApiModifyExecutor::generate_upd_rtdef(const ObTableUpdCtDef &upd_ctdef,
                                                 ObTableUpdRtDef &upd_rtdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_das_dml_rtdef(upd_ctdef.das_ctdef_,
                                 upd_rtdef.das_rtdef_,
                                 nullptr))) {
    LOG_WARN("fail to init das dml rtdef", K(ret));
  } else if (OB_FAIL(init_related_das_rtdef(upd_ctdef.related_ctdefs_,
                                            upd_rtdef.related_rtdefs_))) {
    LOG_WARN("fail to init related das ctdef", K(ret));
  } else {
    upd_rtdef.das_rtdef_.related_ctdefs_ = &upd_ctdef.related_ctdefs_;
    upd_rtdef.das_rtdef_.related_rtdefs_ = &upd_rtdef.related_rtdefs_;
    dml_rtctx_.get_exec_ctx().set_update_columns(&upd_ctdef.assign_columns_);
  }

  return ret;
}

int ObTableApiModifyExecutor::insert_row_to_das(const ObTableInsCtDef &ins_ctdef,
                                                ObTableInsRtDef &ins_rtdef)
{
  int ret = OB_SUCCESS;
  ObDASTabletLoc *tablet_loc = nullptr;
  ObChunkDatumStore::StoredRow* stored_row = nullptr;
  if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
    LOG_WARN("fail to calc partition key", K(ret));
  } else if (OB_FAIL(check_row_null(ins_ctdef.new_row_, ins_ctdef.column_infos_))) {
    LOG_WARN("fail to check row nullable", K(ret));
  } else if (OB_FAIL(ObDMLService::insert_row(ins_ctdef.das_ctdef_,
                                              ins_rtdef.das_rtdef_,
                                              tablet_loc,
                                              dml_rtctx_,
                                              ins_ctdef.new_row_,
                                              stored_row))) {
    LOG_WARN("fail to insert row by dml service", K(ret));
  }

  return ret;
}

int ObTableApiModifyExecutor::delete_row_to_das(const ObTableDelCtDef &del_ctdef,
                                                ObTableDelRtDef &del_rtdef)
{
  int ret = OB_SUCCESS;
  ObDASTabletLoc *tablet_loc = nullptr;
  ObChunkDatumStore::StoredRow* stored_row = nullptr;
  // todo:linjing check rowkey null and skip
  if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
    LOG_WARN("fail tp calc tablet location", K(ret));
  } else if (OB_FAIL(ObDMLService::delete_row(del_ctdef.das_ctdef_,
                                              del_rtdef.das_rtdef_,
                                              tablet_loc,
                                              dml_rtctx_,
                                              del_ctdef.old_row_,
                                              stored_row))) {
    LOG_WARN("fail to delete row to das op", K(ret), K(del_ctdef), K(del_rtdef));
  }

  return ret;
}

int ObTableApiModifyExecutor::get_next_conflict_rowkey(DASTaskIter &task_iter,
                                                       const ObConflictChecker &conflict_checker)
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
        LOG_WARN("fail to get next row from das result", K(ret));
      }
    } else if (OB_FAIL(ssr.init(dml_rtctx_.get_das_alloc(), ins_ctdef->table_rowkey_types_, false))) {
      LOG_WARN("fail to init shadow stored row", K(ret), K(ins_ctdef->table_rowkey_types_));
    } else if (OB_FAIL(ssr.shadow_copy(*dup_row))) {
      LOG_WARN("fail to shadow copy ob new row", K(ret));
    } else if (OB_ISNULL(stored_row = ssr.get_store_row())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("stored row is null", K(ret));
    } else if (OB_FAIL(stored_row_to_exprs(*stored_row,
                                           conflict_checker.checker_ctdef_.data_table_rowkey_expr_,
                                           conflict_checker.eval_ctx_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to fill exprs by stored row", K(ret));
      }
    } else {
      got_row = true;
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::modify_htable_timestamp()
{
  int ret = OB_SUCCESS;
  int64_t now_ms = -ObHTableUtils::current_time_millis();
  const ObITableEntity *entity = static_cast<const ObITableEntity*>(tb_ctx_.get_entity());
  if (entity->get_rowkey_size() != 3) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("htable should be with 3 rowkey columns", K(ret), K(entity));
  } else {
    ObRowkey rowkey = entity->get_rowkey();
    ObObj &t_obj = const_cast<ObObj&>(rowkey.get_obj_ptr()[ObHTableConstants::COL_IDX_T]);  // column T
    ObHTableCellEntity3 htable_cell(entity);
    bool row_is_null = htable_cell.last_get_is_null();
    int64_t timestamp = htable_cell.get_timestamp();
    bool timestamp_is_null = htable_cell.last_get_is_null();
    if (row_is_null || timestamp_is_null) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument for htable put", K(ret), K(row_is_null), K(timestamp_is_null));
    } else if (ObHTableConstants::LATEST_TIMESTAMP == timestamp) { // update timestamp iff LATEST_TIMESTAMP
      t_obj.set_int(now_ms);
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::fetch_conflict_rowkey(sql::ObConflictChecker &conflict_checker)
{
  int ret = OB_SUCCESS;
  DASTaskIter task_iter = dml_rtctx_.das_ref_.begin_task_iter();

  while (OB_SUCC(ret) && !task_iter.is_end()) {
    if (OB_FAIL(get_next_conflict_rowkey(task_iter, conflict_checker))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next conflict rowkey from das_result", K(ret));
      }
    } else if (OB_FAIL(conflict_checker.build_primary_table_lookup_das_task())) {
      LOG_WARN("fail to build lookup_das_task", K(ret));
    }
  }

  ret = (ret == OB_ITER_END ? OB_SUCCESS : ret);
  return ret;
}

int ObTableApiModifyExecutor::reset_das_env(ObTableInsRtDef &ins_rtdef)
{
  int ret = OB_SUCCESS;

  // 释放第一次try insert的das task
  if (OB_FAIL(dml_rtctx_.das_ref_.close_all_task())) {
    LOG_WARN("close all das task failed", K(ret));
  } else {
    dml_rtctx_.das_ref_.reuse();
    // 第二次插入不需要fetch conflict result
    ins_rtdef.das_rtdef_.need_fetch_conflict_ = false;
    ins_rtdef.das_rtdef_.is_duplicated_ = false;
  }

  return ret;
}

int ObTableApiModifyExecutor::check_whether_row_change(const ObChunkDatumStore::StoredRow &upd_old_row,
                                                       const ObChunkDatumStore::StoredRow &upd_new_row,
                                                       const ObTableUpdCtDef &upd_ctdef,
                                                       bool &is_row_changed)
{
  int ret = OB_SUCCESS;

  if (tb_ctx_.is_inc_or_append()) {
    is_row_changed = true;
  } else if (lib::is_mysql_mode()) {
    const ObExprPtrIArray &old_row = upd_ctdef.old_row_;
    const ObExprPtrIArray &new_row = upd_ctdef.new_row_;
    FOREACH_CNT_X(info, upd_ctdef.assign_columns_, OB_SUCC(ret) && !is_row_changed) {
      const uint64_t idx = info->projector_index_;
      if (idx >= upd_old_row.cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid assign idx", K(ret), K(idx), K(upd_old_row.cnt_));
      } else {
        is_row_changed = !ObDatum::binary_equal(upd_old_row.cells()[idx], upd_new_row.cells()[idx]);
      }
    }
  } else {
    //in oracle mode, no matter whether the updated row is changed or not,
    //the row will be updated in the storage
    is_row_changed = true;
  }

  return ret;
}

// if common column equal, check rowkey column, if not equal then report error
int ObTableApiModifyExecutor::check_rowkey_change(const ObChunkDatumStore::StoredRow &upd_old_row,
                                                  const ObChunkDatumStore::StoredRow &upd_new_row)
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    if (OB_UNLIKELY(upd_old_row.cnt_ != upd_new_row.cnt_)
        || OB_UNLIKELY(upd_old_row.cnt_ != tb_ctx_.get_column_items().count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check column size failed", K(ret), K(upd_old_row.cnt_),
                K(upd_new_row.cnt_), K(tb_ctx_.get_column_items().count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tb_ctx_.get_column_items().count(); i++) {
      ObTableColumnItem &item = tb_ctx_.get_column_items().at(i);
      if (item.rowkey_position_ <= 0) {
        // do nothing
      } else if (!ObDatum::binary_equal(upd_old_row.cells()[i], upd_new_row.cells()[i])) {
        ret = OB_ERR_UPDATE_ROWKEY_COLUMN;
        LOG_USER_ERROR(OB_ERR_UPDATE_ROWKEY_COLUMN);
        LOG_WARN("can not update rowkey column", K(ret));
      }
    }
  }
  return ret;
}

int ObTableApiModifyExecutor::to_expr_skip_old(const ObChunkDatumStore::StoredRow &store_row,
                                               const ObTableUpdCtDef &upd_ctdef)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &new_row = upd_ctdef.new_row_;
  const ObIArray<ObTableColumnItem>& column_items = tb_ctx_.get_column_items();
  if (OB_UNLIKELY(store_row.cnt_ != new_row.count()) || OB_UNLIKELY(new_row.count() != column_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum count mismatch", K(ret), K(store_row.cnt_), K(new_row.count()), K(column_items.count()));
  } else {
    // 1. refresh rowkey expr datum
    // not always the primary key is the prefix of table schema
    // e.g., create table test(a varchar(1024), b int primary key);
    for (uint64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
      const ObExpr *expr = new_row.at(i);
      if (column_items.at(i).rowkey_position_ > 0) {
        expr->locate_expr_datum(eval_ctx_) = store_row.cells()[i];
        expr->get_eval_info(eval_ctx_).evaluated_ = true;
        expr->get_eval_info(eval_ctx_).projected_ = true;
      }
    }

    // 2. refresh assign column expr datum
    const ObIArray<ObTableAssignment> &assigns = tb_ctx_.get_assignments();
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
      const ObTableAssignment &assign = assigns.at(i);
      if (OB_ISNULL(assign.column_item_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assign column item is null", K(ret), K(assign));
      } else if (new_row.count() < assign.column_item_->col_idx_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected assign projector_index_", K(ret), K(new_row), K(assign.column_item_));
      } else {
        ObExpr *expr = new_row.at(assign.column_item_->col_idx_);
        if (OB_ISNULL(expr)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("expr is null", K(ret));
        } else if (assign.column_item_->is_generated_column_) {
          // do nothing, generated column not need to fill
        } else if (assign.column_item_->auto_filled_timestamp_ && !assign.is_assigned_) {
          ObDatum *tmp_datum = nullptr;
          if (OB_FAIL(expr->eval(eval_ctx_, tmp_datum))) {
            LOG_WARN("fail to eval current timestamp expr", K(ret));
          }
        } else {
          expr->locate_expr_datum(eval_ctx_) = store_row.cells()[assign.column_item_->col_idx_];
          expr->get_eval_info(eval_ctx_).evaluated_ = true;
          expr->get_eval_info(eval_ctx_).projected_ = true;
          }
      }
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::generate_del_rtdef_for_update(const ObTableUpdCtDef &upd_ctdef,
                                                            ObTableUpdRtDef &upd_rtdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_das_dml_rtdef(*upd_ctdef.ddel_ctdef_,
                                 *upd_rtdef.ddel_rtdef_,
                                 nullptr))) {
    LOG_WARN("fail to init das dml rtdef", K(ret));
  } else if (OB_FAIL(init_related_das_rtdef(upd_ctdef.related_del_ctdefs_,
                                            upd_rtdef.related_del_rtdefs_))) {
    LOG_WARN("fail to init related das ctdef", K(ret));
  } else {
    upd_rtdef.ddel_rtdef_->related_ctdefs_ = &upd_ctdef.related_del_ctdefs_;
    upd_rtdef.ddel_rtdef_->related_rtdefs_ = &upd_rtdef.related_del_rtdefs_;
  }

  return ret;
}

int ObTableApiModifyExecutor::generate_ins_rtdef_for_update(const ObTableUpdCtDef &upd_ctdef,
                                                            ObTableUpdRtDef &upd_rtdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_das_dml_rtdef(*upd_ctdef.dins_ctdef_,
                                 *upd_rtdef.dins_rtdef_,
                                 nullptr))) {
    LOG_WARN("fail to init das dml rtdef", K(ret));
  } else if (OB_FAIL(init_related_das_rtdef(upd_ctdef.related_ins_ctdefs_,
                                            upd_rtdef.related_ins_rtdefs_))) {
    LOG_WARN("fail to init related das ctdef", K(ret));
  } else {
    upd_rtdef.dins_rtdef_->related_ctdefs_ = &upd_ctdef.related_ins_ctdefs_;
    upd_rtdef.dins_rtdef_->related_rtdefs_ = &upd_rtdef.related_ins_rtdefs_;
  }

  return ret;
}

int ObTableApiModifyExecutor::delete_upd_old_row_to_das(const ObRowkey &constraint_rowkey,
                                                        const sql::ObConflictValue &constraint_value,
                                                        const ObTableUpdCtDef &upd_ctdef,
                                                        ObTableUpdRtDef &upd_rtdef,
                                                        sql::ObDMLRtCtx &dml_rtctx)
{
  int ret = OB_SUCCESS;
  ObDASTabletLoc *tablet_loc = nullptr;

  if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
    LOG_WARN("fail to calc tablet location", K(ret));
  } else {
    if (OB_ISNULL(upd_ctdef.ddel_ctdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddel_ctdef can't be null", K(ret));
    } else if (OB_ISNULL(upd_rtdef.ddel_rtdef_)) {
      if (OB_FAIL(ObDASTaskFactory::alloc_das_rtdef(DAS_OP_TABLE_DELETE,
                                                    tb_ctx_.get_allocator(),
                                                    upd_rtdef.ddel_rtdef_))) {
        LOG_WARN("fail to create das delete rtdef", K(ret));
      } else if (OB_FAIL(generate_del_rtdef_for_update(upd_ctdef, upd_rtdef))) {
        LOG_WARN("fail to generate del rtdef for update", K(ret), K(upd_ctdef), K(upd_rtdef));
      }
    }
    ObChunkDatumStore::StoredRow* stored_row = nullptr;
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_ISNULL(upd_rtdef.ddel_rtdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddel_rtdef is null", K(ret));
    } else if (OB_FAIL(ObDMLService::delete_row(*upd_ctdef.ddel_ctdef_,
                                                *upd_rtdef.ddel_rtdef_,
                                                tablet_loc,
                                                dml_rtctx,
                                                upd_ctdef.old_row_,
                                                stored_row))) {
      LOG_WARN("fail to delete row with das", K(ret));
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::insert_upd_new_row_to_das(const ObTableUpdCtDef &upd_ctdef,
                                                        ObTableUpdRtDef &upd_rtdef,
                                                        sql::ObDMLRtCtx &dml_rtctx)
{
  int ret = OB_SUCCESS;
  ObDASTabletLoc *tablet_loc = nullptr;

  if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
    LOG_WARN("fail to calc tablet location", K(ret));
  } else {
    if (OB_ISNULL(upd_ctdef.dins_ctdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dins_ctdef_ can't be null", K(ret));
    } else if (OB_ISNULL(upd_rtdef.dins_rtdef_)) {
      if (OB_FAIL(ObDASTaskFactory::alloc_das_rtdef(DAS_OP_TABLE_INSERT,
                                                    tb_ctx_.get_allocator(),
                                                    upd_rtdef.dins_rtdef_))) {
        LOG_WARN("fail to create das insert rtdef", K(ret));
      } else if (OB_FAIL(generate_ins_rtdef_for_update(upd_ctdef, upd_rtdef))) {
        LOG_WARN("fail to generate del rtdef for update", K(ret), K(upd_ctdef), K(upd_rtdef));
      }
    }

    clear_evaluated_flag();
    ObChunkDatumStore::StoredRow* stored_row = nullptr;
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_ISNULL(upd_rtdef.dins_rtdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dins_rtdef_ is null", K(ret));
    } else if (OB_FAIL(check_row_null(upd_ctdef.new_row_, upd_ctdef.assign_columns_))) {
      LOG_WARN("fail to check row nullable", K(ret));
    } else if (OB_FAIL(ObDMLService::insert_row(*upd_ctdef.dins_ctdef_,
                                                *upd_rtdef.dins_rtdef_,
                                                tablet_loc,
                                                dml_rtctx,
                                                upd_ctdef.new_row_,
                                                stored_row))) {
      LOG_WARN("fail to insert row with das", K(ret));
    }
  }

  return ret;
}

int ObTableApiModifyExecutor::execute_das_task(ObDMLRtCtx &dml_rtctx, bool del_task_ahead)
{
  int ret = OB_SUCCESS;

  if (dml_rtctx.das_ref_.has_task()) {
    if (del_task_ahead) {
      if (OB_FAIL(dml_rtctx.das_ref_.pick_del_task_to_first())) {
        LOG_WARN("fail to remove delete das task first", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml_rtctx.das_ref_.execute_all_task())) {
        LOG_WARN("fail to execute all das task", K(ret));
      }
    }
  }

  return ret;
}

void ObTableApiModifyExecutor::set_need_fetch_conflict(sql::ObDMLRtCtx &upd_rtctx, ObTableInsRtDef &ins_rtdef)
{
  ins_rtdef.das_rtdef_.need_fetch_conflict_ = true;
  dml_rtctx_.set_non_sub_full_task();
  upd_rtctx.set_pick_del_task_first();
  upd_rtctx.set_non_sub_full_task();
}

int ObTableApiModifyExecutor::stored_row_to_exprs(const ObChunkDatumStore::StoredRow &row,
                                                  const ObIArray<ObExpr*> &exprs,
                                                  ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableColumnItem>& column_items = tb_ctx_.get_column_items();

  if (OB_UNLIKELY(row.cnt_ != exprs.count()) && OB_UNLIKELY(row.cnt_ != column_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum count mismatch", K(ret), K(row.cnt_), K(exprs.count()), K(column_items.count()));
  } else {
    for (uint32_t i = 0; i < row.cnt_; ++i) {
      if (column_items.at(i).is_generated_column_) {
        // generate column need to clear the evaluated flag
        exprs.at(i)->clear_evaluated_flag(ctx);
      } else {
        exprs.at(i)->locate_expr_datum(ctx) = row.cells()[i];
        exprs.at(i)->set_evaluated_projected(ctx);
      }
    }
  }
  return ret;
}

void ObTableApiModifyExecutor::reset_new_row_datum(const ObExprPtrIArray &new_row_exprs)
{
  clear_evaluated_flag();
  // reset ptr in ObDatum to reserved buf
  for (int64_t i = 0; i < new_row_exprs.count(); ++i) {
    if (OB_NOT_NULL(new_row_exprs.at(i))) {
      // locate expr datum && reset ptr_ to reserved buf
      new_row_exprs.at(i)->locate_datum_for_write(eval_ctx_);
    }
  }
}

}  // namespace table
}  // namespace oceanbase