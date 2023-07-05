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
#include "ob_table_insert_up_executor.h"
#include "ob_table_cg_service.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/das/ob_das_insert_op.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "ob_htable_utils.h"
using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{
int ObTableApiInsertUpExecutor::generate_insert_up_rtdef(const ObTableInsUpdCtDef &ctdef,
                                                         ObTableInsUpdRtDef &rtdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(generate_ins_rtdef(ctdef.ins_ctdef_, rtdef.ins_rtdef_))) {
    LOG_WARN("fail to generate insert rtdef", K(ret));
  } else if (OB_FAIL(generate_upd_rtdef(ctdef.upd_ctdef_,
                                        rtdef.upd_rtdef_))) {
    LOG_WARN("fail to generate update rtdef", K(ret));
  } else {
    rtdef.ins_rtdef_.das_rtdef_.table_loc_->is_writing_ = true;
  }

  return ret;
}

int ObTableApiInsertUpExecutor::modify_htable_timestamp()
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

int ObTableApiInsertUpExecutor::open()
{
  int ret = OB_SUCCESS;

  const ObSQLSessionInfo &session = tb_ctx_.get_session_info();
  if (OB_FAIL(ObTableApiModifyExecutor::open())) {
    LOG_WARN("fail to oepn ObTableApiModifyExecutor", K(ret));
  } else if (OB_FAIL(generate_insert_up_rtdef(insert_up_spec_.get_ctdef(), insert_up_rtdef_))) {
    LOG_WARN("fail to init insert up rtdef", K(ret));
  } else {
    ObDASTabletLoc *tablet_loc = nullptr;
    ObDASTableLoc *table_loc = insert_up_rtdef_.ins_rtdef_.das_rtdef_.table_loc_;
    if (OB_ISNULL(table_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table location is invalid", K(ret));
    } else if (OB_FAIL(conflict_checker_.init_conflict_checker(insert_up_spec_.get_expr_frame_info(),
                                                               table_loc))) {
      LOG_WARN("fail to init conflict_checker", K(ret));
    } else if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
      LOG_WARN("fail to calc tablet location", K(ret));
    } else {
      conflict_checker_.set_local_tablet_loc(tablet_loc);
      // init update das_ref
      ObMemAttr mem_attr;
      mem_attr.tenant_id_ = session.get_effective_tenant_id();
      mem_attr.label_ = "TableApiInsUpd";
      upd_rtctx_.das_ref_.set_expr_frame_info(insert_up_spec_.get_expr_frame_info());
      upd_rtctx_.das_ref_.set_mem_attr(mem_attr);
      upd_rtctx_.das_ref_.set_execute_directly(true);
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (tb_ctx_.is_htable() && OB_FAIL(modify_htable_timestamp())) {
    LOG_WARN("fail to modify htable timestamp", K(ret));
  }

  return ret;
}

void ObTableApiInsertUpExecutor::set_need_fetch_conflict()
{
  insert_up_rtdef_.ins_rtdef_.das_rtdef_.need_fetch_conflict_ = true;
  dml_rtctx_.set_non_sub_full_task();
  upd_rtctx_.set_pick_del_task_first();
  upd_rtctx_.set_non_sub_full_task();
}

int ObTableApiInsertUpExecutor::refresh_exprs_frame(const ObTableEntity *entity)
{
  int ret = OB_SUCCESS;
  const ObTableInsCtDef &ins_ctdef = insert_up_spec_.get_ctdef().ins_ctdef_;
  const ObTableUpdCtDef &upd_ctdef = insert_up_spec_.get_ctdef().upd_ctdef_;

  clear_evaluated_flag();
  if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is null", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::refresh_insert_up_exprs_frame(tb_ctx_,
                                                                         ins_ctdef.new_row_,
                                                                         upd_ctdef.delta_exprs_,
                                                                         *entity))) {
    LOG_WARN("fail to refresh insert up exprs frame", K(ret), K(*entity));
  }

  return ret;
}

int ObTableApiInsertUpExecutor::get_next_row_from_child()
{
  int ret = OB_SUCCESS;
  const ObTableEntity *entity = static_cast<const ObTableEntity*>(tb_ctx_.get_entity());

  if (cur_idx_ >= 1) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(refresh_exprs_frame(entity))) {
    LOG_WARN("fail to refresh exprs frame", K(ret));
  }

  return ret;
}

int ObTableApiInsertUpExecutor::try_insert_row(bool &is_iter_end, int64_t &insert_rows)
{
  int ret = OB_SUCCESS;
  is_iter_end = false;
  const ObTableInsUpdCtDef &insert_up_ctdef = insert_up_spec_.get_ctdef();

  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_row_from_child())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to load next row from child", K(ret));
      }
    } else if (OB_FAIL(insert_row_to_das(insert_up_ctdef.ins_ctdef_, insert_up_rtdef_.ins_rtdef_))) {
      LOG_WARN("fail to insert row to das", K(ret));
    } else {
      cur_idx_++;
      insert_rows++;
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    is_iter_end = true;
    if (OB_FAIL(execute_das_task(dml_rtctx_, false))) {
      LOG_WARN("fail to execute all das task", K(ret));
    }
  }

  return ret;
}

int ObTableApiInsertUpExecutor::try_update_row()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(conflict_checker_.do_lookup_and_build_base_map(1))) {
    LOG_WARN("fail to build conflict map", K(ret));
  } else if (OB_FAIL(do_insert_up_cache())) {
    LOG_WARN("fail to do insert_up in cache", K(ret));
  } else if (OB_FAIL(prepare_final_insert_up_task())) {
    LOG_WARN("fail to prepare final das task", K(ret));
  } else if (OB_FAIL(execute_das_task(upd_rtctx_, true))) {
    LOG_WARN("fail to execute upd_rtctx_ das task", K(ret));
  } else if (OB_FAIL(execute_das_task(dml_rtctx_, false))) {
    LOG_WARN("fail to execute dml_rtctx_ das task", K(ret));
  }

  return ret;
}

int ObTableApiInsertUpExecutor::execute_das_task(ObDMLRtCtx &dml_rtctx,
                                                 bool del_task_ahead)
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

int ObTableApiInsertUpExecutor::fetch_conflict_rowkey()
{
  int ret = OB_SUCCESS;
  DASTaskIter task_iter = dml_rtctx_.das_ref_.begin_task_iter();

  while (OB_SUCC(ret) && !task_iter.is_end()) {
    if (OB_FAIL(get_next_conflict_rowkey(task_iter, conflict_checker_))) {
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

int ObTableApiInsertUpExecutor::reset_das_env()
{
  int ret = OB_SUCCESS;
  // 释放第一次try insert的das task
  if (OB_FAIL(dml_rtctx_.das_ref_.close_all_task())) {
    LOG_WARN("fail to close all das task", K(ret));
  } else {
    dml_rtctx_.das_ref_.reuse();
  }

  // 因为第二次插入不需要fetch conflict result了
  insert_up_rtdef_.ins_rtdef_.das_rtdef_.need_fetch_conflict_ = false;
  insert_up_rtdef_.ins_rtdef_.das_rtdef_.is_duplicated_ = false;

  return ret;
}

int ObTableApiInsertUpExecutor::check_whether_row_change(const ObChunkDatumStore::StoredRow &upd_old_row,
                                                         const ObChunkDatumStore::StoredRow &upd_new_row)
{
  int ret = OB_SUCCESS;

  if (tb_ctx_.is_inc_or_append()) {
    is_row_changed_ = true;
  } else if (lib::is_mysql_mode()) {
    const ObTableUpdCtDef &upd_ctdef = insert_up_spec_.get_ctdef().upd_ctdef_;
    const ObExprPtrIArray &old_row = upd_ctdef.old_row_;
    const ObExprPtrIArray &new_row = upd_ctdef.new_row_;
    FOREACH_CNT_X(info, upd_ctdef.assign_columns_, OB_SUCC(ret) && !is_row_changed_) {
      const uint64_t idx = info->projector_index_;
      if (idx >= upd_old_row.cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid assign idx", K(ret), K(idx), K(upd_old_row.cnt_));
      } else {
        is_row_changed_ = !ObDatum::binary_equal(upd_old_row.cells()[idx], upd_new_row.cells()[idx]);
      }
    }
  } else {
    //in oracle mode, no matter whether the updated row is changed or not,
    //the row will be updated in the storage
    is_row_changed_ = true;
  }

  return ret;
}

// 通过主键在conflict_checker_中找到冲突旧行，执行更新
// 注意，这里更新后还可能出现二级索引冲突，eg:
// create table t (C1 int, C2 varchar(10), primary key(C1), UNIQUE KEY idx_c2 (C2));
// insert into t values (1, 1);
// insert into t values (2, 2);
// client.insert_up('C1','C2').VALUES(1,2);
// 1. 首先执行insert，冲突，返回冲突行(1,1)
// 2. 然后执行到这个函数，执行update，
int ObTableApiInsertUpExecutor::do_insert_up_cache()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObConflictValue, 1> constraint_values;
  bool is_skipped = false;
  ObChunkDatumStore::StoredRow *insert_row = NULL;
  ObTableUpdRtDef &upd_rtdef = insert_up_rtdef_.upd_rtdef_;
  const ObTableEntity *entity = static_cast<const ObTableEntity*>(tb_ctx_.get_entity());
  const ObExprPtrIArray &new_row_exprs = get_primary_table_insert_row();

  // new_row_exprs因为冲突已经被conflict_checker_刷为冲突行，因此需要重新刷一遍
  if (OB_FAIL(refresh_exprs_frame(entity))) {
    LOG_WARN("fail to refresh exprs frame", K(ret));
  } else if (OB_FAIL(ObChunkDatumStore::StoredRow::build(insert_row, new_row_exprs, eval_ctx_, allocator_))) {
    LOG_WARN("fail to build stored row", K(ret), K(new_row_exprs));
  } else {
    if (OB_ISNULL(insert_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert row is NULL", K(ret));
    } else if (OB_FAIL(conflict_checker_.check_duplicate_rowkey(insert_row,
                                                                constraint_values,
                                                                true))) {
      LOG_WARN("fail to check duplicated key", K(ret), KPC(insert_row));
    } else {
      upd_rtdef.found_rows_++;
      const ObChunkDatumStore::StoredRow *upd_new_row = insert_row;
      const ObChunkDatumStore::StoredRow *upd_old_row = constraint_values.at(0).current_datum_row_;
      if (OB_ISNULL(upd_old_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("upd_old_row is NULL", K(ret));
      } else if (OB_FAIL(check_whether_row_change(*upd_old_row, *upd_new_row))) {
        LOG_WARN("fail to check whether row change", K(ret));
      } else if (is_row_changed_) {
        // do update
        clear_evaluated_flag();
        if (OB_FAIL(conflict_checker_.update_row(upd_new_row, upd_old_row))) {
          LOG_WARN("fail to update row in conflict_checker", K(ret), KPC(upd_new_row), KPC(upd_old_row));
        } else {
          upd_changed_rows_++;
        }
      }
    }
  }

  return ret;
}

int ObTableApiInsertUpExecutor::prepare_final_insert_up_task()
{
  int ret = OB_SUCCESS;
  ObConflictRowMap *primary_map = NULL;
  OZ(conflict_checker_.get_primary_table_map(primary_map));
  CK(OB_NOT_NULL(primary_map));
  ObConflictRowMap::iterator start_row_iter = primary_map->begin();
  ObConflictRowMap::iterator end_row_iter = primary_map->end();
  for (; OB_SUCC(ret) && start_row_iter != end_row_iter; ++start_row_iter) {
    const ObRowkey &constraint_rowkey = start_row_iter->first;
    const ObConflictValue &constraint_value = start_row_iter->second;
    if (!is_row_changed_) {
      // do nothing
    } else if (constraint_value.new_row_source_ == ObNewRowSource::FROM_UPDATE) {
      OZ(do_update(constraint_rowkey, constraint_value));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected row source", K(ret), K(constraint_value.new_row_source_));
    }
  }

  return ret;
}

int ObTableApiInsertUpExecutor::delete_upd_old_row_to_das(const ObRowkey &constraint_rowkey,
                                                          const sql::ObConflictValue &constraint_value)
{
  int ret = OB_SUCCESS;
  ObDASTabletLoc *tablet_loc = nullptr;

  if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
    LOG_WARN("fail to calc tablet location", K(ret));
  } else {
    const ObTableInsUpdCtDef &insert_up_ctdef = insert_up_spec_.get_ctdef();
    const ObTableUpdCtDef &upd_ctdef = insert_up_ctdef.upd_ctdef_;
    ObTableUpdRtDef &upd_rtdef = insert_up_rtdef_.upd_rtdef_;
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
                                                upd_rtctx_,
                                                upd_ctdef.old_row_,
                                                stored_row))) {
      LOG_WARN("fail to delete row with das", K(ret));
    }
  }

  return ret;
}

int ObTableApiInsertUpExecutor::generate_del_rtdef_for_update(const ObTableUpdCtDef &upd_ctdef,
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

int ObTableApiInsertUpExecutor::generate_ins_rtdef_for_update(const ObTableUpdCtDef &upd_ctdef,
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

int ObTableApiInsertUpExecutor::insert_upd_new_row_to_das()
{
  int ret = OB_SUCCESS;
  const ObTableInsUpdCtDef &insert_up_ctdef = insert_up_spec_.get_ctdef();
  ObDASTabletLoc *tablet_loc = nullptr;

  if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
    LOG_WARN("fail to calc tablet location", K(ret));
  } else {
    const ObTableUpdCtDef &upd_ctdef = insert_up_ctdef.upd_ctdef_;
    ObTableUpdRtDef &upd_rtdef = insert_up_rtdef_.upd_rtdef_;
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
    } else if (OB_FAIL(ObDMLService::insert_row(*upd_ctdef.dins_ctdef_,
                                                *upd_rtdef.dins_rtdef_,
                                                tablet_loc,
                                                upd_rtctx_,
                                                upd_ctdef.new_row_,
                                                stored_row))) {
      LOG_WARN("fail to insert row with das", K(ret));
    }
  }

  return ret;
}

int ObTableApiInsertUpExecutor::to_expr_skip_old(const ObChunkDatumStore::StoredRow &store_row,
                                                 const ObRowkey &constraint_rowkey)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = tb_ctx_.get_table_schema();
  const ObTableInsUpdCtDef &insert_up_ctdef = insert_up_spec_.get_ctdef();
  const ObTableUpdCtDef &upd_ctdef = insert_up_ctdef.upd_ctdef_;
  const ObIArray<ObExpr *> &new_row = upd_ctdef.new_row_;
  if (OB_UNLIKELY(store_row.cnt_ != new_row.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum count mismatch", K(ret), K(store_row.cnt_), K(new_row.count()));
  } else {
    // 1. refresh rowkey expr datum
    const int64_t rowkey_col_cnt = tb_ctx_.get_table_schema()->get_rowkey_column_num();
    for (uint64_t i = 0; OB_SUCC(ret) && i < rowkey_col_cnt; ++i) {
      const ObExpr *expr = new_row.at(i);
      expr->locate_expr_datum(eval_ctx_) = store_row.cells()[i];
      expr->get_eval_info(eval_ctx_).evaluated_ = true;
      expr->get_eval_info(eval_ctx_).projected_ = true;
    }

    // 2. refresh assign column expr datum
    const ObTableCtx::ObAssignIds &assign_ids = tb_ctx_.get_assign_ids();
    const int64_t N = assign_ids.count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      uint64_t assign_id = assign_ids.at(i).idx_;
      const ObColumnSchemaV2 *col_schema = nullptr;
      if (OB_ISNULL(col_schema = table_schema->get_column_schema_by_idx(assign_id))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(assign_id), K(*table_schema));
      } else if (assign_id >= store_row.cnt_) {
        ret = OB_ERROR_OUT_OF_RANGE;
        LOG_WARN("assign idx out of range", K(ret), K(assign_id), K(store_row.cnt_));
      } else if (assign_id >= new_row.count()) {
        ret = OB_ERROR_OUT_OF_RANGE;
        LOG_WARN("assign idx out of range", K(ret), K(assign_id), K(new_row.count()));
      } else if (col_schema->is_virtual_generated_column()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should not have virtual generated expr", K(ret));
      } else if (col_schema->is_stored_generated_column()) {
        ObTableCtx &ctx = const_cast<ObTableCtx &>(tb_ctx_);
        if (OB_FAIL(ObTableExprCgService::refresh_generated_column_related_frame(ctx,
                                                                                 upd_ctdef.old_row_,
                                                                                 upd_ctdef.full_assign_row_,
                                                                                 assign_ids,
                                                                                 *col_schema))) {
          LOG_WARN("fail to refresh generated column related frame", K(ret), K(ctx), K(*col_schema));
        }
      } else {
        const ObExpr *expr = new_row.at(assign_id);
        expr->locate_expr_datum(eval_ctx_) = store_row.cells()[assign_id];
        expr->get_eval_info(eval_ctx_).evaluated_ = true;
        expr->get_eval_info(eval_ctx_).projected_ = true;
      }
    }
  }

  return ret;
}

int ObTableApiInsertUpExecutor::do_update(const ObRowkey &constraint_rowkey,
                                          const ObConflictValue &constraint_value)
{
  int ret = OB_SUCCESS;

  if (constraint_value.new_row_source_ == ObNewRowSource::FROM_UPDATE) {
    // current_datum_row_ 是update的new_row
    if (NULL != constraint_value.baseline_datum_row_ &&
        NULL != constraint_value.current_datum_row_) {
      // base_line 和 curr_row 都存在
      OZ(constraint_value.baseline_datum_row_->to_expr(get_primary_table_upd_old_row(),
                                                       eval_ctx_));
      OZ(delete_upd_old_row_to_das(constraint_rowkey, constraint_value));
      OZ(to_expr_skip_old(*constraint_value.current_datum_row_, constraint_rowkey));
      clear_evaluated_flag();
      OZ(insert_upd_new_row_to_das());
    } else if (NULL == constraint_value.baseline_datum_row_ &&
               NULL != constraint_value.current_datum_row_) {
      OZ(to_expr_skip_old(*constraint_value.current_datum_row_, constraint_rowkey));
      OZ(insert_upd_new_row_to_das());
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected constraint_value", K(ret));
    }
  }

  return ret;
}

int ObTableApiInsertUpExecutor::get_next_row()
{
  int ret = OB_SUCCESS;
  bool is_iter_end = false;

  while (OB_SUCC(ret) && !is_iter_end) {
    int64_t insert_rows = 0;
    int64_t savepoint_no = 0;
    // must set conflict_row fetch flag
    set_need_fetch_conflict();
    if (OB_FAIL(ObSqlTransControl::create_anonymous_savepoint(exec_ctx_, savepoint_no))) {
      LOG_WARN("fail to create save_point", K(ret));
    } else if (OB_FAIL(try_insert_row(is_iter_end, insert_rows))) {
      LOG_WARN("fail to try insert row", K(ret));
    } else if (!is_duplicated()) {
      insert_rows_ += insert_rows;
      LOG_TRACE("try insert is not duplicated", K(ret), K(insert_rows_));
    } else if (OB_FAIL(fetch_conflict_rowkey())) {
      LOG_WARN("fail to fetch conflict row", K(ret));
    } else if (OB_FAIL(reset_das_env())) {
      // 这里需要reuse das 相关信息
      LOG_WARN("fail to reset das env", K(ret));
    } else if (OB_FAIL(ObSqlTransControl::rollback_savepoint(exec_ctx_, savepoint_no))) {
      // 本次插入存在冲突, 回滚到save_point
      LOG_WARN("fail to rollback to save_point", K(ret));
    } else if (OB_FAIL(try_update_row())) {
      LOG_WARN("fail to try update row", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    affected_rows_ += insert_rows_ + insert_up_rtdef_.upd_rtdef_.found_rows_;
    // auto inc 操作中, 同步全局自增值value
    if (tb_ctx_.has_auto_inc() && OB_FAIL(tb_ctx_.update_auto_inc_value())) {
      LOG_WARN("fail to update auto inc value", K(ret));
    }
  }
  return ret;
}

int ObTableApiInsertUpExecutor::close()
{
  int ret = OB_SUCCESS;
  int close_ret = OB_SUCCESS;

  if (is_opened_) {
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
    close_ret = ObTableApiModifyExecutor::close();
  }

  return (OB_SUCCESS == ret) ? close_ret : ret;
}

}  // namespace table
}  // namespace oceanbase
