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
using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{

int ObTableApiInsertUpSpec::init_ctdefs_array(int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(insert_up_ctdefs_.allocate_array(alloc_, size))) {
    LOG_WARN("fail to alloc ctdefs array", K(ret), K(size));
  } else {
    // init each element as nullptr
    for (int64_t i = 0; i < size; i++) {
      insert_up_ctdefs_.at(i) = nullptr;
    }
  }
  return ret;
}

ObTableApiInsertUpSpec::~ObTableApiInsertUpSpec()
{
  for (int64_t i = 0; i < insert_up_ctdefs_.count(); i++) {
    if (OB_NOT_NULL(insert_up_ctdefs_.at(i))) {
      insert_up_ctdefs_.at(i)->~ObTableInsUpdCtDef();
    }
  }
  insert_up_ctdefs_.reset();
}

bool ObTableApiInsertUpExecutor::is_duplicated()
{
  for (int64_t i = 0 ; i < insert_up_rtdefs_.count() && !is_duplicated_; i++) {
    is_duplicated_ = insert_up_rtdefs_.at(i).ins_rtdef_.das_rtdef_.is_duplicated_;
  }
  return is_duplicated_;
}

int ObTableApiInsertUpExecutor::generate_insert_up_rtdefs()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(insert_up_rtdefs_.allocate_array(allocator_, insert_up_spec_.get_ctdefs().count()))) {
    LOG_WARN("allocate insert up rtdef failed", K(ret), K(insert_up_spec_.get_ctdefs().count()));
  }
  for (int64_t i = 0; i < insert_up_rtdefs_.count() && OB_SUCC(ret); i++) {
    ObTableInsUpdRtDef &insup_rtdef = insert_up_rtdefs_.at(i);
    ObTableInsUpdCtDef *insup_ctdef = nullptr;
    if (OB_ISNULL(insup_ctdef = insert_up_spec_.get_ctdefs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insup ctdef is NULL", K(ret), K(i));
    } else if (OB_FAIL(generate_ins_rtdef(insup_ctdef->ins_ctdef_, insup_rtdef.ins_rtdef_))) {
      LOG_WARN("fail to generate insert rtdef", K(ret));
    } else if (OB_FAIL(generate_upd_rtdef(insup_ctdef->upd_ctdef_,
                                          insup_rtdef.upd_rtdef_))) {
      LOG_WARN("fail to generate update rtdef", K(ret));
    } else if (OB_FAIL(tb_ctx_.check_insert_up_can_use_put(use_put_))) {
    LOG_WARN("fail to check insert up use put", K(ret));
    } else {
      insup_rtdef.ins_rtdef_.das_rtdef_.table_loc_->is_writing_ = true;
      insup_rtdef.ins_rtdef_.das_rtdef_.use_put_ = use_put_;
    }
  }

  if (!use_put_) {
    set_need_fetch_conflict();
  }

  return ret;
}

int ObTableApiInsertUpExecutor::open()
{
  int ret = OB_SUCCESS;
  ObDASTableLoc *table_loc = nullptr;
  ObDASTabletLoc *tablet_loc = nullptr;
  if (OB_FAIL(ObTableApiModifyExecutor::open())) {
    LOG_WARN("fail to oepn ObTableApiModifyExecutor", K(ret));
  } else if (OB_UNLIKELY(insert_up_spec_.get_ctdefs().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ins ctdef is invalid", K(ret));
  } else if (OB_FAIL(generate_insert_up_rtdefs())) {
    LOG_WARN("fail to init insert up rtdef", K(ret));
  } else if (OB_ISNULL(table_loc = insert_up_rtdefs_.at(0).ins_rtdef_.das_rtdef_.table_loc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table location is invalid", K(ret));
  } else if (OB_FAIL(conflict_checker_.init_conflict_checker(insert_up_spec_.get_expr_frame_info(),
                                                             table_loc,
                                                             false))) {
    LOG_WARN("fail to init conflict_checker", K(ret));
  } else if (OB_FAIL(calc_local_tablet_loc(tablet_loc))) {
    LOG_WARN("fail to calc tablet loc", K(ret));
  } else {
    conflict_checker_.set_local_tablet_loc(tablet_loc);
    // init update das_ref
    const ObSQLSessionInfo &session = tb_ctx_.get_session_info();
    ObMemAttr mem_attr;
    bool use_dist_das = tb_ctx_.need_dist_das();
    mem_attr.tenant_id_ = session.get_effective_tenant_id();
    mem_attr.label_ = "TableApiInsUpd";
    upd_rtctx_.das_ref_.set_expr_frame_info(insert_up_spec_.get_expr_frame_info());
    upd_rtctx_.das_ref_.set_mem_attr(mem_attr);
    upd_rtctx_.das_ref_.set_execute_directly(!use_dist_das);
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
  for (int64_t i = 0; i < insert_up_rtdefs_.count(); i++) {
    ObTableInsRtDef &ins_rtdef = insert_up_rtdefs_.at(i).ins_rtdef_;
    ins_rtdef.das_rtdef_.need_fetch_conflict_ = true;
  }
  dml_rtctx_.set_non_sub_full_task();
  upd_rtctx_.set_pick_del_task_first();
  upd_rtctx_.set_non_sub_full_task();
}

int ObTableApiInsertUpExecutor::refresh_exprs_frame(const ObTableEntity *entity)
{
  int ret = OB_SUCCESS;
  const ObTableInsCtDef &ins_ctdef = insert_up_spec_.get_ctdefs().at(0)->ins_ctdef_;

  if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is null", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::refresh_insert_up_exprs_frame(tb_ctx_,
                                                                         ins_ctdef.new_row_,
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
  } else {
    clear_evaluated_flag();
    if (OB_FAIL(refresh_exprs_frame(entity))) {
      LOG_WARN("fail to refresh exprs frame", K(ret));
    }
  }

  return ret;
}

int ObTableApiInsertUpExecutor::insert_row_to_das()
{
  int ret = OB_SUCCESS;
  int64_t insup_ctdef_count = insert_up_spec_.get_ctdefs().count();
  if (OB_UNLIKELY(insup_ctdef_count != insert_up_rtdefs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of insup ctdef is not equal to rtdef", K(ret), K(insup_ctdef_count), K(insert_up_rtdefs_.count()));
  }
  for (int64_t i = 0; i < insert_up_rtdefs_.count() && OB_SUCC(ret); i++) {
    ObTableInsRtDef &ins_rtdef = insert_up_rtdefs_.at(i).ins_rtdef_;
    const ObTableInsUpdCtDef *insup_ctdef = nullptr;
    if (OB_ISNULL(insup_ctdef = insert_up_spec_.get_ctdefs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insup ctdef is NULL", K(ret), K(i));
    } else if (OB_FAIL(ObTableApiModifyExecutor::insert_row_to_das(insup_ctdef->ins_ctdef_, ins_rtdef))) {
      LOG_WARN("fail to insert row to das", K(ret), K(i), K(insup_ctdef->ins_ctdef_), K(ins_rtdef));
    }
  }
  return ret;
}

int ObTableApiInsertUpExecutor::try_insert_row()
{
  int ret = OB_SUCCESS;
  const ObTableInsUpdCtDef &insert_up_ctdef = *insert_up_spec_.get_ctdefs().at(0);

  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_row_from_child())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to load next row from child", K(ret));
      }
    } else if (OB_FAIL(insert_row_to_das())) {
      LOG_WARN("fail to insert row to das", K(ret));
    } else {
      cur_idx_++;
    }
  }

  if (OB_ITER_END == ret && OB_FAIL(execute_das_task(dml_rtctx_, false/* del_task_ahead */))) {
    LOG_WARN("fail to execute das task");
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
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_up_rtdefs_.count(); i++) {
    ObTableInsRtDef &ins_rtdef = insert_up_rtdefs_.at(i).ins_rtdef_;
    ins_rtdef.das_rtdef_.need_fetch_conflict_ = false;
    ins_rtdef.das_rtdef_.is_duplicated_ = false;
  }

  return ret;
}


int ObTableApiInsertUpExecutor::cache_insert_row()
{
  int ret = OB_SUCCESS;
  const ObExprPtrIArray &new_row_exprs = get_primary_table_new_row();

  if (OB_FAIL(ObChunkDatumStore::StoredRow::build(insert_row_, new_row_exprs, eval_ctx_, allocator_))) {
    LOG_WARN("fail to build stored row", K(ret), K(new_row_exprs));
  } else if (OB_ISNULL(insert_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache insert row is null", K(ret));
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
  ObTableUpdRtDef &upd_rtdef = insert_up_rtdefs_.at(0).upd_rtdef_;

  if (OB_ISNULL(insert_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert row is NULL", K(ret));
  } else if (OB_FAIL(conflict_checker_.check_duplicate_rowkey(insert_row_, constraint_values, true))) {
    LOG_WARN("fail to check duplicated key", K(ret), KPC_(insert_row));
  } else if (constraint_values.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("constraint_values is empty", K(ret), KPC_(insert_row), K(conflict_checker_.conflict_map_array_.count()));
  } else {
    upd_rtdef.found_rows_++;
    const ObChunkDatumStore::StoredRow *upd_new_row = insert_row_;
    const ObChunkDatumStore::StoredRow *upd_old_row = constraint_values.at(0).current_datum_row_;
    old_row_ = upd_old_row;
    if (OB_ISNULL(upd_old_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upd_old_row is NULL", K(ret));
    } else if (OB_FAIL(check_rowkey_change(*upd_old_row,
                                           *upd_new_row))) {
      LOG_WARN("can not update rowkey column", K(ret));
    } else if (OB_FAIL(check_whether_row_change(*upd_old_row,
                                                *upd_new_row,
                                                insert_up_spec_.get_ctdefs().at(0)->upd_ctdef_,
                                                is_row_changed_))) {
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
      OZ(do_update(constraint_value));
    } else if (constraint_value.new_row_source_ == ObNewRowSource::FROM_INSERT) {
      /*
      scene of do_insert():
      case like:
        create table t (c1 int primary key, c2 int, c3 int, unique key idx(c2) local);
        insert into t values (1,1,1);
        insert into t values (2,1,3); // unique key conflict
      info:
        conflict_map_array count: 2
        conflict_map_array[0]:
          {key: 1, value: {base: old, current: NULL, new_row_source: INSERT}}
          {key: 2, value: {base: NULL, current: new, new_row_source: UPDATE}}
        conflict_map_array[1]:
          {key: 1, value: {base: old, current: NULL, new_row_source: UPDATE}}

      NOTE:
        the struct of conflict_map_array after do_insertup_cache() has been shown above.
        do_insert() will delete the old_row and do_update() will insert the new row.
      */
      OZ(do_insert(constraint_value));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected row source", K(ret), K(constraint_value.new_row_source_));
    }
  }

  return ret;
}

int ObTableApiInsertUpExecutor::delete_upd_old_row_to_das()
{
  int ret = OB_SUCCESS;
  int64_t insup_ctdefs_count = insert_up_spec_.get_ctdefs().count();
  if (OB_UNLIKELY(insup_ctdefs_count != insert_up_rtdefs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of insup ctdefs is not equal to rtdefs", K(ret), K(insup_ctdefs_count), K(insert_up_rtdefs_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_up_rtdefs_.count(); i++) {
    ObTableUpdRtDef &upd_rtdef = insert_up_rtdefs_.at(i).upd_rtdef_;
    const ObTableInsUpdCtDef *insup_ctdef = nullptr;
    if (OB_ISNULL(insup_ctdef = insert_up_spec_.get_ctdefs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insup ctdef is NULL", K(ret), K(i));
    } else if (OB_FAIL(ObTableApiModifyExecutor::delete_upd_old_row_to_das(insup_ctdef->upd_ctdef_,
            upd_rtdef, upd_rtctx_))) {
      LOG_WARN("fail to delete old row to das", K(ret), K(i));
    }
  }

  return ret;
}

int ObTableApiInsertUpExecutor::insert_upd_new_row_to_das()
{
  int ret = OB_SUCCESS;
  int64_t insup_ctdef_count = insert_up_spec_.get_ctdefs().count();
  if (OB_UNLIKELY(insup_ctdef_count != insert_up_rtdefs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of insup ctdef is not equal to rtdef", K(ret), K(insup_ctdef_count),
              K(insert_up_rtdefs_.count()));
  }
  for (int64_t i = 0; i < insert_up_rtdefs_.count(); i++) {
    ObTableUpdRtDef &upd_rtdef = insert_up_rtdefs_.at(i).upd_rtdef_;
    ObTableInsUpdCtDef *insup_ctdef = nullptr;
    if (OB_ISNULL(insup_ctdef = insert_up_spec_.get_ctdefs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insup ctdef is NULL", K(ret), K(i));
    } else if (OB_FAIL(ObTableApiModifyExecutor::insert_upd_new_row_to_das(insup_ctdef->upd_ctdef_,
              upd_rtdef, upd_rtctx_))) {
      LOG_WARN("fail to insert row to das", K(ret), K(i));
    }
  }
  return ret;
}

int ObTableApiInsertUpExecutor::do_update(const ObConflictValue &constraint_value)
{
  int ret = OB_SUCCESS;

  if (constraint_value.new_row_source_ == ObNewRowSource::FROM_UPDATE) {
    // current_datum_row_ 是update的new_row
    if (NULL != constraint_value.baseline_datum_row_ &&
        NULL != constraint_value.current_datum_row_) {
      // base_line 和 curr_row 都存在
      if (OB_FAIL(stored_row_to_exprs(*constraint_value.baseline_datum_row_,
                                      get_primary_table_upd_old_row(),
                                      eval_ctx_))) {
        LOG_WARN("fail to load row to old row exprs", K(ret), KPC(constraint_value.baseline_datum_row_));
      } else if (OB_FAIL(to_expr_skip_old(*constraint_value.current_datum_row_,
                                          insert_up_spec_.get_ctdefs().at(0)->upd_ctdef_))) {
        LOG_WARN("fail to load row to new row exprs", K(ret), KPC(constraint_value.current_datum_row_));
      } else {
        for (int i = 0; i < insert_up_rtdefs_.count() && OB_SUCC(ret); i++) {
          const ObTableUpdCtDef &upd_ctdef = insert_up_spec_.get_ctdefs().at(i)->upd_ctdef_;
          ObTableUpdRtDef &upd_rtdef = insert_up_rtdefs_.at(i).upd_rtdef_;
          if (OB_FAIL(ObTableApiModifyExecutor::update_row_to_das(upd_ctdef, upd_rtdef, dml_rtctx_))) {
            LOG_WARN("fail to update row", K(ret));
          }
        }
      }
    } else if (NULL == constraint_value.baseline_datum_row_ &&
               NULL != constraint_value.current_datum_row_) { // 单单是唯一索引冲突的时候，会走这个分支
      OZ(to_expr_skip_old(*constraint_value.current_datum_row_,
                          insert_up_spec_.get_ctdefs().at(0)->upd_ctdef_));
      OZ(insert_upd_new_row_to_das());
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected constraint_value", K(ret));
    }
  }

  return ret;
}

int ObTableApiInsertUpExecutor::do_insert(const ObConflictValue &constraint_value)
{
  int ret = OB_SUCCESS;
  bool has_insert = false;
  bool has_delete = false;
  if (NULL != constraint_value.baseline_datum_row_ &&
      NULL != constraint_value.current_datum_row_) {
    // delete + insert
    OZ(stored_row_to_exprs(*constraint_value.baseline_datum_row_,
                            get_primary_table_upd_old_row(),
                            eval_ctx_));
    OZ(delete_upd_old_row_to_das());
    OZ(to_expr_skip_old(*constraint_value.current_datum_row_,
                        insert_up_spec_.get_ctdefs().at(0)->upd_ctdef_));
    clear_evaluated_flag();
    OZ(insert_upd_new_row_to_das());
  } else if (NULL != constraint_value.baseline_datum_row_ &&
             NULL == constraint_value.current_datum_row_) {
    // only delete
    OZ(stored_row_to_exprs(*constraint_value.baseline_datum_row_,
                            get_primary_table_upd_old_row(),
                            eval_ctx_));
    OZ(delete_upd_old_row_to_das());
  } else if (NULL == constraint_value.baseline_datum_row_ &&
             NULL != constraint_value.current_datum_row_) {
    // only insert
    OZ(to_expr_skip_old(*constraint_value.current_datum_row_,
                        insert_up_spec_.get_ctdefs().at(0)->upd_ctdef_));
    clear_evaluated_flag();
    OZ(insert_upd_new_row_to_das());
  }
  return ret;
}

int ObTableApiInsertUpExecutor::get_next_row()
{
  int ret = OB_SUCCESS;
  transaction::ObTxSEQ savepoint_no;

  if (OB_FAIL(ObSqlTransControl::create_anonymous_savepoint(exec_ctx_, savepoint_no))) {
    LOG_WARN("fail to create save_point", K(ret));
  } else if (OB_FAIL(try_insert_row())) {
    LOG_WARN("fail to try insert row", K(ret));
  } else if (!is_duplicated()) {
    insert_rows_ = 1;
    LOG_TRACE("try insert is not duplicated", K(ret), K(insert_rows_));
  } else if (OB_FAIL(cache_insert_row())) {
    LOG_WARN("fail to cache insert row", K(ret));
  } else if (OB_FAIL(fetch_conflict_rowkey(conflict_checker_))) {
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

  if (OB_SUCC(ret)) {
    affected_rows_ += insert_rows_ + insert_up_rtdefs_.at(0).upd_rtdef_.found_rows_;
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
    // reset the new row datum ptr
    const ObExprPtrIArray &new_row_exprs = get_primary_table_new_row();
    reset_new_row_datum(new_row_exprs);
  }

  return (OB_SUCCESS == ret) ? close_ret : ret;
}

int ObTableApiInsertUpExecutor::get_old_row(const ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  int64_t column_nums = conflict_checker_.checker_ctdef_.table_column_exprs_.count();
  ObNewRow *tmp_row = nullptr;
  char *row_buf = nullptr;
  ObObj *cells = nullptr;
  if (OB_ISNULL(old_row_) || OB_ISNULL(old_row_->cells()) || old_row_->cnt_ != column_nums) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old_row_ is null", K(ret), KPC(old_row_));
  } else if (OB_ISNULL(row_buf = static_cast<char *>(tb_ctx_.get_allocator().alloc(sizeof(ObNewRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObNewRow buffer", K(ret));
  } else if (OB_ISNULL(cells = static_cast<ObObj *>(tb_ctx_.get_allocator().alloc(sizeof(ObObj) * column_nums)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc cells buffer", K(ret), K(column_nums));
  } else {
    tmp_row = new (row_buf) ObNewRow(cells, column_nums);
    for (int64_t i = 0; i < column_nums; ++i) {
      old_row_->cells()[i].to_obj(cells[i], conflict_checker_.checker_ctdef_.table_column_exprs_[i]->obj_meta_);
    }
  }
  if (OB_SUCC(ret)) {
    row = tmp_row;
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
