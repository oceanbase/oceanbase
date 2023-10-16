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
  bool use_put = false;

  if (OB_FAIL(generate_ins_rtdef(ctdef.ins_ctdef_, rtdef.ins_rtdef_))) {
    LOG_WARN("fail to generate insert rtdef", K(ret));
  } else if (OB_FAIL(generate_upd_rtdef(ctdef.upd_ctdef_,
                                        rtdef.upd_rtdef_))) {
    LOG_WARN("fail to generate update rtdef", K(ret));
  } else if (OB_FAIL(tb_ctx_.check_insert_up_can_use_put(use_put))) {
    LOG_WARN("fail to check insert up use put", K(ret));
  } else {
    rtdef.ins_rtdef_.das_rtdef_.table_loc_->is_writing_ = true;
    rtdef.ins_rtdef_.das_rtdef_.use_put_ = use_put;
  }

  if (!use_put) {
    set_need_fetch_conflict(upd_rtctx_, rtdef.ins_rtdef_);
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

int ObTableApiInsertUpExecutor::refresh_exprs_frame(const ObTableEntity *entity)
{
  int ret = OB_SUCCESS;
  const ObTableInsCtDef &ins_ctdef = insert_up_spec_.get_ctdef().ins_ctdef_;
  const ObTableUpdCtDef &upd_ctdef = insert_up_spec_.get_ctdef().upd_ctdef_;

  if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is null", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::refresh_insert_up_exprs_frame(tb_ctx_,
                                                                         ins_ctdef.new_row_,
                                                                         upd_ctdef.delta_row_,
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

int ObTableApiInsertUpExecutor::try_insert_row()
{
  int ret = OB_SUCCESS;
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
  ObTableUpdRtDef &upd_rtdef = insert_up_rtdef_.upd_rtdef_;

  if (OB_ISNULL(insert_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert row is NULL", K(ret));
  } else if (OB_FAIL(conflict_checker_.check_duplicate_rowkey(insert_row_, constraint_values, true))) {
    LOG_WARN("fail to check duplicated key", K(ret), KPC_(insert_row));
  } else {
    upd_rtdef.found_rows_++;
    const ObChunkDatumStore::StoredRow *upd_new_row = insert_row_;
    const ObChunkDatumStore::StoredRow *upd_old_row = constraint_values.at(0).current_datum_row_;
    if (OB_ISNULL(upd_old_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upd_old_row is NULL", K(ret));
    } else if (OB_FAIL(check_whether_row_change(*upd_old_row,
                                                *upd_new_row,
                                                insert_up_spec_.get_ctdef().upd_ctdef_,
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
      OZ(do_update(constraint_rowkey, constraint_value));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected row source", K(ret), K(constraint_value.new_row_source_));
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
      OZ(stored_row_to_exprs(*constraint_value.baseline_datum_row_,
                             get_primary_table_upd_old_row(),
                             eval_ctx_));
      OZ(delete_upd_old_row_to_das(constraint_rowkey,
                                   constraint_value,
                                   insert_up_spec_.get_ctdef().upd_ctdef_,
                                   insert_up_rtdef_.upd_rtdef_,
                                   upd_rtctx_));
      OZ(to_expr_skip_old(*constraint_value.current_datum_row_,
                          insert_up_spec_.get_ctdef().upd_ctdef_));
      clear_evaluated_flag();
      OZ(insert_upd_new_row_to_das(insert_up_spec_.get_ctdef().upd_ctdef_,
                                   insert_up_rtdef_.upd_rtdef_,
                                   upd_rtctx_));
    } else if (NULL == constraint_value.baseline_datum_row_ &&
               NULL != constraint_value.current_datum_row_) { // 单单是唯一索引冲突的时候，会走这个分支
      OZ(to_expr_skip_old(*constraint_value.current_datum_row_,
                          insert_up_spec_.get_ctdef().upd_ctdef_));
      OZ(insert_upd_new_row_to_das(insert_up_spec_.get_ctdef().upd_ctdef_,
                                   insert_up_rtdef_.upd_rtdef_,
                                   upd_rtctx_));
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
  } else if (OB_FAIL(reset_das_env(insert_up_rtdef_.ins_rtdef_))) {
    // 这里需要reuse das 相关信息
    LOG_WARN("fail to reset das env", K(ret));
  } else if (OB_FAIL(ObSqlTransControl::rollback_savepoint(exec_ctx_, savepoint_no))) {
    // 本次插入存在冲突, 回滚到save_point
    LOG_WARN("fail to rollback to save_point", K(ret));
  } else if (OB_FAIL(try_update_row())) {
    LOG_WARN("fail to try update row", K(ret));
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
