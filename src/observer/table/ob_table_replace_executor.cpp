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
#include "ob_table_replace_executor.h"
#include "ob_table_cg_service.h"

namespace oceanbase
{
namespace table
{
int ObTableApiReplaceSpec::init_ctdefs_array(int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_ctdefs_.allocate_array(alloc_, size))) {
    LOG_WARN("fail to alloc ctdefs array", K(ret), K(size));
  } else {
    // init each element as nullptr
    for (int64_t i = 0; i < size; i++) {
      replace_ctdefs_.at(i) = nullptr;
    }
  }
  return ret;
}

ObTableApiReplaceSpec::~ObTableApiReplaceSpec()
{
  for (int64_t i = 0; i < replace_ctdefs_.count(); i++) {
    if (OB_NOT_NULL(replace_ctdefs_.at(i))) {
      replace_ctdefs_.at(i)->~ObTableReplaceCtDef();
    }
  }
  replace_ctdefs_.reset();
}

bool ObTableApiReplaceExecutor::is_duplicated()
{
  bool bret = false;
  for (int64_t i = 0 ; i < replace_rtdefs_.count() && !bret; i++)
  {
    if (replace_rtdefs_.at(i).ins_rtdef_.das_rtdef_.is_duplicated_) {
      bret = true;
    }
  }
  return bret;
}

int ObTableApiReplaceExecutor::generate_replace_rtdefs()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_rtdefs_.allocate_array(allocator_, replace_spec_.get_ctdefs().count()))) {
    LOG_WARN("allocate replace rtdef failed", K(ret), K(replace_spec_.get_ctdefs().count()));
  }
  for (int64_t i = 0; i < replace_rtdefs_.count() && OB_SUCC(ret); i++) {
    ObTableReplaceRtDef &replace_rtdef = replace_rtdefs_.at(i);
    ObTableReplaceCtDef *replace_ctdef = replace_spec_.get_ctdefs().at(i);
    if (OB_ISNULL(replace_ctdef = replace_spec_.get_ctdefs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("repalce ctdef is NULL", K(ret), K(i));
    } else if (OB_FAIL(generate_ins_rtdef(replace_ctdef->ins_ctdef_,
                                   replace_rtdef.ins_rtdef_))) {
      LOG_WARN("fail to generate insert rtdef", K(ret));
    } else if (OB_FAIL(generate_del_rtdef(replace_ctdef->del_ctdef_,
                                          replace_rtdef.del_rtdef_))) {
      LOG_WARN("fail to generate delete rtdef", K(ret));
    } else {
      replace_rtdef.ins_rtdef_.das_rtdef_.table_loc_->is_writing_ = true; //todo:linjing其他的executor还没有设置is_writting
    }
  }

  return ret;
}

int ObTableApiReplaceExecutor::open()
{
  int ret = OB_SUCCESS;
  ObDASTableLoc *table_loc = nullptr;
  ObDASTabletLoc *tablet_loc = nullptr;
  if (OB_FAIL(ObTableApiModifyExecutor::open())) {
    LOG_WARN("fail to oepn ObTableApiModifyExecutor", K(ret));
  } else if (OB_FAIL(generate_replace_rtdefs())) {
    LOG_WARN("fail to init replace rtdef", K(ret));
  } else if (OB_ISNULL(table_loc = replace_rtdefs_.at(0).ins_rtdef_.das_rtdef_.table_loc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table location is invalid", K(ret));
  } else if (OB_FAIL(conflict_checker_.init_conflict_checker(replace_spec_.get_expr_frame_info(),
                                                             table_loc,
                                                             false))) {
    LOG_WARN("fail to init conflict_checker", K(ret));
  } else if (OB_FAIL(calc_local_tablet_loc(tablet_loc))) {
    LOG_WARN("fail to calc tablet location", K(ret));
  } else {
    conflict_checker_.set_local_tablet_loc(tablet_loc);
  }

  return ret;
}

void ObTableApiReplaceExecutor::set_need_fetch_conflict()
{
  for (int64_t i = 0; i < replace_rtdefs_.count(); i++) {
    replace_rtdefs_.at(i).ins_rtdef_.das_rtdef_.need_fetch_conflict_ = true;
  }
  dml_rtctx_.set_pick_del_task_first();
  dml_rtctx_.set_non_sub_full_task();
}

int ObTableApiReplaceExecutor::refresh_exprs_frame(const ObTableEntity *entity)
{
  int ret = OB_SUCCESS;
  const ObTableReplaceCtDef &ctdef = *replace_spec_.get_ctdefs().at(0);

  if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is null", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::refresh_replace_exprs_frame(tb_ctx_,
                                                                       ctdef.ins_ctdef_.new_row_,
                                                                       *entity))) {
    LOG_WARN("fail to refresh replace exprs frame", K(ret), K(*entity));
  }

  return ret;
}

int ObTableApiReplaceExecutor::get_next_row_from_child()
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

int ObTableApiReplaceExecutor::insert_row_to_das()
{
  int ret = OB_SUCCESS;
  int64_t replace_ctdef_count = replace_spec_.get_ctdefs().count();
  if (OB_UNLIKELY(replace_ctdef_count != replace_rtdefs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replace ctdefs size is not equal to rtdefs", K(ret), K(replace_ctdef_count), K(replace_rtdefs_.count()));
  }
  for (int64_t i = 0; i < replace_rtdefs_.count() && OB_SUCC(ret); i++) {
    ObTableInsRtDef &ins_rtdef = replace_rtdefs_.at(i).ins_rtdef_;
    ObTableReplaceCtDef *replace_ctdef = nullptr;
    if (OB_ISNULL(replace_ctdef = replace_spec_.get_ctdefs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replace ctdef is NULL", K(ret), K(i));
    } else if (OB_FAIL(ObTableApiModifyExecutor::insert_row_to_das(replace_ctdef->ins_ctdef_, ins_rtdef))) {
      LOG_WARN("fail to insert row to das", K(ret), K(i), K(replace_ctdef->ins_ctdef_), K(ins_rtdef));
    }
  }
  return ret;
}

int ObTableApiReplaceExecutor::delete_row_to_das()
{
  int ret = OB_SUCCESS;
  int64_t replace_ctdef_count = replace_spec_.get_ctdefs().count();
  if (OB_UNLIKELY(replace_ctdef_count != replace_rtdefs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replace ctdefs size is not equal to rtdefs", K(ret), K(replace_ctdef_count), K(replace_rtdefs_.count()));
  }
  for (int64_t i = 0; i < replace_rtdefs_.count() && OB_SUCC(ret); i++) {
    ObTableDelRtDef &del_rtdef = replace_rtdefs_.at(i).del_rtdef_;
    ObTableReplaceCtDef *replace_ctdef = nullptr;
    ObDASTabletLoc *tablet_loc = nullptr;
    ObChunkDatumStore::StoredRow* stored_row = nullptr;
    if (OB_ISNULL(replace_ctdef = replace_spec_.get_ctdefs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replace ctdef is NULL", K(ret), K(i));
    } else {
      const ObTableDelCtDef &del_ctdef = replace_ctdef->del_ctdef_;
      bool is_primary_table = (i == 0);
      ObExpr *calc_part_id_expr = is_primary_table ? conflict_checker_.checker_ctdef_.calc_part_id_expr_
                                                    : del_ctdef.old_part_id_expr_;
      if (OB_FAIL(ObTableApiModifyExecutor::delete_row_to_das(is_primary_table,
                                                              calc_part_id_expr,
                                                              conflict_checker_.checker_ctdef_.part_id_dep_exprs_,
                                                              del_ctdef,
                                                              del_rtdef))) {
        LOG_WARN("fail to delete row to das", K(ret), K(del_ctdef), K(del_rtdef), K(i));
      }
    }
  }
  return ret;
}

int ObTableApiReplaceExecutor::load_replace_rows(bool &is_iter_end)
{
  int ret = OB_SUCCESS;
  const ObTableReplaceCtDef &ctdef = *replace_spec_.get_ctdefs().at(0);

  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_row_from_child())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to load next row from child", K(ret));
      }
    } else if (OB_FAIL(insert_row_to_das())) {
      LOG_WARN("fail to insert row to das", K(ret));
    } else {
      replace_rtdefs_.at(0).ins_rtdef_.cur_row_num_ = 1;
      cur_idx_++;
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    is_iter_end = true;
  }

  return ret;
}

int ObTableApiReplaceExecutor::post_das_task()
{
  int ret = OB_SUCCESS;

  if (dml_rtctx_.das_ref_.has_task()) {
    if (OB_FAIL(dml_rtctx_.das_ref_.execute_all_task())) {
      LOG_WARN("fail to execute all das task", K(ret));
    }
  }

  return ret;
}

int ObTableApiReplaceExecutor::check_values(bool &is_equal,
                                            const ObChunkDatumStore::StoredRow *replace_row,
                                            const ObChunkDatumStore::StoredRow *delete_row)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  const ObIArray<ObExpr *> &new_row = get_primary_table_new_row();
  const ObIArray<ObExpr *> &old_row = get_primary_table_old_row();
  CK(OB_NOT_NULL(delete_row));
  CK(OB_NOT_NULL(replace_row));
  CK(replace_row->cnt_ == new_row.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < new_row.count(); ++i) {
    const UIntFixedArray &column_ids = replace_spec_.get_ctdefs().at(0)->ins_ctdef_.column_ids_;
    CK(new_row.at(i)->basic_funcs_->null_first_cmp_ == old_row.at(i)->basic_funcs_->null_first_cmp_);
    if (OB_SUCC(ret)) {
      if (share::schema::ObColumnSchemaV2::is_hidden_pk_column_id(column_ids[i])) {
        //隐藏主键列不处理
      } else {
        const ObDatum &insert_datum = replace_row->cells()[i];
        const ObDatum &del_datum = delete_row->cells()[i];
        int cmp_ret = 0;
        if (OB_FAIL(new_row.at(i)->basic_funcs_->null_first_cmp_(insert_datum, del_datum, cmp_ret))) {
          LOG_WARN("fail to compare", K(ret));
        } else if (0 != cmp_ret) {
          is_equal = false;
        }
      }
    }
  }
  return ret;
}

int ObTableApiReplaceExecutor::do_delete(ObConflictRowMap *primary_map)
{
  int ret = OB_SUCCESS;
  const ObTableEntity *entity = nullptr;
  const ObTableReplaceCtDef &ctdef = *replace_spec_.get_ctdefs().at(0);
  ObConflictRowMap::iterator start_row_iter = primary_map->begin();
  ObConflictRowMap::iterator end_row_iter = primary_map->end();

  for (; OB_SUCC(ret) && start_row_iter != end_row_iter; ++start_row_iter) {
    const ObRowkey &constraint_rowkey = start_row_iter->first;
    ObConflictValue &constraint_value = start_row_iter->second;
    if (NULL != constraint_value.baseline_datum_row_) {
      //baseline row is not empty, delete it
      if (OB_FAIL(stored_row_to_exprs(*constraint_value.baseline_datum_row_,
                                      get_primary_table_old_row(),
                                      eval_ctx_))) {
        LOG_WARN("fail to stored row to expr", K(ret));
      } else if (OB_FAIL(delete_row_to_das())) {
        LOG_WARN("fail to shuffle delete row", K(ret), K(constraint_value));
      } else {
        replace_rtdefs_.at(0).del_rtdef_.cur_row_num_ = 1;
      }
    }
  }

  return ret;
}

int ObTableApiReplaceExecutor::do_insert()
{
  int ret = OB_SUCCESS;
  const ObTableEntity *entity = static_cast<const ObTableEntity*>(tb_ctx_.get_entity());
  const ObTableReplaceCtDef &ctdef = *replace_spec_.get_ctdefs().at(0);

  if (OB_ISNULL(insert_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert row is null", K(ret));
  } else if (OB_FAIL(stored_row_to_exprs(*insert_row_, get_primary_table_new_row(), eval_ctx_))) {
    LOG_WARN("stored row to exprs faild", K(ret));
  } else if (OB_FAIL(insert_row_to_das())) {
    LOG_WARN("shuffle insert row failed", K(ret));
  } else {
    replace_rtdefs_.at(0).ins_rtdef_.cur_row_num_ = 1;
  }

  return ret;
}

int ObTableApiReplaceExecutor::cache_insert_row()
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

int ObTableApiReplaceExecutor::prepare_final_replace_task()
{
  int ret = OB_SUCCESS;
  ObConflictRowMap *primary_map = NULL;

  OZ(conflict_checker_.get_primary_table_map(primary_map));
  CK(OB_NOT_NULL(primary_map));
  // Notice: here need to clear the evaluated flag, cause the new_row used in try_insert is the same as old_row in do_delete
  // and if we don't clear the evaluated flag, the generated columns in old_row won't refresh and will use the new_row result
  // which will cause 4377 when do_delete
  clear_evaluated_flag();
  OZ(do_delete(primary_map));
  clear_evaluated_flag();
  OZ(do_insert());

  return ret;
}

int ObTableApiReplaceExecutor::reuse()
{
  int ret = OB_SUCCESS;

  if (dml_rtctx_.das_ref_.has_task()) {
    if (OB_FAIL(dml_rtctx_.das_ref_.close_all_task())) {
      LOG_WARN("fail to close all insert das task", K(ret));
    } else {
      dml_rtctx_.das_ref_.reuse();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(conflict_checker_.reuse())) {
      LOG_WARN("fail to reuse conflict checker", K(ret));
    }
  }

  return ret;
}

int ObTableApiReplaceExecutor::get_next_row()
{
  int ret = OB_SUCCESS;
  bool is_iter_end = false;

  while (OB_SUCC(ret) && !is_iter_end) {
    transaction::ObTxSEQ savepoint_no;
    set_need_fetch_conflict();
    if (OB_FAIL(ObSqlTransControl::create_anonymous_savepoint(exec_ctx_, savepoint_no))) {
      LOG_WARN("fail to create save_point", K(ret));
    } else if (OB_FAIL(load_replace_rows(is_iter_end))) {
      LOG_WARN("fail to load all row", K(ret));
    } else if (OB_FAIL(post_das_task())) {
      LOG_WARN("fail to post all das task", K(ret));
    } else if (!is_duplicated()) {
      LOG_DEBUG("try insert is not duplicated", K(ret));
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
    } else if (OB_FAIL(conflict_checker_.do_lookup_and_build_base_map(1))) {
      LOG_WARN("fail to do table lookup", K(ret));
    } else if (OB_FAIL(prepare_final_replace_task())) {
      LOG_WARN("fail to prepare final das task", K(ret));
    } else if (OB_FAIL(post_das_task())) {
      LOG_WARN("do insert rows post process failed", K(ret));
    }

    if (OB_SUCC(ret) && !is_iter_end) {
      // 只有还有下一个batch时才需要做reuse，如果没有下一个batch，close和destroy中会释放内存
      // 前边逻辑执行成功，这一批batch成功完成replace, reuse环境, 准备下一个batch
      if (OB_FAIL(reuse())) {
        LOG_WARN("fail to reuse", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    affected_rows_ = replace_rtdefs_.at(0).ins_rtdef_.cur_row_num_ + replace_rtdefs_.at(0).del_rtdef_.cur_row_num_;
    // auto inc 操作中, 同步全局自增值value
    if (tb_ctx_.has_auto_inc() && OB_FAIL(tb_ctx_.update_auto_inc_value())) {
      LOG_WARN("fail to update auto inc value", K(ret));
    }
  }
  return ret;
}

int ObTableApiReplaceExecutor::reset_das_env()
{
  int ret = OB_SUCCESS;

  // 释放第一次try insert的das task
  if (OB_FAIL(dml_rtctx_.das_ref_.close_all_task())) {
    LOG_WARN("close all das task failed", K(ret));
  } else {
    dml_rtctx_.das_ref_.reuse();
  }

  // 因为第二次插入不需要fetch conflict result了，如果有conflict
  // 就说明replace into的某些逻辑处理有问题
  for (int64_t i = 0; i < replace_rtdefs_.count(); i++) {
    ObTableInsRtDef &ins_rtdef = replace_rtdefs_.at(i).ins_rtdef_;
    ins_rtdef.das_rtdef_.need_fetch_conflict_ = false;
    ins_rtdef.das_rtdef_.is_duplicated_ = false;
  }

  return ret;
}

int ObTableApiReplaceExecutor::close()
{
  int ret = OB_SUCCESS;
  int close_ret = OB_SUCCESS;

  if (is_opened_) {
    if (OB_FAIL(conflict_checker_.close())) {
      LOG_WARN("fail to close conflict_checker", K(ret));
    }
    // close dml das tasks
    close_ret = ObTableApiModifyExecutor::close();
    // reset the new_row expr datum ptr:
    // each replace use the same spec in multi_replace and the new_row expr datum will
    // be set to a temporally ObChunkDatumStore when meet conflict row, which will be release after current replace
    // done, so need to reset the datum to its origin reserved buffer.Ohterwise, it may cause use after free in next replace
    const ObExprPtrIArray &new_row_exprs = get_primary_table_new_row();
    reset_new_row_datum(new_row_exprs);
  }

  return (OB_SUCCESS == ret) ? close_ret : ret;
}

}  // namespace table
}  // namespace oceanbase
