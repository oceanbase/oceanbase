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
#include "lib/utility/ob_tracepoint.h"
#include "sql/das/ob_das_insert_op.h"
#include "ob_table_cg_service.h"

namespace oceanbase
{
namespace table
{

int ObTableApiReplaceExecutor::generate_replace_rtdef(const ObTableReplaceCtDef &ctdef,
                                                      ObTableReplaceRtDef &rtdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(generate_ins_rtdef(ctdef.ins_ctdef_,
                                 rtdef.ins_rtdef_))) {
    LOG_WARN("fail to generate insert rtdef", K(ret));
  } else if (OB_FAIL(generate_del_rtdef(ctdef.del_ctdef_,
                                        rtdef.del_rtdef_))) {
    LOG_WARN("fail to generate delete rtdef", K(ret));
  } else {
    rtdef.ins_rtdef_.das_rtdef_.table_loc_->is_writing_ = true; //todo:linjing其他的executor还没有设置is_writting
  }

  return ret;
}

int ObTableApiReplaceExecutor::open()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTableApiModifyExecutor::open())) {
    LOG_WARN("fail to oepn ObTableApiModifyExecutor", K(ret));
  } else if (OB_FAIL(generate_replace_rtdef(replace_spec_.get_ctdef(), replace_rtdef_))) {
    LOG_WARN("fail to init replace rtdef", K(ret));
  } else {
    ObDASTabletLoc *tablet_loc = nullptr;
    ObDASTableLoc *table_loc = replace_rtdef_.ins_rtdef_.das_rtdef_.table_loc_;
    if (OB_ISNULL(table_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table location is invalid", K(ret));
    } else if (OB_FAIL(conflict_checker_.init_conflict_checker(replace_spec_.get_expr_frame_info(),
                                                               table_loc))) {
      LOG_WARN("fail to init conflict_checker", K(ret));
    } else if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
      LOG_WARN("fail to calc tablet location", K(ret));
    } else {
      conflict_checker_.set_local_tablet_loc(tablet_loc);
    }
  }

  return ret;
}

void ObTableApiReplaceExecutor::set_need_fetch_conflict()
{
  replace_rtdef_.ins_rtdef_.das_rtdef_.need_fetch_conflict_ = true;
  dml_rtctx_.set_pick_del_task_first();
  dml_rtctx_.set_non_sub_full_task();
}

int ObTableApiReplaceExecutor::refresh_exprs_frame(const ObTableEntity *entity)
{
  int ret = OB_SUCCESS;
  const ObTableReplaceCtDef &ctdef = replace_spec_.get_ctdef();

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

int ObTableApiReplaceExecutor::load_replace_rows(bool &is_iter_end)
{
  int ret = OB_SUCCESS;
  const ObTableReplaceCtDef &ctdef = replace_spec_.get_ctdef();

  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_row_from_child())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to load next row from child", K(ret));
      }
    } else if (OB_FAIL(insert_row_to_das(ctdef.ins_ctdef_, replace_rtdef_.ins_rtdef_))) {
      LOG_WARN("fail to insert row to das", K(ret));
    } else {
      replace_rtdef_.ins_rtdef_.cur_row_num_ = 1;
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
    const UIntFixedArray &column_ids = replace_spec_.get_ctdef().ins_ctdef_.column_ids_;
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
  const ObTableReplaceCtDef &ctdef = replace_spec_.get_ctdef();
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
      } else if (OB_FAIL(delete_row_to_das(ctdef.del_ctdef_, replace_rtdef_.del_rtdef_))) {
        LOG_WARN("fail to shuffle delete row", K(ret), K(constraint_value));
      } else {
        replace_rtdef_.del_rtdef_.cur_row_num_ = 1;
      }
    }
  }

  return ret;
}

int ObTableApiReplaceExecutor::do_insert()
{
  int ret = OB_SUCCESS;
  const ObTableEntity *entity = static_cast<const ObTableEntity*>(tb_ctx_.get_entity());
  const ObTableReplaceCtDef &ctdef = replace_spec_.get_ctdef();

  if (OB_ISNULL(insert_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert row is null", K(ret));
  } else if (OB_FAIL(stored_row_to_exprs(*insert_row_, get_primary_table_new_row(), eval_ctx_))) {
    LOG_WARN("stored row to exprs faild", K(ret));
  } else if (OB_FAIL(insert_row_to_das(ctdef.ins_ctdef_, replace_rtdef_.ins_rtdef_))) {
    LOG_WARN("shuffle insert row failed", K(ret));
  } else {
    replace_rtdef_.ins_rtdef_.cur_row_num_ = 1;
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
  OZ(do_delete(primary_map));
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
    } else if (OB_FAIL(reset_das_env(replace_rtdef_.ins_rtdef_))) {
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
    affected_rows_ = replace_rtdef_.ins_rtdef_.cur_row_num_ + replace_rtdef_.del_rtdef_.cur_row_num_;
    // auto inc 操作中, 同步全局自增值value
    if (tb_ctx_.has_auto_inc() && OB_FAIL(tb_ctx_.update_auto_inc_value())) {
      LOG_WARN("fail to update auto inc value", K(ret));
    }
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
  }

  return (OB_SUCCESS == ret) ? close_ret : ret;
}

}  // namespace table
}  // namespace oceanbase
