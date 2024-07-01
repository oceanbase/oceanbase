/**
 * Copyright (c) 2023 OceanBase
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
#include "ob_table_ttl_executor.h"
#include "src/observer/table/ob_table_cg_service.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/das/ob_das_insert_op.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "src/observer/table/ob_htable_utils.h"
using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{

int ObTableApiTTLExecutor::generate_ttl_rtdef(const ObTableTTLCtDef &ctdef, ObTableTTLRtDef &rtdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(generate_ins_rtdef(ctdef.ins_ctdef_,
                                 rtdef.ins_rtdef_))) {
    LOG_WARN("fail to generate insert rtdef", K(ret));
  } else if (OB_FAIL(generate_del_rtdef(ctdef.del_ctdef_,
                                        rtdef.del_rtdef_))) {
    LOG_WARN("fail to generate delete rtdef", K(ret));
  } else if (OB_FAIL(generate_upd_rtdef(ctdef.upd_ctdef_,
                                        rtdef.upd_rtdef_))) {
    LOG_WARN("fail to generate update rtdef", K(ret));
  }

  return ret;
}

int ObTableApiTTLExecutor::open()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTableApiModifyExecutor::open())) {
    LOG_WARN("fail to oepn ObTableApiModifyExecutor", K(ret));
  } else if (OB_FAIL(generate_ttl_rtdef(ttl_spec_.get_ctdef(), ttl_rtdef_))) {
    LOG_WARN("fail to generate ttl rtdef", K(ret));
  } else {
    ObDASTabletLoc *tablet_loc = nullptr;
    ObDASTableLoc *table_loc = ttl_rtdef_.ins_rtdef_.das_rtdef_.table_loc_;
    if (OB_ISNULL(table_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table location is invalid", K(ret));
    } else if (OB_FAIL(conflict_checker_.init_conflict_checker(ttl_spec_.get_expr_frame_info(), table_loc))) {
      LOG_WARN("fail to init conflict_checker", K(ret));
    } else if (OB_FAIL(calc_tablet_loc(tablet_loc))) {
      LOG_WARN("fail to calc tablet location", K(ret));
    } else {
      conflict_checker_.set_local_tablet_loc(tablet_loc);
      // init update das_ref
      ObMemAttr mem_attr;
      mem_attr.tenant_id_ = tb_ctx_.get_session_info().get_effective_tenant_id();
      mem_attr.label_ = "TableApiTTL";
      upd_rtctx_.das_ref_.set_expr_frame_info(ttl_spec_.get_expr_frame_info());
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

int ObTableApiTTLExecutor::refresh_exprs_frame(const ObTableEntity *entity)
{
  int ret = OB_SUCCESS;
  const ObTableTTLCtDef &ctdef = ttl_spec_.get_ctdef();
  const ObTableInsCtDef &ins_ctdef = ctdef.ins_ctdef_;
  const ObTableUpdCtDef &upd_ctdef = ctdef.upd_ctdef_;

  if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity is null", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::refresh_ttl_exprs_frame(tb_ctx_,
                                                                   ins_ctdef.new_row_,
                                                                   upd_ctdef.delta_row_,
                                                                   *entity))) {
    LOG_WARN("fail to refresh ttl exprs frame", K(ret), K(*entity));
  }

  return ret;
}

int ObTableApiTTLExecutor::get_next_row_from_child()
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

int ObTableApiTTLExecutor::try_insert_row()
{
  int ret = OB_SUCCESS;
  const ObTableTTLCtDef &ctdef = ttl_spec_.get_ctdef();

  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_row_from_child())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row from child", K(ret));
      }
    } else if (OB_FAIL(insert_row_to_das(ctdef.ins_ctdef_, ttl_rtdef_.ins_rtdef_))) {
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

int ObTableApiTTLExecutor::check_expired(bool &is_expired)
{
  int ret = OB_SUCCESS;
  ObExpr *expr = ttl_spec_.get_ctdef().expire_expr_;
  ObDatum *datum = nullptr;

  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expire expr is null", K(ret));
  } else if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
    LOG_WARN("fail to eval expire expr", K(ret), K(*expr));
  } else {
    is_expired = (!datum->is_null() && datum->get_bool());
  }

  return ret;
}

int ObTableApiTTLExecutor::do_delete()
{
  int ret = OB_SUCCESS;
  const ObTableTTLCtDef &ctdef = ttl_spec_.get_ctdef();

  if (OB_FAIL(delete_row_to_das(ctdef.del_ctdef_, ttl_rtdef_.del_rtdef_))) {
    LOG_WARN("fail to delete row to das", K(ret), K(ctdef), K_(ttl_rtdef));
  }

  return ret;
}

int ObTableApiTTLExecutor::do_insert()
{
  int ret = OB_SUCCESS;
  const ObTableTTLCtDef &ctdef = ttl_spec_.get_ctdef();
  const ObTableEntity *entity = static_cast<const ObTableEntity*>(tb_ctx_.get_entity());

  // do_insert前被conflict checker刷成了旧行，需要重新刷一遍
  if (OB_FAIL(refresh_exprs_frame(entity))) {
    LOG_WARN("fail to refresh exprs frame", K(ret));
  } else if (OB_FAIL(insert_row_to_das(ctdef.ins_ctdef_, ttl_rtdef_.ins_rtdef_))) {
    LOG_WARN("fail to insert row to das", K(ret), K(ctdef), K_(ttl_rtdef));
  }

  return ret;
}

int ObTableApiTTLExecutor::update_row_to_conflict_checker()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObConflictValue, 1> constraint_values;
  ObChunkDatumStore::StoredRow *insert_row = nullptr;
  ObTableUpdRtDef &upd_rtdef = ttl_rtdef_.upd_rtdef_;
  const ObTableEntity *entity = static_cast<const ObTableEntity*>(tb_ctx_.get_entity());
  const ObExprPtrIArray &new_row_exprs = get_primary_table_insert_row();

  if (OB_FAIL(refresh_exprs_frame(entity))) {
    LOG_WARN("fail to refresh exprs frame", K(ret));
  } else if (OB_FAIL(ObChunkDatumStore::StoredRow::build(insert_row, new_row_exprs, eval_ctx_, allocator_))) {
    LOG_WARN("fail to build stored row", K(ret), K(new_row_exprs));
  } else if (OB_ISNULL(insert_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert row is NULL", K(ret));
  } else if (OB_FAIL(conflict_checker_.check_duplicate_rowkey(insert_row, constraint_values, true))) {
    LOG_WARN("fail to check duplicated key", K(ret), KPC(insert_row));
  } else {
    upd_rtdef.found_rows_++;
    const ObChunkDatumStore::StoredRow *upd_new_row = insert_row;
    const ObChunkDatumStore::StoredRow *upd_old_row = constraint_values.at(0).current_datum_row_; // 这里只取第一行是和mysql对齐的
    if (OB_ISNULL(upd_old_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upd_old_row is NULL", K(ret));
    } else if (OB_FAIL(check_rowkey_change(*upd_old_row,
                                           *upd_new_row))) {
      LOG_WARN("can not update rowkey column", K(ret));
    } else if (OB_FAIL(check_whether_row_change(*upd_old_row,
                                                *upd_new_row,
                                                ttl_spec_.get_ctdef().upd_ctdef_,
                                                is_row_changed_))) {
      LOG_WARN("fail to check whether row change", K(ret));
    } else if (is_row_changed_) {
      // do update
      clear_evaluated_flag();
      if (OB_FAIL(conflict_checker_.update_row(upd_new_row, upd_old_row))) {
        LOG_WARN("fail to update row in conflict_checker", K(ret), KPC(upd_new_row), KPC(upd_old_row));
      }
    }
  }

  return ret;
}

// create table t(c1 int primary key, c2 int, c3 int,
//                unique key idx0(c2), unique key idx1(c3));
// insert into t values(1, 1, 1),(2, 2, 2),(3, 3, 3);
// insert into t values(3,1,2) ON DUPLICATE KEY UPDATE c1=4;
// 执行insert后map中有3个元素：
//  1->(base:1, 1, 1 curr:1, 1, 1),
//  2->(base:2, 2, 2 curr:2, 2, 2),
//  3->(base:3, 3, 3 curr:3, 3, 3);
// 执行conflict_checker_.update_row后:
//  1->(base:1, 1, 1 curr:nul),
//  2->(base:2, 2, 2 curr:nul),
//  3->(base:3, 3, 3 curr:4, 3, 3);
// delete->base:3, 3, 3, insert->curr:4, 3, 3
// result:
// +----+------+------+
// | c1 | c2   | c3   |
// +----+------+------+
// |  1 |    1 |    1 |
// |  2 |    2 |    2 |
// |  4 |    3 |    3 |
// +----+------+------+
int ObTableApiTTLExecutor::update_row_to_das()
{
  int ret = OB_SUCCESS;
  ObConflictRowMap *primary_map = nullptr;
  OZ(conflict_checker_.get_primary_table_map(primary_map));
  CK(OB_NOT_NULL(primary_map));
  ObConflictRowMap::iterator start = primary_map->begin();
  ObConflictRowMap::iterator end = primary_map->end();
  for (; OB_SUCC(ret) && start != end; ++start) {
    const ObRowkey &constraint_rowkey = start->first;
    const ObConflictValue &constraint_value = start->second;
    if (!is_row_changed_) {
      // do nothing
    } else if (constraint_value.new_row_source_ != ObNewRowSource::FROM_UPDATE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected row source", K(ret), K(constraint_value.new_row_source_));
    } else { // FROM_UPDATE
      // baseline_datum_row_ 代表存储扫描回来的冲突旧行
      // current_datum_row_ 当前更新的新行
      if (NULL != constraint_value.baseline_datum_row_ &&
          NULL != constraint_value.current_datum_row_) {
        OZ(stored_row_to_exprs(*constraint_value.baseline_datum_row_,
                               get_primary_table_upd_old_row(),
                               eval_ctx_));
        OZ(delete_upd_old_row_to_das(constraint_rowkey,
                                     constraint_value,
                                     ttl_spec_.get_ctdef().upd_ctdef_,
                                     ttl_rtdef_.upd_rtdef_,
                                     upd_rtctx_));
        OZ(to_expr_skip_old(*constraint_value.current_datum_row_,
                            ttl_spec_.get_ctdef().upd_ctdef_));
        clear_evaluated_flag();
        OZ(insert_upd_new_row_to_das(ttl_spec_.get_ctdef().upd_ctdef_,
                                     ttl_rtdef_.upd_rtdef_,
                                     upd_rtctx_));
      } else if (NULL == constraint_value.baseline_datum_row_ &&
                 NULL != constraint_value.current_datum_row_) { // 单单是唯一索引冲突的时候，会走这个分支
        OZ(to_expr_skip_old(*constraint_value.current_datum_row_,
                            ttl_spec_.get_ctdef().upd_ctdef_));
        OZ(insert_upd_new_row_to_das(ttl_spec_.get_ctdef().upd_ctdef_,
                                     ttl_rtdef_.upd_rtdef_,
                                     upd_rtctx_));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected constraint_value", K(ret));
      }
    }
  }

  return ret;
}

int ObTableApiTTLExecutor::do_update()
{
  int ret = OB_SUCCESS;
  const ObTableTTLCtDef &ctdef = ttl_spec_.get_ctdef();

  // 1. 刷frame，刷到upd_ctdef.new_row中
  // 2. conflict_checker_.update_row
  // 3. 遍历冲突map，do_update,参考ObTableApiInsertUpExecutor::do_update

  if (OB_FAIL(conflict_checker_.do_lookup_and_build_base_map(1))) { // 1. 使用冲突行去回表，构造冲突行map，key为rowkey，value为旧行
    LOG_WARN("fail to do table lookup", K(ret));
  } else if (OB_FAIL(update_row_to_conflict_checker())) { // 2. 将更新的行刷到conflict_checker，目的是检查更新的行是否还有冲突
    LOG_WARN("fail to update row to conflict checker", K(ret));
  } else if (OB_FAIL(update_row_to_das())) { // 3. 根据实际情况更新行到das
    LOG_WARN("fail to update row to das", K(ret));
  }

  return ret;
}

// 1. 过期，删除旧行，写入新行
// 2. 未过期
//   a. 当前是insert操作，则报错OB_ERR_ROWKEY_CONFLICT
//   b. 当前是insertUp操作，则做update
int ObTableApiTTLExecutor::process_expire()
{
  int ret = OB_SUCCESS;
  ObConflictRowMap *map = nullptr;

  if (OB_FAIL(conflict_checker_.do_lookup_and_build_base_map(1))) {
    LOG_WARN("fail to build conflict map", K(ret));
  } else if (OB_FAIL(conflict_checker_.get_primary_table_map(map))) {
    LOG_WARN("fail to get conflict row map", K(ret));
  } else {
    ObConflictRowMap::iterator start = map->begin();
    ObConflictRowMap::iterator end = map->end();
    for (; OB_SUCC(ret) && start != end; ++start) {
      const ObRowkey &rowkey = start->first;
      ObConflictValue &old_row = start->second;
      if (NULL != old_row.baseline_datum_row_) {
        // 将旧行刷到表达式中，判断是否过期
        if (OB_FAIL(stored_row_to_exprs(*old_row.baseline_datum_row_, get_primary_table_upd_old_row(), eval_ctx_))) {
          LOG_WARN("fail to store row to exprs", K(ret));
        } else if (OB_FAIL(check_expired(is_expired_))) {
          LOG_WARN("fail to check expired", K(ret));
        } else if (is_expired_) { // 过期，删除旧行，写入新行
          LOG_DEBUG("row is expired", K(ret));
          // Notice: here need to clear the evaluated flag, cause the new_row used in try_insert is the same as old_row in do_delete
          //   and if we don't clear the evaluated flag, the generated columns in old_row won't refresh and will use the new_row result
          //   which will cause 4377 when do_delete
          clear_evaluated_flag();
          if (OB_FAIL(do_delete())) {
            LOG_WARN("fail to delete expired old row", K(ret));
          } else if (OB_FAIL(do_insert())) {
            LOG_WARN("fail to insert new row", K(ret));
          } else if (OB_FAIL(execute_das_task(dml_rtctx_, true/* del_task_ahead */))) {
            LOG_WARN("fail to execute insert das task", K(ret));
          } else {
            insert_rows_ = 1;
          }
        } else { // 未过期, insert操作，则报错OB_ERR_ROWKEY_CONFLICT; insertUp操作，则做update
          if (tb_ctx_.is_insert()) {
            ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
            LOG_INFO("current insert is primary key duplicate", K(ret));
          } else { // insertUp
            LOG_DEBUG("row not expired, do update", K(ret));
            if (OB_FAIL(do_update())) {
              LOG_WARN("fail to do update", K(ret));
            } else if (OB_FAIL(execute_das_task(upd_rtctx_, true/* del_task_ahead */))) {
              LOG_WARN("fail to execute upd_rtctx_ das task", K(ret));
            } else if (OB_FAIL(execute_das_task(dml_rtctx_, false/* del_task_ahead */))) {
              LOG_WARN("fail to execute dml_rtctx_ das task", K(ret));
            }
          }
        }
      }
    }
  }

  return ret;
}

// 1. 获取快照点
// 2. 写入记录
//   a. 不冲突，写入成功
//   b. 冲突，需要进一步判断是否是过期
//     ⅰ . 回归到快照点
//     ⅰⅰ. 处理过期逻辑
//       1. 过期，删除旧行，写入新行
//       2. 未过期
//         a. 当前是insert操作，则报错OB_ERR_ROWKEY_CONFLICT
//         b. 当前是insertUp操作，则做update
int ObTableApiTTLExecutor::get_next_row()
{
  int ret = OB_SUCCESS;
  transaction::ObTxSEQ savepoint_no;

  set_need_fetch_conflict(upd_rtctx_, ttl_rtdef_.ins_rtdef_);
  if (OB_FAIL(ObSqlTransControl::create_anonymous_savepoint(exec_ctx_, savepoint_no))) {
    LOG_WARN("fail to create save_point", K(ret));
  } else if (OB_FAIL(try_insert_row())) {
    LOG_WARN("fail to do first insert", K(ret));
  } else if (!is_duplicated()) {
    insert_rows_ = 1;
    LOG_DEBUG("no duplicated, insert successfully");
  } else if (OB_FAIL(fetch_conflict_rowkey(conflict_checker_))) {// 获取所有的冲突主键
    LOG_WARN("fail to fetch conflict rowkey", K(ret));
  } else if (OB_FAIL(reset_das_env(ttl_rtdef_.ins_rtdef_))) { // reuse das 相关信息
    LOG_WARN("fail to reset das env", K(ret));
  } else if (OB_FAIL(ObSqlTransControl::rollback_savepoint(exec_ctx_, savepoint_no))) { // 本次插入存在冲突, 回滚到save_point
    LOG_WARN("fail to rollback to save_point", K(ret), K(savepoint_no));
  } else if (OB_FAIL(process_expire())) { // 处理过期
    LOG_WARN("fail to process expire", K(ret));
  }

  if (OB_SUCC(ret)) {
    affected_rows_ = insert_rows_ + ttl_rtdef_.upd_rtdef_.found_rows_;
  }

  return ret;
}

int ObTableApiTTLExecutor::close()
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
    const ObExprPtrIArray &new_row_exprs = get_primary_table_insert_row();
    reset_new_row_datum(new_row_exprs);
  }

  return (OB_SUCCESS == ret) ? close_ret : ret;
}

}  // namespace table
}  // namespace oceanbase
