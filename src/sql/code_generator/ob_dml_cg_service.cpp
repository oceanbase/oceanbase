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

#define USING_LOG_PREFIX SQL_CG
#include "lib/utility/ob_tracepoint.h"
#include "sql/code_generator/ob_dml_cg_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/schema/ob_schema_mgr.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/dml/ob_dml_ctx_define.h"
#include "share/config/ob_server_config.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/engine/dml/ob_conflict_checker.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_log_for_update.h"
#include "sql/optimizer/ob_log_merge.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_update.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{
template <typename T>
bool var_exist_in_array(const ObIArray<T> &array, const T &var, int32_t &idx)
{
  int64_t tmp_idx = -1;
  bool bret = has_exist_in_array(array, var, &tmp_idx);
  idx = static_cast<int32_t>(tmp_idx);
  return bret;
}

int ObDmlCgService::generate_insert_ctdef(ObLogDelUpd &op,
                                          const IndexDMLInfo &index_dml_info,
                                          ObInsCtDef *&ins_ctdef)
{
  int ret = OB_SUCCESS;
  ObDMLCtDefAllocator<ObInsCtDef> ins_ctdef_allocator(cg_.phy_plan_->get_allocator());
  if (OB_ISNULL(ins_ctdef = ins_ctdef_allocator.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate ins ctdef failed", K(ret));
  } else if (OB_FAIL(generate_insert_ctdef(op, index_dml_info, *ins_ctdef))) {
    LOG_WARN("generate insert ctdef failed", K(ret));
  }
  return ret;
}

int ObDmlCgService::generate_insert_ctdef(ObLogDelUpd &op,
                                          const IndexDMLInfo &index_dml_info,
                                          ObInsCtDef &ins_ctdef)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to generate insert ctdef", K(index_dml_info));
  ObArray<ObRawExpr*> old_row;
  ObArray<ObRawExpr*> new_row;
  if (OB_ISNULL(op.get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(convert_insert_new_row_exprs(index_dml_info, new_row))) {
    LOG_WARN("convert insert new row exprs failed", K(ret));
  } else if (OB_FAIL(generate_dml_base_ctdef(op, index_dml_info,
                                             ins_ctdef,
                                             ObTriggerEvents::get_insert_event(),
                                             old_row,
                                             new_row))) {
    LOG_WARN("generate dml base ctdef failed", K(ret), K(index_dml_info));
  } else if (index_dml_info.is_primary_index_ //generate column infos
      && OB_FAIL(add_all_column_infos(op,
                                      index_dml_info.column_exprs_,
                                      ins_ctdef.is_heap_table_,
                                      ins_ctdef.column_infos_))) {
    LOG_WARN("add column info failed", K(ret), K(index_dml_info.column_exprs_));
  } else if (OB_FAIL(generate_das_ins_ctdef(op,
                                            index_dml_info.ref_table_id_,
                                            index_dml_info,
                                            ins_ctdef.das_ctdef_,
                                            new_row))) {
    LOG_WARN("generate das insert ctdef failed", K(ret));
  } else if (OB_FAIL(generate_related_ins_ctdef(op,
                                                index_dml_info.related_index_ids_,
                                                index_dml_info,
                                                new_row,
                                                ins_ctdef.related_ctdefs_))) {
    LOG_WARN("generate related ins ctdef failed", K(ret));
  }
  if (OB_SUCC(ret) && index_dml_info.is_primary_index_ && op.get_err_log_define().is_err_log_) {
    if (OB_FAIL(generate_err_log_ctdef(op.get_err_log_define(), ins_ctdef.error_logging_ctdef_))) {
      LOG_WARN("generate error_logging ctdef failed", K(ret), K(index_dml_info));
    }
  }

  // generate for replace into and insert_up fetch conflict rowkey
  if (OB_SUCC(ret) && op.get_stmt()->is_insert_stmt() &&
      (static_cast<ObLogInsert&>(op).is_replace() || static_cast<ObLogInsert&>(op).get_insert_up())) {
    ObSEArray<ObRawExpr *, 8> rowkey_exprs;
    const ObIArray<IndexDMLInfo *> &insert_dml_infos = op.get_index_dml_infos();;
    const IndexDMLInfo *primary_dml_info = insert_dml_infos.at(0);
    if (OB_FAIL(convert_data_table_rowkey_info(op, primary_dml_info, ins_ctdef))) {
      LOG_WARN("fail to convert table rowkey info", K(ret), K(rowkey_exprs));
    }
  }

  if (OB_SUCC(ret) && NULL != index_dml_info.new_part_id_expr_) {
    //generate multi_ctdef
    ObDMLCtDefAllocator<ObMultiInsCtDef> multi_ins_allocator(cg_.phy_plan_->get_allocator());
    if (OB_ISNULL(ins_ctdef.multi_ctdef_ = multi_ins_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate multi ins ctdef failed", K(ret));
    } else if (OB_FAIL(generate_multi_ins_ctdef(index_dml_info, *ins_ctdef.multi_ctdef_))) {
      LOG_WARN("generate multi ins ctdef failed", K(ret), K(index_dml_info));
    }
  }
  if (OB_SUCC(ret) && op.is_single_value()) {
    ins_ctdef.is_single_value_ = true;
  }

  LOG_TRACE("finish generate insert ctdef", K(ret), K(ins_ctdef));
  return ret;
}

int ObDmlCgService::generate_lock_ctdef(ObLogForUpdate &op,
                                        const IndexDMLInfo &index_dml_info,
                                        ObLockCtDef *&lock_ctdef)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to generate lock ctdef", K(index_dml_info));
  ObDMLCtDefAllocator<ObLockCtDef> lock_ctdef_allocator(cg_.phy_plan_->get_allocator());
  ObArray<ObRawExpr*> old_row;
  ObArray<ObRawExpr*> new_row;
  if (OB_ISNULL(lock_ctdef = lock_ctdef_allocator.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate lock ctdef failed", K(ret));
  } else if (OB_FAIL(old_row.assign(index_dml_info.column_old_values_exprs_))) {
    LOG_WARN("fail to assign lock old row", K(ret));
  } else if (OB_FAIL(generate_dml_base_ctdef(op,
                                             index_dml_info,
                                             *lock_ctdef,
                                             old_row,
                                             new_row))) {
    LOG_WARN("generate dml base ctdef failed", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(old_row, lock_ctdef->old_row_))) {
    LOG_WARN("generate lock old row exprs failed", K(ret), K(old_row));
  } else if (OB_FAIL(generate_das_lock_ctdef(op, index_dml_info,
                                             lock_ctdef->das_ctdef_,
                                             old_row))) {
    LOG_WARN("generate das lock ctdef failed", K(ret));
  } else {
    lock_ctdef->need_check_filter_null_ = index_dml_info.need_filter_null_;
  }
  if (OB_SUCC(ret) && NULL != index_dml_info.old_part_id_expr_) {
    //generate multi_ctdef
    ObDMLCtDefAllocator<ObMultiLockCtDef> multi_del_allocator(cg_.phy_plan_->get_allocator());
    if (OB_ISNULL(lock_ctdef->multi_ctdef_ = multi_del_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate multi lock ctdef failed", K(ret));
    } else if (OB_FAIL(generate_multi_lock_ctdef(index_dml_info, *lock_ctdef->multi_ctdef_))) {
      LOG_WARN("generate multi lock ctdef failed", K(ret), K(index_dml_info));
    }
  }
  return ret;
}

int ObDmlCgService::generate_merge_ctdef(ObLogMerge &op,
                                         ObMergeCtDef *&merge_ctdef,
                                         uint64_t index_table_id)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to generate merge ctdef", K(index_table_id));
  const ObMergeStmt *merge_stmt = static_cast<const ObMergeStmt*>(op.get_stmt());
  ObDMLCtDefAllocator<ObMergeCtDef> merge_ctdef_allocator(cg_.phy_plan_->get_allocator());

  if (OB_ISNULL(merge_ctdef = merge_ctdef_allocator.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate merge ctdef failed", K(ret));
  }

  if (OB_SUCC(ret) && merge_stmt->has_insert_clause()) {
    ObInsCtDef *ins_ctdef = NULL;
    bool found_dml_info = false;
    const ObIArray<IndexDMLInfo *> &index_dml_infos = op.get_index_dml_infos();
    for (int i = 0; !found_dml_info && OB_SUCC(ret) && i < index_dml_infos.count(); i++) {
      if (OB_ISNULL(index_dml_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dml info is null", K(ret));
      } else if (index_dml_infos.at(i)->ref_table_id_ == index_table_id) {
        if (OB_FAIL(generate_insert_ctdef(op, *index_dml_infos.at(i), ins_ctdef))) {
          LOG_WARN("fail to generate insert ctdef", K(ret),
                   "index_dml_info", *index_dml_infos.at(i));
        } else {
          merge_ctdef->ins_ctdef_ = ins_ctdef;
        }
        found_dml_info = true;
      }
    }
  } // end of generate ins_ctdef

  if (OB_SUCC(ret) && merge_stmt->has_update_clause()) {
    ObUpdCtDef *upd_ctdef = NULL;
    bool found_dml_info = false;
    const ObIArray<IndexDMLInfo *> &upd_dml_infos = op.get_update_infos();
    for (int i = 0; !found_dml_info && OB_SUCC(ret) && i < upd_dml_infos.count(); i++) {
      if (OB_ISNULL(upd_dml_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update dml info is null", K(ret));
      } else if (upd_dml_infos.at(i)->ref_table_id_ == index_table_id) {
        if (OB_FAIL(generate_update_ctdef(op, *upd_dml_infos.at(i), upd_ctdef))) {
          LOG_WARN("fail to generate upd_ctdef", K(ret), "index_dml_info", *upd_dml_infos.at(i));
        } else {
          merge_ctdef->upd_ctdef_ = upd_ctdef;
        }
        found_dml_info = true;
      }
    } // end upd_ctdef

    if (OB_SUCC(ret) && !op.get_delete_condition().empty()) {
      ObDelCtDef *del_ctdef = NULL;
      bool found_del_dml_info = false;
      const ObIArray<IndexDMLInfo *> &del_dml_infos = op.get_delete_infos();
      for (int i = 0; !found_del_dml_info && OB_SUCC(ret) && i < del_dml_infos.count(); i++) {
        if (OB_ISNULL(del_dml_infos.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("delete dml info is null", K(ret));
        } else if (del_dml_infos.at(i)->ref_table_id_ == index_table_id) {
          if (OB_FAIL(generate_delete_ctdef(op, *del_dml_infos.at(i), del_ctdef))) {
            LOG_WARN("fail to generate upd_ctdef", K(ret), "index_dml_info", *del_dml_infos.at(i));
          } else {
            merge_ctdef->del_ctdef_ = del_ctdef;
          }
          found_del_dml_info = true;
        }
      }
    }
  }

  LOG_TRACE("finish generate merge ctdef", K(ret), KPC(merge_ctdef));
  return ret;
}

int ObDmlCgService::generate_delete_ctdef(ObLogDelUpd &op,
                                          const IndexDMLInfo &index_dml_info,
                                          ObDelCtDef *&del_ctdef)
{
  int ret = OB_SUCCESS;
  ObDMLCtDefAllocator<ObDelCtDef> del_ctdef_allocator(cg_.phy_plan_->get_allocator());
  if (OB_ISNULL(del_ctdef = del_ctdef_allocator.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate del ctdef failed", K(ret));
  } else if (OB_FAIL(generate_delete_ctdef(op, index_dml_info, *del_ctdef))) {
    LOG_WARN("generate delete ctdef failed", K(ret));
  }
  return ret;
}

int ObDmlCgService::generate_delete_ctdef(ObLogDelUpd &op,
                                          const IndexDMLInfo &index_dml_info,
                                          ObDelCtDef &del_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 64> old_row;
  ObSEArray<ObRawExpr*, 64> new_row;
  if (OB_FAIL(old_row.assign(index_dml_info.column_old_values_exprs_))) {
    LOG_WARN("fail to assign delete old row", K(ret));
  } else if (OB_FAIL(generate_dml_base_ctdef(op,
                                             index_dml_info,
                                             del_ctdef,
                                             ObTriggerEvents::get_delete_event(),
                                             old_row,
                                             new_row))) {
    LOG_WARN("generate dml base ctdef failed", K(ret), K(index_dml_info));
  } else if (OB_FAIL(generate_das_del_ctdef(op,
                                            index_dml_info.ref_table_id_,
                                            index_dml_info,
                                            del_ctdef.das_ctdef_,
                                            old_row))) {
    LOG_WARN("generate das delete ctdef failed", K(ret));
  } else if (OB_FAIL(generate_related_del_ctdef(op,
                                                index_dml_info.related_index_ids_,
                                                index_dml_info,
                                                old_row,
                                                del_ctdef.related_ctdefs_))) {
    LOG_WARN("generate related del ctdef failed", K(ret));
  } else {
    del_ctdef.need_check_filter_null_ = index_dml_info.need_filter_null_;
    del_ctdef.distinct_algo_ = index_dml_info.distinct_algo_;
  }

  if (OB_SUCC(ret) && lib::is_oracle_mode() &&
      index_dml_info.is_primary_index_ && op.get_err_log_define().is_err_log_) {
    if (OB_FAIL(generate_err_log_ctdef(op.get_err_log_define(), del_ctdef.error_logging_ctdef_))) {
      LOG_WARN("generate error_logging ctdef failed", K(ret), K(index_dml_info));
    }
  }

  if (OB_SUCC(ret) && NULL != index_dml_info.old_part_id_expr_) {
    //generate multi_ctdef
    ObDMLCtDefAllocator<ObMultiDelCtDef> multi_del_allocator(cg_.phy_plan_->get_allocator());
    if (OB_ISNULL(del_ctdef.multi_ctdef_ = multi_del_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate multi del ctdef failed", K(ret));
    } else if (OB_FAIL(generate_multi_del_ctdef(index_dml_info, *del_ctdef.multi_ctdef_))) {
      LOG_WARN("generate multi delete ctdef failed", K(ret), K(index_dml_info));
    }
  }

  if (OB_SUCC(ret) && index_dml_info.is_primary_index_) {
    ObSEArray<ObRawExpr*, 16> distinct_exprs;
    if (OB_FAIL(get_table_unique_key_exprs(op, index_dml_info, distinct_exprs))) {
      LOG_WARN("get table unique key exprs failed", K(ret), K(index_dml_info));
    } else if (OB_FAIL(cg_.generate_rt_exprs(distinct_exprs, del_ctdef.distinct_key_))) {
      LOG_WARN("generate distinct_key exprs failed", K(ret), K(distinct_exprs));
    }
  }
  LOG_TRACE("generate delete ctdef", K(ret), K(del_ctdef));
  return ret;
}

int ObDmlCgService::generate_update_ctdef(ObLogDelUpd &op,
                                          const IndexDMLInfo &index_dml_info,
                                          ObUpdCtDef *&upd_ctdef)
{
  int ret = OB_SUCCESS;
  ObDMLCtDefAllocator<ObUpdCtDef> upd_allocator(cg_.phy_plan_->get_allocator());
  if (OB_ISNULL(upd_ctdef = upd_allocator.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate upd ctdef failed", K(ret));
  } else if (OB_FAIL(generate_update_ctdef(op, index_dml_info, *upd_ctdef))) {
    LOG_WARN("generate update ctdef failed", K(ret));
  }
  return ret;
}

int ObDmlCgService::generate_update_ctdef(ObLogDelUpd &op,
                                          const IndexDMLInfo &index_dml_info,
                                          ObUpdCtDef &upd_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 64> old_row;
  ObSEArray<ObRawExpr*, 64> new_row;
  ObSEArray<ObRawExpr*, 64> full_row;
  const ObAssignments &assigns = index_dml_info.assignments_;
  bool gen_expand_ctdef = false;
  LOG_TRACE("begin to generate update ctdef", K(index_dml_info));
  if (OB_FAIL(old_row.assign(index_dml_info.column_old_values_exprs_))) {
    LOG_WARN("fail to assign update old row", K(ret));
  } else if (OB_FAIL(new_row.assign(old_row))) {
    LOG_WARN("assign new row failed", K(ret));
  } else if (OB_FAIL(append(full_row, old_row))) {
    LOG_WARN("append old row expr to full row failed", K(ret), K(old_row));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); ++i) {
    const ObColumnRefRawExpr *col = assigns.at(i).column_expr_;
    ObRawExpr *assign_expr = assigns.at(i).expr_;
    int64_t assign_idx = OB_INVALID_INDEX;
    if (!has_exist_in_array(index_dml_info.column_exprs_,
                            const_cast<ObColumnRefRawExpr*>(col),
                            &assign_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update column not found in all columns", K(ret), KPC(col));
    } else if (OB_FAIL(full_row.push_back(assign_expr))) {
      LOG_WARN("add assign expr to full row failed", K(ret), KPC(assign_expr));
    } else {
      new_row.at(assign_idx) = assign_expr;
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(generate_dml_base_ctdef(op, index_dml_info,
                                             upd_ctdef,
                                             ObTriggerEvents::get_update_event(),
                                             old_row,
                                             new_row))) {
    LOG_WARN("generate dml base ctdef failed", K(ret), K(index_dml_info));
  } else if (OB_FAIL(cg_.generate_rt_exprs(full_row, upd_ctdef.full_row_))) {
    LOG_WARN("generate dml update full row exprs failed", K(ret), K(full_row));
  } else if (OB_FAIL(generate_das_upd_ctdef(op,
                                            index_dml_info.ref_table_id_,
                                            index_dml_info,
                                            upd_ctdef.dupd_ctdef_,
                                            old_row,
                                            new_row,
                                            full_row))) {
    LOG_WARN("generate das update ctdef failed", K(ret));
  } else if (OB_FAIL(generate_related_upd_ctdef(op,
                                                index_dml_info.related_index_ids_,
                                                index_dml_info,
                                                old_row,
                                                new_row,
                                                full_row,
                                                upd_ctdef.related_upd_ctdefs_))) {
    LOG_WARN("generate related upd ctdef failed", K(ret));
  } else if (OB_FAIL(convert_upd_assign_infos(upd_ctdef.is_heap_table_,
                                              index_dml_info,
                                              upd_ctdef.assign_columns_))) {
    LOG_WARN("convert upd assign infos failed", K(ret), K(index_dml_info));
  } else {
    upd_ctdef.need_check_filter_null_ = index_dml_info.need_filter_null_;
    upd_ctdef.distinct_algo_ = index_dml_info.distinct_algo_;
  }

  // create table t1 (c1 int primary key, c2 int, c3 int) partition by hash(c1) partitions 4;
  // create index t1_idx_c3 on t1(c3);
  // insert into t1 values(1,1,1)(1,2,2) on duplicate key update c3 = c3+1;
  // The final state of t1 is: insert (1, 1, 2),
  // you only need to do update_insert for storage, so you must generate delete + insert ctdef
  if (OB_SUCC(ret) && log_op_def::LOG_INSERT == op.get_type()) {
    ObLogInsert &log_ins_op = static_cast<ObLogInsert &>(op);
    if (log_ins_op.get_insert_up()) {
      gen_expand_ctdef = true;
    }
  }

  if (OB_SUCC(ret) && (index_dml_info.is_update_part_key_ || gen_expand_ctdef)) {
    //the updated row may be moved across partitions, need to generate das delete and das insert
    ObDMLCtDefAllocator<ObDASDelCtDef> ddel_allocator(cg_.phy_plan_->get_allocator());
    ObDMLCtDefAllocator<ObDASInsCtDef> dins_allocator(cg_.phy_plan_->get_allocator());
    if (OB_ISNULL(upd_ctdef.ddel_ctdef_ = ddel_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate das del ctdef failed", K(ret));
    } else if (OB_ISNULL(upd_ctdef.dins_ctdef_ = dins_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate das ins ctdef failed", K(ret));
    } else if (OB_FAIL(generate_das_del_ctdef(op,
                                              index_dml_info.ref_table_id_,
                                              index_dml_info,
                                              *upd_ctdef.ddel_ctdef_,
                                              old_row))) {
      LOG_WARN("generate das delete ctdef for update failed", K(ret));
    } else if (OB_FAIL(generate_related_del_ctdef(op,
                                                  index_dml_info.related_index_ids_,
                                                  index_dml_info,
                                                  old_row,
                                                  upd_ctdef.related_del_ctdefs_))) {
      LOG_WARN("generate related del ctdef failed", K(ret));
    } else if (OB_FAIL(generate_das_ins_ctdef(op,
                                              index_dml_info.ref_table_id_,
                                              index_dml_info,
                                              *upd_ctdef.dins_ctdef_,
                                              new_row))) {
      LOG_WARN("generate das insert ctdef for update failed", K(ret));
    } else if (OB_FAIL(generate_related_ins_ctdef(op,
                                                  index_dml_info.related_index_ids_,
                                                  index_dml_info,
                                                  new_row,
                                                  upd_ctdef.related_ins_ctdefs_))) {
      LOG_WARN("generate related ins ctdef failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    //the updated row may not be changed, so need to generate das lock op
    ObDMLCtDefAllocator<ObDASLockCtDef> dlock_allocator(cg_.phy_plan_->get_allocator());
    if (OB_ISNULL(upd_ctdef.dlock_ctdef_ = dlock_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate das lock ctdef failed", K(ret));
    } else if (OB_FAIL(generate_das_lock_ctdef(op, index_dml_info,
                                               *upd_ctdef.dlock_ctdef_,
                                               old_row))) {
      LOG_WARN("generate das lock ctdef for update failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && lib::is_oracle_mode() &&
      index_dml_info.is_primary_index_ && op.get_err_log_define().is_err_log_) {
    if (OB_FAIL(generate_err_log_ctdef(op.get_err_log_define(), upd_ctdef.error_logging_ctdef_))) {
      LOG_WARN("generate error_logging ctdef failed", K(ret), K(index_dml_info));
    }
  }

  if (OB_SUCC(ret) &&
      NULL != index_dml_info.old_part_id_expr_ &&
      NULL != index_dml_info.new_part_id_expr_) {
    //generate multi_ctdef
    ObDMLCtDefAllocator<ObMultiUpdCtDef> multi_upd_allocator(cg_.phy_plan_->get_allocator());
    if (OB_ISNULL(upd_ctdef.multi_ctdef_ = multi_upd_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate multi upd ctdef failed", K(ret));
    } else if (OB_FAIL(generate_multi_upd_ctdef(op, index_dml_info, *upd_ctdef.multi_ctdef_))) {
      LOG_WARN("generate multi update ctdef failed", K(ret), K(index_dml_info));
    }
  }

  if (OB_SUCC(ret) && index_dml_info.is_primary_index_) {
    ObSEArray<ObRawExpr*, 16> distinct_exprs;
    if (OB_FAIL(get_table_unique_key_exprs(op, index_dml_info, distinct_exprs))) {
      LOG_WARN("get table unique key exprs failed", K(ret), K(index_dml_info));
    } else if (OB_FAIL(cg_.generate_rt_exprs(distinct_exprs, upd_ctdef.distinct_key_))) {
      LOG_WARN("generate distinct_key exprs failed", K(ret), K(distinct_exprs));
    }
  }
  LOG_TRACE("finish generate update ctdef", K(ret), K(upd_ctdef));
  return ret;
}

int ObDmlCgService::get_table_rowkey_exprs(const IndexDMLInfo &index_dml_info,
                                           ObIArray<ObRawExpr*> &rowkey_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t j = 0; OB_SUCC(ret) && j < index_dml_info.rowkey_cnt_; ++j) {
    ObColumnRefRawExpr *col = index_dml_info.column_exprs_.at(j);
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (OB_FAIL(rowkey_exprs.push_back(col))) {
      LOG_WARN("fail to push column expr", K(ret), KPC(col));
    }
  }
  return ret;
}

// for virtual generated column.
int ObDmlCgService::adjust_unique_key_exprs(ObIArray<ObRawExpr*> &unique_key_exprs)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> tmp_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < unique_key_exprs.count(); ++i) {
    ObRawExpr *expr = unique_key_exprs.at(i);
    if (expr->is_column_ref_expr() &&
      static_cast<ObColumnRefRawExpr *>(expr)->is_virtual_generated_column()) {
      // do nothing.
      ObRawExpr *tmp_expr = static_cast<ObColumnRefRawExpr *>(expr)->get_dependant_expr();
      if (OB_FAIL(add_var_to_array_no_dup(tmp_exprs, tmp_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      }
    } else {
      if (OB_FAIL(add_var_to_array_no_dup(tmp_exprs, expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(unique_key_exprs.assign(tmp_exprs))) {
    LOG_WARN("failed to remove generated column exprs", K(ret));
  }
  return ret;
}

int ObDmlCgService::get_table_unique_key_exprs(ObLogDelUpd &op,
                                               const IndexDMLInfo &index_dml_info,
                                               ObIArray<ObRawExpr*> &unique_key_exprs)
{
  int ret = OB_SUCCESS;
  bool is_heap_table = false;
  if (OB_FAIL(check_is_heap_table(op, index_dml_info.ref_table_id_, is_heap_table))) {
    LOG_WARN("check is heap table failed", K(ret));
  } else if (OB_FAIL(index_dml_info.get_rowkey_exprs(unique_key_exprs))) {
    LOG_WARN("get table rowkey failed", K(ret), K(index_dml_info));
  } else if (is_heap_table) {
    if (OB_FAIL(get_heap_table_part_exprs(op, index_dml_info, unique_key_exprs))) {
      LOG_WARN("get heap table part exprs failed", K(ret), K(index_dml_info));
    } else if (OB_FAIL(adjust_unique_key_exprs(unique_key_exprs))){
      LOG_WARN("fail to adjust unique key exprs", K(ret));
    }
  }

  // The reason why batch optimization adds stmt_id to unique_key is
  // For multi_update/multi_delete statements that need to be deduplicated,
  // if different stmt_id statements exist to process duplicate rows,
  // Will be regarded as duplicate rows by deduplication logic, adding stmt_id to unique_key can avoid this problem
  if (OB_SUCC(ret) && op.get_plan()->get_optimizer_context().is_batched_multi_stmt()) {
    ObRawExpr *stmt_id_expr = nullptr;
    if (op.get_stmt_id_expr() == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt_id_expr is nullptr", K(ret));
    } else if (FALSE_IT(stmt_id_expr = const_cast<ObRawExpr *>(op.get_stmt_id_expr()))) {
      // do nothing
    } else if (OB_FAIL(unique_key_exprs.push_back(stmt_id_expr))) {
      LOG_WARN("fail to push_back stmt_id_expr", K(ret), KPC(stmt_id_expr));
    }
  }
  return ret;
}

// This function is only used for insert_up qualified replace_into
// When generating an insert to generate a primary key conflict,
//    the conflicting column information needs to be returned
// For a partitioned table without primary key, return hidden primary key + partition key
// The partition key is a generated column, and there is no need to replace
//    it with a real generated column calculation expression here, for the following reasons:
// 1. The generated columns on the main table are not used as primary keys,
//      and the generated columns on the index table are actually stored, and can be read directly
// 2. For non-primary key partition tables (generated columns are used as partition keys),
//      primary table writing will not cause primary key conflicts, unless it is a bug
int ObDmlCgService::table_unique_key_for_conflict_checker(ObLogDelUpd &op,
                                                          const IndexDMLInfo &index_dml_info,
                                                          ObIArray<ObRawExpr*> &rowkey_exprs)
{
  int ret = OB_SUCCESS;
  bool is_heap_table = false;
  if (OB_FAIL(check_is_heap_table(op, index_dml_info.ref_table_id_, is_heap_table))) {
    LOG_WARN("check is heap table failed", K(ret));
  } else if (OB_FAIL(index_dml_info.get_rowkey_exprs(rowkey_exprs))) {
    LOG_WARN("get table rowkey failed", K(ret), K(index_dml_info));
  } else if (is_heap_table) {
    if (OB_FAIL(get_heap_table_part_exprs(op, index_dml_info, rowkey_exprs))) {
      LOG_WARN("get heap table part exprs failed", K(ret), K(index_dml_info));
    }
  }
  return ret;
}

int ObDmlCgService::get_heap_table_part_exprs(const ObLogicalOperator &op,
                                              const IndexDMLInfo &index_dml_info,
                                              ObIArray<ObRawExpr*> &part_key_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> part_key_ids;
  const ObLogPlan *log_plan = op.get_plan();
  ObSchemaGetterGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  const ObDelUpdStmt *dml_stmt = NULL;
  if (OB_ISNULL(log_plan) ||
      OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(MTL_ID(), index_dml_info.ref_table_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(index_dml_info.ref_table_id_), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_schema));
  } else if (!table_schema->is_partitioned_table()) {
    // do nothing
  } else if (table_schema->get_partition_key_info().get_size() > 0 &&
             OB_FAIL(table_schema->get_partition_key_info().get_column_ids(part_key_ids))) {
    LOG_WARN("failed to get column ids", K(ret));
  } else if (table_schema->get_subpartition_key_info().get_size() > 0 &&
             OB_FAIL(table_schema->get_subpartition_key_info().get_column_ids(part_key_ids))) {
    LOG_WARN("failed to get column ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_key_ids.count(); ++i) {
    bool is_founded = false;
    uint64_t part_col_id = part_key_ids.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && !is_founded && j < index_dml_info.column_exprs_.count(); ++j) {
      ObColumnRefRawExpr *col = index_dml_info.column_exprs_.at(j);
      uint64_t base_cid = OB_INVALID_ID;
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is null", K(ret));
      } else if (OB_FAIL(get_column_ref_base_cid(op, col, base_cid))) {
        LOG_WARN("fail to get column base id", K(ret), KPC(col));
      } else if (part_col_id != base_cid) {
        // not match
      } else if (OB_FAIL(part_key_exprs.push_back(col))) {
        LOG_WARN("fail to push column expr", K(ret), KPC(col));
      } else {
        is_founded = true;
      }
    }
  }
  return ret;
}

int ObDmlCgService::convert_data_table_rowkey_info(ObLogDelUpd &op,
                                                   const IndexDMLInfo *primary_dml_info,
                                                   ObInsCtDef &ins_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASInsCtDef &das_ins_ctdef = ins_ctdef.das_ctdef_;
  ObSEArray<uint64_t, 8> rowkey_column_ids;
  ObSEArray<ObRawExpr *, 8> rowkey_exprs;
  ObSEArray<ObObjMeta, 8> rowkey_column_types;
  // rowkey_exprs的类型一定是column_ref表达式
  if (OB_ISNULL(primary_dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("primary_index_dml_info is null", K(ret));
  } else if (OB_FAIL(table_unique_key_for_conflict_checker(op, *primary_dml_info, rowkey_exprs))) {
    LOG_WARN("get table unique key exprs failed", K(ret), KPC(primary_dml_info));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_exprs.count(); ++i) {
    ObRawExpr *expr = rowkey_exprs.at(i);
    if (!expr->is_column_ref_expr()) {
      // do nothing.
      // For a 4.x partition table without a primary key, there is no redundant partition key in the primary key,
      // so its unique_key is a hidden auto-increment column + partition key. For replace and insert_up scenarios,
      // here will be no primary key conflicts when writing the primary table , so the main table does not need to bring back unique_key
      // (self-increment column + partition construction)
    } else {
      ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(expr);
      uint64_t base_cid = OB_INVALID_ID;
      if (OB_FAIL(get_column_ref_base_cid(op, col_expr, base_cid))) {
        LOG_WARN("get column base cid failed", K(ret), KPC(col_expr));
      } else if (OB_FAIL(rowkey_column_ids.push_back(base_cid))) {
        LOG_WARN("push base cid failed", K(ret), K(base_cid));
      } else if (OB_FAIL(rowkey_column_types.push_back(col_expr->get_result_type()))) {
        LOG_WARN("push column type failed", K(ret), KPC(col_expr));
      }
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(das_ins_ctdef.table_rowkey_cids_.init(rowkey_column_ids.count()))) {
    LOG_WARN("fail to generate rt exprs", K(ret), K(rowkey_exprs));
  } else if (OB_FAIL(append(das_ins_ctdef.table_rowkey_cids_, rowkey_column_ids))) {
    LOG_WARN("append table rowkey column id failed", K(ret), K(rowkey_column_ids));
  } else if (OB_FAIL(das_ins_ctdef.table_rowkey_types_.init(rowkey_column_types.count()))) {
    LOG_WARN("init table_rowkey_types failed", K(ret), K(rowkey_column_types.count()));
  } else if (OB_FAIL(append(das_ins_ctdef.table_rowkey_types_, rowkey_column_types))) {
    LOG_WARN("append table rowkey column type failed", K(ret), K(rowkey_column_types));
  }
  return ret;
}

int ObDmlCgService::generate_replace_ctdef(ObLogInsert &op,
                                           const IndexDMLInfo &ins_index_dml_info,
                                           const IndexDMLInfo &del_index_dml_info,
                                           ObReplaceCtDef *&replace_ctdef)
{
  int ret = OB_SUCCESS;
  ObInsCtDef *ins_ctdef = NULL;
  ObDelCtDef *del_ctdef = NULL;
  ObDMLCtDefAllocator<ObReplaceCtDef> replace_ctdef_allocator(cg_.phy_plan_->get_allocator());

  if (OB_ISNULL(replace_ctdef = replace_ctdef_allocator.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate replace ctdef failed", K(ret));
  } else if (OB_FAIL(generate_insert_ctdef(op, ins_index_dml_info, ins_ctdef))) {
    LOG_WARN("fail to generate insert ctdef", K(ret), "ins_index_dml_info", ins_index_dml_info);
  } else if (OB_FAIL(generate_delete_ctdef(op, del_index_dml_info, del_ctdef))) {
    LOG_WARN("fail to generate delete ctdef", K(ret), "del_index_dml_info", del_index_dml_info);
  } else {
    replace_ctdef->ins_ctdef_ = ins_ctdef;
    replace_ctdef->del_ctdef_ = del_ctdef;
  }
  return ret;
}

int ObDmlCgService::generate_insert_up_ctdef(ObLogInsert &op,
                                             const IndexDMLInfo &ins_index_dml_info,
                                             const IndexDMLInfo &upd_index_dml_info,
                                             ObInsertUpCtDef *&insert_up_ctdef)
{
  int ret = OB_SUCCESS;
  ObInsCtDef *ins_ctdef = NULL;
  ObUpdCtDef *upd_ctdef = NULL;
  uint64_t index_tid = ins_index_dml_info.ref_table_id_;
  ObDMLCtDefAllocator<ObInsertUpCtDef> insert_up_allocator(cg_.phy_plan_->get_allocator());
  if (OB_ISNULL(insert_up_ctdef = insert_up_allocator.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate insert on duplicate key ctdef failed", K(ret));
  } else if (OB_FAIL(generate_insert_ctdef(op, ins_index_dml_info, ins_ctdef))) {
    LOG_WARN("fail to generate insert ctdef", K(ret), "ins_index_dml_info", ins_index_dml_info);
  } else if (OB_FAIL(generate_update_ctdef(op, upd_index_dml_info, upd_ctdef))) {
    LOG_WARN("fail to generate update ctdef", K(ret), "upd_index_dml_info", upd_index_dml_info);
  } else {
    insert_up_ctdef->ins_ctdef_ = ins_ctdef;
    insert_up_ctdef->upd_ctdef_ = upd_ctdef;
  }

  return ret;
}

int ObDmlCgService::generate_conflict_checker_ctdef(ObLogInsert &op,
                                                    const IndexDMLInfo &index_dml_info,
                                                    ObConflictCheckerCtdef &conflict_checker_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> rowkey_exprs;
  bool is_heap_table = false;
  // When the partition key is a virtual generated column,
  // the table with the primary key needs to be replaced,
  // and the table without the primary key does not need to be replaced
  if (OB_FAIL(table_unique_key_for_conflict_checker(op, index_dml_info, rowkey_exprs))) {
    LOG_WARN("get table unique key exprs failed", K(ret), K(index_dml_info));
  } else if (OB_FAIL(check_is_heap_table(op, index_dml_info.ref_table_id_, is_heap_table))) {
    LOG_WARN("check is heap table failed", K(ret));
  } else if (!is_heap_table && OB_FAIL(adjust_unique_key_exprs(rowkey_exprs))) {
    LOG_WARN("fail to replace generated column exprs", K(ret), K(rowkey_exprs));
  } else if (OB_FAIL(cg_.generate_rt_exprs(rowkey_exprs, conflict_checker_ctdef.data_table_rowkey_expr_))) {
    LOG_WARN("fail to generate data_table rowkey_expr", K(ret), K(rowkey_exprs));
  } else if (OB_FAIL(generate_scan_ctdef(op, index_dml_info, conflict_checker_ctdef.das_scan_ctdef_))) {
    LOG_WARN("fail to generate das_scan_ctdef", K(ret));
  } else if (OB_FAIL(generate_constraint_infos(op,
                                               index_dml_info,
                                               conflict_checker_ctdef.cst_ctdefs_))) {
    LOG_WARN("fail to generate constraint_infos", K(ret));
  }  else if (OB_FAIL(cg_.generate_rt_exprs(index_dml_info.column_old_values_exprs_,
                                           conflict_checker_ctdef.table_column_exprs_))) {
    LOG_WARN("fail to generate table columns rt exprs ", K(ret));
  } else {
    conflict_checker_ctdef.use_dist_das_ = op.is_multi_part_dml();
    conflict_checker_ctdef.rowkey_count_ = index_dml_info.rowkey_cnt_;
  }
  // 生成回表的partition id 计算表达式，使用index_del_info的old_part_id_expr_
  if (OB_SUCC(ret) && op.is_multi_part_dml()) {
    ObRawExpr *part_id_expr_for_lookup = NULL;
    ObExpr *rt_part_id_expr = NULL;
    ObSEArray<ObRawExpr *, 4> constraint_dep_exprs;
    ObSEArray<ObRawExpr *, 4> constraint_raw_exprs;
    if (OB_ISNULL(part_id_expr_for_lookup = index_dml_info.lookup_part_id_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_id_expr for lookup is null", K(ret), K(index_dml_info));
    } else if (OB_FAIL(cg_.generate_calc_part_id_expr(*part_id_expr_for_lookup, nullptr, rt_part_id_expr))) {
      LOG_WARN("generate rt part_id_expr failed", K(ret), KPC(part_id_expr_for_lookup));
    } else if (OB_ISNULL(rt_part_id_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rt part_id_expr for lookup is null", K(ret));
    } else if (OB_FAIL(constraint_raw_exprs.push_back(part_id_expr_for_lookup))) {
      LOG_WARN("fail to push part_id_expr to constraint_raw_exprs", K(ret));
    } else if (OB_FAIL(cg_.generate_calc_exprs(constraint_dep_exprs,
                                               constraint_raw_exprs,
                                               conflict_checker_ctdef.part_id_dep_exprs_,
                                               op.get_type(),
                                               false))) {
      LOG_WARN("fail to generate part_id_expr depend calc_expr", K(constraint_dep_exprs),
          K(constraint_raw_exprs), K(conflict_checker_ctdef.part_id_dep_exprs_));
    } else {
      conflict_checker_ctdef.calc_part_id_expr_ = rt_part_id_expr;
    }
  }
  LOG_TRACE("print conflict checker", K(conflict_checker_ctdef));
  return ret;
}

int ObDmlCgService::generate_constraint_infos(ObLogInsert &op,
                                              const IndexDMLInfo &index_dml_info,
                                              ObRowkeyCstCtdefArray &cst_ctdefs)
{
  int ret = OB_SUCCESS;
  ObDMLCtDefAllocator<ObRowkeyCstCtdef> cst_ctdef_allocator(cg_.phy_plan_->get_allocator());
  const ObIArray<ObUniqueConstraintInfo> *log_constraint_infos = op.get_constraint_infos();
  if (OB_ISNULL(log_constraint_infos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log_constraint_info is null", K(ret));
  } else if (OB_FAIL(cst_ctdefs.init(log_constraint_infos->count()))) {
    LOG_WARN("allocate conflict checker spec array failed", K(ret), K(log_constraint_infos->count()));
  }
  for (int i = 0; OB_SUCC(ret) && i < log_constraint_infos->count(); i++) {
    ObRowkeyCstCtdef *rowkey_cst_ctdef = NULL;
    ObSEArray<ObRawExpr *, 4> constraint_dep_exprs;
    ObSEArray<ObRawExpr *, 8> constraint_raw_exprs;
    const ObIArray<ObColumnRefRawExpr*> &constraint_columns =
                                               log_constraint_infos->at(i).constraint_columns_;
    if (OB_ISNULL(rowkey_cst_ctdef = cst_ctdef_allocator.alloc())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey_cst_ctdef is null", K(ret));
    } else if (OB_FAIL(ob_write_string(cg_.phy_plan_->get_allocator(),
                                       log_constraint_infos->at(i).constraint_name_,
                                       rowkey_cst_ctdef->constraint_name_))) {
      LOG_WARN("fail to write string", K(ret), K(i),
               "name", log_constraint_infos->at(i).constraint_name_);
    } else if (OB_FAIL(rowkey_cst_ctdef->rowkey_expr_.init(constraint_columns.count()))) {
      LOG_WARN("init rowkey failed", K(ret), K(constraint_columns.count()));
    } else if (OB_FAIL(rowkey_cst_ctdef->rowkey_accuracys_.init(constraint_columns.count()))) {
      LOG_WARN("init rowkey accuracy failed", K(ret));
    }

    for (int64_t j = 0; OB_SUCC(ret) && j < constraint_columns.count(); ++j) {
      ObExpr *expr = NULL;
      ObColumnRefRawExpr *col_expr = constraint_columns.at(j);
      if (OB_ISNULL(col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_expr is null", K(ret));
      } else if (is_shadow_column(col_expr->get_column_id())
          || col_expr->is_virtual_generated_column()) {
        LOG_DEBUG("constraint exprs", K(is_shadow_column(col_expr->get_column_id())),
                  K(col_expr->is_virtual_generated_column()));
        // for shadow_pk
        ObRawExpr *spk_expr = col_expr->get_dependant_expr();
        if (OB_ISNULL(spk_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("col_expr is null", K(ret));
        } else if (OB_FAIL(cg_.generate_rt_expr(*spk_expr, expr))) {
          LOG_WARN("generate rt expr failed", K(ret), KPC(spk_expr));
        } else if (OB_FAIL(constraint_raw_exprs.push_back(spk_expr))) {
          LOG_WARN("push_back rt expr failed", K(ret), KPC(spk_expr));
        }
      } else {
        if (OB_FAIL(cg_.generate_rt_expr(*col_expr, expr))) {
          LOG_WARN("generate rt expr failed", K(ret), KPC(col_expr));
        } else if (OB_FAIL(constraint_raw_exprs.push_back(col_expr))) {
          LOG_WARN("push_back rt expr failed", K(ret), KPC(col_expr));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(rowkey_cst_ctdef->rowkey_expr_.push_back(expr))) {
          LOG_WARN("fail to push_back expr", K(ret));
        } else if (OB_FAIL(rowkey_cst_ctdef->rowkey_accuracys_.push_back(col_expr->get_accuracy()))) {
          LOG_WARN("fail to store rowkey accuracy", K(ret));
        }
      }
    } // end constraint_columns

    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.column_exprs_.count(); ++i) {
      ObRawExpr *expr = index_dml_info.column_exprs_.at(i);
      if (OB_FAIL(constraint_dep_exprs.push_back(expr))) {
        LOG_WARN("fail to push_back", K(ret), KPC(expr));
      }
    }
    //这里推导constraint_info->rowkey_expr_依赖的calc exprs
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cg_.generate_calc_exprs(constraint_dep_exprs,
                                          constraint_raw_exprs,
                                          rowkey_cst_ctdef->calc_exprs_,
                                          op.get_type(),
                                          false))) {
        LOG_WARN("fail to generate calc depend expr", K(ret),
                 K(constraint_dep_exprs), K(constraint_raw_exprs));
      } else if (OB_FAIL(cst_ctdefs.push_back(rowkey_cst_ctdef))) {
        LOG_WARN("fail to generate calc depend expr", K(ret), KPC(rowkey_cst_ctdef));
      }
    }
  } // end log_constraint_infos
  return ret;
}

int ObDmlCgService::generate_access_exprs(const common::ObIArray<ObColumnRefRawExpr*> &columns,
                               common::ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    ObRawExpr *expr = columns.at(i);
    if (expr->is_column_ref_expr() &&
      static_cast<ObColumnRefRawExpr *>(expr)->is_virtual_generated_column()) {
      // do nothing.
    } else {
      if (OB_FAIL(add_var_to_array_no_dup(access_exprs, expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDmlCgService::generate_scan_ctdef(ObLogInsert &op,
                                        const IndexDMLInfo &index_dml_info,
                                        ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> access_exprs;
  ObSEArray<ObRawExpr*, 16> dep_exprs;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  uint64_t ref_table_id = index_dml_info.ref_table_id_;
  // 主表的index_tid_和ref_table_id_都是一样的
  scan_ctdef.ref_table_id_ = ref_table_id;
  const uint64_t tenant_id = MTL_ID();
  if (OB_ISNULL(op.get_plan()) ||
      OB_ISNULL(schema_guard = op.get_plan()->get_optimizer_context().get_sql_schema_guard()) ||
      OB_ISNULL(schema_guard->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected null", K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_FAIL(schema_guard->get_schema_guard()->get_schema_version(
      TABLE_SCHEMA, tenant_id, ref_table_id, scan_ctdef.schema_version_))) {
    LOG_WARN("fail to get schema version", K(ret), K(tenant_id), K(ref_table_id));
  } else if (OB_FAIL(generate_access_exprs(index_dml_info.column_exprs_, access_exprs))) {
    LOG_WARN("fail to generate access exprs ", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(access_exprs,
                                           scan_ctdef.pd_expr_spec_.access_exprs_))) {
    LOG_WARN("fail to generate rt exprs ", K(ret));
  } else if (OB_FAIL(scan_ctdef.access_column_ids_.init(index_dml_info.column_exprs_.count()))) {
    LOG_WARN("fail to init output_column_ids_ ", K(ret));
  } else {
    ARRAY_FOREACH(index_dml_info.column_exprs_, i) {
      ObColumnRefRawExpr *item = index_dml_info.column_exprs_.at(i);
      uint64_t base_cid = OB_INVALID_ID;
      if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column item", K(i), K(item));
      } else if (item->is_virtual_generated_column() && !item->is_xml_column()) {
        // do nothing.
      } else if (OB_FAIL(get_column_ref_base_cid(op, item, base_cid))) {
        LOG_WARN("get base column id failed", K(ret), K(item));
      } else if (OB_FAIL(scan_ctdef.access_column_ids_.push_back(base_cid))) {
        LOG_WARN("fail to add column id", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    scan_ctdef.table_param_.get_enable_lob_locator_v2()
        = (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0);
    if (OB_FAIL(scan_ctdef.table_param_.convert(*table_schema, scan_ctdef.access_column_ids_))) {
      LOG_WARN("convert table param failed", K(ret));
    } else if (OB_FAIL(cg_.generate_calc_exprs(dep_exprs,
                                               index_dml_info.column_old_values_exprs_,
                                               scan_ctdef.pd_expr_spec_.calc_exprs_,
                                               op.get_type(),
                                               false))) {
      LOG_WARN("generate calc exprs failed", K(ret));
    } else if (OB_FAIL(cg_.tsc_cg_service_.generate_das_result_output(scan_ctdef.access_column_ids_,
                                                                      scan_ctdef,
                                                                      nullptr))) {
      LOG_WARN("generate das result output failed", K(ret));
    }
  }

  return ret;
}

int ObDmlCgService::generate_dml_column_ids(const ObLogicalOperator &op,
                                            const ObIArray<ObColumnRefRawExpr*> &columns_exprs,
                                            ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  column_ids.reset();
  if (!columns_exprs.empty()) {
    if (OB_FAIL(column_ids.reserve(columns_exprs.count()))) {
      LOG_WARN("init column ids array failed", K(ret), K(columns_exprs.count()));
    } else {
      ARRAY_FOREACH(columns_exprs, i) {
        ObColumnRefRawExpr *item = columns_exprs.at(i);
        uint64_t base_cid = OB_INVALID_ID;
        if (OB_ISNULL(item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column item", K(i), K(item));
        } else if (OB_FAIL(get_column_ref_base_cid(op, item, base_cid))) {
          LOG_WARN("get base column id failed", K(ret), K(item));
        } else if (OB_FAIL(column_ids.push_back(base_cid))) {
          LOG_WARN("add column id failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDmlCgService::generate_updated_column_ids(const ObLogDelUpd &log_op,
                                                const ObAssignments &assigns,
                                                const ObIArray<uint64_t> &column_ids,
                                                ObIArray<uint64_t> &updated_column_ids)
{
  int ret = OB_SUCCESS;
  updated_column_ids.reset();
  const ObDMLStmt *stmt = log_op.get_stmt();
  if (!assigns.empty()) {
    if (OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null stmt", K(ret));
    } else if (OB_FAIL(updated_column_ids.reserve(assigns.count()))) {
      LOG_WARN("init updated column ids array failed", K(ret), K(assigns.count()));
    } else {
      ARRAY_FOREACH(assigns, i) {
        ObColumnRefRawExpr *column_expr = assigns.at(i).column_expr_;
        ColumnItem *column_item = nullptr;
        if (OB_ISNULL(column_expr) ||
            OB_ISNULL(column_item = stmt->get_column_item_by_id(column_expr->get_table_id(),
                                                                column_expr->get_column_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(column_expr), K(column_item));
        } else if (!has_exist_in_array(column_ids, column_item->base_cid_)) {
          //not found in column ids, ignore it
        } else if (OB_FAIL(updated_column_ids.push_back(column_item->base_cid_))) {
          LOG_WARN("add updated column id failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDmlCgService::convert_dml_column_info(ObTableID index_tid,
                                            bool only_rowkey,
                                            ObDASDMLBaseCtDef &das_dml_info)
{
  int ret = OB_SUCCESS;
  das_dml_info.column_ids_.reset();
  das_dml_info.column_types_.reset();
  const ObTableSchema *index_schema = nullptr;
  int64_t column_count = 0;
  const uint64_t tenant_id = cg_.opt_ctx_->get_session_info()->get_effective_tenant_id();
  if (OB_FAIL(cg_.opt_ctx_->get_schema_guard()->get_table_schema(
      tenant_id, index_tid, index_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(index_tid));
  } else {
    column_count = only_rowkey ? index_schema->get_rowkey_info().get_size()
                               : index_schema->get_column_count();
    das_dml_info.column_ids_.set_capacity(column_count);
    das_dml_info.column_types_.set_capacity(column_count);
    das_dml_info.column_accuracys_.set_capacity(column_count);
    das_dml_info.rowkey_cnt_ = index_schema->get_rowkey_info().get_size();
    das_dml_info.spk_cnt_ = index_schema->get_shadow_rowkey_info().get_size();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_schema->get_rowkey_info().get_size(); ++i) {
    const ObRowkeyInfo &rowkey_info = index_schema->get_rowkey_info();
    const ObRowkeyColumn *rowkey_column = rowkey_info.get_column(i);
    const ObColumnSchemaV2 *column = index_schema->get_column_schema(rowkey_column->column_id_);
    ObObjMeta column_type;
    column_type = column->get_meta_type();
    column_type.set_scale(column->get_accuracy().get_scale());
    if (is_lob_storage(column_type.get_type())) {
      if (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0) {
        column_type.set_has_lob_header();
      }
    }
    if (OB_FAIL(das_dml_info.column_ids_.push_back(column->get_column_id()))) {
      LOG_WARN("store column id failed", K(ret));
    } else if (OB_FAIL(das_dml_info.column_types_.push_back(column_type))) {
      LOG_WARN("store column meta type failed", K(ret));
    } else if (OB_FAIL(das_dml_info.column_accuracys_.push_back(column->get_accuracy()))) {
      LOG_WARN("store column accuracy failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && !only_rowkey) {
    ObTableSchema::const_column_iterator iter = index_schema->column_begin();
    for (; OB_SUCC(ret) && iter != index_schema->column_end(); ++iter) {
      const ObColumnSchemaV2 *column = *iter;
      ObObjMeta column_type;
      if (!column->is_rowkey_column() && !column->is_virtual_generated_column()) {
        //skip virtual generated column or rowkey
        column_type = column->get_meta_type();
        column_type.set_scale(column->get_accuracy().get_scale());
        if (is_lob_storage(column_type.get_type())) {
          if (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0) {
            column_type.set_has_lob_header();
          }
        }
        if (OB_FAIL(das_dml_info.column_ids_.push_back(column->get_column_id()))) {
          LOG_WARN("store column id failed", K(ret));
        } else if (OB_FAIL(das_dml_info.column_types_.push_back(column_type))) {
          LOG_WARN("store column meta type failed", K(ret));
        } else if (OB_FAIL(das_dml_info.column_accuracys_.push_back(column->get_accuracy()))) {
          LOG_WARN("store column accuracy failed", K(ret));
        }
      }
    }
  }
  return ret;
}

template<typename ExprType>
int ObDmlCgService::add_geo_col_projector(const ObIArray<ExprType*> &cur_row,
                                          const ObIArray<ObRawExpr*> &full_row,
                                          const ObIArray<uint64_t> &dml_column_ids,
                                          uint32_t proj_idx,
                                          ObDASDMLBaseCtDef &das_ctdef,
                                          IntFixedArray &row_projector)
{
  int ret = OB_SUCCESS;
  int64_t column_idx = OB_INVALID_INDEX;
  int64_t projector_idx = OB_INVALID_INDEX;
  uint64_t geo_cid = das_ctdef.table_param_.get_data_table().get_spatial_geo_col_id();
  if (has_exist_in_array(dml_column_ids, geo_cid, &column_idx)) {
    ObRawExpr *column_expr = cur_row.at(column_idx);
    if (!has_exist_in_array(full_row, column_expr, &projector_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row column not found in full row columns", K(ret),
                K(column_idx), KPC(cur_row.at(column_idx)));
    } else {
      row_projector.at(proj_idx) = projector_idx;
    }
  }
  return ret;
}

template<typename OldExprType, typename NewExprType>
int ObDmlCgService::generate_das_projector(const ObIArray<uint64_t> &dml_column_ids,
                                           const ObIArray<uint64_t> &storage_column_ids,
                                           const ObIArray<OldExprType*> &old_row,
                                           const ObIArray<NewExprType*> &new_row,
                                           const ObIArray<ObRawExpr*> &full_row,
                                           ObDASDMLBaseCtDef &das_ctdef)
{
  int ret = OB_SUCCESS;
  IntFixedArray &old_row_projector = das_ctdef.old_row_projector_;
  IntFixedArray &new_row_projector = das_ctdef.new_row_projector_;
  bool is_spatial_index = das_ctdef.table_param_.get_data_table().is_spatial_index()
                          && das_ctdef.op_type_ == DAS_OP_TABLE_UPDATE;
  uint8_t extra_geo = is_spatial_index ? 1 : 0;
  //generate old row projector
  if (!old_row.empty()) {
    //generate storage row projector
    if (OB_FAIL(old_row_projector.prepare_allocate(storage_column_ids.count() + extra_geo))) {
      LOG_WARN("init row projector array failed", K(ret), K(storage_column_ids.count()), K(extra_geo));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < storage_column_ids.count(); ++i) {
      uint64_t storage_cid = storage_column_ids.at(i);
      uint64_t ref_cid = is_shadow_column(storage_cid) ?
                         storage_cid - OB_MIN_SHADOW_COLUMN_ID :
                         storage_cid;
      int64_t column_idx = OB_INVALID_INDEX;
      int64_t projector_idx = OB_INVALID_INDEX;
      old_row_projector.at(i) = OB_INVALID_INDEX;
      if (has_exist_in_array(dml_column_ids, ref_cid, &column_idx)) {
        ObRawExpr *column_expr = old_row.at(column_idx);
        if (!has_exist_in_array(full_row, column_expr, &projector_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row column not found in full row columns", K(ret),
                   K(column_idx), KPC(old_row.at(column_idx)));
        } else {
          old_row_projector.at(i) = projector_idx;
        }
      }
    }
    if (OB_SUCC(ret) && is_spatial_index
        && OB_FAIL(add_geo_col_projector(old_row, full_row, dml_column_ids, storage_column_ids.count(),
                                         das_ctdef, old_row_projector))) {
        LOG_WARN("add geo column projector failed", K(ret));
    }
  }
  //generate new row projector
  if (!new_row.empty()) {
    //generate storage row projector
    if (OB_FAIL(new_row_projector.prepare_allocate(storage_column_ids.count() + extra_geo))) {
      LOG_WARN("init row projector array failed", K(ret), K(storage_column_ids.count()), K(extra_geo));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < storage_column_ids.count(); ++i) {
      uint64_t storage_cid = storage_column_ids.at(i);
      uint64_t ref_cid = is_shadow_column(storage_cid) ?
                         storage_cid - OB_MIN_SHADOW_COLUMN_ID :
                         storage_cid;
      int64_t column_idx = OB_INVALID_INDEX;
      int64_t projector_idx = OB_INVALID_INDEX;
      new_row_projector.at(i) = OB_INVALID_INDEX;
      if (has_exist_in_array(dml_column_ids, ref_cid, &column_idx)) {
        ObRawExpr *column_expr = new_row.at(column_idx);
        if (!has_exist_in_array(full_row, column_expr, &projector_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row column not found in full row columns", K(ret),
                   K(column_idx), KPC(new_row.at(column_idx)));
        } else {
          new_row_projector.at(i) = projector_idx;
        }
      }
    }
    if (OB_SUCC(ret) && is_spatial_index
        && OB_FAIL(add_geo_col_projector(new_row, full_row, dml_column_ids, storage_column_ids.count(),
                                         das_ctdef, new_row_projector))) {
        LOG_WARN("add geo column projector failed", K(ret));
    }
  }
  return ret;
}

int ObDmlCgService::get_column_ref_base_cid(
    const ObLogicalOperator &op,
    const ObColumnRefRawExpr *col,
    uint64_t &base_cid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op.get_stmt()) || OB_ISNULL(col)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    const ColumnItem *item = op.get_stmt()->get_column_item_by_id(
        col->get_table_id(), col->get_column_id());
    if (OB_ISNULL(item)) {
      // No ColumnItem for generated columns, return col->column_id_ directly. e.g.:
      //   create table t1 (c1 int primary key, c2 int, c3 int, unique key uk_c1(c1))
      //   partition by hash(c1) partitions 2;
      // column shadow_pk_0: is generated column generated by c1.
      base_cid = col->get_column_id();
    } else {
      base_cid = item->base_cid_;
    }
  }
  return ret;
}

int ObDmlCgService::get_table_schema_version(const ObLogicalOperator &op,
                                             uint64_t table_id,
                                             int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = op.get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    bool found = false;
    const ObIArray<ObSchemaObjVersion> *dependency_table = stmt->get_global_dependency_table();
    CK(OB_NOT_NULL(dependency_table));
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < dependency_table->count(); ++i) {
      const ObSchemaObjVersion &schema_obj = dependency_table->at(i);
      if (schema_obj.object_type_ == DEPENDENCY_TABLE
          && schema_obj.object_id_ == table_id) {
        schema_version = schema_obj.version_;
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      //local index not exists in dependency table,
      //but local index table is attach with data table, so fetch local index version in schema guard
      ObSchemaGetterGuard *schema_guard = cg_.opt_ctx_->get_schema_guard();
      const uint64_t tenant_id = cg_.opt_ctx_->get_session_info()->get_effective_tenant_id();
      if (OB_FAIL(schema_guard->get_schema_version(TABLE_SCHEMA,
          tenant_id, table_id, schema_version))) {
        LOG_WARN("get table schema version failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDmlCgService::generate_das_dml_ctdef(ObLogDelUpd &op,
                                           ObTableID index_tid,
                                           const IndexDMLInfo &index_dml_info,
                                           ObDASDMLBaseCtDef &das_dml_ctdef)
{
  int ret = OB_SUCCESS;
  das_dml_ctdef.table_id_ = index_dml_info.loc_table_id_;
  das_dml_ctdef.index_tid_ = index_tid;
  das_dml_ctdef.is_ignore_ = op.is_ignore();
  das_dml_ctdef.is_batch_stmt_ = op.get_plan()->get_optimizer_context().is_batched_multi_stmt();
  ObSQLSessionInfo *session = nullptr;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  if (OB_FAIL(convert_dml_column_info(index_tid, false, das_dml_ctdef))) {
    LOG_WARN("add column ids to das_dml_ctdef failed", K(ret));
  } else if (OB_FAIL(get_table_schema_version(op, index_tid, das_dml_ctdef.schema_version_))) {
    LOG_WARN("get table schema version failed", K(ret), K(index_dml_info));
  } else if (!op.has_instead_of_trigger() && (OB_FAIL(convert_table_dml_param(op, das_dml_ctdef)))) {
    LOG_WARN("convert table dml param failed", K(ret));
  } else if (OB_ISNULL(op.get_plan())
      || OB_ISNULL(session = op.get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is invalid", K(op.get_plan()), K(session));
  } else if (OB_FAIL(session->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("get binlog row image failed", K(ret));
  } else {
    das_dml_ctdef.tz_info_ = *session->get_tz_info_wrap().get_time_zone_info();
    das_dml_ctdef.is_total_quantity_log_ = (ObBinlogRowImage::FULL == binlog_row_image);
  }
#ifdef OB_BUILD_TDE_SECURITY
  // generate encrypt_meta for table
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard *schema_guard = NULL;
    const share::schema::ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(schema_guard = op.get_plan()
                  ->get_optimizer_context().get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL schema guard", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(MTL_ID(), index_tid, table_schema))) {
      LOG_WARN("get schema fail", KR(ret), K(index_tid));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", KR(ret), K(index_tid));
    } else {
      ObArray<transaction::ObEncryptMetaCache> metas;
      if (OB_FAIL(init_encrypt_metas_(table_schema, schema_guard, metas))) {
        LOG_WARN("init table encrypt meta fail", KR(ret));
      } else if (metas.count() == 0)  {
        das_dml_ctdef.encrypt_meta_.reset();
      } else if (OB_FAIL(das_dml_ctdef.encrypt_meta_.assign(metas))) {
        LOG_WARN("assign encrypt meta fail", KR(ret), K(index_tid), K(metas));
      }
    }
  }
#endif
  return ret;
}

int ObDmlCgService::generate_das_ins_ctdef(ObLogDelUpd &op,
                                           ObTableID index_tid,
                                           const IndexDMLInfo &index_dml_info,
                                           ObDASInsCtDef &das_ins_ctdef,
                                           const ObIArray<ObRawExpr*> &new_row)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> dml_column_ids;
  ObArray<ObRawExpr*> empty_old_row;
  if (OB_FAIL(generate_das_dml_ctdef(op, index_tid, index_dml_info, das_ins_ctdef))) {
    LOG_WARN("generate das dml ctdef failed", K(ret));
  } else if (OB_FAIL(generate_dml_column_ids(op, index_dml_info.column_exprs_, dml_column_ids))) {
    LOG_WARN("generate dml column ids failed", K(ret));
  } else if (OB_FAIL(generate_das_projector(dml_column_ids,
                                            das_ins_ctdef.column_ids_,
                                            empty_old_row, new_row, new_row,
                                            das_ins_ctdef))) {
    LOG_WARN("add new row projector failed", K(ret), K(new_row));
  }
  return ret;
}

int ObDmlCgService::generate_related_ins_ctdef(ObLogDelUpd &op,
                                               const ObIArray<ObTableID> &related_tids,
                                               const IndexDMLInfo &index_dml_info,
                                               const ObIArray<ObRawExpr*> &new_row,
                                               DASInsCtDefArray &ins_ctdefs)
{
  int ret = OB_SUCCESS;
  //now to generate related local index insert ctdef
  for (int64_t i = 0; OB_SUCC(ret) && i < related_tids.count(); ++i) {
    ObDMLCtDefAllocator<ObDASInsCtDef> das_alloc(cg_.phy_plan_->get_allocator());
    ins_ctdefs.set_capacity(related_tids.count());
    ObDASInsCtDef *related_ctdef = nullptr;
    ObTableID related_tid = related_tids.at(i);
    if (OB_ISNULL(related_ctdef = das_alloc.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate insert related das ctdef failed", K(ret));
    } else if (OB_FAIL(generate_das_ins_ctdef(op, related_tid,
                                              index_dml_info,
                                              *related_ctdef,
                                              new_row))) {
      LOG_WARN("generate das ins ctdef failed", K(ret));
    } else if (OB_FAIL(ins_ctdefs.push_back(related_ctdef))) {
      LOG_WARN("store related ctdef failed", K(ret));
    }
  }
  return ret;
}

int ObDmlCgService::generate_das_del_ctdef(ObLogDelUpd &op,
                                           ObTableID index_tid,
                                           const IndexDMLInfo &index_dml_info,
                                           ObDASDelCtDef &das_del_ctdef,
                                           const common::ObIArray<ObRawExpr*> &old_row)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> dml_column_ids;
  ObArray<ObRawExpr*> empty_new_row;
  if (OB_FAIL(generate_das_dml_ctdef(op, index_tid, index_dml_info, das_del_ctdef))) {
    LOG_WARN("generate das dml ctdef failed", K(ret));
  } else if (OB_FAIL(generate_dml_column_ids(op, index_dml_info.column_exprs_, dml_column_ids))) {
    LOG_WARN("generate dml column ids failed", K(ret));
  } else if (OB_FAIL(generate_das_projector(dml_column_ids,
                                            das_del_ctdef.column_ids_,
                                            old_row, empty_new_row, old_row,
                                            das_del_ctdef))) {
    LOG_WARN("add old row projector failed", K(ret), K(old_row));
  }
  return ret;
}

int ObDmlCgService::generate_related_del_ctdef(ObLogDelUpd &op,
                                               const ObIArray<ObTableID> &related_tids,
                                               const IndexDMLInfo &index_dml_info,
                                               const ObIArray<ObRawExpr*> &old_row,
                                               DASDelCtDefArray &del_ctdefs)
{
  int ret = OB_SUCCESS;
  //now to generate related local index insert ctdef
  for (int64_t i = 0; OB_SUCC(ret) && i < related_tids.count(); ++i) {
    ObDMLCtDefAllocator<ObDASDelCtDef> das_alloc(cg_.phy_plan_->get_allocator());
    del_ctdefs.set_capacity(related_tids.count());
    ObDASDelCtDef *related_ctdef = nullptr;
    ObTableID related_tid = related_tids.at(i);
    if (OB_ISNULL(related_ctdef = das_alloc.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate insert related das ctdef failed", K(ret));
    } else if (OB_FAIL(generate_das_del_ctdef(op,
                                              related_tid,
                                              index_dml_info,
                                              *related_ctdef,
                                              old_row))) {
      LOG_WARN("generate das ins ctdef failed", K(ret));
    } else if (OB_FAIL(del_ctdefs.push_back(related_ctdef))) {
      LOG_WARN("store related ctdef failed", K(ret));
    }
  }
  return ret;
}

int ObDmlCgService::generate_das_upd_ctdef(ObLogDelUpd &op,
                                           ObTableID index_tid,
                                           const IndexDMLInfo &index_dml_info,
                                           ObDASUpdCtDef &das_upd_ctdef,
                                           const ObIArray<ObRawExpr*> &old_row,
                                           const ObIArray<ObRawExpr*> &new_row,
                                           const ObIArray<ObRawExpr*> &full_row)
{
  int ret = OB_SUCCESS;
  const ObAssignments &assigns = index_dml_info.assignments_;
  ObArray<uint64_t> dml_column_ids;
  if (OB_FAIL(generate_das_dml_ctdef(op, index_tid, index_dml_info, das_upd_ctdef))) {
    LOG_WARN("generate das dml base ctdef failed", K(ret), K(index_dml_info));
  } else if (OB_FAIL(generate_updated_column_ids(op, assigns, das_upd_ctdef.column_ids_,
                                                 das_upd_ctdef.updated_column_ids_))) {
    LOG_WARN("add updated column ids failed", K(ret), K(assigns));
  } else if (OB_FAIL(generate_dml_column_ids(op, index_dml_info.column_exprs_, dml_column_ids))) {
    LOG_WARN("generate dml column ids failed", K(ret));
  } else if (OB_FAIL(generate_das_projector(dml_column_ids,
                                            das_upd_ctdef.column_ids_,
                                            old_row, new_row, full_row,
                                            das_upd_ctdef))) {
    LOG_WARN("add old row projector failed", K(ret), K(old_row), K(full_row));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); ++i) {
    const ObColumnRefRawExpr *col = assigns.at(i).column_expr_;
    ObString column_name;
    OZ(ob_write_string(cg_.phy_plan_->get_allocator(), col->get_column_name(), column_name));
  }
  return ret;
}

int ObDmlCgService::generate_related_upd_ctdef(ObLogDelUpd &op,
                                               const ObIArray<ObTableID> &related_tids,
                                               const IndexDMLInfo &index_dml_info,
                                               const ObIArray<ObRawExpr*> &old_row,
                                               const ObIArray<ObRawExpr*> &new_row,
                                               const ObIArray<ObRawExpr*> &full_row,
                                               DASUpdCtDefArray &upd_ctdefs)
{
  int ret = OB_SUCCESS;
  //now to generate related local index insert ctdef
  for (int64_t i = 0; OB_SUCC(ret) && i < related_tids.count(); ++i) {
    ObDMLCtDefAllocator<ObDASUpdCtDef> das_alloc(cg_.phy_plan_->get_allocator());
    upd_ctdefs.set_capacity(related_tids.count());
    ObDASUpdCtDef *related_ctdef = nullptr;
    ObTableID related_tid = related_tids.at(i);
    if (OB_ISNULL(related_ctdef = das_alloc.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate insert related das ctdef failed", K(ret));
    } else if (OB_FAIL(generate_das_upd_ctdef(op,
                                              related_tid,
                                              index_dml_info,
                                              *related_ctdef,
                                              old_row,
                                              new_row,
                                              full_row))) {
      LOG_WARN("generate das ins ctdef failed", K(ret));
    } else if (related_ctdef->updated_column_ids_.empty()) {
      //ignore invalid update ctdef
    } else if (OB_FAIL(upd_ctdefs.push_back(related_ctdef))) {
      LOG_WARN("store related ctdef failed", K(ret));
    }
  }
  return ret;
}

int ObDmlCgService::generate_das_lock_ctdef(ObLogicalOperator &op,
                                            const IndexDMLInfo &index_dml_info,
                                            ObDASLockCtDef &das_lock_ctdef,
                                            const ObIArray<ObRawExpr*> &old_row)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> empty_new_row;
  ObArray<uint64_t> dml_column_ids;
  das_lock_ctdef.table_id_ = index_dml_info.loc_table_id_;
  das_lock_ctdef.index_tid_ = index_dml_info.ref_table_id_;
  ObSQLSessionInfo *session = nullptr;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  if (OB_FAIL(convert_dml_column_info(index_dml_info.ref_table_id_, true, das_lock_ctdef))) {
    LOG_WARN("add column ids to das_lock_ctdef failed", K(ret));
  } else if (OB_FAIL(get_table_schema_version(op,
                                              index_dml_info.ref_table_id_,
                                              das_lock_ctdef.schema_version_))) {
    LOG_WARN("get table schema version failed", K(ret), K(index_dml_info));
  } else if (OB_FAIL(convert_table_dml_param(op, das_lock_ctdef))) {
    LOG_WARN("convert table dml param failed", K(ret));
  } else if (OB_FAIL(generate_dml_column_ids(op, index_dml_info.column_exprs_, dml_column_ids))) {
    LOG_WARN("generate dml column ids failed", K(ret));
  } else if (OB_FAIL(generate_das_projector(dml_column_ids,
                                            das_lock_ctdef.column_ids_,
                                            old_row, empty_new_row, old_row,
                                            das_lock_ctdef))) {
    LOG_WARN("add old row projector failed", K(ret), K(old_row), K(old_row));
  } else if (OB_ISNULL(op.get_plan())
      || OB_ISNULL(session = op.get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is invalid", K(op.get_plan()), K(session));
  } else if (OB_FAIL(session->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("get binlog row image failed", K(ret));
  } else {
    das_lock_ctdef.tz_info_ = *session->get_tz_info_wrap().get_time_zone_info();
    das_lock_ctdef.is_total_quantity_log_ = (ObBinlogRowImage::FULL == binlog_row_image);
  }
  return ret;
}

int ObDmlCgService::convert_table_dml_param(ObLogicalOperator &op, ObDASDMLBaseCtDef &das_dml_ctdef)
{
  UNUSED(op);
  int ret = OB_SUCCESS;
  const uint64_t table_id = das_dml_ctdef.index_tid_;
  ObSqlSchemaGuard *schema_guard = NULL;
  if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table id ", K(ret), K(table_id));
  } else if (OB_ISNULL(schema_guard = cg_.opt_ctx_->get_sql_schema_guard()) ||
             OB_ISNULL(schema_guard->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL schema guard", K(ret));
  } else if (cg_.opt_ctx_->get_session_info()->get_ddl_info().is_ddl()) {
    //ddl operator does not need table dml param, do nothing
  } else if (OB_FAIL(fill_table_dml_param(schema_guard->get_schema_guard(), table_id, das_dml_ctdef))) {
    LOG_WARN("fail to fill table dml param", K(ret), K(table_id));
  }
  return ret;
}

int ObDmlCgService::fill_table_dml_param(share::schema::ObSchemaGetterGuard *guard,
                                         uint64_t table_id,
                                         ObDASDMLBaseCtDef &das_dml_ctdef)
{
  int ret = OB_SUCCESS;
  int64_t t_version = OB_INVALID_VERSION;
  const ObTableSchema *table_schema = NULL;
  uint64_t tenant_id = MTL_ID();
  if (OB_ISNULL(guard) || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(guard), K(table_id));
  } else if (OB_FAIL(guard->get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (OB_FAIL(guard->get_schema_version(tenant_id, t_version))) {
    LOG_WARN("get tenant schema version fail", K(ret), K(tenant_id));
  } else if (OB_FAIL(das_dml_ctdef.table_param_.convert(table_schema,
                                                        t_version,
                                                        das_dml_ctdef.column_ids_))) {
    LOG_WARN("fail to convert table param", K(ret), K(das_dml_ctdef));
  }
  return ret;
}

int ObDmlCgService::check_is_heap_table(ObLogicalOperator &op,
                                        uint64_t ref_table_id,
                                        bool &is_heap_table)
{
  int ret = OB_SUCCESS;
  ObLogPlan *log_plan = op.get_plan();
  ObSchemaGetterGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  const ObDelUpdStmt *dml_stmt = NULL;
  if (OB_ISNULL(log_plan) ||
      OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(MTL_ID(), ref_table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ref_table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_schema));
  } else if (!table_schema->is_heap_table()) {
    is_heap_table = false;
  } else {
    is_heap_table = true;
  }
  return ret;
}

int ObDmlCgService::generate_dml_base_ctdef(ObLogicalOperator &op,
                                            const IndexDMLInfo &index_dml_info,
                                            ObDMLBaseCtDef &dml_base_ctdef,
                                            ObIArray<ObRawExpr*> &old_row,
                                            ObIArray<ObRawExpr*> &new_row)
{
  int ret = OB_SUCCESS;
  dml_base_ctdef.is_primary_index_ = index_dml_info.is_primary_index_;
  dml_base_ctdef.column_ids_.set_capacity(index_dml_info.column_exprs_.count());
  if (OB_FAIL(generate_dml_column_ids(op, index_dml_info.column_exprs_, dml_base_ctdef.column_ids_))) {
    LOG_WARN("generate dml column ids failed", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(old_row, dml_base_ctdef.old_row_))) {
    LOG_WARN("generate old row exprs failed", K(ret));
  } else if (OB_FAIL(cg_.generate_rt_exprs(new_row, dml_base_ctdef.new_row_))) {
    LOG_WARN("generate new row exprs failed", K(ret));
  } else if (index_dml_info.is_primary_index_) {
    bool is_heap_table = false;
    if (OB_FAIL(check_is_heap_table(op, index_dml_info.ref_table_id_, is_heap_table))) {
      LOG_WARN("convert foreign keys failed", K(ret));
    } else {
      dml_base_ctdef.is_heap_table_ = is_heap_table;
    }
  }

  if (OB_SUCC(ret) &&
      op.is_dml_operator() &&
      OB_NOT_NULL(index_dml_info.trans_info_expr_)) {
      ObLogDelUpd &dml_op = static_cast<ObLogDelUpd&>(op);
      // Cg is only needed when the current trans_info_expr_ has a producer operator
    if (has_exist_in_array(dml_op.get_produced_trans_exprs(), index_dml_info.trans_info_expr_)) {
      if (cg_.generate_rt_expr(*index_dml_info.trans_info_expr_, dml_base_ctdef.trans_info_expr_)) {
        LOG_WARN("fail to cg trans_info expr", K(ret), KPC(index_dml_info.trans_info_expr_));
      }
    } else {
      LOG_TRACE("this trans_info_expr not produced", K(ret), K(index_dml_info));
    }
  }

  if (OB_SUCC(ret) &&
      log_op_def::LOG_INSERT == op.get_type()) {
    ObLogInsert &log_ins_op = static_cast<ObLogInsert &>(op);
    if (log_ins_op.get_insert_up()) {
      dml_base_ctdef.das_base_ctdef_.is_insert_up_ = true;
    }
  }
  return ret;
}

int ObDmlCgService::generate_dml_base_ctdef(ObLogDelUpd &op,
                                           const IndexDMLInfo &index_dml_info,
                                           ObDMLBaseCtDef &dml_base_ctdef,
                                           uint64_t dml_event,
                                           common::ObIArray<ObRawExpr*> &old_row,
                                           common::ObIArray<ObRawExpr*> &new_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_dml_base_ctdef(op, index_dml_info, dml_base_ctdef, old_row, new_row))) {
    LOG_WARN("generate dml column ids failed", K(ret));
  }
  // only primary key need to apply constraint, fk and trigger
  //generate check cst info
  if (OB_SUCC(ret) && index_dml_info.is_primary_index_) {
    if (OB_FAIL(convert_check_constraint(op, index_dml_info.ref_table_id_, dml_base_ctdef, index_dml_info))) {
      LOG_WARN("convert check constraint failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && index_dml_info.is_primary_index_) {
    //add foreign key
    if (OB_FAIL(convert_foreign_keys(op, index_dml_info, dml_base_ctdef))) {
      LOG_WARN("convert foreign keys failed", K(ret));
    }
  }
  // Only enable when pl_static_engine is enabled that mysqltest will enable
  if (OB_SUCC(ret) && index_dml_info.is_primary_index_) {
    //add trigger
    if (OB_FAIL(convert_triggers(op, index_dml_info, dml_base_ctdef, dml_event))) {
      LOG_WARN("convert foreign keys failed", K(ret));
    }
  }
  return ret;
}

int ObDmlCgService::convert_triggers(ObLogDelUpd &log_op,
                                     const IndexDMLInfo &dml_info,
                                     ObDMLBaseCtDef &dml_ctdef,
                                     uint64_t dml_event)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(convert_normal_triggers(log_op, dml_info, dml_ctdef, log_op.has_instead_of_trigger(), dml_event))) {
    LOG_WARN("failed to convert normal triggers", K(ret));
  }
  return ret;
}

int ObDmlCgService::add_trigger_arg(const ObTriggerInfo &trigger_info, ObDMLBaseCtDef &dml_ctdef)
{
  int ret = OB_SUCCESS;
  ObTriggerArg trigger_arg;
  trigger_arg.set_trigger_id(trigger_info.get_trigger_id());
  trigger_arg.set_trigger_events(trigger_info.get_trigger_events());
  trigger_arg.set_timing_points(trigger_info.get_timing_points());
  trigger_arg.set_analyze_flag(trigger_info.get_analyze_flag());
  if (OB_FAIL(dml_ctdef.trig_ctdef_.tg_args_.push_back(trigger_arg))) {
    LOG_WARN("failed to add trigger arg", K(ret));
  } else {
    dml_ctdef.trig_ctdef_.all_tm_points_.merge(trigger_arg.get_timing_points());
  }
  return ret;
}

int ObDmlCgService::convert_trigger_rowid(ObLogDelUpd &log_op,
                                          const IndexDMLInfo &dml_info,
                                          ObDMLBaseCtDef &dml_ctdef)
{
  int ret = OB_SUCCESS;
  bool is_merge_insert = false;
  bool is_merge_delete = false;
  if (log_op.get_type() == log_op_def::LOG_MERGE) {
    ObLogMerge &merge = static_cast<ObLogMerge &>(log_op);
    is_merge_insert = merge.is_insert_dml_info(&dml_info);
    is_merge_delete = merge.is_delete_dml_info(&dml_info);
  }
  if (log_op.get_type() != log_op_def::LOG_INSERT &&
      log_op.get_type() != log_op_def::LOG_INSERT_ALL &&
      !is_merge_insert) {
    if (OB_ISNULL(dml_info.old_rowid_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get old row rowid", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_expr(*dml_info.old_rowid_expr_,
                                            dml_ctdef.trig_ctdef_.rowid_old_expr_))) {
      LOG_WARN("failed to generate expr", K(ret));
    }
  }
  if (OB_SUCC(ret) &&
      log_op.get_type() != log_op_def::LOG_DELETE &&
      !is_merge_delete) {
    if (OB_ISNULL(dml_info.new_rowid_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get new row rowid", K(ret));
    } else if (OB_FAIL(cg_.generate_rt_expr(*dml_info.new_rowid_expr_,
                                            dml_ctdef.trig_ctdef_.rowid_new_expr_))) {
      LOG_WARN("failed to generate expr", K(ret));
    }
  }
  return ret;
}

int ObDmlCgService::need_fire_update_event(const ObTableSchema &table_schema,
                                                const ObString &update_events,
                                                const ObLogUpdate &log_op,
                                                const ObSQLSessionInfo &session,
                                                ObIAllocator &allocator,
                                                bool &need_fire)
{
  int ret = OB_SUCCESS;
  if (update_events.empty()) {
    need_fire = true;
  } else {
    need_fire = false;
    // session只是为了传入sql mode？那应该传trigger info中记录的信息。
    ObParser parser(allocator, session.get_sql_mode());
    ParseResult parse_result;
    const ParseNode *update_columns_node = NULL;
    ObString column_name;
    OV (log_op.get_primary_dml_info() != NULL);
    OZ (parser.parse(update_events, parse_result));
    OV (parse_result.result_tree_ != NULL);
    OV (parse_result.result_tree_->children_ != NULL);
    OX (update_columns_node = parse_result.result_tree_->children_[0]);
    OV (update_columns_node != NULL);
    OV (update_columns_node->type_ == T_TG_COLUMN_LIST, OB_ERR_UNEXPECTED, update_columns_node->type_);
    OV (update_columns_node->num_child_ > 0, OB_ERR_UNEXPECTED, update_columns_node->num_child_);
    OV (update_columns_node->children_ != NULL);
    ObSEArray<uint64_t, 4> base_column_ids;
    const ObAssignments &assignments = log_op.get_primary_dml_info()->assignments_;
    const ObDMLStmt *stmt = log_op.get_stmt();
    for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
      ObColumnRefRawExpr *column_expr = assignments.at(i).column_expr_;
      ColumnItem *column_item = nullptr;
      if (OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column expr", K(ret));
      } else if (OB_ISNULL(column_item = stmt->get_column_item_by_id(column_expr->get_table_id(),
                                                                     column_expr->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column item", K(ret), KPC(column_expr));
      } else if (OB_FAIL(base_column_ids.push_back(column_item->base_cid_))) {
        LOG_WARN("failed to push back base cid", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !need_fire && i < update_columns_node->num_child_; i++) {
      const ParseNode *column_node = update_columns_node->children_[i];
      const ObColumnSchemaV2 *column_schema = NULL;
      OV (column_node != NULL);
      OV (column_node->type_ == T_IDENT, OB_ERR_UNEXPECTED, column_node->type_);
      OV (column_node->str_value_ != NULL && column_node->str_len_ > 0);
      OX (column_name.assign_ptr(column_node->str_value_, static_cast<int32_t>(column_node->str_len_)));
      OX (column_schema = table_schema.get_column_schema(column_name));
      if (OB_SUCC(ret) && OB_ISNULL(column_schema)) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_WARN("column not exist", K(ret), K(i), K(column_name));
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < base_column_ids.count(); j++) {
        if (column_schema->get_column_id() == base_column_ids.at(j)) {
          OX (need_fire = !assignments.at(j).is_implicit_);
          break;
        }
      }
    }
  }
  return ret;
}

// for table
int ObDmlCgService::convert_normal_triggers(ObLogDelUpd &log_op,
                                            const IndexDMLInfo &dml_info,
                                            ObDMLBaseCtDef &dml_ctdef,
                                            bool is_instead_of,
                                            uint64_t dml_event)
{
  int ret = OB_SUCCESS;
  ObLogPlan *log_plan = log_op.get_plan();
  ObSchemaGetterGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  ObDASDMLBaseCtDef &das_ctdef = dml_ctdef.das_base_ctdef_;
  ObTrigDMLCtDef &trig_ctdef = dml_ctdef.trig_ctdef_;
  const ObDelUpdStmt *dml_stmt = NULL;
  if (OB_ISNULL(log_plan) ||
      OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_schema_guard()) ||
      OB_ISNULL(dml_stmt = static_cast<const ObDelUpdStmt*>(log_plan->get_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(MTL_ID(), dml_info.ref_table_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if ((table_schema->is_user_table() || table_schema->is_user_view()) &&
      0 < table_schema->get_trigger_list().count()) {
    const uint64_t tenant_id = table_schema->get_tenant_id();
    const ObIArray<uint64_t> &trigger_list = table_schema->get_trigger_list();
    const ObTriggerInfo *trigger_info = NULL;
    ObSEArray<const ObTriggerInfo *, 2> trigger_infos;
    uint64_t trigger_id = OB_INVALID_ID;
    bool need_fire = false;
    const ObSQLSessionInfo *session = NULL;
    ObPhyOperatorType op_type = PHY_INVALID;
    if (OB_ISNULL(session = log_plan->get_optimizer_context().get_session_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else if (OB_FAIL(cg_.get_phy_op_type(log_op, op_type, false))) {
      LOG_WARN("get phy operator type failed", K(ret));
    } else if (!is_instead_of &&
        (NULL != dml_info.new_rowid_expr_ || NULL != dml_info.old_rowid_expr_)) {
      if (OB_FAIL(convert_trigger_rowid(log_op, dml_info, dml_ctdef))) {
        LOG_WARN("failed to convert trigger rowid", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < trigger_list.count(); i++) {
      trigger_id = trigger_list.at(i);
      if (OB_FAIL(schema_guard->get_trigger_info(tenant_id, trigger_id, trigger_info))) {
        LOG_WARN("failed to get trigger info", K(ret), K(tenant_id));
      } else if (OB_ISNULL(trigger_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("trigger info is null", K(tenant_id), K(trigger_id), K(ret));
      } else {
        // if disable trigger, use the previous plan cache, whether trigger is enable ???
        need_fire = trigger_info->has_event(dml_event) && trigger_info->is_enable();
        if (OB_SUCC(ret) && !trigger_info->get_ref_trg_name().empty() && lib::is_oracle_mode()) {
          const ObTriggerInfo *ref_trigger_info = NULL;
          uint64_t ref_db_id = OB_INVALID_ID;
          OZ (schema_guard->get_database_id(tenant_id, trigger_info->get_ref_trg_db_name(), ref_db_id));
          OZ (schema_guard->get_trigger_info(tenant_id, ref_db_id, trigger_info->get_ref_trg_name(),
                                             ref_trigger_info));
          if (OB_SUCC(ret) && NULL == ref_trigger_info) {
            ret = OB_ERR_TRIGGER_NOT_EXIST;
            LOG_WARN("ref_trigger_info is NULL", K(trigger_info->get_ref_trg_db_name()),
                     K(trigger_info->get_ref_trg_name()), K(ret));
            LOG_ORACLE_USER_ERROR(OB_ERR_TRIGGER_NOT_EXIST, trigger_info->get_ref_trg_name().length(),
                                  trigger_info->get_ref_trg_name().ptr());
          }
          if (OB_SUCC(ret)) {
            if (trigger_info->is_simple_dml_type() && !ref_trigger_info->is_compound_dml_type()) {
              if (!(trigger_info->is_row_level_before_trigger() && ref_trigger_info->is_row_level_before_trigger())
                  && !(trigger_info->is_row_level_after_trigger() && ref_trigger_info->is_row_level_after_trigger())
                  && !(trigger_info->is_stmt_level_before_trigger() && ref_trigger_info->is_stmt_level_before_trigger())
                  && !(trigger_info->is_stmt_level_after_trigger()
                       && ref_trigger_info->is_stmt_level_after_trigger())) {
                ret = OB_ERR_RECOMPILATION_OBJECT;
                LOG_WARN("errors during recompilation/revalidation of trigger",
                          KPC(trigger_info), KPC(ref_trigger_info), K(ret));
                // ref_trg_db_name and trigger_info's database_name are the same
                LOG_ORACLE_USER_ERROR(OB_ERR_RECOMPILATION_OBJECT,
                                      trigger_info->get_ref_trg_db_name().length(),
                                      trigger_info->get_ref_trg_db_name().ptr(),
                                      trigger_info->get_trigger_name().length(),
                                      trigger_info->get_trigger_name().ptr());
              }
            }
          }
        }
        if (OB_SUCC(ret) && need_fire && !is_instead_of && op_type == PHY_UPDATE) {
          OZ (need_fire_update_event(*table_schema, trigger_info->get_update_columns(),
                                    static_cast<ObLogUpdate &>(log_op), *session,
                                    log_plan->get_optimizer_context().get_allocator(),
                                    need_fire));
        }
        if (OB_SUCC(ret) && need_fire) {
          OZ (trigger_infos.push_back(trigger_info));
        }
        OX (LOG_DEBUG("TRIGGER", K(trigger_info->get_trigger_name()), K(need_fire), K(is_instead_of)));
      }
    }
    if (OB_SUCC(ret) && trigger_infos.count() > 0) {
      //why we need add extra 1, the reason is trigger can use rowid and now we don't mock rowid
      //column schema, So, we must occupy the postition for rowid in advance.
      int64_t expectd_col_cnt = lib::is_oracle_mode() ? table_schema->get_column_count() + 1 :
                                                        table_schema->get_column_count();
      if (is_instead_of) {
        expectd_col_cnt = dml_stmt->get_instead_of_trigger_column_count();
      }
      trig_ctdef.tg_event_ = dml_event;
      ObTriggerInfo::ActionOrderComparator action_order_com;
      std::sort(trigger_infos.begin(), trigger_infos.end(), action_order_com);
      if (OB_FAIL(action_order_com.get_ret())) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("sort error", K(ret));
      }
      OZ (trig_ctdef.trig_col_info_.init(expectd_col_cnt));
      OZ (trig_ctdef.tg_args_.init(trigger_infos.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < trigger_infos.count(); i++) {
        OZ (add_trigger_arg(*trigger_infos.at(i), dml_ctdef));
        OX (LOG_DEBUG("TRIGGER", K(trigger_infos.at(i)->get_trigger_name())));
      }
      // need skip all hidden columns, see build_record_type_by_table_schema().
      bool is_hidden = false;
      bool is_update = false;
      bool is_gen_col = false;
      bool is_gen_col_dep = false;
      ObSEArray<uint64_t, 64> column_ids;
      const ObColumnSchemaV2 *column_schema = NULL;
      const ObAssignments &assigns = dml_info.assignments_;
      ObSEArray<uint64_t, 64> updated_column_ids;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generate_dml_column_ids(log_op, dml_info.column_exprs_, column_ids))) {
        LOG_WARN("add column ids failed", K(ret));
      } else if (OB_FAIL(generate_updated_column_ids(log_op, assigns, column_ids, updated_column_ids))) {
        LOG_WARN("add updated column ids failed", K(ret), K(assigns));
      } else if (ObTriggerEvents::has_insert_event(dml_event)) {
        OZ(trig_ctdef.new_row_exprs_.init(expectd_col_cnt));
      } else if (ObTriggerEvents::has_delete_event(dml_event)) {
        OZ(trig_ctdef.old_row_exprs_.init(expectd_col_cnt));
      } else if (ObTriggerEvents::has_update_event(dml_event)) {
        OZ(trig_ctdef.old_row_exprs_.init(expectd_col_cnt));
        OZ(trig_ctdef.new_row_exprs_.init(expectd_col_cnt));
        LOG_DEBUG("update columns", K(updated_column_ids));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: not supported", K(ret), K(op_type));
      }
      if (OB_SUCC(ret) && OB_FAIL(trig_ctdef.trig_col_info_.init(expectd_col_cnt))) {
        LOG_WARN("failed to init trigger column info", K(ret));
      }
      ObTableSchema::const_column_iterator cs_iter = table_schema->column_begin();
      ObTableSchema::const_column_iterator cs_iter_end = table_schema->column_end();
      int64_t i = 0;
      int64_t col_idx = INT64_MAX;
      int64_t total_count = is_instead_of ? expectd_col_cnt : table_schema->get_column_count();
      for (i = 0; OB_SUCC(ret) && i < total_count; i++) {
        // how to calc cell_idx and proj_idx ?
        // see
        ObExpr *new_expr = nullptr;
        ObExpr *old_expr = nullptr;
        bool need_add = false;
        if (!is_instead_of) {
          if (cs_iter == cs_iter_end) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: column schema is null", K(ret), K(i));
          } else if (OB_ISNULL(column_schema = *cs_iter)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: column schema is null", K(ret), K(i));
          } else {
            is_hidden = column_schema->is_hidden();
            is_gen_col = column_schema->is_generated_column();
            is_gen_col_dep = column_schema->has_generated_column_deps();
            is_update = has_exist_in_array(updated_column_ids, column_schema->get_column_id());
            if (!has_exist_in_array(column_ids, column_schema->get_column_id(), &col_idx)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Can't found column idx", K(i), K(column_ids),
                K(column_schema->get_column_id()), K(ret));
            }
            ++cs_iter;
          }
        } else {
          // instead trigger基于view, view可能由多个基表组成,所以每个column_id可能出现多次
          // 所以这里不能认为column_id = OB_APP_MIN_COLUMN_ID + i, 直接让 col_idx = i,
          // 因为前面保证了列顺序和view_schema一致
          col_idx = i;
        }
        LOG_DEBUG("debug trigger column", K(ret), K(is_hidden), K(i),
            K(col_idx), K(column_ids));
        if (is_hidden) {
          continue;
        }

        if (OB_SUCC(ret)) {
          LOG_DEBUG("debug trigger normal column", K(ret), K(is_instead_of),
              K(dml_ctdef.old_row_.count()), K(dml_ctdef.new_row_.count()));
          if (ObTriggerEvents::is_insert_event(dml_event)) {
            if (OB_UNLIKELY(col_idx < 0) ||
                OB_UNLIKELY(col_idx >= dml_ctdef.new_row_.count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected col idx", K(col_idx), K(dml_ctdef.new_row_));
            } else {
              new_expr = dml_ctdef.new_row_.at(col_idx);
            }
          } else if (ObTriggerEvents::is_update_event(dml_event)) {
            if (OB_UNLIKELY(col_idx < 0) ||
                OB_UNLIKELY(col_idx >= dml_ctdef.new_row_.count()) ||
                OB_UNLIKELY(col_idx >= dml_ctdef.old_row_.count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected col idx", K(col_idx), K(dml_ctdef.new_row_), K(dml_ctdef.old_row_));
            } else {
              new_expr = dml_ctdef.new_row_.at(col_idx);
              old_expr = dml_ctdef.old_row_.at(col_idx);
            }
          } else if (ObTriggerEvents::is_delete_event(dml_event)) {
            if (OB_UNLIKELY(col_idx < 0) ||
                OB_UNLIKELY(col_idx >= dml_ctdef.old_row_.count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected col idx", K(col_idx), K(dml_ctdef.old_row_));
            } else {
              old_expr = dml_ctdef.old_row_.at(col_idx);
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: not supported dml type",
              K(ret), K(dml_event), K(lbt()));
          }
        }
        if (OB_NOT_NULL(new_expr)) {
          need_add = true;
          if (OB_FAIL(trig_ctdef.new_row_exprs_.push_back(new_expr))) {
            LOG_WARN("failed to push back new row expr", K(ret));
          }
        }
        if (OB_NOT_NULL(old_expr)) {
          need_add = true;
          if (OB_FAIL(trig_ctdef.old_row_exprs_.push_back(old_expr))) {
            LOG_WARN("failed to push back old row expr", K(ret));
          }
        }
        if (need_add && OB_FAIL(trig_ctdef.trig_col_info_.set_trigger_column(
            is_hidden, is_update, is_gen_col, is_gen_col_dep, false))) {
          LOG_WARN("failed to set trigger column", K(ret));
        } else {
          LOG_DEBUG("trigger add trigger column", K(ret), K(need_add), K(i),
            K(new_expr), K(old_expr), K(is_hidden), K(is_update), K(is_gen_col), K(is_gen_col_dep));
        }
      }

      // oracle mode last row is rowid
      if (OB_SUCC(ret) && !is_instead_of && lib::is_oracle_mode()) {
        ObExpr *new_expr = nullptr;
        ObExpr *old_expr = nullptr;
        bool need_add = false;
        if (ObTriggerEvents::is_insert_event(dml_event)) {
          new_expr = trig_ctdef.rowid_new_expr_;
        } else if (ObTriggerEvents::is_update_event(dml_event)) {
          old_expr = trig_ctdef.rowid_old_expr_;
          new_expr = trig_ctdef.rowid_new_expr_;
        } else if (ObTriggerEvents::is_delete_event(dml_event)) {
          old_expr = trig_ctdef.rowid_old_expr_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: not supported dml type",
              K(ret), K(dml_event), K(lbt()));
        }
        if (OB_SUCC(ret)) {
          if (OB_NOT_NULL(new_expr)) {
            need_add = true;
            if (OB_FAIL(trig_ctdef.new_row_exprs_.push_back(new_expr))) {
              LOG_WARN("failed to push back new row expr", K(ret));
            }
          }
          if (OB_NOT_NULL(old_expr)) {
            need_add = true;
            if (OB_FAIL(trig_ctdef.old_row_exprs_.push_back(old_expr))) {
              LOG_WARN("failed to push back old row expr", K(ret));
            }
          }
          if (need_add && OB_FAIL(trig_ctdef.trig_col_info_.set_trigger_rowid())) {
            LOG_WARN("failed to set trigger rowid", K(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!is_instead_of) {
        OV (i == table_schema->get_column_count() && cs_iter == cs_iter_end, OB_ERR_UNEXPECTED, i, table_schema->get_column_count());
      } else {
        OV (i == expectd_col_cnt, OB_ERR_UNEXPECTED, i, expectd_col_cnt);
        if (dml_stmt->get_instead_of_trigger_column_count() != trig_ctdef.trig_col_info_.get_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: the column count of instead of trigger is not match",
            K(ret),
            K(dml_stmt->get_instead_of_trigger_column_count()),
            K(trig_ctdef.trig_col_info_.get_count()),
            K(expectd_col_cnt));
        }
      }
      LOG_DEBUG("debug trigger", K(trig_ctdef.new_row_exprs_.count()),
        K(trig_ctdef.old_row_exprs_.count()));
      bool is_forbid_parallel = false;
      const ObTriggerInfo *trigger_info = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && !is_forbid_parallel && i < trigger_infos.count(); ++i) {
        trigger_info = trigger_infos.at(i);
        if (trigger_info->is_modifies_sql_data() ||
            trigger_info->is_wps() ||
            trigger_info->is_rps() ||
            trigger_info->is_has_sequence()) {
          is_forbid_parallel = true;
        } else if (trigger_info->is_reads_sql_data()) { // dml + trigger(select) serial execute
          is_forbid_parallel = true;
        } else if (trigger_info->is_external_state()) {
          is_forbid_parallel = true;
        }
      }
      if (is_forbid_parallel) {
        cg_.phy_plan_->set_has_nested_sql(true);
        //为了支持触发器/UDF支持异常捕获，要求含有trigger的涉及修改表数据的dml串行执行
        cg_.phy_plan_->set_need_serial_exec(true);
        cg_.phy_plan_->set_contain_pl_udf_or_trigger(true);
      }
    }
  }
  return ret;
}

int ObDmlCgService::add_all_column_infos(ObLogDelUpd &op,
                                         const ObIArray<ObColumnRefRawExpr*> &columns,
                                         bool is_heap_table,
                                         ColContentFixedArray &column_infos)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = op.get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(column_infos.init(columns.count()))) {
    LOG_WARN("fail to init column infos count", K(ret));
  }
  ARRAY_FOREACH(columns, i) {
    const ObColumnRefRawExpr *column = columns.at(i);
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column expr", K(ret), K(i));
    } else {
      const int64_t table_id = column->get_table_id();
      const TableItem *table_item = stmt->get_table_item_by_id(table_id);
      // 对于可更新视图，这里拿到的column expr是视图的输出。此时column expr中NOT_NULL_FLAG表示的是
      // 视图的输出里这个column是否具有NOT NULL的性质，不再表示原始column expr在基表上的约束，因此需
      // 要递归地从视图中获取基表的column expr，并根据基表的column expr判断是否column是否是NOT NULL的
      // e.g. create table t1 (c1 int default null);
      //      update (select c1 from t1 where c1 > 10) v set c1 = null;
      //   这里t1.c1是NULLABLE的，但是视图v的输出中v.c1是NOT NULL的(因为视图中存在空值拒绝条件`c1 > 10`)
      if (OB_ISNULL(table_item) || op.has_instead_of_trigger()) {
        // 找不到说明column expr是shadow pk，直接从column中获取flag信息
      } else if ((table_item->is_generated_table() || table_item->is_temp_table()) &&
                 OB_FAIL(cg_.recursive_get_column_expr(column, *table_item))) {
        LOG_WARN("failed to recursive get column expr", K(ret));
      }

      if (OB_SUCC(ret)) {
        ColumnContent column_content;
        uint64_t base_cid = 0;
        bool skip_this_column = false;
        column_content.projector_index_ = i;
        column_content.auto_filled_timestamp_ =
            column->get_result_type().has_result_flag(ON_UPDATE_NOW_FLAG);
        column_content.is_nullable_ = !column->get_result_type().is_not_null_for_write();
        column_content.srs_id_ = column->get_srs_id();
        if (is_heap_table) {
          if (OB_FAIL(get_column_ref_base_cid(op, column, base_cid))) {
            LOG_WARN("fail to get base_column_id", K(ret), KPC(column));
          } else if (base_cid == OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
            skip_this_column = true;
          }
        }

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (skip_this_column) {
          // this column is hidden_pk of heap table，
          // skip not null check
          LOG_TRACE("skip hidden_pk not check", KPC(column));
        } else if (OB_FAIL(ob_write_string(cg_.phy_plan_->get_allocator(),
                                           column->get_column_name(),
                                           column_content.column_name_))) {
          LOG_WARN("failed to copy column name", K(ret), K(column->get_column_name()));
        } else if (OB_FAIL(column_infos.push_back(column_content))) {
          LOG_WARN("failed to add column info", K(ret), K(column->get_column_name()));
        } else {
          LOG_DEBUG("add column info", KPC(column), K(column_content));
        }
      }
    }
  }
  return ret;
}

int ObDmlCgService::convert_upd_assign_infos(bool is_heap_table,
                                             const IndexDMLInfo &index_dml_info,
                                             ColContentFixedArray &assign_infos)
{
  int ret = OB_SUCCESS;
  const ObAssignments &assigns = index_dml_info.assignments_;
  if (OB_FAIL(assign_infos.init(assigns.count()))) {
    LOG_WARN("init assign info array failed", K(ret), K(assigns.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); ++i) {
    ColumnContent column_content;
    int64_t idx = 0;
    ObColumnRefRawExpr *col = const_cast<ObColumnRefRawExpr*>(assigns.at(i).column_expr_);
    column_content.auto_filled_timestamp_ = col->get_result_type().has_result_flag(ON_UPDATE_NOW_FLAG);
    column_content.is_nullable_ = !col->get_result_type().is_not_null_for_write();
    column_content.is_predicate_column_ = assigns.at(i).is_predicate_column_;
    column_content.srs_id_ = col->get_srs_id();
    column_content.is_implicit_ = assigns.at(i).is_implicit_;
    if (is_heap_table &&
        assigns.at(i).expr_->get_expr_type() == T_TABLET_AUTOINC_NEXTVAL) {
      // skip it
      // update across partition, the hidden_pk of heap table must be generated once
    } else if (OB_FAIL(ob_write_string(cg_.phy_plan_->get_allocator(),
                                col->get_column_name(),
                                column_content.column_name_))) {
      LOG_WARN("failed to copy column name", K(ret), K(col->get_column_name()));
    } else if (!has_exist_in_array(index_dml_info.column_exprs_, col, &idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("assign column not exists in index dml info", K(ret), KPC(col));
    } else if (FALSE_IT(column_content.projector_index_ = static_cast<uint64_t>(idx))) {
      //do nothing
    } else if (OB_FAIL(assign_infos.push_back(column_content))) {
      LOG_WARN("store colum content to assign infos failed", K(ret), K(column_content));
    }
  }
  return ret;
}

int ObDmlCgService::convert_check_constraint(ObLogDelUpd &log_op,
                                             uint64_t ref_table_id,
                                             ObDMLBaseCtDef &dml_base_ctdef,
                                             const IndexDMLInfo &index_dml_info)
{
  int ret = OB_SUCCESS;
  ObLogPlan *log_plan = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;

  if (log_op.get_type() == log_op_def::LOG_DELETE) {
    //delete operator has no check constraint expr, do nothing
  } else if (OB_ISNULL(log_plan = log_op.get_plan()) ||
             OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(log_op), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ref_table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ref_table_id), K(ret));
  } else if (!(table_schema->is_user_table() || table_schema->is_tmp_table())) {
    // do nothing, especially for global index.
    LOG_DEBUG("skip convert constraint",
              "table_id", table_schema->get_table_name_str(),
              "table_type", table_schema->get_table_type());
  } else {
    OZ(cg_.generate_rt_exprs(index_dml_info.ck_cst_exprs_, dml_base_ctdef.check_cst_exprs_));
    if (OB_SUCC(ret)) {
      if (log_op.get_type() == log_op_def::LOG_INSERT_ALL ||
          log_op.get_type() == log_op_def::LOG_MERGE) {
        // insert all/merge into views not allowed, do nothing
      } else {
        OZ(cg_.generate_rt_exprs(log_op.get_view_check_exprs(), dml_base_ctdef.view_check_exprs_));
      }
    }
  }

  return ret;
}

int ObDmlCgService::generate_err_log_ctdef(const ObErrLogDefine &err_log_define,
                                                ObErrLogCtDef &err_log_ins_ctdef)
{
  int ret = OB_SUCCESS;
  err_log_ins_ctdef.is_error_logging_ = err_log_define.is_err_log_;
  err_log_ins_ctdef.reject_limit_ = err_log_define.reject_limit_;
  CK(err_log_define.err_log_column_names_.count() == err_log_define.err_log_value_exprs_.count());
  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(deep_copy_ob_string(cg_.phy_plan_->get_allocator(),
                                         err_log_define.err_log_database_name_,
                                         err_log_ins_ctdef.err_log_database_name_))) {
    LOG_WARN("fail to copy database name", K(ret), K(err_log_define.err_log_database_name_));
  } else if (OB_FAIL(deep_copy_ob_string(cg_.phy_plan_->get_allocator(),
                                         err_log_define.err_log_table_name_,
                                         err_log_ins_ctdef.err_log_table_name_))) {
    LOG_WARN("fail to copy table name", K(ret), K(err_log_define.err_log_table_name_));
  } else if (OB_FAIL(err_log_ins_ctdef.err_log_column_names_.
      prepare_allocate(err_log_define.err_log_column_names_.count()))) {
    LOG_WARN("fail to prepare_allocate", K(ret), K(err_log_define.err_log_column_names_.count()));
  } else if (OB_FAIL(err_log_ins_ctdef.err_log_values_.
                     prepare_allocate(err_log_define.err_log_value_exprs_.count()))) {
    LOG_WARN("fail to prepare_allocate", K(ret), K(err_log_define.err_log_value_exprs_.count()));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < err_log_define.err_log_value_exprs_.count(); i++) {
      ObRawExpr *raw_expr = err_log_define.err_log_value_exprs_.at(i);
      ObExpr *expr = NULL;
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw_expr is null", K(ret), K(i), K(raw_expr));
      } else if (OB_FAIL(deep_copy_ob_string(cg_.phy_plan_->get_allocator(),
                                             err_log_define.err_log_column_names_.at(i),
                                             err_log_ins_ctdef.err_log_column_names_.at(i)))) {
        LOG_WARN("fail to deep copy string", K(ret), K(err_log_define.err_log_column_names_.at(i)));
      } else if (OB_FAIL(cg_.generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("fail to generate rt expr", K(ret), K(i), KPC(raw_expr));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rt expr is null", K(ret), K(i), KPC(raw_expr));
      } else {
        err_log_ins_ctdef.err_log_values_.at(i) = expr;
      }
    }
  }

  return ret;
}

int ObDmlCgService::generate_multi_lock_ctdef(const IndexDMLInfo &index_dml_info,
                                              ObMultiLockCtDef &multi_lock_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_dml_info.old_part_id_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old part id expr is null", K(ret));
  } else if (OB_FAIL(generate_table_loc_meta(index_dml_info, multi_lock_ctdef.loc_meta_))) {
    LOG_WARN("generate table loc meta failed", K(ret));
  } else if (OB_FAIL(cg_.generate_calc_part_id_expr(*index_dml_info.old_part_id_expr_,
                                                    &multi_lock_ctdef.loc_meta_,
                                                    multi_lock_ctdef.calc_part_id_expr_))) {
    LOG_WARN("generate rt expr failed", K(ret));
  }
  return ret;
}

int ObDmlCgService::generate_multi_ins_ctdef(const IndexDMLInfo &index_dml_info,
                                             ObMultiInsCtDef &multi_ins_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_table_loc_meta(index_dml_info, multi_ins_ctdef.loc_meta_))) {
      LOG_WARN("generate table loc meta failed", K(ret));
  } else if (OB_FAIL(cg_.generate_calc_part_id_expr(*index_dml_info.new_part_id_expr_,
                                                    &multi_ins_ctdef.loc_meta_,
                                                    multi_ins_ctdef.calc_part_id_expr_))) {
    LOG_WARN("generate rt expr failed", K(ret));
  } else if (OB_FAIL(ObExprCalcPartitionBase::set_may_add_interval_part(
                                                 multi_ins_ctdef.calc_part_id_expr_,
                                                 MayAddIntervalPart::YES))) {
      LOG_WARN("failed to set partition info", K(ret));
  } else if (!index_dml_info.part_ids_.empty() && index_dml_info.is_primary_index_) {
    if (OB_FAIL(multi_ins_ctdef.hint_part_ids_.assign(index_dml_info.part_ids_))) {
      LOG_WARN("assign part ids failed", K(ret));
    } else {
      LOG_DEBUG("print part ids", K(index_dml_info.part_ids_));
    }
  }
  return ret;
}

int ObDmlCgService::generate_multi_del_ctdef(const IndexDMLInfo &index_dml_info,
                                             ObMultiDelCtDef &multi_del_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_table_loc_meta(index_dml_info, multi_del_ctdef.loc_meta_))) {
    LOG_WARN("generate table loc meta failed", K(ret));
  } else if (OB_FAIL(cg_.generate_calc_part_id_expr(*index_dml_info.old_part_id_expr_,
                                                    &multi_del_ctdef.loc_meta_,
                                                    multi_del_ctdef.calc_part_id_expr_))) {
    LOG_WARN("generate rt expr failed", K(ret));
  }
  return ret;
}

int ObDmlCgService::generate_multi_upd_ctdef(const ObLogDelUpd &op,
                                             const IndexDMLInfo &index_dml_info,
                                             ObMultiUpdCtDef &multi_upd_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_dml_info.old_part_id_expr_) ||
      OB_ISNULL(index_dml_info.new_part_id_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index dml info calc part id exprs is invalid", K(ret));
  } else if (OB_FAIL(generate_table_loc_meta(index_dml_info, multi_upd_ctdef.loc_meta_))) {
    LOG_WARN("generate table loc meta failed", K(ret));
  } else if (OB_FAIL(cg_.generate_calc_part_id_expr(*index_dml_info.old_part_id_expr_,
                                                    &multi_upd_ctdef.loc_meta_,
                                                    multi_upd_ctdef.calc_part_id_old_))) {
    LOG_WARN("generate old part id expr failed", K(ret));
  } else if (OB_FAIL(cg_.generate_calc_part_id_expr(*index_dml_info.new_part_id_expr_,
                                                    &multi_upd_ctdef.loc_meta_,
                                                    multi_upd_ctdef.calc_part_id_new_))) {
    LOG_WARN("generate new part id expr failed", K(ret));
  } else {
    multi_upd_ctdef.is_enable_row_movement_ = true;
    if (lib::is_oracle_mode() && index_dml_info.is_primary_index_) {
      const ObTableSchema *table_schema = nullptr;
      ObSqlSchemaGuard *schema_guard = cg_.opt_ctx_->get_sql_schema_guard();
      if (OB_FAIL(schema_guard->get_table_schema(index_dml_info.ref_table_id_, table_schema))) {
        LOG_WARN("get table schema failed", K(index_dml_info.ref_table_id_), K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(index_dml_info.ref_table_id_));
      } else if (!table_schema->is_enable_row_movement()) {
        multi_upd_ctdef.is_enable_row_movement_ = false;
      }
    }
    if (multi_upd_ctdef.is_enable_row_movement_) {
      if (OB_FAIL(ObExprCalcPartitionBase::set_may_add_interval_part(
                                                 multi_upd_ctdef.calc_part_id_new_,
                                                 MayAddIntervalPart::YES))) {
        LOG_WARN("failed to set partition info", K(ret));
      }
    } else {
      if (OB_FAIL(ObExprCalcPartitionBase::set_may_add_interval_part(
                                     multi_upd_ctdef.calc_part_id_new_,
                                     MayAddIntervalPart::PART_CHANGE_ERR))) {
        LOG_WARN("failed to set partition info", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    const ObDMLStmt *stmt = op.get_stmt();
    const ObUpdateStmt *update_stmt = static_cast<const ObUpdateStmt *>(stmt);
    if (OB_ISNULL(update_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!index_dml_info.part_ids_.empty() &&
               OB_FAIL(multi_upd_ctdef.hint_part_ids_.assign(index_dml_info.part_ids_))) {
      LOG_WARN("failed to assign part ids", K(ret));
    }
  }
  return ret;
}

int ObDmlCgService::generate_table_loc_meta(const IndexDMLInfo &index_dml_info,
                                            ObDASTableLocMeta &loc_meta)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  ObSchemaGetterGuard *schema_guard = nullptr;
  if (OB_ISNULL(cg_.opt_ctx_)
      || OB_ISNULL(schema_guard = cg_.opt_ctx_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(cg_.opt_ctx_), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(MTL_ID(), index_dml_info.ref_table_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(index_dml_info.ref_table_id_));
  } else {
    loc_meta.table_loc_id_ = index_dml_info.loc_table_id_;
    loc_meta.ref_table_id_ = index_dml_info.ref_table_id_;
    loc_meta.select_leader_ = 1;
    loc_meta.is_dup_table_ = (ObDuplicateScope::DUPLICATE_SCOPE_NONE != table_schema->get_duplicate_scope());
    //related local index tablet_id pruning only can be used in local plan or remote plan(all operator
    //use the same das context),
    //because the distributed plan will transfer tablet_id through exchange operator,
    //but the related tablet_id map can not be transfered by exchange operator,
    //unused related pruning in distributed plan's dml operator,
    //we will build the related tablet_id map when dml operator be opened in distributed plan
    loc_meta.unuse_related_pruning_ = (OB_PHY_PLAN_DISTRIBUTED == cg_.opt_ctx_->get_phy_plan_type()
                                       && !cg_.opt_ctx_->get_root_stmt()->is_insert_stmt());
    loc_meta.is_external_table_ = table_schema->is_external_table();
    loc_meta.is_external_files_on_disk_ =
        ObSQLUtils::is_external_files_on_local_disk(table_schema->get_external_file_location());
  }
  if (OB_SUCC(ret) && index_dml_info.is_primary_index_) {
    TableLocRelInfo *rel_info = nullptr;
    rel_info = cg_.opt_ctx_->get_loc_rel_info_by_id(index_dml_info.loc_table_id_,
                                                    index_dml_info.ref_table_id_);
    if (nullptr == rel_info || rel_info->related_ids_.count() <= 1) {
      //the first table id is the source table, <=1 mean no dependency table
    } else {
      loc_meta.related_table_ids_.set_capacity(rel_info->related_ids_.count() - 1);
      for (int64_t i = 0; OB_SUCC(ret) && i < rel_info->related_ids_.count(); ++i) {
        if (rel_info->related_ids_.at(i) == loc_meta.ref_table_id_) {
          //filter itself, do nothing
        } else if (OB_FAIL(loc_meta.related_table_ids_.push_back(rel_info->related_ids_.at(i)))) {
          LOG_WARN("store related table id failed", K(ret), KPC(rel_info), K(i));
        }
      }
    }
  }
  return ret;
}

int ObDmlCgService::convert_insert_new_row_exprs(const IndexDMLInfo &index_dml_info,
                                                 ObIArray<ObRawExpr*> &new_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index_dml_info.column_exprs_.count() !=
      index_dml_info.column_convert_exprs_.count())) {
    LOG_WARN("index dml info column exprs not matched", K(ret), K(index_dml_info));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.column_convert_exprs_.count(); ++i) {
    const ObRawExpr *column_convert = index_dml_info.column_convert_exprs_.at(i);
    if (OB_FAIL(new_row.push_back(const_cast<ObRawExpr*>(column_convert)))) {
      LOG_WARN("store new row expr failed", K(ret));
    }
  }
  return ret;
}

//insert's access expr is special, can't use this interface to convert access expr
int ObDmlCgService::convert_old_row_exprs(const ObIArray<ObColumnRefRawExpr*> &columns,
                                          ObIArray<ObRawExpr*> &access_exprs,
                                          int64_t col_cnt /*= -1*/)
{
  int ret = OB_SUCCESS;
  if (-1 == col_cnt) {
    col_cnt = columns.count();
  }
  CK(col_cnt > 0 && col_cnt <= columns.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
    ObColumnRefRawExpr *col_expr = const_cast<ObColumnRefRawExpr*>(columns.at(i));
    if (OB_FAIL(access_exprs.push_back(col_expr))) {
      LOG_WARN("store storage access expr failed", K(ret));
    }
  }
  return ret;
}

int ObDmlCgService::need_foreign_key_handle(const ObForeignKeyArg &fk_arg,
                                            const common::ObIArray<uint64_t> &updated_column_ids,
                                            const ObIArray<uint64_t> &value_column_ids,
                                            const ObDASOpType &op_type,
                                            bool &need_handle)
{
  int ret = OB_SUCCESS;
  need_handle = true;
  if (ACTION_INVALID == fk_arg.ref_action_) {
    need_handle = false;
  } else if (DAS_OP_TABLE_UPDATE == op_type) {
    // check if foreign key operation is necessary.
    // no matter current table is parent table or child table, the value_column_ids will
    // represent the foreign key related columns of the current table. so we only need to
    // check if these columns maybe updated, by checking if the two arrays are intersected.
    bool has_intersect = false;
    for (int64_t i = 0; !has_intersect && i < value_column_ids.count(); i++) {
      for (int64_t j = 0; !has_intersect && j < updated_column_ids.count(); j++) {
        has_intersect = (value_column_ids.at(i) == updated_column_ids.at(j));
      }
    }
    need_handle = has_intersect;
  } else {
    // nothing.
  }
  return ret;
}

int ObDmlCgService::generate_fk_arg(ObForeignKeyArg &fk_arg,
                                    bool check_parent_table,
                                    const IndexDMLInfo &index_dml_info,
                                    const ObForeignKeyInfo &fk_info,
                                    const ObLogDelUpd &op,
                                    ObRawExpr* fk_part_id_expr,
                                    ObSchemaGetterGuard &schema_guard,
                                    ObDMLBaseCtDef &dml_ctdef)
{
  int ret = OB_SUCCESS;
  bool need_handle = true;
  const ObDatabaseSchema *database_schema = NULL;
  const ObTableSchema *table_schema = NULL;
  const ObColumnSchemaV2 *column_schema = NULL;
  ObIAllocator &allocator = cg_.phy_plan_->get_allocator();
  const ObDASDMLBaseCtDef &das_ctdef = dml_ctdef.das_base_ctdef_;
  ObArray<uint64_t> column_ids;
  ObArray<uint64_t> updated_column_ids;
  fk_arg.use_das_scan_ = check_parent_table;
  const ObIArray<uint64_t> &value_column_ids = check_parent_table ? fk_info.child_column_ids_ : fk_info.parent_column_ids_;
  const ObIArray<uint64_t> &name_column_ids = check_parent_table ? fk_info.parent_column_ids_ : fk_info.child_column_ids_;
  uint64_t name_table_id = check_parent_table ? fk_info.parent_table_id_ : fk_info.child_table_id_;

  if (OB_FAIL(generate_dml_column_ids(op, index_dml_info.column_exprs_, column_ids))) {
    LOG_WARN("add column ids failed", K(ret));
  } else if (OB_FAIL(generate_updated_column_ids(op, index_dml_info.assignments_, column_ids, updated_column_ids))) {
    LOG_WARN("add updated column ids failed", K(ret), K(index_dml_info.assignments_));
  } else if (OB_FAIL(need_foreign_key_handle(fk_arg, updated_column_ids,
                                      value_column_ids, das_ctdef.op_type_,
                                      need_handle))) {
    LOG_WARN("failed to check if need handle foreign key", K(ret));
  } else if (!need_handle) {
    LOG_DEBUG("skip foreign key handle", K(fk_arg));
  } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), name_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(fk_arg), K(name_table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(name_table_id), K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(table_schema->get_tenant_id(),
                                                      table_schema->get_database_id(),
                                                      database_schema))) {
    LOG_WARN("failed to get database schema", K(table_schema->get_database_id()), K(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is null", K(table_schema->get_database_id()), K(ret));
  } else if (OB_FAIL(deep_copy_ob_string(allocator,
                                         database_schema->get_database_name(),
                                         fk_arg.database_name_))) {
    LOG_WARN("failed to deep copy ob string", K(fk_arg),
             K(table_schema->get_table_name()), K(ret));
  } else if (OB_FAIL(deep_copy_ob_string(allocator,
                                         table_schema->get_table_name(),
                                         fk_arg.table_name_))) {
    LOG_WARN("failed to deep copy ob string", K(fk_arg),
             K(table_schema->get_table_name()), K(ret));
  } else if (FALSE_IT(fk_arg.columns_.reset())) {
  } else if (OB_FAIL(fk_arg.columns_.reserve(name_column_ids.count()))) {
    LOG_WARN("failed to reserve foreign key columns", K(name_column_ids.count()), K(ret));
  }
  if ( OB_SUCC(ret) && need_handle) {
    fk_arg.table_id_ = name_table_id;
  }
  for (int64_t i = 0; OB_SUCC(ret) && need_handle && i < name_column_ids.count(); i++) {
    ObForeignKeyColumn fk_column;
    if (OB_ISNULL(column_schema = (table_schema->get_column_schema(name_column_ids.at(i))))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(fk_arg), K(name_column_ids.at(i)), K(ret));
    } else if (OB_FAIL(deep_copy_ob_string(allocator,
                                           column_schema->get_column_name_str(),
                                           fk_column.name_))) {
      LOG_WARN("failed to deep copy ob string", K(fk_arg),
               K(column_schema->get_column_name_str()), K(ret));
    } else if (fk_arg.is_self_ref_
        && !var_exist_in_array(column_ids, name_column_ids.at(i), fk_column.name_idx_)) {
      /**
       * issue/18132630
       * fk_column.name_idx_ is used only for self ref row, that is to say name table and
       * value table is same table.
       * otherwise name_column_ids.at(i) will indicate columns in name table, not value table,
       * and spec is value table here.
       */
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("foreign key column id is not in colunm ids",
                K(fk_arg), K(name_column_ids.at(i)), K(ret));
    } else if (!var_exist_in_array(column_ids, value_column_ids.at(i), fk_column.idx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("foreign key column id is not in colunm ids",
                K(fk_arg), K(value_column_ids.at(i)), K(ret));
    } else {
      fk_column.obj_meta_ = column_schema->get_meta_type();
      if (ob_is_double_tc(fk_column.obj_meta_.get_type())) {
        fk_column.obj_meta_.set_scale(column_schema->get_accuracy().get_scale());
      }
      if (OB_FAIL(fk_arg.columns_.push_back(fk_column))) {
        LOG_WARN("failed to push foreign key column", K(fk_arg), K(fk_column), K(ret));
      }
    }
  }

  // if need use das scan to perform foreign key check, create fk_check_ctdef for fk_arg
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (need_handle) {
    if (check_parent_table) {
      ObDMLCtDefAllocator<ObForeignKeyCheckerCtdef> fk_allocator(cg_.phy_plan_->get_allocator());
      if (OB_ISNULL(fk_arg.fk_ctdef_ = fk_allocator.alloc())) {
        LOG_WARN("failed to alocate foreign key ctdef", K(ret));
      } else if (OB_FAIL(generate_fk_check_ctdef(op, name_table_id,
                                                fk_part_id_expr,
                                                name_column_ids,
                                                schema_guard,
                                                *fk_arg.fk_ctdef_))) {
        LOG_WARN("failed to check foreign key check ctdef", K(ret));
      } else if (OB_FAIL(dml_ctdef.fk_args_.push_back(fk_arg))) {
        LOG_WARN("failed to add foreign key arg", K(fk_arg), K(ret));
      }
    } else {
      if (OB_FAIL(dml_ctdef.fk_args_.push_back(fk_arg))) {
        LOG_WARN("failed to add foreign key arg", K(fk_arg), K(ret));
      } else {
        cg_.phy_plan_->set_has_nested_sql(true);
      }
    }
  } else if (!need_handle) {
    fk_arg.use_das_scan_ = false;
  }
  return ret;
}

int ObDmlCgService::generate_fk_check_ctdef(const ObLogDelUpd &op,
                                            uint64_t name_table_id,
                                            ObRawExpr* fk_part_id_expr,
                                            const common::ObIArray<uint64_t> &name_column_ids,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            ObForeignKeyCheckerCtdef &fk_ctdef)
{
  int ret = OB_SUCCESS;
  uint64_t index_tid = OB_INVALID_ID;
  // check if need create check ctdef
  if (get_fk_check_scan_table_id(name_table_id, name_column_ids, schema_guard, index_tid)) {
    LOG_WARN("failed to get foreign key check scan table id", K(name_table_id), K(ret));
  } else if (OB_INVALID_ID == index_tid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index table id to build das scan task for foreign key check", K(ret));
  } else if (OB_FAIL(generate_fk_scan_ctdef(schema_guard, index_tid, fk_ctdef.das_scan_ctdef_))) {
    LOG_WARN("failed to generate das scan ctdef for foreign key check", K(ret));
  } else if (OB_FAIL(generate_fk_table_loc_info(index_tid, fk_ctdef.loc_meta_, fk_ctdef.tablet_id_, fk_ctdef.is_part_table_))) {
    LOG_WARN("failed to generate table location meta for foreign key check", K(ret));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const ObTableSchema *table_schema = nullptr;
    fk_ctdef.rowkey_ids_.set_capacity(name_column_ids.count());
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_tid, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_FAIL(generate_rowkey_idx_for_foreign_key(name_column_ids, table_schema, fk_ctdef.rowkey_ids_))) {
      LOG_WARN("failed to generate rowkey ids for foreign key", K(ret));
    } else {
      fk_ctdef.rowkey_count_ = table_schema->get_rowkey_column_num();
    }
  }
  // generate the part expr used for building das task to perform foreign key check if parent table is partitioned
  if (OB_SUCC(ret)) {
    ObRawExpr *part_id_expr_for_lookup = NULL;
    ObExpr *rt_part_id_expr = NULL;
    ObSEArray<ObRawExpr *, 4> constraint_dep_exprs;
    ObSEArray<ObRawExpr *, 4> constraint_raw_exprs;
    if (OB_ISNULL(part_id_expr_for_lookup = fk_part_id_expr)) {
      // check if table to perform das task is partiton table
    } else if (OB_FAIL(cg_.generate_calc_part_id_expr(*part_id_expr_for_lookup, nullptr, rt_part_id_expr))) {
      LOG_WARN("generate rt part_id_expr failed", K(ret), KPC(part_id_expr_for_lookup));
    } else if (OB_ISNULL(rt_part_id_expr)) {
      LOG_WARN("rt part_id_expr for lookup is null", K(ret));
    } else if (OB_FAIL(constraint_raw_exprs.push_back(part_id_expr_for_lookup))) {
      LOG_WARN("fail to push part_id_expr to constraint_raw_exprs", K(ret));
    } else if (OB_FAIL(cg_.generate_calc_exprs(constraint_dep_exprs,
                                               constraint_raw_exprs,
                                               fk_ctdef.part_id_dep_exprs_,
                                               op.get_type(),
                                               false))) {
      LOG_WARN("fail to generate part_id_expr depend calc_expr", K(constraint_dep_exprs),
          K(constraint_raw_exprs), K(fk_ctdef.part_id_dep_exprs_));
    } else {
      fk_ctdef.calc_part_id_expr_ = rt_part_id_expr;
    }
  }
  return ret;
}

int ObDmlCgService::generate_rowkey_idx_for_foreign_key(const ObIArray<uint64_t> &name_column_ids,
                                           const ObTableSchema *parent_table,
                                           ObIArray<int64_t> &rowkey_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema used for foreign key check is null", K(ret));
  } else if (OB_FAIL(rowkey_ids.reserve(name_column_ids.count()))) {
    LOG_WARN("failed to reverse rowkey ids", K(ret));
  } else {
    const ObRowkeyInfo &rowkey_info = parent_table->get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < name_column_ids.count(); ++i) {
      const uint64_t column_id = name_column_ids.at(i);
      int64_t index = -1;
      if (OB_FAIL(rowkey_info.get_index(column_id, index))) {
        LOG_WARN("failed to get the index of parent column in primary key", K(ret));
      } else if (OB_FAIL(rowkey_ids.push_back(index))) {
        LOG_WARN("failed to push back rowkey index", K(ret));
      }
    }
  }
  return ret;
}

int ObDmlCgService::generate_fk_table_loc_info(uint64_t index_table_id,
                                               ObDASTableLocMeta &loc_meta,
                                               ObTabletID &tablet_id,
                                               bool &is_part_table)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  ObSchemaGetterGuard *schema_guard = nullptr;
  if (OB_ISNULL(cg_.opt_ctx_)
      || OB_ISNULL(schema_guard = cg_.opt_ctx_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(cg_.opt_ctx_), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(MTL_ID(), index_table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(index_table_id));
  } else {
    loc_meta.table_loc_id_ = index_table_id;
    loc_meta.ref_table_id_ = index_table_id;
    loc_meta.select_leader_ = 1;
    loc_meta.is_dup_table_ = (ObDuplicateScope::DUPLICATE_SCOPE_NONE != table_schema->get_duplicate_scope());
    if (PARTITION_LEVEL_ZERO == table_schema->get_part_level()) {
      tablet_id = table_schema->get_tablet_id();
    } else {
      is_part_table = true;
    }
  }

  return ret;
}

int ObDmlCgService::get_fk_check_scan_table_id(const uint64_t parent_table_id,
                                              const common::ObIArray<uint64_t> &name_column_ids,
                                              share::schema::ObSchemaGetterGuard &schema_guard,
                                              uint64_t &index_table_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, parent_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_FAIL(table_schema->get_fk_check_index_tid(schema_guard, name_column_ids, index_table_id))) {
    LOG_WARN("failed to get scan table id for foreign key check", K(ret));
  }
  return ret;
}

int ObDmlCgService::generate_fk_scan_ctdef(share::schema::ObSchemaGetterGuard &schema_guard,
                                          const uint64_t index_tid,
                                          ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  scan_ctdef.ref_table_id_ = index_tid;
  const uint64_t tenant_id = MTL_ID();
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_tid, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(
      TABLE_SCHEMA, tenant_id, index_tid, scan_ctdef.schema_version_))) {
    LOG_WARN("fail to get schema version", K(ret), K(tenant_id), K(index_tid));
  } else {
    scan_ctdef.table_param_.get_enable_lob_locator_v2()
        = (cg_.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0);
    if (OB_FAIL(scan_ctdef.table_param_.convert(*table_schema, scan_ctdef.access_column_ids_))) {
      LOG_WARN("convert table param failed", K(ret));
    }
  }
  return ret;
}

int ObDmlCgService::generate_fk_scan_part_id_expr(ObLogDelUpd &op,
                                                  uint64_t parent_table_id,
                                                  uint64_t index_tid,
                                                  ObForeignKeyCheckerCtdef &fk_ctdef)
{
  int ret = OB_SUCCESS;

  // check if the table to perform das scan task is partition table
  bool is_part_table = false;
  if (OB_SUCC(ret) && is_part_table) {
    ObRawExpr *part_id_expr_for_lookup = NULL;
    ObExpr *rt_part_id_expr = NULL;
    ObSEArray<ObRawExpr *, 4> constraint_dep_exprs;
    ObSEArray<ObRawExpr *, 4> constraint_raw_exprs;
    ObLogPlan *log_plan = nullptr;
    if (OB_ISNULL(log_plan = op.get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan is null", K(ret));
    } else if (OB_FAIL(log_plan->gen_calc_part_id_expr(parent_table_id,
                                                      index_tid,
                                                      CALC_PARTITION_TABLET_ID,
                                                      part_id_expr_for_lookup))) {
      LOG_WARN("failed to gen calc part id expr", K(ret));
    } else if (OB_ISNULL(part_id_expr_for_lookup)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_id_expr for lookup is null", K(ret));
    } else if (OB_FAIL(cg_.generate_calc_part_id_expr(*part_id_expr_for_lookup, nullptr, rt_part_id_expr))) {
      LOG_WARN("generate rt part_id_expr failed", K(ret), KPC(part_id_expr_for_lookup));
    } else if (OB_ISNULL(rt_part_id_expr)) {
      LOG_WARN("rt part_id_expr for lookup is null", K(ret));
    } else if (OB_FAIL(constraint_raw_exprs.push_back(part_id_expr_for_lookup))) {
      LOG_WARN("fail to push part_id_expr to constraint_raw_exprs", K(ret));
    } else if (OB_FAIL(cg_.generate_calc_exprs(constraint_dep_exprs,
                                               constraint_raw_exprs,
                                               fk_ctdef.part_id_dep_exprs_,
                                               op.get_type(),
                                               false))) {
      LOG_WARN("fail to generate part_id_expr depend calc_expr", K(constraint_dep_exprs),
          K(constraint_raw_exprs), K(fk_ctdef.part_id_dep_exprs_));
    } else {
      fk_ctdef.calc_part_id_expr_ = rt_part_id_expr;
    }
  }
  return ret;
}

int ObDmlCgService::convert_foreign_keys(ObLogDelUpd &op,
                                         const IndexDMLInfo &index_dml_info,
                                         ObDMLBaseCtDef &dml_ctdef)
{
  int ret = OB_SUCCESS;
  ObLogPlan *log_plan = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObIArray<ObForeignKeyInfo> *fk_infos = NULL;
  const ObTableSchema *table_schema = NULL;
  bool check_parent_table = false;
  if (OB_ISNULL(log_plan = op.get_plan()) || OB_ISNULL(cg_.phy_plan_) ||
             OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard()) ||
             OB_ISNULL(schema_guard->get_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(op), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_dml_info.ref_table_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(index_dml_info.ref_table_id_), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(index_dml_info.ref_table_id_), K(ret));
  } else if (!table_schema->is_user_table()) {
    // do nothing, especially for global index.
    LOG_DEBUG("skip convert foreign key",
              "table_id", table_schema->get_table_name_str(),
              "table_type", table_schema->get_table_type());
  } else if (OB_ISNULL(fk_infos = &table_schema->get_foreign_key_infos())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("foreign key infos is null", K(ret));
  } else if (OB_FAIL(dml_ctdef.fk_args_.init(table_schema->get_foreign_key_real_count()))) {
    LOG_WARN("failed to init foreign key stmts", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < fk_infos->count(); i++) {
      const ObForeignKeyInfo &fk_info = fk_infos->at(i);
      ObForeignKeyArg fk_arg(cg_.phy_plan_->get_allocator());
      if (lib::is_oracle_mode()) {
        if (!fk_info.enable_flag_ && fk_info.is_validated()) {
          const ObSimpleDatabaseSchema *database_schema = NULL;
          if (OB_FAIL(schema_guard->get_schema_guard()->get_database_schema(
                      table_schema->get_tenant_id(), table_schema->get_database_id(), database_schema))) {
            LOG_WARN("get database schema failed", K(ret), K(table_schema->get_database_id()));
          } else if (OB_ISNULL(database_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("database_schema is null", K(ret));
          } else {
            ret = OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE;
            LOG_USER_ERROR(OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE,
                database_schema->get_database_name_str().length(),
                database_schema->get_database_name_str().ptr(),
                fk_info.foreign_key_name_.length(), fk_info.foreign_key_name_.ptr());
            LOG_WARN("no insert/delete/update on table with constraint disabled and validated",
                     K(ret));
          }
        } else if (!fk_info.enable_flag_) {
          continue;
        }
      }
      if (fk_info.child_column_ids_.count() != fk_info.parent_column_ids_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child column count and parent column count is not equal",
                 K(ret), K(fk_info.child_column_ids_), K(fk_info.parent_column_ids_));
      }
      if (OB_SUCC(ret) && fk_info.parent_table_id_ == fk_info.child_table_id_) {
        fk_arg.is_self_ref_ = true;
      }
      if (OB_SUCC(ret) && fk_info.table_id_ == fk_info.child_table_id_) {
        if (DAS_OP_TABLE_INSERT == dml_ctdef.dml_type_
            || DAS_OP_TABLE_UPDATE == dml_ctdef.dml_type_) {
          if (lib::is_mysql_mode() && fk_info.is_parent_table_mock_) {
            ObSQLSessionInfo *session = nullptr;
            int64_t foreign_key_checks = 0;
            if (OB_ISNULL(op.get_plan())
                || OB_ISNULL(session = op.get_plan()->get_optimizer_context().get_session_info())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("session is invalid", K(op.get_plan()), K(session));
            } else if (OB_FAIL(session->get_foreign_key_checks(foreign_key_checks))) {
              LOG_WARN("get foreign_key_checks failed", K(ret));
            } else if (1 == foreign_key_checks) {
              ret = OB_ERR_NO_REFERENCED_ROW;
              LOG_WARN("insert or update a child table with a mock parent table", K(ret));
            } else { // skip fk check while foreign_key_checks if off
              fk_arg.ref_action_ = ACTION_INVALID;
            }
          } else {
            fk_arg.ref_action_ = ACTION_CHECK_EXIST;
          }
          check_parent_table = true;
        } else {
          fk_arg.ref_action_ = ACTION_INVALID;
        }
        if (OB_FAIL(ret)) {
        } else if (fk_arg.ref_action_ != ACTION_INVALID &&
                   OB_FAIL(generate_fk_arg(fk_arg, check_parent_table, index_dml_info, fk_info, op,
                                          index_dml_info.fk_lookup_part_id_expr_.at(i),
                                          *schema_guard->get_schema_guard(),
                                          dml_ctdef))) {
          LOG_WARN("failed to add fk arg to dml ctdef", K(ret));
        }
      }
      if (OB_SUCC(ret) && fk_info.table_id_ == fk_info.parent_table_id_) {
        if (DAS_OP_TABLE_UPDATE == dml_ctdef.dml_type_) {
          fk_arg.ref_action_ = fk_info.update_action_;
        } else if (DAS_OP_TABLE_DELETE == dml_ctdef.dml_type_) {
          fk_arg.ref_action_ = fk_info.delete_action_;
        } else {
          fk_arg.ref_action_ = ACTION_INVALID;
        }
        check_parent_table = false;
        if (fk_arg.ref_action_ != ACTION_INVALID && OB_FAIL(generate_fk_arg(fk_arg,
                                                            check_parent_table,
                                                            index_dml_info,
                                                            fk_info, op,
                                                            nullptr, // fk_lookup_part_id_expr_, for parent table, don't use scan task to perform foreign key check
                                                            *schema_guard->get_schema_guard(),
                                                            dml_ctdef))) {
          LOG_WARN("failed to add fk arg to dml ctdef", K(ret));
        }
      }
    } // for
    if (OB_SUCC(ret) && fk_infos->count() > 0) {
      OX (cg_.phy_plan_->set_need_serial_exec(true));
    }
    LOG_TRACE("convert foreign keys finish", K(ret), KPC(fk_infos), K(dml_ctdef.fk_args_));
  }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObDmlCgService::init_encrypt_metas_(
    const share::schema::ObTableSchema *table_schema,
    share::schema::ObSchemaGetterGuard *guard,
    ObIArray<transaction::ObEncryptMetaCache> &meta_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema) || OB_ISNULL(guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is invalid", K(ret));
  } else if (OB_FAIL(init_encrypt_table_meta_(table_schema, guard, meta_array))) {
    LOG_WARN("fail to init encrypt_table_meta", KPC(table_schema), K(ret));
  } else if (!table_schema->is_user_table()) {
    /*do nothing*/
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    const ObTableSchema *index_schema = nullptr;
    if (OB_FAIL(table_schema->get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get simple_index_infos failed", K(ret));
    }
    for (int i = 0; i < simple_index_infos.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(guard->get_table_schema(
                  table_schema->get_tenant_id(),
                  simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("fail to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("index schema not exist", K(simple_index_infos.at(i).table_id_), K(ret));
      } else if (!index_schema->is_index_local_storage()) {
        // do nothing
      } else if (OB_FAIL(init_encrypt_table_meta_(index_schema, guard, meta_array))) {
        LOG_WARN("fail to init encrypt_table_meta", KPC(index_schema), K(ret));
      }
    }
  }
  return ret;
}

int ObDmlCgService::init_encrypt_table_meta_(
    const share::schema::ObTableSchema *table_schema,
    share::schema::ObSchemaGetterGuard *guard,
    ObIArray<transaction::ObEncryptMetaCache>&meta_array)
{
  int ret = OB_SUCCESS;
  transaction::ObEncryptMetaCache meta_cache;
  char master_key[OB_MAX_MASTER_KEY_LENGTH];
  int64_t master_key_length = 0;
  if (OB_ISNULL(table_schema) || OB_ISNULL(guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is invalid", K(ret));
  } else if (!(table_schema->need_encrypt() && table_schema->get_master_key_id() > 0 &&
               table_schema->get_encrypt_key_len() > 0)) {
    // do nothing
  } else if (OB_FAIL(table_schema->get_encryption_id(meta_cache.meta_.encrypt_algorithm_))) {
    LOG_WARN("fail to get encryption id", K(ret));
  } else {
    if (table_schema->is_index_local_storage()) {
      meta_cache.table_id_ = table_schema->get_data_table_id();
      meta_cache.local_index_id_ = table_schema->get_table_id();
    } else {
      meta_cache.table_id_ = table_schema->get_table_id();
    }
    meta_cache.meta_.tenant_id_ = table_schema->get_tenant_id();
    meta_cache.meta_.master_key_version_ = table_schema->get_master_key_id();
    if (OB_FAIL(meta_cache.meta_.encrypted_table_key_.set_content(
        table_schema->get_encrypt_key()))) {
      LOG_WARN("fail to assign encrypt key", K(ret));
    }
    #ifdef ERRSIM
      else if (OB_FAIL(OB_E(EventTable::EN_ENCRYPT_GET_MASTER_KEY_FAILED) OB_SUCCESS)) {
      LOG_WARN("ERRSIM, fail to get master key", K(ret));
    }
    #endif
      else if (OB_FAIL(share::ObMasterKeyGetter::get_master_key(table_schema->get_tenant_id(),
                       table_schema->get_master_key_id(), master_key, OB_MAX_MASTER_KEY_LENGTH,
                       master_key_length))) {
      LOG_WARN("fail to get master key", K(ret));
      // 如果在cg阶段获取主密钥失败了, 有可能是因为RS执行内部sql没有租户资源引起的.
      // 在没有租户资源的情况下获取主密钥, 获取加密租户配置项时会失败
      // 在这种情况下, 我们认为这里是合理的, 缓存中可以不保存主密钥内容
      // cg阶段获取主密钥的任何失败我们可以接受
      // 兜底是执行期再次获取, 再次获取成功了则继续往下走, 失败了则报错出来.
      // 见bug
      ret = OB_SUCCESS;
    } else if (OB_FAIL(meta_cache.meta_.master_key_.set_content(
                                                        ObString(master_key_length, master_key)))) {
      LOG_WARN("fail to assign master_key", K(ret));
    } else if (OB_FAIL(ObEncryptionUtil::decrypt_table_key(meta_cache.meta_))) {
      LOG_WARN("failed to decrypt_table_key", K(ret));
    } else {/*do nothing*/}

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(meta_array.push_back(meta_cache))) {
      LOG_WARN("fail to push back meta array", K(ret));
    }
  }
  return ret;
}
#endif
}  // namespace sql
}  // namespace oceanbase
