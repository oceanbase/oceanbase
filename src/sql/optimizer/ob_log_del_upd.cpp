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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/optimizer/ob_del_upd_log_plan.h"
#include "sql/optimizer/ob_log_del_upd.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/ob_smart_call.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_log_join.h"

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace common;

int IndexDMLInfo::deep_copy(ObIRawExprCopier &expr_copier, const IndexDMLInfo &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  loc_table_id_ = other.loc_table_id_;
  ref_table_id_ = other.ref_table_id_;
  index_name_ = other.index_name_;
  spk_cnt_ = other.spk_cnt_;
  rowkey_cnt_ = other.rowkey_cnt_;
  need_filter_null_ = other.need_filter_null_;
  is_primary_index_ = other.is_primary_index_;
  is_update_unique_key_ = other.is_update_unique_key_;
  is_update_part_key_ = other.is_update_part_key_;
  is_update_primary_key_ = other.is_update_primary_key_;
  assignments_.reset();
  if (OB_FAIL(expr_copier.copy(other.column_exprs_, column_exprs_))) {
    LOG_WARN("failed to assign column exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.column_convert_exprs_,
                                      column_convert_exprs_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.column_old_values_exprs_,
                                      column_old_values_exprs_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(assignments_.prepare_allocate(other.assignments_.count()))) {
    LOG_WARN("failed to prepare allocate assignment array", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.ck_cst_exprs_, ck_cst_exprs_))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(part_ids_.assign(other.part_ids_))) {
    LOG_WARN("failed to assign part ids", K(ret));
  } else if (OB_NOT_NULL(other.trans_info_expr_)) {
    if (OB_FAIL(expr_copier.copy(other.trans_info_expr_, trans_info_expr_))) {
      LOG_WARN("failed to trans info exprs", K(ret), KPC(other.trans_info_expr_));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.assignments_.count(); ++i) {
    if (OB_FAIL(assignments_.at(i).deep_copy(expr_copier,
                                             other.assignments_.at(i)))) {
      LOG_WARN("failed to deep copy assignment", K(ret));
    }
  }
  return ret;
}

int IndexDMLInfo::assign_basic(const IndexDMLInfo &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  loc_table_id_ = other.loc_table_id_;
  ref_table_id_ = other.ref_table_id_;
  index_name_ = other.index_name_;
  spk_cnt_ = other.spk_cnt_;
  rowkey_cnt_ = other.rowkey_cnt_;
  need_filter_null_ = other.need_filter_null_;
  is_primary_index_ = other.is_primary_index_;
  is_update_unique_key_ = other.is_update_unique_key_;
  is_update_part_key_ = other.is_update_part_key_;
  is_update_primary_key_ = other.is_update_primary_key_;
  trans_info_expr_ = other.trans_info_expr_;
  if (OB_FAIL(column_exprs_.assign(other.column_exprs_))) {
    LOG_WARN("failed to assign column exprs", K(ret));
  } else if (OB_FAIL(column_convert_exprs_.assign(other.column_convert_exprs_))) {
    LOG_WARN("failed to assign column conver array", K(ret));
  } else if (OB_FAIL(column_old_values_exprs_.assign(other.column_old_values_exprs_))) {
    LOG_WARN("failed to assign column old values exprs", K(ret));
  } else if (OB_FAIL(assignments_.assign(other.assignments_))) {
    LOG_WARN("failed to assign assignments array", K(ret));
  } else if (OB_FAIL(ck_cst_exprs_.assign(other.ck_cst_exprs_))) {
    LOG_WARN("failed to assign check constraint exprs", K(ret));
  } else if (OB_FAIL(part_ids_.assign(other.part_ids_))) {
    LOG_WARN("failed to assign part ids", K(ret));
  }
  return ret;
}

int IndexDMLInfo::assign(const ObDmlTableInfo &info)
{
  int ret = OB_SUCCESS;
  table_id_ = info.table_id_;
  loc_table_id_ = info.loc_table_id_;
  ref_table_id_ = info.ref_table_id_;
  index_name_ = info.table_name_;
  need_filter_null_ = info.need_filter_null_;
  if (OB_FAIL(column_exprs_.assign(info.column_exprs_))) {
    LOG_WARN("failed to assign column exprs", K(ret));
  } else if (OB_FAIL(ck_cst_exprs_.assign(info.check_constraint_exprs_))) {
    LOG_WARN("failed to assign expr", K(ret));
  } else if (OB_FAIL(part_ids_.assign(info.part_ids_))) {
    LOG_WARN("failed to assign part ids", K(ret));
  }
  return ret;
}

int64_t IndexDMLInfo::to_explain_string(char *buf, int64_t buf_len, ExplainType type) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  BUF_PRINTF("{");
  if (index_name_.empty()) {
    BUF_PRINTF("%lu: ", ref_table_id_);
  } else {
    pos += index_name_.to_string(buf + pos, buf_len - pos);
  }
  int64_t N = column_exprs_.count();
  BUF_PRINTF(": (");
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    if (NULL != column_exprs_.at(i)) {
      if (OB_SUCC(ret)) {
        if (OB_FAIL(column_exprs_.at(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("failed to get_name", K(ret));
        }
      }
      if (i < N - 1) {
        BUF_PRINTF(", ");
      }
    }
  }
  BUF_PRINTF(")");
  BUF_PRINTF("}");

  return pos;
}

int IndexDMLInfo::init_assignment_info(const ObAssignments &assignments,
                                       ObRawExprFactory &expr_factory)
{
  int ret = OB_SUCCESS;
  assignments_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
    if (has_exist_in_array(column_exprs_, assignments.at(i).column_expr_)) {
      //将更新表达式的index添加到index info中，表示该index跟assignment有关
      if (OB_FAIL(assignments_.push_back(assignments.at(i)))) {
        LOG_WARN("add assignment index to assign info failed", K(ret));
      }
    }
  }
  return ret;
}

int IndexDMLInfo::get_rowkey_exprs(ObIArray<ObColumnRefRawExpr *> &rowkey, bool need_spk) const
{
  int ret = OB_SUCCESS;
  rowkey.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < get_real_uk_cnt(); ++i) {
    if (OB_FAIL(rowkey.push_back(column_exprs_.at(i)))) {
      LOG_WARN("failed to push back rowkey expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && need_spk) {
    for (int64_t i = get_real_uk_cnt(); OB_SUCC(ret) && i < rowkey_cnt_; ++i) {
      if (i >= column_exprs_.count() || !is_shadow_column(column_exprs_.at(i)->get_column_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_exprs_ does not contain shadow primary key", K(ret), K(i), K(column_exprs_));
      } else if (OB_FAIL(rowkey.push_back(column_exprs_.at(i)))) {
        LOG_WARN("failed to push back rowkey expr", K(ret));
      }
    }
  }
  return ret;
}

int IndexDMLInfo::get_rowkey_exprs(ObIArray<ObRawExpr *> &rowkey, bool need_spk) const
{
  int ret = OB_SUCCESS;
  rowkey.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < get_real_uk_cnt(); ++i) {
    if (OB_FAIL(rowkey.push_back(column_exprs_.at(i)))) {
      LOG_WARN("failed to push back rowkey expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && need_spk) {
    for (int64_t i = get_real_uk_cnt(); OB_SUCC(ret) && i < rowkey_cnt_; ++i) {
      if (i >= column_exprs_.count() || !is_shadow_column(column_exprs_.at(i)->get_column_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_exprs_ does not contain shadow primary key", K(ret), K(i), K(column_exprs_));
      } else if (OB_FAIL(rowkey.push_back(column_exprs_.at(i)))) {
        LOG_WARN("failed to push back rowkey expr", K(ret));
      }
    }
  }
  return ret;
}

int IndexDMLInfo::init_column_convert_expr(const ObAssignments &assignments)
{
  // 将 col1 = expr1 这种表达式中的 expr1 放到 column_convert_exprs
  // 中，用于后面的插入操作
  int ret = OB_SUCCESS;
  int found = 0; // 在 assignment 中找到匹配表达式的个数
  column_convert_exprs_.reset();
  for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); ++i) {
    ObRawExpr *insert_expr = column_exprs_.at(i);
    // find_replacement_in_assignment
    for (int j = 0; OB_SUCC(ret) && j < assignments.count(); ++j) {
      if (insert_expr == assignments.at(j).column_expr_) {
        insert_expr = const_cast<ObRawExpr *>(assignments.at(j).expr_);
        found++;
        break;
      }
    }
    if (OB_ISNULL(insert_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else if (OB_FAIL(column_convert_exprs_.push_back(insert_expr))) {
      LOG_WARN("fail push back data", K(ret));
    }
  }
  if (OB_SUCC(ret) && found != assignments.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not all update asssigment found in insert target exprs",
             K(ret), K(found), K(assignments.count()), K(assignments));
  }
  return ret;
}

int IndexDMLInfo::convert_old_row_exprs(const ObIArray<ObColumnRefRawExpr*> &columns,
                                          ObIArray<ObRawExpr*> &access_exprs,
                                          int64_t col_cnt /*= -1*/)
{
  int ret = OB_SUCCESS;
  if (-1 == col_cnt) {
    col_cnt = columns.count();
  }
  if (col_cnt > 0 && col_cnt <= columns.count()) {
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(col_cnt), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
    ObColumnRefRawExpr *col_expr = const_cast<ObColumnRefRawExpr*>(columns.at(i));
    if (OB_FAIL(access_exprs.push_back(col_expr))) {
      LOG_WARN("store storage access expr failed", K(ret));
    }
  }
  return ret;
}

int IndexDMLInfo::generate_column_old_values_exprs()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(convert_old_row_exprs(column_exprs_, column_old_values_exprs_))) {
    LOG_WARN("convert old values exprs", K(ret), K(column_exprs_));
  }
  return ret;
}

int IndexDMLInfo::is_new_row_expr(const ObRawExpr *expr, bool &bret) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    bret = false;
  } else {
    bret = ObOptimizerUtil::find_item(column_convert_exprs_, expr)
        || ObOptimizerUtil::find_item(ck_cst_exprs_, expr)
        || lookup_part_id_expr_ == expr
        || new_part_id_expr_ == expr
        || new_rowid_expr_ == expr;
  }
  for (int64_t i = 0; OB_SUCC(ret) && !bret && i < assignments_.count(); ++i) {
    bret = assignments_.at(i).expr_ == expr;
  }
  return ret;
}

ObLogDelUpd::ObLogDelUpd(ObDelUpdLogPlan &plan)
  : ObLogicalOperator(plan),
    my_dml_plan_(plan),
    view_check_exprs_(NULL),
    table_partition_info_(NULL),
    stmt_id_expr_(nullptr),
    lock_row_flag_expr_(NULL),
    ignore_(false),
    is_returning_(false),
    is_multi_part_dml_(false),
    is_pdml_(false),
    gi_charged_(false),
    is_index_maintenance_(false),
    need_barrier_(false),
    is_first_dml_op_(false),
    table_location_uncertain_(false),
    is_pdml_update_split_(false),
    pdml_partition_id_expr_(NULL),
    pdml_is_returning_(false),
    err_log_define_(),
    need_alloc_part_id_expr_(false),
    has_instead_of_trigger_(false),
    produced_trans_exprs_()
{
}

int ObLogDelUpd::get_plan_item_info(PlanText &plan_text,
                                    ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get base plan item info", K(ret));
  } else {
    ObString base_table;
    ObString index_table;
    if (is_pdml()
        && is_index_maintenance()
        && NULL != get_index_dml_infos().at(0)
        && OB_SUCC(get_table_index_name(*get_index_dml_infos().at(0),
                                                base_table,
                                                index_table))) {
      BEGIN_BUF_PRINT;
      if (OB_FAIL(BUF_PRINTF("%.*s(%.*s)",
                             base_table.length(),
                             base_table.ptr(),
                             index_table.length(),
                             index_table.ptr()))) {
        LOG_WARN("failed to print str", K(ret));
      }
      END_BUF_PRINT(plan_item.object_alias_,
                    plan_item.object_alias_len_);
    }
  }
  return ret;
}

uint64_t ObLogDelUpd::hash(uint64_t seed) const
{
  seed = do_hash(is_multi_part_dml_, seed);
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogDelUpd::extract_err_log_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) ||
      OB_UNLIKELY(!get_stmt()->is_update_stmt() &&
                  !get_stmt()->is_delete_stmt() &&
                  !get_stmt()->is_insert_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(generate_errlog_info(
                       static_cast<const ObDelUpdStmt&>(*get_stmt()),
                       get_err_log_define()))) {
    LOG_WARN("failed to generate errlog info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogDelUpd::generate_errlog_info(const ObDelUpdStmt &stmt, ObErrLogDefine &errlog_define)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObColumnRefRawExpr*> &error_log_exprs = stmt.get_error_log_info().error_log_exprs_;
  ObSEArray<ObRawExpr *, 4> dml_columns;
  ObSEArray<ObRawExpr *, 4> dml_values;
  if (stmt.is_insert_stmt()) {
    const ObInsertTableInfo& insert_info = static_cast<const ObInsertStmt&>(stmt).get_insert_table_info();
    if (OB_FAIL(append(dml_columns, insert_info.column_exprs_))) {
      LOG_WARN("failed to append column expr", K(ret));
    } else if (OB_FAIL(append(dml_values, insert_info.column_conv_exprs_))) {
      LOG_WARN("failed to append column convert expr", K(ret));
    }
  } else if (stmt.is_update_stmt()) {
    const ObUpdateTableInfo* update_info = static_cast<const ObUpdateStmt&>(stmt).get_update_table_info().at(0);
    if (OB_ISNULL(update_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null update", K(ret));
    } else {
      const ObAssignments &assigns = update_info->assignments_;
      for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); ++i) {
        if (OB_FAIL(dml_columns.push_back(assigns.at(i).column_expr_))) {
          LOG_WARN("failed to push back assign column", K(ret));
        } else if (OB_FAIL(dml_values.push_back(assigns.at(i).expr_))) {
          LOG_WARN("failed to push back assign value", K(ret));
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < error_log_exprs.count(); ++i) {
    ObColumnRefRawExpr *col_expr = NULL;
    ObRawExpr *val_expr = NULL;
    int64_t idx = OB_INVALID_INDEX;
    if (OB_ISNULL(col_expr = error_log_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!ObOptimizerUtil::find_item(dml_columns, col_expr, &idx)) {
      val_expr = col_expr;
    } else if (OB_UNLIKELY(idx < 0 || idx >= dml_values.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), K(dml_columns.count()), K(dml_values.count()));
    } else {
      val_expr = dml_values.at(idx);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(errlog_define.err_log_value_exprs_.push_back(val_expr))) {
        LOG_WARN("failed to push back errlog expr value", K(ret));
      } else if (OB_FAIL(errlog_define.err_log_column_names_.push_back(col_expr->get_column_name()))) {
        LOG_WARN("failed to push back column name", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    errlog_define.is_err_log_ = stmt.is_error_logging();
    errlog_define.err_log_database_name_ = stmt.get_err_log_database_name();
    errlog_define.err_log_table_name_ = stmt.get_err_log_table_name();
    errlog_define.reject_limit_ = stmt.get_err_log_reject_limit();
  }
  return ret;
}

int ObLogDelUpd::allocate_granule_pre(AllocGIContext &ctx)
{
  return pw_allocate_granule_pre(ctx);
}

int ObLogDelUpd::allocate_granule_post(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  bool is_partition_wise_state = ctx.is_in_partition_wise_state();
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(pw_allocate_granule_post(ctx))){ // 分配完GI以后，会对ctx的状态进行清理
    LOG_WARN("failed to allocate pw gi post", K(ret));
  } else {
    if (is_partition_wise_state && ctx.is_op_set_pw(this)) {
      if (get_type() == log_op_def::LOG_UPDATE || // UPDATE: UPDATE, UPDATE RETURNING
          get_type() == log_op_def::LOG_DELETE || // DELETE: DELETE, DELETE RETURNING
          get_type() == log_op_def::LOG_INSERT || // INSERT: INSERT, INSERT UPDATE, REPLACE
          get_type() == log_op_def::LOG_MERGE) { // FOR UPDATE: FOR UPDATE
        ObSEArray<ObLogicalOperator *, 2> tsc_ops;
        if (OB_FAIL(find_all_tsc(tsc_ops, this))) {
          LOG_WARN("failed to find all tsc", K(ret));
        } else if (tsc_ops.count() < 1){
          // do nothing
          set_gi_above(true);
        } else {
          // tsc op与当前dml算子都需要set gi above
          ARRAY_FOREACH(tsc_ops, idx) {
            ObLogTableScan *tsc_op = static_cast<ObLogTableScan*>(tsc_ops.at(idx));
            tsc_op->set_gi_above(true);
          }
          set_gi_above(true);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("op type doesn't support gi", K(ret), K(get_name()));
      }
    } else {
      LOG_TRACE("not allocate dml gi for px",
        K(ret), K(ctx), K(ctx.is_op_set_pw(this)));
    }
  }
  return ret;
}

int ObLogDelUpd::generate_pdml_partition_id_expr()
{
  int ret = OB_SUCCESS;
  // pdml 分配 partition id expr
  // 1. 如果当前pdml op对应的表是非分区表，就不分配partition id expr
  // 2. 如果当前pdml op对应的表是分区表，就分配partition id expr
  uint64_t table_id = OB_INVALID_ID;
  ObOpPseudoColumnRawExpr *partition_id_expr = nullptr;
  ObLogExchange *producer = NULL;
  ObLogTableScan *src_tsc = NULL;
  if (OB_UNLIKELY(!is_pdml()) ||
      OB_UNLIKELY(index_dml_infos_.count() != 1) ||
      OB_ISNULL(index_dml_infos_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index info array is empty", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::generate_pseudo_partition_id_expr(partition_id_expr))) {
    LOG_WARN("fail allocate part id expr", K(table_id), K(ret));
  } else if (OB_FAIL(find_pdml_part_id_producer(get_child(ObLogicalOperator::first_child),
                                                index_dml_infos_.at(0)->loc_table_id_,
                                                index_dml_infos_.at(0)->ref_table_id_,
                                                producer,
                                                src_tsc))) {
    LOG_WARN("find pdml partition id expr producer failed", K(ret));
  } else if (NULL != src_tsc && NULL != producer) {
    pdml_partition_id_expr_ = partition_id_expr;
    producer->set_partition_id_expr(partition_id_expr);
  } else if (NULL != src_tsc) {
    pdml_partition_id_expr_ = partition_id_expr;
    src_tsc->set_tablet_id_expr(partition_id_expr);
  } else if (NULL != producer) {
    pdml_partition_id_expr_ = partition_id_expr;
    producer->set_partition_id_expr(partition_id_expr);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not found pdml partition id expr producer", K(ret), K(producer), K(src_tsc));
  }
  return ret;
}

int ObLogDelUpd::find_pdml_part_id_producer(ObLogicalOperator *op,
                                            const uint64_t loc_tid,
                                            const uint64_t ref_tid,
                                            ObLogExchange *&producer,
                                            ObLogTableScan *&src_tsc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(op));
  } else if (op->is_dml_operator()) {
    // for pdml insert split by update, generate partition id by exchange above delete
  } else if (op->get_type() == log_op_def::LOG_TABLE_SCAN) {
    // PDML partition id expr在table scan分配的逻辑
    // pdml table scan分配partition id expr的producer
    // table scan中分配partition id expr的producer的逻辑比较特殊：
    //  分配partition id的时候，需要保证partition id expr对应的table id与table
    // scan的table id是相同的
    //  对应insert的dml操作，例如：insert into t1 select from t1，
    //  产生的计划如下：
    //      insert
    //        subplan
    //          GI
    //            TSC
    //            ....
    //
    // 这种情况下，如果给TSC算子分配partition idexpr，那么根据表达式分配的框架，
    // 其会被裁剪掉，因此目前insert与subplan之间会添加一个EX算子.
    // 后期会进行优化，如果insert与subplan是一个full partition wise
    // join，那么就在insert算子上分配一个GI算子，目前先使用在subplan上分配EX算子的方式实现
    ObLogTableScan *tsc = static_cast<ObLogTableScan*>(op);
    if (loc_tid == tsc->get_table_id() &&
        ref_tid == (tsc->get_is_index_global() ? tsc->get_index_table_id() : tsc->get_ref_table_id())) {
      src_tsc = tsc;
      producer = NULL;
    }
  } else {
    if (OB_SUCC(ret) && NULL == producer && op->get_type() == log_op_def::LOG_EXCHANGE
        && static_cast<ObLogExchange*>(op)->is_producer()) {
      // find the first exchange below dml, use this exchange generate partition id for pdml insert
      producer = static_cast<ObLogExchange*>(op);
    }
    for (int64_t i = 0; OB_SUCC(ret) && NULL == src_tsc && i < op->get_num_of_child(); i++) {
      if (OB_FAIL(SMART_CALL(find_pdml_part_id_producer(op->get_child(i), loc_tid, ref_tid, producer, src_tsc)))) {
        LOG_WARN("find pdml part id producer failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && NULL != src_tsc && op->get_type() == log_op_def::LOG_EXCHANGE
        && static_cast<ObLogExchange*>(op)->is_producer()) {
      // generate partition id by exchange above dml target table scan
      producer = static_cast<ObLogExchange*>(op);
    }
  }
  return ret;
}

int ObLogDelUpd::find_trans_info_producer() {
  int ret = OB_SUCCESS;
  for (int64_t i = 0 ; OB_SUCC(ret) && i < index_dml_infos_.count(); i++) {
    ObLogicalOperator *producer = NULL;
    IndexDMLInfo *index_dml_info = index_dml_infos_.at(i);
    if (OB_ISNULL(index_dml_info)) {
      ret = OB_ERR_UNEXPECTED;
    } else if ((!is_pdml() && !index_dml_info->is_primary_index_)) {
      // Don't worry about non-pdml and non-main tables
      // Every operator in pdml needs to try to press down once
    } else if (OB_ISNULL(index_dml_info->trans_info_expr_)) {
      // do nothing
    } else if (OB_FAIL(find_trans_info_producer(*this, index_dml_info->table_id_, producer))) {
      LOG_WARN("fail to find trans info producer", K(ret), KPC(index_dml_info), K(get_name()));
    } else if (NULL == producer) {
      // No error can be reported here,
      // the producer of the corresponding trans_info expression was not found, ignore these
      LOG_TRACE("can not found trans debug info expr producer", K(ret), K(index_dml_info->table_id_));
    } else if (OB_FAIL(add_var_to_array_no_dup(produced_trans_exprs_,
                                               index_dml_info->trans_info_expr_))) {
      LOG_WARN("fail to push trans_info_expr_", K(ret));
    } else {
      if (producer->get_type() == log_op_def::LOG_TABLE_SCAN) {
        if (static_cast<ObLogTableScan *>(producer)->get_trans_info_expr() == index_dml_info->trans_info_expr_) {
          LOG_DEBUG("this expr has find the producer", K(ret));
        } else {
          static_cast<ObLogTableScan *>(producer)->
                      set_trans_info_expr(static_cast<ObOpPseudoColumnRawExpr *>(index_dml_info->trans_info_expr_));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type of pdml partition id producer", K(ret), K(producer));
      }
    }
  }
  return ret;
}

int ObLogDelUpd::find_trans_info_producer(ObLogicalOperator &op,
                                          const uint64_t tid,
                                          ObLogicalOperator *&producer)
{
  int ret = OB_SUCCESS;
  producer = NULL;
  if (op.get_type() == log_op_def::LOG_TABLE_SCAN) {
    ObLogTableScan &tsc = static_cast<ObLogTableScan &>(op);
    if (tid == tsc.get_table_id()) {
      producer = &op;
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL == producer && i < op.get_num_of_child(); i++) {
    if (OB_ISNULL(op.get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null child", K(ret));
    } else if (log_op_def::LOG_JOIN == op.get_type()) {
      ObLogJoin &join_op = static_cast<ObLogJoin&>(op);
      if (IS_LEFT_SEMI_ANTI_JOIN(join_op.get_join_type()) &&
          second_child == i) {
        continue;
      } else if (IS_RIGHT_SEMI_ANTI_JOIN(join_op.get_join_type()) &&
                 first_child == i) {
        continue;
      }
      if (OB_FAIL(SMART_CALL(find_trans_info_producer(*op.get_child(i), tid, producer)))) {
        LOG_WARN("find pdml part id producer failed", K(ret));
      }
    } else if (OB_FAIL(SMART_CALL(find_trans_info_producer(*op.get_child(i), tid, producer)))) {
      LOG_WARN("find pdml part id producer failed", K(ret));
    }
  }
  return ret;
}

int ObLogDelUpd::inner_get_op_exprs(ObIArray<ObRawExpr*> &all_exprs, bool need_column_expr)
{
  int ret = OB_SUCCESS;
  if (is_multi_part_dml() && OB_FAIL(generate_multi_part_partition_id_expr())) {
    LOG_WARN("failed to generate update expr", K(ret));
  } else if (is_pdml() && need_alloc_part_id_expr_ &&
             OB_FAIL(generate_pdml_partition_id_expr())) {
    LOG_WARN("failed to allocate partition id expr", K(ret));
  } else if (OB_FAIL(find_trans_info_producer())) {
    LOG_WARN("failed to find trasn info producer", K(ret));
  } else if (OB_FAIL(generate_rowid_expr_for_trigger())) {
    LOG_WARN("failed to try add rowid col expr for trigger", K(ret));
  } else if (OB_FAIL(generate_part_id_expr_for_foreign_key(all_exprs))) {
    LOG_WARN("failed to generate part expr for foreign key", K(ret));
  } else if (NULL != lock_row_flag_expr_ && OB_FAIL(all_exprs.push_back(lock_row_flag_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(append(all_exprs, view_check_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (NULL != pdml_partition_id_expr_ && OB_FAIL(all_exprs.push_back(pdml_partition_id_expr_))) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_exprs, produced_trans_exprs_))) {
    LOG_WARN("failed to push back exprs", K(ret), K(produced_trans_exprs_));
  } else if (OB_FAIL(get_table_columns_exprs(get_index_dml_infos(), all_exprs, need_column_expr))) {
    LOG_WARN("failed to add table columns to ctx", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else if (get_plan()->get_optimizer_context().is_batched_multi_stmt()) {
    const ObDelUpdStmt *upd_stmt = static_cast<const ObDelUpdStmt*>(get_stmt());
    if (OB_ISNULL(upd_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(stmt_id_expr_ = upd_stmt->get_ab_stmt_id_expr())) {
      // new engine, stmt_id_expr_ come from update_stmt/insert_stmt, allocated by transformer
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt id expr is null", K(ret));
    } else if (OB_FAIL(all_exprs.push_back(upd_stmt->get_ab_stmt_id_expr()))) {
      LOG_WARN("add stmt id expr to all exprs failed", K(ret));
    }
  }
  return ret;
}

int ObLogDelUpd::get_table_columns_exprs(const ObIArray<IndexDMLInfo *> &index_dml_infos,
                                         ObIArray<ObRawExpr*> &all_exprs,
                                         bool need_column_expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> dml_columns;
  ObSEArray<ObRawExpr *, 16> dml_values;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  } else {
    need_column_expr |= get_plan()->get_optimizer_context().is_online_ddl();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); ++i) {
    const IndexDMLInfo *index_dml_info = index_dml_infos.at(i);
    dml_columns.reuse();
    dml_values.reuse();
    if (OB_ISNULL(index_dml_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    //for insert index info, no need add column exprs.
    } else if (need_column_expr &&
               OB_FAIL(append(all_exprs, index_dml_info->column_exprs_))) {
      LOG_WARN("failed to add all_exprs to ctx", K(ret));
    } else if (OB_FAIL(append_array_no_dup(all_exprs, index_dml_info->column_convert_exprs_))) {
      LOG_WARN("failed to append all_exprs", K(ret));
    } else if (OB_FAIL(get_update_exprs(*index_dml_info, dml_columns, dml_values))) {
      LOG_WARN("failed to get update all_exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(all_exprs, dml_columns))) {
      LOG_WARN("failed to append array no dup", K(ret));
    } else if (OB_FAIL(append_array_no_dup(all_exprs, dml_values))) {
      LOG_WARN("failed to add update all_exprs to context", K(ret));
    } else if (OB_FAIL(append_array_no_dup(all_exprs, index_dml_info->ck_cst_exprs_))) {
      LOG_WARN("failed to append check constraint all_exprs", K(ret));
    } else if (NULL != index_dml_infos.at(i)->old_part_id_expr_ &&
               OB_FAIL(all_exprs.push_back(index_dml_info->old_part_id_expr_))) {
      LOG_WARN("failed to push back old partition id expr", K(ret));
    } else if (NULL != index_dml_infos.at(i)->new_part_id_expr_ &&
               OB_FAIL(all_exprs.push_back(index_dml_info->new_part_id_expr_))) {
      LOG_WARN("failed to push back new parititon id expr", K(ret));
    } else if (NULL != index_dml_infos.at(i)->old_rowid_expr_ &&
               OB_FAIL(all_exprs.push_back(index_dml_info->old_rowid_expr_))) {
      LOG_WARN("failed to push back old rowid expr", K(ret));
    } else if (NULL != index_dml_infos.at(i)->new_rowid_expr_ &&
               OB_FAIL(all_exprs.push_back(index_dml_info->new_rowid_expr_))) {
      LOG_WARN("failed to push back new rowid expr", K(ret));
    } else if (NULL != index_dml_infos.at(i)->lookup_part_id_expr_ &&
               OB_FAIL(all_exprs.push_back(index_dml_info->lookup_part_id_expr_))) {
      LOG_WARN("failed to push back lookup part id expr", K(ret));
    } else if (need_column_expr) {
      ObColumnRefRawExpr *column_expr = NULL;
      for (int64_t k = 0; OB_SUCC(ret) && k < index_dml_info->column_exprs_.count(); k++) {
        if (OB_ISNULL(column_expr = index_dml_info->column_exprs_.at(k))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(column_expr), K(ret));
        } else if (is_shadow_column(column_expr->get_column_id()) &&
                   NULL != column_expr->get_dependant_expr() &&
                   OB_FAIL(add_var_to_array_no_dup(all_exprs, column_expr->get_dependant_expr()))) {
          LOG_WARN("failed to add column expr", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}


int ObLogDelUpd::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.expr_producers_.count(); i++) {
    ExprProducer expr_producer = ctx.expr_producers_.at(i);
    if (expr_producer.producer_id_ == id_ && expr_producer.expr_->is_column_ref_expr() &&
        expr_producer.producer_branch_ == OB_INVALID_ID) {
      if (OB_FAIL(mark_expr_produced(const_cast<ObRawExpr*>(expr_producer.expr_), branch_id_, id_, ctx))) {
        LOG_WARN("failed to mark expr produced", K(ret));
      } else { /*do nothing*/ }
    }
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
    LOG_WARN("failed to allocate expr post", K(ret));
  } else { /*do nothing*/}
  return ret;
}

// table id for location lookup.
uint64_t ObLogDelUpd::get_loc_table_id() const
{
  uint64_t tid = common::OB_INVALID_ID;
  if (!get_index_dml_infos().empty() &&
      NULL != get_index_dml_infos().at(0)) {
    tid = get_index_dml_infos().at(0)->loc_table_id_;
  }
  return tid;
}

uint64_t ObLogDelUpd::get_index_tid() const
{
  uint64_t index_tid = common::OB_INVALID_ID;
  if (!get_index_dml_infos().empty() &&
      NULL != get_index_dml_infos().at(0)) {
    index_tid = get_index_dml_infos().at(0)->ref_table_id_;
  }
  return index_tid;
}

uint64_t ObLogDelUpd::get_table_id() const
{
  uint64_t table_id = common::OB_INVALID_ID;
  if (!get_index_dml_infos().empty() &&
      NULL != get_index_dml_infos().at(0)) {
    table_id = get_index_dml_infos().at(0)->table_id_;
  }
  return table_id;
}

int ObLogDelUpd::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(child), K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    card_ = child->get_card();
    op_cost_ = ObOptEstCost::cost_get_rows(child->get_card(), opt_ctx);
    cost_ = op_cost_ + child->get_cost();
  }
  return ret;
}

int ObLogDelUpd::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_FAIL(ObLogicalOperator::compute_sharding_info())) {
    LOG_WARN("failed to compute sharding info", K(ret));
  } else if (OB_ISNULL(get_sharding())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!is_pdml()) {
    is_partition_wise_ = !is_multi_part_dml_ && !child->is_exchange_allocated() &&
                         get_sharding()->is_distributed() &&
                         NULL != get_sharding()->get_phy_table_location_info();
  }
  return ret;
}

int ObLogDelUpd::check_has_trigger(uint64_t tid, bool &has_trg)
{
  int ret = OB_SUCCESS;
  has_trg = false;
  ObSchemaGetterGuard *schema_guard = NULL;
  ObSQLSessionInfo *session_info = NULL;
  const ObTableSchema *tbl_schema = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_schema_guard()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(
             session_info->get_effective_tenant_id(),
             tid, tbl_schema))) {
    LOG_WARN("get table schema failed", K(ret));
  } else if (OB_ISNULL(tbl_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (tbl_schema->get_trigger_list().count() > 0) {
    has_trg = true;
  }
  return ret;
}

int ObLogDelUpd::build_rowid_expr(uint64_t table_id,
                                  uint64_t table_ref_id,
                                  const ObIArray<ObRawExpr*> &rowkeys,
                                  ObRawExpr *&rowid_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *rowid_sysfun_expr = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  ObSQLSessionInfo *session_info = NULL;
  const ObTableSchema *tbl_schema = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_schema_guard()) ||
      OB_ISNULL(get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid params", K(ret), KP(schema_guard), KP(get_plan()));
  } else if (OB_FAIL(schema_guard->get_table_schema(
             session_info->get_effective_tenant_id(),
             table_ref_id, tbl_schema))) {
    LOG_WARN("get_table_schema failed", K(ret), K(table_ref_id));
  } else if (OB_ISNULL(tbl_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_rowid_expr(get_stmt(),
                                                      get_plan()->get_optimizer_context().get_expr_factory(),
                                                      get_plan()->get_optimizer_context().get_allocator(),
                                                      *(get_plan()->get_optimizer_context().get_session_info()),
                                                      *tbl_schema,
                                                      table_id,
                                                      rowkeys,
                                                      rowid_sysfun_expr))) {
    LOG_WARN("failed to build rowid col expr", K(ret));
  } else if (OB_ISNULL(rowid_sysfun_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowid sysfun_expr is NULL", K(ret));
  } else {
    rowid_expr = rowid_sysfun_expr;
  }
  return ret;
}

int ObLogDelUpd::get_rowid_version(int64_t &rowid_version)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard *schema_guard = NULL;
  ObSQLSessionInfo *session_info = NULL;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<ObRawExpr*, 4> rowkey_exprs;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema or plan is NULL", K(ret), KP(schema_guard), KP(get_plan()));
  } else if (OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_schema_guard()) ||
             OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema guard", K(ret), K(schema_guard), K(session_info));
  } else if (OB_FAIL(schema_guard->get_table_schema(
             session_info->get_effective_tenant_id(),
             get_index_tid(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(get_index_tid()));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else {
    ObSEArray<uint64_t, 4> col_ids;
    int64_t rowkey_cnt = 0;
    OZ(table_schema->get_column_ids_serialize_to_rowid(col_ids, rowkey_cnt));
    OZ(table_schema->get_rowid_version(rowkey_cnt, col_ids.count(), rowid_version));
    LOG_DEBUG("get get_rowid_version is", K(rowid_version));
  }
  return ret;
}

int ObLogDelUpd::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  get_op_ordering().reset();
  is_local_order_ = false;
  return ret;
}

int ObLogDelUpd::compute_plan_type()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::compute_plan_type())) {
    LOG_WARN("failed to compute plan type", K(ret));
  } else if (is_multi_part_dml()) {
    location_type_ = ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN;
  } else { /*do nothing*/ }
  return ret;
}

int ObLogDelUpd::get_table_location_type(ObTableLocationType &type)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext& optimizer_context = get_plan()->get_optimizer_context();
  ObAddr &server = optimizer_context.get_local_server_addr();
  type = OB_TBL_LOCATION_UNINITIALIZED;
  if (OB_ISNULL(table_partition_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(table_partition_info_->get_location_type(server, type))) {
    LOG_WARN("get location type failed", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogDelUpd::assign_dml_infos(const ObIArray<IndexDMLInfo *> &index_dml_infos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(index_dml_infos_.assign(index_dml_infos))) {
    LOG_WARN("failed to assign index dml infos", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); ++i) {
    if (OB_ISNULL(index_dml_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index dml info is null", K(ret));
    } else if (index_dml_infos.at(i)->is_primary_index_ &&
               OB_FAIL(loc_table_list_.push_back(index_dml_infos.at(i)->loc_table_id_))) {
      LOG_WARN("failed to add loc table id", K(ret));
    }
  }
  return ret;
}

int ObLogDelUpd::get_index_dml_infos(uint64_t loc_table_id,
                                     ObIArray<const IndexDMLInfo *> &index_infos) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos_.count(); ++i) {
    if (OB_ISNULL(index_dml_infos_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dml index info is null", K(ret));
    } else if (index_dml_infos_.at(i)->loc_table_id_ != loc_table_id) {
      // do nothing
    } else if (OB_FAIL(index_infos.push_back(index_dml_infos_.at(i)))) {
      LOG_WARN("failed to push back dml index info", K(ret));
    }
  }
  return ret;
}

int ObLogDelUpd::get_index_dml_infos(uint64_t loc_table_id,
                                     ObIArray<IndexDMLInfo *> &index_infos)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos_.count(); ++i) {
    if (OB_ISNULL(index_dml_infos_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dml index info is null", K(ret));
    } else if (index_dml_infos_.at(i)->loc_table_id_ != loc_table_id) {
      // do nothing
    } else if (OB_FAIL(index_infos.push_back(index_dml_infos_.at(i)))) {
      LOG_WARN("failed to push back dml index info", K(ret));
    }
  }
  return ret;
}

IndexDMLInfo* ObLogDelUpd::get_primary_dml_info()
{
  IndexDMLInfo *ret = NULL;
  if (1 == loc_table_list_.count()) {
    ret = get_primary_dml_info(loc_table_list_.at(0));
  }
  return ret;
}

const IndexDMLInfo* ObLogDelUpd::get_primary_dml_info() const
{
  const IndexDMLInfo *ret = NULL;
  if (1 == loc_table_list_.count()) {
    ret = get_primary_dml_info(loc_table_list_.at(0));
  }
  return ret;
}

IndexDMLInfo* ObLogDelUpd::get_primary_dml_info(uint64_t loc_table_id)
{
  IndexDMLInfo *ret = NULL;
  for (int64_t i = 0; i < index_dml_infos_.count(); ++i) {
    if (NULL == index_dml_infos_.at(i)) {
      // do nothing
    } else if (index_dml_infos_.at(i)->loc_table_id_ == loc_table_id &&
               index_dml_infos_.at(i)->is_primary_index_) {
      ret = index_dml_infos_.at(i);
      break;
    }
  }
  return ret;
}

const IndexDMLInfo* ObLogDelUpd::get_primary_dml_info(uint64_t loc_table_id) const
{
  const IndexDMLInfo *ret = NULL;
  for (int64_t i = 0; i < index_dml_infos_.count(); ++i) {
    if (NULL == index_dml_infos_.at(i)) {
      // do nothing
    } else if (index_dml_infos_.at(i)->loc_table_id_ == loc_table_id &&
               index_dml_infos_.at(i)->is_primary_index_) {
      ret = index_dml_infos_.at(i);
      break;
    }
  }
  return ret;
}

const ObIArray<uint64_t>& ObLogDelUpd::get_table_list() const
{
  return loc_table_list_;
}

int ObLogDelUpd::get_table_index_name(const IndexDMLInfo &index_info,
                                      ObString &table_name,
                                      ObString &index_name)
{
  int ret = OB_SUCCESS;
  const ObDelUpdStmt *stmt = NULL;
  const TableItem* table_item = nullptr;
  ObSEArray<const ObDmlTableInfo*, 2> table_infos;
  if (OB_ISNULL(stmt = static_cast<const ObDelUpdStmt *>(get_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get primary dml info", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_dml_table_infos(table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  } else {
    index_name = index_info.index_name_;
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < table_infos.count(); ++i) {
      const ObDmlTableInfo* table_info = table_infos.at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table info", K(ret));
      } else if (table_info->loc_table_id_ != index_info.loc_table_id_) {
        // do nothing
      } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(table_info->table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table item", K(ret));
      } else {
        table_name = table_item->get_table_name();
        find = true;
      }
    }
  }
  return ret;
}

int ObLogDelUpd::print_table_infos(const ObString &prefix,
                                   char *buf,
                                   int64_t &buf_len,
                                   int64_t &pos,
                                   ExplainType type)
{
  int ret = OB_SUCCESS;
  const ObIArray<IndexDMLInfo *> &index_dml_infos = get_index_dml_infos();
  // pre check validity
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); ++i) {
    if (OB_ISNULL(index_dml_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index dml info is null", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    pos += prefix.to_string(buf + pos, buf_len - pos);
    BUF_PRINTF("(");
    if (index_dml_infos.count() == 0) {
      BUF_PRINTF("nil");
    } else {
      for (int64_t i = 0; i < index_dml_infos.count(); ++i) {
        const IndexDMLInfo *dml_info = index_dml_infos.at(i);
        ObString table_name;
        ObString index_name;
        ret = get_table_index_name(*dml_info, table_name, index_name);
        BUF_PRINTF("[");

        BUF_PRINTF("{");
        pos += table_name.to_string(buf + pos, buf_len - pos);
        BUF_PRINTF(": (");
        int64_t j = i + 1;
        for (; j < index_dml_infos.count() &&
             index_dml_infos.at(j)->loc_table_id_ == dml_info->loc_table_id_; ++j);
        for (; i < j; ++i) {
          pos += index_dml_infos.at(i)->to_explain_string(buf + pos, buf_len - pos, type);
          BUF_PRINTF(", ");
        }
        --i;
        pos -= 2;
        BUF_PRINTF(")");
        if (T_MERGE_DISTINCT == dml_info->distinct_algo_) {
          BUF_PRINTF(", merge_distinct");
        } else if (T_HASH_DISTINCT == dml_info->distinct_algo_) {
          BUF_PRINTF(", hash_distinct");
        }
        BUF_PRINTF("}");

        BUF_PRINTF("]");
        if (i < index_dml_infos.count() - 1) {
          BUF_PRINTF(", ");
        }
      }
    }
    BUF_PRINTF(")");
  }
  return ret;
}

int ObLogDelUpd::print_assigns(const ObAssignments &assigns,
                               char *buf,
                               int64_t &buf_len,
                               int64_t &pos,
                               ExplainType type)
{
  int ret = OB_SUCCESS;
  int64_t N = assigns.count();
  if (N == 0) {
    if (OB_FAIL(BUF_PRINTF("nil"))) {
      LOG_WARN("BUG_PRINTF fails", K(ret));
    } else { /* Do nothing */ }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (OB_FAIL(BUF_PRINTF("["))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else if (OB_ISNULL(assigns.at(i).column_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("variable_ or variable_->expr_ is NULL", K(ret));
      } else if (OB_FAIL(assigns.at(i).column_expr_
                         ->get_name(buf, buf_len, pos, type))) {
        LOG_WARN("get_name fails", K(ret));
      } else if(OB_FAIL(BUF_PRINTF("="))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else if (OB_ISNULL(assigns.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr_ is NULL", K(ret));
      } else if (OB_FAIL(assigns.at(i).expr_
                         ->get_name(buf, buf_len, pos, type))) {
        LOG_WARN("get_name fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("]"))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
      if (OB_SUCCESS == ret && i < N - 1) {
        if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("BUG_PRINTF fails", K(ret));
        } else { /* Do nothing */ }
      } else { /* Do nothing */ }
    }
  }
  return ret;
}

const common::ObIArray<ObColumnRefRawExpr *>* ObLogDelUpd::get_table_columns() const
{
  const IndexDMLInfo *index_info = get_primary_dml_info();
  const common::ObIArray<ObColumnRefRawExpr *> *ret = NULL;
  if (NULL != index_info) {
    ret = &index_info->column_exprs_;
  }
  return ret;
}

int ObLogDelUpd::generate_old_rowid_expr(IndexDMLInfo &table_dml_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> rowkey_exprs;
  if (OB_FAIL(table_dml_info.get_rowkey_exprs(rowkey_exprs))) {
    LOG_WARN("failed to get rowkey exprs", K(ret));
  } else if (OB_FAIL(build_rowid_expr(table_dml_info.loc_table_id_,
                                      table_dml_info.ref_table_id_,
                                      rowkey_exprs,
                                      table_dml_info.old_rowid_expr_))) {
    LOG_WARN("failed to build rowid expr", K(ret));
  }
  return ret;
}

int ObLogDelUpd::generate_update_new_rowid_expr(IndexDMLInfo &table_dml_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> update_columns;
  ObSEArray<ObRawExpr *, 4> update_values;
  if (OB_FAIL(get_update_exprs(table_dml_info, update_columns, update_values))) {
    LOG_WARN("failed to get update exprs", K(ret));
  } else if (OB_FAIL(convert_expr_by_dml_operation(update_columns,
                                                   update_values,
                                                   table_dml_info.old_rowid_expr_,
                                                   table_dml_info.new_rowid_expr_))) {
    LOG_WARN("failed to convert expr by dml operation", K(ret));
  }
  return ret;
}

int ObLogDelUpd::generate_insert_new_rowid_expr(IndexDMLInfo &table_dml_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> insert_columns;
  ObSEArray<ObRawExpr *, 4> insert_values;
  if (OB_FAIL(get_insert_exprs(table_dml_info, insert_columns, insert_values))) {
    LOG_WARN("failed to get insert exprs", K(ret));
  } else if (OB_FAIL(convert_expr_by_dml_operation(insert_columns,
                                                   insert_values,
                                                   table_dml_info.old_rowid_expr_,
                                                   table_dml_info.new_rowid_expr_))) {
    LOG_WARN("failed to convert expr by dml operation", K(ret));
  } else {
    // insert does not have old rowid expr
    table_dml_info.old_rowid_expr_ = NULL;
  }
  return ret;
}

int ObLogDelUpd::generate_old_calc_partid_expr(IndexDMLInfo &index_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null", K(ret));
  } else if (OB_FAIL(get_plan()->gen_calc_part_id_expr(index_info.loc_table_id_,
                                                      index_info.ref_table_id_,
                                                      CALC_PARTITION_TABLET_ID,
                                                      index_info.old_part_id_expr_))) {
    LOG_WARN("failed to gen calc part id expr", K(ret));
  }
  return ret;
}

// for replace and insert_up conflict scene, generate lookup_part_id_expr.
int ObLogDelUpd::generate_lookup_part_id_expr(IndexDMLInfo &index_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null", K(ret));
  } else if (OB_FAIL(get_plan()->gen_calc_part_id_expr(index_info.loc_table_id_,
                                                      index_info.ref_table_id_,
                                                      CALC_PARTITION_TABLET_ID,
                                                      index_info.lookup_part_id_expr_))) {
    LOG_WARN("failed to gen calc part id expr", K(ret));
  }
  return ret;
}

int ObLogDelUpd::generate_fk_lookup_part_id_expr(IndexDMLInfo &index_dml_info)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = index_dml_info.ref_table_id_;
  ObLogPlan *log_plan = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  ObSQLSessionInfo *session_info = NULL;
  const ObIArray<ObForeignKeyInfo> *fk_infos = NULL;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(log_plan = get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema or plan is NULL", K(ret), KP(schema_guard), KP(get_plan()));
  } else if (OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_schema_guard()) ||
             OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema guard", K(ret), K(schema_guard), K(session_info));
  } else if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                    index_dml_info.ref_table_id_,
                                                    table_schema))) {
    LOG_WARN("failed to get table schema", K(index_dml_info.ref_table_id_), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(index_dml_info.ref_table_id_), K(ret));
  } else if (!table_schema->is_user_table()) {
    // do nothing, especially for global index.
    LOG_DEBUG("skip generate partition key for foreign key",
              "table_name", table_schema->get_table_name_str(),
              "table_type", table_schema->get_table_type());
  } else if (OB_ISNULL(fk_infos = &table_schema->get_foreign_key_infos())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("foreign key infos is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < fk_infos->count(); i++) {
      const ObForeignKeyInfo &fk_info = fk_infos->at(i);
      ObRawExpr* fk_scan_part_expr = nullptr;
      if (fk_info.table_id_ != fk_info.child_table_id_ || fk_info.is_parent_table_mock_) {
        // update parent table, check child table, don't use das task to perform foreign key check
        ret = index_dml_info.fk_lookup_part_id_expr_.push_back(fk_scan_part_expr);
      } else {
        const uint64_t parent_table_id = fk_info.parent_table_id_;
        const ObTableSchema *parent_table_schema = NULL;
        uint64_t scan_index_tid = OB_INVALID_ID;
        if (OB_FAIL(schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                                   parent_table_id,
                                                   parent_table_schema))) {
          LOG_WARN("failed to get table schema of parent table", K(ret), K(parent_table_id));
        } else if (OB_ISNULL(parent_table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("parent table not exist", K(ret), K(parent_table_id));
        } else if (OB_FAIL(parent_table_schema->get_fk_check_index_tid(*schema_guard, fk_info.parent_column_ids_, scan_index_tid))) {
          LOG_WARN("failed to get index tid used to build scan das task for foreign key checks", K(ret));
        } else if (OB_INVALID_ID == scan_index_tid) {
          ret = OB_ERR_CANNOT_ADD_FOREIGN;
          LOG_WARN("get invalid table id to build das scan task for foreign key checks", K(ret));
        } else {
          ObRawExpr* fk_look_up_part_id_expr = nullptr;
          const ObTableSchema* scan_table_schema = nullptr;
          if (schema_guard->get_table_schema(session_info->get_effective_tenant_id(),
                                             scan_index_tid,
                                             scan_table_schema)) {
            LOG_WARN("failed to get scan table schema to perform foreign key check", K(ret), K(scan_index_tid));
          } else if (OB_ISNULL(scan_table_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("table schema scanned for foreign key check not exist", K(ret));
          } else if (scan_table_schema->get_part_level() != PARTITION_LEVEL_ZERO &&
                    OB_FAIL(log_plan->gen_calc_part_id_expr(fk_info.foreign_key_id_, // init table_id by foreign key id
                                                            scan_index_tid,
                                                            CALC_PARTITION_TABLET_ID,
                                                            fk_look_up_part_id_expr))) {
            LOG_WARN("failed to gen calc part id expr", K(ret));
          } else if (OB_FAIL(index_dml_info.fk_lookup_part_id_expr_.push_back(fk_look_up_part_id_expr))) {
            LOG_WARN("failed to push part id expr to array", K(ret), K(index_dml_info.fk_lookup_part_id_expr_));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogDelUpd::convert_insert_new_fk_lookup_part_id_expr(ObIArray<ObRawExpr*> &all_exprs, IndexDMLInfo &index_dml_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> dml_columns;
  ObSEArray<ObRawExpr *, 4> dml_values;
  if (OB_FAIL(get_insert_exprs(index_dml_info, dml_columns, dml_values))) {
    LOG_WARN("failed to get insert exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.fk_lookup_part_id_expr_.count(); ++i) {
      ObRawExpr *fk_look_up_part_id_expr = index_dml_info.fk_lookup_part_id_expr_.at(i);
      if (OB_ISNULL(fk_look_up_part_id_expr)) {
        //nothing to do
      } else if (OB_FAIL(replace_expr_for_fk_part_expr(dml_columns,
                                                      dml_values,
                                                      fk_look_up_part_id_expr))) {
        LOG_WARN("failed to replace column ref expr for partition expr used for foreign key check", K(ret));
      } else if (OB_FAIL(all_exprs.push_back(fk_look_up_part_id_expr))) {
        LOG_WARN("failed to push foreign key check partition expr to all exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObLogDelUpd::convert_update_new_fk_lookup_part_id_expr(ObIArray<ObRawExpr*> &all_exprs, IndexDMLInfo &index_dml_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> dml_columns;
  ObSEArray<ObRawExpr *, 4> dml_values;
  if (OB_FAIL(get_update_exprs(index_dml_info, dml_columns, dml_values))) {
    LOG_WARN("failed to get insert exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.fk_lookup_part_id_expr_.count(); ++i) {
      ObRawExpr *fk_look_up_part_id_expr = index_dml_info.fk_lookup_part_id_expr_.at(i);
      if (OB_ISNULL(fk_look_up_part_id_expr)) {

      } else if (OB_FAIL(replace_expr_for_fk_part_expr(dml_columns,
                                                       dml_values,
                                                       fk_look_up_part_id_expr))) {
        LOG_WARN("failed to replace column ref expr for partition expr used for foreign key check", K(ret));
      } else if (OB_FAIL(all_exprs.push_back(fk_look_up_part_id_expr))) {
        LOG_WARN("failed to push foreign key check partition expr to all exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObLogDelUpd::generate_insert_new_calc_partid_expr(IndexDMLInfo &index_dml_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> dml_columns;
  ObSEArray<ObRawExpr *, 4> dml_values;
  if (OB_FAIL(get_insert_exprs(index_dml_info, dml_columns, dml_values))) {
    LOG_WARN("failed to get insert exprs", K(ret));
  } else if (OB_FAIL(convert_expr_by_dml_operation(
                       dml_columns,
                       dml_values,
                       index_dml_info.old_part_id_expr_,
                       index_dml_info.new_part_id_expr_))) {
    LOG_WARN("failed to convert expr by dml operation", K(ret));
  } else {
    // insert does not have old partition id expr
    index_dml_info.old_part_id_expr_ = NULL;
  }
  return ret;
}

int ObLogDelUpd::generate_update_new_calc_partid_expr(IndexDMLInfo &index_dml_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> update_columns;
  ObSEArray<ObRawExpr *, 4> update_values;
  if (OB_FAIL(get_update_exprs(index_dml_info, update_columns, update_values))) {
    LOG_WARN("failed to get update exprs", K(ret));
  } else if (OB_FAIL(convert_expr_by_dml_operation(update_columns,
                                                   update_values,
                                                   index_dml_info.old_part_id_expr_,
                                                   index_dml_info.new_part_id_expr_))) {
    LOG_WARN("failed to convert expr by dml operation", K(ret));
  }
  return ret;
}


int ObLogDelUpd::convert_expr_by_dml_operation(const ObIArray<ObRawExpr *> &dml_columns,
                                               const ObIArray<ObRawExpr *> &dml_new_values,
                                               ObRawExpr *cur_value,
                                               ObRawExpr *&new_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_value) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current value expr is null", K(ret), K(get_plan()), K(cur_value));
  } else {
    ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
    if (OB_FAIL(copier.add_replaced_expr(dml_columns, dml_new_values))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(copier.copy_on_replace(cur_value, new_value))) {
      LOG_WARN("failed to copy on replace expr", K(ret));
    }
  }
  return ret;
}

int ObLogDelUpd::replace_expr_for_fk_part_expr(const ObIArray<ObRawExpr *> &dml_columns,
                                               const ObIArray<ObRawExpr *> &dml_new_values,
                                               ObRawExpr *fk_part_id_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current value expr is null", K(ret), K(get_plan()));
  } else {
    ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
    if (OB_FAIL(copier.add_replaced_expr(dml_columns, dml_new_values))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < fk_part_id_expr->get_param_count(); ++i) {
        ObRawExpr *param = fk_part_id_expr->get_param_expr(i);
        ObRawExpr *new_param = NULL;
        if (OB_FAIL(copier.copy_on_replace(param, new_param))) {
          LOG_WARN("failed to static replace expr", K(ret));
        } else {
          fk_part_id_expr->get_param_expr(i) = new_param;
        }
      }
    }
  }
  return ret;
}

int ObLogDelUpd::get_update_exprs(const IndexDMLInfo &dml_info,
                                  ObIArray<ObRawExpr *> &dml_columns,
                                  ObIArray<ObRawExpr *> &dml_values)
{
  int ret = OB_SUCCESS;
  const ObAssignments &assigns = dml_info.assignments_;
  for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); ++i) {
    if (OB_FAIL(dml_columns.push_back(assigns.at(i).column_expr_))) {
      LOG_WARN("failed to push back assign column", K(ret));
    } else if (OB_FAIL(dml_values.push_back(assigns.at(i).expr_))) {
      LOG_WARN("failed to push back assign value", K(ret));
    }
  }
  return ret;
}

int ObLogDelUpd::get_insert_exprs(const IndexDMLInfo &dml_info,
                                  ObIArray<ObRawExpr *> &dml_columns,
                                  ObIArray<ObRawExpr *> &dml_values)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(dml_info.column_exprs_.count() != dml_info.column_convert_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column and values size do not match", K(ret));
  } else if (OB_FAIL(append(dml_columns, dml_info.column_exprs_))) {
    LOG_WARN("failed to append column exprs", K(ret));
  } else if (OB_FAIL(append(dml_values, dml_info.column_convert_exprs_))) {
    LOG_WARN("failed to append value exprs", K(ret));
  }
  return ret;
}

int ObLogDelUpd::print_outline_data(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()));
  } else if (is_multi_part_dml()) {
    char *buf = plan_text.buf_;
    int64_t &buf_len = plan_text.buf_len_;
    int64_t &pos = plan_text.pos_;
    const ObDMLStmt *stmt = NULL;
    ObString qb_name;
    if (OB_ISNULL(stmt = get_plan()->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(stmt));
    } else if (OB_FAIL(stmt->get_qb_name(qb_name))) {
      LOG_WARN("fail to get qb_name", K(ret), K(stmt->get_stmt_id()));
    } else if (OB_FAIL(BUF_PRINTF("%s%s(@\"%.*s\")",
                                  ObQueryHint::get_outline_indent(plan_text.is_oneline_),
                                  ObHint::get_hint_name(T_USE_DISTRIBUTED_DML),
                                  qb_name.length(), qb_name.ptr()))) {
      LOG_WARN("fail to print buffer", K(ret), K(buf), K(buf_len), K(pos));
    }
  }
  return ret;
}

int ObLogDelUpd::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_dml_info_exprs(replacer, get_index_dml_infos()))) {
    LOG_WARN("failed to replace dml info exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, view_check_exprs_))) {
    LOG_WARN("failed to replace view check exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, produced_trans_exprs_))) {
    LOG_WARN("failed to replace produced trans exprs", K(ret));
  } else if (NULL != pdml_partition_id_expr_ &&
    OB_FAIL(replace_expr_action(replacer, pdml_partition_id_expr_))) {
    LOG_WARN("failed to replace pdml partition id expr", K(ret));
  }
  return ret;
}

int ObLogDelUpd::replace_dml_info_exprs(
    ObRawExprReplacer &replacer,
    const ObIArray<IndexDMLInfo *> &index_dml_infos)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); i++) {
    IndexDMLInfo *index_dml_info = index_dml_infos.at(i);
    if (OB_ISNULL(index_dml_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(replace_exprs_action(replacer,
                                            index_dml_info->column_convert_exprs_))) {
      LOG_WARN("failed to replace exprs", K(ret));
    } else if (OB_FAIL(replace_exprs_action(replacer,
                                            index_dml_info->ck_cst_exprs_))) {
      LOG_WARN("failed to replace exprs", K(ret));
    } else if (NULL != index_dml_info->new_part_id_expr_ &&
            OB_FAIL(replace_expr_action(replacer, index_dml_info->new_part_id_expr_))) {
      LOG_WARN("failed to replace new parititon id expr", K(ret));
    } else if (NULL != index_dml_info->old_part_id_expr_ &&
      OB_FAIL(replace_expr_action(replacer, index_dml_info->old_part_id_expr_))) {
      LOG_WARN("failed to replace old parititon id expr", K(ret));
    } else if (NULL != index_dml_info->old_rowid_expr_ &&
      OB_FAIL(replace_expr_action(replacer, index_dml_info->old_rowid_expr_))) {
      LOG_WARN("failed to replace old rowid expr", K(ret));
    } else if (NULL != index_dml_info->new_rowid_expr_ &&
      OB_FAIL(replace_expr_action(replacer, index_dml_info->new_rowid_expr_))) {
      LOG_WARN("failed to replace new rowid expr", K(ret));
    } else if (OB_FAIL(replace_exprs_action(replacer, index_dml_info->column_old_values_exprs_))) {
      LOG_WARN("failed to replace column old values exprs ", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info->assignments_.count(); ++i) {
      if (OB_FAIL(replace_expr_action(replacer, index_dml_info->assignments_.at(i).expr_))) {
        LOG_WARN("failed to replace expr", K(ret));
      }
    }
  }
  return ret;
}

int ObLogDelUpd::print_used_hint(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()));
  } else  {
    const ObHint *hint = get_plan()->get_log_plan_hint().get_normal_hint(T_USE_DISTRIBUTED_DML);
    if (NULL != hint) {
      bool match_hint = is_multi_part_dml() ?
                        hint->is_enable_hint() : hint->is_disable_hint();
      if (match_hint && OB_FAIL(hint->print_hint(plan_text))) {
        LOG_WARN("failed to print use multi part dml hint", K(ret));
      }
    }
  }
  return ret;
}

int ObLogDelUpd::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  int ret = OB_SUCCESS;
  const ObIArray<IndexDMLInfo *> &index_dml_infos = get_index_dml_infos();
  for (int64_t i = 0; OB_SUCC(ret) && !is_fixed && i < index_dml_infos.count(); ++i) {
    if (OB_ISNULL(index_dml_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index dml info is null", K(ret));
    } else if (OB_FAIL(index_dml_infos.at(i)->is_new_row_expr(expr, is_fixed))) {
      LOG_WARN("failed to check is new row expr", K(ret));
    }
  }
  return ret;
}

int ObLogDelUpd::check_use_child_ordering(bool &used, int64_t &inherit_child_ordering_index)
{
  int ret = OB_SUCCESS;
  used = false;
  inherit_child_ordering_index = -1;
  return ret;
}