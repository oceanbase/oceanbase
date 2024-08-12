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

#define USING_LOG_PREFIX SQL_REWRITE

#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"
#include "sql/rewrite/ob_transform_segmented_limit_pushdown.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/expr/ob_expr_version.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"

namespace oceanbase {
using namespace common;
namespace sql {

int ObTransformSegmentedLimitPushdown::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                            ObDMLStmt *&stmt,
                                                            bool &trans_happened) {
  int ret = OB_SUCCESS;
  bool is_valid;
  if (OB_FAIL(check_stmt_validity(stmt, is_valid))) {
    LOG_WARN("failed to check stmt validity", K(ret));
  } else if (!is_valid) {
  } else if (OB_FAIL(do_transform(parent_stmts, stmt, trans_happened))) {
    LOG_WARN("failed to push order by limit before join", K(ret));
  }

  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column rel ids", K(ret));
    } else if (OB_FAIL(add_transform_hint(*stmt))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }

  return ret;
}

int ObTransformSegmentedLimitPushdown::check_stmt_validity(ObDMLStmt *stmt,
                                                             bool &is_valid) {

  int ret = OB_SUCCESS;
  is_valid = true;
  ObSelectStmt *select_stmt = nullptr;
  bool is_valid_limit = true;
  bool is_valid_order_by = true;
  bool is_valid_table = true;
  bool is_valid_condition = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(ret));
  } else if (!stmt->is_select_stmt()) {
    is_valid = false;
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt *>(stmt))) {
    is_valid = false;
  } else if (!select_stmt->has_limit() ||
             !select_stmt->has_order_by() ||
             select_stmt->has_subquery() ||
             select_stmt->is_hierarchical_query() ||
             select_stmt->has_group_by() ||
             select_stmt->has_having() ||
             select_stmt->has_rollup() ||
             select_stmt->has_window_function() ||
             select_stmt->has_sequence() ||
             select_stmt->has_distinct()) {
    is_valid = false;
  } else if (OB_FAIL(check_condition(select_stmt, is_valid_condition)) || !is_valid_condition) {
    is_valid = false;
  } else if (OB_FAIL(check_limit(select_stmt, is_valid_limit)) || !is_valid_limit) {
    is_valid = false;
  } else if (OB_FAIL(check_order_by(select_stmt, is_valid_order_by)) || !is_valid_order_by) {
    is_valid = false;
  }
  return ret;
}

int ObTransformSegmentedLimitPushdown::do_transform(ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *&stmt, bool &trans_happened) {
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> pushdown_conds;
  ObSEArray<ObRawExpr *, 8> need_rewrite_in_conds;
  ObStmtFactory *stmt_factory = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObDMLStmt *trans_stmt = nullptr;
  ObSelectStmt *select_stmt = nullptr;

  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) || OB_ISNULL(stmt_factory = ctx_->stmt_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*stmt_factory, *expr_factory, stmt, trans_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt *>(trans_stmt))) {
  } else if (OB_FAIL(extract_conditions(select_stmt, &need_rewrite_in_conds, &pushdown_conds))) {
    LOG_WARN("failed to extract conditions from select stmt", K(ret));
  } else if (need_rewrite_in_conds.count() == 0) {
  } else {
    bool last_transform = false;
    ObSelectStmt *next_trans_stmt = select_stmt;
    for (int64_t i = 0; OB_SUCC(ret) && i < need_rewrite_in_conds.count(); i++) {
      last_transform = (i == need_rewrite_in_conds.count() - 1);
      if (OB_FAIL(do_one_transform(next_trans_stmt,
                                   need_rewrite_in_conds.at(i),
                                   &pushdown_conds,
                                   last_transform))) {
        LOG_WARN("failed to do transform for stmt", K(*next_trans_stmt));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(accept_transform(parent_stmts,
                                   stmt,
                                   trans_stmt,
                                   get_hint(stmt->get_stmt_hint()) != nullptr,
                                   true,
                                   trans_happened))) {
        LOG_WARN("failed to accept transform", K(ret));
      } else if (trans_happened) {
        stmt = trans_stmt;
      }
    }
  }
  return ret;
}

/**
* @brief: transform sql statement base on a 'in condition'.
* Steps:
* - create lateral table
* - create values table
 *    resolve values table info from in list
 *    generate column item for values table
* - create view stmt
*       from list: lateral table
*       where condition: (lateral table).col = (values table).col
* - pushdown limit into view stmt
* - remove where condition of original stmt.
* @return int
*/
int ObTransformSegmentedLimitPushdown::do_one_transform(ObSelectStmt *&select_stmt,
                                                        ObRawExpr *in_condition,
                                                        ObIArray<ObRawExpr *> *pushdown_conds,
                                                        bool last_transform) {
  int ret = OB_SUCCESS;

  TableItem *lateral_table = nullptr;
  TableItem *values_table = nullptr;
  ObExecParamRawExpr *exec_param_expr = nullptr;
  ObSEArray<TableItem *, 8> pushdown_tables;
  if (OB_FAIL(select_stmt->get_from_tables(pushdown_tables))) {
    LOG_WARN("failed to add element into pushdown_tables", K(ret));
  } else if (OB_FAIL(create_lateral_table(select_stmt,
                                          lateral_table))) {
    LOG_WARN("failed to create lateral table", K(ret));
  } else if (OB_FAIL(inlist_to_values_table(select_stmt, in_condition, values_table))) {
    LOG_WARN("failed to create values table", K(ret));
  } else if (OB_FAIL(construct_join_condition(select_stmt,
                                              values_table->table_id_,
                                              in_condition,
                                              exec_param_expr,
                                              pushdown_conds))){
    LOG_WARN("failed to construct condition expr for view stmt", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                          select_stmt,
                                                          lateral_table,
                                                          pushdown_tables,
                                                          NULL,
                                                          NULL,
                                                          NULL,
                                                          NULL,
                                                          NULL,
                                                          NULL,
                                                          &(select_stmt->get_order_items())))) {
    LOG_WARN("failed to create inline view", K(ret));
  } else if (OB_FAIL(pushdown_limit(select_stmt,
                                            lateral_table->ref_query_))) {
  } else {
    lateral_table->type_ = TableItem::LATERAL_TABLE;
    lateral_table->exec_params_.push_back(exec_param_expr);
    if (last_transform) {
      lateral_table->ref_query_->get_condition_exprs().assign(*pushdown_conds);
    }
    select_stmt = static_cast<ObSelectStmt *>(lateral_table->ref_query_);
  }

  return ret;
}

int ObTransformSegmentedLimitPushdown::check_condition(ObSelectStmt *select_stmt, bool &is_valid) {
  int ret = OB_SUCCESS;
  size_t candidate_in_condition_size = 0;
  is_valid = true;
  for (size_t i = 0; OB_SUCC(ret) && i < select_stmt->get_condition_size() && is_valid; i++) {
    bool is_candidate = true;
    if (OB_FAIL(is_candidate_in_condition(select_stmt->get_condition_expr(i), is_candidate))) {
      LOG_WARN("fail to check whether the condition is a candidate 'in condition' need to be rewrite", K(ret));
    } else if (is_candidate) {
      candidate_in_condition_size++;
    }
  }
  if (candidate_in_condition_size == 0) {
    is_valid = false;
  }

  return ret;
}

/* @brief: check whether 'condition_expr' is a candidate 'in condition' need to be rewritten.
*/
int ObTransformSegmentedLimitPushdown::is_candidate_in_condition(ObRawExpr *condition_expr, bool &is_candidate_in_condition) {
  int ret = OB_SUCCESS;
  ObOpRawExpr *in_op_expr = nullptr;
  ObRawExpr *left_expr_without_cast = nullptr;
  if (OB_ISNULL(condition_expr)) {
    is_candidate_in_condition = false;
  } else if (!condition_expr->is_op_expr()) {
    is_candidate_in_condition = false;
  } else if (FALSE_IT(in_op_expr = static_cast<ObOpRawExpr *>(condition_expr))) {
  } else if (in_op_expr->get_expr_type() != T_OP_IN) {
    is_candidate_in_condition = false;
  } else if (in_op_expr->get_param_count() != 2) {
    is_candidate_in_condition = false;
  } else if (in_op_expr->get_param_expr(1)->get_expr_type() != T_OP_ROW) {
    is_candidate_in_condition = false;
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(in_op_expr->get_param_expr(0), left_expr_without_cast))) {
    LOG_WARN("fail to get expr without lossless_cast");
    is_candidate_in_condition = false;
  } else if (left_expr_without_cast->get_expr_type() != T_REF_COLUMN) {
    is_candidate_in_condition = false;
  } else {
    ObOpRawExpr *inlist_expr = static_cast<ObOpRawExpr *>(in_op_expr->get_param_expr(1));
    for (size_t i = 0; is_candidate_in_condition && i < inlist_expr->get_param_count(); i++) {
      ObRawExpr *element_expr = inlist_expr->get_param_expr(i);
      if (element_expr->get_expr_type() == T_QUESTIONMARK) {
      } else if (element_expr->is_op_expr()) {
        is_candidate_in_condition = false;
      } else if (!element_expr->is_static_const_expr()) {
        is_candidate_in_condition = false;
      }
    }
  }
  
  return ret;
}

int ObTransformSegmentedLimitPushdown::check_limit(ObSelectStmt *select_stmt, bool &is_valid) {
  int ret = OB_SUCCESS;
  is_valid = true;
  ObRawExpr *limit_expr = nullptr;
  if (OB_ISNULL(limit_expr = select_stmt->get_limit_expr())) {
    is_valid = false;
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("illegal limit expr", K(ret));
  } else if (OB_NOT_NULL(select_stmt->get_limit_percent_expr())) {
    is_valid = false;
    LOG_TRACE("can not pushdown limit percent expr");
  } else if (select_stmt->is_fetch_with_ties()) {
    is_valid = false;
    LOG_TRACE("can not pushdown fetch with ties");
  } else if (limit_expr->get_expr_type() == T_QUESTIONMARK) {
    //do nothing
  } else if (limit_expr->is_const_raw_expr()) {
    const ObObj &value = static_cast<const ObConstRawExpr *>(limit_expr)->get_value();
    if (value.is_invalid_type() || !value.is_integer_type()) {
      is_valid = false;
    } else if (value.get_int() < 0) {
      is_valid = false;
    }
  } else {
    is_valid = false;
    LOG_TRACE("can not support limit expr type", K(*limit_expr));
  }

  return ret;
}


int ObTransformSegmentedLimitPushdown::check_order_by(ObSelectStmt *select_stmt, bool &is_valid) {
  int ret = OB_SUCCESS;
  is_valid = true;
  ObRawExpr *order_by_expr = nullptr;
  int64_t order_item_size = select_stmt->get_order_item_size();
  if (order_item_size == 0) {
    is_valid = false;
  } else {
    int64_t table_id = -1;
    for (int64_t i = 0; is_valid && i < order_item_size; i++) {
      ObColumnRefRawExpr *col_expr = nullptr;
      if (OB_ISNULL(order_by_expr = select_stmt->get_order_item(i).expr_)) {
        is_valid = false;
      } else if (order_by_expr->get_expr_type() != T_REF_COLUMN) {
        is_valid = false;
      } else if (FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr *>(order_by_expr))) {
      } else if (table_id == -1) {
        table_id = col_expr->get_table_id();
      } else if (col_expr->get_table_id() != table_id) {
        is_valid = false;
      }
    }
  }

  return ret;
}

int ObTransformSegmentedLimitPushdown::get_exec_param_expr(ObSelectStmt *select_stmt,
                                                           uint64_t ref_table_id,
                                                           ObExecParamRawExpr *&exec_param_expr) {
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 8> column_items;
  ObColumnRefRawExpr *col_ref_expr = nullptr;
  if (OB_FAIL(select_stmt->get_column_items(ref_table_id, column_items))) {

  } else if (column_items.count() != 1){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The number of column items of values table must be 1.", K(ret));
  } else if (OB_ISNULL(col_ref_expr = column_items.at(0).get_expr())){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column expr of column item should not be null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_new_exec_param(*ctx_->expr_factory_,
                                                           col_ref_expr,
                                                           exec_param_expr,
                                                           false))) {
    LOG_WARN("failed to create exec param expr");
  }

  return ret;
}


/* @brief: create an equal join condition expression.
 *  - left child of the new 'equal join condition' is the left child of 'in expression'
 *  - right child of the new 'equal join condition' is exec_param_expr
*/
int ObTransformSegmentedLimitPushdown::construct_join_condition(ObSelectStmt *select_stmt,
                                                                uint64_t values_table_id,
                                                                ObRawExpr *in_condition,
                                                                ObExecParamRawExpr *&exec_param_expr,
                                                                ObIArray<ObRawExpr *> *pushdown_conditions) {
  int ret = OB_SUCCESS;
  ObRawExpr *left_expr = static_cast<ObOpRawExpr *>(in_condition)->get_param_expr(0);
  ObRawExpr *eq_expr = nullptr;
  if (OB_FAIL(get_exec_param_expr(select_stmt, values_table_id, exec_param_expr))) { /*获取exec param表达式，被用在subquery的where contidion以及lateral table的exec_params中*/
    LOG_WARN("fail to create exec param expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(*ctx_->expr_factory_,
                                                       ctx_->session_info_,
                                                       exec_param_expr,
                                                       left_expr,
                                                       eq_expr))) {
    LOG_WARN("fail to create equal expr");
  } else {
    pushdown_conditions->push_back(eq_expr);
  }

  // remove where condition from original select statement.
  select_stmt->get_condition_exprs().reset();
  return ret;
}

int ObTransformSegmentedLimitPushdown::pushdown_limit(ObSelectStmt *upper_stmt, ObSelectStmt *generated_view) {
  int ret = OB_SUCCESS;

  ObRawExpr *limit_expr = nullptr;
  ObRawExpr *offset_expr = nullptr;
  ObRawExpr *new_limit_expr = nullptr;

  if (OB_ISNULL(new_limit_expr = limit_expr = upper_stmt->get_limit_expr())) {
  } else if (OB_ISNULL(offset_expr = upper_stmt->get_offset_expr())) {
  } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_,
                                                          ctx_->session_info_,
                                                          T_OP_ADD,
                                                          new_limit_expr,
                                                          limit_expr,
                                                          offset_expr))) {
    LOG_WARN("fail to create add expr", K(ret));
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(new_limit_expr)) {
    generated_view->set_limit_offset(new_limit_expr, nullptr);
  }

  return ret;
}

int ObTransformSegmentedLimitPushdown::create_lateral_table(ObSelectStmt *select_stmt,
                                                              TableItem *&lateral_table) {
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_,
                                                   select_stmt,
                                                   NULL,
                                                   lateral_table))) {
    LOG_WARN("failed to create table item", K(ret));
  } else if (OB_FAIL(select_stmt->add_from_item(lateral_table->table_id_, false))) {
    LOG_WARN("failed to add lateral table into from list", K(ret));
  }
  return ret;
}

int ObTransformSegmentedLimitPushdown::extract_conditions(ObSelectStmt *select_stmt,
                                                          ObIArray<ObRawExpr *> *need_rewrite_in_conds,
                                                          ObIArray<ObRawExpr *> *pushdown_conds) {
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> candidate_in_conditions;
  ObRawExpr *condition_expr = nullptr;
  for (size_t i = 0; i < select_stmt->get_condition_size(); i++) {
    bool is_candidate = true;
    condition_expr = select_stmt->get_condition_expr(i);
    if (OB_FAIL(is_candidate_in_condition(condition_expr, is_candidate))) {
      LOG_WARN("fail to check whether the condition is a candidate 'in condition' need to be rewrite", K(ret));
    } else if (is_candidate) {
      candidate_in_conditions.push_back(condition_expr);
    } else {
      pushdown_conds->push_back(condition_expr);
    }
  }

  if (OB_FAIL(get_need_rewrite_in_conditions(select_stmt, &candidate_in_conditions, need_rewrite_in_conds, pushdown_conds))) {
    LOG_WARN("failed to get in conditions need to be rewritten", K(ret));
  }

  return ret;
}

int ObTransformSegmentedLimitPushdown::inlist_to_values_table(ObSelectStmt *select_stmt,
                                                              ObRawExpr *in_condition,
                                                              TableItem *&values_table) {
  int ret = OB_SUCCESS;
  char *table_buf = nullptr;
  if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_,
                                                   select_stmt,
                                                   NULL,
                                                   values_table))) {
    LOG_WARN("failed to create table item", K(ret));
  } else if (OB_FAIL(select_stmt->add_from_item(values_table->table_id_, false))) {
    LOG_WARN("failed to add values table into from list", K(ret));
  } else if (OB_ISNULL(table_buf = static_cast<char*>(ctx_->allocator_->alloc(sizeof(ObValuesTableDef))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("table_buf is null", K(ret), KP(table_buf));
  } else {
    values_table->type_ = TableItem::VALUES_TABLE;
    values_table->values_table_def_ = new (table_buf) ObValuesTableDef();
    if (OB_FAIL(resolve_values_table_from_inlist(in_condition, values_table->values_table_def_))) {
      LOG_WARN("failed to resolve values table from in list", K(ret));
    } else if (OB_FAIL(gen_values_table_column_items(select_stmt, values_table))) {
      LOG_WARN("failed to generate column item for values table", K(ret));
    } else if (OB_FAIL(estimate_values_table_stats(*values_table->values_table_def_,
                                                   &ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store(),
                                                   ctx_->session_info_))) {
      LOG_WARN("failed to calculate statistics for values table", K(ret));
    }
  }

  return ret;
}

int ObTransformSegmentedLimitPushdown::check_question_mark(ObOpRawExpr *in_list_expr, bool &is_question_mark) {
  int ret = OB_SUCCESS;
  ObRawExpr *row_expr = nullptr;

  if (in_list_expr->get_param_count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inlist length is empty", K(ret));
  } else {

  }

  row_expr = in_list_expr->get_param_expr(0);
  if (row_expr->get_expr_type() == T_QUESTIONMARK) {
    is_question_mark = true;
  } else {
    is_question_mark = false;
  }

  return ret;
}

//Refer to ObInListResolver::resolve_access_param_values_table
int ObTransformSegmentedLimitPushdown::resolve_access_param_values_table(ObOpRawExpr *in_list_expr,
                                                                            ObValuesTableDef *&values_table_def) {
  int ret = OB_SUCCESS;
  ObRawExpr *row_expr = nullptr;
  int row_cnt = values_table_def->row_cnt_;
  int column_cnt = values_table_def->column_cnt_;

  values_table_def->access_type_ = ObValuesTableDef::ACCESS_PARAM;

  for (int i = 0; i < row_cnt; i++) {
    row_expr = in_list_expr->get_param_expr(i);
    if (OB_ISNULL(row_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("got unexpected row expr in inlist", K(*in_list_expr));
    }
    for (int j = 0; j < column_cnt; j++) {
      ObRawExpr *element_expr = column_cnt == 1 ? row_expr : row_expr->get_param_expr(j);
      int idx = static_cast<ObConstRawExpr *>(element_expr)->get_value().get_unknown();
      ObExprResType res_type;
      const ObObjParam &obj_param = ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store().at(idx);
      res_type.set_meta(obj_param.get_param_meta());
      res_type.set_accuracy(obj_param.get_accuracy());
      res_type.set_result_flag(obj_param.get_result_flag());

      if (i == 0) {
        if (OB_FAIL(values_table_def->column_types_.push_back(res_type))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else if (OB_FAIL(try_merge_column_type(j, values_table_def, res_type))){
        LOG_WARN("failed to merge type of the same column from different rows", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    values_table_def->start_param_idx_ = static_cast<ObConstRawExpr *>(in_list_expr->get_param_expr(0))->get_value().get_unknown();
    values_table_def->end_param_idx_ = static_cast<ObConstRawExpr *>(in_list_expr->get_param_expr(row_cnt - 1))->get_value().get_unknown();
  }

  return ret;
}


//Refre to ObInListResolver::resolve_access_obj_values_table
int ObTransformSegmentedLimitPushdown::resolve_access_obj_values_table(ObOpRawExpr *in_list_expr,
                                                                       ObValuesTableDef *&values_table_def) {
  int ret = OB_SUCCESS;
  ObRawExpr *row_expr = nullptr;
  const bool is_oracle_mode = lib::is_oracle_mode();
  ObSQLSessionInfo *session_info = ctx_->session_info_;
  ObLengthSemantics length_semantics = session_info->get_actual_nls_length_semantics();
  ObCollationType coll_type = CS_TYPE_INVALID;
  int row_cnt = values_table_def->row_cnt_;
  int column_cnt = values_table_def->column_cnt_;

  values_table_def->access_type_ = ObValuesTableDef::ACCESS_OBJ;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; i++) {
    row_expr = in_list_expr->get_param_expr(i);

    for (int64_t j = 0; OB_SUCC(ret) && j < column_cnt; j++) {
      ObRawExpr *element_expr = column_cnt == 1 ? row_expr : row_expr->get_param_expr(j);
      ObExprResType res_type;
      ObObjParam obj_param = static_cast<ObConstRawExpr *>(row_expr)->get_value();
      if (OB_FAIL(values_table_def->access_objs_.push_back(obj_param))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        res_type.set_meta(element_expr->get_result_meta());
        res_type.set_accuracy(element_expr->get_accuracy());
        res_type.set_result_flag(element_expr->get_result_flag());
        if (i == 0) {
          if (OB_FAIL(values_table_def->column_types_.push_back(res_type))) {
            LOG_WARN("failed to push back", K(ret));
          }
        } else if (OB_FAIL(try_merge_column_type(j, values_table_def, res_type))){
          LOG_WARN("failed to merge type of the same column from different rows", K(ret));
        }
      }
    }
  }

  return ret;
}

// Update the column type of values table, referring to the implementation of ObInListResolver.
int ObTransformSegmentedLimitPushdown::try_merge_column_type(int col_idx,
                                                             ObValuesTableDef *values_table_def,
                                                             const ObExprResType &res_type) {
  int ret = OB_SUCCESS;

  const bool is_oracle_mode = lib::is_oracle_mode();
  ObSQLSessionInfo *session_info = ctx_->session_info_;
  ObLengthSemantics length_semantics = session_info->get_actual_nls_length_semantics();
  ObCollationType coll_type = CS_TYPE_INVALID;
  ObExprResType new_res_type;
  ObExprVersion dummy_op(*ctx_->allocator_);
  ObSEArray<ObExprResType, 2> tmp_res_types;

  if (OB_FAIL(session_info->get_collation_connection(coll_type))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(tmp_res_types.push_back(values_table_def->column_types_.at(col_idx)))) {
    LOG_WARN("failed to push back res type", K(ret));
  } else if (OB_FAIL(tmp_res_types.push_back(res_type))) {
    LOG_WARN("failed to push back res type", K(ret));
  } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(new_res_type,
                                                              &tmp_res_types.at(0), 2, coll_type, is_oracle_mode, length_semantics,
                                                              session_info))) {
    LOG_WARN("failed to aggregate result type for merge", K(ret));
  } else {
    values_table_def->column_types_.at(col_idx) = new_res_type;
  }

  return ret;
}

int ObTransformSegmentedLimitPushdown::resolve_values_table_from_inlist(ObRawExpr *in_condition,
                                                                        ObValuesTableDef *&values_table_def) {
  int ret = OB_SUCCESS;
  ObOpRawExpr *in_list_expr = static_cast<ObOpRawExpr *>(in_condition->get_param_expr(1));
  ObRawExpr *row_expr = nullptr;
  int64_t row_cnt = in_list_expr->get_param_count();
  bool is_question_mark = false;
  values_table_def->row_cnt_ = row_cnt;
  values_table_def->column_cnt_ = 1;
  if (OB_FAIL(check_question_mark(in_list_expr, is_question_mark))) {
    LOG_WARN("fail to check question mark", K(ret));
  } else if (is_question_mark) {
    resolve_access_param_values_table(in_list_expr, values_table_def);
  } else {
    resolve_access_obj_values_table(in_list_expr, values_table_def);
  }

  return ret;
}

int ObTransformSegmentedLimitPushdown::gen_values_table_column_items(ObSelectStmt *select_stmt,
                                                                       TableItem *values_table)
{
  int ret = OB_SUCCESS;
  ObValuesTableDef *table_def = values_table->values_table_def_;
  int64_t column_cnt = table_def->column_cnt_;
  const ObIArray<ObExprResType> &res_types = table_def->column_types_;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
    ObColumnRefRawExpr *column_expr = NULL;
    if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_REF_COLUMN, column_expr))) {
      LOG_WARN("create column ref raw expr failed", K(ret));
    } else {
      column_expr->set_result_type(res_types.at(i));
//    column_expr->set_result_flag(table_def->access_exprs_.at(i)->get_result_flag());
      column_expr->set_ref_id(values_table->table_id_, i + OB_APP_MIN_COLUMN_ID);
      // compatible Mysql8.0, column name is column_0, column_1, ...
      ObSqlString tmp_col_name;
      char *buf = NULL;
      if (OB_FAIL(tmp_col_name.append_fmt("column_%ld", i))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_ISNULL(buf = static_cast<char*>(ctx_->allocator_->alloc(tmp_col_name.length())))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(buf));
      } else {
        MEMCPY(buf, tmp_col_name.ptr(), tmp_col_name.length());
        ObString column_name(tmp_col_name.length(), buf);
        column_expr->set_column_attr(values_table->table_name_, column_name);
        if (ob_is_enumset_tc(column_expr->get_result_type().get_type()) ||
            column_expr->get_result_type().is_lob_storage()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("values stmt not support such column type", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "type of column in values table");
        } else if (OB_FAIL(column_expr->add_flag(IS_COLUMN))) {
          LOG_WARN("failed to add flag IS_COLUMN", K(ret));
        } else if (OB_FAIL(column_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("failed to formalize a column expr", K(ret));
        } else {
          ColumnItem column_item;
          column_item.expr_ = column_expr;
          column_item.table_id_ = column_expr->get_table_id();
          column_item.column_id_ = column_expr->get_column_id();
          column_item.column_name_ = column_expr->get_column_name();
          if (OB_FAIL(select_stmt->add_column_item(column_item))) {
            LOG_WARN("failed to add column item", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

/*
* TODO:The implementation is similar with ObDMLResolver::add_obj_to_llc_bitmap,
 *  move this function into some util class later.
*/
int ObTransformSegmentedLimitPushdown::add_obj_to_llc_bitmap(const ObObj &obj, char *llc_bitmap, double &num_null)
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  if (OB_ISNULL(llc_bitmap)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected", K(ret), KP(llc_bitmap));
  } else if (obj.is_null()) {
    num_null += 1.0;
  } else {
    if (obj.is_string_type()) {
      hash_value = obj.varchar_hash(obj.get_collation_type(), hash_value);
    } else if (OB_FAIL(obj.hash(hash_value, hash_value))) {
      LOG_WARN("fail to do hash", K(ret), K(obj));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObAggregateProcessor::llc_add_value(hash_value, llc_bitmap, ObOptColumnStat::NUM_LLC_BUCKET))) {
        LOG_WARN("fail to calc llc", K(ret));
      }
    }
  }
  return ret;
}

/* TODO: The implementation is similar with ObDMLResolver::estimate_values_table_stats,
*     move this function into some util class later.
*/
int ObTransformSegmentedLimitPushdown::estimate_values_table_stats(ObValuesTableDef &table_def,
                                                                   const ParamStore *param_store,
                                                                   ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  const int64_t compute_ndv_thredhold = 2000;
  char *llc_bitmap = NULL;
  const int64_t llc_bitmap_size = ObOptColumnStat::NUM_LLC_BUCKET;
  ObArenaAllocator alloc("ValuesTableStat");
  bool is_ps_prepare = false;
  bool has_ps_param = false;
  table_def.column_ndvs_.reset();
  table_def.column_nnvs_.reset();
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect param", K(ret));
  } else {
    is_ps_prepare = session_info->is_varparams_sql_prepare();
  }
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < table_def.column_cnt_; col_idx++) {
    double ndv = table_def.row_cnt_;
    double num_null = 0.0;
    if (ObValuesTableDef::ACCESS_EXPR == table_def.access_type_) {
      /* ndv = table_def.row_cnt_, num_null = 0.0 */
    } else if (OB_ISNULL(llc_bitmap = static_cast<char*>(alloc.alloc(llc_bitmap_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory for uncompressed data failed.", K(ret), K(llc_bitmap_size));
    } else {
      MEMSET(llc_bitmap, 0, llc_bitmap_size);
      if (ObValuesTableDef::FOLD_ACCESS_EXPR == table_def.access_type_) {
        ObRawExpr *access_expr = NULL;
        if (OB_UNLIKELY(col_idx >= table_def.access_exprs_.count()) ||
            OB_ISNULL(access_expr = table_def.access_exprs_.at(col_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpect param", K(ret), K(col_idx), KP(access_expr));
        } else if (T_QUESTIONMARK == access_expr->get_expr_type()) {
          ObConstRawExpr *param_expr = static_cast<ObConstRawExpr *>(access_expr);
          int64_t param_idx = param_expr->get_value().get_unknown();
          if (OB_ISNULL(param_store) ||
              OB_UNLIKELY(param_idx < 0 || param_idx >= param_store->count())) {
            if (is_ps_prepare) {
              has_ps_param = true;
              LOG_TRACE("ps prepare param_store is empty");
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("param_idx is invalid", K(ret), K(param_idx));
            }
          } else if (param_store->at(param_idx).is_ext_sql_array()) {
            const ObSqlArrayObj *array_obj =
                    reinterpret_cast<const ObSqlArrayObj*>(param_store->at(param_idx).get_ext());
            if (OB_ISNULL(array_obj)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected nullptr", K(ret), K(param_idx));
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < array_obj->count_ && i < compute_ndv_thredhold; i++) {
                const ObObjParam &obj_param = array_obj->data_[i];
                if (OB_FAIL(add_obj_to_llc_bitmap(obj_param, llc_bitmap, num_null))) {
                  LOG_WARN("failed to add obj to bitmap", K(ret));
                }
              }
            }
          }
        }
      } else if (ObValuesTableDef::ACCESS_PARAM == table_def.access_type_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < table_def.row_cnt_ && i < compute_ndv_thredhold; i++) {
          int64_t param_idx = table_def.start_param_idx_ + i * table_def.column_cnt_ + col_idx;
          if (OB_UNLIKELY(param_idx < 0 || param_idx >= param_store->count())) {
            if (is_ps_prepare) {
              has_ps_param = true;
              LOG_TRACE("ps prepare param_store is empty");
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("param_idx is invalid", K(ret), K(param_idx));
            }
          } else if (OB_FAIL(add_obj_to_llc_bitmap(param_store->at(param_idx), llc_bitmap, num_null))) {
            LOG_WARN("failed to add obj to bitmap", K(ret));
          }
        }
      } else if (ObValuesTableDef::ACCESS_OBJ == table_def.access_type_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < table_def.row_cnt_ && i < compute_ndv_thredhold; i++) {
          int64_t param_idx = i * table_def.column_cnt_ + col_idx;
          if (OB_FAIL(add_obj_to_llc_bitmap(table_def.access_objs_.at(param_idx), llc_bitmap, num_null))) {
            LOG_WARN("failed to add obj to bitmap", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && !has_ps_param) {
        ndv = MIN(ObGlobalNdvEval::get_ndv_from_llc(llc_bitmap), ndv);
        ndv = table_def.row_cnt_ <= compute_ndv_thredhold ? ndv :
              ObOptSelectivity::scale_distinct(table_def.row_cnt_, compute_ndv_thredhold, ndv);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_def.column_ndvs_.push_back(ndv))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(table_def.column_nnvs_.push_back(num_null))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        LOG_TRACE("print stats", K(ndv), K(num_null));
      }
    }
  }
  return ret;
}
/* @brief: Find all target 'in expression' need to be rewritten in candidate_in_condition.
 * Step1: find 'order elimiation index'
 * Step2: Find all target 'in expression' whose the column is inside the 'order elimination index'
 */
int ObTransformSegmentedLimitPushdown::get_need_rewrite_in_conditions(ObSelectStmt *select_stmt,
                                                                      ObIArray<ObRawExpr *> *candidate_in_conditions,
                                                                      ObIArray<ObRawExpr *> *need_rewrite_in_conditions,
                                                                      ObIArray<ObRawExpr *> *pushdown_conds) {

  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> order_exprs;
  ObSEArray<ObRawExpr *, 8> equal_condition_exprs;
  ObSEArray<uint64_t, 8> order_column_ids;
  ObSEArray<uint64_t, 8> equal_column_ids;
  ObSEArray<uint64_t, 8> index_column_ids;
  ObArenaAllocator alloc;

  uint64_t target_table_id = static_cast<ObColumnRefRawExpr*>(select_stmt->get_order_item(0).expr_)->get_table_id();
  bool found_index = false;
  if (OB_FAIL(select_stmt->get_order_exprs(order_exprs))) {
    LOG_WARN("fail to get order by exprs of stmt.");
  } else if (OB_FAIL(ObOptimizerUtil::extract_column_ids(order_exprs, target_table_id, order_column_ids))) {
    LOG_WARN("fail to extract column ids of order.");
  } else if (OB_FAIL(get_equal_conditions(select_stmt, &equal_condition_exprs))) {
    LOG_WARN("fail to get equal set of stmt.");
  } else if (OB_FAIL(ObOptimizerUtil::extract_column_ids(equal_condition_exprs, target_table_id, equal_column_ids))) {
    LOG_WARN("fail to extract column ids of equal conditions.");
  } else if (OB_FAIL(ObOptimizerUtil::extract_column_ids(*candidate_in_conditions, target_table_id, equal_column_ids))) {
    LOG_WARN("fail to extract column ids of candidate in conditions.");
  } else if (OB_FAIL(find_order_elimination_index(select_stmt, target_table_id, &order_column_ids, &equal_column_ids, &index_column_ids, found_index))) {
    LOG_WARN("fail when try to find 'order elimination' index of table: ", K(target_table_id));
  } else if (found_index) {
    ObRawExpr *without_cast_expr = nullptr;
    ObOpRawExpr *in_condition_expr = nullptr;
    ObColumnRefRawExpr *col_expr = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < candidate_in_conditions->count(); i++) {
      bool need_rewrite = true;
      in_condition_expr = static_cast<ObOpRawExpr*>(candidate_in_conditions->at(i));
      if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(in_condition_expr->get_param_expr(0), without_cast_expr))) {
        LOG_WARN("fail to get expr without lossless_cast");
        need_rewrite = false;
      } else if (without_cast_expr->get_expr_type() != T_REF_COLUMN) {
        need_rewrite = false;
      } else if (FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr *>(without_cast_expr))) {
      } else if (col_expr->get_table_id() != target_table_id) {
        need_rewrite = false;
      } else if (!ObOptimizerUtil::find_item(index_column_ids, col_expr->get_column_id())) {
        need_rewrite = false;
      }

      if (need_rewrite) {
        need_rewrite_in_conditions->push_back(in_condition_expr);
      } else {
        pushdown_conds->push_back(in_condition_expr);
      }
    }
  }
  return ret;
}

int ObTransformSegmentedLimitPushdown::get_equal_conditions(ObSelectStmt *select_stmt,
                                                            ObIArray<ObRawExpr *> *equal_condition_exprs) {
  int ret = OB_SUCCESS;
  ObOpRawExpr *op_expr = nullptr;
  for (int64_t i = 0; i < select_stmt->get_condition_size(); i++) {
    op_expr = static_cast<ObOpRawExpr *>(select_stmt->get_condition_expr(i));
    if (op_expr->get_expr_type() == T_OP_EQ) {
      if (op_expr->get_param_count() != 2) {
      } else if (
              (op_expr->get_param_expr(0)->is_column_ref_expr() && op_expr->get_param_expr(1)->is_const_expr()) ||
              (op_expr->get_param_expr(1)->is_column_ref_expr() && op_expr->get_param_expr(0)->is_const_expr())
              ) {
        equal_condition_exprs->push_back(op_expr);
      }
    }
  }
  return ret;
}

int ObTransformSegmentedLimitPushdown::find_order_elimination_index(ObSelectStmt *select_stmt,
                                                                    uint64_t table_id,
                                                                    ObIArray<uint64_t> *order_column_ids,
                                                                    ObIArray<uint64_t> *equal_column_ids,
                                                                    ObIArray<uint64_t> *index_column_ids,
                                                                    bool &found_index) {
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> index_ids;
  TableItem *table_item = select_stmt->get_table_item_by_id(table_id);
  ObSqlSchemaGuard *schema_guard = ctx_->sql_schema_guard_;

  if (OB_FAIL(ObTransformUtils::get_vaild_index_id(schema_guard, select_stmt, table_item, index_ids))) {
    LOG_WARN("fail to get index ids of target table.");
  } else {
    const ObTableSchema *index_schema = NULL;

    for (int64_t i = 0; OB_SUCC(ret) && i < index_ids.count() && !found_index; ++i) {
      index_column_ids->reuse();
      if (OB_FAIL(schema_guard->get_table_schema(index_ids.at(i), table_item, index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(index_ids.at(i)));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null index schema", K(ret));
      } else if (!index_schema->get_rowkey_info().is_valid()) {
        // do nothing
      } else if (OB_UNLIKELY(index_schema->is_spatial_index())) {
        // Do not consider spatial indexing for the time being
      } else if (OB_FAIL(index_schema->get_rowkey_info().get_column_ids(*index_column_ids))) {
        LOG_WARN("failed to get index cols", K(ret));
      } else {
        int order_column_idx = 0, index_column_idx = 0;
        bool match_order_prefix = false;
        while (order_column_idx < order_column_ids->count() && index_column_idx < index_column_ids->count()) {
          if (match_order_prefix && index_column_ids->at(index_column_idx) == order_column_ids->at(order_column_idx)) {
            index_column_idx++;
            order_column_idx++;
          } else if (!match_order_prefix && index_column_ids->at(index_column_idx) == order_column_ids->at(order_column_idx)) {
            match_order_prefix = true;
            index_column_idx++;
            order_column_idx++;
          } else if (!match_order_prefix && ObOptimizerUtil::find_item(*equal_column_ids, index_column_ids->at(index_column_idx))) {
            index_column_idx++;
          } else {
            break;
          }
        }
        if (order_column_idx == order_column_ids->count()) {
          found_index = true;
        }
      }
    }
  }

  return ret;
}

}
}


