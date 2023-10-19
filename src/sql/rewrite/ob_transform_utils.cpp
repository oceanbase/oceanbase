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

#include "common/ob_common_utility.h"
#include "common/ob_smart_call.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "common/ob_common_utility.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_insert_all_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_rule.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/rewrite/ob_equal_analysis.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {
int ObTransformUtils::LazyJoinInfo::assign(const LazyJoinInfo &other)
{
  int ret = OB_SUCCESS;
  right_table_ = other.right_table_;
  if (OB_FAIL(join_conditions_.assign(other.join_conditions_))) {
    LOG_WARN("failed to assign conditions", K(ret));
  }
  return ret;
}

int ObTransformUtils::decorrelate(ObRawExpr *&expr,
                                  ObIArray<ObExecParamRawExpr *> &exec_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_exec_param_expr()) {
    ObExecParamRawExpr *exec_expr = static_cast<ObExecParamRawExpr*>(expr);
    if (ObOptimizerUtil::find_item(exec_params, exec_expr)) {
      expr = exec_expr->get_ref_expr();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(SMART_CALL(decorrelate(expr->get_param_expr(i), exec_params)))) {
      LOG_WARN("failed to decorrelate expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && expr->is_query_ref_expr()) {
    if (OB_FAIL(inherit_exec_params(exec_params,
                                    static_cast<ObQueryRefRawExpr *>(expr)))) {
      LOG_WARN("failed to exec params", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::decorrelate(ObIArray<ObRawExpr *> &exprs,
                                  ObIArray<ObExecParamRawExpr *> &exec_params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(decorrelate(exprs.at(i), exec_params))) {
      LOG_WARN("failed to decorrelate exec params", K(ret));
    }
  }
  return ret;
}

// no need to iterate child stmt
// it is impossible to decorreated execs params in the child stmt.
// btw, the function seems to be nonsense, how could I decorrelate a stmt?
// decorrelate only happens when a expr is moved from child stmt into its parent.
// it is not possible to decorrelate a expr without cross-query-block movement.
int ObTransformUtils::decorrelate(ObDMLStmt *stmt,
                                  ObIArray<ObExecParamRawExpr *> &exec_params)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExprPointer, 16> relation_expr_ptrs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_expr_ptrs))) {
    LOG_WARN("failed to get relation expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_expr_ptrs.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_FAIL(relation_expr_ptrs.at(i).get(expr))) {
      LOG_WARN("failed to get expr", K(ret));
    } else if (OB_FAIL(decorrelate(expr, exec_params))) {
      LOG_WARN("failed to decorrelat expr", K(ret));
    } else if (OB_FAIL(relation_expr_ptrs.at(i).set(expr))) {
      LOG_WARN("failed to set expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::inherit_exec_params(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                          ObQueryRefRawExpr *query_ref)
{
  int ret = OB_SUCCESS;
  ObArray<ObExecParamRawExpr *> used_exec_params;
  if (OB_FAIL(get_exec_params(exec_params, query_ref->get_ref_stmt(), used_exec_params))) {
    LOG_WARN("failed to get exec params", K(ret));
  } else if (OB_FAIL(query_ref->add_exec_param_exprs(used_exec_params))) {
    LOG_WARN("failed to add exec param exprs", K(ret));
  }
  return ret;
}

int ObTransformUtils::get_exec_params(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                      ObRawExpr *expr,
                                      ObIArray<ObExecParamRawExpr *> &used_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_exec_param_expr() &&
             ObRawExprUtils::find_expr(exec_params, expr) &&
             !ObRawExprUtils::find_expr(used_params, expr)) {
    if (OB_FAIL(used_params.push_back(static_cast<ObExecParamRawExpr *>(expr)))) {
      LOG_WARN("failed to append array no dup", K(ret));
    }
  } else if (expr->is_query_ref_expr()) {
    if (OB_FAIL(get_exec_params(exec_params,
                                static_cast<ObQueryRefRawExpr *>(expr)->get_ref_stmt(),
                                used_params))) {
      LOG_WARN("failed to get exec params from sub stmt", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(get_exec_params(exec_params, expr->get_param_expr(i), used_params))) {
      LOG_WARN("failed to get exec params", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_exec_params(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                      ObSelectStmt *stmt,
                                      ObIArray<ObExecParamRawExpr *> &used_params)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> exprs;
  ObArray<ObSelectStmt *> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (OB_FAIL(stmt->get_from_subquery_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(get_exec_params(exec_params, exprs.at(i), used_params))) {
      LOG_WARN("failed to get exec params", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_FAIL(get_exec_params(exec_params, child_stmts.at(i), used_params))) {
      LOG_WARN("failed to get exec params", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::is_correlated_expr(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                         const ObRawExpr *expr,
                                         bool &bret)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (exec_params.empty()) {
    // do nothing
  } else if (expr->is_exec_param_expr()) {
    bret = ObRawExprUtils::find_expr(exec_params, expr);
  } else if (expr->has_flag(CNT_DYNAMIC_PARAM) ||
             expr->has_flag(CNT_SUB_QUERY) ||
             expr->is_query_ref_expr()) {
    for (int64_t i = 0; OB_SUCC(ret) && !bret && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_correlated_expr(exec_params, expr->get_param_expr(i), bret)))) {
        LOG_WARN("failed to check param expr is correlaetd", K(ret));
      }
    }
    if (OB_SUCC(ret) && expr->is_query_ref_expr()) {
      const ObSelectStmt *subquery = static_cast<const ObQueryRefRawExpr *>(expr)->get_ref_stmt();
      if (OB_FAIL(SMART_CALL(is_correlated_subquery(exec_params, subquery, bret)))) {
        LOG_WARN("failed to check is correlated", K(ret));
      }
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObTransformUtils::is_correlated_subquery(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                             const ObSelectStmt *stmt,
                                             bool &bret)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> relation_exprs;
  ObArray<ObSelectStmt *> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (exec_params.empty()) {
    // do nothing
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (OB_FAIL(stmt->get_from_subquery_stmts(child_stmts))) {
    LOG_WARN("failed to get from subquery stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !bret && i < relation_exprs.count(); ++i) {
    if (OB_FAIL(SMART_CALL(is_correlated_expr(exec_params, relation_exprs.at(i), bret)))) {
      LOG_WARN("failed to check is correlated", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !bret && i < child_stmts.count(); ++i) {
    if (OB_FAIL(SMART_CALL(is_correlated_subquery(exec_params, child_stmts.at(i), bret)))) {
      LOG_WARN("failed to check is correlated", K(ret));
    }
  }
  return ret;
}


// left op right
// left is column, and right is correlated const/const expr, vise versa
int ObTransformUtils::is_simple_correlated_pred(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                                ObRawExpr *cond,
                                                ObColumnRefRawExpr *&col_expr,
                                                ObRawExpr *&const_expr)
{
  int ret = OB_SUCCESS;
  col_expr = NULL;
  const_expr = NULL;
  if (OB_ISNULL(cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("condition is null", K(ret));
  } else if (IS_COMMON_COMPARISON_OP(cond->get_expr_type())) {
    ObRawExpr *left = NULL;
    ObRawExpr *right = NULL;
    if (OB_ISNULL(left = cond->get_param_expr(0)) ||
        OB_ISNULL(right = cond->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(left), K(right));
    } else if (left->is_column_ref_expr() &&
               right->is_const_expr()) {
      col_expr = static_cast<ObColumnRefRawExpr *>(left);
      const_expr = right;
    } else if (right->is_column_ref_expr() &&
               left->is_const_expr()) {
      col_expr = static_cast<ObColumnRefRawExpr *>(right);
      const_expr = left;
    }
    if (OB_SUCC(ret) && const_expr != NULL) {
      bool is_correlated = false;
      if (OB_FAIL(is_correlated_expr(exec_params, const_expr, is_correlated))) {
        LOG_WARN("failed to check is correlated expr", K(ret));
      } else if (!is_correlated) {
        col_expr = NULL;
        const_expr = NULL;
      }
    }
  }
  return ret;
}

int ObTransformUtils::mark_correlated_expr(ObRawExpr *expr, ObIArray<ObRawExpr *> &all_mark_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->has_flag(BE_USED)) {
    // do nothing
  } else if (!expr->has_flag(CNT_DYNAMIC_PARAM) && !expr->has_flag(CNT_SUB_QUERY)) {
    // do nothing
  } else {
    bool is_marked = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(mark_correlated_expr(expr->get_param_expr(i), all_mark_exprs)))) {
        LOG_WARN("failed to mark correlated expr", K(ret));
      } else if (expr->get_param_expr(i)->has_flag(BE_USED)) {
        is_marked = true;
      }
    }
    if (OB_SUCC(ret) && expr->is_query_ref_expr()) {
      bool is_substmt_marked = false;
      if (OB_FAIL(mark_correlated_expr(static_cast<ObQueryRefRawExpr *>(expr)->get_ref_stmt(),
                                       is_substmt_marked,
                                       all_mark_exprs))) {
        LOG_WARN("failed to mark all expr in stmt", K(ret));
      } else if (is_substmt_marked) {
        is_marked = true;
      }
    }
    if (OB_SUCC(ret) && is_marked) {
      if (OB_FAIL(expr->add_flag(BE_USED))) {
        LOG_WARN("failed to add flag", K(ret));
      } else if (OB_FAIL(all_mark_exprs.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::mark_correlated_expr(ObSelectStmt *stmt,
                                           bool &is_stmt_marked,
                                           ObIArray<ObRawExpr *> &all_mark_exprs)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> relation_exprs;
  ObArray<ObSelectStmt *> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (OB_FAIL(stmt->get_from_subquery_stmts(child_stmts))) {
    LOG_WARN("failed to get from subquery stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    if (OB_FAIL(SMART_CALL(mark_correlated_expr(relation_exprs.at(i), all_mark_exprs)))) {
      LOG_WARN("failed to check is correlated", K(ret));
    } else if (relation_exprs.at(i)->has_flag(BE_USED)) {
      is_stmt_marked = true;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    bool is_substmt_marked = false;
    if (OB_FAIL(SMART_CALL(mark_correlated_expr(child_stmts.at(i),
                                                is_substmt_marked,
                                                all_mark_exprs)))) {
      LOG_WARN("failed to check is correlated", K(ret));
    } else if (is_substmt_marked) {
      is_stmt_marked = true;
    }
  }
  return ret;
}

int ObTransformUtils::add_correlated_flag(const ObIArray<ObExecParamRawExpr *> &param_exprs,
                                          ObIArray<ObRawExpr *> &all_mark_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); ++i) {
    if (OB_ISNULL(param_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret));
    } else if (param_exprs.at(i)->has_flag(BE_USED)) {
      // do nothing
    } else if (OB_FAIL(param_exprs.at(i)->add_flag(BE_USED))) {
      LOG_WARN("failed to add be used flag", K(ret));
    } else if (OB_FAIL(all_mark_exprs.push_back(param_exprs.at(i)))) {
      LOG_WARN("failed to push back param expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::unmark_correlated_expr(ObIArray<ObRawExpr *> &all_mark_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_mark_exprs.count(); ++i) {
    if (OB_FAIL(all_mark_exprs.at(i)->clear_flag(BE_USED))) {
      LOG_WARN("failed to clear flag", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::has_nested_subquery(const ObQueryRefRawExpr *query_ref,
                                          bool &has_nested)
{
  int ret = OB_SUCCESS;
  has_nested = false;
  if (OB_ISNULL(query_ref)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ref is null", K(ret), K(query_ref));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_nested && i < query_ref->get_param_count(); ++i) {
    const ObRawExpr *param = NULL;
    if (OB_ISNULL(param = query_ref->get_param_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret));
    } else {
      has_nested = param->has_flag(CNT_SUB_QUERY);
    }
  }
  return ret;
}

//判断expr是否为column expr, 且为primary key或unique index
int ObTransformUtils::is_column_unique(const ObRawExpr *expr,
                                       uint64_t table_id,
                                       ObSchemaChecker *schema_checker,
                                       ObSQLSessionInfo *session_info,
                                       bool &is_unique)
{
  int ret = OB_SUCCESS;
  is_unique = false;
  ObSEArray<ObRawExpr *, 1> expr_array;
  if (OB_FAIL(expr_array.push_back(const_cast<ObRawExpr *>(expr)))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(is_columns_unique(
                     expr_array, table_id, schema_checker, session_info, is_unique))) {
    LOG_WARN("failed to check is columns unique", K(ret));
  }
  return ret;
}

int ObTransformUtils::is_columns_unique(const ObIArray<ObRawExpr *> &exprs,
                                        uint64_t table_id,
                                        ObSchemaChecker *schema_checker,
                                        ObSQLSessionInfo *session_info,
                                        bool &is_unique)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  is_unique = false;
  if (OB_ISNULL(schema_checker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema checker is null", K(ret));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session_info is null", K(ret));
  } else if (OB_FAIL(schema_checker->get_table_schema(session_info->get_effective_tenant_id(), table_id, table_schema))){
    LOG_WARN("failed to get table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema should not be null", K(table_id));
  } else {
    if (table_schema->get_rowkey_column_num() > 0
        && OB_FAIL(exprs_has_unique_subset(exprs, table_schema->get_rowkey_info(), is_unique))) {
      LOG_WARN("failed to check rowkey", K(ret));
    //new heap table not add partition key in rowkey and the tablet id is unique in partition,
    //we need check partition key
    } else if (is_unique && table_schema->is_heap_table() &&
               table_schema->get_partition_key_info().is_valid() &&
               OB_FAIL(exprs_has_unique_subset(exprs, table_schema->get_partition_key_info(), is_unique))) {
      LOG_WARN("failed to check rowkey", K(ret));
    } else if (is_unique && table_schema->is_heap_table() &&
               table_schema->get_subpartition_key_info().is_valid() &&
               OB_FAIL(exprs_has_unique_subset(exprs, table_schema->get_subpartition_key_info(), is_unique))) {
      LOG_WARN("failed to check rowkey", K(ret));
    } else if (!is_unique) {
      ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
      if (OB_FAIL(table_schema->get_simple_index_infos(
                  simple_index_infos, false))) {
        LOG_WARN("get simple_index_infos failed", K(ret));
      }
      for (int64_t i = 0;
          OB_SUCC(ret) && !is_unique && i < simple_index_infos.count();
          ++i) {
        const ObTableSchema *index_schema = NULL;
        if (OB_FAIL(schema_checker->get_table_schema(table_schema->get_tenant_id(),
                    simple_index_infos.at(i).table_id_, index_schema))) {
          LOG_WARN("failed to get table schema", K(ret),
                   "index_id", simple_index_infos.at(i).table_id_);
        } else if (OB_ISNULL(index_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index schema should not be null", K(ret));
        } else if (index_schema->is_unique_index() && index_schema->get_index_column_num() > 0) {
          const ObIndexInfo &index_info = index_schema->get_index_info();
          if (OB_FAIL(exprs_has_unique_subset(exprs, index_info, is_unique))) {
            LOG_WARN("failed to check subset", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::exprs_has_unique_subset(const common::ObIArray<ObRawExpr*> &full,
                                              const common::ObRowkeyInfo &sub,
                                              bool &is_subset)
{
  int ret = OB_SUCCESS;
  is_subset = true;
  // 使用时注意 sub 为空集的情况.
  // 注意提前验证 sub 的所在的 schema 有 full 中的列.
  for (int64_t i = 0; OB_SUCC(ret) && i < sub.get_size(); i++) {
    uint64_t column_id_sub = OB_INVALID_ID;
    if (OB_FAIL(sub.get_column_id(i, column_id_sub))) {
      LOG_WARN("failed to get column id", K(ret));
    } else if (OB_INVALID_ID != column_id_sub) {
      bool is_find = false;
      for (int64_t j = 0; !is_find && OB_SUCC(ret) && j < full.count(); j++) {
        if (OB_ISNULL(full.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL expr", K(ret));
        } else if (full.at(j)->has_flag(IS_COLUMN)
                   && static_cast<ObColumnRefRawExpr*>(full.at(j))->get_column_id() == column_id_sub) {
          is_find = true;
        }
      }
      if (OB_SUCC(ret) && !is_find) {
        is_subset = false;
      }
    }
  }
  return ret;
}

/**
 * @brief  将子查询封装成一个Generated Table，放入主查询的Table Item中
 */
int ObTransformUtils::add_new_table_item(ObTransformerCtx *ctx,
                                         ObDMLStmt *stmt,
                                         ObSelectStmt *subquery,
                                         TableItem *&new_table_item)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  if (OB_ISNULL(stmt)
      || OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_)
      || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to add new table_item because some value is null", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_STMT_ID == stmt->get_stmt_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid stmt id", K(ret), K(stmt->get_stmt_id()));
  } else if (OB_ISNULL(table_item = stmt->create_table_item(*ctx->allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create table item failed");
  } else if (OB_FAIL(stmt->generate_view_name(*ctx->allocator_,
                                              table_item->table_name_))) {
    LOG_WARN("failed to generate view name", K(ret));
  } else if (OB_FAIL(stmt->get_qb_name(table_item->qb_name_))) {
    LOG_WARN("fail to get qb_name", K(ret), K(stmt->get_stmt_id()));
  } else {
    table_item->table_id_ = stmt->get_query_ctx()->available_tb_id_--;
    table_item->type_ = TableItem::GENERATED_TABLE;
    table_item->ref_id_ = OB_INVALID_ID;
    table_item->database_name_ = ObString::make_string("");
    table_item->alias_name_ = table_item->table_name_;
    table_item->ref_query_ = subquery;
    if (OB_FAIL(stmt->set_table_bit_index(table_item->table_id_))) {
      LOG_WARN("fail to add table_id to hash table", K(ret), K(table_item));
    } else if (OB_FAIL(stmt->get_table_items().push_back(table_item))) {
      LOG_WARN("add table item failed", K(ret));
    } else {
      new_table_item = table_item;
    }
  }
  return ret;
}

int ObTransformUtils::add_new_joined_table(ObTransformerCtx *ctx,
                                           ObDMLStmt &stmt,
                                           const ObJoinType join_type,
                                           TableItem *left_table,
                                           TableItem *right_table,
                                           const ObIArray<ObRawExpr *> &joined_conds,
                                           TableItem *&new_join_table,
                                           bool add_table /* = true */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_) ||
      OB_ISNULL(left_table) || OB_ISNULL(right_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is invalid", K(ret), K(ctx), K(stmt), K(left_table), K(right_table));
  } else {
    JoinedTable *joined_table = static_cast<JoinedTable*>(ctx->allocator_->alloc(sizeof(JoinedTable)));
    if (OB_ISNULL(joined_table)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      joined_table = new (joined_table) JoinedTable();
      joined_table->type_ = TableItem::JOINED_TABLE;
      joined_table->table_id_ = stmt.get_query_ctx()->available_tb_id_ --;
      joined_table->joined_type_ = join_type;
      joined_table->left_table_ = left_table;
      joined_table->right_table_ = right_table;
      if (OB_FAIL(joined_table->join_conditions_.assign(joined_conds))) {
        LOG_WARN("failed to push back join conditions", K(ret));
      } else if (OB_FAIL(ObTransformUtils::add_joined_table_single_table_ids(*joined_table, *left_table))) {
        LOG_WARN("failed to add left table ids", K(ret));
      } else if (OB_FAIL(ObTransformUtils::add_joined_table_single_table_ids(*joined_table, *right_table))) {
        LOG_WARN("failed to add right table ids", K(ret));
      } else if (add_table && OB_FAIL(stmt.add_joined_table(joined_table))) {
        LOG_WARN("failed to add joined table into stmt", K(ret));
      } else {
        new_join_table = joined_table;
      }
    }
  }
  return ret;
}

int ObTransformUtils::create_new_column_expr(ObTransformerCtx *ctx,
                                             const TableItem &table_item,
                                             const int64_t column_id,
                                             const SelectItem &select_item,
                                             ObDMLStmt *stmt,
                                             ObColumnRefRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *new_column_ref = NULL;
  bool is_not_null = true;
  uint64_t base_table_id = OB_INVALID_ID;
  uint64_t base_column_id = OB_INVALID_ID;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_) ||
      OB_ISNULL(ctx->exec_ctx_) || OB_ISNULL(plan_ctx = ctx->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(ctx));
  } else if (OB_FAIL(ctx->expr_factory_->create_raw_expr(T_REF_COLUMN, new_column_ref))) {
    LOG_WARN("failed to create a new column ref expr", K(ret));
  } else if (OB_ISNULL(new_column_ref)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_column_ref should not be null", K(ret));
  } else if (OB_FAIL(is_expr_not_null(ctx,
                                      table_item.ref_query_,
                                      select_item.expr_,
                                      NULLABLE_SCOPE::NS_TOP,
                                      is_not_null))) {
    LOG_WARN("failed to check stmt output nullable", K(ret));
  } else {
    ObRawExpr *select_expr = select_item.expr_;
    new_column_ref->set_table_name(table_item.alias_name_);
    new_column_ref->set_column_name(select_item.alias_name_);
    new_column_ref->set_ref_id(table_item.table_id_, column_id);//only one column
    new_column_ref->set_collation_type(select_expr->get_collation_type());
    new_column_ref->set_collation_level(select_expr->get_collation_level());
    new_column_ref->set_result_type(select_expr->get_result_type());
    if (!is_not_null) {
      new_column_ref->unset_result_flag(NOT_NULL_FLAG);
    } else {
      new_column_ref->set_result_flag(NOT_NULL_FLAG);
    }
    if (OB_FAIL(new_column_ref->add_relation_id(stmt->get_table_bit_index(table_item.table_id_)))) {
      LOG_WARN("failed to add relation id", K(ret));
    } else if (select_expr->is_column_ref_expr()) {
      const ObColumnRefRawExpr *old_col = static_cast<const ObColumnRefRawExpr *>(select_expr);
      const ColumnItem *old_col_item = NULL;
      if (OB_ISNULL(table_item.ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is invalid", K(ret));
      } else if (OB_ISNULL(old_col_item = table_item.ref_query_->get_column_item_by_id(
                             old_col->get_table_id(), old_col->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column item", K(ret));
      } else {
        base_table_id = old_col_item->base_tid_;
        base_column_id = old_col_item->base_cid_;
      }
    }
    if (OB_SUCC(ret)) {
      ColumnItem column_item;
      column_item.column_name_ = select_item.alias_name_;
      column_item.expr_ = new_column_ref;
      column_item.table_id_ = table_item.table_id_;
      column_item.column_id_ = column_id;
      column_item.base_tid_ = base_table_id;
      column_item.base_cid_ = base_column_id;
      if (OB_FAIL(stmt->add_column_item(column_item))) {
        LOG_WARN("failed to add column item", K(column_item), K(ret));
      } else if (OB_FAIL(new_column_ref->formalize(ctx->session_info_))) {
        LOG_WARN("failed to formalize a new expr", K(ret));
      } else {
        new_expr = new_column_ref;
      }
    }
  }
  return ret;
}

/*
 *  这里默认调用此函数是为了生成一个新的joined table。
 *  由于当前stmt的joined_tables_中保存的是最顶层的joined table, 因此在生成一个整体的inner join
 *  table 后将stmt的joined_tables_清空, 用于保存接下来生成的新的joined table
 */
int ObTransformUtils::merge_from_items_as_inner_join(ObTransformerCtx *ctx_,
                                                     ObDMLStmt &stmt,
                                                     TableItem *&ret_table)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> dummy_conds;
  ret_table = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_from_item_size(); ++i) {
    const FromItem &item = stmt.get_from_item(i);
    TableItem *table_item = NULL;
    if (item.is_joined_) {
      JoinedTable *joined_table = stmt.get_joined_table(item.table_id_);
      table_item = joined_table;
      if (OB_FAIL(ObOptimizerUtil::remove_item(stmt.get_joined_tables(), joined_table))) {
        LOG_WARN("failed to remove joined table", K(ret));
      }
    } else {
      table_item = stmt.get_table_item_by_id(item.table_id_);
    }
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_ISNULL(ret_table)) {
      ret_table = table_item;
    } else if (OB_FAIL(add_new_joined_table(ctx_, stmt, INNER_JOIN, ret_table,
                                            table_item, dummy_conds, ret_table,
                                            false /* add_table */))) {
      LOG_WARN("failed to add new joined table", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ret_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no associated table item is found", K(ret));
    } else {
      stmt.get_from_items().reset();
    }
  }
  return ret;
}

int ObTransformUtils::create_columns_for_view(ObTransformerCtx *ctx,
                                              TableItem &view_table_item,
                                              ObDMLStmt *stmt,
                                              ObIArray<ObRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  uint64_t candi_column_id = OB_APP_MIN_COLUMN_ID;
  ObSelectStmt *subquery = NULL;
  if (OB_UNLIKELY(!view_table_item.is_generated_table() &&
      !view_table_item.is_temp_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table type", K(view_table_item.type_), K(ret));
  } else if (OB_ISNULL(subquery = view_table_item.ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObSEArray<ObColumnRefRawExpr*, 4> table_columns;
    if (OB_FAIL(stmt->get_column_exprs(view_table_item.table_id_, table_columns))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_select_item_size(); ++i) {
        const SelectItem &select_item = subquery->get_select_item(i);
        ObColumnRefRawExpr *output_column = NULL;
        bool is_have = false;
        for (int64_t j = 0; OB_SUCC(ret) && !is_have && j < table_columns.count(); ++j) {
          output_column = table_columns.at(j);
          if (OB_ISNULL(output_column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected table column null ptr", K(output_column), K(ret));
          } else if (output_column->get_column_id() == candi_column_id) {
            is_have = true;
          } else {/*do nothing*/}
        }
        if (OB_SUCC(ret)) {
          if (is_have) {
            if (OB_FAIL(column_exprs.push_back(output_column))) {
              LOG_WARN("failed to push back column expr", K(ret));
            } else {
              candi_column_id++;
            }
          } else if (OB_FAIL(ObTransformUtils::create_new_column_expr(ctx, view_table_item,
                            candi_column_id++, select_item, stmt, output_column))) {
            LOG_WARN("failed to create column expr for view", K(ret));
          } else if (OB_FAIL(column_exprs.push_back(output_column))) {
            LOG_WARN("failed to push back column expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformUtils::create_columns_for_view
 * add new select exprs into a view stmt, and
 * create columns output for the generated table
 * @return
 */
int ObTransformUtils::create_columns_for_view(ObTransformerCtx *ctx,
                                              TableItem &view_table_item,
                                              ObDMLStmt *stmt,
                                              ObIArray<ObRawExpr *> &new_select_list,
                                              ObIArray<ObRawExpr *> &new_column_list,
                                              bool ignore_dup_select_expr,
                                              bool repeated_select) //create select item and new column
{
  int ret = OB_SUCCESS;
  ObSelectStmt *view_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(view_stmt = view_table_item.ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(ctx), K(view_table_item.ref_query_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < new_select_list.count(); ++i) {
    ObRawExpr *expr = NULL;
    ObColumnRefRawExpr *col = NULL;
    int64_t idx = -1;
    if (OB_ISNULL(expr = new_select_list.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    }
    for (idx = (repeated_select ? view_stmt->get_select_item_size() : 0);
         OB_SUCC(ret) && idx < view_stmt->get_select_item_size() &&
         (expr != view_stmt->get_select_item(idx).expr_ || !ignore_dup_select_expr);
         ++idx);

    if (OB_SUCC(ret)) {
      uint64_t column_id = OB_APP_MIN_COLUMN_ID + idx;
      if (idx >= 0 && idx < view_stmt->get_select_item_size()) {
        if (OB_NOT_NULL(col = stmt->get_column_expr_by_id(view_table_item.table_id_, column_id))) {
          //do nothing
        } else if (OB_FAIL(create_new_column_expr(
                             ctx, view_table_item, column_id,
                             view_stmt->get_select_item(idx), stmt, col))) {
          LOG_WARN("failed to create new column expr", K(ret));
        }
      } else {

        if (OB_FAIL(create_select_item(*ctx->allocator_, expr, view_stmt))) {
          LOG_WARN("failed to create select item", K(ret));
        } else if (OB_FAIL(create_new_column_expr(
                             ctx, view_table_item, column_id,
                             view_stmt->get_select_item(idx), stmt, col))) {
          LOG_WARN("failed to create new column expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(new_column_list.push_back(col))) {
        LOG_WARN("failed to push back column expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::create_select_item(ObIAllocator &allocator,
                                         const ObIArray<ObRawExpr*> &select_exprs,
                                         ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); i++) {
    if (OB_ISNULL(select_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(allocator,
                                                            select_exprs.at(i),
                                                            select_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformUtils::create_select_item(ObIAllocator &allocator,
                                         ObIArray<ColumnItem> &column_items,
                                         ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); i++) {
    if (OB_ISNULL(column_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(allocator,
                                                            column_items.at(i).expr_,
                                                            select_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformUtils::create_select_item(ObIAllocator &allocator,
                                         ObRawExpr *select_expr,
                                         ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(select_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or expr is null", K(ret), K(select_stmt), K(select_expr));
  } else {
    ObString alias_name = select_expr->get_expr_name();
    char name_buf[64];
    int64_t pos = 0;
    if (alias_name.empty()) {
      if (OB_FAIL(select_expr->get_name(name_buf, 64, pos))) {
        ret = OB_SUCCESS;
        pos = sprintf(name_buf, "SEL_%ld", select_stmt->get_select_item_size() + 1);
        pos = (pos < 0 || pos >= 64) ? 0 : pos;
      }
      alias_name.assign(name_buf, static_cast<int32_t>(pos));
    }
    if (OB_FAIL(ob_write_string(allocator, alias_name, alias_name))) {
      LOG_WARN("failed to write string", K(ret));
    } else {
      SelectItem select_item;
      select_item.expr_ = select_expr;
      select_item.expr_name_ = select_expr->get_expr_name();
      select_item.alias_name_ = alias_name;
      if (OB_FAIL(select_stmt->add_select_item(select_item))) {
        LOG_WARN("failed to add select item", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::refresh_select_items_name(ObIAllocator &allocator, ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<SelectItem, 4> select_list;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or expr is null", K(ret), K(select_stmt));
  } else {
    ObIArray<SelectItem> &select_list = select_stmt->get_select_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_list.count(); ++i) {
      ObRawExpr *expr = select_list.at(i).expr_;
      char name_buf[64];
      int64_t pos = 0;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected NULL", K(ret));
      } else {
        ObString alias_name = expr->get_expr_name();
        if (alias_name.empty()) {
          if (OB_FAIL(expr->get_name(name_buf, 64, pos))) {
            ret = OB_SUCCESS;
            pos = sprintf(name_buf, "SEL_%ld", select_stmt->get_select_item_size() + 1);
            pos = (pos < 0 || pos >= 64) ? 0 : pos;
          }
          alias_name.assign(name_buf, static_cast<int32_t>(pos));
        }
        if (OB_FAIL(ob_write_string(allocator, alias_name, alias_name))) {
          LOG_WARN("failed to write string", K(ret));
        } else {
          select_list.at(i).expr_name_ = expr->get_expr_name();
          select_list.at(i).alias_name_ = alias_name;
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::refresh_column_items_name(ObSelectStmt *stmt, int64_t table_id)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *set_view_stmt = NULL;
  ObSEArray<ColumnItem, 4> table_columns;
  uint64_t candi_column_id = OB_APP_MIN_COLUMN_ID;
  if (OB_ISNULL(stmt) ||
      OB_ISNULL(stmt->get_table_item_by_id(table_id)) ||
      OB_ISNULL(set_view_stmt = stmt->get_table_item_by_id(table_id)->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected NULL", K(ret));
  } else if (OB_FAIL(stmt->get_column_items(table_id, table_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < set_view_stmt->get_select_item_size(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret)  && j < table_columns.count(); ++j) {
        ObColumnRefRawExpr *col = table_columns.at(j).expr_;
        if (OB_ISNULL(col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected table column null ptr", K(ret), K(col));
        } else if (col->get_column_id() == candi_column_id) {
          col->set_column_name(set_view_stmt->get_select_item(i).alias_name_);
          stmt->get_column_item(table_id, col->get_column_id())->column_name_ = set_view_stmt->get_select_item(i).alias_name_;
          candi_column_id++;
          break;
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_real_alias_name(ObSelectStmt *stmt, int64_t sel_idx, ObString& alias_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stmt", K(ret));
  } else if (stmt->is_set_stmt()) {
    if (OB_FAIL(SMART_CALL(get_real_alias_name(stmt->get_set_query(0), sel_idx, alias_name)))) {
      LOG_WARN("failed to get set query", K(ret));
    }
  } else if (sel_idx < 0 || sel_idx >= stmt->get_select_item_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected select item idx", K(ret));
  } else {
    alias_name = stmt->get_select_item(sel_idx).alias_name_;
  }
  return ret;
}

int ObTransformUtils::copy_stmt(ObStmtFactory &stmt_factory,
                                const ObDMLStmt *stmt,
                                ObDMLStmt *&dml_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt *select_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(select_stmt))) {
      LOG_WARN("failed to create select stmt", K(ret));
    } else if (OB_FAIL(select_stmt->assign(static_cast<const ObSelectStmt&>(*stmt)))) {
      LOG_WARN("failed to assign select stmt", K(ret));
    } else {
      dml_stmt = select_stmt;
    }
  } else if (stmt->is_update_stmt()) {
    ObUpdateStmt *update_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(update_stmt))) {
      LOG_WARN("failed to create update stmt", K(ret));
    } else if (OB_FAIL(update_stmt->assign(static_cast<const ObUpdateStmt&>(*stmt)))) {
      LOG_WARN("failed to assign update stmt", K(ret));
    } else {
      dml_stmt = update_stmt;
    }
  } else if (stmt->is_delete_stmt()) {
    ObDeleteStmt *delete_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(delete_stmt))) {
      LOG_WARN("failed to create delete stmt", K(ret));
    } else if (OB_FAIL(delete_stmt->assign(static_cast<const ObDeleteStmt&>(*stmt)))) {
      LOG_WARN("failed to assign delete stmt", K(ret));
    } else {
      dml_stmt = delete_stmt;
    }
  } else if (stmt->is_insert_stmt()) {
    ObInsertStmt *insert_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(insert_stmt))) {
      LOG_WARN("failed to create insert stmt", K(ret));
    } else if (OB_FAIL(insert_stmt->assign(static_cast<const ObInsertStmt&>(*stmt)))) {
      LOG_WARN("failed to assign insert stmt", K(ret));
    } else {
      dml_stmt = insert_stmt;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObTransformUtils::deep_copy_stmt(ObStmtFactory &stmt_factory,
                                     ObRawExprFactory &expr_factory,
                                     const ObDMLStmt *stmt,
                                     ObDMLStmt *&dml_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt *select_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(select_stmt))) {
      LOG_WARN("failed to create select stmt", K(ret));
    } else if (OB_FAIL(SMART_CALL(select_stmt->deep_copy(stmt_factory,
                                                        expr_factory,
                                                        *stmt)))) {
      LOG_WARN("failed to deep copy select stmt", K(ret));
    } else {
      dml_stmt = select_stmt;
    }
  } else if (stmt->is_update_stmt()) {
    ObUpdateStmt *update_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(update_stmt))) {
      LOG_WARN("failed to create update stmt", K(ret));
    } else if (OB_FAIL(update_stmt->deep_copy(stmt_factory,
                                              expr_factory,
                                              *stmt))) {
      LOG_WARN("failed to deep copy update stmt", K(ret));
    } else {
      dml_stmt = update_stmt;
    }
  } else if (stmt->is_delete_stmt()) {
    ObDeleteStmt *delete_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(delete_stmt))) {
      LOG_WARN("failed to create delete stmt", K(ret));
    } else if (OB_FAIL(delete_stmt->deep_copy(stmt_factory,
                                              expr_factory,
                                              *stmt))) {
      LOG_WARN("failed to deep copy delete stmt", K(ret));
    } else {
      dml_stmt = delete_stmt;
    }
  } else if (stmt->is_insert_stmt()) {
    ObInsertStmt *insert_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(insert_stmt))) {
      LOG_WARN("failed to create insert stmt", K(ret));
    } else {
      insert_stmt->set_replace(static_cast<const ObInsertStmt*>(stmt)->is_replace());
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(insert_stmt->deep_copy(stmt_factory,
                                              expr_factory,
                                              *stmt))) {
      LOG_WARN("failed to deep copy insert stmt", K(ret));
    } else {
      dml_stmt = insert_stmt;
    }
  } else if (stmt->is_merge_stmt()) {
    ObMergeStmt *merge_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(merge_stmt))) {
      LOG_WARN("failed to create insert stmt", K(ret));
    } else if (OB_FAIL(merge_stmt->deep_copy(stmt_factory,
                                             expr_factory,
                                             *stmt))) {
      LOG_WARN("failed to deep copy insert stmt", K(ret));
    } else {
      dml_stmt = merge_stmt;
    }
  } else if (stmt->is_insert_all_stmt()) {
    ObInsertAllStmt *insert_all_stmt = NULL;
    if (OB_FAIL(stmt_factory.create_stmt(insert_all_stmt))) {
      LOG_WARN("failed to create insert all stmt", K(ret));
    } else if (OB_FAIL(insert_all_stmt->deep_copy(stmt_factory,
                                                  expr_factory,
                                                  *stmt))) {
      LOG_WARN("failed to deep copy insert all stmt", K(ret));
    } else {
      dml_stmt = insert_all_stmt;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type is not valid for deep copy", K(ret), K(stmt->get_stmt_type()));
  }
  return ret;
}


int ObTransformUtils::add_joined_table_single_table_ids(JoinedTable &joined_table, TableItem &child_table)
{
  int ret = OB_SUCCESS;
  if (child_table.is_joined_table()) {
    JoinedTable &child_joined_table = static_cast<JoinedTable &>(child_table);
    for (int64_t i = 0; OB_SUCC(ret) && i < child_joined_table.single_table_ids_.count(); ++i) {
      if (OB_FAIL(joined_table.single_table_ids_.push_back(child_joined_table.single_table_ids_.at(i)))) {
        LOG_WARN("push back single table id failed", K(ret));
      }
    }
  } else if (OB_FAIL(joined_table.single_table_ids_.push_back(child_table.table_id_))) {
    LOG_WARN("push back single table id failed", K(ret));
  }
  return ret;
}

int ObTransformUtils::replace_expr(ObRawExpr *old_expr, ObRawExpr *new_expr, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 1> old_exprs;
  ObSEArray<ObRawExpr*, 1> new_exprs;
  if (OB_FAIL(old_exprs.push_back(old_expr)) || OB_FAIL(new_exprs.push_back(new_expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(replace_expr(old_exprs, new_exprs, expr))) {
    LOG_WARN("failed to replace expr", K(ret));
  }
  return ret;
}

int ObTransformUtils::replace_expr(const ObIArray<ObRawExpr *> &other_exprs,
                                   const ObIArray<ObRawExpr *> &new_exprs,
                                   ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(expr)) {
    ObRawExpr *temp_old_expr = NULL;
    int64_t idx = -1;
    if (ObOptimizerUtil::find_item(other_exprs, expr, &idx)) {
      if (OB_UNLIKELY(idx < 0 || idx >= new_exprs.count()) ||
          OB_ISNULL(new_exprs.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index", K(ret), K(idx), K(new_exprs.count()), K(new_exprs));
      } else {
        expr = new_exprs.at(idx);
        temp_old_expr = other_exprs.at(idx);
        const_cast<ObIArray<ObRawExpr*>&>(other_exprs).at(idx) = NULL;
      }
    } else if (ObOptimizerUtil::find_item(new_exprs, expr, &idx)) {
      if (OB_UNLIKELY(idx < 0 || idx >= other_exprs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index", K(ret), K(idx), K(*expr), K(new_exprs), K(other_exprs));
      } else {
        temp_old_expr = other_exprs.at(idx);
        const_cast<ObIArray<ObRawExpr*>&>(other_exprs).at(idx) = NULL;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(SMART_CALL(expr->replace_expr(other_exprs, new_exprs)))) {
        LOG_WARN("failed to replace expr", K(ret));
      } else if (NULL != temp_old_expr) {
        const_cast<ObIArray<ObRawExpr*>&>(other_exprs).at(idx) = temp_old_expr;
      }
    }
  }
  return ret;
}

int ObTransformUtils::replace_expr_for_order_item(const ObIArray<ObRawExpr *> &other_exprs,
                                                  const ObIArray<ObRawExpr *> &new_exprs,
                                                  ObIArray<OrderItem> &order_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); i++) {
    if (OB_ISNULL(order_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs,
                                                      new_exprs,
                                                      order_items.at(i).expr_))) {
      LOG_WARN("failed to replace column expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_from_item(const ObIArray<FromItem> &other_from_items,
                                                    const uint64_t old_table_id,
                                                    const uint64_t new_table_id,
                                                    common::ObIArray<FromItem> &from_items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other_from_items.count() != from_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal from items count", K(from_items.count()),
        K(other_from_items.count()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_from_items.count(); i++) {
    if (other_from_items.at(i).table_id_ == old_table_id) {
      from_items.at(i).table_id_ = new_table_id;
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_joined_tables(const ObIArray<JoinedTable*> &other_joined_tables,
                                                        const uint64_t old_table_id,
                                                        const uint64_t new_table_id,
                                                        ObIArray<JoinedTable*> &joined_tables)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other_joined_tables.count() != joined_tables.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal joined tables count", K(other_joined_tables.count()),
         K(joined_tables.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < other_joined_tables.count(); i++) {
      if (OB_ISNULL(other_joined_tables.at(i)) || OB_ISNULL(joined_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null joined table", K(other_joined_tables.at(i)),
            K(joined_tables.at(i)), K(ret));
      } else if (OB_FAIL((update_table_id_for_joined_table(*other_joined_tables.at(i),
                                                           old_table_id, new_table_id,
                                                           *joined_tables.at(i))))) {
        LOG_WARN("failed to update table id for joined table", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_joined_table(const JoinedTable &other,
                                                       const uint64_t old_table_id,
                                                       const uint64_t new_table_id,
                                                       JoinedTable &current)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(other.left_table_) || OB_ISNULL(other.right_table_) ||
      OB_ISNULL(current.left_table_) || OB_ISNULL(current.right_table_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null table item", K(ret), K(other.left_table_),
        K(other.right_table_), K(current.left_table_), K(current.right_table_));
  } else if (OB_FAIL(update_table_id(other.single_table_ids_,
                                     old_table_id, new_table_id,
                                     current.single_table_ids_))) {
    LOG_WARN("failed to update table id array", K(ret));
  } else if (other.table_id_ == old_table_id) {
    current.table_id_ = new_table_id;
  } else { /*do nothing*/ }

  if (OB_SUCC(ret) && other.left_table_->is_joined_table() &&
      current.left_table_->is_joined_table()) {
    if (OB_FAIL(SMART_CALL(update_table_id_for_joined_table(static_cast<JoinedTable&>(*other.left_table_),
                                                            old_table_id, new_table_id,
                                                            static_cast<JoinedTable&>(*current.left_table_))))) {
      LOG_WARN("failed to update table if for joined table", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret) && other.right_table_->is_joined_table() &&
      current.right_table_->is_joined_table()) {
    if (OB_FAIL(SMART_CALL(update_table_id_for_joined_table(static_cast<JoinedTable&>(*other.right_table_),
                                                            old_table_id, new_table_id,
                                                            static_cast<JoinedTable&>(*current.right_table_))))) {
      LOG_WARN("failed to update table if for joined table", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_part_item(const ObIArray<ObDMLStmt::PartExprItem> &other_part_expr_items,
                                                    const uint64_t old_table_id,
                                                    const uint64_t new_table_id,
                                                    ObIArray<ObDMLStmt::PartExprItem> &part_expr_items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other_part_expr_items.count() != part_expr_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal part expr items count", K(other_part_expr_items.count()),
        K(part_expr_items.count()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_part_expr_items.count(); i++) {
    if (other_part_expr_items.at(i).table_id_ == old_table_id) {
      part_expr_items.at(i).table_id_ = new_table_id;
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_check_constraint_items(
    const common::ObIArray<ObDMLStmt::CheckConstraintItem> &other_check_constraint_items,
    const uint64_t old_table_id,
    const uint64_t new_table_id,
    common::ObIArray<ObDMLStmt::CheckConstraintItem> &check_constraint_items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other_check_constraint_items.count() != check_constraint_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal check constraint items", K(other_check_constraint_items.count()),
        K(check_constraint_items.count()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_check_constraint_items.count(); i++) {
    if (other_check_constraint_items.at(i).table_id_ == old_table_id) {
      check_constraint_items.at(i).table_id_ = new_table_id;
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_semi_info(const ObIArray<SemiInfo*> &other_semi_infos,
                                                    const uint64_t old_table_id,
                                                    const uint64_t new_table_id,
                                                    ObIArray<SemiInfo*> &semi_infos)
{
  int ret = OB_SUCCESS;
  SemiInfo *other = NULL;
  SemiInfo *current = NULL;
  if (OB_UNLIKELY(other_semi_infos.count() != semi_infos.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal semi infos count", K(other_semi_infos.count()),
        K(semi_infos.count()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_semi_infos.count(); i++) {
    if (OB_ISNULL(other = other_semi_infos.at(i)) || OB_ISNULL(current = semi_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null semi info", K(other), K(current), K(ret));
    } else if (OB_FAIL(update_table_id(other->left_table_ids_, old_table_id, new_table_id,
                                       current->left_table_ids_))) {
      LOG_WARN("failed to update table id array", K(ret));
    } else if (other->right_table_id_ == old_table_id) {
      current->right_table_id_ = new_table_id;
    }
  }
  return ret;
}


int ObTransformUtils::update_table_id_for_column_item(const ObIArray<ColumnItem> &other_column_items,
                                                      const uint64_t old_table_id,
                                                      const uint64_t new_table_id,
                                                      const int32_t old_bit_id,
                                                      const int32_t new_bit_id,
                                                      ObIArray<ColumnItem> &column_items)
{
  int ret = OB_SUCCESS;
  bool has_bit_index = false;
  if (OB_INVALID_ID != old_bit_id && OB_INVALID_ID != new_bit_id) {
    has_bit_index = true;
  }
  if (OB_UNLIKELY(other_column_items.count() != column_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal column item count", K(other_column_items.count()),
        K(column_items.count()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_column_items.count(); i++) {
    if (OB_ISNULL(other_column_items.at(i).expr_) ||
        OB_ISNULL(column_items.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null column expr", K(other_column_items.at(i)),
          K(column_items.at(i)), K(ret));
    } else {
      if (other_column_items.at(i).table_id_ == old_table_id) {
        column_items.at(i).table_id_ = new_table_id;
      } else { /*do nothing*/ }
      if (other_column_items.at(i).expr_->get_table_id() == old_table_id) {
        column_items.at(i).expr_->set_table_id(new_table_id);
      } else { /*do nothing*/ }
      if (has_bit_index &&
          OB_FAIL(update_table_id_index(other_column_items.at(i).expr_->get_relation_ids(),
                                        old_bit_id, new_bit_id,
                                        column_items.at(i).expr_->get_relation_ids()))) {
        LOG_WARN("failed to add table id index", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_for_pseudo_columns(const ObIArray<ObRawExpr*> &other_pseudo_columns,
                                                         const uint64_t old_table_id,
                                                         const uint64_t new_table_id,
                                                         const int32_t old_bit_id,
                                                         const int32_t new_bit_id,
                                                         ObIArray<ObRawExpr*> &pseudo_columns)
{
  int ret = OB_SUCCESS;
  bool has_bit_index = false;
  if (OB_INVALID_ID != old_bit_id && OB_INVALID_ID != new_bit_id) {
    has_bit_index = true;
  }
  if (OB_UNLIKELY(other_pseudo_columns.count() != pseudo_columns.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal pseudo columns count", K(other_pseudo_columns.count()),
        K(pseudo_columns.count()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_pseudo_columns.count(); ++i) {
    if (OB_ISNULL(other_pseudo_columns.at(i)) ||
        OB_ISNULL(pseudo_columns.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pseudo column", K(other_pseudo_columns.at(i)),
          K(pseudo_columns.at(i)), K(ret));
    } else if (T_ORA_ROWSCN == other_pseudo_columns.at(i)->get_expr_type()
               && T_ORA_ROWSCN == pseudo_columns.at(i)->get_expr_type()) {
      ObPseudoColumnRawExpr *pseudo_col1 = static_cast<ObPseudoColumnRawExpr*>(other_pseudo_columns.at(i));
      ObPseudoColumnRawExpr *pseudo_col2 = static_cast<ObPseudoColumnRawExpr*>(pseudo_columns.at(i));
      if (pseudo_col1->get_table_id() == old_table_id) {
        pseudo_col2->set_table_id(new_table_id);
      } else { /*do nothing*/ }
      if (has_bit_index &&
          OB_FAIL(update_table_id_index(pseudo_col1->get_relation_ids(),
                                        old_bit_id, new_bit_id,
                                        pseudo_col2->get_relation_ids()))) {
        LOG_WARN("failed to add table id index", K(ret));
      } else { /*do nothing*/ }
    } else {
      /*do nothing*/
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id(const ObIArray<uint64_t> &old_ids,
                                      const uint64_t old_table_id,
                                      const uint64_t new_table_id,
                                      ObIArray<uint64_t> &new_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(old_ids.count() != new_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have equal ids count", K(old_ids.count()), K(new_ids.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < old_ids.count(); i++) {
      if (old_ids.at(i) == old_table_id) {
        new_ids.at(i) = new_table_id;
      }
    }
  }
  return ret;
}

int ObTransformUtils::update_table_id_index(const ObRelIds &old_ids,
                                            const int32_t old_bit_id,
                                            const int32_t new_bit_id,
                                            ObRelIds &new_ids)
{
  int ret = OB_SUCCESS;
  if (old_ids.has_member(old_bit_id)) {
    if (OB_FAIL(new_ids.add_member(new_bit_id))) {
      LOG_WARN("failed to add new id", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

//TODO 更多表达式后续可放开
bool ObTransformUtils::is_valid_type(ObItemType expr_type)
{
  return T_OP_EQ == expr_type
         || T_OP_LE == expr_type
         || T_OP_LT == expr_type
         || T_OP_GE == expr_type
         || T_OP_GT == expr_type
         || T_OP_NE == expr_type;
}

int ObTransformUtils::is_expr_query(const ObSelectStmt *stmt,
                                    bool &is_expr_type)
{
  int ret = OB_SUCCESS;
  is_expr_type = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (0 == stmt->get_from_item_size()
             && 1 == stmt->get_select_item_size()
             && !stmt->is_contains_assignment()
             && !stmt->has_subquery()
             && 0 == stmt->get_aggr_item_size()
             && 0 == stmt->get_window_func_count()
             && 0 == stmt->get_condition_size()
             && 0 == stmt->get_having_expr_size()
             && !stmt->has_limit()
             && !stmt->is_hierarchical_query()
             && !stmt->is_set_stmt()) {
    is_expr_type = true;
  }
  return ret;
}

int ObTransformUtils::is_aggr_query(const ObSelectStmt *stmt,
                                    bool &is_aggr_type)
{
  int ret = OB_SUCCESS;
  is_aggr_type = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_set_stmt()
             && 0 <  stmt->get_aggr_item_size()
             && 1 == stmt->get_select_item_size()
             && 0 == stmt->get_group_expr_size()
             && 0 == stmt->get_rollup_expr_size()
             && 0 == stmt->get_having_expr_size()
             && !stmt->has_limit()) {
    is_aggr_type = true;
  }
  return ret;
}

int ObTransformUtils::add_is_not_null(ObTransformerCtx *ctx,
                                      const ObDMLStmt *stmt,
                                      ObRawExpr *child_expr,
                                      ObOpRawExpr *&is_not_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  is_not_expr = NULL;
  ObRawExpr* tmp_is_not_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret), K(stmt), K(ctx));
  } else if (OB_ISNULL(ctx->session_info_)
             || OB_ISNULL(expr_factory = ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx fields have null", K(ret), K(ctx->session_info_), K(expr_factory));
  } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*expr_factory,
                                                            child_expr,
                                                            true /*is_not_null*/,
                                                            tmp_is_not_expr))) {
    LOG_WARN("failed to build is not null expr", K(ret));
  } else if (OB_FAIL(tmp_is_not_expr->formalize(ctx->session_info_))) {
    LOG_WARN("failed to formalize a new expr", K(ret));
  } else if (OB_FAIL(tmp_is_not_expr->pull_relation_id())) {
    LOG_WARN("pull expr relation ids failed", K(ret));
  } else {
    is_not_expr = static_cast<ObOpRawExpr *>(tmp_is_not_expr);
  }
  return ret;
}

int ObTransformUtils::is_column_nullable(const ObDMLStmt *stmt,
                                         ObSchemaChecker *schema_checker,
                                         const ObColumnRefRawExpr *col_expr,
                                         const ObSQLSessionInfo *session_info,
                                         bool &is_nullable)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = NULL;
  is_nullable = true;
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_checker) || OB_ISNULL(col_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(schema_checker), K(col_expr));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(
                         col_expr->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get table item", K(ret), K(col_expr->get_table_id()));
  } else if (table_item->is_basic_table()) {
    const ObColumnSchemaV2 *col_schema = NULL;
    if (OB_FAIL(schema_checker->get_column_schema(session_info->get_effective_tenant_id(),
                                                  table_item->ref_id_,
                                                  col_expr->get_column_id(),
                                                  col_schema,
                                                  true))) {
      LOG_WARN("failed to get_column_schema", K(ret));
    } else {
      is_nullable = ! col_schema->is_not_null_for_read();
    }
  }
  return ret;
}

int ObTransformUtils::flatten_joined_table(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<JoinedTable*, 4> tmp_joined_tables;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else if (OB_FAIL(tmp_joined_tables.assign(stmt->get_joined_tables()))) {
    LOG_WARN("failed to assign joined tables", K(ret));
  } else {
    JoinedTable *table = NULL;
    TableItem *left_table = NULL;
    TableItem *right_table = NULL;
    const int64_t origin_joined_table_count = tmp_joined_tables.count();
    ObIArray<JoinedTable*> &joined_tables = stmt->get_joined_tables();
    ObSEArray<FromItem, 4> removed_from_items;
    FromItem item;
    item.is_joined_ = true;
    joined_tables.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_joined_tables.count(); ++i) {
      if (OB_ISNULL(table = tmp_joined_tables.at(i)) || OB_ISNULL(left_table = table->left_table_)
          || OB_ISNULL(right_table = table->right_table_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(table), K(left_table), K(right_table));
      } else if (!table->is_inner_join()) {
        if (OB_FAIL(joined_tables.push_back(table))) {
          LOG_WARN("failed to push back joined table", K(ret));
        } else if (i < origin_joined_table_count) {
          /* do nohing */
        } else if (stmt->add_from_item(table->table_id_, true)) {
          LOG_WARN("failed to add from item", K(ret), K(right_table));
        }
      } else if (OB_FAIL(append(stmt->get_condition_exprs(), table->get_join_conditions()))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FALSE_IT(item.table_id_ = table->table_id_)) {
      } else if (OB_FAIL(removed_from_items.push_back(item))) {
        LOG_WARN("failed to push back from item", K(ret));
      } else {
        if (left_table->is_joined_table()) {
          ret = tmp_joined_tables.push_back(static_cast<JoinedTable*>(left_table));
        } else if (stmt->add_from_item(left_table->table_id_, false)) {
          LOG_WARN("failed to add from item", K(ret), K(left_table));
        }
        if (OB_FAIL(ret)) {
        } else if (right_table->is_joined_table()) {
          ret = tmp_joined_tables.push_back(static_cast<JoinedTable*>(right_table));
        } else if (stmt->add_from_item(right_table->table_id_, false)) {
          LOG_WARN("failed to add from item", K(ret), K(right_table));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_from_items(),
                                                             removed_from_items))) {
      LOG_WARN("failed to remove from items", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::flatten_expr(ObRawExpr *expr,
                                   common::ObIArray<ObRawExpr*> &flattened_exprs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (T_OP_AND == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(flatten_expr(expr->get_param_expr(i),
                                                 flattened_exprs)))) {
        LOG_WARN("failed to flatten expr", K(ret));
      } else { /*do nothing*/ }
    }
  } else {
    if (OB_FAIL(flattened_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformUtils::find_not_null_expr(const ObDMLStmt &stmt,
                                         ObRawExpr *&not_null_expr,
                                         bool &is_valid,
                                         ObTransformerCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> from_tables;
  if (OB_FAIL(stmt.get_from_tables(from_tables))) {
    LOG_WARN("failed to get from tables", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_column_size(); ++i) {
    ObColumnRefRawExpr *col = NULL;
    bool is_not_null = true;
    if (OB_ISNULL(col = stmt.get_column_item(i)->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (!from_tables.is_superset(col->get_relation_ids())) {
      // do nothing
    } else if (OB_FAIL(is_expr_not_null(ctx, &stmt, col, NULLABLE_SCOPE::NS_WHERE, is_not_null))) {
      LOG_WARN("failed to check expr nullable", K(ret));
    } else if (is_not_null) {
      not_null_expr = col;
      break;
    }
  }
  if (OB_SUCC(ret)) {
    is_valid = (not_null_expr != NULL);
  }
  return ret;
}

int ObNotNullContext::generate_stmt_context(int64_t stmt_context)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = stmt_;
  // find null filter
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); ++i) {
    JoinedTable *table = NULL;
    if (!stmt->get_from_item(i).is_joined_) {
      // do nothing
    } else if (OB_ISNULL(table = stmt->get_joined_table(stmt->get_from_item(i).table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null", K(ret));
    } else if (OB_FAIL(add_joined_table(table))) {
      LOG_WARN("failed to add joined table", K(ret));
    }
  }
  if (stmt_context == NULLABLE_SCOPE::NS_FROM) {
    filters_.reset();
  }
  if (OB_SUCC(ret) && stmt_context >= NULLABLE_SCOPE::NS_WHERE) {
    if (OB_FAIL(append(filters_, stmt->get_condition_exprs()))) {
      LOG_WARN("failed to append condition exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_info_size(); ++i) {
      SemiInfo *semi = NULL;
      if (OB_ISNULL(semi = stmt->get_semi_infos().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("semi info is null", K(ret));
      } else if (semi->is_semi_join() && 
                 OB_FAIL(append(filters_, semi->semi_conditions_))) {
        LOG_WARN("failed to append filters", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && stmt->is_select_stmt() && 
      static_cast<const ObSelectStmt *>(stmt)->has_group_by() &&
      stmt_context >= NULLABLE_SCOPE::NS_GROUPBY) {
    const ObSelectStmt *sel_stmt = static_cast<const ObSelectStmt *>(stmt);
    const ObIArray<ObRollupItem> &rollups = sel_stmt->get_rollup_items();
    const ObIArray<ObCubeItem> &cubes = sel_stmt->get_cube_items();
    const ObIArray<ObGroupingSetsItem> &groupingsets = sel_stmt->get_grouping_sets_items();
    ObSEArray<ObRawExpr*, 4> not_null_reserve_exprs;
    ObSEArray<ObRawExpr*, 4> tmp_group_clause_exprs;
    if (sel_stmt->is_scala_group_by()) {
      // non-standard scalar group by,
      // select c1, count(*) from t;
      if (OB_FAIL(sel_stmt->get_column_exprs(group_clause_exprs_))) {
        LOG_WARN("failed to get column exprs", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_aggr_item_size(); ++i) {
        if (OB_ISNULL(sel_stmt->get_aggr_item(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("aggr item is null", K(ret));
        } else if (sel_stmt->get_aggr_item(i)->get_expr_type() == T_FUN_COUNT) {
          // do nothing
        } else if (OB_FAIL(group_clause_exprs_.push_back(sel_stmt->get_aggr_item(i)))) {
          LOG_WARN("failed to push back aggr item", K(ret));
        }
      }
    } else {
      if (OB_FAIL(append(not_null_reserve_exprs, sel_stmt->get_group_exprs()))) {
        LOG_WARN("failed to append groupby exprs", K(ret));
      } else if (OB_FAIL(append(tmp_group_clause_exprs, sel_stmt->get_rollup_exprs()))) {
        LOG_WARN("failed to append rollup exprs", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < groupingsets.count(); ++i) {
        const ObIArray<ObGroupbyExpr> &grouping = groupingsets.at(i).grouping_sets_exprs_;
        const ObIArray<ObRollupItem> &rollup_items = groupingsets.at(i).rollup_items_;
        const ObIArray<ObCubeItem> &cube_items = groupingsets.at(i).cube_items_;
        if (grouping.count() == 1 && rollup_items.count() == 0 && cube_items.count() == 0) {
          if (OB_FAIL(append(not_null_reserve_exprs, grouping.at(0).groupby_exprs_))) {
            LOG_WARN("failed to append into null_except", K(ret));
          }
        } else if (grouping.count() > 1 && rollup_items.count() == 0 && cube_items.count() == 0) {
          ObSEArray<ObRawExpr*, 4> intersection_exprs;
          if (OB_FAIL(append(intersection_exprs, grouping.at(0).groupby_exprs_))) {
            LOG_WARN("failed to append array", K(ret));
          }
          for (int64_t j = 1; OB_SUCC(ret) && intersection_exprs.count() != 0 && j < grouping.count(); ++j) {
            if (OB_FAIL(ObOptimizerUtil::intersect(intersection_exprs, grouping.at(j).groupby_exprs_, intersection_exprs))) {
              LOG_WARN("failed to intersect", K(ret));
            }
          }
          if (OB_SUCC(ret) && intersection_exprs.count() != 0) {
            if (OB_FAIL(append(not_null_reserve_exprs, intersection_exprs))) {
              LOG_WARN("failed to append groupby exprs", K(ret));
            }
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < grouping.count(); ++j) {
            if (OB_FAIL(append(tmp_group_clause_exprs, grouping.at(j).groupby_exprs_))) {
              LOG_WARN("failed to append pad null exprs", K(ret));
            }
          }
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < grouping.count(); ++j) {
            if (OB_FAIL(append(tmp_group_clause_exprs, grouping.at(j).groupby_exprs_))) {
              LOG_WARN("failed to append pad null exprs", K(ret));
            }
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < rollup_items.count(); ++j) {
            for (int64_t k = 0; OB_SUCC(ret) && k < rollup_items.at(j).rollup_list_exprs_.count(); ++k) {
              if (OB_FAIL(append(tmp_group_clause_exprs,
                                 rollup_items.at(j).rollup_list_exprs_.at(k).groupby_exprs_))) {
                LOG_WARN("failed to append pad null exprs", K(ret));
              }
            }
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < cube_items.count(); ++j) {
            for (int64_t k = 0; OB_SUCC(ret) && k < cube_items.at(j).cube_list_exprs_.count(); ++k) {
              if (OB_FAIL(append(tmp_group_clause_exprs,
                                 cube_items.at(j).cube_list_exprs_.at(k).groupby_exprs_))) {
                LOG_WARN("failed to append pad null exprs", K(ret));
              }
            }
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < rollups.count(); ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < rollups.at(i).rollup_list_exprs_.count(); ++j) {
          if (OB_FAIL(append(tmp_group_clause_exprs,
                             rollups.at(i).rollup_list_exprs_.at(j).groupby_exprs_))) {
            LOG_WARN("failed to append rollup item exprs", K(ret));
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < cubes.count(); ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < cubes.at(i).cube_list_exprs_.count(); ++j) {
          if (OB_FAIL(append(tmp_group_clause_exprs,
                             cubes.at(i).cube_list_exprs_.at(j).groupby_exprs_))) {
            LOG_WARN("failed to append cube item exprs", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_group_clause_exprs.count(); ++i) {
          ObRawExpr *expr = tmp_group_clause_exprs.at(i);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected NULL ptr", K(ret));
          } else if (ObOptimizerUtil::find_item(not_null_reserve_exprs, expr)) {
          } else if (OB_FAIL(group_clause_exprs_.push_back(expr))) {
            LOG_WARN("failed to push back groupby expr", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && stmt_context >= NULLABLE_SCOPE::NS_TOP &&
      stmt->is_select_stmt()) {
    if (OB_FAIL(having_filters_.assign(
                  static_cast<const ObSelectStmt *>(stmt)->get_having_exprs()))) {
      LOG_WARN("failed to assign having exprs", K(ret));
    }
  }
  return ret;
}

int ObNotNullContext::add_joined_table(const JoinedTable *table) 
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret), K(table));
  } else if (OB_FAIL(ObTransformUtils::get_outer_join_right_tables(
                       *table, right_table_ids_))) {
    LOG_WARN("failed to get outer join right table", K(ret));
  } else if (OB_FAIL(ObDMLStmt::extract_equal_condition_from_joined_table(
                       table, filters_, true))) {
    LOG_WARN("failed to extract strict condition", K(ret));
  }
  return ret;
}

int ObNotNullContext::add_filter(const ObIArray<ObRawExpr *> &filters)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(filters_, filters))) {
    LOG_WARN("failed to append filters", K(ret));
  }
  return ret;
}

int ObNotNullContext::add_filter(ObRawExpr *filter)
{
  return filters_.push_back(filter);
}

int ObNotNullContext::remove_filter(ObRawExpr *filter)
{
  return ObOptimizerUtil::remove_item(filters_, filter);
}

int ObNotNullContext::add_having_filter(const ObIArray<ObRawExpr *> &filters)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(having_filters_, filters))) {
    LOG_WARN("failed to append filters", K(ret));
  }
  return ret;
}

int ObNotNullContext::add_having_filter(ObRawExpr *filter)
{
  return having_filters_.push_back(filter);
}

int ObNotNullContext::remove_having_filter(ObRawExpr *filter)
{
  return ObOptimizerUtil::remove_item(having_filters_, filter);
}

int ObTransformUtils::get_outer_join_right_tables(const JoinedTable &joined_table,
                                                  ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  TableItem *left = NULL;
  TableItem *right = NULL;
  if (OB_ISNULL(left = joined_table.left_table_) || 
      OB_ISNULL(right = joined_table.right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param table is null", K(ret));
  } else if (joined_table.is_inner_join()) {
    if (left->is_joined_table() &&
        OB_FAIL(SMART_CALL(get_outer_join_right_tables(static_cast<JoinedTable&>(*left), 
                                                       table_ids)))) {
      LOG_WARN("failed to visit left table", K(ret));
    } else if (right->is_joined_table() &&
               OB_FAIL(SMART_CALL(get_outer_join_right_tables(static_cast<JoinedTable&>(*right), 
                                                              table_ids)))) {
      LOG_WARN("failed to visit right table", K(ret));
    }
  } else if (joined_table.is_full_join()){
    if (left->is_joined_table()) {
      if (OB_FAIL(append(table_ids, static_cast<JoinedTable *>(left)->single_table_ids_))) {
        LOG_WARN("failed to append tables ids", K(ret));
      }
    } else if (OB_FAIL(table_ids.push_back(left->table_id_))) {
      LOG_WARN("failed to push back left table id", K(ret));
    }
    if (OB_SUCC(ret) && right->is_joined_table()) {
      if (OB_FAIL(append(table_ids, static_cast<JoinedTable *>(right)->single_table_ids_))) {
        LOG_WARN("failed to append tables ids", K(ret));
      }
    } else if (OB_FAIL(table_ids.push_back(right->table_id_))) {
      LOG_WARN("failed to push back right table id", K(ret));
    }
  } else if (joined_table.is_left_join()) {
    if (left->is_joined_table() &&
        OB_FAIL(SMART_CALL(get_outer_join_right_tables(static_cast<JoinedTable&>(*left),
                                                       table_ids)))){
        LOG_WARN("failed to visit left table", K(ret));
    } else if (right->is_joined_table()) {
      if (OB_FAIL(append(table_ids, static_cast<JoinedTable *>(right)->single_table_ids_))) {
        LOG_WARN("failed to append tables ids", K(ret));
      }
    } else if (OB_FAIL(table_ids.push_back(right->table_id_))) {
      LOG_WARN("failed to push back right table id", K(ret));
    }
  } else if (joined_table.is_right_join() || CONNECT_BY_JOIN == joined_table.joined_type_) {
    if (right->is_joined_table() &&
        OB_FAIL(SMART_CALL(get_outer_join_right_tables(static_cast<JoinedTable&>(*right),
                                                       table_ids)))){
        LOG_WARN("failed to visit right table", K(ret));
    } else if (left->is_joined_table()) {
      if (OB_FAIL(append(table_ids, static_cast<JoinedTable *>(left)->single_table_ids_))) {
        LOG_WARN("failed to append tables ids", K(ret));
      }
    } else if (OB_FAIL(table_ids.push_back(left->table_id_))) {
      LOG_WARN("failed to push back left table id", K(ret));
    }   
  }

  return ret;
}

int ObTransformUtils::is_expr_not_null(ObNotNullContext &ctx,
                                       const ObRawExpr *expr, 
                                       bool &is_not_null, 
                                       ObIArray<ObRawExpr *> *constraints)
{
  int ret = OB_SUCCESS;
  bool in_group_clause = false;
  int64_t constraint_size = constraints != NULL ? constraints->count() : -1;
  is_not_null = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (ObOptimizerUtil::find_item(ctx.group_clause_exprs_, expr)) {
    in_group_clause = true;
  } else if (expr->is_column_ref_expr()) {
    if (OB_FAIL(is_column_expr_not_null(ctx,
                                        static_cast<const ObColumnRefRawExpr *>(expr),
                                        is_not_null,
                                        constraints))) {
      LOG_WARN("failed to check expr not null", K(ret), K(*expr));
    }
  } else if (expr->is_set_op_expr()) {
    if (OB_FAIL(is_set_expr_not_null(ctx,
                                     static_cast<const ObSetOpRawExpr *>(expr),
                                     is_not_null,
                                     constraints))) {
      LOG_WARN("failed to check expr not null", K(ret));
    }
  } else if (expr->is_exec_param_expr()) {
    // do nothing
  } else if (T_FUN_COUNT == expr->get_expr_type() ||
             T_FUN_SYS_LNNVL == expr->get_expr_type() ||
             T_FUN_SYS_ROWNUM == expr->get_expr_type() ||
             T_LEVEL == expr->get_expr_type()) {
    is_not_null = true;
  } else if (!expr->is_const_raw_expr() &&
             OB_FAIL(is_general_expr_not_null(ctx, expr, is_not_null, constraints))) {
    LOG_WARN("failed to check compound expr", K(ret));
  } else if (is_not_null) {
    // do nothing
  } else if (expr->is_static_scalar_const_expr()) {
    bool is_null = false;
    if (OB_FAIL(is_const_expr_not_null(ctx, expr, is_not_null, is_null))) {
      LOG_WARN("failed to check calculable expr not null", K(ret));
    } else if (is_not_null && !expr->is_const_raw_expr() && 
               NULL != constraints &&
               OB_FAIL(constraints->push_back(const_cast<ObRawExpr*>(expr)))) {
      LOG_WARN("failed to push back constraint expr", K(ret));
    }
  } else if(is_mysql_mode() &&
            expr->is_win_func_expr() &&
            expr->get_result_type().is_not_null_for_read()) {
    const ObWinFunRawExpr *win_expr = reinterpret_cast<const ObWinFunRawExpr*>(expr);
    if (T_WIN_FUN_RANK == win_expr->get_func_type() ||
      T_WIN_FUN_DENSE_RANK == win_expr->get_func_type() ||
      T_WIN_FUN_CUME_DIST == win_expr->get_func_type() ||
      T_WIN_FUN_PERCENT_RANK == win_expr->get_func_type() ||
      T_WIN_FUN_ROW_NUMBER == win_expr->get_func_type()) {
      is_not_null = true;
    } else{}
  } else {}

  if (OB_SUCC(ret) && !expr->is_const_expr() && !is_not_null) {
    bool has_null_reject = false;
    if (!ctx.having_filters_.empty() && 
        OB_FAIL(ObTransformUtils::has_null_reject_condition(
                  ctx.having_filters_, expr, has_null_reject))) {
      LOG_WARN("failed to check has null reject condition", K(ret));
    } else if (has_null_reject) {
      is_not_null = true;
    } else if (in_group_clause) {
      // do noting, can not use basic filters
    } else if (!ctx.filters_.empty() && 
               OB_FAIL(ObTransformUtils::has_null_reject_condition(
                         ctx.filters_, expr, has_null_reject))) {
      LOG_WARN("failed to check has null reject condition", K(ret));
    } else if (has_null_reject) {
      is_not_null = true;
    }
    // Since the expr has a null-reject condition,
    // Its not-null property does not rely on any constraints
    if (OB_SUCC(ret) && is_not_null && NULL != constraints) {
      for (int64_t i = constraints->count() - 1; OB_SUCC(ret) && i >= constraint_size; --i) {
        if (OB_FAIL(constraints->remove(i))) {
          LOG_WARN("failed to remove constraints", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::is_const_expr_not_null(ObNotNullContext &ctx,
                                             const ObRawExpr *expr,
                                             bool &is_not_null,
                                             bool &is_null)
{
  int ret = OB_SUCCESS;
  ObObj result;
  bool got_result = false;
  is_not_null = false;
  is_null = false;
  if (OB_ISNULL(expr) || !expr->is_static_scalar_const_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->is_const_raw_expr() && 
             expr->get_expr_type() != T_QUESTIONMARK) {
    const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr *>(expr);
    result = const_expr->get_value();
    got_result = true;
  } else if (expr->get_expr_type() == T_QUESTIONMARK
            && expr->get_result_meta().is_urowid()) {
    /* rowid in pl might incorrectly return a valid value
       instead of an invalid value*/
  } else if (NULL == ctx.exec_ctx_ || NULL == ctx.allocator_) {
    got_result = false;
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(
                       ctx.exec_ctx_,
                       expr,
                       result,
                       got_result,
                       *ctx.allocator_))) {
    LOG_WARN("failed to calc const or calculable expr", K(ret));
  } 
  
  if (OB_SUCC(ret) && got_result) {
    if (result.is_null() || (lib::is_oracle_mode() && result.is_null_oracle())) {
      is_not_null = false;
      is_null = true;
    } else {
      is_not_null = true;
      is_null = false;
    }
  }
  return ret;
}

int ObTransformUtils::is_column_expr_not_null(ObNotNullContext &ctx,
                                              const ObColumnRefRawExpr *expr,
                                              bool &is_not_null,
                                              ObIArray<ObRawExpr *> *constraints)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  const TableItem *table = NULL;
  is_not_null = false;
  if (OB_ISNULL(stmt = ctx.stmt_) || OB_ISNULL(expr) ||
      OB_ISNULL(table = stmt->get_table_item_by_id(expr->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(expr->get_table_id()), K(*stmt));
  } else if (ObOptimizerUtil::find_item(ctx.right_table_ids_, table->table_id_)) {
    // do nothing
  } else if (table->is_basic_table()) {
    if (ctx.is_for_ctas_) {
      is_not_null = expr->get_result_type().has_result_flag(HAS_NOT_NULL_VALIDATE_CONSTRAINT_FLAG);
    } else {
      is_not_null = expr->get_result_type().has_result_flag(NOT_NULL_FLAG);
    }
  } else if (table->is_generated_table() || table->is_temp_table()) {
    int64_t idx = expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
    ObRawExpr *child_expr = NULL;
    ObSelectStmt *child_stmt = table->ref_query_;
    ObNotNullContext child_ctx(ctx, child_stmt);
    if (OB_ISNULL(child_stmt) ||
        OB_UNLIKELY(idx < 0 || idx >= child_stmt->get_select_item_size()) ||
        OB_ISNULL(child_expr = child_stmt->get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is invalid", K(ret), K(idx));
    } else if (OB_FAIL(child_ctx.generate_stmt_context(NULLABLE_SCOPE::NS_TOP))) {
      LOG_WARN("failed to generate stmt context", K(ret));
    } else if (OB_FAIL(is_expr_not_null(child_ctx,
                                        child_expr,
                                        is_not_null,
                                        constraints))) {
      LOG_WARN("failed to check expr not null", K(ret));
    }
  } else if (table->is_values_table()) {
    int64_t idx = expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
    int64_t column_cnt = ctx.stmt_->get_column_size(table->table_id_);
    if (OB_UNLIKELY(idx >= column_cnt || column_cnt == 0 || table->table_values_.empty() ||
                    table->table_values_.count() % column_cnt != 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(idx), KPC(table), K(column_cnt));
    } else {
      is_not_null = true;
      int64_t row_count = table->table_values_.count() / column_cnt;
      for (int64_t i = 0; OB_SUCC(ret) && is_not_null && i < row_count; ++i) {
        if (OB_UNLIKELY(column_cnt * i + idx >= table->table_values_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(i), K(idx), KPC(table), K(column_cnt));
        } else if (OB_FAIL(is_expr_not_null(ctx,
                                            table->table_values_.at(column_cnt * i + idx),
                                            is_not_null,
                                            constraints))) {
          LOG_WARN("failed to check expr not null", K(ret));
        } else {/*do nothing*/}
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObTransformUtils::is_set_expr_not_null(ObNotNullContext &ctx,
                                           const ObSetOpRawExpr *expr,
                                           bool &is_not_null,
                                           ObIArray<ObRawExpr *> *constraints)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(ctx.stmt_);
  is_not_null = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_set_query().count(); ++i) {
    const ObSelectStmt *child_stmt = select_stmt->get_set_query().at(i);
    ObNotNullContext child_ctx(ctx, child_stmt);
    int64_t idx = expr->get_idx();
    ObRawExpr *child_expr = NULL;
    if (OB_ISNULL(child_stmt) ||
        OB_UNLIKELY(idx < 0 || idx >= child_stmt->get_select_item_size()) ||
        OB_ISNULL(child_expr = child_stmt->get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid params", K(ret));
    } else if (OB_FAIL(child_ctx.generate_stmt_context(NULLABLE_SCOPE::NS_TOP))) {
      LOG_WARN("failed to generate stmt context", K(ret));
    } else if (OB_FAIL(SMART_CALL(is_expr_not_null(child_ctx,
                                                   child_expr,
                                                   is_not_null,
                                                   constraints)))) {
      LOG_WARN("failed to check expr not null", K(ret), K(*child_expr));
    } else if (i == 0 && T_OP_EXCEPT == expr->get_expr_type()) {
      break;
    } else if (is_not_null && T_OP_INTERSECT == expr->get_expr_type()) {
      break;
    } else if (!is_not_null && T_OP_UNION == expr->get_expr_type()) {
      break;
    }
  }
  return ret;
}

int ObTransformUtils::is_general_expr_not_null(ObNotNullContext &ctx,
                                               const ObRawExpr *expr, 
                                               bool &is_not_null,
                                               ObIArray<ObRawExpr *> *constraints)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRawExpr *, 2> check_list;
  ObIArray<ObRawExpr *> *param_constraint = constraints;
  is_not_null = false;
  if (expr->has_flag(IS_CONST_EXPR)) {
    param_constraint = NULL;
  }
  if (T_OP_AND == expr->get_expr_type() ||
      T_OP_OR == expr->get_expr_type() ||
      T_OP_BTW == expr->get_expr_type() ||
      T_OP_NOT_BTW == expr->get_expr_type() ||
      T_FUN_WM_CONCAT == expr->get_expr_type() ||
      is_not_null_deduce_type(expr->get_expr_type())) {
    // any null, return null
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(check_list.push_back(expr->get_param_expr(i)))) {
        LOG_WARN("failed to append check list", K(ret));
      }
    }
  } else if (T_OP_LIKE == expr->get_expr_type() ||
             T_OP_NOT_LIKE == expr->get_expr_type()) {
    // c1 like 'xxx' escape NULL 不是 null propagate. escape NULL = escape '\\'
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count() - 1; ++i) {
      if (OB_FAIL(check_list.push_back(expr->get_param_expr(i)))) {
        LOG_WARN("failed to append check list", K(ret));
      }
    }
  } else if (T_FUN_SYS_CAST == expr->get_expr_type()) {
    if (OB_FAIL(check_list.push_back(expr->get_param_expr(0)))) {
      LOG_WARN("failed to append check list", K(ret));
    }
  } else if (T_FUN_SYS_NVL == expr->get_expr_type()) {
    if (OB_FAIL(check_list.push_back(expr->get_param_expr(1)))) {
      LOG_WARN("failed to append check list", K(ret));
    }
  } else if (T_OP_CASE == expr->get_expr_type() ||
             T_OP_ARG_CASE == expr->get_expr_type()) {
    const ObCaseOpRawExpr *case_expr = static_cast<const ObCaseOpRawExpr*>(expr);
    if (OB_UNLIKELY(case_expr->get_then_expr_size() !=
                    case_expr->get_when_expr_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("then expr size does not match when expr", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < case_expr->get_when_expr_size(); ++i) {
      if (OB_FAIL(ctx.filters_.push_back(case_expr->get_when_param_exprs().at(i)))) {
        LOG_WARN("failed to add filter", K(ret));
      } else if (OB_FAIL(SMART_CALL(is_expr_not_null(ctx, 
                                                     case_expr->get_then_param_exprs().at(i),
                                                     is_not_null, 
                                                     param_constraint)))) {
        LOG_WARN("failed to check then expr not null", K(ret));
      } else if (OB_FAIL(ctx.remove_filter(case_expr->get_when_param_exprs().at(i)))) {
        LOG_WARN("failed to remove filter", K(ret));
      } else if (!is_not_null) {
        break;
      }
    }
    if (OB_SUCC(ret) && is_not_null) {
      if (OB_FAIL(check_list.push_back(case_expr->get_default_param_expr()))) {
        LOG_WARN("failed to pushback default expr", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < check_list.count(); ++i) {
    if (OB_FAIL(SMART_CALL(is_expr_not_null(ctx,
                                            check_list.at(i), 
                                            is_not_null, 
                                            param_constraint)))) {
      LOG_WARN("failed to check compound expr", K(ret));
    } else if (!is_not_null) {
      break;
    }
  }
  return ret;
}

int ObTransformUtils::is_expr_not_null(ObTransformerCtx *ctx,
                                       const ObDMLStmt *stmt,
                                       const ObRawExpr *expr,
                                       int context_scope,
                                       bool &is_not_null,
                                       ObIArray<ObRawExpr *> *constraints)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is null", K(ret));
  } else {
    ObNotNullContext not_null_ctx(*ctx, stmt);
    if (OB_FAIL(not_null_ctx.generate_stmt_context(context_scope))) {
      LOG_WARN("failed to generate where context", K(ret));
    } else if (OB_FAIL(is_expr_not_null(not_null_ctx, expr, is_not_null, constraints))) {
      LOG_WARN("failed to check expr not null", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::has_null_reject_condition(const ObIArray<ObRawExpr *> &conditions,
                                                const ObRawExpr *expr,
                                                bool &has_null_reject)
{
  int ret = OB_SUCCESS;
  has_null_reject = false;
  bool is_null_reject = false;
  ObSEArray<const ObRawExpr *, 1> dummy_expr_lists;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(expr));
  } else if (OB_FAIL(dummy_expr_lists.push_back(expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_null_reject && i < conditions.count(); ++i) {
    if (OB_FAIL(is_null_reject_condition(conditions.at(i),
                                         dummy_expr_lists,
                                         is_null_reject))) {
      LOG_WARN("failed to check is null reject condition", K(ret));
    } else if (is_null_reject) {
      has_null_reject = true;
    }
  }
  return ret;
}

int ObTransformUtils::has_null_reject_condition(const ObIArray<ObRawExpr *> &conditions,
                                                const ObIArray<ObRawExpr *> &targets,
                                                bool &has_null_reject)
{
  int ret = OB_SUCCESS;
  has_null_reject = false;
  bool is_null_reject = false;
  ObSEArray<const ObRawExpr *, 1> dummy_expr_lists;
  for (int64_t i = 0; OB_SUCC(ret) && i < targets.count(); ++i) {
    if (OB_FAIL(dummy_expr_lists.push_back(targets.at(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_null_reject && i < conditions.count(); ++i) {
    if (OB_FAIL(is_null_reject_condition(conditions.at(i),
                                         dummy_expr_lists,
                                         is_null_reject))) {
      LOG_WARN("failed to check is null reject condition", K(ret));
    } else if (is_null_reject) {
      has_null_reject = true;
    }
  }
  return ret;
}

/**
 * 检查conditions是否拒绝指定表上的空值
 * 如果conditions不含有指定表的列，认为是不拒绝
 */
int ObTransformUtils::is_null_reject_conditions(const ObIArray<ObRawExpr *> &conditions,
                                                const ObRelIds &target_table,
                                                bool &is_null_reject)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> column_exprs;
  is_null_reject = false;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(conditions, column_exprs))) {
    LOG_WARN("failed to extrace column exprs", K(ret));
  }
  int64_t N = column_exprs.count();
  for (int64_t i = 0; OB_SUCC(ret) && !is_null_reject && i < N; ++i) {
    ObRawExpr *expr = column_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!expr->get_relation_ids().is_subset(target_table)) {
      //do nothing
    } else if (OB_FAIL(has_null_reject_condition(conditions,
                                                 expr,
                                                 is_null_reject))) {
      LOG_WARN("failed to check null reject", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformUtils::is_null_reject_condition
 * 1. condition 是 and/or 嵌套。
 *    要求and有一个子条件是 null_reject；or所有的子条件都是 null_reject
 * 2. condition 返回 false/NULL
 * 3. condition 返回 NULL
 *    要求满足 targets 均为 NULL 时，condition 的计算结果也是 null
 */
int ObTransformUtils::is_null_reject_condition(const ObRawExpr *condition,
                                               const ObIArray<const ObRawExpr *> &targets,
                                               bool &is_null_reject)
{
  int ret = OB_SUCCESS;
  is_null_reject = false;
  if (OB_ISNULL(condition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(condition));
  } else if (T_OP_AND == condition->get_expr_type()) {
    // and 要求一个子条件是 null reject
    for (int64_t i = 0; OB_SUCC(ret) && !is_null_reject
         && i < condition->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_null_reject_condition(condition->get_param_expr(i),
                                                      targets,
                                                      is_null_reject)))) {
        LOG_WARN("failed to check whether param is null reject", K(ret));
      }
    }
  } else if (T_OP_OR == condition->get_expr_type()) {
    // or  要求所有的子条件是 null reject
    is_null_reject = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_null_reject
         && i < condition->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_null_reject_condition(condition->get_param_expr(i),
                                                      targets,
                                                      is_null_reject)))) {
        LOG_WARN("failed to check whether param is null reject", K(ret));
      }
    }
  } else if (OB_FAIL(SMART_CALL(is_simple_null_reject(condition, targets, is_null_reject)))) {
    LOG_WARN("failed to check is simple null reject", K(ret));
  }
  if (OB_SUCC(ret) && is_null_reject) {
    LOG_TRACE("find null reject condition", K(*condition), K(lbt()));
  }
  return ret;
}

int ObTransformUtils::is_simple_null_reject(const ObRawExpr *condition,
                                            const ObIArray<const ObRawExpr *> &targets,
                                            bool &is_null_reject)
{
  int ret = OB_SUCCESS;
  is_null_reject = false;
  const ObRawExpr *expr = NULL;
  bool check_expr_is_null_reject = false;
  bool check_expr_is_null_propagate = false;
  if (OB_ISNULL(condition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(condition));
  } else if (T_OP_IS_NOT == condition->get_expr_type()) {
    if (OB_ISNULL(condition->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition param is null", K(ret));
    } else if (T_NULL == condition->get_param_expr(1)->get_expr_type()) {
      // param is not NULL 要求 param 是 null propagate
      check_expr_is_null_propagate = true;
      expr = condition->get_param_expr(0);
    }
  } else if (T_OP_IS == condition->get_expr_type()) {
    if (OB_ISNULL(condition->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is not correct", K(ret));
    } else if (T_BOOL == condition->get_param_expr(1)->get_expr_type()) {
      const ObConstRawExpr *b_expr =
        static_cast<const ObConstRawExpr*>(condition->get_param_expr(1));
      if (b_expr->get_value().is_true()) {
        // param is TRUE 要求 param 是 null reject
        check_expr_is_null_reject = true;
      } else {
        // param is FALSE 要求 param 是 null propagate
        check_expr_is_null_propagate = true;
      }
      expr = condition->get_param_expr(0);
    }
  } else if (IS_SUBQUERY_COMPARISON_OP(condition->get_expr_type())) {
    if (condition->has_flag(IS_WITH_ANY)) {
      check_expr_is_null_propagate = true;
      expr = condition->get_param_expr(0);
    } else if (condition->has_flag(IS_WITH_ALL)) {
    } else {
      check_expr_is_null_propagate = true;
      expr = condition;
    }
  } else {
    check_expr_is_null_propagate = true;
    expr = condition;
  }
  if (OB_FAIL(ret)) {
  } else if (check_expr_is_null_propagate) {
    if (OB_FAIL(is_null_propagate_expr(expr, targets, is_null_reject))) {
      LOG_WARN("failed to check expr is null propagate", K(ret));
    }
  } else if (check_expr_is_null_reject) {
    if (OB_FAIL(SMART_CALL(is_null_reject_condition(expr, targets, is_null_reject)))) {
      LOG_WARN("failed to check expr is null reject", K(ret));
    }
    OPT_TRACE("check null reject", expr, is_null_reject);
  }
  return ret;
}

int ObTransformUtils::is_null_propagate_expr(const ObRawExpr *expr,
                                             const ObRawExpr *target,
                                             bool &bret)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRawExpr *, 4> tmp;
  if (OB_FAIL(tmp.push_back(target))) {
    LOG_WARN("fail to push back expr", K(ret));
  } else if (OB_FAIL(is_null_propagate_expr(expr, tmp, bret))) {
    LOG_WARN("fail to check is null propagate expr", K(ret));
  }
  return ret;
}

int ObTransformUtils::is_null_propagate_expr(const ObRawExpr *expr,
                                             const ObIArray<ObRawExpr *> &targets,
                                             bool &bret)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRawExpr *, 4> tmp;
  if (OB_FAIL(append(tmp, targets))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(is_null_propagate_expr(expr, tmp, bret))) {
    LOG_WARN("failed to check is null propagate expr", K(ret));
  }
  return ret;
}

int ObTransformUtils::is_null_propagate_expr(const ObRawExpr *expr,
                                             const ObIArray<const ObRawExpr *> &targets,
                                             bool &bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(expr));
  } else if (OB_FAIL(find_expr(targets, expr, bret))) {
    LOG_WARN("failed to find expr", K(ret));
  } else if (bret) {
    // expr exists in targets
  } else if (is_null_propagate_type(expr->get_expr_type()) && !expr->has_flag(IS_WITH_ANY) && !expr->has_flag(IS_WITH_ALL)) {
    for (int64_t i = 0; OB_SUCC(ret) && !bret && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_null_propagate_expr(expr->get_param_expr(i), targets, bret)))) {
        LOG_WARN("failed to check param can propagate null", K(ret));
      }
    }
  } else if (T_OP_LIKE == expr->get_expr_type() || T_OP_NOT_LIKE == expr->get_expr_type()) {
    // c1 like 'xxx' escape NULL 不是 null propagate. escape NULL = escape '\\'
    for (int64_t i = 0; OB_SUCC(ret) && !bret && i < expr->get_param_count() - 1; ++i) {
      if (OB_FAIL(SMART_CALL(is_null_propagate_expr(expr->get_param_expr(i), targets, bret)))) {
        LOG_WARN("failed to check param can propagate null", K(ret));
      }
    }
  } else if (T_OP_AND == expr->get_expr_type() || T_OP_OR == expr->get_expr_type()) {
    // 如果 target = null，所有的子条件输出都是 null，那么 expr 的输出也是 null
    bret = true;
    for (int64_t i = 0; OB_SUCC(ret) && bret && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_null_propagate_expr(expr->get_param_expr(i), targets, bret)))) {
        LOG_WARN("failed to check param can propagate null", K(ret));
      }
    }
  } else if (T_OP_BTW == expr->get_expr_type() || T_OP_NOT_BTW == expr->get_expr_type()) {
    // 如果是 target [not] between ? and ? 的形式，那么 target = null, expr = null
    if (OB_FAIL(SMART_CALL(is_null_propagate_expr(expr->get_param_expr(0), targets, bret)))) {
      LOG_WARN("failed to check param can propagate null", K(ret));
    }
  }
  if (OB_SUCC(ret) && bret) {
    LOG_TRACE("find null propagate expr", K(*expr));
  }
  return ret;
}

inline bool is_valid_arith_expr(const ObItemType type)
{
  return (T_OP_NEG <= type && type <= T_OP_MOD)
         || (T_OP_AGG_ADD <= type && type <= T_OP_AGG_DIV)
         || T_OP_POW == type || T_OP_SIGN == type || T_OP_XOR == type;
}

inline bool is_safe_arith_expr(const ObItemType type)
{
  return (T_OP_NEG <= type && type <= T_OP_MUL)
         || (T_OP_AGG_ADD <= type && type <= T_OP_AGG_MUL)
         || T_OP_POW == type || T_OP_SIGN == type || T_OP_XOR == type;
}

/**
 * @brief is_valid_bool_expr
 * NSEQ is null safe comparison
 * NULL IS [NOT] NULL/TRUE/FALSE => TRUE/FALSE,
 * NULL AND FALSE                => FALSE
 * NULL OR TRUE                  => TRUE
 * NULL >  all (empty set)       => TRUE
 * NULL >  any (empty set)       => FALSE
 */
inline bool is_valid_bool_expr(const ObItemType type)
{
  return ((T_OP_EQ <= type && type <= T_OP_NE)
          || (T_OP_REGEXP <= type && type <= T_OP_NOT_IN)
          || T_OP_BOOL == type
          || IS_SUBQUERY_COMPARISON_OP(type))
         && T_OP_NSEQ != type
         && T_OP_SQ_NSEQ != type
         && T_OP_AND != type
         && T_OP_OR != type;
}

inline bool is_valid_bit_expr(const ObItemType type)
{
  return T_OP_BIT_AND <= type && type <= T_OP_BIT_RIGHT_SHIFT;
}

/**
 * @brief is_valid_sys_func
 * more sys func can be included here
 */
inline bool is_valid_sys_func(const ObItemType type)
{
  bool ret = false;
  const static ObItemType WHITE_LIST[] = {
    T_FUN_SYS_SQRT,
    T_FUN_SYS_LOG_TEN,
    T_FUN_SYS_LOG_TWO,
    T_FUN_SYS_FLOOR,
    T_FUN_SYS_CEIL,
    T_FUN_SYS_LEAST,
    T_FUN_SYS_GREATEST,
    T_FUN_SYS_CAST,
  };
  for (int64_t i = 0; !ret && i < sizeof(WHITE_LIST) / sizeof(ObItemType); ++i) {
    ret = (type == WHITE_LIST[i]);
  }
  return ret;
}

inline bool is_valid_aggr_func(const ObItemType type)
{
  bool ret = false;
  const static ObItemType WHITE_LIST[] = {
    T_FUN_MIN,
    T_FUN_MAX,
    T_FUN_SUM
  };
  for (int64_t i = 0; !ret && i < sizeof(WHITE_LIST) / sizeof(ObItemType); ++i) {
    ret = (type == WHITE_LIST[i]);
  }
  return ret;
}

/**
 * @brief ObTransformUtils::is_null_propagate_type
 * 1. arithmetic operators or functions
 * 2. boolean operators, except for T_OP_NESQ, T_OP_IS, T_OP_IS_NOT,
 * T_OP_AND, T_OP_OR,  T_OP_NOT_IN and T_OP_SQ_*
 * 3. bit operator
 * 4. some sys function
 */
bool ObTransformUtils::is_null_propagate_type(const ObItemType type)
{
  return is_valid_arith_expr(type)
         || is_valid_bool_expr(type)
         || is_valid_bit_expr(type)
         || is_valid_sys_func(type)
         || is_valid_aggr_func(type);
}

bool ObTransformUtils::is_not_null_deduce_type(const ObItemType type)
{
  return is_safe_arith_expr(type)
         || is_valid_bool_expr(type)
         || is_valid_bit_expr(type)
         || is_valid_aggr_func(type);
}

int ObTransformUtils::find_expr(const ObIArray<const ObRawExpr *> &source,
                                const ObRawExpr *target,
                                bool &bret,
                                ObExprEqualCheckContext *check_context)
{
  int ret = OB_SUCCESS;
  bret = false;
  if (OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest expr is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !bret && i < source.count(); ++i) {
    if (OB_ISNULL(source.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr in source is null", K(ret));
    } else if (source.at(i) == target || source.at(i)->same_as(*target, check_context)) {
      bret = true;
    }
  }
  return ret;
}

int ObTransformUtils::find_expr(ObIArray<ObRawExpr *> &source,
                                ObRawExpr *target,
                                bool &bret,
                                ObExprEqualCheckContext *check_context)
{
  int ret = OB_SUCCESS;
  bret = false;
  if (OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest expr is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !bret && i < source.count(); ++i) {
    if (OB_ISNULL(source.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr in source is null", K(ret));
    } else if (source.at(i) == target || source.at(i)->same_as(*target, check_context)) {
      bret = true;
    }
  }
  return ret;
}

/**
 * column <=> ? 或 ？<=> column
 * column is ? 或 column != ?
 * column like ?
 * column not/between ? and ?
 * column in (?,?,?)
 * column in st_function
 * 获取存在于这一类谓词的column
 */
int ObTransformUtils::get_simple_filter_column(const ObDMLStmt *stmt,
                                               ObRawExpr *expr,
                                               int64_t table_id,
                                               ObIArray<ObColumnRefRawExpr*> &col_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  //now rowid expr isn't column expr, is func expr, and the rowid is like primary key, and the
  //calc_rowid_expr contain primary column, so we can extact primary column to use.
  } else if (expr->has_flag(IS_ROWID_SIMPLE_COND) || expr->has_flag(IS_ROWID_RANGE_COND)) {
    ObSEArray<ObRawExpr*, 4> column_exprs;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_UNLIKELY(column_exprs.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), KPC(expr));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
        if (OB_ISNULL(column_exprs.at(i)) ||
            OB_UNLIKELY(!column_exprs.at(i)->is_column_ref_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), KPC(column_exprs.at(i)));
        } else if (OB_FAIL(col_exprs.push_back(static_cast<ObColumnRefRawExpr*>(column_exprs.at(i))))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing*/}
      }
    }
  } else {
    switch(expr->get_expr_type())
    {
      case T_OP_EQ:
      case T_OP_NSEQ:
      case T_OP_LE:
      case T_OP_LT:
      case T_OP_GE:
      case T_OP_GT:
      case T_OP_NE:
      case T_FUN_SYS_ST_INTERSECTS:
      case T_FUN_SYS_ST_COVERS:
      case T_FUN_SYS_ST_DWITHIN:
      case T_FUN_SYS_ST_WITHIN:
      case T_FUN_SYS_ST_CONTAINS:
      {
        ObRawExpr *left = NULL;
        ObRawExpr *right = NULL;
        if (T_FUN_SYS_ST_DWITHIN == expr->get_expr_type() && 3 != expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dwithin expr arguments invalid", K(ret), K(expr->get_param_count()));
        } else if (2 != expr->get_param_count() && T_FUN_SYS_ST_DWITHIN != expr->get_expr_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr must has 2 arguments", K(ret));
        } else if (OB_ISNULL(left = expr->get_param_expr(0)) ||
                   OB_ISNULL(right = expr->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexcept null param expr", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(left, left)) ||
                   OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(right, right))) {
          LOG_WARN("failed to get expr without lossless cast", K(ret));
        } else if (left->is_column_ref_expr() &&
                   table_id == static_cast<ObColumnRefRawExpr*>(left)->get_table_id()) {
          if (right->get_relation_ids().has_member(stmt->get_table_bit_index(table_id))) {
            //do nothing
          } else if (OB_FAIL(add_var_to_array_no_dup(col_exprs, static_cast<ObColumnRefRawExpr*>(left)))) {
            LOG_WARN("failed to push back column expr", K(ret));
          }
        } else if (right->is_column_ref_expr() &&
                   table_id == static_cast<ObColumnRefRawExpr*>(right)->get_table_id()) {
          if (left->get_relation_ids().has_member(stmt->get_table_bit_index(table_id))) {
            //do nothing
          } else if (OB_FAIL(add_var_to_array_no_dup(col_exprs, static_cast<ObColumnRefRawExpr*>(right)))) {
            LOG_WARN("failed to push back column expr", K(ret));
          }
        }
        break;
      }
      case T_OP_IS:
      case T_OP_LIKE:
      case T_OP_BTW:
      case T_OP_NOT_BTW:
      case T_OP_IN:
      {
        ObRawExpr *left = NULL;
        ObRawExpr *right = NULL;
        if (OB_ISNULL(left = expr->get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexcept null param expr", K(ret));
        } else if (left->is_column_ref_expr() &&
                   table_id == static_cast<ObColumnRefRawExpr*>(left)->get_table_id()) {
          bool is_simple = true;
          for (int64_t i = 1; OB_SUCC(ret) && is_simple && i < expr->get_param_count(); ++i) {
            if (OB_ISNULL(right = expr->get_param_expr(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexcept null param expr", K(ret));
            } else if (right->get_relation_ids().has_member(stmt->get_table_bit_index(table_id))) {
              is_simple = false;
            }
          }
          if (OB_SUCC(ret) && is_simple) {
            if (OB_FAIL(add_var_to_array_no_dup(col_exprs, static_cast<ObColumnRefRawExpr*>(left)))) {
              LOG_WARN("failed to push back column expr", K(ret));
            }
          }
        }
        break;
      }
      case T_OP_BOOL:
      {
        if (expr->is_spatial_expr()) {
          ObRawExpr *geo_expr = ObRawExprUtils::skip_inner_added_expr(expr);
          if (OB_FAIL(SMART_CALL(get_simple_filter_column(stmt,
                                                          geo_expr,
                                                          table_id,
                                                          col_exprs)))) {
            LOG_WARN("failed to get spatial filter column", K(ret));
          }
        }
        break;
      }
      case T_OP_AND:
      case T_OP_OR:
      {
        for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
          if (OB_FAIL(SMART_CALL(get_simple_filter_column(stmt,
                                                          expr->get_param_expr(i),
                                                          table_id,
                                                          col_exprs)))) {
            LOG_WARN("failed to get simple filter column", K(ret));
          }
        }
        break;
      }
      default:
      break;
    }
  }
  return ret;
}

/**
 * @brief 获取当前stmt是哪一个stmt的generated table或者是
 * set stmt的child query，如果是generated table，还需要返回table id
 * 如果当前stmt是某个stmt的子查询，例如 c1 > subquery，则认为它没有parent stmt
 */
int ObTransformUtils::get_parent_stmt(const ObDMLStmt *root_stmt,
                                      const ObDMLStmt *stmt,
                                      const ObDMLStmt *&parent_stmt,
                                      int64_t &table_id,
                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  parent_stmt = NULL;
  table_id = OB_INVALID;
  is_valid = false;
  if (OB_ISNULL(root_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (root_stmt == stmt) {
    //do nothing
  } else {
    //检查是否是set stmt的子查询
    if (root_stmt->is_set_stmt()) {
      const ObIArray<ObSelectStmt*> &child_query =
          static_cast<const ObSelectStmt*>(root_stmt)->get_set_query();
      for (int64_t i = 0; OB_SUCC(ret) && i < child_query.count(); ++i) {
        if (child_query.at(i) == stmt) {
          is_valid = true;
          parent_stmt = root_stmt;
        }
      }
    }
    //从所有的generated table中查找
    for (int64_t j = 0; OB_SUCC(ret) && !is_valid && j < root_stmt->get_table_size(); ++j) {
      const TableItem *table = root_stmt->get_table_item(j);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (!table->is_generated_table()) {
        //do nothing
      } else if (stmt == table->ref_query_) {
        is_valid = true;
        table_id = table->table_id_;
        parent_stmt = root_stmt;
      }
    }
    if (OB_FAIL(ret) || is_valid) {
      //do nothing
    } else {
      //没找到递归遍历所有的child stmt
      ObSEArray<ObSelectStmt *, 8> child_stmts;
      if (OB_FAIL(root_stmt->get_child_stmts(child_stmts))) {
        LOG_WARN("failed to get child stmts", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < child_stmts.count(); ++i) {
        if (OB_FAIL(SMART_CALL(get_parent_stmt(child_stmts.at(i),
                                               stmt,
                                               parent_stmt,
                                               table_id,
                                               is_valid)))) {
          LOG_WARN("failed to get parent stmt", K(ret));
        }
      }
    }
  }
  return ret;
}

/**
 * @brief get_simple_filter_column_in_parent_stmt
 * 如果当前stmt是parent stmt的generated table，
 * 检查parent stmt是否存在当前stmt的指定table的简单谓词
 */
int ObTransformUtils::get_simple_filter_column_in_parent_stmt(const ObDMLStmt *root_stmt,
                                                              const ObDMLStmt *stmt,
                                                              const ObDMLStmt *view_stmt,
                                                              int64_t table_id,
                                                              ObIArray<ObColumnRefRawExpr*> &col_exprs)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *parent_stmt = NULL;
  bool is_valid = false;
  int64_t view_table_id = OB_INVALID;
  ObSEArray<ObColumnRefRawExpr*, 8> parent_col_exprs;
  const ObSelectStmt *sel_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!view_stmt->is_select_stmt()) {
    //do nothing
  } else if (OB_FALSE_IT(sel_stmt = static_cast<const ObSelectStmt*>(view_stmt))) {
  } else if (OB_FAIL(get_parent_stmt(root_stmt,
                                     stmt,
                                     parent_stmt,
                                     view_table_id,
                                     is_valid))) {
    LOG_WARN("failed to get parent stmt", K(ret));
  } else if (!is_valid) {
    //do nothing
  } else if (OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (parent_stmt->is_set_stmt()) {
    if OB_FAIL(SMART_CALL(get_simple_filter_column_in_parent_stmt(root_stmt,
                                                                  parent_stmt,
                                                                  view_stmt,
                                                                  table_id,
                                                                  col_exprs))) {
      LOG_WARN("failed to get filter column in parent stmt", K(ret));
    }
  } else if (OB_FAIL(SMART_CALL(get_filter_columns(root_stmt,
                                                   parent_stmt,
                                                   view_table_id,
                                                   parent_col_exprs)))) {
    LOG_WARN("failed to get filter columns", K(ret));
  } else if (!parent_col_exprs.empty()) {
    //检查parent stmt的简单谓词列是否是指定table的column
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_col_exprs.count(); ++i) {
      ObColumnRefRawExpr* col = parent_col_exprs.at(i);
      int64_t sel_idx = -1;
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null column expr", K(ret));
      } else if (OB_FALSE_IT(sel_idx = col->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
      } else if (sel_idx < 0 || sel_idx >= sel_stmt->get_select_item_size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select item index is incorrect", K(sel_idx), K(ret));
      } else {
        ObRawExpr *sel_expr = sel_stmt->get_select_item(sel_idx).expr_;
        ObColumnRefRawExpr *col_expr = NULL;
        if (OB_ISNULL(sel_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (!sel_expr->is_column_ref_expr()) {
          //do nothing
        } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(sel_expr))) {
        } else if (table_id != col_expr->get_table_id()) {
          //do nothing
        } else if (OB_FAIL(add_var_to_array_no_dup(col_exprs, col_expr))) {
          LOG_WARN("failed to push back column expr", K(ret));
        }
      }
    }
  }
  return ret;
}

/**
 * @brief get_filter_columns
 * 自底向上检查所有stmt的conditions，获取指定table上的
 * 简单谓词
 */
int ObTransformUtils::get_filter_columns(const ObDMLStmt *root_stmt,
                                         const ObDMLStmt *stmt,
                                         int64_t table_id,
                                         ObIArray<ObColumnRefRawExpr*> &col_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else {
    const ObIArray<ObRawExpr*> &conditions = stmt->get_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
      if (OB_FAIL(get_simple_filter_column(stmt,
                                          conditions.at(i),
                                          table_id,
                                          col_exprs))) {
        LOG_WARN("failed to get simple filter column", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(get_simple_filter_column_in_parent_stmt(root_stmt,
                                                              stmt,
                                                              stmt,
                                                              table_id,
                                                              col_exprs))) {
      LOG_WARN("failed to get simple filter column in parent stmt", K(ret));
    }
  }
  return ret;
}

/**
 * @brief check_column_match_index
 * 检查当前column是否能够与其他常量谓词或
 * 上层stmt下推的谓词匹配索引前缀
 */
int ObTransformUtils::check_column_match_index(const ObDMLStmt *root_stmt,
                                               const ObDMLStmt *stmt,
                                               ObSqlSchemaGuard *schema_guard,
                                               const ObColumnRefRawExpr *col_expr,
                                               bool &is_match)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = NULL;
  is_match = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(col_expr)
      || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(col_expr), K(schema_guard));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(
                         col_expr->get_table_id()))) {
    //不是当前stmt的column expr，do nothing
    LOG_TRACE("table item not exists in this stmt", K(ret), K(*col_expr), K(table_item));
  } else if (table_item->is_basic_table()) {
    ObSEArray<ObColumnRefRawExpr*, 8> col_exprs;
    if (OB_FAIL(get_filter_columns(root_stmt,
                                   stmt,
                                   table_item->table_id_,
                                   col_exprs))) {
      LOG_WARN("failed to get filter columns", K(ret));
    } else if (OB_FAIL(is_match_index(schema_guard,
                                      stmt,
                                      col_expr,
                                      is_match,
                                      NULL, NULL,
                                      &col_exprs))) {
      LOG_WARN("failed to check is match index", K(ret));
    }
  } else if (table_item->is_generated_table()) {
    //generated table需要递归找到基表
    int64_t sel_idx = col_expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
    const ObSelectStmt *subquery = table_item->ref_query_;
    if (OB_ISNULL(subquery)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ref query", K(ret));
    } else if (OB_FAIL(check_select_item_match_index(root_stmt,
                                                     subquery,
                                                     schema_guard,
                                                     sel_idx,
                                                     is_match))) {
      LOG_WARN("failed to check select item match index", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::check_select_item_match_index(const ObDMLStmt *root_stmt,
                                                    const ObSelectStmt *stmt,
                                                    ObSqlSchemaGuard *schema_guard,
                                                    int64_t sel_index,
                                                    bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(schema_guard));
  } else if (stmt->is_set_stmt()) {
    const ObIArray<ObSelectStmt*> &child_query = stmt->get_set_query();
    for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < child_query.count(); ++i) {
      ret = SMART_CALL(check_select_item_match_index(root_stmt, stmt->get_set_query(i),
                                                     schema_guard, sel_index, is_match));
    }
  } else {
    if (sel_index < 0 || sel_index >= stmt->get_select_item_size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select item index is incorrect", K(sel_index), K(ret));
    } else {
      ObRawExpr *sel_expr = stmt->get_select_item(sel_index).expr_;
      ObColumnRefRawExpr *col = NULL;
      if (OB_ISNULL(sel_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (!sel_expr->is_column_ref_expr()) {
        //do nothing
      } else if (OB_FALSE_IT(col = static_cast<ObColumnRefRawExpr*>(sel_expr))) {
      } else if (OB_FAIL(SMART_CALL(check_column_match_index(root_stmt,
                                                             stmt,
                                                             schema_guard,
                                                             col,
                                                             is_match)))) {
        LOG_WARN("failed to check column match index", K(ret));
      }
    }
  }
  return ret;
}

// output index_ids contains: table_ref_id, mock rowid index id and other index id
int ObTransformUtils::get_vaild_index_id(ObSqlSchemaGuard *schema_guard,
                                         const ObDMLStmt *stmt,
                                         const TableItem *table_item,
                                         ObIArray<uint64_t> &index_ids)
{
  int ret = OB_SUCCESS;
  index_ids.reuse();
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(schema_guard) || OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(schema_guard), K(stmt), K(table_item));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_item->ref_id_, table_schema, table_item->is_link_table()))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_item->ref_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is NULL", K(ret), K(table_schema));
  } else if (OB_FAIL(table_schema->get_simple_index_infos(simple_index_infos, false))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  } else if (OB_FAIL(index_ids.push_back(table_item->ref_id_))) {
    LOG_WARN("failed to push back index id", K(ret), K(table_item->ref_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      if (OB_FAIL(index_ids.push_back(simple_index_infos.at(i).table_id_))) {
        LOG_WARN("failed to push back index id", K(ret), K(table_item->ref_id_));
      }
    }
  }
  return ret;
}

int ObTransformUtils::is_match_index(ObSqlSchemaGuard *schema_guard,
                                     const ObDMLStmt *stmt,
                                     const ObColumnRefRawExpr *col_expr,
                                     bool &is_match,
                                     EqualSets *equal_sets,
                                     ObIArray<ObRawExpr*> *const_exprs,
                                     ObIArray<ObColumnRefRawExpr*> *col_exprs,
                                     const bool need_match_col_exprs)
{
  int ret = OB_SUCCESS;
  uint64_t table_ref_id = OB_INVALID_ID;
  const TableItem *table_item = NULL;
  ObSEArray<uint64_t, 8> index_ids;
  is_match = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(col_expr)
      || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(col_expr), K(schema_guard));
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(
                         col_expr->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get_table_item", K(ret), K(table_item));
  } else if (!table_item->is_basic_table()) {
    // do nothing
  } else if (OB_FAIL(get_vaild_index_id(schema_guard, stmt, table_item, index_ids))) {
    LOG_WARN("fail to get vaild index id", K(ret), K(table_item->ref_id_));
  } else {
    ObSEArray<uint64_t, 8> index_cols;
    const ObTableSchema *index_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < index_ids.count(); ++i) {
      index_cols.reuse();
      if (OB_FAIL(schema_guard->get_table_schema(index_ids.at(i), table_item, index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(index_ids.at(i)));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null index schema", K(ret));
      } else if (!index_schema->get_rowkey_info().is_valid()) {
        // 一些表没有主键信息, information_schema.tables 等
        // do nothing
      } else if (OB_UNLIKELY(index_schema->is_spatial_index())
                && OB_FAIL(index_schema->get_spatial_index_column_ids(index_cols))) {
        LOG_WARN("failed to get domain index cols", K(ret));
      } else if (OB_FAIL(index_schema->get_rowkey_info().get_column_ids(index_cols))) {
        LOG_WARN("failed to get index cols", K(ret));
      } else if (OB_FAIL(is_match_index(stmt,
                                        index_cols,
                                        col_expr,
                                        is_match,
                                        equal_sets,
                                        const_exprs,
                                        col_exprs,
                                        need_match_col_exprs))) {
        LOG_WARN("failed to check is column match index", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::is_match_index(const ObDMLStmt *stmt,
                                     const ObIArray<uint64_t> &index_cols,
                                     const ObColumnRefRawExpr *col_expr,
                                     bool &is_match,
                                     EqualSets *equal_sets,
                                     ObIArray<ObRawExpr*> *const_exprs,
                                     ObIArray<ObColumnRefRawExpr*> *col_exprs,
                                     const bool need_match_col_exprs)
{
  int ret = OB_SUCCESS;
  is_match = false;
  const ObColumnRefRawExpr *index_expr = NULL;
  bool finish_check = false;
  bool is_const = false;
  bool matched_col_exprs = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(col_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt), K(col_expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !finish_check && i < index_cols.count(); ++i) {
    is_const = false;
    if (col_expr->get_column_id() == index_cols.at(i)) {
      is_match = true;
      finish_check = true;
    } else if (OB_ISNULL(index_expr = stmt->get_column_expr_by_id(
                           col_expr->get_table_id(), index_cols.at(i)))) {
      finish_check = true;
    } else if (NULL == equal_sets && NULL != const_exprs &&
               OB_FAIL(ObOptimizerUtil::is_const_expr(index_expr, *const_exprs, is_const))) {
      LOG_WARN("failed to check if is const expr", K(index_expr), K(ret));
    } else if (NULL != equal_sets && NULL != const_exprs &&
               OB_FAIL(ObOptimizerUtil::is_const_expr(index_expr,
                                                      *equal_sets,
                                                      *const_exprs,
                                                      is_const))) {
      LOG_WARN("failed to check is_const_expr", K(ret));
    } else if (is_const) {
      /* do nothing */
    } else if (NULL != col_exprs && ObOptimizerUtil::find_item(*col_exprs, index_expr)) {
      matched_col_exprs = true;
    } else {
      finish_check = true;
    }
  }
  if (OB_SUCC(ret) && is_match && need_match_col_exprs && !matched_col_exprs) {
    is_match = false;
  }
  return ret;
}

int ObTransformUtils::classify_scalar_query_ref(ObIArray<ObRawExpr*> &exprs,
                                                ObIArray<ObRawExpr*> &scalar_query_refs,
                                                ObIArray<ObRawExpr*> &non_scalar_query_refs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(classify_scalar_query_ref(exprs.at(i),
                                          scalar_query_refs,
                                          non_scalar_query_refs))) {
      LOG_WARN("failed to extract query ref exprs", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformUtils::classify_scalar_query_ref
 *    extract query refs from expr, and classify them into two kinds
 * @param expr
 * @param scalar_query_refs: if a query ref returns a scalar value, it is considered as a scalar one.
 * @param non_scalar_query_refs: if a query ref returns multi row or col, it is considered as a nonscalar one.
 *   Such classification is required because, only scalar query ref can acts as a select expr
 * @return
 */
int ObTransformUtils::classify_scalar_query_ref(ObRawExpr *expr,
                                                ObIArray<ObRawExpr*> &scalar_query_refs,
                                                ObIArray<ObRawExpr*> &non_scalar_query_refs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_query_ref_expr()) {
    ObQueryRefRawExpr *query_ref = static_cast<ObQueryRefRawExpr *>(expr);
    if (!query_ref->is_scalar()) {
      // if a query ref returns multi row or multi col,
      // we consider such query ref as a non-scalar one.
      if (OB_FAIL(add_var_to_array_no_dup(non_scalar_query_refs, expr))) {
        LOG_WARN("failed to push back non scalar query refs", K(ret));
      }
    } else {
      // if a query ref returns a scalar value
      // we consider such query ref as a scalar one.
      if (OB_FAIL(add_var_to_array_no_dup(scalar_query_refs, expr))) {
        LOG_WARN("failed to push back query ref", K(ret));
      }
    }
  } else if (T_REF_ALIAS_COLUMN == expr->get_expr_type()) {
    // a very special design for vector update set
    if (OB_FAIL(add_var_to_array_no_dup(scalar_query_refs, expr))) {
      LOG_WARN("failed to push back alias query ref", K(ret));
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(classify_scalar_query_ref(expr->get_param_expr(i),
                                                       scalar_query_refs,
                                                       non_scalar_query_refs)))) {
        LOG_WARN("failed to extract query ref expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::extract_query_ref_expr(const ObIArray<ObRawExpr*> &exprs,
                                             ObIArray<ObQueryRefRawExpr *> &subqueries,
                                             const bool with_nested)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(extract_query_ref_expr(exprs.at(i), subqueries, with_nested))) {
      LOG_WARN("failed to extract query ref exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::extract_query_ref_expr(ObRawExpr *expr,
                                             ObIArray<ObQueryRefRawExpr *> &subqueries,
                                             const bool with_nested)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (!expr->has_flag(CNT_SUB_QUERY)) {
    // do nothing
  } else if (expr->is_query_ref_expr() &&
             OB_FAIL(add_var_to_array_no_dup(subqueries, static_cast<ObQueryRefRawExpr *>(expr)))) {
    LOG_WARN("failed to add subquery to array", K(ret));
  } else if (!expr->is_query_ref_expr() || with_nested) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_query_ref_expr(expr->get_param_expr(i), subqueries)))) {
        LOG_WARN("failed to extract query ref expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::extract_aggr_expr(ObIArray<ObRawExpr*> &exprs,
                                        ObIArray<ObAggFunRawExpr*> &aggrs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(extract_aggr_expr(exprs.at(i), aggrs))) {
      LOG_WARN("failed to extract agg exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::extract_aggr_expr(ObRawExpr *expr,
                                        ObIArray<ObAggFunRawExpr*> &aggrs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_aggr_expr()) {
    ret = add_var_to_array_no_dup(aggrs, static_cast<ObAggFunRawExpr*>(expr));
  } else if (expr->has_flag(CNT_AGG)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_aggr_expr(expr->get_param_expr(i), aggrs)))) {
        LOG_WARN("failed to extract aggr expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::extract_winfun_expr(ObIArray<ObRawExpr*> &exprs,
                                          ObIArray<ObWinFunRawExpr*> &win_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(extract_winfun_expr(exprs.at(i), win_exprs))) {
      LOG_WARN("failed to extract winfun exprs", K(ret));
    }
  }
  return ret;
}

int  ObTransformUtils::extract_winfun_expr(ObRawExpr *expr,
                                           ObIArray<ObWinFunRawExpr*> &win_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_win_func_expr()) {
    ret = add_var_to_array_no_dup(win_exprs, static_cast<ObWinFunRawExpr*>(expr));
  } else if (expr->has_flag(CNT_WINDOW_FUNC)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_winfun_expr(expr->get_param_expr(i), win_exprs)))) {
        LOG_WARN("failed to extract aggr expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::extract_alias_expr(ObRawExpr *expr,
                                         ObIArray<ObAliasRefRawExpr *> &alias_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_alias_ref_expr()) {
    if (OB_FAIL(alias_exprs.push_back(static_cast<ObAliasRefRawExpr*>(expr)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  } else if (expr->has_flag(CNT_ALIAS)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_alias_expr(expr->get_param_expr(i), alias_exprs)))) {
        LOG_WARN("failed to find alias expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::extract_alias_expr(ObIArray<ObRawExpr*> &exprs,
                                         ObIArray<ObAliasRefRawExpr *> &alias_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(extract_alias_expr(exprs.at(i), alias_exprs))) {
      LOG_WARN("failed to extract winfun exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::check_foreign_primary_join(const TableItem *first_table,
                                                 const TableItem *second_table,
                                                 const ObIArray<const ObRawExpr *> &first_exprs,
                                                 const ObIArray<const ObRawExpr *> &second_exprs,
                                                 ObSchemaChecker *schema_checker,
                                                 ObSQLSessionInfo *session_info,
                                                 bool &is_foreign_primary_join,
                                                 bool &is_first_table_parent,
                                                 ObForeignKeyInfo *&foreign_key_info)
{
  int ret = OB_SUCCESS;
  is_foreign_primary_join = false;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(schema_checker) || OB_ISNULL(session_info) ||OB_ISNULL(first_table) || OB_ISNULL(second_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parameters have null", K(ret), K(schema_checker), K(first_table), K(second_table));
  } else if (OB_FAIL(schema_checker->get_table_schema(session_info->get_effective_tenant_id(), first_table->ref_id_, table_schema, first_table->is_link_table()))){
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema should not be null", K(table_schema));
  } else {
    // get foreign key infos from first table's schema
    const ObIArray<ObForeignKeyInfo> &first_infos = table_schema->get_foreign_key_infos();
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < first_infos.count(); ++i) {
      const ObForeignKeyInfo &cur_info = first_infos.at(i);
      const uint64_t parent_id = cur_info.parent_table_id_;
      const uint64_t child_id = cur_info.child_table_id_;
      if (child_id == first_table->ref_id_ && parent_id == second_table->ref_id_) {
        // first table is child table
        is_first_table_parent = false;
        if (OB_FAIL(is_all_foreign_key_involved(first_exprs, second_exprs, cur_info, find))) {
          LOG_WARN("failed to check is all foreign key involved", K(ret));
        }
      } else if (child_id == second_table->ref_id_ && parent_id == first_table->ref_id_) {
        // second table is child table
        is_first_table_parent = true;
        if (OB_FAIL(is_all_foreign_key_involved(second_exprs, first_exprs, cur_info, find))) {
          LOG_WARN("failed to check is all foreign key involved", K(ret));
        }
      }
      if (OB_SUCC(ret) && find) {
        foreign_key_info = &(const_cast<ObForeignKeyInfo&>(cur_info));
        is_foreign_primary_join = true;
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_foreign_primary_join(const TableItem *first_table,
                                                 const TableItem *second_table,
                                                 const ObIArray<ObRawExpr *> &first_exprs,
                                                 const ObIArray<ObRawExpr *> &second_exprs,
                                                 ObSchemaChecker *schema_checker,
                                                 ObSQLSessionInfo *session_info,
                                                 bool &is_foreign_primary_join,
                                                 bool &is_first_table_parent,
                                                 ObForeignKeyInfo *&foreign_key_info)
{
  int ret = OB_SUCCESS;
  is_foreign_primary_join = false;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(schema_checker) || OB_ISNULL(first_table) || OB_ISNULL(second_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parameters have null", K(ret), K(schema_checker), K(first_table), K(second_table));
  } else if (OB_FAIL(schema_checker->get_table_schema(session_info->get_effective_tenant_id(), first_table->ref_id_, table_schema, first_table->is_link_table()))){
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema should not be null", K(table_schema));
  } else {
    // get foreign key infos from first table's schema
    const ObIArray<ObForeignKeyInfo> &first_infos = table_schema->get_foreign_key_infos();
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < first_infos.count(); ++i) {
      const ObForeignKeyInfo &cur_info = first_infos.at(i);
      const uint64_t parent_id = cur_info.parent_table_id_;
      const uint64_t child_id = cur_info.child_table_id_;
      if (child_id == first_table->ref_id_ && parent_id == second_table->ref_id_) {
        // first table is child table
        is_first_table_parent = false;
        if (OB_FAIL(is_all_foreign_key_involved(first_exprs, second_exprs, cur_info, find))) {
          LOG_WARN("failed to check is all foreign key involved", K(ret));
        }
      } else if (child_id == second_table->ref_id_ && parent_id == first_table->ref_id_) {
        // second table is child table
        is_first_table_parent = true;
        if (OB_FAIL(is_all_foreign_key_involved(second_exprs, first_exprs, cur_info, find))) {
          LOG_WARN("failed to check is all foreign key involved", K(ret));
        }
      }
      if (OB_SUCC(ret) && find) {
        foreign_key_info = &(const_cast<ObForeignKeyInfo&>(cur_info));
        is_foreign_primary_join = true;
      }
    }
  }
  return ret;
}

int ObTransformUtils::is_all_foreign_key_involved(const ObIArray<const ObRawExpr *> &child_exprs,
                                                  const ObIArray<const ObRawExpr *> &parent_exprs,
                                                  const ObForeignKeyInfo &info,
                                                  bool &is_all_involved)
{
  int ret = OB_SUCCESS;
  is_all_involved = false;
  const int64_t N = info.child_column_ids_.count();
  // generate stmt时会进行去重，不会出现t1.c1 = t2.c1 and t1.c1 = t2.c1的情况
  if (OB_UNLIKELY(child_exprs.count() != parent_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child exprs and parent exprs should have equal size",
              K(ret), K(child_exprs.count()), K(parent_exprs.count()));
  } else if (N == child_exprs.count()) {
    int64_t match = 0;
    for (int64_t i = 0; i < N; ++i) {
      bool find = false;
      const ObRawExpr *child_expr = child_exprs.at(i);
      const ObRawExpr *parent_expr = parent_exprs.at(i);
      if (OB_ISNULL(child_expr) || OB_ISNULL(parent_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child expr or parent expr is null", K(ret), K(child_expr), K(parent_expr));
      } else if (OB_UNLIKELY(!child_expr->has_flag(IS_COLUMN) || !parent_expr->has_flag(IS_COLUMN))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not column expr", K(ret));
      } else{
        const ObColumnRefRawExpr *child_col = static_cast<const ObColumnRefRawExpr *>(child_exprs.at(i));
        const ObColumnRefRawExpr *parent_col = static_cast<const ObColumnRefRawExpr *>(parent_exprs.at(i));
        for (int64_t j = 0; !find && j < N; ++j) {
          if(parent_col->get_column_id() == info.parent_column_ids_.at(j)
             && child_col->get_column_id() == info.child_column_ids_.at(j)) {
            ++match;
            find = true;
          }
        }
      }
    }
    if (N == match) {
      is_all_involved = true;
    }
  }
  return ret;
}

int ObTransformUtils::is_all_foreign_key_involved(const ObIArray<ObRawExpr *> &child_exprs,
                                                  const ObIArray<ObRawExpr *> &parent_exprs,
                                                  const ObForeignKeyInfo &info,
                                                  bool &is_all_involved)
{
  int ret = OB_SUCCESS;
  is_all_involved = false;
  const int64_t N = info.child_column_ids_.count();
  // generate stmt时会进行去重，不会出现t1.c1 = t2.c1 and t1.c1 = t2.c1的情况
  if (OB_UNLIKELY(child_exprs.count() != parent_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child exprs and parent exprs should have equal size",
              K(ret), K(child_exprs.count()), K(parent_exprs.count()));
  } else if (N == child_exprs.count()) {
    int64_t match = 0;
    for (int64_t i = 0; i < N; ++i) {
      bool find = false;
      ObRawExpr *child_expr = child_exprs.at(i);
      ObRawExpr *parent_expr = parent_exprs.at(i);
      if (OB_ISNULL(child_expr) || OB_ISNULL(parent_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child expr or parent expr is null", K(ret), K(child_expr), K(parent_expr));
      } else if (OB_UNLIKELY(!child_expr->has_flag(IS_COLUMN) || !parent_expr->has_flag(IS_COLUMN))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not column expr", K(ret));
      } else{
        ObColumnRefRawExpr *child_col = static_cast<ObColumnRefRawExpr *>(child_exprs.at(i));
        ObColumnRefRawExpr *parent_col = static_cast<ObColumnRefRawExpr *>(parent_exprs.at(i));
        for (int64_t j = 0; !find && j < N; ++j) {
          if(parent_col->get_column_id() == info.parent_column_ids_.at(j)
             && child_col->get_column_id() == info.child_column_ids_.at(j)) {
            ++match;
            find = true;
          }
        }
      }
    }
    if (N == match) {
      is_all_involved = true;
    }
  }
  return ret;
}

int ObTransformUtils::is_foreign_key_rely(ObSQLSessionInfo* session_info,
                                          const share::schema::ObForeignKeyInfo *foreign_key_info,
                                          bool &is_rely)
{
  int ret = OB_SUCCESS;
  int64_t foreign_key_checks = 0;
  is_rely = false;
  if (OB_ISNULL(session_info) || OB_ISNULL(foreign_key_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param has null", K(ret), K(session_info), K(foreign_key_info));
  } else if (SMO_IS_ORACLE_MODE(session_info->get_sql_mode())) {
    //oracle模式下的主外键约束检查目前用validate_flag_，为确保之后rely_flag生效，可以先判定rely_flag_
    if (foreign_key_info->rely_flag_) {
      is_rely = true;
    } else if (foreign_key_info->is_validated()) {
      is_rely = true;
    } else {
      is_rely = false;
    }
  } else if (!foreign_key_info->is_parent_table_mock_
             && OB_FAIL(session_info->get_foreign_key_checks(foreign_key_checks))) {
    LOG_WARN("get var foreign_key_checks failed", K(ret));
  } else if (foreign_key_checks) {
    is_rely = true;
  }
  return ret;
}

int ObTransformUtils::check_stmt_limit_validity(ObTransformerCtx *ctx,
                                                const ObSelectStmt *select_stmt,
                                                bool &is_valid,
                                                bool &need_add_const_constraint)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  is_valid = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx) || OB_ISNULL(ctx->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt), K(ctx),
                                    K(ctx->exec_ctx_), K(plan_ctx));
  } else if (!select_stmt->has_limit() ||
             select_stmt->get_offset_expr() != NULL ||
             select_stmt->get_limit_percent_expr() != NULL) {
    //do nothing
  } else if (select_stmt->has_distinct()
             || select_stmt->has_group_by()
             || select_stmt->is_set_stmt()
             || select_stmt->is_hierarchical_query()
             || select_stmt->has_order_by()
             || select_stmt->is_contains_assignment()
             || select_stmt->has_window_function()
             || select_stmt->has_sequence()) {
    /*do nothing*/
  } else if (OB_FAIL(select_stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to has rownum", K(ret));
  } else if (has_rownum) {
    /*do nothing */
  } else {
    bool is_null_value = false;
    int64_t limit_value = 0;
    if (OB_FAIL(ObTransformUtils::get_expr_int_value(select_stmt->get_limit_expr(),
                                                  &plan_ctx->get_param_store(),
                                                  ctx->exec_ctx_,
                                                  ctx->allocator_,
                                                  limit_value,
                                                  is_null_value))) {
      LOG_WARN("failed to get_expr_int_value", K(ret));
    } else if (!is_null_value && limit_value >= 1) {
      is_valid = true;
      //Just in case different parameters hit same plan, firstly we need add const param constraint
      need_add_const_constraint = true;
    }
  }
  return ret;
}

int ObTransformUtils::check_stmt_is_non_sens_dul_vals(ObTransformerCtx *ctx,
                                                      const ObDMLStmt *upper_stmt,
                                                      const ObDMLStmt *stmt,
                                                      bool &is_match,
                                                      bool &need_add_limit_constraint)
{
  int ret = OB_SUCCESS;
  is_match = false;
  need_add_limit_constraint = false;
  if (OB_ISNULL(upper_stmt) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(upper_stmt), K(stmt), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < upper_stmt->get_condition_size(); ++i) {
      const ObRawExpr *cond_expr = upper_stmt->get_condition_expr(i);
      if (OB_ISNULL(cond_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null1", K(ret), K(cond_expr));
      } else if (OB_FAIL(check_stmt_is_non_sens_dul_vals_rec(ctx, 
                                                            stmt, 
                                                            cond_expr, 
                                                            is_match, 
                                                            need_add_limit_constraint))) {
        LOG_WARN("failed to check semi join info", K(ret), K(*cond_expr));
      } else {
        /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_stmt_is_non_sens_dul_vals_rec(ObTransformerCtx *ctx,
                                              const ObDMLStmt *stmt,
                                              const ObRawExpr *expr,
                                              bool &is_match,
                                              bool &need_add_limit_constraint)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *param_expr = NULL;
  const ObRawExpr *param_child = NULL;  // child of param
  if (OB_ISNULL(ctx) || OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (is_match) {
    /*do nothing*/
  } else if (T_OP_EXISTS == expr->get_expr_type() ||
                 T_OP_NOT_EXISTS == expr->get_expr_type()) {
    if (OB_ISNULL(param_child = expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(param_child), K(ret));
    } else if (!param_child->is_query_ref_expr()) {
      /*do nothing*/
    } else if (static_cast<const ObQueryRefRawExpr *>(param_child)->get_ref_stmt() == stmt) {
      if (static_cast<const ObQueryRefRawExpr *>(param_child)->get_ref_stmt()->is_spj()) {
        is_match = true;
      } else if (OB_FAIL(check_stmt_limit_validity(ctx,
                                                   static_cast<const ObQueryRefRawExpr *>(param_child)->get_ref_stmt(),
                                                   is_match,
                                                   need_add_limit_constraint))) {
        LOG_WARN("failed to check stmt limit validity", K(ret));
      } 
    }
  } else if (expr->has_flag(IS_WITH_ALL) ||
                 expr->has_flag(IS_WITH_ANY)) {
    if (OB_ISNULL(param_expr = expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL pointer error", K(param_expr), K(ret));
    } else if (!param_expr->is_query_ref_expr()) {
      /*do nothing*/
    } else if (static_cast<const ObQueryRefRawExpr *>(param_expr)->get_ref_stmt() == stmt) {
      is_match = static_cast<const ObQueryRefRawExpr *>(param_expr)->get_ref_stmt()->is_spj();
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      const ObRawExpr *param = expr->get_param_expr(i);
      if (OB_ISNULL(param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null param", K(ret), K(*param));
      } else if (OB_FAIL(SMART_CALL(check_stmt_is_non_sens_dul_vals_rec(ctx,
                                                                      stmt,
                                                                      param,
                                                                      is_match,
                                                                      need_add_limit_constraint)))) {
        LOG_WARN("failed to check semi join info recursively", K(ret), K(*param));
      }
    }
  }
  return ret;
}                                              
int ObTransformUtils::check_exprs_unique(const ObDMLStmt &stmt,
                                         TableItem *table,
                                         const ObIArray<ObRawExpr*> &exprs,
                                         const ObIArray<ObRawExpr*> &conditions,
                                         ObSQLSessionInfo *session_info,
                                         ObSchemaChecker *schema_checker,
                                         bool &is_unique)
{
  return check_exprs_unique_on_table_items(&stmt, session_info, schema_checker,
                                           table, exprs, conditions, true, is_unique);
}

int ObTransformUtils::check_exprs_unique(const ObDMLStmt &stmt,
                                         TableItem *table,
                                         const ObIArray<ObRawExpr*> &exprs,
                                         ObSQLSessionInfo *session_info,
                                         ObSchemaChecker *schema_checker,
                                         bool &is_unique)
{
  ObSEArray<ObRawExpr *, 1> dummy_condition;
  return check_exprs_unique_on_table_items(&stmt, session_info, schema_checker, table,
                                           exprs, dummy_condition, false, is_unique);
}

int ObTransformUtils::check_exprs_unique_on_table_items(const ObDMLStmt *stmt,
                                                        ObSQLSessionInfo *session_info,
                                                        ObSchemaChecker *schema_checker,
                                                        TableItem *table,
                                                        const ObIArray<ObRawExpr*> &exprs,
                                                        const ObIArray<ObRawExpr*> &conditions,
                                                        bool is_strict,
                                                        bool &is_unique)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem *, 1> table_items;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(table_items.push_back(table))) {
    LOG_WARN("failed to push back tableitem", K(ret));
  } else if (OB_FAIL(check_exprs_unique_on_table_items(stmt, session_info, schema_checker,
                                    table_items, exprs, conditions, is_strict, is_unique))) {
    LOG_WARN("failed to check exprs unique on table items", K(ret));
  }
  return ret;
}

int ObTransformUtils::check_exprs_unique_on_table_items(const ObDMLStmt *stmt,
                                                        ObSQLSessionInfo *session_info,
                                                        ObSchemaChecker *schema_checker,
                                                        const ObIArray<TableItem*> &table_items,
                                                        const ObIArray<ObRawExpr*> &exprs,
                                                        const ObIArray<ObRawExpr*> &conditions,
                                                        bool is_strict,
                                                        bool &is_unique)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    ObArenaAllocator alloc("CheckUnique", OB_MALLOC_NORMAL_BLOCK_SIZE,
                           session_info->get_effective_tenant_id(),
                           ObCtxIds::DEFAULT_CTX_ID);
    ObFdItemFactory fd_item_factory(alloc);
    ObRawExprFactory expr_factory(alloc);
    UniqueCheckHelper check_helper;
    check_helper.alloc_ = &alloc;
    check_helper.fd_factory_ = &fd_item_factory;
    check_helper.expr_factory_ = &expr_factory;
    check_helper.schema_checker_ = schema_checker;
    check_helper.session_info_ = session_info;
    UniqueCheckInfo res_info;
    ObRelIds all_tables;
    if (OB_FAIL(compute_tables_property(stmt, check_helper, table_items, conditions, res_info))) {
      LOG_WARN("failed to compute tables property", K(ret));
    } else if (OB_FAIL(get_rel_ids_from_tables(stmt, table_items, all_tables))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (!is_strict && OB_FAIL(append(res_info.fd_sets_, res_info.candi_fd_sets_))) {
      // is strict, use fd_item_set & candi_fd_set check unique
      LOG_WARN("failed to append fd item sets", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(exprs, all_tables, res_info.fd_sets_,
                                                        res_info.equal_sets_,
                                                        res_info.const_exprs_,
                                                        is_unique))) {
      LOG_WARN("failed to check is exprs unique", K(ret));
    } else {
      fd_item_factory.destory();
      expr_factory.destory();
      LOG_TRACE("get is unique result", K(exprs), K(table_items), K(is_unique));
    }
  }
  return ret;
}

int ObTransformUtils::check_stmt_unique(const ObSelectStmt *stmt,
                                        ObSQLSessionInfo *session_info,
                                        ObSchemaChecker *schema_checker,
                                        const bool is_strict,
                                        bool &is_unique)
{
  int ret = OB_SUCCESS;
  is_unique = false;
  ObSEArray<ObRawExpr *, 16> select_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(stmt->get_select_exprs_without_lob(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(SMART_CALL(check_stmt_unique(stmt,
                                                  session_info,
                                                  schema_checker,
                                                  select_exprs,
                                                  is_strict,
                                                  is_unique)))) {
    LOG_WARN("failed to check stmt unique", K(ret));
  }
  return ret;
}

int ObTransformUtils::check_stmt_unique(const ObSelectStmt *stmt,
                                        ObSQLSessionInfo *session_info,
                                        ObSchemaChecker *schema_checker,
                                        const ObIArray<ObRawExpr *> &exprs,
                                        const bool is_strict,
                                        bool &is_unique,
                                        const uint64_t extra_flags) //default value FLAGS_DEFAULT(0)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> select_exprs;
  bool ignore_distinct = (extra_flags & FLAGS_IGNORE_DISTINCT) == FLAGS_IGNORE_DISTINCT;
  bool ignore_group = (extra_flags & FLAGS_IGNORE_GROUP) == FLAGS_IGNORE_GROUP;
  is_unique = false;
  bool need_check = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->is_hierarchical_query()) {
    //层次查询暂时设为not unique, 即使输出列为pk也难以保证unique
    is_unique = false;
    need_check = false;
  } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (stmt->is_set_stmt()) {
    ObSEArray<ObRawExpr *, 16> set_op_exprs;
    // [cast(UNION([1]), DATE(-1, -1))]
    // cast above set_op_exprs is calulated after the distinct
    if (OB_FAIL(ObRawExprUtils::extract_set_op_exprs(select_exprs, set_op_exprs))) {
      LOG_WARN("failed to extract set op exprs", K(ret));
    } else if (stmt->is_set_distinct() && ObOptimizerUtil::subset_exprs(set_op_exprs, exprs)) {
      is_unique = true;
    } else if (ObSelectStmt::INTERSECT != stmt->get_set_op()
               && ObSelectStmt::EXCEPT != stmt->get_set_op()) {
      need_check = false;
    } else {
      need_check = true;
    }
  } else if (0 == stmt->get_from_item_size()    // select expr from dual
             && ObOptimizerUtil::overlap_exprs(select_exprs, exprs)) {
    is_unique = true;
  } else if (!ignore_distinct && stmt->is_distinct()  // distinct
             && ObOptimizerUtil::subset_exprs(select_exprs, exprs)) {
    is_unique = true;
  } else if (!ignore_group && stmt->has_rollup()) {
    // rollup 不忽略 group 时直接返回 false
    // 当 exprs 为 rollup exprs 子集时为 not strict unique, 但这种场景无用
    is_unique = false;
    need_check = false;
  } else if (!ignore_group && stmt->get_group_expr_size() > 0 //group by
             && ObOptimizerUtil::subset_exprs(stmt->get_group_exprs(), exprs)) {
    is_unique = true;
  } else if (!ignore_group && stmt->is_scala_group_by() //scalar group by
             && ObOptimizerUtil::subset_exprs(select_exprs, exprs)) {
    is_unique = true;
  }

  if (OB_FAIL(ret) || is_unique || !need_check) {
    /*do nothing*/
  } else {
    ObArenaAllocator alloc("CheckUnique", OB_MALLOC_NORMAL_BLOCK_SIZE,
                           session_info->get_effective_tenant_id(),
                           ObCtxIds::DEFAULT_CTX_ID);
    ObFdItemFactory fd_item_factory(alloc);
    ObRawExprFactory expr_factory(alloc);
    UniqueCheckHelper check_helper;
    check_helper.alloc_ = &alloc;
    check_helper.fd_factory_ = &fd_item_factory;
    check_helper.expr_factory_ = &expr_factory;
    check_helper.schema_checker_ = schema_checker;
    check_helper.session_info_ = session_info;
    UniqueCheckInfo res_info;
    ObRelIds all_tables;
    if (OB_FAIL(compute_stmt_property(stmt, check_helper, res_info, extra_flags))) {
      LOG_WARN("failed to compute stmt property", K(ret));
    } else if (OB_FAIL(stmt->get_from_tables(all_tables))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (!is_strict && OB_FAIL(append(res_info.fd_sets_, res_info.candi_fd_sets_))) {
      // is strict, use fd_item_set & candi_fd_set check unique
      LOG_WARN("failed to append fd item sets", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(exprs, all_tables, res_info.fd_sets_,
                                                        res_info.equal_sets_,
                                                        res_info.const_exprs_, is_unique))) {
      LOG_WARN("failed to check is exprs unique", K(ret));
    } else {
      fd_item_factory.destory();
      expr_factory.destory();
      LOG_TRACE("get is unique result", K(ret), K(is_unique));
    }
  }
  return ret;
}

int ObTransformUtils::compute_stmt_property(const ObSelectStmt *stmt,
                                            UniqueCheckHelper &check_helper,
                                            UniqueCheckInfo &res_info,
                                            const uint64_t extra_flags) //default value FLAGS_DEFAULT(0)
{
  int ret = OB_SUCCESS;
  bool ignore_distinct = (extra_flags & FLAGS_IGNORE_DISTINCT) == FLAGS_IGNORE_DISTINCT;
  bool ignore_group = (extra_flags & FLAGS_IGNORE_GROUP) == FLAGS_IGNORE_GROUP;
  ObFdItemFactory *fd_factory = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(fd_factory = check_helper.fd_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!ignore_group && stmt->has_rollup()) {
    // 对于 rollup 不计算 property
    // 其实 rollup 后能产生类似于 candi_fd_sets 包含 null 的属性, 暂时不处理
  } else if (stmt->is_set_stmt()) {
    ret = compute_set_stmt_property(stmt, check_helper, res_info, extra_flags);
  } else if (!stmt->is_hierarchical_query()
             && OB_FAIL(compute_path_property(stmt, check_helper, res_info))) {
    LOG_WARN("failed to compute path property", K(ret));
  } else {
    //add const exprs / unique fd
    ObTableFdItem *select_unique_fd = NULL;
    ObTableFdItem *group_unique_fd = NULL;
    ObRelIds table_set;
    ObSEArray<ObRawExpr*, 4> select_exprs;
    if (OB_FAIL(stmt->get_from_tables(table_set))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (0 == stmt->get_from_item_size() || (!ignore_group && stmt->is_scala_group_by())) {
      // from dual / scalar group by: add const exprs & unique fd
      if (OB_FAIL(fd_factory->create_table_fd_item(select_unique_fd, true, select_exprs,
                                                   table_set))) {
        LOG_WARN("failed to create table fd item", K(ret));
      } else if (OB_FAIL(append_array_no_dup(res_info.const_exprs_, select_exprs))) {
        LOG_WARN("failed to append const exprs", K(ret));
      }
    } else if (!ignore_distinct && stmt->is_distinct()) { // distinct: add unique fd item
      ret = fd_factory->create_table_fd_item(select_unique_fd, true, select_exprs,
                                             table_set);
    }

    if (OB_SUCC(ret) && !ignore_group && stmt->get_group_expr_size() > 0) {
      // group by: add unique fd item
      ret = fd_factory->create_table_fd_item(group_unique_fd, true, stmt->get_group_exprs(),
                                             table_set);
    }

    //add unique fd item and deduce fd, 仅在 compute_stmt_property 中进行 deduce_fd_item_set,
    //避免 stmt property 向 upper stmt property 转化时丢失 const 信息.
    //目前仅对 fd_sets 进行推导, candi_fd_sets 仅传递不推导
    if (OB_FAIL(ret)) {
    } else if (NULL != select_unique_fd && OB_FAIL(res_info.fd_sets_.push_back(select_unique_fd))) {
      LOG_WARN("failed to push back fd item set", K(ret));
    } else if (NULL != group_unique_fd && OB_FAIL(res_info.fd_sets_.push_back(group_unique_fd))) {
      LOG_WARN("failed to push back fd item set", K(ret));
    } else if (OB_FAIL(fd_factory->deduce_fd_item_set(res_info.equal_sets_, select_exprs,
                                                      res_info.const_exprs_, res_info.fd_sets_))) {
      LOG_WARN("failed to deduce fd item set", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("get stmt in compute stmt property", K(*stmt));
    LOG_TRACE("get stmt property", K(res_info.const_exprs_), K(res_info.equal_sets_),
                                   K(res_info.fd_sets_), K(res_info.candi_fd_sets_));
  }
  return ret;
}

int ObTransformUtils::compute_set_stmt_property(const ObSelectStmt *stmt,
                                                UniqueCheckHelper &check_helper,
                                                UniqueCheckInfo &res_info,
                                                const uint64_t extra_flags) //default value FLAGS_DEFAULT(0)
{
  int ret = OB_SUCCESS;
  bool ignore_distinct = (extra_flags & FLAGS_IGNORE_DISTINCT) == FLAGS_IGNORE_DISTINCT;
  ObExprFdItem *unique_fd = NULL;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  ObSEArray<ObRawExpr*, 4> set_exprs;
  UniqueCheckInfo right_info;
  ObSEArray<ObRawExpr*, 4> equal_conds;
  EqualSets tmp_equal_sets;
  const ObSelectStmt::SetOperator set_type = stmt->get_set_op();
  ObFdItemFactory *fd_factory = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(fd_factory = check_helper.fd_factory_)
      || OB_ISNULL(check_helper.expr_factory_) || OB_ISNULL(check_helper.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!stmt->is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret));
  } else if (stmt->is_recursive_union()) {
    /*do nothing*/
  } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if ((ObSelectStmt::EXCEPT == set_type || ObSelectStmt::INTERSECT == set_type) &&
             OB_FAIL(SMART_CALL(compute_stmt_property(stmt->get_set_query(0), check_helper,
                                                      res_info)))) {
    LOG_WARN("failed to compute left stmt property", K(ret));
  } else if (ObSelectStmt::INTERSECT == set_type &&
             OB_FAIL(SMART_CALL(compute_stmt_property(stmt->get_set_query(1), check_helper,
                                                      right_info)))) {
    LOG_WARN("failed to compute right stmt property", K(ret));
  } else if (!ignore_distinct && stmt->is_set_distinct() &&
             OB_FAIL(fd_factory->create_expr_fd_item(unique_fd, true, select_exprs,
                                                     select_exprs))) {
    LOG_WARN("failed to create table fd item", K(ret));
  } else if (OB_FAIL(append(res_info.const_exprs_, right_info.const_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(tmp_equal_sets, res_info.equal_sets_))) {
    LOG_WARN("failed to append fd equal set", K(ret));
  } else if (OB_FAIL(append(tmp_equal_sets, right_info.equal_sets_))) {
    LOG_WARN("failed to append fd equal set", K(ret));
  } else if (OB_FAIL(stmt->get_pure_set_exprs(set_exprs))) {
    LOG_WARN("failed to get first set op exprs", K(ret));
  } else if (OB_FAIL(get_equal_set_conditions(*check_helper.expr_factory_,
                                              check_helper.session_info_,
                                              stmt, set_exprs, equal_conds))) {
    LOG_WARN("failed to get equal set conditions", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(check_helper.alloc_, equal_conds,
                                                        tmp_equal_sets, res_info.equal_sets_))) {
    LOG_WARN("failed to compute compute equal set", K(ret));
  } else if (OB_FAIL(append(res_info.fd_sets_, right_info.fd_sets_))) {
    LOG_WARN("failed to append fd item", K(ret));
  } else if (NULL != unique_fd && OB_FAIL(res_info.fd_sets_.push_back(unique_fd))) {
    LOG_WARN("failed to push back fd item set", K(ret));
  } else if (OB_FAIL(append(res_info.candi_fd_sets_, right_info.candi_fd_sets_))) {
    LOG_WARN("failed to append fd item", K(ret));
  } else if (OB_FAIL(append(res_info.not_null_, right_info.not_null_))) {
    LOG_WARN("failed to append fd item", K(ret));
  } else if (OB_FAIL(fd_factory->deduce_fd_item_set(res_info.equal_sets_, select_exprs,
                                                    res_info.const_exprs_, res_info.fd_sets_))) {
    LOG_WARN("failed to deduce fd item set", K(ret));
  }
  return ret;
}

int ObTransformUtils::get_equal_set_conditions(ObRawExprFactory &expr_factory,
                                               ObSQLSessionInfo *session_info,
                                               const ObSelectStmt *stmt,
                                               ObIArray<ObRawExpr*> &set_exprs,
                                               ObIArray<ObRawExpr*> &equal_conds)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObSEArray<ObSelectStmt*, 2> child_stmts;
    if (ObSelectStmt::EXCEPT == stmt->get_set_op()) {
      // keep left equal expr for except stmts
      ret = child_stmts.push_back(stmt->get_set_query(0));
    } else if (ObSelectStmt::INTERSECT == stmt->get_set_op()) {
      // keep right equal expr for intersect stmts
      ret = child_stmts.assign(stmt->get_set_query());
    }
    ObSelectStmt *child_stmt = NULL;
    ObRawExpr *set_expr = NULL;
    ObRawExpr *child_expr = NULL;
    ObRawExpr *equal_expr = NULL;
    int64_t idx = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      if (OB_ISNULL(child_stmt = child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        const int64_t select_size = child_stmt->get_select_item_size();
        for (int64_t j = 0; OB_SUCC(ret) && j < set_exprs.count(); ++j) {
          if (OB_ISNULL(set_expr = set_exprs.at(j)) || OB_UNLIKELY(!set_expr->is_set_op_expr())
              || OB_UNLIKELY((idx = static_cast<ObSetOpRawExpr*>(set_expr)->get_idx()) < 0
                             || idx >= select_size)
              || OB_ISNULL(child_expr = child_stmt->get_select_item(idx).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(expr_factory, session_info,
                                                               child_expr, set_expr, equal_expr))) {
            LOG_WARN("failed to create equal expr", K(ret));
          } else if (OB_FAIL(equal_conds.push_back(equal_expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::add_cast_for_replace(ObRawExprFactory &expr_factory,
                                           const ObRawExpr *from_expr,
                                           ObRawExpr *&to_expr,
                                           ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(from_expr) || OB_ISNULL(to_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", KP(from_expr), KP(to_expr), K(ret));
  } else {
    ObSysFunRawExpr *cast_expr = NULL;
    ObCastMode cm;
    if (OB_FAIL(ObSQLUtils::get_cast_mode_for_replace(from_expr, session_info, cm))) {
      LOG_WARN("failed to get cast mode for replace", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(expr_factory,
                                                        to_expr,
                                                        from_expr->get_result_type(),
                                                        cast_expr, session_info,
                                                        false, cm))) {
      LOG_WARN("failed to create cast expr", K(ret));
    } else {
      to_expr = cast_expr;
    }
  }
  return ret;
}

int ObTransformUtils::add_cast_for_replace_if_need(ObRawExprFactory &expr_factory,
                                                   const ObRawExpr *from_expr,
                                                   ObRawExpr *&to_expr,
                                                   ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(from_expr) || OB_ISNULL(to_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", KP(from_expr), KP(to_expr), K(ret));
  } else {
    const ObExprResType &src_type = from_expr->get_result_type();
    const ObExprResType &dst_type = to_expr->get_result_type();
    bool need_length_cast = (ob_is_string_or_lob_type(dst_type.get_type()) || ob_is_rowid_tc(dst_type.get_type()))
                            ? (src_type.get_length() != dst_type.get_length()) : false;
    bool need_zerofill_cast = from_expr->get_result_type().has_result_flag(ZEROFILL_FLAG) &&
                              !to_expr->get_result_type().has_result_flag(ZEROFILL_FLAG);
    bool need_cast = (src_type.get_type() != dst_type.get_type()) ||
                     (src_type.get_precision() != dst_type.get_precision()) ||
                     (src_type.get_scale() != dst_type.get_scale()) ||
                     need_zerofill_cast || need_length_cast;
    if (ob_is_string_or_lob_type(src_type.get_type())) {
      need_cast |= (src_type.get_collation_type() != dst_type.get_collation_type()) ||
                   (src_type.get_collation_level() != dst_type.get_collation_level());
    }
    if (need_cast && OB_FAIL(add_cast_for_replace(expr_factory, from_expr, to_expr, session_info))) {
      LOG_WARN("failed to add cast for _replace", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::UniqueCheckInfo::assign(const UniqueCheckInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    table_set_.clear_all();
    if (OB_FAIL(table_set_.add_members(other.table_set_))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(const_exprs_.assign(other.const_exprs_))) {
      LOG_WARN("failed to assign exprs", K(ret));
    } else if (OB_FAIL(equal_sets_.assign(other.equal_sets_))) {
      LOG_WARN("failed to assign equal sets", K(ret));
    } else if (OB_FAIL(fd_sets_.assign(other.fd_sets_))) {
      LOG_WARN("failed to assign fd sets", K(ret));
    } else if (OB_FAIL(candi_fd_sets_.assign(other.candi_fd_sets_))) {
      LOG_WARN("failed to assign fd sets", K(ret));
    } else if (OB_FAIL(not_null_.assign(other.not_null_))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  }
  return ret;
}

void ObTransformUtils::UniqueCheckInfo::reset()
{
  table_set_.clear_all();
  const_exprs_.reuse();
  equal_sets_.reuse();
  fd_sets_.reuse();
  candi_fd_sets_.reuse();
  not_null_.reuse();
}

int ObTransformUtils::compute_path_property(const ObDMLStmt *stmt,
                                            UniqueCheckHelper &check_helper,
                                            UniqueCheckInfo &res_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> cond_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(cond_exprs.assign(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else {

    //1. compute inner join from table
    ObSEArray<ObRawExpr*, 1> dummy_inner_join_conds;
    UniqueCheckInfo cur_info;
    bool first_table = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); ++i) {
      const FromItem& from_item = stmt->get_from_item(i);
      TableItem *table = NULL;
      ObSqlBitSet<> rel_ids;
      UniqueCheckInfo left_info;
      UniqueCheckInfo right_info;
      if (from_item.is_joined_) {
        table = static_cast<TableItem*>(stmt->get_joined_table(from_item.table_id_));
      } else {
        table = stmt->get_table_item_by_id(from_item.table_id_);
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected from item", K(ret), K(from_item));
      } else if (OB_FAIL(stmt->get_table_rel_ids(*table, rel_ids))) {
        LOG_WARN("failed to get table relids", K(ret));
      } else if (OB_FAIL(compute_table_property(stmt, check_helper, table, cond_exprs, right_info))) {
        LOG_WARN("failed to compute table property", K(ret));
      } else if (first_table) {
        first_table = false;
        ret = cur_info.assign(right_info);
      } else if (OB_FAIL(left_info.assign(cur_info))) {
        LOG_WARN("failed to assign check info", K(ret));
      } else {
        cur_info.reset();
        ret = compute_inner_join_property(stmt, check_helper, left_info, right_info,
                                          dummy_inner_join_conds, cond_exprs, cur_info);
      }
    }

    //2. enhance fd using semi on condition
    const ObIArray<SemiInfo*> &semi_infos = stmt->get_semi_infos();
    ObSEArray<ObRawExpr *, 8> semi_conditions;
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      if (OB_ISNULL(semi_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (semi_infos.at(i)->is_anti_join()) {
        /* do nothing */
      } else if (OB_FAIL(append(semi_conditions, semi_infos.at(i)->semi_conditions_))) {
        LOG_WARN("failed to append conditions", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(res_info.table_set_.add_members(cur_info.table_set_))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(res_info.const_exprs_.assign(cur_info.const_exprs_))) {
      LOG_WARN("failed to assign exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(semi_conditions,
                                                            res_info.const_exprs_))) {
      LOG_WARN("failed to compute const equivalent exprs", K(ret));
    } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(check_helper.alloc_, semi_conditions,
                                                          cur_info.equal_sets_,
                                                          res_info.equal_sets_))) {
      LOG_WARN("failed to compute equal set", K(ret));
    } else if (OB_FAIL(res_info.fd_sets_.assign(cur_info.fd_sets_))
               || OB_FAIL(res_info.candi_fd_sets_.assign(cur_info.candi_fd_sets_))) {
      LOG_WARN("failed to assign fd item set", K(ret));
    } else if (OB_FAIL(res_info.not_null_.assign(cur_info.not_null_))) {
      LOG_WARN("failed to assign exprs", K(ret));
    } else if (!semi_conditions.empty() &&
               OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(semi_conditions,
                                                            res_info.candi_fd_sets_,
                                                            res_info.fd_sets_,
                                                            res_info.not_null_))) {
      LOG_WARN("failed to enhance fd item set", K(ret));
    } else {
      LOG_TRACE("get info in compute path property", K(res_info.const_exprs_),
                      K(res_info.equal_sets_), K(res_info.fd_sets_), K(res_info.candi_fd_sets_));
    }
  }
  return ret;
}

int ObTransformUtils::compute_tables_property(const ObDMLStmt *stmt,
                                              UniqueCheckHelper &check_helper,
                                              const ObIArray<TableItem*> &table_items,
                                              const ObIArray<ObRawExpr*> &conditions,
                                              UniqueCheckInfo &res_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> cond_exprs;
  ObSEArray<ObRawExpr*, 1> dummy_inner_join_conds;
  UniqueCheckInfo cur_info;
  const TableItem *table = NULL;
  bool first_table = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(cond_exprs.assign(conditions))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    UniqueCheckInfo left_info;
    UniqueCheckInfo right_info;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(table = table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected from item", K(ret));
    } else if (OB_FAIL(compute_table_property(stmt, check_helper, table, cond_exprs, right_info))) {
      LOG_WARN("failed to compute table property", K(ret));
    } else if (first_table) {
      first_table = false;
      ret = cur_info.assign(right_info);
    } else if (OB_FAIL(left_info.assign(cur_info))) {
      LOG_WARN("failed to assign check info", K(ret));
    } else {
      cur_info.reset();
      ret = compute_inner_join_property(stmt, check_helper, left_info, right_info,
                                        dummy_inner_join_conds, cond_exprs, cur_info);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(res_info.table_set_.add_members(cur_info.table_set_))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(res_info.const_exprs_.assign(cur_info.const_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(cond_exprs,
                                                          res_info.const_exprs_))) {
    LOG_WARN("failed to compute const equivalent exprs", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(check_helper.alloc_, cond_exprs,
                                                        cur_info.equal_sets_,
                                                        res_info.equal_sets_))) {
    LOG_WARN("failed to compute equal set", K(ret));
  } else if (OB_FAIL(res_info.fd_sets_.assign(cur_info.fd_sets_))
             || OB_FAIL(res_info.candi_fd_sets_.assign(cur_info.candi_fd_sets_))) {
    LOG_WARN("failed to assign fd item set", K(ret));
  } else if (OB_FAIL(res_info.not_null_.assign(cur_info.not_null_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (!cond_exprs.empty() &&
             OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(cond_exprs,
                                                          res_info.candi_fd_sets_,
                                                          res_info.fd_sets_,
                                                          res_info.not_null_))) {
    LOG_WARN("failed to enhance fd item set", K(ret));
  } else {
    LOG_TRACE("get info in compute tables property", K(res_info.const_exprs_),
                    K(res_info.equal_sets_), K(res_info.fd_sets_), K(res_info.candi_fd_sets_));
  }
  return ret;
}

int ObTransformUtils::compute_table_property(const ObDMLStmt *stmt,
                                             UniqueCheckHelper &check_helper,
                                             const TableItem *table,
                                             ObIArray<ObRawExpr*> &cond_exprs,
                                             UniqueCheckInfo &res_info)
{
  int ret = OB_SUCCESS;
  const JoinedTable *joined_table = NULL;
  if (OB_ISNULL(table) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret));
  } else if (table->is_basic_table()
             && OB_FAIL(compute_basic_table_property(stmt, check_helper,
                                                     table, cond_exprs, res_info))) {
    LOG_WARN("failed to compute basic table property", K(ret));
  } else if ((table->is_generated_table() || table->is_temp_table())
             && OB_FAIL(SMART_CALL(compute_generate_table_property(stmt, check_helper, table,
                                                                   cond_exprs, res_info)))) {
    LOG_WARN("failed to compute generate table property", K(ret));
  } else if (!table->is_joined_table()) {
    ObSqlBitSet<> rel_ids;
    if (OB_FAIL(stmt->get_table_rel_ids(*table, rel_ids))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(res_info.table_set_.add_members(rel_ids))) {
      LOG_WARN("failed to add members", K(ret));
    }
  } else if (FALSE_IT(joined_table = static_cast<const JoinedTable *>(table))) {
  } else if (joined_table->is_inner_join()) {
    ret = SMART_CALL(compute_inner_join_property(stmt, check_helper, joined_table,
                                                 cond_exprs, res_info));
  } else if (IS_OUTER_JOIN(joined_table->joined_type_)) {
    ret = SMART_CALL(compute_outer_join_property(stmt, check_helper, joined_table,
                                                 cond_exprs, res_info));
  } else { // 改写阶段joined table 不存在 CONNECT_BY_JOIN 等
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected joined type", K(ret), K(*table));
  }
  return ret;
}

int ObTransformUtils::compute_basic_table_property(const ObDMLStmt *stmt,
                                                   UniqueCheckHelper &check_helper,
                                                   const TableItem *table,
                                                   ObIArray<ObRawExpr*> &cond_exprs,
                                                   UniqueCheckInfo &res_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> cur_cond_exprs;
  ObSqlBitSet<> table_set;
  ObSqlSchemaGuard *schema_guard = NULL;
  uint64_t index_tids[OB_MAX_INDEX_PER_TABLE];
  int64_t index_count = OB_MAX_INDEX_PER_TABLE;
  if (OB_ISNULL(stmt) || OB_ISNULL(table) || OB_ISNULL(check_helper.alloc_)
      || OB_ISNULL(check_helper.fd_factory_)
      || OB_ISNULL(check_helper.schema_checker_)
      || OB_ISNULL(schema_guard = check_helper.schema_checker_->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!table->is_basic_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table", K(ret), K(*table));
  } else if (OB_FAIL(stmt->get_table_rel_ids(*table, table_set))
             || OB_FAIL(res_info.table_set_.add_members(table_set))) {
    LOG_WARN("failed to get table relids", K(ret));
  } else if (OB_FAIL(extract_table_exprs(*stmt, cond_exprs, *table, cur_cond_exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(cond_exprs, cur_cond_exprs))) {
    LOG_WARN("failed to remove cur cond exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(cur_cond_exprs, res_info.const_exprs_))) {
    LOG_WARN("failed to compute const exprs", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(check_helper.alloc_, cur_cond_exprs,
                                                        res_info.equal_sets_))) {
    LOG_WARN("failed to compute compute equal set", K(ret));
  } else if (OB_FAIL(schema_guard->get_can_read_index_array(table->ref_id_,
                                                            index_tids,
                                                            index_count,
                                                            false,
                                                            true  /*global index*/,
                                                            false /*domain index*/))) {
    LOG_WARN("failed to get can read index", K(ret), K(table->ref_id_));
  } else {
    for (int64_t i = -1; OB_SUCC(ret) && i < index_count; ++i) {
      const ObTableSchema *index_schema = NULL;
      uint64_t index_id = (i == -1 ? table->ref_id_ : index_tids[i]);
      if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null index schema", K(ret));
      } else if (-1 != i && !index_schema->is_unique_index()) {
        // do nothing
      } else if (OB_FAIL(ObOptimizerUtil::try_add_fd_item(stmt, *check_helper.fd_factory_,
                                                          table->table_id_, res_info.table_set_,
                                                          index_schema, cur_cond_exprs,
                                                          res_info.not_null_, res_info.fd_sets_,
                                                          res_info.candi_fd_sets_))) {
        LOG_WARN("failed to try add fd item", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (stmt->is_select_stmt() && OB_FAIL(try_add_table_fd_for_rowid(
                                              static_cast<const ObSelectStmt*>(stmt),
                                              *check_helper.fd_factory_,
                                              res_info.fd_sets_,
                                              table_set))) {
    LOG_WARN("fail to add table fd for rowid expr", K(ret));
  }
  return ret;
}

int ObTransformUtils::need_compute_fd_item_set(ObIArray<ObRawExpr*> &exprs)
{
  bool need = true;
  if (exprs.count() > 128) {
    need = false;
  }
  return need;
}
//extract rowid in select_exprs
//add table fd for related table.
int ObTransformUtils::try_add_table_fd_for_rowid(const ObSelectStmt *stmt,
                                                 ObFdItemFactory &fd_factory,
                                                 ObIArray<ObFdItem *> &fd_item_set,
                                                 const ObSqlBitSet<> &tables)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 1> rowid_exprs;
    ObSEArray<ObRawExpr*, 1> select_exprs;
    if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("fail to get select exprs", K(ret));
    } else if (OB_FAIL(extract_rowid_exprs(select_exprs, rowid_exprs))) {
      LOG_WARN("fail to extract rowid exprs", K(ret));
    } else if (rowid_exprs.count() > 0) {
      ObRelIds rowid_table_set;
      ObRelIds target_table_set;
      ObSEArray<ObRawExpr*, 1> target_exprs;
      for (int64_t i = 0; OB_SUCC(ret) && i < rowid_exprs.count(); i++) {
        if (OB_ISNULL(rowid_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null pointer", K(ret));
        } else {
          ObTableFdItem *select_unique_fd = NULL;
          if (OB_FAIL(target_table_set.intersect(tables, rowid_exprs.at(i)->get_relation_ids()))) {
            LOG_WARN("fail to calc intersect", K(ret));
          } else if (target_table_set.num_members() <= 0) {
          } else if (OB_FAIL(target_exprs.push_back(rowid_exprs.at(i)))) {
            LOG_WARN("fail to push back target expr", K(ret));
          } else if (OB_FAIL(fd_factory.create_table_fd_item(select_unique_fd,
                                                             true,
                                                             target_exprs,
                                                             target_table_set))) {
            LOG_WARN("fail to create table fd", K(ret));
          } else if (OB_FAIL(fd_item_set.push_back(select_unique_fd))) {
            LOG_WARN("fail to push back table fd", K(ret));
          } else {
            target_table_set.clear_all();
            target_exprs.reset();
         }
        }
      }
    }
  }
  return ret;
}

// generate table 不返回 candi_fd_item_set
int ObTransformUtils::compute_generate_table_property(const ObDMLStmt *stmt,
                                                      UniqueCheckHelper &check_helper,
                                                      const TableItem *table,
                                                      ObIArray<ObRawExpr*> &cond_exprs,
                                                      UniqueCheckInfo &res_info)
{
  int ret = OB_SUCCESS;
  UniqueCheckInfo child_info;
  EqualSets tmp_equal_set;
  ObSelectStmt *ref_query = NULL;
  ObSEArray<ObRawExpr*, 4> cur_cond_exprs;
  ObSqlBitSet<> table_set;
  ObSEArray<ObRawExpr*, 1> dummy_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(table) || OB_ISNULL(check_helper.alloc_)
      || OB_ISNULL(check_helper.fd_factory_) || OB_ISNULL(check_helper.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret));
  } else if ((!table->is_generated_table() && !table->is_temp_table()) ||
              OB_ISNULL(ref_query = table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table", K(ret), K(*table));
  } else if (OB_FAIL(SMART_CALL(compute_stmt_property(ref_query, check_helper, child_info)))) {
    LOG_WARN("failed to compute stmt property", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(*table, table_set))
             || OB_FAIL(res_info.table_set_.add_members(table_set))) {
    LOG_WARN("failed to get table relids", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(*check_helper.expr_factory_,
                                                  child_info.equal_sets_, table->table_id_,
                                                  *stmt, *ref_query, true, child_info.const_exprs_,
                                                  res_info.const_exprs_))) {
    LOG_WARN("failed to convert subplan scan expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subplan_const_column(*stmt,
                                                               table->table_id_,
                                                               *ref_query,
                                                               dummy_exprs,
                                                               res_info.const_exprs_))) {
    LOG_WARN("failed to get subplan const column", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_expr(*check_helper.expr_factory_,
                                                  child_info.equal_sets_, table->table_id_,
                                                  *stmt, *ref_query, true, child_info.not_null_,
                                                  res_info.not_null_))) {
    LOG_WARN("failed to convert subplan scan expr", K(ret));
  } else if (OB_FAIL(extract_table_exprs(*stmt, cond_exprs, *table, cur_cond_exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(cond_exprs, cur_cond_exprs))) {
    LOG_WARN("failed to remove cur cond exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(cur_cond_exprs, res_info.const_exprs_))) {
    LOG_WARN("failed to compute const exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_equal_sets(check_helper.alloc_,
                                                        *check_helper.expr_factory_,
                                                        table->table_id_, *stmt, *ref_query,
                                                        child_info.equal_sets_, tmp_equal_set))) {
    LOG_WARN("failed to convert subplan scan equal sets", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(check_helper.alloc_, cur_cond_exprs,
                                                        tmp_equal_set, res_info.equal_sets_))) {
    LOG_WARN("failed to compute equal set", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_fd_item_sets(*check_helper.fd_factory_,
                                                        *check_helper.expr_factory_,
                                                        child_info.equal_sets_,
                                                        child_info.const_exprs_,
                                                        table->table_id_,
                                                        *stmt, *ref_query,
                                                        child_info.fd_sets_,
                                                        res_info.fd_sets_))) {
    LOG_WARN("failed to convert subplan scan fd item sets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::convert_subplan_scan_fd_item_sets(*check_helper.fd_factory_,
                                                        *check_helper.expr_factory_,
                                                        child_info.equal_sets_,
                                                        child_info.const_exprs_,
                                                        table->table_id_,
                                                        *stmt, *ref_query,
                                                        child_info.candi_fd_sets_,
                                                        res_info.candi_fd_sets_))) {
    LOG_WARN("failed to convert subplan scan fd item sets", K(ret));
  } else if (!cur_cond_exprs.empty() &&
             OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(cur_cond_exprs, res_info.candi_fd_sets_,
                                                          res_info.fd_sets_,
                                                          res_info.not_null_))) {
    LOG_WARN("failed to enhance right fd item set", K(ret));
  }
  return ret;
}

int ObTransformUtils::compute_inner_join_property(const ObDMLStmt *stmt,
                                                  UniqueCheckHelper &check_helper,
                                                  const JoinedTable *table,
                                                  ObIArray<ObRawExpr*> &cond_exprs,
                                                  UniqueCheckInfo &res_info)
{
  int ret = OB_SUCCESS;
  const TableItem *left_table = NULL;
  const TableItem *right_table = NULL;
  UniqueCheckInfo left_info;
  UniqueCheckInfo right_info;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!table->is_inner_join() || OB_ISNULL(left_table = table->left_table_)
             || OB_ISNULL(right_table = table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected inner join table", K(ret), K(*table));
  } else if (OB_FAIL(compute_table_property(stmt, check_helper, left_table,
                                            cond_exprs, left_info))) {
    LOG_WARN("failed to compute inner join left table property", K(ret));
  } else if (OB_FAIL(compute_table_property(stmt, check_helper, right_table,
                                            cond_exprs, right_info))) {
    LOG_WARN("failed to compute inner join right table property", K(ret));
  } else if (OB_FAIL(compute_inner_join_property(stmt, check_helper, left_info, right_info,
                                                 table->get_join_conditions(), cond_exprs,
                                                 res_info))) {
    LOG_WARN("failed to compute inner join table property", K(ret));
  }
  return ret;
}

int ObTransformUtils::compute_inner_join_property(const ObDMLStmt *stmt,
                                                  UniqueCheckHelper &check_helper,
                                                  UniqueCheckInfo &left_info,
                                                  UniqueCheckInfo &right_info,
                                                  const ObIArray<ObRawExpr*> &inner_join_cond_exprs,
                                                  ObIArray<ObRawExpr*> &cond_exprs,
                                                  UniqueCheckInfo &res_info)
{
  ObSEArray<ObRawExpr*, 4> left_join_exprs;
  ObSEArray<ObRawExpr*, 4> right_join_exprs;
  ObSEArray<ObRawExpr*, 4> all_left_join_exprs;
  ObSEArray<ObRawExpr*, 4> all_right_join_exprs;
  ObSEArray<ObRawExpr*, 4> cur_cond_exprs;
  EqualSets tmp_equal_sets;
  ObSqlBitSet<> table_set;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(check_helper.alloc_) || OB_ISNULL(check_helper.fd_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(table_set.add_members(left_info.table_set_))
             || OB_FAIL(table_set.add_members(right_info.table_set_))
             || OB_FAIL(res_info.table_set_.add_members(table_set))) {
    LOG_WARN("failed to get table relids", K(ret));
  } else if (OB_FAIL(extract_table_exprs(*stmt, cond_exprs, table_set, cur_cond_exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(cond_exprs, cur_cond_exprs))) {
    LOG_WARN("failed to remove cur cond exprs", K(ret));
  } else if (OB_FAIL(append(cur_cond_exprs, inner_join_cond_exprs))) {
    LOG_WARN("failed to append cur cond exprs", K(ret));
  } else if (OB_FAIL(append(res_info.const_exprs_, left_info.const_exprs_))
              || OB_FAIL(append(res_info.const_exprs_, right_info.const_exprs_))) {
    LOG_WARN("failed to append const exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(cur_cond_exprs, res_info.const_exprs_))) {
    LOG_WARN("failed to compute const exprs", K(ret));
  } else if (OB_FAIL(append(tmp_equal_sets, left_info.equal_sets_))
              || OB_FAIL(append(tmp_equal_sets, right_info.equal_sets_))) {
    LOG_WARN("failed to append const exprs", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(check_helper.alloc_, cur_cond_exprs,
                                                        tmp_equal_sets, res_info.equal_sets_))) {
    LOG_WARN("failed to compute compute equal set", K(ret));
  } else if (!cur_cond_exprs.empty() &&
             OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(cur_cond_exprs, left_info.candi_fd_sets_,
                                                          left_info.fd_sets_,
                                                          left_info.not_null_))) {
    LOG_WARN("failed to enhance left fd item set", K(ret));
  } else if (!cur_cond_exprs.empty() &&
             OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(cur_cond_exprs, right_info.candi_fd_sets_,
                                                          right_info.fd_sets_,
                                                          right_info.not_null_))) {
    LOG_WARN("failed to enhance right fd item set", K(ret));
  } else if (OB_FAIL(append(res_info.not_null_, left_info.not_null_)) ||
             OB_FAIL(append(res_info.not_null_, right_info.not_null_))) {
    LOG_WARN("failed to append not null columns", K(ret));
  } else if (!cur_cond_exprs.empty() &&
             OB_FAIL(ObOptimizerUtil::get_type_safe_join_exprs(cur_cond_exprs,
                                                               left_info.table_set_,
                                                               right_info.table_set_,
                                                               left_join_exprs, right_join_exprs,
                                                               all_left_join_exprs,
                                                               all_right_join_exprs))) {
    LOG_WARN("failed to get type safe join exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::add_fd_item_set_for_left_join(*check_helper.fd_factory_,
                                                  right_info.table_set_, right_join_exprs,
                                                  right_info.const_exprs_, right_info.equal_sets_,
                                                  right_info.fd_sets_, all_left_join_exprs,
                                                  left_info.equal_sets_, left_info.fd_sets_,
                                                  left_info.candi_fd_sets_,
                                                  res_info.fd_sets_, res_info.candi_fd_sets_))) {
    LOG_WARN("failed to add left fd_item_set for inner join", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::add_fd_item_set_for_left_join(*check_helper.fd_factory_,
                                                  left_info.table_set_, left_join_exprs,
                                                  left_info.const_exprs_, left_info.equal_sets_,
                                                  left_info.fd_sets_, all_right_join_exprs,
                                                  right_info.equal_sets_, right_info.fd_sets_,
                                                  right_info.candi_fd_sets_,
                                                  res_info.fd_sets_, res_info.candi_fd_sets_))) {
    LOG_WARN("failed to add right fd_item_set for inner join", K(ret));
  }
  return ret;
}

int ObTransformUtils::compute_outer_join_property(const ObDMLStmt *stmt,
                                                  UniqueCheckHelper &check_helper,
                                                  const JoinedTable *table,
                                                  ObIArray<ObRawExpr*> &cond_exprs,
                                                  UniqueCheckInfo &res_info)
{
  int ret = OB_SUCCESS;
  const TableItem *left_table = NULL;
  UniqueCheckInfo left_info;
  const TableItem *right_table = NULL;
  UniqueCheckInfo right_info;
  ObSEArray<ObRawExpr*, 1> dummy_conds;
  if (OB_ISNULL(stmt) || OB_ISNULL(table) || OB_ISNULL(check_helper.alloc_)
      || OB_ISNULL(check_helper.fd_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!table->is_full_join() && !table->is_left_join() && !table->is_right_join()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected joined table", K(ret), K(*table));
  } else if (table->is_full_join()) {
    /*do nothing*/
  } else if (table->is_left_join() && (OB_ISNULL(left_table = table->left_table_)
             || OB_ISNULL(right_table = table->right_table_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected inner join table", K(ret), K(*table));
  } else if (table->is_right_join() && (OB_ISNULL(left_table = table->right_table_)
             || OB_ISNULL(right_table = table->left_table_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected inner join table", K(ret), K(*table));
  } else if (OB_FAIL(compute_table_property(stmt, check_helper, left_table,
                                            dummy_conds, left_info))) {
    LOG_WARN("failed to compute inner join left table property", K(ret));
  } else if (OB_FAIL(compute_table_property(stmt, check_helper, right_table,
                                            dummy_conds, right_info))) {
    LOG_WARN("failed to compute inner join right table property", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 4> left_join_exprs;
    ObSEArray<ObRawExpr*, 4> right_join_exprs;
    ObSEArray<ObRawExpr*, 4> all_left_join_exprs;
    ObSEArray<ObRawExpr*, 4> all_right_join_exprs;
    ObSEArray<ObRawExpr*, 4> cur_cond_exprs;
    EqualSets tmp_equal_sets;
    ObSqlBitSet<> table_set;
    if (OB_FAIL(table_set.add_members(left_info.table_set_))
             || OB_FAIL(table_set.add_members(right_info.table_set_))
             || OB_FAIL(res_info.table_set_.add_members(table_set))) {
      LOG_WARN("failed to get table relids", K(ret));
    } else if (OB_FAIL(extract_table_exprs(*stmt, cond_exprs, table_set, cur_cond_exprs))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(cond_exprs, cur_cond_exprs))) {
      LOG_WARN("failed to remove cur cond exprs", K(ret));
    } else if (OB_FAIL(append(res_info.const_exprs_, left_info.const_exprs_))) {
      LOG_WARN("failed to append const exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(cur_cond_exprs,
                                                            res_info.const_exprs_))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    } else if (OB_FAIL(append(tmp_equal_sets, left_info.equal_sets_))) {
      LOG_WARN("failed to append const exprs", K(ret));
    } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(check_helper.alloc_, cur_cond_exprs,
                                                          tmp_equal_sets, res_info.equal_sets_))) {
      LOG_WARN("failed to compute compute equal set", K(ret));
    } else if (!cur_cond_exprs.empty() &&
               OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(cur_cond_exprs, left_info.candi_fd_sets_,
                                                        left_info.fd_sets_, left_info.not_null_))) {
      LOG_WARN("failed to enhance left fd item set", K(ret));
    } else if (!cur_cond_exprs.empty() &&
               OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(cur_cond_exprs, right_info.candi_fd_sets_,
                                                    right_info.fd_sets_, right_info.not_null_))) {
      LOG_WARN("failed to enhance right fd item set", K(ret));
    } else if (!table->get_join_conditions().empty() &&
               OB_FAIL(ObOptimizerUtil::enhance_fd_item_set(table->get_join_conditions(),
                                                    right_info.candi_fd_sets_, right_info.fd_sets_,
                                                    right_info.not_null_))) {
      LOG_WARN("failed to enhance right fd item set", K(ret));
    } else if (OB_FAIL(append(res_info.not_null_, left_info.not_null_))) {
      LOG_WARN("failed to append not null columns", K(ret));
    } else if (!table->get_join_conditions().empty() &&
               OB_FAIL(ObOptimizerUtil::get_type_safe_join_exprs(table->get_join_conditions(),
                                                    left_info.table_set_, right_info.table_set_,
                                                    left_join_exprs, right_join_exprs,
                                                    all_left_join_exprs, all_right_join_exprs))) {
      LOG_WARN("failed to get type safe join exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::add_fd_item_set_for_left_join(*check_helper.fd_factory_,
                                                  right_info.table_set_, right_join_exprs,
                                                  right_info.const_exprs_, right_info.equal_sets_,
                                                  right_info.fd_sets_, all_left_join_exprs,
                                                  left_info.equal_sets_, left_info.fd_sets_,
                                                  left_info.candi_fd_sets_,
                                                  res_info.fd_sets_, res_info.candi_fd_sets_))) {
      LOG_WARN("failed to add left fd_item_set for inner join", K(ret));
    }
  }
  return ret;
}

// 获取source_exprs 中所有只包含了 target 的列的expr
int ObTransformUtils::extract_table_exprs(const ObDMLStmt &stmt,
                                          const ObIArray<ObRawExpr *> &source_exprs,
                                          const TableItem &target,
                                          ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_set;
  if (OB_FAIL(stmt.get_table_rel_ids(target, table_set))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else if (OB_FAIL(extract_table_exprs(stmt, source_exprs, table_set, exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  }
  return ret;
}

int ObTransformUtils::extract_table_exprs(const ObDMLStmt &stmt,
                                          const ObIArray<ObRawExpr *> &source_exprs,
                                          const ObIArray<TableItem*> &tables,
                                          ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_set;
  if (OB_FAIL(stmt.get_table_rel_ids(tables, table_set))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else if (OB_FAIL(extract_table_exprs(stmt, source_exprs, table_set, exprs))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  }
  return ret;
}

int ObTransformUtils::extract_table_exprs(const ObDMLStmt &stmt,
                                          const ObIArray<ObRawExpr *> &source_exprs,
                                          const ObSqlBitSet<> &table_set,
                                          ObIArray<ObRawExpr *> &table_exprs)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt);
  for (int64_t i = 0; OB_SUCC(ret) && i < source_exprs.count(); ++i) {
    ObRawExpr *expr = source_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("source expr shoud not be null", K(ret), K(source_exprs), K(i));
    } else if (expr->get_relation_ids().is_empty()) {
      // do nothing
    } else if (!table_set.is_superset2(expr->get_relation_ids())) {
      /* do nothing */
    } else if (OB_FAIL(add_var_to_array_no_dup(table_exprs, expr))) {
      LOG_WARN("failed to push back column expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_table_joined_exprs(const ObDMLStmt &stmt,
                                             const TableItem &source,
                                             const TableItem &target,
                                             const ObIArray<ObRawExpr *> &conditions,
                                             ObIArray<ObRawExpr *> &target_exprs,
                                             ObSqlBitSet<> &join_source_ids,
                                             ObSqlBitSet<> &join_target_ids)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> source_table_ids;
  ObSqlBitSet<> target_table_ids;
  if (OB_FAIL(stmt.get_table_rel_ids(source, source_table_ids))) {
    LOG_WARN("failed to get source table rel ids", K(ret));
  } else if (OB_FAIL(stmt.get_table_rel_ids(target, target_table_ids))) {
    LOG_WARN("failed to get target table rel ids", K(ret));
  } else if (OB_FAIL(get_table_joined_exprs(source_table_ids,
                                            target_table_ids,
                                            conditions,
                                            target_exprs,
                                            join_source_ids,
                                            join_target_ids))) {
    LOG_WARN("failed to get table joined exprs by table rel ids", K(ret));
  }
  return ret;
}

int ObTransformUtils::get_table_joined_exprs(const ObDMLStmt &stmt,
                                             const ObIArray<TableItem *> &sources,
                                             const TableItem &target,
                                             const ObIArray<ObRawExpr *> &conditions,
                                             ObIArray<ObRawExpr *> &target_exprs)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> source_table_ids;
  ObSqlBitSet<> target_table_ids;
  ObSqlBitSet<> join_source_ids;
  ObSqlBitSet<> join_target_ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < sources.count(); ++i) {
    const TableItem *table = sources.at(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(stmt.get_table_rel_ids(*table, source_table_ids))) {
      LOG_WARN("failed to get source table rel ids", K(ret));
    } else {/*do nothing*/}
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(stmt.get_table_rel_ids(target, target_table_ids))) {
    LOG_WARN("failed to get target table rel ids", K(ret));
  } else if (OB_FAIL(get_table_joined_exprs(source_table_ids,
                                            target_table_ids,
                                            conditions,
                                            target_exprs,
                                            join_source_ids,
                                            join_target_ids))) {
    LOG_WARN("failed to get table joined exprs by table rel ids", K(ret));
  } else {/*do nothing*/}
  return ret;
}

// 从所有conditions中，找出等值连接condition，从中获取与target table相关的expr
// 要求condition的形式为 source related expr = target related expr,
// 当target table为t2时
// 连接条件可以为 t1.a = t2.b 和 t1.a = t2.b + 1 或 t1.a = t2.a + t2.b
//        不能为 t2.a = t2.b  或 t2.a = 1
int ObTransformUtils::get_table_joined_exprs(const ObSqlBitSet<> &source_ids,
                                             const ObSqlBitSet<> &target_ids,
                                             const ObIArray<ObRawExpr *> &conditions,
                                             ObIArray<ObRawExpr *> &target_exprs,
                                             ObSqlBitSet<> &join_source_ids,
                                             ObSqlBitSet<> &join_target_ids)
{
  int ret = OB_SUCCESS;
  for(int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
    ObRawExpr *condition = conditions.at(i);
    bool is_valid = false;
    if (OB_ISNULL(condition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition should not be null", K(ret));
    } else if (T_OP_EQ == condition->get_expr_type()) {
      ObOpRawExpr *op = static_cast<ObOpRawExpr *>(condition);
      ObRawExpr *child1 = op->get_param_expr(0);
      ObRawExpr *child2 = op->get_param_expr(1);
      if (OB_ISNULL(child1) || OB_ISNULL(child2)) {
        LOG_WARN("parameter of EQ expr should not be null", K(ret), K(child1), K(child2));
      } else if (!child1->has_flag(CNT_COLUMN) || !child2->has_flag(CNT_COLUMN)){
        /* do nothing */
      } else if (source_ids.is_superset2(child1->get_relation_ids())
                  && target_ids.is_superset2(child2->get_relation_ids())) {
        // child2 contain target exprs
        if (OB_FAIL(ObRelationalExprOperator::is_equivalent(child1->get_result_type(),
                                                            child2->get_result_type(),
                                                            child2->get_result_type(),
                                                            is_valid))) {
          LOG_WARN("failed to check expr is equivalent", K(ret));
        } else if (!is_valid) {
          LOG_TRACE("can not use child1 expr type as the (child1, child2) compare type");
        } else if (OB_FAIL(target_exprs.push_back(child2))) {
          LOG_WARN("failed to push target expr", K(ret));
        } else if (OB_FAIL(join_source_ids.add_members2(child1->get_relation_ids()))) {
          LOG_WARN("failed to add member to join source ids.", K(ret));
        } else if (OB_FAIL(join_target_ids.add_members2(child2->get_relation_ids()))) {
          LOG_WARN("failed to add member to join target ids.", K(ret));
        }
      } else if (target_ids.is_superset2(child1->get_relation_ids())
                  && source_ids.is_superset2(child2->get_relation_ids())) {
        // child1 contain target column
        if (OB_FAIL(ObRelationalExprOperator::is_equivalent(child2->get_result_type(),
                                                            child1->get_result_type(),
                                                            child1->get_result_type(),
                                                            is_valid))) {
          LOG_WARN("failed to check expr is equivalent", K(ret));
        } else if (!is_valid) {
          LOG_TRACE("can not use child2 expr type as the (child2, child1) compare type");
        } else if (OB_FAIL(target_exprs.push_back(child1))) {
          LOG_WARN("failed to push target expr", K(ret));
        } else if (OB_FAIL(join_source_ids.add_members2(child2->get_relation_ids()))) {
          LOG_WARN("failed to add member to join source ids.", K(ret));
        } else if (OB_FAIL(join_target_ids.add_members2(child1->get_relation_ids()))) {
          LOG_WARN("failed to add member to join target ids.", K(ret));
        }
      } else if (target_ids.is_superset2(child1->get_relation_ids())
                  && target_ids.is_superset2(child2->get_relation_ids())) {
        // child1 contain target column and child2 contain target column too.
        if (OB_FAIL(ObRelationalExprOperator::is_equivalent(child1->get_result_type(),
                                                            child2->get_result_type(),
                                                            child2->get_result_type(),
                                                            is_valid))) {
          LOG_WARN("failed to check expr is equivalent", K(ret));
        } else if (!is_valid) {
          LOG_TRACE("can not use child2 expr type as the (child2, child1) compare type");
        } else if (OB_FAIL(join_target_ids.add_members2(child2->get_relation_ids()))) {
          LOG_WARN("failed to add member to join target ids.", K(ret));
        } else if (OB_FAIL(join_target_ids.add_members2(child1->get_relation_ids()))) {
          LOG_WARN("failed to add member to join target ids.", K(ret));
        }
      } else if (source_ids.is_superset2(child1->get_relation_ids())
                  && source_ids.is_superset2(child2->get_relation_ids())) {
        // child1 contain source column and child2 contain source column too.
        if (OB_FAIL(ObRelationalExprOperator::is_equivalent(child2->get_result_type(),
                                                            child1->get_result_type(),
                                                            child1->get_result_type(),
                                                            is_valid))) {
          LOG_WARN("failed to check expr is equivalent", K(ret));
        } else if (!is_valid) {
          LOG_TRACE("can not use child2 expr type as the (child2, child1) compare type");
        } else if (OB_FAIL(join_source_ids.add_members2(child2->get_relation_ids()))) {
          LOG_WARN("failed to add member to join source ids.", K(ret));
        } else if (OB_FAIL(join_source_ids.add_members2(child1->get_relation_ids()))) {
          LOG_WARN("failed to add member to join source ids.", K(ret));
        }
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTransformUtils::get_from_item(ObDMLStmt *stmt, TableItem *table, FromItem &from)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or table is null", K(ret), K(stmt), K(table));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < stmt->get_from_item_size(); ++i) {
    from = stmt->get_from_item(i);
    if (from.table_id_ == table->table_id_) {
      found = true;
    } else if (!from.is_joined_) {
      // do nothing
    } else if (OB_FAIL(ObOptimizerUtil::find_table_item(
                               stmt->get_joined_table(from.table_id_), table->table_id_, found))) {
      LOG_WARN("failed to find table item", K(ret));
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find from item", K(*table));
  }
  return ret;
}

int ObTransformUtils::is_equal_correlation(ObQueryRefRawExpr &query_ref,
                                           ObRawExpr *cond,
                                           bool &is_valid,
                                           ObRawExpr **outer_param,
                                           ObRawExpr **inner_param)
{
  int ret = OB_SUCCESS;
  ObRawExpr *left = NULL;
  ObRawExpr *right = NULL;
  bool left_is_correlated = false;
  bool right_is_correlated = false;
  is_valid = false;
  if (OB_ISNULL(cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("condition expr is null", K(ret), K(cond));
  } else if (cond->get_expr_type() != T_OP_EQ) {
  } else if (OB_ISNULL(left = cond->get_param_expr(0)) ||
             OB_ISNULL(right = cond->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("equal operator is invalid", K(ret), K(left), K(right));
  } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(query_ref.get_exec_params(),
                                                          left,
                                                          left_is_correlated))) {
    LOG_WARN("failed to check is correlated expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(query_ref.get_exec_params(),
                                                          right,
                                                          right_is_correlated))) {
    LOG_WARN("failed to check is correlated expr", K(ret));
  } else {
    if (left_is_correlated == right_is_correlated) {
      // both correlated or both not
      LOG_TRACE("both condition are [not] correlated",
                K(is_valid), K(left_is_correlated), K(right_is_correlated));
    } else if ((left_is_correlated && left->has_generalized_column()) ||
               (right_is_correlated && right->has_generalized_column())) {
      // 同时引用了本层列和上层列
      LOG_TRACE("expr used both inner and outer block columns", K(is_valid));
    } else {
      is_valid = true;
      // left is the expr from the outer stmt
      if (right_is_correlated) {
        ObRawExpr *tmp = left;
        left = right;
        right = tmp;
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      if (OB_FAIL(ObRelationalExprOperator::is_equivalent(
                    left->get_result_type(), right->get_result_type(),
                    right->get_result_type(), is_valid))) {
        LOG_WARN("failed to check is expr result equivalent", K(ret));
      } else if (!is_valid) {
        // left = right <=> group by right (right = right)
        // equal meta should be same for (left, right) comparison and (right, right) comparision
        // be careful about two cases
        // 1. varchar = varchar (but collation is not the same)
        // 2. varchar = int (cast is required)
        LOG_TRACE("cast is required to transform equal as group/partition by", K(is_valid));
      } else {
        if (OB_NOT_NULL(outer_param)) {
          *outer_param = left;
        }
        if (OB_NOT_NULL(inner_param)) {
          *inner_param = right;
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::is_semi_join_right_table(const ObDMLStmt &stmt,
                                               const uint64_t table_id,
                                               bool &is_semi_table)
{
  int ret = OB_SUCCESS;
  const ObIArray<SemiInfo*> &semi_infos = stmt.get_semi_infos();
  is_semi_table = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_semi_table && i < semi_infos.count(); ++i) {
    if (OB_ISNULL(semi_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semi info is null", K(ret));
    } else if (table_id == semi_infos.at(i)->right_table_id_) {
      is_semi_table = true;
    }
  }
  return ret;
}

int ObTransformUtils::merge_table_items(ObDMLStmt *stmt,
                                        const TableItem *source_table,
                                        const TableItem *target_table,
                                        const ObIArray<int64_t> *output_map,
                                        ObIArray<ObRawExpr *> *pushed_col_exprs,
                                        ObIArray<ObRawExpr *> *merged_col_exprs,
                                        ObIArray<ObRawExpr *> *pushed_pseudo_col_exprs,
                                        ObIArray<ObRawExpr *> *merged_pseudo_col_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> from_col_exprs;
  ObSEArray<ObRawExpr *, 16> to_col_exprs;
  ObSEArray<ObRawExpr *, 16> from_pseudo_col_exprs;
  ObSEArray<ObRawExpr *, 16> to_pseudo_col_exprs;
  ObSEArray<ColumnItem, 16> target_column_items;
  ObStmtExprReplacer replacer;
  if (OB_ISNULL(stmt) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(ret), K(stmt), K(source_table),
        K(target_table));
  } else if (OB_FAIL(stmt->get_column_items(target_table->table_id_,
                                            target_column_items))) {
    LOG_WARN("failed to get column items", K(ret));
  } else {
    uint64_t source_table_id = source_table->table_id_;
    uint64_t target_table_id = target_table->table_id_;

    ObArray<ObRawExpr *> target_pseudo_like_column_exprs;
    ObArray<ObRawExpr *> source_pseudo_like_column_exprs;
    if (OB_FAIL(stmt->get_table_pseudo_column_like_exprs(target_table_id, target_pseudo_like_column_exprs))) {
      LOG_WARN("get table pseudo column expr failed", K(ret));
    } else if (OB_FAIL(stmt->get_table_pseudo_column_like_exprs(source_table_id, source_pseudo_like_column_exprs))) {
      LOG_WARN("get table pseudo column expr failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < target_pseudo_like_column_exprs.count(); i++) {
      if (OB_ISNULL(target_pseudo_like_column_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pseudo col expr is null", K(ret));
      } else if (OB_UNLIKELY(!target_pseudo_like_column_exprs.at(i)->is_pseudo_column_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pseudo col expr is not pseudo column expr type", K(ret));
      } else {
        bool found = false;
        ObRawExpr *merged_pseudo_col_expr = NULL;
        for (int64_t j = 0; OB_SUCC(ret) && !found && j < source_pseudo_like_column_exprs.count(); j++) {
          if (OB_ISNULL(source_pseudo_like_column_exprs.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("stmt pseudo like column expr is null", K(ret));
          } else if (source_pseudo_like_column_exprs.at(j)->get_expr_type() ==
                                                            target_pseudo_like_column_exprs.at(i)->get_expr_type()) {
            found = true;
            if (OB_FAIL(from_pseudo_col_exprs.push_back(target_pseudo_like_column_exprs.at(i)))
                    || OB_FAIL(to_pseudo_col_exprs.push_back(source_pseudo_like_column_exprs.at(j)))) {
              LOG_WARN("push expr into array failed", K(ret));
            } else {
              merged_pseudo_col_expr = source_pseudo_like_column_exprs.at(j);
            }
          }
        }

        if (OB_SUCC(ret) && !found) {
          static_cast<ObPseudoColumnRawExpr *>(target_pseudo_like_column_exprs.at(i))->set_table_id(source_table_id);
          static_cast<ObPseudoColumnRawExpr *>(target_pseudo_like_column_exprs.at(i))->set_table_name(
                                                                                    source_table->get_table_name());
          merged_pseudo_col_expr = target_pseudo_like_column_exprs.at(i);
        }

        if (OB_FAIL(ret)) {
        } else if (pushed_pseudo_col_exprs == NULL || merged_pseudo_col_exprs == NULL) {
          //do nothing
        } else if (OB_FAIL(pushed_pseudo_col_exprs->push_back(target_pseudo_like_column_exprs.at(i)))
                  || OB_FAIL(merged_pseudo_col_exprs->push_back(merged_pseudo_col_expr))) {
          LOG_WARN("append failed", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(replacer.add_replace_exprs(from_pseudo_col_exprs, to_pseudo_col_exprs))) {
      LOG_WARN("failed to add replace exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_pseudo_column_like_exprs(), from_pseudo_col_exprs))) {
      LOG_WARN("remove item failed", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < target_column_items.count(); ++i) {
      ColumnItem *target_col = NULL;
      ColumnItem *source_col = NULL;
      ObRawExpr *merged_col_expr = NULL;
      uint64_t column_id = OB_INVALID_ID;
      if (OB_ISNULL(target_col = stmt->get_column_item_by_id(target_table_id,
                                                             target_column_items.at(i).column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (NULL == output_map) {
        column_id = target_col->column_id_;
        source_col = stmt->get_column_item_by_id(source_table_id,
                                                 column_id);
      } else {
        // generated table with output map
        int64_t output_id = target_col->column_id_ - OB_APP_MIN_COLUMN_ID;
        if (OB_UNLIKELY(output_id < 0 || output_id >= output_map->count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected array count", K(output_id),
              K(output_map->count()), K(ret));
        } else if (OB_UNLIKELY(OB_INVALID_ID == output_map->at(output_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid output idx", K(output_id), K(ret));
        } else {
          column_id = output_map->at(output_id) + OB_APP_MIN_COLUMN_ID;
          source_col = stmt->get_column_item_by_id(source_table_id, column_id);
        }
      }
      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (OB_ISNULL(source_col)) {
        //distinct column, to be saved
        target_col->table_id_ = source_table_id;
        target_col->column_id_ = column_id;
        target_col->get_expr()->set_ref_id(source_table_id, column_id);
        target_col->get_expr()->set_table_name(source_table->get_table_name());
        if (OB_FAIL(target_col->get_expr()->pull_relation_id())) {
          LOG_WARN("failed to pull relation id and levels");
        } else {
          merged_col_expr = target_col->get_expr();
        }
      } else {
        //duplicate column item, to be replaced and remove
        if (OB_FAIL(from_col_exprs.push_back(target_col->get_expr()))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(to_col_exprs.push_back(source_col->get_expr()))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(stmt->remove_column_item(target_table_id,
                                                    target_column_items.at(i).column_id_))) {
          LOG_WARN("failed to remove column item", K(ret));
        } else { 
          merged_col_expr = source_col->get_expr(); 
        }
      }

      if (OB_FAIL(ret) || pushed_col_exprs == NULL || merged_col_exprs == NULL) {
        //do nothing
      } else if (OB_FAIL(pushed_col_exprs->push_back(target_column_items.at(i).get_expr())) || 
                 OB_FAIL(merged_col_exprs->push_back(merged_col_expr))) {
        LOG_WARN("push expr into array failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && !from_col_exprs.empty()) {
      if (OB_FAIL(replacer.add_replace_exprs(from_col_exprs, to_col_exprs))) {
        LOG_WARN("failed to add replace exprs", K(ret));
      } else if (OB_FAIL(stmt->iterate_stmt_expr(replacer))) {
        LOG_WARN("failed to iterate stmt expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && stmt->is_delete_stmt()) {
      // for delete stmt, we should adjust index dml info
      if (OB_FAIL(static_cast<ObDeleteStmt*>(stmt)->remove_delete_table_info(target_table_id))) {
        LOG_WARN("failed to remove columns dml infos", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::merge_table_items(ObSelectStmt *source_stmt,
                                        ObSelectStmt *target_stmt,
                                        const TableItem *source_table,
                                        const TableItem *target_table,
                                        ObIArray<ObRawExpr*> &old_exprs,
                                        ObIArray<ObRawExpr*> &new_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 16> target_column_items;
  if (OB_ISNULL(source_stmt) || OB_ISNULL(target_stmt) ||
      OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(ret), K(source_stmt),
        K(target_stmt), K(source_table), K(target_table));
  } else if (OB_FAIL(target_stmt->get_column_items(target_table->table_id_,
                                                   target_column_items))) {
    LOG_WARN("failed to get column items", K(ret));
  } else {
    uint64_t source_table_id = source_table->table_id_;
    uint64_t target_table_id = target_table->table_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_column_items.count(); ++i) {
      uint64_t column_id = target_column_items.at(i).column_id_;
      ColumnItem *target_col = target_stmt->get_column_item_by_id(target_table_id,
                                                                  column_id);
      ColumnItem *source_col = source_stmt->get_column_item_by_id(source_table_id,
                                                                  column_id);
      if (OB_ISNULL(target_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_ISNULL(source_col)) {
        //distinct column, to be saved
        target_col->table_id_ = source_table_id;
        target_col->column_id_ = column_id;
        target_col->get_expr()->set_ref_id(source_table_id, column_id);
        target_col->get_expr()->set_table_name(source_table->get_table_name());
        if (OB_FAIL(target_col->get_expr()->pull_relation_id())) {
          LOG_WARN("failed to pull relation id and levels");
        } else if (OB_FAIL(source_stmt->add_column_item(*target_col))) {
          LOG_WARN("failed to add column item", K(ret));
        } else { /*do nothing*/ }
      } else {
        //duplicate column item, to be replaced and remove
        if (OB_FAIL(old_exprs.push_back(target_col->get_expr()))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(new_exprs.push_back(source_col->get_expr()))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObTransformUtils::find_parent_expr(ObDMLStmt *stmt,
                                       ObRawExpr *target,
                                       ObRawExpr *&root,
                                       ObRawExpr *&parent)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> relation_exprs;
  parent = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(target));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(parent) && i < relation_exprs.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_ISNULL(expr = relation_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    } else if (OB_FAIL(find_parent_expr(expr, target, parent))) {
      LOG_WARN("failed to find parent expr", K(ret));
    } else if (NULL != parent) {
      root = expr;
    }
  }
  return ret;
}

int ObTransformUtils::find_parent_expr(ObRawExpr *expr, ObRawExpr *target, ObRawExpr *&parent)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr) || OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(expr), K(target));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr == target) {
    parent = expr;
  } else if (target->is_query_ref_expr() && !expr->has_flag(CNT_SUB_QUERY)) {
    // do nothing
  } else if (target->is_column_ref_expr() && !expr->has_flag(CNT_COLUMN)) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(parent) && i < expr->get_param_count(); ++i) {
      ObRawExpr *param_expr = NULL;
      if (OB_ISNULL(param_expr = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (target == param_expr) {
        parent = expr;
      } else if (OB_FAIL(SMART_CALL(find_parent_expr(param_expr, target, parent)))) {
        LOG_WARN("failed to find parent expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::find_relation_expr(ObDMLStmt *stmt,
                                         ObIArray<ObRawExpr *> &targets,
                                         ObIArray<ObRawExprPointer> &roots)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExprPointer, 4> relation_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    ObRawExpr *expr = NULL;
    ObRawExpr *parent = NULL;
    if (OB_FAIL(relation_exprs.at(i).get(expr))) {
      LOG_WARN("failed to get expr", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && OB_ISNULL(parent) &&
         j < targets.count(); ++j) {
      if (OB_FAIL(find_parent_expr(expr, targets.at(j), parent))) {
        LOG_WARN("failed to find parent expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(parent)) {
      if (OB_FAIL(roots.push_back(relation_exprs.at(i)))) {
        LOG_WARN("failed to push back relation expr pointer", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::generate_unique_key(ObTransformerCtx *ctx,
                                          ObDMLStmt *stmt,
                                          TableItem *item,
                                          ObIArray<ObRawExpr *> &unique_keys)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<ColumnItem, 4> rowkey_cols;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->schema_checker_) ||
      OB_ISNULL(ctx->expr_factory_) || OB_ISNULL(ctx->session_info_) || OB_ISNULL(item) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx), K(item));
  } else if (OB_UNLIKELY(!item->is_basic_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is not expected basic table", K(*item));
  } else if (OB_FAIL(ctx->schema_checker_->get_table_schema(ctx->session_info_->get_effective_tenant_id(), item->ref_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(table_schema));
  //new heap table not add partition key in rowkey and the tablet id is unique in partition,
  //we need add partition key to ensure the output unique.
  } else if (table_schema->is_heap_table() &&
             OB_FAIL(add_part_column_exprs_for_heap_table(stmt, table_schema,
                                                          item->table_id_, unique_keys))) {
    LOG_WARN("failed to add part column exprs for heap table", K(ret));
  } else {
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
    ObColumnRefRawExpr *col_expr = NULL;
    uint64_t column_id = 0;
    for (int i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
        LOG_WARN("Failed to get column id", K(ret));
      } else if (OB_ISNULL(col_expr = stmt->get_column_expr_by_id(item->table_id_, column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column schema", K(column_id), K(ret));
      } else if (FALSE_IT(col_expr->set_explicited_reference())) {
      } else if (OB_FAIL(unique_keys.push_back(col_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTransformUtils::generate_unique_key(ObTransformerCtx *ctx,
                                          ObDMLStmt *stmt,
                                          ObSqlBitSet<> &ignore_tables,
                                          ObIArray<ObRawExpr *> &unique_keys)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> from_rel_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is null", K(stmt), K(ctx), K(ret));
  } else if (OB_FAIL(stmt->get_from_tables(from_rel_ids))) {
    LOG_WARN("failed to get output rel ids", K(ret));
  } else {
    ObIArray<TableItem *> &table_items = stmt->get_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      TableItem *table = table_items.at(i);
      int32_t idx = OB_INVALID_INDEX;
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(table), K(ret));
      } else if (OB_FALSE_IT(idx = stmt->get_table_bit_index(table->table_id_))) {
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table bit index", K(table->table_id_), K(idx), K(ret));
      } else if (!from_rel_ids.has_member(idx)) {
        // semi join 中的表, 不会输出
      } else if (ignore_tables.has_member(idx)) {
        //do nothing
      } else if (table->is_basic_table()) {
        if (OB_FAIL(generate_unique_key(ctx,
                                        stmt,
                                        table,
                                        unique_keys))) {
          LOG_WARN("failed to generate unique key", K(ret));
        }
      } else if (table->is_generated_table() || table->is_temp_table()) {
        ObSelectStmt *view_stmt = NULL;
        ObSEArray<ObRawExpr*, 4> stmt_unique_keys;
        ObSEArray<ObRawExpr*, 4> column_exprs;
        if (OB_ISNULL(view_stmt = table->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("view_stmt = stmt is null", K(view_stmt), K(ret));
        } else if (OB_FAIL(recursive_set_stmt_unique(view_stmt,
                                                      ctx,
                                                      false,
                                                      &stmt_unique_keys))) {
          LOG_WARN("recursive set stmt unique failed", K(ret));
        } else if (OB_FAIL(create_columns_for_view(ctx,
                                                  *table,
                                                  stmt,
                                                  column_exprs))) {
          //为view生成column exprs
          LOG_WARN("failed to create columns for view", K(ret));
        } else if (OB_FAIL(convert_select_expr_to_column_expr(stmt_unique_keys,
                                                              *view_stmt,
                                                              *stmt,
                                                              table->table_id_,
                                                              unique_keys))) {
          //找到view的unique keys对应的本层column expr
          LOG_WARN("failed to get stmt unique keys columns expr", K(ret));
        } else {
          //do nothing
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_loseless_join(ObDMLStmt *stmt,
                                          ObTransformerCtx *ctx,
                                          TableItem *source_table,
                                          TableItem *target_table,
                                          ObSQLSessionInfo *session_info,
                                          ObSchemaChecker *schema_checker,
                                          ObStmtMapInfo &stmt_map_info,
                                          bool is_on_null_side,
                                          bool &is_loseless,
                                          EqualSets *input_equal_sets) // default value NULL
{
  int ret = OB_SUCCESS;
  bool is_contain = false;
  bool source_unique = false;
  bool target_unique = false;
  ObSEArray<ObRawExpr*, 16> source_exprs;
  ObSEArray<ObRawExpr*, 16> target_exprs;
  ObSEArray<ObRawExpr *, 8> target_tab_cols;
  is_loseless = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(source_table) ||
      OB_ISNULL(target_table) || OB_ISNULL(schema_checker)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(source_table),
        K(target_table), K(schema_checker), K(ret));
  } else if (OB_FAIL(check_table_item_containment(stmt,
                                                  source_table,
                                                  stmt,
                                                  target_table,
                                                  stmt_map_info,
                                                  is_contain))) {
    LOG_WARN("failed to check table item containment", K(ret));
  } else if (!is_contain) {
    /*do nothing*/
  } else if (OB_FAIL(ObTransformUtils::extract_lossless_join_columns(stmt, ctx,
                                                                     source_table,
                                                                     target_table,
                                                                     stmt_map_info.select_item_map_,
                                                                     source_exprs,
                                                                     target_exprs,
                                                                     input_equal_sets))) {
    LOG_WARN("failed to extract lossless join columns", K(ret));
  } else if (OB_FAIL(stmt->get_column_exprs(target_table->table_id_, target_tab_cols))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (is_on_null_side && !target_tab_cols.empty() && target_exprs.empty()) {
    is_loseless = false;
  } else if (OB_FAIL(ObTransformUtils::check_exprs_unique(*stmt, source_table, source_exprs,
                                                session_info, schema_checker, source_unique))) {
    LOG_WARN("failed to check exprs unique", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_exprs_unique(*stmt, target_table, target_exprs,
                                                session_info, schema_checker, target_unique))) {
    LOG_WARN("failed to check exprs unique", K(ret));
  } else if (source_unique && target_unique) {
    is_loseless = true;
    LOG_TRACE("succeed to check lossless join", K(source_unique), K(target_unique),
        K(is_loseless));
  } else {
    is_loseless = false;
    LOG_TRACE("succeed to check lossless join", K(source_unique), K(target_unique),
        K(is_loseless));
  }
  return ret;
}

int ObTransformUtils::check_relations_containment(ObDMLStmt *stmt,
                                                  const common::ObIArray<TableItem*> &source_rels,
                                                  const common::ObIArray<TableItem*> &target_rels,
                                                  common::ObIArray<ObStmtMapInfo> &stmt_map_infos,
                                                  common::ObIArray<int64_t> &rel_map_info,
                                                  bool &is_contain)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> matched_rels;
  is_contain = false;
  if (OB_ISNULL(stmt)){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is null", K(ret));
  } else if (source_rels.count() != target_rels.count()){
    is_contain = false;
  } else {
    is_contain = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_contain && i < source_rels.count(); ++i){
      bool is_matched = false;
      TableItem *source_table = source_rels.at(i);
      ObStmtMapInfo stmt_map_info;
      if (OB_ISNULL(source_table)){
        LOG_WARN("can not find table item", K(source_rels.at(i)));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !is_matched && j < target_rels.count(); ++j){
          if (matched_rels.has_member(j)){
            /*do nothing*/
          } else {
            TableItem *target_table = target_rels.at(j);
            if (OB_ISNULL(target_table)){
              LOG_WARN("can not find table item", K(target_rels.at(j)));
            } else if (OB_FAIL(check_table_item_containment(stmt,
                                                            source_table,
                                                            stmt,
                                                            target_table,
                                                            stmt_map_info,
                                                            is_matched))){
              LOG_WARN("check table item containment failed", K(ret));
            } else if (is_matched &&
                      OB_SUCC(matched_rels.add_member(j)) &&
                      OB_FAIL(rel_map_info.push_back(j))){
              LOG_WARN("push back table id failed", K(ret));
            } else {
              /*do nothing*/
            }
          }
        }
        if (OB_SUCC(ret) && is_matched) {
          if (OB_FAIL(stmt_map_infos.push_back(stmt_map_info))) {
            LOG_WARN("push back stmt map info failed", K(ret));
          } else {
            is_contain = true;
          }
        } else {
          is_contain = false;
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_table_item_containment(ObDMLStmt *source_stmt,
                                                   const TableItem *source_table,
                                                   ObDMLStmt *target_stmt,
                                                   const TableItem *target_table,
                                                   ObStmtMapInfo &stmt_map_info,
                                                   bool &is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = false;
  if (OB_ISNULL(source_stmt) || OB_ISNULL(target_table)
      || OB_ISNULL(target_stmt) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(source_stmt), K(target_stmt), K(source_table),
        K(target_table), K(ret));
  } else if (source_table->is_temp_table() && target_table->is_temp_table()) {
    is_contain = source_table->ref_query_ == target_table->ref_query_;
  } else if (source_table->is_basic_table() && target_table->is_basic_table()) {
    QueryRelation relation = QueryRelation::QUERY_UNCOMPARABLE;
    //zhenling.zzg 修复存在partition hint的情况下，正确性bug
    if (OB_FAIL(ObStmtComparer::compare_basic_table_item(source_stmt,
                                                        source_table,
                                                        target_stmt,
                                                        target_table,
                                                        relation))) {
      LOG_WARN("compare table part failed",K(ret), K(source_table), K(target_table));
    } else if (QueryRelation::QUERY_LEFT_SUBSET == relation ||
               QueryRelation::QUERY_EQUAL == relation) {
      is_contain = true;
      LOG_TRACE("succeed to check table item containment", K(is_contain));
    } else {
      /*do nothing*/
    }
  } else if (source_table->is_generated_table() && target_table->is_generated_table()) {
    QueryRelation relation = QueryRelation::QUERY_UNCOMPARABLE;
    if (OB_ISNULL(source_table->ref_query_) || OB_ISNULL(target_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(source_table->ref_query_),
          K(target_table->ref_query_), K(ret));
    } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(source_table->ref_query_,
                                                              target_table->ref_query_,
                                                              stmt_map_info,
                                                              relation))) {
      LOG_WARN("failed to compute stmt relationship", K(ret));
    } else if (QueryRelation::QUERY_LEFT_SUBSET == relation ||
               QueryRelation::QUERY_EQUAL == relation) {
      is_contain = true;
      LOG_TRACE("succeed to check table item containment", K(is_contain));
    } else {
      is_contain = false;
      LOG_TRACE("succeed to check table item containment", K(is_contain));
    }
  } else {
    /*
     * todo @guoping.wgp: in future, we should check containment relations for the following two more cases
     * case 1: source_table is a basic table and target_table is a generated table
     * case 2: source_table is a generated table and target_table is a basic table
     */
    is_contain = false;
    LOG_TRACE("succeed to check table item containment", K(is_contain));
  }
  return ret;
}

int ObTransformUtils::extract_lossless_join_columns(ObDMLStmt *stmt,
                                                    ObTransformerCtx *ctx,
                                                    const TableItem *source_table,
                                                    const TableItem *target_table,
                                                    const ObIArray<int64_t> &output_map,
                                                    ObIArray<ObRawExpr *> &source_exprs,
                                                    ObIArray<ObRawExpr *> &target_exprs,
                                                    EqualSets *input_equal_sets) // default value NULL
{
  int ret = OB_SUCCESS;
  EqualSets *equal_sets = input_equal_sets;
  ObArenaAllocator allocator;
  ObSEArray<ObRawExpr*, 16> candi_source_exprs;
  ObSEArray<ObRawExpr*, 16> candi_target_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(source_table),
        K(target_table), K(ret));
  } else if (OB_FAIL(extract_lossless_mapping_columns(stmt,
                                                      source_table,
                                                      target_table,
                                                      output_map,
                                                      candi_source_exprs,
                                                      candi_target_exprs))) {
    LOG_WARN("failed to extract lossless mapping columns", K(ret));
  } else if (OB_UNLIKELY(candi_source_exprs.count() != candi_target_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(candi_source_exprs.count()),
        K(candi_target_exprs.count()), K(ret));
  } else if (NULL == equal_sets) {
    equal_sets = &ctx->equal_sets_;
    ret = stmt->get_stmt_equal_sets(*equal_sets, allocator, true);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_source_exprs.count(); i++) {
    ObRawExpr *source = NULL;
    ObRawExpr *target = NULL;
    if (OB_ISNULL(source = candi_source_exprs.at(i)) ||
        OB_ISNULL(target = candi_target_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(source), K(target), K(ret));
    } else if (!ObOptimizerUtil::is_expr_equivalent(source, target, *equal_sets)) {
      /*do nothing*/
    } else if (OB_FAIL(source_exprs.push_back(source))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(target_exprs.push_back(target))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to extract lossless columns", K(source_exprs), K(target_exprs));
  }
  if (NULL == input_equal_sets) {
    equal_sets->reuse();
  }
  return ret;
}

int ObTransformUtils::extract_lossless_mapping_columns(ObDMLStmt *stmt,
                                                       const TableItem *source_table,
                                                       const TableItem *target_table,
                                                       const ObIArray<int64_t> &output_map,
                                                       ObIArray<ObRawExpr*> &candi_source_exprs,
                                                       ObIArray<ObRawExpr*> &candi_target_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 16> source_column_items;
  if (OB_ISNULL(stmt) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(source_table),
        K(target_table), K(ret));
  } else if (OB_FAIL(stmt->get_column_items(source_table->table_id_,
                                            source_column_items))) {
    LOG_WARN("failed to get column items", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < source_column_items.count(); i++) {
      ColumnItem &source_col = source_column_items.at(i);
      ColumnItem *target_col = NULL;
      if (target_table->is_basic_table() || target_table->is_temp_table()) {
        target_col = stmt->get_column_item_by_id(target_table->table_id_,
                                                 source_col.column_id_);

      } else if (target_table->is_generated_table()) {
        int64_t pos = source_col.column_id_ - OB_APP_MIN_COLUMN_ID;
        if (OB_UNLIKELY(pos < 0 || pos >= output_map.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(pos),
              K(output_map.count()), K(ret));
        } else if (OB_INVALID_ID == output_map.at(pos)) {
          /*do nothing*/
        } else {
          target_col = stmt->get_column_item_by_id(target_table->table_id_,
                                                   output_map.at(pos) + OB_APP_MIN_COLUMN_ID);
        }
      } else { /*do nothing*/ }
      if (OB_SUCC(ret) && OB_NOT_NULL(target_col)) {
        if (OB_ISNULL(source_col.expr_) || OB_ISNULL(target_col->expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(source_col.expr_),
              K(target_col->expr_), K(ret));
        } else if (OB_FAIL(candi_source_exprs.push_back(source_col.expr_))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(candi_target_exprs.push_back(target_col->expr_))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /*do nothing*/}
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("succeed to extract lossless mapping column", K(candi_source_exprs),
          K(candi_target_exprs));
    }
  }
  return ret;
}

int ObTransformUtils::adjust_agg_and_win_expr(ObSelectStmt *source_stmt,
                                              ObRawExpr *&source_expr)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(source_stmt) || OB_ISNULL(source_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(source_stmt),
        K(source_expr), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (!source_expr->has_flag(CNT_AGG) && !source_expr->has_flag(CNT_WINDOW_FUNC)) {
    /*do nothing*/
  } else if (source_expr->is_aggr_expr()) {
    ObAggFunRawExpr *same_aggr_expr = NULL;
    if (OB_FAIL(source_stmt->check_and_get_same_aggr_item(source_expr, same_aggr_expr))) {
      LOG_WARN("failed to check and get same aggr item.", K(ret));
    } else if (same_aggr_expr != NULL) {
      source_expr = same_aggr_expr;
    } else if (OB_FAIL(source_stmt->add_agg_item(static_cast<ObAggFunRawExpr&>(*source_expr)))) {
      LOG_WARN("failed to add agg item", K(ret));
    } else { /*do nothing*/ }
  } else {
    if (source_expr->is_win_func_expr()) {
      if (OB_FAIL(source_stmt->add_window_func_expr(static_cast<ObWinFunRawExpr*>(source_expr)))) {
        LOG_WARN("failed to add window function expr", K(ret));
      } else { /*do nothing*/ }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < source_expr->get_param_count(); i++) {
      if (OB_ISNULL(source_expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(adjust_agg_and_win_expr(source_stmt,
                                                            source_expr->get_param_expr(i))))) {
        LOG_WARN("failed to remove duplicated agg expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTransformUtils::check_group_by_consistent(ObSelectStmt *sel_stmt,
                                                bool &is_consistent)
{
  int ret = OB_SUCCESS;
  is_consistent = false;
  if (OB_ISNULL(sel_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (sel_stmt->get_group_exprs().empty() && sel_stmt->get_rollup_exprs().empty()) {
    is_consistent = true;
  } else {
    is_consistent = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_consistent &&
                        i < sel_stmt->get_select_item_size(); i++) {
      ObRawExpr *expr = NULL;
      if (OB_ISNULL(expr = sel_stmt->get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret));
      } else if (expr->has_flag(CNT_AGG) ||
                 expr->is_const_raw_expr() ||
                 ObOptimizerUtil::find_item(sel_stmt->get_group_exprs(), expr) ||
                 ObOptimizerUtil::find_item(sel_stmt->get_rollup_exprs(), expr)) {
        /*do nothing*/
      } else {
        is_consistent = false;
      }
    }
  }
  return ret;
}

int ObTransformUtils::contain_select_ref(ObRawExpr *expr, bool &has)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    has = expr->has_flag(IS_SELECT_REF);
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(contain_select_ref(expr->get_param_expr(i), has))) {
      LOG_WARN("failed to check contain select ref expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::remove_select_items(ObTransformerCtx *ctx,
                                          const uint64_t table_id,
                                          ObSelectStmt &child_stmt,
                                          ObDMLStmt &upper_stmt,
                                          ObIArray<ObRawExpr*> &removed_select_exprs)
{
  int ret = OB_SUCCESS;
  ObIArray<SelectItem> &select_items = child_stmt.get_select_items();
  ObSqlBitSet<> removed_idxs;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    if (ObOptimizerUtil::find_item(removed_select_exprs, select_items.at(i).expr_)) {
      ret = removed_idxs.add_member(i);
    }
  }
  if(OB_SUCC(ret) && remove_select_items(ctx, table_id, child_stmt, upper_stmt, removed_idxs)) {
    LOG_WARN("failed to remove select items", K(ret));
  }
  return ret;
}

int ObTransformUtils::remove_select_items(ObTransformerCtx *ctx,
                                          const uint64_t table_id,
                                          ObSelectStmt &child_stmt,
                                          ObDMLStmt &upper_stmt,
                                          ObSqlBitSet<> &removed_idxs)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  typedef ObSEArray<SelectItem, 8> SelectItems;
  HEAP_VAR(SelectItems, new_select_items) {
    ObSEArray<ColumnItem, 16> new_column_items;
    if (OB_ISNULL(ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("argument invalid", K(ctx), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt.get_select_item_size(); i++) {
        if (removed_idxs.has_member(i) ) {
          // do nothing
        } else {
          ColumnItem *column_item = NULL;
          if (OB_FAIL(new_select_items.push_back(child_stmt.get_select_item(i)))) {
            LOG_WARN("failed to push back select item", K(ret));
          } else if (OB_ISNULL(column_item = upper_stmt.get_column_item_by_id(table_id,
                                                                              i + OB_APP_MIN_COLUMN_ID))) {
            // TODO yibo 如果select item是一个用户变量赋值(@a := 1), 那么即使上层stmt中没有引用到
            // 也不能删除, 因为现在user_variable的标记还不准确, 因此暂时在project pruning中规避
            count++;
            LOG_WARN("fail to find column_item in upper_stmt");
          } else if (OB_FAIL(new_column_items.push_back(*column_item))) {
            LOG_WARN("failed to push back column items", K(ret));
          } else {
            new_column_items.at(new_column_items.count() - 1).set_ref_id(table_id, count + OB_APP_MIN_COLUMN_ID);
            count++;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(upper_stmt.remove_column_item(table_id))) {
          LOG_WARN("failed to remove column item", K(ret));
        } else if (OB_FAIL(upper_stmt.add_column_item(new_column_items))) {
          LOG_WARN("failed to add column item", K(ret));
        } else if (child_stmt.is_set_stmt()) { // todo 对于 set stmt 不能直接 assign
          ret = SMART_CALL(remove_select_items(ctx, child_stmt, removed_idxs));
        } else if (OB_FAIL(child_stmt.get_select_items().assign(new_select_items))) {
          LOG_WARN("failed to assign select item", K(ret));
        } else if (child_stmt.get_select_items().empty() &&
                   OB_FAIL(create_dummy_select_item(child_stmt, ctx))) {
          LOG_WARN("failed to create dummy select item", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

/*@brief,remove_select_items递归移除union stmt无用的select item，这里有一个前提是union stmt的select item
*  和它左右ref_stmt的select item是一一对应起来的
 */
int ObTransformUtils::remove_select_items(ObTransformerCtx *ctx,
                                          ObSelectStmt &union_stmt,
                                          ObSqlBitSet<> &removed_idxs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_UNLIKELY(!union_stmt.is_set_stmt()) || union_stmt.is_recursive_union()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument invalid", K(ret), K(ctx), K(union_stmt));
  } else {
    // union select item 与 child query select item 数量相等, recursive union all 不会到达此处
    const int64_t select_count = union_stmt.get_select_item_size();
    int64_t new_select_count = -1;
    ObSelectStmt *child_stmt = NULL;
    typedef ObSEArray<SelectItem, 8> SelectItems;
    HEAP_VAR(SelectItems, new_select_items) {
      for (int64_t i = 0; OB_SUCC(ret) && i < union_stmt.get_set_query().count(); i++) {
        new_select_items.reuse();
        if (OB_ISNULL(child_stmt = union_stmt.get_set_query().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(child_stmt));
        } else if (OB_UNLIKELY(select_count != child_stmt->get_select_item_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected select item count", K(ret), K(select_count),
                                                   K(child_stmt->get_select_item_size()));
        } else if (child_stmt->is_set_stmt()) {
          ret = SMART_CALL(remove_select_items(ctx, *child_stmt, removed_idxs));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < select_count; ++j) {
            if (removed_idxs.has_member(j)) {
              /*do nothing*/
            } else if (OB_FAIL(new_select_items.push_back(child_stmt->get_select_item(j)))) {
              LOG_WARN("failed to push back select items", K(ret));
            } else {/*do nothing*/}
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(child_stmt->get_select_items().assign(new_select_items))) {
            LOG_WARN("failed to assign select item", K(ret));
          } else if (!child_stmt->get_select_items().empty()) {
            /*do nothing*/
          } else if (OB_FAIL(create_dummy_select_item(*child_stmt, ctx))) {
            LOG_WARN("failed to create dummy select item", K(ret));
          } else { /*do nothing*/ }
        }

        if (OB_FAIL(ret)) {
        } else if (-1 == new_select_count) {
          new_select_count = child_stmt->get_select_item_size();
        } else if (OB_UNLIKELY(new_select_count != child_stmt->get_select_item_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("set-stmt select items no equal", K(ret), K(new_select_count),
                                                     K(child_stmt->get_select_item_size()));
        }
      }
      new_select_items.reuse();
      int64_t idx = 0;
      ObRawExpr *set_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < select_count; i++) {
        if (removed_idxs.has_member(i)) {
          /*do nothing*/
        } else if (OB_FAIL(new_select_items.push_back(union_stmt.get_select_item(i)))) {
          LOG_WARN("failed to push back select items", K(ret));
        } else if (OB_ISNULL(set_expr = ObSelectStmt::get_pure_set_expr(new_select_items.at(idx).expr_))
                   || OB_UNLIKELY(!set_expr->is_set_op_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected set expr", K(ret), K(set_expr));
        } else {
          static_cast<ObSetOpRawExpr*>(set_expr)->set_idx(idx++);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!new_select_items.empty()) {
        union_stmt.get_select_items().assign(new_select_items);
      } else if (OB_ISNULL(ctx->expr_factory_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(ctx->expr_factory_));
      } else {
        union_stmt.get_select_items().reuse();
        ret = union_stmt.create_select_list_for_set_stmt(*ctx->expr_factory_);
      }
    }
  }
  return ret;
}

// not support set stmt
int ObTransformUtils::create_dummy_select_item(ObSelectStmt &stmt, ObTransformerCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObConstRawExpr *const_expr = NULL;
  ObRawExpr *dummy_expr = NULL;
  int64_t const_value = 1;
  if (OB_ISNULL(ctx) ||
      OB_ISNULL(expr_factory = ctx->expr_factory_) ||
      OB_ISNULL(session_info = ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(expr_factory), K(session_info), K(ret));
  } else if (stmt.is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not create dummy select for set stmt", K(ret), K(stmt.get_set_op()));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory,
                                                          ObIntType,
                                                          const_value,
                                                          const_expr))) {
    LOG_WARN("Failed to build const expr", K(ret));
  } else if (OB_ISNULL(dummy_expr = static_cast<ObRawExpr*>(const_expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(dummy_expr->formalize(session_info))) {
    LOG_WARN("Failed to formalize a new expr", K(ret));
  } else {
    SelectItem select_item;
    select_item.alias_name_ = "1";
    select_item.expr_name_ = "1";
    select_item.expr_ = dummy_expr;
    if (OB_FAIL(stmt.add_select_item(select_item))) {
      LOG_WARN("Failed to add dummy expr", K(ret));
    }
  }
  return ret;
}

//select exprs at the same pos in left_stmt/right_stmt have same result type
int ObTransformUtils::create_set_stmt(ObTransformerCtx *ctx,
                                      const ObSelectStmt::SetOperator set_type,
                                      const bool is_distinct,
                                      ObSelectStmt *left_stmt,
                                      ObSelectStmt *right_stmt,
                                      ObSelectStmt *&set_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 2> child_stmts;
  if (OB_ISNULL(left_stmt) || OB_ISNULL(right_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point", K(left_stmt), K(right_stmt), K(ret));
  } else if (OB_FAIL(child_stmts.push_back(left_stmt))
             || OB_FAIL(child_stmts.push_back(right_stmt))) {
    LOG_WARN("failed to push back stmt", K(ret));
  } else if (OB_FAIL(create_set_stmt(ctx, set_type, is_distinct, child_stmts, set_stmt))) {
    LOG_WARN("failed to create union stmt", K(ret));
  }
  return ret;
}

//select exprs at the same pos in child_stmts have same result type
int ObTransformUtils::create_set_stmt(ObTransformerCtx *ctx,
                                      const ObSelectStmt::SetOperator set_type,
                                      const bool is_distinct,
                                      ObIArray<ObSelectStmt*> &child_stmts,
                                      ObSelectStmt *&set_stmt)
{
  int ret = OB_SUCCESS;
  set_stmt = NULL;
  ObSelectStmt *temp_stmt = NULL;
  ObSelectStmt *child_stmt = NULL;
  const int64_t child_num = child_stmts.count();
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->stmt_factory_) || OB_ISNULL(ctx->expr_factory_)
      || OB_ISNULL(ctx->stmt_factory_->get_query_ctx())
      || OB_UNLIKELY(child_num < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point", K(ret), K(ctx), K(child_stmts));
  } else if (child_num > 2 && (ObSelectStmt::UNION != set_type)) {
    ObSEArray<ObSelectStmt*, 2> temp_child_stmts;
    temp_stmt = child_stmts.at(0);
    for (int64_t i = 1; OB_SUCC(ret) && i < child_num; ++i) {
      temp_child_stmts.reuse();
      if (OB_ISNULL(temp_stmt) || OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null point", K(ret), K(temp_stmt), K(child_stmts.at(i)));
      } else if (OB_FAIL(temp_child_stmts.push_back(temp_stmt)) ||
                 OB_FAIL(temp_child_stmts.push_back(child_stmts.at(i)))) {
        LOG_WARN("failed to push back stmt", K(ret));
      } else if (OB_FAIL(SMART_CALL(create_set_stmt(ctx, set_type, is_distinct,
                                                    temp_child_stmts, temp_stmt)))) {
        LOG_WARN("failed to create union stmt", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      set_stmt = temp_stmt;
    }
  } else if (OB_FAIL(ctx->stmt_factory_->create_stmt(temp_stmt))) {
    LOG_WARN("failed to create union stmt", K(temp_stmt), K(ret));
  } else if (OB_ISNULL(temp_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(temp_stmt));
  } else if (OB_FAIL(temp_stmt->get_set_query().assign(child_stmts))) {
    LOG_WARN("failed to assign child query", K(ret));
  } else if (OB_ISNULL(child_stmt = child_stmts.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null point", K(ret), K(child_stmt));
  } else {
    if (ObSelectStmt::UNION != set_type || is_distinct) {
      temp_stmt->assign_set_distinct();
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
        if (OB_ISNULL(child_stmts.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null stmt", K(ret));
        } else if (child_stmts.at(i)->get_select_items().empty() &&
                   OB_FAIL(create_dummy_select_item(*child_stmts.at(i), ctx))) {
          LOG_WARN("failed to create dummy select", K(ret));
        }
      }
    } else {
      temp_stmt->assign_set_all();
    }
    temp_stmt->set_query_ctx(ctx->stmt_factory_->get_query_ctx());
    temp_stmt->assign_set_op(set_type);
    temp_stmt->set_calc_found_rows(child_stmt->is_calc_found_rows());
    // adjust statement id after set query_ctx and set_type
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(temp_stmt->adjust_statement_id(ctx->allocator_,
                                               ctx->src_qb_name_,
                                               ctx->src_hash_val_))) {
      LOG_WARN("failed to adjust statement id", K(ret), K(child_stmt));
    } else if (OB_FAIL(temp_stmt->create_select_list_for_set_stmt(*ctx->expr_factory_))) {
      LOG_WARN("failed to create select list for union", K(ret));
    } else {
      set_stmt = temp_stmt;
    }
  }
  return ret;
}

int ObTransformUtils::pushdown_group_by(ObSelectStmt *parent_stmt,
                                        ObIArray<ObRawExpr *> &pushdown_groupby,
                                        ObIArray<ObRawExpr *> &pushdown_rollup,
                                        ObIArray<ObRawExpr *> &pushdown_aggr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (OB_FAIL(pushdown_groupby.assign(parent_stmt->get_group_exprs()))) {
    LOG_WARN("failed to assign group by exprs.", K(ret));
  } else if (OB_FAIL(pushdown_rollup.assign(parent_stmt->get_rollup_exprs()))) {
    LOG_WARN("failed to assign rollup exprs.", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < parent_stmt->get_aggr_item_size(); i++) {
    if (OB_FAIL(add_var_to_array_no_dup(pushdown_aggr,
                  static_cast<ObRawExpr *>(parent_stmt->get_aggr_items().at(i))))) {
      LOG_WARN("failed to add var", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    parent_stmt->get_group_exprs().reset();
    parent_stmt->get_rollup_exprs().reset();
    parent_stmt->get_aggr_items().reset();
  }
  return ret;
}

/**
 * @brief ObTransformUtils::create_simple_view
 * 在 push_group_by 为 false 时，将 stmt 分解成两层：
 * 内层做 table scan, join 和 filter，构成一次 SPJ 查询
 * 外层做 distinct, group-by, order-by, window function 等非 SPJ 的操作
 * let the view_stmt process some subqueries
 * push_group_by：将聚合函数也移入 view 中计算
 * what is more :
 *   it need follow a basic 'select computing sequence' which when push_group_by is true,
 * then push_conditions need be true
 */
int ObTransformUtils::create_simple_view(ObTransformerCtx *ctx,
                                         ObDMLStmt *stmt,
                                         ObSelectStmt *&view_stmt,
                                         bool push_subquery,
                                         bool push_conditions,
                                         bool push_group_by,
                                         ObAliasRefRawExpr *alias_expr)
{
  int ret = OB_SUCCESS;
  ObStmtFactory *stmt_factory = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObSEArray<ObRawExpr *, 4> select_list;
  ObSEArray<ObRawExpr *, 8> norm_conds;
  ObSEArray<ObRawExpr *, 8> rownum_conds;
  ObSEArray<TableItem *, 4> from_tables;
  ObSEArray<SemiInfo *, 4> semi_infos;
  ObSEArray<ObRawExpr *, 4> group_exprs;
  ObSEArray<ObRawExpr *, 4> rollup_exprs;
  TableItem *view_table_item = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(stmt) ||
      OB_ISNULL(session_info = ctx->session_info_) ||
      OB_ISNULL(stmt_factory = ctx->stmt_factory_) ||
      OB_ISNULL(expr_factory = ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx));
  } else if (stmt->is_set_stmt() ||
             stmt->is_hierarchical_query() ||
             !stmt->is_sel_del_upd()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not create spj stmt", K(ret), K(stmt->is_set_stmt()),
             K(stmt->is_hierarchical_query()),
             K(stmt->is_sel_del_upd()));
  } else if (OB_FAIL(ctx->add_src_hash_val(ObTransformerCtx::SRC_STR_CREATE_SIMPLE_VIEW))) {
    LOG_WARN("failed to add src hash val", K(ret));
  }

  // handle where conditions
  if (OB_SUCC(ret) && push_conditions) {
    if (OB_FAIL(classify_rownum_conds(*stmt, norm_conds, rownum_conds))) {
      LOG_WARN("failed to classify rownum conditions", K(ret));
    }
  }

  // let the view_stmt process some subqueries
  if (OB_SUCC(ret) && push_subquery) {
    ObSEArray<ObRawExpr *, 4> post_join_exprs;
    ObSEArray<ObRawExpr *, 4> non_scalar_query_refs;
    if (OB_FAIL(get_post_join_exprs(stmt, post_join_exprs))) {
      LOG_WARN("failed to get additional push down exprs", K(ret));
    } else if (OB_FAIL(classify_scalar_query_ref(post_join_exprs,
                                                 select_list,
                                                 non_scalar_query_refs))) {
      LOG_WARN("failed to classify scalar query ref", K(ret));
    }
  }

  // push down all group by
  if (OB_SUCC(ret) && push_group_by) {
    ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
    if (OB_UNLIKELY(!stmt->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt", K(ret), KPC(stmt));
    } else if (OB_FAIL(pushdown_group_by(sel_stmt, group_exprs, rollup_exprs, select_list))) {
      LOG_WARN("failed to push down group exprs", K(ret));
    } else if (OB_FAIL(append(sel_stmt->get_condition_exprs(), sel_stmt->get_having_exprs()))) {
      LOG_WARN("failed to push back having exprs into where", K(ret));
    } else if (FALSE_IT(sel_stmt->get_having_exprs().reset())) {
      // never reach
    } else if (OB_FAIL(append_array_no_dup(select_list, group_exprs))) {
      LOG_WARN("failed to push back having exprs into where", K(ret));
    }
  }

  // push down vector assign
  if (OB_SUCC(ret) && NULL != alias_expr) {
    ObQueryRefRawExpr *query_ref = NULL;
    ObUpdateStmt *upd_stmt = static_cast<ObUpdateStmt *>(stmt);
    ObSEArray<ObRawExpr *, 4> alias_exprs;
    if (OB_UNLIKELY(!stmt->is_update_stmt()) ||
        OB_ISNULL(query_ref = static_cast<ObQueryRefRawExpr *>(
                  alias_expr->get_param_expr(0))) ||
        OB_ISNULL(query_ref->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt", K(ret), KPC(stmt));
    } else if (OB_FAIL(upd_stmt->get_vector_assign_values(query_ref, alias_exprs))) {
      LOG_WARN("failed to get vector assign values", K(ret));
    } else if (OB_FAIL(append_array_no_dup(select_list, alias_exprs))) {
      LOG_WARN("failed to append array", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(pushdown_pseudo_column_like_exprs(*stmt, push_group_by, select_list))) {
    LOG_WARN("failed to pushdown pseudo column like exprs", K(ret));
  } else if (OB_FAIL(stmt->get_from_tables(from_tables))) {
    LOG_WARN("failed to get from tables", K(ret));
  } else if (OB_FAIL(semi_infos.assign(stmt->get_semi_infos()))) {
    LOG_WARN("failed to assign semi info", K(ret));
  } else if (ObOptimizerUtil::remove_item(stmt->get_condition_exprs(), norm_conds)) {
    LOG_WARN("failed to remove item", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx,
                                                               stmt,
                                                               view_table_item,
                                                               from_tables,
                                                               &semi_infos))) {
    LOG_WARN("failed to create empty view", K(ret));
  } else if (OB_FAIL(create_inline_view(ctx,
                                        stmt,
                                        view_table_item,
                                        from_tables,
                                        &norm_conds,
                                        &semi_infos,
                                        &select_list,
                                        &group_exprs,
                                        &rollup_exprs))) {
    LOG_WARN("failed to create inline view", K(ret));
  } else {
    ctx->src_hash_val_.pop_back();
    view_stmt = view_table_item->ref_query_;
  }
  return ret;
}

/*
 * select c1, sum(c2) from t1 where c1 > 2 group by grouping sets(c1, c2) having sum(c2) > 3 order by c1 limit 3
 * select v.c1, v.sum(c2) from v (
 *   select c1, sum(c2) from t1 where c1 > 2 group by grouping sets(c1, c2) having sum(c2) > 3
 * ) order by v.c1 limit 3;
 */
int ObTransformUtils::create_view_with_groupby_items(ObSelectStmt *stmt,
                                                     TableItem *&view_table_item,
                                                     ObTransformerCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObStmtFactory *stmt_factory = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObSelectStmt *view_stmt = NULL;
  ObSEArray<ObRawExpr *, 4> select_list;
  ObSEArray<ObRawExpr *, 4> new_cols;
  ObSEArray<ObRawExpr *, 4> groupby_exprs;
  ObSEArray<ObRawExpr *, 4> aggr_list;
  view_table_item = NULL;
  ObStmtExprGetter stmt_getter;
  stmt_getter.remove_all();
  stmt_getter.add_scope(SCOPE_GROUPBY);
  stmt_getter.set_recursive(false);
  if (OB_ISNULL(ctx) ||
      OB_ISNULL(stmt) ||
      OB_ISNULL(stmt_factory = ctx->stmt_factory_) ||
      OB_ISNULL(expr_factory = ctx->expr_factory_) ||
      OB_ISNULL(session_info = ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx), K(stmt));
  } else if (stmt->is_set_stmt() ||
             stmt->is_hierarchical_query()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not create spj stmt", K(ret), K(stmt->is_set_stmt()),
             K(stmt->is_hierarchical_query()));
  } else if (OB_FAIL(stmt_factory->create_stmt<ObSelectStmt>(view_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_FAIL(view_stmt->ObSelectStmt::assign(*stmt))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else if (OB_FAIL(ctx->add_src_hash_val(ObTransformerCtx::SRC_STR_CREATE_SIMPLE_VIEW))) {
    LOG_WARN("failed to add src hash val", K(ret));
  } else {
    // 1. set basic struct for stmt and view_stmt
    view_stmt->get_pseudo_column_like_exprs().reset();
    view_stmt->get_window_func_exprs().reset();
    view_stmt->get_order_items().reset();
    view_stmt->set_limit_offset(NULL, NULL);
    view_stmt->set_limit_percent_expr(NULL);
    view_stmt->set_fetch_with_ties(false);
    view_stmt->set_has_fetch(false);
    view_stmt->clear_sequence();
    view_stmt->set_select_into(NULL);
    view_stmt->assign_all();

    stmt->reset_table_items();
    stmt->get_joined_tables().reuse();
    stmt->get_semi_infos().reuse();
    stmt->get_column_items().reuse();
    stmt->clear_from_items();
    stmt->get_part_exprs().reset();
    stmt->get_check_constraint_items().reset();
    stmt->get_condition_exprs().reset();
    stmt->get_group_exprs().reset();
    stmt->get_rollup_exprs().reset();
    stmt->get_grouping_sets_items().reset();
    stmt->get_rollup_items().reset();
    stmt->get_cube_items().reset();
    stmt->get_having_exprs().reset();
    stmt->get_aggr_items().reset();
    if (OB_FAIL(stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild tables hash", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // for unable to pre aggregate stmt, need to add this
    view_stmt->get_select_items().reset();
    if (OB_FAIL(view_stmt->get_relation_exprs(groupby_exprs, stmt_getter))) {
      LOG_WARN("failed to get_relation_exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(select_list, groupby_exprs))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(append(aggr_list, view_stmt->get_aggr_items()))) {
      LOG_WARN("failed to append aggregation exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(select_list, aggr_list))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(create_select_item(*(ctx->allocator_), select_list, view_stmt))) {
      LOG_WARN("failed to create select items", K(ret));
    } else if (OB_FAIL(view_stmt->formalize_stmt_expr_reference())) {
      LOG_WARN("failed to formalize stmt expr reference", K(ret));
    } else if (OB_FAIL(view_stmt->get_stmt_hint().set_simple_view_hint())) {
      LOG_WARN("failed to set simple view hint", K(ret));
    } else if (OB_FAIL(view_stmt->adjust_statement_id(ctx->allocator_,
                                                      ctx->src_qb_name_,
                                                      ctx->src_hash_val_))) {
      LOG_WARN("failed to adjust statement id", K(ret));
    } else if (view_stmt->get_select_items().empty()) {
      if (OB_FAIL(create_dummy_select_item(*view_stmt, ctx))) {
        LOG_WARN("failed to create dummy select item", K(ret));
      } else if (OB_FAIL(select_list.push_back(view_stmt->get_select_item(0).expr_))) {
        LOG_WARN("failed to push back dummy select expr", K(ret));
      } else { /* do nothing */ }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_new_table_item(ctx, stmt, view_stmt, view_table_item))) {
      LOG_WARN("failed to add new table item", K(ret));
    } else if (OB_ISNULL(view_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(stmt->add_from_item(view_table_item->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(create_columns_for_view(ctx, *view_table_item, stmt, new_cols))) {
      LOG_WARN("failed to create columns for view", K(ret));
    } else if (view_stmt->has_for_update() && lib::is_oracle_mode()) {
      view_table_item->for_update_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    ctx->src_hash_val_.pop_back();
    if (OB_FAIL(ObTransformUtils::generate_select_list(ctx, stmt, view_table_item, &select_list))) {
      LOG_WARN("failed to generate_select_list", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else { /* do nothing */ }
  }
  return ret;
}
int ObTransformUtils::pushdown_pseudo_column_like_exprs(ObDMLStmt &upper_stmt,
                                                        bool push_group_by,
                                                        ObIArray<ObRawExpr*> &pushdown_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> new_pushdown_exprs;
  ObSEArray<ObRawExpr*, 8> new_upper_pseudo_columns;
  ObIArray<ObRawExpr*> &upper_pseudo_columns = upper_stmt.get_pseudo_column_like_exprs();
  ObRawExpr *expr = NULL;
  bool need_pushdown = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < upper_pseudo_columns.count(); ++i) {
    if (OB_ISNULL(expr = upper_pseudo_columns.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (OB_FAIL(check_need_pushdown_pseudo_column(*expr, push_group_by, need_pushdown))) {
      LOG_WARN("failed to check need pushdown pseudo column like exprs", K(ret));
    } else if (need_pushdown && OB_FAIL(new_pushdown_exprs.push_back(expr))) {
      LOG_WARN("failed to push back pushdown expr", K(ret));
    } else if (!need_pushdown && OB_FAIL(new_upper_pseudo_columns.push_back(expr))) {
      LOG_WARN("failed to push back upper expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && !new_pushdown_exprs.empty()) {
    if (OB_FAIL(upper_stmt.get_pseudo_column_like_exprs().assign(new_upper_pseudo_columns))) {
      LOG_WARN("failed to assign pseudo column like exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(pushdown_exprs, new_pushdown_exprs))) {
      LOG_WARN("failed to append pushdown exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::check_need_pushdown_pseudo_column(const ObRawExpr &expr,
                                                        const bool push_group_by,
                                                        bool &need_pushdown)
{
  int ret = OB_SUCCESS;
  need_pushdown = false;
  switch (expr.get_expr_type()) {
    case T_ORA_ROWSCN: {
      need_pushdown = true;
      break;
    }
    case T_FUN_SYS_ROWNUM: {
      if (push_group_by) {
        need_pushdown = true;
      }
      break;
    }
    case T_PSEUDO_EXTERNAL_FILE_COL: {
      need_pushdown = true;
      break;
    }
    default:
      break;
  }
  return ret;
}

/**
 * @brief ObTransformUtils::adjust_updatable_view
 * create part exprs for updatable view
   keep and rename part exprs for update/delete stmt
   because they are required by global index update
 * @param expr_factory
 * @param del_upd_stmt
 * @param view_table_item
 * @return
 */
int ObTransformUtils::adjust_updatable_view(ObRawExprFactory &expr_factory,
                                            ObDelUpdStmt *del_upd_stmt,
                                            TableItem &view_table_item,
                                            ObIArray<uint64_t>* origin_table_ids /* = NULL*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDMLStmt::PartExprItem, 4> part_exprs;
  ObSelectStmt *view_stmt = NULL;
  ObRawExprCopier copier(expr_factory);
  ObSEArray<ObRawExpr *, 4> select_list;
  ObSEArray<ObRawExpr *, 4> column_list;
  ObSEArray<ObDmlTableInfo*, 2> table_infos;
  if (OB_ISNULL(view_stmt = view_table_item.ref_query_) || OB_ISNULL(del_upd_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(view_table_item), K(del_upd_stmt));
  } else if (OB_FAIL(del_upd_stmt->get_view_output(view_table_item,
                                                   select_list,
                                                   column_list))) {
    LOG_WARN("failed to get view output", K(ret));
  } else if (OB_FAIL(copier.add_replaced_expr(select_list, column_list))) {
    LOG_WARN("failed to add replace pair", K(ret));
  } else if (OB_FAIL(del_upd_stmt->get_dml_table_infos(table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
    ObDmlTableInfo* table_info = table_infos.at(i);
    if (OB_ISNULL(table_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table info", K(ret));
    } else if (NULL == origin_table_ids ||
               ObOptimizerUtil::find_item(*origin_table_ids, table_info->table_id_)) {
      table_info->table_id_ = view_table_item.table_id_;
      uint64_t loc_table_id = table_info->loc_table_id_;
      // create partition exprs for index dml infos
      if (OB_FAIL(view_stmt->get_part_expr_items(loc_table_id, part_exprs))) {
        LOG_WARN("failed to get part expr items", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < part_exprs.count(); ++i) {
        ObDMLStmt::PartExprItem part_item;
        if (OB_FAIL(part_item.deep_copy(copier, part_exprs.at(i)))) {
          LOG_WARN("failed to deep copy part expr item", K(ret));
        } else {
          part_exprs.at(i) = part_item;
        }
      }
      if (OB_SUCC(ret) && !part_exprs.empty()) {
        if (OB_FAIL(del_upd_stmt->set_part_expr_items(part_exprs))) {
          LOG_WARN("failed to set part expr items", K(ret));
        }
      }
      // replace values_desc
      if (OB_SUCC(ret)) {
        ObStmtExprReplacer replacer;
        replacer.add_scope(SCOPE_INSERT_DESC);
        replacer.set_recursive(false);
        if (OB_FAIL(replacer.add_replace_exprs(select_list, column_list))) {
          LOG_WARN("failed to add replace exprs", K(ret));
        } else if (OB_FAIL(table_info->iterate_stmt_expr(replacer))) {
          LOG_WARN("failed to iterate stmt expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::create_stmt_with_generated_table(ObTransformerCtx *ctx,
                                                       ObSelectStmt *child_stmt,
                                                       ObSelectStmt *&parent_stmt)
{
  int ret = OB_SUCCESS;
  TableItem *new_table_item = NULL;
  ObSEArray<ObRawExpr *, 8> column_exprs;
  parent_stmt = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_) ||
      OB_ISNULL(child_stmt) || OB_ISNULL(ctx->stmt_factory_) || OB_ISNULL(ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(child_stmt), K(ret));
  } else if (OB_FAIL(ctx->stmt_factory_->create_stmt(parent_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(parent_stmt->set_query_ctx(child_stmt->get_query_ctx()))) {
  } else if (OB_FAIL(parent_stmt->get_stmt_hint().set_simple_view_hint(&child_stmt->get_stmt_hint()))) {
    LOG_WARN("failed to set simple view hint", K(ret));
  } else if (OB_FAIL(parent_stmt->adjust_statement_id(ctx->allocator_,
                                                      ctx->src_qb_name_,
                                                      ctx->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else if (OB_FAIL(add_new_table_item(ctx, parent_stmt, child_stmt, new_table_item))) {
    LOG_WARN("failed to add new table item", K(ret));
  } else if (OB_ISNULL(new_table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(parent_stmt->add_from_item(new_table_item->table_id_, false))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FAIL(parent_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuid table hash", K(ret));
  } else {
    if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx,
                                                          *new_table_item,
                                                          parent_stmt,
                                                          column_exprs))) {
      LOG_WARN("failed to create column items", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx->allocator_,
                                                            column_exprs,
                                                            parent_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    } else {
      parent_stmt->set_select_into(child_stmt->get_select_into());
      child_stmt->set_select_into(NULL);//转移 child_stmt select into 到 parent_stmt
      if (OB_FAIL(parent_stmt->formalize_stmt(ctx->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
      }
    }
  }
  return ret;
}

//使用 join table 创建 simple_stmt, select 由 stmt 中使用的 table 列确定,
//simple_stmt 中直接使用了 stmt 中 joined_table 对应 column expr,
//进行 expr replace 等操作时需要注意
int ObTransformUtils::create_stmt_with_joined_table(ObTransformerCtx *ctx,
                                                    ObDMLStmt *stmt,
                                                    JoinedTable *joined_table,
                                                    ObSelectStmt *&simple_stmt)
{
  int ret = OB_SUCCESS;
  simple_stmt = NULL;
  ObSEArray<TableItem *, 8> table_items;
  if (OB_ISNULL(ctx) || OB_ISNULL(stmt) || OB_ISNULL(joined_table) || OB_ISNULL(ctx->allocator_)
      || OB_ISNULL(ctx->stmt_factory_) || OB_ISNULL(ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ctx->stmt_factory_->create_stmt(simple_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(simple_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(simple_stmt->set_query_ctx(stmt->get_query_ctx()))) {
  } else if (OB_FAIL(simple_stmt->get_stmt_hint().set_simple_view_hint(&stmt->get_stmt_hint()))) {
    LOG_WARN("failed to set simple view hint", K(ret));
  } else if (OB_FAIL(extract_table_items(joined_table, table_items))) {
    LOG_WARN("failed to extract table items", K(ret));
  } else if (OB_FAIL(add_table_item(simple_stmt, table_items))) {
    LOG_WARN("failed to add table item", K(ret));
  } else if (OB_FAIL(simple_stmt->add_joined_table(joined_table))) {
    LOG_WARN("failed to add join table", K(ret));
  } else if (OB_FAIL(simple_stmt->add_from_item(joined_table->table_id_, true))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FAIL(simple_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(simple_stmt->adjust_statement_id(ctx->allocator_,
                                                      ctx->src_qb_name_,
                                                      ctx->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else {
    ObSEArray<ObDMLStmt::PartExprItem, 8> part_items;
    ObSEArray<ColumnItem, 8> column_items;
    ObSqlBitSet<> from_tables_set;
    ObSEArray<ObRawExpr *, 8> tmp_column_exprs;
    ObSEArray<ObRawExpr *, 8> select_exprs;
    ObSEArray<ObRawExpr *, 8> shared_exprs;
    ObSEArray<DmlStmtScope, 1> ignore_scopes;
    if (OB_FAIL(stmt->get_part_expr_items(joined_table->single_table_ids_, part_items))) {
      LOG_WARN("failed to get part expr items", K(ret));
    } else if (OB_FAIL(simple_stmt->set_part_expr_items(part_items))) {
      LOG_WARN("failed to set part expr items", K(ret));
    } else if (OB_FAIL(stmt->get_column_items(joined_table->single_table_ids_, column_items))) {
      LOG_WARN("failed to get column items", K(ret));
    } else if (OB_FAIL(simple_stmt->add_column_item(column_items))) {
      LOG_WARN("failed to add column items", K(ret));
    } else if (OB_FAIL(simple_stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item rel id", K(ret));
    } else if (OB_FAIL(simple_stmt->get_column_exprs(tmp_column_exprs))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(simple_stmt->get_from_tables(from_tables_set))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(extract_table_exprs(*simple_stmt,
                                           tmp_column_exprs,
                                           from_tables_set,
                                           select_exprs))) {
      LOG_WARN("failed to extract table exprs", K(ret));
      // TODO: extract inseparable query exprs if they can be shared
    } else if (OB_FAIL(ignore_scopes.push_back(SCOPE_JOINED_TABLE))) {
      LOG_WARN("failed to push back join table scope", K(ret));
    } else if (OB_FAIL(extract_shared_expr(stmt,
                                           simple_stmt,
                                           shared_exprs,
                                           &ignore_scopes))) {
      LOG_WARN("failed to extract shared exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(select_exprs, shared_exprs))) {
      LOG_WARN("failed to append array", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx->allocator_,
                                                            select_exprs,
                                                            simple_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    } else if (OB_FAIL(simple_stmt->formalize_stmt(ctx->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

//使用 basic table 创建 simple_stmt, select 由 stmt 中使用的 table 列确定,
//simple_stmt 中直接使用了 stmt 中 table 对应 column expr,
//进行 expr replace 等操作时需要注意
int ObTransformUtils::create_stmt_with_basic_table(ObTransformerCtx *ctx,
                                                   ObDMLStmt *stmt,
                                                   TableItem *table,
                                                   ObSelectStmt *&simple_stmt)
{
  int ret = OB_SUCCESS;
  simple_stmt = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(stmt) || OB_ISNULL(table) || OB_ISNULL(ctx->allocator_)
      || OB_ISNULL(ctx->stmt_factory_) || OB_ISNULL(ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!table->is_basic_table() && !table->is_temp_table()
             && !table->is_link_table()
             && !table->is_function_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table type", K(ret), K(table->type_));
  } else if (OB_FAIL(ctx->stmt_factory_->create_stmt(simple_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(simple_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FALSE_IT(simple_stmt->set_query_ctx(stmt->get_query_ctx()))) {
  } else if (OB_FAIL(simple_stmt->get_stmt_hint().set_simple_view_hint(&stmt->get_stmt_hint()))) {
    LOG_WARN("failed to set simple view hint", K(ret));
  } else if (OB_FAIL(add_table_item(simple_stmt, table))) {
    LOG_WARN("failed to add table item", K(ret));
  } else if (OB_FAIL(simple_stmt->add_from_item(table->table_id_, false))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FAIL(simple_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(simple_stmt->adjust_statement_id(ctx->allocator_,
                                                      ctx->src_qb_name_,
                                                      ctx->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else {
    ObSEArray<ObDMLStmt::PartExprItem, 8> part_items;
    ObSEArray<ColumnItem, 8> column_items;
    if (OB_FAIL(stmt->get_part_expr_items(table->table_id_, part_items))) {
      LOG_WARN("failed to get part expr items", K(ret));
    } else if (OB_FAIL(simple_stmt->set_part_expr_items(part_items))) {
      LOG_WARN("failed to set part expr items", K(ret));
    } else if (OB_FAIL(stmt->get_column_items(table->table_id_, column_items))) {
      LOG_WARN("failed to get column items", K(ret));
    } else if (OB_FAIL(simple_stmt->add_column_item(column_items))) {
      LOG_WARN("failed to add column items", K(ret));
    } else if (OB_FAIL(simple_stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item rel id", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx->allocator_,
                                                            column_items,
                                                            simple_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    } else if (OB_FAIL(simple_stmt->formalize_stmt(ctx->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::copy_joined_table_expr(ObRawExprFactory &expr_factory,
                                             ObDMLStmt *stmt,
                                             JoinedTable *table)
{
  int ret = OB_SUCCESS;
  ObRawExprCopier copier(expr_factory);
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
    ObColumnRefRawExpr *col = NULL;
    if (OB_ISNULL(col = stmt->get_column_item(i)->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (!ObOptimizerUtil::find_item(table->single_table_ids_,
                                          col->get_table_id())) {
      // do nothing
    } else if (OB_FAIL(copier.add_skipped_expr(col))) {
      LOG_WARN("failed to add skipped expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_subquery_expr_size(); ++i) {
    if (OB_FAIL(copier.add_skipped_expr(stmt->get_subquery_exprs().at(i), false))) {
      LOG_WARN("failed to add skipped expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(inner_copy_joined_table_expr(copier, table))) {
    LOG_WARN("failed to copy joined table expr", K(ret));
  }
  return ret;
}

int ObTransformUtils::inner_copy_joined_table_expr(ObRawExprCopier &copier,
                                                   JoinedTable *table)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> new_conds;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is null", K(ret), K(table));
  } else if (OB_FAIL(copier.copy(table->join_conditions_, new_conds))) {
    LOG_WARN("failed to copy on replace condition", K(ret));
  } else if (OB_FAIL(table->join_conditions_.assign(new_conds))) {
    LOG_WARN("failed to assign new conditions", K(ret));
  } else if (NULL != table->left_table_ &&
             table->left_table_->is_joined_table() &&
             OB_FAIL(inner_copy_joined_table_expr(copier,
                                                  static_cast<JoinedTable*>(table->left_table_)))) {
    LOG_WARN("failed to inner copy joined table expr", K(ret));
  } else if (NULL != table->right_table_ &&
             table->right_table_->is_joined_table() &&
             OB_FAIL(inner_copy_joined_table_expr(copier,
                                                  static_cast<JoinedTable*>(table->right_table_)))) {
    LOG_WARN("failed to inner copy joined table expr", K(ret));
  }
  return ret;
}

int ObTransformUtils::replace_with_empty_view(ObTransformerCtx *ctx,
                                              ObDMLStmt *stmt,
                                              TableItem *&view_table,
                                              TableItem *from_table,
                                              ObIArray<SemiInfo *> *semi_infos/* = NULL*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem *, 1> tables;
  if (OB_FAIL(tables.push_back(from_table))) {
    LOG_WARN("failed to push back", K(ret));
  } else {
    ret = replace_with_empty_view(ctx, stmt, view_table, tables, semi_infos);
  }
  return ret;
}

int ObTransformUtils::replace_with_empty_view(ObTransformerCtx *ctx,
                                              ObDMLStmt *stmt,
                                              TableItem *&view_table,
                                              ObIArray<TableItem *> &from_tables,
                                              ObIArray<SemiInfo *> *semi_infos/* = NULL*/)
{
  int ret = OB_SUCCESS;
  int add_to_from = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(add_new_table_item(ctx,
                                        stmt,
                                        NULL,
                                        view_table))) {
    LOG_WARN("failed to add new table item", K(ret));
  } else if (NULL != semi_infos &&
             OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_semi_infos(),
                                                  *semi_infos))) {
    LOG_WARN("failed to remove semi infos from stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < from_tables.count(); ++i) {
    TableItem *table = from_tables.at(i);
    bool remove_happened = false;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret));
    } else if (OB_FAIL(replace_table_in_semi_infos(stmt, view_table, table))) {
      LOG_WARN("failed to replace semi infos from stmt", K(ret));
    } else if (OB_FAIL(replace_table_in_joined_tables(stmt, view_table, table))) {
      LOG_WARN("failed to replace table in joined tables", K(ret));
    } else if (OB_FAIL(stmt->remove_from_item(table->table_id_, &remove_happened))) {
      LOG_WARN("failed to remove from item", K(ret));
    } else {
      add_to_from |= remove_happened;
    }
  }
  if (OB_SUCC(ret) && add_to_from &&
      OB_FAIL(stmt->add_from_item(view_table->table_id_, false))) {
    LOG_WARN("failed to add from item", K(ret));
  }
  return ret;
}

int ObTransformUtils::create_inline_view(ObTransformerCtx *ctx,
                                         ObDMLStmt *stmt,
                                         TableItem *&view_table,
                                         TableItem * push_table,
                                         ObIArray<ObRawExpr *> *conditions/* = NULL*/,
                                         ObIArray<SemiInfo *> *semi_infos/* = NULL*/,
                                         ObIArray<ObRawExpr *> *select_exprs/* = NULL*/,
                                         ObIArray<ObRawExpr *> *group_exprs/* = NULL*/,
                                         ObIArray<ObRawExpr *> *rollup_exprs/* = NULL*/,
                                         ObIArray<ObRawExpr *> *having_exprs/* = NULL*/,
                                         ObIArray<OrderItem> *order_items/* = NULL*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem *, 1> tables;
  if (OB_FAIL(tables.push_back(push_table))) {
    LOG_WARN("failed to push back", K(ret));
  } else {
    ret = create_inline_view(ctx, stmt, view_table, tables, conditions,
                             semi_infos, select_exprs, group_exprs,
                             rollup_exprs, having_exprs, order_items);
  }
  return ret;
}


int ObTransformUtils::create_inline_view(ObTransformerCtx *ctx,
                                         ObDMLStmt *stmt,
                                         TableItem *&view_table,
                                         ObIArray<TableItem *> &from_tables,
                                         ObIArray<ObRawExpr *> *conditions/* = NULL*/,
                                         ObIArray<SemiInfo *> *semi_infos/* = NULL*/,
                                         ObIArray<ObRawExpr *> *select_exprs/* = NULL*/,
                                         ObIArray<ObRawExpr *> *group_exprs/* = NULL*/,
                                         ObIArray<ObRawExpr *> *rollup_exprs/* = NULL*/,
                                         ObIArray<ObRawExpr *> *having_exprs/* = NULL*/,
                                         ObIArray<OrderItem> *order_items/* = NULL*/)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *view_stmt = NULL;
  ObSEArray<uint64_t, 8> basic_table_ids;
  ObSEArray<uint64_t, 8> basic_table_ids_exclude_semi;
  bool add_to_from = false;

  if (OB_ISNULL(stmt) || OB_ISNULL(ctx)
     || OB_ISNULL(ctx->expr_factory_)
     || OB_ISNULL(ctx->allocator_)
     || OB_ISNULL(view_table)
     || OB_UNLIKELY(!view_table->is_generated_table())
     || OB_UNLIKELY(NULL != view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(stmt), K(ctx), K(view_table));
  } else if (OB_FAIL(ctx->stmt_factory_->create_stmt(view_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(view_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(view_stmt));
  } else if (OB_FAIL(view_stmt->get_stmt_hint().set_simple_view_hint(&stmt->get_stmt_hint()))) {
    LOG_WARN("failed to set simple view hint", K(ret));
  } else if (FALSE_IT(view_stmt->set_query_ctx(stmt->get_query_ctx()))) {
    // never reach
  } else if (OB_FAIL(view_stmt->adjust_statement_id(ctx->allocator_,
                                                    ctx->src_qb_name_,
                                                    ctx->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else {
    view_table->ref_query_ = view_stmt;
  }

  // 1. construct view stmt
  // 1.1 move from tables
  for (int64_t i = 0; OB_SUCC(ret) && i < from_tables.count(); ++i) {
    bool is_joined_table = (NULL != from_tables.at(i) && from_tables.at(i)->is_joined_table());
    TableItem *table = from_tables.at(i);
    bool remove_happened = false;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null", K(ret), K(table));
    } else if (OB_FAIL(stmt->remove_from_item(table->table_id_, &remove_happened))) {
      LOG_WARN("failed to remove from item", K(ret));
    } else if (OB_FAIL(view_stmt->add_from_item(from_tables.at(i)->table_id_, is_joined_table))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (is_joined_table) {
      if (OB_FAIL(append(basic_table_ids, static_cast<JoinedTable *>(table)->single_table_ids_))) {
        LOG_WARN("failed to append single table ids", K(ret));
      } else if (OB_FAIL(view_stmt->add_joined_table(static_cast<JoinedTable *>(table)))) {
        LOG_WARN("failed to add joined table", K(ret));
      }
    } else if (OB_FAIL(basic_table_ids.push_back(table->table_id_))) {
      LOG_WARN("failed to push back table id", K(ret));
    }
    add_to_from |= remove_happened;
  }

  if (OB_SUCC(ret) && OB_FAIL(basic_table_ids_exclude_semi.assign(basic_table_ids))) {
    LOG_WARN("failed to assign", K(ret));
  }

  // 1.2 move semi filter info
  if (NULL != semi_infos){
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos->count(); ++i) {
      SemiInfo *semi = NULL;
      if (OB_ISNULL(semi = semi_infos->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("semi info is null", K(ret), K(semi));
      } else if (OB_FAIL(stmt->remove_semi_info(semi))) {
        LOG_WARN("failed to remove semi info", K(ret));
      } else if (OB_FAIL(view_stmt->add_semi_info(semi))) {
        LOG_WARN("failed to add semi info", K(ret));
      } else if (OB_FAIL(basic_table_ids.push_back(semi->right_table_id_))) {
        LOG_WARN("failed to push back right table id", K(ret));
      }
    }
  }

  // 1.3 move table item
  for (int64_t i = 0; OB_SUCC(ret) && i < basic_table_ids.count(); ++i) {
    uint64_t table_id = basic_table_ids.at(i);
    TableItem *table = NULL;
    ObArray<ColumnItem> column_items;
    ObArray<ObDMLStmt::PartExprItem> part_expr_items;
    ObDMLStmt::CheckConstraintItem check_constraint_item;
    if (OB_ISNULL(table = stmt->get_table_item_by_id(table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null", K(ret), K(table));
    } else if (OB_FAIL(stmt->get_column_items(table_id, column_items))) {
      LOG_WARN("failed to get column items", K(ret));
    } else if (OB_FAIL(stmt->get_part_expr_items(table_id, part_expr_items))) {
      LOG_WARN("failed to get part expr items", K(ret));
    } else if (OB_FAIL(stmt->get_check_constraint_items(table_id, check_constraint_item))) {
      LOG_WARN("failed to get check constraint item", K(ret));
    } else if (OB_FAIL(view_stmt->get_table_items().push_back(table))) {
      LOG_WARN("failed to add table item", K(ret));
    } else if (OB_FAIL(append(view_stmt->get_column_items(), column_items))) {
      LOG_WARN("failed to add column items", K(ret));
    } else if (OB_FAIL(view_stmt->set_part_expr_items(part_expr_items))) {
      LOG_WARN("failed to set part expr items", K(ret));
    } else if (OB_FAIL(view_stmt->set_check_constraint_item(check_constraint_item))) {
      LOG_WARN("failed to add check constraint items", K(ret));
    } else if (OB_FAIL(stmt->remove_table_item(table))) {
      LOG_WARN("failed to remove table item", K(ret));
    } else if (OB_FAIL(stmt->remove_column_item(basic_table_ids.at(i)))) {
      LOG_WARN("failed to remove column item", K(ret));
    } else if (OB_FAIL(stmt->remove_part_expr_items(basic_table_ids.at(i)))) {
      LOG_WARN("failed to remove part expr items", K(ret));
    } else if (OB_FAIL(stmt->remove_check_constraint_item(basic_table_ids.at(i)))) {
      LOG_WARN("failed to remove check constraint item", K(ret));
    }
  }

  // 1.4 construct conditions, group exprs and order items.
  if (OB_FAIL(ret)) {
  } else if (NULL != conditions &&
             OB_FAIL(view_stmt->get_condition_exprs().assign(*conditions))) {
    LOG_WARN("failed to assign conditions", K(ret));
  } else if (NULL != group_exprs &&
             OB_FAIL(view_stmt->get_group_exprs().assign(*group_exprs))) {
    LOG_WARN("failed to assign group exprs", K(ret));
  } else if (NULL != rollup_exprs &&
             OB_FAIL(view_stmt->get_rollup_exprs().assign(*rollup_exprs))) {
    LOG_WARN("failed to assign rollup exprs", K(ret));
  } else if (NULL != having_exprs &&
             OB_FAIL(view_stmt->get_having_exprs().assign(*having_exprs))) {
    LOG_WARN("failed to assign having exprs", K(ret));
  } else if (NULL != order_items &&
             OB_FAIL(view_stmt->get_order_items().assign(*order_items))) {
    LOG_WARN("failed to assign order items", K(ret));

  // 2 extract aggr and winfun
  } else if (NULL != select_exprs &&
             (OB_FAIL(extract_aggr_expr(*select_exprs, view_stmt->get_aggr_items())))) {
    LOG_WARN("failed to extract aggr expr", K(ret));
  } else if (NULL != select_exprs &&
             (OB_FAIL(extract_winfun_expr(*select_exprs, view_stmt->get_window_func_exprs())))) {
    LOG_WARN("failed to extract aggr expr", K(ret));
  } else if (NULL != having_exprs &&
             (OB_FAIL(extract_aggr_expr(*having_exprs, view_stmt->get_aggr_items())))) {
    LOG_WARN("failed to extract aggr expr", K(ret));
  } else if (NULL != having_exprs &&
             (OB_FAIL(extract_winfun_expr(*having_exprs, view_stmt->get_window_func_exprs())))) {
    LOG_WARN("failed to extract aggr expr", K(ret));


  // 3 generate select list and adjust structures
  } else if (OB_FAIL(view_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(view_stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item by id", K(ret));
  } else if (OB_FAIL(generate_select_list(ctx, stmt, view_table, select_exprs))) {
    LOG_WARN("failed to generate select list", K(ret));
  } else if ((stmt->is_delete_stmt() || stmt->is_update_stmt() || stmt->is_merge_stmt()) &&
            OB_FAIL(adjust_updatable_view(*ctx->expr_factory_, static_cast<ObDelUpdStmt*>(stmt),
                                          *view_table, &basic_table_ids_exclude_semi))) {
    LOG_WARN("failed to adjust updatable view", K(ret));
  } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item by id", K(ret));
  } else if (OB_FAIL(stmt->formalize_stmt(ctx->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else {
    if (view_stmt->has_for_update() && lib::is_oracle_mode()) {
      view_table->for_update_ = true;
    }
  }
  return ret;
}

int ObTransformUtils::generate_select_list(ObTransformerCtx *ctx,
                                           ObDMLStmt *stmt,
                                           TableItem *table,
                                           ObIArray<ObRawExpr *> *basic_select_exprs/*= NULL*/)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *view_stmt = NULL;
  ObArray<ObRawExpr *> shared_exprs;
  ObArray<ObRawExpr *> column_exprs;
  ObArray<ObRawExpr *> select_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(table) ||
      OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_) ||
      OB_UNLIKELY(!table->is_generated_table()) ||
      OB_ISNULL(view_stmt = table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), KPC(table));
  } else if (NULL != basic_select_exprs &&
             OB_FAIL(select_exprs.assign(*basic_select_exprs))) {
    LOG_WARN("failed to assign", K(ret));
  // The shared child exprs of basic_select_exprs should be extracted
  } else if (OB_FAIL(extract_shared_exprs(stmt, view_stmt, shared_exprs, basic_select_exprs))) {
    LOG_WARN("failed to extract shared expr", K(ret));
  } else if (OB_FAIL(remove_const_exprs(shared_exprs, shared_exprs))) {
    LOG_WARN("failed to remove const exprs", K(ret));
  } else if (OB_FAIL(append(select_exprs, shared_exprs))) {
    LOG_WARN("failed to append", K(ret));
  } else if (select_exprs.empty()) {
    if (OB_FAIL(create_dummy_select_item(*view_stmt, ctx))) {
      LOG_WARN("Failed to create dummy select item", K(ret));
    }
  } else if (OB_FAIL(create_columns_for_view(ctx, *table, stmt, select_exprs, column_exprs))) {
    LOG_WARN("failed to create columns for view", K(ret));
  } else if (OB_FAIL(stmt->replace_relation_exprs(select_exprs, column_exprs))) {
    LOG_WARN("failed to replace inner stmt expr", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(stmt->adjust_subquery_list())) {
    LOG_WARN("failed to adjust subquery list", K(ret));
  } else if (OB_FAIL(adjust_pseudo_column_like_exprs(*stmt))) {
    LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
  } else if (OB_FAIL(view_stmt->adjust_subquery_list())) {
    LOG_WARN("failed to adjust subquery list", K(ret));
  } else if (OB_FAIL(adjust_pseudo_column_like_exprs(*view_stmt))) {
    LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
  }
  return ret;
}

int ObTransformUtils::extract_shared_exprs(ObDMLStmt *parent,
                                           ObSelectStmt *view_stmt,
                                           ObIArray<ObRawExpr *> &common_exprs,
                                           const ObIArray<ObRawExpr *> *extra_view_exprs/* = NULL*/)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> relation_exprs;
  if (OB_ISNULL(parent) || OB_ISNULL(view_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(parent), K(view_stmt));
  } else if (OB_FAIL(view_stmt->get_column_exprs(relation_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(view_stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (NULL != extra_view_exprs &&
             OB_FAIL(append(relation_exprs, *extra_view_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(extract_shared_exprs(parent, relation_exprs, common_exprs))) {
    LOG_WARN("failed to extract shared exprs from relation exprs", K(ret));
  }
  return ret;
}

int ObTransformUtils::extract_shared_exprs(ObDMLStmt *parent,
                                           ObIArray<ObRawExpr *> &relation_exprs,
                                           ObIArray<ObRawExpr *> &common_exprs)
{
  int ret = OB_SUCCESS;
  int64_t set_size = 32;
  hash::ObHashSet<uint64_t> expr_set;
  if (OB_ISNULL(parent)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(parent));
  } else if (relation_exprs.count() > set_size) {
    set_size = relation_exprs.count();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr_set.create(set_size,
                                "TransExprSet", "TransExprSet"))) {
      LOG_WARN("failed to create expr set", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    if (OB_FAIL(append_hashset(relation_exprs.at(i), expr_set))) {
      LOG_WARN("failed to append hashset", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    relation_exprs.reuse();
    if (OB_FAIL(parent->get_relation_exprs(relation_exprs))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    if (OB_FAIL(find_hashset(relation_exprs.at(i), expr_set, common_exprs))) {
      LOG_WARN("failed to find expr in hashset", K(ret));
    }
  }
  if (expr_set.created()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = expr_set.destroy())) {
      LOG_WARN("failed to destroy expr set", K(ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObTransformUtils::remove_const_exprs(ObIArray<ObRawExpr *> &input_exprs,
                                         ObIArray<ObRawExpr *> &output_exprs)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> tmp;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_exprs.count(); ++i) {
    if (OB_ISNULL(input_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input expr is null", K(ret));
    } else if (input_exprs.at(i)->is_const_expr()) {
      // do nothing
    } else if (OB_FAIL(tmp.push_back(input_exprs.at(i)))) {
      LOG_WARN("failed to push back input expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(output_exprs.assign(tmp))) {
      LOG_WARN("failed to assign tmp expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::append_hashset(ObRawExpr *expr,
                                     hash::ObHashSet<uint64_t> &expr_set)
{
  int ret = OB_SUCCESS;
  uint64_t key = reinterpret_cast<uint64_t>(expr);
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (OB_HASH_EXIST == expr_set.exist_refactored(key)) {
    // do nothing
  } else if (OB_FAIL(expr_set.set_refactored(key))) {
    LOG_WARN("failed to set key", K(ret), K(expr));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(append_hashset(expr->get_param_expr(i),
                                            expr_set)))) {
        LOG_WARN("failed to append hashset", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::find_hashset(ObRawExpr *expr,
                                   hash::ObHashSet<uint64_t> &expr_set,
                                   ObIArray<ObRawExpr *> &common_exprs)
{
  int ret = OB_SUCCESS;
  uint64_t key = reinterpret_cast<uint64_t>(expr);
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (OB_HASH_EXIST == expr_set.exist_refactored(key)) {
    if (OB_FAIL(add_var_to_array_no_dup(common_exprs, expr))) {
      LOG_WARN("failed to append expr", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(find_hashset(expr->get_param_expr(i),
                                          expr_set,
                                          common_exprs)))) {
        LOG_WARN("failed to find hashset", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::generate_col_exprs(ObDMLStmt *stmt,
                                         const ObIArray<TableItem *> &tables,
                                         const ObIArray<ObRawExpr *> &tmp_select_exprs,
                                         const ObIArray<ObRawExpr *> &tmp_column_exprs,
                                         ObIArray<ObRawExpr *> &old_column_exprs,
                                         ObIArray<ObRawExpr *> &new_column_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || (tmp_column_exprs.count() != tmp_select_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected input", K(ret), K(stmt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_select_exprs.count(); ++i) {
      ObRawExpr *tmp_select_expr = NULL;
      ObRawExpr *tmp_column_expr = NULL;
      if (OB_ISNULL(tmp_select_expr = tmp_select_exprs.at(i)) ||
          OB_ISNULL(tmp_column_expr = tmp_column_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected NULL", K(ret), K(tmp_select_expr), K(tmp_column_expr));
      } else {
        if (ob_is_enumset_tc(tmp_select_expr->get_data_type())) {
          OZ(tmp_column_expr->set_enum_set_values(tmp_select_expr->get_enum_set_values()));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < tables.count(); ++j) {
          ObColumnRefRawExpr *col_expr = NULL;
          TableItem *table = tables.at(j);
          uint64_t table_id = OB_INVALID_INDEX;
          ObRawExpr *tmp_raw_expr = NULL;
          if (OB_ISNULL(table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null table", K(ret));
          } else if (!tmp_select_expr->is_column_ref_expr()) {
            ObSEArray<uint64_t, 8> table_ids;
            // joined table allows non-column ref exprs
            if (!table->is_joined_table()) {
            } else if (OB_FAIL(ObRawExprUtils::extract_table_ids(tmp_select_expr, table_ids))) {
              LOG_WARN("failed to extract table ids", K(ret));
            } else if (ObOptimizerUtil::is_subset(table_ids,
                                                  static_cast<JoinedTable *>(table)->single_table_ids_)) {
              if (OB_FAIL(add_var_to_array_no_dup(old_column_exprs, tmp_select_expr))) {
                LOG_WARN("failed to add select expr", K(ret));
              } else if (OB_FAIL(add_var_to_array_no_dup(new_column_exprs, tmp_column_expr))) {
                LOG_WARN("failed to add column expr", K(ret));
              }
            }
          } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr *>(tmp_select_expr))) {
          } else if (OB_FALSE_IT(table_id = table->is_generated_table() ? table->table_id_ :
                                                            col_expr->get_table_id())) {
          } else if (OB_ISNULL(col_expr = stmt->get_column_expr_by_id(table_id,
                                                                  col_expr->get_column_id()))) {
            // do nothing
          } else if (OB_FALSE_IT(tmp_raw_expr = col_expr)) {
          } else if (is_contain(old_column_exprs, tmp_raw_expr) ||
                     is_contain(old_column_exprs, tmp_select_expr)) {
            // since we have multi tables, the iteration here may contain redundant columns
          } else if (OB_FAIL(old_column_exprs.push_back(col_expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else if (OB_FAIL(new_column_exprs.push_back(tmp_column_expr))) {
            LOG_WARN("failed to push back new column expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::extract_right_tables_from_semi_infos(ObDMLStmt *stmt,
                                                           const ObIArray<SemiInfo *> &semi_infos, 
                                                           ObIArray<TableItem *> &tables)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      SemiInfo *semi_info = semi_infos.at(i);
      if (OB_ISNULL(semi_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) &&
                            j < stmt->get_semi_infos().count(); ++j) {
          SemiInfo *sel_semi_info = stmt->get_semi_infos().at(j);
          if (OB_ISNULL(sel_semi_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (semi_info == sel_semi_info) {
            TableItem *table = NULL;
            if (OB_ISNULL(table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret));
            } else if (OB_FAIL(tables.push_back(table))) {
              LOG_WARN("failed to push back table", K(ret));
            }
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::can_push_down_filter_to_table(TableItem &table, bool &can_push)
{
  int ret = OB_SUCCESS;
  can_push = false;
  ObSelectStmt *ref_query = NULL;
  bool has_rownum = false;
  if (!table.is_generated_table()) {
    can_push = false;
  } else if (OB_ISNULL(ref_query = table.ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ref_query));
  } else if (OB_FAIL(ref_query->has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else if (!has_rownum) {
    can_push = !(ref_query->is_set_stmt() ||
                 ref_query->is_hierarchical_query() ||
                 ref_query->has_limit() ||
                 ref_query->has_sequence() ||
                 ref_query->is_contains_assignment() ||
                 ref_query->has_window_function() ||
                 ref_query->has_group_by() ||
                 ref_query->has_rollup());
  }
  return ret;
}

// add limit 1 for semi right table
int ObTransformUtils::add_limit_to_semi_right_table(ObDMLStmt *stmt,
                                                    ObTransformerCtx *ctx,
                                                    SemiInfo *semi_info)
{
  int ret = OB_SUCCESS;
  TableItem *right_table = NULL;
  TableItem *view_item = NULL;
  ObSelectStmt *ref_query = NULL;
  ObConstRawExpr *const_one = NULL;
  ObRawExpr *limit_expr = NULL;
  ObRawExpr *offset_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_) || OB_ISNULL(semi_info) ||
      OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(ctx), K(semi_info));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx->expr_factory_, ObIntType,
                                                          1, const_one))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (!right_table->is_generated_table()) {
    if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx,
                                                          stmt,
                                                          view_item,
                                                          right_table))) {
      LOG_WARN("failed to create empty view", K(ret));
    } else if (OB_FAIL(create_inline_view(ctx,
                                          stmt,
                                          view_item,
                                          right_table))) {
      LOG_WARN("failed to create inline view", K(ret));
    } else if (OB_ISNULL(view_item) || OB_ISNULL(ref_query = view_item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view_item is null", K(ret), K(view_item), K(ref_query));
    } else {
      ref_query->set_limit_offset(const_one, NULL);
    }
  } else if (OB_ISNULL(ref_query = right_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ref query", K(ret), K(ref_query));
  } else if (OB_UNLIKELY(NULL != ref_query->get_limit_percent_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ref query", K(ret), K(ref_query->get_limit_percent_expr()));
  } else if (NULL == ref_query->get_limit_expr()) {
    ref_query->set_limit_offset(const_one, ref_query->get_offset_expr());
  } else if (OB_FAIL(merge_limit_offset(ctx, ref_query->get_limit_expr(), const_one,
                                        ref_query->get_offset_expr(), NULL,
                                        limit_expr, offset_expr))) {
    LOG_WARN("failed to merge limit offset", K(ret));
  } else {
    ref_query->set_limit_offset(limit_expr, offset_expr);
  }
  return ret;
}

//使用 basic/generate other_table 替换 stmt 中 current_table, other_table 需要已经添加到 table item 中
// 主要步骤:
//  1. 移除 current_table 及 column exprs, 如果为 join table, 同时移除所有 child table
//  2. 替换 semi infos 中表信息
//  3. 替换 joined table 中表信息
//  4. 替换 from table 中表信息
int ObTransformUtils::replace_table_in_stmt(ObDMLStmt *stmt,
                                            TableItem *other_table,
                                            TableItem *current_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> table_ids;
  int64_t from_item_idx = OB_INVALID_INDEX;
  if (OB_ISNULL(stmt) || OB_ISNULL(other_table) || OB_ISNULL(current_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!other_table->is_basic_table() &&
             !other_table->is_temp_table() &&
             !other_table->is_generated_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table type", K(ret), K(*other_table));
  } else if (OB_FAIL(remove_tables_from_stmt(stmt, current_table, table_ids))) {
    LOG_WARN("failed to remove tables from stmt", K(ret));
  } else if (OB_FAIL(replace_table_in_semi_infos(stmt, other_table, current_table))) {
    LOG_WARN("failed to replace table in semi infos", K(ret));
  } else if (OB_FAIL(replace_table_in_joined_tables(stmt, other_table, current_table))) {
    LOG_WARN("failed to replace table in joined tables", K(ret));
  } else if (FALSE_IT(from_item_idx = stmt->get_from_item_idx(current_table->table_id_))) {
  } else if (from_item_idx < 0 || from_item_idx > stmt->get_from_item_size()) {
    /*do nothing*/
  } else if (OB_FAIL(stmt->remove_from_item(current_table->table_id_))) {
    LOG_WARN("failed to remove from item", K(ret));
  } else if (OB_FAIL(stmt->add_from_item(other_table->table_id_,
                                         other_table->is_joined_table()))) {
    LOG_WARN("failed to add from item", K(ret));
  }
  return ret;
}

//移除 stmt 中 table_item 及其 column item
int ObTransformUtils::remove_tables_from_stmt(ObDMLStmt *stmt,
                                              TableItem *table_item,
                                              ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or joined table is null.", K(stmt), K(table_item), K(ret));
  } else if (table_item->type_ == TableItem::JOINED_TABLE) {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table_item);
    if (OB_FAIL(SMART_CALL(remove_tables_from_stmt(stmt, joined_table->left_table_, table_ids)))) {
      LOG_WARN("failed to remove tables from stmt", K(ret));
    } else if (OB_FAIL(SMART_CALL(remove_tables_from_stmt(stmt, joined_table->right_table_,
                                                          table_ids)))) {
      LOG_WARN("failed to remove tables from stmt", K(ret));
    } else { /*do nothing.*/ }
  } else if (OB_FAIL(stmt->remove_table_item(table_item))) {
    LOG_WARN("failed to remove table items.", K(ret));
  } else if (OB_FAIL(stmt->remove_column_item(table_item->table_id_))) {
    LOG_WARN("failed to remove column items.", K(ret));
  } else if (OB_FAIL(table_ids.push_back(table_item->table_id_))) {
    LOG_WARN("failed to push back table id", K(ret));
  } else { /* do nothing. */ }
  return ret;
}

// replace current_table with other_table, other_table is a basic/generate table.
int ObTransformUtils::replace_table_in_semi_infos(ObDMLStmt *stmt,
                                                  const TableItem *other_table,
                                                  const TableItem *current_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> table_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(other_table) || OB_ISNULL(current_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(other_table), K(current_table));
  } else if (OB_UNLIKELY(other_table->is_basic_table() && 
                         other_table->is_temp_table() &&
                         other_table->is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected other table item", K(ret), K(other_table->type_));
  } else if (current_table->is_joined_table()) {
    ret = table_ids.assign(static_cast<const JoinedTable*>(current_table)->single_table_ids_);
  } else {
    ret = table_ids.push_back(current_table->table_id_);
  }
  if (OB_SUCC(ret)) {
    ObIArray<SemiInfo*> &semi_infos = stmt->get_semi_infos();
    bool happend = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      if (OB_ISNULL(semi_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (current_table->table_id_ == semi_infos.at(i)->right_table_id_) {
        semi_infos.at(i)->right_table_id_ = other_table->table_id_;
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(semi_infos.at(i)->left_table_ids_,
                                                      table_ids, &happend))) {
        LOG_WARN("failed to remove item", K(ret));
      } else if (!happend) {
        /* do nothing */
      } else if (OB_FAIL(add_var_to_array_no_dup(semi_infos.at(i)->left_table_ids_,
                                                 other_table->table_id_))) {
        LOG_WARN("failed to add table id", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::replace_table_in_joined_tables(ObDMLStmt *stmt,
                                                     TableItem *other_table,
                                                     TableItem *current_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(other_table) || OB_ISNULL(current_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObSEArray<JoinedTable*, 4> new_joined_tables;
    ObIArray<JoinedTable*> &joined_tables = stmt->get_joined_tables();
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_tables.count(); ++i) {
      if (OB_ISNULL(joined_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (current_table == joined_tables.at(i)) {
        if (other_table->is_joined_table() && OB_FAIL(new_joined_tables.push_back(
                                                        static_cast<JoinedTable*>(other_table)))) {
          LOG_WARN("failed to push back table", K(ret));
        } else {/*do nothing*/}
      } else if (OB_FAIL(replace_table_in_joined_tables(joined_tables.at(i), other_table,
                                                        current_table))) {
        LOG_WARN("failed to replace table in joined tables", K(ret));
      } else if (OB_FAIL(new_joined_tables.push_back(joined_tables.at(i)))) {
        LOG_WARN("failed to push back table", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret) && OB_FAIL(stmt->get_joined_tables().assign(new_joined_tables))) {
      LOG_WARN("failed to assign joined tables", K(ret));
    }
  }
  return ret;
}

//替换 table 内部的 current_table 为 other_table
int ObTransformUtils::replace_table_in_joined_tables(TableItem *table,
                                                     TableItem *other_table,
                                                     TableItem *current_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table) || OB_ISNULL(other_table) || OB_ISNULL(current_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table->is_joined_table()) {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table);
    TableItem *left_table = NULL;
    TableItem *right_table = NULL;
    joined_table->single_table_ids_.reset();
    if (OB_ISNULL(left_table = joined_table->left_table_)
        || OB_ISNULL(right_table = joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (current_table == left_table) {
      joined_table->left_table_ = other_table;
    } else if (OB_FAIL(SMART_CALL(replace_table_in_joined_tables(left_table, other_table,
                                                                 current_table)))) {
      LOG_WARN("failed to replace table in joined tables", K(ret));
    } else if (current_table == right_table) {
      joined_table->right_table_ = other_table;
    } else if (OB_FAIL(SMART_CALL(replace_table_in_joined_tables(right_table, other_table,
                                                                 current_table)))) {
      LOG_WARN("failed to replace table in joined tables", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_joined_table_single_table_ids(*joined_table,
                                                         *joined_table->right_table_))) {
      LOG_WARN("add joined table single table ids failed", K(ret));
    } else if (OB_FAIL(add_joined_table_single_table_ids(*joined_table,
                                                         *joined_table->left_table_))) {
      LOG_WARN("add joined table single table ids failed", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::classify_rownum_conds(ObDMLStmt &stmt,
                                            ObIArray<ObRawExpr *> &spj_conds,
                                            ObIArray<ObRawExpr *> &other_conds)
{
  int ret = OB_SUCCESS;
  if (stmt.is_select_stmt()) {
    ObSelectStmt &sel_stmt = static_cast<ObSelectStmt&>(stmt);
    ObSEArray<ObRawExpr *, 4> sel_exprs;
    ObSEArray<ObQueryRefRawExpr *, 4> subqueries;
    if (OB_FAIL(sel_stmt.get_select_exprs(sel_exprs))) {
      LOG_WARN("get select exprs failed", K(ret));
    } else if (OB_FAIL(extract_query_ref_expr(sel_exprs, subqueries, true))) {
      LOG_WARN("extract query ref expr failed", K(ret), K(sel_exprs));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < subqueries.count(); i++) {
        ObRawExpr *subq_expr = subqueries.at(i);
        if (OB_ISNULL(subq_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unepected null", K(ret));
        } else if (OB_FAIL(subq_expr->add_flag(IS_SELECT_REF))) {
          LOG_WARN("failed to add flag", K(ret));
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_condition_size(); ++i) {
    ObRawExpr *cond_expr = NULL;
    bool has = false;
    if (OB_ISNULL(cond_expr = stmt.get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (cond_expr->has_flag(CNT_ROWNUM)) {
      if (OB_FAIL(other_conds.push_back(cond_expr))) {
        LOG_WARN("failed to add condition expr into upper stmt", K(ret));
      }
    } else if (OB_FAIL(ObTransformUtils::contain_select_ref(cond_expr, has))) {
      LOG_WARN("failed to contain select ref", K(ret));
    } else if (has) {
      if (OB_FAIL(other_conds.push_back(cond_expr))) {
        LOG_WARN("failed to push back condition expr", K(ret));
      }
    } else if (OB_FAIL(spj_conds.push_back(cond_expr))) {
      LOG_WARN("failed to add condition expr into spj stmt", K(ret));
    }
  }
  if (stmt.is_select_stmt()) {
    ObSelectStmt &sel_stmt = static_cast<ObSelectStmt&>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt.get_select_item_size(); ++i) {
      ObRawExpr *sel_expr = NULL;
      if (OB_ISNULL(sel_expr = sel_stmt.get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr is null", K(ret));
      } else if (!sel_expr->has_flag(CNT_SUB_QUERY)) {
        // do nothing
      } else if (OB_FAIL(sel_expr->clear_flag(IS_SELECT_REF))) {
        LOG_WARN("failed to add flag", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief rebuild_select_items
 * rebuild select items with all column item exprs whose relation_ids is in
 * input parameter output_rel_ids
 */
int ObTransformUtils::rebuild_select_items(ObSelectStmt &stmt,
                                           ObRelIds &output_rel_ids)
{
  int ret = OB_SUCCESS;
  SelectItem sel_item;
  stmt.get_select_items().reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_column_size(); ++i) {
    ColumnItem *column_item = stmt.get_column_item(i);
    if (OB_ISNULL(column_item) || OB_ISNULL(column_item->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret), K(column_item));
    } else if (column_item->expr_->is_explicited_reference()
        && column_item->expr_->get_relation_ids().is_subset(output_rel_ids)) {
      sel_item.expr_name_ = column_item->column_name_;
      sel_item.alias_name_ = column_item->column_name_;
      sel_item.expr_ = column_item->expr_;
      if (OB_FAIL(stmt.add_select_item(sel_item))) {
        LOG_WARN("failed to add select item", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_explicated_ref_columns(const uint64_t table_id,
                                                 ObDMLStmt *stmt,
                                                 ObIArray<ObRawExpr*> &table_cols) {
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> tmp_exprs;
  if (OB_FAIL(stmt->get_column_exprs(table_id, tmp_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_exprs.count(); i++) {
      ObRawExpr *expr = tmp_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is null", K(ret));
      } else if (expr->is_explicited_reference()) {
        if (OB_FAIL(table_cols.push_back(expr))) {
          LOG_WARN("failed to push back into array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::right_join_to_left(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed in", K(ret));
  } else {
    int64_t N = stmt->get_joined_tables().count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      JoinedTable *cur_joined = stmt->get_joined_tables().at(i);
      if (OB_FAIL(ObTransformUtils::change_join_type(cur_joined))) {
        LOG_WARN("failed to change right outer join to left", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::change_join_type(TableItem *joined_table)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (NULL == joined_table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer passed in", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (joined_table->is_joined_table()) {
    JoinedTable *cur_joined = static_cast<JoinedTable*>(joined_table);
    if (RIGHT_OUTER_JOIN == cur_joined->joined_type_) {
      TableItem *l_child = cur_joined->left_table_;
      cur_joined->left_table_ = cur_joined->right_table_;
      cur_joined->right_table_ = l_child;
      cur_joined->joined_type_ = LEFT_OUTER_JOIN;
    }
    if (OB_FAIL(SMART_CALL(change_join_type(cur_joined->left_table_)))) {
      LOG_WARN("failed to change left child type", K(ret));
    } else if (OB_FAIL(SMART_CALL(change_join_type(cur_joined->right_table_)))) {
      LOG_WARN("failed to change right child type", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::build_const_expr_for_count(ObRawExprFactory &expr_factory,
                                                 const int64_t value,
                                                 ObConstRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (1 != value && 0 != value) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected const value for count", K(ret), K(value));
  } else if (!lib::is_oracle_mode()) {
    ret = ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, value, expr);
  } else if (1 == value) {
    ret = ObRawExprUtils::build_const_number_expr(expr_factory, ObNumberType,
                                                  number::ObNumber::get_positive_one(), expr);
  } else if (0 == value) {
    ret = ObRawExprUtils::build_const_number_expr(expr_factory, ObNumberType,
                                                  number::ObNumber::get_zero(), expr);
  }
  return ret;
}

// case expr is not null then then_value else default_expr end
int ObTransformUtils::build_case_when_expr(ObDMLStmt &stmt,
                                           ObRawExpr *expr,
                                           ObRawExpr *then_expr,
                                           ObRawExpr *default_expr,
                                           ObRawExpr *&out_expr,
                                           ObTransformerCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObCaseOpRawExpr *case_expr = NULL;
  ObOpRawExpr *is_not_expr = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_) || OB_ISNULL(ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr is null", K(ret), K(ctx));
  } else if (OB_FAIL(ctx->expr_factory_->create_raw_expr(T_OP_CASE, case_expr))) {
    LOG_WARN("failed to create case expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx, &stmt, expr, is_not_expr))) {
    LOG_WARN("failed to build is not null expr", K(ret));
  } else if (OB_FAIL(case_expr->add_when_param_expr(is_not_expr))) {
    LOG_WARN("failed to add when param expr", K(ret));
  } else if (OB_FAIL(case_expr->add_then_param_expr(then_expr))) {
    LOG_WARN("failed to add then expr", K(ret));
  } else if (FALSE_IT(case_expr->set_default_param_expr(default_expr))) {
    // do nothing
  } else if (OB_FAIL(case_expr->formalize(ctx->session_info_))) {
    LOG_WARN("failed to formalize case expr", K(ret));
  } else if (OB_FAIL(case_expr->pull_relation_id())) {
    LOG_WARN("failed to pull relation id and levels", K(ret));
  } else {
    out_expr = case_expr;
  }
  return ret;
}

int ObTransformUtils::compare_const_expr_result(ObTransformerCtx *ctx,
                                                ObRawExpr *expr,
                                                ObItemType op_type,
                                                int64_t value,
                                                bool &is_true)
{
  int ret = OB_SUCCESS;
  is_true = false;
  if (OB_ISNULL(ctx) || OB_ISNULL(expr) ||
      OB_ISNULL(ctx->exec_ctx_) || OB_ISNULL(ctx->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!IS_COMMON_COMPARISON_OP(op_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op type", K(ret));
  } else if (OB_UNLIKELY(!expr->is_static_const_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr", K(ret), K(*expr));
  } else {
    ObObj obj_value;
    bool got_result = true;
    if (expr->get_result_type().is_integer_type() ||
                expr->get_result_type().is_number()) {
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx->exec_ctx_,
                                                            expr,
                                                            obj_value,
                                                            got_result,
                                                            *ctx->allocator_,
                                                            false))) {
        LOG_WARN("failed to calc const or calculable expr", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr result type", K(*expr), K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (!got_result) {
    } else if (obj_value.is_null() || obj_value.is_null_oracle()) {
      is_true = false;
    } else {
      int res = 0;
      number::ObNumber number;
      int64_t expr_val = 0;
      if (obj_value.is_integer_type()) {
        expr_val = obj_value.get_int();
        if (expr_val > value) res = 1;
        else if (expr_val == value) res = 0;
        else if (expr_val < value) res = -1;
      } else if (OB_FAIL(obj_value.get_number(number))) {
        LOG_WARN("unexpected value type", K(ret), K(obj_value));
      } else {
        res = number.compare(value);
      }
      is_true = (op_type == T_OP_GT ? (res > 0 ? true : false) :
                (op_type == T_OP_GE ? (res >= 0 ? true : false) :
                (op_type == T_OP_EQ ? (res == 0 ? true : false) :
                (op_type == T_OP_LT ? (res < 0 ? true : false) :
                (op_type == T_OP_LE ? (res <= 0 ? true : false) :
                (op_type == T_OP_NE ? (res != 0 ? true : false) : false))))));
    }
  }
  return ret;
}


int ObTransformUtils::compare_const_expr_result(ObTransformerCtx *ctx,
                                                ObRawExpr &left_expr,
                                                ObItemType op_type,
                                                ObRawExpr &right_expr,
                                                bool &is_true)
{
  int ret = OB_SUCCESS;
  is_true = false;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->exec_ctx_) ||
      OB_ISNULL(ctx->exec_ctx_->get_physical_plan_ctx()) ||
      OB_ISNULL(ctx->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!IS_COMMON_COMPARISON_OP(op_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op type", K(ret));
  } else if (OB_UNLIKELY(!left_expr.is_static_const_expr()) ||
             OB_UNLIKELY(!right_expr.is_static_const_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr", K(ret));
  } else {
    ObObj left_obj_value;
    ObObj right_obj_value;
    bool left_got_result = true;
    bool right_got_result = true;
    if (left_expr.get_result_type().is_numeric_type() || //only integer and number now
                left_expr.get_result_type().is_number()) {
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx->exec_ctx_,
                                                            &left_expr,
                                                            left_obj_value,
                                                            left_got_result,
                                                            *ctx->allocator_,
                                                            false))) {
        LOG_WARN("failed to calc const or calculable expr", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr result type", K(left_expr), K(ret));
    }
    if (OB_FAIL(ret) || !left_got_result) {
    } else if (right_expr.get_result_type().is_numeric_type() ||
                right_expr.get_result_type().is_number()) {
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx->exec_ctx_,
                                                            &right_expr,
                                                            right_obj_value,
                                                            right_got_result,
                                                            *ctx->allocator_,
                                                            false))) {
        LOG_WARN("failed to calc const or calculable expr", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr result type", K(right_expr), K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (!left_got_result || !right_got_result) {
    } else if (left_obj_value.is_null() || left_obj_value.is_null_oracle() ||
              right_obj_value.is_null() || right_obj_value.is_null_oracle()) {
      is_true = false;
    } else {
      int res = 0;
      number::ObNumber left_number;
      number::ObNumber right_number;
      if (left_obj_value.is_number() && OB_FAIL(left_obj_value.get_number(left_number))) {
        LOG_WARN("unexpected value type", K(ret), K(left_obj_value));
      } else if (right_obj_value.is_number() && OB_FAIL(right_obj_value.get_number(right_number))) {
        LOG_WARN("unexpected value type", K(ret), K(right_obj_value));
      } else if (left_obj_value.is_number() && right_obj_value.is_number()) {
        res = left_number.compare(right_number);
      } else if (left_obj_value.is_integer_type() && right_obj_value.is_number()) {
        int64_t left_val = left_obj_value.get_int();
        res = -right_number.compare(left_val);
      } else if (left_obj_value.is_number() && right_obj_value.is_integer_type()) {
        int64_t right_val = right_obj_value.get_int();
        res = left_number.compare(right_val);
      } else if (left_obj_value.is_integer_type() && right_obj_value.is_integer_type()) {
        int64_t left_val = left_obj_value.get_int();
        int64_t right_val = right_obj_value.get_int();
        res = left_val > right_val ? 1 : (left_val == right_val ? 0 : -1);
      } else {
        // other cases is not considerred.
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result type is not valid", K(ret));
      }
      if (OB_SUCC(ret)) {
        is_true = (op_type == T_OP_GT ? (res > 0 ? true : false) :
                  (op_type == T_OP_GE ? (res >= 0 ? true : false) :
                  (op_type == T_OP_EQ ? (res == 0 ? true : false) :
                  (op_type == T_OP_LT ? (res < 0 ? true : false) :
                  (op_type == T_OP_LE ? (res <= 0 ? true : false) :
                  (op_type == T_OP_NE ? (res != 0 ? true : false) : false))))));
      }
    }
  }
  return ret;
}


int ObTransformUtils::merge_limit_as_zero(ObTransformerCtx &ctx,
                                          ObRawExpr *view_limit,
                                          ObRawExpr *upper_limit,
                                          ObRawExpr *view_offset,
                                          ObRawExpr *upper_offset,
                                          ObRawExpr *&limit_expr,
                                          ObRawExpr *&offset_expr,
                                          bool &is_valid)
{
  int ret = OB_SUCCESS;
  // offset is null or limit is null or a2-b1 < 1 or limit_expr < 1 or upper_limit < 1
  ObConstRawExpr *one_expr = NULL;
  ObConstRawExpr *zero_expr = NULL;
  ObRawExpr *minus_expr = NULL;
  ObSEArray<ObRawExpr *, 4> params;
  bool is_not_neg = false;
  is_valid = false;
  if (OB_ISNULL(ctx.expr_factory_) || OB_ISNULL(ctx.session_info_)
      || OB_ISNULL(ctx.exec_ctx_) || OB_ISNULL(ctx.exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (view_limit == NULL) {
  } else if (OB_FAIL(compare_const_expr_result(&ctx, view_limit, T_OP_GE, 1, is_not_neg))) {
    LOG_WARN("failed to get_expr_int_value", K(ret));
  } else {
    ObRawExpr *cmp_expr = NULL;
    if (one_expr == NULL && OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx.expr_factory_,
                                                            ObIntType,
                                                            1,
                                                            one_expr))) {
      LOG_WARN("create expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx.expr_factory_, ctx.session_info_, T_OP_GE, cmp_expr,
                                                          view_limit, one_expr))) {
      LOG_WARN("create_double_op_expr_failed", K(ret));
    } else if (OB_FAIL(params.push_back(cmp_expr))) {
      LOG_WARN("push back failed", K(ret));
    } else if (!is_not_neg) {
      is_valid = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (upper_limit == NULL) {
  } else if (OB_FAIL(compare_const_expr_result(&ctx, upper_limit, T_OP_GE, 1, is_not_neg))) {
    LOG_WARN("failed to get_expr_int_value", K(ret));
  } else {
    ObRawExpr *cmp_expr = NULL;
    if (one_expr == NULL && OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx.expr_factory_,
                                                            ObIntType,
                                                            0,
                                                            one_expr))) {
      LOG_WARN("create expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx.expr_factory_, ctx.session_info_, T_OP_GE, cmp_expr,
                                                          upper_limit, one_expr))) {
      LOG_WARN("create_double_op_expr_failed", K(ret));
    } else if (OB_FAIL(params.push_back(cmp_expr))) {
      LOG_WARN("push back failed", K(ret));
    } else if (!is_not_neg) {
      is_valid = true;
    }
  }

  if (OB_FAIL(ret) || view_limit == NULL || upper_offset == NULL) {
    //do nothing
  } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx.expr_factory_,
                                                        ctx.session_info_, T_OP_MINUS,
                                                        minus_expr, view_limit,
                                                        upper_offset))) {
    LOG_WARN("failed to create double op expr", K(ret));
  } else if (OB_FAIL(compare_const_expr_result(&ctx, minus_expr, T_OP_GE, 1, is_not_neg))) {
    LOG_WARN("get expr int value faield", K(ret));
  } else if (!is_not_neg) {
    ObRawExpr *cmp_expr = NULL;
    if (one_expr == NULL && OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx.expr_factory_,
                                                                          ObIntType,
                                                                          1,
                                                                          one_expr))) {
      LOG_WARN("create expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx.expr_factory_, ctx.session_info_, T_OP_GE, cmp_expr,
                                                              minus_expr, one_expr))) {
      LOG_WARN("create_double_op_expr_failed", K(ret));
    } else if (OB_FAIL(params.push_back(cmp_expr))) {
      LOG_WARN("push back failed", K(ret));
    } else {
      is_valid = true;
    }
  }

  ObRawExpr *and_expr = NULL;
  if (OB_FAIL(ret)) {
  } else if (params.count() == 0) {
    //do nothing
  } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*(ctx.expr_factory_), params, and_expr))) {
    LOG_WARN("build and expr failed", K(ret));
  } else if (OB_FAIL(and_expr->formalize(ctx.session_info_))) {
    LOG_WARN("formalize expr failed", K(ret));
  } else if (is_valid) {
    if (OB_FAIL(add_param_bool_constraint(&ctx, and_expr, false))) {
      LOG_WARN("build cons failed", K(ret));
    } else if (zero_expr == NULL && OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx.expr_factory_,
                                                            ObIntType,
                                                            0,
                                                            zero_expr))) {
      LOG_WARN("create expr failed", K(ret));
    } else {
      limit_expr = zero_expr;
      offset_expr = NULL;
    }
  } else if (OB_FAIL(add_param_bool_constraint(&ctx, and_expr, true))) {
    LOG_WARN("build cons failed", K(ret));
  }
  return ret;
}

// row expr: T_OP_ROW(param_expr1,...param_exprN)
int ObTransformUtils::build_row_expr(ObRawExprFactory& expr_factory, common::ObIArray<ObRawExpr*>& param_exprs, ObOpRawExpr*& row_expr) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, row_expr))) {
    LOG_WARN("failed to create op row expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); i++) {
      if (OB_FAIL(row_expr->add_param_expr(param_exprs.at(i)))) {
        LOG_WARN("failed to add param expr", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    row_expr = NULL;
  }
  return ret;
}

int ObTransformUtils::query_cmp_to_value_cmp(const ObItemType cmp_type, ObItemType& new_type)
{
  int ret = OB_SUCCESS;
  switch (cmp_type) {
    case T_OP_SQ_EQ:
      new_type = T_OP_EQ;
      break;
    case T_OP_SQ_NSEQ:
      new_type = T_OP_NSEQ;
      break;
    case T_OP_SQ_LE:
      new_type = T_OP_LE;
      break;
    case T_OP_SQ_LT:
      new_type = T_OP_LT;
      break;
    case T_OP_SQ_GE:
      new_type = T_OP_GE;
      break;
    case T_OP_SQ_GT:
      new_type = T_OP_GT;
      break;
    case T_OP_SQ_NE:
      new_type = T_OP_NE;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      new_type = T_INVALID;
      break;
  }
  return ret;
}

//  select ... from (select ... limit a1 offset b1) limit a2 offset b2;
//  select ... from ... limit min(a1 - b2, a2) offset b1 + b2;
/*
         |--res--|  res=a1-b2   or a2
 ---|----|-------|-----------|-----
    b1 b1+b2    b1+a1    b1+b2+a2
*/
int ObTransformUtils::merge_limit_offset(ObTransformerCtx *ctx,
                                         ObRawExpr *view_limit,
                                         ObRawExpr *upper_limit,
                                         ObRawExpr *view_offset,
                                         ObRawExpr *upper_offset,
                                         ObRawExpr *&limit_expr,
                                         ObRawExpr *&offset_expr)
{
  int ret = OB_SUCCESS;
  offset_expr = NULL;
  limit_expr = NULL;
  ObRawExpr *calc_limit_expr = NULL;
  bool is_null_value = false;
  bool set_offset_null = false;
  bool trans_limit_zero = false;
  const ParamStore *param_store = NULL;
  bool is_valid_zero = false;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_) || OB_ISNULL(ctx->session_info_)
      || OB_ISNULL(ctx->exec_ctx_) || OB_ISNULL(ctx->exec_ctx_->get_physical_plan_ctx())
      || OB_ISNULL(param_store = &ctx->exec_ctx_->get_physical_plan_ctx()->get_param_store())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  // try merge
  }
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(merge_limit_as_zero(*ctx, view_limit, upper_limit, view_offset,
                                          upper_offset, limit_expr, offset_expr, is_valid_zero))) {
    LOG_WARN("merge limit as zero failed", K(ret));
  } else if (is_valid_zero) {
    LOG_TRACE("trans to zero limit", K(ret));
  } else {
    if (NULL == view_offset) { //merge offset
      offset_expr = upper_offset;
    } else if (NULL == upper_offset) {
      offset_expr = view_offset;
      //b1+b2
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx->expr_factory_, ctx->session_info_,
                                                            T_OP_ADD, offset_expr, view_offset,
                                                            upper_offset))) {
      LOG_WARN("failed to create double op expr", K(ret));
    }


    //merge limit
    //calc a2 - b1 -> limit_expr
    if (OB_FAIL(ret)) {
    } else if (NULL == view_limit) { //  select ... from (select ... offset ?) limit ? offset ?;
      limit_expr = upper_limit;
    } else if (NULL == upper_limit && NULL == upper_offset) { //  select ... from (select ... limit a1 offset ?) ;
      limit_expr = view_limit;
    } else if (NULL == upper_offset) {//same as zero //  select ... from (select ... limit a1 offset ?) limit a2;
      calc_limit_expr = view_limit; // view_limit - 0 = view_limit = minus_expr;
      ////get a1-b2
    //} else if (view_limit == NULL || upper_offset == NULL) {
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx->expr_factory_,
                                                          ctx->session_info_, T_OP_MINUS,
                                                          calc_limit_expr, view_limit,
                                                          upper_offset))) {
      LOG_WARN("create double op expr", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if ( upper_limit == NULL && calc_limit_expr != NULL) {
      limit_expr = calc_limit_expr;
      bool is_true = false;
      ObRawExpr *cmp_expr = NULL;
      ObConstRawExpr *zero_expr = NULL;

      if (!lib::is_oracle_mode() && OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx->expr_factory_,
                                                        ObIntType,
                                                        0,
                                                        zero_expr))) {
          LOG_WARN("create zero expr failed", K(ret));
      } else if (lib::is_oracle_mode() && OB_FAIL(ObRawExprUtils::build_const_number_expr(*ctx->expr_factory_,
                                                                                          ObNumberType,
                                                                                          ObNumber::get_zero(),
                                                                                          zero_expr))) {
        LOG_WARN("create zero expr failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx->expr_factory_, ctx->session_info_,
                                                                T_OP_GE, cmp_expr, calc_limit_expr, zero_expr))) {
        LOG_WARN("create double op expr failed", K(ret));
      } else if (OB_FAIL(compare_const_expr_result(ctx, *calc_limit_expr, T_OP_GE, *zero_expr, is_true))) {
        LOG_WARN("compare const expr is failed", K(ret));
      } else if (is_true) {
        limit_expr = calc_limit_expr;
        if (OB_FAIL(add_param_bool_constraint(ctx, cmp_expr, true))) {
          LOG_WARN("add cons failed", K(ret));
        }
      } else {
        limit_expr = zero_expr;
        if (OB_FAIL(add_param_bool_constraint(ctx, cmp_expr, false))) {
          LOG_WARN("add cons failed", K(ret));
        }
      }
    } else if ( upper_limit != NULL && calc_limit_expr != NULL) {
      bool is_true = false;
      ObRawExpr *cmp_expr = NULL;
      if (OB_FAIL(compare_const_expr_result(ctx, *upper_limit, T_OP_LE, *calc_limit_expr, is_true))) {
        LOG_WARN("compare const expr is failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx->expr_factory_, ctx->session_info_,
                                                                T_OP_LE, cmp_expr, upper_limit, calc_limit_expr))) {
        LOG_WARN("create double op expr failed", K(ret));
      } else if (is_true) {
        limit_expr = upper_limit;
        if (OB_FAIL(add_param_bool_constraint(ctx, cmp_expr, true))) {
          LOG_WARN("add cons failed", K(ret));
        }
      } else {
        limit_expr = calc_limit_expr;
        if (OB_FAIL(add_param_bool_constraint(ctx, cmp_expr, false))) {
          LOG_WARN("add cons failed", K(ret));
        }
      }
    }
  }


  if (OB_SUCC(ret)) {
  //添加 cast 作用:
  //  1.对于新引擎, 必须保证输出结果为 int
    ObExprResType dst_type;
    dst_type.set_int();
    ObSysFunRawExpr *cast_expr = NULL;
    if (NULL != limit_expr) {
      if (limit_expr->get_result_type().is_int()) {
        //do nothing
      } else {
        OZ(ObRawExprUtils::create_cast_expr(*ctx->expr_factory_, limit_expr, dst_type,
                                            cast_expr, ctx->session_info_));
        CK(NULL != cast_expr);
        if (OB_SUCC(ret)) {
          limit_expr = cast_expr;
        }
      }
    }
    if (OB_SUCC(ret) && NULL != offset_expr) {
      if (offset_expr->get_result_type().is_int()) {
        //do nothing
      } else {
        OZ(ObRawExprUtils::create_cast_expr(*ctx->expr_factory_, offset_expr, dst_type,
                                            cast_expr, ctx->session_info_));
        CK(NULL != cast_expr);
        if (OB_SUCC(ret)) {
          offset_expr = cast_expr;
        }
      }
    }
  }
  return ret;
}

/**
 * @brief get_stmt_limit_value
 * get value of stmt limit expr
 * limit[out]: value of limit expr, -1 represent stmt not has limit,
 *             -2 represent limit value is not a interger
 *             -3 represent has offset, no limit
 *             -4 represent limit value is percent
 */
int ObTransformUtils::get_stmt_limit_value(const ObDMLStmt &stmt, int64_t &limit)
{
  int ret = OB_SUCCESS;
  limit = -1;
  const ObRawExpr *limit_expr = NULL;
  const ObRawExpr *percent_expr = NULL;
  if (!stmt.has_limit()) {
    // do nothing
  } else if (OB_NOT_NULL(percent_expr = stmt.get_limit_percent_expr())) {
    limit = -4;
  } else if (OB_ISNULL(limit_expr = stmt.get_limit_expr())) {
    limit = -3;
  } else if (limit_expr->is_const_raw_expr()) {
    const ObObj &limit_value = static_cast<const ObConstRawExpr*>(limit_expr)->get_value();
    if (limit_value.is_integer_type()) {
      limit = limit_value.get_int();
    } else {
      limit = -2;
    }
  } else {
    limit = -2;
  }
  return ret;
}

// int ObTransformUtils::formalize_limit_expr(ObD)
// {
//   int ret = OB_SUCCESS;

//   return ret;
// }

/**
 * @brief check_limit_value
 * 检查limit的值是否是指定value
 * 如果add_param_constraint为true
 * 需要给定query_ctx，
 * 表示加入常量约束到query context
 */
int ObTransformUtils::check_limit_value(const ObDMLStmt &stmt,
                                        ObExecContext *exec_ctx,
                                        ObIAllocator *allocator,
                                        int64_t limit,
                                        bool &is_equal,
                                        ObPCConstParamInfo &const_param_info)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *limit_expr = NULL;
  ObObj target_value;
  target_value.set_int(ObIntType, limit);
  is_equal = false;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null allocator", K(ret));
  } else if (!stmt.has_limit()) {
    // do nothing
  } else if (OB_NOT_NULL(stmt.get_limit_percent_expr())) {
    //do nothing
  } else if (OB_ISNULL(limit_expr = stmt.get_limit_expr())) {
    //do nothing
  } else if (limit_expr->is_static_scalar_const_expr()) {
    ObSEArray<ObRawExpr*, 4> params;
    ObRawExpr* param_expr = NULL;
    ObConstRawExpr *const_expr = NULL;
    ObObj result;
    bool got_result = false;
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(
                                    exec_ctx,
                                    limit_expr,
                                    result,
                                    got_result,
                                    *allocator))) {
      LOG_WARN("Failed to calc const or calculable expr", K(ret));
    } else if (!got_result) {
      //do nothing
    } else if (target_value.is_invalid_type() ||
               result.is_invalid_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected invalid type", K(ret));
    } else if (!target_value.can_compare(result) ||
               0 != target_value.compare(result)) {
      //do nothing
    } else if (OB_FAIL(ObRawExprUtils::extract_params(const_cast<ObRawExpr*>(limit_expr), params))) {
      LOG_WARN("failed to extract params", K(ret));
    } else if (params.empty()) {
      is_equal = true;
    } else if (1 != params.count()) {
      //如果是limit ? + ?，没有办法抽取约束，则这种情况下返回false，防止误命中计划
    } else if (OB_ISNULL(param_expr = params.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (T_QUESTIONMARK != param_expr->get_expr_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect questionmark expr", K(*param_expr), K(ret));
    } else if (OB_FALSE_IT(const_expr = static_cast<ObConstRawExpr*>(param_expr))) {
    } else if (OB_FAIL(const_param_info.const_idx_.push_back(const_expr->get_value().get_unknown()))) {
      LOG_WARN("failed to push back param idx", K(ret));
    } else if (OB_FAIL(const_param_info.const_params_.push_back(target_value))) {
      LOG_WARN("failed to push back value", K(ret));
    } else {
      is_equal = true;
    }
  } else if (limit_expr->is_const_raw_expr()) {
    const ObObj &limit_value = static_cast<const ObConstRawExpr*>(limit_expr)->get_value();
    if (limit_value.is_invalid_type() ||
        target_value.is_invalid_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected invalid type", K(ret));
    } else if (!limit_value.can_compare(target_value) ||
               0 != limit_value.compare(target_value)) {
    } else {
      is_equal = true;
    }
  }
  return ret;
}

int ObTransformUtils::convert_column_expr_to_select_expr(const common::ObIArray<ObRawExpr*> &column_exprs,
                                                         const ObSelectStmt &inner_stmt,
                                                         common::ObIArray<ObRawExpr*> &select_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *outer_expr = NULL;
  ObRawExpr *inner_expr = NULL;
  ObColumnRefRawExpr *column_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); i++) {
    int64_t pos = OB_INVALID_ID;
    if (OB_ISNULL(outer_expr = column_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_UNLIKELY(!outer_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr type", K(ret));
    } else if (FALSE_IT(column_expr = static_cast<ObColumnRefRawExpr*>(outer_expr))) {
      /*do nothing*/
    } else if (FALSE_IT(pos = column_expr->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
      /*do nothing*/
    } else if (OB_UNLIKELY(pos < 0 || pos >= inner_stmt.get_select_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid array pos", K(pos), K(inner_stmt.get_select_item_size()), K(ret));
    } else if (OB_ISNULL(inner_expr = inner_stmt.get_select_item(pos).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(select_exprs.push_back(inner_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformUtils::convert_set_op_expr_to_select_expr(const common::ObIArray<ObRawExpr*> &set_op_exprs,
                                                         const ObSelectStmt &inner_stmt,
                                                         common::ObIArray<ObRawExpr*> &select_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *outer_expr = NULL;
  ObRawExpr *inner_expr = NULL;
  ObSetOpRawExpr *set_op_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < set_op_exprs.count(); i++) {
    int64_t pos = OB_INVALID_ID;
    if (OB_ISNULL(outer_expr = set_op_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_UNLIKELY(!outer_expr->is_set_op_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr type", K(ret));
    } else if (FALSE_IT(set_op_expr = static_cast<ObSetOpRawExpr*>(outer_expr))) {
      /*do nothing*/
    } else if (FALSE_IT(pos = set_op_expr->get_idx())) {
      /*do nothing*/
    } else if (OB_UNLIKELY(pos < 0 || pos >= inner_stmt.get_select_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid array pos", K(pos), K(inner_stmt.get_select_item_size()), K(ret));
    } else if (OB_ISNULL(inner_expr = inner_stmt.get_select_item(pos).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(select_exprs.push_back(inner_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformUtils::convert_select_expr_to_column_expr(const common::ObIArray<ObRawExpr*> &select_exprs,
                                                        const ObSelectStmt &inner_stmt,
                                                        ObDMLStmt &outer_stmt,
                                                        uint64_t table_id,
                                                        common::ObIArray<ObRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    ObRawExpr *expr = NULL;
    ObColumnRefRawExpr *col = NULL;
    int64_t idx = -1;
    bool find = false;
    if (OB_ISNULL(expr = select_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < inner_stmt.get_select_item_size(); ++j) {
      if (expr == inner_stmt.get_select_item(j).expr_) {
        find = true;
        idx = j;
      }
    }
    uint64_t column_id = idx + OB_APP_MIN_COLUMN_ID;
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (!find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find select expr inner stmt", K(ret));
    } else if (OB_ISNULL(col = outer_stmt.get_column_expr_by_id(table_id, column_id))) {
      //do nothing
    } else if (OB_FAIL(column_exprs.push_back(col))) {
      LOG_WARN("failed to push back column expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::pull_up_subquery(ObDMLStmt *parent_stmt,
                                       ObSelectStmt *child_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(parent_stmt), K(child_stmt), K(ret));
  } else {
    const ObIArray<ObQueryRefRawExpr*> &subquery_exprs = child_stmt->get_subquery_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); ++i) {
      ObQueryRefRawExpr *query_ref = subquery_exprs.at(i);
      if (OB_ISNULL(query_ref) || OB_ISNULL(query_ref->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(query_ref), K(ret));
      } else if (OB_FAIL(parent_stmt->add_subquery_ref(query_ref))) {
        LOG_WARN("failed to add subquery ref", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTransformUtils::get_subquery_expr_from_joined_table(ObDMLStmt *stmt,
                                                          ObIArray<ObQueryRefRawExpr *> &subqueries)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> on_conditions;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_joined_tables().count(); ++i) {
    JoinedTable *joined_table = stmt->get_joined_tables().at(i);
    if (OB_FAIL(get_on_condition(joined_table, on_conditions))) {
      LOG_WARN("failed to extract query ref exprs", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(extract_query_ref_expr(on_conditions, subqueries))) {
      LOG_WARN("failed to extract query ref expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_on_conditions(ObDMLStmt &stmt,
                                        ObIArray<ObRawExpr *> &conditions)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_from_item_size(); ++i) {
    FromItem &from_item = stmt.get_from_item(i);
    JoinedTable *joined_table = NULL;
    if (!from_item.is_joined_) {
      //do nothing
    } else if (OB_ISNULL(joined_table = stmt.get_joined_table(from_item.table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null", K(ret));
    } else if (OB_FAIL(get_on_condition(joined_table, conditions))) {
      LOG_WARN("failed to get on condition", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_on_condition(TableItem *table_item,
                                       ObIArray<ObRawExpr *> &conditions)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table item", K(ret));
  } else if (!table_item->is_joined_table()) {
    // do nothing
  } else {
    JoinedTable *joined_table = static_cast<JoinedTable *>(table_item);
    if (OB_FAIL(append(conditions, joined_table->get_join_conditions()))) {
      LOG_WARN("failed to append join conditions", K(ret));
    } else if (OB_FAIL(get_on_condition(joined_table->left_table_, conditions))) {
      LOG_WARN("failed to get on condition", K(ret));
    } else if (OB_FAIL(get_on_condition(joined_table->right_table_, conditions))) {
      LOG_WARN("failed to get on condition", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_semi_conditions(ObIArray<SemiInfo *> &semi_infos,
                                          ObIArray<ObRawExpr *> &conditions)
{
  int ret = OB_SUCCESS;
  SemiInfo *semi_info = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
    if (OB_ISNULL(semi_info = semi_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(append(conditions, semi_info->semi_conditions_))) {
      LOG_WARN("failed to append semi condition", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::set_limit_expr(ObDMLStmt *stmt, ObTransformerCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *new_limit_count_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(stmt), K(ctx), K(ctx->expr_factory_));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx->expr_factory_,
                                                          ObIntType,
                                                          1,
                                                          new_limit_count_expr))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(new_limit_count_expr->formalize(ctx->session_info_))) {
    LOG_WARN("failed to formalize session info", K(ret));
  } else {
    stmt->set_limit_offset(new_limit_count_expr, stmt->get_offset_expr());
  }
  return ret;
}

int ObTransformUtils::make_pushdown_limit_count(ObRawExprFactory &expr_factory,
                                                const ObSQLSessionInfo &session,
                                                ObRawExpr *limit_count,
                                                ObRawExpr *limit_offset,
                                                ObRawExpr *&pushdown_limit_count)
{
  int ret = OB_SUCCESS;
  pushdown_limit_count = NULL;
  if (NULL == limit_offset) {
    pushdown_limit_count = limit_count;
  } else {
    OZ(ObRawExprUtils::create_double_op_expr(expr_factory,
                                             &session,
                                             T_OP_ADD,
                                             pushdown_limit_count,
                                             limit_count,
                                             limit_offset));
    CK(NULL != pushdown_limit_count);
    OZ(pushdown_limit_count->formalize(&session));
    if (OB_SUCC(ret)
        && !pushdown_limit_count->get_result_type().is_integer_type()) {
      // Cast to integer in static typing engine if needed.
      ObExprResType dst_type;
      dst_type.set_int();
      ObSysFunRawExpr *cast_expr = NULL;
      OZ(ObRawExprUtils::create_cast_expr(
              expr_factory, pushdown_limit_count, dst_type, cast_expr, &session));
      CK(NULL != cast_expr);
      OZ(cast_expr->formalize(&session));
      if (OB_SUCC(ret)) {
        pushdown_limit_count = cast_expr;
      }
    }
  }
  return ret;
}

//realize set stmt output unique
/*@brief, recursive_set_stmt_unique to set stmt unique
* 注意：使用之前需要调用check_can_set_stmt_unique函数确保select_stmt不存在union all,
*      因为unoin all不能通过添加主键方式保证输出唯一
 */
int ObTransformUtils::recursive_set_stmt_unique(ObSelectStmt *select_stmt,
                                                ObTransformerCtx *ctx,
                                                bool ignore_check_unique,/*default false */
                                                ObIArray<ObRawExpr *> *unique_keys)
{
  int ret = OB_SUCCESS;
  if (NULL != unique_keys) {
    unique_keys->reuse();
  }
  bool is_unique = false;
  ObSqlBitSet<> origin_output_rel_ids;
  ObSEArray<ObRawExpr*, 4> added_unique_keys;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(select_stmt), K(ctx));
  } else if (!ignore_check_unique && (OB_FAIL(check_stmt_unique(select_stmt,
                                                                ctx->session_info_,
                                                                ctx->schema_checker_,
                                                                true /* strict */,
                                                                is_unique)))) {
    LOG_WARN("failed to check stmt unique", K(ret));
  } else if (is_unique) {
    if (NULL != unique_keys && OB_FAIL(get_unique_keys_from_unique_stmt(select_stmt,
                                                                        ctx->expr_factory_,
                                                                        *unique_keys,
                                                                        added_unique_keys))) {
      LOG_WARN("failed to get unique keys from unique stmt", K(ret));
    }
  } else if (OB_FAIL(select_stmt->get_from_tables(origin_output_rel_ids))) {
    LOG_WARN("failed to get output rel ids", K(ret));
  } else {
    ObIArray<TableItem *> &table_items = select_stmt->get_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      TableItem *cur_table = table_items.at(i);
      int32_t bit_id = OB_INVALID_INDEX;
      if (OB_ISNULL(cur_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret), K(cur_table));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX ==
                (bit_id = select_stmt->get_table_bit_index(cur_table->table_id_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table bit index", K(ret), K(cur_table->table_id_), K(bit_id));
      } else if (!origin_output_rel_ids.has_member(bit_id)) {
      // semi join 中的表, 不会输出
      } else if (cur_table->is_generated_table() || cur_table->is_temp_table()) {
        ObSelectStmt *view_stmt = NULL;
        ObSEArray<ObRawExpr*, 4> stmt_unique_keys;
        ObSEArray<ObRawExpr*, 4> column_exprs;
        if (OB_ISNULL(view_stmt = cur_table->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("view_stmt = stmt is null", K(ret), K(view_stmt));
        } else if (OB_FAIL(SMART_CALL(recursive_set_stmt_unique(view_stmt,
                                                                ctx,
                                                                false,
                                                                &stmt_unique_keys)))) {
          LOG_WARN("recursive set stmt unique failed", K(ret));
        } else if (OB_FAIL(create_columns_for_view(ctx,
                                                  *cur_table,
                                                  select_stmt,
                                                  column_exprs))) {
          //为view生成column exprs
          LOG_WARN("failed to create columns for view", K(ret));
        } else if (OB_FAIL(convert_select_expr_to_column_expr(stmt_unique_keys,
                                                              *view_stmt,
                                                              *select_stmt,
                                                              cur_table->table_id_,
                                                              added_unique_keys))) {
          //找到stmt unique keys对应的本层column expr
          LOG_WARN("failed to get stmt unique keys columns expr", K(ret));
        }
      } else if (!cur_table->is_basic_table()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect table item type", K(*cur_table), K(ret));
      } else if (OB_FAIL(generate_unique_key(ctx, select_stmt, cur_table, added_unique_keys))) {
        LOG_WARN("failed to generate unique key", K(ret));
      }
    }
  }

  // need add additional select item
  if (OB_SUCC(ret) && !added_unique_keys.empty()) {
    ObSEArray<ObRawExpr*, 4> select_exprs;
    if (OB_FAIL(select_stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_NOT_NULL(unique_keys) && OB_FAIL(append(*unique_keys, added_unique_keys))) {
      LOG_WARN("failed to append unique keys", K(ret));
    } else if (OB_FAIL(add_non_duplicated_select_expr(added_unique_keys, select_exprs))) {
      LOG_WARN("failed to add non-duplicated select expr", K(ret));
    } else if (OB_FAIL(create_select_item(*ctx->allocator_, added_unique_keys, select_stmt))) {
      LOG_WARN("failed to get tables primary keys", K(ret));
    } else if (OB_FAIL(select_stmt->formalize_stmt(ctx->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

/* select_stmt is unique checked by ObTransformUtils::check_stmt_unique */
int ObTransformUtils::get_unique_keys_from_unique_stmt(const ObSelectStmt *select_stmt,
                                                       ObRawExprFactory *expr_factory,
                                                       ObIArray<ObRawExpr*> &unique_keys,
                                                       ObIArray<ObRawExpr*> &added_unique_keys)
{
  int ret = OB_SUCCESS;
  unique_keys.reuse();
  added_unique_keys.reuse();
  ObConstRawExpr *expr = NULL;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(select_stmt), K(expr_factory));
  } else if (OB_FAIL(select_stmt->get_select_exprs_without_lob(unique_keys))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_LIKELY(!unique_keys.empty())) {
    /* do nothing */
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, 0, expr))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(added_unique_keys.push_back(expr))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObTransformUtils::add_non_duplicated_select_expr(ObIArray<ObRawExpr*> &add_select_exprs,
                                                     ObIArray<ObRawExpr*> &org_select_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> new_select_exprs;
  if (OB_FAIL(ObOptimizerUtil::except_exprs(add_select_exprs,
                                            org_select_exprs,
                                            new_select_exprs))) {
    LOG_WARN("failed cal except exprs", K(ret));
  } else if (OB_FAIL(add_select_exprs.assign(new_select_exprs))) {
    LOG_WARN("failed to assign select exprs", K(ret));
  }
  return ret;
}

int ObTransformUtils::check_can_set_stmt_unique(ObDMLStmt *stmt,
                                                bool &can_set_unique)
{
  int ret = OB_SUCCESS;
  can_set_unique = false;
  ObSqlBitSet<> output_rel_ids;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ptr", K(stmt), K(ret));
  } else if (OB_FAIL(stmt->get_from_tables(output_rel_ids))) {
    LOG_WARN("failed to get output rel ids", K(ret));
  } else {
    ObSelectStmt *view_stmt = NULL;
    can_set_unique = true;
    ObIArray<TableItem *> &table_items = stmt->get_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && can_set_unique && i < table_items.count(); ++i) {
      const TableItem *table_item = table_items.at(i);
      int32_t bit_id = OB_INVALID_INDEX;
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null ptr", K(table_item), K(ret));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX ==
                   (bit_id = stmt->get_table_bit_index(table_item->table_id_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table bit index", K(ret), K(table_item->table_id_), K(bit_id));
      } else if (!output_rel_ids.has_member(bit_id)) {
        // semi join 中的表, 不会有影响
      } else if (table_item->is_generated_table() || table_item->is_temp_table()) {
        if (OB_ISNULL(view_stmt = table_item->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null ptr", K(view_stmt), K(ret));
        } else if (view_stmt->is_set_stmt() && !view_stmt->is_set_distinct()) {//union all不能保持唯一
          can_set_unique = false;
        } else if (view_stmt->is_set_stmt() && view_stmt->is_set_distinct()) {
          //do nothing
        } else if (view_stmt->is_hierarchical_query() || view_stmt->has_rollup()) {
          can_set_unique = false;
        } else if (OB_FAIL(SMART_CALL(check_can_set_stmt_unique(view_stmt,
                                                                can_set_unique)))) {
          LOG_WARN("failed to check can set stmt unique", K(ret));
        } else {/*do nothing */}
      } else if (table_item->is_basic_table()) {
        //do nothing
      } else {
        can_set_unique = false;
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_rel_ids_from_tables(const ObDMLStmt *stmt,
                                              const ObIArray<TableItem*> &table_items,
                                              ObRelIds &rel_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null.", K(ret));
  }
  TableItem *table = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    if (OB_ISNULL(table = table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null.", K(ret));
    } else if (OB_FAIL(get_rel_ids_from_table(stmt, table,rel_ids))) {
      LOG_WARN("failed to get rel ids from table", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_rel_ids_from_tables(const ObDMLStmt *stmt,
                                              const ObIArray<uint64_t> &table_ids,
                                              ObRelIds &rel_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null.", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
    if (OB_FAIL(rel_ids.add_member(stmt->get_table_bit_index(table_ids.at(i))))) {
      LOG_WARN("failed to add member", K(ret), K(table_ids.at(i)));
    } else { /* do nothing. */ }
  }
  return ret;
}

int ObTransformUtils::get_left_rel_ids_from_semi_info(const ObDMLStmt *stmt,
                                                      SemiInfo *info,
                                                      ObSqlBitSet<> &rel_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null.", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < info->left_table_ids_.count(); ++i) {
    int64_t idx = stmt->get_table_bit_index(info->left_table_ids_.at(i));
    if (OB_FAIL(rel_ids.add_member(idx))) {
      LOG_WARN("failed to add member", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_rel_ids_from_table(const ObDMLStmt *stmt,
                                            const TableItem *table,
                                            ObRelIds &rel_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null.", K(ret));
  } else if (!table->is_joined_table()) {
    if (OB_FAIL(rel_ids.add_member(stmt->get_table_bit_index(table->table_id_)))) {
      LOG_WARN("failed to add member", K(ret));
    }
  } else {
    const JoinedTable *joined_table = static_cast<const JoinedTable*>(table);
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->single_table_ids_.count(); ++i) {
      if (OB_FAIL(rel_ids.add_member(
          stmt->get_table_bit_index(joined_table->single_table_ids_.at(i))))) {
        LOG_WARN("failed to add member", K(ret), K(joined_table->single_table_ids_.at(i)));
      } else { /* do nothing. */ }
    }
  }
  return ret;
}

int ObTransformUtils::adjust_single_table_ids(JoinedTable *joined_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else {
    joined_table->single_table_ids_.reset();
    TableItem *left_table = joined_table->left_table_;
    TableItem *right_table = joined_table->right_table_;
    if (OB_ISNULL(left_table) || OB_ISNULL(right_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left or right table is null", K(ret));
    } else if (left_table->type_ == TableItem::JOINED_TABLE) {
      JoinedTable *left_join_table = static_cast<JoinedTable*>(left_table);
      if (OB_FAIL(SMART_CALL(adjust_single_table_ids(left_join_table)))) {
        LOG_WARN("failed to adjust single table ids", K(ret));
      } else if (OB_FAIL(append_array_no_dup(joined_table->single_table_ids_,
                                             left_join_table->single_table_ids_))) {
        LOG_WARN("failed to append to single table ids.", K(ret));
      } else { /* do nothing. */ }
    } else {
      if (OB_FAIL(add_var_to_array_no_dup(joined_table->single_table_ids_,
                                          left_table->table_id_))) {
        LOG_WARN("failed to add var to array no dup.", K(ret));
      } else { /* do nothing. */ }
    }
    if (OB_FAIL(ret)) {
    } else if (right_table->type_ == TableItem::JOINED_TABLE) {
      JoinedTable *right_join_table = static_cast<JoinedTable*>(right_table);
      if (OB_FAIL(SMART_CALL(adjust_single_table_ids(right_join_table)))) {
        LOG_WARN("failed to adjust single table ids", K(ret));
      } else if (OB_FAIL(append_array_no_dup(joined_table->single_table_ids_,
                                             right_join_table->single_table_ids_))) {
        LOG_WARN("failed to append to single table ids.", K(ret));
      } else { /* do nothing. */ }
    } else {
      if (OB_FAIL(add_var_to_array_no_dup(joined_table->single_table_ids_,
                                          right_table->table_id_))) {
        LOG_WARN("failed to add var to array no dup.", K(ret));
      } else { /* do nothing. */ }
    }
  }
  return ret;
}

int ObTransformUtils::extract_table_items(TableItem *table_item,
                                          ObIArray<TableItem *> &table_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret));
  } else if (table_item->is_joined_table()) {
    TableItem *left_table = static_cast<JoinedTable*>(table_item)->left_table_;
    TableItem *right_table = static_cast<JoinedTable*>(table_item)->right_table_;
    if (OB_ISNULL(left_table) || OB_ISNULL(right_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null error.", K(ret));
    } else if (OB_FAIL(SMART_CALL(extract_table_items(left_table, table_items)))) {
      LOG_WARN("failed to extaract table items.", K(ret));
    } else if (OB_FAIL(SMART_CALL(extract_table_items(right_table, table_items)))) {
      LOG_WARN("failed to extaract table items.", K(ret));
    } else {}
  } else if (OB_FAIL(table_items.push_back(table_item))) {
    LOG_WARN("failed to push back into table items.", K(ret));
  } else {}
  return ret;
}

int ObTransformUtils::get_base_column(const ObDMLStmt *stmt,
                                      ObColumnRefRawExpr *&col)
{
  int ret = OB_SUCCESS;
  const TableItem *table = NULL;
  ObRawExpr *sel_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(col)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(col));
  } else if (OB_ISNULL(table = stmt->get_table_item_by_id(col->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(table));
  } else if (table->is_basic_table() || table->is_link_table()) {
    /*do nothing*/
  } else if (!table->is_generated_table() && !table->is_temp_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is expected to be generated or temp table", K(ret), K(table));
  } else if (OB_ISNULL(table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ref query", K(ret));
  } else {
    int64_t sid = col->get_column_id() - OB_APP_MIN_COLUMN_ID;
    const ObSelectStmt *view = table->ref_query_;
    if (OB_ISNULL(view) || OB_ISNULL(view = view->get_real_stmt()) ||
        OB_UNLIKELY(sid < 0 || sid >= view->get_select_item_size()) ||
        OB_ISNULL(sel_expr = view->get_select_item(sid).expr_) ||
        OB_UNLIKELY(!sel_expr->is_column_ref_expr() &&
                    sel_expr->get_expr_type() != T_FUN_SYS_MAKEXML)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid generated table column", K(ret), K(view), K(sid), K(sel_expr));
    } else if (sel_expr->get_expr_type() == T_FUN_SYS_MAKEXML) {
      if (sel_expr->get_param_count() != 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid xml generated table column", K(ret), K(sel_expr));
      } else if (!sel_expr->get_param_expr(1)->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid xml generated table column", K(ret), K(sel_expr->get_param_expr(1)->is_column_ref_expr()));
      } else {
        col = static_cast<ObColumnRefRawExpr*>(sel_expr->get_param_expr(1));
      }
    } else if (FALSE_IT(col = static_cast<ObColumnRefRawExpr*>(sel_expr))) {
      /*do nothing*/
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(SMART_CALL(get_base_column(view,
                                                  col)))) {
      LOG_WARN("failed to get update table", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformUtils::get_post_join_exprs
 * the term post-join exprs refers to exprs processed upon the results of join
 * @param stmt
 * @param exprs
 * @param with_vector_assign:
 *        update tc set (c1, c2) = (select sum(d1), sum(d2) from td);
 * @return
 */
int ObTransformUtils::get_post_join_exprs(ObDMLStmt *stmt,
                                          ObIArray<ObRawExpr *> &exprs,
                                          bool with_vector_assgin /*= false*/)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret));
  } else if (OB_FAIL(stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else if (has_rownum) {
    // do nothing
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
    if (sel_stmt->has_rollup()) {
      // do nothing
    } else if (sel_stmt->has_group_by()) {
      if (OB_FAIL(append(exprs, sel_stmt->get_group_exprs()))) {
        LOG_WARN("failed to append group by exprs", K(ret));
      } else if (OB_FAIL(append(exprs, sel_stmt->get_aggr_items()))) {
        LOG_WARN("failed to append aggr items", K(ret));
      }
    } else if (OB_FAIL(sel_stmt->get_select_exprs(exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    }
  } else if (stmt->is_update_stmt()) {
    ObUpdateStmt *upd_stmt = static_cast<ObUpdateStmt *>(stmt);
    if (upd_stmt->has_limit() || upd_stmt->has_order_by()) {
      // do nothing
    } else if (OB_FAIL(upd_stmt->get_assign_values(exprs, with_vector_assgin))) {
      LOG_WARN("failed to get assign value exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::free_stmt(ObStmtFactory &stmt_factory,
                                ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt *, 4> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_FAIL(free_stmt(stmt_factory, child_stmts.at(i)))) {
      LOG_WARN("failed to free stmt", K(ret));
    }
  }
  if (OB_SUCC(ret) && stmt->is_select_stmt()) {
    if (OB_FAIL(SMART_CALL(stmt_factory.free_stmt(static_cast<ObSelectStmt*>(stmt))))) {
      LOG_WARN("failed to free stmt", K(ret));
    }
  }
  return ret;
}

class ObSharedExprExtractor : public ObStmtExprVisitor
{
public:
  int do_visit(ObRawExpr *&expr)
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    bool is_shared = false;
    uint64_t key = reinterpret_cast<uint64_t>(expr);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (expr->has_flag(CNT_COLUMN) ||
               expr->has_flag(CNT_AGG) ||
               expr->has_flag(CNT_SET_OP) ||
               expr->has_flag(CNT_WINDOW_FUNC) ||
               expr->has_flag(CNT_DYNAMIC_PARAM) ||
               expr->has_flag(CNT_SUB_QUERY)) {
      tmp_ret = first_stmt_expr_set_.exist_refactored(key);
      if (tmp_ret == OB_HASH_EXIST) {
        if (!is_first_) {
          if (OB_FAIL(second_stmt_expr_set_.set_refactored(key))) {
            if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to add expr into set", K(ret));
            }
          }
          is_shared = true;
        }
      } else if (tmp_ret == OB_HASH_NOT_EXIST) { // OB_HASH_NOT_EXISTS
        if (is_first_) {
          if (OB_FAIL(first_stmt_expr_set_.set_refactored(key))) {
            LOG_WARN("failed to add expr into set", K(ret));
          }
        }
      } else {
        ret = tmp_ret;
        LOG_WARN("failed to check hash set", K(ret));
      }
      if (!is_shared) {
        for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
          if (OB_FAIL(SMART_CALL(do_visit(expr->get_param_expr(i))))) {
            LOG_WARN("failed to visit first", K(ret));
          }
        }
      }
    }
    return ret;
  }

  bool is_first_;
  hash::ObHashSet<uint64_t> first_stmt_expr_set_;
  hash::ObHashSet<uint64_t> second_stmt_expr_set_;
};

int ObTransformUtils::extract_shared_expr(ObDMLStmt *upper_stmt,
                                          ObDMLStmt *child_stmt,
                                          ObIArray<ObRawExpr*> &shared_exprs,
                                          ObIArray<DmlStmtScope> *upper_scopes,
                                          ObIArray<DmlStmtScope> *child_scopes)
{
  int ret = OB_SUCCESS;
  ObSharedExprExtractor extractor;
  if (OB_FAIL(extractor.first_stmt_expr_set_.create(64))) {
    LOG_WARN("failed to create expr set", K(ret));
  } else if (OB_FAIL(extractor.second_stmt_expr_set_.create(64))) {
    LOG_WARN("failed to create expr set", K(ret));
  } else {
    extractor.is_first_ = true;
    extractor.set_relation_scope();
    if (NULL != child_scopes) {
      extractor.remove_scope(*child_scopes);
    }
    if (OB_FAIL(child_stmt->iterate_stmt_expr(extractor))) {
      LOG_WARN("failed to iterate stmt expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    extractor.is_first_ = false;
    extractor.set_relation_scope();
    if (NULL != upper_scopes) {
      extractor.remove_scope(*upper_scopes);
    }
    if (OB_FAIL(upper_stmt->iterate_stmt_expr(extractor))) {
      LOG_WARN("failed to iterate stmt expr", K(ret));
    } else {
      for (hash::ObHashSet<uint64_t>::iterator it = extractor.second_stmt_expr_set_.begin();
           OB_SUCC(ret) && it != extractor.second_stmt_expr_set_.end(); it++) {
        ObRawExpr *cur_expr = reinterpret_cast<ObRawExpr *>(it->first);
        if (OB_ISNULL(cur_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current expr is null", K(ret), K(cur_expr));
        } else if (cur_expr->is_exec_param_expr()) {
          // do nothing
        } else if (OB_FAIL(shared_exprs.push_back(cur_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(extractor.first_stmt_expr_set_.destroy())) {
      LOG_WARN("failed to detroy stmt expr set", K(ret));
    } else if (OB_FAIL(extractor.second_stmt_expr_set_.destroy())) {
      LOG_WARN("failed to destroy stmt expr set", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::add_param_not_null_constraint(ObIArray<ObExprConstraint> &constraints,
                                                    ObIArray<ObRawExpr *> &not_null_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < not_null_exprs.count(); ++i) {
    if (OB_FAIL(add_param_not_null_constraint(constraints, not_null_exprs.at(i)))) {
      LOG_WARN("failed to add param not null constraint", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::add_param_not_null_constraint(ObIArray<ObExprConstraint> &constraints,
                                                    ObRawExpr *not_null_expr,
                                                    bool is_true)
{
  int ret = OB_SUCCESS;
  bool existed = false;
  if (OB_ISNULL(not_null_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr is null", K(ret));
  } else if (OB_UNLIKELY(!not_null_expr->is_static_const_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pre calculable expr is expected here", K(ret));
  } else {
    ObExprConstraint cons(not_null_expr, PRE_CALC_RESULT_NOT_NULL);
    if (OB_FAIL(add_var_to_array_no_dup(constraints, cons))) {
      LOG_WARN("failed to push back pre calc constraints", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::add_param_not_null_constraint(ObTransformerCtx &ctx,
                                                    ObIArray<ObRawExpr *> &not_null_exprs)
{
  int ret = OB_SUCCESS;
  ret = add_param_not_null_constraint(ctx.expr_constraints_, not_null_exprs);
  return ret;
}

int ObTransformUtils::add_param_not_null_constraint(ObTransformerCtx &ctx,
                                                    ObRawExpr *not_null_expr,
                                                    bool is_true)
{
  int ret = OB_SUCCESS;
  ret = add_param_not_null_constraint(ctx.expr_constraints_, not_null_expr, is_true);
  return ret;
}

int ObTransformUtils::add_param_null_constraint(ObTransformerCtx &ctx,
                                                ObRawExpr *null_expr)
{
  int ret = OB_SUCCESS;
  bool existed = false;
  if (OB_ISNULL(null_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr is null", K(ret));
  } else if (OB_UNLIKELY(!null_expr->is_static_const_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pre calculable expr is expected here", K(ret));
  } else {
    ObExprConstraint cons(null_expr, PRE_CALC_RESULT_NULL);
    if (OB_FAIL(add_var_to_array_no_dup(ctx.expr_constraints_, cons))) {
      LOG_WARN("failed to push back pre calc constraints", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::add_param_lossless_cast_constraint(ObTransformerCtx &ctx,
                                                         ObRawExpr *expr,
                                                         const ObRawExpr *dst_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *cast_expr = expr;
  ObRawExpr *equal_expr = NULL;
  if (OB_ISNULL(expr) || OB_ISNULL(dst_expr) || OB_ISNULL(ctx.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(add_cast_for_replace(*ctx.expr_factory_,
                                          dst_expr,
                                          cast_expr,
                                          ctx.session_info_))) {
    LOG_WARN("failed to add cast expr", K(ret));
  } else if (OB_UNLIKELY(cast_expr == expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("must add cast for lossless cast constraint", KPC(dst_expr), KPC(expr), K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx.expr_factory_,
                                                           ctx.session_info_,
                                                           T_OP_EQ,
                                                           equal_expr,
                                                           cast_expr,
                                                           expr))) {
    LOG_WARN("failed to create double op expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(&ctx, equal_expr, true))) {
    LOG_WARN("failed to add param bool constraint", K(ret));
  }
  return ret;
}

/**
 * is_question_mark_pre_param
 * 改写阶段用于判断一个`?`是否来自参数化。
 * query context中的保存的question_marks_count_表示stmt中`?`的总数，包含参数化的`?`，预计算的`?`，
 * 条件下推抽出的`?`。
 */
// UNUSED
int ObTransformUtils::is_question_mark_pre_param(const ObDMLStmt &stmt,
                                                 const int64_t param_idx,
                                                 bool &is_pre_param,
                                                 int64_t &pre_param_count)
{
  int ret = OB_SUCCESS;
  pre_param_count = 0;
  const ObQueryCtx *query_ctx = NULL;
  is_pre_param = false;
  if (OB_ISNULL(query_ctx = stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (query_ctx->calculable_items_.count() > 0) {
    pre_param_count = -1;
    for (int64_t i = 0; i < query_ctx->calculable_items_.count(); ++i) {
      const ObHiddenColumnItem &cur_item = query_ctx->calculable_items_.at(i);
      if (-1 == pre_param_count) {
        pre_param_count = cur_item.hidden_idx_;
      } else if (cur_item.hidden_idx_ < pre_param_count) {
        pre_param_count = cur_item.hidden_idx_;
      }
    }
  } else {
    pre_param_count = query_ctx->question_marks_count_;
  }
  if (OB_SUCC(ret)) {
    is_pre_param = (param_idx >= 0 && param_idx < pre_param_count);
  }
  return ret;
}

int ObTransformUtils::extract_pseudo_column_like_expr(
                                            ObIArray<ObRawExpr*> &exprs,
                                            ObIArray<ObRawExpr *> &pseudo_column_like_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(extract_pseudo_column_like_expr(exprs.at(i), pseudo_column_like_exprs))) {
      LOG_WARN("failed to extract pseudo column like exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::extract_pseudo_column_like_expr(
                                            ObRawExpr *expr,
                                            ObIArray<ObRawExpr *> &pseudo_column_like_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (ObRawExprUtils::is_pseudo_column_like_expr(*expr)) {
    ret = add_var_to_array_no_dup(pseudo_column_like_exprs, expr);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_pseudo_column_like_expr(expr->get_param_expr(i),
                                                             pseudo_column_like_exprs)))) {
        LOG_WARN("failed to extract pseudo column like expr", K(ret));
      }
    }
  }
  return ret;
}

// zhanyue todo: now can not extract pseudo column in subquery but belongs to cur stmt
// after view merge, v.ora_rowscn is convert to pseudo column for t1
//  select (select v.ora_rowscn) from t2) from (select ora_rowscn from t1) v;
// forbid view merge for this query now
int ObTransformUtils::adjust_pseudo_column_like_exprs(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> relation_exprs;
  ObSEArray<ObRawExpr *, 8> pseudo_column_like_exprs;
  if (OB_FAIL(stmt.get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (OB_FAIL(extract_pseudo_column_like_expr(relation_exprs, pseudo_column_like_exprs))) {
    LOG_WARN("failed to extract pseudo column like expr", K(ret));
  } else if (OB_FAIL(stmt.get_pseudo_column_like_exprs().assign(pseudo_column_like_exprs))) {
    LOG_WARN("failed to assign pseudo column like exprs", K(ret));
  }
  return ret;
}

int ObTransformUtils::check_has_rownum(const ObIArray<ObRawExpr *> &exprs, bool &has_rownum)
{
  int ret = OB_SUCCESS;
  has_rownum = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_rownum && i < exprs.count(); ++i) {
    ObRawExpr *expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->has_flag(CNT_ROWNUM)) {
      has_rownum = true;
    }
  }
  return ret;
}

int ObTransformUtils::add_table_item(ObDMLStmt *stmt, TableItem *table_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(stmt), K(table_item), K(ret));
  } else if (OB_FAIL(stmt->get_table_items().push_back(table_item))) {
    LOG_WARN("failed to push back table item", K(ret));
  } else if (OB_FAIL(stmt->set_table_bit_index(table_item->table_id_))) {
    LOG_WARN("failed to set table bit index", K(ret));
  }
  return ret;
}

int ObTransformUtils::add_table_item(ObDMLStmt *stmt, ObIArray<TableItem *> &table_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
    ret = add_table_item(stmt, table_items.at(i));
  }
  return ret;
}

// 检查是否有相关连接条件, 且内表的列能走到索引上
int ObTransformUtils::check_subquery_match_index(ObTransformerCtx *ctx,
                                                 ObQueryRefRawExpr *query_ref,
                                                 ObSelectStmt *subquery,
                                                 bool &is_match)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_ISNULL(subquery) || OB_ISNULL(query_ref)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret), K(ctx), K(subquery), K(query_ref));
  } else {
    ObRawExpr *cond = NULL;
    ObArenaAllocator alloc;
    EqualSets &equal_sets = ctx->equal_sets_;
    ObSEArray<ObRawExpr *, 4> const_exprs;
    if (OB_FAIL(subquery->get_stmt_equal_sets(equal_sets, alloc, true))) {
      LOG_WARN("failed to get stmt equal sets", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(subquery->get_condition_exprs(),
                                                            const_exprs))) {
      LOG_WARN("failed to compute const equivalent exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < subquery->get_condition_size(); ++i) {
      ObColumnRefRawExpr *col_expr = NULL;
      ObRawExpr *const_expr = NULL;
      if (OB_ISNULL(cond = subquery->get_condition_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition expr is null", K(ret));
      } else if (T_OP_NSEQ == cond->get_expr_type()) {
        // do nothing
      } else if (OB_FAIL(is_simple_correlated_pred(query_ref->get_exec_params(),
                                                   cond,
                                                   col_expr,
                                                   const_expr))) {
        LOG_WARN("failed to check is simple correlated pred", K(ret));
      } else if (col_expr == NULL) {
        // do nothing
      } else if (OB_FAIL(is_match_index(ctx->sql_schema_guard_,
                                        subquery,
                                        col_expr,
                                        is_match,
                                        &equal_sets,
                                        &const_exprs))) {
        LOG_WARN("failed to check is match index", K(ret));
      }
    }
    equal_sets.reuse();
  }
  return ret;
}


int ObTransformUtils::get_limit_value(ObRawExpr *limit_expr,
                                      const ParamStore *param_store,
                                      ObExecContext *exec_ctx,
                                      ObIAllocator *allocator,
                                      int64_t &limit_value,
                                      bool &is_null_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_expr_int_value(limit_expr, param_store, exec_ctx, allocator, limit_value, is_null_value))) {
    LOG_WARN("get_expr int value failed", K(ret));
  } else if (limit_value < 0) {
    limit_value = 0;
  }
  return ret;
}

int ObTransformUtils::get_expr_int_value(ObRawExpr *expr,
                                      const ParamStore *param_store,
                                      ObExecContext *exec_ctx,
                                      ObIAllocator *allocator,
                                      int64_t &value,
                                      bool &is_null_value)
{
  int ret = OB_SUCCESS;
  value = 0;
  is_null_value = false;
  if (NULL == expr) {
    /*do nothing*/
  } else if ( OB_ISNULL(param_store) ||
      OB_ISNULL(exec_ctx) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr), K(param_store),
                                    K(exec_ctx), K(allocator));
  } else if (T_INT == expr->get_expr_type()) {
    value = static_cast<const ObConstRawExpr *>(expr)->get_value().get_int();
  } else if (T_NULL == expr->get_expr_type()) {
    is_null_value = true;
  } else {
    ObObj obj_value;
    bool got_result = false;
    if ((expr->is_static_scalar_const_expr()) &&
               (expr->get_result_type().is_integer_type() ||
                expr->get_result_type().is_number())) {
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                            expr,
                                                            obj_value,
                                                            got_result,
                                                            *allocator,
                                                            false))) {
        LOG_WARN("failed to calc const or calculable expr", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected limit expr type", K(*expr), K(ret));
    }
    if (OB_SUCC(ret)) {
      number::ObNumber number;
      if (obj_value.is_integer_type()) {
        value = obj_value.get_int();
      } else if (obj_value.is_null() || obj_value.is_null_oracle()) {
        is_null_value = true;
      } else if (OB_FAIL(obj_value.get_number(number))) {
        LOG_WARN("unexpected value type", K(ret), K(obj_value));
      } else if (OB_UNLIKELY(!number.is_valid_int64(value))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("number is not valid int64", K(ret), K(value), K(number));
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_percentage_value(ObRawExpr *percent_expr,
                                           const ObDMLStmt *stmt,
                                           const ParamStore *param_store,
                                           ObExecContext *exec_ctx,
                                           ObIAllocator *allocator,
                                           double &percent_value,
                                           bool &is_null_value)
{
  int ret = OB_SUCCESS;
  percent_value = 0.0;
  is_null_value = false;
  if (OB_ISNULL(param_store) || OB_ISNULL(exec_ctx) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(param_store), K(exec_ctx), K(allocator), K(ret));
  } else if (NULL == percent_expr) {
    /*do nothing*/
  } else if (T_DOUBLE == percent_expr->get_expr_type()) {
    percent_value = static_cast<const ObConstRawExpr *>(percent_expr)->get_value().get_double();
  } else if (T_NULL == percent_expr->get_expr_type()) {
    is_null_value = true;
  } else {
    ObObj value;
    bool got_result = false;
    if (percent_expr->is_static_scalar_const_expr() && 
        percent_expr->get_result_type().is_double()) {
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                            percent_expr,
                                                            value,
                                                            got_result,
                                                            *allocator,
                                                            false))) {
        LOG_WARN("failed to calc const or calculable expr", K(ret));
      } else { /*do nothing*/ }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected limit expr type", K(percent_expr->get_expr_type()), K(ret));
    }
    if (OB_SUCC(ret)) {
      if (value.is_double()) {
        percent_value = value.get_double();
      } else if (value.is_null()) {
        is_null_value = true;
      } else { /*do nothin*/ }
    }
  }
  if (OB_SUCC(ret) && !is_null_value) {
    if (percent_value < 0.0) {
      percent_value = 0.0;
    } else if (percent_value > 100.0) {
      percent_value = 100.0;
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformUtils::add_const_param_constraints(ObRawExpr *expr,
                                                  ObTransformerCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  ObSEArray<ObRawExpr*, 4> params;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx) || OB_ISNULL(ctx->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr), K(ctx), K(ctx->exec_ctx_), K(plan_ctx));
  } else if (OB_FAIL(ObRawExprUtils::extract_params(expr, params))) {
    LOG_WARN("failed to extract params", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
      if (OB_ISNULL(params.at(i)) ||
          OB_UNLIKELY(params.at(i)->get_expr_type() != T_QUESTIONMARK)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(params), K(i));
      } else {
        ObPCConstParamInfo param_info;
        ObObj target_obj;
        ObConstRawExpr *const_expr = static_cast<ObConstRawExpr*>(params.at(i));
        int64_t param_idx = const_expr->get_value().get_unknown();
        if (OB_UNLIKELY(param_idx < 0 || param_idx >= plan_ctx->get_param_store().count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(param_idx),
                                            K(plan_ctx->get_param_store().count()));
        } else if (OB_FAIL(param_info.const_idx_.push_back(param_idx))) {
          LOG_WARN("failed to push back param idx", K(ret));
        } else if (OB_FAIL(param_info.const_params_.push_back(
                                                      plan_ctx->get_param_store().at(param_idx)))) {
          LOG_WARN("failed to push back value", K(ret));
        } else if (OB_FAIL(ctx->plan_const_param_constraints_.push_back(param_info))) {
          LOG_WARN("failed to push back param info", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

//pc_constraints are for non-paramlized groupby exprs while eq_constraints are for paramlized groupby exprs
int ObTransformUtils::replace_stmt_expr_with_groupby_exprs(ObSelectStmt *select_stmt,
                                                           ObTransformerCtx *trans_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt));
  } else {
    common::ObIArray<SelectItem>& select_items = select_stmt->get_select_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); i++) {
      if (OB_FAIL(replace_with_groupby_exprs(select_stmt,
                                             select_items.at(i).expr_,
                                             true,
                                             trans_ctx,
                                             false))) {
        LOG_WARN("failed to replace with groupby columns.", K(ret));
      } else { /*do nothing.*/ }
    }
    common::ObIArray<ObRawExpr*>& having_exprs = select_stmt->get_having_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < having_exprs.count(); i++) {
      if (OB_FAIL(replace_with_groupby_exprs(select_stmt,
                                             having_exprs.at(i),
                                             true,
                                             trans_ctx,
                                             false))) {
        LOG_WARN("failed to replace with groupby columns.", K(ret));
      } else { /*do nothing.*/ }
    }
    common::ObIArray<OrderItem> &order_items = select_stmt->get_order_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); i++) {
      if (OB_FAIL(replace_with_groupby_exprs(select_stmt,
                                             order_items.at(i).expr_,
                                             false,
                                             trans_ctx,
                                             false))) {
        LOG_WARN("failed to replace with groupby columns.", K(ret));
      } else { /*do nothing.*/ }
    }
  }
  return ret;
}

int ObTransformUtils::replace_with_groupby_exprs(ObSelectStmt *select_stmt,
                                                 ObRawExpr *&expr,
                                                 bool need_query_compare,
                                                 ObTransformerCtx *trans_ctx,
                                                 bool in_add_expr)
{
  int ret = OB_SUCCESS;
  ObStmtCompareContext check_context;
  check_context.reset();
  check_context.override_const_compare_ = true;
  check_context.override_query_compare_ = need_query_compare;
  common::ObSEArray<ObExprEqualCheckContext::ParamExprPair, 4> param_exprs_local;
  common::ObSEArray<ObPCParamEqualInfo, 4> equal_params_local;
  if (OB_ISNULL(expr) || OB_ISNULL(select_stmt) || OB_ISNULL(select_stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got an unexpected null", K(ret));
  } else {
    check_context.init(&select_stmt->get_query_ctx()->calculable_items_);
    int64_t param_cnt = expr->get_param_count();
    // only first param should be replaced (if needed) for T_OP_IS and T_OP_IS_NOT expr
    //    select null as aa group by aa having null is null;
    // the first null in having exprs is allowed to be parameterized
    // but the second null is not allowed
    if (T_OP_IS == expr->get_expr_type() || T_OP_IS_NOT == expr->get_expr_type()) {
      param_cnt = 1;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; i++) {
      if (OB_FAIL(SMART_CALL(replace_with_groupby_exprs(select_stmt,
                                                        expr->get_param_expr(i),
                                                        need_query_compare,
                                                        trans_ctx,
                                                        T_OP_ADD == expr->get_expr_type())))) {
        LOG_WARN("failed to replace with groupby columns.", K(ret));
      } else { /*do nothing.*/ }
    }
    bool is_existed = false;
    ObIArray<ObRawExpr *> &groupby_exprs = select_stmt->get_group_exprs();
    ObIArray<ObRawExpr *> &rollup_exprs = select_stmt->get_rollup_exprs();
    ObIArray<ObGroupingSetsItem> &grouping_sets_items = select_stmt->get_grouping_sets_items();
    ObIArray<ObRollupItem> &rollup_items = select_stmt->get_rollup_items();
    ObIArray<ObCubeItem> &cube_items = select_stmt->get_cube_items();
    for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < groupby_exprs.count(); i++) {
      if (OB_ISNULL(groupby_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got an unexpected null", K(ret));
      } else if ((is_mysql_mode() || !expr->is_static_const_expr())
                  && groupby_exprs.at(i)->same_as(*expr, &check_context)) {
        expr = groupby_exprs.at(i);
        is_existed = true;
      } else { /*do nothing.*/ }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < rollup_exprs.count(); i++) {
      if (OB_ISNULL(rollup_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got an unexpected null", K(ret));
      } else if ((lib::is_mysql_mode()|| !expr->is_static_const_expr())
                  && rollup_exprs.at(i)->same_as(*expr, &check_context)) {
        expr = rollup_exprs.at(i);
        is_existed = true;
      } else { /*do nothing.*/ }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < grouping_sets_items.count(); i++) {
      ObIArray<ObGroupbyExpr> &grouping_sets_exprs = grouping_sets_items.at(i).grouping_sets_exprs_;
      for (int64_t j = 0; OB_SUCC(ret) && !is_existed && j < grouping_sets_exprs.count(); j++) {
        ObIArray<ObRawExpr*> &groupby_exprs = grouping_sets_exprs.at(j).groupby_exprs_;
        for (int64_t k = 0; OB_SUCC(ret) && !is_existed && k < groupby_exprs.count(); k++) {
          check_context.param_expr_.reset();
          check_context.equal_param_info_.reset();
          if (OB_ISNULL(groupby_exprs.at(k))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("got an unexpected null", K(ret));
          } else if (groupby_exprs.at(k)->same_as(*expr, &check_context)) {
            expr = groupby_exprs.at(k);
            is_existed = true;
          } else if (trans_ctx != NULL && in_add_expr &&
                     expr->get_expr_type() == T_QUESTIONMARK &&
                     groupby_exprs.at(k)->get_expr_type() == T_QUESTIONMARK &&
                     lib::is_oracle_mode()) {
            /*
             * for stmt like: select c1 - 1 from t1 group by grouping sets(c1,1);
             * '1' in select_item should share the same expr with '1' in grouping sets.
             * However, in ob, 'c1 - 1' is converted to 'c1 + (-1)', if enable plan cache. 
             * the (-1) is a T_QUESTIONMARK. We transform (-1) to T_OP_NEG with a child '1', so the child '1' of
             * T_OP_NEG could share the same expr with '1' in grouping sets.
             * we do these in the following function.
             */
            if (OB_FAIL(replace_add_exprs_with_groupby_exprs(expr,
                                                             groupby_exprs.at(k),
                                                             trans_ctx,
                                                             is_existed))) {
              LOG_WARN("replace exprs in add failed", K(ret));
            }
          } else { /*do nothing.*/ }
        }
      }
      if (OB_SUCC(ret) && !is_existed) {
        ObIArray<ObRollupItem> &rollup_items = grouping_sets_items.at(i).rollup_items_;
        for (int64_t j = 0; OB_SUCC(ret) && !is_existed && j < rollup_items.count(); j++) {
          ObIArray<ObGroupbyExpr> &rollup_list_exprs = rollup_items.at(j).rollup_list_exprs_;
          for (int64_t k = 0; OB_SUCC(ret) && !is_existed && k < rollup_list_exprs.count(); k++) {
            ObIArray<ObRawExpr*> &groupby_exprs = rollup_list_exprs.at(k).groupby_exprs_;
            for (int64_t m = 0; OB_SUCC(ret) && !is_existed && m < groupby_exprs.count(); m++) {
              if (OB_ISNULL(groupby_exprs.at(m))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("got an unexpected null", K(ret));
              } else if (groupby_exprs.at(m)->same_as(*expr, &check_context)) {
                expr = groupby_exprs.at(m);
                is_existed = true;
              } else { /*do nothing.*/ }
            }
          }
        }
      }
      if (OB_SUCC(ret) && !is_existed) {
        ObIArray<ObCubeItem> &cube_items = grouping_sets_items.at(i).cube_items_;
        for (int64_t j = 0; OB_SUCC(ret) && !is_existed && j < cube_items.count(); j++) {
          ObIArray<ObGroupbyExpr> &cube_list_exprs = cube_items.at(j).cube_list_exprs_;
          for (int64_t k = 0; OB_SUCC(ret) && !is_existed && k < cube_list_exprs.count(); k++) {
            ObIArray<ObRawExpr*> &groupby_exprs = cube_list_exprs.at(k).groupby_exprs_;
            for (int64_t m = 0; OB_SUCC(ret) && !is_existed && m < groupby_exprs.count(); m++) {
              if (OB_ISNULL(groupby_exprs.at(m))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("got an unexpected null", K(ret));
              } else if (groupby_exprs.at(m)->same_as(*expr, &check_context)) {
                expr = groupby_exprs.at(m);
                is_existed = true;
              } else { /*do nothing.*/ }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !is_existed) {
      for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < rollup_items.count(); i++) {
        ObIArray<ObGroupbyExpr> &rollup_list_exprs = rollup_items.at(i).rollup_list_exprs_;
        for (int64_t j = 0; OB_SUCC(ret) && !is_existed && j < rollup_list_exprs.count(); j++) {
          ObIArray<ObRawExpr*> &groupby_exprs = rollup_list_exprs.at(j).groupby_exprs_;
          for (int64_t k = 0; OB_SUCC(ret) && !is_existed && k < groupby_exprs.count(); k++) {
            if (OB_ISNULL(groupby_exprs.at(k))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("got an unexpected null", K(ret));
            } else if (groupby_exprs.at(k)->same_as(*expr, &check_context)) {
              expr = groupby_exprs.at(k);
              is_existed = true;
            } else { /*do nothing.*/ }
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < cube_items.count(); i++) {
      ObIArray<ObGroupbyExpr> &cube_list_exprs = cube_items.at(i).cube_list_exprs_;
      for (int64_t j = 0; OB_SUCC(ret) && !is_existed && j < cube_list_exprs.count(); j++) {
        ObIArray<ObRawExpr*> &groupby_exprs = cube_list_exprs.at(j).groupby_exprs_;
        for (int64_t k = 0; OB_SUCC(ret) && !is_existed && k < groupby_exprs.count(); k++) {
          if (OB_ISNULL(groupby_exprs.at(k))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("got an unexpected null", K(ret));
          } else if (groupby_exprs.at(k)->same_as(*expr, &check_context)) {
            expr = groupby_exprs.at(k);
            is_existed = true;
          } else { /*do nothing.*/ }
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::replace_add_exprs_with_groupby_exprs(ObRawExpr *&expr_l,
                                                  ObRawExpr *expr_r,
                                                  ObTransformerCtx *trans_ctx,
                                                  bool &is_existed)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  const ObConstRawExpr *left_expr = static_cast<ObConstRawExpr *>(expr_l);
  const ObConstRawExpr *right_expr = static_cast<ObConstRawExpr *>(expr_r);
  int64_t idx_left = -1;
  int64_t idx_right = -1;
  if (OB_ISNULL(trans_ctx) || OB_ISNULL(expr_l) || OB_ISNULL(expr_r)
      || OB_ISNULL(trans_ctx->exec_ctx_) || OB_ISNULL(trans_ctx->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexptectd null pointer", K(ret));
  } else if (OB_ISNULL(expr_factory = trans_ctx->exec_ctx_->get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexptectd null pointer", K(ret));
  } else if (OB_FAIL(left_expr->get_value().get_unknown(idx_left))) {
    LOG_WARN("failed to get idx of quesitonmark", K(ret));
  } else if (OB_FAIL(right_expr->get_value().get_unknown(idx_right))) {
    LOG_WARN("failed to get idx of quesitonmark", K(ret));
  } else {
    const ParamStore &param_store = trans_ctx->exec_ctx_->get_physical_plan_ctx()->get_param_store();
    const ObObjParam &left_param = param_store.at(idx_left);
    const ObObjParam &right_param = param_store.at(idx_right);
    if (!check_objparam_abs_equal(left_param, right_param)) {
      //do nothing.
    } else if (OB_FAIL(add_neg_or_pos_constraint(trans_ctx,
                                                 static_cast<ObConstRawExpr*>(expr_r),
                                                 check_objparam_negative(right_param)))) {
      LOG_WARN("fail to add positive constraint to grouping sets");
    } else if (check_objparam_negative(left_param) && !check_objparam_negative(right_param)) {
      // replace the expr with a T_OP_NEG expr who's child is groupby_exprs.at(k);
      ObOpRawExpr *neg = NULL;
      if (OB_FAIL(expr_factory->create_raw_expr(T_OP_NEG, neg))) {
        LOG_WARN("failed to create neg expr", K(ret));
      } else if (OB_ISNULL(expr_l = neg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("neg expr is NULL", K(ret));
      } else if (OB_FAIL(neg->set_param_expr(expr_r))) {
        LOG_WARN("failed to set param expr", K(ret));
      } else {
        neg->set_result_type(expr_l->get_result_type());
        is_existed = true;
      }
    }
  }
  return ret;
}

bool ObTransformUtils::check_objparam_abs_equal(const ObObjParam &obj1, const ObObjParam &obj2) {
  bool is_abs_equal = false;
  if (obj1.is_number() && obj2.is_number()) {
    is_abs_equal = (obj1.get_number().abs_compare(obj2.get_number()) == 0);
  } else if (obj1.is_double() && obj2.is_double()) {
    is_abs_equal = (obj1.get_double() + obj2.get_double() == 0) || (obj1.get_double() == obj2.get_double());
  } else if(obj1.is_float() && obj2.is_float()) {
    is_abs_equal = (obj1.get_float() + obj2.get_float() == 0) || (obj1.get_float() == obj2.get_float());
  }
  return is_abs_equal;
}

bool ObTransformUtils::check_objparam_negative(const ObObjParam &obj1) {
  bool is_neg = false;
  if (obj1.is_number()) {
    is_neg = obj1.is_negative_number();
  } else if (obj1.is_double()) {
    is_neg = (obj1.get_double() < 0);
  } else if(obj1.is_float()) {
    is_neg = (obj1.get_float()  < 0);
  }
  return is_neg;
}

int ObTransformUtils::add_neg_or_pos_constraint(ObTransformerCtx *trans_ctx,
                                              ObRawExpr *expr,
                                              bool is_negative )
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(trans_ctx) || OB_ISNULL(expr) || OB_ISNULL(trans_ctx->exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (OB_ISNULL(expr_factory = trans_ctx->exec_ctx_->get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (!expr->is_static_const_expr()) {
    // do nothing
  } else {
    // construct expr >= 0;
    ObConstRawExpr *zero_expr = NULL;
    ObOpRawExpr *true_expr = NULL;
    if (OB_FAIL(expr_factory->create_raw_expr(T_INT, zero_expr))) {
      LOG_WARN("create raw expr fail", K(ret));
    } else if (OB_ISNULL(zero_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (!is_negative && OB_FAIL(expr_factory->create_raw_expr(T_OP_GE, true_expr))) {
      LOG_WARN("create raw expr fail", K(ret));
    } else if (is_negative && OB_FAIL(expr_factory->create_raw_expr(T_OP_LT, true_expr))) {
      LOG_WARN("create raw expr fail", K(ret));
    } else if (OB_ISNULL(true_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (OB_FAIL(true_expr->set_param_exprs(static_cast<ObRawExpr*>(expr),
                                                      static_cast<ObRawExpr*>(zero_expr)))) {
      LOG_WARN("set param expr fail", K(ret));
    } else {
      ObObj obj;
      obj.set_int(ObIntType, 0);
      zero_expr->set_value(obj);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(true_expr->formalize(trans_ctx->exec_ctx_->get_my_session()))) {
        LOG_WARN("fail to formalize expr", K(ret));
      } else if (OB_FAIL(add_param_bool_constraint(trans_ctx, true_expr, true))) {
        LOG_WARN("fail to add is true constraint", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::add_equal_expr_value_constraint(ObTransformerCtx *trans_ctx,
                                                      ObRawExpr *left,
                                                      ObRawExpr *right)
{
  int ret = OB_SUCCESS;
  ObRawExpr *equal_expr = NULL;
  if (OB_ISNULL(trans_ctx) || OB_ISNULL(left) || (OB_ISNULL(right)) ||
      OB_ISNULL(trans_ctx->expr_factory_) || OB_ISNULL(trans_ctx->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected NULL ptr", K(ret), KP(trans_ctx), KP(left), KP(right));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*trans_ctx->expr_factory_, T_OP_EQ,
                                                                 left, right, equal_expr))) {
    LOG_WARN("failed to build common binary_op_expr");
  } else if (OB_FAIL(equal_expr->formalize(trans_ctx->session_info_))) {
    LOG_WARN("failed to formalize expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(trans_ctx, equal_expr, true))) {
    LOG_WARN("failed to add param bool constraint", K(ret));
  }
  return ret;
}
int ObTransformUtils::add_param_bool_constraint(ObTransformerCtx *ctx,
                                                ObRawExpr *bool_expr,
                                                const bool is_true,
                                                const bool ignore_const_check /*=false*/)
{
  int ret = OB_SUCCESS;
  bool existed = false;
  if (OB_ISNULL(bool_expr) || OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr is null", K(ret));
  } else if (OB_UNLIKELY(!ignore_const_check && !bool_expr->is_static_const_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pre calculable expr is expected here",KPC(bool_expr), K(ret));
  } else {
    const PreCalcExprExpectResult expect_result = is_true
        ? PreCalcExprExpectResult::PRE_CALC_RESULT_TRUE
        : PreCalcExprExpectResult::PRE_CALC_RESULT_FALSE;
    ObExprConstraint cons(bool_expr, expect_result);
    cons.ignore_const_check_ = ignore_const_check;
    if (OB_FAIL(add_var_to_array_no_dup(ctx->expr_constraints_, cons))) {
      LOG_WARN("failed to push back pre calc constraints", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_all_child_stmts(ObDMLStmt *stmt,
                                          ObIArray<ObSelectStmt*> &child_stmts,
                                          hash::ObHashMap<uint64_t, ObParentDMLStmt> *parent_map)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 8> temp_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(temp_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  int64_t child_stmt_size = temp_stmts.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); ++i) {
    TableItem *table = stmt->get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_temp_table()) {
      //do nothing
    } else if (ObOptimizerUtil::find_item(child_stmts, table->ref_query_) ||
               ObOptimizerUtil::find_item(temp_stmts, table->ref_query_)) {
      //do nothing
    } else if (OB_FAIL(temp_stmts.push_back(table->ref_query_))) {
      LOG_WARN("failed to push back stmt", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(child_stmts, temp_stmts))) {
      LOG_WARN("failed to append temp stmts", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret)&& i < temp_stmts.count(); ++i) {
    uint64_t key = reinterpret_cast<uint64_t>(temp_stmts.at(i));
    ObParentDMLStmt parent_stmt;
    if (i < child_stmt_size) {
      parent_stmt.stmt_ = stmt;
      parent_stmt.pos_ = i;
    } else {
      parent_stmt.stmt_ = NULL;
      parent_stmt.pos_ = OB_INVALID_ID;
    }
    if (OB_ISNULL(temp_stmts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("temp stmt is null", K(ret));
    } else if (parent_map != NULL && OB_FAIL(parent_map->set_refactored(key, parent_stmt))) {
      LOG_WARN("failed to add parent child relation", K(ret));
    } else if (OB_FAIL(SMART_CALL(get_all_child_stmts(temp_stmts.at(i), child_stmts, parent_map)))) {
      LOG_WARN("failed to get all child stmts", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::check_select_expr_is_const(ObSelectStmt* stmt, ObRawExpr* expr, bool &is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (expr->is_const_raw_expr()) {
    is_const = true;
  } else if (expr->is_set_op_expr()) {
    ObSetOpRawExpr * set_expr = static_cast<ObSetOpRawExpr*>(expr);
    int64_t idx = set_expr->get_idx();
    is_const = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_const && i < stmt->get_set_query().count(); ++i) {
      ObSelectStmt *set_query = stmt->get_set_query().at(i);
      if (OB_ISNULL(set_query)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(ret));
      } else if (idx < 0 || idx >= set_query->get_select_item_size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect select idx", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_select_expr_is_const(set_query,
                                                               set_query->get_select_item(idx).expr_,
                                                               is_const)))) {
        LOG_WARN("failed to check is const expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_project_pruning_validity(ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  bool is_const = false;
  if (stmt.has_distinct() || stmt.is_recursive_union() ||
      (stmt.is_set_stmt() && stmt.is_set_distinct()) ||
      stmt.is_hierarchical_query() || stmt.is_contains_assignment() ||
      stmt.is_values_table_query()) {
    // do nothing
    OPT_TRACE("stmt has distinct/assignment or is set stmt");
  } else if (stmt.get_select_item_size() == 1
             && OB_FAIL(check_select_expr_is_const(&stmt, stmt.get_select_item(0).expr_, is_const))) {
    LOG_WARN("failed to check is const expr", K(ret));
  } else if (is_const) {
    // do nothing, only with a dummy output
    OPT_TRACE("only with a dummy output, no need prune");
  } else if (stmt.is_set_stmt()) {
    is_valid = true;
    const ObIArray<ObSelectStmt*> &child_stmts = stmt.get_set_query();
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < child_stmts.count(); ++i) {
      if (OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child query is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_project_pruning_validity(*child_stmts.at(i), is_valid)))) {
        LOG_WARN("failed to check transform validity", K(ret));
      }
    }
  } else {
    is_valid = true;
  }
  return ret;
}

//
//check select_expr是否在set stmt中的order by中使用,如果使用则不能移除
//
int ObTransformUtils::check_select_item_need_remove(const ObSelectStmt *stmt,
                                                    const int64_t idx,
                                                    bool &need_remove)
{
  int ret = OB_SUCCESS;
  need_remove = true;
  ObRawExpr *expr = NULL;
  if (OB_ISNULL(stmt) || idx < 0 || idx >= stmt->get_select_item_size()
      || OB_ISNULL(expr = stmt->get_select_item(idx).expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt", K(ret));
  } else if (!stmt->is_set_stmt()) {
    if (stmt->is_scala_group_by() && expr->has_flag(CNT_AGG)) {
      need_remove = false;
    }
  } else if (OB_ISNULL(expr = ObSelectStmt::get_pure_set_expr(expr))
             || OB_UNLIKELY(!expr->is_set_op_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set expr", K(ret), KPC(expr));
  } else {
    const int64_t child_idx = static_cast<ObSetOpRawExpr*>(expr)->get_idx();
    const ObIArray<ObSelectStmt*> &child_stmts = stmt->get_set_query();
    ObRawExpr *order_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && need_remove && i < child_stmts.count(); ++i) {
      ret = SMART_CALL(check_select_item_need_remove(child_stmts.at(i), child_idx, need_remove));
    }
    for (int64_t i = 0; OB_SUCC(ret) && need_remove && i < stmt->get_order_item_size(); ++i) {
      if (OB_ISNULL(order_expr = stmt->get_order_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null order expr", K(ret));
      } else if (order_expr == expr) {
        need_remove = false;
      }
    }
  }
  return ret;
}

/**
 * Check whether the current subquery can separate spj, and pull related expressions outside of spj
 * */
int ObTransformUtils::check_correlated_exprs_can_pullup(const ObQueryRefRawExpr &query_ref,
                                                        const ObSelectStmt &subquery,
                                                        bool &can_pullup)
{
  int ret = OB_SUCCESS;
  can_pullup = false;
  if (subquery.is_set_stmt()) {
    if (OB_FAIL(check_correlated_exprs_can_pullup_for_set(query_ref, subquery, can_pullup))) {
      LOG_WARN("failed to check correlated expr can pullup for set", K(ret));
    }
  } else {
    ObSEArray<ObRawExpr*, 8> check_exprs;
    bool has_special_expr = false;
    bool is_correlated = false;
    if (subquery.has_distinct() || subquery.is_hierarchical_query()) {
      //do nothing
    } else if (OB_FAIL(check_fixed_expr_correlated(query_ref, subquery, can_pullup))) {
      LOG_WARN("failed to check fixed expr validity", K(ret));
    } else if (!can_pullup) {
      //do nothing
    } else if (OB_FAIL(check_can_pullup_conds(subquery, has_special_expr))) {
      LOG_WARN("failed to check can pullup conds", K(ret));
    } else if (OB_FAIL(is_table_item_correlated(query_ref,
                                                subquery,
                                                is_correlated))) {
      LOG_WARN("failed to check", K(ret));
    } else if (is_correlated) {
      can_pullup = false;
    } else if (OB_FAIL(is_join_conditions_correlated(query_ref, &subquery, is_correlated))) {
      LOG_WARN("failed to check join condition correlated", K(ret));
    } else if (is_correlated) {
      can_pullup = false;
    } else if (OB_FAIL(check_correlated_having_expr_can_pullup(query_ref,
                                                               subquery,
                                                               has_special_expr,
                                                               can_pullup))) {
      LOG_WARN("failed to check can pullup having expr", K(ret));
    } else if (!can_pullup) {
      //do nothing
    } else if (OB_FAIL(check_correlated_where_expr_can_pullup(query_ref,
                                                              subquery,
                                                              has_special_expr,
                                                              can_pullup))) {
      LOG_WARN("failed to check can pullup where expr", K(ret));
    }
    if (OB_SUCC(ret) && can_pullup) {
      // check group-by, rollup, order by exprs
      if (OB_FAIL(append(check_exprs, subquery.get_group_exprs()))) {
        LOG_WARN("failed to append group exprs", K(ret));
      } else if (OB_FAIL(append(check_exprs, subquery.get_rollup_exprs()))) {
        LOG_WARN("failed to append rollup exprs", K(ret));
      } else if (OB_FAIL(subquery.get_order_exprs(check_exprs))) {
        LOG_WARN("failed to append check exprs", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && can_pullup && i < check_exprs.count(); ++i) {
      bool is_correlated = false;
      if (OB_ISNULL(check_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("check expr is null", K(ret), K(check_exprs.at(i)));
      } else if (OB_FAIL(is_correlated_expr(query_ref.get_exec_params(),
                                            check_exprs.at(i),
                                            is_correlated))) {
        LOG_WARN("failed to check is correlated expr", K(ret));
      } else if (is_correlated) {
        can_pullup = false;
      }
    }
  }
  return ret;
}

/**
  * Check whether the current union type subquery can separate spj, and pull related expressions outside of spj
  * It is necessary to ensure that each set query can separate spj and pull related expressions outside of spj
  * At the same time, all related expressions of set query are isomorphic
  * */
int ObTransformUtils::check_correlated_exprs_can_pullup_for_set(const ObQueryRefRawExpr &query_ref,
                                                                const ObSelectStmt &subquery,
                                                                bool &can_pullup)
{
  int ret = OB_SUCCESS;
  can_pullup = true;
  const ObIArray<ObSelectStmt*> &set_queries = subquery.get_set_query();
  ObSelectStmt* first_query = NULL;
  if (subquery.get_set_op() != ObSelectStmt::UNION || subquery.is_recursive_union()) {
    //TODO: pullup correlated exprs for intersect and minus
    can_pullup = !subquery.is_set_distinct();
  }
  ObSEArray<ObRawExpr*, 4> left_select_exprs;
  ObSEArray<ObRawExpr*, 4> right_select_exprs;
  for (int64_t i = 0; can_pullup && OB_SUCC(ret) && i < set_queries.count(); ++i) {
    ObSelectStmt *right_query = set_queries.at(i);
    left_select_exprs.reuse();
    right_select_exprs.reuse();
    if (OB_ISNULL(right_query)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (right_query->is_set_stmt()) {
      can_pullup = false;
    } else if (OB_FAIL(check_correlated_exprs_can_pullup(query_ref, *right_query, can_pullup))) {
      LOG_WARN("failed to check correlated exprs can pullup", K(ret));
    } else if (!can_pullup) {
      //do nothing
    } else if (0 == i) {
      first_query = right_query;
    } else if (OB_FAIL(check_correlated_condition_isomorphic(first_query, 
                                                             right_query,
                                                             query_ref,
                                                             query_ref,
                                                             can_pullup,
                                                             left_select_exprs,
                                                             right_select_exprs))) {
      LOG_WARN("failed to check correlated subquery isomorphic", K(ret));                                                        
    }
  }
  return ret;
}

/**
  * Check whether the related expressions of even subqueries are isomorphic
  * Note: here only check select, where having
  * Calling this interface in other places requires additional attention
  * If new select exprs list is given, the corresponding relationship between the two subqueries separated by spj will be calculated
  */
int ObTransformUtils::check_correlated_condition_isomorphic(ObSelectStmt *left_query,
                                                            ObSelectStmt *right_query,
                                                            const ObQueryRefRawExpr &left_query_ref,
                                                            const ObQueryRefRawExpr &right_query_ref,
                                                            bool &is_valid,
                                                            ObIArray<ObRawExpr*> &left_new_select_exprs,
                                                            ObIArray<ObRawExpr*> &right_new_select_exprs)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSEArray<ObRawExpr*, 4> left_select_exprs;
  ObSEArray<ObRawExpr*, 4> right_select_exprs;
  ObSEArray<ObRawExpr*, 4> left_where_exprs;
  ObSEArray<ObRawExpr*, 4> right_where_exprs;
  ObSEArray<ObRawExpr*, 4> left_having_exprs;
  ObSEArray<ObRawExpr*, 4> right_having_exprs;
  ObArray<ObRawExpr *> exprs;
  bool is_correlated = false;
  //check select exprs
  //select expr needs to be one-to-one correspondence in order
  if (OB_ISNULL(left_query) || OB_ISNULL(right_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(add_correlated_flag(left_query_ref.get_exec_params(), exprs))) {
    LOG_WARN("failed to add mark flag", K(ret));
  } else if (OB_FAIL(add_correlated_flag(right_query_ref.get_exec_params(), exprs))) {
    LOG_WARN("failed to add mark flag", K(ret));
  } else if (OB_FAIL(mark_correlated_expr(left_query, is_correlated, exprs))) {
    LOG_WARN("failed to mark correlated expr", K(ret));
  } else if (OB_FAIL(mark_correlated_expr(right_query, is_correlated, exprs))) {
    LOG_WARN("failed to mark correlated expr", K(ret));
  } else if (OB_FAIL(left_query->get_select_exprs(left_select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(right_query->get_select_exprs(right_select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(is_correlated_exprs_isomorphic(left_select_exprs, 
                                                    right_select_exprs,
                                                    true, 
                                                    is_valid))) {
    LOG_WARN("failed to check exprs isomorphic", K(ret));
  }
  //check where conditions
  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(get_correlated_conditions(left_query_ref.get_exec_params(),
                                          left_query->get_condition_exprs(),
                                          left_where_exprs))) {
      LOG_WARN("failed to get correlated exprs", K(ret));
    } else if (OB_FAIL(get_correlated_conditions(right_query_ref.get_exec_params(),
                                                 right_query->get_condition_exprs(),
                                                 right_where_exprs))) {
      LOG_WARN("failed to get correlated exprs", K(ret));
    } else if (OB_FAIL(is_correlated_exprs_isomorphic(left_where_exprs, 
                                                      right_where_exprs,
                                                      false, 
                                                      is_valid))) {
      LOG_WARN("failed to check exprs isomorphic", K(ret));
    }
  }
  //check having conditions
  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(get_correlated_conditions(left_query_ref.get_exec_params(),
                                          left_query->get_having_exprs(),
                                          left_having_exprs))) {
      LOG_WARN("failed to get correlated exprs", K(ret));
    } else if (OB_FAIL(get_correlated_conditions(right_query_ref.get_exec_params(),
                                                 right_query->get_having_exprs(),
                                                 right_having_exprs))) {
      LOG_WARN("failed to get correlated exprs", K(ret));
    } else if (OB_FAIL(is_correlated_exprs_isomorphic(left_having_exprs, 
                                                      right_having_exprs,
                                                      false, 
                                                      is_valid))) {
      LOG_WARN("failed to check exprs isomorphic", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(unmark_correlated_expr(exprs))) {
      LOG_WARN("failed to unmark correlated expr", K(ret));
    }
  }
  //Align select exprs
  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(pullup_correlated_exprs(left_query_ref,
                                        left_select_exprs,
                                        left_new_select_exprs))) {
      LOG_WARN("failed to pullup correlated exprs", K(ret));
    } else if (OB_FAIL(pullup_correlated_exprs(left_query_ref,
                                               left_where_exprs,
                                               left_new_select_exprs))) {
      LOG_WARN("failed to pullup correlated exprs", K(ret));
    } else if (OB_FAIL(pullup_correlated_exprs(left_query_ref,
                                               left_having_exprs,
                                               left_new_select_exprs))) {
      LOG_WARN("failed to pullup correlated exprs", K(ret));
    } else if (OB_FAIL(pullup_correlated_exprs(right_query_ref,
                                               right_select_exprs,
                                               right_new_select_exprs))) {
      LOG_WARN("failed to pullup correlated exprs", K(ret));
    } else if (OB_FAIL(pullup_correlated_exprs(right_query_ref,
                                               right_where_exprs,
                                               right_new_select_exprs))) {
      LOG_WARN("failed to pullup correlated exprs", K(ret));
    } else if (OB_FAIL(pullup_correlated_exprs(right_query_ref,
                                               right_having_exprs,
                                               right_new_select_exprs))) {
      LOG_WARN("failed to pullup correlated exprs", K(ret));
    } else if (OB_FAIL(check_result_type_same(left_new_select_exprs, 
                                              right_new_select_exprs, 
                                              is_valid))) {
      LOG_WARN("failed to check expr result type", K(ret));
    }
  }
  return ret;
}


int ObTransformUtils::get_correlated_conditions(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                                const ObIArray<ObRawExpr*> &conds,
                                                ObIArray<ObRawExpr*> &correlated_conds)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
    bool bret = false;
    if (OB_FAIL(is_correlated_expr(exec_params, conds.at(i), bret))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!bret) {
      // do nothing
    } else if (OB_FAIL(correlated_conds.push_back(conds.at(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

// Whether the two sets of related expressions are isomorphic
//If the same structure, right is stored in the corresponding order of left
int ObTransformUtils::is_correlated_exprs_isomorphic(ObIArray<ObRawExpr *> &left_exprs,
                                                     ObIArray<ObRawExpr *> &right_exprs,
                                                     bool force_order,
                                                     bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = left_exprs.count() == right_exprs.count();
  int64_t N = left_exprs.count();
  ObSqlBitSet<> isomorphic_members;
  ObSEArray<ObRawExpr*, 4> new_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < N; ++i) {
    bool find = false;
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < N; ++j) {
      bool is_isomorphic = false;
      if (force_order && i != j) {
        //do nothing
      } else if (isomorphic_members.has_member(j)) {
        //do nothing
      } else if (OB_FAIL(is_correlated_expr_isomorphic(left_exprs.at(i),
                                                       right_exprs.at(j),
                                                       is_isomorphic))) {
        LOG_WARN("failed to check is correlated expr isomorphic", K(ret));
      } else if (!is_isomorphic) {
        //do nothing
      } else if (OB_FAIL(isomorphic_members.add_member(j))) {
        LOG_WARN("failed to add member", K(ret));
      } else if (OB_FAIL(new_exprs.push_back(right_exprs.at(j)))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        find = true;
      }
    }
    if (!find) {
      is_valid = false;
    }
  }
  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(right_exprs.assign(new_exprs))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  }
  return ret;
}

/**
  * If the two expressions are of the same type and the related expressions referenced are the same,
  * Is regarded as two expressions isomorphic
  **/
int ObTransformUtils::is_correlated_expr_isomorphic(ObRawExpr *left_expr,
                                                    ObRawExpr* right_expr,
                                                    bool &is_isomorphic)
{
  int ret = OB_SUCCESS;
  is_isomorphic = false;
  bool is_left_correlated = false;
  bool is_right_correlated = false;
  if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (left_expr == right_expr) {
    is_isomorphic = true;
  } else if (OB_FALSE_IT(is_left_correlated = left_expr->has_flag(BE_USED))) {
  } else if (OB_FALSE_IT(is_right_correlated = right_expr->has_flag(BE_USED))) {
  } else if (!is_left_correlated && !is_right_correlated) {
    is_isomorphic = true;
  } else if (!is_left_correlated || !is_right_correlated) {
    is_isomorphic = false;
  } else if (left_expr->get_expr_type() != right_expr->get_expr_type() ||
             left_expr->get_param_count() != right_expr->get_param_count()) {
    is_isomorphic = false;
  } else if (0 == left_expr->get_param_count()) {
    is_isomorphic = left_expr->same_as(*right_expr);
  } else {
    int64_t N = left_expr->get_param_count();
    is_isomorphic = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_isomorphic && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(is_correlated_expr_isomorphic(
                                        left_expr->get_param_expr(i),
                                        right_expr->get_param_expr(i),
                                        is_isomorphic)))) {
        LOG_WARN("failed to check is correlated expr isomorphic", K(ret));
      }
    }
  }
  return ret;
}

/// check whether any fixed expr is correlated
/// a expr is fixed if it can only be produced by current stmt
/// Given a fixed correlated expr, we must firstly produce the exec param,
/// secondly produce the fixed expr, hence such expr can not be pulled up.
int ObTransformUtils::check_fixed_expr_correlated(const ObQueryRefRawExpr &query_ref,
                                                  const ObSelectStmt &stmt,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> fixed_exprs;
  is_valid = true;
  if (OB_FAIL(append(fixed_exprs, stmt.get_aggr_items()))) {
    LOG_WARN("failed to append fixed exprs", K(ret));
  } else if (OB_FAIL(append(fixed_exprs, stmt.get_window_func_exprs()))) {
    LOG_WARN("failed to append window func exprs", K(ret));
  } else if (OB_FAIL(append(fixed_exprs, stmt.get_subquery_exprs()))) {
    LOG_WARN("failed to append subquery exprs", K(ret));
  } else if (OB_FAIL(append(fixed_exprs, stmt.get_pseudo_column_like_exprs()))) {
    LOG_WARN("failed to append pseudo column like exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < fixed_exprs.count(); ++i) {
    bool is_correlated = false;
    if (OB_FAIL(is_correlated_expr(query_ref.get_exec_params(),
                                   fixed_exprs.at(i),
                                   is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (is_correlated) {
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformUtils::check_can_pullup_conds(const ObSelectStmt &view, bool &has_special_expr)
{
  int ret = OB_SUCCESS;
  bool has_limit = view.has_limit();
  bool has_assign = view.is_contains_assignment();
  bool has_rownum = false;
  has_special_expr = false;
  if (OB_FAIL(view.has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else {
    has_special_expr = (has_limit | has_assign | has_rownum);
  }
  return ret;
}

// link.zt why not considering set stmt ?
int ObTransformUtils::is_table_item_correlated(
    const ObQueryRefRawExpr &query_ref, const ObSelectStmt &subquery, bool &contains)
{
  int ret = OB_SUCCESS;
  int64_t N = subquery.get_table_size();
  contains = false;
  for (int64_t i = 0; OB_SUCC(ret) && !contains && i < N; ++i) {
    const TableItem *table = subquery.get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table should not be NULL", K(ret), K(table));
    } else if (table->is_generated_table()) {
      if (OB_FAIL(is_correlated_subquery(query_ref.get_exec_params(), table->ref_query_, contains))) {
        LOG_WARN("check if subquery correlated failed", K(ret));
      }
    } else if (table->is_function_table()) {
      if (OB_FAIL(is_correlated_expr(query_ref.get_exec_params(), table->function_table_expr_, contains))) {
        LOG_WARN("failed to check function table expr correlated", K(ret));
      }
    } else if (table->is_json_table()) {
      if (OB_ISNULL(table->json_table_def_->doc_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (OB_FAIL(is_correlated_expr(query_ref.get_exec_params(), table->json_table_def_->doc_expr_, contains))) {
        LOG_WARN("failed to check function table expr correlated", K(ret));
      }
    } else if (table->is_values_table()) {
      for (int64_t j = 0; OB_SUCC(ret) && !contains && j < table->table_values_.count(); ++j) {
        if (OB_FAIL(is_correlated_expr(query_ref.get_exec_params(), table->table_values_.at(j), contains))) {
          LOG_WARN("failed to check values table expr correlated", K(ret));
        }
      }
    }
  }
  return ret;
}

// check joined table on conditions and semi info semi condition correlated
int ObTransformUtils::is_join_conditions_correlated(const ObQueryRefRawExpr &query_ref,
                                                    const ObSelectStmt *subquery,
                                                    bool &is_correlated)
{
  int ret = OB_SUCCESS;
  is_correlated = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(subquery), K(ret));
  } else {
    const ObIArray<JoinedTable*> &joined_tables = subquery->get_joined_tables();
    const ObIArray<SemiInfo*> &semi_infos = subquery->get_semi_infos();
    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < joined_tables.count(); ++i) {
      if (OB_FAIL(check_joined_conditions_correlated(query_ref.get_exec_params(),
                                                     joined_tables.at(i),
                                                     is_correlated))) {
        LOG_WARN("failed to check joined conditions correlated", K(ret));
      } else {/*do nothing*/}
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < semi_infos.count(); ++i) {
      if (OB_FAIL(check_semi_conditions_correlated(query_ref.get_exec_params(),
                                                   semi_infos.at(i),
                                                   is_correlated))) {
        LOG_WARN("failed to check semi conditions correlated", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_semi_conditions_correlated(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                                       const SemiInfo *semi_info,
                                                       bool &is_correlated)
{
  int ret = OB_SUCCESS;
  is_correlated = false;
  if (OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(semi_info), K(ret));
  } else {
    const ObIArray<ObRawExpr*> &semi_conds = semi_info->semi_conditions_;
    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < semi_conds.count(); ++i) {
      if (OB_FAIL(is_correlated_expr(exec_params, semi_conds.at(i), is_correlated))) {
        LOG_WARN("failed to check is correlated expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_joined_conditions_correlated(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                                         const JoinedTable *joined_table,
                                                         bool &is_correlated)
{
  int ret = OB_SUCCESS;
  is_correlated = false;
  if (OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(joined_table), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < joined_table->join_conditions_.count(); ++i) {
      if (OB_FAIL(is_correlated_expr(exec_params, joined_table->join_conditions_.at(i), is_correlated))) {
        LOG_WARN("failed to check is correlated join condition", K(ret));
      }
    }
    if (OB_SUCC(ret) && !is_correlated &&
        NULL != joined_table->left_table_ &&
        joined_table->left_table_->is_joined_table()) {
      const JoinedTable *left_table = static_cast<const JoinedTable *>(joined_table->left_table_);
      if (OB_FAIL(SMART_CALL(check_joined_conditions_correlated(exec_params, left_table, is_correlated)))) {
        LOG_WARN("failed to check left join table is correlated", K(ret));
      }
    }
    if (OB_SUCC(ret) && !is_correlated &&
        NULL != joined_table->right_table_ &&
        joined_table->right_table_->is_joined_table()) {
      const JoinedTable *right_table = static_cast<const JoinedTable *>(joined_table->right_table_);
      if (OB_FAIL(SMART_CALL(check_joined_conditions_correlated(exec_params, right_table, is_correlated)))) {
        LOG_WARN("failed to check right join table is correlated", K(ret));
      }
    }
  }
  return ret;
}

/**
  * If there are special expressions such as limit, rownum, assignment, etc.
  * Cannot pull up having predicates
  */
int ObTransformUtils::check_correlated_having_expr_can_pullup(
    const ObQueryRefRawExpr &query_ref,
    const ObSelectStmt &subquery,
    bool has_special_expr,
    bool &can_pullup)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> common_part_exprs;
  can_pullup = true;
  //Collect the partition by public expression of the window function
  //Used to check whether it is possible to pull up the condition of having
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery.get_window_func_count(); ++i) {
    const ObWinFunRawExpr *win_expr = NULL;
    if (OB_ISNULL(win_expr = subquery.get_window_func_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("window function expr is null", K(ret));
    } else if (i == 0) {
      if (OB_FAIL(common_part_exprs.assign(win_expr->get_partition_exprs()))) {
        LOG_WARN("failed to assign partition exprs", K(ret));
      }
    } else if (OB_FAIL(ObOptimizerUtil::intersect_exprs(common_part_exprs,
                                                        win_expr->get_partition_exprs(),
                                                        common_part_exprs))) {
      LOG_WARN("failed to intersect expr array", K(ret));
    } else if (common_part_exprs.empty()) {
      break;
    }
  }
  const ObIArray<ObRawExpr*> &having_exprs = subquery.get_having_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && can_pullup && i < having_exprs.count(); ++i) {
    ObSEArray<ObRawExpr *, 4> column_exprs;
    ObRawExpr *expr = having_exprs.at(i);
    bool is_correlated = false;
    if (OB_FAIL(is_correlated_expr(query_ref.get_exec_params(),
                                   expr,
                                   is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated) {
      //do nothing
    } else if (has_special_expr) {
      can_pullup = false;
    } else if (0 == subquery.get_window_func_count()) {
      //No window function does not need to be checked
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (!ObOptimizerUtil::subset_exprs(column_exprs, common_part_exprs)) {
      can_pullup = false;
    }
  }
  return ret;
}

/**
  * If there are special expressions such as limit, rownum, assignment, etc.
  * Can't pull up where predicate
  */
int ObTransformUtils::check_correlated_where_expr_can_pullup(
                                            const ObQueryRefRawExpr &query_ref,
                                            const ObSelectStmt &subquery,
                                            bool has_special_expr,
                                            bool &can_pullup)
{
  int ret = OB_SUCCESS;
  bool is_valid = false; 
  ObSEArray<ObRawExpr *, 4> common_part_exprs;
  can_pullup = true;
  //Common expressions of collection window functions partition by and group by
  if (OB_FAIL(ObOptimizerUtil::get_groupby_win_func_common_exprs(
                                                      subquery,
                                                      common_part_exprs,
                                                      is_valid))) {
    LOG_WARN("failed to get common exprs", K(ret));
  }
  const ObIArray<ObRawExpr*> &where_exprs = subquery.get_condition_exprs();
  for (int64_t i = 0; OB_SUCC(ret) && can_pullup && i < where_exprs.count(); ++i) {
    ObSEArray<ObRawExpr *, 4> column_exprs;
    ObRawExpr *expr = where_exprs.at(i);
    bool is_correlated = false;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (OB_FAIL(is_correlated_expr(query_ref.get_exec_params(),
                                          expr,
                                          is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated) {
      // do nothing
    } else if (has_special_expr) {
      can_pullup = false;
    } else if (!is_valid) {
      //No group by and window function
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (!ObOptimizerUtil::subset_exprs(column_exprs, common_part_exprs)) {
      //If the column of the current stmt contained in the expression does not 
      //belong to the subset of the common expressions of the 
      //window functions partition by and group by
      //That can't pull up the where condition
      can_pullup = false;
    }
  }
  return ret;
}

int ObTransformUtils::is_select_item_contain_subquery(const ObSelectStmt *subquery,
                                                      bool &contain)
{
  int ret = OB_SUCCESS;
  contain = false;
  if (OB_ISNULL(subquery)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(subquery), K(ret));
  } else {
    int64_t item_size = subquery->get_select_item_size();
    const ObRawExpr* expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !contain && i < item_size; ++i) {
      expr = subquery->get_select_item(i).expr_;
      if (OB_ISNULL(expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(expr), K(ret));
      } else if (expr->has_flag(CNT_SUB_QUERY)) {
        contain = true;
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTransformUtils::check_result_type_same(ObIArray<ObRawExpr*> &left_exprs, 
                                            ObIArray<ObRawExpr*> &right_exprs,
                                            bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = left_exprs.count() == right_exprs.count();
  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < left_exprs.count(); ++i) {
    if (OB_FAIL(check_result_type_same(left_exprs.at(i), right_exprs.at(i), is_same))) {
      LOG_WARN("failed to check result type is same", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::check_result_type_same(ObRawExpr* left_expr, 
                                             ObRawExpr* right_expr,
                                             bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = false;
  if (OB_ISNULL(left_expr) || OB_ISNULL(right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else {
    const ObExprResType &left_type = left_expr->get_result_type();
    const ObExprResType &right_type = right_expr->get_result_type();
    is_same = (left_type == right_type);
  }
  return ret;
}

/**
  * Separate spj and pull up related expressions
  * You need to call check_correlated_exprs_can_pullup before calling the interface
  * Ensure that all related expressions can be pulled up
  */
int ObTransformUtils::create_spj_and_pullup_correlated_exprs(const ObQueryRefRawExpr &query_ref,
                                                             ObSelectStmt *&subquery,
                                                             ObTransformerCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObStmtFactory *stmt_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObSEArray<ObRawExpr *, 4> new_select_list;
  ObSEArray<ObRawExpr *, 4> new_column_list;
  TableItem *view_table_item = NULL;
  ObSelectStmt *view_stmt = subquery;
  subquery = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(view_stmt) ||
      OB_ISNULL(session_info = ctx->session_info_) ||
      OB_ISNULL(stmt_factory = ctx->stmt_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt_factory), K(view_stmt));
  } else if (view_stmt->is_set_stmt()) {
    if (OB_FAIL(create_spj_and_pullup_correlated_exprs_for_set(query_ref, view_stmt, ctx))) {
      LOG_WARN("failed to create spj without correlated exprs for set", K(ret));
    } else {
      subquery = view_stmt;
    }
  } else if (OB_FAIL(ObTransformUtils::create_stmt_with_generated_table(ctx,
                                                                        view_stmt,
                                                                        subquery))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (subquery->get_table_items().count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect one table item", K(*subquery), K(ret));
  } else if (OB_ISNULL(view_table_item = subquery->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_FALSE_IT(subquery->get_select_items().reuse())) {
  } else if (OB_FALSE_IT(subquery->get_column_items().reuse())) {
  } else if (OB_FAIL(pullup_correlated_select_expr(query_ref,
                                                   *subquery,
                                                   *view_stmt,
                                                   new_select_list))) {
    LOG_WARN("failed to pullup correlated select expr", K(ret));
  } else if (OB_FAIL(pullup_correlated_conditions(query_ref,
                                                  view_stmt->get_having_exprs(),
                                                  subquery->get_condition_exprs(),
                                                  new_select_list))) {
    LOG_WARN("failed to pullup correlated exprs", K(ret));
  } else if (OB_FAIL(pullup_correlated_conditions(query_ref,
                                                  view_stmt->get_condition_exprs(),
                                                  subquery->get_condition_exprs(),
                                                  new_select_list))) {
    LOG_WARN("failed to pullup correlated conditions", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx,
                                                               *view_table_item,
                                                               subquery,
                                                               new_select_list,
                                                               new_column_list,
                                                               false))) {
    LOG_WARN("failed to create columns for view", K(ret));
  } else if (view_stmt->get_select_item_size() == 0 && 
             OB_FAIL(ObTransformUtils::create_dummy_select_item(*view_stmt, ctx))) {
    LOG_WARN("failed to create dummy select item", K(ret));
  } else if (!new_select_list.empty()) {
    int pos = 0;
    ObIArray<SelectItem> &select_items = subquery->get_select_items();
    for (int64_t i = 0; i < select_items.count(); ++i) {
      SelectItem &item  = select_items.at(i);
      if (OB_FAIL(replace_none_correlated_expr(item.expr_,
                                               query_ref,
                                               pos, 
                                               new_column_list))) {
        LOG_WARN("failed to replace expr", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (subquery->get_condition_size() > 0 &&
               OB_FAIL(replace_none_correlated_exprs(subquery->get_condition_exprs(), 
                                                     query_ref,
                                                     pos, 
                                                     new_column_list))) {
      LOG_WARN("failed to replace exprs", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(subquery->formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else {
      LOG_TRACE("succeed to create spj", K(*subquery));
    }
  }
  return ret;
}

int ObTransformUtils::create_spj_and_pullup_correlated_exprs_for_set(const ObQueryRefRawExpr &query_ref,
                                                                     ObSelectStmt *&stmt,
                                                                     ObTransformerCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!stmt->is_set_stmt()) {
    //do nothing
  } else {
    ObIArray<ObSelectStmt*> &set_queries = stmt->get_set_query();
    ObSEArray<ObSelectStmt*, 4> subqueries;
    ObSelectStmt *first_query = NULL;
    bool can_pullup = true;
    typedef ObSEArray<ObSEArray<ObRawExpr*, 4>, 4> MyArray;
    SMART_VARS_2((MyArray, left_new_select_exprs), (MyArray, right_new_select_exprs)) {
      if (OB_FAIL(left_new_select_exprs.prepare_allocate(set_queries.count()))) {
        LOG_WARN("failed to pre allocate", K(ret));
      } else if (OB_FAIL(right_new_select_exprs.prepare_allocate(set_queries.count()))) {
        LOG_WARN("failed to pre allocate", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < set_queries.count(); ++i) {
        ObSelectStmt *right_query = set_queries.at(i);
        if (OB_ISNULL(right_query)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null stmt", K(ret));
        } else if (0 == i) {
          first_query = right_query;
        } else if (OB_FAIL(check_correlated_condition_isomorphic(first_query, 
                                                                 right_query,
                                                                 query_ref,
                                                                 query_ref,
                                                                 can_pullup,
                                                                 left_new_select_exprs.at(i),
                                                                 right_new_select_exprs.at(i)))) {
          LOG_WARN("failed to check correlated subquery isomorphic", K(ret));                                                        
        }
      }
      //Separate spj for correlated subqueries
      for (int64_t i = 0; OB_SUCC(ret) && i < set_queries.count(); ++i) {
        ObSelectStmt *query = set_queries.at(i);
        if (OB_ISNULL(query)) {
          ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(ret));
        // somehow, the following implemenation is quite tircky,
        // the function actually modify the origin set stmt
        } else if (OB_FAIL(create_spj_and_pullup_correlated_exprs(query_ref, query, ctx))) {
          LOG_WARN("failed to create spj", K(ret));
        } else if (0 < i &&
                  OB_FAIL(adjust_select_item_pos(right_new_select_exprs.at(i),
                                                 set_queries.at(i)))) {
          LOG_WARN("failed to adjust select item pos", K(ret));
        } else if (OB_FAIL(subqueries.push_back(query))) {
          LOG_WARN("failed to push back query", K(ret));
        } else if (0 == i) {
          first_query = set_queries.at(i);
        }
      }
    }
    //Combine related expressions
    if (OB_SUCC(ret)) {
      //Reuse the first stmt
      ObSelectStmt *first_query = subqueries.at(0);
      TableItem *view_table = NULL;
      stmt->get_select_items().reuse();
      if (OB_ISNULL(first_query)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(ret));
      } else if (OB_UNLIKELY(1 != first_query->get_table_items().count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect table item size", KPC(first_query), K(ret));
      } else if (OB_ISNULL(view_table = first_query->get_table_item(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (OB_UNLIKELY(!view_table->is_generated_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect generate table", KPC(view_table), K(ret));
      } else if (OB_FAIL(stmt->create_select_list_for_set_stmt(*ctx->expr_factory_))) {
        LOG_WARN("failed to create select list for union", K(ret));
      } else {
        view_table->ref_query_ = stmt;
        stmt = first_query;
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_exprs_contain_lob_type(ObIArray<ObRawExpr *> &exprs, bool &has_lob)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> col_exprs;
  has_lob = false;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(exprs, col_exprs))) {
    LOG_WARN("extract colum failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_lob && i < col_exprs.count(); i++) {
    ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(col_exprs.at(i));
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("epxr is null", K(ret));
    } else {
      has_lob = (ObLobType == col_expr->get_result_type().get_type() ||
		    ObLongTextType == col_expr->get_result_type().get_type());
    }
  }
  return ret;
}

int ObTransformUtils::check_expr_contain_lob_type(ObRawExpr *expr, bool &has_lob)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 1> exprs;
  if (OB_FAIL(exprs.push_back(expr))) {
    LOG_WARN("push back failed", K(ret));
  } else if (OB_FAIL(check_exprs_contain_lob_type(exprs, has_lob))) {
    LOG_WARN("expr contains lob failed", K(ret));
  }
  return ret;
}

int ObTransformUtils::check_has_assignment(const ObDMLStmt &stmt, bool &has_assignment)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt *, 4> childs;
  has_assignment = stmt.is_contains_assignment();
  if (has_assignment) {
    //do nothing
  } else if (OB_FAIL(stmt.get_child_stmts(childs))) {
    LOG_WARN("get sel exprs failed", K(ret));
  } else {
    for (int64_t i=0; OB_SUCC(ret) && !has_assignment && i < childs.count(); i++) {
      if (OB_ISNULL(childs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is null", K(ret));
      } else if (SMART_CALL(check_has_assignment(*childs.at(i), has_assignment))) {
        LOG_WARN("check_assignment failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::adjust_select_item_pos(ObIArray<ObRawExpr*> &right_select_exprs,
                                            ObSelectStmt *right_query)
{
  int ret = OB_SUCCESS;
  ObSEArray<SelectItem, 4> new_select_items;
  if (OB_ISNULL(right_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (right_select_exprs.empty() && 1 == right_query->get_select_item_size()) {
    //dummy output，do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < right_select_exprs.count(); ++i) {
      bool find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < right_query->get_select_item_size(); ++j) {
        const SelectItem &right_item = right_query->get_select_item(j);
        if (right_item.expr_ != right_select_exprs.at(i)) {
          //do nothing
        } else if (OB_FAIL(new_select_items.push_back(right_item))) {
          LOG_WARN("failed to push back select item", K(ret));
        } else {
          find = true;
        }
      }
      if (OB_SUCC(ret) && !find) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect select expr", KPC(right_select_exprs.at(i)), KPC(right_query), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(right_query->get_select_items().assign(new_select_items))) {
        LOG_WARN("failed to assign select items", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::replace_none_correlated_exprs(ObIArray<ObRawExpr*> &exprs,
                                                    const ObQueryRefRawExpr &query_ref,
                                                    int &pos,
                                                    ObIArray<ObRawExpr*> &new_column_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(replace_none_correlated_expr(exprs.at(i),
                                             query_ref,
                                             pos,
                                             new_column_list))) {
      LOG_WARN("failed to pullup correlated expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::replace_none_correlated_expr(ObRawExpr *&expr,
                                                  const ObQueryRefRawExpr &query_ref,
                                                  int &pos,
                                                  ObIArray<ObRawExpr*> &new_column_list)
{
  int ret = OB_SUCCESS;
  bool is_correlated = false;
  bool is_scalar = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (expr->is_static_const_expr()) {
    //do nothing
  } else if (OB_FAIL(is_scalar_expr(expr, is_scalar))) {
    LOG_WARN("failed to check is scalar expr", K(ret));
  } else if (OB_FAIL(is_correlated_expr(query_ref.get_exec_params(),
                                        expr,
                                        is_correlated))) {
    LOG_WARN("failed to check is correlated expr", K(ret));
  } else if (is_scalar && !is_correlated) {
    if (expr->is_exec_param_expr()) {
      //do nothing
    } else if (pos >= new_column_list.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect array pos", K(pos), K(new_column_list), K(ret));
    } else {
      expr = new_column_list.at(pos);
      ++pos;
    }
  } else {
    int64_t N = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(replace_none_correlated_expr(expr->get_param_expr(i),
                                                          query_ref,
                                                          pos,
                                                          new_column_list)))) {
        LOG_WARN("failed to pullup correlated expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::pullup_correlated_exprs(const ObQueryRefRawExpr &query_ref,
                                              ObIArray<ObRawExpr*> &exprs,
                                              ObIArray<ObRawExpr*> &new_select_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *expr = exprs.at(i);
    bool is_correlated = false;
    if (OB_FAIL(pullup_correlated_expr(query_ref, expr, new_select_list, is_correlated))) {
      LOG_WARN("failed to pullup correlated expr", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::pullup_correlated_expr(const ObQueryRefRawExpr &query_ref,
                                             ObRawExpr *expr,
                                             ObIArray<ObRawExpr*> &new_select_list,
                                             bool &is_correlated)
{
  int ret = OB_SUCCESS;
  is_correlated = false;
  bool is_scalar = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (expr->is_static_const_expr()) {
    //do nothing
  } else if (OB_FAIL(is_scalar_expr(expr, is_scalar))) {
    LOG_WARN("failed to check is scalar expr", K(ret));
  } else if (OB_FAIL(is_correlated_expr(query_ref.get_exec_params(),
                                        expr,
                                        is_correlated))) {
    LOG_WARN("failed to check is correlated expr", K(ret));
  } else if (is_scalar && !is_correlated) {
    if (expr->is_exec_param_expr()) {
      //do nothing
    } else if (OB_FAIL(new_select_list.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  } else {
    int64_t N = expr->get_param_count();
    bool param_correlated = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(pullup_correlated_expr(query_ref,
                                                    expr->get_param_expr(i),
                                                    new_select_list,
                                                    param_correlated)))) {
        LOG_WARN("failed to pullup correlated expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::pullup_correlated_select_expr(const ObQueryRefRawExpr &query_ref,
                                                    ObSelectStmt &stmt,
                                                    ObSelectStmt &view,
                                                    ObIArray<ObRawExpr*> &new_select_list)
{
  int ret = OB_SUCCESS;
  ObIArray<SelectItem> &select_items = view.get_select_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
    ObRawExpr *expr = select_items.at(i).expr_;
    bool is_correlated = false;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select epxr is null", K(ret));
    } else if (OB_FAIL(pullup_correlated_expr(query_ref,
                                              expr,
                                              new_select_list,
                                              is_correlated))) {
      LOG_WARN("failed to pullup correlated expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    //Expressions that are not pulled up will be replaced 
    //after the select item of the view is recreated
    if (OB_FAIL(stmt.get_select_items().assign(select_items))) {
      LOG_WARN("failed to assign select items", K(ret));
    } else {
      //The select item of the view needs to be recreated 
      //after the relevant expression is pulled up
      view.get_select_items().reset();
    }
  }
  return ret;
}

int ObTransformUtils::pullup_correlated_conditions(const ObQueryRefRawExpr &query_ref,
                                                   ObIArray<ObRawExpr *> &exprs,
                                                   ObIArray<ObRawExpr *> &pullup_exprs,
                                                   ObIArray<ObRawExpr *> &new_select_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> remain_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *expr = exprs.at(i);
    bool is_correlated = false;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("having condition is null", K(ret));
    } else if (OB_FAIL(is_correlated_expr(query_ref.get_exec_params(),
                                          expr,
                                          is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (!is_correlated) {
      //非相关条件不需要上拉
      ret = remain_exprs.push_back(expr);
    } else if (OB_FAIL(pullup_correlated_expr(query_ref,
                                              expr,
                                              new_select_list,
                                              is_correlated))) {
      LOG_WARN("failed to pullup correlated expr", K(ret));
    } else if (OB_FAIL(pullup_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(exprs.assign(remain_exprs))) {
      LOG_WARN("failed to assign having exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::extract_rowid_exprs(ObIArray<ObRawExpr *> &exprs,
                                          ObIArray<ObRawExpr *> &rowid_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(extract_rowid_exprs(exprs.at(i), rowid_exprs))) {
      LOG_WARN("Failed to extract rowid exprs", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObTransformUtils::extract_rowid_exprs(ObRawExpr *expr, ObIArray<ObRawExpr *> &rowid_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else if (expr->has_flag(IS_ROWID)) {//calc rowid expr
    if (OB_FAIL(add_var_to_array_no_dup(rowid_exprs, expr))) {
      LOG_WARN("failed to add var to array no dup", K(ret));
    } else {/*do nothing*/}
  } else if (expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(expr);
    if (col_expr->get_column_id() == OB_INVALID_ID &&
        ObCharset::case_insensitive_equal(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME,
                                          col_expr->get_column_name())) {//mock empty rowid expr
      if (OB_FAIL(add_var_to_array_no_dup(rowid_exprs, expr))) {
        LOG_WARN("failed to add var to array no dup", K(ret));
      } else {/*do nothing*/}
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_rowid_exprs(expr->get_param_expr(i), rowid_exprs)))) {
        LOG_WARN("Failed to extract rowid exprs", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObTransformUtils::extract_udt_exprs(ObRawExpr *expr, ObIArray<ObRawExpr *> &udt_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else if (expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(expr);
    if (ob_is_extend(col_expr->get_data_type())
        || ob_is_user_defined_sql_type(col_expr->get_data_type())) {
      if (OB_FAIL(add_var_to_array_no_dup(udt_exprs, expr))) {
        LOG_WARN("failed to add var to array no dup", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_udt_exprs(expr->get_param_expr(i), udt_exprs)))) {
        LOG_WARN("Failed to extract rowid exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::is_batch_stmt_write_table(uint64_t table_id,
                                                const ObDMLStmt &stmt,
                                                bool &is_target_table)
{
  int ret = OB_SUCCESS;
  is_target_table = false;
  const TableItem *table_item = nullptr;
  ObSelectStmt *ref_query_stmt = NULL;
  if (OB_ISNULL(table_item = stmt.get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is not found", K(ret), K(table_id), K(stmt.get_table_items()));
  } else if (OB_ISNULL(ref_query_stmt = table_item->ref_query_)) {
    // if ref_query_ is null, ignore this
  } else if (ref_query_stmt->contain_ab_param()) {
    is_target_table = true;
  }

  return ret;
}

//new heap table not add partition key in rowkey and the tablet id is unique in partition
//we need add partition key.
int ObTransformUtils::add_part_column_exprs_for_heap_table(const ObDMLStmt *stmt,
                                                           const ObTableSchema *table_schema,
                                                           const uint64_t table_id,
                                                           ObIArray<ObRawExpr *> &unique_keys)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(table_schema));
  } else if (table_schema->is_heap_table()) {
    const ObRawExpr *part_expr = stmt->get_part_expr(table_id, table_schema->get_table_id());
    const ObRawExpr *subpart_expr = stmt->get_subpart_expr(table_id, table_schema->get_table_id());
    if (part_expr != NULL &&
        OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, unique_keys))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (subpart_expr != NULL &&
               OB_FAIL(ObRawExprUtils::extract_column_exprs(subpart_expr, unique_keys))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else {
      LOG_TRACE("Succeed to add part column exprs for heap table",
                                                 KPC(part_expr), KPC(subpart_expr), K(unique_keys));
    }
  }
  return ret;
}

int ObTransformUtils::get_generated_table_item(ObDMLStmt &parent_stmt,
                                               ObDMLStmt *child_stmt,
                                               TableItem *&table_item)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < parent_stmt.get_table_size(); ++i) {
    TableItem *table = parent_stmt.get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (!table->is_generated_table()) {
      // do nothing
    } else if (table->ref_query_ == child_stmt) {
      table_item = table;
      break;
    }
  }
  return ret;
}

int ObTransformUtils::get_table_related_condition(ObDMLStmt &stmt,
                                                  const TableItem *table,
                                                  ObIArray<ObRawExpr *> &conditions)
{
  int ret = OB_SUCCESS;
  bool is_semi_right_table = false;;
  for (int64_t i = 0; OB_SUCC(ret) && !is_semi_right_table && i < stmt.get_semi_info_size(); ++i) {
    SemiInfo *semi_info = stmt.get_semi_infos().at(i);
    if (OB_ISNULL(semi_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (semi_info->right_table_id_ == table->table_id_) {
      // generated table is semi/anti join right table, table columns will only appear
      // in semi conditions.
      is_semi_right_table = true;
      ret = conditions.assign(semi_info->semi_conditions_);
    }
  }
  if (OB_SUCC(ret) && !is_semi_right_table) {
    // generated table is not semi/anti join right table, table columns will appear in
    // where, joined table and semi join condition
    if (OB_FAIL(conditions.assign(stmt.get_condition_exprs()))) {
      LOG_WARN("failed to get condition exprs", K(ret));
    } else if (OB_FAIL(get_condition_from_joined_table(stmt, table, conditions))) {
      LOG_WARN("failed to get extra conditions", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_semi_info_size(); ++i) {
        SemiInfo *semi_info = stmt.get_semi_infos().at(i);
        if (OB_ISNULL(semi_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (semi_info->is_semi_join()) {
          // anti join condition is not null reject
          ret = append(conditions, semi_info->semi_conditions_);
        }
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_condition_from_joined_table(ObDMLStmt &stmt,
                                                      const TableItem *target_table,
                                                      ObIArray<ObRawExpr *> &conditions)
{
  int ret = OB_SUCCESS;
  bool add_on_condition = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_joined_tables().count(); ++i) {
    JoinedTable *joined_table = stmt.get_joined_tables().at(i);
    if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!ObOptimizerUtil::find_item(joined_table->single_table_ids_, target_table->table_id_)) {
      // do nothing
    } else if (OB_FAIL(extract_joined_table_condition(joined_table, target_table, conditions, add_on_condition))) {
      LOG_WARN("faield to extract joined table condition", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::extract_joined_table_condition(TableItem *table_item,
                                                     const TableItem *target_table,
                                                     ObIArray<ObRawExpr *> &conditions,
                                                     bool &add_on_condition)
{
  int ret = OB_SUCCESS;
  add_on_condition = false;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table_item == target_table) {
    add_on_condition = true;
  } else if (table_item->is_joined_table()) {
    JoinedTable *joined_table = static_cast<JoinedTable *>(table_item);
    bool check_left = false;
    bool check_right = false;
    if (LEFT_OUTER_JOIN == joined_table->joined_type_) {
      check_right = true;
    } else if (RIGHT_OUTER_JOIN == joined_table->joined_type_) {
      check_left = true;
    } else if (INNER_JOIN == joined_table->joined_type_) {
      check_left = true;
      check_right = true;
    }
    if (OB_SUCC(ret) && check_left && !add_on_condition) {
      if (OB_FAIL(SMART_CALL(extract_joined_table_condition(joined_table->left_table_,
                                                            target_table,
                                                            conditions,
                                                            add_on_condition)))) {
        LOG_WARN("failed to extract joined table condition", K(ret));
      }
    }

    if (OB_SUCC(ret) && check_right && !add_on_condition) {
      if (OB_FAIL(SMART_CALL(extract_joined_table_condition(joined_table->right_table_,
                                                            target_table,
                                                            conditions,
                                                            add_on_condition)))) {
        LOG_WARN("failed to extract joined table condition", K(ret));
      }
    }

    if (OB_SUCC(ret) && add_on_condition &&
        OB_FAIL(append(conditions, joined_table->get_join_conditions()))) {
      LOG_WARN("failed to append join conditions", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::extract_const_bool_expr_info(ObTransformerCtx *ctx,
                                                   const common::ObIArray<ObRawExpr*> &exprs,
                                                   common::ObIArray<int64_t> &true_exprs,
                                                   common::ObIArray<int64_t> &false_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(ret));
  } else {
    ObObj result;
    bool is_true = false;
    bool is_valid = false;
    ObRawExpr *temp = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      is_valid = false;
      if (OB_ISNULL(temp = exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(calc_const_expr_result(temp, ctx, result, is_valid))) {
        LOG_WARN("fail to calc const expr", K(ret));
      }
      if (OB_FAIL(ret) || !is_valid) {
      } else if (OB_FAIL(ObObjEvaluator::is_true(result, is_true))) {
        LOG_WARN("failed to get bool value", K(ret));
      } else if (!is_true && OB_FAIL(false_exprs.push_back(i))) {
        LOG_WARN("failed to push back into array", K(ret));
      } else if (is_true && OB_FAIL(true_exprs.push_back(i))) {
        LOG_WARN("failed to push back into array", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTransformUtils::calc_const_expr_result(ObRawExpr * expr,
                                             ObTransformerCtx *ctx,
                                             ObObj &result,
                                             bool &calc_happend)
{
  int ret = OB_SUCCESS;
  calc_happend = false;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(session = ctx->session_info_) || OB_ISNULL(ctx->allocator_)
      || OB_ISNULL(ctx->exec_ctx_) || OB_ISNULL(ctx->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!expr->is_static_scalar_const_expr()) {
    // do nothing		    bool dummy_bool = false;
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx->exec_ctx_,
                                      expr,
                                      result,
                                      calc_happend,
                                      *ctx->allocator_))) {
    LOG_WARN("failed to calc const or calculable expr", K(ret));
  }
  return ret;
}

int ObTransformUtils::extract_target_exprs_by_idx(const ObIArray<ObRawExpr*> &all_exprs,
                                                  const ObIArray<int64_t> &target_idx,
                                                  ObIArray<ObRawExpr*> &target_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < target_idx.count() ; ++i) {
    if (OB_UNLIKELY(target_idx.at(i) < 0 || target_idx.at(i) >= all_exprs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid array pos", K(ret), K(target_idx.at(i)), K(all_exprs.count()));
    } else if (OB_FAIL(target_exprs.push_back(all_exprs.at(target_idx.at(i))))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformUtils::check_integer_result_type(common::ObIArray<ObRawExpr*> &exprs,
                                                bool &is_valid_type)
{
  int ret = OB_SUCCESS;
  is_valid_type = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid_type && i < exprs.count(); i++) {
    ObRawExpr *tmp = NULL;
    if (OB_ISNULL(tmp = exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else {
      is_valid_type &= tmp->get_result_type().is_integer_type();
    }
  }
  return ret;
}

int ObTransformUtils::construct_trans_tables(const ObDMLStmt *stmt,
                                             const ObIArray<TableItem *> &tables,
                                             ObIArray<TableItem *> &trans_tables)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      if (OB_ISNULL(tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(construct_trans_table(stmt,
                                               tables.at(i),
                                               trans_tables))) {
        LOG_WARN("failed to construct eliminated table", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::get_sorted_table_hint(ObSEArray<TableItem *, 4> &tables,
                                            ObIArray<ObTableInHint> &table_hints) {
  int ret = OB_SUCCESS;
  if (tables.count() > 1) {
    auto cmp_func = [](TableItem* a, TableItem* b) {
      if (OB_ISNULL(a) || OB_ISNULL(b)) {
        return false;
      } else {
        return a->table_id_ > b->table_id_;
      }
    };
    std::sort(tables.begin(), tables.end(), cmp_func);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
    TableItem *table = tables.at(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(table_hints.push_back(
               ObTableInHint(table->qb_name_, table->database_name_, table->get_object_name())))) {
      LOG_WARN("failed to push back hint table", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::construct_trans_table(const ObDMLStmt *stmt,
                                              const TableItem *table,
                                              ObIArray<TableItem *> &trans_tables)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table->is_joined_table()) {
    const JoinedTable *joined_table = static_cast<const JoinedTable *>(table);
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->single_table_ids_.count(); ++i) {
      TableItem *table = stmt->get_table_item_by_id(joined_table->single_table_ids_.at(i));
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(trans_tables.push_back(table))) {
        LOG_WARN("failed to push back trans table", K(ret));
      }
    }
  } else if (OB_FAIL(trans_tables.push_back(const_cast<TableItem *>(table)))) {
    LOG_WARN("failed to push back table", K(ret));
  }
  return ret;
}

int ObTransformUtils::get_exprs_relation_ids(ObIArray<ObRawExpr*> &exprs, ObSqlBitSet<> &exprs_relation_ids)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(exprs_relation_ids.add_members(exprs.at(i)->get_relation_ids()))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  return ret;
}

/**
 * @brief get lazy left join from tables
 * @param [IN]tables: input tables
 * @param [IN]expr_relation_ids: relation ids that left join`right table can not use
 * @param [OUT]lazy_join_infos: output lazy left join
 */
int ObTransformUtils::get_lazy_left_join(ObDMLStmt *stmt,
                                         const ObIArray<TableItem *> &tables,
                                         ObSqlBitSet<> &expr_relation_ids,
                                         ObIArray<LazyJoinInfo> &lazy_join_infos)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
    if (OB_FAIL(inner_get_lazy_left_join(stmt, 
                                         tables.at(i), 
                                         expr_relation_ids,
                                         lazy_join_infos,
                                         false))) {
      LOG_WARN("failed to get lazy left join", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::inner_get_lazy_left_join(ObDMLStmt *stmt,
                                              TableItem *table,
                                              ObSqlBitSet<> &expr_relation_ids,
                                              ObIArray<LazyJoinInfo> &lazy_join_infos,
                                              bool in_full_join)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (!table->is_joined_table()) {
    //do nothing
  } else {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table);
    if (LEFT_OUTER_JOIN == joined_table->joined_type_) {
      bool is_valid = false;
      if (OB_FAIL(check_lazy_left_join_valid(stmt, 
                                             joined_table, 
                                             expr_relation_ids, 
                                             in_full_join, 
                                             is_valid))) {
        LOG_WARN("failed to check lazy left join valid", K(ret));
      } else if (is_valid) {
        LazyJoinInfo lazy_join;
        lazy_join.right_table_ = joined_table->right_table_;
        if (OB_FAIL(lazy_join.join_conditions_.assign(joined_table->join_conditions_))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else if (OB_FAIL(lazy_join_infos.push_back(lazy_join))) {
          LOG_WARN("failed to push back lazy join table", K(ret));
        }
      } else if (OB_FAIL(get_exprs_relation_ids(joined_table->join_conditions_, 
                                                expr_relation_ids))) {
        LOG_WARN("failed to add expr relation ids", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(SMART_CALL(inner_get_lazy_left_join(stmt, 
                                                        joined_table->left_table_,
                                                        expr_relation_ids, 
                                                        lazy_join_infos,
                                                        in_full_join)))) {
          LOG_WARN("failed to get lazy left join", K(ret));
        }
      }
    } else if (INNER_JOIN == joined_table->joined_type_ ||
               FULL_OUTER_JOIN == joined_table->joined_type_) {
      in_full_join |= FULL_OUTER_JOIN == joined_table->joined_type_;
      if (OB_FAIL(get_exprs_relation_ids(joined_table->join_conditions_, 
                                                           expr_relation_ids))) {
        LOG_WARN("failed to add expr relation ids", K(ret));
      } else if (OB_FAIL(SMART_CALL(inner_get_lazy_left_join(stmt, 
                                                            joined_table->left_table_, 
                                                            expr_relation_ids, 
                                                            lazy_join_infos,
                                                            in_full_join)))) {
          LOG_WARN("failed to get lazy left join", K(ret));
        } else if (OB_FAIL(SMART_CALL(inner_get_lazy_left_join(stmt, 
                                                              joined_table->right_table_, 
                                                              expr_relation_ids, 
                                                              lazy_join_infos,
                                                              in_full_join)))) {
          LOG_WARN("failed to get lazy left join", K(ret));
        }
    }
  }
  return ret;
}



int ObTransformUtils::check_lazy_left_join_valid(ObDMLStmt *stmt,
                                                JoinedTable *table,
                                                ObSqlBitSet<> &expr_relation_ids,
                                                bool in_full_join,
                                                bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObSqlBitSet<> left_table_ids;
  ObSqlBitSet<> right_table_ids;
  ObRelIds left_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(table) ||
      OB_ISNULL(table->right_table_) || OB_ISNULL(table->left_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(*table->right_table_, right_table_ids))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else if (expr_relation_ids.overlap(right_table_ids)) {
    is_valid = false;
  } else if (!in_full_join) {
    is_valid = true;
  } else if (OB_FAIL(stmt->get_table_rel_ids(*table->left_table_, left_table_ids))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else if (OB_FAIL(left_ids.add_members(left_table_ids))) {
    LOG_WARN("failed to add members", K(ret));
  } else if (OB_FAIL(is_null_reject_conditions(table->join_conditions_,
                                               left_ids,
                                               is_valid))) {
    LOG_WARN("failed to check is null reject conditions", K(ret));
  }
  return ret;
}

int ObTransformUtils::get_join_keys(ObIArray<ObRawExpr*> &conditions,
                                    ObSqlBitSet<> &table_ids,
                                    ObIArray<ObRawExpr*> &join_keys,
                                    bool &is_simply_join)
{
  int ret = OB_SUCCESS;
  is_simply_join = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_simply_join && i < conditions.count(); ++i) {
    ObRawExpr *expr = conditions.at(i);
    ObRawExpr *l_expr = NULL;
    ObRawExpr *r_expr = NULL;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!expr->has_flag(IS_JOIN_COND)) {
      is_simply_join = false;
    } else if (2 != expr->get_param_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect expr param count", K(ret));
    } else if (OB_ISNULL(l_expr = expr->get_param_expr(0)) || 
               OB_ISNULL(r_expr = expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!l_expr->get_relation_ids().overlap(table_ids) && 
               r_expr->get_relation_ids().is_subset(table_ids)) {
      if (OB_FAIL(join_keys.push_back(r_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (l_expr->get_relation_ids().overlap(table_ids) && 
               !r_expr->get_relation_ids().is_subset(table_ids)) {
      if (OB_FAIL(join_keys.push_back(l_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else {
      is_simply_join = false;
    }
  }
  return ret;
}

/**
 * @brief check joined table can combin with target table
 * @param is_right_child: target table is right child of joined table
 */
int ObTransformUtils::check_joined_table_combinable(ObDMLStmt *stmt,
                                                    JoinedTable *joined_table,
                                                    TableItem *target_table,
                                                    bool is_right_child,
                                                    bool &combinable)
{
  int ret = OB_SUCCESS;
  combinable = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table) ||
      OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (LEFT_OUTER_JOIN != joined_table->joined_type_ && 
             RIGHT_OUTER_JOIN != joined_table->joined_type_) {
    //TODO: INNER JOIN、FULL JOIN、SEMI JOIN、ANTI JOIN
  } else if (is_right_child) {
    if (target_table->is_generated_table()) {
      if (OB_FAIL(check_left_join_right_view_combinable(stmt, 
                                                        target_table, 
                                                        joined_table->join_conditions_, 
                                                        combinable))) {
        LOG_WARN("failed to check left join right view combinable", K(ret));
      }
    } else if (target_table->is_joined_table()) {
      //TODO: check joined table
    } else {
      //do nothing
    }
  } else {
    //TODO: check left child
  }
  return ret;
}

int ObTransformUtils::check_left_join_right_view_combinable(ObDMLStmt *parent_stmt,
                                                            TableItem *view_table,
                                                            ObIArray<ObRawExpr*> &outer_join_conditions,
                                                            bool &combinable)
{
  int ret = OB_SUCCESS;
  combinable = false;
  ObSelectStmt *child_stmt = NULL;
  ObSqlBitSet<> outer_expr_relation_ids;
  ObSEArray<ObRawExpr*, 4> view_exprs;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(view_table) || 
      !view_table->is_generated_table() || 
      OB_ISNULL(child_stmt = view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(get_view_exprs(parent_stmt, 
                                    view_table, 
                                    outer_join_conditions, 
                                    view_exprs))) {
    LOG_WARN("failed to get view exprs", K(ret));
  } else if (OB_FAIL(get_exprs_relation_ids(view_exprs, 
                                            outer_expr_relation_ids))) {
    LOG_WARN("failed to add expr relation ids", K(ret));
  } else if (OB_FAIL(get_exprs_relation_ids(child_stmt->get_condition_exprs(), 
                                            outer_expr_relation_ids))) {
    LOG_WARN("failed to add expr relation ids", K(ret));
  } else {
    ObIArray<JoinedTable*> &joined_tables = child_stmt->get_joined_tables();
    ObIArray<SemiInfo*> &semi_infos = child_stmt->get_semi_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      if (OB_FAIL(get_left_rel_ids_from_semi_info(child_stmt, 
                                                  semi_infos.at(i), 
                                                  outer_expr_relation_ids))) {
        LOG_WARN("failed to get left table ids from semi info", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !combinable && i < joined_tables.count(); ++i) {
      if (OB_FAIL(inner_check_left_join_right_table_combinable(child_stmt,
                                                              joined_tables.at(i), 
                                                              outer_expr_relation_ids, 
                                                              combinable))) {
        LOG_WARN("failed to check left join`s right table combinable", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::inner_check_left_join_right_table_combinable(ObSelectStmt *child_stmt, 
                                                                  TableItem *table, 
                                                                  ObSqlBitSet<> &outer_expr_relation_ids, 
                                                                  bool &combinable)
{
  int ret = OB_SUCCESS;
  combinable = false;
  if (OB_ISNULL(child_stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (table->is_joined_table()) {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table);
    if (LEFT_OUTER_JOIN == joined_table->joined_type_ ||
        RIGHT_OUTER_JOIN == joined_table->joined_type_) {
      ObSqlBitSet<> left_table_ids;
      ObSqlBitSet<> right_table_ids;
      ObRelIds left_ids;
      TableItem *left_table = (LEFT_OUTER_JOIN == joined_table->joined_type_) ? 
                                joined_table->left_table_ : 
                                joined_table->right_table_;
      TableItem *right_table = (RIGHT_OUTER_JOIN == joined_table->joined_type_) ? 
                                joined_table->left_table_ : 
                                joined_table->right_table_;
      if (OB_ISNULL(left_table) || OB_ISNULL(right_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table", K(ret));
      } else if (OB_FAIL(child_stmt->get_table_rel_ids(*right_table, right_table_ids))) {
        LOG_WARN("failed to get table rel ids", K(ret));
      } else if (outer_expr_relation_ids.overlap(right_table_ids)) {
        combinable = false;
      } else if (OB_FAIL(child_stmt->get_table_rel_ids(*left_table, left_table_ids))) {
        LOG_WARN("failed to get table rel ids", K(ret));
      } else if (OB_FAIL(left_ids.add_members(left_table_ids))) {
        LOG_WARN("failed to add members", K(ret));
      } else if (OB_FAIL(is_null_reject_conditions(joined_table->join_conditions_,
                                                   left_ids,
                                                   combinable))) {
        LOG_WARN("failed to check is null reject conditions", K(ret));
      }
      if (OB_SUCC(ret) && !combinable) {
        if (OB_FAIL(ObTransformUtils::get_exprs_relation_ids(joined_table->join_conditions_, 
                                                             outer_expr_relation_ids))) {
          LOG_WARN("failed to add expr relation ids", K(ret));
        } else if (OB_FAIL(SMART_CALL(inner_check_left_join_right_table_combinable(child_stmt, 
                                                                                  left_table,
                                                                                  outer_expr_relation_ids, 
                                                                                  combinable)))) {
          LOG_WARN("failed to check left join`s right table combinable", K(ret));
        }
      }
    } else if (INNER_JOIN == joined_table->joined_type_) {
      if (OB_FAIL(get_exprs_relation_ids(joined_table->join_conditions_, 
                                         outer_expr_relation_ids))) {
        LOG_WARN("failed to add expr relation ids", K(ret));
      } else if (OB_FAIL(SMART_CALL(inner_check_left_join_right_table_combinable(child_stmt,
                                                                                joined_table->left_table_, 
                                                                                outer_expr_relation_ids, 
                                                                                combinable)))) {
        LOG_WARN("failed to check left join`s right table combinable", K(ret));
      } else if (combinable) {
        //find
      } else if (OB_FAIL(SMART_CALL(inner_check_left_join_right_table_combinable(child_stmt,
                                                                                joined_table->right_table_, 
                                                                                outer_expr_relation_ids, 
                                                                                combinable)))) {
        LOG_WARN("failed to check left join`s right table combinable", K(ret));
      }
    } else {
      //do nothing
    }
  } else {
    //do nothing
  }
  return ret;
}

int ObTransformUtils::get_view_exprs(ObDMLStmt *parent_stmt,
                                    TableItem *view_table,
                                    ObIArray<ObRawExpr*> &from_exprs, 
                                    ObIArray<ObRawExpr*> &view_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(view_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else {
    ObSEArray<ObRawExpr *, 4> view_columns;
    ObSEArray<ObRawExpr *, 4> view_selects;
    if (OB_FAIL(parent_stmt->get_view_output(*view_table, 
                                             view_selects, 
                                             view_columns))) {
      LOG_WARN("failed to get view output exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(from_exprs, 
                                                            view_table->table_id_, 
                                                            view_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(view_columns, 
                                                       view_selects, 
                                                       view_exprs))) {
      LOG_WARN("failed to replace exprs", K(ret));
    }
  }
  return ret;
}

/**
 * @brief rebuild window function range between expr
 * for example:
 *  `select sum(a) over (partition by b order by 1 range between c following and d following) from t1`
 *   rebuild bound condition expr: 
 *     upper => (1 +/- c <= 1)
 *     lower => (1 <= 1 +/- d)
 *   if upper and lower are true, then use the partition as range, else return NULL
 * @param win_expr 
 * @param order_expr 
 * @return int 
 */
int ObTransformUtils::rebuild_win_compare_range_expr(ObRawExprFactory* expr_factory,
                                                     ObWinFunRawExpr &win_expr,
                                                     ObRawExpr* order_expr)
{
  int ret = OB_SUCCESS;
  int64_t upper_idx = (win_expr.upper_.is_preceding_ ^ 1) ? 0 : 1;
  int64_t lower_idx = (win_expr.lower_.is_preceding_ ^ 1) ? 0 : 1;
  ObOpRawExpr* upper_expr = NULL;
  ObOpRawExpr* lower_expr = NULL;
  if (OB_NOT_NULL(win_expr.upper_.exprs_[upper_idx]) 
      && OB_FAIL(ObRawExprUtils::build_less_than_expr(*expr_factory, 
                                                      win_expr.upper_.exprs_[upper_idx],
                                                      order_expr,
                                                      upper_expr))) {
    LOG_WARN("failed to build less than expr", K(ret));
  } else if (OB_NOT_NULL(win_expr.lower_.exprs_[lower_idx])
              && OB_FAIL(ObRawExprUtils::build_less_than_expr(*expr_factory,
                                                              order_expr,
                                                              win_expr.lower_.exprs_[lower_idx],
                                                              lower_expr))) {
    LOG_WARN("failed to build less than expr", K(ret));
  } else {
    win_expr.upper_.exprs_[upper_idx] = upper_expr;
    win_expr.lower_.exprs_[lower_idx] = lower_expr;
  }
  return ret;
}

int ObTransformUtils::check_expr_valid_for_stmt_merge(ObIArray<ObRawExpr*> &select_exprs,
                                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < select_exprs.count(); ++i) {
    ObRawExpr *expr = select_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->has_flag(IS_CONST) ||
               expr->has_generalized_column()) {
      // do nothing
    } else if (expr->has_flag(CNT_STATE_FUNC) ||
               expr->has_flag(CNT_DYNAMIC_USER_VARIABLE) ||
               expr->has_flag(CNT_ALIAS) ||
               expr->has_flag(CNT_VALUES) ||
               expr->has_flag(CNT_SEQ_EXPR) ||
               expr->has_flag(CNT_SYS_CONNECT_BY_PATH) ||
               expr->has_flag(CNT_RAND_FUNC) ||
               expr->has_flag(CNT_SO_UDF) ||
               expr->has_flag(CNT_PRIOR) ||
               expr->has_flag(CNT_VOLATILE_CONST) ||
               expr->has_flag(CNT_ASSIGN_EXPR)) {
      // the list is aligned with ObRawExpr::cnt_not_calculable_expr
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformUtils::check_table_contain_in_semi(const ObDMLStmt *stmt,
                                                  const TableItem *table,
                                                  bool &is_contain)
{
  int ret = OB_SUCCESS;
  ObRelIds table_rel_ids;
  ObSqlBitSet<> semi_left_rel_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(get_rel_ids_from_table(stmt, table, table_rel_ids))) {
    LOG_WARN("failed to get rel ids from table", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_info_size(); ++i) {
    if (OB_FAIL(get_left_rel_ids_from_semi_info(stmt, stmt->get_semi_infos().at(i),
                                                semi_left_rel_ids))) {
      LOG_WARN("failed to get left table ids from semi info", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_contain = table_rel_ids.overlap2(semi_left_rel_ids);
  }
  return ret;
}

int ObTransformUtils::extract_copier_exprs(ObRawExprCopier &copier,
                                           ObIArray<ObRawExpr *> &old_exprs,
                                           ObIArray<ObRawExpr *> &new_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<std::pair<ObRawExpr *, ObRawExpr *>, 8> from_to_exprs;
  if (OB_FAIL(copier.get_copied_exprs(from_to_exprs))) {
    LOG_WARN("failed to get copied exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < from_to_exprs.count(); ++i) {
      if (OB_FAIL(old_exprs.push_back(from_to_exprs.at(i).first))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(new_exprs.push_back(from_to_exprs.at(i).second))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::transform_bit_aggr_to_common_expr(ObDMLStmt &stmt,
                                                        ObRawExpr *aggr,
                                                        ObTransformerCtx *ctx,
                                                        ObRawExpr *&out_expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *const_uint64_max = NULL;
  ObConstRawExpr *const_zero = NULL;
  if (OB_ISNULL(aggr) || OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", KR(ret));
  } else if (OB_UNLIKELY(!aggr->is_aggr_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong expr type", K(ret));
  } else if (OB_UNLIKELY(T_FUN_SYS_BIT_AND != aggr->get_expr_type() &&
                         T_FUN_SYS_BIT_OR != aggr->get_expr_type() &&
                         T_FUN_SYS_BIT_XOR != aggr->get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("use wrong func type", KR(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_uint_expr(*(ctx->expr_factory_),
                                                           ObUInt64Type,
                                                           UINT_MAX_VAL[ObUInt64Type],
                                                           const_uint64_max))) {
    LOG_WARN("failed to build const uint expr", KR(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_uint_expr(*(ctx->expr_factory_), ObUInt64Type, 0, const_zero))) {
    LOG_WARN("failed to build const uint expr", KR(ret));
  } else if (OB_FAIL(ObTransformUtils::build_case_when_expr(stmt,
                                                            aggr->get_param_expr(0),
                                                            aggr->get_param_expr(0),
                                                            T_FUN_SYS_BIT_AND == aggr->get_expr_type() ? const_uint64_max : const_zero,
                                                            out_expr,
                                                            ctx))) {
    LOG_WARN("failed to build case when expr", KR(ret));
  }
  return ret;
}

int ObTransformUtils::is_scalar_expr(ObRawExpr* expr, bool &is_scalar)
{
  int ret = OB_SUCCESS;
  is_scalar = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (T_OP_ROW == expr->get_expr_type()) {
    is_scalar = false;
  } else if (expr->is_query_ref_expr()) {
    ObQueryRefRawExpr *query_ref = static_cast<ObQueryRefRawExpr*>(expr);
    is_scalar = query_ref->is_scalar();
  }
  return ret;
}

int ObTransformUtils::pack_stmt(ObTransformerCtx *ctx,
                                ObSelectStmt *parent_stmt,
                                ObSelectStmt **child_stmt_ptr)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *dummy_stmt = NULL;
  ObSelectStmt *child_stmt = NULL;
  TableItem *new_table_item = NULL;
  ObSEArray<ObRawExpr *, 8> column_exprs;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->stmt_factory_) ||
      OB_ISNULL(ctx->allocator_) || OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ctx), K(parent_stmt), K(ret));
  } else if (OB_FAIL(ctx->stmt_factory_->create_stmt<ObSelectStmt>(child_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(ctx->stmt_factory_->create_stmt<ObSelectStmt>(dummy_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(dummy_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(child_stmt->assign(*parent_stmt))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else if (OB_FAIL(parent_stmt->assign(*dummy_stmt))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else if (OB_FAIL(parent_stmt->ObStmt::assign(*child_stmt))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else if (OB_FAIL(parent_stmt->get_stmt_hint().assign(child_stmt->get_stmt_hint()))) {
    LOG_WARN("failed to assign stmt hint", K(ret));
  } else if (OB_FAIL(child_stmt->adjust_statement_id(ctx->allocator_,
                                                     ctx->src_qb_name_,
                                                     ctx->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx,
                                                          parent_stmt,
                                                          child_stmt,
                                                          new_table_item))) {
    LOG_WARN("failed to add new table item", K(ret));
  } else if (OB_ISNULL(new_table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(parent_stmt->add_from_item(new_table_item->table_id_, false))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FAIL(parent_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx,
                                                               *new_table_item,
                                                               parent_stmt,
                                                               column_exprs))) {
    LOG_WARN("failed to create column items", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx->allocator_,
                                                          column_exprs,
                                                          parent_stmt))) {
    LOG_WARN("failed to create select item", K(ret));
  } else {
    parent_stmt->set_select_into(child_stmt->get_select_into());
    child_stmt->set_select_into(NULL);
    if (NULL != child_stmt_ptr) {
      *child_stmt_ptr = child_stmt;
    }
  }
  return ret;
}

/*
 * create a view to pre aggregation and push down group by:
 * select c1, c2 + 1, sum(2*c3) from t1 where c1 > 3 group by grouping sets(c1, c2+1) having c1 is
 * not null order by c1, c2 + 1, sum(2*c3);
 * --->
 * select v1."t1.c1", v1."t1.c2+1", sum(v1.sum(2*t1.c1)) from
 *   (select c1, c2+1, sum(2*c3) from t1 where c1 > 3 group by c1, c2+1) v1
 *   group by grouping sets(v1."t1.c1", v1."t1.c2+1")
 *   having c1 is not null
 *   order by v1."t1.c1", v1."t1.c2+1", sum(v1.sum(2*t1.c1));
 */

int ObTransformUtils::create_view_with_pre_aggregate(ObSelectStmt *stmt,
                                                     ObSelectStmt *&view_stmt,
                                                     ObTransformerCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObStmtFactory *stmt_factory = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  TableItem *view_table_item = NULL;
  ObSEArray<ObRawExpr *, 4> scalar_query_refs;
  ObSEArray<ObRawExpr *, 4> select_list;
  ObSEArray<ObRawExpr *, 4> new_cols;
  ObSEArray<ObRawExpr *, 4> post_join_exprs;
  ObSEArray<ObRawExpr *, 4> non_scalar_query_refs;
  ObSEArray<ObRawExpr *, 4> groupby_exprs;
  ObSEArray<ObRawExpr *, 4> aggr_list;
  ObSEArray<ObRawExpr *, 16> shared_exprs;
  if (OB_ISNULL(ctx) || OB_ISNULL(stmt) ||
      OB_ISNULL(session_info = ctx->session_info_) ||
      OB_ISNULL(stmt_factory = ctx->stmt_factory_) ||
      OB_ISNULL(expr_factory = ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt_factory), K(expr_factory), K(stmt));
  } else if (stmt->is_set_stmt() || stmt->is_hierarchical_query()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not create spj stmt", K(ret), K(stmt->is_set_stmt()),
             K(stmt->is_hierarchical_query()));
  } else if (OB_FAIL(stmt_factory->create_stmt<ObSelectStmt>(view_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_FAIL(view_stmt->ObDMLStmt::assign(*stmt))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else if (OB_FAIL(ctx->add_src_hash_val(ObTransformerCtx::SRC_STR_CREATE_SIMPLE_VIEW))) {
    LOG_WARN("failed to add src hash val", K(ret));
  } else {
    // 1. set basic struct for stmt and view_stmt
    stmt->reset_table_items();
    stmt->get_joined_tables().reuse();
    stmt->get_semi_infos().reuse();
    stmt->get_column_items().reuse();
    stmt->clear_from_items();
    stmt->get_part_exprs().reset();
    stmt->get_check_constraint_items().reset();
    stmt->get_condition_exprs().reset();
    if (OB_FAIL(stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild tables hash", K(ret));
    }
    // consider following parts:
    // select: group-by, rollup, select subquery, window function, distinct, sequence,
    //         order by, limit, select into, having
    view_stmt->set_stmt_type(stmt::T_SELECT);
    view_stmt->get_order_items().reset();
    view_stmt->set_limit_offset(NULL, NULL);
    view_stmt->set_limit_percent_expr(NULL);
    view_stmt->set_fetch_with_ties(false);
    view_stmt->set_has_fetch(false);
    view_stmt->clear_sequence();
    view_stmt->set_select_into(NULL);
    view_stmt->get_pseudo_column_like_exprs().reset();
    view_stmt->get_group_exprs().reset();
    view_stmt->get_rollup_exprs().reset();
    view_stmt->get_grouping_sets_items().reset();
    view_stmt->get_rollup_items().reset();
    view_stmt->get_cube_items().reset();
    view_stmt->get_having_exprs().reset();
    view_stmt->get_aggr_items().reset();
    view_stmt->get_window_func_exprs().reset();
    view_stmt->assign_all();  // set is_distinct_ = false
  }

  // 2. let the view_stmt process some subqueries
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_post_join_exprs(stmt, post_join_exprs))) {
      LOG_WARN("failed to get additional push down exprs", K(ret));
    } else if (OB_FAIL(classify_scalar_query_ref(post_join_exprs,
                                                 scalar_query_refs,
                                                 non_scalar_query_refs))) {
      LOG_WARN("failed to classify scalar query ref", K(ret));
    } else { /* do nothing */ }
  }

  /*
   * 3. create select list (all use expr in origin expr)
   *    = rownum, groupby_exprs, aggr_items, scalar_query_refs.
   */
  if (OB_SUCC(ret)) {
    ObStmtExprGetter stmt_getter;
    stmt_getter.remove_all();
    stmt_getter.add_scope(SCOPE_GROUPBY);
    stmt_getter.set_recursive(false);
    view_stmt->get_select_items().reset();
    ObSEArray<DmlStmtScope, 2> scopes;
    scopes.push_back(SCOPE_WHERE);
    for (int64_t i = 0; i < stmt->get_aggr_items().count(); ++i) {
      if (OB_ISNULL(stmt->get_aggr_items().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected aggr is NULL", K(ret), K(i), K(stmt->get_aggr_items().at(i)));
      } else if (stmt->get_aggr_items().at(i)->get_expr_type() == T_FUN_GROUPING ||
                 stmt->get_aggr_items().at(i)->get_expr_type() == T_FUN_GROUPING_ID ||
                 stmt->get_aggr_items().at(i)->get_expr_type() == T_FUN_GROUP_ID) {
        // do nothing
      } else if (OB_FAIL(view_stmt->get_aggr_items().push_back(stmt->get_aggr_items().at(i)))) {
        LOG_WARN("failed to push back into view's aggr_items", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(stmt->get_relation_exprs(groupby_exprs, stmt_getter))) {
      LOG_WARN("failed to get_relation_exprs", K(ret));
    } else if (OB_FAIL(view_stmt->get_group_exprs().assign(groupby_exprs))) {
      LOG_WARN("failed to assign", K(ret));
    } else if (OB_FAIL(append(aggr_list, view_stmt->get_aggr_items()))) {
      LOG_WARN("failed to append aggregation exprs", K(ret));
    } else if (OB_FAIL(pushdown_pseudo_column_like_exprs(*stmt, true, select_list))) {
      LOG_WARN("failed to pushdown pseudo column like exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(select_list, groupby_exprs))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(append_array_no_dup(select_list, aggr_list))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(append_array_no_dup(select_list, scalar_query_refs))) {
      LOG_WARN("failed to append scalar query refs", K(ret));
    } else if (OB_FAIL(extract_shared_expr(stmt, view_stmt, shared_exprs, NULL, &scopes))) {
      LOG_WARN("failed to extract shared expr", K(ret));
    } else if (OB_FAIL(append_array_no_dup(select_list, shared_exprs))) {
      LOG_WARN("failed to append shared exprs", K(ret));
    } else if (OB_FAIL(create_select_item(*(ctx->allocator_), select_list, view_stmt))) {
      LOG_WARN("failed to create select items", K(ret));
    } else if (OB_FAIL(view_stmt->formalize_stmt_expr_reference())) {
      LOG_WARN("failed to formalize stmt expr reference", K(ret));
    } else if (OB_FAIL(view_stmt->get_stmt_hint().set_simple_view_hint())) {
      LOG_WARN("failed to set simple view hint", K(ret));
    } else if (OB_FAIL(view_stmt->adjust_statement_id(ctx->allocator_,
                                                      ctx->src_qb_name_,
                                                      ctx->src_hash_val_))) {
      LOG_WARN("failed to adjust statement id", K(ret));
    } else if (view_stmt->get_select_items().empty()) {
      if (OB_FAIL(create_dummy_select_item(*view_stmt, ctx))) {
        LOG_WARN("failed to create dummy select item", K(ret));
      } else if (OB_FAIL(select_list.push_back(view_stmt->get_select_item(0).expr_))) {
        LOG_WARN("failed to push back dummy select expr", K(ret));
      } else { /* do nothing */ }
    }
  }
  // 4. link stmt and view_stmt
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_subquery_exprs(),
                                             view_stmt->get_subquery_exprs()))) {
      LOG_WARN("failed to remove subqueries", K(ret));
    } else if (OB_FAIL(add_new_table_item(ctx, stmt, view_stmt, view_table_item))) {
      LOG_WARN("failed to add new table item", K(ret));
    } else if (OB_ISNULL(view_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(stmt->add_from_item(view_table_item->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(create_columns_for_view(ctx, *view_table_item, stmt, new_cols))) {
      LOG_WARN("failed to create columns for view", K(ret));
    } else if (view_stmt->has_for_update() && lib::is_oracle_mode()) {
      view_table_item->for_update_ = true;
    }
  }
  // 5. do stmt's replace and formalize
  if (OB_SUCC(ret)) {
    ctx->src_hash_val_.pop_back();
    ObRawExprCopier copier(*expr_factory);
    if (OB_FAIL(transform_aggregation_exprs(ctx, stmt, *view_table_item))) {
      LOG_WARN("failed to transform_aggregation_exprs", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(select_list, new_cols))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(stmt->copy_and_replace_stmt_expr(copier))) {
      LOG_WARN("failed to copy and replace stmt expr", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else if (OB_FAIL(view_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(adjust_pseudo_column_like_exprs(*view_stmt))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::transform_aggregation_expr(ObTransformerCtx *ctx,
                                                 ObSelectStmt *select_stmt,
                                                 TableItem &view_item,
                                                 ObAggFunRawExpr &aggr_expr,
                                                 ObRawExpr *&new_aggr_expr) {
  int ret = OB_SUCCESS;
  ObRawExpr *view_column = NULL;
  ObAggFunRawExpr *group_aggr = NULL;
  ObItemType aggr_type = aggr_expr.get_expr_type();
  ObItemType group_aggr_type = (aggr_type == T_FUN_COUNT ? T_FUN_COUNT_SUM : aggr_type);
  new_aggr_expr = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or expr_factory is NULL", K(ret), K(ctx));
  } else if (OB_UNLIKELY(aggr_type != T_FUN_MAX &&
                         aggr_type != T_FUN_MIN &&
                         aggr_type != T_FUN_SUM &&
                         aggr_type != T_FUN_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregation type is invalid", K(ret), K(aggr_type));
  } else if (OB_FAIL(get_view_column(ctx, *select_stmt, &view_item, false,
                                     &aggr_expr, view_column))) {
    LOG_WARN("failed to get aggregation column", K(ret));
  } else if (OB_ISNULL(view_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected view_column is NULL", K(ret), K(view_column));
  } else if (OB_FAIL(ctx->expr_factory_->create_raw_expr<ObAggFunRawExpr>(group_aggr_type, group_aggr))) {
    LOG_WARN("failed to create new aggregation expr", K(ret));
  } else if (OB_ISNULL(group_aggr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the copied aggregation expr is null", K(ret));
  } else {
    group_aggr->add_real_param_expr(view_column);
    new_aggr_expr = group_aggr;
  }
  return ret;
}

// tranform select_stmt's aggr_items
// 如果aggr是count(*), count(c1)--》主查询是column_ref
int ObTransformUtils::transform_aggregation_exprs(ObTransformerCtx *ctx,
                                                  ObSelectStmt *select_stmt,
                                                  TableItem &view_item) {
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> origin_aggr_exprs;
  ObSEArray<ObRawExpr *, 4> deduce_aggr_exprs;
  if (OB_ISNULL(ctx) || OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); ++i) {
      ObRawExpr *deduced_expr = NULL;
      if (OB_ISNULL(select_stmt->get_aggr_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (select_stmt->get_aggr_item(i)->get_expr_type() == T_FUN_GROUPING ||
                 select_stmt->get_aggr_item(i)->get_expr_type() == T_FUN_GROUPING_ID ||
                 select_stmt->get_aggr_item(i)->get_expr_type() == T_FUN_GROUP_ID) {
        /* do nothing */
      } else if (OB_FAIL(origin_aggr_exprs.push_back(select_stmt->get_aggr_item(i)))) {
        LOG_WARN("failed to push back origin aggregation expr", K(ret));
      } else if (OB_FAIL(transform_aggregation_expr(ctx, select_stmt, view_item,
                                                    *select_stmt->get_aggr_item(i),
                                                    deduced_expr))) {
        LOG_WARN("failed to transform aggregation expr", K(ret));
      } else if (OB_ISNULL(deduced_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("deduced_expr is null", K(ret));
      } else if (OB_FAIL(deduce_aggr_exprs.push_back(deduced_expr))) {
        LOG_WARN("failed to push back deduced aggregation expr", K(ret));
      } else { /* do nothing */ }
    }
    if (OB_SUCC(ret) && !origin_aggr_exprs.empty()) {
      if (OB_FAIL(select_stmt->replace_relation_exprs(origin_aggr_exprs, deduce_aggr_exprs))) {
        LOG_WARN("failed to replace inner stmt expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_exprs(origin_aggr_exprs,
                                                         deduce_aggr_exprs,
                                                         select_stmt->get_aggr_items()))) {
        LOG_WARN("failed to replace exprs", K(ret));
      } else { /* do nothing */ }
    }
  }
  return ret;
}

int ObTransformUtils::get_view_column(ObTransformerCtx *ctx,
                                      ObDMLStmt &stmt,
                                      TableItem *table_item,
                                      bool is_outer_join_table,
                                      ObRawExpr *aggr_expr,
                                      ObRawExpr *&aggr_column)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  uint64_t column_id = OB_INVALID_ID;
  aggr_column = NULL;
  if (OB_ISNULL(aggr_expr) ||
      OB_ISNULL(table_item) ||
      OB_ISNULL(select_stmt = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(aggr_expr), K(table_item));
  } else if (!table_item->is_generated_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is expected to be generated table", K(ret), K(table_item));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
      if (select_stmt->get_select_item(i).expr_ == aggr_expr) {
        column_id = OB_APP_MIN_COLUMN_ID + i;
        break;
      }
    }
    if (column_id != OB_INVALID_ID) {
      ObColumnRefRawExpr *col_expr = stmt.get_column_expr_by_id(table_item->table_id_, column_id);
      if (OB_ISNULL(col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column item", K(table_item), K(column_id));
      } else if (aggr_expr->get_expr_type() != T_FUN_COUNT || !is_outer_join_table) {
        aggr_column = col_expr;
      } else if (OB_FAIL(wrap_case_when_for_count(ctx, &stmt, col_expr, aggr_column))) {
        LOG_WARN("failed to convert outer join count", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::convert_aggr_expr(ObTransformerCtx *ctx,
                                        ObDMLStmt *stmt,
                                        ObAggFunRawExpr *aggr_expr,
                                        ObRawExpr *&output_expr)
{
  int ret = OB_SUCCESS;
  bool is_not_null = true;
  ObConstRawExpr *value_one = NULL;
  ObConstRawExpr *value_zero = NULL;
  ObSEArray<ObRawExpr *, 4> constraints;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_) || OB_ISNULL(stmt) || OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(ctx), K(stmt), K(aggr_expr));
  } else if (aggr_expr->get_expr_type() == T_FUN_MAX ||
             aggr_expr->get_expr_type() == T_FUN_MIN ||
             aggr_expr->get_expr_type() == T_FUN_SUM) {
    output_expr = aggr_expr->get_param_expr(0);
    if (OB_FAIL(add_cast_for_replace_if_need(*ctx->expr_factory_,
                                             aggr_expr,
                                             output_expr,
                                             ctx->session_info_))) {
      LOG_WARN("failed to add cast expr", K(ret));
    }
  } else if (aggr_expr->get_expr_type() != T_FUN_COUNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid aggregation type", K(ret), K(aggr_expr->get_expr_type()));
  } else if (aggr_expr->get_param_count() == 0) { // count(*)
    // Note 1: actually output expr should be "expr 1L"
    // we do not create the dummy const expr, instead we let
    // the caller to handle this case, which will ignore the expr
    // in deduce the aggreagtion result
    output_expr = aggr_expr;
  } else if (OB_FAIL(is_expr_not_null(ctx, stmt,
                                      aggr_expr->get_real_param_exprs().at(0),
                                      NULLABLE_SCOPE::NS_WHERE,
                                      is_not_null,
                                      &constraints))) { // count(c1)
    LOG_WARN("failed to check expr not null", K(ret));
  } else if (is_not_null) {
    // See Note 1
    if (OB_FAIL(add_param_not_null_constraint(*ctx, constraints))) {
      LOG_WARN("failed to add param not null constraint", K(ret));
    } else {
      output_expr = aggr_expr;
    }
  } else if (OB_FAIL(build_const_expr_for_count(*ctx->expr_factory_, 1L, value_one))) {
    LOG_WARN("failed to create const int expr", K(ret));
  } else if (OB_FAIL(build_const_expr_for_count(*ctx->expr_factory_, 0L, value_zero))) {
    LOG_WARN("failed to create const int expr", K(ret));
  } else if (OB_FAIL(build_case_when_expr(*stmt,
                                          aggr_expr->get_real_param_exprs().at(0),
                                          value_one,
                                          value_zero,
                                          output_expr,
                                          ctx))) {
    LOG_WARN("failed to build case when", K(ret));
  }
  return ret;
}

int ObTransformUtils::wrap_case_when_for_count(ObTransformerCtx *ctx,
                                               ObDMLStmt *stmt,
                                               ObColumnRefRawExpr *view_count,
                                               ObRawExpr *&output,
                                               bool is_count_star /*= false*/)
{
  int ret = OB_SUCCESS;
  const int64_t const_int_value = (is_count_star ? 1 : 0);
  ObRawExpr *case_when = NULL;
  ObConstRawExpr *int_value = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_) || OB_ISNULL(stmt) || OB_ISNULL(view_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(ctx), K(stmt), K(view_count));
  } else if (OB_FAIL(build_const_expr_for_count(*ctx->expr_factory_, const_int_value, int_value))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(build_case_when_expr(*stmt, view_count, view_count, int_value, case_when, ctx))) {
    LOG_WARN("failed to build case when expr", K(ret));
  } else {
    output = case_when;
  }
  return ret;
}

int ObTransformUtils::create_udt_hidden_columns(ObTransformerCtx *ctx,
                                                ObDMLStmt *stmt,
                                                const ObColumnRefRawExpr &udt_expr,
                                                ObColumnRefRawExpr *&col_expr,
                                                bool &need_transform)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const TableItem *table = NULL;
  col_expr = NULL;
  const ColumnItem *column_item = NULL;
  ObSEArray<ObColumnSchemaV2 *, 1> hidden_cols;
  need_transform = false;
  bool from_base = false;
  bool from_xml = false;
  bool view_table_do_transform = (stmt->get_stmt_type() == stmt::T_INSERT ||
                                  stmt->get_stmt_type() == stmt::T_UPDATE ||
                                  stmt->get_stmt_type() == stmt::T_MERGE);
  if (OB_ISNULL(stmt)
      || OB_ISNULL(ctx)
      || OB_ISNULL(ctx->session_info_)
      || OB_ISNULL(ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ctx), K(stmt), K(ret));
  } else if (OB_ISNULL(table = stmt->get_table_item_by_id(udt_expr.get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get table item", K(udt_expr.get_table_id()), K(ret));
  } else if (table->is_view_table_ && (!view_table_do_transform)) {
    // do nothing.
    LOG_INFO("udt columns in views does not need transfrom", K(udt_expr.get_table_id()), K(table));
  } else if (table->is_view_table_ && table->view_base_item_ == NULL) {
    // do nothing
  } else if (OB_FAIL(ctx->schema_checker_->get_table_schema(ctx->session_info_->get_effective_tenant_id(),
                                                            table->ref_id_, table_schema))) {
    if ((table->is_generated_table() || table->is_temp_table()) && OB_NOT_NULL(table->ref_query_)) {
      // situation for select  a from (select xmltype('<note3/>') a from dual )
      LOG_INFO("table schema not found for tmp view does not need transfrom", K(ret), KPC(table));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get table schema", K(ret));
    }
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table should not be null", K(table->ref_id_));
  } else {
    if (table->is_view_table_ && view_table_do_transform) {
      // for view table, we should check col group in base table
      const ObTableSchema *base_table_schema = NULL;
      from_base = true;
      if (table->view_base_item_ == NULL) {
        // do nothing or return error
      } else {
        ObSelectStmt *real_stmt = NULL;
        if (OB_ISNULL(real_stmt = table->ref_query_->get_real_stmt())) {
          // case : view definition is set_op
          // Bug :
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("real stmt is NULL", K(ret));
        } else {
          SelectItem &t_col = real_stmt->get_select_item((udt_expr.get_column_id() - OB_APP_MIN_COLUMN_ID));

          if (t_col.expr_->get_expr_type() == T_FUN_SYS_MAKEXML) { // xmltype special case
            if (t_col.expr_->get_param_count() != 2) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid xml generated table column", K(ret), K(t_col.expr_));
            } else if (!t_col.expr_->get_param_expr(1)->is_column_ref_expr()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid xml generated table column", K(ret), K(t_col.expr_->get_param_expr(1)->is_column_ref_expr()));
            } else {
              from_xml = true;
              col_expr = static_cast<ObColumnRefRawExpr*>(t_col.expr_->get_param_expr(1));
            }
          } else if (t_col.expr_->get_expr_type() == T_REF_COLUMN) {
            int64_t col_id = dynamic_cast<ObColumnRefRawExpr*>(t_col.expr_)->get_column_id();
            if (OB_FAIL(ctx->schema_checker_->get_table_schema(ctx->session_info_->get_effective_tenant_id(),
                                                              table->view_base_item_->ref_id_, base_table_schema))) {
              LOG_WARN("failed to get table schema", K(ret));
            } else if (OB_ISNULL(base_table_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("table should not be null", K(table->view_base_item_->ref_id_));
            } else if (OB_FAIL(base_table_schema->get_column_schema_in_same_col_group(col_id,
                                                                            udt_expr.get_udt_set_id(),
                                                                            hidden_cols))) {
              LOG_WARN("failed to get column schema", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid view column type", K(ret), K(t_col.expr_));
          }
        }
      }
    } else if (OB_FAIL(table_schema->get_column_schema_in_same_col_group(udt_expr.get_column_id(),
                                                                       udt_expr.get_udt_set_id(),
                                                                       hidden_cols))) {
      LOG_WARN("failed to get column schema", K(ret));
    }
    if(OB_FAIL(ret)) {
    } else if (!from_xml && hidden_cols.count() != 1) {
      // xmltype only 1 hidden column currently
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hidden cols count is not expected", K(table->ref_id_), K(hidden_cols.count()));
    } else if (from_base) {
      if (from_xml) {
        column_item = stmt->get_column_item_by_base_id(table->table_id_, col_expr->get_column_id());
      } else {
        column_item = stmt->get_column_item_by_base_id(table->table_id_, hidden_cols.at(0)->get_column_id());
      }
    } else {
      column_item = stmt->get_column_item(table->table_id_, hidden_cols.at(0)->get_column_id());
    }

    if(OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(column_item)) {
      if (OB_ISNULL(column_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item expr is null", K(ret));
      } else {
        col_expr = column_item->expr_;
        need_transform = true;
      }
    } else if (!from_xml && OB_FAIL(ObRawExprUtils::build_column_expr(*ctx->expr_factory_, *hidden_cols.at(0), col_expr))) {
      LOG_WARN("build column expr failed", K(ret));
    } else if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create raw expr for dummy output", K(ret));
    } else {
      if (!from_xml) {
        col_expr->set_table_id(table->table_id_);
        col_expr->set_explicited_reference();
        col_expr->set_column_attr(udt_expr.get_table_name(), col_expr->get_column_name());
      }
      ColumnItem column_item;
      column_item.expr_ = col_expr;
      column_item.table_id_ = col_expr->get_table_id();
      column_item.column_id_ = col_expr->get_column_id();
      column_item.column_name_ = col_expr->get_column_name();
      if (OB_FAIL(stmt->add_column_item(column_item))) {
        LOG_WARN("add column item to stmt failed", K(ret));
      } else {
        need_transform = true;
      }
    }
  }
  return ret;
}

int ObTransformUtils::check_is_index_part_key(ObTransformerCtx &ctx,
                                             ObDMLStmt &stmt,
                                             ObRawExpr *check_expr,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObSQLSessionInfo *session_info = ctx.session_info_;
  if (OB_ISNULL(check_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index", K(ret), K(check_expr));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(check_expr, check_expr))) {
    LOG_WARN("failed to get expr without lossless cast", K(ret));
  } else if (!check_expr->is_column_ref_expr()) {
    // do nothing
  } else {
    ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr *>(check_expr);
    const share::schema::ObColumnSchemaV2 *column_schema = NULL;
    TableItem *table = stmt.get_table_item_by_id(col->get_table_id());
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null", K(ret), K(*col));
    } else if (!table->is_basic_table()) {

    } else if (OB_FAIL(ctx.schema_checker_->get_column_schema(session_info->get_effective_tenant_id(),
                                                              table->ref_id_,
                                                              col->get_column_id(),
                                                              column_schema,
                                                              true))) {
      LOG_WARN("failed to get column schema", K(ret), K(table->ref_id_), K(col->get_column_id()), K(col->get_table_id()), K(table), K(col), K(lbt()));
    } else if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(ret));
    } else if (column_schema->is_rowkey_column()) {
      is_valid = true;
    } else if (OB_FAIL(ctx.schema_checker_->check_column_has_index(column_schema->get_tenant_id(),
                                                                    table->ref_id_,
                                                                    col->get_column_id(),
                                                                    is_valid))) {
      LOG_WARN("failed to check column is a key", K(ret));
    } else if (is_valid) {
      // do nothing
    } else if (ctx.schema_checker_->check_if_partition_key(session_info->get_effective_tenant_id(),
                          table->ref_id_, col->get_column_id(), is_valid)) {
      LOG_WARN("failed to check if partition key", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::get_stmt_map_after_copy(ObDMLStmt *origin_stmt,
                                              ObDMLStmt *new_stmt,
                                              hash::ObHashMap<uint64_t, ObDMLStmt *> &stmt_map)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt *, 4> orgi_child_stmts;
  ObSEArray<ObSelectStmt *, 4> new_child_stmts;
  uint64_t key = reinterpret_cast<uint64_t>(origin_stmt);
  if (OB_ISNULL(origin_stmt) || OB_ISNULL(new_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_UNLIKELY(new_stmt->get_stmt_type() != origin_stmt->get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret));
  } else if (OB_FAIL(origin_stmt->get_child_stmts(orgi_child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else if (OB_FAIL(new_stmt->get_child_stmts(new_child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else if (OB_UNLIKELY(orgi_child_stmts.count() != new_child_stmts.count()) ||
             OB_UNLIKELY(origin_stmt->get_table_size() != new_stmt->get_table_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt count does not match", K(ret));
  } else if (OB_FAIL(stmt_map.set_refactored(key, new_stmt))) {
    LOG_WARN("failed to add parent child relation", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < origin_stmt->get_table_size(); ++i) {
    TableItem *origin_table = origin_stmt->get_table_item(i);
    TableItem *new_table = new_stmt->get_table_item(i);
    ObDMLStmt *dummy = NULL;
    key = reinterpret_cast<uint64_t>(origin_table->ref_query_);
    if (OB_ISNULL(origin_table) || OB_ISNULL(new_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!origin_table->is_temp_table()) {
      //do nothing
    } else if (!new_table->is_temp_table()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table type", K(ret), KPC(origin_table), KPC(new_table));
    } else if (OB_FAIL(stmt_map.get_refactored(key, dummy))) {
      if (ret == OB_HASH_NOT_EXIST) {
        ret = get_stmt_map_after_copy(origin_table->ref_query_,
                                      new_table->ref_query_,
                                      stmt_map);
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < orgi_child_stmts.count(); i ++) {
    if (OB_FAIL(SMART_CALL(get_stmt_map_after_copy(orgi_child_stmts.at(i),
                                                   new_child_stmts.at(i),
                                                   stmt_map)))) {
      LOG_WARN("failed to get child stmt map", K(ret));
    }
  }
  return ret;
}

int ObTransformUtils::expand_temp_table(ObTransformerCtx *ctx, ObDMLStmt::TempTableInfo& table_info)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *temp_table_query = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->stmt_factory_) ||
      OB_UNLIKELY(table_info.table_items_.count() != table_info.upper_stmts_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ctx", K(ret));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < table_info.table_items_.count(); ++j) {
    TableItem *table = table_info.table_items_.at(j);
    ObDMLStmt *upper_stmt = table_info.upper_stmts_.at(j);
    if (OB_ISNULL(table) || OB_ISNULL(upper_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_temp_table()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect temp table item", KPC(table), KPC(upper_stmt), K(ret));
    } else {
      ObSelectStmt *child_stmt = NULL;
      table->type_ = TableItem::GENERATED_TABLE;
      if (0 == j) {
        temp_table_query = table->ref_query_;
        if (OB_ISNULL(temp_table_query)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null stmt", K(ret));
        } else if (OB_FAIL(temp_table_query->formalize_stmt(ctx->session_info_))) {
          LOG_WARN("failed to formalize stmt", K(ret));
        } else if (OB_FAIL(temp_table_query->formalize_stmt_expr_reference())) {
          LOG_WARN("failed to formalize stmt reference", K(ret));
        } else if (OB_FAIL(upper_stmt->formalize_stmt_expr_reference())) {
          LOG_WARN("failed to formalize stmt reference", K(ret));
        }
      } else if (OB_FAIL(ctx->stmt_factory_->create_stmt<ObSelectStmt>(child_stmt))) {
        LOG_WARN("failed to create stmt", K(ret));
      } else if (OB_ISNULL(child_stmt) || OB_ISNULL(temp_table_query)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(ret));
      } else if (OB_FAIL(child_stmt->deep_copy(*ctx->stmt_factory_,
                                                *ctx->expr_factory_,
                                                *temp_table_query))) {
        LOG_WARN("failed to deep copy stmt", K(ret));
      } else if (OB_FAIL(child_stmt->formalize_stmt(ctx->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
      } else if (OB_FAIL(child_stmt->formalize_stmt_expr_reference())) {
        LOG_WARN("failed to formalize stmt reference", K(ret));
      } else if (OB_FAIL(child_stmt->recursive_adjust_statement_id(ctx->allocator_,
                                                                   ctx->src_hash_val_,
                                                                   j))) {
        LOG_WARN("failed to recursive adjust statement id", K(ret));
      } else if (OB_FAIL(child_stmt->update_stmt_table_id(*temp_table_query))) {
        LOG_WARN("failed to update table id", K(ret));
      } else if (OB_FAIL(upper_stmt->formalize_stmt_expr_reference())) {
        LOG_WARN("failed to formalize stmt reference", K(ret));
      } else {
        table->ref_query_ = child_stmt;
      }
    }
  }
  return ret;
}

ObSelectStmtPointer::ObSelectStmtPointer() : stmt_group_() {
}

ObSelectStmtPointer::~ObSelectStmtPointer() {
}

int ObSelectStmtPointer::get(ObSelectStmt *&stmt) const
{
  int ret = OB_SUCCESS;
  if (stmt_group_.count() <= 0
      || OB_ISNULL(stmt_group_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt pointer is not set", K(ret));
  } else {
    stmt = *(stmt_group_.at(0));
  }
  return ret;
}

int ObSelectStmtPointer::set(ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt_group_.count(); ++i) {
    if (OB_ISNULL(stmt_group_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt pointer is null", K(ret));
    } else if (*stmt_group_.at(i) == stmt) {
      // not changed
      break;
    } else {
      *stmt_group_.at(i) = stmt;
    }
  }
  return ret;
}

int ObSelectStmtPointer::add_ref(ObSelectStmt **stmt)
{
  return stmt_group_.push_back(stmt);
}

int ObTransformUtils::check_stmt_contain_oversize_set_stmt(ObDMLStmt *stmt, bool &is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->is_select_stmt()) {
    int64_t size = 0;
    if (OB_FAIL(static_cast<ObSelectStmt *>(stmt)->get_set_stmt_size(size))) {
      LOG_WARN("get set stmt size failed", K(ret));
    } else if (size > ObTransformUtils::MAX_SET_STMT_SIZE_OF_COSTED_BASED_RELUES) {
      is_contain = true;
    }
  }
  if (OB_SUCC(ret) && !is_contain) {
    ObSEArray<ObSelectStmt*, 4> child_stmts;
    if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_contain && i < child_stmts.count(); i++) {
      if (OB_FAIL(SMART_CALL(check_stmt_contain_oversize_set_stmt(child_stmts.at(i), is_contain)))) {
        LOG_WARN("get_stmt_trans_info failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUtils::convert_preds_vector_to_scalar(ObTransformerCtx &ctx,
                                                     ObRawExpr *expr,
                                                     ObIArray<ObRawExpr*> &exprs,
                                                     bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool need_push = true;
  ObRawExprFactory *factory = NULL;
  ObSQLSessionInfo *session = NULL;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr) || OB_ISNULL(factory = ctx.expr_factory_) ||
      OB_ISNULL(session = ctx.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL param", K(ret), KP(expr), KP(factory), KP(session));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr->get_expr_type() == T_OP_EQ || expr->get_expr_type() == T_OP_NSEQ) {
    // 条件1: 是等号
    ObOpRawExpr *op_expr = reinterpret_cast<ObOpRawExpr*>(expr);
    ObRawExpr *param_expr1 = expr->get_param_expr(0);
    ObRawExpr *param_expr2 = expr->get_param_expr(1);
    if (OB_UNLIKELY(2 != op_expr->get_param_count()) ||
        OB_ISNULL(param_expr1) || OB_ISNULL(param_expr2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is wrong", K(op_expr->get_param_count()),
               K(param_expr1), K(param_expr2));
    } else if (T_OP_ROW == param_expr1->get_expr_type() &&
               T_OP_ROW == param_expr2->get_expr_type()) {
      // 条件2: 两边都是 ROW
      need_push = false;
      trans_happened = true;
      if (OB_UNLIKELY(!is_oracle_mode() && param_expr1->get_param_count() != param_expr2->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param number not equal", K(ret), K(param_expr1->get_param_count()),
                 K(param_expr2->get_param_count()));
      } else if (OB_UNLIKELY(is_oracle_mode()
                             && 1 > param_expr2->get_param_count()
                             && param_expr1->get_param_count() != param_expr2->get_param_expr(0)->get_param_count()
                             && param_expr1->get_param_count() != param_expr2->get_param_count())) {
        ret = OB_ERR_INVALID_COLUMN_NUM;
        LOG_WARN("invalid relational operator on oracle mode", K(ret),
                 K(param_expr2->get_param_count()));
      } else {
        if (is_oracle_mode() && T_OP_ROW == param_expr2->get_param_expr(0)->get_expr_type()) {
          param_expr2 = param_expr2->get_param_expr(0);
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < param_expr1->get_param_count(); i++) {
          ObOpRawExpr *new_op_expr = NULL;
          if (OB_FAIL(factory->create_raw_expr(expr->get_expr_type(), new_op_expr))) {
            LOG_WARN("failed to create raw expr", K(ret));
          } else if (OB_ISNULL(new_op_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL new op expr", K(ret));
          } else if (OB_FAIL(new_op_expr->set_param_exprs(
                                param_expr1->get_param_expr(i), param_expr2->get_param_expr(i)))) {
            LOG_WARN("failed to set param expr", K(ret));
          } else if (OB_FAIL(new_op_expr->formalize(session))) {
            LOG_WARN("failed to formalize expr", K(ret));
          } else if (OB_FAIL(new_op_expr->pull_relation_id())) {
            LOG_WARN("failed to pull relation id and levels", K(ret));
          } else if (OB_FAIL(SMART_CALL(convert_preds_vector_to_scalar(ctx,
                                                        reinterpret_cast<ObRawExpr*>(new_op_expr),
                                                        exprs,
                                                        trans_happened)))) {
            /// 对拆分后 expr 继续递归
            LOG_WARN("failed to call inner convert recursive", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_push && OB_FAIL(exprs.push_back(expr))) {
    /// 不满足上述两个条件, 将该 expr 添加至输出.
    LOG_WARN("failed to push back expr", K(ret));
  }
  return ret;
}

// check whether can convert f(A) to f(B) for any B that satisfied A = B
// in mysql mode, the collation type of all params is utf8mb4_general_ci
// we have A := 'a', B1 := 'a ', B2 := 'A', then they satisfied A = B1 and A = B2
// for F1(A) : concat(A, 'b'), we can not convert F1(A) to F(B1) or F(B2)
int ObTransformUtils::check_can_replace(ObRawExpr *expr,
                                        ObIArray<ObRawExpr *> &parent_exprs,
                                        bool used_in_compare,
                                        bool &can_replace)
{
  int ret = OB_SUCCESS;
  can_replace = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (!ob_is_string_or_lob_type(expr->get_result_type().get_type())) {
    can_replace = true;
  } else if (lib::is_oracle_mode() && !expr->get_result_type().is_fixed_len_char_type()) {
    // oracle compare varchar in binary and no pad
    can_replace = true;
  } else {
    bool found_convert = false;
    for (int i = parent_exprs.count() - 1; OB_SUCC(ret) && !found_convert && i >= 0; --i) {
      ObRawExpr *cur_expr = parent_exprs.at(i);
      bool is_bypass = false;
      if (OB_FAIL(check_is_bypass_string_expr(cur_expr, expr, is_bypass))) {
        LOG_WARN("failed to check is bypass string expr", K(ret));
      } else if (is_bypass) {
        // do nothing
      } else if (OB_FAIL(check_convert_string_safely(cur_expr, expr, can_replace))) {
        LOG_WARN("failed to check is convert string expr", K(ret));
      } else {
        found_convert = true;
      }
    }
    if (OB_SUCC(ret)) {
      can_replace = can_replace || (!found_convert && used_in_compare);
    }
  }
  return ret;
}

// check the expr bypass the string input to output, the expr must satisfy following rules:
// 1. the param and result are both string type
// 2. the content of input string is copied to result without modifying
int ObTransformUtils::check_is_bypass_string_expr(const ObRawExpr *expr,
                                                  const ObRawExpr *src_expr,
                                                  bool &is_bypass)
{
  int ret = OB_SUCCESS;
  is_bypass = false;
  if (OB_ISNULL(expr) || OB_ISNULL(src_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else {
    const ObExprOperatorType op_type = expr->get_expr_type();
    if (T_FUN_SYS_CAST == op_type || T_FUN_SYS_CONVERT == op_type) {
      const ObExprResType &src_type = src_expr->get_result_type();
      const ObExprResType &dst_type = expr->get_result_type();
      is_bypass = src_type.get_type() == dst_type.get_type() &&
                  src_type.get_collation_type() == dst_type.get_collation_type();
    } else {
      // ATTENTION: this does not work for the uca900 collation in the future
      is_bypass = T_FUN_SYS_SUBSTR == op_type;
    }
  }
  return ret;
}

// check if the expr result is affected by the collation of param when the expr convert the input
// string to another, the expr must satisfy one of following rules:
// 1. the param is string type and result is not string type
// 2. the content of input string is not just copied to result, such as charset convert, upper
// to simplify the implementation, this function only check functions in mysql mode
int ObTransformUtils::check_convert_string_safely(const ObRawExpr *expr,
                                                  const ObRawExpr *src_expr,
                                                  bool &is_safe)
{
  int ret = OB_SUCCESS;
  is_safe = false;
  if (OB_ISNULL(expr) || OB_ISNULL(src_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else {
    const ObExprOperatorType op_type = expr->get_expr_type();
    if (IS_COMPARISON_OP(op_type) || T_OP_LIKE == op_type) {
      is_safe = true;
    } else if (T_FUN_SYS_CAST == op_type || T_FUN_SYS_CONVERT == op_type) {
      if (!ob_is_string_or_lob_type(expr->get_result_type().get_type())) {
        is_safe = true;
      }
    }
  }
  return ret;
}

// insert into t1 values('a'); insert into t2 values('a ');
// Q1: select * from (select * from t1 intersect select * from t2) v where c1 = 'a';
// Q2: select * from (select * from t1 intersect select * from t2) v where concat(c1,'a') = 'aa';
// only predicate in Q1 can be pushdown into set stmt
// TODO: sean.yyj, concat(c1,'a') = 'aa' can be pushdown into UNION after solved collation level bug
int ObTransformUtils::check_pushdown_into_set_valid(ObRawExpr *expr,
                                                    const ObIArray<ObRawExpr *> &set_op_exprs,
                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> parent_exprs;
  if (OB_FAIL(recursive_check_pushdown_into_set_valid(expr,
                                                      set_op_exprs,
                                                      parent_exprs,
                                                      is_valid))) {
    LOG_WARN("failed to check pullup validity", K(ret));
  }
  return ret;
}

int ObTransformUtils::recursive_check_pushdown_into_set_valid(
    ObRawExpr *expr,
    const ObIArray<ObRawExpr *> &set_op_exprs,
    ObIArray<ObRawExpr *> &parent_exprs,
    bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (!expr->has_flag(CNT_SET_OP)) {
    // do nothing
  } else if (ObOptimizerUtil::find_item(set_op_exprs, expr)) {
    if (OB_FAIL(ObTransformUtils::check_can_replace(expr, parent_exprs, false, is_valid))) {
      LOG_WARN("failed to check can replace expr", K(ret));
    }
  } else if (OB_UNLIKELY(expr->is_set_op_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set op expr", KPC(expr), K(set_op_exprs), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(parent_exprs.push_back(expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
      if (OB_FAIL(SMART_CALL(recursive_check_pushdown_into_set_valid(expr->get_param_expr(i),
                                                                     set_op_exprs,
                                                                     parent_exprs,
                                                                     is_valid)))) {
        LOG_WARN("failed to check pushdown validity", K(ret));
      }
      if (OB_SUCC(ret)) {
        parent_exprs.pop_back();
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
