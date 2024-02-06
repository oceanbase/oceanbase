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

#define USING_LOG_PREFIX SQL_RESV
#include "common/ob_smart_call.h"
#include "share/ob_define.h"
#include "sql/resolver/dml/ob_del_upd_resolver.h"
#include "sql/resolver/dml/ob_default_value_utils.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/resolver/dml/ob_insert_all_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/ob_stmt_type.h"
#include "pl/ob_pl_resolver.h"
#include "sql/parser/parse_malloc.h"
#include "sql/resolver/dml/ob_merge_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace pl;
namespace sql
{

int ObTableAssignment::expand_expr(ObRawExprFactory &expr_factory,
                                   const ObIArray<ObAssignment> &assigns,
                                   ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExprCopier copier(expr_factory);
  for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); ++i) {
    if (OB_FAIL(copier.add_replaced_expr(assigns.at(i).column_expr_,
                                         assigns.at(i).expr_))) {
      LOG_WARN("failed to add replace expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObRawExpr *new_expr = NULL;
    if (OB_FAIL(copier.copy_on_replace(expr, new_expr))) {
      LOG_WARN("failed to do copy on replace", K(ret));
    } else {
      expr = new_expr;
    }
  }
  return ret;
}

int ObTableAssignment::replace_assigment_expr(const common::ObIArray<ObAssignment> &assigns, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(!expr->has_flag(IS_COLUMN))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expr", K(expr));
  } else {
    //在已经加入到assigns中的表达式中查找，如果遇到已经赋值的column，则进行表达式替换；
    //eg:update test set c1 = c1+c2, c2=c1+3
    //经过展开后，会将c1替换成c1+c2, 最后c2的赋值语句变成c2=c1+c2+3
    for (int64_t i = 0; i < assigns.count(); i++) {
      if (assigns.at(i).column_expr_ == expr) {
        expr = assigns.at(i).expr_;
        break;
      }
    }
  }
  return ret;
}

ObDelUpdResolver::ObDelUpdResolver(ObResolverParams &params)
  : ObDMLResolver(params),
    insert_column_ids_(),
    is_column_specify_(false),
    is_oracle_tmp_table_(false),
    oracle_tmp_table_type_(0)
{
  // TODO Auto-generated constructor stub
}

ObDelUpdResolver::~ObDelUpdResolver()
{
  // TODO Auto-generated destructor stub
}

int ObDelUpdResolver::resolve_assignments(const ParseNode &parse_node,
                                          ObIArray<ObTableAssignment> &table_assigns,
                                          ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt = get_stmt();
  ObSEArray<ObColumnRefRawExpr *, 8> column_list;
  ObSEArray<ObRawExpr*, 8> value_list;
  if (OB_ISNULL(stmt) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resolver invalid status", K(stmt), KP_(params_.expr_factory));
  } else if (OB_FAIL(resolve_column_and_values(parse_node, column_list, value_list))) {
    LOG_WARN("failed to resovle column and values", K(ret));
  } else {
  // 处理配对后的value
    ObAssignment assignment;
    ColumnItem *column = NULL;
    TableItem *table = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_list.count(); i++) {
      // 处理column
      ObColumnRefRawExpr *ref_expr = column_list.at(i);
      if (OB_ISNULL(column = stmt->get_column_item_by_id(
                        ref_expr->get_table_id(), ref_expr->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column item failed", K(*ref_expr), K(ret));
      } else if (OB_ISNULL(table = stmt->get_table_item_by_id(
                               ref_expr->get_table_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get table item failed", K(*ref_expr), K(ret));
      } else {
        // Statement `update (select * from t1) t set t.c1 = 1` is legal in oralce, illegal in mysql.
        const bool is_updatable_generated_table = (table->is_generated_table() || table->is_temp_table())
            && (!is_mysql_mode() || table->is_view_table_);
        if (!table->is_basic_table() && !table->is_link_table() && !is_updatable_generated_table) {
          ret = OB_ERR_NON_UPDATABLE_TABLE;
          const ObString &table_name = table->alias_name_;
          ObString scope_name = "UPDATE";
          LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
                         table_name.length(), table_name.ptr(),
                         scope_name.length(), scope_name.ptr());
        }
        if (OB_SUCC(ret) && (table->is_generated_table() || table->is_temp_table())
            && !get_stmt()->has_instead_of_trigger()) {
          if (NULL == table->view_base_item_) {
            if (OB_FAIL(set_base_table_for_updatable_view(*table, *ref_expr))) {
              LOG_WARN("find base table for update view failed", K(ret));
            } else if (OB_ISNULL(table->view_base_item_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("view base item is NULL", K(ret));
            }
          } else {
            if (OB_FAIL(check_same_base_table(*table, *ref_expr))) {
              LOG_WARN("check modified columns is same base table failed",
                  K(ret), K(*table), K(*ref_expr));
            }
          }
        }
      }
      if (OB_SUCC(ret) && T_INSERT_SCOPE == scope) {
        if (OB_FAIL(mock_values_column_ref(ref_expr))) {
          LOG_WARN("fail to add value desc", K(ret));
        }
      }
      // 处理表达式
      if (OB_SUCC(ret)) {
        assignment.column_expr_ = column->expr_;
        ObRawExpr *expr = value_list.at(i);
        bool is_generated_column = true;
        SQL_RESV_LOG(DEBUG, "is standard assignment", K(is_mysql_mode()));
        // 这里Oracle和Mysql的逻辑不一样
        // Mysql仅允许将生成列update为默认值
        // Oracle不允许update生成列，因此分开进行处理
        if (OB_FAIL(check_basic_column_generated(ref_expr, stmt, is_generated_column))) {
          LOG_WARN("check basic column generated failed", K(ret));
        } else {
          if (is_mysql_mode()) {
            if (T_DEFAULT == expr->get_expr_type()) {
              ObDefaultValueUtils utils(stmt, &params_, this);
              if (OB_FAIL(utils.resolve_default_expr(*column, expr, scope))) {
                LOG_WARN("failed to resolve default expr", K(*column), K(ret));
              }
            } else if (is_generated_column) {
              ret = OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN;
              const ObString &column_name = ref_expr->get_column_name();
              const ObString &table_name = ref_expr->get_table_name();
              LOG_USER_ERROR(OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                            column_name.length(), column_name.ptr(),
                            table_name.length(), table_name.ptr());
            }
          } else { // oracle mode
            if (is_generated_column) {
              ret = OB_ERR_UPDATE_OPERATION_ON_VIRTUAL_COLUMNS;
              LOG_WARN("virtual column cannot be updated in Oracle mode", K(ret));
            } else if (ref_expr->is_always_identity_column()) {
              ret = OB_ERR_UPDATE_GENERATED_ALWAYS_IDENTITY_COLUMN;
              LOG_USER_ERROR(OB_ERR_UPDATE_GENERATED_ALWAYS_IDENTITY_COLUMN);
            } else if (T_DEFAULT == expr->get_expr_type()) {
              ObDefaultValueUtils utils(stmt, &params_, this);
              if (OB_FAIL(utils.resolve_default_expr(*column, expr, scope))) {
                LOG_WARN("failed to resolve default expr", K(*column), K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(add_additional_function_according_to_type(column, expr, scope, true))) {
            LOG_WARN("fail to add additional function", K(ret), K(column));
          } else if (OB_FAIL(recursive_values_expr(expr))) {
            LOG_WARN("fail to resolve values expr", K(ret));
          } else {
            // 1. set geo sub type to cast mode to column covert expr when update
            // 2. check geo type while doing column covert.
            if (column->is_geo_ && T_FUN_COLUMN_CONV == expr->get_expr_type()) {
              ObColumnRefRawExpr *raw_expr = column->get_expr();
              if (OB_ISNULL(raw_expr)) {
                ret = OB_ERR_NULL_VALUE;
                LOG_WARN("raw expr in column item is null", K(ret));
              } else {
                ObGeoType geo_type = raw_expr->get_geo_type();
                uint64_t cast_mode = expr->get_extra();
                if (OB_FAIL(ObGeoCastUtils::set_geo_type_to_cast_mode(geo_type, cast_mode))) {
                  LOG_WARN("fail to set geometry type to cast mode", K(ret), K(geo_type));
                } else {
                  expr->set_extra(cast_mode);
                }
              }
            }
            if (OB_SUCC(ret)) {
              assignment.expr_ = expr;
              if (OB_FAIL(add_assignment(table_assigns, table, column, assignment))) {
                LOG_WARN("failed to add assignment", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::resolve_column_and_values(const ParseNode &assign_list,
                                                ObIArray<ObColumnRefRawExpr *> &target_list,
                                                ObIArray<ObRawExpr *> &value_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(T_ASSIGN_LIST != assign_list.type_ || assign_list.num_child_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resolver invalid status", K(ret), K(assign_list.type_));
  } else {
    if (1 == assign_list.num_child_ && T_OBJ_ACCESS_REF == assign_list.children_[0]->type_) {
      //update set ROW=record的扩展用法
      CK (OB_NOT_NULL(get_stmt()));
      if (OB_SUCC(ret)) {
        if (!get_stmt()->is_update_stmt()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("SET ROW must be in update statement", K(ret));
        } else {
          ObUpdateStmt *stmt = static_cast<ObUpdateStmt*>(get_stmt());
          if (1 != stmt->get_table_size()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("SET ROW must be used for single table", K(stmt->get_table_size()), K(ret));
          } else if (params_.secondary_namespace_ == NULL) {
            TableItem *table_item = stmt->get_table_item(0);
            CK (OB_NOT_NULL(table_item));
            if (OB_SUCC(ret)) {
              ObString row = ObString::make_string("row");
              ObString table_name = table_item->get_table_name().length() > 0 ?
                                        table_item->get_table_name() : ObString::make_string(" ");
              ret = OB_ERR_BAD_FIELD_ERROR; //oracle will report an ORA-00936 error, ob does not have the error code.
              LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, row.length(), row.ptr(), table_name.length(), table_name.ptr());
              LOG_WARN("column does not existed", K(ret));
            }
          } else {
            TableItem *table_item = stmt->get_table_item(0);
            const share::schema::ObTableSchema *table_schema = NULL;
            ObArray<ColumnItem> column_items;
            CK (OB_NOT_NULL(table_item));
            OZ (resolve_all_basic_table_columns(*table_item, false, &column_items));
            for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
              OZ (target_list.push_back(column_items.at(i).get_expr()));
            }
            OZ (expand_record_to_columns(*assign_list.children_[0], value_list));
            if (OB_SUCC(ret) && target_list.count() != value_list.count()) {
              ret = OB_ERR_TOO_MANY_VALUES;
              LOG_WARN("too many values", K(ret), K(target_list.count()), K(value_list.count()));
            }
          }
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < assign_list.num_child_; ++i) {
        ParseNode *left_node = NULL;
        ParseNode *right_node = NULL;
        ObRawExpr *value_expr = NULL;
        ObSEArray<ObColumnRefRawExpr *, 4> columns;
        ObSEArray<ObRawExpr *, 4> values;
        if (OB_ISNULL(assign_list.children_[i]) ||
            OB_ISNULL(left_node = assign_list.children_[i]->children_[0]) ||
            OB_ISNULL(right_node = assign_list.children_[i]->children_[1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("assign node is empty",
                   K(ret), K(assign_list.children_[i]), K(left_node), K(right_node));
        } else if (OB_FAIL(resolve_assign_columns(*left_node, columns))) {
          LOG_WARN("failed to resolve assign target", K(ret));
        } else if (OB_FAIL(resolve_sql_expr(*right_node, value_expr))) {
          LOG_WARN("failed to resolve value expr", K(ret));
        } else if (columns.count() == 1) {
          if (value_expr->is_query_ref_expr()) {
            // update t1 set (c1) = (select c1 from t2), c2 = 5;
            // resolve as update t1 set c1 = (select c1 from t2), c2 = 5;
            ObQueryRefRawExpr *query_ref = static_cast<ObQueryRefRawExpr *>(value_expr);
            ObSelectStmt *sel_stmt = NULL;
            if (OB_ISNULL(sel_stmt = query_ref->get_ref_stmt())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("select stmt is null", K(ret));
            } else if (1 < sel_stmt->get_select_item_size()) {
              ret = OB_ERR_TOO_MANY_VALUES;
              LOG_WARN("too many values", K(ret));
            }
          } else if (T_QUESTIONMARK == value_expr->get_expr_type()
            && !params_.is_prepare_stage_) {
            const ObColumnRefRawExpr *col_expr = columns.at(0);
            ObConstRawExpr *c_expr = static_cast<ObConstRawExpr*>(value_expr);
            CK (OB_NOT_NULL(col_expr));
            CK (OB_NOT_NULL(c_expr));
            if (OB_FAIL(ret)) {
            } else if (col_expr->get_result_type().get_obj_meta().is_enum_or_set()
              || col_expr->get_result_type().get_obj_meta().is_urowid()
              || params_.is_batch_stmt_) {
              // enum, set, rowid do not support cast
              int64_t param_idx = 0;
              OZ (c_expr->get_value().get_unknown(param_idx));
              CK (OB_NOT_NULL(params_.param_list_));
              CK (param_idx < params_.param_list_->count());
              OX (const_cast<ObObjParam &>(
              params_.param_list_->at(param_idx)).set_need_to_check_type(true));
            } else {
              OZ (c_expr->add_flag(IS_TABLE_ASSIGN));
              OX (c_expr->set_result_type(col_expr->get_result_type()));
            }
          }
        } else if (OB_UNLIKELY(!value_expr->is_query_ref_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid stmt or value expr", K(ret));
        } else {
          ObQueryRefRawExpr *query_ref = static_cast<ObQueryRefRawExpr *>(value_expr);
          ObSelectStmt *sel_stmt = NULL;
          if (OB_ISNULL(sel_stmt = query_ref->get_ref_stmt())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("select stmt is null", K(ret));
          } else if (columns.count() > sel_stmt->get_select_item_size()) {
            ret = OB_ERR_NOT_ENOUGH_VALUES;
            LOG_WARN("not enough values", K(ret));
          } else if (columns.count() < sel_stmt->get_select_item_size()) {
            ret = OB_ERR_TOO_MANY_VALUES;
            LOG_WARN("too many values", K(ret));
          } else if (OB_UNLIKELY(sel_stmt->get_CTE_table_size() > 0)) {
            ret = OB_ERR_NOT_SUBQUERY;
            LOG_WARN("subquery is cte", K(ret));
          } else if (columns.count() > 1) {
            if (OB_FAIL(try_add_remove_const_epxr(*sel_stmt))) {
              LOG_WARN("failed to add remove const expr", K(ret));
            }
            for (int64_t j = 0; OB_SUCC(ret) && j < columns.count(); ++j) {
              ObAliasRefRawExpr *alias_expr = NULL;
              if (OB_FAIL(ObRawExprUtils::build_query_output_ref(
                            *params_.expr_factory_, query_ref, j, alias_expr))) {
                LOG_WARN("failed to build query output ref", K(ret));
              } else if (OB_FAIL(values.push_back(alias_expr))) {
                LOG_WARN("failed to push back alias expr", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(append(target_list, columns))) {
            LOG_WARN("failed to append target list", K(ret));
          } else if (columns.count() == 1 && OB_FAIL(value_list.push_back(value_expr))) {
            LOG_WARN("failed to append value expr", K(ret));
          } else if (columns.count() > 1 && OB_FAIL(append(value_list, values))) {
            LOG_WARN("failed to append values", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::try_add_remove_const_epxr(ObSelectStmt &stmt)
{
  int ret = OB_SUCCESS;
  CK(NULL != session_info_);
  CK(NULL != params_.expr_factory_);
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_item_size(); ++i) {
      ObRawExpr *&expr = stmt.get_select_item(i).expr_;
      CK(NULL != expr);
      if (OB_SUCC(ret)) {
        ObRawExpr *new_expr = NULL;
        OZ(ObRawExprUtils::build_remove_const_expr(
                *params_.expr_factory_, *session_info_, expr, new_expr));
        CK(NULL != new_expr);
        OX(expr = new_expr);
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::recursive_values_expr(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null");
  } else if (expr && expr->has_flag(CNT_VALUES)) {
    if (expr->has_flag(IS_VALUES)) {
      if (OB_FAIL(process_values_function(expr))) {
        LOG_WARN("fail to resovle values expr", K(ret), K(*expr));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
        if (OB_FAIL(SMART_CALL(recursive_values_expr(expr->get_param_expr(i))))) {
          LOG_WARN("resolve raw expr param failed", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::process_values_function(ObRawExpr *&expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObDelUpdResolver::resolve_assign_columns(const ParseNode &assign_target,
                                             ObIArray<ObColumnRefRawExpr *> &column_list)
{
  int ret = OB_SUCCESS;
  bool is_column_list = false;
  int64_t column_count = 1;
  bool is_merge_resolver = false;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (assign_target.type_ != T_COLUMN_LIST &&
      assign_target.type_ != T_COLUMN_REF) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node", K(ret));
  } else if (assign_target.type_ == T_COLUMN_LIST) {
    column_count = assign_target.num_child_;
    is_column_list = true;
  } else {
    is_merge_resolver = get_stmt()->is_merge_stmt();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    const ParseNode *col_node = (is_column_list ? assign_target.children_[i] :
                                                  &assign_target);
    ObQualifiedName q_name;
    ObRawExpr *col_expr = NULL;
    ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
    if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
      LOG_WARN("resolve column reference name failed", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_column_ref(
                         col_node, case_mode, q_name))) {
      LOG_WARN("resolve column reference name failed", K(ret));
    } else if (q_name.is_star_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("'*' should not be here, parser has already blocked this error", K(ret));
    } else if (lib::is_oracle_mode() &&
               ObCharset::case_insensitive_equal(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME,
                                                 q_name.col_name_)) {
      ret = OB_ERR_VIRTUAL_COL_NOT_ALLOWED;
      LOG_WARN("cannot update rowid pseudo column", K(ret), K(q_name));
    } else if (is_merge_resolver) {
      // merge resolver does not check column unique
      column_namespace_checker_.disable_check_unique();
      if (OB_FAIL(resolve_table_column_expr(q_name, col_expr))) {
        report_user_error_msg(ret, col_expr, q_name);
        LOG_WARN("resolve column ref expr failed", K(ret), K(q_name));
      }
      column_namespace_checker_.enable_check_unique();
    } else {
      // other kinds of resolver
      if (OB_FAIL(resolve_table_column_expr(q_name, col_expr))) {
        report_user_error_msg(ret, col_expr, q_name);
        LOG_WARN("resolve column ref expr failed", K(ret), K(q_name));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reference expr is null", K(ret));
    } else if (!col_expr->is_column_ref_expr()) {
      ret = OB_ERR_NON_UPDATABLE_TABLE;
      LOG_WARN("update column should from updatable table", K(col_expr), K(ret));
    } else {
      ObColumnRefRawExpr *base_col_expr = static_cast<ObColumnRefRawExpr *>(col_expr);
      if (!get_stmt()->has_instead_of_trigger()) {
        if (OB_FAIL(ObTransformUtils::get_base_column(get_stmt(), base_col_expr))) {
          // this is not allow, but to compatible with oracle, the error will report at last
          ret = OB_SUCCESS;
          LOG_WARN("get base column failed", K(ret), KPC(base_col_expr));
        } else if (OB_ISNULL(base_col_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("base_col_expr is null", K(ret));
        }
      }
      if (OB_SUCC(ret) &&
          OB_FAIL(column_list.push_back(static_cast<ObColumnRefRawExpr *>(col_expr)))) {
        LOG_WARN("failed to push back column expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::generate_wrapper_expr_for_assignemnts(ObIArray<ObAssignment> &assigns,
                                                            bool has_before_trigger)
{
  int ret = OB_SUCCESS;
  if (has_before_trigger) {
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); ++i) {
      ObAssignment &as = assigns.at(i);
      ObRawExpr *value_expr = as.expr_;
      if (OB_FAIL(ObRawExprUtils::build_wrapper_inner_expr(
          *params_.expr_factory_, *session_info_, as.expr_, as.expr_))) {
        LOG_WARN("failed to build wrapper inner expr", K(ret));
      } else {
        LOG_DEBUG("add wrapper inner expr", K(*as.expr_), K(as.expr_));
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::resolve_additional_assignments(ObIArray<ObTableAssignment> &assigns,
                                                     const ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard *schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  const TableItem *table_item = NULL;
  ObDMLStmt *stmt = get_stmt();
  bool trigger_exist = false;
  if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("params is invalid", K_(params_.expr_factory), K(stmt));
  } else if (T_UPDATE_SCOPE != scope && T_INSERT_SCOPE != scope) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input scope", K(scope));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); ++i) {
    if (OB_ISNULL(schema_checker_) ||
        OB_ISNULL(schema_guard = schema_checker_->get_schema_guard()) ||
        OB_ISNULL(table_item = stmt->get_table_item_by_id(assigns.at(i).table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid schema checker", K(schema_checker_));
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session_info_ is null", K(ret));
    } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
        table_item->get_base_table_item().ref_id_, table_schema, table_item->is_link_table()))) {
      LOG_WARN("fail to get table schema", K(ret), KPC(table_item));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get table schema", KPC(table_item), K(table_schema));
    } else if (OB_FAIL(table_schema->has_before_update_row_trigger(*schema_guard, trigger_exist))) {
      LOG_WARN("fail to call has_before_update_row_trigger", K(*table_schema));
    } else if (OB_FAIL(generate_wrapper_expr_for_assignemnts(assigns.at(i).assignments_, trigger_exist))) {
      LOG_WARN("failed to resolve addtional assignments for const", K(ret), K(i));
    } else {
      for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
          (OB_SUCCESS == ret && iter != table_schema->column_end()); ++iter) {
        ColumnItem *col_item = NULL;
        ObColumnSchemaV2 *column_schema = *iter;
        bool need_assigned = false;
        uint64_t column_id = OB_INVALID_ID;
        if (NULL == column_schema) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column schema fail", K(column_schema));
        } else if (FALSE_IT(column_id = column_schema->get_column_id())) {
          //do nothing
        } else if (OB_FAIL(check_need_assignment(assigns.at(i).assignments_,
                                                 table_item->table_id_,
                                                 trigger_exist,
                                                 *column_schema,
                                                 need_assigned))) {
          LOG_WARN("fail to check assignment exist", KPC(table_item), K(column_id));
        } else if (need_assigned) {
          // for insert scope, on duplicate key update column list already
          // exists in insert list, therefore, only need to add assignment.
          // add assign
          ObAssignment assignment;
          ObSEArray<ObColumnRefRawExpr *, 1> col_exprs;
          if (OB_FAIL(add_column_to_stmt(*table_item, *column_schema, col_exprs))) {
            LOG_WARN("add column to stmt failed", K(ret), K(*table_item), K(*column_schema));
          } else if (col_exprs.empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no column expr returned", K(ret));
          } else if (OB_ISNULL(col_item = stmt->get_column_item_by_id(
              table_item->table_id_, col_exprs.at(0)->get_column_id()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get column item failed", K(ret));
          } else {
            assignment.column_expr_ = col_item->expr_;
            assignment.is_implicit_ = true;
            if (column_schema->is_on_update_current_timestamp()) {
              assignment.column_expr_->set_result_flag(ON_UPDATE_NOW_FLAG);
              ObDefaultValueUtils utils(stmt, &params_, this);
              if (OB_FAIL(utils.build_now_expr(col_item, assignment.expr_))) {
                LOG_WARN("fail to build default expr", K(ret));
              }
            } else if (column_schema->is_generated_column()) {
              if (OB_FAIL(copy_schema_expr(*params_.expr_factory_,
                                           col_item->expr_->get_dependant_expr(),
                                           assignment.expr_))) {
                LOG_WARN("failed to copy dependant expr", K(ret));
              }
            } else if (trigger_exist) {
              assignment.expr_ = col_item->expr_;
            }
            if (OB_FAIL(ret)) {
              //do nothing
            } else if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(*params_.expr_factory_,
                                                                      *params_.allocator_,
                                                                      *col_item->get_expr(),
                                                                      assignment.expr_,
                                                                      session_info_))) {
              LOG_WARN("fail to build format expr", K(ret));
            } else if (trigger_exist &&
                      OB_FAIL(ObRawExprUtils::build_wrapper_inner_expr(*params_.expr_factory_, *session_info_, assignment.expr_, assignment.expr_))) {
              LOG_WARN("failed to build wrapper inner expr", K(ret));
            } else {
              // 1. set geo sub type to cast mode to column covert expr when update
              // 2. check geo type while doing column covert.
              if (col_item->is_geo_ && T_FUN_COLUMN_CONV == assignment.expr_->get_expr_type()) {
                ObColumnRefRawExpr *raw_expr = col_item->get_expr();
                if (OB_ISNULL(raw_expr)) {
                  ret = OB_ERR_NULL_VALUE;
                  LOG_WARN("raw expr in column item is null", K(ret));
                } else {
                  ObGeoType geo_type = raw_expr->get_geo_type();
                  uint64_t cast_mode = assignment.expr_->get_extra();
                  if (OB_FAIL(ObGeoCastUtils::set_geo_type_to_cast_mode(geo_type, cast_mode))) {
                    LOG_WARN("fail to set geometry type to cast mode", K(ret), K(geo_type));
                  } else {
                    assignment.expr_->set_extra(cast_mode);
                  }
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(add_assignment(assigns, table_item, col_item, assignment))) {
                  LOG_WARN("failed to ass assignment", K(ret));
                }
              }
            }
          }
        }
      }  // end for
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_heap_table_update(assigns.at(i)))) {
        LOG_WARN("fail to add assignment to heap table", K(ret), K(i),
                 "assigns", assigns.at(i));
      }
    }
  }
  return ret;
}

// duplicated assignments are permitted, such SET c2=c1*2, c2=1+c2.
int ObDelUpdResolver::add_assignment(common::ObIArray<ObTableAssignment> &assigns,
                                     const TableItem *table_item,
                                     const ColumnItem *col,
                                     ObAssignment &assign)
{
  int ret = OB_SUCCESS;
  ObTableAssignment *table_assign = NULL;
  int64_t N = assigns.count();
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(table_item) || OB_ISNULL(assign.column_expr_)
      || OB_ISNULL(get_stmt()) || OB_ISNULL(get_stmt()->get_query_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(schema_checker), K(table_item), K_(assign.column_expr));
  } else if (assign.column_expr_->get_result_type().is_lob()
      && params_.is_batch_stmt_) {
    ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
    //batch stmt execution does not support update lob locator columns
    //because we can not do defensive check with lob locator columns
    LOG_TRACE("batch stmt can not supported with lob locator", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    ObTableAssignment &ta = assigns.at(i);
    if (ta.table_id_ == assign.column_expr_->get_table_id()) {
      table_assign = &ta;
      break;
    }
  }

  if (OB_SUCC(ret) && NULL == table_assign) {
    ObTableAssignment new_ta;
    new_ta.table_id_ = table_item->table_id_;
    if (OB_FAIL(assigns.push_back(new_ta))) {
      LOG_WARN("failed to add ta", K(ret));
    } else {
      table_assign = &assigns.at(assigns.count() - 1);
    }
  }
  if (OB_SUCC(ret) && (is_mysql_mode() || assign.column_expr_->is_generated_column())) {
    //in MySQL:
    //The second assignment in the following statement sets col2 to the current (updated) col1 value,
    //not the original col1 value.
    //The result is that col1 and col2 have the same value.
    //This behavior differs from standard SQL.
    //UPDATE t1 SET col1 = col1 + 1, col2 = col1;
    //But in Oracle, its behavior is same with standard SQL
    //set original col1 to col1 and col2
    //For generated column, when cascaded column is updated, the generated column will be updated with new column
    ObRawExprCopier copier(*params_.expr_factory_);
    for (int64_t i = 0; OB_SUCC(ret) && i < table_assign->assignments_.count(); ++i) {
      if (!table_assign->assignments_.at(i).column_expr_->is_xml_column()) {
        if (OB_FAIL(copier.add_replaced_expr(table_assign->assignments_.at(i).column_expr_,
                                            table_assign->assignments_.at(i).expr_))) {
          LOG_WARN("failed to add replaced expr", K(ret));
        }
      } else {
        // is generated column and ref column is xmltype, generated column ref hiddlen column actually
        // udt column replace is done in pre transfrom but here generate column need replace
        const ObRawExpr *from_expr = ObRawExprUtils::get_sql_udt_type_expr_recursively(assign.expr_);
        const ObRawExpr *to_expr = table_assign->assignments_.at(i).expr_;
        if (OB_ISNULL(from_expr)) { // do nonthing
        } else {
          if (OB_FAIL(copier.add_replaced_expr(from_expr, to_expr))) {
            LOG_WARN("failed to add replaced expr", K(ret));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < get_stmt()->get_subquery_expr_size(); ++i) {
      if (OB_FAIL(copier.add_skipped_expr(get_stmt()->get_subquery_exprs().at(i), false))) {
        LOG_WARN("failed to add skipped expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(copier.copy_on_replace(assign.expr_, assign.expr_))) {
      LOG_WARN("failed to copy on replace expr", K(ret));
    }
  }
  bool found = false;
  if (OB_SUCC(ret)) {
    if (get_stmt()->get_query_ctx()->is_prepare_stmt()) {
      // do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !found && i < table_assign->assignments_.count(); ++i) {
        if (assign.column_expr_ == table_assign->assignments_.at(i).column_expr_) {
          table_assign->assignments_.at(i) = assign;
          table_assign->assignments_.at(i).is_duplicated_ = true; //this column was updated repeatedly
          found = true;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !found) {
    if (OB_FAIL(table_assign->assignments_.push_back(assign))) {
      LOG_WARN("store assignment failed", K(ret));
    }
  }
  return ret;
}

int ObDelUpdResolver::check_need_assignment(const common::ObIArray<ObAssignment> &assigns,
                                            uint64_t table_id,
                                            bool before_update_row_trigger_exist,
                                            const ObColumnSchemaV2 &column,
                                            bool &need_assign)
{
  need_assign = false;
  int ret = OB_SUCCESS;
  bool exist = false;
  ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (stmt->has_instead_of_trigger()) {
    // 兼容oracle,这里的列不级联更新
    // do nothing
  } else if (column.is_udt_hidden_column()) {
     // do nothing, will handle in udt transform
  } else if (column.is_generated_column()) {
    if (OB_FAIL(ObResolverUtils::check_whether_assigned(stmt, assigns, table_id, column.get_column_id(), exist))) {
      LOG_WARN("check column whether assigned failed", K(ret));
    } else if (!exist) {
      ObArray<uint64_t> cascaded_columns;
      if (OB_FAIL(column.get_cascaded_column_ids(cascaded_columns))) {
        LOG_WARN("get cascaded column ids failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && !exist && i < cascaded_columns.count(); ++i) {
        if (OB_FAIL(ObResolverUtils::check_whether_assigned(stmt, assigns, table_id, cascaded_columns.at(i), exist))) {
          LOG_WARN("check whether assigned cascaded columns failed", K(ret), K(table_id), K(cascaded_columns.at(i)));
        }
      }
      if (OB_SUCC(ret) && exist) {
        //the generated column isn't assigned, but the cascaded columns have been assigned
        //so assign the generated column
        need_assign = true;
      }
    }
  } else if (column.is_on_update_current_timestamp() || before_update_row_trigger_exist) {
    if (column.get_column_id() == OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
      // for heap_table the hidden_pk should not be updated
    } else if (OB_FAIL(ObResolverUtils::check_whether_assigned(stmt, assigns, table_id, column.get_column_id(), exist))) {
      LOG_WARN("check whether assigned cascaded columns failed", K(ret), K(table_id), K(column.get_column_id()));
    } else if (!exist) {
      need_assign = true;
    }
  }
  return ret;
}

int ObDelUpdResolver::set_base_table_for_updatable_view(TableItem &table_item,
                                                        const ObColumnRefRawExpr &col_ref,
                                                        const bool log_error/* = true */)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *stmt = table_item.ref_query_;
  ObDMLStmt *dml = get_stmt();
  const int64_t idx = col_ref.get_column_id() - OB_APP_MIN_COLUMN_ID;
  OZ(check_stack_overflow());
  if (OB_FAIL(ret)) {
  } else if (!table_item.is_generated_table() && !table_item.is_temp_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not view or stmt is NULL or invalid column id",
        K(ret), K(table_item), K(idx), KP(stmt), K(col_ref));
  } else if (OB_ISNULL(dml) || OB_ISNULL(stmt)
             || idx < 0 || idx >= stmt->get_select_item_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not view or stmt is NULL or invalid column id",
        K(ret), K(table_item), K(idx), KP(stmt), K(col_ref));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null", K(ret));
  } else {
    if (table_item.is_view_table_) {
      const ObTableSchema *table_schema = NULL;
      if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(), stmt->get_view_ref_id(), table_schema, table_item.is_link_table()))) {
        LOG_WARN("get table schema failed", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL table schema", K(ret));
      } else {
        if (!table_schema->get_view_schema().get_view_is_updatable()) {
          ret = OB_ERR_MODIFY_READ_ONLY_VIEW;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!dml->has_instead_of_trigger()
        && OB_FAIL(ObResolverUtils::uv_check_basic(*stmt, dml->is_insert_stmt()))) {
      LOG_WARN("not updatable", K(ret));
    } else {
      ObRawExpr *expr = stmt->get_select_item(idx).expr_;
      if (!expr->is_column_ref_expr() ||
          (lib::is_oracle_mode() &&
           ObCharset::case_insensitive_equal(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME,
                                             static_cast<ObColumnRefRawExpr *>(expr)->get_column_name()))) {
        ret = is_mysql_mode() ? OB_ERR_NONUPDATEABLE_COLUMN : OB_ERR_VIRTUAL_COL_NOT_ALLOWED;
        LOG_WARN("column is not updatable", K(ret), K(col_ref));
      } else {
        ObColumnRefRawExpr *new_col_ref = static_cast<ObColumnRefRawExpr *>(expr);
        TableItem *new_table_item = stmt->get_table_item_by_id(new_col_ref->get_table_id());
        if (NULL == new_table_item) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get NULL table item", K(ret));
        } else {
          table_item.view_base_item_ = new_table_item;
          if (new_table_item->is_basic_table()) {
            const ObTableSchema *base_table_schema = NULL;
            if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                        new_table_item->ref_id_, base_table_schema))) {
              LOG_WARN("get table schema failed", K(ret));
            } else if (OB_ISNULL(base_table_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL table schema", K(ret));
            } else if (OB_UNLIKELY(base_table_schema->is_vir_table())) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "DML operation on Virtual Table/Temporary Table");
            }
          } else if (new_table_item->is_generated_table() || new_table_item->is_temp_table()) {
            const bool inner_log_error = false;
            if (new_table_item->is_view_table_ && is_oracle_mode()
                && stmt_->is_support_instead_of_trigger_stmt()) {
              bool has_tg = false;
              OZ (has_need_fired_trigger_on_view(new_table_item, has_tg));
              OX ((static_cast<ObDelUpdStmt*>(stmt_))
                   ->set_has_instead_of_trigger(has_tg));
            }
            if (OB_FAIL(SMART_CALL(set_base_table_for_updatable_view(*new_table_item,
                                                                     *new_col_ref,
                                                                     inner_log_error)))) {
              LOG_WARN("find base table for updatable view failed", K(ret));
            }
          } else if (new_table_item->is_fake_cte_table()) {
            ret = OB_ERR_ILLEGAL_VIEW_UPDATE;
            LOG_WARN("illegal view update", K(ret));
          } else if (new_table_item->is_values_table()) {
            ret = dml->is_insert_stmt() ? OB_ERR_NON_INSERTABLE_TABLE : OB_ERR_NON_UPDATABLE_TABLE;
            LOG_WARN("view is not updatable", K(ret));
          } else if (new_table_item->is_json_table()) {
            ret = OB_ERR_NON_INSERTABLE_TABLE;
            LOG_WARN("json table can not be insert", K(ret));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column is not updatable", K(ret), K(col_ref));
          }
        }
      }
    }
    if (log_error && OB_SUCCESS != ret) {
      ObString update_str = "UPDATE";
      if (OB_ERR_NONUPDATEABLE_COLUMN == ret) {
        LOG_USER_ERROR(OB_ERR_NONUPDATEABLE_COLUMN,
            col_ref.get_column_name().length(), col_ref.get_column_name().ptr());
      } else if (OB_ERR_NON_INSERTABLE_TABLE == ret) {
        LOG_USER_ERROR(OB_ERR_NON_INSERTABLE_TABLE,
            table_item.table_name_.length(), table_item.table_name_.ptr());
      } else if (OB_ERR_NON_UPDATABLE_TABLE == ret) {
        LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
            table_item.table_name_.length(), table_item.table_name_.ptr(),
            update_str.length(), update_str.ptr());
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::set_base_table_for_view(TableItem &table_item, const bool log_error/* = true */)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *stmt = table_item.ref_query_;
  CK(OB_NOT_NULL(get_stmt()) && !get_stmt()->has_instead_of_trigger());
  OZ(check_stack_overflow());
  if (OB_FAIL(ret)) {
  } else if (!table_item.is_generated_table() || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not generated table or referred query is NULL", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null", K(ret));
  } else {
    if (table_item.is_view_table_) {
      const ObTableSchema *table_schema = NULL;
      if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(), stmt->get_view_ref_id(), table_schema, table_item.is_link_table()))) {
        LOG_WARN("get table schema failed", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL table schema", K(ret));
      } else {
        if (!table_schema->get_view_schema().get_view_is_updatable()) {
          ret = OB_ERR_MODIFY_READ_ONLY_VIEW;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    const bool is_insert = false;
    if (OB_FAIL(ObResolverUtils::uv_check_basic(*stmt, is_insert))) {
      LOG_WARN("not updatable", K(ret));
    } else if (stmt->get_table_items().empty()) {
      // create view v as select 1 a;
      ret = is_mysql_mode() ? OB_ERR_NON_UPDATABLE_TABLE : OB_ERR_ILLEGAL_VIEW_UPDATE;
      LOG_WARN("no table item in select stmt", K(ret));
    } else {
      // get the first table item for oracle mode
      TableItem *base = stmt->get_table_items().at(0);
      if (stmt->get_table_items().count() > 1) {
        // mysql delete join view not supported.
        if (is_mysql_mode()) {
          ret = OB_ERR_VIEW_DELETE_MERGE_VIEW;
          LOG_WARN("delete join view", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (NULL == base) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (base->is_link_table()) {
        table_item.view_base_item_ = base;
      } else if (base->is_basic_table()) {
        table_item.view_base_item_ = base;
        const ObTableSchema *base_table_schema = NULL;
        if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                    base->ref_id_, base_table_schema))) {
          LOG_WARN("get table schema failed", K(ret));
        } else if (OB_ISNULL(base_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL table schema", K(ret));
        } else if (OB_UNLIKELY(base_table_schema->is_vir_table())) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "DML operation on Virtual Table/Temporary Table");
        }
      } else if (base->is_generated_table()) {
        table_item.view_base_item_ = base;
        const bool inner_log_error = false;
        if (OB_FAIL(SMART_CALL(set_base_table_for_view(*base, inner_log_error)))) {
          LOG_WARN("set base table for view failed", K(ret));
        }
      } else if (base->is_values_table()) {
        ret = OB_ERR_NON_UPDATABLE_TABLE;
        LOG_WARN("non update table", K(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table type in view", K(ret), K(*base));
      }
    }
    if (log_error && OB_SUCCESS != ret) {
      ObString del_str = "DELETE";//仅仅只有mysql模式下的delete会用到这个报错信息
      if (OB_ERR_NON_UPDATABLE_TABLE == ret) {
        LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
            table_item.table_name_.length(), table_item.table_name_.ptr(),
            del_str.length(), del_str.ptr());
      } else if (OB_ERR_VIEW_DELETE_MERGE_VIEW == ret) {
        LOG_USER_ERROR(OB_ERR_VIEW_DELETE_MERGE_VIEW,
            table_item.database_name_.length(), table_item.database_name_.ptr(),
            table_item.table_name_.length(), table_item.table_name_.ptr());
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::check_same_base_table(const TableItem &table_item,
                                            const ObColumnRefRawExpr &col_ref,
                                            const bool log_error/* = true */)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *stmt = table_item.ref_query_;
  const int64_t idx = col_ref.get_column_id() - OB_APP_MIN_COLUMN_ID;
  if ((!table_item.is_generated_table() && !table_item.is_temp_table())
      || OB_ISNULL(table_item.view_base_item_)
      || OB_ISNULL(stmt)
      || idx < 0 || idx >= stmt->get_select_item_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not view or stmt is NULL or invalid column id",
        K(ret), K(table_item), K(idx), KP(stmt), K(col_ref));
  } else {
    ObRawExpr *expr = stmt->get_select_item(idx).expr_;
    if (!expr->is_column_ref_expr() ||
        (lib::is_oracle_mode() &&
         ObCharset::case_insensitive_equal(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME,
                                      static_cast<ObColumnRefRawExpr *>(expr)->get_column_name()))) {
      ret = is_mysql_mode() ? OB_ERR_NONUPDATEABLE_COLUMN : OB_ERR_VIRTUAL_COL_NOT_ALLOWED;
      LOG_WARN("column is not updatable", K(ret), K(col_ref));
    } else {
      ObColumnRefRawExpr *new_col_ref = static_cast<ObColumnRefRawExpr *>(expr);
      const TableItem *new_table_item = table_item.view_base_item_;
      if (new_col_ref->get_table_id() != new_table_item->table_id_) {
        ret = is_mysql_mode() ? OB_ERR_VIEW_MULTIUPDATE : OB_ERR_O_VIEW_MULTIUPDATE;
        LOG_WARN("Can not modify more than one base table through a join view", K(ret), K(col_ref));
      } else {
        if (new_table_item->is_basic_table() || new_table_item->is_link_table()) {
          // is base table, do nothing
        } else if (new_table_item->is_generated_table() || new_table_item->is_temp_table()) {
          const bool inner_log_error = false;
          if (OB_FAIL(check_same_base_table(*new_table_item, *new_col_ref, inner_log_error))) {
            LOG_WARN("check update columns is same base table failed", K(ret));
          }
        } else if (new_table_item->is_fake_cte_table()) {
          ret = OB_ERR_ILLEGAL_VIEW_UPDATE;
          LOG_WARN("illegal view update", K(ret));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column is not updatable", K(ret), K(col_ref));
        }
      }
    }
    if (log_error) {
      if (OB_ERR_NONUPDATEABLE_COLUMN == ret) {
        LOG_USER_ERROR(OB_ERR_NONUPDATEABLE_COLUMN,
            col_ref.get_column_name().length(), col_ref.get_column_name().ptr());
      } else if (OB_ERR_VIEW_MULTIUPDATE == ret) {
        LOG_USER_ERROR(OB_ERR_VIEW_MULTIUPDATE,
            table_item.database_name_.length(), table_item.database_name_.ptr(),
            table_item.table_name_.length(), table_item.table_name_.ptr());
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::add_all_column_to_updatable_view(ObDMLStmt &stmt,
                                                       const TableItem &table_item,
                                                       const bool &has_need_fired_tg_on_view)
{
  int ret = OB_SUCCESS;
  OZ(check_stack_overflow());
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params_.session_info_ is null", K(ret));
  } else if (!table_item.is_basic_table() && !table_item.is_link_table() && !table_item.is_generated_table()
             && !table_item.is_temp_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected table item", K(ret), K(table_item));
  } else {
    if ((table_item.is_generated_table() || table_item.is_temp_table()) && !has_need_fired_tg_on_view) {
      if (OB_ISNULL(table_item.ref_query_) || OB_ISNULL(table_item.view_base_item_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("generate table bug reference query is NULL or base table item is NULL",
            K(ret), K(table_item));
      } else if (OB_FAIL(SMART_CALL(add_all_column_to_updatable_view(
          *table_item.ref_query_, *table_item.view_base_item_, has_need_fired_tg_on_view)))) {
        LOG_WARN("add all column for updatable view failed", K(ret), K(table_item));
      }
    }
  }
  if (OB_SUCC(ret)) {
    auto add_select_item_func = [&](ObSelectStmt &select_stmt, ColumnItem &col) {
      int ret = OB_SUCCESS;
      bool found_in_select = false;
      FOREACH_CNT_X(si, select_stmt.get_select_items(), !found_in_select) {
        if (si->expr_ == col.expr_) {
          found_in_select = true;
        }
      }
      if (!found_in_select) {
        SelectItem select_item;
        select_item.implicit_filled_ = true;
        select_item.expr_ = col.expr_;
        // concat column's table name and column name as select item's alias name
        const int32_t size = col.expr_->get_table_name().length()
            + 1 // "."
            + col.expr_->get_column_name().length();
        char *buf = static_cast<char *>(allocator_->alloc(size));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(size));
        } else {
          char *p = buf;
          MEMCPY(p, col.expr_->get_table_name().ptr(), col.expr_->get_table_name().length());
          p += col.expr_->get_table_name().length();
          *p = '.';
          p++;
          MEMCPY(p, col.expr_->get_column_name().ptr(), col.expr_->get_column_name().length());
          select_item.alias_name_.assign_ptr(buf, size);
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(select_stmt.add_select_item(select_item))) {
            LOG_WARN("add select item failed", K(ret));
          }
        }
      }
      return ret;
    };
    ColumnItem *col_item = NULL;
    if (table_item.is_basic_table() || table_item.is_link_table()) {
      const ObTableSchema *table_schema = NULL;
      if (OB_FAIL(schema_checker_->get_table_schema(params_.session_info_->get_effective_tenant_id(), table_item.ref_id_, table_schema, table_item.is_link_table()))) {
        LOG_WARN("get table schema failed", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL table schema", K(ret));
      } else {
        for (auto iter = table_schema->column_begin();
            OB_SUCC(ret) && iter != table_schema->column_end(); iter++) {
          col_item = stmt.get_column_item_by_id(table_item.table_id_, (*iter)->get_column_id());
          if (NULL == col_item) { // column not found
            if (OB_FAIL(resolve_basic_column_item(table_item, (*iter)->get_column_name(), true, col_item, &stmt))) {
              LOG_WARN("resolve basic column item failed", K(ret), K(table_item), K(*iter));
            }
          }
          if (OB_SUCC(ret)) {
            if (stmt::T_SELECT == stmt.get_stmt_type() && !has_need_fired_tg_on_view) {
              // 包含instead-of-trigger的语句这里不需要加入到select_items_里面，因为此前的
              // select_items_已恰好足够使用
              if (OB_FAIL(add_select_item_func(static_cast<ObSelectStmt &>(stmt), *col_item))) {
                LOG_WARN("add column item to select item failed", K(ret));
              }
            }
          }
        }
      }
    } else if (table_item.is_generated_table() || table_item.is_temp_table()) {
      ObSelectStmt *ref_stmt = table_item.ref_query_;
      for (int64_t i = 0; OB_SUCC(ret) && i < ref_stmt->get_select_item_size(); i++) {
        const uint64_t column_id = OB_APP_MIN_COLUMN_ID + i;
        ObString col_name; // not used
        col_item = NULL;
        if (OB_FAIL(resolve_generated_table_column_item(
            table_item, col_name, col_item, &stmt, column_id))) {
          LOG_WARN("resolve generate table item failed", K(ret));
        } else {
          if (stmt::T_SELECT == stmt.get_stmt_type()) {
            if (OB_FAIL(add_select_item_func(static_cast<ObSelectStmt &>(stmt), *col_item))) {
              LOG_WARN("add column item to select item failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::resolve_error_logging(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  const ParseNode *table_name_node = NULL;
  const ParseNode *reject_node = NULL;
  const ObTableSchema *table_schema = NULL;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  CK (OB_NOT_NULL(del_upd_stmt));
  CK (OB_NOT_NULL(schema_checker_));
  CK (OB_NOT_NULL(session_info_));
  ObString database_name = session_info_->get_database_name();
  uint64_t tenant_id = session_info_->get_effective_tenant_id();
  if (ObItemType::T_ERR_LOG_CALUSE != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is not ObItemType::T_ERR_LOG_CALUSE");
  } else {
    del_upd_stmt->set_is_error_logging(true);
    if (OB_ISNULL(table_name_node = node->children_[0])) {
      // 不指定table name
      ObString default_table_name;
      ObString dst_table_name;
      char buf[OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE];  // 128 Bytes
      default_table_name.assign_buffer(buf, OB_MAX_USER_TABLE_NAME_LENGTH_ORACLE);
      const char *default_table_name_prefix = "ERR$_";
      int write_length = default_table_name.write(default_table_name_prefix, strlen(default_table_name_prefix));
      write_length += default_table_name.write(del_upd_stmt->get_table_item(0)->get_table_name().ptr(),
          del_upd_stmt->get_table_item(0)->get_table_name().length());
      if (write_length != del_upd_stmt->get_table_item(0)->get_table_name().length() + strlen(default_table_name_prefix)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("write default table_name fail", K(write_length),
                  K(del_upd_stmt->get_table_item(0)->get_table_name().length() + strlen(default_table_name_prefix)));
      } else if (OB_FAIL(schema_checker_->get_table_schema(tenant_id, database_name,
                         default_table_name, false, table_schema))) {
        LOG_WARN("fail to get_table_schema ", K(ret));
      } else if (OB_FAIL(OB_ISNULL(table_schema))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null");
      } else if (OB_FAIL(ob_write_string(*allocator_, default_table_name, dst_table_name))) {
        LOG_WARN("fail to write table to string", K(ret), K(default_table_name));
      } else if (OB_FAIL(check_err_log_table(dst_table_name, database_name))) {
        LOG_WARN("check_err_log_table fails", K(ret), K(default_table_name), K(database_name));
      }
      LOG_WARN("after append default table name", K(default_table_name));
    } else if (ObItemType::T_INTO_ERR_LOG_TABLE != table_name_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error");
    } else {
      // resolver error logging table  指定table
      if (OB_FAIL(resolve_err_log_table(table_name_node))) {
        LOG_WARN("fail execute resolve_err_log_table", K(ret));
      }
    }
    // resolver error logging tag (simple_expr)
    // TODO @kaizhan.dkz暂时先不开发

    // resolver reject limit
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(reject_node = node->children_[2])) {
      // 不指定reject limit
      del_upd_stmt->set_err_log_reject_limit(0);
    } else if (ObItemType::T_ERR_LOG_LIMIT != reject_node->type_) {
      // error type
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error type of reject_limit node", K(reject_node->type_));
    } else if (OB_FAIL(resolve_err_log_reject(reject_node))) {
      LOG_WARN("resolve_err_log_reject failed", K(ret));
    }
  }
  return ret;
}

int ObDelUpdResolver::check_err_log_table(ObString &table_name, ObString &database_name)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  uint64_t tenant_id = session_info_->get_effective_tenant_id();
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  if (OB_ISNULL(del_upd_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(tenant_id,
                                                       database_name,
                                                       table_name,
                                                       false,
                                                       table_schema))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema is null", K(ret));
  } else {
    del_upd_stmt->set_err_log_table_id(table_schema->get_table_id());
    del_upd_stmt->set_err_log_table_name(table_name);
    del_upd_stmt->set_err_log_database_name(database_name);
    ObColumnIterByPrevNextID iter(*table_schema);
    const ObColumnSchemaV2 *column_schema = NULL;
    int index = 0;
    while (OB_SUCC(ret) && OB_SUCC(iter.next(column_schema))) {
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column is null", K(ret));
      } else if (column_schema->is_shadow_column()) {
        // don't care shadow columns for error logging table
        continue;
      } else if (column_schema->is_invisible_column()) {
        // don't show invisible columns for error logging table
        continue;
      } else if (column_schema->is_hidden()) {
        // jump hidden column, for error logging table
        continue;
      } else if (index < 5) {
        // check table Meet the requirements of the error logging table
        // here check first 5 columns should be ORA_ERR_NUMBER$, ORA_ERR_MESG$ etc...
        if (0 != column_schema->get_column_name_str().case_compare(err_log_default_columns_[index]) ) {
          ret = OB_ERR_MISS_ERR_LOG_MANDATORY_COLUMN;
          LOG_USER_ERROR(OB_ERR_MISS_ERR_LOG_MANDATORY_COLUMN, static_cast<int>(strlen(err_log_default_columns_[index])), err_log_default_columns_[index]);
        } else {
          // TODO oracle默认的5列类型不能为LONG 由于暂时不支持LONG类型，所以暂时不做检查
          // 暂时不实现 ORA_ERR_TAG$ 列
        }
        index++;
      } else {
        ObSEArray<ObDmlTableInfo*, 2> table_info;
        if (OB_FAIL(del_upd_stmt->get_dml_table_infos(table_info))) {
          LOG_WARN("failed to get dml table info", K(ret));
        } else if (OB_UNLIKELY(1 != table_info.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(table_info.count()), K(ret));
        } else if (OB_ISNULL(table_info.at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < table_info.at(0)->column_exprs_.count(); i++) {
            ObColumnRefRawExpr *column_expr = NULL;
            ColumnItem column_item;
            if (OB_ISNULL(column_expr = table_info.at(0)->column_exprs_.at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret));
            } else if (0 != column_expr->get_column_name().case_compare(column_schema->get_column_name_str())) {
              continue;
            } else if (OB_FAIL(check_err_log_support_type(column_expr->get_data_type()))) {
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "type for error logging");
              LOG_WARN("column type is not supported for error_logging", K(column_expr->get_data_type()));
            } else if (OB_FAIL(del_upd_stmt->get_error_log_info().error_log_exprs_.push_back(column_expr))) {
              LOG_WARN("failed to push back column expr", K(ret));
            } else { /*do nothing*/ }
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObDelUpdResolver::check_err_log_support_type(ObObjType column_type)
{
  int ret = OB_SUCCESS;
  ObObjOType column_o_type = ob_obj_type_to_oracle_type(column_type);
  switch (column_o_type) {
  case ObONotSupport:
  case ObONullType:
  case ObOLobType:
  case ObOExtendType:
  case ObOUnknownType:
  case ObOURowIDType:
  case ObOLobLocatorType:
  case ObOUDTSqlType:
  case ObOMaxType:
    ret = OB_NOT_SUPPORTED;
    break;
  default:
    break;
  }
  return ret;
}

// user specified error_logging table name
int ObDelUpdResolver::resolve_err_log_table(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  ObString table_name;
  ObString database_name;
  uint64_t database_id = OB_INVALID_ID;
  uint64_t dblink_id = OB_INVALID_ID;
  ObString synonym_name;
  ObString dblink_name;
  ObString synonym_db_name;
  bool is_db_explicit;
  const ParseNode *relation_factor_node = NULL;
  bool use_sys_tenant = false;
  uint64_t table_id = OB_INVALID_ID;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  bool is_reverse_link = false; // no use
  ObArray<uint64_t> ref_obj_ids;
  CK (OB_NOT_NULL(del_upd_stmt));
  if (OB_ISNULL(relation_factor_node = node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node should not be null", K(ret));
  } else if (T_RELATION_FACTOR != relation_factor_node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relation_factor_node type should be T_RELATION_FACTOR", K(relation_factor_node->type_));
  } else if (OB_FAIL(resolve_table_relation_factor_wrapper(relation_factor_node,
                                                           dblink_id,
                                                           database_id,
                                                           table_name,
                                                           synonym_name,
                                                           synonym_db_name,
                                                           database_name,
                                                           dblink_name,
                                                           is_db_explicit,
                                                           use_sys_tenant,
                                                           is_reverse_link,
                                                           ref_obj_ids))) {
    if (OB_TABLE_NOT_EXIST == ret || OB_ERR_BAD_DATABASE == ret) {
      if (is_information_schema_database_id(database_id)) {
        ret = OB_ERR_UNKNOWN_TABLE;
        LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE, table_name.length(), table_name.ptr(), database_name.length(), database_name.ptr());
      } else {
        ret = OB_TABLE_NOT_EXIST;
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(database_name), to_cstring(table_name));
      }
    } else {
      LOG_WARN("fail to resolve table name", K(ret));
    }
  } else if (OB_FAIL(check_err_log_table(table_name, database_name))) {
    LOG_WARN("check error logging table is not meetting requirement", K(ret), K(table_name), K(database_name));
  }
  LOG_DEBUG("after resolver table_name", K(table_name), K(database_id), K(database_name),
      K(is_db_explicit), K(use_sys_tenant));
  return ret;
}

int ObDelUpdResolver::resolve_err_log_reject(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  const ParseNode *limit_or_unlimited_node = NULL;
  CK (OB_NOT_NULL(del_upd_stmt));
  if (OB_ISNULL(limit_or_unlimited_node = node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null", K(ret));
  } else if (T_SIZE_UNLIMITED == limit_or_unlimited_node->type_) {
    // -1 is special for reject limit, -1 sysmbol unlimited
    del_upd_stmt->set_err_log_reject_limit(-1);
  } else {
    if (limit_or_unlimited_node->value_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reject limit should >= 0", K(ret));
    } else {
      del_upd_stmt->set_err_log_reject_limit(limit_or_unlimited_node->value_);
    }
  }
  return ret;
}

int ObDelUpdResolver::resolve_returning(const ParseNode *parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(parse_tree)) {
    ObDelUpdStmt *del_up_stmt = get_del_upd_stmt();
    ObItemType item_type;
    int64_t aggr_cnt = 0;
    ObSEArray<ObDmlTableInfo*, 1> dml_table_infos;

    CK (OB_NOT_NULL(del_up_stmt));
    CK (OB_LIKELY(T_RETURNING == parse_tree->type_));
    CK (OB_LIKELY(2 == parse_tree->num_child_));
    CK (OB_NOT_NULL(parse_tree->children_[0]));
    CK (OB_LIKELY(T_PROJECT_LIST == parse_tree->children_[0]->type_));
    CK (OB_ISNULL(parse_tree->children_[1]) || OB_LIKELY(T_INTO_VARIABLES == parse_tree->children_[1]->type_));

    if (OB_SUCC(ret)) {
      current_scope_ = T_FIELD_LIST_SCOPE;
      expr_resv_ctx_.set_new_scope();
      const ParseNode *returning_exprs = parse_tree->children_[0];
      const ParseNode *returning_intos = parse_tree->children_[1];
      uint64_t base_table_id = OB_INVALID_ID;
      if (OB_FAIL(del_up_stmt->get_dml_table_infos(dml_table_infos))) {
        LOG_WARN("failed to get dml table infos", K(ret));
      } else if (OB_UNLIKELY(dml_table_infos.count() != 1) || OB_ISNULL(dml_table_infos.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected dml table infos", K(ret), K(dml_table_infos));
      } else if (OB_UNLIKELY(common::OB_INVALID_ID == (base_table_id = dml_table_infos.at(0)->ref_table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("base_table_id is invalid", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < returning_exprs->num_child_; i++) {
        ObRawExpr *expr = NULL;
        CK (OB_NOT_NULL(returning_exprs->children_[i]));
        CK (OB_NOT_NULL(returning_exprs->children_[i]->children_[0]));
        OZ (resolve_sql_expr(*(returning_exprs->children_[i]->children_[0]), expr));
        CK (OB_NOT_NULL(expr));
        OZ (expr->formalize(session_info_));
        // only in static_typing_engine
        // create table t1(c1 int primary key, c2 clob, c3 blob);
        // insert into t1 values(1, 'ddd', empty_blob()) returning c1, concat(c2, '1'), c2, c3;
        // for this insert sql, concat(c2, '1') is a fake lob_locator, c2, c3 is a useful lob_locator
        if (OB_SUCC(ret) &&
            expr->has_flag(IS_COLUMN) && expr->is_column_ref_expr() &&
            (StmtType::T_INSERT == del_up_stmt->get_stmt_type() ||
                StmtType::T_UPDATE == del_up_stmt->get_stmt_type())) {
          ObColumnRefRawExpr *ref_expr = static_cast<ObColumnRefRawExpr *>(expr);
          if (ObObjType::ObLobType == ref_expr->get_data_type() && ref_expr->get_table_id() == base_table_id) {
            ObSysFunRawExpr *lob_expr = NULL;
            if (OB_FAIL(build_returning_lob_expr(ref_expr, lob_expr))) {
              LOG_WARN("fail to build returning lob expr", K(ret));
            }  else if (OB_ISNULL(lob_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("lob_expr is null", K(ret));
            } else {
              expr = lob_expr;
              LOG_DEBUG("build returning lob expr", KPC(expr), KPC(ref_expr), KPC(lob_expr));
            }
          }
        }
        if (OB_SUCC(ret)
            && (ob_is_user_defined_sql_type(expr->get_data_type())
                || ob_is_xml_pl_type(expr->get_data_type(), expr->get_udt_id()))) {
          // ORA-22816 returning clause is currently not object type columns
          // but this is success in ORA: execute immediate 'insert into t1 values(4,5) returning udt1(c1, c2) into :a' using out a;
          // xmltype is not allowed: execute immediate 'insert into t2 values(:b) returning xmltype(c1) into :a' using b, out a;
          ret = OB_ERR_RETURNING_CLAUSE;
          LOG_WARN("RETURNING clause is currently not supported for object type",
                   K(ret), K(expr->get_data_type()));
        }

        if (OB_SUCC(ret)) {
          ObString expr_name;
          expr_name.assign_ptr(returning_exprs->children_[i]->str_value_, static_cast<int32_t>(returning_exprs->children_[i]->str_len_));
          del_up_stmt->add_value_to_returning_exprs(expr);
          del_up_stmt->add_value_to_returning_strs(expr_name);
          item_type = expr->get_expr_type();
          if (IS_AGGR_FUN(item_type)) {
            aggr_cnt++;
          }
        }
      }
      ObArray<ObString> user_vars;
      if (OB_SUCC(ret) && OB_NOT_NULL(returning_intos)) {
        OZ (resolve_into_variables(returning_intos, user_vars,
            del_up_stmt->get_returning_into_exprs(), NULL /* select_stmt */));
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(check_returning_validity())) {
          LOG_WARN("check returning validity failed", K(ret));
        }
      }
      expr_resv_ctx_.revert_scope();
    }
  }
  return ret;
}

int ObDelUpdResolver::check_returning_validity()
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = NULL;
  bool has_single_set_expr = false;
  bool has_simple_expr = false;
  bool has_sequence = false;
  if (OB_ISNULL(del_upd_stmt = get_del_upd_stmt()) ||
      OB_UNLIKELY(!get_stmt()->is_insert_stmt() &&
                  !get_stmt()->is_update_stmt() &&
                  !get_stmt()->is_delete_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < del_upd_stmt->get_returning_exprs().count(); ++i) {
      bool cnt_simple_expr = false;
      bool cnt_single_set_expr = false;
      if (OB_FAIL(check_returinng_expr(del_upd_stmt->get_returning_exprs().at(i),
                                       cnt_single_set_expr,
                                       cnt_simple_expr,
                                       has_sequence))) {
        LOG_WARN("failed to check returning expr", K(ret));
      } else if (has_sequence) {
        ret = OB_ERR_SEQ_NOT_ALLOWED_HERE;
        LOG_WARN("sequence number not allowed here in the returning clause", K(ret));
      } else {
        has_single_set_expr = (cnt_single_set_expr || has_single_set_expr);
        has_simple_expr = (cnt_simple_expr || has_simple_expr);
        if (has_simple_expr && has_single_set_expr) {
          ret = OB_ERR_GROUP_FUNC_NOT_ALLOWED;
          LOG_WARN("cannot combine simple expressions and single-set aggregate expressions",
                   K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < del_upd_stmt->get_returning_aggr_item_size(); ++i) {
      ObAggFunRawExpr *aggr = NULL;
      if (OB_ISNULL(aggr = del_upd_stmt->get_returning_aggr_items().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("returing aggregate expr is null", K(ret));
      } else if (aggr->is_param_distinct()) {
        ret = OB_ERR_GROUP_FUNC_NOT_ALLOWED;
        LOG_WARN("Single-set aggregate functions cannot include the DISTINCT keyword", K(ret));
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::check_returinng_expr(ObRawExpr *expr,
                                           bool &has_single_set_expr,
                                           bool &has_simple_expr,
                                           bool &has_sequenece)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_aggr_expr()) {
    has_single_set_expr = true;
  } else if (expr->is_column_ref_expr()) {
    has_simple_expr = true;
  } else if (expr->get_expr_type() == T_FUN_SYS_SEQ_NEXTVAL) {
    has_sequenece = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    bool cnt_simple_expr = false;
    if (OB_FAIL(check_returinng_expr(expr->get_param_expr(i),
                                     has_single_set_expr,
                                     cnt_simple_expr,
                                     has_sequenece))) {
      LOG_WARN("failed to check returning expr", K(ret));
    } else if (!expr->is_aggr_expr() && cnt_simple_expr) {
      has_simple_expr = true;
    }
  }
  return ret;
}

int ObDelUpdResolver::gen_rowid_expr_for_returning(ObSysFunRawExpr *&rowid_expr)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  ObSEArray<ObRawExpr*, 4> rowkey_exprs;
  ObSEArray<ObDmlTableInfo*, 1> tables_info;
  if (OB_ISNULL(allocator_) || OB_ISNULL(session_info_) ||
      OB_ISNULL(schema_checker_) || OB_ISNULL(del_upd_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(allocator_), K(session_info_), K(del_upd_stmt), K(ret));
  } else if (OB_FAIL(del_upd_stmt->get_dml_table_infos(tables_info))) {
    LOG_WARN("failed to get dml table info", K(ret));
  } else if (OB_UNLIKELY(1 != tables_info.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(tables_info.count()), K(ret));
  } else if (OB_ISNULL(tables_info.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                       tables_info.at(0)->ref_table_id_, table_schema))) {
    LOG_WARN("get table schema failed", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_exprs_serialize_to_rowid(del_upd_stmt,
                                                  table_schema,
                                                  tables_info.at(0)->column_exprs_,
                                                  rowkey_exprs))) {
    LOG_WARN("generated rowkey exprs failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_rowid_expr(del_upd_stmt,
                                                      *params_.expr_factory_,
                                                      *allocator_,
                                                      *(session_info_),
                                                      *table_schema,
                                                      tables_info.at(0)->table_id_,
                                                      rowkey_exprs,
                                                      rowid_expr))) {
    LOG_WARN("build rowid_expr failed", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObDelUpdResolver::get_exprs_serialize_to_rowid(ObDMLStmt *stmt,
                                                   const ObTableSchema *&tbl_schema,
                                                   const ObIArray<ObColumnRefRawExpr*> &all_cols,
                                                   ObIArray<ObRawExpr*> &rowkey_cols)
{
  int ret = OB_SUCCESS;
  rowkey_cols.reuse();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObSEArray<uint64_t, 4> col_ids;
    int64_t rowkey_cnt;
    OZ(tbl_schema->get_column_ids_serialize_to_rowid(col_ids, rowkey_cnt));
    CK(col_ids.count() > 0);
    for (int64_t i = 0; i < all_cols.count(); ++i) {
      int64_t idx;
      ObColumnRefRawExpr* base_col_expr = all_cols.at(i);
      if (OB_FAIL(ObTransformUtils::get_base_column(stmt, base_col_expr))) {
        LOG_WARN("get base column failed", K(ret));
      } else if (OB_ISNULL(base_col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("base_col_expr is null", K(ret));
      } else if (has_exist_in_array(col_ids, base_col_expr->get_column_id(), &idx)) {
        if (OB_FAIL(rowkey_cols.push_back((ObRawExpr*)all_cols.at(i)))) {
          LOG_WARN("rowkey cols push back failed", K(ret), K(i), K(all_cols));
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::build_returning_lob_expr(ObColumnRefRawExpr *ref_expr, ObSysFunRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *table_id_expr = NULL;
  ObConstRawExpr *column_id_expr = NULL;
  ObSysFunRawExpr *rowid_expr = NULL;
  CK(OB_NOT_NULL(session_info_));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObRawExprUtils::build_returning_lob_expr(*params_.expr_factory_,
                                                              *session_info_,
                                                              ref_expr,
                                                              expr))) {
    LOG_WARN("fail to build_returning_lob_expr", K(ret), KPC(ref_expr));
  } else if (OB_FAIL(gen_rowid_expr_for_returning(rowid_expr))) {
    LOG_WARN("fail to gen rowid expr for returning", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob expr is null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*params_.expr_factory_,
                                                          ObUInt64Type,
                                                          ref_expr->get_table_id(),
                                                          table_id_expr))) {
    LOG_WARN("fail to build const int expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*params_.expr_factory_,
                                                          ObUInt64Type,
                                                          ref_expr->get_column_id(),
                                                          column_id_expr))) {
    LOG_WARN("fail to build const int expr", K(ret));
  } else if (OB_FAIL(expr->add_param_expr(table_id_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(expr->add_param_expr(column_id_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(expr->add_param_expr(rowid_expr))) {
    LOG_WARN("fail to add param expr", K(ret));
  } else if (OB_FAIL(expr->formalize(session_info_))) {
    LOG_WARN("formalize fail", K(expr->get_param_count()));
  }
  return ret;
}

bool ObDelUpdResolver::need_all_columns(const ObTableSchema &table_schema, int64_t binlog_row_image)
{
  return (table_schema.is_heap_table() ||
          table_schema.get_foreign_key_infos().count() > 0 ||
          table_schema.get_trigger_list().count() > 0 ||
          table_schema.has_check_constraint() ||
          table_schema.has_generated_and_partkey_column() ||
          binlog_row_image == ObBinlogRowImage::FULL);
}

int ObDelUpdResolver::add_all_columns_to_stmt(const TableItem &table_item,
                                              ObIArray<ObColumnRefRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const TableItem& base_table_item = table_item.get_base_table_item();
  if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params_.session_info_ is null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(params_.session_info_->get_effective_tenant_id(),
                                                       base_table_item.ref_id_,
                                                       table_schema,
                                                       base_table_item.is_link_table()))) {
    LOG_WARN("not find table schema", K(ret), K(base_table_item));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_schema), K(ret));
  } else {
    ObTableSchema::const_column_iterator iter = table_schema->column_begin();
    ObTableSchema::const_column_iterator end = table_schema->column_end();
    for (; OB_SUCC(ret) && iter != end; ++iter) {
      const ObColumnSchemaV2 *column = *iter;
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column schema", K(column));
      } else if (OB_FAIL(add_column_to_stmt(table_item, *column, column_exprs))) {
        LOG_WARN("add column item to stmt failed", K(ret));
      }
    } //end for
  }
  return ret;
}

int ObDelUpdResolver::add_all_lob_columns_to_stmt(const TableItem &table_item,
                                                  ObIArray<ObColumnRefRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const TableItem& base_table_item = table_item.get_base_table_item();
  if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params_.session_info_ is null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(params_.session_info_->get_effective_tenant_id(),
                                                       base_table_item.ref_id_,
                                                       table_schema,
                                                       base_table_item.is_link_table()))) {
    LOG_WARN("not find table schema", K(ret), K(base_table_item));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_schema), K(ret));
  } else {
    ObTableSchema::const_column_iterator iter = table_schema->column_begin();
    ObTableSchema::const_column_iterator end = table_schema->column_end();
    for (; OB_SUCC(ret) && iter != end; ++iter) {
      const ObColumnSchemaV2 *column = *iter;
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column schema", K(column));
      } else if (!column->get_meta_type().is_lob_storage()) {
        // do nothing
      } else if (OB_FAIL(add_column_to_stmt(table_item, *column, column_exprs))) {
        LOG_WARN("add column item to stmt failed", K(ret));
      }
    } //end for
  }
  return ret;
}

int ObDelUpdResolver::add_all_columns_to_stmt_for_trigger(const TableItem &table_item,
                                                          ObIArray<ObColumnRefRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *ref_stmt = table_item.ref_query_;
  if (OB_ISNULL(ref_stmt) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ref_stmt->get_select_item_size(); i++) {
      ObColumnRefRawExpr *col_expr = get_stmt()->get_column_expr_by_id(table_item.table_id_,
                                                                       i + OB_APP_MIN_COLUMN_ID);
      if (OB_ISNULL(col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(add_var_to_array_no_dup(column_exprs, col_expr))) {
        LOG_WARN("failed to append array", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObDelUpdResolver::add_all_rowkey_columns_to_stmt(const TableItem &table_item,
                                                     ObIArray<ObColumnRefRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const ObColumnSchemaV2 *column_schema = NULL;
  uint64_t rowkey_column_id = 0;
  const TableItem &base_table_item = table_item.get_base_table_item();
  ObDelUpdStmt *stmt = get_del_upd_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt));
  } else if (stmt->has_instead_of_trigger()) {
    // do nothing, instead of trigger doesn't have rowkey
  } else if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params_.session_info_ is null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(params_.session_info_->get_effective_tenant_id(),
                                                       base_table_item.ref_id_,
                                                       table_schema,
                                                       base_table_item.is_link_table()))) {
    LOG_WARN("table schema not found", K(base_table_item));
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid table schema", K(table_item));
  } else {
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
        LOG_WARN("get rowkey info failed", K(ret), K(i), K(rowkey_info));
      } else if (OB_FAIL(get_column_schema(base_table_item.ref_id_, rowkey_column_id, column_schema, true, base_table_item.is_link_table()))) {
        LOG_WARN("get column schema failed", K(rowkey_column_id));
      } else if (OB_FAIL(add_column_to_stmt(table_item, *column_schema, column_exprs))) {
        LOG_WARN("add column to stmt failed", K(ret), K(table_item));
      }
    }
  }
  return ret;
}

//for ObDelUpdStmt
// add column's related columns in index to stmt
// if column_id is OB_INVALID_ID, all indexes' columns would be added to stmt
// @param[in] table_id            table id
// @param[in] column_id           column id
int ObDelUpdResolver::add_index_related_columns_to_stmt(const TableItem &table_item,
                                                        const uint64_t column_id,
                                                        ObIArray<ObColumnRefRawExpr *> &column_items)
{
  int ret = OB_SUCCESS;
  ColumnItem *col_item = NULL;
  ObDelUpdStmt *del_upd_stmt = dynamic_cast<ObDelUpdStmt*>(get_stmt());
  if (OB_ISNULL(del_upd_stmt) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("This function only for class inherited ObDelUpdStmt", K(del_upd_stmt), K_(schema_checker));
  } else if (OB_ISNULL(col_item = del_upd_stmt->get_column_item_by_id(table_item.table_id_, column_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column item not found", K(ret), K(table_item), K(column_id));
  } else if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params_.session_info_ is null", K(ret));
  } else {
    uint64_t base_table_id = table_item.get_base_table_item().ref_id_;
    uint64_t base_column_id = (table_item.is_generated_table() || table_item.is_temp_table())
        ? col_item->base_cid_
        : col_item->column_id_;
    const ObTableSchema *table_schema = NULL;
    const ObTableSchema *index_schema = NULL;
    const ObColumnSchemaV2 *column_schema = NULL;
    uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();

    if (OB_FAIL(schema_checker_->get_table_schema(tenant_id, base_table_id, table_schema,
                                                  table_item.get_base_table_item().is_link_table()))) {
      LOG_WARN("invalid table id", K(base_table_id));
    } else if (NULL == (column_schema = table_schema->get_column_schema(base_column_id))) {
      LOG_WARN("get column schema failed", K(ret), K(base_table_id), K(base_column_id));
    } else if (column_schema->is_rowkey_column()) {
      //if the column id is rowkey, wo need to add all columns in table schema to columns
      if (OB_FAIL(add_all_columns_to_stmt(table_item, column_items))) {
        LOG_WARN("add all columns to stmt failed", K(ret));
      } else {
        LOG_DEBUG("add all column to stmt due to the update column is primary key");
      }
    } else {
      uint64_t index_tids[OB_MAX_INDEX_PER_TABLE];
      int64_t index_count = OB_MAX_INDEX_PER_TABLE;
      // get all the indexes
      if (OB_FAIL(schema_checker_->get_can_write_index_array(tenant_id,
                                                             base_table_id,
                                                             index_tids,
                                                             index_count))) {
        LOG_WARN("fail to get index", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < index_count; ++i) {
          uint64_t index_id = index_tids[i];
          // get index schema
          if (OB_FAIL(schema_checker_->get_table_schema(tenant_id, index_id, index_schema))) {
            LOG_WARN("get index schema failed", K(index_id));
          } else {
            //only add the column items in the index schema which contain the column_id
            if (NULL != (column_schema = index_schema->get_column_schema(base_column_id))) {
              if (OB_FAIL(add_all_index_rowkey_to_stmt(table_item, index_schema, column_items))) {
                LOG_WARN("add all index rowkey to stmt failed", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::add_all_index_rowkey_to_stmt(const TableItem &table_item,
                                                   common::ObIArray<ObColumnRefRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *index_schema = NULL;
  uint64_t idx_tids[OB_MAX_INDEX_PER_TABLE];
  int64_t idx_count = OB_MAX_INDEX_PER_TABLE;
  if (OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params_.session_info_ is null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_can_write_index_array(
              params_.session_info_->get_effective_tenant_id(),
              table_item.get_base_table_item().ref_id_, idx_tids, idx_count))) {
    LOG_WARN("failed to get all index", K(ret));
  } else {
    // Secondly, for each index, all all its rowkey
    for (int64_t i = 0; OB_SUCC(ret) && i < idx_count; ++i) {
      if (OB_FAIL(schema_checker_->get_table_schema(params_.session_info_->get_effective_tenant_id(),
                                                    idx_tids[i], index_schema))) {
        LOG_WARN("get index schema failed", "index_id", idx_tids[i], K(ret));
      } else if (OB_FAIL(add_all_index_rowkey_to_stmt(table_item, index_schema, column_exprs))) {
        LOG_WARN("add all index rowkey column to stmt failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::add_all_index_rowkey_to_stmt(const TableItem &table_item,
                                                   const ObTableSchema *index_schema,
                                                   ObIArray<ObColumnRefRawExpr *> &column_items)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt = get_stmt();
  uint64_t rowkey_column_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *column_schema = NULL;
  if (NULL == index_schema || NULL == stmt || !index_schema->is_index_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(index_schema), K(stmt));
  } else {
    uint64_t base_table_id = table_item.get_base_table_item().ref_id_;
    ObTableSchema::const_column_iterator b = index_schema->column_begin();
    ObTableSchema::const_column_iterator e = index_schema->column_end();
    for (; OB_SUCC(ret) && b != e; ++b) {
      if (NULL == (*b)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to to get column schema", K(*b));
      } else {
        rowkey_column_id = (*b)->get_column_id();
        if ((*b)->is_shadow_column()) {
          continue;
        }
        if (OB_FAIL(get_column_schema(base_table_id, rowkey_column_id, column_schema, true, table_item.get_base_table_item().is_link_table()))) {
          LOG_WARN("get column schema failed", K(ret), K(base_table_id), K(rowkey_column_id));
        } else if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get column schema", K(ret), K(base_table_id), K(rowkey_column_id));
        } else if (OB_FAIL(add_column_to_stmt(table_item, *column_schema, column_items))) {
          LOG_WARN("add column to stmt failed",  K(ret), K(table_item), K(*column_schema));
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::add_all_partition_key_columns_to_stmt(const TableItem &table_item,
                                                            ObIArray<ObColumnRefRawExpr*> &column_exprs,
                                                            ObDMLStmt *stmt /*= NULL*/)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const ObColumnSchemaV2 *column_schema = NULL;
  uint64_t rowkey_column_id = 0;
  uint64_t base_table_id = table_item.get_base_table_item().ref_id_;
  stmt = (NULL == stmt) ? get_stmt() : stmt;
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get stmt fail", K(ret), K(stmt), K(schema_checker_));
  } else if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params_.session_info_ is null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(params_.session_info_->get_effective_tenant_id(), base_table_id, table_schema, table_item.is_link_table()))) {
    LOG_WARN("table schema not found", K(base_table_id), K(table_item));
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid table schema", K(table_item));
  } else if (table_schema->is_heap_table()) {
    const ObPartitionKeyInfo &partition_keys = table_schema->get_partition_key_info();
    const ObPartitionKeyInfo &subpartition_keys = table_schema->get_subpartition_key_info();
    ObSEArray<uint64_t, 2> column_ids;
    if (partition_keys.is_valid() && OB_FAIL(partition_keys.get_column_ids(column_ids))) {
      LOG_WARN("fail to get column ids from partition keys", K(ret));
    } else if (subpartition_keys.is_valid() && OB_FAIL(subpartition_keys.get_column_ids(column_ids))) {
      LOG_WARN("fail to get column ids from subpartition keys", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        uint64_t rowkey_column_id = column_ids.at(i);
        if (OB_FAIL(get_column_schema(base_table_id,
                                      rowkey_column_id,
                                      column_schema,
                                      true,
                                      table_item.is_link_table()))) {
          LOG_WARN("get column schema failed", K(base_table_id), K(rowkey_column_id));
        } else if (OB_FAIL(add_column_to_stmt(table_item, *column_schema, column_exprs, stmt))) {
          LOG_WARN("add column to stmt failed", K(ret), K(table_item), KPC(column_schema));
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::uv_check_key_preserved(const TableItem &table_item, bool &key_preserved)
{
  int ret = OB_SUCCESS;
  if (table_item.is_generated_table() && table_item.get_base_table_item().is_link_table()) {
    // skip check link table key preserved, do not check it, remote database will report error if the actual key_preserved is false.
    key_preserved = true;
  } else if (table_item.is_generated_table() || table_item.is_temp_table()) {
    key_preserved = false;
    if (NULL == table_item.ref_query_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref query or view base table item is NULL", K(ret));
    } else {
      ObSEArray<ObColumnRefRawExpr *, 8> pk_cols;
      ObSEArray<ObRawExpr *, 8> pk_cols_raw;
      bool unique = false;
      // rowkey already add to stmt, it is safe to add again.
      if (OB_FAIL(add_all_rowkey_columns_to_stmt(table_item, pk_cols))) {
        LOG_WARN("get all rowkey exprs failed", K(ret));
      // if table is heap table, we need to add partition keys because they're no longer in rowkey
      } else if (OB_FAIL(add_all_partition_key_columns_to_stmt(table_item, pk_cols))) {
        LOG_WARN("fail to add partition keys", K(ret));
      } else {
        FOREACH_CNT_X(e, pk_cols, OB_SUCC(ret)) {
          int64_t idx = (*e)->get_column_id() - OB_APP_MIN_COLUMN_ID;
          if (idx < 0 || idx >= table_item.ref_query_->get_select_item_size()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid select item index", K(ret), K(*e));
          } else if (OB_FAIL(pk_cols_raw.push_back(
              table_item.ref_query_->get_select_item(idx).expr_))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(table_item.ref_query_, session_info_,
                                      schema_checker_, pk_cols_raw, true /* strict */, unique))) {
        LOG_WARN("check stmt unique failed", K(ret));
      } else {
        key_preserved = unique;
      }
    }
  }
  return ret;
}

//检查user_view上是否有对应dml事件触发且状态为enabled的instead of trigger
int ObDelUpdResolver::has_need_fired_trigger_on_view(const TableItem* view_item, bool &has)
{
  int ret = OB_SUCCESS;
  has = false;
  const ObTableSchema *view_schema = NULL;
  ObSchemaGetterGuard *schema_guard = NULL;
  uint64_t view_id = OB_INVALID_ID;
  CK (OB_NOT_NULL(view_item));
  CK (OB_NOT_NULL(schema_checker_));
  CK (OB_NOT_NULL(schema_guard = schema_checker_->get_schema_guard()));
  OX (view_id = view_item->ref_id_);
  if (OB_SUCC(ret) && !view_item->alias_name_.empty()) {
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    CK (OB_NOT_NULL(schema_checker_));
    CK (OB_NOT_NULL(schema_guard = schema_checker_->get_schema_guard()))
    OZ (schema_guard->get_table_id(tenant_id, view_item->database_name_,
                                   view_item->table_name_, false /*is_index*/,
                                   ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, view_id));
  }
  OZ (schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(), view_id, view_schema), view_id);
  CK (OB_NOT_NULL(view_schema));
  if (OB_SUCC(ret) && view_schema->is_user_view()) {
    const uint64_t tenant_id = view_schema->get_tenant_id();
    const ObIArray<uint64_t> &tg_list = view_schema->get_trigger_list();
    const ObTriggerInfo *tg_info = NULL;
    uint64_t tg_id = OB_INVALID_ID;
    uint64_t dml_event = 0;
    switch (stmt_->get_stmt_type())
    {
    case stmt::T_INSERT:
      dml_event = ObTriggerEvents::get_insert_event();
      break;
    case stmt::T_UPDATE:
      dml_event = ObTriggerEvents::get_update_event();
      break;
    case stmt::T_DELETE:
      dml_event = ObTriggerEvents::get_delete_event();
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt type is error", K(stmt_->get_stmt_type()), K(ret));
      break;
    }
    for (int64_t i = 0; OB_SUCC(ret) && !has && i < tg_list.count(); i++) {
      OX (tg_id = tg_list.at(i));
      OZ (schema_guard->get_trigger_info(tenant_id, tg_id, tg_info));
      OV (OB_NOT_NULL(tg_info));
      OX (has = (tg_info->is_enable() && tg_info->has_event(dml_event)));
    }
    if (OB_SUCC(ret) && has) {
      CK (stmt_->is_support_instead_of_trigger_stmt());
      if (OB_SUCC(ret)) {
        ObDelUpdStmt *del_upd_stmt = static_cast<ObDelUpdStmt *>(stmt_);
        if (del_upd_stmt->is_returning()) {
          ret = OB_ERR_RETURNING_CLAUSE;
          LOG_WARN("RETURNING clause is currently not supported for INSTEAD OF Triggers", K(ret));
        }
      }
    }
  }
  return ret;
}

// generated column、identity column
int ObDelUpdResolver::view_pullup_special_column_exprs()
{
  int ret = OB_SUCCESS;
  ObDMLStmt *dml_stmt = get_stmt();
  if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(dml_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param factory or dml stmt is null", K(ret), K(params_.expr_factory_), K(dml_stmt));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dml_stmt->get_table_size(); i++) {
    ObSelectStmt *sel_stmt = NULL;
    const TableItem *t = NULL;
    if (OB_ISNULL(t = dml_stmt->get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(i));
    } else if ((t->is_generated_table() || t->is_temp_table())
               && (OB_NOT_NULL(t->view_base_item_) || dml_stmt->has_instead_of_trigger())) {
      if (dml_stmt->has_instead_of_trigger()) {
        sel_stmt = t->ref_query_;
      } else {
        while (NULL != t && (t->is_generated_table() || t->is_temp_table())) {
          sel_stmt = t->ref_query_;
          t = t->view_base_item_;
        }
      }
      if (OB_ISNULL(sel_stmt) || OB_ISNULL(t)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref_query_ is null", K(ret));
      }
      for(int j = 0; OB_SUCC(ret) && j < dml_stmt->get_column_size(); j++) {
        ColumnItem *view_column_item = dml_stmt->get_column_item(j);
        if (OB_ISNULL(view_column_item) || OB_ISNULL(view_column_item->expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("view column item not exists in stmt or expr_ is null", K(ret),
                                                                           K(view_column_item));
        } else {
          ColumnItem *basic_column_item = NULL;
          if (dml_stmt->has_instead_of_trigger()) {
            basic_column_item = sel_stmt->get_column_item_by_id(view_column_item->base_tid_,
                                                                view_column_item->base_cid_);
          } else if (view_column_item->table_id_ == dml_stmt->get_table_item(i)->table_id_) {
            basic_column_item = sel_stmt->get_column_item_by_id(t->table_id_,
                                                                view_column_item->base_cid_);
          }
          if (OB_NOT_NULL(basic_column_item)) {
            if (OB_ISNULL(basic_column_item->expr_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("basic column item  expr_ is null", K(ret));
            } else if (basic_column_item->expr_->is_generated_column()) {
              ObRawExpr *ref_expr = NULL;
              uint64_t tid = dml_stmt->has_instead_of_trigger() ?
                             view_column_item->base_tid_ : t->ref_id_;
              if (OB_FAIL(copy_schema_expr(*params_.expr_factory_,
                                           basic_column_item->expr_->get_dependant_expr(),
                                           ref_expr))) {
                LOG_WARN("failed to copy dependant expr", K(ret));
              } else if (OB_FAIL(view_pullup_column_ref_exprs_recursively(ref_expr,
                                                                          tid, dml_stmt))){
                LOG_WARN("view pull up generated column exprs recursively failed", K(ret));
              } else {
                view_column_item->expr_->set_dependant_expr(ref_expr);
                view_column_item->expr_->set_column_flags(
                                          basic_column_item->expr_->get_column_flags());
              }
            } else if (basic_column_item->expr_->is_identity_column()) {
              view_column_item->expr_->set_column_flags(
                                        basic_column_item->expr_->get_column_flags());
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::view_pullup_part_exprs()
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt = NULL;
  TableItem *table = NULL;
  if (OB_ISNULL(stmt = get_stmt()) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have NULL", K(ret));
  }
  for (int64_t idx = 0; idx < stmt->get_table_size(); idx++) {
    if (OB_ISNULL(table = stmt->get_table_item(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is NULL", K(ret));
    } else if (&table->get_base_table_item() == table) {
      // skip
    } else if (table->is_generated_table() || table->is_temp_table()) {
      ObSelectStmt *sel_stmt = NULL;
      const TableItem *t = table;
      ObSEArray<ObRawExpr *, 4> view_columns;
      ObSEArray<ObRawExpr *, 4> base_columns;
      ObRawExprCopier copier(*params_.expr_factory_);
      while (NULL != t && (t->is_generated_table() || t->is_temp_table())) {
        sel_stmt = t->ref_query_;
        t = t->view_base_item_;
      }
      if (OB_ISNULL(sel_stmt) || OB_ISNULL(t)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get select stmt for base table item failed", K(ret));
      } else if (OB_FAIL(get_pullup_column_map(*stmt, *sel_stmt, table->table_id_, view_columns, base_columns))) {
        // link.zt seems to have problem, base_tid is better to be refined as table_id
        LOG_WARN("failed to get pullup column map", K(ret));
      } else if (OB_FAIL(copier.add_replaced_expr(base_columns, view_columns))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_part_exprs().count(); ++i) {
        ObDMLStmt::PartExprItem pei = sel_stmt->get_part_exprs().at(i);
        if (pei.table_id_ != table->get_base_table_item().table_id_) {
          continue;
        } else if (OB_FAIL(copier.copy(pei.part_expr_, pei.part_expr_))) {
          LOG_WARN("failed to copy part expr", K(ret));
        } else if (OB_FAIL(copier.copy(pei.subpart_expr_, pei.subpart_expr_))) {
          LOG_WARN("failed to copy subpart expr", K(ret));
        } else if (OB_FAIL(stmt->get_part_exprs().push_back(pei))) {
          LOG_WARN("failed to push back pullup partition expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::expand_record_to_columns(const ParseNode &record_node,
                                               ObIArray<ObRawExpr *> &value_list)
{
  int ret = OB_SUCCESS;
  ObRawExpr *row_expr = NULL;
  CK(T_OBJ_ACCESS_REF == record_node.type_,
     OB_NOT_NULL(params_.secondary_namespace_),
     OB_NOT_NULL(params_.allocator_),
     OB_NOT_NULL(params_.expr_factory_));
  OZ (pl::ObPLResolver::resolve_raw_expr(record_node,
                                         *params_.allocator_,
                                         *params_.expr_factory_,
                                         *params_.secondary_namespace_,
                                         params_.is_prepare_protocol_,
                                         row_expr));
  if (OB_SUCC(ret) && NULL != row_expr) {
    if (row_expr->is_obj_access_expr()) {
      ObObjAccessRawExpr &access_expr = *static_cast<ObObjAccessRawExpr*>(row_expr);
      pl::ObPLDataType composite_type;
      OZ (access_expr.get_final_type(composite_type));
      if (!composite_type.is_record_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support to expand", K(composite_type), K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "SET ROW isn't record type");
      } else {
        const pl::ObRecordType *record_type = NULL;
        const pl::ObUserDefinedType *user_type = NULL;
        ParseNode *column_node = NULL;
        const ParseNode *member_node = &record_node;
        int64_t multi_level_count = 0;
        while (NULL != member_node && member_node->num_child_ > 1 && NULL != member_node->children_[1]) {
          if (NULL != member_node->children_[0] &&
              T_IDENT == member_node->children_[0]->type_ &&
              T_OBJ_ACCESS_REF == member_node->children_[1]->type_) {
            // do nothing
          } else {
            multi_level_count++;
          }
          member_node = member_node->children_[1];
        }
        if (multi_level_count > 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support to expand", K(composite_type), K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "mutil level record reference");
        } else if (OB_ISNULL(column_node = new_terminal_node(allocator_, T_IDENT))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("make db name T_IDENT node failed", K(ret));
        } else if (OB_ISNULL(member_node->children_[1] = new_non_terminal_node(allocator_,
                                                                    T_OBJ_ACCESS_REF,
                                                                    2,
                                                                    column_node,
                                                                    nullptr))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("make db name T_IDENT node failed", K(ret));
        } else { /*do nothing*/ }

        OZ (params_.secondary_namespace_->get_user_type(composite_type.get_user_type_id(),
                                                        user_type, NULL/*no use*/));
        CK (OB_NOT_NULL(user_type));
        OX (record_type = static_cast<const pl::ObRecordType*>(user_type));

        for (int64_t i = 0; OB_SUCC(ret) && i < record_type->get_member_count(); ++i) {
          ObRawExpr* column_expr = NULL;
          column_node->str_value_ = parse_strndup(record_type->get_record_member_name(i)->ptr(),
                                  record_type->get_record_member_name(i)->length(), allocator_);
          column_node->str_len_ = record_type->get_record_member_name(i)->length();
          OZ (resolve_sql_expr(record_node, column_expr));
          OZ (column_expr->formalize(session_info_));
          OZ (value_list.push_back(column_expr));
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support to expand", K(*row_expr), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "SET ROW isn't record type");
    }
  }
  return ret;
}

// input: target table item of dml.
// This function get check constraints from table_item and add into dml_stmt.
int ObDelUpdResolver::resolve_check_constraints(const TableItem* table_item,
                                                ObIArray<ObRawExpr*> &check_exprs)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *dml_stmt = NULL;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(table_item) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_ISNULL(dml_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get stmt null", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                       table_item->get_base_table_item().ref_id_,
                                                       table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_item->get_base_table_item().ref_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get tale schema", K(ret), K(table_schema));
  } else if (OB_FAIL(generate_check_constraint_exprs(table_item, table_schema, check_exprs))) {
    LOG_WARN("fail to add check constraint to stmt", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < check_exprs.count(); ++i) {
      ObRawExpr *&expr = check_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("check constraint expr is null", K(ret));
      } else if (OB_FAIL(view_pullup_column_ref_exprs_recursively(expr,
                                            table_item->get_base_table_item().ref_id_, dml_stmt))) {
        LOG_WARN("view pullup column_ref_exprs recursively failed", K(ret));
      } else if (expr->get_expr_type() == T_FUN_SYS_IS_JSON &&
                 expr->get_param_count() == 5 &&
                 OB_NOT_NULL(expr->get_param_expr(0)) &&
                 OB_NOT_NULL(expr->get_param_expr(2)) &&
                 expr->get_param_expr(0)->get_expr_type() == T_REF_COLUMN &&
                 expr->get_param_expr(2)->get_expr_type() == T_INT) {
        const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr*>(expr->get_param_expr(2));
        ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(expr->get_param_expr(0));
        int is_json_opt = const_expr->get_value().get_int();
        if (is_json_opt == 1) {
          col_expr->set_strict_json_column(IS_JSON_CONSTRAINT_STRICT);
        } else {
          col_expr->set_strict_json_column(IS_JSON_CONSTRAINT_RELAX);
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::resolve_view_check_exprs(uint64_t table_id,
                                               const TableItem* table_item,
                                               const bool cascaded,
                                               common::ObIArray<ObRawExpr*> &check_exprs)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = NULL;
  ObSelectStmt *select_stmt = NULL;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ViewCheckOption check_option = VIEW_CHECK_OPTION_NONE;
  if (OB_ISNULL(table_item) || OB_ISNULL(schema_checker_) || OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_item), K(schema_checker_), K(expr_factory));
  } else if (!table_item->is_generated_table()) {
    // do nothing
  } else if (OB_ISNULL(del_upd_stmt = get_del_upd_stmt()) ||
             OB_ISNULL(select_stmt = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(del_upd_stmt), K(select_stmt));
  } else if (!cascaded && VIEW_CHECK_OPTION_NONE ==
                  (check_option = select_stmt->get_check_option())) {
    if (OB_FAIL(resolve_view_check_exprs(table_id, table_item->view_base_item_, cascaded, check_exprs))) {
      LOG_WARN("resolve view check exprs failed", K(ret));
    }
  } else {
    if (select_stmt->get_condition_size() > 0) {
      ObRawExprCopier copier(*expr_factory);
      ObSEArray<ObRawExpr *, 4> view_columns;
      ObSEArray<ObRawExpr *, 4> base_columns;
      ObSEArray<ObRawExpr *, 4> pullup_conditions;
      // todo link.zt, why the ref_id is used here?
      // may have problem when a table is used twice by the view.
      if (OB_FAIL(get_pullup_column_map(*del_upd_stmt,
                                        *select_stmt,
                                        table_id,
                                        view_columns,
                                        base_columns))) {
        LOG_WARN("failed to get pullup column map", K(ret));
      } else if (OB_FAIL(copier.add_replaced_expr(base_columns, view_columns))) {
        LOG_WARN("failed to add replace pair", K(ret));
      } else if (OB_FAIL(copier.copy_on_replace(select_stmt->get_condition_exprs(),
                                                pullup_conditions))) {
        LOG_WARN("failed to copy condition expr as view check exprs", K(ret));
      } else if (OB_FAIL(append(check_exprs, pullup_conditions))) {
        LOG_WARN("failed to append pullup conditions", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const bool new_cascaded = cascaded || VIEW_CHECK_OPTION_CASCADED == check_option;
      if (OB_FAIL(resolve_view_check_exprs(table_id, table_item->view_base_item_, new_cascaded, check_exprs))) {
        LOG_WARN("resolve view check exprs failed", K(ret));
      }
    }
  }

  return ret;
}

int ObDelUpdResolver::get_pullup_column_map(ObDMLStmt &stmt,
                                            ObSelectStmt &sel_stmt,
                                            uint64_t table_id,
                                            ObIArray<ObRawExpr *> &view_columns,
                                            ObIArray<ObRawExpr *> &base_columns)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_column_size(); ++i) {
    ColumnItem &parent_column = stmt.get_column_items().at(i);
    if (parent_column.table_id_ == table_id) {
      for (int64_t j = 0; OB_SUCC(ret) && j < sel_stmt.get_column_size(); ++j) {
        ColumnItem &child_column = sel_stmt.get_column_items().at(j);
        if (child_column.base_tid_ == parent_column.base_tid_ &&
            child_column.base_cid_ == parent_column.base_cid_) {
          if (OB_FAIL(view_columns.push_back(parent_column.expr_)) ||
              OB_FAIL(base_columns.push_back(child_column.expr_))) {
            LOG_WARN("failed to push back column expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::view_pullup_column_ref_exprs_recursively(ObRawExpr *&expr,
                                                               uint64_t base_table_id,
                                                               const ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or expr is null", K(ret), K(stmt), K(expr));
  } else if(expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *ref_expr = static_cast<ObColumnRefRawExpr *>(expr);
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < stmt->get_column_size(); i++) {
      const ColumnItem *column_item = stmt->get_column_item(i);
      if (OB_ISNULL(column_item) || OB_ISNULL(column_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr null");
      } else if (base_table_id == column_item->base_tid_ &&
        ref_expr->get_column_id() == column_item->base_cid_) {
        expr = column_item->expr_;
        found = true;
      }
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      ObRawExpr *&t_expr = expr->get_param_expr(i);
      if (OB_FAIL(SMART_CALL(view_pullup_column_ref_exprs_recursively(t_expr, base_table_id,
                                                                      stmt)))) {
        LOG_WARN("generated column expr pull up failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::generate_column_conv_function(ObInsertTableInfo &table_info)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt* del_upd_stmt = get_del_upd_stmt();
  if (OB_ISNULL(del_upd_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret));
  } else if (OB_FAIL(table_info.column_conv_exprs_.prepare_allocate(table_info.column_exprs_.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    uint64_t table_id = table_info.table_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_info.column_exprs_.count(); i++) {
      uint64_t column_id = OB_INVALID_ID;
      ColumnItem *column_item = NULL;
      ObRawExpr *column_ref = NULL;
      const ObColumnRefRawExpr *tbl_col = NULL;
      if (OB_ISNULL(tbl_col = table_info.column_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table column", K(ret), K(i), K(table_info.column_exprs_));
      } else if (FALSE_IT(column_id = tbl_col->get_column_id())) {
      } else if (OB_ISNULL(column_item = del_upd_stmt->get_column_item_by_id(table_id, column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to find column item", K(ret), K(column_id), K(column_item), K(*tbl_col));
      } else if (OB_FAIL(find_value_desc(table_info, column_id, column_ref))) {
        LOG_WARN("fail to check column is exists", K(ret), K(column_id));
      } else if ((!session_info_->get_ddl_info().is_ddl() || OB_ISNULL(column_ref)) &&
                 ( tbl_col->is_xml_column() || (tbl_col->is_udt_hidden_column()))) {
        if (!tbl_col->is_xml_column()) {
          // do nothing, hidden column with build with xml column together
        } else if (OB_FAIL(build_column_conv_function_for_udt_column(table_info, i, column_ref))) {
          LOG_WARN("failed to build column conv for udt_columns", K(ret));
        }
      } else if (OB_ISNULL(column_ref)) {
        if (OB_FAIL(build_column_conv_function_with_default_expr(table_info, i))) {
          LOG_WARN("build column convert function with default expr failed", K(ret));
        }
      } else if (OB_FAIL(build_column_conv_function_with_value_desc(table_info, i, column_ref))) {
        LOG_WARN("failed to build column conv with value desc", K(ret));
      }
    } //end for
  }
  return ret;
}

int ObDelUpdResolver::find_value_desc(ObInsertTableInfo &table_info,
                                      uint64_t column_id,
                                      ObRawExpr *&column_ref)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  column_ref = NULL;
  bool find = false;
  ObColumnRefRawExpr *expr = nullptr;
  bool value_from_select = false;
  uint64_t table_id = OB_INVALID_ID;

  if (OB_ISNULL(del_upd_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid insert stmt", K(del_upd_stmt));
  } else {
    value_from_select = del_upd_stmt->get_from_item_size() > 0;
  }
  if (!value_from_select) {
    // do nothing
  } else if (OB_UNLIKELY(del_upd_stmt->get_from_item_size() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid from items", K(ret), K(del_upd_stmt->get_from_items()));
  } else {
    table_id = del_upd_stmt->get_from_item(0).table_id_;
  }

  for (int64_t i = 0; OB_SUCC(ret) && !find && i < table_info.values_desc_.count(); i++) {
    if (OB_ISNULL(expr = table_info.values_desc_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get values expr", K(i), K(table_info.values_desc_), K(ret));
    } else if (column_id == expr->get_column_id()) {
      find = true;
      if (value_from_select) {
        column_ref = del_upd_stmt->get_column_expr_by_id(table_id, i + OB_APP_MIN_COLUMN_ID);
      } else {
        column_ref = table_info.values_desc_.at(i);
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::build_column_conv_function_with_value_desc(ObInsertTableInfo& table_info,
                                                                 const int64_t idx,
                                                                 ObRawExpr *column_ref)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  const ObColumnRefRawExpr *tbl_col = table_info.column_exprs_.at(idx);
  uint64_t table_id = table_info.table_id_;
  if (OB_ISNULL(del_upd_stmt) || OB_ISNULL(column_ref) || OB_ISNULL(tbl_col)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null", K(ret));
  } else {
    uint64_t column_id = tbl_col->get_column_id();
    ColumnItem *column_item = NULL;
    bool skip_convert = false;
    bool trigger_exist = false;
    const schema::ObTableSchema *table_schema = nullptr;
    if (OB_ISNULL(column_item = del_upd_stmt->get_column_item_by_id(table_id, column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null column item", K(ret));
    } else if (session_info_->get_ddl_info().is_ddl()) {
      // TODO: yibo do not check each time
      TableItem* table_item = del_upd_stmt->get_table_item_by_id(table_info.table_id_);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table item", K(ret));
      } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                           table_item->ddl_table_id_, table_schema))) {
        LOG_WARN("get table schema failed", K(ret), K(table_item->ddl_table_id_));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", K(ret), K(table_item->ddl_table_id_));
      } else {
        skip_convert = table_schema->is_index_table() ||
                       column_item->column_id_ == OB_HIDDEN_PK_INCREMENT_COLUMN_ID;
        LOG_TRACE("skip convert expr in ddl", K(table_item->ddl_table_id_), K(skip_convert));
      }
    } else {
      const TableItem *table_item = NULL;
      ObSchemaGetterGuard *schema_guard = NULL;
      if (OB_ISNULL(schema_checker_) ||
          OB_ISNULL(schema_guard = schema_checker_->get_schema_guard())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid schema checker", K(schema_checker_));
      } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                           table_info.ref_table_id_,
                                                           table_schema,
                                                           table_info.is_link_table_))) {
        LOG_WARN("fail to get table schema", K(ret), KPC(table_item));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get table schema", KPC(table_item), K(table_schema));
      } else if (OB_FAIL(table_schema->has_before_insert_row_trigger(*schema_guard, trigger_exist))) {
        LOG_WARN("fail to call has_before_update_row_trigger", K(*table_schema));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!skip_convert && OB_FAIL(add_additional_function_according_to_type(column_item, column_ref,
                                        T_INSERT_SCOPE,
                                        ObObjMeta::is_binary(tbl_col->get_data_type(),
                                                             tbl_col->get_collation_type())))) {
      LOG_WARN("failed to build column conv expr", K(ret));
    } else if (column_item->is_geo_) {
      // 1. set geo sub type to cast mode to column covert expr when update
      // 2. check geo type while doing column covert.
      ObColumnRefRawExpr *raw_expr = column_item->get_expr();
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("raw expr in column item is null", K(ret));
      } else {
        ObGeoType geo_type = raw_expr->get_geo_type();
        uint64_t cast_mode = column_ref->get_extra();
        if (OB_FAIL(ObGeoCastUtils::set_geo_type_to_cast_mode(geo_type, cast_mode))) {
          LOG_WARN("fail to set geometry type to cast mode", K(ret), K(geo_type));
        } else {
          column_ref->set_extra(cast_mode);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (trigger_exist &&
          OB_FAIL(ObRawExprUtils::build_wrapper_inner_expr(*params_.expr_factory_, *session_info_, column_ref, column_ref))) {
          LOG_WARN("failed to build wrapper inner expr", K(ret));
      } else {
        table_info.column_conv_exprs_.at(idx) = column_ref;
        LOG_TRACE("add column conv expr", K(*column_ref), K(trigger_exist));
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::build_column_conv_function_with_default_expr(ObInsertTableInfo& table_info,
                                                                   const int64_t idx)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  const ObColumnRefRawExpr *tbl_col = table_info.column_exprs_.at(idx);
  uint64_t table_id = table_info.table_id_;
  if (OB_ISNULL(del_upd_stmt) || OB_ISNULL(tbl_col) ||
      OB_ISNULL(session_info_) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    ObSchemaGetterGuard *schema_guard = NULL;
    const ObTableSchema* table_schema = NULL;
    bool trigger_exist = false;
    ColumnItem *column_item = del_upd_stmt->get_column_item_by_id(table_id, tbl_col->get_column_id());
    ObRawExpr *function_expr = NULL;
    ObRawExpr *expr = NULL;
    ObDefaultValueUtils utils(del_upd_stmt, &params_, this);
    if (OB_ISNULL(schema_checker_) ||
        OB_ISNULL(schema_guard = schema_checker_->get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid schema checker", K(schema_checker_));
    } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                         table_info.ref_table_id_,
                                                         table_schema,
                                                         table_info.is_link_table_))) {
      LOG_WARN("fail to get table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get table schema", K(table_info), K(table_schema));
    } else if (OB_FAIL(table_schema->has_before_insert_row_trigger(*schema_guard, trigger_exist))) {
      LOG_WARN("fail to call has_before_update_row_trigger", K(*table_schema));
    } else if (OB_ISNULL(column_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null column item", K(ret), K(column_item));
    } else if (OB_FAIL(utils.generate_insert_value(column_item, expr,
                                                   del_upd_stmt->has_instead_of_trigger()))) {
        LOG_WARN("failed to generate insert value", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr should not be null", K(ret));
    } else if (ob_is_enum_or_set_type(expr->get_data_type())) {
      function_expr = expr;
    } else {
      // For char type, compare and hash ignore space
      // For binary type, compare and hash not ignore '\0', so need to padding
      // '\0' for optimizer calculating partition location. As storage do right
      // trim of '\0', so don't worry extra space usage.
      if (ObObjMeta::is_binary(tbl_col->get_data_type(),
                               tbl_col->get_collation_type())) {
        if (OB_FAIL(build_padding_expr(session_info_, column_item, expr))) {
          LOG_WARN("Build padding expr error", K(ret));
        }
      }
      // maybe 没有必要再加一层column
      // conv函数，如果能够保证schema表中的默认值已经是合法值
      if (OB_FAIL(ret)) {
      } else if (expr->get_expr_type() == T_TABLET_AUTOINC_NEXTVAL) {
        // 如果是堆表的隐藏自增列，不需要构建conv表达式
        function_expr = expr;
      } else if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(*params_.expr_factory_,
                                                                *params_.allocator_,
                                                                *column_item->get_expr(),
                                                                expr, session_info_))) {
        LOG_WARN("fail to build column conv expr", K(ret));
      } else if (trigger_exist &&
                OB_FAIL(ObRawExprUtils::build_wrapper_inner_expr(*params_.expr_factory_, *session_info_, expr, expr))) {
        LOG_WARN("failed to build wrapper inner expr", K(ret));
      } else {
        function_expr = expr;
      }
    }
    if (OB_SUCC(ret)) {
      table_info.column_conv_exprs_.at(idx) = function_expr;
      LOG_DEBUG("add column conv expr", K(*function_expr));
    }
  }
  return ret;
}

int ObDelUpdResolver::build_column_conv_function_for_udt_column(ObInsertTableInfo& table_info,
                                                                const int64_t idx,
                                                                ObRawExpr *column_ref)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  const ObColumnRefRawExpr *tbl_col = table_info.column_exprs_.at(idx);
  uint64_t table_id = table_info.table_id_;
  if (OB_ISNULL(del_upd_stmt) || OB_ISNULL(tbl_col) ||
      OB_ISNULL(session_info_) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    ObSchemaGetterGuard *schema_guard = NULL;
    const ObTableSchema* table_schema = NULL;
    bool trigger_exist = false;
    ColumnItem *column_item = del_upd_stmt->get_column_item_by_id(table_id, tbl_col->get_column_id());
    uint64_t udt_set_id = tbl_col->get_udt_set_id();
    ColumnItem *hidden_column_item = NULL;
    int64_t hidd_idx = 0;
    for (int64_t i = 0; OB_ISNULL(hidden_column_item) && i < table_info.column_exprs_.count(); i++) {
      if (table_info.column_exprs_.at(i)->get_column_id() != tbl_col->get_column_id() &&
          table_info.column_exprs_.at(i)->get_udt_set_id() == tbl_col->get_udt_set_id()) {
        hidden_column_item = del_upd_stmt->get_column_item_by_id(table_id, table_info.column_exprs_.at(i)->get_column_id());
        hidd_idx = i;
      }
    }
    if (OB_ISNULL(hidden_column_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("find hidden column failed", K(schema_checker_));
    } else {
      ObRawExpr *function_expr = NULL;
      ObRawExpr *expr = NULL;
      ObDefaultValueUtils utils(del_upd_stmt, &params_, this);
      if (OB_ISNULL(schema_checker_) ||
          OB_ISNULL(schema_guard = schema_checker_->get_schema_guard())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid schema checker", K(schema_checker_));
      } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                          table_info.ref_table_id_,
                                                          table_schema,
                                                          table_info.is_link_table_))) {
        LOG_WARN("fail to get table schema", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get table schema", K(table_info), K(table_schema));
      } else if (OB_FAIL(table_schema->has_before_insert_row_trigger(*schema_guard, trigger_exist))) {
        LOG_WARN("fail to call has_before_update_row_trigger", K(*table_schema));
      } else if (OB_ISNULL(column_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column item", K(ret), K(column_item));
      } else if (OB_NOT_NULL(column_ref)) {
        if (OB_FAIL(add_additional_function_according_to_type(hidden_column_item, column_ref,
                                                              T_INSERT_SCOPE,
                                                              ObObjMeta::is_binary(tbl_col->get_data_type(),
                                                              tbl_col->get_collation_type())))) {
          LOG_WARN("failed to build column conv expr", K(ret));
        } else {
          function_expr = column_ref;
        }
      } else {
        // use default value from default_value_expr_
        if (OB_NOT_NULL(column_item->default_value_expr_)) {
          if (T_FUN_COLUMN_CONV == column_item->default_value_expr_->get_expr_type()) {
            if (column_item->default_value_expr_->get_param_expr(4)->is_const_raw_expr()) {
              expr = column_item->default_value_expr_->get_param_expr(4);
            } else if (column_item->default_value_expr_->get_param_expr(4)->get_expr_type() == T_FUN_SYS_CAST) {
              expr = column_item->default_value_expr_->get_param_expr(4)->get_param_expr(0);
            } else if (column_item->default_value_expr_->get_param_expr(4)->is_sys_func_expr()) {
              expr = column_item->default_value_expr_->get_param_expr(4);
            }
          }
        } else if (!column_item->default_value_.is_null()) {
          ObObj tmp = hidden_column_item->default_value_;
          hidden_column_item->set_default_value(column_item->default_value_);
          hidden_column_item->default_value_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
          hidden_column_item->default_value_.set_collation_level(tmp.get_collation_level());
          hidden_column_item->default_value_.set_type(ObVarcharType);
        }

        if (OB_SUCC(ret) && OB_ISNULL(expr)) {
          if (OB_FAIL(utils.generate_insert_value(hidden_column_item, expr,
                                                  del_upd_stmt->has_instead_of_trigger()))) {
            LOG_WARN("failed to generate insert value", K(ret));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr should not be null", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::transform_udt_column_value_expr(*params_.expr_factory_, expr, expr))) {
          LOG_WARN("transform udt value expr failed", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(*params_.expr_factory_,
                                                                  *params_.allocator_,
                                                                  *hidden_column_item->get_expr(), // build col conv for hidden column
                                                                  expr, session_info_))) {
          LOG_WARN("fail to build column conv expr", K(ret));
        } else if (trigger_exist &&
                  OB_FAIL(ObRawExprUtils::build_wrapper_inner_expr(*params_.expr_factory_, *session_info_, expr, expr))) {
          LOG_WARN("failed to build wrapper inner expr", K(ret));
        } else {
          function_expr = expr;
        }
      }

      if (OB_SUCC(ret)) {
       ObConstRawExpr *c_expr = NULL;
       if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_NULL,
                                                          c_expr))) {
          LOG_WARN("create raw expr failed", K(ret));
        } else {
          // udt column convert expr is useless, make T_NULL for it;
          table_info.column_conv_exprs_.at(idx) = c_expr;
          table_info.column_conv_exprs_.at(hidd_idx) = function_expr;
          LOG_DEBUG("add column conv expr", K(*function_expr));
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::generate_autoinc_params(ObInsertTableInfo &table_info)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  const ObTableSchema *table_schema = NULL;
  int64_t auto_increment_cache_size = -1;
  if (OB_ISNULL(del_upd_stmt) || OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(del_upd_stmt), K(params_.session_info_));
   } else if (OB_FAIL(schema_checker_->get_table_schema(params_.session_info_->get_effective_tenant_id(),
                                                        table_info.ref_table_id_,
                                                        table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_info.ref_table_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_schema));
  } else if (OB_FAIL(params_.session_info_->get_auto_increment_cache_size(auto_increment_cache_size))) {
    LOG_WARN("fail to get increment factor", K(ret));
  } else {
    for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
         OB_SUCCESS == ret && iter != table_schema->column_end(); ++iter) {
      ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid column schema", K(column_schema));
      } else {
        uint64_t column_id = column_schema->get_column_id();
        if (column_schema->is_autoincrement()) {
          del_upd_stmt->set_affected_last_insert_id(true);
          AutoincParam param;
          param.tenant_id_ = params_.session_info_->get_effective_tenant_id();
          param.autoinc_table_id_ = table_info.ref_table_id_;
          param.autoinc_first_part_num_ = table_schema->get_first_part_num();
          param.autoinc_table_part_num_ = table_schema->get_all_part_num();
          param.autoinc_col_id_ = column_id;
          param.auto_increment_cache_size_ = auto_increment_cache_size;
          param.part_level_ = table_schema->get_part_level();
          ObObjType column_type = table_schema->get_column_schema(column_id)->get_data_type();
          param.autoinc_col_type_ = column_type;
          param.autoinc_desired_count_ = 0;
          param.autoinc_mode_is_order_ = table_schema->is_order_auto_increment_mode();
          param.autoinc_version_ = table_schema->get_truncate_version();
          param.autoinc_auto_increment_ = table_schema->get_auto_increment();

          // hidden pk auto-increment variables' default value is 1
          // auto-increment variables for other columns are set in ob_sql.cpp
          // because physical plan may come from plan cache; it need be reset every time
          if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID == column_id) {
            param.autoinc_increment_ = 1;
            param.autoinc_offset_ = 1;
            param.part_value_no_order_ = true;
          } else if (column_schema->is_tbl_part_key_column()) {
            // don't keep intra-partition value asc order when partkey column is auto inc
            param.part_value_no_order_ = true;
          }

          if (OB_FAIL(get_value_row_size(param.total_value_count_))) {
            LOG_WARN("fail to get value row size", K(ret));
          } else if (OB_FAIL(del_upd_stmt->get_autoinc_params().push_back(param))) {
            LOG_WARN("failed to push autoinc_param", K(param), K(ret));
          }
        }
      }
    }//end for
  }
  LOG_DEBUG("generate autoinc_params", "autoin_params", del_upd_stmt->get_autoinc_params());
  return ret;
}

int ObDelUpdResolver::get_value_row_size(uint64_t& value_row_size)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  value_row_size = 1;
  if (OB_ISNULL(del_upd_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (del_upd_stmt->is_insert_stmt()) {
    ObInsertStmt *insert_stmt = static_cast<ObInsertStmt*>(del_upd_stmt);
    if (!insert_stmt->value_from_select()) {
      if (params_.is_batch_stmt_) {
        value_row_size = params_.batch_stmt_num_;
      } else {
        value_row_size = insert_stmt->get_insert_row_count();
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::resolve_insert_columns(const ParseNode *node,
                                             ObInsertTableInfo& table_info)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  if (OB_ISNULL(del_upd_stmt) || OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid insert stmt", K(del_upd_stmt), K_(session_info), K_(schema_checker));
  } else if (OB_ISNULL(table_item = del_upd_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (NULL != node && T_COLUMN_LIST == node->type_) {
    ParseNode *column_node = NULL;
    is_column_specify_ = true;
    if (OB_ISNULL(node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node children", K(node->children_));
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      column_node = node->children_[i];
      ObQualifiedName column_ref;
      ObRawExpr *ref_expr = NULL;
      ObColumnRefRawExpr *column_expr = NULL;
      ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
      bool is_duplicate = false;
      if (OB_ISNULL(column_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid node children", K(column_node));
      } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
        LOG_WARN("fail to get name case mode", K(ret));
      } else if (OB_FAIL(ObResolverUtils::resolve_column_ref(column_node, case_mode, column_ref))) {
        LOG_WARN("failed to resolve column def", K(ret));
      } else if (lib::is_oracle_mode() &&
                 ObCharset::case_insensitive_equal(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME,
                                                   column_ref.col_name_)) {
        ret = OB_ERR_VIRTUAL_COL_NOT_ALLOWED;
        LOG_WARN("cannot insert rowid pseudo column", K(ret), K(column_ref));
      } else if (OB_FAIL(resolve_basic_column_ref(column_ref, ref_expr))) {
        LOG_WARN("resolve basic column reference failed", K(ret));
        report_user_error_msg(ret, ref_expr, column_ref);
      } else if (OB_ISNULL(ref_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_expr is null");
      } else if (!ref_expr->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref expr is invalid", K(ret), KPC(ref_expr));
      } else if (FALSE_IT(column_expr = static_cast<ObColumnRefRawExpr*>(ref_expr))) {
        //do nothing
      } else if (OB_FAIL(check_insert_column_duplicate(column_expr->get_column_id(), is_duplicate))) {
        LOG_WARN("check insert column duplicate failed", K(ret));
      } else if (is_duplicate) {
        ret = OB_ERR_FIELD_SPECIFIED_TWICE;
        LOG_USER_ERROR(OB_ERR_FIELD_SPECIFIED_TWICE, to_cstring(column_expr->get_column_name()));
      } else if (!session_info_->get_ddl_info().is_ddl() && OB_HIDDEN_SESSION_ID_COLUMN_ID == column_expr->get_column_id()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify __session_id value");
      } else if (!session_info_->get_ddl_info().is_ddl() && OB_HIDDEN_SESS_CREATE_TIME_COLUMN_ID == column_expr->get_column_id()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify __sess_create_time value");
      } else if (OB_FAIL(mock_values_column_ref(column_expr))) {
        LOG_WARN("mock values column reference failed", K(ret));
      }
    }//end for
  } else {
    if (del_upd_stmt->get_table_size() != 1
        && del_upd_stmt->get_stmt_type() != stmt::T_MERGE) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("Insert statement only support one table", K(del_upd_stmt->get_stmt_type()), K(ret));
    }
    ObArray<ColumnItem> column_items;
    if (OB_SUCC(ret)) {
      if (table_item->is_basic_table() || table_item->is_link_table()) {
        if (OB_FAIL(resolve_all_basic_table_columns(*table_item, false, &column_items))) {
          LOG_WARN("resolve all basic table columns failed", K(ret));
        }
      } else if (del_upd_stmt->get_stmt_type() == stmt::T_MERGE
                 && table_item->is_generated_table()) {
        if (OB_FAIL(static_cast<ObMergeResolver*>(this)->resolve_merge_generated_table_columns(
                    del_upd_stmt, table_item, column_items))) {
          LOG_WARN("resolve generated table columns failed", K(ret));
        }
      } else if (table_item->is_generated_table() || table_item->is_temp_table()) {
        if (OB_FAIL(resolve_all_generated_table_columns(*table_item, column_items))) {
          LOG_WARN("resolve all generated table columns failed", K(ret));
        }
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("Only base table or view can be inserted", K(table_item->type_), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t N = column_items.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
        uint64_t column_id = column_items.at(i).column_id_;
        bool is_duplicate = false;
        if (OB_FAIL(check_insert_column_duplicate(column_id, is_duplicate))) {
          LOG_WARN("check insert column duplicate failed", K(ret));
        } else if (is_duplicate) {
          ret = OB_ERR_FIELD_SPECIFIED_TWICE;
          LOG_USER_ERROR(OB_ERR_FIELD_SPECIFIED_TWICE, to_cstring(column_items.at(i).column_name_));
        } else if (OB_FAIL(mock_values_column_ref(column_items.at(i).expr_))) {
          LOG_WARN("mock values column reference failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && (table_item->is_generated_table() || table_item->is_temp_table())
      && !del_upd_stmt->has_instead_of_trigger()) {
    FOREACH_CNT_X(desc, table_info.values_desc_, OB_SUCC(ret)) {
      const ColumnItem *column_item = del_upd_stmt->get_column_item_by_id(
          table_item->table_id_, (*desc)->get_column_id());
      if (OB_ISNULL(column_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item not found", K(ret));
      } else {
        if (NULL == table_item->view_base_item_) {
          if (OB_FAIL(set_base_table_for_updatable_view(*table_item, *column_item->expr_))) {
            LOG_WARN("find base table failed", K(ret));
          } else if (OB_ISNULL(table_item->view_base_item_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("view base table is NULL", K(ret));
          }
        } else {
          if (OB_FAIL(check_same_base_table(*table_item, *column_item->expr_))) {
            LOG_WARN("check insert columns is same base table failed",
                K(ret), K(*table_item), K(*column_item->expr_));
          }
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::resolve_insert_values(const ParseNode *node,
                                            ObInsertTableInfo& table_info)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  ObArray<ObRawExpr*> value_row;
  ObArray<int64_t> value_idxs; //store the old order of columns in values_desc
  uint64_t value_count = OB_INVALID_ID;
  bool is_all_default = false;
  bool is_update_view = false;
  if (OB_ISNULL(del_upd_stmt) || OB_ISNULL(node) || OB_ISNULL(session_info_) ||
      T_VALUE_LIST != node->type_ || OB_ISNULL(node->children_) || OB_ISNULL(del_upd_stmt->get_query_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(del_upd_stmt), K(node), K(session_info_), K(ret));
  } else if (is_oracle_mode() && 1 < node->num_child_) {
    // values only hold one row in oracle mode
    // ret = OB_ERR_CMD_NOT_PROPERLY_ENDED;

    // LOG_DEBUG("not supported in oracle mode", K(ret));

    /*
     * If the VALUES clause of an INSERT statement contains a record variable, no other
     * variable or value is allowed in the clause.
     */
  }
  if (FAILEDx(table_info.values_vector_.reserve(node->num_child_ * table_info.values_desc_.count()))) {
    // works for most cases. except label security/timestamp generation needs extend memory
    LOG_WARN("reserve memory fail", K(ret));
  }
  if (OB_SUCC(ret)) {
    TableItem* table_item = NULL;
    if (OB_FAIL(check_need_match_all_params(table_info.values_desc_,
                                            del_upd_stmt->get_query_ctx()->need_match_all_params_))) {
      LOG_WARN("check need match all params failed", K(ret));
    } else if (OB_ISNULL(table_item = del_upd_stmt->get_table_item_by_id(table_info.table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (table_item->is_generated_table()) {
      is_update_view = true;
    }
  }
  if (OB_SUCC(ret)) {
    //move generated columns behind basic columns before resolve values
    ObArray<ObColumnRefRawExpr*> tmp_values_desc;
    if (OB_FAIL(value_idxs.reserve(table_info.values_desc_.count()))) {
      LOG_WARN("fail to reserve memory", K(ret));
    } else if (OB_FAIL(tmp_values_desc.reserve(table_info.values_desc_.count()))) {
      LOG_WARN("fail to reserve memory", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < table_info.values_desc_.count(); ++j) {
        if (OB_ISNULL(table_info.values_desc_.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inalid value desc", K(j), K(table_info.values_desc_));
        } else if ((i == 0 && !table_info.values_desc_.at(j)->is_generated_column())
                   || (i == 1 && table_info.values_desc_.at(j)->is_generated_column())) {
          if (OB_FAIL(tmp_values_desc.push_back(table_info.values_desc_.at(j)))) {
            LOG_WARN("fail to push back values_desc_", K(ret));
          } else if (OB_FAIL(value_idxs.push_back(j))) {
            LOG_WARN("fail to push back value index", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      table_info.values_desc_.reuse();
      if (OB_FAIL(append(table_info.values_desc_, tmp_values_desc))) {
        LOG_WARN("fail to append new values_desc");
      }
    }
  }
  if (OB_SUCC(ret)) {
    //move generated columns behind basic columns before resolve values
    OZ (adjust_values_desc_position(table_info, value_idxs));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode *vector_node = node->children_[i];
    if (OB_ISNULL(vector_node) || OB_ISNULL(vector_node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid node children", K(i), K(node));
    } else if (vector_node->num_child_ != table_info.values_desc_.count()
        && !(1 == vector_node->num_child_  && T_EMPTY == vector_node->children_[0]->type_)
        && !(1 == node->num_child_ && T_OBJ_ACCESS_REF == vector_node->type_)) {
      ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
      LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, i+1);
      LOG_WARN("Column count doesn't match value count",
               "num_child", vector_node->num_child_,
               "vector_count", table_info.values_desc_.count());
    }
    if (OB_SUCC(ret)) {
      if (T_OBJ_ACCESS_REF == vector_node->type_) {
        if (NULL != params_.secondary_namespace_) {
          OZ (expand_record_to_columns(*vector_node, value_row));
        } else {
          ret = OB_UNIMPLEMENTED_FEATURE;
          LOG_WARN("insert values doesn't support record type without '()'", K(ret));
        }
      } else {
        if (OB_FAIL(value_row.reserve(vector_node->num_child_))) {
          LOG_WARN("fail reserve rows", K(ret), "size", vector_node->num_child_);
        }
        for (int32_t j = 0; OB_SUCC(ret) && j < vector_node->num_child_; j++) {
          ObRawExpr *expr = NULL;
          ObRawExpr *tmp_expr = NULL;
          const ObColumnRefRawExpr *column_expr = NULL;
          //for case: values(), read the first child
          const ParseNode *value_node = (1 == vector_node->num_child_) ? vector_node->children_[0] : vector_node->children_[value_idxs.at(j)];
          if (OB_ISNULL(value_node)
              || OB_ISNULL(column_expr = table_info.values_desc_.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("inalid children node", K(j), K(vector_node));
          } else if (T_EMPTY == value_node->type_) {
            //nothing todo
          } else {
            uint64_t column_id = column_expr->get_column_id();
            ObDefaultValueUtils utils(del_upd_stmt, &params_, this);
            bool is_generated_column = false;
            if (OB_FAIL(resolve_sql_expr(*value_node, expr))) {
              LOG_WARN("resolve sql expr failed", K(ret));
            } else if (OB_ISNULL(expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("fail to resolve sql expr", K(ret), K(expr));
            } else if (T_DEFAULT == expr->get_expr_type()) {
              ColumnItem *column_item = NULL;
              if (is_update_view && is_oracle_mode()) {
                ret = OB_ERR_DEFAULT_FOR_MODIFYING_VIEWS;
                LOG_USER_ERROR(OB_ERR_DEFAULT_FOR_MODIFYING_VIEWS);
              } else if (OB_ISNULL(column_item = del_upd_stmt->get_column_item_by_id(table_info.table_id_,
                                                                                     column_id))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get column item by id failed", K(table_info.table_id_), K(column_id));
              } else  if (column_expr->is_generated_column()) {
                //values中对应的生成列出现了default关键字，我们统一将default替换成生成列对应的表达式
                //下面的统一处理逻辑会为values中的column进行替换
                if (OB_FAIL(copy_schema_expr(*params_.expr_factory_,
                                             column_item->expr_->get_dependant_expr(),
                                             expr))) {
                  LOG_WARN("copy expr failed", K(ret));
                }
              } else if (OB_FAIL(utils.resolve_default_expr(*column_item, expr, T_INSERT_SCOPE))) {
                LOG_WARN("fail to resolve default value", "table_id", table_info.table_id_, K(column_id), K(ret));
              }
            } else if (OB_FAIL(check_basic_column_generated(column_expr, del_upd_stmt,
                                                            is_generated_column))) {
              LOG_WARN("check column generated failed", K(ret));
            } else if (is_generated_column) {
              ret = OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN;
              if (!is_oracle_mode()) {
                ColumnItem *orig_col_item = NULL;
                if (NULL != (orig_col_item = del_upd_stmt->get_column_item_by_id(table_info.table_id_, column_id))
                    && orig_col_item->expr_ != NULL) {
                  const ObString &column_name = orig_col_item->expr_->get_column_name();
                  const ObString &table_name = orig_col_item->expr_->get_table_name();
                  LOG_USER_ERROR(OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
                                column_name.length(), column_name.ptr(), table_name.length(), table_name.ptr());
                }
              }
            } else if (column_expr->is_always_identity_column()) {
              ret = OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN;
              LOG_USER_ERROR(OB_ERR_INSERT_INTO_GENERATED_ALWAYS_IDENTITY_COLUMN);
            } else {
              if ((column_expr->is_table_part_key_column()
                  || column_expr->is_table_part_key_org_column())
                  && expr->has_flag(CNT_SEQ_EXPR)) {
                del_upd_stmt->set_has_part_key_sequence(true);
              }
            }
            const ObIArray<ObColumnRefRawExpr*> &dep_cols = table_info.part_generated_col_dep_cols_;
            if (OB_SUCC(ret) && 0 != dep_cols.count()) {
              ColumnItem *col_item = NULL;
              for (int64_t i = 0; OB_SUCC(ret) && i < dep_cols.count(); ++i) {
                const ObColumnRefRawExpr *col_ref = dep_cols.at(i);
                CK(OB_NOT_NULL(col_ref));
                CK(OB_NOT_NULL(col_item = del_upd_stmt->get_column_item_by_id(
                                          col_ref->get_table_id(),
                                          col_ref->get_column_id())));
                if (OB_SUCC(ret)) {
                  if (NULL == col_item->default_value_expr_) {
                    OZ(utils.resolve_default_expr(*col_item, col_item->default_value_expr_,
                                                  T_INSERT_SCOPE));
                  }
                }
              }
            }

            tmp_expr = expr;
            // mysql support insert into xx values(expr, ...), expr contains column_ref
            // which has default values in column schema, replace column ref to const value here
            // eg: CREATE TABLE t1(f1 VARCHAR(100) DEFAULT 'test',id int primary key);
            //     INSERT INTO t1 VALUES(SUBSTR(f1, 1, 3),1);
            //     select * from t1;
            //      f1  id
            //      tes 1
            if (OB_SUCC(ret)) {
              if (tmp_expr->has_flag(CNT_COLUMN)) {
                if (OB_FAIL(replace_column_ref(&value_row, tmp_expr,
                                               column_expr->is_generated_column()))) {
                  LOG_WARN("fail to replace column ref", K(ret));
                } else {
                  SQL_RESV_LOG(DEBUG, "replace column to default", K(ret), K(*tmp_expr));
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(tmp_expr->formalize(session_info_))) {
                LOG_WARN("formalize value expr failed", K(ret));
              } else if (OB_FAIL(value_row.push_back(tmp_expr))) {
                LOG_WARN("Can not add expr_id to ObArray", K(ret));
              }
            }
          } //end else
        } //end for
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_INVALID_ID == value_count) {
        value_count = value_row.count();
      }
      if (OB_FAIL(check_column_value_pair(&value_row, table_info, i+1,
                                          value_count, is_all_default))) {
        LOG_WARN("fail to check column value count", K(ret));
      } else if (is_all_default) {
        //处理insert into test values(),()的情况
        if (OB_FAIL(build_row_for_empty_brackets(value_row, table_info))) {
          LOG_WARN( "fail to build row for empty brackets", K(ret));
        }
      } else {}
      if (OB_SUCC(ret)) {
        if (OB_FAIL(add_new_value_for_oracle_temp_table(value_row))) {
          LOG_WARN("failed to add __session_id value");
        } else if (OB_FAIL(append(table_info.values_vector_, value_row))) {
          LOG_WARN("failed to append value row", K(ret));
        }

      }
    }
    value_row.reset();
  }
  return ret;
}

int ObDelUpdResolver::check_column_value_pair(ObArray<ObRawExpr*> *value_row,
                                              ObInsertTableInfo& table_info,
                                              const int64_t row_index,
                                              const uint64_t value_count,
                                              bool& is_all_default)
{
  int ret = OB_SUCCESS;
  //如果现实指定了列，那么values（）不容许为空
  is_all_default = false;
  if (OB_ISNULL(value_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(value_row));
  } else if (value_row->count() == 0) {
    is_all_default = true;
    if (is_column_specify_) {
      ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
      LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, row_index);
    } else {}
  } else {}
  if (OB_SUCC(ret)) {
    uint64_t row_value_count = value_row->count();
    if (value_count != row_value_count) {
      ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
      LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, row_index);
    } else {}
  }
  if (OB_SUCC(ret)) {
    if (value_row->count() != 0 &&
        table_info.values_desc_.count() != value_row->count()) {
      ret = OB_ERR_COULUMN_VALUE_NOT_MATCH;
      LOG_USER_ERROR(OB_ERR_COULUMN_VALUE_NOT_MATCH, row_index);
    } else {}
  }
  return ret;
}


//如果values后面跟的是空括号的话，需要根据stmt语句中column列表的顺序加入default value;
//特例，如果stmt中没有指定column列表的话，则会在resolve_insert_column时加入全部的列
int ObDelUpdResolver::build_row_for_empty_brackets(ObArray<ObRawExpr*> &value_row,
                                                   ObInsertTableInfo& table_info)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  if (OB_ISNULL(del_upd_stmt)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(del_upd_stmt), K(ret));
  } else {
    ColumnItem *item = NULL;
    ObDefaultValueUtils utils(del_upd_stmt, &params_, static_cast<ObDMLResolver*>(this));
    for (int64_t i = 0; i < table_info.values_desc_.count() && OB_SUCCESS == ret; ++i) {
      ObRawExpr *expr = NULL;
      int64_t column_id = table_info.values_desc_.at(i)->get_column_id();
      if (OB_ISNULL(item = del_upd_stmt->get_column_item_by_id(table_info.table_id_, column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column item", K(column_id));
      } else if (OB_UNLIKELY(item->expr_->is_generated_column())) {
        if (OB_FAIL(copy_schema_expr(*params_.expr_factory_,
                                     item->expr_->get_dependant_expr(),
                                     expr))) {
          LOG_WARN("copy generated column dependant expr failed", K(ret));
        } else if (expr->has_flag(CNT_COLUMN)) {
          if (OB_FAIL(replace_column_ref(&value_row, expr))) {
            LOG_WARN("replace column reference failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(value_row.push_back(expr))) {
          LOG_WARN("fail to push back value expr", K(ret));
        }
      } else if (item->is_auto_increment()) {
        // insert into t (..) values (); 场景下不可以自动生成 nextval 表达式，而应该生成 null
        // 否则会出现问题：
        if (OB_FAIL(ObRawExprUtils::build_null_expr(*params_.expr_factory_, expr))) {
          LOG_WARN("failed to build next_val expr as null", K(ret));
        } else if (OB_FAIL(value_row.push_back(expr))) {
          LOG_WARN("fail to push back value expr", K(ret));
        }
      } else {
        if (OB_FAIL(utils.generate_insert_value(item, expr))) {
          LOG_WARN("fail to generate insert values", K(ret), K(column_id));
        } else if (OB_FAIL(value_row.push_back(expr))) {
          LOG_WARN("fail to push back value expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::check_update_part_key(const ObTableAssignment &ta,
                                         uint64_t ref_table_id,
                                         bool &is_updated,
                                         bool is_link /* = false */)
{
  int ret = OB_SUCCESS;
  is_updated = false;
  ObSEArray<uint64_t, 8> part_key_ids;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret));
  } else if (OB_FAIL(!is_link && get_part_key_ids(ref_table_id, part_key_ids))) {
    LOG_WARN("fail to get part key ids", K(ret), K(ref_table_id));
  }
  for (int64_t i = 0; !is_updated && OB_SUCC(ret) && i < ta.assignments_.count(); ++i) {
    ObColumnRefRawExpr *column_expr = ta.assignments_.at(i).column_expr_;
    ColumnItem *column_item = nullptr;
    if (OB_ISNULL(column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column expr", K(ret));
    } else if (OB_ISNULL(column_item = get_stmt()->get_column_item_by_id(column_expr->get_table_id(),
                                                                         column_expr->get_column_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column item", K(ret), KPC(column_expr));
    } else if (has_exist_in_array(part_key_ids, column_item->base_cid_)) {
      is_updated = true;
    }

  }
  return ret;
}

int ObDelUpdResolver::get_part_key_ids(const int64_t table_id, common::ObIArray<uint64_t> &array)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  array.reuse();
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                       table_id, table_schema))) {
    LOG_WARN("table schema not found", "table_id", table_id);
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get table schema error", K(table_id), K(table_schema));
  } else if (table_schema->is_partitioned_table()) {
    uint64_t part_key_column_id = OB_INVALID_ID;
    if (OB_SUCC(ret)) {
      const ObPartitionKeyInfo &part_key_info = table_schema->get_partition_key_info();
      for (int64_t i = 0; OB_SUCC(ret) && i < part_key_info.get_size(); ++i) {
        if (OB_FAIL(part_key_info.get_column_id(i, part_key_column_id))) {
          LOG_WARN("get rowkey info failed", K(ret), K(i), K(part_key_info));
          break;
        } else if (OB_FAIL(array.push_back(part_key_column_id))) {
          LOG_WARN("fail to add primary key to array", K(ret), K(part_key_column_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      const ObPartitionKeyInfo &subpart_key_info = table_schema->get_subpartition_key_info();
      for (int64_t i = 0; OB_SUCC(ret) && i < subpart_key_info.get_size(); ++i) {
        if (OB_FAIL(subpart_key_info.get_column_id(i, part_key_column_id))) {
          LOG_WARN("get rowkey info failed", K(ret), K(i), K(subpart_key_info));
          break;
        } else if (OB_FAIL(array.push_back(part_key_column_id))) {
          LOG_WARN("fail to add primary key to array", K(ret), K(part_key_column_id));
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::build_hidden_pk_assignment(ObTableAssignment &ta,
                                              const TableItem *table_item,
                                              const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  bool is_end_loop = false;
  ObDMLStmt *stmt = get_stmt();
  ObTableSchema::const_column_iterator iter = table_schema->column_begin();
  for (; OB_SUCC(ret) && !is_end_loop && iter != table_schema->column_end(); ++iter) {
    ColumnItem *col_item = NULL;
    ObColumnSchemaV2 *column_schema = *iter;
    if (NULL == column_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get column schema fail", K(column_schema));
    } else if (column_schema->get_column_id() != OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
      //do nothing
    } else {
      is_end_loop = true;
      ObAssignment assignment;
      ObSEArray<ObColumnRefRawExpr *, 1> col_exprs;
      ObColumnRefRawExpr *col_expr = NULL;
      ObRawExpr *expr = NULL;
      if (OB_FAIL(add_column_to_stmt(*table_item, *column_schema, col_exprs))) {
        LOG_WARN("add column to stmt failed", K(ret), K(*table_item), K(*column_schema));
      } else if (col_exprs.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no column expr returned", K(ret));
      } else if (OB_ISNULL(col_expr = col_exprs.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no column expr returned", K(ret));
      } else if (OB_ISNULL(col_item = stmt->get_column_item_by_id(table_item->table_id_,
                                                                  col_exprs.at(0)->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column item failed", K(ret),
                 "table_id", table_item->table_id_,
                 "column_id", col_exprs.at(0)->get_column_id());
      } else if (OB_FAIL(build_heap_table_hidden_pk_expr(expr, col_expr))) {
        LOG_WARN("fail to build heap_hidden_pk_expr", K(ret), K(col_expr));
      } else {
        assignment.column_expr_ = col_item->expr_;
        assignment.expr_ = expr;
        assignment.is_implicit_ = true;
      }

      if (OB_FAIL(ta.assignments_.push_back(assignment))) {
        LOG_WARN("fail to push  assignements", K(ret), K(assignment));
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::check_heap_table_update(ObTableAssignment &tas)
{
  int ret = OB_SUCCESS;
  const TableItem *table = NULL;
  const ObTableSchema *table_schema = NULL;
  bool is_update_part_key = false;

  ObDMLStmt *stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params_.session_info_ is null", K(ret));
  } else if (OB_ISNULL(table = stmt->get_table_item_by_id(tas.table_id_))) {
    LOG_WARN("fail to get table_item", K(ret), K(tas));
  } else if (OB_FAIL(schema_checker_->get_table_schema(params_.session_info_->get_effective_tenant_id(),
                                                       table->get_base_table_item().ref_id_,
                                                       table_schema, table->is_link_table()))) {
    LOG_WARN("fail to get table schema", K(ret),
             "base_table_id", table->get_base_table_item().ref_id_);
  } else if (!table_schema->is_heap_table()) {
    // 不是堆表，什么都不需要做
  } else if (OB_FAIL(check_update_part_key(tas,
                                           table->get_base_table_item().ref_id_,
                                           is_update_part_key,
                                           table->get_base_table_item().is_link_table()))) {
    LOG_WARN("fail to check whether update part key", K(ret), K(tas),
             "base_table_id", table->get_base_table_item().ref_id_);
  } else if (!is_update_part_key) {
    // 不更新分区建，do nothing
  } else if (OB_FAIL(build_hidden_pk_assignment(tas, table, table_schema))) {
    LOG_WARN("fail to build hidden_pk assignment", K(ret), K(tas), KPC(table));
  }
  return ret;
}

// TODO @yibo, add_column is used to handle some recursive dependency in insert resolver, should remove
int ObDelUpdResolver::generate_insert_table_info(const TableItem &table_item,
                                                 ObInsertTableInfo &table_info,
                                                 bool add_column /*= true */)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  const TableItem &base_table_item = table_item.get_base_table_item();
  const ObTableSchema *table_schema = NULL;
  uint64_t index_tid[OB_MAX_INDEX_PER_TABLE];
  int64_t gindex_cnt = OB_MAX_INDEX_PER_TABLE;
  if (OB_ISNULL(del_upd_stmt) || OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(del_upd_stmt), K(schema_checker_), K(session_info_));
  } else if (OB_FAIL(schema_checker_->get_can_write_index_array(session_info_->get_effective_tenant_id(),
                                                                base_table_item.ref_id_,
                                                                index_tid, gindex_cnt, true))) {
    LOG_WARN("failed to get global index", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                       base_table_item.ref_id_,
                                                       table_schema,
                                                       base_table_item.is_link_table()))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(table_info.part_ids_.assign(base_table_item.part_ids_))) {
      LOG_WARN("failed to assign part ids", K(ret));
  } else if (!del_upd_stmt->has_instead_of_trigger()) {
    if (add_column && OB_FAIL(add_all_rowkey_columns_to_stmt(table_item, table_info.column_exprs_))) {
      LOG_WARN("failed to add rowkey columns", K(ret));
    } else if (add_column && OB_FAIL(add_all_columns_to_stmt(table_item, table_info.column_exprs_))) {
      LOG_WARN("failed to add columns", K(ret));
    // } else if (OB_FAIL(prune_columns_for_ddl(table_item, table_info.column_exprs_))) {
      // LOG_WARN("failed to prune columns for ddl", K(ret));
    } else {
      table_info.table_id_ = table_item.table_id_;
      table_info.loc_table_id_ = base_table_item.table_id_;
      table_info.ref_table_id_ = base_table_item.ref_id_;
      table_info.table_name_ = table_schema->get_table_name_str();
      table_info.is_link_table_ = base_table_item.is_link_table();
    }
  } else {
    uint64_t view_id = OB_INVALID_ID;
    if (add_column && OB_FAIL(add_all_columns_to_stmt_for_trigger(table_item, table_info.column_exprs_))) {
      LOG_WARN("failed to add columns to stmt for trigger", K(ret));
    } else if (OB_FAIL(get_view_id_for_trigger(table_item, view_id))) {
      LOG_WARN("get view id failed", K(table_item), K(ret));
    } else {
      table_info.table_id_ = table_item.table_id_;
      table_info.loc_table_id_ = table_item.table_id_;
      table_info.ref_table_id_ = view_id;
      table_info.table_name_ = table_item.table_name_;
    }
  }
  if (OB_SUCC(ret) && gindex_cnt > 0) {
    del_upd_stmt->set_has_global_index(true);
  }
  return ret;
}

int ObDelUpdResolver::replace_gen_col_dependent_col(ObInsertTableInfo& table_info)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr* col_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info.column_exprs_.count(); i++) {
    if (OB_ISNULL(col_expr = table_info.column_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!col_expr->is_generated_column()) {
      // do nothing
    } else if (i >= table_info.column_conv_exprs_.count() ||
               OB_ISNULL(table_info.column_conv_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null column conv function", K(ret), K(i), K(table_info.column_conv_exprs_.count()));
    } else if (OB_FAIL(replace_col_with_new_value(table_info, table_info.column_conv_exprs_.at(i)))) {
      LOG_WARN("failed to replace col with new value", K(ret));
    }
  }
  return ret;
}

int ObDelUpdResolver::replace_col_with_new_value(ObInsertTableInfo& table_info, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObColumnRefRawExpr*> &all_cols = table_info.column_exprs_;
  const ObIArray<ObRawExpr*> &conv_funcs = table_info.column_conv_exprs_;
  for (int i = 0; OB_SUCC(ret) && i < all_cols.count(); i++) {
    if (i >= conv_funcs.count() || OB_ISNULL(conv_funcs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null column conv function", K(ret), K(i), K(all_cols.count()));
    } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr,
                                                          all_cols.at(i),
                                                          conv_funcs.at(i)))) {
      LOG_WARN("failed to replace ref column", K(ret));
    }
  } // for end

  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr->formalize(session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    }
  }
  return ret;
}

// part_generated_col_dep_cols_.count() != 0 means:
//  in heap table, generated column is partition key
//  we need to get all dependant columns of generated column.
// here we remove all columns which dup with values_desc
// eg: create table t1(c1 int, c2 int, c3 int as(c1+c2));
//     insert into t1(c1) values(1);
//       values_desc: c1
//       dep_cols: c1, c2
//       after remove dup: c2
int ObDelUpdResolver::remove_dup_dep_cols_for_heap_table(ObIArray<ObColumnRefRawExpr*>& dep_cols,
                                                         const ObIArray<ObColumnRefRawExpr*>& values_desc)
{
  int ret = OB_SUCCESS;
  if (0 == dep_cols.count()) {
    // do nothing
  } else {
    ObSEArray<ObColumnRefRawExpr*, 4> cols_no_dup;
    int64_t j = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < dep_cols.count(); ++i) {
      CK(OB_NOT_NULL(dep_cols.at(i)));
      for (j = 0; OB_SUCC(ret) && j < values_desc.count(); ++j) {
        CK(OB_NOT_NULL(values_desc.at(j)));
        if (OB_SUCC(ret) && values_desc.at(j)->get_column_id() ==
                            dep_cols.at(i)->get_column_id()) {
          break;
        }
      } // end for
      if (OB_SUCC(ret) && j >= values_desc.count()) { // not found
        OZ(cols_no_dup.push_back(dep_cols.at(i)));
      }
    } // end for
    OX(dep_cols.reset());
    OZ(dep_cols.assign(cols_no_dup));
  }
  LOG_DEBUG("remove dup dep cols done", K(dep_cols));
  return ret;
}

int ObDelUpdResolver::check_insert_column_duplicate(uint64_t column_id, bool &is_duplicate)
{
  int ret = OB_SUCCESS;
  is_duplicate = false;
  if (OB_HASH_EXIST == (ret = insert_column_ids_.exist_refactored(column_id))) {
    ret = OB_SUCCESS;
    is_duplicate = true;
  } else if (OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("check column id whether exist failed", K(ret), K(column_id));
  } else if (OB_FAIL(insert_column_ids_.set_refactored(column_id))) {
    LOG_WARN("set column id to insert column ids failed", K(ret), K(column_id));
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDelUpdResolver::add_new_sel_item_for_oracle_label_security_table(ObDmlTableInfo& table_info,
                                                                       ObIArray<uint64_t>& the_missing_label_se_columns,
                                                                       ObSelectStmt &select_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<SelectItem, 2> select_items;
  for (int64_t i = 0; OB_SUCC(ret) && i < the_missing_label_se_columns.count(); ++i) {
    SelectItem select_item;
    ObRawExpr *session_row_label_func = NULL;
    uint64_t column_id = the_missing_label_se_columns.at(i);

    if (OB_FAIL(create_session_row_label_expr(table_info, column_id, session_row_label_func))) {
      LOG_WARN("fail to create session row label expr", K(ret), K(column_id));
    } else {
      select_item.expr_ = session_row_label_func;
      if (OB_FAIL(select_items.push_back(select_item))) {
        LOG_WARN("push subquery select items failed", K(ret));
      }
      LOG_DEBUG("add sub select item", K(ret));
    }
  }
  if (OB_SUCC(ret) && !select_items.empty()) {
    if (OB_FAIL(add_select_items(select_stmt, select_items))) {
      LOG_WARN("failed to add select items", K(ret));
    }
  }
  return ret;
}

int ObDelUpdResolver::create_session_row_label_expr(ObDmlTableInfo& table_info,
                                                    uint64_t column_id,
                                                    ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;

  const ObDelUpdStmt *dep_upd_stmt = get_del_upd_stmt();

  if (OB_ISNULL(params_.expr_factory_)
      || OB_ISNULL(schema_checker_)
      || OB_ISNULL(session_info_)
      || OB_ISNULL(dep_upd_stmt)
      || OB_INVALID_ID == column_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), K(column_id), KPC(dep_upd_stmt));
  }

  ObString policy_name;

  if (OB_SUCC(ret)) {
    uint64_t table_id = table_info.ref_table_id_;
    const ObColumnSchemaV2 *column_schema = NULL;
    if (OB_FAIL(get_column_schema(table_id, column_id, column_schema, true,
                  ObSqlSchemaGuard::is_link_table(dep_upd_stmt, table_info.table_id_)))) {
      LOG_WARN("fail to get column item", K(ret), K(table_id), K(column_id));
    } else if (OB_FAIL(schema_checker_->get_label_se_policy_name_by_column_name(session_info_->get_effective_tenant_id(),
                                                                                column_schema->get_column_name_str(),
                                                                                policy_name))) {
      LOG_WARN("fail to get label security policy name", K(ret));
    }
  }

  ObConstRawExpr *policy_name_expr = NULL;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*params_.expr_factory_,
                                                        ObVarcharType,
                                                        policy_name,
                                                        ObCharset::get_default_collation(ObCharset::get_default_charset()),
                                                        policy_name_expr))) {
      LOG_WARN("fail to build policy name expr", K(ret));
    } else if (OB_ISNULL(policy_name_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    }
  }

  ObSysFunRawExpr *session_row_label_func = NULL;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_FUN_LABEL_SE_SESSION_ROW_LABEL,
                                                       session_row_label_func))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_ISNULL(session_row_label_func)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dummy expr is null", K(session_row_label_func));
    } else {
      ObString func_name(N_OLS_SESSION_ROW_LABEL);
      session_row_label_func->set_func_name(func_name);
      if (OB_FAIL(session_row_label_func->add_param_expr(policy_name_expr))) {
        LOG_WARN("fail to add param expr", K(ret));
      } else if (OB_FAIL(session_row_label_func->formalize(session_info_))) {
        LOG_WARN("failed to do formalize", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    expr = session_row_label_func;
  }

  return ret;
}

int ObDelUpdResolver::add_select_items(ObSelectStmt &select_stmt, const ObIArray<SelectItem>& select_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr factory is null", K(ret));
  } else if (!select_stmt.is_set_stmt()) {
    ObRawExprCopier copier(*params_.expr_factory_);
    if (deep_copy_stmt_objects<SelectItem>(copier,
                                           select_items,
                                           select_stmt.get_select_items())) {
      LOG_WARN("failed to deep copy stmt objects", K(ret));
    }
  } else {
    ObIArray<ObSelectStmt*> &child_query = select_stmt.get_set_query();
    const int64_t child_num = child_query.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < child_num; ++i) {
      if (OB_ISNULL(child_query.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(ret));
      } else if (OB_FAIL(SMART_CALL(add_select_items(*child_query.at(i), select_items)))) {
        LOG_WARN("failed to add select items", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(add_select_list_for_set_stmt(select_stmt))) {
      LOG_WARN("failed to create select list", K(ret));
    }
  }
  return ret;
}

/**
 * set stmt的左右子查询添加select item后需要为set stmt创建select item
 */
int ObDelUpdResolver::add_select_list_for_set_stmt(ObSelectStmt &select_stmt)
{
  int ret = OB_SUCCESS;
  SelectItem new_select_item;
  ObExprResType res_type;
  ObSelectStmt *child_stmt = NULL;
  if (!select_stmt.is_set_stmt()) {
    //do nothing
  } else if (OB_ISNULL(child_stmt = select_stmt.get_set_query(0))
             || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret), K(child_stmt));
  } else {
    int64_t num = child_stmt->get_select_item_size();
    for (int64_t i = select_stmt.get_select_item_size(); OB_SUCC(ret) && i < num; i++) {
      SelectItem &select_item = child_stmt->get_select_item(i);
      // unused
      // ObString set_column_name = left_select_item.alias_name_;
      ObItemType set_op_type = static_cast<ObItemType>(T_OP_SET + select_stmt.get_set_op());
      res_type.reset();
      new_select_item.alias_name_ = select_item.alias_name_;
      new_select_item.expr_name_ = select_item.expr_name_;
      new_select_item.is_real_alias_ = select_item.is_real_alias_ ||
                                       ObRawExprUtils::is_column_ref_skip_implicit_cast(select_item.expr_);
      res_type = select_item.expr_->get_result_type();
      if (OB_FAIL(ObRawExprUtils::make_set_op_expr(*params_.expr_factory_, i, set_op_type, res_type,
                                                   session_info_, new_select_item.expr_))) {
        LOG_WARN("create set op expr failed", K(ret));
      } else if (OB_FAIL(select_stmt.add_select_item(new_select_item))) {
        LOG_WARN("push back set select item failed", K(ret));
      } else if (OB_ISNULL(new_select_item.expr_) || OB_UNLIKELY(!new_select_item.expr_->is_set_op_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null or is not set op expr", "set op", PC(new_select_item.expr_));
      }
    }
  }
  return ret;
}

//查询插入的查询项添加session_id值,
//insert into t1 select c1,c2 ... -->
//insert into t1 select c1,c2, session_id值 ...
int ObDelUpdResolver::add_new_sel_item_for_oracle_temp_table(ObSelectStmt &select_stmt)
{
  int ret = OB_SUCCESS;
  if (is_oracle_tmp_table_) {
    ObSysFunRawExpr *session_id_expr = NULL;
    ObConstRawExpr *temp_table_type = NULL;
    ObConstRawExpr *sess_create_time_expr = NULL;
    ObSEArray<SelectItem, 4> select_items;
    SelectItem select_item;
    select_item.is_implicit_added_ = true;

    if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_FUN_GET_TEMP_TABLE_SESSID, session_id_expr))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_INT, sess_create_time_expr))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_INT, temp_table_type))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_ISNULL(session_id_expr) || OB_ISNULL(sess_create_time_expr) || OB_ISNULL(temp_table_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dummy expr is null", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObObj val;
      val.set_int(oracle_tmp_table_type_);
      temp_table_type->set_value(val);
    }
    if (OB_SUCC(ret)) {
      session_id_expr->set_func_name(N_TEMP_TABLE_SSID);
      select_item.expr_ = session_id_expr;
      if (OB_FAIL(session_id_expr->add_param_expr(temp_table_type))) {
        LOG_WARN("fail to add param expr", K(ret));
      } else if (OB_FAIL(session_id_expr->formalize(session_info_))) {
        LOG_WARN("fail to formalize expr", K(ret));
      } else if (OB_FAIL(select_items.push_back(select_item))) {
        LOG_WARN("push subquery select items failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObObj val;
      val.set_int(0);
      sess_create_time_expr->set_value(val);
      select_item.expr_ = sess_create_time_expr;
      if (OB_FAIL(sess_create_time_expr->formalize(session_info_))) {
        LOG_WARN("fail to formalize expr", K(ret));
      } else if (OB_FAIL(select_items.push_back(select_item))) {
        LOG_WARN("push subquery select items failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_select_items(select_stmt, select_items))) {
        LOG_WARN("failed to add select items", K(ret));
      }
    }
    LOG_DEBUG("add __session_id & __sess_create_time to select item succeed");
  }
  return ret;
}

// 值插入、查询插入对oracle临时表都要添加__session_id和__sess_create_time字段到目标列
int ObDelUpdResolver::add_new_column_for_oracle_temp_table(uint64_t ref_table_id,
                                                           uint64_t table_id /* = OB_INVALID_ID */,
                                                           ObDMLStmt *stmt /* = NULL */)
{
  int ret = OB_SUCCESS;
  if (is_oracle_tmp_table_) {
    const share::schema::ObColumnSchemaV2 *column_schema = NULL;
    const share::schema::ObColumnSchemaV2 *column_schema2 = NULL;
    const ObTableSchema *table_schema = NULL;
    ObColumnRefRawExpr *session_id_expr = NULL;
    ObColumnRefRawExpr *sess_create_time_expr = NULL;
    if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid session_info_", K(session_info_));
    } else if (OB_FAIL(get_table_schema(table_id, ref_table_id, stmt, table_schema))) {
      LOG_WARN("not find table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get tale schema", K(ret), K(table_schema));
    } else if (OB_ISNULL(column_schema = (table_schema->get_column_schema(OB_HIDDEN_SESSION_ID_COLUMN_ID)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*params_.expr_factory_,
                                                         *column_schema,
                                                         session_id_expr))) {
      LOG_WARN("build column expr failed", K(ret));
    } else if (OB_ISNULL(session_id_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session id expr is null");
    } else if (OB_FAIL(session_id_expr->formalize(session_info_))) {
      LOG_WARN("fail to formalize rowkey", KPC(session_id_expr), K(ret));
    } else if (OB_ISNULL(column_schema2 = (table_schema->get_column_schema(OB_HIDDEN_SESS_CREATE_TIME_COLUMN_ID)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*params_.expr_factory_,
                                                         *column_schema2,
                                                         sess_create_time_expr))) {
      LOG_WARN("build column expr failed", K(ret));
    } else if (OB_ISNULL(sess_create_time_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session create time expr is null");
    } else if (OB_FAIL(sess_create_time_expr->formalize(session_info_))) {
      LOG_WARN("fail to formalize rowkey", KPC(sess_create_time_expr), K(ret));
    } else {
      session_id_expr->set_ref_id(table_schema->get_table_id(), column_schema->get_column_id());
      session_id_expr->set_column_attr(table_schema->get_table_name(), column_schema->get_column_name_str());
      sess_create_time_expr->set_ref_id(table_schema->get_table_id(), column_schema2->get_column_id());
      sess_create_time_expr->set_column_attr(table_schema->get_table_name(), column_schema2->get_column_name_str());
      LOG_DEBUG("add __session_id & __sess_create_time to target succeed",
                K(*session_id_expr), K(*sess_create_time_expr));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(mock_values_column_ref(session_id_expr))) {
        LOG_WARN("mock values column reference failed", K(ret));
      } else if (OB_FAIL(mock_values_column_ref(sess_create_time_expr))) {
        LOG_WARN("mock values column reference failed", K(ret));
      }
    }
  }
  return ret;
}

// 值插入、查询插入对oracle临时表的都要添加__session_id字段到values
int ObDelUpdResolver::add_new_value_for_oracle_temp_table(ObIArray<ObRawExpr*> &value_row)
{
  int ret = OB_SUCCESS;
  if (is_oracle_tmp_table_) {
    ObSysFunRawExpr *session_id_expr = NULL;
    ObConstRawExpr *sess_create_time_expr = NULL;
    ObConstRawExpr *temp_table_type = NULL;
    if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid session_info_ ", K(session_info_));
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_FUN_GET_TEMP_TABLE_SESSID, session_id_expr))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_INT, sess_create_time_expr))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_INT, temp_table_type))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_ISNULL(session_id_expr) || OB_ISNULL(sess_create_time_expr) || OB_ISNULL(temp_table_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dummy expr is null", K(session_id_expr), K(sess_create_time_expr));
    } else {
      ObObj val;
      val.set_int(0);
      sess_create_time_expr->set_value(val);
      session_id_expr->set_func_name(N_TEMP_TABLE_SSID);
      val.set_int(oracle_tmp_table_type_);
      temp_table_type->set_value(val);
      if (OB_FAIL(session_id_expr->add_param_expr(temp_table_type))) {
        LOG_WARN("fail to add param expr", K(ret));
      } else if (OB_FAIL(session_id_expr->formalize(session_info_))) {
        LOG_WARN("fail to formalize expr", K(ret));
      } else if (OB_FAIL(sess_create_time_expr->formalize(session_info_))) {
        LOG_WARN("fail to formalize expr", K(ret));
      } else if (OB_FAIL(value_row.push_back(session_id_expr))) {
        LOG_WARN("push back to output expr failed", K(ret));
      } else if (OB_FAIL(value_row.push_back(sess_create_time_expr))) {
        LOG_WARN("push back to output expr failed", K(ret));
      }
      LOG_DEBUG("add session id & sess create time value succeed",
                K(session_id_expr), K(sess_create_time_expr), K(value_row));
    }
  }
  return ret;
}

//如果指定column没有label security列，在这里加上这一列，创建表达式指定插入的值为session write label
int ObDelUpdResolver::add_new_column_for_oracle_label_security_table(ObIArray<uint64_t>& the_missing_label_se_columns,
                                                                     uint64_t ref_table_id,
                                                                     uint64_t table_id /* = OB_INVALID_ID */,
                                                                     ObDMLStmt *stmt /* = NULL */)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;

  if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid session_info_", K(session_info_));
  } else if (OB_FAIL(get_table_schema(table_id, ref_table_id, stmt, table_schema))) {
    LOG_WARN("not find table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get tale schema", K(ret), K(table_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < the_missing_label_se_columns.count(); ++i) {
      uint64_t column_id = the_missing_label_se_columns.at(i);
      const share::schema::ObColumnSchemaV2 *column_schema = NULL;
      ObColumnRefRawExpr *label_se_column_expr = NULL;

      if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column schema", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*params_.expr_factory_,
                                                           *column_schema,
                                                           label_se_column_expr))) {
        LOG_WARN("build column expr failed", K(ret));
      } else if (OB_ISNULL(label_se_column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("build expr is NULL", K(ret));
      } else if (OB_FAIL(label_se_column_expr->formalize(session_info_))) {
        LOG_WARN("fail to formalize rowkey", KPC(label_se_column_expr), K(ret));
      } else {
        label_se_column_expr->set_ref_id(table_schema->get_table_id(), column_schema->get_column_id());
        label_se_column_expr->set_column_attr(table_schema->get_table_name(), column_schema->get_column_name_str());
        if (OB_FAIL(mock_values_column_ref(label_se_column_expr))) {
          LOG_WARN("mock values column reference failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDelUpdResolver::add_new_value_for_oracle_label_security_table(ObDmlTableInfo& table_info,
                                                                    ObIArray<uint64_t>& the_missing_label_se_columns,
                                                                    ObIArray<ObRawExpr*> &value_row)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0;
       OB_SUCC(ret) && i < the_missing_label_se_columns.count();
       ++i) {
    ObRawExpr *session_row_label_func = NULL;
    uint64_t column_id = the_missing_label_se_columns.at(i);

    if (OB_FAIL(create_session_row_label_expr(table_info, column_id, session_row_label_func))) {
      LOG_WARN("fail to create session row label expr", K(ret), K(column_id));
    } else if (OB_FAIL(value_row.push_back(session_row_label_func))) {
      LOG_WARN("push back to output expr failed", K(ret));
    }
    LOG_DEBUG("add session row label expr",
              K(session_row_label_func), K(value_row), K(lbt()));
  }

  return ret;
}

int ObDelUpdResolver::resolve_insert_update_assignment(const ParseNode *node, ObInsertTableInfo& table_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTableAssignment, 2> tables_assign;
  if (OB_ISNULL(node) || T_ASSIGN_LIST != node->type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node));
  } else if (OB_FAIL(resolve_assignments(*node, tables_assign, T_UPDATE_SCOPE))) {
    LOG_WARN("resolve assignment error", K(ret));
  } else if (OB_FAIL(resolve_additional_assignments(tables_assign, T_INSERT_SCOPE))) {
    LOG_WARN("resolve additional assignment error", K(ret));
  } else if (tables_assign.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table assignments in insert_stmt should only one table", K(ret), K(tables_assign));
  } else if (OB_FAIL(table_info.assignments_.assign(tables_assign.at(0).assignments_))) {
    LOG_WARN("failed to assign assignmnts", K(ret));
  } else if (OB_FAIL(add_relation_columns(tables_assign))) {
    LOG_WARN("Add needed columns error", K(ret));
  }
  return ret;
}

int ObDelUpdResolver::add_relation_columns(ObIArray<ObTableAssignment> &table_assigns)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  ObTableAssignment &table_assign = table_assigns.at(0);
  ObSEArray<ObDmlTableInfo*, 2> dml_table_infos;
  if (OB_ISNULL(del_upd_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get insert stmt", K(del_upd_stmt));
  } else if (OB_FAIL(del_upd_stmt->get_dml_table_infos(dml_table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  } else if (dml_table_infos.count() != 1 || OB_ISNULL(dml_table_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected dml table infos", K(dml_table_infos), K(ret));
  } else if (dml_table_infos.at(0)->table_id_ != table_assign.table_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table assignment should be the table of del_upd_stmt",
              K(table_assign.table_id_), K(dml_table_infos.at(0)->table_id_), K(ret));
  } else {
    //add auto_incrememnt column
    const ObIArray<ObColumnRefRawExpr*>& table_columns = dml_table_infos.at(0)->column_exprs_;
    ObIArray<share::AutoincParam> &auto_params = del_upd_stmt->get_autoinc_params();
    for (int64_t i = 0; i < auto_params.count() && OB_SUCCESS == ret; i++) {
      if (auto_params.at(i).autoinc_col_id_ != OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
        bool is_exist = false;
        int64_t index = -1;
        for (int64_t j = 0; j < table_columns.count() && OB_SUCC(ret); ++j) {
          const ColumnItem *col_item = del_upd_stmt->get_column_item_by_id(table_assign.table_id_,
              table_columns.at(j)->get_column_id());
          if (OB_ISNULL(col_item)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column item not found", K(ret));
          } else if (auto_params.at(i).autoinc_col_id_ == col_item->base_cid_) {
            is_exist = true;
            index = j;
            break;
          }
        }
        if (is_exist == false) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("auto increment column id not found in table columns", K(ret), K(auto_params.at(i)));
        } else {
          auto_params.at(i).autoinc_old_value_index_ = index;
        }
      }
    }

  }
  return ret;
}

int ObDelUpdResolver::replace_column_ref(ObArray<ObRawExpr*> *value_row,
                                        ObRawExpr *&expr,
                                        bool in_generated_column)
{
  //对于merge stmt，该function并不对expr进行替换
  UNUSED(expr);
  UNUSED(value_row);
  UNUSED(in_generated_column);
  return OB_SUCCESS;
}

int ObDelUpdResolver::get_label_se_columns(ObInsertTableInfo& table_info,
                                           ObIArray<uint64_t>& label_se_columns)
{
  int ret = OB_SUCCESS;
  //handle label security columns
  //统计哪些安全列没有插入，后面做特殊处理
  ObDelUpdStmt *del_upd_stmt = get_del_upd_stmt();
  if (OB_ISNULL(del_upd_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get insert stmt", K(del_upd_stmt));
  } else if (!del_upd_stmt->has_instead_of_trigger()) {
    const ObTableSchema *table_schema = NULL;
    if (OB_FAIL(get_table_schema(table_info.table_id_,
                                 table_info.ref_table_id_,
                                 del_upd_stmt, table_schema))) {
      LOG_WARN("not find table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get tale schema", K(ret), K(table_schema));
    } else if (table_schema->has_label_se_column()) {
      const ObIArray<uint64_t> &label_se_column_ids = table_schema->get_label_se_column_ids();
      for (int64_t i = 0; OB_SUCC(ret) && i < label_se_column_ids.count(); ++i) {
        bool label_se_column_already_handled = false;
        uint64_t column_id = label_se_column_ids.at(i);
        if (OB_FAIL(check_insert_column_duplicate(column_id, label_se_column_already_handled))) {
          LOG_WARN("fail to check insert column duplicate", K(ret));
        } else {
          if (!label_se_column_already_handled) {
            if (OB_FAIL(label_se_columns.push_back(column_id))) { //record for label se columns
              LOG_WARN("push back to array failed", K(ret));
            }
          } else {
            //do nothing
          }
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::prune_columns_for_ddl(const TableItem &table_item,
                                            ObIArray<ObColumnRefRawExpr*> &column_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session_info_), K(schema_checker_));
  } else if (session_info_->get_ddl_info().is_ddl()) {
    const ObTableSchema *ddl_table_schema = nullptr;
    uint64_t ddl_table_id = table_item.ddl_table_id_;
    ObSEArray<ObColumnRefRawExpr*, 8> tmp_column_exprs;
    if (OB_FAIL(tmp_column_exprs.assign(column_exprs))) {
      LOG_WARN("failed to assign column exprs", K(ret));
    } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                         ddl_table_id, ddl_table_schema))) {
      LOG_WARN("not find table schema", K(ret), K(table_item), K(ddl_table_id));
    } else if (nullptr == ddl_table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get illegal table schema", K(ddl_table_schema));
    } else {
      column_exprs.reuse();
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_exprs.count(); ++i) {
        ObColumnRefRawExpr* column = tmp_column_exprs.at(i);
        bool has_column = false;
        if (OB_ISNULL(column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(column));
        } else if (OB_FAIL(ddl_table_schema->has_column(column->get_column_id(), has_column))) {
          LOG_WARN("faild to check schema has column", K(ret));
        } else if (!has_column) {
          // do nothing
        } else if (OB_FAIL(column_exprs.push_back(column))) {
          LOG_WARN("failed to push back column expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::replace_column_ref_for_check_constraint(ObInsertTableInfo& table_info,
                                                              ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(params_.expr_factory_)) {
    LOG_WARN("invalid argument", K(expr));
    ret = OB_INVALID_ARGUMENT;
  } else if (ObRawExprUtils::find_expr(table_info.column_conv_exprs_, expr)) {
    // do nothing
  } else if (expr->get_param_count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(replace_column_ref_for_check_constraint(table_info,
                                                                    expr->get_param_expr(i))))) {
        LOG_WARN("replace column ref for check constraint", K(ret), KPC(expr->get_param_expr(i)));
      }
    }
  } else if (expr->is_column_ref_expr()) {
    const ObIArray<ObColumnRefRawExpr*>& insert_columns = table_info.column_exprs_;
    ObColumnRefRawExpr *b_expr = static_cast<ObColumnRefRawExpr*>(expr);
    if (OB_ISNULL(b_expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid expr or insert_columns", K(b_expr));
    } else {
      const int64_t N = insert_columns.count();
      int64_t index = OB_INVALID_INDEX;
      ret = OB_ENTRY_NOT_EXIST;
      for(int64_t i = 0; i < N && OB_ENTRY_NOT_EXIST == ret; ++i) {
        if (OB_ISNULL(insert_columns.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid insert columns", K(i), K(insert_columns.at(i)));
        } else if (b_expr == insert_columns.at(i)) {
          index = i;
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(index == OB_INVALID_INDEX)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to find column position", K(ret));
        } else {
          expr = table_info.column_conv_exprs_.at(index);
        }
      }
    }
  }

  return ret;
}

int ObDelUpdResolver::add_default_sequence_id_to_stmt(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = nullptr;
  if (OB_ISNULL(del_upd_stmt = get_del_upd_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else {
    for (int64_t i = 0; i < del_upd_stmt->get_column_items().count() && OB_SUCC(ret); i++) {
      const ObRawExpr *default_expr = NULL;
      if (del_upd_stmt->get_column_items().at(i).table_id_ == table_id
          && NULL != (default_expr = del_upd_stmt->get_column_items().at(i).default_value_expr_)
          && default_expr->has_flag(CNT_SEQ_EXPR)) {
        if (OB_FAIL(recursive_search_sequence_expr(default_expr))) {
          LOG_WARN("recursive search sequence expr failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::recursive_search_sequence_expr(const ObRawExpr *default_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(default_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (default_expr->has_flag(IS_SEQ_EXPR)) {
    const ObSequenceRawExpr *seq_raw_expr = static_cast<const ObSequenceRawExpr *>(default_expr);
    int64_t sequence_id = seq_raw_expr->get_sequence_id();
    const ObString &action = seq_raw_expr->get_action();
    if (OB_FAIL(add_sequence_id_to_stmt(sequence_id, 0 == action.case_compare("CURRVAL")))) {
      LOG_WARN("add sequence id to stmt failed", K(ret));
    }
  } else {
    for (int64_t i = 0; i < default_expr->get_param_count() && OB_SUCC(ret); i++) {
      const ObRawExpr *child_expr = default_expr->get_param_expr(i);
      if (child_expr->has_flag(CNT_SEQ_EXPR)
          && OB_FAIL(SMART_CALL(recursive_search_sequence_expr(child_expr)))) {
        LOG_WARN("resursive search sequence expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDelUpdResolver::check_need_match_all_params(const common::ObIArray<ObColumnRefRawExpr*> &value_descs, bool &need_match)
{
  int ret = OB_SUCCESS;
  need_match = false;
  ObSEArray<uint64_t, 4> col_ids;
  ObBitSet<> column_bs;
  for (int64_t i = 0; OB_SUCC(ret) && i < value_descs.count() && !need_match; ++i) {
    ObColumnRefRawExpr *value_desc = value_descs.at(i);
    if (OB_ISNULL(value_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value desc is null", K(ret), K(value_descs));
    } else if (OB_FAIL(column_bs.add_member(value_desc->get_column_id()))) {
      LOG_WARN("add column id failed", K(ret));
    }
  }
  //if a depend column exists in value_desc, need match the datatypes of all params
  for (int64_t i = 0; OB_SUCC(ret) && i < value_descs.count() && !need_match; ++i) {
    if (value_descs.at(i)->is_generated_column()) {
      col_ids.reset();
      if (OB_FAIL(ObRawExprUtils::extract_column_ids(value_descs.at(i)->get_dependant_expr(),
                                                    col_ids))) {
        LOG_WARN("extract column exprs failed", K(ret));
      } else {
        for (int64_t j = 0; j < col_ids.count() && !need_match; ++j) {
          if (column_bs.has_member(col_ids.at(j))) {
            need_match = true;
          }
        }
      }
    }
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
