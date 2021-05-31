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
#include "sql/resolver/dml/ob_default_value_utils.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {
int ObDefaultValueUtils::generate_insert_value(const ColumnItem* column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObDMLDefaultOp op = OB_INVALID_DEFAULT_OP;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column));
  } else if (OB_FAIL(get_default_type_for_insert(column, op))) {
    LOG_WARN("fail to check column default value", K(column), K(ret));
  } else {
    if (OB_NORMAL_DEFAULT_OP == op) {
      if (OB_FAIL(build_default_expr_strict(column, expr))) {
        LOG_WARN("fail to build default expr", K(ret));
      }
    } else if (OB_NOT_STRICT_DEFAULT_OP == op) {
      if (OB_FAIL(build_default_expr_not_strict(column, expr))) {
        LOG_WARN("fail to build default expr according to datatype", K(ret));
      }
    } else if (OB_GENERATED_COLUMN_DEFAULT_OP == op) {
      if (OB_FAIL(build_default_expr_for_generated_column(*column, expr))) {
        LOG_WARN("build default expr for generated column failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("default_value_op is INVALID", K(op), K(ret));
    }
  }
  return ret;
}

int ObDefaultValueUtils::resolve_default_function(ObRawExpr*& expr, ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* fun_expr = static_cast<ObSysFunRawExpr*>(expr);
  if (OB_ISNULL(fun_expr) || OB_UNLIKELY(T_FUN_SYS_DEFAULT != fun_expr->get_expr_type()) ||
      OB_UNLIKELY(ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO != fun_expr->get_param_count() &&
                  ObExprColumnConv::PARAMS_COUNT_WITHOUT_COLUMN_INFO != fun_expr->get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(fun_expr->get_expr_type()), K(fun_expr->get_param_count()));
  } else if (OB_UNLIKELY(fun_expr->get_param_expr(0)->get_expr_type() != T_REF_COLUMN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid default function, the first child is not column_ref", K(expr));
  } else if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(stmt_));
  } else {
    ColumnItem* column_item = NULL;
    ObColumnRefRawExpr* column_expr = static_cast<ObColumnRefRawExpr*>(fun_expr->get_param_expr(0));
    if (OB_ISNULL(column_expr) || (OB_ISNULL((column_item = stmt_->get_column_item_by_id(
                                                  column_expr->get_table_id(), column_expr->get_column_id()))))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get column item");
    } else {
      if (column_item->get_column_type()->is_timestamp()) {
        if (OB_FAIL(build_default_expr_for_timestamp(column_item, expr))) {
          LOG_WARN("fail to build default expr for timestamp", K(ret));
        }
      } else {
        if (OB_FAIL(build_type_expr(column_item, fun_expr->get_param_expr(0)))) {
          LOG_WARN("fail to replace first child", K(ret));
        } else if (OB_FAIL(build_collation_expr(column_item, fun_expr->get_param_expr(1)))) {
          LOG_WARN("fail to build accuracy expr", K(ret));
        } else if (OB_FAIL(build_accuracy_expr(column_item, fun_expr->get_param_expr(2)))) {
          LOG_WARN("fail to build accuracy expr", K(ret));
        } else if (OB_FAIL(build_nullable_expr(column_item, fun_expr->get_param_expr(3)))) {
          LOG_WARN("fail to build is nullable expr", K(ret));
        } else if (OB_FAIL(build_default_function_expr(column_item, fun_expr->get_param_expr(4), scope, false))) {
          LOG_WARN("fail to build default value", K(ret));
        } else if (ob_is_enumset_tc(column_expr->get_data_type())) {
          const ObIArray<ObString>& enum_set_values = column_expr->get_enum_set_values();
          if (OB_FAIL(fun_expr->set_enum_set_values(enum_set_values))) {
            LOG_WARN("failed to set_enum_set_values", K(ret));
          }
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObDefaultValueUtils::resolve_default_expr(const ColumnItem& column_item, ObRawExpr*& expr, ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_item.get_expr()) || (scope != T_INSERT_SCOPE && scope != T_UPDATE_SCOPE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(scope), K(column_item.get_expr()));
  } else if (OB_ISNULL(stmt_) || OB_ISNULL(params_) || OB_ISNULL(params_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(stmt_), KP_(params));
  } else if (column_item.expr_->is_generated_column()) {
    if (OB_FAIL(build_default_expr_for_generated_column(column_item, expr))) {
      LOG_WARN("build default expr for generated column failed", K(ret));
    }
  } else {
    ObSysFunRawExpr* default_func_expr = NULL;
    ObRawExpr* c_expr = NULL;
    ObRawExpr* is_null_expr = NULL;
    ObRawExpr* collation_expr = NULL;
    ObRawExpr* accuracy_expr = NULL;
    ObRawExpr* default_value_expr = NULL;
    if (OB_FAIL(params_->expr_factory_->create_raw_expr(T_FUN_SYS_DEFAULT, default_func_expr))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_ISNULL(default_func_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("default func expr is null");
    } else {
      default_func_expr->set_func_name(ObString::make_string(N_DEFAULT));
      default_func_expr->set_data_type(column_item.get_column_type()->get_type());
      if (OB_FAIL(build_type_expr(&column_item, c_expr))) {
        LOG_WARN("fail to build type expr", K(ret));
      } else if (OB_FAIL(default_func_expr->add_param_expr(c_expr))) {
        LOG_WARN("fail to add param expr", K(ret));
      } else if (OB_FAIL(build_collation_expr(&column_item, collation_expr))) {
        LOG_WARN("fail to build collation expr", K(ret));
      } else if (OB_FAIL(default_func_expr->add_param_expr(collation_expr))) {
        LOG_WARN("fail to add param expr", K(ret));
      } else if (OB_FAIL(build_accuracy_expr(&column_item, accuracy_expr))) {
        LOG_WARN("fail to build accuracy expr", K(ret));
      } else if (OB_FAIL(default_func_expr->add_param_expr(accuracy_expr))) {
        LOG_WARN("fail to add param expr", K(ret), K(accuracy_expr));
      } else if (OB_FAIL(build_nullable_expr(&column_item, is_null_expr))) {
        LOG_WARN("fail to build is nullable expr", K(ret));
      } else if (OB_FAIL(default_func_expr->add_param_expr(is_null_expr))) {
        LOG_WARN("fail to add is nullable expr", K(ret));
      } else if (OB_FAIL(build_default_function_expr(&column_item, default_value_expr, scope, true))) {
        LOG_WARN("fail to build default value expr", K(ret));
      } else if (OB_FAIL(default_func_expr->add_param_expr(default_value_expr))) {
        LOG_WARN("fail to add defualt value expr", K(ret));
      } else {
        const ObColumnSchemaV2* column_schema = NULL;
        if (ob_is_enumset_tc(column_item.get_column_type()->get_type())) {
          if (OB_ISNULL(params_->schema_checker_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema checker is NULL", K(ret));
          } else if (OB_FAIL(params_->schema_checker_->get_column_schema(
                         column_item.table_id_, column_item.column_id_, column_schema))) {
            LOG_WARN("get column schema fail", K(column_item), K(ret));
          } else if (OB_ISNULL(column_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema is NULL", K(column_item), K(ret));
          } else {
            const ObIArray<ObString>& enum_set_values = column_schema->get_extended_type_info();
            if (OB_FAIL(default_func_expr->set_enum_set_values(enum_set_values))) {
              LOG_WARN("failed to set_enum_set_values", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          expr = default_func_expr;
        }
      }
    }
  }
  return ret;
}

int ObDefaultValueUtils::build_default_expr_strict(const ColumnItem* column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* c_expr = NULL;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(column));
  } else if (OB_ISNULL(resolver_) || OB_ISNULL(stmt_) || OB_ISNULL(params_) || OB_ISNULL(params_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid resolver", K(resolver_), K(stmt_), KP_(params));
  } else if (IS_DEFAULT_NOW_OBJ(column->default_value_)) {
    if (OB_FAIL(build_now_expr(column, expr))) {
      LOG_WARN("fail to build now expr", K(ret));
    }
  } else if (NULL != column->default_value_expr_) {
    if (OB_FAIL(build_expr_default_expr(column, const_cast<ColumnItem*>(column)->default_value_expr_, expr))) {
      LOG_WARN("fail to build expr_default expr", K(ret));
    }
  } else if (column->is_auto_increment()) {
    if (OB_FAIL(resolver_->build_autoinc_nextval_expr(expr, column->base_cid_))) {
      LOG_WARN("failed to build next_val expr", K(ret));
    }
  } else if (column->base_cid_ == OB_HIDDEN_PK_PARTITION_COLUMN_ID) {
    // set default value for hidden partition_id column
    if (OB_FAIL(resolver_->build_partid_expr(expr, column->base_tid_))) {
      LOG_WARN("failed to build partition_id expr", K(ret));
    }
  } else if (OB_FAIL(params_->expr_factory_->create_raw_expr(
                 static_cast<ObItemType>(column->default_value_.get_type()), c_expr))) {
    LOG_WARN("create const expr failed", K(ret));
  } else if (OB_ISNULL(column->get_column_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column type is NULL", KPC(column), K(ret));
  } else {
    c_expr->set_accuracy(column->get_column_type()->get_accuracy());
    if (column->get_column_type()->is_enum() && column->is_not_null() && column->default_value_.is_null()) {
      const uint64_t ENUM_FIRST_VAL = 1;
      ObObj enum_val;
      enum_val.set_enum(ENUM_FIRST_VAL);
      c_expr->set_value(enum_val);
    } else {
      c_expr->set_value(column->default_value_);
    }
    if (OB_SUCC(ret) && ob_is_enumset_tc(c_expr->get_data_type())) {
      const ObColumnRefRawExpr* column_expr = column->get_expr();
      if (OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is NULL", KPC(column), K(ret));
      } else if (OB_UNLIKELY(column_expr->get_enum_set_values().count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column_expr", KPC(column_expr), K(ret));
      } else if (OB_FAIL(c_expr->set_enum_set_values(column_expr->get_enum_set_values()))) {
        LOG_WARN("failed to set_enum_set_values", KPC(column_expr), K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      expr = c_expr;
      if (OB_FAIL(expr->formalize(params_->session_info_))) {
        LOG_WARN("failed to extract info", K(ret));
      }
    }
  }
  return ret;
}

int ObDefaultValueUtils::build_now_expr(const ColumnItem* column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* f_expr = NULL;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(column));
  } else if (OB_ISNULL(params_) || OB_ISNULL(params_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid resolver", KP_(params));
  } else if (OB_FAIL(params_->expr_factory_->create_raw_expr(T_FUN_SYS_CUR_TIMESTAMP, f_expr))) {
    LOG_WARN("create sysfunc raw expr failed", K(ret));
  } else if (OB_ISNULL(f_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timestamp expr is null");
  } else {
    f_expr->set_data_type(ObTimestampType);
    f_expr->set_accuracy(column->get_column_type()->get_accuracy());
    f_expr->set_func_name(ObString::make_string(N_CUR_TIMESTAMP));
    expr = f_expr;
    // replace function now() with param expr
    if (OB_FAIL(expr->formalize(params_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    }
  }
  return ret;
}

int ObDefaultValueUtils::build_expr_default_expr(
    const ColumnItem* column, ObRawExpr*& input_expr, ObRawExpr*& const_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr* temp_expr = NULL;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(column));
  } else if (OB_ISNULL(params_) || OB_ISNULL(params_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid resolver", KP_(params));
  } else if (OB_FAIL(ObRawExprUtils::copy_expr(*(params_->expr_factory_), input_expr, temp_expr, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else {
    const_expr = temp_expr;
  }
  return ret;
}

int ObDefaultValueUtils::resolve_column_ref_in_insert(const ColumnItem* column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObDMLDefaultOp op = OB_INVALID_DEFAULT_OP;
  if (OB_ISNULL(column) || OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr), K(column));
  } else if (OB_FAIL(get_default_type_for_column_expr(column, op))) {
    LOG_WARN("fail to get default type for column expr", K(ret));
  } else {
    if (OB_NORMAL_DEFAULT_OP == op) {
      if (OB_FAIL(build_default_expr_strict(column, expr))) {
        LOG_WARN("fail to build default expr", K(ret));
      }
    } else if (OB_NOT_STRICT_DEFAULT_OP == op) {
      if (OB_FAIL(build_default_expr_not_strict(column, expr))) {
        LOG_WARN("fail to build default expr according to datatype", K(ret));
      }
    } else if (OB_TIMESTAMP_COLUMN_DEFAULT_OP == op) {
      if (OB_FAIL(build_default_expr_for_timestamp(column, expr))) {
        LOG_WARN("fail to build default expr for timestamp", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("default_value_op is INVALID", K(op), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    SQL_RESV_LOG(DEBUG, "resolve column ref in insert", K(*expr));
  }
  return ret;
}

// In insert stmt, if there is no column in insert values, the default value of the column needs to be filled
int ObDefaultValueUtils::get_default_type_for_insert(const ColumnItem* column, ObDMLDefaultOp& op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column) || OB_ISNULL(column->expr_) || OB_ISNULL(params_) || OB_ISNULL(params_->session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KPC(column), K(params_), K(ret));
  } else if (column->expr_->is_generated_column()) {
    op = OB_GENERATED_COLUMN_DEFAULT_OP;
  } else if (column->is_not_null() && !share::is_oracle_mode() && !column->is_auto_increment() &&
             column->base_cid_ != OB_HIDDEN_PK_PARTITION_COLUMN_ID) {
    if (!column->default_value_.is_null()) {
      op = OB_NORMAL_DEFAULT_OP;
    } else if (OB_ISNULL(column->get_column_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column type is NULL", KPC(column), K(ret));
    } else if (column->default_value_.is_null() && column->get_column_type()->is_enum()) {
      op = OB_NORMAL_DEFAULT_OP;
    } else {
      // Compatible with mysql, different processing needs to be done according to sql_mode
      // strict mode directly reports an error. non-strict mode takes the default value of the data type
      if (is_strict_mode(params_->session_info_->get_sql_mode())) {
        LOG_USER_ERROR(OB_ERR_NO_DEFAULT_FOR_FIELD, to_cstring(column->column_name_));
        ret = OB_ERR_NO_DEFAULT_FOR_FIELD;
        LOG_WARN("Column can not be null", K(column->column_name_), K(ret));
      } else {
        op = OB_NOT_STRICT_DEFAULT_OP;
      }
    }
  } else {
    op = OB_NORMAL_DEFAULT_OP;
  }
  return ret;
}

// insert into test values(1, default(c2));
/// insert into test set c2 = default(c2);
// update test set c1 = default(c1);
int ObDefaultValueUtils::get_default_type_for_default_function(
    const ColumnItem* column, ObDMLDefaultOp& op, ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  UNUSED(scope);
  if (OB_ISNULL(column) || OB_ISNULL(column->get_column_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column));
  } else if (column->is_auto_increment()) {
    op = OB_NOT_STRICT_DEFAULT_OP;
  } else if (column->get_column_type()->is_timestamp()) {
    op = OB_NOT_STRICT_DEFAULT_OP;
  } else if (column->get_column_type()->is_enum() && column->is_not_null() && column->default_value_.is_null()) {
    op = OB_NORMAL_DEFAULT_OP;
  } else if (column->is_not_null() && column->default_value_.is_null()) {
    ret = OB_ERR_NO_DEFAULT_FOR_FIELD;
    LOG_USER_ERROR(OB_ERR_NO_DEFAULT_FOR_FIELD, to_cstring(column->column_name_));
  } else {
    op = OB_NORMAL_DEFAULT_OP;
  }
  if (OB_SUCC(ret)) {
    SQL_RESV_LOG(DEBUG, "get default type for default function", K(*column), K(op), K(scope));
  }
  return ret;
}

// insert into test values(1, default, 2);
// insert into test set c1 = default;
// update test set c1 = default;
int ObDefaultValueUtils::get_default_type_for_default_expr(
    const ColumnItem* column, ObDMLDefaultOp& op, ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column) || OB_ISNULL(column->get_column_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column));
  } else if (OB_ISNULL(params_) || OB_ISNULL(params_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid inner status", K(params_));
  } else if (column->is_auto_increment()) {
    op = OB_NOT_STRICT_DEFAULT_OP;
  } else if (column->get_column_type()->is_timestamp() || NULL != column->default_value_expr_) {
    op = OB_NORMAL_DEFAULT_OP;
  } else if (column->get_column_type()->is_enum() && column->is_not_null() && column->default_value_.is_null()) {
    op = OB_NORMAL_DEFAULT_OP;
  } else if (is_strict_mode(params_->session_info_->get_sql_mode())) {
    op = OB_NORMAL_DEFAULT_OP;
  } else {
    op = OB_NOT_STRICT_DEFAULT_OP;
  }
  if (OB_SUCC(ret)) {
    SQL_RESV_LOG(DEBUG, "get default type for default expr", K(*column), K(op), K(scope));
  }
  return ret;
}

// insert into test set c1=c1, c1 is the timestamp column
// insert into test values(c1, 3);
// insert into test set c1 = default(c1);
// insert into test values(default(c1));
int ObDefaultValueUtils::build_default_expr_for_timestamp(const ColumnItem* column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObObj default_value;
  ObConstRawExpr* c_expr = NULL;
  ObRawExprFactory* expr_factory = NULL;
  if (OB_ISNULL(column) || OB_ISNULL(params_) || OB_ISNULL(expr_factory = params_->expr_factory_) ||
      OB_UNLIKELY(!column->get_column_type()->is_timestamp())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column), K_(params));
  } else if (OB_FAIL(params_->expr_factory_->create_raw_expr(T_INVALID, c_expr))) {
    LOG_WARN("create timestamp expr failed", K(ret));
  } else if (OB_ISNULL(c_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create raw expr fail", K(c_expr));
  } else if (IS_DEFAULT_NOW_OBJ(column->default_value_)) {
    if (column->is_not_null()) {
      default_value.set_timestamp(ObTimeConverter::ZERO_DATETIME);
      c_expr->set_expr_type(T_TIMESTAMP);
    } else {
      default_value.set_null();
      c_expr->set_expr_type(T_NULL);
    }
  } else {
    default_value = column->default_value_;
    c_expr->set_expr_type(T_TIMESTAMP);
  }
  if (OB_SUCC(ret)) {
    c_expr->set_value(default_value);
    expr = c_expr;
    if (OB_FAIL(expr->formalize(params_->session_info_))) {
      LOG_WARN("failed to extract info", K(ret));
    }
  }
  return ret;
}

// insert into test set c1 = c1;
int ObDefaultValueUtils::get_default_type_for_column_expr(const ColumnItem* column, ObDMLDefaultOp& op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column));
  } else if (column->get_column_type()->is_timestamp()) {
    op = OB_TIMESTAMP_COLUMN_DEFAULT_OP;
  } else if (column->is_auto_increment()) {
    op = OB_NOT_STRICT_DEFAULT_OP;
  } else if (column->is_not_null()) {
    op = OB_NOT_STRICT_DEFAULT_OP;
  } else {
    op = OB_NORMAL_DEFAULT_OP;
  }
  SQL_RESV_LOG(DEBUG, "get default type for column expr", KPC(column), K(op), K(ret));
  return ret;
}

int ObDefaultValueUtils::build_default_expr_not_strict(const ColumnItem* column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* c_expr = NULL;
  ObObj default_value;
  if (OB_ISNULL(column) || OB_ISNULL(params_) || OB_ISNULL(params_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column), KP_(params));
  } else if (!column->default_value_.is_null()) {
    default_value = column->default_value_;
  } else if (!column->is_not_null()) {
    default_value.set_null();
  } else {
    default_value.set_type(column->get_column_type()->get_type());
    if (OB_FAIL(default_value.build_not_strict_default_value())) {
      LOG_WARN("failed to build not strict default value info", K(column), K(ret));
    } else if (default_value.is_string_type()) {
      default_value.set_collation_level(CS_LEVEL_IMPLICIT);
      default_value.set_collation_type(column->get_column_type()->get_collation_type());
    }
  }
  if (OB_SUCC(ret)) {
    // create default value raw expr
    // ObObjType must be the same as the item type related to the data type in ObItemType, so you can cast here
    if (OB_FAIL(params_->expr_factory_->create_raw_expr(static_cast<ObItemType>(default_value.get_type()), c_expr))) {
      LOG_WARN("create default value expr failed", K(ret));
    } else if (OB_ISNULL(c_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create raw expr", K(c_expr));
    } else {
      c_expr->set_value(default_value);
      c_expr->set_accuracy(column->get_column_type()->get_accuracy());
    }
  }

  if (OB_SUCC(ret) && ob_is_enumset_tc(c_expr->get_data_type())) {
    const ObColumnRefRawExpr* column_expr = column->get_expr();
    if (OB_ISNULL(column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is NULL", KPC(column), K(ret));
    } else if (OB_UNLIKELY(column_expr->get_enum_set_values().count() < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column_expr", KPC(column_expr), K(ret));
    } else if (OB_FAIL(c_expr->set_enum_set_values(column_expr->get_enum_set_values()))) {
      LOG_WARN("failed to set_enum_set_values", KPC(column_expr), K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    expr = c_expr;
    if (OB_FAIL(expr->formalize(params_->session_info_))) {
      LOG_WARN("failed to extract info", K(ret));
    }
  }
  return ret;
}

int ObDefaultValueUtils::build_default_function_expr(
    const ColumnItem* column, ObRawExpr*& expr, ObStmtScope scope, const bool is_default_expr)
{
  int ret = OB_SUCCESS;
  ObDMLDefaultOp op = OB_INVALID_DEFAULT_OP;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invald argument", K(column));
  } else if (is_default_expr) {
    if (OB_FAIL(get_default_type_for_default_expr(column, op, scope))) {
      LOG_WARN("fail to get default type for default expr", K(ret));
    }
  } else {
    if (OB_FAIL(get_default_type_for_default_function(column, op, scope))) {
      LOG_WARN("fail to get default type for default function", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_NORMAL_DEFAULT_OP == op) {
      if (OB_FAIL(build_default_expr_strict(column, expr))) {
        LOG_WARN("fail to build default expr", K(ret));
      }
    } else if (OB_NOT_STRICT_DEFAULT_OP == op) {
      if (OB_FAIL(build_default_expr_not_strict(column, expr))) {
        LOG_WARN("fail to build default expr according to datatype", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("default_value_op is INVALID", K(op), K(ret));
    }
  }
  return ret;
}

int ObDefaultValueUtils::build_collation_expr(const ColumnItem* column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* c_expr = NULL;
  ObRawExprFactory* expr_factory = NULL;
  if (OB_ISNULL(column) || OB_UNLIKELY(column->is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to build accuracy expr", K(ret), K(column));
  } else if (OB_ISNULL(params_) || OB_ISNULL(expr_factory = params_->expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid allocator", KP_(params), KP(expr_factory));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory,
                 ObInt32Type,
                 static_cast<int32_t>(column->get_column_type()->get_collation_type()),
                 c_expr))) {
    LOG_WARN("fail to build accuracy expr", K(ret));
  } else {
    expr = c_expr;
    LOG_INFO("build collation expr", K(*expr), K(*column->get_column_type()));
  }
  return ret;
}

int ObDefaultValueUtils::build_accuracy_expr(const ColumnItem* column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* c_expr = NULL;
  ObRawExprFactory* expr_factory = NULL;
  if (OB_ISNULL(column) || OB_UNLIKELY(column->is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to build accuracy expr", K(ret), K(column));
  } else if (OB_ISNULL(params_) || OB_ISNULL(expr_factory = params_->expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid allocator", KP_(params), KP(expr_factory));
  } else {
    int64_t accuracy = column->get_column_type()->get_accuracy().get_accuracy();
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, accuracy, c_expr))) {
      LOG_WARN("fail to build accuracy expr", K(ret));
    } else {
      expr = c_expr;
    }
  }
  return ret;
}

int ObDefaultValueUtils::build_type_expr(const ColumnItem* column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* c_expr = NULL;
  ObRawExprFactory* expr_factory = NULL;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to build accuracy expr", K(ret), K(column));
  } else if (OB_ISNULL(params_) || OB_ISNULL(expr_factory = params_->expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid allocator", KP_(params), KP(expr_factory));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
                 *expr_factory, ObInt32Type, column->get_column_type()->get_type(), c_expr))) {
    LOG_WARN("fail to build accuracy expr", K(ret));
  } else {
    expr = c_expr;
  }
  return ret;
}

int ObDefaultValueUtils::build_nullable_expr(const ColumnItem* column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr* c_expr = NULL;
  ObRawExprFactory* expr_factory = NULL;
  if (OB_ISNULL(column) || OB_UNLIKELY(column->is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to build accuracy expr", K(ret), K(column));
  } else if (OB_ISNULL(params_) || OB_ISNULL(expr_factory = params_->expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid allocator", KP_(params), KP(expr_factory));
  } else if (OB_FAIL(
                 ObRawExprUtils::build_const_int_expr(*expr_factory, ObTinyIntType, !column->is_not_null(), c_expr))) {
    OB_LOG(WARN, "fail to build int expr", K(ret), K(c_expr));
  } else {
    expr = c_expr;
  }
  return ret;
}

int ObDefaultValueUtils::build_default_expr_for_generated_column(const ColumnItem& column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column.expr_) || OB_ISNULL(stmt_) || OB_ISNULL(params_) || OB_ISNULL(params_->expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("column expr is null", K_(column.expr), K_(stmt));
  } else if (OB_FAIL(ObRawExprUtils::copy_expr(
                 *params_->expr_factory_, column.expr_->get_dependant_expr(), expr, COPY_REF_DEFAULT))) {
    LOG_WARN("copy expr failed", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
