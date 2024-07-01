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
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/dml/ob_del_upd_resolver.h"
namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{
int ObDefaultValueUtils::generate_insert_value(const ColumnItem *column,
                                               ObRawExpr* &expr,
                                               bool has_instead_of_trigger)
{
  int ret = OB_SUCCESS;
  ObDMLDefaultOp op = OB_INVALID_DEFAULT_OP;
  if (OB_ISNULL(column) || OB_ISNULL(params_) ||
      OB_ISNULL(params_->expr_factory_) ||
      OB_ISNULL(params_->session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column), K(params_), K(params_->expr_factory_), K(params_->session_info_));
  } else if (OB_FAIL(get_default_type_for_insert(column, op))) {
    LOG_WARN("fail to check column default value", K(column), K(ret));
  } else if (has_instead_of_trigger
             && op > OB_INVALID_DEFAULT_OP) {
    // default expr in insert values is alway replaced to null if has instead of trigger
    if (OB_FAIL(build_default_expr_for_gc_column_ref(*column, expr))) {
      LOG_WARN("fail to build default expr for generate column", K(ret));
    }
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
    } else if (OB_IDENTITY_COLUMN_DEFAULT_OP == op) {
      if (OB_FAIL(build_default_expr_for_identity_column(*column, expr, T_INSERT_SCOPE))) {
        LOG_WARN("build default expr for identity column failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("default_value_op is INVALID", K(op), K(ret));
    }
  }
  return ret;
}

int ObDefaultValueUtils::resolve_default_function_static(
    const ObTableSchema *table_schema,
    const ObSQLSessionInfo &session_info,
    ObRawExprFactory &expr_factory,
    ObRawExpr *&expr,
    const ObResolverUtils::PureFunctionCheckStatus check_status)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *fun_expr = static_cast<ObSysFunRawExpr *>(expr);
  const ObColumnSchemaV2* col_schema = NULL;
  if (OB_ISNULL(table_schema) 
      || OB_ISNULL(fun_expr)
      || OB_UNLIKELY(T_FUN_SYS_DEFAULT != fun_expr->get_expr_type())
      || OB_UNLIKELY(
         ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO != fun_expr->get_param_count()
      && ObExprColumnConv::PARAMS_COUNT_WITHOUT_COLUMN_INFO != fun_expr->get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(fun_expr->get_expr_type()),
                 K(fun_expr->get_param_count()));
  } else if (OB_UNLIKELY(fun_expr->get_param_expr(0)->get_expr_type() != T_REF_COLUMN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid default function, the first child is not column_ref", K(expr));
  } else {
    ObColumnRefRawExpr *column_expr = 
                        static_cast<ObColumnRefRawExpr*>(fun_expr->get_param_expr(0));
    col_schema = table_schema->get_column_schema(column_expr->get_column_name());
    if (OB_ISNULL(col_schema)) {
      LOG_WARN("get null column schema", K(ret));
    } else if (IS_DEFAULT_NOW_OBJ(col_schema->get_cur_default_value())) {
      ret = OB_ERR_ONLY_PURE_FUNC_CANBE_VIRTUAL_COLUMN_EXPRESSION;
      LOG_WARN("only pure sys function can be indexed", K(ret));
    } else if (ObResolverUtils::DISABLE_CHECK == check_status) {
      if (OB_FAIL(build_type_expr_static(expr_factory, 
                                         col_schema->get_data_type(), 
                                         fun_expr->get_param_expr(0)))) {
        LOG_WARN("fail to replace first child", K(ret));
      } else if (OB_FAIL(build_collation_expr_static(expr_factory, 
                                                     col_schema->get_collation_type(), 
                                                     fun_expr->get_param_expr(1)))) {
        LOG_WARN("fail to build accuracy expr", K(ret));
      } else if (OB_FAIL(build_accuracy_expr_static(expr_factory, 
                                                    col_schema->get_accuracy(), 
                                                    fun_expr->get_param_expr(2)))) {
        LOG_WARN("fail to build accuracy expr", K(ret));
      } else if (OB_FAIL(build_nullable_expr_static(expr_factory, 
                                                    col_schema->is_nullable(), 
                                                    fun_expr->get_param_expr(3)))) {
        LOG_WARN("fail to build is nullable expr", K(ret));
      } else if (OB_FAIL(build_default_function_expr_static(
                  expr_factory, col_schema, fun_expr->get_param_expr(4),
                  session_info))) {
        LOG_WARN("fail to build default value", K(ret));
      } else if (ob_is_enumset_tc(col_schema->get_data_type())) {
        const ObIArray<ObString> &enum_set_values = col_schema->get_extended_type_info();
        if (OB_FAIL(fun_expr->set_enum_set_values(enum_set_values))) {
          LOG_WARN("failed to set_enum_set_values", K(ret));
        }
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObDefaultValueUtils::resolve_default_function(ObRawExpr *&expr, ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *fun_expr = static_cast<ObSysFunRawExpr *>(expr);
  if (OB_ISNULL(fun_expr)
      || OB_UNLIKELY(T_FUN_SYS_DEFAULT != fun_expr->get_expr_type())
      || OB_UNLIKELY(
         ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO != fun_expr->get_param_count()
      && ObExprColumnConv::PARAMS_COUNT_WITHOUT_COLUMN_INFO != fun_expr->get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(fun_expr->get_expr_type()),
                 K(fun_expr->get_param_count()));
  } else if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(stmt_));
  } else {
    ColumnItem *column_item = NULL;
    ObColumnRefRawExpr *column_expr = NULL;
    if (fun_expr->get_param_expr(0)->is_exec_param_expr()) {
      ObExecParamRawExpr* exec_param = static_cast<ObExecParamRawExpr*>(fun_expr->get_param_expr(0));
      if (OB_ISNULL(exec_param) || OB_ISNULL(exec_param->get_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exec_param is null", K(ret));
      } else if (OB_UNLIKELY(exec_param->get_ref_expr()->get_expr_type() != T_REF_COLUMN)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref expr of default is not column_ref", K(ret), K(*exec_param->get_ref_expr()));
      } else {
        column_expr = static_cast<ObColumnRefRawExpr*>(exec_param->get_ref_expr());
      }
    } else {
      if (OB_UNLIKELY(fun_expr->get_param_expr(0)->get_expr_type() != T_REF_COLUMN)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid default function, the first child is not column_ref", K(*expr), K(lbt()));
      } else {
        column_expr = static_cast<ObColumnRefRawExpr*>(fun_expr->get_param_expr(0));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_expr is null", K(ret));
      } else {
        for (ObDMLResolver *cur_resolver = resolver_; OB_SUCC(ret) && cur_resolver != NULL;
             cur_resolver = cur_resolver->get_parent_namespace_resolver()) {
          if (OB_ISNULL(cur_resolver->get_stmt())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("resolver get stmt is null", K(ret));
          } else {
            column_item = cur_resolver->get_stmt()->get_column_item_by_id(column_expr->get_table_id(),
                                                                        column_expr->get_column_id());
            if (column_item != NULL) {
              break;
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(column_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get column item", K(*column_expr), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
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
        } else if (OB_FAIL(build_default_function_expr(
                    column_item, fun_expr->get_param_expr(4), scope, false))) {
          LOG_WARN("fail to build default value", K(ret));
        } else if (ob_is_enumset_tc(column_expr->get_data_type())) {
          const ObIArray<ObString> &enum_set_values = column_expr->get_enum_set_values();
          if (OB_FAIL(fun_expr->set_enum_set_values(enum_set_values))) {
            LOG_WARN("failed to set_enum_set_values", K(ret));
          }
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

//为default生成一个default函数
int ObDefaultValueUtils::resolve_default_expr(const ColumnItem &column_item, ObRawExpr *&expr, ObStmtScope scope)
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
  } else if (column_item.expr_->is_identity_column()) {
    if (OB_FAIL(build_default_expr_for_identity_column(column_item, expr, scope))) {
      LOG_WARN("build default expr for identity column failed", K(ret));
    }
  } else {
    ObSysFunRawExpr *default_func_expr = NULL;
    ObRawExpr *c_expr = NULL;
    ObRawExpr *is_null_expr = NULL;
    ObRawExpr *collation_expr = NULL;
    ObRawExpr *accuracy_expr = NULL;
    ObRawExpr *default_value_expr = NULL;
    if (OB_FAIL(params_->expr_factory_->create_raw_expr(T_FUN_SYS_DEFAULT, default_func_expr))) {
      LOG_WARN("create raw expr failed", K(ret));
    } else if (OB_ISNULL(default_func_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("default func expr is null");
    } else if (OB_ISNULL(params_->session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params_.session_info_ is null", K(ret));
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
        const ObColumnSchemaV2 *column_schema = NULL;
        if (ob_is_enumset_tc(column_item.get_column_type()->get_type())) {
          bool is_link = ObSqlSchemaGuard::is_link_table(stmt_, column_item.table_id_);
          if (OB_ISNULL(params_->schema_checker_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema checker is NULL", K(ret));
          } else if (OB_FAIL(params_->schema_checker_->get_column_schema(
                             params_->session_info_->get_effective_tenant_id(),
                             column_item.base_tid_, column_item.base_cid_, column_schema, is_link))) {
            LOG_WARN("get column schema fail", K(column_item), K(ret));
          } else if (OB_ISNULL(column_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema is NULL", K(column_item), K(ret));
          } else {
            const ObIArray<ObString> &enum_set_values = column_schema->get_extended_type_info();
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

int ObDefaultValueUtils::build_default_expr_strict_static(
    ObRawExprFactory& expr_factory, 
    const ObColumnSchemaV2 *column_schema,
    ObRawExpr *&expr,
    const ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_ISNULL(column_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(column_schema));
  } else if (IS_DEFAULT_NOW_OBJ(column_schema->get_cur_default_value())) {
  } else if (OB_FAIL(expr_factory.create_raw_expr(
              static_cast<ObItemType>(column_schema->get_cur_default_value().get_type()), 
              c_expr))) {
    LOG_WARN("create const expr failed", K(ret));
  } else {
    c_expr->set_accuracy(column_schema->get_accuracy());
    if (column_schema->get_meta_type().is_enum()
        && !column_schema->is_nullable()
        && column_schema->get_cur_default_value().is_null()) {
      const uint64_t ENUM_FIRST_VAL = 1;
      ObObj enum_val;
      enum_val.set_enum(ENUM_FIRST_VAL);
      c_expr->set_value(enum_val);
    } else {
      c_expr->set_value(column_schema->get_cur_default_value());
    }
    if (OB_SUCC(ret) && ob_is_enumset_tc(c_expr->get_data_type())) {
      if (OB_UNLIKELY(column_schema->get_extended_type_info().count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column schema", KPC(column_schema), K(ret));
      } else if (OB_FAIL(c_expr->set_enum_set_values(column_schema->get_extended_type_info()))) {
        LOG_WARN("failed to set_enum_set_values", KPC(column_schema), K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      expr = c_expr;
      if (OB_FAIL(expr->formalize(&session_info))) {
        LOG_WARN("failed to extract info", K(ret));
      }
    }
  }
  return ret;
}

int ObDefaultValueUtils::build_default_expr_strict(const ColumnItem *column, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
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
    if (OB_FAIL(build_expr_default_expr(column, const_cast<ColumnItem *>(column)->default_value_expr_, expr))) {
      LOG_WARN("fail to build expr_default expr", K(ret));
    } else {
      ObDelUpdResolver* del_upd_resolver = dynamic_cast<ObDelUpdResolver *>(resolver_);
      if (OB_ISNULL(del_upd_resolver)) {
        // do nothing
      } else if (OB_FAIL(del_upd_resolver->recursive_search_sequence_expr(expr))) {
        LOG_WARN("fail to search sequence expr", K(ret));
      }
    }
  } else if (column->base_cid_ == OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
    if (OB_FAIL(resolver_->build_heap_table_hidden_pk_expr(expr, column->get_expr()))) {
      LOG_WARN("failed to build next_val expr", K(ret), KPC(column->get_expr()));
    }
  } else if (OB_ISNULL(column->get_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, column expr is nullptr", K(ret), KPC(column));
  } else if (column->is_auto_increment()) {
    if (OB_FAIL(resolver_->build_autoinc_nextval_expr(expr,
                                                      column->base_tid_,
                                                      column->base_cid_,
                                                      column->get_expr()->get_table_name(),
                                                      column->get_expr()->get_column_name()))) {
      LOG_WARN("failed to build next_val expr", K(ret));
    }
  } else if (OB_FAIL(params_->expr_factory_->create_raw_expr(
              static_cast<ObItemType>(column->default_value_.get_type()), c_expr))) {
    LOG_WARN("create const expr failed", K(ret));
  } else if (OB_ISNULL(column->get_column_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column type is NULL", KPC(column), K(ret));
  } else {
    c_expr->set_accuracy(column->get_column_type()->get_accuracy());
    if (column->get_column_type()->is_enum()
        && column->is_not_null_for_write()
        && column->default_value_.is_null()) {
      const uint64_t ENUM_FIRST_VAL = 1;
      ObObj enum_val;
      enum_val.set_enum(ENUM_FIRST_VAL);
      c_expr->set_value(enum_val);
    } else {
      c_expr->set_value(column->default_value_);
    }
    if (OB_SUCC(ret) && ob_is_enumset_tc(c_expr->get_data_type())) {
      const ObColumnRefRawExpr *column_expr = column->get_expr();
      if (OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is NULL", KPC(column), K(ret));
      } else if (OB_UNLIKELY(column_expr->get_enum_set_values().count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column_expr", KPC(column_expr), K(ret));
      } else if (OB_FAIL(c_expr->set_enum_set_values(column_expr->get_enum_set_values()))) {
        LOG_WARN("failed to set_enum_set_values", KPC(column_expr), K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      if (!ob_is_numeric_type(column->get_column_type()->get_type())) {
        // For non-numeric types, such as xml, use `_make_xml_binary` instead of the cast function,
        // and does not add extra cast for the default value here.
        expr = c_expr;
      } else {
        ObRawExpr *cast_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(params_->expr_factory_,
                                                            params_->session_info_,
                                                            *c_expr,
                                                            *column->get_column_type(),
                                                            cast_expr))) {
          LOG_WARN("failed to create raw expr.", K(ret));
        } else {
          expr = cast_expr;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(expr->formalize(params_->session_info_))) {
        LOG_WARN("failed to extract info", K(ret));
      }
    }
  }
  return ret;
}

int ObDefaultValueUtils::build_now_expr(const ColumnItem *column, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *f_expr = NULL;
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
    //replace function now() with param expr
    if (OB_FAIL(expr->formalize(params_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    }
  }
  return ret;
}

int ObDefaultValueUtils::build_expr_default_expr(const ColumnItem *column,
    ObRawExpr *&input_expr, ObRawExpr *&const_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *temp_expr = NULL;
  ObRawExpr *seq_expr = nullptr;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(column));
  } else if (OB_ISNULL(params_) || OB_ISNULL(params_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid resolver", KP_(params));
  } else if (OB_FAIL(ObRawExprCopier::copy_expr(*params_->expr_factory_,
                                                input_expr,
                                                temp_expr))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_invalid_sequence_expr(temp_expr, seq_expr))) {
    LOG_WARN("fail to get invalid sequence expr", K(ret));
  } else if (nullptr != seq_expr) {
    ret = OB_ERR_SEQ_NOT_EXIST;
    LOG_WARN("sequence not exist", K(ret));
  } else {
    const_expr = temp_expr;
  }
  return ret;
}

int ObDefaultValueUtils::resolve_column_ref_in_insert(const ColumnItem *column, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObDMLDefaultOp op = OB_INVALID_DEFAULT_OP;
  if (OB_ISNULL(column) || OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr), K(column));
  } else if (OB_FAIL(get_default_type_for_column_expr(column, op))){
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
      //timestamp列比较讨厌，很特殊；只能单独在这里处理；
      //不走寻常路
      if (OB_FAIL(build_default_expr_for_timestamp(column, expr))) {
        LOG_WARN("fail to build default expr for timestamp", K(ret));
      }
    } else if (OB_GENERATED_COLUMN_DEFAULT_OP == op) {
      // the ref_column to generated_column in insert values is always replaced to null
      if (OB_FAIL(build_default_expr_for_gc_column_ref(*column, expr))) {
        LOG_WARN("fail to build default expr for generate column");
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

//在insert stmt中，如果insert values中不带某个列，需要填充该列的default值
int ObDefaultValueUtils::get_default_type_for_insert(const ColumnItem *column, ObDMLDefaultOp &op)
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt *del_upd_stmt = dynamic_cast<ObDelUpdStmt*>(stmt_);
  if (OB_ISNULL(del_upd_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is not insert statement", K(ret));
  } else if (OB_ISNULL(column)
      || OB_ISNULL(column->expr_)
      || OB_ISNULL(params_)
      || OB_ISNULL(params_->session_info_)      ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KPC(column), K(params_), K(ret));
  } else if (column->expr_->is_generated_column()) {
    op = OB_GENERATED_COLUMN_DEFAULT_OP;
  } else if (column->expr_->is_identity_column()) {
    op = OB_IDENTITY_COLUMN_DEFAULT_OP;
  } else if (column->is_not_null_for_write()
             && !lib::is_oracle_mode()
             && !column->is_auto_increment()) {
    if (column->base_cid_ == OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
      op = OB_NORMAL_DEFAULT_OP;
    } else if (!column->default_value_.is_null()) {
      op = OB_NORMAL_DEFAULT_OP;
    } else if (OB_ISNULL(column->get_column_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column type is NULL", KPC(column), K(ret));
    } else if (column->default_value_.is_null()
               && column->get_column_type()->is_enum()) {
      op = OB_NORMAL_DEFAULT_OP;
    } else {
      //根据mysql文档，需要根据sql_mode来作不同的处理
      //strict模式直接报错；非strict模式取数据类型的默认值
      if (params_->session_info_->get_ddl_info().is_ddl()) {
        op = OB_NOT_STRICT_DEFAULT_OP;
      } else if (is_strict_mode(params_->session_info_->get_sql_mode()) && !del_upd_stmt->is_ignore()) {
        LOG_USER_ERROR(OB_ERR_NO_DEFAULT_FOR_FIELD, to_cstring(column->column_name_));
        ret = OB_ERR_NO_DEFAULT_FOR_FIELD;
        LOG_WARN("Column can not be null", K(column->column_name_), K(ret));
      } else {
        LOG_USER_WARN(OB_ERR_NO_DEFAULT_FOR_FIELD, to_cstring(column->column_name_));
        op = OB_NOT_STRICT_DEFAULT_OP;
      }
    }
  } else {
    op = OB_NORMAL_DEFAULT_OP;
  }
  return ret;
}


int ObDefaultValueUtils::get_default_type_for_default_function_static(
    const ObColumnSchemaV2 *column_schema,
    ObDMLDefaultOp &op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(column_schema));
  } else if (column_schema->is_autoincrement()) {
    op = OB_NOT_STRICT_DEFAULT_OP;
  } else if (column_schema->get_meta_type().is_timestamp()) {
    op = OB_NOT_STRICT_DEFAULT_OP;
  } else if (column_schema->get_meta_type().is_enum()
             && !column_schema->is_nullable()
             && column_schema->get_cur_default_value().is_null()) {
    op = OB_NORMAL_DEFAULT_OP;
  } else if (!column_schema->is_nullable() && column_schema->get_cur_default_value().is_null()) {
    ret = OB_ERR_NO_DEFAULT_FOR_FIELD;
    LOG_USER_ERROR(OB_ERR_NO_DEFAULT_FOR_FIELD, column_schema->get_column_name());
  } else {
    op = OB_NORMAL_DEFAULT_OP;
  }
  if (OB_SUCC(ret)) {
    SQL_RESV_LOG(DEBUG, "get default type for default function", K(*column_schema), K(op));
  }
  return ret;
}

//insert into test values(1, default(c2));
///insert into test set c2 = default(c2);
// update test set c1 = default(c1);
int ObDefaultValueUtils::get_default_type_for_default_function(const ColumnItem *column,
                                                               ObDMLDefaultOp &op,
                                                               ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  UNUSED(scope);
  if (OB_ISNULL(column)
      || OB_ISNULL(column->get_column_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column));
  } else if (column->is_auto_increment()) {
    op = OB_NOT_STRICT_DEFAULT_OP;
  } else if (column->get_column_type()->is_timestamp()) {
    op = OB_NOT_STRICT_DEFAULT_OP;
  } else if (column->get_column_type()->is_enum()
             && column->is_not_null_for_write()
             && column->default_value_.is_null()) {
    op = OB_NORMAL_DEFAULT_OP;
  } else if (column->is_not_null_for_write() && column->default_value_.is_null()) {
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

//insert into test values(1, default, 2);
//insert into test set c1 = default;
//update test set c1 = default;
int ObDefaultValueUtils::get_default_type_for_default_expr(const ColumnItem *column,
                                                           ObDMLDefaultOp &op,
                                                           ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column)
      || OB_ISNULL(column->get_column_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column));
  } else if (OB_ISNULL(params_) || OB_ISNULL(params_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid inner status", K(params_));
  } else if (column->is_auto_increment()) {
    op = OB_NOT_STRICT_DEFAULT_OP;
  } else if (column->get_column_type()->is_timestamp() || NULL != column->default_value_expr_) {
    op = OB_NORMAL_DEFAULT_OP;
  } else if (column->get_column_type()->is_enum()
             && column->is_not_null_for_write()
             && column->default_value_.is_null()) {
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

//insert into test set c1=c1, c1为timestamp列
//insert into test values(c1, 3);
//insert into test set c1 = default(c1);
//insert into test values(default(c1));
int ObDefaultValueUtils::build_default_expr_for_timestamp(const ColumnItem *column, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObObj default_value;
  ObConstRawExpr *c_expr = NULL;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(column) || OB_ISNULL(params_) || OB_ISNULL(expr_factory = params_->expr_factory_)
      || OB_UNLIKELY(!column->get_column_type()->is_timestamp())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column), K_(params));
  } else if (OB_FAIL(params_->expr_factory_->create_raw_expr(T_INVALID, c_expr))) {
    LOG_WARN("create timestamp expr failed", K(ret));
  } else if (OB_ISNULL(c_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create raw expr fail", K(c_expr));
  } else if (IS_DEFAULT_NOW_OBJ(column->default_value_)) {
    if (column->is_not_null_for_write()) {
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

//insert into test set c1 = c1;
int ObDefaultValueUtils::get_default_type_for_column_expr(const ColumnItem *column,
                                                          ObDMLDefaultOp &op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column));
  } else if (column->expr_->is_generated_column()) {
    op = OB_GENERATED_COLUMN_DEFAULT_OP;
  } else if (column->get_column_type()->is_timestamp()) {
    op = OB_TIMESTAMP_COLUMN_DEFAULT_OP;
  } else if (column->is_auto_increment()) {
    op = OB_NOT_STRICT_DEFAULT_OP;
  } else if (column->is_not_null_for_write()) {
    op = OB_NOT_STRICT_DEFAULT_OP;
  } else {
    op = OB_NORMAL_DEFAULT_OP;
  }
  SQL_RESV_LOG(DEBUG, "get default type for column expr", KPC(column), K(op), K(ret));
  return ret;
}

int ObDefaultValueUtils::build_default_expr_not_strict_static(
    ObRawExprFactory& expr_factory, 
    const ObColumnSchemaV2 *column_schema,
    ObRawExpr *&expr,
    const ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  ObObj default_value;
  if (OB_ISNULL(column_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_schema));
  } else if (!column_schema->get_cur_default_value().is_null()) {
    default_value = column_schema->get_cur_default_value();
  } else if (column_schema->is_nullable()) {
    default_value.set_null();
  } else {
    default_value.set_type(column_schema->get_data_type());
    if (OB_FAIL(default_value.build_not_strict_default_value(column_schema->get_accuracy().get_precision()))) {
      LOG_WARN("failed to build not strict default value info", K(column_schema), K(ret));
    } else if (default_value.is_string_type()) {
      default_value.set_collation_level(CS_LEVEL_IMPLICIT);
      default_value.set_collation_type(column_schema->get_collation_type());
    }
  }
  if (OB_SUCC(ret)) {
    //create default value raw expr
    //ObObjType必须和ObItemType中的数据类型相关的item type是重合的，所以这里可以cast
    if (OB_FAIL(expr_factory.create_raw_expr(
                static_cast<ObItemType>(default_value.get_type()), c_expr))) {
      LOG_WARN("create default value expr failed", K(ret));
    } else if (OB_ISNULL(c_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create raw expr", K(c_expr));
    } else {
      //const expr的类型会直接从default value中推导，所以这里不需要设置类型，设置了类型也是没有意义
      //后面deduce type会将其flush掉
      c_expr->set_value(default_value);
      c_expr->set_accuracy(column_schema->get_accuracy());
    }
  }

  if (OB_SUCC(ret) && ob_is_enumset_tc(c_expr->get_data_type())) {
    if (OB_UNLIKELY(column_schema->get_extended_type_info().count() < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column_expr", KPC(column_schema), K(ret));
    } else if (OB_FAIL(c_expr->set_enum_set_values(column_schema->get_extended_type_info()))) {
      LOG_WARN("failed to set_enum_set_values", KPC(column_schema), K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    expr = c_expr;
    if (OB_FAIL(expr->formalize(&session_info))) {
      LOG_WARN("failed to extract info", K(ret));
    }
  }
  return ret;
}

int ObDefaultValueUtils::build_default_expr_not_strict(const ColumnItem *column, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  ObObj default_value;
  if (OB_ISNULL(column) || OB_ISNULL(params_) || OB_ISNULL(params_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column), KP_(params));
  } else if (!column->default_value_.is_null()) {
    default_value = column->default_value_;
  } else if (!column->is_not_null_for_write()) {
    default_value.set_null();
  } else {
    default_value.set_type(column->get_column_type()->get_type());
    if (OB_FAIL(default_value.build_not_strict_default_value(column->get_column_type()->get_accuracy().get_precision()))) {
      LOG_WARN("failed to build not strict default value info", K(column), K(ret));
    } else if (default_value.is_string_type()) {
      default_value.set_collation_level(CS_LEVEL_IMPLICIT);
      default_value.set_collation_type(column->get_column_type()->get_collation_type());
    }
  }
  if (OB_SUCC(ret)) {
    //create default value raw expr
    //ObObjType必须和ObItemType中的数据类型相关的item type是重合的，所以这里可以cast
    if (OB_FAIL(params_->expr_factory_->create_raw_expr(static_cast<ObItemType>(default_value.get_type()), c_expr))) {
      LOG_WARN("create default value expr failed", K(ret));
    } else if (OB_ISNULL(c_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create raw expr", K(c_expr));
    } else {
      //const expr的类型会直接从default value中推导，所以这里不需要设置类型，设置了类型也是没有意义
      //后面deduce type会将其flush掉
      c_expr->set_value(default_value);
      c_expr->set_accuracy(column->get_column_type()->get_accuracy());
    }
  }

  if (OB_SUCC(ret) && ob_is_enumset_tc(c_expr->get_data_type())) {
    const ObColumnRefRawExpr *column_expr = column->get_expr();
    if (OB_ISNULL(column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is NULL", KPC(column), K(ret));
    } else if (OB_UNLIKELY(column_expr->get_enum_set_values().count() < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column_expr", KPC(column_expr), K(ret));
    } else if (OB_FAIL(c_expr->set_enum_set_values(column_expr->get_enum_set_values()))) {
      LOG_WARN("failed to set_enum_set_values", KPC(column_expr), K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    expr = c_expr;
    if (OB_FAIL(expr->formalize(params_->session_info_))) {
      LOG_WARN("failed to extract info", K(ret));
    }
  }
  return ret;
}

int ObDefaultValueUtils::build_default_function_expr_static(
    ObRawExprFactory& expr_factory, 
    const ObColumnSchemaV2 *column_schema,
    ObRawExpr *&expr,
    const ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  ObDMLDefaultOp op = OB_INVALID_DEFAULT_OP;
  if (OB_ISNULL(column_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invald argument", K(column_schema));
  } else {
    if (OB_FAIL(get_default_type_for_default_function_static(column_schema, op))) {
      LOG_WARN("fail to get default type for default function", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_NORMAL_DEFAULT_OP == op) {
      if (OB_FAIL(build_default_expr_strict_static(expr_factory, 
                                                   column_schema, 
                                                   expr, 
                                                   session_info))) {
        LOG_WARN("fail to build default expr", K(ret));
      }
    } else if (OB_NOT_STRICT_DEFAULT_OP == op) {
      if (OB_FAIL(build_default_expr_not_strict_static(expr_factory, 
                                                       column_schema, 
                                                       expr, 
                                                       session_info))) {
        LOG_WARN("fail to build default expr according to datatype", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("default_value_op is INVALID", K(op), K(ret));
    }
  }
  return ret;
}

int ObDefaultValueUtils::build_default_function_expr(const ColumnItem *column,
                                                     ObRawExpr *&expr,
                                                     ObStmtScope scope,
                                                     const bool is_default_expr)
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

int ObDefaultValueUtils::build_collation_expr_static(ObRawExprFactory& expr_factory, 
                                                     const common::ObCollationType coll_type, 
                                                     ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
              expr_factory, ObInt32Type,
              coll_type,
              c_expr))) {
    LOG_WARN("fail to build accuracy expr", K(ret));
  } else {
    expr = c_expr;
    LOG_INFO("build collation expr", K(*expr), K(coll_type));
  }
  return ret;
}

int ObDefaultValueUtils::build_collation_expr(const ColumnItem *column, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(column) || OB_UNLIKELY(column->is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to build accuracy expr", K(ret), K(column));
  } else if (OB_ISNULL(params_) || OB_ISNULL(expr_factory = params_->expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid allocator", KP_(params), KP(expr_factory));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObInt32Type,
                                                          static_cast<int32_t>(column->get_column_type()->get_collation_type()),
                                                          c_expr))) {
    LOG_WARN("fail to build accuracy expr", K(ret));
  } else {
    expr = c_expr;
    LOG_INFO("build collation expr", K(*expr), K(*column->get_column_type()));
  }
  return ret;
}

int ObDefaultValueUtils::build_accuracy_expr(const ColumnItem *column, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  ObRawExprFactory *expr_factory = NULL;
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

int ObDefaultValueUtils::build_accuracy_expr_static(ObRawExprFactory& expr_factory, 
                                                    const common::ObAccuracy& accuracy, 
                                                    ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;

  if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, 
                                                   ObIntType, 
                                                   accuracy.get_accuracy(), 
                                                   c_expr))) {
    LOG_WARN("fail to build accuracy expr", K(ret));
  } else {
    expr = c_expr;
  }
  return ret;
}

int ObDefaultValueUtils::build_type_expr_static(ObRawExprFactory& expr_factory, 
                                                const common::ColumnType data_type, 
                                                ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
              expr_factory, ObInt32Type, data_type, c_expr))) {
    LOG_WARN("fail to build accuracy expr", K(ret));
  } else {
    expr = c_expr;
  }
  return ret;
}

int ObDefaultValueUtils::build_type_expr(const ColumnItem *column, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  ObRawExprFactory *expr_factory = NULL;
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

int ObDefaultValueUtils::build_nullable_expr_static(ObRawExprFactory& expr_factory, 
                                                    const bool nullable, 
                                                    ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, 
                                                   ObTinyIntType,
                                                   nullable,
                                                   c_expr))) {
    OB_LOG(WARN, "fail to build int expr", K(ret), K(nullable));
  } else {
    expr = c_expr;
  }
  return ret;
}

int ObDefaultValueUtils::build_nullable_expr(const ColumnItem *column, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(column) || OB_UNLIKELY(column->is_invalid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to build accuracy expr", K(ret), K(column));
  } else if (OB_ISNULL(params_) || OB_ISNULL(expr_factory = params_->expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid allocator", KP_(params), KP(expr_factory));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObTinyIntType,
                                                       !column->is_not_null_for_write(), c_expr))) {
    OB_LOG(WARN, "fail to build int expr", K(ret), K(c_expr));
  } else {
    expr = c_expr;
  }
  return ret;
}

int ObDefaultValueUtils::build_default_expr_for_generated_column(const ColumnItem &column, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column.expr_) || OB_ISNULL(stmt_) || OB_ISNULL(params_) || OB_ISNULL(params_->expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("column expr is null", K_(column.expr), K_(stmt));
  } else if (OB_FAIL(ObDMLResolver::copy_schema_expr(*params_->expr_factory_,
                                                     column.expr_->get_dependant_expr(),
                                                     expr))) {
    LOG_WARN("failed to copy dependant expr", K(ret));
  }
  return ret;
}

int ObDefaultValueUtils::build_default_expr_for_gc_column_ref(const ColumnItem &column, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column.expr_) || OB_ISNULL(stmt_) || OB_ISNULL(params_) || OB_ISNULL(params_->expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("column expr is null", K_(column.expr), K_(stmt));
  } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*params_->expr_factory_, expr))) {
    LOG_WARN("fail to build null expr", K(ret));
  }
  return ret;
}

int ObDefaultValueUtils::build_default_expr_for_identity_column(const ColumnItem &column,
                                                                ObRawExpr *&expr,
                                                                ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column.expr_) || OB_ISNULL(stmt_) || OB_ISNULL(params_) || OB_ISNULL(resolver_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("column expr is null", K(column.expr_), K(stmt_), K(params_), K(ret));
  } else {
    uint64_t tenant_id = params_->session_info_->get_effective_tenant_id();
    const ObColumnSchemaV2 *column_schema = NULL;
    const ObSequenceSchema *sequence_schema = NULL;
    const ObString dummy_db_name;
    uint64_t sequence_id;
    bool is_link = ObSqlSchemaGuard::is_link_table(stmt_, column.table_id_);
    if (OB_FAIL(params_->schema_checker_->get_column_schema(tenant_id,
                column.base_tid_, column.base_cid_, column_schema, is_link))) {
      LOG_WARN("get column schema fail", K(ret));
    } else {
      sequence_id = column_schema->get_sequence_id();
      const ObString seq_oper("NEXTVAL");
      if (OB_FAIL(params_->schema_checker_->get_schema_guard()->get_sequence_schema
                      (tenant_id, sequence_id, sequence_schema))) {
        LOG_WARN("get column schema fail", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_seq_nextval_expr(
                  expr, resolver_->session_info_, resolver_->params_.expr_factory_, dummy_db_name,
                  sequence_schema->get_sequence_name(), seq_oper, sequence_id,
                  resolver_->get_stmt()))) {
        LOG_WARN("resolve column item fail", K(sequence_id), K(ret));
      } else if (OB_FAIL(resolver_->add_sequence_id_to_stmt(sequence_id))) {
        LOG_WARN("fail add sequence id to stmt", K(sequence_id), K(ret));
      } else if ((column.get_expr()->is_table_part_key_column() || 
                  column.get_expr()->is_table_part_key_org_column()) && scope == T_INSERT_SCOPE) {
        ObDelUpdStmt *del_upd_stmt = static_cast<ObDelUpdStmt*>(stmt_);
        del_upd_stmt->set_has_part_key_sequence(true);
      }
    }
  }
  return ret;
}

}
}
