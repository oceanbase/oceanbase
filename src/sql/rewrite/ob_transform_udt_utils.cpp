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
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_common_utility.h"
#include "common/ob_smart_call.h"
#include "share/ob_unit_getter.h"
#include "share/schema/ob_column_schema.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_expr_xmlparse.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/config/ob_server_config.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/rewrite/ob_transform_udt_utils.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_insert_all_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "pl/ob_pl_stmt.h"


using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase
{
using namespace common;
namespace sql
{


int ObTransformUdtUtils::transform_xml_value_expr_inner(ObTransformerCtx *ctx, ObDMLStmt *stmt, ObDmlTableInfo &table_info,
                                                        ObRawExpr *&old_expr)
{
  // replace udt column in value expr
  int ret = OB_SUCCESS;
  if (old_expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *col_ref = static_cast<ObColumnRefRawExpr *>(old_expr);
    if (col_ref->is_xml_column()) {
      bool need_transform = false;
      ObArray<ObColumnRefRawExpr *> hidd_cols;
      ObColumnRefRawExpr *hidd_col = NULL;
      if (OB_FAIL(ObTransformUdtUtils::create_udt_hidden_columns(ctx, stmt, *col_ref, hidd_cols, need_transform))) {
        LOG_WARN("failed to create udt hidden exprs", K(ret));
      } else if (!need_transform) {
        // do nothing
      } else if (hidd_cols.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hidden column is null", K(ret));
      } else if (FALSE_IT(hidd_col = hidd_cols.at(0))) {
      } else if (!hidd_col->get_result_type().is_blob()) {
        // if column is not clob type, don't need add xml_binary
        old_expr = hidd_col;
      } else if (OB_FAIL(transform_sys_makexml(ctx, hidd_col, old_expr))) {
        LOG_WARN("transform xml binary failed", K(ret));
      }
    }
  } else {
    for (int i = 0; i < old_expr->get_param_count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(transform_xml_value_expr_inner(ctx, stmt, table_info, old_expr->get_param_expr(i)))) {
        LOG_WARN("transform udt column value expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUdtUtils::transform_make_xml_binary(ObTransformerCtx *ctx, ObRawExpr *old_expr, ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  // to do: add SYS_MAKEXMLBinary
  ObSysFunRawExpr *make_xml_expr = NULL;
  ObConstRawExpr *c_expr = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(ctx->expr_factory_->create_raw_expr(T_FUN_SYS_PRIV_MAKE_XML_BINARY, make_xml_expr))) {
    LOG_WARN("failed to create fun make xml binary expr", K(ret));
  } else if (OB_ISNULL(make_xml_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml expr is null", K(ret));
  } else if (OB_FAIL(ctx->expr_factory_->create_raw_expr(T_INT, c_expr))) {
    LOG_WARN("create dest type expr failed", K(ret));
  } else if (OB_ISNULL(c_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr is null");
  } else {
    ObObj val;
    val.set_int(0);
    c_expr->set_value(val);
    c_expr->set_param(val);
    if (OB_FAIL(make_xml_expr->set_param_exprs(c_expr, old_expr))) {
      LOG_WARN("set param expr fail", K(ret));
    } else {
      make_xml_expr->set_func_name(ObString::make_string("_make_xml_binary"));
      if (OB_FAIL(make_xml_expr->formalize(ctx->session_info_))) {
        LOG_WARN("make xml epxr formlize fail", K(ret));
      }
      new_expr = make_xml_expr;
    }
  }
  return ret;
}

bool ObTransformUdtUtils::check_assign_value_from_same_table(const ObColumnRefRawExpr &udt_col,
                                                               const ObRawExpr &udt_value,
                                                               uint64_t &udt_set_id)
{
  bool ret = false;
  const ObColumnRefRawExpr *src_expr = ObRawExprUtils::get_column_ref_expr_recursively(udt_value.get_param_expr(4));
  if (OB_NOT_NULL(src_expr) && src_expr->get_table_id() == udt_col.get_table_id() &&
      src_expr->get_udt_id() == udt_col.get_udt_id()) {
    ret = true;
    udt_set_id = src_expr->get_udt_set_id();
  }
  return ret;
}

int ObTransformUdtUtils::transform_replace_udt_column_convert_value(ObDmlTableInfo &table_info,
                                                                      ObIArray<ObColumnRefRawExpr*> &column_exprs,
                                                                      uint64_t udt_set_id,
                                                                      ObIArray<ObRawExpr*> &column_conv_exprs)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < column_exprs.count() && OB_SUCC(ret); i++) {
    ObColumnRefRawExpr *left_expr = column_exprs.at(i);
    // get rid of udt column name
    ObString left_attr_name;
    bool found = false;
    for (int j = 0; j < table_info.column_exprs_.count() && OB_SUCC(ret) && !found; j++) {
      if (table_info.column_exprs_.at(j)->get_udt_set_id() == udt_set_id) {
        ObString qualified_name;
        // get rid of udt column name
        ObString right_attr_name = qualified_name.after('.');
        if (left_attr_name.case_compare(right_attr_name) == 0) {
          found = true;
          // found corresponding column
          ObRawExpr *new_value_expr = table_info.column_exprs_.at(j);
          ObRawExpr *old_conv_expr = column_conv_exprs.at(i);
          ObRawExpr *old_col_ref = NULL;
          ObRawExpr *cast_expr = NULL;
          if (new_value_expr->get_result_type().get_type() != left_expr->get_result_type().get_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr result type unexpected", K(ret), K(new_value_expr->get_result_type().get_type()),
                                                            K(left_expr->get_result_type().get_type()));
          } else if (OB_ISNULL(cast_expr = old_conv_expr->get_param_expr(4))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("convert epxr param is null", K(ret));
          } else if (T_FUN_SYS_CAST != cast_expr->get_expr_type()){
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected expr type", K(ret), K(cast_expr->get_expr_type()));
          } else if (FALSE_IT(old_col_ref = old_conv_expr->get_param_expr(4)->get_param_expr(0))) {
          } else if (OB_ISNULL(old_col_ref)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("old expr is null", K(ret));
          } else if (OB_FAIL(ObTransformUtils::replace_expr(cast_expr, new_value_expr, old_conv_expr))) {
            LOG_WARN("failed to replace column convert expr value", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformUdtUtils::transform_udt_column_conv_function(ObTransformerCtx *ctx,
                                                            ObDMLStmt *stmt,
                                                            ObIArray<ObColumnRefRawExpr*> &column_exprs,
                                                            ObIArray<ObRawExpr*> &column_conv_exprs,
                                                            ObColumnRefRawExpr &udt_col,
                                                            ObIArray<ObColumnRefRawExpr *> &hidd_cols,
                                                            ObRawExpr *value_expr)
{
  int ret = OB_SUCCESS;
  if (udt_col.is_xml_column()) {
    ObColumnRefRawExpr &hidd_col = *hidd_cols.at(0);
    if (OB_FAIL(transform_udt_hidden_column_conv_function_inner(ctx, stmt, column_exprs, column_conv_exprs,
                                                                udt_col, hidd_col, 0, OB_INVALID_VERSION,
                                                                value_expr))) {
      LOG_WARN("failed to transform udt column conv inner", K(ret));
    }
  }
  return ret;
}


int ObTransformUdtUtils::replace_udt_assignment_exprs(ObTransformerCtx *ctx, ObDMLStmt *stmt, ObDmlTableInfo &table_info,
                                                      ObIArray<ObAssignment> &assignments, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;

  for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
    ObArray<ObColumnRefRawExpr *>hidd_cols;
    ObRawExpr *value_expr = NULL;
    ObRawExpr *udt_conv_expr = NULL;
    ObAssignment &assign = assignments.at(i);
    bool trigger_exist = false;
    if (OB_ISNULL(assign.column_expr_) || OB_ISNULL(assign.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("assgin expr is null", K(ret));
    } else if (!assign.column_expr_->is_xml_column()) {
      // do nothins
    } else {
      for (int j = 0; j < table_info.column_exprs_.count() && OB_SUCC(ret); j++) {
        if (table_info.column_exprs_.at(j)->get_column_id() != assign.column_expr_->get_column_id() &&
            table_info.column_exprs_.at(j)->get_udt_set_id() == assign.column_expr_->get_udt_set_id()) {
            if (OB_FAIL(hidd_cols.push_back(table_info.column_exprs_.at(j)))) {
              LOG_WARN("failed to push back hidden exprs", K(ret));
            }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (hidd_cols.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hidden column is null", K(ret));
      } else if (assign.expr_->get_expr_type() == T_FUN_SYS_WRAPPER_INNER) {
        trigger_exist = true;
        udt_conv_expr = assign.expr_->get_param_expr(0);
      } else {
        udt_conv_expr = assign.expr_;
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (udt_conv_expr->get_expr_type() != T_FUN_COLUMN_CONV) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpectd expr type", K(ret), K(assign.expr_->get_expr_type()));
      } else if (OB_ISNULL(value_expr = udt_conv_expr->get_param_expr(4))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw expr param is null");
      } else if (value_expr->get_expr_type() == T_FUN_SYS_CAST) {
        // don't need cast expr
        value_expr = value_expr->get_param_expr(0);
      } else if (value_expr->is_const_raw_expr()) {
        if (value_expr->get_expr_type() == T_QUESTIONMARK) {
          const ParamStore &param_store = ctx->exec_ctx_->get_physical_plan_ctx()->get_param_store();
          ObConstRawExpr *param_expr = static_cast<ObConstRawExpr *>(value_expr);
          int64_t param_idx = param_expr->get_value().get_unknown();
          if (param_idx < 0 || param_idx >= param_store.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param_idx is invalid", K(ret), K(param_idx));
          } else if (param_store.at(param_idx).is_xml_sql_type()
                     || param_store.at(param_idx).is_extend_xml_type()){
            // do nothing
          } else if (ob_is_decimal_int_tc(param_store.at(param_idx).get_type())
                    || ob_is_number_tc(param_store.at(param_idx).get_type())) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_WARN("old_expr_type invalid ObLongTextType type", K(ret), K(param_store.at(param_idx).get_type()));
          } else {
            ObExprResType res_type;
            res_type.set_varchar();
            res_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
            value_expr->set_result_type(res_type);
          }
        }
      }
    }
    if (OB_SUCC(ret) && assign.column_expr_->is_xml_column()) {
      ObRawExpr *new_value_expr = NULL;
      ObRawExpr *old_column_expr = assign.column_expr_;
      if (OB_FAIL(transform_xml_value_expr_inner(ctx, stmt, table_info, value_expr))) {
        LOG_WARN("replace udt column failed", K(ret));
      } else if (OB_FAIL(transform_make_xml_binary(ctx, value_expr, new_value_expr))){
        LOG_WARN("failed to transform udt value exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(*ctx->expr_factory_,
                                                                *ctx->allocator_,
                                                                *hidd_cols.at(0),
                                                                new_value_expr,
                                                                ctx->session_info_))) {
        LOG_WARN("fail to build column conv expr", K(ret), K(hidd_cols.at(0)));
      } else {
        trans_happened = true;
        assign.column_expr_ = hidd_cols.at(0);
        if (trigger_exist) {
          if (OB_FAIL(static_cast<ObSysFunRawExpr*>(assign.expr_)->replace_param_expr(0, new_value_expr))) {
            LOG_WARN("failed to replace wrapper expr param", K(ret));
          }
        } else {
          trans_happened = true;
          assign.column_expr_ = hidd_cols.at(0);
          if (trigger_exist) {
            if (OB_FAIL(static_cast<ObSysFunRawExpr*>(assign.expr_)->replace_param_expr(0, new_value_expr))) {
              LOG_WARN("failed to replace wrapper expr param", K(ret));
            }
          } else {
            assign.expr_ = new_value_expr;
          }
        }
      }
      // process returning clause for update
      if (OB_SUCC(ret) && stmt->get_stmt_type() == stmt::T_UPDATE) {
        ObDelUpdStmt *update_stmt = static_cast<ObDelUpdStmt *>(stmt);
        if (update_stmt->is_returning()) {
          for (int64_t i = 0; OB_SUCC(ret) && i < update_stmt->get_returning_exprs().count(); i++) {
            ObRawExpr *sys_makexml_expr = NULL;
            if (OB_FAIL(transform_sys_makexml(ctx, new_value_expr, sys_makexml_expr))) {
              LOG_WARN("fail to create sys_makexml expr", K(ret));
            } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(update_stmt->get_returning_exprs().at(i),
                                                                  old_column_expr,
                                                                  sys_makexml_expr))) {
              LOG_WARN("fail to replace xml column in returning exprs", K(ret), K(i));
            }
          } // end for
        } //end if: process returning clause
      }
    }
  }

  return ret;
}

int ObTransformUdtUtils::transform_udt_assignments(ObTransformerCtx *ctx, ObDMLStmt *stmt, ObDmlTableInfo &table_info, ObColumnRefRawExpr &udt_col,
                                                   ObRawExpr *udt_value, ObArray<ObColumnRefRawExpr*> &hidden_cols, ObIArray<ObAssignment> &assignments,
                                                   ObAssignment &udt_assign)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> col_conv;
  ObArray<ObColumnRefRawExpr *> col_exprs;
  uint64_t src_udt_set_id = 0;
  for (int i = 0; i < hidden_cols.count() && OB_SUCC(ret); i++) {
    ObRawExpr *new_value_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx->expr_factory_, new_value_expr))) {
      LOG_WARN("build null expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(*ctx->expr_factory_,
                                                              *ctx->allocator_,
                                                              *hidden_cols.at(i),
                                                              new_value_expr,
                                                              ctx->session_info_))) {
      LOG_WARN("fail to build column conv expr", K(ret), K(hidden_cols.at(i)));
    } else if (OB_FAIL(col_conv.push_back(new_value_expr))) {
      LOG_WARN("push back udt hidden col conv failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (check_assign_value_from_same_table(udt_col, *udt_value, src_udt_set_id)) {
    if (OB_FAIL(transform_replace_udt_column_convert_value(table_info, hidden_cols, src_udt_set_id, col_conv))) {
      LOG_WARN("transform replace udt hidden col conv value failed", K(ret));
    } else {
      for (int i = 0; i < hidden_cols.count() && OB_SUCC(ret); i++) {
        ObAssignment hidd_assign;
        hidd_assign.column_expr_ = hidden_cols.at(i);
        hidd_assign.expr_ = col_conv.at(i);
        if (OB_FAIL(assignments.push_back(hidd_assign))) {
          LOG_WARN("push back udt hidden col assignment failed", K(ret));
        }
      }
    }
  } else if (FALSE_IT(col_exprs = hidden_cols)) {
  } else if (OB_FAIL(col_exprs.push_back(&udt_col))) {
    LOG_WARN("push back udt col failed", K(ret));
  } else if (OB_FAIL(col_conv.push_back(udt_value))) {
    LOG_WARN("push back udt col conv failed", K(ret));
  } else if (OB_FAIL(transform_udt_column_conv_function(ctx, stmt, col_exprs, col_conv,
                                                        udt_col, hidden_cols))) {
    LOG_WARN("transform udt col conv failed", K(ret));
  } else {
    // last column conv expr is udt col conv
    uint32_t udt_idx = col_conv.count() - 1;
    udt_assign.expr_ = col_conv.at(udt_idx);
    for (int i = 0; i < col_exprs.count() - 1 && OB_SUCC(ret); i++) {
      ObAssignment hidd_assign;
      hidd_assign.column_expr_ = col_exprs.at(i);
      hidd_assign.expr_ = col_conv.at(i);
      if (OB_FAIL(assignments.push_back(hidd_assign))) {
        LOG_WARN("push back udt hidden col assignment failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformUdtUtils::transform_sys_makexml(ObTransformerCtx *ctx, ObRawExpr *hidden_blob_expr, ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *sys_makexml = NULL;
  ObConstRawExpr *c_expr = NULL;
  ObColumnRefRawExpr *hidd_col = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_) || OB_ISNULL(hidden_blob_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(ctx), KP(hidden_blob_expr));
  } else if (OB_FAIL(ctx->expr_factory_->create_raw_expr(T_FUN_SYS_MAKEXML, sys_makexml))) {
    LOG_WARN("failed to create fun sys_makexml expr", K(ret));
  } else if (OB_ISNULL(sys_makexml)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys_makexml expr is null", K(ret));
  } else if (OB_FAIL(ctx->expr_factory_->create_raw_expr(T_INT, c_expr))) {
    LOG_WARN("create dest type expr failed", K(ret));
  } else if (OB_ISNULL(c_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr is null");
  } else {
    ObObj val;
    val.set_int(0);
    c_expr->set_value(val);
    c_expr->set_param(val);
    if (OB_FAIL(sys_makexml->set_param_exprs(c_expr, hidden_blob_expr))) {
      LOG_WARN("set param expr fail", K(ret));
    } else if (FALSE_IT(sys_makexml->set_func_name(ObString::make_string("SYS_MAKEXML")))) {
    } else if (OB_FAIL(sys_makexml->formalize(ctx->session_info_))) {
      LOG_WARN("failed to formalize", K(ret));
    } else {
      new_expr = sys_makexml;
    }
  }
  return ret;
}

int ObTransformUdtUtils::set_hidd_col_not_null_attr(const ObColumnRefRawExpr &udt_col, ObIArray<ObColumnRefRawExpr *> &column_exprs)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t j = 0; OB_SUCC(ret) && !found && j < column_exprs.count(); ++j) {
    ObColumnRefRawExpr *col_hidden = column_exprs.at(j);
    if (col_hidden->get_udt_set_id() == udt_col.get_udt_set_id() &&
        udt_col.get_column_id() != col_hidden->get_column_id()) {
      found = true;
      if (udt_col.get_result_type().has_result_flag(HAS_NOT_NULL_VALIDATE_CONSTRAINT_FLAG)) {
        col_hidden->set_result_flag(HAS_NOT_NULL_VALIDATE_CONSTRAINT_FLAG);
      }
      if (udt_col.get_result_type().has_result_flag(NOT_NULL_WRITE_FLAG)) {
        col_hidden->set_result_flag(NOT_NULL_WRITE_FLAG);
      }
    }
  }
  if (!found) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find hidden column", K(ret), K(udt_col.get_udt_set_id()));
  }
  return ret;
}

int ObTransformUdtUtils::get_dml_view_col_exprs(const ObDMLStmt *stmt, ObIArray<ObColumnRefRawExpr*> &assign_col_exprs)
{
  int ret = OB_SUCCESS;
  if (stmt->get_stmt_type() == stmt::T_UPDATE) {
    const ObUpdateStmt *upd_stmt = static_cast<const ObUpdateStmt*>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < upd_stmt->get_update_table_info().count(); ++i) {
      ObUpdateTableInfo* table_info = upd_stmt->get_update_table_info().at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table info", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < table_info->assignments_.count(); ++j) {
        ObAssignment &assign = table_info->assignments_.at(j);
        if (OB_ISNULL(assign.column_expr_) || OB_ISNULL(assign.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("assgin expr is null", K(ret));
        } else if (assign.column_expr_->is_xml_column() &&
                  OB_FAIL(add_var_to_array_no_dup(assign_col_exprs, assign.column_expr_))) {
          LOG_WARN("failed to push back column expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformUdtUtils::create_udt_hidden_columns(ObTransformerCtx *ctx,
                                                ObDMLStmt *stmt,
                                                const ObColumnRefRawExpr &udt_expr,
                                                ObIArray<ObColumnRefRawExpr*> &col_exprs,
                                                bool &need_transform)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const TableItem *table = NULL;
  const ColumnItem *column_item = NULL;
  ObArray<ObColumnSchemaV2 *> hidden_cols;
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
    if (((table->is_generated_table() || table->is_temp_table()) && OB_NOT_NULL(table->ref_query_))
        || (table->is_json_table() && (OB_NOT_NULL(table->json_table_def_)))) {
      // situation for select  a from (select xmltype('<note3/>') a from dual )
      LOG_INFO("table schema not found for tmp view does not need transfrom", K(ret), KPC(table));
      ret = OB_SUCCESS;
    } else if (table->is_function_table()) {
      // xml table don't do transform
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
              ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(t_col.expr_->get_param_expr(1));
              column_item = stmt->get_column_item_by_base_id(table->table_id_, col_expr->get_column_id());
              if (OB_NOT_NULL(column_item)) {
                if (OB_ISNULL(column_item->expr_)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("column item expr is null", K(ret));
                } else {
                  col_expr = column_item->expr_;
                  need_transform = true;
                }
              } else {
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
              if (OB_SUCC(ret) && OB_FAIL(col_exprs.push_back(col_expr))) {
                LOG_WARN("add column ref to array failed", K(ret));
              }
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
    } else if (udt_expr.is_xml_column() && hidden_cols.count() != 1) {
      // xmltype only 1 hidden column currently
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hidden cols count is not expected", K(table->ref_id_), K(hidden_cols.count()));
    }
    if (OB_SUCC(ret) && !from_xml) {
      for (uint32_t i = 0; i < hidden_cols.count() && OB_SUCC(ret); i++) {
        ObColumnRefRawExpr* col_expr = NULL;
        if (from_base) {
          column_item = stmt->get_column_item_by_base_id(table->table_id_, hidden_cols.at(i)->get_column_id());
        } else {
          column_item = stmt->get_column_item(table->table_id_, hidden_cols.at(i)->get_column_id());
        }

        if (OB_NOT_NULL(column_item)) {
          if (OB_ISNULL(column_item->expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column item expr is null", K(ret));
          } else {
            col_expr = column_item->expr_;
            need_transform = true;
          }
        } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*ctx->expr_factory_, *hidden_cols.at(i), col_expr))) {
          LOG_WARN("build column expr failed", K(ret));
        } else if (OB_ISNULL(col_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to create raw expr for dummy output", K(ret));
        } else {
          col_expr->set_table_id(table->table_id_);
          col_expr->set_explicited_reference();
          col_expr->set_column_attr(udt_expr.get_table_name(), col_expr->get_column_name());
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
        if (OB_SUCC(ret) && OB_FAIL(col_exprs.push_back(col_expr))) {
          LOG_WARN("add column ref to array failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformUdtUtils::get_update_generated_udt_in_parent_stmt(const ObIArray<ObParentDMLStmt> &parent_stmts,
                                                                   const ObDMLStmt *stmt,
                                                                   ObIArray<ObColumnRefRawExpr*> &col_exprs)
{
  int ret = OB_SUCCESS;

  const ObDMLStmt *root_stmt = NULL;
  ObSEArray<ObColumnRefRawExpr*, 8> parent_col_exprs;
  const ObSelectStmt *sel_stmt = NULL;
  const ObDMLStmt *parent_stmt = NULL;
  int64_t table_id = OB_INVALID;
  bool is_valid = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!stmt->is_select_stmt() || parent_stmts.empty()) {
    //do nothing
  } else if (OB_FALSE_IT(sel_stmt = static_cast<const ObSelectStmt*>(stmt))) {
  } else if (OB_ISNULL(root_stmt = parent_stmts.at(parent_stmts.count()-1).stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get parent stmt", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_parent_stmt(root_stmt, stmt, parent_stmt, table_id, is_valid))) {
    LOG_WARN("failed to get parent stmt", K(ret));
  } else if (!is_valid) {
    //do nothing
  } else if (OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(get_dml_view_col_exprs(parent_stmt, parent_col_exprs))) {
    LOG_WARN("failed to get assignment columns", K(ret));
  } else if (!parent_col_exprs.empty()) {
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
        } else if (OB_FAIL(add_var_to_array_no_dup(col_exprs, col_expr))) {
          LOG_WARN("failed to push back column expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformUdtUtils::check_skip_child_select_view(const ObIArray<ObParentDMLStmt> &parent_stmts,
                                                          ObDMLStmt *stmt, bool &skip_for_view_table)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *sel_stmt = NULL;
  const ObDMLStmt *parent_stmt = NULL;
  bool is_valid = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!stmt->is_select_stmt() || parent_stmts.count() != 1 ||
             stmt->get_table_size() != 1) {
    //do nothing
  } else if (OB_FALSE_IT(sel_stmt = static_cast<const ObSelectStmt*>(stmt))) {
  } else if (OB_ISNULL(parent_stmt = parent_stmts.at(0).stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get parent stmt", K(ret));
  } else if (parent_stmt->get_table_size() != 1 ||
             !(parent_stmt->is_delete_stmt() || parent_stmt->is_update_stmt())) {
    // do nothing
  } else {
    const sql::TableItem *basic_table_item = stmt->get_table_item(0);
    const sql::TableItem *view_table_item = parent_stmt->get_table_item(0);
    if (OB_ISNULL(basic_table_item) || OB_ISNULL(view_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table item", K(ret));
    } else if (basic_table_item->is_basic_table() && view_table_item->is_generated_table()) {
      if (OB_NOT_NULL(view_table_item->ref_query_) &&
          view_table_item->ref_query_->get_table_size() > 0 &&
          OB_NOT_NULL(view_table_item->ref_query_->get_table_item(0)) &&
          basic_table_item->table_id_ == view_table_item->ref_query_->get_table_item(0)->table_id_) {
        skip_for_view_table = true;
      }
    } else if (!basic_table_item->is_basic_table() || !view_table_item->is_view_table_) {
      // do nothing
    } else if (OB_ISNULL(view_table_item->view_base_item_)) {
      // do nothing
    } else if (basic_table_item->table_id_ == view_table_item->view_base_item_->table_id_) {
      skip_for_view_table = true;
    }
  }
  return ret;
}

int ObTransformUdtUtils::transform_query_udt_columns_exprs(ObTransformerCtx *ctx, const ObIArray<ObParentDMLStmt> &parent_stmts,
                                                           ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<DmlStmtScope, 4> scopes;
  ObSEArray<ObRawExpr*, 4> replace_exprs;
  ObSEArray<ObRawExpr*, 4> from_col_exprs;
  ObSEArray<ObRawExpr*, 4> to_col_exprs;
  ObSEArray<ObColumnRefRawExpr*, 8> parent_assign_xml_col_exprs;
  bool skip_for_view_table = false;

  if (OB_FAIL(get_update_generated_udt_in_parent_stmt(parent_stmts, stmt, parent_assign_xml_col_exprs))) {
    LOG_WARN("Fail to get update generated column array.", K(ret));
  } else if (OB_FAIL(check_skip_child_select_view(parent_stmts, stmt, skip_for_view_table))) {
    LOG_WARN("Fail to check if select in delete view.", K(ret));
  } else if (skip_for_view_table) {
    // do nothing
  } else {
    FastUdtExprChecker expr_checker(replace_exprs);
    if (OB_FAIL(scopes.push_back(SCOPE_DML_COLUMN)) ||
        (stmt->get_stmt_type() != stmt::T_MERGE && OB_FAIL(scopes.push_back(SCOPE_DML_VALUE))) ||
        OB_FAIL(scopes.push_back(SCOPE_DML_CONSTRAINT)) ||
        OB_FAIL(scopes.push_back(SCOPE_INSERT_DESC)) ||
        OB_FAIL(scopes.push_back(SCOPE_BASIC_TABLE)) ||
        OB_FAIL(scopes.push_back(SCOPE_DICT_FIELDS)) ||
        OB_FAIL(scopes.push_back(SCOPE_SHADOW_COLUMN)) ||
        ((stmt->get_stmt_type() == stmt::T_INSERT || stmt->get_stmt_type() == stmt::T_UPDATE) &&
          OB_FAIL(scopes.push_back(SCOPE_RETURNING))) ||
        (stmt->get_stmt_type() != stmt::T_MERGE && OB_FAIL(scopes.push_back(SCOPE_INSERT_VECTOR)))) {
      LOG_WARN("Fail to create scope array.", K(ret));
    }
    ObStmtExprGetter visitor;
    visitor.checker_ = &expr_checker;
    visitor.remove_scope(scopes);
    if (OB_SUCC(ret) && OB_FAIL(stmt->iterate_stmt_expr(visitor))) {
      LOG_WARN("get relation exprs failed", K(ret));
    }

    //collect query udt exprs which need to be replaced
    for (int64_t i = 0; OB_SUCC(ret) && i < replace_exprs.count(); i++) {
      if (OB_ISNULL(replace_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replace expr is null", K(ret));
      } else {
        ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(replace_exprs.at(i));
        ObArray<ObColumnRefRawExpr *> hidd_cols;
        ObRawExpr *sys_makexml_expr = NULL;
        bool need_transform = false;
        if (!col_expr->is_xml_column() || has_exist_in_array(parent_assign_xml_col_exprs, col_expr)) {
          // do nothing
        } else if (OB_FAIL(ObTransformUdtUtils::create_udt_hidden_columns(ctx, stmt, *col_expr, hidd_cols, need_transform))) {
          LOG_WARN("failed to create udt hidden exprs", K(ret));
        } else if (need_transform == false) {
          // do nothing
        } else if (OB_FAIL(transform_sys_makexml(ctx, hidd_cols.at(0), sys_makexml_expr))) {
          LOG_WARN("failed to transform make_xml exprs", K(ret));
        } else if (OB_FAIL(from_col_exprs.push_back(col_expr))) {
          LOG_WARN("failed to push back udt exprs", K(ret));
        } else if (OB_FAIL(to_col_exprs.push_back(sys_makexml_expr))) {
          LOG_WARN("failed to push back udt hidden exprs", K(ret));
        }
      }

      //do replace
      if (OB_SUCC(ret) && !from_col_exprs.empty()) {
        ObStmtExprReplacer replacer;
        replacer.remove_scope(scopes);
        if (OB_FAIL(replacer.add_replace_exprs(from_col_exprs, to_col_exprs))) {
          LOG_WARN("failed to add replace exprs", K(ret));
        } else if (OB_FAIL(stmt->iterate_stmt_expr(replacer))) {
          LOG_WARN("failed to iterate stmt expr", K(ret));
        } else {
          trans_happened = true;
        }
      }
    }
  }

  return ret;
}


int ObTransformUdtUtils::transform_udt_columns_constraint_exprs(ObTransformerCtx *ctx, ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<DmlStmtScope, 4> scopes;
  ObSEArray<ObRawExpr*, 4> replace_exprs;
  ObSEArray<ObRawExpr*, 4> from_col_exprs;
  ObSEArray<ObRawExpr*, 4> to_col_exprs;
  FastUdtExprChecker expr_checker(replace_exprs);
  ObStmtExprGetter visitor;
  visitor.remove_all();
  visitor.add_scope(SCOPE_DML_CONSTRAINT);
  visitor.checker_ = &expr_checker;

  if (OB_SUCC(ret) && OB_FAIL(stmt->iterate_stmt_expr(visitor))) {
    LOG_WARN("get relation exprs failed", K(ret));
  }

  //collect query udt exprs which need to be replaced
  for (int64_t i = 0; OB_SUCC(ret) && i < replace_exprs.count(); i++) {
    if (OB_ISNULL(replace_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replace expr is null", K(ret));
    } else {
      ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(replace_exprs.at(i));
      ObArray<ObColumnRefRawExpr *> hidd_cols;
      ObRawExpr *sys_makexml_expr = NULL;
      bool need_transform = false;
      if (!col_expr->is_xml_column()) {
        // do nothing
      } else if (OB_FAIL(ObTransformUdtUtils::create_udt_hidden_columns(ctx,
                                                                        stmt,
                                                                        *col_expr,
                                                                        hidd_cols,
                                                                        need_transform))) {
        LOG_WARN("failed to create udt hidden exprs", K(ret));
      } else if (need_transform == false) {
        // do nothing;
      } else if (OB_FAIL(transform_sys_makexml(ctx, hidd_cols.at(0), sys_makexml_expr))) {
        LOG_WARN("failed to transform make_xml exprs", K(ret));
      } else if (OB_FAIL(from_col_exprs.push_back(col_expr))) {
        LOG_WARN("failed to push back udt exprs", K(ret));
      } else if (OB_FAIL(to_col_exprs.push_back(sys_makexml_expr))) {
        LOG_WARN("failed to push back udt hidden exprs", K(ret));
      }
    }
  }

  //do replace
  if (OB_SUCC(ret) && !from_col_exprs.empty()) {
    ObStmtExprReplacer replacer;
    replacer.remove_all();
    replacer.add_scope(SCOPE_DML_CONSTRAINT);
    if (OB_FAIL(replacer.add_replace_exprs(from_col_exprs, to_col_exprs))) {
      LOG_WARN("failed to add replace exprs", K(ret));
    } else if (OB_FAIL(stmt->iterate_stmt_expr(replacer))) {
      LOG_WARN("failed to iterate stmt expr", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformUdtUtils::transform_udt_hidden_column_conv_function_inner(ObTransformerCtx *ctx,
                                                                         ObDMLStmt *stmt,
                                                                         ObIArray<ObColumnRefRawExpr*> &column_exprs,
                                                                         ObIArray<ObRawExpr*> &column_conv_exprs,
                                                                         ObColumnRefRawExpr &udt_col,
                                                                         ObColumnRefRawExpr &targe_col,
                                                                         uint32_t attr_idx,
                                                                         int64_t schema_version,
                                                                         ObRawExpr *udt_value)
{
  int ret = OB_SUCCESS;
  bool hidd_found = false;
  for (int64_t j = 0; OB_SUCC(ret) && !hidd_found && j < column_exprs.count(); ++j) {
    ObColumnRefRawExpr *col = column_exprs.at(j);
    if (col->get_column_id() == targe_col.get_column_id()) {
      ObRawExpr *old_conv_expr = column_conv_exprs.at(j);
      ObRawExpr *old_col_ref = NULL;
      ObRawExpr *cast_expr = NULL;
      if (udt_col.is_xml_column()) {
        if (OB_NOT_NULL(udt_value)) {
          old_col_ref = udt_value;
        } else {
          const ObColumnRefRawExpr *tmp = ObRawExprUtils::get_column_ref_expr_recursively(old_conv_expr->get_param_expr(4));
          old_col_ref = const_cast<ObRawExpr *>(static_cast<const ObRawExpr *>(tmp));
        }
      } else if (OB_NOT_NULL(cast_expr = old_conv_expr->get_param_expr(4))) {
        if (T_FUN_SYS_CAST != cast_expr->get_expr_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected expr type", K(ret), K(cast_expr->get_expr_type()));
        } else {
          old_col_ref = old_conv_expr->get_param_expr(4)->get_param_expr(0);
        }
      }
      hidd_found = true;
      ObRawExpr *new_value_expr = NULL;
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(old_col_ref)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("old expr is null", K(ret));
      } else if (udt_col.is_xml_column()) {
        if (OB_FAIL(transform_make_xml_binary(ctx, old_col_ref, new_value_expr))) {
          LOG_WARN("failed to transform udt value exprs", K(ret));
        } else if (OB_FAIL(static_cast<ObSysFunRawExpr*>(old_conv_expr)->replace_param_expr(4, new_value_expr))) {
          LOG_WARN("failed to push back udt hidden exprs", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformUdtUtils::transform_returning_exprs(ObTransformerCtx *ctx, ObDelUpdStmt *stmt, ObInsertTableInfo *table_info)
{
  int ret = OB_SUCCESS;
  if (stmt->is_returning()) {
    ObIArray<ObRawExpr*> &column_convert = table_info->column_conv_exprs_;
    const ObIArray<ObColumnRefRawExpr *> &table_columns = table_info->column_exprs_;
    ObSEArray<std::pair<int64_t, int64_t>, 8> xml_col_idxs; // use pair to store xml col idx and its hidden col idx
    for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.count(); i++) {
      ObColumnRefRawExpr *ref_col = table_columns.at(i);
      if (ref_col->is_xml_column()) {
        bool is_found = false;
        for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < table_columns.count(); j++) {
          if (ref_col->get_column_id() != table_columns.at(j)->get_column_id() &&
            ref_col->get_udt_set_id() == table_columns.at(j)->get_udt_set_id()) {
            is_found = true;
            xml_col_idxs.push_back(std::make_pair(i, j));
          }
        } // end for
        if (!is_found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to find hidden column", K(ret), K(i));
        }
      }
    } // end for

    CK(column_convert.count() == table_columns.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_returning_exprs().count(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < xml_col_idxs.count(); j++) {
        ObRawExpr *hidd_col = NULL;
        ObRawExpr *sys_makexml_expr = NULL;
        if (OB_ISNULL(hidd_col = column_convert.at(xml_col_idxs.at(j).second))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("hidden col is NULL", K(ret), K(i), K(j));
        } else if (OB_FAIL(ObTransformUdtUtils::transform_sys_makexml(ctx, hidd_col, sys_makexml_expr))) {
          LOG_WARN("fail to create expr sys_makexml", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(stmt->get_returning_exprs().at(i),
                                                              table_columns.at(xml_col_idxs.at(j).first),
                                                              sys_makexml_expr))) {
          LOG_WARN("fail to replace xml column in returning exprs", K(ret), K(i), K(j));
        }
      }
    }
  }
  return ret;
}

bool ObTransformUdtUtils::is_in_values_desc(const uint64_t column_id, const ObInsertTableInfo &table_info, uint64_t &idx)
{
  bool bRet = false;
  ObColumnRefRawExpr *expr = nullptr;
  for (int64_t i = 0; !bRet && i < table_info.values_desc_.count(); i++) {
    ObColumnRefRawExpr *expr = table_info.values_desc_.at(i);
    if (column_id == expr->get_column_id()) {
      bRet = true;
      idx = i;
    }
  }
  return bRet;
}

int ObTransformUdtUtils::transform_udt_dml_stmt(ObTransformerCtx *ctx, ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  stmt::StmtType type = stmt->get_stmt_type();
  common::ObArray<ObDmlTableInfo*> table_info_assign;
  common::ObArray<ObInsertTableInfo*> table_info_insert;

  if (type == stmt::T_UPDATE) {
    ObUpdateStmt *upd_stmt = static_cast<ObUpdateStmt *>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < upd_stmt->get_update_table_info().count(); ++i) {
      if (OB_FAIL(table_info_assign.push_back(upd_stmt->get_update_table_info().at(i)))) {
        LOG_WARN("failed to push table info", K(ret));
      }
    }
  } else if (type == stmt::T_MERGE) {
    ObMergeStmt *merge_stmt = static_cast<ObMergeStmt *>(stmt);
    if (OB_FAIL(table_info_assign.push_back(&merge_stmt->get_merge_table_info()))) {
      LOG_WARN("failed to push table info", K(ret));
    } else if (OB_FAIL(table_info_insert.push_back(&merge_stmt->get_merge_table_info()))) {
      LOG_WARN("failed to push table info", K(ret));
    }
  } else if (type == stmt::T_INSERT) {
    ObInsertStmt *insert_stmt = static_cast<ObInsertStmt*>(stmt);
    if (OB_FAIL(table_info_insert.push_back(&insert_stmt->get_insert_table_info()))) {
      LOG_WARN("failed to push table info", K(ret));
    }
  } else if (type == stmt::T_INSERT_ALL) {
    ObInsertAllStmt *insert_all_stmt = static_cast<ObInsertAllStmt*>(stmt);
    ObIArray<ObInsertAllTableInfo*> &table_infos = insert_all_stmt->get_insert_all_table_info();
    for (int64_t i = 0; i < table_infos.count(); i++) {
      if (OB_FAIL(table_info_insert.push_back(table_infos.at(i)))) {
        LOG_WARN("failed to push table info", K(ret));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_assign.count(); ++i) {
    bool is_happened = false;
    ObDmlTableInfo* table_info = table_info_assign.at(i);
    if (OB_ISNULL(table_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table info", K(ret));
    } else if (OB_FAIL(replace_udt_assignment_exprs(ctx, stmt, *table_info,
                                                    type == stmt::T_UPDATE ?
                                                    static_cast<ObUpdateTableInfo*>(table_info)->assignments_ :
                                                    static_cast<ObMergeTableInfo*>(table_info)->assignments_,
                                                    is_happened))) {
      LOG_WARN("failed to replace assignment exprs", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to do const propagation for assignment expr", K(is_happened));
    }
  }

  for (int64_t count = 0; OB_SUCC(ret) && count < table_info_insert.count(); ++count) {
    ObInsertTableInfo *table_info = table_info_insert.at(count);
    for (int64_t i = 0; OB_SUCC(ret) && i < table_info->column_exprs_.count(); ++i) {
      ObColumnRefRawExpr *col = table_info->column_exprs_.at(i);
      if (col->is_xml_column()) {
        if (OB_FAIL(set_hidd_col_not_null_attr(*col, table_info->column_exprs_))) {
          LOG_WARN("failed to set hidden column not null attr", K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < table_info->column_exprs_.count() && i < table_info->column_conv_exprs_.count(); ++i) {
      ObRawExpr *value_expr = NULL;
      uint64_t value_idx = 0;
      ObColumnRefRawExpr *col = table_info->column_exprs_.at(i);
      bool is_in_values = is_in_values_desc(col->get_column_id(), *table_info, value_idx);
      if (col->is_xml_column() && is_in_values) {
        ObArray<ObColumnRefRawExpr *> hidd_cols;
        bool need_transform = false;
        if (OB_FAIL(ObTransformUdtUtils::create_udt_hidden_columns(ctx, stmt, *col, hidd_cols, need_transform))) {
          LOG_WARN("failed to create udt hidden exprs", K(ret));
        } else if (need_transform == false) {
          // do nothing
        } else if (type == stmt::T_MERGE && is_in_values &&
                   FALSE_IT(value_expr = table_info->values_vector_.at(value_idx))) {
        } else if (OB_FAIL(transform_udt_column_conv_function(ctx, stmt,
                                                              table_info->column_exprs_,
                                                              table_info->column_conv_exprs_,
                                                              *col, hidd_cols, value_expr))) {
          LOG_WARN("failed to push back column conv exprs", K(ret));
        }
      }
    }
    // process returning exprs
    if (OB_SUCC(ret) && OB_FAIL(transform_returning_exprs(ctx, static_cast<ObDelUpdStmt*>(stmt), table_info))) {
      LOG_WARN("failed to transform returning exprs", K(ret));
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase