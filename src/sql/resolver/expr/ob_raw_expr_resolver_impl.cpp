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
#include "ob_raw_expr_resolver_impl.h"
#include "common/ob_smart_call.h"
#include "common/ob_common_utility.h"
#include "ob_raw_expr_info_extractor.h"
#include "sql/parser/sql_parser_base.h"
#include "sql/resolver/ob_column_ref.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/parser/ob_parser_utils.h"
#include "sql/engine/expr/ob_expr_case.h"
#include "lib/json/ob_json_print_utils.h"
#include "lib/string/ob_sql_string.h"
#include "pl/ob_pl_type.h"
#include "pl/ob_pl_resolver.h"
#include "common/ob_smart_call.h"
#include "share/schema/ob_udt_info.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/ob_pl_udt_object_manager.h"
#endif
#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "deps/oblib/src/lib/json_type/ob_json_path.h"
#include "share/resource_manager/ob_resource_manager.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObRawExprResolverImpl::ObRawExprResolverImpl(ObExprResolveContext &ctx)
    : ctx_(ctx),
      is_contains_assignment_(false)
{
}

int ObRawExprResolverImpl::resolve(const ParseNode *node,
                                   ObRawExpr *&expr,
                                   ObIArray<ObQualifiedName> &columns,
                                   ObIArray<ObVarInfo> &sys_vars,
                                   ObIArray<ObSubQueryInfo> &sub_query_info,
                                   ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                                   ObIArray<ObWinFunRawExpr*> &win_exprs,
                                   ObIArray<ObUDFInfo> &udf_info,
                                   ObIArray<ObOpRawExpr*> &op_exprs,
                                   ObIArray<ObUserVarIdentRawExpr*> &user_var_exprs)
{
  ctx_.columns_ = &columns;
  ctx_.op_exprs_ = &op_exprs;
  ctx_.sys_vars_ = &sys_vars;
  ctx_.sub_query_info_ = &sub_query_info;
  ctx_.aggr_exprs_ = &aggr_exprs;
  ctx_.win_exprs_ = &win_exprs;
  ctx_.udf_info_ = &udf_info;
  ctx_.user_var_exprs_ = &user_var_exprs;
  int ret = recursive_resolve(node, expr);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr->extract_info())) {
      LOG_WARN("failed to extract info", K(ret), K(*expr));
    }
  }
  return ret;
}

int ObRawExprResolverImpl::get_opposite_string(const common::ObString &orig_string, common::ObString &new_string, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char *tmp_buf = NULL;
  if (OB_UNLIKELY(orig_string.empty())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid argument", K(ret));
  } else {
    if ('-' == orig_string[0]) {
      new_string.assign_ptr(orig_string.ptr() + 1, orig_string.length() - 1);
    } else {
      if (OB_ISNULL(tmp_buf = (char*)(allocator.alloc(orig_string.length() + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "failed to allocate memory", "size", orig_string.length() + 1, K(ret));
      } else {
        tmp_buf[0] = '-';
        MEMCPY(tmp_buf + 1, orig_string.ptr(), orig_string.length());
        new_string.assign_ptr(tmp_buf, orig_string.length() + 1);
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::try_negate_const(ObRawExpr *&expr,
                                            const int64_t neg_cnt,
                                            int64_t &remain_neg_cnt)
{
  int ret = OB_SUCCESS;
  remain_neg_cnt = neg_cnt;
  bool neg_const = false;
  const bool is_odd = neg_cnt % 2 == 1;
  if (neg_cnt > 0 && NULL != expr && expr->is_const_raw_expr()) {
    ObConstRawExpr *const_expr = static_cast<ObConstRawExpr *>(expr);
    const ObString &orig_literal_prefix = const_expr->get_literal_prefix();
    neg_const = true;
    switch (const_expr->get_expr_type()) {
      case T_INT: {
        int64_t val = OB_INVALID_ID;
        if (OB_SUCC(const_expr->get_value().get_int(val))) {
          if (INT64_MIN == val) {
            // -val will overflow if val is INT64_MIN, we must use neg expr to handle this.
            neg_const = false;
          } else if (is_odd) {
            ObObj new_val;
            new_val.set_int(-val);
            const_expr->set_expr_type(T_INT);
            const_expr->set_value(new_val);
            const_expr->set_precision(static_cast<ObPrecision>(
                                      const_expr->get_accuracy().get_precision() + 1));
          }
        }
        break;
      }
      case T_FLOAT: {
        float val = static_cast<float>(OB_INVALID_ID);
        if (is_odd && OB_SUCC(const_expr->get_value().get_float(val))) {
          if (!isnan(val)) { // OB only store nan, don't add negative nan
            ObObj new_val;
            new_val.set_float(-val);
            new_val.set_scale(const_expr->get_accuracy().get_scale());
            const_expr->set_expr_type(T_FLOAT);
            const_expr->set_value(new_val);
            const_expr->set_precision(static_cast<ObPrecision>(const_expr->get_accuracy().get_precision() + 1));
          }
        }
        break;
      }
      case T_DOUBLE: {
        double val = static_cast<double>(OB_INVALID_ID);
        if (is_odd && OB_SUCC(const_expr->get_value().get_double(val))) {
          if (!isnan(val)) { // OB only store nan, don't add negative nan
            ObObj new_val;
            new_val.set_double(-val);
            new_val.set_scale(const_expr->get_accuracy().get_scale());
            const_expr->set_expr_type(T_DOUBLE);
            const_expr->set_value(new_val);
            const_expr->set_precision(static_cast<ObPrecision>(const_expr->get_accuracy().get_precision() + 1));
          }
        }
        break;
      }
      case T_NUMBER: {
        number::ObNumber val;
        number::ObNumber negative_val;
        if (is_odd && OB_SUCC(const_expr->get_value().get_number(val))
            && OB_SUCC(val.negate(negative_val, ctx_.expr_factory_.get_allocator()))) {
          ObObj new_val;
          new_val.set_number(negative_val);
          new_val.set_scale(const_expr->get_accuracy().get_scale());
          const_expr->set_expr_type(T_NUMBER);
          const_expr->set_value(new_val);
          if (lib::is_oracle_mode()) {
            const_expr->set_precision(static_cast<ObPrecision>(const_expr->get_accuracy().get_precision()));
          } else {
            const_expr->set_precision(static_cast<ObPrecision>(const_expr->get_accuracy().get_precision() + 1));
          }
        }
        break;
      }
      case T_NUMBER_FLOAT: {
        number::ObNumber val;
        number::ObNumber negative_val;
        if (is_odd && OB_SUCC(const_expr->get_value().get_number_float(val))
            && OB_SUCC(val.negate(negative_val, ctx_.expr_factory_.get_allocator()))) {
          ObObj new_val;
          new_val.set_number_float(negative_val);
          const_expr->set_expr_type(T_NUMBER_FLOAT);
          const_expr->set_value(new_val);
          const_expr->set_precision(static_cast<ObPrecision>(const_expr->get_accuracy().get_precision()));
        }
        break;
      }
      default: {
        neg_const = false;
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (!orig_literal_prefix.empty() && neg_const && is_odd) {
        ObString new_literal_prefix;
        if (OB_FAIL(get_opposite_string(orig_literal_prefix, new_literal_prefix, ctx_.expr_factory_.get_allocator()))) {
          SQL_LOG(WARN, "fail to get_opposite_string", KPC(const_expr), K(ret));
        } else {
          const_expr->set_literal_prefix(new_literal_prefix);
        }
      }
      SQL_LOG(DEBUG, "finish try_negate_const",
              KPC(expr), K(ret), K(neg_cnt), K(neg_const), K(remain_neg_cnt));
    }
  }
  if (OB_FAIL(ret)) {
    // ObConstRawExpr::type_ and ObConstRawExpr::value_ type may mismatch, value_ may be number
    // wile type_ is T_INT. This will cause get value error, ignore it.
    ret = OB_SUCCESS;
    neg_const = false;
  }
  if (0 == neg_cnt || neg_const) {
    remain_neg_cnt = 0;
  } else {
    // Reduce negative count to 2 for even number, 3 for odd number. Why not 1 or 2:
    //
    //   c1: bigint unsigned, values is 9223372036854775808
    //   -c1: -9223372036854775808
    //   --c1: report out of range error
    //   ---c1: report out of range error
    //
    remain_neg_cnt = is_odd ? (neg_cnt > 2 ? 3 : 1) : 2;
  }
  return ret;
}

int ObRawExprResolverImpl::recursive_resolve(const ParseNode *node, ObRawExpr *&expr)
{
  return SMART_CALL(do_recursive_resolve(node, expr));
}

int ObRawExprResolverImpl::do_recursive_resolve(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  expr = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to do stack overflow check", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow", K(ret));
  } else {
    LOG_DEBUG("resolve item", "item_type", get_type_name(node->type_));
    if (IS_DATATYPE_OR_QUESTIONMARK_OP(node->type_)) {
      if (OB_FAIL(process_datatype_or_questionmark(*node, expr))) {
        LOG_WARN("fail to process datatype or questionmark", K(ret), K(node));
      }
    } else {
      switch (node->type_) {
      case T_DEFAULT: {
        ObConstRawExpr *c_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, c_expr))) {
          LOG_WARN("fail to create raw expr", K(ret));
        } else {
          expr = c_expr;
        }
        break;
      }
      case T_DEFAULT_INT: {
        ObConstRawExpr *c_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_INT, c_expr))) {
          LOG_WARN("fail to create raw expr", K(ret));
        } else if (OB_ISNULL(c_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("const expr is null");
        } else {
          ObObj val;
          val.set_int(node->value_);
          c_expr->set_value(val);
          c_expr->set_param(val);
          expr = c_expr;
        }
        break;
      }
      case T_SFU_DOUBLE: {
        ParseNode *tmp_node = const_cast<ParseNode *>(node);
        tmp_node->type_ = T_DOUBLE;
        if (OB_FAIL(process_datatype_or_questionmark(*tmp_node, expr))) {
          LOG_WARN("fail to process datetype or questionmark", K(ret), K(tmp_node));
        } else {/*do nothing*/}
        break;
      }
      case T_CAST_ARGUMENT: {
        ObConstRawExpr *c_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_INT, c_expr))) {
          LOG_WARN("fail to create raw expr", K(ret));
        } else if (OB_ISNULL(c_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("const expr is null");
        } else {
          ObObj val;
          ObObjType data_type = static_cast<ObObjType>(node->int16_values_[OB_NODE_CAST_TYPE_IDX]);
          if (ob_is_string_tc(data_type)) {
            int32_t len = node->int32_values_[1];
            if (lib::is_oracle_mode()) {
              if (OB_UNLIKELY(0 == len)) {
                ret = OB_ERR_ZERO_LEN_COL;
                LOG_WARN("Oracle not allowed zero length", K(ret));
              } else if (((ObVarcharType == data_type || ObNVarchar2Type == data_type) && OB_MAX_ORACLE_VARCHAR_LENGTH < len)
                          || ((ObCharType == data_type || ObNCharType == data_type) && OB_MAX_ORACLE_CHAR_LENGTH_BYTE < len)) {
                ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
                LOG_WARN("column data length is invalid", K(ret), K(len), K(data_type));
                LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, "cast ",
                   static_cast<int>((ObVarcharType == data_type || ObNVarchar2Type == data_type)
                       ? OB_MAX_ORACLE_VARCHAR_LENGTH : OB_MAX_ORACLE_CHAR_LENGTH_BYTE));
              } else if (node->length_semantics_ == LS_DEFAULT) {
                const ObLengthSemantics default_length_semantics = (OB_NOT_NULL(ctx_.session_info_) ? ctx_.session_info_->get_actual_nls_length_semantics() : LS_BYTE);
                c_expr->set_length_semantics(default_length_semantics);
              } else if (OB_UNLIKELY(node->length_semantics_ != LS_BYTE && node->length_semantics_ != LS_CHAR)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("length_semantics_ is invalid", K(ret), K(node->length_semantics_));
              } else {
                c_expr->set_length_semantics(node->length_semantics_);
              }
            }
            if (OB_SUCC(ret) && OUT_OF_STR_LEN == len) {
              ret = OB_ERR_TOO_BIG_DISPLAYWIDTH;
              int64_t max_str_len = static_cast<int64_t>(UINT32_MAX);
              LOG_USER_ERROR(OB_ERR_TOO_BIG_DISPLAYWIDTH, "cast of char", max_str_len);
            }
          } else if (ob_is_raw(data_type)) {
            int32_t len = node->int32_values_[1];
            if (lib::is_oracle_mode()) {
              if (len > OB_MAX_ORACLE_RAW_PL_VAR_LENGTH || len < 0) {
                ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
                LOG_WARN("column data length is invalid", K(ret), K(len), K(data_type));
                LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH,
                               "cast ", static_cast<int>(OB_MAX_ORACLE_RAW_PL_VAR_LENGTH));
              } else if (0 == len) {
                ret = OB_ERR_ZERO_LEN_COL;
                LOG_WARN("column data length cannot be zero on oracle mode", K(ret), K(len));
              }
            }
          } else if (ob_is_extend(data_type)) {
            if (lib::is_mysql_mode()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("extend type in mysql is invalid", K(ret));
            } else {
              const ParseNode *name_node = node->children_[0];
              CK (OB_NOT_NULL(ctx_.session_info_),  OB_NOT_NULL(ctx_.schema_checker_));
              CK (OB_NOT_NULL(name_node));
              CK (T_SP_TYPE == name_node->type_);
              if (OB_SUCC(ret)) {
                uint64_t udt_id = OB_INVALID_ID;
                uint64_t db_id = ctx_.session_info_->get_database_id();
                ObString udt_name(name_node->children_[1]->str_len_, name_node->children_[1]->str_value_);

                if (NULL != name_node->children_[0]) {
                  OZ (ctx_.schema_checker_->get_database_id(ctx_.session_info_->get_effective_tenant_id(),
                                                    ObString(name_node->children_[0]->str_len_, name_node->children_[0]->str_value_),
                                                    db_id));
                }
                OZ (ctx_.schema_checker_->get_udt_id(ctx_.session_info_->get_effective_tenant_id(), db_id, OB_INVALID_ID,
                               ObString(name_node->children_[1]->str_len_, name_node->children_[1]->str_value_), udt_id));

                if (OB_SUCC(ret) && OB_INVALID_ID == udt_id) {
                  if(OB_FAIL(ctx_.schema_checker_->get_udt_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID, OB_INVALID_ID,
                             udt_name, udt_id))) {
                    LOG_WARN("get udt id from sys fail", K(ret), K(udt_name));
                  }
                }

                if (OB_SUCC(ret)) {
                  if (OB_INVALID_ID == udt_id) {
                    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
                    LOG_WARN("Invalid type to cast", K(ObString(name_node->children_[1]->str_len_, name_node->children_[1]->str_value_)), K(ret));
                  } else {
                    c_expr->set_udt_id(udt_id);
                  }
                }

                if (OB_SUCC(ret) && NULL != ctx_.stmt_) {
                  ObStmt *stmt = ctx_.stmt_;
                  uint64_t tenant_id = pl::get_tenant_id_by_object_id(udt_id);
                  const ObUDTTypeInfo *udt_info = NULL;
                  if (OB_FAIL(ctx_.schema_checker_->get_udt_info(tenant_id, udt_id, udt_info))) {
                    LOG_WARN("failed to get udt info", K(ret));
                  } else if (OB_ISNULL(udt_info)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("get null udt info", K(ret));
                  } else if (udt_info->get_schema_version() != common::OB_INVALID_VERSION) {
                    ObSchemaObjVersion udt_schema_version;
                    udt_schema_version.object_id_ = udt_id;
                    udt_schema_version.object_type_ = share::schema::DEPENDENCY_TYPE;
                    udt_schema_version.version_ = udt_info->get_schema_version();
                    uint64_t dep_obj_id = ctx_.view_ref_id_;
                    if (OB_FAIL(stmt->add_global_dependency_table(udt_schema_version))) {
                      LOG_WARN("failed to add global dependency", K(ret));
                    } else if (stmt->add_ref_obj_version(dep_obj_id, db_id,
                                                         ObObjectType::VIEW,
                                                         udt_schema_version,
                                                         ctx_.expr_factory_.get_allocator())) {
                      LOG_WARN("failed to add ref obj version", K(ret));
                    }
                  }
                }
              }
            }
          }

          if (OB_SUCC(ret)) {
            if (is_oracle_mode()) {
              val.set_int(node->value_);
            } else {
              ObCollationType coll_type = static_cast<ObCollationType>(node->int16_values_[OB_NODE_CAST_COLL_IDX]);
              if (CS_TYPE_INVALID != coll_type) {
                // do nothing, for cast('a' as binary)
              } else if (NULL != node->str_value_) {
                // for cast('a' as char char set cs_name)
                ObString cs_name = ObString(node->str_value_);
                ObCharsetType charset_type = CHARSET_INVALID;
                if (CHARSET_INVALID == (charset_type = ObCharset::charset_type(cs_name.trim()))) {
                  ret = OB_ERR_UNKNOWN_CHARSET;
                  LOG_WARN("unknown charset", K(ret), K(cs_name));
                } else {
                  coll_type = ObCharset::get_default_collation(charset_type);
                }
              } else {
                // use connection_collation. for cast('a' as char)
                if (OB_ISNULL(ctx_.session_info_)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected collation type", K(ret));
                } else if (OB_FAIL(ctx_.session_info_->get_collation_connection(coll_type))) {
                  LOG_WARN("failed to get collation", K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                ParseNode tmp_node;
                tmp_node.value_ = node->value_;
                tmp_node.int16_values_[OB_NODE_CAST_COLL_IDX] = coll_type;
                val.set_int(tmp_node.value_);
              }
            }
            c_expr->set_value(val);
            c_expr->set_param(val);
            expr = c_expr;
          }
        }
        break;
      }
      case T_CHAR_CHARSET: {
        //for char(), we need to set collation to first obj which will be used during
        //calc_result_type
        if (OB_FAIL(process_char_charset_node(node, expr))) {
          LOG_WARN("fail to process char charset node", K(ret), K(node));
        }
        break;
      }
      case T_LEFT_VALUE: {  // @user_var := xxx;
        if (OB_FAIL(process_left_value_node(node, expr))) {
          LOG_WARN("fail to process left_value node", K(ret), K(node));
        }
        break;
      }
      case T_SYSTEM_VARIABLE: {
        if (OB_FAIL(process_system_variable_node(node, expr))) {
          LOG_WARN("fail to process system variable node", K(ret), K(node));
        }
        break;
      }
      case T_OP_ORACLE_OUTER_JOIN_SYMBOL: {
        if (OB_FAIL(process_outer_join_symbol_node(node, expr))) {
          LOG_WARN("fail to process outer join column ref node", K(ret), K(node));
        }
        break;
      }
      case T_COLUMN_REF: {
        // star has been expand before, @see ObSelectResolver::resolve_star()
        if (OB_FAIL(process_column_ref_node(node, expr))) {
          LOG_WARN("fail to process column ref node", K(ret), K(node));
        }
        break;
      }
      case T_OP_EXISTS:
        //fall through
      case T_ANY:
        //fall through
      case T_ALL: {
        if (OB_FAIL(process_any_or_all_node(node, expr))) {
          LOG_WARN("fail to process any or all node", K(ret), K(node));
        }
        break;
      }
      case T_OP_GET_USER_VAR: {
        if (OB_FAIL(process_user_var_node(node, expr))) {
          LOG_WARN("fail to process any or all node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_PLSQL_VARIABLE: {
        if (OB_FAIL(process_plsql_var_node(node, expr))) {
          LOG_WARN("failed to process plsql variable node", K(ret), K(node));
        }
        break;
      }
      case T_OP_NOT: {
        if (OB_FAIL(process_not_node(node, expr))) {
          LOG_WARN("fail to process any or all node", K(ret), K(node));
        }
        break;
      }
      case T_OP_POS:
      case T_OP_NEG: {
        if (OB_FAIL(process_pos_or_neg_node(node, expr))) {
          LOG_WARN("fail to process any or all node", K(ret), K(node));
        }
        break;
      }
      case T_OP_AND:
      case T_OP_OR:
      case T_OP_XOR: {
        ObOpRawExpr *m_expr = NULL;
        int64_t num_child = 2;
        if (OB_FAIL(process_node_with_children(node, num_child, m_expr))) {
          LOG_WARN("fail to process node with children", K(ret), K(node));
        } else if (OB_ISNULL(ctx_.session_info_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session_info_ is NULL", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::try_add_bool_expr(m_expr, ctx_.expr_factory_))) {
          LOG_WARN("try_add_bool_expr for add or expr failed", K(ret));
        } else {
          expr = m_expr;
        }
        break;
      }
      case T_OP_ASSIGN:
      case T_OP_ADD:
      case T_OP_MINUS:
      case T_OP_MUL:

      case T_OP_DIV:
      case T_OP_REM:
      case T_OP_POW:
      case T_OP_MOD:
      case T_OP_INT_DIV:
      case T_OP_LE:
      case T_OP_LT:
      case T_OP_EQ:
      case T_OP_NSEQ:
      case T_OP_GE:
      case T_OP_GT:
      case T_OP_NE:
      case T_OP_CNN:
      case T_OP_BIT_AND:
      case T_OP_BIT_OR:
      case T_OP_BIT_XOR:
      case T_OP_BIT_LEFT_SHIFT:
      case T_OP_BIT_RIGHT_SHIFT: {
        if (T_OP_ASSIGN == node->type_) {
          is_contains_assignment_ |= true;
        }
        if (OB_FAIL(process_operator_node(node, expr))) {
          LOG_WARN("fail to process any or all node", K(ret), K(node));
        }
        break;
      }
      case T_OP_IS:
      case T_OP_IS_NOT: {
        if (OB_FAIL(process_is_or_is_not_node(node, expr))) {
          LOG_WARN("fail to process any or all node", K(ret), K(node));
        }
        break;
      }
      case T_OP_BIT_NEG: {
        int64_t child_num = 1;
        ObOpRawExpr *b_expr = NULL;
        if (OB_FAIL(process_node_with_children(node, child_num, b_expr))) {
          LOG_WARN("fail to process node with child", K(ret), K(node->type_));
        } else {
          expr = b_expr;
        }
        break;
      }
      case T_OP_CONNECT_BY_ROOT: {
        if (OB_FAIL(process_connect_by_root_node(*node, expr))) {
          LOG_WARN("fail to process connect_by_root node", K(ret));
        }
        break;
      }
      case T_FUN_SYS_CONNECT_BY_PATH: {
        if (OB_FAIL(process_sys_connect_by_path_node(node, expr))) {
          LOG_WARN("fail to process sys_connect_by_path node", K(ret));
        }
        break;
      }
      case T_OP_PRIOR: {
        if (OB_FAIL(process_prior_node(*node, expr))) {
          LOG_WARN("fail to process prior node", K(node), K(ret));
        }
        break;
      }
      case T_OP_REGEXP:
      case T_OP_NOT_REGEXP: {
        if (OB_FAIL(process_regexp_or_not_regexp_node(node, expr))) {
          LOG_WARN("fail to process prgexp_or_not_regexp node", K(ret), K(node));
        }
        break;
      }
      case T_OP_BTW:
      case T_OP_NOT_BTW: {
        // We will transform between to >= and <= all the time in oracle mode
        // or while expr2 and expr3 have the same res types in mysql mode
        //
        // expr1 NOT BETWEEN expr2 AND expr3 ==>  NOT (expr1 BETWEEN expr2 AND expr3)
        // expr1 BETWEEN expr2 AND expr3 ==> expr1 >= expr2 AND expr1 <= expr3
        if (OB_FAIL(process_between_node(node, expr))) {
          LOG_WARN("fail to process between", K(ret), K(node));
        }
        break;
      }
      case T_OP_LIKE:
      case T_OP_NOT_LIKE: {
        if (OB_FAIL(process_like_node(node, expr))) {
          LOG_WARN("fail to process like_or_between", K(ret), K(node));
        }
        break;
      }
      case T_OP_IN:
        // get through
      case T_OP_NOT_IN: {
        if (OB_FAIL(process_in_or_not_in_node(node, expr))) {
          LOG_WARN("fail to process any or all node", K(ret), K(node));
        }
        break;
      }
      case T_CASE: {
        if (OB_FAIL(process_case_node(node, expr))) {
          LOG_WARN("fail to process case node", K(ret), K(node));
        }
        break;
      }
      case T_EXPR_LIST: {
        if (lib::is_oracle_mode()) {
          /* remove meaningless level */
          ParseNode *child_node = NULL;
          for (int i = 0; i < node->num_child_; i++) {
            child_node = node->children_[i];
            while (T_EXPR_LIST == child_node->type_ && 1 == child_node->num_child_) {
              child_node = child_node->children_[0];
            }
            if (child_node->type_ != T_EXPR_LIST) {
              node->children_[i] = child_node;
            }
          }
          /* expr like (expr1) -> expr1 */
          if (1 == node->num_child_ && T_EXPR_LIST != node->children_[0]->type_) {
            if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], expr)))) {
              LOG_WARN("fail to process node with children only", K(ret),
                  K(node->children_[0]->type_), K(node->children_[0]->num_child_));
            }
            break;
          }
        }
        ObOpRawExpr *multi_expr = NULL;
        if (OB_FAIL(process_node_with_children(node, node->num_child_, multi_expr))) {
          LOG_WARN("fail to process node with children", K(ret), K(node));
        } else {
          multi_expr->set_expr_type(T_OP_ROW);
          expr = multi_expr;
        }
        break;
      }
      case T_SELECT:
      case T_INSERT:
      case T_DELETE:
      case T_UPDATE: {
        if (OB_FAIL(process_sub_query_node(node, expr))) {
          LOG_WARN("fail to process sub query node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_GROUP_ID: {
        if (lib::is_oracle_mode()) {
          ObAggFunRawExpr *agg_expr = NULL;
          if (OB_ISNULL(ctx_.aggr_exprs_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("aggr exprs is null", K(ret));
          } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, agg_expr))) {
            LOG_WARN("fail to create raw expr", K(ret));
          } else if (OB_ISNULL(agg_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("agg_expr is null");
          } else if (OB_FAIL(ctx_.aggr_exprs_->push_back(agg_expr))) {
            LOG_WARN("store aggr expr failed", K(ret));
          } else {
            bool need_add_flag = !ctx_.parents_expr_info_.has_member(IS_AGG);
            if (need_add_flag && OB_FAIL(ctx_.parents_expr_info_.add_member(IS_AGG))) {
              LOG_WARN("failed to add member", K(ret));
            }
          }
          expr = agg_expr;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group_id is only allowed in oracle mode");
        }
        break;
      }
      case T_FUN_GROUPING:
      case T_FUN_GROUPING_ID:
      case T_FUN_COUNT:
      case T_FUN_MAX:
      case T_FUN_MIN:
      case T_FUN_SUM:
      case T_FUN_AVG:
      case T_FUN_MEDIAN:
      case T_FUN_APPROX_COUNT_DISTINCT:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE:
      case T_FUN_STDDEV_POP:
      case T_FUN_STDDEV_SAMP:
      case T_FUN_STDDEV:
      case T_FUN_VARIANCE:
      case T_FUN_CORR:
      case T_FUN_COVAR_POP:
      case T_FUN_COVAR_SAMP:
      case T_FUN_VAR_POP:
      case T_FUN_VAR_SAMP:
      case T_FUN_REGR_SLOPE:
      case T_FUN_REGR_INTERCEPT:
      case T_FUN_REGR_R2:
      case T_FUN_REGR_COUNT:
      case T_FUN_REGR_AVGX:
      case T_FUN_REGR_AVGY:
      case T_FUN_REGR_SXX:
      case T_FUN_REGR_SYY:
      case T_FUN_REGR_SXY:
      case T_FUN_WM_CONCAT:
      case T_FUN_TOP_FRE_HIST:
      case T_FUN_HYBRID_HIST:
      case T_FUN_SYS_BIT_AND:
      case T_FUN_SYS_BIT_OR:
      case T_FUN_SYS_BIT_XOR:
      case T_FUN_JSON_ARRAYAGG:
      case T_FUN_JSON_OBJECTAGG:
      case T_FUN_ORA_JSON_ARRAYAGG:
      case T_FUN_ORA_JSON_OBJECTAGG:
      case T_FUN_ORA_XMLAGG: {

        if (OB_FAIL(process_agg_node(node, expr))) {
          LOG_WARN("fail to process agg node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_GROUP_CUME_DIST:
      case T_FUN_GROUP_RANK:
      case T_FUN_GROUP_DENSE_RANK:
      case T_FUN_GROUP_PERCENT_RANK:
      case T_FUN_GROUP_PERCENTILE_CONT:
      case T_FUN_GROUP_PERCENTILE_DISC:
      case T_FUN_GROUP_CONCAT: {
        if (OB_FAIL(process_group_aggr_node(node, expr))) {
          LOG_WARN("fail to process group concat node", K(ret), K(node));
        }
        break;
      }

      case T_FUN_KEEP_MAX:
      case T_FUN_KEEP_SUM:
      case T_FUN_KEEP_MIN:
      case T_FUN_KEEP_COUNT:
      case T_FUN_KEEP_AVG:
      case T_FUN_KEEP_VARIANCE:
      case T_FUN_KEEP_STDDEV:
      case T_FUN_KEEP_WM_CONCAT: {
        if (OB_FAIL(process_keep_aggr_node(node, expr))) {
          LOG_WARN("fail to process keep aggr node", K(ret), K(node));
        }
        break;
      }

      case T_FUN_SYS_UTC_TIMESTAMP: {
        ObString err_info("utc_timestamp");
        if (OB_FAIL(process_timestamp_node(node, err_info, expr))) {
          LOG_WARN("fail to process timestamp node", K(ret), K(node));
        } else {
          static_cast<ObSysFunRawExpr*>(expr)->set_func_name(ObString::make_string(N_UTC_TIMESTAMP));
        }
        break;
      }
      case T_FUN_SYS_UTC_TIME: {
        ObString err_info("utc_time");
        if (OB_FAIL(process_timestamp_node(node, err_info, expr))) {
          LOG_WARN("fail to process timestamp node", K(ret), K(node));
        } else {
          static_cast<ObSysFunRawExpr*>(expr)->set_func_name(ObString::make_string(N_UTC_TIME));
        }
        break;
      }
      case T_FUN_SYS_CUR_TIMESTAMP: {
        if (lib::is_oracle_mode()) {
          if (OB_FAIL(process_oracle_timestamp_node(node,
                                                    expr,
                                                    MIN_SCALE_FOR_TEMPORAL,
                                                    OB_MAX_TIMESTAMP_TZ_PRECISION,
                                                    DEFAULT_SCALE_FOR_ORACLE_FRACTIONAL_SECONDS))) {
            LOG_WARN("fail to process timestamp node", K(ret), K(node));
          }
        } else { //oracle mode
          ObString err_info("now");
          if (OB_FAIL(process_timestamp_node(node, err_info, expr))) {
            LOG_WARN("fail to process timestamp node", K(ret), K(node));
          }
        }
        if (OB_SUCC(ret)) {
          static_cast<ObSysFunRawExpr*>(expr)->set_func_name(ObString::make_string(N_CUR_TIMESTAMP));
        }
        break;
      }
      case T_FUN_SYS_LOCALTIMESTAMP: {
        if (lib::is_oracle_mode()) {
          if (OB_FAIL(process_oracle_timestamp_node(node,
                                                    expr,
                                                    MIN_SCALE_FOR_TEMPORAL,
                                                    OB_MAX_TIMESTAMP_TZ_PRECISION,
                                                    DEFAULT_SCALE_FOR_ORACLE_FRACTIONAL_SECONDS))) {
            LOG_WARN("fail to process timestamp node", K(ret), K(node));
          }
        } else { //oracle mode
          ObString err_info("now");
          if (OB_FAIL(process_timestamp_node(node, err_info, expr))) {
            LOG_WARN("fail to process timestamp node", K(ret), K(node));
          }
        }
        if (OB_SUCC(ret)) {
          static_cast<ObSysFunRawExpr*>(expr)->set_func_name(ObString::make_string(N_LOCALTIMESTAMP));
        }
        break;
      }
      case T_FUN_SYS_SYSDATE: {
        if (lib::is_oracle_mode()) {
          if (OB_FAIL(process_oracle_timestamp_node(node,
                                                    expr,
                                                    MIN_SCALE_FOR_TEMPORAL,
                                                    OB_MAX_TIMESTAMP_TZ_PRECISION,
                                                    DEFAULT_SCALE_FOR_DATE))) {
            LOG_WARN("fail to process timestamp node", K(ret), K(node));
          }
        } else { //oracle mode
          ObString err_info("sysdate");
          if (OB_FAIL(process_timestamp_node(node, err_info, expr))) {
            LOG_WARN("fail to process timestamp node", K(ret), K(node));
          }
        }
        if (OB_SUCC(ret)) {
          static_cast<ObSysFunRawExpr*>(expr)->set_func_name(ObString::make_string(N_SYSDATE));
        }
        break;
      }
      case T_FUN_SYS_CUR_TIME: {
        ObString err_info("current_time");
        if (OB_FAIL(process_timestamp_node(node, err_info, expr))) {
          LOG_WARN("fail to process timestamp node", K(ret), K(node));
        } else {
          static_cast<ObSysFunRawExpr*>(expr)->set_func_name(ObString::make_string(N_CUR_TIME));
        }
        break;
      }
      case T_FUN_SYS_CUR_DATE: {
        ObSysFunRawExpr *f_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS_CUR_DATE, f_expr))) {
          LOG_WARN("fail to create raw expr", K(ret));
        } else {
          f_expr->set_func_name(ObString::make_string(N_CUR_DATE));
          expr = f_expr;
        }
        break;
      }
      case T_FUN_SYS_UTC_DATE: {
        ObSysFunRawExpr *f_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS_UTC_DATE, f_expr))) {
          LOG_WARN("fail to create raw expr", K(ret));
        } else {
          f_expr->set_func_name(ObString::make_string(N_UTC_DATE));
          expr = f_expr;
        }
        break;
      }
      case T_FUN_SYS_CURRENT_USER: {
        ObSysFunRawExpr *f_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS_CURRENT_USER, f_expr))) {
          LOG_WARN("fail to create raw expr", K(ret));
        } else {
          f_expr->set_func_name(ObString::make_string(N_CURRENT_USER));
          expr = f_expr;
        }
        break;
      }
      case T_COLLATION: {
        // used in internal function `set_collation' to implement COLLATE clause
        if (OB_FAIL(process_collation_node(node, expr))) {
          LOG_WARN("fail to process collation node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_IF: {
        if (OB_FAIL(process_if_node(node, expr))) {
          LOG_WARN("fail to process if node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_INTERVAL: {
        if (OB_FAIL(process_fun_interval_node(node, expr))) {
          LOG_WARN("fail to process fun interval node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_ISNULL: {
        if (OB_FAIL(process_isnull_node(node, expr))) {
          LOG_WARN("fail to process isnull node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_LNNVL: {
        if (OB_FAIL(process_lnnvl_node(node, expr))) {
          LOG_WARN("fail to process lnnvl node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_JSON_ARRAY: {
        if (OB_FAIL(process_json_array_node(node, expr))) {
          LOG_WARN("fail to process array node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_JSON_VALUE: {
        if (OB_FAIL(process_json_value_node(node, expr))) {
          LOG_WARN("fail to process json value node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_IS_JSON: {
        if (OB_FAIL(process_is_json_node(node, expr))) {
          LOG_WARN("fail to process is json node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_JSON_EQUAL: {
        if (OB_FAIL(process_json_equal_node(node, expr))) {
          LOG_WARN("fail to process json equal node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_JSON_MERGE_PATCH: {
        if (OB_FAIL(process_json_mergepatch_node(node, expr))) {
          LOG_WARN("fail to process json mergepatch node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_JSON_QUERY: {
        if (OB_FAIL(process_json_query_node(node, expr))) {
          LOG_WARN("fail to process lnnvl node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_JSON_EXISTS: {
        if (OB_FAIL(process_json_exists_node(node, expr))) {
          LOG_WARN("fail to process json_exists node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_JSON_OBJECT: {
        if (OB_FAIL(process_ora_json_object_node(node, expr))) {
          LOG_WARN("fail to process lnnvl node", K(ret), K(node));
        }
        break;
      }
      case T_NULLX_CLAUSE: {
        modification_type_to_int(const_cast<ParseNode&>(*node));
        // deal node
        if (OB_FAIL(SMART_CALL(recursive_resolve(node, expr)))) {
          LOG_WARN("fail to process node with children only", K(ret),
              K(node->type_), K(node));
        }
        break;
      }
      case T_WEIGHT_STRING_LEVEL_PARAM: {
        modification_type_to_int(const_cast<ParseNode&>(*node));
        // deal node
        if (OB_FAIL(SMART_CALL(recursive_resolve(node, expr)))) {
          LOG_WARN("fail to process node with children only", K(ret),
              K(node->type_), K(node));
        }
        break;
      }
      case T_FUN_SYS_REGEXP_LIKE:
      case T_FUN_SYS: {
        if (OB_FAIL(process_fun_sys_node(node, expr))) {
          if (ret != OB_ERR_FUNCTION_UNKNOWN) {
            LOG_WARN("fail to process system function node", K(ret), K(node));
          } else if (OB_FAIL(process_dll_udf_node(node, expr))) {
            if (ret != OB_ERR_FUNCTION_UNKNOWN) {
              LOG_WARN("fail to process dll user function node", K(ret), K(node));
            } else {
              ParseNode *obj_access = NULL;
              if (OB_FAIL(ObResolverUtils::transform_sys_func_to_objaccess(&ctx_.expr_factory_.get_allocator(), node, obj_access))) {
                LOG_WARN("failed to transform to obj access node", K(ret));
              } else if (OB_ISNULL(obj_access)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("obj access node is null", K(ret));
              } else if (OB_FAIL(process_obj_access_node(*obj_access, expr))) {
                LOG_WARN("failed to process obj access node", K(ret));
                if (get_udf_param_syntax_err()) {
                  // do nothing ....
                } else {
                  ObString func_name(node->children_[0]->str_len_, node->children_[0]->str_value_);
                  ret = OB_ERR_WRONG_PARAMETERS_TO_NATIVE_FCT;
                  LOG_USER_ERROR(OB_ERR_WRONG_PARAMETERS_TO_NATIVE_FCT,
                                func_name.length(), func_name.ptr());
                }
              }
            }
          }
        }
        break;
      }
      case T_FUN_UDF: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("A BUG, Never Be Here!!!", K(ret));
        break;
      }
      case T_WINDOW_FUNCTION: {
        const int64_t orig_win_func_cnt = ctx_.win_exprs_->count();
        if (OB_FAIL(process_window_function_node(node, expr))) {
          LOG_WARN("process window function failed", K(ret));
        }
        for (int64_t i = orig_win_func_cnt; OB_SUCC(ret) && i < ctx_.win_exprs_->count(); ++i) {
          if (OB_FAIL(check_and_canonicalize_window_expr(ctx_.win_exprs_->at(i)))) {
            LOG_WARN("check and canonicalize window expr failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          // remove aggr generated by window_function
          ObIArray<ObAggFunRawExpr *> &aggr_exprs = *ctx_.aggr_exprs_;
          ObSEArray<ObAggFunRawExpr *, 2> clone_aggr_exprs;
          ret = clone_aggr_exprs.assign(aggr_exprs);
          aggr_exprs.reset();
          for (int64_t i = 0; OB_SUCC(ret) && i < clone_aggr_exprs.count(); ++i) {
            bool exist = false;
            for (int64_t j = orig_win_func_cnt; j < ctx_.win_exprs_->count(); ++j) {
              if (clone_aggr_exprs.at(i) == ctx_.win_exprs_->at(j)->get_agg_expr()) {
                exist = true;
                break;
              }
            }
            if (!exist) {
              ret = aggr_exprs.push_back(clone_aggr_exprs.at(i));
            }
          }
        }
        break;
      }
      case T_IDENT: {
        if (OB_FAIL(process_ident_node(*node, expr))) {
          LOG_WARN("process obj access node failed", K(ret));
        }
        break;
      }
      case T_OBJ_ACCESS_REF: {
        if (OB_FAIL(process_obj_access_node(*node, expr))) {
          LOG_WARN("process obj access node failed", K(ret));
        }
        break;
      }
      case T_SP_EXPLICIT_CURSOR_ATTR:
      case T_SP_IMPLICIT_CURSOR_ATTR: {
        if (OB_FAIL(process_cursor_attr_node(*node, expr))) {
          LOG_WARN("process cursor attr node failed", K(ret));
        }
        break;
      }
      case  T_PSEUDO_COLUMN: {
        if (OB_FAIL(process_pseudo_column_node(*node, expr))) {
          LOG_WARN("fail to process pseudo column", K(ret));
        }
        break;
      }
      case T_FUN_SYS_SYSTIMESTAMP: {
        if (OB_FAIL(process_oracle_timestamp_node(node,
                                                  expr,
                                                  MIN_SCALE_FOR_TEMPORAL,
                                                  OB_MAX_TIMESTAMP_TZ_PRECISION,
                                                  DEFAULT_SCALE_FOR_ORACLE_FRACTIONAL_SECONDS))) {
          LOG_WARN("fail to process timestamp node", K(ret), K(node));
        } else {
          static_cast<ObSysFunRawExpr*>(expr)->set_func_name(ObString::make_string(N_SYSTIMESTAMP));
        }
        break;
      }
      case T_SP_CPARAM: {
        OZ (process_call_param_node(node, expr));
        break;
      }
      case T_OP_COLL_PRED:
      case T_OP_MULTISET :{
        OZ (process_multiset_node(node, expr));
        break;
      }
      case T_FUN_SYS_DML_EVENT: {
        OZ (process_dml_event_node(node, expr));
        break;
      }
      case T_FUN_SYS_PART_HASH:
      case T_FUN_SYS_PART_KEY:
      case T_FUN_SYS_SET_COLLATION: {
        OZ (process_internal_sys_function_node(node, expr, node->type_));
        break;
      }
      case T_ODBC_ESCAPE_SEQUENCES: {
        if (OB_FAIL(process_odbc_escape_sequences(node, expr))) {
          LOG_WARN("failed to process odbc escape sequences", K(ret));
        }
        break;
      }
      case T_FUN_SYS_POINT:
      case T_FUN_SYS_LINESTRING:
      case T_FUN_SYS_MULTIPOINT:
      case T_FUN_SYS_MULTILINESTRING:
      case T_FUN_SYS_POLYGON:
      case T_FUN_SYS_MULTIPOLYGON:
      case T_FUN_SYS_GEOMCOLLECTION: {
        OZ (process_geo_func_node(node, expr));
        break;
      }
      case T_FUN_SYS_XML_ELEMENT: {
        if (OB_FAIL(process_xml_element_node(node, expr))) {
          LOG_WARN("failed to process xmlelement node", K(ret));
        }
        break;
      }
      case T_FUN_SYS_XMLPARSE: {
        if (OB_FAIL(process_xmlparse_node(node, expr))) {
          LOG_WARN("fail to process xmlparse node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_XML_ATTRIBUTES: {
        if (OB_FAIL(process_xml_attributes_node(node, expr))) {
          LOG_WARN("fail to process xmlattributes node", K(ret), K(node));
        }
        break;
      }
      case T_FUN_SYS_XML_ATTRIBUTES_VALUES: {
        if (OB_FAIL(process_xml_attributes_values_node(node, expr))) {
          LOG_WARN("fail to process xmlattributes node", K(ret), K(node));
        }
        break;
      }
      case T_REMOTE_SEQUENCE: {
        if (OB_FAIL(process_remote_sequence_node(node, expr))) {
          LOG_WARN("failed to process remote sequence node", K(ret));
        }
        break;
      }
      default:
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("Wrong type in expression", K(get_type_name(node->type_)));
        break;
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_multiset_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse node is null", K(ret));
  } else if (OB_FAIL(process_operator_node(node, expr))) {
    LOG_WARN("process node failed.", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    if (T_OP_MULTISET == node->type_) {
      ObMultiSetRawExpr *ms_expr = static_cast<ObMultiSetRawExpr *>(expr);
      ms_expr->set_multiset_type(static_cast<ObMultiSetType>(node->int32_values_[1]));
      ms_expr->set_multiset_modifier(static_cast<ObMultiSetModifier>(node->int32_values_[0]));
    } else {
      ObCollPredRawExpr *ms_expr = static_cast<ObCollPredRawExpr *>(expr);
      ms_expr->set_multiset_type(static_cast<ObMultiSetType>(node->int32_values_[1]));
      ms_expr->set_multiset_modifier(static_cast<ObMultiSetModifier>(node->int32_values_[0]));
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_ident_node(const ParseNode &node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObQualifiedName q_name;
  if (OB_ISNULL(ctx_.columns_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K_(ctx_.columns));
  } else {
    ObString ident_name(static_cast<int32_t>(node.str_len_), node.str_value_);
    ObObjAccessIdent access_ident(ident_name, OB_INVALID_INDEX);
    if (OB_FAIL(q_name.access_idents_.push_back(access_ident))) {
      LOG_WARN("push back access ident failed", K(ret));
    } else {
      q_name.format_qualified_name(ctx_.case_mode_);
      q_name.parents_expr_info_ = ctx_.parents_expr_info_;
      ObColumnRefRawExpr *b_expr = NULL;
      if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_COLUMN, b_expr))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else if (OB_ISNULL(b_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column ref expr is null");
      } else {
        q_name.ref_expr_ = b_expr;
        if (OB_FAIL(ctx_.columns_->push_back(q_name))) {
          LOG_WARN("Add column failed", K(ret));
        } else {
          expr = b_expr;
        }
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_xml_element_node(const ParseNode *node, ObRawExpr *&expr)
{
  INIT_SUCC(ret);
  CK(OB_NOT_NULL(node));
  if(OB_SUCC(ret) && T_FUN_SYS_XML_ELEMENT != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node->type_ error", K(node->type_));
  } else if (OB_SUCC(ret) && (1 > node->num_child_ || 4 < node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num_child_ error", K(node->num_child_));
  }
  ObSysFunRawExpr *func_expr = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, func_expr))) {
      LOG_WARN("create raw expr failed", K(node->num_child_), K(ret));
    } else if (OB_ISNULL(func_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("func expr is null", K(node->num_child_));
    } else {
      func_expr->set_func_name(ObString::make_string("xmlelement"));
    }
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    const ParseNode *expr_node = node->children_[i];
    CK (OB_NOT_NULL(expr_node));
    if (expr_node->num_child_ > 1 && T_FUN_SYS_XML_ATTRIBUTES != expr_node->type_) {
      for (int j = 0; OB_SUCC(ret) && j < expr_node->num_child_; j++) {
        ObRawExpr *para_expr = NULL;
        CK(OB_NOT_NULL(expr_node->children_[j]));
        OZ(recursive_resolve(expr_node->children_[j], para_expr));
        CK(OB_NOT_NULL(para_expr));
        OZ(func_expr->add_param_expr(para_expr));
      }
    } else {
      ObRawExpr *para_expr = NULL;
      OZ(recursive_resolve(expr_node, para_expr));
      CK(OB_NOT_NULL(para_expr));
      OZ(func_expr->add_param_expr(para_expr));
    }
  }
  OX(expr = func_expr);
  return ret;
}

int ObRawExprResolverImpl::process_xml_attributes_values_node(const ParseNode *node, ObRawExpr *&expr)
{
  INIT_SUCC(ret);
  int cur_col_size = ctx_.columns_->count();
  ParseNode key_node;
  CK(OB_NOT_NULL(node));
  if(OB_SUCC(ret) && T_FUN_SYS_XML_ATTRIBUTES_VALUES != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node->type_ error", K(node->type_));
  } else if (OB_SUCC(ret) && (2 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num_child_ error", K(node->num_child_));
  }
  ObSysFunRawExpr *func_expr = NULL;
  if (OB_FAIL(ret)) {
    LOG_WARN("ret failed", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS_XML_ATTRIBUTES_VALUES, func_expr))) {
    LOG_WARN("create raw expr failed", K(node->num_child_), K(ret));
  } else if (OB_ISNULL(func_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func expr is null", K(node->num_child_));
  } else if (OB_FALSE_IT(func_expr->set_func_name(ObString::make_string("xmlattributes_values")))) {
  } else {
    const ParseNode *expr_value_node = node->children_[0];
    CK (OB_NOT_NULL(expr_value_node));
    ObRawExpr *para_expr = NULL;
    OZ(recursive_resolve(expr_value_node, para_expr));
    CK(OB_NOT_NULL(para_expr));
    OZ(func_expr->add_param_expr(para_expr));
    if (OB_FAIL(ret)) {
      LOG_WARN("ret failed", K(ret));
    } else if (OB_ISNULL(node->children_[1])) {
      ObRawExpr *para_key_expr = NULL;
      ObString col_name;
      para_expr = NULL;
      if (expr_value_node->type_ != T_COLUMN_REF && expr_value_node->type_ != T_OBJ_ACCESS_REF) {
        ret = OB_ERR_XMLELEMENT_ALIASED;
        LOG_WARN("get column raw text failed", K(ret));
      } else if (OB_FAIL(get_column_raw_text_from_node(expr_value_node, col_name))) {
        // bugfix: 49298642
        // parameter 1 of function xmlelement without aliased
        ret = OB_ERR_XMLELEMENT_ALIASED;
        LOG_WARN("get column raw text failed", K(ret));
      } else if (!col_name.empty() && OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(ctx_.local_allocator_,
                                                                                ObString(col_name.length(), easy_string_toupper(col_name.ptr())),
                                                                                &key_node, T_CHAR))) {
        LOG_WARN("create key node failed", K(ret));
      } else {
        OZ(recursive_resolve(&key_node, para_expr));
        CK(OB_NOT_NULL(para_expr));
        OZ(func_expr->add_param_expr(para_expr));
      }
    } else {
      const ParseNode *expr_key_node = node->children_[1];
      CK (OB_NOT_NULL(expr_key_node));
      ObRawExpr *para_expr = NULL;
      OZ(recursive_resolve(expr_key_node, para_expr));
      CK(OB_NOT_NULL(para_expr));
      OZ(func_expr->add_param_expr(para_expr));
    }
  }
  OX(expr = func_expr);
  return ret;
}

int ObRawExprResolverImpl::process_remote_sequence_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObQualifiedName column_ref;
  if (OB_ISNULL(node) || OB_ISNULL(ctx_.columns_) || OB_ISNULL(ctx_.session_info_) ||
      OB_ISNULL(ctx_.schema_checker_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node), K_(ctx_.columns), KPC(ctx_.session_info_), K(ret));
  } else if(OB_SUCC(ret) && T_REMOTE_SEQUENCE != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node->type_ error", K(node->type_));
  } else if (OB_SUCC(ret) && (4 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num_child_ error", K(node->num_child_));
  } else if (OB_FAIL(ObResolverUtils::resolve_column_ref(
            node, ctx_.case_mode_, column_ref))) {
    LOG_WARN("fail to resolve column ref", K(ret));
  } else if (OB_UNLIKELY(column_ref.is_star_)) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("all star should be replaced");
  } else {
    ParseNode* dblink_node = node->children_[3];
    if (dblink_node != NULL) {
      column_ref.dblink_name_.assign_ptr(const_cast<char*>(dblink_node->str_value_),
                                           static_cast<int32_t>(dblink_node->str_len_));
    }
    column_ref.parents_expr_info_ = ctx_.parents_expr_info_;
    ObColumnRefRawExpr *b_expr = NULL;
    uint64_t tenant_id = ctx_.session_info_->get_effective_tenant_id();
    const ObDbLinkSchema *dblink_schema = NULL;
    if (OB_FAIL(ctx_.schema_checker_->get_dblink_schema(tenant_id,
                                                        column_ref.dblink_name_,
                                                        dblink_schema))) {
      LOG_WARN("failed to get dblink schema", K(ret));
    } else if (OB_ISNULL(dblink_schema)) {
      ret = OB_DBLINK_NOT_EXIST_TO_ACCESS;
      LOG_WARN("cat not find dblink", K(column_ref.dblink_name_), K(ret));
    } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_COLUMN, b_expr))) {
      LOG_WARN("fail to create raw expr", K(ret));
    } else if (OB_ISNULL(b_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column ref expr is null");
    } else {
      column_ref.ref_expr_ = b_expr;
      if (lib::is_mysql_mode()) {
        column_ref.database_name_ = dblink_schema->get_database_name();
      } else {
        column_ref.database_name_ = dblink_schema->get_user_name();
      }
      if (OB_FAIL(ctx_.columns_->push_back(column_ref))) {
        LOG_WARN("Add column failed", K(ret));
      } else {
        expr = b_expr;
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_xml_attributes_node(const ParseNode *node, ObRawExpr *&expr) {
  INIT_SUCC(ret);
  CK(OB_NOT_NULL(node));
  if(OB_SUCC(ret) && T_FUN_SYS_XML_ATTRIBUTES != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node->type_ error", K(node->type_));
  } else if (OB_SUCC(ret) && (3 != node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num_child_ error", K(node->num_child_));
  }
  ObSysFunRawExpr *func_expr = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, func_expr))) {
      LOG_WARN("create raw expr failed", K(node->num_child_), K(ret));
    } else if (OB_ISNULL(func_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("func expr is null", K(node->num_child_));
    } else {
      func_expr->set_func_name(ObString::make_string("xmlattributes"));
    }
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < 2; i++) {
    const ParseNode *expr_node = node->children_[i];
    CK (OB_NOT_NULL(expr_node));
    ObRawExpr *para_expr = NULL;
    OZ(recursive_resolve(expr_node, para_expr));
    CK(OB_NOT_NULL(para_expr));
    OZ(func_expr->add_param_expr(para_expr));
  }
  ParseNode *node_ptr = node->children_[2];
  if (OB_ISNULL(node_ptr)) {
    ret = OB_ERR_PARAM_INVALID;
    LOG_WARN("node children_[2] invalid", K(ret));
  } else if (2 != node_ptr->num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num child invalid", K(ret), K(node_ptr->num_child_));
  } else {
    int attributes_count = 0;
    do {
      ObRawExpr *para_expr = NULL;
      if (T_LINK_NODE != node_ptr->type_) {
        ++attributes_count;
        const ParseNode *expr_node = node_ptr;
        CK (OB_NOT_NULL(expr_node));
        OZ(recursive_resolve(expr_node, para_expr));
        if (OB_ERR_XMLELEMENT_ALIASED == ret) {
          LOG_USER_ERROR(OB_ERR_XMLELEMENT_ALIASED, attributes_count);
        }
        CK(OB_NOT_NULL(para_expr));
        for (int i = 0; OB_SUCC(ret) && i < para_expr->get_param_count(); i++) {
          OZ(func_expr->add_param_expr(para_expr->get_param_expr(i)));
        }
        break;
      } else if (T_LINK_NODE == node_ptr->type_ &&
                  node_ptr->children_[0]->num_child_ == 2) {
        ++attributes_count;
        const ParseNode *expr_node = node_ptr->children_[0];
        CK (OB_NOT_NULL(expr_node));
        OZ(recursive_resolve(expr_node, para_expr));
        if (OB_ERR_XMLELEMENT_ALIASED == ret) {
          LOG_USER_ERROR(OB_ERR_XMLELEMENT_ALIASED, attributes_count);
        }
        CK(OB_NOT_NULL(para_expr));
        node_ptr = node_ptr->children_[1];
        for (int i = 0; OB_SUCC(ret) && i < para_expr->get_param_count(); i++) {
          OZ(func_expr->add_param_expr(para_expr->get_param_expr(i)));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("resolver attributes failed", K(ret), K(node_ptr->num_child_), K(node_ptr->children_[0]->num_child_));
      }
    } while (OB_SUCC(ret));
  }
  OX(expr = func_expr);
  return ret;
}

int ObRawExprResolverImpl::process_cursor_attr_node(const ParseNode &node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  pl::ObPLGetCursorAttrInfo info;
  ObRawExpr *child_expr = NULL;
  ObPLGetCursorAttrRawExpr *c_expr = NULL;
  int64_t child_start = ctx_.columns_->count();
  CK (node.type_ == T_SP_EXPLICIT_CURSOR_ATTR || node.type_ == T_SP_IMPLICIT_CURSOR_ATTR);
  OZ (ctx_.expr_factory_.create_raw_expr(T_FUN_PL_GET_CURSOR_ATTR, c_expr));
  CK (OB_NOT_NULL(c_expr));
  OX (static_cast<ObSysFunRawExpr*>(c_expr)
        ->set_func_name(ObString::make_string(N_PL_GET_CURSOR_ATTR)));
  if (OB_FAIL(ret)) {
  } else if (T_SP_EXPLICIT_CURSOR_ATTR == node.type_) {
    CK (OB_NOT_NULL(node.children_[0]));
    OV (T_OBJ_ACCESS_REF == node.children_[0]->type_,
        OB_ERR_UNEXPECTED, node.children_[0]->type_);
    OX (info.set_is_explicit(true));
    OX (info.set_type(node.value_));
    OZ (process_obj_access_node(*(node.children_[0]), child_expr));
    if (OB_SUCC(ret) && T_SP_CURSOR_ROWID == node.value_ && NULL != child_expr) {
      // in current of
      // 1. the value must to be a cursor name
      // 2. the cursor must to be a for update cursor
      const pl::ObPLCursor *cursor = NULL;
      const ObQualifiedName &col = ctx_.columns_->at(ctx_.columns_->count()-1);
      ctx_.secondary_namespace_->get_cursor_by_name(ctx_, col.database_name_,
        col.tbl_name_, col.col_name_, cursor);
      if (NULL == cursor) {
        ret = OB_ERR_NOT_CURSOR_NAME_IN_CURRENT_OF;
        LOG_WARN(" not a cursor name", K(col.col_name_), K(col.tbl_name_), K(col.database_name_), K(ret));
      } else if (!cursor->is_for_update()) {
        ret = OB_ERR_NOT_FOR_UPDATE_CURSOR_IN_CURRENT_OF;
        LOG_USER_ERROR(OB_ERR_NOT_FOR_UPDATE_CURSOR_IN_CURRENT_OF, 
          col.col_name_.length(), col.col_name_.ptr());
        LOG_WARN("current of only support for update select.", K(ret), K(col.col_name_));
      } else if (cursor->has_hidden_rowid()) {
        if (OB_ISNULL(ctx_.stmt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("stmt is NULL", K(ret));
        } else if (ObStmt::is_dml_write_stmt(ctx_.stmt_->stmt_type_)) {
          ObDelUpdStmt *del_upd_stmt = static_cast<ObDelUpdStmt *>(ctx_.stmt_);
          if (OB_ISNULL(del_upd_stmt)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("del_upd_stmt is NULL", K(ret));
          } else if (1 == del_upd_stmt->get_table_items().count()) {
            if (OB_ISNULL(del_upd_stmt->get_table_items().at(0))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("table item is NULL", K(ret));
            } else if (del_upd_stmt->get_table_items().at(0)->ref_id_ != cursor->get_rowid_table_id()) {
              ret = OB_INVALID_ROWID;
              LOG_WARN("invalid ROWID", K(del_upd_stmt->get_table_items().at(0)->ref_id_),
                       K(cursor->get_rowid_table_id()), K(ret));
            }
          }
        }
      }
    }
  } else { // implicit cursor attribute node
    info.set_is_explicit(false);
    if (0 == node.num_child_) {
      info.set_type(node.value_);
    } else if (1 == node.num_child_) {
      CK (OB_NOT_NULL(node.children_[0]));
      if (OB_SUCC(ret)) {
        ObItemType type = node.children_[0]->type_;
        info.set_type(type);
        switch (type) {
          case T_SP_CURSOR_BULK_ROWCOUNT:
          case T_SP_CURSOR_BULK_EXCEPTIONS: {
            ParseNode* bulk_node = node.children_[0];
            CK (OB_LIKELY(1 == bulk_node->num_child_));
            CK (OB_NOT_NULL(bulk_node->children_[0]));
            OZ (SMART_CALL(recursive_resolve(bulk_node->children_[0], child_expr)));
            if (OB_SUCC(ret) && T_SP_CURSOR_BULK_EXCEPTIONS == type) {
              info.set_bulk_exceptions_code_or_idx(bulk_node->value_);
            }
            break;
          }
          case T_SP_CURSOR_BULK_EXCEPTIONS_COUNT: {
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid node type", K(node.children_[0]->type_));
          }
        }
      }
    }
  }
  OX (c_expr->set_pl_get_cursor_attr_info(info));
  // 注意: 在添加Child之前做一次formalize仅仅是为了推导出ROWID属性的类型
  OZ (c_expr->formalize(ctx_.session_info_));
  if (OB_SUCC(ret) && OB_NOT_NULL(child_expr)) {
    OZ (c_expr->add_param_expr(child_expr));
  }
  if (OB_SUCC(ret)) {
    if (T_FUN_PL_GET_CURSOR_ATTR == c_expr->get_expr_type()) {
      // 注意: 替换为ColumnRef的目的是为了在DML语句中把CURSOR%ROWID替换为QuestionMark
      // 参见: ObDMLResolver::resolve_qualified_identifier
      ObQualifiedName column_ref;
      ObObjAccessIdent access_ident;
      ObColumnRefRawExpr *column_expr = NULL;
      access_ident.set_sys_func();
      access_ident.sys_func_expr_ = c_expr;
      OZ (column_ref.access_idents_.push_back(access_ident));
      OX (column_ref.parents_expr_info_ = ctx_.parents_expr_info_);
      OZ (ctx_.expr_factory_.create_raw_expr(T_REF_COLUMN, column_expr));
      CK (OB_NOT_NULL(column_expr));
      OX (column_ref.ref_expr_ = column_expr);
      //GetCursorAttr依赖的所有孩子节点都不是Root
      for (int64_t i = child_start; OB_SUCC(ret) && i < ctx_.columns_->count(); ++i) {
        ctx_.columns_->at(i).is_access_root_ = false;
      }
      OZ (ctx_.columns_->push_back(column_ref));
      OX (expr = column_expr);
    } else {
      OX (expr = c_expr);
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_obj_access_node(const ParseNode &node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_.columns_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K_(ctx_.columns));
  } else if (IS_AGGR_FUN(node.children_[0]->type_) && node.children_[0]->type_ != T_FUN_ORA_XMLAGG) {
    //in oracle ,we could not define standalone or package function with the same name as agg function
    if (IS_KEEP_AGGR_FUN(node.children_[0]->type_)) {
      if (OB_FAIL(process_keep_aggr_node(node.children_[0], expr))) {
        LOG_WARN("process keep agg node failed", K(ret));
      }
    } else if (OB_FAIL(process_agg_node(node.children_[0], expr))) {
      LOG_WARN("process agg node failed", K(ret));
    }
  } else if (node.children_[0]->type_ == T_FUN_ORA_XMLAGG
             && (node.num_child_ == 1
                 || ((node.num_child_ == 2) && OB_ISNULL(node.children_[1])))) {
    // xmlagg without any xmltype member functions
    if (OB_FAIL(process_agg_node(node.children_[0], expr))) {
      LOG_WARN("process xmlagg node failed", K(ret));
    }
  } else {
    ObQualifiedName column_ref;
    int64_t child_start = ctx_.columns_->count();
    if (OB_FAIL(resolve_obj_access_idents(node, column_ref))) {
      LOG_WARN("resolve obj access idents failed", K(ret));
    } else {
      column_ref.format_qualified_name(ctx_.case_mode_);
      column_ref.parents_expr_info_ = ctx_.parents_expr_info_;
      ObColumnRefRawExpr *b_expr = NULL;
      if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_COLUMN, b_expr))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else if (OB_ISNULL(b_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column ref expr is null", K(ret));
      } else {
        column_ref.ref_expr_ = b_expr;
        if (column_ref.is_pl_var()) {
          //PL变量的所有生成的孩子ObQualifiedName都不是root
          for (int64_t i = child_start; OB_SUCC(ret) && i < ctx_.columns_->count(); ++i) {
            ctx_.columns_->at(i).is_access_root_ = false;
          }
        }
        if (OB_FAIL(ctx_.columns_->push_back(column_ref))) {
          LOG_WARN("Add column failed", K(ret));
        } else {
          expr = b_expr;
        }
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::check_sys_connect_by_path_params(const ObRawExpr *param1, const ObRawExpr *param2)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param1) || OB_ISNULL(param2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", KPC(param1), KPC(param2), K(ret));
  } else if (param1->has_flag(CNT_SYS_CONNECT_BY_PATH)) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_WARN("Specified functions not allowed here", KPC(param1), K(ret));
  } else if (OB_UNLIKELY(param2->get_expr_class() != ObRawExpr::EXPR_CONST)) {
    ret = OB_ERR_ILLEGAL_PARAM_FOR_CBY_PATH;
    LOG_WARN("invalid param for sys_connect_by_path", KPC(param2), K(ret));
  } else {
    const ObConstRawExpr *const_param2 = static_cast<const ObConstRawExpr *>(param2);
    const ObObj *param2_value = &const_param2->get_value();
    if (T_QUESTIONMARK == param2->get_expr_type()) {
      int64_t idx = param2_value->get_unknown();
      if (OB_ISNULL(ctx_.param_list_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param_list_ is null", K(ret), K(ctx_.param_list_));
      } else if (idx < 0 || idx >= ctx_.param_list_->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index is out of range", K(idx), K(ctx_.param_list_->count()));
      } else {
        param2_value = &ctx_.param_list_->at(idx);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (param2_value->is_null()) {
      ret = OB_ERR_CBY_CONNECT_BY_PATH_ILLEGAL_PARAM;
      LOG_WARN("invalid argument for sys_connect_by_path", KPC(const_param2), K(ret));
    } else if (param2_value->is_string_type()
               && param2_value->get_string().empty()) {
      ret = OB_ERR_CBY_CONNECT_BY_PATH_ILLEGAL_PARAM;
      LOG_WARN("invalid argument for sys_connect_by_path", KPC(const_param2), K(ret));
    } else if (param2_value->is_integer_type()) {
      //TODO negative number
    }
  }
  return ret;
}

int ObRawExprResolverImpl::is_explict_func_expr(const ParseNode &node, bool &is_func)
{
  int ret = OB_SUCCESS;
  is_func = false;
  const share::schema::ObUDF *udf_info = NULL;
  bool exist = false;
  if (OB_ISNULL(node.children_) || OB_UNLIKELY(node.num_child_ != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is invalid", "node", SJ(ObParserResultPrintWrapper(node)));
  } else if (node.children_[1] == NULL) {
    if (!IS_AGGR_FUN(node.children_[0]->type_)
        && !IS_FUN_SYS_TYPE(node.children_[0]->type_)
        && node.children_[0]->type_ != T_IDENT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child node is invalid", "node", SJ(ObParserResultPrintWrapper(node)));
    } else if (T_FUN_SYS == node.children_[0]->type_) {
      const ParseNode &func_node = *(node.children_[0]);
      ObString func_name(static_cast<int32_t>(func_node.children_[0]->str_len_), func_node.children_[0]->str_value_);
      if (IS_FUN_SYS_TYPE(ObExprOperatorFactory::get_type_by_name(func_name))) {
        is_func = true;
      } else if (OB_ISNULL(ctx_.session_info_) || OB_ISNULL(ctx_.schema_checker_)) {
        //PL resovler don't have session info and schema checker
      } else if (OB_FAIL(ctx_.schema_checker_->get_udf_info(ctx_.session_info_->get_effective_tenant_id(),
                                                            func_name,
                                                            udf_info,
                                                            exist))) {
        LOG_WARN("failed to get udf info");
      } else if (exist) {
        is_func = true;
      } else {
        //do nothing
        //ret = OB_ERR_FUNCTION_UNKNOWN;
        //LOG_USER_ERROR(ret, "FUNCTION", to_cstring(func_name));//throw this error to user
      }
      LOG_DEBUG("is_explict_func_expr", K(func_name), K(is_func), K(exist), "node", SJ(ObParserResultPrintWrapper(node)));
    } else if (IS_FUN_SYS_TYPE(node.children_[0]->type_) || IS_AGGR_FUN(node.children_[0]->type_)) {
      is_func = true;
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_sys_connect_by_path_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *path_expr = NULL;
  ObRawExpr *param1 = NULL;
  ObRawExpr *param2 = NULL;

  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parameter", K(node), K(ret));
  } else if (OB_UNLIKELY(node->type_ != T_FUN_SYS_CONNECT_BY_PATH)
             || OB_UNLIKELY(node->num_child_ != 2)
             || OB_ISNULL(node->children_)
             || OB_ISNULL(node->children_[0])
             || OB_ISNULL(node->children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node", K(node->type_), K(node->num_child_), K(node->children_), K(ret));
  } else if (OB_ISNULL(ctx_.stmt_)
             || OB_UNLIKELY(false == ctx_.stmt_->is_select_stmt())) {
    ret = OB_ERR_CBY_CONNECT_BY_REQUIRED;
    LOG_WARN("CONNECT BY clause required in this query block", K(ret));
  } else if (OB_UNLIKELY(false == static_cast<ObDMLStmt *>(ctx_.stmt_)->is_hierarchical_query())) {
    ret = OB_ERR_CBY_CONNECT_BY_REQUIRED;
    LOG_WARN("CONNECT BY clause required in this query block", K(ret));
  } else if (OB_UNLIKELY(false == is_sys_connect_by_path_expr_valid_scope(ctx_.current_scope_))) {
    ret = OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED;
    LOG_WARN("Connect by path not allowed here", K(ret));
  }  else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS_CONNECT_BY_PATH, path_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(path_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create expr is NULL", K(ret));
  } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], param1)))) {
    LOG_WARN("fail to resolve param1 node", K(ret));
  } else if (OB_FAIL(param1->extract_info())) {
    LOG_WARN("fail to extract info", K(param1), K(ret));
  } else if (OB_UNLIKELY(OB_UNLIKELY(param1->has_flag(CNT_AGG))
                         || OB_UNLIKELY(param1->has_flag(CNT_SUB_QUERY)))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Subquery or aggregate function in sys_connect_by_path is");
  } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[1], param2)))) {
    LOG_WARN("fail to resolve param2 node", K(ret));
  } else if (OB_FAIL(check_sys_connect_by_path_params(param1, param2))) {
    LOG_WARN("fail to check sys_connect_by_path params", KPC(param1), KPC(param2), K(ret));
  } else if (OB_FAIL(path_expr->add_param_expr(param1))) {
    LOG_WARN("fail to push back param", KPC(param1));
  } else if (OB_FAIL(path_expr->add_param_expr(param2))) {
    LOG_WARN("fail to push back param", KPC(param2));
  } else {
    path_expr->set_func_name(N_SYS_CONNECT_BY_PATH);
    expr = path_expr;
    LOG_DEBUG("sys connect by path", K(expr), KPC(expr));
  }
  return ret;
}

int ObRawExprResolverImpl::check_pseudo_column_exist(ObItemType type, ObPseudoColumnRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_ISNULL(ctx_.stmt_)
      || OB_UNLIKELY(false == ctx_.stmt_->is_select_stmt())) {
    ret = OB_ERR_CBY_CONNECT_BY_REQUIRED;
    LOG_WARN("CONNECT BY clause required in this query block", K(ret));
  } else {
    ObDMLStmt *stmt = static_cast<ObDMLStmt *>(ctx_.stmt_);
    for (int64_t i = 0;
         OB_SUCC(ret) && !found && i < stmt->get_pseudo_column_like_exprs().count(); ++i) {
      ObRawExpr *pseudo_column_expr = stmt->get_pseudo_column_like_exprs().at(i);
      if (OB_ISNULL(pseudo_column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(i), K(ret));
      } else if (pseudo_column_expr->get_expr_type() == type) {
        if (OB_LIKELY(pseudo_column_expr->is_pseudo_column_expr())) {
          expr = static_cast<ObPseudoColumnRawExpr *>(pseudo_column_expr);
          found = true;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr should be ObPseudoColumnRawExpr", K(ret), K(*pseudo_column_expr));
        }
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::check_pl_variable(ObQualifiedName &q_name, bool &is_pl_var)
{
  int ret = OB_SUCCESS;
  is_pl_var = false;
  common::ObArenaAllocator allocator;
  ObRawExprFactory expr_factory(allocator);
  ObRawExpr *var = NULL;
  if (NULL == ctx_.secondary_namespace_) {
    // do nothing ...
  } else {
    SET_LOG_CHECK_MODE();
    CK(OB_NOT_NULL(ctx_.secondary_namespace_->get_external_ns()));
    if (OB_SUCC(ret)) {
      ObArray<ObQualifiedName> fake_columns;
      ObArray<ObRawExpr*> fake_exprs;
      if (OB_FAIL(ObResolverUtils::resolve_external_symbol(allocator,
                                                           expr_factory,
                                                           ctx_.secondary_namespace_->get_external_ns()->get_resolve_ctx().session_info_,
                                                           ctx_.secondary_namespace_->get_external_ns()->get_resolve_ctx().schema_guard_,
                                                           &ctx_.secondary_namespace_->get_external_ns()->get_resolve_ctx().sql_proxy_,
                                                           ctx_.external_param_info_,
                                                           ctx_.secondary_namespace_,
                                                           q_name,
                                                           fake_columns,
                                                           fake_exprs,
                                                           var,
                                                           false,/*is_prepare_protocol*/
                                                           true,/*is_check_mode*/
                                                           ctx_.current_scope_ != T_PL_SCOPE /*is_sql_scope*/))) {
        LOG_INFO("failed to resolve external symbol", K(q_name), K(ret));
        if (OB_ERR_INVOKE_STATIC_BY_INSTANCE != ret) {
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(var)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid expr", K(q_name), K(ret));
      } else if (T_QUESTIONMARK == var->get_expr_type()) { // PL基础变量
        is_pl_var = true;
        q_name.access_idents_.at(q_name.access_idents_.count() - 1).set_pl_var();
      } else if (var->is_obj_access_expr()) {
        ObObjAccessRawExpr* obj_access_expr = static_cast<ObObjAccessRawExpr*>(var);
        OX (is_pl_var = true);
        CK (OB_NOT_NULL(obj_access_expr));
        if (OB_FAIL(ret)) {
        } else if (obj_access_expr->is_property()) {
          q_name.access_idents_.at(q_name.access_idents_.count() - 1).set_type_method();
        } else {
          q_name.access_idents_.at(q_name.access_idents_.count() - 1).set_pl_var();
        }
      } else { /*do nothing*/ }
    }
    CANCLE_LOG_CHECK_MODE();
  }
  return ret;
}

int ObRawExprResolverImpl::check_sys_func(ObQualifiedName &q_name, bool &is_sys_func)
{
  int ret = OB_SUCCESS;
  is_sys_func = 1 == q_name.access_idents_.count()
      && (IS_FUN_SYS_TYPE(ObExprOperatorFactory::get_type_by_name(q_name.access_idents_.at(0).access_name_))
          || 0 == q_name.access_idents_.at(0).access_name_.case_compare("sqlerrm")
          || 0 == q_name.access_idents_.at(0).access_name_.case_compare("xmlagg"))
      && q_name.access_idents_.at(0).access_name_.case_compare("nextval") != 0
      && q_name.access_idents_.at(0).access_name_.case_compare("currval") != 0;

  if (is_sys_func) {
    q_name.access_idents_.at(0).set_sys_func();
  }
  return ret;
}

static bool check_is_pl_jsontype(const ObString& name)
{
  return ((name.length() == 13 && ObString("JSON_OBJECT_T").compare(name) == 0)
          || (name.length() == 14 && ObString("JSON_ELEMENT_T").compare(name) == 0));
}

int ObRawExprResolverImpl::check_pl_udf(ObQualifiedName &q_name,
                                        const ObSQLSessionInfo *session_info,
                                        ObSchemaChecker *schema_checker,
                                        pl::ObPLBlockNS *secondary_namespace,
                                        pl::ObProcType &proc_type)
{
  int ret = OB_SUCCESS;
  bool is_pl_udf = false;
  uint64_t udt_id = OB_INVALID_ID;
  proc_type = pl::INVALID_PROC_TYPE;
  if (1 == q_name.access_idents_.count()) {
    if (OB_FAIL(ObResolverUtils::check_routine_exists(session_info,
                                                       schema_checker,
                                                       secondary_namespace,
                                                       ObString(""),
                                                       ObString(""),
                                                       q_name.access_idents_.at(0).access_name_,
                                                       share::schema::ObRoutineType::ROUTINE_FUNCTION_TYPE,
                                                       is_pl_udf,
                                                       proc_type,
                                                       udt_id))) {
      LOG_WARN("failed to check_routine_exists", K(ret), K(q_name));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check pl udf only allow q_name.access_idents_ is 1", K(ret), K(q_name));
  }
  return ret;
}

// PL SCOPE : local udf/local variables > sys function > public function
// SQL SCOPE : sys function > local udf/local variables > public function
// actually we only need recongnise sys function
int ObRawExprResolverImpl::check_name_type(
  ObQualifiedName &q_name, ObStmtScope scope, AccessNameType &type)
{
  int ret = OB_SUCCESS;

  SET_LOG_CHECK_MODE();

  type = UNKNOWN;
  bool check_success = false;
  pl::ObProcType proc_type = pl::INVALID_PROC_TYPE;

  if (T_PL_SCOPE == scope) { //PL Scope: Variable > UDF > SysFunction
    if (OB_FAIL(check_pl_variable(q_name, check_success))) {
      LOG_WARN("check pl variable failed", K(q_name), K(ret));
    } else if (check_success) {
      type = q_name.is_type_method() ? TYPE_METHOD : PL_VAR;
    } else if (OB_FAIL(check_sys_func(q_name, check_success))) {
      LOG_WARN("check system function failed", K(ret), K(q_name));
    } else if (check_success) {
      // Variables > System function > Local pl function, so here only need to check local pl function.
      if (OB_FAIL(check_pl_udf(q_name,
                               ctx_.session_info_,
                               ctx_.schema_checker_,
                               ctx_.secondary_namespace_,
                               proc_type))) {
        LOG_WARN("check pl udf failed", K(ret), K(q_name));
      } else if (proc_type != pl::INVALID_PROC_TYPE && proc_type != pl::STANDALONE_FUNCTION) {
        q_name.access_idents_.at(0).set_pl_udf();
        type = PL_UDF;
      } else {
        type = SYS_FUNC;
      }
    } else { // not variables, not system function, must be udf, do not check it.
      q_name.access_idents_.at(q_name.access_idents_.count() - 1).set_pl_udf();
      type = PL_UDF;
    }
  } else { //SQL Scope: System function > Variable > All pl functions
    if (OB_FAIL(check_sys_func(q_name, check_success))) {
      LOG_WARN("check pl variable failed", K(q_name), K(ret));
    } else if (check_success) {
      type = SYS_FUNC;
    } else if (OB_FAIL(check_pl_variable(q_name, check_success))) {
      LOG_WARN("check pl variable failed", K(q_name), K(ret));
    } else if (check_success) {
      type = q_name.is_type_method() ? TYPE_METHOD : PL_VAR;;
    } else {
      q_name.access_idents_.at(q_name.access_idents_.count() - 1).set_pl_udf();
      type = PL_UDF;
    }
  }

  CANCLE_LOG_CHECK_MODE();
  return ret;
}

void ObRawExprResolverImpl::get_special_func_ident_name(ObString &ident_name, const ObItemType func_type)
{
  // get ident name of spacial exprs not using first child as function name
  if (func_type == T_FUN_SYS_XML_ELEMENT) {
    ident_name = ObString::make_string("xmlelement");
  } else if (func_type == T_FUN_SYS_XMLPARSE) {
    ident_name = ObString::make_string("xmlparse");
  } else if (func_type == T_FUN_ORA_XMLAGG) {
    ident_name = ObString::make_string("xmlagg");
  } else { /* do nothing */}
}

int ObRawExprResolverImpl::resolve_func_node_of_obj_access_idents(const ParseNode &left_node, ObQualifiedName &q_name)
{
  int ret = OB_SUCCESS;
  const ParseNode &func_node = left_node;
  if (OB_ISNULL(func_node.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is NULL", K(func_node.num_child_), K(ret));
  } else {
    ObString ident_name(static_cast<int32_t>(
      func_node.children_[0]->str_len_), func_node.children_[0]->str_value_);
    get_special_func_ident_name(ident_name, func_node.type_);

    // first bit in value_ of T_FUN_SYS node is used to mark NEW keyword,
    // value_ & 0x1 == 1: not used,
    // value_ & 0x1 == 0: used,
    // refer to sql_parser_oracle_mode.y
    bool is_new_key_word_used = !(func_node.value_ & 0x1);

    if (ident_name.empty() && T_PL_SCOPE == ctx_.current_scope_ && lib::is_oracle_mode()) {
      ret = OB_ERR_IDENT_EMPTY;
      LOG_WARN("Identifier cannot be an empty string", K(ret), K(ident_name));
    }
    OZ (q_name.access_idents_.push_back(ObObjAccessIdent(ident_name, OB_INVALID_INDEX)), K(ident_name));

    if (OB_SUCC(ret)) {
      ObObjAccessIdent &access_ident = q_name.access_idents_.at(q_name.access_idents_.count() - 1);

      AccessNameType name_type = UNKNOWN;
      if (!q_name.is_unknown()) {
        if (0 == access_ident.access_name_.case_compare("NEXT")
            || 0 == access_ident.access_name_.case_compare("PRIOR")
            || 0 == access_ident.access_name_.case_compare("EXISTS")) {
          name_type = TYPE_METHOD;
          access_ident.set_type_method();
        } else {
          name_type = PL_UDF;
          access_ident.set_pl_udf();
        }
      } else {
        OZ (check_name_type(q_name, ctx_.current_scope_, name_type), K(q_name), K(name_type));
        if (lib::is_mysql_mode() && PL_VAR == name_type) {
          // mysql can not access variable with '()', if found variable, adjust to udf.
          name_type = PL_UDF;
          access_ident.set_pl_udf();
        }
      }

      if (OB_FAIL(ret)) {
      } else if (name_type != PL_UDF
          && func_node.num_child_ == 3
          && (func_node.children_[2]->type_ == T_DISTINCT || func_node.children_[2]->type_ == T_ALL)) {
        //only pl agg udf allow have distinct/unique/all as common aggr.
        ret = OB_DISTINCT_NOT_ALLOWED;
        LOG_WARN("distinct/all/unique not allowed here", K(ret));
      } else if (is_new_key_word_used && PL_UDF != name_type) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("NEW keyword is only allowed for constructors", K(q_name));
      }

      if (OB_SUCC(ret)) {
        switch (name_type) {
        case SYS_FUNC: { // system function can not access by prefix, so here, access_idents_ must be 1.
          ObRawExpr *func_expr = NULL;
          if (0 == q_name.access_idents_.at(0).access_name_.case_compare("SQLERRM")) {
            OZ (process_sqlerrm_node(&func_node, func_expr));
          } else if (0 == q_name.access_idents_.at(0).access_name_.case_compare("json_equal")) {
            ret = OB_ERR_JSON_EQUAL_OUTSIDE_PREDICATE;
            LOG_WARN("JSON_EQUAL used outside predicate", K(ret));
          } else if (func_node.type_ == T_FUN_SYS_XMLPARSE) {
            OZ (process_xmlparse_node(&func_node, func_expr));
          } else if (func_node.type_ == T_FUN_SYS_XML_ELEMENT) {
            OZ (process_xml_element_node(&func_node, func_expr));
          } else if (func_node.type_ == T_FUN_ORA_XMLAGG) {
            OZ (process_agg_node(&func_node, func_expr));
          }else {
            OZ (process_fun_sys_node(&func_node, func_expr));
          }
          CK (OB_NOT_NULL(func_expr));
          OX (access_ident.sys_func_expr_ = static_cast<ObSysFunRawExpr *>(func_expr));
          for (int64_t i = 0; OB_SUCC(ret) && i < func_expr->get_param_count(); ++i) {
            std::pair<ObRawExpr*, int64_t> param(func_expr->get_param_expr(i), 0);
            OZ (access_ident.params_.push_back(param));
          }
        }
          break;
        case PL_UDF: {
          ParseNode *udf_node = NULL;
          ObRawExpr *udf_expr = NULL;
          // 到这里仅仅知道function node应该是个udf node, 具体是哪个udf是不清楚的, 因此除了udf name，其他的值都填空
          if (OB_FAIL(ObResolverUtils::transform_func_sys_to_udf(&ctx_.expr_factory_.get_allocator(),
                                                                 &func_node,
                                                                 ObString(""),
                                                                 ObString(""),
                                                                 udf_node))) {
            LOG_WARN("transform fun sys to udf node failed", K(ret));
          } else if (OB_FAIL(resolve_udf_node(udf_node, access_ident.udf_info_))) {
            LOG_WARN("process udf node failed", K(ret));
          } else if (OB_ISNULL(udf_expr = access_ident.udf_info_.ref_expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid udf expr", K(ret));
          } else {
            access_ident.udf_info_.is_new_keyword_used_ = is_new_key_word_used;
            if (func_node.num_child_ == 3 && func_node.children_[2]->type_ == T_DISTINCT) {
              static_cast<ObUDFRawExpr*>(udf_expr)->set_is_aggr_udf_distinct(true);
            }
            for (int64_t i = 0; OB_SUCC(ret) && i < udf_expr->get_param_count(); ++i) {
              std::pair<ObRawExpr*, int64_t> param(udf_expr->get_param_expr(i), 0);
              if (OB_FAIL(access_ident.params_.push_back(param))) {
                LOG_WARN("failed to push access_ident parameters", K(ret), K(access_ident));
              }
            }
          }
        }
          break;
        case TYPE_METHOD: {
          if (0 == access_ident.access_name_.case_compare("LIMIT")
              || 0 == access_ident.access_name_.case_compare("COUNT")) {
            // limit and count has no arguments.
            if (func_node.num_child_ > 1 && OB_NOT_NULL(func_node.children_[1])) {
              ret = OB_ERR_CALL_WRONG_ARG;
              LOG_WARN("wrong number or types of arguments in call to procedure", K(ret), K(access_ident));
              LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, access_ident.access_name_.length(), access_ident.access_name_.ptr());
            }
            break;
          }
        } // fall through. continue resolve parameter expression of type method.
        case PL_VAR: {
          if (func_node.num_child_ != 2 || OB_ISNULL(func_node.children_[1])) {
            ret = OB_ERR_NO_FUNCTION_EXIST;
            LOG_WARN("PLS-00222: no function with name 'string' exists in this scope",
                     K(ret), K(func_node.num_child_), K(access_ident));
          } else if (T_EXPR_LIST != func_node.children_[1]->type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("not expr list node!", K(func_node.children_[1]->type_), K(func_node.children_[1]->num_child_), K(ret));
          } else if (func_node.children_[1]->num_child_ != 1) {
            ret = OB_ERR_TABLE_SINGLE_INDEX;
            LOG_WARN("PLS-00316: PL/SQL TABLEs must use a single index", K(ret), K(func_node.children_[1]->num_child_));
          } else {
            const ParseNode *expr_node = func_node.children_[1]->children_[0];
            ObRawExpr *index_expr = NULL;
            int64_t start_child = ctx_.columns_->count();
            if (OB_FAIL(ctx_.parents_expr_info_.add_member(IS_PL_ACCESS_IDX))) {
              LOG_WARN("failed to add parents expr info", K(ret));
            } else if (OB_FAIL(SMART_CALL(recursive_resolve(expr_node, index_expr)))) {
              LOG_WARN("failed to recursive resolve", K(ret));
            } else {
              std::pair<ObRawExpr*, int64_t> param(index_expr, 0);
              if (OB_FAIL(access_ident.params_.push_back(param))) {
                LOG_WARN("push back error", K(ret));
              } else if (OB_FAIL(ctx_.parents_expr_info_.del_member(IS_PL_ACCESS_IDX))) {
                LOG_WARN("failed to del member", K(ret));
              }
            }
            for (int64_t i = start_child; i < ctx_.columns_->count(); ++i) {
              ctx_.columns_->at(i).is_access_root_ = false;
            }
          }
        }
          break;
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected name type", K(ret), K(name_type), K(ctx_.current_scope_), K(q_name));
        } break;
        }
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::resolve_left_node_of_obj_access_idents(const ParseNode &left_node, ObQualifiedName &q_name)
{
  int ret = OB_SUCCESS;
  if (T_QUESTIONMARK == left_node.type_) {
    // quesitonmark in obj access ref must be top node
    CK (q_name.access_idents_.count() <= 0);
    OZ (q_name.access_idents_.push_back(ObObjAccessIdent(ObString(""), left_node.value_)));
    OX (q_name.access_idents_.at(q_name.access_idents_.count() - 1).set_pl_var());
  } else if (T_IDENT == left_node.type_) {
    ObString ident_name(static_cast<int32_t>(left_node.str_len_), left_node.str_value_);
    if (ident_name.empty() && T_PL_SCOPE == ctx_.current_scope_ && lib::is_oracle_mode()) {
      ret = OB_ERR_IDENT_EMPTY;
      LOG_WARN("Identifier cannot be an empty string", K(ret), K(ident_name));
    }
    OZ (q_name.access_idents_.push_back(ObObjAccessIdent(ident_name, OB_INVALID_INDEX)), K(q_name));
    // TODO: may move this check to pl resovler ?
    if (OB_SUCC(ret)
        && T_PL_SCOPE == ctx_.current_scope_
        && q_name.access_idents_.count() > 1
        && (0 == ident_name.case_compare("NEXT")
            || 0 == ident_name.case_compare("PRIOR")
            || 0 == ident_name.case_compare("EXISTS"))
        && lib::is_oracle_mode()) {
      AccessNameType name_type = UNKNOWN;
      OZ (check_name_type(q_name, ctx_.current_scope_, name_type));
      if (OB_SUCC(ret) && name_type == TYPE_METHOD) {
        ret = OB_ERR_CALL_WRONG_ARG;
        LOG_WARN("wrong number or types of arguments in call to procedure", K(ret), K(q_name));
        LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, ident_name.length(), ident_name.ptr());
      }
    }
  } else if (T_FUN_SYS == left_node.type_
             || T_FUN_SYS_XML_ELEMENT == left_node.type_
             || T_FUN_SYS_XMLPARSE == left_node.type_
             || T_FUN_ORA_XMLAGG == left_node.type_) {
    OZ (resolve_func_node_of_obj_access_idents(left_node, q_name));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left node of obj access ref node not T_IDENT/T_QUESTIONMARK/T_FUN_SYS", K(ret), K(left_node.type_));
  }
  return ret;
}

int ObRawExprResolverImpl::resolve_right_node_of_obj_access_idents(const ParseNode &right_node, ObQualifiedName &q_name)
{
  int ret = OB_SUCCESS;
  if (T_OBJ_ACCESS_REF == right_node.type_) {
    if (right_node.children_[0] != NULL && T_EXPR_LIST == right_node.children_[0]->type_) {
      // example: a(1)(2).count, here, we resolve '(2).count' which '(2)' is 'children_[0]' and '.count' is 'children_[1]'
      CK (2 == right_node.num_child_);
      CK (OB_NOT_NULL(right_node.children_[0]));
      CK (OB_NOT_NULL(right_node.children_[1]));
      OZ (resolve_right_node_of_obj_access_idents(*(right_node.children_[0]), q_name));
      OZ (resolve_obj_access_idents(*(right_node.children_[1]), q_name));
    } else {
      // example: a(1).b(1), here, we resolve '.b(1)'
      OZ (resolve_obj_access_idents(right_node, q_name), K(q_name));
    }
  } else {
    // example: a(1)(2) here, we resolve '(2)'
    const ParseNode *element_list = &right_node;
    CK (T_EXPR_LIST == element_list->type_);
    CK (OB_LIKELY(!q_name.access_idents_.empty()));
    for (int64_t i = 0; OB_SUCC(ret) && i < element_list->num_child_; ++i) {
      ObObjAccessIdent &access_ident = q_name.access_idents_.at(q_name.access_idents_.count() - 1);
      ObRawExpr *param_expr = NULL;
      int64_t param_level = OB_INVALID_INDEX;
      CK (OB_NOT_NULL(element_list->children_[i]));
      OZ (SMART_CALL(recursive_resolve(element_list->children_[i], param_expr)));
      if (OB_FAIL(ret)) {
      } else if (access_ident.params_.empty()) {
        if (access_ident.is_pl_var() && access_ident.access_name_.empty()) {
          param_level = 0; // :a(index)
        } else {
          param_level = 1; // f()(index)
        }
      } else {
        param_level = access_ident.params_.at(access_ident.params_.count() - 1).second + 1;
      }
      OZ (access_ident.params_.push_back(std::make_pair(param_expr, param_level)), KPC(param_expr), K(param_level));
    }
  }
  return ret;
}

int ObRawExprResolverImpl::resolve_obj_access_idents(const ParseNode &node, ObQualifiedName &q_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(node.type_ != T_OBJ_ACCESS_REF) || OB_UNLIKELY(node.num_child_ != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid obj access ref node type", K(ret), K(node.type_), K(node.num_child_));
  } else if (OB_ISNULL(node.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left node of obj access is null", K(ret), K(node.type_), KP(node.children_));
  } else if (OB_FAIL(resolve_left_node_of_obj_access_idents(*(node.children_[0]), q_name))) {
    LOG_WARN("failed to resolve left node of obj access", K(ret), K(q_name));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(node.children_[1])) {
    if (OB_FAIL(resolve_right_node_of_obj_access_idents(*(node.children_[1]), q_name))) {
      LOG_WARN("failed to resolve right node of obj access", K(ret));
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_pseudo_column_node(const ParseNode &node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObPseudoColumnRawExpr *pseudo_column_expr = NULL;
  const ParseNode *pseudo_column_node = NULL;
  ObString column_name;
  if (OB_UNLIKELY(node.num_child_ != 1 && node.num_child_ != 2)
      || OB_ISNULL(pseudo_column_node = node.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parameter",K(node.num_child_), K(pseudo_column_node), K(ret));
  } else if ((!IS_PSEUDO_COLUMN_TYPE(pseudo_column_node->type_))
              && pseudo_column_node->type_ != T_CTE_SEARCH_COLUMN
              && pseudo_column_node->type_ != T_CTE_CYCLE_COLUMN) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pseudo column type", K(ret));
  } else if (OB_UNLIKELY(false == is_pseudo_column_valid_scope(ctx_.current_scope_))) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_WARN("pseudo column at invalid scope", K(ctx_.current_scope_), K(ret));
  } else if (OB_FAIL(check_pseudo_column_exist(pseudo_column_node->type_, pseudo_column_expr))) {
    LOG_WARN("fail to check pseudo column exist", K(ret));
  } else if (pseudo_column_expr != NULL) {
    //this type of pseudo_column_expr has been add
    expr = pseudo_column_expr;
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(pseudo_column_node->type_, pseudo_column_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(pseudo_column_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pseudo column expr is NULL", K(ret));
  } else if (pseudo_column_expr->is_hierarchical_query_type()
             && (OB_ISNULL(ctx_.stmt_)
                 || OB_UNLIKELY(false == ctx_.stmt_->is_select_stmt())
                 || (false == static_cast<ObDMLStmt *>(ctx_.stmt_)->is_hierarchical_query()))) {
    ret = OB_ERR_CBY_CONNECT_BY_REQUIRED;
    LOG_WARN("CONNECT BY clause required in this query block", K(ret));
  } else {
    switch (pseudo_column_expr->get_expr_type()) {
      case T_LEVEL:
        pseudo_column_expr->set_data_type(ObNumberType);
        pseudo_column_expr->set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode()][ObNumberType]);
        break;
      case T_CONNECT_BY_ISLEAF:
        pseudo_column_expr->set_data_type(ObNumberType);
        pseudo_column_expr->set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode()][ObNumberType]);
        break;
      case T_CONNECT_BY_ISCYCLE:
        pseudo_column_expr->set_data_type(ObNumberType);
        pseudo_column_expr->set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode()][ObNumberType]);
        break;
      case T_CTE_SEARCH_COLUMN:
        pseudo_column_expr->set_data_type(ObNumberType);
        pseudo_column_expr->set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode()][ObNumberType]);
        if (OB_ISNULL(node.children_[1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("search clause's set var_name has null node");
        } else if (node.children_[1]->str_len_<=0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("search clause's set var_name has 0 str len");
        } else {
          column_name.assign_ptr((char *) node.children_[1]->str_value_, static_cast<int32_t>(node.children_[1]->str_len_));
          pseudo_column_expr->set_alias_column_name(column_name);
        }
        break;
      case T_CTE_CYCLE_COLUMN: {
        pseudo_column_expr->set_data_type(ObVarcharType);
        ObAccuracy accuracy = ObAccuracy::MAX_ACCURACY[ObVarcharType];
        //cycle mark value and non-cycle mark value must be one byte character string values
        accuracy.set_length(1);
        pseudo_column_expr->set_accuracy(accuracy);
        if (OB_ISNULL(node.children_[1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("search clause's set var_name has null node");
        } else if (node.children_[1]->str_len_<=0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("search clause's set var_name has 0 str len");
        } else {
          column_name.assign_ptr((char *) node.children_[1]->str_value_, static_cast<int32_t>(node.children_[1]->str_len_));
          pseudo_column_expr->set_alias_column_name(column_name);
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr type", KPC(pseudo_column_expr), K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (pseudo_column_expr->is_hierarchical_query_type() ||
               pseudo_column_expr->is_cte_query_type()) {
      if (OB_FAIL(static_cast<ObDMLStmt *>(ctx_.stmt_)->get_pseudo_column_like_exprs().
          push_back(pseudo_column_expr))) {
        LOG_WARN("fail to push back pseudo column expr", K(ret));
      } else {
        expr = pseudo_column_expr;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(pseudo_column_expr->get_expr_type()), K(ret));
    }
  }

  return ret;
}

int ObRawExprResolverImpl::process_connect_by_root_node(const ParseNode &node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *root_expr = NULL;
  ObRawExpr *child_expr = NULL;
  if (node.num_child_ != 1
      || OB_ISNULL(node.children_)
      || OB_UNLIKELY(node.type_ != T_OP_CONNECT_BY_ROOT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(node.num_child_), K(node.type_), K(ret));
  } else if (OB_ISNULL(ctx_.stmt_)
             || OB_UNLIKELY(false == ctx_.stmt_->is_select_stmt())) {
    ret = OB_ERR_CBY_CONNECT_BY_REQUIRED;
    LOG_WARN("CONNECT BY clause required in this query block", K(ret));
  } else if (OB_UNLIKELY(false == static_cast<ObDMLStmt *>(ctx_.stmt_)->is_hierarchical_query())) {
    ret = OB_ERR_CBY_CONNECT_BY_REQUIRED;
    LOG_WARN("CONNECT BY clause required in this query block", K(ret));
  } else if (OB_UNLIKELY(false == is_connec_by_root_expr_valid_scope(ctx_.current_scope_))) {
    ret = OB_ERR_CBY_CONNECT_BY_ROOT_ILLEGAL_USED;
    LOG_WARN("Connect by root not allowed here", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node.type_, root_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(SMART_CALL(recursive_resolve(node.children_[0], child_expr)))) {
    LOG_WARN("fail to resolve child expr", K(ret));
  } else if (OB_ISNULL(root_expr)
             || OB_ISNULL(child_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prior expr is NULL", KPC(root_expr), KPC(child_expr), K(ret));
  } else if (OB_FAIL(child_expr->extract_info())) {
    LOG_WARN("fail to extract_info", KPC(child_expr), K(ret));
  } else if (OB_UNLIKELY(child_expr->has_flag(CNT_PSEUDO_COLUMN))
              || OB_UNLIKELY(child_expr->has_flag(CNT_PRIOR))) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_WARN("Specified pseudocolumn or operator not allowed here", KPC(child_expr), K(ret));
  } else if (OB_UNLIKELY(child_expr->has_flag(CNT_CONNECT_BY_ROOT)
            || child_expr->has_flag(CNT_SYS_CONNECT_BY_PATH))) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_WARN("Specified functions not allowed here", KPC(child_expr), K(ret));
  } else if (OB_UNLIKELY(child_expr->has_flag(CNT_AGG))
             || OB_UNLIKELY(child_expr->has_flag(CNT_SUB_QUERY))) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_WARN("Specified pseudocolumn or operator not allowed here", KPC(child_expr), K(ret));
  } else if (OB_FAIL(root_expr->add_param_expr(child_expr))) {
    LOG_WARN("fail to add param", KPC(root_expr), KPC(child_expr), K(ret));
  } else {
    expr = root_expr;
  }
  return ret;
}

int ObRawExprResolverImpl::process_prior_node(const ParseNode &node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *prior_expr = NULL;
  ObRawExpr *child_expr = NULL;
  if (node.num_child_ != 1
      || OB_ISNULL(node.children_)
      || OB_UNLIKELY(node.type_ != T_OP_PRIOR)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(node.num_child_), K(node.type_), K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node.type_, prior_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(SMART_CALL(recursive_resolve(node.children_[0], child_expr)))) {
    LOG_WARN("fail to resolve child expr", K(ret));
  } else if (OB_ISNULL(prior_expr)
             || OB_ISNULL(child_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prior expr is NULL", KPC(prior_expr), KPC(child_expr), K(ret));
  } else if (OB_ISNULL(ctx_.stmt_)
             || OB_UNLIKELY(false == ctx_.stmt_->is_select_stmt())) {
    ret = OB_ERR_CBY_CONNECT_BY_REQUIRED;
    LOG_WARN("CONNECT BY clause required in this query block", K(ret));
  } else if (OB_UNLIKELY(false == static_cast<ObDMLStmt *>(ctx_.stmt_)->is_hierarchical_query())) {
    ret = OB_ERR_CBY_CONNECT_BY_REQUIRED;
    LOG_WARN("CONNECT BY clause required in this query block", K(ret));
  } else if (OB_UNLIKELY(false == is_prior_expr_valid_scope(ctx_.current_scope_))) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_WARN("Specified pseudocolumn or operator not allowed here", KPC(child_expr), K(ret));
  } else if (OB_FAIL(child_expr->extract_info())) {
    LOG_WARN("fail to formalize expr", KPC(child_expr), K(ret));
  } else if (OB_UNLIKELY(child_expr->has_flag(CNT_PRIOR))
             || OB_UNLIKELY(child_expr->has_flag(CNT_PSEUDO_COLUMN))
             || OB_UNLIKELY(child_expr->has_flag(CNT_AGG))
             || OB_UNLIKELY(child_expr->has_flag(CNT_CONNECT_BY_ROOT))
             || OB_UNLIKELY(child_expr->has_flag(CNT_ROWNUM))) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_WARN("Specified pseudocolumn or operator not allowed here", KPC(child_expr), K(ret));
  } else if (OB_UNLIKELY(child_expr->has_flag(CNT_SUB_QUERY))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("prior a subquery still not supported", KPC(child_expr), K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "subquery in prior is");
  } else if (OB_FAIL(prior_expr->add_param_expr(child_expr))) {
    LOG_WARN("fail to add param", KPC(prior_expr), KPC(child_expr), K(ret));
  } else {
    expr = prior_expr;
    LOG_DEBUG("prior expr", K(child_expr), KPC(child_expr), K(prior_expr), KPC(prior_expr));
  }
  return ret;
}

int ObRawExprResolverImpl::process_datatype_or_questionmark(const ParseNode &node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObObjParam val;
  ObConstRawExpr *c_expr = NULL;
  ObString literal_prefix;
  bool is_paramlize = false;
  const ObLengthSemantics default_length_semantics = (OB_NOT_NULL(ctx_.session_info_) ? ctx_.session_info_->get_actual_nls_length_semantics() : LS_BYTE);
  const ObSQLSessionInfo *session_info = ctx_.session_info_;
  int64_t server_collation = CS_TYPE_INVALID;
  ObCollationType nation_collation = OB_NOT_NULL(ctx_.session_info_) ? ctx_.session_info_->get_nls_collation_nation() : CS_TYPE_INVALID;
  if (lib::is_oracle_mode() && nullptr == session_info) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else if (lib::is_oracle_mode() && OB_FAIL(
    session_info->get_sys_variable(share::SYS_VAR_COLLATION_SERVER, server_collation))) {
    LOG_WARN("get sys variables failed", K(ret));
  } else if (OB_FAIL(ObResolverUtils::resolve_const(&node,
                        // stmt_type is only used in oracle mode
                        lib::is_oracle_mode() ? session_info->get_stmt_type() : stmt::T_NONE,
                                             ctx_.expr_factory_.get_allocator(),
                                             ctx_.dest_collation_, nation_collation, ctx_.tz_info_,
                                             val, is_paramlize,
                                             literal_prefix,
                                             default_length_semantics,
                                             static_cast<ObCollationType>(server_collation),
                                             &(ctx_.parents_expr_info_),
                                             session_info->get_sql_mode(),
                                             nullptr != ctx_.secondary_namespace_))) {
    LOG_WARN("failed to resolve const", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(lib::is_mysql_mode() && node.type_ == T_NCHAR ?
                                                                              T_VARCHAR : node.type_, c_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(c_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr is null", K(ret), K(c_expr));
  } else {
    if (node.is_date_unit_) {
      c_expr->set_is_date_unit();
    }
    c_expr->set_value(val);
    //为空串重新设置一次meta信息
    c_expr->set_expr_obj_meta(val.get_param_meta());
    c_expr->set_literal_prefix(literal_prefix);
    // for mysql mode distinguish tinyint and literal bool
    if (T_BOOL == node.type_ || node.is_literal_bool_) {
      c_expr->set_is_literal_bool(true);
    }
    if (!val.is_unknown()) {
      c_expr->set_param(val);
      c_expr->set_accuracy(val.get_accuracy());
      c_expr->set_result_flag(val.get_result_flag()); // not_null etc
    } else {
      if (!ctx_.is_extract_param_type_) {
        if (NULL == ctx_.param_list_) {
          LOG_INFO("NOTICE : UNIT TEST Code direction.", K(ret));
        } else if (ctx_.param_list_->empty()) { //走到这里说明当前val是？,但是param_list是empty，说明是preapre阶段
          ObObjMeta question_mark_meta;
          if (lib::is_oracle_mode()) {
            question_mark_meta.set_char();
            question_mark_meta.set_collation_type(session_info->get_nls_collation());
            c_expr->set_length_semantics(session_info->get_local_nls_length_semantics());
            c_expr->set_length(ObAccuracy::PS_QUESTION_MARK_DEDUCE_LEN);
          } else {
            // Compatible with MySQL, use the default varbinary type for type deduce in the ps prepare phase
            question_mark_meta.set_varbinary();
          }
          question_mark_meta.set_collation_level(CS_LEVEL_IMPLICIT);
          c_expr->set_meta_type(question_mark_meta);
          c_expr->set_expr_obj_meta(question_mark_meta);
          if (NULL == ctx_.external_param_info_) {
            /*do nothing...*/
          } else if (ctx_.is_for_dbms_sql_
                     || (ctx_.is_for_dynamic_sql_ && OB_NOT_NULL(session_info->get_pl_context()))){
            //NOTICE: only need to process PL dynamic sql and dbms sql
            /*dynamic and dbms sql already prepare question mark in parse stage.*/
            bool need_save = true;
            for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.external_param_info_->count(); ++i) {
              CK (OB_NOT_NULL(ctx_.external_param_info_->at(i).first));
              if (OB_SUCC(ret)
                  && ctx_.external_param_info_->at(i).first->same_as(*c_expr)) {
                need_save = false;
                break;
              }
            }
            if (OB_SUCC(ret) && need_save) {
              OZ (ctx_.external_param_info_->push_back(std::make_pair(c_expr, c_expr)));
              OX (ctx_.prepare_param_count_++);
            }
          } else {
            //prepare stmt不需要type，只需要计算?的个数
            if (OB_SUCC(ret) && nullptr != ctx_.secondary_namespace_) {
              const pl::ObPLSymbolTable* symbol_table = NULL;
              const pl::ObPLVar* var = NULL;
              CK (OB_NOT_NULL(symbol_table = ctx_.secondary_namespace_->get_symbol_table()));
              CK (OB_NOT_NULL(var = symbol_table->get_symbol(val.get_unknown())));
              if (OB_SUCC(ret)) {
                if (0 == var->get_name().case_compare(pl::ObPLResolver::ANONYMOUS_ARG)) {
                  if (OB_NOT_NULL(var->get_type().get_meta_type())) {
                    CK (OB_NOT_NULL(var->get_type().get_data_type()));
                    OX (c_expr->set_meta_type(*var->get_type().get_meta_type()));
                    OX (c_expr->set_expr_obj_meta(*var->get_type().get_meta_type()));
                    OX (c_expr->set_accuracy(var->get_type().get_data_type()->get_accuracy()));
                  } else {
                    ObObjMeta meta;
                    OX (meta.set_type(ObExtendType));
                    OX (c_expr->set_meta_type(meta));
                    OX (c_expr->set_udt_id(var->get_type().get_user_type_id()));
                  }
                }
              }
            }
            ObRawExpr *original_expr = c_expr;
            OZ (ObResolverUtils::resolve_external_param_info(*ctx_.external_param_info_,
                                                             ctx_.expr_factory_,
                                                             ctx_.prepare_param_count_,
                                                             original_expr));
            OX (c_expr = static_cast<ObConstRawExpr*>(original_expr));
          }
        } else if (val.get_unknown() >= ctx_.param_list_->count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("question mark index out of param list count",
                   "index", val.get_unknown(), "param_count", ctx_.param_list_->count());
        } else { //execute stmt阶段，注：这里没有设置c_expr的accuracy会不会有问题
          const ObObjParam &param = ctx_.param_list_->at(val.get_unknown());
          c_expr->set_is_literal_bool(param.is_boolean());
          if (param.is_ext()) {
#ifdef OB_BUILD_ORACLE_PL
            if (OB_NOT_NULL(ctx_.session_info_->get_pl_implicit_cursor())
                && ctx_.session_info_->get_pl_implicit_cursor()->get_in_forall()
                && param.is_ext_sql_array()) {
              ObSqlArrayObj *param_array = reinterpret_cast<ObSqlArrayObj*>(param.get_ext());
              CK (OB_NOT_NULL(param_array));
              OX (c_expr->set_meta_type(param_array->element_.get_meta_type()));
              OX (c_expr->set_expr_obj_meta(param_array->element_.get_meta_type()));
              OX (c_expr->set_accuracy(param_array->element_.get_accuracy()));
              OX (c_expr->set_param(param));
            } else {
#endif
              c_expr->set_meta_type(param.get_meta());
              c_expr->set_expr_obj_meta(param.get_param_meta());
              c_expr->set_udt_id(param.get_udt_id());
              c_expr->set_param(param);
#ifdef OB_BUILD_ORACLE_PL
            }
#endif
          } else {
            if (ObNullType == param.get_type() &&
                T_QUESTIONMARK == c_expr->get_expr_type() &&
                ObDateTimeType == param.get_null_meta().get_type()) {
              c_expr->set_meta_type(param.get_null_meta());
            } else {
              c_expr->set_meta_type(ObSQLUtils::is_oracle_empty_string(param)
                                  ? param.get_param_meta() : param.get_meta());
            }
            c_expr->set_expr_obj_meta(param.get_param_meta());
            c_expr->set_accuracy(param.get_accuracy());
            c_expr->set_result_flag(param.get_result_flag()); // not_null etc
            c_expr->set_param(param);

            sql::ObExprResType result_type = c_expr->get_result_type();
            if (result_type.get_length() == -1) {
              if (result_type.is_varchar() || result_type.is_nvarchar2()) {
                result_type.set_length(OB_MAX_ORACLE_VARCHAR_LENGTH);
              } else if (result_type.is_char() || result_type.is_nchar()) {
                result_type.set_length(OB_MAX_ORACLE_CHAR_LENGTH_BYTE);
              }
            }
            c_expr->set_result_type(result_type);
          }
//execute阶段不需要统计prepare_param_count_的个数
//          ctx_.prepare_param_count_++;
//          LOG_DEBUG("prepare stmt add new param", K(ctx_.prepare_param_count_));
        }
      } else {
        int64_t param_idx = val.get_unknown();
        if (OB_ISNULL(ctx_.param_list_)) {
          ret = OB_NOT_INIT;
          LOG_WARN("context param list is null", K(ret));
        } else if (param_idx >= ctx_.param_list_->count()) {
          ret = OB_ERR_BIND_VARIABLE_NOT_EXIST;
          LOG_WARN("bind variable does not exist",
                   K(ret), K(param_idx), K(ctx_.param_list_->count()));
        } else {
          const ObObjParam &param = ctx_.param_list_->at(param_idx);
          c_expr->set_is_literal_bool(param.is_boolean());
          if (param.is_ext()) {
            // values statement
            if (param.is_ext_sql_array()) {
              ObSqlArrayObj *param_array = reinterpret_cast<ObSqlArrayObj*>(param.get_ext());
              const ObExecContext *exec_ctx = NULL;
              const ObPhysicalPlanCtx *phy_ctx = NULL;
              if (OB_ISNULL(param_array) || OB_ISNULL(ctx_.session_info_) ||
                  OB_ISNULL(exec_ctx = ctx_.session_info_->get_cur_exec_ctx()) ||
                  OB_ISNULL(phy_ctx = ctx_.session_info_->get_cur_exec_ctx()->get_physical_plan_ctx())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("param array is invalid", K(ret), KPC(param_array));
              } else {
                c_expr->set_meta_type(param_array->element_.get_meta_type());
                c_expr->set_expr_obj_meta(param_array->element_.get_meta_type());
                c_expr->set_accuracy(param_array->element_.get_accuracy());
                c_expr->set_param(param);
                const ObIArray<ObArrayParamGroup> &array_param_groups = phy_ctx->get_array_param_groups();
                for (int64_t i = 0; i < array_param_groups.count(); i++) {
                  const ObArrayParamGroup &group = array_param_groups.at(i);
                  if (param_idx >= group.start_param_idx_ &&
                      param_idx < group.start_param_idx_ + group.column_count_) {
                    c_expr->set_array_param_group_id(i);
                    break;
                  }
                }
              }
            } else {
#ifndef OB_BUILD_ORACLE_PL
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support array binding", K(ret));
            }
#else
              pl::ObPLNestedTable *param_array = reinterpret_cast<pl::ObPLNestedTable*>(param.get_ext());
              if (OB_ISNULL(param_array)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("param array is invalid", K(ret), KPC(param_array));
              } else {
                c_expr->set_meta_type(param_array->get_element_type().get_meta_type());
                c_expr->set_expr_obj_meta(param_array->get_element_type().get_meta_type());
                c_expr->set_accuracy(param_array->get_element_type().get_accuracy());
                c_expr->set_param(param);
              }
            }
#endif
          } else {
            //questionmark won't set meta_type again
            if (param.get_param_meta().get_type() != param.get_type()) {
              LOG_TRACE("question mark not suited", K(param.get_param_meta().get_type()), K(param.get_type()), K(common::lbt()));
            }
            c_expr->set_meta_type(param.get_param_meta());
            c_expr->set_expr_obj_meta(param.get_param_meta());
            c_expr->set_accuracy(param.get_accuracy());
            c_expr->set_result_flag(param.get_result_flag()); // not_null etc
            c_expr->set_param(param);
          }
        }
      }
    }
    expr = c_expr;
  }
  return ret;
}

int ObRawExprResolverImpl::process_system_variable_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(ctx_.sys_vars_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node), K_(ctx_.sys_vars));
  } else if (!ctx_.is_variable_allowed_) {
    ret = OB_ERR_VIEW_SELECT_CONTAIN_QUESTIONMARK;
    LOG_WARN("View's SELECT contains a variable or parameter", K(ret));
  } else {
    ObString str;
    str.assign_ptr(const_cast<char *>(node->str_value_), static_cast<int32_t>(node->str_len_));
    if (OB_FAIL(ObRawExprUtils::build_get_sys_var(ctx_.expr_factory_, str,
                                                  static_cast<share::ObSetVar::SetScopeType>(node->value_),
                                                  expr))) {
      LOG_WARN("failed to create expr", K(ret));
    } else {
      ObVarInfo var_info;
      var_info.name_.assign_ptr(str.ptr(), static_cast<int32_t>(str.length()));
      var_info.type_ = SYS_VAR;
      if (OB_FAIL(ctx_.sys_vars_->push_back(var_info))) {
        LOG_WARN("failed to store var info", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_char_charset_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *c_expr = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_VARCHAR, c_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else {
    ObString charset_str(node->str_len_, node->str_value_);
    ObCharsetType charset_type = ObCharset::charset_type(charset_str);
    if (CHARSET_INVALID == charset_type) {
      ret = OB_ERR_UNKNOWN_CHARSET;
      LOG_WARN("invalid character set", K(charset_str), K(ret));
      LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset_str.length(), charset_str.ptr());
    } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(charset_type,
                                                                      ctx_.session_info_->get_effective_tenant_id()))) {
      LOG_WARN("failed to check charset data version valid", K(ret));
    } else {
      ObCollationType coll_type = ObCharset::get_system_collation();
      ObObj val;
      val.set_varchar(charset_str);
      val.set_collation_type(coll_type);
      val.set_collation_level(CS_LEVEL_COERCIBLE);
      c_expr->set_value(val);
      expr = c_expr;
    }
  }
  return ret;
}

int ObRawExprResolverImpl::set_geo_func_name(ObSysFunRawExpr *func_expr,
                                             const ObItemType func_type)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(func_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null function expr", K(ret), K(get_type_name(func_type)));
  } else {
    switch (func_type) {
      case T_FUN_SYS_POINT: {
        OX(func_expr->set_func_name(N_POINT));
        break;
      }
      case T_FUN_SYS_LINESTRING: {
        OX(func_expr->set_func_name(N_LINESTRING));
        break;
      }
      case T_FUN_SYS_POLYGON: {
        OX(func_expr->set_func_name(N_POLYGON));
        break;
      }
      case T_FUN_SYS_MULTIPOINT: {
        OX(func_expr->set_func_name(N_MULTIPOINT));
        break;
      }
      case T_FUN_SYS_MULTILINESTRING: {
        OX(func_expr->set_func_name(N_MULTILINESTRING));
        break;
      }
      case T_FUN_SYS_MULTIPOLYGON: {
        OX(func_expr->set_func_name(N_MULTIPOLYGON));
        break;
      }
      case T_FUN_SYS_GEOMCOLLECTION: {
        OX(func_expr->set_func_name(N_GEOMCOLLECTION));
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid geometry function", K(ret), K(func_type));
        break;
      }
    }
  }

  return ret;
}

int ObRawExprResolverImpl::process_geo_func_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *func_expr = NULL;

  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, func_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(set_geo_func_name(func_expr, node->type_))) {
    LOG_WARN("fail to set geo function name", K(ret), K(get_type_name(node->type_)));
  } else if (T_FUN_SYS_POINT == node->type_) {
    ObRawExpr *sub_expr1 = NULL;
    ObRawExpr *sub_expr2 = NULL;
    if (OB_UNLIKELY(2 != node->num_child_) || OB_ISNULL(node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node children", K(ret), K_(node->num_child),
          K_(node->children), K_(node->type));
    } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], sub_expr1)))) {
      LOG_WARN("resolve x child failed", K(ret));
    } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[1], sub_expr2)))) {
      LOG_WARN("resolve y child failed", K(ret));
    } else if (OB_FAIL(func_expr->set_param_exprs(sub_expr1, sub_expr2))) {
      LOG_WARN("failed to add param expr", K(ret));
    }
  } else if (OB_ISNULL(node->children_[0]) && T_FUN_SYS_GEOMCOLLECTION == node->type_) { // SELECT GEOMETRYCOLLECTION();
    // do nothing
  } else {
    ParseNode *expr_list_node = node->children_[0];
    if (OB_ISNULL(expr_list_node) || OB_UNLIKELY(T_EXPR_LIST != expr_list_node->type_)
        || OB_ISNULL(expr_list_node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid children for geometry type function", K(node), K(expr_list_node));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_list_node->num_child_; ++i) {
      ObRawExpr *para_expr = NULL;
      if (OB_ISNULL(expr_list_node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr list node children", K(ret), K(i), K(expr_list_node->children_[i]));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(expr_list_node->children_[i], para_expr)))) {
        LOG_WARN("fail to recursive resolve expr list item", K(ret));
      } else if (OB_FAIL(func_expr->add_param_expr(para_expr))) {
        LOG_WARN("fail to add param expr to expr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    expr = func_expr;
  }

  return ret;
}

int ObRawExprResolverImpl::process_left_value_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObUserVarIdentRawExpr *var_expr = NULL;
  ObIArray<ObUserVarIdentRawExpr *> *all_vars =
      (NULL == ctx_.query_ctx_ ? NULL : &ctx_.query_ctx_->all_user_variable_);
  bool query_has_udf = (NULL == ctx_.query_ctx_ ? false : ctx_.query_ctx_->has_udf_);
  if (OB_ISNULL(node) || OB_ISNULL(ctx_.sys_vars_) || OB_ISNULL(ctx_.user_var_exprs_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node), K_(ctx_.sys_vars), K(ctx_.user_var_exprs_));
  } else {
    ObString str;
    str.assign_ptr(const_cast<char *>(node->str_value_), static_cast<int32_t>(node->str_len_));
    ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, str);
    ObObj val;
    val.set_varchar(str);
    val.set_collation_type(ObCharset::get_system_collation());
    val.set_collation_level(CS_LEVEL_IMPLICIT);

    if (NULL != all_vars) {
      for (int64_t i = 0; OB_SUCC(ret) && i < all_vars->count(); ++i) {
        if (OB_ISNULL(all_vars->at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null user var ident expr", K(ret));
        } else if (all_vars->at(i)->is_same_variable(val)) {
          var_expr = all_vars->at(i);
          var_expr->set_is_contain_assign(true);
        }
      }
    }
    if (OB_SUCC(ret) && NULL == var_expr) {
      if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_USER_VARIABLE_IDENTIFIER, var_expr))) {
        LOG_WARN("fail to create user var ident expr", K(ret));
      } else if (NULL != all_vars && OB_FAIL(all_vars->push_back(var_expr))) {
        LOG_WARN("failed to push back var expr", K(ret));
      } else {
        var_expr->set_value(val);
        var_expr->set_is_contain_assign(true);
      }
    }
    if (OB_SUCC(ret)) {
      var_expr->set_query_has_udf(query_has_udf);
      expr = var_expr;
      ObVarInfo var_info;
      var_info.name_.assign_ptr(str.ptr(), static_cast<int32_t>(str.length()));
      var_info.type_ = USER_VAR;
      if (OB_FAIL(ctx_.sys_vars_->push_back(var_info))) {
        SQL_ENG_LOG(WARN, "fail to push back var info", K(ret), K(var_info.name_));
      } else if (OB_FAIL(add_var_to_array_no_dup(*ctx_.user_var_exprs_, var_expr))) {
        LOG_WARN("failed to add var to array no dup", K(ret));
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_outer_join_symbol_node(
    const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *b_expr = NULL;
  ObRawExpr *col_expr = NULL;;

  CK(OB_NOT_NULL(node), 1 == node->num_child_);
  OZ((ctx_.expr_factory_.create_raw_expr)(T_OP_ORACLE_OUTER_JOIN_SYMBOL, b_expr));
  OZ(process_column_ref_node(node->children_[0], col_expr));
  CK(OB_NOT_NULL(b_expr));
  OZ((b_expr->add_param_expr)(col_expr));
  if (OB_SUCC(ret)) {
    expr = b_expr;
  }
  return ret;
}

int ObRawExprResolverImpl::process_column_ref_node(
    const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObQualifiedName column_ref;

  if (OB_ISNULL(node) || OB_ISNULL(ctx_.columns_) || OB_ISNULL(ctx_.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node), K_(ctx_.columns), KPC(ctx_.session_info_), K(ret));
  } else if (ctx_.session_info_->is_for_trigger_package()
             && ObRawExprUtils::is_new_old_column_ref(node) && !lib::is_oracle_mode()) {
    ParseNode *obj_access_node = NULL;
    OZ (ObRawExprUtils::mock_obj_access_ref_node(ctx_.expr_factory_.get_allocator(),
                                                 obj_access_node, node, ctx_.tg_timing_event_));
    CK (OB_NOT_NULL(obj_access_node));
    OZ (process_obj_access_node(*obj_access_node, expr));
  } else if (OB_FAIL(ObResolverUtils::resolve_column_ref(
            node, ctx_.case_mode_, column_ref))) {
    LOG_WARN("fail to resolve column ref", K(ret));
  } else if (OB_UNLIKELY(column_ref.is_star_)) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("all star should be replaced");
  } else {
    if (column_ref.database_name_.empty() && column_ref.tbl_name_.empty() && !column_ref.col_name_.empty()) {
      if (OB_ISNULL(node->children_[2])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(node), K(column_ref), K(ret));
      } else {
        ObString ident_name(static_cast<int32_t>(node->children_[2]->str_len_), node->children_[2]->str_value_);
        ObObjAccessIdent access_ident(ident_name, OB_INVALID_INDEX);
        if (OB_FAIL(column_ref.access_idents_.push_back(access_ident))) {
          LOG_WARN("push back access ident failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      column_ref.parents_expr_info_ = ctx_.parents_expr_info_;
      ObColumnRefRawExpr *b_expr = NULL;
      if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_COLUMN, b_expr))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else if (OB_ISNULL(b_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column ref expr is null");
      } else {
        column_ref.ref_expr_ = b_expr;
        if (OB_FAIL(ctx_.columns_->push_back(column_ref))) {
          LOG_WARN("Add column failed", K(ret));
        } else {
          expr = b_expr;
        }
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_any_or_all_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *sub_expr = NULL;
  if (OB_UNLIKELY(1 != node->num_child_) ||
      OB_ISNULL(node->children_) ||
      OB_ISNULL(node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children", K(ret), K(node->num_child_), K(node));
  } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], sub_expr)))) {
    LOG_WARN("resolve sub-query failed", K(ret));
  } else if (OB_ISNULL(sub_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sub_expr", K(sub_expr));
  } else if (T_REF_QUERY == sub_expr->get_expr_type()) {
    ObQueryRefRawExpr *sub_ref = static_cast<ObQueryRefRawExpr*>(sub_expr);
    sub_ref->set_is_set(true);
    ObOpRawExpr *op_expr = NULL;
    if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, op_expr))) {
      LOG_WARN("create ObOpRawExpr failed", K(ret));
    } else if (OB_ISNULL(expr = op_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op_expr is null");
    } else if (OB_FAIL(op_expr->set_param_expr(sub_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    }
  } else if (T_OP_ROW == sub_expr->get_expr_type()) {
    ObOpRawExpr *op_expr = NULL;
    if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, op_expr))) {
      LOG_WARN("create ObOpRawExpr failed", K(ret));
    } else if (OB_ISNULL(expr = op_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op_expr is null");
    } else if (OB_FAIL(op_expr->set_param_expr(sub_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    } else { /*do nothing*/ }
  } else {
    expr = sub_expr;
  }
  return ret;
}

int ObRawExprResolverImpl::process_user_var_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(ctx_.sys_vars_) || OB_ISNULL(ctx_.user_var_exprs_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node), K_(ctx_.sys_vars), K(ctx_.user_var_exprs_));
  } else if (OB_UNLIKELY(1 != node->num_child_) || OB_ISNULL(node->children_) ||
             OB_ISNULL(node->children_[0])) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid node children for get user_val", K(ret), K(node->num_child_));
  } else if (!ctx_.is_variable_allowed_) {
    ret = OB_ERR_VIEW_SELECT_CONTAIN_QUESTIONMARK;
    LOG_WARN("View's SELECT contains a variable or parameter", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_get_user_var(
      ctx_.expr_factory_,
      ObString(static_cast<int32_t>(node->children_[0]->str_len_), node->children_[0]->str_value_),
      expr,
      NULL,
      ctx_.query_ctx_,
      ctx_.user_var_exprs_))) {
    LOG_WARN("build get user var failed", K(ret));
  } else {
    ObVarInfo var_info;
    var_info.name_.assign_ptr(node->children_[0]->str_value_,
                              static_cast<int32_t>(node->children_[0]->str_len_));
    var_info.type_ = USER_VAR;
    if (OB_FAIL(ctx_.sys_vars_->push_back(var_info))) {
      SQL_ENG_LOG(WARN, "fail to push back var info", K(ret), K(var_info.name_));
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_not_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *sub_expr = NULL;
  ObOpRawExpr *not_expr = NULL;
  if (OB_ISNULL(node)
      || OB_UNLIKELY(1 != node->num_child_)
      || OB_ISNULL(node->children_)
      || node->type_ != T_OP_NOT
      || OB_ISNULL(ctx_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid not expr or session_info_", K(node), KP(ctx_.session_info_));
  } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], sub_expr)))) {
    LOG_WARN("resolve child expr failed", K(ret));
  } else if (OB_ISNULL(sub_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), K(sub_expr));
  } else if (T_OP_EXISTS == sub_expr->get_expr_type()) {
    sub_expr->set_expr_type(T_OP_NOT_EXISTS);
    expr = sub_expr;
  } else if (T_OP_NOT_EXISTS == sub_expr->get_expr_type()) {
    sub_expr->set_expr_type(T_OP_EXISTS);
    expr = sub_expr;
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, not_expr))) {
    LOG_WARN("create ObOpRawExpr failed", K(ret));
  } else if (OB_FAIL(not_expr->set_param_expr(sub_expr))) {
    LOG_WARN("failed to add param expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::try_add_bool_expr(not_expr, ctx_.expr_factory_))) {
    LOG_WARN("try_add_bool_expr failed", K(ret));
  } else if (OB_ISNULL(expr = not_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), K(expr));
  }
  return ret;
}

int ObRawExprResolverImpl::process_pos_or_neg_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  int64_t neg_cnt = 0;
  int64_t remain_neg_cnt = 0;
  const ParseNode *cur_expr = node;
  if (OB_ISNULL(cur_expr) || (T_OP_POS != cur_expr->type_ && T_OP_NEG != cur_expr->type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(cur_expr));
  }
  for (; OB_SUCC(ret) && NULL != cur_expr && (cur_expr->type_ == T_OP_POS || cur_expr->type_ == T_OP_NEG);) {
    if (OB_UNLIKELY(1 != cur_expr->num_child_) || OB_ISNULL(cur_expr->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid cur expr", K(ret), K(cur_expr->num_child_));
    } else {
      if (cur_expr->type_ == T_OP_NEG) {
        neg_cnt += 1;
      }
      cur_expr = cur_expr->children_[0];
    }
  } // end for
  ObRawExpr *sub_expr = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(SMART_CALL(recursive_resolve(cur_expr, sub_expr)))) {
      LOG_WARN("resolve child expr failed", K(ret));
    } else if (OB_FAIL(try_negate_const(sub_expr, neg_cnt, remain_neg_cnt))) {
      LOG_WARN("try negate const failed", K(ret));
    } else {
      expr = sub_expr;
      ObOpRawExpr *neg = NULL;
      for (int64_t i = 0; i < remain_neg_cnt && OB_SUCC(ret); i++) {
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_NEG, neg))) {
          LOG_WARN("create ObOpRawExpr failed", K(ret));
        } else if (OB_ISNULL(expr = neg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("op expr is null");
        } else if (OB_FAIL(neg->set_param_expr(sub_expr))) {
          LOG_WARN("failed to add param expr", K(ret));
        } else {
          sub_expr = expr;
        }
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_operator_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *sub_expr1 = NULL;
  ObRawExpr *sub_expr2 = NULL;
  ObOpRawExpr *b_expr = NULL;
  bool happened = false;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node));
  } else if (OB_UNLIKELY(2 != node->num_child_) || OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children", K(ret), K_(node->num_child), K_(node->children), K_(node->type));
  } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], sub_expr1)))) {
    LOG_WARN("resolve left child failed", K(ret));
  } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[1], sub_expr2)))) {
    LOG_WARN("resolve right child failed", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, b_expr))) {
    LOG_WARN("create ObOpRawExpr failed", K(ret));
  } else if (OB_ISNULL(expr = b_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (OB_FAIL(b_expr->set_param_exprs(sub_expr1, sub_expr2))) {
    LOG_WARN("failed to add param expr", K(ret));
  } else if (OB_FAIL(convert_any_or_all_expr(expr, happened))) {
    LOG_WARN("failed to convert any all expr", K(ret));
  } else if (lib::is_oracle_mode() && !happened &&
             T_OP_ROW != sub_expr1->get_expr_type() &&
             T_OP_ROW != sub_expr2->get_expr_type()) {
    LOG_DEBUG("==============ORACLE MODE");
    if (OB_ISNULL(ctx_.op_exprs_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctx_.op_exprs_ is null", K(ctx_.op_exprs_), K(ret));
    } else {
      ctx_.op_exprs_->push_back(b_expr);
      LOG_DEBUG("ctx_.op_exprs_ push", K(b_expr->get_param_count()), K(*b_expr));
    }
  }
  return ret;
}

int ObRawExprResolverImpl::convert_any_or_all_expr(ObRawExpr *&expr,
                                                   bool &happened)
{
  int ret = OB_SUCCESS;
  ObRawExpr *sub_expr1 = NULL;
  ObRawExpr *sub_expr2 = NULL;
  ObRawExpr *sub_expr2_child = NULL;
  ObOpRawExpr *op_expr = NULL;
  happened = false;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_.op_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(expr), K(ctx_.op_exprs_), K(ret));
  } else if (T_OP_EQ > expr->get_expr_type()
             || T_OP_NE < expr->get_expr_type()) {
    /*do nothing*/
  } else if (OB_UNLIKELY(2 != expr->get_param_count()) ||
             OB_ISNULL(sub_expr1 = expr->get_param_expr(0)) ||
             OB_ISNULL(sub_expr2 = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(expr->get_param_count()),
        K(sub_expr1), K(sub_expr2), K(ret));
  } else if (T_ANY != sub_expr2->get_expr_type() &&
             T_ALL != sub_expr2->get_expr_type()) {
    /*do nothing*/
  } else if (OB_UNLIKELY(1 != sub_expr2->get_param_count()) ||
             OB_ISNULL(sub_expr2_child = sub_expr2->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(sub_expr2->get_param_count()),
        K(sub_expr2_child), K(ret));
  } else if (T_OP_ROW != sub_expr2_child->get_expr_type()) {
    /*do nothing*/
  } else if (T_OP_EQ == expr->get_expr_type() &&
             T_ANY == sub_expr2->get_expr_type() &&
             OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_IN, op_expr))) {
     LOG_WARN("create ObOpRawExpr failed", K(ret));
  } else if (T_OP_NE == expr->get_expr_type() &&
             T_ALL == sub_expr2->get_expr_type() &&
             OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_NOT_IN, op_expr))) {
      LOG_WARN("create ObOpRawExpr failed", K(ret));
  } else if (OB_NOT_NULL(op_expr)) {
    if (OB_FAIL(op_expr->set_param_exprs(sub_expr1, sub_expr2_child))) {
      LOG_WARN("failed to set param exprs", K(ret));
    } else if (T_OP_ROW != sub_expr1->get_expr_type() &&
               T_OP_ROW != sub_expr2_child->get_expr_type() &&
               OB_FAIL(ctx_.op_exprs_->push_back(op_expr))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else {
      happened = true;
      expr = op_expr;
      LOG_DEBUG("succeed to convert any/all expr", K(*expr));
    }
  } else if (T_ANY == sub_expr2->get_expr_type() &&
             (T_OP_EQ < expr->get_expr_type() && expr->get_expr_type() <= T_OP_NE) &&
             OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_OR, op_expr))) {
    LOG_WARN("failed to create or expr", K(ret));
  } else if (T_ALL == sub_expr2->get_expr_type() &&
             (T_OP_EQ <= expr->get_expr_type() && expr->get_expr_type() < T_OP_NE) &&
             OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_AND, op_expr))) {
    LOG_WARN("failed to create and expr", K(ret));
  } else if (OB_NOT_NULL(op_expr)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_expr2_child->get_param_count(); i++) {
      ObOpRawExpr *tmp_expr = NULL;
      if (OB_ISNULL(sub_expr2_child->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if ((T_OP_EQ != expr->get_expr_type() && T_OP_NE != expr->get_expr_type()) &&
                 (T_OP_ROW == sub_expr2_child->get_param_expr(i)->get_expr_type() ||
                 T_OP_ROW == sub_expr1->get_expr_type())) {
        //由于oracle不支持形如(2,3) < ((2,3)(2,4))向量列表,因此需要禁掉,'<='、'>'、'>='同理
        ret = OB_ERR_OPERATOR_CANNOT_BE_USED_WITH_LIST;
        LOG_WARN("this operator cannot be used with lists", K(ret));
      } else if (T_OP_EQ == expr->get_expr_type() &&
                 OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_EQ, tmp_expr))) {
        LOG_WARN("failed to create equal expr", K(ret));
      } else if (T_OP_NE == expr->get_expr_type() &&
                 OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_NE, tmp_expr))) {
        LOG_WARN("failed to create not equal expr", K(ret));
      } else if (T_OP_LT == expr->get_expr_type() &&
                 OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_LT, tmp_expr))) {
        LOG_WARN("failed to create less expr", K(ret));
      } else if (T_OP_LE == expr->get_expr_type() &&
                 OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_LE, tmp_expr))) {
        LOG_WARN("failed to create less equal expr", K(ret));
      } else if (T_OP_GT == expr->get_expr_type() &&
                 OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_GT, tmp_expr))) {
        LOG_WARN("failed to create great expr", K(ret));
      } else if (T_OP_GE == expr->get_expr_type() &&
                 OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_GE, tmp_expr))) {
        LOG_WARN("failed to create great equal expr", K(ret));
      } else if (OB_ISNULL(tmp_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(tmp_expr->set_param_exprs(sub_expr1,
                                                   sub_expr2_child->get_param_expr(i)))) {
        LOG_WARN("failed to set param exprs", K(ret));
      } else if (T_OP_ROW != sub_expr1->get_expr_type() &&
                 T_OP_ROW != sub_expr2_child->get_param_expr(i)->get_expr_type() &&
                 OB_FAIL(ctx_.op_exprs_->push_back(tmp_expr))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else if (sub_expr2_child->get_param_count() == 1) {//any/all仅仅只有一个row时，没有必要创建and/or
        op_expr = tmp_expr;
      } else if (OB_FAIL(op_expr->add_param_expr(tmp_expr))) {
        LOG_WARN("failed to add param exprs", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      happened = true;
      expr = op_expr;
      LOG_DEBUG("succeed to convert great equal any expr to great equal or expr", K(*expr));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }
  return ret;
}

int ObRawExprResolverImpl::process_is_or_is_not_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *sub_expr1 = NULL;
  ObRawExpr *sub_expr2 = NULL;
  ObOpRawExpr *b_expr = NULL;
  ObConstRawExpr *c_expr = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (OB_UNLIKELY(2 != node->num_child_) || OB_ISNULL(node->children_)
  || OB_ISNULL(node->children_[0])
  || OB_ISNULL(node->children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children", K(ret), K(node->num_child_), K(node->type_));
  } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], sub_expr1)))) {
    LOG_WARN("resolve left child failed", K(ret));
  } else {
    if (T_DEFAULT_NULL == node->children_[1]->type_) {
      ParseNode default_node;
      default_node.type_ = T_NULL;
      default_node.num_child_ = 0;
      default_node.value_ = INT64_MAX;
      default_node.str_len_ = 0;
      default_node.str_value_ = NULL;
      default_node.text_len_ = 0;
      default_node.raw_text_ = NULL;
      if (OB_FAIL(process_datatype_or_questionmark(default_node, sub_expr2))) {
        LOG_WARN("fail to resolver right child node", K(ret), "node_type", node->children_[1]->type_);
      }
    } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[1], sub_expr2)))) {
      LOG_WARN("resolve right child failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, b_expr))) {
      LOG_WARN("create ObOpRawExpr failed", K(ret));
    } else if (OB_ISNULL(b_expr) ) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", KP(b_expr), KP(c_expr));
    } else if (OB_FAIL(b_expr->set_param_exprs(sub_expr1, sub_expr2))) {
      LOG_WARN("failed to add param expr", K(ret));
    } else {
      expr = b_expr;
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_regexp_or_not_regexp_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *t_expr = NULL;
  int64_t num_child = 2;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (OB_FAIL(process_node_with_children(node, num_child, t_expr))) {
    LOG_WARN("fail to process node with children", K(ret));
  } else if (OB_ISNULL(t_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("process node fail with invalid expr", K(ret), K(t_expr));
  } else {
    // convert `A not regex B' to `NOT A regex B'
    if (T_OP_NOT_REGEXP != node->type_) {
      t_expr->set_expr_type(node->type_);
    } else {
      t_expr->set_expr_type(T_OP_REGEXP);
      ObOpRawExpr *not_expr = NULL;
      if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_NOT, not_expr))) {
        LOG_WARN("create ObOpRawExpr failed", K(ret));
      } else if (OB_ISNULL(not_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not expr is null");
      } else if (OB_FAIL(not_expr->set_param_expr(t_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else {
        t_expr = not_expr;
      }
    }
    expr = t_expr;
  }
  return ret;
}

int ObRawExprResolverImpl::process_between_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  const int64_t BTW_PARAM_NUM = 3;
  // Why BTW_PARAM_NUM + 1 ?
  // The content of the 4th raw expr is same to that of the 1st raw expr.
  // But the ptr addresses need to be different because our optimizer relys on it.
  ObRawExpr *btw_params[BTW_PARAM_NUM + 1];

  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (node->type_ != T_OP_BTW && node->type_ != T_OP_NOT_BTW) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid node type", K(node->type_), K(ret));
  } else if (3 != node->num_child_
             || OB_ISNULL(node->children_)
             || OB_ISNULL(node->children_[0])
             || OB_ISNULL(node->children_[1])
             || OB_ISNULL(node->children_[2])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid node", K(ret));
  } else {
    bool can_transform_in_mysql_mode = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < BTW_PARAM_NUM; ++i ) {
      if (OB_FAIL(recursive_resolve(node->children_[i], btw_params[i]))) {
        SQL_RESV_LOG(WARN, "resolve child expr failed", K(ret), K(i));
      } else if (OB_ISNULL(btw_params[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && lib::is_mysql_mode()) {
      if (OB_ISNULL(ctx_.session_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(ctx_.session_info_));
      } else if (OB_FAIL(btw_params[1]->extract_info())) {
        LOG_WARN("fail to extract info child 2 of between node", K(ret));
      } else if (OB_FAIL(btw_params[2]->extract_info())) {
        LOG_WARN("fail to extract info child 3 of between node", K(ret));
      } else if (btw_params[1]->has_flag(CNT_PL_UDF)
                 || btw_params[2]->has_flag(CNT_PL_UDF)) {
        // skip rewrite, relevant issue :
      } else if (btw_params[1]->is_const_expr()
                 && btw_params[2]->is_const_expr()) {
        // We will transform if the second and the third param are both const value or const expr
        // and the result meta types of them are same.
        // Why need const? Because 'resolve_columns' is called later than 'expr_resolver.resolve'.
        if (OB_FAIL(btw_params[1]->deduce_type(ctx_.session_info_))) {
          LOG_WARN("fail to deduce_type child 2 of between node", K(ret));
        } else if (OB_FAIL(btw_params[2]->deduce_type(ctx_.session_info_))) {
          LOG_WARN("fail to deduce_type child 3 of between node", K(ret));
        } else if (btw_params[1]->get_result_meta() == btw_params[2]->get_result_meta()) {
          can_transform_in_mysql_mode = true;
        }
      }
    }
    // The content of the 4th raw expr is same to that of the 1st raw expr.
    // But the ptr addresses need to be different because our optimizer relys on it.
    if (OB_SUCC(ret)) {
      if (lib::is_mysql_mode() && !can_transform_in_mysql_mode) {
        // do nothing
      } else {
        if (OB_FAIL(recursive_resolve(node->children_[0], btw_params[BTW_PARAM_NUM]))) {
          SQL_RESV_LOG(WARN, "resolve child expr failed", K(ret), K(BTW_PARAM_NUM));
        } else if (OB_ISNULL(btw_params[BTW_PARAM_NUM])) {
          LOG_WARN("unexpected null", K(ret), K(BTW_PARAM_NUM));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if ((lib::is_oracle_mode() || can_transform_in_mysql_mode)) {
        // convert between expr to >= and <=
        if (OB_FAIL(transform_between_expr(btw_params, expr, T_OP_NOT_BTW == node->type_))) {
          LOG_WARN("fail to convert between expr", K(ret));
        }
      } else {
        ObOpRawExpr *btw_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, btw_expr))) {
          LOG_WARN("fail to create raw expr", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < BTW_PARAM_NUM; ++i) {
            if (OB_FAIL(btw_expr->add_param_expr(btw_params[i]))) {
              LOG_WARN("fail to set param expr", K(ret), K(i));
            }
          }
        }
        expr = btw_expr;
      }
    }
  }

  return ret;
}

// We will transform between to >= and <= all the time in oracle mode
// or while the result meta type of second param and the third param are same in mysql mode
// expr1 NOT BETWEEN expr2 AND expr3 ==>  NOT (expr1 >= expr2 AND expr1 <= expr3)
// expr1 BETWEEN expr2 AND expr3 ==> expr1 >= expr2 AND expr1 <= expr3
int ObRawExprResolverImpl::transform_between_expr(
    ObRawExpr **btw_params, ObRawExpr *&expr, const bool is_not_btw)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *ge_expr = NULL;
  ObOpRawExpr *le_expr = NULL;
  ObOpRawExpr *and_expr = NULL;
  if (OB_ISNULL(ctx_.session_info_) || OB_ISNULL(ctx_.op_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_.session_info_), K(ctx_.op_exprs_));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_GE, ge_expr))) {
    LOG_WARN("failed to create great equal expr", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_LE, le_expr))) {
    LOG_WARN("failed to create less equal expr", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_AND, and_expr))) {
    LOG_WARN("failed to create a new expr", K(ret));
  } else if (OB_ISNULL(ge_expr) || OB_ISNULL(le_expr) || OB_ISNULL(and_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ge_expr), K(le_expr), K(and_expr));
  } else if (OB_FAIL(add_params_to_op_expr(btw_params[0], btw_params[1], ge_expr))) {
    LOG_WARN("add param to op_expr failed", K(ret), K(btw_params[0]), K(btw_params[1]), K(ge_expr));
  } else if (OB_FAIL(add_params_to_op_expr(btw_params[3], btw_params[2], le_expr))) {
    LOG_WARN("add param to op_expr failed", K(ret), K(btw_params[3]), K(btw_params[2]), K(le_expr));
  } else if (OB_FAIL(and_expr->add_param_expr(ge_expr))) {
    LOG_WARN("add ge_expr to or expr failed", K(ret), K(ge_expr), K(le_expr), K(and_expr));
  } else if (OB_FAIL(and_expr->add_param_expr(le_expr))) {
    LOG_WARN("add le_expr to or expr failed", K(ret), K(ge_expr), K(le_expr), K(and_expr));
  }
  if (OB_SUCC(ret)) {
    if (is_not_btw) { // T_OP_NOT_BTW == node->type_
      // expr1 NOT BETWEEN expr2 AND expr3 ==>  NOT (expr1 >= expr2 AND expr1 <= expr3)
      ObOpRawExpr *not_expr = NULL;
      if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_NOT, not_expr))) {
        LOG_WARN("failed to create a new expr", K(ret));
      } else if (OB_ISNULL(not_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to create not expr", K(ret));
      } else if (OB_FAIL(not_expr->set_param_expr(and_expr))) {
        LOG_WARN("failed to set param for not op", K(ret), K(*not_expr));
      } else {
        expr = not_expr; // T_OP_NOT_BTW
      }
    } else {
      expr = and_expr; // T_OP_BTW
    }
  }

  return ret;
}

int ObRawExprResolverImpl::add_params_to_op_expr(
    ObRawExpr *op_param_1, ObRawExpr *op_param_2, ObOpRawExpr *op_expr)
{
  int ret = OB_SUCCESS;
  bool happened = false;
  ObRawExpr *expr = NULL;

  if (OB_ISNULL(op_param_1) || OB_ISNULL(op_param_2) || OB_ISNULL(op_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret), K(op_param_1), K(op_param_2), K(op_expr));
  } else if (OB_ISNULL(expr = op_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (OB_FAIL(op_expr->set_param_exprs(op_param_1, op_param_2))) {
    LOG_WARN("failed to add param expr", K(ret));
  } else if (OB_FAIL(convert_any_or_all_expr(expr, happened))) {
    LOG_WARN("failed to convert any all expr", K(ret));
  } else if (lib::is_oracle_mode() && !happened &&
             T_OP_ROW != op_param_1->get_expr_type() &&
             T_OP_ROW != op_param_2->get_expr_type()) {
    LOG_DEBUG("==============ORACLE MODE");
    if (OB_ISNULL(ctx_.op_exprs_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctx_.op_exprs_ is null", K(ctx_.op_exprs_), K(ret));
    } else {
      ctx_.op_exprs_->push_back(op_expr);
      LOG_DEBUG("ctx_.op_exprs_ push", K(op_expr->get_param_count()), K(*op_expr));
    }
  }

  return ret;
}

int ObRawExprResolverImpl::process_like_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *t_expr = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if ((T_OP_NOT_LIKE == node->type_ || T_OP_LIKE == node->type_)
      && (node->num_child_ == 3)) {
    // 如果显式指定了escape sign 为'', oracle模式应该报错
    ParseNode* &escape_node = node->children_[2];
    if (OB_ISNULL(escape_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected escape node", K(ret), K(escape_node));
    } else if (T_QUESTIONMARK == escape_node->type_) {
      // 如果经过了fast parser, 那么escape 会被替换为?, 需要去param list中取真正的值
      if (OB_ISNULL(ctx_.param_list_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param_list_ is null", K(ret), K(ctx_.param_list_));
      } else if (ctx_.param_list_->empty() && nullptr != ctx_.secondary_namespace_) {
        const pl::ObPLSymbolTable* symbol_table = NULL;
        const pl::ObPLVar* var = NULL;
        CK (OB_NOT_NULL(symbol_table = ctx_.secondary_namespace_->get_symbol_table()));
        CK (OB_NOT_NULL(var = symbol_table->get_symbol(escape_node->value_)));
        if (0 == var->get_name().case_compare(pl::ObPLResolver::ANONYMOUS_ARG) &&
            NULL != var->get_pl_data_type().get_data_type() &&
            ObNullType == var->get_pl_data_type().get_data_type()->get_obj_type()) {
          ret = OB_ERR_INVALID_ESCAPE_CHAR_LENGTH;
          LOG_WARN("invalid escape char length, expect 1, get 0", K(ret));
        }
      } else if (escape_node->value_ < 0 || escape_node->value_ >= ctx_.param_list_->count()) {
        if (OB_NOT_NULL(ctx_.session_info_) && ctx_.session_info_->is_varparams_sql_prepare()) {
          // skip check question mark about escape node in prepare statement
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index is out of range", K(escape_node->value_), K(ctx_.param_list_->count()));
        }
        // c1 like '123' escape null is illegal in oracle mode, but legal in mysql mode
      } else if (ctx_.param_list_->at(escape_node->value_).is_null() && lib::is_oracle_mode()) {
        ret = OB_ERR_INVALID_ESCAPE_CHAR_LENGTH;
        LOG_WARN("invalid escape char length, expect 1, get 0", K(ret));
      } else if (ctx_.param_list_->at(escape_node->value_).is_null()) {
        // escape null is legal in mysql
      } else {
        // Note: currently when like node has escape, it's childs will not be parameterized and
        // therefore this line is meaningless. However when you wanna to open this, this line may
        // be somewhat useful.
        //escape = ctx_.param_list_->at(escape_node->value_).get_string()[0];
      }
    } else if (((T_CHAR == escape_node->type_ && 0 == escape_node->str_len_)
        || T_NULL == escape_node->type_) && lib::is_oracle_mode()) {
      ret = OB_ERR_INVALID_ESCAPE_CHAR_LENGTH;
      LOG_WARN("invalid escape char length, expect 1, get 0", K(ret));
    }
  } else {
    // do nothing
  }

  if (OB_FAIL(ret)){
  } else if (OB_FAIL(process_node_with_children(node, node->num_child_, t_expr))) {
    LOG_WARN("fail to process node with children", K(ret), K(node));
  } else if (OB_ISNULL(t_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("process node fail with invalid expr", K(ret), K(t_expr));
  } else {
    //如果有两个child_,则说明like 或 not like的escape参数没有显示给出，这里给默认值'\\'
    if ((T_OP_NOT_LIKE == node->type_ || T_OP_LIKE == node->type_)
        && (2 == node->num_child_)) {
      ObRawExpr *escape_expr = NULL;
      ParseNode escape_node;
      escape_node.type_ = T_VARCHAR;
      escape_node.num_child_ = 0;
      escape_node.value_ = INT64_MAX;
      escape_node.str_len_ = 1;
      escape_node.str_value_ = "\\";
      escape_node.text_len_ = 0;
      escape_node.raw_text_ = NULL;

      /*
      bugfix:
      in NO_BACKSLASH_ESCAPES mode, 'like BINARY xxx' stmt should also set the escapes as null, instead of '\' 
      */
      bool no_escapes = false;
      if (node->children_[1]->type_ == T_FUN_SYS && node->children_[1]->num_child_ == 2
          && node->children_[1]->children_[0]->str_len_ ==4 
          && (0 == strcmp(node->children_[1]->children_[0]->str_value_, "cast"))
          && node->children_[1]->children_[1]->num_child_ == 2 // T_EXPR_LIST node
          && node->children_[1]->children_[1]->children_[1]->int16_values_[OB_NODE_CAST_TYPE_IDX] == T_VARCHAR
          && node->children_[1]->children_[1]->children_[1]->int16_values_[OB_NODE_CAST_COLL_IDX] == BINARY_COLLATION) {
        IS_NO_BACKSLASH_ESCAPES(ctx_.session_info_->get_sql_mode(), no_escapes);
      }
      if (OB_FAIL(process_datatype_or_questionmark(escape_node, escape_expr))) {
        LOG_WARN("fail to resolver default excape node", K(ret));
      } else if (OB_FAIL(t_expr->add_param_expr(escape_expr))) {
        LOG_WARN("fail to set param expr");
      } else if (lib::is_oracle_mode() || no_escapes) {
        // Oracle mode, if not specify escape, then no escape, but the implementation of like must contain escape
        // so we rewrite like without escape
        // c1 like '%x\x%' --> c1 like replace('%x\x%', '\','\\') escape '\' -> c1 like '%x\\x%' escape '\'
        ObRawExpr *replace_expr1 = NULL;
        ObRawExpr *replace_expr2 = NULL;
        ObSysFunRawExpr *replace_expr = NULL;
        ParseNode replace_node;
        replace_node.type_ = T_VARCHAR;
        replace_node.num_child_ = 0;
        replace_node.value_ = INT64_MAX;
        replace_node.str_len_ = 2;
        replace_node.str_value_ = "\\\\";
        replace_node.text_len_ = 0;
        replace_node.raw_text_ = NULL;
        if (3 != t_expr->get_param_count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("like expr with escape must contain 3 arguments");
        } else {
          if (OB_FAIL(process_datatype_or_questionmark(escape_node, replace_expr1))) {
            LOG_WARN("fail to resolve replace expr1", K(ret));
          } else if (OB_FAIL(process_datatype_or_questionmark(replace_node, replace_expr2))) {
            LOG_WARN("fail to resolve replace expr2");
          } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, replace_expr))) {
            LOG_WARN("create ObOpRawExpr failed", K(ret));
          } else if (OB_ISNULL(replace_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("func_expr is null");
          } else if (OB_FAIL(replace_expr->add_param_expr(t_expr->get_param_expr(1)))) {
            LOG_WARN("fail to set param expr");
          } else if (OB_FAIL(replace_expr->add_param_expr(replace_expr1))) {
            LOG_WARN("fail to set param expr");
          } else if (OB_FAIL(replace_expr->add_param_expr(replace_expr2))) {
            LOG_WARN("fail to set param expr");
          } else {
            replace_expr->set_func_name(N_REPLACE);
            if (OB_FAIL(t_expr->replace_param_expr(1, replace_expr))) {
              LOG_WARN("fail to replace param expr");
            }
          }
        }
      } else {
        //do nothing
      }
    }
    // convert `A not like B' to `NOT A like B'
    if (OB_SUCC(ret)) {
      ObOpRawExpr *not_expr = NULL;
      if (T_OP_NOT_LIKE != node->type_) {
        t_expr->set_expr_type(node->type_);
      } else {
        t_expr->set_expr_type(T_OP_LIKE);
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_NOT, not_expr))) {
          LOG_WARN("create ObOpRawExpr failed", K(ret));
        } else if (OB_ISNULL(not_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not_expr is null");
        } else if (OB_FAIL(not_expr->set_param_expr(t_expr))) {
          LOG_WARN("failed to add param expr", K(ret));
        } else {
          t_expr = not_expr;
        }
      }
      expr = t_expr;
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_in_or_not_in_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *in_expr = NULL;
  ObRawExpr *sub_expr1 = NULL;
  ObRawExpr *sub_expr2 = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (OB_UNLIKELY(2 != node->num_child_) || OB_ISNULL(node->children_)
  || OB_ISNULL(node->children_[0])
  || OB_ISNULL(node->children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children", K(ret), K(node->children_));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_INVALID, in_expr))) {
    LOG_WARN("create ObOpRawExpr failed", K(ret));
  } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], sub_expr1)))) {
    LOG_WARN("resolve left raw expr failed", K(ret));
  } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[1], sub_expr2)))) {
    LOG_WARN("resolve right child failed", K(ret));
  } else if (OB_ISNULL(sub_expr1) || OB_ISNULL(sub_expr2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resolve get invalid expr", K(ret), K(sub_expr1), K(sub_expr2));
  } else {
    ObItemType param_type2 = sub_expr2->get_expr_type();
    LOG_DEBUG("in or not in with:", K(*sub_expr1), K(*sub_expr2), K(lib::is_oracle_mode()));
    if (T_REF_QUERY == param_type2) {
      ObQueryRefRawExpr *sub_ref = static_cast<ObQueryRefRawExpr*>(sub_expr2);
      sub_ref->set_is_set(true);
      //rewrite expr in sub-query as expr = ANY(sub-query)
      //rewrite expr not in sub-query as expr != ALL(sub-query)
      ObOpRawExpr *u_expr = NULL;
      if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_INVALID, u_expr))) {
        LOG_WARN("create ObOpRawExpr failed", K(ret));
      } else if (OB_ISNULL(in_expr) || OB_ISNULL(u_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", KP(in_expr), KP(u_expr));
      } else {
        ObItemType u_expr_type = (T_OP_IN == node->type_) ? T_ANY : T_ALL;
        ObItemType in_expr_type = (T_OP_IN == node->type_) ? T_OP_EQ : T_OP_NE;
        u_expr->set_expr_type(u_expr_type);
        if (OB_FAIL(u_expr->set_param_expr(sub_expr2))) {
          LOG_WARN("failed to add param expr", K(ret));
        } else if (OB_FAIL(in_expr->set_param_exprs(sub_expr1, u_expr))) {
          LOG_WARN("failed to add param expr", K(ret));
        } else {
          in_expr->set_expr_type(in_expr_type);
          expr = in_expr;
        }
      }
    } else if (T_OP_ROW == param_type2) {
      ObOpRawExpr *row_expr = static_cast<ObOpRawExpr *>(sub_expr2);
      if (OB_ISNULL(row_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to cast ObOpRawExpr", K(ret));
      } else if (1 == row_expr->get_param_count()) {
        ObRawExpr *param = lib::is_oracle_mode() ? row_expr : row_expr->get_param_expr(0);
        if (OB_FAIL(in_expr->set_param_exprs(sub_expr1, param))) {
          LOG_WARN("failed to add param expr", K(ret));
        } else {
          ObItemType expr_type = (T_OP_IN == node->type_) ? T_OP_EQ : T_OP_NE;
          in_expr->set_expr_type(expr_type);
          expr = in_expr;
        }
      } else {
        if (OB_FAIL(in_expr->set_param_exprs(sub_expr1, sub_expr2))) {
          LOG_WARN("failed to add param expr", K(ret));
        } else {
          in_expr->set_expr_type(node->type_);
          expr = in_expr;
        }
      }
      LOG_DEBUG("final in or not in expr ", K(*in_expr));
    } else {
      if (OB_FAIL(in_expr->set_param_exprs(sub_expr1, sub_expr2))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else {
        ObItemType expr_type = (T_OP_IN == node->type_) ? T_OP_EQ : T_OP_NE;
        in_expr->set_expr_type(expr_type);
        expr = in_expr;
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_case_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObCaseOpRawExpr *case_expr = NULL;
  ObRawExpr *arg_expr = NULL;
  bool all_then_null = true;
  if (OB_ISNULL(node) || OB_UNLIKELY(3 != node->num_child_) ||
      OB_UNLIKELY(T_CASE != node->type_) || OB_ISNULL(ctx_.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ParseNode or session_info_", K(node), KP(ctx_.session_info_));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_INVALID, case_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(case_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("case expr is null");
  } else if (NULL != node->children_ && NULL != node->children_[0]) {
    if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], arg_expr)))) {
      LOG_WARN("fail to recursive resolve children", K(ret));
    } else {
      case_expr->set_arg_param_expr(arg_expr);
      case_expr->set_expr_type(T_OP_ARG_CASE);
    }
  } else {
    case_expr->set_expr_type(T_OP_CASE);
  }
  if (OB_SUCC(ret)) {
    ParseNode *when_node = NULL;
    ObRawExpr *when_expr = NULL;
    ObRawExpr *then_expr = NULL;
    if (OB_ISNULL(node->children_) || OB_ISNULL(node->children_[1]) || T_WHEN_LIST != node->children_[1]->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid case children", K(node), K(node->children_));
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < node->children_[1]->num_child_; i++) {
      if (OB_ISNULL(node->children_[1]->children_) || OB_ISNULL(node->children_[1]->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid children", K(node->children_[1]->children_));
      } else {
        when_node = node->children_[1]->children_[i];
        if (OB_UNLIKELY(2 != when_node->num_child_) || OB_ISNULL(when_node->children_)
        || OB_ISNULL(when_node->children_[0])
        || OB_ISNULL(when_node->children_[1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid when node children", K(when_node->children_));
        } else if (OB_FAIL(SMART_CALL(recursive_resolve(when_node->children_[0], when_expr)))) {
          LOG_WARN("fail to recursive resolver", K(ret), K(when_node->children_[0]));
        } else if (OB_FAIL(SMART_CALL(recursive_resolve(when_node->children_[1], then_expr)))) {
          LOG_WARN("fail to recursive resolve", K(ret), K(when_node->children_[1]));
        } else if (OB_FAIL(case_expr->add_when_param_expr(when_expr))) {
          LOG_WARN("Add when expression failed", K(ret));
        } else if (OB_FAIL(case_expr->add_then_param_expr(then_expr))) {
          LOG_WARN("Add then expression failed", K(ret));
        } else if(T_QUESTIONMARK == then_expr->get_expr_type()) {
          if (then_expr->get_result_meta().get_type() != ObNullType) {
            all_then_null = false;
          }
        } else if (then_expr->get_expr_type() != T_NULL) {
          all_then_null = false;
        }
      } //end else
    } // end for
  }
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[2]) {
      ObRawExpr *default_expr = NULL;
      if (T_DEFAULT_NULL == node->children_[2]->type_) {
        ParseNode default_node;
        default_node.type_ = T_NULL;
        default_node.num_child_ = 0;
        default_node.value_ = INT64_MAX;
        default_node.str_len_ = 0;
        default_node.str_value_ = NULL;
        default_node.text_len_ = 0;
        default_node.raw_text_ = NULL;
        if (OB_FAIL(process_datatype_or_questionmark(default_node, default_expr))) {
          LOG_WARN("fail to resolver defalut excape node", K(ret));
        }
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[2], default_expr)))) {
        LOG_WARN("fail to recursive resolve", K(ret), K(node->children_[2]));
      }
      if (OB_SUCC(ret)){
        if (T_QUESTIONMARK == default_expr->get_expr_type()) {
          if (default_expr->get_result_meta().get_type() != ObNullType) {
            all_then_null = false;
          }
        } else if (default_expr->get_expr_type() != T_NULL) {
          all_then_null = false;
        }
        case_expr->set_default_param_expr(default_expr);
      }
    }
  }
  if (OB_SUCC(ret) && all_then_null && is_oracle_mode() && T_PL_SCOPE == ctx_.current_scope_) {
    ret = OB_ERR_CASE_NULL;
    LOG_WARN("at least one result in the CASE expression must not be NULL", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRawExprUtils::try_add_bool_expr(case_expr, ctx_.expr_factory_))) {
      LOG_WARN("try_add_bool_expr for case expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    expr = case_expr;
  }
  return ret;
}

int ObRawExprResolverImpl::process_sub_query_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr *sub_query_expr = NULL;
  if (OB_ISNULL(node) || OB_ISNULL(ctx_.sub_query_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node), K_(ctx_.sub_query_info));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_REF_QUERY, sub_query_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(expr = sub_query_expr)
             || OB_ISNULL(ctx_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KP(expr), KP(ctx_.session_info_), K(ret));
  } else if (node->is_multiset_ && ctx_.current_scope_ == T_PL_SCOPE) {
    ret = OB_ERR_INVALID_SUBQUERY_USE;
    LOG_WARN("subquery not allowed in this context", K(ret));
  } else {
    sub_query_expr->set_cursor(1 == node->value_);
    sub_query_expr->set_is_multiset(node->is_multiset_);

    ObSubQueryInfo sq_info;
    sq_info.sub_query_ = node;
    sq_info.ref_expr_ = sub_query_expr;
    sq_info.parents_expr_info_ = ctx_.parents_expr_info_;
    if (OB_FAIL(ctx_.sub_query_info_->push_back(sq_info))) {
      LOG_WARN("resolve sub-query failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_agg_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *sub_expr = NULL;
  ObAggFunRawExpr *agg_expr = NULL;
  if (OB_ISNULL(ctx_.aggr_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr exprs is null", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, agg_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(agg_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("agg_expr is null");
  } else if (OB_FAIL(ctx_.aggr_exprs_->push_back(agg_expr))) {
    LOG_WARN("store aggr expr failed", K(ret));
  } else if (OB_UNLIKELY(1 > node->num_child_) || OB_ISNULL(node->children_)
      || (2 == node->num_child_ && OB_ISNULL(node->children_[1]) && node->type_ != T_FUN_ORA_XMLAGG)
      || (T_FUN_COUNT == node->type_ && (OB_ISNULL(node->children_[0])))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children", K(node->num_child_));
  } else {
    bool need_add_flag = !ctx_.parents_expr_info_.has_member(IS_AGG);
    if (need_add_flag && OB_FAIL(ctx_.parents_expr_info_.add_member(IS_AGG))) {
      LOG_WARN("failed to add member", K(ret));
    } else if (T_FUN_COUNT == node->type_ && 1 == node->num_child_) {
      if (OB_UNLIKELY(T_STAR != node->children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid node children", K(node), K(node->children_));
      }
      //when '*', do not set parameter
    } else if (T_FUN_COUNT == node->type_ && 2 == node->num_child_ && T_DISTINCT == node->children_[0]->type_) {
      ParseNode *expr_list_node = node->children_[1];
      if (OB_ISNULL(expr_list_node) || OB_UNLIKELY(T_EXPR_LIST != expr_list_node->type_)
      || OB_ISNULL(expr_list_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid children for COUNT function", K(node), K(expr_list_node));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < expr_list_node->num_child_; ++i) {
        sub_expr = NULL;
        if (OB_ISNULL(expr_list_node->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid expr list node children", K(ret), K(i), K(expr_list_node->children_[i]));
        } else if (OB_FAIL(SMART_CALL(recursive_resolve(expr_list_node->children_[i], sub_expr)))) {
          LOG_WARN("fail to recursive resolve expr list item", K(ret));
        } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
          LOG_WARN("fail to add param expr to agg expr", K(ret));
        }
      } //end for
    } else if (T_FUN_APPROX_COUNT_DISTINCT == node->type_ || T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == node->type_) {
      ParseNode *expr_list_node = node->children_[0];
      if (OB_ISNULL(expr_list_node) || OB_UNLIKELY(T_EXPR_LIST != expr_list_node->type_)
      || OB_ISNULL(expr_list_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid children for APPROX_COUNT_DISTINCT(_SYNOPSIS) function", K(node), K(expr_list_node));
      //oracle mode allow only 1 argument
      } else if (lib::is_oracle_mode() && expr_list_node->num_child_ != 1) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("get invalid number of arguments", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < expr_list_node->num_child_; ++i) {
        sub_expr = NULL;
        if (OB_ISNULL(expr_list_node->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid expr list node children", K(ret), K(i), K(expr_list_node->children_[i]));
        } else if (OB_FAIL(SMART_CALL(recursive_resolve(expr_list_node->children_[i], sub_expr)))) {
          LOG_WARN("fail to recursive resolve expr list item", K(ret));
        } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
          LOG_WARN("fail to add param expr to agg expr", K(ret));
        }
      } // end for
    } else if (T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE == node->type_) {
      sub_expr = NULL;
      if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], sub_expr)))) {
        LOG_WARN("fail to recursive resolve node child", K(ret), K(node->children_[0]));
      } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
        LOG_WARN("fail to add param expr", K(ret));
      }
    } else if (T_FUN_CORR == node->type_ || T_FUN_COVAR_POP == node->type_ ||
               T_FUN_COVAR_SAMP == node->type_ || T_FUN_REGR_SLOPE == node->type_  ||
               T_FUN_REGR_INTERCEPT == node->type_ || T_FUN_REGR_COUNT == node->type_ ||
               T_FUN_REGR_R2 == node->type_ || T_FUN_REGR_AVGX == node->type_ ||
               T_FUN_REGR_AVGY == node->type_ || T_FUN_REGR_SXX == node->type_ ||
               T_FUN_REGR_SYY == node->type_ || T_FUN_REGR_SXY == node->type_) {
      sub_expr = NULL;
      if (OB_UNLIKELY(3 != node->num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("op expected 3 childrens", K(ret));
      } else if (node->children_[0] != NULL && node->children_[0]->type_ == T_DISTINCT) {
        ret = OB_DISTINCT_NOT_ALLOWED;
        LOG_WARN("distinct not allowed in aggr", K(ret));
      } else if (OB_FAIL(recursive_resolve(node->children_[1], sub_expr))) {
        LOG_WARN("fail to recursive resolve expr list item", K(ret));
      } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
        LOG_WARN("fail to add param expr to agg expr", K(ret));
      } else if (OB_FAIL(recursive_resolve(node->children_[2], sub_expr))) {
        LOG_WARN("fail to recursive resolve expr list item", K(ret));
      } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
        LOG_WARN("fail to add param expr to agg expr", K(ret));
      }
    } else if (T_FUN_TOP_FRE_HIST == node->type_) {
      ObRawExpr *param_expr = NULL;
      ObRawExpr *error_expr = NULL;
      ObRawExpr *item_size_expr = NULL;
      ObSysFunRawExpr *window_size_expr = NULL;
      ObRawExpr *div_expr = NULL;
      ObConstRawExpr *one_expr = NULL;
      if (OB_ISNULL(ctx_.session_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(ctx_.session_info_));
      } else if (OB_UNLIKELY(3 != node->num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(node->num_child_));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], error_expr)))) {
        LOG_WARN("fail to recursive resolve expr item", K(ret));
      } else if (OB_ISNULL(error_expr) ||
                 OB_UNLIKELY(ObRawExpr::EXPR_CONST != error_expr->get_expr_class())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get invalid window_size_expr", K(ret), K(error_expr));
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(ctx_.expr_factory_,
                                                              ObIntType,
                                                              1,
                                                              one_expr))) {
        LOG_WARN("failed to build const int expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(ctx_.expr_factory_,
                                                                     T_OP_DIV,
                                                                     one_expr,
                                                                     error_expr,
                                                                     div_expr))) {
        LOG_WARN("failed to common binary op expr", K(ret));
      } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS_CEIL, window_size_expr))) {
        LOG_WARN("failed to create fun sys floor", K(ret));
      } else if (OB_ISNULL(window_size_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("floor expr is null", K(ret));
      } else if (OB_FAIL(window_size_expr->set_param_expr(div_expr))) {
        LOG_WARN("failed to set param expr", K(ret));
      } else if (OB_FAIL(agg_expr->add_real_param_expr(window_size_expr))) {
        LOG_WARN("fail to add param expr to agg expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[1], param_expr)))) {
        LOG_WARN("fail to recursive resolve expr item", K(ret));
      } else if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid param_expr", K(ret), K(param_expr));
      } else if (OB_FAIL(agg_expr->add_real_param_expr(param_expr))) {
        LOG_WARN("fail to add param expr to agg expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[2], item_size_expr)))) {
        LOG_WARN("fail to recursive resolve expr item", K(ret));
      } else if (OB_ISNULL(item_size_expr) ||
                 OB_UNLIKELY(ObRawExpr::EXPR_CONST != item_size_expr->get_expr_class())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get invalid item_size_expr", K(ret), K(item_size_expr));
      } else if (OB_FAIL(agg_expr->add_real_param_expr(item_size_expr))) {
        LOG_WARN("fail to add param expr to agg expr", K(ret));
      } else {/*do nothing*/}
    } else if (T_FUN_HYBRID_HIST == node->type_ || T_FUN_JSON_OBJECTAGG == node->type_) {
      if (OB_UNLIKELY(2 != node->num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error, node expected 2 arguments", K(ret), K(node->num_child_));
      } else {
        sub_expr = NULL;
        ObRawExpr *sub_expr2 = NULL;
        if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], sub_expr)))) {
          LOG_WARN("fail to recursive resolve node child", K(ret));
        } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
          LOG_WARN("fail to add param expr", K(ret));
        } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[1], sub_expr2)))) {
          LOG_WARN("fail to recursive resolve node child", K(ret));
        } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr2))) {
          LOG_WARN("fail to add param expr", K(ret));
        } else {
          OrderItem order_item;
          order_item.expr_ = sub_expr;
          order_item.order_type_ = NULLS_FIRST_ASC;
          if (OB_FAIL(agg_expr->add_order_item(order_item))) {
            LOG_WARN("fail to add median order item", K(ret));
          } else {/* do nothing */}
        }
      }
    } else if (T_FUN_GROUPING_ID == node->type_) {
      ParseNode *expr_list_node = node->children_[0];
      if (OB_ISNULL(expr_list_node) || OB_UNLIKELY(T_EXPR_LIST != expr_list_node->type_)
      || OB_ISNULL(expr_list_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid children for GOUPING_ID function", K(node), K(expr_list_node));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < expr_list_node->num_child_; ++i) {
          sub_expr = NULL;
          if (OB_ISNULL(expr_list_node->children_[i])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid expr list node children", K(ret), K(i), K(expr_list_node->children_[i]));
          } else if (OB_FAIL(SMART_CALL(recursive_resolve(expr_list_node->children_[i], sub_expr)))) {
            LOG_WARN("fail to recursive resolve expr list item", K(ret));
          } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
            LOG_WARN("fail to add param expr to agg expr", K(ret));
          }
        } // end for
      }
    } else if (T_FUN_ORA_JSON_ARRAYAGG == node->type_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
        sub_expr = NULL;
        if (OB_ISNULL(node->children_[i])) {
          // do nothing
        } else if (i == PARSE_JSON_ARRAYAGG_DISTINCT
                   && node->children_[PARSE_JSON_ARRAYAGG_DISTINCT]->type_ == T_DISTINCT) {
          ret = OB_DISTINCT_NOT_ALLOWED;
          LOG_WARN("distinct not allowed in json arrayagg", K(ret));
        } else if (i == PARSE_JSON_ARRAYAGG_DISTINCT
                   && node->children_[PARSE_JSON_ARRAYAGG_DISTINCT]->type_ == T_ALL) {
        } else if (i == PARSE_JSON_ARRAYAGG_ORDER) {
          const ParseNode *sort_list = NULL;
          if (OB_UNLIKELY(node->children_[i]->type_ != T_ORDER_BY)
              || OB_UNLIKELY(node->children_[i]->num_child_ != 2)
              || OB_ISNULL(node->children_[i]->children_)
              || OB_ISNULL(sort_list = node->children_[i]->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid parameter", K(node->children_[i]), K(sort_list), K(ret));
          } else if (NULL != node->children_[i]->children_[1]) {
            ret = OB_ERR_ORDER_SIBLINGS_BY_NOT_ALLOWED;
            LOG_WARN("should not be here, invalid agg node", K(ret), K(node));
          } else {
            for (int32_t i = 0; OB_SUCC(ret) && i < sort_list->num_child_; i++) {
              ParseNode *sort_node = sort_list->children_[i];
              OrderItem order_item;
              if (OB_FAIL(SMART_CALL(recursive_resolve(sort_node->children_[0], sub_expr)))) {
                LOG_WARN("fail to recursive resolve expr list item", K(ret));
              } else {
                order_item.expr_ = sub_expr;
                if (sort_node->children_[1]->value_ == 1) {
                  order_item.order_type_ = NULLS_LAST_ASC;
                } else {
                  order_item.order_type_ = NULLS_FIRST_DESC;
                }
                if (OB_FAIL(agg_expr->add_order_item(order_item))) {
                  LOG_WARN("Add order expression error", K(ret));
                }
              }
            }
          }
        } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[i], sub_expr)))) {
          LOG_WARN("fail to recursive resolve expr list item", K(ret));
        } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
          LOG_WARN("fail to add param expr to agg expr", K(ret));
        }
      } // end for
    } else if (T_FUN_ORA_JSON_OBJECTAGG == node->type_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
        sub_expr = NULL;
        ObString def_val(7, "default");
        if (OB_ISNULL(node->children_[i])) {
          // do nothing
        } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[i], sub_expr)))) {
          LOG_WARN("fail to recursive resolve expr list item", K(ret));
        } else if ((i == 4) && (0 == def_val.case_compare(node->children_[i]->raw_text_))) {
          (static_cast<ObConstRawExpr *>(sub_expr))->set_scale(1);
        }
        if (OB_SUCC(ret) && OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
          LOG_WARN("fail to add param expr to agg expr", K(ret));
        }
      } // end for
    } else if (T_FUN_ORA_XMLAGG == node->type_) {
      sub_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
        if (OB_ISNULL(node->children_[i])) {
          // do nothing
        } else if (i == 0) {
          // TODO Subsequent Interception Aggregation Function
          if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[i], sub_expr)))) {
            LOG_WARN("fail to resursive resolve expr list item", K(ret));
          } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
            LOG_WARN("fail to add param expr to agg expr", K(ret));
          }
        } else if (i == 1) {
          const ParseNode *sort_list = NULL;
          if (OB_UNLIKELY(node->children_[1]->type_ != T_ORDER_BY)
              || OB_UNLIKELY(node->children_[1]->num_child_ != 2)
              || OB_ISNULL(node->children_[1]->children_)
              || OB_ISNULL(sort_list = node->children_[1]->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid parameter", K(node->children_[1]));
          } else if (NULL != node->children_[1]->children_[1]) {
            ret = OB_ERR_CBY_OREDER_SIBLINGS_BY_NOT_ALLOWED;
            LOG_WARN("should not be here, invalid agg node");
          } else {
            for (int32_t i = 0; OB_SUCC(ret) && i < sort_list->num_child_; i++) {
              ParseNode *sort_node = sort_list->children_[i];
              OrderItem order_item;
              if (OB_FAIL(SMART_CALL(recursive_resolve(sort_node->children_[0], sub_expr)))) {
                LOG_WARN("fail to recursive_resolve expr list item", K(ret));
              } else if (OB_FAIL(ObResolverUtils::set_direction_by_mode(*sort_node, order_item))) {
                LOG_WARN("failed to set direction by mode", K(ret));
              } else {
                order_item.expr_ = sub_expr;
                if (OB_FAIL(agg_expr->add_order_item(order_item))) {
                  LOG_WARN("Add order expression error", K(ret));
                }
              }
            }
          }
        }
      }
    } else if (T_FUN_COUNT != node->type_
        || (T_FUN_COUNT == node->type_ && 2 == node->num_child_ && T_ALL == node->children_[0]->type_)
        || T_FUN_GROUPING == node->type_) {
      sub_expr = NULL;
      int64_t pos = (T_FUN_GROUPING == node->type_) ? 0 : 1;
      if ((T_FUN_VAR_POP == node->type_ || T_FUN_VAR_SAMP == node->type_ ||
           T_FUN_STDDEV_POP == node->type_ || T_FUN_STDDEV_SAMP == node->type_
           || T_FUN_MEDIAN == node->type_ || T_FUN_JSON_ARRAYAGG == node->type_) &&
          node->children_[0] != NULL && node->children_[0]->type_ == T_DISTINCT) {
        ret = OB_DISTINCT_NOT_ALLOWED;
        LOG_WARN("distinct not allowed in aggr", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[pos], sub_expr)))) {
        LOG_WARN("fail to recursive resolve node child", K(ret));
      } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
        LOG_WARN("fail to add param expr", K(ret));
      } else if (T_FUN_MEDIAN == node->type_) {
        OrderItem order_item;
        order_item.expr_ = sub_expr;
        order_item.order_type_ = NULLS_FIRST_ASC;
        if (OB_FAIL(agg_expr->add_order_item(order_item))) {
          LOG_WARN("fail to add median order item", K(ret));
        } else { /* do nothong */ }
      }
    }  else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be here, invalid agg node", K(ret), K(node));
    }

    if (OB_SUCC(ret)) {
      if (NULL != node->children_[0] && node->children_[0]->type_ == T_DISTINCT) {
        agg_expr->set_param_distinct(true);
      }
      // add invalid table bit index, avoid aggregate function expressions are used as filters
      if (OB_FAIL(agg_expr->get_relation_ids().add_member(0))) {
        LOG_WARN("failed to add member", K(ret));
      } else if (need_add_flag && (ctx_.parents_expr_info_.del_member(IS_AGG))) {
        LOG_WARN("failed to del member", K(ret));
      } else {
        expr = agg_expr;
      }
    }
  }
  OZ(param_not_row_check(expr));
  return ret;
}

int ObRawExprResolverImpl::process_group_aggr_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObAggFunRawExpr *agg_expr = NULL;
  if (OB_ISNULL(ctx_.aggr_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr_exprs_ is null");
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, agg_expr))) {
      LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(agg_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("agg_expr is null");
  } else if (OB_FAIL(ctx_.aggr_exprs_->push_back(agg_expr))) {
    LOG_WARN("store aggr expr failed", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid group concat node", K(ret), K(node->children_));
  } else {
    bool need_add_flag = !ctx_.parents_expr_info_.has_member(IS_AGG);
    ParseNode *expr_list_node = node->children_[1];
    if (need_add_flag && OB_FAIL(ctx_.parents_expr_info_.add_member(IS_AGG))) {
      LOG_WARN("failed to add member", K(ret));
    } else if (OB_ISNULL(expr_list_node) || OB_UNLIKELY(T_EXPR_LIST != expr_list_node->type_)
    || OB_ISNULL(expr_list_node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group_concat node with invalid children", K(ret), K(expr_list_node));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_list_node->num_child_; ++i) {
      ObRawExpr *sub_expr = NULL;
      if (OB_ISNULL(expr_list_node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr list node children", K(ret), K(i), K(expr_list_node->children_[i]));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(expr_list_node->children_[i], sub_expr)))) {
        LOG_WARN("fail to recursive resolve expr list item", K(ret));
      } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
        LOG_WARN("fail to add param expr to agg expr", K(ret));
      } else if ((T_FUN_GROUP_PERCENTILE_DISC == node->type_
                  || T_FUN_GROUP_PERCENTILE_CONT == node->type_)
                  && OB_UNLIKELY(1 < agg_expr->get_real_param_count())) {
        ret = OB_ERR_PARAM_SIZE;
        LOG_WARN("invalid number of arguments", K(ret),
                                                K(node->type_),
                                                K(agg_expr->get_real_param_count()));
      }
    } // end for

    if (OB_SUCC(ret)) {
      if (NULL != node->children_[0] && T_DISTINCT == node->children_[0]->type_) {
        if (T_FUN_GROUP_CONCAT != node->type_ ||
            T_FUN_GROUP_PERCENTILE_CONT == node->type_ ||
            T_FUN_GROUP_PERCENTILE_DISC == node->type_) {
          ret = OB_DISTINCT_NOT_ALLOWED;
          LOG_WARN("distinct not allowed in aggr", K(ret));
        } else {
          agg_expr->set_param_distinct(true);
        }
      }
      // add invalid table bit index, avoid aggregate function expressions are used as filters
      if (OB_SUCCESS == ret && OB_FAIL(agg_expr->get_relation_ids().add_member(0))) {
        LOG_WARN("failed to add member", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      //解析separator
      ObRawExpr *separator_expr = NULL;
      if (NULL != node->children_[3]) {
        if (OB_UNLIKELY(1 != node->children_[3]->num_child_) || OB_ISNULL(node->children_[3]->children_)
        || OB_ISNULL(node->children_[3]->children_[0])
        || !IS_DATATYPE_OR_QUESTIONMARK_OP(node->children_[3]->children_[0]->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid separator_expr child", K(ret), K(node->children_[3]));
        } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[3]->children_[0], separator_expr)))) {
          LOG_WARN("fail to recursive resolve separator expr", K(ret));
        } else if (OB_ISNULL(separator_expr) || OB_UNLIKELY(ObRawExpr::EXPR_CONST != separator_expr->get_expr_class())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid separator_expr", K(ret), K(separator_expr));
        }
      }
      agg_expr->set_separator_param_expr(separator_expr);
    }

    if (OB_SUCC(ret)) {
      //解析order by
      if (NULL != node->children_[2]) {
        const ParseNode *order_by_node = node->children_[2];
        if (OB_ISNULL(order_by_node)
            || OB_UNLIKELY(order_by_node->type_ != T_ORDER_BY)
            || OB_UNLIKELY(order_by_node->num_child_ != 2)
            || OB_ISNULL(order_by_node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("order by node is null or invalid", K(order_by_node));
        } else if (OB_UNLIKELY(NULL != order_by_node->children_[1])) {
          ret = OB_ERR_INVALID_SIBLINGS;
          LOG_WARN("ORDER SIBLINGS BY clause not allowed here");
        } else if (OB_FAIL(process_sort_list_node(order_by_node->children_[0], agg_expr))) {
          LOG_WARN("fail to process sort list node", K(ret), K(node));
        } else if ((T_FUN_GROUP_PERCENTILE_DISC == node->type_
                    || T_FUN_GROUP_PERCENTILE_CONT == node->type_)
                    && OB_FAIL(reset_aggr_sort_nulls_first(agg_expr->get_order_items_for_update()))) {
          LOG_WARN("failed to reset median aggr sort direction", K(ret), K(node));
        } else {/*do nothing*/}
      }
    }
    if (OB_SUCC(ret)) {
      if (need_add_flag && (ctx_.parents_expr_info_.del_member(IS_AGG))) {
        LOG_WARN("failed to del member", K(ret));
      } else {
        expr = agg_expr;
      }
    }
  }
  OZ(param_not_row_check(expr));
  return ret;
}

int ObRawExprResolverImpl::process_keep_aggr_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObAggFunRawExpr *agg_expr = NULL;
  bool keep_is_last = false;
  ObRawExpr *sub_expr = NULL;

  if (OB_ISNULL(ctx_.aggr_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr_exprs_ is null");
  } else if (OB_ISNULL(node) || OB_UNLIKELY(node->num_child_ != 4)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(ret), K(node));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, agg_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(agg_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("agg_expr is null");
  } else if (OB_FAIL(ctx_.aggr_exprs_->push_back(agg_expr))) {
    LOG_WARN("store aggr expr failed", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inalid group concat node", K(ret), K(node->children_));
  } else {
    bool need_add_flag = !ctx_.parents_expr_info_.has_member(IS_AGG);
    if (need_add_flag && OB_FAIL(ctx_.parents_expr_info_.add_member(IS_AGG))) {
      LOG_WARN("failed to add member", K(ret));
    } else if (NULL != node->children_[0] && T_DISTINCT == node->children_[0]->type_) {
      ret = OB_DISTINCT_NOT_ALLOWED;
      LOG_WARN("distinct not allowed in aggr", K(ret));
    } else if (T_FUN_KEEP_COUNT == node->type_ && node->children_[1] != NULL &&
                T_STAR == node->children_[1]->type_) {
      /*do nothing*/
    } else if (OB_FAIL(recursive_resolve(node->children_[1], sub_expr))) {
      LOG_WARN("fail to recursive resolve node child", K(ret), K(node->children_[1]));
    } else if (OB_FAIL(agg_expr->add_real_param_expr(sub_expr))) {
      LOG_WARN("fail to add param expr", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (NULL != node->children_[2] && node->children_[2]->type_ == T_LAST) {
        keep_is_last = true;
      }
      //解析order by
      if (NULL != node->children_[3]) {
        const ParseNode *order_by_node = node->children_[3];
        if (OB_ISNULL(order_by_node)
            || OB_UNLIKELY(order_by_node->type_ != T_ORDER_BY)
            || OB_UNLIKELY(order_by_node->num_child_ != 2)
            || OB_ISNULL(order_by_node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("order by node is null or invalid", K(order_by_node));
        } else if (OB_UNLIKELY(NULL != order_by_node->children_[1])) {
          ret = OB_ERR_INVALID_SIBLINGS;
          LOG_WARN("ORDER SIBLINGS BY clause not allowed here");
        } else if (OB_FAIL(process_sort_list_node(order_by_node->children_[0], agg_expr))) {
          LOG_WARN("fail to process sort list node", K(ret), K(node));
        }
      }
      //当keep分析函数是取排完序之后的最后一行相等值的数据进行处理时,为了在执行的时候能够更加高效,在resolve阶段
      //就可以转为对应的first形式，这样执行的时候只需要取第一行数据，同时还可以少使用一个变量，eg:
      // select max(c1) keep (dense_rank last order by c2) from t1;
      //<==>
      // select max(c1) keep (dense_rank first order by c2 desc NULLS FIRST) from t1;
      if (OB_SUCC(ret) && keep_is_last) {
        if (OB_FAIL(reset_keep_aggr_sort_direction(agg_expr->get_order_items_for_update()))) {
          LOG_WARN("failed to reset keep aggr sort direction", K(ret));
        } else {/*do nothing*/}
      }

      //对于order by item为常量的情形，可以退化普通的聚合函数，比如:
      // count(*) keep(dense_rank first order by 1) from t1  <==> count(*) from t1;
      if (OB_SUCC(ret) && agg_expr->get_order_items().count() == 0) {
        if (OB_FAIL(convert_keep_aggr_to_common_aggr(agg_expr))) {
          LOG_WARN("failed to convert keep aggr to common aggr", K(ret));
        } else {/*do nothing*/}
      }
    }
    if (OB_SUCC(ret)) {
      if (need_add_flag && (ctx_.parents_expr_info_.del_member(IS_AGG))) {
        LOG_WARN("failed to del member", K(ret));
      } else {
        expr = agg_expr;
      }
    }
  }
  OZ(param_not_row_check(expr));
  return ret;
}

int ObRawExprResolverImpl::convert_keep_aggr_to_common_aggr(ObAggFunRawExpr *&agg_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(agg_expr) || OB_UNLIKELY(!IS_KEEP_AGGR_FUN(agg_expr->get_expr_type()) ||
                                         agg_expr->get_order_items().count() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(agg_expr), K(ret));
  } else {
    ObItemType aggr_type = T_INVALID;
    switch (agg_expr->get_expr_type()) {
      case T_FUN_KEEP_MAX:
        aggr_type = T_FUN_MAX;
        break;
      case T_FUN_KEEP_MIN:
        aggr_type = T_FUN_MIN;
        break;
      case T_FUN_KEEP_SUM:
        aggr_type = T_FUN_SUM;
        break;
      case T_FUN_KEEP_COUNT:
        aggr_type = T_FUN_COUNT;
        break;
      case T_FUN_KEEP_AVG:
        aggr_type = T_FUN_AVG;
        break;
      case T_FUN_KEEP_STDDEV:
        aggr_type = T_FUN_STDDEV;
        break;
      case T_FUN_KEEP_VARIANCE:
        aggr_type = T_FUN_VARIANCE;
        break;
      case T_FUN_KEEP_WM_CONCAT:
        aggr_type = T_FUN_GROUP_CONCAT;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid type", K(aggr_type), K(ret));
        break;
    }
    if (OB_SUCC(ret)) {
      agg_expr->set_expr_type(aggr_type);//由于所有的aggr的结构都是一样的，所以直接改变类型就可以
    }
  }
  return ret;
}


//用于group/keep aggr
int ObRawExprResolverImpl::process_sort_list_node(const ParseNode *node, ObAggFunRawExpr *parent_agg_expr)
{
  int ret = OB_SUCCESS;
  // 只有agg function才会进这里
  // 往agg_fun_expr中添加order item信息
  if (OB_ISNULL(node) || OB_ISNULL(parent_agg_expr) || OB_ISNULL(node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node), K(parent_agg_expr));
  } else {
    const ObIArray<ObRawExpr*> &agg_real_param_exprs = parent_agg_expr->get_real_param_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      OrderItem order_item;
      const ParseNode *sort_node = node->children_[i];
      if (OB_ISNULL(sort_node) || OB_UNLIKELY(2 != sort_node->num_child_)
      || OB_ISNULL(sort_node->children_)
      || OB_ISNULL(sort_node->children_[0])
      || OB_ISNULL(sort_node->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid sort node", K(ret), K(sort_node));
      } else if (OB_FAIL(ObResolverUtils::set_direction_by_mode(*sort_node, order_item))) {
        LOG_WARN("failed to set direction by mode", K(ret));
      } else {
        // do nothing
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(sort_node->children_[0]->type_ == T_INT && sort_node->children_[0]->value_ >= 0)) {
          // The order-by item is specified using column position
          // ie. ORDER BY 1 DESC
          // oracle mode: order by 1 中的1被解释成常量表达式而不是列的位置, 此时需要忽略
          if (lib::is_oracle_mode()) {
            //rank、dense_rank、percent_rank、cume_dist需要这个常量值进行判断,所以需要添加进去
            switch (parent_agg_expr->get_expr_type()) {
              case T_FUN_GROUP_RANK:
              case T_FUN_GROUP_PERCENT_RANK:
              case T_FUN_GROUP_DENSE_RANK:
              case T_FUN_GROUP_CUME_DIST:
              case T_FUN_GROUP_PERCENTILE_CONT:
              case T_FUN_GROUP_PERCENTILE_DISC: {
                if (OB_FAIL(recursive_resolve(sort_node->children_[0], order_item.expr_))) {
                  LOG_WARN("fail to recursive resolve order item expr", K(ret));
                }
                break;
              }
              default:
                continue;
            }
          } else {
            int32_t pos = static_cast<int32_t>(sort_node->children_[0]->value_);
            if (pos <= 0 || pos > agg_real_param_exprs.count()) {
              // for SELECT statement, we need to make sure the column positions are valid
              ret = OB_ERR_BAD_FIELD_ERROR;
              _LOG_WARN("Unknown column '%d' in 'order by statement'", pos);
            } else {
              // create expression
              order_item.expr_ = agg_real_param_exprs.at(pos - 1);
            }
          }
        } else if (T_QUESTIONMARK == sort_node->children_[0]->type_) {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("'?' can't after 'order by", K(ret));
        } else if (OB_FAIL(SMART_CALL(recursive_resolve(sort_node->children_[0], order_item.expr_)))) {
          LOG_WARN("fail to recursive resolve order item expr", K(ret));
        }
        OZ(not_row_check(order_item.expr_));
        if (OB_SUCC(ret) && OB_FAIL(parent_agg_expr->add_order_item(order_item))) {
          LOG_WARN("fail to add order item to agg expr", K(ret));
        }
      }
    } // end for
  } //end else
  return ret;
}

int ObRawExprResolverImpl::reset_aggr_sort_nulls_first(ObIArray<OrderItem> &aggr_sort_item)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_sort_item.count(); ++i) {
    switch (aggr_sort_item.at(i).order_type_)
    {
    case NULLS_FIRST_ASC:
    case NULLS_LAST_ASC:
      aggr_sort_item.at(i).order_type_ = NULLS_FIRST_ASC;
      break;
    case NULLS_FIRST_DESC:
    case NULLS_LAST_DESC:
      aggr_sort_item.at(i).order_type_ = NULLS_FIRST_DESC;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected order type", K(ret), K(aggr_sort_item.at(i).order_type_));
      break;
    }
  }
  return ret;
}

int ObRawExprResolverImpl::reset_keep_aggr_sort_direction(ObIArray<OrderItem> &aggr_sort_item)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_sort_item.count(); ++i) {
    switch (aggr_sort_item.at(i).order_type_)
    {
    case NULLS_FIRST_ASC:
      aggr_sort_item.at(i).order_type_ = NULLS_LAST_DESC;
      break;
    case NULLS_LAST_ASC:
      aggr_sort_item.at(i).order_type_ = NULLS_FIRST_DESC;
      break;
    case NULLS_FIRST_DESC:
      aggr_sort_item.at(i).order_type_ = NULLS_LAST_ASC;
      break;
    case NULLS_LAST_DESC:
      aggr_sort_item.at(i).order_type_ = NULLS_FIRST_ASC;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected order type", K(ret), K(aggr_sort_item.at(i).order_type_));
      break;
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_xmlparse_node(const ParseNode *node, ObRawExpr *&expr)
{
  INIT_SUCC(ret);
  // check type and num_child
  CK(OB_NOT_NULL(node));
  if (OB_SUCC(ret) && T_FUN_SYS_XMLPARSE != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node->type_ error");
  } else if (OB_SUCC(ret) && node->num_child_ != 4) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num_child_ error");
  }
  // declare func_expr
  int32_t child_num = 0;
  ObSysFunRawExpr *func_expr = NULL;
  if (OB_SUCC(ret)) {
    child_num = node->num_child_;
    ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, func_expr);
    CK(OB_NOT_NULL(func_expr));
    OX(func_expr->set_func_name(ObString::make_string("xmlparse")));
  }

  // add param
  for (int32_t i = 0; OB_SUCC(ret) && i < child_num; i++) {
    ObRawExpr *para_expr = NULL;
    CK(OB_NOT_NULL(node->children_[i]));
    OZ(SMART_CALL(recursive_resolve(node->children_[i], para_expr)));
    CK(OB_NOT_NULL(para_expr));
    if (i == 3) {
      int flag = node->reserved_;
      if (OB_NOT_NULL(ctx_.stmt_) && ctx_.stmt_->is_select_stmt()){
        flag |= 0x08;
      }
      ObConstRawExpr *const_expr = static_cast<ObConstRawExpr *>(para_expr);
      ObObj val;
      val.set_int(flag);
      const_expr->set_value(val);
      const_expr->set_param(val);
    }

    //para_expr->clear_flag(IS_CALCULABLE_EXPR);
    OZ(func_expr->add_param_expr(para_expr));
  }
  OX(expr = func_expr);
  return ret;
}

int ObRawExprResolverImpl::process_oracle_timestamp_node(const ParseNode *node,
                                                         ObRawExpr *&expr,
                                                         int16_t min_precision,
                                                         int16_t max_precision,
                                                         int16_t default_precision)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *c_expr = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else {
    if (OB_UNLIKELY(NULL == node->children_ || 1 != node->num_child_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(node));
    } else {
      int16_t scale = 0;
      //get precision
      if (NULL != node->children_[0]) {
        scale = static_cast<int16_t>(node->children_[0]->value_);
      } else {
        scale = default_precision;
      }

      if (OB_UNLIKELY(scale > max_precision) || OB_UNLIKELY(scale < min_precision)) {
        ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;

      } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, c_expr))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else if (OB_ISNULL(c_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("c_expr is null");
      } else {
        c_expr->set_scale(scale);
        expr = c_expr;
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_timestamp_node(const ParseNode *node, ObString &err_info, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *c_expr = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else {
    int16_t scale = 0;
    if (OB_UNLIKELY(NULL == node->children_ || 1 != node->num_child_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(node));
    } else {
      if (NULL != node->children_[0]) {
        scale = static_cast<int16_t>(node->children_[0]->value_);
      } else {
        scale = 0;
      }

      if (OB_UNLIKELY(scale > OB_MAX_DATETIME_PRECISION)) {
        ret = OB_ERR_TOO_BIG_PRECISION;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, to_cstring(err_info), OB_MAX_DATETIME_PRECISION);
      } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node->type_, c_expr))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else if (OB_ISNULL(c_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("c_expr is null");
      } else {
        c_expr->set_scale(scale);
        expr = c_expr;
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_collation_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else {
    ObConstRawExpr *c_expr = NULL;
    ObString collation(node->str_len_, node->str_value_);
    ObCollationType collation_type = ObCharset::collation_type(collation);
    if (CS_TYPE_INVALID == collation_type) {
      ret = OB_ERR_UNKNOWN_COLLATION;
      LOG_USER_ERROR(OB_ERR_UNKNOWN_COLLATION, (int)node->str_len_, node->str_value_);
    } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(common::ObCharset::charset_type_by_coll(collation_type),
                                                                      ctx_.session_info_->get_effective_tenant_id()))) {
      LOG_WARN("failed to check charset data version valid", K(ret));
    } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_INT, c_expr))) {
      LOG_WARN("fail to create raw expr", K(ret));
    } else if (OB_ISNULL(c_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("c_expr is null");
    } else {
      ObObj val;
      val.set_int(static_cast<int64_t>(collation_type));
      val.set_collation_type(collation_type);
      val.set_collation_level(CS_LEVEL_IGNORABLE);
      c_expr->set_value(val);
      expr = c_expr;
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_if_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObCaseOpRawExpr *case_expr = NULL;
  if (OB_ISNULL(node) || OB_ISNULL(node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_OP_CASE, case_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(case_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("case_expr is null");
  } else {
    const ParseNode *expr_list = node->children_[1];
    if (OB_ISNULL(expr_list) || OB_UNLIKELY(T_EXPR_LIST != expr_list->type_)
    || OB_UNLIKELY(3 != expr_list->num_child_)
    || OB_ISNULL(expr_list->children_)
    || OB_ISNULL(expr_list->children_[0])
    || OB_ISNULL(expr_list->children_[1])
    || OB_ISNULL(expr_list->children_[2])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parser tree error", K(ret), K(node));
    } else {
      ObRawExpr *when_expr = NULL;
      ObRawExpr *then_expr = NULL;
      ObRawExpr *default_expr = NULL;
      if (OB_FAIL(SMART_CALL(recursive_resolve(expr_list->children_[0], when_expr)))) {
        LOG_WARN("get when expression failed", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(expr_list->children_[1], then_expr)))) {
        LOG_WARN("get then expression failed", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(expr_list->children_[2], default_expr)))) {
        LOG_WARN("get default expression failed", K(ret));
      } else if (OB_FAIL(case_expr->add_when_param_expr(when_expr))) {
        LOG_WARN("add when expression failed", K(ret));
      } else if (OB_FAIL(case_expr->add_then_param_expr(then_expr))) {
        LOG_WARN("add then expression failed", K(ret));
      } else {
        case_expr->set_default_param_expr(default_expr);
        expr = case_expr;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObRawExprUtils::try_add_bool_expr(case_expr, ctx_.expr_factory_))) {
        LOG_WARN("try_add_bool_expr for case expr failed", K(ret));
      }
    }
  }
  return ret;
}

/* resolve system funciton INTERVAL(param1, param2) or INTERVAL(param1, param2, param3...)
 * 1.create a fun expr for INTERVAL
 * 2.check and resolve parameters(node->children_)
 * 3.add all the param exprs to fun expr
 * input:  node
 * output: expr
*/
int ObRawExprResolverImpl::process_fun_interval_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *fun_expr = NULL;
  ObRawExpr *para_expr = NULL;

  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument 1", K(ret), K(node));
  } else if (OB_UNLIKELY(2 > node->num_child_) || OB_ISNULL(node->children_)) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid node children for interval function 2", K(ret), K(node));
  } else if (OB_FAIL(
      ctx_.expr_factory_.create_raw_expr(T_FUN_SYS_INTERVAL, fun_expr))) {
    LOG_WARN("create ObOpRawExpr failed 6", K(ret));
  } else if (OB_ISNULL(fun_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObOpRawExpr is null");
  } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[0], para_expr)))) {
    LOG_WARN("fail to recursive resolve", K(ret), K(node->children_[0]));
  } else if (OB_FAIL(fun_expr->add_param_expr(para_expr))) {
    LOG_WARN("fail to add param expr", K(ret), K(para_expr));
  } else {
    if (OB_UNLIKELY(T_EXPR_LIST != node->children_[1]->type_)) {
      if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[1], para_expr)))) {
        LOG_WARN("fail to recursive resolve", K(ret), K(node->children_[1]));
      } else if (OB_FAIL(fun_expr->add_param_expr(para_expr))) {
        LOG_WARN("fail to add param expr", K(ret), K(para_expr));
      }
    } else {
      const ParseNode &expr_list = *(node->children_[1]);

      for (int32_t i = 0; OB_SUCC(ret) && i < expr_list.num_child_; i++) {
        if (OB_FAIL(SMART_CALL(recursive_resolve(expr_list.children_[i], para_expr)))) {
          LOG_WARN("fail to recursive resolve", K(ret), K(expr_list.children_[i]));
        } else if (OB_FAIL(fun_expr->add_param_expr(para_expr))) {
          LOG_WARN("fail to add param expr", K(ret), K(para_expr));
        }
        LOG_DEBUG("param info:", K(expr_list.children_[i]), K(expr_list.children_[i]->type_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    expr = fun_expr;
  }

  return ret;
}


int ObRawExprResolverImpl::process_isnull_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (OB_UNLIKELY(2 != node->num_child_) || OB_ISNULL(node->children_)
      || OB_ISNULL(node->children_[1])
      || OB_UNLIKELY(1 > node->children_[1]->num_child_)
      || OB_ISNULL(node->children_[1]->children_)
      || OB_ISNULL(node->children_[1]->children_[0])) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid node children for isnull function", K(ret), K(node));
  } else {
    ObRawExpr *obj_expr = NULL;
    if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[1]->children_[0], obj_expr)))) {
      LOG_WARN("resolve child failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(ctx_.expr_factory_,
                                                              obj_expr,
                                                              false/*is_not_null*/,
                                                              expr))) {
      LOG_WARN("create is null expr failed", K(ret));
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_call_param_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObCallParamRawExpr *call_param_expr = NULL;
  ObRawExpr* child_expr = NULL;
  ObString name;
  CK (OB_NOT_NULL(node));
  CK (OB_LIKELY(T_SP_CPARAM == node->type_));
  CK (OB_LIKELY(2 == node->num_child_));
  CK (OB_NOT_NULL(node->children_[0]));
  CK (OB_NOT_NULL(node->children_[1]));
  if (OB_SUCC(ret)) {
    const ParseNode *name_node = NULL;
    if (T_OBJ_ACCESS_REF == node->children_[0]->type_) {
      CK (OB_LIKELY(2 == node->children_[0]->num_child_));
      CK (OB_NOT_NULL(node->children_[0]->children_[0]));
      CK (OB_ISNULL(node->children_[0]->children_[1]));
      CK (OB_LIKELY(T_IDENT == node->children_[0]->children_[0]->type_));
      name_node = node->children_[0]->children_[0];
    } else if (T_IDENT == node->children_[0]->type_) {
      name_node = node->children_[0];
    } else if (T_COLUMN_REF == node->children_[0]->type_ &&
               3 == node->children_[0]->num_child_ &&
               NULL == node->children_[0]->children_[0] &&
               NULL ==  node->children_[0]->children_[1] &&
               T_IDENT == node->children_[0]->children_[2]->type_) {
      name_node = node->children_[0]->children_[2];
    } else {
      ret = OB_ERR_CALL_WRONG_ARG;
      LOG_WARN("PLS-00306: wrong number or types of arguments in call", K(ret));
    }
    OZ (ctx_.expr_factory_.create_raw_expr(T_SP_CPARAM, call_param_expr));
    CK (OB_NOT_NULL(call_param_expr));
    OX (name = ObString(static_cast<int32_t>(name_node->str_len_), name_node->str_value_));
    OX (call_param_expr->set_name(name));
    OZ (recursive_resolve(node->children_[1], child_expr));
    CK (OB_NOT_NULL(child_expr));
    OX (call_param_expr->set_expr(child_expr));
    OX (expr = call_param_expr);
  }
  return ret;
}

int ObRawExprResolverImpl::process_sqlerrm_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObPLSQLCodeSQLErrmRawExpr *func_expr = NULL;
  ObRawExpr *param_expr = NULL;
  ObString name, func_name;
  CK (OB_NOT_NULL(node));
  OZ (ctx_.expr_factory_.create_raw_expr(T_FUN_PL_SQLCODE_SQLERRM, func_expr));
  CK (OB_NOT_NULL(func_expr));
  OX (name = ObString(node->children_[0]->str_len_, node->children_[0]->str_value_));
  CK (OB_LIKELY(0 == name.case_compare("SQLERRM")));
  OZ (ob_write_string(ctx_.expr_factory_.get_allocator(), N_PL_GET_SQLCODE_SQLERRM, func_name));
  OX (func_expr->set_func_name(func_name));
  OX (func_expr->set_is_sqlcode(false));
  if (OB_SUCC(ret) && node->num_child_ > 2) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid argument number for SQLERRM", K(node->num_child_));
  }
  if (OB_SUCC(ret) && 2 == node->num_child_) {
    if (OB_ISNULL(node->children_[1])
        || node->children_[1]->num_child_ != 1) {
      ret = OB_INVALID_ARGUMENT_NUM;
      LOG_WARN("invalid argument number for SQLERRM", K(node->children_[1]->num_child_));
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(node->children_[1]->children_[0])) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid parse tree", K(ret));
      } else if (T_EXPR_WITH_ALIAS == node->children_[1]->children_[0]->type_) {
        ret = OB_ERR_FUNCTION_UNKNOWN;
        LOG_WARN("SQLERRM in argument alias is not supported", K(ret));
      }
    }
    OZ (SMART_CALL(recursive_resolve(node->children_[1]->children_[0], param_expr)));
    CK (OB_NOT_NULL(param_expr));
    OZ (func_expr->add_param_expr(param_expr));
  }
  OX (expr = func_expr);
  return ret;
}

int ObRawExprResolverImpl::process_plsql_var_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObPLSQLVariableRawExpr *plsql_expr = NULL;
  ObString plsql_variable;
  CK (OB_NOT_NULL(node));
  CK (OB_LIKELY(1 == node->num_child_));
  CK (OB_NOT_NULL(node->children_));
  CK (OB_NOT_NULL(node->children_[0]));
  OZ (ctx_.expr_factory_.create_raw_expr(T_FUN_PLSQL_VARIABLE, plsql_expr));
  CK (OB_NOT_NULL(plsql_expr));
  OX (plsql_expr->set_plsql_line(node->children_[0]->int32_values_[0]));
  OZ (ob_write_string(ctx_.expr_factory_.get_allocator(),
                      ObString(node->children_[0]->str_len_, node->children_[0]->str_value_),
                      plsql_variable));
  OX (plsql_expr->set_plsql_variable(plsql_variable));
  OX (expr = plsql_expr);
  return ret;
}

int ObRawExprResolverImpl::process_lnnvl_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  CK(T_FUN_SYS_LNNVL == node->type_);
  CK(1 == node->num_child_);
  CK(OB_NOT_NULL(node->children_));
  CK(OB_NOT_NULL(node->children_[0]));
  ObItemType param_type = node->children_[0]->type_;
  switch (param_type) {
    case T_FUN_SYS_LNNVL:
    case T_FUN_SYS_REGEXP_LIKE:
    case T_OP_EXISTS:
    case T_OP_IS:
    case T_OP_IS_NOT:
    case T_OP_LIKE:
    case T_OP_NOT_LIKE:
    case T_OP_IN:
    case T_OP_NOT_IN:
    case T_OP_LE:
    case T_OP_LT:
    case T_OP_EQ:
    case T_OP_GE:
    case T_OP_GT:
    case T_OP_NE: {
      break;
    }
    default: {
      ret = OB_INCORRECT_USE_OF_OPERATOR;
      ObString tmp_msg("LNNVL");
      LOG_USER_ERROR(OB_INCORRECT_USE_OF_OPERATOR, tmp_msg.length(), tmp_msg.ptr());
      break;
    }
  }
  if (OB_SUCC(ret)) {
    ObSysFunRawExpr *func_expr = NULL;
    ObRawExpr *param_expr = NULL;
    OZ(SMART_CALL(recursive_resolve(node->children_[0], param_expr)));
    CK(OB_NOT_NULL(param_expr));
    OZ(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS_LNNVL, func_expr));
    CK(OB_NOT_NULL(func_expr));
    OZ(func_expr->add_param_expr(param_expr));
    OX(func_expr->set_func_name(ObString::make_string(N_LNNVL)));
    OX(expr = func_expr);
  }
  return ret;
}

int ObRawExprResolverImpl::malloc_new_specified_type_node(common::ObIAllocator &allocator, ObString col_name, ParseNode *col_node, ObItemType type)
{
  INIT_SUCC(ret);
  memset(col_node, 0, sizeof(ParseNode));
  if (type == T_CHAR) {
    col_node->type_ = T_CHAR;
    col_node->str_value_ = col_name.ptr();
    col_node->raw_text_ = col_name.ptr();
    col_node->str_len_ = col_name.length();
    col_node->text_len_ = col_name.length();
    col_node->num_child_ = 2;
    ParseNode *key_child_node;
    if (nullptr == (key_child_node = static_cast<ParseNode*>(allocator.alloc(sizeof(ParseNode))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
    } else {
      key_child_node = new(key_child_node) ParseNode;
      memset(key_child_node, 0, sizeof(ParseNode));
      key_child_node->type_ = T_VARCHAR;
      key_child_node->str_value_ = col_name.ptr();
      key_child_node->raw_text_ = col_name.ptr();
      key_child_node->str_len_ = col_name.length();
      key_child_node->text_len_ = col_name.length();
      int64_t alloc_char_size = sizeof(ParseNode *) * 2;
      ParseNode **k_t_vec = NULL;
      if (OB_ISNULL(k_t_vec = static_cast<ParseNode **>(allocator.alloc(alloc_char_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
      } else {
        k_t_vec[0] = NULL;
        k_t_vec[1] = key_child_node;
        col_node->children_ = k_t_vec;
      }
    }
  } else if (type == T_COLUMN_REF) {
    col_node->type_ = T_COLUMN_REF;
    col_node->str_value_ = col_name.ptr();
    col_node->raw_text_ = col_name.ptr();
    col_node->str_len_ = col_name.length();
    col_node->num_child_ = 3;
    ParseNode *val_node;
    if (nullptr == (val_node = static_cast<ParseNode*>(allocator.alloc(sizeof(ParseNode))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
    } else {
      val_node = new(val_node) ParseNode;
      memset(val_node, 0, sizeof(ParseNode));
      val_node->type_ = T_VARCHAR;
      val_node->str_value_ = col_name.ptr();
      val_node->raw_text_ = col_name.ptr();
      val_node->str_len_ = col_name.length();
      val_node->text_len_ = col_name.length();
      int64_t alloc_col_size = sizeof(ParseNode *) * 3;
      ParseNode **t_vec = NULL;
      if (OB_ISNULL(t_vec = static_cast<ParseNode **>(allocator.alloc(alloc_col_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
      } else {
        t_vec[0] = NULL;
        t_vec[1] = NULL;
        t_vec[2] = val_node;
        col_node->children_ = t_vec;
      }
    }
  } else if (type == T_INT) {
    col_node->type_ = T_INT;
    col_node->flag_ = 2;
    col_node->is_hidden_const_ = 1;
    col_node->int16_values_[0] = 0;
    col_node->int16_values_[1] = 0;
    col_node->int16_values_[2] = 0;
    col_node->int16_values_[3] = 0;
    col_node->int32_values_[0] = 0;
    col_node->int32_values_[1] = 0;
  } else if (type == T_NULL) {
    col_node->type_ = T_NULL;
    col_node->num_child_ = 0;
  } else if (type == T_OBJ_ACCESS_REF) {
    col_node->type_ = T_OBJ_ACCESS_REF;
    col_node->num_child_ = 2;
    col_node->is_hidden_const_ = 1;
    int64_t alloc_access_size = sizeof(ParseNode *) * 2;
    ParseNode **t_vec = NULL;
    if (OB_ISNULL(t_vec = static_cast<ParseNode **>(allocator.alloc(alloc_access_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
    } else {
      t_vec[0] = NULL;
      t_vec[1] = NULL;
      col_node->children_ = t_vec;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type not exits");
  }
  return ret;
}

// use： json value (mismatch ype)
int ObRawExprResolverImpl::expand_node(common::ObIAllocator &allocator, ParseNode *node, int p, ObVector<const ParseNode*> &arr) {
  int ret = OB_SUCCESS;
  if (node->type_ == T_LINK_NODE || node->type_ == T_VALUE_VECTOR)
  {
    for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++){
      if (OB_ISNULL(node->children_[i])) {
      } else {
        if (OB_SUCC(ret) && OB_FAIL(SMART_CALL(expand_node(allocator, node->children_[i], i, arr)))) {
          LOG_WARN("node expand fail", K(ret));
        }
      }
    }
  } else {
    const ParseNode* new_node = node;
    if (OB_FAIL(arr.push_back(node))) {
      LOG_WARN("node resolver fail", K(ret));
    }
  }
  return ret;
}

int ObRawExprResolverImpl::remove_format_json_opt_in_pl(ParseNode *node, int8_t expr_flag)
{
  INIT_SUCC(ret);
  int64_t num = node->num_child_;
  if (expr_flag == OPT_JSON_OBJECT) {
    for (int64_t i = 2; i < num; i += 3) {
      if (OB_ISNULL(node->children_[i]) || OB_ISNULL(node->children_[i - 1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should not null", K(ret), K(i));
      } else if (node->children_[i]->type_ != T_INT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is't int", K(ret), K(node->children_[i]->type_));
      } else if (node->children_[i - 1]->type_ != T_CHAR) {
        node->children_[i]->value_ = 0;
      }
    }
  } else if (expr_flag == OPT_JSON_ARRAY) {
    for (int64_t i = 1; i < num; i += 2) {
      if (OB_ISNULL(node->children_[i]) || OB_ISNULL(node->children_[i - 1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should not null", K(ret), K(i));
      } else if (node->children_[i]->type_ != T_INT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is't int", K(ret), K(node->children_[i]->type_));
      } else if (node->children_[i - 1]->type_ != T_CHAR) {
        node->children_[i]->value_ = 0;
      }
    }
  }
  return ret;
}
int ObRawExprResolverImpl::remove_strict_opt_in_pl(ParseNode *node, int8_t expr_flag)
{
  INIT_SUCC(ret);
  if (expr_flag == OPT_JSON_OBJECT || expr_flag == OPT_JSON_ARRAY) {
    if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not null");
    } else if (node->type_ != T_INT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type is't int");
    } else {
      node->value_ = 0;
    }
  }
  return ret;
}

int ObRawExprResolverImpl::get_column_raw_text_from_node(const ParseNode *node, ObString &col_name)
{
  INIT_SUCC(ret);
  ObQualifiedName column_ref;
  if (node->type_ == T_COLUMN_REF) {
    col_name = node->children_[2]->raw_text_;
  } else if (node->type_ == T_OBJ_ACCESS_REF) {
    while (OB_SUCC(ret) && node->children_[1] != NULL) {
      node = node->children_[1];
      if (node->type_ != T_OBJ_ACCESS_REF) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column name from node", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(node->children_[0]) && node->children_[0]->type_ == T_IDENT) {
        col_name = node->children_[0]->raw_text_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get column name from node", K(ret));
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_ora_json_object_node(const ParseNode *node, ObRawExpr *&expr)
{
  INIT_SUCC(ret);
  ObVector<const ParseNode*> key_value_arr;
  ParseNode* data_node = NULL;
  ParseNode* cur_node_kv = NULL;
  int cur_col_size = ctx_.columns_->count();
  ParseNode key_node;
  CK(OB_NOT_NULL(node));
  int32_t num = 0;
  if (OB_SUCC(ret)) {
    if (T_FUN_SYS_JSON_OBJECT != node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node->type_ error");
    } else if (node->num_child_ != 5) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("num_child_ error");
    }
    num = node->num_child_;
  }

  ObSysFunRawExpr *func_expr = NULL;
  if (OB_SUCC(ret) && OB_SUCC(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, func_expr))) {
    CK(OB_NOT_NULL(func_expr));
    OX(func_expr->set_func_name(ObString::make_string("json_object")));
  } else {
    LOG_WARN("fail to create func_expr");
  }

  // check pl clause : with unique keys/ returning type
  if (OB_SUCC(ret) && lib::is_oracle_mode() && ctx_.current_scope_ == T_PL_SCOPE) {
    if (OB_NOT_NULL(node->children_[0]->children_[0])
        && node->children_[0]->children_[0]->type_ == T_COLUMN_REF
        && OB_NOT_NULL(node->children_[0]->children_[0]->children_[2])
        && node->children_[0]->children_[0]->children_[2]->type_ == T_STAR) {
      ret = OB_ERR_PARSE_PLSQL;
      LOG_USER_ERROR(OB_ERR_PARSE_PLSQL, "\"*\"", "( ) - + case");
    } else if (node->children_[4]->value_ == 1) {
      ret = OB_ERR_INVALID_ARGUMENT_FOR_JSON_CALL;
      LOG_USER_ERROR(OB_ERR_INVALID_ARGUMENT_FOR_JSON_CALL, "JSON");
      LOG_WARN("invalid argument for json call");
    } else if (node->children_[num - 3]->type_ == T_LONGTEXT) {
      ret = OB_ERR_EXPRESSION_WRONG_TYPE;
      LOG_WARN("PLS-00382: expression is of wrong type", K(ret));
    } else if (OB_FAIL(remove_strict_opt_in_pl(const_cast<ParseNode *>(node->children_[3]), OPT_JSON_OBJECT))) {
      LOG_WARN("set format json fail", K(ret));
    } else if (OB_FAIL(remove_format_json_opt_in_pl(const_cast<ParseNode *>(node->children_[0]), OPT_JSON_OBJECT))) {
      LOG_WARN("set format json fail", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const ParseNode *key_value = node->children_[0];
    if (OB_ISNULL(key_value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entry node is null");
    } else {
      int d_len = strlen("default");
      if (node->children_[3]->value_ == 1 && node->children_[1]->value_ == 0
         && OB_NOT_NULL(node->children_[2]->raw_text_) && 0 == strncmp(node->children_[2]->raw_text_,"default", d_len)) {
        int last_arg_idx = key_value->num_child_ - 3;
        if (last_arg_idx >= 0 && key_value->children_[last_arg_idx]->type_ == T_OBJ_ACCESS_REF && key_value->children_[last_arg_idx + 1]->type_ == T_NULL
            && (OB_NOT_NULL(key_value->children_[last_arg_idx + 2]->raw_text_) && 0 == strncmp(key_value->children_[last_arg_idx + 2]->raw_text_, "default", d_len))
            && key_value->children_[last_arg_idx + 1]->value_ == 2) {
          ret = OB_ERR_MISS_VALUE;
          LOG_WARN("illegal use of 'strcit' clause.");
        }
      }

      if (OB_SUCC(ret)) {
        // [key-value(vector)]
        if (OB_NOT_NULL(node->children_[0])) {
          data_node = node->children_[0];
          for (int64_t i = 0; OB_SUCC(ret) && i < data_node->num_child_; i++) {  // 3 node in group
            ObRawExpr *para_expr = NULL;
            cur_node_kv = NULL;
            int val_pos = i + 1; //  key is i where i % 3 == 0 , value is i + 1, format json is i + 2
            if (OB_ISNULL(data_node->children_[i])) {
            } else if ((i % 3 == 1) && data_node->children_[i]->type_ == T_NULL && data_node->children_[i]->value_ == 2) {  // 2 is flag of empty value
              cur_node_kv = data_node->children_[i - 1];
            } else {
              cur_node_kv = data_node->children_[i];
            }
            if (OB_FAIL(ret) || OB_ISNULL(cur_node_kv)) {
            } else if (OB_FAIL(SMART_CALL(recursive_resolve(cur_node_kv, para_expr)))) {
              LOG_WARN("fail to get raw expr from node", K(ret), K(i));
            } else if (OB_ISNULL(para_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("raw expr is null", K(ret), K(i));
            } else if ((i % JSON_OBJECT_GROUP == ObJsonObjectEntry::JSON_OBJECT_KEY) && data_node->children_[val_pos]->type_ == T_NULL
                        && data_node->children_[val_pos]->value_ == 2  // 2 is flag of empty value
                        && !(((para_expr->get_expr_type() == T_COLUMN_REF  // column type
                              || para_expr->get_expr_type() == T_REF_COLUMN)
                            && ctx_.columns_->at(ctx_.columns_->count()-1).access_idents_.at(0).get_type() == oceanbase::sql::UNKNOWN)
                        || para_expr->get_expr_type() == T_FUN_SYS_ROWNUM)) {  // rownum
              ret = OB_ERR_MISS_VALUE;
              LOG_WARN("missing value keyword", K(ret), K(para_expr->get_expr_type()), K(i));
            } else if ((i % JSON_OBJECT_GROUP == ObJsonObjectEntry::JSON_OBJECT_KEY)  && data_node->children_[val_pos]->type_ == T_NULL
                                     && data_node->children_[val_pos]->value_ == 2   // 2 is flag of empty value
                                     && (((para_expr->get_expr_type() == T_COLUMN_REF   // column type
                                          || para_expr->get_expr_type() == T_REF_COLUMN)
                                        && ctx_.columns_->at(ctx_.columns_->count() - 1).access_idents_.at(0).get_type() == oceanbase::sql::UNKNOWN)
                                      || para_expr->get_expr_type() == T_FUN_SYS_ROWNUM)) {  // rownum
              ObString col_name;
              if (para_expr->get_expr_type() == T_FUN_SYS_ROWNUM) {
                col_name.assign_ptr("rownum", 6);
              } else if (ctx_.columns_->count() > cur_col_size && OB_SUCC(get_column_raw_text_from_node(cur_node_kv, col_name))) {
                for (int col_pos = cur_col_size; col_pos < ctx_.columns_->count(); col_pos ++) {
                  ctx_.columns_->pop_back();  // delete column para
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("fail to resolve column", K(ret), K(cur_col_size), K(ctx_.columns_->count()));
              }
              para_expr = NULL;
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(ctx_.local_allocator_, col_name, &key_node, T_CHAR))) {
                LOG_WARN("create json doc node fail", K(ret));
              } else if (OB_FAIL(SMART_CALL(recursive_resolve(&key_node, para_expr)))) {
                LOG_WARN("fail to get raw expr from node", K(ret), K(i));
              } else if (OB_ISNULL(para_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("raw expr is null", K(ret), K(i));
              } else if (OB_FAIL(func_expr->add_param_expr(para_expr))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("fail to add into func expr", K(ret), K(i));
              } else {
                cur_col_size = ctx_.columns_->count();
              }
            } else if (OB_FAIL(func_expr->add_param_expr(para_expr))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to add into func expr", K(ret), K(i));
            } else {
              cur_col_size = ctx_.columns_->count();
            }
          }
        }
        // [null_type][returning_type][strict][unique_keys]
        for (int32_t i = 1; OB_SUCC(ret) && i < num; i++) {
          ObRawExpr *para_expr = NULL;
          CK(OB_NOT_NULL(node->children_[i]));
          OZ(SMART_CALL(recursive_resolve(node->children_[i], para_expr)));
          CK(OB_NOT_NULL(para_expr));
          OZ(func_expr->add_param_expr(para_expr));
        } //end mismatch for
        OX(expr = func_expr);
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::check_first_node(const ParseNode *node)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(node)) {
    // do nothing node->num_child_ > 0 && node->type_ != T_COLUMN_REF)
  } else if (node->type_ == T_LINK_NODE || node->type_ == T_VALUE_VECTOR || node->type_ == T_QUESTIONMARK || node->type_ == T_COLUMN_REF)
  {
    for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++){
      if (OB_SUCC(ret)) {
        if (OB_FAIL(SMART_CALL(check_first_node(node->children_[i])))) {
          LOG_WARN("first node check is star failed", K(ret));
        }
      }
    }
  } else if (node->type_ == T_STAR) {
    ret = OB_ERR_INVALID_COLUMN_SPE;
    LOG_WARN("invalid user.table.column, table.column, or column specification", K(ret));
  }
  return ret;
}

int ObRawExprResolverImpl::process_json_query_node(const ParseNode *node, ObRawExpr *&expr){
  INIT_SUCC(ret);
  CK(OB_NOT_NULL(node));
  if(OB_SUCC(ret) && T_FUN_SYS_JSON_QUERY != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node->type_ error");
  } else if (OB_SUCC(ret) && 11 != node->num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num_child_ error");
  }
  int32_t num = 0;
  ObSysFunRawExpr *func_expr = NULL;
  if (OB_SUCC(ret)) {
    num = node->num_child_;
    ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, func_expr);
    CK(OB_NOT_NULL(func_expr));
    OX(func_expr->set_func_name(ObString::make_string("json_query")));
  }
  const ParseNode *path_node = node->children_[1];
  // check path
  if (OB_SUCC(ret) && OB_FAIL(pre_check_json_path_valid(path_node))) {
    LOG_WARN("pre check path fail", K(ret));
  }

  // check [returning_type]
  const ParseNode *returning_type = node->children_[2];
  if (OB_SUCC(ret)) {
    const char *input = node->children_[0]->str_value_;
    ret = cast_accuracy_check(returning_type, input);
  }
  // pre check default returning type with item method
  if (OB_SUCC(ret)) {
    if (returning_type->type_ == T_NULL) {
      ObString path_str(node->children_[1]->text_len_, node->children_[1]->raw_text_);
      if (OB_FAIL(ObJsonPath::change_json_expr_res_type_if_need(ctx_.local_allocator_, path_str, const_cast<ParseNode&>(*returning_type), OPT_JSON_QUERY))) {
        LOG_WARN("set return type by path item method fail", K(ret), K(path_str));
      }
    }
  }
  if (OB_SUCC(ret) && returning_type->int16_values_[OB_NODE_CAST_TYPE_IDX] != T_VARCHAR
      && returning_type->int16_values_[OB_NODE_CAST_TYPE_IDX] != T_LONGTEXT
      && returning_type->int16_values_[OB_NODE_CAST_TYPE_IDX] != T_JSON
      && returning_type->type_ != T_NULL) {
    ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
    LOG_USER_ERROR(OB_ERR_INVALID_DATA_TYPE_RETURNING);
  }

  if (OB_SUCC(ret) && OB_FAIL(check_first_node(node->children_[0]))) {
    LOG_WARN("invalid user.table.column, table.column, or column specification", K(ret));
  }

  // [json_text][json_path][returning_type][truncate][scalars][pretty][ascii][wrapper][error_type][empty_type][mismatch]
  for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
    ObRawExpr *para_expr = NULL;
    CK(OB_NOT_NULL(node->children_[i]));
    OZ(SMART_CALL(recursive_resolve(node->children_[i], para_expr)));
    CK(OB_NOT_NULL(para_expr));
    OZ(func_expr->add_param_expr(para_expr));
  }
  OX(expr = func_expr);
  return ret;
}

int ObRawExprResolverImpl::process_json_exists_node(const ParseNode *node, ObRawExpr *&expr)
{
  INIT_SUCC(ret);
  CK(T_FUN_SYS_JSON_EXISTS == node->type_);
  CK(5 == node->num_child_);
  int32_t num = node->num_child_;
  ObSysFunRawExpr *func_expr = NULL;
  OZ(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, func_expr));
  CK(OB_NOT_NULL(func_expr));
  OX(func_expr->set_func_name(ObString::make_string("json_exists")));
  // [json_text][json_path][passing_clause][error_type][empty_type]
  for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
    // expand [passing_clause]
    if (i == 2 && node->children_[i]->type_ == T_EXPR_LIST) {
      int64_t pass_num = node->children_[i]->num_child_;
      if (pass_num % 2 != 0) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("num_child_ error");
      } else {
        for (int32_t name_idx = 0; name_idx < pass_num && OB_SUCC(ret); ++name_idx) {
          if (name_idx % 2 == 1) {
            if (node->children_[i]->children_[name_idx]->type_ == T_IDENT) {
              node->children_[i]->children_[name_idx]->type_ = T_CHAR;
            } else {
              ret = OB_ERR_PARSER_SYNTAX;
              LOG_WARN("child_type_of_PASSING error");
            }
          }
          ObRawExpr *para_expr = NULL;
          CK(OB_NOT_NULL(node->children_[i]->children_[name_idx]));
          OZ(SMART_CALL(recursive_resolve(node->children_[i]->children_[name_idx], para_expr)));
          if (OB_NOT_NULL(para_expr) && name_idx % 2 == 0 && para_expr->get_expr_type() == T_REF_QUERY) {
            ret = OB_ERR_INVALID_VARIABLE_IN_JSON_PATH;
            LOG_USER_ERROR(OB_ERR_INVALID_VARIABLE_IN_JSON_PATH);
          } else {
            OZ(func_expr->add_param_expr(para_expr));
          }
        }
      }
    } else {
      ObRawExpr *para_expr = NULL;
      CK(OB_NOT_NULL(node->children_[i]));
      OZ(SMART_CALL(recursive_resolve(node->children_[i], para_expr)));
      CK(OB_NOT_NULL(para_expr));
      OZ(func_expr->add_param_expr(para_expr));
    }
  }
  OX(expr = func_expr);
  return ret;
}

int ObRawExprResolverImpl::pre_check_json_path_valid(const ParseNode *node)
{
  INIT_SUCC(ret);

  ObString j_path_text(node->str_len_, node->str_value_);
  ObJsonPath j_path(j_path_text, &ctx_.local_allocator_);
  if (j_path_text.length() == 0) {
  } else if (OB_FAIL(j_path.parse_path())) {
    ret = OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR;
    LOG_USER_ERROR(OB_ERR_JSON_PATH_EXPRESSION_SYNTAX_ERROR, j_path_text.length(), j_path_text.ptr());
  }

  return ret;
}

// json expr change T_NULLX_CLAUSE to T_INT
// and weight_string expr change T_WEIGHT_STRING_LEVEL_PARAM to T_INT
void ObRawExprResolverImpl::modification_type_to_int(ParseNode &node)
{
  node.type_ = T_INT;
}

int ObRawExprResolverImpl::process_json_value_node(const ParseNode *node, ObRawExpr *&expr)
{
  INIT_SUCC(ret);
  CK(OB_NOT_NULL(node))
  CK(T_FUN_SYS_JSON_VALUE == node->type_);
  // node child_num is nine,
  CK(10 == node->num_child_);
  bool mismatch_vec = false;
  int32_t num = 0;
  ObSysFunRawExpr *func_expr = NULL;
  if (OB_SUCC(ret)) {
    num = node->num_child_;
    OZ(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, func_expr));
    CK(OB_NOT_NULL(func_expr));
    OX(func_expr->set_func_name(ObString::make_string("json_value")));
  }
  // pl mode check whether has mismatch
  if (lib::is_oracle_mode() && ctx_.current_scope_ == T_PL_SCOPE) {
    if (node->children_[9]->num_child_ != 2
        || node->children_[9]->children_[0]->value_ != 3
        || node->children_[9]->children_[1]->value_ != 7) {
      ret = OB_ERR_PARSE_PLSQL;
      LOG_USER_ERROR(OB_ERR_PARSE_PLSQL, "\"MISMATCH\"", "error empty");
    }
  }
  const ParseNode *path_node = node->children_[1];
  // check path
  if (OB_SUCC(ret) && lib::is_oracle_mode() && OB_FAIL(pre_check_json_path_valid(path_node))) {
    LOG_WARN("pre check path fail", K(ret));
  }

  const ParseNode *returning_type = node->children_[2];
  // check [returning_type]
  if (OB_SUCC(ret)) {
    const char *input = node->children_[0]->str_value_;
    ret = cast_accuracy_check(returning_type, input);
  }
  // pre check default returning type with item method
  if (OB_SUCC(ret)) {
    ObString default_val(7, "default");
    if (OB_NOT_NULL(returning_type->raw_text_) && (0 == default_val.case_compare(returning_type->raw_text_)) && node->children_[1]->text_len_ > 0) {
      ObString path_str(node->children_[1]->text_len_, node->children_[1]->raw_text_);
      if (OB_FAIL(ObJsonPath::change_json_expr_res_type_if_need(ctx_.local_allocator_, path_str, const_cast<ParseNode&>(*returning_type), OPT_JSON_VALUE))) {
        LOG_WARN("set return type by path item method fail", K(ret), K(path_str));
      }
    }
  }
  // empty type i, error type i + 3 , distinct empty and error clause
  ParseNode *empty_type = NULL;
  ParseNode *empty_default_value = NULL;
  ParseNode *error_type = NULL;
  ParseNode *error_default_value = NULL;
  if (node->children_[5]->is_input_quoted_ == 1) {
    empty_type = node->children_[7];
    empty_default_value = node->children_[8];
    error_type = node->children_[5];
    error_default_value = node->children_[6];
  } else {
    empty_type = node->children_[5];
    empty_default_value = node->children_[6];
    error_type = node->children_[7];
    error_default_value = node->children_[8];
  }
  // if use defualt, value should not be null , support
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(empty_type) && empty_type->value_ == 2) {
      if (OB_NOT_NULL(empty_default_value) && empty_default_value->type_ == T_NULL) {
        if (lib::is_oracle_mode()) {
          ret = OB_ERR_DEFAULT_VALUE_NOT_LITERAL;
        } else {
          ret = OB_ERR_PARSER_SYNTAX;
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(error_type) && error_type->value_ == 2) {
      if (OB_NOT_NULL(error_default_value) && error_default_value->type_ == T_NULL) {
        if (lib::is_oracle_mode()) {
          ret = OB_ERR_DEFAULT_VALUE_NOT_LITERAL;
        } else {
          ret = OB_ERR_PARSER_SYNTAX;
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(check_first_node(node->children_[0]))) {
    LOG_WARN("invalid db.table.column, table.column, or column specification", K(ret));
  }
  // judge input TODO object type can use ignore and match type
  ObVector<const ParseNode*> mismatch_arr;
  if (OB_SUCC(ret)) {
    const ParseNode *on_mismatch = node->children_[9];
    if (OB_ISNULL(on_mismatch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mismatch node is null", K(ret));
    } else {
      if (OB_FAIL(expand_node(ctx_.local_allocator_, const_cast<ParseNode *>(on_mismatch), 0, mismatch_arr))) {
        LOG_WARN("parse mismatch has error", K(ret), K(mismatch_arr.size()));
      } else {
        // [json_text][json_path][returning_type][empty_type][empty_default_value][error_type][error_default_value]
        ParseNode *cur_node = NULL;
        for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
          cur_node = node->children_[i];
          if (i == 5) {
            cur_node = empty_type;
          } else if (i == 6) {
            cur_node = empty_default_value;
          } else if (i == 7) {
            cur_node = error_type;
          } else if (i == 8) {
            cur_node = error_default_value;
          }
          if (node->children_[i]->type_ == T_LINK_NODE || node->children_[i]->type_ == T_VALUE_VECTOR) {
          } else {
            ObRawExpr *para_expr = NULL;
            CK(OB_NOT_NULL(cur_node));
            OZ(SMART_CALL(recursive_resolve(cur_node, para_expr)));
            CK(OB_NOT_NULL(para_expr));
            OZ(func_expr->add_param_expr(para_expr));
          }
          if (OB_SUCC(ret) && (i == 6 || i == 8)) {
            ObRawExpr *para_expr = NULL;
            CK(OB_NOT_NULL(cur_node));
            OZ(SMART_CALL(recursive_resolve(cur_node, para_expr)));
            CK(OB_NOT_NULL(para_expr));
            OZ(func_expr->add_param_expr(para_expr));
          }
        } //end for
        // ([on_mismatch][opt_mismatch_types] on oracle)
        int8_t mis_check = 0;
        for (int32_t i = 0; OB_SUCC(ret) && i < mismatch_arr.size(); i++) {
          ObRawExpr *para_expr = NULL;
          if (mismatch_arr[i]->value_ >= 0 && mismatch_arr[i]->value_ <= 2) {
            mis_check = 0;
          } else if (mismatch_arr[i]->value_ >= 3 && mismatch_arr[i]->value_ <= 5) {
            switch(mismatch_arr[i]->value_) {
              case OB_JSON_TYPE_MISSING_DATA :{
                if ((mis_check & 1) == 1) {
                  ret = OB_ERR_INVALID_CLAUSE;
                  LOG_USER_ERROR(OB_ERR_INVALID_CLAUSE, "ON ERROR");
                }
                break;
              }
              case OB_JSON_TYPE_EXTRA_DATA :{
                if ((mis_check & 2) == 2) {
                  ret = OB_ERR_INVALID_CLAUSE;
                  LOG_USER_ERROR(OB_ERR_INVALID_CLAUSE, "ON ERROR");
                }
                break;
              }
              case OB_JSON_TYPE_TYPE_ERROR :{
                if ((mis_check & 4) == 4) {
                  ret = OB_ERR_INVALID_CLAUSE;
                  LOG_USER_ERROR(OB_ERR_INVALID_CLAUSE, "ON ERROR");
                }
                break;
              }
              default :{
                break;
              }
            }
          }
          if (OB_FAIL(ret)) {
          } else if (i % 2 == 0 && node->value_ == 2) {   // TODO json value udt type
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("Not support collection and UDT", K(ret));
          } else if (i % 2 == 1 && node->value_ <= 2) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("Not support collection and UDT", K(ret));
          } else {
            CK(OB_NOT_NULL(mismatch_arr[i]));
            OZ(SMART_CALL(recursive_resolve(mismatch_arr[i], para_expr)));
            CK(OB_NOT_NULL(para_expr));
            OZ(func_expr->add_param_expr(para_expr));
          }
        } //end mismatch for
        OX(expr = func_expr);
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_json_equal_node(const ParseNode *node, ObRawExpr *&expr)
{
  INIT_SUCC(ret);
  CK(T_FUN_SYS_JSON_EQUAL == node->type_);
  CK(3 == node->num_child_);
  int32_t num = node->num_child_;
  ObSysFunRawExpr *func_expr = NULL;
  OZ(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, func_expr));
  CK(OB_NOT_NULL(func_expr));
  OX(func_expr->set_func_name(ObString::make_string("json_equal")));

  for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
    ObRawExpr *para_expr = NULL;
    CK(OB_NOT_NULL(node->children_[i]));
    OZ(SMART_CALL(recursive_resolve(node->children_[i], para_expr)));
    CK(OB_NOT_NULL(para_expr));
    OZ(func_expr->add_param_expr(para_expr));
  }
  OX(expr = func_expr);
  return ret;
}

int ObRawExprResolverImpl::process_json_array_node(const ParseNode *node, ObRawExpr *&expr)
{
  INIT_SUCC(ret);
  CK(T_FUN_SYS_JSON_ARRAY == node->type_);
  CK(4 == node->num_child_);
  int32_t num = node->num_child_;
  ObSysFunRawExpr *func_expr = NULL;
  OZ(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, func_expr));
  CK(OB_NOT_NULL(func_expr));
  OX(func_expr->set_func_name(ObString::make_string("json_array")));

  const ParseNode *expr_node = node->children_[0];
  CK (OB_NOT_NULL(expr_node));

  if (OB_SUCC(ret) && lib::is_oracle_mode() && ctx_.current_scope_ == T_PL_SCOPE) {
    if (OB_FAIL(remove_strict_opt_in_pl(const_cast<ParseNode *>(node->children_[3]), OPT_JSON_ARRAY))) {
      LOG_WARN("set strict fail", K(ret));
    } else if (OB_FAIL(remove_format_json_opt_in_pl(const_cast<ParseNode *>(expr_node), OPT_JSON_ARRAY))) {
      LOG_WARN("set format json fail", K(ret));
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < expr_node->num_child_; ++i) {
    ObRawExpr *para_expr = NULL;
    CK(OB_NOT_NULL(expr_node->children_[i]));
    OZ(recursive_resolve(expr_node->children_[i], para_expr));
    CK(OB_NOT_NULL(para_expr));
    OZ(func_expr->add_param_expr(para_expr));
  }

  for (int32_t i = 1; OB_SUCC(ret) && i < num; i++) {
    ObRawExpr *para_expr = NULL;
    CK(OB_NOT_NULL(node->children_[i]));
    OZ(recursive_resolve(node->children_[i], para_expr));
    CK(OB_NOT_NULL(para_expr));
    OZ(func_expr->add_param_expr(para_expr));
  }
  OX(expr = func_expr);
  return ret;
}

int ObRawExprResolverImpl::process_json_mergepatch_node(const ParseNode *node, ObRawExpr *&expr)
{
  INIT_SUCC(ret);
  CK(T_FUN_SYS_JSON_MERGE_PATCH == node->type_);
  CK(7 == node->num_child_);
  int32_t num = node->num_child_;
  ObSysFunRawExpr *func_expr = NULL;
  OZ(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, func_expr));
  CK(OB_NOT_NULL(func_expr));
  OX(func_expr->set_func_name(ObString::make_string("json_merge_patch")));

  for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
    ObRawExpr *para_expr = NULL;
    CK(OB_NOT_NULL(node->children_[i]));
    OZ(SMART_CALL(recursive_resolve(node->children_[i], para_expr)));
    CK(OB_NOT_NULL(para_expr));
    OZ(func_expr->add_param_expr(para_expr));
  }
  OX(expr = func_expr);

  if (OB_SUCC(ret)) {
    ParseNode *trunc_node = node->children_[5];
    ParseNode *type_node = node->children_[2];
    if (trunc_node->value_
        && (T_LONGTEXT == type_node->int16_values_[0]
            || T_JSON == type_node->int16_values_[0])) {
      ret = OB_ERR_UNSUPPORT_TRUNCATE_TYPE;
      LOG_USER_ERROR(OB_ERR_UNSUPPORT_TRUNCATE_TYPE);
    }
  }
  return ret;
 }

int ObRawExprResolverImpl::process_is_json_node(const ParseNode *node, ObRawExpr *&expr)
{
  INIT_SUCC(ret);
  CK(T_FUN_SYS_IS_JSON == node->type_);
  CK(5 == node->num_child_);
  int32_t num = node->num_child_;
  ObSysFunRawExpr *func_expr = NULL;
  OZ(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, func_expr));
  CK(OB_NOT_NULL(func_expr));
  OX(func_expr->set_func_name(ObString::make_string("is_json")));

  for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
    ObRawExpr *para_expr = NULL;
    CK(OB_NOT_NULL(node->children_[i]));
    OZ(SMART_CALL(recursive_resolve(node->children_[i], para_expr)));
    CK(OB_NOT_NULL(para_expr));
    if (OB_SUCC(ret)) {
      OZ(para_expr->clear_flag(IS_CONST_EXPR));
      OZ(func_expr->add_param_expr(para_expr));
    }
  } //end for
  OX(expr = func_expr);
  return ret;
}

int ObRawExprResolverImpl::cast_accuracy_check(const ParseNode *node, const char *input)
{
  INIT_SUCC(ret);
  if (T_NUMBER_FLOAT == node->int16_values_[0]) {
    int64_t precision = node->int16_values_[2];
    if (OB_UNLIKELY(precision < OB_MIN_NUMBER_FLOAT_PRECISION)
        || OB_UNLIKELY(precision > OB_MAX_NUMBER_FLOAT_PRECISION)) {
      ret = OB_FLOAT_PRECISION_OUT_RANGE;
      LOG_WARN("precision of float out of range", K(ret), K(precision));
    }
  } else if (T_FLOAT == node->int16_values_[0]
              || T_DOUBLE == node->int16_values_[0]
              || T_NUMBER == node->int16_values_[0]) {
    const int16_t precision = node->int16_values_[2];
    const int16_t scale = node->int16_values_[3];
    if (lib::is_oracle_mode()) {
      if (precision != PRECISION_UNKNOWN_YET
          && (OB_UNLIKELY(precision < OB_MIN_NUMBER_PRECISION)
              || OB_UNLIKELY(precision > OB_MAX_NUMBER_PRECISION))) {
        ret = OB_NUMERIC_PRECISION_OUT_RANGE;
        LOG_WARN("precision of number overflow", K(ret), K(scale), K(precision));
      } else if (scale != ORA_NUMBER_SCALE_UNKNOWN_YET
                  && (OB_UNLIKELY(scale < OB_MIN_NUMBER_SCALE)
                      || OB_UNLIKELY(scale > OB_MAX_NUMBER_SCALE))) {
        ret = OB_NUMERIC_SCALE_OUT_RANGE;
        LOG_WARN("scale of number out of range", K(ret), K(scale));
      }
    } else {
      if (OB_UNLIKELY(precision < scale)) {
        ret = OB_ERR_M_BIGGER_THAN_D;
        LOG_USER_ERROR(OB_ERR_M_BIGGER_THAN_D, input);
      } else if (OB_UNLIKELY(precision > OB_MAX_DECIMAL_PRECISION)) {
        ret = OB_ERR_TOO_BIG_PRECISION;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, precision, input, OB_MAX_DECIMAL_PRECISION);
      } else if (OB_UNLIKELY(scale > OB_MAX_DOUBLE_FLOAT_SCALE)) {
        ret = OB_ERR_TOO_BIG_SCALE;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_SCALE, scale, input, OB_MAX_DECIMAL_SCALE);
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_fun_sys_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *func_expr = NULL;
  ObString func_name;
  if (OB_ISNULL(node) || OB_ISNULL(ctx_.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node), KP(ctx_.session_info_));
  } else if (OB_UNLIKELY(1 > node->num_child_) || OB_ISNULL(node->children_) || OB_ISNULL(node->children_[0])) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid node children for fun_sys node", K(ret), "node", SJ(ObParserResultPrintWrapper(*node)));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS, func_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(func_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func_expr is null");
  } else {
    ObString name(node->children_[0]->str_len_, node->children_[0]->str_value_);
    if (OB_FAIL(check_internal_function(name))) {
      LOG_WARN("unexpected internal function", K(ret));
    }
    if (OB_SUCC(ret)) {
      //oracle和mysql的decode函数含义完全不同
      if (lib::is_oracle_mode()) {
        if (0 == name.case_compare("ora_decode")) {
          ret = OB_ERR_FUNCTION_UNKNOWN;
          LOG_WARN("ora_decode cannot be reach in oracle mode", K(ret));
        } else if (0 == name.case_compare("decode")) {
          name = ObString::make_string("ora_decode");
        }
      }
    }

    if (OB_SUCC(ret)) {
      // deal with exceptions
      if (0 == name.case_compare("nextval")) {
        ret = OB_ERR_FUNCTION_UNKNOWN;
        LOG_USER_ERROR(OB_ERR_FUNCTION_UNKNOWN, "FUNCTION", name.length(), name.ptr());
      } else if (T_FROM_SCOPE != ctx_.current_scope_ && 0 == name.case_compare("generator")) {
        ret = OB_ERR_FUNCTION_UNKNOWN;
        LOG_USER_ERROR(OB_ERR_FUNCTION_UNKNOWN, "FUNCTION", name.length(), name.ptr());
      }
    }

    if (OB_FAIL(ret)) {
      /*^-^*/
    } else if (OB_FAIL(ob_write_string(ctx_.expr_factory_.get_allocator(), name, func_name))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Malloc function name failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && ObString::make_string("cast") == func_name) {
    if (OB_ISNULL(node->children_) || OB_ISNULL(node->children_[1])
    || OB_ISNULL(node->children_[1]->children_)
    || OB_ISNULL(node->children_[1]->children_[1])
    || OB_ISNULL(node->children_[1]->children_[0])) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid node children", K(ret), K(node));
    } else {
      const ParseNode *cast_arg = node->children_[1]->children_[1];
      const char *input = node->children_[1]->children_[0]->str_value_;
      ret = cast_accuracy_check(cast_arg, input);
    }
    if (OB_SUCC(ret)) {
      func_expr->set_extra(CM_EXPLICIT_CAST);
    }
  }
  // 在递归解析参数列表之前, 先检查下当前函数是否存在, 这是因为如果该函数报错OB_ERR_FUNCTION_UNKNOWN, 外部会继续尝试是否是UDF
  // 如果先解析参数列表, 则可能会将错误的column ref加入ctx_.columns_, 外部解析UDF的时候会重复加入导致解析错误
  if (OB_SUCC(ret)) {
    ObExprOperatorType type;
    type = ObExprOperatorFactory::get_type_by_name(func_name);
    if (OB_UNLIKELY(T_INVALID == (type))) {
      ret = OB_ERR_FUNCTION_UNKNOWN;
    }
  }

  if (OB_SUCC(ret)) {
    func_expr->set_func_name(func_name);
    if (node->num_child_ > 1) {
      if (OB_ISNULL(node->children_) || OB_ISNULL(node->children_[1])
          || OB_UNLIKELY(T_EXPR_LIST != node->children_[1]->type_)) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid node children", K(ret), K(node->children_));
      } else {
        ObRawExpr *para_expr = NULL;
        int32_t num = node->children_[1]->num_child_;
        int current_columns_count = ctx_.columns_->count();
        for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
          if (OB_ISNULL(node->children_[1]->children_[i])) {
            ret = OB_ERR_PARSER_SYNTAX;
            LOG_WARN("invalid parse tree", K(ret));
          } else if (T_EXPR_WITH_ALIAS == node->children_[1]->children_[i]->type_) {
            // only udf allow the the expr alias.
            ret = OB_ERR_FUNCTION_UNKNOWN;
          } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[1]->children_[i], para_expr)))) {
            LOG_WARN("fail to recursive resolve", K(ret), K(node->children_[1]->children_[i]));
          } else if (OB_FAIL(func_expr->add_param_expr(para_expr))) {
            LOG_WARN("fail to add param expr", K(ret), K(para_expr));
          }
        } //end for

        //
        // check param count
        if (ret != OB_SUCCESS) {
          int temp_ret = ret;
          if (OB_FAIL(ObRawExprUtils::function_alias(ctx_.expr_factory_, func_expr))) {
            LOG_WARN("failed to do function alias", K(ret), K(func_expr));
          } else if (OB_FAIL(func_expr->check_param_num(num))) {
            if (OB_ERR_FUNCTION_UNKNOWN != ret) {
              LOG_WARN("failed to check param num", K(func_name), K(num), K(ret));
            }
          }
          if (ret == OB_SUCCESS) {
            ret = temp_ret;
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(process_sys_func_params(*func_expr, current_columns_count))) {
            LOG_WARN("fail process sys func params", K(ret));
          }
        }
      }
    } //end > 1
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRawExprUtils::function_alias(ctx_.expr_factory_, func_expr))) {
      LOG_WARN("failed to do function alias", K(ret), K(func_expr));
    } else if (OB_FAIL(func_expr->check_param_num())) {
      if (OB_ERR_FUNCTION_UNKNOWN != ret) {
        LOG_WARN("failed to check param num", K(func_name), K(ret));
      }
    } else {
      expr = func_expr;
    }
  }

  // 处理可已共享的表达式rownum
  if (OB_SUCC(ret)) {
    if (T_FUN_SYS_ROWNUM == func_expr->get_expr_type() &&
        OB_NOT_NULL(ctx_.stmt_) && ctx_.stmt_->is_dml_stmt()) {
      ObDMLStmt *stmt = static_cast<ObDMLStmt *>(ctx_.stmt_);
      ObRawExpr *rownum_expr = NULL;
      if (OB_FAIL(stmt->get_rownum_expr(rownum_expr))) {
        LOG_WARN("failed to get rownum expr", K(ret));
      } else if (NULL == rownum_expr) {
        if (OB_FAIL(stmt->get_pseudo_column_like_exprs().push_back(func_expr))) {
          LOG_WARN("failed to push back rownum expr", K(ret));
        }
      } else {
        expr = rownum_expr;
      }
    }
  }

  //mark expr is deterministic or not(default deterministic)
  if (OB_SUCC(ret)) {
    //bug:
    //maybe have more exprs, can be added below in the future.
    if (lib::is_oracle_mode() && expr->get_expr_type() == T_FUN_SYS_REGEXP_REPLACE) {
      expr->set_is_deterministic(false);
    }
  }

  return ret;
}

int ObRawExprResolverImpl::process_sys_func_params(ObSysFunRawExpr &func_expr, int current_columns_count)
{
  int ret = OB_SUCCESS;
  const ObExprOperatorType expr_type = ObExprOperatorFactory::get_type_by_name(func_expr.get_func_name());
  switch (expr_type)
  {
    case T_FUN_SYS_NAME_CONST:
      if (current_columns_count != ctx_.columns_->count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_NAME_CONST);
        LOG_WARN("params of name_const contain column references", K(ret));
      }
      break;
    case T_FUN_SYS_UUID2BIN:
    case T_FUN_SYS_BIN2UUID:
      if (2 == func_expr.get_param_count()) {
        //add bool expr for the second param
        ObRawExpr *param_expr = func_expr.get_param_expr(1);
        ObRawExpr *new_param_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::try_create_bool_expr(param_expr, new_param_expr, ctx_.expr_factory_))) {
          LOG_WARN("create_bool_expr failed", K(ret));
        } else if (OB_ISNULL(new_param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new param_expr is NULL", K(ret));
        } else {
          func_expr.replace_param_expr(1, new_param_expr);
        }
      }
      break;
    case T_FUN_SYS_GENERATOR:
      if (1 == func_expr.get_param_count()) {
        ObItemType type = func_expr.get_param_expr(0)->get_expr_type();
        if (T_QUESTIONMARK != type && T_INT != type && T_NUMBER != type) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("the func expr param is invalid", K(*func_expr.get_param_expr(0)), K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "generator function. The argument should be a constant integer");
        }
      }
      break;
    case T_FUN_SYS_DATE_ADD:
        // Added a fourth parameter to distinguish between “date + interval” and “interval + date”
        // So you should use it to distinguish these two cases
        if(4 == func_expr.get_param_count()) {
          ObRawExpr *expr3 = func_expr.get_param_expr(3);
          if(OB_LIKELY(NULL != expr3 && expr3->is_const_raw_expr())) {
            ObConstRawExpr *const_expr = static_cast<ObConstRawExpr *>(expr3);
            int64_t expr3_val = OB_INVALID_ID;
            if(T_INT == const_expr->get_expr_type()) {
              if (OB_FAIL(const_expr->get_value().get_int(expr3_val))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get fourth para value from date_add func failed", K(ret));
              } else if (2 == expr3_val) {
                ObRawExpr *expr0 = func_expr.get_param_expr(0);
                ObRawExpr *expr1 = func_expr.get_param_expr(1);
                if (OB_FAIL(func_expr.remove_param_expr(3))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("the func expr param3 is invalid", K(ret));
                } else if (OB_FAIL(func_expr.remove_param_expr(0))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("the func expr param0 is invalid", K(ret));
                } else if (OB_FAIL(func_expr.remove_param_expr(0))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("the func expr param0 is invalid", K(ret));
                } else if (OB_FAIL(func_expr.add_param_expr(expr0))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("the func expr param0 is invalid", K(ret));
                } else if (OB_FAIL(func_expr.add_param_expr(expr1))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("the func expr param0 is invalid", K(ret));
                }
              } else if (1 == expr3_val) {
                if (OB_FAIL(func_expr.remove_param_expr(3))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("the func expr param3 is invalid", K(ret));
                }
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("the func expr3 is null or not const expr", K(ret));
          }
        }
      break;
    default:
      break;
  }
  return ret;
}

int ObRawExprResolverImpl::resolve_udf_node(const ParseNode *node, ObUDFInfo &udf_info)
{
  int ret = OB_SUCCESS;
  ObUDFRawExpr *func_expr = NULL;
  if (OB_ISNULL(node) || T_FUN_UDF != node->type_ || OB_ISNULL(ctx_.udf_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node), K(ctx_.udf_info_));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid node children", K(ret), K(node->children_));
  } else if (4 != node->num_child_) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid node children number", K(ret), K(node->num_child_));
  } else if (OB_ISNULL(node->children_[0])) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid function name node", K(ret));
  } else if (FALSE_IT(new(&udf_info)ObUDFInfo())) {
    // reinit ObUDFInfo, resolve_qname may call this function mutli times.
  } else if (OB_FAIL(ObResolverUtils::resolve_udf_name_by_parse_node(node, ctx_.case_mode_, udf_info))) {
    LOG_WARN("fail to result udf", K(ret));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_UDF, func_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(func_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret));
  } else if (OB_NOT_NULL(node->children_[1])) {
    if (T_EXPR_LIST != node->children_[1]->type_) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid paramters node", K(ret), K(node->children_[1]));
    } else {
      ObRawExpr *param_expr = NULL;
      int32_t num_child = node->children_[1]->num_child_;
      bool has_assign_expr = false;
      for (int32_t i = 0; OB_SUCC(ret) && i < num_child; ++i) {
        const ParseNode *param_node = node->children_[1]->children_[i];
        if (OB_ISNULL(param_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param node is null", K(ret));
        } else if (T_SP_CPARAM == param_node->type_) {
          if (param_node->num_child_ != 2
            || OB_ISNULL(param_node->children_[0])
            || OB_ISNULL(param_node->children_[1])) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("param node is invalid", K(ret));
          } else if (T_IDENT != param_node->children_[0]->type_) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid param name node", K(ret), K(param_node->children_[0]->type_));
          } else {
            has_assign_expr = true;
            ObString param_name(static_cast<int32_t>(param_node->children_[0]->str_len_),
                                param_node->children_[0]->str_value_);
            if (param_name.empty()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("param node string is empty", K(ret));
            } else if (OB_FAIL(udf_info.param_names_.push_back(param_name))) {
              LOG_WARN("failed to push back", K(ret), K(param_name));
            } else if (OB_FAIL(SMART_CALL(recursive_resolve(param_node->children_[1], param_expr)))) {
              LOG_WARN("failed to recursive resolve", K(ret));
            } else if (OB_FAIL(udf_info.param_exprs_.push_back(param_expr))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
        } else if (has_assign_expr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not get parameter after assign", K(ret));
        } else if (OB_FAIL(SMART_CALL(recursive_resolve(param_node, param_expr)))) {
          LOG_WARN("fail to recursive resolve udf parameters", K(ret), K(param_node));
        } else if (OB_FAIL(func_expr->add_param_expr(param_expr))) {
          LOG_WARN("fail to add param expr", K(ret), K(param_expr));
        } else {
          udf_info.udf_param_num_++;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ctx_.query_ctx_ != NULL) {
      ctx_.query_ctx_->has_udf_ = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.query_ctx_->all_user_variable_.count(); ++i) {
        if (OB_ISNULL(ctx_.query_ctx_->all_user_variable_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null user var expr", K(ret));
        } else {
          ctx_.query_ctx_->all_user_variable_.at(i)->set_query_has_udf(true);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    func_expr->set_func_name(udf_info.udf_name_);
    udf_info.ref_expr_ = func_expr;
    if (OB_FAIL(func_expr->extract_info())) {
      LOG_WARN("fail to extract info of udf", K(ret), K(func_expr));
    }
  }
  return ret;
}


int ObRawExprResolverImpl::not_int_check(const ObRawExpr *expr)		
{		
  int ret = OB_SUCCESS;		
  if (lib::is_mysql_mode() && NULL != expr && T_INT == expr->get_expr_type()) {		
    ret = OB_ERR_WINDOW_ILLEGAL_ORDER_BY;		
    LOG_WARN("int not expected in window function's orderby ", K(ret));		
  }		
  return ret;		
}

int ObRawExprResolverImpl::not_row_check(const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (NULL != expr && T_OP_ROW == expr->get_expr_type()) {
    ret = OB_ERR_INVALID_COLUMN_NUM;
    LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
    LOG_WARN("row not expected", K(ret), K(expr->get_param_count()));
  }
  return ret;
}

int ObRawExprResolverImpl::param_not_row_check(const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && NULL != expr && i < expr->get_param_count(); i++) {
    OZ(not_row_check(expr->get_param_expr(i)));
  }
  return ret;
}

int ObRawExprResolverImpl::process_window_function_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;

  ObWinFunRawExpr *win_func = NULL;
  ParseNode *func_node = NULL;
  const static int win_function_num_child = 2;
  if (!is_win_expr_valid_scope(ctx_.current_scope_)
      || ctx_.parents_expr_info_.has_member(IS_AGG)
      || ctx_.parents_expr_info_.has_member(IS_WINDOW_FUNC)) {
    ret = OB_ERR_INVALID_WINDOW_FUNCTION_PLACE;
    LOG_WARN("window function is not allowed here", K(ret), K(ctx_.current_scope_), K(ctx_.parents_expr_info_));
  } else if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node));
  } else if (OB_ISNULL(node->children_) || node->num_child_ != win_function_num_child) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node->children_), K(node->num_child_));
  } else if (OB_ISNULL(func_node = node->children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(func_node));
  } else if (T_WIN_FUN_RATIO_TO_REPORT == func_node->type_) {
    ParseNode *div_node = NULL;
    if (OB_FAIL(transform_ratio_afun_to_arg_div_sum(node, div_node))) {
      LOG_WARN("transform ratio_to_report node to div(arg, sum) failed", K(ret));
    } else if (OB_ISNULL(div_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("div node is null", K(ret), K(div_node));
    } else if (OB_FAIL(process_operator_node(div_node, expr))) {
      LOG_WARN("process div node failed", K(ret));
    }
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_WINDOW_FUNCTION, win_func))) {
    LOG_WARN("create window function expr failed", K(ret));
  } else {
    ObItemType func_type = func_node->type_;
    ObRawExpr *agg_node_expr = NULL;
    ObArray<ObRawExpr *> func_params;
    ObRawExpr *func_param = NULL;
    if (T_FUN_COUNT == func_type
        || T_FUN_AVG == func_type
        || T_FUN_MIN == func_type
        || T_FUN_MAX == func_type
        || T_FUN_SUM == func_type
        || T_FUN_MEDIAN == func_type
        || T_FUN_STDDEV == func_type
        || T_FUN_STDDEV_SAMP == func_type
        || T_FUN_VARIANCE == func_type
        || T_FUN_STDDEV_POP == func_type
        || T_FUN_APPROX_COUNT_DISTINCT == func_type
        || T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == func_type
        || T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE == func_type
        || T_FUN_GROUP_CONCAT == func_type
        || T_FUN_GROUP_PERCENTILE_CONT == func_type
        || T_FUN_GROUP_PERCENTILE_DISC == func_type
        || T_FUN_CORR == func_type
        || T_FUN_COVAR_POP == func_type
        || T_FUN_COVAR_SAMP == func_type
        || T_FUN_VAR_POP == func_type
        || T_FUN_VAR_SAMP == func_type
        || T_FUN_REGR_SLOPE == func_type
        || T_FUN_REGR_INTERCEPT == func_type
        || T_FUN_REGR_R2 == func_type
        || T_FUN_REGR_COUNT == func_type
        || T_FUN_REGR_AVGX == func_type
        || T_FUN_REGR_AVGY == func_type
        || T_FUN_REGR_SXX == func_type
        || T_FUN_REGR_SYY == func_type
        || T_FUN_REGR_SXY == func_type
        || T_FUN_KEEP_MAX == func_type
        || T_FUN_KEEP_MIN == func_type
        || T_FUN_KEEP_SUM == func_type
        || T_FUN_KEEP_COUNT == func_type
        || T_FUN_KEEP_AVG == func_type
        || T_FUN_KEEP_STDDEV == func_type
        || T_FUN_KEEP_VARIANCE == func_type
        || T_FUN_KEEP_WM_CONCAT == func_type
        || T_FUN_WM_CONCAT == func_type
        || T_FUN_TOP_FRE_HIST == func_type
        || T_FUN_PL_AGG_UDF == func_type
        || T_FUN_HYBRID_HIST == func_type
        || T_FUN_SYS_BIT_AND == func_type
        || T_FUN_SYS_BIT_OR == func_type
        || T_FUN_SYS_BIT_XOR == func_type
        || T_FUN_JSON_ARRAYAGG == func_type
        || T_FUN_JSON_OBJECTAGG == func_type) {
      ctx_.is_win_agg_ = true;
      if (T_FUN_PL_AGG_UDF == func_type) {
        ParseNode *agg_udf_node = NULL;
        ParseNode *obj_access_node = NULL;
        if (OB_UNLIKELY(func_node->num_child_ != 3)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(func_node->num_child_));
        } else if (OB_FAIL(ObRawExprUtils::new_parse_node(agg_udf_node, ctx_.expr_factory_,
                                                          T_FUN_SYS, 3))) {
          LOG_WARN("failed to new parse node", K(ret), K(agg_udf_node));
        } else if (OB_FAIL(ObRawExprUtils::new_parse_node(obj_access_node, ctx_.expr_factory_,
                                                          T_OBJ_ACCESS_REF, 2))) {
          LOG_WARN("failed to new parse node", K(ret), K(obj_access_node));
        } else {
          // structure of T_FUN_PL_AGG_UDF: refer to sql_parser_oracle_mode.y
          CK (OB_NOT_NULL(func_node->children_[0])
              && T_EXPR_LIST == func_node->children_[0]->type_
              && 2 == func_node->children_[0]->num_child_);

          if (OB_SUCC(ret)) {
            agg_udf_node->children_[0] = func_node->children_[0]->children_[0]; // restore IDENT node
            agg_udf_node->children_[1] = func_node->children_[1];
            agg_udf_node->children_[2] = func_node->children_[2];

            obj_access_node->children_[0] = agg_udf_node;
            obj_access_node->children_[1] = nullptr;

            if (nullptr != func_node->children_[0]->children_[1]) { // check if database name is present
              ParseNode *parent_obj_access = nullptr;
              OZ (ObRawExprUtils::new_parse_node(parent_obj_access, ctx_.expr_factory_,T_OBJ_ACCESS_REF, 2));
              OX (parent_obj_access->children_[0] = func_node->children_[0]->children_[1]);
              OX (parent_obj_access->children_[1] = obj_access_node);
              OX (obj_access_node = parent_obj_access);
            }

            OX (func_node = obj_access_node);
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ctx_.parents_expr_info_.add_member(IS_WINDOW_FUNC))) {
        LOG_WARN("failed to add member to parent exprs info.", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(func_node, agg_node_expr)))) {
        LOG_WARN("resolve agg failed", K(ret));
      } else if (OB_FAIL(ctx_.parents_expr_info_.del_member(IS_WINDOW_FUNC))) {
        LOG_WARN("failed to delete member from parent exprs info.", K(ret));
      } else { /*do nothing.*/ }
    } else if (T_WIN_FUN_NTILE == func_type) {
      if (OB_FAIL(ctx_.parents_expr_info_.add_member(IS_WINDOW_FUNC))) {
        LOG_WARN("failed to add member to parents epxrs info.", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(func_node->children_[0], func_param)))) {
        LOG_WARN("recursive resolve node failed", K(ret));
      } else if (OB_FAIL(ctx_.parents_expr_info_.del_member(IS_WINDOW_FUNC))) {
        LOG_WARN("failed to delete member to parents epxrs info.", K(ret));
      } else if (OB_ISNULL(func_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("func_param is null", K(func_param), K(ret));
      } else {
        ObSysFunRawExpr *floor_expr = NULL;
        //we create a floor expr to deal with ntile(1 + 1.1)
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_SYS_FLOOR, floor_expr))) {
          LOG_WARN("create floor function expr failed", K(ret));
        } else if (OB_ISNULL(floor_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sys_floor_expr is null", K(ret));
        } else if (OB_FAIL(floor_expr->add_param_expr(func_param))) {
          LOG_WARN("failed to add param expr", K(ret));
        } else if (OB_FAIL(func_params.push_back(floor_expr))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("push back func_param failed", K(floor_expr), K(ret));
        }
      }
    } else if (T_WIN_FUN_NTH_VALUE == func_type) {
      ParseNode *measure_expr_node = NULL;
      ParseNode *n_expr_node = NULL;
      ObRawExpr *measure_expr = NULL;
      ObRawExpr *n_expr = NULL;
      if (OB_UNLIKELY(func_node->num_child_ != 4)
          || OB_ISNULL(measure_expr_node = func_node->children_[0])
          || OB_ISNULL(n_expr_node = func_node->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("function node is invalid", K(ret));
      } else if (OB_FAIL(ctx_.parents_expr_info_.add_member(IS_WINDOW_FUNC))) {
        LOG_WARN("failed to add member to parents epxrs info.", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(measure_expr_node, measure_expr)))) {
        LOG_WARN("fail to recursive resolve measure_expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(n_expr_node, n_expr)))) {
        LOG_WARN("fail to recursive resolve measure_expr", K(ret));
      } else if (OB_FAIL(ctx_.parents_expr_info_.del_member(IS_WINDOW_FUNC))) {
        LOG_WARN("failed to delete member to parents epxrs info.", K(ret));
      } else if (OB_FAIL(func_params.push_back(measure_expr))) {
        LOG_WARN("fail to add param expr", K(ret));
      } else if (OB_FAIL(func_params.push_back(n_expr))) {
        LOG_WARN("fail to add param expr", K(ret));
      } else if (OB_FAIL(n_expr->extract_info())) {
        LOG_WARN("faield to extract info", K(ret));
      } else if (OB_UNLIKELY(lib::is_mysql_mode() && !n_expr->is_const_expr())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments to nth_value", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "nth_value");
      } else {
        win_func->set_is_from_first(NULL == func_node->children_[2] || T_FIRST == func_node->children_[2]->type_);
        win_func->set_is_ignore_null(!(NULL == func_node->children_[3] || T_RESPECT == func_node->children_[3]->type_));
      }
    } else if (T_WIN_FUN_LEAD == func_type || T_WIN_FUN_LAG == func_type) {
      ParseNode *expr_list_node = NULL;
      ObRawExpr *sub_expr = NULL;
      if (OB_UNLIKELY(func_node->num_child_ != 2) || OB_ISNULL(expr_list_node = func_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("function node is invalid", K(ret));
      } else if (OB_UNLIKELY(T_EXPR_LIST != expr_list_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr list node type is invalid", K(ret));
      } else if (OB_UNLIKELY(expr_list_node->num_child_ < 1 || expr_list_node->num_child_ > 3)) {
        // LEAD and LAG should have at least 1 argument and at most 3 arguments.
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument number", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < expr_list_node->num_child_; ++i) {
          if (OB_ISNULL(expr_list_node->children_[i])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid expr list node children", K(ret), K(i), K(expr_list_node->children_[i]));
          } else if (OB_FAIL(ctx_.parents_expr_info_.add_member(IS_WINDOW_FUNC))) {
            LOG_WARN("failed to add member to parents epxrs info.", K(ret));
          } else if (OB_FAIL(SMART_CALL(recursive_resolve(expr_list_node->children_[i], sub_expr)))) {
            LOG_WARN("fail to recursive resolve expr list item", K(ret));
          } else if (OB_FAIL(ctx_.parents_expr_info_.del_member(IS_WINDOW_FUNC))) {
            LOG_WARN("failed to delete member to parents epxrs info.", K(ret));
          } else if (OB_FAIL(func_params.push_back(sub_expr))) {
            LOG_WARN("fail to add param expr to agg expr", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (NULL == func_node->children_[1] || T_RESPECT == func_node->children_[1]->type_) {
            win_func->set_is_ignore_null(false);
          } else {
            win_func->set_is_ignore_null(true);
          }
        }
      }
    } else if (T_WIN_FUN_FIRST_VALUE == func_type || T_WIN_FUN_LAST_VALUE == func_type) {
      ParseNode *measure_expr_node = NULL;
      ObRawExpr *measure_expr = NULL;
      if (OB_UNLIKELY(func_node->num_child_ != 2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("function node is invalid", K(ret));
      } else if (OB_ISNULL(measure_expr_node = func_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid node children", K(ret));
      } else if (OB_FAIL(ctx_.parents_expr_info_.add_member(IS_WINDOW_FUNC))) {
        LOG_WARN("failed to add member to parents epxrs info.", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_resolve(measure_expr_node, measure_expr)))) {
        LOG_WARN("fail to recursive resolve expr", K(ret));
      } else if (OB_FAIL(ctx_.parents_expr_info_.del_member(IS_WINDOW_FUNC))) {
        LOG_WARN("failed to delete member to parents epxrs info.", K(ret));
      } else if (OB_FAIL(func_params.push_back(measure_expr))) {
        LOG_WARN("fail to add param expr", K(ret));
      } else {
        win_func->set_is_ignore_null(!(NULL == func_node->children_[1] || T_RESPECT == func_node->children_[1]->type_));
      }
      // convert to nth_value
      if (OB_SUCC(ret)) {
        win_func->set_is_from_first(T_WIN_FUN_FIRST_VALUE == func_type);
        func_type = T_WIN_FUN_NTH_VALUE;
        ObConstRawExpr *c_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_INT, c_expr))) {
          LOG_WARN("fail to create raw expr", K(ret));
        } else if (OB_ISNULL(c_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("const expr is null");
        } else {
          ObObj val;
          val.set_int(1);
          c_expr->set_value(val);
          c_expr->set_param(val);
          if (OB_FAIL(func_params.push_back(c_expr))) {
            LOG_WARN("fail to add param expr", K(ret));
          }
        }
      }
    }
    ParseNode *name_node = NULL;
    ParseNode *win_node = NULL;
    if (T_WIN_NEW_GENERALIZED_WINDOW == node->children_[1]->type_) {
      name_node = node->children_[1]->children_[0];
      win_node = node->children_[1]->children_[1];
    } else {
      win_node = node->children_[1];
    }
    // window_function          ->   node                       ->   count(*) over (w order by c)
    // count                    ->   node->child[0]             ->   count(*)
    // new_generalized_window   ->   node->child[1]             ->   (w order by c)
    // name_ob                  ->   node->child[1]->child[0]   ->   w
    // generalized_window       ->   node->child[1]->child[1]   ->   order by c
    if(OB_SUCC(ret)) {
      if (OB_ISNULL(name_node) && OB_ISNULL(win_node))  {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(name_node), K(win_node), K(ret));
      }
    }
    ObWinFunRawExpr *named_win = NULL;
    if (OB_SUCC(ret) && name_node) {
      ObString name(static_cast<int32_t>(name_node->str_len_), name_node->str_value_);
      if (!ctx_.stmt_->is_select_stmt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected stmt", K(ret));
      } else {
        ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(ctx_.stmt_);
        ObIArray<ObWinFunRawExpr *> &named_windows = select_stmt->get_window_func_exprs();
        for (int64_t i = 0; OB_SUCC(ret) && i < named_windows.count(); ++i) {
          if (ObCharset::case_insensitive_equal(name,
                                                named_windows.at(i)->get_win_name())) {
            named_win = named_windows.at(i);
            break;
          }
        }
        if (OB_SUCC(ret) && NULL == named_win)  {
          ret = OB_ERR_WINDOW_NAME_IS_NOT_DEFINE;
          LOG_WARN("name win not exist", K(name), K(ret), K(named_windows));
          LOG_USER_ERROR(OB_ERR_WINDOW_NAME_IS_NOT_DEFINE, name.length(), name.ptr());
        } else if (OB_UNLIKELY(ctx_.current_scope_ == T_NAMED_WINDOWS_SCOPE &&
                               named_win->has_frame_orig())) {
          ret = OB_EER_WINDOW_NO_INHERIT_FRAME;
          LOG_WARN("Named window cann't be modified by another framing property", K(ret));
          ObString tmp_name = named_win->get_win_name().empty() ? ObString("<unnamed window>") : named_win->get_win_name();
          LOG_USER_ERROR(OB_EER_WINDOW_NO_INHERIT_FRAME, tmp_name.length(), tmp_name.ptr());
        }
      }
    }
    ParseNode *partition_by_node = win_node ? win_node->children_[0] : NULL;
    ParseNode *order_by_node = win_node ? win_node->children_[1] : NULL;
    ParseNode *frame_node = win_node ? win_node->children_[2] : NULL;
    int8_t win_seg = 0;
    int8_t r_win_seg = 0;
    const static int8_t p_pos = 1 << 0; // partition
    const static int8_t o_pos = 1 << 1; // order
    const static int8_t f_pos = 1 << 2; // frame
    if (OB_SUCC(ret)) {
      if (named_win) {
        win_seg |= p_pos;
      }
      if (named_win && named_win->get_order_items().count() != 0) {
        win_seg |= o_pos;
      }
      if (named_win && named_win->has_frame_orig()) {
        win_seg |= f_pos;
      }
      if (partition_by_node) {
        r_win_seg |= p_pos;
      }
      if (order_by_node) {
        r_win_seg |= o_pos;
      }
      if (frame_node) {
        r_win_seg |= f_pos;
      }
      if (OB_UNLIKELY(win_seg & r_win_seg)) {
        // verification sequence : frame > partition > order
        if (f_pos & (win_seg & r_win_seg)) {
          ret = OB_EER_WINDOW_NO_INHERIT_FRAME;
          LOG_USER_ERROR(OB_EER_WINDOW_NO_INHERIT_FRAME,
              named_win->get_win_name().length(), named_win->get_win_name().ptr());
        } else if (p_pos & (win_seg & r_win_seg)) {
          ret = OB_EER_WINDOW_NO_CHILD_PARTITIONING;
          LOG_USER_ERROR(OB_EER_WINDOW_NO_CHILD_PARTITIONING);
        } else if (o_pos & (win_seg & r_win_seg)) {
          ret = OB_EER_WINDOW_NO_REDEFINE_ORDER_BY;
          // LOG_USER_ERROR in upper level function
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("Named window modified by another framing, partitioning or ordering property",
            K(ret), K(win_seg), K(r_win_seg), K(win_seg & r_win_seg));
      }
    }
    if (OB_SUCC(ret) && lib::is_oracle_mode() &&
        (T_FUN_GROUP_CONCAT == func_type ||
         T_FUN_KEEP_MAX == func_type ||
         T_FUN_KEEP_MIN == func_type ||
         T_FUN_KEEP_COUNT == func_type ||
         T_FUN_KEEP_SUM == func_type ||
         T_FUN_KEEP_STDDEV == func_type ||
         T_FUN_KEEP_VARIANCE == func_type ||
         T_FUN_MEDIAN == func_type ||
         T_FUN_GROUP_PERCENTILE_CONT == func_type ||
         T_FUN_GROUP_PERCENTILE_DISC == func_type ||
         T_FUN_KEEP_WM_CONCAT == func_type)
        && (OB_NOT_NULL(order_by_node) || OB_NOT_NULL(frame_node))) {
      ret = OB_ORDERBY_CLAUSE_NOT_ALLOWED;
      LOG_WARN("ORDER BY not allowed here", K(ret));
    }
    if (OB_SUCC(ret) && lib::is_oracle_mode()
        && OB_NOT_NULL(frame_node) && OB_ISNULL(order_by_node)) {
      ret = OB_ERR_MISS_ORDER_BY_EXPR;
      LOG_WARN("missing ORDER BY expression in the window specification", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObArray<ObRawExpr *> partition_exprs;
      ObArray<OrderItem> order_items;
      ObFrame frame;
      ObString sort_str;

      if (win_seg & p_pos) {
        partition_by_node = NULL;
        ret = partition_exprs.assign(named_win->get_partition_exprs());
      }
      if (OB_SUCC(ret) && (win_seg & o_pos)) {
        order_by_node = NULL;
        sort_str = named_win->sort_str_;
        ret = order_items.assign(named_win->get_order_items());
      }
      if (OB_SUCC(ret) && (win_seg & f_pos)) {
        frame_node = NULL;
        ret = frame.assign(*named_win);
      }
#define RESOLVE_EXPR_LIST(expr_list_node, exprs)                             \
  if (OB_UNLIKELY(T_EXPR_LIST != expr_list_node->type_)                      \
      || OB_ISNULL(expr_list_node->children_)) {                             \
    ret = OB_ERR_UNEXPECTED;                                                 \
    LOG_WARN("invalid children", K(ret), K(expr_list_node->type_),           \
             K(expr_list_node->children_));                                  \
  }                                                                          \
  for (int64_t i = 0; OB_SUCC(ret) && i < expr_list_node->num_child_; ++i) { \
    ObRawExpr *sub_expr = NULL;                                              \
    if (OB_ISNULL(expr_list_node->children_[i])) {                           \
      ret = OB_ERR_UNEXPECTED;                                               \
      LOG_WARN("invalid expr list node children", K(ret), K(i), K(expr_list_node->children_[i])); \
    } else if (OB_FAIL(SMART_CALL(recursive_resolve(expr_list_node->children_[i], sub_expr)))) {              \
      LOG_WARN("fail to recursive resolve expr list item", K(ret));  \
    } else {                                                         \
      OZ(not_row_check(sub_expr));                                   \
      OZ(exprs.push_back(sub_expr));                                 \
    }                                                                \
 }

      if (OB_SUCC(ret) && NULL != partition_by_node) {
        RESOLVE_EXPR_LIST(partition_by_node, partition_exprs);
      }
      if (OB_SUCC(ret) && NULL != order_by_node) {
        if (OB_UNLIKELY(order_by_node->type_ != T_ORDER_BY)
            || OB_UNLIKELY(order_by_node->num_child_ != 2)
            || OB_ISNULL(order_by_node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("order by node is null or invalid", K(order_by_node));
        } else if (OB_UNLIKELY(NULL != order_by_node->children_[1])) {
          ret = OB_ERR_CBY_OREDER_SIBLINGS_BY_NOT_ALLOWED;
          LOG_WARN("ORDER SIBLINGS BY clause not allowed here");
        } else if (OB_FAIL(process_sort_list_node(order_by_node->children_[0], order_items))) {
          LOG_WARN("process sort list failed", K(ret));
        } else {
          ParseNode *sort_node = order_by_node->children_[0]->children_[0];
          if (sort_node) {
            ParseNode *sort_expr_node = sort_node->children_[0];
            if (sort_expr_node) {
              sort_str.assign_ptr(const_cast<char *>(sort_expr_node->str_value_),
                                  static_cast<int32_t>(sort_expr_node->str_len_));
            }
          }
        }
      }
      if (OB_SUCC(ret) && NULL != frame_node) {
        if (OB_FAIL(process_frame_node(frame_node, frame))) {
          LOG_WARN("process window node failed", K(ret));
        } else if (lib::is_mysql_mode() &&
                   OB_UNLIKELY((frame.get_upper().interval_expr_ != NULL && !frame.get_upper().interval_expr_->is_const_expr()) ||
                               (frame.get_lower().interval_expr_ != NULL && !frame.get_lower().interval_expr_->is_const_expr()))) {
          ret = OB_ERR_WINDOW_RANGE_BOUND_NOT_CONSTANT;
          LOG_WARN("Window has a non-constant frame bound.", K(ret), KPC(frame.get_upper().interval_expr_), KPC(frame.get_lower().interval_expr_));
          ObString tmp_name = named_win == NULL ? ObString("<unnamed window>") : named_win->get_win_name();
          LOG_USER_ERROR(OB_ERR_WINDOW_RANGE_BOUND_NOT_CONSTANT, tmp_name.length(), tmp_name.ptr());
        }
      }
      if (OB_SUCC(ret)) {
        win_func->set_func_type(func_type);
        win_func->set_has_frame_orig(frame_node != NULL);
        if (OB_FAIL(win_func->set_func_params(func_params))) {
          LOG_WARN("set function parameters failed", K(ret));
        } else if (OB_FAIL(win_func->set_partition_exprs(partition_exprs))) {
          LOG_WARN("set partition_by exprs failed", K(ret));
        } else if (OB_FAIL(win_func->set_order_items(order_items))) {
          LOG_WARN("set order_by exprs failed", K(ret));
        } else if (OB_FAIL(win_func->ObFrame::assign(frame))) {
          LOG_WARN("assign frame failed", K(ret));
        } else {
          win_func->sort_str_ = sort_str;
          expr = win_func;
          //for T_FUN_PL_AGG_UDF, we will deal with expr together later.
          if (agg_node_expr != NULL && func_type != T_FUN_PL_AGG_UDF) {
            if (OB_FAIL(process_window_agg_node(func_type, win_func, agg_node_expr, expr))) {
              LOG_WARN("process window agg node failed", K(ret));
            }
          } else {
            expr = win_func;
            ret = ctx_.win_exprs_->push_back(win_func);
            if (func_type == T_FUN_PL_AGG_UDF) {
              win_func->set_pl_agg_udf_expr(agg_node_expr);
            }
          }
        }
      }
    }
  }

  OZ(param_not_row_check(expr));
  return ret;
}

int ObRawExprResolverImpl::process_window_agg_node(const ObItemType func_type,
                                                   ObWinFunRawExpr *win_func,
                                                   ObRawExpr *agg_node_expr,
                                                   ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObAggFunRawExpr *agg_expr = static_cast<ObAggFunRawExpr *>(agg_node_expr);
  if (OB_ISNULL(agg_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null pointer.", K(ret));
  } else if ((T_FUN_SUM == func_type || T_FUN_COUNT == func_type || T_FUN_AVG == func_type ||
              T_FUN_VARIANCE == func_type || T_FUN_STDDEV == func_type) &&
             (agg_expr->is_param_distinct() &&
              (win_func->has_order_items() || win_func->has_frame_orig()))) {
    ret = OB_ORDERBY_CLAUSE_NOT_ALLOWED;
  } else if (FALSE_IT(win_func->set_agg_expr(agg_expr))) {
  } else if (OB_FAIL(ctx_.win_exprs_->push_back(win_func))) {
    LOG_WARN("failed to push back into win exprs.", K(ret));
  } else {
    expr = win_func;
  }

  return ret;
}

int ObRawExprResolverImpl::process_sort_list_node(const ParseNode *node, ObIArray<OrderItem> &order_items)
{
  int ret = OB_SUCCESS;
  order_items.reset();

  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (NULL == node->children_) {
    // do-nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      OrderItem order_item;
      const ParseNode *sort_node = node->children_[i];
      if (OB_ISNULL(sort_node) || OB_UNLIKELY(2 != sort_node->num_child_)
          || OB_ISNULL(sort_node->children_)
          || OB_ISNULL(sort_node->children_[0])
          || OB_ISNULL(sort_node->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid sort node", K(ret), K(sort_node));
      } else if (OB_FAIL(ObResolverUtils::set_direction_by_mode(*sort_node, order_item))) {
        LOG_WARN("failed to set direction by mode", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(SMART_CALL(recursive_resolve(sort_node->children_[0], order_item.expr_)))) {
          LOG_WARN("fail to recursive resolve order item expr", K(ret));
        } else {
          /* check order_item.expr_:		
           * 1. shouldn't be int,		
           * 2. if is number, ignore the order_item.(all group are in same group);
           * shouldn't check int in deduce_type, because stmt like "select rank() over(order by c1) from t1 where c1=1;"
           * the stmt would be rewritten to "select rank() over(order by 1) from t1;".
           * if we check int type while deducing type, the above stmt will raise error, however, it should succeed in mysql.
           */
          OZ(not_int_check(order_item.expr_));
          OZ(not_row_check(order_item.expr_));
          OZ(order_items.push_back(order_item));
        }
      }
    } // end for
  } //end else
  return ret;
}

int ObRawExprResolverImpl::process_frame_node(const ParseNode *node,
                                              ObFrame &frame)
{
  int ret = OB_SUCCESS;

  frame.win_type_ = WINDOW_MAX;
  frame.is_between_ = false;

  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (node->type_ != T_WIN_WINDOW) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node->type_));
  } else {
    ParseNode *win_type_node = node->children_[0];
    frame.is_between_ = 1 == node->value_;

    if (1 == win_type_node->value_) {
      frame.win_type_ = WINDOW_ROWS;
    } else if (2 == win_type_node->value_) {
      frame.win_type_ = WINDOW_RANGE;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(win_type_node->value_));
    }

    if (OB_SUCC(ret)) {
      ParseNode *upper_bound_node = node->children_[1];
      if (OB_FAIL(process_bound_node(upper_bound_node, true, frame.get_upper()))) {
        LOG_WARN("process upper bound node failed", K(ret));
      } else if (frame.is_between_) {
        ParseNode *lower_bound_node = node->children_[2];
        if (OB_FAIL(process_bound_node(lower_bound_node, false, frame.get_lower()))) {
          LOG_WARN("process lower bound node failed", K(ret));
        }
      }
      /* check the frame
      * In mysql, ROWS can't coexists with (INTERVAL expr unit).
      * mysql: select c1, sum(c1) over(order by c1 rows interval 5 day preceding) from t1;
      * mysql will raise error: ERROR 3596 (HY000): INTERVAL can only be used with RANGE frames. 
      */
      if (OB_SUCC(ret) && lib::is_mysql_mode() && frame.win_type_ == WINDOW_ROWS) {
        if (frame.get_upper().type_ == BOUND_INTERVAL &&
            !frame.get_upper().is_nmb_literal_) {
          // upper is a (INTERVAL expr unit)
          ret = OB_ERR_WINDOW_ROWS_INTERVAL_USE;
          LOG_WARN("INTERVAL can only be used with RANGE frames.", K(ret));
        } else if (frame.get_lower().type_ == BOUND_INTERVAL &&
                   !frame.get_lower().is_nmb_literal_) {
          ret = OB_ERR_WINDOW_ROWS_INTERVAL_USE;
          LOG_WARN("INTERVAL can only be used with RANGE frames.", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObRawExprResolverImpl::process_bound_node(const ParseNode *node,
                                              const bool is_upper,
                                              Bound &bound)
{
  int ret = OB_SUCCESS;

  bound.type_ = BOUND_UNBOUNDED;
  bound.is_preceding_ = false;

  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (node->type_ != T_WIN_BOUND) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node->type_));
  } else {
    if (1 == node->value_) {
      bound.type_ = BOUND_CURRENT_ROW;
      bound.is_preceding_ = is_upper;
    } else if (2 == node->value_) {
      bound.is_preceding_ = 1 == node->children_[1]->value_;
      // we distinguish unbounded and expr interval by string_value
      ObString expr_str(static_cast<int32_t>(node->children_[0]->str_len_),
          const_cast<char *>(node->children_[0]->str_value_));
      if (ObCharset::case_insensitive_equal(expr_str, "UNBOUNDED")) {
        bound.type_ = BOUND_UNBOUNDED;
      } else {
        bound.type_ = BOUND_INTERVAL;
        if (OB_FAIL(process_interval_node(node->children_[0],
                                          bound.is_nmb_literal_,
                                          bound.interval_expr_,
                                          bound.date_unit_expr_))) {
          LOG_WARN("process interval node failed", K(ret));
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(node->value_));
    }
  }

  return ret;
}

int ObRawExprResolverImpl::process_interval_node(const ParseNode *node,
                                                 bool &is_nmb_literal,
                                                 ObRawExpr *&interval_expr,
                                                 ObRawExpr *&date_unit_expr)
{
  int ret = OB_SUCCESS;

  is_nmb_literal = false;
  interval_expr = NULL;
  date_unit_expr = NULL;

  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (node->type_ != T_WIN_INTERVAL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node->type_));
  } else {
    is_nmb_literal = 1 == node->value_;
    ParseNode *interval_node = node->children_[0];
    if (OB_ISNULL(interval_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(interval_node), K(ret));
    } else if (OB_FAIL(SMART_CALL(recursive_resolve(interval_node, interval_expr)))) {
      SQL_RESV_LOG(WARN, "resolve failed", K(ret));
    } else if (OB_ISNULL(interval_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(interval_expr));
    } else if (OB_FAIL(interval_expr->extract_info())) {
      LOG_WARN("fail to extract info", K(interval_expr), K(ret));
    } else if (!is_nmb_literal) {
      // date type
      ParseNode *date_unit_node = node->children_[1];
      if (OB_FAIL(SMART_CALL(recursive_resolve(date_unit_node, date_unit_expr)))) {
        SQL_RESV_LOG(WARN, "resolve failed", K(ret));
      } else if (OB_ISNULL(date_unit_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(date_unit_expr));
      } else if (OB_FAIL(date_unit_expr->extract_info())) {
        LOG_WARN("fail to extract info", K(interval_expr), K(ret));
      } else if (!date_unit_expr->is_const_raw_expr()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("not const expr error", K(ret), K(date_unit_expr->get_expr_type()));
      } else {/*do nothing*/}
    }
  }

  return ret;
}


/**
 * window function which can't not specified windowing_clause
 */
bool ObRawExprResolverImpl::should_not_contain_window_clause(const ObItemType func_type)
{
  //https://docs.oracle.com/cd/E11882_01/server.112/e41084/functions004.htm#SQLRF06174
  /**
   * Some analytic functions allow the windowing_clause.
   */
  return T_WIN_FUN_ROW_NUMBER == func_type
      || T_WIN_FUN_NTILE == func_type
      || T_WIN_FUN_CUME_DIST == func_type
      || T_WIN_FUN_LAG == func_type
      || T_WIN_FUN_LEAD == func_type
      || T_WIN_FUN_RANK == func_type
      || T_WIN_FUN_DENSE_RANK == func_type
      || T_WIN_FUN_PERCENT_RANK == func_type;
}

/**
 * window function which should contain order_by clause
 */
bool ObRawExprResolverImpl::should_contain_order_by_clause(const ObItemType func_type)
{
  return T_WIN_FUN_ROW_NUMBER == func_type
      || T_WIN_FUN_NTILE == func_type
      || T_WIN_FUN_RANK == func_type
      || T_WIN_FUN_DENSE_RANK == func_type
      || T_WIN_FUN_PERCENT_RANK == func_type
      || T_WIN_FUN_CUME_DIST == func_type
      || T_WIN_FUN_LAG == func_type
      || T_WIN_FUN_LEAD == func_type;

}

//同oracle一样, 当first_value, last_value指定了窗口时必须有order by;
//之前解析时会转换为nth_value
bool ObRawExprResolverImpl::check_frame_and_order_by_valid(const ObItemType func_type,
                                                           const bool has_order_by,
                                                           const bool has_frame)
{
  bool is_valid = true;
  if (T_WIN_FUN_NTH_VALUE == func_type) {
    if (!has_order_by && has_frame) {
      is_valid = false;
    }
  }
  return is_valid;
}

int ObRawExprResolverImpl::check_and_canonicalize_window_expr(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr));
  } else if (!expr->is_win_func_expr()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr->get_expr_type()));
  } else {
    ObWinFunRawExpr *w_expr = static_cast<ObWinFunRawExpr *>(expr);
    WindowType win_type = w_expr->get_window_type();
    bool is_between = w_expr->is_between();
    Bound upper = w_expr->get_upper();
    Bound lower = w_expr->get_lower();
    ObItemType func_type = w_expr->get_func_type();
    const ObIArray<OrderItem> &order_items = w_expr->get_order_items();

    // @note: 做一些语义检查, 这一步没有在parser做是为了保持语法树的简单清晰
    bool parse_error = false;
    if (win_type != WINDOW_MAX) {
      if (BOUND_UNBOUNDED == upper.type_ && !upper.is_preceding_) {
        parse_error = true;
      }
      if (!parse_error && is_between) {
        if (BOUND_UNBOUNDED == lower.type_ && lower.is_preceding_) {
          parse_error = true;
        }
      }
    }
    if (!parse_error && win_type != WINDOW_MAX && !is_between && BOUND_INTERVAL == upper.type_ && !upper.is_preceding_) {
       parse_error = true;
    }

    if (parse_error) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("parse error", K(ret), K(upper), K(lower), K(win_type));
    }

    // 关于是否需要指定order by以及能否指定frame问题, 请见文档:
    // oracle https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/Analytic-Functions.html#GUID-527832F7-63C0-4445-8C16-307FA5084056
    // 目前来看可以选择兼容mysql来做, 这两点的处理上mysql算是超集

    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      if (should_contain_order_by_clause(func_type) && 0 == order_items.count()) {
        ret = OB_ERR_MISS_ORDER_BY_EXPR;
        LOG_WARN("missing ORDER BY expression in the window specification", K(ret));
      } else if (!check_frame_and_order_by_valid(func_type,
                                                 0 != order_items.count(),
                                                 w_expr->has_frame_orig())) {
          ret = OB_ERR_INVALID_WINDOW_FUNC_USE;

          LOG_WARN("frame clause must be specified for certain window function", K(ret), K(func_type));
        }
    }

    if (OB_SUCC(ret) && lib::is_mysql_mode() && w_expr->has_frame_orig() &&
        WINDOW_RANGE == win_type && 0 == order_items.count() &&
        (w_expr->get_upper().type_ == BOUND_INTERVAL || w_expr->get_lower().type_ == BOUND_INTERVAL)) {
      /* if preceding or following has a specific value (not the default unbounded)
      * in mysql:
      * @OK: select c1, sum(c1) over(range between current row and current row) from t1;
      * @OK: select c1, sum(c1) over(range unbounded preceding) from t1;
      * @error: select c1, sum(c1) over(range 1 preceding) from t1; --error 3587
      */
      ret = OB_ERR_WINDOW_RANGE_FRAME_ORDER_TYPE;
      LOG_WARN("missing ORDER BY expression in the window specification", K(ret));
    }

    // reset frame
    if (OB_SUCC(ret) && win_type != WINDOW_MAX && should_not_contain_window_clause(func_type)) {
      win_type = WINDOW_MAX;
      upper = Bound();
      lower = Bound();
    }

    if (OB_SUCC(ret)) {
      // @note: 得到一个between形式的标准窗口
      // 1. 如果未定义窗口, 自定义一个range between (unbounded preceding) and (unbounded following or current row)
      // 2. 将非between的窗口转成between
      if (WINDOW_MAX == win_type) {
        win_type = WINDOW_RANGE;
        is_between = true;
        if (0 == order_items.count()) {
          upper.type_ = BOUND_UNBOUNDED;
          lower.type_ = BOUND_UNBOUNDED;
        } else {
          upper.type_ = BOUND_UNBOUNDED;
          lower.type_ = BOUND_CURRENT_ROW;
        }
      } else {
        if (!is_between) {
          is_between = true;
          // 走到这里, 必然定义了order by
          lower.type_ = BOUND_CURRENT_ROW;
          lower.is_preceding_ = false;
        }
      }
    }
    
    if (OB_SUCC(ret)) {
      //special case
      if (should_not_contain_window_clause(func_type)) {
        lower.type_ = BOUND_UNBOUNDED;
        if (lib::is_oracle_mode() && w_expr->has_frame_orig()) {
          ret = OB_ERR_INVALID_WINDOW_FUNC_USE;

          LOG_WARN("invalid window specification", K(ret), K(upper), K(lower));
        }
      }
    }

    // 修正unbounded的is_preceding_
    // unbounded语义下is_preceding_没有意义, 这里作修正仅仅是为了explain显示用
    if (OB_SUCC(ret)) {
      if (BOUND_UNBOUNDED == upper.type_) {
        upper.is_preceding_ = true;
      }
      if (BOUND_UNBOUNDED == lower.type_) {
        lower.is_preceding_ = false;
      }
    }

    // oracle compatible, 解析阶段能够识别的无效窗口直接抛错
    if (OB_SUCC(ret)) {
      if ((BOUND_CURRENT_ROW == upper.type_ && BOUND_INTERVAL == lower.type_ && lower.is_preceding_)
          || (BOUND_CURRENT_ROW == lower.type_ && BOUND_INTERVAL == upper.type_ && !upper.is_preceding_)
          || (BOUND_INTERVAL == upper.type_ && !upper.is_preceding_ && BOUND_INTERVAL == lower.type_ && lower.is_preceding_)) {
        ret = OB_ERR_INVALID_WINDOW_FUNC_USE;

        LOG_WARN("invalid window specification", K(ret), K(upper), K(lower));
      }
    }

    // oracle compatible, 排序列为向量时, range窗口不能为表达式, 只能是unbounded or current row
    if (OB_SUCC(ret)) {
      if (order_items.count() > 1 &&
          WINDOW_RANGE == win_type &&
          (upper.interval_expr_ != NULL || lower.interval_expr_ != NULL)) {
        ret = OB_ERR_INVALID_WINDOW_FUNC_USE;

        LOG_WARN("invalid window specification", K(ret), K(win_type), K(upper), K(lower));
      } else if (WINDOW_RANGE == win_type) {
        Bound *bs[2] = { &upper, &lower };
        for (int i = 0; i < 2 && OB_SUCC(ret); i++) {
          Bound *bound = bs[i];
          if (bound->interval_expr_ != NULL) {
            for (int j = 0; j < 2 && OB_SUCC(ret); j++) {
              if (bound->is_nmb_literal_) {//普通数值类型范围加减
                ObOpRawExpr *op_expr = NULL;
                if (j == 0 && OB_FAIL(ObRawExprUtils::build_add_expr(ctx_.expr_factory_,
                                                                     order_items.at(0).expr_,
                                                                     bound->interval_expr_,
                                                                     op_expr))) {
                  LOG_WARN("failed to build add expr", K(ret));
                } else if (j == 1 && OB_FAIL(ObRawExprUtils::build_minus_expr(ctx_.expr_factory_,
                                                                              order_items.at(0).expr_,
                                                                              bound->interval_expr_,
                                                                              op_expr))) {
                  LOG_WARN("failed to build minus expr", K(ret));
                } else if (OB_ISNULL(op_expr)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("NULL ptr", K(ret), K(op_expr));
                } else {
                  bound->exprs_[j] = op_expr;
                }
              } else if (!bound->is_nmb_literal_) {//date类型范围加减
                ObSysFunRawExpr *sys_expr = NULL;
                if (j == 0 && OB_FAIL(ObRawExprUtils::build_date_add_expr(ctx_.expr_factory_,
                                                                          order_items.at(0).expr_,
                                                                          bound->interval_expr_,
                                                                          bound->date_unit_expr_,
                                                                          sys_expr))) {
                  LOG_WARN("failed to build add expr", K(ret));
                } else if (j == 1 && OB_FAIL(ObRawExprUtils::build_date_sub_expr(ctx_.expr_factory_,
                                                                                  order_items.at(0).expr_,
                                                                                  bound->interval_expr_,
                                                                                  bound->date_unit_expr_,
                                                                                  sys_expr))) {
                  LOG_WARN("failed to build minus expr", K(ret));
                } else if (OB_ISNULL(sys_expr)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("NULL ptr", K(ret), K(sys_expr));
                } else {
                  bound->exprs_[j] = sys_expr;
                }
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      w_expr->set_window_type(win_type);
      w_expr->set_is_between(is_between);
      w_expr->set_upper(upper);
      w_expr->set_lower(lower);
    }
  }

  return ret;
}

//查询改写 ratio_to_report(c1) over (partition by c2) ==>
// c1 / case sum(c1) over (partition by c2) when 0 then null else sum(c1) over (partition by c2) end
int ObRawExprResolverImpl::transform_ratio_afun_to_arg_div_sum(const ParseNode *ratio_fun_node,
                                                               ParseNode *&div_node) //out, 最终的 c1 / simple_case
{
  int ret = OB_SUCCESS;
  const ParseNode *func_node = NULL;
  ParseNode *when_list_node = NULL;
  ParseNode *merge_when_list_node = NULL;
  ParseNode *when_node = NULL;
  ParseNode *then_node = NULL;
  ParseNode *divisor_node = NULL;
  div_node = NULL;
  if (OB_ISNULL(ratio_fun_node) || 2 != ratio_fun_node->num_child_
      || OB_ISNULL(ratio_fun_node->children_[0]) || OB_ISNULL(ratio_fun_node->children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ratio_fun_node));
  } else if (OB_NOT_NULL(ratio_fun_node->children_[0]->children_[0])
            || OB_ISNULL(ratio_fun_node->children_[0]->children_[1])
            || OB_NOT_NULL(ratio_fun_node->children_[1]->children_[1])
            || OB_NOT_NULL(ratio_fun_node->children_[1]->children_[2]) ) {
    ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
    LOG_WARN("order by/frame clause not allowed for RATIO_TO_REPORT", K(ret));
          //"the node", SJ(ObParserResultPrintWrapper(*ratio_fun_node)));
  } else {
    func_node = ratio_fun_node->children_[0];
    const_cast<ParseNode*>(func_node)->type_ = T_FUN_SUM;
    //1, 构造when 0 then null的when_list
    if (OB_FAIL(ObRawExprUtils::new_parse_node(when_node, ctx_.expr_factory_, T_INT, 0)) ||
        OB_FAIL(ObRawExprUtils::new_parse_node(then_node, ctx_.expr_factory_, T_NULL, 0))) {
      LOG_WARN("allocate when/then node failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::new_parse_node(when_list_node, ctx_.expr_factory_,
                                                      T_WHEN, 2))) {
      LOG_WARN("allocate when list failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::new_parse_node(merge_when_list_node, ctx_.expr_factory_,
                                                      T_WHEN_LIST, 1))) {
      LOG_WARN("allocate when list failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::new_parse_node(divisor_node, ctx_.expr_factory_,
                                                      T_CASE, 3))) {
      LOG_WARN("allocate divisor failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::new_parse_node(div_node, ctx_.expr_factory_,
                                                      T_OP_DIV, 2))) {
      LOG_WARN("allocate result node failed", K(ret));
    } else {
      when_node->value_ = 0;
      when_list_node->children_[0] = when_node;
      when_list_node->children_[1] = then_node;
      merge_when_list_node->children_[0] = when_list_node;
      divisor_node->children_[0] = const_cast<ParseNode*>(ratio_fun_node);
      divisor_node->children_[1] = merge_when_list_node;
      divisor_node->children_[2] = const_cast<ParseNode*>(ratio_fun_node);

      div_node->children_[0] = func_node->children_[1];
      div_node->children_[1] = divisor_node;
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_dll_udf_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;

  const share::schema::ObUDF *udf_info = nullptr;
  bool exist = false;
  ObString udf_name;
  ObCollationType cs_type;
  if (OB_ISNULL(ctx_.session_info_) || OB_ISNULL(ctx_.schema_checker_)) {
    //PL resolver don't have schema checker and session info
    ret = OB_ERR_FUNCTION_UNKNOWN;
  } else if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node));
  } else if (OB_UNLIKELY(1 > node->num_child_) || OB_ISNULL(node->children_) || OB_ISNULL(node->children_[0])) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid node children for fun_sys node", K(ret), K(node->num_child_), "node", SJ(ObParserResultPrintWrapper(*node)));
  } else if (OB_FAIL(ctx_.session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("failed to get collation", K(ret));
  } else {
    ObString name(node->children_[0]->str_len_, node->children_[0]->str_value_);
    if (OB_FAIL(ob_write_string(ctx_.expr_factory_.get_allocator(), name, udf_name))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Malloc function name failed", K(ret));
    } else if (FALSE_IT(IGNORE_RETURN ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, udf_name))) {
    } else if (OB_FAIL(ctx_.schema_checker_->get_udf_info(ctx_.session_info_->get_effective_tenant_id(),
                                                          udf_name,
                                                          udf_info,
                                                          exist))) {
      LOG_WARN("failed to resolve udf", K(ret));
    } else if (!exist) {
      //we can not find this function in udf
      ret = OB_ERR_FUNCTION_UNKNOWN;
      //do not throw this error to user, just let it go.
      //the pl function will deal with it.
    } else if (OB_ISNULL(udf_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the udf info is null", K(ret));
    } else if (!(udf_info->is_normal_udf() || udf_info->is_agg_udf())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the udf schema is error", K(ret), K(udf_info->get_type()));
    } else if (udf_info->is_normal_udf()) {
      ObSysFunRawExpr *func_expr = NULL;
      ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, udf_name);
      if (OB_FAIL(process_normal_udf_node(node, udf_name, *udf_info, func_expr))) {
        LOG_WARN("failed to process normal user define function", K(ret));
      } else {
        expr = func_expr;
      }
    } else if (udf_info->is_agg_udf()) {
      ObAggFunRawExpr *func_expr = NULL;
      ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, udf_name);
      if (OB_FAIL(process_agg_udf_node(node, *udf_info, func_expr))) {
        LOG_WARN("failed to process agg user define function", K(ret));
      } else {
        expr = func_expr;
      }
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_normal_udf_node(const ParseNode *node,
                                                   const ObString &udf_name,
                                                   const share::schema::ObUDF &udf_info,
                                                   ObSysFunRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObNormalDllUdfRawExpr *func_expr = nullptr;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("normal udf is null", K(ret), K(node));
  } else if (node->num_child_ < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("normal udf is null", K(ret), K(node->num_child_));
  } else if (node->num_child_ > 1
             &&(OB_ISNULL(node->children_)
                || OB_ISNULL(node->children_[1])
                || OB_UNLIKELY(T_EXPR_LIST != node->children_[1]->type_))) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid node children", K(ret), K(node->children_));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_NORMAL_UDF, func_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(func_expr->set_udf_meta(udf_info))) {
    LOG_WARN("set udf info failed", K(ret));
  } else {
    func_expr->set_func_name(udf_name);
    ObSEArray<ObRawExpr*, 16> param_exprs;
    if (OB_FAIL(resolve_udf_param_expr(node, func_expr->get_param_exprs()))) {
      set_udf_param_syntax_err(true);
      LOG_WARN("failed to resolve udf param exprs", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObSysFunRawExpr *tmp_expr = func_expr;
    if (OB_FAIL(ObRawExprUtils::function_alias(ctx_.expr_factory_, tmp_expr))) {
      LOG_WARN("failed to do funcion alias", K(ret), K(func_expr));
    } else {
      expr = tmp_expr;
    }
  }

  return ret;
}

int ObRawExprResolverImpl::process_agg_udf_node(const ParseNode *node,
                                                const share::schema::ObUDF &udf_info,
                                                ObAggFunRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObAggFunRawExpr *agg_expr = nullptr;
  bool need_add_flag = !ctx_.parents_expr_info_.has_member(IS_AGG);
  if (need_add_flag && OB_FAIL(ctx_.parents_expr_info_.add_member(IS_AGG))) {
    LOG_WARN("failed to add member", K(ret));
  } else if (OB_ISNULL(ctx_.aggr_exprs_) || OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr exprs or node is null", K(ret), K(node));
  } else if (node->num_child_ < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr exprs or node is null", K(ret), K(node->num_child_));
  } else if (node->num_child_ > 1
             && (OB_ISNULL(node->children_)
                 || OB_ISNULL(node->children_[1])
                 || OB_UNLIKELY(T_EXPR_LIST != node->children_[1]->type_))) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid node children", K(ret), K(node->children_));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(T_FUN_AGG_UDF, agg_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(ctx_.aggr_exprs_->push_back(agg_expr))) {
    LOG_WARN("store aggr expr failed", K(ret));
  } else if (OB_FAIL(agg_expr->set_udf_meta(udf_info))) {
    LOG_WARN("set udf info failed", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 16> param_exprs;
    if (OB_FAIL(resolve_udf_param_expr(node, agg_expr->get_real_param_exprs_for_update()))) {
      LOG_WARN("failed to resolve udf param exprs", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // add invalid table bit index, avoid aggregate function expressions are used as filters
    if (OB_FAIL(agg_expr->get_relation_ids().add_member(0))) {
      LOG_WARN("failed to add member", K(ret));
    } else if (need_add_flag && (ctx_.parents_expr_info_.del_member(IS_AGG))) {
        LOG_WARN("failed to del member", K(ret));
    } else {
      expr = agg_expr;
    }
  }
  return ret;
}

int ObRawExprResolverImpl::resolve_udf_param_expr(const ParseNode *node,
                                                  common::ObIArray<ObRawExpr*> &param_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *param_expr = nullptr;
  // if the func do not have any param, without_param will be set to true.
  const bool without_param = node->num_child_ == 1;
  int32_t num = without_param ? 0 : node->children_[1]->num_child_;
  const ParseNode *child_node = without_param ? nullptr : node->children_[1];
  for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
    bool has_alias = false;
    const ParseNode *expr_parse_node = child_node->children_[i];
    const ParseNode *expr_alias_node = nullptr;
    if (OB_ISNULL(expr_parse_node)) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("parser error", K(ret));
    } else if (expr_parse_node->type_ == T_EXPR_WITH_ALIAS) {
      if (expr_parse_node->num_child_ != 1
          || OB_ISNULL(expr_parse_node->children_[0])
          || expr_parse_node->children_[0]->type_ != T_ALIAS
          || expr_parse_node->children_[0]->num_child_ != 2
          || OB_ISNULL(expr_parse_node->children_[0]->children_[0])
          || OB_ISNULL(expr_parse_node->children_[0]->children_[1])) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("parser error", K(ret));
      } else {
        expr_alias_node = expr_parse_node->children_[0]->children_[1];
        expr_parse_node = expr_parse_node->children_[0]->children_[0];
        has_alias = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(SMART_CALL(recursive_resolve(expr_parse_node, param_expr)))) {
      LOG_WARN("fail to recursive resolve", K(ret), K(expr_parse_node));
    } else if (OB_FAIL(param_exprs.push_back(param_expr))) {
      LOG_WARN("fail to add param expr", K(ret), K(param_expr));
    } else if (has_alias) {
      ObString alias_name(expr_alias_node->str_len_, expr_alias_node->str_value_);
      param_expr->set_alias_column_name(alias_name);
    }
    if (OB_SUCC(ret)) {
      ObString expr_name(expr_parse_node->str_len_, expr_parse_node->str_value_);
      if(OB_FAIL(param_expr->set_expr_name(expr_name))) {
        LOG_WARN("set expr name failed", K(ret));
      }
    }
  } //end for
  return ret;
}

int ObRawExprResolverImpl::process_dml_event_node(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *f_expr = NULL;
  ObConstRawExpr *c_expr = NULL;
  ObRawExpr *col_expr = NULL;
  OV (OB_NOT_NULL(node), OB_INVALID_ARGUMENT);
  OV (OB_LIKELY(1 == node->num_child_));
  if (OB_SUCC(ret)) {
    OZ (ctx_.expr_factory_.create_raw_expr(node->type_, f_expr));
    OZ (ctx_.expr_factory_.create_raw_expr(T_INT, c_expr));
    CK (OB_NOT_NULL(f_expr) && OB_NOT_NULL(c_expr));
    if (OB_SUCC(ret)) {
      ObObjParam val;
      if (T_DML_EVENT_UPDATING == node->value_) {
        val.set_int(static_cast<int64_t>(ObDmlEventType::DE_UPDATING));
        if (NULL != node->children_[0]) {
          OZ (SMART_CALL(recursive_resolve(node->children_[0], col_expr)));
        }
      } else {
        T_DML_EVENT_INSERTING == node->value_ 
                                 ? val.set_int(static_cast<int64_t>(ObDmlEventType::DE_INSERTING))
                                 : val.set_int(static_cast<int64_t>(ObDmlEventType::DE_DELETING));
      }
      if (OB_SUCC(ret)) {
        val.set_param_meta();
        c_expr->set_param(val);
        c_expr->set_value(val);
      }
      OZ (f_expr->add_param_expr(c_expr));
      if (NULL != col_expr) {
        OZ (f_expr->add_param_expr(col_expr));
      }
      OX (expr = f_expr);
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_internal_sys_function_node(const ParseNode *node, ObRawExpr *&expr, ObItemType node_type)
{
  int ret = OB_SUCCESS;
  CK(2 == node->num_child_);
  CK(OB_NOT_NULL(node->children_));
  CK(OB_NOT_NULL(node->children_[0]) && OB_NOT_NULL(node->children_[1]));
  ObSysFunRawExpr *func_expr = NULL;
  ObString func_name;
  if (OB_ISNULL(node) || OB_ISNULL(ctx_.session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(node), KP(ctx_.session_info_));
  } else if (OB_FAIL(ctx_.expr_factory_.create_raw_expr(node_type, func_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(func_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("func_expr is null");
  } else {
    ObString name(node->children_[0]->str_len_, node->children_[0]->str_value_);
    if (OB_FAIL(ret)) {
      /*^-^*/
    } else if (OB_FAIL(ob_write_string(ctx_.expr_factory_.get_allocator(), name, func_name))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Malloc function name failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    func_expr->set_func_name(func_name);
    if (OB_ISNULL(node->children_) || OB_ISNULL(node->children_[1])
        || OB_UNLIKELY(T_EXPR_LIST != node->children_[1]->type_)) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("invalid node children", K(ret), K(node->children_));
    } else {
      ObRawExpr *para_expr = NULL;
      int32_t num = node->children_[1]->num_child_;
      for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
        if (OB_ISNULL(node->children_[1]->children_[i])) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("invalid parse tree", K(ret));
        } else if (OB_FAIL(SMART_CALL(recursive_resolve(node->children_[1]->children_[i], para_expr)))) {
          LOG_WARN("fail to recursive resolve", K(ret), K(node->children_[1]->children_[i]));
        } else if (OB_FAIL(func_expr->add_param_expr(para_expr))) {
          LOG_WARN("fail to add param expr", K(ret), K(para_expr));
        }
      } //end for
      OX (expr = func_expr);
    }
  }
  return ret;
}

int ObRawExprResolverImpl::check_internal_function(const ObString &name)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx_.session_info_));
  bool exist = false;
  bool is_internal = false;
  if (OB_FAIL(ret)) {
  } else if (ctx_.session_info_->is_inner()) {
    // ignore
  } else if (FALSE_IT(ObExprOperatorFactory::get_internal_info_by_name(name, exist, is_internal))) {
  } else if (exist && is_internal) {
    if (lib::is_mysql_mode()) {
      ret = OB_ERR_SP_DOES_NOT_EXIST;
      LOG_USER_ERROR(OB_ERR_SP_DOES_NOT_EXIST, "FUNCTION",
                      ctx_.session_info_->get_database_name().length(),
                      ctx_.session_info_->get_database_name().ptr(),
                      name.length(), name.ptr());
    } else {
      ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
      LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS,
                      name.length(),
                      name.ptr());
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_odbc_escape_sequences(const ParseNode *node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  const ParseNode *action_node = NULL;
  const ParseNode *expr_node = NULL;
  expr = NULL;
  if (OB_ISNULL(node) || OB_UNLIKELY(node->num_child_ != 2) ||
      OB_ISNULL(action_node = node->children_[0]) ||
      OB_ISNULL(expr_node = node->children_[1]) ||
      OB_UNLIKELY(T_IDENT != action_node->type_ ||
                  action_node->str_value_ == NULL ||
                  action_node->str_len_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(node), K(action_node), K(expr_node));
  } else {
    ObString action_str = ObString(action_node->str_len_, action_node->str_value_);
    if (0 == action_str.case_compare("d") ||
        0 == action_str.case_compare("t") ||
        0 == action_str.case_compare("ts")) {//Date/Time/TimeStamp Type, Try to convert
      ObItemType dst_time_type = (0 == action_str.case_compare("d") ? T_DATE :
                                  (0 == action_str.case_compare("t") ? T_TIME : T_TIMESTAMP));
      if (OB_FAIL(process_odbc_time_literals(dst_time_type, expr_node, expr))) {
        LOG_WARN("failed to process odbc time literals", K(ret));
      } else {/*do nothing*/}
    //fn scalar function action and other invalid action, just ignore action
    } else if (OB_FAIL(SMART_CALL(recursive_resolve(expr_node, expr)))) {
      LOG_WARN("failed to recursive resolve", K(ret));
    }
  }
  return ret;
}

int ObRawExprResolverImpl::process_odbc_time_literals(const ObItemType dst_time_type,
                                                      const ParseNode *expr_node,
                                                      ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  expr = NULL;
  bool is_valid_time_str = false;
  if (OB_ISNULL(expr_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr_node));
  } else if (T_VARCHAR != expr_node->type_ && T_QUESTIONMARK != expr_node->type_) {
    //do nothing
  } else {
    ObString time_str;
    if (T_VARCHAR == expr_node->type_) {
      is_valid_time_str = true;
      time_str.assign_ptr(expr_node->str_value_, static_cast<int32_t>(expr_node->str_len_));
    } else if (OB_ISNULL(ctx_.param_list_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param_list_ is null", K(ret), K(ctx_.param_list_));
    } else if (ctx_.param_list_->count() == 0) {
      //prepare phase of ps protocol, do nothing
    } else if (OB_UNLIKELY(expr_node->value_ < 0 || expr_node->value_ >= ctx_.param_list_->count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index is out of range", K(expr_node->value_), K(ctx_.param_list_->count()));
    } else if (!ctx_.param_list_->at(expr_node->value_).is_varchar()) {
      //do nothing
    } else {
      is_valid_time_str = true;
      time_str = ctx_.param_list_->at(expr_node->value_).get_varchar();
    }
    if (OB_SUCC(ret) && is_valid_time_str) {
      ParseNode tmp_node;
      tmp_node.type_ = dst_time_type;
      tmp_node.num_child_ = 0;
      tmp_node.str_len_ = time_str.length();
      tmp_node.str_value_ = time_str.ptr();
      tmp_node.is_date_unit_ = false;
      if (OB_FAIL(process_datatype_or_questionmark(tmp_node, expr))) {
        if (ret == OB_ERR_WRONG_VALUE) {//invalid time str, go back default way like Mysql.
          ret = OB_SUCCESS;
          is_valid_time_str = false;
          LOG_TRACE("is invalid time string", K(time_str));
        } else {
          LOG_WARN("fail to process datatype or questionmark", K(ret));
        }
      } else if (T_QUESTIONMARK == expr_node->type_) {//questionmark need add const constaint.
        ObPCConstParamInfo const_param_info;
        if (OB_ISNULL(ctx_.query_ctx_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("query context have not been initialized.", K(ret));
        } else if (OB_FAIL(const_param_info.const_idx_.push_back(expr_node->value_))) {
          LOG_WARN("failed to push back param idx", K(ret));
        } else if (OB_FAIL(const_param_info.const_params_.push_back(ctx_.param_list_->at(expr_node->value_)))) {
          LOG_WARN("failed to push back value", K(ret));
        } else if (OB_FAIL(ctx_.query_ctx_->all_plan_const_param_constraints_.push_back(const_param_info))) {
          LOG_WARN("failed to push back const param info", K(ret));
        } else {/* do nothing */}
      }
    }
  }
  if (OB_SUCC(ret) && !is_valid_time_str) {
    if (OB_FAIL(SMART_CALL(recursive_resolve(expr_node, expr)))) {
      LOG_WARN("failed to recursive resolve", K(ret));
    }
  }
  return ret;
}


} //namespace sql
} //namespace oceanbase
