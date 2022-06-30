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
#include "share/ob_define.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_autoincrement_param.h"
#include "share/schema/ob_schema_mgr.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_canonicalizer_impl.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/expr/ob_raw_expr_info_extractor.h"
#include "sql/resolver/expr/ob_raw_expr_info_extractor.h"
#include "sql/resolver/expr/ob_raw_expr_ctxcat_analyzer.h"
#include "sql/resolver/dml/ob_view_table_resolver.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/ob_sql_context.h"
#include "sql/parser/ob_parser.h"
#include "sql/parser/parse_node.h"
#include "sql/parser/parse_malloc.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/expr/ob_raw_expr_canonicalizer_impl.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/dml/ob_default_value_utils.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_get_compat_mode.h"
#include "sql/ob_sql_utils.h"
#include "lib/oblog/ob_trace_log.h"
#include "sql/optimizer/ob_opt_est_utils.h"
#include "sql/resolver/expr/ob_expr.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "sql/parser/ob_item_type_str.h"
#include "sql/ob_select_stmt_printer.h"
#include "lib/utility/ob_fast_convert.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/expr/ob_expr_version.h"
#include "sql/ob_sql_mock_schema_utils.h"
#include "common/ob_smart_call.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;

namespace sql {
ObDMLResolver::ObDMLResolver(ObResolverParams& params)
    : ObStmtResolver(params),
      current_scope_(T_NONE_SCOPE),
      current_level_(0),
      field_list_first_(false),
      parent_namespace_resolver_(NULL),
      column_namespace_checker_(params),
      sequence_namespace_checker_(params),
      gen_col_exprs_(),
      from_items_order_(),
      has_ansi_join_(false),
      has_oracle_join_(false),
      with_clause_without_record_(false)
{}

ObDMLResolver::~ObDMLResolver()
{}

bool ObDMLResolver::need_all_columns(const ObTableSchema& table_schema, int64_t binlog_row_image)
{
  return (table_schema.is_no_pk_table() || table_schema.get_foreign_key_infos().count() > 0 ||
          table_schema.has_check_constraint() || binlog_row_image == ObBinlogRowImage::FULL);
}

ObDMLStmt* ObDMLResolver::get_stmt()
{
  return static_cast<ObDMLStmt*>(stmt_);
}

int ObDMLResolver::check_need_use_sys_tenant(bool& use_sys_tenant) const
{
  use_sys_tenant = false;
  return OB_SUCCESS;
}

int ObDMLResolver::check_in_sysview(bool& in_sysview) const
{
  in_sysview = false;
  return OB_SUCCESS;
}

int ObDMLResolver::alloc_joined_table_item(JoinedTable*& joined_table)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(ptr = allocator_->alloc(sizeof(JoinedTable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "alloc memory for JoinedTable failed", "size", sizeof(JoinedTable));
  } else {
    joined_table = new (ptr) JoinedTable();
  }
  return ret;
}

int ObDMLResolver::create_joined_table_item(
    const ObJoinType joined_type, const TableItem* left_table, const TableItem* right_table, JoinedTable*& joined_table)
{
  int ret = OB_SUCCESS;
  OZ(alloc_joined_table_item(joined_table));
  CK(OB_NOT_NULL(joined_table));
  if (OB_SUCC(ret)) {
    // use inner join if dependency is empty
    joined_table->table_id_ = generate_table_id();
    joined_table->type_ = TableItem::JOINED_TABLE;
    joined_table->joined_type_ = joined_type;
    joined_table->left_table_ = const_cast<TableItem*>(left_table);
    joined_table->right_table_ = const_cast<TableItem*>(right_table);

    // push up single table ids (left deep tree)
    // left table ids
    CK(OB_NOT_NULL(joined_table->left_table_));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (joined_table->left_table_->is_joined_table()) {
      JoinedTable* cur_joined = static_cast<JoinedTable*>(joined_table->left_table_);
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_joined->single_table_ids_.count(); i++) {
        OZ((joined_table->single_table_ids_.push_back)(cur_joined->single_table_ids_.at(i)));
      }
    } else {
      OZ((joined_table->single_table_ids_.push_back)(joined_table->left_table_->table_id_));
    }

    // right table id
    CK(OB_NOT_NULL(joined_table->right_table_));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (joined_table->right_table_->is_joined_table()) {
      JoinedTable* cur_joined = static_cast<JoinedTable*>(joined_table->right_table_);
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_joined->single_table_ids_.count(); i++) {
        OZ((joined_table->single_table_ids_.push_back)(cur_joined->single_table_ids_.at(i)));
      }
    } else {
      OZ((joined_table->single_table_ids_.push_back)(joined_table->right_table_->table_id_));
    }
  }
  return ret;
}

int ObDMLResolver::resolve_sql_expr(
    const ParseNode& node, ObRawExpr*& expr, ObArray<ObQualifiedName>* output_columns /* = NULL*/)
{
  int ret = OB_SUCCESS;
  bool tmp_field_list_first = field_list_first_;
  field_list_first_ = false;
  ObArray<ObQualifiedName> columns;
  bool need_analyze_aggr = false;
  if (output_columns == NULL) {
    output_columns = &columns;
    need_analyze_aggr = true;
  }
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObOpRawExpr*> op_exprs;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObCharsetType character_set_connection = CHARSET_INVALID;
  CK(OB_NOT_NULL(params_.expr_factory_), OB_NOT_NULL(stmt_), OB_NOT_NULL(get_stmt()), OB_NOT_NULL(session_info_));
  OC((params_.session_info_->get_collation_connection)(collation_connection));
  OC((params_.session_info_->get_character_set_connection)(character_set_connection));

  if (OB_SUCC(ret)) {
    ObExprResolveContext ctx(*params_.expr_factory_, session_info_->get_timezone_info(), OB_NAME_CASE_INVALID);
    ctx.dest_collation_ = collation_connection;
    ctx.connection_charset_ = character_set_connection;
    ctx.param_list_ = params_.param_list_;
    ctx.is_extract_param_type_ = !params_.is_prepare_protocol_;  // when prepare do not extract
    ctx.external_param_info_ = &params_.external_param_info_;
    ctx.current_scope_ = current_scope_;
    ctx.stmt_ = static_cast<ObStmt*>(get_stmt());
    ctx.schema_checker_ = schema_checker_;
    ctx.session_info_ = session_info_;
    ctx.prepare_param_count_ = params_.prepare_param_count_;
    ctx.query_ctx_ = params_.query_ctx_;
    ctx.is_for_pivot_ = !need_analyze_aggr;
    ctx.is_for_dynamic_sql_ = params_.is_dynamic_sql_;
    ctx.is_for_dbms_sql_ = params_.is_dbms_sql_;
    ObRawExprResolverImpl expr_resolver(ctx);
    ObIArray<ObUserVarIdentRawExpr*>& user_var_exprs = get_stmt()->get_user_vars();
    OC((session_info_->get_name_case_mode)(ctx.case_mode_));
    OC((expr_resolver.resolve)(
        &node, expr, *output_columns, sys_vars, sub_query_info, aggr_exprs, win_exprs, op_exprs, user_var_exprs));
    if (OB_SUCC(ret)) {
      params_.prepare_param_count_ = ctx.prepare_param_count_;  // prepare param count
    }
    OC((resolve_subquery_info)(sub_query_info));
    if (OB_SUCC(ret)) {
      // are there any user variable assignments?
      get_stmt()->set_contains_assignment(expr_resolver.is_contains_assignment());
    }
    // resolve column(s)
    if (OB_SUCC(ret) && output_columns->count() > 0) {
      if (tmp_field_list_first && stmt_->is_select_stmt()) {
        ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt_);
        if (OB_FAIL(resolve_columns_field_list_first(expr, *output_columns, sel_stmt))) {
          LOG_WARN("resolve columns field list first failed", K(ret));
        }
      } else if (OB_FAIL(resolve_columns(expr, *output_columns))) {
        LOG_WARN("resolve columns failed", K(ret));
      }
    }

    const common::ObIArray<uint64_t>& nextval_sequence_ids = get_stmt()->get_nextval_sequence_ids();
    const common::ObIArray<uint64_t>& currval_sequence_ids = get_stmt()->get_currval_sequence_ids();
    if (OB_SUCC(ret)) {
      if (nextval_sequence_ids.count() > 0) {
        if (OB_FAIL(add_object_version_to_dependency(DEPENDENCY_SEQUENCE, SEQUENCE_SCHEMA, nextval_sequence_ids))) {
          LOG_WARN("add nextval sequence versions failed", K(ret));
        } else {
          // do nothing
        }
      }
      if (currval_sequence_ids.count() > 0) {
        if (OB_FAIL(add_object_version_to_dependency(DEPENDENCY_SEQUENCE, SEQUENCE_SCHEMA, currval_sequence_ids))) {
          LOG_WARN("add currval sequence version failed", K(ret));
        } else {
          // do nothing
        }
      }
    }

    if (OB_SUCC(ret) && aggr_exprs.count() > 0) {
      if (OB_FAIL(resolve_aggr_exprs(expr, aggr_exprs, need_analyze_aggr))) {
        LOG_WARN("resolve aggr exprs failed", K(ret));
      }
    }
    // resolve sys var(s)
    if (OB_SUCC(ret) && sys_vars.count() > 0) {
      if (OB_FAIL(resolve_sys_vars(sys_vars))) {
        LOG_WARN("resolve system variables failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && win_exprs.count() > 0) {
      if (OB_FAIL(resolve_win_func_exprs(expr, win_exprs))) {
        LOG_WARN("resolve aggr exprs failed", K(ret));
      }
    }

    // process oracle compatible implimental cast
    LOG_DEBUG("is oracle mode", K(share::is_oracle_mode()), K(lib::is_oracle_mode()), K(op_exprs));
    if (OB_SUCC(ret) && op_exprs.count() > 0) {
      if (OB_FAIL(ObRawExprUtils::resolve_op_exprs_for_oracle_implicit_cast(
              ctx.expr_factory_, ctx.session_info_, op_exprs))) {
        LOG_WARN("implicit cast failed", K(ret));
      }
    }

    // resolve special expression, like functions, e.g abs, concat
    // acutally not so special, hmm...
    if (OB_SUCC(ret)) {
      // update flag info
      if (OB_FAIL(expr->extract_info())) {
        LOG_WARN("failed to extract info", K(ret), K(*expr));
      } else if (OB_FAIL(resolve_outer_join_symbol(current_scope_, expr))) {
        LOG_WARN("Failed to check and remove outer join symbol", K(ret));
      } else if (OB_FAIL(resolve_special_expr(expr, current_scope_))) {
        LOG_WARN("resolve special expression failed", K(ret));
      }
    }
    // LOG_DEBUG("resolve_sql_expr:5", "usec", ObSQLUtils::get_usec());
    // refresh info again
    if (OB_SUCC(ret)) {
      if (OB_FAIL(expr->extract_info())) {
        LOG_WARN("failed to extract info", K(ret));
      } else if (OB_FAIL(check_expr_param(*expr))) {
        LOG_WARN("check expr param failed", K(ret));
      } else if (ObRawExprUtils::check_composite_cast(expr, *schema_checker_)) {
        LOG_WARN("check composite cast failed", K(ret));
      }
    }
  }

  return ret;
}

// When resolve order by items, search in select items first.
////create table t1(c1 int,c2 int);
////create table t2(c1 int, c2 int);
////select a.c1, b.c2 from t1 a, t2 b order by (c1+c2); is legal,
// c1 and c2 in order by correspond to a.c1 and b.c2 in select items.
int ObDMLResolver::resolve_columns_field_list_first(
    ObRawExpr*& expr, ObArray<ObQualifiedName>& columns, ObSelectStmt* sel_stmt)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> real_exprs;
  if (OB_ISNULL(sel_stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr or select stmt is null", K(expr), K(sel_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    bool found = false;
    if (columns.at(i).tbl_name_.empty()) {
      for (int64_t j = 0; OB_SUCC(ret) && j < sel_stmt->get_select_item_size(); j++) {
        ObRawExpr* select_item_expr = NULL;
        if (OB_ISNULL(select_item_expr = sel_stmt->get_select_item(j).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("select item expr is null", K(ret));
        } else if (select_item_expr->is_column_ref_expr()) {
          ObColumnRefRawExpr* column_ref_expr = static_cast<ObColumnRefRawExpr*>(select_item_expr);
          if (ObCharset::case_insensitive_equal(sel_stmt->get_select_item(j).is_real_alias_
                                                    ? sel_stmt->get_select_item(j).alias_name_
                                                    : column_ref_expr->get_column_name(),
                  columns.at(i).col_name_)) {
            if (found) {
              ObString scope_name = ObString::make_string(get_scope_name(current_scope_));
              ret = OB_NON_UNIQ_ERROR;
              LOG_USER_ERROR(OB_NON_UNIQ_ERROR,
                  columns.at(i).col_name_.length(),
                  columns.at(i).col_name_.ptr(),
                  scope_name.length(),
                  scope_name.ptr());
            } else {
              found = true;
              if (OB_FAIL(real_exprs.push_back(column_ref_expr))) {
                LOG_WARN("push back failed", K(ret));
              } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, columns.at(i).ref_expr_, column_ref_expr))) {
                LOG_WARN("replace column ref expr failed", K(ret));
              } else { /* do nothing */
              }
            }
          }
        } else if (is_oracle_mode()) {
          const SelectItem& select_item = sel_stmt->get_select_item(j);
          if (ObCharset::case_insensitive_equal(
                  select_item.is_real_alias_ ? select_item.alias_name_ : select_item.expr_name_,
                  columns.at(i).col_name_)) {
            if (found) {
              ObString scope_name = ObString::make_string(get_scope_name(T_FIELD_LIST_SCOPE));
              ret = OB_NON_UNIQ_ERROR;
              LOG_USER_ERROR(OB_NON_UNIQ_ERROR,
                  columns.at(i).col_name_.length(),
                  columns.at(i).col_name_.ptr(),
                  scope_name.length(),
                  scope_name.ptr());
            } else {
              found = true;
              if (OB_FAIL(real_exprs.push_back(select_item_expr))) {
                LOG_WARN("push back failed", K(ret));
              } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, columns.at(i).ref_expr_, select_item_expr))) {
                LOG_WARN("replace column ref expr failed", K(ret));
              } else { /* do nothing */
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && false == found) {
      ObQualifiedName& q_name = columns.at(i);
      ObRawExpr* real_ref_expr = NULL;
      if (OB_FAIL(resolve_qualified_identifier(q_name, columns, real_exprs, real_ref_expr))) {
        LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve column ref expr failed", K(ret), K(q_name));
        report_user_error_msg(ret, expr, q_name);
      } else if (OB_FAIL(real_exprs.push_back(real_ref_expr))) {
        LOG_WARN("push back failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, real_ref_expr))) {
        LOG_WARN("replace column ref expr failed", K(ret));
      } else { /*do nothing*/
      }
    }
  }

  return ret;
}

int ObDMLResolver::resolve_into_variables(
    const ParseNode* node, ObIArray<ObString>& user_vars, ObIArray<ObRawExpr*>& pl_vars)
{
  int ret = OB_SUCCESS;

  CK(OB_NOT_NULL(node),
      OB_LIKELY(T_INTO_VARIABLES == node->type_),
      OB_NOT_NULL(node->children_[0]),
      OB_NOT_NULL(get_stmt()));

  if (OB_SUCC(ret)) {
    current_scope_ = T_INTO_SCOPE;
    const ParseNode* into_node = node->children_[0];
    ObRawExpr* expr = NULL;
    ParseNode* ch_node = NULL;
    ObBitSet<> user_var_idx;
    for (int64_t i = 0; OB_SUCC(ret) && i < into_node->num_child_; ++i) {
      ch_node = into_node->children_[i];
      expr = NULL;
      CK(OB_NOT_NULL(ch_node));
      CK(OB_LIKELY(T_USER_VARIABLE_IDENTIFIER == ch_node->type_ /*MySQL Mode for user_var*/
                   || T_IDENT == ch_node->type_                 /*MySQL Mode for pl_var*/
                   || T_OBJ_ACCESS_REF == ch_node->type_        /*Oracle Mode for pl_var*/
                   || T_QUESTIONMARK == ch_node->type_));       /*Oracle Mode for dynamic sql*/
      if (OB_SUCC(ret)) {
        if (T_USER_VARIABLE_IDENTIFIER == ch_node->type_) {
          ObString var_name(ch_node->str_len_, ch_node->str_value_);
          ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, var_name);
          OZ (user_vars.push_back(var_name));
          OZ (user_var_idx.add_member(i));
        } else {
          if (params_.is_prepare_protocol_) {
            ObSEArray<ObQualifiedName, 1> columns;
            ObSEArray<ObVarInfo, 1> var_infos;
            OZ(ObResolverUtils::resolve_const_expr(params_, *ch_node, expr, &var_infos));
            CK(0 == var_infos.count());
            if (OB_SUCC(ret) && expr->get_expr_type() != T_QUESTIONMARK) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("dynamic sql into variable not a question mark", K(ret), KPC(expr));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "dynamic sql into variable not a question mark");
            }
            OZ(pl_vars.push_back(expr));
          } else {
            /*
             * execute select 1 into a; in client.
             * mysql reports "ERROR 1327 (42000): Undeclared variable: a" error.
             * */
            ret = OB_ERR_SP_UNDECLARED_VAR;
            LOG_WARN("PL Variable used in SQL", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && !pl_vars.empty() && !user_vars.empty()) {
      CK(OB_NOT_NULL(params_.expr_factory_), OB_NOT_NULL(params_.session_info_));
      if (OB_SUCC(ret)) {
        ObArray<ObRawExpr*> tmp_exprs;
        int64_t pl_var_idx = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < into_node->num_child_; ++i) {
          expr = NULL;
          ch_node = into_node->children_[i];
          if (user_var_idx.has_member(i)) {
            OZ(ObRawExprUtils::build_get_user_var(*params_.expr_factory_,
                ObString(ch_node->str_len_, ch_node->str_value_),
                expr,
                params_.session_info_,
                params_.query_ctx_,
                &get_stmt()->get_user_vars()));
            OZ(tmp_exprs.push_back(expr));
          } else {
            OZ(tmp_exprs.push_back(pl_vars.at(pl_var_idx++)));
          }
        }
        OZ(pl_vars.assign(tmp_exprs));
        user_vars.reset();
      }
    }
  }
  return ret;
}

// used to find column in all namespace
// search column ref in table columns
// update, delete, insert only has basic table column
// select has joined table column and  basic table column
// so select resolver will overwrite it
int ObDMLResolver::resolve_column_ref_for_subquery(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve_basic_column_ref(q_name, real_ref_expr))) {
    LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve basic column failed", K(ret), K(q_name));
  }
  return ret;
}

// resolve column ref expr that column in single basic or alias table
// TODO :  remove resolve_generated_table_column_item
int ObDMLResolver::resolve_basic_column_ref(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr)
{
  int ret = OB_SUCCESS;
  // check column namespace
  const TableItem* table_item = NULL;
  ColumnItem* column_item = NULL;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt is null", K(ret));
  } else {
    if (OB_FAIL(column_namespace_checker_.check_table_column_namespace(q_name, table_item))) {
      LOG_WARN_IGNORE_COL_NOTFOUND(ret, "check basic column namespace failed", K(ret), K(q_name));
    } else if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is invalid", KPC(table_item));
    } else {
      if (table_item->is_basic_table()) {
        if (OB_FAIL(resolve_basic_column_item(*table_item, q_name.col_name_, false, column_item))) {
          LOG_WARN("resolve column item failed", K(ret));
        }
      } else if (table_item->is_generated_table()) {
        if (OB_FAIL(resolve_generated_table_column_item(*table_item, q_name.col_name_, column_item))) {
          LOG_WARN("resolve column item failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        real_ref_expr = column_item->expr_;
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_basic_column_item(const TableItem& table_item, const ObString& column_name,
    bool include_hidden, ColumnItem*& col_item, ObDMLStmt* stmt /* = NULL */)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr* col_expr = NULL;
  ColumnItem column_item;
  if (NULL == stmt) {
    stmt = get_stmt();
  }
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_checker_) || OB_ISNULL(params_.expr_factory_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema checker is null", K(stmt), K_(schema_checker), K_(params_.expr_factory));
  } else if (OB_UNLIKELY(!table_item.is_basic_table() && !table_item.is_fake_cte_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not base table or alias from base table", K_(table_item.type));
  } else if (NULL != (col_item = stmt->get_column_item(table_item.table_id_, column_name))) {
    // exist, ignore resolve...
  } else {
    // not resolve, so add column item to dml stmt
    const ObColumnSchemaV2* col_schema = NULL;
    // for materialized view, should use materialized view id to resolve column,
    // and its schema id saved in table_item.table_id_
    uint64_t tid = table_item.is_materialized_view_ ? table_item.table_id_ : table_item.ref_id_;
    if (!include_hidden) {
      if (!ObCharset::case_insensitive_equal(column_name, OB_HIDDEN_PK_INCREMENT_COLUMN_NAME)) {
        // do nothing
      } else if (ObResolverUtils::is_restore_user(*session_info_) || ObResolverUtils::is_drc_user(*session_info_)) {
        include_hidden = true;
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(get_column_schema(tid, column_name, col_schema, include_hidden))) {
      LOG_WARN("get column schema failed", K(ret), K(tid), K(column_name));
    } else if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null");
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*params_.expr_factory_, *col_schema, col_expr))) {
      LOG_WARN("build column expr failed", K(ret));
    } else {
      if (is_oracle_mode() && ObLongTextType == col_expr->get_data_type() &&
          !is_virtual_table(col_schema->get_table_id())) {
        col_expr->set_data_type(ObLobType);
      }
      col_expr->set_synonym_db_name(table_item.synonym_db_name_);
      col_expr->set_synonym_name(table_item.synonym_name_);
      col_expr->set_column_attr(table_item.get_table_name(), col_schema->get_column_name_str());
      col_expr->set_from_alias_table(!table_item.alias_name_.empty());
      col_expr->set_database_name(table_item.database_name_);
      // column maybe from alias table, so must reset ref id by table id from table_item
      col_expr->set_ref_id(table_item.table_id_, col_schema->get_column_id());
      bool is_lob_column = (ob_is_text_tc(col_schema->get_data_type())
                            || ob_is_json_tc(col_schema->get_data_type()));
      col_expr->set_lob_column(is_lob_column);
      column_item.set_default_value(col_schema->get_cur_default_value());
    }
    if (OB_SUCC(ret)) {
      ObString col_def;
      ObRawExpr* ref_expr = NULL;
      if (col_schema->is_generated_column()) {
        if (OB_FAIL(col_schema->get_cur_default_value().get_string(col_def))) {
          LOG_WARN("get generated column definition failed", K(ret), K(*col_schema));
        } else if (OB_FAIL(resolve_generated_column_expr(
                       col_def, table_item, col_schema, *col_expr, ref_expr, true, stmt))) {
          LOG_WARN("resolve generated column expr failed", K(ret));
        } else {
          ref_expr->set_for_generated_column();
          col_expr->set_dependant_expr(ref_expr);
        }
      } else if (col_schema->is_default_expr_v2_column()) {
        const bool used_for_generated_column = false;
        if (OB_FAIL(col_schema->get_cur_default_value().get_string(col_def))) {
          LOG_WARN("get expr_default column definition failed", K(ret), KPC(col_schema));
        } else if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(*allocator_,
                       col_def,
                       col_def,
                       CS_TYPE_UTF8MB4_BIN,
                       session_info_->get_local_collation_connection()))) {
        } else if (OB_FAIL(resolve_generated_column_expr(
                       col_def, table_item, col_schema, *col_expr, ref_expr, used_for_generated_column))) {
          LOG_WARN("resolve expr_default column expr failed", K(ret), K(col_def), K(*col_schema));
        } else {
          column_item.set_default_value_expr(ref_expr);
        }
      } else if (share::is_oracle_mode() && OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == col_schema->get_column_id()) {
        if (OB_FAIL(resolve_rowid_column_expr(table_item, stmt, ref_expr))) {
          LOG_WARN("failed to resolve generated column expr", K(ret));
        } else {
          col_expr->set_dependant_expr(ref_expr);
          LOG_TRACE("built rowid column expr", K(col_def), K(*ref_expr), K(*col_expr));
        }
      }
    }
    // init column item
    if (OB_SUCC(ret)) {
      column_item.expr_ = col_expr;
      column_item.table_id_ = col_expr->get_table_id();
      column_item.column_id_ = col_expr->get_column_id();
      column_item.column_name_ = col_expr->get_column_name();
      column_item.base_tid_ = tid;
      column_item.base_cid_ = column_item.column_id_;
      LOG_DEBUG("succ to fill column_item", K(column_item), KPC(col_schema));
      if (OB_FAIL(stmt->add_column_item(column_item))) {
        LOG_WARN("add column item to stmt failed", K(ret));
      } else if (OB_FAIL(col_expr->pull_relation_id_and_levels(stmt->get_current_level()))) {
        LOG_WARN("failed to pullup relation ids", K(ret));
      } else {
        col_item = stmt->get_column_item(stmt->get_column_size() - 1);
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_columns(ObRawExpr*& expr, ObArray<ObQualifiedName>& columns)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> real_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    ObQualifiedName& q_name = columns.at(i);
    ObRawExpr* real_ref_expr = NULL;
    params_.is_column_ref_ = expr->is_column_ref_expr();
    if (OB_FAIL(resolve_qualified_identifier(q_name, columns, real_exprs, real_ref_expr))) {
      LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve column ref expr failed", K(ret), K(q_name));
      report_user_error_msg(ret, expr, q_name);
    } else if (OB_FAIL(real_exprs.push_back(real_ref_expr))) {
      LOG_WARN("push back failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, real_ref_expr))) {
      LOG_WARN("replace column ref expr failed", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObDMLResolver::resolve_qualified_identifier(ObQualifiedName& q_name, ObIArray<ObQualifiedName>& columns,
    ObIArray<ObRawExpr*>& real_exprs, ObRawExpr*& real_ref_expr)
{
  int ret = OB_SUCCESS;
  bool is_external = false;
  if (OB_ISNULL(stmt_) || OB_ISNULL(stmt_->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt_), K(ret));
  } else if (q_name.is_sys_func()) {
    if (OB_FAIL(q_name.access_idents_.at(0).sys_func_expr_->check_param_num())) {
      LOG_WARN("sys func param number not match", K(ret));
    } else {
      real_ref_expr = static_cast<ObRawExpr*>(q_name.access_idents_.at(0).sys_func_expr_);
      is_external = (T_FUN_PL_GET_CURSOR_ATTR == real_ref_expr->get_expr_type());
    }
  } else if (share::is_oracle_mode() && q_name.col_name_.length() == 0) {
    ret = OB_ERR_ZERO_LENGTH_IDENTIFIER;
    LOG_WARN("illegal zero-length identifier", K(ret));
  } else {
    CK(OB_NOT_NULL(get_basic_stmt()));
    if (OB_FAIL(resolve_column_ref_expr(q_name, real_ref_expr))) {
      if (OB_ERR_BAD_FIELD_ERROR == ret) {
        if (OB_FAIL(resolve_sequence_object(q_name, real_ref_expr))) {
          LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve sequence object failed", K(ret), K(q_name));
        }
      }

      if (OB_ERR_BAD_FIELD_ERROR == ret) {
        if (OB_FAIL(resolve_pseudo_column(q_name, real_ref_expr))) {
          LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve pseudo column failed", K(ret), K(q_name));
        }
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < real_exprs.count(); ++i) {
    if (OB_FAIL(ObRawExprUtils::replace_ref_column(real_ref_expr, columns.at(i).ref_expr_, real_exprs.at(i)))) {
      LOG_WARN("replace column ref expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(real_ref_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (T_FUN_PL_SQLCODE_SQLERRM == real_ref_expr->get_expr_type()) {
      ret = OB_ERR_SP_UNDECLARED_VAR;
      LOG_WARN("sqlcode or sqlerrm can not use in dml directly", K(ret), KPC(real_ref_expr));
    } else {
      if (q_name.is_access_root() && is_external && !params_.is_default_param_ && T_INTO_SCOPE != current_scope_ &&
          (real_ref_expr->is_const_expr()             // local variables
              || real_ref_expr->is_obj_access_expr()  // complicated variables
              || real_ref_expr->is_sys_func_expr()    // package variables(system/user variable won't goto here)
              || T_FUN_PL_GET_CURSOR_ATTR == real_ref_expr->get_expr_type())) {
        OZ(ObResolverUtils::resolve_external_param_info(
            params_.external_param_info_, *params_.expr_factory_, params_.prepare_param_count_, real_ref_expr));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    OZ(columns.at(i).replace_access_ident_params(q_name.ref_expr_, real_ref_expr));
  }

  if (OB_ERR_BAD_FIELD_ERROR == ret) {
    // compatible with Oracle:
    //
    // SQL> select nextval from dual;
    // select nextval from dual
    // ERROR at line 1:
    // ORA-00904: "NEXTVAL": invalid identifier
    //
    // SQL> select s.nextval from dual;
    // select s.nextval from dual
    // ERROR at line 1:
    // ORA-02289: sequence does not exist
    ret = update_errno_if_sequence_object(q_name, ret);
  }
  return ret;
}

void ObDMLResolver::report_user_error_msg(int& ret, const ObRawExpr* root_expr, const ObQualifiedName& q_name) const
{
  if (OB_ERR_BAD_FIELD_ERROR == ret && !q_name.tbl_name_.empty()) {
    if (stmt_ != NULL && stmt_->is_select_stmt()) {
      const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
      if (select_stmt->has_set_op()) {
        // can't use table name in union query
        // eg. select c1 from t1 union select c1 from t1 order by t1.c1
        ret = OB_ERR_TABLENAME_NOT_ALLOWED_HERE;
        LOG_USER_ERROR(OB_ERR_TABLENAME_NOT_ALLOWED_HERE, q_name.tbl_name_.length(), q_name.tbl_name_.ptr());
      } else if (select_stmt->get_table_size() <= 0 && current_level_ <= 0) {
        // can't use table name in select from dual
        // eg. select t1.c1
        ret = OB_ERR_UNKNOWN_TABLE;
        ObString tbl_name = concat_table_name(q_name.database_name_, q_name.tbl_name_);
        ObString scope_name = ObString::make_string(get_scope_name(current_scope_));
        ObSQLUtils::copy_and_convert_string_charset(
            *allocator_, tbl_name, tbl_name, CS_TYPE_UTF8MB4_BIN, session_info_->get_local_collation_connection());
        LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE, tbl_name.length(), tbl_name.ptr(), scope_name.length(), scope_name.ptr());
      }
    }
  }
  if (OB_ERR_BAD_FIELD_ERROR == ret) {
    ObString column_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
    ObString scope_name = ObString::make_string(get_scope_name(current_scope_));
    ObSQLUtils::copy_and_convert_string_charset(
        *allocator_, column_name, column_name, CS_TYPE_UTF8MB4_BIN, session_info_->get_local_collation_connection());
    LOG_USER_ERROR(
        OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(), scope_name.length(), scope_name.ptr());
  } else if (OB_NON_UNIQ_ERROR == ret) {
    ObString column_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
    ObString scope_name = ObString::make_string(get_scope_name(current_scope_));
    ObSQLUtils::copy_and_convert_string_charset(
        *allocator_, column_name, column_name, CS_TYPE_UTF8MB4_BIN, session_info_->get_local_collation_connection());
    LOG_USER_ERROR(OB_NON_UNIQ_ERROR, column_name.length(), column_name.ptr(), scope_name.length(), scope_name.ptr());
  } else if (OB_ILLEGAL_REFERENCE == ret) {
    // compatiable with mysql
    // select max(c1) as c from t1 group by c -> err msg:ERROR 1056 (42000): Can't group on 'c'
    // others: select max(c1) as c from t1 group by c+1 ->
    // err msg:ERROR 1247 (42S22): Reference 'c' not supported (reference to group function)
    ObString column_name = q_name.col_name_;
    ObSQLUtils::copy_and_convert_string_charset(
        *allocator_, column_name, column_name, CS_TYPE_UTF8MB4_BIN, session_info_->get_local_collation_connection());
    if (root_expr == q_name.ref_expr_ && q_name.ref_expr_->get_expr_level() == current_level_) {
      ret = OB_WRONG_GROUP_FIELD;
      LOG_USER_ERROR(OB_WRONG_GROUP_FIELD, column_name.length(), column_name.ptr());
    } else {
      LOG_USER_ERROR(OB_ILLEGAL_REFERENCE, column_name.length(), column_name.ptr());
    }
  }
}

// select resolver has namespace more than one layer, select resolve will overwrite it
int ObDMLResolver::resolve_column_ref_expr(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve_table_column_expr(q_name, real_ref_expr))) {
    LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve table column failed", K(q_name), K(ret));
  }
  return ret;
}

int ObDMLResolver::resolve_aggr_exprs(
    ObRawExpr*& expr, common::ObIArray<ObAggFunRawExpr*>& aggr_exprs, const bool need_analyze /* = true*/)
{
  UNUSED(expr);
  UNUSED(need_analyze);
  int ret = OB_SUCCESS;
  if (aggr_exprs.count() > 0 && !is_select_resolver()) {
    if (OB_UNLIKELY(T_FIELD_LIST_SCOPE != current_scope_)) {
      ret = OB_ERR_INVALID_GROUP_FUNC_USE;
      LOG_WARN("invalid scope for agg function", K(ret), K(current_scope_));
    }
    // for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs.count(); i++) {
    // ObAggFunRawExpr *final_aggr = NULL;
    // if (final_aggr != aggr_exprs.at(i)) {
    // if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, aggr_exprs.at(i), final_aggr))) {
    // LOG_WARN("repalce reference column failed", K(ret));
    // }
    // }
    // }
    for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs.count(); i++) {
      ObDelUpdStmt* del_up_stmt = static_cast<ObDelUpdStmt*>(stmt_);
      if (OB_ISNULL(del_up_stmt) || OB_ISNULL(expr)) {
        ret = OB_NOT_INIT;
        LOG_WARN("del_up_stmt is null", K(ret));
      } else if (OB_FAIL(del_up_stmt->add_returning_agg_item(*(aggr_exprs.at(i))))) {
        LOG_WARN("add agg item failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_win_func_exprs(ObRawExpr*& expr, common::ObIArray<ObWinFunRawExpr*>& win_exprs)
{
  UNUSED(expr);
  UNUSED(win_exprs);
  return OB_ERR_INVALID_WINDOW_FUNC_USE;
}

int ObDMLResolver::check_resolve_oracle_sys_view(const ParseNode* node, bool& is_oracle_view)
{
  int ret = OB_SUCCESS;
  is_oracle_view = false;
  ObString table_name;
  ParseNode* db_node = node->children_[0];
  ParseNode* relation_node = node->children_[1];
  int32_t table_len = static_cast<int32_t>(relation_node->str_len_);
  table_name.assign_ptr(const_cast<char*>(relation_node->str_value_), table_len);
  if (nullptr == db_node && is_oracle_mode()) {
    if (session_info_->get_database_name().empty()) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("No database selected");
    } else if (ObSQLUtils::is_oracle_sys_view(table_name)) {
      is_oracle_view = true;
    } else {
      LOG_DEBUG("table_name", K(table_name));
    }
  }
  return ret;
}

int ObDMLResolver::inner_resolve_sys_view(
    const ParseNode* table_node, uint64_t& database_id, ObString& tbl_name, ObString& db_name, bool& use_sys_tenant)
{
  int ret = OB_SUCCESS;
  bool is_db_explicit = false;
  if (OB_FAIL(inner_resolve_sys_view(table_node, database_id, tbl_name, db_name, is_db_explicit, use_sys_tenant))) {
    LOG_WARN("failed to inner_resolve_sys_view", K(ret));
  }
  return ret;
}

// sys_tenant or oracle sys view will resolve again
int ObDMLResolver::inner_resolve_sys_view(const ParseNode* table_node, uint64_t& database_id, ObString& tbl_name,
    ObString& db_name, bool& is_db_explicit, bool& use_sys_tenant)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObString tmp_db_name;
  ObString tmp_tbl_name;
  bool is_oracle_sys_view = false;
  // first check table is sys view in sys tenant
  // if not, then resolve sys view on SYS schema on Oracle mode
  if (OB_SUCCESS != (tmp_ret = check_need_use_sys_tenant(use_sys_tenant))) {
    LOG_WARN("fail to check need use sys tenant", K(tmp_ret));
  } else if (OB_SUCCESS !=
             (tmp_ret = resolve_table_relation_node_v2(table_node, tmp_tbl_name, tmp_db_name, is_db_explicit))) {
    LOG_WARN("fail to resolve table relation node", K(tmp_ret));
  } else if (use_sys_tenant) {
    // sys view in sys tenant
    const bool is_index_table = false;
    const ObTableSchema* table_schema = NULL;
    if (OB_SUCCESS != (tmp_ret = schema_checker_->get_table_schema(
                           OB_SYS_TENANT_ID, tmp_db_name, tmp_tbl_name, is_index_table, table_schema))) {
      LOG_WARN("fail to get table schema", K(tmp_ret), K(tmp_db_name), K(tmp_tbl_name));
    } else if (NULL == table_schema) {
      tmp_ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table_schema should not be NULL", K(tmp_ret));
    } else if (table_schema->is_user_table()) {
      use_sys_tenant = false;
    } else if (OB_SUCCESS != (tmp_ret = schema_checker_->get_database_id(OB_SYS_TENANT_ID, tmp_db_name, database_id))) {
      LOG_WARN("fail to get database id", K(tmp_ret));
    }
  }

  // sys view resolve fail, it need resovle oracle mode again
  if (OB_SUCCESS != tmp_ret) {
    use_sys_tenant = false;
    tmp_ret = OB_SUCCESS;
  }
  // resovle sys view in oracle mode again
  if (!use_sys_tenant && (OB_SUCCESS != (tmp_ret = check_resolve_oracle_sys_view(table_node, is_oracle_sys_view)))) {
    LOG_WARN("fail to check resolve oracle sys view", K(tmp_ret));
  } else if (is_oracle_sys_view) {
    // resolve sys view in oracle mode
    if (OB_SUCCESS != (tmp_ret = resolve_table_relation_node_v2(
                           table_node, tmp_tbl_name, tmp_db_name, is_db_explicit, false, is_oracle_sys_view))) {
      LOG_WARN("fail to resolve table relation node", K(tmp_ret));
    } else {
      const bool is_index_table = false;
      const ObTableSchema* table_schema = NULL;
      if (OB_SUCCESS !=
          (tmp_ret = schema_checker_->get_table_schema(
               session_info_->get_effective_tenant_id(), tmp_db_name, tmp_tbl_name, is_index_table, table_schema))) {
        LOG_WARN("fail to get table schema", K(tmp_ret), K(tmp_db_name), K(tmp_tbl_name));
      } else if (NULL == table_schema) {
        tmp_ret = OB_INVALID_ARGUMENT;
        LOG_WARN("table_schema should not be NULL", K(tmp_ret));
      } else if (!table_schema->is_sys_view()) {
        is_oracle_sys_view = false;
      } else if (OB_SUCCESS != (tmp_ret = schema_checker_->get_database_id(
                                    session_info_->get_effective_tenant_id(), tmp_db_name, database_id))) {
        LOG_WARN("fail to get database id", K(tmp_ret));
      }
    }
  }

  if (tmp_ret != OB_SUCCESS) {
    use_sys_tenant = false;
    is_oracle_sys_view = false;
    ret = tmp_ret;
  } else if (use_sys_tenant || is_oracle_sys_view) {
    ret = OB_SUCCESS;
    db_name = tmp_db_name;
    tbl_name = tmp_tbl_name;
    SQL_RESV_LOG(INFO, "table found in sys tenant", K(tmp_db_name), K(tmp_tbl_name));
  } else {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to resolve table", K(ret));
  }
  return ret;
}

int ObDMLResolver::resolve_table_relation_factor_wrapper(const ParseNode* table_node, uint64_t& dblink_id,
    uint64_t& database_id, ObString& tbl_name, ObString& synonym_name, ObString& synonym_db_name, ObString& db_name,
    ObString& dblink_name, bool& is_db_explicit, bool& use_sys_tenant)
{
  int ret = OB_SUCCESS;

  if (NULL == table_node) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_node should not be NULL", K(ret));
  }

  const uint64_t tenant_id = session_info_->get_effective_tenant_id();

  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_table_relation_factor(table_node,
            dblink_id,
            database_id,
            tbl_name,
            synonym_name,
            synonym_db_name,
            db_name,
            dblink_name,
            is_db_explicit))) {
      if (ret != OB_TABLE_NOT_EXIST) {
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = inner_resolve_sys_view(
                               table_node, database_id, tbl_name, db_name, is_db_explicit, use_sys_tenant))) {
          LOG_WARN("fail to resolve sys view", K(ret));
        } else {
          ret = tmp_ret;
        }
      }
    }
  }

  return ret;
}

int ObDMLResolver::resolve_sys_vars(ObArray<ObVarInfo>& sys_vars)
{
  int ret = OB_SUCCESS;
  ObQueryCtx* query_ctx = NULL;
  if (OB_ISNULL(stmt_) || OB_ISNULL(query_ctx = stmt_->get_query_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt_ or query_ctx is null", K_(stmt), K(query_ctx));
  } else if (OB_FAIL(ObRawExprUtils::merge_variables(sys_vars, query_ctx->variables_))) {
    LOG_WARN("failed to record variables", K(ret));
  }
  return ret;
}

int ObDMLResolver::resolve_basic_table(const ParseNode& parse_tree, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  const ParseNode* table_node = &parse_tree;
  const ParseNode* alias_node = NULL;
  const ParseNode* index_hint_node = NULL;
  const ParseNode* part_node = NULL;
  const ParseNode* sample_node = NULL;
  const ParseNode* time_node = NULL;
  const ParseNode* transpose_node = NULL;
  bool is_db_explicit = false;
  ObDMLStmt* stmt = get_stmt();
  uint64_t tenant_id = OB_INVALID_ID;
  uint64_t dblink_id = OB_INVALID_ID;
  ObString database_name;
  ObString table_name;
  ObString alias_name;
  ObString synonym_name;
  ObString dblink_name;
  ObString synonym_db_name;
  bool use_sys_tenant = false;
  uint64_t database_id = OB_INVALID_ID;
  const ObTableSchema* table_schema = NULL;

  if (T_ORG == parse_tree.type_) {
    table_node = parse_tree.children_[0];
    index_hint_node = parse_tree.children_[1];
    part_node = parse_tree.children_[2];
    if (parse_tree.num_child_ >= 4) {
      sample_node = parse_tree.children_[3];
    }
    if (parse_tree.num_child_ >= 5) {
      time_node = parse_tree.children_[4];
    }
  } else if (T_ALIAS == parse_tree.type_) {
    table_node = parse_tree.children_[0];
    alias_node = parse_tree.children_[1];
    if (T_RELATION_FACTOR == table_node->type_) {
      index_hint_node = parse_tree.children_[2];
      part_node = parse_tree.children_[3];
      if (parse_tree.num_child_ >= 5) {
        sample_node = parse_tree.children_[4];
      }
      if (parse_tree.num_child_ >= 6) {
        time_node = parse_tree.children_[5];
      }
      if (parse_tree.num_child_ >= 7) {
        transpose_node = parse_tree.children_[6];
      }
    }
  }

  if (OB_FAIL(resolve_table_relation_factor_wrapper(table_node,
          dblink_id,
          database_id,
          table_name,
          synonym_name,
          synonym_db_name,
          database_name,
          dblink_name,
          is_db_explicit,
          use_sys_tenant))) {
    if (OB_TABLE_NOT_EXIST == ret || OB_ERR_BAD_DATABASE == ret) {
      if (OB_INFORMATION_SCHEMA_ID == extract_pure_id(database_id)) {
        ret = OB_ERR_UNKNOWN_TABLE;
        LOG_USER_ERROR(
            OB_ERR_UNKNOWN_TABLE, table_name.length(), table_name.ptr(), database_name.length(), database_name.ptr());
      } else {
        ret = OB_TABLE_NOT_EXIST;
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(database_name), to_cstring(table_name));
      }
    } else {
      LOG_WARN("fail to resolve table name", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_INVALID_ID == dblink_id) {
    if (alias_node != NULL) {
      alias_name.assign_ptr(alias_node->str_value_, static_cast<int32_t>(alias_node->str_len_));
    }

    // flag IS to set larger query_timeout.
    if (OB_INFORMATION_SCHEMA_ID == extract_pure_id(database_id)) {
      stmt->set_is_table_flag();
    }
    tenant_id = use_sys_tenant ? OB_SYS_TENANT_ID : session_info_->get_effective_tenant_id();
    bool cte_table_fisrt = (table_node->children_[0] == NULL);
    if (OB_FAIL(resolve_base_or_alias_table_item_normal(tenant_id,
            database_name,
            is_db_explicit,
            table_name,
            alias_name,
            synonym_name,
            synonym_db_name,
            table_item,
            cte_table_fisrt))) {
      LOG_WARN("resolve base or alias table item failed", K(ret));
    } else {
      // set thead local mode if current tenant is oracle mode.
      ObWorker::CompatMode compat_mode;
      ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode);
      CompatModeGuard g(compat_mode);
      bool is_sync_ddl_user = false;
      if (OB_ISNULL(table_item) || OB_ISNULL(stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(stmt), K(ret));
      } else if (OB_FAIL(schema_checker_->get_table_schema(table_item->ref_id_, table_schema))) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("get table schema failed", K_(table_item->table_name), K(tenant_id), K(database_id), K(ret));
      } else if (OB_FAIL(resolve_generated_column_expr_temp(*table_item, *table_schema))) {
        LOG_WARN("resolve generated column expr templte failed", K(ret));
      } else if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
        // liboblog reorder data randomly, so it may update table after drop it.
        // This results in operation on tables in recycle bin.
        LOG_WARN("Failed to check sync_ddl_user", K(ret));
      } else if (!stmt->is_select_stmt() && table_schema->is_in_recyclebin() && !is_sync_ddl_user) {
        ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
        LOG_WARN("write operation on recylebin object is not allowed", K(ret), "stmt_type", stmt->get_stmt_type());
      } else if (table_schema->is_vir_table() && !stmt->is_select_stmt()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "DML operation on Virtual Table/Temporary Table");
      } else if ((params_.is_from_create_view_ || params_.is_from_create_table_) && table_schema->is_mysql_tmp_table()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "View/Table's column refers to a temporary table");
      } else if (OB_FAIL(resolve_table_partition_expr(*table_item, *table_schema))) {
        LOG_WARN("resolve table partition expr failed", K(ret), K(table_name));
      } else if (table_schema->is_oracle_tmp_table() && stmt::T_MERGE == stmt->get_stmt_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "MERGE refers to a temporary table");
      } else if (table_item->is_materialized_view_ && !stmt->is_select_stmt()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "DML operation on Materialized View");
      } else if (OB_UNLIKELY(table_item->is_index_table_)) {
        ObString index_name;
        if (OB_FAIL(ObTableSchema::get_index_name(*allocator_, table_item->ref_id_, table_name, index_name))) {
          LOG_WARN("get index name failed", K(ret));
        } else if (OB_FAIL(create_force_index_hint(table_item->table_id_, index_name))) {
          // table name is actually index_name
          LOG_WARN("create force index hint for data table failed",
              K(ret),
              "table_name",
              table_schema->get_table_name_str(),
              K(index_name));
        }
      } else if (index_hint_node) {
        if (OB_FAIL(resolve_index_hint(table_item->table_id_, index_hint_node))) {
          LOG_WARN("resolve index hint failed", K(ret));
        }
      }

      bool is_expand_mv = false;
      if (OB_SUCCESS == ret) {
        if (table_item->is_materialized_view_) {
          const ObTableSchema* mv_schema = NULL;
          if (OB_FAIL(schema_checker_->get_table_schema(table_item->table_id_, mv_schema))) {
            // if mv supports alias name here, need to replace table_item->table_id_ with table_item->index_id_;
            LOG_WARN("cannot get mv schema", K(ret), K(*table_item));
          } else if (NULL == mv_schema) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mv schema is null, which is not expected", K(*table_item));
          } else if (!mv_schema->can_read_index()) {
            is_expand_mv = true;
            table_item->ref_id_ = table_item->table_id_;
            if (OB_FAIL(expand_view(*table_item))) {
              LOG_WARN("expand view failed", K(ret), K(*table_item));
            }
          }
        }
      }
      if (OB_SUCCESS == ret) {
        if ((!is_expand_mv) && (table_item->is_view_table_ && !table_item->is_materialized_view_)) {
          if (OB_FAIL(expand_view(*table_item))) {
            LOG_WARN("expand view failed", K(ret), K(*table_item));
          }
        }
      }
      if (OB_SUCCESS == ret && part_node) {
        if (is_virtual_table(table_item->ref_id_) && table_schema->get_part_option().get_part_num() > 1) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Partitioned virtual table with partition hint");
        } else if (table_item->cte_type_ != TableItem::CTEType::NOT_CTE) {
          // ret = -14109
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Partitioned cte table with partition hint");
        } else if (OB_FAIL(resolve_partitions(part_node, table_item->table_id_, *table_schema))) {
          LOG_WARN("Resolve partitions error", K(ret));
        } else {
        }
      }
      if (OB_SUCCESS == ret && sample_node != NULL && T_SAMPLE_SCAN == sample_node->type_) {
        if (is_virtual_table(table_item->ref_id_)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "sampling virtual table");
        } else if (OB_FAIL(resolve_sample_clause(sample_node, table_item->table_id_))) {
          LOG_WARN("resolve sample clause failed", K(ret));
        } else {
        }
      }
      if (OB_SUCCESS == ret && is_virtual_table(table_item->ref_id_)) {
        stmt->get_query_ctx()->is_contain_virtual_table_ = true;
      }

      if (OB_SUCCESS == ret) {
        if (OB_FAIL(resolve_transpose_table(transpose_node, table_item))) {
          LOG_WARN("resolve_transpose_table failed", K(ret));
        }
      }
    }
  }  // if (OB_SUCC(ret) && OB_INVALID_ID == dblink_id)
  if (OB_SUCC(ret) && OB_INVALID_ID != dblink_id) {
    if (OB_NOT_NULL(part_node)) {
      ret = OB_ERR_REMOTE_PART_ILLEGAL;
      LOG_WARN("partition extended table name cannot refer to a remote object", K(ret));
    } else if (!OB_ISNULL(alias_node)) {
      alias_name.assign_ptr(alias_node->str_value_, static_cast<int32_t>(alias_node->str_len_));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(resolve_base_or_alias_table_item_dblink(
              dblink_id, dblink_name, database_name, table_name, alias_name, table_item))) {
        LOG_WARN("resolve base or alias table item for dblink failed", K(ret));
      }
    }
  }

  LOG_DEBUG("finish resolve_basic_table", K(ret), KPC(table_item));
  return ret;
}

int ObDMLResolver::resolve_table_drop_oracle_temp_table(TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item) || OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL pointer", K(table_item), K(session_info_), K(schema_checker_), K(ret));
  } else if (table_item->is_system_table_ || table_item->is_index_table_ || table_item->is_view_table_ ||
             table_item->is_recursive_union_fake_table_ || table_item->is_materialized_view_) {
    // do nothing
  } else if (is_oracle_mode() && false == session_info_->get_has_temp_table_flag()) {
    const ObTableSchema* table_schema = NULL;
    if (is_link_table_id(table_item->ref_id_)) {
      // skip
    } else if (OB_FAIL(schema_checker_->get_table_schema(table_item->ref_id_, table_schema))) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("get table schema failed", K_(table_item->table_name), K(ret));
    } else if (OB_NOT_NULL(table_schema) && table_schema->is_oracle_tmp_table()) {
      if (OB_FAIL(session_info_->drop_reused_oracle_temp_tables())) {
        LOG_WARN("fail to drop reused oracle temporary tables", K(ret));
      } else {
        session_info_->set_has_temp_table_flag();
        LOG_DEBUG("succeed to drop oracle temporary table in case of session id reused",
            K(session_info_->get_sessid_for_table()));
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_table(const ParseNode& parse_tree, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  const ParseNode* table_node = &parse_tree;
  const ParseNode* alias_node = NULL;
  const ParseNode* time_node = NULL;
  const ParseNode* transpose_node = NULL;
  ObDMLStmt* stmt = get_stmt();

  if (T_ORG == parse_tree.type_) {
    table_node = parse_tree.children_[0];
    if (parse_tree.num_child_ >= 5) {
      time_node = parse_tree.children_[4];
    }
  } else if (T_ALIAS == parse_tree.type_) {
    table_node = parse_tree.children_[0];
    alias_node = parse_tree.children_[1];
    if (parse_tree.num_child_ >= 6) {
      time_node = parse_tree.children_[5];
    }
    if (parse_tree.num_child_ >= 7) {
      transpose_node = parse_tree.children_[6];
    }
    if (parse_tree.num_child_ >= 8 && OB_NOT_NULL(parse_tree.children_[7])) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("fetch clause can't occur in table attributes", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (!stmt->is_select_stmt() && OB_NOT_NULL(time_node)) {
      ret = OB_ERR_FLASHBACK_QUERY_WITH_UPDATE;
      LOG_WARN("snapshot expression not allowed here", K(ret));
    } else {
      switch (table_node->type_) {
        case T_RELATION_FACTOR: {
          if (OB_FAIL(resolve_basic_table(parse_tree, table_item))) {
            LOG_WARN("resolve basic table failed", K(ret));
          }
          break;
        }
        case T_SELECT: {
          if (OB_ISNULL(alias_node)) {
            ret = OB_ERR_PARSER_SYNTAX;
            LOG_WARN("generated table must have alias name");
          } else {
            bool tmp_have_same_table = params_.have_same_table_name_;
            params_.have_same_table_name_ = false;
            ObString alias_name(alias_node->str_len_, alias_node->str_value_);
            if (OB_FAIL(resolve_generate_table(*table_node, alias_name, table_item))) {
              LOG_WARN("resolve generate table failed", K(ret));
            } else if (OB_FAIL(resolve_transpose_table(transpose_node, table_item))) {
              LOG_WARN("resolve_transpose_table failed", K(ret));
            } else {
              params_.have_same_table_name_ = tmp_have_same_table;
            }
          }

          break;
        }
        case T_JOINED_TABLE: {
          JoinedTable* root = NULL;
          set_has_ansi_join(true);
          if (OB_FAIL(resolve_joined_table(parse_tree, root))) {
            LOG_WARN("resolve joined table failed", K(ret));
          } else if (OB_FAIL(stmt->add_joined_table(root))) {
            LOG_WARN("add joined table failed", K(ret));
          } else {
            table_item = root;
          }
          break;
        }
        case T_TABLE_COLLECTION_EXPRESSION: {
          ret = OB_NOT_SUPPORTED;
          break;
        }
        default:
          /* won't be here */
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("Unknown table type", "node_type", table_node->type_);
          break;
      }
      if (OB_SUCC(ret) && OB_FAIL(resolve_table_drop_oracle_temp_table(table_item))) {
        LOG_WARN("drop oracle temporary table failed in resolve table", K(ret), K(*table_item));
      }
    }
  }

  return ret;
}

int ObDMLResolver::resolve_joined_table(const ParseNode& parse_node, JoinedTable*& joined_table)
{
  int ret = OB_SUCCESS;
  ParseNode* condition_node = parse_node.children_[3];
  ParseNode* attr_node = parse_node.children_[4];
  if (OB_FAIL(resolve_joined_table_item(parse_node, joined_table))) {
    LOG_WARN("resolve joined table item failed", K(ret));
  } else {
    column_namespace_checker_.add_current_joined_table(joined_table);
  }
  if (OB_FAIL(ret)) {
    // do noting;
  } else if (NULL != attr_node && T_NATURAL_JOIN == attr_node->type_) {
    if (OB_FAIL(fill_same_column_to_using(joined_table))) {
      LOG_WARN("failed to fill same columns", K(ret));
    } else if (OB_FAIL(transfer_using_to_on_expr(joined_table))) {
      LOG_WARN("failed to transfer using to on expr", K(ret));
    }
  } else if (condition_node != NULL) {
    if (T_COLUMN_LIST == condition_node->type_) {
      if (OB_FAIL(resolve_using_columns(*condition_node, joined_table->using_columns_))) {
        LOG_WARN("resolve using column failed", K(ret));
      } else if (OB_FAIL(transfer_using_to_on_expr(joined_table))) {
        LOG_WARN("transfer using to on expr failed", K(ret));
      }
    } else {
      // transform join on condition
      ObStmtScope old_scope = current_scope_;
      current_scope_ = T_ON_SCOPE;
      if (OB_FAIL(resolve_and_split_sql_expr_with_bool_expr(*condition_node, joined_table->join_conditions_))) {
        LOG_WARN("resolve and split sql expr failed", K(ret));
      } else { /*do nothing*/
      }
      current_scope_ = old_scope;
    }
  }
  return ret;
}

int ObDMLResolver::resolve_using_columns(const ParseNode& using_node, ObIArray<ObString>& column_names)
{
  int ret = OB_SUCCESS;
  ObString column_name;
  for (int32_t i = 0; OB_SUCCESS == ret && i < using_node.num_child_; ++i) {
    const ParseNode* child_node = NULL;
    if (OB_ISNULL(child_node = using_node.children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child node is null");
    } else {
      // will check using column in tansfer using expr
      column_name.assign_ptr(const_cast<char*>(child_node->str_value_), static_cast<int32_t>(child_node->str_len_));
      // filter out duplicated column
      bool found = false;
      for (int64_t j = 0; j < column_names.count(); j++) {
        if (ObCharset::case_insensitive_equal(column_names.at(j), column_name)) {
          found = true;
          break;
        }
      }
      if (!found) {
        if (OB_FAIL(column_names.push_back(column_name))) {
          LOG_WARN("Add column name failed", K(ret));
        }
      }
    }
  }
  return ret;
}

// major logic is refered to
// - resolve_and_split_sql_expr();
// - resolve_sql_expr();
//  As the left_table might be nested, but the right_table not,
//  therefore, the left_table should only have unique column_names
//  to avoid ambiguous column resolving.
//
int ObDMLResolver::transfer_using_to_on_expr(JoinedTable*& joined_table)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  JoinedTable* cur_table = joined_table;
  if (OB_ISNULL(params_.expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("params is invalid", K_(params_.expr_factory));
  }
  // construct AND expr and columns in exprs
  for (int64_t i = 0; OB_SUCC(ret) && i < cur_table->using_columns_.count(); ++i) {
    // construct t_left_N.ck = t_right.ck
    ObOpRawExpr* b_expr = NULL;
    ObRawExpr* left_expr = NULL;
    ObRawExpr* right_expr = NULL;

    // make two sub exprs: t_left_N.ck and t_right.ck
    const TableItem* left_table = NULL;
    const TableItem* right_table = NULL;
    const ObString& column_name = cur_table->using_columns_.at(i);
    if (OB_FAIL(column_namespace_checker_.check_using_column_namespace(column_name, left_table, right_table))) {
      LOG_WARN("check using column namespace failed", K(column_name));
      if (OB_ERR_BAD_FIELD_ERROR == ret) {
        ObString scope_name = ObString::make_string(get_scope_name(current_scope_));
        LOG_USER_ERROR(
            OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(), scope_name.length(), scope_name.ptr());
      } else if (OB_NON_UNIQ_ERROR == ret) {
        ObString scope_name = ObString::make_string(get_scope_name(current_scope_));
        LOG_USER_ERROR(
            OB_NON_UNIQ_ERROR, column_name.length(), column_name.ptr(), scope_name.length(), scope_name.ptr());
      }
    } else if (left_table->is_joined_table()) {
      if (OB_FAIL(
              resolve_join_table_column_item(static_cast<const JoinedTable&>(*left_table), column_name, left_expr))) {
        LOG_WARN("resolve join table column item failed", K(ret), K(column_name));
      }
    } else {
      ColumnItem* column = NULL;
      if (OB_FAIL(resolve_single_table_column_item(*left_table, column_name, false, column))) {
        LOG_WARN("resolve single table column item failed", K(ret), K(column_name));
      } else {
        left_expr = column->expr_;
      }
    }
    if (OB_SUCC(ret)) {
      if (right_table->is_joined_table()) {
        if (OB_FAIL(resolve_join_table_column_item(
                static_cast<const JoinedTable&>(*right_table), column_name, right_expr))) {
          LOG_WARN("resolve join table column item failed", K(ret), K(column_name));
        }
      } else {
        ColumnItem* column = NULL;
        if (OB_FAIL(resolve_single_table_column_item(*right_table, column_name, false, column))) {
          LOG_WARN("resolve single table column item failed", K(ret), K(column_name));
        } else {
          right_expr = column->expr_;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(
              params_.expr_factory_->create_raw_expr(T_OP_EQ, b_expr))) {  // make equal expr: t_left_N.ck = t_right.ck
        LOG_WARN("b_expr is null", K(ret));
      } else if (OB_ISNULL(b_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("b_expr is null");
      } else if (OB_FAIL(b_expr->set_param_exprs(left_expr, right_expr))) {
        LOG_WARN("set b_expr param exprs failed", K(ret));
      } else if (OB_FAIL(b_expr->formalize(session_info_))) {
        LOG_WARN("resolve formalize expression", K(ret));
      } else if (OB_FAIL(cur_table->join_conditions_.push_back(b_expr))) {
        LOG_WARN("Add expression error", K(ret));
      }
    }
    if (OB_SUCC(ret) && FULL_OUTER_JOIN == cur_table->joined_type_) {
      ObSysFunRawExpr* coalesce_expr = NULL;
      if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_FUN_SYS_COALESCE, coalesce_expr))) {
        LOG_WARN("create raw expr failed", K(ret));
      } else if (OB_FAIL(coalesce_expr->set_param_exprs(left_expr, right_expr))) {
        LOG_WARN("set coalesce expr child failed", K(ret));
      } else if (OB_FAIL(coalesce_expr->formalize(session_info_))) {
        LOG_WARN("formalize coalesce expr failed", K(ret));
      } else if (OB_FAIL(cur_table->coalesce_expr_.push_back(coalesce_expr))) {
        LOG_WARN("push expr to coalesce failed", K(ret));
      }
    }
  }  // end of for

  return ret;
}

// transfer (t1,t2) join (t3,t4) to (t1 join t2) join (t3 join t4)
int ObDMLResolver::transfer_to_inner_joined(const ParseNode& parse_node, JoinedTable*& joined_table)
{
  int ret = OB_SUCCESS;
  ParseNode* table_node = NULL;
  JoinedTable* cur_table = NULL;
  JoinedTable* child_table = NULL;
  JoinedTable* temp_table = NULL;
  TableItem* table_item = NULL;
  for (int64_t j = 0; OB_SUCC(ret) && j < parse_node.num_child_; j++) {
    if (0 == j) {
      if (OB_FAIL(alloc_joined_table_item(cur_table))) {
        LOG_WARN("create joined table item failed", K(ret));
      } else {
        cur_table->table_id_ = generate_table_id();
        cur_table->type_ = TableItem::JOINED_TABLE;
      }
    }
    table_node = parse_node.children_[j];
    if (OB_SUCC(ret)) {
      if (T_JOINED_TABLE == table_node->type_) {
        if (OB_FAIL(resolve_joined_table(*table_node, child_table))) {
          LOG_WARN("resolve child joined table failed", K(ret));
        } else if (0 == j) {
          cur_table->left_table_ = child_table;
        } else {
          cur_table->right_table_ = child_table;
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < child_table->single_table_ids_.count(); ++i) {
          uint64_t child_table_id = child_table->single_table_ids_.at(i);
          if (OB_FAIL(cur_table->single_table_ids_.push_back(child_table_id))) {
            LOG_WARN("push back child_table_id failed", K(ret));
          }
        }
      } else {
        if (OB_FAIL(resolve_table(*table_node, table_item))) {
          LOG_WARN("resolve table failed", K(ret));
        } else if (0 == j) {
          cur_table->left_table_ = table_item;
        } else {
          cur_table->right_table_ = table_item;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(cur_table->single_table_ids_.push_back(table_item->table_id_))) {
            LOG_WARN("push back child table id failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      cur_table->joined_type_ = INNER_JOIN;
      if (j != 0 && j != parse_node.num_child_ - 1) {
        if (OB_FAIL(alloc_joined_table_item(temp_table))) {
          LOG_WARN("create joined table item failed", K(ret));
        } else {
          temp_table->table_id_ = generate_table_id();
          temp_table->type_ = TableItem::JOINED_TABLE;
          temp_table->left_table_ = cur_table;
          cur_table = temp_table;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    joined_table = cur_table;
  }
  return ret;
}

// resolve table column reference
// select&update&delete stmt can access joined table column, generated table column or base table column
int ObDMLResolver::resolve_table_column_expr(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr)
{
  // search order
  // 1. joined table column
  // 2. basic table column or generated table column
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info is null", K_(session_info));
  } else {
    const TableItem* table_item = NULL;
    if (OB_FAIL(column_namespace_checker_.check_table_column_namespace(q_name, table_item))) {
      LOG_WARN_IGNORE_COL_NOTFOUND(ret, "column don't found in table", K(ret), K(q_name));
    } else if (table_item->is_joined_table()) {
      const JoinedTable& joined_table = static_cast<const JoinedTable&>(*table_item);
      if (OB_FAIL(resolve_join_table_column_item(joined_table, q_name.col_name_, real_ref_expr))) {
        LOG_WARN("resolve join table column item failed", K(ret));
      }
    } else {
      ColumnItem* col_item = NULL;
      if (OB_FAIL(resolve_single_table_column_item(*table_item, q_name.col_name_, false, col_item))) {
        LOG_WARN("resolve single table column item failed", K(ret));
      } else if (OB_ISNULL(col_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col item is null", K(ret));
      } else {
        real_ref_expr = col_item->expr_;
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_single_table_column_item(
    const TableItem& table_item, const ObString& column_name, bool include_hidden, ColumnItem*& col_item)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_checker_) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema checker is null", K(stmt), K_(schema_checker), K_(params_.expr_factory));
  } else if (table_item.is_basic_table() || table_item.is_fake_cte_table()) {
    if (OB_FAIL(resolve_basic_column_item(table_item, column_name, include_hidden, col_item))) {
      LOG_WARN("resolve basic column item failed", K(ret));
    } else { /*do nothing*/
    }
  } else if (table_item.is_generated_table()) {
    if (OB_FAIL(resolve_generated_table_column_item(table_item, column_name, col_item))) {
      LOG_WARN("resolve generated table column failed", K(ret));
    }
  } else if (table_item.is_function_table()) {
    if (OB_FAIL(resolve_function_table_column_item(table_item, column_name, col_item))) {
      LOG_WARN("resolve function table column failed", K(ret), K(column_name));
    }
  }
  return ret;
}

int ObDMLResolver::resolve_join_table_column_item(
    const JoinedTable& joined_table, const ObString& column_name, ObRawExpr*& real_ref_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr* coalesce_expr = NULL;
  if (OB_UNLIKELY(joined_table.joined_type_ != FULL_OUTER_JOIN)) {
    // only when column name hit full join table using name, we would search column expr in joined table
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("joined table type is unexpected", K(ret));
  } else {
    if (joined_table.coalesce_expr_.count() > 0) {
      for (int i = 0; i < joined_table.coalesce_expr_.count(); i++) {
        if (ObCharset::case_insensitive_equal(joined_table.using_columns_.at(i), column_name)) {
          coalesce_expr = joined_table.coalesce_expr_.at(i);
          break;
        }
      }
    }
    if (NULL == coalesce_expr) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_DEBUG("full join table using name can't be found", K(column_name));
    } else {
      real_ref_expr = coalesce_expr;
    }
  }
  return ret;
}

int ObDMLResolver::resolve_joined_table_item(const ParseNode& parse_node, JoinedTable*& joined_table)
{
  int ret = OB_SUCCESS;
  ParseNode* table_node = NULL;
  JoinedTable* cur_table = NULL;
  JoinedTable* child_table = NULL;
  TableItem* table_item = NULL;
  ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt_);

  if (OB_ISNULL(select_stmt) || OB_UNLIKELY(parse_node.type_ != T_JOINED_TABLE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(select_stmt), K_(parse_node.type));
  } else if (OB_FAIL(alloc_joined_table_item(cur_table))) {
    LOG_WARN("create joined table item failed", K(ret));
  } else {
    cur_table->table_id_ = generate_table_id();
    cur_table->type_ = TableItem::JOINED_TABLE;
  }
  /* resolve table */
  for (uint64_t i = 1; OB_SUCC(ret) && i <= 2; i++) {
    table_node = parse_node.children_[i];
    // nested join case or normal join case
    if (T_JOINED_TABLE == table_node->type_) {
      if (OB_FAIL(resolve_joined_table(*table_node, child_table))) {
        LOG_WARN("resolve child joined table failed", K(ret));
      } else if (1 == i) {
        cur_table->left_table_ = child_table;
      } else {
        cur_table->right_table_ = child_table;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < child_table->single_table_ids_.count(); ++i) {
        uint64_t child_table_id = child_table->single_table_ids_.at(i);
        if (OB_FAIL(cur_table->single_table_ids_.push_back(child_table_id))) {
          LOG_WARN("push back child_table_id failed", K(ret));
        }
      }
    } else if (T_TABLE_REFERENCES == table_node->type_) {
      if (OB_FAIL(transfer_to_inner_joined(*table_node, child_table))) {
        LOG_WARN("transfer to inner join failed", K(ret));
      } else if (1 == i) {
        cur_table->left_table_ = child_table;
      } else {
        cur_table->right_table_ = child_table;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < child_table->single_table_ids_.count(); ++i) {
        uint64_t child_table_id = child_table->single_table_ids_.at(i);
        if (OB_FAIL(cur_table->single_table_ids_.push_back(child_table_id))) {
          LOG_WARN("push back child_table_id failed", K(ret));
        }
      }
    } else {
      /*
       * for recursive cte. if right child of union all is join,
       * cte can't appear in left of right join, can't appear in right of left join,
       *  and full join is not allowed.
       * */
      if (OB_FAIL(resolve_table(*table_node, table_item))) {
        LOG_WARN("resolve table failed", K(ret));
      } else if (OB_FAIL(check_special_join_table(*table_item, 1 == i, parse_node.children_[0]->type_))) {
        LOG_WARN("check special join table failed", K(ret), K(i));
      } else if (1 == i) {
        cur_table->left_table_ = table_item;
      } else {
        cur_table->right_table_ = table_item;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(cur_table->single_table_ids_.push_back(table_item->table_id_))) {
          LOG_WARN("push back child table id failed", K(ret));
        }
      }
    }
  }

  /* resolve join type */
  if (OB_SUCC(ret)) {
    switch (parse_node.children_[0]->type_) {
      case T_JOIN_FULL:
        cur_table->joined_type_ = FULL_OUTER_JOIN;
        break;
      case T_JOIN_LEFT:
        cur_table->joined_type_ = LEFT_OUTER_JOIN;
        break;
      case T_JOIN_RIGHT:
        cur_table->joined_type_ = RIGHT_OUTER_JOIN;
        break;
      case T_JOIN_INNER:
        cur_table->joined_type_ = INNER_JOIN;
        break;
      default:
        /* won't be here */
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unknown table type in outer join", K_(table_node->type));
        break;
    }
  }
  if (OB_SUCC(ret)) {
    joined_table = cur_table;
  }

  return ret;
}

int ObDMLResolver::check_special_join_table(const TableItem& join_table, bool is_left_child, ObItemType join_type)
{
  UNUSED(is_left_child);
  UNUSED(join_type);
  int ret = OB_SUCCESS;
  if (join_table.is_fake_cte_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "CTE in dml");
  }
  return ret;
}

int ObDMLResolver::resolve_generate_table(
    const ParseNode& table_node, const ObString& alias_name, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  ObSelectResolver select_resolver(params_);
  // from subquery is at same level as current query.
  select_resolver.set_current_level(current_level_);
  select_resolver.set_parent_namespace_resolver(parent_namespace_resolver_);
  if (OB_FAIL(do_resolve_generate_table(table_node, alias_name, select_resolver, table_item))) {
    LOG_WARN("do resolve generated table failed", K(ret));
  }
  return ret;
}

int ObDMLResolver::do_resolve_generate_table(const ParseNode& table_node, const ObString& alias_name,
    ObChildStmtResolver& child_resolver, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* ref_stmt = NULL;
  /* It's allowed to have columns with duplicated name in generated table in sel/upd/del stmt in oracle mode.
   * But it's not allowed to ref duplicated name column.
   * eg: select 1 from (select c1,c1 from t1);
   */
  bool can_skip = (share::is_oracle_mode() && get_stmt()->is_sel_del_upd());
  if (OB_FAIL(child_resolver.resolve_child_stmt(table_node))) {
    LOG_WARN("resolve select stmt failed", K(ret));
  } else if (OB_ISNULL(ref_stmt = child_resolver.get_child_stmt()) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resolve select stmt failed", K(ret));
  } else if (share::is_oracle_mode() && ref_stmt->has_for_update()) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("for update not allowed in from clause", K(ret));
  } else if (OB_FAIL(ObResolverUtils::check_duplicated_column(*ref_stmt, can_skip))) {
    // check duplicate column name for genereated table
    LOG_WARN("check duplicated column failed", K(ret));
  } else if (OB_FAIL(resolve_generate_table_item(ref_stmt, alias_name, table_item))) {
    LOG_WARN("resolve generate table item failed", K(ret));
  } else {
    LOG_DEBUG("finish do_resolve_generate_table", K(alias_name), KPC(table_item), KPC(table_item->ref_query_));
  }
  return ret;
}

int ObDMLResolver::resolve_generate_table_item(
    ObSelectStmt* ref_query, const ObString& alias_name, TableItem*& tbl_item)
{
  int ret = OB_SUCCESS;
  TableItem* item = NULL;
  ObDMLStmt* dml_stmt = get_stmt();
  if (OB_ISNULL(dml_stmt) || OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolver isn't init");
  } else if (OB_UNLIKELY(NULL == (item = dml_stmt->create_table_item(*allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create table item failed");
  } else {
    item->ref_query_ = ref_query;
    item->table_id_ = generate_table_id();
    item->table_name_ = alias_name;
    item->alias_name_ = alias_name;
    item->type_ = TableItem::GENERATED_TABLE;
    item->is_view_table_ = false;
    if (OB_FAIL(dml_stmt->add_table_item(session_info_, item, params_.have_same_table_name_))) {
      LOG_WARN("add table item failed", K(ret));
    } else {
      tbl_item = item;
    }
  }
  return ret;
}

int ObDMLResolver::resolve_function_table_item(const ParseNode& parse_tree, TableItem*& tbl_item)
{
  UNUSED(parse_tree);
  UNUSED(tbl_item);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObDMLResolver::resolve_base_or_alias_table_item_normal(uint64_t tenant_id, const ObString& db_name,
    const bool& is_db_explicit, const ObString& tbl_name, const ObString& alias_name, const ObString& synonym_name,
    const ObString& synonym_db_name, TableItem*& tbl_item, bool cte_table_fisrt)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  TableItem* item = NULL;
  const ObTableSchema* tschema = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolver isn't init", K(stmt), K_(schema_checker), K_(session_info), K_(allocator));
  } else if (OB_UNLIKELY(NULL == (item = stmt->create_table_item(*allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create table item failed");
  } else if (alias_name.length() > 0) {
    item->type_ = TableItem::ALIAS_TABLE;
  } else {
    item->type_ = TableItem::BASE_TABLE;
  }
  if (OB_SUCC(ret)) {
    item->synonym_name_ = synonym_name;
    item->synonym_db_name_ = synonym_db_name;
    item->database_name_ = db_name;
    bool select_index_enabled = false;
    uint64_t database_id = OB_INVALID_ID;
    if (OB_FAIL(session_info_->is_select_index_enabled(select_index_enabled))) {
      LOG_WARN("get select index status failed", K(ret));
    } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, db_name, database_id))) {
      LOG_WARN("get database id failed", K(ret));
    } else if (OB_FAIL(schema_checker_->get_table_schema(
                   tenant_id, database_id, tbl_name, false /*data table first*/, cte_table_fisrt, tschema))) {
      if (OB_TABLE_NOT_EXIST == ret && stmt->is_select_stmt() && select_index_enabled) {
        if (OB_FAIL(schema_checker_->get_table_schema(
                tenant_id, database_id, tbl_name, true /* for index table */, cte_table_fisrt, tschema))) {
          LOG_WARN("table or index doesn't exist", K(tenant_id), K(database_id), K(tbl_name), K(ret));
        }
      } else {
        LOG_WARN("table or index get schema failed", K(ret));
      }
    }

    // restrict accessible virtual table can not be use in sys tenant or sys view.
    if (OB_SUCC(ret) && tschema->is_vir_table() && is_restrict_access_virtual_table(tschema->get_table_id()) &&
        OB_SYS_TENANT_ID != session_info_->get_effective_tenant_id()) {
      bool in_sysview = false;
      if (OB_FAIL(check_in_sysview(in_sysview))) {
        LOG_WARN("check in sys view failed", K(ret));
      } else {
        if (!in_sysview) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("restrict accessible virtual table can not access directly", K(ret), K(db_name), K(tbl_name));
          LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(db_name), to_cstring(tbl_name));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (tschema->is_index_table() || tschema->is_materialized_view()) {
        // feature: select * from index_name where... rewrite to select index_col1, index_col2... from data_table_name
        // where... the feature only use in mysqtest case, not open for user
        const ObTableSchema* tab_schema = NULL;
        item->is_index_table_ = true;
        if (tschema->is_materialized_view()) {
          item->is_materialized_view_ = true;
        }
        item->ref_id_ = tschema->get_data_table_id();
        item->table_id_ = tschema->get_table_id();
        item->type_ = TableItem::ALIAS_TABLE;
        if (OB_FAIL(schema_checker_->get_table_schema(item->ref_id_, tab_schema))) {
          LOG_WARN("get data table schema failed", K(ret), K_(item->ref_id));
        } else {
          item->table_name_ = tab_schema->get_table_name_str();
          item->alias_name_ = tschema->get_table_name_str();
          ObSchemaObjVersion table_version;
          table_version.object_id_ = tab_schema->get_table_id();
          table_version.object_type_ = DEPENDENCY_TABLE;
          table_version.version_ = tab_schema->get_schema_version();
          table_version.is_db_explicit_ = is_db_explicit;
          if (with_clause_without_record_ || common::is_cte_table(table_version.object_id_)) {
            // do nothing
          } else if (OB_FAIL(stmt->add_global_dependency_table(table_version))) {
            LOG_WARN("add global dependency table failed", K(ret));
          }
        }
      } else {
        item->ref_id_ = tschema->get_table_id();
        item->table_name_ = tbl_name;
        item->is_system_table_ = tschema->is_sys_table();
        item->is_view_table_ = tschema->is_view_table();
        if (item->ref_id_ == OB_INVALID_ID) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(db_name), to_cstring(tbl_name));
        } else if (TableItem::BASE_TABLE == item->type_) {
          bool is_exist = false;
          if (OB_FAIL(stmt->check_table_id_exists(item->ref_id_, is_exist))) {
            LOG_WARN("check table id exists failed", K_(item->ref_id));
          } else if (is_exist) {
            // in the whole query stmt, table exists in the other query layer, so subquery must alias it
            // implicit alias table, alias name is table name
            item->table_id_ = generate_table_id();
            item->type_ = TableItem::ALIAS_TABLE;
            if (!synonym_name.empty()) {
              // bug: 31827906
              item->alias_name_ = synonym_name;
              item->database_name_ = synonym_db_name;
            } else {
              item->alias_name_ = tbl_name;
            }
          } else {
            // base table, no alias name
            item->table_id_ = item->ref_id_;
          }
        } else {
          // in order to be compatible with the old version(befor 2.1),
          // the dml statement needs to use ref_id as the table_id, even if it is an alias table
          bool use_ref_id =
              (stmt->is_dml_write_stmt(stmt->get_stmt_type()) && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2100);
          item->table_id_ = use_ref_id ? item->ref_id_ : generate_table_id();
          item->alias_name_ = alias_name;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObSchemaObjVersion table_version;
      table_version.object_id_ = tschema->get_table_id();
      table_version.object_type_ = tschema->is_view_table() ? DEPENDENCY_VIEW : DEPENDENCY_TABLE;
      table_version.version_ = tschema->get_schema_version();
      uint64_t version = -1;
      table_version.is_db_explicit_ = is_db_explicit;
      int64_t parallel_max_servers = 0;
      if (with_clause_without_record_ || common::is_cte_table(table_version.object_id_)) {
        // do nothing
      } else if (OB_FAIL(stmt->add_global_dependency_table(table_version))) {
        LOG_WARN("add global dependency table failed", K(ret));
      } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100 &&
                 (is_inner_table(tschema->get_table_id()) || session_info_->is_inner())) {
        // inner sql not use px before 3.1.0.0.
        stmt->get_query_ctx()->forbid_use_px_ = true;
        LOG_DEBUG("Forbid to use_px",
            K(params_.contain_dml_),
            K(is_inner_table(tschema->get_table_id())),
            K(session_info_->is_inner()));
      } else if (!GCONF._ob_enable_px_for_inner_sql &&
                 ObSQLSessionInfo::USER_SESSION != session_info_->get_session_type()) {
        stmt->get_query_ctx()->forbid_use_px_ = true;
      } else {
        // use px, including PL, inner sql, inner connection sql triggered by CMD
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->add_table_item(session_info_, item, params_.have_same_table_name_))) {
        LOG_WARN("push back table item failed", K(ret), KPC(item));
      } else {
        tbl_item = item;
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_base_or_alias_table_item_dblink(uint64_t dblink_id, const ObString& dblink_name,
    const ObString& database_name, const ObString& table_name, const ObString& alias_name, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  TableItem* item = NULL;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolver isn't init", K(stmt), K_(schema_checker), K_(session_info), K_(allocator));
  } else if (OB_ISNULL(item = stmt->create_table_item(*allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create table item failed");
  } else if (OB_FAIL(schema_checker_->get_link_table_schema(dblink_id, database_name, table_name, table_schema))) {
    LOG_WARN("get link table info failed", K(ret));
  } else {
    // common info.
    if (0 == alias_name.length()) {
      // item->table_id_ = table_schema->get_table_id();
      // table_id_ must be unique, ref_id_
      // ex: SELECT c_id FROM remote_dblink_test.stu2@my_link3 WHERE p_id = (SELECT MIN(p_id) FROM
      // remote_dblink_test.stu2@my_link3); parent table id and sub table id may same if table_id_ using
      // table_schema->get_table_id();
      item->table_id_ = generate_table_id();
      item->type_ = TableItem::BASE_TABLE;
    } else {
      item->table_id_ = generate_table_id();
      item->type_ = TableItem::ALIAS_TABLE;
      item->alias_name_ = alias_name;
    }
    item->ref_id_ = table_schema->get_table_id();
    item->table_name_ = table_name;
    item->is_index_table_ = false;
    item->is_system_table_ = false;
    item->is_view_table_ = false;
    item->is_materialized_view_ = false;
    // dblink info.
    item->dblink_name_ = dblink_name;
    item->link_database_name_ = database_name;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt->add_table_item(session_info_, item))) {
      LOG_WARN("push back table item failed", K(ret));
    } else {
      table_item = item;
    }
  }

  return ret;
}

int ObDMLResolver::expand_view(TableItem& view_item)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = share::is_oracle_mode();
  int64_t org_session_id = 0;
  if (!is_oracle_mode) {
    if (OB_ISNULL(params_.schema_checker_) || OB_ISNULL(params_.schema_checker_->get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null ptr", K(ret), KP(params_.schema_checker_));
    } else {
      // bug19839990, MySQL view resolver ignores temp table now.
      // view contains temp table is not supported.
      // update sess id here to avoid resolve table in view definition as temp table.
      org_session_id = params_.schema_checker_->get_schema_guard()->get_session_id();
      params_.schema_checker_->get_schema_guard()->set_session_id(0);
    }
  }
  if (OB_SUCC(ret)) {
    ObViewTableResolver view_resolver(params_, get_view_db_name(), get_view_name());
    view_resolver.set_current_level(current_level_);
    view_resolver.set_current_view_item(view_item);
    view_resolver.set_parent_namespace_resolver(parent_namespace_resolver_);
    if (OB_FAIL(do_expand_view(view_item, view_resolver))) {
      LOG_WARN("do expand view resolve failed", K(ret));
    }
    if (!is_oracle_mode) {
      params_.schema_checker_->get_schema_guard()->set_session_id(org_session_id);
    }
  }
  return ret;
}

int ObDMLResolver::do_expand_view(TableItem& view_item, ObChildStmtResolver& view_resolver)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();

  if (OB_ISNULL(stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt is null");
  } else {
    // expand view as subquery which use view name as alias
    ObSelectStmt* view_stmt = NULL;
    const ObTableSchema* view_schema = NULL;

    if (OB_FAIL(schema_checker_->get_table_schema(view_item.ref_id_, view_schema))) {
      LOG_WARN("get table schema failed", K(view_item));
    } else {
      // parse and resolve view defination
      ParseResult view_result;
      ObString view_def;

      ObParser parser(
          *params_.allocator_, session_info_->get_sql_mode(), session_info_->get_local_collation_connection());
      if (OB_FAIL(ObSQLUtils::generate_view_definition_for_resolve(*params_.allocator_,
              session_info_->get_local_collation_connection(),
              view_schema->get_view_schema(),
              view_def))) {
        LOG_WARN("fail to generate view definition for resolve", K(ret));
      } else if (OB_FAIL(parser.parse(view_def, view_result))) {
        LOG_WARN("parse view defination failed", K(view_def), K(ret));
      } else {
        // use alias to make all columns number continued
        // view is always in from clause, while not all properties of parents are available to
        // from subquery, so we can't give parent to from substmt.
        // select_resolver.set_upper_scope_stmt(stmt_);
        ParseNode* view_stmt_node = view_result.result_tree_->children_[0];
        if (OB_FAIL(view_resolver.resolve_child_stmt(*view_stmt_node))) {
          if (OB_TABLE_NOT_EXIST == ret || OB_ERR_BAD_FIELD_ERROR == ret || OB_NON_UNIQ_ERROR == ret) {
            ret = OB_ERR_VIEW_INVALID;
            const ObString& db_name = view_item.database_name_;
            const ObString& table_name = view_item.table_name_;
            LOG_USER_ERROR(OB_ERR_VIEW_INVALID, db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
          } else {
            LOG_WARN("expand view table failed", K(ret));
          }
        } else {
          view_stmt = view_resolver.get_child_stmt();
          view_stmt->set_is_view_stmt(true, view_item.ref_id_);
        }
      }
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < view_stmt->get_select_item_size(); ++i) {
          SelectItem& item = view_stmt->get_select_item(i);
          item.is_real_alias_ = true;
        }
      }

      // add child stmt view id
      if (OB_SUCC(ret)) {
        const ObDMLStmt::ObViewTableIds& view_ids = view_stmt->get_view_table_id_store();
        if (OB_FAIL(stmt->add_view_table_ids(view_ids))) {
          LOG_WARN("fail to add view table ids", K(view_ids), K(ret));
        }
      }

      // push-down view_stmt as sub-query for view_item
      if (OB_SUCC(ret)) {
        if (OB_FAIL(stmt->add_view_table_id(view_item.ref_id_))) {
          LOG_WARN("fail to add view table id", K(ret));
        } else {
          view_item.type_ = TableItem::GENERATED_TABLE;
          view_item.ref_query_ = view_stmt;
          view_item.ref_id_ = OB_INVALID_ID;
          view_item.is_view_table_ = true;
        }
      }
    }
  }

  return ret;
}

//
// wrapper for make the whole materialized view schema
//
int ObDMLResolver::make_materalized_view_schema(ObSelectStmt& select_stmt, ObTableSchema& view_schema)
{
  int ret = OB_SUCCESS;

  ObMaterializedViewContext mv_ctx;
  if (OB_FAIL(ObResolverUtils::find_base_and_depend_table(*schema_checker_, select_stmt, mv_ctx))) {
    LOG_WARN("find base and depend table failed", K(ret));
  } else if (OB_INVALID_ID == mv_ctx.base_table_id_ || OB_INVALID_ID == mv_ctx.depend_table_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table id", K(mv_ctx.base_table_id_), K(mv_ctx.depend_table_id_), K(ret));
  } else if (view_schema.get_data_table_id() != mv_ctx.base_table_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base table id is not match", K(view_schema.get_data_table_id()), K(mv_ctx.depend_table_id_), K(ret));
  } else if (OB_FAIL(view_schema.add_base_table_id(mv_ctx.base_table_id_)) ||
             OB_FAIL(view_schema.add_depend_table_id(mv_ctx.depend_table_id_))) {
    LOG_WARN("fail to add base and depend table id", K(mv_ctx.base_table_id_), K(mv_ctx.depend_table_id_), K(ret));
  } else {
    select_stmt.set_base_table_id(mv_ctx.base_table_id_);
    select_stmt.set_depend_table_id(mv_ctx.depend_table_id_);
    if (OB_FAIL(ObResolverUtils::check_materialized_view_limitation(select_stmt, mv_ctx))) {
      LOG_WARN("fail to check materialized view limitattion", K(ret));
    } else if (OB_FAIL(ObResolverUtils::make_columns_for_materialized_view(mv_ctx, *schema_checker_, view_schema))) {
      LOG_WARN("fail to make columns for mv", K(ret), K(mv_ctx));
    } else if (OB_FAIL(make_join_types_for_materalized_view(select_stmt, view_schema))) {
      LOG_WARN("fail to make join types for mv", K(ret));
    } else if (OB_FAIL(make_join_conds_for_materialized_view(select_stmt, view_schema))) {
      LOG_WARN("fail to make join conds for mv", K(ret));
    }
  }
  LOG_INFO("dump mv with columns", K(view_schema));

  return ret;
}

//
// join types
// support two modes
// t1 inner join t2, construct from joined_tables
// t1, t2, construct from table items
//
int ObDMLResolver::make_join_types_for_materalized_view(ObSelectStmt& select_stmt, ObTableSchema& view_schema)
{
  int ret = OB_SUCCESS;

  if (select_stmt.get_joined_tables().count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt.get_joined_tables().count(); ++i) {
      if (OB_FAIL(convert_join_types_for_schema(*select_stmt.get_joined_tables().at(i), view_schema))) {
        LOG_WARN("fail to convert join types for schema", "node", select_stmt.get_joined_tables().at(i));
      }
    }
  } else {
    if (select_stmt.get_table_size() == 2) {
      TableJoinType join_item;
      join_item.table_pair_ =
          std::make_pair(select_stmt.get_table_item(0)->ref_id_, select_stmt.get_table_item(1)->ref_id_);
      join_item.join_type_ = INNER_JOIN;
      if (OB_FAIL(view_schema.get_join_types().push_back(join_item))) {
        LOG_WARN("fail to push back join item", K(ret), K(join_item));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only two join tables supported", K(ret), "tables", select_stmt.get_table_size());
    }
  }
  LOG_INFO("mv join types", K(view_schema.get_join_types()));

  return ret;
}

int ObDMLResolver::convert_join_types_for_schema(JoinedTable& join_table, ObTableSchema& view_schema)
{
  int ret = OB_SUCCESS;
  if (NULL != join_table.left_table_ && !join_table.left_table_->is_joined_table() && join_table.right_table_ != NULL &&
      !join_table.right_table_->is_joined_table()) {

    TableJoinType join_item;
    join_item.table_pair_ = std::make_pair(join_table.left_table_->table_id_, join_table.right_table_->table_id_);
    join_item.join_type_ = join_table.joined_type_;
    view_schema.get_join_types().push_back(join_item);
  } else {
    ret = OB_NOT_SUPPORTED;
#if 0
      /* don't delete */
      /* handle nestest case later */
      if (NULL != join_table.left_table_ && join_table.left_table_->is_joined_table()) {
        JoinedTable &left_table = static_cast<JoinedTable&>(*join_table.left_table_);
        if (OB_FAIL(convert_join_exprs_for_schema(left_table, view_schema))) {
          LOG_WARN("fail to convert left join table", K(ret), "left node", left_table);
        }
      } else {
      }

      if (OB_SUCC(ret) && join_table.right_table_ != NULL && join_table.right_table_->is_joined_table()) {
        JoinedTable &right_table = static_cast<JoinedTable&>(*join_table.right_table_);
        if (OB_FAIL(convert_join_exprs_for_schema(right_table, view_schema))) {
          LOG_WARN("fail to convert right join table", K(ret), "right node", right_table);
        }
      } else {
      }
#endif
  }
  return ret;
}

//
// add join table expr
//
int ObDMLResolver::make_join_conds_for_materialized_view(ObSelectStmt& select_stmt, ObTableSchema& view_schema)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> relation_exprs;
  RelExprChecker expr_checker(relation_exprs);

  if (OB_FAIL(expr_checker.init())) {
    LOG_WARN("init relexpr checker failed", K(ret));
  } else if (select_stmt.get_joined_tables().count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt.get_joined_tables().count(); ++i) {
      if (OB_FAIL(select_stmt.get_join_condition_expr(*select_stmt.get_joined_tables().at(i), expr_checker))) {
        LOG_WARN("fail to get join conds expr", K(ret), K(i), "count", select_stmt.get_joined_tables().count());
      } else {
        ObArray<ObRawExpr*>& cond_exprs = relation_exprs;
        LOG_INFO("join conditions", K(cond_exprs));
        for (int64_t i = 0; OB_SUCC(ret) && i < cond_exprs.count(); i++) {
          if (OB_FAIL(convert_cond_exprs_for_schema(*cond_exprs.at(i), view_schema))) {
            LOG_WARN("fail to convert join expr for schema", K(cond_exprs), K(view_schema.get_join_conds()));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObIArray<ObRawExpr*>& cond_exprs = select_stmt.get_condition_exprs();
    LOG_INFO("where conditions", K(cond_exprs));
    for (int64_t i = 0; OB_SUCC(ret) && i < cond_exprs.count(); i++) {
      if (OB_FAIL(convert_cond_exprs_for_schema(*cond_exprs.at(i), view_schema))) {
        LOG_WARN("fail to convert join expr for schema", K(cond_exprs), K(view_schema.get_join_conds()));
      }
    }
  }
  LOG_INFO("mv join conditions", K(view_schema.get_join_conds()));

  return ret;
}

int ObDMLResolver::convert_cond_exprs_for_schema(ObRawExpr& relation_exprs, ObTableSchema& view_schema)
{
  int ret = OB_SUCCESS;
  ObRawExpr& expr = relation_exprs;

  if (expr.get_expr_type() != T_OP_AND && expr.get_expr_type() != T_OP_EQ) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported in join condition", K(ret), K(expr.get_expr_type()), K(expr));
  } else {
    for (int64_t i = 0; i < 2; i++) {
      if (OB_ISNULL(expr.get_param_expr(i))) {
      } else if (expr.get_param_expr(i)->get_expr_type() == T_OP_AND) {
        if (OB_FAIL(SMART_CALL(convert_cond_exprs_for_schema(*expr.get_param_expr(i), view_schema)))) {
          LOG_WARN("convert cond exprs for schema failed", K(ret));
        }
      } else if (expr.get_param_expr(i)->get_expr_type() == T_REF_COLUMN) {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr.get_param_expr(i));
        view_schema.get_join_conds().push_back(std::make_pair(col_expr->get_table_id(), col_expr->get_column_id()));
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("only basic column ref and AND supported in join condition",
            K(ret),
            "type",
            expr.get_param_expr(i)->get_expr_type());
      }
    }
  }
  return ret;
}

int ObDMLResolver::create_force_index_hint(const uint64_t table_id, const ObString& index_name)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  ObStmtHint& stmt_hint = stmt->get_stmt_hint();
  ObIndexHint index_hint;
  index_hint.type_ = ObIndexHint::FORCE;
  index_hint.table_id_ = table_id;
  if (OB_FAIL(index_hint.index_list_.push_back(index_name))) {
    LOG_WARN("push back index name failed", K(ret));
  } else if (OB_FAIL(stmt_hint.indexes_.push_back(index_hint))) {
    LOG_WARN("push back index hint failed", K(ret));
  }
  return ret;
}

int ObDMLResolver::build_partition_key_info(const ObTableSchema& table_schema, ObRawExpr*& part_expr)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> qualified_names;
  ObRawExpr* partition_key_expr = NULL;
  if (OB_FAIL(ObResolverUtils::build_partition_key_expr(params_, table_schema, partition_key_expr, NULL, false))) {
    LOG_WARN("failed to build partition key expr!", K(ret));
  } else {
    if (qualified_names.count() > 0) {
      if (OB_FAIL(resolve_columns(partition_key_expr, qualified_names))) {
        LOG_WARN("failed to resolve columns", K(ret));
      } else if (OB_FAIL(partition_key_expr->formalize(session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else {
        part_expr = partition_key_expr;
      }
    } else {
      // no primary key, error now
      ret = OB_ERR_PARSE_SQL;
      LOG_WARN("failed to build partition key info");
    }
  }
  return ret;
}

int ObDMLResolver::resolve_table_partition_expr(const TableItem& table_item, const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  const ObString& part_str = table_schema.get_part_option().get_part_func_expr_str();
  ObPartitionFuncType part_type = table_schema.get_part_option().get_part_func_type();
  ObRawExpr* part_expr = NULL;
  ObRawExpr* subpart_expr = NULL;
  ObDMLStmt* dml_stmt = get_stmt();
  if (OB_ISNULL(dml_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("dml_stmt is null");
  } else if (table_schema.get_part_level() != PARTITION_LEVEL_ZERO) {
    if (OB_FAIL(resolve_partition_expr(table_item, table_schema, part_type, part_str, part_expr))) {
      LOG_WARN("Failed to resolve partition expr", K(ret), K(part_str), K(part_type));
    } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
      const ObString& subpart_str = table_schema.get_sub_part_option().get_part_func_expr_str();
      ObPartitionFuncType subpart_type = table_schema.get_sub_part_option().get_part_func_type();
      if (OB_FAIL(resolve_partition_expr(table_item, table_schema, subpart_type, subpart_str, subpart_expr))) {
        LOG_WARN("Failed to resolve partition expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dml_stmt->set_part_expr(table_item.table_id_, table_item.ref_id_, part_expr, subpart_expr))) {
        LOG_WARN("set part expr to dml stmt failed", K(ret));
      } else {
        LOG_TRACE("resolve partition expr", K(*part_expr), K(part_str));
      }
    }
  }
  // resolve global index table partition expr
  if (OB_SUCC(ret)) {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    if (OB_FAIL(table_schema.get_simple_index_infos_without_delay_deleted_tid(simple_index_infos, false))) {
      LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema* index_schema = NULL;
      if (OB_FAIL(schema_checker_->get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("get index schema from schema checker failed", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table not exists", K(simple_index_infos.at(i).table_id_));
      } else if (index_schema->is_final_invalid_index() || !index_schema->is_global_index_table()) {
        // do nothing
      } else if (index_schema->get_part_level() != PARTITION_LEVEL_ZERO) {
        ObPartitionFuncType index_part_type = index_schema->get_part_option().get_part_func_type();
        const ObString& index_part_str = index_schema->get_part_option().get_part_func_expr_str();
        ObRawExpr* index_part_expr = NULL;
        ObRawExpr* index_subpart_expr = NULL;
        if (OB_FAIL(
                resolve_partition_expr(table_item, table_schema, index_part_type, index_part_str, index_part_expr))) {
          LOG_WARN("resolve global index table partition expr failed", K(ret), K(index_part_str), K(index_part_type));
        } else if (OB_FAIL(PARTITION_LEVEL_TWO == index_schema->get_part_level())) {
          ObPartitionFuncType index_subpart_type = index_schema->get_sub_part_option().get_part_func_type();
          const ObString& index_subpart_str = index_schema->get_sub_part_option().get_part_func_expr_str();
          if (OB_FAIL(resolve_partition_expr(
                  table_item, table_schema, index_subpart_type, index_subpart_str, index_subpart_expr))) {
            LOG_WARN("resolve subpart expr failed", K(ret), K(index_subpart_str), K(index_subpart_type));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(dml_stmt->set_part_expr(
                  table_item.table_id_, index_schema->get_table_id(), index_part_expr, index_subpart_expr))) {
            LOG_WARN("set part expr to dml stmt failed", K(ret), K(index_schema->get_table_id()));
          }
        }
      }
    }
  }
  return ret;
}

// for recursively process columns item in resolve_partition_expr
// just wrap columns process logic in resolve_partition_expr
int ObDMLResolver::resolve_columns_for_partition_expr(
    ObRawExpr*& expr, ObIArray<ObQualifiedName>& columns, const TableItem& table_item, bool include_hidden)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> real_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    ObQualifiedName& q_name = columns.at(i);
    ObRawExpr* real_ref_expr = NULL;
    if (q_name.is_sys_func()) {
      if (OB_FAIL(resolve_qualified_identifier(q_name, columns, real_exprs, real_ref_expr))) {
        LOG_WARN("resolve sysfunc expr failed", K(q_name), K(ret));
      } else if (OB_ISNULL(real_ref_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(ret));
      } else if (!real_ref_expr->is_sys_func_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid exor", K(*real_ref_expr), K(ret));
      } else {
        ObSysFunRawExpr* sys_func_expr = static_cast<ObSysFunRawExpr*>(real_ref_expr);
        if (OB_FAIL(sys_func_expr->check_param_num())) {
          LOG_WARN("sys func check param failed", K(ret));
        }
      }
    } else {
      ColumnItem* column_item = NULL;
      if (OB_FAIL(resolve_basic_column_item(table_item, q_name.col_name_, include_hidden, column_item))) {
        LOG_WARN("resolve basic column item failed", K(i), K(q_name), K(ret));
      } else {
        real_ref_expr = column_item->expr_;
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < real_exprs.count(); ++i) {
        OZ(ObRawExprUtils::replace_ref_column(real_ref_expr, columns.at(i).ref_expr_, real_exprs.at(i)));
      }
      OZ(real_exprs.push_back(real_ref_expr));
      OZ(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, real_ref_expr));
    }
  }
  return ret;
}

int ObDMLResolver::resolve_partition_expr(const TableItem& table_item, const ObTableSchema& table_schema,
    const ObPartitionFuncType part_type, const ObString& part_str, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;

  // for special case key()
  if (OB_ISNULL(params_.session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null pointer", K(ret));
  } else if (OB_UNLIKELY(part_type >= PARTITION_FUNC_TYPE_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Error part type", K(ret));
  } else if (PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_type || PARTITION_FUNC_TYPE_KEY_IMPLICIT_V2 == part_type) {
    if (OB_FAIL(ObResolverUtils::build_partition_key_expr(
            params_, table_schema, expr, &columns, PARTITION_FUNC_TYPE_KEY_IMPLICIT_V2 == part_type))) {
      LOG_WARN("failed to build partition key expr!", K(ret));
    }
  } else if (OB_UNLIKELY(part_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Part string should not be empty", K(ret));
  } else {
    ObSqlString sql_str;
    ParseResult parse_result;
    ParseNode* stmt_node = NULL;
    ParseNode* select_node = NULL;
    ParseNode* select_expr_list = NULL;
    ParseNode* select_expr_node = NULL;
    ParseNode* part_expr_node = NULL;
    ObSQLMode sql_mode = params_.session_info_->get_sql_mode();
    if (share::is_oracle_mode()) {
      sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
    }
    ObParser parser(*allocator_, sql_mode);
    LOG_DEBUG("resolve partition expr", K(sql_mode), K(table_schema.get_tenant_id()));
    if (is_inner_table(table_schema.get_table_id())) {
      if (OB_FAIL(sql_str.append_fmt("SELECT partition_%.*s FROM DUAL", part_str.length(), part_str.ptr()))) {
        LOG_WARN("fail to concat string", K(part_str), K(ret));
      }
    } else if (PARTITION_FUNC_TYPE_KEY == part_type) {
      if (OB_FAIL(sql_str.append_fmt("SELECT partition_%.*s FROM DUAL", part_str.length(), part_str.ptr()))) {
        LOG_WARN("fail to concat string", K(part_str), K(ret));
      }
    } else if (PARTITION_FUNC_TYPE_KEY_V2 == part_type) {  // TODO
      if (OB_FAIL(sql_str.append_fmt("SELECT %s(%.*s) FROM DUAL", N_PART_KEY_V2, part_str.length(), part_str.ptr()))) {
        LOG_WARN("fail to concat string", K(part_str), K(ret));
      }
    } else if (PARTITION_FUNC_TYPE_KEY_V3 == part_type) {
      if (OB_FAIL(sql_str.append_fmt("SELECT %s(%.*s) FROM DUAL", N_PART_KEY_V3, part_str.length(), part_str.ptr()))) {
        LOG_WARN("fail to concat string", K(part_str), K(ret));
      }
    } else if (share::is_oracle_mode() && PARTITION_FUNC_TYPE_HASH == part_type) {
      if (OB_FAIL(sql_str.append_fmt("SELECT %s(%.*s) FROM DUAL", N_PART_HASH_V1, part_str.length(), part_str.ptr()))) {
        LOG_WARN("fail to concat string", K(part_str), K(ret));
      }
    } else if (share::is_oracle_mode() && PARTITION_FUNC_TYPE_HASH_V2 == part_type) {
      if (OB_FAIL(sql_str.append_fmt("SELECT %s(%.*s) FROM DUAL", N_PART_HASH_V2, part_str.length(), part_str.ptr()))) {
        LOG_WARN("fail to concat string", K(part_str), K(ret));
      }
    } else {
      // need to wrap some keywords with double quotes in oracle mode to avoid they are
      // resolved as function, such as current_date
      if (share::is_oracle_mode()) {
        ObArenaAllocator calc_buf(ObModIds::OB_SQL_PARSER);
        ObString new_part_str;
        if (OB_FAIL(process_part_str(calc_buf, part_str, new_part_str))) {
          LOG_WARN("failed to process part str");
        } else if (OB_FAIL(sql_str.append_fmt("SELECT (%.*s) FROM DUAL", new_part_str.length(), new_part_str.ptr()))) {
          LOG_WARN("fail to concat string", K(part_str), K(ret));
        } else { /*do nothing*/
        }
      } else if (OB_FAIL(sql_str.append_fmt("SELECT (%.*s) FROM DUAL", part_str.length(), part_str.ptr()))) {
        LOG_WARN("fail to concat string", K(part_str), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(parser.parse(sql_str.string(), parse_result))) {
      ret = OB_ERR_PARSE_SQL;
      _OB_LOG(WARN,
          "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], line_[%d], yycolumn[%d], yylineno_[%d], "
          "sql[%.*s]",
          parse_result.yyscan_info_,
          parse_result.result_tree_,
          parse_result.malloc_pool_,
          parse_result.error_msg_,
          parse_result.start_col_,
          parse_result.end_col_,
          parse_result.line_,
          parse_result.yycolumn_,
          parse_result.yylineno_,
          static_cast<int>(sql_str.length()),
          sql_str.ptr());
    } else {
      if (OB_ISNULL(stmt_node = parse_result.result_tree_) || OB_UNLIKELY(stmt_node->type_ != T_STMT_LIST)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt node is invalid", K(stmt_node));
      } else if (OB_ISNULL(select_node = stmt_node->children_[0]) || OB_UNLIKELY(select_node->type_ != T_SELECT)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select node is invalid", K(select_node));
      } else if (OB_ISNULL(select_expr_list = select_node->children_[PARSE_SELECT_SELECT]) ||
                 OB_UNLIKELY(select_expr_list->type_ != T_PROJECT_LIST)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr list is invalid", K(ret));
      } else if (OB_ISNULL(select_expr_node = select_expr_list->children_[0]) ||
                 OB_UNLIKELY(select_expr_node->type_ != T_PROJECT_STRING)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr node is invalid", K(ret));
      } else if (OB_ISNULL(part_expr_node = select_expr_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part expr node is invalid", K(part_expr_node));
      } else if (OB_FAIL(resolve_partition_expr(*part_expr_node, expr, columns))) {
        LOG_WARN("resolve partition expr failed", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      }
      // destroy syntax tree
      parser.free_result(parse_result);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_columns_for_partition_expr(expr, columns, table_item, table_schema.is_oracle_tmp_table()))) {
      LOG_WARN("resolve columns for partition expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr->formalize(session_info_))) {
      LOG_WARN("formalize expr failed", K(ret));
    }
  }
  return ret;
}

int ObDMLResolver::resolve_partition_expr(
    const ParseNode& part_expr_node, ObRawExpr*& expr, ObIArray<ObQualifiedName>& columns)
{
  int ret = OB_SUCCESS;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObOpRawExpr*> op_exprs;
  ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObCharsetType character_set_connection = CHARSET_INVALID;
  if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(params_.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params_.expr_factory), K_(params_.session_info));
  } else if (OB_FAIL(params_.session_info_->get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(params_.session_info_->get_character_set_connection(character_set_connection))) {
    LOG_WARN("fail to get character_set_connection", K(ret));
  } else {
    ObExprResolveContext ctx(*params_.expr_factory_, params_.session_info_->get_timezone_info(), OB_NAME_CASE_INVALID);
    ctx.dest_collation_ = collation_connection;
    ctx.connection_charset_ = character_set_connection;
    ctx.param_list_ = params_.param_list_;
    ctx.stmt_ = static_cast<ObStmt*>(get_stmt());
    ctx.session_info_ = params_.session_info_;
    ctx.query_ctx_ = params_.query_ctx_;
    ObRawExprResolverImpl expr_resolver(ctx);
    if (OB_FAIL(params_.session_info_->get_name_case_mode(ctx.case_mode_))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(expr_resolver.resolve(&part_expr_node,
                   expr,
                   columns,
                   sys_vars,
                   sub_query_info,
                   aggr_exprs,
                   win_exprs,
                   op_exprs,
                   user_var_exprs))) {
      LOG_WARN("resolve expr failed", K(ret));
    } else if (sub_query_info.count() > 0 || sys_vars.count() > 0 || aggr_exprs.count() > 0 || columns.count() <= 0 ||
               op_exprs.count() > 0) {
      ret = OB_ERR_UNEXPECTED;  // TODO Molly not allow type cast in part expr?
      LOG_WARN("part expr is invalid",
          K(sub_query_info.count()),
          K(sys_vars.count()),
          K(aggr_exprs.count()),
          K(columns.count()));
    }
  }
  return ret;
}

int ObDMLResolver::process_values_function(ObRawExpr*& expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObDMLResolver::resolve_is_expr(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* is_expr = dynamic_cast<ObOpRawExpr*>(expr);
  ObDMLStmt* stmt = get_stmt();
  int64_t num_expr = expr->get_param_count();
  ColumnItem* column_item = NULL;
  if (3 != num_expr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is_expr must have three sub_expr", K(num_expr));
  } else if (NULL == is_expr->get_param_expr(0) || NULL == is_expr->get_param_expr(1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left argument and right argument can not be null", K(ret));
  } else if (T_REF_COLUMN == is_expr->get_param_expr(0)->get_expr_type()) {
    ObColumnRefRawExpr* ref_expr = static_cast<ObColumnRefRawExpr*>(is_expr->get_param_expr(0));
    ObConstRawExpr* flag_expr = static_cast<ObConstRawExpr*>(is_expr->get_param_expr(2));
    if (NULL == (column_item = stmt->get_column_item_by_id(ref_expr->get_table_id(), ref_expr->get_column_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get column item failed");
    } else if (OB_SUCCESS == ret && NULL != column_item) {
      // T_OP_IS has two parameters at firat. The third parameter is for a special case:
      // If it's a date or datetime column and is a not null column, then we can use following query:
      // select * from t1 where c1 is null; to get values like '0000-00-00'.
      if ((column_item->get_column_type()->is_date() || column_item->get_column_type()->is_datetime()) &&
          !share::is_oracle_mode() && column_item->is_not_null()) {
        flag_expr->get_value().set_bool(true);
        flag_expr->set_expr_obj_meta(flag_expr->get_value().get_meta());
        if (OB_FAIL(flag_expr->formalize(session_info_))) {
          LOG_WARN("formalize expr failed", K(ret));
        }
      } else if (column_item->is_auto_increment() && T_NULL == is_expr->get_param_expr(1)->get_expr_type()) {
        if (OB_FAIL(resolve_autoincrement_column_is_null(expr))) {
          LOG_WARN("fail to process autoincrement column is null", K(ret));
        } else {
          stmt->set_affected_last_insert_id(true);
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_special_expr(ObRawExpr*& expr, ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  bool is_found = false;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(stmt));
  } else if (expr->has_flag(CNT_LAST_INSERT_ID) || expr->has_flag(IS_LAST_INSERT_ID)) {
    stmt->set_affected_last_insert_id(true);
  }
  if (OB_SUCC(ret) && (expr->has_flag(CNT_DEFAULT) || expr->has_flag(CNT_DOMAIN_INDEX_FUNC) ||
                          (expr->has_flag(CNT_IS_EXPR) && T_WHERE_SCOPE == scope) || gen_col_exprs_.count() > 0)) {
    // search for matched generated column template first.
    if (OB_FAIL(find_generated_column_expr(expr, is_found))) {
      LOG_WARN("find generated column expr failed", K(ret));
    } else if (!is_found) {
      if (expr->has_flag(IS_DEFAULT)) {
        ObDefaultValueUtils utils(stmt, &params_, this);
        if (OB_FAIL(utils.resolve_default_function(expr, scope))) {
          LOG_WARN("fail to resolve default expr", K(ret), K(*expr));
        }
      } else if (T_OP_IS == expr->get_expr_type()) {
        if (OB_FAIL(resolve_is_expr(expr))) {
          LOG_WARN("resolve special is_expr failed", K(ret));
        }
      } else if (T_FUN_MATCH_AGAINST == expr->get_expr_type()) {
        if (T_WHERE_SCOPE != scope) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "MATCH in the scope");
        } else if (OB_FAIL(resolve_fun_match_against_expr(static_cast<ObFunMatchAgainst&>(*expr)))) {
          LOG_WARN("resolve fun match against expr failed", K(ret));
        } else { /*do nothing*/
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
        if (OB_FAIL(SMART_CALL(resolve_special_expr(expr->get_param_expr(i), scope)))) {
          LOG_WARN("resolve special expr failed", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

// build next_val expr; set expr as its param
int ObDMLResolver::build_autoinc_nextval_expr(ObRawExpr*& expr, uint64_t autoinc_col_id)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* func_expr = NULL;
  ObConstRawExpr* col_id_expr = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K_(session_info), K_(params_.expr_factory));
  } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_FUN_SYS_AUTOINC_NEXTVAL, func_expr))) {
    LOG_WARN("create nextval failed", K(ret));
  } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_UINT64, col_id_expr))) {
    LOG_WARN("create const raw expr failed", K(ret));
  } else {
    ObObj col_id;
    func_expr->set_func_name(ObString::make_string(N_AUTOINC_NEXTVAL));
    col_id.set_uint64(autoinc_col_id);
    col_id_expr->set_value(col_id);
    if (OB_FAIL(func_expr->add_param_expr(col_id_expr))) {
      LOG_WARN("set funcation param expr failed", K(ret));
    } else if (NULL != expr && OB_FAIL(func_expr->add_param_expr(expr))) {
      LOG_WARN("add function param expr failed", K(ret));
    } else if (OB_FAIL(func_expr->formalize(session_info_))) {
      LOG_WARN("failed to extract info", K(ret));
    } else {
      expr = func_expr;
    }
  }
  return ret;
}

// build oracle sequence_object.currval, sequence_object.nextval expr
int ObDMLResolver::build_seq_nextval_expr(ObRawExpr*& expr, const ObQualifiedName& q_name, uint64_t seq_id)
{
  int ret = OB_SUCCESS;
  ObRawExpr* exists_seq_expr = NULL;
  ObSequenceRawExpr* func_expr = NULL;
  ObConstRawExpr* col_id_expr = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(params_.expr_factory_) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K_(session_info), K_(params_.expr_factory), K(get_stmt()));
  } else if (OB_FAIL(get_stmt()->get_sequence_expr(exists_seq_expr, q_name.tbl_name_, q_name.col_name_, seq_id))) {
    LOG_WARN("failed to get sequence expr", K(ret));
  } else if (exists_seq_expr != NULL) {
    expr = exists_seq_expr;
  } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_FUN_SYS_SEQ_NEXTVAL, func_expr))) {
    LOG_WARN("create nextval failed", K(ret));
  } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_UINT64, col_id_expr))) {
    LOG_WARN("create const raw expr failed", K(ret));
  } else {
    ObObj col_id;
    func_expr->set_sequence_meta(q_name.tbl_name_, q_name.col_name_, seq_id);
    func_expr->add_flag(IS_SEQ_EXPR);
    col_id.set_uint64(seq_id);
    col_id_expr->set_value(col_id);
    if (OB_FAIL(func_expr->add_param_expr(col_id_expr))) {
      LOG_WARN("set funcation param expr failed", K(ret));
    } else if (OB_FAIL(func_expr->formalize(session_info_))) {
      LOG_WARN("failed to extract info", K(ret));
    } else if (OB_FAIL(get_stmt()->get_pseudo_column_like_exprs().push_back(func_expr))) {
      LOG_WARN("failed to push back sequence expr", K(ret));
    } else {
      expr = func_expr;
    }
  }
  return ret;
}

// build partid expr; set expr as its param
int ObDMLResolver::build_partid_expr(ObRawExpr*& expr, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* func_expr = NULL;
  ObConstRawExpr* table_id_expr = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K_(session_info), K_(params_.expr_factory));
  } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_FUN_SYS_PART_ID, func_expr))) {
    LOG_WARN("create part_id failed", K(ret));
  } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_UINT64, table_id_expr))) {
    LOG_WARN("create const raw expr failed", K(ret));
  } else {
    ObObj tid;
    tid.set_uint64(table_id);
    table_id_expr->set_value(tid);
    if (OB_FAIL(func_expr->add_param_expr(table_id_expr))) {
      LOG_WARN("add_param_expr failed", K(ret));
    } else if (OB_FAIL(func_expr->formalize(session_info_))) {
      LOG_WARN("failed to extract info", K(ret));
    } else {
      expr = func_expr;
    }
  }
  return ret;
}

int ObDMLResolver::resolve_all_basic_table_columns(
    const TableItem& table_item, bool include_hidden, ObIArray<ColumnItem>* column_items)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  ColumnItem* col_item = NULL;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolver status is invalid", K_(schema_checker), K(stmt));
  } else if (OB_UNLIKELY(!table_item.is_basic_table()) && OB_UNLIKELY(!table_item.is_fake_cte_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table isn't basic table", K_(table_item.type));
  } else {
    const ObTableSchema* table_schema = NULL;
    // If select table is as index table, then * should be all columns of index table instead of primary table.
    uint64_t table_id = table_item.is_index_table_ ? table_item.table_id_ : table_item.ref_id_;
    if (OB_FAIL(schema_checker_->get_table_schema(table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(table_item.ref_id_));
    } else {
      ObColumnIterByPrevNextID iter(*table_schema);
      const ObColumnSchemaV2* column_schema = NULL;
      while (OB_SUCC(ret) && OB_SUCC(iter.next(column_schema))) {
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The column is null", K(ret));
        } else if (column_schema->is_shadow_column()) {
          // don't show shadow columns for select * from idx
          continue;
        } else if (column_schema->is_invisible_column()) {
          // don't show invisible columns for select * from tbl_name
          continue;
        } else if (!include_hidden && column_schema->is_hidden()) {
          // jump hidden column, but if it is sync ddl user,  not jump __pk_increment
          continue;
        } else if (OB_FAIL(resolve_basic_column_item(
                       table_item, column_schema->get_column_name_str(), include_hidden, col_item))) {
          LOG_WARN("resolve column item failed", K(ret));
        } else if (column_items != NULL) {
          if (OB_FAIL(column_items->push_back(*col_item))) {
            LOG_WARN("push back column item failed", K(ret));
          }
        }
      }
      if (ret != OB_ITER_END) {
        LOG_WARN("Failed to iterate all table columns. iter quit. ", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_all_generated_table_columns(const TableItem& table_item, ObIArray<ColumnItem>& column_items)
{
  int ret = OB_SUCCESS;
  auto stmt = get_stmt();
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!table_item.is_generated_table() || OB_ISNULL(table_item.ref_query_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not generated table or ref query is NULL", K(ret));
  } else {
    const ObString column_name;  // not used, keep empty
    for (int64_t i = 0; OB_SUCC(ret) && i < table_item.ref_query_->get_select_item_size(); i++) {
      const uint64_t col_id = OB_APP_MIN_COLUMN_ID + i;
      ColumnItem* col_item = NULL;
      if (OB_FAIL(resolve_generated_table_column_item(table_item, column_name, col_item, stmt, col_id))) {
        LOG_WARN("resolve generate table item failed", K(ret));
      } else if (OB_ISNULL(col_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is NULL", K(ret));
      } else if (OB_FAIL(column_items.push_back(*col_item))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_and_split_sql_expr(const ParseNode& node, ObIArray<ObRawExpr*>& and_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr = NULL;
  // where condition will be canonicalized, all continous AND will be merged
  if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("dml resolve not init", K_(params_.expr_factory), K_(session_info));
  } else if (node.type_ != T_OP_AND) {
    ObExprResolveContext ctx(*params_.expr_factory_, session_info_->get_timezone_info(), OB_NAME_CASE_INVALID);
    ctx.stmt_ = static_cast<ObStmt*>(get_stmt());
    ctx.query_ctx_ = params_.query_ctx_;
    ctx.session_info_ = params_.session_info_;
    ObRawExprCanonicalizerImpl canonicalizer(ctx);
    if (OB_FAIL(resolve_sql_expr(node, expr))) {
      LOG_WARN("resolve sql expr failed", K(ret));
    } else if (OB_FAIL(canonicalizer.canonicalize(expr))) {  // canonicalize expression
      LOG_WARN("resolve canonicalize expression", K(ret));
    } else if (expr->get_expr_type() == T_OP_AND) {
      // no T_OP_AND under another T_OP_AND, which is ensured by canonicalize
      ObOpRawExpr* and_expr = static_cast<ObOpRawExpr*>(expr);
      for (int64_t i = 0; OB_SUCC(ret) && i < and_expr->get_param_count(); i++) {
        ObRawExpr* sub_expr = and_expr->get_param_expr(i);
        OZ((and_exprs.push_back)(sub_expr));
      }
    } else {
      OZ((and_exprs.push_back)(expr));
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < node.num_child_; i++) {
      if (OB_FAIL(SMART_CALL(resolve_and_split_sql_expr(*(node.children_[i]), and_exprs)))) {
        LOG_WARN("resolve and split sql expr failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

// resolve all condition exprs and add bool expr before if needed.
int ObDMLResolver::resolve_and_split_sql_expr_with_bool_expr(const ParseNode& node, ObIArray<ObRawExpr*>& and_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params_.expr_factory_ or session_info_ is NULL", K(ret));
  } else if (OB_FAIL(resolve_and_split_sql_expr(node, and_exprs))) {
    LOG_WARN("resolve_and_split_sql_expr failed", K(ret));
  } else if (session_info_->use_static_typing_engine()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < and_exprs.count(); ++i) {
      ObRawExpr* new_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::try_create_bool_expr(and_exprs.at(i), new_expr, *params_.expr_factory_))) {
        LOG_WARN("try create bool expr failed", K(ret), K(i));
      } else {
        and_exprs.at(i) = new_expr;
      }
    }
  }
  return ret;
}

int ObDMLResolver::add_all_columns_to_stmt(const TableItem& table_item, ObIArray<ObColumnRefRawExpr*>& column_exprs)
{
  int ret = OB_SUCCESS;

  const ObTableSchema* table_schema = NULL;
  uint64_t base_table_id = table_item.get_base_table_item().ref_id_;
  if (OB_FAIL(schema_checker_->get_table_schema(base_table_id, table_schema))) {
    LOG_WARN("not find table schema", K(ret), K(table_item), K(base_table_id));
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get illegal table schema", K(table_schema));
  } else {
    ObTableSchema::const_column_iterator iter = table_schema->column_begin();
    ObTableSchema::const_column_iterator end = table_schema->column_end();
    for (; OB_SUCC(ret) && iter != end; ++iter) {
      const ObColumnSchemaV2* column = *iter;
      if (NULL == column) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column schema", K(column));
      } else if (OB_FAIL(add_column_to_stmt(table_item, *column, column_exprs))) {
        LOG_WARN("add column item to stmt failed", K(ret));
      }
    }  // end for
  }
  return ret;
}

int ObDMLResolver::add_column_to_stmt(const TableItem& table_item, const share::schema::ObColumnSchemaV2& col,
    common::ObIArray<ObColumnRefRawExpr*>& column_exprs, ObDMLStmt* stmt)
{
  int ret = OB_SUCCESS;
  stmt = (NULL == stmt) ? get_stmt() : stmt;
  if (NULL == stmt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get stmt fail", K(stmt));
  } else {
    ColumnItem* column_item = NULL;
    if (table_item.is_generated_table()) {
      column_item = find_col_by_base_col_id(*stmt, table_item, col.get_column_id());
      if (NULL == column_item) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("all basic table's column should add to updatable view before", K(ret));
      }
    } else if (table_item.is_basic_table()) {
      column_item = stmt->get_column_item_by_id(table_item.table_id_, col.get_column_id());
      if (NULL == column_item) {
        if (OB_FAIL(resolve_basic_column_item(table_item, col.get_column_name_str(), true, column_item, stmt))) {
          LOG_WARN("fail to add column item to array", K(ret));
        } else if (OB_ISNULL(column_item) || OB_ISNULL(column_item->expr_)) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          LOG_WARN("failed to add column item");
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should be generated table or base table", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_var_to_array_no_dup(column_exprs, column_item->expr_))) {
        LOG_WARN("fail to add column item to array", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::add_all_rowkey_columns_to_stmt(
    const TableItem& table_item, ObIArray<ObColumnRefRawExpr*>& column_exprs, ObDMLStmt* stmt /*= NULL*/)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  uint64_t rowkey_column_id = 0;
  uint64_t base_table_id = table_item.get_base_table_item().ref_id_;
  stmt = (NULL == stmt) ? get_stmt() : stmt;
  if (NULL == stmt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get stmt fail", K(stmt));
  } else if (OB_FAIL(schema_checker_->get_table_schema(base_table_id, table_schema))) {
    LOG_WARN("table schema not found", K(base_table_id), K(table_item));
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid table schema", K(table_item));
  } else {
    const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
        LOG_WARN("get rowkey info failed", K(ret), K(i), K(rowkey_info));
      } else if (OB_FAIL(get_column_schema(base_table_id, rowkey_column_id, column_schema, true))) {
        LOG_WARN("get column schema failed", K(base_table_id), K(rowkey_column_id));
      } else if (OB_FAIL(add_column_to_stmt(table_item, *column_schema, column_exprs, stmt))) {
        LOG_WARN("add column to stmt failed", K(ret), K(table_item));
      }
    }
  }
  return ret;
}

// for ObDelUpdStmt
// add column's related columns in index to stmt
// if column_id is OB_INVALID_ID, all indexes' columns would be added to stmt
// @param[in] table_id            table id
// @param[in] column_id           column id
int ObDMLResolver::add_index_related_columns_to_stmt(
    const TableItem& table_item, const uint64_t column_id, ObIArray<ObColumnRefRawExpr*>& column_items)
{
  int ret = OB_SUCCESS;
  ColumnItem* col_item = NULL;
  ObDelUpdStmt* del_upd_stmt = dynamic_cast<ObDelUpdStmt*>(get_stmt());
  if (OB_ISNULL(del_upd_stmt) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("This function only for class inherited ObDelUpdStmt", K(del_upd_stmt), K_(schema_checker));
  } else if (OB_ISNULL(col_item = del_upd_stmt->get_column_item_by_id(table_item.table_id_, column_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column item not found", K(ret), K(table_item), K(column_id));
  } else {
    uint64_t base_table_id = table_item.get_base_table_item().ref_id_;
    uint64_t base_column_id = table_item.is_generated_table() ? col_item->base_cid_ : col_item->column_id_;
    const ObTableSchema* table_schema = NULL;
    const ObTableSchema* index_schema = NULL;
    const ObColumnSchemaV2* column_schema = NULL;

    if (OB_FAIL(schema_checker_->get_table_schema(base_table_id, table_schema))) {
      LOG_WARN("invalid table id", K(base_table_id));
    } else if (NULL == (column_schema = table_schema->get_column_schema(base_column_id))) {
      LOG_WARN("get column schema failed", K(ret), K(base_table_id), K(base_column_id));
    } else if (column_schema->is_rowkey_column()) {
      // if the column id is rowkey, wo need to add all columns in table schema to columns
      if (OB_FAIL(add_all_columns_to_stmt(table_item, column_items))) {
        LOG_WARN("add all columns to stmt failed", K(ret));
      } else {
        LOG_DEBUG("add all column to stmt due to the update column is primary key");
      }
    } else {
      uint64_t index_tids[OB_MAX_INDEX_PER_TABLE];
      int64_t index_count = OB_MAX_INDEX_PER_TABLE;
      // get all the indexes
      if (OB_FAIL(schema_checker_->get_can_write_index_array(base_table_id, index_tids, index_count))) {
        LOG_WARN("fail to get index", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < index_count; ++i) {
          uint64_t index_id = index_tids[i];
          // get index schema
          if (OB_FAIL(schema_checker_->get_table_schema(index_id, index_schema))) {
            LOG_WARN("get index schema failed", K(index_id));
          } else {
            // only add the column items in the index schema which contain the column_id
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

// note:
// for update, if schema is index type, all columns in index schema
// should be added, including index defined columns, rowkey columns
// in data schema, and storing columns.
// for delete, the storing columns might be useless, but still added.
//
int ObDMLResolver::add_all_index_rowkey_to_stmt(
    const TableItem& table_item, const ObTableSchema* index_schema, ObIArray<ObColumnRefRawExpr*>& column_items)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  uint64_t rowkey_column_id = OB_INVALID_ID;
  const ObColumnSchemaV2* column_schema = NULL;
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
        if (OB_FAIL(get_column_schema(base_table_id, rowkey_column_id, column_schema, true))) {
          LOG_WARN("get column schema failed", K(ret), K(base_table_id), K(rowkey_column_id));
        } else if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get column schema", K(ret), K(base_table_id), K(rowkey_column_id));
        } else if (OB_FAIL(add_column_to_stmt(table_item, *column_schema, column_items))) {
          LOG_WARN("add column to stmt failed", K(ret), K(table_item), K(*column_schema));
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::add_column_ref_to_set(ObRawExpr*& expr, ObIArray<TableItem*>* table_list)
{
  int ret = OB_SUCCESS;
  bool already_has = false;
  ObDMLStmt* stmt = get_stmt();

  if (OB_ISNULL(stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt is NULL");
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer to expr", K(expr));
  } else if (OB_ISNULL(table_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ObIArray", K(table_list));
  } else if (OB_UNLIKELY(T_REF_COLUMN != expr->get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong type", K(expr->get_expr_type()));
  } else {
    ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr);
    if (OB_UNLIKELY(OB_INVALID_ID == col_expr->get_table_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table id", K(col_expr->get_table_id()));
    } else {
      TableItem* table = stmt->get_table_item_by_id(col_expr->get_table_id());
      for (int64_t i = 0; OB_SUCC(ret) && !already_has && i < table_list->count(); i++) {
        TableItem* cur_table = table_list->at(i);
        if (OB_ISNULL(cur_table) || OB_ISNULL(table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL pointer", K(cur_table), K(table));
        } else if (cur_table->table_id_ == table->table_id_) {
          already_has = true;
        }
      }
      if (OB_SUCC(ret) && !already_has && OB_FAIL(table_list->push_back(const_cast<TableItem*>(table)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_where_clause(const ParseNode* node)
{
  int ret = OB_SUCCESS;

  if (node) {
    current_scope_ = T_WHERE_SCOPE;
    ObDMLStmt* stmt = get_stmt();

    set_has_oracle_join(false);

    CK(OB_NOT_NULL(stmt), OB_NOT_NULL(node->children_[0]), node->type_ == T_WHERE_CLAUSE);
    if (T_SP_EXPLICIT_CURSOR_ATTR == node->children_[0]->type_) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "explicit cursor");
    } else {
      OZ(resolve_and_split_sql_expr_with_bool_expr(*node->children_[0], stmt->get_condition_exprs()));
    }
    OZ(generate_outer_join_tables());
    OZ(deduce_generated_exprs(stmt->get_condition_exprs()));
  }
  return ret;
}

int ObDMLResolver::resolve_order_clause(const ParseNode* order_by_node)
{
  int ret = OB_SUCCESS;
  if (order_by_node) {
    ObDMLStmt* stmt = get_stmt();
    current_scope_ = T_ORDER_SCOPE;
    const ParseNode* sort_list = NULL;
    const ParseNode* siblings_node = NULL;
    if (OB_UNLIKELY(order_by_node->type_ != T_ORDER_BY) || OB_UNLIKELY(order_by_node->num_child_ != 2) ||
        OB_ISNULL(order_by_node->children_) || OB_ISNULL(sort_list = order_by_node->children_[0]) || OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parameter", K(order_by_node), K(sort_list), KPC(stmt), K(ret));
    } else if (FALSE_IT(siblings_node = order_by_node->children_[1])) {
    } else if (NULL != siblings_node) {
      if (OB_LIKELY(stmt->is_hierarchical_query())) {
        if (stmt::T_SELECT == stmt->get_stmt_type() && 0 < static_cast<ObSelectStmt*>(stmt)->get_group_expr_size()) {
          // Either group by or order by siblings exists, but not both
          // eg: select max(c2) from t1 start with c1 = 1 connect by nocycle prior c1 = c2 group by c1,c2 order siblings
          // by c1, c2;
          ret = OB_ERR_INVALID_SIBLINGS;
          LOG_WARN("ORDER SIBLINGS BY clause not allowed here", K(ret));
        } else {
          stmt->set_order_siblings();
        }
      } else {
        ret = OB_ERR_INVALID_SIBLINGS;
        LOG_WARN("ORDER SIBLINGS BY clause not allowed here", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (stmt->get_order_item_size() != 0) {
        stmt->get_order_items().reset();
      }
      for (int32_t i = 0; OB_SUCC(ret) && i < sort_list->num_child_; i++) {
        ParseNode* sort_node = sort_list->children_[i];
        OrderItem order_item;
        if (OB_FAIL(resolve_order_item(*sort_node, order_item))) {
          LOG_WARN("resolve order item failed", K(ret));
        } else if (OB_FAIL(stmt->add_order_item(order_item))) {
          // add the order-by item
          LOG_WARN("Add order expression error", K(ret));
        } else if (NULL != siblings_node && OB_NOT_NULL(order_item.expr_) &&
                   order_item.expr_->has_specified_pseudocolumn()) {
          ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
          LOG_WARN("Specified pseudo column or operator not allowed here", K(ret));
        }
        if (OB_ERR_AGGREGATE_ORDER_FOR_UNION == ret) {
          LOG_USER_ERROR(OB_ERR_AGGREGATE_ORDER_FOR_UNION, i + 1);
        }
      }
    }  // end of for
  }    // end of if
  return ret;
}

int ObDMLResolver::resolve_order_item(const ParseNode& sort_node, OrderItem& order_item)
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr;
  if (ObResolverUtils::set_direction_by_mode(sort_node, order_item)) {
    LOG_WARN("failed to set direction by mode", K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(sort_node.children_[0]->type_ == T_INT)) {
    ret = OB_ERR_PARSER_SYNTAX;
    SQL_RESV_LOG(WARN, "index order item not support in update");
  } else if (OB_FAIL(resolve_sql_expr(*(sort_node.children_[0]), expr))) {
    SQL_RESV_LOG(WARN, "resolve sql expression failed", K(ret));
  } else {
    order_item.expr_ = expr;
  }
  return ret;
}

int ObDMLResolver::resolve_limit_clause(const ParseNode* node)
{
  int ret = OB_SUCCESS;
  if (node) {
    current_scope_ = T_LIMIT_SCOPE;
    ObDMLStmt* stmt = get_stmt();
    ParseNode* limit_node = NULL;
    ParseNode* offset_node = NULL;
    if (node->type_ == T_LIMIT_CLAUSE) {
      limit_node = node->children_[0];
      offset_node = node->children_[1];
    } else if (node->type_ == T_COMMA_LIMIT_CLAUSE) {
      limit_node = node->children_[1];
      offset_node = node->children_[0];
    }
    ObRawExpr* limit_count = NULL;
    ObRawExpr* limit_offset = NULL;
    // resolve the question mark with less value first
    if (limit_node != NULL && limit_node->type_ == T_QUESTIONMARK && offset_node != NULL &&
        offset_node->type_ == T_QUESTIONMARK && limit_node->value_ > offset_node->value_) {
      if (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, *offset_node, limit_offset, NULL)) ||
          OB_FAIL(ObResolverUtils::resolve_const_expr(params_, *limit_node, limit_count, NULL))) {
        LOG_WARN("Resolve limit/offset error", K(ret));
      }
    } else {
      if (limit_node != NULL) {
        if (limit_node->type_ != T_LIMIT_INT && limit_node->type_ != T_LIMIT_UINT &&
            limit_node->type_ != T_QUESTIONMARK && limit_node->type_ != T_INT) {
          ret = OB_ERR_RESOLVE_SQL;
          LOG_WARN("Wrong type of limit value");
        } else {
          if (T_LIMIT_INT == limit_node->type_) {
            limit_node->type_ = T_INT;
          } else if (T_LIMIT_UINT == limit_node->type_) {
            limit_node->type_ = T_UINT64;
          }
          if (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, *limit_node, limit_count, NULL))) {
            LOG_WARN("Resolve limit error", K(ret));
          }
        }
      }
      if (ret == OB_SUCCESS && offset_node != NULL) {
        if (offset_node->type_ != T_INT && offset_node->type_ != T_UINT64 && offset_node->type_ != T_QUESTIONMARK) {
          ret = OB_ERR_RESOLVE_SQL;
          LOG_WARN("Wrong type of limit value", K(ret), K(offset_node->type_));
        } else if (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, *offset_node, limit_offset, NULL))) {
          LOG_WARN("Resolve offset error", K(ret));
        }
      }
    }
    CK(session_info_)
    if (OB_SUCC(ret)) {
      // make sure limit expr is int value in static typing engine.
      ObRawExpr** exprs[] = {&limit_count, &limit_offset};
      for (int64_t i = 0; i < ARRAYSIZEOF(exprs) && OB_SUCC(ret); i++) {
        ObExprResType dst_type;
        dst_type.set_int();
        ObSysFunRawExpr* cast_expr = NULL;
        if (NULL != (*exprs[i]) && !ob_is_int_tc((*exprs[i])->get_result_type().get_type())) {
          OZ(ObRawExprUtils::create_cast_expr(*params_.expr_factory_, *exprs[i], dst_type, cast_expr, session_info_));
          CK(NULL != cast_expr);
          if (OB_SUCC(ret)) {
            *exprs[i] = cast_expr;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      stmt->set_limit_offset(limit_count, limit_offset);
    }
  }
  return ret;
}

int ObDMLResolver::resolve_hints(const ParseNode* node)
{
  int ret = OB_SUCCESS;
  ObString stmt_name;
  LOG_DEBUG("start to resolve query hints");
  ObDMLStmt* stmt = get_stmt();
  ObQueryCtx* query_ctx = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Stmt should not be NULL", K(ret));
  } else if (OB_ISNULL(query_ctx = stmt->get_query_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Stmt's query ctx should not be NULL", K(ret));
  } else if (OB_FAIL(session_variable_opt_influence())) {
    LOG_WARN("Session variable opt influence error", K(ret));
  } else if (node) {
    ObStmtHint& stmt_hint = stmt->get_stmt_hint();
    OB_ASSERT(node->type_ == T_HINT_OPTION_LIST);
    for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      ParseNode* hint_node = node->children_[i];
      if (!hint_node) {
        continue;
      }
      LOG_DEBUG("resolve hint node", "type", hint_node->type_);
      switch (hint_node->type_) {
        case T_HOTSPOT:
          stmt_hint.hotspot_ = true;
          break;
        case T_INDEX: {
          if (3 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Index hint should have 2 children", K(ret));
          } else {
            ParseNode* qb_node = hint_node->children_[0];
            ParseNode* table_node = hint_node->children_[1];
            ParseNode* index_name_node = hint_node->children_[2];
            if (NULL == table_node || NULL == index_name_node) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("table name node and index name node should not be NULL", K(ret));
            } else {
              ObOrgIndexHint index_hint;
              ObString qb_name;
              if (OB_FAIL(resolve_qb_name_node(qb_node, qb_name))) {
                LOG_WARN("Failed to resolve qb name node", K(ret));
              } else if (OB_FAIL(resolve_table_relation_in_hint(table_node,
                             index_hint.table_.table_name_,
                             index_hint.table_.db_name_,
                             index_hint.table_.qb_name_))) {
                LOG_WARN("Resolve table relation fail", K(ret));
              } else {
                index_hint.index_name_.assign_ptr(
                    index_name_node->str_value_, static_cast<int32_t>(index_name_node->str_len_));

                if (qb_name.empty()) {
                  if (OB_FAIL(stmt_hint.org_indexes_.push_back(index_hint))) {
                    LOG_WARN("Add index hint failed");
                  }
                } else if (OB_FAIL(query_ctx->add_index_hint(qb_name, index_hint))) {
                  LOG_WARN("Add index hint failed");
                } else {
                }
              }
            }
          }
          break;
        }
        case T_FROZEN_VERSION: {
          stmt_hint.frozen_version_ = hint_node->children_[0]->value_;
          stmt_hint.read_consistency_ = FROZEN;
          break;
        }
        case T_TOPK:
          stmt_hint.topk_precision_ = hint_node->children_[0]->value_;
          stmt_hint.sharding_minimum_row_count_ = hint_node->children_[1]->value_;
          break;
        case T_QUERY_TIMEOUT: {
          if (hint_node->children_[0]->value_ > OB_MAX_USER_SPECIFIED_TIMEOUT) {
            stmt_hint.query_timeout_ = OB_MAX_USER_SPECIFIED_TIMEOUT;
            LOG_USER_WARN(OB_ERR_TIMEOUT_TRUNCATED);
          } else {
            stmt_hint.query_timeout_ = hint_node->children_[0]->value_;
          }
          break;
        }
        case T_READ_CONSISTENCY:
          if (hint_node->value_ == 2) {
            stmt_hint.read_consistency_ = FROZEN;
          } else if (hint_node->value_ == 3) {
            stmt_hint.read_consistency_ = WEAK;
          } else if (hint_node->value_ == 4) {
            stmt_hint.read_consistency_ = STRONG;
          } else {
            ret = OB_ERR_HINT_UNKNOWN;
            _OB_LOG(ERROR, "unknown hint value, ret=%d", ret);
          }
          break;
        case T_LOG_LEVEL: {
          const char* str = hint_node->children_[0]->str_value_;
          int32_t length = static_cast<int32_t>(hint_node->children_[0]->str_len_);
          if (NULL != str) {
            int tmp_ret = OB_SUCCESS;
            ObString log_level(length, str);
            if (0 == log_level.case_compare("disabled")) {
              // allowed for variables
            } else if (OB_UNLIKELY(tmp_ret = OB_LOGGER.parse_check(str, length))) {
              LOG_WARN("Log level parse check error", K(tmp_ret));
            } else {
              stmt_hint.log_level_.assign_ptr(str, length);
            }
          }
          break;
        }
        case T_TRANS_PARAM: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("TRANS_PARAM hint should have 2 children", K(ret));
          } else if (parse_trans_param_hint(hint_node->children_[0], hint_node->children_[1], stmt_hint)) {
            LOG_WARN("failed to parset opt param hint.", K(ret));
          } else { /* do nothing. */
          }
          break;
        }
        case T_QB_NAME: {
          if (1 != hint_node->num_child_ || NULL == hint_node->children_[0]) {
            ret = OB_ERR_PARSE_SQL;
            LOG_WARN("Parse sql failed", K(ret));
          } else if (static_cast<int32_t>(hint_node->children_[0]->str_len_) <= OB_MAX_QB_NAME_LENGTH) {
            stmt_name.assign_ptr(
                hint_node->children_[0]->str_value_, static_cast<int32_t>(hint_node->children_[0]->str_len_));
          } else {
          }  // do nothing
          break;
        }
        case T_FULL: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Full hint should have 2 children", K(ret));
          } else {
            ObString qb_name;
            ObOrgIndexHint index_hint;
            if (OB_FAIL(resolve_qb_name_node(hint_node->children_[0], qb_name))) {
              LOG_WARN("Failed to resolve qb name node", K(qb_name), K(ret));
            } else if (OB_FAIL(resolve_table_relation_in_hint(hint_node->children_[1],
                           index_hint.table_.table_name_,
                           index_hint.table_.db_name_,
                           index_hint.table_.qb_name_))) {
              LOG_WARN("Resolve table relation fail", K(ret));
            } else {
              index_hint.index_name_.assign_ptr(
                  ObStmtHint::PRIMARY_KEY, static_cast<int32_t>(strlen(ObStmtHint::PRIMARY_KEY)));
              if (qb_name.empty()) {
                if (OB_FAIL(stmt_hint.org_indexes_.push_back(index_hint))) {
                  LOG_WARN("Add full hint failed", K(ret));
                }
              } else {
                if (OB_FAIL(query_ctx->add_index_hint(qb_name, index_hint))) {
                  LOG_WARN("Add full hint failed", K(ret));
                }
              }
            }
          }
          break;
        }
        case T_LEADING: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Leading hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.join_order_,
                         query_ctx->join_order_,
                         stmt_hint.join_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            LOG_DEBUG(
                "Succ to add leading hint", K(stmt_hint.join_order_.count()), K(stmt_hint.join_order_pairs_.count()));
          }
          break;
        }
        case T_USE_MERGE: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Use merge hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.use_merge_,
                         query_ctx->use_merge_,
                         stmt_hint.use_merge_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            LOG_DEBUG("Succ to add use merge hint");
          }
          break;
        }
        case T_NO_USE_MERGE: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Use no merge hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.no_use_merge_,
                         query_ctx->no_use_merge_,
                         stmt_hint.no_use_merge_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            LOG_DEBUG("Succ to add no use merge hint");
          }
          break;
        }
        case T_USE_HASH: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Use hash hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.use_hash_,
                         query_ctx->use_hash_,
                         stmt_hint.use_hash_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            LOG_DEBUG("Succ to add use hash hint");
          }
          break;
        }
        case T_NO_USE_HASH: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Use hash hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.no_use_hash_,
                         query_ctx->no_use_hash_,
                         stmt_hint.no_use_hash_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            LOG_DEBUG("Succ to add no use hash hint");
          }
          break;
        }
        case T_USE_NL: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Use nl hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.use_nl_,
                         query_ctx->use_nl_,
                         stmt_hint.use_nl_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            LOG_DEBUG("Succ to add nl hint", "use nl", stmt_hint.use_nl_);
          }
          break;
        }
        case T_NO_USE_NL: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Use no nl hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.no_use_nl_,
                         query_ctx->no_use_nl_,
                         stmt_hint.no_use_nl_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            LOG_DEBUG("Succ to add no nl hint", "no use nl", stmt_hint.use_nl_);
          }
          break;
        }
        case T_PX_JOIN_FILTER: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("px join filter hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.px_join_filter_,
                         query_ctx->px_join_filter_,
                         stmt_hint.px_join_filter_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            LOG_DEBUG("Succ to add no px join filter hint", "px join filter", stmt_hint.px_join_filter_);
          }
          break;
        }
        case T_NO_PX_JOIN_FILTER: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Use no nl hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.no_px_join_filter_,
                         query_ctx->no_px_join_filter_,
                         stmt_hint.no_px_join_filter_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            LOG_DEBUG("Succ to add no px join filter hint", "no px join filter", stmt_hint.no_px_join_filter_);
          }
          break;
        }
        case T_FORCE_REFRESH_LOCATION_CACHE: {
          stmt_hint.force_refresh_lc_ = true;
          break;
        }
        case T_ORDERED: {
          stmt_hint.join_ordered_ = true;
          break;
        }
        case T_USE_PLAN_CACHE: {
          if (1 == hint_node->value_) {
            stmt_hint.plan_cache_policy_ = OB_USE_PLAN_CACHE_NONE;
          } else if (2 == hint_node->value_) {
            stmt_hint.plan_cache_policy_ = OB_USE_PLAN_CACHE_DEFAULT;
          } else {
            ret = OB_ERR_HINT_UNKNOWN;
            LOG_WARN("Unknown hint.", K(ret));
          }
          break;
        }
        case T_USE_JIT: {
          if (1 == hint_node->value_) {
            stmt_hint.use_jit_policy_ = OB_USE_JIT_AUTO;
          } else if (2 == hint_node->value_) {
            stmt_hint.use_jit_policy_ = OB_USE_JIT_FORCE;
          } else {
            ret = OB_ERR_HINT_UNKNOWN;
            LOG_WARN("Unknown hint.", K(ret));
          }
          break;
        }
        case T_NO_USE_JIT: {
          stmt_hint.use_jit_policy_ = OB_NO_USE_JIT;
          break;
        }
        case T_USE_PX: {
          stmt_hint.use_px_ = ObUsePxHint::ENABLE;
          stmt_hint.has_px_hint_ = true;
          break;
        }
        case T_NO_USE_PX: {
          stmt_hint.use_px_ = ObUsePxHint::DISABLE;
          stmt_hint.has_px_hint_ = true;
          break;
        }
        case T_ENABLE_PARALLEL_DML: {
          stmt_hint.pdml_option_ = ObPDMLOption::ENABLE;
          break;
        }
        case T_DISABLE_PARALLEL_DML: {
          stmt_hint.pdml_option_ = ObPDMLOption::DISABLE;
          break;
        }
        case T_NO_EXPAND: {
          if (OB_ISNULL(hint_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no expand hint is null.", K(ret));
          } else if (OB_FAIL(parse_qb_in_rewrite_hint(
                         hint_node, query_ctx->no_expand_, stmt_hint.use_expand_, ObUseRewriteHint::NO_EXPAND))) {
            LOG_WARN("failed to resolve the no expand hint.", K(ret));
          } else {
          }  // do nothing.
          break;
        }
        case T_USE_CONCAT: {
          if (OB_ISNULL(hint_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("use concat hint is null.", K(ret));
          } else if (OB_FAIL(parse_qb_in_rewrite_hint(
                         hint_node, query_ctx->use_concat_, stmt_hint.use_expand_, ObUseRewriteHint::USE_CONCAT))) {
            LOG_WARN("failed to resolve the use concat hint.", K(ret));
          } else {
          }  // do nothing.
          break;
        }
        case T_MERGE_HINT: {
          if (OB_ISNULL(hint_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("merge hint is null.", K(ret));
          } else if (OB_FAIL(parse_qb_in_rewrite_hint(
                         hint_node, query_ctx->merge_, stmt_hint.use_view_merge_, ObUseRewriteHint::V_MERGE))) {
            LOG_WARN("failed to resolve the merge hint.", K(ret));
          } else {
          }  // do nothing.
          break;
        }
        case T_NO_MERGE_HINT: {
          if (OB_ISNULL(hint_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no merge hint is null.", K(ret));
          } else if (OB_FAIL(parse_qb_in_rewrite_hint(
                         hint_node, query_ctx->no_merge_, stmt_hint.use_view_merge_, ObUseRewriteHint::NO_V_MERGE))) {
            LOG_WARN("failed to resolve the no merge hint.", K(ret));
          } else {
          }  // do nothing.
          break;
        }
        case T_UNNEST: {
          if (OB_ISNULL(hint_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unnest hint is null.", K(ret));
          } else if (OB_FAIL(parse_qb_in_rewrite_hint(
                         hint_node, query_ctx->unnest_, stmt_hint.use_unnest_, ObUseRewriteHint::UNNEST))) {
            LOG_WARN("failed to resolve the unnest hint.", K(ret));
          } else {
          }  // do nothing.
          break;
        }
        case T_NO_UNNEST: {
          if (OB_ISNULL(hint_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no unnest hint is null.", K(ret));
          } else if (OB_FAIL(parse_qb_in_rewrite_hint(
                         hint_node, query_ctx->no_unnest_, stmt_hint.use_unnest_, ObUseRewriteHint::NO_UNNEST))) {
            LOG_WARN("failed to resolve the no unnest hint.", K(ret));
          } else {
          }  // do nothing.
          break;
        }
        case T_PLACE_GROUP_BY: {
          if (OB_ISNULL(hint_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("place group by hint is null.", K(ret));
          } else if (OB_FAIL(parse_qb_in_rewrite_hint(hint_node,
                         query_ctx->place_group_by_,
                         stmt_hint.use_place_groupby_,
                         ObUseRewriteHint::PLACE_GROUPBY))) {
            LOG_WARN("failed to resolve the place groupby hint.", K(ret));
          } else {
          }  // do nothing.
          break;
        }
        case T_NO_PLACE_GROUP_BY: {
          if (OB_ISNULL(hint_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no place group by hint is null.", K(ret));
          } else if (OB_FAIL(parse_qb_in_rewrite_hint(hint_node,
                         query_ctx->no_place_group_by_,
                         stmt_hint.use_place_groupby_,
                         ObUseRewriteHint::NO_PLACE_GROUPBY))) {
            LOG_WARN("failed to resolve the no place groupby hint.", K(ret));
          } else {
          }  // do nothing.
          break;
        }
        case T_NO_PRED_DEDUCE: {
          if (OB_ISNULL(hint_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no pred deduce hint is null", K(ret));
          } else if (OB_FAIL(parse_qb_in_rewrite_hint(hint_node,
                         query_ctx->no_pred_deduce_,
                         stmt_hint.use_pred_deduce_,
                         ObUseRewriteHint::NO_PRED_DEDUCE))) {
            LOG_WARN("failed to resolve the no pred deduce hint", K(ret));
          }
          break;
        }
        case T_USE_HASH_AGGREGATE: {
          stmt_hint.aggregate_ |= OB_USE_HASH_AGGREGATE;
          if (stmt_hint.aggregate_ & OB_NO_USE_HASH_AGGREGATE) {
            stmt_hint.aggregate_ = OB_UNSET_AGGREGATE_TYPE;
          }
          break;
        }
        case T_NO_USE_HASH_AGGREGATE: {
          stmt_hint.aggregate_ |= OB_NO_USE_HASH_AGGREGATE;
          if (stmt_hint.aggregate_ & OB_USE_HASH_AGGREGATE) {
            stmt_hint.aggregate_ = OB_UNSET_AGGREGATE_TYPE;
          }
          break;
        }
        case T_USE_LATE_MATERIALIZATION: {
          stmt_hint.use_late_mat_ = OB_USE_LATE_MATERIALIZATION;
          break;
        }
        case T_NO_USE_LATE_MATERIALIZATION: {
          stmt_hint.use_late_mat_ = OB_NO_USE_LATE_MATERIALIZATION;
          break;
        }
        case T_NO_REWRITE: {
          OB_ASSERT(T_NO_REWRITE == hint_node->type_);
          stmt_hint.no_rewrite_ = true;
        } break;
        case T_TRACE_LOG: {
          // make sure that the trace log starts
          // NOTE: no need to call SET_SAMPLE_FORCE_TRACE_LOG since we just need to make sure
          //       the logging starts. Printing will be forced as long as 'force_trace_log_'
          //       is true, which will be set in the ObSqlCtx.
          LOG_DEBUG("user set trace_log hint");
          stmt_hint.force_trace_log_ = true;  // not used at the moment
        } break;
        case T_USE_BNL: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Use nl hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.use_bnl_,
                         query_ctx->use_bnl_,
                         stmt_hint.use_bnl_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            stmt_hint.bnl_allowed_ = true;
            LOG_DEBUG("Succ to add bnl hint");
          }
          break;
        }
        case T_NO_USE_BNL: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Use no bnl hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.no_use_bnl_,
                         query_ctx->no_use_bnl_,
                         stmt_hint.no_use_bnl_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            LOG_DEBUG("Succ to add no bnl hint");
          }
          break;
        }
        case T_USE_NL_MATERIALIZATION: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Use no bnl hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.use_nl_materialization_,
                         query_ctx->use_nl_materialization_,
                         stmt_hint.use_nl_materialization_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            LOG_DEBUG("Succ to add no bnl hint");
          }
          break;
        }
        case T_NO_USE_NL_MATERIALIZATION: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Use no bnl hint should have 2 children", K(ret));
          } else if (OB_FAIL(parse_tables_in_hint(hint_node->children_[0],
                         hint_node->children_[1],
                         stmt_hint.no_use_nl_materialization_,
                         query_ctx->no_use_nl_materialization_,
                         stmt_hint.no_use_nl_materialization_order_pairs_))) {
            LOG_WARN("Failed to parse tables in hint", K(ret));
          } else {
            LOG_DEBUG("Succ to add no bnl hint");
          }
          break;
        }
        case T_MAX_CONCURRENT: {
          if (1 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("max concurrent node should have 1 child", K(ret));
          } else if (OB_ISNULL(hint_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("child of max concurrent node should not be NULL", K(ret));
          } else if (hint_node->children_[0]->value_ >= 0) {
            stmt_hint.max_concurrent_ = hint_node->children_[0]->value_;
          } else { /*do nothing*/
          }
          break;
        }
        case T_NO_PARALLEL: {
          stmt_hint.parallel_ = 1;
          break;
        }
        case T_PARALLEL: {
          if (1 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("stmt parallel degree node should have 1 child", K(ret), K(hint_node->num_child_));
          } else if (OB_ISNULL(hint_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("child of stmt parallel degree node should not be NULL", K(ret));
          } else if (OB_FAIL(resolve_parallel_in_hint(hint_node->children_[0], stmt_hint.parallel_))) {
            LOG_WARN("fail to resolve parallel in hint", K(ret));
          }
          break;
        }
        case T_PQ_DISTRIBUTE: {
          if (4 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("PQ Distribute should have 4 child", K(ret));
          } else {
            if (OB_FAIL(resolve_pq_distribute_node(hint_node->children_[0],
                    hint_node->children_[1],
                    hint_node->children_[2],
                    hint_node->children_[3],
                    stmt_hint.org_pq_distributes_,
                    query_ctx->org_pq_distributes_))) {
              LOG_WARN("failed to resolve pq distribute node.", K(ret));
            } else {
            }  // do nothing.
          }
          break;
        }
        case T_PQ_MAP: {
          if (2 != hint_node->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("PQ map should have 2 child", K(ret));
          } else {
            ObString qb_name;
            ObOrgPQMapHint pq_map_hint;
            if (OB_FAIL(resolve_qb_name_node(hint_node->children_[0], qb_name))) {
              LOG_WARN("failed to resolve qb name node", K(qb_name), K(ret));
            } else if (OB_FAIL(resolve_table_relation_in_hint(hint_node->children_[1],
                           pq_map_hint.table_.table_name_,
                           pq_map_hint.table_.db_name_,
                           pq_map_hint.table_.qb_name_))) {
              LOG_WARN("resolve table relation fail", K(ret));
            } else {
              pq_map_hint.use_pq_map_ = true;
              if (qb_name.empty()) {
                if (OB_FAIL(stmt_hint.org_pq_maps_.push_back(pq_map_hint))) {
                  LOG_WARN("add pq mape hint failed", K(ret));
                }
              } else {
                if (OB_FAIL(query_ctx->add_pq_map_hint(qb_name, pq_map_hint))) {
                  LOG_WARN("add pq map hint failed", K(ret));
                }
              }
            }
          }
          break;
        }
        case T_TRACING: {
          common::ObSEArray<uint64_t, 8> tracing_ids;
          if (OB_FAIL(resolve_tracing_hint(hint_node, tracing_ids))) {
            LOG_WARN("Failed to resolve tracing hint", K(ret));
          } else if (OB_FAIL(query_ctx->add_tracing_hint(tracing_ids))) {
            LOG_WARN("Failed to add tracing hint");
          }
          break;
        }
        case T_STAT: {
          common::ObSEArray<uint64_t, 8> stat_ids;
          if (OB_FAIL(resolve_tracing_hint(hint_node, stat_ids))) {
            LOG_WARN("Failed to resolve tracing hint", K(ret));
          } else if (OB_FAIL(query_ctx->add_stat_hint(stat_ids))) {
            LOG_WARN("Failed to add tracing hint");
          }
          break;
        }
        default: {
          ret = OB_ERR_HINT_UNKNOWN;
          LOG_WARN("Unknown hint", "hint_name", get_type_name(hint_node->type_));
          break;
        }
      }
      if (OB_SUCC(ret) && T_MAX_CONCURRENT != hint_node->type_) {
        stmt_hint.has_hint_exclude_concurrent_ = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    /**
     *  If the system var _ob_use_parallel_execution was true,
     *  and the sql doesn't contain any no_use_px hint,
     *  and it doesn't contain any inner table,
     *  and (it contains a distributed table or we set parallel >= 2),
     *  then the hint was set to use_px.
     *
     *  We forbid all insert/update/delete to use px (without px and pdml hint).
     *  We open px for insert/update/delete (with px and pdml hint).
     */

    if (ObUsePxHint::DISABLE == stmt->get_stmt_hint().use_px_) {
      // use px hint is a global hint.
      // if no use px is used in one stmt, then px is forbidden in the whole query.
      stmt->get_query_ctx()->forbid_use_px_ = true;
    }

    if (stmt->get_query_ctx()->forbid_use_px_) {
      stmt->get_stmt_hint().use_px_ = ObUsePxHint::DISABLE;
      LOG_DEBUG("Do forbid to use px");
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObStmtHint::add_whole_hint(stmt))) {
        LOG_WARN("Add whole hint error", K(ret));
      } else {
        // stmt id name stored to query_ctx. stmt with set op not needed.
        if (!(stmt::T_SELECT == stmt->get_stmt_type() && static_cast<ObSelectStmt*>(stmt)->has_set_op())) {
          if (OB_FAIL(query_ctx->add_stmt_id_name(stmt->get_stmt_id(), stmt_name, stmt))) {
            LOG_WARN("Add stmt id name error", K(ret));
          }
        }
        // that query_timeout with hint
        if (OB_SUCC(ret)) {
          int64_t query_timeout = stmt->get_stmt_hint().query_timeout_;
          if (OB_ISNULL(session_info_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("session info is NULL", K(ret));
          } else if (query_timeout > 0) {
            int64_t start_time = session_info_->get_query_start_time();
            THIS_WORKER.set_timeout_ts(start_time + query_timeout);
          }
        }
      }
    }
  }
  return ret;
}

// Forbit select with order by limit exists in subquery in Oralce mode
// eg: select 1 from t1 where c1 in (select d1 from t2 order by c1); --error
// If fetch clause also eixsts in subquery, then order by is allowed:
// eg: select 1 from t1 where c1 in (select d1 from t2 order by c1 fetch next 1 rows only); --right
int ObDMLResolver::check_order_by_for_subquery_stmt(const ObSubQueryInfo& info)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("fail to check select stmt order by clause", K(ret), K(current_scope_), K(info));
  if (is_oracle_mode() && T_FROM_SCOPE != current_scope_) {
    LOG_DEBUG("fail to check select stmt order by clause", K(ret), K(current_scope_), K(info));
    if (OB_ISNULL(info.ref_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to check select stmt order by clause", K(ret));
    } else if (!info.ref_expr_->is_cursor() &&
               (OB_NOT_NULL(info.ref_expr_->get_ref_stmt()) && !info.ref_expr_->get_ref_stmt()->has_fetch()) &&
               OB_FAIL(check_stmt_order_by(info.ref_expr_->get_ref_stmt()))) {
      LOG_WARN("fail to check select stmt order by clause", K(ret));
    }
  }
  return ret;
}

int ObDMLResolver::check_stmt_order_by(const ObSelectStmt* stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null after resolve", K(ret));
  } else {
    if (stmt->has_order_by()) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("order by is forbit to exists in subquery ", K(ret));
    } else if (stmt->has_set_op()) {
      ObSEArray<ObSelectStmt*, 2> child_stmts;
      if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
        LOG_WARN("fail to get child stmts", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        const ObSelectStmt* sub_stmt = child_stmts.at(i);
        if (OB_FAIL(SMART_CALL(check_stmt_order_by(sub_stmt)))) {
          LOG_WARN("fail to check sub stmt order by", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_subquery_info(const ObIArray<ObSubQueryInfo>& subquery_info)
{
  int ret = OB_SUCCESS;
  if (current_level_ + 1 >= OB_MAX_SUBQUERY_LAYER_NUM && subquery_info.count() > 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Too many levels of subquery");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_info.count(); i++) {
    const ObSubQueryInfo& info = subquery_info.at(i);
    ObSelectResolver subquery_resolver(params_);
    subquery_resolver.set_current_level(current_level_ + 1);
    subquery_resolver.set_parent_namespace_resolver(this);
    if (OB_FAIL(add_cte_table_to_children(subquery_resolver))) {
      LOG_WARN("add CTE table to children failed", K(ret));
    } else if (info.parents_expr_info_.has_member(IS_AGG)) {
      subquery_resolver.set_parent_aggr_level(current_level_);
    }
    if (OB_FAIL(do_resolve_subquery_info(info, subquery_resolver))) {
      LOG_WARN("do resolve subquery info failed", K(ret));
    }
  }
  return ret;
}

int ObDMLResolver::do_resolve_subquery_info(const ObSubQueryInfo& subquery_info, ObChildStmtResolver& child_resolver)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  ObSelectStmt* sub_stmt = NULL;
  if (OB_ISNULL(subquery_info.sub_query_) || OB_ISNULL(subquery_info.ref_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery info is invalid", K_(subquery_info.sub_query), K_(subquery_info.ref_expr));
  } else if (OB_UNLIKELY(T_SELECT != subquery_info.sub_query_->type_)) {
    ret = OB_ERR_ILLEGAL_TYPE;
    LOG_WARN("Unknown statement type in subquery", "stmt_type", subquery_info.sub_query_->type_);
  } else if (OB_FAIL(child_resolver.resolve_child_stmt(*(subquery_info.sub_query_)))) {
    LOG_WARN("resolve select subquery failed", K(ret));
  } else {
    sub_stmt = child_resolver.get_child_stmt();
    subquery_info.ref_expr_->set_output_column(sub_stmt->get_select_item_size());
    // store result types of select items of subquery in ObUnaryRef
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_stmt->get_select_item_size(); ++i) {
      ObRawExpr* target_expr = sub_stmt->get_select_item(i).expr_;
      if (OB_ISNULL(target_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("target expr is null");
      } else {
        const ObExprResType& column_type = target_expr->get_result_type();
        if (OB_FAIL(subquery_info.ref_expr_->add_column_type(column_type))) {
          LOG_WARN("add column type to subquery ref expr failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    subquery_info.ref_expr_->set_ref_stmt(sub_stmt);
    subquery_info.ref_expr_->set_expr_level(current_level_);
    if (OB_FAIL(stmt->add_subquery_ref(const_cast<ObSubQueryInfo&>(subquery_info).ref_expr_))) {
      LOG_WARN("failed to add subquery reference", K(ret));
    } else {
      stmt->set_subquery_flag(true);
      if (OB_FAIL(check_order_by_for_subquery_stmt(subquery_info))) {
        LOG_WARN("check subquery order by failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::try_add_remove_const_epxr(ObSelectStmt& stmt)
{
  int ret = OB_SUCCESS;
  CK(NULL != session_info_);
  CK(NULL != params_.expr_factory_);
  if (OB_SUCC(ret) && session_info_->use_static_typing_engine()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_item_size(); ++i) {
      ObRawExpr*& expr = stmt.get_select_item(i).expr_;
      CK(NULL != expr);
      if (OB_SUCC(ret) && expr->has_const_or_const_expr_flag()) {
        ObRawExpr* new_expr = NULL;
        OZ(ObRawExprUtils::build_remove_const_expr(*params_.expr_factory_, *session_info_, expr, new_expr));
        CK(NULL != new_expr);
        OX(expr = new_expr);
      }
    }
  }
  return ret;
}

int ObDMLResolver::check_expr_param(const ObRawExpr& expr)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if ((lib::is_mysql_mode() || stmt->is_insert_stmt()) && T_REF_QUERY == expr.get_expr_type()) {
    const ObQueryRefRawExpr& ref_query = static_cast<const ObQueryRefRawExpr&>(expr);
    if (1 != ref_query.get_output_column()) {
      ret = OB_ERR_INVALID_COLUMN_NUM;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      LOG_WARN("ref_query output column", K(ref_query.get_output_column()));
    }
  } else if (T_OP_ROW == expr.get_expr_type()) {
    const ObRawExpr* e = &expr;
    // need check row expr child, e.g.: +((c1, c2)) is resolved to: ROW(ROW(c1, c2))
    while (OB_SUCC(ret) && T_OP_ROW == e->get_expr_type() && 1 == e->get_param_count()) {
      e = e->get_param_expr(0);
      CK(NULL != e);
    }
    if (OB_SUCC(ret) && 1 != e->get_param_count()) {
      ret = OB_ERR_INVALID_COLUMN_NUM;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_NUM, (int64_t)1);
      LOG_WARN("op_row output column", K(ret), K(e->get_param_count()));
    }
  }
  return ret;
}

int ObDMLResolver::resolve_index_hint(const uint64_t table_id, const ParseNode* index_hint_node)
{
  int ret = OB_SUCCESS;
  if (index_hint_node && index_hint_node->num_child_ > 0) {
    const ParseNode* index_hint_first = index_hint_node->children_[0];  // only use first now
    const ParseNode* index_list = NULL;
    const ParseNode* index_hint_type = NULL;
    if (OB_ISNULL(index_hint_first)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index_hint_list num child more than 0, but first is NULL", K(ret));
    } else if (2 != index_hint_first->num_child_) {
      ret = OB_ERR_PARSE_SQL;
      LOG_WARN("Parse SQL error, index hint should have 2 children", K(ret));
    } else if (OB_ISNULL(index_hint_type = index_hint_first->children_[0])) {
      ret = OB_ERR_PARSE_SQL;
      LOG_WARN("Index hint type should not be NULL", K(ret));
    } else if (NULL == (index_list = index_hint_first->children_[1])) {
      // do nothing
    } else {
      ObDMLStmt* stmt = get_stmt();
      ObStmtHint& stmt_hint = stmt->get_stmt_hint();
      ObIndexHint index_hint;
      // get index hint type
      switch (index_hint_type->type_) {
        case T_USE: {
          index_hint.type_ = ObIndexHint::USE;
          break;
        }
        case T_FORCE: {
          index_hint.type_ = ObIndexHint::FORCE;
          break;
        }
        case T_IGNORE: {
          index_hint.type_ = ObIndexHint::IGNORE;
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Index hint type error", K(ret));
        }
      }
      // get index names
      ObString index_name;
      for (int i = 0; OB_SUCC(ret) && i < index_list->num_child_; i++) {
        if (OB_ISNULL(index_list->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Index name node should not be NULL", K(ret));
        } else {
          index_name.assign_ptr(
              index_list->children_[i]->str_value_, static_cast<int32_t>(index_list->children_[i]->str_len_));
          if (OB_FAIL(index_hint.index_list_.push_back(index_name))) {
            LOG_WARN("Push index_name to index hint error", K(ret));
          }
        }
      }
      // add table id
      if (OB_SUCC(ret)) {
        index_hint.table_id_ = table_id;
        if (OB_FAIL(stmt_hint.indexes_.push_back(index_hint))) {
          LOG_WARN("Push index hint to query hint error", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_partitions(
    const ParseNode* part_node, const uint64_t table_id, const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  if (NULL != part_node) {
    OB_ASSERT(1 == part_node->num_child_ && part_node->children_[0]->num_child_ > 0);
    ObStmtHint& stmt_hint = get_stmt()->get_stmt_hint();
    ObPartHint part_hint;
    part_hint.table_id_ = table_id;
    const ParseNode* name_list = part_node->children_[0];
    ObString partition_name;
    for (int i = 0; OB_SUCC(ret) && i < name_list->num_child_; i++) {
      ObSEArray<int64_t, 16> partition_ids;
      partition_name.assign_ptr(
          name_list->children_[i]->str_value_, static_cast<int32_t>(name_list->children_[i]->str_len_));
      // here just conver partition_name to its lowercase
      ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, partition_name);
      ObPartGetter part_getter(table_schema);
      if (T_USE_PARTITION == part_node->type_) {
        if (OB_FAIL(part_getter.get_part_ids(partition_name, partition_ids))) {
          LOG_WARN("failed to get part ids", K(ret), K(partition_name));
          if (OB_UNKNOWN_PARTITION == ret && share::is_mysql_mode()) {
            LOG_USER_ERROR(OB_UNKNOWN_PARTITION,
                partition_name.length(),
                partition_name.ptr(),
                table_schema.get_table_name_str().length(),
                table_schema.get_table_name_str().ptr());
          }
        }
      } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2230) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("use subpartition() with minimal cluster < 2230", K(ret));
      } else if (OB_FAIL(part_getter.get_subpart_ids(partition_name, partition_ids))) {
        LOG_WARN("failed to get subpart ids", K(ret), K(partition_name));
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(append_array_no_dup(part_hint.part_ids_, partition_ids))) {
          LOG_WARN("Push partition id error", K(ret));
        } else if (OB_FAIL(part_hint.part_names_.push_back(partition_name))) {
          LOG_WARN("failed to push back partition name", K(ret));
        } else {
          LOG_INFO("part ids", K(partition_name), K(partition_ids));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt_hint.part_hints_.push_back(part_hint))) {
        LOG_WARN("Add partition hint error", K(ret));
      }
    }
  }
  return ret;
}

// In current implement, partition name should be 'pX' for one level partition,
//'pXspX' for two level partition, X is an unsigned interger
int ObDMLResolver::parse_partition(const ObString& part_name, const ObPartitionLevel part_level, const int64_t part_num,
    const int64_t subpart_num, int64_t& partition_id)
{
  int ret = OB_SUCCESS;
  partition_id = 0;
  int64_t part_id = OB_INVALID_INDEX;
  int64_t subpart_id = OB_INVALID_INDEX;
  if (part_name.length() <= 1) {
    ret = OB_UNKNOWN_PARTITION;
  } else {
    if ('p' != part_name[0]) {
      ret = OB_UNKNOWN_PARTITION;
    } else {
      part_id = 0;
      int64_t i = 1;
      bool sub_part = false;
      // parse part_id
      for (; OB_SUCC(ret) && i < part_name.length(); ++i) {
        if (part_name[i] >= '0' && part_name[i] <= '9') {
          part_id = part_id * 10 + part_name[i] - '0';
        } else if (i < (part_name.length() - 2) && 's' == part_name[i] && 'p' == part_name[i + 1]) {
          sub_part = true;  // sp,there's sub part
          break;
        } else {
          ret = OB_UNKNOWN_PARTITION;
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (sub_part) {
        i += 2;
        subpart_id = 0;
        if (i >= part_name.length()) {
          ret = OB_UNKNOWN_PARTITION;
        }
        for (; OB_SUCC(ret) && i < part_name.length(); ++i) {
          if (part_name[i] >= '0' && part_name[i] <= '9') {
            subpart_id = subpart_id * 10 + part_name[i] - '0';
          } else {
            ret = OB_UNKNOWN_PARTITION;
            break;
          }
        }
      } else {
      }  // do nothing
    }
  }
  if (OB_SUCCESS == ret) {
    if (part_id >= part_num) {
      ret = OB_UNKNOWN_PARTITION;
    } else if (PARTITION_LEVEL_TWO == part_level) {
      if (OB_INVALID_INDEX == subpart_id || subpart_id >= subpart_num) {
        ret = OB_UNKNOWN_PARTITION;
      } else {
        partition_id = generate_phy_part_id(part_id, subpart_id, PARTITION_LEVEL_TWO);
      }
    } else if (OB_INVALID_INDEX != subpart_id) {
      ret = OB_UNKNOWN_PARTITION;
    } else {
      partition_id = part_id;
    }
  }

  if (OB_UNKNOWN_PARTITION == ret) {
    LOG_WARN("Unknown partiton", K(ret), K(part_name), K(part_num), K(subpart_num));
  }
  return ret;
}

// duplicated assignments are permitted, such SET c2=c1*2, c2=1+c2.
int ObDMLResolver::add_assignment(
    ObTablesAssignments& assigns, const TableItem* table_item, const ColumnItem* col, ObAssignment& assign)
{
  int ret = OB_SUCCESS;
  ObTableAssignment* table_assign = NULL;
  int64_t N = assigns.count();
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(table_item) || OB_ISNULL(assign.column_expr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(schema_checker), K(table_item), K_(assign.column_expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    ObTableAssignment& ta = assigns.at(i);
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
    // in MySQL:
    // The second assignment in the following statement sets col2 to the current (updated) col1 value,
    // not the original col1 value.
    // The result is that col1 and col2 have the same value.
    // This behavior differs from standard SQL.
    // UPDATE t1 SET col1 = col1 + 1, col2 = col1;
    // But in Oracle, its behavior is same with standard SQL
    // set original col1 to col1 and col2
    // For generated column, when cascaded column is updated, the generated column will be updated with new column
    if (OB_FAIL(ObTableAssignment::expand_expr(table_assign->assignments_, assign.expr_))) {
      LOG_WARN("expand generated column expr failed", K(ret));
    }
  }
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < table_assign->assignments_.count(); ++i) {
    if (assign.column_expr_ == table_assign->assignments_.at(i).column_expr_) {
      table_assign->assignments_.at(i) = assign;
      table_assign->assignments_.at(i).is_duplicated_ = true;  // this column was updated repeatedly
      found = true;
    }
  }

  if (OB_SUCC(ret) && !found) {
    if (!table_assign->is_update_part_key_) {
      if (OB_FAIL(schema_checker_->check_if_partition_key(
              col->base_tid_, col->base_cid_, table_assign->is_update_part_key_))) {
        LOG_WARN("check if partition key failed", K(ret), K(*col), KPC(table_item), KPC(assign.column_expr_));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_assign->assignments_.push_back(assign))) {
        LOG_WARN("store assignment failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::check_whether_assigned(
    const ObAssignments& assigns, uint64_t table_id, uint64_t base_column_id, bool& exist)
{
  int ret = OB_SUCCESS;
  int64_t N = assigns.count();
  exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && !exist && i < N; ++i) {
    const ObAssignment& as = assigns.at(i);
    if (OB_ISNULL(as.column_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_expr is null");
    } else if (as.base_column_id_ == base_column_id && as.column_expr_->get_table_id() == table_id) {
      exist = true;
    }
  }
  return ret;
}

int ObDMLResolver::check_need_assignment(
    const ObAssignments& assigns, uint64_t table_id, const ObColumnSchemaV2& column, bool& need_assign)
{
  need_assign = false;
  int ret = OB_SUCCESS;
  bool exist = false;
  if (column.is_generated_column()) {
    if (OB_FAIL(check_whether_assigned(assigns, table_id, column.get_column_id(), exist))) {
      LOG_WARN("check column whether assigned failed", K(ret));
    } else if (!exist) {
      ObArray<uint64_t> cascaded_columns;
      if (OB_FAIL(column.get_cascaded_column_ids(cascaded_columns))) {
        LOG_WARN("get cascaded column ids failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && !exist && i < cascaded_columns.count(); ++i) {
        if (OB_FAIL(check_whether_assigned(assigns, table_id, cascaded_columns.at(i), exist))) {
          LOG_WARN("check whether assigned cascaded columns failed", K(ret), K(table_id), K(cascaded_columns.at(i)));
        }
      }
      if (OB_SUCC(ret) && exist) {
        // the generated column isn't assigned, but the cascaded columns have been assigned
        // so assign the generated column
        need_assign = true;
      }
    }
  } else if (column.is_on_update_current_timestamp()) {
    if (OB_FAIL(check_whether_assigned(assigns, table_id, column.get_column_id(), exist))) {
      LOG_WARN("check whether assigned cascaded columns failed", K(ret), K(table_id), K(column.get_column_id()));
    } else if (!exist) {
      need_assign = true;
    }
  }
  return ret;
}

int ObDMLResolver::resolve_additional_assignments(ObTablesAssignments& assigns, const ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  const TableItem* table_item = NULL;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("params is invalid", K_(params_.expr_factory), K(stmt));
  } else if (T_UPDATE_SCOPE != scope && T_INSERT_SCOPE != scope) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input scope", K(scope));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); ++i) {
    if (OB_ISNULL(schema_checker_) || OB_ISNULL(schema_guard = schema_checker_->get_schema_guard()) ||
        OB_ISNULL(table_item = stmt->get_table_item_by_id(assigns.at(i).table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid schema checker", K(schema_checker_));
    } else if (OB_FAIL(schema_checker_->get_table_schema(table_item->get_base_table_item().ref_id_, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), KPC(table_item));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get table schema", KPC(table_item), K(table_schema));
    } else {
      for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
           (OB_SUCCESS == ret && iter != table_schema->column_end());
           ++iter) {
        ColumnItem* col_item = NULL;
        ObColumnSchemaV2* column_schema = *iter;
        bool need_assigned = false;
        uint64_t column_id = OB_INVALID_ID;
        if (NULL == column_schema) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column schema fail", K(column_schema));
        } else if (FALSE_IT(column_id = column_schema->get_column_id())) {
          // do nothing
        } else if (OB_FAIL(check_need_assignment(
                       assigns.at(i).assignments_, table_item->table_id_, *column_schema, need_assigned))) {
          LOG_WARN("fail to check assignment exist", KPC(table_item), K(column_id));
        } else if (need_assigned) {
          // for insert scope, on duplicate key update column list already
          // exists in insert list, therefore, only need to add assignment.
          // add assign
          ObAssignment assignment;
          ObSEArray<ObColumnRefRawExpr*, 1> col_exprs;
          if (OB_FAIL(add_column_to_stmt(*table_item, *column_schema, col_exprs))) {
            LOG_WARN("add column to stmt failed", K(ret), K(*table_item), K(*column_schema));
          } else if (col_exprs.empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no column expr returned", K(ret));
          } else if (OB_ISNULL(col_item = stmt->get_column_item_by_id(
                                   table_item->table_id_, col_exprs.at(0)->get_column_id()))) {
            LOG_WARN("get column item failed", K(ret));
          } else {
            assignment.column_expr_ = col_item->expr_;
            assignment.base_table_id_ = col_item->base_tid_;
            assignment.base_column_id_ = col_item->base_cid_;
            assignment.is_implicit_ = true;
            if (column_schema->is_on_update_current_timestamp()) {
              assignment.column_expr_->set_result_flag(OB_MYSQL_ON_UPDATE_NOW_FLAG);
              ObDefaultValueUtils utils(stmt, &params_, this);
              if (OB_FAIL(utils.build_now_expr(col_item, assignment.expr_))) {
                LOG_WARN("fail to build default expr", K(ret));
              }
            } else if (column_schema->is_generated_column()) {
              if (OB_FAIL(ObRawExprUtils::copy_expr(*params_.expr_factory_,
                      col_item->expr_->get_dependant_expr(),
                      assignment.expr_,
                      COPY_REF_DEFAULT))) {
                LOG_WARN("copy expr failed", K(ret));
              }
            }
            if (OB_FAIL(ret)) {
              // do nothing
            } else if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(
                           *params_.expr_factory_, column_schema, assignment.expr_, session_info_))) {
              LOG_WARN("fail to build format expr", K(ret));
            } else if (OB_FAIL(add_assignment(assigns, table_item, col_item, assignment))) {
              LOG_WARN("failed to ass assignment", K(ret));
            }
          }
        }
      }  // end for
    }
  }
  return ret;
}

int ObDMLResolver::check_basic_column_generated(
    const ObColumnRefRawExpr* col_expr, ObDMLStmt* dml_stmt, bool& is_generated)
{
  int ret = OB_SUCCESS;
  is_generated = false;
  const TableItem* table_item = NULL;
  const ColumnItem* view_column_item = NULL;
  if (OB_ISNULL(col_expr) || OB_ISNULL(dml_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr or stmt is null", K(ret), K(col_expr), K(dml_stmt));
  } else if (col_expr->is_generated_column()) {
    is_generated = true;
  } else if (OB_ISNULL(table_item = dml_stmt->get_table_item_by_id(col_expr->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get table item item by id failed", K(ret), KPC(col_expr));
  } else if (false == (table_item->is_generated_table() && OB_NOT_NULL(table_item->view_base_item_))) {
    // do thing
  } else if (OB_ISNULL(view_column_item =
                           dml_stmt->get_column_item_by_id(col_expr->get_table_id(), col_expr->get_column_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get column item item by id failed", K(ret), KPC(col_expr));
  } else {
    ObSelectStmt* select_stmt = NULL;
    ColumnItem* basic_column_item = NULL;
    while (table_item->is_generated_table() && OB_NOT_NULL(table_item->view_base_item_)) {
      select_stmt = table_item->ref_query_;
      table_item = table_item->view_base_item_;
    }
    if (OB_ISNULL(select_stmt) || OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref_query_ is null", K(ret));
    } else if (OB_ISNULL(basic_column_item =
                             select_stmt->get_column_item_by_id(table_item->table_id_, view_column_item->base_cid_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get column item item by id failed", K(ret));
    } else if (OB_ISNULL(basic_column_item->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr of column item is null", K(ret));
    } else if (basic_column_item->expr_->is_generated_column()) {
      is_generated = true;
    }
  }
  return ret;
}

int ObDMLResolver::resolve_assignments(const ParseNode& parse_node, ObTablesAssignments& assigns, ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  ObSEArray<ObColumnRefRawExpr*, 8> column_list;
  ObSEArray<ObRawExpr*, 8> value_list;
  if (OB_ISNULL(stmt) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resolver invalid status", K(stmt), KP_(params_.expr_factory));
  } else if (OB_FAIL(resolve_column_and_values(parse_node, column_list, value_list))) {
    LOG_WARN("failed to resovle column and values", K(ret));
  } else {
    ObAssignment assignment;
    ColumnItem* column = NULL;
    TableItem* table = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_list.count(); i++) {
      ObColumnRefRawExpr* ref_expr = column_list.at(i);
      if (OB_ISNULL(column = stmt->get_column_item_by_id(ref_expr->get_table_id(), ref_expr->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column item failed", K(*ref_expr), K(ret));
      } else if (OB_ISNULL(table = stmt->get_table_item_by_id(ref_expr->get_table_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get table item failed", K(*ref_expr), K(ret));
      } else {
        // Statement `update (select * from t1) t set t.c1 = 1` is legal in oralce, illegal in mysql.
        const bool is_updatable_generated_table =
            table->is_generated_table() && (!is_mysql_mode() || table->is_view_table_);
        if (!table->is_basic_table() && !is_updatable_generated_table) {
          ret = OB_ERR_NON_UPDATABLE_TABLE;
          const ObString& table_name = table->alias_name_;
          ObString scope_name = "UPDATE";
          LOG_USER_ERROR(
              OB_ERR_NON_UPDATABLE_TABLE, table_name.length(), table_name.ptr(), scope_name.length(), scope_name.ptr());
        }
        if (OB_SUCC(ret) && table->is_generated_table()) {
          if (NULL == table->view_base_item_) {
            if (OB_FAIL(set_base_table_for_updatable_view(*table, *ref_expr))) {
              LOG_WARN("find base table for update view failed", K(ret));
            } else if (OB_ISNULL(table->view_base_item_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("view base item is NULL", K(ret));
            }
          } else {
            if (OB_FAIL(check_same_base_table(*table, *ref_expr))) {
              LOG_WARN("check modified columns is same base table failed", K(ret), K(*table), K(*ref_expr));
            }
          }
        }
      }
      if (OB_SUCC(ret) && T_INSERT_SCOPE == scope) {
        if (OB_FAIL(mock_values_column_ref(ref_expr))) {
          LOG_WARN("fail to add value desc", K(ret));
        }
      }
      // update column index for auto-increment column in update clause
      if (OB_SUCC(ret)) {
        ObIArray<AutoincParam>& autoinc_params = stmt->get_autoinc_params();
        for (int64_t j = 0; j < autoinc_params.count(); ++j) {
          if (column->column_id_ == autoinc_params.at(j).autoinc_col_id_) {
            autoinc_params.at(j).autoinc_update_col_index_ = i;
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        assignment.column_expr_ = column->expr_;
        assignment.base_table_id_ = column->base_tid_;
        assignment.base_column_id_ = column->base_cid_;
        ObRawExpr* expr = value_list.at(i);
        bool is_generated_column = true;
        SQL_RESV_LOG(DEBUG, "is standard assignment", K(is_mysql_mode()));
        if (T_DEFAULT == expr->get_expr_type()) {
          ObDefaultValueUtils utils(stmt, &params_, this);
          if (OB_FAIL(utils.resolve_default_expr(*column, expr, scope))) {
            LOG_WARN("failed to resolve default expr", K(*column), K(ret));
          }
        } else if (OB_FAIL(check_basic_column_generated(ref_expr, stmt, is_generated_column))) {
          LOG_WARN("check basic column generated failed", K(ret));
        } else if (is_generated_column) {
          ret = OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN;
          const ObString& column_name = ref_expr->get_column_name();
          const ObString& table_name = ref_expr->get_table_name();
          LOG_USER_ERROR(OB_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
              column_name.length(),
              column_name.ptr(),
              table_name.length(),
              table_name.ptr());
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(add_additional_function_according_to_type(column, expr, scope, true))) {
            LOG_WARN("fail to add additional function", K(ret), K(column));
          } else if (OB_FAIL(recursive_values_expr(expr))) {
            LOG_WARN("fail to resolve values expr", K(ret));
          } else {
            assignment.expr_ = expr;
            if (OB_FAIL(add_assignment(assigns, table, column, assignment))) {
              LOG_WARN("failed to add assignment", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

// Check the pad flag on generated_column is consistent with the sql_mode on session.
// For the upgraded cluster, the flag is not set, so only returns error if the dependent column
// is char type and the generated column is stored or used by an index
int ObDMLResolver::check_pad_generated_column(
    const ObSQLSessionInfo& session_info, const ObTableSchema& table_schema, const ObColumnSchemaV2& column_schema)
{
  int ret = OB_SUCCESS;
  if (!column_schema.is_generated_column()) {
    // do nothing
  } else if (is_pad_char_to_full_length(session_info.get_sql_mode()) ==
             column_schema.has_column_flag(PAD_WHEN_CALC_GENERATED_COLUMN_FLAG)) {
    // do nothing
  } else {
    bool has_char_dep_column = false;
    bool is_stored_column = column_schema.is_stored_generated_column();
    ObSEArray<uint64_t, 5> cascaded_columns;
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    if (OB_FAIL(column_schema.get_cascaded_column_ids(cascaded_columns))) {
      LOG_WARN("failed to get cascaded_column_ids", K(column_schema));
    } else if (OB_FAIL(table_schema.get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
      LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !has_char_dep_column && i < cascaded_columns.count(); ++i) {
      uint64_t column_id = cascaded_columns.at(i);
      const ObColumnSchemaV2 *cascaded_col_schema = table_schema.get_column_schema(column_id);
      if (OB_ISNULL(cascaded_col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column", K(table_schema), K(column_id), K(ret));
      } else if (ObCharType == cascaded_col_schema->get_data_type() ||
                 ObNCharType == cascaded_col_schema->get_data_type()) {
        has_char_dep_column = true;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_stored_column && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_table_schema = NULL;
      if (OB_FAIL(schema_checker_->get_table_schema(simple_index_infos.at(i).table_id_, index_table_schema))) {
        LOG_WARN("get_table_schema failed", "table id", simple_index_infos.at(i).table_id_, K(ret));
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema should not be null", K(ret));
      } else if (OB_FAIL(index_table_schema->has_column(column_schema.get_column_id(), is_stored_column))) {
        LOG_WARN("falied to check if column is in index schema", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (has_char_dep_column && is_stored_column) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("change PAD_CHAR option after created generated column",
          K(session_info.get_sql_mode()),
          K(column_schema),
          K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "change PAD_CHAR option after created generated column");
    }
  }
  return ret;
}

int ObDMLResolver::build_padding_expr(const ObSQLSessionInfo* session, const ColumnItem* column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(session));
  CK(OB_NOT_NULL(column));
  CK(OB_NOT_NULL(expr));
  CK(OB_NOT_NULL(get_stmt()));
  const TableItem* table_item = NULL;
  CK(OB_NOT_NULL(table_item = get_stmt()->get_table_item_by_id(column->table_id_)));
  if (OB_SUCC(ret)) {
    const ObColumnSchemaV2* column_schema = NULL;
    const uint64_t tid = OB_INVALID_ID == column->base_tid_ ? column->table_id_ : column->base_tid_;
    const uint64_t cid = OB_INVALID_ID == column->base_cid_ ? column->column_id_ : column->base_cid_;
    if (OB_FAIL(get_column_schema(tid, cid, column_schema, true))) {
      LOG_WARN("fail to get column schema", K(ret), K(*column));
    } else if (NULL == column_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get column schema fail", K(column_schema));
    } else if (OB_FAIL(build_padding_expr(session, column_schema, expr))) {
      LOG_WARN("fail to build padding expr", K(ret));
    }
  }
  return ret;
}

int ObDMLResolver::build_padding_expr(
    const ObSQLSessionInfo* session, const ObColumnSchemaV2* column_schema, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session) || OB_ISNULL(column_schema) || OB_ISNULL(expr) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(session), K(column_schema), K(expr), K_(params_.expr_factory));
  } else if (ObObjMeta::is_binary(column_schema->get_data_type(), column_schema->get_collation_type())) {
    if (OB_FAIL(
            ObRawExprUtils::build_pad_expr(*params_.expr_factory_, false, column_schema, expr, this->session_info_))) {
      LOG_WARN("fail to build pading expr for binary", K(ret));
    }
  } else if (ObCharType == column_schema->get_data_type() || ObNCharType == column_schema->get_data_type()) {
    if (is_pad_char_to_full_length(session->get_sql_mode())) {
      if (OB_FAIL(
              ObRawExprUtils::build_pad_expr(*params_.expr_factory_, true, column_schema, expr, this->session_info_))) {
        LOG_WARN("fail to build pading expr for char", K(ret));
      }
    } else {
      if (OB_FAIL(ObRawExprUtils::build_trim_expr(column_schema, *params_.expr_factory_, session_info_, expr))) {
        LOG_WARN("fail to build trime expr for char", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::build_nvl_expr(const ColumnItem* column_item, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(column_item) || OB_ISNULL(params_.expr_factory_) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column schema is NULL", K(column_item), K_(params_.expr_factory), K(stmt));
  } else if (column_item->get_column_type()->is_timestamp() && column_item->is_not_null()) {
    bool explicit_value = false;
    if (NULL == session_info_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info is NULL", K(ret));
    } else if (OB_FAIL(session_info_->get_explicit_defaults_for_timestamp(explicit_value))) {
      LOG_WARN("fail to get explicit_defaults_for_timestamp", K(ret));
    } else if (!explicit_value) {
      if (OB_FAIL(ObRawExprUtils::build_nvl_expr(*params_.expr_factory_, column_item, expr))) {
        LOG_WARN("fail to build nvl_expr", K(ret));
      } else if (OB_FAIL(expr->formalize(session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::build_nvl_expr(const ObColumnSchemaV2* column_schema, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(column_schema) || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column schema is NULL", K(column_schema), K(stmt));
  } else {
    ColumnItem* column_item = NULL;
    if (NULL ==
        (column_item = stmt->get_column_item_by_id(column_schema->get_table_id(), column_schema->get_column_id()))) {
      LOG_WARN("fail to get column item",
          K(ret),
          "table_id",
          column_schema->get_table_id(),
          "column_id",
          column_schema->get_column_id());
    } else if (OB_FAIL(build_nvl_expr(column_item, expr))) {
      LOG_WARN("fail to build nvl expr", K(ret));
    }
  }
  return ret;
}

int ObDMLResolver::recursive_values_expr(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
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

// handle c1 is null (c1 is a auto increment column)
int ObDMLResolver::resolve_autoincrement_column_is_null(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  bool sql_auto_is_null = false;
  if (OB_ISNULL(session_info_) || OB_ISNULL(params_.expr_factory_) || OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session info is NULL", K_(session_info), K_(params_.expr_factory), K(expr));
  } else if (!is_mysql_mode()) {
    // nothing to do
  } else if (OB_UNLIKELY(expr->get_expr_type() != T_OP_IS) || OB_ISNULL(expr->get_param_expr(0)) ||
             OB_ISNULL(expr->get_param_expr(1)) ||
             OB_UNLIKELY(expr->get_param_expr(0)->get_expr_type() != T_REF_COLUMN) ||
             OB_UNLIKELY(expr->get_param_expr(1)->get_expr_type() != T_NULL)) {
    LOG_WARN("invalid argument for resolve auto_increment column", K(*expr));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(session_info_->get_sql_auto_is_null(sql_auto_is_null))) {
    LOG_WARN("fail to get sql_auto_is_null", K(ret));
  } else if (!sql_auto_is_null) {
    // nothing to do
  } else if (OB_FAIL(ObRawExprUtils::build_equal_last_insert_id_expr(*params_.expr_factory_, expr, session_info_))) {
    LOG_WARN("fail to build eqaul last_insert_id_expr", K(ret), K(*expr));
  }
  return ret;
}

int ObDMLResolver::replace_column_to_default(ObRawExpr*& origin)
{
  int ret = OB_SUCCESS;
  UNUSED(origin);
  return ret;
}

int ObDMLResolver::add_rowkey_ids(const int64_t table_id, common::ObIArray<uint64_t>& array)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  if (OB_FAIL(schema_checker_->get_table_schema(table_id, table_schema))) {
    LOG_WARN("table schema not found", "table_id", table_id);
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get table schema error", K(table_id), K(table_schema));
  } else {
    uint64_t rowkey_column_id = 0;
    const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
    array.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
        LOG_WARN("get rowkey info failed", K(ret), K(i), K(rowkey_info));
        break;
      } else if (OB_FAIL(array.push_back(rowkey_column_id))) {
        LOG_WARN("fail to add primary key to array", K(ret), K(rowkey_column_id));
      }
    }
  }
  return ret;
}

int ObDMLResolver::get_part_key_ids(const int64_t table_id, common::ObIArray<uint64_t>& array)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  array.reuse();
  if (OB_FAIL(schema_checker_->get_table_schema(table_id, table_schema))) {
    LOG_WARN("table schema not found", "table_id", table_id);
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get table schema error", K(table_id), K(table_schema));
  } else if (table_schema->is_partitioned_table()) {
    uint64_t part_key_column_id = OB_INVALID_ID;
    if (OB_SUCC(ret)) {
      const ObPartitionKeyInfo& part_key_info = table_schema->get_partition_key_info();
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
      const ObPartitionKeyInfo& subpart_key_info = table_schema->get_subpartition_key_info();
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

int ObDMLResolver::session_variable_opt_influence()
{
  int ret = OB_SUCCESS;
  // deal variables influence optimizer
  if (!session_info_) {
    ret = OB_NOT_INIT;
  } else if (NULL == get_stmt()) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get stmt is null", K(ret));
  } else {
    ObStmtHint& stmt_hint = get_stmt()->get_stmt_hint();
    ObObj val;
    if (OB_FAIL(session_info_->get_sys_variable(SYS_VAR_OB_ENABLE_HASH_GROUP_BY, val))) {
      LOG_WARN("Get sys variable error", K(ret));
    } else if (!val.get_bool()) {
      stmt_hint.aggregate_ = OB_NO_USE_HASH_AGGREGATE;  // only merge group can do
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(session_info_->get_sys_variable(SYS_VAR_OB_ENABLE_BLK_NESTEDLOOP_JOIN, val))) {
        LOG_WARN("Get sys variable error", K(ret));
      } else if (val.get_bool()) {
        stmt_hint.bnl_allowed_ = true;
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_parallel_in_hint(const ParseNode* parallel_node, int64_t& parallel)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolver isn't init", K(session_info_));
  } else if (OB_ISNULL(parallel_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parallel_node is NULL", K(ret));
  } else if (parallel_node->value_ < 1) {
  } else {
    parallel = parallel_node->value_;
  }

  return ret;
}

int ObDMLResolver::resolve_tracing_hint(const ParseNode* tracing_node, common::ObIArray<uint64_t>& tracing_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tracing_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid tracing node", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tracing_node->num_child_; ++i) {
      ParseNode* node = tracing_node->children_[i];
      if (OB_ISNULL(node)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid tracing node", K(ret));
      } else if (node->value_ < 0) {
        // Invalid operator id, do nothing.
      } else if (OB_FAIL(add_var_to_array_no_dup(tracing_ids, (uint64_t)node->value_))) {
        LOG_WARN("Failed to add tracing id", K(ret));
      }
    }
    LOG_DEBUG("Resolve tracing hint", K(tracing_ids));
  }
  return ret;
}

int ObDMLResolver::parse_trans_param_hint(
    const ParseNode* param_name, const ParseNode* param_value, ObStmtHint& stmt_hint)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_name) || OB_ISNULL(param_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer.", K(ret));
  } else {
    ObString trans_param_name;
    ObString trans_param_value;
    trans_param_name.assign_ptr(param_name->str_value_, static_cast<int32_t>(param_name->str_len_));
    if (!trans_param_name.case_compare(ObStmtHint::ENABLE_EARLY_LOCK_RELEASE_HINT)) {
      if (param_value->type_ == T_VARCHAR) {
        trans_param_value.assign_ptr(param_value->str_value_, static_cast<int32_t>(param_value->str_len_));
        if (!trans_param_value.case_compare("true")) {
          stmt_hint.enable_lock_early_release_ = true;
        } else {
          stmt_hint.enable_lock_early_release_ = false;
        }
      } else { /*do nothing.*/
      }
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObDMLResolver::parse_tables_in_hint(const ParseNode* qb_name_node, const ParseNode* hint_table,
    common::ObIArray<ObTableInHint>& hint_table_arr, common::ObIArray<ObTablesInHint>& hint_tables_arr,
    common::ObIArray<std::pair<uint8_t, uint8_t>>& hint_pair_arr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(hint_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hint table should not be NULL", K(hint_table), K(ret));
  } else {
    if (NULL != qb_name_node) {
      ObString qb_name;
      if (OB_FAIL(resolve_qb_name_node(qb_name_node, qb_name))) {
        LOG_WARN("Failed to resolve qb name node", K(ret));
      } else if (!qb_name.empty()) {
        ObTablesInHint* tables_in_hint = NULL;
        // get the qb name hint tables arr.
        if (has_qb_name_in_hints(hint_tables_arr, tables_in_hint, qb_name)) {
          if (OB_FAIL(add_hint_table_list(hint_table, tables_in_hint->tables_, tables_in_hint->join_order_pairs_))) {
            LOG_WARN("Failed to add hint table list", K(ret));
          } else {  // do nothing.
          }
        } else if (OB_ISNULL(tables_in_hint = hint_tables_arr.alloc_place_holder())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Allocate ObTablesInHint from array error", K(ret));
        } else if (OB_FAIL(
                       add_hint_table_list(hint_table, tables_in_hint->tables_, tables_in_hint->join_order_pairs_))) {
          LOG_WARN("Failed to add hint table list", K(ret));
        } else {
          tables_in_hint->qb_name_ = qb_name;
        }  // do nothing.
      } else if (OB_FAIL(add_hint_table_list(hint_table, hint_table_arr, hint_pair_arr))) {
        LOG_WARN("Failed to add hint table list", K(ret));
      } else {
      }  // do nothing
    } else {
      if (OB_FAIL(add_hint_table_list(hint_table, hint_table_arr, hint_pair_arr))) {
        LOG_WARN("Failed to add hint table list", K(ret));
      }
    }
  }

  return ret;
}

int ObDMLResolver::parse_qb_in_rewrite_hint(const ParseNode* hint_node, common::ObIArray<ObString>& qb_names,
    ObUseRewriteHint::Type& type, const ObUseRewriteHint::Type hint_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hint_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hint node is null.", K(ret));
  } else if (OB_ISNULL(hint_node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the rewrite hint has no child.", K(ret));
  } else {
    ObString qb_name;
    ParseNode* qb_table_name = hint_node->children_[0];
    if (OB_ISNULL(qb_table_name)) {
      type = hint_type;
    } else if (T_OPT_QB_NAME != qb_table_name->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("qb table name is not the type.", K(ret));
    } else if (1 != qb_table_name->num_child_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("qb table name has no one children.", K(ret));
    } else if (OB_ISNULL(qb_table_name->children_[0])) {
      // allowed use hint like USE_CONCAT()
      type = hint_type;
    } else if (OB_FAIL(resolve_qb_name_node(qb_table_name->children_[0], qb_name))) {
      LOG_WARN("failed to resolve the rewrite hint.", K(ret));
    } else if (OB_FAIL(qb_names.push_back(qb_name))) {
      LOG_WARN("failed to push back qb name.", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObDMLResolver::add_hint_table_list(const ParseNode* table_list, common::ObIArray<ObTableInHint>& hint_table_list,
    common::ObIArray<std::pair<uint8_t, uint8_t>>& hint_pair_arr, bool is_in_leading)
{
  int ret = OB_SUCCESS;
  int64_t org_hint_len = 0;
  int64_t cur_hint_len = 0;
  int64_t cur_pair_idx = INT64_MAX;
  bool need_add_pair = true;
  const ParseNode* hint_table = NULL;
  if (OB_ISNULL(table_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table list should not be NULL", K(ret));
  } else if (T_RELATION_FACTOR_IN_USE_JOIN_HINT_LIST == table_list->type_) {
    const ParseNode* hint_table = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_list->num_child_; ++i) {
      hint_table = table_list->children_[i];
      if (OB_ISNULL(hint_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Hint table should not be NULL", K(ret));
      } else if (OB_FAIL(SMART_CALL(add_hint_table_list(hint_table, hint_table_list, hint_pair_arr, false)))) {
        LOG_WARN("Failed to add ObTableInHint", K(ret));
      } else {
      }  // do nothing
    }
  } else if (T_RELATION_FACTOR_IN_HINT_LIST == table_list->type_) {
    org_hint_len = hint_table_list.count();
    if ((org_hint_len <= 0 && is_in_leading) || org_hint_len >= UINT8_MAX) {
      need_add_pair = false;
    } else if (hint_pair_arr.count() > 0) {
      const std::pair<uint8_t, uint8_t>& last_pair = hint_pair_arr.at(hint_pair_arr.count() - 1);
      if (last_pair.first == org_hint_len)
        need_add_pair = false;
    }

    if (need_add_pair &&
        OB_SUCC(hint_pair_arr.push_back(std::make_pair(static_cast<uint8_t>(org_hint_len), UINT8_MAX)))) {
      cur_pair_idx = hint_pair_arr.count() - 1;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_list->num_child_; ++i) {
      hint_table = table_list->children_[i];
      if (OB_ISNULL(hint_table)) {
        /* possible, do nothing */
      } else if (OB_FAIL(SMART_CALL(add_hint_table_list(hint_table, hint_table_list, hint_pair_arr, is_in_leading)))) {
        LOG_WARN("Failed to add_leading_hint_table_list", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      cur_hint_len = hint_table_list.count();
      if (cur_pair_idx != INT64_MAX && cur_hint_len < UINT8_MAX) {
        std::pair<uint8_t, uint8_t>& leading_pair = hint_pair_arr.at(cur_pair_idx);
        if (leading_pair.second != UINT8_MAX) {
          LOG_WARN("Leading hint table changed already", "pari.second", leading_pair.second, "new value", cur_hint_len);
        } else {
          leading_pair.second = static_cast<uint8_t>(cur_hint_len - 1);
        }
      }
    } else { /* do nothing */
    }
  } else if (T_LINK_NODE == table_list->type_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_list->num_child_; ++i) {
      hint_table = table_list->children_[i];
      if (OB_ISNULL(hint_table)) {
        /* possible, do nothing */
      } else if (OB_FAIL(SMART_CALL(add_hint_table_list(hint_table, hint_table_list, hint_pair_arr, is_in_leading)))) {
        LOG_WARN("Failed to add_leading_hint_table_list", K(ret));
      }
    }
  } else if (T_RELATION_FACTOR_IN_HINT != table_list->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table list type should be T_RELATION_FACTOR_IN_HINT", "actual type is ", table_list->type_);
  } else {
    hint_table = table_list;
    if (OB_ISNULL(hint_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Hint table should not be NULL", K(ret));
    } else {
      ObString qb_name;
      ObString db_name;
      ObString table_name;
      if (OB_FAIL(resolve_table_relation_in_hint(hint_table, table_name, db_name, qb_name))) {
        LOG_WARN("Failed to resolve table relation in hint", K(ret));
      } else if (OB_FAIL(hint_table_list.push_back(ObTableInHint(qb_name, db_name, table_name)))) {
        LOG_WARN("Failed to add ObTableInHint", K(ret));
      } else { /* do nothing */
      }
    }

    bool need_add_pair_in_hint = true;
    if (hint_pair_arr.count() > 0) {
      std::pair<uint8_t, uint8_t>& leading_pair = hint_pair_arr.at(hint_pair_arr.count() - 1);
      if (leading_pair.second == UINT8_MAX) {
        need_add_pair_in_hint = false;
      } else {
      }  // do nothing.
    } else {
    }  // do nothing.

    if (OB_SUCC(ret) && need_add_pair_in_hint && !is_in_leading) {
      uint8_t hint_table_index = static_cast<uint8_t>(hint_table_list.count() - 1);
      if (OB_FAIL(hint_pair_arr.push_back(std::make_pair(hint_table_index, hint_table_index)))) {
        LOG_WARN("failed to push back the hint pair.");
      } else {
      }  // do nothing.
    }
  }
  return ret;
}

int ObDMLResolver::resolve_tables_in_hint(const ParseNode* table_list, common::ObIArray<ObTableInHint>& hint_table_arr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table list is null.", K(ret));
  } else if (T_LINK_NODE == table_list->type_ || T_RELATION_FACTOR_IN_HINT_LIST == table_list->type_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_list->num_child_; i++) {
      if (OB_FAIL(SMART_CALL(resolve_tables_in_hint(table_list->children_[i], hint_table_arr)))) {
        LOG_WARN("failed to resolve tables in hint.", K(ret));
      } else {
      }  // do nothing.
    }
  } else if (T_RELATION_FACTOR_IN_HINT == table_list->type_) {
    ObTableInHint table_in_hint;
    if (OB_FAIL(resolve_table_relation_in_hint(
            table_list, table_in_hint.table_name_, table_in_hint.db_name_, table_in_hint.qb_name_))) {
      LOG_WARN("resolve table relation failed", K(ret));
    } else if (OB_FAIL(hint_table_arr.push_back(table_in_hint))) {
      LOG_WARN("fail to push back into the hint table array.", K(ret));
    } else {
    }  // do nothing.
  } else {
  }  // do nothing.
  return ret;
}

int ObDMLResolver::resolve_pq_distribute_node(const ParseNode* qb_name_node, const ParseNode* table_list,
    const ParseNode* distribute_method_left, const ParseNode* distribute_method_right,
    common::ObIArray<ObOrgPQDistributeHint>& pq_distribute, common::ObIArray<ObQBNamePQDistributeHint>& pq_distributes)
{
  int ret = OB_SUCCESS;
  ObString qb_name;
  ObOrgPQDistributeHint pq_hint;

  if (OB_ISNULL(distribute_method_left)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL distribute method", K(ret));
  } else if (OB_FAIL(resolve_qb_name_node(qb_name_node, qb_name))) {
    LOG_WARN("failed to resolve query block name", K(qb_name), K(ret));
  } else if (OB_FAIL(resolve_tables_in_hint(table_list, pq_hint.tables_))) {
    LOG_WARN("failed to resolve tables in hint.", K(ret));
  } else {
    bool valid_distribute_method = true;
    if (NULL == distribute_method_right) {
      pq_hint.is_join_ = false;
      auto type = distribute_method_left->type_;
      if (T_DISTRIBUTE_NONE == type) {
        pq_hint.load_ = ObPQDistributeMethod::NONE;
      } else if (T_DISTRIBUTE_PARTITION == type) {
        pq_hint.load_ = ObPQDistributeMethod::PARTITION;
      } else if (T_RANDOM == type) {
        pq_hint.load_ = ObPQDistributeMethod::RANDOM;
      } else if (T_DISTRIBUTE_RANDOM_LOCAL == type) {
        pq_hint.load_ = ObPQDistributeMethod::RANDOM_LOCAL;
      } else {
        valid_distribute_method = false;
      }
    } else {
      pq_hint.is_join_ = true;
      auto outer = distribute_method_left->type_;
      auto inner = distribute_method_right->type_;
      if (T_DISTRIBUTE_HASH == outer && T_DISTRIBUTE_HASH == inner) {
        pq_hint.outer_ = ObPQDistributeMethod::HASH;
        pq_hint.inner_ = ObPQDistributeMethod::HASH;
      } else if (T_DISTRIBUTE_BROADCAST == outer && T_DISTRIBUTE_NONE == inner) {
        pq_hint.outer_ = ObPQDistributeMethod::BROADCAST;
        pq_hint.inner_ = ObPQDistributeMethod::NONE;
      } else if (T_DISTRIBUTE_NONE == outer && T_DISTRIBUTE_BROADCAST == inner) {
        pq_hint.outer_ = ObPQDistributeMethod::NONE;
        pq_hint.inner_ = ObPQDistributeMethod::BROADCAST;
      } else if (T_DISTRIBUTE_PARTITION == outer && T_DISTRIBUTE_NONE == inner) {
        pq_hint.outer_ = ObPQDistributeMethod::PARTITION;
        pq_hint.inner_ = ObPQDistributeMethod::NONE;
      } else if (T_DISTRIBUTE_NONE == outer && T_DISTRIBUTE_PARTITION == inner) {
        pq_hint.outer_ = ObPQDistributeMethod::NONE;
        pq_hint.inner_ = ObPQDistributeMethod::PARTITION;
      } else if (T_DISTRIBUTE_NONE == outer && T_DISTRIBUTE_NONE == inner) {
        pq_hint.outer_ = ObPQDistributeMethod::NONE;
        pq_hint.inner_ = ObPQDistributeMethod::NONE;
      } else {
        valid_distribute_method = false;
      }
    }
    if (OB_SUCC(ret) && valid_distribute_method) {
      if (qb_name.empty()) {
        if (OB_FAIL(pq_distribute.push_back(pq_hint))) {
          LOG_WARN("array push back failed", K(ret));
        }
      } else {
        if (OB_FAIL(pq_distributes.push_back(ObQBNamePQDistributeHint(qb_name, pq_hint)))) {
          LOG_WARN("array push back failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDMLResolver::resolve_qb_name_node(const ParseNode* qb_name_node, common::ObString& qb_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(qb_name_node)) {
    qb_name = ObString::make_empty_string();
  } else if (T_VARCHAR != qb_name_node->type_ && T_IDENT != qb_name_node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("qb name node type should be T_VARCHAR or T_IDENT", K(ret));
  } else {
    qb_name.assign_ptr(qb_name_node->str_value_, static_cast<int32_t>(qb_name_node->str_len_));
  }
  return ret;
}

int ObDMLResolver::resolve_table_relation_in_hint(
    const ParseNode* hint_table, ObString& table_name, ObString& db_name, ObString& qb_name)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(hint_table), T_RELATION_FACTOR_IN_HINT == hint_table->type_, 2 == hint_table->num_child_);

  OC((resolve_qb_name_node)(hint_table->children_[1], qb_name));
  bool is_db_explicit = false;
  OC((resolve_table_relation_node_v2)(hint_table->children_[0], table_name, db_name, is_db_explicit, true, false));

  return ret;
}
bool ObDMLResolver::is_need_add_additional_function(const ObRawExpr* expr)
{
  bool bret = false;
  if (OB_ISNULL(expr)) {
    LOG_WARN("invalid argument to check whether to add additional function", K(expr));
  } else if (T_FUN_COLUMN_CONV == expr->get_expr_type()) {
    bret = false;
  } else {
    bret = true;
  }
  return bret;
}

// can't add pad expr above child of column_conv in static engine, because the convert function
// of column_conv is based on cast expr. column_conv won't do cast operation.
// It calls eval_func of cast expr instead.
// eg: column_conv -> cast_expr -> child_expr, add pad expr directly may cover cast expr,results in
//     column_conv -> pad_expr -> cast_expr -> child_expr,
// so we need to erase inner expr first, then add pad, and formalize in the end. Final result is:
//     column_conv -> cast_expr -> pad_expr -> child_expr
int ObDMLResolver::try_add_padding_expr_for_column_conv(const ColumnItem* column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(session_info_));
  CK(OB_NOT_NULL(column));
  CK(OB_NOT_NULL(expr));
  if (OB_SUCC(ret) && T_FUN_COLUMN_CONV == expr->get_expr_type() && !stmt_->is_prepare_stmt()) {
    CK(ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO == expr->get_param_count() ||
        ObExprColumnConv::PARAMS_COUNT_WITHOUT_COLUMN_INFO == expr->get_param_count());
    CK(OB_NOT_NULL(expr->get_param_expr(4)));
    if (OB_SUCC(ret) && session_info_->use_static_typing_engine()) {
      ObRawExpr*& ori_child = expr->get_param_expr(4);
      ObRawExpr* real_child = NULL;
      OZ(ObRawExprUtils::erase_inner_added_exprs(ori_child, real_child));
      CK(OB_NOT_NULL(real_child));
      if (OB_SUCC(ret) && real_child->get_expr_type() != T_FUN_PAD && real_child->get_expr_type() != T_FUN_INNER_TRIM) {
        if (OB_FAIL(build_padding_expr(session_info_, column, real_child))) {
          LOG_WARN("fail to build padding expr", K(ret));
        } else {
          ObRawExpr*& ref_child = expr->get_param_expr(4);
          CK(OB_NOT_NULL(ref_child));
          CK(OB_NOT_NULL(real_child));
          OX(ref_child = real_child);
          OZ(expr->formalize(session_info_));
        }
      }
    } else if (OB_SUCC(ret)) {
      ObRawExpr*& ori_child = expr->get_param_expr(4);
      if (ori_child->get_expr_type() != T_FUN_PAD && ori_child->get_expr_type() != T_FUN_INNER_TRIM) {
        if (OB_FAIL(build_padding_expr(session_info_, column, ori_child))) {
          LOG_WARN("fail to build padding expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::add_additional_function_according_to_type(
    const ColumnItem* column, ObRawExpr*& expr, ObStmtScope scope, bool need_padding)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column) || OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column), K(expr));
  } else if (OB_ISNULL(column->expr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column expr", K(ret), K(column), K(column->expr_));
  } else if (!is_need_add_additional_function(expr)) {
    if (need_padding && OB_FAIL(try_add_padding_expr_for_column_conv(column, expr))) {
      LOG_WARN("fail try add padding expr for column conv expr", K(ret));
    }
  } else {
    if (OB_SUCC(ret)) {
      if (T_INSERT_SCOPE == scope && column->is_auto_increment()) {
        if (session_info_->use_static_typing_engine()) {
          // In the old engine, nextval() expr returned ObObj with different types:
          // return ObUInt64Type for generate type if input obj is zero or the original input obj.
          // Not acceptable in static typing engine, so convert to the defined data type first.
          if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(
                  *params_.expr_factory_, *params_.allocator_, *column->get_expr(), expr, session_info_))) {
            LOG_WARN("fail to build column conv expr", K(ret), K(column));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(build_autoinc_nextval_expr(expr, column->base_cid_))) {
          LOG_WARN("fail to build nextval expr", K(ret), K(column->base_cid_));
        }
      } else if (column->get_column_type()->is_timestamp()) {
        if (OB_FAIL(build_nvl_expr(column, expr))) {
          LOG_WARN("fail to build nvl expr", K(ret), K(column), K(*expr));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (need_padding && !stmt_->is_prepare_stmt()) {
        OZ(build_padding_expr(session_info_, column, expr));
      }
      OZ(ObRawExprUtils::build_column_conv_expr(
          *params_.expr_factory_, *params_.allocator_, *column->get_expr(), expr, session_info_));
    }
  }  // end else
  return ret;
}

int ObDMLResolver::resolve_fun_match_against_expr(ObFunMatchAgainst& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr.get_param_expr(0)) || expr.get_param_expr(0)->get_expr_type() != T_OP_ROW ||
      OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column list is invalid", K(expr.get_param_expr(0)), K(get_stmt()));
  } else {
    const TableItem* table_item = NULL;
    ColumnItem* column_item = NULL;
    ObOpRawExpr* column_list = static_cast<ObOpRawExpr*>(expr.get_param_expr(0));
    if (column_list->get_param_count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column list is empty", K(column_list->get_param_count()));
    } else {
      uint64_t table_id = OB_INVALID_ID;
      ColumnReferenceSet column_set;
      const ObColumnSchemaV2* fulltext_col = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_list->get_param_count(); ++i) {
        // check all table id
        ObColumnRefRawExpr* col_ref = NULL;
        if (OB_ISNULL(column_list->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column list param is invalid", K(column_list->get_param_expr(i)));
        } else if (!column_list->get_param_expr(i)->is_column_ref_expr()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MATCH");
        } else if (FALSE_IT(col_ref = static_cast<ObColumnRefRawExpr*>(column_list->get_param_expr(i)))) {
          // do nothing
        } else if (OB_FAIL(column_set.add_member(col_ref->get_column_id() - OB_APP_MIN_COLUMN_ID))) {
          LOG_WARN("add to column set failed", K(ret));
        } else if (0 == i) {
          table_id = col_ref->get_table_id();
        } else if (col_ref->get_table_id() != table_id) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MATCH");
        }
      }
      // get fulltext column
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_ISNULL(table_item = get_stmt()->get_table_item_by_id(table_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item don't exist", K(table_id));
      } else if (!table_item->is_basic_table()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MATCH");
      } else if (OB_FAIL(schema_checker_->get_fulltext_column(table_item->ref_id_, column_set, fulltext_col))) {
        LOG_WARN("get fulltext column schema failed", K(ret));
      } else if (OB_ISNULL(fulltext_col)) {
        ret = OB_ERR_KEY_DOES_NOT_EXISTS;
        LOG_WARN("fulltext column don't exist");
      } else if (OB_FAIL(
                     resolve_basic_column_item(*table_item, fulltext_col->get_column_name_str(), true, column_item))) {
        LOG_WARN("resolve basic column item failed", K(ret));
      } else {
        expr.set_real_column(column_item->expr_);
      }
    }
    // create fulltext key
    if (OB_SUCC(ret)) {
      ObRawExprCtxCatAnalyzer ctxcat_analyzer(*params_.expr_factory_, session_info_);
      if (OB_FAIL(ctxcat_analyzer.create_fulltext_filter(expr))) {
        LOG_WARN("splite search keywords failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_generated_column_expr(const ObString& expr_str, const TableItem& table_item,
    const ObColumnSchemaV2* column_schema, const ObColumnRefRawExpr& column, ObRawExpr*& ref_expr,
    const bool used_for_generated_column, ObDMLStmt* stmt /* = NULL */)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  ObRawExprFactory* expr_factory = NULL;
  ObSQLSessionInfo* session_info = NULL;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(expr_factory = params_.expr_factory_) || OB_ISNULL(session_info = params_.session_info_) ||
      OB_ISNULL(schema_checker_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expr_factory is null", K_(params_.expr_factory), K_(params_.session_info));
  } else if (OB_NOT_NULL(column_schema) &&
             OB_FAIL(schema_checker_->get_table_schema(column_schema->get_table_id(), table_schema))) {
    LOG_WARN("get table schema error", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(
                 expr_str, *expr_factory, *session_info, ref_expr, columns, schema_checker_))) {
    LOG_WARN("build generated column expr failed", K(ret));
  } else if (!used_for_generated_column && !columns.empty()) {
    bool is_all_sys_func = true;
    for (int64_t i = 0; is_all_sys_func && i < columns.count(); i++) {
      is_all_sys_func = columns.at(i).is_sys_func();
    }

    if (is_all_sys_func) {
      // do nothing
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no need referece other column, it should not happened", K(expr_str), K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    ColumnItem* col_item = NULL;
    ObRawExpr* real_ref_expr = NULL;
    ObArray<ObRawExpr*> real_exprs;
    if (columns.at(i).is_sys_func()) {
      if (OB_FAIL(resolve_qualified_identifier(columns.at(i), columns, real_exprs, real_ref_expr))) {
        LOG_WARN("resolve sysfunc expr failed", K(columns.at(i)), K(ret));
      } else if (OB_ISNULL(real_ref_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(ret));
      } else if (!real_ref_expr->is_sys_func_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid exor", K(*real_ref_expr), K(ret));
      } else {
        ObSysFunRawExpr* sys_func_expr = static_cast<ObSysFunRawExpr*>(real_ref_expr);
        if (OB_FAIL(sys_func_expr->check_param_num())) {
          LOG_WARN("sys func check param failed", K(ret));
        }
      }
    } else {
      if (OB_FAIL(resolve_basic_column_item(table_item, columns.at(i).col_name_, false, col_item, stmt))) {
        LOG_WARN("resolve basic column item failed", K(ret));
      } else if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is null", K(col_item));
      } else {
        real_ref_expr = col_item->expr_;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(real_exprs.push_back(ref_expr))) {
        LOG_WARN("push back error", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(ref_expr, columns.at(i).ref_expr_, real_ref_expr))) {
        LOG_WARN("replace column reference expr failed", K(ret));
      } else { /*do nothing*/
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_pad_generated_column(*session_info, *table_schema, *column_schema))) {
      LOG_WARN("check pad generated column failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_pad_expr_recursively(
                   *expr_factory, *session_info, *table_schema, *column_schema, ref_expr))) {
      LOG_WARN("build padding expr for column_ref failed", K(ret));
    } else if (OB_FAIL(build_padding_expr(session_info, column_schema, ref_expr))) {
      LOG_WARN("build padding expr for self failed", K(ret));
    } else if (OB_FAIL(ref_expr->formalize(session_info))) {
      LOG_WARN("formailize column reference expr failed", K(ret));
    } else if (ObRawExprUtils::need_column_conv(column.get_result_type(), *ref_expr)) {
      if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(*expr_factory, *allocator_, column, ref_expr, session_info))) {
        LOG_WARN("build column convert expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_generated_column_expr_temp(const TableItem& table_item, const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  bool has_index = false;
  const ObColumnSchemaV2* col_schema = NULL;
  if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(schema_checker_) || OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml resolver param isn't inited", K_(params_.expr_factory), K_(schema_checker), K_(params_.session_info));
  } else if (table_schema.has_generated_column()) {
    ObArray<uint64_t> column_ids;
    ObRawExpr* expr = NULL;
    if (OB_FAIL(table_schema.get_generated_column_ids(column_ids))) {
      LOG_WARN("get generated column ids failed", K(ret));
    }
    const bool is_insert_new_no_pk_table = get_stmt()->is_insert_stmt() && table_schema.is_new_no_pk_table();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      ObString expr_def;
      if (OB_ISNULL(col_schema = table_schema.get_column_schema(column_ids.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get column schema failed", K(column_ids.at(i)));
      } else if (!col_schema->is_generated_column()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is not generated column", K(*col_schema));
      } else if (OB_FAIL(schema_checker_->check_column_has_index(
                     col_schema->get_table_id(), col_schema->get_column_id(), has_index))) {
        LOG_WARN("check column whether has index failed", K(ret));
      } else if (!col_schema->is_stored_generated_column() && !has_index &&
                 !(is_insert_new_no_pk_table && col_schema->is_tbl_part_key_column())) {
        // do nothing
      } else if (OB_FAIL(col_schema->get_cur_default_value().get_string(expr_def))) {
        LOG_WARN("get string from current default value failed", K(ret), K(col_schema->get_cur_default_value()));
      } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(expr_def,
                     *params_.expr_factory_,
                     *params_.session_info_,
                     table_item.table_id_,
                     table_schema,
                     *col_schema,
                     expr,
                     schema_checker_))) {
        LOG_WARN("build generated column expr failed", K(ret));
      } else if (is_insert_new_no_pk_table && col_schema->is_tbl_part_key_column() &&
                 OB_FAIL(get_all_column_ref(
                     expr, static_cast<ObInsertStmt*>(get_stmt())->get_part_generated_col_dep_cols()))) {
        LOG_WARN("get all column ref failed", K(ret));
      } else {
        GenColumnNamePair gen_column_name_pair(table_item, col_schema->get_column_name_str());
        GenColumnExprPair gen_col_expr_pair(expr, gen_column_name_pair);
        ret = gen_col_exprs_.push_back(gen_col_expr_pair);
      }
    }
  }
  return ret;
}

int ObDMLResolver::find_generated_column_expr(ObRawExpr*& expr, bool& is_found)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(expr));

  is_found = false;
  if (current_scope_ != T_INSERT_SCOPE && current_scope_ != T_UPDATE_SCOPE &&
      !(expr->has_flag(IS_CONST) || expr->has_flag(IS_CONST_EXPR))) {
    // find all the possible const param constraint first
    OC((find_const_params_for_gen_column)(*expr));

    int64_t found_idx = 0;
    ObColumnRefRawExpr* ref_expr = NULL;
    ObExprEqualCheckContext check_ctx;
    check_ctx.override_const_compare_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < gen_col_exprs_.count(); ++i) {
      if (OB_ISNULL(gen_col_exprs_.at(i).first)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("gen col expr is null");
      } else if (gen_col_exprs_.at(i).first->same_as(*expr, &check_ctx) && OB_SUCC(check_ctx.err_code_)) {
        is_found = true;
        found_idx = i;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("compare expr same as failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_found) {
      // if found, store the const params
      CK(OB_NOT_NULL(params_.param_list_));
      CK(OB_NOT_NULL(params_.query_ctx_));
      ObPCConstParamInfo const_param_info;
      ObObj const_param;
      for (int64_t i = 0; OB_SUCC(ret) && i < check_ctx.param_expr_.count(); i++) {
        int64_t param_idx = check_ctx.param_expr_.at(i).param_idx_;
        CK(param_idx < params_.param_list_->count());
        if (OB_SUCC(ret)) {
          const_param.meta_ = params_.param_list_->at(i).meta_;
          const_param.v_ = params_.param_list_->at(param_idx).v_;
        }
        OC((const_param_info.const_idx_.push_back)(param_idx));
        OC((const_param_info.const_params_.push_back)(const_param));
      }
      if (check_ctx.param_expr_.count() > 0) {
        OC((params_.query_ctx_->all_plan_const_param_constraints_.push_back)(const_param_info));
        LOG_DEBUG("plan const constraint", K(params_.query_ctx_->all_plan_const_param_constraints_));
      }

      const TableItem& table_item = gen_col_exprs_.at(found_idx).second.first;
      const ObString& column_name = gen_col_exprs_.at(found_idx).second.second;
      ColumnItem* col_item = NULL;
      if (OB_FAIL(resolve_basic_column_item(table_item, column_name, true, col_item))) {
        LOG_WARN("resolve basic column item failed", K(ret), K(table_item), K(column_name));
      } else if (OB_ISNULL(col_item) || OB_ISNULL(ref_expr = col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_item is invalid", K(col_item), K(ref_expr));
      } else {
        expr = ref_expr;
      }
    }
  }
  return ret;
}

int ObDMLResolver::find_const_params_for_gen_column(const ObRawExpr& expr)
{
  int ret = OB_SUCCESS;

  CK(OB_NOT_NULL(params_.query_ctx_));

  for (int64_t i = 0; OB_SUCC(ret) && i < gen_col_exprs_.count(); i++) {
    CK(OB_NOT_NULL(gen_col_exprs_.at(i).first));
    ObExprEqualCheckContext check_context;
    ObPCConstParamInfo const_param_info;

    check_context.err_code_ = OB_SUCCESS;
    check_context.override_const_compare_ = false;

    if (OB_SUCC(ret) && gen_col_exprs_.at(i).first->same_as(expr, &check_context)) {
      if (OB_FAIL(check_context.err_code_)) {
        LOG_WARN("failed to compare exprs", K(ret));
      } else if (check_context.param_expr_.count() > 0) {
        // generate column may not contain const param, so check this
        const_param_info.const_idx_.reset();
        const_param_info.const_params_.reset();
        for (int64_t i = 0; OB_SUCC(ret) && i < check_context.param_expr_.count(); i++) {
          ObExprEqualCheckContext::ParamExprPair& param_expr = check_context.param_expr_.at(i);
          CK(OB_NOT_NULL(param_expr.expr_), param_expr.param_idx_ >= 0);
          if (OB_SUCC(ret)) {
            OC((const_param_info.const_idx_.push_back)(param_expr.param_idx_));

            const ObConstRawExpr* c_expr = dynamic_cast<const ObConstRawExpr*>(param_expr.expr_);
            CK(OB_NOT_NULL(c_expr));
            OC((const_param_info.const_params_.push_back)(c_expr->get_value()));
          }
        }
        OC((params_.query_ctx_->all_possible_const_param_constraints_.push_back)(const_param_info));
        LOG_DEBUG("found all const param constraints", K(params_.query_ctx_->all_possible_const_param_constraints_));
      }
    }
  }
  return ret;
}

int ObDMLResolver::deduce_generated_exprs(ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> generate_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && !params_.is_from_create_view_ && i < exprs.count(); ++i) {
    ObRawExpr* expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(exprs), K(i), K(expr), K(ret));
    } else {
      ObColumnRefRawExpr* column_expr = NULL;
      ObRawExpr* value_expr = NULL;
      ObRawExpr* escape_expr = NULL;
      ObRawExpr* param_expr1 = NULL;
      ObRawExpr* param_expr2 = NULL;
      ObItemType type = expr->get_expr_type();
      // Oracle mode not support lob in/not in
      if (share::is_oracle_mode() && (T_OP_IN == expr->get_expr_type() || T_OP_NOT_IN == expr->get_expr_type() ||
                                         T_OP_EQ == expr->get_expr_type() || T_OP_NE == expr->get_expr_type() ||
                                         T_OP_SQ_EQ == expr->get_expr_type() || T_OP_SQ_NE == expr->get_expr_type())) {
        if (OB_ISNULL(param_expr1 = expr->get_param_expr(0)) || OB_ISNULL(param_expr2 = expr->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(ret), K(*expr), K(expr->get_param_expr(0)), K(expr->get_param_expr(1)));
        } else if (param_expr1->get_result_type().is_lob() || param_expr2->get_result_type().is_lob() ||
                   param_expr1->get_result_type().is_lob_locator() || param_expr2->get_result_type().is_lob_locator()) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("oracle lob can't be the param of this operation type",
              K(ret),
              K(expr->get_expr_type()),
              KPC(param_expr1),
              KPC(param_expr2),
              K(param_expr1->get_result_type()),
              K(param_expr2->get_result_type()));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (IS_BASIC_CMP_OP(expr->get_expr_type()) || IS_SINGLE_VALUE_OP(expr->get_expr_type())) {
        // only =/</<=/>/>=/IN/like can deduce generated exprs
        if (OB_ISNULL(param_expr1 = expr->get_param_expr(0)) || OB_ISNULL(param_expr2 = expr->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(*expr), K(expr->get_param_expr(0)), K(expr->get_param_expr(1)), K(ret));
        } else if (T_OP_LIKE == expr->get_expr_type()) {
          /*
          err1: should add const expr for expr2
          err2: if expr2 is 'a%d' deduce is error
                c1 like 'a%d' DOESN'T MEAN:
                substr(c1, 1, n) like substr('a%d', 1, n)
                because after %, there is normal char.
          */
          column_expr = NULL;
        } else if (T_OP_IN == expr->get_expr_type()) {
          if (T_OP_ROW == param_expr2->get_expr_type() && !param_expr2->has_generalized_column() &&
              param_expr1->get_result_type().is_string_type()) {
            bool all_match = true;
            for (int64_t j = 0; OB_SUCC(ret) && all_match && j < param_expr2->get_param_count(); ++j) {
              if (OB_ISNULL(param_expr2->get_param_expr(j))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("param expr2 is null");
              } else if (!param_expr2->get_param_expr(j)->get_result_type().is_string_type() ||
                         param_expr2->get_param_expr(j)->get_collation_type() != param_expr1->get_collation_type()) {
                all_match = false;
              }
            }
            if (OB_SUCC(ret) && all_match) {
              column_expr = static_cast<ObColumnRefRawExpr*>(expr->get_param_expr(0));
              value_expr = expr->get_param_expr(1);
            }
          }
        } else if (param_expr1->is_column_ref_expr() && param_expr2->is_const_expr()) {
          if (param_expr1->get_result_type().is_string_type()  // only for string and same collation
              && param_expr2->get_result_type().is_string_type() &&
              param_expr1->get_collation_type() == param_expr2->get_collation_type()) {
            column_expr = static_cast<ObColumnRefRawExpr*>(expr->get_param_expr(0));
            value_expr = expr->get_param_expr(1);
          }
        } else if (param_expr1->is_const_expr() && param_expr2->is_column_ref_expr()) {
          if (param_expr1->get_result_type().is_string_type() && param_expr2->get_result_type().is_string_type() &&
              param_expr1->get_collation_type() == param_expr2->get_collation_type()) {
            type = get_opposite_compare_type(expr->get_expr_type());
            column_expr = static_cast<ObColumnRefRawExpr*>(expr->get_param_expr(1));
            value_expr = expr->get_param_expr(0);
          }
        }

        // only column op const
        if (OB_SUCC(ret) && NULL != column_expr && column_expr->has_generated_column_deps()) {
          for (int64_t j = 0; OB_SUCC(ret) && j < gen_col_exprs_.count(); ++j) {
            const ObRawExpr* dep_expr = gen_col_exprs_.at(j).first;
            const ObRawExpr* substr_expr = NULL;
            if (OB_ISNULL(dep_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("generated column expr is null", K(gen_col_exprs_), K(j), K(ret));
            } else if (ObRawExprUtils::has_prefix_str_expr(*dep_expr, *column_expr, substr_expr)) {
              ObRawExpr* new_expr = NULL;
              ObColumnRefRawExpr* left_expr = NULL;
              const TableItem& table_item = gen_col_exprs_.at(j).second.first;
              const ObString& column_name = gen_col_exprs_.at(j).second.second;
              ColumnItem* col_item = NULL;
              ObItemType gen_type = type;
              if (T_OP_GT == type) {
                gen_type = T_OP_GE;
              } else if (T_OP_LT == type) {
                gen_type = T_OP_LE;
              }
              if (OB_FAIL(resolve_basic_column_item(table_item, column_name, true, col_item))) {
                LOG_WARN("resolve basic column reference failed", K(ret));
              } else if (OB_ISNULL(col_item) || OB_ISNULL(left_expr = col_item->expr_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("column item is invalid", K(col_item), K(left_expr));
              } else if (OB_FAIL(build_prefix_index_compare_expr(*left_expr,
                             const_cast<ObRawExpr*>(substr_expr),
                             gen_type,
                             *value_expr,
                             escape_expr,
                             new_expr))) {
                LOG_WARN("build prefix index compare expr failed", K(ret));
              } else if (OB_FAIL(generate_exprs.push_back(new_expr))) {
                LOG_WARN("push back error", K(ret));
              } else { /*do nothing*/
              }
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(exprs, generate_exprs))) {
      LOG_WARN("append error", K(ret));
    } else if (OB_ISNULL(get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get stmt is null", K(get_stmt()), K(ret));
    } else if (OB_FAIL(get_stmt()->add_deduced_exprs(generate_exprs))) {
      LOG_WARN("add generated exprs failed", K(generate_exprs), K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObDMLResolver::add_synonym_obj_id(const ObSynonymChecker& synonym_checker, bool error_with_exist)
{
  int ret = OB_SUCCESS;
  if (synonym_checker.has_synonym()) {
    ObStmt* stmt = get_basic_stmt();
    if (OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is NULL", K(ret));
    } else if (OB_FAIL(stmt->add_synonym_ids(synonym_checker.get_synonym_ids(), error_with_exist))) {
      LOG_WARN("fail to add synonym ids", K(ret));
    } else if (OB_FAIL(add_object_version_to_dependency(
                   DEPENDENCY_SYNONYM, SYNONYM_SCHEMA, stmt->get_synonym_id_store()))) {
      LOG_WARN("add synonym version failed", K(ret));
    }
  }
  return ret;
}

int ObDMLResolver::resolve_table_relation_factor(const ParseNode* node, uint64_t& dblink_id, uint64_t& database_id,
    ObString& table_name, ObString& synonym_name, ObString& synonym_db_name, ObString& db_name, ObString& dblink_name)
{
  bool is_db_explicit = false;
  UNUSED(is_db_explicit);
  return resolve_table_relation_factor(
      node, dblink_id, database_id, table_name, synonym_name, synonym_db_name, db_name, dblink_name, is_db_explicit);
}

int ObDMLResolver::resolve_table_relation_factor(const ParseNode* node, uint64_t tenant_id, uint64_t& dblink_id,
    uint64_t& database_id, common::ObString& table_name, common::ObString& synonym_name,
    common::ObString& synonym_db_name, common::ObString& db_name, common::ObString& dblink_name,
    ObSynonymChecker& synonym_checker)
{
  bool is_db_explicit = false;
  return resolve_table_relation_factor(node,
      tenant_id,
      dblink_id,
      database_id,
      table_name,
      synonym_name,
      synonym_db_name,
      db_name,
      dblink_name,
      is_db_explicit,
      synonym_checker);
}

int ObDMLResolver::resolve_table_relation_factor(const ParseNode* node, uint64_t tenant_id, uint64_t& dblink_id,
    uint64_t& database_id, common::ObString& table_name, common::ObString& synonym_name,
    common::ObString& synonym_db_name, common::ObString& dblink_name, common::ObString& db_name)
{
  bool is_db_explicit = false;
  UNUSED(is_db_explicit);
  return resolve_table_relation_factor(node,
      tenant_id,
      dblink_id,
      database_id,
      table_name,
      synonym_name,
      synonym_db_name,
      db_name,
      dblink_name,
      is_db_explicit);
}

int ObDMLResolver::resolve_table_relation_factor(const ParseNode* node, uint64_t& dblink_id, uint64_t& database_id,
    ObString& table_name, ObString& synonym_name, ObString& synonym_db_name, ObString& db_name, ObString& dblink_name,
    bool& is_db_explicit)
{
  return resolve_table_relation_factor(node,
      session_info_->get_effective_tenant_id(),
      dblink_id,
      database_id,
      table_name,
      synonym_name,
      synonym_db_name,
      db_name,
      dblink_name,
      is_db_explicit);
}

// add object in the schema version depend set.
// when schema version of object changes, check whether need to generate object again with depend set.
int ObDMLResolver::add_object_version_to_dependency(
    ObDependencyTableType table_type, ObSchemaType schema_type, const ObIArray<uint64_t>& object_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(schema_checker_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt or schema_checker is null", K(get_stmt()), K_(schema_checker));
  } else {
    int64_t schema_version = OB_INVALID_VERSION;
    ObSchemaObjVersion obj_version;
    for (int64_t i = 0; OB_SUCC(ret) && i < object_ids.count(); ++i) {
      if (OB_FAIL(schema_checker_->get_schema_version(object_ids.at(i), schema_type, schema_version))) {
        LOG_WARN("get schema version failed", K(object_ids.at(i)), K(table_type), K(schema_type), K(ret));
      } else if (OB_UNLIKELY(OB_INVALID_VERSION == schema_version)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("object schema is unknown", K(object_ids.at(i)), K(table_type), K(schema_type), K(ret));
      } else {
        obj_version.object_id_ = object_ids.at(i);
        obj_version.object_type_ = table_type, obj_version.version_ = schema_version;
        if (with_clause_without_record_) {
          // do nothing
        } else if (OB_FAIL(get_stmt()->add_global_dependency_table(obj_version))) {
          LOG_WARN("add global dependency table failed", K(ret), K(table_type), K(schema_type));
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_table_relation_factor(const ParseNode* node, uint64_t tenant_id, uint64_t& dblink_id,
    uint64_t& database_id, ObString& table_name, ObString& synonym_name, ObString& synonym_db_name, ObString& db_name,
    ObString& dblink_name, bool& is_db_explicit)
{
  ObSynonymChecker synonym_checker;
  return resolve_table_relation_factor(node,
      tenant_id,
      dblink_id,
      database_id,
      table_name,
      synonym_name,
      synonym_db_name,
      db_name,
      dblink_name,
      is_db_explicit,
      synonym_checker);
}

int ObDMLResolver::resolve_table_relation_factor(const ParseNode* node, uint64_t tenant_id, uint64_t& dblink_id,
    uint64_t& database_id, ObString& table_name, ObString& synonym_name, ObString& synonym_db_name, ObString& db_name,
    ObString& dblink_name, bool& is_db_explicit, ObSynonymChecker& synonym_checker)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret));
  } else if (OB_FAIL(resolve_dblink_name(node, dblink_name))) {
    LOG_WARN("resolve dblink name failed", K(ret));
  } else {
    if (dblink_name.empty()) {
      if (OB_FAIL(resolve_table_relation_factor_normal(node,
              tenant_id,
              database_id,
              table_name,
              synonym_name,
              synonym_db_name,
              db_name,
              is_db_explicit,
              synonym_checker))) {
        LOG_WARN("resolve table relation factor failed", K(ret));
        // table_name may be dblink table, here to test is,
        int tmp_ret = ret;
        ret = OB_SUCCESS;
        if (OB_FAIL(resolve_dblink_with_synonym(tenant_id, table_name, dblink_name, db_name, dblink_id))) {
          LOG_WARN("try synonym with dblink failed", K(ret));
          ret = tmp_ret;
        }
      }
    } else {
      if (OB_FAIL(resolve_table_relation_factor_dblink(node, tenant_id, dblink_name, dblink_id, table_name, db_name))) {
        LOG_WARN("resolve table relation factor from dblink failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_dblink_name(const ParseNode* table_node, ObString& dblink_name)
{
  int ret = OB_SUCCESS;
  dblink_name.reset();
  if (!OB_ISNULL(table_node) && table_node->num_child_ > 2 && !OB_ISNULL(table_node->children_) &&
      !OB_ISNULL(table_node->children_[2])) {
    int32_t dblink_name_len = static_cast<int32_t>(table_node->children_[2]->str_len_);
    dblink_name.assign_ptr(table_node->children_[2]->str_value_, dblink_name_len);
  }
  return ret;
}

int ObDMLResolver::resolve_dblink_with_synonym(
    uint64_t tenant_id, ObString& table_name, ObString& dblink_name, ObString& db_name, uint64_t& dblink_id)
{
  int ret = OB_SUCCESS;
  // dblink name must be something like 'db_name.tbl_name@dblink', or 'tbl_name@dblink'
  ObString tmp_table_name;
  ObString tmp_db_name;
  CK(OB_NOT_NULL(allocator_));
  OZ(ob_write_string(*allocator_, table_name, tmp_table_name));
  ObString tbl_sch_name = tmp_table_name.split_on('@');
  if (tmp_table_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    OZ(schema_checker_->get_dblink_id(tenant_id, tmp_table_name, dblink_id));
    OZ(schema_checker_->get_dblink_user(tenant_id, tmp_table_name, tmp_db_name, *allocator_));
    if (OB_FAIL(ret)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_INVALID_ID == dblink_id) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      OZ(ob_write_string(*allocator_, tmp_table_name, dblink_name));
      ObString remote_schema_name;
      CK(!tbl_sch_name.empty());
      OX(remote_schema_name = tbl_sch_name.split_on('.'));
      if (OB_FAIL(ret)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        if (!remote_schema_name.empty()) {
          if (0 != tmp_db_name.case_compare(remote_schema_name)) {
            ret = OB_ERR_UNEXPECTED;
          } else { /*do nothing*/
          }
        }
        // convert db_name to upper, for the field in all_object is upper
        if (OB_SUCC(ret)) {
          char letter;
          char* src_ptr = tmp_db_name.ptr();
          for (ObString::obstr_size_t i = 0; i < tmp_db_name.length(); ++i) {
            letter = src_ptr[i];
            if (letter >= 'a' && letter <= 'z') {
              src_ptr[i] = static_cast<char>(letter - 32);
            }
          }
        }
        OZ(ob_write_string(*allocator_, tbl_sch_name, table_name));
        OZ(ob_write_string(*allocator_, tmp_db_name, db_name));
      }
    }
  }
  return ret;
}

int ObDMLResolver::check_unique_index(const ObIArray<ObColumnRefRawExpr*>& column_exprs, bool& has_unique_index) const
{
  int ret = OB_SUCCESS;
  for (int64_t j = 0; OB_SUCC(ret) && j < column_exprs.count(); ++j) {
    if (NULL == column_exprs.at(j)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(ret));
    } else {
      ObRawExpr* target_expr = column_exprs.at(j);
      if (target_expr->is_column_ref_expr()) {
        ObColumnRefRawExpr* column_ref_expr = (ObColumnRefRawExpr*)(target_expr);
        if (column_ref_expr->is_virtual_generated_column() && !OB_ISNULL(column_ref_expr->get_dependant_expr()) &&
            column_ref_expr->get_dependant_expr()->get_expr_type() == T_OP_SHADOW_UK_PROJECT) {
          has_unique_index = true;
          break;
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::is_open_all_pdml_feature(ObDelUpdStmt* dml_stmt, bool& is_open) const
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  const TableItem* table_item = NULL;
  bool enable_all_pdml_feature = false;
  ret = E(EventTable::EN_ENABLE_PDML_ALL_FEATURE) OB_SUCCESS;
  LOG_TRACE("event: check pdml all feature", K(ret));
  if (OB_FAIL(ret)) {
    enable_all_pdml_feature = true;
    ret = OB_SUCCESS;
  }
  LOG_TRACE("event: check pdml all feature result", K(ret), K(enable_all_pdml_feature));
  // check whether all pdml feature is opened:
  // 1. if open, is_open = true
  // 2. if not all open, check whether any unstable feature is forbidden. If exists, is_open = false
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the session info is null", K(ret));
  } else if (enable_all_pdml_feature) {
    is_open = true;
  } else if (OB_ISNULL(dml_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the dml stmt is null", K(ret));
  } else if (OB_ISNULL(table_item = dml_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the table item is null", K(ret));
  } else if (OB_ISNULL(schema_guard = schema_checker_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the schema guart is null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_item->get_base_table_item().ref_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(*table_item));
  } else if (table_schema->get_foreign_key_infos().count() > 0) {
    // forbid PDML if foreign key exists.
    LOG_TRACE("dml has foreign key, disable pdml", K(ret));
    is_open = false;
  } else if (!session_info_->use_static_typing_engine() && dml_stmt->get_check_constraint_exprs_size() > 0) {
    // forbid PDML if check constrain exists.
    LOG_TRACE("dml has constraint, old engine, disable pdml", K(ret));
    is_open = false;
  } else {
    // check global unique index, udpate(row movement)
    int global_index_cnt = dml_stmt->get_all_table_columns().at(0).index_dml_infos_.count();
    // traverse all index dml info in the dml operation: check whether contains unique index, row movement(update)
    for (int idx = 0; idx < global_index_cnt && OB_SUCC(ret) && is_open; idx++) {
      // check whether current global index contains unique constraint. Forbid PDML if contains.
      const ObIArray<ObColumnRefRawExpr*>& column_exprs =
          dml_stmt->get_all_table_columns().at(0).index_dml_infos_.at(idx).column_exprs_;
      bool has_unique_index = false;
      LOG_TRACE("check pdml unique index", K(column_exprs));
      if (OB_FAIL(check_unique_index(column_exprs, has_unique_index))) {
        LOG_WARN("failed to check has unique index", K(ret));
      } else if (has_unique_index) {
        LOG_TRACE("dml has unique index, disable pdml", K(ret));
        is_open = false;
        break;
      }
    }
  }
  LOG_TRACE("check use all pdml feature", K(ret), K(is_open));
  return ret;
}

int ObDMLResolver::resolve_table_relation_factor_normal(const ParseNode* node, uint64_t tenant_id,
    uint64_t& database_id, ObString& table_name, ObString& synonym_name, ObString& synonym_db_name, ObString& db_name,
    ObSynonymChecker& synonym_checker)
{
  bool is_db_explicit = false;
  UNUSED(is_db_explicit);
  return resolve_table_relation_factor_normal(node,
      tenant_id,
      database_id,
      table_name,
      synonym_name,
      synonym_db_name,
      db_name,
      is_db_explicit,
      synonym_checker);
}

int ObDMLResolver::resolve_table_relation_factor_normal(const ParseNode* node, uint64_t tenant_id,
    uint64_t& database_id, ObString& table_name, ObString& synonym_name, ObString& synonym_db_name, ObString& db_name,
    bool& is_db_explicit, ObSynonymChecker& synonym_checker)
{
  int ret = OB_SUCCESS;
  database_id = OB_INVALID_ID;
  is_db_explicit = false;
  ObString orig_name;
  ObString out_db_name;
  ObString out_table_name;
  synonym_db_name.reset();

  if (OB_FAIL(resolve_table_relation_node_v2(node, table_name, db_name, is_db_explicit))) {
    LOG_WARN("failed to resolve table relation node!", K(ret));
  } else if (FALSE_IT(orig_name.assign_ptr(table_name.ptr(), table_name.length()))) {
  } else if (FALSE_IT(synonym_db_name.assign_ptr(db_name.ptr(), db_name.length()))) {
  } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, db_name, database_id))) {
    if (OB_SCHEMA_EAGAIN != ret) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("Invalid database name, database not exist", K(db_name), K(tenant_id), K(ret));
    }
  } else if (share::is_oracle_mode() && 0 == db_name.case_compare(OB_SYS_DATABASE_NAME)) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("Invalid database name, cannot access oceanbase db on Oracle tenant", K(db_name), K(tenant_id), K(ret));
  } else if (OB_FAIL(
                 resolve_table_relation_recursively(tenant_id, database_id, table_name, db_name, synonym_checker))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      if (synonym_checker.has_synonym()) {
        ret = OB_ERR_SYNONYM_TRANSLATION_INVALID;
        LOG_WARN("Synonym translation is no longer valid");
        LOG_USER_ERROR(OB_ERR_SYNONYM_TRANSLATION_INVALID, to_cstring(orig_name));
      }
    }
    synonym_db_name.reset();
    synonym_name.reset();
    LOG_WARN("fail to resolve table relation recursively", K(tenant_id), K(ret));
  } else if (false == synonym_checker.has_synonym()) {
    synonym_name.reset();
    synonym_db_name.reset();
  } else {
    synonym_name = orig_name;
    ObStmt* stmt = get_basic_stmt();
    if (OB_NOT_NULL(stmt)) {
      if (OB_FAIL(add_synonym_obj_id(synonym_checker, false /* error_with_exist */))) {
        LOG_WARN("add_synonym_obj_id failed", K(ret));
      }
    }
  }

  // table_name and db_name memory may from schema, so deep copy the content to SQL memory
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is NULL", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, table_name, out_table_name))) {
    LOG_WARN("fail to deep copy string", K(table_name), K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator_, db_name, out_db_name))) {
    LOG_WARN("fail to deep copy string", K(db_name), K(ret));
  } else {
    table_name = out_table_name;
    db_name = out_db_name;
  }
  return ret;
}

int ObDMLResolver::resolve_table_relation_factor_dblink(const ParseNode* table_node, const uint64_t tenant_id,
    const ObString& dblink_name, uint64_t& dblink_id, ObString& table_name, ObString& database_name)
{
  int ret = OB_SUCCESS;
  // db name node may null
  ParseNode* dbname_node = table_node->children_[0];
  if (OB_ISNULL(table_node) || OB_ISNULL(table_node->children_) || OB_ISNULL(table_node->children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table node or children is NULL", K(ret));
  } else if (OB_FAIL(schema_checker_->get_dblink_id(tenant_id, dblink_name, dblink_id))) {
    LOG_WARN("failed to get dblink info", K(ret), K(dblink_name));
  } else if (OB_INVALID_ID == dblink_id) {
    ret = OB_DBLINK_NOT_EXIST_TO_ACCESS;
    LOG_WARN("dblink not exist", K(ret), K(tenant_id), K(dblink_name));
  } else {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("allocator is null", K(ret));
    } else if (OB_FAIL(schema_checker_->get_dblink_user(tenant_id, dblink_name, database_name, *allocator_))) {
      LOG_WARN("failed to get dblink user name", K(tenant_id), K(database_name));
    } else if (database_name.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dblink user name is empty", K(ret));
    } else {
      ObString tmp_dbname;
      if (OB_NOT_NULL(dbname_node)) {
        int32_t database_name_len = static_cast<int32_t>(dbname_node->str_len_);
        tmp_dbname.assign_ptr(dbname_node->str_value_, database_name_len);
        // the one saved in the schema may different from the one in the parse node, check it.
        if (0 != database_name.case_compare(tmp_dbname)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user name is not same", K(database_name), K(tmp_dbname));
        } else {
          // do nothing
        }
      } else {
        // do nothing
      }
      // database name may lower char, translate to upper, for all_object's user name is upper
      // why we use database_name, but not dbname_node, because dbname_node is not always exist
      char letter;
      char* src_ptr = database_name.ptr();
      for (ObString::obstr_size_t i = 0; i < database_name.length(); ++i) {
        letter = src_ptr[i];
        if (letter >= 'a' && letter <= 'z') {
          src_ptr[i] = static_cast<char>(letter - 32);
        }
      }
    }
    if (OB_SUCC(ret)) {
      int32_t table_name_len = static_cast<int32_t>(table_node->children_[1]->str_len_);
      table_name.assign_ptr(table_node->children_[1]->str_value_, table_name_len);
    }
  }
  return ret;
}

int ObDMLResolver::resolve_table_relation_recursively(uint64_t tenant_id, uint64_t& database_id, ObString& table_name,
    ObString& db_name, ObSynonymChecker& synonym_checker)
{
  int ret = OB_SUCCESS;
  bool exist_with_synonym = false;
  ObString object_table_name;
  uint64_t object_db_id;
  uint64_t synonym_id;
  ObString object_db_name;
  const ObDatabaseSchema* database_schema = NULL;
  if (OB_FAIL(check_table_exist_or_not(tenant_id, database_id, table_name, db_name))) {
    if (OB_TABLE_NOT_EXIST == ret) {  // try again, with synonym
      ret = OB_SUCCESS;
      if (OB_FAIL(schema_checker_->get_synonym_schema(
              tenant_id, database_id, table_name, object_db_id, synonym_id, object_table_name, exist_with_synonym))) {
        LOG_WARN("get synonym schema failed", K(tenant_id), K(database_id), K(table_name), K(ret));
      } else if (exist_with_synonym) {
        synonym_checker.set_synonym(true);
        if (OB_FAIL(synonym_checker.add_synonym_id(synonym_id))) {
          LOG_WARN(
              "fail to add synonym id", K(synonym_id), K(database_id), K(table_name), K(object_table_name), K(ret));
        } else if (OB_FAIL(schema_checker_->get_database_schema(tenant_id, object_db_id, database_schema))) {
          LOG_WARN("get db schema failed", K(tenant_id), K(object_db_id), K(ret));
        } else if (OB_ISNULL(database_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get db schema succeed, but schema pointer is null", K(tenant_id), K(object_db_id), K(ret));
        } else {
          db_name = database_schema->get_database_name_str();
          table_name = object_table_name;
          database_id = object_db_id;
          if (OB_FAIL(SMART_CALL(
                  resolve_table_relation_recursively(tenant_id, database_id, table_name, db_name, synonym_checker)))) {
            LOG_WARN("fail to resolve table relation", K(tenant_id), K(database_id), K(table_name), K(ret));
          }
        }
      } else {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("synonym not exist", K(tenant_id), K(database_id), K(table_name), K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::check_table_exist_or_not(
    uint64_t tenant_id, uint64_t& database_id, ObString& table_name, ObString& db_name)
{
  int ret = OB_SUCCESS;
  database_id = OB_INVALID_ID;
  if (OB_FAIL(schema_checker_->get_database_id(tenant_id, db_name, database_id))) {
    if (OB_SCHEMA_EAGAIN != ret) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_WARN("Invalid database name, database not exist", K(db_name), K(tenant_id));
    }
  } else {
    bool is_exist = false;
    bool select_index_enabled = false;
    if (OB_FAIL(session_info_->is_select_index_enabled(select_index_enabled))) {
      LOG_WARN("fail to get select_index_enabled", K(ret));
    } else if (select_index_enabled && is_select_resolver()) {
      if (OB_FAIL(schema_checker_->check_table_or_index_exists(tenant_id, database_id, table_name, is_exist))) {
        LOG_WARN("fail to check table or index exist", K(tenant_id), K(database_id), K(table_name), K(ret));
      }
    } else {
      const bool is_index = false;
      if (OB_FAIL(schema_checker_->check_table_exists(tenant_id, database_id, table_name, is_index, is_exist))) {
        LOG_WARN("fail to check table or index exist", K(tenant_id), K(database_id), K(table_name), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (!is_exist) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_INFO("table not exist", K(tenant_id), K(database_id), K(table_name), K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_function_table_column_item(const TableItem& table_item, const ObDataType& data_type,
    const ObString& column_name, uint64_t column_id, ColumnItem*& col_item)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr* col_expr = NULL;
  ColumnItem column_item;
  sql::ObExprResType result_type;
  ObDMLStmt* stmt = get_stmt();
  CK(OB_NOT_NULL(stmt));
  CK(OB_NOT_NULL(params_.expr_factory_));
  CK(OB_LIKELY(table_item.is_function_table()));
  OZ(params_.expr_factory_->create_raw_expr(T_REF_COLUMN, col_expr));
  CK(OB_NOT_NULL(col_expr));
  OX(col_expr->set_ref_id(table_item.table_id_, column_id));
  OX(result_type.set_meta(data_type.get_meta_type()));
  OX(result_type.set_accuracy(data_type.get_accuracy()));
  OX(col_expr->set_result_type(result_type));
  if (table_item.get_object_name().empty()) {
    OX(col_expr->set_column_name(column_name));
  } else {
    OX(col_expr->set_column_attr(table_item.get_object_name(), column_name));
  }
  OX(col_expr->set_database_name(table_item.database_name_));
  if (OB_SUCC(ret) && ob_is_enumset_tc(col_expr->get_data_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support enum set in table function", K(ret));
  }
  OX(column_item.expr_ = col_expr);
  OX(column_item.table_id_ = col_expr->get_table_id());
  OX(column_item.column_id_ = col_expr->get_column_id());
  OX(column_item.column_name_ = col_expr->get_column_name());
  OZ(col_expr->extract_info());
  OZ(stmt->add_column_item(column_item));
  OX(col_item = stmt->get_column_item(stmt->get_column_size() - 1));
  return ret;
}

int ObDMLResolver::resolve_function_table_column_item(
    const TableItem& table_item, const ObString& column_name, ColumnItem*& col_item)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = get_stmt();
  CK(OB_NOT_NULL(stmt));
  CK(OB_LIKELY(table_item.is_function_table()));
  if (OB_FAIL(ret)) {
    // do nothing ...
  } else if (NULL != (col_item = stmt->get_column_item(table_item.table_id_, column_name))) {
    // exist, ignore resolve...
  } else {
    ObSEArray<ColumnItem, 16> col_items;
    OZ(ObResolverUtils::check_function_table_column_exist(table_item, params_, column_name));
    OZ(resolve_function_table_column_item(table_item, col_items));
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      ColumnItem* column_item = stmt->get_column_item(i);
      CK(OB_NOT_NULL(column_item));
      if (OB_SUCC(ret) && column_item->table_id_ == table_item.table_id_ &&
          ObCharset::case_compat_mode_equal(column_item->column_name_, column_name)) {
        col_item = column_item;
        break;
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(col_item)) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("not found column in table function", K(ret), K(column_name));
    }
  }
  return ret;
}

int ObDMLResolver::resolve_function_table_column_item(const TableItem& table_item, ObIArray<ColumnItem>& col_items)
{
  UNUSED(table_item);
  UNUSED(col_items);
  return OB_NOT_SUPPORTED;
}

int ObDMLResolver::resolve_generated_table_column_item(const TableItem& table_item, const common::ObString& column_name,
    ColumnItem*& col_item, ObDMLStmt* stmt /* = NULL */, const uint64_t column_id /* = OB_INVALID_ID */)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr* col_expr = NULL;
  ColumnItem column_item;
  if (NULL == stmt) {
    stmt = get_stmt();
  }
  if (OB_ISNULL(stmt) || OB_ISNULL(schema_checker_) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema checker is null", K(stmt), K_(schema_checker), K_(params_.expr_factory));
  } else if (OB_UNLIKELY(!table_item.is_generated_table() && !table_item.is_fake_cte_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not generated table", K_(table_item.type));
  }
  bool found = false;
  if (OB_SUCC(ret)) {
    if (OB_INVALID_ID != column_id) {
      col_item = stmt->get_column_item_by_id(table_item.table_id_, column_id);
    } else {
      col_item = stmt->get_column_item(table_item.table_id_, column_name);
    }
    found = NULL != col_item;
  }
  if (OB_SUCC(ret) && !found) {
    ObSelectStmt* ref_stmt = table_item.ref_query_;
    bool is_break = false;
    if (OB_ISNULL(ref_stmt)) {
      ret = OB_NOT_INIT;
      LOG_WARN("generate table ref stmt is null");
    }

    for (int64_t i = 0; OB_SUCC(ret) && !is_break && i < ref_stmt->get_select_item_size(); ++i) {
      SelectItem& ref_select_item = ref_stmt->get_select_item(i);
      if (column_id != OB_INVALID_ID ? i + OB_APP_MIN_COLUMN_ID == column_id
                                     : ObCharset::case_compat_mode_equal(column_name, ref_select_item.alias_name_)) {
        ObRawExpr* select_expr = ref_select_item.expr_;
        if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_REF_COLUMN, col_expr))) {
          LOG_WARN("create column expr failed", K(ret));
        } else if (OB_ISNULL(select_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("select expr is null");
        } else if (OB_FAIL(select_expr->deduce_type(session_info_))) {
          LOG_WARN("deduce select expr type failed", K(ret));
        } else {
          // because of view table, generated table item may be has database_name and table name,
          // also alias name maybe be empty
          col_expr->set_ref_id(table_item.table_id_, i + OB_APP_MIN_COLUMN_ID);
          col_expr->set_result_type(select_expr->get_result_type());
          col_expr->set_column_attr(table_item.get_table_name(), ref_select_item.alias_name_);
          col_expr->set_database_name(table_item.database_name_);
          col_expr->set_unpivot_mocked_column(ref_select_item.is_unpivot_mocked_column_);
          // set enum_set_values
          if (ob_is_enumset_tc(select_expr->get_data_type())) {
            if (OB_FAIL(col_expr->set_enum_set_values(select_expr->get_enum_set_values()))) {
              LOG_WARN("failed to set_enum_set_values", K(ret));
            }
          }
          column_item.set_default_value(ref_select_item.default_value_);
          column_item.set_default_value_expr(ref_select_item.default_value_expr_);
          is_break = true;

          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(erase_redundant_generated_table_column_flag(*ref_stmt, select_expr, *col_expr))) {
            LOG_WARN("erase redundant generated table column flag failed", K(ret));
          } else {
            if (select_expr->is_column_ref_expr()) {
              ObColumnRefRawExpr* col_ref = static_cast<ObColumnRefRawExpr*>(select_expr);
              ColumnItem* item = ref_stmt->get_column_item_by_id(col_ref->get_table_id(), col_ref->get_column_id());
              if (OB_ISNULL(item)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("column item should not be null", K(ret));
              } else if ((stmt->is_update_stmt() || stmt->is_insert_stmt()) && table_item.is_view_table_ &&
                         OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == col_ref->get_column_id()) {
                // no need to check merge stmt, because merge into view is not supported.
                ret = OB_ERR_VIRTUAL_COL_NOT_ALLOWED;
                LOG_WARN("cannot update rowid pseudo column", K(ret), K(*item), K(*col_ref));
              } else {
                column_item.base_tid_ = item->base_tid_;
                column_item.base_cid_ = item->base_cid_;
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col_expr is null");
    }
    // init column item
    if (OB_SUCC(ret)) {
      column_item.expr_ = col_expr;
      column_item.table_id_ = col_expr->get_table_id();
      column_item.column_id_ = col_expr->get_column_id();
      column_item.column_name_ = col_expr->get_column_name();
      if (OB_FAIL(col_expr->extract_info())) {
        LOG_WARN("extract column expr info failed", K(ret));
      } else if (OB_FAIL(stmt->add_column_item(column_item))) {
        LOG_WARN("add column item to stmt failed", K(ret));
      } else {
        col_item = stmt->get_column_item(stmt->get_column_size() - 1);
      }
    }
  }
  return ret;
}

int ObDMLResolver::set_base_table_for_updatable_view(
    TableItem& table_item, const ObColumnRefRawExpr& col_ref, const bool log_error /* = true */)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* stmt = table_item.ref_query_;
  ObDMLStmt* dml = get_stmt();
  const int64_t idx = col_ref.get_column_id() - OB_APP_MIN_COLUMN_ID;
  OZ(check_stack_overflow());
  if (OB_FAIL(ret)) {
  } else if (!table_item.is_generated_table() || OB_ISNULL(dml) || OB_ISNULL(stmt) || idx < 0 ||
             idx >= stmt->get_select_item_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "table is not view or stmt is NULL or invalid column id", K(ret), K(table_item), K(idx), KP(stmt), K(col_ref));
  } else {
    if (table_item.is_view_table_) {
      const ObTableSchema* table_schema = NULL;
      if (OB_FAIL(schema_checker_->get_table_schema(stmt->get_view_ref_id(), table_schema))) {
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
    if (OB_FAIL(ObResolverUtils::uv_check_basic(*stmt, dml->is_insert_stmt()))) {
      LOG_WARN("not updatable", K(ret));
    } else {
      ObRawExpr* expr = stmt->get_select_item(idx).expr_;
      if (!expr->is_column_ref_expr()) {
        ret = is_mysql_mode() ? OB_ERR_NONUPDATEABLE_COLUMN : OB_ERR_VIRTUAL_COL_NOT_ALLOWED;
        LOG_WARN("column is not updatable", K(ret), K(col_ref));
      } else {
        ObColumnRefRawExpr* new_col_ref = static_cast<ObColumnRefRawExpr*>(expr);
        TableItem* new_table_item = stmt->get_table_item_by_id(new_col_ref->get_table_id());
        if (NULL == new_table_item) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get NULL table item", K(ret));
        } else {
          table_item.view_base_item_ = new_table_item;
          if (new_table_item->is_basic_table()) {
            // find base table, do nothing
          } else if (new_table_item->is_generated_table()) {
            const bool inner_log_error = false;
            if (OB_FAIL(
                    SMART_CALL(set_base_table_for_updatable_view(*new_table_item, *new_col_ref, inner_log_error)))) {
              LOG_WARN("find base table for updatable view failed", K(ret));
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
    }
    if (log_error && OB_SUCCESS != ret) {
      ObString update_str = "UPDATE";
      if (OB_ERR_NONUPDATEABLE_COLUMN == ret) {
        LOG_USER_ERROR(
            OB_ERR_NONUPDATEABLE_COLUMN, col_ref.get_column_name().length(), col_ref.get_column_name().ptr());
      } else if (OB_ERR_NON_INSERTABLE_TABLE == ret) {
        LOG_USER_ERROR(OB_ERR_NON_INSERTABLE_TABLE, table_item.table_name_.length(), table_item.table_name_.ptr());
      } else if (OB_ERR_NON_UPDATABLE_TABLE == ret) {
        LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
            table_item.table_name_.length(),
            table_item.table_name_.ptr(),
            update_str.length(),
            update_str.ptr());
      }
    }
  }
  return ret;
}

int ObDMLResolver::set_base_table_for_view(TableItem& table_item, const bool log_error /* = true */)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* stmt = table_item.ref_query_;
  OZ(check_stack_overflow());
  if (OB_FAIL(ret)) {
  } else if (!table_item.is_generated_table() || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not generated table or referred query is NULL", K(ret));
  } else {
    if (table_item.is_view_table_) {
      const ObTableSchema* table_schema = NULL;
      if (OB_FAIL(schema_checker_->get_table_schema(stmt->get_view_ref_id(), table_schema))) {
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
      TableItem* base = stmt->get_table_items().at(0);
      if (stmt->get_table_items().count() > 1) {
        // mysql delete join view not supported.
        if (is_mysql_mode()) {
          ret = OB_ERR_VIEW_DELETE_MERGE_VIEW;
          LOG_WARN("delete join view", K(ret));
        }
      }
      if (NULL == base) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (base->is_basic_table()) {
        table_item.view_base_item_ = base;
      } else if (base->is_generated_table()) {
        table_item.view_base_item_ = base;
        const bool inner_log_error = false;
        if (OB_FAIL(SMART_CALL(set_base_table_for_view(*base, inner_log_error)))) {
          LOG_WARN("set base table for view failed", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table type in view", K(ret), K(*base));
      }
    }
    if (log_error && OB_SUCCESS != ret) {
      ObString del_str = "DELETE";
      if (OB_ERR_NON_UPDATABLE_TABLE == ret) {
        LOG_USER_ERROR(OB_ERR_NON_UPDATABLE_TABLE,
            table_item.table_name_.length(),
            table_item.table_name_.ptr(),
            del_str.length(),
            del_str.ptr());
      } else if (OB_ERR_VIEW_DELETE_MERGE_VIEW == ret) {
        LOG_USER_ERROR(OB_ERR_VIEW_DELETE_MERGE_VIEW,
            table_item.database_name_.length(),
            table_item.database_name_.ptr(),
            table_item.table_name_.length(),
            table_item.table_name_.ptr());
      }
    }
  }
  return ret;
}

int ObDMLResolver::check_same_base_table(
    const TableItem& table_item, const ObColumnRefRawExpr& col_ref, const bool log_error /* = true */)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* stmt = table_item.ref_query_;
  const int64_t idx = col_ref.get_column_id() - OB_APP_MIN_COLUMN_ID;
  if (!table_item.is_generated_table() || OB_ISNULL(table_item.view_base_item_) || OB_ISNULL(stmt) || idx < 0 ||
      idx >= stmt->get_select_item_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "table is not view or stmt is NULL or invalid column id", K(ret), K(table_item), K(idx), KP(stmt), K(col_ref));
  } else {
    ObRawExpr* expr = stmt->get_select_item(idx).expr_;
    if (!expr->is_column_ref_expr()) {
      ret = is_mysql_mode() ? OB_ERR_NONUPDATEABLE_COLUMN : OB_ERR_VIRTUAL_COL_NOT_ALLOWED;
      LOG_WARN("column is not updatable", K(ret), K(col_ref));
    } else {
      ObColumnRefRawExpr* new_col_ref = static_cast<ObColumnRefRawExpr*>(expr);
      const TableItem* new_table_item = table_item.view_base_item_;
      if (new_col_ref->get_table_id() != new_table_item->table_id_) {
        ret = is_mysql_mode() ? OB_ERR_VIEW_MULTIUPDATE : OB_ERR_O_VIEW_MULTIUPDATE;
        LOG_WARN("Can not modify more than one base table through a join view", K(ret), K(col_ref));
      } else {
        if (new_table_item->is_basic_table()) {
          // is base table, do nothing
        } else if (new_table_item->is_generated_table()) {
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
        LOG_USER_ERROR(
            OB_ERR_NONUPDATEABLE_COLUMN, col_ref.get_column_name().length(), col_ref.get_column_name().ptr());
      } else if (OB_ERR_VIEW_MULTIUPDATE == ret) {
        LOG_USER_ERROR(OB_ERR_VIEW_MULTIUPDATE,
            table_item.database_name_.length(),
            table_item.database_name_.ptr(),
            table_item.table_name_.length(),
            table_item.table_name_.ptr());
      }
    }
  }
  return ret;
}

int ObDMLResolver::add_all_column_to_updatable_view(ObDMLStmt& stmt, const TableItem& table_item)
{
  int ret = OB_SUCCESS;
  OZ(check_stack_overflow());
  if (OB_FAIL(ret)) {
  } else if (!table_item.is_basic_table() && !table_item.is_generated_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected table item", K(ret), K(table_item));
  } else {
    if (table_item.is_generated_table()) {
      if (OB_ISNULL(table_item.ref_query_) || OB_ISNULL(table_item.view_base_item_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("generate table bug reference query is NULL or base table item is NULL", K(ret), K(table_item));
      } else if (OB_FAIL(SMART_CALL(
                     add_all_column_to_updatable_view(*table_item.ref_query_, *table_item.view_base_item_)))) {
        LOG_WARN("add all column for updatable view failed", K(ret), K(table_item));
      }
    }
  }
  if (OB_SUCC(ret)) {
    auto add_select_item_func = [&](ObSelectStmt& select_stmt, ColumnItem& col) {
      int ret = OB_SUCCESS;
      bool found_in_select = false;
      FOREACH_CNT_X(si, select_stmt.get_select_items(), !found_in_select)
      {
        if (si->expr_ == col.expr_) {
          found_in_select = true;
        }
      }
      if (!found_in_select) {
        SelectItem select_item;
        select_item.implicit_filled_ = true;
        select_item.expr_ = col.expr_;
        select_item.default_value_ = col.default_value_;
        // concat column's table name and column name as select item's alias name
        const int32_t size = col.expr_->get_table_name().length() + 1  // "."
                             + col.expr_->get_column_name().length();
        char* buf = static_cast<char*>(allocator_->alloc(size));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(size));
        } else {
          char* p = buf;
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
    ColumnItem* col_item = NULL;
    if (table_item.is_basic_table()) {
      const ObTableSchema* table_schema = NULL;
      if (OB_FAIL(schema_checker_->get_table_schema(table_item.ref_id_, table_schema))) {
        LOG_WARN("get table schema failed", K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL table schema", K(ret));
      } else {
        for (auto iter = table_schema->column_begin(); OB_SUCC(ret) && iter != table_schema->column_end(); iter++) {
          col_item = stmt.get_column_item_by_id(table_item.table_id_, (*iter)->get_column_id());
          if (NULL == col_item) {  // column not found
            if (OB_FAIL(resolve_basic_column_item(table_item, (*iter)->get_column_name(), true, col_item, &stmt))) {
              LOG_WARN("resolve basic column item failed", K(ret), K(table_item), K(*iter));
            }
          }
          if (OB_SUCC(ret)) {
            if (stmt::T_SELECT == stmt.get_stmt_type()) {
              if (OB_FAIL(add_select_item_func(static_cast<ObSelectStmt&>(stmt), *col_item))) {
                LOG_WARN("add column item to select item failed", K(ret));
              }
            }
          }
        }
      }
    } else if (table_item.is_generated_table()) {
      ObSelectStmt* ref_stmt = table_item.ref_query_;
      for (int64_t i = 0; OB_SUCC(ret) && i < ref_stmt->get_select_item_size(); i++) {
        const uint64_t column_id = OB_APP_MIN_COLUMN_ID + i;
        ObString col_name;  // not used
        col_item = NULL;
        if (OB_FAIL(resolve_generated_table_column_item(table_item, col_name, col_item, &stmt, column_id))) {
          LOG_WARN("resolve generate table item failed", K(ret));
        } else {
          if (stmt::T_SELECT == stmt.get_stmt_type()) {
            if (OB_FAIL(add_select_item_func(static_cast<ObSelectStmt&>(stmt), *col_item))) {
              LOG_WARN("add column item to select item failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

bool ObDMLResolver::in_updatable_view_path(const TableItem& table_item, const ObColumnRefRawExpr& col) const
{
  bool in_path = false;
  if (table_item.table_id_ == col.get_table_id()) {
    if (table_item.is_generated_table()) {
      const int64_t cid = col.get_column_id() - OB_APP_MIN_COLUMN_ID;
      if (NULL != table_item.ref_query_ && NULL != table_item.view_base_item_ && cid >= 0 &&
          cid < table_item.ref_query_->get_select_item_size()) {
        auto expr = table_item.ref_query_->get_select_item(cid).expr_;
        if (expr->is_column_ref_expr()) {
          in_path = in_updatable_view_path(*table_item.view_base_item_, *static_cast<const ObColumnRefRawExpr*>(expr));
        }
      }
    } else {
      in_path = true;
    }
  }
  return in_path;
}

ColumnItem* ObDMLResolver::find_col_by_base_col_id(
    ObDMLStmt& stmt, const uint64_t table_id, const uint64_t base_column_id)
{
  ColumnItem* c = NULL;
  const TableItem* t = stmt.get_table_item_by_id(table_id);
  if (OB_ISNULL(t)) {
    LOG_WARN("get table item failed", K(table_id));
  } else {
    c = find_col_by_base_col_id(stmt, *t, base_column_id);
  }
  return c;
}

ColumnItem* ObDMLResolver::find_col_by_base_col_id(
    ObDMLStmt& stmt, const TableItem& table_item, const uint64_t base_column_id)
{
  ColumnItem* item = NULL;
  FOREACH_CNT_X(col, stmt.get_column_items(), NULL == item)
  {
    if (col->table_id_ == table_item.table_id_ && col->base_cid_ == base_column_id &&
        in_updatable_view_path(table_item, *col->expr_)) {
      item = &(*col);
    }
  }
  return item;
}

int ObDMLResolver::erase_redundant_generated_table_column_flag(
    const ObSelectStmt& ref_stmt, const ObRawExpr* ref_expr, ObColumnRefRawExpr& col_expr) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ref_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref_expr is null");
  } else if (ref_expr->is_column_ref_expr()) {
    bool is_null = false;
    const ObColumnRefRawExpr& ref_col_expr = static_cast<const ObColumnRefRawExpr&>(*ref_expr);
    if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(&ref_stmt, ref_col_expr.get_table_id(), is_null))) {
      LOG_WARN("is table on null side failed", K(ret));
    } else if (is_null) {
      col_expr.unset_result_flag(OB_MYSQL_NOT_NULL_FLAG);
      col_expr.unset_result_flag(OB_MYSQL_AUTO_INCREMENT_FLAG);
      col_expr.unset_result_flag(OB_MYSQL_PRI_KEY_FLAG);
      col_expr.unset_result_flag(OB_MYSQL_PART_KEY_FLAG);
      col_expr.unset_result_flag(OB_MYSQL_MULTIPLE_KEY_FLAG);
    }
  }
  return ret;
}

int ObDMLResolver::build_prefix_index_compare_expr(ObRawExpr& column_expr, ObRawExpr* prefix_expr, ObItemType type,
    ObRawExpr& value_expr, ObRawExpr* escape_expr, ObRawExpr*& new_op_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* substr_expr = NULL;
  if (T_OP_LIKE == type) {
    // build value substr expr
    ObOpRawExpr* like_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::create_substr_expr(*params_.expr_factory_,
            params_.session_info_,
            &value_expr,
            prefix_expr->get_param_expr(1),
            prefix_expr->get_param_expr(2),
            substr_expr))) {
      LOG_WARN("create substr expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_like_expr(
                   *params_.expr_factory_, params_.session_info_, &column_expr, substr_expr, escape_expr, like_expr))) {
      LOG_WARN("build like expr failed", K(ret));
    } else {
      new_op_expr = like_expr;
    }
  } else {
    ObRawExpr* right_expr = NULL;
    if (T_OP_IN == type) {
      ObOpRawExpr* row_expr = NULL;
      if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_OP_ROW, row_expr))) {
        LOG_WARN("create to_type expr failed", K(ret));
      } else if (OB_ISNULL(row_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("to_type is null");
      } else {
        right_expr = row_expr;
        for (int64_t k = 0; OB_SUCC(ret) && k < value_expr.get_param_count(); ++k) {
          if (OB_FAIL(ObRawExprUtils::create_substr_expr(*params_.expr_factory_,
                  params_.session_info_,
                  value_expr.get_param_expr(k),
                  prefix_expr->get_param_expr(1),
                  prefix_expr->get_param_expr(2),
                  substr_expr))) {
            LOG_WARN("create substr expr failed", K(ret));
          } else if (OB_FAIL(row_expr->add_param_expr(substr_expr))) {
            LOG_WARN("set param expr failed", K(ret));
          }
        }
      }
    } else {
      if (OB_FAIL(ObRawExprUtils::create_substr_expr(*params_.expr_factory_,
              params_.session_info_,
              &value_expr,
              prefix_expr->get_param_expr(1),
              prefix_expr->get_param_expr(2),
              substr_expr))) {
        LOG_WARN("create substr expr failed", K(ret));
      } else {
        right_expr = substr_expr;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
              *params_.expr_factory_, params_.session_info_, type, new_op_expr, &column_expr, right_expr))) {
        LOG_WARN("failed to create double op expr", K(ret), K(type), K(column_expr), KPC(right_expr));
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_sample_clause(const ParseNode* sample_node, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* stmt;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt should not be NULL", K(ret));
  } else if (OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sampling in dml statement");
  } else {
    stmt = static_cast<ObSelectStmt*>(get_stmt());
    enum SampleNode { METHOD = 0, PERCENT = 1, SEED = 2, SCOPE = 3 };
    if (OB_ISNULL(sample_node) || OB_ISNULL(sample_node->children_[METHOD]) ||
        OB_ISNULL(sample_node->children_[PERCENT])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sample node should not be NULL", K(ret));
    } else {
      SampleInfo sample_info;
      sample_info.table_id_ = table_id;
      if (sample_node->children_[METHOD]->value_ == 2) {
        sample_info.method_ = SampleInfo::BLOCK_SAMPLE;
      } else {
        sample_info.method_ = SampleInfo::ROW_SAMPLE;
      }

      sample_info.percent_ = 0;
      if (sample_node->children_[PERCENT]->type_ == T_SFU_INT) {
        sample_info.percent_ = static_cast<double>(sample_node->children_[PERCENT]->value_);
      } else if (sample_node->children_[PERCENT]->type_ == T_SFU_DECIMAL) {
        ObString str_percent(sample_node->children_[PERCENT]->str_len_, sample_node->children_[PERCENT]->str_value_);
        if (OB_FAIL(ObOptEstObjToScalar::convert_string_to_scalar_for_number(str_percent, sample_info.percent_))) {
          LOG_WARN("failed to convert string to number", K(ret));
        } else { /*do nothing*/
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected node type for sample percent", K(ret));
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        if (sample_node->children_[SEED] != NULL) {
          sample_info.seed_ = sample_node->children_[SEED]->value_;
        } else {
          // seed is set to -1 when not provided and we will pick a random seed in this case.
          sample_info.seed_ = -1;
        }
        // resolve sample scope
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_ISNULL(sample_node->children_[SCOPE])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sample scope should not be null", K(ret));
        } else if (sample_node->children_[SCOPE]->type_ == T_ALL) {
          sample_info.scope_ = SampleInfo::SampleScope::SAMPLE_ALL_DATA;
        } else if (sample_node->children_[SCOPE]->type_ == T_BASE) {
          sample_info.scope_ = SampleInfo::SampleScope::SAMPLE_BASE_DATA;
        } else if (sample_node->children_[SCOPE]->type_ == T_INCR) {
          sample_info.scope_ = SampleInfo::SampleScope::SAMPLE_INCR_DATA;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unrecognized sample scope", K(ret), K(sample_node->children_[SCOPE]->type_));
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(stmt->add_sample_info(sample_info))) {
          LOG_WARN("add sample info failed", K(ret), K(sample_info));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (sample_info.percent_ < 0.000001 || sample_info.percent_ >= 100.0) {
        ret = OB_ERR_INVALID_SAMPLING_RANGE;
      } else if (sample_info.seed_ != -1 && (sample_info.seed_ > (4294967295))) {
        LOG_WARN("seed value out of range");
      }
    }
  }

  return ret;
}

int ObDMLResolver::resolve_transpose_columns(const ParseNode& column_node, ObIArray<ObString>& columns)
{
  int ret = OB_SUCCESS;
  if (column_node.type_ == T_COLUMN_LIST) {
    if (OB_UNLIKELY(column_node.num_child_ <= 0) || OB_ISNULL(column_node.children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_node is unexpected", K(column_node.num_child_), KP(column_node.children_), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_node.num_child_; ++i) {
        const ParseNode* tmp_node = column_node.children_[i];
        if (OB_ISNULL(tmp_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_node is unexpected", KP(tmp_node), K(ret));
        } else {
          ObString column_name_(tmp_node->str_len_, tmp_node->str_value_);
          if (OB_FAIL(columns.push_back(column_name_))) {
            LOG_WARN("fail to push_back column_name_", K(column_name_), K(ret));
          }
        }
      }
    }
  } else {
    ObString column_name(column_node.str_len_, column_node.str_value_);
    if (OB_FAIL(columns.push_back(column_name))) {
      LOG_WARN("fail to push_back column_name", K(column_name), K(ret));
    }
  }
  return ret;
}

int ObDMLResolver::resolve_const_exprs(const ParseNode& expr_node, ObIArray<ObRawExpr*>& const_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr* const_expr = NULL;
  if (expr_node.type_ == T_EXPR_LIST) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_node.num_child_; ++i) {
      const ParseNode* tmp_node = expr_node.children_[i];
      const_expr = NULL;
      if (OB_ISNULL(tmp_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tmp_node is unexpected", KP(tmp_node), K(ret));
      } else if (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, *tmp_node, const_expr, NULL))) {
        LOG_WARN("fail to resolve_const_expr", K(ret));
      } else if (OB_UNLIKELY(!const_expr->has_const_or_const_expr_flag()) ||
                 OB_UNLIKELY(const_expr->has_flag(ObExprInfoFlag::CNT_CUR_TIME))) {
        ret = OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES;
        LOG_WARN("non-constant expression is not allowed for pivot|unpivot values", KPC(const_expr), K(ret));
      } else if (OB_FAIL(const_exprs.push_back(const_expr))) {
        LOG_WARN("fail to push_back const_expr", KPC(const_expr), K(ret));
      }
    }
  } else {
    if (OB_FAIL(ObResolverUtils::resolve_const_expr(params_, expr_node, const_expr, NULL))) {
      LOG_WARN("fail to resolve_const_expr", K(ret));
    } else if (OB_UNLIKELY(!const_expr->has_const_or_const_expr_flag()) ||
               OB_UNLIKELY(const_expr->has_flag(ObExprInfoFlag::CNT_CUR_TIME))) {
      ret = OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES;
      LOG_WARN("non-constant expression is not allowed for pivot|unpivot values", KPC(const_expr), K(ret));
    } else if (OB_FAIL(const_exprs.push_back(const_expr))) {
      LOG_WARN("fail to push_back const_expr", KPC(const_expr), K(ret));
    }
  }
  return ret;
}

int ObDMLResolver::check_pivot_aggr_expr(ObRawExpr* expr) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->has_flag(IS_AGG))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr expr_ is unexpected", KPC(expr), K(ret));
  } else if (expr->get_expr_type() == T_FUN_GROUP_CONCAT) {
    // succ
  } else if (OB_UNLIKELY(static_cast<ObAggFunRawExpr*>(expr)->get_real_param_count() > 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("aggr expr_ has invalid argument", KPC(expr), K(ret));
  } else {
    switch (expr->get_expr_type()) {
      case T_FUN_MAX:
      case T_FUN_MIN:
      case T_FUN_SUM:
      case T_FUN_COUNT:
      case T_FUN_AVG:
      case T_FUN_APPROX_COUNT_DISTINCT:
      case T_FUN_STDDEV:
      case T_FUN_VARIANCE: {
        break;
      }
      default: {
        ret = OB_ERR_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION;
        LOG_WARN("expect aggregate function inside pivot operation", KPC(expr), K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_transpose_clause(
    const ParseNode& transpose_node, TransposeItem& transpose_item, ObIArray<ObString>& columns_in_aggrs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(transpose_node.type_ != T_PIVOT && transpose_node.type_ != T_UNPIVOT) ||
      OB_UNLIKELY(transpose_node.num_child_ != 4) || OB_ISNULL(transpose_node.children_) ||
      OB_ISNULL(transpose_node.children_[0]) || OB_ISNULL(transpose_node.children_[1]) ||
      OB_ISNULL(transpose_node.children_[2]) || OB_ISNULL(transpose_node.children_[3])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transpose_node is unexpected",
        K(transpose_node.type_),
        K(transpose_node.num_child_),
        KP(transpose_node.children_),
        K(ret));
  } else if (T_PIVOT == transpose_node.type_) {
    transpose_item.set_pivot();

    // pivot aggr
    const ParseNode& aggr_node = *transpose_node.children_[0];
    if (OB_UNLIKELY(aggr_node.type_ != T_PIVOT_AGGR_LIST) || OB_UNLIKELY(aggr_node.num_child_ <= 0) ||
        OB_ISNULL(aggr_node.children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr_node is unexpected", K(aggr_node.type_), K(aggr_node.num_child_), KP(aggr_node.children_), K(ret));
    } else {
      ObArray<ObQualifiedName> qualified_name_in_aggr;
      for (int64_t i = 0; OB_SUCC(ret) && i < aggr_node.num_child_; ++i) {
        const ParseNode* tmp_node = aggr_node.children_[i];
        const ParseNode* alias_node = NULL;
        TransposeItem::AggrPair aggr_pair;
        qualified_name_in_aggr.reuse();
        if (OB_ISNULL(tmp_node) || OB_UNLIKELY(tmp_node->type_ != T_PIVOT_AGGR) ||
            OB_UNLIKELY(tmp_node->num_child_ != 2) || OB_ISNULL(tmp_node->children_) ||
            OB_ISNULL(tmp_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tmp_node is unexpected", KP(tmp_node), K(ret));
        } else if (OB_FAIL(resolve_sql_expr(*tmp_node->children_[0], aggr_pair.expr_, &qualified_name_in_aggr))) {
          LOG_WARN("fail to resolve_sql_expr", K(ret));
        } else if (OB_FAIL(check_pivot_aggr_expr(aggr_pair.expr_))) {
          LOG_WARN("fail to check_aggr_expr", K(ret));
        } else if (NULL != (alias_node = tmp_node->children_[1]) &&
                   FALSE_IT(aggr_pair.alias_name_.assign_ptr(alias_node->str_value_, alias_node->str_len_))) {
        } else if (OB_FAIL(transpose_item.aggr_pairs_.push_back(aggr_pair))) {
          LOG_WARN("fail to push_back aggr_pair", K(aggr_pair), K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < qualified_name_in_aggr.count(); ++j) {
            ObString& column_name = qualified_name_in_aggr.at(j).col_name_;
            if (!has_exist_in_array(columns_in_aggrs, column_name)) {
              if (OB_FAIL(columns_in_aggrs.push_back(column_name))) {
                LOG_WARN("fail to push_back column_name", K(column_name), K(ret));
              }
            }
          }
        }
      }
    }

    // pivot for
    if (OB_SUCC(ret)) {
      if (OB_FAIL(resolve_transpose_columns(*transpose_node.children_[1], transpose_item.for_columns_))) {
        LOG_WARN("fail to resolve_transpose_columns", K(ret));
      }
    }

    // pivot in
    if (OB_SUCC(ret)) {
      const ParseNode& in_node = *transpose_node.children_[2];
      if (OB_UNLIKELY(in_node.type_ != T_PIVOT_IN_LIST) || OB_UNLIKELY(in_node.num_child_ <= 0) ||
          OB_ISNULL(in_node.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in_node is unexpected", K(in_node.type_), K(in_node.num_child_), KP(in_node.children_), K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < in_node.num_child_; ++i) {
          const ParseNode* column_node = in_node.children_[i];
          const ParseNode* alias_node = NULL;
          TransposeItem::InPair in_pair;
          if (OB_ISNULL(column_node) || OB_UNLIKELY(column_node->type_ != T_PIVOT_IN) ||
              OB_UNLIKELY(column_node->num_child_ != 2) || OB_ISNULL(column_node->children_) ||
              OB_ISNULL(column_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column_node is unexpected", KP(column_node), K(ret));
          } else if (OB_FAIL(resolve_const_exprs(*column_node->children_[0], in_pair.exprs_))) {
            LOG_WARN("fail to resolve_const_exprs", K(ret));
          } else if (OB_UNLIKELY(in_pair.exprs_.count() != transpose_item.for_columns_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("in expr number is equal for columns", K(in_pair.exprs_), K(transpose_item.for_columns_), K(ret));
          } else if (NULL != (alias_node = column_node->children_[1])) {
            if (OB_UNLIKELY(alias_node->str_len_ > OB_MAX_COLUMN_NAME_LENGTH)) {
              ret = OB_ERR_TOO_LONG_IDENT;
              LOG_WARN("alias name for pivot is too long",
                  K(ret),
                  K(ObString(alias_node->str_len_, alias_node->str_value_)));
            } else {
              in_pair.pivot_expr_alias_.assign_ptr(alias_node->str_value_, alias_node->str_len_);
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(transpose_item.in_pairs_.push_back(in_pair))) {
            LOG_WARN("fail to push_back in_pair", K(in_pair), K(ret));
          }
        }  // end of for in node
      }
    }

    // alias
    if (OB_SUCC(ret)) {
      const ParseNode& alias = *transpose_node.children_[3];
      if (alias.str_len_ > 0 && alias.str_value_ != NULL) {
        transpose_item.alias_name_.assign_ptr(alias.str_value_, alias.str_len_);
      }
    }
  } else {
    transpose_item.set_unpivot();
    transpose_item.set_include_nulls(2 == transpose_node.value_);

    // unpivot column
    if (OB_FAIL(resolve_transpose_columns(*transpose_node.children_[0], transpose_item.unpivot_columns_))) {
      LOG_WARN("fail to resolve_transpose_columns", K(ret));

      // unpivot for
    } else if (OB_FAIL(resolve_transpose_columns(*transpose_node.children_[1], transpose_item.for_columns_))) {
      LOG_WARN("fail to resolve_transpose_columns", K(ret));

      // unpivot in
    } else {
      const ParseNode& in_node = *transpose_node.children_[2];
      if (OB_UNLIKELY(in_node.type_ != T_UNPIVOT_IN_LIST) || OB_UNLIKELY(in_node.num_child_ <= 0) ||
          OB_ISNULL(in_node.children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in_node is unexpected", K(in_node.type_), K(in_node.num_child_), KP(in_node.children_), K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < in_node.num_child_; ++i) {
          const ParseNode* column_node = in_node.children_[i];
          TransposeItem::InPair in_pair;
          if (OB_ISNULL(column_node) || OB_UNLIKELY(column_node->type_ != T_UNPIVOT_IN) ||
              OB_UNLIKELY(column_node->num_child_ != 2) || OB_ISNULL(column_node->children_) ||
              OB_ISNULL(column_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column_node is unexpectedl", KP(column_node), K(ret));
          } else if (OB_FAIL(resolve_transpose_columns(*column_node->children_[0], in_pair.column_names_))) {
            LOG_WARN("fail to resolve_transpose_columns", K(ret));
          } else if (OB_UNLIKELY(in_pair.column_names_.count() != transpose_item.unpivot_columns_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unpivot column count is not match for in column count", K(transpose_item), K(in_pair), K(ret));
          } else if (NULL != column_node->children_[1]) {
            if (OB_FAIL(resolve_const_exprs(*column_node->children_[1], in_pair.exprs_))) {
              LOG_WARN("fail to resolve_const_exprs", K(ret));
            } else if (OB_UNLIKELY(in_pair.exprs_.count() != transpose_item.for_columns_.count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("in column count is not match in literal count", K(transpose_item), K(in_pair), K(ret));
            } else if (OB_FAIL(transpose_item.in_pairs_.push_back(in_pair))) {
              LOG_WARN("fail to push_back in_pair", K(in_pair), K(ret));
            }
          } else if (OB_FAIL(transpose_item.in_pairs_.push_back(in_pair))) {
            LOG_WARN("fail to push_back in_pair", K(in_pair), K(ret));
          }
        }  // end of for in node
      }
    }  // end of in

    // alias
    if (OB_SUCC(ret)) {
      const ParseNode& alias = *transpose_node.children_[3];
      if (alias.str_len_ > 0 && alias.str_value_ != NULL) {
        transpose_item.alias_name_.assign_ptr(alias.str_value_, alias.str_len_);
      }
    }
  }
  LOG_DEBUG("finish resolve_transpose_clause", K(transpose_item), K(columns_in_aggrs), K(ret));
  return ret;
}

int ObDMLResolver::resolve_transpose_table(const ParseNode* transpose_node, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (transpose_node == NULL) {
    // do nothing
  } else if (OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_factory_ is null", K(ret));
  } else if (OB_ISNULL(ptr = params_.expr_factory_->get_allocator().alloc(sizeof(TransposeItem)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no more memory to create TransposeItem");
  } else {
    TransposeItem* transpose_item = new (ptr) TransposeItem();
    TableItem* orig_table_item = table_item;
    table_item = NULL;
    ObSEArray<ObString, 16> columns_in_aggrs;
    ObSqlString transpose_def;
    if (OB_ISNULL(orig_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_item or stmt is unexpected", KP(orig_table_item), K(ret));
    } else if (OB_FAIL(column_namespace_checker_.add_reference_table(orig_table_item))) {
      LOG_WARN("add reference table to namespace checker failed", K(ret));
    } else if (OB_FAIL(resolve_transpose_clause(*transpose_node, *transpose_item, columns_in_aggrs))) {
      LOG_WARN("resolve transpose clause failed", K(ret));
    } else if (OB_FAIL(get_transpose_target_sql(columns_in_aggrs, *orig_table_item, *transpose_item, transpose_def))) {
      LOG_WARN("fail to get_transpose_target_sql", KPC(orig_table_item), K(ret));
    } else if (OB_FAIL(remove_orig_table_item(*orig_table_item))) {
      LOG_WARN("remove_orig_table_item failed", K(ret));
    } else if (OB_FAIL(expand_transpose(transpose_def, *transpose_item, table_item))) {
      LOG_WARN("expand_transpose failed", K(ret));
    }
  }
  return ret;
}

int ObDMLResolver::expand_transpose(
    const ObSqlString& transpose_def, TransposeItem& transpose_item, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  ObParser parser(*params_.allocator_, session_info_->get_sql_mode());
  ParseResult transpose_result;
  if (OB_FAIL(parser.parse(transpose_def.string(), transpose_result))) {
    LOG_WARN("parse view defination failed", K(transpose_def.string()), K(ret));
  } else if (OB_ISNULL(transpose_result.result_tree_) || OB_ISNULL(transpose_result.result_tree_->children_) ||
             OB_UNLIKELY(transpose_result.result_tree_->num_child_ < 1) ||
             OB_ISNULL(transpose_result.result_tree_->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transpose_result.result_tree_ is null", K(transpose_result.result_tree_), K(ret));
  } else {
    // use select resolver
    ObSelectResolver select_resolver(params_);
    select_resolver.set_current_level(current_level_);
    select_resolver.set_in_subquery(true);
    select_resolver.set_parent_namespace_resolver(parent_namespace_resolver_);
    select_resolver.set_transpose_item(&transpose_item);
    if (OB_FAIL(add_cte_table_to_children(select_resolver))) {
      LOG_WARN("add_cte_table_to_children failed", K(ret));
    } else if (OB_FAIL(do_resolve_generate_table(*transpose_result.result_tree_->children_[0],
                   transpose_item.alias_name_,
                   select_resolver,
                   table_item))) {
      LOG_WARN("do_resolve_generate_table failed", K(ret));
    } else if (OB_FAIL(mark_unpivot_table(transpose_item, table_item))) {
      LOG_WARN("fail to mark_unpivot_table", KPC(table_item), K(ret));
    }
    LOG_DEBUG("finish expand_transpose", K(transpose_def), K(transpose_item), KPC(table_item));
  }
  return ret;
}

int ObDMLResolver::mark_unpivot_table(TransposeItem& transpose_item, TableItem* table_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item) || OB_UNLIKELY(!table_item->is_generated_table()) || OB_ISNULL(table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_item or unexpected", KPC(table_item), K(ret));
  } else if (transpose_item.is_unpivot() && transpose_item.need_use_unpivot_op()) {
    ObSelectStmt& select_stmt = *table_item->ref_query_;
    for (int64_t i = select_stmt.get_unpivot_info().get_output_column_count(); i < select_stmt.get_select_item_size();
         i++) {
      SelectItem& item = select_stmt.get_select_item(i);
      item.is_unpivot_mocked_column_ = true;
    }
  }
  return ret;
}

int get_column_item_idx_by_name(ObIArray<ColumnItem>& array, const ObString& var, int64_t& idx)
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_INDEX;
  const int64_t num = array.count();
  for (int64_t i = 0; i < num; i++) {
    if (var == array.at(i).column_name_) {
      idx = i;
      break;
    }
  }
  return ret;
}

int ObDMLResolver::get_transpose_target_sql(
    const ObIArray<ObString>& columns_in_aggrs, TableItem& table_item, TransposeItem& transpose_item, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  sql.reuse();
  // 1.get columns
  ObSEArray<ColumnItem, 16> column_items;
  if (table_item.is_basic_table() || table_item.is_fake_cte_table()) {
    if (OB_FAIL(resolve_all_basic_table_columns(table_item, false, &column_items))) {
      LOG_WARN("resolve all basic table columns failed", K(ret));
    }
  } else if (table_item.is_generated_table()) {
    if (OB_FAIL(resolve_all_generated_table_columns(table_item, column_items))) {
      LOG_WARN("resolve all generated table columns failed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not support this table", K(table_item), K(ret));
  }

  int64_t idx = OB_INVALID_INDEX;
  if (transpose_item.is_pivot()) {
    // 2.check and get groupby column
    for (int64_t i = 0; OB_SUCC(ret) && i < columns_in_aggrs.count(); ++i) {
      const ObString& column_name = columns_in_aggrs.at(i);
      if (OB_FAIL(get_column_item_idx_by_name(column_items, column_name, idx))) {
        LOG_WARN("fail to get_column_item_idx_by_name", K(column_name), K(ret));
      } else if (idx >= 0) {
        if (OB_FAIL(column_items.remove(idx))) {
          LOG_WARN("fail to remove column_item", K(idx), K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < transpose_item.for_columns_.count(); ++i) {
      ObString& for_column = transpose_item.for_columns_.at(i);
      if (OB_FAIL(get_column_item_idx_by_name(column_items, for_column, idx))) {
        LOG_WARN("fail to get_column_item_idx_by_name", K(for_column), K(ret));
      } else if (idx >= 0) {
        if (OB_FAIL(column_items.remove(idx))) {
          LOG_WARN("fail to remove column_item", K(idx), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      transpose_item.old_column_count_ = column_items.count();
      if (OB_FAIL(get_target_sql_for_pivot(column_items, table_item, transpose_item, sql))) {
        LOG_WARN("fail to get_target_sql_for_pivot", K(ret));
      }
    }
    LOG_DEBUG("finish get_transpose_target_sql", K(ret), K(sql), K(transpose_item));
    transpose_item.reset();  // no need aggr/for/in expr, reset here
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < transpose_item.in_pairs_.count(); ++i) {
      const common::ObIArray<ObString>& in_columns = transpose_item.in_pairs_.at(i).column_names_;
      for (int64_t j = 0; OB_SUCC(ret) && j < in_columns.count(); ++j) {
        const ObString& column_name = in_columns.at(j);
        if (OB_FAIL(get_column_item_idx_by_name(column_items, column_name, idx))) {
          LOG_WARN("fail to get_column_item_idx_by_name", K(column_name), K(ret));
        } else if (idx >= 0) {
          if (OB_FAIL(column_items.remove(idx))) {
            LOG_WARN("fail to remove column_item", K(idx), K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      transpose_item.old_column_count_ = column_items.count();
      if (OB_FAIL(get_target_sql_for_unpivot(column_items, table_item, transpose_item, sql))) {
        LOG_WARN("fail to get_target_sql_for_unpivot", K(ret));
      }
    }
    LOG_DEBUG("finish get_transpose_target_sql", K(ret), K(sql), K(transpose_item));
  }
  return ret;
}

// for example
//
// t1(c1, c2, c3, c4)
//
// from_list(basic_table):
// t1
// pivot (
//  sum(c1) as sum,
//  max(c1)
//  for (c2, c3)
//  in ((1, 1) as "11",
//      (2, 2)
//      )
//)
// t11
//
// from_list(generated_table):
//(select * from t1)
// pivot (
//  sum(c1) as sum,
//  max(c1)
//  for (c2, c3)
//  in ((1, 1) as "11",
//      (2, 2)
//      )
//) t11
//
// from_list(target_table):
//(select
//  c4,
//  sum(case when (c2, c3) in ((1, 1)) then c1 end) as "11_sum",
//  max(case when (c2, c3) in ((1, 1)) then c1 end) as "11",
//  sum(case when (c2, c3) in ((2, 2)) then c1 end) as "2_2_sum",
//  max(case when (c2, c3) in ((2, 2)) then c1 end) as "2_2"
// from t1
// group by c4
//) t11
int ObDMLResolver::get_target_sql_for_pivot(
    const ObIArray<ColumnItem>& column_items, TableItem& table_item, TransposeItem& transpose_item, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  sql.reuse();
  if (transpose_item.is_pivot()) {
    if (!transpose_item.alias_name_.empty()) {
      if (OB_FAIL(sql.append("SELECT * FROM ( "))) {
        LOG_WARN("fail to append_fmt", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append("SELECT"))) {
      LOG_WARN("fail to append_fmt", K(ret));
    } else if (table_item.is_generated_table() && table_item.ref_query_ != NULL) {
      const ObStmtHint& stmt_hint = table_item.ref_query_->get_stmt_hint();
      if ((stmt_hint.aggregate_ != OB_UNSET_AGGREGATE_TYPE)) {
        const bool is_no_use_hash = (stmt_hint.aggregate_ & OB_NO_USE_HASH_AGGREGATE);
        if (OB_FAIL(sql.append_fmt(" /*+%s*/", is_no_use_hash ? "NO_USE_HASH_AGGREGATION" : "USE_HASH_AGGREGATION"))) {
          LOG_WARN("fail to append_fmt", K(ret));
        }
      }
    }

    if (!column_items.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
        const ObString& column_name = column_items.at(i).column_name_;
        if (OB_FAIL(sql.append_fmt(" \"%.*s\",", column_name.length(), column_name.ptr()))) {
          LOG_WARN("fail to append_fmt", K(column_name), K(ret));
        }
      }
    }
    const int64_t DISTINCT_LENGTH = strlen("DISTINCT");
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], expr_str_buf)
    {
      MEMSET(expr_str_buf, 0, sizeof(expr_str_buf));
      for (int64_t i = 0; OB_SUCC(ret) && i < transpose_item.in_pairs_.count(); ++i) {
        const TransposeItem::InPair& in_pair = transpose_item.in_pairs_.at(i);

        for (int64_t j = 0; OB_SUCC(ret) && j < transpose_item.aggr_pairs_.count(); ++j) {
          const TransposeItem::AggrPair& aggr_pair = transpose_item.aggr_pairs_.at(j);
          const char* format_str =
              ((static_cast<const ObAggFunRawExpr*>(aggr_pair.expr_))->is_param_distinct() ? " %s(DISTINCT CASE WHEN ("
                                                                                           : " %s(CASE WHEN (");
          if (OB_FAIL(sql.append_fmt(format_str, ob_aggr_func_str(aggr_pair.expr_->get_expr_type())))) {
            LOG_WARN("fail to append_fmt", K(aggr_pair.expr_->get_expr_type()), K(ret));
          }
          for (int64_t k = 0; OB_SUCC(ret) && k < transpose_item.for_columns_.count(); ++k) {
            const ObString& column_name = transpose_item.for_columns_.at(k);
            if (OB_FAIL(sql.append_fmt("\"%.*s\",", column_name.length(), column_name.ptr()))) {
              LOG_WARN("fail to append_fmt", K(column_name), K(ret));
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(sql.set_length(sql.length() - 1))) {
            LOG_WARN("fail to set_length", K(sql.length()), K(ret));
          } else if (OB_FAIL(sql.append(") in (("))) {
            LOG_WARN("fail to append", K(ret));
          }

          for (int64_t k = 0; OB_SUCC(ret) && k < in_pair.exprs_.count(); ++k) {
            ObRawExpr* expr = in_pair.exprs_.at(k);
            int64_t pos = 0;
            ObRawExprPrinter expr_printer(
                expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos, TZ_INFO(params_.session_info_));
            if (OB_FAIL(expr_printer.do_print(expr, T_NONE_SCOPE, true))) {
              LOG_WARN("print expr definition failed", KPC(expr), K(ret));
            } else if (OB_FAIL(sql.append_fmt("%.*s,", static_cast<int32_t>(pos), expr_str_buf))) {
              LOG_WARN("fail to append_fmt", K(expr_str_buf), K(ret));
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(sql.set_length(sql.length() - 1))) {
            LOG_WARN("fail to set_length", K(sql.length()), K(ret));
          } else if (OB_FAIL(sql.append("))"))) {
            LOG_WARN("fail to append", K(ret));
          }

          int64_t pos = 0;
          int32_t expr_name_length = strlen(ob_aggr_func_str(aggr_pair.expr_->get_expr_type())) + 1;
          if ((static_cast<const ObAggFunRawExpr*>(aggr_pair.expr_))->is_param_distinct()) {
            expr_name_length += DISTINCT_LENGTH;
          }
          ObRawExprPrinter expr_printer(
              expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos, TZ_INFO(params_.session_info_));
          if (OB_FAIL(expr_printer.do_print(aggr_pair.expr_, T_NONE_SCOPE, true))) {
            LOG_WARN("print expr definition failed", KPC(aggr_pair.expr_), K(ret));
          } else if (OB_FAIL(sql.append_fmt(" THEN %.*s END) AS \"",
                         static_cast<int32_t>(pos - expr_name_length - 1),
                         expr_str_buf + expr_name_length))) {
            LOG_WARN("fail to append_fmt", K(aggr_pair.alias_name_), K(ret));
          } else {
            ObString tmp(pos, expr_str_buf);
            int64_t sql_length = sql.length();
            if (in_pair.pivot_expr_alias_.empty()) {
              for (int64_t k = 0; OB_SUCC(ret) && k < in_pair.exprs_.count(); ++k) {
                ObRawExpr* expr = in_pair.exprs_.at(k);
                int64_t pos = 0;
                ObRawExprPrinter expr_printer(
                    expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos, TZ_INFO(params_.session_info_));
                if (OB_FAIL(expr_printer.do_print(expr, T_NONE_SCOPE, true))) {
                  LOG_WARN("print expr definition failed", KPC(expr), K(ret));
                } else if (OB_FAIL(sql.append_fmt("%.*s_", static_cast<int32_t>(pos), expr_str_buf))) {
                  LOG_WARN("fail to append_fmt", K(expr_str_buf), K(ret));
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(sql.set_length(sql.length() - 1))) {
                LOG_WARN("fail to set_length", K(sql.length()), K(ret));
              }
            } else {
              if (OB_FAIL(
                      sql.append_fmt("%.*s", in_pair.pivot_expr_alias_.length(), in_pair.pivot_expr_alias_.ptr()))) {
                LOG_WARN("fail to append_fmt", K(in_pair.pivot_expr_alias_), K(ret));
              }
            }
            if (OB_FAIL(ret)) {
            } else if (!aggr_pair.alias_name_.empty()) {
              if (OB_FAIL(sql.append_fmt("_%.*s", aggr_pair.alias_name_.length(), aggr_pair.alias_name_.ptr()))) {
                LOG_WARN("fail to append_fmt", K(aggr_pair.alias_name_), K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              sql.set_length(MIN(sql.length(), sql_length + OB_MAX_COLUMN_NAME_LENGTH));
              if (OB_FAIL(sql.append("\","))) {
                LOG_WARN("fail to append", K(ret));
              }
            }
          }
        }  // end of aggr
      }    // end of in
    }      // end SMART_VAR

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.set_length(sql.length() - 1))) {
      LOG_WARN("fail to set_length", K(sql.length()), K(ret));
    } else if (OB_FAIL(format_from_subquery(transpose_item.alias_name_, table_item, expr_str_buf, sql))) {
      LOG_WARN("fail to format_from_subquery", K(table_item), K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (!column_items.empty()) {
      if (OB_FAIL(sql.append(" GROUP BY"))) {
        LOG_WARN("fail to append", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
        const ObString& column_name = column_items.at(i).column_name_;
        if (OB_FAIL(sql.append_fmt(" \"%.*s\",", column_name.length(), column_name.ptr()))) {
          LOG_WARN("fail to append_fmt", K(column_name), K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql.set_length(sql.length() - 1))) {
        LOG_WARN("fail to set_length", K(sql.length()), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (!transpose_item.alias_name_.empty()) {
        if (OB_FAIL(sql.append_fmt(" ) %.*s", transpose_item.alias_name_.length(), transpose_item.alias_name_.ptr()))) {
          LOG_WARN("fail to append", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::format_from_subquery(
    const ObString& unpivot_alias_name, TableItem& table_item, char* expr_str_buf, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  if (table_item.is_basic_table()) {
    if (OB_FAIL(sql.append_fmt(" FROM %.*s", table_item.table_name_.length(), table_item.table_name_.ptr()))) {
      LOG_WARN("fail to append_fmt", K(table_item.table_name_), K(ret));
    } else if (!table_item.alias_name_.empty() && table_item.alias_name_ != table_item.table_name_) {
      if (OB_FAIL(sql.append_fmt(" %.*s", table_item.alias_name_.length(), table_item.alias_name_.ptr()))) {
        LOG_WARN("fail to append_fmt", K(table_item.alias_name_), K(ret));
      }
    } else if (!unpivot_alias_name.empty()) {
      if (OB_FAIL(sql.append_fmt(" %.*s", unpivot_alias_name.length(), unpivot_alias_name.ptr()))) {
        LOG_WARN("fail to append_fmt", K(unpivot_alias_name), K(ret));
      }
    }
  } else {
    ObIArray<ObString>* column_list = NULL;
    bool is_set_subquery = false;
    int64_t pos = 0;
    ObSelectStmtPrinter stmt_printer(expr_str_buf,
        OB_MAX_DEFAULT_VALUE_LENGTH,
        &pos,
        static_cast<ObSelectStmt*>(table_item.ref_query_),
        table_item.ref_query_->tz_info_,
        column_list,
        is_set_subquery);
    if (OB_FAIL(stmt_printer.do_print())) {
      LOG_WARN("fail to print generated table", K(ret));
    } else if (OB_FAIL(sql.append_fmt(" FROM (%.*s)", static_cast<int32_t>(pos), expr_str_buf))) {
      LOG_WARN("fail to append_fmt", K(ret));
    } else if (table_item.cte_type_ == TableItem::NOT_CTE && !table_item.table_name_.empty()) {
      if (OB_FAIL(sql.append_fmt(" %.*s", table_item.table_name_.length(), table_item.table_name_.ptr()))) {
        LOG_WARN("fail to append_fmt", K(ret));
      }
    } else if (!unpivot_alias_name.empty()) {
      if (OB_FAIL(sql.append_fmt(" %.*s", unpivot_alias_name.length(), unpivot_alias_name.ptr()))) {
        LOG_WARN("fail to append_fmt", K(unpivot_alias_name), K(ret));
      }
    }
  }
  return ret;
}

// for example
//
// t1(c1, c2, c3, c4)
//
// from_list(basic_table):
// t1
// unpivot exclude nulls (
//(sal1, sal2)
// for (deptno1, deptno2)
// in ((c2, c3),
//    (c3, c4) as ('c33', 'c44')
//)
// t11
//
// from_list(generated_table):
//(select * from t1)
// unpivot exclude nulls (
//(sal1, sal2)
// for (deptno1, deptno2)
// in ((c2, c3),
//    (c3, c4) as ('c33', 'c44')
//)
//) t11
//
// from_list(target_table):
// select * from
// (select c1, 'c2_c3' as deptno1, 'c2_c3' as deptno2, c2 as sal1, c3 as sal2 ,
//             'c33' as deptno1, 'c44' as deptno2, c3 as sal1, c4 as sal2
//  from pivoted_emp2
//  where (c2 is not null or c3 is not null)
//         and (c3 is not null or c4 is not null)
// )
int ObDMLResolver::get_target_sql_for_unpivot(
    const ObIArray<ColumnItem>& column_items, TableItem& table_item, TransposeItem& transpose_item, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  sql.reuse();
  if (transpose_item.is_unpivot()) {
    if (OB_FAIL(sql.append("SELECT /*+NO_REWRITE UNPIVOT*/* FROM (SELECT /*+NO_REWRITE*/"))) {
      LOG_WARN("fail to append", K(ret));
    } else if (!column_items.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
        const ObString& column_name = column_items.at(i).column_name_;
        if (OB_FAIL(sql.append_fmt(" \"%.*s\",", column_name.length(), column_name.ptr()))) {
          LOG_WARN("fail to append_fmt", K(column_name), K(ret));
        }
      }
    }

    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], expr_str_buf)
    {
      MEMSET(expr_str_buf, 0, sizeof(expr_str_buf));
      for (int64_t i = 0; OB_SUCC(ret) && i < transpose_item.in_pairs_.count(); ++i) {
        const TransposeItem::InPair& in_pair = transpose_item.in_pairs_.at(i);
        for (int64_t j = 0; OB_SUCC(ret) && j < transpose_item.for_columns_.count(); ++j) {
          const ObString& for_column_name = transpose_item.for_columns_.at(j);

          if (in_pair.exprs_.empty()) {
            if (OB_FAIL(sql.append(" '"))) {
              LOG_WARN("fail to append_fmt", K(ret));
            }
            // TODO::use upper
            for (int64_t k = 0; OB_SUCC(ret) && k < in_pair.column_names_.count(); ++k) {
              const ObString& column_name = in_pair.column_names_.at(k);
              if (OB_FAIL(sql.append_fmt("%.*s_", column_name.length(), column_name.ptr()))) {
                LOG_WARN("fail to append_fmt", K(column_name), K(ret));
              }
            }

            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(sql.set_length(sql.length() - 1))) {
              LOG_WARN("fail to set_length", K(sql.length()), K(ret));
            } else if (0 == i) {
              if (OB_FAIL(sql.append_fmt("' AS \"%.*s\",", for_column_name.length(), for_column_name.ptr()))) {
                LOG_WARN("fail to append", K(for_column_name), K(ret));
              }
            } else if (OB_FAIL(
                           sql.append_fmt("' AS \"%ld_%.*s\",", i, for_column_name.length(), for_column_name.ptr()))) {
              LOG_WARN("fail to append", K(for_column_name), K(ret));
            }
          } else {
            ObRawExpr* expr = in_pair.exprs_.at(j);
            int64_t pos = 0;
            ObRawExprPrinter expr_printer(
                expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos, TZ_INFO(params_.session_info_));
            if (OB_FAIL(expr_printer.do_print(expr, T_NONE_SCOPE, true))) {
              LOG_WARN("print expr definition failed", KPC(expr), K(ret));
            } else if (0 == i) {
              if (OB_FAIL(sql.append_fmt(" %.*s AS \"%.*s\",",
                      static_cast<int32_t>(pos),
                      expr_str_buf,
                      for_column_name.length(),
                      for_column_name.ptr()))) {
                LOG_WARN("fail to append", K(for_column_name), K(ret));
              }
            } else if (OB_FAIL(sql.append_fmt(" %.*s AS \"%ld_%.*s\",",
                           static_cast<int32_t>(pos),
                           expr_str_buf,
                           i,
                           for_column_name.length(),
                           for_column_name.ptr()))) {
              LOG_WARN("fail to append", K(for_column_name), K(ret));
            }
          }
        }

        for (int64_t j = 0; OB_SUCC(ret) && j < in_pair.column_names_.count(); ++j) {
          const ObString& in_column_name = in_pair.column_names_.at(j);
          const ObString& unpivot_column_name = transpose_item.unpivot_columns_.at(j);
          if (0 == i) {
            if (OB_FAIL(sql.append_fmt(" \"%.*s\" AS \"%.*s\",",
                    in_column_name.length(),
                    in_column_name.ptr(),
                    unpivot_column_name.length(),
                    unpivot_column_name.ptr()))) {
              LOG_WARN("fail to append_fmt", K(in_column_name), K(unpivot_column_name), K(ret));
            }
          } else if (OB_FAIL(sql.append_fmt(" \"%.*s\" AS \"%ld_%.*s\",",
                         in_column_name.length(),
                         in_column_name.ptr(),
                         i,
                         unpivot_column_name.length(),
                         unpivot_column_name.ptr()))) {
            LOG_WARN("fail to append_fmt", K(in_column_name), K(unpivot_column_name), K(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql.set_length(sql.length() - 1))) {
        LOG_WARN("fail to set_length", K(sql.length()), K(ret));
      } else if (OB_FAIL(format_from_subquery(transpose_item.alias_name_, table_item, expr_str_buf, sql))) {
        LOG_WARN("fail to format_from_subquery", K(table_item), K(ret));
      } else if (transpose_item.is_exclude_null()) {
        if (OB_FAIL(sql.append(" WHERE"))) {
          LOG_WARN("fail to append", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < transpose_item.in_pairs_.count(); ++i) {
          const TransposeItem::InPair& in_pair = transpose_item.in_pairs_.at(i);
          const char* format_str = (i != 0 ? " OR (" : " (");
          if (OB_FAIL(sql.append(format_str))) {
            LOG_WARN("fail to append", K(ret));
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < in_pair.column_names_.count(); ++j) {
            const ObString& column_name = in_pair.column_names_.at(j);
            const char* format_str = (j != 0 ? " OR \"%.*s\" IS NOT NULL" : " \"%.*s\" IS NOT NULL");
            if (OB_FAIL(sql.append_fmt(format_str, column_name.length(), column_name.ptr()))) {
              LOG_WARN("fail to append_fmt", K(column_name), K(ret));
            }
          }
          if (OB_FAIL(sql.append(")"))) {
            LOG_WARN("fail to append", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql.append(")"))) {
          LOG_WARN("fail to append", K(ret));
        } else if (!transpose_item.alias_name_.empty()) {
          if (OB_FAIL(sql.append_fmt(" %.*s", transpose_item.alias_name_.length(), transpose_item.alias_name_.ptr()))) {
            LOG_WARN("fail to append", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::remove_orig_table_item(TableItem& table_item)
{
  int ret = OB_SUCCESS;
  // need remove last saved table item
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is unexpected", KP(stmt), K(ret));
  } else if (OB_FAIL(column_namespace_checker_.remove_reference_table(table_item.table_id_))) {
    LOG_WARN("failed to remove_reference_table", K(ret));
  } else if (OB_FAIL(stmt->remove_table_item(&table_item))) {
    LOG_WARN("failed to remove target table items", K(ret));
  } else if (OB_FAIL(stmt->remove_column_item(table_item.table_id_))) {
    LOG_WARN("failed to remove column items.", K(ret));
  } else if (OB_FAIL(stmt->remove_part_expr_items(table_item.table_id_))) {
    LOG_WARN("failed to remove part expr item", K(ret));
  } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
    LOG_WARN("rebuild table hash failed", K(ret));
  }
  return ret;
}

// int ObDMLResolver::print_unpivot_table(char *buf, const int64_t size, int64_t &pos,
//    const TableItem &table_item)
//{
//  int ret = OB_SUCCESS;
//  if (OB_ISNULL(buf)
//      || OB_UNLIKELY(table_item.has_unpivot())) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("argument is unexpected", KP(buf), K(table_item), K(ret));
//  } else if (OB_UNLIKELY(size <= pos)) {
//    ret = OB_SIZE_OVERFLOW;
//    LOG_WARN("size is overflow", K(size), K(pos), K(ret));
//  } else {
//    databuff_printf(buf, size, pos, "()");
//  }
//  return ret;
//}

int ObDMLResolver::resolve_returning(const ParseNode* parse_tree)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(parse_tree)) {
    ObDelUpdStmt* del_up_stmt = static_cast<ObDelUpdStmt*>(stmt_);
    ObItemType item_type;
    int64_t aggr_cnt = 0;
    int64_t org_mocked_tables_cnt = ObSQLMockSchemaUtils::get_all_mocked_tables().count();

    CK(OB_NOT_NULL(del_up_stmt));
    CK(OB_LIKELY(T_RETURNING == parse_tree->type_));
    CK(OB_LIKELY(2 == parse_tree->num_child_));
    CK(OB_NOT_NULL(parse_tree->children_[0]));
    CK(OB_LIKELY(T_PROJECT_LIST == parse_tree->children_[0]->type_));
    CK(OB_ISNULL(parse_tree->children_[1]) || OB_LIKELY(T_INTO_VARIABLES == parse_tree->children_[1]->type_));

    if (OB_SUCC(ret)) {
      current_scope_ = T_FIELD_LIST_SCOPE;
      const ParseNode* returning_exprs = parse_tree->children_[0];
      const ParseNode* returning_intos = parse_tree->children_[1];
      for (int64_t i = 0; OB_SUCC(ret) && i < returning_exprs->num_child_; i++) {
        ObRawExpr* expr = NULL;
        CK(OB_NOT_NULL(returning_exprs->children_[i]));
        CK(OB_NOT_NULL(returning_exprs->children_[i]->children_[0]));
        OZ(resolve_sql_expr(*(returning_exprs->children_[i]->children_[0]), expr));
        CK(OB_NOT_NULL(expr));
        OZ(recursive_replace_rowid_col(expr));
        OZ(expr->formalize(session_info_));
        if (OB_SUCC(ret)) {
          ObString expr_name;
          expr_name.assign_ptr(
              returning_exprs->children_[i]->str_value_, static_cast<int32_t>(returning_exprs->children_[i]->str_len_));
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
        OZ(resolve_into_variables(returning_intos, user_vars, del_up_stmt->get_returning_into_exprs()));
      }
      if (OB_SUCC(ret) && !user_vars.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("returning not support into user vars", K(ret));
      }

      if (OB_SUCC(ret)) {
        del_up_stmt->set_returning(true);
        if (OB_FAIL(check_returning_validity())) {
          LOG_WARN("check returning validity failed", K(ret));
        }
      }
      // all rowids in returing exprs were replaced with calc_urowid(pk1, pk2, ...)
      // no need to mock rowid column for rowids in returning exprs
      if (OB_SUCC(ret)) {
        const int64_t updated_mocked_tables_cnt = ObSQLMockSchemaUtils::get_all_mocked_tables().count();
        int64_t pop_cnt = updated_mocked_tables_cnt - org_mocked_tables_cnt;
        while (del_up_stmt->get_stmt_type() == stmt::T_INSERT && pop_cnt-- > 0) {
          ObSQLMockSchemaUtils::pop_mock_table();
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::check_returning_validity()
{
  int ret = OB_SUCCESS;
  ObDelUpdStmt* del_upd_stmt = NULL;
  bool has_single_set_expr = false;
  bool has_simple_expr = false;
  bool has_sequence = false;
  if (OB_ISNULL(del_upd_stmt = static_cast<ObDelUpdStmt*>(get_stmt())) ||
      OB_UNLIKELY(!get_stmt()->is_insert_stmt() && !get_stmt()->is_update_stmt() && !get_stmt()->is_delete_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < del_upd_stmt->get_returning_exprs().count(); ++i) {
      bool cnt_simple_expr = false;
      bool cnt_single_set_expr = false;
      if (OB_FAIL(check_returinng_expr(
              del_upd_stmt->get_returning_exprs().at(i), cnt_single_set_expr, cnt_simple_expr, has_sequence))) {
        LOG_WARN("failed to check returning expr", K(ret));
      } else if (has_sequence) {
        ret = OB_ERR_SEQ_NOT_ALLOWED_HERE;
        LOG_WARN("sequence number not allowed here in the returning clause", K(ret));
      } else {
        has_single_set_expr = (cnt_single_set_expr || has_single_set_expr);
        has_simple_expr = (cnt_simple_expr || has_simple_expr);
        if (has_simple_expr && has_single_set_expr) {
          ret = OB_ERR_GROUP_FUNC_NOT_ALLOWED;
          LOG_WARN("cannot combine simple expressions and single-set aggregate expressions", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < del_upd_stmt->get_returning_aggr_item_size(); ++i) {
      ObAggFunRawExpr* aggr = NULL;
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

int ObDMLResolver::check_returinng_expr(
    ObRawExpr* expr, bool& has_single_set_expr, bool& has_simple_expr, bool& has_sequenece)
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
    if (OB_FAIL(check_returinng_expr(expr->get_param_expr(i), has_single_set_expr, cnt_simple_expr, has_sequenece))) {
      LOG_WARN("failed to check returning expr", K(ret));
    } else if (!expr->is_aggr_expr() && cnt_simple_expr) {
      has_simple_expr = true;
    }
  }
  return ret;
}

int ObDMLResolver::update_errno_if_sequence_object(const ObQualifiedName& q_name, int old_ret)
{
  int ret = old_ret;
  if (!q_name.tbl_name_.empty() && ObSequenceNamespaceChecker::is_curr_or_next_val(q_name.col_name_)) {
    ret = OB_OBJECT_NAME_NOT_EXIST;
    LOG_WARN("sequence not exists", K(q_name), K(old_ret), K(ret));
    LOG_USER_ERROR(OB_OBJECT_NAME_NOT_EXIST, "sequence");
  }
  return ret;
}

int ObDMLResolver::resolve_sequence_object(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr)
{
  int ret = OB_SUCCESS;
  uint64_t sequence_id = OB_INVALID_ID;
  ObRawExpr* column_expr = NULL;
  ObDMLStmt* stmt = NULL;
  ObSynonymChecker syn_checker;
  if (!q_name.tbl_name_.empty() && ObSequenceNamespaceChecker::is_curr_or_next_val(q_name.col_name_)) {
    LOG_DEBUG("sequence object", K(q_name));
    if (OB_FAIL(sequence_namespace_checker_.check_sequence_namespace(q_name, syn_checker, sequence_id))) {
      LOG_WARN_IGNORE_COL_NOTFOUND(ret, "check basic column namespace failed", K(ret), K(q_name));
    } else if (OB_ISNULL(stmt = get_stmt())) {
      ret = OB_NOT_INIT;
      LOG_WARN("stmt is null");
    } else if (OB_UNLIKELY(T_FIELD_LIST_SCOPE != current_scope_ && T_UPDATE_SCOPE != current_scope_ &&
                           T_INSERT_SCOPE != current_scope_)) {
      // sequence can only appears in following three scenes:
      //  - select seq from ...
      //  - insert into t1 values (seq...
      //  - update t1 set c1 = seq xxxx
      // can't appear in where, group by, limit or having clause.
      ret = OB_ERR_SEQ_NOT_ALLOWED_HERE;
    } else if (OB_FAIL(build_seq_nextval_expr(column_expr, q_name, sequence_id))) {
      LOG_WARN("resolve column item failed", K(ret));
    } else {
      real_ref_expr = column_expr;
      // record sequence id in plan.
      if (0 == q_name.col_name_.case_compare("NEXTVAL")) {
        if (OB_FAIL(add_sequence_id_to_stmt(sequence_id))) {
          LOG_WARN("fail add id to stmt", K(sequence_id), K(ret));
        }
      } else if (0 == q_name.col_name_.case_compare("CURRVAL")) {
        if (OB_FAIL(add_sequence_id_to_stmt(sequence_id, true))) {
          LOG_WARN("fail add id to stmt", K(sequence_id), K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (syn_checker.has_synonym()) {
        // add synonym depedency schemas
        bool error_with_exist = false;
        if (OB_FAIL(stmt->add_synonym_ids(syn_checker.get_synonym_ids(), error_with_exist))) {
          LOG_WARN("failed to add synonym ids", K(ret));
        } else if (OB_FAIL(add_object_version_to_dependency(
                       DEPENDENCY_SYNONYM, SYNONYM_SCHEMA, stmt->get_synonym_id_store()))) {
          LOG_WARN("add synonym version failed", K(ret));
        } else {
          // do nothing
        }
      } else {
        // do nothing
      }
    }
  } else {
    ret = OB_ERR_BAD_FIELD_ERROR;
  }
  return ret;
}

int ObDMLResolver::add_sequence_id_to_stmt(uint64_t sequence_id, bool is_currval)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(stmt), K(ret));
  } else {
    bool exist = false;
    const ObIArray<uint64_t>& ids = is_currval ? stmt->get_currval_sequence_ids() : stmt->get_nextval_sequence_ids();

    FOREACH_CNT_X(id, ids, !exist)
    {
      if (*id == sequence_id) {
        exist = true;
      }
    }
    if (!exist) {
      if (is_currval) {
        if (OB_FAIL(stmt->add_currval_sequence_id(sequence_id))) {
          LOG_WARN("failed to push back sequence id", K(ret));
        } else {
          // do nothing
        }
      } else if (OB_FAIL(stmt->add_nextval_sequence_id(sequence_id))) {
        LOG_WARN("fail push back sequence id", K(sequence_id), K(ids), K(ret));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObDMLResolver::view_pullup_column_ref_exprs_recursively(
    ObRawExpr*& expr, uint64_t base_table_id, const ObDMLStmt* stmt)
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
  } else if (expr->is_column_ref_expr()) {
    ObColumnRefRawExpr* ref_expr = static_cast<ObColumnRefRawExpr*>(expr);
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < stmt->get_column_size(); i++) {
      const ColumnItem* column_item = stmt->get_column_item(i);
      if (OB_ISNULL(column_item) || OB_ISNULL(column_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr null");
      } else if (base_table_id == column_item->base_tid_ && ref_expr->get_column_id() == column_item->base_cid_) {
        expr = column_item->expr_;
        found = true;
      }
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      ObRawExpr*& t_expr = expr->get_param_expr(i);
      if (OB_FAIL(SMART_CALL(view_pullup_column_ref_exprs_recursively(t_expr, base_table_id, stmt)))) {
        LOG_WARN("generated column expr pull up failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::add_check_constraint_to_stmt(const share::schema::ObTableSchema* table_schema)
{
  int ret = OB_SUCCESS;
  const ParseNode* node = NULL;
  ObDMLStmt* dml_stmt = static_cast<ObDMLStmt*>(stmt_);

  if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_ISNULL(params_.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is NULL", K(ret));
  } else {
    for (ObTableSchema::const_constraint_iterator iter = table_schema->constraint_begin();
         OB_SUCC(ret) && iter != table_schema->constraint_end();
         iter++) {
      ObRawExpr* check_constraint_expr = NULL;
      ObConstraint tmp_constraint;
      ObSEArray<ObRawExpr*, 8> tmp_check_constraint_exprs;
      if ((*iter)->get_constraint_type() != CONSTRAINT_TYPE_CHECK) {
        continue;
      } else if (!(*iter)->get_enable_flag() && (*iter)->get_validate_flag()) {
        const ObSimpleDatabaseSchema* database_schema = NULL;
        if (OB_ISNULL(params_.schema_checker_->get_schema_guard())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema guard ptr is null ptr", K(ret), KP(params_.schema_checker_));
        } else if (OB_FAIL(params_.schema_checker_->get_schema_guard()->get_database_schema(
                       table_schema->get_database_id(), database_schema))) {
          LOG_WARN("get_database_schema failed", K(ret), K(table_schema->get_database_id()));
        } else if (OB_ISNULL(database_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("database_schema is null", K(ret));
        } else {
          const ObString& origin_database_name = database_schema->get_database_name_str();
          if (origin_database_name.empty() || (*iter)->get_constraint_name_str().empty()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("database name or cst name is null",
                K(ret),
                K(origin_database_name),
                K((*iter)->get_check_expr_str().empty()));
          } else {
            ret = OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE;
            LOG_USER_ERROR(OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE,
                origin_database_name.length(),
                origin_database_name.ptr(),
                (*iter)->get_constraint_name_str().length(),
                (*iter)->get_constraint_name_str().ptr());
            LOG_WARN("no insert on table with constraint disabled and validated", K(ret));
          }
        }
      } else if (!(*iter)->get_enable_flag() && !(*iter)->get_validate_flag()) {
        continue;
      } else if (OB_FAIL(ObRawExprUtils::parse_bool_expr_node_from_str(
                     (*iter)->get_check_expr_str(), *(params_.allocator_), node))) {
        LOG_WARN("parse expr node from string failed", K(ret));
      } else if (OB_FAIL(ObResolverUtils::resolve_check_constraint_expr(
                     params_, node, *table_schema, tmp_constraint, check_constraint_expr))) {
        LOG_WARN("resolve check constraint expr failed", K(ret));
      } else if (OB_FAIL(tmp_check_constraint_exprs.push_back(check_constraint_expr))) {
        LOG_WARN("array push back failed", K(ret));
      } else if (OB_FAIL(deduce_generated_exprs(tmp_check_constraint_exprs))) {
        LOG_WARN("deduce generated exprs failed", K(ret));
      } else if (OB_FAIL(dml_stmt->add_value_to_check_constraint_exprs(check_constraint_expr))) {
        LOG_WARN("add value to check_constraint_exprs failed", K(ret));
      }
    }
  }

  return ret;
}

int ObDMLResolver::resolve_check_constraints(const TableItem* table_item)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* dml_stmt = NULL;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(table_item) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_ISNULL(dml_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get stmt null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(table_item->get_base_table_item().ref_id_, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(table_item->get_base_table_item().ref_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get tale schema", K(ret), K(table_schema));
  } else if (OB_FAIL(add_check_constraint_to_stmt(table_schema))) {
    LOG_WARN("fail to add check constraint to stmt", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dml_stmt->get_check_constraint_exprs_size(); i++) {
      ObRawExpr*& expr = dml_stmt->get_check_constraint_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("check constraint expr is null", K(ret));
      } else if (OB_FAIL(view_pullup_column_ref_exprs_recursively(
                     expr, table_item->get_base_table_item().ref_id_, dml_stmt))) {
        LOG_WARN("view pullup column_ref_exprs recursively failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDMLResolver::view_pullup_generated_column_exprs()
{
  int ret = OB_SUCCESS;
  ObDMLStmt* dml_stmt = get_stmt();
  if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(dml_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param factory or dml stmt is null", K(ret), K(params_.expr_factory_), K(dml_stmt));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dml_stmt->get_table_size(); i++) {
    ObSelectStmt* sel_stmt = NULL;
    const TableItem* t = NULL;
    if (OB_ISNULL(t = dml_stmt->get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(i));
    } else if (t->is_generated_table() && OB_NOT_NULL(t->view_base_item_)) {
      while (NULL != t && t->is_generated_table()) {
        sel_stmt = t->ref_query_;
        t = t->view_base_item_;
      }
      if (OB_ISNULL(sel_stmt) || OB_ISNULL(t)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref_query_ is null", K(ret));
      }
      for (int j = 0; OB_SUCC(ret) && j < dml_stmt->get_column_size(); j++) {
        ColumnItem* view_column_item = dml_stmt->get_column_item(j);
        if (OB_ISNULL(view_column_item) || OB_ISNULL(view_column_item->expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("view column item not exists in stmt or expr_ is null", K(ret), K(view_column_item));
        } else if (view_column_item->table_id_ == dml_stmt->get_table_item(i)->table_id_) {
          ColumnItem* basic_column_item = sel_stmt->get_column_item_by_id(t->table_id_, view_column_item->base_cid_);
          if (OB_NOT_NULL(basic_column_item)) {
            if (OB_ISNULL(basic_column_item->expr_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("basic column item  expr_ is null", K(ret));
            } else if (basic_column_item->expr_->is_generated_column()) {
              ObRawExpr* ref_expr = NULL;
              if (OB_FAIL(ObRawExprUtils::copy_expr(*(params_.expr_factory_),
                      basic_column_item->expr_->get_dependant_expr(),
                      ref_expr,
                      COPY_REF_DEFAULT))) {
                LOG_WARN("copy expr failed", K(ret), K(basic_column_item->expr_->get_dependant_expr()));
              } else if (OB_FAIL(view_pullup_column_ref_exprs_recursively(ref_expr, t->ref_id_, dml_stmt))) {
                LOG_WARN("view pull up generated column exprs recursively failed", K(ret));
              } else {
                view_column_item->expr_->set_dependant_expr(ref_expr);
                view_column_item->expr_->set_column_flags(basic_column_item->expr_->get_column_flags());
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::view_pullup_part_exprs()
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  }

  TableItem* table = NULL;
  for (int64_t idx = 0; idx < stmt->get_table_size(); idx++) {
    if (OB_ISNULL(table = stmt->get_table_item(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is NULL", K(ret));
    } else if (table->is_generated_table()) {
      ObSelectStmt* sel_stmt = NULL;
      const TableItem* t = table;
      while (NULL != t && t->is_generated_table()) {
        sel_stmt = t->ref_query_;
        t = t->view_base_item_;
      }
      if (NULL == sel_stmt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get select stmt for base table item failed", K(ret));
      } else {
        // pullup partition expr
        FOREACH_CNT_X(pei, sel_stmt->get_part_exprs(), OB_SUCC(ret))
        {
          if (pei->table_id_ != table->get_base_table_item().table_id_) {
            continue;
          }
          ObDMLStmt::PartExprItem new_it = *pei;
          ObRawExpr** to_exprs[] = {&new_it.part_expr_, &new_it.subpart_expr_};
          ObRawExpr* from_exprs[] = {pei->part_expr_, pei->subpart_expr_};
          for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(to_exprs); i++) {
            ObRawExpr*& to = *to_exprs[i];
            ObRawExpr* from = from_exprs[i];
            if (NULL == from) {
              continue;
            }
            ObSEArray<ObRawExpr*, 4> col_exprs;
            if (OB_FAIL(ObRawExprUtils::copy_expr(*params_.expr_factory_, from, to, COPY_REF_DEFAULT))) {
              LOG_WARN("copy expr failed");
            } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(to, col_exprs))) {
              LOG_WARN("extract column exprs failed", K(ret));
            } else {
              FOREACH_CNT_X(expr, col_exprs, OB_SUCC(ret))
              {
                ColumnItem* col = NULL;
                if (NULL == *expr ||
                    OB_ISNULL(col = find_col_by_base_col_id(
                                  *stmt, table->table_id_, static_cast<ObColumnRefRawExpr*>(*expr)->get_column_id()))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("NULL expr or ColumnItem not found", K(ret), K(*expr), KP(col));
                } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(to, *expr, col->expr_))) {
                  LOG_WARN("replace column ref failed", K(ret));
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(stmt->get_part_exprs().push_back(new_it))) {
              LOG_WARN("push back part expr item failed", K(ret));
            }
          }
        }
      }

      // pullup partition hint
      if (OB_SUCC(ret)) {
        const ObPartHint* part_hint = sel_stmt->get_stmt_hint().get_part_hint(table->get_base_table_item().table_id_);
        if (NULL != part_hint) {
          ObPartHint new_part_hint;
          new_part_hint.table_id_ = table->table_id_;
          if (OB_FAIL(new_part_hint.part_ids_.assign(part_hint->part_ids_))) {
            LOG_WARN("array assign failed", K(ret));
          } else if (OB_FAIL(new_part_hint.part_names_.assign(part_hint->part_names_))) {
            LOG_WARN("failed to assign part names", K(ret));
          } else if (OB_FAIL(stmt->get_stmt_hint().part_hints_.push_back(new_part_hint))) {
            LOG_WARN("push back part hint failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::add_cte_table_to_children(ObChildStmtResolver& child_resolver)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(stmt));
  } else {
    const ObIArray<TableItem*>& CTE_tables = stmt->get_CTE_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < CTE_tables.count(); i++) {
      if (OB_FAIL(child_resolver.add_cte_table_item(CTE_tables.at(i)))) {
        LOG_WARN("add cte table to children failed");
      } else {
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_index_rowkey_exprs(uint64_t table_id, const ObTableSchema& index_schema,
    ObIArray<ObColumnRefRawExpr*>& column_exprs, bool use_shared_spk /*= true*/)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* dml_stmt = get_stmt();
  if (OB_ISNULL(dml_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid insert stmt", K(dml_stmt));
  } else {
    uint64_t rowkey_column_id = 0;
    const ObRowkeyInfo& rowkey_info = index_schema.get_rowkey_info();
    ColumnItem* col = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      ObColumnRefRawExpr* col_expr = NULL;
      if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
        LOG_WARN("get rowkey column id failed", K(ret));
      } else if (OB_ISNULL(col = find_col_by_base_col_id(*dml_stmt, table_id, rowkey_column_id))) {
        ObDelUpdStmt* del_upd_stmt = NULL;
        if (dml_stmt->is_dml_write_stmt()) {
          del_upd_stmt = static_cast<ObDelUpdStmt*>(dml_stmt);
        }
        if (!is_shadow_column(rowkey_column_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column expr by id failed", K(table_id), K(rowkey_column_id));
        } else if ((NULL != del_upd_stmt) && use_shared_spk &&
                   OB_FAIL(del_upd_stmt->find_index_column(
                       table_id, index_schema.get_table_id(), rowkey_column_id, col_expr))) {
          LOG_WARN("fail to find shadow pk column", K(ret));
        } else if ((NULL == col_expr) &&
                   OB_FAIL(resolve_shadow_pk_expr(table_id, rowkey_column_id, index_schema, col_expr))) {
          LOG_WARN("resolve shadow primary key expr failed", K(ret), K(table_id));
        }
      } else {
        col_expr = col->expr_;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(column_exprs.push_back(col_expr))) {
          LOG_WARN("store column expr to column exprs failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_index_all_column_exprs(
    uint64_t table_id, const ObTableSchema& index_schema, ObIArray<ObColumnRefRawExpr*>& column_exprs)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* dml_stmt = get_stmt();
  if (OB_ISNULL(dml_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid insert stmt", K(dml_stmt));
  } else if (OB_FAIL(resolve_index_rowkey_exprs(table_id, index_schema, column_exprs))) {
    // 1. Firstly, add all rowkey columns
    LOG_WARN("resolve index rowkey exprs failed", K(ret), K(table_id));
  } else {
    // 2. Then, all other columns
    ObColumnRefRawExpr* col_expr = NULL;
    ColumnItem* col_item = NULL;
    ObTableSchema::const_column_iterator iter = index_schema.column_begin();
    ObTableSchema::const_column_iterator end = index_schema.column_end();
    for (; OB_SUCC(ret) && iter != end; ++iter) {
      const ObColumnSchemaV2* column = *iter;
      // skip all rowkeys
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column schema", K(column));
      } else if (!column->is_rowkey_column()) {
        if (OB_ISNULL(col_item = find_col_by_base_col_id(*dml_stmt, table_id, column->get_column_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column item by id failed", K(ret), K(table_id), K(column->get_column_id()));
        } else if (OB_ISNULL(col_expr = col_item->expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr_ of column item is null", K(ret), K(*col_item));
        } else if (OB_FAIL(column_exprs.push_back(col_expr))) {
          LOG_WARN("store column expr to column exprs failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::fill_index_column_convert_exprs(const ObAssignments& assignments,
    ObIArray<ObColumnRefRawExpr*>& column_exprs, ObIArray<ObRawExpr*>& column_convert_exprs)
{
  int ret = OB_SUCCESS;
  int found = 0;
  column_convert_exprs.reset();
  for (int i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
    ObRawExpr* insert_expr = column_exprs.at(i);
    // find_replacement_in_assignment
    for (int j = 0; OB_SUCC(ret) && j < assignments.count(); ++j) {
      if (insert_expr == assignments.at(j).column_expr_) {
        insert_expr = const_cast<ObRawExpr*>(assignments.at(j).expr_);
        found++;
        break;
      }
    }
    if (OB_ISNULL(insert_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else if (OB_FAIL(column_convert_exprs.push_back(insert_expr))) {
      LOG_WARN("fail push back data", K(ret));
    }
  }
  if (OB_SUCC(ret) && found != assignments.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not all update asssigment found in insert target exprs",
        K(ret),
        K(found),
        K(assignments.count()),
        K(assignments));
  }
  return ret;
}

int ObDMLResolver::resolve_index_related_column_exprs(uint64_t table_id, const ObTableSchema& index_schema,
    const ObAssignments& assignments, ObIArray<ObColumnRefRawExpr*>& column_exprs)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* dml_stmt = get_stmt();
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  if (OB_ISNULL(dml_stmt) || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dml_stmt is null", K(dml_stmt), K_(session_info));
  } else if (OB_FAIL(session_info_->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    uint64_t rowkey_column_id = 0;
    // 1. Firstly, add all rowkey columns
    const ObRowkeyInfo& rowkey_info = index_schema.get_rowkey_info();
    ObColumnRefRawExpr* col_expr = NULL;
    const ColumnItem* col_item = NULL;
    bool is_modify_rowkey = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
        LOG_WARN("get rowkey column id failed", K(ret));
      } else if (OB_ISNULL(col_item = find_col_by_base_col_id(*dml_stmt, table_id, rowkey_column_id))) {
        ObDelUpdStmt* del_upd_stmt = NULL;
        if (dml_stmt->is_dml_write_stmt()) {
          del_upd_stmt = static_cast<ObDelUpdStmt*>(dml_stmt);
        }
        if (!is_shadow_column(rowkey_column_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column expr by id failed", K(table_id), K(rowkey_column_id));
        } else if ((NULL != del_upd_stmt) && OB_FAIL(del_upd_stmt->find_index_column(
                                                 table_id, index_schema.get_table_id(), rowkey_column_id, col_expr))) {
          LOG_WARN("fail to find shadow pk column", K(ret));
        } else if ((NULL == col_expr) &&
                   OB_FAIL(resolve_shadow_pk_expr(table_id, rowkey_column_id, index_schema, col_expr))) {
          LOG_WARN("resolve shadow primary key expr failed", K(ret), K(table_id), K(rowkey_column_id));
        }
      } else if (OB_ISNULL(col_expr = col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr of column item is null", K(ret), K(*col_item));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(column_exprs.push_back(col_expr))) {
          LOG_WARN("store column expr to column exprs failed", K(ret));
        } else if (!is_modify_rowkey) {
          if (OB_FAIL(check_whether_assigned(assignments, table_id, rowkey_column_id, is_modify_rowkey))) {
            LOG_WARN("check whether assigned rowkey failed", K(ret), K(table_id), K(rowkey_column_id), K(assignments));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_modify_rowkey || ObBinlogRowImage::FULL == binlog_row_image) {
        // modify rowkey, need add all columns in schema
        ObTableSchema::const_column_iterator iter = index_schema.column_begin();
        ObTableSchema::const_column_iterator end = index_schema.column_end();
        for (; OB_SUCC(ret) && iter != end; ++iter) {
          const ObColumnSchemaV2* column = *iter;
          // skip all rowkeys
          if (OB_ISNULL(column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid column schema", K(column));
          } else if (!column->is_rowkey_column()) {
            if (OB_ISNULL(col_item = find_col_by_base_col_id(*dml_stmt, table_id, column->get_column_id()))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get column item by id failed", K(ret), K(table_id), K(column->get_column_id()));
            } else if (OB_ISNULL(col_expr = col_item->expr_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr of column item is null", K(ret), K(*col_item));
            } else if (OB_FAIL(column_exprs.push_back(col_expr))) {
              LOG_WARN("store column expr to column exprs failed", K(ret));
            }
          }
        }
      } else {
        // not modify rowkey, only add modified columns and rowkey of primary table
        for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
          col_expr = const_cast<ObColumnRefRawExpr*>(assignments.at(i).column_expr_);
          if (OB_ISNULL(col_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("assignment column expr is null");
          } else if (OB_ISNULL(index_schema.get_column_schema(col_expr->get_column_id()))) {
          } else if (OB_FAIL(column_exprs.push_back(col_expr))) {
            LOG_WARN("store column expr to column exprs failed", K(ret));
          }
        }
        int64_t column_count = column_exprs.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
          ObColumnRefRawExpr* cur_expr = column_exprs.at(i);
          if (OB_ISNULL(cur_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null expr", K(ret));
          } else if (is_shadow_column(cur_expr->get_column_id())) {
            uint64_t ref_rowkey_id = cur_expr->get_column_id() - OB_MIN_SHADOW_COLUMN_ID;
            if (OB_ISNULL(col_item = find_col_by_base_col_id(*dml_stmt, table_id, ref_rowkey_id))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get column item by id failed", K(ret), K(table_id), K(ref_rowkey_id));
            } else if (OB_ISNULL(col_expr = col_item->expr_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr of column item is null", K(ret), K(*col_item));
            } else if (OB_FAIL(add_var_to_array_no_dup(column_exprs, col_expr))) {
              LOG_WARN("failed to add var to array no dup", K(ret));
            }
          } else { /* do nothing */
          }
        }
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_shadow_pk_expr(
    uint64_t table_id, uint64_t column_id, const ObTableSchema& index_schema, ObColumnRefRawExpr*& spk_expr)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2* spk_schema = NULL;
  ObDMLStmt* stmt = get_stmt();
  const ObRowkeyInfo& rowkey_info = index_schema.get_rowkey_info();
  ObOpRawExpr* spk_project_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("dml resolver not init", K(stmt), K_(params_.expr_factory));
  } else if (OB_UNLIKELY(!is_shadow_column(column_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column_id is invalid", K(column_id));
  } else if (OB_ISNULL(spk_schema = index_schema.get_column_schema(column_id))) {
    ret = OB_ERR_COLUMN_NOT_FOUND;
    LOG_WARN("column not found", K(column_id), K(index_schema));
  } else {
    ObString index_name;
    if (OB_FAIL(ObRawExprUtils::build_column_expr(*params_.expr_factory_, *spk_schema, spk_expr))) {
      LOG_WARN("create column ref raw expr failed", K(ret));
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_OP_SHADOW_UK_PROJECT, spk_project_expr))) {
      LOG_WARN("create shadow unique key projector failed", K(ret));
    } else if (OB_FAIL(index_schema.get_index_name(index_name))) {
      LOG_WARN("get index name from index schema failed", K(ret));
    } else {
      spk_expr->set_table_name(index_name);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
    uint64_t rowkey_column_id = OB_INVALID_ID;
    if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
      LOG_WARN("get rowkey column id failed", K(ret));
    } else if (!is_shadow_column(rowkey_column_id)) {
      const ColumnItem* col = find_col_by_base_col_id(*stmt, table_id, rowkey_column_id);
      if (OB_FAIL(spk_project_expr->add_param_expr(col->expr_))) {
        LOG_WARN("add param expr to shadow unique key project expr failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t real_column_id = column_id - OB_MIN_SHADOW_COLUMN_ID;
    const ColumnItem* col = find_col_by_base_col_id(*stmt, table_id, real_column_id);
    if (OB_FAIL(spk_project_expr->add_param_expr(col->expr_))) {
      LOG_WARN("add param expr to spk project expr failed", K(ret));
    } else if (OB_FAIL(spk_project_expr->formalize(session_info_))) {
      LOG_WARN("formalize shadow unique key failed", K(ret));
    } else {
      spk_expr->set_dependant_expr(spk_project_expr);
      spk_expr->set_column_flags(VIRTUAL_GENERATED_COLUMN_FLAG);
    }
  }
  return ret;
}

int ObDMLResolver::resolve_column_and_values(
    const ParseNode& assign_list, ObIArray<ObColumnRefRawExpr*>& target_list, ObIArray<ObRawExpr*>& value_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(T_ASSIGN_LIST != assign_list.type_ || assign_list.num_child_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resolver invalid status", K(ret), K(assign_list.type_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assign_list.num_child_; ++i) {
    ParseNode* left_node = NULL;
    ParseNode* right_node = NULL;
    ObRawExpr* value_expr = NULL;
    ObSEArray<ObColumnRefRawExpr*, 4> columns;
    ObSEArray<ObRawExpr*, 4> values;
    if (OB_ISNULL(assign_list.children_[i]) || OB_ISNULL(left_node = assign_list.children_[i]->children_[0]) ||
        OB_ISNULL(right_node = assign_list.children_[i]->children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("assign node is empty", K(ret), K(assign_list.children_[i]), K(left_node), K(right_node));
    } else if (OB_FAIL(resolve_assign_columns(*left_node, columns))) {
      LOG_WARN("failed to resolve assign target", K(ret));
    } else if (OB_FAIL(resolve_sql_expr(*right_node, value_expr))) {
      LOG_WARN("failed to resolve value expr", K(ret));
    } else if (left_node->type_ != T_COLUMN_LIST) {
      // do nothing
    } else if (OB_UNLIKELY(!value_expr->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid stmt or value expr", K(ret));
    } else if (!get_stmt()->is_update_stmt()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only update stmt support vector assignment clause", K(ret));
    } else if (assign_list.num_child_ != 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only support one vector assignment clause", K(ret));
    } else {
      ObQueryRefRawExpr* query_ref = static_cast<ObQueryRefRawExpr*>(value_expr);
      ObSelectStmt* sel_stmt = NULL;
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
        static_cast<ObUpdateStmt*>(get_stmt())->set_update_set(true);
        for (int64_t j = 0; OB_SUCC(ret) && j < columns.count(); ++j) {
          ObAliasRefRawExpr* alias_expr = NULL;
          if (OB_FAIL(ObRawExprUtils::build_query_output_ref(*params_.expr_factory_, query_ref, j, alias_expr))) {
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
  return ret;
}

int ObDMLResolver::resolve_assign_columns(const ParseNode& assign_target, ObIArray<ObColumnRefRawExpr*>& column_list)
{
  int ret = OB_SUCCESS;
  bool is_column_list = false;
  int64_t column_count = 1;
  bool is_merge_resolver = false;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (assign_target.type_ != T_COLUMN_LIST && assign_target.type_ != T_COLUMN_REF) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node", K(ret));
  } else if (assign_target.type_ == T_COLUMN_LIST) {
    column_count = assign_target.num_child_;
    is_column_list = true;
  } else {
    is_merge_resolver = get_stmt()->is_merge_stmt();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
    const ParseNode* col_node = (is_column_list ? assign_target.children_[i] : &assign_target);
    ObQualifiedName q_name;
    ObRawExpr* col_expr = NULL;
    ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
    if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
      LOG_WARN("resolve column reference name failed", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_column_ref(col_node, case_mode, q_name))) {
      LOG_WARN("resolve column reference name failed", K(ret));
    } else if (q_name.is_star_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("'*' should not be here, parser has already blocked this error", K(ret));
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
    } else if (OB_UNLIKELY(
                   OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == static_cast<ObColumnRefRawExpr*>(col_expr)->get_column_id())) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      report_user_error_msg(ret, col_expr, q_name);
      LOG_WARN("resolve ref expr failed", K(ret), K(q_name));
    } else if (!col_expr->is_column_ref_expr()) {
      ret = OB_ERR_NON_UPDATABLE_TABLE;
      LOG_WARN("update column should from updatable table", K(col_expr), K(ret));
    } else if (OB_FAIL(column_list.push_back(static_cast<ObColumnRefRawExpr*>(col_expr)))) {
      LOG_WARN("failed to push back column expr", K(ret));
    }
  }
  return ret;
}

/**
 * @bref check legality of join condition of oracle outer join.
 * 1. check scope
 * 2. check number of predicates: in, or.
 */
int ObDMLResolver::check_oracle_outer_join_condition(const ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  ;
  CK(OB_NOT_NULL(expr));
  if (OB_SUCC(ret) && (expr->has_flag(CNT_OUTER_JOIN_SYMBOL))) {
    if (OB_UNLIKELY(expr->has_flag(CNT_IN) || expr->has_flag(CNT_OR))) {
      /**
       * ORA-01719:
       * 01719. 00000 -  "outer join operator (+) not allowed in operand of OR or IN"
       * *Cause:    An outer join appears in an or clause.
       * *Action:   If A and B are predicates, to get the effect of (A(+) or B),
       *            try (select where (A(+) and not B)) union all (select where (B)).
       * ----
       * error: a(+) = b or c = d
       * error: a(+) in (1, 2)
       * OK: a(+) in (1) [IN is T_OP_EQ in here]
       */
      ret = OB_ERR_OUTER_JOIN_AMBIGUOUS;
      LOG_WARN("outer join operator (+) not allowed in operand of OR or IN", K(ret));
    } else if (expr->has_flag(CNT_SUB_QUERY)) {
      /**
       * ORA-01799:
       * 01799. 00000 -  "a column may not be outer-joined to a subquery"
       * *Cause:    <expression>(+) <relop> (<subquery>) is not allowed.
       * *Action:   Either remove the (+) or make a view out of the subquery.
       *            In V6 and before, the (+) was just ignored in this case.
       * ----
       * error: a(+) = (select * from t1);
       */
      ret = OB_ERR_OUTER_JOIN_WITH_SUBQUERY;
      LOG_WARN("column may not be outer-joined to a subquery");
    } else if (has_ansi_join()) {
      /**
       * ORA-25156:
       * 25156. 00000 -  "old style outer join (+) cannot be used with ANSI joins"
       * *Cause:    When a query block uses ANSI style joins, the old notation
       *            for specifying outer joins (+) cannot be used.
       * *Action:   Use ANSI style for specifying outer joins also.
       * ----
       * error: select * from t1 left join t2 on t1.c1 = t2.c1 where t1.c1 = t2.c1(+)
       */
      ret = OB_ERR_OUTER_JOIN_WITH_ANSI_JOIN;
      LOG_WARN("old style outer join (+) cannot be used with ANSI joins");
    }
  }
  return ret;
}

/**
 * @bref remove T_OP_ORACLE_OUTER_JOIN_SYMBOL in exprs.
 * Method is remove all nodes with IS_OUTER_JOIN_SYMBOL flag.
 */
int ObDMLResolver::remove_outer_join_symbol(ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(expr));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!expr->has_flag(CNT_OUTER_JOIN_SYMBOL)) {
    // do nothing
  } else if (expr->has_flag(IS_OUTER_JOIN_SYMBOL)) {
    CK(expr->get_param_count() == 1, OB_NOT_NULL(expr->get_param_expr(0)));
    if (OB_SUCC(ret)) {
      expr = expr->get_param_expr(0);
      OZ(SMART_CALL(remove_outer_join_symbol(expr)));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      OZ(SMART_CALL(remove_outer_join_symbol(expr->get_param_expr(i))));
    }
  }
  return ret;
}

int ObDMLResolver::resolve_outer_join_symbol(const ObStmtScope scope, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(expr));
  if (OB_SUCC(ret) && (expr->has_flag(CNT_OUTER_JOIN_SYMBOL))) {
    if (OB_UNLIKELY(T_FIELD_LIST_SCOPE == scope || T_CONNECT_BY_SCOPE == scope || T_START_WITH_SCOPE == scope ||
                    T_ORDER_SCOPE == scope)) {
      /*
       * ORA-30563:
       * 30563. 00000 -  "outer join operator (+) is not allowed here"
       * *Cause:    An attempt was made to reference (+) in either the select-list,
       *            CONNECT BY clause, START WITH clause, or ORDER BY clause.
       * *Action:   Do not use the operator in the select-list, CONNECT
       *            BY clause, START WITH clause, or ORDER BY clause.
       */
      ret = OB_ERR_OUTER_JOIN_NOT_ALLOWED;
      LOG_WARN("outer join operator (+) is not allowed here", K(ret));
    } else if (T_WHERE_SCOPE != current_scope_) {
      OZ(remove_outer_join_symbol(expr));
    } else {
      set_has_oracle_join(true);
    }
  }
  return ret;
}

int ObDMLResolver::generate_outer_join_tables()
{
  int ret = OB_SUCCESS;
  if (has_oracle_join()) {
    ObDMLStmt* stmt = get_stmt();
    CK(OB_NOT_NULL(stmt));

    if (OB_SUCC(ret)) {
      ObArray<ObBitSet<>> table_dependencies;
      OZ(generate_outer_join_dependency(stmt->get_table_items(), stmt->get_condition_exprs(), table_dependencies));
      OZ(build_outer_join_table_by_dependency(table_dependencies, *stmt));
      // remove predicate
      ObArray<JoinedTable*> joined_tables;
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); i++) {
        const FromItem& from_item = stmt->get_from_item(i);
        if (from_item.is_joined_) {
          OZ((joined_tables.push_back)(stmt->get_joined_table(from_item.table_id_)));
        }
      }

      OZ(deliver_outer_join_conditions(stmt->get_condition_exprs(), joined_tables));
    }
  }
  return ret;
}

int ObDMLResolver::generate_outer_join_dependency(const ObIArray<TableItem*>& table_items,
    const ObIArray<ObRawExpr*>& exprs, ObIArray<ObBitSet<>>& table_dependencies)
{
  int ret = OB_SUCCESS;
  table_dependencies.reset();
  ObArray<uint64_t> all_table_ids;
  // init param
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); i++) {
    OZ((table_dependencies.push_back)(ObBitSet<>()));
    CK(OB_NOT_NULL(table_items.at(i)));
    OZ((all_table_ids.push_back)(table_items.at(i)->table_id_));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    const ObRawExpr* expr = exprs.at(i);
    ObArray<uint64_t> left_tables;
    ObArray<uint64_t> right_tables;
    CK(OB_NOT_NULL(expr));
    OZ(check_oracle_outer_join_condition(expr));
    OZ(extract_column_with_outer_join_symbol(expr, left_tables, right_tables));

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_UNLIKELY(right_tables.count() > 1)) {
      /**
       * ORA-01468:
       * 01468. 00000 -  "a predicate may reference only one outer-joined table"
       * *Cause:
       * *Action:
       */
      ret = OB_ERR_MULTI_OUTER_JOIN_TABLE;
      LOG_WARN("a predicate may reference only one outer-joined table", K(ret));
    } else if (1 == right_tables.count() && 0 != left_tables.count()) {
      OZ(add_oracle_outer_join_dependency(all_table_ids, left_tables, right_tables.at(0), table_dependencies));
    }
  }
  return ret;
}

int ObDMLResolver::extract_column_with_outer_join_symbol(
    const ObRawExpr* expr, ObIArray<uint64_t>& left_tables, ObIArray<uint64_t>& right_tables)
{
  int ret = OB_SUCCESS;
  bool is_right = false;
  CK(OB_NOT_NULL(expr));
  if (OB_SUCC(ret)) {
    if (expr->has_flag(IS_OUTER_JOIN_SYMBOL)) {
      CK(1 == expr->get_param_count(), OB_NOT_NULL(expr->get_param_expr(0)));
      if (OB_SUCC(ret)) {
        expr = expr->get_param_expr(0);
        is_right = true;
      }
    }

    if (expr->has_flag(IS_COLUMN)) {
      const ObColumnRefRawExpr* col_expr = static_cast<const ObColumnRefRawExpr*>(expr);
      if (!is_right) {
        OZ((common::add_var_to_array_no_dup)(left_tables, col_expr->get_table_id()));
      } else {
        OZ((common::add_var_to_array_no_dup)(right_tables, col_expr->get_table_id()));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
        CK(OB_NOT_NULL(expr->get_param_expr(i)));
        OZ(SMART_CALL(extract_column_with_outer_join_symbol(expr->get_param_expr(i), left_tables, right_tables)));
      }
    }
  }
  return ret;
}

int ObDMLResolver::add_oracle_outer_join_dependency(const ObIArray<uint64_t>& all_tables,
    const ObIArray<uint64_t>& left_tables, uint64_t right_table_id, ObIArray<ObBitSet<>>& table_dependencies)
{
  int ret = OB_SUCCESS;
  int64_t right_idx = OB_INVALID_INDEX_INT64;
  CK(table_dependencies.count() == all_tables.count());

  if (OB_SUCC(ret) && OB_UNLIKELY(!has_exist_in_array(all_tables, right_table_id, &right_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Cannot find right table", K(ret), K(all_tables), K(right_table_id));
  }
  CK(0 <= right_idx, right_idx < all_tables.count());

  for (int64_t i = 0; OB_SUCC(ret) && i < left_tables.count(); i++) {
    int64_t left_idx = OB_INVALID_INDEX_INT64;
    if (OB_UNLIKELY(!has_exist_in_array(all_tables, left_tables.at(i), &left_idx))) {
    } else {
      CK(0 <= left_idx, left_idx < all_tables.count());
      table_dependencies.at(right_idx).add_member(left_idx);
    }
  }
  return ret;
}

int ObDMLResolver::build_outer_join_table_by_dependency(const ObIArray<ObBitSet<>>& table_dependencies, ObDMLStmt& stmt)
{
  int ret = OB_SUCCESS;

  ObBitSet<> built_tables;
  TableItem* last_table_item = NULL;
  bool is_found = true;

  CK(table_dependencies.count() == stmt.get_table_items().count());

  // strategy: smallest-numbered available vertex first
  for (int64_t cnt = 0; OB_SUCC(ret) && is_found && cnt < table_dependencies.count(); cnt++) {
    is_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < table_dependencies.count(); i++) {
      if (!built_tables.has_member(i) && built_tables.is_superset(table_dependencies.at(i))) {
        is_found = true;
        if (NULL == last_table_item) {
          last_table_item = stmt.get_table_item(i);
        } else {
          JoinedTable* new_joined_table = NULL;
          ObJoinType joined_type =
              table_dependencies.at(i).is_empty() ? ObJoinType::INNER_JOIN : ObJoinType::LEFT_OUTER_JOIN;

          OZ(create_joined_table_item(joined_type, last_table_item, stmt.get_table_item(i), new_joined_table));

          last_table_item = static_cast<TableItem*>(new_joined_table);
        }
        // table is built
        OZ((built_tables.add_member)(i));
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!is_found)) {
      ret = OB_ERR_OUTER_JOIN_NESTED;
      LOG_WARN("two tables cannot be outer-joined to each other", K(ret));
    }
  }

  // clean info
  ARRAY_FOREACH(stmt.get_from_items(), i)
  {
    OZ((column_namespace_checker_.remove_reference_table)(stmt.get_from_item(i).table_id_));
  }

  OX((stmt.get_joined_tables().reset()));
  OX((stmt.clear_from_item()));

  // add info to stmt
  OZ((stmt.add_from_item)(last_table_item->table_id_, last_table_item->is_joined_table()));

  if (OB_SUCC(ret) && last_table_item->is_joined_table()) {
    OZ((stmt.add_joined_table)(static_cast<JoinedTable*>(last_table_item)));
  }

  OZ((column_namespace_checker_.add_reference_table)(last_table_item));

  return ret;
}

int ObDMLResolver::deliver_outer_join_conditions(ObIArray<ObRawExpr*>& exprs, ObIArray<JoinedTable*>& joined_tables)
{
  int ret = OB_SUCCESS;
  for (int64_t i = exprs.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    ObArray<uint64_t> left_tables;
    ObArray<uint64_t> right_tables;
    ObRawExpr* expr = exprs.at(i);

    CK(OB_NOT_NULL(expr));
    OZ(extract_column_with_outer_join_symbol(expr, left_tables, right_tables));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_UNLIKELY(right_tables.count() > 1)) {
      /**
       * ORA-01468:
       * 01468. 00000 -  "a predicate may reference only one outer-joined table"
       * *Cause:
       * *Action:
       * ----
       */
      ret = OB_ERR_MULTI_OUTER_JOIN_TABLE;
      LOG_WARN("a predicate may reference only one outer-joined table", K(ret));
    } else if (1 == right_tables.count()) {
      ObArray<uint64_t> table_ids;
      OZ((append)(table_ids, left_tables));
      OZ((append)(table_ids, right_tables));
      bool is_delivered = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_delivered && j < joined_tables.count(); j++) {
        OZ(deliver_expr_to_outer_join_table(expr, table_ids, joined_tables.at(j), is_delivered));
      }

      OZ(remove_outer_join_symbol(expr));
      OZ((expr->extract_info)());
      if (OB_SUCC(ret) && is_delivered) {
        OZ((exprs.remove)(i));
      }
    }
  }
  return ret;
}

int ObDMLResolver::deliver_expr_to_outer_join_table(
    const ObRawExpr* expr, const ObIArray<uint64_t>& table_ids, JoinedTable* joined_table, bool& is_delivered)
{
  int ret = OB_SUCCESS;

  CK(OB_NOT_NULL(joined_table));
  is_delivered = false;
  if (OB_SUCC(ret)) {
    bool in_left = false;
    bool in_right = false;
    bool force_deliver = false;

    // check left
    CK(OB_NOT_NULL(joined_table->left_table_));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (joined_table->left_table_->is_joined_table()) {
      JoinedTable* cur_joined = static_cast<JoinedTable*>(joined_table->left_table_);
      for (int64_t i = 0; OB_SUCC(ret) && !in_left && i < cur_joined->single_table_ids_.count(); i++) {
        in_left = has_exist_in_array(table_ids, cur_joined->single_table_ids_.at(i));
      }
    } else {
      in_left = has_exist_in_array(table_ids, joined_table->left_table_->table_id_);
      if (in_left && joined_table->joined_type_ == ObJoinType::RIGHT_OUTER_JOIN) {
        force_deliver = true;
      }
    }

    // check right
    CK(OB_NOT_NULL(joined_table->right_table_));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (joined_table->right_table_->is_joined_table()) {
      JoinedTable* cur_joined = static_cast<JoinedTable*>(joined_table->right_table_);
      for (int64_t i = 0; OB_SUCC(ret) && !in_right && i < cur_joined->single_table_ids_.count(); i++) {
        in_right = has_exist_in_array(table_ids, cur_joined->single_table_ids_.at(i));
      }
    } else {
      in_right = has_exist_in_array(table_ids, joined_table->right_table_->table_id_);
      if (in_right && joined_table->joined_type_ == ObJoinType::LEFT_OUTER_JOIN) {
        force_deliver = true;
      }
    }

    // analyze result, recursive if not found
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (force_deliver || (in_left && in_right)) {
      is_delivered = true;
      OZ((joined_table->join_conditions_.push_back)(const_cast<ObRawExpr*>(expr)));
    } else if (in_left && joined_table->left_table_->is_joined_table()) {
      JoinedTable* cur_joined = static_cast<JoinedTable*>(joined_table->left_table_);
      OZ(SMART_CALL(deliver_expr_to_outer_join_table(expr, table_ids, cur_joined, is_delivered)));
    } else if (in_right && joined_table->right_table_->is_joined_table()) {
      JoinedTable* cur_joined = static_cast<JoinedTable*>(joined_table->right_table_);
      OZ(SMART_CALL(deliver_expr_to_outer_join_table(expr, table_ids, cur_joined, is_delivered)));
    }
  }
  return ret;
}

int ObDMLResolver::fill_same_column_to_using(JoinedTable*& joined_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 8> left_column_names;
  ObSEArray<ObString, 8> right_column_names;
  if (OB_ISNULL(joined_table)) {
    LOG_WARN("NULL joined table", K(ret));
  } else if (OB_FAIL(get_columns_from_table_item(joined_table->left_table_, left_column_names))) {
    LOG_WARN("failed to get left column names", K(ret));
  } else if (OB_FAIL(get_columns_from_table_item(joined_table->right_table_, right_column_names))) {
    LOG_WARN("failed to get right column names", K(ret));
  } else {
    // No overflow risk because all string from ObColumnSchemaV2.
    /*
     * find all common column and put to using_columns_.
     */
    for (int64_t i = 0; OB_SUCC(ret) && i < left_column_names.count(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < right_column_names.count(); j++) {
        if (ObCharset::case_insensitive_equal(left_column_names.at(i), right_column_names.at(j))) {
          if (OB_FAIL(joined_table->using_columns_.push_back(left_column_names.at(i)))) {
            LOG_WARN("failed to push back column name", K(ret));
          }
        }
      }
    }
  }
  // remove redundant using column
  ObIArray<ObString>& using_columns = joined_table->using_columns_;
  for (int64_t i = 0; OB_SUCC(ret) && i < using_columns.count(); i++) {
    for (int64_t j = using_columns.count() - 1; OB_SUCC(ret) && j > i; j--) {
      if (ObCharset::case_insensitive_equal(using_columns.at(i), using_columns.at(j))) {
        if (OB_FAIL(using_columns.remove(j))) {
          LOG_WARN("failed to remove redundant column name in using", K(ret));
        }
      }
    }
  }
  return ret;
}

/**
 * get all non-hidden columns from a TableItem.
 * call this function recursively if it's a JoinedTable.
 */
int ObDMLResolver::get_columns_from_table_item(const TableItem* table_item, ObIArray<ObString>& column_names)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item) || OB_ISNULL(schema_checker_) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL param", K(ret), K(table_item), K(schema_checker_));
  } else if (table_item->is_joined_table()) {
    const JoinedTable* joined_table = reinterpret_cast<const JoinedTable*>(table_item);
    if (OB_ISNULL(joined_table->left_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left table of joined table is NULL", K(ret));
    } else if (OB_ISNULL(joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("right table of joined table is NULL", K(ret));
    } else if (OB_FAIL(SMART_CALL(get_columns_from_table_item(joined_table->left_table_, column_names)))) {
      LOG_WARN("failed to get columns from left table item", K(ret));
    } else if (OB_FAIL(SMART_CALL(get_columns_from_table_item(joined_table->right_table_, column_names)))) {
      LOG_WARN("failed to get columns from right table item", K(ret));
    }
  } else if (table_item->is_generated_table()) {
    ObSelectStmt* table_ref = table_item->ref_query_;
    if (OB_ISNULL(table_ref)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generate table is null");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ref->get_select_item_size(); ++i) {
      const SelectItem& select_item = table_ref->get_select_item(i);
      if (OB_FAIL(column_names.push_back(select_item.alias_name_))) {
        LOG_WARN("push back column name failed", K(ret));
      }
    }
  } else if (table_item->is_function_table()) {
    OZ(ObResolverUtils::get_all_function_table_column_names(*table_item, params_, column_names));
  } else if (table_item->is_basic_table() || table_item->is_fake_cte_table()) {
    /**
     * CTE_TABLE is same as BASIC_TABLE or ALIAS_TABLE
     */
    const ObTableSchema* table_schema = NULL;
    if (OB_FAIL(schema_checker_->get_table_schema(table_item->ref_id_, table_schema))) {
      /**
       * Should not return OB_TABLE_NOT_EXIST.
       * Because tables have been checked in resolve_table already.
       */
      LOG_WARN("get table schema failed", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get NULL table schema", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count(); i++) {
      const ObColumnSchemaV2* column_schema = table_schema->get_column_schema_by_idx(i);
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL column schema", K(ret));
      } else if (column_schema->is_hidden()) {
        // do noting
      } else if (OB_FAIL(column_names.push_back(column_schema->get_column_name_str()))) {
        LOG_WARN("failed to push back column name", K(ret));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this table item is not supported", K(ret), K(table_item->type_));
  }
  return ret;
}

int ObDMLResolver::resolve_pseudo_column(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr)
{
  int ret = OB_SUCCESS;
  ObPseudoColumnRawExpr* pseudo_column_expr = NULL;
  if (0 == q_name.col_name_.case_compare("ORA_ROWSCN")) {
    const TableItem* table_item = NULL;
    if (OB_FAIL(column_namespace_checker_.check_rowscn_table_column_namespace(q_name, table_item))) {
      LOG_WARN("check rowscn table colum namespace failed", K(ret));
    } else if (OB_ISNULL(table_item)) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("ORA_ROWSCN pseudo column only avaliable in basic table", K(ret));
    } else if (OB_FAIL(get_stmt()->check_pseudo_column_exist(T_ORA_ROWSCN, pseudo_column_expr))) {
      LOG_WARN("fail to check pseudo column exist", K(ret));
    } else if (pseudo_column_expr != NULL) {
      // this type of pseudo_column_expr has been add
      real_ref_expr = pseudo_column_expr;
    } else if (OB_ISNULL(params_.expr_factory_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param factory is null", K(ret), K_(params_.expr_factory));
    } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_ORA_ROWSCN, pseudo_column_expr))) {
      LOG_WARN("create rowscn pseudo column expr failed", K(ret));
    } else if (OB_ISNULL(pseudo_column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pseudo column expr is null", K(ret));
    } else {
      pseudo_column_expr->set_data_type(ObIntType);
      pseudo_column_expr->set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);
      pseudo_column_expr->set_table_id(table_item->table_id_);
      pseudo_column_expr->add_relation_id(get_stmt()->get_table_bit_index(table_item->table_id_));
      pseudo_column_expr->set_expr_level(current_level_);
      real_ref_expr = pseudo_column_expr;
      OZ(get_stmt()->get_pseudo_column_like_exprs().push_back(pseudo_column_expr));
    }

    LOG_DEBUG("rowscn ", K(*pseudo_column_expr));
  } else {
    ret = OB_ERR_BAD_FIELD_ERROR;
  }

  return ret;
}

bool ObDMLResolver::has_qb_name_in_hints(
    common::ObIArray<ObTablesInHint>& hint_tables_arr, ObTablesInHint*& tables_in_hint, const ObString& qb_name)
{
  bool has_qb_name = false;
  for (int64_t i = 0; i < hint_tables_arr.count(); i++) {
    if (qb_name == hint_tables_arr.at(i).qb_name_) {
      tables_in_hint = &hint_tables_arr.at(i);
      has_qb_name = true;
      break;
    }
  }
  return has_qb_name;
}

int ObDMLResolver::uv_check_key_preserved(const TableItem& table_item, bool& key_preserved)
{
  int ret = OB_SUCCESS;
  if (table_item.is_generated_table()) {
    key_preserved = false;
    if (NULL == table_item.ref_query_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref query or view base table item is NULL", K(ret));
    } else {
      ObSEArray<ObColumnRefRawExpr*, 8> pk_cols;
      ObSEArray<ObRawExpr*, 8> pk_cols_raw;
      bool unique = false;
      // rowkey already add to stmt, it is safe to add again.
      if (OB_FAIL(add_all_rowkey_columns_to_stmt(table_item, pk_cols))) {
        LOG_WARN("get all rowkey exprs failed", K(ret));
      } else {
        FOREACH_CNT_X(e, pk_cols, OB_SUCC(ret))
        {
          int64_t idx = (*e)->get_column_id() - OB_APP_MIN_COLUMN_ID;
          if (idx < 0 || idx >= table_item.ref_query_->get_select_item_size()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid select item index", K(ret), K(*e));
          } else if (OB_FAIL(pk_cols_raw.push_back(table_item.ref_query_->get_select_item(idx).expr_))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(
                     table_item.ref_query_, session_info_, schema_checker_, pk_cols_raw, true /* strict */, unique))) {
        LOG_WARN("check stmt unique failed", K(ret));
      } else {
        key_preserved = unique;
      }
    }
  }
  return ret;
}

int ObDMLResolver::update_child_stmt_px_hint_for_dml()
{
  int ret = OB_SUCCESS;
  ObUsePxHint::Type dml_px_hint = get_stmt()->get_stmt_hint().use_px_;
  ObArray<ObSelectStmt*> child_stmts;
  if (OB_FAIL(get_stmt()->get_child_stmts(child_stmts))) {
    LOG_WARN("get child stmt failed", K(ret));
  }
  for (int64_t i = 0; i < child_stmts.count() && OB_SUCC(ret); i++) {
    child_stmts.at(i)->get_stmt_hint().use_px_ = dml_px_hint;
  }
  return ret;
}

int ObDMLResolver::fill_index_column_convert_exprs(bool use_static_engine, const IndexDMLInfo& primary_dml_info,
    const ObIArray<ObColumnRefRawExpr*>& column_exprs, ObIArray<ObRawExpr*>& column_convert_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
    int idx = -1;
    bool is_shadow_pk_column = false;
    for (int64_t j = 0; idx < 0 && !is_shadow_pk_column && j < primary_dml_info.column_exprs_.count() && OB_SUCC(ret);
         ++j) {
      if (OB_ISNULL(column_exprs.at(i)) || OB_ISNULL(primary_dml_info.column_exprs_.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret), K(column_exprs.at(i)), K(primary_dml_info.column_exprs_.at(j)));
      } else if (column_exprs.at(i)->get_column_id() == primary_dml_info.column_exprs_.at(j)->get_column_id()) {
        idx = j;
      } else {
        ObColumnRefRawExpr* column_ref_expr = column_exprs.at(i);
        if (column_ref_expr->is_virtual_generated_column() && !OB_ISNULL(column_ref_expr->get_dependant_expr()) &&
            column_ref_expr->get_dependant_expr()->get_expr_type() == T_OP_SHADOW_UK_PROJECT) {
          is_shadow_pk_column = true;
        }
      }
    }
    if (is_shadow_pk_column && use_static_engine) {
      ObColumnRefRawExpr* column_ref_expr = column_exprs.at(i);
      if (OB_FAIL(column_convert_exprs.push_back(column_ref_expr->get_dependant_expr()))) {
        LOG_WARN("failed to push back to column convert exprs", K(ret));
      }
    } else if (is_shadow_pk_column) {
      ObColumnRefRawExpr* column_ref_expr = column_exprs.at(i);
      ObOpRawExpr* dependent_expr = static_cast<ObOpRawExpr*>(column_ref_expr->get_dependant_expr());
      for (int column_idx = 0; column_idx < dependent_expr->get_param_count() && OB_SUCC(ret); column_idx++) {
        const ObRawExpr* dependent_raw_expr = dependent_expr->get_param_expr(column_idx);
        bool found = false;
        for (int pri_column_idx = 0; pri_column_idx < primary_dml_info.column_exprs_.count() && OB_SUCC(ret) && !found;
             pri_column_idx++) {
          if (OB_ISNULL(primary_dml_info.column_exprs_.at(pri_column_idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("the primary dml column expr is null", K(ret), K(pri_column_idx));
          } else if (dependent_raw_expr == primary_dml_info.column_exprs_.at(pri_column_idx)) {
            found = true;
            // replace dependency column of shadow pk with column convert exprs of primary dml info.
            if (OB_FAIL(dependent_expr->replace_param_expr(
                    column_idx, primary_dml_info.column_convert_exprs_.at(pri_column_idx)))) {
              LOG_WARN("failed to replace param expr", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && !found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not found the dependent ref expr in primary dml info column exprs",
              K(ret),
              K(*dependent_raw_expr),
              K(primary_dml_info.column_exprs_));
        }
      }
      if (OB_SUCC(ret)) {
        // means all dependent column exprs of shadow pk column have been replaced.
        if (OB_FAIL(column_convert_exprs.push_back(column_ref_expr))) {
          LOG_WARN("failed push expr to column convert exprs", K(ret));
        }
      }
      LOG_INFO("handle unique index shadow pk column", K(ret), K(*column_ref_expr));
    } else if (OB_SUCC(ret) && idx < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column convert expr not found", K(ret), K(primary_dml_info), KPC(column_exprs.at(i)));
    } else {
      OZ(column_convert_exprs.push_back(primary_dml_info.column_convert_exprs_.at(idx)));
    }
  }
  LOG_TRACE("check column convert expr", K(column_exprs.count()), K(column_convert_exprs.count()));
  return ret;
}

int ObDMLResolver::construct_calc_rowid_expr(
    const ObTableSchema* table_schema, const ObString& database_name, common::ObString& rowid_expr_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null schema or allocator", K(ret));
  } else {
    const uint64_t table_id = table_schema->get_table_id();
    ObSqlString tmp_expr_str;
    ObArray<uint64_t> col_ids;
    int64_t rowkey_cnt = -1;
    if (OB_FAIL(table_schema->get_column_ids_serialize_to_rowid(col_ids, rowkey_cnt))) {
      LOG_WARN("get col ids failed", K(ret));
    } else if (OB_UNLIKELY(col_ids.count() < rowkey_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected col_ids", K(ret), K(col_ids), K(rowkey_cnt));
    } else if (OB_FAIL(tmp_expr_str.append("calc_urowid("))) {
      LOG_WARN("failed to append str", K(ret));
    } else {
      const ObColumnSchemaV2* column_schema = NULL;
      if (table_schema->is_old_no_pk_table()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("rowid for old heap table not support", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "rowid for heap table before 227 not support");
      } else if (table_schema->is_new_no_pk_table()) {
        // serialize all rowkey for new no pk table
        if (OB_UNLIKELY(rowkey_cnt != col_ids.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("col_ids cnt is invalid", K(ret), K(col_ids), K(rowkey_cnt));
        } else if (OB_FAIL(tmp_expr_str.append_fmt("%ld,", ObURowIDData::NO_PK_ROWID_VERSION))) {
          LOG_WARN("failed to append str", K(ret));
        }
      } else {
        int64_t version = ObURowIDData::PK_ROWID_VERSION;
        if (col_ids.count() != rowkey_cnt) {
          if (OB_FAIL(ObURowIDData::get_part_gen_col_version(rowkey_cnt, version))) {
            LOG_WARN("get_part_gen_col_version failed", K(ret), K(rowkey_cnt), K(version));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(tmp_expr_str.append_fmt("%ld,", version))) {
          LOG_WARN("failed to append str", K(ret));
        }
      }
      CK(0 < col_ids.count());
      for (int i = 0; OB_SUCC(ret) && i < col_ids.count(); i++) {
        if (OB_FAIL(schema_checker_->get_column_schema(table_id, col_ids.at(i), column_schema, true))) {
          LOG_WARN("failed to get column schema", K(ret));
        } else if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid null column schema", K(ret));
        } else if (OB_FAIL(tmp_expr_str.append_fmt("%.*s.%.*s.\"%.*s\"",
                       database_name.length(),
                       database_name.ptr(),
                       table_schema->get_table_name_str().length(),
                       table_schema->get_table_name_str().ptr(),
                       column_schema->get_column_name_str().length(),
                       column_schema->get_column_name_str().ptr()))) {
          LOG_WARN("failed to append str", K(ret));
        } else if (i == col_ids.count() - 1) {
          if (OB_FAIL(tmp_expr_str.append(")"))) {
            LOG_WARN("failed to append str", K(ret));
          }
        } else if (OB_FAIL(tmp_expr_str.append(", "))) {
          LOG_WARN("failed to append str", K(ret));
        }
      }  // for end
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(ob_write_string(*allocator_, tmp_expr_str.string(), rowid_expr_str))) {
        LOG_WARN("failed to write string", K(ret));
      } else {
        LOG_DEBUG("constructed rowid expr", K(rowid_expr_str));
      }
    }
  }
  return ret;
}

int ObDMLResolver::resolve_rowid_column_expr(const TableItem& table_item, ObDMLStmt* stmt, ObRawExpr*& ref_expr)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  const ObDatabaseSchema* db_schema = NULL;
  ObString rowid_expr_str;
  ObArray<ObQualifiedName> columns;
  ObSQLSessionInfo* session_info = params_.session_info_;
  ObRawExprFactory* expr_factory = params_.expr_factory_;
  ObIAllocator* allocator = params_.allocator_;
  const uint64_t table_id = is_fake_table(table_item.table_id_) ? table_item.ref_id_ : table_item.table_id_;
  const ParseNode* expr_node = NULL;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(session_info) || OB_ISNULL(expr_factory) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(schema_checker_), K(session_info), K(expr_factory), K(allocator));
  } else if (OB_FAIL(schema_checker_->get_table_schema(table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null table schema", K(ret));
  } else if (OB_FAIL(schema_checker_->get_database_schema(
                 table_schema->get_tenant_id(), table_schema->get_database_id(), db_schema))) {
    LOG_WARN("failed to get database schema", K(ret));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null db schema", K(ret));
  } else if (OB_FAIL(construct_calc_rowid_expr(table_schema, db_schema->get_database_name_str(), rowid_expr_str))) {
    LOG_WARN("failed to construct expr strs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::parse_expr_node_from_str(rowid_expr_str, *allocator, expr_node))) {
    LOG_WARN("failed to parse expr node", K(ret));
  } else if (OB_ISNULL(expr_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr node", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(
                 *expr_factory, *session_info, *expr_node, ref_expr, columns, schema_checker_))) {
    LOG_WARN("failed to build generated column", K(ret));
  } else {
    // do nothing
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    ColumnItem* col_item = NULL;
    ObRawExpr* real_ref_expr = NULL;
    if (OB_FAIL(resolve_basic_column_item(table_item, columns.at(i).col_name_, true, col_item, stmt))) {
      LOG_WARN("failed to resolve basic column item", K(ret));
    } else if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column item is null", K(ret), K(col_item));
    } else {
      real_ref_expr = col_item->expr_;
      if (OB_FAIL(ObRawExprUtils::replace_ref_column(ref_expr, columns.at(i).ref_expr_, real_ref_expr))) {
        LOG_WARN("failed to replace column reference expr", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ref_expr->formalize(session_info))) {
      LOG_WARN("failed to formalize column expr", K(ret));
    }
  }
  return ret;
}

// get all column ref expr recursively
int ObDMLResolver::get_all_column_ref(ObRawExpr* expr, ObIArray<ObColumnRefRawExpr*>& arr)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(expr));
  if (OB_SUCC(ret)) {
    const ObItemType type = expr->get_expr_type();
    if (T_REF_COLUMN == type) {
      ObColumnRefRawExpr* col_ref = static_cast<ObColumnRefRawExpr*>(expr);
      if (OB_FAIL(add_var_to_array_no_dup(arr, col_ref))) {
        LOG_WARN("push back expr failed", K(ret));
      }
    } else if (IS_EXPR_OP(type) || IS_FUN_SYS_TYPE(type)) {
      ObOpRawExpr* op_expr = static_cast<ObOpRawExpr*>(expr);
      for (int64_t i = 0; OB_SUCC(ret) && i < op_expr->get_param_count(); ++i) {
        if (OB_FAIL(SMART_CALL(get_all_column_ref(op_expr->get_param_expr(i), arr)))) {
          LOG_WARN("get_all_column_ref failed", K(ret));
        }
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObDMLResolver::recursive_replace_rowid_col(ObRawExpr*& raw_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null raw expr", K(ret));
  } else if (T_REF_COLUMN == raw_expr->get_expr_type()) {
    ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(raw_expr);
    if (OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID == col_expr->get_column_id()) {
      ObRawExpr* dependent_expr = col_expr->get_dependant_expr();
      if (OB_ISNULL(dependent_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null dependent expr", K(ret));
      } else {
        raw_expr = dependent_expr;
      }
    } else {
      // do nothing
    }
  } else if (raw_expr->has_flag(CNT_COLUMN)) {
    for (int i = 0; i < raw_expr->get_param_count(); i++) {
      if (OB_FAIL(recursive_replace_rowid_col(raw_expr->get_param_expr(i)))) {
        LOG_WARN("failed to replace rowid column", K(ret));
      }
    }
  }
  return ret;
}

/*@brief, ObDMLResolver::process_part_str wrap some special keyword with double quotes, such as:
 * create table t1(SYSTIMESTAMP int) partition by range(SYSTIMESTAMP) (parition "p0" values less than 10000);
 * select SYSTIMESTAMP from dual; ==> select "SYSTIMESTAMP" from dual;
 * otherwise, it may be resolved as a function. other similar keywords:
 * SYSTIMESTAMP, CURRENT_DATE, LOCALTIMESTAMP, CURRENT_TIMESTAMP, SESSIONTIMEZONE, DBTIMEZONE,
 * CONNECT_BY_ISCYCLE, CONNECT_BY_ISLEAF
 */

#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')

int ObDMLResolver::process_part_str(ObIAllocator& calc_buf, const ObString& part_str, ObString& new_part_str)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  char* tmp_buf = NULL;
  const char* part_ptr = part_str.ptr();
  int32_t part_len = part_str.length();
  int64_t buf_len = part_len + part_len / 10 * 2;
  int32_t real_len = 0;
  int64_t offset = 0;
  if (OB_ISNULL(buf = static_cast<char*>(calc_buf.alloc(buf_len))) ||
      OB_ISNULL(tmp_buf = static_cast<char*>(calc_buf.alloc(part_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(buf), K(buf_len), K(tmp_buf), K(part_len));
  } else if (buf_len == part_len) {
    new_part_str.assign_ptr(part_ptr, part_len);
    LOG_TRACE("succeed to process part str", K(new_part_str));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i <= part_len; ++i) {
      if (i < part_len && ISSPACE(part_ptr[i])) {
        /*do nothing*/
      } else if (part_ptr[i] == ',' || i == part_len) {
        if (0 == STRNCASECMP(tmp_buf, "SYSTIMESTAMP", 12) || 0 == STRNCASECMP(tmp_buf, "CURRENT_DATE", 12) ||
            0 == STRNCASECMP(tmp_buf, "LOCALTIMESTAMP", 14) || 0 == STRNCASECMP(tmp_buf, "CURRENT_TIMESTAMP", 17) ||
            0 == STRNCASECMP(tmp_buf, "SESSIONTIMEZONE", 15) || 0 == STRNCASECMP(tmp_buf, "DBTIMEZONE", 10) ||
            0 == STRNCASECMP(tmp_buf, "CONNECT_BY_ISLEAF", 17) || 0 == STRNCASECMP(tmp_buf, "CONNECT_BY_ISCYCLE", 18)) {
          buf[real_len++] = '\"';
          for (int64_t j = 0; j < offset; ++j) {
            tmp_buf[j] = toupper(tmp_buf[j]);
          }
          MEMCPY(buf + real_len, tmp_buf, offset);
          real_len = real_len + offset;
          buf[real_len++] = '\"';
        } else {
          MEMCPY(buf + real_len, tmp_buf, offset);
          real_len = real_len + offset;
        }
        if (part_ptr[i] == ',') {
          buf[real_len++] = ',';
        }
        offset = 0;
        MEMSET(tmp_buf, 0, part_len);
      } else if (OB_UNLIKELY(offset >= part_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(offset), K(part_len), K(ret));
      } else {
        tmp_buf[offset++] = part_ptr[i];
      }
    }
    if (OB_SUCC(ret)) {
      new_part_str.assign_ptr(buf, real_len);
      LOG_TRACE("succeed to process part str", K(new_part_str));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
