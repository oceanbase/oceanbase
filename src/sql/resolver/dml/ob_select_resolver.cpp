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
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/dml/ob_del_upd_resolver.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/json/ob_json_print_utils.h"  // for SJ
#include "lib/time/ob_time_utility.h"
#include "lib/profile/ob_perf_event.h"
#include "lib/string/ob_sql_string.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/ob_time_utility2.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/expr/ob_raw_expr_info_extractor.h"
#include "sql/resolver/dml/ob_aggr_expr_push_up_analyzer.h"
#include "sql/resolver/dml/ob_group_by_checker.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/expr/ob_expr_version.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "sql/engine/expr/ob_json_param_type.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace jit::expr;
namespace sql
{

ObSelectResolver::ObSelectResolver(ObResolverParams &params)
  : ObDMLResolver(params),
    current_recursive_cte_table_item_(NULL),
    current_cte_involed_stmt_(NULL),
    has_calc_found_rows_(false),
    has_top_limit_(false),
    in_set_query_(false),
    is_sub_stmt_(false),
    standard_group_checker_(),
    transpose_item_(NULL),
    is_left_child_(false),
    having_has_self_column_(false),
    has_grouping_(false),
    has_group_by_clause_(false),
    has_nested_aggr_(false),
    is_top_stmt_(false),
    has_resolved_field_list_(false)
{
  params_.is_from_create_view_ = params.is_from_create_view_;
  params_.is_from_create_table_ = params.is_from_create_table_;
  params_.is_specified_col_name_ = params.is_specified_col_name_;
  auto_name_id_ = 1;
}

ObSelectResolver::~ObSelectResolver()
{
}

ObSelectStmt *ObSelectResolver::get_select_stmt()
{
  return static_cast<ObSelectStmt*>(stmt_);
}

int ObSelectResolver::resolve_set_query(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  bool recursive_union = false;
  bool need_swap_child = false;

  if (cte_ctx_.is_with_resolver() && OB_FAIL(check_query_is_recursive_union(parse_tree, recursive_union, need_swap_child))) {
    LOG_WARN("failed to do resolve set query", K(ret));
  } else if (recursive_union) {
    if (OB_FAIL(do_resolve_set_query_in_cte(parse_tree, need_swap_child))) {
      LOG_WARN("failed to do resolve set query in cte", K(ret));
    }
  } else if (OB_FAIL(do_resolve_set_query(parse_tree))) {
    LOG_WARN("failed to do resolve set query", K(ret));
  }
  return ret;
}

int ObSelectResolver::do_check_basic_table_in_cte_recursive_union(const ParseNode &parse_tree, bool &recursive_union)
{
  int ret = OB_SUCCESS;
  const ParseNode *table_node = &parse_tree;
  bool no_defined_database_name = true;

  if (T_ORG == parse_tree.type_) {
    table_node = parse_tree.children_[0];
  } else if (T_ALIAS == parse_tree.type_) {
    table_node = parse_tree.children_[0];
  }
  no_defined_database_name = (table_node->children_[0] == NULL);
  // compare current table name is equal to current cte table name
  ObString tblname(table_node->str_len_, table_node->str_value_);
  if (cte_ctx_.is_with_resolver()
      && ObCharset::case_insensitive_equal(cte_ctx_.current_cte_table_name_, tblname)
      && tblname.length()
      && no_defined_database_name) {
    recursive_union = true;
  }
  return ret;
}


// recursive test node to find a cte table
int ObSelectResolver::do_check_node_in_cte_recursive_union(const ParseNode* current_node, bool &recursive_union) 
{
  int ret = OB_SUCCESS;
  while (OB_NOT_NULL(current_node)
        && current_node->type_ != T_RELATION_FACTOR
        && current_node->type_ != T_WITH_CLAUSE_LIST
        && current_node->num_child_ == 1) {
    // the current node with only one child node expands immediately to prevent the recursive level from being too high
    current_node = current_node->children_[0];
  }

  if (OB_ISNULL(current_node)) {
  } else if (current_node->type_ == T_WITH_CLAUSE_LIST && is_oracle_mode()) {
    ret = OB_ERR_UNSUPPORTED_USE_OF_CTE;
    LOG_WARN("invalid argument, oracle cte do not support a with clause nest", K(ret));
  } else if (current_node->type_ == T_RELATION_FACTOR) {
    // find relation factor, check it
    if (OB_FAIL(do_check_basic_table_in_cte_recursive_union(*current_node, recursive_union))) {
      LOG_WARN("failed to do check basic table is in cte recursive union", K(ret));
    } 
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < current_node->num_child_ && !recursive_union; i += 1) {
      if (OB_FAIL(SMART_CALL(do_check_node_in_cte_recursive_union(current_node->children_[i], recursive_union)))) {
        LOG_WARN("failed to do check node is in cte recursive union", K(ret));
      }
    }
  }

  return ret;
}

// test if a query node contains recursive nodes
int ObSelectResolver::check_query_is_recursive_union(const ParseNode &parse_tree, bool &recursive_union, bool &need_swap_child)
{
  int ret = OB_SUCCESS;
  ParseNode *left_node = NULL;
  ParseNode *right_node = NULL;
  
  bool left_recursive_union = false;
  bool right_recursive_union = false;

  if (OB_ISNULL(left_node = parse_tree.children_[PARSE_SELECT_FORMER])
      || OB_ISNULL(right_node = parse_tree.children_[PARSE_SELECT_LATER])
      || OB_ISNULL(parse_tree.children_[PARSE_SELECT_SET])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (NULL != parse_tree.children_[PARSE_SELECT_WITH] && is_oracle_mode()) {
    ret = OB_ERR_UNSUPPORTED_USE_OF_CTE;
    LOG_WARN("invalid argument, oracle cte do not support a with clause nest", K(ret));
  } else if (OB_FAIL(do_check_node_in_cte_recursive_union(left_node, left_recursive_union))) {
    //test left branch
    LOG_WARN("failed to check set query in cte is recursive union", K(ret));
  } else if (OB_FAIL(do_check_node_in_cte_recursive_union(right_node, right_recursive_union))) {
    //test right branch
    LOG_WARN("failed to check set query in cte is recursive union", K(ret));
  }
  
  recursive_union = left_recursive_union || right_recursive_union;

  /**
    *  为什么需要交换左右支？
    *  with cte(c1) as (select 1 from dual union all select c1+1 from cte where c1 < 100)
    *  select * from cte;
    *
    *  with cte(c1) as (select c1+1 from cte where c1 < 100 && select 1 from dual)
    *  select * from cte;
    *
    *  oracle支持这两种写法。之前在cte的实现时是误判了，认为只能左边是anchor member。
    *  对于recursive cte的revolber解析来说，左右支解析是敏感的，右边的解析依赖于左边先被
    *  解析。为什么呢？因为假设在没有解析左边的时候就开始解析右边，我们完全不知道cte这张表
    *  的c1列是什么类型。所以这里先判断是否需要交换左右支。
    */
  if (is_oracle_mode() && left_recursive_union && !right_recursive_union) {
    need_swap_child = true;
  }

  return ret;
}

// resolve 对于非cte, union, check child 能否展平（limit、order、fetch）
int ObSelectResolver::do_resolve_set_query_in_cte(const ParseNode &parse_tree, bool swap_branch)
{
  int ret = OB_SUCCESS;
  bool need_swap_child = false;
  ObSelectStmt *select_stmt = get_select_stmt();
  SelectParserOffset left_member = PARSE_SELECT_FORMER;
  SelectParserOffset right_member = PARSE_SELECT_LATER;
  ObSelectResolver left_resolver(params_);
  ObSelectResolver right_resolver(params_);
  ObSelectStmt *left_select_stmt = NULL;
  ObSelectStmt *right_select_stmt = NULL;

  left_resolver.set_current_level(current_level_);
  left_resolver.set_current_view_level(current_view_level_);
  left_resolver.set_in_set_query(true);
  left_resolver.set_parent_namespace_resolver(parent_namespace_resolver_);
  left_resolver.set_calc_found_rows(has_calc_found_rows_);

  right_resolver.set_current_level(current_level_);
  right_resolver.set_current_view_level(current_view_level_);
  right_resolver.set_in_set_query(true);
  right_resolver.set_parent_namespace_resolver(parent_namespace_resolver_);

  OC( (left_resolver.set_cte_ctx)(cte_ctx_) );
  OC( (right_resolver.set_cte_ctx)(cte_ctx_) );

  left_resolver.cte_ctx_.set_recursive_left_branch();

  if (swap_branch) {
    left_member = PARSE_SELECT_LATER;
    right_member = PARSE_SELECT_FORMER;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(select_stmt) || OB_ISNULL(parse_tree.children_[PARSE_SELECT_FORMER])
      || OB_ISNULL(parse_tree.children_[PARSE_SELECT_LATER])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(select_stmt),
        K(parse_tree.children_[PARSE_SELECT_FORMER]), K(parse_tree.children_[PARSE_SELECT_LATER]));
  } else if (parse_tree.children_[PARSE_SELECT_LATER]->value_ == 1) {
    ret = OB_ERR_ILLEGAL_ID;
    LOG_WARN("Select for update statement can not process set query");
  } else if (OB_FAIL(set_stmt_set_type(select_stmt, parse_tree.children_[PARSE_SELECT_SET]))) {
    LOG_WARN("failed to set stmt set type", K(ret));
  } else if (OB_FAIL(resolve_with_clause(parse_tree.children_[PARSE_SELECT_WITH]))) {
    LOG_WARN("failed to resolve with clause", K(ret));
  } else if (OB_FAIL(add_cte_table_to_children(left_resolver)) ||
             OB_FAIL(add_cte_table_to_children(right_resolver))) {
    LOG_WARN("failed to add cte table to children", K(ret));
  } else if (OB_FAIL(left_resolver.resolve_child_stmt(*(parse_tree.children_[left_member])))) {
    if (OB_ERR_NEED_INIT_BRANCH_IN_RECURSIVE_CTE == ret) {
      if (is_oracle_mode()){
        /* do nothing */
        LOG_WARN("Failed to resolve child stmt", K(ret));
      } else if (cte_ctx_.has_recursive_word_) {
        ret = OB_ERR_CTE_NEED_QUERY_BLOCKS;  // mysql error: Recursive Common Table Expression 'cte' should have one or
                                             // more non-recursive query blocks followed by one or more recursive ones
        LOG_WARN("Failed to resolve child stmt", K(ret));
      } else {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("cte table shows in left union stmt without recursive keyword", K(ret));
      }
    } else {
      LOG_WARN("Failed to find anchor member", K(ret));
    }
  } else if (OB_ISNULL(left_select_stmt = left_resolver.get_child_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stmt");
  } else if (lib::is_oracle_mode() && left_select_stmt->is_set_stmt()) {
    ret = OB_ERR_NEED_ONLY_TWO_BRANCH_IN_RECURSIVE_CTE;
    LOG_WARN("UNION ALL operation in recursive WITH clause must have only two branches", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::wrap_enum_set_for_stmt(*params_.expr_factory_, left_select_stmt, session_info_))) {
    LOG_WARN("failed to wrap_enum_set_for_stmt", KPC(left_select_stmt));
  } else {
    if (swap_branch) {
      select_stmt->set_children_swapped();
    }
  } 
  
  if (OB_SUCC(ret)) {
    if (!cte_ctx_.has_cte_param_list_ &&
        !left_resolver.cte_ctx_.cte_col_names_.empty()) {
      right_resolver.cte_ctx_.cte_col_names_.reset();
      cte_ctx_.cte_col_names_.reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < left_resolver.cte_ctx_.cte_col_names_.count(); ++i) {
        // to right resolver
        if (OB_FAIL(right_resolver.cte_ctx_.cte_col_names_.push_back(
                left_resolver.cte_ctx_.cte_col_names_.at(i)))) {
          LOG_WARN("pass cte column name to child resolver failed", K(ret));
        // to parent resolver
        } else if (OB_FAIL(cte_ctx_.cte_col_names_.push_back(
                left_resolver.cte_ctx_.cte_col_names_.at(i)))) {
          LOG_WARN("pass cte column name to child resolver failed", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(right_resolver.cte_ctx_.set_recursive_right_branch(left_select_stmt,
                            parse_tree.children_[left_member], !select_stmt->is_set_distinct()))) {
  } else if (OB_FAIL(right_resolver.resolve_child_stmt(*parse_tree.children_[right_member]))) {
    LOG_WARN("failed to resolve child stmt", K(ret));
  } else if (OB_FAIL(resolve_into_clause(ObResolverUtils::get_select_into_node(parse_tree)))) {
    LOG_WARN("failed to resolve into clause", K(ret));
  } else if (OB_ISNULL(right_select_stmt = right_resolver.get_child_stmt())
             || OB_ISNULL(left_select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(left_select_stmt), K(right_select_stmt));
  } else {
    select_stmt->add_set_query(left_select_stmt);
    select_stmt->add_set_query(right_select_stmt);
    select_stmt->set_calc_found_rows(left_select_stmt->is_calc_found_rows());
    /**MySQL
     * The types of the CTE result columns are inferred from the column types of the nonrecursive SELECT part only,
     * and the columns are all nullable. For type determination, the recursive SELECT part is ignored.
    */
    if (OB_FAIL(ObOptimizerUtil::gen_set_target_list(allocator_, session_info_,
                                                     params_.expr_factory_, *left_select_stmt,
                                                     *right_select_stmt, select_stmt,
                                                     lib::is_mysql_mode() && right_resolver.cte_ctx_.is_recursive(),
                                                     &cte_ctx_.cte_col_names_))) {
      LOG_WARN("failed to gen set target list.", K(ret));
    } else if (!right_resolver.cte_ctx_.is_recursive()) {
      /*do nothing*/
    } else if (lib::is_oracle_mode() && OB_FAIL(check_cte_set_types(*left_select_stmt, *right_select_stmt))) {
      LOG_WARN("check cte set types", K(ret));
    } else if (select_stmt->is_set_distinct() || ObSelectStmt::UNION != select_stmt->get_set_op()) {
      // 必须是union all
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "recursive WITH clause using operation not union all");
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "recursive WITH clause using union (distinct) operation");
    } else if (OB_FAIL(check_recursive_cte_limited())) {
      LOG_WARN("failed to check recursive cte limited", K(ret));
    } else if (OB_NOT_NULL(parse_tree.children_[PARSE_SELECT_LIMIT])) {
      ret = OB_ERR_CTE_ILLEGAL_RECURSIVE_BRANCH;
      LOG_WARN("use limit clause in the recursive cte is not allowed", K(ret));
    } else if (is_oracle_mode() &&
               OB_FAIL(adjust_recursive_cte_table_columns(select_stmt, right_select_stmt))) {
      LOG_WARN("failed to adjust recursive cte table columns", K(ret));
    } else {
      /**
      * 设置这个一个set query是否是with clause中的递归类型
      * 这个带有set op 为union的stmt，被标记为递归union，在后续展开时，平常的union all算子将被R union算子取代
      */
      select_stmt->set_recursive_union(true);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (parse_tree.children_[PARSE_SELECT_FOR_UPD] != NULL && is_oracle_mode()) {
    ret = OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED;
    LOG_WARN("set stmt can not have for update clause", K(ret));
  } else if (OB_FAIL(resolve_order_clause(parse_tree.children_[PARSE_SELECT_ORDER]))) {
    LOG_WARN("failed to resolve order clause", K(ret));
  } else if (OB_FAIL(resolve_limit_clause(parse_tree.children_[PARSE_SELECT_LIMIT]))) {
    LOG_WARN("failed to resolve limit clause", K(ret));
  } else if (OB_FAIL(resolve_fetch_clause(parse_tree.children_[PARSE_SELECT_FETCH]))) {
    LOG_WARN("failed to resolve fetch clause", K(ret));
  } else if (OB_FAIL(resolve_check_option_clause(parse_tree.children_[PARSE_SELECT_WITH_CHECK_OPTION]))) {
    LOG_WARN("failed to resolve check option clause", K(ret));
  } else if (OB_FAIL(resolve_set_query_hint())) {
    LOG_WARN("failed to resolve set query hint", K(ret));
  } else if (OB_FAIL(select_stmt->formalize_stmt(session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else if (OB_FAIL(check_order_by())) {
    LOG_WARN("failed to check order by", K(ret));
  } else if (OB_FAIL(check_udt_set_query())) {
    LOG_WARN("failed to check udt set query", K(ret));
  } else if (has_top_limit_) {
    has_top_limit_ = false;
    select_stmt->set_has_top_limit(NULL != parse_tree.children_[PARSE_SELECT_LIMIT]);
  }
  return ret;
}

// just add id name pair to generate qb name.
int ObSelectResolver::resolve_set_query_hint()
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt = NULL;
  ObQueryCtx *query_ctx = NULL;
  ObString qb_name;
  if (OB_ISNULL(stmt = get_stmt()) || OB_ISNULL(query_ctx = stmt->get_query_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Stmt and query ctx should not be NULL. ", K(ret), K(stmt), K(query_ctx));
  } else if (OB_FAIL(query_ctx->get_query_hint_for_update().set_stmt_id_map_info(*stmt, qb_name))) {
    LOG_WARN("failed to add id name pair", K(ret));
  }
  return ret;
}

int ObSelectResolver::do_resolve_set_query(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  ParseNode *left_node = NULL;
  ParseNode *right_node = NULL;
  ObSEArray<ObSelectStmt*, 2> left_child_stmts;
  ObSEArray<ObSelectStmt*, 2> right_child_stmts;
  bool force_serial_set_order = false;
  if (OB_ISNULL(left_node = parse_tree.children_[PARSE_SELECT_FORMER])
      || OB_ISNULL(right_node = parse_tree.children_[PARSE_SELECT_LATER])
      || OB_ISNULL(parse_tree.children_[PARSE_SELECT_SET]) || OB_ISNULL(select_stmt)
      || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(left_node), K(right_node), K(parse_tree.children_[PARSE_SELECT_SET]),
                                        K(select_stmt), K(session_info_), K(ret));
  } else if (right_node->value_ == 1) {
    ret = OB_ERR_ILLEGAL_ID;
    LOG_WARN("Select for update statement can not process set query", K(ret));
  } else if (OB_FAIL(set_stmt_set_type(select_stmt, parse_tree.children_[PARSE_SELECT_SET]))) {
    LOG_WARN("failed to set stmt set type", K(ret));
  } else if (OB_FAIL(resolve_into_clause(ObResolverUtils::get_select_into_node(parse_tree)))) {
    LOG_WARN("failed to resolve into clause", K(ret));
  } else if (parse_tree.children_[PARSE_SELECT_FOR_UPD] != NULL && is_oracle_mode()) {
    ret = OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED;
    LOG_WARN("set stmt can not have for update clause", K(ret));
  } else if (OB_FAIL(resolve_with_clause(parse_tree.children_[PARSE_SELECT_WITH]))) {
    LOG_WARN("failed to resolve with clause", K(ret));
  } else if (T_SET_UNION == parse_tree.children_[PARSE_SELECT_SET]->type_) {
    // union 进行展平
    if (OB_FAIL(SMART_CALL(do_resolve_set_query(*left_node, left_child_stmts, true)))) {
      LOG_WARN("failed to do resolve set query", K(ret));
    } else if (OB_FAIL(SMART_CALL(do_resolve_set_query(*right_node, right_child_stmts)))) {
      LOG_WARN("failed to do resolve set query", K(ret));
    }
  } else {
    // union 以外 set 不进行展平
    ObSelectStmt *left_child_stmt = NULL;
    ObSelectStmt *right_child_stmt= NULL;
    if (OB_FAIL(SMART_CALL(do_resolve_set_query(*left_node, left_child_stmt, true)))) {
      LOG_WARN("failed to do resolve set query", K(ret));
    } else if (OB_FAIL(SMART_CALL(do_resolve_set_query(*right_node, right_child_stmt)))) {
      LOG_WARN("failed to do resolve set query", K(ret));
    } else if (OB_FAIL(left_child_stmts.push_back(left_child_stmt)) ||
               OB_FAIL(right_child_stmts.push_back(right_child_stmt))) {
      LOG_WARN("failed set child stmts", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    select_stmt->get_set_query().reuse();
    if (OB_FAIL(select_stmt->get_set_query().assign(left_child_stmts)) ||
        OB_FAIL(append(select_stmt->get_set_query(), right_child_stmts))) {
      LOG_WARN("failed add child stmts", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::gen_set_target_list(allocator_, session_info_,
                                                            params_.expr_factory_,
                                                            left_child_stmts, right_child_stmts,
                                                            select_stmt))) {
      LOG_WARN("failed to get set target list", K(ret));
    } else {
      select_stmt->set_calc_found_rows(select_stmt->get_set_query(0)->is_calc_found_rows());
    }
  }

  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(session_info_->is_serial_set_order_forced(force_serial_set_order, lib::is_oracle_mode()))) {
    LOG_WARN("fail to get explicit_defaults_for_timestamp", K(ret));
  } else if (!force_serial_set_order) {
    //do nothing
  } else if (T_SET_UNION == parse_tree.children_[PARSE_SELECT_SET]->type_ &&
             NULL != parse_tree.children_[PARSE_SELECT_SET]->children_[0] &&
             T_ALL == parse_tree.children_[PARSE_SELECT_SET]->children_[0]->type_) {
      // for set query except union-all/recursive, when force serial set order, will add select expr as oder by expr
      force_serial_set_order = false;
  }

  if (OB_FAIL(ret)) {
  } else if (parse_tree.children_[PARSE_SELECT_FOR_UPD] != NULL && is_oracle_mode()) {
    ret = OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED;
    LOG_WARN("set stmt can not have for update clause", K(ret));
  } else if (OB_FAIL(resolve_order_clause(parse_tree.children_[PARSE_SELECT_ORDER], force_serial_set_order))) {
    LOG_WARN("failed to resolve order clause", K(ret));
  } else if (OB_FAIL(resolve_limit_clause(parse_tree.children_[PARSE_SELECT_LIMIT]))) {
    LOG_WARN("failed to resolve limit clause", K(ret));
  } else if (OB_FAIL(resolve_fetch_clause(parse_tree.children_[PARSE_SELECT_FETCH]))) {
    LOG_WARN("failed to resolve fetch clause", K(ret));
  } else if (OB_FAIL(resolve_check_option_clause(parse_tree.children_[PARSE_SELECT_WITH_CHECK_OPTION]))) {
    LOG_WARN("failed to resolve check option clause", K(ret));
  } else if (OB_FAIL(resolve_set_query_hint())) {
    LOG_WARN("failed to resolve set query hint", K(ret));
  } else if (OB_FAIL(select_stmt->formalize_stmt(session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else if (OB_FAIL(check_order_by())) {
    LOG_WARN("failed to check order by", K(ret));
  } else if (OB_FAIL(check_udt_set_query())) {
    LOG_WARN("failed to check udt set query", K(ret));
  } else if (has_top_limit_) {
    has_top_limit_ = false;
    select_stmt->set_has_top_limit(NULL != parse_tree.children_[PARSE_SELECT_LIMIT]);
  }
  return ret;
}

int ObSelectResolver::check_udt_set_query()
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (select_stmt->is_set_stmt()) {
    if (select_stmt->get_set_op() == ObSelectStmt::UNION && !select_stmt->is_set_distinct()) {
      // UNION ALL
      // do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); i++) {
        ObRawExpr *expr = select_stmt->get_select_item(i).expr_;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (expr->get_result_type().is_ext()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("set operator for udt not supported", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "set operator for udt");
        }
      }
    }
  }
  return ret;
}

// resolve 对于非cte, union, check child 能否展平（limit、order、fetch）
int ObSelectResolver::do_resolve_set_query(const ParseNode &parse_tree,
                                           common::ObIArray<ObSelectStmt*> &child_stmts,
                                           const bool is_left_child) /*default false*/
{
  int ret = OB_SUCCESS;
  bool can_flatten = false;
  bool is_type_same = false;
  ObSelectStmt *select_stmt = NULL;
  if (OB_ISNULL(select_stmt = get_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(select_stmt));
  } else if (parse_tree.children_[PARSE_SELECT_FOR_UPD] != NULL && is_oracle_mode()) {
    ret = OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED;
    LOG_WARN("set stmt can not have for update clause", K(ret));
  } else if (OB_FAIL(is_set_type_same(select_stmt, parse_tree.children_[PARSE_SELECT_SET],
                                      is_type_same))) {
    LOG_WARN("failed to check is set type same", K(ret));
  } else if (!is_type_same) {
    // select_stmt 与待 resolve set stmt 类型不同, 无法展平
    can_flatten = false;
  } else if (NULL != parse_tree.children_[PARSE_SELECT_ORDER] ||
             NULL != parse_tree.children_[PARSE_SELECT_LIMIT] ||
             NULL != parse_tree.children_[PARSE_SELECT_FETCH]) {
    // 待 resolve set stmt 有 order by / limit / fetch, 无法展平
    can_flatten = false;
  } else if (ObSelectStmt::UNION == select_stmt->get_set_op()) {
    // 仅对 union 进行展平
    can_flatten = true;
  }

  if (OB_FAIL(ret)) {
  } else if (can_flatten) {
    ObSEArray<ObSelectStmt*, 2> left_child_stmts;
    ObSEArray<ObSelectStmt*, 2> right_child_stmts;
    ParseNode *left_node = NULL;
    ParseNode *right_node = NULL;
    if (OB_ISNULL(left_node = parse_tree.children_[PARSE_SELECT_FORMER])
        || OB_ISNULL(right_node = parse_tree.children_[PARSE_SELECT_LATER])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (right_node->value_ == 1) {
      ret = OB_ERR_ILLEGAL_ID;
      LOG_WARN("Select for update statement can not process set query", K(ret));
    } else if (lib::is_oracle_mode() && OB_NOT_NULL(parse_tree.children_[PARSE_SELECT_WITH])) {
      ret = OB_ERR_UNSUPPORTED_USE_OF_CTE;
      LOG_WARN("oracle not support use of cte", K(ret));
    } else if (OB_FAIL(resolve_into_clause(ObResolverUtils::get_select_into_node(parse_tree)))) {
      LOG_WARN("failed to resolve into clause", K(ret));
    } else if (OB_FAIL(resolve_with_clause(parse_tree.children_[PARSE_SELECT_WITH]))) {
      LOG_WARN("failed to resolve with clause", K(ret));
    } else if (OB_FAIL(SMART_CALL(do_resolve_set_query(*left_node, left_child_stmts,
                                                       is_left_child)))) {
      LOG_WARN("failed to do resolve set query", K(ret));
    } else if (OB_FAIL(SMART_CALL(do_resolve_set_query(*right_node, right_child_stmts)))) {
      LOG_WARN("failed to do resolve set query", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::try_add_cast_to_set_child_list(allocator_, session_info_,
                                          params_.expr_factory_, select_stmt->is_set_distinct(),
                                          left_child_stmts, right_child_stmts, NULL))) {
      LOG_WARN("failed to try add cast to set child list", K(ret));
    } else if (OB_FAIL(append(child_stmts, left_child_stmts)) ||
               OB_FAIL(append(child_stmts, right_child_stmts))) {
      LOG_WARN("failed to append stmts", K(ret));
    }
  } else {
    ObSelectStmt *child_stmt = NULL;
    if (OB_FAIL(do_resolve_set_query(parse_tree, child_stmt, is_left_child))) {
      LOG_WARN("failed to do resolve set query", K(ret));
    } else if (OB_FAIL(child_stmts.push_back(child_stmt))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::do_resolve_set_query(const ParseNode &parse_tree,
                                           ObSelectStmt *&child_stmt,
                                           const bool is_left_child) /*default false*/
{
  int ret = OB_SUCCESS;
  child_stmt = NULL;
  ObSelectResolver child_resolver(params_);

  child_resolver.set_current_level(current_level_);
  child_resolver.set_current_view_level(current_view_level_);
  child_resolver.set_in_set_query(true);
  child_resolver.set_parent_namespace_resolver(parent_namespace_resolver_);
  child_resolver.set_calc_found_rows(is_left_child && has_calc_found_rows_);
  child_resolver.set_is_left_child(is_left_child);

  if (OB_FAIL(child_resolver.set_cte_ctx(cte_ctx_))) {
    LOG_WARN("failed to set cte ctx", K(ret));
  } else if (OB_FAIL(add_cte_table_to_children(child_resolver))) {
    LOG_WARN("failed to add cte table to children", K(ret));
  } else if (OB_FAIL(child_resolver.resolve_child_stmt(parse_tree))) {
    LOG_WARN("failed to resolve child stmt", K(ret));
  } else if (OB_ISNULL(child_stmt = child_resolver.get_child_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null child stmt", K(ret));
  }

  return ret;
}

int ObSelectResolver::set_stmt_set_type(ObSelectStmt *select_stmt,
                                        ParseNode *set_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(set_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    // assign set type
    switch (set_node->type_) {
    case T_SET_UNION:
      select_stmt->assign_set_op(ObSelectStmt::UNION);
      break;
    case T_SET_INTERSECT:
      select_stmt->assign_set_op(ObSelectStmt::INTERSECT);
      break;
    case T_SET_EXCEPT:
      select_stmt->assign_set_op(ObSelectStmt::EXCEPT);
      break;
    default:
      ret = OB_ERR_OPERATOR_UNKNOWN;
      LOG_WARN("unknown set operator of set clause");
      break;
    }
    // check distinct and all
    if (OB_FAIL(ret)) {
    } else if (1 != set_node->num_child_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrong num_child_", K(set_node->num_child_));
    } else if (NULL == set_node->children_[0]) {
      select_stmt->assign_set_distinct();
    } else {
      switch (set_node->children_[0]->type_) {
      case T_ALL:
        select_stmt->assign_set_all();
        break;
      case T_DISTINCT:
        select_stmt->assign_set_distinct();
        break;
      default:
        ret = OB_ERR_OPERATOR_UNKNOWN;
        LOG_WARN("unknown set operator of set option");
        break;
      }
    }
  }
  return ret;
}

int ObSelectResolver::is_set_type_same(const ObSelectStmt *select_stmt,
                                       ParseNode *set_node,
                                       bool &is_type_same)
{
  int ret = OB_SUCCESS;
  is_type_same = false;
  if (OB_ISNULL(set_node)) {
    /*do nothing*/
  } else if ((ObSelectStmt::INTERSECT == select_stmt->get_set_op()
              && T_SET_INTERSECT == set_node->type_)
             || (ObSelectStmt::EXCEPT == select_stmt->get_set_op()
                 && T_SET_EXCEPT == set_node->type_)) {
    is_type_same = true;
  } else if (ObSelectStmt::UNION == select_stmt->get_set_op() && T_SET_UNION == set_node->type_) {
    if (1 != set_node->num_child_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrong num_child_", K(set_node->num_child_));
    } else if (NULL == set_node->children_[0] || T_DISTINCT == set_node->children_[0]->type_) {
      is_type_same = select_stmt->is_set_distinct();
    } else if (T_ALL == set_node->children_[0]->type_) {
      is_type_same = !select_stmt->is_set_distinct();
    } else {
      ret = OB_ERR_OPERATOR_UNKNOWN;
      LOG_WARN("unknown set operator of set option");
    }
  } else {
    is_type_same = false;
  }
  return ret;
}

int ObSelectResolver::check_recursive_cte_limited()
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  ObSelectStmt *right_stmt = NULL;
  if (OB_ISNULL(select_stmt = get_select_stmt()) ||
      OB_ISNULL(right_stmt = select_stmt->get_set_query(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the recursive union stmt/right subquery is null", K(ret));
  } else if (OB_UNLIKELY(right_stmt->has_group_by())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "group by in recursive with clause");
  } else if (OB_UNLIKELY(right_stmt->has_limit())){
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "limit in recursive with clause");
  } else if (OB_UNLIKELY(right_stmt->has_top_limit())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "limit in recursive with clause");
  } else if (OB_UNLIKELY(right_stmt->has_distinct())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "distinct in recursive with clause");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < right_stmt->get_select_items().count(); ++i) {
      SelectItem& item = right_stmt->get_select_items().at(i);
      if (OB_UNLIKELY(jit::expr::ObIRawExpr::EXPR_AGGR == item.expr_->get_expr_class())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "aggregation in recursive with clause");
      }
    }
  }
  return ret;
}

int ObSelectResolver::check_cte_set_types(ObSelectStmt &left_stmt, ObSelectStmt &right_stmt)
{
  int ret = OB_SUCCESS;
  ObExprResType res_type;
  int64_t num = left_stmt.get_select_item_size();
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(select_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("params is invalid", K(ret), K(select_stmt), K(params_.expr_factory_));
  } else if (left_stmt.get_select_item_size() != right_stmt.get_select_item_size()) {
    ret = OB_ERR_COLUMN_SIZE;
    LOG_WARN("The used SELECT statements have a different number of columns", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
    SelectItem &left_select_item = left_stmt.get_select_item(i);
    SelectItem &right_select_item = right_stmt.get_select_item(i);
    ObExprResType l_type = left_select_item.expr_->get_result_type();
    ObExprResType r_type = right_select_item.expr_->get_result_type();
    if (l_type != r_type) {
      if (((ObObjMeta)l_type) == ((ObObjMeta)r_type)) {
        //类型相等，不考虑精度
      } else if (l_type.is_character_type() && r_type.is_character_type()) {
        //都是char大类型
      } else if (l_type.is_integer_type() && r_type.is_integer_type()) {
        //都是integer类型
      } else if (is_oracle_mode() && l_type.is_numeric_type() && r_type.is_numeric_type()) {
        //both numeric on oralce mode
      } else {
        //union all两边的类型不一样，不能强转
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "different types from different recursive cte union all branchs");
        LOG_WARN("different type in recursive cte not supported", K(ret));
      }
    }
  }
  return ret;
}

// checker is different between mysql and oracle mode
// oracle mode:
//   resolve path: from -> where -> connect by -> group by -> having -> select_items -> order by
//   so after group by, exprs in having, select items and order by must exists on group by exprs
// mysql mode
//   resolve path: from -> where -> select_items -> group by -> having -> order by
int ObSelectResolver::check_group_by()
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  CK(OB_NOT_NULL(select_stmt), OB_NOT_NULL(session_info_));
  CK(OB_NOT_NULL(select_stmt->get_query_ctx()));
  bool only_need_constraints = true;
  if (is_only_full_group_by_on(session_info_->get_sql_mode()) &&
      !select_stmt->get_query_ctx()->is_prepare_stmt()) {
    if (is_oracle_mode()) {
      only_need_constraints = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_group_expr_size(); i++) {
        ObRawExpr *group_by_expr = NULL;
        if (OB_ISNULL(group_by_expr = select_stmt->get_group_exprs().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group by expr is null", K(ret));
        } else if (ObUserDefinedSQLType == group_by_expr->get_data_type()) {
          ret = OB_ERR_NO_ORDER_MAP_SQL;
          LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
        } else if (ObLongTextType == group_by_expr->get_data_type()
                  || ObLobType == group_by_expr->get_data_type()
                  || ObJsonType == group_by_expr->get_data_type()
                  || ObGeometryType == group_by_expr->get_data_type()
                  || ObExtendType == group_by_expr->get_data_type()) {
          ret = (lib::is_oracle_mode() && ObJsonType == group_by_expr->get_data_type())
                  ? OB_ERR_INVALID_CMP_OP : OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("group by lob expr is not allowed", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_rollup_expr_size(); i++) {
        ObRawExpr *rollup_expr = NULL;
        if (OB_ISNULL(rollup_expr = select_stmt->get_rollup_exprs().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rollup expr is null", K(ret));
        } else if (ObLongTextType == rollup_expr->get_data_type()
                  || ObLobType == rollup_expr->get_data_type()
                  || ObExtendType == rollup_expr->get_data_type()
                  || ObUserDefinedSQLType == rollup_expr->get_data_type()) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("group by lob or udt expr is not allowed", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_grouping_sets_items_size(); i++) {
        const ObIArray<ObGroupbyExpr> &grouping_sets_exprs =
                                  select_stmt->get_grouping_sets_items().at(i).grouping_sets_exprs_;
        if (OB_FAIL(check_rollup_items_valid(
                                    select_stmt->get_grouping_sets_items().at(i).rollup_items_))) {
          LOG_WARN("failed to check rollup items valid", K(ret));
        } else if (OB_FAIL(check_cube_items_valid(
                                      select_stmt->get_grouping_sets_items().at(i).cube_items_))) {
          LOG_WARN("failed to check cube items valid", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < grouping_sets_exprs.count(); ++j) {
            const ObIArray<ObRawExpr*> &groupby_exprs = grouping_sets_exprs.at(j).groupby_exprs_;
            for (int64_t k = 0; OB_SUCC(ret) && k < groupby_exprs.count(); ++k) {
              ObRawExpr *groupby_expr = NULL;
              if (OB_ISNULL(groupby_expr = groupby_exprs.at(k))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("group by expr is null", K(ret));
              } else if (ObLongTextType == groupby_expr->get_data_type()
                        || ObLobType == groupby_expr->get_data_type()
                        || ObJsonType == groupby_expr->get_data_type()
                        || ObGeometryType == groupby_expr->get_data_type()
                        || ObExtendType == groupby_expr->get_data_type()
                        || ObUserDefinedSQLType == groupby_expr->get_data_type()) {
                ret = OB_ERR_INVALID_TYPE_FOR_OP;
                LOG_WARN("group by lob or udt expr is not allowed", K(ret));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(check_rollup_items_valid(select_stmt->get_rollup_items()))) {
          LOG_WARN("failed to check multi rollup items valid", K(ret));
        } else if (OB_FAIL(check_cube_items_valid(select_stmt->get_cube_items()))) {
          LOG_WARN("failed to check multi cube items valid", K(ret));
        } else {/*do nothing*/}
      }
    } else {
      //在解析过程中，standard group checker会记录需要检查的column和expr,在所有语句都解析完成后
      if (OB_FAIL(standard_group_checker_.check_only_full_group_by())) {
        LOG_WARN("failed to check group by");
      }
    }
  }

  if (OB_SUCC(ret)) {
    // skip add const constraint during prepare stage in PL
    const ParamStore *param_store = (NULL != params_.secondary_namespace_) ? NULL : params_.param_list_;
    if (OB_FAIL(ObGroupByChecker::check_group_by(param_store,
                                                 select_stmt,
                                                 having_has_self_column_,
                                                 has_group_by_clause(),
                                                 only_need_constraints))) {
      LOG_WARN("failed to check group by in oracle mode");
    }
  }

  // replace with same group by columns.
  // groupby之上的计算我们放在这里统一处理：
  // 1. select item/having/order item中的表达式树(子树)需要每个都在group by列中找到
  // 2. 递归查找是否在groupby列中，将在groupby的列的指针替换。
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTransformUtils::replace_stmt_expr_with_groupby_exprs(select_stmt, NULL))) {
      LOG_WARN("failed to replace stmt expr with groupby columns", K(ret));
    }
  }
  return ret;
}

// 1. lob or udt type can't be ordered
// 2. the order item should be exists in select items if has distinct
int ObSelectResolver::check_order_by()
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(ret));
  } else if (is_oracle_mode() && select_stmt->has_order_by()) {
    bool has_distinct = select_stmt->has_distinct();
    // If sql return single row, then don't check order by
    // eg: select distinct count(*) from t1 order by c1; -- return single row,then don't check
    bool need_check = !select_stmt->is_single_set_query();
    if (need_check) {
      // 1. check lob type or udt
      common::ObArray<ObRawExpr*> order_item_exprs;
      common::ObIArray<OrderItem> &order_items = select_stmt->get_order_items();
      // special case: select count(*) from t1 order by c1; --c1 is blob, but optimized to be remove
      for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
        if (ob_is_text_tc(order_items.at(i).expr_->get_data_type())
            || ob_is_lob_tc(order_items.at(i).expr_->get_data_type())
            || ob_is_json_tc(order_items.at(i).expr_->get_data_type())
            || ob_is_geometry_tc(order_items.at(i).expr_->get_data_type())
            || ob_is_extend(order_items.at(i).expr_->get_data_type())) {
          if (ob_is_xml_sql_type(order_items.at(i).expr_->get_data_type(), order_items.at(i).expr_->get_subschema_id())
              || ob_is_xml_pl_type(order_items.at(i).expr_->get_data_type(), order_items.at(i).expr_->get_udt_id())) {
            ret = OB_ERR_NO_ORDER_MAP_SQL;
            LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
          } else {
            ret = (lib::is_oracle_mode() && ob_is_json_tc(order_items.at(i).expr_->get_data_type()))
              ? OB_ERR_INVALID_CMP_OP : OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_WARN("lob or json expr can't order", K(ret), K(*order_items.at(i).expr_));
          }
          LOG_WARN("lob or json expr can't order", K(ret), K(*order_items.at(i).expr_));
        } else if (has_distinct) {
          if (OB_FAIL(order_item_exprs.push_back(order_items.at(i).expr_))) {
            LOG_WARN("fail to push back expr", K(ret));
          }
        }
      }
      // 2. check if has distinct
      if (OB_SUCC(ret) && has_distinct) {
        common::ObArray<ObRawExpr*> select_item_exprs;
        common::ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
        for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
          ObRawExpr *expr = select_items.at(i).expr_;
          if (OB_FAIL(select_item_exprs.push_back(expr))) {
              LOG_WARN("fail to push back expr", K(ret));
            // Support non-standard semantics in oracle
            //
          } else if (T_FUN_SYS_TO_CHAR == expr->get_expr_type() && expr->get_param_count() == 1) {
            if (OB_ISNULL(expr->get_param_expr(0))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("to_char has no child expr", K(ret));
            } else if (expr->get_param_expr(0)->get_result_type().is_number()) {
              if (OB_FAIL(select_item_exprs.push_back(expr->get_param_expr(0)))) {
                LOG_WARN("fail to push back expr", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          // skip add const constraint during prepare stage in PL
          const ParamStore *param_store = (NULL != params_.secondary_namespace_) ? NULL : params_.param_list_;
          if (OB_FAIL(ObGroupByChecker::check_by_expr(param_store,
                                                      select_stmt,
                                                      select_item_exprs,
                                                      order_item_exprs,
                                                      OB_ERR_NOT_SELECTED_EXPR,
                                                      true))) {
            LOG_WARN("fail to check order by", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObSelectResolver::search_connect_group_by_clause(const ParseNode &parent,
                                   const ParseNode *&start_with,
                                   const ParseNode *&connect_by,
                                   const ParseNode *&group_by,
                                   const ParseNode *&having)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
    start_with = NULL;
    connect_by = NULL;
    group_by = NULL;
    having = NULL;
    const ParseNode *node = NULL;
    for (int64_t i = PARSE_SELECT_DYNAMIC_SW_CBY;
         i <= PARSE_SELECT_DYNAMIC_HAVING && OB_SUCC(ret);
         i++) {
      if (NULL == (node = parent.children_[i])) {
        // do nothing
      } else {
        switch (node->type_) {
          case T_START_WITH : {
            start_with = node->children_[0];
            break;
          }
          case T_CONNECT_BY_CLAUSE : {
            connect_by = node;
            break;
          }
          case T_GROUPBY_CLAUSE : {
            group_by = node;
            break;
          }
          case T_HAVING : {
            having = node->children_[0];
            break;
          }
          default : {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("unexpected node type", K(ret), K(node->type_));
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(NULL == connect_by && NULL != start_with)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect connect by is null while start with is not null", K(ret));
    }
  } else {
    start_with = NULL;
    connect_by = NULL;
    group_by = parent.children_[PARSE_SELECT_DYNAMIC_GROUP];
    having = parent.children_[PARSE_SELECT_DYNAMIC_HAVING];
  }
  return ret;
}

int ObSelectResolver::check_and_mark_aggr_in_having_scope(ObSelectStmt *select_stmt) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_having_expr_size(); ++i) {
      ObRawExpr* expr = select_stmt->get_having_exprs().at(i);
      ObArray<ObAggFunRawExpr*> aggrs;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL ptr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::extract_aggr_expr(expr, aggrs))) {
        LOG_WARN("failed to extrace aggr expr", K(ret));
      } else {
        // having aggr must in inner stmt
        for (int64_t j = 0; OB_SUCC(ret) && j < aggrs.count(); ++j) {
          ObAggFunRawExpr* aggr_expr = aggrs.at(j);
          if (OB_ISNULL(aggr_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is NULL ptr", K(ret));
          } else if (aggr_expr->contain_nested_aggr()) {
            ret = OB_ERR_WRONG_FIELD_WITH_GROUP;
            LOG_USER_ERROR(OB_ERR_WRONG_FIELD_WITH_GROUP,
                           aggr_expr->get_expr_name().length(),
                           aggr_expr->get_expr_name().ptr());
          } else {
            aggr_expr->set_nested_aggr_inner_stmt(true);
          }
        }
      }
    }
  }
  return ret;
}

// block :
// 1 id and aggr(aggr(col)) in diff level
// select id from test group by id order by max(max(id));
// select id from test group by id order by max(max(data));
// select id, max(max(data)) from test group by id;
// select item must be outer
// select max(data) + 1 as data1 group by id order by max(data1);
int ObSelectResolver::check_aggr_in_select_scope(ObSelectStmt *select_stmt) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else {
    ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      ObArray<ObAggFunRawExpr*> aggrs;
      if (OB_ISNULL(select_items.at(i).expr_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr in select items.", K(ret));
        //compatible oracle: select 1, sum(max(c1)) from t1 group by c1;
      } else if (select_items.at(i).expr_->is_const_expr()) {
        //do nothing
      } else if (!select_items.at(i).expr_->has_flag(CNT_AGG)) {
        //in oracle it's "not a single-group group function."
        // select id, max(max(id))
        ret = OB_ERR_WRONG_FIELD_WITH_GROUP;
        ObString column_name = select_items.at(i).is_real_alias_ ?
              select_items.at(i).alias_name_ :
              select_items.at(i).expr_name_;
        LOG_USER_ERROR(OB_ERR_WRONG_FIELD_WITH_GROUP,
                      column_name.length(),
                      column_name.ptr());
      } else if (OB_FAIL(ObTransformUtils::extract_aggr_expr(select_items.at(i).expr_, aggrs))){
        LOG_WARN("failed to extrace aggr_expr", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < aggrs.count(); ++j) {
          ObAggFunRawExpr* aggr_expr = aggrs.at(j);
          if (OB_ISNULL(aggr_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is NULL ptr", K(ret));
          } else if (aggr_expr->in_inner_stmt()) {
            ret = OB_ERR_NOT_A_SINGLE_GROUP_FUNCTION;
            LOG_WARN("select in aggr alias can not be nested in aggr", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (select_stmt->get_group_expr_size() == 0 &&
          select_stmt->get_rollup_expr_size() == 0 &&
          select_stmt->get_grouping_sets_items_size() == 0 &&
          select_stmt->get_rollup_items_size() == 0 &&
          select_stmt->get_cube_items_size() == 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("nested group function without group by", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested group function without group by");
      }
    }
  }
  return ret;
}

int ObSelectResolver::mark_aggr_in_select_scope(ObSelectStmt *select_stmt) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else {
    ObRawExprCopier copier(*params_.expr_factory_);
    ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
    ObSEArray<ObAggFunRawExpr*, 4> origin_mark_inner_expr;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      ObArray<ObAggFunRawExpr*> aggrs;
      if (OB_ISNULL(select_items.at(i).expr_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr in select items.", K(ret));
      } else if (OB_FAIL(ObTransformUtils::extract_aggr_expr(select_items.at(i).expr_, aggrs))){
        LOG_WARN("failed to extrace aggr_eObIRawExprCopierxpr", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < aggrs.count(); ++j) {
          ObAggFunRawExpr* aggr_expr = aggrs.at(j);
          if (OB_ISNULL(aggr_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is NULL ptr", K(ret));
          } else if (aggr_expr->in_inner_stmt()) {
            // select max(id) + 1 from test group by id having max(id) = 1 order by max(id),max(max(data));
            // select sum(b),sum(b) + sum(c) as inn from t3 group by b,c having sum(b)+sum(c) > 1 order by 1,sum(b) + sum(sum(e + c));
            if(OB_FAIL(add_var_to_array_no_dup(origin_mark_inner_expr, aggr_expr))) {
              LOG_WARN("error to add aggr to array", K(ret));
            }
          } else {
            aggr_expr->set_nested_aggr_inner_stmt(false);
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < origin_mark_inner_expr.count(); ++i) {
      ObRawExpr *aggr_expr = origin_mark_inner_expr.at(i);
      ObRawExpr *aggr_expr_cp = NULL;
      if (OB_FAIL(ObRawExprCopier::copy_expr_node(*params_.expr_factory_, aggr_expr, aggr_expr_cp))) {
        LOG_WARN("failed to expr copy", K(ret));
      } else if (aggr_expr_cp == NULL) {
        LOG_WARN("unexpected null ptr", K(ret));
      } else {
        ObAggFunRawExpr* new_agg = static_cast<ObAggFunRawExpr*>(aggr_expr_cp);
        new_agg->set_nested_aggr_inner_stmt(false);
        if (OB_FAIL(copier.add_replaced_expr(aggr_expr, new_agg))) {
          LOG_WARN("failed to add replace expr", K(ret));
        } else if (OB_FAIL(select_stmt->add_agg_item(*new_agg))) {
          LOG_WARN("failed to add agg item", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      ObRawExpr* new_expr = NULL;
      if (OB_ISNULL(select_items.at(i).expr_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr in select items.", K(ret));
      } else if (OB_FAIL(copier.copy_on_replace(select_items.at(i).expr_, new_expr))) {
        LOG_WARN("failed to copy on replace the expr", K(ret));
      } else if (new_expr == NULL) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret));
      } else {
        select_items.at(i).expr_ = new_expr;
      }
    }
    ObIArray<OrderItem> &order_items = select_stmt->get_order_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
      ObRawExpr* new_expr = NULL;
      if (OB_ISNULL(order_items.at(i).expr_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr in order items.", K(ret));
      } else if (OB_FAIL(copier.copy_on_replace(order_items.at(i).expr_, new_expr))) {
        LOG_WARN("failed to copy on replace the expr", K(ret));
      } else if (new_expr == NULL) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret));
      } else {
        order_items.at(i).expr_ = new_expr;
      }
    }
  }
  return ret;
}

// if orderby has aggr(aggr) then orderby should be outer else it should be inner
// positive example following max(data) have to checked;
// select max(data) group by id order by max(max(data));
// select max(id) group by id order by max(max(id));
// negetive example following max(data) should not to be checked in inner stmt
// select id from test group by id having max(data) = 1 ordered by max(max(data))
int ObSelectResolver::mark_aggr_in_order_by_scope(ObSelectStmt *select_stmt) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else {
    ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
    ObSEArray<ObAggFunRawExpr*, 4> select_agg_expr;

    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      ObArray<ObAggFunRawExpr*> aggrs;
      if (OB_ISNULL(select_items.at(i).expr_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr in select items.", K(ret));
      } else if (OB_FAIL(ObTransformUtils::extract_aggr_expr(select_items.at(i).expr_, aggrs))){
        LOG_WARN("failed to extrace aggr_eObIRawExprCopierxpr", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < aggrs.count(); ++j) {
          ObAggFunRawExpr* aggr_expr = aggrs.at(j);
          if (OB_ISNULL(aggr_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null ptr", K(ret));
          } else if (OB_FAIL(add_var_to_array_no_dup(select_agg_expr, aggr_expr))) {
            LOG_WARN("error to add aggr to array", K(ret));
          }
        }
      }
    }

    ObIArray<OrderItem> &order_items = select_stmt->get_order_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
      ObArray<ObAggFunRawExpr*> aggrs;
      if (OB_ISNULL(order_items.at(i).expr_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid expr in select items.", K(ret));
        //compatible oracle: select 1, sum(max(c1)) from t1 group by c1;
      } else if (order_items.at(i).expr_->is_const_expr()) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::extract_aggr_expr(order_items.at(i).expr_, aggrs))) {
        LOG_WARN("invalid expr in extrace aggr expr", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < aggrs.count(); ++j) {
          ObAggFunRawExpr* aggr_expr = aggrs.at(j);
          if (OB_ISNULL(aggr_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is NULL ptr", K(ret));
          } else if (aggr_expr->contain_nested_aggr()) {
            aggr_expr->set_nested_aggr_inner_stmt(false);
          } else {
            if(!aggr_expr->in_inner_stmt()) {
              // there are two types of aggr in order by
              // 1 derive from the select -- outer
              // 2 derive from the having -- inner
              // 3 new appear in order by -- inner
              // select max(id) from test group by id order by 1,max(max(data));
              // select max(id) from test group by id order by max(id),max(max(data));
              // select sum(b) + sum(c) from t3 group by b,c having sum(b)+sum(c) > 1 order by 1,sum(b) + sum(sum(e + c));
              // select sum(b) + sum(c),sum(sum(b)) from t3 group by b,c having sum(b)+sum(c) > 1 order by 1,sum(b) + sum(sum(e + c));
              // In oracle next stmt can be compiled but running with error.
              // select sum(b) + sum(c) from t3 group by b,c having sum(b)+sum(c) > 1 order by 1,sum(b) + sum(sum(e + c)) + sum(e);
              if (!has_exist_in_array(select_agg_expr, aggr_expr)) {
                aggr_expr->set_nested_aggr_inner_stmt(true);
              }
            }
            // this branch means aggr_expr in_inner_stmt
            // select sub(c) from test group by b having sum(b) > 1 order by sum(b)
          }
        }
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_normal_query(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  // ObStmt *st = NULL;
  // ObSelectStmt *s_t = static_cast<ObSelectStmt *>(st);
  // used to record name win expr count
  int64_t count_name_win_expr = 0;
  const ParseNode *start_with = NULL;
  const ParseNode *connect_by = NULL;
  const ParseNode *group_by = NULL;
  const ParseNode *having = NULL;
  ObSelectStmt *select_stmt = get_select_stmt();
  bool has_rollup = false;
  CK(OB_NOT_NULL(select_stmt),
     OB_NOT_NULL(session_info_),
     OB_NOT_NULL(select_stmt->get_query_ctx()));

  set_in_exists_subquery(2 == parse_tree.value_);

  /**
   * @muhang.zb
   * 定义了一个cte，无论最终是否在主句使用，都必须要进行解析
   */
  OZ( resolve_with_clause(parse_tree.children_[PARSE_SELECT_WITH]) );

  /* normal select */
  if (OB_SUCC(ret)) {
    select_stmt->assign_set_op(ObSelectStmt::NONE);
  }
  OZ( resolve_query_options(parse_tree.children_[PARSE_SELECT_DISTINCT]) );
  if (OB_SUCC(ret) && is_only_full_group_by_on(session_info_->get_sql_mode())) {
    OZ( standard_group_checker_.init() );
  }
  OZ( search_connect_group_by_clause(parse_tree, start_with, connect_by, group_by, having) );
  if (OB_SUCC(ret) && OB_NOT_NULL(group_by)) {
    set_has_group_by_clause();
    OZ (check_rollup_clause(group_by, has_rollup));
  }
  if (OB_SUCC(ret) && (start_with != NULL || connect_by != NULL)) {
    select_stmt->set_hierarchical_query(true);
  }
  OZ( resolve_hints(parse_tree.children_[PARSE_SELECT_HINTS]) );
  /* resolve from clause */
  OZ( resolve_from_clause(parse_tree.children_[PARSE_SELECT_FROM]) );
  /* resolve start with clause */
  OZ( resolve_start_with_clause(start_with) );
  /* resolve connect by clause */
  OZ( resolve_connect_by_clause(connect_by) );
  /* resolve where clause */
  OZ( resolve_where_clause(parse_tree.children_[PARSE_SELECT_WHERE]) );

  if (OB_SUCC(ret) && !is_oracle_mode()) {
    /* resolve named window clause */
    OZ( resolve_named_windows_clause(parse_tree.children_[PARSE_SELECT_NAMED_WINDOWS]) );
  }
  /* resolve select clause */
  if (!is_oracle_mode()) {
    // mysql resolve: from->where->select_item->group by->having->order by
    count_name_win_expr = select_stmt->get_window_func_count();
    if (has_rollup) {
      expr_resv_ctx_.set_new_scope();
    }
    OZ( resolve_field_list(*(parse_tree.children_[PARSE_SELECT_SELECT])));
  }

  /* resolve group by clause */
  OZ( resolve_group_clause(group_by) );

  if (has_rollup && is_oracle_mode()) {
    expr_resv_ctx_.set_new_scope();
  }

  /* resolve having clause */
  OZ( resolve_having_clause(having) );

  if (is_oracle_mode()) {
    // oracle resolve: from->where->connect by->group by->having->select_item->order by
    OZ( resolve_field_list(*(parse_tree.children_[PARSE_SELECT_SELECT])) );
    if (OB_SUCC(ret)) {
      set_has_resolved_field_list(true);
    }
  }
  OZ( resolve_order_clause(parse_tree.children_[PARSE_SELECT_ORDER]) );
  OZ( resolve_limit_clause(parse_tree.children_[PARSE_SELECT_LIMIT]) );
  OZ( resolve_fetch_clause(parse_tree.children_[PARSE_SELECT_FETCH]) );
  OZ( resolve_check_option_clause(parse_tree.children_[PARSE_SELECT_WITH_CHECK_OPTION]) );
  OZ( resolve_into_clause(ObResolverUtils::get_select_into_node(parse_tree)) );
  OZ( resolve_for_update_clause(parse_tree.children_[PARSE_SELECT_FOR_UPD]) );

  if (OB_SUCC(ret)) {
    bool has_flashback_query = false;
    //select for update要求stmt中任何一处都不能出现flashback query相关属性
    if (select_stmt->has_for_update() &&
        OB_FAIL(check_stmt_has_flashback_query(select_stmt, true, has_flashback_query))) {
      LOG_WARN("failed to check stmt has flashback query", K(ret));
    } else if (has_flashback_query) {
      ret = OB_ERR_FLASHBACK_QUERY_WITH_UPDATE;
      LOG_WARN("select for update and flashback query exists", K(ret));
    }
  }

  if (OB_SUCC(ret) && has_top_limit_) {
    has_top_limit_ = false;
    select_stmt->set_has_top_limit(NULL != parse_tree.children_[PARSE_SELECT_LIMIT]);
  }

  //bug:
  //由于支持mysql模式下的name window,需要提前解析name window保存下来，然后再解析引用的win expr的表达式,当前实现
  //方式是保存在select stmt中,但是在全部解析完之后没有把那些name window的对应win_expr去除掉,导致生成的计划有问题
  //因此，这里在全部解析完stmt各个部分之后需要根据之前记录的name winexpr个数去除stmt中无用的name win expr
  if (OB_SUCC(ret) && count_name_win_expr > 0) {
    ObSEArray<ObWinFunRawExpr*, 4> new_win_func_exprs;
    for (int64_t i = count_name_win_expr;
          OB_SUCC(ret) && i < select_stmt->get_window_func_count();
          ++i) {
      if (OB_FAIL(new_win_func_exprs.push_back(select_stmt->get_window_func_expr(i)))) {
        LOG_WARN("failed to push back win exprs", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(select_stmt->get_window_func_exprs().assign(new_win_func_exprs))) {
        LOG_WARN("failed to assign win exprs", K(ret));
      } else { /*do nothing*/ }
    }
  }

  OZ( select_stmt->formalize_stmt(session_info_) );

  if (OB_SUCC(ret) && has_nested_aggr_) {
    if (OB_FAIL(check_aggr_in_select_scope(select_stmt))) {
      LOG_WARN("failed to check expr in select scope", K(ret));
    } else if (OB_FAIL(check_and_mark_aggr_in_having_scope(select_stmt))) {
      LOG_WARN("failed to check and mark the expr having", K(ret));
    } else if (OB_FAIL(mark_aggr_in_select_scope(select_stmt))) {
      LOG_WARN("failed to check and mark the expr select", K(ret));
    } else if (OB_FAIL(mark_aggr_in_order_by_scope(select_stmt))) {
      LOG_WARN("failed to check and mark the expr order", K(ret));
    }
  }

  //统一为本层的表达式进行only full group by验证，避免检查的逻辑过于分散
  OZ( check_group_by() );
  OZ( check_order_by() );
  OZ( check_pseudo_columns() );
  OZ( check_window_exprs() );
  OZ( check_sequence_exprs() );
  OZ( check_unsupported_operation_in_recursive_branch() );
  if (OB_SUCC(ret)) {
    //for topk, 在这里需要标示select 语句是否符合使用近似计算的要求，有group by，有order by且有limit
    //并且使用了topk的hint 且不是select for update 语句，且 from base table 且不需要calc found rows。
    //查询语句中没有distinct 且 查询中不涉及子查询
    //由于fetch clause利用limit设计，同时考虑到需要支持百分比及with ties功能，因此这里分配topn需要考虑这一情形
    if (select_stmt->get_query_ctx()->get_global_hint().is_topk_specified()
        && (1 == select_stmt->get_from_item_size())
        && (!select_stmt->is_calc_found_rows())
        && select_stmt->get_group_expr_size() > 0
        && !select_stmt->has_rollup()
        && !select_stmt->has_cube()
        && select_stmt->get_window_func_exprs().empty()
        && select_stmt->has_order_by()
        && !select_stmt->has_sequence()
        && !select_stmt->has_select_into()
        && (select_stmt->has_limit() && !select_stmt->is_fetch_with_ties() &&
            select_stmt->get_limit_percent_expr() == NULL)
        && (!select_stmt->has_distinct())
        && (!select_stmt->has_subquery())) {
      const FromItem from_item = select_stmt->get_from_item(0);
      TableItem *table_item = select_stmt->get_table_item_by_id(from_item.table_id_);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got table item is NULL", K(from_item), K(ret));
      } else if (table_item->is_basic_table() && (!table_item->for_update_)) {
        select_stmt->set_match_topk(true);
      } else {
        select_stmt->set_match_topk(false);
      }
    }
  }

  // rowscn伪列不能再flashback query和view中使用
  if (OB_SUCC(ret)) {
    bool has_ora_rowscn = false;
    const common::ObIArray<SelectItem> &items = select_stmt->get_select_items();
    for (int64_t i = 0; i < items.count(); i++) {
      const SelectItem item = items.at(i);
      if (nullptr != item.expr_
          && (item.expr_->has_flag(IS_ORA_ROWSCN_EXPR)
              || item.expr_->has_flag(CNT_ORA_ROWSCN_EXPR))) {
        has_ora_rowscn = true;
      }
    }

    if (has_ora_rowscn) {
      bool has_flashback_query = false;
      if (OB_FAIL(check_stmt_has_flashback_query(select_stmt, false, has_flashback_query))) {
        LOG_WARN("failed to check stmt has flashback query", K(ret));
      } else if (has_flashback_query) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "rowscn used with flashback query");
        LOG_WARN("rowscn can't use with flashback query", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  bool is_stack_overflow = false;
  if (NULL == (select_stmt = create_stmt<ObSelectStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create select stmt");
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else {
    if (OB_INVALID_TENANT_ID != params_.show_tenant_id_) {
      select_stmt->set_tenant_id(params_.show_tenant_id_);
    }
    select_stmt->set_show_seed(params_.show_seed_);
    /* -----------------------------------------------------------------
     * The later resolve may need some information resolved by the former one,
     * so please follow the resolving orders:
     *
     * 0. with clause
     * 1. set clause
     * 2. from clause
     * 3. start with clause
     * 4. connect by clause
     * 5. where clause
     * 6. select clause
     * 7. group by clause
     * 8. having clause
     * 9. order by clause
     * 10.limit clause
     * 11.fetch clause(oracle mode)
     * -----------------------------------------------------------------
     */

    // resolve outline data hint first
    if (OB_FAIL(resolve_outline_data_hints())) {
      LOG_WARN("resolve outline data hints failed", K(ret));
    } else if (parse_tree.children_[PARSE_SELECT_SET] != NULL) {
      /* resolve set clause */
      if (OB_FAIL(SMART_CALL(resolve_set_query(parse_tree)))) {
        LOG_WARN("resolve set query failed", K(ret));
      }
    } else {
      if (OB_FAIL(SMART_CALL(resolve_normal_query(parse_tree)))) {
        LOG_WARN("resolve normal query failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !is_oracle_mode() && !cte_ctx_.has_cte_param_list_) {
    cte_ctx_.cte_col_names_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); i++) {
      if (OB_FAIL(cte_ctx_.cte_col_names_.push_back(select_stmt->get_select_item(i).alias_name_))) {
        LOG_WARN("push back column alias name failed", K(ret));
      }
    }
  }

  return ret;
}

int ObSelectResolver::resolve_query_options(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  bool is_set_distinct = false;
  bool is_set_all = false;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("select stmt is null");
  } else if (NULL == node) {
    //nothing to do
  } else if (node->type_ != T_QEURY_EXPRESSION_LIST) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(node->type_));
  } else {
    ParseNode *option_node = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      option_node = node->children_[i];
      if (option_node->type_ == T_DISTINCT) {
        is_set_distinct = true;
      } else if (option_node->type_ == T_ALL) {
        is_set_all = true;
      } else if (option_node->type_ == T_FOUND_ROWS) {
        if (has_calc_found_rows_) {
          has_calc_found_rows_ = false;
          select_stmt->set_calc_found_rows(true);
        } else {
          ret = OB_ERR_CANT_USE_OPTION_HERE;
          LOG_USER_ERROR(OB_ERR_CANT_USE_OPTION_HERE, "SQL_CALC_FOUND_ROWS");
        }
      } else if (option_node->type_ == T_STRAIGHT_JOIN) {
        select_stmt->set_select_straight_join(true);
      }
    }
  }
  if (OB_SUCC(ret)) {
    //默认为all
    if (is_set_all && is_set_distinct) {
      ret = OB_ERR_WRONG_USAGE;
      LOG_USER_ERROR(OB_ERR_WRONG_USAGE, "ALL and DISTINCT");
    } else if (is_set_distinct) {
      select_stmt->assign_distinct();
    } else {
      select_stmt->assign_all();
    }
  }
  return ret;
}

int ObSelectResolver::resolve_for_update_clause(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (NULL == node) {
    // do nothing
  } else if (is_oracle_mode()) {
    OZ (resolve_for_update_clause_oracle(*node));
  } else {
    OZ (resolve_for_update_clause_mysql(*node));
  }
  return ret;
}

int ObSelectResolver::resolve_for_update_clause_mysql(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  int64_t wait_us = -1;
  if (OB_ISNULL(select_stmt = get_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get select stmt", K(ret));
  } else if (T_SFU_INT != node.type_ && T_SFU_DECIMAL != node.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("for update wait info is wrong", K(ret));
  } else if (T_SFU_INT == node.type_) {
    wait_us = node.value_ < 0 ? -1 : node.value_ * 1000000LL;
  } else if (T_SFU_DECIMAL == node.type_) {
    ObString time_str(node.str_len_, node.str_value_);
    if (OB_FAIL(ObTimeUtility2::str_to_time(
                  time_str, wait_us, ObTimeUtility2::DIGTS_SENSITIVE))) {
      LOG_WARN("str to time failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(set_for_update_mysql(*select_stmt, wait_us))) {
    LOG_WARN("failed to set for update", K(ret));
  }
  return ret;
}

int ObSelectResolver::set_for_update_mysql(ObSelectStmt &stmt, const int64_t wait_us)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < stmt.get_table_size(); ++idx) {
    if (OB_ISNULL(table_item = stmt.get_table_item(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Table item is NULL", K(ret));
    } else if (table_item->is_basic_table()) {
      table_item->for_update_ = true;
      table_item->for_update_wait_us_ = wait_us;
    } else if (table_item->is_link_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("mysql dblink not support select for update", K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::resolve_for_update_clause_oracle(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *stmt = NULL;
  const ParseNode *of_node = NULL;
  const ParseNode *wait_or_skip_node = NULL;
  int64_t wait_us = -1;
  bool skip_locked = false;
  current_scope_ = T_FIELD_LIST_SCOPE;
  if (current_level_ > 0) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("for update is not allowed in subquery", K(ret));
  } else if (OB_UNLIKELY(node.type_ != T_FOR_UPDATE) ||
             OB_UNLIKELY(node.num_child_ != 2) ||
             FALSE_IT(of_node = node.children_[0]) ||
             FALSE_IT(wait_or_skip_node = node.children_[1]) ||
             OB_ISNULL(stmt = get_select_stmt()) ||
             OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid for update parse node", K(ret), K(node.type_), K(node.num_child_), K(stmt));
  }

  if (OB_SUCC(ret)) {
    if (NULL != wait_or_skip_node) {
      if (wait_or_skip_node->type_ == T_SKIP_LOCKED) {
        // skip locked
        skip_locked = true;
        wait_us = 0;
      } else if (wait_or_skip_node->type_ == T_SFU_INT) {
        // wait INTNUM or no wait
        // nowait  wait_us = 0;
        wait_us = wait_or_skip_node->value_ * 1000000LL;
        skip_locked = false;
      } else if (wait_or_skip_node->type_ == T_SFU_DECIMAL) {
        // "select * from t1 for update wait 1.0;" is same as "wait 1"
        // "select * from t1 for update wait 1.5;" throw OB_ERR_REQUIRE_INTEGER
        ObNumber value;
        ObString time_str(wait_or_skip_node->str_len_, wait_or_skip_node->str_value_);
        if (OB_ISNULL(allocator_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("allocator is null", K(ret));
        } else if (OB_FAIL(value.from(wait_or_skip_node->str_value_,
                                      wait_or_skip_node->str_len_,
                                      *allocator_))) {
          LOG_WARN("from number failed", K(ret));
        } else if (OB_UNLIKELY(!value.is_valid_int())) {
          ret = OB_ERR_REQUIRE_INTEGER;
          LOG_WARN("wait time not a integer value", K(ret), K(value));
        } else if (OB_FAIL(ObTimeUtility2::str_to_time(
                      time_str, wait_us, ObTimeUtility2::DIGTS_SENSITIVE))) {
          LOG_WARN("str to time failed", K(ret));
        } else {
          skip_locked = false;
        }
      }
    } else {
      wait_us = -1;
      skip_locked = false;
    }
  }

  if (OB_SUCC(ret) && NULL != of_node) {
    for (int64_t i = 0; OB_SUCC(ret) && i < of_node->num_child_; ++i) {
      const ParseNode *column_node = NULL;
      ObRawExpr *expr = NULL;
      if (OB_ISNULL(column_node = of_node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("of node is null", K(ret));
      } else if (OB_FAIL(resolve_sql_expr(*column_node, expr))) {
        LOG_WARN("failed to resolve sql expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is invalid", K(ret), K(expr));
      } else if (is_oracle_mode() && expr->get_expr_type() == T_FUN_SYS_CALC_UROWID) {
        ret = OB_ERR_USE_ROWID_FOR_UPDATE;
        LOG_WARN("FOR UPDATE OF ROWID is illegal", K(ret), K(*expr));
      } else if (OB_UNLIKELY(!expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is invalid", K(ret), K(expr));
      } else {
        ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr*>(expr);
        if (OB_FAIL(set_for_update_oracle(*stmt, wait_us, skip_locked, col))) {
          LOG_WARN("failed to set for update table", K(ret));
        } else if (OB_FAIL(stmt->get_for_update_columns().push_back(col))) {
          LOG_WARN("failed to push back lock column", K(ret));
        }
      }
    }
  }

  //如果是PL里的可更新游标，增加rowid属性
  if (OB_SUCC(ret) && ((NULL != params_.secondary_namespace_ && params_.is_cursor_)
                       || (session_info_->is_client_return_rowid()
                           && NULL == params_.secondary_namespace_
                           && NULL == session_info_->get_pl_context()))) {
    SelectItem rowid_item;
    ObSelectStmt *select_stmt = stmt;
    TableItem *table_item = NULL;
    TableItem *add_rowid_table_item = NULL;
    const ObTableSchema *table_schema = NULL;
    int64_t for_update_cnt = 0;
    CK (OB_NOT_NULL(params_.schema_checker_));
    for (int64_t i = 0; OB_SUCC(ret) && for_update_cnt <= 1 && i < select_stmt->get_table_size(); i++) {
      // 1. 单表的for update, 需要把表的rowid加入到 select_item中
      // 2. 多表join for update, 不能加入rowid,因为不确定要加哪个表的rowid
      // 3. 多表join for update of t1.c1, of 后面指定更新的表时,需要把指定的表的rowid加入到select_item中
      CK (OB_NOT_NULL(table_item = select_stmt->get_table_item(i)));
      if (OB_SUCC(ret) && (table_item->for_update_ || 1 == select_stmt->get_table_size())) {
        for_update_cnt++;
        if (for_update_cnt > 1) {
          add_rowid_table_item = NULL;
        } else if (table_item->is_basic_table()) {
          OX (add_rowid_table_item = table_item);
        }
      }
    }
    if (OB_SUCC(ret) && add_rowid_table_item != NULL) {
      ObRawExpr *rowid_expr = NULL;
      OZ (params_.schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(), 
                                                    add_rowid_table_item->ref_id_,
                                                    table_schema,
                                                    add_rowid_table_item->is_link_table()));
      if (OB_FAIL(resolve_rowid_expr(select_stmt, *add_rowid_table_item, rowid_expr))) {
        LOG_WARN("resolve rowid expr failed", K(ret));
      } else {
        OX (rowid_item.expr_name_ = ObString(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME));
        OX (rowid_item.alias_name_ = ObString(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME));
        OX (rowid_item.expr_ = rowid_expr);
        OX (rowid_item.is_hidden_rowid_ = true);
        OZ (set_select_item(rowid_item, false/* is_auto_gen*/));
      }
    }
  }
  if (OB_SUCC(ret) && NULL == of_node) {
    // lock all tables
    if (OB_FAIL(set_for_update_oracle(*stmt, wait_us, skip_locked))) {
      LOG_WARN("failed to set for update", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (is_in_set_query() ||
        stmt->has_group_by() ||
        stmt->has_distinct()) {
      ret = OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED;
      LOG_WARN("for update can not exists in stmt with distinct, group", K(ret));
    } else if (stmt->has_fetch()) {//fetch 和 for update不能同时出现
      ret = OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT;
      LOG_WARN("for update stmt can't have fetch clause", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObSelectResolver::set_for_update_oracle
 * @param stmt: the targe stmt
 * @param wait_us: for update wait ts
 * @param skip_locked: skip locked
 * @param col: the column of table which should be locked,
 *             if col = NULL, all tables in the stmt should be locked
 * @return
 */
int ObSelectResolver::set_for_update_oracle(ObSelectStmt &stmt,
                                            const int64_t wait_us,
                                            bool skip_locked,
                                            ObColumnRefRawExpr *col)
{
  int ret = OB_SUCCESS;
  if (stmt.is_set_stmt()) {
    ret = OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT;
    LOG_WARN("invalid for update", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
    TableItem *table = NULL;
    if (OB_ISNULL(table = stmt.get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (col != NULL && col->get_table_id() != table->table_id_) {
      // does not lock this table, skip here
    } else {
      table->for_update_ = true;
      table->for_update_wait_us_ = wait_us;
      table->skip_locked_ = skip_locked;
      if (table->is_basic_table()) {
        ObSEArray<ObColumnRefRawExpr *, 4> rowkeys;
        if (OB_FAIL(add_all_rowkey_columns_to_stmt(*table, rowkeys, &stmt))) {
          LOG_WARN("failed to add rowkey columns to stmt", K(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < rowkeys.count(); ++j) {
          if (OB_ISNULL(rowkeys.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("rowkey expr is null", K(ret));
          } else {
            rowkeys.at(j)->set_explicited_reference();
          }
        }
      } else if (table->is_generated_table() || table->is_temp_table()) {
        ObSelectStmt *view = NULL;
        ObColumnRefRawExpr *view_col = NULL;
        if (OB_ISNULL(view = table->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("view is invalid", K(ret), K(*table));
        } else if (0 == view->get_table_size()) {
          // table is DUAL, does not need FOR UPDATE
          table->for_update_ = false;
        } else if (NULL != col) {
          int64_t sel_id = col->get_column_id() - OB_APP_MIN_COLUMN_ID;
          ObRawExpr *sel_expr = NULL;
          if (OB_UNLIKELY(sel_id < 0 || sel_id >= view->get_select_item_size()) ||
              OB_ISNULL(sel_expr = view->get_select_item(sel_id).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column id is invalid", K(ret), K(*col), K(sel_expr));
          } else if (OB_UNLIKELY(!sel_expr->is_column_ref_expr())) {
            ret = OB_ERR_FOR_UPDATE_EXPR_NOT_ALLOWED;
            LOG_WARN("invalid for update", K(ret));
          } else {
            view_col = static_cast<ObColumnRefRawExpr*>(sel_expr);
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(set_for_update_oracle(*view, wait_us, skip_locked, view_col))) {
          LOG_WARN("failed to set for update", K(ret));
        }
      }
    }
  }
  return ret;
}

// If expr is literal and type is numeric, then cast to int, find the referred select item
// special case:
// 1) negetive numeric value
//    select 1 from dual order by -0; --ok
//    select 1 from dual order by +0; --error
//    select 1 from dual order by -0E0;  --ok
//    select 1 from dual order by +0E0;  --error
// 2) value to floor
//    select c1 from t1 order by 1.3;  --ok
//    select c1 from t1 order by 1.9;  --ok
//    select c1 from t1 order by 2.3;  --error
//    select c1 from t1 order by 2.1;  --error
int ObSelectResolver::resolve_literal_order_item(const ParseNode &sort_node, ObRawExpr *expr, OrderItem &order_item, ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  if (!is_oracle_mode()) {
    // nothing to do
  } else if (T_OBJ_ACCESS_REF == sort_node.type_) {
    //do nothing
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_const_raw_expr()) {
    const ObObj &value = static_cast<ObConstRawExpr*>(expr)->get_value();
    if (!value.is_numeric_type()) {
      // not numeric type
    } else if (sort_node.str_len_ > 0 && '-' == sort_node.str_value_[0]) {
      // skip negative value, like -0, -0E0
      LOG_DEBUG("sort node is negative", K(ret), K(sort_node.str_value_[0]), K(sort_node.str_len_), K(value));
    } else {
      // 1. cast to number. if be cast to int type, it will be round, it's unexpected, like 1.9 round to 2
      // 2. get int value
      ObObjMeta number_type;
      number_type.set_number();
      number_type.set_scale(NUMBER_SCALE_UNKNOWN_YET);
      int64_t pos = -1;
      int64_t scale = -1;
      ObObj number_obj;
      number::ObNumber number_value;
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session_info_);
      ObCastCtx cast_ctx(allocator_, &dtc_params, CM_NONE, ObCharset::get_system_collation());
      if (OB_FAIL(ObObjCaster::to_type(number_type.get_type(), cast_ctx, value, number_obj))) {
        LOG_WARN("fail to cast int object", K(ret), K(value));
      } else if (OB_FAIL(number_obj.get_number(number_value))) {
        LOG_WARN("fail to get number value", K(ret), K(value));
      } else if (number_value.is_negative()) {
        // if value < 0, then don't cast to int value
        // eg: select * from t1 order by -0.1; --it's ok
        // but select * from t1 order by 0.1; --it's error
        LOG_DEBUG("const value is negative", K(number_value));
      } else if (!number_value.is_int_parts_valid_int64(pos, scale)) {
        ret = OB_ERR_ORDER_BY_ITEM_NOT_IN_SELECT_LIST;
        LOG_WARN("value is invalid", K(ret), K(number_value), K(value));
      } else if (OB_FAIL(resolve_order_item_by_pos(pos, order_item, select_stmt))) {
        LOG_WARN("fail to get order item", K(ret));
      }
      LOG_DEBUG("sort node", K(ret), K(sort_node.str_value_[0]), K(sort_node.str_len_), K(value));
    }
  }
  return ret;
}

int ObSelectResolver::resolve_order_item_by_pos(int64_t pos, OrderItem &order_item, ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  if (pos <= 0 || pos > select_stmt->get_select_item_size()) {
    // for SELECT statement, we need to make sure the column positions are valid
    if (is_oracle_mode()) {
      ret = OB_ERR_ORDER_BY_ITEM_NOT_IN_SELECT_LIST;
      LOG_WARN("ORDER BY item must be the number of a SELECT-list expression", K(ret), K(pos));
    } else {
      ret = OB_ERR_BAD_FIELD_ERROR;
      char buff[OB_MAX_ERROR_MSG_LEN];
      snprintf(buff, OB_MAX_ERROR_MSG_LEN, "%d", static_cast<int32_t>(pos));
      ObString scope_name = ObString::make_string(get_scope_name(current_scope_));
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, (int)strlen(buff), buff, scope_name.length(), scope_name.ptr());
    }
  } else {
    // create expression
    const SelectItem &select_item = select_stmt->get_select_item(pos - 1);
    order_item.expr_ = select_item.expr_;
  }
  return ret;
}

int ObSelectResolver::resolve_order_item(const ParseNode &sort_node, OrderItem &order_item)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  if (OB_ISNULL(select_stmt = get_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got an unexpected null", K(ret));
  } else if (OB_FAIL(ObResolverUtils::set_direction_by_mode(sort_node, order_item))) {
    LOG_WARN("failed to set order type by mode", K(ret));
  } else if (!is_oracle_mode()
             && OB_UNLIKELY(sort_node.children_[0]->type_ == T_INT && sort_node.children_[0]->value_ >= 0)) {
    // The order-by item is specified using column position
    // ie. ORDER BY 1 DESC
    int32_t pos = static_cast<int32_t>(sort_node.children_[0]->value_);
    if (OB_FAIL(resolve_order_item_by_pos(pos, order_item, select_stmt))) {
      LOG_WARN("fail to get order item", K(ret));
    }
  } else if (params_.is_prepare_protocol_ 
             && !params_.is_prepare_stage_
             && sort_node.children_[0]->type_ == T_QUESTIONMARK) {
    ObRawExpr *null_expr = NULL;
    if (OB_ISNULL(params_.expr_factory_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params is invalid", K(params_.expr_factory_));
    } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*params_.expr_factory_, null_expr))) {
      LOG_WARN("fail to build null expr", K(ret));
    } else {
      order_item.expr_ = null_expr;
    }
  } else if (T_QUESTIONMARK == sort_node.children_[0]->type_ && !params_.is_prepare_protocol_) {
    ret = OB_ERR_PARSE_SQL;
    LOG_WARN("'?' can't after 'order by", K(ret));
  } else {
    if (is_oracle_mode()) {
      field_list_first_ = true;
    }
    if (OB_FAIL(resolve_sql_expr(*(sort_node.children_[0]), order_item.expr_))) {
       LOG_WARN("resolve sql expression failed", K(ret));
    } else if (OB_FAIL(resolve_literal_order_item(*(sort_node.children_[0]), order_item.expr_, order_item, select_stmt))) {
      LOG_WARN("fail to resolve literal order item", K(ret), K(*order_item.expr_));
    } else if (OB_FAIL(resolve_shared_order_item(order_item, select_stmt))) {
      LOG_WARN("failed to resolve shared order item", K(ret));
    } else { }
  }
  if (OB_SUCC(ret) && is_only_full_group_by_on(session_info_->get_sql_mode())) {
    if (OB_FAIL(standard_group_checker_.add_unsettled_expr(order_item.expr_))) {
      LOG_WARN("add unsettled expr to standard group checker failed", K(ret));
    }
  }

  //oracle mode, check set query order by item
  if (OB_SUCC(ret) && select_stmt->is_set_stmt() && is_oracle_mode()) {
    ObSEArray<ObRawExpr*, 4> select_exprs;
    if (OB_FAIL(select_stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (ObOptimizerUtil::find_item(select_exprs, order_item.expr_)) {
      /*do nothing*/
    } else {
      ret = OB_ERR_ORDER_BY_ITEM_NOT_IN_SELECT_LIST;
      LOG_WARN("ORDER BY item must be the number of a SELECT-list expression", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(order_item.expr_) && order_item.expr_->has_flag(CNT_ASSIGN_EXPR)) {
    LOG_USER_WARN(OB_ERR_DEPRECATED_SYNTAX, "Setting user variables within expressions",
      "SET variable=expression, ... or SELECT expression(s) INTO variables(s)");
    if (OB_NOT_NULL(session_info_) && OB_NOT_NULL(session_info_->get_cur_exec_ctx()) &&
        OB_NOT_NULL(session_info_->get_cur_exec_ctx()->get_sql_ctx())) {
      const ObSqlCtx *sql_ctx = session_info_->get_cur_exec_ctx()->get_sql_ctx();
      LOG_ERROR("Variable assignment in order by items will cause uncertain behavior",
                K(ObString(sql_ctx->sql_id_)));
    }
  }
  return ret;
}

int ObSelectResolver::resolve_field_list(const ParseNode &node)
{
  int ret = OB_SUCCESS;
  ParseNode *project_node = NULL;
  ParseNode *alias_node = NULL;
  bool is_bald_star = false;
  ObSelectStmt *select_stmt = NULL;
  bool enable_modify_null_name = false;
  //LOG_INFO("resolve_select_1", "usec", ObSQLUtils::get_usec());
  current_scope_ = T_FIELD_LIST_SCOPE;
  if (OB_ISNULL(session_info_) || OB_ISNULL(select_stmt = get_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(session_info_),
        K(select_stmt), K(ret));
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
    alias_node = NULL;
    project_node = NULL;
    SelectItem select_item;
    // Rule for expr_name and alias_name
    // expr_name is filled with expr name, both ref_expr and other complex expr.
    // alias_name is filled with expr_name for default, but updated with real alias name if exists.
    // the special case is the column ref, which expr_name will be replaced with unqualified column name.
    select_item.expr_name_.assign_ptr(node.children_[i]->str_value_,
                                      static_cast<int32_t>(node.children_[i]->str_len_));
    // In Oracle ps mode, the alias name of the bind variable is ":" + num, not the actual value. As follows:
    // PREPARE STMT FROM 'SELECT ?, ?, ? FROM DUAL';
    // SET @I1 = 1;
    // EXECUTE STMT USING @I1, @I1, @I1;
    // col_name1 is ":1", col_name2 is ":2", col_name3 is ":3"
    if (is_oracle_mode()
        && params_.is_prepare_protocol_
        && 1 == node.children_[i]->num_child_
        && T_QUESTIONMARK == node.children_[i]->children_[0]->type_) {
      ObString alias_name;
      char temp_str_buf[OB_MAX_COLUMN_NAME_LENGTH];
      if (snprintf(temp_str_buf, sizeof(temp_str_buf), ":%ld", node.children_[i]->children_[0]->value_ + 1) < 0) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("failed to generate buffer for temp_str_buf", K(ret));
      }
      if (OB_SUCC(ret)) {
        alias_name = ObString::make_string(temp_str_buf);
        if (OB_FAIL(ob_write_string(*allocator_, alias_name, select_item.alias_name_))) {
          LOG_WARN("Can not malloc space for alias name", K(ret));
        }
      }
    } else if (OB_FAIL(session_info_->check_feature_enable(ObCompatFeatureType::PROJECT_NULL,
                                                           enable_modify_null_name))) {
      LOG_WARN("failed to check feature enable", K(ret));
    } else if (is_mysql_mode() && node.children_[i]->children_[0]->type_ == T_NULL &&
               enable_modify_null_name) {
      // MySQL sets the alias of standalone null value("\N","null"...) to "NULL" during projection.
      // Note: when null value is in a composite expression, its alias is not modified.
      ObString alias_name = ObString::make_string("NULL");
      if (OB_FAIL(ob_write_string(*allocator_, alias_name, select_item.alias_name_))) {
        LOG_WARN("Can not malloc space for alias name", K(ret));
      }
    } else {
      select_item.alias_name_.assign_ptr(node.children_[i]->str_value_,
                                      static_cast<int32_t>(node.children_[i]->str_len_));
    }

    project_node = node.children_[i]->children_[0];
    if (project_node->type_ == T_STAR
        || (project_node->type_ == T_COLUMN_REF && project_node->children_[2]->type_ == T_STAR)) {
      if (project_node->type_ == T_STAR) {
        if (is_bald_star) {
          ret = OB_ERR_STAR_DUPLICATE;
          LOG_WARN("Wrong usage of '*'");
          break;
        } else {
          is_bald_star = true;
        }
      }
      //oracle不允许有同表名的基表时select item引用其列,比如:
      //select * from t1,t1 ==> NO
      //select 1 from t1,t1 ==> YES
      //select * from (select * from t1), (select * from t1) ==> YES
      if (OB_FAIL(ret)) {
      } else if (params_.have_same_table_name_) {
        ret = OB_NON_UNIQ_ERROR;
        LOG_WARN("column in all tables is ambiguous", K(ret));
      } else if (OB_FAIL(resolve_star(project_node))) {
        LOG_WARN("resolve star failed", K(ret));
      }
      continue;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(*allocator_,
                                             session_info_->get_dtc_params(),
                                             select_item.expr_name_,
                                             ObCharset::REPLACE_UNKNOWN_CHARACTER))) {
        LOG_WARN("fail to charset convert", K(ret));
      } else if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(*allocator_,
                                             session_info_->get_dtc_params(),
                                             select_item.alias_name_,
                                             ObCharset::REPLACE_UNKNOWN_CHARACTER))) {
        LOG_WARN("fail to charset convert", K(ret));
      }
    }
    bool is_auto_gen = false;
    if (OB_SUCC(ret)) {
      //处理alias
      ObCollationType cs_type = CS_TYPE_INVALID;
      if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
        LOG_WARN("fail to get collation_connection", K(ret));
      } else if (project_node->type_ == T_ALIAS) {
        alias_node = project_node->children_[1];
        project_node = project_node->children_[0];
        select_item.is_real_alias_ = true;
        /* check if the alias name is legal */
        select_item.alias_name_.assign_ptr(const_cast<char *>(alias_node->str_value_),
                                           static_cast<int32_t>(alias_node->str_len_));
        if (OB_UNLIKELY(alias_node->str_len_ > OB_MAX_COLUMN_NAME_LENGTH && is_oracle_mode())) {
          ret = OB_ERR_TOO_LONG_IDENT;
          LOG_WARN("alias name too long", K(ret), K(select_item.alias_name_));
        }
      } else if (OB_UNLIKELY((params_.is_from_create_view_ || params_.is_from_create_table_)
                             && !params_.is_specified_col_name_
                             && (lib::is_oracle_mode() && 0 == select_item.expr_name_.case_compare(
                                                           OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME)))) {
        // must name alias for rowid
        //  eg: create view/table as select rowid from t1;
        ret = OB_NO_COLUMN_ALIAS;
        LOG_WARN("no column alias for rowid pseudo column", K(ret));
        LOG_USER_ERROR(OB_NO_COLUMN_ALIAS, select_item.expr_name_.length(),
                       select_item.expr_name_.ptr());
      }
    }

    if (OB_SUCC(ret)) {
      ObRawExpr *sel_expr = NULL;
      if (OB_FAIL(resolve_sql_expr(*project_node, select_item.expr_))) {
        LOG_WARN("resolve sql expr failed", K(ret));
        if (OB_EER_WINDOW_NO_REDEFINE_ORDER_BY == ret
            && OB_NOT_NULL(project_node)
            && OB_NOT_NULL(project_node->children_[1])
            && OB_NOT_NULL(project_node->children_[1]->children_[0])) {
          LOG_USER_ERROR(OB_EER_WINDOW_NO_REDEFINE_ORDER_BY,
              (int)strlen("<unnamed window>"), "<unnamed window>",
              (int)(project_node->children_[1]->children_[0]->str_len_),
              project_node->children_[1]->children_[0]->str_value_);
        }
      } else if (OB_ISNULL(sel_expr = select_item.expr_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("select expr is null", K(select_item), K(ret));
      } else if (sel_expr->is_exec_param_expr()) {
        sel_expr = static_cast<ObExecParamRawExpr *>(sel_expr)->get_ref_expr();
        if (OB_ISNULL(sel_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("select expr is null", K(ret));
        }
      }

      if (OB_SUCC(ret) && NULL == alias_node) {
        LOG_DEBUG("select item info", K(select_item));
        if (sel_expr->is_column_ref_expr()) {
          // for t1.c1, extract the exact column name of c1 for searching in resolve_columns
          if (project_node->type_ == T_COLUMN_REF) {
            alias_node = project_node->children_[2];
            select_item.alias_name_.assign_ptr(const_cast<char *>(alias_node->str_value_),
                                               static_cast<int32_t>(alias_node->str_len_));
          } else if (T_OBJ_ACCESS_REF == project_node->type_) {
            while (NULL != project_node->children_[1]) {
              project_node = project_node->children_[1];
            }
            if (T_OBJ_ACCESS_REF != project_node->type_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected select item type", K(select_item), K(ret));
            } else {
              alias_node = project_node->children_[0];
              /* bugfix: table has fbi index, select fbi_expr, alias name is empty
              create table t1(c1 number,c2 number);
              create index t1_g_idx on t1(ceil(c1)) global;
              select ceil(c1),ceil(c2) from t1;
              +------+----------+
              |      | CEIL(C2) |   -----》 not display CEIL(C1) COLUM NAME
              +------+----------+
              |    4 |        5 |
              +------+----------+
              */
              if (alias_node->str_len_ > 0) {
                select_item.alias_name_.assign_ptr(const_cast<char *>(alias_node->str_value_),
                                                   static_cast<int32_t>(alias_node->str_len_));
              }
            }
          } else if (T_REF_COLUMN == sel_expr->get_expr_type()) {
            // deal with generated column
            ObColumnRefRawExpr *ref_expr = dynamic_cast<ObColumnRefRawExpr *>(sel_expr);
            if (OB_ISNULL(ref_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("got an unexpected null", K(ret));
            } else {
              select_item.alias_name_ = ref_expr->get_column_name();
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected select item type", K(select_item), K(ret));
          }
        } else if (T_FUN_SYS_SEQ_NEXTVAL == sel_expr->get_expr_type()) {
          // sequence expr, expr is seq_name.nextval or seq_name.currval
          // but column name displayed should be nextval or currval
          do {
            if (T_OP_POS == project_node->type_) {
              project_node = project_node->children_[0];
            } else if (T_EXPR_LIST == project_node->type_) {
              project_node = project_node->children_[0];
            } else {
              break;
            }
          } while (true);
          if (T_REMOTE_SEQUENCE == project_node->type_) {
            if (project_node->num_child_ < 3) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected select item type",
                       K(select_item), K(project_node->type_), K(project_node->num_child_), K(ret));
            } else {
              alias_node = project_node->children_[2];
              select_item.alias_name_.assign_ptr(const_cast<char *>(alias_node->str_value_),
                                                 static_cast<int32_t>(alias_node->str_len_));
            }
          } else if (is_oracle_mode()) {
            if (T_OBJ_ACCESS_REF != project_node->type_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected select item type", K(select_item), K(project_node->type_), K(ret));
            }
            while (OB_SUCC(ret) && NULL != project_node->children_[1]) {
              project_node = project_node->children_[1];
            }
            if (OB_FAIL(ret)) {
              // do nothing
            } else if (T_OBJ_ACCESS_REF != project_node->type_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected select item type", K(select_item), K(project_node->type_), K(ret));
            } else {
              alias_node = project_node->children_[0];
              select_item.alias_name_.assign_ptr(const_cast<char *>(alias_node->str_value_),
                                                 static_cast<int32_t>(alias_node->str_len_));
            }
          } else {
            // mysql mode
            if (T_COLUMN_REF != project_node->type_ || project_node->num_child_ < 3) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected select item type",
                       K(select_item), K(project_node->type_), K(project_node->num_child_), K(ret));
            } else {
              alias_node = project_node->children_[2];
              select_item.alias_name_.assign_ptr(const_cast<char *>(alias_node->str_value_),
                                                 static_cast<int32_t>(alias_node->str_len_));
            }
          }
        } else if (T_FUN_SYS_CALC_UROWID == sel_expr->get_expr_type()) {
          ObString rowid_name(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME);
          if (OB_FAIL(ob_write_string(*allocator_, rowid_name, select_item.alias_name_))) {
            LOG_WARN("failed to ob write string", K(ret));
          }
        } else if (T_FUN_SYS_NAME_CONST == sel_expr->get_expr_type()) {
          const ParseNode *expr_list_node = project_node->children_[1];
          const ObRawExpr *name_expr = nullptr;
          if (2 != expr_list_node->num_child_) {
            //do nothing
          } else if (OB_ISNULL(name_expr = sel_expr->get_param_expr(0))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret), K(name_expr));
          } else if (name_expr->is_const_raw_expr() && T_QUESTIONMARK != name_expr->get_expr_type()) {
            char buf[OB_MAX_ALIAS_NAME_LENGTH + 1];
            int64_t pos = 0;
            const ObObj &value = static_cast<const ObConstRawExpr*>(name_expr)->get_value();
            if (value.is_numeric_type()){
              const ParseNode *alias_node = expr_list_node->children_[0];
              if (OB_ISNULL(alias_node)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected null", K(ret));
              } else if (alias_node->str_len_ > 0 && '-' == alias_node->str_value_[0]) {
                ret = OB_INVALID_ARGUMENT;
                LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_NAME_CONST);
                LOG_WARN("the first param of name_const can't be negtive", K(ret));
              } else if (OB_FAIL(value.print_sql_literal(buf, OB_MAX_ALIAS_NAME_LENGTH + 1, pos))) {
                LOG_WARN("fail to print_sql_literal", K(value), K(ret));
              }
            } else if (value.is_string_type()) {
              const ParseNode *alias_node = expr_list_node->children_[0];
              if (OB_ISNULL(alias_node)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected null", K(ret));
              } else if ((pos = alias_node->str_len_) > 0) {
                if (pos > OB_MAX_ALIAS_NAME_LENGTH + 1) {
                  pos = OB_MAX_ALIAS_NAME_LENGTH + 1;
                }
                MEMCPY(buf, alias_node->str_value_, pos);
              }
            } else if (value.is_temporal_type()) {
              char time_buf[31];
              if (OB_FAIL(value.print_sql_literal(time_buf, OB_MAX_ALIAS_NAME_LENGTH + 1, pos))) {
                LOG_WARN("fail to print_plain_str_literal", K(value), K(ret));
              } else if (pos < 2 || pos > 31) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("length of time sting is not valid", K(ret));
              } else {
                MEMCPY(buf, time_buf + 1, pos - 2);
                pos = pos - 2;
              }
            } else if (OB_FAIL(value.print_sql_literal(buf, OB_MAX_ALIAS_NAME_LENGTH + 1, pos))) {
              LOG_WARN("fail to print_sql_literal", K(value), K(ret));
            }
            if (OB_SUCC(ret)) {
              ObString name_str(pos, buf);
              if (OB_FAIL(ObSQLUtils::make_field_name(name_str.ptr(),
                                                      static_cast<const int64_t>(name_str.length()),
                                                      CS_TYPE_UTF8MB4_GENERAL_CI,
                                                      allocator_,
                                                      name_str))) {
                LOG_WARN("fail to copy column alias name", K(name_str), K(ret));
              } else if (OB_FAIL(ob_write_string(*allocator_, name_str, select_item.alias_name_))) {
                LOG_WARN("failed to ob write string", K(ret));
              }
            }
          } else if (T_FUN_SYS_VERSION == name_expr->get_expr_type()) {
            ObString version;
            if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_VERSION, version))) {
              LOG_WARN("fail to get version", K(ret));
            } else if (OB_FAIL(ob_write_string(*allocator_, version, select_item.alias_name_))) {
              LOG_WARN("failed to ob write string", K(ret));
            }
          } else if (T_FUN_SYS_OB_VERSION == name_expr->get_expr_type()) {
            if (OB_FAIL(ob_write_string(*allocator_, common::ObString(PACKAGE_VERSION), select_item.alias_name_))) {
              LOG_WARN("failed to ob write string", K(ret));
            }
          } else if (T_FUN_SYS_ICU_VERSION == name_expr->get_expr_type()) {
            if (OB_FAIL(ob_write_string(*allocator_,
                                        common::ObString(ObExprRegexContext::icu_version_string()),
                                        select_item.alias_name_))) {
              LOG_WARN("failed to ob write string", K(ret));
            }
          } else {
            //invalid name, do nothing
          }
        } else if (is_oracle_mode()
                    && T_QUESTIONMARK == sel_expr->get_expr_type()
                    && is_colum_without_alias(project_node)) {
          while (OB_SUCC(ret) && NULL != project_node->children_[1]) {
            project_node = project_node->children_[1];
          }
          if (OB_FAIL(ret)) {
          } else if (T_OBJ_ACCESS_REF != project_node->type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected select item type", K(select_item), K(project_node->type_), K(ret));
          } else {
            alias_node = project_node->children_[0];
            select_item.alias_name_.assign_ptr(const_cast<char *>(alias_node->str_value_),
                                                static_cast<int32_t>(alias_node->str_len_));
            if (OB_UNLIKELY(alias_node->str_len_ > OB_MAX_COLUMN_NAME_LENGTH)) {
              ret = OB_ERR_TOO_LONG_IDENT;
              LOG_WARN("alias name too long", K(ret), K(select_item.alias_name_));
            }
          }
        } else if (is_oracle_mode() && 0 == select_stmt->get_table_size()
                   && (0 == select_item.expr_name_.case_compare("\"DUMMY\"")
                       || 0 == select_item.expr_name_.case_compare("DUMMY"))) {
          const char *ptr_name = "DUMMY";
          ObString string_name(ptr_name);
          select_item.alias_name_ = string_name;
          select_item.is_real_alias_ = true;
        } else if (T_FUN_SYS_JSON_QUERY == sel_expr->get_expr_type()
            && OB_FAIL(add_alias_from_dot_notation(sel_expr, select_item))) {  // deal dot notation without alias
          LOG_WARN("fail to resolve alias in dot notation", K(ret));
        } else {
          if (params_.is_prepare_protocol_
              || !session_info_->get_local_ob_enable_plan_cache()
              || 0 == node.children_[i]->is_val_paramed_item_idx_) {
            // ps 不参数化列; plan cache关闭后不参数化列
            // do nothing
          } else if (OB_ISNULL(params_.select_item_param_infos_) ||
                     node.children_[i]->value_ >= params_.select_item_param_infos_->count()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(params_.select_item_param_infos_));
          } else {
            int64_t idx = node.children_[i]->value_;
            const SelectItemParamInfo &param_info = params_.select_item_param_infos_->at(idx);
            select_item.paramed_alias_name_.assign_ptr(param_info.paramed_field_name_, param_info.name_len_);
            if (OB_FAIL(select_item.questions_pos_.assign(param_info.questions_pos_))) {
              LOG_WARN("failed to assign array", K(ret));
            } else if (OB_FAIL(select_item.params_idx_.assign(param_info.params_idx_))) {
              LOG_WARN("failed to assign array", K(ret));
            } else {
              select_item.esc_str_flag_ = param_info.esc_str_flag_;
              select_item.neg_param_idx_ = param_info.neg_params_idx_;

              LOG_DEBUG("select item param info",
                        K(select_item.alias_name_), K(select_item.params_idx_),
                        K(select_item.paramed_alias_name_),
                        K(select_item.esc_str_flag_), K(select_item.questions_pos_),
                        K(select_item), K(((ObDMLStmt *)stmt_)->get_column_items()));
            }
          }
          is_auto_gen = true;
        }
      }

      if (OB_SUCC(ret) && is_oracle_mode() && T_FUN_SYS_XMLSEQUENCE == sel_expr->get_expr_type()) {
        // Currently, xmlsequence is not supported in the select clause.
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("xmlsequence in select clause is not supported", K(ret));
      }

      if (OB_SUCC(ret) && is_oracle_mode() && sel_expr->has_flag(CNT_SUB_QUERY)) {
        if (OB_FAIL(check_subquery_return_one_column(*sel_expr))) {
          LOG_WARN("failed to check subquery return one column", K(ret));
        }
      }

      // 如果 select field list 表达式中包含了 sequence，则需要判断当前
      // select 语句是否为子查询，或者是否为 set 语句
      // **例外**：insert into select xxxx 的情景下，允许 sequence
      if (OB_SUCC(ret) && sel_expr->has_flag(CNT_SEQ_EXPR)) {
        if (in_set_query_ ||
            (params_.resolver_scope_stmt_type_ == ObItemType::T_INSERT  && current_level_ != 0) ||
            (params_.resolver_scope_stmt_type_ != ObItemType::T_INSERT  && (current_level_ > 1 || is_substmt()))) {
          // 对于 from (select xxxx) a 的场景，a 这个子查询的 current_level_ 和上一级相同，
          // 但设置了 parent namespace，故而还是可以加以区分
          ret = OB_ERR_SEQ_NOT_ALLOWED_HERE;
        }
      }
    }

    // for unqualified column, if current stmt exists joined table with using,
    // it's determined by join type.
    // select c1 from t1 left join t2 using(c1) right join t3 using(c1) => c1 is t3.c1
    // select t2.c1 from t1 left join t2 using(c1) right join t3 using(c1) => c1 is t2.c1
    // select c1 from t1 left join t2 using(c1) right join t3 using(c1),
    //                t3 t left join t4 using(c1) right join t5 using(c1); ==> ambiguious
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(set_select_item(select_item, is_auto_gen))) {
      // construct select item from select_expr
      LOG_WARN("set select item failed", K(ret));
    } else if (is_only_full_group_by_on(session_info_->get_sql_mode())) {
      if (OB_FAIL(standard_group_checker_.add_unsettled_expr(select_item.expr_))) {
        LOG_WARN("add unsettled expr to standard group checker failed", K(ret));
      }
    }

    // for oracle mode, check grouping here
    if (OB_FAIL(ret) || !is_oracle_mode()) {
      /*do nothing*/
    } else if (OB_ISNULL(select_item.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(recursive_check_grouping_columns(select_stmt, select_item.expr_, false))) {
      LOG_WARN("failed to recursive check grouping columns", K(ret));
    } else {/*do nothing*/}
  } // end for

  if (OB_SUCC(ret)) {
    // Oracle mode, * can't be used with other expresion
    // like: select 1,* from t1; select *,1 from t1;
    if (is_oracle_mode() && is_bald_star && node.num_child_ > 1) {
      ret = OB_ERR_PARSE_SQL;
      LOG_WARN("star can't be used with other expression", K(ret));
    }
  }
  return ret;
}

inline bool ObSelectResolver::is_colum_without_alias(ParseNode *project_node) {
  bool bret = OB_NOT_NULL(project_node);
  ParseNode *cur_node = project_node;
  while (bret && OB_NOT_NULL(cur_node)) {
    if (T_OBJ_ACCESS_REF != cur_node->type_
         || OB_ISNULL(cur_node->children_[0])
         || T_IDENT != cur_node->children_[0]->type_) {
      bret = false;
    } else {
      cur_node = cur_node->children_[1];
    }
  }
  return bret;
}

int ObSelectResolver::add_alias_from_dot_notation(ObRawExpr *sel_expr, SelectItem& select_item)
{
  INIT_SUCC(ret);
  int64_t pos = -1;
  int64_t len = 0;
  ObString path_str;
  ObConstRawExpr* path_expr = NULL;
  // whether is dot notation
  if (OB_NOT_NULL(sel_expr->get_param_expr(JSN_QUE_MISMATCH))
      && JSN_QUERY_MISMATCH_DOT == static_cast<ObConstRawExpr*>(sel_expr->get_param_expr(JSN_QUE_MISMATCH))->get_value().get_int()) {
    if (!select_item.alias_name_.empty()) {
      select_item.is_real_alias_ = true;
    } else if (OB_NOT_NULL(sel_expr->get_param_expr(JSN_QUE_PATH))
               && T_CHAR == sel_expr->get_param_expr(JSN_QUE_PATH)->get_expr_type()) {
      path_expr = static_cast<ObConstRawExpr*>(sel_expr->get_param_expr(JSN_QUE_PATH));
      path_str = path_expr->get_value().get_string();
      pos = path_str.length() - 1;
      len = 0;
      char *buf = NULL;
      while (pos >= 0 && path_str[pos] != '.') {
        pos --;
      }
      len = path_str.length() - (pos + 1);
      if (pos < 0) {
      } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(len)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(buf));
      } else {
        MEMCPY(buf, path_str.ptr() + (pos + 1), len);
        ObString alias_name(len, buf);
        select_item.alias_name_ = alias_name;
        select_item.is_real_alias_ = true;
      }
    }
  }
  return ret;
}

int ObSelectResolver::expand_target_list(
  const TableItem &table_item, ObIArray<SelectItem> &target_list)
{
  int ret = OB_SUCCESS;
  ObArray<ColumnItem> column_items;
  if (table_item.is_basic_table() || table_item.is_link_table()) {
    if (OB_FAIL(resolve_all_basic_table_columns(table_item, false, &column_items))) {
      LOG_WARN("resolve all basic table columns failed", K(ret), K(table_item));
    }
  } else if (table_item.is_generated_table() ||
             table_item.is_temp_table() ||
             table_item.is_lateral_table()) {
    if (OB_FAIL(resolve_all_generated_table_columns(table_item, &column_items))) {
      LOG_WARN("resolve all generated table columns failed", K(ret));
    }
  } else if (table_item.is_fake_cte_table()) {
    if (OB_FAIL(resolve_all_fake_cte_table_columns(table_item, &column_items))) {
      LOG_WARN("resolve fake cte table failed", K(ret));
    }
  } else if (table_item.is_function_table()) {
    if (OB_FAIL(resolve_all_function_table_columns(table_item, &column_items))) {
      LOG_WARN("resolve function table columns failed", K(ret));
    }
  } else if (table_item.is_json_table()) {
    if (OB_FAIL(resolve_all_json_table_columns(table_item, &column_items))) {
      LOG_WARN("resolve function table columns failed", K(ret));
    }
  } else if (table_item.is_values_table()) {
    if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(get_stmt()->get_column_size() == 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), KPC(get_stmt()));
    } else if (OB_FAIL(append(column_items, get_stmt()->get_column_items()))) {
      LOG_WARN("failed to append", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table type", K_(table_item.type), K(ret));
  }

  const bool is_child_unpivot_select = (NULL != table_item.ref_query_
                                        && table_item.ref_query_->is_unpivot_select());
  LOG_DEBUG("do expand_target_list", K(is_child_unpivot_select), KPC(table_item.ref_query_));
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
    const ColumnItem &col_item = column_items.at(i);
    SelectItem tmp_select_item;
    if (table_item.is_generated_table() || table_item.is_temp_table()) {
      if (OB_ISNULL(table_item.ref_query_) || i >= table_item.ref_query_->get_select_item_size()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(table_item.ref_query_),
                 K(i), K(table_item.ref_query_->get_select_item_size()));
      } else {
        const SelectItem &select_item = table_item.ref_query_->get_select_item(i);
        if (is_child_unpivot_select && select_item.is_unpivot_mocked_column_) {
          LOG_DEBUG("continue", K(select_item));
          continue;
        } else {
          tmp_select_item.questions_pos_ = select_item.questions_pos_;
          tmp_select_item.params_idx_ = select_item.params_idx_;
          tmp_select_item.neg_param_idx_ = select_item.neg_param_idx_;
          tmp_select_item.esc_str_flag_ = select_item.esc_str_flag_;
          tmp_select_item.paramed_alias_name_ = select_item.paramed_alias_name_;
          tmp_select_item.need_check_dup_name_ = select_item.need_check_dup_name_;
          tmp_select_item.is_unpivot_mocked_column_ = select_item.is_unpivot_mocked_column_;
        }
      }
    }
    tmp_select_item.alias_name_ = col_item.column_name_;
    tmp_select_item.expr_name_ = col_item.column_name_;
    tmp_select_item.is_real_alias_ = false;
    tmp_select_item.expr_ = col_item.expr_;
    if (OB_FAIL(target_list.push_back(tmp_select_item))) {
      LOG_WARN("push back target list failed", K(ret));
    }
  }
  return ret;
}

// construct select item from select_expr
int ObSelectResolver::set_select_item(SelectItem &select_item, bool is_auto_gen)
{
  int ret = OB_SUCCESS;
  ObCollationType cs_type = CS_TYPE_INVALID;

  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt) || OB_ISNULL(session_info_) || OB_ISNULL(select_item.expr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("select stmt is null", K_(session_info), K(select_stmt), K_(select_item.expr));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (!select_item.expr_->is_column_ref_expr()) {
    if (NULL != params_.secondary_namespace_ && !select_item.is_real_alias_ && is_auto_gen
        && select_item.alias_name_.length() > static_cast<size_t>(OB_MAX_COLUMN_NAME_LENGTH)) {
      ObString tmp_col_name;
      ObString col_name;
#define AILAS_NAME_LEN 100
      char temp_str_buf[AILAS_NAME_LEN] = { 0 };
      if (snprintf(temp_str_buf, sizeof(temp_str_buf), "Name_exp_%ld", auto_name_id_++) < 0) {
        ret = OB_SIZE_OVERFLOW;
        SQL_RESV_LOG(WARN, "failed to generate buffer for temp_str_buf", K(ret));
      }
#undef AILAS_NAME_LEN
      if (OB_SUCC(ret)) {
        tmp_col_name = ObString::make_string(temp_str_buf);
        if (OB_FAIL(ob_write_string(*allocator_, tmp_col_name, col_name))) {
          SQL_RESV_LOG(WARN, "Can not malloc space for constraint name", K(ret));
        } else {
          select_item.alias_name_.assign_ptr(col_name.ptr(), col_name.length());
        }
      }
      if (OB_SUCC(ret) &&
          OB_FAIL(ObSQLUtils::check_column_name(cs_type, select_item.alias_name_))) {
        LOG_WARN("fail to make field name", K(ret));
      }
    } else if (OB_FAIL(ObSQLUtils::check_and_copy_column_alias_name(cs_type, is_auto_gen,
                                                                    allocator_,
                                                                    select_item.alias_name_))) {
      LOG_WARN("check and copy column alias name failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(select_stmt->add_select_item(select_item))) {
    LOG_WARN("add select item to select stmt failed", K(ret));
  } else { /*do nothing.*/ }
  return ret;
}

// find matched table in ijoined table groups, set jointable_idx to the group index if found.
int ObSelectResolver::find_joined_table_group_for_table(
    const uint64_t table_id,
    int64_t &jointable_idx)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();

  jointable_idx = -1;
  ObIArray<JoinedTable*> &joined_tables = select_stmt->get_joined_tables();
  for (int i = 0; i < joined_tables.count(); i++) {
    bool found = false;
    for (int j = 0; j < joined_tables.at(i)->single_table_ids_.count(); j++) {
      if (joined_tables.at(i)->single_table_ids_.at(j) == table_id) {
        found = true;
        break;
      }
    }

    if (found) {
      jointable_idx = i;
      break;
    }
  }

  return ret;
}

// background:
// Tables in table_items of select stmt is added by the executed sequence, either
// based table or generated table. For joined table of each group, join info is
// saved in joined_tables of current select stmt. As for joined table with using,
// columns will be coalesced based on using list and joined table.
//
// idea:
// For each table item in table_items:
//  - based table, add all olumns in table schema
//  - generated table, add all select column items
//  - joined table, find all rest joined tables from one joined_table group, and
//  coalesce columns for based/generated table, which based/generated table columns
//  are producted by the rule above. Skip tables in current joined group for looping.
//
// words:
// table group: seperated by ','
// joined table group: tree of joined table in one table group
// join group: short of joined table group
//
int ObSelectResolver::resolve_star_for_table_groups(ObStarExpansionInfo &star_expansion_info)
{
  ObSelectStmt *select_stmt = get_select_stmt();
  int ret = OB_SUCCESS;
  int64_t num = 0;
  int64_t jointable_idx = -1;
  bool oracle_star_expand = lib::is_oracle_mode() && is_top_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null");
  } else {
    num = select_stmt->get_table_size();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
    ObArray<SelectItem> target_list;
    const TableItem *table_item = select_stmt->get_table_item(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null");
    } else if (has_oracle_join()) {
      table_item = get_from_items_order(i);
      if (OB_ISNULL(table_item) || table_item->is_joined_table()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_item has wrong type", K(table_item));
      } else {
        if (OB_FAIL(expand_target_list(*table_item, target_list))) {
          LOG_WARN("resolve table columns failed", K(ret), K(table_item));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < target_list.count(); ++i) {
          if (OB_FAIL(select_stmt->add_select_item(target_list.at(i)))) {
            LOG_WARN("add select item to select stmt failed", K(ret));
          } else if (is_only_full_group_by_on(session_info_->get_sql_mode())) {
            //如果是only full group by，所有target list中的列都必须检查是否满足group约束
            if (OB_FAIL(standard_group_checker_.add_unsettled_column(target_list.at(i).expr_))) {
              LOG_WARN("add unsettled column failed", K(ret));
            } else if (OB_FAIL(standard_group_checker_.add_unsettled_expr(target_list.at(i).expr_))) {
              LOG_WARN("add unsettled expr failed", K(ret));
            }
            //同上
          }
        }
      }
    } else {
      if (OB_FAIL(find_joined_table_group_for_table(table_item->table_id_, jointable_idx))) {
        LOG_WARN("find_joined_table_group_for_table failed", K(ret), K(table_item));
      } else if (jointable_idx != -1) {
        // located in joined table with jointable_idx of joined_tables
        if (OB_FAIL(find_select_columns_for_join_group(jointable_idx, &target_list))) {
          LOG_WARN("find_select_columns_for_join_group failed", K(ret));
        } else {
          // skip next tables in joined group
          i += select_stmt->get_joined_tables().at(jointable_idx)->single_table_ids_.count() - 1;
          // push back select items to select stmt
          for (int j = 0; OB_SUCC(ret) && j < target_list.count(); j++) {
            SelectItem &item = target_list.at(j);
            if (OB_FAIL(item.expr_->extract_info())) {
              LOG_WARN("extract info failed", K(ret));
            } else if (OB_FAIL(item.expr_->deduce_type(session_info_))) {
              LOG_WARN("deduce type failed", K(ret));
            } else if (OB_FAIL(select_stmt->add_select_item(item))) {
              LOG_WARN("add_select_item failed", K(ret), K(item));
            } else if (is_only_full_group_by_on(session_info_->get_sql_mode())) {
              //如果是only full group by，所有target list中的列都必须检查是否满足group约束
              if (OB_FAIL(standard_group_checker_.add_unsettled_column(item.expr_))) {
                LOG_WARN("add unsettled column failed", K(ret));
              } else if (OB_FAIL(standard_group_checker_.add_unsettled_expr(item.expr_))) {
                LOG_WARN("add unsettled expr failed", K(ret));
              }
              //对于select * from t1 group by c1, c2;这样的语句，*展开就是column，所以表达式以及表达式引用到的列都是自己
            }
          }
        }
      } else {
        // based table or alias table or generated table
        OZ( expand_target_list(*table_item, target_list), table_item );
        for (int64_t i = 0; OB_SUCC(ret) && i < target_list.count(); ++i) {
          if (OB_FAIL(select_stmt->add_select_item(target_list.at(i)))) {
            LOG_WARN("add select item to select stmt failed", K(ret));
          } else if (is_only_full_group_by_on(session_info_->get_sql_mode())) {
            //如果是only full group by，所有target list中的列都必须检查是否满足group约束
            OZ( standard_group_checker_.add_unsettled_column(target_list.at(i).expr_) );
            OZ( standard_group_checker_.add_unsettled_expr(target_list.at(i).expr_) );
          }
        }
      }
    }
    if (OB_SUCC(ret) && oracle_star_expand) {
      for (int64_t i = 0; OB_SUCC(ret) && i < target_list.count(); ++i) {
        if (OB_FAIL(star_expansion_info.column_name_list_.push_back(target_list.at(i).expr_name_))) {
          LOG_WARN("failed to push back select item expr name", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_all_json_table_columns(
  const TableItem &table_item, ObIArray<ColumnItem> *column_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_items)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid params, null column items", K(ret));
  } else if (OB_FAIL(resolve_json_table_column_all_items(table_item, *column_items))) {
    LOG_WARN("fail to resolve json table column items", K(ret));
  }
  return ret;
}

int ObSelectResolver::resolve_all_function_table_columns(
  const TableItem &table_item, ObIArray<ColumnItem> *column_items)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(column_items));
  OZ (resolve_function_table_column_item(table_item, *column_items));
  return ret;
}

int ObSelectResolver::is_need_check_col_dup(const ObRawExpr *expr, bool &need_check)
{
  int ret = OB_SUCCESS;
  need_check = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is null", K(ret));
  } else if (params_.need_check_col_dup_) {
    need_check = true;
  } else if (T_QUESTIONMARK == expr->get_expr_type()) {
    need_check = false;
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && need_check && j < expr->get_children_count(); ++j) {
      const ObRawExpr *child = expr->get_param_expr(j);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child", K(child));
      } else if (OB_FAIL(SMART_CALL(is_need_check_col_dup(child, need_check)))) {
        LOG_WARN("failed to check if need to check col duplicate", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_all_generated_table_columns(
  const TableItem &table_item, ObIArray<ColumnItem> *column_items)
{
  int ret = OB_SUCCESS;
  ColumnItem *col_item = NULL;
  ObSelectStmt *table_ref = table_item.ref_query_;
  bool is_exists = false;
  if (OB_ISNULL(table_ref)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("generate table is null", K(ret), K(table_ref));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ref->get_select_item_size(); ++i) {
    const SelectItem &select_item = table_ref->get_select_item(i);
    const bool is_joined_dup_column = select_item.expr_->is_column_ref_expr()?
        static_cast<ObColumnRefRawExpr *>(select_item.expr_)->is_joined_dup_column():false;
    bool need_check_col_dup = true;
    bool is_skip = is_oracle_mode() && !table_ref->is_view_stmt() && is_joined_dup_column;
    /* 这里进行一次重复列名检测是原因是oracle支持generated table含有重复列名的不引用重复列名的查询，比如：
    *  select 1 from (select c1,c1 from t1)；因此在oracle模式resolve generated table时会跳过检查重复列
    *  但是对于select * from(select c1,c1 from t1)；这样的查询肯定引用了，因此必须在展开*对应的查询时进行一次
    *  检查，如果存在重复列是不允许的；
     */
    if (OB_FAIL(resolve_generated_table_column_item(table_item,
                                                    select_item.alias_name_,
                                                    col_item,
                                                    NULL,
                                                    i + OB_APP_MIN_COLUMN_ID,
                                                    i,
                                                    is_skip))) {
      LOG_WARN("resolve column item failed", K(ret));
    } else if (column_items != NULL) {
      if (OB_FAIL(column_items->push_back(*col_item))) {
        LOG_WARN("push back column item failed", K(ret));
      }
    }
  }
  LOG_DEBUG("finish resolve_all_generated_table_columns", KPC(column_items), K(table_item),
            KPC(table_ref));
  return ret;
}

// rules:
// table name can not same in same level, including alias
// specified columns are ahead of star
// columns are append by table group, seperated by ','.
// each group containers the mixtrue of based/joined/generated table.
int ObSelectResolver::resolve_star(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  bool oracle_star_expand = lib::is_oracle_mode() && is_top_stmt();
  const share::schema::ObTableSchema *table_schema = NULL;
  ObStarExpansionInfo star_expansion_info;
  if (OB_ISNULL(node) || OB_ISNULL(session_info_)
      || OB_ISNULL(select_stmt) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid status", K(node), K_(session_info), K(select_stmt), K(params_.expr_factory_));
  } else {
    star_expansion_info.start_pos_ = node->stmt_loc_.first_column_;
    star_expansion_info.end_pos_ = node->stmt_loc_.last_column_;
  }
  if (OB_FAIL(ret)) {
  } else if (node->type_ == T_STAR) {
    int64_t num = select_stmt->get_table_size();
    if (num <= 0) {
      // select *
      // select * from dual
      SelectItem select_item;
      if (lib::is_mysql_mode()) {
        ObConstRawExpr *c_expr = NULL;
        if (!is_in_exists_subquery()) {
          ret = OB_ERR_NO_TABLES_USED;
          LOG_WARN("No tables used");
        } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*params_.expr_factory_,
                                                        ObIntType, 1, c_expr))) {
          LOG_WARN("fail to build const int expr", K(ret));
        } else if (OB_FALSE_IT(select_item.expr_ = c_expr)) {
        } else if (OB_FAIL(select_stmt->add_select_item(select_item))) {
          LOG_WARN("failed to add select item", K(ret));
        } else {/*do nothing*/}
      } else {
        // (select * from dual) is legitimate for oracle ==> output: X
        ObConstRawExpr *c_expr = NULL;
        const char *ptr_value = "X";
        const char *ptr_name = "DUMMY";
        ObString string_value(1, ptr_value);
        ObString string_name(ptr_name);
        if (select_stmt->has_group_by() || has_group_by_clause()) {
          ret = OB_ERR_WRONG_FIELD_WITH_GROUP;
          LOG_DEBUG("not a GROUP BY expression", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*params_.expr_factory_,
                   ObCharType,ptr_value, session_info_->get_nls_collation(), c_expr))){
          LOG_WARN("fail to create const string c_expr", K(ret));
        } else {
          ObSysFunRawExpr *cast_expr = NULL;
          ObExprResType res_type;
          res_type.set_type(ObVarcharType);
          res_type.set_length(1);
          res_type.set_length_semantics(LS_BYTE);
          res_type.set_collation_level(CS_LEVEL_IMPLICIT);
          res_type.set_collation_type(session_info_->get_nls_collation());
          if (OB_FAIL(ObRawExprUtils::create_cast_expr(*params_.expr_factory_, c_expr,
                      res_type, cast_expr, session_info_))) {
            LOG_WARN("create cast expr for dummy failed", K(ret));
          } else if (OB_FAIL(cast_expr->clear_flag(IS_INNER_ADDED_EXPR))) {
            LOG_WARN("failed to clear flag for cast expr", K(ret));
          } else if (OB_FAIL(cast_expr->formalize(session_info_))) {
            LOG_WARN("failed to formalize cast expr", K(ret));
          } else {
            select_item.expr_ = cast_expr;
            select_item.expr_name_ = string_name;
            select_item.alias_name_ = string_name;
            select_item.is_real_alias_ = true;
            if (OB_FAIL(select_stmt->add_select_item(select_item))) {
              LOG_WARN("failed to add select item", K(ret));
            } else if (oracle_star_expand
                       && OB_FAIL(star_expansion_info.column_name_list_.push_back(string_name))) {
              LOG_WARN("failed to push back dummy", K(ret));
            } else {/*do nothing*/}
          }
        }
      }
    } else if (OB_FAIL(resolve_star_for_table_groups(star_expansion_info))) {
      LOG_WARN("resolve star for table groups failed", K(ret));
    }
  } else if (node->type_ == T_COLUMN_REF && node->children_[2]->type_ == T_STAR) {
    ObQualifiedName column_ref;
    bool is_json_wildcard_column = false;  // special input : tab_name.column_name.*
    bool is_column_name_equal = false;
    const TableItem* tab_item = NULL;
    ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
    if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_column_ref(node, case_mode, column_ref))) {
      LOG_WARN("fail to resolve table name", K(ret));
    } else {
      ObSEArray<const TableItem*, 8> table_items;
      ObArray<SelectItem> target_list;
      if (OB_FAIL(select_stmt->get_all_table_item_by_tname(session_info_, column_ref.database_name_,
                                                           column_ref.tbl_name_, table_items))) {
        LOG_WARN("get all matched table failed", K(ret));
      } else if (table_items.count() <= 0) {
        ret = OB_SUCCESS;
        ObString db_name;
        if (lib::is_oracle_mode() && OB_FAIL(select_stmt->get_all_table_item_by_tname(session_info_, db_name,
                                                                      column_ref.database_name_, table_items))) {
          LOG_WARN("get all matched table failed", K(ret));
        } else if (lib::is_mysql_mode() || table_items.count() <= 0) {
          ret = OB_ERR_BAD_TABLE;
        } else {
          is_json_wildcard_column = true;
        }
        if (ret != 0) {  // according to oracle , need cover error code
          ret = OB_ERR_BAD_TABLE;
          ObString table_name = concat_table_name(column_ref.database_name_, column_ref.tbl_name_);
          LOG_USER_ERROR(OB_ERR_BAD_TABLE, table_name.length(), table_name.ptr());
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
        target_list.reset();
        tab_item = table_items.at(i);
        if (OB_ISNULL(tab_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(tab_item), K(ret));
        } else if (OB_FAIL(expand_target_list(*tab_item, target_list))) {
          LOG_WARN("resolve table columns failed", K(ret), K(tab_item), K(i));
        } else if (is_json_wildcard_column) {
          if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(), tab_item->ref_id_, table_schema))) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("get table schema failed", K_(tab_item->table_name), K(tab_item->ref_id_), K(ret));
          } else if (OB_ISNULL(table_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("get table schema failed", K_(tab_item->table_name), K(tab_item->ref_id_), K(ret));
          } else if (OB_NOT_NULL(table_schema->get_column_schema(column_ref.tbl_name_))
                      && !table_schema->get_column_schema(column_ref.tbl_name_)->is_json()) {
            ret = OB_ERR_TABLE_NAME_NOT_IN_LIST;
            LOG_WARN("table name not in from list", K(ret), K(column_ref.tbl_name_));
          }
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < target_list.count(); ++j) {
          is_column_name_equal = is_json_wildcard_column & (0 != column_ref.tbl_name_.case_compare(target_list.at(j).alias_name_));
          if (!is_column_name_equal && OB_FAIL(select_stmt->add_select_item(target_list.at(j)))) {
            LOG_WARN("add select item to select stmt failed", K(ret));
          } else if (oracle_star_expand
                     && OB_FAIL(star_expansion_info.column_name_list_.push_back(target_list.at(j).expr_name_))) {
            LOG_WARN("failed to push back select item expr name", K(ret));
          } else if (is_only_full_group_by_on(session_info_->get_sql_mode())) {
            //如果是only full group by，所有target list中的列都必须检查是否满足group约束
            if (is_column_name_equal) {    // target column not equal with current column without judge
            } else if (OB_FAIL(standard_group_checker_.add_unsettled_column(target_list.at(j).expr_))) {
              LOG_WARN("add unsettled column failed", K(ret));
            } else if (OB_FAIL(standard_group_checker_.add_unsettled_expr(target_list.at(j).expr_))) {
              LOG_WARN("add unsettled expr to standard group checker failed", K(ret));
            }
          }
          if (OB_SUCC(ret) && !is_column_name_equal) {
            ret = column_namespace_checker_.check_column_existence_in_using_clause(
                    table_items.at(i)->table_id_, target_list.at(j).expr_name_);
          }
        }
      }
    }
  } else {
    /* won't be here */
  }
  if (OB_SUCC(ret) && oracle_star_expand
      && OB_FAIL(params_.star_expansion_infos_.push_back(star_expansion_info))) {
    LOG_WARN("failed to push back star expansion info", K(ret));
  }
  return ret;
}


// coalesce columns from left and right columns for the given joined type,
// with or without using_columns
int ObSelectResolver::coalesce_select_columns_for_joined_table(
  const ObIArray<SelectItem> *left,
  const ObIArray<SelectItem> *right,
  const ObJoinType type,
  const ObIArray<ObString> &using_columns,
  ObIArray<SelectItem> *coalesced_columns)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> coalesced_column_ids;
  const ObIArray<SelectItem> *items = left;
  if (using_columns.count() > 0 && type == RIGHT_OUTER_JOIN) {
    items = right;
  }
  // 1. pick up matched using columns from select items
  for (int64_t j = 0; OB_SUCC(ret) && j < items->count(); j++) {
    const SelectItem *item = &items->at(j);
    for (int64_t i = 0; OB_SUCC(ret) && i < using_columns.count(); ++i) {
      if (ObCharset::case_insensitive_equal(item->alias_name_, using_columns.at(i))) {
        if (OB_FAIL(coalesced_columns->push_back(*item))) {
          LOG_WARN("coalesced_columns->push_back item failed", K(ret));
        }
        if (OB_FAIL(coalesced_column_ids.push_back(j))) {
          LOG_WARN("coalesced_column_ids.push_back failed", K(ret));
        }
        break;
      }
    }
  }

  // 2. pick up rest columns
  if (using_columns.count() > 0) {
    if (coalesced_column_ids.count() <= 0) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      SQL_RESV_LOG(WARN,"Column not exist", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < items->count(); i++) {
      int64_t j = 0;
      for (j = 0; j < coalesced_column_ids.count(); j++) {
        if (coalesced_column_ids.at(j) == i) {
          break;
        }
      }
      if (j == coalesced_column_ids.count()) {
        if (OB_FAIL(coalesced_columns->push_back(items->at(i)))) {
          LOG_WARN("coalesced_columns->push_back failed", K(ret));
        }
      }
    }

    // 3. pick up rest counts in other side
    items = right;
    if (using_columns.count() > 0 && type == RIGHT_OUTER_JOIN) {
      items = left;
    }
    if (coalesced_columns->count() <= 0) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      SQL_RESV_LOG(WARN,"Column not exist", K(ret));
    }
    if (OB_SUCC(ret)) {
      // if we find a duplicated column name (exclude the using column), we need to set the hidden id
      // to distinguish them.
      int64_t left_column_num = coalesced_columns->count();
      for (int64_t i = 0; OB_SUCC(ret) && i < items->count(); i++) {
        const SelectItem *item = &items->at(i);
        bool found = false;
        bool duplicated = false;
        int64_t dup_col_index = 0;
        for (int64_t j = 0; OB_SUCC(ret) && j < using_columns.count(); ++j) {
          if (ObCharset::case_insensitive_equal(item->alias_name_, using_columns.at(j))) {
            found = true;
            break;
          }
        }
        for(dup_col_index = 0; dup_col_index < left_column_num; dup_col_index++ ) {
          if (ObCharset::case_insensitive_equal(item->alias_name_, coalesced_columns->at(dup_col_index).alias_name_)) {
            duplicated=true; 
            break;
          }
        }
        if (!found) {
          if (OB_FAIL(coalesced_columns->push_back(*item))) {
            LOG_WARN("coalesced_columns->push_back failed", K(ret));
          }
          // if found duplicated and the duplicated column is not the using column.
          // set the is_joined_dup_column flag; the i-th column in items (the last column in coalesced_columns) is 
          // duplicated with the left_column_index-th column in coalesced_columns.
          if (OB_SUCC(ret) && duplicated) {
            static_cast<ObColumnRefRawExpr*>(coalesced_columns->at(dup_col_index).expr_)->set_joined_dup_column(true);
            static_cast<ObColumnRefRawExpr*>(coalesced_columns->at(coalesced_columns->count()-1).expr_)->set_joined_dup_column(true);
          }
        }
      }
    }
  }
  return ret;
}

// jointable will be the root node of joined table, if you want to loop the whole tree.
int ObSelectResolver::find_select_columns_for_joined_table_recursive(
  const JoinedTable *jointable,
  ObIArray<SelectItem> *sorted_select_items)
{
  int ret = OB_SUCCESS;
  ObArray<SelectItem> left_select_items;
  ObArray<SelectItem> right_select_items;

  if (jointable->left_table_->type_ == TableItem::JOINED_TABLE) {
    OC( (find_select_columns_for_joined_table_recursive)(
          static_cast<const JoinedTable*>(jointable->left_table_),
          &left_select_items));
  } else {
    OC( (expand_target_list)(*jointable->left_table_, left_select_items) );
  }

  if (OB_SUCC(ret)) {
    if (jointable->right_table_->type_ == TableItem::JOINED_TABLE) {
      OC( (find_select_columns_for_joined_table_recursive)(
            static_cast<const JoinedTable*>(jointable->right_table_),
            &right_select_items));
    } else {
      OC( (expand_target_list)(*jointable->right_table_, right_select_items) );
    }
  }

  ResolverJoinInfo *join_info = NULL;
  if (get_joininfo_by_id(jointable->table_id_, join_info)) {
    OC( (coalesce_select_columns_for_joined_table)(
         &left_select_items,
         &right_select_items,
         jointable->joined_type_,
         join_info->using_columns_,
         sorted_select_items));
  }

  return ret;
}

int ObSelectResolver::find_select_columns_for_join_group(
  const int64_t jointable_idx,
  ObArray<SelectItem> *sorted_select_items)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = get_select_stmt();
  const JoinedTable *jointable = select_stmt->get_joined_tables().at(jointable_idx);

  if (OB_FAIL(find_select_columns_for_joined_table_recursive(jointable, sorted_select_items))) {
    LOG_WARN("find_select_columns_for_joined_table_recursive failed!", K(*jointable));
  } else {
    const ObIArray<JoinedTable*> &joined_tables = select_stmt->get_joined_tables();
    int64_t n_select_count = sorted_select_items->count();
    for (int64_t i = 0; i < n_select_count; ++i) {
      ObRawExpr *coalesce_expr = NULL;
      for (int j = 0; j < joined_tables.count(); j++) {
        const JoinedTable* joined_table = joined_tables.at(j);
        OZ(recursive_find_coalesce_expr(joined_table,
                                        sorted_select_items->at(i).alias_name_,
                                        coalesce_expr));
        if (coalesce_expr != NULL) {
          sorted_select_items->at(i).expr_ = coalesce_expr;
        }
      }
    }
  }
  return ret;
}

/*               |-------------------------------------r_union_stmt--------------------------------|
 * with cte() as ( left_stmt union all right stmt  ) search by item + pseudo, cycle by item + pseudo
 * r_union_stmt这个stmt的解析过程中，是没有任何一个table item出现在这个层次的stmt的
 * 没有办法按照常规的表达式T_COLUMN_REF产生表达式，并作为column_item加到stmt中
 * 因为加入的时候会检查是否该列的表是否在stmt中。
 * search或者cycle的item在在left_stmt和right_stmt中代表的列不一样，所以无法直接使用左支或者右支的select item
 * 所以只能类似于generate table item中的column item产生的方式来产生
 * */
int ObSelectResolver::generate_fake_column_expr(
  const share::schema::ObColumnSchemaV2 *column_schema,
  ObSelectStmt *left_stmt,
  ObColumnRefRawExpr *&fake_col_expr)
{
  int ret = OB_SUCCESS;
  int64_t projector_offset = column_schema->get_cte_generate_column_projector_offset();
  ObSelectStmt *select_stmt = NULL;
  ObColumnRefRawExpr *col_expr = NULL;
  ColumnItem column_item;
  CK( OB_NOT_NULL(select_stmt = get_select_stmt()),
      OB_NOT_NULL(column_schema),
      OB_NOT_NULL(left_stmt),
      !OB_UNLIKELY(projector_offset >= left_stmt->get_select_item_size()),
      OB_NOT_NULL(left_stmt->get_select_item(projector_offset).expr_),
      !(projector_offset >= cte_ctx_.cte_col_names_.count()));
  OC( (params_.expr_factory_->create_raw_expr)(T_REF_COLUMN, col_expr) );
  OC( (left_stmt->get_select_item(projector_offset).expr_->deduce_type)(session_info_) );
  if (OB_SUCC(ret)) {
    //build a new column ref expr
    col_expr->set_ref_id(
      current_recursive_cte_table_item_->ref_id_,
      column_schema->get_column_id());
    col_expr->set_result_type(left_stmt->get_select_item(projector_offset).expr_->get_result_type());
    col_expr->set_column_attr(current_recursive_cte_table_item_->get_table_name(),
                              cte_ctx_.cte_col_names_.at(projector_offset));
    col_expr->set_database_name(current_recursive_cte_table_item_->database_name_);
    col_expr->set_column_flags(CTE_GENERATED_COLUMN_FLAG);
    if (OB_FAIL(col_expr->extract_info())) {
      LOG_WARN("extract column expr info failed", K(ret));
    } else {
      fake_col_expr = col_expr;
    }
  }
  return ret;
}

int ObSelectResolver::resolve_search_item(const ParseNode *sort_list, ObSelectStmt *r_union_stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* left_stmt = r_union_stmt->get_set_query(0);
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_list->num_child_; ++i) {
    ParseNode *sort_node = sort_list->children_[i];
    OrderItem order_item;
    ObColumnRefRawExpr *fake_col_expr = NULL;
    const share::schema::ObColumnSchemaV2 *column_schema;
    ParseNode *alias_node = sort_node->children_[0];
    ObString column_alia_name;
    if (OB_FAIL(ObResolverUtils::set_direction_by_mode(*sort_node, order_item))) {
      LOG_WARN("failed to set direction by mode", K(ret));
    } else if (OB_ISNULL(alias_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alias node is null", K(ret));
    } else if (T_OBJ_ACCESS_REF != alias_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alias node is not a obj access", K(ret));
    } else if (OB_ISNULL(alias_node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alias node is invalid", K(ret));
    } else {
      column_alia_name.assign_ptr(
        (char *) (alias_node->children_[0]->str_value_),
        static_cast<int32_t>(alias_node->children_[0]->str_len_));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_column_schema(current_recursive_cte_table_item_->ref_id_,
                                    column_alia_name,
                                    column_schema,
                                    false /*get_hidden*/,
                                    current_recursive_cte_table_item_->is_link_table()))) {
        LOG_WARN("the col item has been resolve, but we do not find it in the search clause resolver", K(ret));
      } else if (OB_FAIL(generate_fake_column_expr(column_schema, left_stmt, fake_col_expr))){
        LOG_WARN("generate the column item", K(ret), K(*column_schema));
      } else {
        order_item.expr_ = fake_col_expr;
        if (OB_FAIL(r_union_stmt->add_search_item(order_item))) {
          // add the search order item
          LOG_WARN("Add order expression error", K(ret));
        }
      }
    }

  } // end of for
  return ret;
}

int ObSelectResolver::resolve_search_pseudo(const ParseNode *search_set_clause,
                                            ObSelectStmt *r_union_stmt,
                                            ObString &search_pseudo_column_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(search_set_clause) || OB_ISNULL(r_union_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("search set clause/r_union_stmt is null", K(ret), K(search_set_clause), K(r_union_stmt));
  } else if (OB_FAIL(resolve_and_split_sql_expr(*search_set_clause, r_union_stmt->get_cte_exprs()))) {
    LOG_WARN("resolve and split sql expr failed", K(ret));
  } else {
    int64_t count = r_union_stmt->get_cte_exprs().count() - 1;
    if (count < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("we need at least 1 cte expr!", K(ret));
    } else {
      ObRawExpr* search_pseudo_column_expr = r_union_stmt->get_cte_exprs().at(count);
      if (T_CTE_SEARCH_COLUMN != search_pseudo_column_expr->get_expr_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected t_cte_search_column", K(ret));
      } else {
        SelectItem select_item;
        select_item.expr_ = search_pseudo_column_expr;
        select_item.alias_name_.assign_ptr(search_pseudo_column_expr->get_alias_column_name().ptr(),
            search_pseudo_column_expr->get_alias_column_name().length());
        search_pseudo_column_name.assign_ptr(search_pseudo_column_expr->get_alias_column_name().ptr(),
            search_pseudo_column_expr->get_alias_column_name().length());
        select_item.is_real_alias_ = true;
        if (OB_FAIL(r_union_stmt->add_select_item(select_item))) {
          // construct select item from select_expr
          LOG_WARN("set cte search select item failed", K(ret));
        }
      }
    }
  }

  return ret;
}

/**
 * 在search clause中必须严格出现在cte（col alias）定义的列名，不允许出现类似cte.col，指定是定义的名字。
 * 例如：
 *     cte( a, b, c) as (.....) search depth by a, c, b set ordering
 *     只能是单独的一个列的表现形式，已经oracle中测试并确认
 */
int ObSelectResolver::resolve_search_clause(const ParseNode &parse_tree,
                                            const TableItem* cte_table_item,
                                            ObString& search_pseudo_column_name)
{
  int ret = OB_SUCCESS;
  current_scope_ = T_WITH_CLAUSE_SEARCH_SCOPE;
  const ParseNode* sort_list = parse_tree.children_[0];
  const ParseNode* search_set_clause = parse_tree.children_[1];
  ObSelectStmt* r_union_stmt = cte_table_item->ref_query_;
  //获得深度 广度优先选项，查询列选项以及伪列的列名
  if (OB_ISNULL(r_union_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the recursive union can not be null", K(ret));
  } else if (OB_ISNULL(r_union_stmt->get_set_query(0))
             && OB_ISNULL(r_union_stmt->get_set_query(1))) {
    ret = OB_ERR_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE;
    LOG_WARN("SEARCH and CYCLE clauses can only be specified for recursive WITH clause elements", K(ret));
  } else if (OB_ISNULL(current_recursive_cte_table_item_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the recursive cte table item can not be null", K(ret));
  } else if (T_SEARCH_DEPTH_NODE == parse_tree.type_) {
    r_union_stmt->set_breadth_strategy(false);
  } else if (T_SEARCH_BREADTH_NODE == parse_tree.type_) {
    r_union_stmt->set_breadth_strategy(true);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected search clause parse tree", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_search_item(sort_list, r_union_stmt))) {
      LOG_WARN("resolve search item failed", K(ret));
    } else if (OB_FAIL(resolve_search_pseudo(search_set_clause, r_union_stmt, search_pseudo_column_name))) {
      LOG_WARN("resolve search pseudo failed", K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::resolve_cycle_item(const ParseNode *alias_list, ObSelectStmt *r_union_stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* left_stmt = r_union_stmt->get_set_query(0);
  ObSEArray<ObString, 8> col_names;
  for (int64_t i = 0; OB_SUCC(ret) && i < alias_list->num_child_; ++i) {
    ParseNode *alias_node = alias_list->children_[i];
    ObString column_alia_name;
    ColumnItem col_item;
    ObColumnRefRawExpr *fake_col_expr = NULL;
    const share::schema::ObColumnSchemaV2 *column_schema;
    column_alia_name.assign_ptr((char *) (alias_node->str_value_),
                                static_cast<int32_t>(alias_node->str_len_));
    if (OB_FAIL(get_column_schema(current_recursive_cte_table_item_->ref_id_,
                                  column_alia_name,
                                  column_schema,
                                  false /*get_hidden*/,
                                  current_recursive_cte_table_item_->is_link_table()))) {
      LOG_WARN("the col item has been resolve, but we do not find it in the cycle clause resolver", K(ret));
    } else if (OB_FAIL(generate_fake_column_expr(column_schema, left_stmt, fake_col_expr))) {
      LOG_WARN("generate the column item", K(ret));
    } else {
      int64_t projector_offset = column_schema->get_cte_generate_column_projector_offset();
      //set col item
      ObResolverUtils::resolve_default_value_and_expr_from_select_item(left_stmt->get_select_item(projector_offset),
                                                                       col_item,
                                                                       left_stmt); 
      col_item.expr_ = fake_col_expr;
      col_item.column_id_ = column_schema->get_column_id();
      col_item.table_id_ = current_recursive_cte_table_item_->ref_id_;
      col_item.column_name_ = column_schema->get_column_name_str();
      if (OB_FAIL(r_union_stmt->add_cycle_item(col_item))) {
        LOG_WARN("add cycle r union stmt col item failed", K(ret));
      }
    }
  } // end of for
  return ret;
}

int ObSelectResolver::resolve_cycle_pseudo(const ParseNode *cycle_set_clause,
                                           ObSelectStmt *r_union_stmt,
                                           const ParseNode *cycle_value,
                                           const ParseNode *cycle_default_value,
                                           ObString &cycle_pseudo_column_name)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr_v = nullptr;
  ObRawExpr *expr_d_v = nullptr;
  //for pseudo column
  if (OB_ISNULL(cycle_set_clause)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cycle clause must have set pseudo column", K(ret));
  } else if (OB_FAIL(resolve_and_split_sql_expr(*cycle_set_clause,
                                                r_union_stmt->get_cte_exprs()))) {
    LOG_WARN("resolve and split sql expr failed", K(ret));
  } else if (cycle_value->type_ == T_QUESTIONMARK
        && cycle_default_value->type_ == T_QUESTIONMARK) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("cycle value can not be '?'",
               K(ret),
               K(cycle_value->text_len_),
               K(cycle_default_value->text_len_));
  } else if ((cycle_value->type_ == T_VARCHAR || cycle_value->type_ == T_INT)
             && (cycle_default_value->type_ == T_VARCHAR || cycle_default_value->type_ == T_INT)) {
    if (cycle_value->str_len_ > 0 && cycle_default_value->str_len_ > 0) {
      if (OB_FAIL(resolve_sql_expr(*cycle_value, expr_v))) {
        LOG_WARN("resolve sql expr failed", K(ret));
      } else if (OB_FAIL(resolve_sql_expr(*cycle_default_value, expr_d_v))) {
        LOG_WARN("resolve sql expr failed", K(ret));
      }
    } else {
      ret = OB_ERR_CTE_ILLEGAL_CYCLE_NON_CYCLE_VALUE;
      LOG_WARN("invalid cycle argument",
               K(ret),
               K(cycle_value->str_len_),
               K(cycle_default_value->str_len_));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t cycle_column_idx = r_union_stmt->get_cte_exprs().count();
    if (OB_UNLIKELY(0 == cycle_column_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("a recursive union stmt without cte expr is unexpected", K(ret));
    } else {
      ObRawExpr *expr = r_union_stmt->get_cte_exprs().at(cycle_column_idx - 1);
      if (OB_UNLIKELY(T_CTE_CYCLE_COLUMN != expr->get_expr_type())) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("must be cycle pseudo column", K(ret));
      } else {
        ObPseudoColumnRawExpr* cycle_expr = static_cast<ObPseudoColumnRawExpr*>(expr);
        cycle_expr->set_cte_cycle_value(expr_v, expr_d_v);
        SelectItem select_item;
        select_item.expr_ = cycle_expr;
        select_item.alias_name_.assign_ptr(cycle_expr->get_alias_column_name().ptr(),
                                           cycle_expr->get_alias_column_name().length());
        cycle_pseudo_column_name.assign_ptr(cycle_expr->get_alias_column_name().ptr(),
                                            cycle_expr->get_alias_column_name().length());
        select_item.is_real_alias_ = true;
        if (OB_FAIL(r_union_stmt->add_select_item(select_item))) {
          // construct select item from select_expr
          LOG_WARN("failed to set cte search select item", K(ret));
        }
      }
    }
  }
  return ret;
}


/**
 * 在cycle clause中必须严格出现在cte（col alias）定义的列名，不允许出现类似cte.col，指定是定义的名字。
 * 例如：
 *     cte( a, b, c) as (.....) search depth by a, c, b set ordering cycle a
 *     只能是单独的一个列的表现形式，已经oracle中测试并确认，不能采用类似cte.a cte.b这样的表现形式
 */
int ObSelectResolver::resolve_cycle_clause(const ParseNode &parse_tree,
                                           const TableItem *cte_table_item,
                                           ObString &cycle_pseudo_column_name)
{
  int ret = OB_SUCCESS;
  current_scope_ = T_WITH_CLAUSE_CYCLE_SCOPE;
  ObSelectStmt *r_union_stmt = cte_table_item->ref_query_;
  const ParseNode *alias_list = parse_tree.children_[0];
  const ParseNode *cycle_set_clause = parse_tree.children_[1];
  const ParseNode *cycle_value = parse_tree.children_[2];
  const ParseNode *cycle_default_value = parse_tree.children_[3];

  CK( OB_NOT_NULL(r_union_stmt),
      OB_NOT_NULL(current_recursive_cte_table_item_));

  if (OB_ISNULL(r_union_stmt->get_set_query(0))
      && OB_ISNULL(r_union_stmt->get_set_query(1))) {
    ret = OB_ERR_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE;
    LOG_WARN("SEARCH and CYCLE clauses can only be specified for recursive WITH clause elements", K(ret));
  }

  OZ( resolve_cycle_item(alias_list, r_union_stmt) );
  OZ( resolve_cycle_pseudo(cycle_set_clause,
                           r_union_stmt,
                           cycle_value,
                           cycle_default_value,
                           cycle_pseudo_column_name) );
  return ret;
}

int ObSelectResolver::get_current_recursive_cte_table(ObSelectStmt *ref_stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *right_stmt = nullptr;
  if (!ref_stmt->is_recursive_union()) {
    //do nothing
  } else if (OB_ISNULL(ref_stmt)
      || OB_ISNULL(right_stmt = ref_stmt->get_set_query(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(ref_stmt), K(right_stmt));
  } else {
    current_cte_involed_stmt_ = right_stmt;
    int64_t table_count = right_stmt->get_table_size();
    int64_t cte_table_count = 0;
    common::ObIArray<TableItem*>& table_items = right_stmt->get_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_count; ++i) {
      if (table_items.at(i)->is_fake_cte_table()) {
        current_recursive_cte_table_item_ = table_items.at(i);
        ++cte_table_count;
      }
    }
    if (OB_SUCC(ret) && 1 != cte_table_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the r union stmt's right child stmt must have only one cte table", K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::check_cycle_clause(const ParseNode &cycle_node)
{
  int ret = OB_SUCCESS;
  CK( !OB_UNLIKELY(cycle_node.num_child_ != 4) );
  CK( OB_NOT_NULL(cycle_node.children_[0]) );
  CK( OB_NOT_NULL(cycle_node.children_[1]) );
  CK( OB_NOT_NULL(cycle_node.children_[2]) );
  CK( OB_NOT_NULL(cycle_node.children_[3]) );
  CK( OB_NOT_NULL(cycle_node.children_[1]->children_[1]) );
  const ParseNode *cycle_set_clause = cycle_node.children_[1];
  const ParseNode *alias_list = cycle_node.children_[0];
  if (OB_SUCC(ret)) {
    ObSEArray<ObString, 8> alias;
    for (int64_t i = 0; OB_SUCC(ret) && i < alias_list->num_child_; ++i) {
      ParseNode *alias_node = alias_list->children_[i];
      ObString column_alia_name;
      column_alia_name.assign_ptr((char *) (alias_node->str_value_),
                                  static_cast<int32_t>(alias_node->str_len_));
      for (int64_t j = 0; j < alias.count() && OB_SUCC(ret); ++j) {
        if (ObCharset::case_insensitive_equal(alias.at(j), column_alia_name)) {
          ret = OB_ERR_CTE_DUPLICATE_NAME_IN_CYCLE_CLAUSE;
          LOG_WARN("duplicate name found in cycle column list for CYCLE clause of WITH clause", K(ret));
        }
      }
      OZ( alias.push_back(column_alia_name) );
      if (OB_SUCC(ret)) {
        bool found = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < cte_ctx_.cte_col_names_.count() && !found; ++j) {
          if (ObCharset::case_insensitive_equal(cte_ctx_.cte_col_names_.at(j), column_alia_name)) {
            found = true;
          }
        }
        if (!found) {
          ret = OB_ERR_CTE_ILLEGAL_COLUMN_IN_CYCLE_CLAUSE;
          LOG_WARN("element in cycle column list of CYCLE clause must appear in the column alias list of the WITH clause element", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ParseNode *set_column = cycle_set_clause->children_[1];
    ObString set_column_name;
    set_column_name.assign_ptr((char *) (set_column->str_value_),
                               static_cast<int32_t>(set_column->str_len_));
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < cte_ctx_.cte_col_names_.count() && !found; ++i) {
      if (ObCharset::case_insensitive_equal(cte_ctx_.cte_col_names_.at(i), set_column_name)) {
        found = true;
      }
    }
    if (found) {
      ret = OB_ERR_CTE_ILLEGAL_CYCLE_PSEUDO_NAME;
      LOG_WARN("cycle mark column name for CYCLE clause must not be part of the column alias list", K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::check_search_clause(const ParseNode &search_node)
{
  int ret = OB_SUCCESS;
  const ParseNode* sort_list = search_node.children_[0];
  const ParseNode* search_set_clause = search_node.children_[1];
  CK( OB_NOT_NULL(sort_list));
  CK( OB_NOT_NULL(search_set_clause));
  CK( search_set_clause->num_child_ == 2 );
  CK( OB_NOT_NULL(search_set_clause->children_[1]->str_value_) );
  CK( search_set_clause->children_[1]->str_len_ > 0 );
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_list->num_child_; ++i) {
      ParseNode *sort_node = sort_list->children_[i];
      ParseNode *alias_node = sort_node->children_[0];
      ObString column_alia_name;
      if (OB_ISNULL(alias_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alias node is null", K(ret));
      } else if (T_OBJ_ACCESS_REF != alias_node->type_) {
        // For oracle error code compatibility
        ret = OB_ERR_CTE_ILLEGAL_COLUMN_IN_SERACH_CALUSE;
        LOG_WARN("alias node is not a obj access", K(ret));
      } else if (OB_ISNULL(alias_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alias node is invalid", K(ret));
      } else {
        column_alia_name.assign_ptr((char *) (alias_node->children_[0]->str_value_),
                                              static_cast<int32_t>(alias_node->children_[0]->str_len_));
        bool found = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < cte_ctx_.cte_col_names_.count() && !found; ++j) {
          if (ObCharset::case_insensitive_equal(cte_ctx_.cte_col_names_.at(j), column_alia_name)) {
            found = true;
          }
        }
        if (!found) {
          ret = OB_ERR_CTE_ILLEGAL_COLUMN_IN_SERACH_CALUSE;
          LOG_WARN("element in sort specification list of SEARCH clause did not appear in the column alias list of the WITH clause element", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ParseNode *set_value = search_set_clause->children_[1];
    ObString set_column_name;
    set_column_name.assign_ptr((char *) (set_value->str_value_),
                               static_cast<int32_t>(set_value->str_len_));
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < cte_ctx_.cte_col_names_.count() && !found; ++i) {
      if (ObCharset::case_insensitive_equal(cte_ctx_.cte_col_names_.at(i), set_column_name)) {
        found = true;
      }
    }
    if (found) {
      ret = OB_ERR_CTE_ILLEGAL_SEARCH_PSEUDO_NAME;
      LOG_WARN("sequence column name for SEARCH clause must not be part of the column alias list", K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::check_search_cycle_set_column(const ParseNode &search_node, const ParseNode &cycle_node) {
  int ret = OB_SUCCESS;
  const ParseNode* search_set_clause = search_node.children_[1]->children_[1];
  const ParseNode* cycle_set_clause = cycle_node.children_[1]->children_[1];
  ObString search_set_column_name;
  search_set_column_name.assign_ptr((char *) (search_set_clause->str_value_),
                                    static_cast<int32_t>(search_set_clause->str_len_));
  ObString cycle_set_column_name;
  cycle_set_column_name.assign_ptr((char *) (cycle_set_clause->str_value_),
                                   static_cast<int32_t>(cycle_set_clause->str_len_));

  if (ObCharset::case_insensitive_equal(search_set_column_name, cycle_set_column_name)) {
    ret = OB_ERR_CTE_DUPLICATE_SEQ_NAME_CYCLE_COLUMN;
    LOG_WARN("sequence column for SEARCH clause must be different from the cycle mark column for CYCLE clause", K(ret));
  }
  return ret;
}

int ObSelectResolver::check_cycle_values(const ParseNode &cycle_node) {
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (cycle_node.children_[2]->str_len_ != 1
        || cycle_node.children_[3]->str_len_ != 1) {
      ret = OB_ERR_CTE_ILLEGAL_CYCLE_NON_CYCLE_VALUE;
      LOG_WARN("cycle mark value and non-cycle mark value must be one byte character string values", K(ret));
    } else {
      const ParseNode* cycle_value_node = cycle_node.children_[2];
      const ParseNode* non_cycle_value_node = cycle_node.children_[3];
      ObString cycle_value;
      ObString non_cycle_value;
      cycle_value.assign_ptr((char *) (cycle_value_node->str_value_),
                            static_cast<int32_t>(cycle_value_node->str_len_));
      non_cycle_value.assign_ptr((char *) (non_cycle_value_node->str_value_),
                                static_cast<int32_t>(non_cycle_value_node->str_len_));
      if (ObCharset::case_insensitive_equal(cycle_value, non_cycle_value)) {
        ret = OB_ERR_CTE_DUPLICATE_CYCLE_NON_CYCLE_VALUE;
        LOG_WARN("cycle value for CYCLE clause must be different from the non-cycle value", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectResolver::check_cte_pseudo(const ParseNode *search_node, const ParseNode *cycle_node) {
  int ret = OB_SUCCESS;
  // For oracle error code compatibility
  if ((OB_NOT_NULL(search_node) || OB_NOT_NULL(cycle_node))
      && cte_ctx_.cte_col_names_.empty()) {
    ret = OB_ERR_CTE_NEED_COLUMN_ALIAS_LIST;
    LOG_WARN("WITH clause element did not have a column alias list", K(ret));
  }

  if (OB_NOT_NULL(search_node)) {
    if (OB_FAIL(check_search_clause(*search_node))) {
      LOG_WARN("Invalid search clause", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(cycle_node)) {
    if (OB_FAIL(check_cycle_clause(*cycle_node))) {
      LOG_WARN("Invalid cycle clause", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(cycle_node) && OB_NOT_NULL(search_node)) {
    if (OB_FAIL(check_search_cycle_set_column(*search_node, *cycle_node))) {
      LOG_WARN("Invalid search/cycle clause", K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::resolve_cte_pseudo_column(const ParseNode *search_node,
                                                const ParseNode *cycle_node,
                                                const TableItem *table_item,
                                                ObString &search_pseudo_column_name,
                                                ObString &cycle_pseudo_column_name)
{
  int ret = OB_SUCCESS;
  if(OB_ISNULL(table_item)) {
    //do nothing

  } else if (OB_ISNULL(table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the recursive union can not be null", K(ret));
  } else {

    if (OB_SUCC(ret) && OB_NOT_NULL(search_node)) {
      OZ( resolve_search_clause(*search_node, table_item, search_pseudo_column_name) );
      OZ( check_pseudo_column_name_legal(search_pseudo_column_name) );
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(cycle_node)) {
      OZ( resolve_cycle_clause(*cycle_node, table_item, cycle_pseudo_column_name) );
      OZ( check_pseudo_column_name_legal(cycle_pseudo_column_name) );
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(cycle_node)) {
    if (OB_FAIL(check_cycle_values(*cycle_node))) {
      LOG_WARN("Invalid cycle/non-cycle values", K(ret));
    }
  }

  if (OB_SUCC(ret)
      && !(table_item->ref_query_->is_recursive_union())
      && (OB_NOT_NULL(cycle_node) || OB_NOT_NULL(search_node))) {
    //oracle 默认非递归的CTE不可以使用search和cycle
    ret = OB_ERR_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE;
    LOG_WARN("SEARCH and CYCLE clauses can only be specified for recursive WITH clause elements", K(ret));
  }
  return ret;
}

int ObSelectResolver::add_parent_cte_table_item(TableItem *table_item) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_var_to_array_no_dup(parent_cte_tables_, table_item))) {
    LOG_WARN("add parent cte table failed", K(ret));
  }
  return ret;
}

int ObSelectResolver::resolve_from_clause(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(node)) {
    current_scope_ = T_FROM_SCOPE;
    ObSelectStmt *select_stmt = NULL;
    TableItem *table_item = NULL;
    CK( OB_NOT_NULL(select_stmt = get_select_stmt()),
        node->type_ == T_FROM_LIST,
        node->num_child_ >= 1);
    for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i += 1) {
      ParseNode *table_node = node->children_[i];
      CK(OB_NOT_NULL(table_node));
      const bool old_flag = session_info_->is_table_name_hidden();
      bool is_table_hidden = false;
      const ObSessionDDLInfo &ddl_info = session_info_->get_ddl_info();
      // add foreign key will use select xx from t1 minus select xx from t2, here t1 is source table, t2 is dest table
      if (ddl_info.is_ddl()) {
        if (in_set_query_) {
          is_table_hidden = is_left_child_ ? ddl_info.is_source_table_hidden() : ddl_info.is_dest_table_hidden();
        } else {
          is_table_hidden = ddl_info.is_source_table_hidden();
        }
      }
      // TODO@wenqu: wait flags from session info
      session_info_->set_table_name_hidden(is_table_hidden);
      OZ( resolve_table(*table_node, table_item) );
      session_info_->set_table_name_hidden(old_flag);
      OZ( column_namespace_checker_.add_reference_table(table_item), table_item );
      OZ( select_stmt->add_from_item(table_item->table_id_, table_item->is_joined_table()) );
      // oracle outer join will change from items
      OZ( add_from_items_order(table_item), table_item );
      if (OB_SUCC(ret)) {
        select_stmt->set_has_reverse_link(table_item->is_reverse_link_);
      }
    }
    OZ( gen_unpivot_target_column(node->num_child_, *select_stmt, *table_item) );
    OZ( check_recursive_cte_usage(*select_stmt) );
  }
  return ret;
}

int ObSelectResolver::gen_unpivot_target_column(const int64_t table_count,
    ObSelectStmt &select_stmt, TableItem &table_item)
{
  int ret = OB_SUCCESS;
  if (NULL != transpose_item_ && 1 == table_count) {
    select_stmt.set_from_pivot(transpose_item_->is_pivot());
     //mark this stmt is transpose item
    if (transpose_item_->is_unpivot() && transpose_item_->need_use_unpivot_op()) {
      //use unpivot operation
      select_stmt.set_transpose_item(const_cast<TransposeItem *>(transpose_item_));

      const int64_t old_column_count = transpose_item_->old_column_count_;
      const int64_t new_column_count = transpose_item_->unpivot_columns_.count()
                                        + transpose_item_->for_columns_.count();

      ObSelectStmt *child_select_stmt = table_item.ref_query_;
      int64_t select_item_count = 0;
      ObCollationType coll_type = CS_TYPE_INVALID;
      LOG_DEBUG("begin gen_unpivot_target_column", K(table_item), KPC(child_select_stmt));
      if (OB_ISNULL(params_.expr_factory_)
          || OB_ISNULL(session_info_)
          || OB_ISNULL(child_select_stmt)
          || FALSE_IT(select_item_count = child_select_stmt->get_select_item_size())
          || OB_UNLIKELY(new_column_count < 0)
          || OB_UNLIKELY(old_column_count < 0)
          || OB_UNLIKELY(old_column_count >= select_item_count)
          || OB_UNLIKELY((select_item_count - old_column_count) % new_column_count != 0)
          || OB_UNLIKELY((select_item_count - old_column_count) / new_column_count <= 1)
          ) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("params is invalid", K(ret), K(table_item),
                 K(params_.expr_factory_), K(old_column_count), K(new_column_count),
                 K(select_item_count), KPC(child_select_stmt));
      } else if (OB_FAIL(session_info_->get_collation_connection(coll_type))) {
        LOG_WARN("fail to get_collation_connection", K(ret));
      } else {
        const ObLengthSemantics default_ls = session_info_->get_actual_nls_length_semantics();
        ObSEArray<ObExprResType, 16> types;
        ObExprResType res_type;
        ObExprVersion dummy_op(*allocator_);

        for (int64_t colum_idx = 0; OB_SUCC(ret) && colum_idx < new_column_count; colum_idx++) {
          res_type.reset();
          types.reset();

          int64_t item_idx = old_column_count + colum_idx;
          SelectItem &first_select_item = child_select_stmt->get_select_item(item_idx);
          item_idx += new_column_count;
          if (OB_ISNULL(first_select_item.expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is null", K(ret), K(first_select_item.expr_));
          } else if (OB_FAIL(types.push_back(first_select_item.expr_->get_result_type()))) {
            LOG_WARN("fail to push left_type", K(ret));
          }
          while (OB_SUCC(ret) && item_idx < select_item_count) {
            SelectItem &tmp_select_item = child_select_stmt->get_select_item(item_idx);
            if (OB_ISNULL(tmp_select_item.expr_) ) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("expr is null", K(ret), K(tmp_select_item.expr_));
            } else if (first_select_item.expr_->get_result_type()
                       != tmp_select_item.expr_->get_result_type()) {
              const ObExprResType &first_type = first_select_item.expr_->get_result_type();
              const ObExprResType &tmp_type = tmp_select_item.expr_->get_result_type();
              if (!((first_type.is_null() && !tmp_type.is_lob())
                    || (tmp_type.is_null() && !first_type.is_lob())
                    || (first_type.is_character_type() && tmp_type.is_character_type())
                    || (ob_is_oracle_numeric_type(first_type.get_type())
                        && ob_is_oracle_numeric_type(tmp_type.get_type()))
                    || (ob_is_oracle_temporal_type(first_type.get_type())
                        && (ob_is_oracle_temporal_type(tmp_type.get_type()))))) {
                ret = OB_ERR_EXP_NEED_SAME_DATATYPE;
                LOG_WARN("expression must have same datatype as corresponding expression", K(ret),
                         K(first_type), K(tmp_type));
              } else if (first_type.is_character_type()
                         && tmp_type.is_character_type()
                         && (first_type.is_varchar_or_char() != tmp_type.is_varchar_or_char())) {
                ret = OB_ERR_CHARACTER_SET_MISMATCH;
                LOG_WARN("character set mismatch", K(ret), K(first_type), K(tmp_type));
              } else if (OB_FAIL(types.push_back(tmp_type))) {
                LOG_WARN("fail to push type", K(ret), K(tmp_type));
              }
            }
            item_idx += new_column_count;
          }

          if (OB_SUCC(ret)) {
            if (types.count() == 1) {
              res_type = first_select_item.expr_->get_result_type();
            } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(res_type,
                                                                        &types.at(0),
                                                                        types.count(),
                                                                        coll_type,
                                                                        true,
                                                                        default_ls))) {
              LOG_WARN("fail to aggregate_result_type_for_merge", K(ret), K(types));
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_UNLIKELY(ObNullType == res_type.get_type() || ObMaxType == res_type.get_type())) {
              ret = OB_ERR_INVALID_TYPE_FOR_OP;
              LOG_WARN("column type incompatible", K(ret), K(res_type));
            } else {
              item_idx = old_column_count + colum_idx;
              while (OB_SUCC(ret) && item_idx < select_item_count) {
                SelectItem &select_item = child_select_stmt->get_select_item(item_idx);
                if (select_item.expr_->get_result_type() != res_type) {
                  ObSysFunRawExpr *new_expr = NULL;
                  if (OB_FAIL(ObRawExprUtils::create_cast_expr(*params_.expr_factory_,
                                                               select_item.expr_,
                                                               res_type,
                                                               new_expr,
                                                               session_info_))) {
                    LOG_WARN("create cast expr for stmt failed", K(ret));
                  } else if (OB_FAIL(new_expr->add_flag(IS_INNER_ADDED_EXPR))) {
                    LOG_WARN("failed to add flag", K(ret));
                  } else {
                    select_item.expr_ = new_expr;
                    LOG_DEBUG("add cast for column", K(select_item), K(res_type));
                  }
                }
                item_idx += new_column_count;
              }
            }
          }
        }//end of for
      }
      LOG_DEBUG("finish gen_unpivot_target_column", K(table_item), KPC(child_select_stmt));
    }
    //reset
    transpose_item_ = NULL;
  }
  return ret;
}

int ObSelectResolver::resolve_group_clause(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  bool has_explicit_dir = false;
  ObSelectStmt *select_stmt = get_select_stmt();
  common::ObIArray<ObRawExpr*>& groupby_exprs = select_stmt->get_group_exprs();
  common::ObIArray<ObRawExpr*>& rollup_exprs = select_stmt->get_rollup_exprs();
  common::ObSEArray<OrderItem, 4> order_items;
  if (OB_ISNULL(node)) {//do nothing for has no groupby clause.
  } else if (OB_FAIL(resolve_group_by_list(node,
                                           groupby_exprs,
                                           rollup_exprs,
                                           order_items,
                                           has_explicit_dir))) {
    LOG_WARN("failed to resolve group rollup list.", K(ret));
  } else if (!has_explicit_dir) {
    /* do nothing. */
  } else if (rollup_exprs.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); i++) {
      if (OB_FAIL(select_stmt->add_rollup_dir(order_items.at(i).order_type_))) {
        LOG_WARN("failed to push back to order items.", K(ret));
      } else {/* do nothing. */}
    }
  } else if (OB_FAIL(append(select_stmt->get_order_items(), order_items))) {
      LOG_WARN("failed to append order itmes by groupby into select stmt.", K(ret));
  } else { /* do nothing. */}

  //for mysql mode, check grouping here
  if (OB_FAIL(ret) || is_oracle_mode()) {
    /*do nothing*/
  } else if (rollup_exprs.count() <= 0 && has_grouping()) {
    ret = OB_ERR_WRONG_FIELD_WITH_GROUP;
    LOG_WARN("the grouping must be with be roll up clause", K(ret));
  } else if (OB_FAIL(check_grouping_columns())) {
    LOG_WARN("failed to check grouping columns", K(ret));
  } // do nothing

  return ret;
}

int ObSelectResolver::check_rollup_clause(const ParseNode *node, bool &has_rollup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(node->children_) ||
      OB_UNLIKELY(T_GROUPBY_CLAUSE != node->type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resolver arguments", K(ret), K(node));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      const ParseNode *child_node = node->children_[i];
      if (OB_ISNULL(child_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(child_node));
      } else if (child_node->type_ == T_ROLLUP_LIST ||
                 child_node->type_ == T_CUBE_LIST ||
                 child_node->type_ == T_GROUPING_SETS_LIST ||
                 child_node->type_ == T_WITH_ROLLUP_CLAUSE) {
        has_rollup = true;
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_group_by_list(const ParseNode *node,
                                            common::ObIArray<ObRawExpr*> &groupby_exprs,
                                            common::ObIArray<ObRawExpr*> &rollup_exprs,
                                            common::ObIArray<OrderItem> &order_items,
                                            bool &has_explicit_dir)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  current_scope_ = T_GROUP_SCOPE;
  if (OB_ISNULL(node) || OB_ISNULL(node->children_) || OB_ISNULL(select_stmt) ||
      OB_UNLIKELY(T_GROUPBY_CLAUSE != node->type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resolver arguments", K(ret), K(node), K(select_stmt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      const ParseNode *child_node = node->children_[i];
      if (OB_ISNULL(child_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(child_node));
      } else {
        switch (child_node->type_) {
          case T_NULL: {
            /*compatible oracle: select c1 from t1 group by c1, (); do nothing, just skip*/
            break;
          }
          case T_GROUPBY_KEY: {
            if (OB_ISNULL(child_node->children_) || OB_UNLIKELY(child_node->num_child_ != 1)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid resolver arguments", K(ret), K(child_node));
            } else if (OB_FAIL(resolve_groupby_node(child_node->children_[0],
                                                    child_node,
                                                    groupby_exprs,
                                                    rollup_exprs,
                                                    order_items,
                                                    has_explicit_dir,
                                                    true,
                                                    0))) {// 0 level.
              LOG_WARN("failed to resolve group node.", K(ret));
            } else {/*do nothing*/}
            break;
          }
          case T_ROLLUP_LIST: {
            ObRollupItem rollup_item;
            if (OB_FAIL(resolve_rollup_list(child_node, rollup_item))) {
              LOG_WARN("failed to resolve rollup list", K(ret));
            } else if (OB_FAIL(select_stmt->add_rollup_item(rollup_item))) {
              LOG_WARN("failed to add rollup item", K(ret));
            }
            break;
          }
          case T_CUBE_LIST: {
            ObCubeItem cube_item;
            if (OB_FAIL(resolve_cube_list(child_node, cube_item))) {
              LOG_WARN("failed to resolve cube list", K(ret));
            } else if (OB_FAIL(select_stmt->add_cube_item(cube_item))) {
              LOG_WARN("failed to add cube item", K(ret));
            } else {/* do nothing */}
            break;
          }
          case T_GROUPING_SETS_LIST: {
            HEAP_VAR(ObGroupingSetsItem, grouping_sets_item) {
              if (OB_FAIL(resolve_grouping_sets_list(child_node, grouping_sets_item))) {
                LOG_WARN("failed to resolve grouping sets list", K(ret));
              } else if (OB_FAIL(select_stmt->add_grouping_sets_item(grouping_sets_item))) {
                LOG_WARN("failed to add grouping sets item", K(ret));
              } else {/*do nothing*/}
            }
            break;
          }
          case T_WITH_ROLLUP_CLAUSE: {
            if (OB_FAIL(resolve_with_rollup_clause(child_node,
                                                   groupby_exprs,
                                                   rollup_exprs,
                                                   order_items,
                                                   has_explicit_dir))) {
              LOG_WARN("failed to resolve with rollup clause", K(ret));
            } else {/*do nothing*/}
            break;
          }
          default: {
            /* won't be here */
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected group by type", K(ret), K(get_type_name(child_node->type_)));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_rollup_list(const ParseNode *node, ObRollupItem &rollup_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_UNLIKELY(node->type_ != T_ROLLUP_LIST || !lib::is_oracle_mode())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(node));
  } else {
    ObSEArray<ObRawExpr*, 4> dummy_groupby_exprs;
    ObSEArray<OrderItem, 4> dummy_order_items;
    bool dummy_has_explicit_dir = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      ObGroupbyExpr item;
      const ParseNode *group_key_node = node->children_[i];
      if (OB_ISNULL(group_key_node) ||
          OB_UNLIKELY(group_key_node->type_ != T_GROUPBY_KEY ||
                      group_key_node->num_child_ != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid resolver arguments", K(ret), K(group_key_node));
      } else if (OB_FAIL(resolve_groupby_node(group_key_node->children_[0],
                                              group_key_node,
                                              dummy_groupby_exprs,
                                              item.groupby_exprs_,
                                              dummy_order_items,
                                              dummy_has_explicit_dir,
                                              false,
                                              0))) {// 0 level.
        LOG_WARN("failed to resolve group node.", K(ret));
      } else if (OB_FAIL(rollup_item.rollup_list_exprs_.push_back(item))) {
        LOG_WARN("failed to push back item", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_grouping_sets_list(const ParseNode *node,
                                                 ObGroupingSetsItem &grouping_sets_item)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt) || OB_ISNULL(node) ||
      OB_UNLIKELY(node->type_ != T_GROUPING_SETS_LIST || !lib::is_oracle_mode())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(select_stmt), K(node));
  } else {
    ObSEArray<ObRawExpr*, 4> dummy_rollup_exprs;
    ObSEArray<OrderItem, 4> dummy_order_items;
    bool dummy_has_explicit_dir = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      const ParseNode *child_node = node->children_[i];
      if (OB_ISNULL(child_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(child_node));
      } else {
        ObGroupbyExpr item;
        switch (child_node->type_) {
          case T_NULL: {
            //for grouping sets may occur this situation:
            //  select count(c1) from t1 group by grouping sets(c1, ());
            // <==>
            //  select count(c1) from t1 group by c1 union all select count(c1) from t1;
            //we should consider it to compatible oracle better.
            if (OB_FAIL(grouping_sets_item.grouping_sets_exprs_.push_back(item))) {
              LOG_WARN("failed to push back into gs exprs.", K(ret));
            }
            break;
          }
          case T_GROUPBY_KEY: {
            ObGroupbyExpr item;
            if (OB_ISNULL(child_node->children_) || OB_UNLIKELY(child_node->num_child_ != 1)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid resolver arguments", K(ret), K(child_node));
            } else if (OB_FAIL(resolve_groupby_node(child_node->children_[0],
                                                    child_node,
                                                    item.groupby_exprs_,
                                                    dummy_rollup_exprs,
                                                    dummy_order_items,
                                                    dummy_has_explicit_dir,
                                                    true,
                                                    0))) {// 0 level.
              LOG_WARN("failed to resolve group node.", K(ret));
            } else if (OB_FAIL(grouping_sets_item.grouping_sets_exprs_.push_back(item))) {
              LOG_WARN("failed to push back into gs exprs.", K(ret));
            }
            break;
          }
          case T_ROLLUP_LIST: {
            ObRollupItem rollup_item;
            if (OB_FAIL(resolve_rollup_list(child_node, rollup_item))) {
              LOG_WARN("failed to resolve rollup list", K(ret));
            } else if (OB_FAIL(grouping_sets_item.rollup_items_.push_back(rollup_item))) {
              LOG_WARN("failed to add grouping sets item", K(ret));
            }
            break;
          }
          case T_CUBE_LIST: {
            ObCubeItem cube_item;
            if (OB_FAIL(resolve_cube_list(child_node, cube_item))) {
              LOG_WARN("failed to resolve cube list", K(ret));
            } else if (OB_FAIL(grouping_sets_item.cube_items_.push_back(cube_item))) {
              LOG_WARN("failed push back into grouping sets item", K(ret));
            } else {/* do nothing */}
            break;
          }
          default: {
            /* won't be here */
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected type", K(ret), K(get_type_name(child_node->type_)));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_cube_list(const ParseNode *node, ObCubeItem &cube_item)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> dummy_groupby_exprs;
  ObSEArray<OrderItem, 4> dummy_order_items;
  bool dummy_has_explicit_dir = false;
  if (OB_ISNULL(node) || OB_UNLIKELY(node->type_ != T_CUBE_LIST || !lib::is_oracle_mode())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(node));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      ObGroupbyExpr item;
      const ParseNode *group_key_node = node->children_[i];
      if (OB_ISNULL(group_key_node) ||
          OB_UNLIKELY(group_key_node->type_ != T_GROUPBY_KEY || group_key_node->num_child_ != 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid resolver arguments", K(ret), K(group_key_node));
      } else if (OB_FAIL(resolve_groupby_node(group_key_node->children_[0],
                                              group_key_node,
                                              dummy_groupby_exprs,
                                              item.groupby_exprs_,
                                              dummy_order_items,
                                              dummy_has_explicit_dir,
                                              false,
                                              0))) {// 0 level.
        LOG_WARN("failed to resolve group node.", K(ret));
      } else if (OB_FAIL(cube_item.cube_list_exprs_.push_back(item))) {
        LOG_WARN("failed to push back item", K(ret));
      } else { /* do nothing */ }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_with_rollup_clause(const ParseNode *node,
                                                 common::ObIArray<ObRawExpr*> &groupby_exprs,
                                                 common::ObIArray<ObRawExpr*> &rollup_exprs,
                                                 common::ObIArray<OrderItem> &order_items,
                                                 bool &has_explicit_dir)
{
  int ret = OB_SUCCESS;
  const ParseNode *sort_list_node = NULL;
  ObSelectStmt *select_stmt = get_select_stmt();
  // for: select a, sum(b) from t group by a with rollup.
  // with rollup is the children[0] and sort key list is children[1].
  if (OB_ISNULL(node) || OB_ISNULL(select_stmt) || OB_UNLIKELY(!lib::is_mysql_mode()) ||
      OB_UNLIKELY(T_WITH_ROLLUP_CLAUSE != node->type_ || node->num_child_ != 2) ||
      OB_ISNULL(sort_list_node = node->children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(node), K(sort_list_node), K(select_stmt));
  } else {
    bool has_rollup = false;
    if (node->children_[0] != NULL) {
      if (node->children_[0]->type_ != T_ROLLUP) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid type", K(get_type_name(node->children_[0]->type_)));
      } else {
        has_rollup = true;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_list_node->num_child_; ++i) {
      const ParseNode *sort_node = sort_list_node->children_[i];
      if (OB_ISNULL(sort_node) || OB_ISNULL(sort_node->children_) ||
          OB_UNLIKELY(T_SORT_KEY != sort_node->type_ || sort_node->num_child_ != 2)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid resolver arguments", K(ret));
      } else if (OB_FAIL(resolve_groupby_node(sort_node->children_[0],
                                              sort_node,
                                              groupby_exprs,
                                              rollup_exprs,
                                              order_items,
                                              has_explicit_dir,
                                              !(select_stmt->has_rollup() || has_rollup),
                                              0))) {// 0 level.
        LOG_WARN("failed to resolve group node.", K(ret));
      } else  {/*do nothing*/}
    }
  }
  return ret;
}

int ObSelectResolver::resolve_group_by_sql_expr(const ParseNode *group_node,
                                                const ParseNode *group_sort_node,
                                                common::ObIArray<ObRawExpr*> &groupby_exprs,
                                                common::ObIArray<ObRawExpr*> &rollup_exprs,
                                                common::ObIArray<OrderItem> &order_items,
                                                ObSelectStmt *select_stmt,
                                                bool &has_explicit_dir,
                                                bool is_groupby_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  bool is_oracle_compatible = is_oracle_mode();
  if (OB_ISNULL(group_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resolver arguments", K(ret));
  } else if (!is_oracle_compatible && group_node->type_ == T_INT) {
    int64_t pos = group_node->value_;
    if (pos <= 0 || pos > select_stmt->get_select_item_size()) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      char buff[OB_MAX_ERROR_MSG_LEN];
      snprintf(buff, OB_MAX_ERROR_MSG_LEN, "%ld", pos);
      ObString scope_name = ObString::make_string(get_scope_name(current_scope_));
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, (int)strlen(buff), buff, scope_name.length(), scope_name.ptr());
    }
    if (OB_SUCC(ret)) {
      expr = select_stmt->get_select_item(pos - 1).expr_;
      if (!expr) {
        ret = OB_ERR_ILLEGAL_ID;
        LOG_WARN("Can not find expression", K(expr), K(ret));
      } else if (expr->has_flag(CNT_AGG) || expr->has_flag(CNT_WINDOW_FUNC)) {
        ret = OB_WRONG_GROUP_FIELD;
        const ObString &alias_name = select_stmt->get_select_item(pos-1).alias_name_;
        LOG_USER_ERROR(OB_WRONG_GROUP_FIELD, alias_name.length(), alias_name.ptr());
      } else { /*do nothing*/ }
    }
  } else if (OB_FAIL(resolve_sql_expr(*group_node, expr))) {
    LOG_WARN("resolve sql expr failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    OrderItem order_item;
    order_item.expr_ = expr;
    if (!is_oracle_compatible &&
        group_sort_node->num_child_ == 2 &&
        NULL != group_sort_node->children_[1]) {
      has_explicit_dir = true;
      if (OB_FAIL(ObResolverUtils::set_direction_by_mode(*group_sort_node, order_item))) {
        LOG_WARN("failed to set direction by mode", K(ret));
      } else { /*do nothing.*/ }
    } else {
      order_item.order_type_ = default_asc_direction();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(order_items.push_back(order_item))) {
      LOG_WARN("failed to add order element to stmt", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (is_groupby_expr && OB_FAIL(groupby_exprs.push_back(expr))) {
      LOG_WARN("failed to add group by expression to stmt", K(ret));
    } else if(!is_groupby_expr && OB_FAIL(rollup_exprs.push_back(expr))) {
      LOG_WARN("failed to add rollup expression to stmt", K(ret));
    } else if (is_oracle_mode() && expr->has_flag(CNT_SYS_CONNECT_BY_PATH)) {
      // eg: select count*(*) from t1 connect by nocycle prior c1 > c2 group by SYS_CONNECT_BY_PATH('a','/');
      ret = OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED;
      LOG_WARN("SYS_CONNECT_BY_PATH function is not allowed here", K(ret));
    } else if (is_only_full_group_by_on(session_info_->get_sql_mode())) {
      if (OB_FAIL(standard_group_checker_.add_group_by_expr(expr))) {
        LOG_WARN("add group by expr to standard group checker failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_groupby_node(const ParseNode *group_node,
                                           const ParseNode *group_sort_node,
                                           common::ObIArray<ObRawExpr*> &groupby_exprs,
                                           common::ObIArray<ObRawExpr*> &rollup_exprs,
                                           common::ObIArray<OrderItem> &order_items,
                                           bool &has_explicit_dir,
                                           bool is_groupby_expr,
                                           int group_expr_level)
{
  int ret = OB_SUCCESS;
  bool is_oracle_compatible = is_oracle_mode();
  ObSelectStmt *select_stmt = get_select_stmt();
  bool is_stack_overflow = false;
  if (OB_ISNULL(group_node)) {
    ret = OB_INVALID_ARGUMENT; /* Won't be here */
    LOG_WARN("error group by node", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (group_node->type_ == T_EXPR_LIST) {
    /****************************************************************************
    suppport row in oracle,such as (by jiangxiu):
    select c1 from t1 group by ((c1)); ==> select c1 from t1 group by c1;
    select c1,c2 from t1 group by (c1, c2); ==> select c1,c2 from t1 group by c1, c2;
    select c1,c2 from t1 group by (c1, c2), c3; ==> select c1,c2 from t1 group by c1, c2, c3;
    select c1,c2 from t1 group by ((c1)), (c2, c3); ==> select c1,c2 from t1 group by c1, c2, c3;
    ****************************************************************************/
    if (!is_oracle_compatible) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("row is illegal in group by", K(ret));
    } else if (++group_expr_level > 1 && group_node->num_child_ > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "group by nested row");
      LOG_WARN("not valid group by expr.", K(ret));
    } else {
      ParseNode *expr_list_node = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < group_node->num_child_; i++) {
        expr_list_node = group_node->children_[i];
        if (OB_ISNULL(expr_list_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr list node is null", K(ret));
        } else if (OB_FAIL(SMART_CALL(resolve_groupby_node(expr_list_node,
                                                           group_sort_node,
                                                           groupby_exprs,
                                                           rollup_exprs,
                                                           order_items,
                                                           has_explicit_dir,
                                                           is_groupby_expr,
                                                           group_expr_level)))) {
          LOG_WARN("failed to resolve group by node.", K(ret));
        } else {
        } //do nothing.
      }
    }
  } else if (OB_FAIL(resolve_group_by_sql_expr(group_node,
                                               group_sort_node,
                                               groupby_exprs,
                                               rollup_exprs,
                                               order_items,
                                               select_stmt,
                                               has_explicit_dir,
                                               is_groupby_expr))) {
    LOG_WARN("failed to resolve group by sql expr.", K(ret));
  } else {
  } // do nothing.

  return ret;
}

int ObSelectResolver::can_find_group_column(ObRawExpr *&col_expr,
                                            const ObIArray<ObGroupingSetsItem> &grouping_sets_items,
                                            bool &can_find,
                                            ObStmtCompareContext *check_context/*default = NULL*/)
{
  int ret = OB_SUCCESS;
  can_find = false;
  for (int64_t i = 0 ; OB_SUCC(ret) && !can_find && i < grouping_sets_items.count(); ++i) {
    const ObIArray<ObGroupbyExpr> &grouping_sets_exprs =
                                                     grouping_sets_items.at(i).grouping_sets_exprs_;
    if (OB_FAIL(can_find_group_column(col_expr,
                                      grouping_sets_items.at(i).rollup_items_,
                                      can_find,
                                      check_context))) {
      LOG_WARN("failed to failed to find group column.");
    } else if (!can_find && OB_FAIL(can_find_group_column(col_expr,
                                                          grouping_sets_items.at(i).cube_items_,
                                                          can_find,
                                                          check_context))) {
      LOG_WARN("failed to find group column.");
    } else {
      for (int64_t j = 0 ; OB_SUCC(ret) && !can_find && j < grouping_sets_exprs.count(); ++j) {
        if (OB_FAIL(can_find_group_column(col_expr,
                                          grouping_sets_exprs.at(j).groupby_exprs_,
                                          can_find,
                                          check_context))) {
          LOG_WARN("failed to find group column.", K(ret));
        } else {
          /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObSelectResolver::can_find_group_column(ObRawExpr *&col_expr,
                                            const ObIArray<ObRollupItem> &rollup_items,
                                            bool &can_find,
                                            ObStmtCompareContext *check_context/*default = NULL*/)
{
  int ret = OB_SUCCESS;
  can_find = false;
  for (int64_t i = 0 ; OB_SUCC(ret) && !can_find && i < rollup_items.count(); ++i) {
    const ObIArray<ObGroupbyExpr> &rollup_list_exprs = rollup_items.at(i).rollup_list_exprs_;
    for (int64_t j = 0 ; OB_SUCC(ret) && !can_find && j < rollup_list_exprs.count(); ++j) {
      if (OB_FAIL(can_find_group_column(col_expr,
                                        rollup_list_exprs.at(j).groupby_exprs_,
                                        can_find,
                                        check_context))) {
        LOG_WARN("failed to find group column.", K(ret));
      } else {
        /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSelectResolver::can_find_group_column(ObRawExpr *&col_expr,
                                            const ObIArray<ObCubeItem> &cube_items,
                                            bool &can_find,
                                            ObStmtCompareContext *check_context/*default = NULL*/)
{
  int ret = OB_SUCCESS;
  can_find = false;
  for (int64_t i = 0 ; OB_SUCC(ret) && !can_find && i < cube_items.count(); ++i) {
    const ObIArray<ObGroupbyExpr> &cube_list_exprs = cube_items.at(i).cube_list_exprs_;
    for (int64_t j = 0 ; OB_SUCC(ret) && !can_find && j < cube_list_exprs.count(); ++j) {
      if (OB_FAIL(can_find_group_column(col_expr,
                                        cube_list_exprs.at(j).groupby_exprs_,
                                        can_find,
                                        check_context))) {
        LOG_WARN("failed to find group column.", K(ret));
      } else {
        /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSelectResolver::can_find_group_column(ObRawExpr *&col_expr,
                                            const common::ObIArray<ObRawExpr*> &exprs,
                                            bool &can_find,
                                            ObStmtCompareContext *check_context/*default = NULL*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(col_expr));
  } else {
    can_find = false;
    for (int64_t i = 0 ; OB_SUCC(ret) && !can_find && i < exprs.count(); ++i) {
      ObRawExpr *raw_expr = NULL;
      if (OB_ISNULL(raw_expr = exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the expr in groups is null", K(exprs));
      } else if (col_expr->same_as(*raw_expr, check_context)) {
        can_find = true;
        col_expr = raw_expr;
      }
    }
  }
  return ret;
}

int ObSelectResolver::check_grouping_columns()
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("select_stmt is null", K(select_stmt), K_(session_info));
  } else {
    common::ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      if (OB_ISNULL(select_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(recursive_check_grouping_columns(select_stmt, select_items.at(i).expr_, false))) {
        LOG_WARN("failed to recursive check grouping columns", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectResolver::check_grouping_columns(ObSelectStmt &stmt, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  bool find = false;
  /*
   * bugfix:
   * for grouping/grouping_id:
   * select grouping(1+1) from t1 group by rollup(1+1). 
   */
  ObStmtCompareContext questionmark_checker;
  questionmark_checker.reset();
  questionmark_checker.override_const_compare_ = true;
  questionmark_checker.ora_numeric_compare_ = true;
  if (OB_ISNULL(params_.query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer.", K(ret));
  } else if (FALSE_IT(questionmark_checker.init(&params_.query_ctx_->calculable_items_))) {
    // skip
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr", K(ret));
  } else if (OB_FAIL(can_find_group_column(expr,
                                           stmt.get_rollup_exprs(),
                                           find,
                                           &questionmark_checker))) {
    LOG_WARN("failed to find group column.", K(ret));
  } else if (!find && is_oracle_mode()
             && OB_FAIL(can_find_group_column(expr,
                                              stmt.get_group_exprs(),
                                              find,
                                              &questionmark_checker))) {
    LOG_WARN("failed to find group column.", K(ret));
  } else if (!find && is_oracle_mode()
             && OB_FAIL(can_find_group_column(expr,
                                              stmt.get_grouping_sets_items(),
                                              find,
                                              &questionmark_checker))) {
    LOG_WARN("failed to find group column.", K(ret));
  } else if (!find && is_oracle_mode()
             && OB_FAIL(can_find_group_column(expr,
                                              stmt.get_rollup_items(),
                                              find,
                                              &questionmark_checker))) {
    LOG_WARN("failed to find group column.", K(ret));
  } else if (!find && is_oracle_mode()
             && OB_FAIL(can_find_group_column(expr,
                                              stmt.get_cube_items(),
                                              find,
                                              &questionmark_checker))) {
    LOG_WARN("failed to find group column.", K(ret));
  } else if (!find) {
    ret = OB_ERR_WRONG_FIELD_WITH_GROUP;
    LOG_WARN("the grouping by column must be a group by column", K(ret));
  } else {
    // add constraints.
    for (int64_t i = 0; OB_SUCC(ret) && i < questionmark_checker.equal_param_info_.count(); i++) {
      if (OB_FAIL(params_.query_ctx_->all_equal_param_constraints_.push_back(
                                              questionmark_checker.equal_param_info_.at(i)))) {
        LOG_WARN("fail to push back equal constraints", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_having_clause(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (node) {
    current_scope_ = T_HAVING_SCOPE;
    if (OB_FAIL(resolve_and_split_sql_expr_with_bool_expr(*node,
                                                  select_stmt->get_having_exprs()))) {
      LOG_WARN("resolve and split sql expr failed", K(ret));
    } else { /*do nothing.*/ }
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_having_expr_size(); i++) {
      ObRawExpr* expr = select_stmt->get_having_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL ptr", K(ret));
      } else if (OB_FAIL(recursive_check_grouping_columns(select_stmt, expr, false))) {
        LOG_WARN("failed to recursive check grouping columns", K(ret));
      } else if (expr->has_flag(CNT_ROWNUM)
          || expr->has_flag(CNT_LEVEL)
          || expr->has_flag(CNT_CONNECT_BY_ISLEAF)
          || expr->has_flag(CNT_CONNECT_BY_ISCYCLE)
          || expr->has_flag(CNT_CONNECT_BY_ROOT)){
        set_having_has_self_column();
      } else { /*do nothing.*/ }
    }
  }
  return ret;
}

int ObSelectResolver::get_refindex_from_named_windows(const ParseNode *ref_name_node,
                                                      const ParseNode *node,
                                                      int64_t& ref_idx)
{
  int ret = OB_SUCCESS;
  ref_idx = -1;
  if (OB_ISNULL(ref_name_node) || OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref name node or node is NULL ptr", K(ret));
  } else {
    ObString ref_name(static_cast<int32_t>(ref_name_node->str_len_),
                      ref_name_node->str_value_);
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      ParseNode *cur_named_win_node = node->children_[i];
      ParseNode *cur_name_node = NULL;
      if (OB_ISNULL(cur_named_win_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur name win node is NULL ptr", K(ret));
      } else if (OB_ISNULL(cur_name_node = cur_named_win_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur name node is NULL ptr", K(ret));
      } else {
        ObString cur_ref_name(static_cast<int32_t>(cur_name_node->str_len_),
                              cur_name_node->str_value_);
        if (ObCharset::case_insensitive_equal(ref_name, cur_ref_name)) {
          ref_idx = i;
          break;
        }
      }
    }
  }
  return ret;
}

int ObSelectResolver::check_duplicated_name_window(ObString &name,
                                                   const ObIArray<ObString> &resolved_name_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < resolved_name_list.count(); ++i) {
    const ObString &cur_name = resolved_name_list.at(i);
    if (OB_UNLIKELY(ObCharset::case_insensitive_equal(name, cur_name))) {
      ret =  OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "duplicate window name");
      LOG_WARN("duplicate window name", K(name), K(cur_name), K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::mock_to_named_windows(ObString &name,
                                            ParseNode *win_node)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt) || OB_ISNULL(win_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt or win node unexpected null.", K(ret));
  } else {
    const ParseNode *mock_node = NULL;
    ObString win_str(win_node->str_len_, win_node->str_value_);
    ObSqlString sql_str;
    ObRawExpr *expr = NULL;
    //bug18840807, 此时frame的常量已经参数化, 重新resolve会导致参数化信息丢失, cg时期望para实际常量;
    //后续若计划重用执行阶段会取错常量值, 这里修改了parse_node的窗口指针使用已参数化的
    if (OB_FAIL(sql_str.append_fmt("COUNT(1) OVER %.*s",
                                   win_str.length(),
                                   win_str.ptr()))) {
      LOG_WARN("fail to concat string", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::parse_expr_node_from_str(sql_str.string(),
                                                                params_.session_info_->get_charsets4parser(),
                                                                params_.expr_factory_->get_allocator(),
                                                                mock_node))) {
      LOG_WARN("parse expr node from string failed", K(ret));
    } else if (2 != mock_node->num_child_
        || T_WIN_NEW_GENERALIZED_WINDOW != mock_node->children_[1]->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parse result is not expected", K(ret), K(mock_node->num_child_), K(mock_node->type_));
    } else {
      mock_node->children_[1] = win_node;
    }

    if (OB_FAIL(ret)) {
      //do nothing...
    } else if (OB_FAIL(resolve_sql_expr(*mock_node, expr))) {
      LOG_WARN("failed to resolve sql expr failed", K(ret));
      if (OB_EER_WINDOW_NO_REDEFINE_ORDER_BY == ret && OB_NOT_NULL(win_node->children_[0])) {
        LOG_USER_ERROR(OB_EER_WINDOW_NO_REDEFINE_ORDER_BY, name.length(), name.ptr(),
            (int)(win_node->children_[0]->str_len_), win_node->children_[0]->str_value_);
      }
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(expr), K(ret));
    } else if (!expr->is_win_func_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ptr", K(expr->get_expr_type()), K(ret));
    } else {
      ObWinFunRawExpr *win_expr = static_cast<ObWinFunRawExpr *>(expr);
      win_expr->set_win_name(name);
      select_stmt->get_window_func_exprs().pop_back();
      ret = select_stmt->get_window_func_exprs().push_back(win_expr);
    }
  }
  return ret;
}

int ObSelectResolver::resolve_named_windows_clause(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {// no named windows.
  } else if (OB_UNLIKELY(node->type_ != T_WIN_NAMED_WINDOWS)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(node->type_), K(ret));
  } else if (OB_UNLIKELY(node->num_child_ > OB_MAX_NAMED_WINDOW_FUNCTION_NUM)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "named windows more than 127");
    LOG_WARN("too many windows", K(ret));
  } else {
    current_scope_ = T_NAMED_WINDOWS_SCOPE;
    int64_t ref_list_cnt = 0; // 引用关系链表
    int64_t ref_list[OB_MAX_NAMED_WINDOW_FUNCTION_NUM];
    bool resolved[OB_MAX_NAMED_WINDOW_FUNCTION_NUM] = { false };
    ObSEArray<ObString, 32> resolved_name_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      if (resolved[i]) {
        continue;
      }
      // 按依赖关系依次解析，优先解析被引用的window
      ParseNode *name_node = NULL;
      ParseNode *win_node = NULL;
      ParseNode *named_win_node = NULL;
      ref_list[ref_list_cnt++] = i;
      // named_window             ->   named_win_node   ->   window w1 as (w2 partition by c)
      // name_ob                  ->   name_node        ->   w1
      // new_generalized_window   ->   win_node         ->   w2 partition by c
      while (OB_SUCC(ret) && ref_list_cnt > 0) {
        LOG_DEBUG("current window", K(ref_list[ref_list_cnt - 1]), K(ref_list_cnt));
        named_win_node = node->children_[ref_list[ref_list_cnt - 1]];
        if (OB_ISNULL(named_win_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr",  K(ret));
        } else if (OB_ISNULL(name_node = named_win_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret));
        } else if (OB_ISNULL(win_node = named_win_node->children_[1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret));
        } else {
          // 1. 未引用任何window
          // 2. 引用已经解析好的window
          // 这两种情况会直接解析当前window，否则转而解析被引用的window
          bool resolve_expr = false;
          int64_t ref_idx = -1;
          ParseNode *ref_name_node = win_node->children_[0];
          if (NULL == ref_name_node) {
            resolve_expr = true;
          } else if (OB_FAIL(get_refindex_from_named_windows(ref_name_node,
                                                             node,
                                                             ref_idx))) {
            LOG_WARN("failed to get ref index from named windows.", K(ret));
          } else if (OB_UNLIKELY(-1 == ref_idx)) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "ref not existed window");
            LOG_WARN("ref window not exist", K(ret));
          } else if ((ref_list + ref_list_cnt) != std::find(ref_list,
                                                            ref_list + ref_list_cnt,
                                                            ref_idx)) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "circle ref window");
            LOG_WARN("circle ref window not supported", K(ref_list), K(ref_idx), K(ret));
          } else if (resolved[ref_idx]) {
            resolve_expr = true;
          } else {
            resolve_expr = false;
            ref_list[ref_list_cnt++] = ref_idx;
          }
          if (OB_SUCC(ret) && resolve_expr) {
            ObString name(static_cast<int32_t>(name_node->str_len_),
                          name_node->str_value_);
            if (OB_FAIL(check_duplicated_name_window(name, resolved_name_list))) {
              LOG_WARN("has duplicated name widow when check them.", K(ret));
            } else if (OB_FAIL(resolved_name_list.push_back(name))) {
              LOG_WARN("failed to push back win name", K(ret));
            } else if (OB_FAIL(mock_to_named_windows(name,
                                                     win_node))) {
              LOG_WARN("failed to mock to name window", K(ret));
            } else {
              resolved[ref_list[ref_list_cnt - 1]] = true;
              ref_list_cnt--;
            }
          }
        }
      }
    }
    LOG_DEBUG("resolve_named_windows_clause finish", K(ret));
  }
  return ret;
}

int ObSelectResolver::resolve_into_const_node(const ParseNode *node, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null", K(ret));
  } else if (T_CHAR == node->type_ || T_VARCHAR == node->type_) {
    ObCollationType cs_type = params_.session_info_->get_local_collation_connection();
    ObString node_str(node->str_len_, node->str_value_);
    if (lib::is_oracle_mode()) {
      OZ (ObResolverUtils::escape_char_for_oracle_mode(*allocator_, node_str, cs_type));
    }
    if (OB_SUCC(ret)) {
      obj.set_varchar(node_str);
      obj.set_collation_type(cs_type);
    }
  } else if (T_HEX_STRING == node->type_) {
    ObString node_str(node->str_len_, node->str_value_);
    obj.set_varchar(node_str);
    obj.set_collation_type(CS_TYPE_BINARY);
  } else if (T_QUESTIONMARK == node->type_) {
    obj.set_unknown(node->value_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type must be varchar or ?", K(ret), K(node->type_));
  }
  return ret;
}

int ObSelectResolver::resolve_into_field_node(
  const ParseNode *list_node,
  ObSelectIntoItem &into_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(list_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("str node is null", K(ret));
  } else {
    for (int32_t i = 0 ; i < list_node->num_child_ ; ++i) {
      ParseNode *node = list_node->children_[i];
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("str node or into_item is null", K(ret));
      } else if (T_FIELD_TERMINATED_STR == node->type_) {
        if (OB_FAIL(resolve_into_const_node(node->children_[0], into_item.field_str_))) {
          LOG_WARN("resolve into outfile field str", K(ret));
        }
      } else if (T_OPTIONALLY_CLOSED_STR == node->type_
                 || T_CLOSED_STR == node->type_) {
        if (node->num_child_ != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child num should be one", K(ret));
        } else if (node->children_[0]->str_len_ == 0 || node->children_[0]->str_len_ == 1) {
          into_item.closed_cht_.meta_.set_char();
          into_item.closed_cht_.set_char_value(node->children_[0]->str_value_,
                                               node->children_[0]->str_len_);
          into_item.closed_cht_.set_collation_type(params_.session_info_->get_local_collation_connection());
        } else {
          ret = OB_WRONG_FIELD_TERMINATORS;
          LOG_WARN("closed str should be a character", K(ret), K(node->children_[0]->str_value_));
        }
        if (T_OPTIONALLY_CLOSED_STR == node->type_) {
          into_item.is_optional_ = true;
        }
      } else if (T_ESCAPED_STR == node->type_) {
        if (node->num_child_ != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child num should be one", K(ret));
        } else if (node->children_[0]->str_len_ == 0 || node->children_[0]->str_len_ == 1) {
          into_item.escaped_cht_.meta_.set_char();
          into_item.escaped_cht_.set_char_value(node->children_[0]->str_value_,
                                                node->children_[0]->str_len_);
          into_item.escaped_cht_.set_collation_type(params_.session_info_->get_local_collation_connection());
        } else {
          ret = OB_WRONG_FIELD_TERMINATORS;
          LOG_WARN("escaped str should be a character", K(ret), K(node->children_[0]->str_value_));
        }
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObSelectResolver::resolve_into_line_node(const ParseNode *list_node, ObSelectIntoItem &into_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(list_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("str node is null", K(ret));
  } else {
    for (int32_t i = 0 ; i < list_node->num_child_ ; ++i) {
      ParseNode *str_node = list_node->children_[i];
      if (OB_ISNULL(str_node)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (T_LINE_TERMINATED_STR == str_node->type_) {
        if (OB_FAIL(resolve_into_const_node(str_node->children_[0], into_item.line_str_))) {
          LOG_WARN("resolve into outfile field str", K(ret));
        }
      } else {
        // escape
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_into_file_node(const ParseNode *list_node, ObSelectIntoItem &into_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(list_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("list node is null", K(ret));
  } else {
    for (int32_t i = 0 ; OB_SUCC(ret) && i < list_node->num_child_; ++i) {
      ParseNode *node = list_node->children_[i];
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child of list node is null", K(ret));
      } else if (T_SINGLE_OPT == node->type_) {
        if (node->num_child_ != 1 || OB_ISNULL(node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected single node", K(ret));
        } else {
          into_item.is_single_ = node->children_[0]->value_;
        }
      } else if (T_MAX_FILE_SIZE == node->type_) {
        if (OB_FAIL(resolve_max_file_size_node(node, into_item))) {
          LOG_WARN("failed to resolve max file size", K(ret));
        }
      } else {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("child of into file node has wrong type", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_max_file_size_node(const ParseNode *file_size_node, ObSelectIntoItem &into_item)
{
  int ret = OB_SUCCESS;
  ParseNode *child = NULL;
  int64_t parse_int_value = 0;
  if (OB_ISNULL(file_size_node) || T_MAX_FILE_SIZE != file_size_node->type_
      || file_size_node->num_child_ != 1 || OB_ISNULL(child = file_size_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected file size node", K(ret));
  } else if (T_INT == child->type_) {
    parse_int_value = static_cast<int64_t>(child->value_);
  } else if (T_VARCHAR == child->type_) {
    if (OB_FAIL(resolve_varchar_file_size(child, parse_int_value))) {
      LOG_WARN("failed to resolve varchar value", K(ret));
    }
  } else {
    ret = OB_ERR_PARSE_SQL;
    LOG_WARN("child of max file size node has wrong type", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(parse_int_value <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file size value should be positive", K(ret), K(parse_int_value));
  } else {
    into_item.max_file_size_ = parse_int_value;
  }
  return ret;
}

int ObSelectResolver::resolve_varchar_file_size(const ParseNode *child, int64_t &parse_int_value) const
{
  int ret = OB_SUCCESS;
  bool valid = false;
  common::ObSqlString buf;
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("varchar node get unexpected null", K(ret));
  } else if (OB_FAIL(buf.append(child->str_value_, child->str_len_))) {
    LOG_WARN("failed to assign child str", K(ret), K(child->str_value_), K(child->str_len_));
  } else {
    parse_int_value = common::ObConfigCapacityParser::get(buf.ptr(), valid, false, true);
    if (!valid) {
      ret = OB_ERR_PARSE_SQL;
      LOG_WARN("failed to parse file size varchar value to int", K(ret), K(buf));
    }
  }
  return ret;
}
int ObSelectResolver::resolve_into_clause(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (NULL != node) {
    current_scope_ = T_INTO_SCOPE;
    ObSelectIntoItem *into_item = NULL;
    ObSelectStmt *select_stmt = get_select_stmt();
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alloctor is null", K(ret));
    } else if (OB_ISNULL(into_item = static_cast<ObSelectIntoItem *>
                         (allocator_->alloc(sizeof(ObSelectIntoItem))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("into item is null", K(ret));
    } else if (OB_ISNULL(select_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select stmt is NULL", K(ret));
    } else if (OB_UNLIKELY(is_sub_stmt_)) { //in subquery
      ret = OB_INAPPROPRIATE_INTO;
      LOG_WARN("select into can not in subquery", K(ret));
    } else if (OB_UNLIKELY(is_in_set_query())) {
      ret = OB_INAPPROPRIATE_INTO;
      LOG_WARN("select into can not in set query", K(ret));
    } else if (is_mysql_mode() && params_.is_from_create_view_) {
      ret = OB_ERR_VIEW_SELECT_CONTAIN_INTO;
      LOG_WARN("View's SELECT contains a 'INTO' clause.", K(ret));
    } else {
      new(into_item) ObSelectIntoItem();
      into_item->into_type_ = node->type_;
      if (T_INTO_OUTFILE == node->type_) { // into outfile
        if (OB_FAIL(resolve_into_const_node(node->children_[0], into_item->outfile_name_))) { //name
          LOG_WARN("resolve into outfile name failed", K(ret));
        } else if (NULL != node->children_[1]) { // charset
          ObCharsetType charset_type = CHARSET_INVALID;
          ObString charset(node->children_[1]->str_len_, node->children_[1]->str_value_);
          if (CHARSET_INVALID == (charset_type = ObCharset::charset_type(charset.trim()))) {
            ret = OB_ERR_UNKNOWN_CHARSET;
            LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset.length(), charset.ptr());
          } else if (CHARSET_UTF16 == charset_type) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("select into outfile character set utf16", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "upload data using utf16");
          } else {
            into_item->cs_type_ = ObCharset::get_default_collation(charset_type);
          }
        }
        if (OB_SUCC(ret) && NULL !=  node->children_[2]) { //field
          if (OB_FAIL(resolve_into_field_node(node->children_[2], *into_item))) {
            LOG_WARN("reosolve into field node failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && NULL != node->children_[3]) { // line
          if (OB_FAIL(resolve_into_line_node(node->children_[3], *into_item))) {
            LOG_WARN("reosolve into line node failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && NULL != node->children_[4]) { // file: single & max_file_size
          if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_1_0) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support to use file option during updating", K(ret));
	          LOG_USER_ERROR(OB_NOT_SUPPORTED, "use file option during updating");
          } else if (OB_FAIL(resolve_into_file_node(node->children_[4], *into_item))) {
            LOG_WARN("reosolve into line node failed", K(ret));
          }
        }
      } else if (T_INTO_DUMPFILE  == node->type_) { // into dumpfile
        if (OB_FAIL(resolve_into_const_node(node->children_[0], into_item->outfile_name_))) {
          LOG_WARN("resolve into outfile name failed", K(ret));
        }
      } else if (T_INTO_VARIABLES == node->type_) { // into @x,@y....
        if (OB_FAIL(resolve_into_variables(node,
                                           into_item->user_vars_,
                                           into_item->pl_vars_,
                                           select_stmt))) {
          LOG_WARN("resolve into variable failed", K(ret));
        }
      } else {
        //do nothing
      }
      if (OB_SUCC(ret)) {
        select_stmt->set_select_into(into_item);
      }
    }
  }
  return ret;
}


int ObSelectResolver::resolve_column_ref_in_all_namespace(
  const ObQualifiedName &q_name,
  ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  //first, find column in current namespace
  //mysql5.6中alias name不能出现在where子句中，但是可以出现在group by、having、order by子句中
  //如果普通列和alias name重复，那么在group by、having子句中优先使用基础列，并汇报WARNNING
  //order by子句中，优先使用alias name
  if (OB_UNLIKELY(T_ORDER_SCOPE == current_scope_)) {
    if ((lib::is_mysql_mode() && params_.is_column_ref_) || lib::is_oracle_mode()) {
      // should raise an error
      // select id + 1 as data, data from test order by data
      // select id as data, data from test order by data
      if (OB_FAIL(resolve_column_ref_alias_first(q_name, real_ref_expr))) {
        LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve column ref alias first failed", K(ret), K(q_name));
      }
    } else {
      // if the item behind order by is an expr in mysql mode, then we should resolve column
      // select id as data, data from test order by data + 1;
      // select id as data, data from test order by sum(data);
      // select id + 1 as data, data from test order by sum(data);
      if (OB_FAIL(resolve_column_ref_table_first(q_name, real_ref_expr, false))) {
        LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve column ref table first failed", K(ret), K(q_name));
      }
    }
  } else if (OB_UNLIKELY(T_HAVING_SCOPE == current_scope_)) {
    if (OB_FAIL(resolve_column_ref_for_having(q_name, real_ref_expr))) {
      LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve column ref for having failed", K(ret), K(q_name));
    }
  }  else if (T_WITH_CLAUSE_SEARCH_SCOPE == current_scope_) {
    if (OB_FAIL(resolve_column_ref_for_search(q_name, real_ref_expr))) {
      LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve column ref for search failed",  K(ret), K(q_name));
    }
  } else {
    //search column in table columns first
    if (OB_FAIL(resolve_column_ref_table_first(q_name, real_ref_expr))) {
      LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve column ref table first failed", K(ret), K(q_name));
    }
  }
  ObQueryRefRawExpr *query_ref = NULL;
  for (ObDMLResolver *cur_resolver = get_parent_namespace_resolver();
      OB_ERR_BAD_FIELD_ERROR == ret && cur_resolver != NULL;
      cur_resolver = cur_resolver->get_parent_namespace_resolver()) {
    ObRawExpr *exec_param = NULL;
    ObIArray<ObExecParamRawExpr*> *query_ref_exec_params = NULL;
    //for insert into t1 values((select c1 from dual)) ==> can't check column c1 in t1;
    if (cur_resolver->get_basic_stmt() != NULL &&
        cur_resolver->get_basic_stmt()->is_insert_stmt()) {
    //INSERT INTO t0 values (1,10) ON DUPLICATE KEY UPDATE b = (SELECT y FROM t1 WHERE x = values(a));
    // ==> should check column a in duplicate key update;
      if (static_cast<ObDelUpdResolver*>(cur_resolver)->is_resolve_insert_update()) {
        if (OB_FAIL(cur_resolver->resolve_column_ref_expr(q_name, real_ref_expr))) {
          LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve column ref failed", K(ret), K(q_name));
        }
      }
    } else if (OB_FAIL(cur_resolver->resolve_column_ref_for_subquery(q_name, real_ref_expr))) {
      LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve column for subquery failed", K(ret), K(q_name));
    }
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_ISNULL(query_ref_exec_params = cur_resolver->get_query_ref_exec_params())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no subquery is found", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::get_exec_param_expr(*params_.expr_factory_,
                                                           query_ref_exec_params,
                                                           real_ref_expr,
                                                           exec_param))) {
      LOG_WARN("failed to get exec param expr", K(ret));
    } else {
      /// succeed to resolve the correlated column, do the replace here
      real_ref_expr = exec_param;
    }
  }
  return ret;
}

int ObSelectResolver::resolve_column_ref_expr(
  const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  //设置resolve的列是否存在本层的聚集函数中，如果存在聚集函数中，本层的列可以不需要进行only full group by约束
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info is null");
  } else  if (OB_FALSE_IT(const_cast<ObQualifiedName&>(q_name).current_resolve_level_ = current_level_)) {

  } else if (q_name.parents_expr_info_.has_member(IS_AGG) &&
             !q_name.parents_expr_info_.has_member(IS_WINDOW_FUNC)) {
    const_cast<ObQualifiedName&>(q_name).parent_aggr_level_ = current_level_;
  } else {
    const_cast<ObQualifiedName&>(q_name).parent_aggr_level_ = parent_aggr_level_;
  }
  if (OB_SUCC(ret) && OB_FAIL(resolve_column_ref_in_all_namespace(q_name, real_ref_expr))) {
    LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve column ref in all namespace failed", K(ret));
  }
  return ret;
}

//resolve table column reference
//select stmt can access joined table column, generated table column or base table column
int ObSelectResolver::resolve_table_column_ref(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr)
{
  //search order
  //1. joined table column
  //2. basic table column or generated table column
  //3. object (sequence)
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve_table_column_expr(q_name, real_ref_expr))) {
    LOG_WARN("resolve table column expr failed", K(ret), K(q_name), K(lbt()));
  } else if (column_need_check_group_by(q_name)) {
    //任何一个表达式引用到的本层的列，都必须记录到standard group checker中，并进行only full group by检查
    // In Oracle mode, it will add all referenced columns, but group by checker will check every expression recursively intead of one by one column
    if (OB_FAIL(standard_group_checker_.add_unsettled_column(real_ref_expr))) {
      LOG_WARN("add unsettled column failed", K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::resolve_alias_column_ref(
  const ObQualifiedName &q_name,
  ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  //resolve column in target list can't reference the alias column
  //such as select 1 as a, (select a) ->error
  //select 1 as a, a; ->error
  ObSelectStmt *select_stmt = get_select_stmt();
  real_ref_expr = NULL;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(q_name.ref_expr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("select stmt is null", K(select_stmt), K_(q_name.ref_expr));
  } else if ((!lib::is_oracle_mode() && current_level_ < q_name.current_resolve_level_) ||current_scope_ != T_FIELD_LIST_SCOPE) {
    // mysql can use parent filed list, so test current_level_ < q_name.current_resolve_level_
    // such as create table t1(c1 int);create table t2(c1 int);select c1 as id /*(level=0)*/, (/*(level=1)*/select c1 from t1 where c1 = id) from t2;
    const SelectItem *cur_item = NULL;
    if (!q_name.tbl_name_.empty()) {
      //select t1.c1 from t1 having t1.c1 > 0   should check if t1 is correct
      //select t1.c1 from t1 having t2.c1 > 0
      //select c1, c2 as c1 from t1 having t1.c1 > 0 //should choose c1
      //select t1.c1 as cc from t1 having t1.c1 > 0; //should found c1
      //select t1.c1 as cc from t1 having t2.c1 > 0; //should not found c1
      for (int32_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
        bool is_hit = false;
        cur_item = &select_stmt->get_select_item(i);
        if (cur_item->expr_ != NULL) {
          ObColumnRefRawExpr *col_expr = NULL;
          if (cur_item->expr_->is_column_ref_expr()) {
            col_expr = static_cast<ObColumnRefRawExpr *>(cur_item->expr_);
          }
          if (NULL == col_expr) {
            continue;
          }
          if (OB_FAIL(ObResolverUtils::check_column_name(
                        session_info_, q_name, *col_expr, is_hit))) {
            LOG_WARN("check column name failed", K(ret));
          } else if (is_hit) {
            if (NULL == real_ref_expr) {
              ret = column_namespace_checker_.check_column_existence_in_using_clause(
                                    col_expr->get_table_id(), col_expr->get_column_name());
              if (OB_SUCC(ret)) {
                real_ref_expr = col_expr;
              }
            } else if (real_ref_expr != col_expr) {
              ret = OB_NON_UNIQ_ERROR;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cur item expr is null");
        }
      }
      if (OB_SUCC(ret) && NULL == real_ref_expr) {
        ret = OB_ERR_BAD_FIELD_ERROR;
      }
    } else {
      // first loop, find the matched alias column with expr
      for (int32_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
        cur_item = &select_stmt->get_select_item(i);
        if (ObCharset::case_insensitive_equal(q_name.col_name_, cur_item->alias_name_)) {
          /*
           * for oracle mode, column uniqueness is checked among all tables
           * for mysql mode, column uniqueness is only checked among select items
           * for example, create table t1(a int, b int, c int); create table t2(a int, b int, c int);
           * select t1.a, t1.b from t1 left join t2 on t1.a = t2.a order by b
           * for mysql mode, it is ok, and b refer to t1.b
           * for oracle mode, it will get error "column ambiguously defined"
           */
          if (NULL == real_ref_expr) {
            if (lib::is_oracle_mode() && cur_item->expr_->is_column_ref_expr() &&
                !cur_item->is_real_alias_) {
              const TableItem *table_item = NULL;
              ret = column_namespace_checker_.check_table_column_namespace(q_name, table_item);
            }
            if (OB_SUCC(ret)) {
             real_ref_expr = cur_item->expr_;
            }
          } else if (real_ref_expr != cur_item->expr_) {
            ret = OB_NON_UNIQ_ERROR;
          }
        }
      }
      if (OB_SUCC(ret) && NULL == real_ref_expr) {
        // might not found, the caller will check the col_item is NULL or not
        //select c1 as cc from t1 having c1 > 0; //should found c1
        for (int32_t i = 0; i < select_stmt->get_select_item_size(); ++i) {
          cur_item = &select_stmt->get_select_item(i);
          if (cur_item->is_real_alias_ && cur_item->expr_->is_column_ref_expr()) {
            if (ObCharset::case_insensitive_equal(q_name.col_name_, cur_item->expr_name_)) {
              real_ref_expr = cur_item->expr_;
              break;
            }
          }
        }
      }
      if (OB_SUCC(ret) && NULL == real_ref_expr) {
        ret = OB_ERR_BAD_FIELD_ERROR;
      }
    }
  } else {
    ret = OB_ERR_BAD_FIELD_ERROR;
  }
  //group by clause can't use aggregate function alias name
  if (OB_SUCC(ret) && T_GROUP_SCOPE == current_scope_) {
    if (real_ref_expr->has_flag(CNT_AGG)) {
      ret = OB_ILLEGAL_REFERENCE;
      // 为了能给group by报错
      // 兼容mysql报错,
      // select count(c1) as c from t1 group by c和select count(c1)
      // as c from t1 group by (select c)报错不同
    }
  }
  // subquery cannot ref parent aggr/window function alias
  // SELECT SUM(c1) OVER () AS c, (SELECT SUM(c) from t2)  FROM t1;
  // SELECT SUM(c1) AS c, (SELECT SUM(c) from t2)  FROM t1;
  if(OB_SUCC(ret) && current_level_ < q_name.current_resolve_level_ && T_FIELD_LIST_SCOPE == current_scope_) {
    if (real_ref_expr->has_flag(CNT_AGG) ||
        real_ref_expr->has_flag(CNT_WINDOW_FUNC)) {
      ret = OB_ILLEGAL_REFERENCE;
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(wrap_alias_column_ref(q_name, real_ref_expr))) {
    LOG_WARN("wrap alias column ref failed", K(ret), K(q_name));
  }
  return ret;
}

int ObSelectResolver::resolve_column_ref_in_group_by(
  const ObQualifiedName &q_name,
  ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("select stmt is null");
  } else if (!is_oracle_mode() && q_name.parent_aggr_level_ < current_level_) {
    //the column don't located in aggregate function in having clause
    // resolve column refs from group by and rollup exprs
    ObSEArray<ObRawExpr*, 16> group_and_rollup_exprs;
    if (OB_FAIL(append(group_and_rollup_exprs, select_stmt->get_group_exprs()))) {
      LOG_WARN("failed to append group exprs", K(ret));
    } else if (OB_FAIL(append(group_and_rollup_exprs, select_stmt->get_rollup_exprs()))) {
      LOG_WARN("failed to append rollup exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < group_and_rollup_exprs.count(); ++i) {
      bool is_hit = false;
      ObRawExpr *expr = NULL;
      ObColumnRefRawExpr *col_ref = NULL;
      if (OB_ISNULL(expr = group_and_rollup_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (!expr->is_column_ref_expr()) {
        // do nothing
      } else if (OB_FALSE_IT(col_ref = static_cast<ObColumnRefRawExpr*>(expr))) {
      } else if (OB_FAIL(ObResolverUtils::check_column_name(session_info_, q_name, *col_ref, is_hit))) {
        LOG_WARN("check column name failed", K(ret), K(q_name));
      } else if (is_hit) {
        if (OB_ISNULL(real_ref_expr)) {
          real_ref_expr = col_ref;
        } else if (real_ref_expr != col_ref) {
          ret = OB_NON_UNIQ_ERROR;
        }
      }
    }
  } else if (OB_FAIL(resolve_table_column_ref(q_name, real_ref_expr))) {
    LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve table column ref failed", K(ret));
  }
  if (OB_SUCC(ret) && NULL == real_ref_expr) {
    ret = OB_ERR_BAD_FIELD_ERROR;
  }
  return ret;
}

int ObSelectResolver::resolve_column_ref_alias_first(
  const ObQualifiedName &q_name,
  ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  //search column ref in alias list first
  //if alias column exist, use alias column, otherwise, search column ref in table columns
  if (OB_FAIL(resolve_alias_column_ref(q_name, real_ref_expr))) {
    if (OB_ERR_BAD_FIELD_ERROR == ret) {
      if (OB_FAIL(resolve_table_column_ref(q_name, real_ref_expr))) {
        LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve table column refs failed", K(ret), K(q_name));
      }
    } else {
      LOG_WARN("resolve alias column ref failed", K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::resolve_column_ref_for_search(
  const ObQualifiedName &q_name,
  ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  UNUSED(q_name);
  ColumnItem *col_item = NULL;
  if (T_WITH_CLAUSE_SEARCH_SCOPE != current_scope_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("we only resolve the search clause at the search scope");
  } else if (OB_ISNULL(current_recursive_cte_table_item_) || OB_ISNULL(current_cte_involed_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in order to resolve the search clause, the recursive cte table item can not be null");
  } else {
    ObDMLStmt *stmt = current_cte_involed_stmt_;
    TableItem& table_item = *current_recursive_cte_table_item_;
    if (OB_ISNULL(stmt) || OB_ISNULL(schema_checker_) || OB_ISNULL(params_.expr_factory_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema checker is null", K(stmt), K_(schema_checker), K_(params_.expr_factory));
    } else if (OB_UNLIKELY(!table_item.is_basic_table() && !table_item.is_fake_cte_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not base table or alias from base table", K_(table_item.type));
    } else if (NULL != (col_item = stmt->get_column_item(table_item.table_id_, q_name.col_name_))) {
      real_ref_expr = col_item->expr_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the col item has been resolve, but we do not find it in the search clause resolver");
    }
  }
  return ret;
}

int ObSelectResolver::set_having_self_column(const ObRawExpr *real_ref_expr)
{
  int ret = OB_SUCCESS;
  // set having has columns
  if (OB_ISNULL(real_ref_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null or real ref expr is null", K(ret));
  } else if (!real_ref_expr->is_exec_param_expr()) {
    // set having has self column
    set_having_has_self_column();
    LOG_DEBUG("set having self column", K(*real_ref_expr));
  } else {
    LOG_DEBUG("different level", K(*real_ref_expr));
  }
  return ret;
}

/**
 * The SQL standard requires that HAVING must reference only columns in the GROUP BY clause or
 * columns used in aggregate functions. However, MySQL supports an extension to this behavior,
 * and permits HAVING to refer to columns in the SELECT list and columns in outer subqueries as well
 */
int ObSelectResolver::resolve_column_ref_for_having(
  const ObQualifiedName &q_name,
  ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *check_ref = NULL;
  bool is_oracle_compatible = is_oracle_mode();
  if (OB_FAIL(resolve_column_ref_in_group_by(q_name, real_ref_expr))) {
    if (OB_ERR_BAD_FIELD_ERROR == ret) {
      if (OB_FAIL(resolve_alias_column_ref(q_name, real_ref_expr))) {
        LOG_WARN_IGNORE_COL_NOTFOUND(
          ret,
          "resolve alias column reference failed",
          K(ret),
          K(q_name));
      }
    } else {
      LOG_WARN("resolve column ref in group by failed", K(ret), K(q_name));
    }
  } else if (is_oracle_compatible) {
    if (OB_FAIL(set_having_self_column(real_ref_expr))) {
      LOG_WARN("set having self column failed", K(ret), K(q_name));
    }
  } else if (OB_FAIL(resolve_alias_column_ref(q_name, check_ref))) {
    //check column whether exists in alias select list
    if (OB_ERR_BAD_FIELD_ERROR == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("resolve alias column ref failed", K(ret), K(q_name));
    }
  } else if (!ObRawExprUtils::is_same_column_ref(real_ref_expr, check_ref)) {
    // if column name exist in both group columns and alias name list,
    // use table column and produce warning msg
    ObString col_name = concat_qualified_name(
      q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
    ObString scope_name = ObString::make_string(get_scope_name(current_scope_));
    LOG_USER_WARN(OB_NON_UNIQ_ERROR,
                  col_name.length(),
                  col_name.ptr(),
                  scope_name.length(),
                  scope_name.ptr());
  }

  return ret;
}

int ObSelectResolver::resolve_column_ref_table_first(
  const ObQualifiedName &q_name,
  ObRawExpr *&real_ref_expr,
  bool need_further_match_alias /* = true */)
{
  int ret = OB_SUCCESS;
  //search column ref in table columns first, follow by alias name
  //if table column exist, check column name whether exist in alias name list
  ObRawExpr *tmp_ref = NULL;
  if (OB_FAIL(resolve_table_column_ref(q_name, real_ref_expr))) {
    if (OB_ERR_BAD_FIELD_ERROR == ret) {
      if (OB_FAIL(resolve_alias_column_ref(q_name, real_ref_expr))) {
        LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve alias column ref failed", K(ret), K(q_name));
      }
    } else if (OB_NON_UNIQ_ERROR == ret && lib::is_mysql_mode() &&
               T_GROUP_SCOPE == current_scope_) {
      // in mysql mode, for t1(c1, c2), t2(c1, c2), select t1.c1 from t1, t2 group by c1;
      // the c1 in group by is resolved as t1.c1 in select items.
      if (OB_FAIL(resolve_alias_column_ref(q_name, real_ref_expr))) {
        ret = OB_NON_UNIQ_ERROR;
        LOG_WARN("resolve table column ref failed", K(ret));
      }
    } else {
      LOG_WARN("resolve table column ref failed", K(ret));
    }
  } else if (need_further_match_alias) {
    if (OB_FAIL(resolve_alias_column_ref(q_name, tmp_ref))) {
      if (OB_ERR_BAD_FIELD_ERROR == ret || OB_ILLEGAL_REFERENCE == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("try to hit column on target list failed", K(ret));
      }
    } else if (!ObRawExprUtils::is_same_column_ref(real_ref_expr, tmp_ref)) {
      //if column name exist in both table columns and alias name list, use table column and produce warning msg
      ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
      ObString scope_name = ObString::make_string(get_scope_name(current_scope_));
      LOG_USER_WARN(OB_NON_UNIQ_ERROR, col_name.length(), col_name.ptr(), scope_name.length(), scope_name.ptr());
    }
  }
  return ret;
}

int ObSelectResolver::create_joined_table_item(JoinedTable *&joined_table)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (NULL == (ptr = allocator_->alloc(sizeof(JoinedTable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "alloc memory for JoinedTable failed", "size", sizeof(JoinedTable));
  } else {
    joined_table = new (ptr) JoinedTable();
  }
  return ret;
}

int ObSelectResolver::check_special_join_table(const TableItem &join_table, bool is_left_child, ObItemType join_type)
{
  int ret = OB_SUCCESS;
  if (is_left_child) {
    if (join_table.is_fake_cte_table() && cte_ctx_.is_with_resolver()
        && (T_JOIN_RIGHT == join_type || T_JOIN_FULL == join_type)) {
      ret = OB_ERR_ILLEGAL_JOIN_IN_RECURSIVE_CTE;
      LOG_WARN("recursive cte table placed at right join's left is not allowed, and full join is not allowed", K(ret));
    }
  } else {
    if (join_table.is_fake_cte_table() && cte_ctx_.is_with_resolver()
        && (T_JOIN_LEFT == join_type || T_JOIN_FULL == join_type)) {
      ret = OB_ERR_ILLEGAL_JOIN_IN_RECURSIVE_CTE;
      LOG_WARN("recursive cte in left join' right is not allowed, and full join is not allowed", K(ret));
    }
  }
  return ret;
}
//find coalesce_expr in joined table
//@param jointable_idx: the index of joinedtable, use to match correct using_columns/coalesce_expr in
//               jointable_using_columns and jointable_coalesce_exprs_
int ObSelectResolver::recursive_find_coalesce_expr(const JoinedTable *&joined_table,
                                                    const ObString &cname,
                                                    ObRawExpr *&coalesce_expr)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ResolverJoinInfo *join_info = NULL;
  if (get_joininfo_by_id(joined_table->table_id_, join_info) && join_info->coalesce_expr_.count() > 0)
  {
    for (int i = 0; !found && i < join_info->coalesce_expr_.count(); i++) {
      if (ObCharset::case_insensitive_equal(join_info->using_columns_.at(i), cname)) {
        coalesce_expr = join_info->coalesce_expr_.at(i);
        found = true;
      }
    }
  }

  if (!found) {
    if (RIGHT_OUTER_JOIN == joined_table->joined_type_) {
      if (TableItem::JOINED_TABLE == joined_table->right_table_->type_) {
        const JoinedTable *right_table = static_cast<const JoinedTable*>(joined_table->right_table_);
        OZ(SMART_CALL(recursive_find_coalesce_expr(right_table, cname, coalesce_expr)));
      }
    } else if (TableItem::JOINED_TABLE == joined_table->left_table_->type_) {
      const JoinedTable *left_table = static_cast<const JoinedTable*>(joined_table->left_table_);
      OZ(SMART_CALL(recursive_find_coalesce_expr(left_table, cname, coalesce_expr)));
    }
  }
  return ret;
}

int ObSelectResolver::resolve_subquery_info(const ObIArray<ObSubQueryInfo> &subquery_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info is null");
  } else if (current_level_ + 1 >= OB_MAX_SUBQUERY_LAYER_NUM && subquery_info.count() > 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "too many levels of subqueries");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_info.count(); i++) {
    const ObSubQueryInfo &info = subquery_info.at(i);
    ObSelectResolver subquery_resolver(params_);
    subquery_resolver.set_current_level(current_level_ + 1);
    subquery_resolver.set_is_sub_stmt(true);
    subquery_resolver.set_parent_namespace_resolver(this);
    subquery_resolver.set_current_view_level(current_view_level_);
    set_query_ref_exec_params(info.ref_expr_ == NULL ? NULL : &info.ref_expr_->get_exec_params());
    resolve_alias_for_subquery_ = !(T_FIELD_LIST_SCOPE == current_scope_
                                   && info.parents_expr_info_.has_member(IS_AGG));
    if (OB_FAIL(subquery_resolver.add_parent_gen_col_exprs(gen_col_exprs_))) {
      LOG_WARN("failed to add parent gen col exprs", K(ret));
    }
    OZ( subquery_resolver.set_cte_ctx(cte_ctx_, true, true) );
    OZ( add_cte_table_to_children(subquery_resolver) );
    if (info.parents_expr_info_.has_member(IS_AGG)) {
      subquery_resolver.set_parent_aggr_level(current_level_);
    } else if (is_only_full_group_by_on(session_info_->get_sql_mode())) {
      subquery_resolver.set_parent_aggr_level(parent_aggr_level_);
    }
    OZ ( do_resolve_subquery_info(info, subquery_resolver) );
    set_query_ref_exec_params(NULL);
  }
  return ret;
}

//can't find column in current namespace, continue to search column in all parent namespace
//compatible with mysql, use of the outer subqueries column follow these rules
//if subquery in having clause, 1. use column in group by, 2. use column in select list
//such as: select c2 as c1 from t1 group by c1 having c2>(select t1.c1 from t) -> use
// t1.c1 in group by
//select c1 from t1 group by c2 having c2>(select t1.c3 from t) -> error, t1.c3 not
// in group by or select list
//if subquery in others clause, 1. use column in table columns, 2. use column in select list
//such as: t1(c1, c2), t2(a), select c1 as c2 from t1 group by (select c2 from t2) -> use t1.c2
//select c1 as c2 from t1 order by (select c2 from t2) -> use t1.c2
//but order by not in subquery will use alias name first, such as: select c1 as c2
// from t1 order by c2 -> use t1.c1
int ObSelectResolver::resolve_column_ref_for_subquery(
  const ObQualifiedName &q_name,
  ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_HAVING_SCOPE == current_scope_)) {
    if (OB_FAIL(resolve_column_ref_in_group_by(q_name, real_ref_expr))) {
      LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve column ref in group by failed", K(ret), K(q_name));
    } else {
      if (is_oracle_mode()) {
        if (OB_FAIL(set_having_self_column(real_ref_expr))) {
          LOG_WARN("set having self column failed", K(ret), K(q_name));
        }
      }
    }
  } else if (OB_FAIL(resolve_table_column_ref(q_name, real_ref_expr))) {
    LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve table column failed", K(ret), K(q_name));
  }
  if (OB_ERR_BAD_FIELD_ERROR == ret && resolve_alias_for_subquery_) {
    if (OB_FAIL(resolve_alias_column_ref(q_name, real_ref_expr))) {
      LOG_WARN_IGNORE_COL_NOTFOUND(ret, "resolve alias column ref failed", K(ret), K(q_name));
    }
  }
  return ret;
}

inline bool ObSelectResolver::column_need_check_group_by(const ObQualifiedName &q_name) const
{
  bool bret = true;
  if (OB_ISNULL(session_info_)) {
    bret = false;
  } else if (!is_only_full_group_by_on(session_info_->get_sql_mode())) {
    bret = false;
  } else if (T_FIELD_LIST_SCOPE != current_scope_ && T_ORDER_SCOPE != current_scope_) {
    bret = false;
  } else if (q_name.parent_aggr_level_ >= 0 && current_level_ <= q_name.parent_aggr_level_) {
    //aggr_level不为-1说明该column一定存在aggr function中 current_level_ > aggr_level说明
    //引用的current level低于aggr_level
    //即使aggr没有发生上推，current level的column也不会影响aggr的聚集情况，不受聚集的约束
    //这样能够确保这样的column一定不在aggr中，或者在aggr中，但是不受aggr约束，需要检查group by的合法性
    //因此这里会存在误判的可能，因为aggr function还没有被上推，被需要检查，但是没有检查的列，
    //在上推的分析过程中会再次将其加入到standard group checker中
    //例如:select c1, (select (select count(t2.c1+t1.c1) from t3) from t2) from t1;
    //在这个例子中,count()属于第二层,这里认为t2.c1和t1.c1都不需要check group by,
    //但是在aggre上推分析过程中发现，t2.c1位于count()中，不需要检查,t1.c1需要检查，这里误判了，
    //在聚集上推分析中会加入到standard group checker中
    bret = false;
  }
  return bret;
}

int ObSelectResolver::wrap_alias_column_ref(
  const ObQualifiedName &q_name,
  ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  //aggr in window function isn't used to wrap column ref, just only expand alias column, and do
  //wrap alias column ref is used to help analyze aggregate pullup for alias column in aggr. the
  //other situation should expand alias column directly.
  //eg: select sum(t1.c1) from t1 order by (select sum(t1.c1) from t2);
  // sum(t1.c1) in subquery is from parent stmt.
  if (!q_name.parents_expr_info_.has_member(IS_WINDOW_FUNC) &&
      q_name.parent_aggr_level_ >= 0 &&
      current_level_ <= q_name.parent_aggr_level_) {
    ObAliasRefRawExpr *alias_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_alias_column_expr(
                  *params_.expr_factory_,
                  real_ref_expr,
                  current_level_,
                  alias_expr))) {
      LOG_WARN("build alias column expr failed", K(ret));
    } else {
      real_ref_expr = alias_expr;
    }
  }
  return ret;
}

int ObSelectResolver::mark_nested_aggr_if_required(
    const ObIArray<ObAggFunRawExpr*> &aggr_exprs)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *stmt = static_cast<ObSelectStmt*>(get_stmt());
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (!has_nested_aggr_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs.count(); i++) {
      if (OB_ISNULL(aggr_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggr item is null", K(ret));
      } else if (OB_FAIL(aggr_exprs.at(i)->extract_info())) {
        LOG_WARN("failed to extract info", K(ret));
      } else if (aggr_exprs.at(i)->contain_nested_aggr()) {
        has_nested_aggr_ = true;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && has_nested_aggr_) {
    ObArray<ObAggFunRawExpr*> param_aggrs;
    ObArray<ObWinFunRawExpr *> param_winfuncs;
    if (OB_UNLIKELY(is_mysql_mode())) {
      ret = OB_ERR_INVALID_GROUP_FUNC_USE;
      LOG_WARN("invalid scope for agg function", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs.count(); ++i) {
      ObAggFunRawExpr *aggr = NULL;
      if (OB_ISNULL(aggr = aggr_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggr item is null", K(ret));
      } else if (OB_FAIL(aggr_exprs.at(i)->extract_info())) {
        LOG_WARN("failed to extract info", K(ret));
      } else if (aggr->has_flag(CNT_AGG) ||
                 aggr->has_flag(CNT_WINDOW_FUNC)) {
        for (int64_t j = 0; OB_SUCC(ret) && j < aggr->get_param_count(); ++j) {
          if (OB_FAIL(ObTransformUtils::extract_aggr_expr(aggr->get_param_expr(j),
                                                          param_aggrs))) {
            LOG_WARN("failed to extract aggr expr", K(ret));
          } else if (OB_FAIL(ObTransformUtils::extract_winfun_expr(aggr->get_param_expr(j),
                                                                   param_winfuncs))) {
            LOG_WARN("failed to extract param win funcs", K(ret));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param_winfuncs.count(); ++i) {
      if (OB_ISNULL(param_winfuncs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param winfunc is null", K(ret));
      } else if (param_winfuncs.at(i)->has_flag(CNT_AGG)) {
        ret = OB_ERR_INVALID_GROUP_FUNC_USE;
        LOG_WARN("aggregated nested in same level", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param_aggrs.count(); ++i) {
      if (OB_ISNULL(param_aggrs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("normal aggr is null", K(ret));
      } else if (param_aggrs.at(i)->contain_nested_aggr()) {
        ret = OB_ERR_INVALID_GROUP_FUNC_USE;
        LOG_WARN("nested aggr should not contain nested aggr", K(ret));
      } else {
        param_aggrs.at(i)->set_nested_aggr_inner_stmt(true);
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_aggr_exprs(ObRawExpr *&expr, ObIArray<ObAggFunRawExpr*> &aggr_exprs,
    const bool need_analyze/* = true*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr factory is null", K(ret));
  } else if (OB_FAIL(mark_nested_aggr_if_required(aggr_exprs))) {
    LOG_WARN("failed check nested aggr.", K(ret));
  } else if (need_analyze) {
    for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs.count(); ++i) {
      ObRawExpr *final_aggr = NULL;
      ObAggrExprPushUpAnalyzer aggr_pushup_analyzer(*this);
      if (OB_ISNULL(aggr_exprs.at(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid aggr_expr", K(ret));
      } else if (OB_FAIL(aggr_pushup_analyzer.analyze_and_push_up_aggr_expr(*params_.expr_factory_,
                                                                            aggr_exprs.at(i),
                                                                            final_aggr))) {
        LOG_WARN("resolve aggr expr failed", K(ret));
      } else if (final_aggr != aggr_exprs.at(i)) {
        if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, aggr_exprs.at(i), final_aggr))) {
          LOG_WARN("replace reference column failed", K(ret));
        } else { /*do nothing.*/ }
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_win_func_exprs(ObRawExpr *&expr, common::ObIArray<ObWinFunRawExpr*> &win_exprs)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(expr) || OB_ISNULL(select_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(expr), K(select_stmt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < win_exprs.count(); ++i) {
      ObWinFunRawExpr *win_expr = static_cast<ObWinFunRawExpr *>(win_exprs.at(i));
      ObAggFunRawExpr *agg_expr = win_expr->get_agg_expr();
      if (OB_ISNULL(win_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arg", K(ret), K(win_expr));
      } else if (OB_ISNULL(agg_expr)) {
      } else if (OB_FAIL(agg_expr->formalize(session_info_))) {
        LOG_WARN("formalize agg expr failed", K(ret));
      } else {/*do nothing.*/}
      ObWinFunRawExpr *final_win_expr = NULL;
      const int64_t N = select_stmt->get_window_func_exprs().count();
      if (OB_SUCC(ret)) {
        if (OB_FAIL(check_ntile_compatiable_with_mysql(win_expr))) {
          LOG_WARN("failed to handle compat with mysql ntile.", K(ret));
        } else if (OB_ISNULL(final_win_expr = select_stmt->get_same_win_func_item(win_expr))) {
          ret = select_stmt->add_window_func_expr(win_expr);
        } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, win_exprs.at(i), final_win_expr))) {
          LOG_WARN("failed to replace ref column.", K(ret), K(*win_exprs.at(i)));
        } else {/*do nothing.*/}
      }
    }
  }
  return ret;
}

int ObSelectResolver::add_aggr_expr(ObAggFunRawExpr *&final_aggr_expr)
{
  int ret = OB_SUCCESS;
  ObAggFunRawExpr *same_aggr_expr = NULL;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt) || OB_ISNULL(final_aggr_expr) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("select_stmt is null", K(select_stmt), K(final_aggr_expr), K_(session_info));
  } else if (OB_UNLIKELY(select_stmt->is_set_stmt())) {
    ret = OB_ERR_AGGREGATE_ORDER_FOR_UNION;
    LOG_WARN("can't use aggregate function in union stmt");
  } else if (OB_FAIL(select_stmt->check_and_get_same_aggr_item(final_aggr_expr, // 这里实际上判断是错误的
                                                               same_aggr_expr))) {
    LOG_WARN("failed to check and get same aggr item.", K(ret));
  } else if (same_aggr_expr != NULL) {
    final_aggr_expr = same_aggr_expr;
  } else if (lib::is_oracle_mode() &&
             final_aggr_expr->get_expr_type() == T_FUN_GROUP_CONCAT &&
             OB_FAIL(check_listagg_aggr_param_valid(final_aggr_expr))) {
    LOG_WARN("failed to check list agg param valid", K(ret));
  } else if (OB_FAIL(select_stmt->add_agg_item(*final_aggr_expr))) {
    LOG_WARN("add new aggregate function failed", K(ret));
  }
  if (OB_SUCC(ret) && is_only_full_group_by_on(session_info_->get_sql_mode())) {
    standard_group_checker_.set_has_group(true);
  }
  return ret;
}

int ObSelectResolver::add_unsettled_column(ObRawExpr *column_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {

    ret = OB_NOT_INIT;
    LOG_WARN("session_info is null");
  } else if (is_only_full_group_by_on(session_info_->get_sql_mode())) {
    if (OB_FAIL(standard_group_checker_.add_unsettled_column(column_expr))) {
      LOG_WARN("add unsettled column to standard group checker failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && T_HAVING_SCOPE == current_scope_) {
    if (OB_FAIL(check_column_ref_in_group_by_or_field_list(column_expr))) {
      LOG_WARN_IGNORE_COL_NOTFOUND(ret, "check column ref in group by failed", K(ret));
    }
  }
  return ret;
}

/**
 * create table t1(a int, b int);
 * create table t2(c int);
 * SELECT a FROM t1 GROUP BY a HAVING a IN (SELECT c FROM t2 WHERE MAX(b)>20)
 * 在上面的查询中，b位于max()中，但是此时max()还在子查询中，我们不能确定b是否是上层中合法的列
 * 对于having子句，mysql认为having子句中的列必须出现在group exprs中或者出现在aggregate function
 * 以及field list中，列出现在aggregate function中的列可以来源于当前表中的任何列，不需要出现在group by中
 * 而在解析b的时候还无法判断b是否真正位于聚集函数中，因为聚集函数可能上推，也可能无法上推
 * 在resolve column的时候我们将在聚集函数中的列都视为存在aggregate function中，不需要被group by约束
 * 在聚集上推的过程中，确定了聚集函数最终的层次后，我们再将没有出现在aggregate function中的column推回到
 * 应有的层次上进行group by exprs的检查
 */
int ObSelectResolver::check_column_ref_in_group_by_or_field_list(const ObRawExpr *column_ref) const
{
  int ret = OB_SUCCESS;
  bool found_expr = false;
  const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(stmt_);
  if (OB_ISNULL(select_stmt) || OB_ISNULL(column_ref)) {
    ret = OB_NOT_INIT;
    LOG_WARN("select_stmt is null", K(select_stmt), K(column_ref));
  } else if (column_ref->is_column_ref_expr()) {
    const ObColumnRefRawExpr *column_expr = static_cast<const ObColumnRefRawExpr*>(column_ref);
    for (int64_t i = 0;
         OB_SUCC(ret) && !found_expr && i < select_stmt->get_group_expr_size();
         ++i) {
      if (select_stmt->get_group_exprs().at(i) == column_ref) {
        found_expr = true;
      }
    }
    for (int64_t i = 0;
         OB_SUCC(ret) && !found_expr && i < select_stmt->get_select_item_size();
         ++i) {
      if (select_stmt->get_select_item(i).expr_ == column_ref) {
        found_expr = true;
      }
    }
    if (OB_SUCC(ret) && !found_expr) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      ObString column_name = concat_qualified_name(
        column_expr->get_database_name(),
        column_expr->get_table_name(),
        column_expr->get_column_name());
      ObString scope_name = ObString::make_string(get_scope_name(current_scope_));
      LOG_USER_ERROR(
        OB_ERR_BAD_FIELD_ERROR,
        column_name.length(),
        column_name.ptr(),
        scope_name.length(),
        scope_name.ptr());
    }
  }
  return ret;
}

// use_sys_tenant 标记是否需要以系统租户的身份获取schema
int ObSelectResolver::check_need_use_sys_tenant(bool &use_sys_tenant) const
{
  int ret = OB_SUCCESS;
  if (params_.is_from_show_resolver_) {
    // 若当前已经是系统租户, 则忽略
    if (OB_ISNULL(session_info_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("session info is null");
    } else if (OB_SYS_TENANT_ID == session_info_->get_effective_tenant_id()) {
      use_sys_tenant = false;
    } else {
      use_sys_tenant = true;
    }
  } else {
    use_sys_tenant = false;
  }
  return ret;
}

int ObSelectResolver::check_in_sysview(bool &in_sysview) const
{
  int ret = OB_SUCCESS;
  in_sysview = params_.is_from_show_resolver_ || params_.is_in_sys_view_;
  return ret;
}

int ObSelectResolver::resolve_start_with_clause(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (node != NULL) {//存在start with clause
    current_scope_ = T_START_WITH_SCOPE;
    ObSelectStmt *select_stmt = get_select_stmt();
    if (OB_ISNULL(select_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select stmt is NULL", K(ret));
    } else if (OB_FAIL(resolve_and_split_sql_expr(*node, select_stmt->get_start_with_exprs()))) {
      LOG_WARN("resolve and split sql expr failed", K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::check_connect_by_expr_validity(const ObRawExpr *raw_expr, bool is_prior)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_UNLIKELY(raw_expr->is_pseudo_column_expr() &&
                          (is_prior || raw_expr->get_expr_type() != T_LEVEL))) {
    ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
    LOG_WARN("Only Level pseudo without prior clause allow here", K(ret));
  } else if (OB_UNLIKELY(raw_expr->is_aggr_expr() ||
                          raw_expr->get_expr_class() == ObRawExpr::EXPR_PL_QUERY_REF)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "expr in connect by");
    LOG_WARN("expr is not supported here for connect by condition", K(ret));
  } else {
    for (int64_t i = 0; i < raw_expr->get_param_count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(check_connect_by_expr_validity(raw_expr->get_param_expr(i), is_prior))) {
        LOG_WARN("check_connect_by_expr_validity failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectResolver::resolve_connect_by_clause(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (node != NULL) {//存在connect by clause
    current_scope_ = T_CONNECT_BY_SCOPE;
    ObSelectStmt *select_stmt = get_select_stmt();
    const ParseNode *nocycle_node = NULL;
    const ParseNode *connect_by_exprs_node = NULL;
    if (OB_ISNULL(select_stmt)
        || OB_UNLIKELY(node->num_child_ != 2)
        || OB_UNLIKELY(node->type_ != T_CONNECT_BY_CLAUSE)
        || OB_ISNULL(connect_by_exprs_node = node->children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(select_stmt), K(node->num_child_),
               K(node->type_), K(connect_by_exprs_node), K(ret));
    } else {
      expr_resv_ctx_.set_new_scope();
      nocycle_node = node->children_[0];
      select_stmt->set_nocycle(nocycle_node != NULL);
      OZ( resolve_and_split_sql_expr(
            *connect_by_exprs_node,
            select_stmt->get_connect_by_exprs()) );
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_connect_by_exprs().count(); ++i) {
        ObRawExpr *raw_expr = NULL;
        if (OB_ISNULL(raw_expr = select_stmt->get_connect_by_exprs().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("connect by expr is null", K(ret));
        } else if (raw_expr->has_flag(CNT_CONNECT_BY_ROOT)) {
          ret = OB_ERR_CBY_CONNECT_BY_ROOT_ILLEGAL_USED;
          LOG_WARN("CONNECT BY ROOT operator is not supported in the START WITH or in the CONNECT BY condition", K(ret));
        } else if (raw_expr->has_flag(CNT_SYS_CONNECT_BY_PATH)) {
          ret = OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED;
          LOG_WARN("CONNECT BY PATH operator is not supported in the START WITH or in the CONNECT BY condition", K(ret));
        } else if (OB_FAIL(check_connect_by_expr_validity(raw_expr, raw_expr->has_flag(CNT_PRIOR)))) {
          LOG_WARN("check_connect_by_expr_validity failed", K(ret));
        } else if (raw_expr->has_flag(CNT_PRIOR)) {
          select_stmt->set_has_prior(true);
        }
      }
      expr_resv_ctx_.set_new_scope();
    }
    // window function in select list not supported.
    if (OB_SUCC(ret)) {
      int64_t count = select_stmt->get_select_item_size();
      for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
        SelectItem &select_item = select_stmt->get_select_item(i);
        if (OB_ISNULL(select_item.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("select item is null", K(ret));
        } else if (select_item.expr_->has_flag(CNT_WINDOW_FUNC)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "window function in connect by");
          LOG_WARN("window function in connect by not supported", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSelectResolver::check_pseudo_columns()
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(select_stmt), K_(session_info));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_pseudo_column_like_exprs().count(); ++i) {
    ObRawExpr *pseudo_column = select_stmt->get_pseudo_column_like_exprs().at(i);
    if (OB_ISNULL(pseudo_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid pseudo column", K(ret));
    } else if (T_CONNECT_BY_ISCYCLE == pseudo_column->get_expr_type()
               && !select_stmt->is_nocycle()) {
      ret = OB_ERR_CBY_NOCYCLE_REQUIRED;
      LOG_WARN("NOCYCLE keyword is required with CONNECT_BY_ISCYCLE pseudo column", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (select_stmt->is_order_siblings()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_order_item_size(); ++i) {
      const ObRawExpr *sort_expr = select_stmt->get_order_item(i).expr_;
      if (OB_ISNULL(sort_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sort expr is NULL", K(ret));
      } else if (OB_UNLIKELY(sort_expr->has_flag(CNT_SYS_CONNECT_BY_PATH))) {
        ret = OB_ERR_CBY_CONNECT_BY_PATH_NOT_ALLOWED;
        LOG_WARN("Connect by path not allowed here", K(ret));
      } else if (OB_UNLIKELY(sort_expr->has_flag(CNT_CONNECT_BY_ROOT))) {
        ret = OB_ERR_CBY_CONNECT_BY_ROOT_ILLEGAL_USED;
        LOG_WARN("Connect by root not allowed here", K(ret));
      } else if (OB_UNLIKELY(sort_expr->has_flag(CNT_PSEUDO_COLUMN))
                 || OB_UNLIKELY(sort_expr->has_flag(CNT_PRIOR))) {
        ret = OB_ERR_CBY_PSEUDO_COLUMN_NOT_ALLOWED;
        LOG_WARN("invalid sort expr for order siblings", KPC(sort_expr), K(ret));
      } else if (OB_UNLIKELY(sort_expr->has_flag(CNT_SUB_QUERY))
                 || OB_UNLIKELY(sort_expr->has_flag(CNT_AGG))) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
                       "Subquery or aggregate function in order siblings by clause is");
      }
    }
  }
  return ret;
}

//同oracle一样, ntile(arg1) (partition by arg2...) 要求arg1 = arg2或者是基于arg2的运算, 如同group by 和 select的有效性检查
int ObSelectResolver::check_win_func_arg_valid(ObSelectStmt *select_stmt,
                                               const ObItemType func_type,
                                               common::ObIArray<ObRawExpr *> &arg_exp_arr,
                                               common::ObIArray<ObRawExpr *> &partition_exp_arr)
{
  int ret = OB_SUCCESS;
  if (T_WIN_FUN_NTILE == func_type || T_FUN_GROUP_CONCAT == func_type) {
    if (OB_ISNULL(select_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt", K(ret));
    } else {
      // skip add const constraint during prepare stage in PL
      const ParamStore *param_store = (NULL != params_.secondary_namespace_) ? NULL : params_.param_list_;
      if (OB_FAIL(ObGroupByChecker::check_analytic_function(param_store,
                                                            select_stmt,
                                                            arg_exp_arr,
                                                            partition_exp_arr))) {
        LOG_WARN("check_analytic_function failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectResolver::check_window_exprs()
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(select_stmt));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_window_func_exprs().count(); ++i) {
    ObWinFunRawExpr *win_expr = select_stmt->get_window_func_exprs().at(i);
    if (OB_ISNULL(win_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(win_expr), K(ret));
    } else if (win_expr->get_agg_expr() != NULL &&
               win_expr->get_agg_expr()->has_flag(CNT_WINDOW_FUNC)) {
      ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
      LOG_WARN("agg function's param cannot be window function", K(ret));
    } else {
      const ObIArray<ObRawExpr *> &partition_exprs = win_expr->get_partition_exprs();
      const ObIArray<OrderItem> &order_items = win_expr->get_order_items();
      ObSEArray<ObRawExpr *, 4> exprs;
      ObSEArray<ObRawExpr *, 4> arg_exprs;
      ObRawExpr *bound_expr_arr[2] = {NULL, NULL};
      bool need_check_order_datatype = (lib::is_oracle_mode()
                                       && WINDOW_RANGE == win_expr->get_window_type()
                                       && (BOUND_INTERVAL == win_expr->get_upper().type_
                                           || BOUND_INTERVAL == win_expr->get_lower().type_));
      for (int64_t j = 0; OB_SUCC(ret) && j < partition_exprs.count(); ++j) {
        ret = exprs.push_back(partition_exprs.at(j));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < order_items.count(); ++j) {
        ret = exprs.push_back(order_items.at(j).expr_);
      }
      if (OB_SUCC(ret) && win_expr->get_upper().interval_expr_ != NULL) {
        ret = exprs.push_back(win_expr->get_upper().interval_expr_);
        bound_expr_arr[0] = win_expr->get_upper().interval_expr_;
      }
      if (OB_SUCC(ret) && win_expr->get_lower().interval_expr_ != NULL) {
        ret = exprs.push_back(win_expr->get_lower().interval_expr_);
        bound_expr_arr[1] = win_expr->get_lower().interval_expr_;
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < exprs.count(); ++j) {
        ObRawExpr *expr = exprs.at(j);
        if (OB_UNLIKELY(expr->has_flag(CNT_WINDOW_FUNC))) {
          ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
          LOG_WARN("partition or sort or interval expr within window function nest window function not supported",
                   K(ret), K(*expr), K(j));
        }
      }
      if (OB_FAIL(ret)) {
        //do nothing...
      } else if (T_FUN_GROUP_CONCAT == win_expr->get_func_type() && NULL != win_expr->get_agg_expr() && is_oracle_mode()) {
        if (win_expr->get_agg_expr()->get_real_param_exprs().count() > 2) {
          ret = OB_ERR_PARAM_SIZE;
          LOG_WARN("incorrect argument number to call listagg", K(win_expr->get_agg_expr()->get_real_param_exprs().count()));
        } else if (win_expr->get_agg_expr()->get_real_param_exprs().count() == 2) {
          if (OB_FAIL(arg_exprs.push_back(win_expr->get_agg_expr()->get_real_param_exprs().at(1)))) {
            LOG_WARN("push seperator expr failed", K(ret));
          }
        }
      } else {
        if (OB_FAIL(arg_exprs.assign(win_expr->get_func_params()))) {
          LOG_WARN("assign func param failed", K(ret));
        }
      }
      //检查分析函数参数 和 partition by是否符合要求
      if (OB_SUCC(ret) && OB_FAIL(check_win_func_arg_valid(select_stmt,
                                                           win_expr->get_func_type(),
                                                           arg_exprs,
                                                           const_cast<ObIArray<ObRawExpr *>&>(partition_exprs)))) {
        LOG_WARN("argument should be a function of expressions in PARTITION BY", K(ret));
      }
      //检查frame是range时数据类型的有效性, 规则同oracle
      if (OB_SUCC(ret) && need_check_order_datatype) {
        if (1 != order_items.count()) {
          ret = OB_ERR_INVALID_WINDOW_FUNC_USE;

          LOG_WARN("invalid window specification", K(ret), K(order_items.count()));
        } else if (OB_ISNULL(order_items.at(0).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("order by expr should not be null!", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++ i) {
            const ObObjType &order_res_type = order_items.at(0).expr_->get_data_type();
            if (bound_expr_arr[i] != NULL) {
              if (ob_is_numeric_type(bound_expr_arr[i]->get_data_type())
                  || ob_is_string_tc(bound_expr_arr[i]->get_data_type())) {
                if (!ob_is_numeric_type(order_res_type) && !ob_is_datetime_tc(order_res_type)) {
                  ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
                  LOG_WARN("invalid datatype in order by for range clause", K(ret), K(order_res_type));
                }
              } else {
                //to do: 支持interval后这里要处理interval的情况
                ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
                LOG_WARN("invalid datatype in order by", K(i),
                         K(bound_expr_arr[i]->get_data_type()), K(ret), K(order_res_type));
              }
            }
          }
        }
      }
      // oracle 模式下不允许对 lob 类型进行 order by
      if (OB_SUCC(ret) && lib::is_oracle_mode()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
          if (ob_is_text_tc(order_items.at(i).expr_->get_data_type())
              || ob_is_lob_tc(order_items.at(i).expr_->get_data_type())
              || ob_is_json_tc(order_items.at(i).expr_->get_data_type())
              || ob_is_geometry_tc(order_items.at(i).expr_->get_data_type())
              || ob_is_extend(order_items.at(i).expr_->get_data_type())) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_WARN("lob or udt expr can't order", K(ret), K(*order_items.at(i).expr_));
          }
        }
      }
      if (OB_SUCC(ret) && lib::is_oracle_mode()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_exprs.count(); i++) {
          ObRawExpr *param_expr = partition_exprs.at(i);
          if (OB_ISNULL(param_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param is null", K(ret));
          } else if (ob_is_lob_locator(param_expr->get_data_type())
                    || ob_is_text_tc(param_expr->get_data_type())
                    || ob_is_extend(param_expr->get_data_type())) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_WARN("invalid partition by expr", K(ret), K(i), KPC(param_expr));
          }
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_having_exprs().count() +
         select_stmt->get_group_exprs().count(); ++i) {
    ObRawExpr *expr = i < select_stmt->get_having_exprs().count() ?
       select_stmt->get_having_exprs().at(i) :
       select_stmt->get_group_exprs().at(i - select_stmt->get_having_exprs().count());
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(i), K(expr));
    } else if (OB_UNLIKELY(expr->has_flag(CNT_WINDOW_FUNC))) {
      ret = OB_ERR_INVALID_WINDOW_FUNC_USE;
      LOG_WARN("window function exists in having or group scope not supported",
               K(ret), K(*expr), K(i));
    }
  }

  return ret;
}

int ObSelectResolver::check_pseudo_column_name_legal(const ObString& name)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cte_ctx_.cte_col_names_.count(); ++i) {
    if (ObCharset::case_insensitive_equal(cte_ctx_.cte_col_names_.at(i), name)) {
      ret = OB_ERR_CTE_ILLEGAL_SEARCH_PSEUDO_NAME;
      LOG_WARN("the pseudo column can not be same with the defined cte column name", K(ret));
    }
  }
  return ret;
}


/* sequence 有如下禁忌用法：
 *  1. 不能同时与 having、order by、group by 等一起使用
 *  2. 不能出现在 subquery 中的任何地方 (insert into select 除外)
 *  3. 不能出现在 where 表达式里（resolve expr 处检查过了）
 **/
int ObSelectResolver::check_sequence_exprs()
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(select_stmt));
  } else if (select_stmt->has_sequence()) {
    if (select_stmt->has_order_by() ||
        select_stmt->has_distinct() ||
        select_stmt->has_having() ||
        select_stmt->has_group_by() ||
        params_.is_from_create_view_) {
      ret = OB_ERR_SEQ_NOT_ALLOWED_HERE;
      LOG_WARN("sequence can not be used with create-view/select-subquery/orderby/groupby/distinct", K(ret));

    }
  }
  return ret;
}

/*
 *  The recursive component of the UNION ALL in a recursive WITH clause
 *  element used an operation that was currently not supported.  The
 *  following should not be used in the recursive branch of the
 *  UNION ALL operation: GROUP BY, DISTINCT, MODEL, grouping sets,
 *  CONNECT BY, window functions, HAVING, aggregate functions.
 **/
int ObSelectResolver::check_unsupported_operation_in_recursive_branch()
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (!cte_ctx_.is_recursive()) {
    // Do nothing
  } else if (cte_ctx_.cte_resolve_level_ >= 2) {
    // Recursive table cann't be quoted in a subquery,
    // Q1:
    // with cte(c1) as (select 1 from dual union all select c1 + 1 from (select * from cte where c1 < 3) where c1 < 5) select * from cte;
    // You will got a error, 32042. 00000 -  "recursive WITH clause must reference itself directly in one of the UNION ALL branches"
    // Q2:
    // with cte(c,d) AS (SELECT c1,c2 from t1 where c1 < 3 union all select c+1, d+1 from cte, t2 where t2.c1 = c and t2.c2 > some (select c1 from t44  t99 group by c1)) select * from cte;
    // No need to check subquery at the rigth union, because recursive table cann't be here.
    // So.do nothing
  } else if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(select_stmt));
  } else {
    if (select_stmt->has_order_by() ||
        select_stmt->has_distinct() ||
        select_stmt->has_having() ||
        select_stmt->has_group_by() ||
        !select_stmt->get_window_func_exprs().empty() ||
        !select_stmt->get_connect_by_exprs().empty() ||
        !select_stmt->get_aggr_items().empty()) {
      ret = OB_ERR_CTE_ILLEGAL_RECURSIVE_BRANCH;
      LOG_WARN("unsupported operation in recursive branch of recursive WITH clause", K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::resolve_all_fake_cte_table_columns(const TableItem &table_item, common::ObIArray<ColumnItem> *column_items)
{
  return ObDMLResolver::resolve_all_basic_table_columns(table_item, false, column_items);
}

int ObSelectResolver::check_recursive_cte_usage(const ObSelectStmt &select_stmt)
{
  int ret = OB_SUCCESS;
  int64_t fake_cte_table_count = 0;
  for (int64_t i = 0; i < select_stmt.get_table_items().count(); ++i) {
    const TableItem *table_item = select_stmt.get_table_items().at(i);
    if (table_item->is_fake_cte_table()) {
      fake_cte_table_count++;
    }
  }
  if (cte_ctx_.invalid_recursive_union() && fake_cte_table_count >= 1) {
    if (lib::is_mysql_mode()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "recursive UNION DISTINCT in Recursive Common Table Expression");
    } else {
      ret = OB_ERR_NEED_UNION_ALL_IN_RECURSIVE_CTE;
    }
    LOG_WARN("recursive WITH clause must use a UNION ALL operation", K(ret));
  } else if (fake_cte_table_count > 1) {
    ret = OB_ERR_CTE_RECURSIVE_QUERY_NAME_REFERENCED_MORE_THAN_ONCE;
    LOG_WARN("Recursive query name referenced more than once in recursive branch of recursive WITH clause element", K(ret), K(fake_cte_table_count));
  }
  return ret;
}

int ObSelectResolver::check_correlated_column_ref(const ObSelectStmt &select_stmt, ObRawExpr *expr, bool &correalted_query)
{
  int ret = OB_SUCCESS;
  if (expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(expr);
    uint64_t table_id = col_expr->get_table_id();
    const TableItem *table_item = nullptr;
    if (nullptr == (table_item = select_stmt.get_table_item_by_id(table_id))) {
      correalted_query = true;
      LOG_WARN("Column expr not in this stmt", K(ret));
    } else {
      LOG_DEBUG("Find table item", K(*select_stmt.get_table_item_by_id(table_id)));
    }
  }
  if (!correalted_query) {
    int64_t param_expr_count = expr->get_param_count();
    for (int64_t i = 0; i < param_expr_count && !correalted_query; ++i) {
      ObRawExpr *param_expr = expr->get_param_expr(i);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Param expr is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_correlated_column_ref(select_stmt, param_expr,
                                                                correalted_query)))) {
        LOG_WARN("Failed to check correlated column", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectResolver::check_ntile_compatiable_with_mysql(ObWinFunRawExpr *win_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(win_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("win expr is null.", K(ret));
  } else if (T_WIN_FUN_NTILE == win_expr->get_func_type() && lib::is_mysql_mode()) {
    if (1 != win_expr->get_func_params().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ntile param count should be 1", K(ret));
    } else {
      ObRawExpr *func_param = win_expr->get_func_params().at(0);
      bool is_valid = true;
      if (OB_ISNULL(func_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param is null", K(ret));
      } else if (OB_FAIL(check_ntile_validity(func_param, is_valid))) {
        LOG_WARN("failed to check ntile validity", K(ret));
      } else if (!is_valid) {
        ret = OB_ERR_NOT_CONST_EXPR;
        LOG_WARN("The argument of the window function should be a constant for a partition", K(ret), K(*func_param));
      }
    }
  }
  return ret;
}

int ObSelectResolver::check_ntile_validity(const ObRawExpr *expr,
                                           bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_column_ref_expr() ||
             expr->is_aggr_expr() ||
             expr->is_win_func_expr() ||
             expr->get_expr_type() == T_FUN_SYS_RANDOM ||
             expr->get_expr_type() == T_FUN_SYS_RAND) {
    is_valid = false;
  } else if (expr->is_exec_param_expr()) {
    if (OB_FAIL(SMART_CALL(check_ntile_validity(static_cast<const ObExecParamRawExpr *>(expr)->get_ref_expr(),
                                                is_valid)))) {
      LOG_WARN("failed to check ntile validity", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(SMART_CALL(check_ntile_validity(expr->get_param_expr(i),
                                                is_valid)))) {
      LOG_WARN("failed to check ntile validity", K(ret));
    }
  }
  if (OB_SUCC(ret) && expr->is_query_ref_expr()) {
    if (OB_FAIL(SMART_CALL(check_ntile_validity(static_cast<const ObQueryRefRawExpr *>(expr)->get_ref_stmt(),
                                                is_valid)))) {
      LOG_WARN("failed to check ntile validity", K(ret));
    }
  }
  return ret;
}

int ObSelectResolver::check_ntile_validity(const ObSelectStmt *stmt,
                                           bool &is_valid) {
  int ret = OB_SUCCESS;
  ObArray<ObSelectStmt*> child_stmts;
  ObArray<ObRawExpr *> relation_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->get_column_size() > 0 ||
             stmt->get_window_func_count() > 0 ||
             stmt->get_aggr_item_size() > 0) {
    is_valid = false;
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < relation_exprs.count(); ++i) {
    if (OB_FAIL(check_ntile_validity(relation_exprs.at(i), is_valid))) {
      LOG_WARN("failed to check ntile validity", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < child_stmts.count(); ++i) {
    if (OB_FAIL(check_ntile_validity(child_stmts.at(i), is_valid))) {
      LOG_WARN("failed to check ntile validity", K(ret));
    }
  }
  return ret;
}

/**
 * oracle在select item中如果有子查询，当且仅当
 * 子查询为exists、not exists的参数时，
 * 允许子查询返回多列，
 */
int ObSelectResolver::check_subquery_return_one_column(const ObRawExpr &expr, bool is_exists_param)
{
  int ret = OB_SUCCESS;
  if (T_FUN_UDF == expr.get_expr_type()) {
    // select ff(cursor(select * from tbl)) from dual;
    // do nothing
  } else if (expr.has_flag(IS_SUB_QUERY)) {
    const ObQueryRefRawExpr &query_expr = static_cast<const ObQueryRefRawExpr&>(expr);
    if (1 != query_expr.get_output_column() && !is_exists_param && !query_expr.is_cursor()) {
      ret = OB_ERR_TOO_MANY_VALUES;
      LOG_WARN("subquery return too many columns", K(query_expr.get_output_column()));
    }
  } else {
    bool is_exists_param = T_OP_EXISTS == expr.get_expr_type() || T_OP_NOT_EXISTS == expr.get_expr_type();
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
      const ObRawExpr *cur_expr = expr.get_param_expr(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null expr", K(ret));
      } else if (!cur_expr->has_flag(CNT_SUB_QUERY)) {
        // do nothing
      } else if (OB_FAIL(check_subquery_return_one_column(*cur_expr, is_exists_param))) {
        LOG_WARN("failed to check subquery return one column", K(ret));
      }
    }
  }
  return ret;
}


/*fetch clause:
*[OFFSET offset {ROW | ROWS}] FETCH {NEXT | FIRST} {rowcount| percent PERCENT} {ROW | ROWS} {ONLY | WITH TIES}
 */
int ObSelectResolver::resolve_fetch_clause(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(params_.expr_factory_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(params_.expr_factory_));
  } else if (lib::is_oracle_mode() && NULL != node) {
    current_scope_ = T_LIMIT_SCOPE;
    ObSelectStmt *select_stmt = get_select_stmt();
    select_stmt->set_has_fetch(true);
    ParseNode *limit_node = NULL;
    ParseNode *offset_node = NULL;
    ParseNode *percent_node = NULL;
    if (node->type_ == T_FETCH_CLAUSE) {
      offset_node = node->children_[0];
      limit_node = node->children_[1];
      percent_node = node->children_[2];
    } else if (node->type_ == T_FETCH_TIES_CLAUSE) {
      offset_node = node->children_[0];
      limit_node = node->children_[1];
      percent_node = node->children_[2];
      if (select_stmt->has_order_by()) {
        select_stmt->set_fetch_with_ties(true);
      }
    }
    ObRawExpr* limit_offset = NULL;
    ObRawExpr* case_limit_offset = NULL;
    ObRawExpr* limit_count = NULL;
    ObRawExpr* case_limit_count = NULL;
    ObRawExpr* limit_percent = NULL;
    //1.处理offset node,由于oracle支持为常量的double类型，但是对于行数来说，本质上为整型，同时兼容oracle行为，
    //  向下取整，由于老引擎cast函数转换时无法保证向下取整，所以需要显示分配一个floor函数
    if (offset_node != NULL) {
      ObSysFunRawExpr *floor_offset_expr = NULL;
      if (OB_FAIL(resolve_sql_expr(*offset_node, limit_offset))) {
        LOG_WARN("failed to resolve sql expr", K(ret));
      } else if (OB_ISNULL(limit_offset)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(limit_offset));
      } else if (!limit_offset->is_const_expr()) {
        ret = OB_ERR_INVALID_SQL_ROW_LIMITING;
        LOG_WARN("Invalid SQL ROW LIMITING expression was specified", K(ret));
      } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_FUN_SYS_FLOOR,
                                                                floor_offset_expr))) {
        LOG_WARN("failed to create fun sys floor", K(ret));
      } else if (OB_ISNULL(floor_offset_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("floor expr is null", K(ret));
      } else if (OB_FAIL(floor_offset_expr->set_param_expr(limit_offset))) {
        LOG_WARN("failed to set param expr", K(ret));
      //保证offset为非负数,避免在计划生成时转换limit + offset出现负数,需要进行二次处理
      } else if (OB_ISNULL(limit_offset = floor_offset_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(limit_offset));
      } else {
        floor_offset_expr->set_func_name(ObString::make_string("FLOOR"));
        ObExprResType dst_type;
        dst_type.set_int();
        ObSysFunRawExpr *cast_expr = NULL;
        OZ(ObRawExprUtils::create_cast_expr(
                *params_.expr_factory_, limit_offset, dst_type, cast_expr, session_info_));
        CK(NULL != cast_expr);
        OX(limit_offset = cast_expr);
        OZ(limit_offset->add_flag(IS_INNER_ADDED_EXPR));
      }
    }
    //2.1 处理fetch node ==> limit count同上，可以在resolve阶段提前计算出结果
    if (OB_SUCC(ret)) {
      if (limit_node != NULL) {
        ObSysFunRawExpr *floor_count_expr = NULL;
        if (OB_FAIL(resolve_sql_expr(*limit_node, limit_count))) {
          LOG_WARN("failed to resolve sql expr", K(ret));
        } else if (OB_ISNULL(limit_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(limit_count));
        } else if (!limit_count->is_const_expr()) {
          ret = OB_ERR_INVALID_SQL_ROW_LIMITING;
          LOG_WARN("Invalid SQL ROW LIMITING expression was specified", K(ret));
        } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_FUN_SYS_FLOOR,
                                                                  floor_count_expr))) {
          LOG_WARN("failed to create fun sys floor", K(ret));
        } else if (OB_ISNULL(floor_count_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("floor expr is null", K(ret));
        } else if (OB_FAIL(floor_count_expr->set_param_expr(limit_count))) {
          LOG_WARN("failed to set param expr", K(ret));
        //保证limit为非负数,避免在计划生成时转换limit + offset出现负数,在preprocess进行二次处理
        } else if (OB_ISNULL(limit_count = floor_count_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(limit_count));
        } else {
          floor_count_expr->set_func_name(ObString::make_string("FLOOR"));
          ObExprResType dst_type;
          dst_type.set_int();
          ObSysFunRawExpr *cast_expr = NULL;
          OZ(ObRawExprUtils::create_cast_expr(
                  *params_.expr_factory_, limit_count, dst_type, cast_expr, session_info_));
          CK(NULL != cast_expr);
          OX(limit_count = cast_expr);
          OZ(limit_count->add_flag(IS_INNER_ADDED_EXPR));
        }
      }
    }
    //2.2 处理fetch node ==> limit percent，由于这里计算的为总行数的百分比,因此不能直接转为int,同时考虑计算的总
    //                                     行时存在小数，因此这里统一设置百分比为double类型
    if (OB_SUCC(ret)) {
      if (percent_node != NULL) {
        if (OB_FAIL(resolve_sql_expr(*percent_node, limit_percent))) {
          LOG_WARN("failed to resolve sql expr", K(ret));
        } else if (OB_ISNULL(limit_percent)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(limit_percent));
        } else if (!limit_percent->is_const_expr()) {
          ret = OB_ERR_INVALID_SQL_ROW_LIMITING;
          LOG_WARN("Invalid SQL ROW LIMITING expression was specified", K(ret));
        } else {
          ObExprResType dst_type;
          dst_type.set_double();
          ObSysFunRawExpr *cast_expr = NULL;
          OZ(ObRawExprUtils::create_cast_expr(
                  *params_.expr_factory_, limit_percent, dst_type, cast_expr, session_info_));
          CK(NULL != cast_expr);
          OX(limit_percent = cast_expr);
          OZ(limit_percent->add_flag(IS_INNER_ADDED_EXPR));
        }
      }
    }
    if (OB_SUCC(ret)) {
      select_stmt->set_fetch_info(limit_offset, limit_count, limit_percent);
    }
  }
  return ret;
}

int ObSelectResolver::resolve_check_option_clause(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (NULL != node) {
    const int64_t node_value = node->value_;
    ObSelectStmt *select_stmt = get_select_stmt();
    if (OB_UNLIKELY(node_value < 0 || node_value >= static_cast<int64_t>(VIEW_CHECK_OPTION_MAX))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected check option node value", K(ret), K(node_value));
    } else if (OB_ISNULL(select_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select stmt is null", K(ret));
    } else {
      select_stmt->set_check_option(static_cast<ViewCheckOption>(node_value));
    }
  }
  return ret;
}

/* ObSelectResolver::check_auto_gen_column_names()
 *
 * For a long expr with no alias
 * MySQL will rename the overlong auto generated alias to "Name_exp_x",
 * but Oracle will throw "identifier is too long" error.
 */
int ObSelectResolver::check_auto_gen_column_names() {
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = get_select_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(ret));
  } else if (OB_FAIL(recursive_check_auto_gen_column_names(select_stmt, true))) {
    LOG_WARN("fail to check auto gen column names", K(ret));
  }
  return ret;
}

int ObSelectResolver::recursive_check_auto_gen_column_names(ObSelectStmt *select_stmt,
                                                            bool in_outer_stmt) {
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt), K(allocator_));
  } else if (OB_FAIL(select_stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("fail to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
    ObSelectStmt *child_stmt = child_stmts.at(i);
    if (OB_ISNULL(child_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child select stmt is null", K(ret), K(i));
    } else if (OB_FAIL(SMART_CALL(recursive_check_auto_gen_column_names(child_stmt, false)))) {
      LOG_WARN("fail to check child stmt", K(ret), K(i));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
    SelectItem *select_item = &(select_stmt->get_select_item(i));
    if (OB_ISNULL(select_item) || OB_ISNULL(select_item->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select item expr is null", K(ret), K(select_item));
    } else if (OB_FAIL(recursive_update_column_name(select_stmt, select_item->expr_))) {
      LOG_WARN("fail to update column name", K(ret), KPC(select_item));
    } else if (select_item->alias_name_.length() > static_cast<size_t>(OB_MAX_COLUMN_NAME_LENGTH)) {
      if (lib::is_oracle_mode() && in_outer_stmt) {
        ret = OB_ERR_TOO_LONG_IDENT;
        LOG_WARN("auto generated alias is too long", K(ret), K(select_item->alias_name_.length()), K(select_item->alias_name_));
      } else {
        char temp_str_buf[OB_MAX_COLUMN_NAME_BUF_LENGTH] = { 0 };
        if (snprintf(temp_str_buf, sizeof(temp_str_buf), SYNTHETIC_FIELD_NAME "%ld", auto_name_id_++) < 0) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("failed to generate buffer for temp_str_buf", K(ret));
        } else {
          ObString tmp_col_name = ObString::make_string(temp_str_buf);
          ObString col_name;
          if (OB_FAIL(ob_write_string(*allocator_, tmp_col_name, col_name))) {
            LOG_WARN("Can not malloc space for constraint name", K(ret));
          } else {
            select_item->alias_name_.assign_ptr(col_name.ptr(), col_name.length());
          }
        }
      }
    }
  }
  return ret;
}

int ObSelectResolver::recursive_update_column_name(ObSelectStmt *select_stmt,
                                                   ObRawExpr *expr) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(expr) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt), K(expr), K(allocator_));
  } else if (expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *col_ref_expr = static_cast<ObColumnRefRawExpr*>(expr);
    TableItem *table_item = NULL;
    ObSelectStmt *ref_stmt = NULL;
    SelectItem *ref_select_item = NULL;
    int64_t select_item_idx = col_ref_expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
    ObString col_name;
    if (OB_ISNULL(table_item = select_stmt->get_table_item_by_id(col_ref_expr->get_table_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (!table_item->is_generated_table()) {
      // do nothing
    } else if (OB_ISNULL(ref_stmt = table_item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref query stmt is null", K(ret));
    } else if (select_item_idx < 0 || select_item_idx >= ref_stmt->get_select_item_size()) {
      // do nothing, maybe col_ref_expr is ROWID or other pseudo column
    } else if (OB_ISNULL(ref_select_item = &ref_stmt->get_select_item(select_item_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select item is null", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator_, ref_select_item->alias_name_, col_name))) {
      LOG_WARN("Can not malloc space for constraint name", K(ret));
    } else if (col_name.length() > 0) {
      // some columns such as ROWID in oracle mode may not have alias name, hence only
      // replace column name when the ref column's alias name (col_name) is not empty.
      col_ref_expr->get_column_name().assign_ptr(col_name.ptr(), col_name.length());
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(recursive_update_column_name(select_stmt, expr->get_param_expr(i))))) {
        LOG_WARN("fail to update child column name", K(ret), K(i), KPC(expr));
      }
    }
  }
  return ret;
}

int ObSelectResolver::check_rollup_items_valid(const ObIArray<ObRollupItem> &rollup_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < rollup_items.count(); i++) {
    const ObIArray<ObGroupbyExpr> &rollup_list_exprs = rollup_items.at(i).rollup_list_exprs_;
    for (int64_t j = 0; OB_SUCC(ret) && j < rollup_list_exprs.count(); ++j) {
      const ObIArray<ObRawExpr*> &groupby_exprs = rollup_list_exprs.at(j).groupby_exprs_;
      for (int64_t k = 0; OB_SUCC(ret) && k < groupby_exprs.count(); ++k) {
        ObRawExpr *groupby_expr = NULL;
        if (OB_ISNULL(groupby_expr = groupby_exprs.at(k))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rollup expr is null", K(ret));
        } else if (lib::is_oracle_mode() && ObGeometryType == groupby_expr->get_data_type()) {
           // oracle error code compability
          ret = OB_ERR_COMPARE_VARRAY_LOB_ATTR;
          LOG_WARN("Incorrect cmp type with geometry arguments", K(ret));
        } else if (ObLongTextType == groupby_expr->get_data_type()
                  || ObLobType == groupby_expr->get_data_type()
                  || ObJsonType == groupby_expr->get_data_type()
                  || ObGeometryType == groupby_expr->get_data_type()
                  || ObExtendType == groupby_expr->get_data_type()
                  || ObUserDefinedSQLType == groupby_expr->get_data_type()) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("group by lob or udt expr is not allowed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSelectResolver::check_cube_items_valid(const ObIArray<ObCubeItem> &cube_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cube_items.count(); i++) {
    const ObIArray<ObGroupbyExpr> &cube_list_exprs = cube_items.at(i).cube_list_exprs_;
    for (int64_t j = 0; OB_SUCC(ret) && j < cube_list_exprs.count(); ++j) {
      const ObIArray<ObRawExpr*> &groupby_exprs = cube_list_exprs.at(j).groupby_exprs_;
      for (int64_t k = 0; OB_SUCC(ret) && k < groupby_exprs.count(); ++k) {
        ObRawExpr *groupby_expr = NULL;
        if (OB_ISNULL(groupby_expr = groupby_exprs.at(k))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group by expr is null", K(ret));
        } else if (ObLongTextType == groupby_expr->get_data_type() ||
                   ObLobType == groupby_expr->get_data_type() ||
                   ObJsonType == groupby_expr->get_data_type()) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("group by lob expr is not allowed", K(ret), K(groupby_expr->get_data_type()));
        }
      }
    }
  }
  return ret;
}

int ObSelectResolver::recursive_check_grouping_columns(ObSelectStmt *stmt, ObRawExpr *expr, bool is_in_aggr)
{
  int ret = OB_SUCCESS;
  ObAggFunRawExpr *c_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(expr));
  } else if (!expr->has_flag(CNT_AGG)) {
    /*do nothing*/
  } else if (T_FUN_GROUPING == expr->get_expr_type()) {
    if (OB_ISNULL(c_expr = static_cast<ObAggFunRawExpr*>(expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unable to convert expr to ObAggFunRawExpr", K(ret));
    } else if (is_in_aggr && c_expr->is_aggr_expr()) {
      ret = OB_ERR_GROUP_FUNC_NOT_ALLOWED;
      LOG_WARN("grouping shouldn't be nested", K(ret));
    } else if (1 != c_expr->get_real_param_exprs().count() ||
               OB_ISNULL(c_expr->get_real_param_exprs().at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check grouping has unexpected err", K(ret));      
    } else if (OB_FAIL(check_grouping_columns(*stmt,
                                  c_expr->get_real_param_exprs_for_update().at(0)))) {
      LOG_WARN("failed to check grouping columns", K(ret));
    } else {
      assign_grouping();
    }
  } else if (T_FUN_GROUPING_ID == expr->get_expr_type()) {
    if (OB_ISNULL(c_expr = static_cast<ObAggFunRawExpr*>(expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unable to convert expr to ObAggFunRawExpr", K(ret));
    } else if (is_in_aggr && c_expr->is_aggr_expr()) {
      ret = OB_ERR_GROUP_FUNC_NOT_ALLOWED;
      LOG_WARN("grouping shouldn't be nested", K(ret));
    } else if (c_expr->get_real_param_count() < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check grouping_id has unexpected err", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < c_expr->get_real_param_count(); i++) {
        if (OB_FAIL(check_grouping_columns(*stmt,
                                  c_expr->get_real_param_exprs_for_update().at(i)))) {
          LOG_WARN("fail to check grouping columns", K(ret));
        }
      }
    }
  } else if (T_FUN_GROUP_ID == expr->get_expr_type()) {
    if (OB_ISNULL(c_expr = static_cast<ObAggFunRawExpr*>(expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unable to convert expr to ObAggFunRawExpr", K(ret));
    } else if (is_in_aggr && c_expr->is_aggr_expr()) {
      ret = OB_ERR_GROUP_FUNC_NOT_ALLOWED;
      LOG_WARN("group_id shouldn't be nested", K(ret));
    } else if (stmt->get_group_expr_size() == 0 &&
               stmt->get_rollup_expr_size() == 0 &&
               stmt->get_grouping_sets_items_size() == 0 &&
               stmt->get_rollup_items_size() == 0 &&
               stmt->get_cube_items_size() == 0) {
      ret = OB_ERR_GROUPING_FUNC_WITHOUT_GROUP_BY;
      LOG_WARN("GROUPING function only supported with GROUP BY CUBE or ROLLUP", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr *expr_param = expr->get_param_expr(i);
      if (OB_ISNULL(expr_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_check_grouping_columns(stmt, expr_param, is_in_aggr || expr->is_aggr_expr())))) {
        LOG_WARN("failed to recursive check grouping columns", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObSelectResolver::resolve_shared_order_item(OrderItem &order_item, ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  ObRawExpr *expr = NULL;
  bool find = false;
  ObQuestionmarkEqualCtx cmp_ctx(false);
  if (OB_ISNULL(select_stmt) ||
      OB_ISNULL(params_.query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret));
  } else if (select_stmt->is_order_siblings()) {
    // do noting
  } else if (OB_FAIL(select_stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (ObOptimizerUtil::find_item(select_exprs, order_item.expr_)) {
    find = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < select_exprs.count(); ++i) {
    if (OB_ISNULL(expr = select_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!expr->same_as(*order_item.expr_, &cmp_ctx)) {
      cmp_ctx.equal_pairs_.reuse();
    } else if (OB_FAIL(append(params_.query_ctx_->all_equal_param_constraints_,
                              cmp_ctx.equal_pairs_))) {
      LOG_WARN("failed to add equal constraint", K(ret));
    } else {
      order_item.expr_ = expr;
      find = true;
    }
  }
  return ret;
}

int ObSelectResolver::adjust_recursive_cte_table_columns(const ObSelectStmt* parent_stmt, ObSelectStmt *right_stmt)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  ObSEArray<ColumnItem, 4> columns;
  ObSEArray<ObRawExpr*, 4> parent_sel_exprs;
  if (OB_ISNULL(parent_stmt) ||
      OB_ISNULL(right_stmt) ||
      OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(parent_stmt->get_select_exprs(parent_sel_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < right_stmt->get_table_size(); ++i) {
    columns.reuse();
    if (OB_ISNULL(table_item = right_stmt->get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!table_item->is_fake_cte_table() ||
               table_item->table_name_ != cte_ctx_.current_cte_table_name_) {
      //do nothing
    } else if (OB_FAIL(right_stmt->get_column_items(table_item->table_id_, columns))) {
      LOG_WARN("failed to get column items", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < columns.count(); ++j) {
        ObRawExpr *sel_expr = NULL;
        ObRawExpr *column_expr = NULL;
        int64_t idx = columns.at(j).column_id_ - OB_APP_MIN_COLUMN_ID;
        if (OB_UNLIKELY(idx >= parent_sel_exprs.count()) ||
            OB_ISNULL(sel_expr = parent_sel_exprs.at(idx)) ||
            OB_ISNULL(column_expr = columns.at(j).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(sel_expr), K(ret));
        } else if (OB_FAIL(schema_checker_->adjust_fake_cte_column_type(table_item->ref_id_,
                                                                        columns.at(j).column_id_,
                                                                        sel_expr->get_data_type(),
                                                                        sel_expr->get_accuracy()))) {
          LOG_WARN("failed to adjust fake cte column type", K(ret));
        } else {
          column_expr->set_data_type(sel_expr->get_data_type());
          column_expr->set_accuracy(sel_expr->get_accuracy());
        }
      }
    }
  }
  return ret;
}

int ObSelectResolver::check_listagg_aggr_param_valid(ObAggFunRawExpr *aggr_expr)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    ObSEArray<ObRawExpr*, 4> check_separator_exprs;
    ObSEArray<ObRawExpr*, 4> all_group_by_exprs;
    if (OB_ISNULL(aggr_expr) || OB_ISNULL(get_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (aggr_expr->get_expr_type() != T_FUN_GROUP_CONCAT ||
               aggr_expr->get_real_param_count() < 2) {
      //do nothing
    } else if (OB_UNLIKELY(aggr_expr->get_real_param_count() > 2)) {
      ret = OB_ERR_PARAM_SIZE;
      LOG_WARN("invalid number of arguments", K(ret), KPC(aggr_expr));
    } else if (aggr_expr->get_real_param_exprs().at(aggr_expr->get_real_param_count() - 1)->is_const_expr()) {
      //do nothing
    } else if (aggr_expr->get_real_param_exprs().at(aggr_expr->get_real_param_count() - 1)->has_flag(CNT_AGG)) {
      ret = OB_ERR_ARGUMENT_SHOULD_CONSTANT;
      LOG_WARN("argument is should be a const expr", K(ret), KPC(aggr_expr));
    } else if (OB_FAIL(check_separator_exprs.push_back(aggr_expr->get_real_param_exprs().at(aggr_expr->get_real_param_count() - 1)))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(get_select_stmt()->get_all_group_by_exprs(all_group_by_exprs))) {
      LOG_WARN("failed to get all group by exprs", K(ret));
    } else if (OB_FAIL(ObGroupByChecker::check_by_expr(params_.param_list_,
                                                       get_select_stmt(),
                                                       all_group_by_exprs,
                                                       check_separator_exprs,
                                                       OB_ERR_ARGUMENT_SHOULD_CONSTANT_OR_GROUP_EXPR))) {
      LOG_WARN("fail to check by expr", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
