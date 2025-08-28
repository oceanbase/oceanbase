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

#include "sql/resolver/dml/ob_inlist_resolver.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/engine/expr/ob_expr_version.h"
#include "sql/rewrite/ob_transform_pre_process.h"

namespace oceanbase
{

using namespace common;

namespace sql
{
struct InListRewriteInfo {
  bool is_valid_as_values_table_;
  bool is_question_mark_;
  // the start_param_idx_ and end_param_idx_ are valid only if is_question_mark_ == true
  // the start_param_idx_ is the start index of question marks in inlist
  int64_t start_param_idx_;
  // the end_param_idx_ would be updated during calling `get_inlist_rewrite_info`
  // it would be set as the index of the last processed question mark
  // so after done calling `get_inlist_rewrite_info` for all columns of an inlist
  // it would be set as the last index of question marks in inlist
  int64_t end_param_idx_;
  ObSEArray<DistinctObjMeta, 4> param_types_; // for each column

  InListRewriteInfo(): is_valid_as_values_table_(true),
                       is_question_mark_(false),
                       start_param_idx_(-1),
                       end_param_idx_(-1) {}

  void reset() {
    is_valid_as_values_table_ = true;
    is_question_mark_ = false;
    start_param_idx_ = -1;
    end_param_idx_ = -1;
    param_types_.reuse();
  }
};

int ObInListResolver::resolve_inlist(ObInListInfo &inlist_info)
{
  int ret = OB_SUCCESS;
  const ParseNode *list_node = inlist_info.in_list_;
  const int64_t column_cnt = inlist_info.column_cnt_;
  const int64_t row_cnt = inlist_info.row_cnt_;
  ObValuesTableDef *table_def = NULL;
  if (OB_UNLIKELY(row_cnt <= 0 || column_cnt <= 0) || OB_ISNULL(cur_resolver_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got NULL ptr", K(ret), KP(list_node), KP(cur_resolver_));
  } else {
    ObResolverParams &params = cur_resolver_->params_;
    if (OB_ISNULL(params.session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("got NULL ptr", K(ret), KP(list_node), KP(cur_resolver_));
    } else if (OB_FAIL(resolve_values_table_from_inlist(list_node,
                                                        column_cnt,
                                                        row_cnt,
                                                        inlist_info.is_question_mark_,
                                                        params.is_prepare_stage_,
                                                        NULL == params.session_info_->get_pl_context(),
                                                        params.param_list_,
                                                        params.session_info_,
                                                        params.allocator_,
                                                        table_def))) {
      LOG_WARN("failed to resolve values table from inlist", K(ret));
    } else if (OB_FAIL(resolve_subquery_from_values_table(params.stmt_factory_,
                                                          params.session_info_,
                                                          params.allocator_,
                                                          params.query_ctx_,
                                                          params.expr_factory_,
                                                          table_def,
                                                          params.is_prepare_protocol_ && params.is_prepare_stage_,
                                                          column_cnt,
                                                          inlist_info.in_list_expr_))) {
      LOG_WARN("failed to alloc and init values stmt", K(ret));
    }
  }
  return ret;
}

int ObInListResolver::resolve_values_table_from_inlist(const ParseNode *in_list,
                                                const int64_t column_cnt,
                                                const int64_t row_cnt,
                                                const bool is_question_mark,
                                                const bool is_prepare_stage,
                                                const bool is_called_in_sql,
                                                const ParamStore *param_store,
                                                ObSQLSessionInfo *session_info,
                                                ObIAllocator *allocator,
                                                ObValuesTableDef *&table_def)
{
  int ret = OB_SUCCESS;
  char *table_buf = NULL;
  // we treat question marks in prepare stmt as objs instead of params
  ObValuesTableDef::TableAccessType access_type = is_question_mark && !is_prepare_stage
                                                  ? ObValuesTableDef::ACCESS_PARAM 
                                                  : ObValuesTableDef::ACCESS_OBJ;
  if (OB_ISNULL(allocator) || OB_ISNULL(session_info) || OB_ISNULL(in_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected NULL ptr", K(ret));
  } else if (OB_UNLIKELY(in_list->num_child_ != row_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected param", K(ret), K(row_cnt), K(in_list->num_child_));
  } else if (OB_ISNULL(table_buf = static_cast<char*>(allocator->alloc(sizeof(ObValuesTableDef))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("sub_query or table_buf is null", K(ret), KP(table_buf));
  } else {
    table_def = new (table_buf) ObValuesTableDef();
    table_def->column_cnt_ = column_cnt;
    table_def->row_cnt_ = row_cnt;
    table_def->access_type_ = access_type;
    table_def->is_const_ = true;
  }
  if (OB_FAIL(ret)) {
  } else if (ObValuesTableDef::ACCESS_PARAM == access_type &&
             OB_FAIL(resolve_access_param_values_table(*in_list, column_cnt, row_cnt, param_store,
                                                 session_info, allocator, is_called_in_sql, *table_def))) {
    LOG_WARN("failed to resolve access param values table", K(ret));
  } else if (ObValuesTableDef::ACCESS_OBJ == access_type &&
             OB_FAIL(resolve_access_obj_values_table(*in_list, column_cnt, row_cnt, session_info,
                                                     allocator, is_prepare_stage, is_called_in_sql, *table_def))) {
    LOG_WARN("failed to resolve access obj values table", K(ret));
  } else if (OB_FAIL(cur_resolver_->estimate_values_table_stats(*table_def))) {
    LOG_WARN("failed to estimate values table stats", K(ret));
  }
  return ret;
}

int ObInListResolver::resolve_subquery_from_values_table(ObStmtFactory *stmt_factory,
                                                         ObSQLSessionInfo *session_info,
                                                         ObIAllocator *allocator,
                                                         ObQueryCtx *query_ctx,
                                                         ObRawExprFactory *expr_factory,
                                                         ObValuesTableDef *table_def,
                                                         const bool is_prepare_stmt,
                                                         const int64_t column_cnt,
                                                         ObQueryRefRawExpr *query_ref)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *subquery = NULL;
  if (OB_ISNULL(allocator) || OB_ISNULL(session_info) || OB_ISNULL(stmt_factory) ||
      OB_ISNULL(query_ctx) || OB_ISNULL(expr_factory) || OB_ISNULL(query_ref) ||
      OB_ISNULL(table_def)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected NULL ptr", K(ret));
  } else if (OB_FAIL(stmt_factory->create_stmt(subquery))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create stmt success, but stmt is null");
  } else {
    subquery->set_query_ctx(query_ctx);
    subquery->get_query_ctx()->set_is_prepare_stmt(is_prepare_stmt);		
    subquery->get_query_ctx()->set_timezone_info(get_timezone_info(session_info));		
    subquery->get_query_ctx()->set_sql_stmt_coll_type(get_obj_print_params(session_info).cs_type_);
    subquery->assign_distinct();
    query_ref->set_ref_stmt(subquery);
    query_ref->set_is_set(true);
    query_ref->set_output_column(column_cnt);
    if (OB_FAIL(subquery->set_stmt_id())) {
      LOG_WARN("fail to set stmt id", K(ret));
    } else if (OB_FAIL(ObResolverUtils::create_values_table_query(session_info, allocator,
                                                                  expr_factory, query_ctx, subquery,
                                                                  table_def))) {
      LOG_WARN("failed to resolve values table query", K(ret));
    } else if (OB_FAIL(cur_resolver_->get_stmt()->add_subquery_ref(query_ref))) {
      LOG_WARN("failed to add subquery reference", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
        if (OB_FAIL(query_ref->add_column_type(table_def->column_types_.at(i)))) {
          LOG_WARN("add column type to subquery ref expr failed", K(ret));
        }
      }
    }
  }
  return ret;
}

// notice that if:
// 1. rewrite_info.is_valid_as_values_table_ == false, or
// 2. rewrite_info.is_question_mark_ == true && helper.is_prepare_stmt_ == true
// then rewrite_info.param_types_ will not be filled up as expected
int ObInListResolver::get_inlist_rewrite_info(const ParseNode &in_list,
                                              const int64_t column_cnt,
                                              int64_t col_idx,
                                              ObInListsResolverHelper &helper,
                                              InListRewriteInfo &rewrite_info)
{
  int64_t ret = OB_SUCCESS;
  int64_t row_cnt = -1;
  bool enable_hybrid_inlist = ObTransformUtils::is_enable_hybrid_inlist_rewrite(helper.optimizer_features_enable_version_);
  if (OB_ISNULL(in_list.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", K(ret));
  } else if (FALSE_IT(row_cnt = in_list.num_child_)) {
  } else if (row_cnt <= 0 || column_cnt <= 0 || col_idx >= column_cnt) {
    // delay error later
    rewrite_info.is_valid_as_values_table_ = false;
  } else {
    rewrite_info.is_valid_as_values_table_ = true;
    while (OB_SUCC(ret) && rewrite_info.param_types_.count() < column_cnt) {
      if (OB_FAIL(rewrite_info.param_types_.push_back(DistinctObjMeta()))) {
        LOG_WARN("failed to push back empty param type", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && rewrite_info.is_valid_as_values_table_ && i < row_cnt; ++i) {
      if (OB_ISNULL(in_list.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_UNLIKELY(column_cnt > 1 && in_list.children_[i]->num_child_ != column_cnt)) {
        // delay error later
        rewrite_info.is_valid_as_values_table_ = false;
      } else {
        DistinctObjMeta cur_param_type = DistinctObjMeta();
        bool is_const_type = false;
        const ParseNode *node = column_cnt == 1 ? in_list.children_[i] 
                                                : in_list.children_[i]->children_[col_idx];
        if (OB_ISNULL(node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected param", K(ret));
        } else if (FALSE_IT(rewrite_info.is_question_mark_ = T_QUESTIONMARK == node->type_)) {
        } else if (rewrite_info.is_question_mark_) {
          // check if param indexes are continuous for values table
          if (-1 == rewrite_info.start_param_idx_ 
              && FALSE_IT(rewrite_info.start_param_idx_ = node->value_)) {
          } else if (FALSE_IT(rewrite_info.end_param_idx_ = node->value_)) {
          } else if (OB_UNLIKELY(node->value_ != rewrite_info.start_param_idx_ + col_idx + i * column_cnt
                                 || node->value_ < 0)) {
            rewrite_info.is_valid_as_values_table_ = false;
          }
        } else { /*do nothing*/ }
        if (OB_FAIL(ret) || !rewrite_info.is_valid_as_values_table_) {
        } else if (helper.is_prepare_stmt_ && rewrite_info.is_question_mark_) {
          // skip getting types for question marks in prepare stmt, because they have no type
        } else if (!(T_QUESTIONMARK == node->type_ && rewrite_info.is_question_mark_) &&
                   !(IS_DATATYPE_OP(node->type_) && !rewrite_info.is_question_mark_)) {
          // not const type
          rewrite_info.is_valid_as_values_table_ = false;
        } else if (OB_FAIL(ObResolverUtils::fast_get_param_type(*node,
                                                                helper.param_store_, 
                                                                helper.connect_collation_,
                                                                helper.nchar_collation_, 
                                                                helper.server_collation_, 
                                                                helper.enable_decimal_int_, 
                                                                helper.alloc_,
                                                                cur_param_type.obj_type_, 
                                                                cur_param_type.coll_type_,
                                                                cur_param_type.coll_level_))) {
          // not const type
          LOG_WARN("failed to fast get param type", K(ret));
          ret = OB_SUCCESS;
          rewrite_info.is_valid_as_values_table_ = false;
        } else if (0 == i) {
          rewrite_info.param_types_.at(col_idx) = cur_param_type;
        } else if (rewrite_info.param_types_.at(col_idx) == cur_param_type) {
        } else if (enable_hybrid_inlist && ObNullType == cur_param_type.obj_type_) {
          // ignore null type
        } else if (enable_hybrid_inlist && ObNullType == rewrite_info.param_types_.at(col_idx).obj_type_) {
          rewrite_info.param_types_.at(col_idx) = cur_param_type;
        } else {
          rewrite_info.is_valid_as_values_table_ = false;
          LOG_WARN("get inconsistent param types in big inlist", K(column_cnt), K(row_cnt), K(i),
                   K(col_idx), K(cur_param_type), K(rewrite_info.param_types_.at(col_idx)));
        }
      }
    }
  }
  return ret;
}

int ObInListResolver::check_inlist_rewrite_enable(const ParseNode &in_list,
                                                  const ObItemType op_type,
                                                  const ObRawExpr &left_expr,
                                                  const ObStmtScope &scope,
                                                  const bool is_root_condition,
                                                  const bool is_need_print,
                                                  const bool is_in_pl_prepare,
                                                  const ObSQLSessionInfo *session_info,
                                                  const ParamStore *param_store,
                                                  const ObStmt *stmt,
                                                  ObIAllocator &alloc,
                                                  bool &is_question_mark,
                                                  bool &is_enable)
{
  int ret = OB_SUCCESS;
  is_enable = false;
  int64_t threshold = INT64_MAX;
  uint64_t optimizer_features_enable_version = 0;
  bool is_prepare_stmt = false;
  // 1. check basic requests
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(session_info));
  } else if (T_WHERE_SCOPE != scope 
             || !is_root_condition 
             || (T_OP_IN != op_type && T_OP_NOT_IN != op_type)
             || T_EXPR_LIST != in_list.type_
             || is_need_print
             || is_in_pl_prepare
             || (NULL != stmt
                 && stmt->is_select_stmt()
                 && static_cast<const ObSelectStmt *>(stmt)->is_hierarchical_query())) {
    LOG_TRACE("no need rewrite inlist", 
              K(is_root_condition), K(scope), K(in_list.type_), K(op_type), K(is_need_print));
  } else {
    if (NULL == stmt) {
      if (OB_FAIL(session_info->get_optimizer_features_enable_version(optimizer_features_enable_version))) {
        LOG_WARN("failed to check ddl schema version", K(ret));
      } else {
        threshold = session_info->get_inlist_rewrite_threshold();
      }
    } else {
      if (OB_ISNULL(stmt->get_query_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        is_prepare_stmt = stmt->get_query_ctx()->is_prepare_stmt();
        threshold = session_info->get_inlist_rewrite_threshold();
        const ObGlobalHint &global_hint = stmt->get_query_ctx()->get_global_hint();
        if (OB_FAIL(global_hint.opt_params_.get_integer_opt_param(
                                              ObOptParamHint::INLIST_REWRITE_THRESHOLD, threshold))) {
          LOG_WARN("failed to get integer opt param", K(ret));
        } else if (global_hint.has_valid_opt_features_version()) {
          optimizer_features_enable_version = global_hint.opt_features_version_;
        } else if (OB_FAIL(session_info->get_optimizer_features_enable_version(optimizer_features_enable_version))) {
          LOG_WARN("failed to check ddl schema version", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!ObTransformUtils::is_enable_values_table_rewrite(optimizer_features_enable_version)) {
        LOG_TRACE("current optimizer version is less then COMPAT_VERSION_4_3_2");
      } else if (in_list.num_child_ < threshold) {
        LOG_TRACE("check rewrite inlist threshold", K(threshold), K(in_list.num_child_));
      } else if ((GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_2_0 &&
                  GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0) ||
                  GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0) {
        is_enable = true;
      }
    }
  }
  // 2. check same node type requests
  //    and whether parameter indexes correspond to what a values table expected
  if (OB_SUCC(ret) && is_enable) {
    const int64_t row_cnt = in_list.num_child_;
    const int64_t column_cnt = T_OP_ROW == left_expr.get_expr_type() ? left_expr.get_param_count() :
                                                                       1;
    ObSEArray<DistinctObjMeta, 4> param_types;
    ObCollationType connect_collation = CS_TYPE_INVALID;
    ObCollationType nchar_collation = session_info->get_nls_collation_nation();
    int64_t server_collation = CS_TYPE_INVALID;
    bool enable_decimal_int = false;
    if (OB_UNLIKELY(row_cnt <= 0 || column_cnt <= 0) || OB_ISNULL(in_list.children_[0]) ||
        OB_UNLIKELY(column_cnt > 1 && in_list.children_[0]->num_child_ != column_cnt)) {
      is_enable = false;  /* delay return error code */
    } else if (OB_FAIL(session_info->get_collation_connection(connect_collation))) {
      LOG_WARN("fail to get collation_connection", K(ret));
    } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(session_info, enable_decimal_int))) {
      LOG_WARN("fail to check enable decimal int", K(ret));
    } else if (lib::is_oracle_mode() && OB_FAIL(session_info->get_sys_variable(
                                              share::SYS_VAR_COLLATION_SERVER, server_collation))) {
      LOG_WARN("get sys variables failed", K(ret));
    } else {
      ObInListsResolverHelper helper(alloc, 
                                     param_store, 
                                     connect_collation,
                                     nchar_collation, 
                                     static_cast<ObCollationType>(server_collation), 
                                     enable_decimal_int,
                                     is_prepare_stmt,
                                     optimizer_features_enable_version);
      InListRewriteInfo rewrite_info;
      for (int64_t j = 0; OB_SUCC(ret) && is_enable && j < column_cnt; ++j) {
        if (OB_FAIL(get_inlist_rewrite_info(in_list, column_cnt, j, helper, rewrite_info))) {
          LOG_WARN("failed to get inlist const param type", K(ret));
        } else if (!rewrite_info.is_valid_as_values_table_) {
          is_enable = false;
        } else if (helper.is_prepare_stmt_ && rewrite_info.is_question_mark_) {
          // skip additional check for prepare stmt with question mark
        } else if (rewrite_info.param_types_.count() <= j) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param types count", K(ret), K(j));
        } else if (ob_is_enum_or_set_type(rewrite_info.param_types_.at(j).obj_type_) 
                   || is_lob_locator(rewrite_info.param_types_.at(j).obj_type_)) {
          is_enable = false;
        }
      }
      is_question_mark = rewrite_info.is_question_mark_;
    }
  }
  return ret;
}

int ObInListResolver::resolve_access_param_values_table(const ParseNode &in_list,
                                                        const int64_t column_cnt,
                                                        const int64_t row_cnt,
                                                        const ParamStore *param_store,
                                                        ObSQLSessionInfo *session_info,
                                                        ObIAllocator *allocator,
                                                        const bool is_called_in_sql,
                                                        ObValuesTableDef &table_def)
{
  int ret = OB_SUCCESS;
  const ParseNode *row_node = NULL;
  ObCollationType coll_type = CS_TYPE_INVALID;
  ObLengthSemantics length_semantics = LS_DEFAULT;
  if (OB_ISNULL(allocator) || OB_ISNULL(session_info) || OB_ISNULL(param_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected NULL ptr", K(ret));
  } else if (OB_FAIL(session_info->get_collation_connection(coll_type))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; i++) {
    row_node = in_list.children_[i];
    if (OB_ISNULL(row_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("got unexpected ptr", K(ret), KP(row_node), K(i));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < column_cnt; j++) {
      const ParseNode *element = column_cnt == 1 ? row_node : row_node->children_[j];
      const ObObjParam &obj_param = param_store->at(element->value_);
      ObRawExprResType res_type;
      res_type.set_meta(obj_param.get_param_meta());
      res_type.set_accuracy(obj_param.get_accuracy());
      res_type.set_result_flag(obj_param.get_result_flag());
      if (i == 0) {
        if (OB_FAIL(table_def.column_types_.push_back(res_type))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else {
        // is not same ObExprResType, than compute a new one
        ObExprResType new_res_type;
        ObExprVersion dummy_op(*allocator);
        ObSEArray<ObExprResType, 2> tmp_res_types;
        ObExprTypeCtx type_ctx;
        ObSQLUtils::init_type_ctx(session_info, type_ctx);
        if (OB_FAIL(tmp_res_types.push_back(table_def.column_types_.at(j)))) {
          LOG_WARN("failed to push back res type", K(ret));
        } else if (OB_FAIL(tmp_res_types.push_back(res_type))) {
          LOG_WARN("failed to push back res type", K(ret));
        } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(new_res_type,
                           &tmp_res_types.at(0), 2, lib::is_oracle_mode(), type_ctx,
                           false, false, is_called_in_sql))) {
          LOG_WARN("failed to aggregate result type for merge", K(ret));
        } else {
          table_def.column_types_.at(j) = new_res_type;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    table_def.start_param_idx_ = column_cnt == 1 ? in_list.children_[0]->value_ :
                                                   in_list.children_[0]->children_[0]->value_;
    table_def.end_param_idx_ = column_cnt == 1 ? in_list.children_[row_cnt - 1]->value_ :
                                  in_list.children_[row_cnt - 1]->children_[column_cnt - 1]->value_;
  }
  return ret;
}

int ObInListResolver::resolve_access_obj_values_table(const ParseNode &in_list,
                                                      const int64_t column_cnt,
                                                      const int64_t row_cnt,
                                                      ObSQLSessionInfo *session_info,
                                                      ObIAllocator *allocator,
                                                      const bool is_prepare_stage,
                                                      const bool is_called_in_sql,
                                                      ObValuesTableDef &table_def)
{
  int ret = OB_SUCCESS;
  const bool is_oracle_mode = lib::is_oracle_mode();
  const bool is_parameterize = false;
  const ObTimeZoneInfo *timezone_info = NULL;
  const bool is_from_pl = nullptr != cur_resolver_->params_.secondary_namespace_;
  ObCollationType coll_type = CS_TYPE_INVALID;
  ObLengthSemantics length_semantics = LS_DEFAULT;
  int64_t server_collation = CS_TYPE_INVALID;
  stmt::StmtType stmt_type = stmt::T_NONE;
  ObExprInfo parents_expr_info;
  ObString literal_prefix;
  ObCollationType nchar_collation = CS_TYPE_INVALID;
  const ParseNode *row_node = NULL;
  bool enable_decimal_int = false;
  ObCompatType compat_type = COMPAT_MYSQL57;
  bool enable_mysql_compatible_dates = false;
  if (OB_ISNULL(allocator) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected NULL ptr", K(ret));
  } else if (OB_FAIL(session_info->get_compatibility_control(compat_type))) {
    LOG_WARN("failed to get compat type", K(ret));
  } else if (OB_FAIL(session_info->get_collation_connection(coll_type))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (is_oracle_mode && OB_FAIL(session_info->get_sys_variable(
                                       share::SYS_VAR_COLLATION_SERVER, server_collation))) {
    LOG_WARN("get sys variables failed", K(ret));
  } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(session_info, enable_decimal_int))) {
    LOG_WARN("fail to check enable decimal int", K(ret));
  } else if (OB_FAIL(ObSQLUtils::check_enable_mysql_compatible_dates(session_info, false,
                       enable_mysql_compatible_dates))) {
    LOG_WARN("fail to check enable mysql compatible dates", K(ret));
  } else {
    if (lib::is_oracle_mode() && cur_resolver_->params_.is_expanding_view_) {
      // numeric constants should parsed with ObNumber in view expansion for oracle mode
      enable_decimal_int = false;
    }
    length_semantics = session_info->get_actual_nls_length_semantics();
    timezone_info = session_info->get_timezone_info();
    stmt_type = is_oracle_mode ? session_info->get_stmt_type() : stmt::T_NONE;
    nchar_collation = session_info->get_nls_collation_nation();
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; i++) {
    row_node = in_list.children_[i];
    if (OB_ISNULL(row_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("got unexpected ptr", K(ret), KP(row_node));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < column_cnt; j++) {
      const ParseNode *element = column_cnt == 1 ? row_node : row_node->children_[j];
      ObObjParam obj_param;
      ObRawExprResType res_type;
      if (OB_FAIL(ObResolverUtils::resolve_const(element, stmt_type, *allocator, coll_type,
                                                 nchar_collation, timezone_info, obj_param, is_parameterize,
                                                 literal_prefix, length_semantics,
                                                 static_cast<ObCollationType>(server_collation),
                                                 &parents_expr_info,
                                                 session_info->get_sql_mode(),
                                                 enable_decimal_int,
                                                 compat_type,
                                                 enable_mysql_compatible_dates,
                                                 session_info->get_min_const_integer_precision(),
                                                 is_from_pl))) {
        LOG_WARN("failed to resolve const", K(ret));
      } else if (OB_FAIL(table_def.access_objs_.push_back(obj_param))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        res_type.set_meta(obj_param.get_param_meta());
        res_type.set_accuracy(obj_param.get_accuracy());
        res_type.set_result_flag(obj_param.get_result_flag());
        if (i == 0) {
          if (OB_FAIL(table_def.column_types_.push_back(res_type))) {
            LOG_WARN("failed to push back", K(ret));
          }
        } else if (is_prepare_stage 
                   && (ObUnknownType == res_type.get_type()
                       || ObUnknownType == table_def.column_types_.at(j).get_type())) {
          // in prepare stage, question marks are resolved as unknown type
          // since unknown type cannot be merged with other normal types
          // we ignore type aggregation if:
          // 1. the current node is a question mark
          // 2. the values table contains a question mark
          // this is safe because correct column types will be deduced later in execute stage
        } else if (!ObSQLUtils::is_same_type(res_type, table_def.column_types_.at(j))) {
          ObExprResType new_res_type;
          ObExprVersion dummy_op(*allocator);
          ObSEArray<ObExprResType, 2> tmp_res_types;
          ObExprTypeCtx type_ctx;
          ObSQLUtils::init_type_ctx(session_info, type_ctx);
          if (OB_FAIL(tmp_res_types.push_back(table_def.column_types_.at(j)))) {
            LOG_WARN("failed to push back res type", K(ret));
          } else if (OB_FAIL(tmp_res_types.push_back(res_type))) {
            LOG_WARN("failed to push back res type", K(ret));
          } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(new_res_type,
                            &tmp_res_types.at(0), 2, is_oracle_mode,
                            type_ctx, false, false, is_called_in_sql))) {
            LOG_WARN("failed to aggregate result type for merge", K(ret));
          } else {
            table_def.column_types_.at(j) = new_res_type;
          }
        }
      }
    }
  }
  return ret;
}

// dst_in_node is NOT NULL: merge the right child of dst_in_node with src_in_node
// dst_in_node is NULL: copy src_in_node to dst_in_node
//                T_OP_IN (deep copy)
//                 /               \
// left_child (shallow copy)    T_EXPR_LIST (deep copy)
//                                  |
//                            children nodes (shallow copy)
int ObInListResolver::merge_two_in_nodes(ObIAllocator &alloc,
                                         const ParseNode *src_in_node,
                                         ParseNode *&dst_in_node)
{
  int ret = OB_SUCCESS;
  int parser_ret = OB_SUCCESS;
  ParseNode dummy;
  if (OB_ISNULL(src_in_node) 
      || OB_UNLIKELY(2 != src_in_node->num_child_)
      || OB_ISNULL(src_in_node->children_)
      || OB_ISNULL(src_in_node->children_[0]) 
      || OB_ISNULL(src_in_node->children_[1])
      || T_EXPR_LIST != src_in_node->children_[1]->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src IN node", K(ret), K(src_in_node));
  } else if (NULL != dst_in_node) {
    if (OB_UNLIKELY(2 != dst_in_node->num_child_)
        || OB_ISNULL(dst_in_node->children_)
        || OB_ISNULL(dst_in_node->children_[0])
        || OB_ISNULL(dst_in_node->children_[1])
        || T_EXPR_LIST != dst_in_node->children_[1]->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid dst IN node", K(ret));
    } else if (OB_ISNULL(dst_in_node->children_[1] = append_child(&alloc, &parser_ret, 
                                                                  dst_in_node->children_[1], 
                                                                  src_in_node->children_[1]))
               || OB_FAIL(parser_ret)) {
      // dst_in_node is not NULL, then merge the right child of dst_in_node with src_in_node
      ret = OB_SUCCESS == parser_ret ? OB_ERR_UNEXPECTED : parser_ret;
      LOG_WARN("failed to append child node of in op", K(ret), K(parser_ret));
    } else { /* do nothing */ }
  } else {
    // dst_in_node is NULL, then copy src_in_node to dst_in_node
    // no need to deep copy the left child though
    if (OB_ISNULL(dst_in_node = new_node(&alloc, src_in_node->type_, 2))
        || OB_ISNULL(dst_in_node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create new in node", K(ret));
    } else if (OB_FAIL(deep_copy_parse_node_base(&alloc, src_in_node, dst_in_node))) {
      LOG_WARN("failed to deep copy IN node", K(ret));
    } else if (FALSE_IT(dst_in_node->children_[0] = src_in_node->children_[0])) { // shallow copy left child
    } else if (OB_ISNULL(dst_in_node->children_[1] = new_node(&alloc, src_in_node->children_[1]->type_, 0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create new right child node of in op", K(ret));
    } else if (OB_FAIL(deep_copy_parse_node_base(&alloc, 
                                                 src_in_node->children_[1], 
                                                 dst_in_node->children_[1]))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to deep copy right child node of in op", K(ret));
    } else {
      dst_in_node->children_[1]->num_child_ = 0;
      dst_in_node->children_[1]->value_ = 0;
      if (OB_ISNULL(dst_in_node->children_[1] = append_child(&alloc, &parser_ret, 
                                                             dst_in_node->children_[1], 
                                                             src_in_node->children_[1]))
          || OB_FAIL(parser_ret)) {
        ret = OB_SUCCESS == parser_ret ? OB_ERR_UNEXPECTED : parser_ret;
        LOG_WARN("failed to append child node of in op", K(ret), K(parser_ret));
      }
    }
  }
  return ret;
}


// Two IN nodes can be merged if:
// 1. both of them can be rewritten as a values table independently 
// 2. the param indexes of the right child of the two IN nodes are continuous
// 3. the param types of each column of the right children of two IN nodes are the same
int ObInListResolver::check_can_merge_inlists(const ParseNode *last_in_node,
                                              const ParseNode *cur_in_node,
                                              ObInListsResolverHelper &helper,
                                              InListRewriteInfo &last_info,
                                              bool &can_merge)
{
  int ret = OB_SUCCESS;
  InListRewriteInfo cur_info;
  int64_t column_cnt = -1;
  // last_info.param_types_ is empty means the last inlist info has not been filled yet
  bool need_process_last = last_info.param_types_.count() == 0;
  if (OB_ISNULL(last_in_node) || OB_ISNULL(last_in_node->children_) 
      || OB_UNLIKELY(2 != last_in_node->num_child_)
      || OB_ISNULL(last_in_node->children_[0]) || OB_ISNULL(last_in_node->children_[1])
      || OB_ISNULL(cur_in_node) || OB_ISNULL(cur_in_node->children_) 
      || OB_UNLIKELY(2 != cur_in_node->num_child_)
      || OB_ISNULL(cur_in_node->children_[0]) || OB_ISNULL(cur_in_node->children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid in node", K(ret), K(last_in_node), K(cur_in_node));
  } else {
    column_cnt = T_EXPR_LIST == cur_in_node->children_[0]->type_ ? cur_in_node->children_[0]->num_child_ : 1;
  }
  for (int64_t j = 0; OB_SUCC(ret) && can_merge && j < column_cnt; ++j) {
    if (need_process_last && OB_FAIL(get_inlist_rewrite_info(*last_in_node->children_[1],
                                                             column_cnt, j, helper, last_info))) {
      LOG_WARN("fail to get param type for inlist", K(ret));
    } else if (OB_FAIL(get_inlist_rewrite_info(*cur_in_node->children_[1],
                                               column_cnt, j, helper, cur_info))) {
      LOG_WARN("fail to get param type for inlist", K(ret));
    } else if (!last_info.is_valid_as_values_table_ 
               || !cur_info.is_valid_as_values_table_) {
      can_merge = false;  // CAN NOT merge inlists if one cannot be converted to values table
    } else if (0 == j     // it is enough to check the first column only
               && last_info.is_question_mark_ && cur_info.is_question_mark_ 
               && last_info.end_param_idx_ + column_cnt != cur_info.start_param_idx_) {
      can_merge = false;  // CAN NOT merge inlists if there param indexes are not continuous
    } else if (OB_UNLIKELY(last_info.param_types_.count() <= j
                           || cur_info.param_types_.count() <= j)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param types count", K(ret), K(j), K(last_info.param_types_), K(cur_info.param_types_));
    } else if (last_info.param_types_.at(j) == cur_info.param_types_.at(j)) {
      can_merge = true;   // two inlists have same param type for this column
    } else {
      can_merge = false;  // CAN NOT merge inlists otherwise
    }
  }
  if (OB_SUCC(ret) && can_merge) {
    // the end_param_idx_ should be updated if the two inlists can be merged
    last_info.end_param_idx_ = cur_info.end_param_idx_;
  }
  return ret;
}

int ObInListResolver::do_merge_inlists(ObIAllocator &alloc,
                                       ObInListsResolverHelper &helper,
                                       const ParseNode *root_node,
                                       const ParseNode *&ret_node)
{
  int ret = OB_SUCCESS;
  int parser_ret = OB_SUCCESS;
  int64_t new_child_num = 0;  // to track children num of the merged_node
  bool happened = false;
  ParseNode *last_in_node = NULL;
  InListRewriteInfo last_info;
  ParseNode *merged_node = NULL;
  ParseNode *new_in_node = NULL;
  if (OB_ISNULL(root_node) || OB_ISNULL(root_node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid root node", K(ret));
  } else if (OB_ISNULL(merged_node = new_node(&alloc, root_node->type_, 0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create new node", K(ret));
  } else if (OB_FAIL(deep_copy_parse_node_base(&alloc, root_node, merged_node))) {
    LOG_WARN("failed to deep copy parse node base", K(ret));
  } else {
    merged_node->value_ = 0;  // it is necessary to set capacity to 0
    merged_node->num_child_ = 0;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < root_node->num_child_; ++i) {
    ParseNode *in_node = NULL;
    bool can_merge = false;
    bool is_mergeable_in_node = false;
    // 1. basic check
    //    a. the in_node is an IN node under an OR node, or a NOT IN node under an AND node
    //    b. the right child of the IN/NOT_IN node is a T_EXPR_LIST
    if (OB_ISNULL(in_node = root_node->children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node children", K(ret));
    } else if (!(T_OP_OR == root_node->type_ && T_OP_IN == in_node->type_) 
               && !(T_OP_AND == root_node->type_ && T_OP_NOT_IN == in_node->type_)) {
    } else if (OB_ISNULL(in_node->children_) || OB_UNLIKELY(2 != in_node->num_child_)
               || OB_ISNULL(in_node->children_[0]) || OB_ISNULL(in_node->children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid IN node children", K(ret));
    } else if (T_EXPR_LIST != in_node->children_[1]->type_) {
    } else {
      is_mergeable_in_node = true;
      if (NULL != last_in_node 
          && parsenode_equal(last_in_node->children_[0], in_node->children_[0], NULL)) {
        // if the current in_node is mergeable,
        // and there is valid last_in_node that has the same left branch with the current in_node
        // then try merge the current in_node with the last one
        can_merge = true;
      }
    }
    // 2. further check whether the current IN node can be merged with the last one
    //    and do merge if so
    if (OB_FAIL(ret)) {
    } else if (can_merge && OB_FAIL(check_can_merge_inlists(last_in_node, in_node, 
                                                                helper, last_info, can_merge))) {
      LOG_WARN("fail to check whether can merge two inlists", K(ret));
    } else if (!can_merge) {
      last_info.reset();
      if (NULL != last_in_node 
          && merged_node->num_child_ + 1 == new_child_num
          && (OB_ISNULL(merged_node = push_back_child(&alloc, &parser_ret, merged_node, last_in_node))
              || OB_FAIL(parser_ret))) {
        // there is a remaining last_in_node that has not been pushed back to the children of merged_node
        ret = OB_SUCCESS == parser_ret ? OB_ERR_UNEXPECTED : parser_ret;
        LOG_WARN("failed to append child node", K(ret), K(parser_ret));
      } else if (is_mergeable_in_node) {
        // the current node is a mergeable IN node which may be merged later
        // update the last_in_node and do not push it back to merged_node yet
        last_in_node = in_node;
        new_child_num = merged_node->num_child_ + 1;  // +1 for placeholder of current IN node
      } else if (OB_ISNULL(merged_node = push_back_child(&alloc, &parser_ret, merged_node, in_node))
                 || OB_FAIL(parser_ret)) {
        ret = OB_SUCCESS == parser_ret ? OB_ERR_UNEXPECTED : parser_ret;
        LOG_WARN("failed to append child node", K(ret), K(parser_ret));
      } else {
        last_in_node = NULL;
        new_child_num = merged_node->num_child_;
      }
    } else {
      happened = true;
      if (merged_node->num_child_ + 1 == new_child_num) {
        // the first IN node of the current sequence has not been pushed back to merged_node yet
        // copy it to a new node and push it back to merged_node
        ParseNode *new_in_node = NULL;
        if (OB_FAIL(merge_two_in_nodes(alloc, last_in_node, new_in_node))) {
          LOG_WARN("fail to merge in node", K(ret), K(new_in_node));
        } else if (OB_ISNULL(merged_node = push_back_child(&alloc, &parser_ret, merged_node, new_in_node))
                   || OB_FAIL(parser_ret)) {
          ret = OB_SUCCESS == parser_ret ? OB_ERR_UNEXPECTED : parser_ret;
          LOG_WARN("failed to append child node", K(ret), K(parser_ret));
        } else {
          new_child_num = merged_node->num_child_;
        }
      } else if (OB_UNLIKELY(merged_node->num_child_ != new_child_num)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected child num", K(ret), K(merged_node->num_child_), K(new_child_num));
      }
      // now merge two IN nodes
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(merge_two_in_nodes(alloc, in_node, 
                                            merged_node->children_[merged_node->num_child_ - 1]))) {
        LOG_WARN("fail to merge in node", K(ret));
      } else {
        last_in_node = merged_node->children_[merged_node->num_child_ - 1];
      }
    }
  }
  // process the last IN node that has not been pushed back to merged_node
  if (OB_FAIL(ret)) {
  } else if (!happened) {
  } else if (NULL != last_in_node 
             && merged_node->num_child_ + 1 == new_child_num
             && (OB_ISNULL(merged_node = push_back_child(&alloc, &parser_ret, merged_node, last_in_node))
                 || OB_FAIL(parser_ret))) {
    ret = OB_SUCCESS == parser_ret ? OB_ERR_UNEXPECTED : parser_ret;
    LOG_WARN("failed to append child node", K(ret), K(parser_ret));
  } else if (OB_UNLIKELY(merged_node->num_child_ != new_child_num || 0 == merged_node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child num", K(ret), K(merged_node->num_child_), K(new_child_num));
  } else if (1 == merged_node->num_child_) {
    ret_node = merged_node->children_[0];
  } else {
    ret_node = merged_node;
  }
  return ret;
}

int ObInListResolver::try_merge_inlists(ObExprResolveContext &resolve_ctx,
                                        const bool is_root_condition,
                                        const ParseNode *root_node,
                                        const ParseNode *&ret_node)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session_info = resolve_ctx.session_info_;
  const ObStmt *stmt = resolve_ctx.stmt_;
  ObIAllocator &alloc = resolve_ctx.expr_factory_.get_allocator();
  ObCollationType connect_collation = CS_TYPE_INVALID;
  ObCollationType nchar_collation = CS_TYPE_INVALID;
  bool enable_decimal_int = false;
  bool is_prepare_stmt = false;
  int64_t server_collation = CS_TYPE_INVALID;
  uint64_t optimizer_features_enable_version = 0;
  bool is_enable = false;
  ret_node = root_node;
  if (OB_ISNULL(session_info) || OB_ISNULL(root_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (T_WHERE_SCOPE != resolve_ctx.current_scope_ // not WHERE scope
             || resolve_ctx.is_need_print_               // need print
             || !is_root_condition                       // not root condition
             || NULL != resolve_ctx.secondary_namespace_ // is pl prepare stage
             || (NULL != stmt                            // is hierarchical query
                 && stmt->is_select_stmt()
                 && static_cast<const ObSelectStmt *>(stmt)->is_hierarchical_query())) {            
    LOG_TRACE("no need to merge inlists", 
              K(resolve_ctx.current_scope_), 
              K(resolve_ctx.is_need_print_),
              K(is_root_condition));
  } else {
    if (NULL == stmt) {
      if (OB_FAIL(session_info->get_optimizer_features_enable_version(optimizer_features_enable_version))) {
        LOG_WARN("failed to check ddl schema version", K(ret));
      } else { /*do nothing*/ }
    } else if (OB_ISNULL(stmt->get_query_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      is_prepare_stmt = stmt->get_query_ctx()->is_prepare_stmt();
      const ObGlobalHint &global_hint = stmt->get_query_ctx()->get_global_hint();
      if (global_hint.has_valid_opt_features_version()) {
        optimizer_features_enable_version = global_hint.opt_features_version_;
      } else if (OB_FAIL(session_info->get_optimizer_features_enable_version(optimizer_features_enable_version))) {
        LOG_WARN("failed to check ddl schema version", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_FAIL(ret)) {
    } else if (optimizer_features_enable_version >= COMPAT_VERSION_4_3_5_BP2) {
      is_enable = true;
    } else {
      LOG_TRACE("current optimizer version is less then COMPAT_VERSION_4_3_5_BP2");
    }
  }
  if (OB_FAIL(ret) || !is_enable) {
  } else if (FALSE_IT(nchar_collation = session_info->get_nls_collation_nation())) {
  } else if (OB_FAIL(session_info->get_collation_connection(connect_collation))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(session_info, enable_decimal_int))) {
    LOG_WARN("fail to check enable decimal int", K(ret));
  } else if (lib::is_oracle_mode() 
            && OB_FAIL(session_info->get_sys_variable(share::SYS_VAR_COLLATION_SERVER, server_collation))) {
    LOG_WARN("get sys variables failed", K(ret));
  } else {
    ObInListsResolverHelper helper(alloc, 
                                   resolve_ctx.param_list_, 
                                   connect_collation,
                                   nchar_collation, 
                                   static_cast<ObCollationType>(server_collation), 
                                   enable_decimal_int,
                                   is_prepare_stmt,
                                   optimizer_features_enable_version);
    if (OB_FAIL(do_merge_inlists(alloc, helper, root_node, ret_node))) {
      LOG_WARN("fail to merge inlist", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
