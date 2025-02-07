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
  //    and whether paramameter indexes correspond to what a values table expected
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
      int64_t start_param_idx = -1;
      for (int64_t j = 0; OB_SUCC(ret) && is_enable && j < column_cnt; ++j) {
        DistinctObjMeta param_type_prev;
        for (int64_t i = 0; OB_SUCC(ret) && is_enable && i < row_cnt; ++i) {
          if (OB_UNLIKELY(column_cnt > 1 && in_list.children_[i]->num_child_ != column_cnt)) {
            is_enable = false; /* delay return error code */
          } else {
            DistinctObjMeta param_type_cur;
            const ParseNode *node = column_cnt == 1 ? in_list.children_[i]
                                                    : in_list.children_[i]->children_[j];
            if (OB_ISNULL(node)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected param", K(ret));
            } else if (FALSE_IT(is_question_mark = T_QUESTIONMARK == node->type_)) {
            } else if (is_question_mark
                       && OB_UNLIKELY(-1 == start_param_idx)
                       && FALSE_IT(start_param_idx = node->value_)) {
            } else if (is_question_mark
                       && OB_UNLIKELY(node->value_ != start_param_idx + j + i * column_cnt || node->value_ < 0)) {
              // param idx in position (i, j) is not expected, do not rewrite
              is_enable = false;
            } else if (is_prepare_stmt && is_question_mark) {
              // skip type matching for question marks in prepare stmt, because they have no type
            } else if (OB_FAIL(get_const_node_types(node,
                                                    param_store,
                                                    is_question_mark,
                                                    connect_collation,
                                                    nchar_collation,
                                                    static_cast<ObCollationType>(server_collation),
                                                    enable_decimal_int,
                                                    alloc,
                                                    param_type_cur,
                                                    is_enable))) {
              LOG_WARN("failed to got const node types", K(ret));
            } else if (!is_enable) {
            } else if (ObMaxType == param_type_prev.obj_type_) {
              param_type_prev = param_type_cur;
              if (lib::is_oracle_mode() && ObCharType == param_type_cur.obj_type_) {
                // in oracle mode, inlist to values table rewrite may cast char types to varchar2
                // but comparison behaviors for chars and varchar2s are different for trailing spaces
                // which will lead to unexpect comparison result
                is_enable = false;
              } else if (ob_is_enum_or_set_type(param_type_cur.obj_type_)
                         || is_lob_locator(param_type_cur.obj_type_)) {
                is_enable = false;
              }
            } else if (param_type_prev == param_type_cur) {
            } else {
              is_enable = false;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObInListResolver::get_const_node_types(const ParseNode *node,
                                           const ParamStore *param_store,
                                           const bool is_question_mark,
                                           const ObCollationType connect_collation,
                                           const ObCollationType nchar_collation,
                                           const ObCollationType server_collation,
                                           const bool enable_decimal_int,
                                           ObIAllocator &alloc,
                                           DistinctObjMeta &param_type,
                                           bool &is_const)
{
  int ret = OB_SUCCESS;
  is_const = false;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (!(T_QUESTIONMARK == node->type_ && is_question_mark) &&
             !(IS_DATATYPE_OP(node->type_) && !is_question_mark)) {
    /* is_const = false*/
  } else if (OB_FAIL(ObResolverUtils::fast_get_param_type(*node, param_store, connect_collation,
                                            nchar_collation, server_collation, enable_decimal_int, alloc,
                                            param_type.obj_type_, param_type.coll_type_,
                                            param_type.coll_level_))) {
    LOG_WARN("failed to fast get param type", K(ret));
    ret = OB_SUCCESS;
    /* is_const = false*/
  } else {
    is_const = true;
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
      ObExprResType res_type;
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
                           true, false, is_called_in_sql))) {
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
  const bool is_paramlize = false;
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
      ObExprResType res_type;
      if (OB_FAIL(ObResolverUtils::resolve_const(element, stmt_type, *allocator, coll_type,
                                                 nchar_collation, timezone_info, obj_param, is_paramlize,
                                                 literal_prefix, length_semantics,
                                                 static_cast<ObCollationType>(server_collation),
                                                 &parents_expr_info,
                                                 session_info->get_sql_mode(),
                                                 enable_decimal_int,
                                                 compat_type,
                                                 enable_mysql_compatible_dates,
                                                 session_info->get_local_ob_enable_plan_cache(),
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
                            type_ctx, true, false, is_called_in_sql))) {
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

}  // namespace sql
}  // namespace oceanbase
