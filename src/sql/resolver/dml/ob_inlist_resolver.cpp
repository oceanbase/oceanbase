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
  ObSEArray<ObExprResType, 4> res_types;
  if (OB_UNLIKELY(row_cnt <= 0 || column_cnt <= 0) || OB_ISNULL(cur_resolver_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got NULL ptr", K(ret), KP(list_node), KP(cur_resolver_));
  } else {
    ObResolverParams &params = cur_resolver_->params_;
    if (OB_FAIL(resolve_values_table_from_inlist(list_node, column_cnt, row_cnt,
                                                 inlist_info.is_question_mark_, params.param_list_,
                                                 params.session_info_, params.allocator_, res_types,
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
                                                          res_types,
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
                                                const ParamStore *param_store,
                                                ObSQLSessionInfo *session_info,
                                                ObIAllocator *allocator,
                                                ObIArray<ObExprResType> &res_types,
                                                ObValuesTableDef *&table_def)
{
  int ret = OB_SUCCESS;
  char *table_buf = NULL;
  ObValuesTableDef::TableAccessType access_type = is_question_mark ?
                                      ObValuesTableDef::ACCESS_PARAM : ObValuesTableDef::ACCESS_OBJ;
  if (OB_ISNULL(allocator) || OB_ISNULL(session_info) || OB_ISNULL(in_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected NULL ptr", K(ret));
  } else if (OB_ISNULL(table_buf = static_cast<char*>(allocator->alloc(sizeof(ObValuesTableDef))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("sub_query or table_buf is null", K(ret), KP(table_buf));
  } else {
    table_def = new (table_buf) ObValuesTableDef();
    table_def->column_cnt_ = column_cnt;
    table_def->row_cnt_ = row_cnt;
    table_def->access_type_ = access_type;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(in_list->num_child_ != row_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected param", K(ret), K(row_cnt), K(in_list->num_child_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; i++) {
      const ParseNode *node = in_list->children_[i];
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got unexpected ptr", K(ret), KP(node), K(i));
      } else if (column_cnt > 1 &&
                 OB_UNLIKELY(T_EXPR_LIST != node->type_ || node->num_child_ != column_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got unexpected param", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < column_cnt; j++) {
          const ParseNode *element = column_cnt == 1 ? node : node->children_[j];
          if (ObValuesTableDef::ACCESS_PARAM == access_type) {
            if (OB_FAIL(get_questionmark_node_params(element, param_store, *session_info,
                                                     *allocator, j, i, res_types))) {
              LOG_WARN("failed to get const node", K(ret));
            } else {
              if (i == 0 && j == 0) {
                table_def->start_param_idx_ = element->value_;
              }
              if (i == row_cnt - 1 && j == column_cnt - 1) {
                table_def->end_param_idx_ = element->value_;
              }
            }
          } else if (ObValuesTableDef::ACCESS_OBJ == access_type) {
            if (OB_FAIL(get_const_node_params(element, *session_info, *allocator, j, i, res_types,
                                              table_def->access_objs_))) {
              LOG_WARN("failed to get const node", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObInListResolver::get_questionmark_node_params(const ParseNode *node,
                                                   const ParamStore *param_store,
                                                   ObSQLSessionInfo &session_info,
                                                   ObIAllocator &allocator,
                                                   const int64_t column_idx,
                                                   const int64_t row_idx,
                                                   ObIArray<ObExprResType> &res_types)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected params", K(ret));
  } else if (OB_UNLIKELY(row_idx == 0 && res_types.count() != column_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_types should contain next column", K(ret), K(column_idx));
  } else if (OB_UNLIKELY(row_idx > 0 && res_types.count() <= column_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_types.count should large than column idx", K(ret), K(column_idx));
  } else if (T_QUESTIONMARK == node->type_) {
    if (OB_ISNULL(param_store) ||
        OB_UNLIKELY(node->value_ < 0 || node->value_ >= param_store->count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("got unexpected params", K(ret), K(node->value_));
    } else {
      ObExprResType res_type;
      const ObObjParam &obj_param = param_store->at(node->value_);
      res_type.set_meta(obj_param.get_param_meta());
      res_type.set_accuracy(obj_param.get_accuracy());
      res_type.set_result_flag(obj_param.get_result_flag());
      if (row_idx == 0) {
        if (OB_FAIL(res_types.push_back(res_type))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else if (!ObSQLUtils::is_same_type(res_type, res_types.at(column_idx))) {
        // is not same ObExprResType, than compute a new one
        ObExprResType new_res_type;
        ObExprVersion dummy_op(allocator);
        ObCollationType coll_type = CS_TYPE_INVALID;
        ObSEArray<ObExprResType, 2> tmp_res_types;
        if (OB_FAIL(tmp_res_types.push_back(res_types.at(column_idx)))) {
          LOG_WARN("failed to push back res type", K(ret));
        } else if (OB_FAIL(tmp_res_types.push_back(res_type))) {
          LOG_WARN("failed to push back res type", K(ret));
        } else if (OB_FAIL(session_info.get_collation_connection(coll_type))) {
          LOG_WARN("fail to get collation_connection", K(ret));
        } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(new_res_type,
                          &tmp_res_types.at(0), tmp_res_types.count(), coll_type, false,
                          session_info.get_actual_nls_length_semantics()))) {
          LOG_WARN("failed to aggregate result type for merge", K(ret));
        } else {
          res_types.at(column_idx) = new_res_type;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node should be datatype or question mark", K(ret), K(node->type_));
  }
  return ret;
}

int ObInListResolver::get_const_node_params(const ParseNode *node,
                                            ObSQLSessionInfo &session_info,
                                            ObIAllocator &allocator,
                                            const int64_t column_idx,
                                            const int64_t row_idx,
                                            ObIArray<ObExprResType> &res_types,
                                            ObIArray<ObObj> &obj_array)
{
  int ret = OB_SUCCESS;
  ObObjParam obj_param;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got unexpected params", K(ret));
  } else if (OB_UNLIKELY(row_idx == 0 && res_types.count() != column_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_types should contain next column", K(ret), K(column_idx));
  } else if (OB_UNLIKELY(row_idx > 0 && res_types.count() <= column_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_types.count should large than column idx", K(ret), K(column_idx));
  } else if (IS_DATATYPE_OP(node->type_)) {
    bool is_oracle_mode = lib::is_oracle_mode();
    const bool is_paramlize = false;
    ObCollationType collation_connection = CS_TYPE_INVALID;
    int64_t server_collation = CS_TYPE_INVALID;
    ObExprInfo parents_expr_info;
    ObString literal_prefix;
    if (OB_FAIL(session_info.get_collation_connection(collation_connection))) {
      LOG_WARN("fail to get collation_connection", K(ret));
    } else if (is_oracle_mode && OB_FAIL(session_info.get_sys_variable(
                                              share::SYS_VAR_COLLATION_SERVER, server_collation))) {
      LOG_WARN("get sys variables failed", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_const(node,
                       is_oracle_mode ? session_info.get_stmt_type() : stmt::T_NONE, allocator,
                       collation_connection, session_info.get_nls_collation_nation(),
                       session_info.get_timezone_info(), obj_param, is_paramlize, literal_prefix,
                       session_info.get_actual_nls_length_semantics(),
                       static_cast<ObCollationType>(server_collation), &parents_expr_info,
                       session_info.get_sql_mode(),
                       nullptr != cur_resolver_->params_.secondary_namespace_))) {
      LOG_WARN("failed to resolve const", K(ret));
    } else if (OB_FAIL(obj_array.push_back(obj_param))) {
      LOG_WARN("failed to push back", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node should be datatype or question mark", K(ret), K(node->type_));
  }
  if (OB_SUCC(ret)) {
    ObExprResType res_type;
    res_type.set_meta(obj_param.get_param_meta());
    res_type.set_accuracy(obj_param.get_accuracy());
    res_type.set_result_flag(obj_param.get_result_flag());
    if (row_idx == 0) {
      if (OB_FAIL(res_types.push_back(res_type))) {
        LOG_WARN("failed to push back", K(ret));
      }
    } else if (!ObSQLUtils::is_same_type(res_type, res_types.at(column_idx))) {
      // is not same ObExprResType, than compute a new one
      ObExprResType new_res_type;
      ObExprVersion dummy_op(allocator);
      ObCollationType coll_type = CS_TYPE_INVALID;
      ObSEArray<ObExprResType, 2> tmp_res_types;
      if (OB_FAIL(tmp_res_types.push_back(res_types.at(column_idx)))) {
        LOG_WARN("failed to push back res type", K(ret));
      } else if (OB_FAIL(tmp_res_types.push_back(res_type))) {
        LOG_WARN("failed to push back res type", K(ret));
      } else if (OB_FAIL(session_info.get_collation_connection(coll_type))) {
        LOG_WARN("fail to get collation_connection", K(ret));
      } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(new_res_type,
                         &tmp_res_types.at(0), tmp_res_types.count(), coll_type, false,
                         session_info.get_actual_nls_length_semantics()))) {
        LOG_WARN("failed to aggregate result type for merge", K(ret));
      } else {
        res_types.at(column_idx) = new_res_type;
      }
    }
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
                                                         ObIArray<ObExprResType> &res_types,
                                                         ObQueryRefRawExpr *query_ref)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *subquery = NULL;
  TableItem *table_item = NULL;
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
    if (OB_FAIL(subquery->set_stmt_id())) {
      LOG_WARN("fail to set stmt id", K(ret));
    } else {
      query_ref->set_ref_stmt(subquery);
      query_ref->set_is_set(true);
    }
  }
  if (OB_SUCC(ret)) {
    ObString alias_name;
    if (OB_ISNULL(table_item = subquery->create_table_item(*allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create table item failed");
    } else if (OB_FAIL(subquery->generate_values_table_name(*allocator, alias_name))) {
      LOG_WARN("failed to generate func table name", K(ret));
    } else {
      table_item->table_id_ = cur_resolver_->generate_table_id();
      table_item->table_name_ = alias_name;
      table_item->alias_name_ = alias_name;
      table_item->type_ = TableItem::VALUES_TABLE;
      table_item->is_view_table_ = false;
      table_item->values_table_def_ = table_def;
      if (OB_FAIL(subquery->add_table_item(session_info, table_item))) {
        LOG_WARN("add table item failed", K(ret));
      } else if (OB_FAIL(subquery->add_from_item(table_item->table_id_))) {
        LOG_WARN("add from table failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(res_types.count() != column_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("got unexpected ptr", K(ret), K(column_cnt), K(res_types.count()));
    } else {
      query_ref->set_output_column(column_cnt);
      for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
        ObColumnRefRawExpr *column_expr = NULL;
        ObSqlString tmp_col_name;
        char *buf = NULL;
        if (OB_FAIL(expr_factory->create_raw_expr(T_REF_COLUMN, column_expr))) {
          LOG_WARN("create column ref raw expr failed", K(ret));
        } else if (OB_ISNULL(column_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(("value desc is null"), K(ret));
        } else if (OB_FAIL(column_expr->add_flag(IS_COLUMN))) {
          LOG_WARN("failed to add flag IS_COLUMN", K(ret));
        } else if (OB_FAIL(tmp_col_name.append_fmt("column_%ld", i))) {
          LOG_WARN("failed to append fmt", K(ret));
        } else if (OB_ISNULL(buf = static_cast<char*>(allocator->alloc(tmp_col_name.length())))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), K(buf));
        } else {
          column_expr->set_result_type(res_types.at(i));
          column_expr->set_ref_id(table_item->table_id_, i + OB_APP_MIN_COLUMN_ID);
          MEMCPY(buf, tmp_col_name.ptr(), tmp_col_name.length());
          ObString column_name(tmp_col_name.length(), buf);
          column_expr->set_column_attr(table_item->table_name_, column_name);
          ColumnItem column_item;
          column_item.expr_ = column_expr;
          column_item.table_id_ = column_expr->get_table_id();
          column_item.column_id_ = column_expr->get_column_id();
          column_item.column_name_ = column_expr->get_column_name();

          SelectItem select_item;
          select_item.alias_name_ = column_expr->get_column_name();
          select_item.expr_name_ = column_expr->get_column_name();
          select_item.is_real_alias_ = false;
          select_item.expr_ = column_expr;
          if (OB_FAIL(subquery->add_column_item(column_item))) {
            LOG_WARN("failed to add column item", K(ret));
          } else if (OB_FAIL(subquery->add_select_item(select_item))) {
            LOG_WARN("failed to add select item", K(ret));
          } else if (OB_FAIL(query_ref->add_column_type(column_expr->get_result_type()))) {
            LOG_WARN("add column type to subquery ref expr failed", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cur_resolver_->estimate_values_table_stats(*table_def))) {
      LOG_WARN("failed to estimate values table stats", K(ret));
    } else if (OB_FAIL(cur_resolver_->get_stmt()->add_subquery_ref(query_ref))) {
      LOG_WARN("failed to add subquery reference", K(ret));
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
                                                  const bool is_prepare_protocol,
                                                  const bool is_in_pl,
                                                  const ObSQLSessionInfo *session_info,
                                                  const ParamStore *param_store,
                                                  const ObStmt *stmt,
                                                  bool &is_question_mark,
                                                  bool &is_enable)
{
  int ret = OB_SUCCESS;
  is_enable = false;
  int64_t threshold = INT64_MAX;
  // 1. check basic requests
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(session_info));
  } else if (T_WHERE_SCOPE != scope || !is_root_condition || T_OP_IN != op_type ||
             T_EXPR_LIST != in_list.type_ || is_need_print || is_prepare_protocol) {
    LOG_TRACE("no need rewrite inlist", K(is_root_condition), K(scope), K(in_list.type_),
              K(op_type), K(is_need_print), K(is_prepare_protocol));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_2_0) {
    LOG_TRACE("current version less than 4_2_2_0");
  } else {
    threshold = session_info->get_inlist_rewrite_threshold();
    if (stmt != NULL && stmt->get_query_ctx() != NULL) {
      const ObGlobalHint &global_hint = stmt->get_query_ctx()->get_global_hint();
      if (OB_FAIL(global_hint.opt_params_.get_integer_opt_param(
                                            ObOptParamHint::INLIST_REWRITE_THRESHOLD, threshold))) {
        LOG_WARN("failed to get integer opt param", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_enable = in_list.num_child_ >= threshold;
      LOG_TRACE("check rewrite inlist threshold", K(threshold), K(in_list.num_child_));
    }
  }
  // 2. check same node type requests
  if (OB_SUCC(ret) && is_enable) {
    const int64_t row_cnt = in_list.num_child_;
    const int64_t column_cnt = T_OP_ROW == left_expr.get_expr_type() ? left_expr.get_param_count() :
                                                                       1;
    ObSEArray<DistinctObjMeta, 4> param_types;
    ObCollationType connect_collation = CS_TYPE_INVALID;
    ObCollationType nchar_collation = session_info->get_nls_collation_nation();
    int64_t server_collation = CS_TYPE_INVALID;
    if (OB_UNLIKELY(row_cnt <= 0 || column_cnt <= 0) || OB_ISNULL(in_list.children_[0]) ||
        OB_UNLIKELY(column_cnt > 1 && in_list.children_[0]->num_child_ != column_cnt)) {
      is_enable = false;  /* delay return error code */
    } else if (OB_FAIL(session_info->get_collation_connection(connect_collation))) {
      LOG_WARN("fail to get collation_connection", K(ret));
    } else if (lib::is_oracle_mode() && OB_FAIL(session_info->get_sys_variable(
                                              share::SYS_VAR_COLLATION_SERVER, server_collation))) {
      LOG_WARN("get sys variables failed", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && is_enable && j < column_cnt; j++) {
        const ParseNode *node = column_cnt == 1 ? in_list.children_[0] :
                                                  in_list.children_[0]->children_[j];
        DistinctObjMeta param_type;
        // know this inlist is question mark or const
        if (j == 0) {
          if (OB_ISNULL(node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected param", K(ret));
          } else {
            is_question_mark = T_QUESTIONMARK == node->type_;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(get_const_node_types(node, param_store, is_question_mark,
                           connect_collation, nchar_collation, static_cast<ObCollationType>(server_collation),
                           param_type, is_enable))) {
          LOG_WARN("failed to got const node types", K(ret));
        } else if (is_enable) {
          if (ob_is_enum_or_set_type(param_type.obj_type_) ||
              is_lob_locator(param_type.obj_type_)) {
            is_enable = false;
          } else if (OB_FAIL(param_types.push_back(param_type))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      }

      for (int64_t i = 1; OB_SUCC(ret) && is_enable && i < row_cnt; i++) {
        for (int64_t j = 0; OB_SUCC(ret) && is_enable && j < column_cnt; j++) {
          if (OB_UNLIKELY(column_cnt > 1 && in_list.children_[i]->num_child_ != column_cnt)) {
            is_enable = false; /* delay return error code */
          } else {
            const ParseNode *node = column_cnt == 1 ? in_list.children_[i] :
                                                      in_list.children_[i]->children_[j];
            DistinctObjMeta param_type;
            if (OB_FAIL(get_const_node_types(node, param_store, is_question_mark,
                        connect_collation, nchar_collation, static_cast<ObCollationType>(server_collation),
                        param_type, is_enable))) {
              LOG_WARN("failed to got const node types", K(ret));
            } else if (is_enable && param_type == param_types.at(j)) {
              /*is same type*/
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
                                           DistinctObjMeta &param_type,
                                           bool &is_const)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (T_QUESTIONMARK == node->type_ && is_question_mark) {
    if (OB_ISNULL(param_store) ||
        OB_UNLIKELY(node->value_ < 0 || node->value_ >= param_store->count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(ret));
    } else {
      param_type.obj_type_ = param_store->at(node->value_).get_param_meta().get_type();
      param_type.coll_type_ = param_store->at(node->value_).get_param_meta().get_collation_type();
      param_type.coll_level_ = param_store->at(node->value_).get_param_meta().get_collation_level();
      is_const = true;
    }
  } else if (IS_DATATYPE_OP(node->type_) && !is_question_mark) {
    if (OB_FAIL(get_datatype_node_equal_objtype(*node, connect_collation, nchar_collation,
                                                server_collation, param_type))) {
      ret = OB_SUCCESS;
      is_const = false;
    } else {
      is_const = true;
    }
  } else {
    is_const = false;
  }
  return ret;
}

int ObInListResolver::get_datatype_node_equal_objtype(const ParseNode &node,
                                                      const ObCollationType connect_collation,
                                                      const ObCollationType nchar_collation,
                                                      const ObCollationType server_collation,
                                                      DistinctObjMeta &obj_meta)
{
  int ret = OB_SUCCESS;
  ObCollationType coll_type = CS_TYPE_INVALID;
  ObCollationLevel coll_level = CS_LEVEL_INVALID;
  ObObjType obj_type = ObMaxType;
  if (IS_DATATYPE_OP(node.type_)) {
    if (T_VARCHAR == node.type_ ||
        T_CHAR == node.type_ ||
        T_NVARCHAR2 == node.type_ ||
        T_NCHAR == node.type_) {
      bool is_nchar = T_NVARCHAR2 == node.type_ || T_NCHAR == node.type_;
      obj_type = lib::is_mysql_mode() && is_nchar ? ObVarcharType :
                                                    static_cast<ObObjType>(node.type_);
      coll_level = CS_LEVEL_COERCIBLE;
      if (OB_UNLIKELY(node.str_len_ > OB_MAX_LONGTEXT_LENGTH)) {
        ret = OB_ERR_INVALID_INPUT_ARGUMENT;
      } else if (lib::is_oracle_mode()) {
        coll_type = is_nchar ? nchar_collation : server_collation;
        if (node.str_len_ == 0) {
          obj_type = is_nchar ? ObNCharType : ObCharType;
        }
      } else {
        if (0 == node.num_child_) {
          coll_type = is_nchar ? CS_TYPE_UTF8MB4_GENERAL_CI : connect_collation;
        } else if (NULL != node.children_[0] && T_CHARSET == node.children_[0]->type_) {
          ObString charset(node.children_[0]->str_len_, node.children_[0]->str_value_);
          ObCharsetType charset_type = ObCharset::charset_type(charset.trim());
          coll_type = ObCharset::get_default_collation(charset_type);
        } else {
          coll_type = connect_collation;
        }
      }
    } else if (T_IEEE754_NAN == node.type_ || T_IEEE754_INFINITE == node.type_) {
      obj_type = ObDoubleType;
      coll_type = CS_TYPE_BINARY;
      coll_level = CS_LEVEL_NUMERIC;
    } else if (T_BOOL == node.type_) {
      obj_type = ObTinyIntType;
      coll_type = CS_TYPE_BINARY;
      coll_level = CS_LEVEL_NUMERIC;
    } else {
      obj_type = static_cast<ObObjType>(node.type_);
      coll_type = CS_TYPE_BINARY;
      coll_level = CS_LEVEL_NUMERIC;
    }
  }

  if (ObMaxType == obj_type || CS_TYPE_INVALID == coll_type || CS_LEVEL_INVALID == coll_level) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resolve node type failed.", K(ret), K(obj_type), K(coll_type), K(coll_level));
  } else {
    obj_meta.obj_type_ = obj_type;
    obj_meta.coll_type_ = coll_type;
    obj_meta.coll_level_ = coll_level;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
