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

#include "sql/resolver/dml/ob_transpose_resolver.h"
#include "sql/engine/expr/ob_expr_version.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/rewrite/ob_transform_pre_process.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase {

using namespace common;

namespace sql {

int TransposeDef::assign(const TransposeDef &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    //do nothing
  } else {
    alias_name_ = other.alias_name_;
    orig_table_item_ = other.orig_table_item_;
  }
  return ret;
}

int TransposeDef::deep_copy(ObIRawExprCopier &expr_copier,
                             const TransposeDef &other)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("transpose item should not deep copy", K(ret));
  return ret;
}

int PivotDef::AggrPair::assign(const PivotDef::AggrPair &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    //do nothing
  } else {
    expr_ = other.expr_;
    alias_name_ = other.alias_name_;
  }
  return ret;
}

int PivotDef::InPair::assign(const PivotDef::InPair &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    //do nothing
  } else if (OB_FAIL(const_exprs_.assign(other.const_exprs_))) {
    LOG_WARN("assign searray failed", K(other), K(ret));
  } else {
    name_ = other.name_;
  }
  return ret;
}

int PivotDef::assign(const PivotDef &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TransposeDef::assign(other))) {
    LOG_WARN("failed to assign transpose def", K(ret));
  } else if (OB_FAIL(group_columns_.assign(other.group_columns_)) ||
             OB_FAIL(group_column_names_.assign(other.group_column_names_)) ||
             OB_FAIL(for_columns_.assign(other.for_columns_)) ||
             OB_FAIL(for_column_names_.assign(other.for_column_names_)) ||
             OB_FAIL(aggr_pairs_.assign(other.aggr_pairs_)) ||
             OB_FAIL(in_pairs_.assign(other.in_pairs_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

int UnpivotDef::InPair::assign(const UnpivotDef::InPair &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    //do nothing
  } else if (OB_FAIL(const_exprs_.assign(other.const_exprs_))) {
    LOG_WARN("assign searray failed", K(other), K(ret));
  } else if (OB_FAIL(column_names_.assign(other.column_names_))) {
    LOG_WARN("assign searray failed", K(other), K(ret));
  } else if (OB_FAIL(column_exprs_.assign(other.column_exprs_))) {
    LOG_WARN("assign searray failed", K(other), K(ret));
  }
  return ret;
}

int UnpivotDef::assign(const UnpivotDef &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TransposeDef::assign(other))) {
    LOG_WARN("failed to assign transpose def", K(ret));
  } else if (OB_FAIL(orig_columns_.assign(other.orig_columns_)) ||
             OB_FAIL(orig_column_names_.assign(other.orig_column_names_)) ||
             OB_FAIL(label_columns_.assign(other.label_columns_)) ||
             OB_FAIL(value_columns_.assign(other.value_columns_)) ||
             OB_FAIL(in_pairs_.assign(other.in_pairs_))) {
    LOG_WARN("failed to assign", K(ret));
  } else {
    is_include_null_ = other.is_include_null_;
  }
  return ret;
}

// transpose_table_node
//   |--table_node
//   |--transpose_node
//      |--alias
// resolve table_node
// resolve transpose_table
int ObTransposeResolver::resolve(const ParseNode &parse_tree) {
  int ret = OB_SUCCESS;
  void *table_item_ptr = NULL;
  bool tmp_has_same_table_name = false;
  ParseNode *origin_table = NULL;
  ParseNode *transpose_node = NULL;
  ObResolverParams *params = NULL;
  ObDMLStmt *stmt = NULL;
  TableItem *orig_table_item = NULL;
  if (T_TRANSPOSE_TABLE != parse_tree.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid transpose parse tree type", K(ret));
  } else if (OB_ISNULL(cur_resolver_) || OB_ISNULL(stmt = cur_resolver_->get_stmt()) ||
             OB_ISNULL(origin_table = parse_tree.children_[0]) ||
             OB_ISNULL(transpose_node = parse_tree.children_[1]) ||
             FALSE_IT(params = &cur_resolver_->params_) ||
             OB_ISNULL(params->allocator_) ||
             OB_ISNULL(params->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (FALSE_IT(tmp_has_same_table_name = params->have_same_table_name_)) {
  } else if (OB_FAIL(cur_resolver_->resolve_table(*origin_table, orig_table_item))) {
    LOG_WARN("failed to resolve table", K(ret));
  } else if (FALSE_IT(params->have_same_table_name_ = tmp_has_same_table_name)) {
  } else {
    TransposeDef *trans_def = NULL;
    ObColumnNamespaceChecker &column_namespace_checker = cur_resolver_->get_column_namespace_checker();
    ObSEArray<ObString, 16> columns_in_aggrs;
    if (OB_ISNULL(orig_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_item or stmt is unexpected", KP(orig_table_item), K(ret));
    } else if (FALSE_IT(column_namespace_checker.set_transpose_origin_table(*orig_table_item))) {
    } else if (OB_FAIL(resolve_transpose_clause(*transpose_node, *orig_table_item, trans_def, columns_in_aggrs))) {
      LOG_WARN("resolve transpose clause failed", K(ret));
    } else if (OB_ISNULL(trans_def)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (trans_def->is_unpivot() &&
               OB_FAIL(try_add_cast_to_unpivot(static_cast<UnpivotDef &>(*trans_def)))) {
      LOG_WARN("fail to add cast to unpivot", KPC(orig_table_item), K(ret));
    } else if (OB_FAIL(get_old_or_group_column(columns_in_aggrs, *orig_table_item, *trans_def))) {
      LOG_WARN("fail to get old or group column", KPC(orig_table_item), K(ret));
    } else if (OB_FAIL(transform_to_view(*orig_table_item, *trans_def, table_item_))) {
      LOG_WARN("failed to transform to view", K(ret));
    } else {
      cur_resolver_->get_column_namespace_checker().clear_transpose();
      trans_def->orig_table_item_ = orig_table_item;
      // for print stmt
      table_item_->transpose_table_def_ = trans_def;
    }
  }
  return ret;
}

int ObTransposeResolver::resolve_transpose_clause(
    const ParseNode &transpose_node,
    TableItem &orig_table_item,
    TransposeDef *&trans_def,
    ObIArray<ObString> &columns_in_aggrs)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(cur_resolver_)
      || OB_ISNULL(cur_resolver_->get_stmt())
      || OB_UNLIKELY(transpose_node.type_ != T_PIVOT && transpose_node.type_ != T_UNPIVOT)
      || OB_UNLIKELY(transpose_node.num_child_ != 4)
      || OB_ISNULL(transpose_node.children_)
      || OB_ISNULL(transpose_node.children_[0])
      || OB_ISNULL(transpose_node.children_[1])
      || OB_ISNULL(transpose_node.children_[2])
      || OB_ISNULL(transpose_node.children_[3])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transpose_node is unexpected", K(transpose_node.type_), K(transpose_node.num_child_),
             KP(transpose_node.children_), K(ret));
  } else if (T_PIVOT == transpose_node.type_) {
    if (OB_ISNULL(ptr = cur_resolver_->params_.allocator_->alloc(sizeof(PivotDef)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create unpivot info failed");
    } else {
      PivotDef * pivot_def = new (ptr) PivotDef();
      // resolve pivot clause
      if (OB_FAIL(resolve_pivot_clause(transpose_node, orig_table_item, *pivot_def, columns_in_aggrs))) {
        LOG_WARN("failed to resolve pivot clause", K(ret));
      } else {
        trans_def = pivot_def;
      }
    }
  } else {
    if (OB_ISNULL(ptr = cur_resolver_->params_.allocator_->alloc(sizeof(UnpivotDef)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("create unpivot info failed");
    } else {
      UnpivotDef * unpivot_def = new (ptr) UnpivotDef();
      // resolve unpivot clause
      if (OB_FAIL(resolve_unpivot_clause(transpose_node, orig_table_item, *unpivot_def))) {
        LOG_WARN("failed to resolve pivot clause", K(ret));
      } else {
        trans_def = unpivot_def;
      }
    }
  }
  //alias
  if (OB_SUCC(ret)) {
    const ParseNode &alias = *transpose_node.children_[3];
    if (alias.str_len_ > 0 && alias.str_value_ != NULL) {
      trans_def->alias_name_.assign_ptr(alias.str_value_, alias.str_len_);
    } else if (OB_FAIL(cur_resolver_->get_stmt()->generate_anonymous_view_name(
                    *cur_resolver_->params_.allocator_, trans_def->alias_name_))) {
      LOG_WARN("failed to generate anonymous view name", K(ret));
    }
  }
  LOG_DEBUG("finish resolve_transpose_clause", K(trans_def), K(columns_in_aggrs), K(ret));
  return ret;
}

int ObTransposeResolver::resolve_pivot_clause(
    const ParseNode &transpose_node,
    TableItem &orig_table_item,
    PivotDef &pivot_def,
    common::ObIArray<common::ObString> &columns_in_aggrs)
{
  int ret = OB_SUCCESS;
  //pivot aggr
  const ParseNode &aggr_node = *transpose_node.children_[0];
  if (OB_UNLIKELY(aggr_node.type_ != T_PIVOT_AGGR_LIST)
      || OB_UNLIKELY(aggr_node.num_child_ <= 0)
      || OB_ISNULL(aggr_node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr_node is unexpected", K(aggr_node.type_), K(aggr_node.num_child_),
              KP(aggr_node.children_), K(ret));
  } else {
    ObArray<ObQualifiedName> qualified_name_in_aggr;
    for (int64_t i = 0; OB_SUCC(ret) && i < aggr_node.num_child_; ++i) {
      const ParseNode *tmp_node = aggr_node.children_[i];
      const ParseNode *alias_node = NULL;
      PivotDef::AggrPair aggr_pair;
      qualified_name_in_aggr.reuse();
      if (OB_ISNULL(tmp_node)
          || OB_UNLIKELY(tmp_node->type_ != T_PIVOT_AGGR)
          || OB_UNLIKELY(tmp_node->num_child_ != 2)
          || OB_ISNULL(tmp_node->children_)
          || OB_ISNULL(tmp_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tmp_node is unexpected", KP(tmp_node), K(ret));
      } else if (OB_FAIL(cur_resolver_->resolve_sql_expr(*tmp_node->children_[0],
                                                          aggr_pair.expr_,
                                                          &qualified_name_in_aggr))) {
        LOG_WARN("fail to resolve_sql_expr", K(ret));
      } else if (OB_FAIL(check_pivot_aggr_expr(aggr_pair.expr_))) {
        LOG_WARN("fail to check_aggr_expr", K(ret));
      } else if (NULL != (alias_node = tmp_node->children_[1])
                  && FALSE_IT(aggr_pair.alias_name_.assign_ptr(alias_node->str_value_,
                                                              alias_node->str_len_))) {
      } else if (OB_FAIL(pivot_def.aggr_pairs_.push_back(aggr_pair))) {
        LOG_WARN("fail to push_back aggr_pair", K(aggr_pair), K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < qualified_name_in_aggr.count(); ++j) {
          ObString &column_name = qualified_name_in_aggr.at(j).col_name_;
          if (!has_exist_in_array(columns_in_aggrs, column_name)) {
            if (OB_FAIL(columns_in_aggrs.push_back(column_name))) {
              LOG_WARN("fail to push_back column_name", K(column_name), K(ret));
            }
          }
        }
      }
    }
  }

  //pivot for
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_columns(*transpose_node.children_[1],
                                orig_table_item,
                                pivot_def.for_column_names_,
                                pivot_def.for_columns_))) {
      LOG_WARN("fail to resolve transpose columns", K(ret));
    }
  }

  //pivot in
  if (OB_SUCC(ret)) {
    const ParseNode &in_node = *transpose_node.children_[2];
    if (OB_UNLIKELY(in_node.type_ != T_PIVOT_IN_LIST)
        || OB_UNLIKELY(in_node.num_child_ <= 0)
        || OB_ISNULL(in_node.children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in_node is unexpected", K(in_node.type_), K(in_node.num_child_),
                KP(in_node.children_), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < in_node.num_child_; ++i) {
        const ParseNode *column_node = in_node.children_[i];
        const ParseNode *alias_node = NULL;
        PivotDef::InPair in_pair;
        if (OB_ISNULL(column_node)
            || OB_UNLIKELY(column_node->type_ != T_PIVOT_IN)
            || OB_UNLIKELY(column_node->num_child_ != 2)
            || OB_ISNULL(column_node->children_)
            || OB_ISNULL(column_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_node is unexpected", KP(column_node), K(ret));
        } else if (OB_FAIL(resolve_const_exprs(*column_node->children_[0],
                                                in_pair.const_exprs_))) {
          LOG_WARN("fail to resolve_const_exprs", K(ret));
        } else if (OB_UNLIKELY(in_pair.const_exprs_.count() != pivot_def.for_column_names_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("in expr number is not equal for columns", K(in_pair.const_exprs_),
                    K(pivot_def.for_column_names_), K(ret));
        } else if (NULL != (alias_node = column_node->children_[1])) {
          if (OB_UNLIKELY(alias_node->str_len_ > OB_MAX_COLUMN_NAME_LENGTH)) {
            ret = OB_ERR_TOO_LONG_IDENT;
            LOG_WARN("alias name for pivot is too long", K(ret),
                      K(ObString(alias_node->str_len_, alias_node->str_value_)));
          } else {
            in_pair.name_.assign_ptr(alias_node->str_value_, alias_node->str_len_);
          }
        } else /* no alias, use const expr name as in_pair's name */ {
          if (OB_FAIL(get_combine_name(in_pair.const_exprs_, in_pair.name_))) {
            LOG_WARN("failed to get combine name", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(pivot_def.in_pairs_.push_back(in_pair))) {
          LOG_WARN("fail to push_back in_pair", K(in_pair), K(ret));
        }
      }//end of for in node
    }
  }
  return ret;
}

int ObTransposeResolver::resolve_unpivot_clause(const ParseNode &transpose_node,
                                                TableItem &orig_table_item,
                                                UnpivotDef &unpivot_def)
{
  int ret = OB_SUCCESS;
  unpivot_def.set_include_nulls(2 == transpose_node.value_);
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unpivot is not supported during updating", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "unpivot is not supported during updating");

  //unpivot column (value column)
  } else if (OB_FAIL(resolve_column_names(*transpose_node.children_[0],
                                    unpivot_def.value_columns_))) {
    LOG_WARN("fail to resolve_transpose_columns", K(ret));

  //unpivot for
  } else if (OB_FAIL(resolve_column_names(*transpose_node.children_[1],
                                          unpivot_def.label_columns_))) {
    LOG_WARN("fail to resolve_transpose_columns", K(ret));

  //unpivot in
  } else {
    const ParseNode &in_node = *transpose_node.children_[2];
    if (OB_UNLIKELY(in_node.type_ != T_UNPIVOT_IN_LIST)
        || OB_UNLIKELY(in_node.num_child_ <= 0)
        || OB_ISNULL(in_node.children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in_node is unexpected", K(in_node.type_), K(in_node.num_child_),
                KP(in_node.children_), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < in_node.num_child_; ++i) {
        const ParseNode *column_node = in_node.children_[i];
        UnpivotDef::InPair in_pair;
        if (OB_ISNULL(column_node)
            || OB_UNLIKELY(column_node->type_ != T_UNPIVOT_IN)
            || OB_UNLIKELY(column_node->num_child_ != 2)
            || OB_ISNULL(column_node->children_)
            || OB_ISNULL(column_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_node is unexpectedl", KP(column_node), K(ret));
        } else if (OB_FAIL(resolve_columns(*column_node->children_[0],
                                            orig_table_item,
                                            in_pair.column_names_,
                                            in_pair.column_exprs_))) {
          LOG_WARN("fail to resolve transpose columns", K(ret));
        } else if (OB_UNLIKELY(in_pair.column_names_.count() != unpivot_def.value_columns_.count())) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("unpivot column count is not match for in column count", K(unpivot_def),
                    K(in_pair), K(ret));
        } else if (NULL != column_node->children_[1]) {
          if (OB_FAIL(resolve_const_exprs(*column_node->children_[1], in_pair.const_exprs_))) {
            LOG_WARN("fail to resolve_const_exprs", K(ret));
          } else if (OB_UNLIKELY(in_pair.const_exprs_.count() != unpivot_def.label_columns_.count())){
            ret = OB_ERR_PARSER_SYNTAX;
            LOG_WARN("in column count is not match in literal count", K(unpivot_def), K(in_pair), K(ret));
          }
        } else {
          // 根据in_pair的名字拼凑一个name作为const_expr
          ObString name;
          ObConstRawExpr *const_expr = NULL;
          if (OB_FAIL(get_combine_name(in_pair.column_names_, name))) {
            LOG_WARN("failed to get combine name", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(
                          *cur_resolver_->params_.expr_factory_, ObVarcharType,
                          name,
                          cur_resolver_->params_.session_info_->get_nls_collation(),
                          const_expr))) {
            LOG_WARN("failed to build const string expr", K(ret));
          } else if (OB_ISNULL(const_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("const expr is null", K(ret));
          } else {
            const_expr->set_length(name.length());
            const_expr->set_length_semantics(cur_resolver_->params_.session_info_->get_local_nls_length_semantics());
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < unpivot_def.label_columns_.count(); ++i) {
            if (OB_FAIL(in_pair.const_exprs_.push_back(const_expr))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(unpivot_def.in_pairs_.push_back(in_pair))) {
          LOG_WARN("fail to push_back in_pair", K(in_pair), K(ret));
        }
      }//end of for in node
    }
  }//end of in
  return ret;
}

int ObTransposeResolver::check_pivot_aggr_expr(ObRawExpr *expr) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr expr_ is unexpected", KPC(expr), K(ret));
  } else if (!expr->is_aggr_expr()) {
    ret = OB_ERR_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION;
    LOG_WARN("expect aggregate function inside pivot operation", KPC(expr), K(ret));
  } else if (static_cast<ObAggFunRawExpr *>(expr)->get_real_param_count() > 2 ||
             static_cast<ObAggFunRawExpr *>(expr)->get_order_items().count() > 0) {
    ret = OB_ERR_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION;
    LOG_WARN("not support aggregation type in pivot", K(ret), K(expr->get_expr_type()));
  } else {
    // white list
    switch (expr->get_expr_type()) {
      case T_FUN_COUNT:
      case T_FUN_MAX:
      case T_FUN_MIN:
      case T_FUN_AVG:
      case T_FUN_SUM:
      case T_FUN_COUNT_SUM:
      case T_FUN_APPROX_COUNT_DISTINCT:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
      case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE:
      case T_FUN_GROUP_CONCAT:
      case T_FUN_MEDIAN:
      case T_FUN_JSON_ARRAYAGG:
      case T_FUN_ORA_JSON_ARRAYAGG:
      case T_FUN_JSON_OBJECTAGG:
      case T_FUN_ORA_JSON_OBJECTAGG:
      case T_FUN_ORA_XMLAGG:
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

int ObTransposeResolver::resolve_columns(const ParseNode &column_node,
                                         TableItem &table_item,
                                         ObIArray<ObString> &column_names,
                                         ObIArray<ObRawExpr *> &columns)
{
  int ret = OB_SUCCESS;
  ColumnItem *column_item = NULL;
  columns.reuse();
  if (OB_FAIL(resolve_column_names(column_node, column_names))) {
    LOG_WARN("failed to resolve column names", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
    if (OB_FAIL(resolve_table_column_item(table_item, column_names.at(i), column_item))) {
      LOG_WARN("failed to resolve single table column item", K(ret));
    } else if (OB_ISNULL(column_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if(OB_FAIL(columns.push_back(column_item->get_expr()))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObTransposeResolver::resolve_column_names(const ParseNode &column_node, ObIArray<ObString> &columns)
{
  int ret = OB_SUCCESS;
  if (column_node.type_ == T_COLUMN_LIST) {
    if (OB_UNLIKELY(column_node.num_child_ <= 0)
        || OB_ISNULL(column_node.children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_node is unexpected", K(column_node.num_child_),
               KP(column_node.children_), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_node.num_child_; ++i) {
        const ParseNode *tmp_node = column_node.children_[i];
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

int ObTransposeResolver::resolve_const_exprs(
    const ParseNode &expr_node,
    ObIArray<ObRawExpr *> &const_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *const_expr = NULL;
  if (expr_node.type_ == T_EXPR_LIST) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_node.num_child_; ++i) {
      const ParseNode *tmp_node = expr_node.children_[i];
      const_expr = NULL;
      if (OB_ISNULL(tmp_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tmp_node is unexpected", KP(tmp_node), K(ret));
      } else if (OB_FAIL(cur_resolver_->resolve_sql_expr(*tmp_node, const_expr))) {
        LOG_WARN("fail to resolve_sql_expr", K(ret));
      } else if (OB_UNLIKELY(!const_expr->is_const_expr())
                 || OB_UNLIKELY(const_expr->has_flag(ObExprInfoFlag::CNT_CUR_TIME))) {
        ret = OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES;
        LOG_WARN("non-constant expression is not allowed for pivot|unpivot values",
                 KPC(const_expr), K(ret));
      } else if (OB_FAIL(const_exprs.push_back(const_expr))) {
        LOG_WARN("fail to push_back const_expr", KPC(const_expr), K(ret));
      }
    }
  } else {
    if (OB_FAIL(cur_resolver_->resolve_sql_expr(expr_node, const_expr))) {
      LOG_WARN("fail to resolve_sql_expr", K(ret));
    } else if (OB_UNLIKELY(!const_expr->is_const_expr())
               || OB_UNLIKELY(const_expr->has_flag(ObExprInfoFlag::CNT_CUR_TIME))) {
      ret = OB_ERR_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES;
      LOG_WARN("non-constant expression is not allowed for pivot|unpivot values",
               KPC(const_expr), K(ret));
    } else if (OB_FAIL(const_exprs.push_back(const_expr))) {
      LOG_WARN("fail to push_back const_expr", KPC(const_expr), K(ret));
    }
  }
  return ret;
}

int ObTransposeResolver::resolve_table_column_item(const TableItem &table_item,
                                                   ObString &column_name,
                                                   ColumnItem *&column_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_resolver_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table_item.is_joined_table()) {
    const JoinedTable &joined_table = static_cast<const JoinedTable &>(table_item);
    ColumnItem *left_column_item = NULL;
    ColumnItem *right_column_item = NULL;
    if (OB_ISNULL(joined_table.left_table_) || OB_ISNULL(joined_table.right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      if (OB_SUCC(ret) && OB_FAIL(SMART_CALL(resolve_table_column_item(
              *joined_table.left_table_, column_name, left_column_item)))) {
        if (ret == OB_ERR_BAD_FIELD_ERROR) {
          left_column_item = NULL;
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(SMART_CALL(resolve_table_column_item(
              *joined_table.right_table_, column_name, right_column_item)))) {
        if (ret == OB_ERR_BAD_FIELD_ERROR) {
          right_column_item = NULL;
          ret = OB_SUCCESS;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (left_column_item == NULL && right_column_item == NULL) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("not found column in joined table", K(ret), K(column_name));
      } else if (left_column_item != NULL && right_column_item != NULL) {
        ret = OB_NON_UNIQ_ERROR;
        LOG_WARN("column in joined tables is ambiguous", K(column_name));
      } else if (left_column_item != NULL) {
        column_item = left_column_item;
      } else {
        column_item = right_column_item;
      }
    }
  } else if (OB_FAIL(cur_resolver_->resolve_single_table_column_item(
                 table_item, column_name, false, column_item))) {
    LOG_WARN("failed to resolve single table column item", K(ret), K(table_item));
  }
  return ret;
}

int ObTransposeResolver::resolve_all_table_columns(const TableItem &table_item,
                                                   ObIArray<ColumnItem> &column_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_resolver_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(cur_resolver_), K(table_item));
  } else if (table_item.is_basic_table() || table_item.is_fake_cte_table() || table_item.is_link_table()) {
    if (OB_FAIL(cur_resolver_->resolve_all_basic_table_columns(table_item, false, &column_items))) {
      LOG_WARN("resolve all basic table columns failed", K(ret));
    }
  } else if (table_item.is_generated_table() || table_item.is_temp_table()) {
    if (OB_FAIL(cur_resolver_->resolve_all_generated_table_columns(table_item, column_items))) {
      LOG_WARN("resolve all generated table columns failed", K(ret));
    }
  } else if (table_item.is_joined_table()) {
    const JoinedTable &joined_table = static_cast<const JoinedTable &> (table_item);
    if (OB_ISNULL(joined_table.left_table_) || OB_ISNULL(joined_table.right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(SMART_CALL(resolve_all_table_columns(*joined_table.left_table_, column_items)))) {
      LOG_WARN("failed to resolve columns for left table of joined table", K(ret));
    } else if (OB_FAIL(SMART_CALL(resolve_all_table_columns(*joined_table.right_table_, column_items)))) {
      LOG_WARN("failed to resolve columns for right table of joined table", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not support this table", K(table_item), K(ret));
  }
  return ret;
}

int ObTransposeResolver::get_old_or_group_column(
    const ObIArray<ObString> &columns_in_aggrs,
    TableItem &orig_table_item,
    TransposeDef &trans_def)
{
  int ret = OB_SUCCESS;
  //1.get columns
  ObSEArray<ColumnItem, 16> column_items;
  if (OB_FAIL(resolve_all_table_columns(orig_table_item, column_items))) {
    LOG_WARN("failed to resolve all table columns", K(ret));
  }

  // 2. get group/old column
  if (trans_def.is_pivot()) {
    PivotDef &pivot_def = static_cast<PivotDef &> (trans_def);
    if (OB_FAIL(remove_column_item_by_names(column_items, columns_in_aggrs))) {
      LOG_WARN("failed to remove column item by name", K(ret));
    } else if (OB_FAIL(remove_column_item_by_names(column_items, pivot_def.for_column_names_))) {
      LOG_WARN("failed to remove column item by name", K(ret));
    }
    pivot_def.group_columns_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
      ObColumnRefRawExpr *col_ref_expr = column_items.at(i).get_expr();
      if (OB_ISNULL(col_ref_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_ref_expr is null", K(ret));
      } else if (OB_FAIL(col_ref_expr->set_expr_name(col_ref_expr->get_column_name()))) {
        LOG_WARN("failed to set expr name", K(ret));
      } else if (OB_FAIL(pivot_def.group_columns_.push_back(col_ref_expr)) ||
                OB_FAIL(pivot_def.group_column_names_.push_back(col_ref_expr->get_column_name()))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (ObUserDefinedSQLType == col_ref_expr->get_data_type()) {
        ret = OB_ERR_NO_ORDER_MAP_SQL;
        LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
      } else if (ObLongTextType == col_ref_expr->get_data_type()
                || ObLobType == col_ref_expr->get_data_type()
                || ObJsonType == col_ref_expr->get_data_type()
                || ObGeometryType == col_ref_expr->get_data_type()
                || ObExtendType == col_ref_expr->get_data_type()) {
        ret = (lib::is_oracle_mode() && ObJsonType == col_ref_expr->get_data_type())
                ? OB_ERR_INVALID_CMP_OP : OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("group by lob expr is not allowed", K(ret));
      }
    }
  } else {
    UnpivotDef &unpivot_def = static_cast<UnpivotDef &> (trans_def);
    for (int64_t i = 0; OB_SUCC(ret) && i < unpivot_def.in_pairs_.count(); ++i) {
      const common::ObIArray<ObString> &in_columns = unpivot_def.in_pairs_.at(i).column_names_;
      if (OB_FAIL(remove_column_item_by_names(column_items, in_columns))) {
        LOG_WARN("failed to remove column item by name", K(ret));
      }
    }
    unpivot_def.orig_columns_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
      ObColumnRefRawExpr *col_ref_expr = column_items.at(i).get_expr();
      if (OB_ISNULL(col_ref_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_ref_expr is null", K(ret));
      } else if (OB_FAIL(col_ref_expr->set_expr_name(col_ref_expr->get_column_name()))) {
        LOG_WARN("failed to set expr name", K(ret));
      } else if (OB_FAIL(unpivot_def.orig_columns_.push_back(col_ref_expr)) ||
                OB_FAIL(unpivot_def.orig_column_names_.push_back(col_ref_expr->get_column_name()))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObTransposeResolver::try_add_cast_to_unpivot(UnpivotDef &unpivot_def)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExprResType, 4> left_types;
  ObSEArray<ObExprResType, 4> tmp_types;
  ObSEArray<ObExprResType, 4> res_types;
  // get types
  for (int64_t i = 0; OB_SUCC(ret) && i < unpivot_def.in_pairs_.count(); ++i) {
    UnpivotDef::InPair &in_pair = unpivot_def.in_pairs_.at(i);
    tmp_types.reuse();
    for (int64_t j = 0; OB_SUCC(ret) && j < in_pair.const_exprs_.count(); ++j) {
      if (OB_ISNULL(in_pair.const_exprs_.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(tmp_types.push_back(in_pair.const_exprs_.at(j)->get_result_type()))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < in_pair.column_exprs_.count(); ++j) {
      if (OB_ISNULL(in_pair.column_exprs_.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(tmp_types.push_back(in_pair.column_exprs_.at(j)->get_result_type()))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (i == 0) {
      if (OB_FAIL(left_types.assign(tmp_types))) {
        LOG_WARN("failed to assign", K(ret));
      }
    } else {
      if (OB_FAIL(get_merge_type_for_unpivot(left_types, tmp_types, res_types))) {
        LOG_WARN("invalid types", K(ret));
      } else if (OB_FAIL(left_types.assign(res_types))) {
        LOG_WARN("failed to assign", K(ret));
      }
    }
  }

  // add cast
  for (int64_t i = 0; OB_SUCC(ret) && i < unpivot_def.in_pairs_.count(); ++i) {
    ObRawExpr *casted_expr = NULL;
    UnpivotDef::InPair &in_pair = unpivot_def.in_pairs_.at(i);
    tmp_types.reuse();
    for (int64_t j = 0; OB_SUCC(ret) && j < in_pair.const_exprs_.count(); ++j) {
      if (OB_ISNULL(in_pair.const_exprs_.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(cur_resolver_->params_.expr_factory_,
                                                                 cur_resolver_->params_.session_info_,
                                                                 *in_pair.const_exprs_.at(j),
                                                                 left_types.at(j),
                                                                 casted_expr))) {
        LOG_WARN("failed to try add cast expr", K(ret));
      } else if (OB_ISNULL(casted_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else {
        in_pair.const_exprs_.at(j) = casted_expr;
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < in_pair.column_exprs_.count(); ++j) {
      if (OB_ISNULL(in_pair.column_exprs_.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(cur_resolver_->params_.expr_factory_,
                                                                 cur_resolver_->params_.session_info_,
                                                                 *in_pair.column_exprs_.at(j),
                                                                 left_types.at(in_pair.const_exprs_.count() + j),
                                                                 casted_expr))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_ISNULL(casted_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else {
        in_pair.column_exprs_.at(j) = casted_expr;
      }
    }
  }

  // record res types for label columns and value columns
  for (int64_t i = 0; OB_SUCC(ret) && i < left_types.count(); ++i) {
    if (OB_UNLIKELY(left_types.count() != unpivot_def.label_columns_.count() + unpivot_def.value_columns_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("count of res types not equal", K(ret));
    } else if (i < unpivot_def.label_columns_.count()) {
      if (OB_FAIL(unpivot_def.label_col_types_.push_back(left_types.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    } else {
      if (OB_FAIL(unpivot_def.value_col_types_.push_back(left_types.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObTransposeResolver::get_merge_type_for_unpivot(ObIArray<ObExprResType> &left_types,
                                                    ObIArray<ObExprResType> &right_types,
                                                    ObIArray<ObExprResType> &res_types)
{
  int ret = OB_SUCCESS;
  ObExprResType res_type;
  ObIAllocator *allocator = NULL;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(cur_resolver_) ||
      OB_ISNULL(allocator = cur_resolver_->params_.allocator_) ||
      OB_ISNULL(session_info = cur_resolver_->params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params is null", K(ret));
  } else if (OB_UNLIKELY(left_types.count() != right_types.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count of left types and right types not equal", K(ret));
  } else {
    res_types.reuse();
    const int64_t num = left_types.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
      res_type.reset();
      ObExprResType &left_type = left_types.at(i);
      ObExprResType &right_type = right_types.at(i);
      if (!left_type.has_result_flag(NOT_NULL_FLAG) || !right_type.has_result_flag(NOT_NULL_FLAG)) {
        left_type.unset_result_flag(NOT_NULL_FLAG);
        right_type.unset_result_flag(NOT_NULL_FLAG);
      }
      if (left_type != right_type || ob_is_enumset_tc(right_type.get_type())) {
        ObSEArray<ObExprResType, 2> types;
        ObExprVersion dummy_op(*allocator);
        ObCollationType coll_type = CS_TYPE_INVALID;
        ObExprTypeCtx type_ctx;
        ObSQLUtils::init_type_ctx(session_info, type_ctx);
        bool skip_add_cast = false;
        if (lib::is_oracle_mode()) {
          if (OB_FAIL(ObOptimizerUtil::check_oracle_mode_set_type_validity(
                  session_info->is_varparams_sql_prepare(), left_type, right_type, false, &skip_add_cast))) {
            LOG_WARN("left and right type of set operator not valid in oracle mode", K(ret));
          }
          LOG_DEBUG("data type check for each select item in set operator", K(left_type),
                                                                            K(right_type));
        }
        if (OB_FAIL(ret)) {
        } else if (skip_add_cast) {
          res_type = left_type;
        } else if (OB_FAIL(types.push_back(left_type)) || OB_FAIL(types.push_back(right_type))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(res_type, &types.at(0), 2,
                            is_oracle_mode(), type_ctx))) {
          if (session_info->is_varparams_sql_prepare()) {
            skip_add_cast = true;
            res_type = left_type;
            LOG_WARN("failed to deduce type in ps prepare stage", K(types));
          } else {
            LOG_WARN("failed to aggregate result type for merge", K(ret));
          }
        }
        if (OB_FAIL(ret) || skip_add_cast) {
        } else if (ObMaxType == res_type.get_type()) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("column type incompatible", K(left_type), K(right_type));
        } else if ((left_type.is_ext() && !right_type.is_ext()) ||
                   (!left_type.is_ext() && right_type.is_ext()) ||
                   (left_type.is_ext() && right_type.is_ext() &&
                    left_type.get_udt_id() != right_type.get_udt_id())) {
          ret = OB_ERR_EXP_NEED_SAME_DATATYPE;
          LOG_WARN("expression must have same datatype as corresponding expression", K(ret), K(left_type), K(right_type));
        }
      } else {
        res_type = left_type;
      }
      if (OB_SUCC(ret)  && OB_FAIL(res_types.push_back(res_type))) {
        LOG_WARN("failed to push back res type", K(ret));
      }
    }
  }
  return ret;
}

int ObTransposeResolver::transform_to_view(TableItem &orig_table_item,
                                           TransposeDef &trans_def,
                                           TableItem *&view_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> select_exprs;
  ObSEArray<ObRawExpr *, 4> condition_exprs;
  ObSEArray<ObRawExpr *, 4> group_exprs;
  ObSEArray<ObRawExpr *, 4> *group_exprs_ptr = NULL;
  ObSEArray<ObRawExpr *, 4> old_table_cols;
  ObSEArray<ObRawExpr *, 4> new_table_cols;
  if (trans_def.is_pivot()) {
    if (OB_FAIL(get_exprs_for_pivot_table(static_cast<PivotDef &>(trans_def), select_exprs, group_exprs))) {
      LOG_WARN("failed to build inline view for pivot", K(ret));
    } else {
      group_exprs_ptr = &group_exprs;
    }
  } else /* unpivot */ {
    if (OB_FAIL(get_exprs_for_unpivot_table(static_cast<UnpivotDef &>(trans_def), select_exprs, condition_exprs))) {
      LOG_WARN("failed to build inline view for unpivot", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_new_table_item(view_table, trans_def.alias_name_))) {
    LOG_WARN("failed to replace with empty view", K(ret));
  } else if (OB_FAIL(push_transpose_table_into_view(view_table,
                                                    &orig_table_item,
                                                    trans_def,
                                                    select_exprs,
                                                    condition_exprs,
                                                    group_exprs_ptr))) {
  LOG_WARN("failed to push transpose table into view", K(ret));
  } else if (OB_UNLIKELY(!view_table->is_generated_table()) ||
             OB_ISNULL(view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected view table", K(ret), K(*view_table));
  }
  return ret;
}

int ObTransposeResolver::get_exprs_for_pivot_table(PivotDef &pivot_def,
                                                   ObIArray<ObRawExpr *> &select_exprs,
                                                   ObIArray<ObRawExpr *> &group_exprs)
{
  int ret = OB_SUCCESS;
  // get group columns
  ObIArray<ObRawExpr *> &groups = pivot_def.group_columns_;
  for (int64_t i = 0; OB_SUCC(ret) && i < groups.count(); ++i) {
    if (OB_FAIL(select_exprs.push_back(groups.at(i))) ||
        OB_FAIL(group_exprs.push_back(groups.at(i)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  // get other select exprs
  for (int64_t i = 0; OB_SUCC(ret) && i < pivot_def.in_pairs_.count(); ++i) {
    PivotDef::InPair in_pair = pivot_def.in_pairs_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < pivot_def.aggr_pairs_.count(); ++j) {
      PivotDef::AggrPair &aggr_pair = pivot_def.aggr_pairs_.at(j);
      ObRawExpr *select_expr = NULL;
      if (OB_FAIL(build_select_expr_for_pivot(aggr_pair, in_pair, pivot_def.for_columns_, select_expr))) {
        LOG_WARN("failed to build select expr for pivot", K(ret));
      } else if (OB_ISNULL(select_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select_expr is null", K(ret));
      } else if (OB_FAIL(select_exprs.push_back(select_expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

// aggr(expr) -> aggr(case when (for_exprs) in (in_exprs) then expr end)
// count(*) -> count(case when (for_exprs) in (in_exprs) then 1 end)
// list_agg(expr1, expr2) -> list_agg(case when (for_exprs) in (in_exprs) then expr1 end, expr2)
// ((c1, c2) in (1, 2)) is in fact (c1=1 and c2=2), so 'for_exprs in in_exprs' is 'for_expr1 = in_expr2 and ...'
int ObTransposeResolver::build_select_expr_for_pivot(
    PivotDef::AggrPair &aggr_pair,
    PivotDef::InPair &in_pair,
    ObIArray<ObRawExpr *> &for_exprs,
    ObRawExpr *&select_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(cur_resolver_) ||
      OB_ISNULL(expr_factory = cur_resolver_->params_.expr_factory_) ||
      OB_ISNULL(session_info = cur_resolver_->params_.session_info_) ||
      OB_ISNULL(aggr_pair.expr_) ||
      OB_UNLIKELY(for_exprs.count() != in_pair.const_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null or invalid expr_exprs and in_pair", K(ret));
  } else {
    ObSEArray<ObRawExpr *, 2> param_exprs;
    ObSEArray<ObRawExpr *, 2> new_param_exprs;
    ObAggFunRawExpr *ori_aggr_expr = static_cast<ObAggFunRawExpr *> (aggr_pair.expr_);
    ObAggFunRawExpr *aggr_expr = NULL;
    ObRawExpr *when_expr = NULL;
    ObRawExpr *null_expr = NULL;
    ObSEArray<ObRawExpr *, 1> equal_exprs;
    if (ori_aggr_expr->get_real_param_count() == 0) /* count(*) */ {
      ObConstRawExpr *const_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, 1, const_expr))) {
        LOG_WARN("failed to build const expr 1", K(ret));
      } else if (OB_FAIL(param_exprs.push_back(const_expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    } else {
      if (OB_FAIL(param_exprs.assign(ori_aggr_expr->get_real_param_exprs()))) {
        LOG_WARN("failed to assign", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(expr_factory->create_raw_expr(aggr_pair.expr_->get_expr_type(), aggr_expr))) {
      LOG_WARN("failed to create raw expr", K(ret));
    } else if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(aggr_expr->assign(*aggr_pair.expr_))) {
      LOG_WARN("failed to assign aggr expr", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < for_exprs.count(); ++i) {
      ObRawExpr *equal_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::create_equal_expr(*expr_factory,
                                                    session_info,
                                                    in_pair.const_exprs_.at(i),
                                                    for_exprs.at(i),
                                                    equal_expr))) {
        LOG_WARN("failed to create equal expr", K(ret));
      } else if (OB_ISNULL(equal_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(equal_exprs.push_back(equal_expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory,
                                                      equal_exprs,
                                                      when_expr))) {
      LOG_WARN("failed to build and expr", K(ret));
    } else if (OB_ISNULL(when_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (ObRawExprUtils::build_null_expr(*expr_factory, null_expr)) {
      LOG_WARN("failed to build null expr", K(ret));
    } else if (OB_ISNULL(null_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); ++i) {
      ObRawExpr *case_expr = NULL;
      if (OB_ISNULL(param_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (ObRawExprUtils::build_case_when_expr(*expr_factory,
                                                      when_expr /*when expr*/,
                                                      param_exprs.at(i)/*then expr*/,
                                                      null_expr /*default_expr*/,
                                                      case_expr)) {
        LOG_WARN("failed to build case when expr", K(ret));
      } else if (OB_ISNULL(case_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(new_param_exprs.push_back(case_expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (FAILEDx(aggr_expr->get_real_param_exprs_for_update().assign(new_param_exprs))) {
      LOG_WARN("failed to assign", K(ret));
    } else {
      select_expr = aggr_expr;
    }
  }

  if (OB_SUCC(ret)) {
    ObSqlString comb_name;
    ObString name_string;
    if (aggr_pair.alias_name_.empty()) {
      // in_pair name
      if (OB_FAIL(comb_name.append(in_pair.name_))) {
        LOG_WARN("failed to append", K(ret));
      }
    } else {
      // aggr_name + '_' + in_pair_name
      if (OB_FAIL(comb_name.append(in_pair.name_)) ||
          OB_FAIL(comb_name.append("_")) ||
          OB_FAIL(comb_name.append(aggr_pair.alias_name_))) {
        LOG_WARN("failed to append", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_write_string(*cur_resolver_->params_.allocator_, comb_name.string(), name_string))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (OB_FAIL(select_expr->set_expr_name(name_string))) {
      LOG_WARN("failed to set expr name", K(ret));
    }
  }
  return ret;
}

int ObTransposeResolver::get_exprs_for_unpivot_table(UnpivotDef &trans_def,
                                                     ObIArray<ObRawExpr *> &select_exprs,
                                                     ObIArray<ObRawExpr *> &condition_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(cur_resolver_) ||
      OB_ISNULL(expr_factory = cur_resolver_->params_.expr_factory_) ||
      OB_UNLIKELY(!trans_def.is_unpivot()) ||
      OB_UNLIKELY(trans_def.in_pairs_.count() == 0) ||
      OB_UNLIKELY(trans_def.label_columns_.count() != trans_def.label_col_types_.count()) ||
      OB_UNLIKELY(trans_def.value_columns_.count() != trans_def.value_col_types_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument or context", K(ret));
  } else {
    // old columns (old column expr)
    ObIArray<ObRawExpr *> &old_columns = trans_def.orig_columns_;
    ObSEArray<ObRawExpr *, 1> param_exprs;
    ObRawExpr *target_expr = NULL;
    bool need_unpivot_op = trans_def.in_pairs_.count() > 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < old_columns.count(); ++i) {
      if (OB_FAIL(select_exprs.push_back(old_columns.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    // lab columns (unpivot(in_pair.const_exprs))
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_def.label_columns_.count(); ++i) {
      param_exprs.reuse();
      for (int64_t j = 0; OB_SUCC(ret) && j < trans_def.in_pairs_.count(); ++j) {
        UnpivotDef::InPair &in_pair = trans_def.in_pairs_.at(j);
        if (OB_UNLIKELY(in_pair.const_exprs_.count() != trans_def.label_columns_.count()) ||
            OB_ISNULL(in_pair.const_exprs_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid count or unexpected null", K(ret));
        } else if (OB_FAIL(param_exprs.push_back(in_pair.const_exprs_.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (need_unpivot_op) {
        if (OB_FAIL(ObRawExprUtils::build_unpivot_expr(*expr_factory, param_exprs, target_expr, true))) {
          LOG_WARN("failed to build unpivot expr", K(ret));
        }
      } else {
        target_expr = param_exprs.at(0);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(target_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unpivot_expr is null", K(ret));
      } else if (OB_FAIL(target_expr->set_expr_name(trans_def.label_columns_.at(i)))) {
        LOG_WARN("failed to set expr name", K(ret));
      } else if (OB_FALSE_IT(target_expr->set_result_type(trans_def.label_col_types_.at(i)))) {
        LOG_WARN("failed to set result type", K(ret));
      } else if (OB_FAIL(select_exprs.push_back(target_expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    // value column (unpivot(in_pair.column_exprs))
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_def.value_columns_.count(); ++i) {
      param_exprs.reuse();
      for (int64_t j = 0; OB_SUCC(ret) && j < trans_def.in_pairs_.count(); ++j) {
        UnpivotDef::InPair &in_pair = trans_def.in_pairs_.at(j);
        if (OB_UNLIKELY(in_pair.column_exprs_.count() != trans_def.value_columns_.count()) ||
            OB_ISNULL(in_pair.column_exprs_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid count or unexpected null", K(ret));
        } else if (OB_FAIL(param_exprs.push_back(in_pair.column_exprs_.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (need_unpivot_op) {
        if (OB_FAIL(ObRawExprUtils::build_unpivot_expr(*expr_factory, param_exprs, target_expr, false))) {
          LOG_WARN("failed to build unpivot expr", K(ret));
        }
      } else {
        target_expr = param_exprs.at(0);
        if (trans_def.is_exclude_null()) {
          ObRawExpr *not_null_expr = NULL;
          if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*expr_factory, target_expr, true, not_null_expr))) {
            LOG_WARN("failed to build is not null expr", K(ret));
          } else if (OB_ISNULL(not_null_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret));
          } else if (OB_FAIL(condition_exprs.push_back(not_null_expr))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(target_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unpivot_expr is null", K(ret));
      } else if (OB_FAIL(target_expr->set_expr_name(trans_def.value_columns_.at(i)))) {
        LOG_WARN("failed to set expr name", K(ret));
      } else if (OB_FALSE_IT(target_expr->set_result_type(trans_def.value_col_types_.at(i)))) {
        LOG_WARN("failed to set result type", K(ret));
      } else if (OB_FAIL(select_exprs.push_back(target_expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObTransposeResolver::get_unpivot_item(UnpivotDef &unpivot_def,
                                          ObIArray<ObRawExpr *> &select_exprs,
                                          ObUnpivotItem *&unpivot_item)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = NULL;
  void *ptr = NULL;
  if (OB_ISNULL(cur_resolver_) ||
      OB_ISNULL(allocator = cur_resolver_->params_.allocator_) ||
      OB_UNLIKELY(!unpivot_def.is_unpivot())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected argument", K(ret));
  } else if (unpivot_def.in_pairs_.count() == 1) {
    unpivot_item = NULL;
  } else if (OB_ISNULL(ptr = allocator->alloc(sizeof(ObUnpivotItem)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create unpivot info failed");
  } else if (OB_ISNULL(unpivot_item = new (ptr) ObUnpivotItem())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    int64_t old_count = unpivot_def.orig_columns_.count();
    int64_t lab_count = unpivot_def.label_columns_.count();
    int64_t val_count = unpivot_def.value_columns_.count();
    unpivot_item->is_include_null_ = unpivot_def.is_include_null();
    if (OB_UNLIKELY(select_exprs.count() != old_count + lab_count + val_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected argument", K(ret), K(select_exprs), K(old_count), K(lab_count), K(val_count));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < old_count; ++i) {
      if (OB_FAIL(unpivot_item->origin_exprs_.push_back(select_exprs.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < lab_count; ++i) {
      if (OB_FAIL(unpivot_item->label_exprs_.push_back(select_exprs.at(old_count + i)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < val_count; ++i) {
      if (OB_FAIL(unpivot_item->value_exprs_.push_back(select_exprs.at(old_count + lab_count + i)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObTransposeResolver::push_transpose_table_into_view(
    TableItem *&view_table,
    TableItem *origin_table,
    TransposeDef &trans_def,
    ObIArray<ObRawExpr *> &select_exprs,
    ObIArray<ObRawExpr *> &conditions,
    ObIArray<ObRawExpr *> *group_exprs /* = NULL*/)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt = NULL;
  ObSelectStmt *view_stmt = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObIAllocator *allocator = NULL;
  ObSEArray<uint64_t, 8> basic_table_ids;

  if (OB_ISNULL(cur_resolver_)
     || OB_ISNULL(origin_table)
     || OB_ISNULL(stmt = cur_resolver_->get_stmt())
     || OB_ISNULL(expr_factory = cur_resolver_->params_.expr_factory_)
     || OB_ISNULL(allocator = cur_resolver_->params_.allocator_)
     || OB_ISNULL(view_table)
     || OB_UNLIKELY(!view_table->is_generated_table())
     || OB_UNLIKELY(NULL != view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(stmt));
  } else if (OB_FAIL(cur_resolver_->params_.stmt_factory_->create_stmt(view_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_ISNULL(view_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(view_stmt));
  } else if (OB_FAIL(view_stmt->get_stmt_hint().assign(stmt->get_stmt_hint()))) {
    LOG_WARN("failed to set simple view hint", K(ret));
  } else if (FALSE_IT(view_stmt->set_query_ctx(stmt->get_query_ctx()))) {
    // never reach
  } else if (OB_FAIL(view_stmt->set_stmt_id())) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else {
    view_table->ref_query_ = view_stmt;
    view_table->table_name_ = trans_def.alias_name_;
    view_table->alias_name_ = trans_def.alias_name_;
    if (trans_def.is_pivot()) {
      view_stmt->set_from_pivot(true);
    } else {
      ObUnpivotItem *unpivot_item = NULL;
      if (OB_FAIL(get_unpivot_item(static_cast<UnpivotDef &>(trans_def), select_exprs, unpivot_item))) {
        LOG_WARN("failed to get unpivot info", K(ret));
      } else {
        view_stmt->set_unpivot_item(unpivot_item);
      }
    }
  }

  // 1. construct view stmt
  // 1.1 move from tables
  if (OB_SUCC(ret)) {
    bool is_joined_table = (NULL != origin_table && origin_table->is_joined_table());
    TableItem *table = origin_table;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null", K(ret), K(table));
    } else if (OB_FAIL(view_stmt->add_from_item(table->table_id_, is_joined_table))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (is_joined_table) {
      if (OB_FAIL(stmt->remove_joined_table_item(table->table_id_))) {
        LOG_WARN("failed to remove joined table", K(ret));
      } else if (OB_FAIL(append(basic_table_ids, static_cast<JoinedTable *>(table)->single_table_ids_))) {
        LOG_WARN("failed to append single table ids", K(ret));
      } else if (OB_FAIL(view_stmt->add_joined_table(static_cast<JoinedTable *>(table)))) {
        LOG_WARN("failed to add joined table", K(ret));
      }
    } else if (OB_FAIL(basic_table_ids.push_back(table->table_id_))) {
      LOG_WARN("failed to push back table id", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < basic_table_ids.count(); ++i) {
    uint64_t table_id = basic_table_ids.at(i);
    TableItem *table = NULL;
    ObArray<ColumnItem> column_items;
    ObArray<PartExprItem> part_expr_items;
    CheckConstraintItem check_constraint_item;
    if (OB_ISNULL(table = stmt->get_table_item_by_id(table_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null", K(ret), K(table));
    } else if (OB_FAIL(stmt->get_column_items(table_id, column_items))) {
      LOG_WARN("failed to get column items", K(ret));
    } else if (OB_FAIL(stmt->get_part_expr_items(table_id, part_expr_items))) {
      LOG_WARN("failed to get part expr items", K(ret));
    } else if (OB_FAIL(stmt->get_check_constraint_items(table_id, check_constraint_item))) {
      LOG_WARN("failed to get check constraint item", K(ret));
    } else if (OB_FAIL(view_stmt->get_table_items().push_back(table))) {
      LOG_WARN("failed to add table item", K(ret));
    } else if (OB_FAIL(append(view_stmt->get_column_items(), column_items))) {
      LOG_WARN("failed to add column items", K(ret));
    } else if (OB_FAIL(view_stmt->set_part_expr_items(part_expr_items))) {
      LOG_WARN("failed to set part expr items", K(ret));
    } else if (OB_FAIL(view_stmt->set_check_constraint_item(check_constraint_item))) {
      LOG_WARN("failed to add check constraint items", K(ret));
    } else if (OB_FAIL(stmt->remove_table_item(table))) {
      LOG_WARN("failed to remove table item", K(ret));
    } else if (OB_FAIL(stmt->remove_column_item(basic_table_ids.at(i)))) {
      LOG_WARN("failed to remove column item", K(ret));
    } else if (OB_FAIL(stmt->remove_part_expr_items(basic_table_ids.at(i)))) {
      LOG_WARN("failed to remove part expr items", K(ret));
    } else if (OB_FAIL(stmt->remove_check_constraint_item(basic_table_ids.at(i)))) {
      LOG_WARN("failed to remove check constraint item", K(ret));
    }
  }

  // 2 construct group exprs.
  if (OB_FAIL(ret)) {
  } else if ( OB_FAIL(view_stmt->get_condition_exprs().assign(conditions))) {
    LOG_WARN("failed to assign conditions", K(ret));
  } else if (NULL != group_exprs &&
             OB_FAIL(view_stmt->get_group_exprs().assign(*group_exprs))) {
    LOG_WARN("failed to assign group exprs", K(ret));

  // 3 extract aggr and winfun
  } else if (OB_FAIL(ObTransformUtils::extract_aggr_expr(select_exprs, view_stmt->get_aggr_items()))) {
    LOG_WARN("failed to extract aggr expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_winfun_expr(select_exprs, view_stmt->get_window_func_exprs()))) {
    LOG_WARN("failed to extract aggr expr", K(ret));

  // 4 generate select list and adjust structures
  } else if (OB_FAIL(view_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(view_stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item by id", K(ret));
  } else if (OB_FAIL(generate_select_list(view_table, select_exprs))) {
    LOG_WARN("failed to generate select list", K(ret));
  } else if ((stmt->is_delete_stmt() || stmt->is_update_stmt() || stmt->is_merge_stmt()) &&
            OB_FAIL(ObTransformUtils::adjust_updatable_view(*expr_factory,
                                                            static_cast<ObDelUpdStmt*>(stmt),
                                                            *view_table,
                                                            &basic_table_ids))) {
    LOG_WARN("failed to adjust updatable view", K(ret));
  } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item by id", K(ret));
  } else if (OB_FAIL(stmt->formalize_stmt(cur_resolver_->params_.session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else {
    if (view_stmt->has_for_update() && lib::is_oracle_mode()) {
      view_table->for_update_ = true;
    }
  }
  if (OB_SUCC(ret) && stmt->is_select_stmt()) {
    //check qualify filters
    if (OB_FAIL(ObTransformUtils::pushdown_qualify_filters(static_cast<ObSelectStmt *>(stmt)))) {
      LOG_WARN("check pushdown qualify filters failed", K(ret));
    }
  }
  return ret;
}

int ObTransposeResolver::add_new_table_item(TableItem *&view_table, const ObString &alias_name)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  ObIAllocator *allocator = NULL;
  ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(cur_resolver_) ||
      OB_ISNULL(stmt = cur_resolver_->get_stmt()) ||
      OB_ISNULL(allocator = cur_resolver_->params_.allocator_) ||
      OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to add new table_item because some value is null", K(ret));
  } else if (OB_ISNULL(table_item = stmt->create_table_item(*allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create table item failed");
  } else {
    table_item->table_id_ = stmt->get_query_ctx()->available_tb_id_--;
    table_item->type_ = TableItem::GENERATED_TABLE;
    table_item->ref_id_ = OB_INVALID_ID;
    table_item->database_name_ = ObString::make_string("");
    table_item->table_name_ = alias_name;
    table_item->alias_name_ = alias_name;
    table_item->ref_query_ = NULL;
    if (OB_FAIL(stmt->set_table_bit_index(table_item->table_id_))) {
      LOG_WARN("fail to add table_id to hash table", K(ret), K(table_item));
    } else if (OB_FAIL(stmt->get_table_items().push_back(table_item))) {
      LOG_WARN("add table item failed", K(ret));
    } else {
      view_table = table_item;
    }
  }
  return ret;
}

// copied from ObTransformUtils
int ObTransposeResolver::generate_select_list(TableItem *table,
                                              ObIArray<ObRawExpr *> &basic_select_exprs)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt = NULL;
  ObSelectStmt *view_stmt = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObArray<ObRawExpr *> shared_exprs;
  ObArray<ObRawExpr *> column_exprs;
  ObArray<ObRawExpr *> select_exprs;
  if (OB_ISNULL(cur_resolver_) || OB_ISNULL(stmt = cur_resolver_->get_stmt()) ||
      OB_ISNULL(table) || OB_ISNULL(expr_factory = cur_resolver_->params_.expr_factory_) ||
      OB_UNLIKELY(!table->is_generated_table()) ||
      OB_ISNULL(view_stmt = table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), KPC(table));
  } else if (OB_FAIL(select_exprs.assign(basic_select_exprs))) {
    LOG_WARN("failed to assign", K(ret));
  // The shared child exprs of basic_select_exprs should be extracted
  } else if (OB_FAIL(ObTransformUtils::extract_shared_exprs(stmt, view_stmt, shared_exprs, &basic_select_exprs))) {
    LOG_WARN("failed to extract shared expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::remove_const_exprs(shared_exprs, shared_exprs))) {
    LOG_WARN("failed to remove const exprs", K(ret));
  } else if (OB_FAIL(append(select_exprs, shared_exprs))) {
    LOG_WARN("failed to append", K(ret));
  } else if (select_exprs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_select_item(
                 *cur_resolver_->params_.allocator_, select_exprs, view_stmt))) {
    LOG_WARN("failed to create select items", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*stmt))) {
    LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*view_stmt))) {
    LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
  }
  return ret;
}

int ObTransposeResolver::get_column_item_idx_by_name(
    ObIArray<ColumnItem> &array,
    const ObString &var,
    int64_t &idx)
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

int ObTransposeResolver::remove_column_item_by_names(
    ObIArray<ColumnItem> &column_items,
    const ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
    const ObString &name = names.at(i);
    if (OB_FAIL(get_column_item_idx_by_name(column_items, name, idx))) {
      LOG_WARN("fail to get_column_item_idx_by_name", K(name), K(ret));
    } else if (idx >= 0) {
      if (OB_FAIL(column_items.remove(idx))) {
        LOG_WARN("fail to remove column_item", K(idx), K(ret));
      }
    }
  }
  return ret;
}

int ObTransposeResolver::get_combine_name(ObIArray<ObString> &names, ObString &comb_name) {
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = NULL;
  ObSqlString str;
  char name_buf[OB_MAX_COLUMN_NAME_LENGTH];
  int64_t pos = 0;
  if (OB_ISNULL(cur_resolver_) ||
      OB_ISNULL(allocator = cur_resolver_->params_.allocator_) ||
      OB_UNLIKELY(names.count() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
    ObString name = names.at(i);
    if (OB_UNLIKELY(name.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(str.append_fmt("%.*s_", name.length(), name.ptr()))) {
      LOG_WARN("failed to append fmt", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (str.length() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected str with its length equals to 0", K(ret));
  } else {
    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(str.length() - 1)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no more memory to create in_pair_name");
    } else {
      MEMCPY(buf, str.ptr(), str.length() - 1);
      comb_name.assign(buf, str.length() - 1);
    }
  }
  return ret;
}

int ObTransposeResolver::get_combine_name(ObIArray<ObRawExpr *> &exprs, ObString &comb_name) {
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = NULL;
  ObSqlString str;
  char name_buf[OB_MAX_COLUMN_NAME_LENGTH];
  int64_t pos = 0;
  if (OB_ISNULL(cur_resolver_) ||
      OB_ISNULL(allocator = cur_resolver_->params_.allocator_) ||
      OB_UNLIKELY(exprs.count() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < exprs.count(); ++j) {
    ObRawExpr *expr = exprs.at(j);
    pos = 0;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(expr->get_name(name_buf, OB_MAX_COLUMN_NAME_LENGTH, pos))) {
      LOG_WARN("failed to get expr name", K(ret));
    } else if (pos == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr name is empty", K(ret));
    } else if (OB_FAIL(str.append_fmt("%.*s_", int(pos), name_buf))) {
      LOG_WARN("failed to append fmt", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (str.length() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected str with its length equals to 0", K(ret));
  } else {
    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(str.length() - 1)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no more memory to create in_pair_name");
    } else {
      MEMCPY(buf, str.ptr(), str.length() - 1);
      comb_name.assign(buf, str.length() - 1);
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase