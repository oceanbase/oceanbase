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
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObMergeStmt::ObMergeStmt()
    : ObDelUpdStmt(stmt::T_MERGE),
      table_info_()
{
}

ObMergeStmt::~ObMergeStmt()
{
}

int ObMergeStmt::deep_copy_stmt_struct(ObIAllocator &allocator,
                                       ObRawExprCopier &expr_copier,
                                       const ObDMLStmt &input)
{
  int ret = OB_SUCCESS;
  const ObMergeStmt &other = static_cast<const ObMergeStmt &>(input);
  if (OB_UNLIKELY(input.get_stmt_type() != get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret));
  } else if (OB_FAIL(ObDelUpdStmt::deep_copy_stmt_struct(allocator,
                                                         expr_copier,
                                                         input))) {
    LOG_WARN("failed to deep copy stmt struct", K(ret));
  } else if (OB_FAIL(table_info_.deep_copy(expr_copier, other.table_info_))) {
    LOG_WARN("failed to deep copy table info", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObMergeStmt::assign(const ObMergeStmt &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDelUpdStmt::assign(other))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(table_info_.assign(other.table_info_))) {
    LOG_WARN("failed to assign table info", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObMergeStmt::check_table_be_modified(uint64_t ref_table_id, bool& is_modified) const
{
  is_modified = (table_info_.ref_table_id_ == ref_table_id);
  return OB_SUCCESS;
}

int64_t ObMergeStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_),
       N_TABLE, table_items_,
       N_PARTITION_EXPR, part_expr_items_,
       K_(table_info),
       N_QUERY_CTX, stmt_hint_);
  J_OBJ_END();
  return pos;
}

int ObMergeStmt::get_assignments_exprs(ObIArray<ObRawExpr*> &exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.assignments_.count(); ++i) {
    if (OB_FAIL(exprs.push_back(table_info_.assignments_.at(i).expr_))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObMergeStmt::get_dml_table_infos(ObIArray<ObDmlTableInfo*>& dml_table_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml_table_info.push_back(&table_info_))) {
    LOG_WARN("failed to push back table info", K(ret));
  }
  return ret;
}

int ObMergeStmt::get_dml_table_infos(ObIArray<const ObDmlTableInfo*>& dml_table_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml_table_info.push_back(&table_info_))) {
    LOG_WARN("failed to push back table info", K(ret));
  }
  return ret;
}

int ObMergeStmt::get_view_check_exprs(ObIArray<ObRawExpr*>& view_check_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(view_check_exprs, table_info_.view_check_exprs_))) {
    LOG_WARN("failed to append view check exprs", K(ret));
  }
  return ret;
}

int ObMergeStmt::get_value_exprs(ObIArray<ObRawExpr *> &value_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(table_info_.column_conv_exprs_.count() != 0 &&
                  table_info_.column_exprs_.count() != table_info_.column_conv_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column count", K(table_info_), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.column_conv_exprs_.count(); ++i) {
    ObRawExpr *param = NULL;
    ObRawExpr *column_expr = table_info_.column_exprs_.at(i);
    ObRawExpr *column_conv_expr = table_info_.column_conv_exprs_.at(i);
    if (OB_ISNULL(column_expr) || OB_ISNULL(column_conv_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret));
    } else if (column_conv_expr->get_expr_type() != T_FUN_COLUMN_CONV) {
      param = column_conv_expr;
    } else {
      param = column_conv_expr->get_param_expr(4);
      if (OB_ISNULL(param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (ObRawExprUtils::need_column_conv(column_expr->get_result_type(), *param)) {
        param = column_conv_expr;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (OB_FAIL(value_exprs.push_back(param))) {
        LOG_WARN("failed to push back param expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObMergeStmt::part_key_is_updated(bool &is_updated) const
{
  int ret = OB_SUCCESS;
  is_updated = false;
  if (!table_info_.is_link_table_ &&
      OB_FAIL(check_part_key_is_updated(table_info_.assignments_, is_updated))) {
    LOG_WARN("failed to check part key is updated", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObMergeStmt::part_key_has_rand_value(bool &has) const
{
  int ret = OB_SUCCESS;
  has = false;
  //添加insert values中的分区键对应的value expr
  int64_t value_desc_cnt = table_info_.values_desc_.count();
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < value_desc_cnt; ++i) {
    ObColumnRefRawExpr *col_expr = table_info_.values_desc_.at(i);
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (IS_SHADOW_COLUMN(col_expr->get_column_id())) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::get_base_column(this, col_expr))) {
      LOG_WARN("failed to get base column", K(ret));
    } else if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (col_expr->is_table_part_key_column()
              || col_expr->is_table_part_key_org_column()) {
      for (int64_t j = i; OB_SUCC(ret) && !has && j < table_info_.values_vector_.count(); j += value_desc_cnt) {
        if (table_info_.values_vector_.at(j)->has_flag(CNT_RAND_FUNC)
            || table_info_.values_vector_.at(j)->has_flag(CNT_STATE_FUNC)) {
          has = true;
        }
      }
    }
  }
  return ret;
}

int ObMergeStmt::part_key_has_auto_inc(bool &has) const
{
  int ret = OB_SUCCESS;
  has = false;
  for (int64_t i = 0; OB_SUCCESS == ret && i < table_info_.column_exprs_.count(); ++i) {
    ObColumnRefRawExpr *col_expr = table_info_.column_exprs_.at(i);
    if (IS_SHADOW_COLUMN(col_expr->get_column_id())) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::get_base_column(this, col_expr))) {
      LOG_WARN("failed to get base column", K(ret));
    } else if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if ((col_expr->is_table_part_key_column() || col_expr->is_table_part_key_org_column()) &&
               col_expr->is_auto_increment()) {
      has = true;
      break;
    }
  }

  return ret;
}

int ObMergeStmt::part_key_has_subquery(bool &has) const
{
  int ret = OB_SUCCESS;
  has = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < table_info_.values_desc_.count(); i++) {
    ObColumnRefRawExpr *column_expr = NULL;
    if (OB_ISNULL(column_expr = table_info_.values_desc_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (IS_SHADOW_COLUMN(column_expr->get_column_id())) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::get_base_column(this, column_expr))) {
      LOG_WARN("failed to get base column", K(ret));
    } else if (OB_ISNULL(column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (column_expr->is_table_part_key_column() || column_expr->is_table_part_key_org_column()) {
      for (int64_t j = i; OB_SUCC(ret) && !has && j < table_info_.values_vector_.count();
           j += table_info_.values_desc_.count()) {
        if (table_info_.values_vector_.at(j)->has_flag(CNT_SUB_QUERY)
            || table_info_.values_vector_.at(j)->has_flag(CNT_DYNAMIC_PARAM)) {
          has = true;
        }
      }
    }
  }
  return ret;
}

}//sql
}//common
