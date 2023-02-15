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
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

int ObUniqueConstraintInfo::assign(const ObUniqueConstraintInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(constraint_columns_.assign(other.constraint_columns_))) {
    LOG_WARN("failed to assign constraint columns", K(ret));
  } else {
    table_id_ = other.table_id_;
    index_tid_ = other.index_tid_;
    constraint_name_ = other.constraint_name_;
  }
  return ret;
}

int ObUniqueConstraintInfo::deep_copy(const ObUniqueConstraintInfo &other,
                                      ObIRawExprCopier &expr_copier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_copier.copy(other.constraint_columns_, constraint_columns_))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else {
    table_id_ = other.table_id_;
    index_tid_ = other.index_tid_;
    constraint_name_ = other.constraint_name_;
  }
  return ret;
}

ObInsertStmt::ObInsertStmt()
  : ObDelUpdStmt(stmt::T_INSERT),
    is_all_const_values_(true),
    table_info_()
{
}

ObInsertStmt::~ObInsertStmt()
{
}

int ObInsertStmt::deep_copy_stmt_struct(ObIAllocator &allocator,
                                        ObRawExprCopier &expr_copier,
                                        const ObDMLStmt &input)
{
  int ret = OB_SUCCESS;
  const ObInsertStmt &other = static_cast<const ObInsertStmt &>(input);
  if (OB_UNLIKELY(get_stmt_type() != input.get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret));
  } else if (OB_FAIL(ObDelUpdStmt::deep_copy_stmt_struct(allocator,
                                                         expr_copier,
                                                         input))) {
    LOG_WARN("failed to deep copy stmt struct", K(ret));
  } else if (OB_FAIL(table_info_.deep_copy(expr_copier,other.table_info_))) {
    LOG_WARN("failed to deep copy table info", K(ret));
  } else {
    is_all_const_values_ = other.is_all_const_values_;
  }
  return ret;
}

int ObInsertStmt::assign(const ObInsertStmt &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDelUpdStmt::assign(other))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(table_info_.assign(other.table_info_))) {
    LOG_WARN("failed to assign table info", K(ret));
  } else {
    is_all_const_values_ = other.is_all_const_values_;
  }
  return ret;
}

int ObInsertStmt::check_table_be_modified(uint64_t ref_table_id, bool& is_modified) const
{
  is_modified = table_info_.ref_table_id_ == ref_table_id;
  return OB_SUCCESS;
}

int64_t ObInsertStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  ObSEArray<ObSelectStmt *, 1> child_stmts;
  if (get_child_stmts(child_stmts) != OB_SUCCESS) {
    databuff_printf(buf, buf_len, pos, "ERROR get child stmts failed");
  } else {
    J_KV(N_STMT_TYPE, ((int)stmt_type_),
         N_TABLE, table_items_,
         N_FROM, from_items_,
         N_PARTITION_EXPR, part_expr_items_,
         N_IS_IGNORE, ignore_,
         N_COLUMN_CONV_FUNCTION, table_info_.column_conv_exprs_,
         N_STMT_HINT, stmt_hint_,
         N_QUERY_CTX, *query_ctx_,
         N_VALUE, table_info_.values_vector_,
         "value_desc", table_info_.values_desc_,
         "returning", returning_exprs_,
         N_CHILD_STMT, child_stmts);
    if (is_insert_up()) {
      J_COMMA();
      J_KV(N_ASSIGN, table_info_.assignments_);
    }
    if (is_error_logging()) {
      J_KV("is_err_log", is_error_logging(),
           "err_log_table_name", error_log_info_.table_name_,
           "err_log_database_name", error_log_info_.database_name_,
           "err_log_table_id", error_log_info_.table_id_,
           "err_log_reject_limit", error_log_info_.reject_limit_,
           "err_log_exprs", error_log_info_.error_log_exprs_);
    }
  }
  J_OBJ_END();
  return pos;
}

int ObInsertStmt::part_key_has_rand_value(bool &has) const
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

int ObInsertStmt::part_key_has_auto_inc(bool &has) const
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

int ObInsertStmt::part_key_has_subquery(bool &has) const
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

int ObInsertStmt::part_key_is_updated(bool &is_updated) const
{
  int ret = OB_SUCCESS;
  is_updated = false;
  if (!table_info_.is_link_table_ &&
      OB_FAIL(check_part_key_is_updated(table_info_.assignments_, is_updated))) {
    LOG_WARN("failed to check part key is updated", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObInsertStmt::get_value_exprs(ObIArray<ObRawExpr *> &value_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(table_info_.column_exprs_.count() != table_info_.column_conv_exprs_.count())) {
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

int ObInsertStmt::get_values_desc_for_heap_table(common::ObIArray<ObColumnRefRawExpr*> &arr) const
{
  int ret = OB_SUCCESS;
  arr.reuse();
  if (OB_UNLIKELY(table_info_.part_generated_col_dep_cols_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else if (OB_FAIL(append(arr, table_info_.values_desc_))) {
    LOG_WARN("failed to append expr", K(ret));
  } else if (OB_FAIL(append(arr, table_info_.part_generated_col_dep_cols_))) {
    LOG_WARN("failed to append expr", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObInsertStmt::get_value_vectors_for_heap_table(common::ObIArray<ObRawExpr*> &arr) const
{
  int ret = OB_SUCCESS;
  arr.reuse();
  if (OB_UNLIKELY(table_info_.part_generated_col_dep_cols_.count() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("use get_values_desc instead", K(ret));
  } else {
    const int64_t row_num = table_info_.values_vector_.count() / table_info_.values_desc_.count();
    const int64_t col_num = table_info_.values_desc_.count();
    ObSEArray<ObRawExpr*, 4> def_values;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.part_generated_col_dep_cols_.count(); ++i) {
      ColumnItem *col_item = NULL;
      ObColumnRefRawExpr *col_ref = table_info_.part_generated_col_dep_cols_.at(i);
      CK(OB_NOT_NULL(col_ref));
      OX(col_item = get_column_item_by_id(col_ref->get_table_id(), col_ref->get_column_id()));
      CK(OB_NOT_NULL(col_item));
      CK(OB_NOT_NULL(col_item->default_value_expr_));
      OZ(def_values.push_back(col_item->default_value_expr_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_num; ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < col_num; ++j) {
        OZ(arr.push_back(table_info_.values_vector_.at(i * col_num + j)));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < def_values.count(); ++j) {
        OZ(arr.push_back(def_values.at(j)));
      }
    }
  }
  return ret;
}

int ObInsertStmt::get_ddl_sort_keys(common::ObIArray<OrderItem> &sort_keys) const
{
  int ret = OB_SUCCESS;
  sort_keys.reset();
  CK(2 == get_table_size());
  if (OB_SUCC(ret)) {
    const TableItem *insert_table_item = get_table_item(0);
    const TableItem *table_item = get_table_item(1);
    common::ObArray<ObSelectStmt*> c_stmts;
    ObArray<ObRawExpr *> column_list;
    ObArray<ObRawExpr *> view_column_list;
    if (OB_FAIL(get_child_stmts(c_stmts))) {
      LOG_WARN("fail to get child stmts", K(ret));
    } else if (1 != c_stmts.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt count is unexpected", K(ret));
    } else if (c_stmts.at(0)->get_order_items().count() < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("assign sort keys failed", K(ret));
    } else if (OB_FAIL(sort_keys.assign(c_stmts.at(0)->get_order_items()))) {
      LOG_WARN("fail to assign sort keys", K(ret));
    } else if (OB_FAIL(get_ddl_view_output(*table_item, view_column_list))) {
      LOG_WARN("fail to get view output", K(ret));
    } else if (OB_FAIL(c_stmts.at(0)->get_select_exprs(column_list))) {
      LOG_WARN("get select exprs failed", K(ret));
    } else {
      LOG_INFO("get ddl sort keys", K(sort_keys), K(column_list), K(view_column_list));
      ObSEArray<uint64_t, 4> column_ids; // the offset in select_items of sortkey
      for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys.count(); ++i) {
        int64_t j = 0;
        for (int64_t j = 0; OB_SUCC(ret) && j < column_list.count(); ++j) {
          if (sort_keys.at(i).expr_ == column_list.at(j)) {
            if (j >= view_column_list.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("error unexpected, view column list is not as expected", K(ret), K(j), K(view_column_list));
            } else if (OB_FAIL(column_ids.push_back(j))) {
              LOG_WARN("fail to push back column ids");
            } else {
              sort_keys.at(i).expr_ = view_column_list.at(j);
            }
            break;
          }
        }
        if (OB_SUCC(ret) && j == column_list.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sort key must be prefix of select list", K(ret), K(sort_keys), K(column_list), K(view_column_list));
        }
      }
      if (OB_SUCC(ret)) {
        const common::ObIArray<ObRawExpr*> &column_conv_exprs = get_column_conv_exprs();
        const common::ObIArray<ObColumnRefRawExpr*> &value_desc = get_values_desc();
        const common::ObIArray<ObColumnRefRawExpr*> &column_exprs = table_info_.column_exprs_;
        for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys.count(); ++i) {
          if (column_ids.at(i) >= value_desc.count()) {
            //  ignore, since for geometry, column_list.count() could be bigger than value_desc.count();
          } else {
            // the n-th values desc's column id
            uint64_t column_id = value_desc.at(column_ids.at(i))->get_column_id();
            // get column_exprs'offset by column_id;
            bool found = false;
            for (int64_t j = 0; OB_SUCC(ret) && !found && j < column_exprs.count(); j++) {
              if (OB_ISNULL(column_exprs.at(j))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("column exprs is null", K(ret));
              } else if (column_exprs.at(j)->get_column_id() == column_id) {
                sort_keys.at(i).expr_ = column_conv_exprs.at(j);
                found = true;
              }
            }
            if (OB_SUCC(ret) && !found) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("can't found column conv expr", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObInsertStmt::get_assignments_exprs(ObIArray<ObRawExpr*> &exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info_.assignments_.count(); ++i) {
    if (OB_FAIL(exprs.push_back(table_info_.assignments_.at(i).expr_))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObInsertStmt::get_dml_table_infos(ObIArray<ObDmlTableInfo*>& dml_table_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml_table_info.push_back(&table_info_))) {
    LOG_WARN("failed to push back table info", K(ret));
  }
  return ret;
}

int ObInsertStmt::get_dml_table_infos(ObIArray<const ObDmlTableInfo*>& dml_table_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml_table_info.push_back(&table_info_))) {
    LOG_WARN("failed to push back table info", K(ret));
  }
  return ret;
}

int ObInsertStmt::get_view_check_exprs(ObIArray<ObRawExpr*>& view_check_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(view_check_exprs, table_info_.view_check_exprs_))) {
    LOG_WARN("failed to append view check exprs", K(ret));
  }
  return ret;
}

int64_t ObInsertStmt::get_instead_of_trigger_column_count() const
{
  const TableItem *table_item = NULL;
  int64_t column_count = 0;
  if (NULL != (table_item = get_table_item_by_id(table_info_.table_id_)) &&
      table_item->is_view_table_ &&
      NULL != table_item->ref_query_) {
    column_count = table_item->ref_query_->get_select_item_size();
  }
  return column_count;
}

}  // namespace sql
}  // namespace oceanbase
