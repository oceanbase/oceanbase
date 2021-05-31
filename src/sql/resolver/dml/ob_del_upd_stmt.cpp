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
#include "ob_del_upd_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "common/ob_smart_call.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

uint64_t TableColumns::hash(uint64_t seed) const
{
  seed = do_hash(table_name_, seed);
  int64_t N = index_dml_infos_.count();
  for (int64_t i = 0; i < N; ++i) {
    seed = do_hash(index_dml_infos_.at(i), seed);
  }
  return seed;
}

int TableColumns::deep_copy(ObRawExprFactory& expr_factory, const TableColumns& other)
{
  int ret = OB_SUCCESS;
  table_name_ = other.table_name_;
  if (OB_FAIL(index_dml_infos_.prepare_allocate(other.index_dml_infos_.count()))) {
    LOG_WARN("failed to prepare allocate index dml infos array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.index_dml_infos_.count(); ++i) {
    if (OB_FAIL(index_dml_infos_.at(i).deep_copy(expr_factory, other.index_dml_infos_.at(i)))) {
      LOG_WARN("failed to deep copy index dml info", K(ret));
    }
  }
  return ret;
}

int IndexDMLInfo::deep_copy(ObRawExprFactory& expr_factory, const IndexDMLInfo& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  loc_table_id_ = other.loc_table_id_;
  index_tid_ = other.index_tid_;
  index_name_ = other.index_name_;
  part_cnt_ = other.part_cnt_;
  all_part_num_ = other.all_part_num_;
  rowkey_cnt_ = other.rowkey_cnt_;
  need_filter_null_ = other.need_filter_null_;
  distinct_algo_ = other.distinct_algo_;
  assignments_.reset();
  if (OB_FAIL(column_exprs_.assign(other.column_exprs_))) {
    LOG_WARN("failed to assign column exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.column_convert_exprs_, column_convert_exprs_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to copy exprs", K(ret));
  } else if (OB_FAIL(primary_key_ids_.assign(other.primary_key_ids_))) {
    LOG_WARN("failed to assign rowkey ids", K(ret));
  } else if (OB_FAIL(part_key_ids_.assign(other.part_key_ids_))) {
    LOG_WARN("failed to assign partkey ids", K(ret));
  } else if (OB_FAIL(assignments_.prepare_allocate(other.assignments_.count()))) {
    LOG_WARN("failed to prepare allocate assignment array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.assignments_.count(); ++i) {
    if (OB_FAIL(assignments_.at(i).deep_copy(expr_factory, other.assignments_.at(i)))) {
      LOG_WARN("failed to deep copy assignment", K(ret));
    }
  }
  return ret;
}

int ObDelUpdStmt::deep_copy_stmt_struct(
    ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& input)
{
  int ret = OB_SUCCESS;
  const ObDelUpdStmt& other = static_cast<const ObDelUpdStmt&>(input);
  if (OB_UNLIKELY(input.get_stmt_type() != get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret));
  } else if (OB_FAIL(ObDMLStmt::deep_copy_stmt_struct(stmt_factory, expr_factory, other))) {
    LOG_WARN("failed to deep copy stmt structure", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.returning_exprs_, returning_exprs_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to deep copy returning fileds", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.returning_into_exprs_, returning_into_exprs_, COPY_REF_DEFAULT))) {
    LOG_WARN("failed to deep copy returning into fields", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                 expr_factory, other.returning_agg_items_, returning_agg_items_, COPY_REF_SHARED))) {
    LOG_WARN("failed to deep copy returning aggregation exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_order_items(
                 expr_factory, other.order_items_, order_items_, COPY_REF_DEFAULT))) {
    LOG_WARN("deep copy order items failed", K(ret));
  } else if (OB_FAIL(returning_strs_.assign(other.returning_strs_))) {
    LOG_WARN("failed to assign returning strings", K(ret));
  } else if (OB_FAIL(all_table_columns_.prepare_allocate(other.all_table_columns_.count()))) {
    LOG_WARN("failed to prepare allocate table columns array", K(ret));
  } else {
    ignore_ = other.ignore_;
    is_returning_ = other.is_returning_;
    has_global_index_ = other.has_global_index_;
    dml_source_from_join_ = other.dml_source_from_join_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.all_table_columns_.count(); ++i) {
    if (OB_FAIL(all_table_columns_.at(i).deep_copy(expr_factory, other.all_table_columns_.at(i)))) {
      LOG_WARN("failed to deep copy all table columns", K(ret));
    }
  }
  return ret;
}

int ObDelUpdStmt::assign(const ObDelUpdStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLStmt::assign(other))) {
    LOG_WARN("failed to copy stmt", K(ret));
  } else if (OB_FAIL(all_table_columns_.assign(other.all_table_columns_))) {
    LOG_WARN("failed to assign all table columns", K(ret));
  } else if (OB_FAIL(returning_exprs_.assign(other.returning_exprs_))) {
    LOG_WARN("failed to assign returning exprs", K(ret));
  } else if (OB_FAIL(returning_strs_.assign(other.returning_strs_))) {
    LOG_WARN("failed to assign returning strs", K(ret));
  } else if (OB_FAIL(returning_agg_items_.assign(other.returning_agg_items_))) {
    LOG_WARN("failed to assign returning agg items", K(ret));
  } else if (OB_FAIL(returning_into_exprs_.assign(other.returning_into_exprs_))) {
    LOG_WARN("failed to assign returning into exprs", K(ret));
  } else {
    ignore_ = other.ignore_;
    is_returning_ = other.is_returning_;
    has_global_index_ = other.has_global_index_;
    dml_source_from_join_ = other.dml_source_from_join_;
  }
  return ret;
}

IndexDMLInfo* ObDelUpdStmt::get_table_dml_info(uint64_t table_id)
{
  IndexDMLInfo* primary_dml_info = NULL;
  TableColumns* table_columns = NULL;
  int64_t N = all_table_columns_.count();
  for (int64_t i = 0; table_columns == NULL && i < N; i++) {
    ObIArray<IndexDMLInfo>& index_infos = all_table_columns_.at(i).index_dml_infos_;
    if (!index_infos.empty() && table_id == index_infos.at(0).table_id_) {
      table_columns = &all_table_columns_.at(i);
    }
  }
  if (NULL != table_columns) {
    primary_dml_info = &(table_columns->index_dml_infos_.at(0));
  }
  return primary_dml_info;
}

int ObDelUpdStmt::check_dml_need_filter_null()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < joined_tables_.count(); ++i) {
    if (OB_FAIL(recursively_check_filter_null(joined_tables_.at(i)))) {
      LOG_WARN("recursively check filter null failed", K(ret), K_(joined_tables));
    }
  }
  return ret;
}

int ObDelUpdStmt::recursively_check_filter_null(const JoinedTable* cur_table)
{
  int ret = OB_SUCCESS;
  bool is_stack_verflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_verflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_verflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("recursively check filter null stack overflow", K(ret));
  } else if (OB_ISNULL(cur_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("current table is null", K(cur_table));
  } else if (LEFT_OUTER_JOIN == cur_table->joined_type_ || RIGHT_OUTER_JOIN == cur_table->joined_type_) {
    const TableItem* target_table =
        (LEFT_OUTER_JOIN == cur_table->joined_type_ ? cur_table->right_table_ : cur_table->left_table_);
    if (OB_ISNULL(target_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("right join table is null");
    } else if (target_table->is_basic_table()) {
      IndexDMLInfo* table_dml_info = get_table_dml_info(target_table->table_id_);
      if (table_dml_info != NULL) {
        table_dml_info->need_filter_null_ = true;
      }
    } else if (target_table->is_joined_table()) {
      const JoinedTable* target_join_table = static_cast<const JoinedTable*>(target_table);
      for (int64_t j = 0; OB_SUCC(ret) && j < target_join_table->single_table_ids_.count(); ++j) {
        uint64_t table_id = target_join_table->single_table_ids_.at(j);
        IndexDMLInfo* table_dml_info = get_table_dml_info(table_id);
        if (table_dml_info != NULL) {
          table_dml_info->need_filter_null_ = true;
        }
      }
    }
  } else if (FULL_OUTER_JOIN == cur_table->joined_type_) {
    for (int64_t j = 0; OB_SUCC(ret) && j < cur_table->single_table_ids_.count(); ++j) {
      uint64_t table_id = cur_table->single_table_ids_.at(j);
      IndexDMLInfo* table_dml_info = get_table_dml_info(table_id);
      if (table_dml_info != NULL) {
        table_dml_info->need_filter_null_ = true;
      }
    }
  } else {
    if (cur_table->left_table_ != NULL && cur_table->left_table_->is_joined_table()) {
      const JoinedTable* left_table = static_cast<const JoinedTable*>(cur_table->left_table_);
      if (OB_FAIL(SMART_CALL(recursively_check_filter_null(left_table)))) {
        LOG_WARN("recursively check filter null failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && cur_table->right_table_ != NULL && cur_table->right_table_->is_joined_table()) {
      const JoinedTable* right_table = static_cast<const JoinedTable*>(cur_table->right_table_);
      if (OB_FAIL(SMART_CALL(recursively_check_filter_null(right_table)))) {
        LOG_WARN("recursively check filter null failed", K(ret));
      }
    }
  }
  return ret;
}

IndexDMLInfo* ObDelUpdStmt::get_or_add_table_columns(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  IndexDMLInfo* primary_dml_info = NULL;
  TableColumns* table_columns = NULL;
  int64_t N = all_table_columns_.count();
  for (int64_t i = 0; table_columns == NULL && i < N; i++) {
    ObIArray<IndexDMLInfo>& index_infos = all_table_columns_.at(i).index_dml_infos_;
    if (!index_infos.empty() && table_id == index_infos.at(0).table_id_) {
      table_columns = &all_table_columns_.at(i);
    }
  }

  if (NULL == table_columns) {
    // if no table columns found, we need to add one
    TableColumns empty_tab_columns;
    IndexDMLInfo empty_dml_info;
    if (OB_FAIL(empty_tab_columns.index_dml_infos_.push_back(empty_dml_info))) {
      LOG_WARN("store empty index dml infos failed", K(ret));
    } else if (OB_FAIL(all_table_columns_.push_back(empty_tab_columns))) {
      LOG_WARN("failed to push back table columns", K(ret));
      table_columns = NULL;
    } else {
      table_columns = &(all_table_columns_.at(all_table_columns_.count() - 1));
    }
  }

  if (NULL != table_columns && table_columns->table_name_.length() <= 0) {
    TableItem* table_item = NULL;
    if (NULL != (table_item = get_table_item_by_id(table_id))) {
      table_columns->table_name_.assign_ptr(table_item->table_name_.ptr(), table_item->table_name_.length());
    }
  }
  if (NULL != table_columns) {
    primary_dml_info = &(table_columns->index_dml_infos_.at(0));
  }

  return primary_dml_info;
}

/**
 * @brief ObDelUpdStmt::update_base_tid_cid
 * update base table, column id for updatable view
 * @return
 */
int ObDelUpdStmt::update_base_tid_cid()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_all_table_columns().count(); ++i) {
    TableColumns& tab_cols = get_all_table_columns().at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < tab_cols.index_dml_infos_.count(); ++j) {
      IndexDMLInfo& index_info = tab_cols.index_dml_infos_.at(j);
      uint64_t base_tid = OB_INVALID_ID;
      for (int64_t k = 0; OB_SUCC(ret) && k < index_info.column_exprs_.count(); ++k) {
        ObColumnRefRawExpr* col = index_info.column_exprs_.at(k);
        ColumnItem* col_item = NULL;
        if (OB_ISNULL(col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column expr is null", K(ret), K(col));
        } else if (IS_SHADOW_COLUMN(col->get_column_id())) {
          // do nothing
        } else if (OB_ISNULL(col_item = get_column_item_by_id(col->get_table_id(), col->get_column_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column item is not found", K(ret), K(*col), K(col_item));
        } else if (OB_FAIL(ObTransformUtils::get_base_column(this, col))) {
          LOG_WARN("failed to get column base info", K(ret));
        } else if (OB_ISNULL(col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(col), K(ret));
        } else {
          col_item->base_tid_ = col->get_table_id();
          col_item->base_cid_ = col->get_column_id();
          if (OB_UNLIKELY(col_item->base_tid_ == OB_INVALID_ID) ||
              OB_UNLIKELY(k != 0 && col_item->base_tid_ != base_tid)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("base table id is invalid", K(ret), K(col_item->base_tid_), K(base_tid));
          } else if (k == 0) {
            base_tid = col_item->base_tid_;
          }
        }
      }
      for (int64_t k = 0; OB_SUCC(ret) && k < index_info.assignments_.count(); ++k) {
        ObColumnRefRawExpr* col = index_info.assignments_.at(k).column_expr_;
        ColumnItem* col_item = NULL;
        if (OB_ISNULL(col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column expr is null", K(ret), K(col));
        } else if (IS_SHADOW_COLUMN(col->get_column_id())) {
          // do nothing
        } else if (OB_ISNULL(col_item = get_column_item_by_id(col->get_table_id(), col->get_column_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get column item", K(ret), K(col_item));
        } else {
          index_info.assignments_.at(k).base_table_id_ = col_item->base_tid_;
          index_info.assignments_.at(k).base_column_id_ = col_item->base_cid_;
        }
      }
      if (OB_SUCC(ret) && index_info.loc_table_id_ != base_tid) {
        for (int64_t k = 0; OB_SUCC(ret) && k < part_expr_items_.count(); ++k) {
          if (part_expr_items_.at(k).table_id_ == index_info.loc_table_id_ &&
              part_expr_items_.at(k).index_tid_ == index_info.index_tid_) {
            part_expr_items_.at(k).table_id_ = base_tid;
          }
        }
        index_info.loc_table_id_ = base_tid;
      }
    }
  }
  return ret;
}

int ObDelUpdStmt::replace_inner_stmt_expr(
    const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLStmt::replace_inner_stmt_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace inner stmt expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, returning_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, returning_into_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, returning_agg_items_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_table_columns_.count(); i++) {
      ObIArray<IndexDMLInfo>& index_dml_infos = all_table_columns_.at(i).index_dml_infos_;
      for (int64_t j = 0; OB_SUCC(ret) && j < index_dml_infos.count(); j++) {
        IndexDMLInfo& index_dml_info = index_dml_infos.at(j);
        if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, index_dml_info.column_exprs_))) {
          LOG_WARN("failed to replace exprs", K(ret));
        } else if (OB_FAIL(
                       ObTransformUtils::replace_exprs(other_exprs, new_exprs, index_dml_info.column_convert_exprs_))) {
          LOG_WARN("failed to replace exprs", K(ret));
        } else if (OB_FAIL(replace_table_assign_exprs(other_exprs, new_exprs, index_dml_info.assignments_))) {
          LOG_WARN("failed to replace exprs", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObDelUpdStmt::replace_table_assign_exprs(const common::ObIArray<ObRawExpr*>& other_exprs,
    const common::ObIArray<ObRawExpr*>& new_exprs, ObAssignments& assignments)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); i++) {
    if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, assignments.at(i).expr_))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(
                   other_exprs, new_exprs, reinterpret_cast<ObRawExpr*&>(assignments.at(i).column_expr_)))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObDelUpdStmt::inner_get_relation_exprs(RelExprCheckerBase& expr_checker)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_table_columns_.count(); ++i) {
    TableColumns& table_columns = all_table_columns_.at(i);
    ObIArray<ObColumnRefRawExpr*>& column_exprs = table_columns.index_dml_infos_.at(0).column_exprs_;
    for (int64_t j = 0; OB_SUCC(ret) && j < column_exprs.count(); ++j) {
      ObRawExpr*& column_expr = reinterpret_cast<ObRawExpr*&>(column_exprs.at(j));
      if (OB_FAIL(expr_checker.add_expr(column_expr))) {
        LOG_WARN("add assign expr to relation exprs failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(expr_checker.add_exprs(table_columns.index_dml_infos_.at(0).column_convert_exprs_))) {
        LOG_WARN("fail to add expr to expr checker", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < returning_exprs_.count(); i++) {
    ObRawExpr*& returning_expr = reinterpret_cast<ObRawExpr*&>(returning_exprs_.at(i));
    if (OB_FAIL(expr_checker.add_expr(returning_expr))) {
      LOG_WARN("add expr to relation expr failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < returning_into_exprs_.count(); i++) {
    ObRawExpr*& returning_into_expr = reinterpret_cast<ObRawExpr*&>(returning_into_exprs_.at(i));
    if (OB_FAIL(expr_checker.add_expr(returning_into_expr))) {
      LOG_WARN("add expr to relation expr failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < check_constraint_exprs_.count(); i++) {
    ObRawExpr*& check_constraint_expr = reinterpret_cast<ObRawExpr*&>(check_constraint_exprs_.at(i));
    if (OB_FAIL(expr_checker.add_expr(check_constraint_expr))) {
      LOG_WARN("add expr to relation expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDMLStmt::inner_get_relation_exprs(expr_checker))) {
      LOG_WARN("get delup stmt relation exprs failed", K(ret));
    }
  }
  return ret;
}

int ObDelUpdStmt::add_multi_table_dml_info(const IndexDMLInfo& index_dml_info)
{
  int ret = OB_SUCCESS;
  TableColumns* table_column = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && NULL == table_column && i < all_table_columns_.count(); ++i) {
    if (!all_table_columns_.at(i).index_dml_infos_.empty() &&
        all_table_columns_.at(i).index_dml_infos_.at(0).table_id_ == index_dml_info.table_id_) {
      table_column = &(all_table_columns_.at(i));
    }
  }
  CK(OB_NOT_NULL(table_column));
  if (OB_SUCC(ret)) {
    OC((table_column->index_dml_infos_.push_back)(index_dml_info));
  }
  return ret;
}

int ObDelUpdStmt::find_index_column(
    uint64_t table_id, uint64_t index_id, uint64_t column_id, ObColumnRefRawExpr*& col_expr)
{
  int ret = OB_SUCCESS;
  TableColumns* table_column = NULL;
  col_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && NULL == col_expr && i < all_table_columns_.count(); ++i) {
    if (!all_table_columns_.at(i).index_dml_infos_.empty() &&
        all_table_columns_.at(i).index_dml_infos_.at(0).table_id_ == table_id) {
      ObIArray<IndexDMLInfo>& index_dml_infos = all_table_columns_.at(i).index_dml_infos_;
      for (int64_t j = 0; OB_SUCC(ret) && NULL == col_expr && j < index_dml_infos.count(); j++) {
        IndexDMLInfo index_dml_info = index_dml_infos.at(j);
        if (index_dml_info.table_id_ == table_id && index_dml_info.index_tid_ == index_id) {
          for (int64_t k = 0; OB_SUCC(ret) && NULL == col_expr && k < index_dml_info.column_exprs_.count(); k++) {
            ObColumnRefRawExpr* col = index_dml_info.column_exprs_.at(k);
            CK(OB_NOT_NULL(col));
            if (OB_SUCC(ret) && col->get_column_id() == column_id) {
              col_expr = col;
            }
          }  // for col end
        }
      }  // for index end
    }
  }  // for table end

  return ret;
}

bool ObDelUpdStmt::check_table_be_modified(uint64_t ref_table_id) const
{
  int ret = OB_SUCCESS;
  bool is_exists = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_exists && i < all_table_columns_.count(); ++i) {
    if (all_table_columns_.at(i).index_dml_infos_.at(0).index_tid_ == ref_table_id) {
      is_exists = true;
      break;
    }
  }
  LOG_DEBUG("check duplicate table being modified", K(is_exists), K(ref_table_id), K(all_table_columns_.count()));
  return is_exists;
}

const ObIArray<TableColumns>* ObDelUpdStmt::get_slice_from_all_table_columns(
    ObIAllocator& allocator, int64_t table_idx, int64_t index_idx) const
{
  int ret = OB_SUCCESS;
  typedef ObSEArray<TableColumns, 1> OneTableColumns;
  ObIArray<TableColumns>* one_table_columns = nullptr;
  void* buf = nullptr;
  if (table_idx < 0 || table_idx >= all_table_columns_.count()) {
    LOG_WARN("invalid table idx", K(table_idx), K(index_idx));
  } else if (index_idx < 0 || index_idx >= all_table_columns_.at(table_idx).index_dml_infos_.count()) {
    LOG_WARN("invalid index idx", K(table_idx), K(index_idx));
  } else if (nullptr == (buf = allocator.alloc(sizeof(OneTableColumns)))) {
    LOG_WARN("no memory", KP(buf));
  } else {
    one_table_columns = new (buf) OneTableColumns();
    TableColumns table_columns;
    table_columns.table_name_ = all_table_columns_.at(table_idx).table_name_;
    if (OB_FAIL(table_columns.index_dml_infos_.push_back(
            all_table_columns_.at(table_idx).index_dml_infos_.at(index_idx)))) {
      LOG_WARN("fail push back index dml info", K(ret));
    } else if (OB_FAIL(one_table_columns->push_back(table_columns))) {
      LOG_WARN("fail pushback table columns", K(table_columns));
    }
  }

  if (OB_FAIL(ret)) {
    one_table_columns = nullptr;
  }
  return one_table_columns;
}

int64_t TableColumns::to_explain_string(char* buf, int64_t buf_len, ExplainType type) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  BUF_PRINTF("{");
  pos += table_name_.to_string(buf + pos, buf_len - pos);
  int64_t N = index_dml_infos_.count();
  BUF_PRINTF(": (");
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    pos += index_dml_infos_.at(i).to_explain_string(buf + pos, buf_len - pos, type);
    if (i < N - 1) {
      BUF_PRINTF(", ");
    }
  }
  BUF_PRINTF(")");
  if (T_MERGE_DISTINCT == index_dml_infos_.at(0).distinct_algo_) {
    BUF_PRINTF(", merge_distinct");
  } else if (T_HASH_DISTINCT == index_dml_infos_.at(0).distinct_algo_) {
    BUF_PRINTF(", hash_distinct");
  }
  BUF_PRINTF("}");

  return pos;
}

int64_t IndexDMLInfo::to_explain_string(char* buf, int64_t buf_len, ExplainType type) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  BUF_PRINTF("{");
  if (index_name_.empty()) {
    BUF_PRINTF("%lu: ", index_tid_);
  } else {
    pos += index_name_.to_string(buf + pos, buf_len - pos);
  }
  int64_t N = column_exprs_.count();
  BUF_PRINTF(": (");
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    if (NULL != column_exprs_.at(i)) {
      if (OB_SUCC(ret)) {
        if (OB_FAIL(column_exprs_.at(i)->get_name(buf, buf_len, pos, type))) {
          LOG_WARN("failed to get_name", K(ret));
        }
      }
      if (i < N - 1) {
        BUF_PRINTF(", ");
      }
    }
  }
  BUF_PRINTF(")");
  BUF_PRINTF("}");

  return pos;
}

int IndexDMLInfo::init_assignment_info(const ObAssignments& assignments)
{
  int ret = OB_SUCCESS;
  assignments_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
    if (has_exist_in_array(column_exprs_, assignments.at(i).column_expr_)) {
      if (OB_FAIL(assignments_.push_back(assignments.at(i)))) {
        LOG_WARN("add assignment index to assign info failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(init_column_convert_expr(assignments_))) {
    LOG_WARN("fail init column convert exprs", K(ret));
  }
  return ret;
}

int IndexDMLInfo::init_column_convert_expr(const ObAssignments& assignments)
{
  int ret = OB_SUCCESS;
  int found = 0;
  column_convert_exprs_.reset();
  for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); ++i) {
    ObRawExpr* insert_expr = column_exprs_.at(i);
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
    } else if (OB_FAIL(column_convert_exprs_.push_back(insert_expr))) {
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

int IndexDMLInfo::add_spk_assignment_info(ObRawExprFactory& expr_factory)
{
  int ret = OB_SUCCESS;
  int64_t org_assignment_cnt = assignments_.count();
  if (0 == org_assignment_cnt) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count(); ++i) {
      ObColumnRefRawExpr* col_expr = column_exprs_.at(i);
      if (OB_ISNULL(col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is null", K(ret));
      } else if (is_shadow_column(col_expr->get_column_id())) {
        const ObRawExpr* spk_expr = col_expr->get_dependant_expr();
        ObRawExpr* new_spk_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, spk_expr, new_spk_expr, COPY_REF_DEFAULT))) {
          LOG_WARN("fail to copy part expr", K(ret));
        } else if (OB_FAIL(ObTableAssignment::expand_expr(assignments_, new_spk_expr))) {
          LOG_WARN("fail to expand expr", K(ret));
        }
        ObAssignment assignment;
        assignment.column_expr_ = col_expr;
        assignment.expr_ = new_spk_expr;
        assignment.base_table_id_ = col_expr->get_table_id();
        assignment.base_column_id_ = col_expr->get_column_id();
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(assignments_.push_back(assignment))) {
          LOG_WARN("fail to push spk assignements", K(ret), K(assignment));
        } else {
          LOG_DEBUG("spk assignment", K(assignment));
        }
      }
    }
  }

  return ret;
}

int ObDelUpdStmt::inner_get_share_exprs(ObIArray<ObRawExpr*>& candi_share_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLStmt::inner_get_share_exprs(candi_share_exprs))) {
    LOG_WARN("failed to mark share exprs", K(ret));
  } else if (OB_FAIL(append(candi_share_exprs, returning_agg_items_))) {
    LOG_WARN("failed to append returning aggr items", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_table_columns_.count(); i++) {
    const ObIArray<IndexDMLInfo>& index_dml_infos = all_table_columns_.at(i).index_dml_infos_;
    for (int64_t j = 0; OB_SUCC(ret) && j < index_dml_infos.count(); j++) {
      const IndexDMLInfo& index_dml_info = index_dml_infos.at(j);
      if (OB_FAIL(append(candi_share_exprs, index_dml_info.column_exprs_))) {
        LOG_WARN("failed to replace exprs", K(ret));
      }
    }
  }
  return ret;
}

uint64_t ObDelUpdStmt::get_insert_table_id(uint64_t table_offset /*default 0*/) const
{
  uint64_t table_id = OB_INVALID_ID;
  if (table_items_.count() >= 1 && table_offset < table_items_.count() && table_items_.at(table_offset) != NULL) {
    table_id = table_items_.at(table_offset)->table_id_;
  }
  return table_id;
}

uint64_t ObDelUpdStmt::get_insert_base_tid(uint64_t table_offset /*default 0*/) const
{
  uint64_t tid = OB_INVALID_ID;
  if (table_items_.count() >= 1 && table_offset < table_items_.count() && table_items_.at(table_offset) != NULL) {
    tid = table_items_.at(table_offset)->get_base_table_item().ref_id_;
  }
  return tid;
}

uint64_t ObDelUpdStmt::get_ref_table_id() const
{
  uint64_t ref_table_id = common::OB_INVALID_ID;
  if (all_table_columns_.count() >= 1) {
    if (all_table_columns_.at(0).index_dml_infos_.count() >= 1) {
      ref_table_id = all_table_columns_.at(0).index_dml_infos_.at(0).index_tid_;
    }
  }
  return ref_table_id;
}
