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

#include "sql/ob_sql_temp_table.h"
#include "sql/rewrite/ob_predicate_deduce.h"
#include "sql/ob_sql_context.h"
#include "sql/optimizer/ob_optimizer_util.h"
#define USING_LOG_PREFIX SQL_OPT

using namespace oceanbase::sql;

OB_SERIALIZE_MEMBER(ObTempTableResultInfo,
                    addr_,
                    interm_result_ids_);

OB_SERIALIZE_MEMBER(ObSqlTempTableCtx,
                    interm_result_infos_,
                    temp_table_id_,
                    is_local_interm_result_);

int ObSqlTempTableInfo::collect_temp_tables(ObIAllocator &allocator,
                                            ObDMLStmt &stmt,
                                            ObIArray<ObSqlTempTableInfo*> &temp_table_infos,
                                            ObQueryCtx *query_ctx,
                                            bool do_collect_filter)
{
  int ret = OB_SUCCESS;
  ObSqlTempTableInfo *temp_table_info = NULL;
  void *ptr = NULL;
  TableItem *table = NULL;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
    if (OB_ISNULL(child_stmts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(SMART_CALL(collect_temp_tables(allocator, *child_stmts.at(i),
                                                      temp_table_infos, query_ctx, do_collect_filter)))) {
      LOG_WARN("failed to add all temp tables", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_items().count(); i++) {
    bool find = true;
    if (OB_ISNULL(table = stmt.get_table_items().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!table->is_temp_table()) {
      //do nothing
    } else {
      find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < temp_table_infos.count(); j++) {
        ObSqlTempTableInfo* info = temp_table_infos.at(j);
        if (OB_ISNULL(info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null info", K(ret));
        } else if (info->table_query_ == table->ref_query_) {
          find = true;
          table->ref_id_ = info->temp_table_id_;

          TableInfo table_info;
          table_info.upper_stmt_ = &stmt;
          table_info.table_item_ = table;
          if (do_collect_filter &&
              OB_FAIL(collect_temp_table_filters(table_info.upper_stmt_,
                                                 table_info.table_item_,
                                                 table_info.table_filters_,
                                                 table_info.filter_conditions_))) {
            LOG_WARN("failed to collect temp table info", K(ret));
          } else if (OB_FAIL(info->table_infos_.push_back(table_info))) {
            LOG_WARN("failed to push back table info", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && !find) {
      if (OB_ISNULL(table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(collect_temp_tables(allocator, *table->ref_query_,
                                                        temp_table_infos, query_ctx, do_collect_filter)))) {
        LOG_WARN("failed to add all temp tables", K(ret));
      } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObSqlTempTableInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        temp_table_info = new (ptr) ObSqlTempTableInfo();
        table->ref_id_ = (NULL == query_ctx) ? OB_INVALID_ID : query_ctx->available_tb_id_--;
        temp_table_info->temp_table_id_ = table->ref_id_;
        temp_table_info->table_name_ = table->table_name_;
        temp_table_info->table_query_ = table->ref_query_;

        TableInfo table_info;
        table_info.upper_stmt_ = &stmt;
        table_info.table_item_ = table;
        if (do_collect_filter &&
            OB_FAIL(collect_temp_table_filters(table_info.upper_stmt_,
                                               table_info.table_item_,
                                               table_info.table_filters_,
                                               table_info.filter_conditions_))) {
          LOG_WARN("failed to collect temp table info", K(ret));
        } else if (OB_FAIL(temp_table_info->table_infos_.push_back(table_info))) {
          LOG_WARN("failed to push back table item", K(ret));
        } else if (OB_FAIL(temp_table_infos.push_back(temp_table_info))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSqlTempTableInfo::collect_specified_temp_table(ObIAllocator &allocator,
                                                     ObSelectStmt *specified_query,
                                                     const ObIArray<ObDMLStmt *> &upper_stmts,
                                                     const ObIArray<TableItem *> &table_items,
                                                     ObSqlTempTableInfo &temp_table_info,
                                                     bool &all_has_filter)
{
  int ret = OB_SUCCESS;
  temp_table_info.reset();
  if (OB_ISNULL(specified_query) ||
      OB_UNLIKELY(upper_stmts.count() != table_items.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(specified_query), K(upper_stmts), K(table_items));;
  } else {
    temp_table_info.table_query_ = specified_query;
    all_has_filter = upper_stmts.count() > 0;
    for (int64_t i = 0; OB_SUCC(ret) && all_has_filter && i < upper_stmts.count(); i ++) {
      TableItem *table = table_items.at(i);
      ObDMLStmt *stmt = upper_stmts.at(i);
      if (OB_ISNULL(stmt) || OB_ISNULL(table) ||
          OB_UNLIKELY(specified_query != table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected params", K(table_items), K(upper_stmts));
      } else {
        TableInfo table_info;
        table_info.upper_stmt_ = stmt;
        table_info.table_item_ = table;
        if (OB_FAIL(collect_temp_table_filters(table_info.upper_stmt_,
                                               table_info.table_item_,
                                               table_info.table_filters_,
                                               table_info.filter_conditions_))) {
          LOG_WARN("failed to collect temp table info", K(ret));
        } else if (OB_FAIL(temp_table_info.table_infos_.push_back(table_info))) {
          LOG_WARN("failed to push back table info", K(ret));
        } else {
          all_has_filter &= !table_info.table_filters_.empty();
        }
      }
    }
  }
  return ret;
}

int ObSqlTempTableInfo::collect_temp_table_filters(ObDMLStmt *stmt,
                                                   TableItem *table,
                                                   ObIArray<ObRawExpr*> &table_filters,
                                                   ObIArray<ObRawExprCondition*> &filter_conditions)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_ids;
  int32_t table_idx = OB_INVALID_INDEX;
  uint64_t table_id = OB_INVALID_ID;
  if (OB_ISNULL(stmt) || OB_ISNULL(table) || OB_ISNULL(table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (OB_FALSE_IT(table_idx = stmt->get_table_bit_index(table->table_id_))) {
  } else if (OB_FAIL(table_ids.add_member(table_idx))) {
    LOG_WARN("failed to add member", K(table_idx), K(ret));
  } else if (OB_FAIL(get_candi_exprs(table_ids,
                                     stmt->get_condition_exprs(),
                                     table_filters,
                                     filter_conditions))) {
    LOG_WARN("failed to get candi exprs", K(ret));
  } else {
    table_id = table->table_id_;
  }
  //如果是joined table内部表，如果在左侧，则可以使用where condition、
  //如果在右侧，则不能使用where condition，选择可以使用的on condition
  bool find = false;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < stmt->get_from_item_size(); ++i) {
    FromItem &from = stmt->get_from_item(i);
    if (from.table_id_ == table_id) {
      find = true;
    } else if (from.is_joined_) {
      JoinedTable *joined_table = stmt->get_joined_table(from.table_id_);
      if (OB_ISNULL(joined_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (!ObOptimizerUtil::find_item(joined_table->single_table_ids_, table_id)) {
        //do nothing
      } else if (OB_FAIL(collect_table_filters_in_joined_table(joined_table,
                                                               table_id,
                                                               table_ids,
                                                               table_filters,
                                                               filter_conditions))) {
        LOG_WARN("failed to get table filters", K(ret));
      } else {
        find = true;
      }
    }
  }
  return ret;
}

int ObSqlTempTableInfo::collect_table_filters_in_joined_table(JoinedTable *table,
                                                              uint64_t table_id,
                                                              const ObSqlBitSet<> &table_ids,
                                                              ObIArray<ObRawExpr*> &table_filters,
                                                              ObIArray<ObRawExprCondition*> &filter_conditions)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> candi_filters;
  bool in_left = false;
  bool in_right = false;
  if (OB_ISNULL(table) || OB_ISNULL(table->left_table_) ||
      OB_ISNULL(table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (table->left_table_->is_joined_table()) {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table->left_table_);
    if (ObOptimizerUtil::find_item(joined_table->single_table_ids_, table_id)) {
      in_left = true;
    }
  } else if (!table->left_table_->is_joined_table()) {
    if (table_id == table->left_table_->table_id_) {
      in_left = true;
    }
  }
  if (OB_SUCC(ret) && !in_left) {
    if (table->right_table_->is_joined_table()) {
      JoinedTable *joined_table = static_cast<JoinedTable*>(table->right_table_);
      if (ObOptimizerUtil::find_item(joined_table->single_table_ids_, table_id)) {
        in_right = true;
      }
    } else if (!table->right_table_->is_joined_table()) {
      if (table_id == table->right_table_->table_id_) {
        in_right = true;
      }
    }
  }
  if (OB_SUCC(ret) && in_left) {
    if (INNER_JOIN == table->joined_type_) {
      if (OB_FAIL(get_candi_exprs(table_ids,
                                  table->join_conditions_,
                                  table_filters,
                                  filter_conditions))) {
        LOG_WARN("failed to get candi exprs", K(ret));
      }
    } else if (LEFT_OUTER_JOIN == table->joined_type_) {
      //do nothing
    } else if (RIGHT_OUTER_JOIN == table->joined_type_) {
      table_filters.reuse();
      filter_conditions.reuse();
      if (OB_FAIL(get_candi_exprs(table_ids,
                                  table->join_conditions_,
                                  table_filters,
                                  filter_conditions))) {
        LOG_WARN("failed to get candi exprs", K(ret));
      }
    } else {
      table_filters.reuse();
      filter_conditions.reuse();
    }
    if (OB_SUCC(ret) && table->left_table_->is_joined_table()) {
      JoinedTable *joined_table =  static_cast<JoinedTable*>(table->left_table_);
      if (OB_FAIL(SMART_CALL(collect_table_filters_in_joined_table(joined_table,
                                                                   table_id,
                                                                   table_ids,
                                                                   table_filters,
                                                                   filter_conditions)))) {
        LOG_WARN("failed to get table filters", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && in_right) {
    if (INNER_JOIN == table->joined_type_) {
      if (OB_FAIL(get_candi_exprs(table_ids,
                                  table->join_conditions_,
                                  table_filters,
                                  filter_conditions))) {
        LOG_WARN("failed to get candi exprs", K(ret));
      }
    } else if (LEFT_OUTER_JOIN == table->joined_type_) {
      table_filters.reuse();
      filter_conditions.reuse();
      if (OB_FAIL(get_candi_exprs(table_ids,
                                  table->join_conditions_,
                                  table_filters,
                                  filter_conditions))) {
        LOG_WARN("failed to get candi exprs", K(ret));
      }
    } else if (RIGHT_OUTER_JOIN == table->joined_type_) {
      //do nothing
    } else {
      table_filters.reuse();
      filter_conditions.reuse();
    }
    if (OB_SUCC(ret) && table->right_table_->is_joined_table()) {
      JoinedTable *joined_table =  static_cast<JoinedTable*>(table->right_table_);
      if (OB_FAIL(SMART_CALL(collect_table_filters_in_joined_table(joined_table,
                                                                   table_id,
                                                                   table_ids,
                                                                   table_filters,
                                                                   filter_conditions)))) {
        LOG_WARN("failed to get table filters", K(ret));
      }
    }
  }
  return ret;
}

int ObSqlTempTableInfo::get_candi_exprs(const ObSqlBitSet<> &table_ids,
                                        ObIArray<ObRawExpr*> &exprs,
                                        ObIArray<ObRawExpr*> &candi_exprs,
                                        ObIArray<ObRawExprCondition*> &candi_conditions)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (ObPredicateDeduce::contain_special_expr(*expr)) {
      // do nothing
    } else if (expr->has_flag(CNT_DYNAMIC_PARAM)) {
      //do nothing
    } else if (!expr->get_relation_ids().is_subset(table_ids)) {
      //do nothing
    } else if (OB_FAIL(candi_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(candi_conditions.push_back(&exprs))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}
