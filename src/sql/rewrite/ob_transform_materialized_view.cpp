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

#define USING_LOG_PREFIX SQL_REWRITE

#include "ob_transform_materialized_view.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "common/ob_common_utility.h"
#include "ob_transformer_impl.h"
#include "sql/parser/ob_item_type.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/ob_sql_context.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
using namespace share::schema;

uint64_t ObTransformMaterializedView::get_real_tid(uint64_t tid, ObSelectStmt& stmt)
{
  uint64_t real_tid = OB_INVALID_ID;
  for (int64_t i = 0; i < stmt.get_table_size(); i++) {
    TableItem* table_item = stmt.get_table_item(i);
    if (tid == table_item->table_id_) {
      real_tid = table_item->ref_id_;
      break;
    }
  }
  return real_tid;
}

int ObTransformMaterializedView::is_col_in_mv(uint64_t tid, uint64_t cid, MVDesc mv_desc, bool& result)
{
  int ret = OB_SUCCESS;
  result = false;

  const ObTableSchema* table_schema = NULL;
  if (OB_FAIL(ctx_->schema_checker_->get_table_schema(mv_desc.mv_tid_, table_schema))) {
    LOG_WARN("fail to get table schema", K(mv_desc.mv_tid_));
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table schema", K(mv_desc.mv_tid_));
  } else {
    for (int64_t i = 0; i < table_schema->get_column_count(); i++) {
      const ObColumnSchemaV2* column_schema = table_schema->get_column_schema_by_idx(i);
      uint64_t tmp_cid = cid;
      if (tid != mv_desc.base_tid_) {
        tmp_cid = ObTableSchema::gen_materialized_view_column_id(tmp_cid);
      }
      if (tmp_cid == column_schema->get_column_id()) {
        result = true;
        break;
      }
    }
  }

  return ret;
}

int ObTransformMaterializedView::is_all_column_covered(
    ObSelectStmt& stmt, MVDesc mv_desc, const JoinTableIdPair& idp, bool& result)
{
  int ret = OB_SUCCESS;
  result = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_column_size(); i++) {
    const ColumnItem* ci = stmt.get_column_item(i);
    if (ci->table_id_ == idp.first || ci->table_id_ == idp.second) {
      bool flag = false;
      uint64_t real_tid = get_real_tid(ci->table_id_, stmt);
      if (OB_FAIL(is_col_in_mv(real_tid, ci->column_id_, mv_desc, flag))) {
        SQL_REWRITE_LOG(WARN, "fail to do is_col_in_mv", K(ret));
      } else if (!flag) {
        result = false;
        break;
      }
    }
  }
  return ret;
}

int ObTransformMaterializedView::expr_equal(
    ObSelectStmt* select_stmt, const ObRawExpr* r1, ObSelectStmt* mv_stmt, const ObRawExpr* r2, bool& result)
{
  int ret = OB_SUCCESS;
  result = false;
  if (r1->get_expr_class() != r2->get_expr_class()) {
    result = false;
  } else {
    switch (r1->get_expr_class()) {
      case ObRawExpr::EXPR_OPERATOR:
        if (r1->get_expr_type() != T_OP_EQ || r2->get_expr_type() != T_OP_EQ) {
          result = false;
        } else {
          if (r1->get_param_count() != 2 || r2->get_param_count() != 2) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            if (OB_FAIL(expr_equal(select_stmt, r1->get_param_expr(0), mv_stmt, r2->get_param_expr(0), result))) {
              SQL_REWRITE_LOG(WARN, "fail to do expr equal", K(ret));
            } else if (result) {
              if (OB_FAIL(expr_equal(select_stmt, r1->get_param_expr(1), mv_stmt, r2->get_param_expr(1), result))) {
                SQL_REWRITE_LOG(WARN, "fail to do expr equal", K(ret));
              }
            }

            if (OB_SUCC(ret) && !result) {
              if (OB_FAIL(expr_equal(select_stmt, r1->get_param_expr(0), mv_stmt, r2->get_param_expr(1), result))) {
                SQL_REWRITE_LOG(WARN, "fail to do expr equal", K(ret));
              } else if (result) {
                if (OB_FAIL(expr_equal(select_stmt, r1->get_param_expr(1), mv_stmt, r2->get_param_expr(0), result))) {
                  SQL_REWRITE_LOG(WARN, "fail to do expr equal", K(ret));
                }
              }
            }
          }
        }
        break;
      case ObRawExpr::EXPR_COLUMN_REF: {
        const ObColumnRefRawExpr* col_expr1 = static_cast<const ObColumnRefRawExpr*>(r1);
        const ObColumnRefRawExpr* col_expr2 = static_cast<const ObColumnRefRawExpr*>(r2);
        result = true;
        uint64_t tid1 = get_real_tid(col_expr1->get_table_id(), *select_stmt);
        uint64_t tid2 = get_real_tid(col_expr2->get_table_id(), *mv_stmt);
        if (tid1 != tid2) {
          result = false;
        } else {
          uint64_t cid1 = col_expr1->get_column_id();
          uint64_t cid2 = col_expr2->get_column_id();
          if (cid1 != cid2) {
            result = false;
          }
        }
        break;
      }
      default:
        result = false;
        break;
    }
  }
  return ret;
}

int ObTransformMaterializedView::check_where_condition_covered(
    ObSelectStmt* select_stmt, ObSelectStmt* mv_stmt, bool& result, ObBitSet<>& expr_idx)
{
  int ret = OB_SUCCESS;
  result = true;
  if (OB_FAIL(expr_idx.reserve(select_stmt->get_condition_size()))) {
    SQL_REWRITE_LOG(WARN, "fail to reserve space for bit set", K(ret), K(select_stmt->get_condition_size()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_stmt->get_condition_size(); i++) {
    const ObRawExpr* expr = mv_stmt->get_condition_expr(i);
    bool flag = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < select_stmt->get_condition_size(); j++) {
      if (!expr_idx.has_member(j)) {
        const ObRawExpr* select_expr = select_stmt->get_condition_expr(j);
        bool equal = false;
        if (OB_FAIL(expr_equal(select_stmt, select_expr, mv_stmt, expr, equal))) {
          SQL_REWRITE_LOG(WARN, "", K(ret));
        } else if (equal) {
          if (OB_FAIL(expr_idx.add_member(j))) {
            SQL_REWRITE_LOG(WARN, "fail to add member", K(ret));
          } else {
            flag = true;
            break;
          }
        }
      }
    }
    if (!flag) {
      result = false;
      break;
    }
  }
  return ret;
}

int ObTransformMaterializedView::get_column_id(uint64_t mv_tid, uint64_t tid, uint64_t cid, uint64_t& mv_cid)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  if (OB_FAIL(ctx_->schema_checker_->get_table_schema(mv_tid, table_schema))) {
    LOG_WARN("fail to get table schema", K(mv_tid), K(tid), K(cid), K(ret));
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table schema", K(mv_tid), K(tid), K(cid), K(ret));
  } else {
    if (tid == table_schema->get_data_table_id()) {
      mv_cid = cid;
    } else {
      mv_cid = table_schema->gen_materialized_view_column_id(cid);
    }
  }
  return ret;
}

int ObTransformMaterializedView::rewrite_column_id(
    ObSelectStmt* select_stmt, MVDesc mv_desc, const JoinTableIdPair& idp)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_column_size(); i++) {
    ColumnItem* ci = select_stmt->get_column_item(i);
    if (ci->table_id_ == idp.first || ci->table_id_ == idp.second) {
      uint64_t mv_cid = 0;
      if (OB_FAIL(get_column_id(mv_desc.mv_tid_, get_real_tid(ci->table_id_, *select_stmt), ci->column_id_, mv_cid))) {
        LOG_WARN("fail to get column id", K(ret));
      } else {
        ci->expr_->set_ref_id(mv_desc.mv_tid_, mv_cid);
        ci->expr_->set_table_name(mv_desc.table_name_);
        ci->table_id_ = mv_desc.mv_tid_;
        ci->column_id_ = mv_cid;
      }
    }
  }
  return ret;
}

int ObTransformMaterializedView::rewrite_column_id2(ObSelectStmt* select_stmt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_column_size(); i++) {
    ColumnItem* ci = select_stmt->get_column_item(i);
    ci->expr_->get_relation_ids().reuse();
    if (OB_FAIL(ci->expr_->add_relation_id(select_stmt->get_table_bit_index(ci->expr_->get_table_id())))) {
      LOG_WARN("add relation id to expr failed", K(ret));
    }
  }
  return ret;
}

int ObTransformMaterializedView::get_join_tid_cid(
    uint64_t tid, uint64_t cid, uint64_t& out_tid, uint64_t& out_cid, const ObTableSchema& mv_schema)
{
  int ret = OB_SUCCESS;
  out_tid = OB_INVALID_ID;
  out_cid = OB_INVALID_ID;
  const common::ObIArray<std::pair<uint64_t, uint64_t>>& join_conds = mv_schema.get_join_conds();
  std::pair<uint64_t, uint64_t> j1;
  std::pair<uint64_t, uint64_t> j2;
  for (int64_t i = 0; OB_SUCC(ret) && i < join_conds.count(); i += 2) {
    j1 = join_conds.at(i);
    j2 = join_conds.at(i + 1);
    if (j1.first == tid && j1.second == cid) {
      out_tid = j2.first;
      out_cid = j2.second;
    } else if (j2.first == tid && j2.second == cid) {
      out_tid = j1.first;
      out_cid = j1.second;
    }
  }
  return ret;
}

int ObTransformMaterializedView::is_rowkey_equal_covered(
    ObSelectStmt* select_stmt, uint64_t mv_tid, uint64_t rowkey_tid, uint64_t base_or_dep_tid, bool& is_covered)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> rowkey_covered_set;
  is_covered = false;

  const ObTableSchema* mv_schema = NULL;
  const ObTableSchema* table_schema = NULL;
  const ObRowkeyInfo* rowkey_info = NULL;
  if (OB_FAIL(ctx_->schema_checker_->get_table_schema(rowkey_tid, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(rowkey_tid));
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is null", K(rowkey_tid));
  } else {
    rowkey_info = &(table_schema->get_rowkey_info());
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx_->schema_checker_->get_table_schema(mv_tid, mv_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(mv_tid));
    } else if (NULL == mv_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is null", K(mv_tid));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_condition_size(); i++) {
    const ObRawExpr* expr = select_stmt->get_condition_expr(i);
    ObArray<ObRawExpr*> column_exprs;
    if (expr->has_flag(IS_SIMPLE_COND)) {
      const ObRawExpr* tmp_expr = NULL;
      const ObColumnRefRawExpr* col_expr = NULL;
      uint64_t tid = OB_INVALID_ID;
      uint64_t cid = OB_INVALID_ID;
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, column_exprs))) {
        LOG_WARN("fail to extract_column_exprs", K(ret));
      } else if (column_exprs.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("When expr is simple cond, column_exprs count must be one");
      } else {
        tmp_expr = column_exprs.at(0);
      }

      if (OB_SUCC(ret)) {
        if (!tmp_expr->is_column_ref_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_expr must be column ref");
        } else {
          col_expr = static_cast<const ObColumnRefRawExpr*>(tmp_expr);
        }
      }

      if (OB_SUCC(ret)) {
        cid = col_expr->get_column_id();
        tid = get_real_tid(col_expr->get_table_id(), *select_stmt);
        if (rowkey_tid == tid) {
          int64_t idx = 0;
          if (OB_SUCCESS == (rowkey_info->get_index(cid, idx))) {
            if (OB_FAIL(rowkey_covered_set.add_member(idx))) {
              LOG_WARN("fail to add bit set", K(ret), K(idx));
            } else if (rowkey_covered_set.num_members() == rowkey_info->get_size()) {
              is_covered = true;
              break;
            }
          }
        } else if (tid == base_or_dep_tid) {
          uint64_t out_tid = OB_INVALID_ID;
          uint64_t out_cid = OB_INVALID_ID;
          if (OB_FAIL(get_join_tid_cid(tid, cid, out_tid, out_cid, *mv_schema))) {
            LOG_WARN("fail to get join tid cid", K(ret));
          } else if (out_tid != OB_INVALID_ID) {
            int64_t idx = 0;
            if (OB_SUCCESS == (rowkey_info->get_index(out_cid, idx))) {
              if (OB_FAIL(rowkey_covered_set.add_member(idx))) {
                LOG_WARN("fail to add bit set", K(ret), K(idx));
              } else if (rowkey_covered_set.num_members() == rowkey_info->get_size()) {
                is_covered = true;
                break;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformMaterializedView::check_can_transform(
    ObSelectStmt*& select_stmt, MVDesc mv_desc, bool& result, JoinTableIdPair& idp, ObBitSet<>& expr_idx)
{
  int ret = OB_SUCCESS;
  result = false;
  bool match = true;
  ObSelectStmt* mv = mv_desc.mv_stmt_;
  idp.first = OB_INVALID_ID;
  idp.second = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_from_item_size(); i++) {
    const FromItem& from_item = select_stmt->get_from_item(i);
    if (from_item.is_joined_) {
      continue;
    }
    TableItem* ti = select_stmt->get_table_item_by_id(from_item.table_id_);
    if (!ti->is_basic_table()) {
      continue;
    }

    if (ti->ref_id_ == mv_desc.base_tid_ && idp.first == OB_INVALID_ID) {
      idp.first = ti->table_id_;
    } else if (ti->ref_id_ == mv_desc.dep_tid_ && idp.second == OB_INVALID_ID) {
      idp.second = ti->table_id_;
    }
    if (idp.first != OB_INVALID_ID && idp.second != OB_INVALID_ID) {
      match = true;
      break;
    }
  }
  if (OB_SUCC(ret) && match) {
    const ObIArray<ObOrgIndexHint>& org_indexes = select_stmt->get_stmt_hint().org_indexes_;
    if (org_indexes.count() != 0) {
      match = false;
    }
  }
  if (OB_SUCC(ret) && match) {
    if (OB_FAIL(is_all_column_covered(*select_stmt, mv_desc, idp, result))) {
      SQL_REWRITE_LOG(WARN, "fail to check is_all_column_covered", K(ret));
    }
  }

  if (OB_SUCC(ret) && result) {
    if (OB_FAIL(check_where_condition_covered(select_stmt, mv, result, expr_idx))) {
      SQL_REWRITE_LOG(WARN, "fail to check where condition cover", K(ret));
    }
  }

  if (OB_SUCC(ret) && result) {
    bool is_covered = false;
    if (OB_FAIL(
            is_rowkey_equal_covered(select_stmt, mv_desc.mv_tid_, mv_desc.base_tid_, mv_desc.dep_tid_, is_covered))) {
      LOG_WARN("fail to check is_rowkey_equal_covered", K(ret), K(mv_desc.base_tid_));
    } else if (!is_covered) {
      if (OB_FAIL(
              is_rowkey_equal_covered(select_stmt, mv_desc.mv_tid_, mv_desc.dep_tid_, mv_desc.base_tid_, is_covered))) {
        LOG_WARN("fail to check is_rowkey_equal_covered", K(ret), K(mv_desc.dep_tid_));
      } else if (is_covered) {
        result = false;
      }
    }
  }

  return ret;
}

int ObTransformMaterializedView::reset_part_expr(ObSelectStmt* select_stmt, uint64_t mv_tid, const JoinTableIdPair& idp)
{
  int ret = OB_SUCCESS;
  common::ObIArray<ObDMLStmt::PartExprItem>& part_exprs = select_stmt->get_part_exprs();
  for (int64_t i = 0; i < part_exprs.count(); i++) {
    if (part_exprs.at(i).table_id_ == idp.first || part_exprs.at(i).table_id_ == idp.second) {
      part_exprs.at(i).table_id_ = mv_tid;
    }
  }
  return ret;
}

int ObTransformMaterializedView::reset_table_item(
    ObSelectStmt* select_stmt, uint64_t mv_tid, uint64_t base_tid, const JoinTableIdPair& idp)
{
  int ret = OB_SUCCESS;
  ObArray<TableItem*> table_item_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_table_size(); i++) {
    TableItem* table_item = select_stmt->get_table_item(i);
    if (table_item->table_id_ != idp.first && table_item->table_id_ != idp.second) {
      if (OB_FAIL(table_item_array.push_back(table_item))) {
        SQL_REWRITE_LOG(WARN, "fail to push item to array", K(ret));
      }
    }
  }
  TableItem* new_table_item = NULL;
  if (OB_SUCC(ret)) {
    new_table_item = select_stmt->create_table_item(*(ctx_->allocator_));
    if (NULL == new_table_item) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_REWRITE_LOG(WARN, "fail to create table item");
    } else {
      new_table_item->is_index_table_ = true;
      new_table_item->is_materialized_view_ = true;
      new_table_item->table_id_ = mv_tid;
      new_table_item->ref_id_ = base_tid;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_item_array.push_back(new_table_item))) {
      SQL_REWRITE_LOG(WARN, "fail to push item to array", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(select_stmt->reset_table_item(table_item_array))) {
      SQL_REWRITE_LOG(WARN, "fail to reset_table_item", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    select_stmt->get_table_hash().reset();
    if (OB_FAIL(select_stmt->set_table_bit_index(OB_INVALID_ID))) {
      LOG_WARN("fail to set_table_bit_index", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_item_array.count(); i++) {
      if (OB_FAIL(select_stmt->set_table_bit_index(table_item_array.at(i)->table_id_))) {
        LOG_WARN("fail to set_table_bit_index", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformMaterializedView::reset_where_condition(ObSelectStmt* select_stmt, ObBitSet<>& expr_idx)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> new_conditions;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_condition_size(); i++) {
    ObRawExpr* expr = select_stmt->get_condition_expr(i);
    if (!expr_idx.has_member(i)) {
      if (OB_FAIL(new_conditions.push_back(expr))) {
        SQL_REWRITE_LOG(WARN, "fail to push item to array", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    select_stmt->get_condition_exprs().reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < new_conditions.count(); i++) {
      if (OB_FAIL(select_stmt->get_condition_exprs().push_back(new_conditions.at(i)))) {
        SQL_REWRITE_LOG(WARN, "fail to push back item", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformMaterializedView::reset_from_item(ObSelectStmt* select_stmt, uint64_t mv_tid, const JoinTableIdPair& idp)
{
  int ret = OB_SUCCESS;
  ObArray<FromItem> new_from_item_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_from_item_size(); i++) {
    bool flag = true;
    const FromItem& from_item = select_stmt->get_from_item(i);
    if (!from_item.is_joined_) {
      TableItem* ti = select_stmt->get_table_item_by_id(from_item.table_id_);
      if (ti->is_basic_table()) {
        if (ti->table_id_ == idp.first || ti->table_id_ == idp.second) {
          flag = false;
        }
      }
    }

    if (flag) {
      if (OB_FAIL(new_from_item_array.push_back(from_item))) {
        SQL_REWRITE_LOG(WARN, "fail to push item to array", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    FromItem mv_from_item;
    mv_from_item.table_id_ = mv_tid;
    mv_from_item.is_joined_ = false;
    if (OB_FAIL(new_from_item_array.push_back(mv_from_item))) {
      SQL_REWRITE_LOG(WARN, "fail to push item to array", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(select_stmt->reset_from_item(new_from_item_array))) {
      SQL_REWRITE_LOG(WARN, "fail to reset_from_item", K(ret));
    }
  }
  return ret;
}

int ObTransformMaterializedView::inner_transform(ObDMLStmt*& stmt, ObIArray<MVDesc>& mv_array, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool can_transform = false;
  ObRowDesc table_idx;
  ObBitSet<> expr_idx;
  trans_happened = false;
  if (stmt::T_SELECT == stmt->get_stmt_type()) {
    ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < mv_array.count(); i++) {
      MVDesc& mv_desc = mv_array.at(i);
      JoinTableIdPair idp;
      if (OB_FAIL(check_can_transform(select_stmt, mv_desc, can_transform, idp, expr_idx))) {
        SQL_REWRITE_LOG(WARN, "fail to check_can_transform", K(ret));
      } else if (can_transform) {
        if (OB_ISNULL(stmt->get_query_ctx())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("query ctx is NULL", K(ret));
        } else if (FALSE_IT(stmt->get_query_ctx()->is_contain_mv_ = true)) {
        } else if (OB_FAIL(reset_where_condition(select_stmt, expr_idx))) {
          SQL_REWRITE_LOG(WARN, "fail to reset_where_condition", K(ret));
        } else if (OB_FAIL(rewrite_column_id(select_stmt, mv_desc, idp))) {
          SQL_REWRITE_LOG(WARN, "fail to adjust_expr_column", K(ret));
        } else if (OB_FAIL(reset_from_item(select_stmt, mv_desc.mv_tid_, idp))) {
          SQL_REWRITE_LOG(WARN, "fail to reset_from_item", K(ret));
        } else if (OB_FAIL(reset_table_item(select_stmt, mv_desc.mv_tid_, mv_desc.base_tid_, idp))) {
          SQL_REWRITE_LOG(WARN, "fail to reset_table_item", K(ret));
        } else if (OB_FAIL(reset_part_expr(select_stmt, mv_desc.mv_tid_, idp))) {
          SQL_REWRITE_LOG(WARN, "fail to reset_part_expr", K(ret));
        } else if (OB_FAIL(rewrite_column_id2(select_stmt))) {
          SQL_REWRITE_LOG(WARN, "fail to adjust_expr_column", K(ret));
        } else if (OB_FAIL(select_stmt->pull_all_expr_relation_id_and_levels())) {
          LOG_WARN("pull select stmt relation ids failed", K(ret));
        } else {
          trans_happened = true;
        }
      }
    }
  }
  return ret;
}

int ObTransformMaterializedView::get_view_stmt(
    const share::schema::ObTableSchema& index_schema, ObSelectStmt*& view_stmt)
{
  int ret = OB_SUCCESS;
  ObResolverParams resolver_ctx;
  resolver_ctx.allocator_ = ctx_->allocator_;
  resolver_ctx.schema_checker_ = ctx_->schema_checker_;
  resolver_ctx.session_info_ = ctx_->session_info_;
  resolver_ctx.stmt_factory_ = ctx_->stmt_factory_;
  resolver_ctx.expr_factory_ = ctx_->expr_factory_;
  resolver_ctx.query_ctx_ = ctx_->stmt_factory_->get_query_ctx();

  ObParser parser(*(ctx_->allocator_), DEFAULT_MYSQL_MODE, ctx_->session_info_->get_local_collation_connection());
  ObSelectResolver view_resolver(resolver_ctx);
  ParseResult view_result;
  ObString view_def;
  if (OB_FAIL(ObSQLUtils::generate_view_definition_for_resolve(*(ctx_->allocator_),
          ctx_->session_info_->get_local_collation_connection(),
          index_schema.get_view_schema(),
          view_def))) {
    LOG_WARN("fail to generate view definition for resolve", K(ret));
  } else if (OB_FAIL(parser.parse(view_def, view_result))) {
    LOG_WARN("parse view defination failed", K(view_def), K(ret));
  } else {
    ParseNode* view_stmt_node = view_result.result_tree_->children_[0];
    if (!view_stmt_node) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected children for view result after parse", K(ret));
    } else if (OB_FAIL(view_resolver.resolve(*view_stmt_node))) {
      LOG_WARN("expand view table failed", K(ret));
    } else {
      view_stmt = view_resolver.get_select_stmt();
    }
  }
  return ret;
}

int ObTransformMaterializedView::get_all_related_mv_array(ObDMLStmt*& stmt, ObIArray<MVDesc>& mv_array)
{
  int ret = OB_SUCCESS;
  uint64_t idx_tids[OB_MAX_INDEX_PER_TABLE];
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); i++) {
    TableItem* table_item = stmt->get_table_item(i);
    if (!table_item->is_index_table_ && table_item->is_basic_table()) {
      uint64_t table_id = table_item->ref_id_;
      int64_t idx_count = OB_MAX_INDEX_PER_TABLE;
      if (OB_FAIL(ctx_->schema_checker_->get_can_read_index_array(table_id, idx_tids, idx_count, true))) {
        SQL_REWRITE_LOG(WARN, "fail to get_can_read_index_array", K(table_id), K(ret));
      }
      MVDesc mv_desc;
      for (int j = 0; OB_SUCC(ret) && j < idx_count; j++) {
        uint64_t index_tid = idx_tids[j];
        const ObTableSchema* index_schema = NULL;
        if (OB_FAIL(ctx_->schema_checker_->get_table_schema(index_tid, index_schema))) {
          SQL_REWRITE_LOG(WARN, "fail to get table schema", K(table_id), K(index_tid), K(j), K(idx_count), K(ret));
        } else if (share::schema::MATERIALIZED_VIEW == index_schema->get_table_type()) {
          ObSelectStmt* view_stmt = NULL;
          mv_desc.mv_tid_ = index_tid;
          mv_desc.base_tid_ = table_id;
          mv_desc.dep_tid_ = index_schema->get_depend_table_ids().at(0);
          mv_desc.table_name_ = index_schema->get_table_name_str();
          if (OB_FAIL(get_view_stmt(*index_schema, view_stmt))) {
            SQL_REWRITE_LOG(WARN, "fail to get_view_stmt", K(ret));
          } else {
            mv_desc.mv_stmt_ = view_stmt;
            if (OB_FAIL(mv_array.push_back(mv_desc))) {
              SQL_REWRITE_LOG(WARN, "fail to push back view_stmt", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformMaterializedView::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObArray<MVDesc> mv_array;
    if (OB_FAIL(get_all_related_mv_array(stmt, mv_array))) {
      SQL_REWRITE_LOG(WARN, "fail to get_all_related_mv_array", K(ret));
    } else if (OB_FAIL(inner_transform(stmt, mv_array, trans_happened))) {
      SQL_REWRITE_LOG(WARN, "fail to do transformer", K(ret));
    }
  }
  return ret;
}
