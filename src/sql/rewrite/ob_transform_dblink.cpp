/**
 * Copyright (c) 2023 OceanBase
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
#include "sql/rewrite/ob_transform_dblink.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_insert_all_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "common/ob_smart_call.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

int ObTransformDBlink::transform_one_stmt(ObIArray<ObParentDMLStmt> &parent_stmts,
                                               ObDMLStmt *&stmt,
                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (transform_for_write_) {
    if (stmt->is_dml_write_stmt() &&
        OB_FAIL(reverse_link_table(stmt, trans_happened))) {
      LOG_WARN("failed to reverse link table", K(ret));
    } else {
      LOG_TRACE("succeed to reverse link table", K(ret));
    }
  } else {
    if ((OB_E(EventTable::EN_GENERATE_PLAN_WITH_RECONSTRUCT_SQL) OB_SUCCESS) != OB_SUCCESS) {
      //dblink trace point
      if (OB_FAIL(formalize_link_table(stmt))) {
        LOG_WARN("failed to formalize link table", K(ret));
      }
    } else if (OB_FAIL(pack_link_table(stmt, trans_happened))) {
      LOG_WARN("failed to pack link table", K(ret));
    } else if (OB_UNLIKELY(stmt->is_dml_write_stmt() && stmt->is_dblink_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dblink stmt", KPC(stmt));
    } else {
      LOG_TRACE("succeed to pack link table", K(ret));
    }
  }
  return ret;
}

int ObTransformDBlink::reverse_link_table(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  uint64_t target_dblink_id = OB_INVALID_ID;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(get_target_dblink_id(stmt, target_dblink_id))) {
    LOG_WARN("failed to get target dblink id", K(ret));
  } else if (OB_INVALID_ID == target_dblink_id) {
    //do nothing
  } else if (OB_FAIL(check_dml_link_valid(stmt, target_dblink_id))) {
    LOG_WARN("failed to check dml link valid", K(ret));
  } else if (OB_FAIL(inner_reverse_link_table(stmt, target_dblink_id))) {
    LOG_WARN("failed to reverse link table", K(ret));
  } else if (OB_FAIL(reverse_link_table_for_temp_table(stmt, target_dblink_id))) {
    LOG_WARN("failed to reverse temp table", K(ret));
  } else {
    trans_happened = true;
    stmt->set_dblink_id(target_dblink_id);
    stmt->set_reverse_link();
  }
  return ret;
}

int ObTransformDBlink::check_dml_link_valid(ObDMLStmt *stmt, uint64_t target_dblink_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt ", K(ret));
  } else if (stmt->is_dml_write_stmt()) {
    bool link_oracle = false;
    bool has_default = false;
    ObSEArray<ObAssignment, 4> table_assigns;
    ObDelUpdStmt *dml_stmt = static_cast<ObDelUpdStmt*>(stmt);
    //check returning
    if (!dml_stmt->get_returning_exprs().empty() ||
        !dml_stmt->get_returning_into_exprs().empty()) {
      ret = OB_ERR_RETURNING_CLAUSE;
      LOG_WARN("return in dblink not supported", K(ret));
    } else if (OB_FAIL(check_link_oracle(target_dblink_id, link_oracle))) {
      LOG_WARN("failed to check link oracle", K(ret));
      //check insert values
    } else if (!link_oracle) {
      //do nothing
    } else if (stmt->is_insert_stmt() &&
              !static_cast<ObInsertStmt*>(stmt)->value_from_select() &&
              static_cast<ObInsertStmt*>(stmt)->get_insert_row_count() > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("insert multi values not support in dblink", K(ret));
    } else if (OB_FAIL(get_table_assigns(stmt, table_assigns))) {
      LOG_WARN("failed to get table assigns", K(ret));
    } else if (OB_FAIL(check_has_default_value(table_assigns, has_default))) {
      LOG_WARN("failed to check has default value", K(ret));
    } else if (has_default) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("set default value not support in oracle", K(ret));
    }
  }
  return ret;
}

int ObTransformDBlink::get_table_assigns(ObDMLStmt *stmt, ObIArray<ObAssignment> &assigns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stmt", K(ret));
  } else if (stmt->is_update_stmt()) {
    ObUpdateStmt * upd_stmt = static_cast<ObUpdateStmt*>(stmt);
    ObIArray<ObUpdateTableInfo*> &table_infos = upd_stmt->get_update_table_info();
    for (int i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
      ObUpdateTableInfo *table_info = table_infos.at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table info", K(ret));
      } else if (OB_FAIL(append(assigns, table_info->assignments_))) {
        LOG_WARN("failed to append exprs", K(ret));
      }
    }
  } else if (stmt->is_insert_all_stmt()) {
    ObInsertAllStmt *insert_all_stmt = static_cast<ObInsertAllStmt*>(stmt);
    ObIArray<ObInsertAllTableInfo*> &table_infos = insert_all_stmt->get_insert_all_table_info();
    for (int i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
      ObInsertAllTableInfo *table_info = table_infos.at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table info", K(ret));
      } else if (OB_FAIL(append(assigns, table_info->assignments_))) {
        LOG_WARN("failed to append exprs", K(ret));
      }
    }
  } else if (stmt->is_insert_stmt()) {
    ObInsertStmt *insert_stmt = static_cast<ObInsertStmt*>(stmt);
    ObInsertTableInfo &table_info = insert_stmt->get_insert_table_info();
    if (OB_FAIL(assigns.assign(table_info.assignments_))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  } else if (stmt->is_merge_stmt()) {
    ObMergeStmt *merge_stmt = static_cast<ObMergeStmt*>(stmt);
    ObMergeTableInfo &table_info = merge_stmt->get_merge_table_info();
    if (OB_FAIL(assigns.assign(table_info.assignments_))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformDBlink::check_has_default_value(ObIArray<ObAssignment> &assigns, bool &has_default)
{
  int ret = OB_SUCCESS;
  has_default = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_default && i <assigns.count(); ++i) {
    ObAssignment &assign = assigns.at(i);
    if (OB_ISNULL(assign.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (T_FUN_SYS_DEFAULT == assign.expr_->get_expr_type()) {
      has_default = true;
    } else if (T_FUN_COLUMN_CONV != assign.expr_->get_expr_type()) {
      //do nothing
    } else if (assign.expr_->get_param_count() < 5) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect expr param count", K(ret));
    } else if (OB_ISNULL(assign.expr_->get_param_expr(4))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr param", K(ret));
    } else if (T_FUN_SYS_DEFAULT == assign.expr_->get_param_expr(4)->get_expr_type()) {
      has_default = true;
    }
  }
  return ret;
}

int ObTransformDBlink::get_target_dblink_id(ObDMLStmt *stmt, uint64_t &dblink_id)
{
  int ret = OB_SUCCESS;
  dblink_id = OB_INVALID_ID;
  ObDmlTableInfo *table_info = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!stmt->is_dml_write_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect stmt type", K(ret));
  } else {
    TableItem *target_table = NULL;
    ObSEArray<ObDmlTableInfo*, 4> table_infos;
    ObDelUpdStmt *dml_stmt = static_cast<ObDelUpdStmt*>(stmt);
    if (OB_FAIL(dml_stmt->get_dml_table_infos(table_infos))) {
      LOG_WARN("failed to get table infos", K(ret));
    } else if (1 != table_infos.count()) {
      //do nothing
    } else if (OB_ISNULL(table_infos.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table info", K(ret));
    } else if (OB_ISNULL(target_table=stmt->get_table_item_by_id(table_infos.at(0)->table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (target_table->is_link_table()) {
      dblink_id = target_table->is_reverse_link_ ? 0 : target_table->dblink_id_;
    } else if (target_table->is_generated_table()) {
      if (OB_FAIL(SMART_CALL(recursive_get_target_dblink_id(target_table->ref_query_, dblink_id)))) {
        LOG_WARN("failed to get target dblink id", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformDBlink::recursive_get_target_dblink_id(ObSelectStmt *stmt, uint64_t &dblink_id)
{
  int ret = OB_SUCCESS;
  dblink_id = OB_INVALID_ID;
  TableItem *target_table = NULL;
  uint64_t target_dblink_id = OB_INVALID_ID;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < stmt->get_table_items().count(); ++i) {
    if (OB_ISNULL(target_table = stmt->get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table", K(ret));
    } else if (target_table->is_link_table()) {
      if (OB_INVALID_ID == target_dblink_id) {
        target_dblink_id = target_table->dblink_id_;
      } else if (target_dblink_id != target_table->dblink_id_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("multi dblink table dml not support", K(ret));
      }
    } else if (target_table->is_generated_table() ||
               target_table->is_temp_table()) {
      uint64_t id = OB_INVALID_ID;
      if (OB_FAIL(SMART_CALL(recursive_get_target_dblink_id(target_table->ref_query_, id)))) {
        LOG_WARN("failed to get target dblink id", K(ret));
      } else if (OB_INVALID_ID == target_dblink_id) {
        target_dblink_id = id;
      } else if (id != target_dblink_id) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("multi dblink table dml not support", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "multi dblink table dml not support");
      }
    } else if (OB_INVALID_ID != target_dblink_id) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("multi dblink table dml not support", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    dblink_id = target_dblink_id;
  }
  return ret;
}

int ObTransformDBlink::inner_reverse_link_table(ObDMLStmt *stmt, uint64_t target_dblink_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  bool has_invalid_expr = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_FAIL(SMART_CALL(inner_reverse_link_table(child_stmts.at(i), target_dblink_id)))) {
      LOG_WARN("failed to reverse link table", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(has_invalid_link_expr(*stmt, has_invalid_expr))) {
    LOG_WARN("failed to check stmt has invalid link expr", K(ret));
  } else if (has_invalid_expr) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("dblink write with special expr not supported", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dblink write with user defined function/variable/type");
  } else if (OB_FAIL(reverse_link_tables(stmt->get_table_items(), target_dblink_id))) {
    LOG_WARN("failed to reverse link table", K(ret));
  } else if (OB_FAIL(reverse_link_sequence(*stmt, target_dblink_id))) {
    LOG_WARN("failed to reverse link sequence", K(ret));
  } else if (OB_FAIL(formalize_link_table(stmt))) {
    LOG_WARN("failed to formalize link table", K(ret));
  }
  return ret;
}

int ObTransformDBlink::reverse_one_link_table(TableItem *table, uint64_t target_dblink_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table", K(ret));
  } else if (table->is_reverse_link_ && table->is_link_type()) {
    table->is_reverse_link_ = false;
    if (OB_FAIL(ob_write_string(*ctx_->allocator_,
                                table->link_database_name_,
                                table->database_name_))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (table->dblink_name_.empty()) {
      table->type_ = TableItem::BASE_TABLE;
    }
  } else if (table->is_link_type()) {
    if (table->dblink_id_ == target_dblink_id) {
      table->type_ = TableItem::BASE_TABLE;
      if (OB_FAIL(ob_write_string(*ctx_->allocator_,
                                  table->link_database_name_,
                                  table->database_name_))) {
        LOG_WARN("failed to write string", K(ret));
      }
    } else {
      table->is_reverse_link_ = true;
      if (OB_FAIL(ob_write_string(*ctx_->allocator_,
                                  table->link_database_name_,
                                  table->database_name_))) {
        LOG_WARN("failed to write string", K(ret));
      }
    }
  } else if (table->is_view_table_) {
    table->database_name_ = ObString::make_empty_string();
  } else if (table->is_basic_table()) {
    table->type_ = TableItem::LINK_TABLE;
    table->is_reverse_link_ = true;
    if (OB_FAIL(ob_write_string(*ctx_->allocator_,
                                table->database_name_,
                                table->link_database_name_))) {
      LOG_WARN("failed to write string", K(ret));
    }
  } else if (table->is_joined_table()) {
    if (OB_FAIL(reverse_one_link_table(static_cast<JoinedTable*>(table)->left_table_, target_dblink_id))) {
      LOG_WARN("failed to reverse left_table_", K(ret));
    } else if (OB_FAIL(reverse_one_link_table(static_cast<JoinedTable*>(table)->right_table_, target_dblink_id))) {
      LOG_WARN("failed to reverse right_table_", K(ret));
    }
  } else {
    //do nothing
  }
  return ret;
}

int ObTransformDBlink::reverse_link_tables(ObIArray<TableItem*> &tables, uint64_t target_dblink_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
    if (OB_FAIL(reverse_one_link_table(tables.at(i), target_dblink_id))) {
      LOG_WARN("failed to reverse one link table", K(ret));
    }
  }
  return ret;
}

int ObTransformDBlink::reverse_link_tables(ObDMLStmt &stmt,
                                           ObIArray<TableItem*> &tables,
                                           ObIArray<SemiInfo*> &semi_infos,
                                           uint64_t target_dblink_id)
{
  int ret = OB_SUCCESS;
  OZ(reverse_link_tables(tables, target_dblink_id));
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); i ++) {
    TableItem* right_table = NULL;
    SemiInfo * semi_info = NULL;
    if (OB_ISNULL(semi_info = semi_infos.at(i)) ||
        OB_ISNULL(right_table = stmt.get_table_item_by_id(semi_info->right_table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret));
    } else if (OB_FAIL(reverse_one_link_table(right_table, target_dblink_id))) {
      LOG_WARN("failed to reverse one link table", K(ret));
    }
  }
  return ret;
}

int ObTransformDBlink::reverse_link_sequence(ObDMLStmt &stmt, uint64_t target_dblink_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 2> seq_exprs;
  if (OB_FAIL(stmt.get_sequence_exprs(seq_exprs))) {
    LOG_WARN("failed to get sequence exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < seq_exprs.count(); ++i) {
    ObRawExpr *expr = seq_exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (T_FUN_SYS_SEQ_NEXTVAL == expr->get_expr_type()) {
      ObSequenceRawExpr *seq_expr = static_cast<ObSequenceRawExpr*>(expr);
      if (target_dblink_id != seq_expr->get_dblink_id()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("read local sequence object in dblink write not support", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "read local sequence object in dblink write not support");
      } else {
        seq_expr->set_dblink_id(OB_INVALID_ID);
      }
    }
  }
  return ret;
}

int ObTransformDBlink::reverse_link_table_for_temp_table(ObDMLStmt *root_stmt, uint64_t target_dblink_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDMLStmt::TempTableInfo, 4> temp_table_infos;
  if (OB_ISNULL(root_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(root_stmt->collect_temp_table_infos(temp_table_infos))) {
    LOG_WARN("failed to collect temp table infos", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos.count(); ++i) {
    if (OB_FAIL(inner_reverse_link_table(temp_table_infos.at(i).temp_table_query_,
                                         target_dblink_id))) {
      LOG_WARN("failed to reverse link table", K(ret));
    }
  }
  return ret;
}

int ObTransformDBlink::pack_link_table(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  uint64_t dblink_id = OB_INVALID_ID;
  bool is_reverse_link = false;
  ObSEArray<LinkTableHelper, 4> helpers;
  bool all_table_from_one_dblink = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(add_flashback_query_for_dblink(stmt))) {
    LOG_WARN("add flashback query for dblink failed", K(ret));
  } else if (OB_FAIL(collect_link_table(stmt,
                                        helpers,
                                        dblink_id,
                                        is_reverse_link,
                                        all_table_from_one_dblink))) {
    LOG_WARN("failed to collect link table", K(ret));
  } else if (all_table_from_one_dblink) {
    if (OB_FAIL(reverse_link_tables(stmt->get_table_items(),
                                    is_reverse_link ? 0 : dblink_id))) {
      LOG_WARN("failed to reverse link table", K(ret));
    } else if (OB_FAIL(reverse_link_sequence(*stmt, is_reverse_link ? 0 : dblink_id))) {
      LOG_WARN("failed to reverse link sequence", K(ret));
    } else if (OB_FAIL(formalize_link_table(stmt))) {
      LOG_WARN("failed to formalize link table stmt", K(ret));
    } else if (lib::is_oracle_mode() &&
               OB_FAIL(extract_limit(stmt, stmt))) {
      LOG_WARN("failed to formalize limit", K(ret));
    } else {
      stmt->set_dblink_id(is_reverse_link ? 0 : dblink_id);
      trans_happened = true;
    }
  } else if (OB_FAIL(collect_pushdown_conditions(stmt, helpers))) {
    LOG_WARN("failed to collect pushdown conditions", K(ret));
  } else if (OB_FAIL(split_link_table_info(stmt, helpers))) {
    LOG_WARN("failed to split link table info", K(ret));
  } else if (!helpers.empty()) {
    trans_happened = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < helpers.count(); ++i) {
      if (OB_FAIL(inner_pack_link_table(stmt, helpers.at(i)))) {
        LOG_WARN("failed to pack link table", K(ret));
      } else {
        LOG_TRACE("succeed to pack one link stmt", K(helpers.at(i)));
      }
    }
  }
  return ret;
}

int ObTransformDBlink::has_invalid_link_expr(ObDMLStmt &stmt, bool &has_invalid_expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> exprs;
  has_invalid_expr = false;
  if (OB_FAIL(stmt.get_relation_exprs(exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !has_invalid_expr && i < exprs.count(); i++) {
      bool is_valid = false;
      if (OB_FAIL(check_link_expr_valid(exprs.at(i), is_valid))) {
        LOG_WARN("failed to check link expr valid", KPC(exprs.at(i)), K(ret));
      } else if (!is_valid) {
        has_invalid_expr = true;
      }
    }
  }
  return ret;
}

int ObTransformDBlink::check_link_expr_valid(ObRawExpr *expr, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (expr->has_flag(CNT_PL_UDF) ||
             expr->has_flag(CNT_SO_UDF) ||
             expr->has_flag(CNT_DYNAMIC_USER_VARIABLE)) {
    // special flag is invalid
  } else if (T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS == expr->get_expr_type() ||
             T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE == expr->get_expr_type() ||
             T_FUN_SYS_ESTIMATE_NDV == expr->get_expr_type() ||
             T_OP_GET_USER_VAR == expr->get_expr_type()) {
    // special function is invalid
  } else if (expr->get_result_type().is_ext()) {
    // special type is invalid
  } else {
    is_valid = true;
  }

  if (OB_SUCC(ret) && !is_valid) {
    LOG_DEBUG("expr should not appear in the link stmt", KPC(expr));
  }

  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < expr->get_param_count(); i++) {
    ret = SMART_CALL(check_link_expr_valid(expr->get_param_expr(i), is_valid));
  }
  return ret;
}

int ObTransformDBlink::collect_link_table(ObDMLStmt *stmt,
                                          ObIArray<LinkTableHelper> &helpers,
                                          uint64_t &dblink_id,
                                          bool &is_reverse_link,
                                          bool &all_table_from_one_dblink)
{
  int ret = OB_SUCCESS;
  all_table_from_one_dblink = true;
  bool has_special_expr = false;
  is_reverse_link = false;
  ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (stmt->is_hierarchical_query() || stmt->is_unpivot_select() ||
             (stmt->is_select_stmt() && sel_stmt->has_select_into()) ||
             stmt->is_dml_write_stmt()) {
    all_table_from_one_dblink = false;
  } else if (has_invalid_link_expr(*stmt, has_special_expr)) {
    LOG_WARN("failed to check stmt has invalid link expr", K(ret));
  } else if (has_special_expr) {
    all_table_from_one_dblink = false;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_info_size(); ++i) {
    TableItem *table = NULL;
    SemiInfo *semi_info = NULL;
    if (OB_ISNULL(semi_info = stmt->get_semi_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (OB_FAIL(inner_collect_link_table(stmt,
                                        semi_info,
                                        helpers,
                                        all_table_from_one_dblink))) {
      LOG_WARN("failed to collect link table", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); ++i) {
    FromItem &item = stmt->get_from_item(i);
    TableItem *table = NULL;
    ObSqlBitSet<> rel_ids;
    if (!item.is_joined_) {
      table = stmt->get_table_item_by_id(item.table_id_);
    } else {
      table = stmt->get_joined_table(item.table_id_);
    }
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table", K(ret), K(item));
    } else if (OB_FAIL(stmt->get_table_rel_ids(*table, rel_ids))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    } else if (OB_FAIL(inner_collect_link_table(table,
                                                NULL,
                                                helpers,
                                                all_table_from_one_dblink))) {
      LOG_WARN("failed to collect link table", K(ret));
    }
  }
  if (OB_SUCC(ret) && helpers.count() == 1) {
    dblink_id = helpers.at(0).dblink_id_;
    is_reverse_link = helpers.at(0).is_reverse_link_;
  }
  all_table_from_one_dblink &= (1 == helpers.count() || stmt->is_set_stmt());
  //check sequence
  if (OB_SUCC(ret) && all_table_from_one_dblink) {
    ObSEArray<ObRawExpr*, 2> seq_exprs;
    if (OB_FAIL(stmt->get_sequence_exprs(seq_exprs))) {
      LOG_WARN("failed to get sequence exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && all_table_from_one_dblink && i < seq_exprs.count(); ++i) {
      ObRawExpr *expr = seq_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (T_FUN_SYS_SEQ_NEXTVAL == expr->get_expr_type()) {
        ObSequenceRawExpr *seq_expr = static_cast<ObSequenceRawExpr*>(expr);
        if (dblink_id != seq_expr->get_dblink_id()) {
          all_table_from_one_dblink = false;
        }
      }
    }
  }
  //check child stmt
  if (OB_SUCC(ret) && all_table_from_one_dblink) {
    ObSEArray<ObSelectStmt*, 4> child_stmts;
    if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && all_table_from_one_dblink && i < child_stmts.count(); ++i) {
      ObSelectStmt *child_stmt = child_stmts.at(i);
      if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(ret));
      } else if (stmt->is_set_stmt() && 0 == i) {
        if (OB_INVALID_ID == child_stmt->get_dblink_id() &&
            !child_stmt->is_reverse_link()) {
          all_table_from_one_dblink = false;
        } else {
          dblink_id = child_stmt->get_dblink_id();
          is_reverse_link = child_stmt->is_reverse_link();
        }
      } else if (dblink_id != child_stmt->get_dblink_id() &&
                 !child_stmt->is_reverse_link()) {
        all_table_from_one_dblink = false;
      }
    }
  }
  return ret;
}

int ObTransformDBlink::inner_collect_link_table(TableItem *table,
                                                JoinedTable *parent_table,
                                                ObIArray<LinkTableHelper> &helpers,
                                                bool &all_table_from_one_dblink)
{
  int ret = OB_SUCCESS;
  uint64_t dblink_id = OB_INVALID_ID;
  bool is_link_table = false;
  bool is_reverse_link = false;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (table->is_joined_table()) {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table);
    if (OB_FAIL(check_is_link_table(table,
                                    dblink_id,
                                    is_link_table,
                                    is_reverse_link))) {
      LOG_WARN("failed to check pis link table", K(ret));
    } else if (is_link_table) {
      if (OB_FAIL(add_link_table(table,
                                  dblink_id,
                                  is_reverse_link,
                                  parent_table,
                                  NULL,
                                  helpers))) {
        LOG_WARN("failed to add link table", K(ret));
      }
    } else {
      if (OB_FAIL(SMART_CALL(inner_collect_link_table(joined_table->left_table_,
                                                      joined_table,
                                                      helpers,
                                                      all_table_from_one_dblink)))) {
        LOG_WARN("failed to collect link table", K(ret));
      } else if (OB_FAIL(SMART_CALL(inner_collect_link_table(joined_table->right_table_,
                                                            joined_table,
                                                            helpers,
                                                            all_table_from_one_dblink)))) {
        LOG_WARN("failed to collect link table", K(ret));
      }
    }
  } else {
    if (OB_FAIL(check_is_link_table(table,
                                    dblink_id,
                                    is_link_table,
                                    is_reverse_link))) {
      LOG_WARN("failed to check is link table", K(ret));
    } else if (is_link_table) {
      if (OB_FAIL(add_link_table(table,
                                  dblink_id,
                                  is_reverse_link,
                                  parent_table,
                                  NULL,
                                  helpers))) {
        LOG_WARN("failed to add link table", K(ret));
      }
    } else {
      all_table_from_one_dblink = false;
    }
  }
  return ret;
}

int ObTransformDBlink::check_is_link_table(TableItem *table,
                                          uint64_t &dblink_id,
                                          bool &is_link_table,
                                          bool &is_reverse_link)
{
  int ret = OB_SUCCESS;
  is_link_table = false;
  dblink_id = OB_INVALID_ID;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table", K(ret));
  } else if (table->is_link_table()) {
    is_link_table = true;
    dblink_id = table->dblink_id_;
    is_reverse_link = table->is_reverse_link_;
  } else if (table->is_generated_table() || table->is_temp_table()) {
    if (OB_ISNULL(table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ref query", K(ret));
    } else {
      dblink_id = table->ref_query_->get_dblink_id();
      is_link_table = (OB_INVALID_ID != dblink_id);
      is_reverse_link = table->ref_query_->is_reverse_link();
    }
  } else if (table->is_joined_table()) {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table);
    uint64_t left_dblink_id = OB_INVALID_ID;
    uint64_t right_dblink_id = OB_INVALID_ID;
    bool left_is_link_table = false;
    bool right_is_link_table = false;
    bool left_is_reverse_link = false;
    bool right_is_reverse_link = false;
    if (CONNECT_BY_JOIN == joined_table->joined_type_) {
      is_link_table = false;
    } else if (OB_FAIL(SMART_CALL(check_is_link_table(joined_table->left_table_,
                                              left_dblink_id,
                                              left_is_link_table,
                                              left_is_reverse_link)))) {
      LOG_WARN("failed to check is link table", K(ret));
    } else if (OB_FAIL(SMART_CALL(check_is_link_table(joined_table->right_table_,
                                                      right_dblink_id,
                                                      right_is_link_table,
                                                      right_is_reverse_link)))) {
      LOG_WARN("failed to check is link table", K(ret));
    } else if (left_is_link_table &&
               right_is_link_table &&
               (left_dblink_id == right_dblink_id ||
                (left_is_reverse_link && right_is_reverse_link))) {
      bool has_special_expr = false;
      if (OB_FAIL(has_none_pushdown_expr(joined_table->join_conditions_,
                                        left_dblink_id,
                                        has_special_expr))) {
        LOG_WARN("failed to check has none pushdown expr", K(ret));
      } else if (!has_special_expr) {
        is_link_table = true;
        dblink_id = left_dblink_id;
        is_reverse_link = left_is_reverse_link;
      }
    }
  }
  return ret;
}

int ObTransformDBlink::check_is_link_semi_info(ObDMLStmt &stmt,
                                               SemiInfo &semi_info,
                                               uint64_t &dblink_id,
                                               bool &is_link_semi_info,
                                               bool &right_is_link_table,
                                               bool &is_reverse_link)
{
  int ret = OB_SUCCESS;
  is_link_semi_info = false;
  right_is_link_table = false;
  ObRelIds semi_tables;
  const ObIArray<uint64_t> &left_table_ids = semi_info.left_table_ids_;
  TableItem *right_table = NULL;
  if (OB_ISNULL(right_table = stmt.get_table_item_by_id(semi_info.right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_FAIL(check_is_link_table(right_table,
                                         dblink_id,
                                         right_is_link_table,
                                         is_reverse_link))) {
    LOG_WARN("failed to check is link table", K(ret));
  } else if (right_is_link_table) {
    is_link_semi_info = true;
    if (OB_FAIL(ObTransformUtils::get_rel_ids_from_tables(&stmt,
                                                          semi_info.left_table_ids_,
                                                          semi_tables))) {
      LOG_WARN("failed to get rel ids", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_link_semi_info && i < stmt.get_from_item_size(); ++i) {
      FromItem &item = stmt.get_from_item(i);
      TableItem *left_table = NULL;
      ObSqlBitSet<> from_rel_ids;
      uint64_t left_dblink_id = OB_INVALID_ID;
      bool left_is_link_table = false;
      bool left_is_reverse_link = false;
      if (!item.is_joined_) {
        left_table = stmt.get_table_item_by_id(item.table_id_);
      } else {
        left_table = stmt.get_joined_table(item.table_id_);
      }
      if (OB_ISNULL(left_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get table", K(ret), K(item));
      } else if (OB_FAIL(stmt.get_table_rel_ids(*left_table, from_rel_ids))) {
        LOG_WARN("failed to get table rel ids", K(ret));
      } else if (!semi_tables.overlap(from_rel_ids)) {
        // skip
      } else if (OB_FAIL(check_is_link_table(left_table,
                                             left_dblink_id,
                                             left_is_link_table,
                                             left_is_reverse_link))) {
        LOG_WARN("failed to check is link table", K(ret));
      } else if (left_is_link_table &&
                 right_is_link_table &&
                 (left_dblink_id == dblink_id ||
                 (left_is_reverse_link && is_reverse_link))) {
        if (OB_FAIL(semi_tables.add_members(from_rel_ids))) {
          LOG_WARN("failed to add member", K(ret));
        }
      } else {
        is_link_semi_info = false;
      }
    }
  }
  bool has_special_expr = false;
  if (OB_FAIL(ret) || !is_link_semi_info) {
  } else if (OB_FAIL(has_none_pushdown_expr(semi_info.semi_conditions_,
                                            dblink_id,
                                            has_special_expr))) {
    LOG_WARN("failed to check has none pushdown expr", K(ret));
  } else if (has_special_expr) {
    is_link_semi_info = false;
  }
  return ret;
}


int ObTransformDBlink::inner_collect_link_table(ObDMLStmt *stmt,
                                                SemiInfo *semi_info,
                                                ObIArray<LinkTableHelper> &helpers,
                                                bool &all_table_from_one_dblink)
{
  int ret = OB_SUCCESS;
  uint64_t dblink_id = OB_INVALID_ID;
  bool is_link_semi_info = false;
  bool right_is_link_table = false;
  bool is_reverse_link = false;
  if (OB_ISNULL(semi_info) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_FAIL(check_is_link_semi_info(*stmt,
                                             *semi_info,
                                             dblink_id,
                                             is_link_semi_info,
                                             right_is_link_table,
                                             is_reverse_link))) {
    LOG_WARN("failed to check is link table", K(ret));
  } else if (is_link_semi_info) {
    if (OB_FAIL(add_link_semi_info(semi_info,
                                   dblink_id,
                                   is_reverse_link,
                                   helpers))) {
      LOG_WARN("failed to add link semi info", K(ret));
    }
  } else if (right_is_link_table) {
    TableItem *table = stmt->get_table_item_by_id(semi_info->right_table_id_);
    if (OB_FAIL(add_link_table(table,
                               dblink_id,
                               is_reverse_link,
                               NULL,
                               semi_info,
                               helpers))) {
      LOG_WARN("failed to add link table", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    all_table_from_one_dblink &= is_link_semi_info;
  }
  return ret;
}

int ObTransformDBlink::add_link_table(TableItem *table,
                                      uint64_t dblink_id,
                                      bool is_reverse_link,
                                      JoinedTable *parent_table,
                                      SemiInfo* parent_semi_info,
                                      ObIArray<LinkTableHelper> &helpers)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table", K(ret));
  } else if (NULL != parent_table || NULL != parent_semi_info) {
    //pushdown into sigle stmt
    LinkTableHelper temp;
    temp.dblink_id_ = dblink_id;
    temp.is_reverse_link_ = is_reverse_link;
    temp.parent_table_ = parent_table;
    temp.parent_semi_info_ = parent_semi_info;
    if (OB_FAIL(temp.table_items_.push_back(table))) {
      LOG_WARN("failed to push back table item", K(ret));
    } else if (OB_FAIL(helpers.push_back(temp))) {
      LOG_WARN("failed to push back helper", K(ret));
    }
  } else {
    LinkTableHelper *helper = NULL;
    //find link table from same dblink
    for (int64_t i = 0; OB_INVALID_ID != dblink_id && NULL == helper && i < helpers.count(); ++i) {
      if ((dblink_id == helpers.at(i).dblink_id_ ||
          (is_reverse_link && helpers.at(i).is_reverse_link_)) &&
          helpers.at(i).parent_table_ == NULL &&
          helpers.at(i).parent_semi_info_ == NULL) {
        helper = &helpers.at(i);
      }
    }
    //find
    if (OB_SUCC(ret) && NULL != helper) {
      if (OB_FAIL(helper->table_items_.push_back(table))) {
        LOG_WARN("failed to push back table item", K(ret));
      }
    }
    //not find
    if (OB_SUCC(ret) && NULL == helper) {
      LinkTableHelper temp;
      temp.dblink_id_ = dblink_id;
      temp.is_reverse_link_ = is_reverse_link;
      temp.parent_table_ = parent_table;
      temp.parent_semi_info_ = parent_semi_info;
      if (OB_FAIL(temp.table_items_.push_back(table))) {
        LOG_WARN("failed to push back table item", K(ret));
      } else if (OB_FAIL(helpers.push_back(temp))) {
        LOG_WARN("failed to push back helper", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformDBlink::add_link_semi_info(SemiInfo *semi_info,
                                          uint64_t dblink_id,
                                          bool is_reverse_link,
                                          ObIArray<LinkTableHelper> &helpers)
{
  int ret = OB_SUCCESS;
  LinkTableHelper *helper = NULL;
  if (OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret));
  }
  for (int64_t i = 0; OB_INVALID_ID != dblink_id && NULL == helper && i < helpers.count(); ++i) {
    if ((dblink_id == helpers.at(i).dblink_id_ ||
        (is_reverse_link && helpers.at(i).is_reverse_link_)) &&
        helpers.at(i).parent_table_ == NULL) {
      helper = &helpers.at(i);
    }
  }
  //find
  if (OB_SUCC(ret) && NULL != helper) {
    if (OB_FAIL(helper->semi_infos_.push_back(semi_info))) {
      LOG_WARN("failed to push back semi info", K(ret));
    }
  }
  // not find
  if (OB_SUCC(ret) && NULL == helper) {
    LinkTableHelper temp;
    temp.dblink_id_ = dblink_id;
    temp.is_reverse_link_ = is_reverse_link;
    if (OB_FAIL(temp.semi_infos_.push_back(semi_info))) {
      LOG_WARN("failed to push back semi info", K(ret));
    } else if (OB_FAIL(helpers.push_back(temp))) {
      LOG_WARN("failed to push back helper", K(ret));
    }
  }
  return ret;
}

int ObTransformDBlink::split_link_table_info(ObDMLStmt *stmt, ObIArray<LinkTableHelper> &helpers)
{
  int ret = OB_SUCCESS;
  ObSEArray<LinkTableHelper, 4> new_helpers;
  ObSEArray<LinkTableHelper, 4> temp_helpers;
  //split table items with cross product
  for (int64_t i = 0; OB_SUCC(ret) && i < helpers.count(); ++i) {
    if (helpers.at(i).table_items_.count() != 1) {
      if (OB_FAIL(inner_split_link_table_info(stmt,
                                              helpers.at(i),
                                              temp_helpers))) {
        LOG_WARN("failed to add helper", K(ret));
      }
    } else if (OB_FAIL(temp_helpers.push_back(helpers.at(i)))) {
      LOG_WARN("failed to add helper", K(ret));
    }
  }
  //remove table item which is generate link table
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_helpers.count(); ++i) {
    if (!(temp_helpers.at(i).table_items_.count() == 1 &&
          temp_helpers.at(i).semi_infos_.empty() &&
          temp_helpers.at(i).conditions_.empty())) {
      if (OB_FAIL(new_helpers.push_back(temp_helpers.at(i)))) {
        LOG_WARN("failed to add helper", K(ret));
      }
    } else if (OB_ISNULL(temp_helpers.at(i).table_items_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (temp_helpers.at(i).table_items_.at(0)->is_generated_table()) {
      //remove this link table info
    } else if (OB_FAIL(new_helpers.push_back(temp_helpers.at(i)))) {
      LOG_WARN("failed to add helper", K(ret));
    }
  }
  if (OB_SUCC(ret) &&
      OB_FAIL(helpers.assign(new_helpers))) {
    LOG_WARN("failed to assign helpers", K(ret));
  }
  return ret;
}

int ObTransformDBlink::inner_split_link_table_info(ObDMLStmt *stmt,
                                                  LinkTableHelper &helper,
                                                  ObIArray<LinkTableHelper> &new_helpers)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_rel_ids_from_tables(stmt,
                                                               helper.table_items_,
                                                               table_ids))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else {
    UnionFind uf(stmt->get_from_item_size());
    if (OB_FAIL(uf.init())) {
      LOG_WARN("failed to init union find", K(ret));
    }
    //compute connect into with pushdown conditions
    for (int64_t i = 0; OB_SUCC(ret) && i < helper.conditions_.count(); ++i) {
      ObRawExpr *expr = helper.conditions_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (!expr->has_flag(IS_JOIN_COND)) {
        //do nothing
      } else if (OB_FAIL(connect_table(stmt, expr, uf))) {
        LOG_WARN("failed to connect table", K(ret));
      }
    }
    //split table item with cross product
    ObSEArray<LinkTableHelper, 4> temp_helpers;
    for (int64_t i = 0; OB_SUCC(ret) && i < helper.table_items_.count(); ++i) {
      TableItem *table = helper.table_items_.at(i);
      bool find = false;
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table", K(ret));
      }
      //find table info with connect info
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < temp_helpers.count(); ++j) {
        TableItem *right_table = NULL;
        if (temp_helpers.at(j).table_items_.empty() ||
            OB_ISNULL(right_table=temp_helpers.at(j).table_items_.at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect empty table items", K(ret));
        } else if (OB_FAIL(uf.is_connected(stmt->get_from_item_idx(table->table_id_),
                                           stmt->get_from_item_idx(right_table->table_id_),
                                           find))) {
          LOG_WARN("failed to check is connect", K(ret));
        } else if (!find) {
          //do nothing
        } else if (OB_FAIL(temp_helpers.at(j).table_items_.push_back(table))) {
          LOG_WARN("failed to push back table", K(ret));
        }
      }
      //create new helper
      if (OB_SUCC(ret) && !find) {
        LinkTableHelper temp;
        temp.dblink_id_ = helper.dblink_id_;
        temp.is_reverse_link_ = helper.is_reverse_link_;
        temp.parent_table_ = helper.parent_table_;
        temp.parent_semi_info_ = helper.parent_semi_info_;
        if (OB_FAIL(temp.table_items_.push_back(table))) {
          LOG_WARN("failed to push back table item", K(ret));
        } else if (OB_FAIL(temp_helpers.push_back(temp))) {
          LOG_WARN("failed to push back helper", K(ret));
        }
      }
    }
    //redistribute conditions
    for (int64_t i = 0; OB_SUCC(ret) && i < helper.conditions_.count(); ++i) {
      ObRawExpr *expr = helper.conditions_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < temp_helpers.count(); ++j) {
        table_ids.reuse();
        if (OB_FAIL(ObTransformUtils::get_rel_ids_from_tables(stmt,
                                                              temp_helpers.at(j).table_items_,
                                                              table_ids))) {
          LOG_WARN("failed to get table ids", K(ret));
        } else if (!table_ids.is_superset(expr->get_relation_ids())) {
          //do nothing
        } else if (OB_FAIL(temp_helpers.at(j).conditions_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
    //redistribute semi infos
    for (int64_t i = 0; OB_SUCC(ret) && i < helper.semi_infos_.count(); ++i) {
      SemiInfo *semi_info = helper.semi_infos_.at(i);
      ObRelIds semi_tables;
      if (OB_ISNULL(semi_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null semi info", K(ret));
      } else if (OB_FAIL(ObTransformUtils::get_rel_ids_from_tables(stmt,
                                                                   semi_info->left_table_ids_,
                                                                   semi_tables))) {
      LOG_WARN("failed to get rel ids", K(ret));
      }
      bool find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < temp_helpers.count(); ++j) {
        table_ids.reuse();
        if (OB_FAIL(ObTransformUtils::get_rel_ids_from_tables(stmt,
                                                              temp_helpers.at(j).table_items_,
                                                              table_ids))) {
          LOG_WARN("failed to get table ids", K(ret));
        } else if (!table_ids.is_superset(semi_tables)) {
          //do nothing
        } else if (OB_FAIL(temp_helpers.at(j).semi_infos_.push_back(semi_info))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else {
          find = true;
        }
      }
      if (OB_SUCC(ret) && !find) {
        TableItem *table = stmt->get_table_item_by_id(semi_info->right_table_id_);
        LinkTableHelper temp;
        temp.dblink_id_ = helper.dblink_id_;
        temp.is_reverse_link_ = helper.is_reverse_link_;
        temp.parent_table_ = helper.parent_table_;
        temp.parent_semi_info_ = helper.parent_semi_info_;
        if (OB_FAIL(temp.table_items_.push_back(table))) {
          LOG_WARN("failed to push back table item", K(ret));
        } else if (OB_FAIL(temp_helpers.push_back(temp))) {
          LOG_WARN("failed to push back helper", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(append(new_helpers, temp_helpers))) {
      LOG_WARN("failed to append helpers", K(ret));
    }
  }
  return ret;
}

int ObTransformDBlink::connect_table(ObDMLStmt *stmt,
                                     ObRawExpr *expr,
                                     UnionFind &uf)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> from_idxs;
  if (OB_FAIL(get_from_item_idx(stmt, expr, from_idxs))) {
    LOG_WARN("failed to get from idxs", K(ret));
  } else if (2 > from_idxs.count()) {
    //do nothing
  } else {
    for (int64_t i = 1; OB_SUCC(ret) && i < from_idxs.count(); ++i) {
      if (OB_FAIL(uf.connect(from_idxs.at(0), from_idxs.at(i)))) {
        LOG_WARN("failed to connect table", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformDBlink::get_from_item_idx(ObDMLStmt *stmt,
                                         ObRawExpr *expr,
                                         ObIArray<int64_t> &idxs)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); ++i) {
    table_ids.reuse();
    if (OB_FAIL(ObTransformUtils::get_rel_ids_from_table(
                        stmt,
                        stmt->get_table_item(stmt->get_from_item(i)),
                        table_ids))) {
      LOG_WARN("failed to get rel ids", K(ret));
    } else if (!table_ids.overlap(expr->get_relation_ids())) {
      //do nothing
    } else if (OB_FAIL(idxs.push_back(i))) {
      LOG_WARN("failed to push back table id", K(ret));
    }
  }
  return ret;
}

int ObTransformDBlink::check_can_pushdown(ObDMLStmt *stmt, const LinkTableHelper &helper, bool &can_push)
{
  int ret = OB_SUCCESS;
  bool is_on_null_side = false;
  JoinedTable *joined_table = helper.parent_table_;
  TableItem *table = NULL;
  if (NULL != joined_table) {
    if (OB_UNLIKELY(1 != helper.table_items_.count()) ||
        OB_ISNULL(table = helper.table_items_.at(0)) ||
        OB_UNLIKELY(table != joined_table->left_table_ && table != joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected push down tables", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(stmt, table->table_id_, is_on_null_side))) {
      LOG_WARN("failed to check table on null side", K(ret));
    }
  }
  can_push = !is_on_null_side;
  return ret;
}

int ObTransformDBlink::collect_pushdown_conditions(ObDMLStmt *stmt, ObIArray<LinkTableHelper> &helpers)
{
  int ret = OB_SUCCESS;
  ObRelIds table_ids;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < helpers.count(); ++i) {
    table_ids.reuse();
    bool can_push = false;
    if (OB_FAIL(check_can_pushdown(stmt, helpers.at(i), can_push))) {
      LOG_WARN("failed to check if conditions can be push", K(ret));
    } else if (!can_push) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::get_rel_ids_from_tables(stmt,
                                                                 helpers.at(i).table_items_,
                                                                 table_ids))) {
      LOG_WARN("failed to get rel ids", K(ret));
    } else {
      bool has_special_expr = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < stmt->get_condition_size(); ++j) {
        ObRawExpr *expr = stmt->get_condition_expr(j);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (!expr->get_relation_ids().is_subset(table_ids)) {
          //do nothing
        } else if (OB_FAIL(has_none_pushdown_expr(expr,
                                                  helpers.at(i).dblink_id_,
                                                  has_special_expr))) {
          LOG_WARN("failed to check has none push down expr", K(ret));
        } else if (has_special_expr) {
          //do nothing
        } else if (OB_FAIL(helpers.at(i).conditions_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformDBlink::has_none_pushdown_expr(ObIArray<ObRawExpr*> &exprs,
                                              uint64_t dblink_id,
                                              bool &has)
{
  int ret = OB_SUCCESS;
  has = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has && i < exprs.count(); ++i) {
    if (OB_FAIL(has_none_pushdown_expr(exprs.at(i), dblink_id, has))) {
      LOG_WARN("failed to check has none pushdown expr", K(ret));
    }
  }
  return ret;
}

int ObTransformDBlink::has_none_pushdown_expr(ObRawExpr* expr,
                                              uint64_t dblink_id,
                                              bool &has)
{
  int ret = OB_SUCCESS;
  has = false;
  bool is_valid = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (OB_FAIL(check_link_expr_valid(expr, is_valid))) {
    LOG_WARN("failed to check link expr valid", K(ret));
  } else if (!is_valid) {
    has = true;
  } else if (expr->has_flag(CNT_ROWNUM) ||
             expr->has_flag(CNT_SEQ_EXPR)) {
    has = true;
  } else if (expr->has_flag(IS_SUB_QUERY)) {
    ObQueryRefRawExpr *query_expr = static_cast<ObQueryRefRawExpr*>(expr);
    if (OB_ISNULL(query_expr->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ref stmt", K(ret));
    } else if (dblink_id != query_expr->get_ref_stmt()->get_dblink_id()) {
      has = true;
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && !has && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(has_none_pushdown_expr(expr->get_param_expr(i),
                                                    dblink_id,
                                                    has)))) {
        LOG_WARN("failed to check expr can pushdown", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformDBlink::inner_pack_link_table(ObDMLStmt *stmt, LinkTableHelper &helper)
{
  int ret = OB_SUCCESS;
  TableItem *view_table = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(reverse_link_tables(*stmt,
                                         helper.table_items_,
                                         helper.semi_infos_,
                                         helper.is_reverse_link_ ? 0
                                         : helper.dblink_id_))) {
    LOG_WARN("failed to reverse link table", K(ret));
  } else if (ObOptimizerUtil::remove_item(stmt->get_condition_exprs(), helper.conditions_)) {
    LOG_WARN("failed to remove item", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                               stmt,
                                                               view_table,
                                                               helper.table_items_,
                                                               &helper.semi_infos_))) {
    LOG_WARN("failed to create empty view", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                          stmt,
                                                          view_table,
                                                          helper.table_items_,
                                                          &helper.conditions_,
                                                          &helper.semi_infos_))) {
    LOG_WARN("failed to create inline view", K(ret));
  } else if (OB_ISNULL(view_table) || OB_ISNULL(view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (OB_FAIL(formalize_link_table(view_table->ref_query_))) {
    LOG_WARN("failed to formalize link table", K(ret));
  } else {
    view_table->ref_query_->set_dblink_id(helper.is_reverse_link_ ? 0
                                         : helper.dblink_id_);
  }
  return ret;
}

int ObTransformDBlink::formalize_link_table(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(formalize_table_name(stmt))) {
    LOG_WARN("failed to formalize table name", K(ret));
  } else if (OB_FAIL(formalize_select_item(stmt))) {
    LOG_WARN("failed to formalize select item", K(ret));
  } else if (OB_FAIL(formalize_bool_select_expr(stmt))) {
    LOG_WARN("failed to formalize bool select item", K(ret));
  } else if (OB_FAIL(formalize_column_item(stmt))) {
    LOG_WARN("failed to formalize column item", K(ret));
  }
  return ret;
}

// For compatibility with Oracle before 12c,
// We can not send the sql with "fetch" clause.
// So, we perform limit operations locally.
int ObTransformDBlink::extract_limit(ObDMLStmt *stmt, ObDMLStmt *&dblink_stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *child_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!stmt->is_select_stmt()
            || !stmt->has_limit()
            || stmt->is_fetch_with_ties()
            || NULL != stmt->get_limit_percent_expr()) {
    dblink_stmt = stmt;
  } else if (ObTransformUtils::pack_stmt(ctx_, static_cast<ObSelectStmt *>(stmt), &child_stmt)) {
    LOG_WARN("failed to pack the stmt", K(ret));
  } else {
    stmt->set_limit_offset(child_stmt->get_limit_expr(), child_stmt->get_offset_expr());
    child_stmt->set_limit_offset(NULL, NULL);
    dblink_stmt = child_stmt;
  }
  return ret;
}

int ObTransformDBlink::formalize_table_name(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 4> table_names;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_items().count(); ++i) {
    TableItem *table = stmt->get_table_item(i);
    bool find = false;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < table_names.count(); ++j) {
      if (table_names.at(j).compare(table->get_table_name()) == 0) {
        find = true;
      }
    }
    if (find || table->get_table_name().empty()) {
      if (OB_FAIL(stmt->generate_view_name(*ctx_->allocator_,
                                           table->alias_name_))) {
        LOG_WARN("failed to generate view name", K(ret));
      }
    } else if (table->is_link_type() && table->alias_name_.empty()) {
      if (OB_FAIL(stmt->generate_view_name(*ctx_->allocator_,
                                           table->alias_name_))) {
        LOG_WARN("failed to generate view name", K(ret));
      }
    }
    if (OB_SUCC(ret) &&
        OB_FAIL(table_names.push_back(table->get_table_name()))) {
      LOG_WARN("failed to push back table name", K(ret));
    }
  }
  return ret;
}

int ObTransformDBlink::formalize_column_item(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else {
    ObIArray<ColumnItem> &column_items = stmt->get_column_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_items.count(); ++i) {
      ObColumnRefRawExpr *col = column_items.at(i).expr_;
      TableItem *table = NULL;
      ObString alias_name;
      if (OB_ISNULL(col) ||
          OB_ISNULL(table=stmt->get_table_item_by_id(col->get_table_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null column expr", K(ret));
      } else if (OB_FALSE_IT(col->set_table_name(table->get_table_name()))) {
      } else if (OB_FALSE_IT(col->set_database_name(table->database_name_))) {
      } else if (table->is_generated_table() || table->is_temp_table()) {
        int64_t sel_idx = col->get_column_id() - OB_APP_MIN_COLUMN_ID;
        if (OB_FAIL(ObTransformUtils::get_real_alias_name(table->ref_query_,
                                                          sel_idx,
                                                          alias_name))) {
          LOG_WARN("failed to get real alias name", K(ret));
        } else {
          col->set_column_name(alias_name);
        }
      }
      if (OB_SUCC(ret) && table->is_link_type()) {
        col->get_database_name().reset();
      }
    }
  }
  return ret;
}

int ObTransformDBlink::formalize_select_item(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  const uint64_t MAX_COLUMN_NAME_LENGTH_ORACLE_11_g = 30;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
    ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
    uint64_t id = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      SelectItem &select_item = select_items.at(i);
      bool need_new_alias = false;
      if (lib::is_oracle_mode() &&
          select_item.alias_name_.length() > MAX_COLUMN_NAME_LENGTH_ORACLE_11_g) {
        // for compatibility with Oracle 11 g,
        // rename the overlength expr.
        need_new_alias = true;
      }
      for (int64_t j = 0; !need_new_alias && j < i; j ++) {
        const ObString &tname = select_items.at(j).alias_name_.empty() ?
                                select_items.at(j).expr_name_ :
                                select_items.at(j).alias_name_ ;
        if (0 == select_item.alias_name_.case_compare(tname)) {
          need_new_alias = true;
        }
      }
      if (need_new_alias) {
        int64_t pos = 0;
        const uint64_t OB_MAX_SUBQUERY_NAME_LENGTH = 64;
        const char *ALIAS_NAME = "ALIAS";
        char buf[OB_MAX_SUBQUERY_NAME_LENGTH];
        int64_t buf_len = OB_MAX_SUBQUERY_NAME_LENGTH;
        if (OB_FAIL(BUF_PRINTF("%s", ALIAS_NAME))) {
          LOG_WARN("append name to buf error", K(ret));
        } else {
          bool find_unique = false;
          int64_t old_pos = pos;
          do {
            pos = old_pos;
            id ++;
            if (OB_FAIL(BUF_PRINTF("%ld", id))) {
              LOG_WARN("Append idx to stmt_name error", K(ret));
            } else {
              bool find_dup = false;
              for (int64_t j = 0; !find_dup && j < select_items.count(); ++j) {
                const ObString &tname = select_items.at(j).alias_name_.empty() ?
                                        select_items.at(j).expr_name_ :
                                        select_items.at(j).alias_name_ ;
                if (0 == tname.case_compare(buf)) {
                  find_dup = true;
                }
              }
              find_unique = !find_dup;
            }
          } while(!find_unique && OB_SUCC(ret));
        }
        if (OB_SUCC(ret)) {
          ObString generate_name(pos, buf);
          if (OB_FAIL(ob_write_string(*ctx_->allocator_, generate_name, select_item.alias_name_))) {
            LOG_WARN("failed to write string", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformDBlink::formalize_bool_select_expr(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (!lib::is_oracle_mode()) {
    // do nothing
  } else if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (stmt->is_select_stmt()) {
    // formalize bool select expr p like `a > b` as
    // case when p then 1 when not p then 0 else null
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
    ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      SelectItem &select_item = select_items.at(i);
      ObCaseOpRawExpr *case_when_expr = NULL;
      ObRawExpr *bool_expr = select_item.expr_;
      ObRawExpr *not_expr = NULL;
      ObConstRawExpr *one_expr = NULL;
      ObConstRawExpr *zero_expr = NULL;
      ObRawExpr *null_expr = NULL;
      bool is_bool_expr = false;
      if (OB_ISNULL(select_item.expr_)) {
        LOG_WARN("unexpected select item", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::check_is_bool_expr(select_item.expr_, is_bool_expr))) {
        LOG_WARN("failed to check is bool expr", K(ret));
      } else if (!is_bool_expr) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                              ObIntType,
                                                              1,
                                                              one_expr))) {
        LOG_WARN("failed to build const number expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_not_expr(*ctx_->expr_factory_,
                                                        bool_expr,
                                                        not_expr))) {
        LOG_WARN("failed to build not expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                              ObIntType,
                                                              0,
                                                              zero_expr))) {
        LOG_WARN("failed to build const number expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, null_expr))) {
        LOG_WARN("faile to build null expr", K(ret));
      } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_CASE, case_when_expr))) {
        LOG_WARN("create case expr failed", K(ret));
      } else if (OB_ISNULL(case_when_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(case_when_expr));
      } else if (OB_FAIL(case_when_expr->add_when_param_expr(bool_expr))) {
        LOG_WARN("failed to add when param expr", K(ret));
      } else if (OB_FAIL(case_when_expr->add_then_param_expr(one_expr))) {
        LOG_WARN("failed to add then expr", K(ret));
      } else if (OB_FAIL(case_when_expr->add_when_param_expr(not_expr))) {
        LOG_WARN("failed to add when param expr", K(ret));
      } else if (OB_FAIL(case_when_expr->add_then_param_expr(zero_expr))) {
        LOG_WARN("failed to add then expr", K(ret));
      } else {
        case_when_expr->set_default_param_expr(null_expr);
        select_item.expr_ = case_when_expr;
        if (OB_FAIL(case_when_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("failed to formalize expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformDBlink::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                      const int64_t current_level,
                                      const ObDMLStmt &stmt,
                                      bool &need_trans)
{
  UNUSED(parent_stmts);
  UNUSED(current_level);
  UNUSED(stmt);
  need_trans = true;
  return OB_SUCCESS;
}

int ObTransformDBlink::check_link_oracle(int64_t dblink_id, bool &link_oracle)
{
  int ret = OB_SUCCESS;
  link_oracle = false;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->sql_schema_guard_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else {
    int64_t tenant_id = ctx_->session_info_->get_effective_tenant_id();
    const ObDbLinkSchema *dblink_schema = NULL;
    if (OB_FAIL(ctx_->sql_schema_guard_->get_dblink_schema(tenant_id,
                                                           dblink_id,
                                                           dblink_schema))) {
      LOG_WARN("failed to get dblink schema", K(ret));
    } else if (OB_ISNULL(dblink_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null dblink schema", K(ret));
    } else if (common::sqlclient::DBLINK_DRV_OCI == dblink_schema->get_driver_proto()) {
      link_oracle = true;
    }
  }
  return ret;
}

int ObTransformDBlink::add_flashback_query_for_dblink(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  const ObDbLinkSchema *dblink_schema = NULL;
  ObConstRawExpr *c_expr = NULL;
  uint64_t current_scn = OB_INVALID_ID;
  bool need_add = false;
  uint64_t tenant_id = OB_INVALID_ID;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->sql_schema_guard_) ||
      OB_ISNULL(ctx_->session_info_) ||
      OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret), K(ctx_), K(stmt));
  } else {
    tenant_id = ctx_->session_info_->get_effective_tenant_id();
    for (int64_t i = 0; i < stmt->get_table_items().count() && OB_SUCC(ret); i++) {
      TableItem *table_item = stmt->get_table_item(i);
      uint64_t dblink_id = OB_INVALID_ID;
      bool need_add = false;
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null param", K(ret));
      } else if (!table_item->is_link_table()
                 || TableItem::NOT_USING != table_item->flashback_query_type_) {
      // do nothing if not dblink table or already have flashback query
      } else if (FALSE_IT(dblink_id = table_item->dblink_id_)) {
      } else if (table_item->is_reverse_link_) {
        need_add = true;
      } else if (OB_FAIL(ctx_->sql_schema_guard_->get_dblink_schema(tenant_id, dblink_id, dblink_schema))) {
        LOG_WARN("failed to get dblink schema", K(ret), K(tenant_id), K(dblink_id));
      } else if (OB_ISNULL(dblink_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dblink schema is null", K(ret), K(tenant_id), K(dblink_id), KPC(table_item));
      } else if (static_cast<common::sqlclient::DblinkDriverProto>(dblink_schema->get_driver_proto()) != common::sqlclient::DBLINK_DRV_OB) {
        // do nothing if not connect to ob
      } else {
        need_add = true;
      }
      if (OB_FAIL(ret) || !need_add) {
      } else if (OB_ISNULL(stmt->get_query_ctx()) || OB_ISNULL(ctx_->expr_factory_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query ctx or expr factory is null", K(ret), K(stmt->get_query_ctx()),
                K(ctx_->expr_factory_));
      } else if (OB_FAIL(stmt->get_query_ctx()->sql_schema_guard_.get_link_current_scn(dblink_id,
                tenant_id, ctx_->session_info_, current_scn))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get dblink current scn failed", K(ret), K(dblink_id));
        }
      } else if (OB_INVALID_ID == current_scn) {
        // remote server not support current_scn function, do nothing
      } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_INT, c_expr))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else if (OB_ISNULL(c_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("const expr is null");
      } else {
        ObObj val;
        val.set_uint64(current_scn);
        c_expr->set_value(val);
        c_expr->set_param(val);
        table_item->flashback_query_expr_ = c_expr;
        table_item->flashback_query_type_ = TableItem::USING_SCN;
      }
    }
  }
  return ret;
}

}
}
