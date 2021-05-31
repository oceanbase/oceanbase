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
#include "sql/resolver/ob_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/ob_stmt_resolver.h"
//#include "sql/resolver/ob_schema_checker.h"
#include "share/schema/ob_table_schema.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_column_schema.h"
#include "sql/ob_sql_context.h"
#include "common/ob_field.h"
#include "sql/parser/ob_parser.h"
#include "common/sql_mode/ob_sql_mode.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {

// query_ctx_ no deep_copy
// child_stmts_ no deep_copy
// parent_stmt no deep_copy
// synonym_is_store no deep_copy
int ObStmt::assign(const ObStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(synonym_id_store_.assign(other.synonym_id_store_))) {
    LOG_WARN("failed to assign synonym id store", K(ret));
  } else {
    stmt_type_ = other.stmt_type_;
    literal_stmt_type_ = other.literal_stmt_type_;
    sql_stmt_ = other.sql_stmt_;
    query_ctx_ = other.query_ctx_;
    tz_info_ = other.tz_info_;
    stmt_id_ = other.stmt_id_;
  }
  return ret;
}

int ObStmt::deep_copy(const ObStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObStmt::check_table_id_exists(uint64_t table_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_HASH_EXIST == (ret = get_query_ctx()->table_ids_.exist_refactored(table_id))) {
    ret = OB_SUCCESS;
    is_exist = true;
  } else if (OB_HASH_NOT_EXIST == ret) {
    if (OB_FAIL(get_query_ctx()->table_ids_.set_refactored(table_id))) {
      SQL_RESV_LOG(WARN, "insert table_id to set failed", K(table_id), K(ret));
    }
  } else {
    SQL_RESV_LOG(WARN, "check table_id in table_ids set failed", K(table_id));
  }
  return ret;
}

ObIArray<ObRawExpr*>& ObStmt::get_exec_param_ref_exprs()
{
  return query_ctx_->exec_param_ref_exprs_;
}

ObIArray<ObRawExpr*>& ObStmt::get_exec_param_ref_exprs() const
{
  return query_ctx_->exec_param_ref_exprs_;
}

bool ObStmt::get_fetch_cur_time() const
{
  return query_ctx_->fetch_cur_time_;
}

int64_t ObStmt::get_pre_param_size() const
{
  return query_ctx_->question_marks_count_;
}

void ObStmt::increase_question_marks_count()
{
  ++query_ctx_->question_marks_count_;
}

int64_t ObStmt::get_question_marks_count() const
{
  return query_ctx_->question_marks_count_;
}

int ObStmt::add_calculable_item(const ObHiddenColumnItem& calcuable_item)
{
  increase_question_marks_count();
  return query_ctx_->calculable_items_.push_back(calcuable_item);
}

const ObIArray<ObHiddenColumnItem>& ObStmt::get_calculable_exprs() const
{
  return query_ctx_->calculable_items_;
}

common::ObIArray<ObHiddenColumnItem>& ObStmt::get_calculable_exprs()
{
  return query_ctx_->calculable_items_;
}

void ObStmt::set_stmt_id()
{
  if (NULL == query_ctx_) {
    stmt_id_ = 0;
  } else {
    stmt_id_ = query_ctx_->get_new_stmt_id();
  }
}

int ObStmt::get_stmt_name_by_id(int64_t stmt_id, ObString& stmt_name) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query_ctx_ is NULL", K(ret));
  } else if (OB_FAIL(query_ctx_->get_stmt_name_by_id(stmt_id, stmt_name))) {
    LOG_WARN("fail to get stmt name by id", K(ret), K(stmt_id));
  } else { /*do nothing*/
  }
  return ret;
}

int ObStmt::get_first_stmt(common::ObString& first_stmt)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SQL_PARSER);
  ObSEArray<ObString, 1> queries;
  ObMPParseStat parse_stat;
  ObParser parser(allocator, DEFAULT_OCEANBASE_MODE);
  if (OB_FAIL(parser.split_multiple_stmt(get_sql_stmt(), queries, parse_stat, true /* return the first stmt */))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get first statement from multiple statements failed", K(ret));
  } else if (0 == queries.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get non-statement from multiple statements", K(ret));
  } else {
    first_stmt = queries.at(0);
  }
  return ret;
}

int ObStmt::get_stmt_org_name_by_id(int64_t stmt_id, common::ObString& org_name) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query_ctx_ is NULL", K(ret));
  } else if (OB_FAIL(query_ctx_->get_stmt_org_name_by_id(stmt_id, org_name))) {
    LOG_WARN("fail to get stmt name by id", K(ret), K(stmt_id));
  } else { /*do nothing*/
  }
  return ret;
}

int ObStmt::get_stmt_name(ObString& stmt_name) const
{
  return get_stmt_name_by_id(stmt_id_, stmt_name);
}

int ObStmt::get_stmt_org_name(common::ObString& org_name) const
{
  return get_stmt_org_name_by_id(stmt_id_, org_name);
}

int ObStmt::distribute_hint_in_query_ctx(common::ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Query ctx is NULL", K(ret));
  } else if (OB_FAIL(query_ctx_->generate_stmt_name(allocator))) {
    LOG_WARN("Failed to generate stmt name", K(ret));
  } else if (OB_FAIL(query_ctx_->distribute_hint_to_stmt())) {
    LOG_WARN("Failed to distribute hint to stmt", K(ret));
  } else {
  }
  return ret;
}

ObStmt::~ObStmt()
{}

int ObStmt::check_synonym_id_exist(uint64_t synonym_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < synonym_id_store_.count(); ++i) {
    if (synonym_id == synonym_id_store_.at(i)) {
      is_exist = true;
    }
  }
  return ret;
}

int ObStmt::add_synonym_ids(const ObIArray<uint64_t>& synonym_ids, bool error_with_exist)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < synonym_ids.count(); ++i) {
    uint64_t cur_synonym_id = synonym_ids.at(i);
    if (OB_FAIL(check_synonym_id_exist(cur_synonym_id, is_exist))) {
      LOG_WARN("fail to check synoym id exist", K(cur_synonym_id), K(ret));
    } else if (is_exist) {
      if (error_with_exist) {
        ret = OB_ERR_LOOP_OF_SYNONYM;
        LOG_WARN("looping chain of synonyms", K(cur_synonym_id), K(ret));
      }
    } else if (OB_FAIL(synonym_id_store_.push_back(cur_synonym_id))) {
      LOG_WARN("fail to add synonym id", K(cur_synonym_id), K(ret));
    }
  }
  return ret;
}

int ObStmt::add_global_dependency_table(const ObSchemaObjVersion& dependency_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_query_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("query_ctx is null");
  } else {
    bool is_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < get_query_ctx()->global_dependency_tables_.count(); ++i) {
      const ObSchemaObjVersion& obj_version = get_query_ctx()->global_dependency_tables_.at(i);
      // the operation in plan cache is different for explicit and implicit dbname,
      // so we need make a judgment here.
      if (obj_version.get_object_id() == dependency_table.get_object_id() &&
          obj_version.object_type_ == dependency_table.object_type_ &&
          obj_version.is_db_explicit() == dependency_table.is_db_explicit()) {
        is_found = true;
      }
    }
    if (OB_SUCC(ret) && !is_found) {
      ret = get_query_ctx()->global_dependency_tables_.push_back(dependency_table);
    }
  }
  return ret;
}

int ObStmt::add_table_stat_version(const ObOptTableStatVersion& table_stat_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_query_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("query_ctx is null");
  } else {
    bool is_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < get_query_ctx()->table_stat_versions_.count(); ++i) {
      const ObOptTableStatVersion& stat_version = get_query_ctx()->table_stat_versions_.at(i);
      if (stat_version.key_.table_id_ == table_stat_version.key_.table_id_) {
        is_found = true;
      }
    }
    if (OB_SUCC(ret) && !is_found) {
      ret = get_query_ctx()->table_stat_versions_.push_back(table_stat_version);
    }
  }
  return ret;
}

const ObIArray<ObSchemaObjVersion>* ObStmt::get_global_dependency_table() const
{
  const ObIArray<ObSchemaObjVersion>* ret = NULL;
  if (query_ctx_ != NULL) {
    ret = &(query_ctx_->global_dependency_tables_);
  }
  return ret;
}

ObIArray<share::schema::ObSchemaObjVersion>* ObStmt::get_global_dependency_table()
{
  ObIArray<ObSchemaObjVersion>* ret = NULL;
  if (query_ctx_ != NULL) {
    ret = &(query_ctx_->global_dependency_tables_);
  }
  return ret;
}

const ObIArray<ObOptTableStatVersion>* ObStmt::get_table_stat_versions() const
{
  const ObIArray<ObOptTableStatVersion>* ret = NULL;
  if (query_ctx_ != NULL) {
    ret = &(query_ctx_->table_stat_versions_);
  }
  return ret;
}

void ObStmtFactory::destory()
{
  DLIST_FOREACH_NORET(node, stmt_store_.get_obj_list())
  {
    if (node != NULL && node->get_obj() != NULL) {
      node->get_obj()->~ObStmt();
    }
  }
  stmt_store_.destory();
  if (query_ctx_ != NULL) {
    query_ctx_->~ObQueryCtx();
    query_ctx_ = NULL;
  }
}

ObQueryCtx* ObStmtFactory::get_query_ctx()
{
  void* ptr = NULL;
  if (NULL == query_ctx_) {
    if ((ptr = allocator_.alloc(sizeof(ObQueryCtx))) != NULL) {
      query_ctx_ = new (ptr) ObQueryCtx();
    } else {
      LOG_WARN("create query ctx failed", "query_ctx size", sizeof(ObQueryCtx));
    }
  }
  return query_ctx_;
}

template <>
int ObStmtFactory::create_stmt<ObSelectStmt>(ObSelectStmt*& stmt)
{
  int ret = common::OB_SUCCESS;
  void* ptr = NULL;
  if (free_list_.get_obj_list().is_empty()) {
    ptr = allocator_.alloc(sizeof(ObSelectStmt));
  } else {
    stmt = free_list_.get_obj_list().remove_first()->get_obj();
    stmt->~ObSelectStmt();
    ptr = stmt;
  }
  stmt = NULL;
  if (OB_UNLIKELY(NULL == ptr)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "no more memory to stmt");
  } else {
    stmt = new (ptr) ObSelectStmt();
    if (OB_FAIL(stmt_store_.store_obj(stmt))) {
      SQL_RESV_LOG(WARN, "store stmt failed", K(ret));
      stmt->~ObSelectStmt();
      stmt = NULL;
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
