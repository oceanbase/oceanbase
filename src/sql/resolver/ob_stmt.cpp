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

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

// query_ctx_ no deep_copy
// child_stmts_ no deep_copy
// parent_stmt no deep_copy
// synonym_is_store no deep_copy
int ObStmt::assign(const ObStmt &other)
{
  int ret = OB_SUCCESS;
  stmt_type_ = other.stmt_type_;
  query_ctx_ = other.query_ctx_;
  stmt_id_ = other.stmt_id_;
  return ret;
}

int ObStmt::deep_copy(const ObStmt &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else { /*do nothing*/ }
  return ret;
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

int ObStmt::add_calculable_item(const ObHiddenColumnItem &calcuable_item)
{
  increase_question_marks_count();
  return query_ctx_->calculable_items_.push_back(calcuable_item);
}

const ObIArray<ObHiddenColumnItem> &ObStmt::get_calculable_exprs() const
{
  return query_ctx_->calculable_items_;
}

common::ObIArray<ObHiddenColumnItem> &ObStmt::get_calculable_exprs()
{
  return query_ctx_->calculable_items_;
}

int ObStmt::set_stmt_id()
{
  int ret = OB_SUCCESS;
  if(NULL == query_ctx_) {
    stmt_id_ = OB_INVALID_STMT_ID;
  } else {
    stmt_id_ = query_ctx_->get_new_stmt_id();
    ret = query_ctx_->query_hint_.add_stmt_id_map(stmt_id_, get_stmt_type());
  }
  return ret;
}

int ObStmt::get_first_stmt(common::ObString &first_stmt)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SQL_PARSER);
  ObSEArray<ObString, 1> queries;
  ObMPParseStat parse_stat;
  ObParser parser(allocator, DEFAULT_OCEANBASE_MODE);
  if (OB_ISNULL(query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ctx is null", K(ret));
  } else if (OB_FAIL(parser.split_multiple_stmt(query_ctx_->get_sql_stmt(), queries, parse_stat,
                                         true /* return the first stmt */))) {
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

ObStmt::~ObStmt()
{
}

int ObStmt::add_global_dependency_table(const ObSchemaObjVersion &dependency_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_query_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("query_ctx is null");
  } else {
    bool is_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < get_query_ctx()->global_dependency_tables_.count(); ++i) {
      const ObSchemaObjVersion &obj_version = get_query_ctx()->global_dependency_tables_.at(i);
      //此处需要判断是否显示给db名，因为同名表情况对于指定db和不指定db在plan cache中处理不一样
      if (obj_version.get_object_id() == dependency_table.get_object_id()
          && obj_version.object_type_ == dependency_table.object_type_
          && obj_version.is_db_explicit() == dependency_table.is_db_explicit()) {
        is_found = true;
      }
    }
    if (OB_SUCC(ret) && !is_found) {
      ret = get_query_ctx()->global_dependency_tables_.push_back(dependency_table);
    }
  }
  return ret;
}


const ObIArray<ObSchemaObjVersion> *ObStmt::get_global_dependency_table() const
{
  const ObIArray<ObSchemaObjVersion> *ret = NULL;
  if (query_ctx_ != NULL) {
    ret = &(query_ctx_->global_dependency_tables_);
  }
  return ret;
}

ObIArray<share::schema::ObSchemaObjVersion> *ObStmt::get_global_dependency_table()
{
  ObIArray<ObSchemaObjVersion> *ret = NULL;
  if (query_ctx_ != NULL) {
    ret = &(query_ctx_->global_dependency_tables_);
  }
  return ret;
}


int ObStmt::add_ref_obj_version(const uint64_t dep_obj_id,
                                const uint64_t dep_db_id,
                                const ObObjectType dep_obj_type,
                                const ObSchemaObjVersion &ref_obj_version,
                                common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_query_ctx())) {
    ret = OB_NOT_INIT;
    LOG_WARN("query_ctx is null");
  } else if (OB_FAIL(get_query_ctx()->reference_obj_tables_.add_ref_obj_version(
             dep_obj_id, dep_db_id, dep_obj_type, ref_obj_version, allocator))) {
    LOG_WARN("failed to add reference obj version", K(ret));
  }
  return ret;
}

const ObReferenceObjTable *ObStmt::get_ref_obj_table() const
{
  const ObReferenceObjTable *ret = nullptr;
  if (query_ctx_ != nullptr) {
    ret = &(query_ctx_->reference_obj_tables_);
  }
  return ret;
}

ObReferenceObjTable *ObStmt::get_ref_obj_table()
{
  ObReferenceObjTable *ret = nullptr;
  if (query_ctx_ != nullptr) {
    ret = &(query_ctx_->reference_obj_tables_);
  }
  return ret;
}

void ObStmtFactory::destory()
{
  DLIST_FOREACH_NORET(node, stmt_store_.get_obj_list()) {
    if (node != NULL && node->get_obj() != NULL) {
      node->get_obj()->~ObStmt();
    }
  }
  stmt_store_.destroy();
  if (query_ctx_ != NULL) {
    query_ctx_->~ObQueryCtx();
    query_ctx_ = NULL;
  }
}

ObQueryCtx *ObStmtFactory::get_query_ctx()
{
  void *ptr = NULL;
  if (NULL == query_ctx_) {
    if ((ptr = allocator_.alloc(sizeof(ObQueryCtx))) != NULL) {
      query_ctx_ = new(ptr) ObQueryCtx();
    } else {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "create query ctx failed", "query_ctx size", sizeof(ObQueryCtx));
    }
  }
  return query_ctx_;
}

int ObStmtFactory::free_stmt(ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObObjNode<ObStmt*> *del_node = NULL;
  DLIST_FOREACH_NORET(node, stmt_store_.get_obj_list()) {
    if (node != NULL && node->get_obj() == stmt) {
      del_node = node;
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(stmt_store_.get_obj_list().remove(del_node))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is not found", K(ret));
    } else if (OB_FAIL(free_list_.store_obj(stmt))) {
      LOG_WARN("failed to store stmt", K(ret));
    } else {
      stmt->~ObSelectStmt();
    }
  }
  return ret;
}

template<>
int ObStmtFactory::create_stmt<ObSelectStmt>(ObSelectStmt *&stmt)
{
  int ret = common::OB_SUCCESS;
  void *ptr = NULL;
  if (free_list_.get_obj_list().is_empty()) {
    ptr = allocator_.alloc(sizeof(ObSelectStmt));
  } else {
    stmt = free_list_.get_obj_list().remove_first()->get_obj();
    ptr = stmt;
  }
  stmt = NULL;
  if (OB_UNLIKELY(NULL == ptr)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "no more memory to stmt");
  } else {
    stmt = new(ptr) ObSelectStmt();
    if (OB_FAIL(stmt_store_.store_obj(stmt))) {
      SQL_RESV_LOG(WARN, "store stmt failed", K(ret));
      stmt->~ObSelectStmt();
      stmt = NULL;
    } else if (OB_FAIL(stmt->init_stmt(table_hash_allocator_, wrapper_allocator_))) {
      LOG_WARN("failed to init tables hash", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
