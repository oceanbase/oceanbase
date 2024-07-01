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
#include "sql/resolver/ob_stmt_resolver.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/ob_field.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

uint64_t ObStmtResolver::generate_table_id()
{
  if (NULL != params_.query_ctx_) {
    return params_.query_ctx_->available_tb_id_--;
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "query ctx pointer is null");
    return OB_INVALID_ID;
  }
}

int ObStmtResolver::resolve_table_relation_node(const ParseNode *node,
                                                ObString &table_name,
                                                ObString &db_name,
                                                bool is_org/*false*/,
                                                bool is_oracle_sys_view,
                                                char **dblink_name_ptr,
                                                int32_t *dblink_name_len,
                                                bool *has_dblink_node)
{
  int ret = OB_SUCCESS;
  bool is_db_explicit = false;
  UNUSED(is_db_explicit);
  if (OB_FAIL(resolve_table_relation_node_v2(node,
                                             table_name,
                                             db_name,
                                             is_db_explicit,
                                             is_org,
                                             is_oracle_sys_view,
                                             dblink_name_ptr,
                                             dblink_name_len,
                                             has_dblink_node))) {
    LOG_WARN("failed to resolve table name", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

// description: 解析关联表
//
// @param [in] node         与关联表相关的节点
// @param [out] table_name  向 arg 中填充的关联表名称
// @param [out] db_name     向 arg 中填充的关联库名称

// @return oceanbase error code defined in lib/ob_errno.def
int ObStmtResolver::resolve_table_relation_node_v2(const ParseNode *node,
                                                   ObString &table_name,
                                                   ObString &db_name,
                                                   bool &is_db_explicit,
                                                   bool is_org/*false*/,
                                                   bool is_oracle_sys_view,
                                                   char **dblink_name_ptr,
                                                   int32_t *dblink_name_len,
                                                   bool *has_dblink_node)
{
  int ret = OB_SUCCESS;
  is_db_explicit = false;
  ParseNode *db_node = node->children_[0];
  ParseNode *relation_node = node->children_[1];
  int32_t table_len = static_cast<int32_t>(relation_node->str_len_);
  table_name.assign_ptr(const_cast<char*>(relation_node->str_value_), table_len);
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_NOT_NULL(has_dblink_node)) {
    *has_dblink_node = false;
    if (node->num_child_ >= 3 && NULL != node->children_[2]) {
      *has_dblink_node = true;
    }
  }
  if (NULL != dblink_name_ptr &&
      NULL != dblink_name_len &&
      node->num_child_ >= 3 && 
      NULL != node->children_[2] && 
      T_DBLINK_NAME == node->children_[2]->type_ &&
      NULL != node->children_[2]->children_ &&
      2 == node->children_[2]->num_child_ &&
      NULL != node->children_[2]->children_[0] &&
      NULL != node->children_[2]->children_[1]) {
    //Obtaining dblink_name here is not to obtain the dblink name itself, but to determine whether there is an opt_dblink node
    ParseNode *dblink_name_node = node->children_[2];
    if (node->children_[2]->children_[1]->value_) { // dblink name is @!
      *dblink_name_ptr = const_cast<char*>(node->children_[2]->children_[0]->str_value_);
      *dblink_name_len = 1;
    } else { // dblink name is @xxxx or @, @ will be skip before here
      *dblink_name_ptr = const_cast<char*>(node->children_[2]->children_[0]->str_value_);
      *dblink_name_len = static_cast<int32_t>(node->children_[2]->children_[0]->str_len_);
    }
  }
  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
    SERVER_LOG(WARN, "fail to get name case mode", K(mode), K(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else {
    bool perserve_lettercase = lib::is_oracle_mode() ?
        true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
    int tmp_ret = ObSQLUtils::check_and_convert_table_name(cs_type, perserve_lettercase, table_name);
    //因索引表存在前缀,故第1次检查table_name超长时,需要继续获取db信息以判断是否索引表
    if (OB_SUCCESS == tmp_ret || OB_ERR_TOO_LONG_IDENT == tmp_ret
        || (session_info_->get_ddl_info().is_ddl() && OB_WRONG_TABLE_NAME == tmp_ret)) {
      if (NULL == db_node) {
        if (is_oracle_sys_view) {
          // ObString tmp(OB_ORA_SYS_SCHEMA_NAME); // right code
          ObString tmp("SYS");
          if (OB_FAIL(ob_write_string(*allocator_, tmp, db_name))) {
            LOG_WARN("fail to write db name", K(ret));
          }
        } else if (is_org || params_.is_resolve_fake_cte_table_) {
          db_name = ObString::make_empty_string();
        } else if (session_info_->get_database_name().empty()) {
          ret = OB_ERR_NO_DB_SELECTED;
          LOG_WARN("No database selected");
        } else {
          db_name = session_info_->get_database_name();
        }
      } else {
        is_db_explicit = true;
        int32_t db_len = static_cast<int32_t>(db_node->str_len_);
        db_name.assign_ptr(const_cast<char*>(db_node->str_value_), db_len);
        if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(cs_type, perserve_lettercase, db_name))) {
          LOG_WARN("fail to check and convert database name", K(db_name), K(ret));
        } else {
          CK (OB_NOT_NULL(schema_checker_->get_schema_guard()));
          OZ (ObSQLUtils::cvt_db_name_to_org(*schema_checker_->get_schema_guard(),
                                             session_info_,
                                             db_name,
                                             allocator_));
        }
      }
      if (OB_SUCCESS == ret && (OB_ERR_TOO_LONG_IDENT == tmp_ret || OB_WRONG_TABLE_NAME == tmp_ret)) {
         //直接查询索引表时,因索引前缀放宽表名长度限制
         stmt::StmtType stmt_type = (NULL == get_basic_stmt()) ? stmt::T_NONE : get_basic_stmt()->get_stmt_type();
         bool is_index_table = false;
         uint64_t tenant_id = session_info_->get_effective_tenant_id();
         const bool is_hidden = session_info_->is_table_name_hidden();
         if (OB_FAIL(schema_checker_->check_table_exists(tenant_id, db_name, table_name, true, is_hidden, is_index_table))) {
           LOG_WARN("fail to check and convert table name", K(tenant_id), K(db_name), K(table_name), K(ret));
         } else if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(cs_type, perserve_lettercase, table_name, stmt_type, is_index_table))) {
           LOG_WARN("fail to check and convert table name", K(table_name), K(stmt_type), K(is_index_table), K(ret));
         }
      } else if (OB_ERR_TOO_LONG_IDENT == tmp_ret) {
        //为与mysql兼容,优先返回第1次检查表名的错误码
        ret = tmp_ret;
        LOG_WARN("fail to check and convert table name", K(table_name), K(ret));
      } else {  } // do  nothing
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to check and convert table name", K(table_name), K(ret));
    }
  }
  return ret;
}

int ObStmtResolver::resolve_dblink_name(const ParseNode *table_node, uint64_t tenant_id, ObString &dblink_name, bool &is_reverse_link, bool &has_dblink_node)
{
  int ret = OB_SUCCESS;
  dblink_name.reset();
  if (!OB_ISNULL(table_node) && table_node->num_child_ > 2 &&
      !OB_ISNULL(table_node->children_) && !OB_ISNULL(table_node->children_[2])) {
    const ParseNode *dblink_node = table_node->children_[2];
    has_dblink_node = true;
    if (!lib::is_oracle_mode()) {
      uint64_t compat_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
        LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
      } else if (compat_version < DATA_VERSION_4_2_0_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("mysql dblink is not supported when MIN_DATA_VERSION is below DATA_VERSION_4_2_0_0", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (2 == dblink_node->num_child_ && !OB_ISNULL(dblink_node->children_) &&
        !OB_ISNULL(dblink_node->children_[0])) {
      int32_t dblink_name_len = static_cast<int32_t>(dblink_node->children_[0]->str_len_);
      dblink_name.assign_ptr(dblink_node->children_[0]->str_value_, dblink_name_len);
      if (!OB_ISNULL(dblink_node->children_[1])) {
        is_reverse_link = dblink_node->children_[1]->value_;
      }
    }
  }
  return ret;
}

int ObStmtResolver::resolve_ref_factor(const ParseNode *node, 
                                       ObSQLSessionInfo *session_info, 
                                       ObString &table_name, 
                                       ObString &db_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(node), K(session_info), K(ret));
  } else {
    ParseNode *db_node = node->children_[0];
    ParseNode *relation_node = node->children_[1];
    int32_t table_len = static_cast<int32_t>(relation_node->str_len_);
    table_name.assign_ptr(const_cast<char*>(relation_node->str_value_), table_len);
    ObNameCaseMode mode = OB_NAME_CASE_INVALID;
    ObCollationType cs_type = CS_TYPE_INVALID;
    if (OB_FAIL(session_info->get_name_case_mode(mode))) {
      SERVER_LOG(WARN, "fail to get name case mode", K(mode), K(ret));
    } else if (OB_FAIL(session_info->get_collation_connection(cs_type))) {
      LOG_WARN("fail to get collation_connection", K(ret));
    } else {
      bool perserve_lettercase = lib::is_oracle_mode() ?
          true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
      if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(cs_type, perserve_lettercase, table_name))) {
        LOG_WARN("fail to check and convert relation name", K(table_name), K(ret));
      } else {
        if (NULL == db_node) {
          if (session_info->get_database_name().empty()) {
            ret = OB_ERR_NO_DB_SELECTED;
            LOG_WARN("No database selected");
          } else {
            db_name = session_info->get_database_name();
          }
        } else {
          int32_t db_len = static_cast<int32_t>(db_node->str_len_);
          db_name.assign_ptr(const_cast<char*>(db_node->str_value_), db_len);
          if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(cs_type, perserve_lettercase, db_name))) {
            LOG_WARN("fail to check and convert database name", K(db_name), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObStmtResolver::resolve_database_factor(const ParseNode *node,
                                            uint64_t tenant_id,
                                            uint64_t &database_id,
                                            ObString &db_name)
{
  int ret = OB_SUCCESS;
  database_id = OB_INVALID_ID;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node is NULL", K(ret));
  } else if (OB_UNLIKELY(T_IDENT != node->type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node type is not T_IDENT", K(ret), K(node->type_));
  } else if (FALSE_IT(db_name.assign_ptr(const_cast<char*>(node->str_value_),
                                  static_cast<int32_t>(node->str_len_)))) {
    // won't be here
  } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, db_name, database_id))) {
    LOG_USER_ERROR(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
  }
  return ret;
}

int ObStmtResolver::normalize_table_or_database_names(ObString &name)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode;
  if (name.empty() || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name is empty", K(name), K(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", K(ret));
  } else if (lib::is_oracle_mode()) {
    /* ^-^ */
  } else if (OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
    ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, name);
  }
  
  return ret;
}

int ObSynonymChecker::add_synonym_id(uint64_t synonym_id, uint64_t database_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < synonym_ids_.count(); ++i) {
    if (OB_UNLIKELY(synonym_id == synonym_ids_.at(i))) {
      ret = OB_ERR_LOOP_OF_SYNONYM;
      LOG_WARN("looping chain of synonyms", K(synonym_id), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(synonym_ids_.push_back(synonym_id))) {
    LOG_WARN("fail to add synonym_id", K(synonym_id), K(ret));
  } else if (OB_FAIL(database_ids_.push_back(database_id))) {
    LOG_WARN("fail to add database id", K(database_id), K(ret));
  }
  return ret;
}

int ObStmtResolver::get_column_schema(const uint64_t table_id,
    const ObString &column_name,
    const share::schema::ObColumnSchemaV2 *&column_schema,
    const bool get_hidden /* = false */,
    bool is_link /* = false */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "not init", K(ret), KP(schema_checker_), KP(session_info_));
  } else {
    const bool is_rowid_col =
      lib::is_oracle_mode()
      && ObCharset::case_insensitive_equal(column_name, OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME);
    const bool hidden = is_rowid_col || get_hidden || session_info_->is_inner();

    // generated column added by function-based index is hidden in OB but can be selected in oracle
    if (OB_FAIL(schema_checker_->get_column_schema(
        session_info_->get_effective_tenant_id(), table_id, column_name, column_schema, true, is_link))) {
      LOG_WARN("fail to get column schema", K(table_id), K(column_name), K(ret));
    } else if (!hidden && column_schema->is_hidden() && !column_schema->is_generated_column() && !column_schema->is_udt_hidden_column()) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_INFO("do not get hidden column", K(table_id), K(column_name), K(ret));
    }
  }
  return ret;
}

int ObStmtResolver::get_column_schema(const uint64_t table_id,
    const uint64_t column_id,
    const share::schema::ObColumnSchemaV2 *&column_schema,
    const bool get_hidden /* = false */,
    bool is_link /* = false */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "not init", K(ret), KP(schema_checker_), KP(session_info_));
  } else {
    const bool hidden = get_hidden || session_info_->is_inner();
    if (OB_FAIL(schema_checker_->get_column_schema(session_info_->get_effective_tenant_id(), table_id, column_id, column_schema, hidden, is_link))) {
      SQL_RESV_LOG(WARN, "get_column_schema failed", K(ret), K(session_info_->get_effective_tenant_id()), K(table_id), K(column_id), K(hidden), K(is_link));
    }
  }
  return ret;
}

int ObStmtResolver::check_table_name_equal(const ObString &name1, const ObString &name2, bool &equal)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  equal = false;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session info", K(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", K(ret));
  } else if (OB_NAME_CASE_INVALID == case_mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected name case mode", K(ret));
  } else {
    equal = ObCharset::case_mode_equal(case_mode, name1, name2);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
