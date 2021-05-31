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

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {

uint64_t ObStmtResolver::generate_table_id()
{
  if (NULL != params_.query_ctx_) {
    return params_.query_ctx_->available_tb_id_--;
  } else {
    LOG_WARN("query ctx pointer is null");
    return OB_INVALID_ID;
  }
}

int ObStmtResolver::resolve_table_relation_node(
    const ParseNode* node, ObString& table_name, ObString& db_name, bool is_org /*false*/, bool is_oracle_sys_view)
{
  int ret = OB_SUCCESS;
  bool is_db_explicit = false;
  UNUSED(is_db_explicit);
  if (OB_FAIL(resolve_table_relation_node_v2(node, table_name, db_name, is_db_explicit, is_org, is_oracle_sys_view))) {
    LOG_WARN("failed to resolve table name", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

// description: Resolve the association table
//
// @param [in] node         Nodes related to the association table
// @param [out] table_name  The name of the association table to be filled in arg
// @param [out] db_name     The name of the association database to be filled in arg

// @return oceanbase error code defined in lib/ob_errno.def
int ObStmtResolver::resolve_table_relation_node_v2(const ParseNode* node, ObString& table_name, ObString& db_name,
    bool& is_db_explicit, bool is_org /*false*/, bool is_oracle_sys_view)
{
  int ret = OB_SUCCESS;
  is_db_explicit = false;
  ParseNode* db_node = node->children_[0];
  ParseNode* relation_node = node->children_[1];
  int32_t table_len = static_cast<int32_t>(relation_node->str_len_);
  table_name.assign_ptr(const_cast<char*>(relation_node->str_value_), table_len);
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
    SERVER_LOG(WARN, "fail to get name case mode", K(mode), K(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else {
    bool perserve_lettercase = share::is_oracle_mode() ? true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
    int tmp_ret = ObSQLUtils::check_and_convert_table_name(cs_type, perserve_lettercase, table_name);
    if (OB_SUCCESS == tmp_ret || OB_ERR_TOO_LONG_IDENT == tmp_ret) {
      if (NULL == db_node) {
        if (is_oracle_sys_view) {
          // ObString tmp(OB_ORA_SYS_SCHEMA_NAME); // right code
          ObString tmp("SYS");
          if (OB_FAIL(ob_write_string(*allocator_, tmp, db_name))) {
            LOG_WARN("fail to write db name", K(ret));
          }
        } else if (is_org) {
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
          CK(OB_NOT_NULL(schema_checker_->get_schema_guard()));
          OZ(ObSQLUtils::cvt_db_name_to_org(*schema_checker_->get_schema_guard(), session_info_, db_name));
        }
      }
      if (OB_SUCCESS == ret && OB_ERR_TOO_LONG_IDENT == tmp_ret) {
        stmt::StmtType stmt_type = (NULL == get_basic_stmt()) ? stmt::T_NONE : get_basic_stmt()->get_stmt_type();
        bool is_index_table = false;
        uint64_t tenant_id = session_info_->get_effective_tenant_id();
        if (OB_FAIL(schema_checker_->check_table_exists(tenant_id, db_name, table_name, true, is_index_table))) {
          LOG_WARN("fail to check and convert table name", K(tenant_id), K(db_name), K(table_name), K(ret));
        } else if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(
                       cs_type, perserve_lettercase, table_name, stmt_type, is_index_table))) {
          LOG_WARN("fail to check and convert table name", K(table_name), K(stmt_type), K(is_index_table), K(ret));
        }
      } else if (OB_ERR_TOO_LONG_IDENT == tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("fail to check and convert table name", K(table_name), K(ret));
      } else {
      }  // do  nothing
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to check and convert table name", K(table_name), K(ret));
    }
  }
  return ret;
}

int ObStmtResolver::resolve_ref_factor(
    const ParseNode* node, ObSQLSessionInfo* session_info, ObString& table_name, ObString& db_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(node), K(session_info), K(ret));
  } else {
    ParseNode* db_node = node->children_[0];
    ParseNode* relation_node = node->children_[1];
    int32_t table_len = static_cast<int32_t>(relation_node->str_len_);
    table_name.assign_ptr(const_cast<char*>(relation_node->str_value_), table_len);
    ObNameCaseMode mode = OB_NAME_CASE_INVALID;
    ObCollationType cs_type = CS_TYPE_INVALID;
    if (OB_FAIL(session_info->get_name_case_mode(mode))) {
      SERVER_LOG(WARN, "fail to get name case mode", K(mode), K(ret));
    } else if (OB_FAIL(session_info->get_collation_connection(cs_type))) {
      LOG_WARN("fail to get collation_connection", K(ret));
    } else {
      bool perserve_lettercase = share::is_oracle_mode() ? true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
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

int ObStmtResolver::resolve_database_factor(
    const ParseNode* node, uint64_t tenant_id, uint64_t& database_id, ObString& db_name)
{
  int ret = OB_SUCCESS;
  database_id = OB_INVALID_ID;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node is NULL", K(ret));
  } else if (OB_UNLIKELY(T_IDENT != node->type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node type is not T_IDENT", K(ret), K(node->type_));
  } else if (FALSE_IT(db_name.assign_ptr(const_cast<char*>(node->str_value_), static_cast<int32_t>(node->str_len_)))) {
    // won't be here
  } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, db_name, database_id))) {
    LOG_USER_ERROR(OB_ERR_BAD_DATABASE, db_name.length(), db_name.ptr());
  }
  return ret;
}

int ObStmtResolver::normalize_table_or_database_names(ObString& name)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode;
  if (name.empty() || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name is empty", K(name), K(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", K(ret));
  } else if (share::is_oracle_mode()) {
    /* ^-^ */
  } else if (OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
    ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, name);
  }

  return ret;
}

int ObSynonymChecker::add_synonym_id(uint64_t synonym_id)
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
  }
  return ret;
}

int ObStmtResolver::get_column_schema(const uint64_t table_id, const ObString& column_name,
    const share::schema::ObColumnSchemaV2*& column_schema, const bool get_hidden /* = false */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "not init", K(ret), KP(schema_checker_), KP(session_info_));
  } else {
    const bool is_rowid_col =
        share::is_oracle_mode() && ObCharset::case_insensitive_equal(column_name, OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME);
    const bool hidden = is_rowid_col || get_hidden || session_info_->is_inner();

    ret = schema_checker_->get_column_schema(table_id, column_name, column_schema, hidden);
  }
  return ret;
}

int ObStmtResolver::get_column_schema(const uint64_t table_id, const uint64_t column_id,
    const share::schema::ObColumnSchemaV2*& column_schema, const bool get_hidden /* = false */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "not init", K(ret), KP(schema_checker_), KP(session_info_));
  } else {
    const bool is_rowid_col = share::is_oracle_mode() && column_id == OB_HIDDEN_LOGICAL_ROWID_COLUMN_ID;
    const bool hidden = get_hidden || session_info_->is_inner();
    ret = schema_checker_->get_column_schema(table_id, column_id, column_schema, hidden);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
