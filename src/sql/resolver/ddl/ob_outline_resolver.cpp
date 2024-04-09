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
#include "sql/resolver/ddl/ob_outline_resolver.h"

#include "sql/ob_sql_utils.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/dml/ob_insert_resolver.h"
#include "sql/resolver/dml/ob_update_resolver.h"
#include "sql/resolver/dml/ob_delete_resolver.h"
#include "sql/resolver/dml/ob_select_resolver.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObOutlineResolver::resolve_outline_name(const ParseNode *node, ObString &db_name, ObString &outline_name)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;

  if (OB_ISNULL(node)
      || OB_UNLIKELY(T_RELATION_FACTOR != node->type_)
      || OB_UNLIKELY(RELATION_FACTOR_CHILD_COUNT > node->num_child_)
      || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children is NULL", K(node), K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
    LOG_WARN("fail to get name case mode", K(mode), K(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL", K(ret));
  } else {
    const ParseNode *db_name_node = node->children_[0];
    const ParseNode *outline_name_node = node->children_[1];

    //get outline name, TODO(tingshuai.yts):check outline_name length
    outline_name.assign_ptr(outline_name_node->str_value_, static_cast<ObString::obstr_size_t>(outline_name_node->str_len_));

    //get database name
    bool perserve_lettercase = lib::is_oracle_mode() ?
        true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
    if (NULL == db_name_node) {
      if (session_info_->get_database_name().empty()) {
        db_name = OB_MOCK_DEFAULT_DATABASE_NAME;
      } else {
        db_name = session_info_->get_database_name();
      }
    } else {
      db_name.assign_ptr(db_name_node->str_value_, static_cast<int32_t>(db_name_node->str_len_));
      if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(cs_type, perserve_lettercase, db_name))) {
        LOG_WARN("fail to check and convert database name", K(db_name), K(ret));
      } else {
        CK (OB_NOT_NULL(schema_checker_));
        CK (OB_NOT_NULL(schema_checker_->get_schema_guard()));
        OZ (ObSQLUtils::cvt_db_name_to_org(*schema_checker_->get_schema_guard(),
                                           session_info_,
                                           db_name,
                                           allocator_));
      }
    }
  }
  return ret;
}

int ObOutlineResolver::resolve_outline_stmt(const ParseNode *node, ObStmt *&out_stmt, ObString &out_sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret), K(node), K(stmt_));
  } else {
    ObStmt *outline_stmt = NULL;
    ObResolver resolver(params_);
    if (node->type_ != T_SELECT
        && node->type_ != T_INSERT
        && node->type_ != T_REPLACE
        && node->type_ != T_DELETE
        && node->type_ != T_UPDATE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected node type", K(node->type_), K(ret));
    } else if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *node, outline_stmt))) {
      LOG_WARN("fail to resolve", K(ret));
    } else if (OB_ISNULL(outline_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid outline_stmt is NULL", K(ret));
    } else {
      out_stmt = outline_stmt;
      out_sql.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
      if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
              *allocator_, session_info_->get_dtc_params(), out_sql))) {
        LOG_WARN("fail to convert sql text", K(ret));
      }
    }
  }
  return ret;
}

int ObOutlineResolver::resolve_outline_target(const ParseNode *target_node, ObString &outline_target)
{
  int ret = OB_SUCCESS;
  if (NULL == target_node) {
    //has no outline_target, keep empty
    outline_target.reset();
  } else {
    outline_target.assign_ptr(target_node->str_value_, static_cast<int32_t>(target_node->str_len_));
    if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
            *allocator_, session_info_->get_dtc_params(), outline_target))) {
      LOG_WARN("fail to convert sql text", K(ret));
    }
  }
  return ret;
}

}//sql
}//oceanbase
