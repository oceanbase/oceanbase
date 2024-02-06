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
#include "sql/resolver/ddl/ob_drop_database_resolver.h"

#include "sql/resolver/ddl/ob_drop_database_stmt.h"
#include "sql/resolver/ddl/ob_database_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"

/**
 *  DROP DATABASE database_name
 */
namespace oceanbase
{
using namespace common;
namespace sql
{

ObDropDatabaseResolver::ObDropDatabaseResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObDropDatabaseResolver::~ObDropDatabaseResolver()
{
}

int ObDropDatabaseResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  if (OB_ISNULL(node)
    || OB_UNLIKELY(node->type_ != T_DROP_DATABASE)
    || OB_UNLIKELY(node->num_child_ != DB_NODE_COUNT)
    || OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret), K(node));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else {
    ObDropDatabaseStmt *drop_database_stmt = NULL;
    if (OB_ISNULL(drop_database_stmt = create_stmt<ObDropDatabaseStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create drop_database_stmt", K(ret));
    } else {
      stmt_ = drop_database_stmt;
      drop_database_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    }
    //resolve if exist
    if (OB_SUCC(ret)) {
      if (node->children_[IF_EXIST] != NULL) {
        if (node->children_[IF_EXIST]->type_ != T_IF_EXISTS) {
          LOG_WARN("invalid parse tree", K(ret));
        } else {
          drop_database_stmt->set_if_exist(true);;
        }
      }
    }
    /* database name */
    if (OB_SUCC(ret)) {
      //resolve database name
      ObString database_name;
      ParseNode *dbname_node = node->children_[DBNAME];
      if (OB_ISNULL(dbname_node) || OB_UNLIKELY(T_IDENT != dbname_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse tree", K(ret));
      } else {
        database_name.assign_ptr(dbname_node->str_value_,
                                 static_cast<int32_t>(dbname_node->str_len_));
        ObNameCaseMode mode = OB_NAME_CASE_INVALID;
        if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
            LOG_WARN("fail to get name case mode", K(mode), K(ret));
        } else {
          bool perserve_lettercase = lib::is_oracle_mode() ?
              true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
          ObCollationType cs_type = CS_TYPE_INVALID;
          if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
            LOG_WARN("fail to get collation_connection", K(ret));
          } else if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(
                      cs_type, perserve_lettercase, database_name))) {
            LOG_WARN("fail to check and convert database name", K(database_name), K(ret));
          } else {
            CK (OB_NOT_NULL(schema_checker_));
            CK (OB_NOT_NULL(schema_checker_->get_schema_guard()));
            OZ (ObSQLUtils::cvt_db_name_to_org(*schema_checker_->get_schema_guard(),
                                               session_info_,
                                               database_name));
            OX (drop_database_stmt->set_database_name(database_name));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObString server_charset;
      int64_t coll_cs_server_int64 = -1;
      int64_t coll_server_int64 = -1;
      if (OB_FAIL(session_info_->get_sys_variable(
                  share::SYS_VAR_CHARACTER_SET_SERVER, coll_cs_server_int64))) {
        LOG_WARN("failed to get character_set_server", K(ret));
      } else if (OB_FAIL(session_info_->get_sys_variable(
                  share::SYS_VAR_COLLATION_SERVER, coll_server_int64))) {
        LOG_WARN("failed to get server collation info", K(ret));
      } else if (false == ObCharset::is_valid_collation(coll_cs_server_int64)
                 || false == ObCharset::is_valid_collation(coll_server_int64)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid collation type", K(ret), K(coll_cs_server_int64),
                     K(coll_server_int64));
      } else if (OB_FAIL(ObCharset::charset_name_by_coll(
                  static_cast<ObCollationType>(coll_cs_server_int64),
                  server_charset))) {
        LOG_WARN("fail to get charset name by collation type", K(ret),
                     K(coll_cs_server_int64));
      } else {
        drop_database_stmt->set_server_charset(server_charset);
        drop_database_stmt->set_server_collation(
            ObString(ObCharset::collation_name(static_cast<ObCollationType>(coll_server_int64))));
      }
    }
    if (OB_SUCC(ret)) {
      ObObj is_recyclebin_open;
      if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_RECYCLEBIN, is_recyclebin_open))){
        LOG_WARN("get sys variable failed", K(ret));
      } else {
        drop_database_stmt->set_to_recyclebin(is_recyclebin_open.get_bool());
      }
    }
  }
  return ret;
}

} //end namespace sql
} //end namespace oceanbase
