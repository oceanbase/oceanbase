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
#include "sql/resolver/ddl/ob_create_database_resolver.h"

#include "sql/ob_sql_utils.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_database_resolver.h"
#include "sql/resolver/ddl/ob_create_database_stmt.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObCreateDatabaseResolver::ObCreateDatabaseResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObCreateDatabaseResolver::~ObCreateDatabaseResolver()
{
}

int ObCreateDatabaseResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_CREATE_DATABASE)
      || OB_UNLIKELY(node->num_child_ != DATABASE_NODE_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid node children", K(node), K(node->children_));
  } else {
    ObCreateDatabaseStmt *create_database_stmt = NULL;
    if (OB_ISNULL(create_database_stmt = create_stmt<ObCreateDatabaseStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create create_database_stmt", K(ret));
    } else {
      stmt_ = create_database_stmt;
      create_database_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
    }
    //resolve if not exists
    if (OB_SUCC(ret)) {
      if (node->children_[IF_NOT_EXIST] != NULL) {
        if (node->children_[IF_NOT_EXIST]->type_ != T_IF_NOT_EXISTS) {
          LOG_WARN("invalid parse tree", K(ret));
        } else {
          create_database_stmt->set_if_not_exists(true);
        }
      }
    }
    //resolve database name
    if (OB_SUCC(ret)) {
      ObString database_name;
      //node->children_指针上面已经做了判断，这里不再做判断
      ParseNode *dbname_node = node->children_[DBNAME];
      if (OB_ISNULL(dbname_node) || dbname_node->type_ != T_IDENT) {
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
          } else if (OB_FAIL(create_database_stmt->set_database_name(database_name))) {
            LOG_WARN("set database name failed", K(ret));
          }
        }
      }
    }
    //resolve database options
    if (OB_SUCC(ret)) {
      //node->children_指针上面已经做了判断，这里不再做判断
      ParseNode *dboption_node = node->children_[DATABASE_OPTION];
      if (NULL != dboption_node) {
        if (T_DATABASE_OPTION_LIST != dboption_node->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid parse tree", K(ret));
        } else {
          ObDatabaseResolver<ObCreateDatabaseStmt> resolver;
          if (OB_FAIL(resolver.resolve_database_options(create_database_stmt, dboption_node, session_info_))) {
            LOG_WARN("resolve database option failed", K(ret));
          }
        }
      } else {
        //empty
      }
    }
    if (OB_SUCC(ret)) {
      if (CHARSET_INVALID == create_database_stmt->get_charset_type() &&  CS_TYPE_INVALID == create_database_stmt->get_collation_type()) {
         // For CREATE DATABASE statements, the server character set and collation are used as default
         // values for table definitions if the database character set and collation are not specified.
         // To override this, provide explicit CHARACTER SET and COLLATE table options.
         // http://dev.mysql.com/doc/refman/5.7/en/charset-database.html
         int64_t coll_cs_srv_int64 = -1;
         int64_t coll_srv_int64 = -1;
         if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_CHARACTER_SET_SERVER, coll_cs_srv_int64))) {
           SQL_RESV_LOG(WARN, "fail to get sys variable character_set_server", K(ret));
         } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_COLLATION_SERVER, coll_srv_int64))) {
           SQL_RESV_LOG(WARN, "fail to get sys variable collation_server", K(ret));
         } else if (!ObCharset::is_valid_collation(coll_cs_srv_int64) || !ObCharset::is_valid_collation(coll_srv_int64)) {
           ret = OB_ERR_UNEXPECTED;
           SQL_RESV_LOG(WARN, "invalid collation type", K(ret), K(coll_cs_srv_int64), K(coll_srv_int64));
         } else {
           ObCharsetType cs = ObCharset::charset_type_by_coll(static_cast<ObCollationType>(coll_cs_srv_int64));
           ObCollationType coll_type = static_cast<ObCollationType>(coll_srv_int64);
           create_database_stmt->set_charset_type(cs);
           create_database_stmt->set_collation_type(coll_type);
         }
      }
    }
  }
  LOG_INFO("resolve create database finish", K(ret));
  return ret;
}

} //end namespace sql
} //end namespace oceanbase
