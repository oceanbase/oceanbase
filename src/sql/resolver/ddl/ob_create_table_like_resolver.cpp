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

#include "sql/resolver/ddl/ob_create_table_like_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObCreateTableLikeResolver::ObCreateTableLikeResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObCreateTableLikeResolver::~ObCreateTableLikeResolver()
{
}

int ObCreateTableLikeResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  bool is_temporary_table = false;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "session info should not be null", K(ret));
  } else if (T_CREATE_TABLE_LIKE != parse_tree.type_ || OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else {
    ObCreateTableLikeStmt *create_table_like_stmt = NULL;
    if (NULL == (create_table_like_stmt = create_stmt<ObCreateTableLikeStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "failed to create create_table_like stmt", K(ret));
    } else {
      stmt_ = create_table_like_stmt;
    }
    //resolve temporary option
    if (OB_SUCC(ret)) {
      if (NULL != parse_tree.children_[0]) {
        if (T_TEMPORARY == parse_tree.children_[0]->type_) {
          is_temporary_table = true;
        } else if (T_EXTERNAL == parse_tree.children_[0]->type_) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "create external table like");
        } else {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid argument.",
                       K(ret), K(parse_tree.children_[0]->type_));
        }
      }
      if (OB_SUCC(ret) && is_temporary_table) {
        char create_host_str[OB_MAX_HOST_NAME_LENGTH];
        MYADDR.ip_port_to_string(create_host_str, OB_MAX_HOST_NAME_LENGTH);
        if (OB_ISNULL(allocator_)) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "not init", K(ret));
        } else if (OB_FAIL(create_table_like_stmt->set_create_host(*allocator_, ObString(create_host_str)))) {
          SQL_RESV_LOG(WARN, "set create host failed", K(ret));
        } else {
          CHECK_COMPATIBILITY_MODE(session_info_);
          if (lib::is_oracle_mode()) {
            ret = OB_NOT_SUPPORTED;
            SQL_RESV_LOG(WARN, "create temporary table like not supported!", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "create oracle temporary table like");
          } else {
            create_table_like_stmt->set_table_type(share::schema::TMP_TABLE);
          }
        }
      } else {
        create_table_like_stmt->set_table_type(share::schema::USER_TABLE);
      }
    }
    //if not exist
    if (OB_SUCC(ret)) {
      if (NULL != parse_tree.children_[1]) {
        create_table_like_stmt->set_if_not_exist(true);
      } else {
        create_table_like_stmt->set_if_not_exist(false);
      }
    }

    if (OB_SUCC(ret)) {
      ParseNode *new_relation_node = parse_tree.children_[2];
      ParseNode *origin_relation_node = parse_tree.children_[3];
      if (NULL != new_relation_node && NULL != origin_relation_node) {
        //resolve table
        ObString origin_table_name;
        ObString origin_database_name;
        ObString new_table_name;
        ObString new_database_name;
        if (OB_FAIL(resolve_table_relation_node(new_relation_node,
                                                new_table_name,
                                                new_database_name))) {
          SQL_RESV_LOG(WARN, "failed to resolve table name.",
                       K(new_table_name), K(new_database_name), K(ret));
        } else if (OB_FAIL(resolve_table_relation_node(origin_relation_node,
                                                       origin_table_name,
                                                       origin_database_name))) {
            SQL_RESV_LOG(WARN, "failed to resolve origin name.",
                         K(origin_table_name), K(origin_database_name), K(ret));
        } else if (ObString(OB_RECYCLEBIN_SCHEMA_NAME) == new_database_name
                   || ObString(OB_PUBLIC_SCHEMA_NAME) == new_database_name) {
          ret = OB_OP_NOT_ALLOW;
          SQL_RESV_LOG(WARN, "create table in recyclebin database is not allowed", K(ret),
                       K(new_table_name), K(new_database_name),
                       K(origin_table_name), K(origin_database_name));
        } else {
          bool db_equal = false;
          bool table_equal = false;
          if (OB_FAIL(ObResolverUtils::name_case_cmp(session_info_,
                                                     origin_database_name,
                                                     new_database_name,
                                                     OB_TABLE_NAME_CLASS,
                                                     db_equal))) {
            SQL_RESV_LOG(WARN, "failed to compare db names", K(origin_database_name),
                         K(new_database_name), K(ret));
          } else if (OB_FAIL(ObResolverUtils::name_case_cmp(session_info_,
                                                            origin_table_name,
                                                            new_table_name,
                                                            OB_TABLE_NAME_CLASS,
                                                            table_equal))) {
            SQL_RESV_LOG(WARN, "failed to compare table names", K(origin_table_name),
                         K(new_table_name), K(ret));
          } else if (db_equal && table_equal) {
            ret = OB_ERR_NONUNIQ_TABLE;
            LOG_USER_ERROR(OB_ERR_NONUNIQ_TABLE, origin_table_name.length(), origin_table_name.ptr());
          } else {
            create_table_like_stmt->set_new_table_name(new_table_name);
            create_table_like_stmt->set_new_db_name(new_database_name);
            create_table_like_stmt->set_origin_table_name(origin_table_name);
            create_table_like_stmt->set_origin_db_name(origin_database_name);
            create_table_like_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
          }
        }
      } else {
        ret = OB_ERR_PARSE_SQL;
        SQL_RESV_LOG(WARN, "relation node should not be null!", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      create_table_like_stmt->set_define_user_id(session_info_->get_priv_user_id());
    }
  }
  return ret;
}

} //namespace common
} //namespace oceanbase
