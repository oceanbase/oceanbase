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
#include "sql/resolver/dcl/ob_lock_user_resolver.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using oceanbase::share::schema::ObUserInfo;

ObLockUserResolver::ObLockUserResolver(ObResolverParams &params)
    : ObDCLResolver(params)
{
}

ObLockUserResolver::~ObLockUserResolver()
{
}

int ObLockUserResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "node is null", K(ret));
  } else if (T_LOCK_USER != node->type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "wrong node type", K(ret), K(node->type_));
  } else if (2 != node->num_child_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "not enough children", K(ret));
  } else if (OB_ISNULL(node->children_[0]) || OB_ISNULL(node->children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "child is null", K(node->children_[0]), K(node->children_[1]), K(ret));
  } else if (T_USERS != node->children_[0]->type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "1st child type error", "type", node->children_[0]->type_, K(ret));
  } else if (T_BOOL != node->children_[1]->type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "2nd child type error", "type", node->children_[1]->type_, K(ret));
  } else {
    ObLockUserStmt *lock_user_stmt = NULL;
    if (OB_ISNULL(lock_user_stmt = create_stmt<ObLockUserStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "Failed to create ObLockUserStmt", K(ret));
    } else {
      ParseNode ** name_nodes = node->children_[0]->children_;
      if (OB_ISNULL(name_nodes)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(ERROR, "no name nodes", K(ret));
      } else if (OB_ISNULL(params_.session_info_)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(ERROR, "params_.session_info_ is null", K(ret));
      } else {
        uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();
        if (OB_INVALID_ID == tenant_id) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(ERROR, "tenant_id invalid", K(ret));
        } else {
          bool locked = node->children_[1]->value_ == 1 ? true : false;
          lock_user_stmt->set_tenant_id(tenant_id);
          lock_user_stmt->set_locked(locked);
          stmt_ = lock_user_stmt;
          ObString user_name;
          ObString host_name;
          for (int64_t i = 0; OB_SUCC(ret) && i < node->children_[0]->num_child_; ++i) {
            if (OB_ISNULL(name_nodes[i])) {
              ret = OB_INVALID_ARGUMENT;
              SQL_RESV_LOG(ERROR, "name node is null", K(ret));
            } else if (2 != name_nodes[i]->num_child_) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("sql_parser parse user error", K(ret));
            } else if (OB_ISNULL(name_nodes[i]->children_[0])) {
              ret = OB_INVALID_ARGUMENT;
              SQL_RESV_LOG(ERROR, "name invalid", KP(name_nodes[i]->children_[0]), K(ret));
            } else {
              ParseNode *user_hostname_node = name_nodes[i];
              //0: user name; 1: host name
              user_name.assign_ptr(user_hostname_node->children_[0]->str_value_,
                  static_cast<int32_t>(user_hostname_node->children_[0]->str_len_));
              if (NULL == user_hostname_node->children_[1]) {
                host_name.assign_ptr(OB_DEFAULT_HOST_NAME, static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
              } else {
                host_name.assign_ptr(user_hostname_node->children_[1]->str_value_,
                                     static_cast<int32_t>(user_hostname_node->children_[1]->str_len_));
              }
              if (OB_FAIL(check_dcl_on_inner_user(node->type_,
                                                  session_info_->get_priv_user_id(),
                                                  user_name,
                                                  host_name))) {
                LOG_WARN("failed to check dcl on inner-user or unsupport to modify reserved user",
                          K(ret), K(params_.session_info_->get_user_name()), K(user_name));
              } else if (OB_FAIL(lock_user_stmt->add_user(user_name, host_name))) {
                SQL_RESV_LOG(WARN, "Add user error", K(user_name), K(host_name), K(ret));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      OZ (schema_checker_->check_ora_ddl_priv(
            session_info_->get_effective_tenant_id(),
            session_info_->get_priv_user_id(),
            ObString(""),
            stmt::T_LOCK_USER,
            session_info_->get_enable_role_array()),
            session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
    }
  }
  return ret;
}
