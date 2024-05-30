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

#include "sql/resolver/dcl/ob_alter_user_proxy_resolver.h"

#include "share/schema/ob_schema_struct.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/dcl/ob_grant_resolver.h"
#include "share/ob_rpc_struct.h"
#include "lib/encrypt/ob_encrypted_helper.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using oceanbase::share::schema::ObUserInfo;

ObAlterUserProxyResolver::ObAlterUserProxyResolver(ObResolverParams &params)
    : ObDCLResolver(params)
{
}

ObAlterUserProxyResolver::~ObAlterUserProxyResolver()
{
}

int ObAlterUserProxyResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *client_user = NULL;
  ParseNode *proxy_user = NULL;
  ParseNode *with_role = NULL;
  ParseNode *type_node = NULL;
  ObAlterUserProxyStmt *alter_user_proxy_stmt = NULL;
  uint64_t tenant_data_version = 0;
  if (OB_ISNULL(params_.session_info_) || OB_ISNULL(params_.schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(parse_tree.type_ != T_ALTER_USER_PROXY)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt type", K(ret), K(parse_tree.type_));
  } else if (OB_UNLIKELY(parse_tree.num_child_ != 4)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt children", K(ret), K(parse_tree.num_child_));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(params_.session_info_->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (!ObSQLUtils::is_data_version_ge_423_or_432(tenant_data_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter user grant connect through is not supported when data version is below 4.2.3 or 4.3.2");
  } else if (OB_ISNULL(client_user = parse_tree.children_[0])
            || OB_ISNULL(proxy_user = parse_tree.children_[1])
            || OB_ISNULL(with_role = parse_tree.children_[2])
            || OB_ISNULL(type_node = parse_tree.children_[3])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error", K(ret));
  } else if (OB_UNLIKELY(with_role->type_ != T_DEFAULT_ROLE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type error", K(ret));
  } else if (OB_UNLIKELY(client_user->type_ != T_USERS
                      || proxy_user->type_ != T_USERS
                      || client_user->num_child_ <= 0
                      || proxy_user->num_child_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (OB_ISNULL(alter_user_proxy_stmt = create_stmt<ObAlterUserProxyStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to create stmt", K(ret));
  } else {
    uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();
    bool is_grant = type_node->value_ == 1 ? false : true;
    alter_user_proxy_stmt->set_tenant_id(tenant_id);
    alter_user_proxy_stmt->set_is_grant(is_grant);
    ObArray<const ObUserInfo *> client_user_infos;
    for (int64_t i = 0; OB_SUCC(ret) && i < client_user->num_child_; i++) {
      ObString user_name;
      ObString user_host;
      ParseNode *user = client_user->children_[i];
      if (OB_ISNULL(user) || OB_UNLIKELY(user->num_child_ != 2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      } else if (NULL == user->children_[1]) {
        user_host = ObString(OB_DEFAULT_HOST_NAME);
      } else {
        user_host = ObString(user->children_[1]->str_len_, user->children_[1]->str_value_);
      }
      if (OB_SUCC(ret)) {
        user_name = ObString(user->children_[0]->str_len_, user->children_[0]->str_value_);
      }
      const ObUserInfo *client_user_info = NULL;
      if (OB_SUCC(ret)) {
        if (0 == user_name.case_compare(OB_ORA_SYS_USER_NAME)) {
          if (params_.session_info_->get_user_id() != OB_ORA_SYS_USER_ID) {
            ret = OB_ERR_NO_SYS_PRIVILEGE;
            LOG_WARN("no sys privilege", K(ret));
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("proxy user act as client sys is not supported", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "proxy user act as client 'SYS'");
          }
        } else if (OB_FAIL(params_.schema_checker_->get_user_info(tenant_id, user_name, user_host, client_user_info))) {
          LOG_WARN("get user info failed", K(ret), K(user_name), K(user_host));
        } else if (OB_UNLIKELY(client_user_info->is_role())) {
          ret = ER_NO_SUCH_USER;
          LOG_WARN("user not existed", K(ret));
        } else if (OB_FAIL(alter_user_proxy_stmt->add_client_user_id(client_user_info->get_user_id()))) {
          LOG_WARN("add client user id failed", K(ret));
        } else if (OB_FAIL(client_user_infos.push_back(client_user_info))) {
          LOG_WARN("push back failed", K(ret));
        } else if (OB_FAIL(alter_user_proxy_stmt->get_ddl_arg().based_schema_object_infos_.
                    push_back(ObBasedSchemaObjectInfo(
                              client_user_info->get_user_id(),
                              USER_SCHEMA,
                              client_user_info->get_schema_version())))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < proxy_user->num_child_; i++) {
      ObString user_name;
      ObString user_host;
      ParseNode *user = proxy_user->children_[i];
      if (OB_ISNULL(user) || OB_UNLIKELY(user->num_child_ != 2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      } else if (NULL == user->children_[1]) {
        user_host = ObString(OB_DEFAULT_HOST_NAME);
      } else {
        user_host = ObString(user->children_[1]->str_len_, user->children_[1]->str_value_);
      }
      if (OB_SUCC(ret)) {
        user_name = ObString(user->children_[0]->str_len_, user->children_[0]->str_value_);
      }
      const ObUserInfo *proxy_user_info = NULL;
      if (OB_SUCC(ret)) {
        if (0 == user_name.case_compare(OB_ORA_SYS_USER_NAME)) {
          if (params_.session_info_->get_user_id() != OB_ORA_SYS_USER_ID) {
            ret = OB_ERR_NO_SYS_PRIVILEGE;
            LOG_WARN("no sys privilege", K(ret));
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("proxy user act as client sys is not supported", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "proxy user act as client 'SYS'");
          }
        } else if (OB_FAIL(params_.schema_checker_->get_user_info(tenant_id, user_name, user_host, proxy_user_info))) {
          LOG_WARN("get user info failed", K(ret), K(user_name), K(user_host));
        } else if (OB_UNLIKELY(proxy_user_info->is_role())) {
          ret = ER_NO_SUCH_USER;
          LOG_WARN("user not existed", K(ret));
        } else if (OB_FAIL(alter_user_proxy_stmt->add_proxy_user_id(proxy_user_info->get_user_id()))) {
          LOG_WARN("add client user id failed", K(ret));
        } else if (OB_FAIL(alter_user_proxy_stmt->get_ddl_arg().based_schema_object_infos_.
                    push_back(ObBasedSchemaObjectInfo(
                              proxy_user_info->get_user_id(),
                              USER_SCHEMA,
                              proxy_user_info->get_schema_version())))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < alter_user_proxy_stmt->get_proxy_user_id().count(); i++) {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < alter_user_proxy_stmt->get_proxy_user_id().count(); j++) {
        if (alter_user_proxy_stmt->get_proxy_user_id().at(i) == alter_user_proxy_stmt->get_proxy_user_id().at(j)) {
          ret = OB_ERR_DUPLICATE_USERNAME_IN_LIST;
          LOG_WARN("duplicate name in list", K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < alter_user_proxy_stmt->get_client_user_id().count(); i++) {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < alter_user_proxy_stmt->get_client_user_id().count(); j++) {
        if (alter_user_proxy_stmt->get_client_user_id().at(i) == alter_user_proxy_stmt->get_client_user_id().at(j)) {
          ret = OB_ERR_DUPLICATE_USERNAME_IN_LIST;
          LOG_WARN("duplicate name in list", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (with_role->value_ == 1) {
        alter_user_proxy_stmt->set_flags(PROXY_USER_MAY_ACTIVATE_ROLE);
      } else if (with_role->value_ == 2) {
        alter_user_proxy_stmt->set_flags(PROXY_USER_ROLE_CAN_NOT_BE_ACTIVATED);
      } else if (with_role->value_ == 3) {
        alter_user_proxy_stmt->set_flags(PROXY_USER_NO_ROLES_BE_ACTIVATED);
      } else if (with_role->value_ == 4) {
        alter_user_proxy_stmt->set_flags(PROXY_USER_ACTIVATE_ALL_ROLES);
      }

      if (with_role->value_ == 1 || with_role->value_ == 2) {
        ParseNode *role_list = NULL;
        if (OB_UNLIKELY(with_role->num_child_ != 1) || OB_ISNULL(role_list = with_role->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret));
        }
        for (int i = 0; OB_SUCC(ret) && i < role_list->num_child_; ++i) {
          ParseNode *role = role_list->children_[i];
          if (OB_ISNULL(role)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("role node is null", K(ret));
          } else {
            ObString role_name;
            role_name.assign_ptr(const_cast<char *>(role->str_value_),
                                static_cast<int32_t>(role->str_len_));
            // check roles exists
            const ObUserInfo *role_info = NULL;
            if (OB_FAIL(params_.schema_checker_->get_user_info(tenant_id,
                                        role_name,
                                        // role has fixed host_name '%'
                                        ObString::make_string(OB_DEFAULT_HOST_NAME),
                                        role_info))) {
              LOG_WARN("get user role info failed", K(ret));
            } else if (OB_ISNULL(role_info) || OB_UNLIKELY(!role_info->is_role())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected error", K(ret));
            } else if (OB_FAIL(alter_user_proxy_stmt->add_role_id(role_info->get_user_id()))) {
              LOG_WARN("push back failed", K(ret));
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < client_user_infos.count(); i++) {
                bool found = false;
                const ObUserInfo *user_info = client_user_infos.at(i);
                if (OB_ISNULL(user_info)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected error", K(ret));
                }
                for (int64_t j = 0; OB_SUCC(ret) && !found && j < user_info->get_role_id_array().count(); j++) {
                  if (role_info->get_user_id() == user_info->get_role_id_array().at(j)) {
                    found = true;
                  }
                }
                if (OB_SUCC(ret) && !found) {
                  ret = OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST;
                  LOG_WARN("role not granted", K(ret));
                  LOG_USER_ERROR(OB_ERR_ROLE_NOT_GRANTED_OR_DOES_NOT_EXIST,
                          role_info->get_user_name_str().length(), role_info->get_user_name_str().ptr());
                }
              }
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ (schema_checker_->check_ora_ddl_priv(
          params_.session_info_->get_effective_tenant_id(),
          params_.session_info_->get_priv_user_id(),
          ObString(""),
          stmt::T_ALTER_USER_PROXY,
          params_.session_info_->get_enable_role_array()));
  }
  return ret;
}
