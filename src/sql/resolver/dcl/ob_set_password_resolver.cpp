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
#include "sql/resolver/dcl/ob_set_password_resolver.h"

#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObSetPasswordResolver::ObSetPasswordResolver(ObResolverParams &params)
    : ObDCLResolver(params)
{
}

ObSetPasswordResolver::~ObSetPasswordResolver()
{
}

bool ObSetPasswordResolver::is_hex_literal(const ObString &str)
{
  bool bool_ret = true;
  for (int i = 0; bool_ret && i < str.length(); ++i) {
    if (!isxdigit(str[i])) {
      bool_ret = false;
      break;
    }
  }
  return bool_ret;
}

bool ObSetPasswordResolver::is_valid_mysql41_passwd(const ObString &str)
{
  bool bret = true;
  if (str.length() != 41) {
    bret = false;
  } else if (str.ptr()[0] != '*') {
    bret = false;
  } else {
    ObString trimed;
    trimed.assign_ptr(str.ptr() + 1, 40);
    bret = is_hex_literal(trimed);
  }
  return bret;
}


int ObSetPasswordResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode*>(&parse_tree);
  ObSetPasswordStmt *set_pwd_stmt = NULL;
  if (OB_ISNULL(session_info_) || OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Session info  and nodeshould not be NULL", KP(session_info_), KP(node), K(ret));
  } else if (OB_UNLIKELY(T_SET_PASSWORD != node->type_) ||
             OB_UNLIKELY(lib::is_oracle_mode() && 4 != node->num_child_) ||
             OB_UNLIKELY(lib::is_mysql_mode() && 5 != node->num_child_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Set password ParseNode error", K(node->type_), K(node->num_child_), K(ret));
  } else {
    if (OB_ISNULL(set_pwd_stmt = create_stmt<ObSetPasswordStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Failed to create ObSetPasswordStmt", K(ret));
    } else {
      stmt_ = set_pwd_stmt;
      set_pwd_stmt->set_tenant_id(session_info_->get_effective_tenant_id());
      ObString user_name;
      ObString host_name;
      const ObString &session_user_name = session_info_->get_user_name();
      const ObString &session_host_name = session_info_->get_host_name();
      bool is_valid = false;
      if (lib::is_mysql_mode() && NULL != node->children_[4]) {
        /* here code is to mock a auth plugin check. */
        ObString auth_plugin(static_cast<int32_t>(node->children_[4]->str_len_),
                              node->children_[4]->str_value_);
        ObString default_auth_plugin;
        if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN,
                                                    default_auth_plugin))) {
          LOG_WARN("fail to get block encryption variable", K(ret));
        } else if (0 != auth_plugin.compare(default_auth_plugin)) {
          ret = OB_ERR_PLUGIN_IS_NOT_LOADED;
          LOG_USER_ERROR(OB_ERR_PLUGIN_IS_NOT_LOADED, auth_plugin.length(), auth_plugin.ptr());
        } else {/* do nothing */}
      }
      if (OB_SUCC(ret) && NULL != node->children_[0]) {
        ParseNode *user_hostname_node = node->children_[0];
        if (OB_FAIL(check_role_as_user(user_hostname_node, is_valid))) {
          LOG_WARN("failed to check role as user", K(ret));
        } else if (!is_valid) {
          ret = OB_USER_NOT_EXIST;
          LOG_ORACLE_USER_ERROR(OB_USER_NOT_EXIST, int(user_hostname_node->str_len_), user_hostname_node->str_value_);
        } else if (OB_ISNULL(user_hostname_node->children_[0])) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("username should not be NULL", K(ret));
        } else {
          user_name.assign_ptr(user_hostname_node->children_[0]->str_value_,
                        static_cast<int32_t>(user_hostname_node->children_[0]->str_len_));
          if (NULL == user_hostname_node->children_[1]) {
            host_name.assign_ptr(OB_DEFAULT_HOST_NAME, static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
          } else {
            host_name.assign_ptr(user_hostname_node->children_[1]->str_value_,
                                 static_cast<int32_t>(user_hostname_node->children_[1]->str_len_));
          }
          if (0 == user_name.compare(session_user_name)
              && 0 == host_name.compare(session_host_name)) {
            set_pwd_stmt->set_for_current_user(true);
          }
        }
      } else {
        user_name = session_user_name;
        host_name = session_host_name;
        set_pwd_stmt->set_for_current_user(true);
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(check_dcl_on_inner_user(node->type_,
                                            params_.session_info_->get_priv_user_id(),
                                            user_name,
                                            host_name))) {
          LOG_WARN("failed to check dcl on inner-user or unsupport to modify reserved user", K(ret),
                   K(params_.session_info_->get_priv_user_id()), K(user_name));
        }
      }
      if (OB_SUCC(ret)) {
        // replace password to *** in query_string for audit
        ObString masked_sql;
        if (session_info_->is_inner()) {
          // do nothing in inner_sql
        } else if (OB_FAIL(mask_password_for_single_user(allocator_,
            session_info_->get_current_query_string(), node, 1, masked_sql))) {
          LOG_WARN("fail to mask_password_for_single_user", K(ret));
        } else {
          set_pwd_stmt->set_masked_sql(masked_sql);
        }
      }
      if (OB_SUCC(ret)) {
        ObSSLType ssl_type = ObSSLType::SSL_TYPE_NOT_SPECIFIED;
        ObString infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_MAX)] = {};
        if ((NULL == node->children_[1]) && (NULL == node->children_[2])) {
          const ParseNode *child_node = node->children_[3];
          if (OB_ISNULL(child_node)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("alter user ParseNode error", K(ret));
          } else if (T_TLS_OPTIONS == child_node->type_) {
            if (OB_FAIL(resolve_require_node(*child_node, user_name, host_name, ssl_type, infos))) {
              LOG_WARN("resolve require node failed", K(ret));
            }
          } else if (T_USER_RESOURCE_OPTIONS == child_node->type_) {
            if (OB_FAIL(resolve_resource_option_node(*child_node, user_name, host_name, ssl_type, infos))) {
              LOG_WARN("resolve resource option node failed", K(ret));
            }
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("alter user ParseNode error", K(ret), K(child_node->type_));
          }
        } else if (OB_ISNULL(node->children_[1]) || OB_ISNULL(node->children_[2])) {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("The child 1 or child 2 should not be NULL",
              K(ret), "child 1", node->children_[1], "child 2", node->children_[2]);
        } else {
          ObString password(static_cast<int32_t>(node->children_[1]->str_len_),
                            node->children_[1]->str_value_);
          if (!lib::is_oracle_mode() && OB_FAIL(check_password_strength(password))) {
            LOG_WARN("fail to check password strength", K(ret));
          } else if (lib::is_oracle_mode() && OB_FAIL(
                     resolve_oracle_password_strength(user_name, host_name, password))) {
            LOG_WARN("fail to check password strength", K(ret));
          } else if (0 != password.length()) {//set password
            bool need_enc = (1 == node->children_[2]->value_) ? true : false;
            if (!need_enc && (!is_valid_mysql41_passwd(password))) {
              ret = OB_ERR_PASSWORD_FORMAT;
              LOG_WARN("Wrong password hash format");
            } else {
              set_pwd_stmt->set_need_enc(need_enc);
            }
          } else {
            set_pwd_stmt->set_need_enc(false); //clear password
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(set_pwd_stmt->set_user_password(user_name, host_name, password))) {
              LOG_WARN("Failed to set UserPasswordStmt");
            } else if (OB_FAIL(set_pwd_stmt->add_ssl_info(get_ssl_type_string(ssl_type),
                                                          infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_ISSUER)],
                                                          infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_CIPHER)],
                                                          infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_SUBJECT)]))) {
              LOG_WARN("Failed to add_ssl_info", K(ssl_type),
                       "ISSUER", infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_ISSUER)],
                       "CIPHER", infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_CIPHER)],
                       "SUBJECT", infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_SUBJECT)], K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check() 
        && set_pwd_stmt->get_for_current_user() == false) {
      OZ (schema_checker_->check_ora_ddl_priv(
            session_info_->get_effective_tenant_id(),
            session_info_->get_priv_user_id(),
            ObString(""),
            stmt::T_SET_PASSWORD,
            session_info_->get_enable_role_array()),
            session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
  }
  return ret;
}

int ObSetPasswordResolver::resolve_require_node(const ParseNode &require_info,
    const ObString &user_name, const ObString &host_name, ObSSLType &ssl_type, ObString *infos)
{
  int ret = OB_SUCCESS;
  //alter user require ssl_info
  ParseNode *ssl_infos = NULL;
  ObSetPasswordStmt *set_pwd_stmt = static_cast<ObSetPasswordStmt *>(stmt_);
  if (OB_UNLIKELY(T_TLS_OPTIONS != require_info.type_)
      || OB_UNLIKELY(require_info.num_child_ != 1)
      || OB_ISNULL(ssl_infos = require_info.children_[0])
      || OB_ISNULL(set_pwd_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Create user ParseNode error", K(ret), K(ssl_infos), K(set_pwd_stmt));
  } else {
    ssl_type = static_cast<ObSSLType>(static_cast<int32_t>(ObSSLType::SSL_TYPE_NONE) + (ssl_infos->type_ - T_TLS_NONE));

    if (ObSSLType::SSL_TYPE_SPECIFIED == ssl_type) {
      ParseNode *specified_ssl_infos = NULL;

      if (OB_UNLIKELY(ssl_infos->num_child_ <= 0)
          || OB_ISNULL(specified_ssl_infos = ssl_infos->children_[0])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Create user ParseNode error", K(ret), K(ssl_infos->num_child_), KP(specified_ssl_infos));
      } else {
        bool check_repeat[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_MAX)] = {};
        for (int i = 0; i < specified_ssl_infos->num_child_ && OB_SUCC(ret); ++i) {
          ParseNode *ssl_info = specified_ssl_infos->children_[i];
          if (OB_ISNULL(ssl_info)) {
            ret = OB_ERR_PARSE_SQL;
            LOG_WARN("The child of parseNode should not be NULL", K(ret), K(i));
          } else if (OB_UNLIKELY(ssl_info->num_child_ != 1)) {
            ret = OB_ERR_PARSE_SQL;
            LOG_WARN("The num_child_is error", K(ret), K(i), K(ssl_info->num_child_));
          } else if (OB_UNLIKELY(check_repeat[ssl_info->type_ - T_TLS_CIPHER])) {
            ret = OB_ERR_DUP_ARGUMENT;
            LOG_WARN("Option used twice in statement", K(ret), K(ssl_info->type_));
            LOG_USER_ERROR(OB_ERR_DUP_ARGUMENT, get_ssl_spec_type_str(static_cast<ObSSLSpecifiedType>(ssl_info->type_ - T_TLS_CIPHER)));
          } else {
            check_repeat[ssl_info->type_ - T_TLS_CIPHER] = true;
            infos[ssl_info->type_ - T_TLS_CIPHER].assign_ptr(ssl_info->children_[0]->str_value_, ssl_info->children_[0]->str_len_);
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObString password;
    set_pwd_stmt->set_need_enc(false);
    if (OB_FAIL(set_pwd_stmt->set_user_password(user_name, host_name, password))) {
      LOG_WARN("Failed to set UserPasswordStmt");
    } else if (OB_FAIL(set_pwd_stmt->add_ssl_info(get_ssl_type_string(ssl_type),
                                                  infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_CIPHER)],
                                                  infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_ISSUER)],
                                                  infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_SUBJECT)]))) {
      LOG_WARN("Failed to add_ssl_info", K(ssl_type),
                "CIPHER", infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_CIPHER)],
                "ISSUER", infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_ISSUER)],
                "SUBJECT", infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_SUBJECT)], K(ret));
    }
  }
  return ret;
}

int ObSetPasswordResolver::resolve_resource_option_node(const ParseNode &resource_options,
  const ObString &user_name, const ObString &host_name, ObSSLType &ssl_type, ObString *infos)
{
  int ret = OB_SUCCESS;
  ObSetPasswordStmt *set_pwd_stmt = static_cast<ObSetPasswordStmt *>(stmt_);
  if (OB_ISNULL(set_pwd_stmt) || T_USER_RESOURCE_OPTIONS != resource_options.type_
      || OB_ISNULL(resource_options.children_)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource options argument", K(ret), K(set_pwd_stmt),
              K(resource_options.type_), K(resource_options.children_));
  } else {
    for (int64_t i = 0; i < resource_options.num_child_; i++) {
      ParseNode *res_option = resource_options.children_[i];
      if (OB_ISNULL(res_option)) {
        ret = common::OB_INVALID_ARGUMENT;
        LOG_WARN("null res option", K(ret), K(i));
      } else if (T_MAX_CONNECTIONS_PER_HOUR == res_option->type_) {
        uint64_t max_connections_per_hour = static_cast<uint64_t>(res_option->value_);
        max_connections_per_hour = max_connections_per_hour > MAX_CONNECTIONS ? MAX_CONNECTIONS
                                    : max_connections_per_hour;
        set_pwd_stmt->set_max_connections_per_hour(max_connections_per_hour);
      } else if (T_MAX_USER_CONNECTIONS == res_option->type_) {
        uint64_t max_user_connections = static_cast<uint64_t>(res_option->value_);
        max_user_connections = max_user_connections > MAX_CONNECTIONS ? MAX_CONNECTIONS
                                : max_user_connections;
        set_pwd_stmt->set_max_user_connections(max_user_connections);
      }
    }
  }
  if (OB_SUCC(ret)) {
    set_pwd_stmt->set_modify_max_connections(true);
    ObString password;
    set_pwd_stmt->set_need_enc(false);
    if (OB_FAIL(set_pwd_stmt->set_user_password(user_name, host_name, password))) {
      LOG_WARN("Failed to set UserPasswordStmt");
    } else if (OB_FAIL(set_pwd_stmt->add_ssl_info(get_ssl_type_string(ssl_type),
                        infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_CIPHER)],
                        infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_ISSUER)],
                        infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_SUBJECT)]))) {
      LOG_WARN("Failed to add_ssl_info", K(ssl_type),
                "CIPHER", infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_CIPHER)],
                "ISSUER", infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_ISSUER)],
                "SUBJECT", infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_SUBJECT)], K(ret));
    }
  }
  return ret;
}

int ObSetPasswordResolver::resolve_oracle_password_strength(common::ObString &user_name,
                                                            common::ObString &hostname,
                                                            common::ObString &password)
{
  int ret = OB_SUCCESS;
  const share::schema::ObUserInfo *user = NULL;
  if (OB_ISNULL(session_info_) ||
      OB_ISNULL(schema_checker_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Session info or schema checker should not be NULL", K(ret));
  } else if (OB_FAIL(schema_checker_->get_user_info(session_info_->get_effective_tenant_id(),
                                                    user_name, hostname, user))) {
    LOG_WARN("fail to get user info", K(ret));
  } else {
    int64_t profile_id = user->get_profile_id();
    if (OB_FAIL(check_oracle_password_strength(session_info_->get_effective_tenant_id(),
      profile_id, password, user_name))) {
      LOG_WARN("fail to check oracle password strength", K(ret));
    }
  }
  return ret; 
}

/*
bugfix :
alter user resource identified by password core cause by invalid memory access
*/
int ObSetPasswordResolver::check_role_as_user(ParseNode *user_hostname_node, bool &is_valid) {
  is_valid = false;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(user_hostname_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to check_role_as_user, user_hostname_node is NULL", K(ret));
  } else if (!lib::is_oracle_mode() || T_VARCHAR != user_hostname_node->type_) {
    is_valid = true;
  } else {
    ObString node_str(user_hostname_node->str_len_, user_hostname_node->str_value_);
    if (0 != node_str.case_compare(OB_ORA_RESOURCE_ROLE_NAME) &&
        0 != node_str.case_compare(OB_ORA_PUBLIC_ROLE_NAME) &&
        0 != node_str.case_compare(OB_ORA_CONNECT_ROLE_NAME) &&
        0 != node_str.case_compare(OB_ORA_STANDBY_REPLICATION_ROLE_NAME)) {
      is_valid = true;
    }
  }
  return ret;
}