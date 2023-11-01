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
#include "sql/resolver/dcl/ob_create_user_resolver.h"
#include "sql/resolver/ddl/ob_database_resolver.h"
#include "sql/resolver/dcl/ob_set_password_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "objit/common/ob_item_type.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObCreateUserResolver::ObCreateUserResolver(ObResolverParams &params)
    : ObDCLResolver(params)
{
}

ObCreateUserResolver::~ObCreateUserResolver()
{
}

int ObCreateUserResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObCreateUserStmt *create_user_stmt = NULL;
  if (OB_UNLIKELY(lib::is_oracle_mode() && 5 != parse_tree.num_child_)
      || OB_UNLIKELY(lib::is_mysql_mode() && 4 != parse_tree.num_child_)
      || OB_UNLIKELY(T_CREATE_USER != parse_tree.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expect 4 child in mysql mode and 5 child in oracle mode, create user type",
             "actual_num", parse_tree.num_child_,
             "type", parse_tree.type_,
             K(ret));
  } else if (OB_ISNULL(params_.session_info_)
             || OB_ISNULL(schema_checker_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Session info or schema checker should not be NULL", K(ret));
	} else if (OB_ISNULL(create_user_stmt = create_stmt<ObCreateUserStmt>())) {
		ret = OB_ALLOCATE_MEMORY_FAILED;
		LOG_ERROR("Failed to create ObCreateUserStmt", K(ret));
	} else {
    stmt_ = create_user_stmt;
		ParseNode *if_not_exist = const_cast<ParseNode*>(parse_tree.children_[0]);
    ParseNode *users = const_cast<ParseNode*>(parse_tree.children_[1]);
    ParseNode *require_info = const_cast<ParseNode*>(parse_tree.children_[2]);
    ParseNode *resource_options = !lib::is_oracle_mode() ? const_cast<ParseNode*>(parse_tree.children_[3]) : NULL;
    ParseNode *profile = lib::is_oracle_mode() ? const_cast<ParseNode*>(parse_tree.children_[3]) : NULL;
    ParseNode *primary_zone = lib::is_oracle_mode() ? const_cast<ParseNode*>(parse_tree.children_[4]) : NULL; 
    ParseNode *ssl_infos = NULL;
    create_user_stmt->set_tenant_id(params_.session_info_->get_effective_tenant_id());
		//resolve if_not_exists
    if (OB_SUCC(ret)) {
      if (NULL != if_not_exist) {
        if (T_IF_NOT_EXISTS != if_not_exist->type_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(if_not_exist->type_), K(ret));
        } else {
          create_user_stmt->set_if_not_exists(true);
        }
      }
    }
    // resolve users' specs
    if (OB_FAIL(ret)) {
      // bypass
    } else if (OB_ISNULL(users) || OB_UNLIKELY(T_USERS != users->type_) || OB_UNLIKELY(users->num_child_ <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Create user ParseNode error", K(ret));
    } else {
      // replace password to *** in query_string for audit
      ObString masked_sql;
      if (session_info_->is_inner()) {
        // do nothing in inner_sql
      } else if (OB_FAIL(mask_password_for_users(allocator_,
          session_info_->get_current_query_string(), users, 1, masked_sql))) {
        LOG_WARN("fail to mask_password_for_users", K(ret));
      } else {
        create_user_stmt->set_masked_sql(masked_sql);
      }
      for (int i = 0; i < users->num_child_ && OB_SUCCESS == ret; ++i) {
        ParseNode *user_pass = users->children_[i];
        if (OB_ISNULL(user_pass)) {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("The child of parseNode should not be NULL", K(ret), K(i));
        } else if (OB_UNLIKELY(lib::is_oracle_mode() && 4 != user_pass->num_child_) ||
                   OB_UNLIKELY(lib::is_mysql_mode() && 5 != user_pass->num_child_ )) {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("sql_parser parse user_identification error", K(ret));
        } else if (OB_ISNULL(user_pass->children_[0])) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Child 0 of user_pass should not be NULL", K(ret));
        } else {
          ObString user_name(user_pass->children_[0]->str_len_, user_pass->children_[0]->str_value_);
          ObString host_name;

          if (user_pass->children_[0]->type_ != T_IDENT
              && OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                           *allocator_, session_info_->get_dtc_params(), user_name))) {
            LOG_WARN("fail to convert user name to utf8", K(ret), K(user_name),
                     KPHEX(user_name.ptr(), user_name.length()));
          } else if (!session_info_->is_inner() && (0 == user_name.case_compare(OB_RESTORE_USER_NAME))) {
            ret = OB_ERR_NO_PRIVILEGE;
            LOG_WARN("__oceanbase_inner_restore_user is reserved", K(ret));
          } else if (NULL == user_pass->children_[3]) {
            host_name.assign_ptr(OB_DEFAULT_HOST_NAME, static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
          } else {
            host_name.assign_ptr(user_pass->children_[3]->str_value_,
                                 static_cast<int32_t>(user_pass->children_[3]->str_len_));
          }
          if (OB_SUCC(ret) && lib::is_mysql_mode() && NULL != user_pass->children_[4]) {
            /* here code is to mock a auth plugin check. */
            ObString auth_plugin(static_cast<int32_t>(user_pass->children_[4]->str_len_),
                                  user_pass->children_[4]->str_value_);
            ObString default_auth_plugin;
            if (OB_FAIL(session_info_->get_sys_variable(SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN,
                                                        default_auth_plugin))) {
              LOG_WARN("fail to get block encryption variable", K(ret));
            } else if (0 != auth_plugin.compare(default_auth_plugin)) {
              ret = OB_ERR_PLUGIN_IS_NOT_LOADED;
              LOG_USER_ERROR(OB_ERR_PLUGIN_IS_NOT_LOADED, auth_plugin.length(), auth_plugin.ptr());
            } else {/* do nothing */}
          }
          if (OB_SUCC(ret) && lib::is_oracle_mode() && 0 != host_name.compare(OB_DEFAULT_HOST_NAME)) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "create user with hostname");
            LOG_WARN("create user should not use hostname in oracle mode", K(ret));
          }
          ObString password;
          ObString need_enc_str = ObString::make_string("NO");
          if (OB_SUCC(ret)) {
            if (user_name.empty()) {
              ret = OB_CANNOT_USER;
              LOG_WARN("user name is empty", K(ret));
              ObString create_user = ObString::make_string("CREATE USER");
              LOG_USER_ERROR(OB_CANNOT_USER, create_user.length(), create_user.ptr(), host_name.length(), host_name.ptr());
            } else if (OB_ISNULL(user_pass->children_[1])) {
              password = ObString::make_string("");
              //no enc
            } else if (OB_ISNULL(user_pass->children_[2])) {
              ret = OB_ERR_PARSE_SQL;
              LOG_WARN("Child 2 of user_pass should not be NULL here", K(ret));
            } else {
              password.assign_ptr(user_pass->children_[1]->str_value_,
                                  static_cast<int32_t>(user_pass->children_[1]->str_len_));
              bool need_enc = (1 == user_pass->children_[2]->value_);
              if (need_enc) {
                need_enc_str = ObString::make_string("YES");
              } else {
                //no enc
                if (!ObSetPasswordResolver::is_valid_mysql41_passwd(password)) {
                  ret = OB_ERR_PASSWORD_FORMAT;
                  LOG_WARN("Wrong password format", K(user_name), K(password), K(ret));
                }
              }
            }
          }
          uint64_t profile_id = OB_INVALID_ID;
          if (OB_SUCC(ret) && NULL != profile) {
            ParseNode *profile_name = nullptr;
            if (OB_UNLIKELY(profile->type_ != T_USER_PROFILE)
              || OB_UNLIKELY(profile->num_child_ != 1)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid type", K(ret), K(profile->type_), K(profile->num_child_));
            } else if (OB_ISNULL(profile_name = profile->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid profile name", K(ret));
            } else if (profile_name->type_ == T_DEFAULT) {
              profile_id = OB_INVALID_ID;
            } else if (OB_FAIL(schema_checker_->get_profile_id(params_.session_info_->get_effective_tenant_id(),
                                                               ObString(profile_name->str_len_, profile_name->str_value_),
                                                               profile_id))) {
              LOG_WARN("fail to get profile id", K(ret));
            }
          }
          create_user_stmt->set_profile_id(profile_id);  //只有oracle模式profile id是有效的
          if (OB_SUCC(ret)) {
            if (!lib::is_oracle_mode() && OB_FAIL(check_password_strength(password))) {
              LOG_WARN("password don't satisfied current policy", K(ret));
            } else if (lib::is_oracle_mode() && OB_FAIL(check_oracle_password_strength(
              params_.session_info_->get_effective_tenant_id(),
              profile_id,
              password, user_name))) {
              LOG_WARN("fail to check password strength", K(ret));
            }
          }
          
          if (OB_SUCC(ret)) {
            if (user_name.length() > OB_MAX_USER_NAME_LENGTH) {
              ret = OB_WRONG_USER_NAME_LENGTH;
              LOG_USER_ERROR(OB_WRONG_USER_NAME_LENGTH, user_name.length(), user_name.ptr());
            } else if (OB_FAIL(create_user_stmt->add_user(user_name, host_name, password, need_enc_str))) {
              LOG_WARN("Failed to add user to ObCreateUserStmt", K(user_name), K(host_name), K(password), K(ret));
            } else {
              //do nothing
            }
          }
        }
      } //end of for
    }

    ObSSLType ssl_type = ObSSLType::SSL_TYPE_NOT_SPECIFIED;
    ObString infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_MAX)] = {};
    // resolve users' specs
    if (OB_FAIL(ret)) {
      // bypass
    } else if (NULL == require_info) {
      // bypass
    } else if (OB_UNLIKELY(T_TLS_OPTIONS != require_info->type_)
               || OB_UNLIKELY(require_info->num_child_ != 1)
               || OB_ISNULL(ssl_infos = require_info->children_[0])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Create user ParseNode error", K(ret), K(require_info->type_), K(require_info->num_child_), KP(ssl_infos));
    } else if (OB_UNLIKELY(ssl_infos->type_ < T_TLS_NONE && ssl_infos->type_ > T_TLS_SPECIFIED)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Create user ParseNode error", K(ret), K(ssl_infos->type_));
    } else {
      ssl_type = static_cast<ObSSLType>(static_cast<int32_t>(ObSSLType::SSL_TYPE_NONE) + (ssl_infos->type_ - T_TLS_NONE));

      if (ObSSLType::SSL_TYPE_SPECIFIED == ssl_type) {
        ParseNode *specified_ssl_infos = NULL;
        if (OB_UNLIKELY(ssl_infos->num_child_ != 1)
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
    if (OB_SUCC(ret) && OB_NOT_NULL(primary_zone)) {
      if (T_PRIMARY_ZONE != primary_zone->type_ || NULL == primary_zone->children_ || 
          primary_zone->num_child_ != 1) {
        ret = common::OB_INVALID_ARGUMENT;
        LOG_WARN("invalid primary_zone argument", K(ret), "num_child", primary_zone->num_child_);
      } else if (OB_FAIL(ObDatabaseResolver<ObCreateUserStmt>::resolve_primary_zone(
            create_user_stmt, primary_zone->children_[0]))) {
        LOG_WARN("fail to resolve primary zone", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_user_stmt->add_ssl_info(get_ssl_type_string(ssl_type),
                                                 infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_CIPHER)],
                                                 infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_ISSUER)],
                                                 infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_SUBJECT)]))) {
        LOG_WARN("Failed to add_ssl_info", K(ssl_type),
                 "CIPHER", infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_CIPHER)],
                 "ISSUER", infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_ISSUER)],
                 "SUBJECT", infos[static_cast<int32_t>(ObSSLSpecifiedType::SSL_SPEC_TYPE_SUBJECT)], K(ret));
      }
    }
    if (OB_SUCC(ret) && NULL != resource_options) {
      if (T_USER_RESOURCE_OPTIONS != resource_options->type_
                || OB_ISNULL(resource_options->children_)) {
        ret = common::OB_INVALID_ARGUMENT;
        LOG_WARN("invalid resource options argument", K(ret), K(resource_options->type_),
                  K(resource_options->children_));
      } else {
        for (int64_t i = 0; i < resource_options->num_child_; i++) {
          ParseNode *res_option = resource_options->children_[i];
          if (OB_ISNULL(res_option)) {
            ret = common::OB_INVALID_ARGUMENT;
            LOG_WARN("null res option", K(ret), K(i));
          } else if (T_MAX_CONNECTIONS_PER_HOUR == res_option->type_) {
            uint64_t max_connections_per_hour = static_cast<uint64_t>(res_option->value_);
            max_connections_per_hour = max_connections_per_hour > MAX_CONNECTIONS ? MAX_CONNECTIONS
                                       : max_connections_per_hour;
            create_user_stmt->set_max_connections_per_hour(max_connections_per_hour);
          } else if (T_MAX_USER_CONNECTIONS == res_option->type_) {
            uint64_t max_user_connections = static_cast<uint64_t>(res_option->value_);
            max_user_connections = max_user_connections > MAX_CONNECTIONS ? MAX_CONNECTIONS
                                   : max_user_connections;
            create_user_stmt->set_max_user_connections(max_user_connections);
          }
        }
      }
    }

    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      OZ (schema_checker_->check_ora_ddl_priv(
            session_info_->get_effective_tenant_id(),
            session_info_->get_priv_user_id(),
            ObString(""),
            stmt::T_CREATE_USER,
            session_info_->get_enable_role_array()),
            session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
  }
  return ret;
}
