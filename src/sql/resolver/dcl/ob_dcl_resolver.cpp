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
#include "observer/ob_server_struct.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "sql/engine/expr/ob_expr_validate_password_strength.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::observer;

int ObDCLResolver::check_and_convert_name(ObString &db, ObString &table)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Session info is not inited", K(ret));
  } else if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
    LOG_WARN("fail to get name case mode", K(mode), K(ret));
  } else {
    bool perserve_lettercase = lib::is_oracle_mode() ?
        true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
    ObCollationType cs_type = CS_TYPE_INVALID;
    if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
      LOG_WARN("fail to get collation_connection", K(ret));
    } else if (db.length() > 0
               && OB_FAIL(ObSQLUtils::check_and_convert_db_name(
                       cs_type, perserve_lettercase, db))) {
      LOG_WARN("Check and convert db name error", K(ret));
    } else if (table.length() > 0
               && OB_FAIL(ObSQLUtils::check_and_convert_table_name(
                       cs_type, perserve_lettercase, table))) {
      LOG_WARN("Check and convert table name error", K(ret));
    } else {
      //do nothing
      if (db.length() > 0) {
        CK (OB_NOT_NULL(schema_checker_));
        CK (OB_NOT_NULL(schema_checker_->get_schema_guard()));
        OZ (ObSQLUtils::cvt_db_name_to_org(*schema_checker_->get_schema_guard(),
                                           session_info_,
                                           db,
                                           allocator_));
      }
    }
  }
  return ret;
}

int ObDCLResolver::check_password_strength(common::ObString &password)
{
  int ret = OB_SUCCESS;
  int64_t pw_policy = 0;
  int64_t check_user_name_flag = 0;
  size_t char_len = ObCharset::strlen_char(ObCharset::get_system_collation(), password.ptr(),
                                               static_cast<int64_t>(password.length()));
  bool passed = true;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Session info is not inited", K(ret));
  } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_POLICY, pw_policy))) {
    LOG_WARN("fail to get validate_password_policy variable", K(ret));
  } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME, check_user_name_flag))) {
    LOG_WARN("fail to get validate_password_check_user_name variable", K(ret));
  } else if (!check_user_name_flag && OB_FAIL(check_user_name(password, session_info_->get_user_name()))) {
    LOG_WARN("password cannot be the same with user name", K(ret));
  } else if (OB_FAIL(ObExprValidatePasswordStrength::validate_password_low(password,
                                                                           char_len,
                                                                           *session_info_,
                                                                           passed))) {
    LOG_WARN("password len dont satisfied current pw policy", K(ret));
  } else if (ObPasswordPolicy::LOW == pw_policy) {
    // do nothing
  } else if (ObPasswordPolicy::MEDIUM == pw_policy) {
    if (OB_FAIL(ObExprValidatePasswordStrength::validate_password_medium(password,
                                                                         char_len,
                                                                         *session_info_,
                                                                         passed))) {
      LOG_WARN("password len dont satisfied current pw policy", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the value of password policy is unexpected", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!passed)) {
      ret = OB_ERR_NOT_VALID_PASSWORD;
      LOG_WARN("the password is not valid", K(ret));
    }
  }
  return ret;
}

int ObDCLResolver::check_oracle_password_strength(int64_t tenant_id,
                                                  int64_t profile_id,
                                                  common::ObString &password, 
                                                  common::ObString &user_name)
{
  int ret = OB_SUCCESS;
  ObString old_password("NULL");
  ObString function_name;
  // 如果profile_id未指定, 使用默认profile.
  if (profile_id == OB_INVALID_ID) {
    profile_id = OB_ORACLE_TENANT_INNER_PROFILE_ID;
  }
  if (OB_ISNULL(schema_checker_) ||
      OB_ISNULL(schema_checker_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema checker is null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_schema_guard()->
                     get_user_profile_function_name(tenant_id, profile_id, function_name))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    if (0 == function_name.length() || 
        0 == function_name.case_compare("NULL")) {
       /*do nothing*/
    } else if (password.length() <= 0 ||
               user_name.length() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("password cannot be null", K(ret));
    } else {
      common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
      ObSqlString sql;
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        sqlclient::ObMySQLResult *sql_result = NULL;
        ObISQLConnection *conn = NULL;
        ObInnerSQLConnectionPool *pool = NULL;
        if (OB_FAIL(sql.append_fmt("SELECT  %.*s('%.*s', '%.*s', %.*s) AS RES FROM DUAL",
            function_name.length(), function_name.ptr(), 
            user_name.length(), user_name.ptr(),
            password.length(), password.ptr(),
            old_password.length(), old_password.ptr()))) {
          LOG_WARN("append sql failed", K(ret), K(sql));
        } else if (OB_ISNULL(pool = static_cast<ObInnerSQLConnectionPool*>(
                             sql_proxy->get_pool()))) {
          ret = OB_NOT_INIT;
          LOG_WARN("connection pool is NULL", K(ret));
        } else if (OB_FAIL(pool->acquire(session_info_, conn))) {
          LOG_WARN("failed to acquire inner connection", K(ret));
        } else if (OB_FAIL(conn->execute_read(session_info_->get_effective_tenant_id(), 
              sql.ptr(), res, true))) {
          LOG_WARN("execute sql failed", K(ret), K(sql));
        } else if (OB_ISNULL(sql_result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", K(ret));
        } else if (OB_FAIL(sql_result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("result next failed", K(ret));
          }
        } else {
          int64_t verify_result = 0;
          EXTRACT_INT_FIELD_MYSQL(*sql_result, "RES", verify_result, int64_t);
          if (OB_SUCC(ret)) { 
            if (1 != verify_result) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("fail to verify password", K(ret));
            }
          }
        }
        if (OB_NOT_NULL(conn) && OB_NOT_NULL(sql_proxy)) {
          sql_proxy->close(conn, true);
        }
      }
    }
  }
  return ret;
}

int ObDCLResolver::check_user_name(common::ObString &password, const common::ObString &user_name)
{
  int ret = OB_SUCCESS;
  if (ObCharset::case_insensitive_equal(password, user_name)) {
    ret = OB_ERR_NOT_VALID_PASSWORD;
    LOG_WARN("the password cannot be the same with the user_name", K(ret));
  }
  return ret;
}

int ObDCLResolver::mask_password_for_single_user(ObIAllocator *allocator,
    const common::ObString &src,
    const ParseNode *user_pass,
    int64_t pwd_idx,
    common::ObString &masked_sql)
{
  int ret = OB_SUCCESS;
  ParseNode *pass_node = NULL;
  if (OB_ISNULL(user_pass)) {
    ret = OB_ERR_PARSE_SQL;
    LOG_WARN("The parseNode should not be NULL", K(ret));
  } else if (user_pass->num_child_ <= pwd_idx) {
    ret = OB_ERR_PARSE_SQL;
    LOG_WARN("sql_parser parse user_identification error", K(ret));
  } else if (FALSE_IT(pass_node = user_pass->children_[pwd_idx])) {
  } else if (OB_FAIL(mask_password_for_passwd_node(allocator, src, pass_node, masked_sql))) {
        LOG_WARN("failed to generated masked_sql", K(src), K(ret));
  }
  
  LOG_DEBUG("finish mask_password_for_users", K(src), K(masked_sql));
  return ret;
}

int ObDCLResolver::mask_password_for_users(ObIAllocator *allocator,
    const common::ObString &src,
    const ParseNode *users,
    int64_t pwd_idx,
    common::ObString &masked_sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(users) 
      || OB_UNLIKELY(T_USERS != users->type_) 
      || OB_UNLIKELY(users->num_child_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("users ParseNode error", K(ret));
  } else {
    for (int i = 0; i < users->num_child_ && OB_SUCC(ret); ++i) {
      ParseNode *user_pass = users->children_[i];
      ParseNode *pass_node = NULL;
      if (OB_ISNULL(user_pass)) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("The child of parseNode should not be NULL", K(ret), K(i));
      } else if (user_pass->num_child_ == 0) {
        // do nothing
      } else if (user_pass->num_child_ <= pwd_idx) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("sql_parser parse user_identification error", K(ret));
      } else if (FALSE_IT(pass_node = user_pass->children_[pwd_idx])) {
      } else if (OB_FAIL(mask_password_for_passwd_node(allocator, src, pass_node, masked_sql))) {
        LOG_WARN("failed to generated masked_sql", K(src), K(ret));
      }
    }
  }

  LOG_DEBUG("finish mask_password_for_users", K(src), K(masked_sql));
  return ret;
}

int ObDCLResolver::mask_password_for_passwd_node(
    ObIAllocator *allocator,
    const common::ObString &src,
    const ParseNode *passwd_node,
    common::ObString &masked_sql,
    bool skip_enclosed_char)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  ObString tmp_sql;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is NULL", K(ret));
  } else if (OB_ISNULL(src.ptr()) || OB_UNLIKELY(0 >= src_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src sql_text should not be NULL", K(src), K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator, src, tmp_sql))) {
    LOG_WARN("fail to ob_write_string", K(src), K(ret));
  } else if (OB_ISNULL(passwd_node)) {
    // do nothing
  } else if (passwd_node->stmt_loc_.last_column_ >= src_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("last column overflow", K(src_len), K(passwd_node->stmt_loc_.last_column_), K(ret));
  } else {
    int64_t start_pos = passwd_node->stmt_loc_.first_column_;
    int64_t end_pos = passwd_node->stmt_loc_.last_column_ + 1;
    if (skip_enclosed_char && end_pos - start_pos >= 2) {
      start_pos += 1;
      end_pos -= 1;
    }
    uint64_t pwd_len = end_pos - start_pos;
    MEMSET(tmp_sql.ptr() + start_pos, password_mask_, pwd_len);
  }
  if (OB_SUCC(ret)) {
    masked_sql = tmp_sql;
  }
  LOG_DEBUG("finish mask_password_for_passwd_node", K(src), K(masked_sql));
  return ret;
}

int ObDCLResolver::check_dcl_on_inner_user(const ObItemType &type,
                                           const uint64_t &session_user_id,
                                           const ObString &user_name,
                                           const ObString &host_name) {
  int ret = OB_SUCCESS;
  uint64_t user_id = OB_INVALID_ID;
  bool is_valid = true;
  if (GCONF._enable_reserved_user_dcl_restriction) {
    if (T_ALTER_USER_DEFAULT_ROLE == type ||
        T_ALTER_USER_PRIMARY_ZONE == type ||
        T_ALTER_USER_PROFILE == type ||
        T_DROP_USER == type ||
        T_GRANT == type ||
        T_LOCK_USER == type ||
        T_RENAME_USER == type ||
        T_REVOKE == type ||
        T_REVOKE_ALL == type ||
        T_REVOKE_ROLE == type ||
        T_SET_PASSWORD == type ||
        T_SET_ROLE == type ||
        T_SYSTEM_REVOKE == type) {
      if (user_name.empty() || session_user_id == OB_INVALID_ID) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed. get empty user name or invalid session user id", K(ret), K(user_name),
                 K(session_user_id));
      } else if (OB_ISNULL(schema_checker_) || OB_ISNULL(params_.session_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed. get NULL ptr", K(ret), K(schema_checker_), K(params_.session_info_));
      } else if (OB_FAIL(schema_checker_->get_user_id(
                                                  params_.session_info_->get_effective_tenant_id(),
                                                  user_name,
                                                  host_name,
                                                  user_id))) {
        if (OB_USER_NOT_EXIST == ret) {
          // do not check user exists here
          ret = OB_SUCCESS;
          LOG_TRACE("user is not exists", K(user_name), K(host_name));
        } else {
          LOG_WARN("failed to get user id", K(ret), K(user_name));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_SYS_USER_ID == user_id ||
            OB_ORA_SYS_USER_ID == user_id ||
            OB_ORA_AUDITOR_USER_ID == user_id ||
            OB_ORA_LBACSYS_USER_ID == user_id) {
          if (session_user_id != user_id &&
              OB_SYS_USER_ID != session_user_id &&
              OB_ORA_SYS_USER_ID != session_user_id) {
            is_valid = false;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !is_valid) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "modify on reserved user");
  }
  return ret;
}

int ObDCLResolver::check_dcl_on_inner_user(const ObItemType &type,
                                           const uint64_t &session_user_id,
                                           const uint64_t &user_id) {
  int ret = OB_SUCCESS;
  bool is_valid = true;
  if (GCONF._enable_reserved_user_dcl_restriction) {
    if (T_ALTER_USER_DEFAULT_ROLE == type ||
        T_ALTER_USER_PRIMARY_ZONE == type ||
        T_ALTER_USER_PROFILE == type ||
        T_DROP_USER == type ||
        T_GRANT == type ||
        T_LOCK_USER == type ||
        T_RENAME_USER == type ||
        T_REVOKE == type ||
        T_REVOKE_ALL == type ||
        T_REVOKE_ROLE == type ||
        T_SET_PASSWORD == type ||
        T_SET_ROLE == type ||
        T_SYSTEM_REVOKE == type) {
      if (OB_INVALID_ID == user_id || OB_INVALID_ID == session_user_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed.invalid session user id/user id", K(ret), K(user_id), K(session_user_id));
      } else if (OB_SYS_USER_ID == user_id ||
                 OB_ORA_SYS_USER_ID == user_id ||
                 OB_ORA_AUDITOR_USER_ID == user_id ||
                 OB_ORA_LBACSYS_USER_ID == user_id) {
        if (session_user_id != user_id &&
            OB_SYS_USER_ID != session_user_id &&
            OB_ORA_SYS_USER_ID != session_user_id) {
          is_valid = false;
        }
      }
    }
  }
  if (OB_SUCC(ret) && !is_valid) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "modify on reserved user");
  }
  return ret;
}

int ObDCLResolver::resolve_user_list_node(ParseNode *user_node,
                                          ParseNode *top_node,
                                          ObString &user_name,
                                          ObString &host_name)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *user_info = NULL;
  if (OB_ISNULL(user_node)) {
    ret = OB_ERR_PARSE_SQL;
    LOG_WARN("The child of user_hostname node should not be NULL", K(ret));
  } else if (2 != user_node->num_child_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_parser parse user error", K(ret));
  } else if (OB_ISNULL(user_node->children_[0])) {
    // 0: user, 1: hostname
    ret = OB_ERR_PARSE_SQL;
    LOG_WARN("The child of user node should not be NULL", K(ret));
  } else {
    ParseNode *user_hostname_node = user_node;
    user_name = ObString (user_hostname_node->children_[0]->str_len_, user_hostname_node->children_[0]->str_value_);
    if (NULL == user_hostname_node->children_[1]) {
      host_name.assign_ptr(OB_DEFAULT_HOST_NAME, static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
    } else {
      host_name.assign_ptr(user_hostname_node->children_[1]->str_value_,
                           static_cast<int32_t>(user_hostname_node->children_[1]->str_len_));
    }
    if (OB_FAIL(schema_checker_->get_user_info(params_.session_info_->get_effective_tenant_id(),
                                               user_name, host_name, user_info))) {
      LOG_WARN("failed to get user info", K(ret), K(user_name));
      if (OB_USER_NOT_EXIST == ret) {
        // 跳过, RS统一处理, 兼容MySQL行为
        ret = OB_SUCCESS;
        user_info = NULL;
      }
    } else if (is_inner_user_or_role(user_info->get_user_id())) {
      ret = OB_ERR_NO_PRIVILEGE;
      SQL_RESV_LOG(WARN, "Can not drop internal user", K(ret));
    } else if (OB_FAIL(check_dcl_on_inner_user(top_node->type_,
                                               params_.session_info_->get_priv_user_id(),
                                               user_info->get_user_id()))) {
      LOG_WARN("failed to check dcl on inner-user or unsupport to modify reserved user",
               K(ret), K(params_.session_info_->get_user_name()), K(user_name));
    }
  }
  return ret;
}

int ObDCLResolver::resolve_user_host(const ParseNode *user_pass,
                                     common::ObString &user_name,
                                     common::ObString &host_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(user_pass) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(user_pass->children_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Child 0 of user_pass should not be NULL", K(ret));
  } else {
    if (user_pass->children_[0]->type_ == T_FUN_SYS_CURRENT_USER) {
      user_name = session_info_->get_user_name();
      host_name = session_info_->get_host_name();
    } else {
      user_name = ObString(user_pass->children_[0]->str_len_, user_pass->children_[0]->str_value_);
    }

    if (user_pass->children_[0]->type_ != T_IDENT
        && OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                     *allocator_, session_info_->get_dtc_params(), user_name))) {
      LOG_WARN("fail to convert user name to utf8", K(ret), K(user_name),
               KPHEX(user_name.ptr(), user_name.length()));
    } else if (!session_info_->is_inner() && (0 == user_name.case_compare(OB_RESTORE_USER_NAME))) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("__oceanbase_inner_restore_user is reserved", K(ret));
    } else if (NULL == user_pass->children_[3]) {
      if (user_pass->children_[0]->type_ == T_FUN_SYS_CURRENT_USER) {
        //skip
      } else {
        host_name.assign_ptr(OB_DEFAULT_HOST_NAME, static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
      }
    } else {
      host_name.assign_ptr(user_pass->children_[3]->str_value_,
                           static_cast<int32_t>(user_pass->children_[3]->str_len_));
    }
    if (OB_SUCC(ret) && lib::is_mysql_mode() && NULL != user_pass->children_[4]) {
      /* here code is to mock a auth plugin check. */
      ObString auth_plugin(static_cast<int32_t>(user_pass->children_[4]->str_len_),
                            user_pass->children_[4]->str_value_);
      ObString default_auth_plugin;
      if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_DEFAULT_AUTHENTICATION_PLUGIN,
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
  }
  return ret;
}
