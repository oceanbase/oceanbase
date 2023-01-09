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
  if (OB_ISNULL(session_info_)) {
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
                                           db));
      }
    }
  }
  return ret;
}

int ObDCLResolver::check_password_strength(common::ObString &password, common::ObString &user_name)
{
  int ret = OB_SUCCESS;
  int64_t pw_policy = 0;
  uint64_t valid_pw_len = 0;
  if (OB_ISNULL(session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Session info is not inited", K(ret));
    // 0 代表密码政策为low, 1代表密码政策为medium
  } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_POLICY, pw_policy))) {
    LOG_WARN("fail to get validate_password_policy variable", K(ret));
  } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_LENGTH, valid_pw_len))) {
    LOG_WARN("fail to get validate_password_length variable", K(ret));
  } else if (ObPasswordPolicy::LOW == pw_policy) {
    if (OB_FAIL(check_password_len(password, valid_pw_len))) {
      LOG_WARN("password len dont satisfied current pw policy", K(ret));
    }
  } else if (ObPasswordPolicy::MEDIUM == pw_policy) {
    uint64_t valid_pw_len = 0;
    int64_t check_user_name_flag = 0;
    uint64_t mix_case_count = 0;
    uint64_t number_count = 0;
    uint64_t special_char_count = 0;
    if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_CHECK_USER_NAME, check_user_name_flag))) {
      LOG_WARN("fail to get validate_password_check_user_name variable", K(ret));
    } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_NUMBER_COUNT, number_count))) {
      LOG_WARN("fail to get validate_password_number_count variable", K(ret));
    } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_SPECIAL_CHAR_COUNT, special_char_count))) {
      LOG_WARN("fail to get validate_password_length variable", K(ret));
    } else if (OB_FAIL(session_info_->get_sys_variable(share::SYS_VAR_VALIDATE_PASSWORD_MIXED_CASE_COUNT, mix_case_count))) {
      LOG_WARN("fail to get validate_password_mixed_case_count variable", K(ret));
    } else if (OB_FAIL(check_number_count(password, number_count))) {
      LOG_WARN("password number count not satisfied current pw policy", K(ret));
    } else if (OB_FAIL(check_special_char_count(password, special_char_count))) {
      LOG_WARN("password special char count not satisfied current pw policy", K(ret));
    } else if (OB_FAIL(check_mixed_case_count(password, mix_case_count))) {
      LOG_WARN("password mixed case count not satisfied current pw policy", K(ret));
    } else if (!check_user_name_flag && OB_FAIL(check_user_name(password, user_name))) {
      LOG_WARN("password cannot be the same with user name", K(ret));
    } else if (OB_FAIL(check_password_len(password, valid_pw_len))) {
      LOG_WARN("password len dont satisfied current pw policy", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the value of password policy is unexpectd", K(ret));
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


int ObDCLResolver::check_number_count(common::ObString &password, const int64_t &number_count)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  for (int i = 0; OB_SUCC(ret) && i < password.length(); ++i) {
    if (password[i] >= '0' && password[i] <= '9') {
      count++;
    }
    if (count >= number_count) {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (number_count > count) {
      ret = OB_ERR_NOT_VALID_PASSWORD;
      LOG_WARN("the password is not valid", K(ret));
    }
  }
  return ret;
}

int ObDCLResolver::check_special_char_count(common::ObString &password, const int64_t &special_char_count)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  for (int i = 0; OB_SUCC(ret) && i < password.length(); ++i) {
    if ((password[i] >= '!' && password[i] <= '/')||
        (password[i] >= ':' && password[i] <= '?')) {
      count++;
    }
    if (count >= special_char_count) {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (special_char_count > count) {
      ret = OB_ERR_NOT_VALID_PASSWORD;
      LOG_WARN("the password is not valid", K(ret));
    }
  }
  return ret;
}

int ObDCLResolver::check_mixed_case_count(common::ObString &password, const int64_t &mix_case_count)
{
  int ret = OB_SUCCESS;
  int64_t lower_count = 0;
  int64_t upper_count = 0;
  for (int i = 0; OB_SUCC(ret) && i < password.length(); ++i) {
    if (islower(password[i])) {
      lower_count++;
    } else if (isupper(password[i])) {
      upper_count++;
    }
    if (lower_count >= mix_case_count && upper_count >= mix_case_count) {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (mix_case_count > lower_count || mix_case_count > upper_count) {
      ret = OB_ERR_NOT_VALID_PASSWORD;
      LOG_WARN("the password is not valid", K(ret));
    }
  }
  return ret;
}

int ObDCLResolver::check_password_len(common::ObString &password, const int64_t &password_len)
{
  int ret = OB_SUCCESS;
  if (password.length() < password_len) {
    ret = OB_ERR_NOT_VALID_PASSWORD;
    LOG_WARN("the password is not valid", K(ret));
  }
  return ret;
}

int ObDCLResolver::check_user_name(common::ObString &password, common::ObString &user_name)
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

int ObDCLResolver::mask_password_for_passwd_node(ObIAllocator *allocator,
    const common::ObString &src,
    const ParseNode *passwd_node,
    common::ObString &masked_sql)
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
    uint64_t pwd_len = passwd_node->stmt_loc_.last_column_ -
                        passwd_node->stmt_loc_.first_column_ + 1;
    MEMSET(tmp_sql.ptr() + passwd_node->stmt_loc_.first_column_, password_mask_, pwd_len);
  }
  if (OB_SUCC(ret)) {
    masked_sql = tmp_sql;
  }
  LOG_DEBUG("finish mask_password_for_passwd_node", K(src), K(masked_sql));
  return ret;
}
