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

#define USING_LOG_PREFIX RS
#include "rootserver/ob_ddl_sql_generator.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_priv_common.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace obrpc;

namespace rootserver
{

static const char *IF_NOT_EXIST = "if not exists";

int ObDDLSqlGenerator::get_priv_name(const int64_t priv, const char *&name)
{
  int ret = OB_SUCCESS;
  name = NULL;
  switch (priv) {
    case 0:
      name = "USAGE"; break; //usage means no privilege
    case OB_PRIV_ALTER:
      name = "ALTER"; break;
    case OB_PRIV_CREATE:
      name = "CREATE"; break;
    case OB_PRIV_CREATE_USER:
      name = "CREATE USER"; break;
    case OB_PRIV_DELETE:
      name = "DELETE"; break;
    case OB_PRIV_DROP:
      name = "DROP"; break;
    case OB_PRIV_GRANT:
      name = "GRANT OPTION"; break;
    case OB_PRIV_INSERT:
      name = "INSERT"; break;
    case OB_PRIV_UPDATE:
      name = "UPDATE"; break;
    case OB_PRIV_SELECT:
      name = "SELECT"; break;
    case OB_PRIV_INDEX:
      name = "INDEX"; break;
    case OB_PRIV_CREATE_VIEW:
      name = "CREATE VIEW"; break;
    case OB_PRIV_SHOW_VIEW:
      name = "SHOW VIEW"; break;
    case OB_PRIV_SHOW_DB:
      name = "SHOW DATABASES"; break;
    case OB_PRIV_SUPER:
      name = "SUPER"; break;
    case OB_PRIV_PROCESS:
      name = "PROCESS"; break;
    case OB_PRIV_BOOTSTRAP:
      name = "BOOSTRAP"; break;
    case OB_PRIV_CREATE_SYNONYM:
      name = "CREATE SYNONYM"; break;
    case OB_PRIV_AUDIT:
      name = "AUDIT"; break;
    case OB_PRIV_COMMENT:
      name = "COMMENT"; break;
    case OB_PRIV_LOCK:
      name = "LOCK"; break;
    case OB_PRIV_RENAME:
      name = "RENAME"; break;
    case OB_PRIV_REFERENCES:
      name = "REFERENCES"; break;
    case OB_PRIV_FLASHBACK:
      name = "FLASHBACK"; break;
    case OB_PRIV_READ:
      name = "READ"; break;
    case OB_PRIV_WRITE:
      name = "WRITE"; break;
    case OB_PRIV_FILE:
      name = "FILE"; break;
    case OB_PRIV_ALTER_TENANT:
      name = "ALTER TENANT"; break;
    case OB_PRIV_ALTER_SYSTEM:
      name = "ALTER SYSTEM"; break;
    case OB_PRIV_CREATE_RESOURCE_POOL:
      name = "CREATE RESOURCE POOL"; break;
    case OB_PRIV_CREATE_RESOURCE_UNIT:
      name = "CREATE RESOURCE UNIT"; break;
    case OB_PRIV_REPL_SLAVE:
      name = "REPLICATION SLAVE"; break;
    case OB_PRIV_REPL_CLIENT:
      name = "REPLICATION CLIENT"; break;
    case OB_PRIV_DROP_DATABASE_LINK:
      name = "DROP DATABASE LINK"; break;
    case OB_PRIV_CREATE_DATABASE_LINK:
      name = "CREATE DATABASE LINK"; break;
    case OB_PRIV_EXECUTE:
      name = "EXECUTE"; break;
    case OB_PRIV_ALTER_ROUTINE:
      name = "ALTER ROUTINE"; break;
    case OB_PRIV_CREATE_ROUTINE:
      name = "CREATE ROUTINE"; break;
    case OB_PRIV_CREATE_TABLESPACE:
      name = "CREATE TABLESPACE"; break;
    case OB_PRIV_SHUTDOWN:
      name = "SHUTDOWN"; break;
    case OB_PRIV_RELOAD:
      name = "RELOAD"; break;
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid priv", K(ret), K(priv));
    }
  }
  return ret;
}

int ObDDLSqlGenerator::gen_create_user_sql(const ObAccountArg &account,
                                           const ObString &password,
                                           ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  sql_string.reset();
  if (OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user_name is empty", K(account), K(password), K(ret));
  } else {
    if (account.is_role_) {
      static const char* const CREATE_ROLE_SQL = "CREATE ROLE \"%.*s\"";
      if (OB_FAIL(sql_string.append_fmt(CREATE_ROLE_SQL,
              account.user_name_.length(),
              account.user_name_.ptr()))) {
        LOG_WARN("append sql failed", K(account), K(ret));
      }
    } else {
      char CREATE_USER_SQL[] = "CREATE USER %s `%.*s`";
      char NEW_CREATE_USER_SQL[] = "CREATE USER %s `%.*s`@`%.*s`";
      if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
        if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(CREATE_USER_SQL),
                                          lib::is_oracle_mode() ? "" : IF_NOT_EXIST,
                                          account.user_name_.length(),
                                          account.user_name_.ptr()))) {
          LOG_WARN("append sql failed", K(account), K(password), K(ret));
        }
      } else {
        if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(NEW_CREATE_USER_SQL),
                                          lib::is_oracle_mode() ? "" : IF_NOT_EXIST,
                                          account.user_name_.length(),
                                          account.user_name_.ptr(),
                                          account.host_name_.length(),
                                          account.host_name_.ptr()))) {
          LOG_WARN("append sql failed", K(account), K(password), K(ret));
        }
      }
    }
  }
  // mysql mode 且密码不为空串
  if (OB_SUCC(ret) && !lib::is_oracle_mode() && !password.empty()) {
    if (OB_FAIL(sql_string.append_fmt(" IDENTIFIED BY PASSWORD '%.*s'",
                                      password.length(),
                                      password.ptr()))) {
      LOG_WARN("append sql failed", K(password), K(ret), K(account));
    }
  } else if (OB_SUCC(ret) && lib::is_oracle_mode() 
             && !password.empty()) {
    // oracle mode 且密码不为空串
    if (OB_FAIL(sql_string.append_fmt(" IDENTIFIED BY VALUES \"%.*s\"",
                                      password.length(),
                                      password.ptr()))) {
      LOG_WARN("append sql failed", K(password), K(ret), K(account));
    }
  } else if (OB_SUCC(ret) && lib::is_oracle_mode() 
             && password.empty()) {
    // oracle mode 且密码为空串
    if (OB_FAIL(sql_string.append(" IDENTIFIED BY \"\""))) {
      LOG_WARN("append sql failed", K(ret), K(account));
    }
  }

  return ret;
}

int ObDDLSqlGenerator::gen_alter_role_sql(const ObAccountArg &account,
                                          const ObString &password,
                                          ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  sql_string.reset();
  if (OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("role_name is empty()", K(account), K(password), K(ret));
  } else {
    static const char* const ALTER_ROLE_SQL = "ALTER ROLE \"%.*s\"";
    if (OB_FAIL(sql_string.append_fmt(ALTER_ROLE_SQL,
                                      account.user_name_.length(),
                                      account.user_name_.ptr()))) {
      LOG_WARN("append sql failed", K(account), K(ret));
    } else if (password.empty()) {
      if (OB_FAIL(sql_string.append_fmt(" NOT IDENTIFIED"))) {
        LOG_WARN("append sql failed", K(account), K(ret));
      }
    } else {
      if (OB_FAIL(sql_string.append_fmt(" IDENTIFIED BY VALUES \"%.*s\"",
                                        password.length(),
                                        password.ptr()))) {
        LOG_WARN("append sql failed", K(account), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLSqlGenerator::append_ssl_info_sql(const ObSSLType &ssl_type,
                                           const common::ObString &ssl_cipher,
                                           const common::ObString &x509_issuer,
                                           const common::ObString &x509_subject,
                                           common::ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  //static const char* const APPEND_SSL_INFO_SQL = " REQUIRE USER if not exists `%.*s`";
  switch (ssl_type) {
    case ObSSLType::SSL_TYPE_NOT_SPECIFIED: {
      //do nothings
      break;
    }
    case ObSSLType::SSL_TYPE_NONE: {
      if (OB_FAIL(sql_string.append_fmt(" REQUIRE NONE "))) {
        OB_LOG(WARN, "fail to append ssl info", K(ret));
      }
      break;
    }
    case ObSSLType::SSL_TYPE_ANY: {
      if (OB_FAIL(sql_string.append_fmt(" REQUIRE SSL "))) {
        OB_LOG(WARN, "fail to append ssl info", K(ret));
      }
      break;
    }
    case ObSSLType::SSL_TYPE_X509: {
      if (OB_FAIL(sql_string.append_fmt(" REQUIRE X509 "))) {
        OB_LOG(WARN, "fail to append ssl info", K(ret));
      }
      break;
    }
    case ObSSLType::SSL_TYPE_SPECIFIED: {
      if (OB_FAIL(sql_string.append_fmt(" REQUIRE "))) {
        OB_LOG(WARN, "fail to append ssl info", K(ret));
      } else if (!ssl_cipher.empty() && OB_FAIL(sql_string.append_fmt("CIPHER '%.*s' ", ssl_cipher.length(), ssl_cipher.ptr()))) {
        OB_LOG(WARN, "fail to append ssl info", K(ret));
      } else if (!x509_issuer.empty() && OB_FAIL(sql_string.append_fmt("ISSUER '%.*s' ", x509_issuer.length(), x509_issuer.ptr()))) {
        OB_LOG(WARN, "fail to append ssl info", K(ret));
      } else if (!x509_subject.empty() && OB_FAIL(sql_string.append_fmt("SUBJECT '%.*s' ", x509_subject.length(), x509_subject.ptr()))) {
        OB_LOG(WARN, "fail to append ssl info", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unknown ssl type", K(ssl_type), K(ret));
    }
  }
  return ret;
}


int ObDDLSqlGenerator::gen_set_passwd_sql(const ObAccountArg &account,
                                          const ObString &password,
                                          ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  char SET_PASSWD_SQL[] = "SET PASSWORD FOR `%.*s` = '%.*s'";
  char NEW_SET_PASSWD_SQL[] = "SET PASSWORD FOR `%.*s`@`%.*s` = '%.*s'";
  if (OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("username or password should not be null", K(account), K(password), K(ret));
  } else {
    if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(SET_PASSWD_SQL),
                                        account.user_name_.length(),
                                        account.user_name_.ptr(),
                                        password.length(),
                                        password.ptr()))) {
        LOG_WARN("append sql failed", K(account), K(password), K(ret));
      }
    } else {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(NEW_SET_PASSWD_SQL),
                                        account.user_name_.length(),
                                        account.user_name_.ptr(),
                                        account.host_name_.length(),
                                        account.host_name_.ptr(),
                                        password.length(),
                                        password.ptr()))) {
        LOG_WARN("append sql failed", K(account), K(password), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLSqlGenerator::gen_set_max_connections_sql(const ObAccountArg &account,
                                                  const uint64_t max_connections_per_hour,
                                                  const uint64_t max_user_connections,
                                                  ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  bool set_max_connections_per_hour = max_connections_per_hour != OB_INVALID_ID;
  bool set_max_user_connections = max_user_connections != OB_INVALID_ID;
  const ObString max_connections_per_hour_str = ObString::make_string("MAX_CONNECTIONS_PER_HOUR");
  const ObString max_user_connections_str = ObString::make_string("MAX_USER_CONNECTIONS");
  char SET_MAX_CONNECTIONS_SQL[] = "SET %.*s FOR `%.*s` = '%lu' ";
  char NEW_SET_MAX_CONNECTIONS_SQL[] = "SET %.*s FOR `%.*s`@`%.*s` = '%lu' ";
  if (OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("username should not be null", K(account), K(ret));
  } else {
    if (set_max_connections_per_hour) {
      if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
        if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(SET_MAX_CONNECTIONS_SQL),
                                          max_connections_per_hour_str.length(),
                                          max_connections_per_hour_str.ptr(),
                                          account.user_name_.length(),
                                          account.user_name_.ptr(),
                                          max_connections_per_hour))) {
          LOG_WARN("append sql failed", K(account), K(ret));
        }
      } else {
        if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(NEW_SET_MAX_CONNECTIONS_SQL),
                                          max_connections_per_hour_str.length(),
                                          max_connections_per_hour_str.ptr(),
                                          account.user_name_.length(),
                                          account.user_name_.ptr(),
                                          account.host_name_.length(),
                                          account.host_name_.ptr(),
                                          max_connections_per_hour))) {
          LOG_WARN("append sql failed", K(account), K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && set_max_user_connections) {
      if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
        if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(SET_MAX_CONNECTIONS_SQL),
                                          max_user_connections_str.length(),
                                          max_user_connections_str.ptr(),
                                          account.user_name_.length(),
                                          account.user_name_.ptr(),
                                          max_user_connections))) {
          LOG_WARN("append sql failed", K(account), K(ret));
        }
      } else {
        if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(NEW_SET_MAX_CONNECTIONS_SQL),
                                          max_user_connections_str.length(),
                                          max_user_connections_str.ptr(),
                                          account.user_name_.length(),
                                          account.user_name_.ptr(),
                                          account.host_name_.length(),
                                          account.host_name_.ptr(),
                                          max_user_connections))) {
          LOG_WARN("append sql failed", K(account), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLSqlGenerator::gen_alter_user_require_sql(const obrpc::ObAccountArg &account,
    const obrpc::ObSetPasswdArg &arg, common::ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  const share::schema::ObSSLType ssl_type = arg.ssl_type_;
  const common::ObString &ssl_cipher = arg.ssl_cipher_;
  const common::ObString &x509_issuer = arg.x509_issuer_;
  const common::ObString &x509_subject = arg.x509_subject_;
  static const char* const SET_SSL_SQL = "ALTER USER `%.*s`@`%.*s` ";
  if (OB_UNLIKELY(!account.is_valid()) || OB_UNLIKELY(ObSSLType::SSL_TYPE_NOT_SPECIFIED == ssl_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("username  or ssl_type is invalid", K(account),  K(ssl_type), K(ret));
  } else {
    if (OB_FAIL(sql_string.append_fmt(SET_SSL_SQL,
                                      account.user_name_.length(),
                                      account.user_name_.ptr(),
                                      account.host_name_.length(),
                                      account.host_name_.ptr()))) {
      LOG_WARN("append sql failed", K(account), K(ret));
    } else if (OB_FAIL(append_ssl_info_sql(ssl_type, ssl_cipher, x509_issuer, x509_subject, sql_string))) {
      LOG_WARN("append sql failed", K(ssl_type), K(ret));
    }
  }
  return ret;
}

int ObDDLSqlGenerator::gen_drop_user_sql(const ObAccountArg &account,
                                         ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  sql_string.reset();
  if (OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user_name should not be null", K(account));
  } else {
    if (account.is_role_) {
      static const char* const DROP_USER_SQL = "DROP ROLE \"%.*s\"";
      if (OB_FAIL(sql_string.append_fmt(DROP_USER_SQL,
              account.user_name_.length(),
              account.user_name_.ptr()))) {
        LOG_WARN("append sql failed", K(account), K(ret));
      }
    } else {
      char DROP_USER_SQL[] = "DROP USER `%.*s`";
      char NEW_DROP_USER_SQL[] = "DROP USER `%.*s`@`%.*s`";
      if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
        if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(DROP_USER_SQL),
                                          account.user_name_.length(),
                                          account.user_name_.ptr()))) {
          LOG_WARN("append sql failed", K(account), K(ret));
        }
      } else {
        if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(NEW_DROP_USER_SQL),
                                          account.user_name_.length(),
                                          account.user_name_.ptr(),
                                          account.host_name_.length(),
                                          account.host_name_.ptr()))) {
          LOG_WARN("append sql failed", K(account), K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && lib::is_oracle_mode() && !account.is_role_) {
      if (OB_FAIL(sql_string.append_fmt(" CASCADE"))) {
        LOG_WARN("append sql failed", K(ret), K(account));
      }
    }
  }
  return ret;
}

int ObDDLSqlGenerator::gen_lock_user_sql(const obrpc::ObAccountArg &account,
                                         const bool locked,
                                         ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  char LOCK_USER_SQL[] = "ALTER USER `%.*s` ACCOUNT LOCK";
  char UNLOCK_USER_SQL[] = "ALTER USER `%.*s` ACCOUNT UNLOCK";
  char NEW_LOCK_USER_SQL[] = "ALTER USER `%.*s`@`%.*s` ACCOUNT LOCK";
  char NEW_UNLOCK_USER_SQL[] = "ALTER USER `%.*s`@`%.*s` ACCOUNT UNLOCK";
  if (OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user_namae is empty", K(ret), K(account));
  } else {
    if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(locked ? LOCK_USER_SQL : UNLOCK_USER_SQL),
                                        account.user_name_.length(),
                                        account.user_name_.ptr()))) {
        LOG_WARN("append sql failed", K(account), K(ret), K(locked));
      }
    } else {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(locked ? NEW_LOCK_USER_SQL : NEW_UNLOCK_USER_SQL),
                                        account.user_name_.length(),
                                        account.user_name_.ptr(),
                                        account.host_name_.length(),
                                        account.host_name_.ptr()))) {
        LOG_WARN("append sql failed", K(account), K(ret), K(locked));
      }
    }
  }
  return ret;
}

int ObDDLSqlGenerator::gen_rename_user_sql(const ObAccountArg &old_account,
                                           const ObAccountArg &new_account,
                                           ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  char RENAME_USER_SQL[] = "RENAME USER `%.*s` to `%.*s`";
  char NEW_RENAME_USER_SQL[] = "RENAME USER `%.*s`@`%.*s` to `%.*s`@`%.*s`";
  if (OB_UNLIKELY(!old_account.is_valid()) || OB_UNLIKELY(!new_account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("old_account or new_account should not be null", K(old_account));
  } else {
    if (0 == old_account.host_name_.compare(OB_DEFAULT_HOST_NAME)
        && 0 == new_account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(RENAME_USER_SQL),
                                        old_account.user_name_.length(),
                                        old_account.user_name_.ptr(),
                                        new_account.user_name_.length(),
                                        new_account.user_name_.ptr()))) {
        LOG_WARN("append sql failed", K(old_account), K(new_account), K(ret));
      }
    } else {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(NEW_RENAME_USER_SQL),
                                        old_account.user_name_.length(),
                                        old_account.user_name_.ptr(),
                                        old_account.host_name_.length(),
                                        old_account.host_name_.ptr(),
                                        new_account.user_name_.length(),
                                        new_account.user_name_.ptr(),
                                        new_account.host_name_.length(),
                                        new_account.host_name_.ptr()))) {
        LOG_WARN("append sql failed", K(old_account), K(new_account), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLSqlGenerator::priv_to_name(const ObPrivSet priv, ObSqlString &priv_str)
{
  int ret = OB_SUCCESS;
  priv_str.reset();
  if (priv == 0) {
    //no privilege
    const char* priv_name = NULL;
    if (OB_FAIL(get_priv_name(priv, priv_name))) {
      LOG_WARN("get priv name failed", K(priv), K(ret));
    } else if (OB_ISNULL(priv_name)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("priv_name should not be null", K(ret), K(priv));
    } else if (OB_FAIL(priv_str.append(priv_name))) {
      LOG_WARN("append priv name failed", K(priv), K(priv_name), K(ret));
    }
  } else {
    for (int i = OB_PRIV_SHIFT::OB_PRIV_INVALID_SHIFT + 1;
        OB_SUCC(ret) && i < OB_PRIV_SHIFT::OB_PRIV_MAX_SHIFT_PLUS_ONE; ++i) {
      if (OB_PRIV_HAS_ANY(priv, OB_PRIV_GET_TYPE(i))) {
        const char* priv_name = NULL;
        if (OB_FAIL(get_priv_name(OB_PRIV_GET_TYPE(i), priv_name))) {
          LOG_WARN("get priv name failed", K(i), K(ret));
        } else if (OB_ISNULL(priv_name)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("priv_name should not be null", K(ret), K(i));
        } else {
          if (priv_str.empty()) {
            if (OB_FAIL(priv_str.append(priv_name))) {
              LOG_WARN("append priv name failed", K(ret), K(i));
            }
          } else if (OB_FAIL(priv_str.append_fmt(", %s", priv_name))) {
            LOG_WARN("append priv name failed", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

const char * ObDDLSqlGenerator::ora_obj_priv_names[] =
{
#define OB_OBJ_PRIV_TYPE_DEF(priv_id, priv_name) priv_name,
#include "share/schema/ob_obj_priv_type.h"
#undef OB_OBJ_PRIV_TYPE_DEF
};

int ObDDLSqlGenerator::raw_privs_to_name_ora(const share::ObRawObjPrivArray &obj_priv_array, 
                                             ObSqlString &priv_str)
{
  int ret = OB_SUCCESS;
  share::ObRawObjPriv priv_id;
  priv_str.reset();
  for (int i = 0; OB_SUCC(ret) && i < obj_priv_array.count(); i++) {
    priv_id = obj_priv_array.at(i);
    if (i > 0) {
      OZ (priv_str.append(", "));
    }
    OZ (priv_str.append(ora_obj_priv_names[priv_id]));
  }
  return ret;
}

int ObDDLSqlGenerator::gen_table_priv_sql(const obrpc::ObAccountArg &account,
                                          const ObNeedPriv &need_priv,
                                          const bool is_grant,
                                          ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  char GRANT_TABLE_SQL[] = "GRANT %s ON `%.*s`.`%.*s` TO `%.*s`";
  char REVOKE_TABLE_SQL[] = "REVOKE %s ON `%.*s`.`%.*s` FROM `%.*s`";
  char NEW_GRANT_TABLE_SQL[] = "GRANT %s ON `%.*s`.`%.*s` TO `%.*s`@`%.*s`";
  char NEW_REVOKE_TABLE_SQL[] = "REVOKE %s ON `%.*s`.`%.*s` FROM `%.*s`@`%.*s`";
  ObSqlString priv_string;
  if (OB_UNLIKELY(need_priv.db_.empty()) || OB_UNLIKELY(need_priv.table_.empty()) || OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db or table or user_name is empty", K(need_priv), K(account), K(ret));
  } else if (need_priv.priv_level_ != OB_PRIV_TABLE_LEVEL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("priv level is invalid", K(need_priv), K(ret));
  } else if (need_priv.priv_set_ & (~(OB_PRIV_TABLE_ACC | OB_PRIV_GRANT))) {
    ret = OB_ILLEGAL_GRANT_FOR_TABLE;
    LOG_WARN("Grant/Revoke privilege than can not be used",
              "priv_type", ObPrintPrivSet(need_priv.priv_set_), K(ret));
  } else if ((need_priv.priv_set_ & OB_PRIV_TABLE_ACC) == OB_PRIV_TABLE_ACC) {
    if (OB_FAIL(priv_string.append("ALL PRIVILEGES"))) {
      LOG_WARN("append sql failed", K(ret));
    } else if (!is_grant) {
      if ((need_priv.priv_set_ & OB_PRIV_GRANT)) {
        if (OB_FAIL(priv_string.append(", GRANT OPTION"))) {
          LOG_WARN("append sql failed", K(ret));
        }
      }
    }
  } else if (OB_FAIL(priv_to_name(need_priv.priv_set_, priv_string))) {
    LOG_WARN("get priv to name failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(is_grant ? GRANT_TABLE_SQL : REVOKE_TABLE_SQL),
                                        priv_string.string().ptr(),
                                        need_priv.db_.length(),
                                        need_priv.db_.ptr(),
                                        need_priv.table_.length(),
                                        need_priv.table_.ptr(),
                                        account.user_name_.length(),
                                        account.user_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    } else {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(is_grant ? NEW_GRANT_TABLE_SQL : NEW_REVOKE_TABLE_SQL),
                                        priv_string.string().ptr(),
                                        need_priv.db_.length(),
                                        need_priv.db_.ptr(),
                                        need_priv.table_.length(),
                                        need_priv.table_.ptr(),
                                        account.user_name_.length(),
                                        account.user_name_.ptr(),
                                        account.host_name_.length(),
                                        account.host_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && is_grant) {
    if (need_priv.priv_set_ & OB_PRIV_GRANT) {
      if (OB_FAIL(sql_string.append(" WITH GRANT OPTION"))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }
  LOG_DEBUG("gen table priv sql", K(sql_string.string()), K(priv_string.string()),
            K(need_priv), K(is_grant), K(account));
  return ret;
}

int ObDDLSqlGenerator::gen_column_priv_sql(const obrpc::ObAccountArg &account,
                                          const ObNeedPriv &need_priv,
                                          const bool is_grant,
                                          ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  sql_string.reset();
  char GRANT_COLUMN_SQL[] = "GRANT %s(%.*s) ON `%.*s`.`%.*s` TO `%.*s`";
  char REVOKE_COLUMN_SQL[] = "REVOKE %s(%.*s) ON `%.*s`.`%.*s` FROM `%.*s`";
  char NEW_GRANT_COLUMN_SQL[] = "GRANT %s(%.*s) ON `%.*s`.`%.*s` TO `%.*s`@`%.*s`";
  char NEW_REVOKE_COLUMN_SQL[] = "REVOKE %s(%.*s) ON `%.*s`.`%.*s` FROM `%.*s`@`%.*s`";
  ObSqlString priv_string;
  if (OB_UNLIKELY(need_priv.db_.empty()) || OB_UNLIKELY(need_priv.table_.empty()) || OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db or table or user_name is empty", K(need_priv), K(account), K(ret));
  } else if (need_priv.priv_level_ != OB_PRIV_TABLE_LEVEL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("priv level is invalid", K(need_priv), K(ret));
  } else if (need_priv.priv_set_ & (~(OB_PRIV_TABLE_ACC | OB_PRIV_GRANT))) {
    ret = OB_ILLEGAL_GRANT_FOR_TABLE;
    LOG_WARN("Grant/Revoke privilege than can not be used",
              "priv_type", ObPrintPrivSet(need_priv.priv_set_), K(ret));
  } else if ((need_priv.priv_set_ & OB_PRIV_TABLE_ACC) == OB_PRIV_TABLE_ACC) {
    if (OB_FAIL(priv_string.append("ALL PRIVILEGES"))) {
      LOG_WARN("append sql failed", K(ret));
    } else if (!is_grant) {
      if ((need_priv.priv_set_ & OB_PRIV_GRANT)) {
        if (OB_FAIL(priv_string.append(", GRANT OPTION"))) {
          LOG_WARN("append sql failed", K(ret));
        }
      }
    }
  } else if (OB_FAIL(priv_to_name(need_priv.priv_set_, priv_string))) {
    LOG_WARN("get priv to name failed", K(ret));
  }
  ObSqlString columns_string;
  for (int64_t i = 0; OB_SUCC(ret) && i < need_priv.columns_.count(); i++) {
    if (i != 0 && OB_FAIL(columns_string.append(","))) {
      LOG_WARN("append failed", K(ret));
    } else if (OB_FAIL(columns_string.append(need_priv.columns_.at(i)))) {
      LOG_WARN("append failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(is_grant ? GRANT_COLUMN_SQL : REVOKE_COLUMN_SQL),
                                        priv_string.string().ptr(),
                                        columns_string.string().length(),
                                        columns_string.string().ptr(),
                                        need_priv.db_.length(),
                                        need_priv.db_.ptr(),
                                        need_priv.table_.length(),
                                        need_priv.table_.ptr(),
                                        account.user_name_.length(),
                                        account.user_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    } else {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(is_grant ? NEW_GRANT_COLUMN_SQL : NEW_REVOKE_COLUMN_SQL),
                                        priv_string.string().ptr(),
                                        columns_string.string().length(),
                                        columns_string.string().ptr(),
                                        need_priv.db_.length(),
                                        need_priv.db_.ptr(),
                                        need_priv.table_.length(),
                                        need_priv.table_.ptr(),
                                        account.user_name_.length(),
                                        account.user_name_.ptr(),
                                        account.host_name_.length(),
                                        account.host_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && is_grant) {
    if (need_priv.priv_set_ & OB_PRIV_GRANT) {
      if (OB_FAIL(sql_string.append(" WITH GRANT OPTION"))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }
  LOG_DEBUG("gen table priv sql", K(sql_string.string()), K(priv_string.string()),
            K(need_priv), K(is_grant), K(account));
  return ret;
}

int ObDDLSqlGenerator::gen_table_priv_sql_ora(const obrpc::ObAccountArg &account,
                                              const ObTablePrivSortKey &table_priv_key,
                                              const bool revoke_all_flag,
                                              const share::ObRawObjPrivArray &obj_priv_array,
                                              const bool is_grant,
                                              ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  char GRANT_TABLE_SQL[] = "GRANT %s ON \"%.*s\".\"%.*s\" TO \"%.*s\"";
  char REVOKE_TABLE_SQL[] = "REVOKE %s ON \"%.*s\".\"%.*s\" FROM \"%.*s\"";
  char NEW_GRANT_TABLE_SQL[] = "GRANT %s ON \"%.*s\".\"%.*s\" TO \"%.*s\"@\"%.*s\"";
  char NEW_REVOKE_TABLE_SQL[] = "REVOKE %s ON \"%.*s\".\"%.*s\" FROM \"%.*s\"@\"%.*s\"";
  ObSqlString priv_string;
  if (OB_UNLIKELY(table_priv_key.db_.empty()) || OB_UNLIKELY(table_priv_key.table_.empty()) 
      || OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db or table or user_name is empty", K(table_priv_key), K(account), K(ret));
  } else if (true == revoke_all_flag) {
    if (OB_FAIL(priv_string.append("ALL PRIVILEGES"))) {
      LOG_WARN("append sql failed", K(ret));
    }
  } else if (OB_FAIL(raw_privs_to_name_ora(obj_priv_array, priv_string))) {
    LOG_WARN("get priv to name failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(is_grant ? GRANT_TABLE_SQL : 
                                                                         REVOKE_TABLE_SQL),
                                        priv_string.string().ptr(),
                                        table_priv_key.db_.length(),
                                        table_priv_key.db_.ptr(),
                                        table_priv_key.table_.length(),
                                        table_priv_key.table_.ptr(),
                                        account.user_name_.length(),
                                        account.user_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    } else {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(is_grant ? NEW_GRANT_TABLE_SQL : 
                                                                         NEW_REVOKE_TABLE_SQL),
                                        priv_string.string().ptr(),
                                        table_priv_key.db_.length(),
                                        table_priv_key.db_.ptr(),
                                        table_priv_key.table_.length(),
                                        table_priv_key.table_.ptr(),
                                        account.user_name_.length(),
                                        account.user_name_.ptr(),
                                        account.host_name_.length(),
                                        account.host_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }

  LOG_DEBUG("gen table priv sql", K(sql_string.string()), K(priv_string.string()),
            K(revoke_all_flag), K(obj_priv_array), K(account));
  return ret;
}

int ObDDLSqlGenerator::gen_routine_priv_sql(const obrpc::ObAccountArg &account,
                                          const ObNeedPriv &need_priv,
                                          const bool is_grant,
                                          ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  char GRANT_PROCEDURE_SQL[] = "GRANT %s ON PROCEDURE `%.*s`.`%.*s` TO `%.*s`";
  char REVOKE_PROCEDURE_SQL[] = "REVOKE %s ON PROCEDURE `%.*s`.`%.*s` FROM `%.*s`";
  char GRANT_FUNCTION_SQL[] = "GRANT %s ON FUNCTION `%.*s`.`%.*s` TO `%.*s`";
  char REVOKE_FUNCTION_SQL[] = "REVOKE %s ON FUNCTION `%.*s`.`%.*s` FROM `%.*s`";
  char NEW_GRANT_PROCEDURE_SQL[] = "GRANT %s ON PROCEDURE `%.*s`.`%.*s` TO `%.*s`@`%.*s`";
  char NEW_REVOKE_PROCEDURE_SQL[] = "REVOKE %s ON PROCEDURE `%.*s`.`%.*s` FROM `%.*s`@`%.*s`";
  char NEW_GRANT_FUNCTION_SQL[] = "GRANT %s ON FUNCTION `%.*s`.`%.*s` TO `%.*s`@`%.*s`";
  char NEW_REVOKE_FUNCTION_SQL[] = "REVOKE %s ON FUNCTION `%.*s`.`%.*s` FROM `%.*s`@`%.*s`";
  ObSqlString priv_string;
  if (OB_UNLIKELY(need_priv.db_.empty()) || OB_UNLIKELY(need_priv.table_.empty()) || OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db or table or user_name is empty", K(need_priv), K(account), K(ret));
  } else if (need_priv.priv_level_ != OB_PRIV_ROUTINE_LEVEL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("priv level is invalid", K(need_priv), K(ret));
  } else if (need_priv.priv_set_ & (~(OB_PRIV_ROUTINE_ACC | OB_PRIV_GRANT))) {
    ret = OB_ILLEGAL_GRANT_FOR_TABLE;
    LOG_WARN("Grant/Revoke privilege than can not be used",
              "priv_type", ObPrintPrivSet(need_priv.priv_set_), K(ret));
  } else if (OB_FAIL(priv_to_name(need_priv.priv_set_, priv_string))) {
    LOG_WARN("get priv to name failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(is_grant ? (need_priv.obj_type_ == ObObjectType::PROCEDURE ? GRANT_PROCEDURE_SQL : GRANT_FUNCTION_SQL)
                                                                       : (need_priv.obj_type_ == ObObjectType::PROCEDURE ? REVOKE_PROCEDURE_SQL : REVOKE_FUNCTION_SQL)),
                                        priv_string.string().ptr(),
                                        need_priv.db_.length(),
                                        need_priv.db_.ptr(),
                                        need_priv.table_.length(),
                                        need_priv.table_.ptr(),
                                        account.user_name_.length(),
                                        account.user_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    } else {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(is_grant ? (need_priv.obj_type_ == ObObjectType::PROCEDURE ? NEW_GRANT_PROCEDURE_SQL : NEW_GRANT_FUNCTION_SQL)
                                                                       : (need_priv.obj_type_ == ObObjectType::PROCEDURE ? NEW_REVOKE_PROCEDURE_SQL : NEW_REVOKE_FUNCTION_SQL)),
                                        priv_string.string().ptr(),
                                        need_priv.db_.length(),
                                        need_priv.db_.ptr(),
                                        need_priv.table_.length(),
                                        need_priv.table_.ptr(),
                                        account.user_name_.length(),
                                        account.user_name_.ptr(),
                                        account.host_name_.length(),
                                        account.host_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && is_grant) {
    if (need_priv.priv_set_ & OB_PRIV_GRANT) {
      if (OB_FAIL(sql_string.append(" WITH GRANT OPTION"))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }
  LOG_DEBUG("gen routine priv sql", K(sql_string.string()), K(priv_string.string()),
            K(need_priv), K(is_grant), K(account));
  return ret;
}

int ObDDLSqlGenerator::gen_db_priv_sql(const obrpc::ObAccountArg &account,
                                       const ObNeedPriv &need_priv,
                                       const bool is_grant,
                                       ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  char GRANT_DB_SQL[] = "GRANT %s ON `%.*s`.* TO `%.*s`";
  char REVOKE_DB_SQL[] = "REVOKE %s ON `%.*s`.* FROM `%.*s`";
  char NEW_GRANT_DB_SQL[] = "GRANT %s ON `%.*s`.* TO `%.*s`@`%.*s`";
  char NEW_REVOKE_DB_SQL[] = "REVOKE %s ON `%.*s`.* FROM `%.*s`@`%.*s`";
  ObSqlString priv_string;
  if (OB_UNLIKELY(need_priv.db_.empty()) || OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db or user_name is empty", K(ret), K(need_priv), K(account));
  } else if (need_priv.priv_level_ != OB_PRIV_DB_LEVEL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("priv level is invalid", K(need_priv), K(ret));
  } else if (need_priv.priv_set_ & (~(OB_PRIV_DB_ACC | OB_PRIV_GRANT))) {
    ret = OB_ILLEGAL_GRANT_FOR_TABLE;
    LOG_WARN("Grant/Revoke privilege than can not be used",
              "priv_type", ObPrintPrivSet(need_priv.priv_set_), K(ret));
  } else if ((need_priv.priv_set_ & OB_PRIV_DB_ACC) == OB_PRIV_DB_ACC) {
    if (OB_FAIL(priv_string.append("ALL PRIVILEGES"))) {
      LOG_WARN("append sql failed", K(ret));
    } else if (!is_grant) {
      if ((need_priv.priv_set_ & OB_PRIV_GRANT)) {
        if (OB_FAIL(priv_string.append(", GRANT OPTION"))) {
          LOG_WARN("append sql failed", K(ret));
        }
      }
    }
  } else if (OB_FAIL(priv_to_name(need_priv.priv_set_, priv_string))) {
    LOG_WARN("get priv to name failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(is_grant ? GRANT_DB_SQL : REVOKE_DB_SQL),
                                        priv_string.string().ptr(),
                                        need_priv.db_.length(),
                                        need_priv.db_.ptr(),
                                        account.user_name_.length(),
                                        account.user_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    } else {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(is_grant ? NEW_GRANT_DB_SQL : NEW_REVOKE_DB_SQL),
                                        priv_string.string().ptr(),
                                        need_priv.db_.length(),
                                        need_priv.db_.ptr(),
                                        account.user_name_.length(),
                                        account.user_name_.ptr(),
                                        account.host_name_.length(),
                                        account.host_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && is_grant) {
    if (need_priv.priv_set_ & OB_PRIV_GRANT) {
      if (OB_FAIL(sql_string.append(" WITH GRANT OPTION"))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }
  LOG_INFO("mingyin gen db priv sql", K(sql_string.string()), K(is_grant), K(need_priv));
  return ret;
}

int ObDDLSqlGenerator::gen_revoke_all_sql(const obrpc::ObAccountArg &account,
                                          ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  char REVOKE_ALL_SQL[] = "REVOKE ALL PRIVILEGES, GRANT OPTION FROM `%.*s`";
  char NEW_REVOKE_ALL_SQL[] = "REVOKE ALL PRIVILEGES, GRANT OPTION FROM `%.*s`@`%.*s`";
  if (OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("account is empty", K(ret), K(account));
  } else {
    if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(REVOKE_ALL_SQL),
                                        account.user_name_.length(),
                                        account.user_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    } else {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(NEW_REVOKE_ALL_SQL),
                                        account.user_name_.length(),
                                        account.user_name_.ptr(),
                                        account.host_name_.length(),
                                        account.host_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
    LOG_DEBUG("gen revoke sql finished", K(account),
              K(sql_string.string()), K(ret));
  }
  return ret;
}

int ObDDLSqlGenerator::gen_user_priv_sql(const obrpc::ObAccountArg &account,
                                         const ObNeedPriv &need_priv,
                                         const bool is_grant,
                                         ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  char GRANT_USER_SQL[] = "GRANT %s ON *.* TO `%.*s`";
  char REVOKE_USER_SQL[] = "REVOKE %s ON *.* FROM `%.*s`";
  char NEW_GRANT_USER_SQL[] = "GRANT %s ON *.* TO `%.*s`@`%.*s`";
  char NEW_REVOKE_USER_SQL[] = "REVOKE %s ON *.* FROM `%.*s`@`%.*s`";
  ObSqlString priv_string;
  if (OB_UNLIKELY(!account.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("account is empty", K(ret), K(account));
  } else if (need_priv.priv_level_ != OB_PRIV_USER_LEVEL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("priv level is invalid", K(need_priv), K(ret));
  } else if (need_priv.priv_set_ & OB_PRIV_BOOTSTRAP) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bootstrap priv is not allowed to grant", K(ret));
  } else if ((need_priv.priv_set_ & OB_PRIV_ALL) == OB_PRIV_ALL) {//super set of OB_PRIV_ALL
    if (OB_FAIL(priv_string.append("ALL PRIVILEGES"))) {
      LOG_WARN("append sql failed", K(ret));
//    } else if (!is_grant) {//revoke
      //revoke all privilege, grant option on *.* from xxx;
//      if (need_priv.priv_set_ & OB_PRIV_GRANT) {
//        if (OB_FAIL(priv_string.append(", GRANT OPTION"))) {
//          LOG_WARN("append sql failed", K(ret));
//        }
//      }
    }
  } else if (OB_FAIL(priv_to_name(need_priv.priv_set_, priv_string))) {
    LOG_WARN("get priv to name failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (0 == account.host_name_.compare(OB_DEFAULT_HOST_NAME)) {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(is_grant ? GRANT_USER_SQL : REVOKE_USER_SQL),
                                        priv_string.string().ptr(),
                                        account.user_name_.length(),
                                        account.user_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    } else {
      if (OB_FAIL(sql_string.append_fmt(adjust_ddl_format_str(is_grant ? NEW_GRANT_USER_SQL : NEW_REVOKE_USER_SQL),
                                        priv_string.string().ptr(),
                                        account.user_name_.length(),
                                        account.user_name_.ptr(),
                                        account.host_name_.length(),
                                        account.host_name_.ptr()))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && is_grant) {
    if (need_priv.priv_set_ & OB_PRIV_GRANT) {
      //grant all on xx.* to user with grant option
      if (OB_FAIL(sql_string.append(" WITH GRANT OPTION"))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }
  LOG_DEBUG("gen user priv sql", K(sql_string.string()),
            K(priv_string.string()), K(need_priv), K(is_grant), K(ret));
  return ret;
}

int ObDDLSqlGenerator::gen_audit_stmt_sql(const ObString &username,
                                          const ObSAuditModifyType modify_type,
                                          const ObSAuditSchema &audit_schema,
                                          const ObSAuditOperByType by_type,
                                          const ObSAuditOperWhenType when_type,
                                          ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_string.append_fmt("%s %s ", (AUDIT_MT_ADD == modify_type ? "AUDIT" : "NOAUDIT"),
                     get_audit_operation_type_str(audit_schema.get_operation_type())))) {
    LOG_WARN("append sql failed", K(modify_type), K(ret));
  } else if ((AUDIT_STMT == audit_schema.get_audit_type())
             && OB_FAIL(sql_string.append_fmt("BY \"%.*s\" ", username.length(), username.ptr()))) {
    LOG_WARN("append sql failed", K(username), K(ret));
  } else if (AUDIT_MT_ADD == modify_type
             && OB_FAIL(sql_string.append(AUDIT_BY_SESSION == by_type ? "BY SESSION " : "BY ACCESS "))) {
    LOG_WARN("append sql failed", K(by_type), K(ret));
  } else if (AUDIT_WHEN_NOT_SET != when_type
             && OB_FAIL(sql_string.append_fmt("WHENEVER %sSUCCESSFUL ",
                                              AUDIT_WHEN_FAILURE == when_type ? "NOT " : ""))) {
    LOG_WARN("append sql failed", K(when_type), K(ret));
  }
  return ret;
}

char *ObDDLSqlGenerator::adjust_ddl_format_str(char *ori_format_str)
{
  if (OB_ISNULL(ori_format_str)) {
    //do nothing
  } else if (lib::is_oracle_mode()) {
    for (int i = 0; i < strlen(ori_format_str); ++i) {
      if (*(ori_format_str + i) == '`') {
        *(ori_format_str + i) = '"';
      }
    }
  } else {
    //do nothing
  }
  return ori_format_str;
}

int ObDDLSqlGenerator::gen_audit_object_sql(const common::ObString &schema_name,
                                            const common::ObString &object_name,
                                            const ObSAuditModifyType modify_type,
                                            const ObSAuditSchema &audit_schema,
                                            const ObSAuditOperByType by_type,
                                            const ObSAuditOperWhenType when_type,
                                            ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_string.append_fmt("%s %s ",
                     (AUDIT_MT_ADD == modify_type ? "AUDIT" : "NOAUDIT"),
                     get_audit_operation_type_str(audit_schema.get_operation_type())))) {
    LOG_WARN("append sql failed", K(modify_type), K(ret));
  } else if (AUDIT_OBJ_DEFAULT == audit_schema.get_audit_type()) {
    if (OB_FAIL(sql_string.append_fmt("ON DEFAULT "))) {
      LOG_WARN("append sql failed", K(ret));
    }
  } else {
    if (OB_FAIL(sql_string.append_fmt("ON \"%.*s\".\"%.*s\" ",
                                      schema_name.length(), schema_name.ptr(),
                                      object_name.length(), object_name.ptr()))) {
      LOG_WARN("append sql failed", K(schema_name), K(object_name), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (AUDIT_MT_ADD == modify_type
             && OB_FAIL(sql_string.append(AUDIT_BY_SESSION == by_type ? "BY SESSION " : "BY ACCESS "))) {
    LOG_WARN("append sql failed", K(by_type), K(ret));
  } else if (AUDIT_WHEN_NOT_SET != when_type
             && OB_FAIL(sql_string.append_fmt("WHENEVER %sSUCCESSFUL ",
                                              AUDIT_WHEN_FAILURE == when_type ? "NOT " : ""))) {
    LOG_WARN("append sql failed", K(when_type), K(ret));
  }
  return ret;
}
} //end of namespace rootserver
} //end of namespace oceanbase
