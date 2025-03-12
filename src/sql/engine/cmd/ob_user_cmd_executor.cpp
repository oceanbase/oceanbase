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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_user_cmd_executor.h"

#include "lib/encrypt/ob_encrypted_helper.h"
#include "sql/resolver/dcl/ob_create_user_stmt.h"
#include "sql/resolver/dcl/ob_drop_user_stmt.h"
#include "sql/resolver/dcl/ob_lock_user_stmt.h"
#include "sql/resolver/dcl/ob_rename_user_stmt.h"
#include "sql/resolver/dcl/ob_alter_user_profile_stmt.h"
#include "sql/resolver/dcl/ob_alter_user_proxy_stmt.h"
#include "sql/resolver/dcl/ob_alter_user_primary_zone_stmt.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{
int ObCreateUserExecutor::encrypt_passwd(const common::ObString& pwd,
                                         common::ObString& encrypted_pwd,
                                         char *enc_buf,
                                         int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(enc_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("enc_buf is NULL", K(ret));
  } else if (buf_len < ENC_BUF_LEN) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("Encrypt buf not enough");
  } else {
    encrypted_pwd.assign_ptr(enc_buf, ENC_STRING_BUF_LEN);
    if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage2(pwd, encrypted_pwd))) {
      SQL_ENG_LOG(WARN, "failed to encrypt passwd", K(ret));
    }
  }
  return ret;
}

int ObCreateUserExecutor::check_user_valid(ObSchemaGetterGuard& schema_guard,
                                           uint64_t priv_set,
                                           int64_t tenant_id,
                                           const ObString &user_name,
                                           const ObString &host_name,
                                           const ObString &opreation_name)
{
  int ret = OB_SUCCESS;
  ObSqlString full_user_name;
  bool existed = false;
  if (!ObSchemaChecker::enable_mysql_pl_priv_check(tenant_id, schema_guard)) {
  } else if (OB_FAIL(full_user_name.append_fmt("%.*s@%.*s", user_name.length(), user_name.ptr(),
                                                host_name.length(), host_name.ptr()))) {
    LOG_WARN("append fmt failed", K(ret));
  } else if (OB_FAIL(schema_guard.check_routine_definer_existed(tenant_id, full_user_name.string(), existed))) {
    LOG_WARN("check routine definer existed failed", K(ret));
  } else if (existed) {
    if ((priv_set & OB_PRIV_SUPER) != 0) {
      LOG_USER_WARN(OB_ERR_USER_REFFERD_AS_DEFINER, user_name.length(), user_name.ptr(), host_name.length(), host_name.ptr());
    } else {
      ret = OB_ERR_OPERATION_ON_USER_REFERRED_AS_DEFINER;
      LOG_WARN("create user has definer", K(ret));
      LOG_USER_ERROR(OB_ERR_OPERATION_ON_USER_REFERRED_AS_DEFINER, opreation_name.length(), opreation_name.ptr(),
                        user_name.length(), user_name.ptr(), host_name.length(), host_name.ptr());
    }
  }
  LOG_DEBUG("check user valid status", K(priv_set), K(ret));
  return ret;
}

int ObCreateUserExecutor::userinfo_extract_user_name(
      const common::ObIArray<share::schema::ObUserInfo> &user_infos,
      const common::ObIArray<int64_t> &index,
      common::ObIArray<common::ObString> &users,
      common::ObIArray<common::ObString> &hosts)
{
  int ret = OB_SUCCESS;
  users.reset();
  hosts.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < index.count(); ++i) {
    int64_t in = index.at(i);
    if (OB_UNLIKELY(in < 0) || OB_UNLIKELY(in >= user_infos.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Userinfo index out of range", K(user_infos), K(index), K(in));
    } else if (OB_FAIL(users.push_back(user_infos.at(in).get_user_name_str()))) {
      LOG_WARN("Failed to add username", K(user_infos), K(index));
    } else if (OB_FAIL(hosts.push_back(user_infos.at(in).get_host_name_str()))) {
      LOG_WARN("Failed to add username", K(user_infos), K(index));
    }
  }
  return ret;
}

int ObCreateUserExecutor::execute(ObExecContext &ctx, ObCreateUserStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const uint64_t tenant_id = stmt.get_tenant_id();
  const ObStrings &users = stmt.get_users();
  const bool if_not_exist = stmt.get_if_not_exists();
  const int64_t FIX_MEMBER_CNT = 4;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else if (OB_UNLIKELY(users.count() <= FIX_MEMBER_CNT) || OB_UNLIKELY(0 != users.count() % FIX_MEMBER_CNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Resolve create user error. Users should have user and pwd", "ObStrings count", users.count());
  } else {
    ObString user_name;
    ObString host_name;
    ObString pwd;
    ObString need_enc;
    ObString ssl_type;
    ObString ssl_cipher;
    ObString x509_issuer;
    ObString x509_subject;
    ObSSLType ssl_type_enum = ObSSLType::SSL_TYPE_NOT_SPECIFIED;
    ObCreateUserArg &arg = static_cast<ObCreateUserArg &>(stmt.get_ddl_arg());
    arg.tenant_id_ = tenant_id;
    arg.user_infos_.reset();
    arg.if_not_exist_ = if_not_exist;
    const int64_t users_cnt = users.count() - FIX_MEMBER_CNT;

    if (OB_FAIL(users.get_string(users_cnt, ssl_type))) {
      LOG_WARN("Get string from ObStrings error", K(ret));
    } else if (OB_FAIL(users.get_string(users_cnt + 1, ssl_cipher))) {
      LOG_WARN("Get string from ObStrings error", K(ret));
    } else if (OB_FAIL(users.get_string(users_cnt + 2, x509_issuer))) {
      LOG_WARN("Get string from ObStrings error", K(ret));
    } else if (OB_FAIL(users.get_string(users_cnt + 3, x509_subject))) {
      LOG_WARN("Get string from ObStrings error", K(ret));
    } else if (OB_UNLIKELY(ObSSLType::SSL_TYPE_MAX == (ssl_type_enum = get_ssl_type_from_string(ssl_type)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("known ssl_type", K(ssl_type), K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < users_cnt; i += FIX_MEMBER_CNT) {
      if (OB_FAIL(users.get_string(i, user_name))) {
        LOG_WARN("Get string from ObStrings error",
            "count", users.count(), K(i), K(ret));
      } else if (OB_FAIL(users.get_string(i + 1, host_name))) {
        LOG_WARN("Get string from ObStrings error",
            "count", users.count(), K(i), K(ret));
      } else if (OB_FAIL(users.get_string(i + 2, pwd))) {
        LOG_WARN("Get string from ObStrings error",
            "count", users.count(), K(i), K(ret));
      } else if (OB_FAIL(users.get_string(i + 3, need_enc))) {
        LOG_WARN("Get string from ObStrings error",
            "count", users.count(), K(i), K(ret));
      } else {
        ObUserInfo user_info;
        if (ObString::make_string("YES") == need_enc) {
          if (pwd.length() > 0) {
            ObString pwd_enc;
            char enc_buf[ENC_BUF_LEN] = {0};
            if (OB_FAIL(encrypt_passwd(pwd, pwd_enc, enc_buf, ENC_BUF_LEN))) {
              LOG_WARN("Encrypt password failed", K(ret));
            } else if (OB_FAIL(user_info.set_passwd(pwd_enc))) {
              LOG_WARN("set password failed", K(ret));
            }
          }
        } else {
          if (OB_FAIL(user_info.set_passwd(pwd))) {
            LOG_WARN("Failed to set password", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          ObSchemaGetterGuard schema_guard;
          if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
            LOG_WARN("get tenant schema guard failed", K(ret));
          } else if (OB_FAIL(ObCreateUserExecutor::check_user_valid(schema_guard, ctx.get_my_session()->get_user_priv_set(), tenant_id,
                                                                    user_name, host_name, "CREATE USER"))) {
            LOG_WARN("check user valid failed", K(ret));
          } else if (OB_FAIL(user_info.set_user_name(user_name))) {
            LOG_WARN("set user name failed", K(ret));
          } else if (OB_FAIL(user_info.set_host(host_name))) {
            LOG_WARN("set host name failed", K(ret));
          } else if (FALSE_IT(user_info.set_ssl_type(ssl_type_enum))) {
            LOG_WARN("set ssl_type failed", K(ret));
          } else if (OB_FAIL(user_info.set_ssl_cipher(ssl_cipher))) {
            LOG_WARN("set ssl_cipher failed", K(ret));
          } else if (OB_FAIL(user_info.set_x509_issuer(x509_issuer))) {
            LOG_WARN("set x509_issuer failed", K(ret));
          } else if (OB_FAIL(user_info.set_x509_subject(x509_subject))) {
            LOG_WARN("set x509_subject failed", K(ret));
          } else if (FALSE_IT(user_info.set_password_last_changed(ObTimeUtility::current_time()))) {
            LOG_WARN("set set_password_last_changed failed", K(ret));
          } else {
            user_info.set_tenant_id(tenant_id);
            if (user_name.empty()) {
              user_info.set_user_id(OB_EMPTY_USER_ID);
            }
            user_info.set_profile_id(stmt.get_profile_id());
            user_info.set_max_connections(stmt.get_max_connections_per_hour());
            user_info.set_max_user_connections(stmt.get_max_user_connections());
            if (OB_FAIL(arg.user_infos_.push_back(user_info))) {
              LOG_WARN("Add user info to array error", K(ret));
            } else {
              LOG_DEBUG("Add user info to array", K(user_info));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(create_user(common_rpc_proxy, arg))) {
        LOG_WARN("Create user rpc failed", K(ret));
      }
    }
  }
  return ret;
}

int ObCreateUserExecutor::create_user(obrpc::ObCommonRpcProxy *rpc_proxy,
                                      const obrpc::ObCreateUserArg& arg) const
{
  int ret = OB_SUCCESS;
  ObSArray<int64_t> failed_index;
  ObSqlString fail_msg;
  if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input argument error", K(rpc_proxy), K(ret));
  } else if (OB_FAIL(rpc_proxy->create_user(arg, failed_index))) {
    LOG_WARN("Create user error", K(ret));
  } else if (0 != failed_index.count()) {
    ObSArray<ObString> failed_users;
    ObSArray<ObString> failed_hosts;
    if (OB_FAIL(userinfo_extract_user_name(arg.user_infos_, failed_index, failed_users, failed_hosts))) {
      LOG_WARN("Failed to extract user name", K(arg.user_infos_), K(failed_index), K(ret));
    } else if (OB_FAIL(ObDropUserExecutor::build_fail_msg(failed_users, failed_hosts, fail_msg))) {
      LOG_WARN("Build fail msg error", K(ret));
    } else {
      ret = OB_CANNOT_USER;
      LOG_USER_ERROR(OB_CANNOT_USER, (int)strlen("CREATE USER"), "CREATE USER",
          (int)fail_msg.length(), fail_msg.ptr());
    }
  } else {
    //Create user completely success
  }
  return ret;
}

int ObDropUserExecutor::build_fail_msg_for_one(const ObString &user, const ObString &host,
                                               common::ObSqlString &msg) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(msg.append_fmt("'%.*s'@'%.*s'",
                                    user.length(), user.ptr(),
                                    host.length(), host.ptr()))) {
    LOG_WARN("Build msg fail", K(user), K(host), K(ret));
  }
  return ret;
}

int ObDropUserExecutor::build_fail_msg(const common::ObIArray<common::ObString> &users,
    const common::ObIArray<common::ObString> &hosts, common::ObSqlString &msg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(users.count() < 1) || OB_UNLIKELY(users.count() != hosts.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(users.count()), K(hosts.count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < users.count(); ++i) {
      if (0 != i && OB_FAIL(msg.append_fmt(","))) {
        LOG_WARN("Build msg fail", K(ret));
      }
      if (OB_SUCC(ret)) {
        const ObString &user = users.at(i);
        const ObString &host = hosts.at(i);
        if (OB_FAIL(build_fail_msg_for_one(user, host, msg))) {
          LOG_WARN("Build fail msg fail", K(user), K(host), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDropUserExecutor::string_array_index_extract(const common::ObIArray<common::ObString> &src_users,
    const common::ObIArray<common::ObString> &src_hosts, const common::ObIArray<int64_t> &index,
    common::ObIArray<common::ObString> &dst_users, common::ObIArray<common::ObString> &dst_hosts)
{
  int ret = OB_SUCCESS;
  dst_users.reset();
  dst_hosts.reset();
  int64_t in = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < index.count(); ++i) {
    in = index.at(i);
    if (in >= src_users.count() || in < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("String index out of range", K(ret), K(in), K(src_users.count()));
    } else if (OB_FAIL(dst_users.push_back(src_users.at(in)))) {
      LOG_WARN("Failed to push back user", K(ret));
    } else if (OB_FAIL(dst_hosts.push_back(src_hosts.at(in)))) {
      LOG_WARN("Failed to push back host", K(ret));
      //do nothing
    }
  }
  return ret;
}

int ObDropUserExecutor::execute(ObExecContext &ctx, ObDropUserStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const uint64_t tenant_id = stmt.get_tenant_id();
  const ObStrings *user_names = NULL;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", K(ret));
  } else if (OB_ISNULL(user_names = stmt.get_users())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("names is NULL", K(ret));
  } else if (OB_UNLIKELY(user_names->count() % 2 != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user not specified", K(ret));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (NULL == (common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    ObString user_name;
    ObString host_name;
    ObDropUserArg &arg = static_cast<ObDropUserArg &>(stmt.get_ddl_arg());
    arg.tenant_id_ = tenant_id;
    {
      ObSchemaGetterGuard schema_guard;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < user_names->count(); i += 2) {
        if (OB_FAIL(user_names->get_string(i, user_name))) {
          LOG_WARN("Get user name failed", K(ret));
        } else if (OB_FAIL(user_names->get_string(i + 1, host_name))) {
          LOG_WARN("Get host name failed", K(ret));
        } else if (OB_FAIL(arg.users_.push_back(user_name))) {
          LOG_WARN("Add user name failed", K(ret));
        } else if (OB_FAIL(arg.hosts_.push_back(host_name))) {
          LOG_WARN("Add host name failed", K(ret));
        } else if (OB_FAIL(ObCreateUserExecutor::check_user_valid(schema_guard, ctx.get_my_session()->get_user_priv_set(), tenant_id,
                                                                  user_name, host_name, "DROP USER"))) {
          LOG_WARN("check user valid failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(drop_user(common_rpc_proxy, arg, stmt.get_if_exists()))) {
        LOG_WARN("Drop user completely failed", K(ret));
      } else {
        //do nothing
      }
    }
  }
  return ret;
}

int ObDropUserExecutor::drop_user(obrpc::ObCommonRpcProxy *rpc_proxy,
                                  const obrpc::ObDropUserArg &arg,
                                  bool if_exist_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", K(arg), K(ret));
  } else if (OB_UNLIKELY(arg.users_.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user not specified", K(ret));
  } else if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rpc_proxy is null", K(ret));
  } else {
    ObSArray<int64_t> failed_index;
    ObSqlString fail_msg;
    if (OB_FAIL(rpc_proxy->drop_user(arg, failed_index))) {
      LOG_WARN("Lock user failed", K(ret));
    }
    if (0 != failed_index.count()) {
      ObSArray<ObString> failed_users;
      ObSArray<ObString> failed_hosts;
      if (OB_FAIL(ObDropUserExecutor::string_array_index_extract(arg.users_, arg.hosts_,
                                                                 failed_index, failed_users,
                                                                 failed_hosts))) {
        LOG_WARN("Failed to extract user name", K(arg), K(ret));
      } else if (if_exist_stmt) {
        if (OB_UNLIKELY(failed_users.count() < 1) || OB_UNLIKELY(failed_users.count() != failed_users.count())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(failed_users.count()), K(failed_users.count()), K(ret));
        } else {
          for (int i = 0; OB_SUCC(ret) && i < failed_users.count(); ++i) {
            ObSqlString fail_msg_one;
            if (OB_FAIL(ObDropUserExecutor::build_fail_msg_for_one(failed_users.at(i), failed_hosts.at(i), fail_msg_one))) {
              LOG_WARN("Build fail msg error", K(arg), K(ret));
            } else {
              LOG_USER_WARN(OB_CANNOT_USER_IF_EXISTS, (int)fail_msg_one.length(), fail_msg_one.ptr());
            }
          }
        }
      } else if (!if_exist_stmt) {
        if (OB_FAIL(ObDropUserExecutor::build_fail_msg(failed_users, failed_hosts, fail_msg))) {
          LOG_WARN("Build fail msg error", K(arg), K(ret));
        } else {
          const char *ERR_CMD = (arg.is_role_ && lib::is_mysql_mode()) ? "DROP ROLE" : "DROP USER";
          ret = OB_CANNOT_USER;
          LOG_USER_ERROR(OB_CANNOT_USER, (int)strlen(ERR_CMD), ERR_CMD, (int)fail_msg.length(), fail_msg.ptr());
        }
      }
    }
  }
  return ret;
}

int ObLockUserExecutor::execute(ObExecContext &ctx, ObLockUserStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const uint64_t tenant_id = stmt.get_tenant_id();
  const ObStrings *user_names = NULL;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", K(ret));
  } else if (OB_ISNULL(user_names = stmt.get_users())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("names is NULL", K(ret));
  } else if (OB_UNLIKELY(user_names->count() % 2 != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user not specified", K(ret));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    ObString user_name;
    ObString host_name;
    ObLockUserArg &arg = static_cast<ObLockUserArg &>(stmt.get_ddl_arg());
    arg.tenant_id_ = tenant_id;
    arg.locked_ = stmt.is_locked();
    for (int64_t i = 0; OB_SUCC(ret) && i < user_names->count(); i += 2) {
      if (OB_FAIL(user_names->get_string(i, user_name))) {
        LOG_WARN("Get user name failed", K(ret));
      } else if (OB_FAIL(user_names->get_string(i + 1, host_name))) {
        LOG_WARN("Get host name failed", K(ret));
      } else if (OB_FAIL(arg.users_.push_back(user_name))) {
        LOG_WARN("Add user name failed", K(ret));
      } else if (OB_FAIL(arg.hosts_.push_back(host_name))) {
        LOG_WARN("Add host name failed", K(ret));
      } else {
        //do nothing
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(lock_user(common_rpc_proxy, arg))) {
        LOG_WARN("Rename user completely failed", K(arg), K(ret));
      } else {
        //do nothing
      }
    }
  }
  return ret;
}

int ObLockUserExecutor::lock_user(obrpc::ObCommonRpcProxy *rpc_proxy,
                                  const obrpc::ObLockUserArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", K(arg), K(ret));
  } else if (OB_UNLIKELY(arg.users_.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user not specified", K(ret));
  } else if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rpc_proxy is null", K(ret));
  } else {
    ObSArray<int64_t> failed_index;
    ObSqlString fail_msg;
    if (OB_FAIL(rpc_proxy->lock_user(arg, failed_index))) {
      LOG_WARN("Lock user failed", K(ret));
      if (OB_FAIL(ObDropUserExecutor::build_fail_msg(arg.users_, arg.hosts_, fail_msg))) {
        LOG_WARN("Build fail msg error", K(arg), K(ret));
      } else {
        ret = OB_CANNOT_USER;
        LOG_USER_ERROR(OB_CANNOT_USER, (int)strlen("LOCK USER"), "LOCK USER", (int)fail_msg.length(), fail_msg.ptr());
      }
    } else if (0 != failed_index.count()) {
      ObSArray<ObString> failed_users;
      ObSArray<ObString> failed_hosts;
      if (OB_FAIL(ObDropUserExecutor::string_array_index_extract(
          arg.users_, arg.hosts_, failed_index, failed_users, failed_hosts))) {
        LOG_WARN("Failed to extract user name", K(arg), K(ret));
      } else {
        if (OB_FAIL(ObDropUserExecutor::build_fail_msg(failed_users, failed_hosts, fail_msg))) {
          LOG_WARN("Build fail msg error", K(arg), K(ret));
        } else {
          ret = OB_CANNOT_USER;
          LOG_USER_ERROR(OB_CANNOT_USER, (int)strlen("LOCK USER"), "LOCK USER", (int)fail_msg.length(), fail_msg.ptr());
        }
      }
    } else {
      //do nothing
    }
  }
  return ret;
}

int ObAlterUserProfileExecutor::set_role_exec(ObExecContext &ctx, ObAlterUserProfileStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  uint64_t role_id = OB_INVALID_ID;
  CK (ObAlterUserProfileStmt::SET_ROLE == stmt.get_set_role_flag());
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    const uint64_t tenant_id = session->get_effective_tenant_id();
    const uint64_t user_id = lib::is_mysql_mode() ? session->get_priv_user_id() : session->get_user_id();
    const ObUserInfo * user_info = NULL;
    common::ObArray<uint64_t> enable_role_id_array;
    ObSchemaGetterGuard schema_guard;

    obrpc::ObAlterUserProfileArg &arg = static_cast<obrpc::ObAlterUserProfileArg &>(stmt.get_ddl_arg());
    OZ (GCTX.schema_service_->get_tenant_schema_guard(
                  tenant_id,
                  schema_guard));
    OZ (schema_guard.get_user_info(tenant_id, user_id, user_info));
    if (OB_SUCC(ret) && NULL == user_info) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user info is null", K(ret));
    }
    if (OB_SUCC(ret)) {
      switch (arg.default_role_flag_) {
      case OB_DEFAULT_ROLE_ALL:
        //OZ (session->set_enable_role_array(user_info->get_role_id_array()));
        OZ (append(enable_role_id_array, user_info->get_role_id_array()));
        break;
      case OB_DEFAULT_ROLE_NONE:
        OX (enable_role_id_array.reset());
        break;
      case OB_DEFAULT_ROLE_LIST:
        OX (enable_role_id_array.reset());
        for (int i = 0; OB_SUCC(ret) && i < arg.role_id_array_.count(); i++) {
          OX (role_id = arg.role_id_array_.at(i));
          OZ (enable_role_id_array.push_back(role_id));
        }
        break;
      case OB_DEFAULT_ROLE_ALL_EXCEPT:
        OX (enable_role_id_array.reset());
        /* scan all role granted to the user */
        for (int i = 0; OB_SUCC(ret) && i < user_info->get_role_id_array().count(); i++) {
          OX (role_id = user_info->get_role_id_array().at(i));
          /* if not in execpt set, then push back */
          if (OB_SUCC(ret) && !has_exist_in_array(arg.role_id_array_, role_id)) {
            OZ (enable_role_id_array.push_back(role_id));
          }
        }
        break;
      case OB_DEFAULT_ROLE_DEFAULT:
        OX (enable_role_id_array.reset());
        for (int i = 0; OB_SUCC(ret) && i < user_info->get_role_id_array().count(); i++) {
          if (user_info->get_disable_option(user_info->get_role_id_option_array().at(i)) == 0) {
            OZ (enable_role_id_array.push_back(user_info->get_role_id_array().at(i)));
          }
        }
        break;
      }
      if (lib::is_oracle_mode()) {
        OZ (enable_role_id_array.push_back(OB_ORA_PUBLIC_ROLE_ID));
      }
      OZ (session->set_enable_role_array(enable_role_id_array));
    }
  }
  return ret;
}

int ObAlterUserProfileExecutor::execute(ObExecContext &ctx, ObAlterUserProfileStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (ObAlterUserProfileStmt::SET_ROLE == stmt.get_set_role_flag()) {
    OZ (set_role_exec(ctx, stmt));
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->alter_user_profile(stmt.get_ddl_arg()))) {
    LOG_WARN("alter user profile failed", K(stmt.get_ddl_arg()), K(ret));
  }
  return ret;
}

int ObAlterUserProxyExecutor::execute(ObExecContext &ctx, ObAlterUserProxyStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  obrpc::ObAlterUserProxyRes res;
  if (OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (OB_FALSE_IT(stmt.get_ddl_arg().ddl_stmt_str_ = stmt.get_query_ctx()->get_sql_stmt())) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->alter_user_proxy(stmt.get_ddl_arg(), res))) {
    LOG_WARN("alter user proxy failed", K(stmt.get_ddl_arg()), K(ret));
  }
  return ret;
}

int ObRenameUserExecutor::execute(ObExecContext &ctx, ObRenameUserStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const uint64_t tenant_id = stmt.get_tenant_id();
  const ObStrings *rename_infos = NULL;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", K(ret));
  } else if (OB_ISNULL(rename_infos = stmt.get_rename_infos())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("names is NULL", K(ret));
  } else if (rename_infos->count() < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user not specified", K(ret));
  } else if (rename_infos->count() % 4 != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("old and new names count not match", K(ret));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else {
    ObString old_username;
    ObString old_hostname;
    ObString new_username;
    ObString new_hostname;
    ObRenameUserArg &arg = static_cast<ObRenameUserArg &>(stmt.get_ddl_arg());
    arg.tenant_id_ = tenant_id;
    {
      ObSchemaGetterGuard schema_guard;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      }
      //rename_infos arr contains old names and new names in pairs, so step is 2
      for (int64_t i = 0; OB_SUCC(ret) && i < rename_infos->count(); i += 4) {
        if (OB_FAIL(rename_infos->get_string(i, old_username))) {
          LOG_WARN("Get origin name failed", K(ret));
        } else if (OB_FAIL(rename_infos->get_string(i + 1, old_hostname))) {
          LOG_WARN("Get to name failed", K(ret));
        } else if (OB_FAIL(rename_infos->get_string(i + 2, new_username))) {
          LOG_WARN("Get to name failed", K(ret));
        } else if (OB_FAIL(rename_infos->get_string(i + 3, new_hostname))) {
          LOG_WARN("Get to name failed", K(ret));
        } else if (OB_FAIL(arg.old_users_.push_back(old_username))) {
          LOG_WARN("Add origin user name failed", K(ret));
        } else if (OB_FAIL(arg.old_hosts_.push_back(old_hostname))) {
          LOG_WARN("Add origin host name failed", K(ret));
        } else if (OB_FAIL(arg.new_users_.push_back(new_username))) {
          LOG_WARN("Add new user name failed", K(ret));
        } else if (OB_FAIL(arg.new_hosts_.push_back(new_hostname))) {
          LOG_WARN("Add new host name failed", K(ret));
        } else if (OB_FAIL(ObCreateUserExecutor::check_user_valid(schema_guard, ctx.get_my_session()->get_user_priv_set(), tenant_id,
                                                                  old_username, old_hostname, "RENAME USER"))) {
          LOG_WARN("check user valid failed", K(ret));
        } else if (OB_FAIL(ObCreateUserExecutor::check_user_valid(schema_guard, ctx.get_my_session()->get_user_priv_set(), tenant_id,
                                                                  new_username, new_hostname, "RENAME USER"))) {
          LOG_WARN("check user valid failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rename_user(common_rpc_proxy, arg))) {
        LOG_WARN("Rename user completely failed", K(arg), K(ret));
      }
    }
  }
  return ret;
}

int ObRenameUserExecutor::rename_user(obrpc::ObCommonRpcProxy *rpc_proxy,
                                      const obrpc::ObRenameUserArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arg", K(arg), K(ret));
  } else if (OB_UNLIKELY(arg.old_users_.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user not specified", K(arg), K(ret));
  } else if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rpc_proxy is null", K(ret));
  } else {
    ObSArray<int64_t> failed_index;
    ObSqlString fail_msg;
    if (OB_FAIL(rpc_proxy->rename_user(arg, failed_index))) {
      LOG_WARN("Rename user failed", K(ret));
      if (OB_FAIL(ObDropUserExecutor::build_fail_msg(arg.old_users_, arg.old_hosts_, fail_msg))) {
        LOG_WARN("Build fail msg error", K(arg), K(ret));
      } else {
        ret = OB_CANNOT_USER;
        LOG_USER_ERROR(OB_CANNOT_USER, (int)strlen("RENAME USER"), "RENAME USER", (int)fail_msg.length(), fail_msg.ptr());
      }
    } else if (0 != failed_index.count()) {
      ObSArray<ObString> failed_users;
      ObSArray<ObString> failed_hosts;
      if (OB_FAIL(ObDropUserExecutor::string_array_index_extract(
          arg.old_users_, arg.old_hosts_, failed_index, failed_users, failed_hosts))) {
        LOG_WARN("Failed to extract user name", K(arg), K(ret));
      } else {
        if (OB_FAIL(ObDropUserExecutor::build_fail_msg(failed_users, failed_hosts, fail_msg))) {
          LOG_WARN("Build fail msg error", K(arg), K(ret));
        } else {
          ret = OB_CANNOT_USER;
          LOG_USER_ERROR(OB_CANNOT_USER, (int)strlen("RENAME USER"), "RENAME USER", (int)fail_msg.length(), fail_msg.ptr());
        }
      }
    } else {
      //Rename user completely success
    }
  }
  return ret;
}

int ObAlterUserPrimaryZoneExecutor::execute(ObExecContext &ctx, ObAlterUserPrimaryZoneStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObString first_stmt;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "session is NULL");
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("fail to get first stmt" , K(ret));
  } else {
    stmt.arg_.exec_tenant_id_ = session->get_effective_tenant_id();
    stmt.arg_.ddl_stmt_str_ = first_stmt;
    OZ(common_rpc_proxy->alter_database(stmt.arg_));
  }

  return ret;
}

}// ns sql
}// ns oceanbase
