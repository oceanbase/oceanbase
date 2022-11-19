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
#include "sql/engine/cmd/ob_dcl_executor.h"

#include "lib/encrypt/ob_encrypted_helper.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/dcl/ob_grant_stmt.h"
#include "sql/resolver/dcl/ob_revoke_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/cmd/ob_user_cmd_executor.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObGrantExecutor::execute(ObExecContext &ctx, ObGrantStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObSQLSessionInfo *session_info = NULL;
  const uint64_t tenant_id = stmt.get_tenant_id();
  const ObStrings &users = stmt.get_users();
  ObIAllocator &allocator = ctx.get_allocator();
  obrpc::ObGrantArg &arg = static_cast<obrpc::ObGrantArg &>(stmt.get_ddl_arg());
  const bool is_role = arg.roles_.count() > 0;

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(session_info = ctx.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Get my session error");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (!is_role) {
    ObString user_name;
    ObString host_name;
    ObString pwd;
    ObString need_enc;
    //i += 4, each with user_name, pwd, need_enc
    if (OB_UNLIKELY(users.count() <= 0) || OB_UNLIKELY(0 != users.count() % 4)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Resolve users error. Users should have user and pwd",
               "ObStrings count", users.count(), K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < users.count(); i += 4) {
      if (OB_FAIL(users.get_string(i, user_name))) {
        LOG_WARN("Get string from ObStrings error", "count",
            users.count(), K(i), K(ret));
      } else if (OB_FAIL(users.get_string(i + 1, host_name))) {
        LOG_WARN("Get string from ObStrings error", "count",
            users.count(), K(i), K(ret));
      } else if (OB_FAIL(users.get_string(i + 2, pwd))) {
        LOG_WARN("Get string from ObStrings error", "count",
            users.count(), K(i), K(ret));
      } else if (OB_FAIL(users.get_string(i + 3, need_enc))) {
        LOG_WARN("Get string from ObStrings error", "count",
            users.count(), K(i), K(ret));
      } else {
        if (OB_FAIL(arg.users_passwd_.push_back(user_name))) {
          LOG_WARN("failed to add user", K(ret));
        } else if (OB_FAIL(arg.hosts_.push_back(host_name))) {
          LOG_WARN("failed to add user", K(ret));
        } else if (ObString::make_string("YES") == need_enc) {
          ObString pwd_enc;
          if (pwd.length() > 0) {
            char *enc_buf = NULL;
            if (NULL == (enc_buf = static_cast<char *>(allocator.alloc(ENC_BUF_LEN)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_ERROR("Failed to allocate memory", K(ret), K(ENC_BUF_LEN));
            } else if (OB_FAIL(ObCreateUserExecutor::encrypt_passwd(pwd, 
                                                                    pwd_enc, 
                                                                    enc_buf, 
                                                                    ENC_BUF_LEN))) {
              LOG_WARN("Encrypt password failed", K(ret));
            } else { }//do nothing
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(arg.users_passwd_.push_back(pwd_enc))) {
            LOG_WARN("failed to add password", K(ret));
          } else { }//do nothing
        } else {
          if (OB_FAIL(arg.users_passwd_.push_back(pwd))) {
            LOG_WARN("failed to add password", K(ret));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing.
  } else if (OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query ctx is null", K(ret));
  } else {
    arg.tenant_id_ = tenant_id;
    arg.db_ = stmt.get_database_name();
    arg.table_ = stmt.get_table_name();
    arg.priv_set_ = stmt.get_priv_set();
    arg.priv_level_ = stmt.get_grant_level();
    arg.need_create_user_ = stmt.get_need_create_user();
    arg.has_create_user_priv_ = session_info->get_user_priv_set() & OB_PRIV_CREATE_USER;
    arg.ddl_stmt_str_ = stmt.get_query_ctx()->get_sql_stmt();
    arg.option_ = stmt.get_option();
    arg.object_type_ = stmt.get_object_type();
    arg.object_id_ = stmt.get_object_id();
    arg.grantor_id_ = stmt.get_grantor_id();
    arg.ins_col_ids_ = stmt.get_ins_col_ids();
    arg.upd_col_ids_ = stmt.get_upd_col_ids();
    arg.ref_col_ids_ = stmt.get_ref_col_ids();
    arg.is_inner_ = session_info->is_inner();
    if (OB_FAIL(common_rpc_proxy->grant(arg))) {
      LOG_WARN("Grant privileges to user error", K(ret), K(arg));
    }
  }
  return ret;
}

int ObRevokeExecutor::execute(ObExecContext &ctx, ObRevokeStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;

  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("get common rpc proxy failed");
  } else {
    switch (stmt.get_grant_level()) {
      case OB_PRIV_USER_LEVEL: {
        if (OB_FAIL(revoke_user(common_rpc_proxy, stmt))) {
          LOG_WARN("grant_revoke_user error", K(ret));
        }
        break;
      }
      case OB_PRIV_DB_LEVEL: {
        if (OB_FAIL(revoke_db(common_rpc_proxy, stmt))) {
          LOG_WARN("grant_revoke_user error", K(ret));
        }
        break;
      }
      case OB_PRIV_TABLE_LEVEL: {
        if (OB_FAIL(revoke_table(common_rpc_proxy, stmt))) {
          LOG_WARN("grant_revoke_user error", K(ret));
        }
        break;
      case OB_PRIV_SYS_ORACLE_LEVEL: { // Oracle revoke role and sys_priv
        if (OB_FAIL(revoke_sys_priv(common_rpc_proxy, stmt))) {
          LOG_WARN("grant_revoke_user error", K(ret));
        }
        break;
      }
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Resolver may be error, invalid grant_level",
                   "grant_level", ob_priv_level_str(stmt.get_grant_level()), K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObRevokeExecutor::revoke_user(obrpc::ObCommonRpcProxy *rpc_proxy, ObRevokeStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input argument error", K(rpc_proxy), K(ret));
  } else {
    obrpc::ObRevokeUserArg &arg = static_cast<obrpc::ObRevokeUserArg &>(stmt.get_ddl_arg());
    arg.tenant_id_ = stmt.get_tenant_id();
    const ObIArray<uint64_t> &user_ids = stmt.get_users();
    const bool is_role = arg.role_ids_.count() > 0;
    if (is_role) {
      for (int i = 0; OB_SUCC(ret) && i < user_ids.count(); ++i) {
        arg.user_id_ = user_ids.at(i);
        if (OB_FAIL(rpc_proxy->revoke_user(arg))) {
          LOG_WARN("revoke user error", K(arg), K(ret));
        }
      }
    } else if (0 == user_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("User ids is empty, resolver may be error", K(ret));
    } else {
      arg.revoke_all_ = stmt.get_revoke_all();
      arg.priv_set_ = stmt.get_priv_set();
      for (int i = 0; OB_SUCC(ret) && i < user_ids.count(); i++) {
        arg.user_id_ = user_ids.at(i);
        if (OB_FAIL(rpc_proxy->revoke_user(arg))) {
          LOG_WARN("revoke user error", K(arg), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRevokeExecutor::revoke_db(obrpc::ObCommonRpcProxy *rpc_proxy, ObRevokeStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input argument error", K(rpc_proxy), K(ret));
  } else {
    obrpc::ObRevokeDBArg &arg = static_cast<obrpc::ObRevokeDBArg &>(stmt.get_ddl_arg());
    arg.tenant_id_ = stmt.get_tenant_id();
    arg.priv_set_ = stmt.get_priv_set();
    arg.db_ = stmt.get_database_name();
    const ObIArray<uint64_t> &user_ids = stmt.get_users();
    if (0 == user_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("User ids is empty, resolver may be error", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < user_ids.count(); i++) {
        arg.user_id_ = user_ids.at(i);
        if (OB_FAIL(rpc_proxy->revoke_database(arg))) {
          LOG_WARN("revoke user error", K(arg), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRevokeExecutor::revoke_table(obrpc::ObCommonRpcProxy *rpc_proxy, ObRevokeStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input argument error", K(rpc_proxy), K(ret));
  } else {
    obrpc::ObRevokeTableArg &arg = static_cast<obrpc::ObRevokeTableArg &>(stmt.get_ddl_arg());
    arg.tenant_id_ = stmt.get_tenant_id();
    arg.priv_set_ = stmt.get_priv_set();
    arg.db_ = stmt.get_database_name();
    arg.table_ = stmt.get_table_name();
    arg.obj_id_ = stmt.get_obj_id();
    arg.obj_type_ = static_cast<uint64_t>(stmt.get_object_type());
    arg.grantor_id_ = stmt.get_grantor_id();
    arg.revoke_all_ora_ = stmt.get_revoke_all_ora();

    const ObIArray<uint64_t> &user_ids = stmt.get_users();
    if (0 == user_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("User ids is empty, resolver may be error", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < user_ids.count(); i++) {
        arg.user_id_ = user_ids.at(i);
        if (OB_FAIL(rpc_proxy->revoke_table(arg))) {
          LOG_WARN("revoke user error", K(arg), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRevokeExecutor::revoke_sys_priv(obrpc::ObCommonRpcProxy *rpc_proxy, ObRevokeStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input argument error", K(rpc_proxy), K(ret));
  } else {
    obrpc::ObRevokeSysPrivArg &arg = static_cast<obrpc::ObRevokeSysPrivArg &>(stmt.get_ddl_arg());
    arg.tenant_id_ = stmt.get_tenant_id();
    // 在resolve阶段，已经设置了arg的priv_array
    // 在resolve阶段，也已经设置了arg的role_ids_
    const ObIArray<uint64_t> &user_ids = stmt.get_users();
    if (0 == user_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("User ids is empty, resolver may be error", K(ret));
    } else if (OB_ISNULL(stmt.get_query_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query ctx is null", K(ret));
    } else {
      arg.ddl_stmt_str_ = stmt.get_query_ctx()->get_sql_stmt();
      for (int i = 0; OB_SUCC(ret) && i < user_ids.count(); i++) {
        arg.grantee_id_ = user_ids.at(i);
        if (OB_FAIL(rpc_proxy->revoke_sys_priv(arg))) {
          LOG_WARN("revoke user error", K(arg), K(ret));
        }
      }
    }
  }
  return ret;
}

}// ns sql
}// ns oceanbase
