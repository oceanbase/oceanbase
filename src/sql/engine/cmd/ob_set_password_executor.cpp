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
#include "sql/engine/cmd/ob_set_password_executor.h"
#include "lib/string/ob_sql_string.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/dcl/ob_set_password_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_user_cmd_executor.h"

namespace oceanbase
{

using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace sql
{

ObSetPasswordExecutor::ObSetPasswordExecutor()
{
}

ObSetPasswordExecutor::~ObSetPasswordExecutor()
{
}

int ObSetPasswordExecutor::execute(ObExecContext &ctx, ObSetPasswordStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const uint64_t tenant_id = stmt.get_tenant_id();
  const common::ObStrings *user_passwd = NULL;
  const int64_t FIX_MEMBER_CNT = 7;
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", K(ret));
  } else if (OB_ISNULL(user_passwd = stmt.get_user_password())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user_passwd is null", K(ret));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("task_exec_ctx is null", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) {
    ret = OB_NOT_INIT;
    LOG_WARN("common_rpc_proxy is null", K(ret));
  } else if (OB_UNLIKELY(FIX_MEMBER_CNT != user_passwd->count())) {
    ret = OB_ERR_UNEXPECTED;;
    LOG_WARN("invalid set pwd stmt, wrong user passwd count", K(ret));
  } else {
    ObString user_name;
    ObString host_name;
    ObString passwd;
    ObString ssl_type;
    ObString ssl_cipher;
    ObString x509_issuer;
    ObString x509_subject;
    ObSSLType ssl_type_enum = ObSSLType::SSL_TYPE_NOT_SPECIFIED;

    if (OB_FAIL(user_passwd->get_string(0, user_name))) {
      LOG_WARN("Get user name failed", K(ret));
    } else if (OB_FAIL(user_passwd->get_string(1, host_name))) {
      LOG_WARN("Get passwd failed", K(ret));
    } else if (OB_FAIL(user_passwd->get_string(2, passwd))) {
      LOG_WARN("Get passwd failed", K(ret));
    } else if (OB_FAIL(user_passwd->get_string(3, ssl_type))) {
      LOG_WARN("Get string from ObStrings error", K(ret));
    } else if (OB_FAIL(user_passwd->get_string(4, ssl_cipher))) {
      LOG_WARN("Get string from ObStrings error", K(ret));
    } else if (OB_FAIL(user_passwd->get_string(5, x509_issuer))) {
      LOG_WARN("Get string from ObStrings error", K(ret));
    } else if (OB_FAIL(user_passwd->get_string(6, x509_subject))) {
      LOG_WARN("Get string from ObStrings error", K(ret));
    } else if (OB_UNLIKELY(ObSSLType::SSL_TYPE_MAX == (ssl_type_enum = get_ssl_type_from_string(ssl_type)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("known ssl_type", K(ssl_type), K(ret));
    } else {
      char enc_buf[ENC_BUF_LEN] = {0};
      ObSetPasswdArg arg;
      arg.tenant_id_ = tenant_id;
      arg.user_ = user_name;
      arg.host_ = host_name;
      arg.ssl_type_ = ssl_type_enum;
      arg.ssl_cipher_ = ssl_cipher;
      arg.x509_issuer_ = x509_issuer;
      arg.x509_subject_ = x509_subject;
      arg.exec_tenant_id_ = tenant_id;
      arg.max_connections_per_hour_ = stmt.get_max_connections_per_hour();
      arg.max_user_connections_= stmt.get_max_user_connections();
      arg.modify_max_connections_ = stmt.get_modify_max_connections();
      if (stmt.get_need_enc()) {
        if (OB_FAIL(ObCreateUserExecutor::encrypt_passwd(passwd, arg.passwd_, enc_buf, ENC_BUF_LEN))) {
          LOG_WARN("Encrypt passwd failed", K(ret));
        }
      } else {
        arg.passwd_ = passwd;
      }
      if (OB_SUCC(ret) && OB_FAIL(common_rpc_proxy->set_passwd(arg))) {
          LOG_WARN("Set password failed", K(ret));
      } else if (0 == user_name.case_compare(session->get_user_name())) {
        session->set_password_expired(false);
      }
    }
  }
  return ret;
}



}/* ns sql*/
}/* ns oceanbase */
