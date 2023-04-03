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
#include "sql/resolver/dcl/ob_set_password_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObSetPasswordStmt::ObSetPasswordStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_SET_PASSWORD),
      masked_sql_(), tenant_id_(false), need_enc_(false), for_current_user_(false),
      modify_max_connections_(false), max_connections_per_hour_(OB_INVALID_ID),
      max_user_connections_(OB_INVALID_ID)
{
}

ObSetPasswordStmt::ObSetPasswordStmt()
    : ObDDLStmt(NULL, stmt::T_SET_PASSWORD),
      masked_sql_(), tenant_id_(false), need_enc_(false), for_current_user_(false),
      modify_max_connections_(false), max_connections_per_hour_(OB_INVALID_ID),
      max_user_connections_(OB_INVALID_ID)
{
}

ObSetPasswordStmt::~ObSetPasswordStmt()
{
}

int ObSetPasswordStmt::set_user_password(const common::ObString &user_name,
                                         const common::ObString &host_name,
                                         const common::ObString &password)
{
  int ret = OB_SUCCESS;
  if (0 != user_pwd_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be only set once", K(ret));
  } else if (OB_FAIL(user_pwd_.add_string(user_name))) {
    LOG_WARN("failed to add string", K(ret));
  } else if (OB_FAIL(user_pwd_.add_string(host_name))) {
    LOG_WARN("failed to add string", K(ret));
  } else if (OB_FAIL(user_pwd_.add_string(password))) {
    LOG_WARN("failed to add string", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

int ObSetPasswordStmt::add_ssl_info(const common::ObString &ssl_type,
                                    const common::ObString &ssl_cipher,
                                    const common::ObString &x509_issuer,
                                    const common::ObString &x509_subject)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(user_pwd_.add_string(ssl_type))) {
    LOG_WARN("failed to add ssl_type", K(ret));
  } else if (OB_FAIL(user_pwd_.add_string(ssl_cipher))) {
    LOG_WARN("failed to add ssl_cipher", K(ret));
  } else if (OB_FAIL(user_pwd_.add_string(x509_issuer))) {
    LOG_WARN("failed to add x509_issuer", K(ret));
  } else if (OB_FAIL(user_pwd_.add_string(x509_subject))) {
    LOG_WARN("failed to add x509_subject", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

int64_t ObSetPasswordStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf) {
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_),
         "user_pwd", user_pwd_);
    J_OBJ_END();
  }
  return pos;
}
