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
#include "sql/resolver/dcl/ob_create_user_stmt.h"

#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_strings.h"
#include "lib/utility/ob_print_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObCreateUserStmt::ObCreateUserStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_CREATE_USER),
      tenant_id_(OB_INVALID_ID),
      users_(),
      masked_sql_(),
      if_not_exist_(false),
      profile_id_(OB_INVALID_ID),
      max_connections_per_hour_(0),
      max_user_connections_(0)
{
}

ObCreateUserStmt::ObCreateUserStmt()
    : ObDDLStmt(NULL, stmt::T_CREATE_USER),
      tenant_id_(OB_INVALID_ID),
      users_(),
      masked_sql_(),
      if_not_exist_(false),
      profile_id_(OB_INVALID_ID),
      max_connections_per_hour_(0),
      max_user_connections_(0)
{
}

ObCreateUserStmt::~ObCreateUserStmt()
{
}

int ObCreateUserStmt::add_user(const common::ObString &user_name,
                               const common::ObString &host_name,
                               const common::ObString &password,
                               const common::ObString &need_enc)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(users_.add_string(user_name))) {
    LOG_WARN("failed to add user", K(ret));
  } else if (OB_FAIL(users_.add_string(host_name))) {
    LOG_WARN("failed to add host_name", K(ret));
  } else if (OB_FAIL(users_.add_string(password))) {
    LOG_WARN("failed to add password", K(ret));
  } else if (OB_FAIL(users_.add_string(need_enc))) {
    LOG_WARN("failed to add need enc", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

int ObCreateUserStmt::add_ssl_info(const common::ObString &ssl_type,
                                   const common::ObString &ssl_cipher,
                                   const common::ObString &x509_issuer,
                                   const common::ObString &x509_subject)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(users_.add_string(ssl_type))) {
    LOG_WARN("failed to add ssl_type", K(ret));
  } else if (OB_FAIL(users_.add_string(ssl_cipher))) {
    LOG_WARN("failed to add ssl_cipher", K(ret));
  } else if (OB_FAIL(users_.add_string(x509_issuer))) {
    LOG_WARN("failed to add x509_issuer", K(ret));
  } else if (OB_FAIL(users_.add_string(x509_subject))) {
    LOG_WARN("failed to add x509_subject", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

int64_t ObCreateUserStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf) {
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_),
         "users_to_create", users_);
    J_OBJ_END();
  }
  return pos;
}
