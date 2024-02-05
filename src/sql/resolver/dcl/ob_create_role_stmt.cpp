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
#include "sql/resolver/dcl/ob_create_role_stmt.h"

#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_strings.h"
#include "lib/utility/ob_print_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObCreateRoleStmt::ObCreateRoleStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_CREATE_ROLE),
      tenant_id_(OB_INVALID_ID),
      role_name_(),
      password_(),
      need_enc_(false),
      if_not_exists_(false)
{
}

ObCreateRoleStmt::ObCreateRoleStmt()
    : ObDDLStmt(NULL, stmt::T_CREATE_ROLE),
      tenant_id_(OB_INVALID_ID),
      role_name_(),
      password_(),
      need_enc_(false),
      if_not_exists_(false)
{
}

ObCreateRoleStmt::~ObCreateRoleStmt()
{
}

int64_t ObCreateRoleStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf) {
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_),
         "role_to_create", role_name_,
         "password", password_);
    J_OBJ_END();
  }
  return pos;
}
