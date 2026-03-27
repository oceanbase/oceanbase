/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dcl/ob_drop_role_stmt.h"


using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObDropRoleStmt::ObDropRoleStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_DROP_ROLE),
      tenant_id_(OB_INVALID_ID),
      role_name_(),
      if_exists_(false)
{
}

ObDropRoleStmt::ObDropRoleStmt()
    : ObDDLStmt(NULL, stmt::T_DROP_ROLE),
      tenant_id_(OB_INVALID_ID),
      role_name_(),
      if_exists_(false)
{
}

ObDropRoleStmt::~ObDropRoleStmt()
{
}

int64_t ObDropRoleStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf) {
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_),
         "role_to_drop", role_name_);
    J_OBJ_END();
  }
  return pos;
}
