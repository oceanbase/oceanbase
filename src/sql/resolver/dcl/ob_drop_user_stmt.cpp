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

#include "sql/resolver/dcl/ob_drop_user_stmt.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObDropUserStmt::ObDropUserStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_DROP_USER),
      tenant_id_(OB_INVALID_ID)
{
}

ObDropUserStmt::ObDropUserStmt()
    : ObDDLStmt(NULL, stmt::T_DROP_USER),
      tenant_id_(OB_INVALID_ID)
{
}

ObDropUserStmt::~ObDropUserStmt()
{
}

int ObDropUserStmt::add_user(const common::ObString &user_name, const common::ObString &host_name)
{
  int ret = OB_SUCCESS;
  if (0 == user_name.compare(OB_SYS_USER_NAME)
      && 0 == host_name.compare(OB_SYS_HOST_NAME)) {
    ret = OB_ERR_NO_PRIVILEGE;
    SQL_RESV_LOG(WARN, "Can not drop root user", K(ret));
  } else if (OB_FAIL(users_.add_string(user_name))) {
    SQL_RESV_LOG(WARN, "failed to add user to DropUserStmt", K(ret));
  } else if (OB_FAIL(users_.add_string(host_name))) {
    SQL_RESV_LOG(WARN, "failed to add host_name to DropUserStmt", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

int64_t ObDropUserStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf) {
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_),
         "users", users_);
    J_OBJ_END();
  }
  return pos;
}
