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

#include "sql/resolver/dcl/ob_lock_user_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
ObLockUserStmt::ObLockUserStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_LOCK_USER), tenant_id_(OB_INVALID_ID) , locked_(false)
{
}

ObLockUserStmt::ObLockUserStmt()
    : ObDDLStmt(NULL, stmt::T_LOCK_USER), tenant_id_(OB_INVALID_ID) , locked_(false)
{
}

ObLockUserStmt::~ObLockUserStmt()
{
}

int ObLockUserStmt::add_user(const ObString &user_name, const common::ObString &host_name)
{
  int ret = OB_SUCCESS;
  if (0 == user_name.compare(OB_SYS_USER_NAME)
      && 0 == host_name.compare(OB_SYS_HOST_NAME)) {
    ret = OB_ERR_NO_PRIVILEGE;
    SQL_RESV_LOG(WARN, "Can not lock root user", K(ret));
  } else if (OB_FAIL(user_.add_string(user_name))) {
    SQL_RESV_LOG(WARN, "Add user failed", K(user_name), K(ret));
  } else if (OB_FAIL(user_.add_string(host_name))) {
    SQL_RESV_LOG(WARN, "Add host failed", K(user_name), K(host_name), K(ret));
  } else {
    //do nothing
  }
  return ret;
}

