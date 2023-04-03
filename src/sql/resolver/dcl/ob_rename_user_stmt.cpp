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

#include "sql/resolver/dcl/ob_rename_user_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObRenameUserStmt::ObRenameUserStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_RENAME_USER), tenant_id_(OB_INVALID_ID)
{
}

ObRenameUserStmt::ObRenameUserStmt()
    : ObDDLStmt(NULL, stmt::T_RENAME_USER), tenant_id_(OB_INVALID_ID)
{
}

ObRenameUserStmt::~ObRenameUserStmt()
{
}

int ObRenameUserStmt::add_rename_info(const common::ObString &from_user,
    const common::ObString &from_host, const common::ObString &to_user,
    const common::ObString &to_host)
{
  int ret = OB_SUCCESS;
  if (0 == from_user.compare(OB_SYS_USER_NAME)
      && 0 == from_host.compare(OB_SYS_HOST_NAME)) {
    ret = OB_ERR_NO_PRIVILEGE;
    SQL_RESV_LOG(WARN, "Can not rename root to other name", K(ret));
  } else if (OB_FAIL(rename_infos_.add_string(from_user))) {
    SQL_RESV_LOG(WARN, "failed to add string", K(ret));
  } else if (OB_FAIL(rename_infos_.add_string(from_host))) {
    SQL_RESV_LOG(WARN, "failed to add string", K(ret));
  } else if (OB_FAIL(rename_infos_.add_string(to_user))) {
    SQL_RESV_LOG(WARN, "failed to add string", K(ret));
  } else if (OB_FAIL(rename_infos_.add_string(to_host))) {
    SQL_RESV_LOG(WARN, "failed to add string", K(ret));
  }
  return ret;
}
const ObStrings *ObRenameUserStmt::get_rename_infos() const
{
  return &rename_infos_;
}

int64_t ObRenameUserStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf) {
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_),
         "rename_infos", rename_infos_);
    J_OBJ_END();
  }
  return pos;
}
