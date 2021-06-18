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

#include "sql/resolver/dcl/ob_alter_user_profile_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
ObAlterUserProfileStmt::ObAlterUserProfileStmt(common::ObIAllocator* name_pool)
    : ObDDLStmt(name_pool, stmt::T_ALTER_USER_PROFILE), arg_(), set_role_flag_(0)
{}

ObAlterUserProfileStmt::ObAlterUserProfileStmt()
    : ObDDLStmt(NULL, stmt::T_ALTER_USER_PROFILE), arg_(), set_role_flag_(0)
{}

ObAlterUserProfileStmt::~ObAlterUserProfileStmt()
{}
int64_t ObAlterUserProfileStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(stmt_type), K_(arg));
  J_OBJ_END();
  return pos;
}
