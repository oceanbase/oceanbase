/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/resolver/dcl/ob_alter_user_profile_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
ObAlterUserProfileStmt::ObAlterUserProfileStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_ALTER_USER_PROFILE), arg_(), set_role_flag_(0)
{
}

ObAlterUserProfileStmt::ObAlterUserProfileStmt()
    : ObDDLStmt(NULL, stmt::T_ALTER_USER_PROFILE), arg_(), set_role_flag_(0)
{
}

ObAlterUserProfileStmt::~ObAlterUserProfileStmt()
{
}

