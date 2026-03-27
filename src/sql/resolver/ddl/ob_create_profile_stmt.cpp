/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_create_profile_stmt.h"


using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObUserProfileStmt::ObUserProfileStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_USER_PROFILE),
      create_profile_arg_()
{
}

ObUserProfileStmt::ObUserProfileStmt()
    : ObDDLStmt(NULL, stmt::T_USER_PROFILE),
      create_profile_arg_()
{
}

ObUserProfileStmt::~ObUserProfileStmt()
{
}
