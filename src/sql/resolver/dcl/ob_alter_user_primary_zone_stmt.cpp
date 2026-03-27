/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/resolver/dcl/ob_alter_user_primary_zone_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
ObAlterUserPrimaryZoneStmt::ObAlterUserPrimaryZoneStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_ALTER_USER_PRIMARY_ZONE), arg_()
           
{
}

ObAlterUserPrimaryZoneStmt::ObAlterUserPrimaryZoneStmt()
    : ObDDLStmt(NULL, stmt::T_ALTER_USER_PRIMARY_ZONE), arg_()
{
}

ObAlterUserPrimaryZoneStmt::~ObAlterUserPrimaryZoneStmt()
{
}
