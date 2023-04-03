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
