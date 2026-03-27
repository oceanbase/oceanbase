/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/resolver/dcl/ob_alter_user_proxy_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
ObAlterUserProxyStmt::ObAlterUserProxyStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_ALTER_USER_PROXY), arg_()
{
}

ObAlterUserProxyStmt::ObAlterUserProxyStmt()
    : ObDDLStmt(NULL, stmt::T_ALTER_USER_PROXY), arg_()
{
}

ObAlterUserProxyStmt::~ObAlterUserProxyStmt()
{
}
