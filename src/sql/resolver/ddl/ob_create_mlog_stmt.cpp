/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_create_mlog_stmt.h"
namespace oceanbase
{
namespace sql
{
ObCreateMLogStmt::ObCreateMLogStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_CREATE_MLOG),
    create_mlog_arg_()
{
}

ObCreateMLogStmt::ObCreateMLogStmt()
    : ObDDLStmt(stmt::T_CREATE_MLOG),
    create_mlog_arg_()
{
}

ObCreateMLogStmt::~ObCreateMLogStmt()
{
}

} // sql
} // oceanbase