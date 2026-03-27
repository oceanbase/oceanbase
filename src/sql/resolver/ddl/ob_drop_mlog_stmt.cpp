/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_drop_mlog_stmt.h"
namespace oceanbase
{
namespace sql
{
ObDropMLogStmt::ObDropMLogStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_DROP_MLOG),
    drop_index_arg_()
{
}

ObDropMLogStmt::ObDropMLogStmt()
    : ObDDLStmt(stmt::T_DROP_MLOG),
    drop_index_arg_()
{
}

ObDropMLogStmt::~ObDropMLogStmt()
{
}

} // sql
} // oceanbase