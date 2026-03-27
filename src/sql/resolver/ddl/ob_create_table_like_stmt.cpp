/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/resolver/ddl/ob_create_table_like_stmt.h"

namespace oceanbase
{
namespace sql
{

ObCreateTableLikeStmt::ObCreateTableLikeStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_CREATE_TABLE_LIKE)
{
}

ObCreateTableLikeStmt::ObCreateTableLikeStmt()
    : ObDDLStmt(stmt::T_CREATE_TABLE_LIKE)
{
}

ObCreateTableLikeStmt::~ObCreateTableLikeStmt()
{
}




} //namespace sql
} //namespace oceanbase




