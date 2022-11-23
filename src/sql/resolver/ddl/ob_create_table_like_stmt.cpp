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




