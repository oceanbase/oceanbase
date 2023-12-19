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