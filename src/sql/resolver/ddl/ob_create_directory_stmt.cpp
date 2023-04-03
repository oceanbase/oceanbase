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

#include "sql/resolver/ddl/ob_create_directory_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{
ObCreateDirectoryStmt::ObCreateDirectoryStmt()
  : ObDDLStmt(stmt::T_CREATE_DIRECTORY),
    arg_()
{
}

ObCreateDirectoryStmt::ObCreateDirectoryStmt(common::ObIAllocator *name_pool)
  : ObDDLStmt(name_pool, stmt::T_CREATE_DIRECTORY),
    arg_()
{
}

ObCreateDirectoryStmt::~ObCreateDirectoryStmt()
{
}
} // namespace sql
} // namespace oceanbase