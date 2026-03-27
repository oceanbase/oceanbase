/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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