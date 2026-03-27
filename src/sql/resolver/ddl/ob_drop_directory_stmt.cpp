/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/resolver/ddl/ob_drop_directory_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{
ObDropDirectoryStmt::ObDropDirectoryStmt()
  : ObDDLStmt(stmt::T_DROP_DIRECTORY),
    arg_()
{
}

ObDropDirectoryStmt::ObDropDirectoryStmt(common::ObIAllocator *name_pool)
  : ObDDLStmt(name_pool, stmt::T_DROP_DIRECTORY),
    arg_()
{
}

ObDropDirectoryStmt::~ObDropDirectoryStmt()
{
}
} // namespace sql
} // namespace oceanbase