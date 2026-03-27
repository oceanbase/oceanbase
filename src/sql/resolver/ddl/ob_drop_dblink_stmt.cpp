/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_drop_dblink_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace sql
{
ObDropDbLinkStmt::ObDropDbLinkStmt()
   : ObDDLStmt(stmt::T_DROP_DBLINK),
     drop_dblink_arg_()
{}

ObDropDbLinkStmt::ObDropDbLinkStmt(common::ObIAllocator *name_pool)
  : ObDDLStmt(name_pool, stmt::T_DROP_DBLINK),
    drop_dblink_arg_()
{}

ObDropDbLinkStmt::~ObDropDbLinkStmt()
{
}

}//namespace sql
}//namespace oceanbase
