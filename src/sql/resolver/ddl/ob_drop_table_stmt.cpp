/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ddl/ob_drop_table_stmt.h"

namespace oceanbase
{
using namespace common;
using obrpc::ObTableItem;
namespace sql
{
ObDropTableStmt::ObDropTableStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_DROP_TABLE),
    drop_table_arg_(),
    is_view_stmt_(false)
{
}

ObDropTableStmt::ObDropTableStmt()
  : ObDDLStmt(stmt::T_DROP_TABLE),
    drop_table_arg_(),
    is_view_stmt_(false)
{
}

ObDropTableStmt::~ObDropTableStmt()
{
}

int ObDropTableStmt::add_table_item(const ObTableItem &table_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(drop_table_arg_.tables_.push_back(table_item))) {
    LOG_WARN("failed to add table item!", K(table_item), K(ret));
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
