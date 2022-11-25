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

#include "sql/resolver/ddl/ob_optimize_stmt.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
using namespace common;
using obrpc::ObTableItem;
namespace sql
{
ObOptimizeTableStmt::ObOptimizeTableStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_OPTIMIZE_TABLE), optimize_table_arg_()
{
}

ObOptimizeTableStmt::ObOptimizeTableStmt()
    : ObDDLStmt(stmt::T_OPTIMIZE_TABLE), optimize_table_arg_()
{
}

int ObOptimizeTableStmt::add_table_item(const ObTableItem &table_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(optimize_table_arg_.tables_.push_back(table_item))) {
    LOG_WARN("failed to add table item", K(ret), K(table_item));
  }
  return ret;
}

ObOptimizeTenantStmt::ObOptimizeTenantStmt(ObIAllocator *name_pool)
  : ObDDLStmt(name_pool, stmt::T_OPTIMIZE_TENANT), optimize_tenant_arg_()
{
}

ObOptimizeTenantStmt::ObOptimizeTenantStmt()
  : ObDDLStmt(stmt::T_OPTIMIZE_TENANT), optimize_tenant_arg_()
{
}

void ObOptimizeTenantStmt::set_tenant_name(const ObString &tenant_name)
{
  optimize_tenant_arg_.tenant_name_ = tenant_name;
}

ObOptimizeAllStmt::ObOptimizeAllStmt(ObIAllocator *name_pool)
  : ObDDLStmt(name_pool, stmt::T_OPTIMIZE_ALL), optimize_all_arg_()
{
}

ObOptimizeAllStmt::ObOptimizeAllStmt()
  : ObDDLStmt(stmt::T_OPTIMIZE_ALL), optimize_all_arg_()
{
}

}  // end namespace sql
}  // end namespace oceanbase
