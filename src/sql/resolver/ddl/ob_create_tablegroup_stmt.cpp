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

#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

ObCreateTablegroupStmt::ObCreateTablegroupStmt()
: ObTablegroupStmt(stmt::T_CREATE_TABLEGROUP)
{
  create_tablegroup_arg_.if_not_exist_ = false;
}

ObCreateTablegroupStmt::ObCreateTablegroupStmt(common::ObIAllocator *name_pool)
: ObTablegroupStmt(name_pool, stmt::T_CREATE_TABLEGROUP)
{
}

ObCreateTablegroupStmt::~ObCreateTablegroupStmt()
{
}

void ObCreateTablegroupStmt::set_if_not_exists(bool if_not_exists)
{
  create_tablegroup_arg_.if_not_exist_ = if_not_exists;
}

void ObCreateTablegroupStmt::set_tenant_id(const uint64_t tenant_id)
{
  create_tablegroup_arg_.tablegroup_schema_.set_tenant_id(tenant_id);
}

obrpc::ObCreateTablegroupArg& ObCreateTablegroupStmt::get_create_tablegroup_arg()
{
  return create_tablegroup_arg_;
}
int ObCreateTablegroupStmt::set_primary_zone(const common::ObString &zone)
{
  return OB_SUCCESS; // ignore this (not support in 4.0)
}
int ObCreateTablegroupStmt::set_locality(const common::ObString &locality)
{
  return OB_SUCCESS; // ignore this (not support in 4.0)
}
