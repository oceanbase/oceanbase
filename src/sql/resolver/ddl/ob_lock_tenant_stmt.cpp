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

#include "sql/resolver/ddl/ob_lock_tenant_stmt.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

ObLockTenantStmt::ObLockTenantStmt(common::ObIAllocator *name_pool)
  : ObDDLStmt(name_pool, stmt::T_LOCK_TENANT),
    lock_tenant_arg_()
{
}

ObLockTenantStmt::ObLockTenantStmt()
  : ObDDLStmt(stmt::T_LOCK_TENANT),
    lock_tenant_arg_()
{
}

ObLockTenantStmt::~ObLockTenantStmt()
{
}

void ObLockTenantStmt::print(FILE *fp, int32_t level, int32_t index)
{
  UNUSED(index);
  UNUSED(fp);
  UNUSED(level);
}


void ObLockTenantStmt::set_tenant_name(const ObString &tenant_name)
{
  lock_tenant_arg_.tenant_name_ = tenant_name;
}

void ObLockTenantStmt::set_locked(const bool is_locked)
{
  lock_tenant_arg_.is_locked_ = is_locked;
}

} /* sql */
} /* oceanbase */
