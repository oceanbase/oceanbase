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

#include "sql/resolver/ddl/ob_create_tenant_stmt.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

ObCreateTenantStmt::ObCreateTenantStmt(common::ObIAllocator *name_pool)
  : ObDDLStmt(name_pool, stmt::T_CREATE_TENANT),
    create_tenant_arg_(),
    sys_var_nodes_()
{
}

ObCreateTenantStmt::ObCreateTenantStmt()
  : ObDDLStmt(stmt::T_CREATE_TENANT),
    create_tenant_arg_(),
    sys_var_nodes_()
{
}

ObCreateTenantStmt::~ObCreateTenantStmt()
{
}

void ObCreateTenantStmt::print(FILE *fp, int32_t level, int32_t index)
{
  UNUSED(index);
  UNUSED(fp);
  UNUSED(level);
}

int ObCreateTenantStmt::add_resource_pool(const common::ObString &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_tenant_arg_.pool_list_.push_back(res))) {
    LOG_WARN("save string failed", K(ret), K(res));
  }
  return ret;
}

int ObCreateTenantStmt::set_resource_pool(const ObIArray<ObString> &res)
{
  int ret = OB_SUCCESS;
  create_tenant_arg_.pool_list_.reset();
  if (OB_FAIL(create_tenant_arg_.pool_list_.assign(res))) {
    LOG_WARN("save string array failed", K(ret), K(res));
  }
  return ret;
}

int ObCreateTenantStmt::add_zone(const common::ObString &zone)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_tenant_arg_.tenant_schema_.add_zone(zone))) {
    LOG_WARN("save string failed", K(ret), K(zone));
  }
  return ret;
}

int ObCreateTenantStmt::set_comment(const common::ObString &comment)
{
  return create_tenant_arg_.tenant_schema_.set_comment(comment);
}

int ObCreateTenantStmt::set_locality(const common::ObString &locality)
{
  return create_tenant_arg_.tenant_schema_.set_locality(locality);
}

void ObCreateTenantStmt::set_primary_zone(const common::ObString &zone)
{
  create_tenant_arg_.tenant_schema_.set_primary_zone(zone);
}

void ObCreateTenantStmt::set_enable_arbitration_service(const bool enable_arbitration_service)
{
  create_tenant_arg_.tenant_schema_.set_arbitration_service_status(
      enable_arbitration_service
      ? share::ObArbitrationServiceStatus(share::ObArbitrationServiceStatus::ENABLING)
      : share::ObArbitrationServiceStatus(share::ObArbitrationServiceStatus::DISABLED));
}

void ObCreateTenantStmt::set_if_not_exist(const bool if_not_exist)
{
  create_tenant_arg_.if_not_exist_ = if_not_exist;
}

void ObCreateTenantStmt::set_tenant_name(const ObString &tenant_name)
{
  create_tenant_arg_.tenant_schema_.set_tenant_name(tenant_name);
}

void ObCreateTenantStmt::set_charset_type(const common::ObCharsetType type)
{
  create_tenant_arg_.tenant_schema_.set_charset_type(type);
}

void ObCreateTenantStmt::set_collation_type(const common::ObCollationType type)
{
  create_tenant_arg_.tenant_schema_.set_collation_type(type);
}

int ObCreateTenantStmt::set_default_tablegroup_name(const common::ObString &tablegroup_name)
{
  return create_tenant_arg_.tenant_schema_.set_default_tablegroup_name(tablegroup_name);
}

void ObCreateTenantStmt::set_create_standby_tenant()
{
  create_tenant_arg_.is_creating_standby_ = true;
}

void ObCreateTenantStmt::set_log_restore_source(const common::ObString &log_restore_source)
{
  create_tenant_arg_.log_restore_source_ = log_restore_source;
}

} /* sql */
} /* oceanbase */
