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

#include "sql/resolver/ddl/ob_modify_tenant_stmt.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

ObModifyTenantSpecialOption::ObModifyTenantSpecialOption()
  : progressive_merge_num_(-1)
{
}

ObModifyTenantStmt::ObModifyTenantStmt(common::ObIAllocator *name_pool)
  : ObDDLStmt(name_pool, stmt::T_MODIFY_TENANT),
    for_current_tenant_(false),
    modify_tenant_arg_(),
    sys_var_nodes_(),
    special_option_()
{
}

ObModifyTenantStmt::ObModifyTenantStmt()
  : ObDDLStmt(stmt::T_MODIFY_TENANT),
    for_current_tenant_(false),
    modify_tenant_arg_(),
    sys_var_nodes_(),
    special_option_()
{
}

ObModifyTenantStmt::~ObModifyTenantStmt()
{
}

void ObModifyTenantStmt::print(FILE *fp, int32_t level, int32_t index)
{
  UNUSED(index);
  UNUSED(fp);
  UNUSED(level);
}

int ObModifyTenantStmt::add_resource_pool(const common::ObString &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(modify_tenant_arg_.pool_list_.push_back(res))) {
    LOG_WARN("save string failed", K(ret), K(res));
  }
  return ret;
}

int ObModifyTenantStmt::add_zone(const common::ObString &zone)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(modify_tenant_arg_.tenant_schema_.add_zone(zone))) {
    LOG_WARN("save string failed", K(ret), K(zone));
  }
  return ret;
}

int ObModifyTenantStmt::set_comment(const common::ObString &comment)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(modify_tenant_arg_.tenant_schema_.set_comment(comment))) {
    LOG_WARN("fail to set comment", K(ret), K(comment));
  }
  return ret;
}

int ObModifyTenantStmt::set_locality(const common::ObString &locality)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(modify_tenant_arg_.tenant_schema_.set_locality(locality))) {
    LOG_WARN("fail to set locality", K(ret), K(locality));
  }
  return ret;
}

void ObModifyTenantStmt::set_primary_zone(const common::ObString &zone)
{
  modify_tenant_arg_.tenant_schema_.set_primary_zone(zone);
}

void ObModifyTenantStmt::set_tenant_name(const ObString &tenant_name)
{
  modify_tenant_arg_.tenant_schema_.set_tenant_name(tenant_name);
}

void ObModifyTenantStmt::set_charset_type(const common::ObCharsetType type)
{
  modify_tenant_arg_.tenant_schema_.set_charset_type(type);
}

void ObModifyTenantStmt::set_enable_arbitration_service(const bool enable_arbitration_service)
{
  modify_tenant_arg_.tenant_schema_.set_arbitration_service_status(
      enable_arbitration_service
      ? share::ObArbitrationServiceStatus(share::ObArbitrationServiceStatus::ENABLING)
      : share::ObArbitrationServiceStatus(share::ObArbitrationServiceStatus::DISABLING));
}

void ObModifyTenantStmt::set_collation_type(const common::ObCollationType type)
{
  modify_tenant_arg_.tenant_schema_.set_collation_type(type);
}

int ObModifyTenantStmt::set_default_tablegroup_name(const common::ObString &tablegroup_name)
{
  return modify_tenant_arg_.tenant_schema_.set_default_tablegroup_name(tablegroup_name);
}

void ObModifyTenantStmt::set_new_tenant_name(const ObString &new_tenant_name)
{
  modify_tenant_arg_.new_tenant_name_ = new_tenant_name;
}

} /* sql */
} /* oceanbase */
