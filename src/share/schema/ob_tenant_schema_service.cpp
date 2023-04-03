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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_tenant_schema_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/schema/ob_multi_version_schema_service.h"


namespace oceanbase
{
namespace share
{
namespace schema
{

int ObTenantSchemaService::mtl_init(ObTenantSchemaService *&tenant_schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_schema_service is null", K(ret));
  } else {
    tenant_schema_service->schema_service_ = &GSCHEMASERVICE;
  }
  return ret;
}

void ObTenantSchemaService::destroy()
{
  schema_service_ = nullptr;
}

}
}
}