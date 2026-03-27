/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OB_TENANT_SCHEMA_SERVICE_H
#define OCEANBASE_OB_TENANT_SCHEMA_SERVICE_H

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObTenantSchemaService
{
public:
  static int mtl_init(ObTenantSchemaService *&tenant_schema_service);

  ObTenantSchemaService()
    : schema_service_(nullptr)
  {
  }
  ~ObTenantSchemaService() {}

  void destroy();
  ObMultiVersionSchemaService *get_schema_service() { return schema_service_; }

private:
  ObMultiVersionSchemaService *schema_service_;
};

}
}
}

#endif