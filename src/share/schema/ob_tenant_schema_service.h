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