/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_CATALOG_LOCATION_SCHEMA_PROVIDER_H
#define OB_CATALOG_LOCATION_SCHEMA_PROVIDER_H

#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace share
{

class ObCatalogLocationSchemaProvider
{
public:
  explicit ObCatalogLocationSchemaProvider(schema::ObSchemaGetterGuard &schema_guard);
  int get_access_info_by_path(ObIAllocator &allocator,
                              const uint64_t tenant_id,
                              const common::ObString &access_path,
                              common::ObString &access_info) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCatalogLocationSchemaProvider);
  schema::ObSchemaGetterGuard &schema_guard_;
};

} // namespace share
} // namespace oceanbase

#endif // OB_CATALOG_LOCATION_SCHEMA_PROVIDER_H
