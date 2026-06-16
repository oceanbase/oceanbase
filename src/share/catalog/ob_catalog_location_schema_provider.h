/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
                              common::ObString &access_info,
                              uint64_t &location_id,
                              common::ObString &sub_path) const;
  // get access info by location object id, no session/privilege check needed
  int get_access_info_by_id(const uint64_t tenant_id,
                            const uint64_t location_id,
                            common::ObString &access_info) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCatalogLocationSchemaProvider);
  schema::ObSchemaGetterGuard &schema_guard_;
};

} // namespace share
} // namespace oceanbase

#endif // OB_CATALOG_LOCATION_SCHEMA_PROVIDER_H
