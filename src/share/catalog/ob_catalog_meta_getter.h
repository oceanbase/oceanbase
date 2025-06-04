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

#ifndef __SHARE_OB_CATALOG_META_GETTER_H__
#define __SHARE_OB_CATALOG_META_GETTER_H__

#include "share/catalog/ob_external_catalog.h"

namespace oceanbase
{
namespace share
{

class ObCatalogMetaGetter final : public ObICatalogMetaGetter
{
public:
  ObCatalogMetaGetter(ObSchemaGetterGuard &schema_getter_guard, ObIAllocator &allocator)
      : schema_getter_guard_(schema_getter_guard), allocator_(allocator)
  {
  }

  ~ObCatalogMetaGetter() = default;

  int list_namespace_names(const uint64_t tenant_id, const uint64_t catalog_id, common::ObIArray<common::ObString> &ns_names) override;
  int list_table_names(const uint64_t tenant_id,
                       const uint64_t catalog_id,
                       const common::ObString &ns_name,
                       const ObNameCaseMode case_mode,
                       common::ObIArray<common::ObString> &tbl_names) override;
  int fetch_namespace_schema(const uint64_t tenant_id,
                             const uint64_t catalog_id,
                             const common::ObString &ns_name,
                             const ObNameCaseMode case_mode,
                             share::schema::ObDatabaseSchema &database_schema) override;
  int fetch_table_schema(const uint64_t tenant_id,
                         const uint64_t catalog_id,
                         const common::ObString &ns_name,
                         const common::ObString &tbl_name,
                         const ObNameCaseMode case_mode,
                         share::schema::ObTableSchema &table_schema) override;

  int fetch_basic_table_info(const uint64_t tenant_id,
                             const uint64_t catalog_id,
                             const common::ObString &ns_name,
                             const common::ObString &tbl_name,
                             const ObNameCaseMode case_mode,
                             ObCatalogBasicTableInfo &table_info);

private:
  ObSchemaGetterGuard &schema_getter_guard_;
  ObIAllocator &allocator_;

  int get_catalog_(const uint64_t tenant_id, const uint64_t catalog_id, ObIExternalCatalog *&catalog);
};

} // namespace share
} // namespace oceanbase

#endif // __SHARE_OB_CATALOG_META_GETTER_H__
