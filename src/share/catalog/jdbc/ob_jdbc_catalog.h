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

#ifndef OCEANBASE_SHARE_CATALOG_JDBC_OB_JDBC_CATALOG_H_
#define OCEANBASE_SHARE_CATALOG_JDBC_OB_JDBC_CATALOG_H_

#include "share/catalog/ob_catalog_properties.h"
#include "share/catalog/ob_external_catalog.h"
#include "share/catalog/jdbc/ob_jdbc_connection_pool.h"

namespace oceanbase
{
namespace sql
{
namespace jdbc
{
struct ObJdbcCharsetContext;
}
}
namespace plugin
{
class ObJdbcCatalogPlugin;
}
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}

class ObJdbcCatalog final : public ObIExternalCatalog
{
public:
  ObJdbcCatalog(common::ObIAllocator &allocator,
                schema::ObSchemaGetterGuard &schema_guard,
                int64_t catalog_schema_version)
      : allocator_(allocator),
        schema_guard_(schema_guard),
        plugin_name_(),
        catalog_schema_version_(catalog_schema_version)
  { }
  virtual ~ObJdbcCatalog();

  int list_namespace_names(common::ObIArray<common::ObString> &ns_names) override;
  int list_table_names(const common::ObString &db_name,
                       const ObNameCaseMode case_mode,
                       common::ObIArray<common::ObString> &tbl_names) override;
  int fetch_namespace_schema(const uint64_t database_id,
                             const common::ObString &ns_name,
                             const ObNameCaseMode case_mode,
                             share::schema::ObDatabaseSchema *&database_schema) override;
  int fetch_lake_table_metadata(common::ObIAllocator &allocator,
                                const uint64_t database_id,
                                const uint64_t table_id,
                                const common::ObString &ns_name,
                                const common::ObString &tbl_name,
                                const ObNameCaseMode case_mode,
                                ObILakeTableMetadata *&table_metadata) override;
  int fetch_latest_table_schema_version(const common::ObString &ns_name,
                                        const common::ObString &tbl_name,
                                        const ObNameCaseMode case_mode,
                                        int64_t &schema_version) override;

private:
  int do_init(const common::ObString &properties) override;
  int get_charset_context(const common::ObCompatibilityMode target_mode,
                          sql::jdbc::ObJdbcCharsetContext &charset_context);

private:
  common::ObIAllocator &allocator_;
  schema::ObSchemaGetterGuard &schema_guard_;
  common::ObString plugin_name_;
  int acquire(ObJdbcConnectionGuard &guard);
  int64_t catalog_schema_version_;
  DISALLOW_COPY_AND_ASSIGN(ObJdbcCatalog);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_JDBC_OB_JDBC_CATALOG_H_
