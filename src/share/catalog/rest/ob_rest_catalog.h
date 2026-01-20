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
#ifndef _SHARE_OB_REST_CATALOG_H
#define _SHARE_OB_REST_CATALOG_H

#include "share/catalog/ob_external_catalog.h"
#include "share/catalog/ob_catalog_location_schema_provider.h"
#include "share/catalog/rest/client/ob_curl_rest_client.h"
#include "share/catalog/rest/requests/ob_rest_http_request.h"
#include "share/catalog/rest/responses/ob_rest_http_response.h"

namespace oceanbase
{
namespace share
{

class ObRestCatalog : public ObIExternalCatalog
{
using ObRestAuthType = ObRestCatalogProperties::ObRestAuthType;
public:
  explicit ObRestCatalog(common::ObIAllocator &allocator)
      : allocator_(allocator), rest_properties_(),
        client_(nullptr)
  {
  }

  ~ObRestCatalog();

  virtual int list_namespace_names(common::ObIArray<common::ObString> &ns_names) override;
  virtual int list_table_names(const common::ObString &ns_name,
                               const ObNameCaseMode case_mode,
                               common::ObIArray<common::ObString> &tb_names) override;
  virtual int fetch_namespace_schema(const uint64_t database_id,
                                     const common::ObString &ns_name,
                                     const ObNameCaseMode case_mode,
                                     share::schema::ObDatabaseSchema *&database_schema) override;
  virtual int fetch_lake_table_metadata(ObIAllocator &allocator,
                                        const uint64_t database_id,
                                        const uint64_t table_id,
                                        const common::ObString &ns_name,
                                        const common::ObString &tbl_name,
                                        const ObNameCaseMode case_mode,
                                        ObILakeTableMetadata *&table_metadata) override;
  virtual int fetch_latest_table_schema_version(const common::ObString &ns_name,
                                                const common::ObString &tbl_name,
                                                const ObNameCaseMode case_mode,
                                                int64_t &schema_version) override;
  virtual int fetch_table_statistics(ObIAllocator &allocator,
                                     sql::ObSqlSchemaGuard &sql_schema_guard,
                                     const ObILakeTableMetadata *table_metadata,
                                     const ObIArray<ObString> &partition_values,
                                     const ObIArray<ObString> &column_names,
                                     ObOptExternalTableStat *&external_table_stat,
                                     ObIArray<ObOptExternalColumnStat *> &external_table_column_stats) override;
private:
  virtual int do_init(const common::ObString &properties) override;
  int execute_warpper(ObRestHttpRequest &req, ObRestHttpResponse &rsp);

  common::ObIAllocator &allocator_;
  ObRestCatalogProperties rest_properties_;
  ObBaseRestClient *client_;

  const static constexpr char *ENDPOINT_CONFIG = "/v1/config";
  const static constexpr char *ENDPOINT_NAMESPACES = "/v1/%.*s%snamespaces";
  const static constexpr char *ENDPOINT_NAMESPACE = "/v1/%.*s%snamespaces/%.*s";
  const static constexpr char *ENDPOINT_TABLES = "/v1/%.*s%snamespaces/%.*s/tables";
  const static constexpr char *ENDPOINT_TABLE = "/v1/%.*s%snamespaces/%.*s/tables/%.*s";
  const static constexpr char *VENDED_CREDENTAIL_ENABLED_HEADER = "X-Iceberg-Access-Delegation";
  const static constexpr char *VENDED_CREDENTAIL_ENABLED_VALUE = "vended-credentials";
  DISALLOW_COPY_AND_ASSIGN(ObRestCatalog);
};
}
}

#endif /* _SHARE_OB_REST_CATALOG_H */