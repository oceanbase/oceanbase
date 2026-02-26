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

#ifndef OB_CATALOG_SQL_SERVICE_H
#define OB_CATALOG_SQL_SERVICE_H

#include "ob_ddl_sql_service.h"
#include "src/share/ob_dml_sql_splicer.h"

namespace oceanbase
{
namespace common
{
class ObString;
class ObISQLClient;
}
namespace share
{
namespace schema
{

class ObCatalogSchema;

class ObCatalogSqlService : public ObDDLSqlService
{
public:
  explicit ObCatalogSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObCatalogSqlService() {}

  int apply_new_schema(const ObCatalogSchema &schema,
                       ObISQLClient &sql_client,
                       const ObSchemaOperationType ddl_type,
                       const common::ObString &ddl_stmt_str);

  int add_schema(ObISQLClient &sql_client,
                 const ObCatalogSchema &schema);
  int alter_schema(ObISQLClient &sql_client,
                   const ObCatalogSchema &schema);
  int drop_schema(ObISQLClient &sql_client,
                  const ObCatalogSchema &schema);
  int grant_revoke_catalog(const ObCatalogPrivSortKey &catalog_priv_key,
                           const ObPrivSet priv_set,
                           const int64_t new_schema_version,
                           const ObString &ddl_stmt_str,
                           ObISQLClient &sql_client);

private:
  int gen_sql(common::ObSqlString &sql,
              common::ObSqlString &values,
              const ObCatalogSchema &schema);
  int gen_catalog_priv_dml(const uint64_t exec_tenant_id,
                           const ObCatalogPrivSortKey &catalog_priv_key,
                           const ObPrivSet &priv_set,
                           share::ObDMLSqlSplicer &dml);

private:
  DISALLOW_COPY_AND_ASSIGN(ObCatalogSqlService);
};



}
}
}

#endif // OB_CATALOG_SQL_SERVICE_H
