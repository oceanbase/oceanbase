/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SYNONYM_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SYNONYM_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"

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
class ObSynonymInfo;

class ObSynonymSqlService : public ObDDLSqlService
{
public:
  ObSynonymSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObSynonymSqlService() {}

  virtual int insert_synonym(const ObSynonymInfo &synonym_info,
                             common::ObISQLClient *sql_client,
                             const common::ObString *ddl_stmt_str = NULL);
  virtual int replace_synonym(const ObSynonymInfo &synonym_info,
                             common::ObISQLClient *sql_client,
                             const common::ObString *ddl_stmt_str = NULL);
  virtual int delete_synonym(const uint64_t tenant_id,
                             const uint64_t database_id,
                             const uint64_t synonym_id,
                             const int64_t new_schema_version,
                             common::ObISQLClient *sql_client,
                             const common::ObString *ddl_stmt_str = NULL);
private:
  int add_synonym(common::ObISQLClient &sql_client, const ObSynonymInfo &synonym_info,
                  const bool only_history = false);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSynonymSqlService);
};


} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_SYNONYM_SQL_SERVICE_H_
