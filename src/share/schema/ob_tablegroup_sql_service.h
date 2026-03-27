/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_TABLEGROUP_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_TABLEGROUP_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"

namespace oceanbase
{
namespace share
{
class ObDMLSqlSplicer;
namespace schema
{
class ObTablegroupSchema;

class ObTablegroupSqlService : public ObDDLSqlService
{
public:
  ObTablegroupSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObTablegroupSqlService() {}
  virtual int insert_tablegroup(const ObTablegroupSchema &tablegroup_schema,
                                common::ObISQLClient &sql_client,
                                const common::ObString *ddl_stmt_str = NULL);
  virtual int delete_tablegroup(
          const ObTablegroupSchema &tablegroup_schema,
          const int64_t new_schema_version,
          common::ObISQLClient &sql_client,
          const common::ObString *ddl_stmt_str = NULL);
  virtual int update_tablegroup(ObTablegroupSchema &new_schema,
                                common::ObISQLClient &sql_client,
                                const common::ObString *ddl_stmt_str = NULL);
private:
  int add_tablegroup(common::ObISQLClient &sql_client,
                     const share::schema::ObTablegroupSchema &tablegroup,
                     const bool only_history);

  int gen_tablegroup_dml(const uint64_t exec_tenant_id,
                         const share::schema::ObTablegroupSchema &tablegroup_schema,
                         share::ObDMLSqlSplicer &dml);

  DISALLOW_COPY_AND_ASSIGN(ObTablegroupSqlService);
};


} //end of namespace share
} //end of namespace schema
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_TABLEGROUP_SQL_SERVICE_H_
