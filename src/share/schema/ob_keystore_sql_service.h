/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_KEYSTORE_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_KEYSTORE_SQL_SERVICE_H_
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
class ObKeystoreSchema;
class ObKeystoreSqlService : public ObDDLSqlService
{
public:
  ObKeystoreSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObKeystoreSqlService() {}
  virtual int create_keystore(const ObKeystoreSchema &schema,
      common::ObISQLClient &sql_client,
      const common::ObString *ddl_stmt_str = NULL);
  virtual int alter_keystore(const ObKeystoreSchema &keystore_schema,
                             common::ObISQLClient &sql_client,
                             const common::ObString *ddl_stmt_str);
private:
  int add_keystore(common::ObISQLClient &sql_client,
      const ObKeystoreSchema &schema,
      const bool only_history = false);
  int update_keystore(common::ObISQLClient &sql_client,
      const ObKeystoreSchema &schema);
private:
  DISALLOW_COPY_AND_ASSIGN(ObKeystoreSqlService);
};
} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
#endif //OCEANBASE_SHARE_SCHEMA_OB_KEYSTORE_SQL_SERVICE_H_