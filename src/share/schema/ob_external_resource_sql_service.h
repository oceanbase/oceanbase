/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_EXTERNAL_RESOURCE_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_EXTERNAL_RESOURCE_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"

namespace oceanbase
{

namespace common
{

class ObString;
class ObISQLClient;

}

namespace obrpc
{

class ObCreateExternalResourceArg;

}
namespace share
{

namespace schema
{

class ObSimpleExternalResourceSchema;

class ObExternalResourceSqlService final: public ObDDLSqlService
{
public:
  ObExternalResourceSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service)
  {  }

virtual ~ObExternalResourceSqlService() = default;

int create_external_resource(const ObSimpleExternalResourceSchema &new_schema,
                             const ObString &content,
                             const ObString &comment,
                             const ObString &ddl_stmt,
                             common::ObISQLClient &sql_client);

int drop_external_resource(const ObSimpleExternalResourceSchema &schema,
                           const ObString &ddl_stmt,
                           common::ObISQLClient &sql_client);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExternalResourceSqlService);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_EXTERNAL_RESOURCE_SQL_SERVICE_H_
