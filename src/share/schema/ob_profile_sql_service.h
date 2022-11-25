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

#ifndef OB_PROFILE_SQL_SERVICE_H
#define OB_PROFILE_SQL_SERVICE_H

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

class ObProfileSchema;

class ObProfileSqlService : public ObDDLSqlService
{
public:
  explicit ObProfileSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObProfileSqlService() {}

  int apply_new_schema(const ObProfileSchema &schema,
                       ObISQLClient &sql_client,
                       ObSchemaOperationType ddl_type,
                       const common::ObString &ddl_stmt_str);

  int add_schema(ObISQLClient &sql_client,
                 const ObProfileSchema &schema);
  int alter_schema(ObISQLClient &sql_client,
                   const ObProfileSchema &schema);
  int drop_schema(ObISQLClient &sql_client,
                  const ObProfileSchema &schema);

private:
  int gen_sql(common::ObSqlString &sql,
              common::ObSqlString &values,
              const ObProfileSchema &schema);

private:
  DISALLOW_COPY_AND_ASSIGN(ObProfileSqlService);
};



}
}
}

#endif // OB_PROFILE_SQL_SERVICE_H
