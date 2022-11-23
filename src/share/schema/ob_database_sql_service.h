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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_DATABASE_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_DATABASE_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}
namespace share
{
namespace schema
{
class ObDatabaseSchema;

class ObDatabaseSqlService : public ObDDLSqlService
{
public:
  ObDatabaseSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObDatabaseSqlService() {}

  virtual int insert_database(const ObDatabaseSchema &database_schema,
                              common::ObISQLClient &sql_client,
                              const common::ObString *ddl_stmt_str = NULL,
                              const bool is_only_history = false);
  virtual int update_database(const ObDatabaseSchema &database_schema,
                              common::ObISQLClient &sql_client,
                              const ObSchemaOperationType op_type,
                              const common::ObString *ddl_stmt_str = NULL);
  virtual int delete_database(const ObDatabaseSchema &db_schema,
                              const int64_t new_schema_version,
                              common::ObISQLClient &sql_client,
                              const common::ObString *ddl_stmt_str = NULL);


  DISALLOW_COPY_AND_ASSIGN(ObDatabaseSqlService);
};

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_DATABASE_SQL_SERVICE_H_
