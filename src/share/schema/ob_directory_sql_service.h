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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_DIRECTORY_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_DIRECTORY_SQL_SERVICE_H_

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
class ObDirectorySchema;

class ObDirectorySqlService : public ObDDLSqlService
{
public:
  explicit ObDirectorySqlService(ObSchemaService &schema_service);
  virtual ~ObDirectorySqlService();

  ObDirectorySqlService(const ObDirectorySqlService&) = delete;
  ObDirectorySqlService &operator=(const ObDirectorySqlService&) = delete;

  int apply_new_schema(const ObDirectorySchema &schema,
                       ObISQLClient &sql_client,
                       ObSchemaOperationType ddl_type,
                       const common::ObString &ddl_stmt_str);
private:
  int add_schema(ObISQLClient &sql_client, const ObDirectorySchema &schema);
  int alter_schema(ObISQLClient &sql_client, const ObDirectorySchema &schema);
  int drop_schema(ObISQLClient &sql_client, const ObDirectorySchema &schema);
  int gen_sql(common::ObSqlString &sql, common::ObSqlString &values, const ObDirectorySchema &schema);
private:
  static constexpr int THE_SYS_TABLE_IDX = 0;
  static constexpr int THE_HISTORY_TABLE_IDX = 1;
  static const char *DIRECTORY_TABLES[2];
};
} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_DIRECTORY_SQL_SERVICE_H_
