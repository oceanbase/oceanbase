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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_CONTEXT_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_CONTEXT_SQL_SERVICE_H_

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
class ObDMLSqlSplicer;
namespace schema
{
class ObContextSchema;

class ObContextSqlService : public ObDDLSqlService
{
public:
  ObContextSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObContextSqlService() {}

  virtual int insert_context(const ObContextSchema &context_schema,
                              common::ObISQLClient *sql_client,
                              const common::ObString *ddl_stmt_str = NULL);
  virtual int alter_context(const ObContextSchema &context_schema,
                               common::ObISQLClient *sql_client,
                               const common::ObString *ddl_stmt_str = NULL);
  virtual int delete_context(const uint64_t tenant_id,
                              const uint64_t context_id,
                              const ObString &ctx_namespace,
                              const int64_t new_schema_version,
                              const ObContextType &type,
                              common::ObISQLClient *sql_client,
                              const common::ObString *ddl_stmt_str = NULL);
  virtual int drop_context(const ObContextSchema &context_schema,
                            const int64_t new_schema_version,
                            common::ObISQLClient *sql_client,
                            bool &need_clean,
                            const common::ObString *ddl_stmt_str = NULL);
private:
  int add_context(common::ObISQLClient &sql_client, const ObContextSchema &context_schema,
                   const bool only_history = false);
  int format_dml_sql(const ObContextSchema &context_schema,
                     ObDMLSqlSplicer &dml,
                     bool &is_history);
private:
  DISALLOW_COPY_AND_ASSIGN(ObContextSqlService);
};

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_CONTEXT_SQL_SERVICE_H_
