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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_TENANT_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_TENANT_SQL_SERVICE_H_

#include "share/schema/ob_ddl_sql_service.h"

namespace oceanbase
{
namespace common
{
class ObISQlClient;
}
namespace share
{
namespace schema
{
struct ObSchemaOperation;
class ObTenantSchema;

class ObTenantSqlService : public ObDDLSqlService
{
public:
  ObTenantSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObTenantSqlService() {}

  virtual int insert_tenant(const ObTenantSchema &tenant_schema,
                            const ObSchemaOperationType op,
                            common::ObISQLClient &sql_client,
                            const common::ObString *ddl_stmt_str = NULL);
  virtual int alter_tenant(const ObTenantSchema &tenant_schema,
                           common::ObISQLClient &sql_client,
                           const ObSchemaOperationType op,
                           const common::ObString *ddl_stmt_str = NULL);
  virtual int rename_tenant(const ObTenantSchema &tenant_schema,
                            common::ObISQLClient &sql_client,
                            const common::ObString *ddl_stmt_str = NULL);
  virtual int delay_to_drop_tenant(const ObTenantSchema &tenant_schema,
                                   ObMySQLTransaction &trans,
                                   const ObString *ddl_stmt_str);
  virtual int drop_tenant_to_recyclebin(const ObTenantSchema &tenant_schema,
                                        common::ObISQLClient &sql_client,
                                        const ObSchemaOperationType op,
                                        const common::ObString *ddl_stmt_str = NULL);
  virtual int delete_tenant(const uint64_t tenant_id,
                            const int64_t new_schema_version,
                            common::ObISQLClient &sql_client,
                            const common::ObString *ddl_stmt_str = NULL);
private:
  int replace_tenant(const ObTenantSchema &tenant_schema,
                     const ObSchemaOperationType op,
                     common::ObISQLClient &sql_client,
                     const common::ObString *ddl_stmt_str);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantSqlService);
};


} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_TENANT_SQL_SERVICE_H_
