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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_RLS_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_RLS_SQL_SERVICE_H_

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
class ObRlsPolicySchema;
class ObRlsGroupSchema;
class ObRlsContextSchema;

class ObRlsSqlService : public ObDDLSqlService
{
public:
  ObRlsSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObRlsSqlService() {}

  template<class schemaT>
  int apply_new_schema(const schemaT &schema,
                       const int64_t new_schema_version,
                       ObISQLClient &sql_client,
                       ObSchemaOperationType ddl_type,
                       const common::ObString &ddl_stmt_str);
  int drop_rls_sec_column(const ObRlsPolicySchema &policy_schema,
                          const ObRlsSecColumnSchema &column_schema,
                          const int64_t new_schema_version,
                          ObISQLClient &sql_client,
                          const common::ObString &ddl_stmt_str);

private:
  template<class schemaT>
  int add_schema(common::ObISQLClient &sql_client,
                 const schemaT &schema,
                 const int64_t new_schema_version);
  template<class schemaT>
  int drop_schema(common::ObISQLClient &sql_client,
                  const schemaT &schema,
                  const int64_t new_schema_version);
  int alter_schema(common::ObISQLClient &sql_client,
                   const ObRlsPolicySchema &schema,
                   const int64_t new_schema_version);
  int add_schema_cascade(common::ObISQLClient &sql_client,
                         const ObRlsPolicySchema &schema,
                         const int64_t new_schema_version);
  int drop_schema_cascade(common::ObISQLClient &sql_client,
                          const ObRlsPolicySchema &schema,
                          const int64_t new_schema_version);

  template<class schemaT>
  int gen_sql(common::ObSqlString &sql, common::ObSqlString &values, const schemaT &schema);

  template<class schemaT>
  void fill_schema_operation(const schemaT &schema,
                             const int64_t new_schema_version,
                             ObSchemaOperationType ddl_type,
                             ObSchemaOperation &operation,
                             const common::ObString &ddl_stmt_str);

  static constexpr int THE_SYS_TABLE_IDX = 0;
  static constexpr int THE_HISTORY_TABLE_IDX = 1;
  static const char *RLS_POLICY_TABLES[2];
  static const char *RLS_GROUP_TABLES[2];
  static const char *RLS_CONTEXT_TABLES[2];
  static const char *RLS_SEC_COLUMN_TABLES[2];
  static const char *RLS_CS_ATTRIBUTE_TABLES[2];
private:
  DISALLOW_COPY_AND_ASSIGN(ObRlsSqlService);
};

template<class schemaT>
int ObRlsSqlService::apply_new_schema(const schemaT &schema,
                                      const int64_t new_schema_version,
                                      ObISQLClient &sql_client,
                                      ObSchemaOperationType ddl_type,
                                      const ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;

  switch (ddl_type) {
  case OB_DDL_CREATE_RLS_GROUP:
  case OB_DDL_CREATE_RLS_CONTEXT:
  case OB_DDL_RLS_POLICY_ADD_ATTRIBUTE:
    ret = add_schema(sql_client, schema, new_schema_version);
    break;
  case OB_DDL_DROP_RLS_GROUP:
  case OB_DDL_DROP_RLS_CONTEXT:
  case OB_DDL_RLS_POLICY_DROP_ATTRIBUTE:
    ret = drop_schema(sql_client, schema, new_schema_version);
    break;
  default:
    ret = OB_NOT_SUPPORTED;
    SHARE_SCHEMA_LOG(WARN, "not support ddl type", K(ret), K(ddl_type));
  }

  if (OB_FAIL(ret)) {
    SHARE_SCHEMA_LOG(WARN, "exec sql on inner table failed", K(ret), K(ddl_type));
  } else {
    ObSchemaOperation operation;
    fill_schema_operation(schema, new_schema_version, ddl_type, operation, ddl_stmt_str);
    if (OB_FAIL(log_operation(operation, sql_client))) {
      SHARE_SCHEMA_LOG(WARN, "Failed to log operation", K(ret));
    }
  }

  return ret;
}

template<class schemaT>
void ObRlsSqlService::fill_schema_operation(const schemaT &schema,
                                            const int64_t new_schema_version,
                                            ObSchemaOperationType ddl_type,
                                            ObSchemaOperation &operation,
                                            const ObString &ddl_stmt_str)
{
  operation.tenant_id_ = schema.get_tenant_id();
  operation.table_id_ = schema.get_operation_table_id();
  operation.op_type_ = ddl_type;
  operation.schema_version_ = new_schema_version;
  operation.ddl_stmt_str_ = ddl_stmt_str;
}


} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_RLS_SQL_SERVICE_H_
