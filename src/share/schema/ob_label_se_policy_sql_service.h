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

#ifndef OB_LABEL_SE_POLICY_SQL_SERVICE_H_
#define OB_LABEL_SE_POLICY_SQL_SERVICE_H_

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
class ObLabelSePolicySchema;

class ObLabelSePolicySqlService : public ObDDLSqlService
{
public:

  ObLabelSePolicySqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObLabelSePolicySqlService() {}

  template<class schemaT>
  int apply_new_schema(const schemaT &schema,
                       ObISQLClient &sql_client,
                       ObSchemaOperationType ddl_type,
                       const common::ObString &ddl_stmt_str);

private:

  template<class schemaT>
  int add_schema(common::ObISQLClient &sql_client,
                 const schemaT &schema);

  template<class schemaT>
  int alter_schema(common::ObISQLClient &sql_client,
                 const schemaT &schema);

  template<class schemaT>
  int drop_schema(common::ObISQLClient &sql_client,
                 const schemaT &schema);

  template<class schemaT>
  int gen_sql(common::ObSqlString &sql, common::ObSqlString &values, const schemaT &schema);

  template<class schemaT>
  void fill_schema_operation(const schemaT &schema,
                             ObSchemaOperationType ddl_type,
                             ObSchemaOperation &operation,
                             const common::ObString &ddl_stmt_str);
private:
  DISALLOW_COPY_AND_ASSIGN(ObLabelSePolicySqlService);
};


template<class schemaT>
int ObLabelSePolicySqlService::apply_new_schema(const schemaT &schema,
                                                ObISQLClient &sql_client,
                                                ObSchemaOperationType ddl_type,
                                                const ObString &ddl_stmt_str)
{
  int ret = OB_SUCCESS;

  switch (ddl_type) {
  case OB_DDL_CREATE_LABEL_SE_POLICY:
  case OB_DDL_CREATE_LABEL_SE_LEVEL:
  case OB_DDL_CREATE_LABEL_SE_LABEL:
  case OB_DDL_CREATE_LABEL_SE_USER_LEVELS:
    ret = add_schema(sql_client, schema);
    break;
  case OB_DDL_ALTER_LABEL_SE_POLICY:
  case OB_DDL_ALTER_LABEL_SE_LEVEL:
  case OB_DDL_ALTER_LABEL_SE_LABEL:
  case OB_DDL_ALTER_LABEL_SE_USER_LEVELS:
    ret = alter_schema(sql_client, schema);
    break;
  case OB_DDL_DROP_LABEL_SE_POLICY:
  case OB_DDL_DROP_LABEL_SE_LEVEL:
  case OB_DDL_DROP_LABEL_SE_LABEL:
  case OB_DDL_DROP_LABEL_SE_USER_LEVELS:
    ret = drop_schema(sql_client, schema);
    break;
  default:
    ret = OB_NOT_SUPPORTED;
    SHARE_SCHEMA_LOG(WARN, "not support ddl type", K(ret), K(ddl_type));
  }

  if (OB_FAIL(ret)) {
    SHARE_SCHEMA_LOG(WARN, "exec sql on inner table failed", K(ret), K(ddl_type));
  } else {
    ObSchemaOperation operation;
    fill_schema_operation(schema, ddl_type, operation, ddl_stmt_str);
    if (OB_FAIL(log_operation(operation, sql_client))) {
      SHARE_SCHEMA_LOG(WARN, "Failed to log operation", K(ret));
    }
  }

  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase



#endif // OB_LABEL_SE_POLICY_SQL_SERVICE_H_
