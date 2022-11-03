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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SYS_VARIABLE_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SYS_VARIABLE_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"
#include "share/ob_dml_sql_splicer.h"
//#include "share/schema/ob_schema_struct.h"

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
class ObSysVariableSchema;
class ObSysVarSchema;

class ObSysVariableSqlService : public ObDDLSqlService
{
public:
  ObSysVariableSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObSysVariableSqlService() {}

  virtual int replace_sys_variable(ObSysVariableSchema &sys_variable_schema,
                                   common::ObISQLClient &sql_client,
                                   const ObSchemaOperationType &operation_type,
                                   const common::ObString *ddl_stmt_str = NULL);
private:
  int replace_system_variable(const ObSysVarSchema &sysvar_schema,
                              const ObSchemaOperationType &op,
                              common::ObISQLClient &sql_client,
                              const common::ObString *ddl_stmt_str);
  int replace_system_variable(const ObSysVarSchema &sysvar_schema, common::ObISQLClient &sql_client);
  int gen_sys_variable_dml(ObDMLSqlSplicer &dml, const ObSysVarSchema &sysvar_schema, bool is_history);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysVariableSqlService);
};


} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_SYS_VARIABLE_SQL_SERVICE_H_
