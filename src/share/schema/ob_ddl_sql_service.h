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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_DDL_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_DDL_SQL_SERVICE_H_

#include "lib/utility/ob_macro_utils.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_utils.h"

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

struct ObSchemaOperation;
class ObDDLSqlService
{
public:
  ObDDLSqlService(ObSchemaService &schema_service)
    : schema_service_(schema_service){}
  virtual ~ObDDLSqlService() {}
  // Do nothing, simply push the schema version once
  int log_nop_operation(const ObSchemaOperation &schema_operation,
                        const int64_t new_schema_version,
                        const common::ObString &ddl_sql_str,
                        common::ObISQLClient &sql_client);

protected:
  virtual int log_operation(ObSchemaOperation &ddl_operation,
                            common::ObISQLClient &sql_client,
                            int64_t sql_exec_tenant_id = OB_INVALID_TENANT_ID,
                            common::ObSqlString *public_sql_string = NULL);
private:
  uint64_t fill_schema_id(const uint64_t exec_tenant_id, const uint64_t schema_id);

protected:
  ObSchemaService &schema_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDDLSqlService);
};

struct TSIDDLVar
{
  //  In bootstrap or other internal requests, exec_tenant_id_ will be the OB_SYS_TENANT_ID identity, 
  //  so the default value is changed to the system tenant
  //  ddl_id_str_ default value is null
  //  Before processing the rpc request, exec_tenant_id_/ddl_id_str_ will be set, 
  //  and the value is obtained from the arg of the resolver
  //  In log operation, write to __all_ddl_operation
  uint64_t exec_tenant_id_;
  common::ObString *ddl_id_str_;
  TSIDDLVar() :
      exec_tenant_id_(common::OB_SYS_TENANT_ID),
      ddl_id_str_(NULL)
  {}
};

struct TSILastOper {
  uint64_t last_operation_schema_version_;
  uint64_t last_operation_tenant_id_;
  TSILastOper():
      last_operation_schema_version_(OB_INVALID_VERSION),
      last_operation_tenant_id_(OB_INVALID_TENANT_ID)
  {}
};

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_DDL_SQL_SERVICE_H_
