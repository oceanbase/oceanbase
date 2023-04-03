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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SECURITY_AUDIT_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SECURITY_AUDIT_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"
#include "share/ob_rpc_struct.h"
namespace oceanbase
{
namespace common
{
class ObISQLClient;
}
namespace share
{
class ObDMLSqlSplicer;

namespace schema
{
class ObUserInfo;

class ObAuditSqlService : public ObDDLSqlService
{
public:
  ObAuditSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObAuditSqlService() {}
  int handle_audit_metainfo(const ObSAuditSchema &audit_schema,
                            const ObSAuditModifyType modify_type,
                            const bool need_update,
                            const int64_t new_schema_version,
                            const ObString *ddl_stmt_str,
                            common::ObISQLClient &sql_client,
                            common::ObSqlString &public_sql_string);
  int add_audit_metainfo(common::ObISQLClient &sql_client,
                         const ObSAuditSchema &audit_schema,
                         const int64_t new_schema_version);
  int update_audit_metainfo(common::ObISQLClient &sql_client,
                            const ObSAuditModifyType modify_type,
                            const ObSAuditSchema &audit_schema,
                            const int64_t new_schema_version);
  int del_audit_metainfo(common::ObISQLClient &sql_client,
                         const ObSAuditSchema &audit_schema,
                         const int64_t new_schema_version);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAuditSqlService);
};


} //end of namespace share
} //end of namespace schema
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_SECURITY_AUDIT_SQL_SERVICE_H_
