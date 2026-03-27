/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_ROOTSERVER_OB_SENSITIVE_RULE_DDL_SERVICE_H_
#define _OCEANBASE_ROOTSERVER_OB_SENSITIVE_RULE_DDL_SERVICE_H_

#include "rootserver/ob_ddl_service.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
namespace rootserver
{
class ObSensitiveRuleDDLService
{
public:
  ObSensitiveRuleDDLService(ObDDLService *ddl_service) : ddl_service_(ddl_service) {}
  virtual ~ObSensitiveRuleDDLService() {}
  int handle_sensitive_rule_ddl(const obrpc::ObSensitiveRuleDDLArg &arg);
  int grant_revoke_sensitive_rule(const ObSensitiveRulePrivSortKey &sensitive_rule_priv_key,
                                  const ObString &user_name,
                                  const ObString &host_name,
                                  const ObNeedPriv &need_priv,
                                  const bool grant,
                                  share::schema::ObSchemaGetterGuard &schema_guard);
  int revoke_sensitive_rule(const ObRevokeSensitiveRuleArg &arg);
  int drop_sensitive_column_caused_by_drop_column_online(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema &origin_table_schema,
    const common::ObIArray<uint64_t> &drop_cols_id_arr,
    common::ObMySQLTransaction &trans);
  int rebuild_hidden_table_sensitive_columns(const ObTableSchema &orig_table_schema,
                                             const ObTableSchema &hidden_table_schema,
                                             const share::schema::AlterTableSchema &alter_table_schema,
                                             ObSchemaGetterGuard &schema_guard,
                                             ObMySQLTransaction &trans);
private:
  int ddl_validation_check(ObSchemaOperationType ddl_type,
                          ObSensitiveRuleSchema &new_schema,
                           ObSchemaGetterGuard &schema_guard);
  int sensitive_field_validation_check(const ObSensitiveRuleSchema &schema,
                                   ObSchemaGetterGuard &schema_guard,
                                   bool should_exist);

  ObDDLService *ddl_service_;
};
} // end namespace rootserver
} // end namespace oceanbase

#endif // _OCEANBASE_ROOTSERVER_OB_SENSITIVE_RULE_DDL_SERVICE_H_