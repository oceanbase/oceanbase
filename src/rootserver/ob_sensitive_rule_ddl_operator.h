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

#ifndef _OCEANBASE_ROOTSERVER_OB_SENSITIVE_RULE_DDL_OPERATOR_H_
#define _OCEANBASE_ROOTSERVER_OB_SENSITIVE_RULE_DDL_OPERATOR_H_

#include "share/schema/ob_latest_schema_guard.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_sensitive_rule_sql_service.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace rootserver
{
class ObSensitiveRuleDDLOperator
{
public:
  ObSensitiveRuleDDLOperator(share::schema::ObMultiVersionSchemaService &schema_service,
                             common::ObMySQLProxy &sql_proxy)
    : schema_service_(schema_service),
      sql_proxy_(sql_proxy)
  {}
  virtual ~ObSensitiveRuleDDLOperator() {}
  int handle_sensitive_rule_function(ObSensitiveRuleSchema &schema,
                                     ObMySQLTransaction &trans,
                                     const share::schema::ObSchemaOperationType ddl_type,
                                     const ObString &ddl_stmt_str,
                                     const uint64_t tenant_id,
                                     ObSchemaGetterGuard &schema_guard,
                                     uint64_t user_id);
  int grant_or_revoke_after_ddl(ObSensitiveRuleSchema &schema,
                                ObMySQLTransaction &trans,
                                const share::schema::ObSchemaOperationType ddl_type,
                                ObSchemaGetterGuard &schema_guard,
                                uint64_t user_id);
  int grant_revoke_sensitive_rule(const ObSensitiveRulePrivSortKey &sensitive_rule_priv_key,
                                  const ObPrivSet priv_set,
                                  const bool grant,
                                  const common::ObString &ddl_stmt_str,
                                  common::ObMySQLTransaction &trans);
  int drop_sensitive_column_in_drop_table(const ObTableSchema &table_schema,
                                          ObMySQLTransaction &trans,
                                          ObSchemaGetterGuard &schema_guard);
  int drop_sensitive_column_in_drop_table(const ObTableSchema &table_schema,
                                          ObMySQLTransaction &trans,
                                          ObIArray<const ObSensitiveRuleSchema *> &sensitive_rules);
  int drop_sensitive_column_cascades(const ObTableSchema &table_schema,
                                     const ObIArray<uint64_t> &drop_column_ids,
                                     common::ObMySQLTransaction &trans,
                                     ObSchemaGetterGuard &schema_guard);
private:
  int build_drop_sensitive_column_schema(const ObSensitiveRuleSchema *sensitive_rule,
                                         const ObTableSchema &table_schema,
                                         const ObIArray<uint64_t> &drop_column_ids,
                                         ObSensitiveRuleSchema &drop_schema);
  int handle_sensitive_rule_function_inner(ObSensitiveRuleSchema &schema,
                                           const ObSensitiveRuleSchema *exist_schema,
                                           ObMySQLTransaction &trans,
                                           const share::schema::ObSchemaOperationType ddl_type,
                                           const ObString &ddl_stmt_str,
                                           const uint64_t tenant_id,
                                           ObSensitiveRuleSchema &new_schema);
  int update_table_schema(ObSensitiveRuleSchema &schema,
                          ObMySQLTransaction &trans,
                          ObSchemaGetterGuard &schema_guard,
                          const uint64_t tenant_id);
  share::schema::ObMultiVersionSchemaService &schema_service_;
  common::ObMySQLProxy &sql_proxy_;
};
} // end namespace rootserver
} // end namespace oceanbase

#endif // _OCEANBASE_ROOTSERVER_OB_SENSITIVE_RULE_DDL_OPERATOR_H_