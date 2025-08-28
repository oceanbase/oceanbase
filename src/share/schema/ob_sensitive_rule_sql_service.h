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

#ifndef OB_SENSITIVE_RULE_SQL_SERVICE_H
#define OB_SENSITIVE_RULE_SQL_SERVICE_H

#include "ob_ddl_sql_service.h"
#include "src/share/ob_dml_sql_splicer.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
} // end namespace common
namespace share
{
namespace schema
{
class ObSensitiveRuleSchema;

class ObSensitiveRuleSqlService : public ObDDLSqlService
{
public:
  explicit ObSensitiveRuleSqlService(ObSchemaService &schema_service) 
    : ObDDLSqlService(schema_service) {}
  virtual ~ObSensitiveRuleSqlService() {}

  int apply_new_schema(const ObSensitiveRuleSchema &schema,
                       ObISQLClient &sql_client,
                       const ObSchemaOperationType ddl_type,
                       const ObString &ddl_stmt_str);
  int grant_revoke_sensitive_rule(const ObSensitiveRulePrivSortKey &sensitive_rule_priv_key,
                                  const ObPrivSet priv_set,
                                  const int64_t new_schema_version,
                                  const ObString &ddl_stmt_str,
                                  ObISQLClient &sql_client);
private:
  int add_schema(ObISQLClient &sql_client, const ObSensitiveRuleSchema &schema);
  int drop_schema(ObISQLClient &sql_client, const ObSensitiveRuleSchema &schema);
  int alter_schema(ObISQLClient &sql_client, const ObSensitiveRuleSchema &schema);
  int gen_insert_sql(common::ObSqlString &sql, 
                     common::ObSqlString &values, 
                     const ObSensitiveRuleSchema &schema);
  int add_column_schema(ObISQLClient &sql_client, const ObSensitiveRuleSchema &schema);
  int drop_column_schema(ObISQLClient &sql_client, const ObSensitiveRuleSchema &schema);
  int gen_sql(common::ObSqlString &sql,
              common::ObSqlString &values,
              const ObSensitiveRuleSchema &schema);
  int gen_sensitive_rule_priv_dml(const uint64_t exec_tenant_id,
                                  const ObSensitiveRulePrivSortKey &sensitive_rule_priv_key,
                                  const ObPrivSet &priv_set,
                                  share::ObDMLSqlSplicer &dml);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSensitiveRuleSqlService);
};
} // end namespace schema
} // end namespace share
} // end namespace oceanbase

#endif //OB_SENSITIVE_RULE_SQL_SERVICE_H