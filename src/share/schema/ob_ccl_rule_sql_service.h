/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_CCL_RULE_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_CCL_RULE_SQL_SERVICE_H_

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
class ObCCLRuleSchema;

class ObCCLRuleSqlService : public ObDDLSqlService
{
public:
  ObCCLRuleSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObCCLRuleSqlService() {}

  virtual int insert_ccl_rule(const ObCCLRuleSchema &ccl_rule_schema,
                              common::ObISQLClient &sql_client,
                              const common::ObString *ddl_stmt_str = NULL);
  virtual int delete_ccl_rule(const ObCCLRuleSchema &ccl_rule_schema,
                              const int64_t new_schema_version,
                              common::ObISQLClient &sql_client,
                              const common::ObString *ddl_stmt_str = NULL);

private:
  int gen_sql(ObSqlString &sql, ObSqlString &values, const ObCCLRuleSchema &ccl_rule_schema);
  static constexpr int THE_SYS_TABLE_IDX = 0;
  static constexpr int THE_HISTORY_TABLE_IDX = 1;
  static const char *CCL_RULE_TABLES[2];
private:
  DISALLOW_COPY_AND_ASSIGN(ObCCLRuleSqlService);
};


} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_CCL_RULE_SQL_SERVICE_H_
