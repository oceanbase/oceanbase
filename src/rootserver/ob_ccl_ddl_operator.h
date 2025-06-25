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

#ifndef OCEANBASE_ROOTSERVER_OB_CCL_DDL_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_CCL_DDL_OPERATOR_H_

#include "share/ob_rpc_struct.h"
#include "rootserver/ob_ccl_ddl_service.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase {

namespace rootserver {
class ObCclDDLOperator {
public:
  ObCclDDLOperator(share::schema::ObMultiVersionSchemaService &schema_service,
                   common::ObMySQLProxy &sql_proxy)
      : schema_service_(schema_service), sql_proxy_(sql_proxy) {}
  virtual ~ObCclDDLOperator() {}
  int create_ccl_rule(ObCCLRuleSchema &ccl_rule_schema,
                      ObMySQLTransaction &trans,
                      const ObString *ddl_stmt_str);

  int drop_ccl_rule(uint64_t tenant_id, const ObCCLRuleSchema &ccl_rule_schema,
                    ObMySQLTransaction &trans,
                    const ObString *ddl_stmt_str);

private:
  share::schema::ObMultiVersionSchemaService &schema_service_;
  common::ObMySQLProxy &sql_proxy_;
};

} // end namespace rootserver
} // end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_CCL_DDL_OPERATOR_H_