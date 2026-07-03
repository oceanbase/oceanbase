/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_AI_PROVIDER_DDL_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_AI_PROVIDER_DDL_OPERATOR_H_

#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_ai_provider_mgr.h"

namespace oceanbase
{

namespace rootserver
{
class ObAIProviderDDLOperator
{
public:
  ObAIProviderDDLOperator(share::schema::ObMultiVersionSchemaService &schema_service)
      : schema_service_(schema_service) {}
  virtual ~ObAIProviderDDLOperator() {}

  int register_provider(share::schema::ObAIProviderSchema &provider_schema,
                        const share::schema::ObAIProviderSchema *old_provider_schema,
                        const ObString &ddl_stmt,
                        common::ObMySQLTransaction &trans);
  int unregister_provider(const share::schema::ObAIProviderSchema &provider_schema,
                          const ObString &ddl_stmt,
                          common::ObMySQLTransaction &trans);
  int check_gateway_reference(const share::schema::ObAIProviderSchema &provider_schema,
                              common::ObMySQLTransaction &trans,
                              bool &is_referenced);

private:
  share::schema::ObMultiVersionSchemaService &schema_service_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_AI_PROVIDER_DDL_OPERATOR_H_
