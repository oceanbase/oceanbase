/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_AI_GATEWAY_DDL_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_AI_GATEWAY_DDL_OPERATOR_H_

#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_ai_gateway_mgr.h"

namespace oceanbase
{

namespace rootserver
{
class ObAIGatewayDDLOperator
{
public:
  ObAIGatewayDDLOperator(share::schema::ObMultiVersionSchemaService &schema_service)
      : schema_service_(schema_service) {}
  virtual ~ObAIGatewayDDLOperator() {}

  int register_gateway(share::schema::ObAIGatewaySchema &gateway_schema,
                       const share::schema::ObAIGatewaySchema *old_gateway_schema,
                       const ObString &ddl_stmt,
                       common::ObMySQLTransaction &trans);
  int unregister_gateway(const share::schema::ObAIGatewaySchema &gateway_schema,
                         const ObString &ddl_stmt,
                         common::ObMySQLTransaction &trans);
  int check_endpoint_provider_exists(const share::schema::ObAIGatewaySchema &gateway_schema,
                                     common::ObMySQLTransaction &trans);

private:
  share::schema::ObMultiVersionSchemaService &schema_service_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_AI_GATEWAY_DDL_OPERATOR_H_
