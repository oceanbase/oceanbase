/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_ROOTSERVER_OB_AI_GATEWAY_DDL_SERVICE_H_
#define _OCEANBASE_ROOTSERVER_OB_AI_GATEWAY_DDL_SERVICE_H_

#include "share/schema/ob_schema_mgr.h"
#include "rootserver/ob_ddl_service.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
namespace rootserver
{
class ObAIGatewayDDLService
{
public:
  ObAIGatewayDDLService(ObDDLService &ddl_service) : ddl_service_(ddl_service) {}
  virtual ~ObAIGatewayDDLService() {}

  int create_gateway(const obrpc::ObCreateAiGatewayArg &arg);
  int alter_gateway(const obrpc::ObAlterAiGatewayArg &arg);
  int drop_gateway(const obrpc::ObDropAiGatewayArg &arg);
private:
  ObDDLService &ddl_service_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // _OCEANBASE_ROOTSERVER_OB_AI_GATEWAY_DDL_SERVICE_H_
