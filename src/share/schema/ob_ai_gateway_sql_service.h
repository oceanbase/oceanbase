/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBASE_SHARE_SCHEMA_OB_AI_GATEWAY_SQL_SERVICE_H_
 #define OCEANBASE_SHARE_SCHEMA_OB_AI_GATEWAY_SQL_SERVICE_H_

 #include "ob_ddl_sql_service.h"

 namespace oceanbase
 {

 namespace common
 {

 class ObString;
 class ObISQLClient;

 }

 namespace obrpc
 {

 class ObCreateAiGatewayArg;
 class ObAlterAiGatewayArg;
 class ObDropAiGatewayArg;

 }

 namespace share
 {

 namespace schema
 {

 class ObAIGatewaySchema;

 class ObAIGatewaySqlService final: public ObDDLSqlService
 {
 public:
   ObAIGatewaySqlService(ObSchemaService &schema_service)
     : ObDDLSqlService(schema_service)
   {  }

 virtual ~ObAIGatewaySqlService() = default;

 int create_ai_gateway(const ObAIGatewaySchema &new_schema,
                       const ObString &ddl_stmt,
                       common::ObISQLClient &sql_client);

 int alter_ai_gateway(const ObAIGatewaySchema &new_schema,
                      const ObAIGatewaySchema &old_schema,
                      const int64_t new_schema_version,
                      const ObString &ddl_stmt,
                      common::ObISQLClient &sql_client);

 int drop_ai_gateway(const ObAIGatewaySchema &schema,
                     const int64_t new_schema_version,
                     const ObString &ddl_stmt,
                     common::ObISQLClient &sql_client);

 private:
   DISALLOW_COPY_AND_ASSIGN(ObAIGatewaySqlService);
 };

 } // namespace schema
 } // namespace share
 } // namespace oceanbase

 #endif // OCEANBASE_SHARE_SCHEMA_OB_AI_GATEWAY_SQL_SERVICE_H_
