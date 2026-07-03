/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBASE_SHARE_SCHEMA_OB_AI_PROVIDER_SQL_SERVICE_H_
 #define OCEANBASE_SHARE_SCHEMA_OB_AI_PROVIDER_SQL_SERVICE_H_

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

 class ObRegisterProviderArg;
 class ObUnregisterProviderArg;

 }

 namespace share
 {

 namespace schema
 {

 class ObAIProviderSchema;

 class ObAIProviderSqlService final: public ObDDLSqlService
 {
 public:
   ObAIProviderSqlService(ObSchemaService &schema_service)
     : ObDDLSqlService(schema_service)
   {  }

 virtual ~ObAIProviderSqlService() = default;

 int create_ai_provider(const ObAIProviderSchema &new_schema,
                        const ObString &ddl_stmt,
                        common::ObISQLClient &sql_client);

 int alter_ai_provider(const ObAIProviderSchema &new_schema,
                       const ObAIProviderSchema &old_schema,
                       const int64_t new_schema_version,
                       const ObString &ddl_stmt,
                       common::ObISQLClient &sql_client);

 int drop_ai_provider(const ObAIProviderSchema &schema,
                      const int64_t new_schema_version,
                      const ObString &ddl_stmt,
                      common::ObISQLClient &sql_client);

 private:
   DISALLOW_COPY_AND_ASSIGN(ObAIProviderSqlService);
 };

 } // namespace schema
 } // namespace share
 } // namespace oceanbase

 #endif // OCEANBASE_SHARE_SCHEMA_OB_AI_PROVIDER_SQL_SERVICE_H_
