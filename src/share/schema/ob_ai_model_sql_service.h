/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBASE_SHARE_SCHEMA_OB_AI_MODEL_SQL_SERVICE_H_
 #define OCEANBASE_SHARE_SCHEMA_OB_AI_MODEL_SQL_SERVICE_H_

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

 class ObCreateAiModelArg;

 }
 namespace share
 {

 namespace schema
 {

 class ObAiModelSchema;

 class ObAiModelSqlService final: public ObDDLSqlService
 {
 public:
   ObAiModelSqlService(ObSchemaService &schema_service)
     : ObDDLSqlService(schema_service)
   {  }

 virtual ~ObAiModelSqlService() = default;

 int create_ai_model(const ObAiModelSchema &new_schema,
                     const ObString &ddl_stmt,
                     common::ObISQLClient &sql_client);

 int drop_ai_model(const ObAiModelSchema &schema,
                   const int64_t new_schema_version,
                   const ObString &ddl_stmt,
                   common::ObISQLClient &sql_client);

 private:
   DISALLOW_COPY_AND_ASSIGN(ObAiModelSqlService);
 };

 } // namespace schema
 } // namespace share
 } // namespace oceanbase

 #endif // OCEANBASE_SHARE_SCHEMA_OB_AI_MODEL_SQL_SERVICE_H_
