/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
