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

 #ifndef OCEANBASE_SHARE_SCHEMA_OB_LOCATION_SQL_SERVICE_H_
 #define OCEANBASE_SHARE_SCHEMA_OB_LOCATION_SQL_SERVICE_H_

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
 class ObLocationSchema;

 class ObLocationSqlService : public ObDDLSqlService
 {
 public:
   explicit ObLocationSqlService(ObSchemaService &schema_service);
   virtual ~ObLocationSqlService();

   ObLocationSqlService(const ObLocationSqlService&) = delete;
   ObLocationSqlService &operator=(const ObLocationSqlService&) = delete;

   int apply_new_schema(const ObLocationSchema &schema,
                        ObISQLClient &sql_client,
                        ObSchemaOperationType ddl_type,
                        const common::ObString &ddl_stmt_str);
 private:
   int add_schema(ObISQLClient &sql_client, const ObLocationSchema &schema);
   int alter_schema(ObISQLClient &sql_client, const ObLocationSchema &schema);
   int drop_schema(ObISQLClient &sql_client, const ObLocationSchema &schema);
   int gen_sql(common::ObSqlString &sql, common::ObSqlString &values, const ObLocationSchema &schema);
 private:
   static constexpr int THE_SYS_TABLE_IDX = 0;
   static constexpr int THE_HISTORY_TABLE_IDX = 1;
   static const char *LOCATION_TABLES[2];
 };
 } // namespace schema
 } // namespace share
 } // namespace oceanbase

 #endif // OCEANBASE_SHARE_SCHEMA_OB_LOCATION_SQL_SERVICE_H_
