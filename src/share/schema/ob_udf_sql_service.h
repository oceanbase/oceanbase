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

#ifndef OB_UDF_SQL_SERVICE_H_
#define OB_UDF_SQL_SERVICE_H_

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
class ObUDF;

class ObUDFSqlService : public ObDDLSqlService
{
public:
  ObUDFSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObUDFSqlService() {}

  virtual int insert_udf(const ObUDF &udf_info,
                         common::ObISQLClient *sql_client,
                         const common::ObString *ddl_stmt_str = NULL);
  /*virtual int replace_udf(const ObUDFInfo &udf_info,
    common::ObISQLClient *sql_client,
    const common::ObString *ddl_stmt_str = NULL);
    virtual int alter_udf(const ObUDFInfo &udf_info, 
    common::ObISQLClient *sql_client, 
    const common::ObString *ddl_stmt_str = NULL); */
  virtual int delete_udf(const uint64_t tenant_id,
                         const common::ObString &name,
                         const int64_t new_schema_version,
                         common::ObISQLClient *sql_client,
                         const common::ObString *ddl_stmt_str = NULL);

  virtual int drop_udf(const ObUDF &udf_info,
                       const int64_t new_schema_version,
                       common::ObISQLClient *sql_client,
                       const common::ObString *ddl_stmt_str = NULL);

private:
  int add_udf(common::ObISQLClient &sql_client, 
              const ObUDF &udf_info,
              const bool only_history = false);
private:
  DISALLOW_COPY_AND_ASSIGN(ObUDFSqlService);
};


} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif
