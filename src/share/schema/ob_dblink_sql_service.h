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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_DBLINK_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_DBLINK_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"
#include "share/ob_dml_sql_splicer.h"

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
class ObDbLinkInfo;

class ObDbLinkSqlService : public ObDDLSqlService
{
public:
  ObDbLinkSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObDbLinkSqlService() {}
public:
  int insert_dblink(const ObDbLinkBaseInfo &dblink_info,
                    const int64_t is_deleted,
                    common::ObISQLClient &sql_client,
                    const common::ObString *ddl_stmt_str);
  int delete_dblink(const uint64_t tenant_id,
                    const uint64_t dblink_id,
                    common::ObISQLClient &sql_client);
private:
  int add_pk_columns(const uint64_t tenant_id,
                     const uint64_t dblink_id,
                     ObDMLSqlSplicer &dml);
  int add_normal_columns(const ObDbLinkBaseInfo &dblink_info,
                         ObDMLSqlSplicer &dml);
  int add_history_columns(const ObDbLinkBaseInfo &dblink_info,
                          int64_t is_deleted,
                          ObDMLSqlSplicer &dml);

  DISALLOW_COPY_AND_ASSIGN(ObDbLinkSqlService);
};


} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_DBLINK_SQL_SERVICE_H_
