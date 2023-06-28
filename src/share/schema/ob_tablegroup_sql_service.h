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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_TABLEGROUP_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_TABLEGROUP_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"

namespace oceanbase
{
namespace share
{
class ObDMLSqlSplicer;
namespace schema
{
class ObTablegroupSchema;

class ObTablegroupSqlService : public ObDDLSqlService
{
public:
  ObTablegroupSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObTablegroupSqlService() {}
  virtual int insert_tablegroup(const ObTablegroupSchema &tablegroup_schema,
                                common::ObISQLClient &sql_client,
                                const common::ObString *ddl_stmt_str = NULL);
  virtual int delete_tablegroup(
          const ObTablegroupSchema &tablegroup_schema,
          const int64_t new_schema_version,
          common::ObISQLClient &sql_client,
          const common::ObString *ddl_stmt_str = NULL);
  virtual int update_tablegroup(ObTablegroupSchema &new_schema,
                                common::ObISQLClient &sql_client,
                                const common::ObString *ddl_stmt_str = NULL);
private:
  int add_tablegroup(common::ObISQLClient &sql_client,
                     const share::schema::ObTablegroupSchema &tablegroup,
                     const bool only_history);

  int gen_tablegroup_dml(const uint64_t exec_tenant_id,
                         const share::schema::ObTablegroupSchema &tablegroup_schema,
                         share::ObDMLSqlSplicer &dml);

  DISALLOW_COPY_AND_ASSIGN(ObTablegroupSqlService);
};


} //end of namespace share
} //end of namespace schema
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_TABLEGROUP_SQL_SERVICE_H_
