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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_OUTLINE_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_OUTLINE_SQL_SERVICE_H_

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
class ObOutlineInfo;

class ObOutlineSqlService : public ObDDLSqlService
{
public:
  ObOutlineSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObOutlineSqlService() {}

  virtual int insert_outline(const ObOutlineInfo &outline_info,
                             common::ObISQLClient &sql_client,
                             const common::ObString *ddl_stmt_str = NULL);
  virtual int replace_outline(const ObOutlineInfo &outline_info,
                             common::ObISQLClient &sql_client,
                             const common::ObString *ddl_stmt_str = NULL);
  virtual int alter_outline(const ObOutlineInfo &outline_info,
                             common::ObISQLClient &sql_client,
                             const common::ObString *ddl_stmt_str = NULL);
  virtual int delete_outline(const uint64_t tenant_id,
                             const uint64_t database_id,
                             const uint64_t outline_id,
                             const int64_t new_schema_version,
                             common::ObISQLClient &sql_client,
                             const common::ObString *ddl_stmt_str = NULL);

  virtual int drop_outline(const ObOutlineInfo &outline_info,
                           const int64_t new_schema_version,
                           common::ObISQLClient &sql_client,
                           const common::ObString *ddl_stmt_str = NULL);
private:
  int add_outline(common::ObISQLClient &sql_client, const ObOutlineInfo &outline_info,
                  const bool only_history = false);
private:
  DISALLOW_COPY_AND_ASSIGN(ObOutlineSqlService);
};


} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif //OCEANBASE_SHARE_SCHEMA_OB_OUTLINE_SQL_SERVICE_H_
