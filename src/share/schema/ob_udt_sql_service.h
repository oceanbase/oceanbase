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

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_UDT_SQL_SERVICE_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_UDT_SQL_SERVICE_H_
#include "share/schema/ob_ddl_sql_service.h"

namespace oceanbase
{
namespace common
{
class ObString;
class ObISQLClient;
} // namespace common
namespace share
{
class ObDMLSqlSplicer;
namespace schema
{
class ObUDTTypeInfo;
class ObUDTTypeAttr;
class ObUDTCollectionType;
class ObUDTObjectType;

class ObUDTSqlService : public ObDDLSqlService
{
public:
  explicit ObUDTSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObUDTSqlService() {}

  int create_udt(ObUDTTypeInfo &udt_info,
                 common::ObISQLClient *sql_client,
                 const common::ObString *ddl_stmt_str = NULL);
  int replace_udt(ObUDTTypeInfo& udt_info,
                  const ObUDTTypeInfo *old_udt_info,
                  const int64_t del_param_schema_version,
                  common::ObISQLClient *sql_client,
                  const common::ObString *ddl_stmt_str = NULL);
  int drop_udt(const ObUDTTypeInfo &udt_info,
               const int64_t new_schema_version,
               common::ObISQLClient &sql_client,
               const common::ObString *ddl_stmt_str = NULL);
  int update_type_schema_version(common::ObISQLClient &sql_client,
                                  const ObUDTTypeInfo &udt_info,
                                  const common::ObString *ddl_stmt_str);
private:
  int add_udt(common::ObISQLClient &sql_client,
              const ObUDTTypeInfo &udt_info,
              bool is_replace = false,
              bool only_history = false);
  int add_udt_coll(common::ObISQLClient &sql_client,
                   const ObUDTTypeInfo &udt_info,
                   bool only_history);
  int add_udt_attrs(common::ObISQLClient &sql_client,
                    const ObUDTTypeInfo &udt_info,
                    bool only_history = false);
  int add_udt_object(common::ObISQLClient &sql_client,
                    const ObUDTTypeInfo &udt_info,
                    bool is_replace = false,
                    bool body_only = false,
                    bool only_history = false);
  int del_udt(common::ObISQLClient &sql_client,
              const ObUDTTypeInfo &udt_info,
              int64_t new_schema_version);
  int del_udt_coll(common::ObISQLClient &sql_client,
                   const ObUDTTypeInfo &udt_info,
                   int64_t new_schema_version);
  int del_udt_attrs(common::ObISQLClient &sql_client,
                    const ObUDTTypeInfo &udt_info,
                    int64_t new_schema_version);
  int del_udt_object(common::ObISQLClient &sql_client,
                    const ObUDTTypeInfo &udt_info,
                    int64_t object_type,
                    int64_t new_schema_version);
  int del_udt_objects_in_udt(common::ObISQLClient &sql_client,
                    const ObUDTTypeInfo &udt_info,
                    int64_t new_schema_version);
  int gen_udt_dml(const uint64_t tenant_id, const ObUDTTypeInfo &udt_info, ObDMLSqlSplicer &dml);
  int gen_udt_coll_dml(const uint64_t tenant_id, const ObUDTCollectionType &coll_info, ObDMLSqlSplicer &dml);
  int gen_udt_attr_dml(const uint64_t tenant_id, const ObUDTTypeAttr &udt_param, ObDMLSqlSplicer &dml);
  int gen_udt_object_dml(const uint64_t tenant_id, const ObUDTObjectType &udt_object, ObDMLSqlSplicer &dml);

private:
  DISALLOW_COPY_AND_ASSIGN(ObUDTSqlService);
};
} //end of schema
} //end of share
} //end of oceanbase
#endif /* OCEANBASE_SRC_SHARE_SCHEMA_OB_UDT_SQL_SERVICE_H_ */
