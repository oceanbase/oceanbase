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

namespace oceanbase {
namespace share {
class ObDMLSqlSplicer;
namespace schema {
class ObTablegroupSchema;

class ObTablegroupSqlService : public ObDDLSqlService {
public:
  ObTablegroupSqlService(ObSchemaService& schema_service) : ObDDLSqlService(schema_service)
  {}
  virtual ~ObTablegroupSqlService()
  {}
  virtual int insert_tablegroup(const ObTablegroupSchema& tablegroup_schema, common::ObISQLClient& sql_client,
      const common::ObString* ddl_stmt_str = NULL);
  virtual int delete_tablegroup(const ObTablegroupSchema& tablegroup_schema, const int64_t new_schema_version,
      common::ObISQLClient& sql_client, const common::ObString* ddl_stmt_str = NULL, bool is_delay_delete = false,
      const common::ObString* delay_deleted_name = NULL);
  virtual int update_tablegroup(
      ObTablegroupSchema& new_schema, common::ObISQLClient& sql_client, const common::ObString* ddl_stmt_str = NULL);
  int drop_tablegroup_for_inspection(
      const ObTablegroupSchema& tablegroup_schema, const int64_t new_schema_version, common::ObISQLClient& sql_client);
  int add_inc_part_info(common::ObISQLClient& sql_client, const ObTablegroupSchema& ori_tablegroup,
      const ObTablegroupSchema& inc_tablegroup, const int64_t schema_version);

  int drop_inc_part_info(common::ObISQLClient& sql_client, const ObTablegroupSchema& ori_tablegroup,
      const ObTablegroupSchema& inc_tablegroup, const int64_t schema_version, bool is_delay_delete);
  int drop_part_info_for_inspection(ObISQLClient& sql_client, const ObTablegroupSchema& tablegroup_schema,
      const ObTablegroupSchema& inc_tablegroup, const int64_t schema_version);
  int update_tablegroup_schema_version(common::ObISQLClient& sql_client, const ObTablegroupSchema& tablegroup);
  int update_partition_option(common::ObISQLClient& sql_client, const ObTablegroupSchema& tablegroup,
      const ObSchemaOperationType opt_type, const common::ObString* ddl_stmt_str = NULL);
  int update_partition_option_without_log(common::ObISQLClient& sql_client, const ObTablegroupSchema& tablegroup);
  int update_max_used_part_id(common::ObISQLClient& client, const int64_t schema_version,
      const ObTablegroupSchema& orig_schema, const ObTablegroupSchema& alter_schema);

  int modify_dest_partition(common::ObISQLClient& client, const ObTablegroupSchema& tablegroup_schema);
  int ddl_log(
      common::ObISQLClient& client, const ObSchemaOperationType op_type, const ObTablegroupSchema& tablegroup_schema);
  int check_tablegroup_options(const share::schema::ObTablegroupSchema& tablegroup);

private:
  int add_tablegroup(
      common::ObISQLClient& sql_client, const share::schema::ObTablegroupSchema& tablegroup, const bool only_history);

  int gen_tablegroup_dml(const uint64_t exec_tenant_id, const share::schema::ObTablegroupSchema& tablegroup_schema,
      share::ObDMLSqlSplicer& dml);

  DISALLOW_COPY_AND_ASSIGN(ObTablegroupSqlService);
};

}  // namespace schema
}  // namespace share
}  // end of namespace oceanbase

#endif  // OCEANBASE_SHARE_SCHEMA_OB_TABLEGROUP_SQL_SERVICE_H_
