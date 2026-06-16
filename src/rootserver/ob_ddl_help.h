/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_DDL_HELP_H
#define OCEANBASE_ROOTSERVER_OB_DDL_HELP_H
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_partition_modify.h"
namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class ObSplitInfo;
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace rootserver
{
class ObDDLService;
class ObTableGroupHelp
{
public:
  ObTableGroupHelp(ObDDLService &ddl_service,
                   share::schema::ObMultiVersionSchemaService &schema_service,
                   common::ObMySQLProxy &sql_proxy)
      : ddl_service_(&ddl_service),
      schema_service_(&schema_service),
      sql_proxy_(&sql_proxy)
  {}
  ~ObTableGroupHelp() {}

  int check_table_partition_in_tablegroup(const share::schema::ObTableSchema *first_table_schema,
                                          share::schema::ObTableSchema &table_schema,
                                          share::schema::ObSchemaGetterGuard &schema_guard,
                                          const share::schema::ObTablegroupSchema *tablegroup = nullptr);

  int add_tables_to_tablegroup(common::ObMySQLTransaction &trans,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               const share::schema::ObTablegroupSchema &tablegroup_schema,
                               const obrpc::ObAlterTablegroupArg &arg);

  int update_table_tablegroup(
      common::ObMySQLTransaction &trans,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t new_tablegroup_id,
      const share::schema::ObTableSchema &table_schema,
      const ObString &ddl_stmt_str,
      const share::schema::ObTableSchema *first_table_schema = nullptr);

  int modify_partition_option(ObMySQLTransaction &trans,
                              ObSchemaGetterGuard &schema_guard,
                              const ObTablegroupSchema &tablegroup_schema,
                              const obrpc::ObAlterTablegroupArg &arg);

  int check_table_alter_tablegroup(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema *first_table_schema,
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObTableSchema &new_table_schema);

  int modify_sharding_type(const obrpc::ObAlterTablegroupArg &arg,
                           const ObTablegroupSchema &tablegroup_schema,
                           common::ObMySQLTransaction &trans,
                           ObSchemaGetterGuard &schema_guard);
  int modify_scope_type(const obrpc::ObAlterTablegroupArg &arg,
                        const ObTablegroupSchema &tablegroup_schema,
                        common::ObMySQLTransaction &trans);

private:

  int check_table_partition_option(const ObTableSchema *table_schema,
                                   const ObTableSchema *first_table_schema,
                                   ObSchemaGetterGuard &schema_guard,
                                   bool check_subpart,
                                   bool &is_matched);
  int check_all_table_partition_option(const ObTablegroupSchema &tablegroup_schema,
                                       ObSchemaGetterGuard &schema_guard,
                                       bool check_subpart,
                                       bool &is_matched,
                                       common::ObSqlString &user_error);

  int check_all_partition_option_for_subpart_sharding(
      const ObTablegroupSchema &tablegroup_schema,
      ObSchemaGetterGuard &schema_guard,
      bool &is_matched,
      common::ObSqlString &user_error);

  int check_partition_option(const share::schema::ObTablegroupSchema &tablegroup,
                             const share::schema::ObTableSchema *fist_table_schema,
                             const share::schema::ObTableSchema &table,
                             ObSchemaGetterGuard &schema_guard);

  DISALLOW_COPY_AND_ASSIGN(ObTableGroupHelp);

private:
  ObDDLService *ddl_service_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
};


} //namespace rootserver
} //namespace oceanbase
#endif


