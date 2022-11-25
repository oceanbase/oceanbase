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

  int check_partition_option_for_create_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                              share::schema::ObTableSchema &table_schema);

  int add_tables_to_tablegroup(common::ObMySQLTransaction &trans,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               const share::schema::ObTablegroupSchema &tablegroup_schema,
                               const obrpc::ObAlterTablegroupArg &arg);


  int modify_partition_option(common::ObMySQLTransaction &trans,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              const share::schema::ObTablegroupSchema &tablegroup_schema,
                              const obrpc::ObAlterTablegroupArg &arg);

  int check_table_alter_tablegroup(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObTableSchema &new_table_schema);

private:
  int check_partition_option(const share::schema::ObTablegroupSchema &tablegroup,
                             share::schema::ObTableSchema &table);

  int check_partition_option(
      const share::schema::ObTablegroupSchema &tablegroup,
      share::schema::ObTableSchema &table,
      bool is_subpart,
      bool &is_matched);

  int check_alter_partition(const share::schema::ObPartitionSchema *&orig_part_schema,
                            share::schema::ObPartitionSchema *&alter_part_schema,
                            const obrpc::ObAlterTablegroupArg::ModifiableOptions alter_part_type,
                            int64_t expr_num,
                            bool is_tablegroup);
  int check_add_partition(const share::schema::ObPartitionSchema *&orig_part_schema,
                          share::schema::ObPartitionSchema *&alter_part_schema,
                          int64_t expr_num,
                          bool is_tablegroup);
  int check_drop_partition(const share::schema::ObPartitionSchema *&orig_part_schema,
                           const share::schema::ObPartitionSchema *alter_part_schema,
                           bool is_tablegroup);
  int check_partarray_expr_name_valid(const share::schema::ObPartitionSchema *&orig_part_schema,
                                      const share::schema::ObPartitionSchema *alter_part_schema,
                                      int64_t expr_num,
                                      const ObString *split_part_name = NULL);

  int batch_modify_table_partitions(
      common::ObMySQLTransaction &trans,
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTablegroupSchema &inc_tablegroup_schema,
      const share::schema::ObTablegroupSchema &orig_tablegroup_schema,
      const int64_t new_schema_version,
      const int64_t expr_num,
      obrpc::ObAlterTablegroupArg::ModifiableOptions alter_part_type);

  int modify_drop_partition(common::ObMySQLTransaction &trans,
                            share::schema::ObSchemaGetterGuard &schema_guard,
                            const share::schema::ObTablegroupSchema &orig_tablegroup_schema,
                            share::schema::ObTablegroupSchema &inc_tablegroup_schema,
                            const int64_t new_schema_version,
                            const int64_t expr_num,
                            const obrpc::ObAlterTablegroupArg &arg);
  int modify_add_partition(common::ObMySQLTransaction &trans,
                           share::schema::ObSchemaGetterGuard &schema_guard,
                           const share::schema::ObTablegroupSchema &orig_tablegroup_schema,
                           share::schema::ObTablegroupSchema &inc_tablegroup_schema,
                           const int64_t new_schema_version,
                           const int64_t expr_num,
                           const obrpc::ObAlterTablegroupArg &arg);
  int check_part_expr_num_and_value_type(
      const bool is_oracle_mode,
      const share::schema::ObPartition *alter_part,
      const share::schema::ObPartition *orig_part,
      const share::schema::ObPartitionFuncType part_func_type,
      int64_t expr_num);

  int add_table_partition_in_tablegroup(const share::schema::ObTableSchema &orig_table_schema,
                                        const share::schema::ObTablegroupSchema &inc_tablegroup_schema,
                                        const int64_t schema_version,
                                        share::schema::AlterTableSchema &alter_table_schema,
                                        common::ObMySQLTransaction &client);

  DISALLOW_COPY_AND_ASSIGN(ObTableGroupHelp);

private:
  ObDDLService *ddl_service_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
};


} //namespace rootserver
} //namespace oceanbase
#endif


