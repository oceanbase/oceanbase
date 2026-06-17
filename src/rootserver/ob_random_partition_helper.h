/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_ROOTSERVER_OB_RANDOM_PARTITION_HELPER_H_
#define OCEABASE_ROOTSERVER_OB_RANDOM_PARTITION_HELPER_H_

#include "logservice/ob_log_base_type.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_ddl_operator.h"


namespace oceanbase
{
namespace rootserver
{
class ObRandomPartitionHelper {
public:
  ObRandomPartitionHelper() {}
  ~ObRandomPartitionHelper() {}


  static int alter_table_random_part_attr_if_need(
      const obrpc::ObAlterTableArg &alter_table_arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTableSchema &table_schema,
      rootserver::ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans);
  static int update_lob_meta_table_random_part_attr(
    const share::schema::ObTableSchema &table_schema,
    share::schema::ObSchemaGetterGuard &schema_guard,
    rootserver::ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans,
    common::ObArray<common::ObTabletID> &tablet_ids);

  static int get_available_ls_cnt(const uint64_t tenant_id, int64_t &ls_cnt);

  static int get_active_tablets(const ObTableSchema &table_schema,
                                const ObIArray<ObTabletID> &inactive_tablet_ids,
                                ObIArray<int64_t> &marked_inactive_part_indexs,
                                ObIArray<ObTabletID> &active_tablet_ids);
  static int mark_tablets_as_inactive(const ObTableSchema &table_schema,
                                      const int64_t mark_inactive_num,
                                      ObIArray<int64_t> &marked_inactive_part_indexs);

  static int add_partitions(rootserver::ObDDLService &ddl_service,
                            const uint64_t tenant_id,
                            const int64_t tenant_data_version,
                            const share::schema::ObTableSchema &table_schema,
                            const int64_t add_part_num,
                            share::schema::ObMultiVersionSchemaService *schema_service,
                            common::ObMySQLProxy *sql_proxy,
                            ObTableSchema &new_table_schema,
                            ObIArray<ObTabletID> &active_tablet_ids,
                            obrpc::ObAlterTableArg &alter_table_arg,
                            share::schema::ObSchemaGetterGuard &schema_guard,
                            common::ObMySQLTransaction &trans);
  static int get_max_alloc_seq_id(const ObTableSchema &table_schema, int64_t &max_alloc_seq_id);

  static int build_add_partition_arg(rootserver::ObDDLService &ddl_service,
                                     const ObTableSchema &table_schema,
                                     const int64_t add_part_num,
                                     const int64_t max_alloc_seq_id,
                                     const uint64_t specified_value,
                                     share::schema::AlterTableSchema &alter_table_schema);

  static int set_tablet_status_to_inactive(const ObIArray<int64_t> &marked_inactive_part_indexs,
                                           const ObTableSchema &table_schema,
                                           const bool need_update_table_attribute,
                                           share::schema::ObSchemaGetterGuard &schema_guard,
                                           share::schema::ObMultiVersionSchemaService *schema_service,
                                           common::ObMySQLProxy *sql_proxy,
                                           ObTableSchema &new_table_schema,
                                           ObMySQLTransaction &trans);

  static int alter_random_distribution_partition(ObDDLService &ddl_service, obrpc::ObAlterTableArg &arg, obrpc::ObAlterRandomPartitionRes &res);

  static int is_auto_inc_pk(const share::schema::ObTableSchema &table_schema, bool &auto_inc_pk);
  static int get_random_partkey_column_id(const share::schema::ObTableSchema &table_schema, uint64_t &column_id);
  static int check_is_random_partkey(const share::schema::ObTableSchema &table_schema, const uint64_t column_id, bool &is_random_partkey);
  static int check_enable_random_partition(const share::schema::ObTableSchema &table_schema);

  static int resolve_partition_random(const bool is_subpartition, share::schema::ObTableSchema &table_schema);

  // reset partitions of random-partitioned table back to the initial state, used by TRUNCATE TABLE
  static int reset_to_initial_partitions(const int64_t target_part_num, share::schema::ObTableSchema &table_schema);
};


}//share
}//oceanbase

#endif /* OCEABASE_STORAGE_PARTITION_RANDOM_DISTRIBUTION_HELPER_H_ */
