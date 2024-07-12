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

#ifndef _OCEANBASE_ROOTSERVER_OB_DDL_SERVICE_H_
#define _OCEANBASE_ROOTSERVER_OB_DDL_SERVICE_H_

#include "lib/ob_define.h"
#include "share/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_placement_hashset.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_leader_election_waiter.h"
#include "lib/worker.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_unit_getter.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "ob_root_balancer.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_unit_replica_counter.h"
#include "share/ls/ob_ls_table_operator.h"
#include "storage/tablet/ob_tablet_binding_helper.h"
#include "storage/ddl/ob_ddl_clog.h"
#include "share/ob_freeze_info_proxy.h"
#include "common/ob_common_utility.h"
#include "share/config/ob_config.h" // ObConfigPairs
#include "rootserver/parallel_ddl/ob_index_name_checker.h"
#include "rootserver/parallel_ddl/ob_tablet_balance_allocator.h"

namespace oceanbase
{
namespace common
{
using ObAddrIArray = ObIArray<ObAddr>;
using ObAddrArray = ObSEArray<ObAddr, 3>;
class ObMySQLProxy;
class ObAddr;
class ObMySQLTransaction;
}
namespace obrpc
{
class ObAccountArg;
class ObCreateMLogArg;
}

namespace share
{
class SCN;
class ObLSTableOperator;
class ObAutoincrementService;
class ObSplitInfo;
struct TenantUnitRepCnt;
namespace schema
{
class ObTenantSchema;
class ObDatabaseSchema;
class ObTablegroupSchema;
class ObTableSchema;
class ObMultiVersionSchemaService;
class ObNeedPriv;
class ObSchemaMgr;
class ObMViewInfo;
}
}

namespace obrpc
{
class ObSrvRpcProxy;
class ObCommonRpcProxy;
}
namespace palf
{
  struct PalfBaseInfo;
}
namespace rootserver
{
class ObDDLOperator;
class ObZoneManager;
class ObUnitManager;
class ObCommitAlterTenantLocalityArg;
class ObCommitAlterTablegroupLocalityArg;
class ObCommitAlterTableLocalityArg;
class ObDDLSQLTransaction;
class ObTableGroupHelp;
//class ObFreezeInfoManager;
class ObSnapshotInfoManager;

class ObDDLService
{
public:
  typedef std::pair<share::ObLSID, common::ObTabletID> LSTabletID;
public:
  friend class ObTableGroupHelp;
  friend class ObStandbyClusterSchemaProcessor;
  ObDDLService();
  virtual ~ObDDLService() {}

  int init(obrpc::ObSrvRpcProxy &rpc_proxy,
           obrpc::ObCommonRpcProxy &common_rpc,
           common::ObMySQLProxy &sql_proxy,
           share::schema::ObMultiVersionSchemaService &schema_service,
           share::ObLSTableOperator &lst_operator,
           ObZoneManager &zone_mgr,
           ObUnitManager &unit_mgr,
           ObSnapshotInfoManager &snapshot_mgr);
  bool is_inited() const { return inited_; }
  void stop() { stopped_ = true; }
  void restart() { stopped_ = false; }
  bool is_stopped() { return stopped_; }
  // these functions should be called after ddl_service has been inited
  share::schema::ObMultiVersionSchemaService &get_schema_service() { return *schema_service_; }
  common::ObMySQLProxy &get_sql_proxy() { return *sql_proxy_; }
  ObUnitManager &get_unit_manager() { return *unit_mgr_; }
  ObZoneManager &get_zone_mgr() { return *zone_mgr_; }
  ObSnapshotInfoManager &get_snapshot_mgr() { return *snapshot_mgr_; }
  share::ObLSTableOperator &get_lst_operator() { return *lst_operator_; }
  share::schema::ObIndexNameChecker &get_index_name_checker() { return index_name_checker_; }
  share::schema::ObNonPartitionedTableTabletAllocator &get_non_partitioned_tablet_allocator()
  {
     return non_partitioned_tablet_allocator_;
  }

  // create_index_table will fill table_id and frozen_version to table_schema
  virtual int create_index_table(const obrpc::ObCreateIndexArg &arg,
                                 const uint64_t tenant_data_version,
                                 share::schema::ObTableSchema &table_schema,
                                 ObMySQLTransaction &sql_trans);

  virtual int create_mlog_table(ObMySQLTransaction &sql_trans,
                                const obrpc::ObCreateMLogArg &arg,
                                const uint64_t tenant_data_version,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                share::schema::ObTableSchema &table_schema);

  virtual int create_mlog_tablet(ObMySQLTransaction &trans,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 const share::schema::ObTableSchema &mlog_schema,
                                 const bool need_check_tablet_cnt,
                                 const uint64_t tenant_data_version);

  virtual int add_mlog(ObMySQLTransaction &trans,
                       const obrpc::ObCreateMLogArg &arg,
                       ObSchemaGetterGuard &schema_guard,
                       const share::schema::ObTableSchema &mlog_schema);

  int rebuild_index(const obrpc::ObRebuildIndexArg &arg,
                    obrpc::ObAlterTableRes &res);

  int rebuild_index_in_trans(share::schema::ObSchemaGetterGuard &schema_guard,
                             const share::schema::ObTableSchema &data_table_schema,
                             share::schema::ObTableSchema &table_schema,
                             const ObString *ddl_stmt_str,
                             ObMySQLTransaction *sql_trans,
                             const uint64_t tenant_data_version);

  int create_inner_expr_index(ObMySQLTransaction &trans,
                              const share::schema::ObTableSchema &orig_table_schema,
                              const uint64_t tenant_data_version,
                              share::schema::ObTableSchema &new_table_schema,
                              common::ObIArray<share::schema::ObColumnSchemaV2*> &new_columns,
                              share::schema::ObTableSchema &index_schema);
  // check whether the foreign key related table is executing offline ddl, creating index, and constrtaint task.
  // And ddl should be refused if the foreign key related table is executing above ddl.
  int check_fk_related_table_ddl(
      const share::schema::ObTableSchema &data_table_schema,
      const share::ObDDLType &ddl_type);
  int create_global_index(
      ObMySQLTransaction &trans,
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &table_schema,
      const uint64_t tenant_data_version,
      share::schema::ObTableSchema &index_schema);
  int create_global_inner_expr_index(
      ObMySQLTransaction &trans,
      const share::schema::ObTableSchema &orig_table_schema,
      const uint64_t tenant_data_version,
      share::schema::ObTableSchema &new_table_schema,
      common::ObIArray<share::schema::ObColumnSchemaV2*> &new_columns,
      share::schema::ObTableSchema &index_schema);
  template <typename SCHEMA>
  int check_primary_zone_locality_condition(
      const SCHEMA &schema,
      const ObIArray<common::ObZone> &zone_list,
      const ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      share::schema::ObSchemaGetterGuard &schema_guard);
  template <typename SCHEMA>
  int extract_first_primary_zone_array(
      const SCHEMA &schema,
      const ObIArray<common::ObZone> &zone_list,
      ObIArray<common::ObZone> &first_primary_zone_array);
  int get_primary_regions_and_zones(
      const ObIArray<common::ObZone> &zone_list,
      const ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      const ObIArray<common::ObZone> &first_primary_zone_array,
      ObIArray<common::ObRegion> &primary_regions,
      ObIArray<common::ObZone> &zones_in_primary_regions);
  int do_check_primary_zone_locality_condition(
      const ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      const ObIArray<common::ObZone> &first_primary_zone_array,
      const ObIArray<common::ObZone> &zones_in_primary_regions,
      const ObIArray<common::ObRegion> &primary_regions,
      const ObIArray<share::ObZoneReplicaAttrSet> &zone_locality);
  int do_check_primary_zone_region_condition(
      const ObIArray<common::ObZone> &zones_in_primary_regions,
      const ObIArray<common::ObRegion> &primary_regions,
      const ObIArray<share::ObZoneReplicaAttrSet> &zone_locality);
  int do_check_mixed_zone_locality_condition(
      const ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      const ObIArray<share::ObZoneReplicaAttrSet> &zone_locality);
  int do_check_mixed_locality_primary_zone_condition(
      const ObIArray<common::ObZone> &first_primary_zone_array,
      const ObIArray<share::ObZoneReplicaAttrSet> &zone_locality);
  // batch mode in trx
  virtual int create_user_tables(
      const bool if_not_exist,
      const common::ObString &ddl_stmt_str,
      const share::schema::ObErrorInfo &error_info,
      common::ObIArray<share::schema::ObTableSchema> &table_schemas,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const obrpc::ObSequenceDDLArg &sequence_ddl_arg,
      const uint64_t last_replay_log_id,
      const common::ObIArray<share::schema::ObDependencyInfo> *dependency_infos,
      ObIArray<ObMockFKParentTableSchema> &mock_fk_parent_table_schema_array,
      int64_t &ddl_task_id);

  virtual int create_table_like(const obrpc::ObCreateTableLikeArg &arg);

  virtual int alter_table(obrpc::ObAlterTableArg &alter_table_arg,
                          obrpc::ObAlterTableRes &res);
  virtual int drop_table(const obrpc::ObDropTableArg &drop_table_arg, const obrpc::ObDDLRes &res);
  int check_table_exists(const uint64_t tenant_id,
                         const obrpc::ObTableItem &table_item,
                         const share::schema::ObTableType expected_table_type,
                         share::schema::ObSchemaGetterGuard &guard,
                         const share::schema::ObTableSchema **table_schema);
  int create_hidden_table(const obrpc::ObCreateHiddenTableArg &create_hidden_table_arg,
                                      obrpc::ObCreateHiddenTableRes &res);
  int mview_complete_refresh(const obrpc::ObMViewCompleteRefreshArg &arg,
                             obrpc::ObMViewCompleteRefreshRes &res,
                             share::schema::ObSchemaGetterGuard &schema_guard);
  int mview_complete_refresh_in_trans(const obrpc::ObMViewCompleteRefreshArg &arg,
                                      obrpc::ObMViewCompleteRefreshRes &res,
                                      ObDDLSQLTransaction &trans,
                                      common::ObIAllocator &allocator,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      const uint64_t tenant_data_version,
                                      ObDDLTaskRecord &task_record);
  /**
   * For recover restore table ddl task, it is a cross-tenant task, including,
   * 1. Create a hidden table under different tenant but associated with the source table;
   * 2. Do not change any attribute of the source table;
   * 3. Create a recover ddl task to complement the data of the hidden table;
  */
  int recover_restore_table_ddl_task(
      const obrpc::ObRecoverRestoreTableDDLArg &arg);
  int check_index_on_foreign_key(const ObTableSchema *index_table_schema,
                                const common::ObIArray<ObForeignKeyInfo> &foreign_key_infos,
                                bool &have_index);
  virtual int update_index_status(const obrpc::ObUpdateIndexStatusArg &arg);
  virtual int update_mview_status(const obrpc::ObUpdateMViewStatusArg &arg);

  int upgrade_table_schema(const obrpc::ObUpgradeTableSchemaArg &arg);
  virtual int add_table_schema(share::schema::ObTableSchema &table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard);
  virtual int drop_inner_table(const share::schema::ObTableSchema &table_schema, const bool delete_priv = true);
  virtual int create_sys_tenant(const obrpc::ObCreateTenantArg &arg,
                                share::schema::ObTenantSchema &tenant_schema);
  virtual int create_tenant(const obrpc::ObCreateTenantArg &arg,
                            obrpc::UInt64 &tenant_id);
  virtual int create_tenant_end(const uint64_t tenant_id);
  virtual int commit_alter_tenant_locality(const rootserver::ObCommitAlterTenantLocalityArg &arg);
  virtual int modify_tenant(const obrpc::ObModifyTenantArg &arg);
  virtual int drop_tenant(const obrpc::ObDropTenantArg &arg);
  virtual int flashback_tenant(const obrpc::ObFlashBackTenantArg &arg);
  virtual int purge_tenant(const obrpc::ObPurgeTenantArg &arg);

  virtual int lock_tenant(const common::ObString &tenant_name, const bool is_lock);
  virtual int add_system_variable(const obrpc::ObAddSysVarArg &arg);
  virtual int modify_system_variable(const obrpc::ObModifySysVarArg &arg);
  virtual int create_database(const bool if_not_exist,
                              share::schema::ObDatabaseSchema &database_schema,
                              const common::ObString *ddl_stmt_str,
                              ObMySQLTransaction *trans = NULL);
  virtual int alter_database(const obrpc::ObAlterDatabaseArg &arg);
  virtual int drop_database(const obrpc::ObDropDatabaseArg &arg,
                            obrpc::ObDropDatabaseRes &res,
                            ObDDLSQLTransaction *trans = NULL);
  virtual int create_tablegroup(const bool if_not_exist,
                                share::schema::ObTablegroupSchema &tablegroup_schema,
                                const common::ObString *ddl_stmt_str);
  virtual int drop_tablegroup(const obrpc::ObDropTablegroupArg &arg);
  virtual int alter_tablegroup(const obrpc::ObAlterTablegroupArg &arg);
  virtual int try_format_partition_schema(share::schema::ObPartitionSchema &table_schema);
  virtual int generate_schema(const obrpc::ObCreateTableArg &arg,
                              share::schema::ObTableSchema &schema);
  int create_index_tablet(const ObTableSchema &index_schema,
                          ObMySQLTransaction &trans,
                          share::schema::ObSchemaGetterGuard &schema_guard,
                          const bool need_check_tablet_cnt,
                          const uint64_t tenant_data_version);
  virtual int alter_table_index(obrpc::ObAlterTableArg &alter_table_arg,
                                const share::schema::ObTableSchema &orgin_table_schema,
                                share::schema::ObTableSchema &new_table_schema,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                ObDDLOperator &ddl_operator,
                                ObMySQLTransaction &trans,
                                common::ObArenaAllocator &allocator,
                                const uint64_t tenant_data_version,
                                obrpc::ObAlterTableRes &res,
                                ObIArray<ObDDLTaskRecord> &ddl_tasks);
  int generate_object_id_for_partition_schemas(
      ObIArray<ObTableSchema> &partition_schemas);
  int generate_object_id_for_partition_schema(
      ObPartitionSchema &partition_schema,
      const bool gen_subpart_only = false,
      share::ObIDGenerator *batch_id_generator = NULL);
  int generate_tables_tablet_id(ObIArray<ObTableSchema> &table_schemas);
  int generate_tablet_id(ObTableSchema &schema,
                         share::ObIDGenerator *batch_id_generator = NULL);
  int alter_table_column(
      const share::schema::ObTableSchema &origin_table_schema,
      const share::schema::AlterTableSchema & alter_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      obrpc::ObAlterTableArg &alter_table_arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans,
      common::ObIArray<share::schema::ObTableSchema> *global_idx_schema_array = NULL);
  static int redistribute_column_ids(
      ObTableSchema &new_table_schema);
  int add_new_column_to_table_schema(
      const share::schema::ObTableSchema &origin_table_schema,
      const share::schema::AlterTableSchema &alter_table_schema,
      const common::ObTimeZoneInfoWrap &tz_info_wrap,
      const common::ObString &nls_formats,
      share::schema::ObLocalSessionVar &local_session_var,
      obrpc::ObSequenceDDLArg &sequence_ddl_arg,
      common::ObIAllocator &allocator,
      share::schema::ObTableSchema &new_table_schema,
      share::schema::AlterColumnSchema &alter_column_schema,
      ObIArray<ObString> &gen_col_expr_arr,
      share::schema::ObSchemaGetterGuard &schema_guard,
      uint64_t &curr_udt_set_id,
      ObDDLOperator *ddl_operator,
      common::ObMySQLTransaction *trans);
  int add_column_to_column_group(
      const share::schema::ObTableSchema &origin_table_schema,
      const share::schema::AlterTableSchema &alter_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans);
  int gen_alter_column_new_table_schema_offline(
      const share::schema::ObTableSchema &origin_table_schema,
      share::schema::AlterTableSchema &alter_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      obrpc::ObAlterTableArg &alter_table_arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &need_redistribute_column_id);
  int gen_alter_partition_new_table_schema_offline(
      const share::schema::AlterTableSchema & alter_table_schema,
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObTableSchema &new_table_schema);

  // MockFKParentTable begin
  int gen_mock_fk_parent_table_for_create_fk(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const obrpc::ObCreateForeignKeyArg &foreign_key_arg,
      const ObMockFKParentTableSchema *mock_fk_parent_table_ptr,
      ObForeignKeyInfo &foreign_key_info,
      ObMockFKParentTableSchema &mock_fk_parent_table_schema);

  int prepare_gen_mock_fk_parent_tables_for_drop_fks(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const int64_t tenant_id,
      const ObIArray<const ObForeignKeyInfo*> &foreign_key_info_array,
      ObIArray<const ObMockFKParentTableSchema*> &mock_fk_parent_table_schema_ptr_array,
      ObIArray<ObMockFKParentTableSchema> &mock_fk_parent_table_schema_array);
  int gen_mock_fk_parent_tables_for_drop_fks(
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObIArray<const ObMockFKParentTableSchema*> &mock_fk_parent_table_schema_ptr_array,
      ObIArray<ObMockFKParentTableSchema> &mock_fk_parent_table_schema_array);

  int gen_mock_fk_parent_table_for_drop_table(
      share::schema::ObSchemaService *schema_service,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const obrpc::ObDropTableArg &drop_table_arg,
      const DropTableIdHashSet &drop_table_set,
      const ObIArray<ObForeignKeyInfo> &foreign_key_infos,
      const ObForeignKeyInfo &violated_foreign_key_info,
      const ObTableSchema *table_schema,
      ObMockFKParentTableSchema &mock_fk_parent_table_schema);

  int check_fk_columns_type_for_replacing_mock_fk_parent_table(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const ObTableSchema &real_parent_table_schema,
      const ObMockFKParentTableSchema *&mock_parent_table_schema);
  int get_uk_cst_id_for_replacing_mock_fk_parent_table(
      const ObIArray<const share::schema::ObTableSchema*> &index_table_schemas,
      share::schema::ObForeignKeyInfo &foreign_key_info);
  int gen_mock_fk_parent_table_for_replacing_mock_fk_parent_table(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t drop_mock_fk_parent_table_id,
      const share::schema::ObTableSchema &real_parent_table,
      const ObIArray<const share::schema::ObTableSchema*> &uk_index_schemas,
      ObMockFKParentTableSchema &mock_fk_parent_table_schema);
  // MockFKParentTable end

  // generate two array of orig_table_schemas and new_table_schemas by orig_table_schema and new_table_schema
  //
  // @param [in] op_type, modify part ddl op
  // @param [out] orig_table_schemas, the first is orig_table_schema, others are its local indexes schema
  // @param [out] new_table_schemas, the first is new_table_schema, others are its local indexes schema
  // @param [in] orig_table_schema, orig table schema for ddl
  // @param [in] new_table_schema, new table schema for ddl
  int generate_tables_array(const obrpc::ObAlterTableArg::AlterPartitionType op_type,
                            common::ObIArray<const ObTableSchema*> &orig_table_schemas,
                            common::ObIArray<ObTableSchema*> &new_table_schemas,
                            common::ObIArray<AlterTableSchema*> &inc_table_schemas,
                            common::ObIArray<AlterTableSchema*> &del_table_schemas,
                            const ObTableSchema &orig_table_schema,
                            ObTableSchema &new_table_schema,
                            AlterTableSchema &inc_table_schema,
                            share::schema::ObSchemaGetterGuard &schema_guard,
                            ObArenaAllocator &allocator);
  bool is_add_and_drop_partition(const obrpc::ObAlterTableArg::AlterPartitionType &op_type);
  // execute alter_table_partitions for some tables which are data table and its local indexes
  //
  // @param [in] op_type, modify part ddl op
  // @param [in] orig_table_schemas, the first is orig_table_schema, others are its local indexes schema
  // @param [out] new_table_schemas, the first is new_table_schema, others are its local indexes schema
  // @param [out] inc_table_schemas, save inc_table_schemas for dropping/creating tablets
  // @param [in] inc_table_schema, modify part info
  int alter_tables_partitions(const obrpc::ObAlterTableArg &alter_table_arg,
                              common::ObIArray<const ObTableSchema*> &orig_table_schemas,
                              common::ObIArray<ObTableSchema*> &new_table_schemas,
                              common::ObIArray<AlterTableSchema*> &inc_table_schemas,
                              common::ObIArray<AlterTableSchema*> &del_table_schemas,
                              ObDDLOperator &ddl_operator,
                              ObSchemaGetterGuard &schema_guard,
                              ObMySQLTransaction &trans);
  virtual int alter_table_partitions(const obrpc::ObAlterTableArg &alter_table_arg,
                                     const share::schema::ObTableSchema &orig_table_schema,
                                     share::schema::AlterTableSchema &inc_table_schema,
                                     share::schema::AlterTableSchema &del_table_schema,
                                     share::schema::ObTableSchema &new_table_schema,
                                     ObDDLOperator &ddl_operator,
                                     ObSchemaGetterGuard &schema_guard,
                                     ObMySQLTransaction &trans,
                                     const ObTableSchema &orig_data_table_schema);
  virtual int alter_table_constraints(const obrpc::ObAlterTableArg::AlterConstraintType type,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      const share::schema::ObTableSchema &orig_table_schema,
                                      const share::schema::AlterTableSchema &inc_table_schema,
                                      share::schema::ObTableSchema &new_table_schema,
                                      ObDDLOperator &ddl_operator,
                                      ObMySQLTransaction &trans);
  virtual int alter_table_foreign_keys(const share::schema::ObTableSchema &orig_table_schema,
                                       share::schema::ObTableSchema &inc_table_schema,
                                       ObDDLOperator &ddl_operator,
                                       ObMySQLTransaction &trans);
  virtual int truncate_table(const obrpc::ObTruncateTableArg &arg,
                             const obrpc::ObDDLRes &ddl_res,
                             const share::SCN &frozen_scn);
  int get_last_schema_version(int64_t &last_schema_version);
  int check_db_and_table_is_exist(const obrpc::ObTruncateTableArg &arg,
                                  ObMySQLTransaction &trans,
                                  uint64_t &database_id,
                                  uint64_t &table_id);
  int get_index_lob_table_schema(const ObTableSchema &orig_table_schema,
                                 const ObRefreshSchemaStatus &schema_status,
                                 common::ObArray<const ObTableSchema*> &table_schemas,
                                 ObArenaAllocator &allocator,
                                 ObMySQLTransaction &trans);
  int check_table_schema_is_legal(const ObDatabaseSchema & databae_schema,
                                  const ObTableSchema &table_schema,
                                  const bool check_foreign_key,
                                  ObMySQLTransaction &trans);
  int check_is_foreign_key_parent_table(const ObTableSchema &table_schema,
                                        ObMySQLTransaction &trans);

  int drop_and_create_tablet(const int64_t &schema_version,
                             const ObIArray<const ObTableSchema*> &orig_table_schemas,
                             const ObIArray<ObTableSchema*> &new_table_schemas,
                             ObMySQLTransaction &trans);
  int generate_table_schemas(const ObIArray<const ObTableSchema*> &orig_table_schemas,
                             ObIArray<ObTableSchema*> &new_table_schemas,
                             ObIArray<int64_t> &gen_schema_version_array,
                             ObArenaAllocator &allocator,
                             int64_t &task_id);
  virtual int new_truncate_table(const obrpc::ObTruncateTableArg &arg,
                                 obrpc::ObDDLRes &ddl_res,
                                 const share::SCN &frozen_scn);
  int drop_not_null_cst_in_column_flag(const ObTableSchema &orig_table_schema,
                                       const AlterTableSchema &alter_table_schema,
                                       ObTableSchema &new_table_schema,
                                       ObDDLOperator &ddl_operator,
                                       ObMySQLTransaction &trans);

  int alter_not_null_cst_in_column_flag(const ObTableSchema &orig_table_schema,
                                       const AlterTableSchema &alter_table_schema,
                                       ObTableSchema &new_table_schema,
                                       ObDDLOperator &ddl_operator,
                                       ObMySQLTransaction &trans);

  int alter_table_auto_increment(const ObTableSchema &orig_table_schema,
                                 const AlterTableSchema &alter_table_schema,
                                 const obrpc::ObAlterTableArg &alter_table_arg,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 ObTableSchema &new_table_schema,
                                 ObDDLOperator &ddl_operator,
                                 ObMySQLTransaction &trans);

  int get_tablets(
      const uint64_t tenant_id,
      const ObArray<common::ObTabletID> &tablet_ids,
      common::ObIArray<LSTabletID> &tablets,
      ObDDLSQLTransaction &trans);
  int build_modify_tablet_binding_args(
      const uint64_t tenant_id,
      const ObArray<common::ObTabletID> &tablet_ids,
      const bool is_hidden_tablets,
      const int64_t schema_version,
      common::ObIArray<storage::ObBatchUnbindTabletArg> &args,
      ObDDLSQLTransaction &trans);
  int unbind_hidden_tablets(
      const share::schema::ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &hidden_table_schema,
      const int64_t schema_version,
      ObDDLSQLTransaction &trans);
  int write_ddl_barrier(
      const share::schema::ObTableSchema &hidden_table_schema,
      ObDDLSQLTransaction &trans);
  // Register MDS for read and write defense verification after single table ddl
  // like add column not null default null.
  int build_single_table_rw_defensive(
      const uint64_t tenant_id,
      const ObArray<common::ObTabletID> &tablet_ids,
      const int64_t schema_version,
      ObDDLSQLTransaction &trans);
  int check_hidden_table_constraint_exist(
      const ObTableSchema *hidden_table_schema,
      const ObTableSchema *orig_table_schema,
      ObSchemaGetterGuard &schema_guard);

  /**
   * This function is called by the storage layer in the three stage of offline ddl.
   * all the following steps are completed in the same trans:
   *    step1: if the original table is a parent table, after swap the status of the two tables
   *    need to rename the original fk name to the newly generated hidden fk name , and then rename
   *    the newly generated hidden fk name to the original fk name on the child table
   *    step2: modify the state of the all index tables to hidden
   *    step3: modify the state of the all hidden index tables to non-hidden
   *    step4: rename orig table name to hidden table name and modify the state to hidden
   *    step5: rename hidden table name to orig table name and modify the state to non-hidden
   */
  int swap_orig_and_hidden_table_state(obrpc::ObAlterTableArg &alter_table_arg);

  /**
   * The function is designed for the recover restore table ddl, which is to check whether the object
   * in table schema is duplicated with others in the sample table space.
   * If the object is named by the default function of the system, then a new object name will be
   * generated to replace the old.
  */
  int check_and_replace_default_index_name_on_demand(
      const bool is_oracle_mode,
      common::ObIAllocator &allocator,
      const ObTableSchema &hidden_data_schema,
      const ObString &target_data_table_name,
      ObTableSchema &new_index_schema);
  int check_and_replace_dup_constraint_name_on_demand(
      const bool is_oracle_mode,
      ObSchemaGetterGuard &tenant_schema_guard,
      ObTableSchema &hidden_data_schema,
      common::ObIAllocator &allocator,
      ObDDLOperator &ddl_operator,
      ObDDLSQLTransaction &trans);
  // The rule to recover foreign key,
  // 1. child table and parent table in the same database/user;
  // 2. child table and parent table all exist.
  // 3. child table and parent table in the destination tenant space satisfy the foreign-key built rule.
  int check_and_replace_fk_info_on_demand(
      ObSchemaGetterGuard &src_tenant_schema_guard,
      ObSchemaGetterGuard &dst_tenant_schema_guard,
      const ObTableSchema &hidden_table_schema,
      const bool is_recover_child_table,
      ObForeignKeyInfo &new_fk_info);
  // To check whether to recover the foreign key by checking columns matched, PK/Unique matched, etc.
  int check_rebuild_foreign_key_satisfy(
      obrpc::ObCreateForeignKeyArg &create_fk_arg,
      const ObTableSchema &parent_table_schema,
      const ObTableSchema &child_table_schema,
      sql::ObSchemaChecker &schema_checker,
      const ObConstraintType &expected_cst_type);
  /**
   * This function is called by the DDL RESTORE TABLE TASK.
   * This task will create a hidden table, but will not be associated with the original table,
   * and any attribute of the origin table will not change to avoid cross-tenant transactions.
   * And the following function will make the hidden table and its' rebuilt indexes visible after data filling.
  */
  int make_recover_restore_tables_visible(obrpc::ObAlterTableArg &alter_table_arg);
  /**
   * This function is called by the storage layer in the second stage of offline ddl
   * For foreign keys that refer to the columns of the original table, a corresponding hidden
   * foreign key can be created in the target table referencing the original table according to
   * the type of the foreign key. For the foreign key referenced by the original table
   * it needs to create the corresponding hidden foreign key in the new hidden table
   *
   * @param [in] alter_table_arg
   * @param [out] cst_ids: new foreign key id
   */
  int rebuild_hidden_table_foreign_key_in_trans(obrpc::ObAlterTableArg &alter_table_arg,
                                                common::ObSArray<uint64_t> &cst_ids);
  /**
   * This function is called by the storage layer in the second stage of offline ddl
   * For each constraint object in the original table, if it is a check constraint, need to
   * be able to create a check constraint object in the new hidden table
   *
   * @param [in] alter_table_arg
   * @param [out] cst_ids: new constraint id
   */
  int rebuild_hidden_table_constraints_in_trans(obrpc::ObAlterTableArg &alter_table_arg,
                                                common::ObSArray<uint64_t> &cst_ids);
  /**
   * This function is called by the storage layer in the second stage of offline ddl
   * For each index object of the original table, a related hidden index can be created in
   * the schema of the new hidden table according to the columns involved in the index of
   * the original table
   *
   * @param [in] alter_table_arg
   * @param [in] frozen_version
   * @param [out] index_ids: new index table id
   */
  int rebuild_hidden_table_index_in_trans(obrpc::ObAlterTableArg &alter_table_arg,
                                          common::ObSArray<uint64_t> &index_ids);
  /**
   * This function is called by the storage layer in the fourth stage of offline ddl
   * If successful, the original table and dependent objects related to the original table need to be cleaned up
   * If fails, need to clean up the newly created hidden table and related dependent objects created on it
   * finally, write the alter table statement to the __all_ddl_operation table
   *
   * @param [in] alter_table_arg
   * @param [in] is_success: successful cleanup or failed cleanup
   */
  int cleanup_garbage(obrpc::ObAlterTableArg &alter_table_arg);
  int modify_hidden_table_fk_state(obrpc::ObAlterTableArg &alter_table_arg);
  int modify_hidden_table_not_null_column_state(const obrpc::ObAlterTableArg &alter_table_arg);
  int maintain_obj_dependency_info(const obrpc::ObDependencyObjDDLArg &arg);
  int process_schema_object_dependency(
      const uint64_t tenant_id,
      const bool is_standby,
      const share::schema::ObReferenceObjTable::DependencyObjKeyItemPairs &dep_objs,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObMySQLTransaction &trans,
      ObDDLOperator &ddl_operator,
      share::schema::ObReferenceObjTable::ObSchemaRefObjOp op);
  int fill_truncate_table_fk_err_msg(const share::schema::ObForeignKeyInfo &foreign_key_info,
                                     share::schema::ObSchemaGetterGuard &schema_guard,
                                     const share::schema::ObTableSchema &parent_table_schema,
                                     char* buf,
                                     const int64_t &buf_len,
                                     int64_t &pos) const;
  int fill_truncate_table_fk_err_msg_without_schema_guard(const share::schema::ObForeignKeyInfo &foreign_key_info,
                                                          const share::schema::ObTableSchema &parent_table_schema,
                                                          const ObRefreshSchemaStatus &schema_status,
                                                          ObMySQLTransaction &trans,
                                                          char *buf,
                                                          const int64_t &buf_len,
                                                          int64_t &pos) const;
  int get_uk_cst_id_for_self_ref(const ObIArray<share::schema::ObTableSchema> &table_schemas,
                                 const obrpc::ObCreateForeignKeyArg &foreign_key_arg,
                                 share::schema::ObForeignKeyInfo &foreign_key_info);
  int check_constraint_name_is_exist(share::schema::ObSchemaGetterGuard &schema_guard,
                                     const share::schema::ObTableSchema &table_schema,
                                     const common::ObString &constraint_name,
                                     const bool is_foreign_key, // this param is only effective in mysql mode
                                     bool &is_constraint_name_exist);
int check_udt_id_is_exist(share::schema::ObSchemaGetterGuard &schema_guard,
                          const share::schema::ObColumnSchemaV2 &col_schema,
                          const uint64_t tenant_id);
int check_table_udt_id_is_exist(share::schema::ObSchemaGetterGuard &schema_guard,
                                const share::schema::ObTableSchema &table_schema,
                                const uint64_t tenant_id);
  int check_cst_name_dup_for_rename_table_mysql(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema *from_table_schema,
      const uint64_t to_database_id);
  int deal_with_cst_for_alter_table(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const ObTableSchema *orig_table_schema,
      obrpc::ObAlterTableArg &alter_table_arg,
      ObMockFKParentTableSchema &mock_fk_parent_table_schema);

  int check_can_drop_primary_key(
      const share::schema::ObTableSchema &orgin_table_schema);
  int drop_primary_key(ObTableSchema &new_table_schema);
  int add_primary_key(
      const common::ObIArray<common::ObString> &pk_column_names,
      share::schema::ObTableSchema &new_table_schema);
  // 1. table with primary key -> hidden table with new primary key, i.e., modify primary key.
  // 2. table without primary key -> hidden table with primary key, i.e., add primary key.
  // 3. table with primary key -> hidden table without primary key, i.e., drop primary key.
  int create_hidden_table_with_pk_changed(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const common::ObSArray<common::ObString> &index_columns,
      const share::schema::ObTableSchema &orgin_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans,
      common::ObArenaAllocator &allocator,
      const obrpc::ObIndexArg::IndexActionType &index_action_type,
      const uint64_t tenant_data_version);
  int get_add_pk_index_name(const share::schema::ObTableSchema &origin_table_schema,
                            share::schema::ObTableSchema &new_table_schema,
                            const obrpc::ObIndexArg::IndexActionType &index_action_type,
                            const common::ObIArray<obrpc::ObIndexArg *> &index_arg_list,
                            share::schema::ObSchemaGetterGuard &schema_guard,
                            ObString &index_name);
  virtual int rename_table(const obrpc::ObRenameTableArg &rename_table_arg);
  int collect_temporary_tables_in_session(const obrpc::ObDropTableArg &drop_table_arg);
  int need_collect_current_temp_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                      obrpc::ObDropTableArg &drop_table_arg,
                                      const share::schema::ObSimpleTableSchemaV2 *table_schema,
                                      bool &need_collect);
  int check_sessid_exist_in_temp_table(const ObString &db_name,
                                       const ObString &tab_name,
                                       const uint64_t tenant_id,
                                       const uint64_t session_id,
                                       bool &exists);
  int check_table_exist(share::schema::ObTableSchema &table_schema);
  int check_database_exist(const uint64_t tenant_id, const common::ObString &database_name, uint64_t &database_id);
  int check_create_with_db_id(share::schema::ObDatabaseSchema &schema);
  int replace_table_schema_type(share::schema::ObTableSchema &schema);

  int flashback_table_to_time_point(const obrpc::ObFlashBackTableToScnArg &arg);
  //----Functions for recyclebin ----

  int check_object_name_matches_db_name(
      const uint64_t tenant_id,
      const ObString &origin_table_name,
      const uint64_t database_id,
      bool &is_match);
  static int get_object_name_with_origin_name_in_recyclebin(
      const uint64_t tenant_id,
      const ObString &origin_table_name,
      const uint64_t database_id,
      const ObRecycleObject::RecycleObjType recycle_type,
      ObString &object_name,
      const bool is_newest,
      common::ObIAllocator *allocator,
      common::ObMySQLProxy *sql_proxy);
  int flashback_table_from_recyclebin(const obrpc::ObFlashBackTableFromRecyclebinArg &arg);
  int flashback_table_from_recyclebin_in_trans(const share::schema::ObTableSchema &table_schema,
                               const uint64_t new_db_id,
                               const common::ObString &new_table_name,
                               const common::ObString &ddl_stmt_str,
                               share::schema::ObSchemaGetterGuard &guard);
  int flashback_aux_table(const share::schema::ObTableSchema &table_schema,
                          share::schema::ObSchemaGetterGuard &schema_guard,
                          ObMySQLTransaction &trans,
                          ObDDLOperator &ddl_operator,
                          const uint64_t new_db_id,
                          const share::schema::ObTableType table_type);
  int flashback_trigger(const share::schema::ObTableSchema &table_schema,
                        const uint64_t new_database_id,
                        const common::ObString &new_table_name,
                        share::schema::ObSchemaGetterGuard &schema_guard,
                        ObMySQLTransaction &trans,
                        ObDDLOperator &ddl_operator);
  int purge_table(const obrpc::ObPurgeTableArg &arg, ObMySQLTransaction *trans = NULL);

  int flashback_database(const obrpc::ObFlashBackDatabaseArg &arg);
  int flashback_database_in_trans(const share::schema::ObDatabaseSchema &db_schema,
                                  const common::ObString &new_db_name,
                                  share::schema::ObSchemaGetterGuard &guard,
                                  const common::ObString &ddl_stmt_str);
  int purge_database(const obrpc::ObPurgeDatabaseArg &arg, ObMySQLTransaction *trans = NULL);
  int purge_tenant_expire_recycle_objects(const obrpc::ObPurgeRecycleBinArg &arg,
                                          int64_t &purge_expire_objects);
  int purge_recyclebin_except_tenant(
      const obrpc::ObPurgeRecycleBinArg &arg,
      const ObIArray<share::schema::ObRecycleObject> &recycle_objs,
      int64_t &purged_objects);
  int purge_recyclebin_tenant(
      const obrpc::ObPurgeRecycleBinArg &arg,
      const ObIArray<share::schema::ObRecycleObject> &recycle_objs,
      int64_t &purged_objects);
  int flashback_index(const obrpc::ObFlashBackIndexArg &arg);
  int flashback_index_in_trans(share::schema::ObSchemaGetterGuard &schema_guard,
                               const share::schema::ObTableSchema &table_schema,
                               const uint64_t new_db_id,
                               const common::ObString &new_table_name,
                               const common::ObString &ddl_stmt_str);
  int purge_index(const obrpc::ObPurgeIndexArg &arg);
  int create_user(obrpc::ObCreateUserArg &arg,
                  common::ObIArray<int64_t> &failed_index);
  virtual int revoke_all(const uint64_t tenant_id,
                         const common::ObString &user_name,
                         const common::ObString &host_name,
                         const uint64_t user_id,
                         share::schema::ObSchemaGetterGuard &schema_guard);
  int rename_user(const obrpc::ObRenameUserArg &arg,
                  ObIArray<int64_t> &failed_index);
  int set_passwd(const obrpc::ObSetPasswdArg &arg);
  int drop_user(const obrpc::ObDropUserArg &arg,
                common::ObIArray<int64_t> &failed_index);
  int alter_role(const obrpc::ObAlterRoleArg &arg);
  int alter_role_in_trans(const uint64_t tenant_id,
                          const uint64_t role_id,
                          const common::ObString &new_passwd,
                          const common::ObString *ddl_stmt_str,
                          share::schema::ObSchemaGetterGuard &schema_guard);

  int grant_sys_priv_to_ur(const obrpc::ObGrantArg &arg,
                           share::schema::ObSchemaGetterGuard &schema_guard);
  int exists_role_grant_cycle(share::schema::ObSchemaGetterGuard &schema_guard,
                              const uint64_t tenant_id,
                              const ObUserInfo &role_info,
                              const share::schema::ObUserInfo *user_info,
                              const bool is_oracle_mode);
  virtual int grant(const obrpc::ObGrantArg &arg);
  int revoke(const obrpc::ObRevokeUserArg &arg);
  virtual int grant_priv_to_user(const uint64_t tenant_id,
                                 const uint64_t user_id,
                                 const common::ObString &user_name,
                                 const common::ObString &host_name,
                                 const share::schema::ObNeedPriv &need_priv,
                                 const share::ObRawObjPrivArray &obj_priv_array,
                                 const uint64_t option,
                                 const bool is_from_inner_sql,
                                 share::schema::ObObjPrivSortKey &obj_priv_key,
                                 share::schema::ObSchemaGetterGuard &schema_guard);
  virtual int grant_table_and_col_privs_to_user(
      const obrpc::ObGrantArg &arg,
      uint64_t grantee_id,
      ObString &user_name,
      ObString &host_name,
      share::schema::ObNeedPriv &need_priv,
      share::schema::ObSchemaGetterGuard &schema_guard);
  virtual int grant_revoke_user(const uint64_t tenant_id,
                                const uint64_t user_id,
                                const ObPrivSet priv_set,
                                const bool grant,
                                const bool is_from_inner_sql,
                                const common::ObString *ddl_stmt_str,
                                share::schema::ObSchemaGetterGuard &schema_guard);

 int grant_or_revoke_column_priv_mysql(const uint64_t tenant_id,
                                        const uint64_t table_id,
                                        const uint64_t user_id,
                                        const ObString& user_name,
                                        const ObString& host_name,
                                        const ObString& db,
                                        const ObString& table,
                                        const ObIArray<std::pair<ObString, ObPrivType>> &column_names_priv,
                                        ObDDLOperator &ddl_operator,
                                        ObDDLSQLTransaction &trans,
                                        ObSchemaGetterGuard &schema_guard,
                                        const bool is_grant);

  int grant_table_and_column_mysql(const obrpc::ObGrantArg &arg,
                                         uint64_t user_id,
                                         const ObString &user_name,
                                         const ObString &host_name,
                                         const ObNeedPriv &need_priv,
                                         share::schema::ObSchemaGetterGuard &schema_guard);
  int lock_user(const obrpc::ObLockUserArg &arg, common::ObIArray<int64_t> &failed_index);
  int standby_grant(const obrpc::ObStandbyGrantArg &arg);

  int alter_user_profile(const obrpc::ObAlterUserProfileArg &arg);
  int alter_user_default_role(const obrpc::ObAlterUserProfileArg &arg);
  int alter_user_proxy(const obrpc::ObAlterUserProxyArg &arg);
  int get_all_users_in_tenant_with_profile(const uint64_t tenant_id,
                                           const uint64_t profile_id,
                                           share::schema::ObSchemaGetterGuard &schema_guard,
                                           ObIArray<uint64_t> &user_ids);
  virtual int grant_database(const share::schema::ObOriginalDBKey &db_key,
                             const ObPrivSet priv_set,
                             const common::ObString *ddl_stmt_str,
                             share::schema::ObSchemaGetterGuard &schema_guard);
  virtual int revoke_database(const share::schema::ObOriginalDBKey &db_key,
                              const ObPrivSet priv_set);
  virtual int revoke_syspriv(const uint64_t tenant_id,
                             const uint64_t grantee_id,
                             const share::ObRawPrivArray &sys_priv_array,
                             const common::ObSArray<uint64_t> &role_ids,
                             const common::ObString *ddl_stmt_str);
  virtual int revoke_role_inner_trans(ObDDLOperator &ddl_operator,
                                      ObMySQLTransaction &trans,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      const uint64_t tenant_id,
                                      const uint64_t user_id,
                                      const common::ObSArray<uint64_t> &role_ids,
                                      const bool log_operation);
  virtual int grant_table(const share::schema::ObTablePrivSortKey &table_key,
                          const ObPrivSet priv_set,
                          const common::ObString *ddl_stmt_str,
                          share::schema::ObSchemaGetterGuard &schema_guard);
  virtual int grant_table(const share::schema::ObTablePrivSortKey &table_key,
                          const ObPrivSet priv_set,
                          const common::ObString *ddl_stmt_str,
                          const share::ObRawObjPrivArray &obj_priv_array,
                          const uint64_t option,
                          const share::schema::ObObjPrivSortKey &obj_key,
                          share::schema::ObSchemaGetterGuard &schema_guard);

  virtual int grant_routine(
    const share::schema::ObRoutinePrivSortKey &routine_key,
    const ObPrivSet priv_set,
    const ObString *ddl_stmt_str,
    const uint64_t option,
    share::schema::ObSchemaGetterGuard &schema_guard);

  virtual int revoke_routine(
    const share::schema::ObRoutinePrivSortKey &routine_key,
    const ObPrivSet priv_set);
  virtual int revoke_table(const obrpc::ObRevokeTableArg &arg,
                           const share::schema::ObTablePrivSortKey &table_key,
                           const ObPrivSet priv_set,
                           const share::schema::ObObjPrivSortKey &obj_key,
                           const share::ObRawObjPrivArray &obj_priv_array,
                           const bool revoke_all_ora);
  virtual int revoke_table_and_column_mysql(const obrpc::ObRevokeTableArg& arg);
  //----End of functions for managing privileges----
  //----Functions for managing outlines----
  virtual int check_outline_exist(share::schema::ObOutlineInfo &Outline_info,
                                  const bool create_or_replace, bool &is_update);
  virtual int create_outline(share::schema::ObOutlineInfo &outline_info, const bool is_update,
                             const common::ObString *ddl_stmt_str,
                             share::schema::ObSchemaGetterGuard &schema_guard);
  virtual int alter_outline(const obrpc::ObAlterOutlineArg &alter_outline_arg);
  virtual int drop_outline(const obrpc::ObDropOutlineArg &drop_outline_arg);
  //----End of functions for managing outlines----
  //----Functions for managing dblinks----
  virtual int create_dblink(const obrpc::ObCreateDbLinkArg &arg,
                            const common::ObString *ddl_stmt_str);
  virtual int drop_dblink(const obrpc::ObDropDbLinkArg &arg,
                          const common::ObString *ddl_stmt_str);
  //----End of functions for managing dblinks----
  //----Functions for managing synonym----
  virtual int check_synonym_exist(share::schema::ObSynonymInfo &Synonym_info, const bool create_or_replace, bool &is_update);
  virtual int create_synonym(share::schema::ObSynonymInfo &synonym_info,
                             const ObDependencyInfo &dep_info,
                             const common::ObString *ddl_stmt_str,
                             bool is_replace,
                             share::schema::ObSchemaGetterGuard &schema_guard);
  virtual int drop_synonym(const obrpc::ObDropSynonymArg &drop_synonym_arg);
  //----End of functions for managing synonym----

  //----Functions for managing udf----
  virtual int create_user_defined_function(share::schema::ObUDF &udf_info,
                                           const common::ObString &ddl_stmt_str);
  virtual int drop_user_defined_function(const obrpc::ObDropUserDefinedFunctionArg &drop_func_arg);
  virtual int check_udf_exist(uint64 tenant_id, const common::ObString &name, bool &is_exsit, uint64_t &udf_id);
  //----End of functions for managing udf----

  //----Functions for managing routine----
  virtual int create_routine(share::schema::ObRoutineInfo &routine_info,
                             const share::schema::ObRoutineInfo *old_routine_info,
                             bool replace,
                             share::schema::ObErrorInfo &error_info,
                             common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                             const common::ObString *ddl_stmt_str,
                             share::schema::ObSchemaGetterGuard &schema_guard);
  virtual int alter_routine(const share::schema::ObRoutineInfo &routine_info,
                            share::schema::ObErrorInfo &error_info,
                            const ObString *ddl_stmt_str,
                            share::schema::ObSchemaGetterGuard &schema_guard);
  virtual int drop_routine(const share::schema::ObRoutineInfo &routine_info,
                           share::schema::ObErrorInfo &error_info,
                           const common::ObString *ddl_stmt_str,
                           share::schema::ObSchemaGetterGuard &schema_guard);
  //----End of functions for managing routine----

  //----Functions for managing udt----
  virtual int create_udt(share::schema::ObUDTTypeInfo &routine_info,
                         const share::schema::ObUDTTypeInfo *old_routine_info,
                         ObIArray<share::schema::ObRoutineInfo> &public_routine_infos,
                         share::schema::ObErrorInfo &error_info,
                         share::schema::ObSchemaGetterGuard &schema_guard,
                         common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                         const common::ObString *ddl_stmt_str,
                         bool need_replace,
                         bool exist_valid_udt,
                         bool specify_force);
  virtual int drop_udt(const share::schema::ObUDTTypeInfo &routine_info,
                       share::schema::ObSchemaGetterGuard &schema_guard,
                       const common::ObString *ddl_stmt_str,
                       bool specify_force,
                       bool exist_valid_udt);
  //----End of functions for managing routine----

  //----Functions for managing package----
  virtual int create_package(share::schema::ObSchemaGetterGuard &schema_guard,
                             const share::schema::ObPackageInfo *old_package_info,
                             share::schema::ObPackageInfo &new_package_info,
                             common::ObIArray<share::schema::ObRoutineInfo> &public_routine_infos,
                             share::schema::ObErrorInfo &error_info,
                             common::ObIArray<share::schema::ObDependencyInfo> &dep_infos,
                             const common::ObString *ddl_stmt_str);
  virtual int alter_package(share::schema::ObSchemaGetterGuard &schema_guard,
                            ObPackageInfo &package_info,
                            ObIArray<ObRoutineInfo> &public_routine_infos,
                            share::schema::ObErrorInfo &error_info,
                            const common::ObString *ddl_stmt_str);
  virtual int drop_package(const share::schema::ObPackageInfo &package_info,
                           share::schema::ObErrorInfo &error_info,
                           const common::ObString *ddl_stmt_str);
  //----End of functions for managing package----

  //----Functions for managing trigger----
  virtual int create_trigger(const obrpc::ObCreateTriggerArg &arg,
                             ObSchemaGetterGuard &schema_guard,
                             obrpc::ObCreateTriggerRes *res);
  virtual int drop_trigger(const obrpc::ObDropTriggerArg &arg);
  virtual int alter_trigger(const obrpc::ObAlterTriggerArg &arg,
                            obrpc::ObRoutineDDLRes *res = nullptr);
  //----End of functions for managing trigger----

  //----Functions for managing sequence----
  int do_sequence_ddl(const obrpc::ObSequenceDDLArg &arg);
  //----End of functions for managing sequence----

  //----Functions for managing context----
  int do_context_ddl(const obrpc::ObContextDDLArg &arg);
  //----End of functions for managing context----

  //----Functions for managing schema revise----
  int do_schema_revise(const obrpc::ObSchemaReviseArg &arg);
  //----End of functions for managing schema revise----

  // keystore
  int do_keystore_ddl(const obrpc::ObKeystoreDDLArg &arg);

  //----Functions for managing label security policy----
  int handle_label_se_policy_ddl(const obrpc::ObLabelSePolicyDDLArg &arg);
  int handle_label_se_component_ddl(const obrpc::ObLabelSeComponentDDLArg &arg);
  int handle_label_se_label_ddl(const obrpc::ObLabelSeLabelDDLArg &arg);
  int handle_label_se_user_level_ddl(const obrpc::ObLabelSeUserLevelDDLArg &arg);
  //----End of functions for managing label security policy----
  // tablespace
  int do_tablespace_ddl(const obrpc::ObTablespaceDDLArg &arg);

  //----Functions for managing profile----
  int handle_profile_ddl(const obrpc::ObProfileDDLArg &arg);
  //----End of functions for managing profile----

  //----Functions for directory object----
  int create_directory(const obrpc::ObCreateDirectoryArg &arg, const ObString *ddl_stmt_str);
  int drop_directory(const obrpc::ObDropDirectoryArg &arg, const ObString *ddl_stmt_str);
  //----End of functions for directory object----

  //----Functions for managing row level security----
  int handle_rls_policy_ddl(const obrpc::ObRlsPolicyDDLArg &arg);
  int handle_rls_group_ddl(const obrpc::ObRlsGroupDDLArg &arg);
  int handle_rls_context_ddl(const obrpc::ObRlsContextDDLArg &arg);
  //----End of functions for managing row level security----

  // refresh local schema busy wait
  virtual int refresh_schema(const uint64_t tenant_id, int64_t *schema_version = NULL);
  // notify other servers to refresh schema (call switch_schema  rpc)
  virtual int notify_refresh_schema(const common::ObAddrIArray &addrs);

  //TODO(jingqian): only used by random_choose_unit
  inline int set_sys_units(const common::ObIArray<share::ObUnit> &sys_units);

  obrpc::ObCommonRpcProxy *get_common_rpc() { return common_rpc_; }
  int get_tenant_schema_guard_with_version_in_inner_table(const uint64_t tenant_id, share::schema::ObSchemaGetterGuard &schema_guard);
  /**
   * NOTICE: The interface is designed for Offline DDL operation only.
   * The caller can not obtain the schema via the hold_buf_src_tenant_schema_guard whose
   * validity is limited by whether src_tenant_id and dst_tenant_id are the same.
   *
   * 1. This interface will provide the same tenant schema guard when src_tenant_id = dst_tenant_id,
   *    to avoid using two different versions of the guard caused by the parallel ddl under the tenant.
   * 2. This interface will provide corresponding tenant schema guard when src_tenant_id != dst_tenant_id.
   *
   * @param [in] src_tenant_id
   * @param [in] dst_tenant_id
   * @param [in] hold_buf_src_tenant_schema_guard: hold buf, invalid when src_tenant_id = dst_tenant_id.
   * @param [in] hold_buf_dst_tenant_schema_guard: hold buf.
   * @param [out] src_tenant_schema_guard:
   *    pointer to the hold_buf_dst_tenant_schema_guard if src_tenant_id = dst_tenant_id,
   *    pointer to the hold_buf_src_tenant_schema_guard if src_tenant_id != dst_tenant_id,
   *    is always not nullptr if the interface return OB_SUCC.
* @param [out] dst_tenant_schema_guard:
   *    pointer to the hold_buf_dst_tenant_schema_guard,
   *    is always not nullptr if the interface return OB_SUCC.
  */
  int get_tenant_schema_guard_with_version_in_inner_table(
    const uint64_t src_tenant_id,
    const uint64_t dst_tenant_id,
    share::schema::ObSchemaGetterGuard &hold_buf_src_tenant_schema_guard,
    share::schema::ObSchemaGetterGuard &hold_buf_dst_tenant_schema_guard,
    share::schema::ObSchemaGetterGuard *&src_tenant_schema_guard,
    share::schema::ObSchemaGetterGuard *&dst_tenant_schema_guard);
  int drop_index_to_recyclebin(const share::schema::ObTableSchema &table_schema);
  int check_tenant_in_alter_locality(const uint64_t tenant_id, bool &in_alter_locality);
  // trigger
  int create_trigger_in_trans(share::schema::ObTriggerInfo &new_trigger_info,
                              share::schema::ObErrorInfo &error_info,
                              ObIArray<ObDependencyInfo> &dep_infos,
                              const common::ObString *ddl_stmt_str,
                              bool for_insert_errors,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              int64_t &table_schema_version);
  int drop_trigger_in_trans(const share::schema::ObTriggerInfo &trigger_info,
                            const common::ObString *ddl_stmt_str,
                            share::schema::ObSchemaGetterGuard &schema_guard);
  int try_get_exist_trigger(share::schema::ObSchemaGetterGuard &schema_guard,
                            const share::schema::ObTriggerInfo &new_trigger_info,
                            const share::schema::ObTriggerInfo *&old_trigger_info,
                            bool with_replace);
  int rebuild_trigger_package(share::schema::ObSchemaGetterGuard &schema_guard,
                              const share::schema::ObTableSchema &table_schema,
                              ObDDLOperator &ddl_operator,
                              ObMySQLTransaction &trans);
  int rebuild_trigger_package(share::schema::ObSchemaGetterGuard &schema_guard,
                              const uint64_t tenant_id,
                              const common::ObIArray<uint64_t> &trigger_list,
                              const common::ObString &database_name,
                              const common::ObString &table_name,
                              ObDDLOperator &ddl_operator,
                              ObMySQLTransaction &trans);
  int create_trigger_for_truncate_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                        const common::ObIArray<uint64_t> &origin_trigger_list,
                                        share::schema::ObTableSchema &new_table_schema,
                                        ObDDLOperator &ddl_operator,
                                        ObMySQLTransaction &trans);
  int adjust_trigger_action_order(share::schema::ObSchemaGetterGuard &schema_guard,
                                  ObDDLSQLTransaction &trans,
                                  ObDDLOperator &ddl_operator,
                                  ObTriggerInfo &trigger_info,
                                  bool is_create_trigger);
  int recursive_alter_ref_trigger(share::schema::ObSchemaGetterGuard &schema_guard,
                                  ObDDLSQLTransaction &trans,
                                  ObDDLOperator &ddl_operator,
                                  const ObTriggerInfo &ref_trigger_info,
                                  const common::ObIArray<uint64_t> &trigger_list,
                                  const ObString &trigger_name,
                                  int64_t action_order);
  int recursive_check_trigger_ref_cyclic(share::schema::ObSchemaGetterGuard &schema_guard,
                                         const ObTriggerInfo &ref_trigger_info,
                                         const common::ObIArray<uint64_t> &trigger_list,
                                         const ObString &create_trigger_name,
                                         const ObString &generate_cyclic_name);

  // only push schema version, and publish schema
  int log_nop_operation(const obrpc::ObDDLNopOpreatorArg &arg);

  virtual int publish_schema(const uint64_t tenant_id);

  virtual int publish_schema_and_get_schema_version(const uint64_t tenant_id,
                                                    const common::ObAddrIArray &addrs,
                                                    int64_t *schema_version = NULL);

  int force_set_locality(
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTenantSchema &new_tenant);

  // column redefinition (drop column) in 4.0
  int delete_column_from_schema(obrpc::ObAlterTableArg &alter_table_arg);
  int delete_constraint_update_new_table(
      const AlterTableSchema &alter_table_schema,
      ObTableSchema &new_table_schema);
  int update_new_table_rls_flag(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<int64_t> &drop_cols_id_arr,
      ObTableSchema &table_schema);
  int drop_column_update_new_table(
      const ObTableSchema &origin_table_schema,
      ObTableSchema &new_table_schema,
      const ObColumnSchemaV2 &orig_column_schema);
  // remap index tables of orig_data_table to hidden table.
  int remap_index_tablets_to_new_indexs(
      obrpc::ObAlterTableArg &alter_table_arg,
      const ObTableSchema &orig_table_schema,
      const ObTableSchema &hidden_table_schema,
      ObSchemaGetterGuard &schema_guard,
      ObSArray<ObTableSchema> &table_schemas,
      common::ObMySQLTransaction &trans);
  int swap_orig_and_hidden_table_state(
      obrpc::ObAlterTableArg &alter_table_arg,
      const ObTableSchema &orig_table_schema,
      const ObTableSchema &hidden_table_schema,
      ObSchemaGetterGuard &schema_guard,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans,
      ObSArray<ObTableSchema> &new_table_schemas);
  int remap_index_tablets_and_take_effect(
      obrpc::ObAlterTableArg &alter_table_arg);
  int update_autoinc_schema(obrpc::ObAlterTableArg &alter_table_arg);
  int build_aux_lob_table_schema_if_need(ObTableSchema &data_table_schema,
                                         ObIArray<ObTableSchema> &table_schemas);
  int rename_dropping_index_name(
      const uint64_t data_table_id,
      const uint64_t database_id,
      const bool is_inner_and_fts_index,
      const obrpc::ObDropIndexArg &drop_index_arg,
      ObSchemaGetterGuard &schema_guard,
      ObDDLOperator &ddl_operator,
      ObMySQLTransaction &trans,
      common::ObIArray<share::schema::ObTableSchema> &new_index_schemas);
  int get_index_schema_by_name(
      const uint64_t data_table_id,
      const uint64_t database_id,
      const obrpc::ObDropIndexArg &drop_index_arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema *&index_table_schema);

  // lock partition, unlock when ddl trans end
  int lock_partitions(ObMySQLTransaction &trans,
                      const share::schema::ObTableSchema &table_schema);
  int lock_tablets(ObMySQLTransaction &trans,
                   const int64_t tenant_id,
                   const int64_t table_id,
                   const ObTabletIDArray &tablet_ids);
  // lock table, unlock when ddl trans end
  int lock_table(ObMySQLTransaction &trans,
                 const ObSimpleTableSchemaV2 &table_schema);
  // lock mview object, unlock when ddl trans end
  // Must before locking the container table
  int lock_mview(ObMySQLTransaction &trans, const ObSimpleTableSchemaV2 &table_schema);
  int recompile_view(const ObTableSchema &view_schema, const bool reset_view_column_infos, ObDDLSQLTransaction &trans);
  int recompile_all_views_batch(const uint64_t tenant_id, const common::ObIArray<uint64_t > &view_ids);
  int try_add_dep_info_for_all_synonyms_batch(const uint64_t tenant_id, const common::ObIArray<uint64_t> &synonym_ids);
  int try_check_and_set_table_schema_in_tablegroup(
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTableSchema &schema);

  int reset_parallel_cache(const uint64_t tenant_id);
private:
  enum PartitionBornMethod : int64_t
  {
    PBM_INVALID = 0,
    PBM_DIRECTLY_CREATE,
    PBM_BINDING,
    PBM_MAX,
  };
  enum AlterLocalityOp
  {
    ALTER_LOCALITY = 0,
    ROLLBACK_ALTER_LOCALITY,
    NOP_LOCALITY_OP,
    ALTER_LOCALITY_OP_INVALID,
  };
  enum CreateTenantStatus
  {
    CREATE_TENANT_FAILED,
    CREATE_TENANT_TRANS_ONE_DONE,
    CREATE_TENANT_TRANS_TWO_DONE,
    CREATE_TENANT_TRANS_THREE_DONE,
  };
  enum RenameOracleObjectType
  {
    RENAME_TYPE_INVALID = 0,
    RENAME_TYPE_TABLE_OR_VIEW = 1,
    RENAME_TYPE_SYNONYM = 2,
    RENAME_TYPE_SEQUENCE = 3,
  };
  typedef common::hash::ObHashMap<
          common::ObZone,
          share::ObZoneReplicaNumSet,
          common::hash::NoPthreadDefendMode> ZoneLocalityMap;
  typedef ZoneLocalityMap::const_iterator const_zone_map_iterator;
  typedef ZoneLocalityMap::iterator zone_map_iterator;

  typedef common::ObArray<share::ObUnit> ObPartitionUnits;
  typedef common::ObArray<ObPartitionUnits> ObPartitionUnitsArray;
  static const int64_t WAIT_ELECT_LEADER_TIMEOUT_US = 120 * 1000 * 1000;  // 120s
  static const int64_t REFRESH_SCHEMA_INTERVAL_US = 500 * 1000;              //500ms

  enum AlterLocalityType
  {
    TO_NEW_LOCALITY = 0,
    ROLLBACK_LOCALITY,
    LOCALITY_NOT_CHANGED,
    ALTER_LOCALITY_INVALID,
  };

  int calc_partition_object_id_cnt_(
      const ObPartitionSchema &partition_schema,
      const bool gen_subpart_only,
      int64_t &object_cnt);
  int calc_table_tablet_id_cnt_(
      const ObTableSchema &table_schema,
      uint64_t &tablet_cnt);

  int check_has_domain_index(
      ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      bool &fts_exist);
  int check_has_index_operation(
      ObSchemaGetterGuard &schema_guard,
      const uint64_t teannt_id,
      const uint64_t table_id,
      bool &has_index_operation);
  int check_is_adding_constraint(const uint64_t tenant_id, const uint64_t table_id, bool &is_building);
  int modify_tenant_inner_phase(const obrpc::ObModifyTenantArg &arg,
      const ObTenantSchema *orig_tenant_schema,
      ObSchemaGetterGuard &schema_guard,
      bool is_standby,
      bool is_restore);
  int update_global_index(obrpc::ObAlterTableArg &arg,
      const uint64_t tenant_id,
      const share::schema::ObTableSchema &orig_table_schema,
      ObDDLOperator &ddl_operator,
      ObMySQLTransaction &trans,
      const uint64_t tenant_data_version);
  int fill_interval_info_for_set_interval(const ObTableSchema &orig_table_schema,
      ObTableSchema &new_table_schema,
      AlterTableSchema &inc_table_schema);
  int fill_interval_info_for_offline(const ObTableSchema &orig_table_schema,
                                     ObTableSchema &new_table_schema);
  int reset_interval_info_for_interval_to_range(ObTableSchema &new_table_schema);
  int check_rename_object_type(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const uint64_t database_id,
      const ObString &object_name,
      const share::schema::ObTableSchema *&table_schema,
      const share::schema::ObSynonymInfo *&synonym_info,
      const share::schema::ObSequenceSchema *&sequence_schema,
      RenameOracleObjectType &obj_type);
  int record_tenant_locality_event_history(
      const AlterLocalityOp &alter_locality_op,
      const obrpc::ObModifyTenantArg &arg,
      const share::schema::ObTenantSchema &tenant_schema,
      ObMySQLTransaction &trans);
  int check_inner_stat() const;
  template <typename SCHEMA>
  int check_pools_unit_num_enough_for_schema_locality(
      const common::ObIArray<share::ObResourcePoolName> &pools,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const SCHEMA &schema);
  int get_zones_in_region(
      const common::ObRegion &region,
      const common::ObIArray<common::ObString> &zone_list,
      common::ObIArray<common::ObZone> &zones);
  int get_sample_table_schema(
      common::ObIArray<const share::schema::ObSimpleTableSchemaV2 *> &table_schemas,
      const share::schema::ObSimpleTableSchemaV2 *&sample_table_schema);
  int get_valid_index_schema_by_id_for_drop_index_(
      const uint64_t data_table_id,
      const obrpc::ObDropIndexArg &drop_index_arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema *&index_table_schema);
  int set_tablegroup_id(share::schema::ObTableSchema &table_schema);
  template<typename SCHEMA>
  int set_default_tablegroup_id(SCHEMA &schema);
  int create_index_or_mlog_table_in_trans(share::schema::ObTableSchema &table_schema,
                            const common::ObString *ddl_stmt_str,
                            ObMySQLTransaction *sql_trans,
                            share::schema::ObSchemaGetterGuard &schema_guard,
                            const bool need_check_tablet_cnt,
                            const uint64_t tenant_data_version);
  int create_tablets_in_trans_(common::ObIArray<share::schema::ObTableSchema> &table_schemas,
                              ObDDLOperator &ddl_operator,
                              ObMySQLTransaction &trans,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              const uint64_t tenant_data_version);
  int create_tablets_in_trans_for_mv_(common::ObIArray<share::schema::ObTableSchema> &table_schemas,
                              ObDDLOperator &ddl_operator,
                              ObMySQLTransaction &trans,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              const uint64_t tenant_data_version);

  /*
   * Check and set various options of modify tenant, among which the modifications of zone_list,
   *  locality and resource_pool are related to each other.
   * 1. Individual modification of the tenant's zone_list is not supported; the result of zone_list is calculated
   *  by locality and resource_pool.
   * 2. When modifying the locality, only support adding one, deleting one and modifying the locality of a zone.
   */
  int set_new_tenant_options(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const obrpc::ObModifyTenantArg &arg,
      share::schema::ObTenantSchema &new_tenant_schema,
      const share::schema::ObTenantSchema &orig_tenant_schema,
      AlterLocalityOp &alter_locality_op);
  int set_raw_tenant_options(
      const obrpc::ObModifyTenantArg &arg,
      share::schema::ObTenantSchema &new_tenant_schema);
  int check_alter_tenant_locality_type(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTenantSchema &orig_tenant_schema,
      const share::schema::ObTenantSchema &new_tenant_schema,
      AlterLocalityType &alter_locality_type);
  int try_rollback_modify_tenant_locality(
      const obrpc::ObModifyTenantArg &arg,
      share::schema::ObTenantSchema &new_tenant_schema,
      const share::schema::ObTenantSchema &orig_tenant_schema,
      const common::ObIArray<common::ObZone> &zones_in_pool,
      const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      AlterLocalityOp &alter_locality_op);
  int try_modify_tenant_locality(
      const obrpc::ObModifyTenantArg &arg,
      share::schema::ObTenantSchema &new_tenant_schema,
      const share::schema::ObTenantSchema &orig_tenant_schema,
      const common::ObIArray<common::ObZone> &zones_in_pool,
      const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      AlterLocalityOp &alter_locality_op);

  int parse_and_set_create_tenant_new_locality_options(
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTenantSchema &schema,
      const common::ObIArray<share::ObResourcePoolName> &pools,
      const common::ObIArray<common::ObZone> &zones_list,
      const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list);

  template<typename T>
  int set_schema_zone_list(
      share::schema::ObSchemaGetterGuard &schema_guard,
      T &schema,
      const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list);
  int generate_zone_list_by_locality(
      const share::schema::ZoneLocalityIArray &zone_locality,
      const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      common::ObArray<common::ObZone> &zone_list) const;
  int check_and_modify_tenant_locality(
      const obrpc::ObModifyTenantArg &arg,
      share::schema::ObTenantSchema &new_tenant_schema,
      const share::schema::ObTenantSchema &orig_tenant_schema,
      const common::ObIArray<common::ObZone> &zones_in_pool,
      const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list);
  // When alter tenant modifies the locality, call the following function to get the zone_list of
  // the resource pool corresponding to the new tenant schema
  int get_new_tenant_pool_zone_list(
      const obrpc::ObModifyTenantArg &arg,
      const share::schema::ObTenantSchema &tenant_schema,
      common::ObIArray<share::ObResourcePoolName> &resource_pool_names,
      common::ObIArray<common::ObZone> &zones_in_pool,
      common::ObIArray<share::schema::ObZoneRegion> &zone_region_list);
  int get_tenant_pool_zone_list(
      const share::schema::ObTenantSchema &tenant_schema,
      common::ObIArray<common::ObZone> &zones_in_pool);
  int get_zones_of_pools(const common::ObIArray<share::ObResourcePoolName> &resource_pool_names,
                         common::ObIArray<common::ObZone> &zones_in_pool);
  int set_new_database_options(const obrpc::ObAlterDatabaseArg &arg,
                               share::schema::ObDatabaseSchema &new_database_schema);
  int check_tablegroup_in_single_database(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &table_schema);
  int set_new_table_options(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const share::schema::AlterTableSchema &alter_table_schema,
      const share::schema::ObTenantSchema &tenant_schema,
      share::schema::ObTableSchema &new_table_schema,
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &need_update_index_table,
      AlterLocalityOp &alter_locality_op);
  int set_raw_table_options(
      const share::schema::AlterTableSchema &alter_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &need_update_index_table);
  virtual int create_tables_in_trans(
              const bool if_not_exist,
              const common::ObString &ddl_stmt_str,
              const share::schema::ObErrorInfo &error_info,
              common::ObIArray<share::schema::ObTableSchema> &table_schemas,
              const obrpc::ObSequenceDDLArg &sequence_ddl_arg,
              const uint64_t last_replay_log_id,
              const common::ObIArray<share::schema::ObDependencyInfo> *dep_infos,
              ObIArray<ObMockFKParentTableSchema> &mock_fk_parent_table_schema_array,
              int64_t &ddl_task_id);
  int print_view_expanded_definition(
      const share::schema::ObTableSchema &table_schema,
      ObString &ddl_stmt_str,
      common::ObIAllocator &allocator,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool if_not_exist);
  int update_tables_attribute(common::ObIArray<ObTableSchema*> &new_table_schemas,
                              ObDDLOperator &ddl_operator,
                              common::ObMySQLTransaction &trans,
                              const share::schema::ObSchemaOperationType operation_type,
                              const common::ObString &ddl_stmt_str);
  int alter_table_in_trans(obrpc::ObAlterTableArg &alter_table_arg,

                           obrpc::ObAlterTableRes &res,
                           const uint64_t tenant_data_version);
  int need_modify_not_null_constraint_validate(const obrpc::ObAlterTableArg &alter_table_arg,
                                               bool &is_add_not_null_col,
                                               bool &need_modify) const;
  bool need_check_constraint_validity(const obrpc::ObAlterTableArg &alter_table_arg) const;
  // offline ddl cannot appear at the same time with other ddl types
  // Offline ddl cannot appear at the same time as offline ddl
  int check_is_offline_ddl(obrpc::ObAlterTableArg &alter_table_arg,
                           share::ObDDLType &ddl_type,
                           bool &ddl_need_retry_at_executor);
  int check_is_oracle_mode_add_column_not_null_ddl(const obrpc::ObAlterTableArg &alter_table_arg,
                                                   ObSchemaGetterGuard &schema_guard,
                                                   bool &is_oracle_mode_add_column_not_null_ddl,
                                                   bool &is_default_value_null);
  int check_can_bind_tablets(const share::ObDDLType ddl_type,
                             bool &bind_tablets);
  int check_ddl_with_primary_key_operation(const obrpc::ObAlterTableArg &alter_table_arg,
                                           bool &with_primary_key_operation);
  int do_offline_ddl_in_trans(obrpc::ObAlterTableArg &alter_table_arg,
                              obrpc::ObAlterTableRes &res);
  int add_not_null_column_to_table_schema(
      obrpc::ObAlterTableArg &alter_table_arg,
      const ObTableSchema &origin_table_schema,
      ObTableSchema &new_table_schema,
      ObSchemaGetterGuard &schema_guard,
      ObDDLOperator &ddl_operator,
      ObDDLSQLTransaction &trans);
  int add_not_null_column_default_null_to_table_schema(
      obrpc::ObAlterTableArg &alter_table_arg,
      const ObTableSchema &origin_table_schema,
      ObTableSchema &new_table_schema,
      ObSchemaGetterGuard &schema_guard,
      ObDDLOperator &ddl_operator,
      ObDDLSQLTransaction &trans);
  int do_oracle_add_column_not_null_in_trans(obrpc::ObAlterTableArg &alter_table_arg,
                                             ObSchemaGetterGuard &schema_guard,
                                             const bool is_default_value_null);
  int gen_new_index_table_name(
      const common::ObString &orig_index_table_name,
      const uint64_t orig_table_id,
      const uint64_t new_table_id,
      common::ObIAllocator &allocator,
      common::ObString &new_index_table_name);
  int gen_hidden_obj_name(const uint64_t obj_id,
                          const uint64_t table_id,
                          const uint64_t origin_fk_id,
                          common::ObIAllocator &allocator,
                          common::ObString &hidden_obj_name,
                          const share::schema::ObSchemaType schema_type);
  int is_foreign_key_name_prefix_match(const ObForeignKeyInfo &origin_fk_info,
                                       const ObForeignKeyInfo &hidden_fk_info,
                                       common::ObIAllocator &allocator,
                                       bool &is_match);
  // in the first stage, create a hidden table without creating constraints, foreign keys
  // and indexes. if it needs to be created, it will be created in the second stage
  int create_user_hidden_table(
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObTableSchema &hidden_table_schema,
      const obrpc::ObSequenceDDLArg *sequence_ddl_arg,
      const bool bind_tablets,
      share::schema::ObSchemaGetterGuard &src_tenant_schema_guard,
      share::schema::ObSchemaGetterGuard &dst_tenant_schema_guard,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans,
      common::ObIAllocator &allocator,
      const uint64_t tenant_data_version,
      const ObString &index_name = ObString(""));
  int rebuild_triggers_on_hidden_table(
      const share::schema::ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &hidden_table_schema,
      ObSchemaGetterGuard &src_tenant_schema_guard,
      ObSchemaGetterGuard &dst_tenant_schema_guard,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans);
  int drop_child_table_fk(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const share::schema::ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &hidden_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLTransaction &trans);
  int swap_child_table_fk_name(
      const uint64_t child_table_id,
      const share::schema::ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &hidden_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans,
      common::ObIAllocator &allocator);
  // if the original table is a parent table, after swap the status of the two tables
  // need to rename the original fk name to the newly generated hidden fk name , and then rename
  // the newly generated hidden fk name to the original fk name on the child table
  int swap_all_child_table_fk_name(
      const share::schema::ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &hidden_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObMySQLTransaction &trans,
      common::ObIAllocator &allocator);
  int prepare_hidden_table_schema(
      const share::schema::ObTableSchema &orig_table_schema,
      common::ObIAllocator &allocator,
      share::schema::ObTableSchema &hidden_table_schema,
      const ObString &index_name);
  int rebuild_hidden_table_priv(
      const share::schema::ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &hidden_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans);
  int rebuild_hidden_table_rls_objects(
      const share::schema::ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &hidden_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans);
  // NOTE: if_offline_ddl is true only in the execution phase of offline ddl, skip check_can_do_ddl check
  int get_and_check_table_schema(
      const obrpc::ObAlterTableArg &alter_table_arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::AlterTableSchema &alter_table_schema,
      const share::schema::ObTableSchema *&table_schema,
      bool if_offline_ddl = false);
  int check_support_alter_pk_and_columns(const obrpc::ObAlterTableArg &alter_table_arg,
                                         const obrpc::ObIndexArg::IndexActionType &index_action_type,
                                         bool &is_support);
  int check_alter_table_index(const obrpc::ObAlterTableArg &alter_table_arg,
                              share::ObDDLType &ddl_type);
  int check_is_change_column_order(const share::schema::ObTableSchema &table_schema,
                                   const share::schema::AlterColumnSchema &alter_column_schema,
                                   bool &is_change_column_order) const;
  int check_alter_column_is_offline(const share::schema::ObTableSchema &orig_table_schema,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    const share::schema::ObColumnSchemaV2 &orig_column_schema,
                                    share::schema::AlterColumnSchema &alter_column_schema,
                                    bool &is_offline) const;
  int check_is_add_column_online(const share::schema::ObTableSchema &table_schema,
                                 const share::schema::AlterColumnSchema &alter_column_schema,
                                 share::ObDDLType &tmp_ddl_type);
  int check_is_modify_partition_key(const ObTableSchema &orig_table_schema,
                                    const AlterTableSchema &alter_table_schema,
                                    bool &is_modify_partition_key);
  int check_is_change_cst_column_name(const share::schema::ObTableSchema &table_schema,
                                      const AlterTableSchema &alter_table_schema,
                                      bool &change_cst_column_name);
  int check_is_alter_decimal_int_offline(const share::ObDDLType &ddl_type,
                                         const share::schema::ObTableSchema &table_schema,
                                         const AlterTableSchema &alter_table_schema,
                                         bool &is_alter_decimal_int_off);
  int check_exist_stored_gen_col(const ObTableSchema &orig_table_schema,
                                 const AlterTableSchema &alter_table_schema,
                                 bool &is_exist);
  int check_alter_table_column(obrpc::ObAlterTableArg &alter_table_arg,
                               const share::schema::ObTableSchema &orig_table_schema,
                               share::schema::ObSchemaGetterGuard &schema_guard,
                               const bool is_oracle_mode,
                               share::ObDDLType &ddl_type,
                               bool &ddl_need_retry_at_executor);
  int check_alter_table_partition(const obrpc::ObAlterTableArg &alter_table_arg,
                                  const share::schema::ObTableSchema &orig_table_schema,
                                  const bool is_oracle_mode,
                                  share::ObDDLType &ddl_type);
  int check_convert_to_character(obrpc::ObAlterTableArg &alter_table_arg,
                                 const share::schema::ObTableSchema &orig_table_schema,
                                 share::ObDDLType &ddl_type);
  int check_is_add_identity_column(const share::schema::ObTableSchema &orig_table_schema,
                                   const share::schema::ObTableSchema &hidden_table_schema,
                                   bool &is_add_identity_column);
  int alter_table_primary_key(obrpc::ObAlterTableArg &alter_table_arg,
                              const share::schema::ObTableSchema &orgin_table_schema,
                              share::schema::ObTableSchema &new_table_schema,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              ObDDLOperator &ddl_operator,
                              common::ObMySQLTransaction &trans,
                              common::ObArenaAllocator &allocator,
                              const uint64_t tenant_data_version);
  int check_alter_partition_with_tablegroup(const ObTableSchema *orig_table_schema,
                                            ObTableSchema &new_table_schema,
                                            ObSchemaGetterGuard &schema_guard);
  int alter_table_partition_by(obrpc::ObAlterTableArg &alter_table_arg,
                              const share::schema::ObTableSchema &orgin_table_schema,
                              share::schema::ObTableSchema &new_table_schema,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              ObDDLOperator &ddl_operator,
                              common::ObMySQLTransaction &trans,
                              const uint64_t tenant_data_version);
  int convert_to_character_for_partition(const ObCollationType &to_collation,
                           share::schema::ObTableSchema &new_table_schema);
  int convert_to_character(obrpc::ObAlterTableArg &alter_table_arg,
                           const share::schema::ObTableSchema &orgin_table_schema,
                           share::schema::ObTableSchema &new_table_schema,
                           share::schema::ObSchemaGetterGuard &schema_guard,
                           ObDDLOperator &ddl_operator,
                           common::ObMySQLTransaction &trans,
                           const uint64_t tenant_data_version);
  int check_alter_column_group(const obrpc::ObAlterTableArg &alter_table_arg, share::ObDDLType &ddl_type) const;
  int alter_column_group(obrpc::ObAlterTableArg &alter_table_arg,
                         const share::schema::ObTableSchema &origin_table_schema,
                         share::schema::ObTableSchema &new_table_schema,
                         share::schema::ObSchemaGetterGuard &schema_guard,
                         ObDDLOperator &ddl_operator,
                         common::ObMySQLTransaction &trans);


  int check_alter_table_constraint(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const ObTableSchema &orig_table_schema,
      share::ObDDLType &ddl_type);
  int check_can_alter_table_constraints(
    const obrpc::ObAlterTableArg::AlterConstraintType op_type,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &orig_table_schema,
    const AlterTableSchema &inc_table_schema);
  int get_orig_and_hidden_table_schema(
      const obrpc::ObAlterTableArg &alter_table_arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObSchemaGetterGuard &dest_schema_guard,
      const share::schema::AlterTableSchema &alter_table_schema,
      const share::schema::ObTableSchema *&orig_table_schema,
      const share::schema::ObTableSchema *&hidden_table_schema);
  int build_hidden_index_table_map(
      const share::schema::ObTableSchema &hidden_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::hash::ObHashMap<common::ObString, uint64_t> &new_index_table_map);
  int get_rebuild_foreign_key_infos(
      const obrpc::ObAlterTableArg &alter_table_arg,
      share::schema::ObSchemaGetterGuard &src_tenant_schema_guard,
      share::schema::ObSchemaGetterGuard &dst_tenant_schema_guard,
      const ObTableSchema &orig_table_schema,
      const ObTableSchema &hidden_table_schema,
      const bool rebuild_child_table_fk,
      ObIArray<ObForeignKeyInfo> &original_fk_infos,
      ObIArray<ObForeignKeyInfo> &rebuild_fk_infos);
  int rebuild_hidden_table_foreign_key(
      obrpc::ObAlterTableArg &alter_table_arg,
      const share::schema::ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &hidden_table_schema,
      const bool rebuild_child_table_fk,
      share::schema::ObSchemaGetterGuard &src_tenant_schema_guard,
      share::schema::ObSchemaGetterGuard &dst_tenant_schema_guard,
      common::ObMySQLTransaction &trans,
      common::ObSArray<uint64_t> &cst_ids);
  int get_hidden_table_column_id_by_orig_column_id(
    const ObTableSchema &orig_table_schema,
    const ObTableSchema &hidden_table_schema,
    const share::ObColumnNameMap &col_name_map,
    const uint64_t orig_column_id,
    uint64_t &hidden_column_id) const;
  int convert_hidden_table_column_ids_by_orig_column_ids(
    const ObTableSchema &orig_table_schema,
    const ObTableSchema &hidden_table_schema,
    const share::ObColumnNameMap &col_name_map,
    ObIArray<uint64_t> &column_ids) const;
  int copy_constraint_for_hidden_table(
      const ObTableSchema &orig_table_schema,
      const ObTableSchema &hidden_table_schema,
      const share::ObColumnNameMap &col_name_map,
      const ObConstraint &orig_constraint,
      ObConstraint &hidden_constraint);
  int check_and_get_rebuild_constraints(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const ObTableSchema &orig_table_schema,
      const ObTableSchema &new_table_schema,
      ObIArray<ObConstraint> &rebuild_constraints);
  int rebuild_hidden_table_constraints(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const share::schema::ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &hidden_table_schema,
      common::ObMySQLTransaction &trans,
      common::ObSArray<uint64_t> &cst_ids);
  int rebuild_hidden_table_index(
      const uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans,
      common::ObSArray<share::schema::ObTableSchema> &new_table_schemas);
  int add_new_index_schema(
      obrpc::ObAlterTableArg &alter_table_arg,
      const share::schema::ObTableSchema &orig_table_schema,
      const ObTableSchema &hidden_table_schema,
      ObSchemaGetterGuard &schema_guard,
      ObSchemaGetterGuard &dest_schema_guard,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans,
      ObSArray<ObTableSchema> &new_table_schemas,
      ObSArray<uint64_t> &index_ids);
  int check_index_table_need_rebuild(
      const share::schema::ObTableSchema &index_table_schema,
      const common::ObIArray<int64_t> &drop_cols_id_arr,
      const bool is_oracle_mode,
      bool &need_rebuild);
  int reconstruct_index_schema(
      obrpc::ObAlterTableArg &alter_table_arg,
      const share::schema::ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &hidden_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObSchemaGetterGuard &dest_schema_guard,
      const common::ObIArray<int64_t> &drop_cols_id_arr,
      const share::ObColumnNameMap &col_name_map,
      const common::ObTimeZoneInfo &tz_info,
      common::ObIAllocator &allocator,
      common::ObSArray<share::schema::ObTableSchema> &new_table_schemas,
      common::ObSArray<uint64_t> &index_ids);
  int gen_hidden_index_schema_columns(
      const share::schema::ObTableSchema &orig_index_schema,
      const common::ObIArray<int64_t> &drop_cols_id_arr,
      const share::ObColumnNameMap &col_name_map,
      share::schema::ObTableSchema &new_table_schema,
      share::schema::ObTableSchema &index_schema);
  int alter_table_sess_active_time_in_trans(obrpc::ObAlterTableArg &alter_table_arg,
                                            obrpc::ObAlterTableRes &res,
                                            const uint64_t tenant_data_version);
  int truncate_table_in_trans(const obrpc::ObTruncateTableArg &arg,
                              const share::schema::ObTableSchema &orig_table_schema,
                              common::ObIArray<share::schema::ObTableSchema> &table_schemas,
                              const common::ObIArray<share::schema::ObRecycleObject> &index_recycle_objs,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              ObMySQLTransaction &trans,
                              const common::ObString *ddl_stmt_str,
                              const common::ObString &db_name);
  int new_truncate_table_in_trans(const ObIArray<const share::schema::ObTableSchema*> &orig_table_schemas,
                                  ObDDLSQLTransaction &trans,
                                  const ObString *ddl_stmt_str,
                                  obrpc::ObDDLRes &ddl_res);
  int restore_obj_priv_after_truncation(
      ObDDLOperator &ddl_operator,
      ObMySQLTransaction &trans,
      common::ObIArray<share::schema::ObObjPriv> &orig_obj_privs_ora,
      uint64_t new_table_id,
      const common::ObString &database_name,
      const common::ObString &table_name);
  int drop_aux_table_in_truncate(
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObMySQLTransaction &trans,
      ObDDLOperator &ddl_operator,
      const share::schema::ObTableType table_type,
      const bool to_recyclebin);
  int truncate_oracle_temp_table(const ObString &db_name,
                                 const ObString &tab_name,
                                 const uint64_t tenant_id,
                                 const uint64_t session_id,
                                 const int64_t sess_create_time);
  int generate_tmp_idx_schemas(
      const share::schema::ObTableSchema &new_table_schema,
      ObIArray<share::schema::ObTableSchema> &idx_schemas,
      share::schema::ObSchemaGetterGuard &schema_guard);
  int fill_column_collation(
      const ObSQLMode sql_mode,
      const bool is_oracle_mode,
      const share::schema::ObTableSchema &table_schema,
      common::ObIAllocator &allocator,
      share::schema::ObColumnSchemaV2 &alter_column_schema);
  int resolve_orig_default_value(share::schema::ObColumnSchemaV2 &column_schema,
                                 const common::ObTimeZoneInfoWrap &tz_info_wrap,
                                 const common::ObString *nls_formats,
                                 common::ObIAllocator &allocator);
  int resolve_timestamp_column(share::schema::AlterColumnSchema *alter_column_schema,
                               share::schema::ObTableSchema &new_table_schema,
                               share::schema::ObColumnSchemaV2 &new_column_schema,
                               const common::ObTimeZoneInfoWrap &tz_info_wrap,
                               const common::ObString *nls_formats,
                               common::ObIAllocator &allocator);
  int deal_default_value_padding(share::schema::ObColumnSchemaV2 &column_schema,
                                 common::ObIAllocator &allocator);
  int pre_check_orig_column_schema(
      const share::schema::AlterColumnSchema &alter_column_schema,
      const share::schema::ObTableSchema &origin_table_schema,
      common::hash::ObHashSet<ObColumnNameHashWrapper> &update_column_name_set);
  int check_generated_column_modify_authority(
      const share::schema::ObColumnSchemaV2 &old_column_schema,
      const share::schema::AlterColumnSchema &alter_column_schema,
      bool is_oracle_mode);
  int update_generated_column_schema(
      const share::schema::AlterColumnSchema &alter_column_schema,
      const share::schema::ObColumnSchemaV2 &orig_column_schema,
      const share::schema::ObTableSchema &origin_table_schema,
      const common::ObTimeZoneInfoWrap &tz_info_wrap,
      const share::schema::ObLocalSessionVar *local_session_var,
      share::schema::ObTableSchema &new_table_schema,
      const bool need_update_default_value,
      const bool need_update_session_var,
      ObDDLOperator *ddl_operator = NULL,
      common::ObMySQLTransaction *trans = NULL);
  int modify_generated_column_default_value(share::schema::ObColumnSchemaV2 &generated_column,
                                            common::ObString &column_name,
                                            const common::ObString &new_column_name,
                                            const share::schema::ObTableSchema &table_schema,
                                            const common::ObTimeZoneInfo &tz_info);
  int modify_generated_column_local_vars(ObColumnSchemaV2 &generated_column,
                                        const common::ObString &column_name,
                                        const ObObjType origin_type,
                                        const AlterColumnSchema &new_column_schema,
                                        const ObTableSchema &table_schema,
                                        const share::schema::ObLocalSessionVar *local_session_var);
  int modify_depend_column_type(sql::ObRawExpr *expr,
                                const ObString &column_name,
                                const AlterColumnSchema &alter_column_schema,
                                lib::Worker::CompatMode compat_mode);

  int modify_part_func_expr(const ObString &orig_column_name,
                            const ObString &alter_column_name,
                            share::schema::ObTableSchema &table_schema,
                            const common::ObTimeZoneInfo &tz_info,
                            common::ObIAllocator &allocator);
  int modify_func_expr_column_name(const ObString &orig_column_name,
                                   const ObString &alter_column_name,
                                   share::schema::ObTableSchema &table_schema,
                                   const common::ObTimeZoneInfo &tz_info,
                                   common::ObIAllocator &allocator,
                                   bool is_sub_part);
  int modify_part_func_expr_for_global_index(
      const share::schema::ObColumnSchemaV2 &orig_column,
      const share::schema::ObColumnSchemaV2 &alter_column,
      share::schema::ObTableSchema &table_schema,
      const common::ObTimeZoneInfo &tz_info,
      common::ObIAllocator &allocator,
      ObDDLOperator *ddl_operator,
      common::ObMySQLTransaction *trans,
      common::ObIArray<share::schema::ObTableSchema> *global_idx_schema_array = NULL);
  // Traverse all the check constraint expr str,
  // replacing the old column name with new name
  int modify_constraint_check_expr(
      const share::schema::ObColumnSchemaV2 &orig_column,
      const share::schema::ObColumnSchemaV2 &alter_column,
      share::schema::ObTableSchema &table_schema,
      obrpc::ObAlterTableArg &alter_table_arg,
      const bool is_oracle_mode,
      const common::ObTimeZoneInfo &tz_info,
      common::ObIAllocator &allocator,
      ObDDLOperator *ddl_operator,
      common::ObMySQLTransaction *trans);
  // column_id_array is invalid if newly-added constraint is referenced to
  // newly-added column, due to invalid column id in the resolver stage.
  int refill_columns_id_for_not_null_constraint(
    const share::schema::ObTableSchema &alter_table_schema,
    const share::schema::ObColumnSchemaV2 &alter_column_schema);
  int refill_columns_id_for_check_constraint(
    const share::schema::ObTableSchema &orig_table_schema,
    const share::schema::ObTableSchema &alter_table_schema,
    const share::schema::ObColumnSchemaV2 &alter_column_schema,
    const bool is_oracle_mode,
    common::ObIAllocator &allocator);
  /**
   * Rebuild check constraint expr, if the expr str contains the old column name
   *
   * @new_check_expr_str [out] new check expr str, if contains old column name
   * @need_modify_check_expr [out] is true, if contains old column name
   */
  int rebuild_constraint_check_expr(
      const share::schema::ObColumnSchemaV2 &orig_column,
      const share::schema::ObColumnSchemaV2 &alter_column,
      const share::schema::ObConstraint &cst,
      share::schema::ObTableSchema &table_schema,
      const common::ObTimeZoneInfo &tz_info,
      common::ObIAllocator &allocator,
      ObString &new_check_expr_str,
      bool &need_modify_check_expr);
  int check_can_alter_column_type(
      const share::schema::ObColumnSchemaV2 &src_column,
      const share::schema::ObColumnSchemaV2 &dst_column,
      const share::schema::ObTableSchema &table_schema,
      const bool is_oracle_mode);
  int check_is_change_column_type(
      const share::schema::ObColumnSchemaV2 &src_column,
      const share::schema::ObColumnSchemaV2 &dst_column,
      bool &is_change_column_type);
  int check_column_in_index(
      const uint64_t column_id,
      const share::schema::ObTableSchema &table_schema,
      bool &is_in_index);
  int fill_new_column_attributes(
      const share::schema::AlterColumnSchema &alter_column_schema,
      share::schema::ObColumnSchemaV2 &new_column_schema);
  int check_modify_column_when_upgrade(
      const share::schema::ObColumnSchemaV2 &new_column,
      const share::schema::ObColumnSchemaV2 &orig_column);
  int alter_shadow_column_for_index(
    const ObArray<ObTableSchema> &idx_schema_array,
    const AlterColumnSchema *alter_column_schema,
    const ObColumnSchemaV2 &new_column_schema,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans);

  int check_new_column_for_index(
      ObIArray<share::schema::ObTableSchema> &idx_schemas,
      const share::schema::ObColumnSchemaV2 &new_column_schema);
  //virtual int init_tenant_profile(common::ObMySQLTransaction &trans);
  //
  // materialized view related
  //
  int alter_table_update_index_and_view_column(
      const share::schema::ObTableSchema &new_table_schema,
      const share::schema::ObColumnSchemaV2 &new_column_schema,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans,
      const common::ObIArray<share::schema::ObTableSchema> *global_idx_schema_array = NULL);
  int alter_table_update_aux_column(
      const share::schema::ObTableSchema &new_table_schema,
      const share::schema::ObColumnSchemaV2 &new_column_schema,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans,
      const share::schema::ObTableType table_type,
      const common::ObIArray<share::schema::ObTableSchema> *global_idx_schema_array = NULL);
  int alter_sequence_in_alter_column(const share::schema::ObTableSchema &table_schema,
                                     share::schema::ObColumnSchemaV2 &column_schema,
                                     common::ObMySQLTransaction &trans,
                                     share::schema::ObSchemaGetterGuard &schema_guard,
                                     obrpc::ObSequenceDDLArg &sequence_ddl_arg);
  int drop_sequence_in_drop_column(const share::schema::ObColumnSchemaV2 &column_schema,
                                   common::ObMySQLTransaction &trans,
                                   share::schema::ObSchemaGetterGuard &schema_guard);
  int update_prev_id_for_add_column(const share::schema::ObTableSchema &origin_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      share::schema::AlterColumnSchema &alter_column_schema,
      ObDDLOperator *ddl_operator,
      common::ObMySQLTransaction *trans);

  int alter_table_update_cg_column(common::ObMySQLTransaction &trans,
                                   ObDDLOperator &ddl_operator,
                                   share::schema::ObColumnSchemaV2 &new_column_schema,
                                   share::schema::ObTableSchema &new_table_schema);

  bool is_zone_exist(const common::ObArray<common::ObZone> &zones, const common::ObZone &zone);
  int try_drop_sys_ls_(const uint64_t meta_tenant_id,
                       common::ObMySQLTransaction &trans);
  virtual int flashback_tenant_in_trans(const share::schema::ObTenantSchema &tenant_schema,
                                        const ObString &new_tenant_name,
                                        share::schema::ObSchemaGetterGuard &schema_guard,
                                        const ObString &ddl_stmt_str);
  int get_tenant_object_name_with_origin_name_in_recyclebin(
      const ObString &origin_tenant_name,
      ObString &object_name,
      common::ObIAllocator *allocator,
      const bool is_flashback);
  int build_need_flush_role_array(share::schema::ObSchemaGetterGuard &schema_guard,
                                  uint64_t tenant_id,
                                  const share::schema::ObUserInfo *user_info,
                                  const obrpc::ObAlterUserProfileArg &arg,
                                  bool &need_flush,
                                  common::ObIArray<uint64_t> &role_id_array,
                                  common::ObIArray<uint64_t> &disable_flag_array);
  int drop_resource_pool_pre(const uint64_t tenant_id,
                             common::ObIArray<uint64_t> &drop_ug_id_array,
                             ObIArray<share::ObResourcePoolName> &pool_names,
                             ObMySQLTransaction &trans);
  int drop_resource_pool_final(const uint64_t tenant_id,
                               common::ObIArray<uint64_t> &drop_ug_id_array,
                               ObIArray<share::ObResourcePoolName> &pool_names);
  // private funcs for drop column
  int get_all_dropped_udt_hidden_column_ids(const ObTableSchema &orig_table_schema,
                                            const ObColumnSchemaV2 &orig_column_schema,
                                            common::ObIArray<int64_t> &drop_cols_id_arr,
                                            int64_t &columns_cnt_in_new_table);
  int get_all_dropped_column_ids(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const ObTableSchema &orig_table_schema,
      common::ObIArray<int64_t> &drop_cols_id_arr,
      int64_t *new_table_cols_cnt = nullptr);
  int drop_udt_hidden_columns(const ObTableSchema &origin_table_schema,
                              ObTableSchema &new_table_schema,
                              const ObColumnSchemaV2 &new_origin_col,
                              int64_t new_schema_version);
  int check_can_drop_columns(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObTableSchema &new_table_schema);
  int check_can_drop_column(
      const common::ObString &orig_column_name,
      const share::schema::ObColumnSchemaV2 *orig_column_schema,
      const ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &new_table_schema,
      const int64_t new_table_cols_cnt,
      ObSchemaGetterGuard &schema_guard);
  int check_drop_column_with_drop_foreign_key(
      const obrpc::ObAlterTableArg &alter_table_arg,
      const share::schema::ObTableSchema &orig_table_schema,
      const common::ObIArray<int64_t> &drop_cols_id_arr);
  int check_drop_column_with_drop_constraint(
      const obrpc::ObAlterTableArg &alter_table_arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &orig_table_schema,
      const common::ObIArray<int64_t> &drop_cols_id_arr);
  int drop_constraint_caused_by_drop_column(
      const obrpc::ObAlterTableArg &alter_table_arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans);
  int delete_column_from_schema_in_trans(
      const share::schema::AlterTableSchema &alter_table_schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans);
  int delete_auto_increment_attribute(
      const share::schema::ObTableSchema *orig_table_schema,
      share::schema::ObTableSchema &new_table_schema,
      share::schema::AlterTableSchema &alter_table_schema);
  int create_aux_lob_table_if_need(
      ObTableSchema &data_table_schema,
      ObSchemaGetterGuard &schema_guard,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans,
      const bool need_sync_schema_version,
      bool &is_add_lob);
  int lock_tables_of_database(const share::schema::ObDatabaseSchema &database_schema,
                              ObMySQLTransaction &trans);
  int lock_tables_in_recyclebin(const share::schema::ObDatabaseSchema &database_schema,
                                ObMySQLTransaction &trans);
  int get_dropping_domain_index_invisiable_aux_table_schema(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      const uint64_t index_table_id,
      const bool is_fts_index,
      const ObString &index_name,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans,
      common::ObIArray<share::schema::ObTableSchema> &new_aux_schemas);
  int build_single_table_rw_defensive_(
    const uint64_t tenant_id,
    const uint64_t tenant_data_version,
    const ObArray<ObTabletID> &tablet_ids,
    const int64_t schema_version,
    ObDDLSQLTransaction &trans);
  int build_rw_defense_for_table_(
      const uint64_t tenant_data_version,
      const ObTableSchema &table_schema,
      const int64_t new_data_table_schema_version,
      const ObIArray<std::pair<uint64_t, int64_t>> &aux_schema_versions,
      ObDDLSQLTransaction &trans);

public:
  int generate_aux_index_schema(
      const obrpc::ObGenerateAuxIndexSchemaArg &arg,
      obrpc::ObGenerateAuxIndexSchemaRes &result);
  int check_parallel_ddl_conflict(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const obrpc::ObDDLArg &arg);
  int construct_zone_region_list(
      common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      const common::ObIArray<common::ObZone> &zone_list);

  int handle_security_audit(const obrpc::ObSecurityAuditArg &arg);

  static int check_and_get_object_name(share::schema::ObSchemaGetterGuard &schema_guard,
                                       const share::schema::ObSAuditType audit_type,
                                       const uint64_t teannt_id,
                                       const uint64_t obj_object_id,
                                       common::ObString &schema_name,
                                       common::ObString &object_name);

  int check_create_tenant_locality(
      const common::ObIArray<common::ObString> &pool_list,
      share::schema::ObTenantSchema &tenant_schema,
      share::schema::ObSchemaGetterGuard &schema_guard);

  int check_create_tenant_replica_options(
      share::schema::ObTenantSchema &tenant_schema,
      share::schema::ObSchemaGetterGuard &schema_guard);

  static int gen_tenant_init_config(
             const uint64_t tenant_id,
             const uint64_t compatible_version,
             common::ObConfigPairs &tenant_config);
  static int notify_init_tenant_config(
             obrpc::ObSrvRpcProxy &rpc_proxy,
             const common::ObIArray<common::ObConfigPairs> &init_configs,
             const common::ObIArray<common::ObAddr> &addrs);

#ifdef OB_BUILD_TDE_SECURITY
  int check_need_create_root_key(const obrpc::ObCreateTenantArg &arg, bool &need_create);
  int get_root_key_from_primary(const obrpc::ObCreateTenantArg &arg,
  const uint64_t tenant_id, obrpc::RootKeyType &key_type,
  common::ObString &key_value,
  common::ObIAllocator &allocator);
  int standby_create_root_key(
             const uint64_t tenant_id,
             const obrpc::ObCreateTenantArg &arg,
             const common::ObIArray<common::ObAddr> &addrs);
  static int create_root_key(
             obrpc::ObSrvRpcProxy &rpc_proxy,
             const uint64_t tenant_id,
             const common::ObIArray<common::ObAddr> &addrs);
  static int notify_root_key(
             obrpc::ObSrvRpcProxy &rpc_proxy,
             const obrpc::ObRootKeyArg &arg,
             const common::ObIArray<common::ObAddr> &addrs,
             obrpc::ObRootKeyResult &result,
             const bool enable_default = true,
             const bool skip_call_rs = false,
             const uint64_t &cluster_id = OB_INVALID_CLUSTER_ID,
             common::ObIAllocator *allocator = NULL);
#endif
private:
  int check_schema_generated_for_aux_index_schema_(
      const obrpc::ObGenerateAuxIndexSchemaArg &arg,
      ObSchemaGetterGuard &schema_guard,
      const ObTableSchema *data_schema,
      bool &schema_generated,
      uint64_t &index_table_id);
  int adjust_cg_for_offline(ObTableSchema &new_table_schema);
  int add_column_group(const obrpc::ObAlterTableArg &alter_table_arg,
                       const share::schema::ObTableSchema &ori_table_schema,
                       share::schema::ObTableSchema &new_table_schema);

  int drop_column_group(const obrpc::ObAlterTableArg &alter_table_arg,
                        const share::schema::ObTableSchema &ori_table_schema,
                        share::schema::ObTableSchema &new_table_schema);
  int handle_security_audit_for_stmt(const obrpc::ObSecurityAuditArg &arg,
                                     share::schema::ObSAuditSchema &audit_schema);
  int handle_security_audit_for_object(const obrpc::ObSecurityAuditArg &arg,
                                       share::schema::ObSAuditSchema &audit_schema);
  int check_and_update_audit_schema(share::schema::ObSchemaGetterGuard &schema_guard,
                                    share::schema::ObSchemaService *schema_service_impl,
                                    share::schema::ObSAuditSchema &audit_schema,
                                    const share::schema::ObSAuditModifyType modify_type,
                                    bool &need_update,
                                    bool &need_continue);

  const char* ddl_type_str(const share::ObDDLType ddl_type);
public:
  int refresh_unit_replica_counter(const uint64 tenant_id);
  int check_restore_point_allow(const int64_t tenant_id, const share::schema::ObTableSchema &table_schema);
  // used only by create normal tenant
  int check_create_tenant_schema(
      const common::ObIArray<common::ObString> &pool_list,
      share::schema::ObTenantSchema &tenant_schema,
      share::schema::ObSchemaGetterGuard &schema_guard);
  template<typename SCHEMA>
  int get_schema_primary_regions(
      const SCHEMA &schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObIArray<common::ObRegion> &primary_regions);
public:
  int ddl_rlock();
  int ddl_wlock();
  int ddl_unlock() { return ddl_lock_.unlock(); }
private:
  int generate_tenant_schema(
      const obrpc::ObCreateTenantArg &arg,
      const share::ObTenantRole &tenant_role,
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTenantSchema &user_tenant_schema,
      share::schema::ObSysVariableSchema &user_sys_variable,
      share::schema::ObTenantSchema &meta_tenant_schema,
      share::schema::ObSysVariableSchema &meta_sys_variable,
      common::ObIArray<common::ObConfigPairs> &init_configs);
  int init_schema_status(
      const uint64_t tenant_id,
      const share::ObTenantRole &tenant_role);
  int init_system_variables(
      const obrpc::ObCreateTenantArg &arg,
      share::schema::ObTenantSchema &tenant_schema,
      share::schema::ObSysVariableSchema &sys_variable_schema);
  int inner_create_tenant_(
      const obrpc::ObCreateTenantArg &arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      obrpc::UInt64 &tenant_id);
  int create_tenant_schema(
      const obrpc::ObCreateTenantArg &arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTenantSchema &user_tenant_schema,
      share::schema::ObTenantSchema &meta_tenant_schema,
      const common::ObIArray<common::ObConfigPairs> &init_configs);
  int create_normal_tenant(
      const uint64_t tenant_id,
      const ObIArray<share::ObResourcePoolName> &pool_list,
      const share::schema::ObTenantSchema &tenant_schema,
      const share::ObTenantRole &tenant_role,
      const share::SCN &recovery_until_scn,
      share::schema::ObSysVariableSchema &sys_variable,
      const bool create_ls_with_palf,
      const palf::PalfBaseInfo &palf_base_info,
      const common::ObIArray<common::ObConfigPairs> &init_configs,
      bool is_creating_standby,
      const common::ObString &log_restore_source,
      const uint64_t source_tenant_id);
  int set_sys_ls_status(const uint64_t tenant_id);
  int create_tenant_sys_ls(
      const share::schema::ObTenantSchema &tenant_schema,
      const common::ObIArray<share::ObResourcePoolName> &pool_list,
      const bool create_ls_with_palf,
      const palf::PalfBaseInfo &palf_base_info,
      const uint64_t source_tenant_id);
  int create_tenant_user_ls(const uint64_t tenant_id);
  int broadcast_sys_table_schemas(
      const uint64_t tenant_id,
      common::ObIArray<share::schema::ObTableSchema> &tables);
  int create_tenant_sys_tablets(
      const uint64_t tenant_id,
      common::ObIArray<share::schema::ObTableSchema> &tables);
  int init_tenant_schema(
      const uint64_t tenant_id,
      const share::schema::ObTenantSchema &tenant_schema,
      const share::ObTenantRole &tenant_role,
      const share::SCN &recovery_until_scn,
      common::ObIArray<share::schema::ObTableSchema> &tables,
      share::schema::ObSysVariableSchema &sys_variable,
      const common::ObIArray<common::ObConfigPairs> &init_configs,
      bool is_creating_standby,
      const common::ObString &log_restore_source);
  int set_log_restore_source(
      const uint64_t tenant_id,
      const common::ObString &log_restore_source,
      common::ObMySQLTransaction &trans);
  int insert_restore_or_clone_tenant_job_(
      const uint64_t tenant_id,
      const ObString &tenant_name,
      const share::ObTenantRole &tenant_role,
      const uint64_t source_tenant_id);
  int create_sys_table_schemas(
      ObDDLOperator &ddl_operator,
      ObMySQLTransaction &trans,
      common::ObIArray<share::schema::ObTableSchema> &tables);
  int try_force_drop_tenant(const share::schema::ObTenantSchema &tenant_schema);

  int handle_security_audit_in_trans(const share::schema::ObSAuditSchema &audit_schema,
                                     const share::schema::ObSAuditModifyType modify_type,
                                     const bool need_update,
                                     const common::ObString &ddl_str,
                                     common::ObSqlString &public_sql_string,
                                     share::schema::ObSchemaGetterGuard &schema_guard);
  int build_obj_key(
      share::schema::ObObjPrivSortKey &obj_priv_key,           /* out: obj sort key*/
      const uint64_t tenant_id,
      const uint64_t user_id,
      const common::ObString &db,
      const common::ObString &table,
      const share::ObRawObjPrivArray &obj_priv_array,
      share::schema::ObSchemaGetterGuard &schema_guard);

  //----Functions for managing privileges----
  int check_user_exist(const share::schema::ObUserInfo &user_info) const;
  bool is_user_exist(const uint64_t tenant_id, const uint64_t user_id) const;
  int create_user(share::schema::ObUserInfo &user_info,
                  uint64_t creator_id,
                  uint64_t &user_id);
  int create_user_in_trans(share::schema::ObUserInfo &user_info,
                           uint64_t creator_id,
                           uint64_t &user_id,
                           share::schema::ObSchemaGetterGuard &schema_guard);

  int create_mysql_roles_in_trans(const uint64_t tenant_id,
                                  const bool if_not_exist,
                                  common::ObIArray<share::schema::ObUserInfo> &user_infos);
  int replay_alter_user(const share::schema::ObUserInfo &user_info,
      share::schema::ObSchemaGetterGuard &schema_guard);
  int set_passwd_in_trans(const uint64_t tenant_id,
                          const uint64_t user_id,
                          const common::ObString &new_passwd,
                          const common::ObString *ddl_stmt_str,
                          share::schema::ObSchemaGetterGuard &schema_guard);
  int set_max_connection_in_trans(const uint64_t tenant_id,
                                  const uint64_t user_id,
                                  const uint64_t max_connections_per_hour,
                                  const uint64_t max_user_connections,
                                  const ObString *ddl_stmt_str,
                                  share::schema::ObSchemaGetterGuard &schema_guard);
  int alter_user_require_in_trans(const uint64_t tenant_id,
                                  const uint64_t user_id,
                                  const obrpc::ObSetPasswdArg &arg,
                                  const common::ObString *ddl_stmt_str,
                                  share::schema::ObSchemaGetterGuard &schema_guard);
  int rename_user_in_trans(const uint64_t tenant_id,
                           const uint64_t user_id,
                           const obrpc::ObAccountArg &new_account,
                           const common::ObString *ddl_stmt_str,
                           share::schema::ObSchemaGetterGuard &schema_guard);
  int lock_user_in_trans(const uint64_t tenant_id,
                         const uint64_t user_id,
                         const bool locked,
                         const common::ObString *ddl_stmt_str,
                         share::schema::ObSchemaGetterGuard &schema_guard);
  int drop_user_in_trans(const uint64_t tenant_id,
                         const common::ObIArray<uint64_t> &user_ids,
                         const ObString *ddl_stmt_str);

  //----End of Functions for managing privileges----
  template<typename SCHEMA>
  int set_schema_replica_num_options(
      SCHEMA &schema,
      ObLocalityDistribution &locality_dist,
      common::ObIArray<share::ObUnitInfo> &unit_infos);
  template<typename SCHEMA>
  int check_create_schema_replica_options(
      SCHEMA &schema,
      common::ObArray<common::ObZone> &zone_list,
      share::schema::ObSchemaGetterGuard &schema_guard);
 template<typename SCHEMA>
  int check_and_set_primary_zone(
      SCHEMA &schema,
      const common::ObIArray<common::ObZone> &zone_list,
      share::schema::ObSchemaGetterGuard &schema_guard);
  template<typename SCHEMA>
  int check_empty_primary_zone_locality_condition(
      SCHEMA &schema,
      const common::ObIArray<common::ObZone> &zone_list,
      share::schema::ObSchemaGetterGuard &schema_guard);
  int check_schema_zone_list(
      common::ObArray<common::ObZone> &zone_list);
  int check_alter_schema_replica_options(
      const bool alter_primary_zone,
      share::schema::ObTenantSchema &new_schema,
      const share::schema::ObTenantSchema &orig_schema,
      common::ObArray<common::ObZone> &zone_list,
      share::schema::ObSchemaGetterGuard &schema_guard);
  template<typename SCHEMA>
  int trim_and_set_primary_zone(
      SCHEMA &new_schema,
      const SCHEMA &orig_schema,
      const common::ObIArray<common::ObZone> &zone_list,
      share::schema::ObSchemaGetterGuard &schema_guard);
  int format_primary_zone_from_zone_score_array(
      common::ObIArray<share::schema::ObZoneScore> &zone_score_array,
      char *buf,
      int64_t buf_len);
  int try_modify_databases_attributes_in_tenant(
      const obrpc::ObModifyTenantArg &arg,
      ObDDLOperator &ddl_operator,
      ObMySQLTransaction &trans,
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTenantSchema &tenant_schema,
      int64_t &sub_pz_count);
  int try_modify_tablegroups_attributes_in_tenant(
      const obrpc::ObModifyTenantArg &arg,
      ObDDLOperator &ddl_operator,
      ObMySQLTransaction &trans,
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTenantSchema &tenant_schema,
      int64_t &sub_pz_count);
  int try_modify_tables_attributes_in_tenant(
      const obrpc::ObModifyTenantArg &arg,
      ObDDLOperator &ddl_operator,
      ObMySQLTransaction &trans,
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTenantSchema &tenant_schema,
      int64_t &sub_pz_count);
  int check_alter_tenant_replica_options(
      const obrpc::ObModifyTenantArg &arg,
      share::schema::ObTenantSchema &new_tenant_schema,
      const share::schema::ObTenantSchema &orig_tenant_schema,
      share::schema::ObSchemaGetterGuard &schema_guard);
  int get_pools(const common::ObIArray<common::ObString> &pool_strs,
                common::ObIArray<share::ObResourcePoolName> &pools);

  typedef common::hash::ObPlacementHashSet<share::schema::ObIndexNameHashWrapper, common::OB_MAX_COLUMN_NUMBER> AddIndexNameHashSet;
  typedef common::hash::ObPlacementHashSet<share::schema::ObIndexNameHashWrapper, common::OB_MAX_COLUMN_NUMBER> DropIndexNameHashSet;
  typedef common::hash::ObPlacementHashSet<share::schema::ObIndexNameHashWrapper, common::OB_MAX_COLUMN_NUMBER> RenameIndexNameHashSet;
  typedef common::hash::ObPlacementHashSet<share::schema::ObIndexNameHashWrapper, common::OB_MAX_COLUMN_NUMBER> AlterIndexNameHashSet;
  int generate_index_name(obrpc::ObCreateIndexArg &create_index_arg,
                          const share::schema::ObTableSchema &origin_table_schema,
                          const AddIndexNameHashSet &add_index_name_set,
                          const DropIndexNameHashSet &drop_index_name_set,
                          share::schema::ObSchemaGetterGuard &schema_guard,
                          common::ObArenaAllocator &allocator);
  int check_index_table_exist(const uint64_t tenant_id,
                              const uint64_t database_id,
                              const uint64_t table_id,
                              const common::ObString &index_name,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              bool &is_exist);
  int drop_table_in_trans(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &table_schema,
      const bool is_rebuild_index,
      const bool is_index,
      const bool to_recyclebin,
      const common::ObString *ddl_stmt_str,
      ObMySQLTransaction *sql_trans,
      share::schema::DropTableIdHashSet *drop_table_set,
      ObMockFKParentTableSchema *mock_fk_parent_table_ptr,
      const bool delete_priv = true);
  int drop_aux_table_in_drop_table(
      ObMySQLTransaction &trans,
      ObDDLOperator &ddl_operator,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &table_schema,
      const share::schema::ObTableType table_type,
      const bool to_recyclebin);
  int drop_trigger_in_drop_table(
      ObMySQLTransaction &trans,
      ObDDLOperator &ddl_operator,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &table_schema,
      const bool to_recyclebin);
  int rebuild_table_schema_with_new_id(const share::schema::ObTableSchema &orig_table_schema,
                                       const share::schema::ObDatabaseSchema &new_database_schema,
                                       const common::ObString &new_table_name,
                                       const common::ObString &create_host,
                                       const int64_t session_id,
                                       const share::schema::ObTableType table_type_,
                                       share::schema::ObSchemaService &schema_service,
                                       common::ObIArray<share::schema::ObTableSchema> &new_scheams,
                                       common::ObArenaAllocator &allocator,
                                       const uint64_t define_user_id);
  int check_enable_sys_table_ddl(const share::schema::ObTableSchema &table_schema,
                                 const share::schema::ObSchemaOperationType operation_type);
  int construct_drop_sql(const obrpc::ObTableItem &table_item,
                         const share::schema::ObTableType table_type,
                         common::ObSqlString &sql,
                         bool is_oracle_mode,
                         bool is_cascade_constrains);
  int log_drop_warn_or_err_msg(const obrpc::ObTableItem table_item,
                               bool if_exists,
                               common::ObSqlString &err_table_list);
  int log_rebuild_warn_or_err_msg(const obrpc::ObRebuildIndexArg &arg,
                                  common::ObSqlString &err_table_list);
  int alter_outline_in_trans(const obrpc::ObAlterOutlineArg &alter_outline_arg);

  virtual int publish_schema(const uint64_t tenant_id,
                             const common::ObAddrIArray &addrs);

  int check_tenant_has_been_dropped_(const uint64_t tenant_id, bool &is_dropped);

  int get_zone_region(
      const common::ObZone &zone,
      const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      common::ObRegion &region);
  int construct_region_list(
      common::ObIArray<common::ObRegion> &region_list,
      const common::ObIArray<common::ObZone> &zone_list);
  int modify_and_cal_resource_pool_diff(
      common::ObMySQLTransaction &trans,
      common::ObIArray<uint64_t> &new_ug_id_array,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTenantSchema &new_tenant_schema,
      const common::ObIArray<common::ObString> &new_pool_list,
      bool &grant,
      common::ObIArray<share::ObResourcePoolName> &diff_pools);

  // try_alter_meta_tenant_schema: modify meta tenant options
  // @param [in] ddl_operator, operator to do ddl
  // @param [in] arg, tenant options modified by client
  // @param [in] trans, to make sure user-tenant and meta_tenant in the same trans
  // @param [in] sys_schema_guard, to get meta tenant schema
  // @param [in] related_tenant_schema, related user-tenant schema
  // ATTENTION: only locality and primary_zone can be modified in meta_tenant
  int try_alter_meta_tenant_schema(
      ObDDLOperator &ddl_operator,
      const obrpc::ObModifyTenantArg &arg,
      common::ObMySQLTransaction &trans,
      share::schema::ObSchemaGetterGuard &sys_schema_guard,
      const share::schema::ObTenantSchema &related_tenant_schema);

  int check_grant_pools_permitted(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<share::ObResourcePoolName> &to_be_grant_pools,
      const share::schema::ObTenantSchema &tenant_schema,
      bool &is_permitted);
  int check_revoke_pools_permitted(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
      const share::schema::ObTenantSchema &tenant_schema,
      bool &is_permitted);
  int check_gts_tenant_revoke_pools_permitted(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
      const share::schema::ObTenantSchema &tenant_schema,
      bool &is_permitted);
  int check_normal_tenant_revoke_pools_permitted(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
      const share::schema::ObTenantSchema &tenant_schema,
      bool &is_permitted);
  int cal_resource_pool_list_diff(
      const common::ObIArray<share::ObResourcePoolName> &long_pool_name_list,
      const common::ObIArray<share::ObResourcePoolName> &short_pool_name_list,
      common::ObIArray<share::ObResourcePoolName> &diff_pools);
  template<typename SCHEMA, typename ALTER_SCHEMA>
  int fill_part_name(const SCHEMA &orig_schema,
                     ALTER_SCHEMA &alter_schema);
  template<typename SCHEMA, typename ALTER_SCHEMA>
  int check_partition_name_valid(const SCHEMA &orig_schema,
                                 const ALTER_SCHEMA &alter_schema,
                                 const ObString part_name,
                                 bool &valid);
  int check_index_valid_for_alter_partition(const share::schema::ObTableSchema &orig_table_schema,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            const bool is_drop_truncate_and_alter_index,
                                            const bool is_split);
  int check_alter_partitions(const share::schema::ObTableSchema &orig_table_schema,
                             obrpc::ObAlterTableArg &alter_table_arg);
  int check_alter_rename_partitions_(const share::schema::ObTableSchema &orig_table_schema,
                                                const obrpc::ObAlterTableArg &alter_table_arg);
  int check_alter_rename_subpartitions_(const share::schema::ObTableSchema &orig_table_schema,
                                                const obrpc::ObAlterTableArg &alter_table_arg);
  int check_alter_drop_partitions(const share::schema::ObTableSchema &orig_table_schema,
                                  const obrpc::ObAlterTableArg &alter_table_arg,
                                  const bool is_truncate);
  int check_alter_drop_subpartitions(const share::schema::ObTableSchema &orig_table_schema,
                                     const obrpc::ObAlterTableArg &alter_table_arg);
  int check_alter_add_partitions(const share::schema::ObTableSchema &orig_table_schema,
                                 obrpc::ObAlterTableArg &alter_table_arg);
  int filter_out_duplicate_interval_part(const share::schema::ObTableSchema &orig_table_schema,
                                         share::schema::ObTableSchema &alter_table_schema);
  int check_alter_add_subpartitions(const share::schema::ObTableSchema &orig_table_schema,
                                 const obrpc::ObAlterTableArg &alter_table_arg);
  int check_alter_set_interval(const share::schema::ObTableSchema &orig_table_schema,
                               const obrpc::ObAlterTableArg &alter_table_arg);
  int check_add_list_partition(const share::schema::ObPartitionSchema &orig_part,
                               const share::schema::ObPartitionSchema &new_part,
                               const int64_t split_part_id = OB_INVALID_PARTITION_ID);
  int check_add_list_subpartition(const share::schema::ObPartition &orig_part,
                                  const share::schema::ObPartition &new_part);
  int is_list_values_equal(const common::ObIArray<common::ObNewRow> &fir_values,
                           const common::ObIArray<common::ObNewRow> &sed_values,
                           bool &equal);
  int update_sys_variables(const common::ObIArray<obrpc::ObSysVarIdValue> &sys_var_list,
                           const share::schema::ObSysVariableSchema &old_sys_variable,
                           share::schema::ObSysVariableSchema &new_sys_variable,
                           bool& value_changed);

  virtual int reconstruct_table_schema_from_recyclebin(share::schema::ObTableSchema &index_table_schema,
                                                       const share::schema::ObRecycleObject &recycle_obj,
                                                       share::schema::ObSchemaGetterGuard &guard);
  int get_database_id(share::schema::ObSchemaGetterGuard &schema_guard,
                      uint64_t tenant_id,
                      const common::ObString &database_name,
                      uint64_t &database_id);
  int get_object_info(share::schema::ObSchemaGetterGuard &schema_guard,
                      const uint64_t tenant_id,
                      const common::ObString &object_database,
                      const common::ObString &object_name,
                      share::schema::ObSchemaType &object_type,
                      uint64_t &object_id);
  int update_mysql_tenant_sys_var(
      const share::schema::ObTenantSchema &tenant_schema,
      const share::schema::ObSysVariableSchema &sys_variable,
      share::schema::ObSysParam *sys_params,
      int64_t params_capacity);
  int update_oracle_tenant_sys_var(
      const share::schema::ObTenantSchema &tenant_schema,
      const share::schema::ObSysVariableSchema &sys_variable,
      share::schema::ObSysParam *sys_params,
      int64_t params_capacity);
  int update_special_tenant_sys_var(
      const share::schema::ObSysVariableSchema &sys_variable,
      share::schema::ObSysParam *sys_params,
      int64_t params_capacity);
  int get_is_standby_cluster(bool &is_standby) const;
  int check_can_alter_column(
      const int64_t tenant_id,
      const share::schema::AlterTableSchema &alter_table_schema,
      const share::schema::ObTableSchema &orig_table_schema);
  int add_sys_table_lob_aux(const int64_t tenant_id, const uint64_t table_id,
                            ObTableSchema &meta_schema, ObTableSchema &data_schema);
  int check_has_multi_autoinc(share::schema::ObTableSchema &table_schema);

private:
#ifdef OB_BUILD_ARBITRATION
  int check_tenant_arbitration_service_status_(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const share::ObArbitrationServiceStatus &old_status,
      const share::ObArbitrationServiceStatus &new_status);
#endif
  int check_tenant_primary_zone_(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTenantSchema &new_tenant_schema);
  int get_partition_by_subpart_name(const share::schema::ObTableSchema &orig_table_schema,
                                    const share::schema::ObSubPartition &subpart_name,
                                    const share::schema::ObPartition *&part,
                                    const share::schema::ObSubPartition *&subpart);
  int gen_inc_table_schema_for_add_part(
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::AlterTableSchema &inc_table_schema);
  int gen_inc_table_schema_for_add_subpart(
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::AlterTableSchema &inc_table_schema);
  int gen_inc_table_schema_for_drop_part(
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::AlterTableSchema &inc_table_schema);
  int gen_inc_table_schema_for_drop_subpart(
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::AlterTableSchema &inc_table_schema);
public:
  //not check belong to the same table
  int check_same_partition(const bool is_oracle_mode, const ObPartition &l, const ObPartition &r,
                           const ObPartitionFuncType part_type, bool &is_matched) const;
  //not check belong to the same table
  int check_same_subpartition(const bool is_oracle_mode, const ObSubPartition &l, const ObSubPartition &r,
                              const ObPartitionFuncType part_type, bool &is_matched) const;
private:
  //After renaming a partition/subpartition, the consistency of the partition name between the data table and aux table is no longer guaranteed.
  //Therefore, the partition names in the inc aux table must be synchronized with the ori aux table after assigning the data table's partition
  //schema to the inc aux table.
  //This function relies on the assumption that the inc table schema has a valid partition name.
  int fix_local_idx_part_name_(const ObSimpleTableSchemaV2 &ori_data_table_schema,
                               const ObSimpleTableSchemaV2 &ori_table_schema,
                               ObSimpleTableSchemaV2 &inc_table_schema);
  //This function relies on the assumption that the inc table schema has a valid subpartition name.
  int fix_local_idx_subpart_name_(const ObSimpleTableSchemaV2 &ori_data_table_schema,
                                  const ObSimpleTableSchemaV2 &ori_table_schema,
                                  ObSimpleTableSchemaV2 &inc_table_schema);
  //During the process of adding a partition/subpartition, we only check whether the partition schema of the argument is valid.
  //It's possible for the inc aux table's partition name to duplicate with an existing partition name if one renames a partition/subpartition
  //to another name and then adds a partition/subpartition with the same name.
  //In this case, we will generate a name with a part/subpart id to replace the inc part/subpart name to avoid duplication.
  int fix_local_idx_part_name_for_add_part_(const ObSimpleTableSchemaV2 &ori_table_schema,
                                          ObSimpleTableSchemaV2 &inc_table_schema);

  int fix_local_idx_part_name_for_add_subpart_(const ObSimpleTableSchemaV2 &ori_table_schema,
                                          ObSimpleTableSchemaV2 &inc_table_schema);
  int gen_inc_table_schema_for_rename_part_(
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::AlterTableSchema &inc_table_schema);
  int gen_inc_table_schema_for_trun_part(
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::AlterTableSchema &inc_table_schema,
      share::schema::AlterTableSchema &del_table_schema);
  int gen_inc_table_schema_for_rename_subpart_(
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::AlterTableSchema &inc_table_schema);
  int gen_inc_table_schema_for_trun_subpart(
      const share::schema::ObTableSchema &orig_table_schema,
      share::schema::AlterTableSchema &inc_table_schema,
      share::schema::AlterTableSchema &del_table_schema);
  //get gts value, return OB_STATE_NOT_MATCH when is not external consistent
  int get_tenant_external_consistent_ts(const int64_t tenant_id, share::SCN &scn);
  int get_part_by_part_id(
      const share::schema::ObPartitionSchema &partition_schema,
      const int64_t part_id,
      const share::schema::ObPartition *&part);
  int check_table_pk(const share::schema::ObTableSchema &orig_table_schema);
  int clean_global_context(const ObContextSchema &context_schema);

  int get_hard_code_system_table_schema_(
      const uint64_t tenant_id,
      const uint64_t table_id,
      share::schema::ObTableSchema &hard_code_schema);
  int create_system_table_(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &hard_code_schema);
  int alter_system_table_column_(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &hard_code_schema);
  int get_obj_privs_ora(const uint64_t tenant_id,
                        const uint64_t obj_id,
                        const uint64_t obj_type,
                        ObSchemaGetterGuard &schema_guard,
                        ObIArray<ObObjPriv> &obj_privs);
  int restore_obj_privs_for_table(const uint64_t new_table_id,
                                  const common::ObString &database_name,
                                  const common::ObString &table_name,
                                  ObDDLOperator &ddl_operator,
                                  ObMySQLTransaction &trans,
                                  common::ObIArray<share::schema::ObObjPriv> &orig_obj_privs_ora);
  int inner_drop_and_create_tablet_(const int64_t &schema_version,
                                    const ObIArray<const ObTableSchema*> &orig_table_schemas,
                                    const ObIArray<ObTableSchema*> &new_table_schemas,
                                    ObMySQLTransaction &trans);
  int check_alter_tenant_when_rebalance_is_disabled_(
      const share::schema::ObTenantSchema &orig_tenant_schema,
      const share::schema::ObTenantSchema &new_tenant_schema);

  // this function is used for add extra tenant config init during create excepet data version
  // The addition of new configuration items requires the addition or modification of related test cases to ensure their effectiveness.
  int add_extra_tenant_init_config_(
      const uint64_t tenant_id,
      common::ObIArray<common::ObConfigPairs> &init_configs);

private:
  int check_locality_compatible_(ObTenantSchema &schema);

  int pre_rename_mysql_columns_online(const ObTableSchema &origin_table_schema,
                          const AlterTableSchema &alter_table_schema,
                          const bool is_oracle_mode,
                          ObTableSchema &new_table_schema,
                          obrpc::ObAlterTableArg &alter_table_arg,
                          sql::ObSchemaChecker &schema_checker,
                          ObDDLOperator &ddl_operator,
                          common::hash::ObHashSet<ObColumnNameHashWrapper> &update_column_name_set,
                          common::ObMySQLTransaction &trans,
                          ObSchemaGetterGuard &schema_guard,
                          ObIArray<ObTableSchema> &idx_schema_array,
                          ObIArray<ObTableSchema> *global_idx_schema_array);

  int drop_column_online(const ObTableSchema &origin_table_schema, ObTableSchema &new_table_schema,
                         const ObString &orig_column_name, ObDDLOperator &ddl_operator,
                         ObSchemaGetterGuard &schema_guard, common::ObMySQLTransaction &trans,
                         common::hash::ObHashSet<ObColumnNameHashWrapper> &update_column_name_set);

  int drop_column_offline(const ObTableSchema &origin_table_schema,
                          ObTableSchema &new_table_schema,
                          ObSchemaGetterGuard &schema_guard, const ObString &orig_column_name,
                          const int64_t new_tbl_cols_cnt);
  int prepare_change_modify_column_online(AlterColumnSchema &alter_col,
                           const ObTableSchema &origin_table_schema,
                           const AlterTableSchema &alter_table_schema,
                           const bool is_oracle_mode,
                           obrpc::ObAlterTableArg &alter_table_arg,
                           ObTableSchema &new_table_schema,
                           sql::ObSchemaChecker &schema_checker,
                           ObDDLOperator &ddl_operator,
                           common::ObMySQLTransaction &trans,
                           ObSchemaGetterGuard &schema_guard,
                           ObIArray<ObTableSchema> *global_idx_schema_array,
                           common::hash::ObHashSet<ObColumnNameHashWrapper> &update_column_name_set,
                           ObColumnSchemaV2 &new_column_schema);

  int prepare_change_modify_column_offline(AlterColumnSchema &alter_col,
                           const ObTableSchema &origin_table_schema,
                           const AlterTableSchema &alter_table_schema,
                           const bool is_oracle_mode,
                           obrpc::ObAlterTableArg &alter_table_arg,
                           ObTableSchema &new_table_schema,
                           sql::ObSchemaChecker &schema_checker,
                           ObSchemaGetterGuard &schema_guard,
                           common::hash::ObHashSet<ObColumnNameHashWrapper> &update_column_name_set,
                           ObColumnSchemaV2 &new_column_schema,
                           bool &is_contain_part_key);
  int pre_rename_mysql_columns_offline(
    const ObTableSchema &origin_table_schema, AlterTableSchema &alter_table_schema,
    bool is_oracle_mode, obrpc::ObAlterTableArg &alter_table_arg, ObTableSchema &new_table_schema,
    sql::ObSchemaChecker &schema_checker,
    ObSchemaGetterGuard &schema_guard,
    common::hash::ObHashSet<ObColumnNameHashWrapper> &update_column_name_set,
    bool &need_redistribute_column_id,
    bool &is_contain_part_key);

  int check_new_columns_for_index(ObIArray<ObTableSchema> &idx_schemas,
                                  const ObTableSchema &origin_table_schema,
                                  ObIArray<ObColumnSchemaV2> &new_column_schemas);

  int check_rename_first(const AlterTableSchema &alter_table_schema,
                         const ObTableSchema &table_schema,
                         const bool is_oracle_mode,
                         bool &is_rename_first);

  inline bool is_rename_column(const AlterColumnSchema &alter_column_schema)
  {
    ObColumnNameHashWrapper orig_key(alter_column_schema.get_origin_column_name());
    ObColumnNameHashWrapper new_key(alter_column_schema.get_column_name_str());
    return !(orig_key == (new_key));
  }
  int start_mview_complete_refresh_task(ObMySQLTransaction &trans,
                                        ObSchemaGetterGuard &schema_guard,
                                        const ObTableSchema &mview_schema,
                                        const ObTableSchema &container_table_schema,
                                        const ObIArray<ObDependencyInfo> *dep_infos,
                                        common::ObIAllocator &allocator,
                                        const uint64_t tenant_data_version,
                                        const share::schema::ObMViewInfo &mview_info,
                                        ObDDLTaskRecord &task_record);

  bool need_modify_dep_obj_status(const obrpc::ObAlterTableArg &alter_table_arg) const;

private:
  bool inited_;
  volatile bool stopped_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  obrpc::ObCommonRpcProxy *common_rpc_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::ObLSTableOperator *lst_operator_;
  //TODO(jingqian): used to choose partition server, use load balancer finnally
  ObZoneManager *zone_mgr_;
  ObUnitManager *unit_mgr_;
  ObSnapshotInfoManager *snapshot_mgr_;
  ObLatch ddl_lock_; // for ddl concurrent control

  // for paralled ddl to cache oracle's index name map
  share::schema::ObIndexNameChecker index_name_checker_;
  share::schema::ObNonPartitionedTableTabletAllocator non_partitioned_tablet_allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDDLService);
};

class ObDDLSQLTransaction : public common::ObMySQLTransaction
{
public:
  ObDDLSQLTransaction(share::schema::ObMultiVersionSchemaService *schema_service,
                      const bool need_end_signal = true,
                      const bool enable_query_stash = false,
                      const bool enable_ddl_parallel = false,
                      const bool enable_check_ddl_epoch = true,
                      const bool enable_check_newest_schema = true)
                      : common::ObMySQLTransaction(enable_query_stash),
                        schema_service_(schema_service),
                        tenant_id_(OB_INVALID_TENANT_ID),
                        start_operation_schema_version_(OB_INVALID_VERSION),
                        start_operation_tenant_id_(OB_INVALID_TENANT_ID),
                        need_end_signal_(need_end_signal),
                        trans_start_schema_version_(0),
                        enable_ddl_parallel_(enable_ddl_parallel),
                        enable_check_ddl_epoch_(enable_check_ddl_epoch),
                        trans_start_ddl_epoch_(OB_INVALID_VERSION),
                        enable_check_newest_schema_(enable_check_newest_schema)
                        {}
  virtual ~ObDDLSQLTransaction();

public:
  // If you commit the transaction, you need to write a line in ddl_operation once, the mark of end
  virtual int end(const bool commit) override;
  int64_t get_start_schema_version() const
  {
    return start_operation_schema_version_;
  }
  virtual int start(ObISQLClient *proxy,
                    const uint64_t &tenant_id,
                    const int64_t &tenant_refreshed_schema_version,
                    bool with_snapshot = false) override;
  virtual int start(ObISQLClient *proxy,
                    const uint64_t tenant_id,
                    bool with_snapshot = false,
                    const int32_t group_id = 0) override;
  static int lock_all_ddl_operation(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id);
  // use table_lock support ddl exclusive
  int lock_all_ddl_operation(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const bool enable_ddl_parallel);
  int register_tx_data(const uint64_t tenant_id,
                       const share::ObLSID &ls_id,
                       const transaction::ObTxDataSourceType &type,
                       const char *buf,
                       const int64_t buf_len);
  void disable_serialize_inc_schemas() { trans_start_schema_version_ = 0; }
  // serialize inc schemas from (start_schema_version, ]
  int serialize_inc_schemas(const int64_t start_schema_version);
private:
  // generate inc schema_metas and regist multi_data_source data
  // all schemas should contains basic info(id/name/schema_version/charset_type/collation_type)
  // @param [in] allocator          allocator used to generate meta and serialization.
  // @param [in] tenant_schemas     tenant_schema_array, tenant_schema should contains
  //                                compatibility_mode, and should record drop_tenant_time and
  //                                is_in_recyclebin if tenant is dropped.
  // @param [in] database_schemas   database_schema_array, database_schema should contains
  //                                tenant_id, and should record is_in_recyclebin if database is deleted
  // @param [in] table_schemas      table_schema_array, table_schema should contains table basic info including
  //                                tenant_id/db_id/table_type/table_mode/ etc. and also contains
  //                                rowkey_info/column_info/lob table info(meta_tid/piece_tid)/
  //                                index info(index_type/index_col_cnt/index_col_info) and
  //                                could provide table_id_arr..
  //                                ColumnSchema recorded in
  //                                table_schema(cols/rowkey_cols/index_cols) should preovide
  //                                rowkey_position/index_position/meta_type/accuracy/column_flag/
  //                                is_part_key_col/cur_default_value/orig_default_value/extended_type_info/... etc.
  // PLEASE REFER src/logservice/data_dictionary/ob_data_dict_struct.cpp FOR DETAIL.
  int serialize_inc_schemas_(
      ObIAllocator &allocator,
      const ObIArray<const ObTenantSchema*> &tenant_schemas,
      const ObIArray<const ObDatabaseSchema*> &database_schemas,
      const ObIArray<const ObTableSchema*> &table_schemas);
  // regist multi_data_source data into trans
  int regist_multi_source_data_();
  int lock_ddl_epoch_(common::ObMySQLTransaction &trans);

private:
  share::schema::ObMultiVersionSchemaService *schema_service_;
  //insert into the tenant's __all_ddl_operation
  //such as the first and third trans of create tenant, the tenant_id is OB_SYS_TENANT_ID;
  //the second trans of create tenant, the tenant_id is creating tennat.
  int64_t tenant_id_;
  // Filter out only one 1503 DDL transaction to prevent the schema from being invalidly pushed up
  int64_t start_operation_schema_version_;
  uint64_t start_operation_tenant_id_;
  //no need to set end_signal while ddl end transaction
  bool need_end_signal_;
  // Used for fetch increment schemas generate by this DDL trans.
  // 1. when bootstrap/create tenant, trans_start_schema_version_ is 0, won't fetch increment schema.
  // 2. when enable_ddl_parallel_ = true(truncate table in 4.1), trans_start_schema_version_ is meaningless, it needs fetch increment schema alone.
  // 3. in some situations, serialize inc schemas may be useless(eg. drop database to recyclebin). Can disable serialize logic by disable_serialize_inc_schemas().
  // 4. other situations, fetch increament schemas in (trans_start_schema_version_, ].
  int64_t trans_start_schema_version_;
  // enable ddl parallel
  bool enable_ddl_parallel_;

  bool enable_check_ddl_epoch_;
  // for compare
  int64_t trans_start_ddl_epoch_;

  // default true to check newest schema; daily major set false not check just use schema from inner table
  bool enable_check_newest_schema_;
};
// Fill in the partition name and the high values of the last partition
template<typename SCHEMA, typename ALTER_SCHEMA>
int ObDDLService::fill_part_name(const SCHEMA &orig_schema,
                                 ALTER_SCHEMA &alter_schema)
{
  int ret = OB_SUCCESS;
  const int64_t part_num = alter_schema.get_partition_num();
  share::schema::ObPartition **part_array = alter_schema.get_part_array();
  int64_t part_id = OB_INVALID_ID;
  int64_t max_part_id = OB_INVALID_ID;
  if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(WARN, "part_array is null", K(ret), K(part_array));
  } else if (OB_FAIL(orig_schema.get_max_part_idx(max_part_id, orig_schema.is_external_table()))) {
    RS_LOG(WARN, "fail to get max part id", KR(ret), K(max_part_id));
  }
  // Supplement the default partition name p+OB_MAX_PARTITION_NUM_MYSQL, accumulate after judging duplicates
  // Only Oracle mode will go to this logic
  //FIXME: partition_name may still conflict in one table since we can specify partition_name.
  max_part_id += OB_MAX_PARTITION_NUM_MYSQL;
  ObString part_name_str;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
    if (OB_ISNULL(part_array[i])) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "part is null", K(ret), K(part_array[i]));
    }
    while (OB_SUCC(ret) && part_array[i]->is_empty_partition_name()) {
      // Process each partition with an empty partition name
      char part_name[OB_MAX_PARTITION_NAME_LENGTH];
      int64_t pos = 0;
      if (OB_FAIL(databuff_printf(part_name, OB_MAX_PARTITION_NAME_LENGTH, pos, "P%ld", max_part_id))) {
        RS_LOG(WARN, "failed to constrate partition name", K(ret), K(max_part_id));
      } else {
        part_name_str.assign(part_name, static_cast<int32_t>(pos));
        bool is_valid = false;
        if (OB_FAIL(check_partition_name_valid(orig_schema, alter_schema, part_name_str, is_valid))) {
          RS_LOG(WARN, "failed to check partition name valid", K(ret), K(part_name_str));
        } else if (is_valid) {
          // If the partition name is reasonable, can add it to the partition, prepare to process
          // the next empty partition name
          if (OB_FAIL(part_array[i]->set_part_name(part_name_str))) {
            RS_LOG(WARN, "failed to set partition name", K(ret), K(part_name_str));
          }
          max_part_id++;
          break;
        } else {
          // If the partition is unreasonable, prepare for the next partition name verification
          max_part_id++;
        }
      }
    }// end while
  }// end for
  return ret;
}
// Check whether the partition name is in conflict with the existing partition,
// and whether the newly added partition conflicts
template <typename SCHEMA, typename ALTER_SCHEMA>
int ObDDLService::check_partition_name_valid(const SCHEMA &orig_schema,
                                             const ALTER_SCHEMA &alter_schema,
                                             const ObString part_name, bool &valid)
{
  int ret = OB_SUCCESS;
  share::schema::ObPartition **part_array = alter_schema.get_part_array();
  const int64_t part_num = alter_schema.get_partition_num();
  if (OB_ISNULL(part_array)) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "part array is null", K(ret), K(part_name));
  } else {
    valid = true;
    // Determine whether there is a conflict between the partition name and the newly added partition name
    for (int64_t j = 0; OB_SUCC(ret) && j < part_num; ++j) {
      if (OB_ISNULL(part_array[j])) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "failed to cherk partition", K(ret), K(part_array[j]));
      } else if (part_array[j]->get_part_name().empty()) {
        continue;
      } else if (ObCharset::case_insensitive_equal(part_name,
                                                   part_array[j]->get_part_name())) {
        valid = false;
        break;
      }
    }// end for
    // Determine whether the partition name is conflicting with the existing partition
    if (orig_schema.get_part_level() != share::schema::PARTITION_LEVEL_ZERO) {
      const int64_t orig_part_num = orig_schema.get_partition_num();
      share::schema::ObPartition **orig_part_array = orig_schema.get_part_array();
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(orig_part_array)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "part_array is null", K(ret), K(orig_part_array));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < orig_part_num; ++i) {
        if (OB_ISNULL(orig_part_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          RS_LOG(WARN, "part is null", K(ret), KP(orig_part_array[i]), K(orig_schema));
        } else if (ObCharset::case_insensitive_equal(orig_part_array[i]->get_part_name(),
                                                     part_name)) {
          valid = false;
          RS_LOG(INFO, "partition name not valid", K(part_name), K(orig_part_array));
          break;
        }
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbasr
#endif // _OCEANBASE_ROOTSERVER_OB_DDL_SERVICE_H_
