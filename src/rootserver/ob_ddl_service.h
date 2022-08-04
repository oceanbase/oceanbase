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

#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_placement_hashset.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_leader_election_waiter.h"
#include "share/ob_worker.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_unit_getter.h"
#include "rootserver/ob_ddl_operator.h"
#include "ob_root_balancer.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_unit_replica_counter.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
class ObAddr;
class ObMySQLTransaction;
template <class T>
class ObIArray;
}  // namespace common
namespace obrpc {
class ObAccountArg;
}

namespace share {
class ObPartitionTableOperator;
class ObAutoincrementService;
class ObSplitInfo;
namespace schema {
class ObTenantSchema;
class ObDatabaseSchema;
class ObTablegroupSchema;
class ObTableSchema;
class ObMultiVersionSchemaService;
class ObNeedPriv;
class ObSchemaMgr;
}  // namespace schema
}  // namespace share

namespace obrpc {
class ObSrvRpcProxy;
class ObCommonRpcProxy;
}  // namespace obrpc

namespace rootserver {
class ObDDLOperator;
class ObServerManager;
class ObZoneManager;
class ObUnitManager;
class ObPartitionCreator;
class ObCommitAlterTenantLocalityArg;
class ObCommitAlterTablegroupLocalityArg;
class ObCommitAlterTableLocalityArg;
class ObTableGroupHelp;
class ObFreezeInfoManager;
class ObSnapshotInfoManager;
class ObSinglePartBalance;

class ObDDLService {
public:
  typedef common::hash::ObHashMap<common::ObPGKey, share::ObPartitionInfo, common::hash::NoPthreadDefendMode>
      PartitionInfoMap;

public:
  friend class ObTableGroupHelp;
  friend class ObStandbyClusterSchemaProcessor;
  ObDDLService();
  virtual ~ObDDLService()
  {}

  int init(obrpc::ObSrvRpcProxy& rpc_proxy, obrpc::ObCommonRpcProxy& common_rpc, common::ObMySQLProxy& sql_proxy,
      share::schema::ObMultiVersionSchemaService& schema_service, share::ObPartitionTableOperator& pt_operator,
      ObServerManager& server_mgr, ObZoneManager& zone_mgr, ObUnitManager& unit_mgr, ObRootBalancer& root_balancer,
      ObFreezeInfoManager& freeze_info_manager, ObSnapshotInfoManager& snapshot_mgr, ObRebalanceTaskMgr& task_mgr);
  bool is_inited() const
  {
    return inited_;
  }
  void stop()
  {
    stopped_ = true;
  }
  void restart()
  {
    stopped_ = false;
  }
  bool is_stopped()
  {
    return stopped_;
  }
  // these functions should be called after ddl_service has been inited
  share::schema::ObMultiVersionSchemaService& get_schema_service()
  {
    return *schema_service_;
  }
  common::ObMySQLProxy& get_sql_proxy()
  {
    return *sql_proxy_;
  }
  ObServerManager& get_server_manager()
  {
    return *server_mgr_;
  }
  ObZoneManager& get_zone_mgr()
  {
    return *zone_mgr_;
  }
  ObSnapshotInfoManager& get_snapshot_mgr()
  {
    return *snapshot_mgr_;
  }
  ObFreezeInfoManager& get_freeze_info_mgr()
  {
    return *freeze_info_manager_;
  }
  ObRootBalancer* get_root_balancer()
  {
    return root_balancer_;
  }
  share::ObPartitionTableOperator& get_pt_operator()
  {
    return *pt_operator_;
  }

  virtual int statistic_primary_zone_entity_count();
  virtual int upgrade_cluster_create_ha_gts_util();
  virtual int prepare_create_partition(ObPartitionCreator& creator, share::schema::ObTableSchema& table_schema,
      const int64_t paxos_replica_num, const common::ObArray<share::ObUnit>& units,
      const common::ObAddr& suggest_leader, const int64_t frozen_version, const bool is_standby);
  // create_user_table will fill table_id and frozen_version to table_schema
  virtual int create_user_table(
      const obrpc::ObCreateIndexArg& arg, share::schema::ObTableSchema& table_schema, const int64_t frozen_version);

  int rebuild_index(const obrpc::ObRebuildIndexArg& arg, const int64_t frozen_version, obrpc::ObAlterTableRes& res);

  int rebuild_index_in_trans(share::schema::ObSchemaGetterGuard& schema_guard,
      share::schema::ObTableSchema& table_schema, const int64_t frozen_version, const ObString* ddl_stmt_str,
      const obrpc::ObCreateTableMode create_mode, ObMySQLTransaction* sql_trans,
      bool* is_delay_delete = NULL /* Bring out the delayed delete behavior */);

  int create_inner_expr_index(const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObTableSchema& new_table_schema, common::ObIArray<share::schema::ObColumnSchemaV2*>& new_columns,
      share::schema::ObTableSchema& index_schema, const obrpc::ObCreateTableMode create_mode,
      const int64_t frozen_version, const common::ObString* ddl_stmt_str);
  int generate_global_index_locality_and_primary_zone(
      const share::schema::ObTableSchema& table_schema, share::schema::ObTableSchema& index_schema);
  int create_global_index(const obrpc::ObCreateIndexArg& arg, const share::schema::ObTableSchema& table_schema,
      share::schema::ObTableSchema& index_schema, const int64_t frozen_version);
  int create_global_inner_expr_index(const obrpc::ObCreateIndexArg& arg,
      const share::schema::ObTableSchema& orig_table_schema, share::schema::ObTableSchema& new_table_schema,
      common::ObIArray<share::schema::ObColumnSchemaV2*>& new_columns, share::schema::ObTableSchema& index_schema,
      const int64_t frozen_version);
  template <typename SCHEMA>
  int check_primary_zone_locality_condition(const SCHEMA& schema, const ObIArray<common::ObZone>& zone_list,
      const ObIArray<share::schema::ObZoneRegion>& zone_region_list, share::schema::ObSchemaGetterGuard& schema_guard);
  template <typename SCHEMA>
  int extract_first_primary_zone_array(const SCHEMA& schema, const ObIArray<common::ObZone>& zone_list,
      ObIArray<common::ObZone>& first_primary_zone_array);
  int get_primary_regions_and_zones(const ObIArray<common::ObZone>& zone_list,
      const ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      const ObIArray<common::ObZone>& first_primary_zone_array, ObIArray<common::ObRegion>& primary_regions,
      ObIArray<common::ObZone>& zones_in_primary_regions);
  int do_check_primary_zone_locality_condition(const ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      const ObIArray<common::ObZone>& first_primary_zone_array,
      const ObIArray<common::ObZone>& zones_in_primary_regions, const ObIArray<common::ObRegion>& primary_regions,
      const ObIArray<share::ObZoneReplicaAttrSet>& zone_locality);
  int do_check_primary_zone_region_condition(const ObIArray<common::ObZone>& zones_in_primary_regions,
      const ObIArray<common::ObRegion>& primary_regions, const ObIArray<share::ObZoneReplicaAttrSet>& zone_locality);
  int do_check_mixed_zone_locality_condition(const ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      const ObIArray<share::ObZoneReplicaAttrSet>& zone_locality);
  int do_check_mixed_locality_primary_zone_condition(const ObIArray<common::ObZone>& first_primary_zone_array,
      const ObIArray<share::ObZoneReplicaAttrSet>& zone_locality);
  // batch mode in trx
  virtual int create_user_tables(const bool if_not_exist, const common::ObString& ddl_stmt_str,
      common::ObIArray<share::schema::ObTableSchema>& table_schemas, const int64_t frozen_version,
      obrpc::ObCreateTableMode create_mode, share::schema::ObSchemaGetterGuard& schema_guard,
      const uint64_t last_replay_log_id = 0);

  virtual int create_table_like(const obrpc::ObCreateTableLikeArg& arg, const int64_t frozen_version);

  int update_table_schema_version(const share::schema::ObTableSchema* table_schema);
  int update_sys_table_schema_version_in_tenant_space();
  virtual int alter_table(obrpc::ObAlterTableArg& alter_table_arg, const int64_t frozen_version);
  virtual int drop_table(const obrpc::ObDropTableArg& drop_table_arg);
  virtual int force_drop_schema(const obrpc::ObForceDropSchemaArg& arg);
  virtual int update_index_status(const obrpc::ObUpdateIndexStatusArg& arg, const int64_t create_mem_version);

  virtual int add_table_schema(share::schema::ObTableSchema& table_schema);
  virtual int drop_inner_table(const share::schema::ObTableSchema& table_schema);
  virtual int create_sys_tenant(const obrpc::ObCreateTenantArg& arg, share::schema::ObTenantSchema& tenant_schema);
  virtual int create_tenant(
      const obrpc::ObCreateTenantArg& arg, share::schema::ObTenantSchema& tenant_schema, const int64_t frozen_version);
  virtual int create_tenant_end(const uint64_t tenant_id);
  virtual int commit_alter_tenant_locality(const rootserver::ObCommitAlterTenantLocalityArg& arg);
  virtual int commit_alter_tablegroup_locality(const rootserver::ObCommitAlterTablegroupLocalityArg& arg);
  virtual int commit_alter_table_locality(const rootserver::ObCommitAlterTableLocalityArg& arg);
  virtual int create_tenant_partitions(const obrpc::ObCreateTenantArg& arg,
      const share::schema::ObTenantSchema& tenant_schema, common::ObAddrArray& leader_addrs,
      const int64_t schema_version, const int64_t frozen_version, ObPartitionCreator& creator);
  virtual int modify_tenant(const obrpc::ObModifyTenantArg& arg);
  virtual int drop_tenant(const obrpc::ObDropTenantArg& arg);
  virtual int flashback_tenant(const obrpc::ObFlashBackTenantArg& arg);
  virtual int purge_tenant(const obrpc::ObPurgeTenantArg& arg);

  virtual int lock_tenant(const common::ObString& tenant_name, const bool is_lock);
  virtual int add_system_variable(const obrpc::ObAddSysVarArg& arg);
  virtual int modify_system_variable(const obrpc::ObModifySysVarArg& arg);
  virtual int create_database(const bool if_not_exist, share::schema::ObDatabaseSchema& database_schema,
      const common::ObString* ddl_stmt_str, common::ObMySQLTransaction* trans = NULL);
  virtual int alter_database(const obrpc::ObAlterDatabaseArg& arg);
  virtual int drop_database(
      const obrpc::ObDropDatabaseArg& arg, obrpc::UInt64& affected_row, common::ObMySQLTransaction* trans = NULL);
  virtual int create_tablegroup(const bool if_not_exist, share::schema::ObTablegroupSchema& tablegroup_schema,
      const common::ObString* ddl_stmt_str, const obrpc::ObCreateTableMode create_mode);
  virtual int create_tablegroup_partitions(
      const obrpc::ObCreateTableMode create_mode, const share::schema::ObTablegroupSchema& tablegroup_schema);
  virtual int drop_tablegroup(const obrpc::ObDropTablegroupArg& arg);
  virtual int alter_tablegroup(const obrpc::ObAlterTablegroupArg& arg);
  virtual int generate_schema(
      const obrpc::ObCreateTableArg& arg, share::schema::ObTableSchema& schema, const int64_t frozen_version);
  virtual int alter_table_index(const obrpc::ObAlterTableArg& alter_table_arg,
      const share::schema::ObTableSchema& orgin_table_schema, share::schema::ObTableSchema& new_table_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, const int64_t frozen_version, ObDDLOperator& ddl_operator,
      common::ObMySQLTransaction& trans, common::ObArenaAllocator& allocator);
  virtual int alter_table_partitions(const obrpc::ObAlterTableArg::AlterPartitionType type,
      const share::schema::ObTableSchema& orig_table_schema, share::schema::AlterTableSchema& inc_table_schema,
      share::schema::ObTableSchema& new_table_schema, ObDDLOperator& ddl_operator, common::ObMySQLTransaction& trans);
  virtual int alter_table_constraints(const obrpc::ObAlterTableArg::AlterConstraintType type,
      const share::schema::ObTableSchema& orig_table_schema, share::schema::AlterTableSchema& inc_table_schema,
      share::schema::ObTableSchema& new_table_schema, ObDDLOperator& ddl_operator, common::ObMySQLTransaction& trans);
  virtual int alter_table_foreign_keys(const share::schema::ObTableSchema& orig_table_schema,
      share::schema::AlterTableSchema& inc_table_schema, share::schema::ObTableSchema& new_table_schema,
      ObDDLOperator& ddl_operator, common::ObMySQLTransaction& trans);
  virtual int truncate_table(const obrpc::ObTruncateTableArg& arg, const int64_t frozen_version);
  int force_drop_tablegroup_and_partitions(const uint64_t tablegroup_id,
      share::schema::ObSchemaGetterGuard& schema_guard, const common::ObIArray<int64_t>& partition_ids,
      ObMySQLTransaction& trans);
  int fill_truncate_table_fk_err_msg(const share::schema::ObForeignKeyInfo& foreign_key_info,
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema& parent_table_schema,
      char* buf, const int64_t& buf_len, int64_t& pos) const;
  int get_uk_cst_id_for_self_ref(const ObIArray<share::schema::ObTableSchema>& table_schemas,
      const obrpc::ObCreateForeignKeyArg& foreign_key_arg, share::schema::ObForeignKeyInfo& foreign_key_info);
  int check_constraint_name_is_exist(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& table_schema, const common::ObString& constraint_name,
      bool& is_constraint_name_exist);
  virtual int rename_table(const obrpc::ObRenameTableArg& rename_table_arg);
  int collect_temporary_tables_in_session(const obrpc::ObDropTableArg& drop_table_arg);
  int need_collect_current_temp_table(share::schema::ObSchemaGetterGuard& schema_guard,
      obrpc::ObDropTableArg& drop_table_arg, const share::schema::ObSimpleTableSchemaV2* table_schema,
      bool& need_collect);
  int check_sessid_exist_in_temp_table(const ObString& db_name, const ObString& tab_name, const uint64_t tenant_id,
      const uint64_t session_id, bool& exists);
  int check_table_exist(share::schema::ObTableSchema& table_schema);
  int check_database_exist(const uint64_t tenant_id, const common::ObString& database_name, uint64_t& database_id);
  int check_create_with_db_id(share::schema::ObDatabaseSchema& schema);
  int replace_table_schema_type(share::schema::ObTableSchema& schema);
  // drop/truncate limitation for depend table
  int check_table_has_materialized_view(
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema& table_schema, bool& has_mv);

  //----Functions for recyclebin ----

  int check_object_name_matches_db_name(
      const uint64_t tenant_id, const ObString& origin_table_name, const uint64_t database_id, bool& is_match);
  static int get_object_name_with_origin_name_in_recyclebin(const uint64_t tenant_id, const ObString& origin_table_name,
      const uint64_t database_id, ObString& object_name, common::ObIAllocator* allocator,
      common::ObMySQLProxy* sql_proxy);
  int flashback_table_from_recyclebin(const obrpc::ObFlashBackTableFromRecyclebinArg& arg);
  int flashback_table_from_recyclebin_in_trans(const share::schema::ObTableSchema& table_schema,
      const uint64_t new_db_id, const common::ObString& new_table_name, const common::ObString& ddl_stmt_str,
      share::schema::ObSchemaGetterGuard& guard);
  int flashback_aux_table(const share::schema::ObTableSchema& table_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, ObMySQLTransaction& trans, ObDDLOperator& ddl_operator,
      const uint64_t new_db_id, const share::schema::ObTableType table_type);
  int purge_table(const obrpc::ObPurgeTableArg& arg, int64_t& pz_count, ObMySQLTransaction* trans = NULL);

  int flashback_database(const obrpc::ObFlashBackDatabaseArg& arg);
  int flashback_database_in_trans(const share::schema::ObDatabaseSchema& db_schema, const common::ObString& new_db_name,
      share::schema::ObSchemaGetterGuard& guard, const common::ObString& ddl_stmt_str);
  int purge_database(const obrpc::ObPurgeDatabaseArg& arg, int64_t& pz_count, ObMySQLTransaction* trans = NULL);
  int purge_tenant_expire_recycle_objects(const obrpc::ObPurgeRecycleBinArg& arg, int64_t& purge_expire_objects);
  int purge_recyclebin_except_tenant(const obrpc::ObPurgeRecycleBinArg& arg,
      const ObIArray<share::schema::ObRecycleObject>& recycle_objs, int64_t& purged_objects);
  int purge_recyclebin_tenant(const obrpc::ObPurgeRecycleBinArg& arg,
      const ObIArray<share::schema::ObRecycleObject>& recycle_objs, int64_t& purged_objects);
  int flashback_index(const obrpc::ObFlashBackIndexArg& arg);
  int flashback_index_in_trans(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& table_schema, const uint64_t new_db_id,
      const common::ObString& new_table_name, const common::ObString& ddl_stmt_str);
  int purge_index(const obrpc::ObPurgeIndexArg& arg);
  //----End of Functions for recyclebin ---
  //----Functions for managing privileges----
  virtual int create_sys_user(share::schema::ObUserInfo& user_info);
  int create_user(obrpc::ObCreateUserArg& arg, common::ObIArray<int64_t>& failed_index);
  virtual int revoke_all(const uint64_t tenant_id, const common::ObString& user_name, const common::ObString& host_name,
      const uint64_t user_id);
  int rename_user(const obrpc::ObRenameUserArg& arg, ObIArray<int64_t>& failed_index);
  int set_passwd(const obrpc::ObSetPasswdArg& arg);
  int drop_user(const obrpc::ObDropUserArg& arg, common::ObIArray<int64_t>& failed_index);

  int grant_sys_priv_to_ur(const obrpc::ObGrantArg& arg, share::schema::ObSchemaGetterGuard& schema_guard);
  int exists_role_grant_cycle(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
      uint64_t role_id, const share::schema::ObUserInfo* user_info);
  virtual int grant(const obrpc::ObGrantArg& arg);
  int revoke(const obrpc::ObRevokeUserArg& arg);
  virtual int grant_priv_to_user(const uint64_t tenant_id, const uint64_t user_id, const common::ObString& user_name,
      const common::ObString& host_name, const share::schema::ObNeedPriv& need_priv,
      const share::ObRawObjPrivArray& obj_priv_array, const uint64_t option,
      share::schema::ObObjPrivSortKey& obj_priv_key);
  virtual int grant_table_and_col_privs_to_user(const obrpc::ObGrantArg& arg, uint64_t grantee_id, ObString& user_name,
      ObString& host_name, share::schema::ObNeedPriv& need_priv);
  virtual int grant_revoke_user(const uint64_t tenant_id, const uint64_t user_id, const ObPrivSet priv_set,
      const bool grant, const common::ObString* ddl_stmt_str);

  int lock_user(const obrpc::ObLockUserArg& arg, common::ObIArray<int64_t>& failed_index);
  int standby_grant(const obrpc::ObStandbyGrantArg& arg);

  int alter_user_profile(const obrpc::ObAlterUserProfileArg& arg);
  int alter_user_default_role(const obrpc::ObAlterUserProfileArg& arg);
  int get_all_users_in_tenant_with_profile(
      const uint64_t profile_id, share::schema::ObSchemaGetterGuard& schema_guard, ObIArray<uint64_t>& user_ids);
  virtual int grant_database(
      const share::schema::ObOriginalDBKey& db_key, const ObPrivSet priv_set, const common::ObString* ddl_stmt_str);
  virtual int revoke_database(const share::schema::ObOriginalDBKey& db_key, const ObPrivSet priv_set);
  virtual int revoke_syspriv(const uint64_t tenant_id, const uint64_t grantee_id,
      const share::ObRawPrivArray& sys_priv_array, const common::ObString* ddl_stmt_str);
  virtual int grant_table(const share::schema::ObTablePrivSortKey& table_key, const ObPrivSet priv_set,
      const common::ObString* ddl_stmt_str);
  virtual int grant_table(const share::schema::ObTablePrivSortKey& table_key, const ObPrivSet priv_set,
      const common::ObString* ddl_stmt_str, const share::ObRawObjPrivArray& obj_priv_array, const uint64_t option,
      const share::schema::ObObjPrivSortKey& obj_key);
  virtual int revoke_table(const share::schema::ObTablePrivSortKey& table_key, const ObPrivSet priv_set,
      const share::schema::ObObjPrivSortKey& obj_key, const share::ObRawObjPrivArray& obj_priv_array,
      const bool revoke_all_ora);
  //----End of functions for managing privileges----
  //----Functions for managing outlines----
  virtual int check_outline_exist(
      share::schema::ObOutlineInfo& Outline_info, const bool create_or_replace, bool& is_update);
  virtual int create_outline(
      share::schema::ObOutlineInfo& outline_info, const bool is_update, const common::ObString* ddl_stmt_str);
  virtual int alter_outline(const obrpc::ObAlterOutlineArg& alter_outline_arg);
  virtual int drop_outline(const obrpc::ObDropOutlineArg& drop_outline_arg);
  //----End of functions for managing outlines----
  //----Functions for managing dblinks----
  virtual int create_dblink(const obrpc::ObCreateDbLinkArg& arg, const common::ObString* ddl_stmt_str);
  virtual int drop_dblink(const obrpc::ObDropDbLinkArg& arg, const common::ObString* ddl_stmt_str);
  //----End of functions for managing dblinks----
  //----Functions for managing synonym----
  virtual int check_synonym_exist(
      share::schema::ObSynonymInfo& Synonym_info, const bool create_or_replace, bool& is_update);
  virtual int create_synonym(
      share::schema::ObSynonymInfo& synonym_info, const common::ObString* ddl_stmt_str, bool is_replace);
  virtual int drop_synonym(const obrpc::ObDropSynonymArg& drop_synonym_arg);
  //----End of functions for managing synonym----

  //----Functions for managing udf----
  virtual int create_user_defined_function(share::schema::ObUDF& udf_info, const common::ObString& ddl_stmt_str);
  virtual int drop_user_defined_function(const obrpc::ObDropUserDefinedFunctionArg& drop_func_arg);
  virtual int check_udf_exist(uint64 tenant_id, const common::ObString& name, bool& is_exsit);
  //----End of functions for managing udf----

  //----Functions for managing sequence----
  int do_sequence_ddl(const obrpc::ObSequenceDDLArg& arg);
  //----End of functions for managing sequence----

  //----Functions for managing profile----
  int handle_profile_ddl(const obrpc::ObProfileDDLArg& arg);
  //----End of functions for managing profile----

  virtual int wait_elect_sys_leaders(
      const common::ObArray<uint64_t>& table_ids, const common::ObArray<int64_t>& part_nums);
  int check_need_wait_leader_by_table_schema(const share::schema::ObTableSchema& schema);
  int check_need_wait_leader_by_tablegroup_schema(const share::schema::ObTablegroupSchema& tablegroup_schema);
  int check_need_wait_leader_by_id(const uint64_t table_id, const int64_t partition_id);
  // refresh local schema busy wait
  virtual int refresh_schema(const uint64_t tenant_id);
  // notify other servers to refresh schema (call switch_schema  rpc)
  virtual int notify_refresh_schema(const common::ObAddrIArray& addrs);

  // TODO(): only used by random_choose_unit
  inline int set_sys_units(const common::ObIArray<share::ObUnit>& sys_units);

  obrpc::ObCommonRpcProxy* get_common_rpc()
  {
    return common_rpc_;
  }
  int get_tenant_schema_guard_with_version_in_inner_table(
      const uint64_t tenant_id, share::schema::ObSchemaGetterGuard& schema_guard);
  int drop_index_to_recyclebin(const share::schema::ObTableSchema& table_schema);
  int check_tenant_in_alter_locality(const uint64_t tenant_id, bool& in_alter_locality);
  int split_partition(const share::schema::ObTableSchema* schema);
  int split_tablegroup_partition(
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTablegroupSchema* tablegroup_schema);

  // only push schema version, and publish schema
  int log_nop_operation(const obrpc::ObDDLNopOpreatorArg& arg);
  // When the partition creation fails, the empty schema version needs to be pushed up once,
  // and the abandoned partition gc can be dropped.
  int process_create_partition_failed(const int64_t tenant_id);
  int finish_schema_split(int64_t& split_schema_version);

  virtual int publish_schema(const uint64_t tenant_id);

  int create_or_bind_tables_partitions(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& table_schema, const int64_t last_schema_version,
      obrpc::ObCreateTableMode create_mode, const int64_t frozen_version,
      const common::ObIArray<share::schema::ObTableSchema>& table_schemas, const uint64_t last_replay_log_id = 0);

  int force_set_locality(share::schema::ObSchemaGetterGuard& schema_guard, share::schema::ObTenantSchema& new_tenant);

  int check_split_partition_can_execute() const;

  int create_table_partitions_for_physical_restore(const obrpc::ObRestorePartitionsArg& arg,
      const common::hash::ObHashSet<int64_t>& base_part_id_set, const share::ObSimpleFrozenStatus frozen_status,
      const int64_t last_schema_version, share::schema::ObSchemaGetterGuard& schema_guard);
  int create_tablegroup_partitions_for_physical_restore(const obrpc::ObRestorePartitionsArg& arg,
      const common::hash::ObHashSet<int64_t>& base_part_id_set, const share::ObSimpleFrozenStatus frozen_status,
      const int64_t last_schema_version, share::schema::ObSchemaGetterGuard& schema_guard);
  int create_partitions_for_physical_restore(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObPartitionSchema& partition_schema, const obrpc::ObRestorePartitionsArg& arg,
      const common::hash::ObHashSet<int64_t>& base_part_id_set, const share::ObSimpleFrozenStatus frozen_status,
      const int64_t last_schema_version, const common::ObIArray<ObPartitionAddr>& addrs);
  int construct_table_schemas_for_physical_restore(share::schema::ObSchemaGetterGuard& schema_guard,
      const obrpc::ObRestorePartitionsArg& restore_arg, const common::hash::ObHashSet<int64_t>& base_part_id_set,
      const common::ObIArray<int64_t>& part_ids, common::ObIArray<share::schema::ObTableSchema>& base_table_schemas,
      common::ObIArray<share::schema::ObTableSchema>& inc_table_schemas);
  int construct_partitions_for_standby(share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<ObPartitionKey>& key, const int64_t schema_id, ObPartitionCreator& creator);
  int alloc_tenant_partitions(ObUnitManager* unit_mgr, ObZoneManager* zone_mgr,
      const common::ObIArray<share::ObResourcePoolName>& pools, const share::schema::ObTenantSchema& tenant_schema,
      ObReplicaCreator::ZoneUnitPtrArray& zone_units_ptr, ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr,
      ObPartitionAddr& partition_addr);

  int modify_schema_in_restore(const obrpc::ObRestoreModifySchemaArg& arg);

private:
  enum PartitionBornMethod : int64_t {
    PBM_INVALID = 0,
    PBM_DIRECTLY_CREATE,
    PBM_BINDING,
    PBM_MAX,
  };
  enum AlterLocalityOp {
    ALTER_LOCALITY = 0,
    ROLLBACK_ALTER_LOCALITY,
    NOP_LOCALITY_OP,
    ALTER_LOCALITY_OP_INVALID,
  };
  enum CreateTenantStatus {
    CREATE_TENANT_FAILED,
    CREATE_TENANT_TRANS_ONE_DONE,
    CREATE_TENANT_TRANS_TWO_DONE,
    CREATE_TENANT_TRANS_THREE_DONE,
  };
  enum RenameOracleObjectType {
    RENAME_TYPE_INVALID = 0,
    RENAME_TYPE_TABLE_OR_VIEW = 1,
    RENAME_TYPE_SYNONYM = 2,
    RENAME_TYPE_SEQUENCE = 3,
  };
  typedef common::hash::ObHashMap<common::ObZone, share::ObZoneReplicaNumSet, common::hash::NoPthreadDefendMode>
      ZoneLocalityMap;
  typedef common::hash::ObHashMap<common::ObRegion, share::ObRegionReplicaNumSet, common::hash::NoPthreadDefendMode>
      RegionLocalityMap;
  typedef ZoneLocalityMap::const_iterator const_zone_map_iterator;
  typedef ZoneLocalityMap::iterator zone_map_iterator;
  typedef RegionLocalityMap::const_iterator const_region_map_iterator;
  typedef RegionLocalityMap::iterator region_map_iterator;

  typedef common::ObArray<share::ObUnit> ObPartitionUnits;
  typedef common::ObArray<ObPartitionUnits> ObPartitionUnitsArray;
  static const int64_t WAIT_ELECT_LEADER_TIMEOUT_US = 120 * 1000 * 1000;  // 120s
  static const int64_t REFRESH_SCHEMA_INTERVAL_US = 500 * 1000;           // 500ms

  enum AlterLocalityType {
    TO_NEW_LOCALITY = 0,
    ROLLBACK_LOCALITY,
    LOCALITY_NOT_CHANGED,
    ALTER_LOCALITY_INVALID,
  };
  int update_global_index(obrpc::ObAlterTableArg& arg, const uint64_t tenant_id,
      const share::schema::ObTableSchema& orig_table_schema, ObDDLOperator& ddl_operator, const int64_t frozen_version,
      common::ObMySQLTransaction& trans);
  int upgrade_cluster_create_tenant_ha_gts_util(const uint64_t tenant_id);
  int check_rename_object_type(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
      const uint64_t database_id, const ObString& object_name, const share::schema::ObTableSchema*& table_schema,
      const share::schema::ObSynonymInfo*& synonym_info, const share::schema::ObSequenceSchema*& sequence_schema,
      RenameOracleObjectType& obj_type);
  int record_tenant_locality_event_history(const AlterLocalityOp& alter_locality_op,
      const obrpc::ObModifyTenantArg& arg, const share::schema::ObTenantSchema& tenant_schema,
      common::ObMySQLTransaction& trans);
  int record_table_locality_event_history(const AlterLocalityOp& alter_locality_op,
      const obrpc::ObAlterTableArg& alter_table_arg, const share::schema::ObTableSchema& table_schema,
      common::ObMySQLTransaction& trans);
  int record_tablegroup_locality_event_history(const AlterLocalityOp& alter_locality_op,
      const obrpc::ObAlterTablegroupArg& alter_tablegroup_arg,
      const share::schema::ObTablegroupSchema& tablegroup_schema, common::ObMySQLTransaction& trans);
  int check_inner_stat() const;
  template <typename SCHEMA>
  int check_pools_unit_num_enough_for_schema_locality(const common::ObIArray<share::ObResourcePoolName>& pools,
      share::schema::ObSchemaGetterGuard& schema_guard, const SCHEMA& schema);
  int do_create_tenant_partitions(const obrpc::ObCreateTenantArg& arg, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTenantSchema& tenant_schema, const int64_t schema_version,
      const ObITablePartitionAddr& table_addr, const int64_t frozen_version, ObPartitionCreator& creator);
  int get_tenant_units_and_scalescope(const common::ObIArray<obrpc::ObSysVarIdValue>& sys_var_list,
      const common::ObIArray<common::ObString>& pool_list, const share::schema::ObTenantSchema& tenant_schema,
      ObReplicaCreator::ZoneUnitArray& all_zone_unit, ObReplicaCreator::ZoneUnitPtrArray& all_zone_unit_ptr,
      bool& small_tenant);
  int alloc_tenant_partitions(const common::ObIArray<share::ObResourcePoolName>& pools,
      const share::schema::ObTenantSchema& tenant_schema, ObReplicaCreator::ZoneUnitPtrArray& zone_units_ptr,
      ObPartitionAddr& partition_addr);
  int get_zones_in_region(const common::ObRegion& region, const common::ObIArray<common::ObString>& zone_list,
      common::ObIArray<common::ObZone>& zones);
  int check_table_schema_not_in_new_tablegroup(
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema& table_schema);
  int get_sample_table_schema(common::ObIArray<const share::schema::ObSimpleTableSchemaV2*>& table_schemas,
      const share::schema::ObSimpleTableSchemaV2*& sample_table_schema);
  template <typename SCHEMA1, typename SCHEMA2>
  int check_primary_zone_match(
      share::schema::ObSchemaGetterGuard& schema_guard, SCHEMA1& new_table, SCHEMA2& sample_table, bool& match);
  template <typename SCHEMA1, typename SCHEMA2>
  int check_schemas_locality_match(
      SCHEMA1& left, SCHEMA2& right, share::schema::ObSchemaGetterGuard& schema_guard, bool& match);
  bool is_tables_locality_match(const common::ObIArray<share::ObZoneReplicaAttrSet>& left,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& rigth);
  bool is_locality_replica_attr_match(const share::ObReplicaAttrSet& left, const share::ObReplicaAttrSet& right);
  int check_locality_and_primary_zone_complete_match(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTablegroupSchema& tablegroup_schema);
  template <typename T>
  int check_locality_with_tenant(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTenantSchema& tenant_schema, T& table_schema);
  template <typename T>
  int try_set_and_check_locality_with_tenant(const bool just_check_zone_list,
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTenantSchema& tenant_schema,
      T& table_schema);
  int set_locality_with_tablegroup(
      share::schema::ObSchemaGetterGuard& schema_guard, share::schema::ObTableSchema& schema);
  int check_table_tenant_locality_completed_match(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& table_schema, const share::schema::ObTenantSchema& tenant_schema,
      bool& is_completed_match) const;
  int do_check_locality_in_tenant(share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& table_zone_locality,
      const share::schema::ObTenantSchema& tenant_schema,
      const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list, bool& match);
  template <typename T>
  int check_paxos_locality_match(const bool just_check_zone_list, share::schema::ObSchemaGetterGuard& schema_guard,
      const T& schema, const share::schema::ObTenantSchema& tenant_schema,
      const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list, bool& match);
  int check_table_tenant_locality_match(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObSimpleTableSchemaV2& table_schema, const share::schema::ObTenantSchema& tenant_schema,
      bool& match);
  int check_tablegroup_tenant_locality_match(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObSimpleTablegroupSchema& tablegroup_schema,
      const share::schema::ObTenantSchema& tenant_schema, bool& match);
  template <typename SCHEMA>
  int check_zone_list_with_tenant(share::schema::ObSchemaGetterGuard& schema_guard, const SCHEMA& schema,
      const share::schema::ObTenantSchema& tenant_schema, bool& zone_match);
  template <typename SCHEMA>
  int check_zone_list_legal_with_tenant(share::schema::ObSchemaGetterGuard& schema_guard, const SCHEMA& schema,
      const share::schema::ObTenantSchema& tenant_schema, bool& zone_list_legal);
  int get_zones_from_zone_region_list(const common::ObRegion& region,
      const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      common::ObIArray<common::ObZone>& zones_in_region) const;
  int set_tablegroup_id(share::schema::ObTableSchema& table_schema);
  template <typename SCHEMA>
  int set_default_tablegroup_id(SCHEMA& schema);
  int create_table_in_trans(share::schema::ObTableSchema& table_schema, const int64_t frozen_version,
      const obrpc::ObCreateTableMode create_mode, const common::ObString* ddl_stmt_str, ObMySQLTransaction* sql_trans);
  int create_inner_expr_index_in_trans(const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObTableSchema& new_table_schema, common::ObIArray<share::schema::ObColumnSchemaV2*>& new_columns,
      share::schema::ObTableSchema& index_schema, const obrpc::ObCreateTableMode create_mode,
      const int64_t frozen_version, const common::ObString* ddl_stmt_str);
  /*
   * Check and set various options of modify tenant, among which the modifications of zone_list,
   *  locality and resource_pool are related to each other.
   * 1. Individual modification of the tenant's zone_list is not supported; the result of zone_list is calculated
   *  by locality and resource_pool.
   * 2. When modifying the locality, only support adding one, deleting one and modifying the locality of a zone.
   */
  int set_new_tenant_options(share::schema::ObSchemaGetterGuard& schema_guard, const obrpc::ObModifyTenantArg& arg,
      share::schema::ObTenantSchema& new_tenant_schema, const share::schema::ObTenantSchema& orig_tenant_schema,
      AlterLocalityOp& alter_locality_op);
  int set_raw_tenant_options(const obrpc::ObModifyTenantArg& arg, share::schema::ObTenantSchema& new_tenant_schema);
  int check_alter_tenant_locality_type(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTenantSchema& orig_tenant_schema, const share::schema::ObTenantSchema& new_tenant_schema,
      AlterLocalityType& alter_locality_type);
  int try_rollback_modify_tenant_locality(const obrpc::ObModifyTenantArg& arg,
      share::schema::ObTenantSchema& new_tenant_schema, const share::schema::ObTenantSchema& orig_tenant_schema,
      const common::ObIArray<common::ObZone>& zones_in_pool,
      const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list, AlterLocalityOp& alter_locality_op);
  int try_modify_tenant_locality(const obrpc::ObModifyTenantArg& arg, share::schema::ObTenantSchema& new_tenant_schema,
      const share::schema::ObTenantSchema& orig_tenant_schema, const common::ObIArray<common::ObZone>& zones_in_pool,
      const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list, AlterLocalityOp& alter_locality_op);

  int generate_single_deployment_tenant_sys_replica_num(
      const common::ObIArray<share::ObResourcePoolName>& pools, int64_t& replica_num);
  int parse_and_set_create_tenant_new_locality_options(share::schema::ObSchemaGetterGuard& schema_guard,
      share::schema::ObTenantSchema& schema, const common::ObIArray<share::ObResourcePoolName>& pools,
      const common::ObIArray<common::ObZone>& zones_list,
      const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list);

  template <typename T>
  int parse_and_set_new_locality_options(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id,
      T& schema, const common::ObIArray<common::ObZone>& zones_list,
      const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list, bool extend_empty_locality = true);
  template <typename T>
  int set_schema_zone_list(share::schema::ObSchemaGetterGuard& schema_guard, T& schema,
      const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list);
  int generate_zone_list_by_locality(const share::schema::ZoneLocalityIArray& zone_locality,
      const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      common::ObArray<common::ObZone>& zone_list) const;
  int check_and_modify_tenant_locality(const obrpc::ObModifyTenantArg& arg,
      share::schema::ObTenantSchema& new_tenant_schema, const share::schema::ObTenantSchema& orig_tenant_schema,
      const common::ObIArray<common::ObZone>& zones_in_pool,
      const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list);
  // When alter tenant modifies the locality, call the following function to get the zone_list of
  // the resource pool corresponding to the new tenant schema
  int get_new_tenant_pool_zone_list(const obrpc::ObModifyTenantArg& arg,
      const share::schema::ObTenantSchema& tenant_schema,
      common::ObIArray<share::ObResourcePoolName>& resource_pool_names, common::ObIArray<common::ObZone>& zones_in_pool,
      common::ObIArray<share::schema::ObZoneRegion>& zone_region_list);
  int get_tenant_pool_zone_list(
      const share::schema::ObTenantSchema& tenant_schema, common::ObIArray<common::ObZone>& zones_in_pool);
  int get_zones_of_pools(const common::ObIArray<share::ObResourcePoolName>& resource_pool_names,
      common::ObIArray<common::ObZone>& zones_in_pool);
  int set_new_database_options(
      const obrpc::ObAlterDatabaseArg& arg, share::schema::ObDatabaseSchema& new_database_schema);
  int check_tablegroup_in_single_database(
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema& table_schema);
  int set_new_table_options(const obrpc::ObAlterTableArg& alter_table_arg,
      const share::schema::AlterTableSchema& alter_table_schema, const share::schema::ObTenantSchema& tenant_schema,
      share::schema::ObTableSchema& new_table_schema, const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, bool& need_update_index_table,
      AlterLocalityOp& alter_locality_op);
  int set_raw_table_options(const share::schema::AlterTableSchema& alter_table_schema,
      share::schema::ObTableSchema& new_table_schema, share::schema::ObSchemaGetterGuard& schema_guard,
      bool& need_update_index_table);
  int check_alter_table_locality_type(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& orig_table_schema, const share::schema::ObTableSchema& new_table_schema,
      AlterLocalityType& alter_locality_type);
  int try_rollback_alter_table_locality(const obrpc::ObAlterTableArg& arg,
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTenantSchema& tenant_schema,
      share::schema::ObTableSchema& new_table_schema, const share::schema::ObTableSchema& orig_table_schema,
      const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      const obrpc::ObAlterTableArg& alter_table_arg, AlterLocalityOp& alter_locality_op);
  int try_alter_table_locality(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTenantSchema& tenant_schema, const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObTableSchema& new_table_schema,
      const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      const obrpc::ObAlterTableArg& alter_table_arg, AlterLocalityOp& alter_locality_op);
  virtual int create_tables_in_trans(const bool if_not_exist, const common::ObString& ddl_stmt_str,
      common::ObIArray<share::schema::ObTableSchema>& table_schemas, const int64_t frozen_version,
      obrpc::ObCreateTableMode create_mode, const uint64_t last_replay_log_id = 0);
  int print_view_expanded_definition(const share::schema::ObTableSchema& table_schema, ObString& ddl_stmt_str,
      common::ObIAllocator& allocator, share::schema::ObSchemaGetterGuard& schema_guard, bool if_not_exist);
  int alter_table_in_trans(obrpc::ObAlterTableArg& alter_table_arg, const int64_t frozen_version);
  int alter_table_sess_active_time_in_trans(obrpc::ObAlterTableArg& alter_table_arg, const int64_t frozen_version);
  int truncate_table_in_trans(const share::schema::ObTableSchema& orig_table_schema,
      common::ObIArray<share::schema::ObTableSchema>& table_schemas,
      const common::ObIArray<share::schema::ObRecycleObject>& index_recycle_objs,
      share::schema::ObSchemaGetterGuard& schema_guard, ObMySQLTransaction& trans, const common::ObString* ddl_stmt_str,
      const bool to_recyclebin, obrpc::ObCreateTableMode create_mode, const common::ObString& db_name);
  int restore_obj_priv_after_truncation(ObDDLOperator& ddl_operator, ObMySQLTransaction& trans,
      common::ObIArray<share::schema::ObObjPriv>& orig_obj_privs_ora, uint64_t new_table_id,
      const common::ObString& database_name, const common::ObString& table_name);
  int drop_aux_table_in_truncate(const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, ObMySQLTransaction& trans, ObDDLOperator& ddl_operator,
      const share::schema::ObTableType table_type, const bool to_recyclebin);
  int truncate_oracle_temp_table(const ObString& db_name, const ObString& tab_name, const uint64_t tenant_id,
      const uint64_t session_id, const int64_t sess_create_time);
  virtual int create_table_partitions(const share::schema::ObTableSchema& table_schema, const int64_t schema_version,
      obrpc::ObCreateTableMode create_mode, const int64_t frozen_version,
      const common::ObIArray<share::schema::ObTableSchema>& schemas, const uint64_t last_replay_log_id = 0);
  virtual int binding_table_partitions(const share::schema::ObTableSchema& table_schema,
      const share::schema::ObTablegroupSchema* tablegroup_schema, const int64_t schema_version,
      obrpc::ObCreateTableMode create_mode, const common::ObIArray<share::schema::ObTableSchema>& schemas);
  virtual int binding_add_partitions_for_add(const int64_t schema_version,
      const share::schema::ObTableSchema& alter_table_schema, const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObTablegroupSchema& tablegroup_schema, obrpc::ObCreateTableMode& create_mode);

  virtual int create_tablegroup_partitions_for_create(const share::schema::ObTablegroupSchema& tablegroup_schema,
      const common::ObIArray<ObPartitionAddr>& tablegroup_addr, const common::ObIArray<int64_t>& part_ids,
      const share::ObSimpleFrozenStatus& frozen_status, const obrpc::ObCreateTableMode create_mode);
  virtual int update_max_used_part_id_if_needed(const bool is_add, const share::schema::ObTableSchema& table_schema,
      const share::schema::ObTableSchema& inc_table_schema);
  virtual int update_max_used_subpart_id_if_needed(const bool is_add, const share::schema::ObTableSchema& table_schema,
      const share::schema::ObTableSchema& inc_table_schema);
  virtual int add_partitions_for_add(const int64_t schema_version, const share::schema::ObTableSchema& table_schema,
      const share::schema::ObTableSchema& inc_table_schema, const common::ObIArray<int64_t>& partition_ids,
      const obrpc::ObCreateTableMode create_mode);

  virtual int add_partitions_for_split(const int64_t schema_version, const share::schema::ObTableSchema& table_schema,
      const share::schema::ObTableSchema& inc_table_schema, const share::schema::ObTableSchema& new_schema,
      common::ObIArray<int64_t>& partition_ids);
  virtual int add_tablegroup_partitions_for_add(const share::schema::ObTablegroupSchema& tablegroup_schema,
      const share::schema::ObTablegroupSchema& inc_tablegroup_schema, const common::ObIArray<int64_t>& partition_ids,
      const obrpc::ObCreateTableMode create_mode);
  virtual int add_tablegroup_partitions_for_split(const share::schema::ObTablegroupSchema& tablegroup_schema,
      const share::schema::ObTablegroupSchema& new_tablegroup_schema, const common::ObIArray<int64_t>& partition_ids,
      const obrpc::ObCreateTableMode create_mode);
  bool is_zone_exist(const common::ObArray<common::ObZone>& zones, const common::ObZone& zone);
  virtual int create_partitions_for_create(const share::schema::ObTableSchema& table, const int64_t schema_version,
      const ObITablePartitionAddr& table_addrs, const common::ObIArray<share::schema::ObTableSchema>& schemas,
      const obrpc::ObCreateTableMode create_mode, const uint64_t last_replay_log_id = 0);
  virtual int create_partitions_for_add(const int64_t schema_version, const share::schema::ObTableSchema& table,
      const int64_t inc_partition_cnt, const ObITablePartitionAddr& table_addr,
      const common::ObIArray<int64_t>& partition_ids, const obrpc::ObCreateTableMode create_mode);
  virtual int flashback_tenant_in_trans(const share::schema::ObTenantSchema& tenant_schema,
      const ObString& new_tenant_name, share::schema::ObSchemaGetterGuard& schema_guard, const ObString& ddl_stmt_str);
  int get_tenant_object_name_with_origin_name_in_recyclebin(const ObString& origin_tenant_name, ObString& object_name,
      common::ObIAllocator* allocator, const bool is_flashback);
  int build_need_flush_role_array(share::schema::ObSchemaGetterGuard& schema_guard, uint64_t tenant_id,
      const share::schema::ObUserInfo* user_info, const obrpc::ObAlterUserProfileArg& arg, bool& need_flush,
      common::ObIArray<uint64_t>& role_id_array, common::ObIArray<uint64_t>& disable_flag_array);
  int drop_resource_pool_pre(const uint64_t tenant_id, ObIArray<share::ObResourcePoolName>& pool_names,
      const bool is_standby, ObMySQLTransaction& trans);
  int drop_resource_pool_final(
      const uint64_t tenant_id, const bool is_standby, ObIArray<share::ObResourcePoolName>& pool_names);

public:
  int construct_zone_region_list(common::ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      const common::ObIArray<common::ObZone>& zone_list);
  virtual int create_partitions_for_split(const int64_t schema_version, const share::schema::ObTableSchema& table,
      const int64_t inc_partition_cnt, const ObITablePartitionAddr& table_addr,
      const share::schema::ObTableSchema& new_schema, const common::ObIArray<int64_t>& partition_ids,
      const obrpc::ObCreateTableMode create_mode);

  int check_all_server_frozen_version(const int64_t frozen_version);

  int check_create_tenant_locality(const common::ObIArray<common::ObString>& pool_list,
      share::schema::ObTenantSchema& tenant_schema, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_tablegroup_locality_with_tenant(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTenantSchema& tenant_schema, const share::schema::ObTablegroupSchema& tablegroup);
  int check_table_locality_with_tenant(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTenantSchema& tenant_schema, const share::schema::ObSimpleTableSchemaV2& table);

  int check_create_tenant_replica_options(
      share::schema::ObTenantSchema& tenant_schema, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_create_table_replica_options(
      share::schema::ObSimpleTableSchemaV2& table_schema, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_create_database_replica_options(
      share::schema::ObDatabaseSchema& database_schema, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_create_tablegroup_replica_options(
      share::schema::ObTablegroupSchema& tablegroup_schema, share::schema::ObSchemaGetterGuard& schema_guard);

  bool is_sync_primary_ddl();
  int clear_partition_member_list(const int64_t max_schema_version, const int64_t tenant_id, const bool is_inner_table);

private:
  int do_modify_system_variable(
      uint64_t tenant_id, const share::schema::ObSysVarSchema& modify_var, share::schema::ObSysVarSchema& new_schema);

  virtual int prepare_create_partitions(ObPartitionCreator& creator, const uint64_t table_id,
      const int64_t schema_version, const int64_t partition_num, const int64_t partition_cnt,
      const int64_t paxos_replica_num, const int64_t non_paxos_replica_num,
      const common::ObIArray<int64_t>& partition_ids, const ObITablePartitionAddr& table_addr,
      const common::ObIArray<share::schema::ObTableSchema>& schemas, const bool is_bootstrap, const bool is_standby,
      obrpc::ObCreateTableMode create_mode, const int64_t restore, const share::ObSimpleFrozenStatus& frozen_status,
      const uint64_t last_replay_log_id = 0);

  virtual int prepare_create_partitions(ObPartitionCreator& creator, const share::schema::ObTableSchema& new_schema,
      const uint64_t table_id, const int64_t schema_version, const int64_t partition_num, const int64_t partition_cnt,
      const int64_t paxos_replica_num, const int64_t non_paxos_replica_num,
      const common::ObIArray<int64_t>& partition_ids, const ObITablePartitionAddr& table_addr,
      const common::ObIArray<share::schema::ObTableSchema>& schemas, const bool is_bootstrap, const bool is_standby,
      obrpc::ObCreateTableMode create_mode, const int64_t restore, const share::ObSimpleFrozenStatus& frozen_status,
      const uint64_t last_replay_log_id = 0);
  int fill_partition_member_list(const ObPartitionAddr& part_addr, const int64_t timestamp,
      const int64_t paxos_replica_count, obrpc::ObCreatePartitionArg& arg);
  //  int generate_partition_id(const share::schema::ObTableSchema &table,
  //                            const int64_t partition_idx,
  //                            int64_t &partition_id);
  int construct_create_partition_creator(const common::ObPartitionKey& pkey,
      const common::ObIArray<share::schema::ObTableSchema>& schemas, const int64_t paxos_replica_num,
      const int64_t non_paxos_replica_num, const int64_t schema_version, const int64_t last_replay_log_id,
      const obrpc::ObCreateTableMode create_mode, const int64_t restore, const ObPartitionAddr& partition_addr,
      const share::ObSplitPartition& split_info, const share::ObSimpleFrozenStatus& frozen_status,
      const bool is_standby, const bool is_bootstrap, ObPartitionCreator& creator);
  int fill_create_binding_partition_arg(const common::ObPartitionKey& pkey, const common::ObPGKey& pgkey,
      const common::ObZone& zone_, const common::ObIArray<share::schema::ObTableSchema>& schemas,
      const int64_t paxos_replica_num, const int64_t non_paxos_replica_num, const int64_t schema_version,
      const int64_t frozen_version, const int64_t frozen_timestamp, const obrpc::ObCreateTableMode create_mode,
      obrpc::ObCreatePartitionArg& arg);
  int fill_create_partition_arg(const uint64_t table_id, const int64_t partition_cnt, const int64_t paxos_replica_num,
      const int64_t non_paxos_replica_num, const int64_t partition_id, const ObReplicaAddr& replica_addr,
      const int64_t lease_start_ts, const bool is_bootstrap, const bool is_standby, const int64_t restore,
      const share::ObSimpleFrozenStatus& frozen_status, obrpc::ObCreatePartitionArg& arg);
  int set_flag_role(const bool initial_leader, const bool is_standby, const int64_t restore, const uint64_t table_id,
      common::ObRole& role);
  int fill_flag_replica(const uint64_t table_id, const int64_t partition_cnt, const int64_t partition_id,
      const obrpc::ObCreatePartitionArg& arg, const ObReplicaAddr& replica_addr,
      share::ObPartitionReplica& flag_replica);

  int try_modify_tenant_primary_zone_entity_count(common::ObMySQLTransaction& trans,
      share::schema::ObSchemaGetterGuard& schema_guard, const bool is_increment, const int64_t num,
      const uint64_t tenant_id);

  int statistic_tenant_primary_zone_entity_count(const uint64_t tenant_id);
  int get_part_by_id(share::schema::AlterTableSchema& table_schema, const int64_t part_id,
      const share::schema::ObPartition*& partition);

public:
  int get_tenant_primary_zone_entity_count(
      const uint64_t tenant_id, share::schema::ObSchemaGetterGuard& schema_guard, int64_t& pz_entity_count);
  int refresh_unit_replica_counter(const uint64 tenant_id);
  int check_restore_point_allow(const int64_t tenant_id, const int64_t table_id);
  int init_help_tables(share::schema::ObTenantSchema& tenant_schema);

private:
  // used only by create normal tenant
  int check_tenant_schema(const common::ObIArray<common::ObString>& pool_list,
      share::schema::ObTenantSchema& tenant_schema, share::schema::ObSchemaGetterGuard& schema_guard);
  int create_tenant_env(share::schema::ObSchemaGetterGuard& schema_guard, const obrpc::ObCreateTenantArg& arg,
      const common::ObRegion& region, share::schema::ObTenantSchema& tenant_schema, const int64_t frozen_version,
      const common::ObString* ddl_stmt_str = NULL);
  int init_tenant_sys_params(const obrpc::ObCreateTenantArg& arg, share::schema::ObTenantSchema& tenant_schema,
      share::schema::ObSysVariableSchema& sys_variable_schema);
  int build_obj_key(share::schema::ObObjPrivSortKey& obj_priv_key, /* out: obj sort key*/
      const uint64_t tenant_id, const uint64_t user_id, const common::ObString& db, const common::ObString& table,
      const share::ObRawObjPrivArray& obj_priv_array, share::schema::ObSchemaGetterGuard& schema_guard);

  enum GTSOperation {
    GTS_TURN_ON = 0,
    GTS_TURN_OFF,
    GTS_REMAIN_ON,
    GTS_REMAIN_OFF,
    GTS_OPERATION_INVALID,
  };
  int check_tenant_gts_operation(const share::schema::ObSysVariableSchema& old_sys_variable,
      const share::schema::ObSysVariableSchema& new_sys_variable, GTSOperation& gts_operation);

  // TODO(): for test create tenant
  int create_tenant_space(const uint64_t tenant_id);

  //----Functions for managing privileges----
  int check_user_exist(const share::schema::ObUserInfo& user_info) const;
  bool is_user_exist(const uint64_t tenant_id, const uint64_t user_id) const;
  int create_user(share::schema::ObUserInfo& user_info, uint64_t creator_id, uint64_t& user_id,
      const bool is_replay_schema, common::ObString& primary_zone);
  int create_user_in_trans(
      share::schema::ObUserInfo& user_info, uint64_t creator_id, uint64_t& user_id, common::ObString& primary_zone);
  int replay_alter_user(const share::schema::ObUserInfo& user_info);
  int set_passwd_in_trans(const uint64_t tenant_id, const uint64_t user_id, const common::ObString& new_passwd,
      const common::ObString* ddl_stmt_str);
  int set_max_connection_in_trans(const uint64_t tenant_id, const uint64_t user_id, const uint64_t max_connections_per_hour,
      const uint64_t max_user_connections, const ObString *ddl_stmt_str);
  int alter_user_require_in_trans(const uint64_t tenant_id, const uint64_t user_id, const obrpc::ObSetPasswdArg& arg,
      const common::ObString* ddl_stmt_str);
  int rename_user_in_trans(const uint64_t tenant_id, const uint64_t user_id, const obrpc::ObAccountArg& new_account,
      const common::ObString* ddl_stmt_str);
  int lock_user_in_trans(
      const uint64_t tenant_id, const uint64_t user_id, const bool locked, const common::ObString* ddl_stmt_str);
  int drop_user_in_trans(const uint64_t tenant_id, const uint64_t user_id, const ObString* ddl_stmt_str);

  //----End of Functions for managing privileges----
  template <typename SCHEMA>
  int set_schema_replica_num_options(
      SCHEMA& schema, ObLocalityDistribution& locality_dist, common::ObIArray<share::ObUnitInfo>& unit_infos);
  template <typename SCHEMA>
  int check_create_schema_replica_options(
      SCHEMA& schema, common::ObArray<common::ObZone>& zone_list, share::schema::ObSchemaGetterGuard& schema_guard);
  int try_check_tenant_turn_gts_on_condition(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTenantSchema& old_tenant_schema, const share::schema::ObTenantSchema& new_tenant_schema,
      const share::schema::ObSysVariableSchema& new_sys_variable);
  template <typename TABLE_SCHEMA>
  int check_table_primary_zone_gts_condition(const share::schema::ObTenantSchema& old_tenant_schema,
      const share::schema::ObTenantSchema& new_tenant_schema,
      const share::schema::ObSysVariableSchema& new_sys_variable, const TABLE_SCHEMA& table_schema,
      share::schema::ObSchemaGetterGuard& schema_guard);
  int check_database_primary_zone_gts_condition(const share::schema::ObTenantSchema& old_tenant_schema,
      const share::schema::ObTenantSchema& new_tenant_schema,
      const share::schema::ObSysVariableSchema& new_sys_variable,
      const share::schema::ObDatabaseSchema& database_schema, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_new_tablegroup_primary_zone_gts_condition(const share::schema::ObTenantSchema& old_tenant_schema,
      const share::schema::ObTenantSchema& new_tenant_schema,
      const share::schema::ObSysVariableSchema& new_sys_variable,
      const share::schema::ObTablegroupSchema& tablegroup_schema, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_tenant_primary_zone_gts_condition(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTenantSchema& new_tenant_schema,
      const share::schema::ObSysVariableSchema& new_sys_variable,
      const share::schema::ObSysVariableSchema& old_sys_variable);
  template <typename SCHEMA>
  int get_schema_primary_regions(const SCHEMA& schema, share::schema::ObSchemaGetterGuard& schema_guard,
      common::ObIArray<common::ObRegion>& primary_regions);
  template <typename SCHEMA>
  int check_and_set_primary_zone(SCHEMA& schema, const common::ObIArray<common::ObZone>& zone_list,
      share::schema::ObSchemaGetterGuard& schema_guard);
  template <typename SCHEMA>
  int check_empty_primary_zone_locality_condition(SCHEMA& schema, const common::ObIArray<common::ObZone>& zone_list,
      share::schema::ObSchemaGetterGuard& schema_guard);
  int check_schema_zone_list(common::ObArray<common::ObZone>& zone_list);
  template <typename SCHEMA>
  int check_alter_schema_replica_options(const bool alter_primary_zone, SCHEMA& new_schema, const SCHEMA& orig_schema,
      common::ObArray<common::ObZone>& zone_list, share::schema::ObSchemaGetterGuard& schema_guard);
  template <typename SCHEMA>
  int trim_and_set_primary_zone(SCHEMA& new_schema, const SCHEMA& orig_schema,
      const common::ObIArray<common::ObZone>& zone_list, share::schema::ObSchemaGetterGuard& schema_guard);
  int format_primary_zone_from_zone_score_array(
      common::ObIArray<share::schema::ObZoneScore>& zone_score_array, char* buf, int64_t buf_len);
  int try_modify_databases_attributes_in_tenant(const obrpc::ObModifyTenantArg& arg, ObDDLOperator& ddl_operator,
      ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard,
      share::schema::ObTenantSchema& tenant_schema, int64_t& sub_pz_count);
  int try_modify_tablegroups_attributes_in_tenant(const obrpc::ObModifyTenantArg& arg, ObDDLOperator& ddl_operator,
      ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard,
      share::schema::ObTenantSchema& tenant_schema, int64_t& sub_pz_count);
  int try_modify_tables_attributes_in_tenant(const obrpc::ObModifyTenantArg& arg, ObDDLOperator& ddl_operator,
      ObMySQLTransaction& trans, share::schema::ObSchemaGetterGuard& schema_guard,
      share::schema::ObTenantSchema& tenant_schema, int64_t& sub_pz_count);
  int check_alter_tenant_replica_options(const obrpc::ObModifyTenantArg& arg,
      share::schema::ObTenantSchema& new_tenant_schema, const share::schema::ObTenantSchema& orig_tenant_schema,
      share::schema::ObSchemaGetterGuard& schema_guard);
  int check_alter_table_replica_options(const obrpc::ObAlterTableArg& arg,
      share::schema::ObTableSchema& new_table_schema, const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObSchemaGetterGuard& schema_guard);
  int check_alter_database_replica_options(const obrpc::ObAlterDatabaseArg& arg,
      share::schema::ObDatabaseSchema& new_database_schema, const share::schema::ObDatabaseSchema& orig_database_schema,
      share::schema::ObSchemaGetterGuard& schema_guard);
  int check_alter_tablegroup_replica_options(const obrpc::ObAlterTablegroupArg& arg,
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTablegroupSchema& orig_tablegroup_schema,
      share::schema::ObTablegroupSchema& new_tablegroup_schema);
  int get_pools(
      const common::ObIArray<common::ObString>& pool_strs, common::ObIArray<share::ObResourcePoolName>& pools);

  typedef common::hash::ObPlacementHashSet<share::schema::ObIndexNameHashWrapper, common::OB_MAX_COLUMN_NUMBER>
      AddIndexNameHashSet;
  typedef common::hash::ObPlacementHashSet<share::schema::ObIndexNameHashWrapper, common::OB_MAX_COLUMN_NUMBER>
      DropIndexNameHashSet;
  typedef common::hash::ObPlacementHashSet<share::schema::ObIndexNameHashWrapper, common::OB_MAX_COLUMN_NUMBER>
      RenameIndexNameHashSet;
  typedef common::hash::ObPlacementHashSet<share::schema::ObIndexNameHashWrapper, common::OB_MAX_COLUMN_NUMBER>
      AlterIndexNameHashSet;
  int generate_index_name(obrpc::ObCreateIndexArg& create_index_arg,
      const share::schema::ObTableSchema& origin_table_schema, const AddIndexNameHashSet& add_index_name_set,
      const DropIndexNameHashSet& drop_index_name_set, share::schema::ObSchemaGetterGuard& schema_guard,
      common::ObArenaAllocator& allocator);
  int check_index_table_exist(const uint64_t tenant_id, const uint64_t database_id, const uint64_t table_id,
      const common::ObString& index_name, share::schema::ObSchemaGetterGuard& schema_guard, bool& is_exist);
  int drop_table_in_trans(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& table_schema, const bool is_rebuild_index, const bool is_index,
      const bool to_recyclebin, const common::ObString* ddl_stmt_str, ObMySQLTransaction* sql_trans,
      share::schema::DropTableIdHashSet* drop_table_set = NULL,
      bool* is_delay_delete = NULL /* Bring out the delayed delete behavior */);
  int drop_aux_table_in_drop_table(common::ObMySQLTransaction& trans, ObDDLOperator& ddl_operator,
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema& table_schema,
      const share::schema::ObTableType table_type, const bool to_recyclebin);
  int rebuild_table_schema_with_new_id(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObDatabaseSchema& new_database_schema, const common::ObString& new_table_name,
      const common::ObString& create_host, const share::schema::ObTableType table_type_,
      share::schema::ObSchemaService& schema_service, const uint64_t frozen_version,
      common::ObIArray<share::schema::ObTableSchema>& new_scheams, common::ObArenaAllocator& allocator);
  int check_enable_sys_table_ddl(
      const share::schema::ObTableSchema& table_schema, const share::schema::ObSchemaOperationType operation_type);

  int check_table_exists(const uint64_t tenant_id, const obrpc::ObTableItem& table_item,
      const share::schema::ObTableType expected_table_type, share::schema::ObSchemaGetterGuard& guard,
      const share::schema::ObTableSchema** table_schema);
  int construct_drop_sql(const obrpc::ObTableItem& table_item, const share::schema::ObTableType table_type,
      common::ObSqlString& sql, bool is_oracle_mode);
  int log_drop_warn_or_err_msg(
      const obrpc::ObTableItem table_item, bool if_exists, common::ObSqlString& err_table_list);
  int log_rebuild_warn_or_err_msg(const obrpc::ObRebuildIndexArg& arg, common::ObSqlString& err_table_list);
  int alter_outline_in_trans(const obrpc::ObAlterOutlineArg& alter_outline_arg);

  virtual int publish_schema(const uint64_t tenant_id, const common::ObAddrIArray& addrs);
  int get_zone_region(const common::ObZone& zone, const common::ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      common::ObRegion& region);
  int construct_region_list(
      common::ObIArray<common::ObRegion>& region_list, const common::ObIArray<common::ObZone>& zone_list);
  int modify_and_cal_resource_pool_diff(common::ObISQLClient& client, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTenantSchema& new_tenant_schema, const common::ObIArray<common::ObString>& new_pool_list,
      bool& grant, common::ObIArray<share::ObResourcePoolName>& diff_pools);
  int check_grant_pools_permitted(share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<share::ObResourcePoolName>& to_be_grant_pools,
      const share::schema::ObTenantSchema& tenant_schema, bool& is_permitted);
  int check_revoke_pools_permitted(share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<share::ObResourcePoolName>& new_pool_name_list,
      const share::schema::ObTenantSchema& tenant_schema, bool& is_permitted);
  int check_gts_tenant_revoke_pools_permitted(share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<share::ObResourcePoolName>& new_pool_name_list,
      const share::schema::ObTenantSchema& tenant_schema, bool& is_permitted);
  int check_normal_tenant_revoke_pools_permitted(share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<share::ObResourcePoolName>& new_pool_name_list,
      const share::schema::ObTenantSchema& tenant_schema, bool& is_permitted);
  int cal_resource_pool_list_diff(const common::ObIArray<share::ObResourcePoolName>& long_pool_name_list,
      const common::ObIArray<share::ObResourcePoolName>& short_pool_name_list,
      common::ObIArray<share::ObResourcePoolName>& diff_pools);
  int init_tenant_storage_format_version(share::schema::ObTenantSchema& tenant_schema);
  template <typename SCHEMA, typename ALTER_SCHEMA>
  int complete_split_partition(const SCHEMA& orig_schema, ALTER_SCHEMA& alter_schema, const bool is_split = false);
  template <typename SCHEMA, typename ALTER_SCHEMA>
  int check_partition_name_valid(
      const SCHEMA& orig_schema, const ALTER_SCHEMA& alter_schema, const ObString part_name, bool& valid);
  template <typename SCHEMA>
  int check_split_table_partition_valid(const SCHEMA& schema);
  int check_index_valid_for_alter_partition(const share::schema::ObTableSchema& orig_table_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, const bool is_drop_truncate_and_alter_index,
      const bool is_split);
  int check_new_partition_key_valid(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::AlterTableSchema& alter_table_schema, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_unique_index_cover_partition_column(
      share::schema::ObTableSchema& table_schema, const share::schema::ObTableSchema& index_schema);
  int check_alter_partitions(
      const share::schema::ObTableSchema& orig_table_schema, const obrpc::ObAlterTableArg& alter_table_arg);
  int check_alter_drop_partitions(const share::schema::ObTableSchema& orig_table_schema,
      const obrpc::ObAlterTableArg& alter_table_arg, const bool is_truncate);
  int check_alter_drop_subpartitions(
      const share::schema::ObTableSchema& orig_table_schema, const obrpc::ObAlterTableArg& alter_table_arg);
  int check_alter_add_partitions(
      const share::schema::ObTableSchema& orig_table_schema, const obrpc::ObAlterTableArg& alter_table_arg);
  int check_alter_add_subpartitions(
      const share::schema::ObTableSchema& orig_table_schema, const obrpc::ObAlterTableArg& alter_table_arg);
  int check_alter_split_partitions(
      const share::schema::ObTableSchema& orig_table_schema, const obrpc::ObAlterTableArg& alter_table_arg);
  int check_alter_partition_table(const share::schema::ObTableSchema& orig_table_schema,
      const obrpc::ObAlterTableArg& alter_table_arg, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_add_list_partition(const share::schema::ObPartitionSchema& orig_part,
      const share::schema::ObPartitionSchema& new_part, const int64_t split_part_id = OB_INVALID_PARTITION_ID);
  int check_add_list_subpartition(
      const share::schema::ObPartition& orig_part, const share::schema::ObPartition& new_part);
  int check_split_list_partition_match(const share::schema::ObPartitionSchema& new_part,
      const share::schema::ObPartitionSchema& orig_part, const int64_t split_part_idx);
  int is_list_values_equal(const common::ObIArray<common::ObNewRow>& fir_values,
      const common::ObIArray<common::ObNewRow>& sed_values, bool& equal);
  int adjust_partition_id_to_continuous(share::schema::ObTableSchema& table);
  int update_partition_cnt_within_partition_table(const share::schema::ObTableSchema& orig_table_schema);
  int normalize_tablegroup_option(share::schema::ObTablegroupSchema& tablegroup_schema);
  int update_sys_variables(const common::ObIArray<obrpc::ObSysVarIdValue>& sys_var_list,
      const share::schema::ObSysVariableSchema& old_sys_variable, share::schema::ObSysVariableSchema& new_sys_variable,
      bool& value_changed);
  int try_rollback_alter_tablegroup_locality(const obrpc::ObAlterTablegroupArg& arg,
      share::schema::ObTablegroupSchema& new_schema, const share::schema::ObTablegroupSchema& orig_schema,
      AlterLocalityOp& alter_locality_op);
  int try_alter_tablegroup_locality(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTenantSchema& tenant_schema, const share::schema::ObTablegroupSchema& orig_tablegroup,
      share::schema::ObTablegroupSchema& new_tablegroup, const ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      AlterLocalityOp& alter_locality_op);
  int check_alter_tablegroup_locality_type(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTablegroupSchema& orig_tablegroup, const share::schema::ObTablegroupSchema& new_tablegroup,
      AlterLocalityType& alter_locality_type);
  int try_check_and_set_table_schema_in_tablegroup(
      share::schema::ObSchemaGetterGuard& schema_guard, share::schema::ObTableSchema& schema);
  int check_tablegroup_alter_locality(const obrpc::ObAlterTablegroupArg& arg,
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTablegroupSchema& orig_tablegroup,
      share::schema::ObTablegroupSchema& new_tablegroup, AlterLocalityOp& alter_locality_op);
  int set_new_tablegroup_options(common::ObMySQLTransaction& trans, const obrpc::ObAlterTablegroupArg& arg,
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTablegroupSchema& orig_tablegroup,
      share::schema::ObTablegroupSchema& new_tablegroup);
  template <typename Schema>
  int calc_schema_replica_num(share::schema::ObSchemaGetterGuard& schema_guard, const Schema& schema,
      int64_t& paxos_replica_num, int64_t& non_paxos_replica_num, const obrpc::ObCreateTableMode create_mode);
  int try_compensate_readonly_all_server(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObPartitionSchema& schema, share::schema::ZoneLocalityIArray& zone_locality);

  virtual int reconstruct_table_schema_from_recyclebin(share::schema::ObTableSchema& index_table_schema,
      const share::schema::ObRecycleObject& recycle_obj, share::schema::ObSchemaGetterGuard& guard);
  int get_database_id(share::schema::ObSchemaGetterGuard& schema_guard, uint64_t tenant_id,
      const common::ObString& database_name, uint64_t& database_id);
  //  virtual int sync_readonly_sys_param(
  //      share::schema::ObSysVariableSchema &new_sys_variable,
  //      share::schema::ObTenantSchema &new_tenant_schema,
  //      const share::schema::ObTenantSchema &ori_tenant_schema);
  virtual int init_system_variables(const obrpc::ObCreateTenantArg& arg, share::schema::ObTenantSchema& tenant_schema,
      const share::schema::ObSysVariableSchema& sys_variable, share::schema::ObSysParam* sys_params, int64_t count);
  int update_sys_tenant_sys_var(const share::schema::ObSysVariableSchema& sys_variable,
      share::schema::ObSysParam* sys_params, int64_t params_capacity);
  int get_is_standby_cluster(bool& is_standby) const;
  int check_can_alter_column(const int64_t tenant_id, const share::schema::AlterTableSchema& alter_table_schema,
      const share::schema::ObTableSchema& orig_table_schema);
  bool add_sys_table_index(const uint64_t table_id, common::ObIArray<share::schema::ObTableSchema>& schemas);

private:
  // gts tenant associated
  int modify_gts_tenant(const obrpc::ObModifyTenantArg& arg, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTenantSchema& tenant_schema);
  int get_partition_by_subpart_name(const share::schema::ObTableSchema& orig_table_schema,
      const share::schema::ObSubPartition& subpart_name, const share::schema::ObPartition*& part,
      const share::schema::ObSubPartition*& subpart);
  int gen_inc_table_schema_for_drop_subpart(
      const share::schema::ObTableSchema& orig_table_schema, share::schema::AlterTableSchema& inc_table_schema);
  // get gts value, return OB_STATE_NOT_MATCH when is not external consistent
  int get_tenant_external_consistent_ts(const int64_t tenant_id, int64_t& ts);
  int gen_inc_table_schema_for_trun_subpart(
      const share::schema::ObTableSchema& orig_table_schema, share::schema::AlterTableSchema& inc_table_schema);
  int check_table_pk(const share::schema::ObTableSchema& orig_table_schema);

private:
  bool inited_;
  volatile bool stopped_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  obrpc::ObCommonRpcProxy* common_rpc_;
  common::ObMySQLProxy* sql_proxy_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::ObPartitionTableOperator* pt_operator_;
  // TODO(): used to choose partition server, use load balancer finnally
  ObServerManager* server_mgr_;
  ObZoneManager* zone_mgr_;
  ObUnitManager* unit_mgr_;
  ObRootBalancer* root_balancer_;
  ObFreezeInfoManager* freeze_info_manager_;
  ObSnapshotInfoManager* snapshot_mgr_;
  ObRebalanceTaskMgr* task_mgr_;
  mutable common::SpinRWLock pz_entity_cnt_lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDDLService);
};

class ObDDLSQLTransaction : public common::ObMySQLTransaction {
public:
  ObDDLSQLTransaction(share::schema::ObMultiVersionSchemaService* schema_service)
      : schema_service_(schema_service),
        tenant_id_(OB_INVALID_ID),
        start_operation_schema_version_(OB_INVALID_VERSION),
        start_operation_tenant_id_(OB_INVALID_TENANT_ID)
  {}
  virtual ~ObDDLSQLTransaction(){};

public:
  virtual int start(ObISQLClient* proxy, bool with_snapshot = false) override;
  // If you commit the transaction, you need to write a line in ddl_operation once, the mark of end
  virtual int end(const bool commit) override;
  // When the transaction ends, the schema will be written once, and the last tenant_id will be taken by default
  // But there is no way to deal with cross-tenant transactions, add an interface,
  // it can modify the location of the generated schema
  // After the end, tenant_id needs to be cleared for both success and failure
  void set_end_tenant_id(const int64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  int64_t get_start_schema_version() const
  {
    return start_operation_schema_version_;
  }

private:
  int try_lock_all_ddl_operation(common::ObMySQLTransaction& trans, const uint64_t tenant_id);

private:
  share::schema::ObMultiVersionSchemaService* schema_service_;
  int64_t tenant_id_;
  // Filter out only one 1503 DDL transaction to prevent the schema from being invalidly pushed up
  int64_t start_operation_schema_version_;
  uint64_t start_operation_tenant_id_;
};
// Fill in the partition name and the high values of the last partition
template <typename SCHEMA, typename ALTER_SCHEMA>
int ObDDLService::complete_split_partition(const SCHEMA& orig_schema, ALTER_SCHEMA& alter_schema, const bool is_split)
{
  int ret = OB_SUCCESS;
  const int64_t part_num = alter_schema.get_partition_num();
  share::schema::ObPartition** part_array = alter_schema.get_part_array();
  if (OB_ISNULL(part_array)) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(WARN, "part_array is null", K(ret), K(part_array));
  }
  // Supplement the default partition name p+OB_MAX_PARTITION_NUM_MYSQL, accumulate after judging duplicates
  // Only Oracle mode will go to this logic
  int64_t max_part_id = OB_MAX_PARTITION_NUM_MYSQL + orig_schema.get_part_option().get_max_used_part_id();
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
    }  // end while
  }    // end for
  if (is_split) {
    const int64_t orig_part_num = orig_schema.get_partition_num();
    share::schema::ObPartition** orig_part_array = orig_schema.get_part_array();
    int64_t index = -1;
    // The divided partition exists
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(orig_part_array)) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "part_array is null", K(ret), K(orig_part_array));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_part_num; ++i) {
      index++;
      if (OB_ISNULL(orig_part_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "part is null", K(ret), KP(orig_part_array[i]), K(index), K(orig_schema));
      } else if (ObCharset::case_insensitive_equal(
                     orig_part_array[i]->get_part_name(), alter_schema.get_split_partition_name())) {
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (index >= orig_part_num) {
      ret = OB_ERR_DROP_PARTITION_NON_EXISTENT;
      RS_LOG(WARN,
          "partition to be split not exist",
          K(ret),
          "split partition name",
          alter_schema.get_split_partition_name());
      LOG_USER_ERROR(OB_ERR_DROP_PARTITION_NON_EXISTENT, "split");
    }
    if (OB_FAIL(ret)) {
    } else if (orig_schema.is_range_part()) {
      share::schema::ObPartition* last_partition = part_array[part_num - 1];
      const ObRowkey* rowkey_last = &orig_schema.get_part_array()[index]->get_high_bound_val();
      // The high value of the last partition may be empty, put the high value of the split partition into it
      if (OB_NOT_NULL(last_partition) && OB_NOT_NULL(rowkey_last) &&
          OB_ISNULL(last_partition->get_high_bound_val().ptr())) {
        if (OB_FAIL(last_partition->set_high_bound_val(*rowkey_last))) {
          RS_LOG(WARN, "failed to set high boundary", K(ret));
        } else {
          alter_schema.get_part_option().set_part_num(alter_schema.get_partition_num());
        }
      }
    } else if (orig_schema.is_list_part()) {
      // The value of the list partition is empty, the last partition is not empty,
      // and the number of values of the list partition is 0
      share::schema::ObPartition* last_partition = part_array[part_num - 1];
      if (OB_NOT_NULL(last_partition) && 0 == last_partition->get_list_row_values().count()) {
        common::hash::ObHashSet<common::ObRowkey> list_row_map;
        if (OB_FAIL(list_row_map.create(
                OB_MAX_VERSION_COUNT, ObModIds::OB_PARTITION_SPLIT, ObModIds::OB_PARTITION_SPLIT))) {
          RS_LOG(WARN, "failed to create list value", K(ret));
        } else {
          common::ObRowkey row_key;
          for (int64_t i = 0; OB_SUCC(ret) && i < part_num - 1; ++i) {
            if (OB_ISNULL(part_array[i])) {
              ret = OB_ERR_UNEXPECTED;
              RS_LOG(WARN, "partition is null", K(ret), K(i), K(part_num));
            } else {
              const ObIArray<common::ObNewRow>* list_values = &(part_array[i]->get_list_row_values());
              if (OB_ISNULL(list_values)) {
                ret = OB_ERR_UNEXPECTED;
                RS_LOG(WARN, "list row value is null", K(ret), K(list_values));
              }
              // In the list partition, there may only be one default partition and it will appear in the last
              // partition, so it appears in the middle partition The default partition is wrong
              for (int64_t j = 0; OB_SUCC(ret) && j < list_values->count(); ++j) {
                const common::ObNewRow* new_row = &(list_values->at(j));
                if (1 == new_row->get_count() && new_row->get_cell(0).is_max_value()) {
                  ret = OB_ERR_UNEXPECTED;
                  RS_LOG(WARN, "DEFAULT partition must be last partition specified", K(ret), K(orig_schema));
                } else {
                  row_key.reset();
                  row_key.assign(new_row->cells_, new_row->get_count());
                  if (OB_FAIL(list_row_map.set_refactored(row_key))) {
                    RS_LOG(WARN, "failed to insert hash map", K(ret), K(row_key));
                  }
                }
              }
            }
          }  // end for
          const ObIArray<common::ObNewRow>* orig_list_value =
              &(orig_schema.get_part_array()[index]->get_list_row_values());
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(orig_list_value)) {
            ret = OB_ERR_UNEXPECTED;
            RS_LOG(WARN, "list values is null", K(ret), K(orig_list_value));
          } else {
            int64_t count = orig_list_value->count();
            for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
              const common::ObNewRow* new_row = &(orig_list_value->at(i));
              row_key.reset();
              row_key.assign(new_row->cells_, new_row->get_count());
              if (OB_HASH_EXIST == (ret = list_row_map.exist_refactored(row_key))) {
                ret = OB_SUCCESS;
              } else if (OB_HASH_NOT_EXIST == ret) {
                if (OB_FAIL(last_partition->add_list_row(*new_row))) {
                  RS_LOG(WARN, "failed to add list row", K(ret), K(i), K(orig_schema));
                }
              } else {
                RS_LOG(WARN, "failed to get refactored", K(ret), K(row_key));
              }
            }  // end for find row not in hashmap
          }
        }
      }
    }
  }
  return ret;
}
// Check whether the partition name is in conflict with the existing partition,
// and whether the newly added partition conflicts
template <typename SCHEMA, typename ALTER_SCHEMA>
int ObDDLService::check_partition_name_valid(
    const SCHEMA& orig_schema, const ALTER_SCHEMA& alter_schema, const ObString part_name, bool& valid)
{
  int ret = OB_SUCCESS;
  share::schema::ObPartition** part_array = alter_schema.get_part_array();
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
      } else if (ObCharset::case_insensitive_equal(part_name, part_array[j]->get_part_name())) {
        valid = false;
        break;
      }
    }  // end for
    // Determine whether the partition name is conflicting with the existing partition
    if (orig_schema.get_part_level() != share::schema::PARTITION_LEVEL_ZERO) {
      const int64_t orig_part_num = orig_schema.get_partition_num();
      share::schema::ObPartition** orig_part_array = orig_schema.get_part_array();
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(orig_part_array)) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "part_array is null", K(ret), K(orig_part_array));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < orig_part_num; ++i) {
        if (OB_ISNULL(orig_part_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          RS_LOG(WARN, "part is null", K(ret), KP(orig_part_array[i]), K(orig_schema));
        } else if (ObCharset::case_insensitive_equal(orig_part_array[i]->get_part_name(), part_name)) {
          valid = false;
          RS_LOG(INFO, "partition name not valid", K(part_name), K(orig_part_array));
          break;
        }
      }
    }
  }
  return ret;
}
template <typename SCHEMA>
int ObDDLService::check_split_table_partition_valid(const SCHEMA& schema)
{
  int ret = OB_SUCCESS;
  if (schema.is_range_part() || schema.is_list_part()) {
    const int64_t part_num = schema.get_part_option().get_part_num();
    share::schema::ObPartition** part_array = schema.get_part_array();
    if (OB_ISNULL(part_array) || OB_ISNULL(part_array[part_num - 1])) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "part_array is null", K(ret), K(part_array));
    } else {
      share::schema::ObPartition* part = part_array[part_num - 1];
      if (schema.is_range_part()) {
        int64_t count = part->get_high_bound_val().length();
        for (int64_t idx = 0; idx < count && OB_SUCC(ret); ++idx) {
          if (!part->get_high_bound_val().ptr()[idx].is_max_value()) {
            ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
            RS_LOG(WARN, "may lack row if not have max_value", K(ret), K(idx), K(count));
            LOG_USER_ERROR(OB_NO_PARTITION_FOR_GIVEN_VALUE);
          }
        }
      } else {
        if (1 != part->get_list_row_values().count() || 1 != part->get_list_row_values().at(0).get_count() ||
            !part->get_list_row_values().at(0).get_cell(0).is_max_value()) {
          ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
          RS_LOG(WARN, "may lack row if not have max_value", K(ret));
          LOG_USER_ERROR(OB_NO_PARTITION_FOR_GIVEN_VALUE);
        }
      }
    }
  }
  return ret;
}
}  // end namespace rootserver
}  // namespace oceanbase
#endif  // _OCEANBASE_ROOTSERVER_OB_DDL_SERVICE_H_
