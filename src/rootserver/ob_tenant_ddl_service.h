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

#ifndef _OCEANBASE_ROOTSERVER_OB_TENANT_DDL_SERVICE_H_
#define _OCEANBASE_ROOTSERVER_OB_TENANT_DDL_SERVICE_H_ 1

#include "share/ob_rpc_struct.h"
#include "src/share/ls/ob_ls_table_operator.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_zone_manager.h"

namespace oceanbase
{
namespace common
{
using ObAddrIArray = ObIArray<ObAddr>;
using ObAddrArray = ObSEArray<ObAddr, 3>;
}
namespace share
{
namespace schema
{
class ObDDLTransController;
}
}
namespace rootserver
{
class ObDDLService;
struct ObSysStat
{
  struct Item;
  typedef common::ObDList<Item> ItemList;

  struct Item : public common::ObDLinkBase<Item>
  {
    Item() : name_(NULL), value_(), info_(NULL) {}
    Item(ItemList &list, const char *name, const char *info);

    TO_STRING_KV("name", common::ObString(name_), K_(value), "info", common::ObString(info_));
    const char *name_;
    common::ObObj value_;
    const char *info_;
  };

  ObSysStat();

  // set values after bootstrap
  int set_initial_values(const uint64_t tenant_id);

  TO_STRING_KV(K_(item_list));

  ItemList item_list_;

  // only root tenant own
  Item ob_max_used_tenant_id_;
  Item ob_max_used_unit_config_id_;
  Item ob_max_used_resource_pool_id_;
  Item ob_max_used_unit_id_;
  Item ob_max_used_server_id_;
  Item ob_max_used_ddl_task_id_;
  Item ob_max_used_unit_group_id_;

  // all tenant own
  Item ob_max_used_normal_rowid_table_tablet_id_;
  Item ob_max_used_extended_rowid_table_tablet_id_;
  Item ob_max_used_ls_id_;
  Item ob_max_used_ls_group_id_;
  Item ob_max_used_sys_pl_object_id_;
  Item ob_max_used_object_id_;
  Item ob_max_used_rewrite_rule_version_;
};
class ObTenantDDLService
{
public:
  ObTenantDDLService() : inited_(false), stopped_(false), unit_mgr_(NULL), ddl_service_(NULL),
  rpc_proxy_(NULL), common_rpc_(NULL), schema_service_(NULL), lst_operator_(NULL),
  ddl_trans_controller_(NULL), zone_mgr_(NULL) {}
  // the entry for create_tenant, used for other modules such as restore tenant and create tenant stmt
  // this function may not run in rs, so it should keep static to avoid use ObTenantDDLService members
  static int schedule_create_tenant(const obrpc::ObCreateTenantArg &arg, obrpc::UInt64 &tenant_id);
  // create tenant schema in sys tenant
  // this function should run in ddl thread
  int create_tenant(const obrpc::ObCreateTenantArg &arg,
      obrpc::ObCreateTenantSchemaResult &result);
  // create tenant schemas
  int create_normal_tenant(obrpc::ObParallelCreateNormalTenantArg &arg);
  virtual int modify_tenant(const obrpc::ObModifyTenantArg &arg);

  virtual int create_sys_tenant(const obrpc::ObCreateTenantArg &arg,
                                share::schema::ObTenantSchema &tenant_schema);
  virtual int create_tenant_end(const uint64_t tenant_id);
  virtual int commit_alter_tenant_locality(const rootserver::ObCommitAlterTenantLocalityArg &arg);
  virtual int drop_tenant(const obrpc::ObDropTenantArg &arg);
  virtual int flashback_tenant(const obrpc::ObFlashBackTenantArg &arg);
  virtual int purge_tenant(const obrpc::ObPurgeTenantArg &arg);
  virtual int lock_tenant(const common::ObString &tenant_name, const bool is_lock);
  virtual int flashback_tenant_in_trans(const share::schema::ObTenantSchema &tenant_schema,
                                        const ObString &new_tenant_name,
                                        share::schema::ObSchemaGetterGuard &schema_guard,
                                        const ObString &ddl_stmt_str);
  void stop() { stopped_ = true; }
  void restart() { stopped_ = false; }
  bool is_stopped() { return stopped_; }
  int force_set_locality(
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTenantSchema &new_tenant);

  int init(
      ObUnitManager &unit_mgr_,
      ObDDLService &ddl_service,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      obrpc::ObCommonRpcProxy &common_rpc,
      common::ObMySQLProxy &sql_proxy,
      share::schema::ObMultiVersionSchemaService &schema_service,
      share::ObLSTableOperator &lst_operator,
      ObZoneManager &zone_mgr);

  enum AlterLocalityOp
  {
    ALTER_LOCALITY = 0,
    ROLLBACK_ALTER_LOCALITY,
    NOP_LOCALITY_OP,
    ALTER_LOCALITY_OP_INVALID,
  };

  enum AlterLocalityType
  {
    TO_NEW_LOCALITY = 0,
    ROLLBACK_LOCALITY,
    LOCALITY_NOT_CHANGED,
    ALTER_LOCALITY_INVALID,
  };

public:
  int construct_zone_region_list(
      common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      const common::ObIArray<common::ObZone> &zone_list);
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
  template <typename SCHEMA>
  int check_primary_zone_locality_condition(
       const SCHEMA &schema,
       const ObIArray<common::ObZone> &zone_list,
       const ObIArray<share::schema::ObZoneRegion> &zone_region_list,
       share::schema::ObSchemaGetterGuard &schema_guard);

  static int gen_tenant_init_config(
             const uint64_t tenant_id,
             const uint64_t compatible_version,
             common::ObConfigPairs &tenant_config);
  static int notify_init_tenant_config(
      obrpc::ObSrvRpcProxy &rpc_proxy,
      const common::ObIArray<common::ObConfigPairs> &init_configs,
      const common::ObIArray<common::ObAddr> &addrs);

  static int generate_drop_tenant_arg(
      const uint64_t tenant_id,
      const ObString &tenant_name,
      ObSqlString &ddl_stmt,
      obrpc::ObDropTenantArg &arg);

  static int get_pools(const common::ObIArray<common::ObString> &pool_strs,
                common::ObIArray<share::ObResourcePoolName> &pools);
  static int get_tenant_zone_priority(const ObTenantSchema &tenant_schema,
      ObZone &primary_zone,
      ObSqlString &zone_priority);

  static int replace_sys_stat(const uint64_t tenant_id,
      ObSysStat &sys_stat,
      common::ObISQLClient &trans);

private:
  int insert_tenant_merge_info_(const share::schema::ObSchemaOperationType op,
                               const share::schema::ObTenantSchema &tenant_schema,
                               common::ObMySQLTransaction &trans);
  int refresh_creating_tenant_schema_(const ObTenantSchema &tenant_schema);
  int get_tenant_schema_(
      const obrpc::ObParallelCreateNormalTenantArg &arg,
      ObTenantSchema &tenant_schema);
  int set_tenant_compatibility_(const obrpc::ObCreateTenantArg &arg, ObTenantSchema &tenant_schema);
  int init_tenant_env_before_schema_(
      const ObTenantSchema &tenant_schema,
      const obrpc::ObParallelCreateNormalTenantArg &arg);
  int init_tenant_env_after_schema_(
      const ObTenantSchema &tenant_schema,
      const obrpc::ObParallelCreateNormalTenantArg &arg);
  int init_user_tenant_env_(const uint64_t tenant_id, ObMySQLTransaction &trans);
  int init_meta_tenant_env_(
      const ObTenantSchema &tenant_schema,
      const obrpc::ObCreateTenantArg &arg,
      const common::ObIArray<common::ObConfigPairs> &init_configs,
      ObMySQLTransaction &trans);
  int set_sys_ls_(const uint64_t tenant_id, ObMySQLTransaction &trans);
  int fill_user_sys_ls_info_(
      const ObTenantSchema &meta_tenant_schema,
      ObMySQLTransaction &trans);
  int init_tenant_configs_(const uint64_t tenant_id,
      const common::ObIArray<common::ObConfigPairs> &init_configs,
      common::ObMySQLTransaction &trans);
  int init_tenant_config_(
      const uint64_t tenant_id,
      const common::ObConfigPairs &tenant_config,
      common::ObMySQLTransaction &trans);
  int init_tenant_config_from_seed_(
      const uint64_t tenant_id,
      common::ObMySQLTransaction &trans);

  int init_tenant_sys_stats_(const uint64_t tenant_id,
      common::ObMySQLTransaction &trans);

  int broadcast_tenant_init_config_(const uint64_t tenant_id);

  int insert_restore_or_clone_tenant_job_(
      const uint64_t tenant_id,
      const ObString &tenant_name,
      const share::ObTenantRole &tenant_role,
      const uint64_t source_tenant_id,
      ObMySQLTransaction &trans);

  int set_log_restore_source_(
      const uint64_t tenant_id,
      const common::ObString &log_restore_source,
      ObMySQLTransaction &trans);

  int init_tenant_global_stat_(
      const uint64_t tenant_id,
      const common::ObIArray<common::ObConfigPairs> &init_configs,
      ObMySQLTransaction &trans);

  int get_ls_member_list_for_creating_tenant_(
      const uint64_t tenant_id,
      const int64_t ls_id,
      ObAddr &leader,
      common::ObIArray<ObAddr> &addrs);

private:
  int check_inner_stat();
  int create_sys_table_schemas(
      ObDDLOperator &ddl_operator,
      ObMySQLTransaction &trans,
      common::ObIArray<share::schema::ObTableSchema> &tables);
  template <typename SCHEMA>
  int extract_first_primary_zone_array(
      const SCHEMA &schema,
      const ObIArray<common::ObZone> &zone_list,
      ObIArray<common::ObZone> &first_primary_zone_array);
  template <typename SCHEMA>
  int check_pools_unit_num_enough_for_schema_locality(
      const common::ObIArray<share::ObResourcePoolName> &pools,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const SCHEMA &schema);
  template<typename SCHEMA>
  int trim_and_set_primary_zone(
      SCHEMA &new_schema,
      const SCHEMA &orig_schema,
      const common::ObIArray<common::ObZone> &zone_list,
      share::schema::ObSchemaGetterGuard &schema_guard);
  template<typename SCHEMA>
  int check_create_schema_replica_options(
      SCHEMA &schema,
      common::ObArray<common::ObZone> &zone_list,
      share::schema::ObSchemaGetterGuard &schema_guard);
  template<typename SCHEMA>
  int check_empty_primary_zone_locality_condition(
      SCHEMA &schema,
      const common::ObIArray<common::ObZone> &zone_list,
      share::schema::ObSchemaGetterGuard &schema_guard);
  template<typename SCHEMA>
  int check_and_set_primary_zone(
      SCHEMA &schema,
      const common::ObIArray<common::ObZone> &zone_list,
      share::schema::ObSchemaGetterGuard &schema_guard);
  template<typename SCHEMA>
  int get_schema_primary_regions(
     const SCHEMA &schema,
     share::schema::ObSchemaGetterGuard &schema_guard,
     common::ObIArray<common::ObRegion> &primary_regions);
  template<typename SCHEMA>
  int set_schema_replica_num_options(
       SCHEMA &schema,
       ObLocalityDistribution &locality_dist,
       common::ObIArray<share::ObUnitInfo> &unit_infos);
  template<typename T>
  int set_schema_zone_list(
       share::schema::ObSchemaGetterGuard &schema_guard,
       T &schema,
       const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list);

  int set_tenant_schema_charset_and_collation(
      ObTenantSchema &tennat_schema,
      const obrpc::ObCreateTenantArg &create_tenant_arg);

  // check whether we can create the tenant
  int create_tenant_check_(const obrpc::ObCreateTenantArg &arg,
      bool &need_create,
      share::schema::ObSchemaGetterGuard &schema_guard);

  int check_alter_tenant_replica_options(
      const obrpc::ObModifyTenantArg &arg,
      share::schema::ObTenantSchema &new_tenant_schema,
      const share::schema::ObTenantSchema &orig_tenant_schema,
      share::schema::ObSchemaGetterGuard &schema_guard);
  int check_alter_schema_replica_options(
      const bool alter_primary_zone,
      share::schema::ObTenantSchema &new_schema,
      const share::schema::ObTenantSchema &orig_schema,
      common::ObArray<common::ObZone> &zone_list,
      share::schema::ObSchemaGetterGuard &schema_guard);
  int check_alter_tenant_locality_type(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTenantSchema &orig_tenant_schema,
      const share::schema::ObTenantSchema &new_tenant_schema,
      AlterLocalityType &alter_locality_type);
  int check_and_modify_tenant_locality(
      const obrpc::ObModifyTenantArg &arg,
      share::schema::ObTenantSchema &new_tenant_schema,
      const share::schema::ObTenantSchema &orig_tenant_schema,
      const common::ObIArray<common::ObZone> &zones_in_pool,
      const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list);

  int check_alter_tenant_when_rebalance_is_disabled_(
      const share::schema::ObTenantSchema &orig_tenant_schema,
      const share::schema::ObTenantSchema &new_tenant_schema);
  int check_revoke_pools_permitted(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
      const share::schema::ObTenantSchema &tenant_schema,
      bool &is_permitted);
  int check_grant_pools_permitted(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<share::ObResourcePoolName> &to_be_grant_pools,
      const share::schema::ObTenantSchema &tenant_schema,
      bool &is_permitted);
  int check_normal_tenant_revoke_pools_permitted(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
      const share::schema::ObTenantSchema &tenant_schema,
      bool &is_permitted);

  int check_create_tenant_schema(
      const ObIArray<ObString> &pool_list,
      ObTenantSchema &tenant_schema,
      share::schema::ObSchemaGetterGuard &schema_guard);

  int check_create_tenant_replica_options(
      share::schema::ObTenantSchema &tenant_schema,
      share::schema::ObSchemaGetterGuard &schema_guard);
  int check_tenant_primary_zone_(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTenantSchema &new_tenant_schema);

  int check_create_tenant_locality(
      const common::ObIArray<common::ObString> &pool_list,
      share::schema::ObTenantSchema &tenant_schema,
      share::schema::ObSchemaGetterGuard &schema_guard);
  int check_schema_zone_list(
      common::ObArray<common::ObZone> &zone_list);
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

  int check_locality_compatible_(ObTenantSchema &schema, const bool for_create_tenant);

  // When alter tenant modifies the locality, call the following function to get the zone_list of
  // the resource pool corresponding to the new tenant schema
  int get_new_tenant_pool_zone_list(
      const obrpc::ObModifyTenantArg &arg,
      const share::schema::ObTenantSchema &tenant_schema,
      common::ObIArray<share::ObResourcePoolName> &resource_pool_names,
      common::ObIArray<common::ObZone> &zones_in_pool,
      common::ObIArray<share::schema::ObZoneRegion> &zone_region_list);

  int format_primary_zone_from_zone_score_array(
      common::ObIArray<share::schema::ObZoneScore> &zone_score_array,
      char *buf,
      int64_t buf_len);

  int get_tenant_schema_guard_with_version_in_inner_table(const uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &schema_guard);

  int publish_schema(const uint64_t tenant_id);
  int publish_schema(const uint64_t tenant_id, const common::ObAddrIArray &addrs);

  int get_zones_of_pools(const common::ObIArray<share::ObResourcePoolName> &resource_pool_names,
                         common::ObIArray<common::ObZone> &zones_in_pool);
  int get_tenant_pool_zone_list(
      const share::schema::ObTenantSchema &tenant_schema,
      common::ObIArray<common::ObZone> &zones_in_pool);

  int set_raw_tenant_options(
      const obrpc::ObModifyTenantArg &arg,
      share::schema::ObTenantSchema &new_tenant_schema);

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

  int modify_tenant_inner_phase(const obrpc::ObModifyTenantArg &arg,
      const ObTenantSchema *orig_tenant_schema,
      ObSchemaGetterGuard &schema_guard,
      bool is_restore);

  int broadcast_sys_table_schemas(const uint64_t tenant_id, common::ObIArray<ObTableSchema> &tables);

  int create_tenant_sys_tablets(const uint64_t tenant_id, common::ObIArray<ObTableSchema> &tables);

  int update_sys_variables(const common::ObIArray<obrpc::ObSysVarIdValue> &sys_var_list,
                           const share::schema::ObSysVariableSchema &old_sys_variable,
                           share::schema::ObSysVariableSchema &new_sys_variable,
                           bool& value_changed);

  int cal_resource_pool_list_diff(
      const common::ObIArray<share::ObResourcePoolName> &long_pool_name_list,
      const common::ObIArray<share::ObResourcePoolName> &short_pool_name_list,
      common::ObIArray<share::ObResourcePoolName> &diff_pools);
  int record_tenant_locality_event_history(
      const AlterLocalityOp &alter_locality_op,
      const obrpc::ObModifyTenantArg &arg,
      const share::schema::ObTenantSchema &tenant_schema,
      ObMySQLTransaction &trans);

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

  int drop_resource_pool_pre(const uint64_t tenant_id,
                             common::ObIArray<uint64_t> &drop_ug_id_array,
                             ObIArray<share::ObResourcePoolName> &pool_names,
                             ObMySQLTransaction &trans);
  int drop_resource_pool_final(const uint64_t tenant_id,
                               common::ObIArray<uint64_t> &drop_ug_id_array,
                               ObIArray<share::ObResourcePoolName> &pool_names);
  int try_drop_sys_ls_(const uint64_t meta_tenant_id,
                       common::ObMySQLTransaction &trans);
  //get gts value, return OB_STATE_NOT_MATCH when is not external consistent
  int get_tenant_external_consistent_ts(const int64_t tenant_id, share::SCN &scn);

  int try_force_drop_tenant(const share::schema::ObTenantSchema &tenant_schema);

  int get_tenant_object_name_with_origin_name_in_recyclebin(
      const ObString &origin_tenant_name,
      ObString &object_name,
      common::ObIAllocator *allocator,
      const bool is_flashback);
#ifdef OB_BUILD_ARBITRATION
  int check_tenant_arbitration_service_status_(
      ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const share::ObArbitrationServiceStatus &old_status,
      const share::ObArbitrationServiceStatus &new_status);
#endif
  int modify_and_cal_resource_pool_diff(
      common::ObMySQLTransaction &trans,
      common::ObIArray<uint64_t> &new_ug_id_array,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTenantSchema &new_tenant_schema,
      const common::ObIArray<common::ObString> &new_pool_list,
      bool &grant,
      common::ObIArray<share::ObResourcePoolName> &diff_pools);

  int init_tenant_schema(
      const obrpc::ObCreateTenantArg &create_tenant_arg,
      const share::schema::ObTenantSchema &tenant_schema,
      common::ObIArray<share::schema::ObTableSchema> &tables);

  int generate_tenant_init_configs(const obrpc::ObCreateTenantArg &arg,
      const uint64_t user_tenant_id,
      common::ObIArray<common::ObConfigPairs> &init_configs);

  int generate_tenant_schema(
      const obrpc::ObCreateTenantArg &arg,
      const share::ObTenantRole &tenant_role,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObTenantSchema &user_tenant_schema,
      ObTenantSchema &meta_tenant_schema,
      common::ObIArray<common::ObConfigPairs> &init_configs);

  int init_schema_status(
      const uint64_t tenant_id,
      const share::ObTenantRole &tenant_role);

  int init_system_variables(
      const obrpc::ObCreateTenantArg &arg,
      const ObTenantSchema &tenant_schema,
      ObSysVariableSchema &sys_variable_schema);
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
  int construct_region_list(
      common::ObIArray<common::ObRegion> &region_list,
      const common::ObIArray<common::ObZone> &zone_list);

  int parse_and_set_create_tenant_new_locality_options(
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTenantSchema &schema,
      const common::ObIArray<share::ObResourcePoolName> &pools,
      const common::ObIArray<common::ObZone> &zones_list,
      const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list);
  int generate_zone_list_by_locality(
      const share::schema::ZoneLocalityIArray &zone_locality,
      const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      common::ObArray<common::ObZone> &zone_list) const;
  int get_primary_regions_and_zones(
      const ObIArray<common::ObZone> &zone_list,
      const ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      const ObIArray<common::ObZone> &first_primary_zone_array,
      ObIArray<common::ObRegion> &primary_regions,
      ObIArray<common::ObZone> &zones_in_primary_regions);
  int get_zone_region(
      const common::ObZone &zone,
      const common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      common::ObRegion &region);
  int create_tenant_schema(
      const obrpc::ObCreateTenantArg &arg,
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObTenantSchema &user_tenant_schema,
      share::schema::ObTenantSchema &meta_tenant_schema,
      const common::ObIArray<common::ObConfigPairs> &init_configs);

  // this function is used for add extra tenant config init during create excepet data version
  // The addition of new configuration items requires the addition or modification of related test cases to ensure their effectiveness.
  int add_extra_tenant_init_config_(
      const uint64_t tenant_id,
      common::ObIArray<common::ObConfigPairs> &init_configs);
private:
  bool inited_;
  volatile bool stopped_;
  ObUnitManager *unit_mgr_;
  ObDDLService *ddl_service_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  obrpc::ObCommonRpcProxy *common_rpc_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::ObLSTableOperator *lst_operator_;
  share::schema::ObDDLTransController *ddl_trans_controller_;
  ObZoneManager *zone_mgr_;
};
}
}
#endif // _OCEANBASE_ROOTSERVER_OB_TENANT_DDL_SERVICE_H_
