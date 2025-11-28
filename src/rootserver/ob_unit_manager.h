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

#ifndef OCEANBASE_ROOTSERVER_OB_UNIT_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_UNIT_MANAGER_H_

#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_unit_table_operator.h"
#include "lib/worker.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_unit_placement_strategy.h"
namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObServerConfig;
class ObMySQLTransaction;
}
namespace share
{
struct ObServerStatus;
struct ObUnitStat;
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace rootserver
{
class ObZoneManager;
class ObServerManager;
class ObRootBalancer;
class ObUnitManager
{
public:
  friend class ObServerBalancer;
  typedef common::hash::ObHashMap<uint64_t, share::ObResourcePool *> IdPoolMap;
  typedef common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> TenantPoolsMap;
  struct UnitZoneOrderCmp
  {
    bool operator()(const share::ObUnit *left, const share::ObUnit *right) {
      bool bool_ret = true;
      if (nullptr == left && nullptr == right) {
        bool_ret = false;
      } else if (nullptr == left && nullptr != right) {
        bool_ret = true;
      } else if (nullptr != left && nullptr == right) {
        bool_ret = false;
      } else if (left->zone_ < right->zone_) {
        bool_ret = true;
      } else if (left->zone_ > right->zone_) {
        bool_ret = false;
      } else {
        bool_ret = left->unit_id_ < right->unit_id_;
      }
      return bool_ret;
    }
  };
  struct DeleteUnitPtrCmp
  {
    DeleteUnitPtrCmp(const common::ObArray<uint64_t> &gts_unit_ids) : gts_unit_ids_(gts_unit_ids), ret_(OB_SUCCESS) {}
    bool operator()(const share::ObUnit *left, const share::ObUnit *right);
    int check_unit_server_active(const share::ObUnit &unit, bool &is_active);
    const common::ObArray<uint64_t> &gts_unit_ids_;
    int ret_;
  };
  struct ZoneUnit
  {
    ZoneUnit() : zone_(), unit_infos_() {}
    ~ZoneUnit() {}

    bool is_valid() const { return !zone_.is_empty() && unit_infos_.count() > 0; }
    void reset() { zone_.reset(); unit_infos_.reset(); }
    int assign(const ZoneUnit &other);

    TO_STRING_KV(K_(zone), K_(unit_infos));
  public:
    common::ObZone zone_;
    common::ObArray<share::ObUnitInfo> unit_infos_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ZoneUnit);
  };

  struct ObUnitLoad: public ObIServerResourceDemand
  {
    ObUnitLoad() : unit_(NULL), unit_config_(NULL), pool_(NULL) {}
    ~ObUnitLoad() {}

    inline bool is_valid() const {
      return NULL != pool_ && NULL != unit_ && NULL != unit_config_;
    }
    // return -1 if resource_type is invalid
    virtual double get_demand(ObResourceType resource_type) const override;
    inline bool is_sys_unit() const { return NULL != pool_ && common::OB_SYS_TENANT_ID == pool_->tenant_id_; }
    uint64_t get_tenant_id() const { return NULL != pool_ ? pool_->tenant_id_ : common::OB_INVALID_TENANT_ID; }
    uint64_t get_unit_id() const { return NULL != unit_ ? unit_->unit_id_ : common::OB_INVALID_ID; }
    uint64_t get_resource_pool_id() const { return NULL != pool_ ? pool_->resource_pool_id_ : common::OB_INVALID_ID; }
    TO_STRING_KV(
        "unit_id", get_unit_id(),
        "tenant_id", get_tenant_id(),
        "resource_pool_id", get_resource_pool_id(),
        KPC_(unit_config),
        KP_(unit), KP_(unit_config), KP_(pool));
  public:
    share::ObUnit *unit_;
    share::ObUnitConfig *unit_config_;
    share::ObResourcePool *pool_;
  };
  struct ObUnitNumCountMap : public common::hash::ObHashMap<int64_t, int64_t>
  {
    ObUnitNumCountMap() : common::hash::ObHashMap<int64_t, int64_t>() {}
    int inc_unit_num_cnt(const int64_t unit_num);
    int dec_unit_num_cnt(const int64_t unit_num);
  };

  enum EndMigrateOp {
    COMMIT,
    ABORT,
    REVERSE,
  };

  const char *end_migrate_op_type_to_str(const EndMigrateOp &t);

public:
  ObUnitManager(ObServerManager &server_mgr, ObZoneManager &zone_mgr);
  virtual ~ObUnitManager();

  int init(common::ObMySQLProxy &proxy,
           common::ObServerConfig &server_config,
           obrpc::ObSrvRpcProxy &srv_rpc_proxy,
           share::schema::ObMultiVersionSchemaService &schema_service,
           ObRootBalancer &root_balance,
           ObRootService &root_service);
  virtual int load();
  common::SpinRWLock& get_lock() { return lock_; }
  common::ObMySQLProxy &get_sql_proxy() { return *proxy_; }
  int fetch_new_unit_config_id(uint64_t &unit_config_id);
  int fetch_new_resource_pool_id(uint64_t &resource_pool_id);
  // unit config related
  virtual int create_unit_config(const share::ObUnitConfig &unit_config,
                                 const bool if_not_exist);
  virtual int drop_unit_config(const share::ObUnitConfigName &name, const bool if_exist);
  virtual int alter_unit_config(const share::ObUnitConfig &unit_config);
  virtual int check_unit_config_exist(const share::ObUnitConfigName &unit_config_name,
                                      bool &is_exist);

  // resource pool related
  virtual int check_resource_pool_exist(const share::ObResourcePoolName &resource_pool_name,
                                        bool &is_exist);
  virtual int clone_resource_pool(share::ObResourcePool &resource_pool,
                                  const share::ObUnitConfigName &unit_config_name,
                                  const uint64_t source_tenant_id);
  virtual int check_tenant_pools_altering_unit_num(const uint64_t tenant_id, bool &check_ret);
  virtual int create_resource_pool(share::ObResourcePool &resource_pool,
                                   const share::ObUnitConfigName &config_name,
                                   const bool if_not_exist);
  virtual int get_zone_pools_unit_num(
      const common::ObZone &zone,
      const common::ObIArray<share::ObResourcePoolName> &new_pool_name_list,
      int64_t &total_unit_num,
      int64_t &full_unit_num,
      int64_t &logonly_unit_num);
  virtual int alter_resource_pool(
      const share::ObResourcePool &alter_pool,
      const share::ObUnitConfigName &config_name,
      const common::ObIArray<uint64_t> &delete_unit_id_array,
      const common::ObString &sql_text);
  virtual int drop_resource_pool(
      const uint64_t pool_id,
      const bool if_exist);
  virtual int drop_resource_pool(
      const share::ObResourcePoolName &name,
      const bool if_exist);
  //Delete resource pool and associated unit
  virtual int remove_resource_pool_unit_in_trans(const int64_t resource_pool_id,
                                                 common::ObMySQLTransaction &trans);
  //Delete the resource pool and associated unit in memory, and other memory structures
  virtual int delete_resource_pool_unit(share::ObResourcePool *pool);
  virtual int split_resource_pool(const share::ObResourcePoolName &pool_name,
                                  const common::ObIArray<common::ObString> &split_pool_list,
                                  const common::ObIArray<common::ObZone> &zone_list);
  virtual int merge_resource_pool(const common::ObIArray<common::ObString> &old_pool_list,
                                  const common::ObIArray<common::ObString> &new_pool_list);
  virtual int alter_resource_tenant(
      const uint64_t tenant_id,
      const int64_t new_unit_num,
      const common::ObIArray<uint64_t> &unit_group_id_array,
      const common::ObString &sql_text);
  static int find_alter_resource_tenant_unit_num_rs_job(
    const uint64_t tenant_id,
    int64_t &job_id,
    common::ObISQLClient &sql_proxy);
  virtual int grant_pools(
      common::ObMySQLTransaction &trans,
      const lib::Worker::CompatMode compat_mode,
      const common::ObIArray<share::ObResourcePoolName> &pool_names,
      const uint64_t tenant_id,
      const uint64_t source_tenant_id,
      const bool check_data_version);
  virtual int revoke_pools(
      common::ObMySQLTransaction &trans,
      const common::ObIArray<share::ObResourcePoolName> &pool_names,
      const uint64_t tenant_id);
  virtual int get_tenant_alive_servers_non_block(const uint64_t tenant_id,
                                           common::ObIArray<common::ObAddr> &servers);
  virtual int get_pool_ids_of_tenant(const uint64_t tenant_id,
                                     common::ObIArray<uint64_t> &pool_ids) const;
  virtual int get_pool_names_of_tenant(const uint64_t tenant_id,
      common::ObIArray<share::ObResourcePoolName> &pool_names) const;
  virtual int get_unit_config_by_pool_name(
      const common::ObString &pool_name,
      share::ObUnitConfig &unit_config) const;
  virtual int get_zones_of_pools(const common::ObIArray<share::ObResourcePoolName> &pool_names,
                                 common::ObIArray<common::ObZone> &zones) const;
  virtual int get_pools(common::ObIArray<share::ObResourcePool> &pools) const;
  virtual int create_sys_units(const common::ObIArray<share::ObUnit> &sys_units);
  virtual int get_tenant_pool_zone_list(const uint64_t tenant_id,
                                        common::ObIArray<common::ObZone> &zone_list) const;
  virtual int cancel_migrate_out_units(const common::ObAddr &server);
  virtual int check_server_empty(const common::ObAddr &server,
                                 bool &is_empty) const;
  virtual int finish_migrate_unit(const uint64_t unit_id);
  virtual int finish_migrate_unit_not_in_tenant(
              share::ObResourcePool *pool);
  virtual int finish_migrate_unit_not_in_locality(
              uint64_t tenant_id,
              share::schema::ObSchemaGetterGuard *schema_guard,
              ObArray<common::ObZone> zone_list);


  virtual int get_unit_infos_of_pool(const uint64_t resource_pool_id,
                                     common::ObIArray<share::ObUnitInfo> &unit_infos) const;
  virtual int get_deleting_units_of_pool(const uint64_t resource_pool_id,
                                         common::ObIArray<share::ObUnit> &units) const;
  virtual int commit_alter_tenant_resource_unit_num(const uint64_t tenant_id,
      const ObIArray<share::ObUnit> &unit_array);
  virtual int get_unit_infos(const common::ObIArray<share::ObResourcePoolName> &pools,
                             common::ObIArray<share::ObUnitInfo> &unit_infos);
  virtual int get_servers_by_pools(const common::ObIArray<share::ObResourcePoolName> &pools,
                                   common::ObIArray<ObAddr> &addrs);

  virtual int get_unit_ids(common::ObIArray<uint64_t> &unit_ids) const;
  virtual int get_logonly_unit_by_tenant(const int64_t tenant_id,
                                         common::ObIArray<share::ObUnitInfo> &logonly_unit_infos);
  virtual int get_logonly_unit_by_tenant(share::schema::ObSchemaGetterGuard &schema_guard,
                                         const int64_t tenant_id,
                                         common::ObIArray<share::ObUnitInfo> &logonly_unit_infos);
  virtual int get_tenants_of_server(const common::ObAddr &server,
      common::hash::ObHashSet<uint64_t> &tenant_id_set) const;
  virtual int check_tenant_on_server(const uint64_t tenant_id,
      const common::ObAddr &server, bool &on_server) const;
  int get_tenant_unit_servers_with_lock(
      const uint64_t tenant_id,
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &server_array) const;
  int get_tenant_unit_servers(
      const uint64_t tenant_id,
      const common::ObZone &zone,
      common::ObIArray<common::ObAddr> &server_array) const;

  virtual int admin_migrate_unit(const uint64_t unit_id,
                                 const common::ObAddr &dst,
                                 bool is_cancel = false);
  int check_enough_resource_for_delete_server(
      const ObAddr &server,
      const ObZone &zone);
  template <typename SCHEMA>
  int check_schema_zone_unit_enough(
      const common::ObZone &zone,
      const int64_t total_unit_num,
      const int64_t full_unit_num,
      const int64_t logonly_unit_num,
      const SCHEMA &schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &enough);
  int check_pools_unit_legality_for_locality(
      const common::ObIArray<share::ObResourcePoolName> &pools,
      const common::ObIArray<common::ObZone> &schema_zone_list,
      const common::ObIArray<share::ObZoneReplicaNumSet> &zone_locality,
      bool &is_legal);
  static int calc_sum_load(const common::ObArray<ObUnitLoad> *unit_loads,
                           share::ObUnitConfig &sum_load,
                           const bool include_ungranted_unit = true);
  // get hard limit
  int get_hard_limit(double &hard_limit) const;

  static int convert_pool_name_list(
      const common::ObIArray<common::ObString> &split_pool_list,
      common::ObIArray<share::ObResourcePoolName> &split_pool_name_list);
  static int check_tenant_in_heterogeneous_deploy_mode(
      const uint64_t tenant_id, bool &in_hetero_deploy_mode);
  static int check_tenant_gts_standalone_enabled(
      const uint64_t tenant_id, bool &enabled);

  static int get_tenant_gts_unit_ids(
      const uint64_t tenant_id,
      ObIArray<uint64_t> &unit_ids);
private:
  enum AlterUnitNumType
  {
    AUN_SHRINK = 0,
    AUN_ROLLBACK,
    AUN_EXPAND,
    AUN_NOP,
    AUN_MAX,
  };

  enum AlterResourceErr
  {
    MIN_CPU = 0,
    MAX_CPU,
    MEMORY,
    LOG_DISK,
    DATA_DISK,
    ALT_ERR
  };

  struct PoolOperationInfo
  {
    PoolOperationInfo() : old_unit_num_(0), new_unit_num_(0), alter_type_(AUN_NOP), replica_type_(REPLICA_TYPE_FULL) {}
    PoolOperationInfo(int64_t old_unit_num, int64_t new_unit_num, AlterUnitNumType alter_type, common::ObReplicaType replica_type)
     : old_unit_num_(old_unit_num), new_unit_num_(new_unit_num), alter_type_(alter_type), replica_type_(replica_type) {}
    ~PoolOperationInfo() {}
    bool operator==(const PoolOperationInfo &other) const
    {
      return old_unit_num_ == other.old_unit_num_
          && new_unit_num_ == other.new_unit_num_
          && alter_type_ == other.alter_type_
          && replica_type_ == other.replica_type_;
    }
    bool operator!=(const PoolOperationInfo &other) const
    {
      return !(*this == other);
    }
    TO_STRING_KV(K_(old_unit_num), K_(new_unit_num), K_(alter_type), K_(replica_type))
    int64_t old_unit_num_;
    int64_t new_unit_num_;
    AlterUnitNumType alter_type_;
    common::ObReplicaType replica_type_;
  };

  struct ZoneUnitPtr
  {
    ZoneUnitPtr() : zone_(), unit_ptrs_() {}
    ~ZoneUnitPtr() {}

    bool is_valid() const { return !zone_.is_empty() && unit_ptrs_.count() > 0; }
    void reset() { zone_.reset(); unit_ptrs_.reset(); }
    int assign(const ZoneUnitPtr &other);
    int sort_by_unit_id();

    TO_STRING_KV(K_(zone), K_(unit_ptrs));
  public:
    common::ObZone zone_;
    common::ObSEArray<share::ObUnit *, 16> unit_ptrs_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ZoneUnitPtr);
  };

  class UnitIdCmp
  {
  public:
    UnitIdCmp() : ret_(common::OB_SUCCESS) {}
    ~UnitIdCmp() {}
    bool operator()(const share::ObUnit *left, const share::ObUnit *right) {
      bool bool_ret = false;
      if (common::OB_SUCCESS != ret_) {
      } else if (OB_UNLIKELY(NULL == left || NULL == right)) {
        ret_ = common::OB_ERR_UNEXPECTED;
      } else if (left->unit_id_ < right->unit_id_) {
        bool_ret = true;
      } else {
        bool_ret = false;
      }
      return bool_ret;
    }
    int get_ret() const { return ret_; }
  private:
    int ret_;
  };

  struct UnitNum
  {
    UnitNum() : full_unit_num_(0), logonly_unit_num_(0) {}
    UnitNum(const int64_t full_unit_num, const int64_t logonly_unit_num)
      : full_unit_num_(full_unit_num), logonly_unit_num_(logonly_unit_num) {}
    TO_STRING_KV(K_(full_unit_num), K_(logonly_unit_num));
    void reset() { full_unit_num_ = 0; logonly_unit_num_ = 0; }
    int64_t full_unit_num_;
    int64_t logonly_unit_num_;
  };

  static const int64_t UNIT_MAP_BUCKET_NUM = 1024 * 64;
  static const int64_t CONFIG_MAP_BUCKET_NUM = 1024;
  static const int64_t CONFIG_REF_COUNT_MAP_BUCKET_NUM = 1024;
  static const int64_t CONFIG_POOLS_MAP_BUCKET_NUM = 1024;
  static const int64_t POOL_MAP_BUCKET_NUM = 1024;
  static const int64_t UNITLOAD_MAP_BUCKET_NUM = 1024;
  static const int64_t TENANT_POOLS_MAP_BUCKET_NUM = 1024;
  static const int64_t SERVER_MIGRATE_UNITS_MAP_BUCKET_NUM = 1024;
  static const int64_t SERVER_REF_COUNT_MAP_BUCKET_NUM = 1024;
  static const int64_t NOTIFY_RESOURCE_RPC_TIMEOUT = 9 * 1000000; // 9 second

private:
  // make sure lock_ is held when calling this method
  int check_inner_stat_() const;
  // for ObServerBalancer
  IdPoolMap& get_id_pool_map() { return id_pool_map_; }
  TenantPoolsMap& get_tenant_pools_map() { return tenant_pools_map_; }
  int try_migrate_unit(const uint64_t unit_id,
                       const uint64_t tenant_id,
                       const share::ObUnitStat &unit_stat,
                       const common::ObIArray<share::ObUnitStat> &migrating_unit_stat,
                       const common::ObAddr &dst,
                       const share::ObServerResourceInfo &dst_resource_info,
                       const bool is_manual = false);
  int get_zone_units(const common::ObArray<share::ObResourcePool *> &pools,
                     common::ObArray<ZoneUnit> &zone_units) const;
  virtual int end_migrate_unit(const uint64_t unit_id, const EndMigrateOp end_migrate_op = COMMIT);
  int get_excluded_servers(
      const share::ObUnit &unit,
      const share::ObUnitStat &unit_stat,
      const char *module,
      const ObIArray<share::ObServerInfoInTable> &servers_info, // servers info in unit.zone_
      const ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info, // active servers' resource info in unit.zone_
      common::ObIArray<common::ObAddr> &servers) const;
  int get_excluded_servers(const uint64_t resource_pool_id,
                           const common::ObZone &zone,
                           const char *module,
                           const bool new_allocate_pool, 
                           common::ObIArray<common::ObAddr> &excluded_servers) const;
  int choose_server_for_unit(
      const share::ObUnitResource &config,
      const common::ObZone &zone,
      const common::ObArray<common::ObAddr> &excluded_servers,
      const char *module,
      const ObIArray<share::ObServerInfoInTable> &active_servers_info, // active_servers_info of the give zone,
      const ObIArray<obrpc::ObGetServerResourceInfoResult> &active_servers_resource_info, // active_servers_resource_info of the give zone
      common::ObAddr &server,
      std::string &resource_not_enough_reason) const;

  int check_expand_zone_resource_allowed_by_old_unit_stat_(
      const uint64_t tenant_id,
      bool &is_allowed);
  int check_expand_zone_resource_allowed_by_new_unit_stat_(
      const common::ObIArray<share::ObResourcePoolName> &pool_names);
  int check_expand_zone_resource_allowed_by_data_disk_size_(
      const uint64_t tenant_id,
      const common::ObIArray<share::ObResourcePoolName> &pool_names);
  int check_grant_pools_unit_num_legal_(
      const uint64_t tenant_id,
      const common::ObIArray<share::ObResourcePoolName> &pool_names);
  int check_grant_pools_replica_type_legal_(
      const uint64_t tenant_id,
      const common::ObIArray<share::ObResourcePoolName> &pool_names);
  int check_unit_num_legal_after_pools_operation_(
      const uint64_t tenant_id,
      const common::ObIArray<ObUnitManager::PoolOperationInfo> &pool_op_info_array,
      const bool in_hetero_deploy_mode,
      const bool is_gts_standalone);
  int get_migrate_units_by_server(const ObAddr &server,
                                  common::ObIArray<uint64_t> &migrate_units) const;
  int try_cancel_migrate_unit(const share::ObUnit &unit, bool &is_canceled);
  //////end of server_balance

  static int check_bootstrap_pool(const share::ObResourcePool &pool);
  int have_enough_resource(const obrpc::ObGetServerResourceInfoResult &report_server_resource_info,
                           const share::ObUnitResource &unit_resource,
                           const double limit,
                           bool &is_enough,
                           AlterResourceErr &err_index) const;
  virtual int migrate_unit_(const uint64_t unit_id, const common::ObAddr &dst, const bool is_manual = false);
  int inner_get_all_unit_infos_by_tenant_(const uint64_t tenant_id,
                                          ObIArray<share::ObUnitInfo> &unit_infos);
  int do_migrate_unit_notify_resource_(const share::ObResourcePool &pool,
                                       const share::ObUnit &new_unit,
                                       const bool is_manual,
                                       const bool granted);
  int do_migrate_unit_in_trans_(const share::ObResourcePool &pool,
                                const share::ObUnit &new_unit,
                                const bool is_manual,
                                const bool granted);
  int do_migrate_unit_inmemory_(const share::ObUnit &new_unit,
                                share::ObUnit *unit,
                                const bool is_manual,
                                const bool granted);
  int inner_get_unit_info_by_id(const uint64_t unit_id, share::ObUnitInfo &unit) const;
  int check_server_enough(const uint64_t tenant_id,
                          const common::ObIArray<share::ObResourcePoolName> &pool_names,
                          bool &enough);

  int inner_get_active_unit_infos_of_tenant(const share::schema::ObTenantSchema &tenant_schema,
                                            common::ObIArray<share::ObUnitInfo> &unit_info);

  int inner_get_unit_infos_of_pool_(const uint64_t resource_pool_id,
                                   common::ObIArray<share::ObUnitInfo> &unit_infos) const;

  int inner_get_zone_alive_unit_infos_by_tenant(
      const uint64_t tenant_id,
      const common::ObZone &zone,
      common::ObIArray<share::ObUnitInfo> &unit_infos) const;

  int inner_get_pool_ids_of_tenant(const uint64_t tenant_id,
                                   ObIArray<uint64_t> &pool_ids) const;

  int check_resource_pool(share::ObResourcePool &resource_pool, const bool is_clone_tenant) const;
  int allocate_pool_units_(common::ObISQLClient &client,
                          const share::ObResourcePool &pool,
                          const common::ObIArray<common::ObZone> &zones,
                          common::ObIArray<uint64_t> *new_unit_group_id_array,
                          const bool new_allocate_pool,
                          const int64_t increase_delta_unit_num,
                          const share::ObUnit::Status new_unit_status,
                          const char *module,
                          common::ObIArray<share::ObUnit> &new_units);
  int get_pool_servers(const uint64_t resource_pool_id,
                       const common::ObZone &zone,
                       common::ObIArray<common::ObAddr> &servers) const;
  int get_pools_servers(const common::ObIArray<share::ObResourcePool *> &pools,
      common::hash::ObHashMap<common::ObAddr, int64_t> &server_ref_count_map) const;
  int add_unit(common::ObISQLClient &client, const share::ObUnit &unit);
  // load balance related
  int inner_get_unit_ids(common::ObIArray<uint64_t> &unit_ids) const;
  int inner_get_tenant_zone_full_unit_num(
      const int64_t tenant_id,
      const common::ObZone &zone,
      int64_t &unit_num);
  int inner_get_tenant_pool_zone_list(
      const uint64_t tenant_id,
      common::ObIArray<common::ObZone> &zone_list) const;
  int get_tenant_zone_all_unit_loads(
      const int64_t tenant_id,
      const common::ObZone &zone,
      common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads);
  int get_tenant_zone_unit_loads(
      const int64_t tenant_id,
      const common::ObZone &zone,
      const common::ObReplicaType replica_type,
      common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads);
  int register_alter_resource_tenant_unit_num_rs_job(
      const uint64_t tenant_id,
      const int64_t new_unit_num,
      const common::ObString &sql_text,
      common::ObMySQLTransaction &trans);
  int cancel_alter_resource_tenant_unit_num_rs_job(
      const uint64_t tenant_id,
      common::ObMySQLTransaction &trans);
  int create_alter_resource_tenant_unit_num_rs_job(
      const uint64_t tenant_id,
      const int64_t new_unit_num,
      int64_t &job_id,
      const common::ObString &sql_text,
      common::ObMySQLTransaction &trans);
  int complete_migrate_unit_rs_job_in_pool(
      const int64_t resource_pool_id,
      const int result_ret,
	    common::ObMySQLTransaction &trans);

  // alter pool related
  int inner_check_single_logonly_pool_for_locality(
      const share::ObResourcePool &pool,
      const common::ObIArray<share::ObZoneReplicaAttrSet> &zone_locality,
      bool &is_legal);
  int inner_check_logonly_pools_for_locality(
      const common::ObIArray<share::ObResourcePool *> &pools,
      const common::ObIArray<share::ObZoneReplicaAttrSet> &zone_locality,
      bool &is_legal);
  int inner_check_pools_unit_num_enough_for_locality(
      const common::ObIArray<share::ObResourcePool *> &pools,
      const common::ObIArray<common::ObZone> &schema_zone_list,
      const common::ObIArray<share::ObZoneReplicaNumSet> &zone_locality,
      bool &is_enough);
  int alter_pool_unit_config(share::ObResourcePool *pool,
                             const share::ObUnitConfigName &config_name);
  int get_to_be_deleted_units_in_homo_mode(
      const uint64_t tenant_id,
      const PoolOperationInfo &pool_op_info,
      const common::ObIArray<uint64_t> &delete_unit_group_id_array,
      common::ObIArray<share::ObUnit> &to_be_deleted_units);
  int determine_new_added_unit_status(
      const share::ObResourcePool &pool,
      share::ObUnit::Status &unit_status);
  int rollback_pool_unit_num_(
      common::ObMySQLTransaction &trans,
      share::ObResourcePool *pool,
      const int64_t new_unit_num);
  int precheck_for_alter_pool_unit_num_(
    share::ObResourcePool *pool,
    const int64_t new_unit_num,
    const ObIArray<ObUnitManager::PoolOperationInfo> &pool_op_info_array);
  int alter_pool_unit_num(
      share::ObResourcePool *pool,
      int64_t unit_num,
      const common::ObIArray<uint64_t> &delete_unit_id_array,
      const common::ObString &sql_text);
  int reallocate_pool_unit_group_id_(
      common::ObMySQLTransaction &trans,
      const share::ObResourcePool *pool,
      const common::ObIArray<uint64_t> &new_unit_group_id_array);
  int alter_homo_resource_tenant_adjust_ug_(
      common::ObMySQLTransaction &trans,
      const PoolOperationInfo &pool_op_info,
      common::ObIArray<share::ObResourcePool *> &altered_pools);
  int adjust_altered_pools_unit_group_id_(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      common::ObIArray<share::ObResourcePool *> &altered_pools);
  int determine_pool_unit_group_id_(
      common::ObMySQLTransaction &trans,
      const share::ObResourcePool *pool,
      const ObIArray<ObArray<uint64_t>> &ref_pools_ug_id_arrays,
      ObArray<uint64_t> &unit_group_id_array);
  int do_alter_pool_unit_num_(
      common::ObMySQLTransaction &trans,
      share::ObResourcePool *pool,
      const PoolOperationInfo &pool_op_info,
      const common::ObIArray<uint64_t> &delete_unit_id_array);

  int process_delete_unit_group_in_homo_mode_(
      const uint64_t tenant_id,
      const ObIArray<share::ObResourcePool *> &pools,
      const ObIArray<ObUnitManager::PoolOperationInfo> &pool_op_info_array,
      const ObIArray<uint64_t> &delete_unit_group_id_array,  // user specified
      ObIArray<ObArray<uint64_t>> &delete_unit_id_array);
  int batch_determine_altered_pools_op_info_(
      common::ObArray<share::ObResourcePool *> &pools,
      const int64_t new_unit_num,
      ObArray<share::ObResourcePool *> &altered_pools,
      ObArray<PoolOperationInfo> &pool_op_info_array);
  int determine_alter_unit_num_type(
      share::ObResourcePool *pool,
      const int64_t unit_num,
      AlterUnitNumType &alter_unit_num_type);
  int shrink_pool_unit_num_(
      common::ObMySQLTransaction &trans,
      share::ObResourcePool *pool,
      const int64_t unit_num,
      const common::ObIArray<uint64_t> &delete_unit_id_array);
  int build_sorted_zone_unit_ptr_array(
      const share::ObResourcePool *pool,
      common::ObIArray<ZoneUnitPtr> &zone_unit_ptrs) const;
  int check_shrink_unit_num_zone_condition(
      share::ObResourcePool *pool,
      const int64_t new_unit_num,
      const common::ObIArray<uint64_t> &delete_unit_id_array);
  int fill_delete_unit_ptr_array(
      share::ObResourcePool *pool,
      const common::ObIArray<uint64_t> &delete_unit_id_array,
      const int64_t new_unit_num,
      common::ObIArray<share::ObUnit *> &output_delete_unit_ptr_array);
  int check_shrink_granted_pool_allowed(
      share::ObResourcePool *pool,
      const int64_t unit_num);
  int check_shrink_granted_pool_allowed_by_migrate_unit(
      share::ObResourcePool *pool,
      bool &is_allowed);
  int check_shrink_granted_pool_allowed_by_locality(
      share::ObResourcePool *pool,
      const int64_t unit_num,
      bool &is_allowed);
  int check_shrink_granted_pool_allowed_by_alter_locality(
      share::ObResourcePool *pool,
      bool &is_allowed);
  int do_check_shrink_granted_pool_allowed_by_locality(
      const common::ObIArray<share::ObResourcePool *> &pools,
      const common::ObIArray<common::ObZone> &schema_zone_list,
      const common::ObIArray<share::ObZoneReplicaNumSet> &zone_locality,
      const common::ObIArray<int64_t> &new_unit_nums,
      bool &is_allowed);
  int check_all_pools_granted(
      const common::ObIArray<share::ObResourcePool *> &pools,
      bool &all_granted);
  int check_shrink_granted_pool_allowed_by_tenant_locality(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const common::ObIArray<share::ObResourcePool *> &pools,
      const common::ObIArray<int64_t> &new_unit_nums,
      bool &is_allowed);
  int get_unit_count_per_zone_by_status_(
      const share::ObResourcePool *pool,
      int64_t &active_unit_cnt,
      int64_t &adding_unit_cnt,
      int64_t &deleting_unit_cnt);
  int alter_pool_zone_list(share::ObResourcePool *pool,
                           const common::ObIArray<common::ObZone> &zone_list);
  int add_pool_zone_list(share::ObResourcePool *pool,
                         const common::ObIArray<common::ObZone> &zone_list);
  int remove_pool_zone_list(share::ObResourcePool *pool,
                            const common::ObIArray<common::ObZone> &zone_list);
  int cal_to_be_add_pool_zone_list(const common::ObIArray<common::ObZone> &prev_zone_list,
                                   const common::ObIArray<common::ObZone> &cur_zone_list,
                                   common::ObIArray<common::ObZone> &to_be_add_zones) const;
  int cal_to_be_removed_pool_zone_list(const common::ObIArray<common::ObZone> &prev_zone_list,
                                       const common::ObIArray<common::ObZone> &cur_zone_list,
                                       common::ObIArray<common::ObZone> &to_be_removed_zones) const;
  template <typename SCHEMA>
  int inner_check_schema_zone_unit_enough(
      const common::ObZone &zone,
      const int64_t total_unit_num,
      const int64_t full_unit_num,
      const int64_t logonly_unit_num,
      const SCHEMA &schema,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &enough);
  int inner_get_zone_pools_unit_num(const common::ObZone &zone,
                                    const common::ObIArray<share::ObResourcePool *> &pool_list,
                                    int64_t &total_unit_num,
                                    int64_t &full_unit_num,
                                    int64_t &logonly_unit_num);
  int check_can_add_pool_zone_list_by_locality(
      const share::ObResourcePool *pool,
      const common::ObIArray<common::ObZone> &to_be_add_zones,
      bool &can_add);
  int check_can_remove_pool_zone_list(const share::ObResourcePool *pool,
                                      const common::ObIArray<common::ObZone> &to_be_removed_zones,
                                      bool &can_remove);
  int do_add_pool_zone_list(share::ObResourcePool *pool,
                            const common::ObIArray<common::ObZone> &new_zone_list,
                            const common::ObIArray<common::ObZone> &to_be_add_zones);
  int do_remove_pool_zone_list(share::ObResourcePool *pool,
                               const common::ObIArray<common::ObZone> &new_zone_list,
                               const common::ObIArray<common::ObZone> &to_be_removed_zones);
  int check_full_resource_pool_memory_condition(
      const common::ObIArray<share::ObResourcePool *> &pools,
      const int64_t memory_size) const;
  int check_shrink_memory(const share::ObResourcePool &pool,
                          const int64_t old_memory,
                          const int64_t new_memory) const;
  int change_pool_config(share::ObResourcePool *pool,
                         share::ObUnitConfig *config,
                         share::ObUnitConfig *new_config);
  int check_pool_intersect_(const uint64_t tenant_id,
                           const common::ObIArray<share::ObResourcePoolName> &pool_names,
                           bool &intersect);
  int check_pool_ownership_(const uint64_t tenant_id,
                           const common::ObIArray<share::ObResourcePoolName> &pool_names,
                           const bool grant);

  int do_grant_pools_(common::ObMySQLTransaction &trans,
                     const lib::Worker::CompatMode compat_mode,
                     const ObIArray<share::ObResourcePoolName> &pool_names,
                     const uint64_t tenant_id,
                     const bool check_data_version);

  int do_revoke_pools_(common::ObMySQLTransaction &trans,
                      const common::ObIArray<share::ObResourcePoolName> &pool_names,
                      const uint64_t tenant_id);

  int build_zone_sorted_unit_array_(const share::ObResourcePool *pool,
                                common::ObArray<share::ObUnit*> &units);

  // build hashmaps
  int build_unit_map(const common::ObIArray<share::ObUnit> &units);
  int build_config_map(const common::ObIArray<share::ObUnitConfig> &configs);
  int build_pool_map(const common::ObIArray<share::ObResourcePool> &pools);

  // insert into memory
  int insert_unit_config(share::ObUnitConfig *config);
  int inc_config_ref_count(const uint64_t config_id);
  int dec_config_ref_count(const uint64_t config_id);
  int update_pool_map(share::ObResourcePool *resource_pool);
  int insert_unit(share::ObUnit *unit);
  int insert_unit_loads(share::ObUnit *unit);
  int insert_unit_load(const common::ObAddr &server, const ObUnitLoad &load);
  int insert_load_array(const common::ObAddr &addr, common::ObArray<ObUnitLoad> *load);
  int update_pool_load(share::ObResourcePool *pool, share::ObUnitConfig *new_config);
  int update_unit_load(share::ObUnit *unit, share::ObResourcePool *new_pool);
  int gen_unit_load(share::ObUnit *unit, ObUnitLoad &load) const;
  int gen_unit_load(const uint64_t unit_id, ObUnitLoad &load) const;
  int insert_tenant_pool(const uint64_t tenant_id, share::ObResourcePool *resource_pool);
  int insert_config_pool(const uint64_t config_id, share::ObResourcePool *resource_pool);
  int insert_migrate_unit(const common::ObAddr &src_server, const uint64_t unit_id);

  // delete from memory
  int delete_unit_config(const uint64_t config_id,
                         const share::ObUnitConfigName &config_name);
  int delete_resource_pool(const uint64_t pool_id,
                           const share::ObResourcePoolName &pool_name);
  int delete_units_of_pool(const uint64_t resource_pool_id);
  int delete_units_in_zones(const uint64_t resource_pool_id,
                            const common::ObIArray<common::ObZone> &to_be_removed_zones);
  int delete_inmemory_units(const uint64_t resource_pool_id,
                            const common::ObIArray<uint64_t> &unit_ids);
  int delete_invalid_inmemory_units(const uint64_t resource_pool_id,
                                    const common::ObIArray<uint64_t> &valid_unit_ids);
  int delete_unit_loads(const share::ObUnit &unit);
  int delete_unit_load(const common::ObAddr &server, const uint64_t unit_id);
  int delete_tenant_pool(const uint64_t tenant_id, share::ObResourcePool *pool);
  int delete_config_pool(const uint64_t config_id, share::ObResourcePool *pool);
  int delete_migrate_unit(const common::ObAddr &src_server, const uint64_t unit_id);
  int inner_drop_resource_pool(share::ObResourcePool *pool);

  int get_unit_config_by_name(const share::ObUnitConfigName &name,
                              share::ObUnitConfig *&config) const;
  int get_unit_config_by_id(const uint64_t config_id, share::ObUnitConfig *&config) const;
  // if not exist, return OB_ENTRY_NOT_EXIST
  int get_config_ref_count(const uint64_t config_id, int64_t &ref_count) const;
  int get_server_ref_count(common::hash::ObHashMap<common::ObAddr, int64_t> &map,
      const common::ObAddr &server, int64_t &server_ref_count) const;
  int set_server_ref_count(common::hash::ObHashMap<common::ObAddr, int64_t> &map,
      const common::ObAddr &server, const int64_t server_ref_count) const;
  int inner_get_resource_pool_by_name(
      const share::ObResourcePoolName &name,
      share::ObResourcePool *&pool) const;
  int get_resource_pool_by_id(const uint64_t pool_id,
                              share::ObResourcePool *&pool) const;
  int get_units_by_pool(const uint64_t pood_id, common::ObArray<share::ObUnit *> *&units) const;
  int get_unit_by_id(const uint64_t unit_id, share::ObUnit *&unit) const;
  int get_loads_by_server(const common::ObAddr &server, common::ObArray<ObUnitLoad> *&loads) const;
  int get_pools_by_tenant_(const uint64_t tenant_id,
                          common::ObArray<share::ObResourcePool *> *&pools) const;
  int get_pools_by_config(const uint64_t tenant_id,
                          common::ObArray<share::ObResourcePool *> *&pools) const;
  int get_migrate_units_by_server(const common::ObAddr &server,
                                  common::ObArray<uint64_t> *&migrate_units) const;
  int fetch_new_unit_id(uint64_t &unit_id);
  int batch_fetch_new_unit_group_id(const int64_t count, common::ObArray<uint64_t> &unit_group_ids);
  int fetch_new_unit_group_id(uint64_t &unit_group_id);
  int extract_unit_ids(const common::ObIArray<share::ObUnit *> &units,
                       common::ObIArray<uint64_t> &unit_ids);
  int try_notify_tenant_server_unit_resource_(
      const uint64_t tenant_id,
      const bool is_delete, /*Expansion of semantics, possibly deleting resources*/
      ObNotifyTenantServerResourceProxy &notify_proxy,
      const uint64_t unit_config_id,
      const lib::Worker::CompatMode compat_mode,
      const share::ObUnit &unit,
      const bool if_not_grant,
      const bool skip_offline_server,
      const bool check_data_version);
  int build_notify_create_unit_resource_rpc_arg_(
      const uint64_t tenant_id,
      const share::ObUnit &unit,
      const lib::Worker::CompatMode compat_mode,
      const uint64_t unit_config_id,
      const bool if_not_grant,
      const bool fill_data_version,
      obrpc::TenantServerUnitConfig &rpc_arg) const;
  int get_meta_and_user_data_version_(
      const uint64_t tenant_id,
      uint64_t &user_data_version,
      uint64_t &meta_data_version) const;
  int check_dest_data_version_is_loaded_(
      const uint64_t tenant_id, const ObAddr &addr);
  int do_notify_unit_resource_(
    const common::ObAddr server,
    const obrpc::TenantServerUnitConfig &notify_arg,
    ObNotifyTenantServerResourceProxy &notify_proxy);
  int rollback_persistent_units_(
      const uint64_t tenant_id,
      const common::ObArray<share::ObUnit> &units,
      ObNotifyTenantServerResourceProxy &notify_proxy);
  int sum_servers_resources(ObUnitPlacementStrategy::ObServerResource &server_resource,
                            const share::ObUnitConfig &unit_config);
  int get_pools_by_id(
      const common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
      const uint64_t id, common::ObArray<share::ObResourcePool *> *&pools) const;
  int insert_id_pool(
      common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
      common::ObPooledAllocator<common::ObArray<share::ObResourcePool *> > &allocator,
      const uint64_t id,
      share::ObResourcePool *resource_pool);
  int insert_id_pool_array(
      common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
      const uint64_t id,
      common::ObArray<share::ObResourcePool *> *pools);
  int delete_id_pool(
      common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> &map,
      common::ObPooledAllocator<common::ObArray<share::ObResourcePool *> > &allocator,
      const uint64_t id,
      share::ObResourcePool *resource_pool);
  int cancel_migrate_unit(
      const share::ObUnit &unit,
      const bool migrate_from_server_can_migrate_in);
  int check_split_pool_name_condition(
      const common::ObIArray<share::ObResourcePoolName> &split_pool_name_list);
  int check_split_pool_zone_condition(
      const common::ObIArray<common::ObZone> &split_zone_list,
      const share::ObResourcePool &pool);
  int do_split_resource_pool(
      share::ObResourcePool *pool,
      const common::ObIArray<share::ObResourcePoolName> &split_pool_name_list,
      const common::ObIArray<common::ObZone> &split_zone_list);
  int do_split_pool_persistent_info(
      share::ObResourcePool *pool,
      const common::ObIArray<share::ObResourcePoolName> &split_pool_name_list,
      const common::ObIArray<common::ObZone> &split_zone_list,
      common::ObIArray<share::ObResourcePool *> &allocate_pool_ptrs);
  int do_split_pool_inmemory_info(
      share::ObResourcePool *pool,
      common::ObIArray<share::ObResourcePool *> &allocate_pool_ptrs);
  int fill_splitting_pool_basic_info(
      const share::ObResourcePoolName &new_pool_name,
      share::ObResourcePool *new_pool,
      const common::ObZone &zone,
      share::ObResourcePool *orig_pool);
  int split_pool_unit_persistent_info(
      common::ObMySQLTransaction &trans,
      const common::ObZone &zone,
      share::ObResourcePool *new_pool,
      share::ObResourcePool *orig_pool);
  int split_pool_unit_inmemory_info(
      const common::ObZone &zone,
      share::ObResourcePool *new_pool,
      share::ObResourcePool *orig_pool);
  int convert_pool_name_list(
      const common::ObIArray<common::ObString> &old_pool_list,
      common::ObIArray<share::ObResourcePoolName> &old_pool_name_list,
      const common::ObIArray<common::ObString> &new_pool_list,
      share::ObResourcePoolName &merge_pool_name);
  int check_merge_pool_name_condition(
      const share::ObResourcePoolName &merge_pool_name);
  int check_old_pool_name_condition(
      common::ObIArray<share::ObResourcePoolName> &old_pool_name_list,
      common::ObIArray<common::ObZone> &merge_zone_list,
      common::ObIArray<share::ObResourcePool*> &old_pool);
  int do_merge_resource_pool(
      const share::ObResourcePoolName &merge_pool_name,
      const common::ObIArray<common::ObZone> &merge_zone_list,
      common::ObIArray<share::ObResourcePool*> &old_pool);
  int do_merge_pool_persistent_info(
      share::ObResourcePool *&allocate_pool_ptr,
      const share::ObResourcePoolName &merge_pool_name,
      const common::ObIArray<common::ObZone> &merge_zone_list,
      const common::ObIArray<share::ObResourcePool*> &old_pool);
  int fill_merging_pool_basic_info(
      share::ObResourcePool *&allocate_pool_ptr,
      const share::ObResourcePoolName &merge_pool_name,
      const common::ObIArray<common::ObZone> &merge_zone_list,
      const common::ObIArray<share::ObResourcePool*> &old_pool);
  int merge_pool_unit_persistent_info(
      common::ObMySQLTransaction &trans,
      share::ObResourcePool *new_pool,
      share::ObResourcePool *orig_pool);
  int do_merge_pool_inmemory_info(
      share::ObResourcePool *new_pool/*allocate_pool_ptr*/,
      common::ObIArray<share::ObResourcePool*> &old_pool);
  int merge_pool_unit_inmemory_info(
      share::ObResourcePool *new_pool/*allocate_pool_ptr*/,
      common::ObIArray<share::ObResourcePool*> &old_pool);
  int inner_create_unit_config_(
      const share::ObUnitConfig &unit_config,
      const bool if_not_exist);
  int inner_create_resource_pool_(
      share::ObResourcePool &resource_pool,
      const share::ObUnitConfigName &config_name,
      const bool if_not_exist,
      const uint64_t source_tenant_id,
      const common::ObIArray<share::ObUnitInfo> &source_units);
  int inner_try_delete_migrate_unit_resource(
      const uint64_t unit_id,
      const common::ObAddr &migrate_from_server);
  int get_servers_resource_info_via_rpc(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    ObIArray<obrpc::ObGetServerResourceInfoResult> &report_server_resource_info) const;
  int get_server_resource_info_via_rpc(
    const share::ObServerInfoInTable &server_info,
    obrpc::ObGetServerResourceInfoResult &report_servers_resource_info) const ;
  int inner_check_pool_altering_unit_num(
      const uint64_t pool_id,
      bool &is_shrinking);
  int inner_commit_alter_unit_num_(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const common::ObArray<share::ObResourcePool*> &pools);

  int check_shrink_resource_(const common::ObIArray<share::ObResourcePool *> &pools,
      const share::ObUnitResource &old_resource,
      const share::ObUnitResource &new_resource) const;
  int check_shrink_resource_(const share::ObResourcePool &pool,
      const share::ObUnitResource &resource,
      const share::ObUnitResource &new_resource) const;
  int check_expand_resource_(
      const char *module,
      const common::ObIArray<share::ObResourcePool *> &pools,
      const share::ObUnitResource &old_resource,
      const share::ObUnitResource &new_resource) const;
  int check_expand_resource_(
      const share::ObServerInfoInTable &server_info,
      const share::ObUnitResource &expand_resource,
      bool &can_expand,
      AlterResourceErr &err_index) const;
  int check_data_disk_size_mode_change_(
      const common::ObIArray<share::ObResourcePool *> &pools,
      const share::ObUnitResource &old_ur,
      const share::ObUnitResource &new_ur) const;
  // clone tenant resource pool related
  int construct_resource_pool_to_clone_(
      const uint64_t source_tenant_id,
      share::ObResourcePool &pool_to_clone);
  int construct_source_tenant_unit_num_(
      const uint64_t source_tenant_id,
      int64_t &unit_num);
  int check_new_pool_units_for_clone_tenant_(
      ObISQLClient &client,
      const share::ObResourcePool &pool,
      const common::ObIArray<share::ObUnitInfo> &source_units);
  int construct_server_resources_info_(
      const ObZone &zone,
      const ObIArray<share::ObUnitInfo> &source_units,
      ObIArray<share::ObServerInfoInTable> &server_infos,
      ObIArray<ObUnitPlacementStrategy::ObServerResource> &server_resources);
  int check_server_resources_and_persist_unit_info_(
      ObISQLClient &client,
      ObNotifyTenantServerResourceProxy &notify_proxy,
      const ObZone &zone,
      const share::ObResourcePool &pool,
      const ObIArray<share::ObServerInfoInTable> &server_infos,
      const ObIArray<ObUnitPlacementStrategy::ObServerResource> &server_resources,
      const share::ObUnitResource &config);

  int try_persist_unit_info_(
      ObNotifyTenantServerResourceProxy &notify_proxy,
      ObISQLClient &client,
      const ObZone &zone,
      const share::ObResourcePool &pool,
      const lib::Worker::CompatMode &compat_mode,
      const uint64_t unit_group_id,
      const share::ObUnit::Status new_unit_status,
      const ObAddr &server,
      ObIArray<share::ObUnit> &new_units,
      ObIArray<share::ObUnit> &units);
  int check_server_status_valid_and_construct_log_(
      const share::ObServerInfoInTable &server_info,
      const bool for_clone_tenant,
      bool &is_server_valid,
      std::string &not_valid_reason) const;
  int check_server_resource_enough_and_construct_log_(
      const ObUnitPlacementStrategy::ObServerResource &server_resource,
      const share::ObUnitResource &config,
      bool &is_resource_enough,
      std::string &resource_not_enough_reason) const;
  int construct_valid_servers_resource_(
      const ObZone &zone,
      const share::ObUnitResource &config,
      const ObIArray<ObAddr> &excluded_servers,
      const ObIArray<share::ObServerInfoInTable> &servers_info,
      const ObIArray<ObUnitPlacementStrategy::ObServerResource> &server_resources,
      const char* module,
      const bool for_clone_tenant,
      int64_t &not_excluded_server_count,
      std::string &resource_not_enough_reason,
      ObIArray<ObUnitPlacementStrategy::ObServerResource> &valid_server_resources) const;

  int get_tenant_units_and_unit_group_ids_(
      ObISQLClient &client,
      const uint64_t tenant_id,
      common::ObIArray<share::ObUnit> &tenant_units,
      common::ObIArray<uint64_t> &unit_group_id_array);
  // arrange unit related
  int allocate_new_pool_units_(
      common::ObISQLClient &client,
      const share::ObResourcePool &pool,
      const char *module);
  int increase_units_in_zones_(common::ObISQLClient &client,
      share::ObResourcePool &pool,
      const common::ObIArray<common::ObZone> &to_be_add_zones,
      const char *module);
  int expand_pool_unit_num_(
      common::ObMySQLTransaction &trans,
      share::ObResourcePool *pool,
      const int64_t unit_num);
  int check_enough_resource_for_delete_server_(
      const ObAddr &server,
      const ObZone &zone,
      const ObIArray<share::ObServerInfoInTable> &servers_info,
      const ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info);
  static int order_report_servers_resource_info_(
    const ObIArray<share::ObServerInfoInTable> &servers_info,
    const ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info,
    ObIArray<obrpc::ObGetServerResourceInfoResult> &ordered_report_servers_resource_info);

  int check_server_have_enough_resource_for_delete_server_(
      const ObUnitLoad &unit_load,
      const common::ObZone &zone,
      const ObIArray<share::ObServerInfoInTable> &servers_info,
      ObIArray<ObUnitPlacementStrategy::ObServerResource> &initial_servers_resource,
      std::string &resource_not_enough_reason);
  int compute_server_resource_(
    const obrpc::ObGetServerResourceInfoResult &report_server_resource_info,
    ObUnitPlacementStrategy::ObServerResource &server_resource) const;
  int build_server_resources_(
      const ObIArray<obrpc::ObGetServerResourceInfoResult> &report_servers_resource_info,
      ObIArray<ObUnitPlacementStrategy::ObServerResource> &initial_server_resource) const;
  int do_choose_server_for_unit_(const share::ObUnitResource &config,
      const ObZone &zone,
      const ObArray<ObAddr> &excluded_servers,
      const ObIArray<share::ObServerInfoInTable> &servers_info,
      const ObIArray<ObUnitPlacementStrategy::ObServerResource> &server_resources,
      const char *module,
      ObAddr &server,
      std::string &resource_not_enough_reason) const;
  bool check_resource_enough_for_unit_(
      const ObUnitPlacementStrategy::ObServerResource &r,
      const share::ObUnitResource &u,
      const double hard_limit,
      ObResourceType &not_enough_resource,
      AlterResourceErr &not_enough_resource_config) const;
  static int check_tenant_exist_(
         const uint64_t tenant_id,
         bool &is_exist);
  static int read_parameter_from_seed_tenant_(
         const char * parameter_name,
         ObString &parameter_value);
  //LOCK IN
  int commit_alter_unit_num_in_trans_(
    const common::ObIArray<share::ObResourcePool *> &pools,
    common::ObMySQLTransaction &trans,
    common::ObIArray<uint64_t> &adding_unit_ids,
    common::ObIArray<common::ObArray<uint64_t>> &pools_deleting_unit_ids);
  // tools
  const char *alter_resource_err_to_str(AlterResourceErr err) const
  {
    const char *str = "UNKNOWN";
    switch (err) {
      case MIN_CPU: { str = "MIN_CPU"; break; }
      case MAX_CPU: { str = "MAX_CPU"; break; }
      case MEMORY: { str = "MEMORY_SIZE"; break; }
      case LOG_DISK: { str = "LOG_DISK_SIZE"; break; }
      case DATA_DISK: { str = "DATA_DISK_SIZE"; break; }
      default: { str = "UNKNOWN"; break; }
    }
    return str;
  }
  int check_tenant_enable_logonly_replica_(const uint64_t tenant_id, bool &enable_logonly_replica);

private:
  bool inited_;
  bool loaded_;
  common::ObMySQLProxy *proxy_;
  common::ObServerConfig *server_config_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;
  ObServerManager &server_mgr_;
  ObZoneManager &zone_mgr_;
  share::ObUnitTableOperator ut_operator_;
  common::hash::ObHashMap<uint64_t, share::ObUnitConfig *> id_config_map_;
  common::hash::ObHashMap<share::ObUnitConfigName, share::ObUnitConfig *> name_config_map_;
  common::hash::ObHashMap<uint64_t, int64_t> config_ref_count_map_;
  common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool *> *> config_pools_map_;
  common::ObPooledAllocator<common::ObArray<share::ObResourcePool *> > config_pools_allocator_;
  common::ObPooledAllocator<share::ObUnitConfig> config_allocator_;
  IdPoolMap id_pool_map_;
  common::hash::ObHashMap<share::ObResourcePoolName, share::ObResourcePool *> name_pool_map_;
  common::ObPooledAllocator<share::ObResourcePool> pool_allocator_;
  common::hash::ObHashMap<uint64_t, common::ObArray<share::ObUnit *> *> pool_unit_map_;
  common::ObPooledAllocator<common::ObArray<share::ObUnit *> > pool_unit_allocator_;
  common::hash::ObHashMap<uint64_t, share::ObUnit *> id_unit_map_;
  // NOTE:
  // unit_group_id in inner_table is recorded in __all_unit.
  // Now this column can be updated both in ObUnitMananger and TenantBalanceService,
  //  so ObUnit::unit_group_id_ in_memory can not ensure accuracy.
  // We only maintain this value in inner_table, and if there's any need to read this value,
  //  read it from inner_table instead of memory.
  common::ObPooledAllocator<share::ObUnit> allocator_;
  common::hash::ObHashMap<common::ObAddr, common::ObArray<ObUnitLoad> *> server_loads_;
  common::ObPooledAllocator<common::ObArray<ObUnitLoad> > load_allocator_;
  TenantPoolsMap tenant_pools_map_;
  common::ObPooledAllocator<common::ObArray<share::ObResourcePool *> > tenant_pools_allocator_;
  common::hash::ObHashMap<common::ObAddr, common::ObArray<uint64_t> *> server_migrate_units_map_;
  common::ObPooledAllocator<common::ObArray<uint64_t> > migrate_units_allocator_;
  common::SpinRWLock lock_;
  ObRootService *root_service_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObRootBalancer *root_balance_;
  DISALLOW_COPY_AND_ASSIGN(ObUnitManager);
};

template<typename SCHEMA>
int ObUnitManager::check_schema_zone_unit_enough(
    const common::ObZone &zone,
    const int64_t total_unit_num,
    const int64_t full_unit_num,
    const int64_t logonly_unit_num,
    const SCHEMA &schema,
    share::schema::ObSchemaGetterGuard &schema_guard,
    bool &enough)
{
  int ret = OB_SUCCESS;
  enough = true;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat_())) {
    RS_LOG(WARN, "variable is not init", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret), K(zone));
  } else if (OB_FAIL(inner_check_schema_zone_unit_enough(
          zone, total_unit_num, full_unit_num, logonly_unit_num,
          schema, schema_guard, enough))) {
    RS_LOG(WARN, "fail to inner check schema zone unit enough", K(ret));
  }
  return ret;
}

template <typename SCHEMA>
int ObUnitManager::inner_check_schema_zone_unit_enough(
    const common::ObZone &zone,
    const int64_t total_unit_num,
    const int64_t full_unit_num,
    const int64_t logonly_unit_num,
    const SCHEMA &schema,
    share::schema::ObSchemaGetterGuard &schema_guard,
    bool &enough)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObZoneReplicaNumSet> zone_locality_array;
  enough = true;
  UNUSED(logonly_unit_num);
  if (OB_FAIL(check_inner_stat_())) {
    RS_LOG(WARN, "variable is not init", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret), K(zone));
  } else if (OB_FAIL(schema.get_zone_replica_attr_array_inherit(schema_guard, zone_locality_array))) {
    RS_LOG(WARN, "fail to get zone replica num array", K(ret));
  } else {
    bool find = false;
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < zone_locality_array.count(); ++i) {
      share::ObZoneReplicaNumSet &num_set = zone_locality_array.at(i);
      if (zone != num_set.zone_) {
        // go on next
      } else {
        find = true;
        int64_t full_and_readonly_num
            = num_set.get_full_replica_num()
              + (num_set.get_readonly_replica_num() == ObLocalityDistribution::ALL_SERVER_CNT
                  ? 0 : num_set.get_readonly_replica_num());
        if (total_unit_num < num_set.get_specific_replica_num()) {
          // The total number of unit num is less than the number of specific replica num,
          // which is not enough.
          enough = false;
        } else if (full_unit_num < full_and_readonly_num) {
          enough = false;
        }
        break;
      }
    }
    if (OB_FAIL(ret)) {
      // bypass
    } else if (!find) { // no zone locality exist, this is enough
      enough = true;
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_UNIT_MANAGER_H_
