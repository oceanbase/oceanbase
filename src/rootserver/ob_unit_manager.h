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
#include "share/ob_unit_getter.h"
#include "share/ob_unit_stat.h"
#include "share/ob_unit_table_operator.h"
#include "share/ob_worker.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_unit_placement_strategy.h"
namespace oceanbase {
namespace common {
class ObISQLClient;
class ObServerConfig;
class ObMySQLTransaction;
}  // namespace common
namespace share {
class ObServerStatus;
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share
namespace rootserver {
class ObZoneManager;
class ObUnitPlacementStrategy;
class ObRebalanceTaskMgr;
class ServerBalancer;
class UnitStat;
class ObFetchPrimaryDDLOperator;
class ObRootBalancer;
class ObUnitManager {
public:
  friend class ServerBalancer;
  friend class ObServerBalancer;
  friend class ObRsGtsUnitDistributor;
  struct ZoneUnit {
    ZoneUnit() : zone_(), unit_infos_()
    {}
    ~ZoneUnit()
    {}

    bool is_valid() const
    {
      return !zone_.is_empty() && unit_infos_.count() > 0;
    }
    void reset()
    {
      zone_.reset();
      unit_infos_.reset();
    }
    int assign(const ZoneUnit& other);

    TO_STRING_KV(K_(zone), K_(unit_infos));

  public:
    common::ObZone zone_;
    common::ObArray<share::ObUnitInfo> unit_infos_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ZoneUnit);
  };

  struct ObUnitLoad : public ObIServerResourceDemand {
    ObUnitLoad() : unit_(NULL), unit_config_(NULL), pool_(NULL)
    {}
    ~ObUnitLoad()
    {}

    inline bool is_valid() const
    {
      return NULL != pool_ && NULL != unit_ && NULL != unit_config_;
    }
    // return -1 if resource_type is invalid
    virtual double get_demand(ObResourceType resource_type) const override;
    inline bool is_sys_unit() const
    {
      return NULL != pool_ && common::OB_SYS_TENANT_ID == pool_->tenant_id_;
    }
    TO_STRING_KV(KP_(unit), KP_(unit_config), KP_(pool));

  public:
    share::ObUnit* unit_;
    share::ObUnitConfig* unit_config_;
    share::ObResourcePool* pool_;
  };
  struct ObServerLoad;
  struct ObUnitLoadOrder {
    explicit ObUnitLoadOrder(int& ret, ObServerLoad& server_load, double* weights, int64_t weights_count)
        : ret_(ret), server_load_(server_load), weights_(weights), weights_count_(weights_count)
    {}
    ~ObUnitLoadOrder()
    {}

    bool operator()(const ObUnitLoad& left, const ObUnitLoad& right);

    int& ret_;
    ObServerLoad& server_load_;
    double* weights_;
    int64_t weights_count_;
  };

  // @todo move this class out of this file
  struct ObServerLoad : public ObIServerResource {
    ObServerLoad() : sum_load_(), status_(), unit_loads_(), mark_delete_indexes_(), all_normal_unit_(false)
    {}
    ~ObServerLoad()
    {}

    bool is_valid() const;
    void reset();

    int get_load(double* weights, int64_t weights_count, double& load) const;
    int get_load_if_add(double* weights, int64_t weights_count, const share::ObUnitConfig& config, double& load) const;

    int build(const common::ObArray<ObUnitLoad>* unit_loads, const share::ObServerStatus& status);
    int mark_delete(const int64_t index);
    int add_load(const ObUnitLoad& load);
    int subtract_last_unit(ObUnitLoad& unit_load);
    int assign(const ObServerLoad& other);

    // return -1 if resource_type is invalid
    virtual double get_assigned(ObResourceType resource_type) const override;
    virtual double get_capacity(ObResourceType resource_type) const override;
    virtual double get_max_assigned(ObResourceType resource_type) const override;

    TO_STRING_KV(K_(sum_load), K_(status), K_(all_normal_unit), K_(mark_delete_indexes));

    share::ObUnitConfig sum_load_;
    share::ObServerStatus status_;
    common::ObArray<ObUnitLoad> unit_loads_;  // unit loads on one server, sorted desc
    common::ObArray<int64_t> mark_delete_indexes_;
    bool all_normal_unit_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObServerLoad);
  };

  struct ObServerLoadOrder {
    explicit ObServerLoadOrder(int& ret, double* weights, int64_t weights_count)
        : ret_(ret), weights_(weights), weights_count_(weights_count)
    {}
    ~ObServerLoadOrder()
    {}

    bool operator()(const ObServerLoad* left, const ObServerLoad* right);

    int& ret_;
    double* weights_;
    int64_t weights_count_;
  };

  enum EndMigrateOp {
    COMMIT,
    ABORT,
    REVERSE,
  };
  typedef common::hash::ObHashMap<uint64_t, share::ObResourcePool*> IdPoolMap;
  typedef common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool*>*> TenantPoolsMap;

public:
  ObUnitManager(ObServerManager& server_mgr, ObZoneManager& zone_mgr);
  virtual ~ObUnitManager();

  int init(common::ObMySQLProxy& proxy, common::ObServerConfig& server_config, ObILeaderCoordinator& leader_coordinator,
      share::schema::ObMultiVersionSchemaService& schema_service, ObRootBalancer& root_balance);
  virtual bool check_inner_stat() const
  {
    return inited_ && loaded_;
  }
  virtual int load();
  common::SpinRWLock& get_lock()
  {
    return lock_;
  }
  void set_unit_placement_strategy(ObUnitPlacementStrategy* s)
  {
    unit_placement_strategy_ = s;
  }
  common::ObMySQLProxy& get_sql_proxy()
  {
    return *proxy_;
  }

  // unit config related
  virtual int create_unit_config(share::ObUnitConfig& unit_config, const bool if_not_exist);
  virtual int drop_unit_config(const share::ObUnitConfigName& name, const bool if_exist);
  virtual int alter_unit_config(share::ObUnitConfig& unit_config);

  // resource pool related
  virtual int check_tenant_pools_in_shrinking(const uint64_t tenant_id, bool& is_shrinking);
  virtual int check_pool_in_shrinking(const uint64_t pool_id, bool& is_shrinking);
  virtual int create_resource_pool(
      share::ObResourcePool& resource_pool, const share::ObUnitConfigName& config_name, const bool if_not_exist);
  virtual int upgrade_cluster_create_ha_gts_util();
  virtual int get_zone_pools_unit_num(const common::ObZone& zone,
      const common::ObIArray<share::ObResourcePoolName>& new_pool_name_list, int64_t& total_unit_num,
      int64_t& full_unit_num, int64_t& logonly_unit_num);
  virtual int alter_resource_pool(const share::ObResourcePool& alter_pool, const share::ObUnitConfigName& config_name,
      const common::ObIArray<uint64_t>& delete_unit_id_array);
  virtual int drop_resource_pool(const share::ObResourcePoolName& name, const bool if_exist);
  virtual int get_resource_pool_by_name(const share::ObResourcePoolName& name, share::ObResourcePool& pool) const;
  // Delete resource pool and associated unit
  virtual int remove_resource_pool_unit_in_trans(const int64_t resource_pool_id, common::ObMySQLTransaction& trans);
  // Delete the resource pool and associated unit in memory, and other memory structures
  virtual int delete_resource_pool_unit(share::ObResourcePool* pool);
  virtual int drop_standby_resource_pool(
      const common::ObIArray<share::ObResourcePoolName>& pool_names, common::ObMySQLTransaction& trans);
  virtual int commit_drop_standby_resource_pool(const common::ObIArray<share::ObResourcePoolName>& pool_names);
  virtual int split_resource_pool(const share::ObResourcePoolName& pool_name,
      const common::ObIArray<common::ObString>& split_pool_list, const common::ObIArray<common::ObZone>& zone_list);
  virtual int merge_resource_pool(
      const common::ObIArray<common::ObString>& old_pool_list, const common::ObIArray<common::ObString>& new_pool_list);
  virtual int check_locality_for_logonly_unit(const share::schema::ObTenantSchema& tenant_schema,
      const common::ObIArray<share::ObResourcePoolName>& pool_names, bool& is_permitted);
  virtual int grant_pools(common::ObISQLClient& client, const share::ObWorker::CompatMode compat_mode,
      const common::ObIArray<share::ObResourcePoolName>& pool_names, const uint64_t tenant_id,
      const bool is_bootstrap = false, const bool if_not_grant = false, const bool skip_offline_server = false);
  virtual int grant_pools_for_standby(common::ObISQLClient& client,
      const common::ObIArray<share::ObResourcePoolName>& pool_names, const uint64_t tenant_id);
  virtual int revoke_pools(common::ObISQLClient& client, const common::ObIArray<share::ObResourcePoolName>& pool_names,
      const uint64_t tenant_id);
  virtual int commit_change_pool_owner(
      const bool grant, const common::ObIArray<share::ObResourcePoolName>& pool_names, const uint64_t tenant_id);
  virtual int get_pool_ids_of_tenant(const uint64_t tenant_id, common::ObIArray<uint64_t>& pool_ids) const;
  virtual int get_pool_names_of_tenant(
      const uint64_t tenant_id, common::ObIArray<share::ObResourcePoolName>& pool_names) const;
  virtual int get_unit_config_by_pool_name(const common::ObString& pool_name, share::ObUnitConfig& unit_config) const;
  virtual int get_pools_of_tenant(const uint64_t tenant_id, common::ObIArray<share::ObResourcePool>& pools) const;
  virtual int get_zones_of_pools(
      const common::ObIArray<share::ObResourcePoolName>& pool_names, common::ObIArray<common::ObZone>& zones) const;
  virtual int get_pool_id(const share::ObResourcePoolName& pool_name, uint64_t& pool_id) const;
  virtual int get_pools(common::ObIArray<share::ObResourcePool>& pools) const;
  virtual int create_sys_units(const common::ObIArray<share::ObUnit>& sys_units);
  virtual int create_gts_units(const common::ObIArray<share::ObUnit>& gts_units);
  virtual int get_tenant_pool_zone_list(const uint64_t tenant_id, common::ObIArray<common::ObZone>& zone_list) const;
  // migrate units on server to other servers, invoked when try delete server
  // virtual int migrate_out_units(const common::ObAddr &server,
  //    const common::ObIArray<common::ObAddr> &excluded_servers, bool &empty);
  // cancel migrate units on server to other servers
  virtual int cancel_migrate_out_units(const common::ObAddr& server);

  virtual int check_server_empty(const common::ObAddr& server, bool& is_empty) const;
  virtual int finish_migrate_unit(const uint64_t unit_id);
  virtual int finish_migrate_unit_not_in_tenant(share::ObResourcePool* pool, int64_t& total_task_cnt);
  virtual int finish_migrate_unit_not_in_locality(uint64_t tenant_id, share::schema::ObSchemaGetterGuard* schema_guard,
      ObArray<common::ObZone> zone_list, int64_t& task_cnt);

  bool has_migrating_unit() const
  {
    return server_migrate_units_map_.size() != 0;
  }
  void dump();

  virtual int get_unit_infos_of_pool(
      const uint64_t resource_pool_id, common::ObIArray<share::ObUnitInfo>& unit_infos) const;
  virtual int get_deleting_units_of_pool(const uint64_t resource_pool_id, common::ObIArray<share::ObUnit>& units) const;
  virtual int commit_shrink_resource_pool(const uint64_t resource_pool_id);
  virtual int get_active_unit_infos_of_pool(
      const uint64_t resource_pool_id, common::ObIArray<share::ObUnitInfo>& unit_infos) const;
  virtual int get_all_unit_infos_by_tenant(const uint64_t tenant_id, common::ObIArray<share::ObUnitInfo>& unit_infos);
  virtual int get_all_gts_unit_infos(common::ObIArray<share::ObUnitInfo>& unit_infos);
  virtual int get_active_unit_infos_by_tenant(
      const uint64_t tenant_id, common::ObIArray<share::ObUnitInfo>& unit_infos);
  virtual int get_active_unit_infos_by_tenant(
      const share::schema::ObTenantSchema& tenant_schema, common::ObIArray<share::ObUnitInfo>& unit_infos) const;
  virtual int get_zone_active_unit_infos_by_tenant(
      const uint64_t tenant_id, const common::ObZone& zone, common::ObIArray<share::ObUnitInfo>& unit_infos) const;
  virtual int get_zone_alive_unit_infos_by_tenant(
      const uint64_t tenant_id, const common::ObZone& zone, common::ObIArray<share::ObUnitInfo>& unit_infos) const;
  virtual int get_zone_active_unit_infos_by_tenant(const uint64_t tenant_id,
      const common::ObIArray<common::ObZone>& tenant_zone_list, common::ObIArray<share::ObUnitInfo>& unit_info) const;
  virtual int get_unit_infos(
      const common::ObIArray<share::ObResourcePoolName>& pools, common::ObIArray<share::ObUnitInfo>& unit_infos);

  virtual int get_unit_ids(common::ObIArray<uint64_t>& unit_ids) const;
  virtual int get_logonly_unit_ids(common::ObIArray<uint64_t>& unit_ids) const;
  virtual int get_logonly_unit_by_tenant(
      const int64_t tenant_id, common::ObIArray<share::ObUnitInfo>& logonly_unit_infos);
  virtual int get_logonly_unit_by_tenant(share::schema::ObSchemaGetterGuard& schema_guard, const int64_t tenant_id,
      common::ObIArray<share::ObUnitInfo>& logonly_unit_infos);
  virtual int get_unit_info_by_id(const uint64_t unit_id, share::ObUnitInfo& unit_info) const;
  virtual int get_unit(const uint64_t tenant_id, const common::ObAddr& server, share::ObUnit& unit) const;
  virtual int get_tenants_of_server(
      const common::ObAddr& server, common::hash::ObHashSet<uint64_t>& tenant_id_set) const;
  virtual int check_tenant_on_server(const uint64_t tenant_id, const common::ObAddr& server, bool& on_server) const;

  virtual int admin_migrate_unit(
      const uint64_t unit_id, const common::ObAddr& dst, ObRebalanceTaskMgr* task_mgr, bool is_cancel = false);
  int check_enough_resource_for_delete_server(const ObAddr& server, const ObZone& zone);
  template <typename SCHEMA>
  int check_schema_zone_unit_enough(const common::ObZone& zone, const int64_t total_unit_num,
      const int64_t full_unit_num, const int64_t logonly_unit_num, const SCHEMA& schema,
      share::schema::ObSchemaGetterGuard& schema_guard, bool& enough);
  int check_pools_unit_legality_for_locality(const common::ObIArray<share::ObResourcePoolName>& pools,
      const common::ObIArray<common::ObZone>& schema_zone_list,
      const common::ObIArray<share::ObZoneReplicaNumSet>& zone_locality, bool& is_legal);
  // get all server loads
  int get_server_loads(
      const common::ObZone& zone, common::ObArray<ObServerLoad>& server_loads, double* weights, int64_t weights_count);
  static int calc_sum_load(const common::ObArray<ObUnitLoad>* unit_loads, share::ObUnitConfig& sum_load);
  // get hard limit
  int get_hard_limit(double& hard_limit) const;
  int try_cancel_migrate_unit(const share::ObUnit& unit);
  int distrubte_for_unit_intersect(const uint64_t tenant_id, const common::ObIArray<share::ObResourcePoolName>& pools);

  ////////////////////////////////////////////////////////////////
private:
  static const int64_t UNIT_MAP_BUCKET_NUM = 1024 * 64;
  static const int64_t CONFIG_MAP_BUCKET_NUM = 1024;
  static const int64_t CONFIG_REF_COUNT_MAP_BUCKET_NUM = 1024;
  static const int64_t CONFIG_POOLS_MAP_BUCKET_NUM = 1024;
  static const int64_t POOL_MAP_BUCKET_NUM = 1024;
  static const int64_t UNITLOAD_MAP_BUCKET_NUM = 1024;
  static const int64_t TENANT_POOLS_MAP_BUCKET_NUM = 1024;
  static const int64_t SERVER_MIGRATE_UNITS_MAP_BUCKET_NUM = 1024;
  static const int64_t SERVER_REF_COUNT_MAP_BUCKET_NUM = 1024;

  struct ZoneUnitPtr {
    ZoneUnitPtr() : zone_(), unit_ptrs_()
    {}
    ~ZoneUnitPtr()
    {}

    bool is_valid() const
    {
      return !zone_.is_empty() && unit_ptrs_.count() > 0;
    }
    void reset()
    {
      zone_.reset();
      unit_ptrs_.reset();
    }
    int assign(const ZoneUnitPtr& other);
    int sort_by_unit_id_desc();

    TO_STRING_KV(K_(zone), K_(unit_ptrs));

  public:
    common::ObZone zone_;
    common::ObSEArray<share::ObUnit*, 16> unit_ptrs_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ZoneUnitPtr);
  };

  class UnitGroupIdCmp {
  public:
    UnitGroupIdCmp() : ret_(common::OB_SUCCESS)
    {}
    ~UnitGroupIdCmp()
    {}
    bool operator()(const share::ObUnit* left, const share::ObUnit* right)
    {
      bool bool_ret = false;
      if (common::OB_SUCCESS != ret_) {
      } else if (OB_UNLIKELY(NULL == left || NULL == right)) {
        ret_ = common::OB_ERR_UNEXPECTED;
      } else if (left->unit_id_ > right->unit_id_) {
        bool_ret = true;
      } else {
        bool_ret = false;
      }
      return bool_ret;
    }
    int get_ret() const
    {
      return ret_;
    }

  private:
    int ret_;
  };

  enum AlterUnitNumType {
    AUN_SHRINK = 0,
    AUN_ROLLBACK_SHRINK,
    AUN_EXPEND,
    AUN_NOP,
    AUN_MAX,
  };
  enum AlterResourceErr { MIN_CPU = 0, MAX_CPU, MIN_MEM, MAX_MEM, ALT_ERR };

protected:
  static const int64_t NOTIFY_RESOURCE_RPC_TIMEOUT = 9 * 1000000;  // 9 second
  // for ObServerBalancer
  IdPoolMap& get_id_pool_map()
  {
    return id_pool_map_;
  }
  TenantPoolsMap& get_tenant_pools_map()
  {
    return tenant_pools_map_;
  }
  // Before attempting to migrate the unit,
  // check whether the target unit space is sufficient,
  // if it is insufficient, do not migrate,
  // and return OB_OP_NOT_ALLOW
  int try_migrate_unit(const uint64_t unit_id, const share::ObUnitStat& unit_stat,
      const common::ObIArray<share::ObUnitStat>& migrating_unit_stat, const common::ObAddr& dst,
      const bool is_manual = false);
  int get_zone_units(const common::ObArray<share::ObResourcePool*>& pools, common::ObArray<ZoneUnit>& zone_units) const;
  virtual int end_migrate_unit(const uint64_t unit_id, const EndMigrateOp end_migrate_op = COMMIT);
  int get_excluded_servers(
      const share::ObUnit& unit, const share::ObUnitStat& unit_stat, common::ObIArray<common::ObAddr>& servers) const;

  int get_excluded_servers(const share::ObUnit& unit, common::ObIArray<common::ObAddr>& servers) const;
  int choose_server_for_unit(const share::ObUnitConfig& config, const common::ObZone& zone,
      const common::ObArray<common::ObAddr>& excluded_servers, common::ObAddr& server) const;
  int inner_choose_server_for_unit(const share::ObUnitConfig& config, const common::ObZone& zone,
      const common::ObArray<common::ObAddr>& excluded_servers, common::ObAddr& server) const;
  // get soft limit
  int get_soft_limit(double& soft_limit) const;
  int get_server_loads_internal(const common::ObZone& zone, const bool only_active,
      common::ObArray<ObServerLoad>& server_loads, double& sum_load, int64_t& alive_server_count, double* weights,
      int64_t weights_count);
  int check_unit_group_normal(const share::ObUnit& unit, bool& normal);
  int check_can_migrate_in(const common::ObAddr& server, bool& can_migrate_in) const;
  int get_migrate_units_by_server(const ObAddr& server, common::ObIArray<uint64_t>& migrate_units) const;
  int try_cancel_migrate_unit(const share::ObUnit& unit, bool& is_canceled);
  //////end of server_balance

  static int copy_config_options(const share::ObUnitConfig& src, share::ObUnitConfig& dst);
  static int check_duplicated_server(const common::ObIArray<common::ObAddr>& servers, bool& duplicated_exist);
  static int check_bootstrap_pool(const share::ObResourcePool& pool);

  int check_has_intersect_pg(const share::ObUnit& a, const share::ObUnit& b, bool& intersect);
  int have_enough_resource(const share::ObServerStatus& server_status, const share::ObUnitConfig& unit_config,
      const double limit, bool& is_enough, AlterResourceErr& err_index) const;
  virtual int migrate_unit(const uint64_t unit_id, const common::ObAddr& dst, const bool is_manual = false);
  int inner_get_unit_info_by_id(const uint64_t unit_id, share::ObUnitInfo& unit) const;

  int check_server_enough(
      const uint64_t tenant_id, const common::ObIArray<share::ObResourcePoolName>& pool_names, bool& enough);

  int inner_get_active_unit_infos_of_tenant(const uint64_t tenant_id, common::ObIArray<share::ObUnitInfo>& unit_info);
  int inner_get_active_unit_infos_of_tenant(
      const share::schema::ObTenantSchema& tenant_schema, common::ObIArray<share::ObUnitInfo>& unit_info);

  int filter_logonly_task(
      const common::ObIArray<share::ObResourcePool*>& pools, common::ObIArray<share::ObZoneReplicaNumSet>& zone_task);

  int inner_get_unit_infos_of_pool(
      const uint64_t resource_pool_id, common::ObIArray<share::ObUnitInfo>& unit_infos) const;

  int inner_get_zone_alive_unit_infos_by_tenant(
      const uint64_t tenant_id, const common::ObZone& zone, common::ObIArray<share::ObUnitInfo>& unit_infos) const;

  int inner_get_pool_ids_of_tenant(const uint64_t tenant_id, ObIArray<uint64_t>& pool_ids) const;

  int create_sys_unit(common::ObISQLClient& client, const share::ObResourcePool& pool);

  // we may set min_xxx in this function, so config not const
  int check_unit_config(const share::ObUnitConfig& rpc_config, share::ObUnitConfig& config) const;
  int check_resource_pool(share::ObResourcePool& resource_pool) const;
  // arrange unit related
  int allocate_pool_units(common::ObISQLClient& client, const share::ObResourcePool& pool);
  int get_tenant_unit_servers(const uint64_t tenant_id, const common::ObZone& zone,
      const common::ObIArray<uint64_t>& processed_pool_id_array, common::ObIArray<common::ObAddr>& server_array);
  int allocate_unit_groups(common::ObISQLClient& client, const share::ObResourcePool& pool,
      const bool new_allocate_pool, const int64_t increase_delta_unit_num,
      common::ObIArray<common::ObAddr>& new_servers);
  int get_pool_servers(
      const uint64_t resource_pool_id, const common::ObZone& zone, common::ObIArray<common::ObAddr>& servers) const;
  int get_pools_servers(const common::ObIArray<share::ObResourcePool*>& pools,
      common::hash::ObHashMap<common::ObAddr, int64_t>& server_ref_count_map) const;
  int calc_load_if_plus(
      const share::ObServerStatus& server_status, const share::ObUnitConfig& config, double& load) const;
  int add_unit(common::ObISQLClient& client, const share::ObUnit& unit);
  // load balance related
  int inner_get_unit_ids(common::ObIArray<uint64_t>& unit_ids) const;
  int inner_get_tenant_zone_full_unit_num(const int64_t tenant_id, const common::ObZone& zone, int64_t& unit_num);
  int inner_get_tenant_pool_zone_list(const uint64_t tenant_id, common::ObIArray<common::ObZone>& zone_list) const;
  int get_tenant_zone_all_unit_loads(
      const int64_t tenant_id, const common::ObZone& zone, common::ObIArray<ObUnitManager::ObUnitLoad>& unit_loads);
  int get_tenant_zone_unit_loads(const int64_t tenant_id, const common::ObZone& zone,
      const common::ObReplicaType replica_type, common::ObIArray<ObUnitManager::ObUnitLoad>& unit_loads);
  int register_shrink_pool_unit_num_rs_job(const uint64_t resource_pool_id, const uint64_t tenant_id,
      const int64_t new_unit_num, common::ObMySQLTransaction& trans);
  int rollback_shrink_pool_unit_num_rs_job(
      const uint64_t resource_pool_id, const uint64_t tenant_id, common::ObMySQLTransaction& trans);
  int complete_shrink_pool_unit_num_rs_job(
      const uint64_t resource_pool_id, const uint64_t tenant_id, common::ObMySQLTransaction& trans);
  int complete_migrate_unit_rs_job_in_pool(
      const int64_t resource_pool_id, const int result_ret, common::ObMySQLTransaction& trans);
  int check_unit_can_migrate(const uint64_t tenant_id, bool& can_migrate);

  // alter pool related
  struct UnitNum {
    UnitNum() : full_unit_num_(0), logonly_unit_num_(0)
    {}
    UnitNum(const int64_t full_unit_num, const int64_t logonly_unit_num)
        : full_unit_num_(full_unit_num), logonly_unit_num_(logonly_unit_num)
    {}
    TO_STRING_KV(K_(full_unit_num), K_(logonly_unit_num));
    void reset()
    {
      full_unit_num_ = 0;
      logonly_unit_num_ = 0;
    }
    int64_t full_unit_num_;
    int64_t logonly_unit_num_;
  };

  int inner_check_single_logonly_pool_for_locality(const share::ObResourcePool& pool,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, bool& is_legal);
  int inner_check_logonly_pools_for_locality(const common::ObIArray<share::ObResourcePool*>& pools,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, bool& is_legal);
  int inner_check_pools_unit_num_enough_for_locality(const common::ObIArray<share::ObResourcePool*>& pools,
      const common::ObIArray<common::ObZone>& schema_zone_list,
      const common::ObIArray<share::ObZoneReplicaNumSet>& zone_locality, bool& is_enough);
  int alter_pool_unit_config(share::ObResourcePool* pool, const share::ObUnitConfigName& config_name);
  int get_newest_schema_guard_in_inner_table(
      const uint64_t tenant_id, share::schema::ObSchemaGetterGuard& schema_guard);
  int alter_pool_unit_num(
      share::ObResourcePool* pool, int64_t unit_num, const common::ObIArray<uint64_t>& delete_unit_id_array);
  int determine_alter_unit_num_type(
      share::ObResourcePool* pool, const int64_t unit_num, AlterUnitNumType& alter_unit_num_type);
  int shrink_pool_unit_num(
      share::ObResourcePool* pool, const int64_t unit_num, const common::ObIArray<uint64_t>& delete_unit_id_array);
  int build_sorted_zone_unit_ptr_array(share::ObResourcePool* pool, common::ObIArray<ZoneUnitPtr>& zone_unit_ptrs);
  int check_shrink_unit_num_zone_condition(share::ObResourcePool* pool, const int64_t alter_unit_num,
      const common::ObIArray<uint64_t>& delete_unit_id_array);
  int fill_delete_unit_ptr_array(share::ObResourcePool* pool, const common::ObIArray<uint64_t>& delete_unit_id_array,
      const int64_t alter_unit_num, common::ObIArray<share::ObUnit*>& output_delete_unit_ptr_array);
  int shrink_not_granted_pool(
      share::ObResourcePool* pool, const int64_t unit_num, const common::ObIArray<uint64_t>& delete_unit_id_array);
  int shrink_granted_pool(
      share::ObResourcePool* pool, const int64_t unit_num, const common::ObIArray<uint64_t>& delete_unit_id_array);
  int check_shrink_granted_pool_allowed(share::ObResourcePool* pool, const int64_t unit_num, bool& is_allowed);
  int check_shrink_granted_pool_allowed_by_migrate_unit(
      share::ObResourcePool* pool, const int64_t unit_num, bool& is_allowed);
  int check_shrink_granted_pool_allowed_by_locality(
      share::ObResourcePool* pool, const int64_t unit_num, bool& is_allowed);
  int check_shrink_granted_pool_allowed_by_alter_locality(share::ObResourcePool* pool, bool& is_allowed);
  int do_check_shrink_granted_pool_allowed_by_locality(const common::ObIArray<share::ObResourcePool*>& pools,
      const common::ObIArray<common::ObZone>& schema_zone_list,
      const common::ObIArray<share::ObZoneReplicaNumSet>& zone_locality, const common::ObIArray<int64_t>& new_unit_nums,
      bool& is_allowed);
  int check_all_pools_granted(const common::ObIArray<share::ObResourcePool*>& pools, bool& all_granted);
  int check_shrink_granted_pool_allowed_by_tenant_locality(share::schema::ObSchemaGetterGuard& schema_guard,
      const uint64_t tenant_id, const common::ObIArray<share::ObResourcePool*>& pools,
      const common::ObIArray<int64_t>& new_unit_nums, bool& is_allowed);
  int check_shrink_granted_pool_allowed_by_single_partition_locality(
      const share::schema::ObPartitionSchema& partition_schema, share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<share::ObResourcePool*>& pools, const common::ObIArray<int64_t>& new_unit_nums,
      bool& is_allowed);
  int check_shrink_granted_pool_allowed_by_partition_locality(share::schema::ObSchemaGetterGuard& schema_guard,
      const uint64_t tenant_id, const common::ObIArray<share::ObResourcePool*>& pools,
      const common::ObIArray<int64_t>& new_unit_nums, bool& is_allowed);
  int rollback_shrink_pool_unit_num(share::ObResourcePool* pool, const int64_t unit_num);
  int expend_pool_unit_num(share::ObResourcePool* pool, const int64_t unit_num);
  int get_pool_complete_unit_num_and_status(
      share::ObResourcePool* pool, int64_t& unit_num_per_zone, bool& has_unit_num_modification);
  int alter_pool_zone_list(share::ObResourcePool* pool, const common::ObIArray<common::ObZone>& zone_list);
  int add_pool_zone_list(share::ObResourcePool* pool, const common::ObIArray<common::ObZone>& zone_list);
  int remove_pool_zone_list(share::ObResourcePool* pool, const common::ObIArray<common::ObZone>& zone_list);
  int cal_to_be_add_pool_zone_list(const common::ObIArray<common::ObZone>& prev_zone_list,
      const common::ObIArray<common::ObZone>& cur_zone_list, common::ObIArray<common::ObZone>& to_be_add_zones) const;
  int cal_to_be_removed_pool_zone_list(const common::ObIArray<common::ObZone>& prev_zone_list,
      const common::ObIArray<common::ObZone>& cur_zone_list,
      common::ObIArray<common::ObZone>& to_be_removed_zones) const;
  template <typename SCHEMA>
  int inner_check_schema_zone_unit_enough(const common::ObZone& zone, const int64_t total_unit_num,
      const int64_t full_unit_num, const int64_t logonly_unit_num, const SCHEMA& schema,
      share::schema::ObSchemaGetterGuard& schema_guard, bool& enough)
  {
    int ret = OB_SUCCESS;
    common::ObArray<share::ObZoneReplicaNumSet> zone_locality_array;
    enough = true;
    UNUSED(logonly_unit_num);
    if (!check_inner_stat()) {
      ret = OB_INNER_STAT_ERROR;
      RS_LOG(WARN, "variable is not init", K(ret));
    } else if (zone.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "invalid argument", K(ret), K(zone));
    } else if (OB_FAIL(schema.get_zone_replica_attr_array_inherit(schema_guard, zone_locality_array))) {
      RS_LOG(WARN, "fail to get zone replica num array", K(ret));
    } else {
      bool find = false;
      for (int64_t i = 0; !find && OB_SUCC(ret) && i < zone_locality_array.count(); ++i) {
        share::ObZoneReplicaNumSet& num_set = zone_locality_array.at(i);
        if (zone != num_set.zone_) {
          // go on next
        } else {
          find = true;
          int64_t full_and_readonly_num = num_set.get_full_replica_num() +
                                          (num_set.get_readonly_replica_num() == ObLocalityDistribution::ALL_SERVER_CNT
                                                  ? 0
                                                  : num_set.get_readonly_replica_num());
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
      } else if (!find) {  // no zone locality exist, this is enough
        enough = true;
      }
    }
    return ret;
  }
  int inner_get_zone_pools_unit_num(const common::ObZone& zone,
      const common::ObIArray<share::ObResourcePool*>& pool_list, int64_t& total_unit_num, int64_t& full_unit_num,
      int64_t& logonly_unit_num);
  int check_can_add_pool_zone_list(
      const share::ObResourcePool* pool, const common::ObIArray<common::ObZone>& to_be_add_zones, bool& can_add);
  int check_can_remove_pool_zone_list(
      const share::ObResourcePool* pool, const common::ObIArray<common::ObZone>& to_be_removed_zones, bool& can_remove);
  int do_add_pool_zone_list(share::ObResourcePool* pool, const common::ObIArray<common::ObZone>& new_zone_list,
      const common::ObIArray<common::ObZone>& to_be_add_zones);
  int do_remove_pool_zone_list(share::ObResourcePool* pool, const common::ObIArray<common::ObZone>& new_zone_list,
      const common::ObIArray<common::ObZone>& to_be_removed_zones);
  int check_full_resource_pool_memory_condition(
      const common::ObIArray<share::ObResourcePool*>& pools, const share::ObUnitConfig& new_config) const;
  int check_expand_config(const share::ObResourcePool& pool, const share::ObUnitConfig& config,
      const share::ObUnitConfig& new_config) const;
  int check_expand_config(const common::ObIArray<share::ObResourcePool*>& pools, const share::ObUnitConfig& old_config,
      const share::ObUnitConfig& new_config) const;
  int check_expand_config(const common::ObAddr& server, const share::ObUnitConfig& expand_config, bool& can_expand,
      AlterResourceErr& err_index) const;
  int check_shrink_config(const common::ObIArray<share::ObResourcePool*>& pools, const share::ObUnitConfig& old_config,
      const share::ObUnitConfig& new_config) const;
  int check_shrink_config(const share::ObResourcePool& pool, const share::ObUnitConfig& config,
      const share::ObUnitConfig& new_config) const;
  int check_shrink_memory(
      const share::ObResourcePool& pool, const int64_t min_memory, const int64_t new_min_memory) const;
  int change_pool_config(share::ObResourcePool* pool, share::ObUnitConfig* config, share::ObUnitConfig* new_config);
  int check_pool_intersect(
      const uint64_t tenant_id, const common::ObIArray<share::ObResourcePoolName>& pool_names, bool& intersect);
  // change resource pool owner
  int change_pool_owner(common::ObISQLClient& client, const share::ObWorker::CompatMode compat_mode, const bool grant,
      const common::ObIArray<share::ObResourcePoolName>& pool_names, const uint64_t tenant_id, const bool is_bootstrap,
      const bool if_not_grant = false, const bool skip_offline_server = false);

  // migrate all units on server to other servers
  //  int make_server_empty(const common::ObAddr &server,
  //                        const common::ObIArray<common::ObAddr> &excluded_servers,
  //                        const common::ObArray<ObUnitLoad> *loads);
  //
  int add_temp_tenant(const uint64_t pool_id, const common::ObIArray<common::ObAddr>& servers);

  // build hashmaps
  int build_unit_map(const common::ObIArray<share::ObUnit>& units);
  int build_config_map(const common::ObIArray<share::ObUnitConfig>& configs);
  int build_pool_map(const common::ObIArray<share::ObResourcePool>& pools);

  // insert into memory
  int insert_unit_config(share::ObUnitConfig* config);
  int inc_config_ref_count(const uint64_t config_id);
  int dec_config_ref_count(const uint64_t config_id);
  int update_pool_map(share::ObResourcePool* resource_pool);
  int insert_unit(share::ObUnit* unit);
  int insert_units_of_pool(common::ObArray<share::ObUnit*>* units);
  int insert_unit_loads(share::ObUnit* unit);
  int insert_unit_load(const common::ObAddr& server, const ObUnitLoad& load);
  int insert_load_array(const common::ObAddr& addr, common::ObArray<ObUnitLoad>* load);
  int update_pool_load(share::ObResourcePool* pool, share::ObUnitConfig* new_config);
  int update_unit_load(share::ObUnit* unit, share::ObResourcePool* new_pool);
  int gen_unit_load(share::ObUnit* unit, ObUnitLoad& load) const;
  int gen_unit_load(const uint64_t unit_id, ObUnitLoad& load) const;
  int insert_tenant_pool(const uint64_t tenant_id, share::ObResourcePool* resource_pool);
  int insert_config_pool(const uint64_t config_id, share::ObResourcePool* resource_pool);
  int insert_migrate_unit(const common::ObAddr& src_server, const uint64_t unit_id);

  // delete from memory
  int delete_unit_config(const uint64_t config_id, const share::ObUnitConfigName& config_name);
  int delete_resource_pool(const uint64_t pool_id, const share::ObResourcePoolName& pool_name);
  int delete_units_of_pool(const uint64_t resource_pool_id);
  int increase_units_in_zones(common::ObISQLClient& client, share::ObResourcePool* pool,
      const common::ObIArray<common::ObZone>& to_be_add_zones);
  int delete_units_in_zones(
      const uint64_t resource_pool_id, const common::ObIArray<common::ObZone>& to_be_removed_zones);
  int delete_inmemory_units(const uint64_t resource_pool_id, const common::ObIArray<uint64_t>& unit_ids);
  int delete_invalid_inmemory_units(const uint64_t resource_pool_id, const common::ObIArray<uint64_t>& valid_unit_ids);
  int delete_unit_loads(const share::ObUnit& unit);
  int delete_unit_load(const common::ObAddr& server, const uint64_t unit_id);
  int delete_tenant_pool(const uint64_t tenant_id, share::ObResourcePool* pool);
  int delete_config_pool(const uint64_t config_id, share::ObResourcePool* pool);
  int delete_migrate_unit(const common::ObAddr& src_server, const uint64_t unit_id);

  int get_unit_config_by_name(const share::ObUnitConfigName& name, share::ObUnitConfig*& config) const;
  int get_unit_config_by_id(const uint64_t config_id, share::ObUnitConfig*& config) const;
  // if not exist, return OB_ENTRY_NOT_EXIST
  int get_config_ref_count(const uint64_t config_id, int64_t& ref_count) const;
  int get_server_ref_count(common::hash::ObHashMap<common::ObAddr, int64_t>& map, const common::ObAddr& server,
      int64_t& server_ref_count) const;
  int set_server_ref_count(common::hash::ObHashMap<common::ObAddr, int64_t>& map, const common::ObAddr& server,
      const int64_t server_ref_count) const;
  int inner_get_resource_pool_by_name(const share::ObResourcePoolName& name, share::ObResourcePool*& pool) const;
  int get_resource_pool_by_id(const uint64_t pool_id, share::ObResourcePool*& pool) const;
  int get_units_by_pool(const uint64_t pood_id, common::ObArray<share::ObUnit*>*& units) const;
  int get_unit_by_id(const uint64_t unit_id, share::ObUnit*& unit) const;
  int get_loads_by_server(const common::ObAddr& server, common::ObArray<ObUnitLoad>*& loads) const;
  int get_pools_by_tenant(const uint64_t tenant_id, common::ObArray<share::ObResourcePool*>*& pools) const;
  int get_pools_by_config(const uint64_t tenant_id, common::ObArray<share::ObResourcePool*>*& pools) const;
  int get_migrate_units_by_server(const common::ObAddr& server, common::ObArray<uint64_t>*& migrate_units) const;
  int fetch_new_unit_config_id(uint64_t& unit_config_id);
  int fetch_new_resource_pool_id(uint64_t& resource_pool_id);
  int fetch_new_unit_id(uint64_t& unit_id);
  int extract_unit_ids(const common::ObIArray<share::ObUnit*>& units, common::ObIArray<uint64_t>& unit_ids);
  int try_notify_tenant_server_unit_resource(const uint64_t tenant_id,
      const bool is_delete, /*Expansion of semantics, possibly deleting resources*/
      ObNotifyTenantServerResourceProxy& notify_proxy, const share::ObResourcePool& new_pool,
      const share::ObWorker::CompatMode compat_mode, const share::ObUnit& unit, const bool if_not_grant,
      const bool skip_offline_server);
  static int64_t get_unit_min_memory();
  int check_server_have_enough_resource_for_delete_server(const ObUnitLoad& unit_load,
      const ObIArray<share::ObServerStatus>& statuses, const common::ObZone& zone,
      ObIArray<ObUnitPlacementStrategy::ObServerResource>& initial_servers_resource);
  int get_server_resource(const share::ObServerStatus& status, const share::ObUnitConfig& unit_config,
      ObUnitPlacementStrategy::ObServerResource& server_resource);
  int build_initial_servers_resources(const ObIArray<share::ObServerStatus>& statuses,
      ObIArray<ObUnitPlacementStrategy::ObServerResource>& initial_server_resource);
  int sum_servers_resources(
      ObUnitPlacementStrategy::ObServerResource& server_resource, const share::ObUnitConfig& unit_config);

protected:
  int inner_try_notify_tenant_server_unit_resource(const uint64_t tenant_id,
      const bool is_delete, /*Expansion of semantics, possibly deleting resources*/
      ObNotifyTenantServerResourceProxy& notify_proxy, const share::ObResourcePool& new_pool,
      const share::ObWorker::CompatMode compat_mode, const share::ObUnit& unit, const bool if_not_grant);
  int get_pools_by_id(const common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool*>*>& map,
      const uint64_t id, common::ObArray<share::ObResourcePool*>*& pools) const;
  int insert_id_pool(common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool*>*>& map,
      common::ObPooledAllocator<common::ObArray<share::ObResourcePool*> >& allocator, const uint64_t id,
      share::ObResourcePool* resource_pool);
  int insert_id_pool_array(common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool*>*>& map,
      const uint64_t id, common::ObArray<share::ObResourcePool*>* pools);
  int delete_id_pool(common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool*>*>& map,
      common::ObPooledAllocator<common::ObArray<share::ObResourcePool*> >& allocator, const uint64_t id,
      share::ObResourcePool* resource_pool);
  int cancel_migrate_unit(const share::ObUnit& unit, const bool is_gts_unit);
  int convert_pool_name_list(const common::ObIArray<common::ObString>& split_pool_list,
      common::ObIArray<share::ObResourcePoolName>& split_pool_name_list);
  int check_split_pool_name_condition(const common::ObIArray<share::ObResourcePoolName>& split_pool_name_list);
  int check_split_pool_zone_condition(
      const common::ObIArray<common::ObZone>& split_zone_list, const share::ObResourcePool& pool);
  int do_split_resource_pool(share::ObResourcePool* pool,
      const common::ObIArray<share::ObResourcePoolName>& split_pool_name_list,
      const common::ObIArray<common::ObZone>& split_zone_list);
  int do_split_pool_persistent_info(share::ObResourcePool* pool,
      const common::ObIArray<share::ObResourcePoolName>& split_pool_name_list,
      const common::ObIArray<common::ObZone>& split_zone_list,
      common::ObIArray<share::ObResourcePool*>& allocate_pool_ptrs);
  int do_split_pool_inmemory_info(
      share::ObResourcePool* pool, common::ObIArray<share::ObResourcePool*>& allocate_pool_ptrs);
  int fill_splitting_pool_basic_info(const share::ObResourcePoolName& new_pool_name, share::ObResourcePool* new_pool,
      const common::ObZone& zone, share::ObResourcePool* orig_pool);
  int split_pool_unit_persistent_info(common::ObMySQLTransaction& trans, const common::ObZone& zone,
      share::ObResourcePool* new_pool, share::ObResourcePool* orig_pool);
  int split_pool_unit_inmemory_info(
      const common::ObZone& zone, share::ObResourcePool* new_pool, share::ObResourcePool* orig_pool);
  int convert_pool_name_list(const common::ObIArray<common::ObString>& old_pool_list,
      common::ObIArray<share::ObResourcePoolName>& old_pool_name_list,
      const common::ObIArray<common::ObString>& new_pool_list, share::ObResourcePoolName& merge_pool_name);
  int check_merge_pool_name_condition(const share::ObResourcePoolName& merge_pool_name);
  int check_old_pool_name_condition(common::ObIArray<share::ObResourcePoolName>& old_pool_name_list,
      common::ObIArray<common::ObZone>& merge_zone_list, common::ObIArray<share::ObResourcePool*>& old_pool);
  int do_merge_resource_pool(const share::ObResourcePoolName& merge_pool_name,
      const common::ObIArray<common::ObZone>& merge_zone_list, common::ObIArray<share::ObResourcePool*>& old_pool);
  int do_merge_pool_persistent_info(share::ObResourcePool*& allocate_pool_ptr,
      const share::ObResourcePoolName& merge_pool_name, const common::ObIArray<common::ObZone>& merge_zone_list,
      const common::ObIArray<share::ObResourcePool*>& old_pool);
  int fill_merging_pool_basic_info(share::ObResourcePool*& allocate_pool_ptr,
      const share::ObResourcePoolName& merge_pool_name, const common::ObIArray<common::ObZone>& merge_zone_list,
      const common::ObIArray<share::ObResourcePool*>& old_pool);
  int merge_pool_unit_persistent_info(
      common::ObMySQLTransaction& trans, share::ObResourcePool* new_pool, share::ObResourcePool* orig_pool);
  int do_merge_pool_inmemory_info(
      share::ObResourcePool* new_pool /*allocate_pool_ptr*/, common::ObIArray<share::ObResourcePool*>& old_pool);
  int merge_pool_unit_inmemory_info(
      share::ObResourcePool* new_pool /*allocate_pool_ptr*/, common::ObIArray<share::ObResourcePool*>& old_pool);
  int try_upgrade_cluster_create_gts_unit_config();
  int do_upgrade_cluster_create_gts_unit_config();
  int try_upgrade_cluster_create_gts_resource_pool();
  int init_upgrade_cluster_gts_resource_pool(const common::ObZone& zone, share::ObResourcePool& pool);
  int inner_create_unit_config(share::ObUnitConfig& unit_config, const bool if_not_exist);
  int inner_create_resource_pool(
      share::ObResourcePool& resource_pool, const share::ObUnitConfigName& config_name, const bool if_not_exist);
  int inner_commit_change_pool_owner(
      const bool grant, const ObIArray<share::ObResourcePoolName>& pool_names, const uint64_t tenant_id);
  int inner_try_delete_migrate_unit_resource(const uint64_t unit_id, const common::ObAddr& migrate_from_server);

private:
  bool inited_;
  bool loaded_;
  common::ObMySQLProxy* proxy_;
  common::ObServerConfig* server_config_;
  ObILeaderCoordinator* leader_coordinator_;
  ObServerManager& server_mgr_;
  ObZoneManager& zone_mgr_;
  share::ObUnitTableOperator ut_operator_;
  common::hash::ObHashMap<uint64_t, share::ObUnitConfig*> id_config_map_;
  common::hash::ObHashMap<share::ObUnitConfigName, share::ObUnitConfig*> name_config_map_;
  common::hash::ObHashMap<uint64_t, int64_t> config_ref_count_map_;
  common::hash::ObHashMap<uint64_t, common::ObArray<share::ObResourcePool*>*> config_pools_map_;
  common::ObPooledAllocator<common::ObArray<share::ObResourcePool*> > config_pools_allocator_;
  common::ObPooledAllocator<share::ObUnitConfig> config_allocator_;
  IdPoolMap id_pool_map_;
  common::hash::ObHashMap<share::ObResourcePoolName, share::ObResourcePool*> name_pool_map_;
  common::ObPooledAllocator<share::ObResourcePool> pool_allocator_;
  common::hash::ObHashMap<uint64_t, common::ObArray<share::ObUnit*>*> pool_unit_map_;
  common::ObPooledAllocator<common::ObArray<share::ObUnit*> > pool_unit_allocator_;
  common::hash::ObHashMap<uint64_t, share::ObUnit*> id_unit_map_;
  common::ObPooledAllocator<share::ObUnit> allocator_;
  common::hash::ObHashMap<common::ObAddr, common::ObArray<ObUnitLoad>*> server_loads_;
  common::ObPooledAllocator<common::ObArray<ObUnitLoad> > load_allocator_;
  TenantPoolsMap tenant_pools_map_;
  common::ObPooledAllocator<common::ObArray<share::ObResourcePool*> > tenant_pools_allocator_;
  common::hash::ObHashMap<common::ObAddr, common::ObArray<uint64_t>*> server_migrate_units_map_;
  common::ObPooledAllocator<common::ObArray<uint64_t> > migrate_units_allocator_;
  common::SpinRWLock lock_;
  ObUnitPlacementStrategy* unit_placement_strategy_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObRootBalancer* root_balance_;
};

template <typename SCHEMA>
int ObUnitManager::check_schema_zone_unit_enough(const common::ObZone& zone, const int64_t total_unit_num,
    const int64_t full_unit_num, const int64_t logonly_unit_num, const SCHEMA& schema,
    share::schema::ObSchemaGetterGuard& schema_guard, bool& enough)
{
  int ret = OB_SUCCESS;
  enough = true;
  SpinRLockGuard guard(lock_);
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    RS_LOG(WARN, "variable is not init", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret), K(zone));
  } else if (OB_FAIL(inner_check_schema_zone_unit_enough(
                 zone, total_unit_num, full_unit_num, logonly_unit_num, schema, schema_guard, enough))) {
    RS_LOG(WARN, "fail to inner check schema zone unit enough", K(ret));
  }
  return ret;
}

}  // namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_UNIT_MANAGER_H_
