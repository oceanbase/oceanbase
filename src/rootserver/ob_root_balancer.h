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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_BALANCER_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_BALANCER_H_

#include "share/ob_define.h"
#include "share/ob_unit_getter.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "ob_thread_idling.h"
#include "ob_replica_creator.h"
#include "ob_unit_balancer.h"
#include "ob_partition_balancer.h"
#include "ob_balance_info.h"
#include "ob_migrate_unit.h"
#include "ob_unit_stat_manager.h"
#include "ob_partition_group_coordinator.h"
#include "ob_server_checker.h"
#include "ob_rereplication.h"
#include "ob_root_utils.h"
#include "ob_server_balancer.h"
#include "ob_lost_replica_checker.h"
#include "ob_locality_checker.h"
#include "ob_alter_locality_checker.h"
#include "rootserver/backup/ob_partition_validate.h"
#include "rootserver/restore/ob_restore_info.h"
#include "ob_shrink_resource_pool_checker.h"
#include "ob_single_zone_mode_migrate_replica.h"
#include "lib/thread/ob_async_task_queue.h"
#include "ob_partition_backup.h"
#include "share/ob_unit_replica_counter.h"
#include "ob_single_partition_balance.h"

namespace oceanbase {
namespace common {
class ModulePageArena;
class ObServerConfig;
}  // namespace common
namespace share {
class ObPartitionInfo;
class ObPartitionTableOperator;
class ObSplitInfo;
namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace observer {
class ObRestoreCtx;
}
namespace rootserver {
class ObUnitManager;
class ObServerManager;
class ObRebalanceTaskMgr;
class ObLeaderCoordinator;
class ObZoneManager;
class ObEmptyServerChecker;
class ObRootBalancer;
class ObDDLService;
class ObSingleZoneModeMigrateReplica;
class ObSinglePartitionBalance;

class ObRootBalanceIdling : public ObThreadIdling {
public:
  explicit ObRootBalanceIdling(volatile bool& stop, const ObRootBalancer& host) : ObThreadIdling(stop), host_(host)
  {}

  virtual int64_t get_idle_interval_us();

private:
  const ObRootBalancer& host_;
};

struct ObBalancerTargetSchemaInfo {
public:
  ObBalancerTargetSchemaInfo() : lock_(), tenant_schema_versions_()
  {}
  ~ObBalancerTargetSchemaInfo()
  {}
  int set(const common::ObIArray<share::TenantIdAndSchemaVersion>& tenant_schema_version);
  int update(const share::TenantIdAndSchemaVersion& tenant_schema_version);
  bool is_schema_new_enough(const common::ObIArray<share::TenantIdAndSchemaVersion>& curr_schema_versions) const;
  void reset();
  TO_STRING_KV(K_(tenant_schema_versions));

private:
  common::SpinRWLock lock_;
  common::ObArray<share::TenantIdAndSchemaVersion> tenant_schema_versions_;
};

// Cluster wide load balancer, responsible for:
// - ob server load balance. (migrate unit)
// - partition allocate.
// - replicas count guarantee.
// - coordinate partition group members to the same.
// - unit load balance. (migrate partition replica)
class ObRootBalancer : public ObRsReentrantThread, public share::ObCheckStopProvider {
public:
  enum TaskStatus { UNKNOWN = 0, HAS_TASK, NO_TASK, MAX };
  ObRootBalancer();
  virtual ~ObRootBalancer();
  int init(common::ObServerConfig& cfg, share::schema::ObMultiVersionSchemaService& schema_service,
      ObDDLService& ddl_service, ObUnitManager& unit_mgr, ObServerManager& server_mgr,
      share::ObPartitionTableOperator& pt_operator, share::ObRemotePartitionTableOperator& remote_pt_operator,
      ObLeaderCoordinator& leader_coordinator, ObZoneManager& zone_mgr, ObEmptyServerChecker& empty_server_checker,
      ObRebalanceTaskMgr& task_mgr, obrpc::ObSrvRpcProxy& rpc_proxy, observer::ObRestoreCtx& restore_ctx,
      common::ObAddr& self_addr, common::ObMySQLProxy& sql_proxy, ObSinglePartBalance& single_part_balance);

  // Allocate user table partition address.
  // Including server address (not only unit_id) in result in order to avoid invalid unit_id
  // can't found server address.
  virtual void notify_locality_modification();
  virtual bool has_locality_modification() const;
  virtual bool has_task() const;
  void reset_task_count();
  int set_target_schema_info(const common::ObIArray<share::TenantIdAndSchemaVersion>& schemas)
  {
    wakeup();
    return target_schema_info_.set(schemas);
  }
  int update_target_schema(const share::TenantIdAndSchemaVersion& tenant_schema)
  {
    wakeup();
    return target_schema_info_.update(tenant_schema);
  }
  virtual bool have_server_deleting() const;
  void check_server_deleting();
  virtual int alloc_tablegroup_partitions_for_create(const share::schema::ObTablegroupSchema& tablegroup_schema,
      const obrpc::ObCreateTableMode create_mode, common::ObIArray<ObPartitionAddr>& tablegroup_addr);
  virtual int alloc_partitions_for_create(const share::schema::ObTableSchema& table,
      const obrpc::ObCreateTableMode create_mode, ObITablePartitionAddr& addr);
  virtual int alloc_table_partitions_for_standby(const share::schema::ObTableSchema& table,
      const common::ObIArray<ObPartitionKey>& keys, const obrpc::ObCreateTableMode create_mode,
      ObITablePartitionAddr& addr, share::schema::ObSchemaGetterGuard& guard);
  virtual int alloc_tablegroup_partitions_for_standby(const share::schema::ObTablegroupSchema& table_group,
      const common::ObIArray<ObPartitionKey>& keys, const obrpc::ObCreateTableMode create_mode,
      ObITablePartitionAddr& addr, share::schema::ObSchemaGetterGuard& guard);

  virtual int standby_alloc_partitions_for_split(const share::schema::ObTableSchema& table,
      const common::ObIArray<int64_t>& source_partition_ids, const common::ObIArray<int64_t>& dest_partition_ids,
      ObITablePartitionAddr& addr);
  template <typename SCHEMA>
  int alloc_partitions_for_add(const SCHEMA& table, const SCHEMA& inc_table, const obrpc::ObCreateTableMode create_mode,
      ObITablePartitionAddr& addr);
  template <typename SCHEMA>
  int alloc_partitions_for_split(const SCHEMA& table, const SCHEMA& new_schema, ObITablePartitionAddr& addr);

  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }

  // main entry, never exit.
  virtual int do_balance();
  // do balance for all tenant
  virtual int all_balance(int64_t& total_task_cnt);

  virtual int non_emergency_balance(int64_t& task_cnt);
  // balance partition's in %tenant_id
  virtual int tenant_balance(const uint64_t tenant_id, int64_t& task_cnt);
  int check_locality(int64_t& task_cnt);
  void stop();
  void wakeup();

  bool is_inited() const
  {
    return inited_;
  }
  bool is_stop() const
  {
    return stop_;
  }
  void set_active();
  // return OB_CANCELED if stop, else return OB_SUCCESS
  int check_stop() const;
  int idle() const;
  int check_tenant_group_config_legality(
      common::ObIArray<ObTenantGroupParser::TenantNameGroup>& tenant_groups, bool& legal);
  int fill_faillist(const int64_t count, const common::ObIArray<uint64_t>& table_ids,
      const common::ObIArray<int64_t>& partition_ids, const common::ObIArray<common::ObAddr>& servers,
      const share::ObPartitionReplica::FailList& fail_msgs);
  int check_in_switchover_or_disabled();
  ObRebalanceTaskMgr* get_rebalance_task_mgr()
  {
    return task_mgr_;
  }
  int64_t get_schedule_interval() const;
  int refresh_unit_replica_counter(const uint64_t tenant_id);
  int create_unit_replica_counter(const uint64_t tenant_id);
  int destory_tenant_unit_array();

private:
  int multiple_zone_deployment_tenant_balance(const uint64_t tenant_id,
      balancer::ObLeaderBalanceGroupContainer& balance_group_container,
      ObRootBalanceHelp::BalanceController& balance_controller, int64_t& task_cnt);
  int check_can_do_balance(const uint64_t tenant_id, bool& can_do_recovery, bool& can_do_rebalance) const;
  int can_check_locality(const uint64_t tenant_id, bool& can) const;
  int restore_tenant();
  int cancel_unit_migration(int64_t& task_cnt);
  static const int64_t LOG_INTERVAL = 30 * 1000 * 1000;

private:
  bool inited_;
  volatile int64_t active_;
  mutable ObRootBalanceIdling idling_;
  common::ObServerConfig* config_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObDDLService* ddl_service_;

  ObLeaderCoordinator* leader_coordinator_;
  ObRebalanceTaskMgr* task_mgr_;
  ObUnitManager* unit_mgr_;
  ObUnitStatManager unit_stat_mgr_;

  ObServerBalancer server_balancer_;
  ObReplicaCreator replica_creator_;
  TenantBalanceStat tenant_stat_;
  ObRereplication rereplication_;
  ObUnitBalancer unit_balancer_;
  ObPartitionBalancer pg_balancer_;

  ObMigrateUnit migrate_unit_;
  ObServerChecker server_checker_;
  ObPartitionGroupCoordinator pg_coordinator_;
  ObLostReplicaChecker lost_replica_checker_;
  ObLocalityChecker locality_checker_;
  share::ObPartitionTableOperator* pt_operator_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  ObZoneManager* zone_mgr_;
  ObAlterLocalityChecker alter_locality_checker_;
  observer::ObRestoreCtx* restore_ctx_;
  volatile TaskStatus task_status_;
  ObShrinkResourcePoolChecker shrink_resource_pool_checker_;
  share::ObUnitStatGetter unit_stat_getter_;
  ObServerManager* server_manager_;
  ObSingleZoneModeMigrateReplica single_zone_mode_migrate_replica_;
  ObPartitionBackup pg_backup_;
  ObPartitionValidate pg_validate_;
  bool has_server_deleting_;
  ObBalancerTargetSchemaInfo target_schema_info_;
  // root balancer hopes to use a sufficiently new schema to check the status of the replica
  common::ObArray<share::TenantIdAndSchemaVersion> curr_schema_info_;
  // Schema used for current load balancing scheduling
  ObSinglePartBalance* single_part_balance_;
  common::SpinRWLock refresh_unit_replica_cnt_lock_;
};

class ObBlacklistProcess : public share::ObAsyncTask {
public:
  explicit ObBlacklistProcess() : init_(false), count_(0)
  {}
  virtual ~ObBlacklistProcess()
  {}
  virtual int process();
  int init(const int64_t count, share::ObPartitionTableOperator& pt, const common::ObIArray<uint64_t>& table_ids,
      const common::ObIArray<int64_t>& partition_ids, const common::ObIArray<common::ObAddr>& servers,
      const share::ObPartitionReplica::FailList& fail_msgs);
  virtual int64_t get_deep_copy_size() const;
  share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;

private:
  bool init_;
  int64_t count_;
  share::ObPartitionTableOperator* pt_;
  common::ObArray<uint64_t> table_ids_;
  common::ObArray<int64_t> partition_ids_;
  common::ObArray<common::ObAddr> servers_;
  share::ObPartitionReplica::FailList fail_msgs_;
};

template <typename SCHEMA>
int ObRootBalancer::alloc_partitions_for_split(
    const SCHEMA& table, const SCHEMA& new_table, ObITablePartitionAddr& addr)
{
  return replica_creator_.alloc_partitions_for_split(table, new_table, addr);
}

template <typename SCHEMA>
int ObRootBalancer::alloc_partitions_for_add(const SCHEMA& table, const SCHEMA& inc_table,
    const obrpc::ObCreateTableMode create_mode, ObITablePartitionAddr& addr)
{
  int ret = OB_SUCCESS;
  ObArray<share::TenantUnitRepCnt*> ten_unit_arr;
  if (OB_FAIL(single_part_balance_->get_tenant_unit_array(ten_unit_arr, table.get_tenant_id()))) {
    RS_LOG(WARN, "fail to get tenant unit array", K(ret));
  } else if (OB_FAIL(replica_creator_.alloc_partitions_for_add(table, inc_table, create_mode, ten_unit_arr, addr))) {
    RS_LOG(WARN, "alloc partition address failed", K(ret), K(table), K(inc_table), K(create_mode), K(ten_unit_arr));
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_ROOT_BALANCER_H_
