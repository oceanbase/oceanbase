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

#define USING_LOG_PREFIX RS_LB

#include "ob_root_balancer.h"

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_task_define.h"
#include "share/ob_define.h"
#include "share/ob_multi_cluster_util.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_restore_ctx.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_partition_creator.h"
#include "rootserver/ob_ddl_service.h"
#include "ob_leader_coordinator.h"
#include "ob_unit_manager.h"
#include "ob_locality_checker.h"
#include "lib/stat/ob_diagnose_info.h"
#include "rootserver/restore/ob_restore_mgr.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_single_partition_balance.h"
#include "share/ob_unit_replica_counter.h"

namespace oceanbase {

using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
namespace rootserver {
using namespace balancer;

int64_t ObRootBalanceIdling::get_idle_interval_us()
{
  const int64_t min_idle_time = 10 * 1000000;
  int64_t idle_time = ((host_.has_locality_modification() || host_.has_task() || host_.have_server_deleting())
                           ? min_idle_time
                           : GCONF.balancer_idle_time);
  return idle_time;
}
/////////////////////
int ObBalancerTargetSchemaInfo::set(const common::ObIArray<TenantIdAndSchemaVersion>& tenant_schemas)
{
  int ret = OB_SUCCESS;
  if (tenant_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_schemas));
  } else {
    SpinWLockGuard guard(lock_);
    tenant_schema_versions_.reset();
    if (OB_FAIL(tenant_schema_versions_.assign(tenant_schemas))) {
      LOG_WARN("fail to assign", KR(ret));
    } else {
      LOG_INFO("set balance target schema", K(tenant_schemas));
    }
  }
  return ret;
}

int ObBalancerTargetSchemaInfo::update(const TenantIdAndSchemaVersion& tenant_schema)
{
  int ret = OB_SUCCESS;
  if (!tenant_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_schema));
  } else {
    SpinWLockGuard guard(lock_);
    bool find = false;
    for (int64_t i = 0; i < tenant_schema_versions_.count() && OB_SUCC(ret); i++) {
      TenantIdAndSchemaVersion old_tenant_schema = tenant_schema_versions_.at(i);
      if (tenant_schema.tenant_id_ == old_tenant_schema.tenant_id_) {
        find = true;
        if (tenant_schema.schema_version_ < old_tenant_schema.schema_version_) {
          LOG_WARN("schema version should not fallback", K(tenant_schema), K(old_tenant_schema));
        } else {
          tenant_schema_versions_.at(i).schema_version_ = tenant_schema.schema_version_;
          LOG_INFO("update tenant balance target schema version", K(tenant_schema), K(old_tenant_schema));
        }
        break;
      }
    }  // end for
    if (OB_SUCC(ret) && !find) {
      if (OB_FAIL(tenant_schema_versions_.push_back(tenant_schema))) {
        LOG_WARN("fail to push back", KR(ret), K(tenant_schema));
      } else {
        LOG_INFO("set tenant balance target schema version", K(tenant_schema));
      }
    }
  }  // end else
  return ret;
}

void ObBalancerTargetSchemaInfo::reset()
{
  SpinWLockGuard guard(lock_);
  tenant_schema_versions_.reset();
}

// when switchover primary/standby, in order to ensure that the backup database copy is complete,
// it relies on the load balancing task to refresh the schema enough
// At the same time, the replication tasks corresponding to the schema have been completed. task_status_ is NO_TASK;
// The best way is to switch the process and directly iterate the PT table, to prove that all replicas under the latest
// schema are complete. But this query is slower, which seriously affects the execution time of switchover. Therefore,
// it is judged by the memory status of load balancing, in theory, as long as the schema seen by the load balancing task
// is new enough and there are no tasks to do, it is considered that it can be switched currently; The standby database
// playback thread is responsible for the broadcast of the schema, It is guaranteed to directly set the latest (new
// enough) schema_version to root_balancer; However, in order to prevent the load balancing task from failing to see a
// sufficiently new schema and misjudgment, it is necessary to proactively notify the load balancing when the standby
// database finds a new partition. E.g: build new tenant / build new partitions/ build new tables. need to notify the
// corresponding schema_version of the load balancing thread, and modify the current memory status to task_status_ to
// UNKNOWN; Only when the schema version of the load balancer brushed is greater than the specified version and no task
// is generated, then there is really no task.
//
// Ensure that all tenants can be seen
// 1. Judge by the schema_version of the system tenant.
// As long as the system tenant's schema is new enough, the tenants in curr_schema_version can be guaranteed to be
// complete.
// 2. At this time, if the tenant is in the target_schema but not in the curr_schema, the tenant has been deleted.
// 3. If the tenant is in the curr_schema, but not in the target_schema, it means that the tenant has not created a user
// table,
//   as long as the full replica of the system table is completed.
//   This is the common tenant system table visible through the system tenant's schema.
// Only need to consider which schemas have both
bool ObBalancerTargetSchemaInfo::is_schema_new_enough(
    const ObIArray<TenantIdAndSchemaVersion>& curr_schema_versions) const
{
  bool bret = true;
  SpinRLockGuard guard(lock_);
  for (int64_t i = 0; i < tenant_schema_versions_.count() && bret; i++) {
    const TenantIdAndSchemaVersion& target_version = tenant_schema_versions_.at(i);
    for (int64_t j = 0; j < curr_schema_versions.count() && bret; j++) {
      const TenantIdAndSchemaVersion& curr_version = curr_schema_versions.at(j);
      if (target_version.tenant_id_ == curr_version.tenant_id_) {
        if (target_version.schema_version_ > curr_version.schema_version_) {
          bret = false;
          LOG_INFO("tenant schema is not new enough",
              K(target_version),
              K(curr_version),
              K(tenant_schema_versions_),
              K(curr_schema_versions));
        }
      }
    }
  }
  return bret;
}

/////////////////////
ObRootBalancer::ObRootBalancer()
    : ObRsReentrantThread(true),
      inited_(false),
      active_(0),
      idling_(stop_, *this),
      config_(NULL),
      schema_service_(NULL),
      ddl_service_(NULL),
      leader_coordinator_(NULL),
      task_mgr_(NULL),
      unit_mgr_(NULL),
      unit_stat_mgr_(),
      server_balancer_(),
      replica_creator_(),
      tenant_stat_(),
      rereplication_(),
      unit_balancer_(),
      pg_balancer_(),
      migrate_unit_(),
      server_checker_(),
      pg_coordinator_(),
      locality_checker_(),
      pt_operator_(NULL),
      rpc_proxy_(NULL),
      zone_mgr_(NULL),
      alter_locality_checker_(stop_),
      restore_ctx_(NULL),
      task_status_(UNKNOWN),
      shrink_resource_pool_checker_(stop_),
      unit_stat_getter_(),
      server_manager_(NULL),
      single_zone_mode_migrate_replica_(),
      has_server_deleting_(false),
      target_schema_info_(),
      curr_schema_info_(),
      single_part_balance_(),
      refresh_unit_replica_cnt_lock_()
{}

ObRootBalancer::~ObRootBalancer()
{
  tenant_stat_.reuse();
}

int ObRootBalancer::init(common::ObServerConfig& cfg, share::schema::ObMultiVersionSchemaService& schema_service,
    ObDDLService& ddl_service, ObUnitManager& unit_mgr, ObServerManager& server_mgr,
    share::ObPartitionTableOperator& pt_operator, share::ObRemotePartitionTableOperator& remote_pt_operator,
    ObLeaderCoordinator& leader_coordinator, ObZoneManager& zone_mgr, ObEmptyServerChecker& empty_server_checker,
    ObRebalanceTaskMgr& task_mgr, ObSrvRpcProxy& rpc_proxy, observer::ObRestoreCtx& restore_ctx, ObAddr& self_addr,
    ObMySQLProxy& sql_proxy, ObSinglePartBalance& single_part_balance)
{
  int ret = OB_SUCCESS;
  static const int64_t root_balancer_thread_cnt = 1;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("root balancer already inited", K(ret));
  } else if (OB_FAIL(unit_stat_getter_.init(pt_operator, schema_service, *this))) {
    LOG_WARN("fail init unit stat getter", K(ret));
  } else if (OB_FAIL(tenant_stat_.init(unit_mgr,
                 server_mgr,
                 pt_operator,
                 remote_pt_operator,
                 zone_mgr,
                 task_mgr,
                 *this,
                 ddl_service.get_sql_proxy()))) {
    LOG_WARN("init tenant stat failed", K(ret));
  } else if (OB_FAIL(unit_stat_mgr_.init(schema_service, unit_mgr, unit_stat_getter_))) {
    LOG_WARN("init unit stat mgr failed", K(ret));
  } else if (OB_FAIL(replica_creator_.init(schema_service, unit_mgr, server_mgr, pt_operator, zone_mgr, *this))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(server_balancer_.init(
                 schema_service, unit_mgr, leader_coordinator, zone_mgr, server_mgr, unit_stat_mgr_, tenant_stat_))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(unit_balancer_.init(cfg, pt_operator, task_mgr, zone_mgr, tenant_stat_, *this))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(pg_balancer_.init(cfg, schema_service, pt_operator, task_mgr, zone_mgr, tenant_stat_, *this))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(server_checker_.init(unit_mgr, server_mgr, empty_server_checker, tenant_stat_))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(pg_coordinator_.init(task_mgr, tenant_stat_, *this))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(migrate_unit_.init(unit_mgr, task_mgr, tenant_stat_, *this))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(
                 rereplication_.init(schema_service, zone_mgr, pt_operator, task_mgr, tenant_stat_, *this, unit_mgr))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(lost_replica_checker_.init(server_mgr, pt_operator, schema_service))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(create(root_balancer_thread_cnt, "RootBalance"))) {
    LOG_WARN("create root balancer thread failed", K(ret), K(root_balancer_thread_cnt));
  } else if (OB_FAIL(locality_checker_.init(
                 schema_service, tenant_stat_, task_mgr, zone_mgr, *this, server_mgr, unit_mgr))) {
    LOG_WARN("fail to init locality checker", K(ret));
  } else if (OB_FAIL(alter_locality_checker_.init(&schema_service,
                 &leader_coordinator,
                 GCTX.rs_rpc_proxy_,
                 &pt_operator,
                 &server_mgr,
                 self_addr,
                 &zone_mgr,
                 &unit_mgr))) {
    LOG_WARN("fail to init alter locality checker", K(ret));
  } else if (OB_FAIL(shrink_resource_pool_checker_.init(&schema_service,
                 &leader_coordinator,
                 &rpc_proxy,
                 self_addr,
                 &unit_mgr,
                 &pt_operator,
                 &tenant_stat_,
                 &task_mgr,
                 &server_mgr))) {
    LOG_WARN("fail to init shrink resource pool checker", K(ret));
  } else if (OB_FAIL(single_zone_mode_migrate_replica_.init(
                 cfg, schema_service, pt_operator, task_mgr, zone_mgr, tenant_stat_, *this))) {
    LOG_WARN("fail to init single zone mode migrate replica", K(ret));
  } else if (OB_FAIL(pg_backup_.init(
                 cfg, schema_service, sql_proxy, pt_operator, task_mgr, zone_mgr, tenant_stat_, server_mgr, *this))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(pg_validate_.init(sql_proxy, task_mgr, server_mgr, zone_mgr))) {
    LOG_WARN("failed to init partition validator", K(ret));
  } else {
    config_ = &cfg;
    zone_mgr_ = &zone_mgr;
    schema_service_ = &schema_service;
    ddl_service_ = &ddl_service;
    leader_coordinator_ = &leader_coordinator;
    unit_mgr_ = &unit_mgr;
    task_mgr_ = &task_mgr;
    pt_operator_ = &pt_operator;
    rpc_proxy_ = &rpc_proxy;
    (void)ATOMIC_SET(&task_status_, UNKNOWN);
    restore_ctx_ = &restore_ctx;
    server_manager_ = &server_mgr;
    has_server_deleting_ = false;
    target_schema_info_.reset();
    curr_schema_info_.reset();
    single_part_balance_ = &single_part_balance;
    inited_ = true;
  }
  return ret;
}

int ObRootBalancer::alloc_tablegroup_partitions_for_create(const share::schema::ObTablegroupSchema& tablegroup_schema,
    const obrpc::ObCreateTableMode create_mode, common::ObIArray<ObPartitionAddr>& tablegroup_addr)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(refresh_unit_replica_cnt_lock_);
  ObArray<TenantUnitRepCnt*> ten_unit_arr;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(single_part_balance_->get_tenant_unit_array(ten_unit_arr, tablegroup_schema.get_tenant_id()))) {
    LOG_WARN("fail to get tenant unit array", K(ret));
  } else if (OB_FAIL(replica_creator_.alloc_tablegroup_partitions_for_create(
                 tablegroup_schema, create_mode, tablegroup_addr, ten_unit_arr))) {
    LOG_WARN("fail to alloc tablegroup partition for create", K(ret));
  } else if (OB_FAIL(single_part_balance_->update_tenant_unit_replica_capacity(
                 tablegroup_schema.get_tenant_id(), ten_unit_arr))) {
    LOG_WARN("fail to set tenant unit", K(tablegroup_schema.get_tenant_id()), K(ten_unit_arr));
  }
  return ret;
}

int ObRootBalancer::standby_alloc_partitions_for_split(const share::schema::ObTableSchema& table,
    const common::ObIArray<int64_t>& source_partition_ids, const common::ObIArray<int64_t>& dest_partition_ids,
    ObITablePartitionAddr& addr)
{
  return replica_creator_.standby_alloc_partitions_for_split(table, source_partition_ids, dest_partition_ids, addr);
}

int ObRootBalancer::alloc_partitions_for_create(
    const share::schema::ObTableSchema& table, obrpc::ObCreateTableMode create_mode, ObITablePartitionAddr& addr)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(refresh_unit_replica_cnt_lock_);
  ObArray<TenantUnitRepCnt*> ten_unit_arr;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(single_part_balance_->get_tenant_unit_array(ten_unit_arr, table.get_tenant_id()))) {
    LOG_WARN("fail to get tenant unit array", K(ret));
  } else if (OB_FAIL(replica_creator_.alloc_partitions_for_create(table, create_mode, addr, ten_unit_arr))) {
    LOG_WARN("fail to alloc tablegroup partition for create", K(ret));
  } else if (OB_FAIL(single_part_balance_->update_tenant_unit_replica_capacity(table.get_tenant_id(), ten_unit_arr))) {
    LOG_WARN("fail to set tenant unit", K(table.get_tenant_id()), K(ten_unit_arr));
  }
  return ret;
}

int ObRootBalancer::alloc_table_partitions_for_standby(const share::schema::ObTableSchema& table,
    const common::ObIArray<ObPartitionKey>& keys, obrpc::ObCreateTableMode create_mode, ObITablePartitionAddr& addr,
    share::schema::ObSchemaGetterGuard& guard)
{
  return replica_creator_.alloc_table_partitions_for_standby(table, keys, create_mode, addr, guard);
}

int ObRootBalancer::alloc_tablegroup_partitions_for_standby(const share::schema::ObTablegroupSchema& table_group,
    const common::ObIArray<ObPartitionKey>& keys, obrpc::ObCreateTableMode create_mode, ObITablePartitionAddr& addr,
    share::schema::ObSchemaGetterGuard& guard)
{
  return replica_creator_.alloc_tablegroup_partitions_for_standby(table_group, keys, create_mode, addr, guard);
}

int ObRootBalancer::refresh_unit_replica_counter(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<share::TenantUnitRepCnt> tmp_ten_unit_reps;
  const int64_t start = ObTimeUtility::current_time();
  ObArray<share::ObUnitInfo> unit_infos;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_active_unit_infos_by_tenant(tenant_id, unit_infos))) {
    LOG_WARN("fail to get active unit infos by tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(single_part_balance_->refresh_unit_replica_counter(
                 tenant_id, start, unit_infos, schema_guard, tmp_ten_unit_reps))) {
    LOG_WARN("fail to refresh unit replica  counter", K(ret));
  } else {
    SpinWLockGuard guard(refresh_unit_replica_cnt_lock_);
    if (OB_FAIL(
            single_part_balance_->set_tenant_unit_replica_capacity(tenant_id, start, unit_infos, tmp_ten_unit_reps))) {
      LOG_WARN("fail to set tenant unti replica capacity", K(ret));
    }
  }
  return ret;
}

int ObRootBalancer::create_unit_replica_counter(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(refresh_unit_replica_cnt_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(single_part_balance_->create_unit_replica_counter(tenant_id))) {
    LOG_WARN("fail to create unit replica counter", K(ret));
  }
  return ret;
}

int ObRootBalancer::destory_tenant_unit_array()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(refresh_unit_replica_cnt_lock_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(single_part_balance_->destory_tenant_unit_array())) {
    LOG_WARN("fail to destory tenant unit array", K(ret));
  }
  return ret;
}

int ObRootBalancer::idle() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(idling_.idle())) {
    LOG_WARN("idle failed", K(ret));
  } else {
    LOG_INFO("root balance idle", "idle_time", idling_.get_idle_interval_us());
  }
  return ret;
}

void ObRootBalancer::wakeup()
{
  if (!inited_) {
    LOG_WARN("not init");
  } else {
    reset_task_count();
    idling_.wakeup();
  }
}

void ObRootBalancer::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObRsReentrantThread::stop();
    idling_.wakeup();
    target_schema_info_.reset();
    curr_schema_info_.reset();
  }
}

int ObRootBalancer::check_stop() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ret = stop_ ? OB_CANCELED : OB_SUCCESS;
  }
  return ret;
}

void ObRootBalancer::set_active()
{
  const int64_t old_val = ATOMIC_SET(&active_, 1);
  UNUSED(old_val);
  wakeup();
}

void ObRootBalancer::notify_locality_modification()
{
  alter_locality_checker_.notify_locality_modification();
}

bool ObRootBalancer::has_task() const
{
  return UNKNOWN == ATOMIC_LOAD(&task_status_) || HAS_TASK == ATOMIC_LOAD(&task_status_);
}

bool ObRootBalancer::has_locality_modification() const
{
  return alter_locality_checker_.has_locality_modification();
}

void ObRootBalancer::check_server_deleting()
{
  if (OB_ISNULL(server_manager_)) {
    has_server_deleting_ = false;
  } else {
    has_server_deleting_ = server_manager_->have_server_deleting();
  }
}

bool ObRootBalancer::have_server_deleting() const
{
  return has_server_deleting_;
}

void ObRootBalancer::run3()
{
  LOG_INFO("root balance start");
  int ret = OB_SUCCESS;
  // every time the root balance start to run, we need to try check locality modification
  notify_locality_modification();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(do_balance())) {
    LOG_WARN("all balance failed", K(ret));
  }
  tenant_stat_.reuse();
  LOG_INFO("root balance stop", K(ret));
}

void ObRootBalancer::reset_task_count()
{
  LOG_INFO("reset task count, maybe has task to do");
  (void)ATOMIC_SET(&task_status_, UNKNOWN);
}

int ObRootBalancer::do_balance()
{
  int ret = OB_SUCCESS;
  int64_t failed_times = 0;
  int64_t total_task_cnt = 0;
  int64_t start_time = 0;
  int64_t finish_time = 0;
  int64_t remained_task_count = -1;
  // Whether auxiliary calculation needs to write balance_start/balance_finish log
  int64_t task_count_in_round = 0;
  // Count the tasks generated in a round of load balancing
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  }
  ObArray<uint64_t> all_tenant;
  while (OB_SUCC(ret)) {
    update_last_run_timestamp();
    if (OB_FAIL(check_stop())) {
      break;
    }
    check_server_deleting();
    if (!ATOMIC_LOAD(&active_)) {
      if (OB_FAIL(idle())) {
        LOG_WARN("idle failed", K(ret));
        break;
      } else {
        continue;
      }
    }
    if (OB_SUCC(ret)) {
      if (-1 == remained_task_count) {
      } else {
        remained_task_count =
            task_mgr_->get_high_priority_task_info_cnt() + task_mgr_->get_low_priority_task_info_cnt();
      }
    }
    curr_schema_info_.reset();
    start_time = ObTimeUtility::current_time();
    if (OB_FAIL(all_balance(total_task_cnt))) {
      failed_times++;
      LOG_WARN("do balance round failed", K(ret), K(failed_times));
      ret = OB_SUCCESS;
      EVENT_ADD(RS_BALANCER_FAIL_COUNT, 1);
    } else {
      failed_times = 0;
      EVENT_ADD(RS_BALANCER_SUCC_COUNT, 1);
    }
    finish_time = ObTimeUtility::current_time();

    //  balance strategy:
    //  1. The number of tasks changes from 0 to non-zero, and counts as balance to start
    //  2. There are no tasks in the queue, and the number of tasks changes from non-zero to zero,
    //     which is counted as the end of balance
    if (OB_SUCC(ret)) {
      task_count_in_round += total_task_cnt;
      if (0 >= remained_task_count && 0 != total_task_cnt) {
        // New effective round
        LOG_INFO("tenant balance start", K(remained_task_count), K(total_task_cnt));
        ROOTSERVICE_EVENT_ADD("balancer", "tenant_balance_started", "start_time", start_time);
        remained_task_count = total_task_cnt;
      } else if (0 == remained_task_count && 0 == total_task_cnt) {
        // End of this round
        LOG_INFO("tenant balance finish", K(task_count_in_round), K(total_task_cnt), K(remained_task_count));
        ROOTSERVICE_EVENT_ADD(
            "balancer", "tenant_balance_finished", "finish_time", finish_time, "task_count", task_count_in_round);
        task_count_in_round = 0;
        remained_task_count = -1;
      }
    }
    if (OB_SUCC(ret)) {
      // nothing todo
      bool all_server_alive = false;
      if (total_task_cnt > 0 || remained_task_count > 0) {
        LOG_INFO("root balance has task to do");
        (void)ATOMIC_SET(&task_status_, HAS_TASK);
      } else if (!config_->is_rereplication_enabled()) {
        // Check whether migration replication and balance are allowed,
        // otherwise it is impossible to confirm whether there are tasks
        if (REACH_TIME_INTERVAL(LOG_INTERVAL)) {
          // Print every 30 minutes
          LOG_INFO("disable rereplication or disable rebalance, maybe has task todo",
              "enable_rereplication",
              config_->is_rereplication_enabled(),
              "enable_rebalance",
              config_->is_rebalance_enabled());
        }
        (void)ATOMIC_SET(&task_status_, UNKNOWN);
      } else if (OB_FAIL(server_manager_->check_all_server_active(all_server_alive))) {
        LOG_WARN("failed to check all server active", K(ret));
        (void)ATOMIC_SET(&task_status_, UNKNOWN);
      } else if (!all_server_alive) {
        // When the state of a server is not active, tasks may not be generated,
        // and there is no way to guarantee that no tasks are generated
        if (REACH_TIME_INTERVAL(LOG_INTERVAL)) {
          // Print every 30 minutes
          LOG_INFO("has server is not active, maybe has task todo", K(ret), K(all_server_alive));
        }
        (void)ATOMIC_SET(&task_status_, UNKNOWN);
      } else if (target_schema_info_.is_schema_new_enough(curr_schema_info_)) {
        (void)ATOMIC_SET(&task_status_, NO_TASK);
      }
    } else {
      LOG_INFO("reset task count, maybe has task to do");
      (void)ATOMIC_SET(&task_status_, UNKNOWN);
    }

    // idle after success or failed 2 times
    if (0 == failed_times || failed_times >= 2) {
      if (OB_FAIL(idle())) {
        LOG_WARN("idle failed", K(ret));
      }
    }
  }
  return ret;
}

int ObRootBalancer::all_balance(int64_t& total_task_cnt)
{
  int ret = OB_SUCCESS;
  total_task_cnt = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(tenant_stat_.reuse_replica_count_mgr())) {
    LOG_WARN("reuse_replica_count_mgr failed", K(ret));
  } else if (OB_FAIL(unit_stat_mgr_.gather_stat())) {
    LOG_WARN("gather all tenant unit stat failed, refuse to do server_balance", K(ret));
    unit_stat_mgr_.reuse();
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret)) {
    // ignore unit balance result
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = server_balancer_.balance_servers())) {
      LOG_WARN("do unit balance failed", K(tmp_ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(restore_tenant())) {
      LOG_WARN("fail do restore", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t non_emergency_task_cnt = 0;
    ObArray<share::ObResourcePool> pools;
    if (OB_FAIL(non_emergency_balance(non_emergency_task_cnt))) {
      LOG_WARN("fail to tenants balance", K(ret));
      ret = OB_SUCCESS;  // ignore ret
    }
    if (OB_FAIL(unit_mgr_->get_pools(pools))) {
      LOG_WARN("fail to get pools", K(ret));
    } else {
      FOREACH_CNT_X(pool, pools, OB_SUCC(ret))
      {
        if (OB_FAIL(unit_mgr_->finish_migrate_unit_not_in_tenant(pool, total_task_cnt))) {
          LOG_WARN("fail to finish migrate unit not in tenant", K(ret));
        }
        ret = OB_SUCCESS;  // ignore ret
      }
    }
    total_task_cnt += non_emergency_task_cnt;
    LOG_INFO("finish one round balance",
        "rereplication_enabled",
        config_->is_rereplication_enabled(),
        "rebalance_enabled",
        config_->is_rebalance_enabled(),
        "high_prio_task_info_cnt",
        task_mgr_->get_high_priority_task_info_cnt(),
        "low_prio_task_info_cnt",
        task_mgr_->get_low_priority_task_info_cnt(),
        K(total_task_cnt),
        K(non_emergency_task_cnt),
        K(ret));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = server_checker_.try_notify_empty_server_checker())) {
    LOG_WARN("try notify empty server checker failed", K(ret));
  }

  if (OB_SUCCESS != (tmp_ret = server_checker_.try_delete_server())) {
    LOG_WARN("try_delete_server failed", K(tmp_ret));
  }

  if (OB_SUCCESS != (tmp_ret = lost_replica_checker_.check_lost_replicas())) {
    LOG_WARN("fail check lost replicas", K(tmp_ret));
  }

  if (OB_SUCCESS != (tmp_ret = shrink_resource_pool_checker_.check_shrink_resource_pool_finished())) {
    LOG_WARN("fail to check shrink resource pool", K(tmp_ret));
  }

  if (OB_SUCC(ret) && (NULL != task_mgr_)) {
    ObRootBalanceHelp::BalanceController balance_controller;
    ObString switch_config_str = GCONF.__balance_controller.str();
    if (OB_FAIL(ObRootBalanceHelp::parse_balance_info(switch_config_str, balance_controller))) {
      LOG_WARN("fail to parse balance switch", K(ret), "balance_switch", switch_config_str);
    } else if (GCONF.is_rebalance_enabled() || balance_controller.at(ObRootBalanceHelp::ENABLE_SERVER_BALANCE)) {
      if (OB_FAIL(server_balancer_.tenant_group_balance())) {
        LOG_WARN("fail to do tenant group balance", K(ret));
      }
    }
  }
  return ret;
}

int ObRootBalancer::restore_tenant()
{
  LOG_INFO("restore tenant second phase begin");
  int ret = OB_SUCCESS;
  ObRestoreMgr restore_mgr(stop_);
  ObRestoreMgrCtx restore_ctx;
  restore_ctx.conn_env_ = restore_ctx_;
  restore_ctx.sql_proxy_ = &(ddl_service_->get_sql_proxy());
  restore_ctx.task_mgr_ = task_mgr_;
  restore_ctx.tenant_stat_ = &tenant_stat_;
  restore_ctx.schema_service_ = schema_service_;
  restore_ctx.ddl_service_ = ddl_service_;
  restore_ctx.root_balancer_ = const_cast<ObRootBalancer*>(this);
  if (OB_FAIL(restore_mgr.init(&restore_ctx))) {
    LOG_WARN("fail init restore mgr", K(ret));
  } else if (OB_FAIL(restore_mgr.restore())) {
    LOG_WARN("fail do restore", K(ret));
  }
  return ret;
}

int ObRootBalancer::non_emergency_balance(int64_t& total_task_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> all_tenant;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(leader_coordinator_->get_tenant_ids(all_tenant))) {
    LOG_WARN("fetch all tenant id failed", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    DEBUG_SYNC(UNIT_BALANCE_BEFORE_PARTITION_BALANCE);
    FOREACH_X(t, all_tenant, OB_SUCCESS == ret)
    {
      const uint64_t tenant_id = *t;
      // if have enough task in task queue, idle
      if (task_mgr_->is_busy()) {
        LOG_INFO("has enough rebalance tasks, idle");
        while (OB_SUCC(ret) && task_mgr_->is_busy()) {
          if (OB_FAIL(idle())) {
            LOG_WARN("idle failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
        // nop
      } else if (OB_FAIL(check_stop())) {
        LOG_WARN("balancer stop", K(ret));
      } else {
        // In order to achieve tenant balance error isolation,
        // the error code of each balance is ignored
        int64_t task_cnt = 0;
        if (OB_SUCCESS != (tmp_ret = tenant_balance(tenant_id, task_cnt))) {
          LOG_WARN("tenant balance failed, mock task count as 1", K(tmp_ret), K(tenant_id));
          task_cnt = 1;
        }
        total_task_cnt += task_cnt;
        if (OB_SYS_TENANT_ID == tenant_id) {
          leader_coordinator_->set_sys_balanced();
        }
      }
    }  // end foreach
  }
  return ret;
}

int ObRootBalancer::can_check_locality(const uint64_t tenant_id, bool& can) const
{
  int ret = OB_SUCCESS;
  can = false;
  int64_t high_priority_task_cnt = 0;
  int64_t low_priority_task_cnt = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(task_mgr_->get_tenant_task_info_cnt(tenant_id, high_priority_task_cnt, low_priority_task_cnt))) {
    LOG_WARN("fail to get tenant task cnt", K(ret), K(tenant_id));
  } else if (high_priority_task_cnt + low_priority_task_cnt <= 0) {
    can = true;
  }
  return ret;
}

int ObRootBalancer::check_can_do_balance(const uint64_t tenant_id, bool& can_do_recovery, bool& can_do_rebalance) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    can_do_recovery = false;
    can_do_rebalance = false;
    if (config_->is_rereplication_enabled() || config_->is_rebalance_enabled()) {
      int64_t high_priority_task_info_cnt = 0;
      int64_t low_priority_task_info_cnt = 0;
      if (OB_FAIL(task_mgr_->get_tenant_task_info_cnt(
              tenant_id, high_priority_task_info_cnt, low_priority_task_info_cnt))) {
        LOG_WARN("get tenant task count failed", K(ret), K(tenant_id));
      } else {
        // Design ideas:
        // 1. Rebuild is the highest priority task;
        //    Always look at the high priority queue first when the task is executed;
        // 2. When there are high-priority tasks, low-priority tasks can be generated.
        //    The low-priority queue will not affect the high-priority execution of the rebuild,
        //    which helps to improve concurrency.
        // 3. Replication and migration are low-priority tasks;
        //    need to wait for the completion of the previous batch of tasks before continuing to the next batch of
        //    tasks; There may be dependencies between tasks and tasks.
        // 4. For low-priority tasks,
        //    the total number of tasks is also limited to prevent multiple tenants from balancing at the same time
        //    and generating too many tasks
        if (0 == low_priority_task_info_cnt && config_->is_rereplication_enabled()) {
          can_do_recovery = true;
        }
        if (0 == low_priority_task_info_cnt && config_->is_rebalance_enabled() &&
            task_mgr_->get_low_priority_task_info_cnt() < ObRebalanceTaskMgr::QUEUE_LOW_TASK_CNT) {
          can_do_rebalance = true;
        }
      }
    }
  }
  return ret;
}

int ObRootBalancer::multiple_zone_deployment_tenant_balance(const uint64_t tenant_id,
    ObLeaderBalanceGroupContainer& balance_group_container, ObRootBalanceHelp::BalanceController& balance_controller,
    int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  int64_t backup_task_cnt = 0;

  DEBUG_SYNC(AFTER_TENANT_BALANCE_GATHER_STAT);

  bool can_do_rebalance = false;
  bool can_do_recovery = false;
  if (OB_FAIL(check_can_do_balance(tenant_id, can_do_recovery, can_do_rebalance))) {
    LOG_WARN("check can do balance failed", K(ret));
  }

  // step1: Kick out the permanent offline replica
  //        Do not consider the two configuration items
  //        is_rereplication_enable and is_rebalance_enable
  if (OB_SUCC(ret) && 0 == task_cnt) {
    // Remove the replica on the permanently offline server from the member list
    if (OB_FAIL(rereplication_.remove_permanent_offline_replicas(task_cnt))) {
      LOG_WARN("remove permanent offline replicas failed", K(ret), K(tenant_id));
    } else {
      if (task_cnt > 0) {
        LOG_INFO("remove permanent offline replicas", K(tenant_id), K(task_cnt));
      }
    }
  }

  if (OB_SUCC(ret) && 0 == task_cnt) {
    if (OB_FAIL(rereplication_.process_only_in_memberlist_replica(task_cnt))) {
      LOG_WARN("fail to process only in member list replica", K(ret));
    } else {
      if (task_cnt > 0) {
        LOG_INFO("process only in member list", K(tenant_id), K(task_cnt));
      }
    }
  }

  // First do type transform and then copy, reuse old data
  // Delete first and then copy and migrate to avoid copying and migrating redundant copies
  // step2: Locality verification; first do type transform and then delete, try to reuse copy data
  // Not controlled by the switch. Always ensure that the locality is consistent
  if (balance_controller.at(ObRootBalanceHelp::ENABLE_MODIFY_QUORUM)) {
    if (OB_SUCC(ret) && 0 == task_cnt) {
      LOG_INFO("start locality check do modify quorum", K(tenant_id));
      bool can = false;
      if (OB_FAIL(can_check_locality(tenant_id, can))) {
        LOG_WARN("fail to check", K(ret), K(tenant_id));
      } else if (!can) {
        // nothing todo
      } else if (OB_FAIL(locality_checker_.do_modify_quorum(balance_group_container.get_hash_index(), task_cnt))) {
        LOG_WARN("fail to do modify quorum", K(ret), K(tenant_id));
      } else if (task_cnt > 0) {
        LOG_INFO("locality check do modify quorum", K(tenant_id), K(task_cnt));
      }
    }
  }
  if (balance_controller.at(ObRootBalanceHelp::ENABLE_TYPE_TRANSFORM)) {
    if (OB_SUCC(ret) && 0 == task_cnt) {
      LOG_INFO("start locality check do type transform", K(tenant_id));
      bool can = false;
      if (OB_FAIL(can_check_locality(tenant_id, can))) {
        LOG_WARN("fail to check", K(ret), K(tenant_id));
      } else if (!can) {
        // nothing todo
      } else if (OB_FAIL(locality_checker_.do_type_transform(balance_group_container.get_hash_index(), task_cnt))) {
        LOG_WARN("fail to do type transform", K(ret), K(tenant_id));
      } else if (task_cnt > 0) {
        LOG_INFO("locality check do type transform", K(tenant_id), K(task_cnt));
      }
    }
  }
  if (balance_controller.at(ObRootBalanceHelp::ENABLE_DELETE_REDUNDANT)) {
    if (OB_SUCC(ret) && 0 == task_cnt) {
      bool can = false;
      LOG_INFO("start locality check do delete redundant", K(tenant_id));
      if (OB_FAIL(can_check_locality(tenant_id, can))) {
        LOG_WARN("fail to check", K(ret), K(tenant_id));
      } else if (!can) {
        // nothing todo
      } else if (OB_FAIL(locality_checker_.do_delete_redundant(balance_group_container.get_hash_index(), task_cnt))) {
        LOG_WARN("fail to do delete redundant", K(ret), K(tenant_id));
      } else if (task_cnt > 0) {
        LOG_INFO("locality check do delete redundant", K(tenant_id), K(task_cnt));
      }
    }
  }

  // Disaster recovery, supplement or reconstruction replica
  // Both rebuild/copy are regarded as disaster recovery operations;
  // they are of parallel priority;
  // failures in tasks will not block the entire disaster recovery operation
  if (OB_SUCC(ret) && 0 == task_cnt) {
    // Not restricted by is_rereplication_enable, unit has been migrated, keep replica and unit consistent
    // replicate replica to unit if unit is migrate from permanent offline server
    int tmp_ret = OB_SUCCESS;
    if (balance_controller.at(ObRootBalanceHelp::ENABLE_REPLICATE_TO_UNIT)) {
      int64_t tmp_task_cnt = 0;
      if (OB_FAIL(rereplication_.replicate_to_unit(tmp_task_cnt))) {
        LOG_WARN("replicate to unit failed", K(ret), K(tenant_id));
        tmp_ret = ret;  // tmp_ret may be overwritten
        ret = OB_SUCCESS;
      } else {
        if (tmp_task_cnt > 0) {
          LOG_INFO("replicate to unit", K(tenant_id), K(tmp_task_cnt));
          task_cnt += tmp_task_cnt;
        }
      }
    }
    // If a copy task has been generated before, it cannot continue to be generated,
    // otherwise it may cause redundant tasks;
    if (OB_SUCC(ret) && 0 == task_cnt &&
        (can_do_recovery || balance_controller.at(ObRootBalanceHelp::ENABLE_REPLICATE))) {
      // make sure active replica count >= schema replica count
      int64_t tmp_task_cnt = 0;
      if (OB_FAIL(rereplication_.replicate_enough_replica(balance_group_container.get_hash_index(), tmp_task_cnt))) {
        LOG_WARN("replicate enough replicas failed", K(ret), K(tenant_id));
      } else {
        if (tmp_task_cnt > 0) {
          LOG_INFO("replica enough replicas", K(tenant_id), K(tmp_task_cnt));
          task_cnt += tmp_task_cnt;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }  // Disaster recovery operation ends

  // Unit shrinking operation of shrink resource pool
  if (balance_controller.at(ObRootBalanceHelp::ENABLE_SHRINK)) {
    if (OB_SUCC(ret) && 0 == task_cnt) {
      if (OB_FAIL(shrink_resource_pool_checker_.try_shrink_tenant_resource_pools(task_cnt))) {
        LOG_WARN("fail to try shrink resource pool", K(ret));
      } else {
        if (task_cnt > 0) {
          LOG_INFO("try shrink resoure pool", K(tenant_id), K(task_cnt));
        }
      }
    }
  }

  // Backup priority is lower than disaster tolerance,
  // but it is not mutually exclusive with migration,
  // and the priority is the same as migration
  if (OB_SUCCESS == ret && 0 == task_cnt) {
    if (OB_FAIL(pg_backup_.partition_backup(backup_task_cnt, tenant_id))) {
      LOG_WARN("failed to do partition backup", K(ret), K(tenant_id), K(task_cnt));
    }
    if (OB_SUCC(ret)) {
      if (backup_task_cnt > 0) {
        LOG_INFO("do partition backup", K(ret), K(tenant_id), K(backup_task_cnt));
      }
    }
  }

  if (OB_SUCCESS == ret && 0 == task_cnt) {
    int64_t validate_task_cnt = 0;
    if (OB_FAIL(pg_validate_.partition_validate(tenant_id, validate_task_cnt))) {
      LOG_WARN("failed to validate partition of tenant", K(ret), K(tenant_id));
    }
    if (OB_SUCC(ret)) {
      if (validate_task_cnt > 0) {
        LOG_INFO("do partition validate", K(ret), K(tenant_id));
      }
    }
  }

  // step4: The status of the operation unit migration
  if (OB_FAIL(migrate_unit_.unit_migrate_finish(task_cnt))) {
    LOG_WARN("check unit migrate finish failed", K(ret), K(tenant_id));
  } else if (task_cnt > 0) {
    LOG_INFO("check unit migrate finish", K(tenant_id), K(task_cnt));
  }

  // step 5: Copy type alignment
  if (OB_SUCCESS == ret && 0 == task_cnt &&
      (can_do_rebalance || balance_controller.at(ObRootBalanceHelp::ENABLE_COORDINATE_PG))) {
    // coordinate partition group member to the same unit
    // We count this kind of rebalance task as replicate task.
    if (OB_FAIL(check_stop())) {
      LOG_WARN("balancer stop", K(ret));
    } else if (OB_FAIL(pg_coordinator_.coordinate_pg_member(task_cnt))) {
      LOG_WARN("coordinate partition group member to the same failed", K(ret), K(tenant_id));
    } else {
      if (task_cnt > 0) {
        LOG_INFO("coordinate partition group member to the same", K(tenant_id), K(task_cnt));
      }
    }
  }
  if (OB_SUCCESS == ret && 0 == task_cnt &&
      (can_do_recovery || can_do_rebalance || balance_controller.at(ObRootBalanceHelp::ENABLE_CANCEL_UNIT_MIGRATION))) {
    LOG_INFO("start to cancel unit migrate for blocked server", K(tenant_id));
    if (OB_FAIL(cancel_unit_migration(task_cnt))) {
      LOG_WARN("fail to cancel unit migration", K(ret), K(tenant_id));
    } else if (task_cnt > 0) {
      LOG_INFO("cancel unit migration finish", K(ret), K(tenant_id), K(task_cnt));
    }
  }

  if (OB_SUCCESS == ret && 0 == task_cnt &&
      (can_do_rebalance || balance_controller.at(ObRootBalanceHelp::ENABLE_MIGRATE_TO_UNIT))) {
    // The unit has been migrated and remains consistent with the unit
    LOG_INFO("start to check unit migrate finish", K(ret), K(tenant_id));
    if (OB_FAIL(migrate_unit_.migrate_to_unit(task_cnt))) {
      LOG_WARN("migrate replica to unit failed", K(ret), K(tenant_id));
    } else {
      if (task_cnt > 0) {
        LOG_INFO("migrate replica to unit", K(tenant_id), K(task_cnt));
      }
    }
  }

  // step 6: Start load balancing in the unit
  // Tenant is a prerequisite for load balancing
  // 1. There is no flag replica. Because flag replica only exists in meta,
  //    migration is not easy to handle
  bool check_passed = true;
  if (OB_SUCC(ret) && 0 == task_cnt) {
    FOREACH_X(r, tenant_stat_.all_replica_, check_passed)
    {
      if (r->only_in_member_list()) {
        check_passed = false;
        LOG_INFO("tenant can not do rebalance task right now, "
                 "because it still has flag replica",
            "replica",
            *r);
      }
    }
  }
  if (OB_SUCCESS == ret && 0 == task_cnt &&
      ((check_passed && can_do_rebalance) || balance_controller.at(ObRootBalanceHelp::ENABLE_PARTITION_BALANCE))) {
    // tenant_stat_.print_stat();
    // balance tablegroup partition group count
    // Partition groups are evenly distributed to different Units to avoid partition group aggregation

    if (OB_FAIL(pg_balancer_.partition_balance(task_cnt, balance_group_container.get_hash_index()))) {
      LOG_WARN("partition group balance failed", K(ret), K(tenant_id));
    } else {
      if (task_cnt > 0) {
        LOG_INFO("partition group balance", K(tenant_id), K(task_cnt));
      }
    }
  }
  if (OB_SUCCESS == ret && 0 == task_cnt &&
      ((check_passed && can_do_rebalance) || balance_controller.at(ObRootBalanceHelp::ENABLE_UNIT_BALANCE))) {
    // balance tenant unit load
    if (OB_FAIL(unit_balancer_.unit_load_balance_v2(
            task_cnt, balance_group_container.get_hash_index(), balance_group_container.get_processed_tids()))) {
      LOG_WARN("unit load balance failed", K(ret), K(tenant_id));
    }
    if (OB_SUCC(ret)) {
      if (task_cnt > 0) {
        LOG_INFO("unit load balance", K(ret), K(tenant_id), K(task_cnt));
      }
    }
  }

  if (OB_SUCC(ret)) {
    task_cnt += backup_task_cnt;
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = alter_locality_checker_.check_alter_locality_finished(
                         tenant_id, balance_group_container.get_hash_index(), tenant_stat_))) {
    LOG_WARN("fail to check alter locality finished", K(tmp_ret));
  }

  // Record a log for easy checking of tasks that are not scheduled
  LOG_INFO(
      "finish one round tenant_balance", K(task_cnt), K(can_do_rebalance), K(can_do_recovery), K(tenant_id), K(ret));
  return ret;
}

// Note:
//  - is_rereplication_enabled to control disaster tolerance,
//    replication is not accurate,
//    and disaster tolerance can also be achieved through migration.
//    Continue this name for compatibility
//  - is_rebalance_enabled Control load balancing
//
//  server_balancer does both disaster recovery and balance
//  - The server_balancer moves the unit out because the server goes offline,
//    which belongs to the category of disaster tolerance control
//  - Server_balancer moves the unit out due to uneven load,
//    which belongs to the category of load balancing control
int ObRootBalancer::tenant_balance(const uint64_t tenant_id, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  task_cnt = 0;
  ObSchemaGetterGuard schema_guard;
  ObRootBalanceHelp::BalanceController balance_controller;
  tenant_stat_.reuse();
  ObString swtich_config_str = config_->__balance_controller.str();
  common::ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_BALANCER);
  TenantSchemaGetter stat_finder(tenant_id);
  ObLeaderBalanceGroupContainer balance_group_container(schema_guard, stat_finder, allocator);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get schema manager failed", K(ret));
  } else if (OB_FAIL(balance_group_container.init(tenant_id))) {
    LOG_WARN("fail to init balance group container", K(ret), K(tenant_id));
  } else if (OB_FAIL(balance_group_container.build())) {
    LOG_WARN("fail to build balance index builder", K(ret));
  } else if (OB_FAIL(tenant_stat_.gather_stat(tenant_id, &schema_guard, balance_group_container.get_hash_index()))) {
    LOG_WARN("gather tenant balance statistics failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObRootBalanceHelp::parse_balance_info(swtich_config_str, balance_controller))) {
    LOG_WARN("fail to parse balance switch", K(ret), "balance_swtich", swtich_config_str);
  } else {
    // assign unit id to replicas with invalid unit id
    if (OB_FAIL(tenant_stat_.unit_assign())) {
      LOG_WARN("unit assign failed", K(ret), K(tenant_id));
      ret = OB_SUCCESS;
    } else {
      if (OB_FAIL(multiple_zone_deployment_tenant_balance(
              tenant_id, balance_group_container, balance_controller, task_cnt))) {
        LOG_WARN("fail to execute multiple zone deployment tenant balance", K(ret));
        ret = OB_SUCCESS;
      }
    }
    // finish unit not in locality
    ObArray<common::ObZone> zone_list;
    if (OB_FAIL(unit_mgr_->get_tenant_pool_zone_list(tenant_id, zone_list))) {
      LOG_WARN("fail to get tenant pool zone list", K(ret));
    } else if (OB_FAIL(unit_mgr_->finish_migrate_unit_not_in_locality(tenant_id, &schema_guard, zone_list, task_cnt))) {
      LOG_WARN("fail to finish migrate unit not in locality", K(ret));
    }
  }
  return ret;
}

int ObRootBalancer::cancel_unit_migration(int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  TenantBalanceStat& ts = tenant_stat_;
  task_cnt = 0;
  FOREACH_X(zu, ts.all_zone_unit_, OB_SUCC(ret))
  {
    FOREACH_X(u, zu->all_unit_, OB_SUCC(ret))
    {
      if ((*u)->info_.unit_.migrate_from_server_.is_valid() && (*u)->server_->blocked_) {
        // Clean up the replicas that have been migrated in the past
        LOG_WARN("unit migrate blocked destination server, try to cancel migrate",
            K(ret),
            "unit",
            (*u)->info_.unit_,
            "unit_server",
            (*u)->server_->server_);
        FOREACH_X(p, ts.sorted_partition_, OB_SUCCESS == ret)
        {
          if (OB_FAIL(check_stop())) {
            LOG_WARN("balancer stop", K(ret));
            break;
          }
          if (!(*p)->has_leader()) {
            LOG_WARN("partition have no leader, can't replicate", "partition", *(*p));
            continue;
          } else if (!(*p)->is_valid_quorum()) {
            // The quorum of partition was introduced in 1.4.7.1.
            // It depends on the report and may be an illegal value.
            // In this case, skip it first to avoid affecting the generation of subsequent tasks.
            LOG_WARN("partition quorum is invalid, just skip", KPC(*p));
            continue;
          }
          FOR_BEGIN_END_E(r, *(*p), ts.all_replica_, OB_SUCCESS == ret)
          {
            if (r->unit_->info_.unit_.unit_id_ == (*u)->info_.unit_.unit_id_ &&
                r->server_->server_ == (*u)->server_->server_) {
              task_cnt++;
              ObPartitionKey pkey;
              const bool is_valid_paxos_replica = ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_);
              // need to delete;
              if (!is_valid_paxos_replica) {
                const char* comment = balancer::REMOVE_MIGRATE_UNIT_BLOCK_REPLICA;
                common::ObArray<ObRemoveNonPaxosTaskInfo> task_info_array;
                ObRemoveNonPaxosTask task;
                ObRemoveNonPaxosTaskInfo task_info;
                OnlineReplica remove_member;
                remove_member.member_ = ObReplicaMember(r->server_->server_, r->member_time_us_, r->replica_type_);
                remove_member.zone_ = r->zone_;
                if (r->is_in_blacklist(
                        ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA, r->server_->server_, &tenant_stat_)) {
                  ret = OB_REBALANCE_TASK_CANT_EXEC;
                  LOG_WARN(
                      "repica do remove non paxos replica frequent failed, now in black list", K(ret), " replice", *r);
                } else if (OB_FAIL(ts.gen_partition_key(*(*p), pkey))) {
                  LOG_WARN("fail to gen partition key", K(ret), "partition", *(*p), "replica", *r);
                } else if (OB_FAIL(task_info.build(remove_member, pkey))) {
                  LOG_WARN("fail to build remove non paxos replica task info", K(ret));
                } else if (OB_FAIL(task_info_array.push_back(task_info))) {
                  LOG_WARN("fail to push back", K(ret));
                } else if (OB_FAIL(task.build(task_info_array, remove_member.member_.get_server(), comment))) {
                  LOG_WARN("fail to build remove member task", K(ret));
                } else if (OB_FAIL(task_mgr_->add_task(task))) {
                  LOG_WARN("fail to add task", K(ret), K(task));
                } else {
                  LOG_INFO("add remove non paxos replica task success", K(task), "partition", *(*p), K(task_cnt));
                }
              } else if ((*p)->valid_member_cnt_ <= majority((*p)->schema_replica_cnt_) || !(*p)->can_balance()) {
                // nothing todo
                // TODO: need to add additional checks when supporting L-replica copies
              } else {
                const char* comment = balancer::REMOVE_MIGRATE_UNIT_BLOCK_MEMBER;
                ObRemoveMemberTask task;
                common::ObArray<ObRemoveMemberTaskInfo> task_info_array;
                int64_t quorum = 0;
                ObRemoveMemberTaskInfo task_info;
                OnlineReplica remove_member;
                remove_member.member_ = ObReplicaMember(r->server_->server_, r->member_time_us_, r->replica_type_);
                remove_member.zone_ = r->zone_;
                if (r->is_in_blacklist(ObRebalanceTaskType::MEMBER_CHANGE, r->server_->server_, &tenant_stat_)) {
                  ret = OB_REBALANCE_TASK_CANT_EXEC;
                  LOG_WARN("replica do member frequent failed, now in black list", K(ret), "replica", *r);
                } else if (OB_FAIL(tenant_stat_.gen_partition_key(*(*p), pkey))) {
                  LOG_WARN("fail to gen partition key", K(ret), "partition", *(*p), "replica", *r);
                } else if (OB_FAIL(tenant_stat_.get_remove_replica_quorum_size(
                               *(*p), r->zone_, r->replica_type_, quorum))) {
                  LOG_WARN("fail to get quorum size", K(ret), "partition", *(*p), "replica", *r);
                } else if (OB_FAIL(task_info.build(remove_member, pkey, quorum, (*p)->get_quorum()))) {
                  LOG_WARN("fail to build remove member task info", K(ret));
                } else if (OB_FAIL(task_info_array.push_back(task_info))) {
                  LOG_WARN("fail to push back", K(ret));
                } else if (OB_FAIL(task.build(task_info_array, remove_member.member_.get_server(), comment))) {
                  LOG_WARN("fail to build remove member task", K(ret));
                } else if (OB_FAIL(task_mgr_->add_task(task))) {
                  LOG_WARN("fail to add task", K(ret), K(task));
                } else {
                  LOG_INFO("add task to remove member", K(task), "partition", *(*p), K(task_cnt));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObArray<uint64_t> tenant_ids;
          ObAdminClearBalanceTaskArg::TaskType type = obrpc::ObAdminClearBalanceTaskArg::ALL;
          if (OB_FAIL(task_mgr_->clear_task(tenant_ids, type))) {
            LOG_WARN("fail ti flush task", K(ret));
          } else {
            task_cnt++;
          }
        }
        break;
      } else {
        // nothing todo
      }
    }
  }
  return ret;
}

int ObRootBalancer::check_tenant_group_config_legality(
    common::ObIArray<ObTenantGroupParser::TenantNameGroup>& tenant_groups, bool& legal)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(server_balancer_.check_tenant_group_config_legality(tenant_groups, legal))) {
    LOG_WARN("fail to check tenant group config legality", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObRootBalancer::fill_faillist(const int64_t count, const common::ObIArray<uint64_t>& table_ids,
    const common::ObIArray<int64_t>& partition_ids, const common::ObIArray<common::ObAddr>& servers,
    const share::ObPartitionReplica::FailList& fail_msgs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(GCTX.root_service_->submit_black_list_task(count, table_ids, partition_ids, servers, fail_msgs))) {
    LOG_WARN(
        "failed to add task to asyn queue", K(ret), K(count), K(table_ids), K(partition_ids), K(servers), K(fail_msgs));
  }
  return ret;
}

int64_t ObRootBalancer::get_schedule_interval() const
{
  return idling_.get_idle_interval_us();
}

int64_t ObBlacklistProcess::get_deep_copy_size() const
{
  return sizeof(*this);
}

ObAsyncTask* ObBlacklistProcess::deep_copy(char* buf, const int64_t buf_size) const
{
  ObAsyncTask* task = NULL;
  int ret = OB_SUCCESS;
  const int64_t need_size = get_deep_copy_size();
  if (!init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is null", K(ret));
  } else if (buf_size < need_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is not long enough", K(need_size), K(buf_size), K(ret));
  } else {
    task = new (buf) ObBlacklistProcess();
    if (OB_FAIL(static_cast<ObBlacklistProcess*>(task)->init(
            count_, *pt_, table_ids_, partition_ids_, servers_, fail_msgs_))) {
      LOG_WARN("failed to init new task", K(ret));
    }
  }
  return task;
}

int ObBlacklistProcess::init(const int64_t count, share::ObPartitionTableOperator& pt,
    const common::ObIArray<uint64_t>& table_ids, const common::ObIArray<int64_t>& partition_ids,
    const common::ObIArray<common::ObAddr>& servers, const share::ObPartitionReplica::FailList& fail_msgs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_ids_.assign(table_ids))) {
    LOG_WARN("failed to copy table id", K(ret));
  } else if (OB_FAIL(partition_ids_.assign(partition_ids))) {
    LOG_WARN("failed to copy partition id", K(ret));
  } else if (OB_FAIL(servers_.assign(servers))) {
    LOG_WARN("failed to set server", K(ret));
  } else if (OB_FAIL(copy_assign(fail_msgs_, fail_msgs))) {
    LOG_WARN("failed to set failmsg", K(ret));
  } else {
    init_ = true;
    count_ = count;
    pt_ = &pt;
  }
  return ret;
}

int ObBlacklistProcess::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("do insert black list", K(count_), K(fail_msgs_));
  if (!init_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(pt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition operator is null", K(ret));
  } else if (count_ != table_ids_.count() || count_ != partition_ids_.count() || count_ != servers_.count() ||
             count_ != fail_msgs_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to do check", K(count_), K(table_ids_), K(partition_ids_), K(servers_), K(fail_msgs_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_ids_.count(); ++i) {
    ObPartitionReplica::FailList fail_list;
    if (OB_FAIL(fail_list.push_back(fail_msgs_.at(i)))) {
      LOG_WARN("failed to push back fail msg", K(ret), K(fail_msgs_));
    } else if (OB_FAIL(pt_->update_fail_list(table_ids_.at(i), partition_ids_.at(i), servers_.at(i), fail_list))) {
      LOG_WARN("failed to process update fail list", K(ret), K(fail_list), K(i));
    }
  }  // end for
  return ret;
}
}  // end namespace rootserver
}  // end namespace oceanbase
