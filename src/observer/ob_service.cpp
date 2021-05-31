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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_service.h"

#include <new>
#include <string.h>
#include <cmath>

#include "share/ob_define.h"
#include "lib/ob_running_mode.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/utility/utility.h"
#include "lib/time/ob_tsc_timestamp.h"

#include "common/ob_partition_key.h"
#include "common/ob_member_list.h"
#include "common/ob_zone.h"
#include "share/ob_version.h"

#include "share/ob_version.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_table_proxy.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_sstable_checksum_operator.h"
#include "share/ob_pg_partition_meta_table_operator.h"
#include "share/ob_zone_table_operation.h"

#include "storage/ob_partition_migrator.h"
#include "storage/ob_partition_component_factory.h"
#include "storage/ob_partition_migrator.h"
#include "storage/ob_i_table.h"
#include "storage/ob_pg_partition.h"
#include "storage/ob_wrs_utils.h"  // get_tenant_replica_wrs_info
#include "storage/ob_partition_scheduler.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_join_order.h"
#include "rootserver/ob_bootstrap.h"
#include "observer/ob_server.h"
#include "observer/ob_dump_task_generator.h"
#include "observer/ob_server_schema_updater.h"
#include "ob_server_event_history_table_operator.h"

namespace oceanbase {

using namespace common;
using namespace rootserver;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace observer {

static int64_t total_get_member_list_and_leader_cnt = 0;

ObSchemaReleaseTimeTask::ObSchemaReleaseTimeTask() : schema_updater_(nullptr), is_inited_(false)
{}

int ObSchemaReleaseTimeTask::init(ObServerSchemaUpdater& schema_updater, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSchemaReleaseTimeTask has already been inited", K(ret));
  } else {
    schema_updater_ = &schema_updater;
    is_inited_ = true;
    if (OB_FAIL(TG_SCHEDULE(tg_id, *this, REFRESH_INTERVAL, true /*schedule repeatly*/))) {
      LOG_WARN("fail to schedule task ObSchemaReleaseTimeTask", K(ret));
    }
  }
  return ret;
}

void ObSchemaReleaseTimeTask::destroy()
{
  is_inited_ = false;
  schema_updater_ = nullptr;
}

void ObSchemaReleaseTimeTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSchemaReleaseTimeTask has not been inited", K(ret));
  } else if (OB_ISNULL(schema_updater_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSchemaReleaseTimeTask task got null ptr", K(ret));
  } else if (OB_FAIL(schema_updater_->try_release_schema())) {
    LOG_WARN("ObSchemaReleaseTimeTask failed", K(ret));
  }
}

//////////////////////////////////////

// here gctx may hasn't been initialized already
ObService::ObService(const ObGlobalContext& gctx)
    : inited_(false),
      in_register_process_(false),
      service_started_(false),
      stopped_(false),
      schema_updater_(),
      partition_table_updater_(),
      index_status_report_queue_(),
      rebuild_flag_report_queue_(),
      pt_checker_(),
      lease_state_mgr_(),
      heartbeat_process_(gctx, schema_updater_, lease_state_mgr_),
      gctx_(gctx)
{}

ObService::~ObService()
{}

int ObService::init(common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;

  const static int64_t INDEX_STATUS_REPORT_THREAD_CNT = 1;
  const static int64_t REBUILD_FLAG_REPORT_THREAD_CNT = 1;

  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Oceanbase service has already init", K(ret));
  } else if (!gctx_.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gctx not init", "gctx inited", gctx_.is_inited(), K(ret));
  } else if (OB_FAIL(heartbeat_process_.init())) {
    LOG_WARN("heartbeat_process_.init failed", K(ret));
  } else if (OB_FAIL(schema_updater_.init(gctx_.self_addr_, gctx_.schema_service_))) {
    LOG_ERROR("client_manager_.initialize failed", "self_addr", gctx_.self_addr_, K(ret));
  } else if (OB_FAIL(partition_table_updater_.init())) {
    LOG_WARN("init partition table updater failed", K(ret));
  } else if (OB_FAIL(checksum_updater_.init())) {
    LOG_WARN("fail to init checksum updater", K(ret));
  } else if (OB_FAIL(ObPGPartitionMTUpdater::get_instance().init())) {
    LOG_WARN("fail to init checksum updater", K(ret));
  } else if (OB_FAIL(index_status_report_queue_.init(&index_updater_,
                 INDEX_STATUS_REPORT_THREAD_CNT,
                 !lib::is_mini_mode() ? ObPartitionTableUpdater::MAX_PARTITION_CNT
                                      : ObPartitionTableUpdater::MINI_MODE_MAX_PARTITION_CNT,
                 "IdxStatRp"))) {
    LOG_WARN("init index status report queue failed",
        K(ret),
        LITERAL_K(INDEX_STATUS_REPORT_THREAD_CNT),
        "queue_size",
        static_cast<int64_t>(ObPartitionTableUpdater::MAX_PARTITION_CNT));
  } else if (OB_FAIL(rebuild_flag_report_queue_.init(&rebuild_updater_,
                 REBUILD_FLAG_REPORT_THREAD_CNT,
                 !lib::is_mini_mode() ? ObPartitionTableUpdater::MAX_PARTITION_CNT
                                      : ObPartitionTableUpdater::MINI_MODE_MAX_PARTITION_CNT,
                 "RebuFlagRp"))) {
    LOG_WARN("init rebuild flag report queue failed",
        K(ret),
        LITERAL_K(REBUILD_FLAG_REPORT_THREAD_CNT),
        "queue_size",
        static_cast<int64_t>(ObPartitionTableUpdater::MAX_PARTITION_CNT));
  } else if (OB_FAIL(pt_checker_.init(
                 *gctx_.pt_operator_, *gctx_.schema_service_, *gctx_.par_ser_, lib::TGDefIDs::ServerGTimer))) {
    LOG_WARN("init pt checker failed", K(ret));
  } else if (OB_FAIL(SERVER_EVENT_INSTANCE.init(sql_proxy, gctx_.self_addr_))) {
    LOG_WARN("init server event history table failed", K(ret));
  } else if (OB_FAIL(ObAllServerTracer::get_instance().init(lib::TGDefIDs::ServerGTimer, server_trace_task_))) {
    STORAGE_LOG(WARN, "fail to init ObAllServerTracer", K(ret));
  } else if (OB_FAIL(OB_TSC_TIMESTAMP.init())) {
    LOG_WARN("fail to init tsc timestamp", K(ret));
  } else if (OB_FAIL(schema_release_task_.init(schema_updater_, lib::TGDefIDs::ServerGTimer))) {
    LOG_WARN("fail to init schema release task", K(ret));
  } else if (OB_FAIL(tenant_backup_task_updater_.init(sql_proxy))) {
    LOG_WARN("failed to init tenant backup task updater", K(ret));
  } else if (OB_FAIL(pg_backup_task_updater_.init(sql_proxy))) {
    LOG_WARN("failed to init pg backup task updater", K(ret));
  } else if (OB_FAIL(tenant_validate_task_updater_.init(sql_proxy))) {
    LOG_WARN("failed to init tenant validate task updater", K(ret));
  } else if (OB_FAIL(pg_validate_task_updater_.init(sql_proxy))) {
    LOG_WARN("failed to init pg validate task updater", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObService::register_self()
{
  int ret = OB_SUCCESS;
  in_register_process_ = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("service not initialized, can't register self", K(ret));
  } else if (!lease_state_mgr_.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("lease_state_mgr_ not init", K(ret));
  } else if (OB_FAIL(lease_state_mgr_.register_self_busy_wait())) {
    LOG_WARN("register self failed", K(ret));
  } else if (!lease_state_mgr_.is_valid_heartbeat()) {
    ret = OB_ERROR;
    LOG_ERROR("can't renew lease", K(ret), "heartbeat_expire_time", lease_state_mgr_.get_heartbeat_expire_time());
  } else {
    in_register_process_ = false;
    service_started_ = true;
    SERVER_EVENT_ADD("observice", "register");
  }
  return ret;
}

int ObService::start()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  }

  // lease service
  if (OB_SUCC(ret)) {
    if (OB_FAIL(lease_state_mgr_.init(gctx_.rs_rpc_proxy_, gctx_.rs_mgr_, &heartbeat_process_, *this))) {
      LOG_ERROR("lease_state_mgr_.init failed", K(ret));
    } else if (OB_FAIL(register_self())) {
      LOG_ERROR("register self failed", K(ret));
    } else {
      LOG_INFO("[NOTICE] regist to rs success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(report_replica())) {
      LOG_WARN("request report replica failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(schedule_pt_check_task())) {
      LOG_WARN("schedule_pt_check_task failed", K(ret));
    }
  }
  return ret;
}

void ObService::set_stop()
{
  LOG_INFO("[NOTICE] observice need stop now");
  lease_state_mgr_.set_stop();
}

void ObService::stop()
{
  if (!inited_) {
    LOG_WARN("not init");
  } else {
    LOG_INFO("[NOTICE] start to stop");
    SERVER_EVENT_ADD("observer", "stop");
    service_started_ = false;
    stopped_ = true;
    schema_updater_.stop();
    partition_table_updater_.stop();
    checksum_updater_.stop();
    ObPGPartitionMTUpdater::get_instance().stop();
    index_status_report_queue_.stop();
    rebuild_flag_report_queue_.stop();
    pt_checker_.stop();
    LOG_INFO("[NOTICE] observice finish stop");
  }
}

void ObService::wait()
{
  if (!inited_) {
    LOG_WARN("not init");
  } else {
    schema_updater_.wait();
    partition_table_updater_.wait();
    checksum_updater_.wait();
    ObPGPartitionMTUpdater::get_instance().wait();
    index_status_report_queue_.wait();
    rebuild_flag_report_queue_.wait();
  }
}

int ObService::destroy()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_updater_.destroy();
    lease_state_mgr_.destroy();
    if (OB_FAIL(pt_checker_.destroy())) {
      LOG_WARN("pt_checker_ destroy failed", K(ret));
    }
    SERVER_EVENT_INSTANCE.destroy();
    LOG_INFO("server event table operator destroy");
  }
  return ret;
}

int ObService::batch_get_role(const ObBatchGetRoleArg& arg, ObBatchGetRoleResult& result)
{
  int ret = OB_SUCCESS;
  int64_t count = arg.keys_.count();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(result.results_.reserve(count))) {
    LOG_WARN("fail to init results", K(ret), K(arg));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      const ObPartitionKey& key = arg.keys_.at(i);
      ObRole role;
      int res_ret = OB_SUCCESS;
      if (!key.is_valid()) {
        res_ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(res_ret), K(key));
      } else if (OB_SUCCESS != (res_ret = gctx_.par_ser_->get_role(key, role))) {
        LOG_WARN("fail to get role", K(res_ret), K(key));
      } else if (!is_strong_leader(role)) {
        res_ret = OB_NOT_MASTER;
      }
      if (OB_FAIL(result.results_.push_back(res_ret))) {
        LOG_WARN("fail to push back ret", K(ret), K(res_ret));
      }
    }
  }
  return ret;
}

int ObService::get_role(const ObPartitionKey& part_key, common::ObRole& role)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!part_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(part_key), K(ret));
  } else if (gctx_.par_ser_->get_role(part_key, role)) {
    LOG_WARN("fail to get role", K(ret), K(part_key));
  }
  return ret;
}

int ObService::get_leader_member(const ObPartitionKey& part_key, ObIArray<ObAddr>& member_list)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!part_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(part_key), K(ret));
  } else {
    ObMemberList ml;
    if (OB_FAIL(gctx_.par_ser_->get_leader_curr_member_list(part_key, ml))) {
      LOG_WARN("get leader current member list failed", K(ret));
    } else if (ml.get_member_number() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid member list", K(ml), K(ret));
    } else {
      member_list.reuse();
      ObAddr addr;
      for (int64_t i = 0; OB_SUCC(ret) && i < ml.get_member_number(); ++i) {
        addr.reset();
        if (OB_FAIL(ml.get_server_by_index(i, addr))) {
          LOG_WARN("get member failed", K(ret), K(i), K(ml));
        } else if (!addr.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid addr", K(addr), K(ret));
        } else if (OB_FAIL(member_list.push_back(addr))) {
          LOG_WARN("push to array failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObService::update_baseline_schema_version(const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService* schema_service = gctx_.schema_service_;
  if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(schema_version));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", KR(ret));
  } else if (OB_FAIL(schema_service->update_baseline_schema_version(schema_version))) {
    LOG_WARN("fail to update baseline schema version", KR(ret), K(schema_version));
  } else {
    LOG_INFO("update baseline schema version success", K(schema_version));
  }
  return ret;
}

int ObService::get_member_list_and_leader(const common::ObPartitionKey& part_key, obrpc::ObMemberListAndLeaderArg& arg)
{
  int ret = OB_SUCCESS;
  ObMemberList member_list;
  ObChildReplicaList children_list;
  ObAddr leader;
  ObRole role;
  ObAddr server;
  ObReplicaType replica_type = common::ObReplicaType::REPLICA_TYPE_MAX;
  ObReplicaProperty property;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!part_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(part_key));
  } else if (OB_FAIL(gctx_.par_ser_->get_curr_leader_and_memberlist(
                 part_key, leader, role, member_list, children_list, replica_type, property))) {
    LOG_WARN("failed to get get_curr_leader_and_memberlist", K(ret), K(part_key));
  } else {
    arg.reset();
    arg.leader_ = leader;
    arg.self_ = gctx_.self_addr_;
    arg.replica_type_ = replica_type;
    arg.property_ = property;
    arg.role_ = role;
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
      if (OB_FAIL(member_list.get_server_by_index(i, server))) {
        LOG_WARN("failed to get server by index", K(ret), K(i));
      } else if (OB_FAIL(arg.member_list_.push_back(server))) {
        LOG_WARN("failed to add member", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(arg.lower_list_.assign(children_list))) {
      LOG_WARN("fail to deep copy", K(ret));
    }
  }
  static const int64_t STATISTICS_INTERVAL_US = 10000000;
  ATOMIC_INC(&total_get_member_list_and_leader_cnt);
  if (REACH_TIME_INTERVAL(STATISTICS_INTERVAL_US)) {
    LOG_INFO("get member_list_and_leader statistics",
        K(total_get_member_list_and_leader_cnt),
        "avg_rpc",
        total_get_member_list_and_leader_cnt / (STATISTICS_INTERVAL_US / 1000000));
    ATOMIC_STORE(&total_get_member_list_and_leader_cnt, 0);
  }
  return ret;
}

int ObService::get_member_list_and_leader_v2(
    const common::ObPartitionKey& part_key, obrpc::ObGetMemberListAndLeaderResult& arg)
{
  int ret = OB_SUCCESS;
  ObMemberList member_list;
  ObChildReplicaList children_list;
  ObAddr leader;
  ObRole role;
  ObMember member;
  ObReplicaType replica_type = common::ObReplicaType::REPLICA_TYPE_MAX;
  ObReplicaProperty property;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!part_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(part_key));
  } else if (OB_FAIL(gctx_.par_ser_->get_curr_leader_and_memberlist(
                 part_key, leader, role, member_list, children_list, replica_type, property))) {
    LOG_WARN("failed to get get_curr_leader_and_memberlist", K(ret), K(part_key));
  } else {
    arg.reset();
    arg.leader_ = leader;
    arg.self_ = gctx_.self_addr_;
    arg.replica_type_ = replica_type;
    arg.property_ = property;
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
      if (OB_FAIL(member_list.get_member_by_index(i, member))) {
        LOG_WARN("failed to get server by index", K(ret), K(i));
      } else if (OB_FAIL(arg.member_list_.push_back(member))) {
        LOG_WARN("failed to add member", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(arg.lower_list_.assign(children_list))) {
      LOG_WARN("fail to deep copy", K(ret));
    }
  }
  static const int64_t STATISTICS_INTERVAL_US = 10000000;
  ATOMIC_INC(&total_get_member_list_and_leader_cnt);
  if (REACH_TIME_INTERVAL(STATISTICS_INTERVAL_US)) {
    LOG_INFO("get member_list_and_leader statistics",
        K(total_get_member_list_and_leader_cnt),
        "avg_rpc",
        total_get_member_list_and_leader_cnt / (STATISTICS_INTERVAL_US / 1000000));
    ATOMIC_STORE(&total_get_member_list_and_leader_cnt, 0);
  }
  return ret;
}

int ObService::batch_get_member_list_and_leader(
    const obrpc::ObLocationRpcRenewArg& arg, obrpc::ObLocationRpcRenewResult& res)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(arg));
  } else {
    ObMemberListAndLeaderArg result;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.keys_.count(); i++) {
      const ObPartitionKey& pkey = arg.keys_.at(i);
      result.reset();
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = get_member_list_and_leader(pkey, result))) {
        LOG_WARN("fail to get member_list and leader", K(tmp_ret), K(pkey));
      }
      // always push back result
      if (OB_FAIL(res.results_.push_back(result))) {
        LOG_WARN("push back result failed", K(ret), K(pkey), K(result));
      }
    }
  }
  return ret;
}

int ObService::get_pg_key(const ObPartitionKey& pkey, ObPGKey& pg_key) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not inited", K(ret));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pkey), K(ret));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_service is null", K(ret));
  } else if (pkey.is_pg()) {
    pg_key = pkey;
  } else {
    ObPartitionGroupIndex& pg_index = gctx_.par_ser_->get_pg_index();
    if (OB_FAIL(pg_index.get_pg_key(pkey, pg_key))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        pg_key = pkey;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get pg_key", K(ret), K(pkey));
      }
    }
  }
  return ret;
}

int ObService::fill_partition_replica(const ObPGKey& pg_key, share::ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* part = NULL;
  ObIPartitionGroupGuard guard;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not inited", K(ret));
  } else if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pg_key), K(ret));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_service is null", K(ret));
  } else {
    const ObPGMgr& pg_mgr = gctx_.par_ser_->get_pg_mgr();
    if (OB_FAIL(pg_mgr.get_pg(pg_key, nullptr /*file_id*/, guard))) {
      LOG_WARN("fail to get pg guard", K(pg_key));
    } else if (NULL == (part = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part is null", KP(part), K(ret));
    } else if (OB_FAIL(fill_partition_replica(part, replica))) {
      LOG_WARN("failed to fill_partition_replica", K(ret), K(pg_key));
    }
  }
  return ret;
}

int ObService::fill_partition_replica(storage::ObIPartitionGroup* part, share::ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("service not inited", K(ret));
  } else if (OB_ISNULL(part)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(part));
  } else {
    const ObPartitionKey& part_key = part->get_partition_key();
    replica.reset();
    replica.table_id_ = part_key.table_id_;
    replica.partition_id_ = part_key.get_partition_id();
    replica.partition_cnt_ = part_key.get_partition_cnt();
    replica.zone_ = gctx_.config_->zone.str();
    replica.server_ = gctx_.self_addr_;
    replica.sql_port_ = gctx_.config_->mysql_port;

    int64_t leader_active_time = 0;
    if (!part->is_valid()) {
      ret = OB_INVALID_PARTITION;
      LOG_WARN("invalid partition", K(ret), K(part_key));
    } else if (OB_ISNULL(part->get_log_service())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL partition log service", K(ret), K(part_key), KP(part));
    } else if (OB_FAIL(
                   part->get_log_service()->get_role_and_last_leader_active_time(replica.role_, leader_active_time))) {
      LOG_WARN("get partition role failed", K(ret), K(part_key));
      //  After the version number is greater than or equal to 2.1,
      //  the observer schedules the rebuild task by itself,
      //  but still needs to report the rebuild tag,
      //  because it needs to skip the replica in rebuild while major merge
    } else if (OB_FAIL(part->get_log_service()->is_need_rebuild(replica.rebuild_))) {
      LOG_WARN("get partition rebuild state failed", K(ret), K(part_key));
    } else if (OB_FAIL(part->get_log_service()->get_replica_num(replica.quorum_))) {
      LOG_WARN("get partition quorum failed", K(ret), K(part_key));
    } else {
      if (replica.is_leader_like()) {
        replica.to_leader_time_ = leader_active_time;
      }
      if (OB_FAIL(part->fill_replica(replica))) {
        LOG_WARN("fill replica failed", K(ret), K(part_key));
        // temporary online replica doesn't have sstable
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        }
      }

      if (OB_FAIL(ret)) {
      } else {
        ObMemberList member_list;
        if (OB_FAIL(part->get_curr_member_list_for_report(member_list))) {
          LOG_WARN("get_curr_member_list_for_report failed", K(ret));
        } else {
          ObMember member;
          for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
            member.reset();
            if (OB_FAIL(member_list.get_member_by_index(i, member))) {
              LOG_WARN("get_member_by_index failed", K(i), K(ret));
            } else if (!member.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("member is invalid", K(member), K(ret));
            } else {
              if (OB_FAIL(replica.member_list_.push_back(
                      ObPartitionReplica::Member(member.get_server(), member.get_timestamp())))) {
                LOG_WARN("push_back failed", K(ret));
              }
            }
          }
        }
      }
    }
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("finish fill partition replica", K(ret), K(part_key), K(replica));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_TRACE("finish fill partition replica", K(ret), K(part_key), K(replica));
    }
  }
  return ret;
}

const ObAddr& ObService::get_self_addr()
{
  return gctx_.self_addr_;
}

int ObService::fill_partition_table_update_task(const common::ObPartitionKey& pkey, ObPGPartitionMTUpdateItem& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid()) || pkey.is_pg()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pkey));
  } else {
    ObIPartitionGroupGuard guard;
    ObReportStatus report_status;
    common::ObRole role;
    ObReplicaStatus status;
    int64_t leader_active_time = 0;
    int checksum = 0;
    if (OB_FAIL(gctx_.par_ser_->get_partition(pkey, guard))) {
      LOG_WARN("fail to get partition", K(ret), K(pkey));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, partition must not be NULL", K(ret));
    } else if (OB_FAIL(guard.get_partition_group()->fill_pg_partition_replica(pkey, status, report_status))) {
      LOG_WARN("get pg partition fail", K(ret), K(pkey));
    } else if (OB_FAIL(guard.get_partition_group()->get_log_service()->get_role_and_last_leader_active_time(
                   role, leader_active_time))) {
      LOG_WARN("get partition role failed", K(ret), K(pkey));
    } else {
      item.tenant_id_ = pkey.get_tenant_id();
      item.table_id_ = pkey.get_table_id();
      item.partition_id_ = pkey.get_partition_id();
      item.svr_ip_ = GCTX.self_addr_;
      item.role_ = role;
      item.status_ = status;
      item.replica_type_ = guard.get_partition_group()->get_replica_type();
      item.row_count_ = report_status.row_count_;
      item.data_size_ = report_status.data_size_;
      item.required_size_ = report_status.required_size_;
      item.data_version_ = report_status.data_version_;
      item.data_checksum_ = report_status.data_checksum_;
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("fill pg partition replica success", K(pkey), K(item));
  }

  return ret;
}

int ObService::fill_checksum(const common::ObPartitionKey& pkey, const uint64_t sstable_id, const int sstable_type,
    const ObSSTableChecksumUpdateType update_type, common::ObIArray<ObSSTableDataChecksumItem>& data_checksum_items,
    common::ObIArray<ObSSTableColumnChecksumItem>& column_checksum_items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pkey));
  } else {
    ObPartitionStorage* storage = NULL;
    ObIPartitionGroupGuard guard;
    ObPGPartitionGuard pg_partition_guard;
    ObSEArray<uint64_t, OB_MAX_SSTABLE_PER_TABLE> sstable_ids;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema* main_table_schema = NULL;
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    const uint64_t fetch_tenant_id = is_inner_table(pkey.get_table_id()) ? OB_SYS_TENANT_ID : pkey.get_tenant_id();
    if (OB_FAIL(gctx_.par_ser_->get_partition(pkey, guard))) {
      LOG_WARN("fail to get partition", K(ret), K(pkey));
    } else if (OB_ISNULL(guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, partition must not be NULL", K(ret));
    } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(pkey, pg_partition_guard))) {
      LOG_WARN("get pg partition fail", K(ret), K(pkey));
    } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("pg partition not exist", K(ret), K(pkey));
    } else if (OB_ISNULL(
                   storage = static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, partition storage must not be NULL", K(ret));
    } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_full_schema_guard(
                   fetch_tenant_id, schema_guard))) {
      STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(fetch_tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(pkey.get_table_id(), main_table_schema))) {
      STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(pkey));
    } else {
      bool need_report_column_checksum = false;
      if (nullptr == main_table_schema) {
        need_report_column_checksum = true;
      } else {
        if (main_table_schema->is_global_index_table()) {
          need_report_column_checksum = true;
        } else if (OB_FAIL(main_table_schema->get_simple_index_infos(simple_index_infos))) {
          STORAGE_LOG(WARN, "fail to get index tid array", K(ret));
        } else {
          const ObSimpleTableSchemaV2* index_schema = nullptr;
          for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count() && !need_report_column_checksum; ++i) {
            if (OB_FAIL(schema_guard.get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
              STORAGE_LOG(WARN, "fail to get table schema", K(ret));
            } else if (OB_ISNULL(index_schema)) {
              ret = OB_TABLE_NOT_EXIST;
              STORAGE_LOG(WARN, "fail to get index schema", K(ret), K(simple_index_infos.at(i).table_id_));
            } else if (index_schema->is_global_index_table()) {
              need_report_column_checksum = true;
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        const bool need_update_data_checksum = observer::need_update_data_checksum(update_type);
        const bool need_update_column_checksum =
            need_report_column_checksum && observer::need_update_column_checksum(update_type);
        if (OB_INVALID_ID == sstable_id) {
          if (OB_FAIL(storage->get_partition_store().get_all_table_ids(sstable_ids))) {
            LOG_WARN("fail to get all table ids", K(ret));
          }
        } else {
          if (OB_FAIL(sstable_ids.push_back(sstable_id))) {
            LOG_WARN("fail to push back sstable id", K(ret));
          }
        }

        for (int64_t i = 0; OB_SUCC(ret) && i < sstable_ids.count(); ++i) {
          ObSSTableChecksumItem item;
          ObSSTableDataChecksumItem& data_checksum = item.data_checksum_;
          data_checksum.tenant_id_ = extract_tenant_id(pkey.get_table_id());
          data_checksum.data_table_id_ = pkey.get_table_id();
          data_checksum.partition_id_ = pkey.get_partition_id();
          data_checksum.server_ = GCTX.self_addr_;
          if (OB_INVALID_ID == sstable_id) {
            const int table_types[] = {
                ObITable::MAJOR_SSTABLE, ObITable::MINOR_SSTABLE, ObITable::MULTI_VERSION_MINOR_SSTABLE};
            for (int64_t j = 0; OB_SUCC(ret) && j < sizeof(table_types) / sizeof(int); ++j) {
              item.reset();
              if (OB_FAIL(storage->fill_checksum(sstable_ids.at(i), table_types[j], item))) {
                if (OB_ENTRY_NOT_EXIST != ret) {
                  LOG_WARN("fail to fill checksum", K(ret), "sstable_id", sstable_ids.at(i));
                } else {
                  ret = OB_SUCCESS;
                }
              } else if (need_update_data_checksum && OB_FAIL(data_checksum_items.push_back(item.data_checksum_))) {
                LOG_WARN("fail to push back item", K(ret));
              } else if (need_update_column_checksum) {
                for (int64_t k = 0; OB_SUCC(ret) && k < item.column_checksum_.count(); ++k) {
                  if (OB_FAIL(column_checksum_items.push_back(item.column_checksum_.at(k)))) {
                    LOG_WARN("fail to push back column checksum item", K(ret));
                  }
                }
              }
            }
          } else {
            if (OB_FAIL(storage->fill_checksum(sstable_ids.at(i), sstable_type, item))) {
              if (OB_ENTRY_NOT_EXIST != ret) {
                LOG_WARN("fail to fill checksum", K(ret), "sstable_id", sstable_ids.at(i));
              } else {
                ret = OB_SUCCESS;
              }
            } else if (need_update_data_checksum && OB_FAIL(data_checksum_items.push_back(item.data_checksum_))) {
              LOG_WARN("fail to push back item", K(ret));
            } else if (need_update_column_checksum) {
              for (int64_t k = 0; OB_SUCC(ret) && k < item.column_checksum_.count(); ++k) {
                if (OB_FAIL(column_checksum_items.push_back(item.column_checksum_.at(k)))) {
                  LOG_WARN("fail to push back column checksum item", K(ret));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObService::pt_sync_update(const ObPartitionKey& part_key)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!part_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(part_key), K(ret));
  } else if (OB_FAIL(partition_table_updater_.sync_update(part_key))) {
    LOG_WARN("async_update failed", K(part_key), K(ret));
  }
  return ret;
}

void ObService::submit_pg_pt_update_task(const ObPartitionArray& pg_partition_keys)
{
  int ret = OB_SUCCESS;
  const int64_t version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // Batch update all partition information under PG
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_partition_keys.count(); ++i) {
      if (OB_FAIL(ObPGPartitionMTUpdater::get_instance().add_task(
              pg_partition_keys.at(i), ObPGPartitionMTUpdateType::INSERT_ON_UPDATE, version, GCTX.self_addr_))) {
        LOG_WARN("fail to async update pg partition meta table", K(ret), K(pg_partition_keys.at(i)));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("async update pg partition meta table success", K(pg_partition_keys));
    }
  }
  UNUSED(ret);
}

int ObService::submit_pt_update_role_task(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pkey));
  } else if (OB_FAIL(partition_table_updater_.async_update_partition_role(pkey))) {
    LOG_WARN("fail to async update partition role", KR(ret), K(pkey));
  } else {
    // do nothing
  }
  return ret;
}

int ObService::submit_pt_update_task(
    const ObPartitionKey& part_key, const bool need_report_checksum, const bool with_role)
{
  int ret = OB_SUCCESS;
  const bool is_remove = false;
  const int64_t version = 0;
  const ObSSTableChecksumUpdateType update_type = ObSSTableChecksumUpdateType::UPDATE_ALL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!part_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(part_key), K(ret));
  } else if (OB_FAIL(partition_table_updater_.async_update(part_key, with_role))) {
    LOG_WARN("async_update failed", K(part_key), K(ret));
  } else if (need_report_checksum && !part_key.is_pg() &&
             OB_FAIL(checksum_updater_.add_task(part_key, is_remove, update_type))) {
    LOG_WARN("fail to async update sstable checksum", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObService::submit_checksum_update_task(const ObPartitionKey& part_key, const uint64_t sstable_id,
    const int sstable_type, const ObSSTableChecksumUpdateType update_type, const bool task_need_batch)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!part_key.is_valid() || !is_valid_checksum_update_type(update_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(part_key), K(ret), K(part_key), K(sstable_id), K(sstable_type), K(update_type));
  } else if (OB_FAIL(checksum_updater_.add_task(
                 part_key, false /*is remove*/, update_type, sstable_id, sstable_type, task_need_batch))) {
    LOG_WARN("fail to async update sstable checksum", K(ret));
  } else if (!part_key.is_pg()) {
    const int64_t version = 0;
    if (OB_FAIL(ObPGPartitionMTUpdater::get_instance().add_task(
            part_key, ObPGPartitionMTUpdateType::INSERT_ON_UPDATE, version, GCTX.self_addr_))) {
      LOG_WARN("fail to async update pg partition meta table", K(ret), K(part_key));
    }
  }

  return ret;
}

int ObService::submit_async_refresh_schema_task(const uint64_t tenant_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id ||
             !ObSchemaService::is_formal_version(schema_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(schema_updater_.async_refresh_schema(tenant_id, schema_version))) {
    LOG_WARN("fail to async refresh schema", K(ret), K(tenant_id), K(schema_version));
  }
  return ret;
}

int ObService::report_rebuild_replica(const common::ObPartitionKey& part_key, const common::ObAddr& server,
    const storage::ObRebuildSwitch& rebuild_switch)
{
  int ret = OB_SUCCESS;
  bool rebuild = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!part_key.is_valid() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_key), K(server));
  } else if (OB_ISNULL(gctx_.pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL partition table operator", K(ret));
  } else {
    switch (rebuild_switch) {
      case storage::OB_REBUILD_ON:
        rebuild = true;
        break;
      case storage::OB_REBUILD_OFF:
        rebuild = false;
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unkown rebuild switch", K(part_key), K(server), K(rebuild_switch), K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(gctx_.pt_operator_->update_rebuild_flag(
              part_key.get_table_id(), part_key.get_partition_id(), server, rebuild))) {
        LOG_WARN("update partition table rebuild flag failed", K(ret), K(part_key), K(server), K(rebuild));
      }
    }
  }
  return ret;
}

int ObService::report_rebuild_replica_async(const common::ObPartitionKey& part_key, const common::ObAddr& server,
    const storage::ObRebuildSwitch& rebuild_switch)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!part_key.is_valid() || !server.is_valid() || OB_REBUILD_INVALID == rebuild_switch) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_key), K(server), K(rebuild_switch));
  } else {
    ObRebuildFlagReporter task;
    if (OB_FAIL(task.init(part_key, server, rebuild_switch))) {
      LOG_WARN("init rebulid flag report task failed", K(ret), K(part_key), K(server), K(rebuild_switch));
    } else {
      ret = rebuild_flag_report_queue_.add(task);
      LOG_INFO("submit rebuild flag report task", K(ret), K(task));
      if (OB_FAIL(ret)) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("submit rebuild flag report task failed", K(ret), K(task));
        }
      }
    }
  }
  return ret;
}

int ObService::schedule_pt_check_task()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // to make different start check partition table task in different time
    const int64_t interval_in_sec = GCONF.partition_table_check_interval / 1000000;
    const int64_t delay = (random() % interval_in_sec) * 1000000;
    const int64_t repeat = false;
    if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ServerGTimer, pt_checker_, delay, repeat))) {
      LOG_WARN("schedule partition table check task failed", K(delay), K(repeat), K(ret));
    }
  }
  return ret;
}

int ObService::report_merge_finished(int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (frozen_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(frozen_version));
  } else {
    if (frozen_version > 0) {
      if (OB_FAIL(partition_table_updater_.sync_merge_finish(frozen_version))) {
        LOG_WARN("add report merge finish task failed", K(ret), K(frozen_version));
      }
      SERVER_EVENT_ADD("daily_merge", "report_merge_finished", K(ret), K(frozen_version));
    }
  }
  return ret;
}

int ObService::report_merge_error(const common::ObPartitionKey& part_key, const int error_code)
{
  int ret = OB_SUCCESS;
  LOG_WARN("merge error", K(part_key), K(error_code));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!part_key.is_valid() || OB_SUCCESS == error_code) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_key), K(error_code));
  } else {
    ObMergeErrorArg arg;
    arg.partition_key_ = part_key;
    arg.server_ = gctx_.self_addr_;
    arg.error_code_ = error_code;
    obrpc::ObCommonRpcProxy rs_rpc = *gctx_.rs_rpc_proxy_;
    rs_rpc.set_rs_mgr(*gctx_.rs_mgr_);
    if (OB_FAIL(rs_rpc.merge_error(arg))) {
      LOG_WARN("report merge error failed", K(ret), K(arg));
    }
    SERVER_EVENT_ADD("daily_merge", "report_merge_error", K(ret), K(error_code), "partition", part_key);
  }
  return ret;
}

int ObService::report_local_index_build_complete(const common::ObPartitionKey& part_key, const uint64_t index_id,
    const share::schema::ObIndexStatus index_status, const int32_t ret_code)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!part_key.is_valid() || OB_INVALID_ID == index_id || index_status <= INDEX_STATUS_NOT_FOUND ||
             index_status >= INDEX_STATUS_MAX) {
    // don't check ret_code here
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(part_key), K(index_id), K(index_status), K(ret));
  } else {
    ObIndexStatusReporter task;
    if (OB_FAIL(task.init(part_key, get_self_addr(), index_id, index_status, ret_code, *gctx_.sql_proxy_))) {
      LOG_WARN("init index status report task failed", K(ret), K(part_key), K(index_id), K(ret_code));
    } else {
      ret = index_status_report_queue_.add(task);
      LOG_INFO("submit index status report task", K(ret), K(task));
      if (OB_FAIL(ret)) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("submit index status report task failed", K(ret), K(task));
        }
      }
    }
    SERVER_EVENT_ADD("daily_merge", "index_build_complete", K(ret));
    if (OB_SUCCESS != ret_code) {
      LOG_ERROR("Fail to build local index, ", K(part_key), K(index_id), K(index_status), K(ret_code));
    }
  }
  return ret;
}

int ObService::submit_pt_remove_task(const ObPartitionKey& part_key)
{
  int ret = OB_SUCCESS;
  const int64_t version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!part_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part_key", K(part_key), K(ret));
  } else if (OB_FAIL(partition_table_updater_.async_remove(part_key))) {
    LOG_WARN("async_remove failed", K(part_key), K(ret));
  } else if (OB_FAIL(checksum_updater_.add_task(part_key, true /*is remove*/))) {
    LOG_WARN("fail to add task", K(ret), K(part_key));
  } else if (part_key.is_pg()) {
    // do nothing
  } else if (OB_FAIL(ObPGPartitionMTUpdater::get_instance().add_task(
                 part_key, ObPGPartitionMTUpdateType::DELETE, version, GCTX.self_addr_))) {
    LOG_WARN("fail to async delete pg partition meta table", K(ret), K(part_key));
  } else {
    LOG_INFO("async delete pg partition meta table success", K(part_key));
  }
  return ret;
}

int ObService::reach_partition_limit(const obrpc::ObReachPartitionLimitArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition service ptr is null", K(ret));
  } else if (gctx_.par_ser_->reach_tenant_partition_limit(arg.batch_cnt_, arg.tenant_id_, arg.is_pg_arg_)) {
    ret = OB_TOO_MANY_PARTITIONS_ERROR;
    LOG_WARN("too many partitions on this server", K(ret), K(arg));
  }
  return ret;
}

// should return success if all partition have merge to specific frozen_version
int ObService::check_frozen_version(const obrpc::ObCheckFrozenVersionArg& arg)
{
  LOG_INFO("receive check frozen version request", K(arg));
  int ret = OB_SUCCESS;
  ObPartitionScheduler& scheduler = ObPartitionScheduler::get_instance();
  int64_t last_merged_version = scheduler.get_merged_version();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (arg.frozen_version_ != last_merged_version) {
    ret = OB_ERR_CHECK_DROP_COLUMN_FAILED;
    LOG_WARN("last merged version not match", K(ret), K(arg), K(last_merged_version));
  }
  return ret;
}

int ObService::get_min_sstable_schema_version(
    const obrpc::ObGetMinSSTableSchemaVersionArg& arg, obrpc::ObGetMinSSTableSchemaVersionRes& result)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService* schema_service = gctx_.schema_service_;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < arg.tenant_id_arg_list_.size(); ++i) {
      // The minimum schema_version used by storage will increase with the major version,
      // storage only need to keep schema history used by a certain number major version.
      // min_schema_version = scheduler.get_min_schema_version(arg.tenant_id_arg_list_.at(i));
      int tmp_ret = OB_SUCCESS;
      const uint64_t tenant_id = arg.tenant_id_arg_list_.at(i);
      int64_t min_schema_version = 0;
      if (OB_SUCCESS != (tmp_ret = schema_service->get_recycle_schema_version(tenant_id, min_schema_version))) {
        min_schema_version = OB_INVALID_VERSION;
        LOG_WARN("fail to get recycle schema version", K(tmp_ret), K(tenant_id));
      } else {
        ObPartitionScheduler& scheduler = ObPartitionScheduler::get_instance();
        min_schema_version = min(min_schema_version, scheduler.get_merged_schema_version(tenant_id));
      }
      if (OB_FAIL(result.ret_list_.push_back(min_schema_version))) {
        LOG_WARN("push error", K(ret), K(arg));
      }
    }
  }
  return ret;
}

int ObService::create_partition(const obrpc::ObCreatePartitionArg& arg)
{
  UNUSED(arg);
  return OB_NOT_SUPPORTED;
}

int ObService::check_partition_need_update_pt_(
    const obrpc::ObCreatePartitionBatchArg& batch_arg, obrpc::ObCreatePartitionBatchRes& batch_res, bool& need_update)
{
  int ret = OB_SUCCESS;
  need_update = true;

  if (batch_arg.args_.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(batch_arg));
  } else if (batch_arg.args_.count() != batch_res.ret_list_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result count mismatch with argument count",
        K(ret),
        "res_count",
        batch_res.ret_list_.count(),
        "arg_count",
        batch_arg.args_.count());
  } else {
    // for record whether the first partition needs to update meta table
    bool first_need_sstable = false;
    const ObCreatePartitionArg& first = batch_arg.args_.at(0);
    if (OB_FAIL(first.check_need_create_sstable(first_need_sstable))) {
      LOG_WARN("fail to check need create sstable", K(ret));
    } else if (is_sys_table(first.partition_key_.get_table_id())) {
      // sys_table updated to all_core_table
      need_update = false;
    } else {
      // do nothing
    }
    // Check whether the other partitions are the same as the first partition
    FOREACH_X(arg, batch_arg.args_, OB_SUCC(ret) && need_update)
    {
      bool this_need_sstable = false;
      if (OB_FAIL(arg->check_need_create_sstable(this_need_sstable))) {
        LOG_WARN("fail to check need create sstable", K(ret));
      } else if (arg->partition_key_.get_table_id() != first.partition_key_.get_table_id() ||
                 arg->memstore_version_ != first.memstore_version_ || arg->replica_type_ != first.replica_type_ ||
                 first_need_sstable != this_need_sstable) {
        need_update = false;
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObService::create_partition_batch(
    const obrpc::ObCreatePartitionBatchArg& batch_arg, obrpc::ObCreatePartitionBatchRes& batch_res)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(DEFORE_OBS_CREATE_PARTITION);
  int64_t start_timestamp = ObTimeUtility::current_time();
  LOG_INFO("receive batch create partitions request", K(start_timestamp), K(batch_arg.args_.count()), K(batch_arg));
  int64_t partition_count = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!batch_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch arg", K(batch_arg), K(ret));
  } else if (OB_ISNULL(gctx_.par_ser_) || OB_ISNULL(gctx_.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL partition service or sql proxy",
        K(ret),
        "partition_service",
        gctx_.par_ser_,
        "sql_proxy",
        gctx_.sql_proxy_);
  } else if (OB_FAIL(gctx_.par_ser_->get_partition_count(partition_count))) {
    LOG_WARN("fail to get partition count", K(ret));
  } else {
    const obrpc::ObCreatePartitionArg& arg = batch_arg.args_.at(0);
    // add partition to pg
    if (arg.is_create_pg_partition()) {
      if (OB_FAIL(gctx_.par_ser_->create_batch_pg_partitions(batch_arg.args_, batch_res.ret_list_))) {
        LOG_WARN("failed to create batch partition groups.", K(ret));
      }
      // create partition group / stand alone partition
    } else if (OB_FAIL(gctx_.par_ser_->create_batch_partition_groups(batch_arg.args_, batch_res.ret_list_))) {
      LOG_WARN("failed to create batch partition groups.", K(ret));
    } else {
      // do nothing
    }
  }
  int64_t end_time = ObTimeUtility::current_time();
  int64_t cost_time = end_time - start_timestamp;
  LOG_INFO("create partition register end", K(ret), K(cost_time));
  // update partition table
  if (OB_SUCC(ret)) {
    bool need_update = true;
    if (OB_FAIL(check_partition_need_update_pt_(batch_arg, batch_res, need_update))) {
      LOG_WARN("check partition need update partition table error", K(ret), K(batch_arg), K(batch_res));
    } else if (need_update) {
      bool first_need_sstable = false;
      const ObCreatePartitionArg& first = batch_arg.args_.at(0);
      if (OB_FAIL(first.check_need_create_sstable(first_need_sstable))) {
        LOG_WARN("fail to check need create sstable", K(ret));
      } else {
        // update __all_tenant_meta_table
        int tmp_ret = OB_SUCCESS;
        int64_t data_version = first_need_sstable ? first.memstore_version_ - 1 : 0;
        // Those situations need to update sstable_checksum:
        // 1. standalone partition
        // 2. normal partition under pg;
        if (OB_SUCCESS != (tmp_ret = sync_report_replica_info(data_version, first, batch_arg))) {
          LOG_INFO("partition table sync report fail or timeout, need start async report",
              KR(tmp_ret),
              K(batch_arg),
              K(batch_res));
          // It is failed to report meta_table, start an asynchronous task to update in the background
          FOREACH_X(arg, batch_arg.args_, OB_SUCC(ret))
          {
            ObPartitionArray pkeys;
            if (OB_FAIL(submit_pt_update_task(arg->partition_key_))) {
              LOG_WARN("submit partition table async update task failed",
                  K(ret),
                  K(arg->partition_key_),
                  "data_version",
                  arg->memstore_version_ - 1);
            } else if (arg->partition_key_.is_pg()) {
              // do nothing
            } else if (OB_FAIL(pkeys.push_back(arg->partition_key_))) {
              LOG_WARN("pkeys push back error", K(ret), K(arg->partition_key_));
            } else {
              // do nothing
            }
            if (OB_SUCC(ret) && pkeys.count() > 0) {
              // update partition meta table without consider the error codes
              submit_pg_pt_update_task(pkeys);
            }
          }
        } else {
          // It is success to update meta table, need to update all_sstable_checksum asynchronously
          const bool is_remove = false;
          const int64_t version = 0;
          const ObSSTableChecksumUpdateType update_type = ObSSTableChecksumUpdateType::UPDATE_ALL;
          FOREACH_X(arg, batch_arg.args_, OB_SUCC(ret))
          {
            if (!arg->partition_key_.is_pg()) {
              if (OB_FAIL(checksum_updater_.add_task(arg->partition_key_, is_remove, update_type))) {
                LOG_WARN("fail to async update sstable checksum", K(ret), K(arg->partition_key_));
              } else if (OB_FAIL(ObPGPartitionMTUpdater::get_instance().add_task(arg->partition_key_,
                             ObPGPartitionMTUpdateType::INSERT_ON_UPDATE,
                             version,
                             GCTX.self_addr_))) {
                LOG_WARN("fail to async update sstable checksum", K(ret), K(arg->partition_key_));
              } else {
                // do nothing
              }
            }
          }
        }
      }
    } else {
      FOREACH_X(arg, batch_arg.args_, OB_SUCC(ret))
      {
        // RS not in service (in bootstrap) while creating __all_core_table partition,
        // update replica not need (always fail).
        if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != arg->partition_key_.get_table_id()) {
          ObPartitionArray pkeys;
          if (OB_FAIL(submit_pt_update_task(arg->partition_key_))) {
            LOG_WARN("submit partition table async update task failed",
                K(ret),
                "partition_key",
                arg->partition_key_,
                "data_version",
                arg->memstore_version_ - 1);
          } else if (arg->partition_key_.is_pg()) {
            // do nothing
          } else if (OB_FAIL(pkeys.push_back(arg->partition_key_))) {
            LOG_WARN("pkeys push back error", K(ret), K(arg->partition_key_));
          } else {
            // update partition meta table without consider the error code
            submit_pg_pt_update_task(pkeys);
          }
        }
      }
    }
  }
  int64_t cost = ObTimeUtility::current_time() - start_timestamp;
  int64_t update_pt_cost = ObTimeUtility::current_time() - end_time;
  LOG_INFO("create partition batch finish",
      K(ret),
      "partition count",
      batch_arg.args_.count(),
      K(cost),
      K(update_pt_cost),
      K(batch_res.ret_list_.count()),
      K(batch_res));
  if (batch_arg.args_.count() == 1) {
    SERVER_EVENT_ADD("ddl",
        "create_partition",
        K(ret),
        "partition",
        batch_arg.args_.at(0).partition_key_,
        K(update_pt_cost),
        K(cost),
        K(ret));
  } else {
    SERVER_EVENT_ADD("ddl",
        "batch_create_partition",
        K(ret),
        "partition_count",
        batch_arg.args_.count(),
        K(cost),
        K(update_pt_cost),
        "total_partition_cnt",
        partition_count,
        K(ret));
  }
  return ret;
}

/*
 * input:
 * data_version
 * first: arg of first create_partition
 * batch_arg: arg of batch create partition
 *
 */
int ObService::sync_report_replica_info(
    const int64_t data_version, const ObCreatePartitionArg& first, const obrpc::ObCreatePartitionBatchArg& batch_arg)

{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObNormalPartitionTableProxy pt_proxy(*gctx_.sql_proxy_);
    ObTimeoutCtx timeout_ctx;
    int64_t timeout_us = THIS_WORKER.get_timeout_remain();
    const int64_t MAX_EXECUTE_TIMEOUT_US = 2L * 100 * 1000;  // 200ms
    // The meaning of REMAIN_TIME: the reserved time for statement and the return of RPC
    // after Synchronous reporting meta table
    const int64_t REMAIN_TIME = 1L * 100 * 1000;  // 100ms
    int64_t stmt_timeout = MAX_EXECUTE_TIMEOUT_US;
    if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
      // if timeout is setted, the longest waiting time is 200ms except for the reserved time of 100ms
      timeout_us = std::min(timeout_us - REMAIN_TIME, stmt_timeout);
    } else {
      // just wait for 200ms, if timeout is not setted
      timeout_us = stmt_timeout;
    }
    if (timeout_us <= 0) {
      // nothing todo
      // At this time, the remaining time of the statement is less than the reserved value,
      // and it is no need to synchronous report
    } else if (OB_FAIL(timeout_ctx.set_timeout(timeout_us))) {
      LOG_WARN("failed to set timeout", KR(ret), K(timeout_us));
    } else if (OB_FAIL(pt_proxy.update_fresh_replica_version(
                   gctx_.self_addr_, GCONF.mysql_port, batch_arg.args_, data_version))) {
      LOG_WARN("update fresh replica version failed",
          KR(ret),
          K(gctx_.self_addr_),
          "data_version",
          first.memstore_version_ - 1);
    }
    if (OB_SUCC(ret)) {
      if (timeout_us <= 0) {
        ret = OB_TIMEOUT;
      }
    }
  }
  return ret;
}

int ObService::check_unique_index_request(const obrpc::ObCheckUniqueIndexRequestArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, partition service must not be NULL", K(ret));
  } else {
    // For the time being, write a fake unique check, and remove it after the column check is completed
    obrpc::ObCheckUniqueIndexResponseArg response_arg;
    ObAddr rs_addr;
    response_arg.pkey_ = arg.pkey_;
    response_arg.index_id_ = arg.index_id_;
    response_arg.ret_code_ = OB_SUCCESS;
    response_arg.is_valid_ = true;
    if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("innner system error, rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
    } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
      LOG_WARN("fail to get rootservice address", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).check_unique_index_response(response_arg))) {
      LOG_WARN("fail to check unique index response", K(ret), K(arg));
    }
  }
  return ret;
}

int ObService::calc_column_checksum_request(const obrpc::ObCalcColumnChecksumRequestArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, partition service must not be NULL", K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->calc_column_checksum(
                 arg.pkey_, arg.index_id_, arg.schema_version_, arg.execution_id_, arg.snapshot_version_))) {
    LOG_WARN("fail to do calculate column checksum request", K(ret));
  }
  return ret;
}

int ObService::check_single_replica_major_sstable_exist(const obrpc::ObCheckSingleReplicaMajorSSTableExistArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, partition service must not be NULL", K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->check_single_replica_major_sstable_exist(arg.pkey_, arg.index_id_))) {
    LOG_WARN("fail to check self major sstable exist", K(ret), K(arg));
  }
  return ret;
}

int ObService::check_single_replica_major_sstable_exist(
    const obrpc::ObCheckSingleReplicaMajorSSTableExistArg& arg, obrpc::ObCheckSingleReplicaMajorSSTableExistResult& res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, partition service must not be NULL", K(ret));
  } else if (OB_FAIL(
                 gctx_.par_ser_->check_single_replica_major_sstable_exist(arg.pkey_, arg.index_id_, res.timestamp_))) {
    LOG_WARN("fail to check self major sstable exist", K(ret), K(arg));
  }
  return ret;
}

int ObService::check_all_replica_major_sstable_exist(const obrpc::ObCheckAllReplicaMajorSSTableExistArg& arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, partition service must not be NULL", K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->check_all_replica_major_sstable_exist(arg.pkey_, arg.index_id_))) {
    LOG_WARN("fail to check self major sstable exist", K(ret), K(arg));
  }
  return ret;
}

int ObService::check_all_replica_major_sstable_exist(
    const obrpc::ObCheckAllReplicaMajorSSTableExistArg& arg, obrpc::ObCheckAllReplicaMajorSSTableExistResult& res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService has not been inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, partition service must not be NULL", K(ret));
  } else if (OB_FAIL(
                 gctx_.par_ser_->check_all_replica_major_sstable_exist(arg.pkey_, arg.index_id_, res.max_timestamp_))) {
    LOG_WARN("fail to check self major sstable exist", K(ret), K(arg));
  }
  return ret;
}

int ObService::fetch_root_partition(share::ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const ObPartitionKey partition_key(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
        ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
        ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM);
    if (OB_FAIL(fill_partition_replica(partition_key, replica))) {
      LOG_WARN("fetch_root_partition failed", K(ret), K(partition_key));
    } else {
      LOG_INFO("fetch root partition succeed", K(replica));
    }
  }
  return ret;
}

int ObService::add_replica(const obrpc::ObAddReplicaArg& arg, const share::ObTaskId& task_id)
{
  LOG_INFO("receive add replica request", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->add_replica(arg, task_id))) {
    LOG_WARN("add replica fail", K(arg), K(ret));
  } else {
    LOG_INFO("add replica successfully", K(arg));
  }
  SERVER_EVENT_ADD("ddl", "add_replica", K(ret), "partition", arg.key_);
  return ret;
}

int ObService::remove_member(const obrpc::ObMemberChangeArg& arg)
{
  LOG_INFO("receive remove member request", K(arg));
  obrpc::ObMCLogRpcInfo mc_log_info;

  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->remove_replica_mc(arg, mc_log_info))) {
    LOG_WARN("remove member failed", K(ret), K(arg));
  } else {
    LOG_INFO("remove member success", K(arg));
  }
  SERVER_EVENT_ADD("ddl", "remove_member", K(ret), "partition", arg.key_, "member", arg.member_);
  return ret;
}

int ObService::modify_quorum(const obrpc::ObModifyQuorumArg& arg)
{
  LOG_INFO("receive modify quorum request", K(arg));
  int ret = OB_SUCCESS;
  ObMCLogRpcInfo mc_log_info;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition service is null", K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->change_quorum_mc(arg, mc_log_info))) {
    LOG_WARN("modify quorum failed", K(ret), K(arg));
  } else {
    LOG_INFO("modify quorum success", K(arg));
  }
  SERVER_EVENT_ADD(
      "ddl", "change_quorum", K(ret), "partition", arg.key_, "quorum", arg.quorum_, "orig_quorum", arg.quorum_);
  return ret;
}

int ObService::add_replica_batch(const obrpc::ObAddReplicaBatchArg& arg)
{
  int ret = OB_SUCCESS;

  LOG_INFO("receive add_replica_batch request", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->batch_add_replica(arg, arg.task_id_))) {
    LOG_WARN("fail to add replica batch", K(ret), K(arg));
  } else {
    LOG_INFO("add_replica_batch successfully", K(arg));
  }
  return ret;
}

int ObService::remove_non_paxos_replica_batch(
    const obrpc::ObRemoveNonPaxosReplicaBatchArg& arg, obrpc::ObRemoveNonPaxosReplicaBatchResult& result)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("receive remove_non_paxos_replica_batch request", "arg_cnt", arg.arg_array_.count());

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition service is null", K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->batch_remove_non_paxos_replica(arg, result))) {
    LOG_WARN("remove_non_paxos_replica_batch fail", K(ret), K(arg));
  }
  const int64_t cost = ObTimeUtility::current_time() - start;
  LOG_INFO("batch remove non paxos repilca cost", "arg_cnt", arg.arg_array_.count(), K(cost));
  SERVER_EVENT_ADD("ddl",
      "batch_remove_non_paxos_replica",
      K(ret),
      K(cost),
      "partition_cnt",
      arg.arg_array_.count(),
      "result_cnt",
      result.return_array_.count(),
      "ret",
      ret);

  return ret;
}

int ObService::remove_member_batch(const obrpc::ObMemberChangeBatchArg& arg, obrpc::ObMemberChangeBatchResult& result)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("receive remove_member_batch request", "arg_cnt", arg.arg_array_.count());

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition service is null", K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->batch_remove_replica_mc(arg, result))) {
    LOG_WARN("batch remove member failed", K(ret), "arg_cnt", arg.arg_array_.count());
  }
  const int64_t cost = ObTimeUtility::current_time() - start;
  LOG_INFO("batch remove member cost", "arg_cnt", arg.arg_array_.count(), K(cost));
  SERVER_EVENT_ADD("ddl",
      "batch_remove_member",
      K(ret),
      K(cost),
      "partition_cnt",
      arg.arg_array_.count(),
      "result_cnt",
      result.return_array_.count(),
      "ret",
      ret);

  return ret;
}

int ObService::modify_quorum_batch(const obrpc::ObModifyQuorumBatchArg& arg, obrpc::ObModifyQuorumBatchResult& result)
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  LOG_INFO("receive modify_quorum_batch request", "arg_cnt", arg.arg_array_.count());

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition service is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.arg_array_.count(); i++) {
      const ObModifyQuorumArg& mc_arg = arg.arg_array_.at(i);
      if (OB_FAIL(result.return_array_.push_back(OB_ERROR))) {
        LOG_WARN("fail to add ret", K(ret));
      } else {
        ObMCLogRpcInfo mc_log_info;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = gctx_.par_ser_->change_quorum_mc(mc_arg, mc_log_info))) {
          LOG_WARN("fail to change quorum", K(ret), K(mc_arg));
        }
        result.return_array_.at(i) = tmp_ret;
      }
    }
  }
  const int64_t cost = ObTimeUtility::current_time() - start;
  LOG_INFO("batch modify quorum cost", "arg_cnt", arg.arg_array_.count(), K(cost));
  SERVER_EVENT_ADD("ddl",
      "batch_modify_quorum",
      K(ret),
      K(cost),
      "partition_cnt",
      arg.arg_array_.count(),
      "result_cnt",
      result.return_array_.count(),
      "ret",
      ret);
  return ret;
}

int ObService::migrate_replica_batch(const obrpc::ObMigrateReplicaBatchArg& arg)
{
  int ret = OB_SUCCESS;

  LOG_INFO("receive migrate replica batch request", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->migrate_replica_batch(arg))) {
    LOG_WARN("add migrate batch fail", K(arg), K(ret));
  } else {
    LOG_INFO("add migrate batch successfully", K(arg));
  }
  return ret;
}

int ObService::standby_cutdata_batch_task(const obrpc::ObStandbyCutDataBatchTaskArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive standby_cutdata_batch_task request", K(arg));

  return ret;
}

int ObService::rebuild_replica_batch(const obrpc::ObRebuildReplicaBatchArg& arg)
{
  int ret = OB_SUCCESS;

  LOG_INFO("receive rebuild_replica_batch request", K(arg));

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (1 != arg.arg_array_.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("rebuild_replica_batch is not supported yet", K(ret), K(arg));
  } else if (OB_FAIL(rebuild_replica(arg.arg_array_.at(0), arg.task_id_))) {
    LOG_WARN("rebuild_replica_batch fail", K(ret), K(arg));
  } else {
    LOG_INFO("rebuild_replica_batch successfully", K(arg));
  }

  return ret;
}

int ObService::backup_replica_batch(const obrpc::ObBackupBatchArg& arg)
{
  int ret = OB_SUCCESS;

  LOG_INFO("receive backup replica batch request", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->backup_replica_batch(arg))) {
    LOG_WARN("add migrate batch fail", K(arg), K(ret));
  } else {
    LOG_INFO("backup replica batch successfully", K(arg));
  }
  return ret;
}

int ObService::validate_backup_batch(const obrpc::ObValidateBatchArg& arg)
{
  int ret = OB_SUCCESS;

  LOG_INFO("receive validate backup batch request", K(arg));
  ;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(gctx_.par_ser_->validate_backup_batch(arg))) {
    LOG_WARN("validate backup batch fail", K(ret), K(arg));
  } else {
    LOG_INFO("validate backup batch successfully", K(arg));
  }
  return ret;
}

int ObService::check_sys_task_exist(const share::ObTaskId& arg, bool& res)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (arg.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(SYS_TASK_STATUS_MGR.task_exist(arg, res))) {
    LOG_WARN("failed to check task exist", K(ret), K(arg));
  }
  return ret;
}

int ObService::check_migrate_task_exist(const share::ObTaskId& arg, bool& res)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (arg.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(ObPartGroupMigrator::get_instance().task_exist(arg, res))) {
    LOG_WARN("failed to check migrate task exist", K(ret), K(arg));
  }
  return ret;
}

int ObService::change_replica_batch(const obrpc::ObChangeReplicaBatchArg& arg)
{
  int ret = OB_SUCCESS;

  LOG_INFO("receive change_replica_batch request", K(arg));

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->batch_change_replica(arg))) {
    LOG_WARN("change_replica_batch fail", K(ret), K(arg));
  } else {
    LOG_INFO("change_replica_batch successfully", K(arg));
  }

  if (arg.arg_array_.count() > 0) {
    SERVER_EVENT_ADD("ddl",
        "batch_change_replica",
        K(ret),
        "task_id",
        arg.task_id_,
        "count",
        arg.arg_array_.count(),
        "from",
        arg.arg_array_.at(0).src_,
        "to",
        arg.arg_array_.at(0).dst_,
        "quorum",
        arg.arg_array_.at(0).quorum_);
  }
  return ret;
}

int ObService::report_replica(const obrpc::ObReportSingleReplicaArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive report replica request", K(arg.partition_key_));
  const bool need_report_checksum = false;
  const bool with_role_report = true;
  ObPartitionArray pkeys;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.partition_key_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(arg.partition_key_), K(ret));
  } else {
    ObIPartitionGroup* pg = NULL;
    ObIPartitionGroupGuard guard;
    // If the replica is pg, it is need to update all partitions under pg
    if (OB_FAIL(gctx_.par_ser_->get_partition(arg.partition_key_, guard))) {
      if (OB_ENTRY_NOT_EXIST != ret && OB_PARTITION_NOT_EXIST != ret) {
        LOG_WARN("get partition failed", K(ret), K(arg));
      } else {
        // the partition was deleted, no need to report;
        ret = OB_SUCCESS;
      }
    } else if (NULL == (pg = guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pg is null", KP(pg), K(ret));
    } else if (OB_FAIL(pg->get_all_pg_partition_keys(pkeys))) {
      LOG_WARN("get all pg partition keys error", K(arg));
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(submit_pt_update_task(arg.partition_key_, need_report_checksum, with_role_report))) {
      LOG_WARN("async_update failed", K(arg.partition_key_), K(ret));
    }
    // update partition meta table, ignore failed
    submit_pg_pt_update_task(pkeys);
  }
  SERVER_EVENT_ADD("storage", "report_replica", K(ret), "partition", arg.partition_key_);
  return ret;
}

int ObService::migrate_replica(const obrpc::ObMigrateReplicaArg& arg, const share::ObTaskId& task_id)
{
  LOG_INFO("receive migrate replica request", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->migrate_replica(arg, task_id))) {
    LOG_WARN("migrate replica fail", K(arg), K(ret));
  } else {
    LOG_INFO("migrate replica successfully", K(arg));
  }
  SERVER_EVENT_ADD("ddl", "migrate_replica", K(ret), "partition", arg.key_, "from", arg.src_, "to", arg.dst_);
  return ret;
}

int ObService::minor_freeze(const obrpc::ObMinorFreezeArg& arg, obrpc::Int64& result)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  LOG_INFO("receive minor freeze request", K(arg), "is_pkey_valid", arg.partition_key_.is_valid());

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition service shouldn't be null in gctx", K(ret));
  } else {
    if (arg.partition_key_.is_valid()) {
      if (OB_FAIL(gctx_.par_ser_->minor_freeze(arg.partition_key_))) {
        LOG_WARN("fail to do minor freeze, ", "pkey", arg.partition_key_, K(ret));
      }
    } else if (arg.tenant_ids_.count() > 0) {
      // minor freeze with tenants
      for (int i = 0; i < arg.tenant_ids_.count(); ++i) {
        int tmp_ret = OB_SUCCESS;
        uint64_t tenant_id = arg.tenant_ids_.at(i);
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = gctx_.par_ser_->minor_freeze(tenant_id)))) {
          LOG_WARN("fail to do minor freeze", K(tenant_id), K(tmp_ret));
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
        }
      }
    } else {
      if (OB_FAIL(gctx_.par_ser_->minor_freeze(OB_INVALID_TENANT_ID))) {
        LOG_WARN("fail to minor freeze all", K(ret));
      }
    }
  }

  result = ret;
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("finish minor freeze request", K(ret), K(arg), K(cost_ts));
  return ret;
}

int ObService::remove_non_paxos_replica(const obrpc::ObRemoveNonPaxosReplicaArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive remove replica request", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObService not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->remove_replica(arg.key_, arg.dst_))) {
    LOG_WARN("remove replica fail", K(arg), K(ret));
  } else {
    LOG_INFO("remove replica successfully", K(arg));
  }
  SERVER_EVENT_ADD("ddl", "remove_replica", K(ret), "partition", arg.key_);
  return ret;
}

int ObService::rebuild_replica(const obrpc::ObRebuildReplicaArg& arg, const share::ObTaskId& task_id)
{
  LOG_INFO("receive rebuild replica request", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->rebuild_replica(arg, task_id))) {
    LOG_WARN("rebuild replica fail", K(arg), K(ret));
  } else {
    LOG_INFO("rebuild replica successfully", K(arg));
  }
  SERVER_EVENT_ADD("ddl", "rebuild_replica", K(ret), "partition", arg.key_, "from", arg.src_, "to", arg.dst_);
  return ret;
}

int ObService::restore_replica(const obrpc::ObRestoreReplicaArg& arg, const share::ObTaskId& task_id)
{
  LOG_INFO("receive restore replica request", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->restore_replica(arg, task_id))) {
    LOG_WARN("failed to restore_replica", K(arg), K(ret));
  } else {
    LOG_INFO("restore replica successfully", K(arg));
  }
  SERVER_EVENT_ADD("restore", "restore_replica", K(ret), "partition", arg.key_, "from", arg.src_, "to", arg.dst_);
  return ret;
}

int ObService::physical_restore_replica(const obrpc::ObPhyRestoreReplicaArg& arg, const share::ObTaskId& task_id)
{
  LOG_INFO("receive physical restore replica request", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->physical_restore_replica(arg, task_id))) {
    LOG_WARN("failed to restore_replica", K(arg), K(ret));
  } else {
    LOG_INFO("physical restore replica success", K(arg));
  }
  SERVER_EVENT_ADD(
      "restore", "physical_restore_replica", K(ret), "partition", arg.key_, "from", arg.src_, "to", arg.dst_);
  return ret;
}

int ObService::get_tenant_log_archive_status(
    const share::ObGetTenantLogArchiveStatusArg& arg, share::ObTenantLogArchiveStatusWrapper& result)
{
  LOG_INFO("receive get_tenant_log_archive_status request", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->get_tenant_log_archive_status(arg, result))) {
    LOG_WARN("failed to restore_replica", K(arg), K(ret));
  } else {
    LOG_INFO("finish get_tenant_log_archive_status request", K(arg), K(result));
  }
  return ret;
}

int ObService::copy_sstable_batch(const obrpc::ObCopySSTableBatchArg& arg)
{
  LOG_INFO("receive copy sstable batch request", K(arg));
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_COPY_SSTABLE_TYPE_INVALID == arg.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid copy sstable type", K(ret), K(arg));
  } else if (1 != arg.arg_array_.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("copy_sstable_batch is not supported yet", K(ret), K(arg));
  } else {
    switch (arg.type_) {
      case OB_COPY_SSTABLE_TYPE_LOCAL_INDEX: {
        for (int64_t i = 0; OB_SUCC(ret) && i < arg.arg_array_.count(); ++i) {
          const obrpc::ObCopySSTableArg& single_arg = arg.arg_array_.at(i);
          if (OB_FAIL(gctx_.par_ser_->copy_local_index(single_arg, arg.task_id_))) {
            LOG_WARN("fail to copy local index", K(ret), K(single_arg));
          }
        }
        break;
      }
      case OB_COPY_SSTABLE_TYPE_GLOBAL_INDEX: {
        for (int64_t i = 0; OB_SUCC(ret) && i < arg.arg_array_.count(); ++i) {
          const obrpc::ObCopySSTableArg& single_arg = arg.arg_array_.at(i);
          if (OB_FAIL(gctx_.par_ser_->copy_global_index(single_arg, arg.task_id_))) {
            LOG_WARN("fail to copy global index", K(ret), K(single_arg));
          }
        }
        break;
      }
      case OB_COPY_SSTABLE_TYPE_RESTORE_FOLLOWER: {
        if (OB_FAIL(gctx_.par_ser_->restore_follower_replica(arg))) {
          if (OB_RESTORE_PARTITION_IS_COMPELETE == ret) {
            LOG_INFO("restore follower replica is already finished", K(ret));
          } else {
            LOG_WARN("failed to restore follower", K(ret), K(arg));
          }
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid copy sstable rpc type", K(ret), "type", arg.type_);
      }
    }
  }
  if (arg.arg_array_.count() > 0) {
    SERVER_EVENT_ADD("ddl",
        "copy_sstable_batch",
        K(ret),
        "partition",
        arg.arg_array_.at(0).key_,
        "src",
        arg.arg_array_.at(0).src_,
        "dst",
        arg.arg_array_.at(0).dst_);
  } else {
    SERVER_EVENT_ADD("ddl", "copy_sstable_batch", K(ret));
  }
  return ret;
}

int ObService::change_replica(const obrpc::ObChangeReplicaArg& arg, const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive change replica info request", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->change_replica(arg, task_id))) {
    LOG_WARN("change replica info fail", K(arg), K(ret));
  } else {
    LOG_INFO("change replica info successfully", K(arg));
  }
  SERVER_EVENT_ADD(
      "ddl", "change_replica", K(ret), "partition", arg.key_, "from", arg.src_, "to", arg.dst_, "quorum", arg.quorum_);
  return ret;
}

int ObService::check_ctx_create_timestamp_elapsed(
    const obrpc::ObCheckCtxCreateTimestampElapsedArg& arg, obrpc::ObCheckCtxCreateTimestampElapsedResult& result)
{
  int ret = OB_SUCCESS;
  ObRole role;
  LOG_INFO("receive get checksum cal snapshot", K(arg));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_UNLIKELY(NULL == gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition service shouldn't be null in gctx", K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->get_role(arg.pkey_, role))) {
    LOG_WARN("fail to get role", K(ret));
  } else if (!is_strong_leader(role)) {
    ret = OB_NOT_MASTER;
  } else if (OB_FAIL(gctx_.par_ser_->check_ctx_create_timestamp_elapsed(arg.pkey_, arg.sstable_exist_ts_))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("check ctx create timestamp failed", K(ret));
    }
  } else {
    transaction::ObTransService* txs = gctx_.par_ser_->get_trans_service();
    if (OB_UNLIKELY(NULL == txs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("txs ptr is null", K(ret));
    } else if (OB_FAIL(txs->get_publish_version(arg.pkey_, result.snapshot_))) {
      LOG_WARN("fail to get publish version", K(ret));
    } else if (OB_FAIL(gctx_.par_ser_->get_role(arg.pkey_, role))) {
      LOG_WARN("fail to get role", K(ret));
    } else if (!is_strong_leader(role)) {
      ret = OB_NOT_MASTER;
    }
  }
  return ret;
}

int ObService::check_schema_version_elapsed(
    const obrpc::ObCheckSchemaVersionElapsedArg& arg, obrpc::ObCheckSchemaVersionElapsedResult& result)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive check schema version elapsed", K(arg));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_UNLIKELY(NULL == gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition service shouldn't be null in gctx", K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->check_schema_version_elapsed(
                 arg.pkey_, arg.schema_version_, 0 /*index_id*/, result.snapshot_))) {
    LOG_WARN("fail to do check schema version elapsed", K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObService::get_member_list(const ObPartitionKey& partition_key, obrpc::ObServerList& members)
{
  // TODO(): add logic
  UNUSED(partition_key);
  UNUSED(members);
  return OB_SUCCESS;
}

int ObService::switch_leader(const obrpc::ObSwitchLeaderArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive switch leader request", K(arg));
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->change_leader(arg.partition_key_, arg.leader_addr_))) {
    LOG_WARN("change_leader failed", K(arg), K(ret));
  }
  SERVER_EVENT_ADD("election", "switch_leader", K(ret), "partition", arg.partition_key_, "leader", arg.leader_addr_);
  return ret;
}

int ObService::batch_switch_rs_leader(const ObAddr& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive batch switch rs leader request", K(arg));

  int64_t start_timestamp = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gctx par_ser is NULL", K(arg));
  } else if (!arg.is_valid()) {
    if (OB_FAIL(gctx_.par_ser_->auto_batch_change_rs_leader())) {
      LOG_WARN("fail to auto batch change rs leader", KR(ret));
    }
  } else if (OB_FAIL(gctx_.par_ser_->batch_change_rs_leader(arg))) {
    LOG_WARN("fail to batch change rs leader", K(arg), KR(ret));
  }

  int64_t cost = ObTimeUtility::current_time() - start_timestamp;
  SERVER_EVENT_ADD("election", "batch_switch_rs_leader", K(ret), "leader", arg, K(cost));
  return ret;
}

int ObService::switch_leader_list(const obrpc::ObSwitchLeaderListArg& arg)
{
  int ret = OB_SUCCESS;

  LOG_INFO("receive switch leader list request", K(arg));
  int64_t start_timestamp = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.partition_key_list_.count(); ++i) {
      if (OB_FAIL(gctx_.par_ser_->change_leader(arg.partition_key_list_.at(i), arg.leader_addr_))) {
        LOG_WARN("change_leader failed", K(ret), K(i), K(arg.partition_key_list_.at(i)), K(arg.leader_addr_));
      }
    }
  }
  int64_t cost = ObTimeUtility::current_time() - start_timestamp;
  if (arg.partition_key_list_.count() == 1) {
    SERVER_EVENT_ADD("election",
        "switch_leader",
        K(ret),
        "partition",
        arg.partition_key_list_.at(0),
        "leader",
        arg.leader_addr_,
        K(cost));
  } else {
    SERVER_EVENT_ADD("election",
        "batch_switch_leader",
        K(ret),
        "partition_count",
        arg.partition_key_list_.count(),
        "leader",
        arg.leader_addr_,
        K(cost));
  }

  return ret;
}

int ObService::get_leader_candidates_v2(const ObGetLeaderCandidatesV2Arg& arg, ObGetLeaderCandidatesResult& result)
{
  // It is used to obtain valid_candidates in batch after 2.0
  int ret = OB_SUCCESS;
  ObSArray<ObAddrSArray> candidate_list_array;
  ObSArray<CandidateStatusList> candidate_status_array;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->get_dst_candidates_array(
                 arg.partitions_, arg.prep_candidates_, candidate_list_array, candidate_status_array))) {
    LOG_WARN("get_dst_candidates_array failed", K(ret));
  } else if (candidate_list_array.count() != candidate_status_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array count not match",
        K(ret),
        "candidate_cnt",
        candidate_list_array.count(),
        "status_cnt",
        candidate_status_array.count());
  } else {
    DEBUG_SYNC(OBSERVICE_GET_LEADER_CANDIDATES);
    for (int64_t i = 0; OB_SUCC(ret) && i < candidate_list_array.count(); ++i) {
      if (OB_FAIL(result.candidates_.push_back(candidate_list_array.at(i)))) {
        LOG_WARN("push_back failed", K(ret));
      } else if (OB_FAIL(result.candidate_status_array_.push_back(candidate_status_array.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }

  return ret;
}

int ObService::get_leader_candidates(const ObGetLeaderCandidatesArg& arg, ObGetLeaderCandidatesResult& result)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    DEBUG_SYNC(OBSERVICE_GET_LEADER_CANDIDATES);
    ObMemberList member_list;
    ObServerList candidate;
    ObAddr server;
    bool NEW_GET_LEADER_CANDIDATE = !(GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000);
    FOREACH_CNT_X(partition, arg.partitions_, OB_SUCCESS == ret)
    {
      member_list.reset();
      candidate.reset();
      if (!NEW_GET_LEADER_CANDIDATE) {
        if (OB_FAIL(gctx_.par_ser_->get_dst_leader_candidate(*partition, member_list))) {
          LOG_WARN("get_dst_leader_candidate failed", "partition", *partition, K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
            server.reset();
            if (OB_FAIL(member_list.get_server_by_index(i, server))) {
              LOG_WARN("get_server_by_index failed", K(i), K(ret));
            } else if (OB_FAIL(candidate.push_back(server))) {
              LOG_WARN("push_back failed", K(ret));
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(candidate.push_back(gctx_.self_addr_))) {
            LOG_WARN("push_back failed", K(ret));
          } else if (OB_FAIL(result.candidates_.push_back(candidate))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }
      } else {
        ObGetLeaderCandidatesV2Arg this_arg;
        ObGetLeaderCandidatesResult this_result;
        if (OB_FAIL(this_arg.partitions_.push_back(*partition))) {
          LOG_WARN("fail to push back partition key", K(ret));
        } else if (OB_FAIL(get_leader_member(*partition, this_arg.prep_candidates_))) {
          LOG_WARN("fail to get leader member", K(ret));
        } else if (OB_FAIL(get_leader_candidates_v2(this_arg, this_result))) {
          LOG_WARN("fail to get leader candidates", K(ret));
        } else if (this_result.candidates_.count() <= 0 || this_result.candidates_.count() > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result candidates count unexpected", K(ret));
        } else if (this_result.candidate_status_array_.count() <= 0 ||
                   this_result.candidate_status_array_.count() > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result candidate status count unexpected", K(ret));
        } else if (OB_FAIL(result.candidates_.push_back(this_result.candidates_.at(0)))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(result.candidate_status_array_.push_back(this_result.candidate_status_array_.at(0)))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
        }  // no more to do
      }
    }
  }
  return ret;
}

int ObService::switch_schema(const obrpc::ObSwitchSchemaArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to switch schema", K(arg));
  const ObRefreshSchemaInfo& schema_info = arg.schema_info_;
  const int64_t schema_version = schema_info.get_schema_version();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_version), K(ret));
  } else if (arg.force_refresh_) {
    ObSEArray<uint64_t, 1> tenant_ids;
    ObMultiVersionSchemaService* schema_service = gctx_.schema_service_;
    int64_t sys_schema_version = OB_INVALID_VERSION;
    if (schema_info.is_valid() && OB_INVALID_TENANT_ID != schema_info.get_tenant_id() &&
        OB_SYS_TENANT_ID != schema_info.get_tenant_id()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid schema info", K(ret), K(schema_info));
    } else if (OB_FAIL(tenant_ids.push_back(OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to push back tenant_id", K(ret));
    } else if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", K(ret));
    } else if (OB_FAIL(schema_service->refresh_and_add_schema(tenant_ids))) {
      LOG_WARN("fail to refresh schema", K(ret), K(schema_info));
    } else if (schema_info.get_schema_version() <= 0) {
      // skip
    } else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(OB_SYS_TENANT_ID, sys_schema_version))) {
      LOG_WARN("fail to get local sys schema_version", K(ret));
    } else if (schema_info.get_schema_version() > sys_schema_version) {
      ret = OB_EAGAIN;
      LOG_WARN("schema is not new enough", K(ret), K(schema_info), K(sys_schema_version));
    }
  } else if (!schema_info.is_valid()) {
    if (OB_FAIL(schema_updater_.try_reload_schema(schema_version))) {
      LOG_WARN("reload schema failed", K(schema_version), K(ret));
    } else {
      LOG_INFO("switch schema version success", K(ret), K(schema_version));
    }
  } else {
    if (OB_FAIL(schema_updater_.try_reload_schema(schema_info))) {
      LOG_WARN("reload schema failed", K(schema_info), K(ret));
    } else {
      LOG_INFO("switch schema version success", K(ret), K(schema_info));
    }
  }
  // SERVER_EVENT_ADD("schema", "switch_schema", K(ret), K(schema_info));
  return ret;
}

int ObService::bootstrap(const obrpc::ObBootstrapArg& arg)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = 600 * 1000 * 1000LL;  // 10 minutes
  const obrpc::ObServerInfoList& rs_list = arg.server_list_;
  LOG_INFO("bootstrap timeout", K(timeout), "worker_timeout_ts", THIS_WORKER.get_timeout_ts());
  if (!inited_) {
    ret = OB_NOT_INIT;
    BOOTSTRAP_LOG(WARN, "not init", K(ret));
  } else if (rs_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    BOOTSTRAP_LOG(WARN, "rs_list is empty", K(rs_list), K(ret));
  } else {
    ObPreBootstrap pre_bootstrap(
        *gctx_.srv_rpc_proxy_, rs_list, *gctx_.pt_operator_, *gctx_.config_, arg, *gctx_.rs_rpc_proxy_);
    ObAddr master_rs;
    bool server_empty = false;
    ObCheckServerEmptyArg new_arg;
    new_arg.mode_ = ObCheckServerEmptyArg::BOOTSTRAP;
    int64_t initial_frozen_version = 0;
    int64_t initial_schema_version = 0;
    obrpc::ObBootstrapArg tmp_arg;
    ObSArray<storage::ObFrozenStatus> frozen_status;
    ObSArray<share::TenantIdAndSchemaVersion> freeze_schemas;
    const bool wait_log_scan = true;
    if (OB_FAIL(check_server_empty(new_arg, wait_log_scan, server_empty))) {
      BOOTSTRAP_LOG(WARN, "check_server_empty failed", K(ret), K(new_arg));
    } else if (!server_empty) {
      ret = OB_ERR_SYS;
      BOOTSTRAP_LOG(WARN, "observer is not empty", K(ret));
    } else if (OB_FAIL(pre_bootstrap.prepare_bootstrap(
                   master_rs, initial_frozen_version, initial_schema_version, frozen_status, freeze_schemas))) {
      BOOTSTRAP_LOG(ERROR, "failed to prepare boot strap", K(rs_list), K(ret));
    } else if (OB_FAIL(tmp_arg.assign(arg))) {
      BOOTSTRAP_LOG(WARN, "fail to assign", KR(ret), K(arg));
    } else if (OB_FAIL(tmp_arg.frozen_status_.assign(frozen_status))) {
      BOOTSTRAP_LOG(WARN, "failed to assign frozen status", KR(ret), K(frozen_status));
    } else if (OB_FAIL(tmp_arg.freeze_schemas_.assign(freeze_schemas))) {
      BOOTSTRAP_LOG(WARN, "failed to assign freeze schemas", KR(ret), K(freeze_schemas));
    } else {
      const ObCommonRpcProxy& rpc_proxy = *gctx_.rs_rpc_proxy_;
      bool boot_done = false;
      const int64_t MAX_RETRY_COUNT = 30;
      tmp_arg.initial_frozen_version_ = initial_frozen_version;
      tmp_arg.initial_schema_version_ = initial_schema_version;
      for (int i = 0; !boot_done && i < MAX_RETRY_COUNT; i++) {
        ret = OB_SUCCESS;
        int64_t rpc_timeout = timeout;
        if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
          rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
        }
        if (OB_FAIL(rpc_proxy.to_addr(master_rs).timeout(rpc_timeout).execute_bootstrap(tmp_arg))) {
          if (OB_RS_NOT_MASTER == ret) {
            BOOTSTRAP_LOG(
                INFO, "master root service not ready", K(master_rs), "retry_count", i, K(rpc_timeout), K(ret));
            SLEEP(1);
          } else {
            BOOTSTRAP_LOG(ERROR, "execute bootstrap fail", K(master_rs), K(rpc_timeout), K(ret));
            break;
          }
        } else {
          boot_done = true;
        }
      }
      if (boot_done) {
        BOOTSTRAP_LOG(INFO, "succeed to do_boot_strap", K(rs_list), K(master_rs));
      }
    }
  }

  return ret;
}

int ObService::check_deployment_mode_match(const obrpc::ObCheckDeploymentModeArg& arg, obrpc::Bool& match)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const bool single_zone_deployment_on = false;
    if (single_zone_deployment_on == arg.single_zone_deployment_on_) {
      match = true;
    } else {
      match = false;
    }
  }
  return ret;
}

int ObService::is_empty_server(const obrpc::ObCheckServerEmptyArg& arg, obrpc::Bool& is_empty)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    bool server_empty = false;
    const bool wait_log_scan = (ObCheckServerEmptyArg::BOOTSTRAP == arg.mode_);
    if (OB_FAIL(check_server_empty(arg, wait_log_scan, server_empty))) {
      LOG_WARN("check_server_empty failed", K(ret), K(arg));
    } else {
      is_empty = server_empty;
    }
  }
  return ret;
}

int ObService::get_partition_count(obrpc::ObGetPartitionCountResult& result)
{
  int ret = OB_SUCCESS;
  result.reset();

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->get_partition_count(result.partition_count_))) {
    LOG_WARN("failed to get partition count", K(ret));
  }
  return ret;
}

int ObService::get_partition_stat(obrpc::ObPartitionStatList& partition_stat_list)
{
  int ret = OB_SUCCESS;
  partition_stat_list.reuse();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObIPartitionGroupIterator* partition_iter = gctx_.par_ser_->alloc_pg_iter();
    if (NULL == partition_iter) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc_scan_iter failed", K(ret));
    } else {
      ObPartitionStat partition_stat;
      ObIPartitionGroup* partition = NULL;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(partition_iter->get_next(partition))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("partition_iter get_next failed", K(ret));
          }
        } else if (NULL == partition) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is null", KP(partition), K(ret));
        } else {
          partition_stat.reset();
          partition_stat.partition_key_ = partition->get_partition_key();
          partition_stat.stat_ = ObPartitionStat::WRITABLE;
          if (OB_FAIL(partition_stat_list.push_back(partition_stat))) {
            LOG_WARN("push_back failed", K(partition_stat), K(ret));
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    // revert partition iterator
    if (NULL != partition_iter) {
      gctx_.par_ser_->revert_pg_iter(partition_iter);
      partition_iter = NULL;
    }
  }
  return ret;
}

int ObService::check_server_empty(const ObCheckServerEmptyArg& arg, const bool wait_log_scan, bool& is_empty)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (ObCheckServerEmptyArg::BOOTSTRAP == arg.mode_) {
      // "is_valid_heartbeat" is valid:
      // 1. RS is between "start_service" and "full_service", the server has not added to RS;
      // 2. RS is in "full_service" and the server has added to RS;
      // 3. To avoid misjudgment in scenario 1 while add server, this check is skipped here
      if (lease_state_mgr_.is_valid_heartbeat()) {
        LOG_WARN("server already in rootservice lease");
        is_empty = false;
      }
    }

    // wait log scan finish
    if (is_empty && wait_log_scan) {
      const int64_t WAIT_LOG_SCAN_TIME_US = 2 * 1000 * 1000;  // only wait 2s for empty server
      const int64_t SLEEP_INTERVAL_US = 500;
      const int64_t start_time_us = ObTimeUtility::current_time();
      int64_t end_time_us = start_time_us;
      int64_t timeout_ts = THIS_WORKER.get_timeout_ts();
      if (INT64_MAX == THIS_WORKER.get_timeout_ts()) {
        timeout_ts = start_time_us + WAIT_LOG_SCAN_TIME_US;
      }
      while (!stopped_ && !gctx_.par_ser_->is_scan_disk_finished()) {
        end_time_us = ObTimeUtility::current_time();
        if (end_time_us > timeout_ts) {
          LOG_WARN("wait log scan finish timeout", K(timeout_ts), LITERAL_K(WAIT_LOG_SCAN_TIME_US));
          is_empty = false;
          break;
        }
        usleep(static_cast<int32_t>(std::min(timeout_ts - end_time_us, SLEEP_INTERVAL_US)));
      }
    }
    if (is_empty) {
      if (!gctx_.par_ser_->is_empty()) {
        LOG_WARN("partition service is not empty");
        is_empty = false;
      }
    }
    if (is_empty) {
      if (!OBSERVER.is_log_dir_empty()) {
        LOG_WARN("log dir is not empty");
        is_empty = false;
      }
    }
  }
  return ret;
}

int ObService::report_replica()
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive report all replicas request");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObIPartitionGroupIterator* partition_iter = NULL;
    ObIPartitionGroup* partition = NULL;
    int64_t replica_count = 0;

    if (NULL == (partition_iter = gctx_.par_ser_->alloc_pg_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc partition iter, ", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObPartitionArray pkeys;
        if (OB_FAIL(partition_iter->get_next(partition))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("Fail to get next partition, ", K(ret));
          }
        } else if (NULL == partition) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("The partition is NULL, ", K(ret));
        } else if (OB_FAIL(partition->get_all_pg_partition_keys(pkeys))) {
          LOG_WARN("get all pg partition keys error", "pg_key", partition->get_partition_key());
          if (OB_ENTRY_NOT_EXIST != ret && OB_PARTITION_NOT_EXIST != ret) {
            LOG_WARN("get partition failed", K(ret));
          } else {
            // The partition has been deleted. There is no need to trigger the report
            ret = OB_SUCCESS;
          }
        } else if (OB_FAIL(submit_pt_update_task(
                       partition->get_partition_key(), true /*need report checksum*/, true /*with role report*/))) {
          if (OB_PARTITION_NOT_EXIST == ret) {
            // The GC thread is already working,
            // and deleted during traversal, the replica has been deleted needs to be avoided blocking the start process
            ret = OB_SUCCESS;
            LOG_INFO("this partition is already not exist", K(ret), "partition_key", partition->get_partition_key());
          } else {
            LOG_WARN(
                "submit partition table update task failed", K(ret), "partition_key", partition->get_partition_key());
          }
        } else {
          // Update partition meta table without concern for error codes
          submit_pg_pt_update_task(pkeys);
          ++replica_count;
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }

    if (NULL != partition_iter) {
      gctx_.par_ser_->revert_pg_iter(partition_iter);
    }
    LOG_INFO("submit all replicas report", K(ret));
    SERVER_EVENT_ADD("storage", "report_replica", K(ret), "replica_count", replica_count);
  }
  return ret;
}

int ObService::recycle_replica()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // gctx_.par_ser_->garbage_clean();
  }
  return ret;
}

int ObService::clear_location_cache()
{
  // TODO : implement after kvcache support clear interface
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObService::drop_replica(const obrpc::ObDropReplicaArg& arg)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive drop replica request");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else {
    ObRemoveNonPaxosReplicaArg rm_arg;
    rm_arg.key_ = arg.partition_key_;
    rm_arg.dst_ = arg.member_;
    rm_arg.switch_epoch_ = GCTX.get_switch_epoch2();
    if (OB_FAIL(remove_non_paxos_replica(rm_arg))) {
      LOG_WARN("remove_replica failed", K(rm_arg), K(ret));
    }
  }
  SERVER_EVENT_ADD("storage", "drop_replica", K(ret), "partition", arg.partition_key_);
  return ret;
}

int ObService::set_ds_action(const obrpc::ObDebugSyncActionArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(GDS.set_global_action(arg.reset_, arg.clear_, arg.action_))) {
    LOG_WARN("set debug sync global action failed", K(ret), K(arg));
  }
  return ret;
}

int ObService::update_cluster_info(ObClusterInfoArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSEDx(arg);
  return ret;
}

int ObService::request_heartbeat(ObLeaseRequest& lease_request)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(heartbeat_process_.init_lease_request(lease_request))) {
    LOG_WARN("init_lease_request failed", K(ret));
  }
  return ret;
}

// only for bootstrap, Use new mode directly
int ObService::broadcast_sys_schema(const ObSArray<ObTableSchema>& table_schemas)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (table_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_schemas is empty", K(table_schemas), K(ret));
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(gctx_.schema_service_->init_sys_schema(table_schemas))) {
    LOG_WARN("init_sys_schema failed", K(table_schemas), K(ret));
  } else {
  }
  return ret;
}

// get tenant's refreshed schema version in new mode
int ObService::get_tenant_refreshed_schema_version(
    const obrpc::ObGetTenantSchemaVersionArg& arg, obrpc::ObGetTenantSchemaVersionResult& result)
{
  int ret = OB_SUCCESS;
  result.schema_version_ = OB_INVALID_VERSION;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret));
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(
                 arg.tenant_id_, result.schema_version_, false /*core_version*/, true /*use_new_mode*/))) {
    LOG_WARN("fail to get tenant refreshed schema version", K(ret), K(arg));
  }
  return ret;
}

int ObService::check_partition_table()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(pt_checker_.schedule_task())) {
    LOG_WARN("pt_checker schedule_task failed", K(ret));
  }
  return ret;
}

int ObService::sync_pg_partition_table(const obrpc::Int64& arg)
{
  LOG_INFO("receive sync pg partition meta table request");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (arg <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(ObPGPartitionMTUpdater::get_instance().sync_pg_pt(arg))) {
    LOG_WARN("submit sync pg partition meta table failed", K(ret), K(arg));
  }
  SERVER_EVENT_ADD("storage", "sync_pg_partition_table", K(ret), "version", arg);
  return ret;
}

int ObService::sync_partition_table(const obrpc::Int64& arg)
{
  LOG_INFO("receive sync partition table request");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (arg <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(partition_table_updater_.sync_pt(arg))) {
    LOG_WARN("submit sync partition table failed", K(ret), K(arg));
  }
  SERVER_EVENT_ADD("storage", "sync_partition_table", K(ret), "version", arg);
  return ret;
}

int ObService::check_dangling_replica_exist(const obrpc::Int64& arg)
{
  LOG_INFO("receive check dangling replica request");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (arg <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(pt_checker_.schedule_task(arg))) {
    LOG_WARN("pt_checker schedule_task failed", K(ret), K(arg));
  }
  SERVER_EVENT_ADD("storage", "check_dangling_replica_exist", K(ret), "version", arg);
  return ret;
}

int ObService::get_server_heartbeat_expire_time(int64_t& lease_expire_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    lease_expire_time = lease_state_mgr_.get_heartbeat_expire_time();
  }
  return ret;
}

bool ObService::is_heartbeat_expired() const
{
  bool bret = false;  // returns false on error
  if (OB_UNLIKELY(!inited_)) {
    LOG_WARN("not init");
  } else {
    bret = !lease_state_mgr_.is_valid_heartbeat();
  }
  return bret;
}

bool ObService::is_svr_lease_valid() const
{
  // Determine if local lease is valid in OFS mode
  bool bret = false;
  if (OB_UNLIKELY(!inited_)) {
    LOG_WARN("not init");
  } else {
    bret = lease_state_mgr_.is_valid_lease();
  }
  return bret;
}

int64_t ObService::get_partition_table_updater_user_queue_size() const
{
  return partition_table_updater_.get_partition_table_updater_user_queue_size();
}
int64_t ObService::get_partition_table_updater_sys_queue_size() const
{
  return partition_table_updater_.get_partition_table_updater_sys_queue_size();
}
int64_t ObService::get_partition_table_updater_core_queue_size() const
{
  return partition_table_updater_.get_partition_table_updater_core_queue_size();
}

int ObService::set_tracepoint(const obrpc::ObAdminSetTPArg& arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (arg.event_name_.length() > 0) {
      ObSqlString str;
      if (OB_FAIL(str.assign(arg.event_name_))) {
        LOG_WARN("string assign failed", K(ret));
      } else {
        TP_SET_EVENT(str.ptr(), arg.error_code_, arg.occur_, arg.trigger_freq_);
      }
    } else {
      TP_SET_EVENT(arg.event_no_, arg.error_code_, arg.occur_, arg.trigger_freq_);
    }
    LOG_INFO("set event", K(arg));
  }
  return ret;
}

int ObService::cancel_sys_task(const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(task_id));
  } else if (OB_FAIL(SYS_TASK_STATUS_MGR.cancel_task(task_id))) {
    LOG_WARN("failed to cancel sys task", K(ret), K(task_id));
  }
  return ret;
}

int ObService::get_all_partition_status(int64_t& inactive_num, int64_t& total_num) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->get_all_partition_status(inactive_num, total_num))) {
    LOG_WARN("fail to get all partition status", K(ret));
  }
  return ret;
}

int ObService::get_tenant_group_string(common::ObString& ttg_string)
{
  int ret = OB_SUCCESS;
  ttg_string = GCONF.tenant_groups.str();
  return ret;
}

int ObService::get_root_server_status(ObGetRootserverRoleResult& get_role_result)
{
  int ret = OB_SUCCESS;
  // TODO : replace with LEADER && FOLLOWER. (same to ObRsMgr)
  const ObPartitionKey partition_key(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
      ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
      ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM);
  ObPartitionReplica replica;
  get_role_result.replica_.reset();

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(gctx_.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid root_service", K(ret));
  } else if (OB_FAIL(fill_partition_replica(partition_key, replica))) {
    if (OB_PARTITION_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      get_role_result.type_ = REPLICA_TYPE_MAX;
      get_role_result.role_ = ObRoleMgr::SLAVE;
      get_role_result.zone_ = GCONF.zone.str();
      get_role_result.status_ = status::INVALID;
    } else {
      LOG_WARN("fail to fill partition replica", K(ret), K(partition_key));
    }
  } else if (OB_FAIL(get_role_result.replica_.assign(replica))) {
    LOG_WARN("fail to assign replica", K(ret), K(replica));
  } else {
    get_role_result.type_ = replica.replica_type_;
    get_role_result.role_ = gctx_.root_service_->in_service() ? ObRoleMgr::OB_MASTER : ObRoleMgr::SLAVE;
    get_role_result.zone_ = GCONF.zone.str();
    get_role_result.status_ = gctx_.root_service_->get_status();
    if (gctx_.root_service_->in_service()) {
      if (OB_FAIL(gctx_.root_service_->get_root_partition(get_role_result.partition_info_))) {
        LOG_WARN("fail to get root partition", KR(ret));
      }
    }
  }
  return ret;
}

int ObService::get_master_root_server(obrpc::ObGetRootserverRoleResult& result)
{
  int ret = OB_SUCCESS;
  ObAddr master_rs;
  bool found = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(gctx_.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs mgr is null", K(ret));
  } else if (OB_NOT_NULL(gctx_.root_service_)) {
    // If itself is RS, it is no need to get RS by RPC
    if (gctx_.root_service_->in_service()) {
      if (OB_FAIL(get_root_server_status(result))) {
        LOG_WARN("failed to get rootserver status", K(ret), K(master_rs));
      } else {
        found = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (found) {
    // If itself is RS, it is no need to get RS by RPC
  } else if (OB_FAIL(gctx_.rs_mgr_->get_master_root_server(master_rs))) {
    LOG_WARN("failed to find master rs", K(ret), K(master_rs));
  } else if (gctx_.self_addr_ == master_rs) {
    // The master_rs stored in rs_mgr must not be local,
    // it has been judged that the itself is not master_rs
    ret = OB_RS_NOT_MASTER;
    LOG_WARN("not master rootserver", K(ret), K(master_rs));
  } else if (OB_ISNULL(gctx_.rs_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else if (OB_FAIL(gctx_.rs_rpc_proxy_->to_addr(master_rs).get_root_server_status(result))) {
    LOG_WARN("failed to get rootserver role", K(ret), K(master_rs));
  }
  return ret;
}

int ObService::refresh_core_partition()
{
  int ret = OB_SUCCESS;
  const int64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
  const int64_t partition_id = ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID;
  ObPartitionLocation location;
  bool is_cache_hit = false;
  int64_t expire_renew_time = INT64_MAX;
  if (OB_FAIL(GCTX.location_cache_->get(table_id, partition_id, location, expire_renew_time, is_cache_hit))) {
    LOG_WARN("fail to refresh core partition", K(ret));
  } else {
#if !defined(NDEBUG)
    LOG_INFO("refresh core partition success", K(location));
#endif
  }
  return ret;
}

int ObService::split_partition(const obrpc::ObSplitPartitionArg& split_info, obrpc::ObSplitPartitionResult& result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (!split_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(split_info));
  } else if (OB_FAIL(gctx_.par_ser_->split_partition(split_info.split_info_, result.get_result()))) {
    LOG_WARN("fail to split partition", K(ret));
  }
  return ret;
}

int ObService::stop_partition_write(const obrpc::Int64& switchover_timestamp, obrpc::Int64& result)
{
  // TODO for switchover
  int ret = OB_SUCCESS;
  result = switchover_timestamp;
  return ret;
}

int ObService::check_partition_log(const obrpc::Int64& switchover_timestamp, obrpc::Int64& result)
{
  // Check that the log of all replicas in local have reached synchronization status
  // The primary has stopped writing
  int ret = OB_SUCCESS;
  int64_t balance_task = ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_GROUP_MIGRATE);
  if (balance_task > 0) {
    ret = OB_EAGAIN;
    result = switchover_timestamp;
    LOG_INFO("observer already has task to do", K(switchover_timestamp), K(balance_task));
  } else if (OB_FAIL(gctx_.par_ser_->check_all_partition_sync_state(switchover_timestamp))) {
    LOG_WARN("fail to check_all_partition_sync_state", K(ret));
  } else {
    result = switchover_timestamp;
  }
  return ret;
}

int ObService::batch_set_member_list(const obrpc::ObBatchStartElectionArg& arg, obrpc::Int64& result)
{
  UNUSED(arg);
  UNUSED(result);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObService::batch_write_cutdata_clog(const obrpc::ObBatchWriteCutdataClogArg& arg, obrpc::ObBatchCheckRes& result)
{
  UNUSED(result);

  LOG_INFO("receive batch write cutdata clog request", "task_count", arg.pkeys_.count(), "epoch", arg.index_);

#if !defined(NDEBUG)
  for (int64_t i = 0; i < arg.pkeys_.count(); i++) {
    LOG_INFO("output pkey", K(i), K(arg.pkeys_.at(i)));
  }
#endif

  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObService::batch_wait_leader(const obrpc::ObBatchCheckLeaderArg& arg, obrpc::ObBatchCheckRes& result)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  LOG_INFO("receive batch wait leader request", "task_count", arg.pkeys_.count(), "epoch", arg.index_);
#if !defined(NDEBUG)
  for (int64_t i = 0; i < arg.pkeys_.count(); i++) {
    LOG_INFO("output pkey", K(i), K(arg.pkeys_.at(i)));
  }
#endif
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(gctx_.par_ser_->batch_check_leader_active(arg, result))) {
    LOG_WARN("fail to batch_check_leader_active", K(ret));
  } else {
    // do nothing
  }

  result.index_ = arg.index_;
  int64_t cost = ObTimeUtility::current_time() - start_time;
  LOG_INFO("batch_wait_leader finished", KR(ret), K(cost), "task_count", arg.pkeys_.count());
  return ret;
}

int ObService::estimate_partition_rows(const obrpc::ObEstPartArg& arg, obrpc::ObEstPartRes& res) const
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("receive estimate rows request", K(arg));
  if (!inited_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("service is not inited", K(ret));
  } else if (OB_FAIL(
                 ObOptEstCost::storage_estimate_rowcount(gctx_.par_ser_, ObStatManager::get_instance(), arg, res))) {
    LOG_WARN("failed to estimate partition rowcount", K(ret));
  }
  return ret;
}

int ObService::get_wrs_info(const obrpc::ObGetWRSArg& arg, obrpc::ObGetWRSResult& result)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = arg.tenant_id_;
  const bool need_filter = arg.need_filter_;
  const bool need_inner_table = (ObGetWRSArg::USER_TABLE != arg.scope_);
  const bool need_user_table = (ObGetWRSArg::INNER_TABLE != arg.scope_);

  result.reset();
  if (OB_FAIL(get_tenant_replica_wrs_info(
          result.replica_wrs_info_list_, tenant_id, need_filter, need_inner_table, need_user_table))) {
    LOG_WARN("fail to get tenant replica wrs info",
        KR(ret),
        K(tenant_id),
        K(need_filter),
        K(need_inner_table),
        K(need_user_table));
  }

  result.err_code_ = ret;
  result.self_addr_ = gctx_.self_addr_;

  return OB_SUCCESS;
}

int ObService::check_physical_flashback_succ(
    const obrpc::ObCheckPhysicalFlashbackArg& arg, obrpc::ObPhysicalFlashbackResultArg& result)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("service is not inited", K(ret));
  } else if (OB_FAIL(gctx_.par_ser_->check_physical_flashback_succ(arg, result))) {
    LOG_WARN("fail to check physical flashback", K(ret), K(arg));
  }

  return ret;
}

int ObService::update_pg_backup_task_info(const common::ObIArray<share::ObPGBackupTaskInfo>& pg_task_info_array)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo task_info;
  ObTenantBackupTaskInfo dest_task_info;
  ObMySQLTransaction trans;
  const int64_t MAX_EXECUTE_TIMEOUT_US = 5 * 60L * 1000 * 1000;  // 5min
  int64_t stmt_timeout = MAX_EXECUTE_TIMEOUT_US;
  ObTimeoutCtx timeout_ctx;
  int64_t macro_block_count = 0;
  int64_t finish_partition_count = 0;
  int64_t finish_macro_block_count = 0;
  int64_t input_bytes = 0;
  int64_t output_bytes = 0;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("service do not init", K(ret));
  } else if (pg_task_info_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg backup status is invalid", K(ret), K(pg_task_info_array));
  } else {
    for (int64_t i = 0; i < pg_task_info_array.count(); ++i) {
      const ObPGBackupTaskInfo& pg_task_info = pg_task_info_array.at(i);
      if (OB_SUCCESS != pg_task_info.result_) {
        // do nothing
      } else {
        finish_partition_count += pg_task_info.finish_partition_count_;
        macro_block_count += pg_task_info.macro_block_count_;
        finish_macro_block_count += pg_task_info.finish_macro_block_count_;
        input_bytes += pg_task_info.input_bytes_;
        output_bytes += pg_task_info.output_bytes_;
      }
    }

    if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
      LOG_WARN("fail to set trx timeout", K(ret), K(stmt_timeout));
    } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
      LOG_WARN("set timeout context failed", K(ret));
    } else if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
      LOG_WARN("failed to start trans", K(ret));
    } else {
      const uint64_t tenant_id = extract_tenant_id(pg_task_info_array.at(0).table_id_);
      if (OB_FAIL(tenant_backup_task_updater_.get_tenant_backup_task(tenant_id, trans, task_info))) {
        LOG_WARN("failed to get tenant backup task info", K(ret), K(tenant_id));
      } else if (ObTenantBackupTaskInfo::DOING != task_info.status_ &&
                 ObTenantBackupTaskInfo::CANCEL != task_info.status_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant backup task status is unexpected", K(ret), K(task_info));
      } else if (OB_FAIL(pg_backup_task_updater_.update_status_and_result_and_statics(pg_task_info_array))) {
        LOG_WARN("failed to update pg backup task status", K(ret), K(pg_task_info_array));
      } else {
        dest_task_info = task_info;
        dest_task_info.finish_partition_count_ += finish_macro_block_count;
        dest_task_info.macro_block_count_ += macro_block_count;
        dest_task_info.finish_macro_block_count_ += finish_macro_block_count;
        dest_task_info.input_bytes_ += input_bytes;
        dest_task_info.output_bytes_ += output_bytes;
        if (OB_FAIL(tenant_backup_task_updater_.update_tenant_backup_task(trans, task_info, dest_task_info))) {
          LOG_WARN("failed to update tenant backup task", K(ret), K(task_info), K(dest_task_info));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans.end(true /*commit*/))) {
          OB_LOG(WARN, "failed to commit", K(ret));
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(false /*rollback*/))) {
          OB_LOG(WARN, "failed to rollback", K(ret), K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObService::refresh_memory_stat()
{
  return ObDumpTaskGenerator::generate_mod_stat_task();
}

int ObService::renew_in_zone_hb(const share::ObInZoneHbRequest& arg, share::ObInZoneHbResponse& result)
{
  int ret = OB_SUCCESS;
  UNUSED(arg);
  UNUSED(result);
  return ret;
}

int ObService::pre_process_server_reply(const obrpc::ObPreProcessServerReplyArg& arg)
{
  int ret = OB_SUCCESS;
  UNUSED(arg);
  return ret;
}

int ObService::broadcast_rs_list(const ObRsListArg& arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service do not init", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs mgr is null", KR(ret), K(arg));
  } else if (OB_FAIL(GCTX.rs_mgr_->force_set_master_rs(arg.master_rs_))) {
    LOG_WARN("fail to set master rs", KR(ret), K(arg));
  } else {
    LOG_INFO("observer set master rs success", K(arg));
  }
  return ret;
}
}  // end namespace observer
}  // end namespace oceanbase
