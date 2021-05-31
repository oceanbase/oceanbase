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

#include "ob_clog_mgr.h"
#include "share/ob_define.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/compress/ob_compressor_pool.h"
#include "share/ob_thread_mgr.h"
#include "election/ob_election_mgr.h"
#include "election/ob_election_priority.h"
#include "storage/transaction/ob_trans_log.h"
#include "ob_batch_submit_ctx.h"
#include "ob_batch_submit_task.h"
#include "storage/ob_partition_service.h"
#include "ob_log_external_rpc.h"
#include "ob_batch_submit_mock_test.h"
#include "ob_log_stress.h"
#include "ob_clog_sync_msg.h"
#include "ob_partition_log_service.h"
#include "storage/transaction/ob_trans_define.h"
#include "ob_clog_config.h"
#include "observer/ob_server.h"
#include "election/ob_election_group_priority.h"
#include "ob_log_replay_engine_wrapper.h"
#include "storage/ob_i_partition_group.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace extlog;
using namespace obrpc;
using namespace logservice;
using namespace archive;
namespace clog {

// The number of threads used for callbacks is set to be the same as the number of cpu cores
int64_t ObCLogMgr::CLOG_CB_THREAD_COUNT = common::get_cpu_num();
int64_t ObCLogMgr::MINI_MODE_CLOG_CB_THREAD_COUNT = 4;

static void print_clog_config(void);
ObCLogMgr::ObCLogMgr()
    : is_inited_(false),
      is_running_(false),
      clog_mgr_config_(),
      log_engine_(),
      fetch_log_engine_(),
      event_scheduler_(),
      pkt_handler_(),
      log_executor_(),
      log_ext_executor_v2_(),
      partition_service_(NULL),
      replay_engine_(NULL),
      election_mgr_(NULL),
      self_addr_(),
      rpc_(NULL),
      scan_runnable_(),
      state_driver_runnable_(),
      // replay_driver_runnable_(),
      log_archive_and_restore_driver_(),
      cb_handler_(),
      clog_thread_pool_(),
      sp_thread_pool_(),
      cb_engine_(),
      archive_mgr_(),
      archive_restore_engine_(),
      check_log_file_collect_task_(),
      checkpoint_log_replica_task_(),
      check_clog_disk_full_task_()
{}

ObCLogMgr::~ObCLogMgr()
{
  destroy();
}

int ObCLogMgr::init(storage::ObPartitionService* partition_service, ObLogReplayEngineWrapper* replay_engine,
    election::ObIElectionMgr* election_mgr, const common::ObAddr& self_addr, obrpc::ObBatchRpc* batch_rpc,
    obrpc::ObLogRpcProxy* rpc, common::ObMySQLProxy* sql_proxy, const CLogMgrConfig& config)
{
  int ret = OB_SUCCESS;
  ObLogExternalExecutorWithBreakpoint::Config lee_config_v2;
  lee_config_v2.ilog_dir_ = config.le_config_.index_log_dir_;
  lee_config_v2.read_timeout_ = config.le_config_.read_timeout_;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(partition_service) || OB_ISNULL(replay_engine) || OB_ISNULL(election_mgr) ||
             !self_addr.is_valid() || OB_ISNULL(rpc) || OB_ISNULL(sql_proxy) || !config.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(self_addr));
  } else if (OB_FAIL(event_scheduler_.init())) {
    CLOG_LOG(WARN, "event_scheduler_ init failed", K(ret));
  } else if (OB_FAIL(log_engine_.init(config.le_config_, self_addr, batch_rpc, rpc, &cb_engine_, partition_service))) {
    CLOG_LOG(WARN, "log_engine_ init failed", K(ret));
  } else if (OB_FAIL(fetch_log_engine_.init(partition_service))) {
    CLOG_LOG(WARN, "fetch_log_engine_ init failed", K(ret));
  } else if (OB_FAIL(pkt_handler_.init(partition_service, this))) {
    CLOG_LOG(WARN, "pkt_handler init failed", K(ret));
  } else if (OB_FAIL(log_executor_.init(&pkt_handler_, &log_engine_))) {
    CLOG_LOG(WARN, "log_executor_ init failed", K(ret));
  } else if (OB_FAIL(log_ext_executor_v2_.init(lee_config_v2, &log_engine_, partition_service))) {
    CLOG_LOG(WARN, "log_ext_executor_v2_ init failed", K(ret));
  } else if (OB_FAIL(external_log_service_.init(partition_service, &log_engine_, this, self_addr))) {
    CLOG_LOG(WARN, "external_log_service_ init error", K(ret));
  } else if (OB_FAIL(scan_runnable_.init(partition_service, &log_engine_))) {
    CLOG_LOG(WARN, "log scan runnable init failed", K(ret));
  } else if (OB_FAIL(cb_handler_.init(partition_service, &cb_engine_))) {
    CLOG_LOG(WARN, "cb_handler_ init failed", K(ret));
  } else if (OB_FAIL(clog_thread_pool_.init(lib::TGDefIDs::LogCb, &cb_handler_, self_addr))) {
    CLOG_LOG(WARN, "clog_thread_pool_ init failed", K(ret));
  } else if (OB_FAIL(sp_thread_pool_.init(lib::TGDefIDs::SpLogCb, &cb_handler_, self_addr))) {
    CLOG_LOG(WARN, "sp_thread_pool_ init failed", K(ret));
  } else if (OB_FAIL(cb_engine_.init(lib::TGDefIDs::LogCb, lib::TGDefIDs::SpLogCb))) {
    CLOG_LOG(WARN, "cb_engine_ init failed", K(ret));
  } else if (OB_FAIL(batch_submit_ctx_mgr_.init(partition_service, &log_engine_, self_addr))) {
    CLOG_LOG(WARN, "batch_submit_ctx_mgr_ init failed", K(ret));
  } else if (OB_FAIL(remote_log_query_engine_.init(sql_proxy, &log_engine_, self_addr))) {
    CLOG_LOG(WARN, "remote_log_query_engine_ init failed", K(ret));
  } else if (OB_FAIL(archive_mgr_.init(&log_engine_, &external_log_service_, partition_service, self_addr))) {
    CLOG_LOG(WARN, "archive_mgr_ init failed", K(ret));
  } else if (OB_FAIL(archive_restore_engine_.init(partition_service))) {
    CLOG_LOG(WARN, "archive_restore_engine_ init failed", K(ret));
  } else if (OB_FAIL(check_log_file_collect_task_.init(this))) {
    CLOG_LOG(WARN, "check_log_file_collect_task_ init failed", K(ret));
  } else if (OB_FAIL(checkpoint_log_replica_task_.init(partition_service))) {
    CLOG_LOG(WARN, "checkpoint_log_replica_task_ init failed", K(ret));
  } else if (OB_FAIL(check_clog_disk_full_task_.init(partition_service))) {
    CLOG_LOG(WARN, "check_clog_disk_full_task_ init failed", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::CFC))) {
    CLOG_LOG(WARN, "fail to initialize clog file collect timer");
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::CKPTLogRep))) {
    CLOG_LOG(WARN, "fail to initialize checkpoint log replica timer");
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::CCDF))) {
    CLOG_LOG(WARN, "fail to initialize check clog disk full timer");
  } else {
    partition_service_ = partition_service;
    replay_engine_ = replay_engine;
    election_mgr_ = election_mgr;
    self_addr_ = self_addr;
    rpc_ = rpc;
    clog_mgr_config_ = config;
    ObLogStressRunnable::register_signal_handler(partition_service, &log_engine_);
    ObBatchSubmitMockTest::register_signal_handler(partition_service, this, self_addr);
    is_inited_ = true;
    print_clog_config();
  }
  if (OB_SUCCESS != ret && !is_inited_) {
    destroy();
  }
  CLOG_LOG(INFO, "clog_mgr init finished", K(ret));
  return ret;
}

void ObCLogMgr::destroy()
{
  CLOG_LOG(INFO, "clog_mgr destroy");
  is_inited_ = false;
  log_engine_.destroy();
  partition_service_ = NULL;
  replay_engine_ = NULL;
  election_mgr_ = NULL;
  self_addr_.reset();
  rpc_ = NULL;
  scan_runnable_.destroy();
  state_driver_runnable_.destroy();
  // replay_driver_runnable_.destroy();
  log_archive_and_restore_driver_.destroy();
  cb_engine_.destroy();
  external_log_service_.destroy();
  batch_submit_ctx_mgr_.destroy();
  remote_log_query_engine_.destroy();
  archive_mgr_.destroy();
  archive_restore_engine_.destroy();
  TG_DESTROY(lib::TGDefIDs::CFC);
  TG_DESTROY(lib::TGDefIDs::CKPTLogRep);
  TG_DESTROY(lib::TGDefIDs::CCDF);
}

/* interface for creating partition*/
int ObCLogMgr::create_partition(const ObPartitionKey& partition_key, const int64_t replica_num,
    const ObMemberList& member_list, const ObAddr& leader, const int64_t lease_start, const ObVersion& freeze_version,
    const ObReplicaType replica_type, const ObReplicaProperty replica_property, const int64_t last_submit_timestamp,
    const uint64_t last_replay_log_id, const int16_t archive_restore_state, const bool need_skip_mlist_check,
    ObIPartitionLogService* pls)
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO,
      "create partition",
      K(partition_key),
      K(member_list),
      K(leader),
      K(lease_start),
      K(replica_type),
      K(last_submit_timestamp));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!partition_key.is_valid() || !leader.is_valid() || !freeze_version.is_valid() || replica_num <= 0 ||
             replica_num > OB_MAX_MEMBER_NUMBER ||
             (!need_skip_mlist_check && (member_list.get_member_number() > replica_num)) ||
             !ObReplicaTypeCheck::is_replica_type_valid(replica_type) || last_submit_timestamp < 0 ||
             OB_INVALID_ID == last_replay_log_id || OB_ISNULL(pls)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K(ret),
        K(partition_key),
        K(freeze_version),
        K(replica_type),
        K(replica_num),
        K(member_list),
        K(last_submit_timestamp),
        K(last_replay_log_id),
        KP(pls));
  } else if (OB_FAIL(create_partition_(partition_key,
                 replica_num,
                 member_list,
                 freeze_version,
                 replica_type,
                 replica_property,
                 last_submit_timestamp,
                 last_replay_log_id,
                 archive_restore_state,
                 need_skip_mlist_check,
                 pls))) {
    CLOG_LOG(WARN, "create partition failed", K(partition_key), K(replica_type), K(last_submit_timestamp), K(ret));
  } else {
  }
  CLOG_LOG(INFO,
      "finish create partition",
      K(partition_key),
      K(member_list),
      K(leader),
      K(lease_start),
      K(replica_type),
      K(last_submit_timestamp),
      K(ret));
  return ret;
}

int ObCLogMgr::create_partition_(const ObPartitionKey& partition_key, const int64_t replica_num,
    const ObMemberList& member_list, const ObVersion& freeze_version, const ObReplicaType replica_type,
    const ObReplicaProperty replica_property, const int64_t last_submit_timestamp, const uint64_t last_replay_log_id,
    const int16_t archive_restore_state, const bool need_skip_mlist_check, ObIPartitionLogService* pls)
{
  int ret = OB_SUCCESS;
  common::ObBaseStorageInfo info;

  common::ObILogAllocator* alloc_mgr = NULL;
  const uint64_t tenant_id = partition_key.get_tenant_id();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!partition_key.is_valid() || !freeze_version.is_valid() || replica_num <= 0 ||
             replica_num > OB_MAX_MEMBER_NUMBER || !ObReplicaTypeCheck::is_replica_type_valid(replica_type) ||
             !replica_property.is_valid() || last_submit_timestamp < 0 || OB_INVALID_ID == last_replay_log_id ||
             OB_ISNULL(pls)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K(ret),
        K(partition_key),
        K(freeze_version),
        K(replica_type),
        K(replica_property),
        K(replica_num),
        K(last_submit_timestamp),
        KP(pls));
  } else if (!is_scan_finished()) {
    CLOG_LOG(WARN, "can not create new partition while scanning", K(partition_key));
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(info.init(
                 replica_num, member_list, last_submit_timestamp, last_replay_log_id, need_skip_mlist_check))) {
    CLOG_LOG(WARN,
        "ObBaseStorageInfo init failed",
        K(replica_num),
        K(member_list),
        K(replica_type),
        K(last_replay_log_id),
        K(ret));
  } else if (OB_FAIL(TMA_MGR_INSTANCE.get_tenant_log_allocator(tenant_id, alloc_mgr))) {
    CLOG_LOG(WARN, "get_tenant_log_allocator failed", K(ret), K(tenant_id));
  } else if (NULL == alloc_mgr) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "get_tenant_log_allocator return NULL", K(ret), K(tenant_id), KP(alloc_mgr));
  } else if (OB_FAIL(pls->init(&log_engine_,
                 replay_engine_,
                 &fetch_log_engine_,
                 election_mgr_,
                 partition_service_,
                 &cb_engine_,
                 alloc_mgr,
                 &event_scheduler_,
                 self_addr_,
                 freeze_version,
                 partition_key,
                 replica_type,
                 replica_property,
                 info,
                 archive_restore_state,
                 need_skip_mlist_check,
                 &remote_log_query_engine_,
                 &archive_mgr_,
                 &archive_restore_engine_,
                 CREATE_PARTITION))) {
    CLOG_LOG(WARN, "ObPartitionLogService init failed", K(ret), K(partition_key));
  } else if ((share::REPLICA_RESTORE_DATA <= archive_restore_state &&
                 archive_restore_state < share::REPLICA_RESTORE_LOG) &&
             OB_FAIL(pls->set_offline())) {
    // physical restore data phase need set clog offline
    STORAGE_LOG(WARN, "fail to set_offline", K(ret), K(partition_key));
  } else if (archive_restore_state >= share::REPLICA_RESTORE_DATA && OB_FAIL(pls->set_scan_disk_log_finished())) {
    // partitions created with physical restore flag need skip scan disk log stage, similar with add_partition
    // standby replica will always run here.
    CLOG_LOG(WARN, "set_scan_disk_log_finished failed", K(ret), K(partition_key));
  } else {
  }
  return ret;
}

/* interface for server restart */
int ObCLogMgr::assign_partition(const ObPartitionKey& partition_key, const ObReplicaType replica_type,
    const ObReplicaProperty replica_property, const ObBaseStorageInfo& info, const ObVersion& freeze_version,
    const int16_t archive_restore_state, ObIPartitionLogService* pls)
{
  int ret = OB_SUCCESS;

  common::ObILogAllocator* alloc_mgr = NULL;
  const uint64_t tenant_id = partition_key.get_tenant_id();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!partition_key.is_valid() || !freeze_version.is_valid() || OB_ISNULL(pls)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!scan_runnable_.is_before_scan()) {
    CLOG_LOG(WARN, "can not assign partition after beginning scanning");
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(TMA_MGR_INSTANCE.get_tenant_log_allocator(tenant_id, alloc_mgr))) {
    CLOG_LOG(WARN, "get_tenant_log_allocator failed", K(ret), K(tenant_id));
  } else if (NULL == alloc_mgr) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "get_tenant_log_allocator return NULL", K(ret), K(tenant_id), KP(alloc_mgr));
  } else if (OB_FAIL(pls->init(&log_engine_,
                 replay_engine_,
                 &fetch_log_engine_,
                 election_mgr_,
                 partition_service_,
                 &cb_engine_,
                 alloc_mgr,
                 &event_scheduler_,
                 self_addr_,
                 freeze_version,
                 partition_key,
                 replica_type,
                 replica_property,
                 info,
                 archive_restore_state,
                 false,
                 &remote_log_query_engine_,
                 &archive_mgr_,
                 &archive_restore_engine_,
                 ASSIGN_PARTITION))) {
    CLOG_LOG(WARN, "ObPartitionLogService init failed", K(ret));
  } else if ((share::REPLICA_RESTORE_DATA <= archive_restore_state &&
                 archive_restore_state < share::REPLICA_RESTORE_LOG) &&
             OB_FAIL(pls->set_offline())) {
    // physical restore data state need set clog offline
  } else {
  }
  CLOG_LOG(INFO,
      "finish assign partition",
      K(partition_key),
      K(replica_type),
      "base_storage_info",
      info,
      K(freeze_version),
      K(ret));
  return ret;
}

/* interface for migrating */
int ObCLogMgr::add_partition(const ObPartitionKey& partition_key, const ObReplicaType replica_type,
    const ObReplicaProperty replica_property, const common::ObBaseStorageInfo& info,
    const common::ObVersion& freeze_version, const ObAddr& src_server, const int16_t archive_restore_state,
    ObIPartitionLogService* pls)
{
  int ret = OB_SUCCESS;
  uint64_t max_ilog_id = OB_INVALID_ID;
  common::ObILogAllocator* alloc_mgr = NULL;
  const uint64_t tenant_id = partition_key.get_tenant_id();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!partition_key.is_valid() || !freeze_version.is_valid() || !src_server.is_valid() || OB_ISNULL(pls)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K(ret),
        K(partition_key),
        K(replica_type),
        K(info),
        K(freeze_version),
        K(src_server),
        KP(pls));
  } else if (!is_scan_finished()) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "can not add new partition while scanning", K(ret), K(partition_key));
  } else if (OB_FAIL(TMA_MGR_INSTANCE.get_tenant_log_allocator(tenant_id, alloc_mgr))) {
    CLOG_LOG(WARN, "get_tenant_log_allocator failed", K(ret), K(tenant_id));
  } else if (NULL == alloc_mgr) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "get_tenant_log_allocator return NULL", K(ret), K(tenant_id), KP(alloc_mgr));
  } else if (OB_FAIL(pls->init(&log_engine_,
                 replay_engine_,
                 &fetch_log_engine_,
                 election_mgr_,
                 partition_service_,
                 &cb_engine_,
                 alloc_mgr,
                 &event_scheduler_,
                 self_addr_,
                 freeze_version,
                 partition_key,
                 replica_type,
                 replica_property,
                 info,
                 archive_restore_state,
                 false,
                 &remote_log_query_engine_,
                 &archive_mgr_,
                 &archive_restore_engine_,
                 ADD_PARTITION))) {
    CLOG_LOG(WARN, "ObPartitionLogService init failed", K(partition_key), K(ret));
  } else if (OB_FAIL(pls->set_offline())) {
    CLOG_LOG(WARN, "set_offline failed", K(partition_key), K(ret));
  } else if (OB_FAIL(pls->set_scan_disk_log_finished())) {
    CLOG_LOG(WARN, "set_scan_disk_log_finished failed", K(partition_key), K(ret));
  } else if (OB_FAIL(pls->set_follower_active())) {
    CLOG_LOG(WARN, "set_follower_active failed", K(partition_key), K(ret));
  } else {
  }
  CLOG_LOG(INFO,
      "finish add partition",
      K(partition_key),
      K(replica_type),
      "base_storage_info",
      info,
      K(freeze_version),
      K(max_ilog_id),
      K(ret));
  return ret;
}

int ObCLogMgr::handle_packet(int pcode, const char* data, int64_t len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (OB_ISNULL(data) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(log_executor_.handle_packet(pcode, data, len))) {
    CLOG_LOG(WARN, "handle packet failed", K(ret));
  } else {
  }
  return ret;
}

int ObCLogMgr::req_start_log_id_by_ts(
    const obrpc::ObLogReqStartLogIdByTsRequest& req_msg, obrpc::ObLogReqStartLogIdByTsResponse& result)
{
  UNUSED(req_msg);
  UNUSED(result);
  CLOG_LOG(ERROR, "deprecated rpc");
  return OB_NOT_SUPPORTED;
#if 0
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(log_ext_executor_.req_start_log_id_by_ts(req_msg, result))) {
    CLOG_LOG(WARN, "log executor req_start_log_id_by_ts failed", K(ret));
  } else {
    // success
  }
  // rpc ret code, always OB_SUCCESS
  return OB_SUCCESS;
#endif
}

int ObCLogMgr::req_start_pos_by_log_id(
    const obrpc::ObLogReqStartPosByLogIdRequest& req_msg, obrpc::ObLogReqStartPosByLogIdResponse& result)
{
  UNUSED(req_msg);
  UNUSED(result);
  CLOG_LOG(ERROR, "deprecated rpc");
  return OB_NOT_SUPPORTED;
#if 0
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(log_ext_executor_.req_start_pos_by_log_id(req_msg, result))) {
    CLOG_LOG(WARN, "log executor req_start_pos_by_log_id failed", K(ret));
  } else {
    // success
  }
  // rpc ret code, always OB_SUCCESS
  return OB_SUCCESS;
#endif
}

int ObCLogMgr::fetch_log(
    const obrpc::ObLogExternalFetchLogRequest& req_msg, obrpc::ObLogExternalFetchLogResponse& result)
{
  UNUSED(req_msg);
  UNUSED(result);
  CLOG_LOG(ERROR, "deprecated rpc");
  return OB_NOT_SUPPORTED;
#if 0
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(log_ext_executor_.fetch_log(req_msg, result))) {
    CLOG_LOG(WARN, "log executor fetch_log failed", K(ret));
  } else {
    // success
  }
  // rpc ret code, always OB_SUCCESS
  return OB_SUCCESS;
#endif
}

int ObCLogMgr::req_heartbeat_info(
    const obrpc::ObLogReqHeartbeatInfoRequest& req_msg, obrpc::ObLogReqHeartbeatInfoResponse& result)
{
  UNUSED(req_msg);
  UNUSED(result);
  CLOG_LOG(ERROR, "deprecated rpc");
  return OB_NOT_SUPPORTED;
#if 0
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(log_ext_executor_.req_heartbeat_info(req_msg, result))) {
    CLOG_LOG(WARN, "log executor req_heartbeat_info failed", K(ret));
  } else {
    // success
  }
  // rpc ret code, always OB_SUCCESS
  return OB_SUCCESS;
#endif
}

int ObCLogMgr::req_start_log_id_by_ts_with_breakpoint(const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req_msg,
    obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& result)
{
  UNUSED(req_msg);
  UNUSED(result);
  CLOG_LOG(ERROR, "deprecated rpc");
  return OB_NOT_SUPPORTED;
#if 0
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(log_ext_executor_v2_.req_start_log_id_by_ts_with_breakpoint(req_msg, result))) {
    CLOG_LOG(WARN, "log executorv2 req_start_log_id_by_ts_with_breakpoint failed", K(ret));
  } else {
    // success
  }
  // rpc ret code, always OB_SUCCESS
  return OB_SUCCESS;
#endif
}

int ObCLogMgr::req_start_pos_by_log_id_with_breakpoint(
    const obrpc::ObLogReqStartPosByLogIdRequestWithBreakpoint& req_msg,
    obrpc::ObLogReqStartPosByLogIdResponseWithBreakpoint& result)
{
  UNUSED(req_msg);
  UNUSED(result);
  CLOG_LOG(ERROR, "deprecated rpc");
  return OB_NOT_SUPPORTED;
#if 0
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(log_ext_executor_v2_.req_start_pos_by_log_id_with_breakpoint(req_msg, result))) {
    CLOG_LOG(WARN, "log executor req_start_pos_by_log_id_with failed", K(ret));
  } else {
    // success
  }
  // rpc ret code, always OB_SUCCESS
  return OB_SUCCESS;
#endif
}

int ObCLogMgr::run_check_log_file_collect_task()
{
  int ret = OB_SUCCESS;
  static int64_t collect_task_run_times = 0;
  const int64_t begin_ts = ObTimeUtility::current_time();
  collect_task_run_times++;
  CLOG_LOG(INFO, "begin run_check_log_file_collect_task", K(begin_ts), K(collect_task_run_times));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (!is_scan_finished()) {
    CLOG_LOG(INFO, "scanning, not execute file collect task, wait next time");
    ret = OB_SUCCESS;
  } else if (OB_FAIL(log_engine_.update_min_using_file_id())) {
    CLOG_LOG(WARN, "update min using file id failed", K(ret));
  } else {
    // do nothing
  }
  const int64_t end_ts = ObTimeUtility::current_time();
  EVENT_SET(CLOG_LAST_CHECK_LOG_FILE_COLLECT_TIME, end_ts);
  CLOG_LOG(INFO,
      "finish run_check_log_file_collect_task",
      K(ret),
      K(begin_ts),
      K(end_ts),
      "task_cost_time",
      end_ts - begin_ts,
      K(collect_task_run_times));
  // log_engine_.try_recycle_file();

  return ret;
}

int ObCLogMgr::start()
{
  int ret = OB_SUCCESS;
  const bool repeat = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(log_engine_.start())) {
    CLOG_LOG(WARN, "log engine start failed", K(ret));
  } else if (OB_FAIL(event_scheduler_.start())) {
    CLOG_LOG(WARN, "event_scheduler_ start failed", K(ret));
  } else if (OB_FAIL(archive_mgr_.start())) {
    CLOG_LOG(WARN, "archive_mgr_ start failed", K(ret));
  } else if (OB_FAIL(state_driver_runnable_.init(partition_service_, election_mgr_))) {
    CLOG_LOG(WARN, "state_driver_runnable start fail", "ret", ret);
    // } else if (OB_FAIL(replay_driver_runnable_.init(partition_service_))) {
    //  CLOG_LOG(WARN, "replay_driver_runnable start fail", "ret", ret);
  } else if (OB_FAIL(log_archive_and_restore_driver_.init(partition_service_))) {
    CLOG_LOG(WARN, "log_archive_and_restore_driver_ start fail", KR(ret));
  } else if (OB_FAIL(scan_runnable_.start())) {
    CLOG_LOG(WARN, "scan_runnable start fail", "ret", ret);
  } else if (OB_FAIL(batch_submit_ctx_mgr_.start())) {
    CLOG_LOG(WARN, "batch_submit_ctx_mgr_ start failed", K(ret));
  } else if (OB_FAIL(remote_log_query_engine_.start())) {
    CLOG_LOG(WARN, "remote_log_query_engine_ start failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(
                 lib::TGDefIDs::CFC, check_log_file_collect_task_, CLOG_FILE_COLLECT_CHECK_INTERVAL, repeat))) {
    CLOG_LOG(WARN, "fail to schedule check_log_file_collect_task_", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::CKPTLogRep,
                 checkpoint_log_replica_task_,
                 CLOG_CHECKPOINT_LOG_REPLICA_INTERVAL,
                 repeat))) {
    CLOG_LOG(WARN, "fail to schedule checkpoint_log_replica_task_", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(
                 lib::TGDefIDs::CCDF, check_clog_disk_full_task_, CLOG_CHECK_CLOG_DISK_FULL_INTERVAL, repeat))) {
    CLOG_LOG(WARN, "fail to schedule check_clog_disk_full_task_", K(ret));
  } else {
    is_running_ = true;
  }
  return ret;
}

void ObCLogMgr::stop()
{
  int tmp_ret = OB_SUCCESS;
  is_running_ = false;
  if (OB_SUCCESS != (tmp_ret = event_scheduler_.stop())) {
    CLOG_LOG(WARN, "event_scheduler_ stop failed", K(tmp_ret));
  }

  log_archive_and_restore_driver_.stop();
  state_driver_runnable_.stop();
  archive_mgr_.stop();
  archive_restore_engine_.stop();
  // replay_driver_runnable_.stop();
  fetch_log_engine_.destroy();
  scan_runnable_.stop();
  log_engine_.stop();
  batch_submit_ctx_mgr_.stop();
  remote_log_query_engine_.stop();
  TG_STOP(lib::TGDefIDs::CFC);
  TG_STOP(lib::TGDefIDs::CKPTLogRep);
  TG_STOP(lib::TGDefIDs::CCDF);
}

void ObCLogMgr::wait()
{
  int tmp_ret = OB_SUCCESS;
  log_archive_and_restore_driver_.wait();
  state_driver_runnable_.wait();
  if (OB_SUCCESS != (tmp_ret = event_scheduler_.wait())) {
    CLOG_LOG(WARN, "event_scheduler_ wait failed", K(tmp_ret));
  }
  // replay_driver_runnable_.wait();
  scan_runnable_.wait();
  archive_mgr_.wait();
  archive_restore_engine_.wait();
  // election_mgr_ should stop after state driver thread exits, otherwise election_mgr_->get_leader will report an error
  if (OB_SUCCESS != (tmp_ret = election_mgr_->stop())) {
    CLOG_LOG(WARN, "election_mgr_ stop failed", K(tmp_ret));
  }
  if (OB_SUCCESS != (tmp_ret = election_mgr_->wait())) {
    CLOG_LOG(WARN, "election_mgr_ wait failed", K(tmp_ret));
  }
  remote_log_query_engine_.wait();
  log_engine_.wait();
  clog_thread_pool_.destroy();
  sp_thread_pool_.destroy();
  batch_submit_ctx_mgr_.wait();
  TG_WAIT(lib::TGDefIDs::CFC);
  TG_WAIT(lib::TGDefIDs::CKPTLogRep);
  TG_WAIT(lib::TGDefIDs::CCDF);
}

bool ObCLogMgr::is_scan_finished() const
{
  return scan_runnable_.is_scan_finished();
}

int ObCLogMgr::handle_get_mc_ts_request(
    const obrpc::ObLogGetMCTsRequest& request_msg, obrpc::ObLogGetMCTsResponse& result)
{
  int ret = OB_SUCCESS;
  bool is_normal_partition = false;
  const int64_t before_handle_ts = ObTimeUtility::current_time();
  result.reset();
  const common::ObPartitionKey partition_key = request_msg.get_partition_key();
  ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(partition_service_->get_partition(partition_key, guard)) || NULL == guard.get_partition_group() ||
             NULL == (log_service = guard.get_partition_group()->get_log_service())) {
    CLOG_LOG(WARN, "get_log_service failed", K(partition_key));
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!(guard.get_partition_group()->is_valid())) {
    ret = OB_INVALID_PARTITION;
    CLOG_LOG(WARN, "partition is invalid", K(partition_key), K(ret));
  } else if (OB_FAIL(log_service->check_is_normal_partition(is_normal_partition))) {
    CLOG_LOG(WARN, "check_normal_partition failed", K(ret), K(partition_key));
  } else {
    result.set_partition_key(partition_key);
    result.set_membership_timestamp(log_service->get_membership_timestamp());
    result.set_max_confirmed_log_id(log_service->get_next_index_log_id() - 1);
    result.set_is_normal_partition(is_normal_partition);
  }
  const int64_t after_handle_ts = ObTimeUtility::current_time();
  if (after_handle_ts - before_handle_ts > 50 * 1000) {
    CLOG_LOG(WARN, "handle cost too much time", K(request_msg), "time", after_handle_ts - before_handle_ts);
  }
  CLOG_LOG(INFO, "handle_get_mc_ts_request", K(ret), K(request_msg), K(result));
  return ret;
}

int ObCLogMgr::handle_get_priority_array_request(
    const obrpc::ObLogGetPriorityArrayRequest& request_msg, obrpc::ObLogGetPriorityArrayResponse& result)
{
  int ret = OB_SUCCESS;
  bool has_non_candidate = false;
  const int64_t before_handle_ts = ObTimeUtility::current_time();
  result.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else {
    const ObPartitionArray& pkey_array = request_msg.get_partition_array();
    const int64_t pkey_cnt = pkey_array.count();
    election::PriorityArray priority_array;
    for (int64_t pkey_idx = 0; OB_SUCC(ret) && (pkey_idx < pkey_cnt); ++pkey_idx) {
      int tmp_ret = OB_SUCCESS;
      ObIPartitionGroupGuard guard;
      ObIPartitionLogService* log_service = NULL;
      const ObPartitionKey cur_pkey = pkey_array.at(pkey_idx);
      election::ObElectionPriority cur_priority;
      cur_priority.reset();
      if (OB_SUCCESS != (tmp_ret = partition_service_->get_partition(cur_pkey, guard)) ||
          NULL == guard.get_partition_group() ||
          NULL == (log_service = guard.get_partition_group()->get_log_service())) {
        tmp_ret = OB_ENTRY_NOT_EXIST;
        CLOG_LOG(WARN, "get_log_service failed", K(tmp_ret), K(cur_pkey));
      } else if (!(guard.get_partition_group()->is_valid())) {
        tmp_ret = OB_INVALID_PARTITION;
        CLOG_LOG(WARN, "partition is invalid", K(cur_pkey), K(tmp_ret));
      } else if (guard.get_partition_group()->is_splitting()) {
        CLOG_LOG(WARN, "partition is splitting", K(cur_pkey));
      } else if (OB_SUCCESS != (tmp_ret = log_service->on_get_election_priority(cur_priority))) {
        CLOG_LOG(WARN, "on_get_election_priority failed", K(tmp_ret), K(cur_pkey));
      } else {
        if (!cur_priority.is_candidate()) {
          has_non_candidate = true;
        }
      }
      if (OB_FAIL(priority_array.push_back(cur_priority))) {
        CLOG_LOG(WARN, "priority_array push_back failed", K(ret));
      }
    }
    if (OB_SUCCESS == ret) {
      result.set_priority_array(priority_array);
    }
  }
  const int64_t after_handle_ts = ObTimeUtility::current_time();
  if (after_handle_ts - before_handle_ts > 50 * 1000) {
    CLOG_LOG(WARN, "handle cost too much time", K(request_msg), "time", after_handle_ts - before_handle_ts);
  }
  if (has_non_candidate) {
    CLOG_LOG(INFO, "finish handle_get_priority_array_request", K(ret), K(request_msg), K(result));
  }
  return ret;
}

int ObCLogMgr::handle_get_mc_ctx_array_request(
    const obrpc::ObLogGetMcCtxArrayRequest& request_msg, obrpc::ObLogGetMcCtxArrayResponse& result)
{
  int ret = OB_SUCCESS;
  const int64_t before_handle_ts = ObTimeUtility::current_time();
  result.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else {
    const common::ObPartitionArray& partition_array = request_msg.get_partition_array();
    const int64_t partition_cnt = partition_array.count();
    McCtxArray mc_ctx_array;
    for (int64_t partition_idx = 0; (OB_SUCCESS == ret) && (partition_idx < partition_cnt); partition_idx++) {
      int tmp_ret = OB_SUCCESS;
      ObIPartitionGroupGuard guard;
      ObIPartitionLogService* log_service = NULL;
      bool is_normal_partition = false;
      const common::ObPartitionKey partition_key = partition_array[partition_idx];
      if (OB_SUCCESS != (tmp_ret = partition_service_->get_partition(partition_key, guard)) ||
          NULL == guard.get_partition_group() ||
          NULL == (log_service = guard.get_partition_group()->get_log_service())) {
        tmp_ret = OB_ENTRY_NOT_EXIST;
        CLOG_LOG(WARN, "get_log_service failed", K(tmp_ret), K(partition_key));
      } else if (!(guard.get_partition_group()->is_valid())) {
        tmp_ret = OB_INVALID_PARTITION;
        CLOG_LOG(WARN, "partition is invalid", K(partition_key), K(tmp_ret));
      } else if (OB_FAIL(log_service->check_is_normal_partition(is_normal_partition))) {
        CLOG_LOG(WARN, "check_is_normal_partition failed", K(ret), K(partition_key));
      } else {
        McCtx mc_ctx;
        mc_ctx.partition_key_ = partition_key;
        mc_ctx.mc_timestamp_ = log_service->get_membership_timestamp();
        mc_ctx.max_confirmed_log_id_ = log_service->get_next_index_log_id() - 1;
        mc_ctx.is_normal_partition_ = is_normal_partition;

        if (OB_FAIL(mc_ctx_array.push_back(mc_ctx))) {
          CLOG_LOG(WARN, "mc_ctx_array push_back failed", K(ret));
        }
      }
    }
    if (OB_SUCCESS == ret) {
      result.set_mc_ctx_array(mc_ctx_array);
    }
  }
  const int64_t after_handle_ts = ObTimeUtility::current_time();
  if (after_handle_ts - before_handle_ts > 50 * 1000) {
    CLOG_LOG(WARN, "handle cost too much time", K(request_msg), "time", after_handle_ts - before_handle_ts);
  }
  CLOG_LOG(INFO, "handle_get_mc_ctx_array_request", K(ret), K(request_msg), K(result));
  return ret;
}

int ObCLogMgr::get_last_slide_log_id(const ObPartitionKey& pkey, uint64_t& log_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(partition_service_->get_partition(pkey, guard)) || NULL == guard.get_partition_group() ||
             NULL == (log_service = guard.get_partition_group()->get_log_service())) {
    CLOG_LOG(WARN, "get_log_service failed", K(pkey));
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!(guard.get_partition_group()->is_valid())) {
    ret = OB_INVALID_PARTITION;
    CLOG_LOG(WARN, "partition is invalid", K(pkey), K(ret));
  } else {
    log_id = log_service->get_last_slide_log_id();
  }
  return ret;
}

static void print_clog_config(void)
{
  CLOG_LOG(INFO,
      "clog_config",
      K(ENABLE_CLOG_PERF),
      K(CLOG_PERF_WARN_THRESHOLD),
      K(ENABLE_CLOG_TRACE_PROFILE),
      K(CLOG_TRACE_PROFILE_WARN_THRESHOLD),
      K(ENABLE_CLOG_CACHE),
      K(CLOG_FETCH_LOG_THREAD_COUNT),
      K(CLOG_FETCH_LOG_TASK_QUEUE_SIZE),
      K(CLOG_CB_THREAD_COUNT),
      K(CLOG_CB_TASK_QUEUE_SIZE),
      K(CLOG_SP_CB_THREAD_COUNT),
      K(CLOG_SP_CB_TASK_QUEUE_SIZE));
  CLOG_LOG(INFO,
      "clog_config",
      K(CLOG_FILE_COLLECT_CHECK_INTERVAL),
      K(CLOG_SWITCH_LEADER_ALLOW_THRESHOLD),
      K(CLOG_STATE_DRIVER_INTERVAL),
      K(CLOG_REPLAY_DRIVER_LOWER_INTERVAL),
      K(CLOG_REPLAY_DRIVER_UPPER_INTERVAL),
      K(CLOG_REPLAY_DRIVER_RUN_THRESHOLD));
  CLOG_LOG(INFO,
      "clog_config",
      K(CLOG_LEADER_ACTIVE_SYNC_TIMEOUT),
      K(CLOG_LEADER_RECONFIRM_SYNC_TIMEOUT),
      K(CLOG_RECONFIRM_FETCH_MAX_LOG_ID_INTERVAL),
      K(CLOG_DISK_BUFFER_COUNT),
      K(CLOG_DISK_BUFFER_SIZE));
}

int ObCheckLogFileCollectTask::init(ObICLogMgr* clog_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(clog_mgr)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    clog_mgr_ = clog_mgr;
    is_inited_ = true;
    disable_timeout_check();
  }
  return ret;
}

void ObCheckLogFileCollectTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCheckLogFileCollectTask is not inited");
  } else if (OB_ISNULL(clog_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(clog_mgr_->run_check_log_file_collect_task())) {
    CLOG_LOG(WARN, "run_check_log_file_collect_task error", K(ret));
  } else {
    CLOG_LOG(INFO, "run_check_log_file_collect_task success");
  }
}

int ObCLogMgr::query_max_ilog_id_(const common::ObPartitionKey& pkey, uint64_t& ret_max_ilog_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret), K(pkey));
  } else if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(pkey));
  } else if (OB_FAIL(log_engine_.query_max_ilog_id(pkey, ret_max_ilog_id)) && OB_PARTITION_NOT_EXIST != ret &&
             OB_FILE_RECYCLED != ret) {
    CLOG_LOG(WARN, "query_max_ilog_id failed", K(ret), K(pkey));
  }

  if (OB_PARTITION_NOT_EXIST == ret || OB_FILE_RECYCLED == ret) {
    ret_max_ilog_id = 0;
    ret = OB_SUCCESS;
  }

  return ret;
}

//======================= batch change member begin =========================
int ObCLogMgr::batch_check_partition_array_(const common::ObPartitionArray& partition_array)
{
  // check batch change member request of standby cluster, ensuring that there are no mixed private/non-private tables
  int ret = OB_SUCCESS;
  bool is_standby_non_private_change = false;
  if (GCTX.is_standby_cluster()) {
    bool has_private_tbl = false;
    const int64_t part_cnt = partition_array.count();
    for (int64_t i = 0; i < part_cnt && OB_SUCC(ret); ++i) {
      const ObPartitionKey& tmp_pkey = partition_array.at(i);
      if (share::ObMultiClusterUtil::is_cluster_private_table(tmp_pkey.get_table_id())) {
        // private table
        has_private_tbl = true;
        if (is_standby_non_private_change) {
          ret = OB_INVALID_ARGUMENT;
          CLOG_LOG(WARN,
              "mixed non-private and private tables in partition_array, unexpected",
              K(ret),
              K(i),
              K(tmp_pkey),
              K(partition_array));
        }
      } else {
        is_standby_non_private_change = true;
        if (has_private_tbl) {
          ret = OB_INVALID_ARGUMENT;
          CLOG_LOG(WARN,
              "mixed non-private and private tables in partition_array, unexpected",
              K(ret),
              K(i),
              K(tmp_pkey),
              K(partition_array));
        }
      }
    }
  }
  return ret;
}

int ObCLogMgr::batch_add_member(const common::ObPartitionArray& partition_array,
    const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
    common::ObReturnArray& ret_array, ObMCLogInfoArray& log_info_array)
{
  int ret = OB_SUCCESS;
  const bool is_add_member = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(batch_check_partition_array_(partition_array))) {
    CLOG_LOG(WARN, "batch_check_partition_array_ failed", K(ret), K(partition_array));
  } else {
    ret = batch_change_member_(partition_array, member_array, quorum_array, is_add_member, ret_array, log_info_array);
    CLOG_LOG(INFO,
        "batch_add_member finished",
        K(ret),
        K(partition_array),
        K(member_array),
        K(quorum_array),
        K(is_add_member),
        K(ret_array),
        K(log_info_array));
  }
  return ret;
}

int ObCLogMgr::batch_remove_member(const common::ObPartitionArray& partition_array,
    const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
    common::ObReturnArray& ret_array, ObMCLogInfoArray& log_info_array)
{
  int ret = OB_SUCCESS;
  const bool is_add_member = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(batch_check_partition_array_(partition_array))) {
    CLOG_LOG(WARN, "batch_check_partition_array_ failed", K(ret), K(partition_array));
  } else {
    ret = batch_change_member_(partition_array, member_array, quorum_array, is_add_member, ret_array, log_info_array);
    CLOG_LOG(INFO,
        "batch_remove_member finished",
        K(ret),
        K(partition_array),
        K(member_array),
        K(quorum_array),
        K(is_add_member),
        K(ret_array),
        K(log_info_array));
  }
  return ret;
}

int ObCLogMgr::batch_is_member_change_done(const common::ObPartitionArray& partition_array,
    const ObMCLogInfoArray& log_info_array, common::ObReturnArray& ret_array)
{
  int ret = OB_SUCCESS;

  const int64_t partition_cnt = partition_array.count();
  const int64_t log_info_cnt = log_info_array.count();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (partition_cnt != log_info_cnt) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "partition_array and log_info_array do not match", K(ret), K(partition_cnt), K(log_info_cnt));
  } else {
    for (int64_t idx = 0; (OB_SUCCESS == ret) && (idx < partition_cnt); idx++) {
      int tmp_ret = OB_SUCCESS;
      const common::ObPartitionKey& partition_key = partition_array[idx];
      const obrpc::ObMCLogInfo& log_info = log_info_array[idx];

      storage::ObIPartitionGroupGuard guard;
      ObIPartitionLogService* log_service = NULL;
      if ((OB_SUCCESS != (tmp_ret = partition_service_->get_partition(partition_key, guard))) ||
          NULL == guard.get_partition_group() ||
          NULL == (log_service = guard.get_partition_group()->get_log_service())) {
        tmp_ret = OB_PARTITION_NOT_EXIST;
        CLOG_LOG(WARN, "invalid partition", K(tmp_ret), K(partition_service_), K(partition_key));
      } else if (!(guard.get_partition_group()->is_valid())) {
        tmp_ret = OB_INVALID_PARTITION;
        CLOG_LOG(WARN, "partition is invalid", K(tmp_ret), K(partition_key));
      } else if (OB_SUCCESS != (tmp_ret = log_service->is_member_change_done(log_info))) {
        // do nothing;
      } else {
      }

      if (OB_FAIL(ret_array.push_back(tmp_ret))) {
        CLOG_LOG(WARN, "ret_array push_back failed", K(ret), K(tmp_ret), K(partition_key));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rewrite_ret_value_(ret_array))) {
      CLOG_LOG(WARN, "rewrite_ret_value_ failed", K(ret));
    }
  }

  return ret;
}

int ObCLogMgr::batch_change_member_(const common::ObPartitionArray& partition_array,
    const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array, const bool is_add_member,
    common::ObReturnArray& ret_array, ObMCLogInfoArray& log_info_array)
{
  int ret = OB_SUCCESS;

  const int64_t partition_cnt = partition_array.count();
  const int64_t member_cnt = member_array.count();
  const int64_t quorum_cnt = quorum_array.count();

  McTimestampArray mc_timestamp_array;
  MemberListArray member_list_array;
  ProposalIDArray proposal_id_array;
  ObPartitionArray need_reset_partition_array;
  ReturnMap ret_map;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (partition_cnt != member_cnt || partition_cnt != quorum_cnt) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "partition_array, member_array or quorum_array do not match",
        K(ret),
        K(partition_cnt),
        K(member_cnt),
        K(quorum_cnt));
  } else if (OB_FAIL(ret_map.init(ObModIds::OB_CLOG_MGR))) {
    CLOG_LOG(WARN, "ret_map init failed", K(ret));
    // step 1: both remove_member/add_member need exec
  } else if (OB_FAIL(pre_batch_change_member_(partition_array,
                 quorum_array,
                 is_add_member,
                 mc_timestamp_array,
                 member_list_array,
                 proposal_id_array,
                 ret_map))) {
    CLOG_LOG(WARN, "pre_batch_change_member_ failed", K(ret));
    // step 2: only remove_member need exec
  } else if (!is_add_member && OB_FAIL(batch_block_partition_split_(partition_array, ret_map))) {
    CLOG_LOG(WARN, "batch block partition split failed", K(ret));
    // step 3: both remove_member/add_member need exec
  } else if (OB_FAIL(batch_check_remote_mc_ts_(
                 partition_array, member_array, quorum_array, mc_timestamp_array, member_list_array, ret_map))) {
    CLOG_LOG(WARN, "batch_check_remote_mc_ts_ failed", K(ret));
    // step 4: only remove_member need exec
  } else if (!is_add_member &&
             OB_FAIL(batch_set_member_changing_(partition_array, need_reset_partition_array, ret_map))) {
    CLOG_LOG(WARN, "batch_set_member_changing_ failed", K(ret));
    // step 5: only remove_member need exec
  } else if (!is_add_member && OB_FAIL(batch_wait_log_confirmed_(partition_array, ret_map))) {
    CLOG_LOG(WARN, "batch_wait_log_confirmed_ failed", K(ret));
    // step 6: only remove_member need exec
  } else if (!is_add_member &&
             OB_FAIL(batch_check_remote_mc_ts_sync_(
                 partition_array, member_array, quorum_array, mc_timestamp_array, member_list_array, ret_map))) {
    CLOG_LOG(WARN, "batch_check_remote_mc_ts_sync_ failed", K(ret));
    // step 7: remove_member/add_member need exec
  } else if (OB_FAIL(batch_execute_change_member_(partition_array,
                 member_array,
                 quorum_array,
                 mc_timestamp_array,
                 proposal_id_array,
                 is_add_member,
                 log_info_array,
                 ret_map))) {
    CLOG_LOG(WARN, "batch_add_member_ failed", K(ret));
    // step 8: only remove_member need exec
  } else if (!is_add_member && OB_FAIL(batch_unblock_partition_split_(partition_array, ret_map))) {
    CLOG_LOG(WARN, "batch_unblock_partition_split_ failed", K(ret));
  } else {
    // do nothing
  }

  if (!is_add_member) {
    // only remove_member need exec
    (void)batch_reset_member_changing_(need_reset_partition_array);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rewrite_ret_value_(partition_array, ret_map, ret_array))) {
      CLOG_LOG(WARN, "rewrite_ret_value_ failed", K(ret));
    }
  }

  return ret;
}

int ObCLogMgr::pre_batch_change_member_(const common::ObPartitionArray& partition_array,
    const common::ObQuorumArray& quorum_array, const bool is_add_member, McTimestampArray& mc_timestamp_array,
    MemberListArray& member_list_array, ProposalIDArray& proposal_id_array, ReturnMap& ret_map)
{
  // caller already checked is_inited_ and arguments
  int ret = OB_SUCCESS;
  const int64_t partition_cnt = partition_array.count();

  for (int idx = 0; (OB_SUCCESS == ret) && (idx < partition_cnt); idx++) {
    int64_t mc_timestamp = OB_INVALID_TIMESTAMP;
    ObMemberList member_list;
    ObProposalID proposal_id;
    const common::ObPartitionKey& partition_key = partition_array[idx];
    const int64_t quorum = quorum_array[idx];

    storage::ObIPartitionGroupGuard guard;
    ObIPartitionLogService* log_service = NULL;
    if ((OB_SUCCESS != (ret = partition_service_->get_partition(partition_key, guard))) ||
        NULL == guard.get_partition_group() || NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      ret = OB_PARTITION_NOT_EXIST;
      CLOG_LOG(WARN, "invalid partition", K(ret), K(partition_service_), K(partition_key));
    } else if (!(guard.get_partition_group()->is_valid())) {
      ret = OB_INVALID_PARTITION;
      CLOG_LOG(WARN, "partition is invalid", K(ret), K(partition_key));
    } else if (OB_SUCCESS !=
               (ret = log_service->pre_change_member(quorum, is_add_member, mc_timestamp, member_list, proposal_id))) {
      CLOG_LOG(WARN,
          "pre_change_member failed",
          K(ret),
          K(partition_key),
          K(quorum),
          K(is_add_member),
          K(mc_timestamp),
          K(member_list),
          K(proposal_id));
    } else {
      // do nothing;
    }

    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "have partition failed", K(partition_key), K(ret));
    } else if (OB_FAIL(mc_timestamp_array.push_back(mc_timestamp))) {
      CLOG_LOG(WARN, "mc_timestamp push_back failed", K(ret), K(partition_key), K(mc_timestamp));
    } else if (OB_FAIL(member_list_array.push_back(member_list))) {
      CLOG_LOG(WARN, "member_list_array push_back failed", K(ret), K(partition_key), K(member_list));
    } else if (OB_FAIL(proposal_id_array.push_back(proposal_id))) {
      CLOG_LOG(WARN, "proposal_id_array push_back faield", K(ret), K(partition_key), K(proposal_id_array));
    } else if (OB_FAIL(ret_map.insert(partition_key, ret))) {
      CLOG_LOG(WARN, "ret_map insert failed", K(ret), K(ret), K(partition_key));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObCLogMgr::batch_check_remote_mc_ts_(const common::ObPartitionArray& partition_array,
    const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
    const McTimestampArray& mc_timestamp_array, const MemberListArray& member_list_array, ReturnMap& ret_map)
{
  const uint64_t FOLLOWER_MAX_GAP = 1000;
  return internal_batch_check_remote_mc_ts_(
      partition_array, member_array, quorum_array, mc_timestamp_array, member_list_array, ret_map, FOLLOWER_MAX_GAP);
}

int ObCLogMgr::batch_set_member_changing_(const common::ObPartitionArray& partition_array,
    common::ObPartitionArray& need_reset_partition_array, ReturnMap& ret_map)
{
  int ret = OB_SUCCESS;
  const int64_t partition_cnt = partition_array.count();

  for (int idx = 0; (OB_SUCCESS == ret) && (idx < partition_cnt); idx++) {
    int tmp_ret = OB_SUCCESS;
    const common::ObPartitionKey& partition_key = partition_array[idx];

    storage::ObIPartitionGroupGuard guard;
    ObIPartitionLogService* log_service = NULL;
    if (!is_partition_success_(ret_map, partition_key)) {
      continue;
    } else if ((OB_SUCCESS != (tmp_ret = partition_service_->get_partition(partition_key, guard))) ||
               NULL == guard.get_partition_group() ||
               NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      tmp_ret = OB_PARTITION_NOT_EXIST;
      CLOG_LOG(WARN, "invalid partition", K(tmp_ret), K(partition_service_), K(partition_key));
    } else if (!(guard.get_partition_group()->is_valid())) {
      tmp_ret = OB_INVALID_PARTITION;
      CLOG_LOG(WARN, "partition is invalid", K(tmp_ret), K(partition_key));
    } else if (OB_SUCCESS != (tmp_ret = log_service->set_member_changing())) {
      CLOG_LOG(WARN, "set_member_changing failed", K(tmp_ret), K(partition_key));
    } else {
      // do nothing
    }

    if (OB_SUCCESS == tmp_ret) {
      if (OB_FAIL(need_reset_partition_array.push_back(partition_key))) {
        CLOG_LOG(WARN, "need_reset_partition_array push_back failed", K(ret), K(partition_key));
        if (OB_SUCCESS != (tmp_ret = log_service->reset_member_changing())) {
          CLOG_LOG(ERROR, "reset_member_changing failed", K(tmp_ret), K(partition_key));
        }
      }
    } else if (OB_FAIL(ret_map.insert_or_update(partition_key, tmp_ret))) {
      CLOG_LOG(WARN, "ret_map insert_or_update failed", K(ret), K(tmp_ret), K(partition_key));
    } else {
      // do nothing;
    }
  }

  return ret;
}

int ObCLogMgr::batch_wait_log_confirmed_(const common::ObPartitionArray& partition_array, ReturnMap& ret_map)
{
  int ret = OB_SUCCESS;
  const int64_t partition_cnt = partition_array.count();
  const int64_t MAX_WAIT_TIME = 400 * 1000;  // 400 ms, 200 ms for rpc, 200 ms for flush log
  const int64_t MIN_WAIT_TIME = 100 * 1000;  // 100 ms
  const int64_t begin_time = ObClockGenerator::getClock();

  for (int idx = 0; (OB_SUCCESS == ret) && (idx < partition_cnt); idx++) {
    int tmp_ret = OB_SUCCESS;
    const common::ObPartitionKey& partition_key = partition_array[idx];

    storage::ObIPartitionGroupGuard guard;
    ObIPartitionLogService* log_service = NULL;
    if (!is_partition_success_(ret_map, partition_key)) {
      continue;
    } else if ((OB_SUCCESS != (tmp_ret = partition_service_->get_partition(partition_key, guard))) ||
               NULL == guard.get_partition_group() ||
               NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      tmp_ret = OB_PARTITION_NOT_EXIST;
      CLOG_LOG(WARN, "invalid partition", K(tmp_ret), K(partition_service_), K(partition_key));
    } else if (!(guard.get_partition_group()->is_valid())) {
      tmp_ret = OB_INVALID_PARTITION;
      CLOG_LOG(WARN, "partition is invalid", K(tmp_ret), K(partition_key));
    } else if (OB_SUCCESS != (tmp_ret = log_service->wait_log_confirmed(begin_time, MAX_WAIT_TIME))) {
      CLOG_LOG(WARN, "wait_log_confirmed failed", K(tmp_ret), K(partition_key));
    } else {
      // do nothing
    }

    if (OB_SUCCESS != tmp_ret) {
      if (OB_FAIL(ret_map.insert_or_update(partition_key, tmp_ret))) {
        CLOG_LOG(WARN, "ret_map insert_or_update failed", K(ret), K(tmp_ret), K(partition_key));
      }
    }
  }
  const int64_t wait_time = ObClockGenerator::getClock() - begin_time;
  if (wait_time < MIN_WAIT_TIME && wait_time >= 0) {
    usleep(static_cast<useconds_t>(MIN_WAIT_TIME - wait_time));
  }

  return ret;
}

int ObCLogMgr::batch_check_remote_mc_ts_sync_(const common::ObPartitionArray& partition_array,
    const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
    const McTimestampArray& mc_timestamp_array, const MemberListArray& member_list_array, ReturnMap& ret_map)
{
  const uint64_t FOLLOWER_MAX_GAP = 0;
  return internal_batch_check_remote_mc_ts_(
      partition_array, member_array, quorum_array, mc_timestamp_array, member_list_array, ret_map, FOLLOWER_MAX_GAP);
}

int ObCLogMgr::internal_batch_check_remote_mc_ts_(const common::ObPartitionArray& partition_array,
    const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
    const McTimestampArray& mc_timestamp_array, const MemberListArray& member_list_array, ReturnMap& ret_map,
    const uint64_t follower_max_gap)
{
  int ret = OB_SUCCESS;

  ServerArray server_array;
  ServerPartitionMap server_partition_map;
  ServerMcCtxMap server_mc_ctx_map;
  PartitionInfoCtxMap partition_info_ctx_map;

  if (OB_FAIL(server_partition_map.init(ObModIds::OB_CLOG_MGR))) {
    CLOG_LOG(WARN, "server_partition_map init failed", K(ret));
  } else if (OB_FAIL(server_mc_ctx_map.init(ObModIds::OB_CLOG_MGR))) {
    CLOG_LOG(WARN, "server_mc_ctx_map init failed", K(ret));
  } else if (OB_FAIL(partition_info_ctx_map.init(ObModIds::OB_CLOG_MGR))) {
    CLOG_LOG(WARN, "partition_info_ctx_map failed", K(ret));
  } else if (OB_FAIL(construct_server_array_(member_list_array, member_array, server_array))) {
    CLOG_LOG(WARN, "construct_server_array_ failed", K(ret));
  } else if (OB_FAIL(construct_partition_info_ctx_map_(
                 partition_array, quorum_array, mc_timestamp_array, member_list_array, partition_info_ctx_map))) {
    CLOG_LOG(WARN, "construct_partition_info_ctx_map_ failed", K(ret));
  } else if (OB_FAIL(construct_server_partition_map_(
                 server_array, partition_array, member_array, member_list_array, server_partition_map))) {
    CLOG_LOG(WARN, "construct_server_partition_map_ failed", K(ret));
  } else if (OB_FAIL(construct_server_mc_ctx_map_(server_array, server_partition_map, server_mc_ctx_map))) {
    CLOG_LOG(WARN, "construct_server_mc_ctx_map_ failed", K(ret));
  } else if (OB_FAIL(check_partition_remote_mc_ts_(partition_array,
                 server_array,
                 partition_info_ctx_map,
                 server_mc_ctx_map,
                 follower_max_gap,
                 ret_map))) {
    CLOG_LOG(WARN, "check_partition_remote_mc_ts_ failed", K(ret));
  }

  return ret;
}

int ObCLogMgr::batch_execute_change_member_(const common::ObPartitionArray& partition_array,
    const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
    const McTimestampArray& mc_timestamp_array, const ProposalIDArray& proposal_id_array, const bool is_add_member,
    ObMCLogInfoArray& log_info_array, ReturnMap& ret_map)
{
  int ret = OB_SUCCESS;
  const int64_t partition_cnt = partition_array.count();

  for (int idx = 0; (OB_SUCCESS == ret) && (idx < partition_cnt); idx++) {
    int tmp_ret = OB_SUCCESS;
    const common::ObPartitionKey& partition_key = partition_array[idx];
    const ObMember& member = member_array[idx];
    const int64_t quorum = quorum_array[idx];
    const int64_t mc_timestamp = mc_timestamp_array[idx];
    const ObProposalID& proposal_id = proposal_id_array[idx];
    ObMCLogInfo log_info;

    storage::ObIPartitionGroupGuard guard;
    ObIPartitionLogService* log_service = NULL;
    if (!is_partition_success_(ret_map, partition_key)) {
      if (OB_FAIL(log_info_array.push_back(log_info))) {
        CLOG_LOG(WARN, "log_info_array push_back failed", K(ret), K(partition_key));
      }
      continue;
    } else if ((OB_SUCCESS != (tmp_ret = partition_service_->get_partition(partition_key, guard))) ||
               NULL == guard.get_partition_group() ||
               NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      tmp_ret = OB_PARTITION_NOT_EXIST;
      CLOG_LOG(WARN, "invalid partition", K(tmp_ret), K(partition_service_), K(partition_key));
    } else if (!(guard.get_partition_group()->is_valid())) {
      tmp_ret = OB_INVALID_PARTITION;
      CLOG_LOG(WARN, "partition is invalid", K(tmp_ret), K(partition_key));
    } else if (OB_SUCCESS != (tmp_ret = log_service->batch_change_member(
                                  member, quorum, mc_timestamp, proposal_id, is_add_member, log_info))) {
      CLOG_LOG(WARN,
          "batch_change_member failed",
          K(tmp_ret),
          K(partition_key),
          K(member),
          K(mc_timestamp),
          K(proposal_id),
          K(is_add_member));
    } else {
      // do nothing
    }

    if (OB_SUCCESS != tmp_ret) {
      if (OB_FAIL(ret_map.insert_or_update(partition_key, tmp_ret))) {
        CLOG_LOG(WARN, "ret_map insert_or_update failed", K(ret), K(tmp_ret), K(partition_key));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(log_info_array.push_back(log_info))) {
        CLOG_LOG(WARN, "log_info_array push_back failed", K(ret), K(partition_key));
      }
    }
  }

  return ret;
}

int ObCLogMgr::batch_block_partition_split_(const common::ObPartitionArray& partition_array, ReturnMap& ret_map)
{
  int ret = OB_SUCCESS;
  const int64_t partition_cnt = partition_array.count();

  for (int idx = 0; (OB_SUCCESS == ret) && (idx < partition_cnt); idx++) {
    int tmp_ret = OB_SUCCESS;
    const common::ObPartitionKey& partition_key = partition_array[idx];

    storage::ObIPartitionGroupGuard guard;
    if (!is_partition_success_(ret_map, partition_key)) {
      continue;
    } else if ((OB_SUCCESS != (tmp_ret = partition_service_->get_partition(partition_key, guard))) ||
               NULL == guard.get_partition_group()) {
      tmp_ret = OB_PARTITION_NOT_EXIST;
      CLOG_LOG(WARN, "invalid partition", K(tmp_ret), K(partition_service_), K(partition_key));
    } else if (!(guard.get_partition_group()->is_valid())) {
      tmp_ret = OB_INVALID_PARTITION;
      CLOG_LOG(WARN, "partition is invalid", K(tmp_ret), K(partition_key));
    } else if (OB_SUCCESS != (tmp_ret = guard.get_partition_group()->block_partition_split_by_mc())) {
      CLOG_LOG(WARN, "block partiton split failed", K(tmp_ret), K(partition_key));
    } else {
      // do nothing
    }

    if (OB_FAIL(ret_map.insert_or_update(partition_key, tmp_ret))) {
      CLOG_LOG(WARN, "ret_map insert_or_update failed", K(ret), K(tmp_ret), K(partition_key));
    }
  }

  return ret;
}

int ObCLogMgr::batch_unblock_partition_split_(const common::ObPartitionArray& partition_array, ReturnMap& ret_map)
{
  int ret = OB_SUCCESS;
  const int64_t partition_cnt = partition_array.count();

  for (int idx = 0; (OB_SUCCESS == ret) && (idx < partition_cnt); idx++) {
    int ret_status = OB_SUCCESS;
    const common::ObPartitionKey& partition_key = partition_array[idx];
    storage::ObIPartitionGroupGuard guard;
    if (OB_FAIL(ret_map.get(partition_key, ret_status))) {
      CLOG_LOG(ERROR, "ret_map get failed", K(ret), K(partition_key));
    } else if (OB_SUCCESS == ret_status || OB_PARTITION_IS_SPLITTING == ret_status) {
      // do nothing
    } else if (OB_FAIL(partition_service_->get_partition(partition_key, guard))) {
      CLOG_LOG(WARN, "get partition failed", K(ret), K(partition_key));
    } else if (NULL == guard.get_partition_group()) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "partition is null", K(ret), K(partition_key));
    } else if (OB_FAIL(guard.get_partition_group()->unblock_partition_split_by_mc())) {
      CLOG_LOG(WARN, "unblock partition split failed", K(ret), K(partition_key), K(ret_status));
    } else {
      CLOG_LOG(INFO, "unblock partition split success", K(partition_key), K(ret_status));
    }
    // return ret
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObCLogMgr::batch_reset_member_changing_(const common::ObPartitionArray& need_reset_partition_array)
{
  int ret = OB_SUCCESS;
  const int64_t partition_cnt = need_reset_partition_array.count();

  bool have_unexpected_error = false;
  for (int idx = 0; (OB_SUCCESS == ret) && (idx < partition_cnt); idx++) {
    int tmp_ret = OB_SUCCESS;
    const common::ObPartitionKey& partition_key = need_reset_partition_array[idx];

    storage::ObIPartitionGroupGuard guard;
    ObIPartitionLogService* log_service = NULL;
    if ((OB_SUCCESS != (tmp_ret = partition_service_->get_partition(partition_key, guard))) ||
        NULL == guard.get_partition_group() || NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      tmp_ret = OB_PARTITION_NOT_EXIST;
      have_unexpected_error = true;
      CLOG_LOG(ERROR, "invalid partition", K(tmp_ret), K(partition_service_), K(partition_key));
    } else if (!(guard.get_partition_group()->is_valid())) {
      tmp_ret = OB_INVALID_PARTITION;
      have_unexpected_error = true;
      CLOG_LOG(ERROR, "partition is invalid", K(tmp_ret), K(partition_key));
    } else if (OB_SUCCESS != (tmp_ret = log_service->reset_member_changing())) {
      have_unexpected_error = true;
      CLOG_LOG(ERROR, "reset_member_changing failed", K(tmp_ret), K(partition_key));
    } else {
      // do nothing
    }
  }

  if (have_unexpected_error) {
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

int ObCLogMgr::rewrite_ret_value_(
    const common::ObPartitionArray& partition_array, const ReturnMap& ret_map, common::ObReturnArray& ret_array)
{
  int ret = OB_SUCCESS;
  const int64_t partition_cnt = partition_array.count();
  bool have_failed_partition = false;

  for (int idx = 0; (OB_SUCCESS == ret) && (idx < partition_cnt); idx++) {
    int tmp_ret = OB_SUCCESS;
    const common::ObPartitionKey& partition_key = partition_array[idx];

    if (OB_FAIL(ret_map.get(partition_key, tmp_ret))) {
      CLOG_LOG(ERROR, "ret_map get failed", K(ret), K(partition_key));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ret_array.push_back(tmp_ret))) {
      CLOG_LOG(WARN, "ret_array push_back failed", K(ret), K(partition_key), K(tmp_ret));
    } else if (OB_SUCCESS != tmp_ret) {
      have_failed_partition = true;
    } else {
      // do nothing
    }
  }

  if (OB_SUCC(ret) && have_failed_partition) {
    ret = OB_PARTIAL_FAILED;
  }

  return ret;
}

int ObCLogMgr::rewrite_ret_value_(const common::ObReturnArray& ret_array)
{
  int ret = OB_SUCCESS;
  const int64_t partition_cnt = ret_array.count();
  bool have_failed_partition = false;

  for (int idx = 0; (OB_SUCCESS == ret) && (idx < partition_cnt); idx++) {
    const int tmp_ret = ret_array[idx];
    if (OB_SUCCESS != tmp_ret) {
      have_failed_partition = true;
    }
  }

  if (OB_SUCC(ret) && have_failed_partition) {
    ret = OB_PARTIAL_FAILED;
  }

  return ret;
}

bool ObCLogMgr::is_partition_success_(const ReturnMap& ret_map, const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_FAIL(ret_map.get(partition_key, tmp_ret))) {
    // partition_key must exist in hash_map
    CLOG_LOG(ERROR, "ret_map get failed", K(ret), K(partition_key));
    tmp_ret = OB_ERR_UNEXPECTED;
  }

  return OB_SUCCESS == tmp_ret;
}

int ObCLogMgr::batch_add_member(const obrpc::ObChangeMemberArgs& ctx_array, obrpc::ObChangeMemberCtxs& return_ctx_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool has_partial_failed = false;

  const int64_t total_partition_cnt = ctx_array.count();
  const int64_t MAX_BATCH_CNT = 2000;  // 200 ms for handle, it's enough
  int64_t curr_idx = 0;

  while (curr_idx < total_partition_cnt && OB_SUCC(ret)) {
    const int64_t idx_upper_limit = std::min(total_partition_cnt, curr_idx + MAX_BATCH_CNT);
    const int64_t switch_epoch = ctx_array[curr_idx].switch_epoch_;
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2240 && OB_INVALID_VERSION != switch_epoch &&
        switch_epoch != GCTX.get_switch_epoch2()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "switch_epoch not match", K(ret));
    }

    common::ObPartitionArray partition_array;
    common::ObMemberArray member_array;
    common::ObQuorumArray quorum_array;
    common::ObReturnArray ret_array;
    ObMCLogInfoArray log_info_array;

    for (int64_t idx = curr_idx; (OB_SUCCESS == ret) && (idx < idx_upper_limit); idx++) {
      if (OB_FAIL(partition_array.push_back(ctx_array[idx].partition_key_))) {
        CLOG_LOG(WARN, "partition_array push_back failed", K(ret));
      } else if (OB_FAIL(member_array.push_back(ctx_array[idx].member_))) {
        CLOG_LOG(WARN, "member_array push_back failed", K(ret));
      } else if (OB_FAIL(quorum_array.push_back(ctx_array[idx].quorum_))) {
        CLOG_LOG(WARN, "quorum_array push_back failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t partition_cnt = partition_array.count();
      ret = batch_add_member(partition_array, member_array, quorum_array, ret_array, log_info_array);
      if (OB_SUCCESS == ret || OB_PARTIAL_FAILED == ret) {
        if (ret_array.count() != partition_cnt || log_info_array.count() != partition_cnt) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "partition cnt not match", K(partition_cnt), K(ret_array.count()), K(log_info_array.count()));
        } else {
          for (int64_t idx = 0; (tmp_ret == OB_SUCCESS) && (idx < idx_upper_limit - curr_idx); idx++) {
            obrpc::ObChangeMemberCtx return_ctx;
            return_ctx.partition_key_ = partition_array[idx];
            return_ctx.ret_value_ = ret_array[idx];
            return_ctx.log_info_ = log_info_array[idx];
            if (OB_SUCCESS != (tmp_ret = return_ctx_array.push_back(return_ctx))) {
              CLOG_LOG(WARN, "return_ctx_array push_back failed", K(ret));
            }
          }
          if (OB_SUCCESS != tmp_ret) {
            ret = tmp_ret;
          }
        }
      }
    }
    curr_idx = idx_upper_limit;
    if (OB_PARTIAL_FAILED == ret) {
      ret = OB_SUCCESS;
      has_partial_failed = true;
    }
  }

  if (has_partial_failed && OB_SUCC(ret)) {
    ret = OB_PARTIAL_FAILED;
  }

  return ret;
}

int ObCLogMgr::batch_remove_member(
    const obrpc::ObChangeMemberArgs& ctx_array, obrpc::ObChangeMemberCtxs& return_ctx_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool has_partial_failed = false;

  const int64_t total_partition_cnt = ctx_array.count();
  const int64_t MAX_BATCH_CNT = 2000;  // 200 ms for handle, it's enough
  int64_t curr_idx = 0;

  while (curr_idx < total_partition_cnt && OB_SUCC(ret)) {
    const int64_t idx_upper_limit = std::min(total_partition_cnt, curr_idx + MAX_BATCH_CNT);
    const int64_t switch_epoch = ctx_array[curr_idx].switch_epoch_;
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2240 && OB_INVALID_VERSION != switch_epoch &&
        switch_epoch != GCTX.get_switch_epoch2()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "switch_epoch not match", K(ret));
    }

    common::ObPartitionArray partition_array;
    common::ObMemberArray member_array;
    common::ObQuorumArray quorum_array;
    common::ObReturnArray ret_array;
    ObMCLogInfoArray log_info_array;

    for (int64_t idx = curr_idx; (OB_SUCCESS == ret) && (idx < idx_upper_limit); idx++) {
      if (OB_FAIL(partition_array.push_back(ctx_array[idx].partition_key_))) {
        CLOG_LOG(WARN, "partition_array push_back failed", K(ret));
      } else if (OB_FAIL(member_array.push_back(ctx_array[idx].member_))) {
        CLOG_LOG(WARN, "member_array push_back failed", K(ret));
      } else if (OB_FAIL(quorum_array.push_back(ctx_array[idx].quorum_))) {
        CLOG_LOG(WARN, "quorum_array push_back failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t partition_cnt = partition_array.count();
      ret = batch_remove_member(partition_array, member_array, quorum_array, ret_array, log_info_array);
      if (OB_SUCCESS == ret || OB_PARTIAL_FAILED == ret) {
        if (ret_array.count() != partition_cnt || log_info_array.count() != partition_cnt) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "partition cnt not match", K(partition_cnt), K(ret_array.count()), K(log_info_array.count()));
        } else {
          for (int64_t idx = 0; (tmp_ret == OB_SUCCESS) && (idx < idx_upper_limit - curr_idx); idx++) {
            obrpc::ObChangeMemberCtx return_ctx;
            return_ctx.partition_key_ = partition_array[idx];
            return_ctx.ret_value_ = ret_array[idx];
            return_ctx.log_info_ = log_info_array[idx];
            if (OB_SUCCESS != (tmp_ret = return_ctx_array.push_back(return_ctx))) {
              CLOG_LOG(WARN, "return_ctx_array push_back failed", K(ret));
            }
          }
          if (OB_SUCCESS != tmp_ret) {
            ret = tmp_ret;
          }
        }
      }
    }
    curr_idx = idx_upper_limit;
    if (OB_PARTIAL_FAILED == ret) {
      ret = OB_SUCCESS;
      has_partial_failed = true;
    }
  }

  if (has_partial_failed && OB_SUCC(ret)) {
    ret = OB_PARTIAL_FAILED;
  }

  return ret;
}

int ObCLogMgr::batch_is_member_change_done(obrpc::ObChangeMemberCtxs& return_ctx_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  common::ObPartitionArray partition_array;
  common::ObReturnArray ret_array;
  ObMCLogInfoArray log_info_array;

  const int64_t partition_cnt = return_ctx_array.count();
  for (int idx = 0; (OB_SUCCESS == ret) && (idx < partition_cnt); idx++) {
    if (OB_FAIL(partition_array.push_back(return_ctx_array[idx].partition_key_))) {
      CLOG_LOG(WARN, "partition_array push_back failed", K(ret));
    } else if (OB_FAIL(log_info_array.push_back(return_ctx_array[idx].log_info_))) {
      CLOG_LOG(WARN, "log_info_array push_back failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ret = batch_is_member_change_done(partition_array, log_info_array, ret_array);
    if (OB_SUCCESS == ret || OB_PARTIAL_FAILED == ret) {
      if (ret_array.count() != partition_cnt) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "partition cnt not match", K(partition_cnt), K(ret_array.count()));
      } else {
        return_ctx_array.reset();
        for (int idx = 0; (tmp_ret == OB_SUCCESS) && (idx < partition_cnt); idx++) {
          obrpc::ObChangeMemberCtx return_ctx;
          return_ctx.partition_key_ = partition_array[idx];
          return_ctx.ret_value_ = ret_array[idx];
          return_ctx.log_info_ = log_info_array[idx];
          if (OB_SUCCESS != (tmp_ret = return_ctx_array.push_back(return_ctx))) {
            CLOG_LOG(WARN, "return_ctx_array push_back failed", K(ret));
          }
        }
        if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
        }
      }
    }
  }
  return ret;
}

int ObCLogMgr::construct_server_array_(
    const MemberListArray& member_list_array, const ObMemberArray& member_array, ServerArray& server_array)
{
  int ret = OB_SUCCESS;
  const int64_t member_list_cnt = member_list_array.count();
  const int64_t member_cnt = member_array.count();
  if (member_list_cnt != member_cnt) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(member_list_cnt), K(member_cnt));
  } else {
    for (int64_t member_list_idx = 0; (OB_SUCCESS == ret) && (member_list_idx < member_list_cnt); member_list_idx++) {
      const common::ObMemberList member_list = member_list_array[member_list_idx];
      const common::ObAddr change_addr = member_array[member_list_idx].get_server();
      const int64_t member_list_number = member_list.get_member_number();
      for (int64_t member_idx = 0; (OB_SUCCESS == ret) && (member_idx < member_list_number); member_idx++) {
        ObAddr server;
        if (OB_SUCCESS != (ret = member_list.get_server_by_index(member_idx, server))) {
          CLOG_LOG(WARN, "get_server_by_index failed", K(ret));
        } else if ((server != self_addr_) && (server != change_addr) && (!is_server_contains_(server_array, server)) &&
                   (OB_FAIL(server_array.push_back(server)))) {
          CLOG_LOG(WARN, "server_array push_back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObCLogMgr::is_server_contains_(const ServerArray& server_array, const common::ObAddr& addr)
{
  bool bool_ret = false;
  const int64_t cnt = server_array.count();
  for (int64_t i = 0; i < cnt; i++) {
    if (addr == server_array[i]) {
      bool_ret = true;
      break;
    }
  }
  return bool_ret;
}

int ObCLogMgr::construct_partition_info_ctx_map_(const common::ObPartitionArray& partition_array,
    const common::ObQuorumArray& quorum_array, const McTimestampArray& mc_timestamp_array,
    const MemberListArray& member_list_array, PartitionInfoCtxMap& partition_info_ctx_map)
{
  int ret = OB_SUCCESS;
  const int64_t partition_cnt = partition_array.count();
  const int64_t quorum_cnt = quorum_array.count();
  const int64_t mc_timestamp_cnt = mc_timestamp_array.count();
  const int64_t member_list_cnt = member_list_array.count();

  if (partition_cnt != quorum_cnt || partition_cnt != mc_timestamp_cnt || partition_cnt != member_list_cnt) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(
        WARN, "invalid arguments", K(ret), K(partition_cnt), K(quorum_cnt), K(mc_timestamp_cnt), K(member_list_cnt));
  } else {
    for (int64_t idx = 0; (OB_SUCCESS == ret) && (idx < partition_cnt); idx++) {
      const common::ObPartitionKey partition_key = partition_array[idx];
      const common::ObMemberList member_list = member_list_array[idx];
      const int64_t quorum = quorum_array[idx];
      const int64_t mc_timestamp = mc_timestamp_array[idx];

      PartitionInfoCtx ctx;
      ctx.member_list_ = member_list;
      ctx.quorum_ = quorum;
      ctx.mc_timestamp_ = mc_timestamp;
      ctx.sync_num_ = 0;

      if (OB_FAIL(partition_info_ctx_map.insert(partition_key, ctx))) {
        CLOG_LOG(WARN, "partition_info_ctx_map insert failed", K(ret));
      }
    }
  }
  return ret;
}

int ObCLogMgr::construct_server_partition_map_(const ServerArray& server_array,
    const common::ObPartitionArray& partition_array, const common::ObMemberArray& member_array,
    const MemberListArray& member_list_array, ServerPartitionMap& server_partition_map)
{
  int ret = OB_SUCCESS;
  const int64_t server_cnt = server_array.count();
  const int64_t partition_cnt = partition_array.count();
  const int64_t member_cnt = member_array.count();
  const int64_t member_list_cnt = member_list_array.count();

  if (partition_cnt != member_cnt || partition_cnt != member_list_cnt) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_cnt), K(member_cnt), K(member_list_cnt));
  } else {
    for (int64_t server_idx = 0; (OB_SUCCESS == ret) && (server_idx < server_cnt); server_idx++) {
      const ObAddr server = server_array[server_idx];
      common::ObPartitionArray tmp_array;
      for (int64_t partition_idx = 0; (OB_SUCCESS == ret) && (partition_idx < partition_cnt); partition_idx++) {
        const common::ObPartitionKey partition_key = partition_array[partition_idx];
        const common::ObMemberList member_list = member_list_array[partition_idx];
        const common::ObAddr change_member = member_array[partition_idx].get_server();
        if (member_list.contains(server) && change_member != server) {
          if (OB_FAIL(tmp_array.push_back(partition_key))) {
            CLOG_LOG(WARN, "tmp_array push_back failed", K(ret));
          }
        }
      }
      if (OB_SUCCESS == ret) {
        if (OB_FAIL(server_partition_map.insert(server, tmp_array))) {
          CLOG_LOG(WARN, "server_partition_map insert failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObCLogMgr::get_candidates_array(const common::ObPartitionIArray& pkey_array,
    const common::ObAddrIArray& dst_server_list, common::ObSArray<common::ObAddrSArray>& candidate_list_array,
    common::ObSArray<CandidateStatusList>& candidate_status_array) const
{
  // if self is not partition leader, return OB_NOT_MASTER
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t pkey_cnt = pkey_array.count();

  if (pkey_array.count() <= 0 || dst_server_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(pkey_array), K(dst_server_list));
  } else {
    candidate_list_array.reset();
    // firstly initialize output candidate_list_array, and insert server_list with self into it
    for (int64_t pkey_idx = 0; OB_SUCC(ret) && (pkey_idx < pkey_cnt); ++pkey_idx) {
      common::ObAddrSArray tmp_server_list;
      CandidateStatusList tmp_cand_status_list;
      CandidateStatus tmp_candidate_status;
      tmp_server_list.reset();
      tmp_candidate_status.set_in_black_list(false /*not in blacklist*/);
      if (OB_FAIL(tmp_server_list.push_back(self_addr_))) {
        CLOG_LOG(WARN, "tmp_server_list push_back failed", K(ret));
      } else if (OB_FAIL(tmp_cand_status_list.push_back(tmp_candidate_status))) {
        CLOG_LOG(WARN, "fail to push back", K(ret));
      } else if (OB_FAIL(candidate_list_array.push_back(tmp_server_list))) {
        CLOG_LOG(WARN, "candidate_list_array push_back failed", K(ret));
      } else if (OB_FAIL(candidate_status_array.push_back(tmp_cand_status_list))) {
        CLOG_LOG(WARN, "candidate status array push back failed", K(ret));
      }
    }
    // Get the priority list of each partition on the machine
    election::PriorityArray self_priority_array;
    common::ObRole role = common::INVALID_ROLE;
    for (int64_t pkey_idx = 0; OB_SUCC(ret) && (pkey_idx < pkey_cnt); ++pkey_idx) {
      const ObPartitionKey cur_pkey = pkey_array.at(pkey_idx);
      election::ObElectionPriority self_priority;
      self_priority.reset();
      // firstly check if self is leader, and get priority of self
      ObIPartitionGroupGuard guard;
      ObIPartitionLogService* log_service = NULL;
      if (OB_SUCCESS != (tmp_ret = partition_service_->get_partition(cur_pkey, guard)) ||
          NULL == guard.get_partition_group() ||
          NULL == (log_service = guard.get_partition_group()->get_log_service())) {
        tmp_ret = OB_ENTRY_NOT_EXIST;
        CLOG_LOG(WARN, "get_log_service failed", K(tmp_ret), K(cur_pkey));
      } else if (!(guard.get_partition_group()->is_valid())) {
        tmp_ret = OB_INVALID_PARTITION;
        CLOG_LOG(WARN, "partition is invalid", K(cur_pkey), K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = log_service->get_role(role)) || !is_leader_by_election(role)) {
        // self is not leader, fail
        ret = tmp_ret;
        CLOG_LOG(WARN, "self is not election leader", K(tmp_ret), K(cur_pkey), K(role));
      } else if (OB_SUCCESS != (tmp_ret = log_service->on_get_election_priority(self_priority))) {
        CLOG_LOG(WARN, "on_get_election_priority failed", K(tmp_ret), K(cur_pkey));
      } else if (!self_priority.is_valid()) {
        CLOG_LOG(WARN, "self priority is invalid", K(cur_pkey), K(self_priority));
      }
      // If failure occurs when obtain priority, insert an invalid priority, and make sure that no candidates are
      // returned for the partition.
      if (OB_SUCC(ret)) {
        if (OB_FAIL(self_priority_array.push_back(self_priority))) {
          CLOG_LOG(WARN, "array push_back failed", K(ret));
        }
      }
    }
    // get each partition's priority in each server
    const int64_t server_cnt = dst_server_list.count();
    for (int64_t server_idx = 0; OB_SUCC(ret) && (server_idx < server_cnt); ++server_idx) {
      ObAddr cur_server = dst_server_list.at(server_idx);
      election::PriorityArray cur_svr_priority_array;
      if (self_addr_ == cur_server) {
        // skip self
      } else if (OB_SUCCESS !=
                 (tmp_ret = log_engine_.get_remote_priority_array(cur_server, pkey_array, cur_svr_priority_array))) {
        // When rpc fails, it is considered that the destination is not a candidate
        CLOG_LOG(WARN, "get_remote_priority_array failed", K(tmp_ret));
      } else if (cur_svr_priority_array.count() != pkey_cnt) {
        CLOG_LOG(WARN,
            "array size not match",
            K(cur_server),
            K(pkey_cnt),
            "cur_svr_priority_array cnt",
            cur_svr_priority_array.count());
      } else {
        // Process the priority_array returned by the server
        for (int64_t pkey_idx = 0; OB_SUCC(ret) && (pkey_idx < pkey_cnt); ++pkey_idx) {
          const ObPartitionKey cur_pkey = pkey_array.at(pkey_idx);
          election::ObElectionPriority cur_self_priority = self_priority_array[pkey_idx];
          bool is_in_member_list = false;
          CandidateStatus tmp_candidate_status;
          tmp_candidate_status.set_in_black_list(cur_svr_priority_array[pkey_idx].is_in_election_blacklist());
          // collect valid_candidates
          if (!cur_self_priority.is_valid()) {
            // The priority of local machine is invalid, indicating that self is not the leader or failed to obtain
            // priority, skip
          } else if (!cur_svr_priority_array[pkey_idx].is_valid()) {
            // the priority of dest replica is invalid, skip
            CLOG_LOG(WARN, "dst_server priority is invalid", K(cur_pkey), K(cur_server));
          } else if (!cur_svr_priority_array[pkey_idx].is_candidate()) {
            // dest replica's is_candidate is false, skip
            CLOG_LOG(WARN,
                "dst_server priority is not candidate",
                K(cur_pkey),
                K(cur_server),
                "priority",
                cur_svr_priority_array[pkey_idx]);
          } else if (OB_SUCCESS != (tmp_ret = check_svr_in_member_list_(cur_pkey, cur_server, is_in_member_list))) {
            CLOG_LOG(WARN, "check_svr_in_member_list_ failed", K(tmp_ret));
          } else if (!is_in_member_list) {
            // dest replica is not in current member_list, skip
            CLOG_LOG(WARN, "dst_server not in member_list", K(cur_pkey), K(cur_server));
          } else if (cur_svr_priority_array[pkey_idx].compare_with_buffered_logid(cur_self_priority) < 0) {
            // skip servers whose priority is less than self
            CLOG_LOG(WARN,
                "dst_server priority is less than self",
                K(cur_pkey),
                K(cur_server),
                "priority",
                cur_svr_priority_array[pkey_idx],
                K(cur_self_priority));
          } else if (OB_FAIL(candidate_list_array.at(pkey_idx).push_back(cur_server))) {
            CLOG_LOG(WARN, "array push_back failed", K(ret));
          } else if (OB_FAIL(candidate_status_array.at(pkey_idx).push_back(tmp_candidate_status))) {
            CLOG_LOG(WARN, "array push back failed", K(ret));
          } else {
            // do nothing
          }
        }
      }
    }
  }

  return ret;
}

int ObCLogMgr::check_svr_in_member_list_(
    const ObPartitionKey& pkey, const ObAddr& server, bool& is_in_member_list) const
{
  int ret = OB_SUCCESS;
  is_in_member_list = false;
  ObIPartitionGroupGuard guard;
  ObIPartitionLogService* pls = NULL;
  if (!pkey.is_valid() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(partition_service_->get_partition(pkey, guard)) || NULL == guard.get_partition_group() ||
             NULL == (pls = guard.get_partition_group()->get_log_service())) {
    ret = OB_ENTRY_NOT_EXIST;
    CLOG_LOG(WARN, "get_log_service failed", K(ret), K(pkey));
  } else {
    is_in_member_list = pls->is_svr_in_member_list(server);
  }

  return ret;
}

int ObCLogMgr::construct_server_mc_ctx_map_(
    const ServerArray& server_array, const ServerPartitionMap& server_partition_map, ServerMcCtxMap& server_mc_ctx_map)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t server_cnt = server_array.count();

  for (int64_t server_idx = 0; (OB_SUCCESS == ret) && (server_idx < server_cnt); server_idx++) {
    const ObAddr server = server_array[server_idx];
    common::ObPartitionArray partition_array;
    McCtxArray mc_ctx_array;
    if (OB_FAIL(server_partition_map.get(server, partition_array))) {
      CLOG_LOG(WARN, "server_partition_map get failed", K(ret));
    } else if (OB_SUCCESS != (tmp_ret = log_engine_.get_remote_mc_ctx_array(server, partition_array, mc_ctx_array))) {
      CLOG_LOG(WARN, "get_remote_mc_ctx failed", K(tmp_ret));
    } else if (OB_FAIL(server_mc_ctx_map.insert(server, mc_ctx_array))) {
      CLOG_LOG(WARN, "server_mc_ctx_map insert failed", K(ret), K(server));
    }
  }

  return ret;
}

int ObCLogMgr::check_partition_remote_mc_ts_(const ObPartitionArray& partition_array, const ServerArray& server_array,
    PartitionInfoCtxMap& partition_info_ctx_map, const ServerMcCtxMap& server_mc_ctx_map,
    const int64_t follower_max_gap, ReturnMap& ret_map)
{
  int ret = OB_SUCCESS;
  const int64_t server_cnt = server_array.count();
  const int64_t partition_cnt = partition_array.count();

  for (int64_t server_idx = 0; (OB_SUCCESS == ret) && (server_idx < server_cnt); server_idx++) {
    int tmp_ret = OB_SUCCESS;
    const ObAddr server = server_array[server_idx];
    McCtxArray mc_ctx_array;
    if (OB_SUCCESS != (tmp_ret = server_mc_ctx_map.get(server, mc_ctx_array))) {
      CLOG_LOG(WARN, "server_mc_ctx_map get failed", K(ret));
    } else {
      const int64_t mc_ctx_array_cnt = mc_ctx_array.count();
      for (int64_t mc_ctx_array_idx = 0; (OB_SUCCESS == ret) && (mc_ctx_array_idx < mc_ctx_array_cnt);
           mc_ctx_array_idx++) {
        McCtx mc_ctx = mc_ctx_array[mc_ctx_array_idx];
        const ObPartitionKey partition_key = mc_ctx.partition_key_;
        bool remote_replica_is_normal = mc_ctx.is_normal_partition_;
        if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2275) {
          remote_replica_is_normal = true;
        }

        PartitionInfoCtx partition_info_ctx;
        uint64_t partition_max_log_id = OB_INVALID_ID;
        if (OB_SUCCESS != (tmp_ret = get_partition_max_log_id_(partition_key, partition_max_log_id))) {
          CLOG_LOG(WARN, "get_partition_max_log_id failed", K(tmp_ret), K(partition_key));
        } else if (OB_FAIL(partition_info_ctx_map.get(partition_key, partition_info_ctx))) {
          CLOG_LOG(WARN, "partition_info_ctx_map get failed", K(ret));
        } else {
          if (mc_ctx.mc_timestamp_ == partition_info_ctx.mc_timestamp_ &&
              partition_max_log_id <= follower_max_gap + mc_ctx.max_confirmed_log_id_ &&
              true == remote_replica_is_normal) {
            partition_info_ctx.sync_num_++;
            if (OB_FAIL(partition_info_ctx_map.insert_or_update(partition_key, partition_info_ctx))) {
              CLOG_LOG(WARN, "partition_info_ctx_map insert_or_update failed", K(ret));
            }
          }
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    for (int64_t partition_idx = 0; (OB_SUCCESS == ret) && (partition_idx < partition_cnt); partition_idx++) {
      const ObPartitionKey partition_key = partition_array[partition_idx];
      PartitionInfoCtx partition_info_ctx;
      if (!is_partition_success_(ret_map, partition_key)) {
        continue;
      } else if (OB_FAIL(partition_info_ctx_map.get(partition_key, partition_info_ctx))) {
        CLOG_LOG(WARN, "partition_info_ctx_map get failed", K(ret), K(partition_key));
      } else if (partition_info_ctx.sync_num_ < partition_info_ctx.quorum_ / 2) {
        int tmp_ret = OB_LOG_NOT_SYNC;
        if (OB_FAIL(ret_map.insert_or_update(partition_key, tmp_ret))) {
          CLOG_LOG(WARN, "ret_map insert_or_update failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObCLogMgr::get_partition_max_log_id_(const common::ObPartitionKey& partition_key, uint64_t& partition_max_log_id)
{
  int ret = OB_SUCCESS;

  if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key));
  } else {
    storage::ObIPartitionGroupGuard guard;
    ObIPartitionLogService* log_service = NULL;

    if ((OB_SUCCESS != (ret = partition_service_->get_partition(partition_key, guard))) ||
        NULL == guard.get_partition_group() || NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      ret = OB_PARTITION_NOT_EXIST;
      CLOG_LOG(WARN, "invalid partition", K(ret), K(partition_service_), K(partition_key));
    } else if (!(guard.get_partition_group()->is_valid())) {
      ret = OB_INVALID_PARTITION;
      CLOG_LOG(WARN, "partition is invalid", K(ret), K(partition_key));
    } else if (OB_SUCCESS != (ret = log_service->get_partition_max_log_id(partition_max_log_id))) {
      CLOG_LOG(WARN, "get_partition_max_log_id failed", K(ret), K(partition_key));
    } else {
      // do nothing
    }
  }
  return ret;
}

//======================= batch change member end =========================
int ObCLogMgr::get_need_freeze_partition_array(NeedFreezePartitionArray& partition_array) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "is not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(log_engine_.get_need_freeze_partition_array(partition_array))) {
    CLOG_LOG(WARN, "get_need_freeze_partition_array failed", K(ret));
  }
  return ret;
}

uint32_t ObCLogMgr::get_clog_min_using_file_id() const
{
  uint32_t ret_value = 0;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "ObCLogMgr is not inited");
  } else {
    ret_value = log_engine_.get_clog_min_using_file_id();
  }
  return ret_value;
}

int ObCLogMgr::get_clog_using_disk_space(int64_t& using_space) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr not init");
  } else {
    ret = log_engine_.get_clog_using_disk_space(using_space);
  }
  return ret;
}

int ObCLogMgr::get_ilog_using_disk_space(int64_t& using_space) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr not init");
  } else {
    ret = log_engine_.get_ilog_using_disk_space(using_space);
  }
  return ret;
}

int ObCheckpointLogReplicaTask::init(storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(partition_service)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    partition_service_ = partition_service;
    is_inited_ = true;
  }
  CLOG_LOG(INFO, "ObCheckpointLogReplicaTask is inited", K(ret));
  return ret;
}

void ObCheckpointLogReplicaTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    storage::ObIPartitionGroupIterator* partition_iter = NULL;
    if (NULL == (partition_iter = partition_service_->alloc_pg_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(ERROR, "partition_service alloc_scan_iter failed", K(ret));
    } else {
      storage::ObIPartitionGroup* partition = NULL;
      while (OB_SUCC(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_FAIL(partition_iter->get_next(partition)) || NULL == partition) {
          // do nothing;
        } else if (!partition->is_valid()) {
          // do nothing;
        } else if (REPLICA_TYPE_LOGONLY == partition->get_replica_type()) {
          const common::ObPartitionKey partition_key = partition->get_partition_key();
          if (OB_SUCCESS != (tmp_ret = partition_service_->minor_freeze(partition_key))) {
            CLOG_LOG(WARN, "minor freeze log replica failed", K(tmp_ret), K(partition_key));
          }
        } else {
          // do nothing
        }
      }
    }

    if (NULL != partition_iter) {
      partition_service_->revert_pg_iter(partition_iter);
      partition_iter = NULL;
    }
  }
  return;
}

int ObCheckCLogDiskFullTask::init(storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(partition_service)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    partition_service_ = partition_service;
    is_inited_ = true;
  }
  return ret;
}

void ObCheckCLogDiskFullTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_CLOG_DISK_MGR.is_disk_space_enough()) {
    // do nothing
  } else if (OB_FAIL(do_run_timer_task_())) {
    CLOG_LOG(WARN, "do_run_timer_task_ failed", K(ret));
  }
}

int ObCheckCLogDiskFullTask::do_run_timer_task_()
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  CLOG_LOG(INFO, "do_run_timer_task_ start", K(ret));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(partition_service_->try_revoke_all_leader(election::ObElection::RevokeType::CLOG_DISK_FULL))) {
    CLOG_LOG(WARN, "try_leader_revoke failed", K(ret));
  }
  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  CLOG_LOG(INFO, "do_run_timer_task_ end", K(ret), K(cost_ts));
  return ret;
}

int ObCLogMgr::batch_submit_log(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
    const ObLogInfoArray& log_info_array, const ObISubmitLogCbArray& cb_array)
{
  int ret = OB_SUCCESS;

  bool can_batch = false;
  ObMemberList member_list;
  int64_t replica_num = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObCLogMgr is not running", K(ret));
  } else if (!is_scan_finished()) {
    ret = OB_STATE_NOT_MATCH;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "is_scan_finished false", K(ret));
    }
#ifdef TRANS_ERROR
  } else if (0 == ObRandom::rand(1, 100) % 20) {
    ret = OB_TRANS_ONE_PC_NOT_ALLOWED;
    CLOG_LOG(WARN, "batch submit log random error", K(ret), K(trans_id), K(partition_array), K(log_info_array));
#endif
  } else if (!check_batch_submit_arguments_(trans_id, partition_array, log_info_array, cb_array)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(trans_id), K(partition_array), K(log_info_array));
  } else if (OB_FAIL(check_can_batch_submit_(partition_array, can_batch)) || !can_batch) {
    CLOG_LOG(TRACE, "now cannot batch submit", K(ret), K(partition_array), K(can_batch));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_member_list_and_replica_num_(partition_array, member_list, replica_num))) {
    CLOG_LOG(
        WARN, "get_member_list_and_replica_num_ failed", K(ret), K(partition_array), K(member_list), K(replica_num));
  } else if (OB_FAIL(
                 batch_submit_log_(trans_id, partition_array, log_info_array, cb_array, member_list, replica_num))) {
    CLOG_LOG(WARN,
        "batch_submit_log_ failed",
        K(ret),
        K(trans_id),
        K(partition_array),
        K(log_info_array),
        K(member_list),
        K(replica_num));
  } else {
    EVENT_ADD(CLOG_BATCH_SUBMITTED_COUNT, partition_array.count());
  }
  // one phase commit fails, turn to two phase commit
  if (OB_SUCCESS != ret) {
    if (EXECUTE_COUNT_PER_SEC(2)) {
      CLOG_LOG(WARN, "batch submit log failed", K(ret));
    }
    ret = OB_TRANS_ONE_PC_NOT_ALLOWED;
  }

  return ret;
}

int ObCLogMgr::check_can_batch_submit(const common::ObPartitionArray& partition_array, bool& can_batch)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObCLogMgr is not running", K(ret));
  } else if (partition_array.count() <= 0 || partition_array.count() > MAX_BATCH_SUBMIT_CNT) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_array), "count", partition_array.count());
  } else if (OB_FAIL(check_can_batch_submit_(partition_array, can_batch))) {
    CLOG_LOG(WARN, "check_can_batch_submit_ failed", K(ret), K(partition_array));
  }

  return ret;
}

int ObCLogMgr::check_can_batch_submit_(const common::ObPartitionArray& partition_array, bool& can_batch)
{
  int ret = OB_SUCCESS;

  can_batch = true;
  common::ObMemberList saved_member_list;
  int64_t saved_replica_num = 0;
  for (int64_t index = 0; can_batch && index < partition_array.count(); index++) {
    int tmp_ret = OB_SUCCESS;
    const common::ObPartitionKey& partition_key = partition_array[index];
    storage::ObIPartitionGroupGuard guard;
    ObIPartitionLogService* log_service = NULL;

    int64_t unused_epoch = false;
    ObTsWindows unused_windows;

    common::ObMemberList tmp_member_list;
    int64_t tmp_replica_num = 0;

    if (OB_SUCCESS != (tmp_ret = partition_service_->get_partition(partition_key, guard)) ||
        NULL == guard.get_partition_group() || NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      can_batch = false;
      tmp_ret = OB_PARTITION_NOT_EXIST;
      CLOG_LOG(WARN, "invalid partition", K(tmp_ret), K(partition_key), K(can_batch));
    } else if (!guard.get_partition_group()->is_valid()) {
      can_batch = false;
      tmp_ret = OB_INVALID_PARTITION;
      CLOG_LOG(WARN, "partition is invalid", K(tmp_ret), K(partition_key), K(can_batch));
    } else if (OB_SUCCESS != (tmp_ret = log_service->get_role_unsafe(unused_epoch, unused_windows))) {
      can_batch = false;
      CLOG_LOG(TRACE, "get_role_unsafe failed", K(tmp_ret), K(partition_key));
    } else if (OB_SUCCESS != (tmp_ret = log_service->try_get_curr_member_list(tmp_member_list))) {
      can_batch = false;
      CLOG_LOG(WARN, "get_curr_member_list failed", K(ret), K(partition_key), K(can_batch), K(tmp_member_list));
    } else if (OB_SUCCESS != (tmp_ret = log_service->try_get_replica_num(tmp_replica_num))) {
      can_batch = false;
      CLOG_LOG(WARN, "get_replica_num failed", K(ret), K(partition_key), K(can_batch), K(tmp_replica_num));
    } else if (0 == index) {
      saved_member_list = tmp_member_list;
      saved_replica_num = tmp_replica_num;
    } else if (!saved_member_list.member_addr_equal(tmp_member_list) || saved_replica_num != tmp_replica_num) {
      can_batch = false;
      CLOG_LOG(TRACE,
          "check_can_batch_submit_ member_list or replica_num not match",
          K(ret),
          K(partition_array),
          K(partition_key),
          K(saved_member_list),
          K(tmp_member_list),
          K(saved_replica_num),
          K(tmp_replica_num));
    }
  }

  return ret;
}

bool ObCLogMgr::check_batch_submit_arguments_(const transaction::ObTransID& trans_id,
    const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array,
    const ObISubmitLogCbArray& cb_array)
{
  bool bool_ret = true;
  const int64_t partition_array_count = partition_array.count();
  int64_t total_log_size = 0;

  if (!trans_id.is_valid()) {
    bool_ret = false;
  } else if (partition_array_count <= 0 || partition_array_count > MAX_BATCH_SUBMIT_CNT) {
    bool_ret = false;
  } else if (partition_array_count != log_info_array.count()) {
    bool_ret = false;
  } else if (partition_array_count != cb_array.count()) {
    bool_ret = false;
  } else {
    ObLogEntryHeader log_header;
    for (int64_t index = 0; bool_ret && index < partition_array.count(); index++) {
      const common::ObPartitionKey& partition_key = partition_array[index];
      const ObLogInfo& log_info = log_info_array[index];
      const ObISubmitLogCb* submit_cb = cb_array[index];
      if (!partition_key.is_valid() || !log_info.is_valid() || NULL == submit_cb) {
        bool_ret = false;
      } else {
        total_log_size += log_info.get_size() + log_header.get_serialize_size();
      }
    }
    if (total_log_size > common::OB_MAX_LOG_ALLOWED_SIZE) {
      bool_ret = false;
    }
  }

  return bool_ret;
}

int ObCLogMgr::batch_submit_log_(const transaction::ObTransID& trans_id,
    const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array,
    const ObISubmitLogCbArray& cb_array, const common::ObMemberList& member_list, const int64_t replica_num)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const ObLogInfoArray& orig_log_info_array = log_info_array;
  ObLogInfoArray new_log_info_array;
  new_log_info_array.reset();
  ObLogPersistSizeArray size_array;
  size_array.reset();
  ObBatchSubmitCtx* ctx = NULL;
  common::ObILogAllocator* alloc_mgr = NULL;

  if (OB_UNLIKELY(partition_array.count() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid partition array is empty", K(ret), K(trans_id), K(partition_array));
  } else {
    const int64_t tenant_id = partition_array.at(0).get_tenant_id();
    for (int64_t index = 0; index < cb_array.count(); index++) {
      ObISubmitLogCb* submit_cb = cb_array[index];
      if (NULL != submit_cb) {
        submit_cb->base_class_reset();
      }
    }

    if (OB_FAIL(TMA_MGR_INSTANCE.get_tenant_log_allocator(tenant_id, alloc_mgr))) {
      CLOG_LOG(WARN, "get_tenant_log_allocator failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(alloc_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "get_tenant_log_allocator return NULL", K(ret), K(tenant_id));
    } else if (OB_FAIL(leader_construct_log_info_(
                   partition_array, orig_log_info_array, new_log_info_array, size_array, alloc_mgr))) {
      CLOG_LOG(WARN, "leader_construct_log_info_ failed", K(ret), K(trans_id), K(partition_array));
    } else if (OB_FAIL(batch_submit_ctx_mgr_.alloc_ctx(trans_id,
                   partition_array,
                   new_log_info_array,
                   size_array,
                   cb_array,
                   member_list,
                   replica_num,
                   self_addr_,
                   alloc_mgr,
                   ctx))) {
      if (OB_SUCCESS != (tmp_ret = cleanup_log_info_array_(new_log_info_array, alloc_mgr))) {
        CLOG_LOG(WARN, "failed to cleanup log info array", K(tmp_ret), K(trans_id), K(partition_array));
      }
      CLOG_LOG(WARN, "batch_submit_ctx_mgr_ alloc_ctx failed", K(ret), K(trans_id), K(partition_array));
      // batch_submit_to_disk_ or batch_submit_to_net_ fails,waiting split
    } else if (OB_SUCCESS !=
               (tmp_ret = batch_submit_to_disk_(trans_id, partition_array, new_log_info_array, alloc_mgr))) {
      CLOG_LOG(WARN, "batch_submit_to_disk_ failed", K(tmp_ret), K(trans_id), K(partition_array));
    } else if (OB_SUCCESS !=
               (tmp_ret = batch_submit_to_net_(member_list, trans_id, partition_array, new_log_info_array))) {
      CLOG_LOG(WARN, "batch_submit_to_net_ failed", K(tmp_ret), K(member_list), K(trans_id), K(partition_array));
    }
  }

  if (NULL != ctx) {
    batch_submit_ctx_mgr_.revert_ctx(ctx);
    ctx = NULL;
  }
  return ret;
}

int ObCLogMgr::batch_flush_cb(const transaction::ObTransID& trans_id, const ObLogCursor& base_log_cursor)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObCLogMgr is not running", K(ret));
  } else if (!trans_id.is_valid() || !base_log_cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(trans_id), K(base_log_cursor));
  } else if (OB_FAIL(batch_flush_cb_(trans_id, base_log_cursor))) {
    CLOG_LOG(WARN, "batch_flush_cb_ failed", K(ret), K(trans_id), K(base_log_cursor));
  }

  return ret;
}

int ObCLogMgr::batch_receive_log(const transaction::ObTransID& trans_id,
    const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array, const common::ObAddr& leader,
    const int64_t cluster_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObCLogMgr is not running", K(ret));
  } else if (!trans_id.is_valid() || partition_array.count() == 0 ||
             partition_array.count() != log_info_array.count() || !leader.is_valid() ||
             OB_INVALID_CLUSTER_ID == cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments",
        K(ret),
        K(trans_id),
        K(partition_array),
        K(log_info_array),
        K(leader),
        K(cluster_id));
  } else if (!log_engine_.is_disk_space_enough()) {
    ret = OB_LOG_OUTOF_DISK_SPACE;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "log outof disk space", K(ret));
    }
  } else if (!is_scan_finished()) {
    ret = OB_STATE_NOT_MATCH;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "is_scan_finished false", K(ret));
    }
  } else if (GCTX.is_in_disabled_state() && cluster_id != obrpc::ObRpcNetHandler::CLUSTER_ID) {
    CLOG_LOG(WARN,
        "cannot receive log from a different cluster in disabled state",
        K(ret),
        K(trans_id),
        K(partition_array),
        K(leader),
        K(cluster_id));
  } else if (OB_FAIL(batch_receive_log_(trans_id, partition_array, log_info_array, leader))) {
    CLOG_LOG(WARN, "batch_receive_log_ failed", K(ret), K(trans_id), K(partition_array), K(leader));
  }

  return ret;
}

int ObCLogMgr::batch_ack_log(
    const transaction::ObTransID& trans_id, const common::ObAddr& server, const ObBatchAckArray& batch_ack_array)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObCLogMgr is not running", K(ret));
  } else if (!trans_id.is_valid() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(trans_id), K(server));
  } else if (OB_FAIL(batch_ack_log_(trans_id, server, batch_ack_array))) {
    CLOG_LOG(WARN, "batch_ack_log_ failed", K(ret), K(trans_id), K(server));
  }

  return ret;
}

int ObCLogMgr::batch_flush_cb_(const transaction::ObTransID& trans_id, const ObLogCursor& base_log_cursor)
{
  int ret = OB_SUCCESS;

  ObBatchSubmitCtx* ctx = NULL;
  if (OB_FAIL(batch_submit_ctx_mgr_.get_ctx(trans_id, ctx)) && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "batch_submit_ctx_mgr_ get_ctx failed", K(ret), K(trans_id));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // already del, do nothing
    ret = OB_SUCCESS;
  } else if (OB_FAIL(ctx->flush_cb(base_log_cursor)) && OB_NO_NEED_BATCH_CTX != ret) {
    CLOG_LOG(WARN, "flush_cb failed", K(ret), K(trans_id), K(base_log_cursor));
  } else if (OB_NO_NEED_BATCH_CTX == ret) {
    if (OB_FAIL(batch_submit_ctx_mgr_.free_ctx(trans_id))) {
      CLOG_LOG(ERROR, "batch_submit_ctx_mgr_ free_ctx failed", K(ret), K(trans_id));
    }
  } else {
    // do nothing
  }

  if (NULL != ctx) {
    batch_submit_ctx_mgr_.revert_ctx(ctx);
    ctx = NULL;
  }

  return ret;
}

int ObCLogMgr::batch_ack_log_(
    const transaction::ObTransID& trans_id, const common::ObAddr& server, const ObBatchAckArray& batch_ack_array)
{
  int ret = OB_SUCCESS;

  ObBatchSubmitCtx* ctx = NULL;
  if (OB_FAIL(batch_submit_ctx_mgr_.get_ctx(trans_id, ctx)) && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "batch_submit_ctx_mgr_ get_ctx failed", K(ret), K(trans_id));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // already del, do nothing
    ret = OB_SUCCESS;
  } else if (OB_FAIL(ctx->ack_log(server, batch_ack_array)) && OB_NO_NEED_BATCH_CTX != ret) {
    CLOG_LOG(WARN, "ack_log failed", K(ret), K(trans_id), K(server), K(batch_ack_array));
  } else if (OB_NO_NEED_BATCH_CTX == ret) {
    if (OB_FAIL(batch_submit_ctx_mgr_.free_ctx(trans_id))) {
      CLOG_LOG(ERROR, "batch_submit_ctx_mgr_ free_ctx failed", K(ret), K(trans_id));
    }
  } else {
    // do nothing
  }

  if (NULL != ctx) {
    batch_submit_ctx_mgr_.revert_ctx(ctx);
    ctx = NULL;
  }

  return ret;
}

int ObCLogMgr::batch_receive_log_(const transaction::ObTransID& trans_id,
    const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array, const common::ObAddr& leader)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const ObLogInfoArray& orig_log_info_array = log_info_array;
  ObLogInfoArray new_log_info_array;
  new_log_info_array.reset();
  ObLogPersistSizeArray size_array;
  size_array.reset();

  ObBatchSubmitCtx* ctx = NULL;
  char* compressed_buf = NULL;
  common::ObILogAllocator* alloc_mgr = NULL;

  if (OB_UNLIKELY(partition_array.count() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid partition array is empty", K(ret), K(trans_id), K(partition_array));
  } else {
    const int64_t tenant_id = partition_array.at(0).get_tenant_id();
    if (OB_FAIL(TMA_MGR_INSTANCE.get_tenant_log_allocator(tenant_id, alloc_mgr))) {
      CLOG_LOG(WARN, "get_tenant_log_allocator failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(alloc_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "get_tenant_log_allocator return NULL", K(ret), K(tenant_id));
    } else if (OB_FAIL(follower_construct_log_info_(
                   partition_array, orig_log_info_array, new_log_info_array, size_array, alloc_mgr))) {
      CLOG_LOG(WARN, "follower_construct_log_info_ failed", K(ret), K(trans_id), K(partition_array));
    } else if (OB_FAIL(batch_submit_ctx_mgr_.alloc_ctx(
                   trans_id, partition_array, new_log_info_array, size_array, leader, alloc_mgr, ctx))) {
      if (OB_SUCCESS != (tmp_ret = cleanup_log_info_array_(new_log_info_array, alloc_mgr))) {
        CLOG_LOG(WARN, "failed to cleanup log info array", K(tmp_ret), K(trans_id), K(partition_array));
      }
      CLOG_LOG(WARN,
          "batch_submit_ctx_mgr_ alloc_ctx failed",
          K(ret),
          K(trans_id),
          K(partition_array),
          K(new_log_info_array),
          K(leader));
    } else if (OB_FAIL(batch_submit_to_disk_(trans_id, partition_array, new_log_info_array, alloc_mgr))) {
      CLOG_LOG(WARN,
          "batch_submit_to_disk_ failed",
          K(ret),
          K(trans_id),
          K(partition_array),
          K(new_log_info_array),
          K(leader));
    } else { /*do nothing*/
    }
  }

  if (NULL != ctx) {
    batch_submit_ctx_mgr_.revert_ctx(ctx);
    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = batch_submit_ctx_mgr_.free_ctx(trans_id))) {
        CLOG_LOG(ERROR, "batch_submit_ctx_mgr_ free_ctx failed", K(ret), K(trans_id));
      }
    }
    ctx = NULL;
  }
  return ret;
}

int ObCLogMgr::batch_submit_to_disk_(const transaction::ObTransID& trans_id,
    const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array,
    common::ObILogAllocator* alloc_mgr)
{
  int ret = OB_SUCCESS;

  ObBatchSubmitDiskTask* disk_task = NULL;
  if (NULL == alloc_mgr) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "alloc_mgr is NULL", K(ret), KP(alloc_mgr), K(partition_array));
  } else if (NULL == (disk_task = ObBatchSubmitDiskTaskFactory::alloc(alloc_mgr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc ObBatchSubmitDiskTask failed", K(ret), K(trans_id), K(partition_array), K(log_info_array));
  } else if (OB_FAIL(disk_task->init(trans_id, partition_array, log_info_array, this, &log_engine_))) {
    CLOG_LOG(WARN, "disk_task init failed", K(ret), K(trans_id), K(partition_array), K(log_info_array));
  } else if (OB_FAIL(log_engine_.submit_flush_task(disk_task))) {
    CLOG_LOG(WARN, "submit flush task failed", K(ret), K(trans_id), K(partition_array), K(log_info_array));
  }

  if (OB_SUCCESS != ret && NULL != disk_task) {
    ObBatchSubmitDiskTaskFactory::free(disk_task);
    disk_task = NULL;
  }

  return ret;
}

int ObCLogMgr::get_member_list_and_replica_num_(
    const common::ObPartitionArray& partition_array, common::ObMemberList& member_list, int64_t& replica_num)
{
  int ret = OB_SUCCESS;

  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  if (partition_array.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_array));
  } else {
    const common::ObPartitionKey& partition_key = partition_array[0];
    if (OB_FAIL(partition_service_->get_partition(partition_key, guard)) || NULL == guard.get_partition_group() ||
        NULL == (log_service = guard.get_partition_group()->get_log_service())) {
      ret = OB_PARTITION_NOT_EXIST;
      CLOG_LOG(WARN, "invalid argument", K(ret), K(partition_key));
    } else if (!guard.get_partition_group()->is_valid()) {
      ret = OB_INVALID_PARTITION;
      CLOG_LOG(WARN, "partition is invalid", K(ret), K(partition_key));
    } else if (OB_FAIL(log_service->try_get_curr_member_list(member_list))) {
      CLOG_LOG(WARN, "get_curr_member_list failed", K(ret), K(partition_key), K(member_list));
    } else if (OB_FAIL(log_service->try_get_replica_num(replica_num))) {
      CLOG_LOG(WARN, "get_replica_num failed", K(ret), K(partition_key), K(replica_num));
    }
  }

  return ret;
}

int ObCLogMgr::leader_construct_log_info_(const common::ObPartitionArray& partition_array,
    const ObLogInfoArray& orig_log_info_array, ObLogInfoArray& new_log_info_array, ObLogPersistSizeArray& size_array,
    common::ObILogAllocator*& alloc_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "alloc_mgr is NULL", K(partition_array), K(ret));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < partition_array.count(); index++) {
      int64_t pos = 0;
      char* new_buff = NULL;
      ObLogEntryHeader log_header;
      ObLogEntry log_entry;
      ObLogInfo new_log_info;

      const ObLogType log_type = OB_LOG_SUBMIT;
      const common::ObPartitionKey& partition_key = partition_array[index];
      const uint64_t log_id = orig_log_info_array[index].get_log_id();
      const char* orig_buff = orig_log_info_array[index].get_buf();
      const int64_t data_len = orig_log_info_array[index].get_size();
      const int64_t generation_timestamp = common::ObClockGenerator::getClock();
      const int64_t epoch_id = orig_log_info_array[index].get_proposal_id().ts_;
      const common::ObProposalID proposal_id = orig_log_info_array[index].get_proposal_id();
      const int64_t submit_timestamp = orig_log_info_array[index].get_submit_timestamp();
      const common::ObVersion unused_freeze_version = ObVersion(1, 0);
      const bool is_trans_log = true;
      const bool need_flushed = true;

      if (OB_SUCC(ret)) {
        if (OB_FAIL(log_header.generate_header(log_type,
                partition_key,
                log_id,
                orig_buff,
                data_len,
                generation_timestamp,
                epoch_id,
                proposal_id,
                submit_timestamp,
                unused_freeze_version,
                is_trans_log))) {
          CLOG_LOG(WARN,
              "log_header genearte_header failed",
              K(ret),
              K(log_type),
              K(partition_key),
              K(log_id),
              K(generation_timestamp),
              K(epoch_id),
              K(proposal_id),
              K(submit_timestamp),
              K(unused_freeze_version));
        } else if (OB_FAIL(log_entry.generate_entry(log_header, orig_buff))) {
          CLOG_LOG(WARN, "log_entry generate_entry failed", K(ret), K(log_header));
        } else if (NULL == (new_buff = static_cast<char*>(alloc_mgr->ge_alloc(log_entry.get_serialize_size())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          CLOG_LOG(WARN, "alloc memory failed", K(ret), K(partition_key), K(log_id));
        } else if (OB_FAIL(log_entry.serialize(new_buff, log_entry.get_serialize_size(), pos))) {
          CLOG_LOG(WARN, "serialize failed", K(ret), K(partition_key), K(log_id));
        } else if (OB_FAIL(new_log_info.set(new_buff,
                       log_entry.get_serialize_size(),
                       log_id,
                       submit_timestamp,
                       proposal_id,
                       need_flushed))) {
          CLOG_LOG(WARN, "new_log_info set failed", K(ret), K(partition_key), K(log_id));
        } else if (OB_FAIL(new_log_info_array.push_back(new_log_info))) {
          CLOG_LOG(WARN, "new_log_info_array push_back failed", K(ret), K(partition_key), K(log_id));
        } else { /*do nothing*/
        }

        if (OB_SUCC(ret)) {
          int32_t persist_size = (int32_t)new_log_info.get_size();
          if (OB_FAIL(size_array.push_back(persist_size))) {
            CLOG_LOG(WARN, "new_log_info_array push_back failed", K(ret), K(new_log_info));
          }
        }
      }
      // free memory if fail
      if (OB_FAIL(ret)) {
        if (NULL != new_buff) {
          alloc_mgr->ge_free(new_buff);
          new_buff = NULL;
        }
        for (int64_t i = 0; i < index; i++) {
          void* ptr = static_cast<void*>(const_cast<char*>(new_log_info_array[i].get_buf()));
          alloc_mgr->ge_free(ptr);
          ptr = NULL;
          // new_log_info_array[i].reset();
        }
        new_log_info_array.reset();
        size_array.reset();
      }
    }
  }
  return ret;
}

int ObCLogMgr::follower_construct_log_info_(const common::ObPartitionArray& partition_array,
    const ObLogInfoArray& orig_log_info_array, ObLogInfoArray& new_log_info_array, ObLogPersistSizeArray& size_array,
    common::ObILogAllocator*& alloc_mgr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_ISNULL(alloc_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "alloc_mgr is NULL", K(alloc_mgr), K(ret));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < partition_array.count(); index++) {
      ObLogInfo new_log_info;
      char* new_buff = NULL;
      const char* orig_buff = orig_log_info_array[index].get_buf();
      const int64_t data_len = orig_log_info_array[index].get_size();
      const uint64_t log_id = orig_log_info_array[index].get_log_id();
      const int64_t submit_timestamp = orig_log_info_array[index].get_submit_timestamp();
      const common::ObProposalID& proposal_id = orig_log_info_array[index].get_proposal_id();
      const common::ObPartitionKey& partition_key = partition_array[index];
      bool need_flushed = true;

      if (OB_SUCCESS != (tmp_ret = check_can_receive_batch_log_(partition_key, log_id))) {
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN, "check_can_receive_batch_log_ failed", K(ret), K(partition_key), K(log_id));
        }
        need_flushed = false;
      }
      if (OB_SUCC(ret)) {
        // verify checksum
        ObLogEntry log_entry;
        int64_t pos = 0;
        if (OB_FAIL(log_entry.deserialize(orig_buff, data_len, pos))) {
          CLOG_LOG(ERROR, "log_entry deserialize failed", K(ret), K(partition_key));
        } else if (!log_entry.check_integrity()) {
          ret = OB_INVALID_DATA;
          CLOG_LOG(ERROR, "log_entry check_integrity failed", K(partition_key), K(log_entry));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(NULL == (new_buff = static_cast<char*>(alloc_mgr->ge_alloc(data_len))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          CLOG_LOG(WARN, "alloc memory failed", K(ret), K(partition_key), K(log_id));
        } else {
          ObLogInfo compressed_log_info;
          MEMCPY(new_buff, orig_buff, data_len);
          if (OB_FAIL(new_log_info.set(new_buff, data_len, log_id, submit_timestamp, proposal_id, need_flushed))) {
            CLOG_LOG(WARN, "new_log_info set failed", K(ret), K(partition_key), K(log_id));
          } else if (OB_FAIL(new_log_info_array.push_back(new_log_info))) {
            CLOG_LOG(WARN, "new_log_info_array push_back failed", K(ret), K(partition_key), K(log_id));
          } else { /*do nothing*/
          }

          if (OB_SUCC(ret)) {
            int32_t persist_size = (int32_t)new_log_info.get_size();
            if (OB_FAIL(size_array.push_back(persist_size))) {
              CLOG_LOG(WARN, "new_log_info_array push_back failed", K(ret), K(new_log_info));
            }
          }
        }
        // free memory if fail
        if (OB_FAIL(ret)) {
          if (NULL != new_buff) {
            alloc_mgr->ge_free(new_buff);
            new_buff = NULL;
          }
          for (int64_t i = 0; i < index; i++) {
            void* ptr = static_cast<void*>(const_cast<char*>(new_log_info_array[i].get_buf()));
            alloc_mgr->ge_free(ptr);
            ptr = NULL;
            // new_log_info_array[i].reset();
          }
          new_log_info_array.reset();
          size_array.reset();
        }
      }
    }
  }
  return ret;
}

int ObCLogMgr::batch_submit_to_net_(const common::ObMemberList& member_list, const transaction::ObTransID& trans_id,
    const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array)
{
  int ret = OB_SUCCESS;

  common::ObMemberList tmp_member_list;
  if (OB_FAIL(tmp_member_list.deep_copy(member_list))) {
    CLOG_LOG(WARN, "member_list deep_copy failed", K(ret), K(member_list));
  } else if (tmp_member_list.contains(self_addr_) && OB_FAIL(tmp_member_list.remove_server(self_addr_))) {
    CLOG_LOG(ERROR, "remove_server failed", K(ret), K(tmp_member_list), K(self_addr_));
  } else if (tmp_member_list.is_valid() &&
             OB_FAIL(log_engine_.submit_batch_log(tmp_member_list, trans_id, partition_array, log_info_array))) {
    CLOG_LOG(WARN,
        "submit_batch_log failed",
        K(ret),
        K(tmp_member_list),
        K(trans_id),
        K(partition_array),
        K(log_info_array));
  }

  return ret;
}

int ObCLogMgr::query_log_info_with_log_id(const common::ObPartitionKey& partition_key, const uint64_t log_id,
    int64_t& accum_checksum, int64_t& submit_timestamp, int64_t& epoch_id)
{
  int ret = OB_SUCCESS;
  ObLogEntry log_entry;
  bool is_batch_committed = false;
  if (OB_FAIL(query_log_info_with_log_id(partition_key, log_id, log_entry, accum_checksum, is_batch_committed))) {
    CLOG_LOG(WARN, "failed to query_log_info_with_log_id", K(ret), K(partition_key), K(log_id));
  } else {
    epoch_id = log_entry.get_header().get_epoch_id();
    submit_timestamp = log_entry.get_header().get_submit_timestamp();
  }
  return ret;
}

int ObCLogMgr::query_log_info_with_log_id(const common::ObPartitionKey& partition_key, const uint64_t log_id,
    ObLogEntry& log_entry, int64_t& accum_checksum, bool& is_batch_committed)
{
  int ret = OB_SUCCESS;
  ObLogCursorExt log_cursor_ext;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret), K(partition_key), K(log_id));
  } else if (OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key), K(log_id));
  } else if (OB_FAIL(log_engine_.get_cursor(partition_key, log_id, log_cursor_ext))) {
    if (OB_CURSOR_NOT_EXIST == ret) {
      // The first log ilog written by Partition has not been flushed or loaded by ilog_storage.
      // If a minor freeze occurs at this time, need read the contents of this log, then log_engine_ will return
      // OB_CURSOR_NOT_EXIST. In this scenario, OB_EAGAIN is returned, and the upper layer will try again to read the
      // data.
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "ilog_storage get invalid log, cursor not exist", K(ret), K(partition_key), K(log_id));
    } else if (OB_NEED_RETRY == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
      ret = OB_EAGAIN;
    } else {
      CLOG_LOG(WARN, "log_engine_ get_cursor failed", K(ret), K(partition_key), K(log_id));
    }
  } else {
    ObReadParam read_param;
    read_param.file_id_ = log_cursor_ext.get_file_id();
    read_param.offset_ = log_cursor_ext.get_offset();
    read_param.read_len_ = log_cursor_ext.get_size();
    ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_CACHE_ID);
    ObReadBuf& rbuf = guard.get_read_buf();
    if (OB_UNLIKELY(!rbuf.is_valid())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "failed to alloc read_buf", K(ret), K(rbuf), K(partition_key), K(log_id), K(log_cursor_ext));
    } else if (OB_FAIL(log_engine_.read_log_by_location(read_param, rbuf, log_entry))) {
      CLOG_LOG(WARN, "read_log_by_location failed", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
    } else {
      accum_checksum = log_cursor_ext.get_accum_checksum();
      is_batch_committed = log_cursor_ext.is_batch_committed();
    }
  }
  return ret;
}

int ObCLogMgr::get_election_group_priority(const uint64_t tenant_id, election::ObElectionGroupPriority& priority) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret));
  } else if (!is_running_) {
    // clog_mgr stops before election_mgr, so is_running==false may occur
    ret = OB_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "ObCLogMgr is not running", K(ret));
    }
  } else if (tenant_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else {
    priority.reset();
    const int64_t now = ObTimeUtility::current_time();
    const bool is_candidate = partition_service_->is_tenant_active(tenant_id) &&
                              OBSERVER.get_gctx().rs_server_status_ == share::RSS_IS_WORKING;
    int tmp_ret = OB_SUCCESS;
    bool is_data_disk_error = false;
    bool is_clog_disk_error = log_engine_.is_clog_disk_error();
    if (OB_SUCCESS != (tmp_ret = ObIOManager::get_instance().is_disk_error(is_data_disk_error))) {
      CLOG_LOG(WARN, "is_data_disk_error failed", K(tmp_ret));
    }
    if (is_clog_disk_error) {
      priority.set_system_clog_disk_error();
    }
    if (is_data_disk_error) {
      priority.set_system_data_disk_error();
    }
    if (!partition_service_->is_service_started()) {
      priority.set_system_service_not_started();
    }
    priority.set_candidate(is_candidate);
    if (!is_candidate) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        CLOG_LOG(WARN,
            "is_candidate is false",
            K(tenant_id),
            "is_tenant_active",
            partition_service_->is_tenant_active(tenant_id),
            "rs_server_status",
            OBSERVER.get_gctx().rs_server_status_);
      }
    }
  }

  return ret;
}

int ObCLogMgr::handle_get_remote_log_request(
    const obrpc::ObLogGetRemoteLogRequest& request_msg, obrpc::ObLogGetRemoteLogResponse& result)
{
  int ret = OB_SUCCESS;
  result.reset();
  const common::ObPartitionKey& partition_key = request_msg.get_partition_key();
  const uint64_t log_id = request_msg.get_log_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret), K(partition_key), K(log_id));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObCLogMgr is not running", K(ret), K(partition_key), K(log_id));
  } else if (!partition_key.is_valid() || OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key), K(log_id));
  } else {
    ObLogEntry log_entry;
    uint64_t unused_log_id = OB_INVALID_ID;
    int64_t submit_timestamp = OB_INVALID_TIMESTAMP;
    transaction::ObTransID trans_id;

    ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_CACHE_ID);
    ObReadBuf& rbuf = guard.get_read_buf();
    if (OB_UNLIKELY(!rbuf.is_valid())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "failed to alloc read_buf", K(ret), K(rbuf), K(partition_key), K(log_id));
    } else if (OB_FAIL(get_log_from_ilog_storage_(partition_key, log_id, rbuf, log_entry))) {
      CLOG_LOG(WARN, "get_log_from_ilog_storage_ failed", K(ret), K(partition_key), K(log_id));
    } else if (OB_FAIL(transaction::ObTransLogParseUtils::parse_redo_prepare_log(
                   log_entry, unused_log_id, submit_timestamp, trans_id))) {
      CLOG_LOG(ERROR, "parse_redo_prepare_log failed", K(ret), K(partition_key), K(log_id));
      ret = OB_ERR_UNEXPECTED;
    } else if (unused_log_id != log_id) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "log_id not match", K(ret), K(partition_key), K(log_id), K(unused_log_id));
    } else { /*do nothing*/
    }
    result.set(partition_key, log_id, trans_id, submit_timestamp, ret);
    CLOG_LOG(INFO,
        "handle_get_remote_log_request finished",
        K(ret),
        K(partition_key),
        K(log_id),
        K(trans_id),
        K(submit_timestamp));
  }

  return ret;
}

int ObCLogMgr::get_log_from_ilog_storage_(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, ObReadBuf& rbuf, ObLogEntry& log_entry)
{
  int ret = OB_SUCCESS;
  ObLogCursorExt log_cursor;
  ObReadParam read_param;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret), K(partition_key));
  } else if (!partition_key.is_valid() || OB_INVALID_ID == log_id || !rbuf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key), K(log_id));
  } else if (OB_FAIL(log_engine_.get_cursor(partition_key, log_id, log_cursor))) {
    if (OB_CURSOR_NOT_EXIST == ret) {
      CLOG_LOG(WARN, "ilog_storage get invalid log, cursor not exist", K(ret), K(partition_key), K(log_id));
    } else if (OB_NEED_RETRY == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
      ret = OB_EAGAIN;
    } else {
      CLOG_LOG(WARN, "ilog_storage get_cursor failed", K(ret), K(partition_key), K(log_id));
    }
  } else {
    read_param.file_id_ = log_cursor.get_file_id();
    read_param.offset_ = log_cursor.get_offset();
    read_param.read_len_ = log_cursor.get_size();
    if (OB_FAIL(log_engine_.read_log_by_location(read_param, rbuf, log_entry))) {
      CLOG_LOG(WARN, "read_log_by_location failed", K(ret), K(partition_key), K(read_param));
    }
  }
  return ret;
}

int ObCLogMgr::check_can_receive_batch_log_(const common::ObPartitionKey& partition_key, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObCLogMgr is not inited", K(ret));
  } else if (!partition_key.is_valid() || OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key));
  } else if (OB_FAIL(partition_service_->get_partition(partition_key, guard)) || NULL == guard.get_partition_group() ||
             NULL == (log_service = guard.get_partition_group()->get_log_service())) {
    ret = OB_ENTRY_NOT_EXIST;
    CLOG_LOG(WARN, "get_log_service failed", K(ret), K(partition_key), K(log_id));
  } else if (!(guard.get_partition_group()->is_valid())) {
    ret = OB_INVALID_PARTITION;
    CLOG_LOG(WARN, "partition is invalid", K(ret), K(partition_key), K(log_id));
  } else if (OB_FAIL(log_service->check_can_receive_batch_log(log_id))) {
    CLOG_LOG(TRACE, "check_can_receive_batch_log failed", K(ret), K(partition_key), K(log_id));
  } else {
    // do nothing
  }
  return ret;
}

int ObCLogMgr::cleanup_log_info_array_(ObLogInfoArray& array, common::ObILogAllocator*& alloc_mgr)
{
  int ret = OB_SUCCESS;
  // free mem of compress_log_info_array
  if (OB_ISNULL(alloc_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "alloc mgr is NULL");
  } else {
    int64_t array_size = array.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < array_size; i++) {
      void* ptr = static_cast<void*>(const_cast<char*>(array[i].get_buf()));
      if (NULL != ptr) {
        alloc_mgr->ge_free(ptr);
        ptr = NULL;
      }
    }
    array.reset();
  }
  return ret;
}

int ObCLogMgr::delete_all_log_files()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(log_engine_.delete_all_clog_files())) {
    CLOG_LOG(ERROR, "log_engine_.delete_all_clog_files failed", K(ret));
  } else if (OB_FAIL(log_engine_.delete_all_ilog_files())) {
    CLOG_LOG(ERROR, "log_engine_.delete_all_ilog_files failed", K(ret));
  } else {
    CLOG_LOG(INFO, "delete_all_log_files success");
  }
  return ret;
}

int ObCLogMgr::add_pg_archive_task(storage::ObIPartitionGroup* partition)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(archive_mgr_.add_pg_log_archive_task(partition))) {
    CLOG_LOG(WARN, "add_pg_archive_task fail", K(ret), KPC(partition));
  }
  return ret;
}

int ObCLogMgr::try_advance_restoring_clog()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(archive_restore_engine_.try_advance_restoring_clog())) {
    CLOG_LOG(WARN, "failed to try_advance_restoring_clog", KR(ret));
  }
  return ret;
}

int ObCLogMgr::delete_pg_archive_task(storage::ObIPartitionGroup* partition)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(archive_mgr_.delete_pg_log_archive_task(partition))) {
    CLOG_LOG(WARN, "delete_pg_archive_task fail", K(ret), KPC(partition));
  }
  return ret;
}

int ObCLogMgr::mark_log_archive_encount_fatal_err(
    const common::ObPartitionKey& pkey, const int64_t incarnation, const int64_t archive_round)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "clog_mgr is not inited", KR(ret), K(pkey), K(incarnation), K(archive_round));
  } else if (OB_FAIL(archive_mgr_.mark_encounter_fatal_err(pkey, incarnation, archive_round))) {
    CLOG_LOG(WARN, "failed to mark_encounter_fatal_error", KR(ret), K(pkey), K(incarnation), K(archive_round));
  } else { /*do nothing*/
  }
  return ret;
}

int ObCLogMgr::get_archive_pg_map(archive::PGArchiveMap*& map)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "clog_mgr is not inited", KR(ret));
  } else if (OB_FAIL(archive_mgr_.get_archive_pg_map(map))) {
    CLOG_LOG(WARN, "failed to get_archive_pg_map", KR(ret));
  }
  return ret;
}

bool ObCLogMgr::is_server_archive_stop(const int64_t incarnation, const int64_t archive_round)
{
  return archive_mgr_.is_server_archive_stop(incarnation, archive_round);
}

}  // namespace clog
}  // end namespace oceanbase
