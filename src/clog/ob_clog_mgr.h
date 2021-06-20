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

#ifndef OCEANBASE_CLOG_OB_CLOG_MGR_
#define OCEANBASE_CLOG_OB_CLOG_MGR_

#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "common/ob_member.h"
#include "election/ob_election_cb.h"
#include "ob_fetch_log_engine.h"
#include "ob_batch_submit_ctx_mgr.h"
#include "ob_clog_sync_msg.h"
#include "ob_log_callback_engine.h"
#include "ob_log_callback_handler.h"
#include "ob_log_callback_thread_pool.h"
#include "ob_log_define.h"
#include "ob_log_engine.h"
#include "ob_log_event_scheduler.h"
#include "ob_log_executor.h"
#include "ob_log_external_executor_v2.h"
#include "ob_log_membership_mgr_V2.h"
#include "ob_log_replay_driver_runnable.h"
#include "ob_log_scan_runnable.h"
#include "ob_log_state_driver_runnable.h"
#include "ob_partition_log_packet_handler.h"
#include "ob_external_log_service.h"
#include "ob_partition_log_packet_handler.h"
#include "ob_remote_log_query_engine.h"
#include "ob_log_archive_and_restore_driver.h"
#include "archive/ob_archive_mgr.h"
#include "archive/ob_archive_restore_engine.h"

namespace oceanbase {
namespace common {
class ObILogAllocator;
}
namespace share {
class ObIPSCb;
}
namespace storage {
class ObPartitionService;
class ObIPartitionGroup;
}  // namespace storage
namespace election {
class ObIElectionMgr;
class ObElectionGroupPriority;
}  // namespace election
namespace obrpc {
class ObBatchRpc;
class ObLogRpcProxy;
}  // namespace obrpc
namespace transaction {
class ObTransID;
}
namespace clog {
class ObIPartitionLogService;
class ObLogReplayEngineWrapper;
struct CLogMgrConfig {
  int64_t pls_tlimit_;
  int64_t pls_hlimit_;
  int64_t pls_psize_;
  int64_t tlimit_;
  int64_t hlimit_;
  int64_t psize_;
  ObLogEnv::Config le_config_;
  bool is_valid() const
  {
    return (pls_tlimit_ >= 0) && (pls_hlimit_ >= 0) && (pls_psize_ >= 0) && (tlimit_ >= 0) && (hlimit_ >= 0) &&
           (psize_ >= 0) && le_config_.is_valid();
  }
};

class ObCheckLogFileCollectTask : public common::ObTimerTask {
public:
  ObCheckLogFileCollectTask() : clog_mgr_(NULL), is_inited_(false)
  {}
  virtual ~ObCheckLogFileCollectTask()
  {}

public:
  int init(ObICLogMgr* clog_mgr);
  virtual void runTimerTask();

private:
  ObICLogMgr* clog_mgr_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCheckLogFileCollectTask);
};

class ObCheckpointLogReplicaTask : public common::ObTimerTask {
public:
  ObCheckpointLogReplicaTask() : partition_service_(NULL), is_inited_(false)
  {}
  virtual ~ObCheckpointLogReplicaTask()
  {}

public:
  int init(storage::ObPartitionService* partition_service);
  virtual void runTimerTask();

private:
  storage::ObPartitionService* partition_service_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCheckpointLogReplicaTask);
};

class ObCheckCLogDiskFullTask : public common::ObTimerTask {
public:
  ObCheckCLogDiskFullTask() : partition_service_(NULL), is_inited_(false)
  {}
  virtual ~ObCheckCLogDiskFullTask()
  {}

public:
  int init(storage::ObPartitionService* partition_service);
  void runTimerTask() final;

private:
  int do_run_timer_task_();

private:
  storage::ObPartitionService* partition_service_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCheckCLogDiskFullTask);
};

class ObICLogMgr : public election::ObIElectionGroupPriorityGetter {
public:
  ObICLogMgr()
  {}
  virtual ~ObICLogMgr()
  {}

public:
  virtual int init(storage::ObPartitionService* partition_service, ObLogReplayEngineWrapper* replay_engine,
      election::ObIElectionMgr* election_mgr, const common::ObAddr& self_addr, obrpc::ObBatchRpc* batch_rpc,
      obrpc::ObLogRpcProxy* rpc, common::ObMySQLProxy* sql_proxy, const CLogMgrConfig& config) = 0;
  virtual void destroy() = 0;
  virtual int create_partition(const common::ObPartitionKey& partition_key, const int64_t replica_num,
      const common::ObMemberList& member_list, const common::ObAddr& leader, const int64_t lease_start,
      const common::ObVersion& freeze_version, const common::ObReplicaType replica_type,
      const common::ObReplicaProperty replica_property, const int64_t last_submit_timestamp,
      const uint64_t last_replay_log_id, const int16_t archive_restore_state, const bool need_skip_mlist_check,
      ObIPartitionLogService* pls) = 0;
  virtual int assign_partition(const common::ObPartitionKey& partition_key, const ObReplicaType replica_type,
      const common::ObReplicaProperty replica_property, const common::ObBaseStorageInfo& info,
      const common::ObVersion& freeze_version, const int16_t archive_restore_state, ObIPartitionLogService* pls) = 0;
  virtual int add_partition(const common::ObPartitionKey& partition_key, const ObReplicaType replica_type,
      const common::ObReplicaProperty replica_property, const common::ObBaseStorageInfo& info,
      const common::ObVersion& freeze_version, const ObAddr& src_server, const int16_t archive_restore_state,
      ObIPartitionLogService* pls) = 0;
  virtual int handle_packet(int pcode, const char* data, int64_t len) = 0;
  virtual int run_check_log_file_collect_task() = 0;
  virtual int req_start_log_id_by_ts(
      const obrpc::ObLogReqStartLogIdByTsRequest& req_msg, obrpc::ObLogReqStartLogIdByTsResponse& result) = 0;
  virtual int req_start_pos_by_log_id(
      const obrpc::ObLogReqStartPosByLogIdRequest& req_msg, obrpc::ObLogReqStartPosByLogIdResponse& result) = 0;
  virtual int fetch_log(
      const obrpc::ObLogExternalFetchLogRequest& req_msg, obrpc::ObLogExternalFetchLogResponse& result) = 0;
  virtual int req_heartbeat_info(
      const obrpc::ObLogReqHeartbeatInfoRequest& req_msg, obrpc::ObLogReqHeartbeatInfoResponse& result) = 0;
  virtual int req_start_log_id_by_ts_with_breakpoint(const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req_msg,
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& result) = 0;
  virtual int req_start_pos_by_log_id_with_breakpoint(
      const obrpc::ObLogReqStartPosByLogIdRequestWithBreakpoint& req_msg,
      obrpc::ObLogReqStartPosByLogIdResponseWithBreakpoint& result) = 0;
  virtual int handle_get_mc_ts_request(
      const obrpc::ObLogGetMCTsRequest& request_msg, obrpc::ObLogGetMCTsResponse& result) = 0;
  virtual int handle_get_mc_ctx_array_request(
      const obrpc::ObLogGetMcCtxArrayRequest& request_msg, obrpc::ObLogGetMcCtxArrayResponse& result) = 0;
  virtual int handle_get_priority_array_request(
      const obrpc::ObLogGetPriorityArrayRequest& request_msg, obrpc::ObLogGetPriorityArrayResponse& result) = 0;
  virtual bool is_scan_finished() const = 0;
  virtual ObILogEngine* get_log_engine() = 0;
  virtual logservice::ObExtLogService* get_external_log_service() = 0;
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void wait() = 0;
  virtual int get_need_freeze_partition_array(NeedFreezePartitionArray& partition_array) const = 0;
  virtual uint32_t get_clog_min_using_file_id() const = 0;
  virtual int get_clog_using_disk_space(int64_t& using_space) const = 0;
  virtual int get_ilog_using_disk_space(int64_t& using_space) const = 0;
  //===================== batch change member begin ================
  virtual int batch_add_member(
      const obrpc::ObChangeMemberArgs& ctx_array, obrpc::ObChangeMemberCtxs& return_ctx_array) = 0;
  virtual int batch_remove_member(
      const obrpc::ObChangeMemberArgs& ctx_array, obrpc::ObChangeMemberCtxs& return_ctx_array) = 0;
  virtual int batch_is_member_change_done(obrpc::ObChangeMemberCtxs& return_ctx_array) = 0;
  // Function: This function is responsible for performing the operation of adding members of a group of Partitions,
  //           which does not contain any retry logic
  //
  // Arguments:
  // * partition_array: Partition list
  // * member_array: member to be added for each partition.
  // * quorum_array: quorum for each partition.
  // Output:
  // * ret_array: ret code for each partition(similar with ObPartitionLogService::add_member)
  // * log_info_array: MCLogInfo for each partition,which is used as argument for is_member_change_done.
  //
  // The array size of all input parameters is equal in size and corresponds to one by one.
  // When the function returns OB_SUCCESS or OB_PARTIAL_FAILED, the array size of all output parameters is consistent
  // with the input parameters and corresponds to one to one. When the function returns other error codes, ignore the
  // return values of ret_array and log_info_array.
  //
  // If an entry of ret_array is not OB_SUCCESS, log_info_array returns an invalid mc_log_info for the entry.
  //
  // ret code:
  // OB_NOT_INIT/OB_INVALID_ARGUMENT for normal case.
  // OB_SUCCESS, all Partititions execute success.
  // OB_PARTIAL_FAILED, some Partition execution fails, the caller needs to check ret_array to determine the execution
  // state of each Partition.
  virtual int batch_add_member(const common::ObPartitionArray& partition_array,
      const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
      common::ObReturnArray& ret_array, ObMCLogInfoArray& log_info_array) = 0;
  // similar with batch_add_member.
  // This function performs the delete member operation of a group of Partitions.
  virtual int batch_remove_member(const common::ObPartitionArray& partition_array,
      const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
      common::ObReturnArray& ret_array, ObMCLogInfoArray& log_info_array) = 0;
  // Function: Check whether the operation of adding/deleting members of a group of Parttiions is performed
  // successfully.
  //
  // Arguments:
  // * partition_array: Partition list
  // * log_info_array: ObMCLogInfo returned by add_member/remove_member for each partition
  //
  // Outputs:
  // * ret_array: Whether the execution of each Partition member change operation has been completed,
  //              see ObPartitionLogService::is_member_change_done function
  //
  // ret code:
  // OB_NOT_INIT/OB_INVALID_ARGUMENT etc. for normal case.
  // OB_SUCCESS, all Partititions execute success.
  // OB_PARTIAL_FAILED, some Partition execution fails, the caller needs to check ret_array to determine the execution
  // state of each Partition.
  virtual int batch_is_member_change_done(const common::ObPartitionArray& partition_array,
      const ObMCLogInfoArray& log_info_array, common::ObReturnArray& ret_array) = 0;
  //===================== batch chdange member end ================

  // Function:
  // Single-machine distributed transaction batch commits log, performing one phase optimization.
  // If return OB_SUCCESS, one phase commit successfully.
  // If return code is not OB_SUCCESS, the transaction degenerates into a regular two-phase commit.
  //
  // Arguments:
  // * trans_id           : transaction id, unique identifier of the transaction
  // * partition_array    : Partition list of all participants in the transaction
  // * log_info_array     : The info array corresponding to each participant's log, including buf, proposal_id and
  // log_id, submit_timestamp.
  // * cb_array           : The cb array corresponding to each participant's log (details in ob_i_submit_log_cb.h)
  //
  // ret code:
  //
  // OB_NOT_INIT
  // OB_INVALID_ARGUMENT
  // In addition to requiring all the fields passed in to be legal, it is also required that the counts of several
  // arrays are equal, and the total length of each buf is less than OB_MAX_LOG_ALLOWED_SIZE OB_SUCCESS
  virtual int batch_submit_log(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, const ObISubmitLogCbArray& cb_array) = 0;
  virtual int check_can_batch_submit(const common::ObPartitionArray& partition_array, bool& can_batch) = 0;
  virtual int batch_flush_cb(const transaction::ObTransID& trans_id, const ObLogCursor& base_log_cursor) = 0;
  virtual int batch_receive_log(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, const common::ObAddr& leader, const int64_t cluster_id) = 0;
  // Currently batch_ack_log does not check proposal_id. Assuming the same trans_id, batch_submit_log will only be
  // called once
  virtual int batch_ack_log(
      const transaction::ObTransID& trans_id, const common::ObAddr& server, const ObBatchAckArray& batch_ack_array) = 0;
  virtual int query_log_info_with_log_id(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      int64_t& accum_checksum, int64_t& submit_timestamp, int64_t& epoch_id) = 0;
  virtual int query_log_info_with_log_id(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      ObLogEntry& log_entry, int64_t& accum_checksum, bool& is_batch_committed) = 0;
  virtual int get_election_group_priority(
      const uint64_t tenant_id, election::ObElectionGroupPriority& priority) const = 0;
  virtual int get_candidates_array(const common::ObPartitionIArray& pkey_array,
      const common::ObAddrIArray& dst_server_list, common::ObSArray<common::ObAddrSArray>& candidate_mlist_array,
      common::ObSArray<obrpc::CandidateStatusList>& status_array) const = 0;
  virtual int handle_get_remote_log_request(
      const obrpc::ObLogGetRemoteLogRequest& request_msg, obrpc::ObLogGetRemoteLogResponse& result) = 0;
  virtual ObLogCallbackEngine& get_cb_engine() = 0;
  virtual int delete_all_log_files() = 0;
  virtual int add_pg_archive_task(storage::ObIPartitionGroup* partition) = 0;
  virtual int try_advance_restoring_clog() = 0;
  virtual int delete_pg_archive_task(storage::ObIPartitionGroup* partition) = 0;
  virtual int mark_log_archive_encount_fatal_err(
      const common::ObPartitionKey& pkey, const int64_t incarnation, const int64_t archive_round) = 0;
  virtual int get_archive_pg_map(archive::PGArchiveMap*& map) = 0;
  virtual bool is_server_archive_stop(const int64_t incarnation, const int64_t archive_round) = 0;
};

class ObCLogMgr : public ObICLogMgr {
public:
  ObCLogMgr();
  virtual ~ObCLogMgr();

public:
  virtual int init(storage::ObPartitionService* partition_service, ObLogReplayEngineWrapper* replay_engine,
      election::ObIElectionMgr* election_mgr, const common::ObAddr& self_addr, obrpc::ObBatchRpc* batch_rpc,
      obrpc::ObLogRpcProxy* rpc, common::ObMySQLProxy* sql_proxy, const CLogMgrConfig& config);
  void destroy();
  virtual int create_partition(const common::ObPartitionKey& partition_key, const int64_t replica_num,
      const common::ObMemberList& member_list, const common::ObAddr& leader, const int64_t lease_start,
      const common::ObVersion& freeze_version, const common::ObReplicaType replica_type,
      const common::ObReplicaProperty replica_property, const int64_t last_submit_timestamp,
      const uint64_t last_replay_log_id, const int16_t archive_restore_state, const bool need_skip_mlist_check,
      ObIPartitionLogService* pls);
  virtual int assign_partition(const common::ObPartitionKey& partition_key, const ObReplicaType replica_type,
      const common::ObReplicaProperty replica_property, const common::ObBaseStorageInfo& info,
      const common::ObVersion& freeze_version, const int16_t archive_restore_state, ObIPartitionLogService* pls);
  virtual int add_partition(const common::ObPartitionKey& partition_key, const ObReplicaType replica_type,
      const common::ObReplicaProperty replica_property, const common::ObBaseStorageInfo& info,
      const common::ObVersion& freeze_version, const ObAddr& src_server, const int16_t archive_restore_state,
      ObIPartitionLogService* pls);
  virtual int handle_packet(int pcode, const char* data, int64_t len);
  virtual int run_check_log_file_collect_task();
  virtual int req_start_log_id_by_ts(
      const obrpc::ObLogReqStartLogIdByTsRequest& req_msg, obrpc::ObLogReqStartLogIdByTsResponse& result);
  virtual int req_start_pos_by_log_id(
      const obrpc::ObLogReqStartPosByLogIdRequest& req_msg, obrpc::ObLogReqStartPosByLogIdResponse& result);
  virtual int fetch_log(
      const obrpc::ObLogExternalFetchLogRequest& req_msg, obrpc::ObLogExternalFetchLogResponse& result);
  virtual int req_heartbeat_info(
      const obrpc::ObLogReqHeartbeatInfoRequest& req_msg, obrpc::ObLogReqHeartbeatInfoResponse& result);
  virtual int req_start_log_id_by_ts_with_breakpoint(const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req_msg,
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& result);
  virtual int req_start_pos_by_log_id_with_breakpoint(
      const obrpc::ObLogReqStartPosByLogIdRequestWithBreakpoint& req_msg,
      obrpc::ObLogReqStartPosByLogIdResponseWithBreakpoint& result);
  virtual int handle_get_mc_ts_request(
      const obrpc::ObLogGetMCTsRequest& request_msg, obrpc::ObLogGetMCTsResponse& result);
  virtual int handle_get_mc_ctx_array_request(
      const obrpc::ObLogGetMcCtxArrayRequest& request_msg, obrpc::ObLogGetMcCtxArrayResponse& result);
  virtual int handle_get_priority_array_request(
      const obrpc::ObLogGetPriorityArrayRequest& request_msg, obrpc::ObLogGetPriorityArrayResponse& result);
  ObLogExecutor& get_log_executor()
  {
    return log_executor_;
  }
  virtual logservice::ObExtLogService* get_external_log_service()
  {
    return &external_log_service_;
  }
  int start();
  void stop();
  void wait();
  bool is_scan_finished() const;
  // for test
  int get_last_slide_log_id(const common::ObPartitionKey& partition_key, uint64_t& log_id);
  ObILogEngine* get_log_engine()
  {
    return &log_engine_;
  }
  virtual int get_need_freeze_partition_array(NeedFreezePartitionArray& partition_array) const;
  virtual uint32_t get_clog_min_using_file_id() const;
  virtual int get_clog_using_disk_space(int64_t& using_space) const override;
  virtual int get_ilog_using_disk_space(int64_t& using_space) const override;

  //===================== transaction one phase commit begin ================
public:
  virtual int batch_submit_log(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, const ObISubmitLogCbArray& cb_array);
  virtual int check_can_batch_submit(const common::ObPartitionArray& partition_array, bool& can_batch);
  virtual int batch_flush_cb(const transaction::ObTransID& trans_id, const ObLogCursor& base_log_cursor);
  virtual int batch_receive_log(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, const common::ObAddr& leader, const int64_t cluster_id);
  virtual int batch_ack_log(
      const transaction::ObTransID& trans_id, const common::ObAddr& server, const ObBatchAckArray& batch_ack_array);
  //===================== transaction one phase commit end ================

  // Query the accumulative checksum and submit_timestamp values corresponding to this log_id based on log_id.
  // use case:
  //     When minor freeze need obtain base_storage_info, only the log_id corresponding to the snapshot point is saved
  //     in the transaction context At this time, the accumulative checksum and submit_timetamp corresponding to this
  //     log_id need to be returned and saved in base_storage_info
  //
  //     accum_checksum and submit_timestamp are saved in ilog entry, so just need query ilog_storage.
  //     Since the minor freeze has not been completed at this time, the log must not be recycled, so this query must be
  //     success.
  //
  // ret code:
  // OB_SUCCESS
  // OB_EAGAIN                  Loading cusor_cache, need external retry
  // OB_ERR_UNEXPECTED          query log_id not exist, unexpected
  //
  // other code:
  // OB_NOT_INIT
  // OB_INVALID_ARGUMENT
  virtual int query_log_info_with_log_id(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      int64_t& accum_checksum, int64_t& submit_timestamp, int64_t& epoch_id);
  virtual int query_log_info_with_log_id(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      ObLogEntry& log_entry, int64_t& accum_checksum, bool& is_batch_committed);
  // **********************************************************
  // batch get valid_candidates
  virtual int get_candidates_array(const common::ObPartitionIArray& pkey_array,
      const common::ObAddrIArray& dst_server_list, common::ObSArray<common::ObAddrSArray>& candidate_mlist_array,
      common::ObSArray<obrpc::CandidateStatusList>& status_array) const;
  //===================== batch change member begin ================
public:
  virtual int batch_add_member(const obrpc::ObChangeMemberArgs& ctx_array, obrpc::ObChangeMemberCtxs& return_ctx_array);
  virtual int batch_remove_member(
      const obrpc::ObChangeMemberArgs& ctx_array, obrpc::ObChangeMemberCtxs& return_ctx_array);
  virtual int batch_is_member_change_done(obrpc::ObChangeMemberCtxs& return_ctx_array);
  virtual int batch_add_member(const common::ObPartitionArray& partition_array,
      const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
      common::ObReturnArray& ret_array, ObMCLogInfoArray& log_info_array);
  virtual int batch_remove_member(const common::ObPartitionArray& partition_array,
      const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
      common::ObReturnArray& ret_array, ObMCLogInfoArray& log_info_array);
  virtual int batch_is_member_change_done(const common::ObPartitionArray& partition_array,
      const ObMCLogInfoArray& log_info_array, common::ObReturnArray& ret_array);
  virtual int get_election_group_priority(const uint64_t tenant_id, election::ObElectionGroupPriority& priority) const;
  virtual int handle_get_remote_log_request(
      const obrpc::ObLogGetRemoteLogRequest& request_msg, obrpc::ObLogGetRemoteLogResponse& result);
  virtual ObLogCallbackEngine& get_cb_engine()
  {
    return cb_engine_;
  }
  // ==================== physical flashback =====================
  int delete_all_log_files();
  // ==================== log archive =====================
  int add_pg_archive_task(storage::ObIPartitionGroup* partition);
  int try_advance_restoring_clog();
  int delete_pg_archive_task(storage::ObIPartitionGroup* partition);
  int mark_log_archive_encount_fatal_err(
      const common::ObPartitionKey& pkey, const int64_t incarnation, const int64_t archive_round);
  int get_archive_pg_map(archive::PGArchiveMap*& map);
  bool is_server_archive_stop(const int64_t incarnation, const int64_t archive_round);

public:
  // clog callback thread num
  static int64_t CLOG_CB_THREAD_COUNT;
  static int64_t MINI_MODE_CLOG_CB_THREAD_COUNT;

private:
  static const int64_t OB_ARRAY_COUNT = 16;
  typedef common::ObSEArray<int64_t, OB_ARRAY_COUNT> McTimestampArray;
  typedef common::ObSEArray<common::ObMemberList, OB_ARRAY_COUNT> MemberListArray;
  typedef common::ObSEArray<common::ObProposalID, OB_ARRAY_COUNT> ProposalIDArray;
  typedef common::ObLinearHashMap<common::ObPartitionKey, int> ReturnMap;

  typedef common::ObSEArray<common::ObAddr, OB_ARRAY_COUNT> ServerArray;
  typedef common::ObLinearHashMap<common::ObAddr, common::ObPartitionArray> ServerPartitionMap;
  struct PartitionInfoCtx {
    PartitionInfoCtx()
    {
      reset();
    }
    void reset()
    {
      member_list_.reset();
      quorum_ = 0;
      mc_timestamp_ = 0;
      sync_num_ = 0;
    }

    common::ObMemberList member_list_;
    int64_t quorum_;
    int64_t mc_timestamp_;
    int64_t sync_num_;
    TO_STRING_KV(K(member_list_), K(quorum_), K(mc_timestamp_), K(sync_num_));
  };
  typedef common::ObLinearHashMap<common::ObAddr, McCtxArray> ServerMcCtxMap;
  typedef common::ObLinearHashMap<common::ObPartitionKey, PartitionInfoCtx> PartitionInfoCtxMap;

  int batch_check_partition_array_(const common::ObPartitionArray& partition_array);
  int batch_change_member_(const common::ObPartitionArray& partition_array, const common::ObMemberArray& member_array,
      const common::ObQuorumArray& quorum_array, const bool is_add_member, common::ObReturnArray& ret_array,
      ObMCLogInfoArray& log_info_array);
  int pre_batch_change_member_(const common::ObPartitionArray& partition_array,
      const common::ObQuorumArray& quorum_array, const bool is_add_member, McTimestampArray& mc_timestamp_array,
      MemberListArray& member_list_array, ProposalIDArray& proposal_id_array, ReturnMap& ret_map);
  int batch_check_remote_mc_ts_(const common::ObPartitionArray& partition_array,
      const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
      const McTimestampArray& mc_timestamp_array, const MemberListArray& member_list_array, ReturnMap& ret_map);
  int batch_set_member_changing_(const common::ObPartitionArray& partition_array,
      common::ObPartitionArray& need_reset_partition_array, ReturnMap& ret_map);
  int batch_wait_log_confirmed_(const common::ObPartitionArray& partition_array, ReturnMap& ret_map);
  int batch_check_remote_mc_ts_sync_(const common::ObPartitionArray& partition_array,
      const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
      const McTimestampArray& mc_timestamp_array, const MemberListArray& member_list_array, ReturnMap& ret_map);
  int internal_batch_check_remote_mc_ts_(const common::ObPartitionArray& partition_array,
      const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
      const McTimestampArray& mc_timestamp_array, const MemberListArray& member_list_array, ReturnMap& ret_map,
      const uint64_t follower_max_gap);
  int batch_execute_change_member_(const common::ObPartitionArray& partition_array,
      const common::ObMemberArray& member_array, const common::ObQuorumArray& quorum_array,
      const McTimestampArray& mc_timestamp_array, const ProposalIDArray& proposal_id_array, const bool is_add_member,
      ObMCLogInfoArray& log_info_array, ReturnMap& ret_map);
  int batch_block_partition_split_(const common::ObPartitionArray& partition_array, ReturnMap& ret_map);
  int batch_unblock_partition_split_(const common::ObPartitionArray& partition_array, ReturnMap& ret_map);
  int batch_reset_member_changing_(const common::ObPartitionArray& need_reset_partition_array);
  int rewrite_ret_value_(
      const common::ObPartitionArray& partition_array, const ReturnMap& ret_map, common::ObReturnArray& ret_array);
  int rewrite_ret_value_(const common::ObReturnArray& ret_array);
  bool is_partition_success_(const ReturnMap& ret_map, const common::ObPartitionKey& partition_key);

  int construct_server_array_(
      const MemberListArray& member_list_array, const common::ObMemberArray& member_array, ServerArray& server_array);
  bool is_server_contains_(const ServerArray& server_array, const common::ObAddr& addr);
  int construct_partition_info_ctx_map_(const common::ObPartitionArray& partition_array,
      const common::ObQuorumArray& quorum_array, const McTimestampArray& mc_timestamp_array,
      const MemberListArray& member_list_array, PartitionInfoCtxMap& partition_info_ctx_map);
  int construct_server_partition_map_(const ServerArray& server_array, const common::ObPartitionArray& partition_array,
      const common::ObMemberArray& member_array, const MemberListArray& member_list_array,
      ServerPartitionMap& server_partition_map);
  int construct_server_mc_ctx_map_(const ServerArray& server_array, const ServerPartitionMap& server_partition_map,
      ServerMcCtxMap& server_mc_ctx_map);
  int check_partition_remote_mc_ts_(const ObPartitionArray& partition_array, const ServerArray& server_array,
      PartitionInfoCtxMap& partition_info_ctx_map, const ServerMcCtxMap& server_mc_ctx_map,
      const int64_t follower_max_gap, ReturnMap& ret_map);
  int get_partition_max_log_id_(const common::ObPartitionKey& partiton_key, uint64_t& partition_max_log_id);
  //===================== batch change member end ================
private:
  int create_partition_(const common::ObPartitionKey& partition_key, const int64_t replica_num,
      const common::ObMemberList& member_list, const common::ObVersion& freeze_version,
      const common::ObReplicaType replica_type, const common::ObReplicaProperty replia_property,
      const int64_t last_submit_timestamp, const uint64_t last_replay_log_id, const int16_t archive_restore_state,
      const bool need_skip_mlist_check, ObIPartitionLogService* pls);
  int query_max_ilog_id_(const common::ObPartitionKey& pkey, uint64_t& ret_max_file_id);
  //===================== transaction one phase commit begin ================
private:
  int check_can_batch_submit_(const common::ObPartitionArray& partition_array, bool& can_batch);
  bool check_batch_submit_arguments_(const transaction::ObTransID& trans_id,
      const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array,
      const ObISubmitLogCbArray& cb_array);
  int batch_submit_log_(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, const ObISubmitLogCbArray& cb_array,
      const common::ObMemberList& member_list, const int64_t replica_num);
  int batch_flush_cb_(const transaction::ObTransID& trans_id, const ObLogCursor& base_log_cursor);
  int batch_ack_log_(
      const transaction::ObTransID& trans_id, const common::ObAddr& server, const ObBatchAckArray& batch_ack_array);
  int batch_receive_log_(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, const common::ObAddr& leader);
  int batch_submit_to_disk_(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& log_info_array, common::ObILogAllocator* alloc_mgr);
  int get_member_list_and_replica_num_(
      const common::ObPartitionArray& partition_array, common::ObMemberList& member_list, int64_t& replica_num);
  int leader_construct_log_info_(const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& orig_log_info_array, ObLogInfoArray& new_log_info_array, ObLogPersistSizeArray& size_array,
      common::ObILogAllocator*& alloc_mgr);
  int follower_construct_log_info_(const common::ObPartitionArray& partition_array,
      const ObLogInfoArray& orig_log_info_array, ObLogInfoArray& new_log_info_array, ObLogPersistSizeArray& size_array,
      common::ObILogAllocator*& alloc_mgr);
  int batch_submit_to_net_(const common::ObMemberList& member_list, const transaction::ObTransID& trans_id,
      const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array);
  int get_log_from_ilog_storage_(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, ObReadBuf& rbuf, ObLogEntry& log_entry);
  int check_can_receive_batch_log_(const common::ObPartitionKey& partition_key, const uint64_t log_id);
  int cleanup_log_info_array_(ObLogInfoArray& array, common::ObILogAllocator*& alloc_mgr);
  //===================== transaction one phase commit end ================
  int check_svr_in_member_list_(const ObPartitionKey& pkey, const ObAddr& server, bool& is_in_member_list) const;

private:
  bool is_inited_;
  bool is_running_;
  CLogMgrConfig clog_mgr_config_;
  ObLogEngine log_engine_;
  ObFetchLogEngine fetch_log_engine_;
  ObLogEventScheduler event_scheduler_;
  ObPartitionLogPacketHandler pkt_handler_;
  ObLogExecutor log_executor_;
  extlog::ObLogExternalExecutorWithBreakpoint log_ext_executor_v2_;
  logservice::ObExtLogService external_log_service_;
  storage::ObPartitionService* partition_service_;
  ObLogReplayEngineWrapper* replay_engine_;
  election::ObIElectionMgr* election_mgr_;
  common::ObAddr self_addr_;
  obrpc::ObLogRpcProxy* rpc_;
  ObLogScanRunnable scan_runnable_;
  ObLogStateDriverRunnable state_driver_runnable_;
  // ObLogReplayDriverRunnable replay_driver_runnable_;
  ObLogArchiveAndRestoreDriver log_archive_and_restore_driver_;
  ObLogCallbackHandler cb_handler_;
  ObLogCallbackThreadPool clog_thread_pool_;
  ObLogCallbackThreadPool sp_thread_pool_;
  ObLogCallbackEngine cb_engine_;
  ObBatchSubmitCtxMgr batch_submit_ctx_mgr_;
  ObRemoteLogQueryEngine remote_log_query_engine_;
  archive::ObArchiveMgr archive_mgr_;
  archive::ObArchiveRestoreEngine archive_restore_engine_;
  // Background thread
  ObCheckLogFileCollectTask check_log_file_collect_task_;
  ObCheckpointLogReplicaTask checkpoint_log_replica_task_;
  ObCheckCLogDiskFullTask check_clog_disk_full_task_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCLogMgr);
};

}  // namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_CLOG_MGR_
