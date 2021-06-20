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

#ifndef OCEANBASE_CLOG_OB_I_LOG_ENGINE_H_
#define OCEANBASE_CLOG_OB_I_LOG_ENGINE_H_

#include "share/ob_cascad_member_list.h"
#include "common/ob_idc.h"
#include "ob_log_common.h"
#include "ob_log_reader_interface.h"
#include "ob_i_disk_log_buffer.h"
#include "ob_info_block_handler.h"
#include "ob_log_req.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace clog {
class ObILogNetTask;
class ObConfirmedInfo;
class ObLogTask;
class ObCommitLogEnv;
class ObIndexLogEnv;
class ObLogCache;
class ObILogEngine {
public:
  typedef ObDiskBufferTask FlushTask;

public:
  ObILogEngine()
  {}
  virtual ~ObILogEngine()
  {}

public:
  virtual ObIRawLogIterator* alloc_raw_log_iterator(
      const file_id_t start_file_id, const file_id_t end_file_id, const offset_t offset, const int64_t timeout) = 0;
  virtual void revert_raw_log_iterator(ObIRawLogIterator* iter) = 0;
  virtual int read_log_by_location(const ObReadParam& param, ObReadBuf& buf, ObLogEntry& entry) = 0;

  virtual int read_log_by_location(const ObReadParam& param, ObReadBuf& buf, ObLogEntry& entry, ObReadCost& cost) = 0;
  virtual int read_log_by_location(const ObLogTask& log_task, ObReadBuf& buf, ObLogEntry& entry) = 0;
  virtual int get_clog_real_length(const ObReadParam& param, int64_t& real_length) = 0;
  virtual int read_data_from_hot_cache(
      const file_id_t want_file_id, const offset_t want_offset, const int64_t want_size, char* user_buf) = 0;
  virtual int read_data_direct(const ObReadParam& param, ObReadBuf& rbuf, ObReadRes& res, ObReadCost& cost) = 0;
  virtual int submit_flush_task(FlushTask* task) = 0;
  virtual int submit_net_task(const share::ObCascadMemberList& mem_list, const common::ObPartitionKey& key,
      const ObPushLogMode push_mode, ObILogNetTask* task) = 0;
  virtual int submit_fetch_log_resp(const common::ObAddr& server, const int64_t cluster_id,
      const common::ObPartitionKey& key, const int64_t network_limit, const ObPushLogMode push_mode,
      ObILogNetTask* task) = 0;
  virtual int submit_push_ms_log_req(const common::ObAddr& server, const int64_t cluster_id,
      const common::ObPartitionKey& key, ObILogNetTask* task) = 0;
  virtual int submit_fake_ack(const common::ObAddr& server, const common::ObPartitionKey& key, const uint64_t log_id,
      const common::ObProposalID proposal_id) = 0;
  virtual int submit_fake_push_log_req(const common::ObMemberList& member_list, const common::ObPartitionKey& key,
      const uint64_t log_id, const common::ObProposalID proposal_id) = 0;
  virtual int submit_log_ack(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const uint64_t log_id, const common::ObProposalID proposal_id) = 0;
  virtual int standby_query_sync_start_id(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const int64_t send_ts) = 0;
  virtual int submit_sync_start_id_resp(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const int64_t original_send_ts, const uint64_t sync_start_id) = 0;
  virtual int submit_standby_log_ack(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const uint64_t log_id, const common::ObProposalID proposal_id) = 0;
  virtual int submit_renew_ms_log_ack(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const uint64_t log_id, const int64_t submit_timestamp,
      const common::ObProposalID proposal_id) = 0;
  virtual int fetch_log_from_all_follower(const common::ObMemberList& mem_list, const common::ObPartitionKey& key,
      const uint64_t start_id, const uint64_t end_id, const common::ObProposalID proposal_id,
      const uint64_t max_confirmed_log_id) = 0;
  virtual int fetch_log_from_leader(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const ObFetchLogType fetch_type, const uint64_t start_id,
      const uint64_t end_id, const common::ObProposalID proposal_id, const common::ObReplicaType replica_type,
      const uint64_t max_confirmed_log_id) = 0;
  virtual int submit_check_rebuild_req(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const uint64_t start_id) = 0;
  virtual int fetch_register_server(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key,
      const common::ObRegion& region,  // region must be passed in, because primary cannot get standby servers' region
      const common::ObIDC& idc, const common::ObReplicaType replica_type, const int64_t next_replay_ts,
      const bool is_request_leader, const bool is_need_force_register) = 0;
  virtual int response_register_server(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const bool is_assign_parent_succeed,
      const share::ObCascadMemberList& candidate_list, const int32_t msg_type) = 0;
  virtual int request_replace_sick_child(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const common::ObAddr& sick_child) = 0;
  virtual int reject_server(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const int32_t msg_type) = 0;
  virtual int notify_restore_log_finished(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const uint64_t log_id) = 0;
  virtual int notify_reregister(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const share::ObCascadMember& new_leader) = 0;
  virtual int submit_prepare_rqst(const common::ObMemberList& mem_list, const common::ObPartitionKey& key,
      const common::ObProposalID proposal_id) = 0;
  virtual int submit_standby_prepare_rqst(const common::ObMemberList& mem_list, const common::ObPartitionKey& key,
      const common::ObProposalID proposal_id) = 0;
  virtual int broadcast_info(const common::ObMemberList& mem_list, const common::ObPartitionKey& key,
      const common::ObReplicaType& replica_type, const uint64_t max_confirmed_log_id) = 0;
  virtual int submit_confirmed_info(const share::ObCascadMemberList& mem_list, const common::ObPartitionKey& key,
      const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed) = 0;
  virtual int submit_renew_ms_confirmed_info(const share::ObCascadMemberList& mem_list,
      const common::ObPartitionKey& key, const uint64_t log_id, const common::ObProposalID& ms_proposal_id,
      const ObConfirmedInfo& confirmed_info) = 0;
  virtual int prepare_response(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& partition_key, const common::ObProposalID proposal_id, const uint64_t max_log_id,
      const int64_t max_log_ts) = 0;
  virtual int standby_prepare_response(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& partition_key, const common::ObProposalID proposal_id, const uint64_t ms_log_id,
      const int64_t membership_version, const common::ObMemberList& member_list) = 0;
  virtual int send_keepalive_msg(const common::ObAddr& server, const int64_t cluster_id,
      const common::ObPartitionKey& partition_key, const uint64_t next_log_id, const int64_t next_log_ts_lb,
      const uint64_t deliver_cnt) = 0;
  virtual int send_restore_alive_msg(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& partition_key, const uint64_t start_log_id) = 0;
  virtual int send_restore_alive_req(const common::ObAddr& server, const common::ObPartitionKey& partition_key) = 0;
  virtual int send_restore_alive_resp(
      const common::ObAddr& server, const int64_t dst_cluster_id, const common::ObPartitionKey& partition_key) = 0;
  virtual int notify_restore_leader_takeover(const common::ObAddr& server, const common::ObPartitionKey& key) = 0;
  virtual int send_leader_max_log_msg(const common::ObAddr& server, const int64_t cluster_id,
      const common::ObPartitionKey& partition_key, const int64_t switchover_epoch, const uint64_t max_log_id,
      const int64_t next_log_ts) = 0;
  virtual int send_sync_log_archive_progress_msg(const common::ObAddr& server, const int64_t cluster_id,
      const common::ObPartitionKey& partition_key, const ObPGLogArchiveStatus& status) = 0;
  virtual int notify_follower_log_missing(const common::ObAddr& server, const int64_t cluster_id,
      const common::ObPartitionKey& partition_key, const uint64_t start_log_id, const bool is_in_member_list,
      const int32_t msg_type) = 0;
  virtual void update_clog_info(const int64_t max_submit_timestamp) = 0;
  virtual void update_clog_info(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, const int64_t submit_timestamp) = 0;
  virtual int reset_clog_info_block() = 0;
  virtual int get_clog_info_handler(const file_id_t file_id, ObCommitInfoBlockHandler& handler) = 0;
  virtual int get_remote_membership_status(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& partition_key, int64_t& timestamp, uint64_t& max_confirmed_log_id,
      bool& remote_replica_is_normal) = 0;
  virtual int64_t get_free_quota() const = 0;
  virtual bool is_disk_space_enough() const = 0;
  virtual int submit_batch_log(const common::ObMemberList& member_list, const transaction::ObTransID& trans_id,
      const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array) = 0;
  virtual int submit_batch_ack(
      const common::ObAddr& leader, const transaction::ObTransID& trans_id, const ObBatchAckArray& batch_ack_array) = 0;
  virtual int query_remote_log(const common::ObAddr& server, const common::ObPartitionKey& partition_key,
      const uint64_t log_id, transaction::ObTransID& trans_id, int64_t& submit_timestamp) = 0;
  virtual int get_clog_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id) = 0;
  virtual uint32_t get_clog_min_using_file_id() const = 0;
  virtual uint32_t get_clog_min_file_id() const = 0;
  virtual uint32_t get_clog_max_file_id() const = 0;
  // ================== interface for ObIlogStorage begin====================
  virtual int get_cursor_batch(
      const common::ObPartitionKey& pkey, const uint64_t query_log_id, ObGetCursorResult& result) = 0;
  virtual int get_cursor_batch(const common::ObPartitionKey& pkey, const uint64_t query_log_id,
      ObLogCursorExt& log_cursor, ObGetCursorResult& result, uint64_t& cursor_start_log_id) = 0;
  virtual int get_cursor_batch_from_file(
      const common::ObPartitionKey& pkey, const uint64_t query_log_id, ObGetCursorResult& result) = 0;
  virtual int get_cursor(
      const common::ObPartitionKey& pkey, const uint64_t query_log_id, ObLogCursorExt& log_cursor_ext) = 0;
  virtual int submit_cursor(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, const ObLogCursorExt& log_cursor_ext) = 0;
  virtual int submit_cursor(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      const ObLogCursorExt& log_cursor_ext, const common::ObMemberList& memberlist, const int64_t replica_num,
      const int64_t memberlist_version) = 0;
  virtual int query_max_ilog_id(const common::ObPartitionKey& pkey, uint64_t& ret_max_ilog_id) = 0;
  virtual int query_max_flushed_ilog_id(const common::ObPartitionKey& pkey, uint64_t& ret_max_ilog_id) = 0;
  virtual int get_ilog_memstore_min_log_id_and_ts(
      const common::ObPartitionKey& pkey, uint64_t& min_log_id, int64_t& min_log_ts) = 0;
  virtual int locate_by_timestamp(const common::ObPartitionKey& pkey, const int64_t start_ts, uint64_t& target_log_id,
      int64_t& target_log_timestamp) = 0;
  virtual int locate_ilog_file_by_log_id(
      const common::ObPartitionKey& pkey, const uint64_t start_log_id, uint64_t& end_log_id, file_id_t& ilog_id) = 0;
  virtual int fill_file_id_cache() = 0;
  virtual int ensure_log_continuous_in_file_id_cache(
      const common::ObPartitionKey& partition_key, const uint64_t log_id) = 0;
  virtual int get_ilog_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id) = 0;
  virtual int query_next_ilog_file_id(file_id_t& next_ilog_file_id) = 0;
  virtual int get_index_info_block_map(const file_id_t file_id, IndexInfoBlockMap& index_info_block_map) = 0;
  virtual int check_need_block_log(bool& is_need) const = 0;

  // want_size refers to the length in clog, which may be the length after compression, and the returned data is after
  // decompression
  virtual int read_uncompressed_data_from_hot_cache(const common::ObAddr& addr, const int64_t seq,
      const file_id_t want_file_id, const offset_t want_offset, const int64_t want_size, char* user_buf,
      const int64_t buf_size, int64_t& origin_data_len) = 0;
  virtual ObLogCache* get_ilog_log_cache() = 0;

  virtual int check_is_clog_obsoleted(const common::ObPartitionKey& partition_key, const file_id_t file_id,
      const offset_t offset, bool& is_obsoleted) const = 0;

  virtual bool is_clog_disk_error() const = 0;
  // ================== interface for ObIlogStorage end  ====================
};

class ObILogNetTask {
public:
  virtual ~ObILogNetTask()
  {}
  virtual const char* get_data_buffer() const = 0;
  virtual common::ObProposalID get_proposal_id() const = 0;
  virtual int64_t get_data_len() const = 0;
};

class ObLogNetTask : public ObILogNetTask {
public:
  ObLogNetTask(common::ObProposalID proposal_id, const char* buff, int64_t data_len)
      : proposal_id_(proposal_id), buff_(buff), data_len_(data_len)
  {}
  const char* get_data_buffer() const
  {
    return buff_;
  }
  common::ObProposalID get_proposal_id() const
  {
    return proposal_id_;
  }
  int64_t get_data_len() const
  {
    return data_len_;
  }

private:
  common::ObProposalID proposal_id_;
  const char* buff_;
  int64_t data_len_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogNetTask);
};
}  // end namespace clog
}  // end namespace oceanbase

#endif /* OCEANBASE_CLOG_OB_I_LOG_ENGINE_H_ */
