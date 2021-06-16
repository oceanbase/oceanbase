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

#ifndef OCEANBASE_CLOG_OB_LOG_ENGINE_
#define OCEANBASE_CLOG_OB_LOG_ENGINE_
#include "lib/lock/ob_drw_lock.h"
#include "lib/stat/ob_latch_define.h"
#include "share/ob_cascad_member.h"
#include "share/ob_thread_pool.h"
#include "share/ob_i_ps_cb.h"
#include "ob_i_log_engine.h"
#include "ob_disk_log_buffer.h"
#include "ob_info_block_handler.h"
#include "ob_log_req.h"
#include "ob_log_type.h"
#include "ob_clog_writer.h"
#include "ob_raw_entry_iterator.h"
#include "ob_clog_sync_msg.h"
#include "ob_ilog_storage.h"
#include "election/ob_election_priority.h"

namespace oceanbase {
namespace obrpc {
class ObBatchRpc;
class ObLogRpcProxy;
}  // namespace obrpc
namespace common {
class ObILogFileStore;
}
namespace clog {
class ObLogTask;
class ObTailCursor;
class ObTailHelper;
class ObLogEngineAccessor;
class ObIlogInfoBlockReader;
class ObBatchSubmitDiskTask;
class ObCLogBaseFileWriter;

class ObHotCacheWarmUpHelper {
public:
  static int warm_up(ObLogDirectReader& reader, ObTailCursor& tail, ObLogCache& cache);
  // for unittest
  static void test_warm_up(ObLogCache& cache)
  {
    cache.hot_cache_.base_offset_ = 0;
    cache.hot_cache_.head_offset_ = 0;
    cache.hot_cache_.tail_offset_ = 0;
    cache.hot_cache_.is_inited_ = true;
  }
};

class ObLogEnv {
public:
  struct Config {
    Config() : log_dir_(NULL), index_log_dir_(NULL), cache_name_(NULL)
    {
      reset();
    }
    ~Config()
    {}
    void reset();
    bool is_valid() const;
    const char* log_dir_;
    const char* index_log_dir_;
    const char* log_shm_path_;
    const char* index_log_shm_path_;
    const char* cache_name_;
    const char* index_cache_name_;
    int64_t cache_priority_;
    int64_t index_cache_priority_;
    int64_t file_size_;
    int64_t read_timeout_;
    int64_t write_timeout_;
    int64_t write_queue_size_;
    int64_t disk_log_buffer_cnt_;
    int64_t disk_log_buffer_size_;
    int64_t ethernet_speed_;
    TO_STRING_KV(K_(log_dir), K_(index_log_dir), K_(log_shm_path), K_(index_log_shm_path), K_(cache_name),
        K_(index_cache_name), K_(cache_priority), K_(index_cache_priority), K_(file_size), K_(read_timeout),
        K_(write_timeout), K_(disk_log_buffer_size), K_(disk_log_buffer_cnt), K_(ethernet_speed));
  };

public:
  ObLogEnv()
      : is_inited_(false),
        log_file_writer_(NULL),
        file_store_(NULL),
        partition_service_(NULL),
        min_start_file_id_(OB_INVALID_FILE_ID),
        tail_locator_(NULL)
  {}
  virtual ~ObLogEnv();
  int init(const Config& cfg, const common::ObAddr& self_addr, ObIInfoBlockHandler* info_block_handler,
      ObLogTailLocator* tail_locator, ObICallbackHandler* callback_handler,
      storage::ObPartitionService* partition_service, const char* cache_name, const int64_t hot_cache_size,
      const file_id_t start_file_id, const int64_t disk_buffer_size, const ObLogWritePoolType write_pool_type,
      const bool enable_log_cache);
  int start(file_id_t& start_file_id, offset_t& offset);
  int start();
  int set_min_using_file_id(const file_id_t file_id);
  void stop();
  void wait();
  void destroy();

public:
  ObLogDirectReader& get_reader()
  {
    return direct_reader_;
  }
  const ObCLogWriter& get_writer() const
  {
    return log_writer_;
  }
  ObDiskLogBuffer& get_disk_log_buffer()
  {
    return disk_log_buffer_;
  }
  int warm_up_cache(ObLogDirectReader& reader, ObTailCursor& tail, ObLogCache& cache);
  int64_t get_free_quota() const;
  int update_free_quota();
  int check_is_clog_writer_congested(bool& is_congested) const;
  uint32_t get_min_using_file_id() const;
  void try_recycle_file();
  virtual int get_using_disk_space(int64_t& using_space) const = 0;
  virtual int get_total_disk_space(int64_t& total_space) const;
  uint32_t get_min_file_id() const;
  uint32_t get_max_file_id() const;

public:
  VIRTUAL_TO_STRING_KV(K_(is_inited), KP_(partition_service));

protected:
  // When the number of flashing items accounted for the percentage of max_buffer_item_cnt_ exceeds this value,
  // the clog_writer is returned to be busy
  static const int64_t BUFFER_ITEM_CONGESTED_PERCENTAGE = 50;
  bool cluster_version_before_2000_() const;
  int init_log_file_writer(const char* log_dir, const char* shm_path, const ObILogFileStore* file_store);

  bool is_inited_;
  Config config_;
  ObCLogBaseFileWriter* log_file_writer_;
  ObLogCache log_cache_;
  ObILogFileStore* file_store_;
  ObLogDirectReader direct_reader_;
  ObTailCursor log_tail_;
  ObCLogWriter log_writer_;
  ObDiskLogBuffer disk_log_buffer_;
  storage::ObPartitionService* partition_service_;
  file_id_t min_start_file_id_;
  ObLogTailLocator* tail_locator_;
};

class ObCommitLogEnv : public ObLogEnv {
public:
  ObCommitLogEnv()
  {
    preread_file_id_ = 0;
  }
  virtual ~ObCommitLogEnv()
  {}
  int init(const ObLogEnv::Config& cfg, const common::ObAddr& self_addr, common::ObICallbackHandler* callback_handler,
      storage::ObPartitionService* partition_service);
  void destroy();
  ObCommitInfoBlockHandler& get_info_block_handler()
  {
    return info_block_handler_;
  }
  int update_min_using_file_id(NeedFreezePartitionArray& partition_array, const bool need_freeze_based_on_used_space);
  int get_log_info_block_reader(ObILogInfoBlockReader*& reader);
  void revert_log_info_block_reader(ObILogInfoBlockReader* reader);
  int get_using_disk_space(int64_t& used_space) const;
  ObILogFileStore* get_log_file_store() const
  {
    return file_store_;
  }
  int get_min_file_mtime(time_t& min_clog_mtime);

private:
  common::ObLinearHashMap<common::ObPartitionKey, bool> partition_hash_map_;
  int preread_freeze_array_(const file_id_t file_id, const file_id_t max_file_id, ObILogInfoBlockReader* reader,
      NeedFreezePartitionArray& partition_array);
  int append_freeze_array_(
      NeedFreezePartitionArray& partition_array, const NeedFreezePartitionArray& tmp_partition_array);
  int get_min_file_mtime_(time_t& min_clog_mtime);

protected:
  ObCommitInfoBlockHandler info_block_handler_;
  // Provide pre-reading function for triggering minor freeze on demand, and trigger minor freeze earlier
  static const int64_t PREREAD_FILE_NUM = 256;
  NeedFreezePartitionArray freeze_array_[PREREAD_FILE_NUM];
  file_id_t preread_file_id_;
};

class NetworkLimitManager {
public:
  NetworkLimitManager();
  ~NetworkLimitManager();

public:
  int init(const int64_t ethernet_speed);
  void destroy();
  int get_limit(const common::ObAddr& addr, int64_t& network_limit);
  int get_limit(const common::ObMemberList& member_list, int64_t& network_limit);
  int try_limit(const common::ObAddr& addr, const int64_t size, const int64_t network_limit);

private:
  struct HashV {
    void reset()
    {
      aggregate_size_ = 0;
      last_reset_ts_ = common::ObTimeUtility::current_time();
    }
    int64_t aggregate_size_;
    int64_t last_reset_ts_;
  };
  class CheckFunction {
  public:
    CheckFunction(const int64_t size, const int64_t limit) : size_(size), limit_(limit)
    {}
    ~CheckFunction()
    {}

  public:
    bool operator()(const common::ObAddr& addr, HashV& value)
    {
      UNUSED(addr);
      bool bool_ret = false;
      if (ObTimeUtility::current_time() - value.last_reset_ts_ >= RESET_NETWORK_LIMIT_INTERVAL) {
        value.last_reset_ts_ = ObTimeUtility::current_time();
        value.aggregate_size_ = 0;
      }

      if (size_ + value.aggregate_size_ > limit_) {
        bool_ret = false;
      } else {
        bool_ret = true;
        value.aggregate_size_ += size_;
      }

      return bool_ret;
    }

  private:
    int64_t size_;
    int64_t limit_;
  };

private:
  static const int64_t RESET_ADDR_ARRAY_INTERVAL = 120 * 1000 * 1000;  // 2 minutes
  static const int64_t RESET_NETWORK_LIMIT_INTERVAL = 100 * 1000;      // 100ms
  static const int64_t SLEEP_TS = 1 * 1000;                            // 1ms
  static const int64_t NET_PERCENTAGE = 60;
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  bool addr_contains_(const common::ObAddr& addr) const;
  bool addr_contains_(const common::ObMemberList& member_list) const;
  int update_addr_array_(const common::ObMemberList& member_list);
  void try_reset_addr_array_();

private:
  bool is_inited_;
  RWLock lock_;  // only protect addr_array_;
  ObAddrArray addr_array_;
  int64_t ethernet_speed_;
  common::ObLinearHashMap<common::ObAddr, HashV> hash_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(NetworkLimitManager);
};

class ObLogRpcStat {
public:
  ObLogRpcStat()
  {
    reset_();
  }
  ~ObLogRpcStat()
  {}

public:
  void stat(const enum ObLogReqType& type)
  {
    if (type >= 0 && type < OB_LOG_MAX_REQ_TYPE_ID) {
      ATOMIC_INC(&cnt_[type]);
      if (REACH_TIME_INTERVAL(600 * 1000 * 1000)) {
        print_();
        reset_();
      }
    }
  }

private:
  void reset_()
  {
    for (int64_t i = 0; i < OB_LOG_MAX_REQ_TYPE_ID; i++) {
      ATOMIC_STORE(&cnt_[i], 0);
    }
  }
  void print_()
  {
    const int64_t NUM_PER_LINE = 5;
    const int64_t lines = OB_LOG_MAX_REQ_TYPE_ID / NUM_PER_LINE;
    const int64_t remainder = OB_LOG_MAX_REQ_TYPE_ID % NUM_PER_LINE;
    for (int64_t i = 0; i < lines; i++) {
      CLOG_LOG(INFO,
          "log rpc stat",
          "req_id",
          i * NUM_PER_LINE,
          "cnt",
          cnt_[i * NUM_PER_LINE],
          "req_id",
          i * NUM_PER_LINE + 1,
          "cnt",
          cnt_[i * NUM_PER_LINE + 1],
          "req_id",
          i * NUM_PER_LINE + 2,
          "cnt",
          cnt_[i * NUM_PER_LINE + 2],
          "req_id",
          i * NUM_PER_LINE + 3,
          "cnt",
          cnt_[i * NUM_PER_LINE + 3],
          "req_id",
          i * NUM_PER_LINE + 4,
          "cnt",
          cnt_[i * NUM_PER_LINE + 4]);
    }
    for (int64_t i = 0; i < remainder; i++) {
      CLOG_LOG(INFO, "log rpc stat", "req_id", NUM_PER_LINE * lines + i, "cnt", cnt_[NUM_PER_LINE * lines + i]);
    }
  }

private:
  int64_t cnt_[OB_LOG_MAX_REQ_TYPE_ID + 10];
};

class ObLogEngine : public ObILogEngine, public share::ObThreadPool {
  // TODO private/public function
  friend class ObLogEngineAccessor;  // for clog perf test
public:
  ObLogEngine() : is_inited_(false), batch_rpc_(NULL), rpc_(NULL)
  {}
  virtual ~ObLogEngine()
  {
    destroy();
  }
  int init(const ObLogEnv::Config& cfg, const common::ObAddr& self_addr, obrpc::ObBatchRpc* batch_rpc,
      obrpc::ObLogRpcProxy* rpc, common::ObICallbackHandler* callback_handler,
      storage::ObPartitionService* partition_service);
  int start() override;
  void stop() override;
  void wait() override;
  void destroy();
  void run1() override;
  ObIRawLogIterator* alloc_raw_log_iterator(const file_id_t start_file_id, const file_id_t end_file_id,
      const offset_t offset, const int64_t timeout) override;
  void revert_raw_log_iterator(ObIRawLogIterator* iter) override;
  int read_log_by_location(const ObReadParam& param, ObReadBuf& buf, ObLogEntry& entry) override;
  int read_log_by_location(const ObLogTask& log_task, ObReadBuf& buf, ObLogEntry& entry) override;
  int read_log_by_location(const ObReadParam& param, ObReadBuf& buf, ObLogEntry& entry, ObReadCost& cost) override;
  int get_clog_real_length(const ObReadParam& param, int64_t& real_length) override;
  // read clog from hot cache
  int read_data_from_hot_cache(
      const file_id_t want_file_id, const offset_t want_offset, const int64_t want_size, char* user_buf) override;

  // want_size refers to the length in clog, which may be the length after compression,
  // and the returned data is after decompression
  int read_uncompressed_data_from_hot_cache(const common::ObAddr& addr, const int64_t seq, const file_id_t want_file_id,
      const offset_t want_offset, const int64_t want_size, char* user_buf, const int64_t buf_size,
      int64_t& origin_data_len) override;
  // read clog from disk directly
  int read_data_direct(const ObReadParam& param, ObReadBuf& rbuf, ObReadRes& res, ObReadCost& cost) override;
  int submit_flush_task(FlushTask* task) override;
  int submit_flush_task(ObBatchSubmitDiskTask* task);
  int submit_net_task(const share::ObCascadMemberList& mem_list, const common::ObPartitionKey& key,
      const ObPushLogMode push_mode, ObILogNetTask* task) override;
  int submit_fetch_log_resp(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const int64_t network_limit, const ObPushLogMode push_mode,
      ObILogNetTask* task) override;
  int submit_push_ms_log_req(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, ObILogNetTask* task) override;
  int submit_fake_ack(const common::ObAddr& server, const common::ObPartitionKey& key, const uint64_t log_id,
      const ObProposalID proposal_id) override;
  int submit_fake_push_log_req(const common::ObMemberList& member_list, const common::ObPartitionKey& key,
      const uint64_t log_id, const ObProposalID proposal_id) override;
  int submit_log_ack(const common::ObAddr& server, const int64_t dst_cluster_id, const common::ObPartitionKey& key,
      const uint64_t log_id, const common::ObProposalID proposal_id) override;
  int standby_query_sync_start_id(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const int64_t send_ts) override;
  int submit_sync_start_id_resp(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const int64_t original_send_ts, const uint64_t sync_start_id) override;
  int submit_standby_log_ack(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const uint64_t log_id, const ObProposalID proposal_id) override;
  int submit_renew_ms_log_ack(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const uint64_t log_id, const int64_t submit_timestamp,
      const common::ObProposalID proposal_id) override;
  // send fetch log request to all followers
  int fetch_log_from_all_follower(const common::ObMemberList& mem_list, const common::ObPartitionKey& key,
      const uint64_t start_id, const uint64_t end_id, const common::ObProposalID proposal_id,
      const uint64_t max_confirmed_log_id) override;
  int fetch_log_from_leader(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const ObFetchLogType fetch_type, const uint64_t start_id,
      const uint64_t end_id, const common::ObProposalID proposal_id, const common::ObReplicaType replica_type,
      const uint64_t max_confirmed_log_id) override;
  int submit_check_rebuild_req(const common::ObAddr& server, const int64_t dst_cluster_id, const ObPartitionKey& key,
      const uint64_t start_id) override;
  int fetch_register_server(const common::ObAddr& server, const int64_t dst_cluster_id, const ObPartitionKey& key,
      const common::ObRegion& region, const common::ObIDC& idc, const common::ObReplicaType replica_type,
      const int64_t next_replay_log_ts, const bool is_request_leader, const bool is_need_force_register) override;
  int response_register_server(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const bool is_assign_parent_succeed,
      const share::ObCascadMemberList& candidate_list, const int32_t msg_type) override;
  int request_replace_sick_child(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const common::ObAddr& sick_child) override;
  int reject_server(const common::ObAddr& server, const int64_t dst_cluster_id, const common::ObPartitionKey& key,
      const int32_t msg_type) override;
  int notify_restore_log_finished(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& key, const uint64_t log_id) override;
  int notify_reregister(const common::ObAddr& server, const int64_t dst_cluster_id, const common::ObPartitionKey& key,
      const share::ObCascadMember& new_leader) override;
  int submit_prepare_rqst(const common::ObMemberList& mem_list, const common::ObPartitionKey& key,
      const common::ObProposalID proposal_id) override;
  int submit_standby_prepare_rqst(const common::ObMemberList& mem_list, const common::ObPartitionKey& key,
      const common::ObProposalID proposal_id) override;
  int broadcast_info(const common::ObMemberList& mem_list, const common::ObPartitionKey& key,
      const common::ObReplicaType& replica_type, const uint64_t max_confirmed_log_id) override;
  // confirmed_info msg is special that no need compare proposal_id
  int submit_confirmed_info(const share::ObCascadMemberList& mem_list, const common::ObPartitionKey& key,
      const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed) override;
  int submit_renew_ms_confirmed_info(const share::ObCascadMemberList& mem_list, const common::ObPartitionKey& key,
      const uint64_t log_id, const common::ObProposalID& ms_proposal_id,
      const ObConfirmedInfo& confirmed_info) override;
  int prepare_response(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& partition_key, const common::ObProposalID proposal_id, const uint64_t max_log_id,
      const int64_t max_log_ts) override;
  int standby_prepare_response(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& partition_key, const ObProposalID proposal_id, const uint64_t ms_log_id,
      const int64_t membership_version, const common::ObMemberList& member_list) override;
  int send_keepalive_msg(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& partition_key, const uint64_t next_log_id, const int64_t next_log_ts_lb,
      const uint64_t deliver_cnt) override;
  int send_restore_alive_msg(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& partition_key, const uint64_t start_log_id) override;
  int send_restore_alive_req(const common::ObAddr& server, const common::ObPartitionKey& partition_key) override;
  int send_restore_alive_resp(
      const common::ObAddr& server, const int64_t dst_cluster_id, const common::ObPartitionKey& partition_key) override;
  int notify_restore_leader_takeover(const common::ObAddr& server, const common::ObPartitionKey& key) override;
  int send_leader_max_log_msg(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& partition_key, const int64_t switchover_epoch, const uint64_t max_log_id,
      const int64_t next_log_ts) override;
  int send_sync_log_archive_progress_msg(const common::ObAddr& server, const int64_t cluster_id,
      const common::ObPartitionKey& partition_key, const ObPGLogArchiveStatus& status) override;
  virtual int notify_follower_log_missing(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& partition_key, const uint64_t start_log_id, const bool is_in_member_list,
      const int32_t msg_type) override;
  virtual void update_clog_info(const int64_t max_submit_timestamp) override;
  virtual void update_clog_info(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, const int64_t submit_timestamp) override;
  int reset_clog_info_block() override;
  int get_clog_info_handler(const file_id_t file_id, ObCommitInfoBlockHandler& handler) override;
  int get_remote_membership_status(const common::ObAddr& server, const int64_t dst_cluster_id,
      const common::ObPartitionKey& partition_key, int64_t& timestamp, uint64_t& max_confirmed_log_id,
      bool& remote_replica_is_normal) override;
  int get_remote_mc_ctx_array(
      const common::ObAddr& server, const common::ObPartitionArray& partition_array, McCtxArray& mc_ctx_array);
  int update_min_using_file_id();
  uint32_t get_clog_min_using_file_id() const override;
  uint32_t get_clog_min_file_id() const override;
  uint32_t get_clog_max_file_id() const override;
  int64_t get_free_quota() const override;
  bool is_disk_space_enough() const override;
  void try_recycle_file();
  int get_need_freeze_partition_array(NeedFreezePartitionArray& partition_array) const;
  virtual int submit_batch_log(const common::ObMemberList& member_list, const transaction::ObTransID& trans_id,
      const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array) override;
  virtual int submit_batch_ack(const common::ObAddr& leader, const transaction::ObTransID& trans_id,
      const ObBatchAckArray& batch_ack_array) override;
  int get_remote_priority_array(const common::ObAddr& server, const common::ObPartitionIArray& partition_array,
      election::PriorityArray& priority_array) const;
  virtual int query_remote_log(const common::ObAddr& server, const common::ObPartitionKey& partition_key,
      const uint64_t log_id, transaction::ObTransID& trans_id, int64_t& submit_timestamp) override;
  int get_clog_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id) override;
  int delete_all_clog_files();
  // ================== interface for ObIlogStorage begin====================
  int get_cursor_batch(
      const common::ObPartitionKey& pkey, const uint64_t query_log_id, ObGetCursorResult& result) override;
  int get_cursor_batch(const common::ObPartitionKey& pkey, const uint64_t query_log_id, ObLogCursorExt& log_cursor,
      ObGetCursorResult& result, uint64_t& cursor_start_log_id) override;
  int get_cursor_batch_from_file(
      const common::ObPartitionKey& pkey, const uint64_t query_log_id, ObGetCursorResult& result) override;
  int get_cursor(const common::ObPartitionKey& pkey, const uint64_t query_log_id, ObLogCursorExt& log_cursor) override;
  int submit_cursor(
      const common::ObPartitionKey& pkey, const uint64_t log_id, const ObLogCursorExt& log_cursor_ext) override;
  int submit_cursor(const common::ObPartitionKey& partition_key, const uint64_t log_id,
      const ObLogCursorExt& log_cursor_ext, const common::ObMemberList& memberlist, const int64_t replica_num,
      const int64_t memberlist_version) override;
  int query_max_ilog_id(const common::ObPartitionKey& pkey, uint64_t& ret_max_ilog_id) override;
  int query_max_flushed_ilog_id(const common::ObPartitionKey& pkey, uint64_t& ret_max_ilog_id) override;
  int get_ilog_memstore_min_log_id_and_ts(
      const common::ObPartitionKey& pkey, uint64_t& min_log_id, int64_t& min_log_ts) override;
  int get_ilog_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id) override;
  int query_next_ilog_file_id(file_id_t& next_ilog_file_id) override;
  int locate_by_timestamp(const common::ObPartitionKey& pkey, const int64_t start_ts, uint64_t& target_log_id,
      int64_t& target_log_timestamp) override;
  int locate_ilog_file_by_log_id(const common::ObPartitionKey& pkey, const uint64_t start_log_id, uint64_t& end_log_id,
      file_id_t& ilog_id) override;
  int fill_file_id_cache() override;
  int ensure_log_continuous_in_file_id_cache(
      const common::ObPartitionKey& partition_key, const uint64_t log_id) override;
  int get_index_info_block_map(const file_id_t file_id, IndexInfoBlockMap& index_info_block_map) override;
  int check_need_block_log(bool& is_need) const override;
  int delete_all_ilog_files();
  ObLogCache* get_ilog_log_cache() override
  {
    return is_inited_ ? &ilog_log_cache_ : NULL;
  }

  int check_is_clog_obsoleted(const common::ObPartitionKey& partition_key, const file_id_t file_id,
      const offset_t offset, bool& is_obsoleted) const override;
  // ================== interface for ObIlogStorage end  ====================
  int get_clog_using_disk_space(int64_t& space) const;
  int get_ilog_using_disk_space(int64_t& space) const;
  bool is_clog_disk_error() const override;

private:
  int fetch_log_from_server(
      const common::ObAddr& server, const common::ObPartitionKey& key, const uint64_t start_id, const uint64_t end_id);
  template <typename Req>
  int post_packet(const uint64_t tenant_id, const common::ObAddr& server, const common::ObPartitionKey& key,
      ObLogReqType type, Req& req);
  template <typename Req>
  int post_packet(const uint64_t tenant_id, const common::ObAddr& server, const int64_t cluster_id,
      const common::ObPartitionKey& key, ObLogReqType type, Req& req);
  template <typename Req>
  int post_packet(const common::ObMemberList& mem_list, const common::ObPartitionKey& key, ObLogReqType type, Req& req);
  template <typename Req>
  int post_packet(
      const share::ObCascadMemberList& mem_list, const common::ObPartitionKey& key, ObLogReqType type, Req& req);

  ObILogInfoBlockReader* get_clog_info_block_reader();
  void revert_clog_info_block_reader(ObILogInfoBlockReader* reader);
  int update_free_quota();
  inline const ObCommitLogEnv* get_clog_env_() const
  {
    return &clog_env_;
  }
  inline ObCommitLogEnv* get_clog_env_()
  {
    return &clog_env_;
  }
  int set_need_freeze_partition_array_(const NeedFreezePartitionArray& partition_array);
  int check_need_freeze_based_on_used_space_(bool& is_need) const;
  void get_dst_list_(const share::ObCascadMemberList& mem_list, share::ObCascadMemberList& dst_list) const;
  int delete_file_(const char* name);

private:
  static const int64_t PREPARE_CACHE_FILE_INTERVAL = 100 * 1000;
  static const int64_t RESERVED_DISK_USAGE_PERFERT = 80;

private:
  bool is_inited_;
  // instance for clog
  ObCommitLogEnv clog_env_;
  ObLogCache ilog_log_cache_;
  ObIlogStorage ilog_storage_;
  // common
  obrpc::ObBatchRpc* batch_rpc_;
  obrpc::ObLogRpcProxy* rpc_;

  mutable common::ObSpinLock need_freeze_partition_array_lock_;
  NeedFreezePartitionArray need_freeze_partition_array_;
  ObLogEnv::Config cfg_;
  NetworkLimitManager network_limit_manager_;
  ObLogRpcStat rpc_stat_;
  DISALLOW_COPY_AND_ASSIGN(ObLogEngine);
};
};  // end namespace clog
};  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_ENGINE_
