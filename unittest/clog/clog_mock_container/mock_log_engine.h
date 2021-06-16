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

#ifndef OCEANBASE_UNITTEST_CLOG_MOCK_CONTAINER_MOCK_OB_LOG_ENGINE_H_
#define OOCEANBASE_UNITTEST_CLOG_MOCK_CONTAINER_MOCK_OB_LOG_ENGINE_H_
#include "clog/ob_i_disk_log_buffer.h"
#include "common/ob_member_list.h"
#include "lib/net/ob_addr.h"
#include "common/ob_partition_key.h"
#include "clog/ob_log_reader_interface.h"
#include "clog/ob_i_log_engine.h"

namespace oceanbase {
namespace clog {
class ObIDiskLogBuffer;
class ObINetLogBufferMgr;
class ObILogNetTask;
class ObConfirmedInfo;
class ObLogTask;
class MockObLogEngine : public ObILogEngine {
public:
  typedef ObDiskBufferTask FlushTask;

public:
  MockObLogEngine()
  {}
  ~MockObLogEngine()
  {}
  ObIRawLogIterator* alloc_raw_log_iterator(
      const file_id_t start_file_id, const file_id_t end_file_id, const offset_t offset, const int64_t timeout)
  {
    UNUSED(start_file_id);
    UNUSED(end_file_id);
    UNUSED(offset);
    UNUSED(timeout);
    return NULL;
  }
  void revert_raw_log_iterator(ObIRawLogIterator* iter)
  {
    UNUSED(iter);
  }
  ObIRawIndexIterator* alloc_raw_index_iterator(const file_id_t start_file_id, const file_id_t end_file_id,
      const offset_t offset, const int64_t timeout, const bool use_cache)
  {
    UNUSED(start_file_id);
    UNUSED(end_file_id);
    UNUSED(offset);
    UNUSED(timeout);
    UNUSED(use_cache);
    return NULL;
  }
  void revert_raw_index_iterator(ObIRawIndexIterator* iter)
  {
    UNUSED(iter);
  }
  int read_log_by_location(const ObReadParam& param, ObLogEntry& entry)
  {
    UNUSED(param);
    UNUSED(entry);
    return common::OB_SUCCESS;
  }
  int read_log_by_location(const ObReadParam& param, ObLogEntry& entry, ObReadCost& cost)
  {
    UNUSED(param);
    UNUSED(entry);
    UNUSED(cost);
    return common::OB_SUCCESS;
  }
  int read_log_by_location(const ObLogTask& log_task, ObLogEntry& entry)
  {
    UNUSED(log_task);
    UNUSED(entry);
    return common::OB_SUCCESS;
  }
  int read_data_with_ctx(ObReadCtx& ctx, file_id_t f, offset_t o, int64_t s, char* b, ObReadCost& cost)
  {
    UNUSED(ctx);
    UNUSED(f);
    UNUSED(o);
    UNUSED(s);
    UNUSED(b);
    UNUSED(cost);
    return common::OB_SUCCESS;
  }
  int read_data_from_hot_cache(
      const file_id_t want_file_id, const offset_t want_offset, const int64_t want_size, char* user_buf)
  {
    UNUSED(want_file_id);
    UNUSED(want_offset);
    UNUSED(want_size);
    UNUSED(user_buf);
    return common::OB_SUCCESS;
  }
  int read_data_direct(const ObReadParam& param, ObReadBuf& rbuf, ObReadRes& res, ObReadCost& cost)
  {
    UNUSED(param);
    UNUSED(rbuf);
    UNUSED(res);
    UNUSED(cost);
    return common::OB_SUCCESS;
  }
  int submit_flush_task(FlushTask* task)
  {
    UNUSED(task);
    return common::OB_SUCCESS;
  }
  int submit_fetch_log_resp(
      const common::ObAddr& server, const common::ObPartitionKey& key, const int64_t network_limit, ObILogNetTask* task)
  {
    UNUSED(server);
    UNUSED(key);
    UNUSED(network_limit);
    UNUSED(task);
    return common::OB_SUCCESS;
  }
  int submit_log_ack(const common::ObAddr& server, const common::ObPartitionKey& key, const uint64_t log_id,
      const common::ObProposalID proposal_id)
  {
    UNUSED(server);
    UNUSED(key);
    UNUSED(log_id);
    UNUSED(proposal_id);
    return common::OB_SUCCESS;
  }
  int fetch_log_from_all_follower(const common::ObMemberList& mem_list, const common::ObPartitionKey& key,
      const uint64_t start_id, const uint64_t end_id, const common::ObProposalID proposal_id)
  {
    UNUSED(mem_list);
    UNUSED(key);
    UNUSED(start_id);
    UNUSED(end_id);
    UNUSED(proposal_id);
    return common::OB_SUCCESS;
  }
  int fetch_log_from_leader(const common::ObAddr& server, const common::ObPartitionKey& key, const uint64_t start_id,
      const uint64_t end_id, const common::ObProposalID proposal_id, const common::ObReplicaType replica_type)
  {
    UNUSED(server);
    UNUSED(key);
    UNUSED(start_id);
    UNUSED(end_id);
    UNUSED(proposal_id);
    UNUSED(replica_type);
    return common::OB_SUCCESS;
  }
  int fetch_register_server(const common::ObAddr& server, const ObPartitionKey& key,
      const common::ObReplicaType replica_type, const bool is_request_leader, const bool is_need_force_register)
  {
    UNUSED(server);
    UNUSED(key);
    UNUSED(replica_type);
    UNUSED(is_request_leader);
    UNUSED(is_need_force_register);
    return common::OB_SUCCESS;
  }
  int request_replace_sick_child(
      const common::ObAddr& server, const common::ObPartitionKey& key, const common::ObAddr& sick_child)
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(key);
    UNUSED(sick_child);
    return ret;
  }
  int reject_server(const common::ObAddr& server, const common::ObPartitionKey& key, const int32_t msg_type)
  {
    UNUSED(server);
    UNUSED(key);
    UNUSED(msg_type);
    return common::OB_SUCCESS;
  }
  bool is_in_black_list(const common::ObAddr& server) const
  {
    UNUSED(server);
    return true;
  }
  int submit_prepare_rqst(
      const common::ObMemberList& mem_list, const common::ObPartitionKey& key, const common::ObProposalID proposal_id)
  {
    UNUSED(mem_list);
    UNUSED(key);
    UNUSED(proposal_id);
    return common::OB_SUCCESS;
  }
  int prepare_response(const common::ObAddr& server, const common::ObPartitionKey& partition_key,
      const common::ObProposalID proposal_id, const uint64_t max_log_id, const int64_t max_log_ts)
  {
    UNUSED(server);
    UNUSED(partition_key);
    UNUSED(proposal_id);
    UNUSED(max_log_id);
    UNUSED(max_log_ts);
    return common::OB_SUCCESS;
  }
  int send_keepalive_msg(const common::ObAddr& server, const common::ObPartitionKey& partition_key,
      const uint64_t next_log_id, const int64_t next_log_ts_lb, const uint64_t deliver_cnt)
  {
    UNUSED(server);
    UNUSED(partition_key);
    UNUSED(next_log_id);
    UNUSED(next_log_ts_lb);
    UNUSED(deliver_cnt);
    return common::OB_SUCCESS;
  }

  int send_sync_log_archive_progress_msg(const common::ObAddr& server, const int64_t cluster_id,
      const common::ObPartitionKey& partition_key, const int64_t log_archive_round, const uint64_t last_archived_log_id,
      const int64_t last_archived_log_ts)
  {
    UNUSED(server);
    UNUSED(cluster_id);
    UNUSED(partition_key);
    UNUSED(log_archive_round);
    UNUSED(last_archived_log_id);
    UNUSED(last_archived_log_ts);
    return common::OB_SUCCESS;
  }
  int submit_index_flush_task(FlushTask* task)
  {
    UNUSED(task);
    return common::OB_SUCCESS;
  }
  void update_clog_info(const int64_t submit_timestamp)
  {
    UNUSED(submit_timestamp);
  }
  void update_clog_info(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, const int64_t submit_timestamp)
  {
    UNUSED(partition_key);
    UNUSED(log_id);
    UNUSED(submit_timestamp);
  }
  void update_ilog_info(
      const common::ObPartitionKey& partition_key, const uint64_t log_id, const int64_t submit_timestamp)
  {
    UNUSED(partition_key);
    UNUSED(log_id);
    UNUSED(submit_timestamp);
  }
  void update_ilog_info(const int64_t submit_timestamp)
  {
    UNUSED(submit_timestamp);
  }
  ObILogInfoBlockReader* get_info_block_reader(const InfoBlockReaderType type)
  {
    UNUSED(type);
    return NULL;
  }
  void revert_info_block_reader(ObILogInfoBlockReader* reader)
  {
    UNUSED(reader);
  }

  // new functions
  ObILogInfoBlockReader* get_clog_info_block_reader()
  {
    return NULL;
  }
  void revert_clog_info_block_reader(ObILogInfoBlockReader* reader)
  {
    UNUSED(reader);
  }
  ObILogInfoBlockReader* get_ilog_info_block_reader()
  {
    return NULL;
  }
  void revert_ilog_info_block_reader(ObILogInfoBlockReader* reader)
  {
    UNUSED(reader);
  }
  virtual ObCommitLogEnv* get_clog_env(const bool is_sys)
  {
    UNUSED(is_sys);
    return NULL;
  }
  virtual ObIndexLogEnv* get_ilog_env(const bool is_sys)
  {
    UNUSED(is_sys);
    return NULL;
  }
  virtual void revert_clog_env(ObCommitLogEnv* log_env)
  {
    UNUSED(log_env);
  }
  virtual void revert_ilog_env(ObIndexLogEnv* log_env)
  {
    UNUSED(log_env);
  }

  int reset_clog_info_block()
  {
    return common::OB_SUCCESS;
  }
  int reset_ilog_info_block()
  {
    return common::OB_SUCCESS;
  }
  int switch_ilog_file()
  {
    return common::OB_SUCCESS;
  }

  virtual int notify_follower_log_missing(
      const common::ObAddr& a, const common::ObPartitionKey& b, uint64_t c, const bool d)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    UNUSED(d);
    return common::OB_SUCCESS;
  }
  virtual file_id_t get_flying_ilog_file_id()
  {
    return common::OB_INVALID_FILE_ID;
  }
  virtual file_id_t get_flying_clog_file_id()
  {
    return common::OB_INVALID_FILE_ID;
  }
  virtual ObIndexInfoBlockHandler& get_flying_ilog_info_handler()
  {
    return fake_handler_;
  }
  void revert_flying_ilog_info_handler(ObIndexInfoBlockHandler* handler)
  {
    UNUSED(handler);
  }
  virtual int get_clog_info_handler(clog::file_id_t a, clog::ObCommitInfoBlockHandler& b)
  {
    UNUSED(a);
    UNUSED(b);
    return common::OB_SUCCESS;
  }
  virtual int get_ilog_info_handler(clog::file_id_t a, clog::ObIndexInfoBlockHandler& b, ObReadCost& cost)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(cost);
    return common::OB_SUCCESS;
  }
  virtual int get_remote_membership_status(const common::ObAddr& server, const common::ObPartitionKey& partition_key,
      int64_t& timestamp, uint64_t& max_confirmed_log_id, bool& is_in_sync)
  {
    UNUSED(server);
    UNUSED(partition_key);
    UNUSED(timestamp);
    UNUSED(max_confirmed_log_id);
    UNUSED(is_in_sync);
    return common::OB_SUCCESS;
  }
  virtual int get_follower_sync_info(const common::ObAddr& a, const common::ObPartitionKey& b, uint64_t& c, int64_t& d)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    UNUSED(d);
    return common::OB_SUCCESS;
  }
  virtual int add_to_black_list(const common::ObAddr& server)
  {
    UNUSED(server);
    return common::OB_SUCCESS;
  }
  virtual int remove_from_black_list(const common::ObAddr& server, bool& is_exec_remove)
  {
    UNUSED(server);
    UNUSED(is_exec_remove);
    return common::OB_SUCCESS;
  }
  virtual void try_limit_network(const ObLogEntry& log_entry)
  {
    UNUSED(log_entry);
  }
  virtual int64_t get_free_quota() const
  {
    return 0;
  };
  virtual bool is_using_new_log_env() const
  {
    return true;
  }
  ObILogDir* get_sys_ilog_dir()
  {
    return &si_dir_;
  }
  ObILogDir* get_user_ilog_dir()
  {
    return &ui_dir_;
  }
  ObILogDir* get_sys_clog_dir()
  {
    return &sc_dir_;
  }
  ObILogDir* get_user_clog_dir()
  {
    return &uc_dir_;
  }
  ObILogDir* get_clog_dir()
  {
    return NULL;
  }
  ObILogDir* get_ilog_dir()
  {
    return NULL;
  }
  int switch_clog_file()
  {
    return common::OB_SUCCESS;
  }
  bool is_disk_space_enough() const
  {
    return true;
  }
  /*
  virtual int set_new_env_min_using_file_id(const bool is_sys,
                                            const file_id_t ilog_file_id,
                                            const file_id_t clog_file_id)
                                            */
  int get_corresponding_clog_file_id(const file_id_t ilog_file_id, file_id_t& clog_file_id)
  {
    UNUSED(ilog_file_id);
    UNUSED(clog_file_id);
    return common::OB_SUCCESS;
  }
  virtual int broadcast_info(const common::ObMemberList& mem_list, const common::ObPartitionKey& key,
      const common::ObReplicaType& replica_type, const uint64_t max_confirmed_log_id)
  {
    UNUSED(mem_list);
    UNUSED(key);
    UNUSED(replica_type);
    UNUSED(max_confirmed_log_id);
    return 0;
  }
  void set_all_scan_finish()
  {}
  virtual int set_env_min_using_file_id(const file_id_t ilog_file_id, const file_id_t clog_file_id)
  {
    UNUSED(ilog_file_id);
    UNUSED(clog_file_id);
    return common::OB_SUCCESS;
  }
  virtual int set_new_env_min_using_file_id(const file_id_t ilog_file_id, const file_id_t clog_file_id)
  {
    UNUSED(ilog_file_id);
    UNUSED(clog_file_id);
    return common::OB_SUCCESS;
  }
  virtual int submit_batch_log(const common::ObMemberList& member_list, const transaction::ObTransID& trans_id,
      const common::ObPartitionArray& partition_array, const ObLogInfoArray& log_info_array)
  {
    UNUSED(member_list);
    UNUSED(trans_id);
    UNUSED(partition_array);
    UNUSED(log_info_array);
    return common::OB_SUCCESS;
  }
  virtual int submit_batch_ack(
      const common::ObAddr& leader, const transaction::ObTransID& trans_id, const ObBatchAckArray& batch_ack_array)
  {
    UNUSED(leader);
    UNUSED(trans_id);
    UNUSED(batch_ack_array);
    return common::OB_SUCCESS;
  }
  virtual int query_remote_log(const common::ObAddr& server, const common::ObPartitionKey& partition_key,
      const uint64_t log_id, transaction::ObTransID& trans_id, int64_t& submit_timestamp)
  {
    UNUSED(server);
    UNUSED(partition_key);
    UNUSED(log_id);
    UNUSED(trans_id);
    UNUSED(submit_timestamp);
    return common::OB_SUCCESS;
  }
  virtual void record_flush_cb_histogram(const int64_t cost_ts)
  {
    UNUSED(cost_ts);
  }
  virtual void record_batch_flush_cb_histogram(const int64_t cost_ts)
  {
    UNUSED(cost_ts);
  }
  virtual int get_cursor_batch(const common::ObPartitionKey& pkey, const uint64_t query_log_id,
      ObGetCursorResult& result, ObIlogStorageQueryCost& csr_cost, const uint64_t retry_limit,
      const unsigned sleep_time_on_retry_us)
  {
    int ret = OB_SUCCESS;
    UNUSED(pkey);
    UNUSED(query_log_id);
    UNUSED(result);
    UNUSED(csr_cost);
    UNUSED(retry_limit);
    UNUSED(sleep_time_on_retry_us);
    return ret;
  }
  virtual int get_cursor_batch(const common::ObPartitionKey& pkey, const uint64_t query_log_id,
      ObLogCursorExt& log_cursor, ObGetCursorResult& result, uint64_t& cursor_start_log_id)
  {
    int ret = OB_SUCCESS;
    UNUSED(pkey);
    UNUSED(query_log_id);
    UNUSED(log_cursor);
    UNUSED(result);
    UNUSED(cursor_start_log_id);
    return ret;
  }
  virtual int get_cursor(const common::ObPartitionKey& pkey, const uint64_t query_log_id, ObLogCursorExt& log_cursor)
  {
    int ret = OB_SUCCESS;
    UNUSED(pkey);
    UNUSED(query_log_id);
    UNUSED(log_cursor);
    return ret;
  }
  virtual int query_max_ilog_id(const common::ObPartitionKey& pkey, uint64_t& ret_max_ilog_id)
  {
    int ret = OB_SUCCESS;
    UNUSED(pkey);
    UNUSED(ret_max_ilog_id);
    return ret;
  }
  virtual int query_ilog_file_id(
      const common::ObPartitionKey& pkey, const uint64_t query_log_id, file_id_t& ilog_file_id)
  {
    int ret = OB_SUCCESS;
    UNUSED(pkey);
    UNUSED(query_log_id);
    UNUSED(ilog_file_id);
    return ret;
  }
  virtual int guarantee_extend_min()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  virtual int locate_by_timestamp(const common::ObPartitionKey& pkey, const int64_t start_ts, uint64_t& target_log_id,
      int64_t& target_log_timestamp)
  {
    int ret = OB_SUCCESS;
    UNUSED(pkey);
    UNUSED(start_ts);
    UNUSED(target_log_id);
    UNUSED(target_log_timestamp);
    return ret;
  }

private:
  ObIndexInfoBlockHandler fake_handler_;
  ObLogDir si_dir_;
  ObLogDir ui_dir_;
  ObLogDir sc_dir_;
  ObLogDir uc_dir_;
};
/*
class ObILogNetTask
{
public:
  ~ObILogNetTask() {}
  char *get_data_buffer() const{return NULL;}
  common::ObProposalID get_proposal_id() const{
    return 0;}
  int64_t get_data_len() const{return 0;}
};

class ObLogNetTask : public ObILogNetTask
{
public:
  ObLogNetTask(common::ObProposalID proposal_id, char *buff,
               int64_t data_len) : proposal_id_(proposal_id),
                                   buff_(buff), data_len_(data_len) {};
  char *get_data_buffer() const {return buff_;}
  common::ObProposalID get_proposal_id() const {return proposal_id_;}
  int64_t get_data_len() const {return data_len_;}
private:
  common::ObProposalID proposal_id_;
  char *buff_;
  int64_t data_len_;
};
*/
}  // end namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_UNITTEST_CLOG_MOCK_CONTAINER_MOCK_OB_LOG_ENGINE_H_
