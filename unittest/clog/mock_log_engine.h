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

#ifndef OCEANBASE_UNITTEST_MOCK_LOG_ENGINE_H_
#define OCEANBASE_UNITTEST_MOCK_LOG_ENGINE_H_
#include "clog/ob_i_log_engine.h"
#include "mock_log_reader.h"
#include "clog_mock_container/mock_log_engine.h"
namespace oceanbase {
namespace clog {
class MockLogEngine : public MockObLogEngine {
public:
  MockLogEngine()
  {}
  virtual ~MockLogEngine()
  {}
  virtual ObIRawLogIterator* alloc_raw_log_iterator(const bool is_sys, const uint64_t file_id, const int64_t timeout)
  {
    UNUSED(is_sys);
    UNUSED(file_id);
    UNUSED(timeout);
    MockRawLogIterator* ptr =
        static_cast<MockRawLogIterator*>(common::ob_malloc(sizeof(MockRawLogIterator), common::ObModIds::OB_UPS_LOG));
    ptr->init();
    return ptr;
  }
  virtual void revert_raw_log_iterator(ObIRawLogIterator* iter)
  {
    common::ob_free(iter);
  }
  virtual int read_log_by_location(const bool is_sys, const clog::ObReadParam& param, ObLogEntry& entry)
  {
    UNUSED(is_sys);
    UNUSED(param);
    UNUSED(entry);
    return common::OB_SUCCESS;
  }
  virtual int read_log_by_id(const clog::ObReadParam& param, ObLogEntry& entry)
  {
    UNUSED(param);
    UNUSED(entry);
    return common::OB_SUCCESS;
  }
  virtual int submit_flush_task(FlushTask* task)
  {
    ObLogCursor log_cursor;
    task->after_consume(common::OB_SUCCESS, &log_cursor);
    return common::OB_SUCCESS;
  };
  virtual int post_log(const common::ObAddr& server, const common::ObPartitionKey& key, const char* buf, int64_t len,
      common::ObProposalID propose_id)
  {
    UNUSED(server);
    UNUSED(key);
    UNUSED(buf);
    UNUSED(len);
    UNUSED(propose_id);
    return common::OB_SUCCESS;
  }
  virtual int submit_net_task(
      const common::ObMemberList& mem_list, const common::ObPartitionKey& key, ObILogNetTask* task)
  {
    UNUSED(mem_list);
    UNUSED(key);
    UNUSED(task);
    return common::OB_SUCCESS;
  }
  virtual int submit_log_ack(const common::ObAddr& server, const common::ObPartitionKey& key, const uint64_t log_id,
      const common::ObProposalID propose_id, ObLogType type)
  {
    UNUSED(server);
    UNUSED(key);
    UNUSED(log_id);
    UNUSED(propose_id);
    UNUSED(type);
    return common::OB_SUCCESS;
  }
  virtual int fetch_log_from_all_follower(const common::ObMemberList& mem_list, const common::ObPartitionKey& key,
      const uint64_t start_id, const uint64_t end_id, const common::ObProposalID propose_id)
  {
    UNUSED(mem_list);
    UNUSED(key);
    UNUSED(start_id);
    UNUSED(end_id);
    UNUSED(propose_id);
    return common::OB_SUCCESS;
  }
  virtual int fetch_log_from_leader(const common::ObAddr& server, const common::ObPartitionKey& key,
      const uint64_t start_id, const uint64_t end_id, const common::ObProposalID propose_id)
  {
    UNUSED(server);
    UNUSED(key);
    UNUSED(start_id);
    UNUSED(end_id);
    UNUSED(propose_id);
    return common::OB_SUCCESS;
  }
  virtual int submit_prepare_rqst(
      const common::ObMemberList& mem_list, const common::ObPartitionKey& key, const common::ObProposalID propose_id)
  {
    UNUSED(mem_list);
    UNUSED(key);
    UNUSED(propose_id);
    return common::OB_SUCCESS;
  }
  virtual int broadcast_mc_log_to_members(const common::ObPartitionKey& partition_key,
      const common::ObMemberList& sendout_member_list, const char* buff, const int64_t buff_len)
  {
    UNUSED(partition_key);
    UNUSED(sendout_member_list);
    UNUSED(buff);
    UNUSED(buff_len);
    int ret = common::OB_SUCCESS;
    return ret;
  }
  virtual int fetch_latest_mc_log(const common::ObPartitionKey& partition_key, const common::ObAddr& leader)
  {
    UNUSED(partition_key);
    UNUSED(leader);
    int ret = common::OB_SUCCESS;
    return ret;
  }
  virtual int ack_mc_log_to_leader(const common::ObPartitionKey& partition_key, const common::ObAddr& leader,
      const common::ObAddr& server, const int64_t mc_timestamp, const common::ObProposalID proposal_id)
  {
    UNUSED(partition_key);
    UNUSED(leader);
    UNUSED(server);
    UNUSED(mc_timestamp);
    UNUSED(proposal_id);
    int ret = common::OB_SUCCESS;
    return ret;
  }
  virtual int prepare_response(const common::ObAddr& server, const common::ObPartitionKey& partition_key,
      const uint64_t max_log_id, const common::ObProposalID propose_id)
  {
    UNUSED(server);
    UNUSED(partition_key);
    UNUSED(max_log_id);
    UNUSED(propose_id);
    return common::OB_SUCCESS;
  }
  virtual int get_current_cursor(uint64_t& file_id, int64_t& offset)
  {
    UNUSED(file_id);
    UNUSED(offset);
    return common::OB_SUCCESS;
  }
  virtual int get_search_min_fid(const oceanbase::clog::ObReadParam& a, uint64_t& b)
  {
    UNUSED(a);
    UNUSED(b);
    return common::OB_SUCCESS;
  }
  virtual int submit_fetch_log_resp(const oceanbase::common::ObMemberList& a,
      const oceanbase::common::ObPartitionKey& b, oceanbase::clog::ObILogNetTask* c)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    return common::OB_SUCCESS;
  }
  virtual int get_cur_min_fid(uint64_t& file_id)
  {
    UNUSED(file_id);
    return common::OB_SUCCESS;
  }
  virtual int get_cur_max_fid(uint64_t& file_id)
  {
    UNUSED(file_id);
    return common::OB_SUCCESS;
  }
  virtual ObILogPartitionMetaReader* alloc_partition_meta_reader()
  {
    return NULL;
  }
  virtual void revert_partition_meta_reader(ObILogPartitionMetaReader* reader)
  {
    UNUSED(reader);
    return;
  }
  int get_remote_membership_timestamp(
      const common::ObAddr& server, const common::ObPartitionKey& partition_key, int64_t& timestamp)
  {
    UNUSED(server);
    UNUSED(partition_key);
    UNUSED(timestamp);
    return common::OB_SUCCESS;
  }
  ObIndexInfoBlockHandler& get_flying_ilog_info_handler(const bool is_sys)
  {
    UNUSED(is_sys);
    return fake_handler_;
  }
  void revert_flying_ilog_info_handler(ObIndexInfoBlockHandler* handler)
  {
    UNUSED(handler);
  }
  int get_clog_info_handler(const bool is_sys, const file_id_t file_id, ObCommitInfoBlockHandler& handler)
  {
    UNUSED(is_sys);
    UNUSED(file_id);
    UNUSED(handler);
    return common::OB_SUCCESS;
  }
  int get_ilog_info_handler(const bool is_sys, const file_id_t file_id, ObIndexInfoBlockHandler& handler)
  {
    UNUSED(is_sys);
    UNUSED(file_id);
    UNUSED(handler);
    return common::OB_SUCCESS;
  }
  int update_ilog_info(const bool is_sys, const common::ObVersion& version)
  {
    UNUSED(is_sys);
    UNUSED(version);
    return common::OB_SUCCESS;
  }

  file_id_t get_flying_ilog_file_id(const bool is_sys)
  {
    UNUSED(is_sys);
    return common::OB_INVALID_FILE_ID;
  }
  file_id_t get_flying_clog_file_id(const bool is_sys)
  {
    UNUSED(is_sys);
    return common::OB_INVALID_FILE_ID;
  }
  virtual int add_to_black_list(const common::ObAddr& server)
  {
    UNUSED(server);
    return common::OB_SUCCESS;
  }
  virtual int remove_from_black_list(const common::ObAddr& server)
  {
    UNUSED(server);
    return common::OB_SUCCESS;
  }
  virtual void set_all_scan_finish()
  {}
  virtual int broadcast_info(const common::ObMemberList& mem_list, const common::ObPartitionKey& key,
      const common::ObReplicaType& replica_type, const uint64_t max_confirmed_log_id)
  {
    UNUSED(mem_list);
    UNUSED(key);
    UNUSED(replica_type);
    UNUSED(max_confirmed_log_id);
    return 0;
  }

private:
  ObIndexInfoBlockHandler fake_handler_;
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_MOCK_LOG_ENGINE_H_
