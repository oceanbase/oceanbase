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

#ifndef OCEANBASE_UNITTEST_MOCK_LOG_UTILS_H_
#define OCEANBASE_UNITTEST_MOCK_LOG_UTILS_H_

#include "gtest/gtest.h"
#include "lib/allocator/ob_malloc.h"
#include <locale.h>
#include "clog/ob_log_state_mgr.h"
#include "clog/ob_i_net_log_buffer.h"
#include "clog/ob_i_disk_log_buffer.h"
#include "clog/ob_i_net_log_buffer_mgr.h"
#include "clog/ob_log_engine.h"
#include "storage/ob_i_partition_mgr.h"
#include "../storage/mockcontainer/mock_ob_partition.h"
#include "../storage/mockcontainer/mock_ob_election_mgr.h"
#include "clog/ob_log_callback_engine.h"
#include "../storage/mockcontainer/mock_ob_partition_mgr.h"
#include "election/ob_election_rpc.h"

namespace oceanbase
{
namespace clog
{
using namespace oceanbase::election;

class MockSubmitLogCb : public ObISubmitLogCb
{
public:
  MockSubmitLogCb() {}
  ~MockSubmitLogCb() {}
  //virtual int on_success(const common::ObPartitionKey &parition_key, const uint64_t log_id,  const int64_t version) = 0;
  virtual int on_success(const common::ObPartitionKey &partition_key, const uint64_t log_id,
                         const int64_t trans_version)
  {
    UNUSED(partition_key);
    UNUSED(log_id);
    UNUSED(trans_version);
    return OB_SUCCESS;
  }
};

/*
class MockLogEngine: public ObILogEngine
{
public:
  MockLogEngine() {}
  virtual ~MockLogEngine() {}
  virtual ObIRawLogIterator *alloc_raw_log_iterator(const uint64_t file_id, const int64_t timeout)
  {
    UNUSED(file_id);
    UNUSED(timeout);
    MockRawLogIterator *ptr = static_cast<MockRawLogIterator *>(common::ob_malloc(sizeof(
                                                                                      MockRawLogIterator), common::ObModIds::OB_UPS_LOG));
    ptr->init();
    return ptr;
  }
  virtual void revert_raw_log_iterator(ObIRawLogIterator *iter)
  {
    common::ob_free(iter);
  }
  virtual int read_log_by_location(const clog::ObReadParam &param, ObLogEntry &entry)
  {
    UNUSED(param);
    UNUSED(entry);
    return common::OB_SUCCESS;
  }
  virtual int read_log_by_id(const clog::ObReadParam &param, ObLogEntry &entry)
  {
    UNUSED(param);
    UNUSED(entry);
    return common::OB_SUCCESS;
  }
  virtual int submit_flush_task(FlushTask *task)
  {
    ObLogCursor log_cursor;
    task->after_consume(common::OB_SUCCESS, &log_cursor);
    return common::OB_SUCCESS;
  };
  virtual int post_log(const common::ObAddr &server,
                       const common::ObPartitionKey &key,
                       const char *buf,
                       int64_t len,
                       common::ObProposalID propose_id)
  {
    UNUSED(server);
    UNUSED(key);
    UNUSED(buf);
    UNUSED(len);
    UNUSED(propose_id);
    return common::OB_SUCCESS;
  }
  virtual int submit_net_task(const common::ObMemberList &mem_list,
                              const common::ObPartitionKey &key,
                              ObILogNetTask *task)
  {
    UNUSED(mem_list);
    UNUSED(key);
    UNUSED(task);
    return common::OB_SUCCESS;
  }
  virtual int submit_log_ack(const common::ObAddr &server,
                             const common::ObPartitionKey &key,
                             const uint64_t log_id,
                             const common::ObProposalID propose_id,
                             ObLogType type)
  {
    UNUSED(server);
    UNUSED(key);
    UNUSED(log_id);
    UNUSED(propose_id);
    UNUSED(type);
    return common::OB_SUCCESS;
  }
  virtual int fetch_log_from_all_follower(const common::ObMemberList &mem_list,
                                          const common::ObPartitionKey &key,
                                          const uint64_t start_id,
                                          const common::ObProposalID propose_id)
  {
    UNUSED(mem_list);
    UNUSED(key);
    UNUSED(start_id);
    UNUSED(propose_id);
    return common::OB_SUCCESS;
  }
  virtual int fetch_log_from_leader(const common::ObAddr &server,
                                    const common::ObPartitionKey &key,
                                    const uint64_t start_id,
                                    const uint64_t end_id,
                                    const common::ObProposalID propose_id)
  {
    UNUSED(server);
    UNUSED(key);
    UNUSED(start_id);
    UNUSED(end_id);
    UNUSED(propose_id);
    return common::OB_SUCCESS;
  }
  virtual int submit_prepare_rqst(const common::ObMemberList &mem_list,
                                  const common::ObPartitionKey &key,
                                  const common::ObProposalID propose_id)
  {
    UNUSED(mem_list);
    UNUSED(key);
    UNUSED(propose_id);
    return common::OB_SUCCESS;
  }
  virtual int broadcast_mc_log_to_members(const common::ObPartitionKey &partition_key,
                                          const common::ObMemberList &sendout_member_list,
                                          const char *buff,
                                          const int64_t buff_len)
  {
    UNUSED(partition_key);
    UNUSED(sendout_member_list);
    UNUSED(buff);
    UNUSED(buff_len);
    int ret = common::OB_SUCCESS;
    return ret;
  }
  virtual int fetch_latest_mc_log(const common::ObPartitionKey &partition_key,
                                  const common::ObAddr &leader)
  {
    UNUSED(partition_key);
    UNUSED(leader);
    int ret = common::OB_SUCCESS;
    return ret;
  }
  virtual int ack_mc_log_to_leader(const common::ObPartitionKey &partition_key,
                                   const common::ObAddr &leader,
                                   const common::ObAddr &server,
                                   const int64_t mc_timestamp,
                                   const common::ObProposalID proposal_id)
  {
    UNUSED(partition_key);
    UNUSED(leader);
    UNUSED(server);
    UNUSED(mc_timestamp);
    UNUSED(proposal_id);
    int ret = common::OB_SUCCESS;
    return ret;
  }
  virtual int prepare_response(const common::ObAddr &server,
                               const common::ObPartitionKey &partition_key,
                               const uint64_t max_log_id,
                               const common::ObProposalID propose_id)
  {
    UNUSED(server);
    UNUSED(partition_key);
    UNUSED(max_log_id);
    UNUSED(propose_id);
    return common::OB_SUCCESS;
  }
  virtual int get_current_cursor(uint64_t &file_id, int64_t &offset)
  {
    UNUSED(file_id);
    UNUSED(offset);
    return common::OB_SUCCESS;
  }
  virtual int get_search_min_fid(const oceanbase::clog::ObReadParam &a, uint64_t &b)
  {
    UNUSED(a);
    UNUSED(b);
    return common::OB_SUCCESS;
  }
  virtual int submit_fetch_log_resp(const oceanbase::common::ObMemberList &a,
                                    const oceanbase::common::ObPartitionKey &b, oceanbase::clog::ObILogNetTask *c)
  {
    UNUSED(a);
    UNUSED(b);
    UNUSED(c);
    return common::OB_SUCCESS;
  }
  virtual int get_cur_min_fid(uint64_t &file_id)
  {
    UNUSED(file_id);
    return common::OB_SUCCESS;
  }
  virtual ObILogPartitionMetaReader *alloc_partition_meta_reader()
  {
    return NULL;
  }
  virtual void revert_partition_meta_reader(ObILogPartitionMetaReader *reader)
  {
    UNUSED(reader);
    return;
  }
};
*/

class MockElectionMgr : public  election::MockObIElectionMgr
{
public:
  MockElectionMgr() : leader_() {}
  virtual int start_all()
  {
    return 0;
  }
  int stop_all()
  {
    return 0;
  }
  virtual int handle_election_msg(const ObElectionMsgBuffer &msgbuf,
                                  obrpc::ObElectionRpcResult &result)
  {
    UNUSED(msgbuf);
    UNUSED(result);
    return 0;
  }
  int init(const common::ObAddr &self, obrpc::ObElectionRpcProxy *client_manager)
  {
    UNUSED(self);
    UNUSED(client_manager);
    return 0;
  }
  int add_partition(const common::ObPartitionKey &partition, int64_t replica_num,
                    election::ObIElectionCallback *election_cb)
  {
    UNUSED(partition);
    UNUSED(replica_num);
    UNUSED(election_cb);
    return 0;
  }
  int remove_partition(const common::ObPartitionKey &partition)
  {
    UNUSED(partition);
    return  0;
  }
  int change_leader_async(const common::ObPartitionKey &partition, const common::ObAddr &leader)
  {
    UNUSED(partition);
    UNUSED(leader);
    change_leader_ = true;
    return  0;
  }
  int start(const common::ObPartitionKey &partition)
  {
    UNUSED(partition);
    return  0;
  }
  int stop(const common::ObPartitionKey &partition)
  {
    UNUSED(partition);
    return 0;
  }
  virtual int set_candidate(const common::ObPartitionKey &partition,
                            const common::ObMemberList &prev_mlist,
                            const common::ObMemberList &curr_mlist)
  {
    UNUSED(partition);
    UNUSED(prev_mlist);
    UNUSED(curr_mlist);
    return 0;
  }
  int get_prev_candidate(const common::ObPartitionKey &partition, common::ObMemberList &mlist) const
  {
    UNUSED(partition);
    UNUSED(mlist);
    return 0;
  }
  int get_curr_candidate(const common::ObPartitionKey &partition, common::ObMemberList &mlist) const
  {
    UNUSED(partition);
    UNUSED(mlist);
    return 0;
  }
  int get_leader(const common::ObPartitionKey &partition, common::ObAddr &leader) const
  {
    UNUSED(partition);
    leader = leader_;
    return 0;
  }
public:
  bool change_leader_;
  ObAddr leader_;
};

class MockLogCallbackEngine: public ObILogCallbackEngine
{
public:
  virtual int init(common::S2MQueueThread *worker_thread_pool,
                   common::S2MQueueThread *sp_thread_pool)
  {
    UNUSED(worker_thread_pool);
    UNUSED(sp_thread_pool);
    return common::OB_SUCCESS;
  }
  virtual void destroy()
  {
    return;
  }
  virtual int submit_flush_cb_task(const common::ObPartitionKey &partition_key,
                                   const ObLogFlushCbArg &flush_cb_arg)
  {
    UNUSED(partition_key);
    UNUSED(flush_cb_arg);
    return common::OB_SUCCESS;
  }
  virtual int submit_member_change_success_cb_task(const common::ObPartitionKey &partition_key,
                                                   const int64_t mc_timestamp,
                                                   const common::ObMemberList &prev_member_list,
                                                   const common::ObMemberList &curr_member_list)
  {
    UNUSED(partition_key);
    UNUSED(mc_timestamp);
    UNUSED(prev_member_list);
    UNUSED(curr_member_list);
    return common::OB_SUCCESS;
  }
  virtual int submit_leader_takeover_cb_task(const common::ObPartitionKey &partition_key)
  {
    UNUSED(partition_key);
    return common::OB_SUCCESS;
  }
  virtual int submit_leader_revoke_cb_task(const common::ObPartitionKey &partition_key)
  {
    UNUSED(partition_key);
    return common::OB_SUCCESS;
  }
};

class MockObPSCb : public ObIPSCb
{
public:
  virtual int64_t get_min_using_file_id() const {return 0;}
  virtual int on_leader_revoke(const common::ObPartitionKey &partition_key)
  {UNUSED(partition_key); return 0;}
  virtual int on_leader_takeover(const common::ObPartitionKey &partition_key)
  {UNUSED(partition_key); return 0;}
  virtual int on_leader_active(const common::ObPartitionKey &partition_key)
  {UNUSED(partition_key); return 0;}
  virtual int on_member_change_success(
      const common::ObPartitionKey &partition_key,
      const int64_t mc_timestamp,
      const common::ObMemberList &prev_member_list,
      const common::ObMemberList &curr_member_list)
  {
    UNUSED(partition_key);
    UNUSED(mc_timestamp);
    UNUSED(prev_member_list);
    UNUSED(curr_member_list);
    return 0;
  }
  virtual int handle_log_missing(const common::ObPartitionKey &pkey,
                                 const common::ObAddr &server)
  {
    UNUSED(pkey);
    UNUSED(server);
    return common::OB_SUCCESS;
  }
  virtual int get_server_locality_array(
      common::ObIArray<share::ObServerLocality> &server_locality_array,
      bool &has_readonly_zone) const
  {
    UNUSED(server_locality_array);
    UNUSED(has_readonly_zone);
    return common::OB_SUCCESS;
  }
};

} // namespace clog
} // namespace oceanbase

#endif // OCEANBASE_UNITTEST_MOCK_LOG_UTILS_H_
