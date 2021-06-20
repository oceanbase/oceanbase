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

#ifndef OB_UNITTEST_MOCK_ELECTION_RPC_H_
#define OB_UNITTEST_MOCK_ELECTION_RPC_H_

#include "common/ob_queue_thread.h"
#include "common/ob_queue_thread.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/transaction/ob_trans_rpc.h"
#include "election/ob_election_rpc.h"
#include "election/ob_election_msg.h"

namespace oceanbase {
using namespace transaction;
using namespace election;
using namespace common;

namespace unittest {

class ElectionMsgTask {
public:
  ElectionMsgTask()
  {
    reset();
  }
  ~ElectionMsgTask()
  {}
  void reset()
  {
    sender_ = ObAddr();
    msgbuf_ = ObElectionMsgBuffer();
    send_ts_ = 0;
  }

public:
  ObAddr sender_;
  ObElectionMsgBuffer msgbuf_;
  int64_t send_ts_;
};

class ElectionMsgTaskFactory {
public:
  static ElectionMsgTask* alloc();
  static void release(ElectionMsgTask* task);

private:
  static int64_t alloc_count_;
  static int64_t release_count_;
};

class MockElectionRpc : public ObIElectionRpc, public common::ObSimpleThreadPool {
public:
  MockElectionRpc() : is_inited_(false), election_mgr_(NULL), handle_cnt_(0), handle_time_(0)
  {}
  virtual ~MockElectionRpc()
  {
    destroy();
  }
  int init(obrpc::ObElectionRpcProxy* rpc_proxy, ObIElectionMgr* election_mgr, const common::ObAddr& self)
  {
    UNUSED(rpc_proxy);
    UNUSED(election_mgr);
    UNUSED(self);
    return OB_SUCCESS;
  }
  int init(const common::ObAddr& self, ObIElectionMgr* election_mgr);
  void clear_black_list()
  {
    black_list_.clear();
  }
  int add_to_black_list(const common::ObAddr& dst);
  int remove_from_black_list(const common::ObAddr& dst);
  bool is_in_black_list(const common::ObAddr& dst);

  int post_election_msg(
      const common::ObAddr& server, const common::ObPartitionKey& partition, const ObElectionMsg& msg);
  int post_election_group_msg(const common::ObAddr& server, const ObElectionGroupId& eg_id,
      const ObPartArrayBuffer& part_array_buf, const ObElectionMsg& msg);
  int start()
  {
    return OB_SUCCESS;
  }
  int stop()
  {
    return OB_SUCCESS;
  }
  int wait()
  {
    return OB_SUCCESS;
  }
  void destroy()
  {
    if (is_inited_) {
      is_inited_ = false;
      common::ObSimpleThreadPool::destroy();
    }
  }

  void handle(void* task);

private:
  const int64_t RPC_THREAD_NUM = 2;
  const int64_t RPC_TASK_LIMIT = 10000;

private:
  bool is_inited_;
  ObAddr self_;
  ObIElectionMgr* election_mgr_;
  common::ObLinearHashMap<common::ObAddr, bool> black_list_;
  int64_t handle_cnt_;
  int64_t handle_time_;
};

}  // namespace unittest
}  // namespace oceanbase
#endif
