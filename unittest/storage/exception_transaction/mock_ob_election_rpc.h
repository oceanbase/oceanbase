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

#ifndef OB_TEST_OB_TEST_MOCK_ELECTION_RPC_H_
#define OB_TEST_OB_TEST_MOCK_ELECTION_RPC_H_

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
class ElectionRpcTask {
public:
  ElectionRpcTask()
  {
    reset();
  }
  ~ElectionRpcTask()
  {}
  void reset()
  {
    server_ = ObAddr();
    msgbuf_ = ObElectionMsgBuffer();
    partition_ = ObPartitionKey();
    timestamp_ = 0;
  }

public:
  ObAddr server_;
  ObElectionMsgBuffer msgbuf_;
  ObPartitionKey partition_;
  int64_t timestamp_;
};

class ElectionRpcTaskFactory {
public:
  static ElectionRpcTask* alloc();
  static void release(ElectionRpcTask* task);

private:
  static int64_t alloc_count_;
  static int64_t release_count_;
};

class MockObElectionRpc : public ObIElectionRpc, public common::ObSimpleThreadPool {
public:
  MockObElectionRpc() : inited_(false), election_mgr_(NULL)
  {}
  ~MockObElectionRpc()
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
    ObSimpleThreadPool::destroy();
  }
  int post_election_msg(const common::ObAddr& server, const common::ObPartitionKey& partition, const ObElectionMsg& msg)
  {
    UNUSED(server);
    UNUSED(partition);
    UNUSED(msg);
    return OB_SUCCESS;
  }
  int init(ObIElectionMgr* election_mgr, const ObAddr& self);
  void handle(void* task);

private:
  bool inited_;
  ObIElectionMgr* election_mgr_;
};

}  // namespace unittest
}  // namespace oceanbase
#endif
