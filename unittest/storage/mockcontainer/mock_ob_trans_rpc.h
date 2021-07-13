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

#ifndef MOCK_OB_TRANS_RPC_H_
#define MOCK_OB_TRANS_RPC_H_

#include "common/ob_partition_key.h"
#include "common/ob_queue_thread.h"

#include "storage/transaction/ob_trans_rpc.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/transaction/ob_trans_msg.h"

#include "test_ob_transaction.h"

namespace oceanbase {
namespace transaction {

using namespace common;
using namespace obrpc;

struct RpcTask {
  ObAddr server;
  ObTransMsg msg;
  int64_t msg_type;
};

class MockObTransRpc : public ObITransRpc, public ObSimpleThreadPool {
public:
  MockObTransRpc(STMap* st_map)
  {
    st_map_ = st_map;
  }
  ~MockObTransRpc()
  {
    destroy();
  }
  int init(ObTransRpcProxy* rpc_proxy, ObTransService* trans_service, const ObAddr& self)
  {
    int ret = OB_SUCCESS;
    UNUSED(self);
    UNUSED(rpc_proxy);
    UNUSED(trans_service);

    if (OB_SUCCESS != (ret = ObSimpleThreadPool::init(4, 100000))) {
      TRANS_LOG(WARN, "ObSimpleThreadPool init error", K(ret));
    }

    return ret;
  }

  int start()
  {
    return OB_SUCCESS;
  }
  int stop()
  {
    destroy();
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

  int post_trans_msg(const ObAddr& server, const ObTransMsg& msg, const int64_t msg_type)
  {
    int ret = OB_SUCCESS;

    RpcTask* rpc_task = NULL;
    if (NULL == (rpc_task = new RpcTask)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (OB_FAIL(rpc_task->init(msg, ObTransRpcTaskType::ERROR_MSG_TASK))) {
        TRANS_LOG(WARN, "rpc task init error", K(ret), "rpc_task", *rpc_task);
      } else if (OB_SUCCESS != (ret = push(rpc_task))) {
        TRANS_LOG(WARN, "push task error", K(ret));
        delete rpc_task;
        rpc_task = NULL;
      }
    }

    return ret;
  }

  virtual int ObTransRpc::post_batch_msg(const uint64_t tenant_id, const ObAddr& server, const obrpc::ObIFill& msg,
      const int64_t msg_type, const ObPartitionKey& pkey)
  {
    UNUSED(tenant_id);
    UNUSED(server);
    UNUSED(msg);
    UNUSED(msg_type);
    UNUSED(pkey);
    return OB_SUCCESS;
  }

  int post_trans_err_msg(const common::ObAddr& server, const ObTransErrMsg& msg)
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(msg);
    return ret;
  }

public:
  void handle(void* task)
  {
    int tmp_ret = 0;
    RpcTask* rpc_task = static_cast<RpcTask*>(task);
    TestServer* test_server = NULL;

    if (HASH_EXIST != (tmp_ret = st_map_->get(rpc_task->get_msg().get_addr(), test_server))) {
      TRANS_LOG(WARN, "get test by server error", "ret", tmp_ret);
    } else if (OB_SUCCESS != (tmp_ret = test_server->handle_trans_msg(rpc_task->get_msg()))) {
      TRANS_LOG(WARN, "handle transaction message error", "ret", tmp_ret, "msg", rpc_task->get_msg());
    } else {
      // TRANS_LOG(INFO, "handle transaction message success", "to", server, K(msg_type));
    }

    delete rpc_task;
  }

private:
  STMap* st_map_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif
