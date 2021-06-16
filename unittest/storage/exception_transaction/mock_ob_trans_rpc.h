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

#ifndef OB_TEST_OB_TEST_MOCK_TRANS_RPC_H_
#define OB_TEST_OB_TEST_MOCK_TRANS_RPC_H_

#include "common/ob_queue_thread.h"
#include "common/ob_queue_thread.h"
#include "storage/transaction/ob_trans_service.h"
#include "storage/transaction/ob_trans_rpc.h"

namespace oceanbase {
using namespace transaction;
using namespace common;
namespace unittest {
class MockObTransRpc : public ObITransRpc, public common::ObSimpleThreadPool {
public:
  MockObTransRpc() : is_inited_(false), trans_service_(NULL)
  {}
  ~MockObTransRpc()
  {
    destroy();
  }
  int init(obrpc::ObTransRpcProxy* rpc_proxy, ObTransService* trans_service, const common::ObAddr& self)
  {
    UNUSED(rpc_proxy);
    UNUSED(trans_service);
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
  int post_trans_msg(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTransMsg& msg, const int64_t msg_type)
  {
    UNUSED(tenant_id);
    UNUSED(server);
    UNUSED(msg);
    UNUSED(msg_type);
    return OB_SUCCESS;
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
  int post_trans_resp_msg(const uint64_t tenant_id, const common::ObAddr& server, const ObTransMsg& msg)
  {
    UNUSED(tenant_id);
    UNUSED(server);
    UNUSED(msg);
    return OB_SUCCESS;
  }
  int init(ObTransService* trans_service, const ObAddr& self);
  void handle(void* task);

private:
  bool is_inited_;
  ObTransService* trans_service_;
};

}  // namespace unittest
}  // namespace oceanbase

#endif
