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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_SESSION_HANDLER_
#define OCEANBASE_RPC_OBRPC_OB_RPC_SESSION_HANDLER_

#include <stdint.h>

#include "io/easy_io.h"

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_thread_cond.h"

namespace oceanbase
{
namespace rpc
{
class ObRequest;
} // end of namespace rpc

namespace obrpc
{

struct ObRpcReverseKeepaliveArg;
class ObRpcSessionHandler
{
public:
  ObRpcSessionHandler();
  virtual ~ObRpcSessionHandler() {};

  /**
   * prepare for wait next request packet.
   * this function for some worker thread want to suspend
   * execute for wait next request from client on same channel.
   * called before send_response avoid next request missed by wait thread.
   */
  virtual int prepare_for_next_request(int64_t session_id);

  /**
   * wait for next request packet from client on same channel.
   * called after send_response, call prepare_for_next_request first.
   * @param session_id
   * @param next_request packet object
   * @param timeout
   */
  virtual int wait_for_next_request(int64_t session_id,
                                    rpc::ObRequest *&req,
                                    const int64_t timeout,
                                    const ObRpcReverseKeepaliveArg& reverse_keepalive_arg);

  bool wakeup_next_thread(rpc::ObRequest &req);

  virtual int destroy_session(int64_t session_id);

  virtual int64_t generate_session_id();

  inline void set_max_wait_thread_count(const uint64_t max_wait_count)
  { max_waiting_thread_count_ = max_wait_count; }

private:
  enum { MAX_COND_COUNT = common::OB_MAX_CPU_NUM * 32 };
  struct WaitObject
  {
    int64_t thid_;
    rpc::ObRequest *req_;
    WaitObject()
        : thid_(-1), req_(NULL)
    {}
    WaitObject(rpc::ObRequest *req)
        : req_(req)
    {}
  };

  int get_session_id(const rpc::ObRequest &req, int64_t &session_id) const;
  common::ObThreadCond& get_next_cond_(int64_t id) { return next_cond_[id % MAX_COND_COUNT]; }

private:
  static const int32_t DEFAULT_WAIT_TIMEOUT_MS = 1000;
  static const int32_t MAX_WAIT_TIMEOUT_MS = 30000;
  static const int64_t MAX_WAIT_THREAD_COUNT = 100;

private:
  common::hash::ObHashMap<int64_t, WaitObject, common::hash::SpinReadWriteDefendMode> next_wait_map_;
  common::ObThreadCond next_cond_[MAX_COND_COUNT];

  volatile uint64_t sessid_;
  volatile uint64_t waiting_thread_count_;
  uint64_t max_waiting_thread_count_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcSessionHandler);
};

}
}

#endif
