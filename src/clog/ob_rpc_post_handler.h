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

#ifndef OCEANBASE_CLOG_OB_RPC_POST_HANDLER_
#define OCEANBASE_CLOG_OB_RPC_POST_HANDLER_
#include "ob_buffer_task.h"
#include "lib/lock/ob_fcond.h"
#include "lib/queue/ob_link_queue.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace clog {
class ObILogRpc;
class ObRpcPostHandler : public share::ObThreadPool {
public:
  struct Task;
  ObRpcPostHandler() : rpc_(NULL)
  {}
  virtual ~ObRpcPostHandler()
  {
    destroy();
  }
  int init(ObILogRpc* rpc);
  int post(const common::ObAddr& server, const int pcode, const char* data, const int64_t len);
  int submit(
      ObIBatchBufferTask* callback, const common::ObAddr& server, const int pcode, const char* buf, const int64_t len);
  void stop();
  void wait();
  void destroy();

private:
  void run1();
  int handle_task();

private:
  const static int64_t WAIT_TASK_TIMEOUT = 100 * 1000;
  ObILogRpc* rpc_;
  common::ObLinkQueue queue_;
  common::ObFCond cond_;
  DISALLOW_COPY_AND_ASSIGN(ObRpcPostHandler);
};
};      // end namespace clog
};      // end namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_RPC_POST_HANDLER_
