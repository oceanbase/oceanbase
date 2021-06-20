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

#ifndef _OCEABASE_RPC_FRAME_OB_REQ_QUEUE_THREAD_H_
#define _OCEABASE_RPC_FRAME_OB_REQ_QUEUE_THREAD_H_

#include "lib/ob_define.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/net/ob_addr.h"
#include "rpc/frame/obi_req_qhandler.h"
#include "lib/thread/thread_pool.h"

namespace oceanbase {
namespace rpc {
class ObRequest;
namespace frame {
using common::ObAddr;

class ObReqQueue {
public:
  static const int LIGHTY_QUEUE_SIZE = (1 << 18);
  ObReqQueue(int queue_capacity = LIGHTY_QUEUE_SIZE);

  virtual ~ObReqQueue();

  void set_qhandler(ObiReqQHandler* handler);

  bool push(ObRequest* req, int max_queue_len, bool block = true);

  void set_host(const common::ObAddr& host);
  void loop();

  size_t size() const
  {
    return queue_.size();
  }

private:
  int process_task(void* task);

  DISALLOW_COPY_AND_ASSIGN(ObReqQueue);

protected:
  bool wait_finish_;
  bool stop_;

  common::ObLightyQueue queue_;
  ObiReqQHandler* qhandler_;

  static const int64_t MAX_PACKET_SIZE = 2 * 1024 * 1024L;  // 2M

  ObAddr host_;
};

class ObReqQueueThread : public ObReqQueue {
public:
  ObReqQueueThread() : thread_(*this)
  {}
  lib::ThreadPool& get_thread()
  {
    return thread_;
  }

private:
  class Thread : public lib::ThreadPool {
  public:
    Thread(ObReqQueue& queue) : queue_(queue)
    {}
    void run1()
    {
      queue_.loop();
    }
    ObReqQueue& queue_;
  } thread_;
};

}  // end namespace frame
}  // end namespace rpc
}  // end namespace oceanbase

#endif /* _OCEABASE_RPC_FRAME_OB_REQ_QUEUE_THREAD_H_ */
