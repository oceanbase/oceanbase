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

#ifndef _OCEABASE_OBSERVER_OB_SRV_DELIVER_H_
#define _OCEABASE_OBSERVER_OB_SRV_DELIVER_H_

#include "lib/thread/ob_thread_name.h"
#include "lib/thread/thread_mgr_interface.h"
#include "rpc/frame/ob_req_deliver.h"
#include "share/ob_thread_pool.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {

namespace omt {
class ObTenant;
}  // end of namespace omt
namespace obrpc {
class ObRpcSessionHandler;
}  // end of namespace obrpc

namespace observer {

using obrpc::ObRpcSessionHandler;
using rpc::frame::ObiReqQHandler;
using rpc::frame::ObReqQueue;

class QueueThread {
public:
  QueueThread(const char* thread_name = nullptr) : thread_(queue_, thread_name)
  {}

public:
  class Thread : public lib::TGRunnable {
  public:
    Thread(ObReqQueue& queue, const char* thread_name) : queue_(queue), thread_name_(thread_name)
    {}
    void run1()
    {
      if (thread_name_ != nullptr) {
        lib::set_thread_name(thread_name_, get_thread_idx());
      }
      queue_.loop();
    }

  private:
    ObReqQueue& queue_;
    const char* thread_name_;
  } thread_;
  ObReqQueue queue_;
};

class ObSrvDeliver : public rpc::frame::ObReqQDeliver {
public:
  ObSrvDeliver(ObiReqQHandler& qhandler, ObRpcSessionHandler& session_handler, ObGlobalContext& gctx);

  int init();
  void stop();

  int repost(void* node);
  virtual int deliver(rpc::ObRequest& req);
  void set_host(const common::ObAddr& host)
  {
    host_ = host;
  }

  int create_queue_thread(int tg_id, const char* thread_name, QueueThread*& qthread);

private:
  int init_queue_threads();

  int deliver_rpc_request(rpc::ObRequest& req);

  int deliver_mysql_request(rpc::ObRequest& req);

private:
  bool is_inited_;
  bool stop_;
  common::ObAddr host_;
  QueueThread* lease_queue_;
  QueueThread* ddl_queue_;
  QueueThread* mysql_queue_;
  QueueThread* diagnose_queue_;
  ObRpcSessionHandler& session_handler_;
  ObGlobalContext& gctx_;

  DISALLOW_COPY_AND_ASSIGN(ObSrvDeliver);

public:
  static const int64_t MAX_QUEUE_LEN = 10000;
  static const int LEASE_TASK_THREAD_CNT = 3;
  static const int MINI_MODE_LEASE_TASK_THREAD_CNT = 1;
  static const int DDL_TASK_THREAD_CNT = 1;
  static const int MYSQL_TASK_THREAD_CNT = 6;
  static const int MINI_MODE_MYSQL_TASK_THREAD_CNT = 2;
  static const int MYSQL_DIAG_TASK_THREAD_CNT = 2;
  static const int MINI_MODE_MYSQL_DIAG_TASK_THREAD_CNT = 1;
};  // end of class ObSrvDeliver

}  // end of namespace observer
}  // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OB_SRV_DELIVER_H_ */
