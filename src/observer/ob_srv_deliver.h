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
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{

namespace omt
{
class ObTenant;
} // end of namespace omt
namespace obrpc
{
class ObRpcSessionHandler;
} // end of namespace obrpc

namespace observer
{

using rpc::frame::ObReqQueue;
using rpc::frame::ObiReqQHandler;
using obrpc::ObRpcSessionHandler;

class QueueThread
{
public:
  QueueThread(const char *thread_name = nullptr,
              uint64_t tenant_id = OB_SERVER_TENANT_ID,
              uint64_t group_id = share::OBCG_DEFAULT)
      : thread_(queue_, thread_name, tenant_id, group_id), tg_id_(-1),
        tenant_id_(tenant_id), n_thread_(0) {}

  ~QueueThread() { destroy(); }

  int init() { return queue_.init(tenant_id_); }

public:
  int set_thread_count(int thread_cnt) {
    int ret = OB_SUCCESS;
    if (thread_cnt != n_thread_) {
      ret = TG_SET_THREAD_CNT(tg_id_, thread_cnt);
      n_thread_ = thread_cnt;
    }
    return ret;
  }
  void stop()
  {
    if (tg_id_ != -1) {
      TG_STOP(tg_id_);
    }
  }
  void wait()
  {
    if (tg_id_ != -1) {
      TG_WAIT(tg_id_);
    }
  }
  void destroy()
  {
    if (tg_id_ != -1) {
      TG_DESTROY(tg_id_);
      tg_id_ = -1;
    }
  }
  class Thread : public lib::TGRunnable {
  public:
    Thread(ObReqQueue &queue, const char *thread_name,
            const uint64_t tenant_id, const uint64_t group_id)
        : queue_(queue), thread_name_(thread_name),
          tenant_id_(tenant_id), group_id_(group_id) {}
    void run1()
    {
      if (thread_name_ != nullptr) {
        lib::set_thread_name(thread_name_, get_thread_idx());
      }
      lib::SET_GROUP_ID(group_id_);
      queue_.loop();
    }

  private:
    ObReqQueue &queue_;
    const char *thread_name_;
    const uint64_t tenant_id_;
    const uint64_t group_id_;
  } thread_;
  ObReqQueue queue_;
  int tg_id_;

private:
  uint64_t tenant_id_;
  int n_thread_;
};

class ObSrvDeliver
    : public rpc::frame::ObReqQDeliver
{
public:
  ObSrvDeliver(ObiReqQHandler &qhandler,
               ObRpcSessionHandler &session_handler,
               ObGlobalContext &gctx);

  int init();
  void stop();

  int repost(void* node);
  virtual int deliver(rpc::ObRequest &req);
  void set_host(const common::ObAddr &host) { host_ = host; }
  int create_queue_thread(int tg_id, const char *thread_name, QueueThread *&qthread);
  int get_mysql_login_thread_count_to_set(int cfg_cnt);
  int set_mysql_login_thread_count(int cnt);
  virtual int lock_tenant_list() override;
  virtual int unlock_tenant_list() override;
private:
  int init_queue_threads();

  int deliver_rpc_request(rpc::ObRequest &req);

  int deliver_mysql_request(rpc::ObRequest &req);
  int acquire_diagnostic_info_object(int64_t tenant_id, int64_t group_id, int64_t session_id,
      ObDiagnosticInfo *&di, bool using_cache = false, bool check_throttle = false, omt::ObTenant *tenant = NULL);

private:
  bool is_inited_;
  bool stop_;
  common::ObAddr host_;
  QueueThread *lease_queue_;
  QueueThread *ddl_queue_;
  QueueThread *ddl_parallel_queue_;
  QueueThread *mysql_queue_;
  QueueThread *diagnose_queue_;
  ObRpcSessionHandler &session_handler_;
  ObGlobalContext &gctx_;
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
}; // end of class ObSrvDeliver

} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OB_SRV_DELIVER_H_ */
