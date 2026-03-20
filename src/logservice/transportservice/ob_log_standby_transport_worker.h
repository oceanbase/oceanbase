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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_STANDBY_TRANSPORT_WORKER_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_STANDBY_TRANSPORT_WORKER_H_

#include "lib/queue/ob_lighty_queue.h"                      // ObLightyQueue
#include "common/ob_queue_thread.h"                         // ObCond
#include "lib/thread/thread_mgr_interface.h"                // TGRunnable
#include "share/ob_ls_id.h"                                 // ObLSID
#include "logservice/transportservice/ob_log_transport_rpc_define.h"

namespace oceanbase
{
namespace storage
{
class ObLSService;
}

namespace logservice
{
class ObLogService;

// TODO by qingxia: consider thread number, current is 1, may be not enough
// 强同步备库接收日志的独立worker线程
// 使用 TG 线程框架管理线程生命周期
class ObLogStandbyTransportWorker : public lib::TGRunnable
{
public:
  ObLogStandbyTransportWorker();
  ~ObLogStandbyTransportWorker();

  int init(const uint64_t tenant_id,
           storage::ObLSService *ls_svr,
           ObLogService *log_service);
  void destroy();
  int start();
  void stop();
  void wait();
  void signal();

public:
  // 提交日志接收任务到队列
  // @retval OB_SIZE_OVERFLOW    task num more than queue limit
  // @retval other code          unexpected error
  int submit_transport_task(const ObLogTransportReq &req);

  // 检查是否需要停止接收日志（failover时）
  bool need_stop() const { return ATOMIC_LOAD(&stop_flag_); }

private:
  void run1() override;
  void do_thread_task_();

private:
  bool is_inited_;
  bool stop_flag_;
  int tg_id_;
  uint64_t tenant_id_;
  storage::ObLSService *ls_svr_;
  ObLogService *log_service_;
  common::ObCond cond_;
  int64_t not_init_warn_time_us_;
  int64_t running_info_print_time_us_;

  static const int64_t THREAD_RUN_INTERVAL = 10 * 1000L;  // 10ms
  // 分批处理大小：每次处理每个日志流的任务数量，控制排序开销
  static const int64_t BATCH_SIZE = 512;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStandbyTransportWorker);
};

} // namespace logservice
} // namespace oceanbase

#endif /* OCEANBASE_LOGSERVICE_OB_LOG_STANDBY_TRANSPORT_WORKER_H_ */
