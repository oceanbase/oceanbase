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

#ifndef OCEANBASE_OBSERVER_TABLET_LOCATION_BROADCAST_
#define OCEANBASE_OBSERVER_TABLET_LOCATION_BROADCAST_

#include "observer/ob_uniq_task_queue.h"
#include "share/transfer/ob_transfer_info.h"               // ObTransferTaskInfo
#include "share/location_cache/ob_location_update_task.h"  // ObTabletLocationSendTask
#include "share/rpc/ob_async_rpc_proxy.h"                  // ObSrvRpcProxy, RPC_F


namespace oceanbase
{
namespace share
{
class ObTabletLocationSender;
class ObTabletLocationUpdater;
class ObTabletLSService;
typedef observer::ObUniqTaskQueue<share::ObTabletLocationBroadcastTask,
    ObTabletLocationSender> ObTLSenderQueue;
typedef observer::ObUniqTaskQueue<share::ObTabletLocationBroadcastTask,
    ObTabletLocationUpdater> ObTLUpdaterQueue;
RPC_F(obrpc::OB_TABLET_LOCATION_BROADCAST, obrpc::ObTabletLocationSendArg,
      obrpc::ObTabletLocationSendResult, ObTabletLocationSendProxy);

class TabletLocationStatistics
{
public:
  const int64_t ONE_SECOND_US = 1 * 1000 * 1000L;
  const static int64_t CHECK_INTERVAL_US = 10 * 1000 * 1000L; // 10s
  TabletLocationStatistics() :
    lock_(), task_cnt_(0), tablet_suc_cnt_(0), tablet_fail_cnt_(0),
    total_exec_us_(0), total_wait_us_(0) {}
  ~TabletLocationStatistics() {}
  void dump_statistics(const int exec_ret,
                      const int64_t exec_ts,
                      const int64_t wait_ts,
                      const int64_t tablet_cnt,
                      const int64_t rate_limit = -1);
  TO_STRING_KV(K_(tablet_suc_cnt), K_(tablet_fail_cnt), K_(total_exec_us), K_(total_wait_us));
private:
  void reset_();
  int64_t get_total_cnt_() const;
  void calc_(const int ret,
            const int64_t exec_us,
            const int64_t wait_us,
            const int64_t tablet_cnt);
private:
  common::ObSpinLock lock_;
  int64_t task_cnt_;
  int64_t tablet_suc_cnt_;
  int64_t tablet_fail_cnt_;
  uint64_t total_exec_us_;
  uint64_t total_wait_us_;
};

class TabletLocationRateLimit
{
public:
  const int64_t ONE_SECOND_US = 1 * 1000 * 1000L;
public:
  TabletLocationRateLimit() :
    lock_(), tablet_cnt_(0), start_ts_(common::OB_INVALID_TIMESTAMP) {}
  ~TabletLocationRateLimit() {}
  void control_rate_limit(const int64_t tablet_cnt,
                          const int64_t exec_ts,
                          int64_t rate_limit_conf,
                          int64_t &wait_ts);
  TO_STRING_KV(K_(tablet_cnt), K_(start_ts));
private:
  void reset_();
  int64_t calc_wait_ts_(const int64_t tablet_cnt,
                       const int64_t exec_ts,
                       const int64_t frequency);
private:
  common::ObSpinLock lock_;
  int64_t tablet_cnt_;
  int64_t start_ts_;
};

class ObTabletLocationSender
{
public:
  ObTabletLocationSender()
    : inited_(false), stopped_(true), srv_rpc_proxy_(nullptr),
      send_queue_(), statistics_(), rate_limit_() {}
  virtual ~ObTabletLocationSender() { destroy(); }
  int init(obrpc::ObSrvRpcProxy * srv_rpc_proxy);
  int check_inner_stat() const;
  // Unused.
  int process_barrier(const ObTabletLocationBroadcastTask &task, bool &stopped);
  void stop();
  void wait();
  void destroy();

  int submit_broadcast_task(const ObTabletLocationBroadcastTask &task_info);
  int batch_process_tasks(const common::ObIArray<share::ObTabletLocationBroadcastTask> &tasks,
                          bool &stopped);
  inline int64_t get_rate_limit()
  {
    return GCONF._auto_broadcast_tablet_location_rate_limit;
  }
private:
  static const int64_t THREAD_CNT = 1;
  static const int64_t TASK_QUEUE_SIZE = 100000;
  bool inited_;
  bool stopped_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;
  ObTLSenderQueue send_queue_;
  TabletLocationStatistics statistics_;
  TabletLocationRateLimit rate_limit_;
};

class ObTabletLocationUpdater
{
public:
  ObTabletLocationUpdater()
    : inited_(false), stopped_(true),
      update_queue_(), statistics_() {}
  virtual ~ObTabletLocationUpdater() { destroy(); }
  int init(share::ObTabletLSService *tablet_ls_service);
  int check_inner_stat() const;
  // Unused.
  int process_barrier(const ObTabletLocationBroadcastTask &task, bool &stopped);
  void stop();
  void wait();
  void destroy();

  int submit_update_task(const share::ObTabletLocationBroadcastTask &received_task);
  int batch_process_tasks(const common::ObIArray<share::ObTabletLocationBroadcastTask> &tasks,
                          bool &stopped);
private:
  static const int64_t THREAD_CNT = 1;
  static const int64_t TASK_QUEUE_SIZE = 100000;
  bool inited_;
  bool stopped_;
  share::ObTabletLSService *tablet_ls_service_;
  ObTLUpdaterQueue update_queue_;
  TabletLocationStatistics statistics_;
};

} // end namespace share
} // end namespace oceanbase
#endif  // OCEANBASE_OBSERVER_TABLET_LOCATION_BROADCAST_