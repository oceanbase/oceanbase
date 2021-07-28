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

#ifndef OCEANBASE_OBSERVER_OB_PARTITION_LOCATION_UPDATER_H_
#define OCEANBASE_OBSERVER_OB_PARTITION_LOCATION_UPDATER_H_

#include "share/ob_srv_rpc_proxy.h"
#include "observer/ob_uniq_task_queue.h"
#include "share/partition_table/ob_partition_location_task.h"
namespace oceanbase {
namespace storage {
class ObIAliveServerTracer;
class ObPartitionService;
}  // namespace storage
namespace share {
class ObPartitionLocationCache;
}
namespace obrpc {
class ObSrvRpcProxy;
}
namespace observer {
class ObPartitionLocationUpdater;
class ObService;

typedef ObUniqTaskQueue<share::ObPartitionBroadcastTask, ObPartitionLocationUpdater> ObPTSenderQueue;
typedef ObUniqTaskQueue<share::ObPartitionUpdateTask, ObPartitionLocationUpdater> ObPTReceiverQueue;

class ObPartitionLocationUpdater {
public:
  const static int64_t UPDATER_THREAD_CNT = 1;
  const static int64_t MINI_MODE_MAX_PARTITION_CNT = 10000;
  const static int64_t MAX_PARTITION_CNT = 1000000;
  const static int64_t CHECK_INTERVAL_US = 1 * 1000 * 1000L;  // 1s

  enum QueueType { SENDER = 0, RECEIVER = 1, MAX_TYPE = 2 };

  ObPartitionLocationUpdater()
      : inited_(false),
        stopped_(false),
        thread_cnt_(UPDATER_THREAD_CNT),
        queue_size_(MINI_MODE_MAX_PARTITION_CNT),
        ob_service_(NULL),
        partition_service_(NULL),
        srv_rpc_proxy_(NULL),
        location_cache_(NULL),
        server_tracer_(NULL),
        sender_(),
        receiver_()
  {}
  virtual ~ObPartitionLocationUpdater()
  {
    destroy();
  }

  int init(observer::ObService& ob_service, storage::ObPartitionService*& partition_service,
      obrpc::ObSrvRpcProxy*& srv_rpc_proxy, share::ObPartitionLocationCache*& location_cache,
      share::ObIAliveServerTracer& server_tracer);

  void stop();
  void wait();
  void destroy();

  virtual int submit_broadcast_task(const share::ObPartitionBroadcastTask& task);
  virtual int submit_update_task(const share::ObPartitionUpdateTask& task);

  virtual int process_barrier(const share::ObPartitionBroadcastTask& task, bool& stopped);
  virtual int process_barrier(const share::ObPartitionUpdateTask& task, bool& stopped);

  virtual int batch_process_tasks(const common::ObIArray<share::ObPartitionBroadcastTask>& tasks, bool& stopped);
  virtual int batch_process_tasks(const common::ObIArray<share::ObPartitionUpdateTask>& tasks, bool& stopped);

private:
  int check_inner_stat() const;
  void dump_statistic(
      const QueueType queue_type, const int exec_ret, const int64_t exec_ts, const int64_t wait_ts, const int64_t cnt);
  void control_rate_limit(const QueueType queue_type, const int64_t exec_ts, const int64_t cnt,
      const int64_t rate_limit_conf, int64_t& wait_ts);

private:
  bool inited_;
  bool stopped_;
  int64_t thread_cnt_;
  int64_t queue_size_;
  observer::ObService* ob_service_;
  storage::ObPartitionService* partition_service_;
  obrpc::ObSrvRpcProxy* srv_rpc_proxy_;
  share::ObPartitionLocationCache* location_cache_;
  share::ObIAliveServerTracer* server_tracer_;
  ObPTSenderQueue sender_;
  ObPTReceiverQueue receiver_;
};
}  // end namespace observer
}  // end namespace oceanbase

#endif  // OCEANBASE_OBSERVER_OB_PARTITION_LOCATION_UPDATER_H_
