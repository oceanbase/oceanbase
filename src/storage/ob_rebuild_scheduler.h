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

#ifndef OCEANBASE_STORAGE_OB_REBUILD_SCHEDULER_H_
#define OCEANBASE_STORAGE_OB_REBUILD_SCHEDULER_H_

#include "lib/net/ob_addr.h"
#include "lib/hash/ob_hashset.h"
#include "lib/queue/ob_dedup_queue.h"
#include "lib/task/ob_timer.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_heap.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "common/ob_partition_key.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase {
namespace obrpc {
class ObPartitionServiceRpcProxy;
}
namespace storage {
class ObPartitionService;
class ObRebuildReplicaTaskScheduler;

struct ObRebuildReplicaInfo {
  enum RebuildAction { PREPARE = 0, DOING };
  ObRebuildReplicaInfo();
  bool is_valid() const;
  void reset();

  ObPGKey pg_key_;
  ObReplicaType replica_type_;
  ObReplicaProperty replica_property_;
  common::ObAddr parent_addr_;
  ObReplicaType parent_replica_type_;
  int64_t local_minor_snapshot_version_;
  int64_t local_major_snapshot_version_;
  int64_t remote_minor_snapshot_version_;
  int64_t remote_major_snapshot_version_;
  bool is_share_major_;
  RebuildAction rebuild_action_;
  int64_t local_last_replay_log_id_;
  int64_t remote_last_replay_log_id_;

  TO_STRING_KV(K_(pg_key), K_(replica_type), K_(replica_property), K_(parent_addr), K_(parent_replica_type),
      K_(local_minor_snapshot_version), K_(local_major_snapshot_version), K_(remote_minor_snapshot_version),
      K_(remote_major_snapshot_version), K_(rebuild_action), K_(is_share_major), K_(local_last_replay_log_id),
      K_(remote_last_replay_log_id));
};

struct ObRebuildReplicaResult {
  ObRebuildReplicaResult();
  bool is_valid() const;
  void reset();
  int set_result(const ObPGKey& pg_key, const int32_t result);
  int32_t count() const
  {
    return results_.count();
  }

  ObArray<int32_t> results_;
  ObArray<ObPGKey> pg_keys_;
  TO_STRING_KV(K_(results), K_(pg_keys));
};

class ObRebuildReplicaService;
class ObRebuildReplicaTaskProducer : public common::ObTimerTask {
public:
  ObRebuildReplicaTaskProducer(ObRebuildReplicaTaskScheduler& scheduler);
  virtual ~ObRebuildReplicaTaskProducer();
  int init(ObPartitionService* partition_service, obrpc::ObPartitionServiceRpcProxy* srv_rpc_proxy,
      ObRebuildReplicaService* rebuild_replica_service);

  inline void stop()
  {
    stopped_ = true;
  }
  void destroy();
  virtual void runTimerTask();

private:
  struct ObRebuildReplicaConvergeInfo {
    ObRebuildReplicaConvergeInfo();
    int assign(const ObRebuildReplicaConvergeInfo& task);
    int add_replica_info(const ObRebuildReplicaInfo& replica_info);
    bool is_valid() const;

    ObArray<ObRebuildReplicaInfo> replica_info_array_;
    common::ObAddr parent_addr_;

    TO_STRING_KV(K_(replica_info_array), K_(parent_addr));
    DISALLOW_COPY_AND_ASSIGN(ObRebuildReplicaConvergeInfo);
  };

private:
  int generate_replica_info();
  int build_local_replica_info();
  int get_remote_replica_info();
  int inner_get_remote_replica_info(ObRebuildReplicaConvergeInfo& converge_info);
  int set_replica_info(const ObRebuildReplicaInfo& d_replica_info);
  int add_replica_info();

private:
  bool is_inited_;
  volatile bool stopped_;
  storage::ObPartitionService* partition_service_;
  obrpc::ObPartitionServiceRpcProxy* srv_rpc_proxy_;
  ObRebuildReplicaService* rebuild_replica_service_;
  common::ObArray<ObRebuildReplicaConvergeInfo> converge_info_array_;
  ObRebuildReplicaTaskScheduler& scheduler_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRebuildReplicaTaskProducer);
};

class ObRebuildReplicaTaskScheduler : public lib::ThreadPool {
public:
  ObRebuildReplicaTaskScheduler();
  virtual ~ObRebuildReplicaTaskScheduler();
  int init(ObPartitionService* partition_service, ObRebuildReplicaService* rebuild_replica_service);

  void destroy();
  void run1() final;
  void notify();

private:
  struct HeapComparator {
    explicit HeapComparator(int& ret);
    bool operator()(const ObRebuildReplicaInfo* info1, const ObRebuildReplicaInfo* info2);
    int get_error_code()
    {
      return ret_;
    }

  private:
    int& ret_;
  };

private:
  typedef common::ObBinaryHeap<const ObRebuildReplicaInfo*, HeapComparator> Heap;
  typedef common::hash::ObHashMap<ObPGKey, ObRebuildReplicaInfo, common::hash::NoPthreadDefendMode> ObReplicaInfoMap;
  int build_task_heap(Heap& heap);
  int add_task(Heap& heap);
  int rebuild_replica_info_map();
  int add_replica_info_to_heap(Heap& heap);
  int change_rebuild_action(const ObPGKey& pg_key, const ObRebuildReplicaInfo::RebuildAction rebuild_action);

private:
  static const int64_t SCHEDULER_WAIT_TIME_MS = 60 * 1000;  // 60s
  bool is_inited_;
  ObPartitionService* partition_service_;
  ObRebuildReplicaService* rebuild_replica_service_;
  int cmp_ret_;
  HeapComparator compare_;
  ObReplicaInfoMap replica_info_map_;
  common::ObThreadCond cond_;
  bool need_idle_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRebuildReplicaTaskScheduler);
};

class ObRebuildReplicaService {
public:
  ObRebuildReplicaService();
  virtual ~ObRebuildReplicaService();
  void destroy();
  int init(storage::ObPartitionService& partition_service, obrpc::ObPartitionServiceRpcProxy& srv_rpc_proxy);
  int set_replica_info(const common::ObIArray<ObRebuildReplicaInfo>& replica_info_array);
  int get_replica_info(common::ObIArray<ObRebuildReplicaInfo>& replica_info_array);
  ObRebuildReplicaTaskScheduler& get_scheduler()
  {
    return scheduler_;
  }
  void stop();

private:
  static const int64_t DELAY_US = 15LL * 1000LL * 1000LL;  // 15s
  bool is_inited_;
  common::TCRWLock lock_;
  ObRebuildReplicaTaskScheduler scheduler_;
  ObRebuildReplicaTaskProducer task_producer_;
  ObArray<ObRebuildReplicaInfo> replica_info_array_;
  common::ObTimer task_procuder_timer_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRebuildReplicaService);
};

}  // end namespace storage
}  // end namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_REBUILD_SCHEDULER_H_
