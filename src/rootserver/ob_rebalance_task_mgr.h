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

#ifndef OCEANBASE_ROOTSERVER_OB_REBALANCE_TASK_MGR_H_
#define OCEANBASE_ROOTSERVER_OB_REBALANCE_TASK_MGR_H_

#include "lib/lock/ob_thread_cond.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "ob_rebalance_task.h"

namespace oceanbase {
namespace share {
class ObPartitionTableOperator;
}
namespace common {
class ObServerConfig;
}
namespace obrpc {
class ObSrvRpcProxy;
}
namespace sql {
class ObBKGDTaskCompleteArg;
}

namespace rootserver {
class ObRebalanceTaskExecutor;
class ObServerManager;
class ObRootBalancer;
class ObGlobalIndexBuilder;
class ObRebalanceTaskMgr;
class ObServerRecoveryMachine;

class ObRebalanceTaskQueue {
public:
  typedef common::ObDList<ObRebalanceTask> TaskList;
  typedef common::hash::ObHashMap<ObRebalanceTaskKey, ObRebalanceTaskInfo*, common::hash::NoPthreadDefendMode>
      TaskInfoMap;

  ObRebalanceTaskQueue();
  virtual ~ObRebalanceTaskQueue();

  int init(common::ObServerConfig& config, ObTenantTaskStatMap& tenant_stat, ObServerTaskStatMap& server_stat,
      const int64_t bucket_num, obrpc::ObSrvRpcProxy* rpc_proxy);

  // If partition already has task in queue (or in schedule), return OB_ENTRY_EXIST.
  // Set %task to exist task or new add task for OB_ENTRY_EXIST or OB_SUCCESS returned.
  int push_task(ObRebalanceTaskMgr& task_mgr, const ObRebalanceTaskQueue& sibling_queue, const ObRebalanceTask& task,
      bool& has_task_info_in_schedule);

  // Try to pop one task for executing.
  // Get task from %wait_list_ then set to schedule state and move to %schedule_list_.
  //
  // Return OB_SUCCESS and assign NULL to %task, if no task can be scheduled
  // (e.g.: all task exceed server schedule limit).
  int pop_task(ObRebalanceTask*& task);

  // Get in schedule task by task_info.
  // Used to detect whether the finished task is scheduled from this queue,
  // when receive MIGRATE_OVER.
  //
  // Set %task to NULL if in schedule task not found.
  int get_schedule_task(const ObRebalanceTask& input_task, ObRebalanceTask*& task);
  int get_schedule_task(const ObRebalanceTaskInfo& task_info, const ObAddr& dest, ObRebalanceTask*& task);

  // Get execute timeout task if exist.
  // Set %task to NULL if no timeout task.
  int get_timeout_task(const int64_t network_bandwidth, ObRebalanceTask*& task);

  // get the task that not in progress on observer
  int get_not_in_progress_task(ObServerManager* server_mgr, ObRebalanceTask*& task);

  // Get discard tasks which in schedule
  int get_discard_in_schedule_task(
      const common::ObAddr& server, const int64_t start_service_time, ObRebalanceTask*& task);

  int discard_waiting_task(const common::ObAddr& server, const int64_t start_service_time);

  // Update statistics and remove task from %in_schedule_list_ when finish schedule.
  // When finished, task memory will be released and %task can not be used again.
  int finish_schedule(ObRebalanceTask* task);

  // Set or clear sibling in schedule flag.
  int set_sibling_in_schedule(const ObRebalanceTask& task, const bool in_schedule);

  int set_sibling_in_schedule(const ObRebalanceTaskKey& task_key, const bool in_schedule);

  int has_in_schedule_task_info(const ObRebalanceTaskKey& pkey, bool& has) const;

  int has_in_migrate_or_transform_task_info(
      const common::ObPartitionKey& key, const common::ObAddr& addr, bool& has) const;

  int flush_wait_task(const uint64_t tenant_id, const obrpc::ObAdminClearBalanceTaskArg::TaskType& type);
  int flush_wait_task(const ObZone zone, const ObServerManager* server_mgr,
      const obrpc::ObAdminClearBalanceTaskArg::TaskType& type, const uint64_t tenant_id = OB_INVALID_ID);
  int64_t wait_task_cnt() const
  {
    return wait_list_.get_size();
  }
  int64_t in_schedule_task_cnt() const
  {
    return schedule_list_.get_size();
  }

  int64_t task_info_cnt()
  {
    return task_info_map_.size();
  }

  TaskList& get_wait_list()
  {
    return wait_list_;
  }
  TaskList& get_schedule_list()
  {
    return schedule_list_;
  }

  void reuse();

private:
  int do_push_task(ObRebalanceTaskMgr& task_mgr, const ObRebalanceTaskQueue& sibling_queue, const ObRebalanceTask& task,
      bool& has_task_info_in_schedule);
  int check_balance_task_can_pop(const ObRebalanceTaskInfo* task_info, const int64_t out_cnt_lmt,
      const int64_t in_cnt_lmt, bool& task_info_can_schedule);
  int check_backup_task_can_pop(const ObRebalanceTaskInfo* task_info, bool& task_info_can_schedule);

private:
  static const int64_t BACKUP_IN_CNT_LIMIT = 1;
  bool is_inited_;
  common::ObServerConfig* config_;

  ObTenantTaskStatMap* tenant_stat_map_;
  ObServerTaskStatMap* server_stat_map_;

  common::ObFIFOAllocator task_alloc_;
  TaskList wait_list_;
  TaskList schedule_list_;
  TaskInfoMap task_info_map_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObRebalanceTaskQueue);
};

//
// Rebalance task stands for partition replication or migration task.
// Rebalance task manager offers the following guarantees:
//
// 1. Tasks in schedule (are executing) limit:
//    - No more than %current_rebalance_task_limit tasks in cluster.
//    - No more than %server_copy_in_task_limit tasks copy data in for any Ob Server.
//    - No more than %server_copy_out_task_limit tasks data out for any Ob Server.
//    - Only one task for any partition. (replication or migration)
// 2. Replication task has a higher priority, migration no need to be scheduled if replication
//    task exists.
// 3. Only one replication task and one migration task exist for any partition.
class ObRebalanceTaskMgr : public ObRsReentrantThread {
public:
  const static int64_t TASK_QUEUE_LIMIT = 1 << 16;
  const static int64_t QUEUE_LOW_TASK_CNT = TASK_QUEUE_LIMIT / 4;
  const static int64_t ONCE_ADD_TASK_CNT = TASK_QUEUE_LIMIT / 2;
  const static int64_t SAFE_DISCARD_TASK_INTERVAL = 200 * 1000;            // 200ms
  const static int64_t NETWORK_BANDWIDTH = 20 * 1000 * 1000;               // 20MB/s
  const static int64_t DATA_IN_CLEAR_INTERVAL = 20 * 60 * 1000000;         // 60 min;
  const static int64_t CHECK_IN_PROGRESS_INTERVAL_PER_TASK = 5 * 1000000;  // 5s for each task
  const static int64_t CONCURRENCY_LIMIT_INTERVAL = 10 * 60 * 1000000L;    // 10min

  ObRebalanceTaskMgr();

  int init(common::ObServerConfig& config, ObRebalanceTaskExecutor& executor, ObServerManager* server_mgr,
      obrpc::ObSrvRpcProxy* rpc_proxy, ObRootBalancer* balancer, ObGlobalIndexBuilder* global_index_builder);
  // for unittest
  int init(common::ObServerConfig& config, ObRebalanceTaskExecutor& executor);

  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  void stop();

  // add_task() will never block. Task with no source_ field will be executed immediately.
  //
  // Caller should begin add rebalance task when associated queue's tasks less than
  // %QUEUE_LOW_TASK_CNT, and stop add task when add %ONCE_ADD_TASK_CNT tasks.
  //
  // Return OB_ENTRY_EXIST if the partition already has same type task in queue.
  // (caller should ignore OB_ENTRY_EXIST and continue add task).
  //
  // Return OB_SIZE_OVERFLOW if queue already full. (caller should stop adding task)
  virtual int add_task(const ObRebalanceTask& task);
  // if add success, increase %task_cnt
  virtual int add_task(const ObRebalanceTask& task, int64_t& task_info_cnt);
  // Call when execute finish. (after receive OB_ADD_REPLICA_RES, OB_MIGRATE_REPLICA_RES)
  virtual int execute_over(const ObRebalanceTask& task, const ObIArray<int>& rc_array);
  // If server restarted, previous generated rebalance tasks should be discarded.
  // %start_service_time may be INT64_MAX if all tasks should be discarded.
  virtual int discard_task(const common::ObAddr& server, const int64_t start_service_time);

  virtual int64_t get_high_priority_task_info_cnt() const;
  virtual int64_t get_low_priority_task_info_cnt() const;
  virtual int get_tenant_task_info_cnt(
      const uint64_t tenant_id, int64_t& high_priority_task_cnt, int64_t& low_priority_task_cnt) const;

  virtual int has_in_migrate_or_transform_task_info(
      const common::ObPartitionKey& pkey, const common::ObAddr& server, bool& has) const;

  // clear the task in wait_list_;
  int clear_task(const common::ObIArray<uint64_t>& tenant_ids, const obrpc::ObAdminClearBalanceTaskArg::TaskType& type);
  int clear_task(const common::ObIArray<ObZone>& zone_names, const obrpc::ObAdminClearBalanceTaskArg::TaskType& type,
      uint64_t tenant_id = OB_INVALID_ID);
  int clear_task(uint64_t tenant_id, const obrpc::ObAdminClearBalanceTaskArg::TaskType type);
  int get_min_out_data_size_server(
      const common::ObIArray<common::ObAddr>& candidate, common::ObIArray<int64_t>& candidate_idxs);

  void reuse();
  int set_self(const common::ObAddr& self);
  // schedule limit flag need be cleared when config updated
  void clear_reach_concurrency_limit()
  {
    concurrency_limited_ts_ = 0;
  }
  int64_t get_reach_concurrency_limit() const
  {
    return concurrency_limited_ts_;
  }
  void set_reach_concurrency_limit()
  {
    concurrency_limited_ts_ = common::ObTimeUtility::current_time();
  }

  bool is_busy() const
  {
    return get_high_priority_task_info_cnt() > QUEUE_LOW_TASK_CNT &&
           get_low_priority_task_info_cnt() > QUEUE_LOW_TASK_CNT;
  }
  bool is_idle() const
  {
    return 0 == get_high_priority_task_info_cnt() && 0 == get_low_priority_task_info_cnt();
  }
  int get_all_tasks(common::ObIAllocator& allocator, common::ObIArray<ObRebalanceTask*>& tasks);
  int get_all_tasks_count(
      int64_t& high_wait_count, int64_t& high_scheduled_count, int64_t& low_wait_count, int64_t& low_scheduled_count);
  int log_task_result(const ObRebalanceTask& task, const ObIArray<int>& rc_array);
  int check_dest_server_migrate_in_blocked(const ObRebalanceTask& task, bool& is_available);
  int check_dest_server_out_of_disk(const ObRebalanceTask& task, bool& is_available);
  int check_dest_server_has_too_many_partitions(const ObRebalanceTask& task, bool& is_available);
  int get_schedule_task(const ObRebalanceTaskInfo& task_info, const ObAddr& dest, common::ObIAllocator& allocator,
      ObRebalanceTask*& task);
  int64_t get_schedule_interval() const;

private:
  common::ObThreadCond& get_cond()
  {
    return cond_;
  }
  ObRebalanceTaskQueue& get_high_priority_queue()
  {
    return high_task_queue_;
  }
  ObRebalanceTaskQueue& get_low_priority_queue()
  {
    return low_task_queue_;
  }

  // Return OB_SUCCESS and assign NULL to %task, if no task can be scheduled
  int pop_task(ObRebalanceTask*& task);

  // Send task to execute. (execute_finish() will be called if send fail)
  int execute_task(const ObRebalanceTask& task);

  int dump_statistics() const;

  // check task in progress on observer, if not, discard the task and reschedule again
  int check_task_in_progress(int64_t& last_check_task_in_progress_ts);
  // the implementation of check_task_in_progress
  int do_check_task_in_progress();
  // Set or clear sibling in schedule flag.
  int set_sibling_in_schedule(const ObRebalanceTask& task, const bool in_schedule);

  // Do execute over work with lock acquired.
  // when a load balance task is executed, observer may return a error code
  // OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT, which tell rootserver this task is up to
  // the upper limit to migrate in. rootserver shall not schedule new task to the observer until the
  // following two situations:
  // 1 the migrate in restriction timeout
  // 2 a task finish from this restricted observer
  // every time do_execute_over() is invoked, need_try_clear_server_data_in_limit is used to tell whether
  // to clean up the migration in restriction
  int do_execute_over(const ObRebalanceTask& task, const ObIArray<int>& rc_array,
      const bool need_try_clear_server_data_in_limit = true);

  // for black list
  int process_failed_task(const ObRebalanceTask& task, const ObIArray<int>& rc_array);
  int get_task_cnt(int64_t& wait_cnt, int64_t& in_schedule_cnt);
  int set_server_data_in_limit(const ObAddr& addr);
  void try_clear_server_data_in_limit(const ObAddr& addr);
  int notify_global_index_builder(const ObRebalanceTask& task);
  virtual bool need_process_failed_task() const
  {
    return true;
  }

private:
  bool inited_;
  common::ObServerConfig* config_;

  // has waiting task but can not be scheduled task reach server_data_copy_[in/out]_concurrency
  volatile int64_t concurrency_limited_ts_;

  common::ObThreadCond cond_;

  ObTenantTaskStatMap tenant_stat_;
  ObServerTaskStatMap server_stat_;

  ObRebalanceTaskQueue queues_[ObRebalanceTaskPriority::MAX_PRI];
  ObRebalanceTaskQueue& high_task_queue_;  // queues_[0]
  ObRebalanceTaskQueue& low_task_queue_;   // queues_[1]
  common::ObAddr self_;
  ObRebalanceTaskExecutor* task_executor_;
  ObServerManager* server_mgr_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  ObRootBalancer* balancer_;
  ObGlobalIndexBuilder* global_index_builder_;

  DISALLOW_COPY_AND_ASSIGN(ObRebalanceTaskMgr);
};

}  // end namespace rootserver
}  // end namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_REBALANCE_TASK_MGR_H_
