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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_SCHEDULER_
#define OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_SCHEDULER_

#include "lib/queue/ob_lighty_queue.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_id_map.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "common/ob_queue_thread.h"
#include "sql/executor/ob_job_parser.h"
#include "sql/executor/ob_distributed_job_control.h"
#include "sql/executor/ob_task_spliter_factory.h"
#include "sql/executor/ob_addrs_provider_factory.h"
#include "sql/executor/ob_sql_scheduler.h"
#include "sql/executor/ob_local_job_control.h"
#include "sql/executor/ob_task_event.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/executor/ob_sql_execution_id_map.h"
#include "sql/executor/ob_trans_result_collector.h"
#include "sql/ob_sql_trans_util.h"

namespace oceanbase {
namespace sql {
class ObDistributedJobExecutor;
class ObExecStatCollector;
class ObDistributedSchedulerManager;
class ObDistributedSchedulerCtx;
class ObDistributedExecContext;
class ObDistributedScheduler : public ObSqlScheduler {
public:
  friend class ObSignalFinishQueue;
  friend class ObDistributedSchedulerManager;

  ObDistributedScheduler();
  virtual ~ObDistributedScheduler();

  virtual void reset();
  // called by query thread

  virtual int schedule(ObExecContext& ctx, ObPhysicalPlan* phy_plan);
  int parse_all_jobs_and_start_root_job(ObExecContext& ctx, ObPhysicalPlan* phy_plan);
  int kill_all_jobs(ObExecContext& ctx, ObJobControl& jc);
  int close_all_results(ObExecContext& ctx);
  int init();
  int stop();
  inline void set_exec_stat_collector(ObExecStatCollector* collector);
  inline ObExecStatCollector* get_exec_stat_collector();
  inline int init_trans_result(ObSQLSessionInfo& session, ObExecutorRpcImpl* exec_rpc);
  inline uint64_t get_execution_id()
  {
    return execution_id_;
  }
  int pop_task_result_for_root(
      ObExecContext& ctx, uint64_t root_op_id, ObTaskResult& task_result, int64_t timeout_timestamp);
  int pop_task_event_for_sche(const ObExecContext& ctx, ObTaskCompleteEvent*& task_event, int64_t timeout_timestamp);
  int signal_root_finish();
  int signal_job_iter_end(common::ObLightyQueue& finish_queue);
  int signal_schedule_error(int sche_ret);
  int signal_schedule_finish();
  int signal_can_serial_exec();
  int check_root_finish()
  {
    return root_finish_ ? common::OB_ERR_INTERRUPTED : common::OB_SUCCESS;
  }
  int check_schedule_error()
  {
    return sche_ret_;
  }
  int wait_root_use_up_data(ObExecContext& ctx);
  int wait_schedule_finish(/*int64_t timeout_timestamp*/);
  int wait_can_serial_exec(ObExecContext& ctx, int64_t timeout_timestamp);
  int wait_all_task(int64_t timeout, const bool is_build_index)
  {
    return trans_result_.wait_all_task(timeout, is_build_index);
  }
  int get_schedule_ret()
  {
    return sche_ret_;
  }
  void set_sche_thread_started(bool sche_thread_started)
  {
    sche_thread_started_ = sche_thread_started;
  }
  uint64_t get_scheduler_id() const
  {
    return scheduler_id_;
  }

  int atomic_push_err_rpc_addr(const common::ObAddr& addr);

private:
  static const int64_t OB_MAX_SKIPPED_TASK_EVENTS_QUEUE_CAPACITY = 1024L * 16L;
  typedef int (ObDistributedScheduler::*ObCheckStatus)();

  int merge_trans_result(const ObTaskCompleteEvent& task_event);
  int set_task_status(const ObTaskID& task_id, ObTaskStatus status);
  int signal_finish_queue(const ObTaskCompleteEvent& task_event);
  int pop_task_idx(const ObExecContext& ctx, common::ObLightyQueue& finish_queue, int64_t timeout_timestamp,
      ObCheckStatus check_func, int64_t& task_idx);
  int get_task_event(int64_t task_event_idx, ObTaskCompleteEvent*& task_event);
  inline void set_execution_id(uint64_t execution_id)
  {
    execution_id_ = execution_id;
  }
  inline void* idx_to_ptr(int64_t idx)
  {
    return reinterpret_cast<void*>(idx + 1);
  }
  inline int64_t ptr_to_idx(void* ptr)
  {
    return reinterpret_cast<int64_t>(ptr) - 1;
  }

  uint64_t next_scheduler_id();

private:
  static const int64_t MAX_FINISH_QUEUE_CAPACITY = 4096;
  static const int64_t NOP_EVENT = INT64_MIN + 1;
  static const int64_t SCHE_ITER_END = INT64_MIN + 2;

  common::ObArenaAllocator allocator_;
  uint64_t execution_id_;
  common::ObLightyQueue finish_queue_;
  common::ObSEArray<ObTaskCompleteEvent*, 64> response_task_events_;
  common::ObSpinLock lock_;

  ObTaskSpliterFactory spfactory_;
  ObDistributedJobControl job_control_;
  ObJobParser parser_;
  ObExecStatCollector* exec_stat_collector_;
  ObTransResultCollector trans_result_;
  volatile bool should_stop_;
  volatile bool root_finish_;
  volatile bool sche_finish_;
  volatile int sche_ret_;
  common::ObCond sche_finish_cond_;

  volatile bool can_serial_exec_;
  common::ObCond can_serial_exec_cond_;

  //===================main thread vars===================
  bool sche_thread_started_;

  // check scheduler id to discard message of other scheduler.
  // (index building may retry with same execution_id, may receive message of previous scheduler)
  uint64_t scheduler_id_;
  common::ObArray<ObAddr> rpc_error_addrs_;

  DISALLOW_COPY_AND_ASSIGN(ObDistributedScheduler);
}; /* end ObDistributedScheduler */

class ObSchedulerThreadPool : public lib::TGTaskHandler {
  virtual void handle(void* task);
};

class ObDistributedSchedulerManager {
public:
  // private static variable
  static const int64_t DEFAULT_ID_MAP_SIZE = (1 << 20);
  static const int64_t MINI_MODE_ID_MAP_SIZE = (128 << 10);
  static const int64_t SCHEDULER_THREAD_NUM = 128;
  static const int64_t MINI_MODE_SCHEDULER_THREAD_NUM = 4;
  static const int64_t SCHEDULER_THREAD_QUEUE = 256;

private:
  // private static variable
  static ObDistributedSchedulerManager* instance_;
  typedef ObSqlExecutionIDMap ExecutionIDMap;

public:
  class ObDistributedSchedulerHolder {
  public:
    ObDistributedSchedulerHolder();
    virtual ~ObDistributedSchedulerHolder();

    void reset();
    int init(ObDistributedScheduler* scheduler, uint64_t execution_id, ExecutionIDMap& execution_id_map);
    int get_scheduler(ObDistributedScheduler*& scheduler);

  private:
    bool inited_;
    uint64_t execution_id_;
    ObDistributedScheduler* scheduler_;
    ExecutionIDMap* execution_id_map_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObDistributedSchedulerHolder);
  };

  class ObDistributedSchedulerKiller {
  public:
    ObDistributedSchedulerKiller()
    {}
    virtual ~ObDistributedSchedulerKiller()
    {}

    void reset()
    {}
    void operator()(const uint64_t execution_id);

  private:
    DISALLOW_COPY_AND_ASSIGN(ObDistributedSchedulerKiller);
  };

public:
  friend class ObDistributedSchedulerKiller;

  static int build_instance();
  static ObDistributedSchedulerManager* get_instance();

  ObDistributedSchedulerManager();
  virtual ~ObDistributedSchedulerManager();

  void reset();
  int alloc_scheduler(ObExecContext& ctx, uint64_t& execution_id);
  int free_scheduler(uint64_t execution_id);
  int close_scheduler(ObExecContext& ctx, uint64_t execution_id);
  int get_scheduler(uint64_t execution_id, ObDistributedSchedulerHolder& scheduler_holder);
  int parse_jobs_and_start_sche_thread(
      uint64_t execution_id, ObExecContext& ctx, ObPhysicalPlan* phy_plan, int64_t timeout_timestamp);
  int do_schedule(ObDistributedSchedulerCtx& sched_ctx, ObDistributedExecContext& dis_exec_ctx);
  int signal_scheduler(ObTaskCompleteEvent& task_event, const uint64_t scheduler_id = 0);
  int signal_schedule_error(uint64_t execution_id, int sche_ret, const ObAddr addr, const uint64_t scheduler_id = 0);
  int collect_extent_info(ObTaskCompleteEvent& task_event);
  int merge_trans_result(const ObTaskCompleteEvent& task_event);
  int set_task_status(const ObTaskID& task_id, ObTaskStatus status);
  int stop();

private:
  // private function
  int init();

private:
  // private common variable
  bool inited_;
  ExecutionIDMap execution_id_map_;
  volatile bool is_stopping_;
  ObDistributedSchedulerKiller distributed_scheduler_killer_;
  ObSchedulerThreadPool scheduler_pool_;

  DISALLOW_COPY_AND_ASSIGN(ObDistributedSchedulerManager);
};

inline void ObDistributedScheduler::set_exec_stat_collector(ObExecStatCollector* collector)
{
  exec_stat_collector_ = collector;
}

inline ObExecStatCollector* ObDistributedScheduler::get_exec_stat_collector()
{
  return exec_stat_collector_;
}

inline int ObDistributedScheduler::init_trans_result(ObSQLSessionInfo& session, ObExecutorRpcImpl* exec_rpc)
{
  ObDistributedSchedulerManager* dist_task_mgr = ObDistributedSchedulerManager::get_instance();
  return trans_result_.init(session, exec_rpc, dist_task_mgr, NULL /*mini_task_mgr*/);
}

class ObDistributedSchedulerCtx {
public:
  const uint64_t* trace_id_;
  uint64_t execution_id_;
  ObExecContext* exec_ctx_;
  char* exec_ctx_buf_;
  int64_t buf_len_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDistributedSchedulerCtx);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_SCHEDULER_ */
//// end of header file
