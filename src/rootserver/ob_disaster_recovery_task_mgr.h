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

#ifndef OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_MGR_H_
#define OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_MGR_H_

#include "lib/lock/ob_thread_cond.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "ob_disaster_recovery_task.h"
#include "rootserver/ob_disaster_recovery_task_table_updater.h"

namespace oceanbase
{
namespace common
{
class ObServerConfig;
class ObMySQLProxy;
}
namespace obrpc
{
class ObSrvRpcProxy;
struct ObDRTaskReplyResult;
}
namespace rootserver
{
class ObDRTaskExecutor;
class ObDRTaskMgr;

class ObDRTaskQueue
{
public:
  typedef common::ObDList<ObDRTask> TaskList;
  typedef common::hash::ObHashMap<ObDRTaskKey,
                                  ObDRTask *,
                                  common::hash::NoPthreadDefendMode> TaskMap;
public:
  ObDRTaskQueue();
  virtual ~ObDRTaskQueue();
public:
  void reuse();
  void reset();
  // init a ObDRTaskQueue
  // @param [in] config, server config
  // @param [in] bucket_num, the size of task_map
  // @param [in] rpc_proxy, to send rpc
  int init(
      common::ObServerConfig &config,
      const int64_t bucket_num,
      obrpc::ObSrvRpcProxy *rpc_proxy,
      ObDRTaskPriority priority);

public:
  // check whether a task in task_map is executing
  // @param [in] task_key, task's tenant_id and ls_id
  // @param [out] task_in_scheduling, whether this task is in scheduling
  int check_task_in_scheduling(
      const ObDRTaskKey &task_key,
      bool &task_in_scheduling) const;

  // check whether a task exist in task_map
  // @param [in] task_key, task's tenant_id and ls_id
  // @param [out] task_exist, whether task exist in task_map
  int check_task_exist(
      const ObDRTaskKey &task_key,
      bool &task_exist);

  // push a task into this queue's wait_list
  // @param [in] task_mgr, to deal with concurrency_limit
  // @param [in] sibling_queue, to check whether another task is scheduling in sibling_queue
  // @param [in] task, the task to push in
  // @param [out] has_task_in_schedule, whether another task is scheduling in sibling_queue
  int push_task_in_wait_list(
      ObDRTaskMgr &task_mgr,
      const ObDRTaskQueue &sibling_queue,
      const ObDRTask &task,
      bool &has_task_in_schedule);

  // push a task into this queue's schedule_list
  // @param [in] task, the task to push in
  int push_task_in_schedule_list(
      ObDRTask &task);

  // pop a task and move it from wait_list to schedule_list
  // @param [out] task, the task to pop
  int pop_task(
      ObDRTask *&task);

  // get a certain task from task_map
  // @param [in] task_id, the only id to identify a task
  // @param [in] task_key, tenant_id and ls_id to locate task quickly in task_map
  // @param [out] the task getted
  int get_task(
      const share::ObTaskId &task_id,
      const ObDRTaskKey &task_key,
      ObDRTask *&task);

  // to deal with those not running tasks in schedule_list
  // @param [in] task_mgr, to execute over a task
  int handle_not_in_progress_task(
      ObDRTaskMgr &task_mgr);

  // remove task from schedule_list and clean it
  // @param [in] task, the task to finish schedule
  int finish_schedule(
      ObDRTask *task);

  // set task's sibling_in_schedule
  // @param [in] task, to locate which task to modify
  // @oaram [in] in_schedule, whether another task is scheduling in sibling queue
  int set_sibling_in_schedule(
      const ObDRTask &task,
      const bool in_schedule);

  // set task's sibling_in_schedule
  // @param [in] task_key, to locate which task to modify quickly
  // @oaram [in] in_schedule, whether another task is scheduling in sibling queue
  int set_sibling_in_schedule(
      const ObDRTaskKey &task_key,
      const bool in_schedule);

  int64_t wait_task_cnt() const { return wait_list_.get_size(); }
  int64_t in_schedule_task_cnt() const { return schedule_list_.get_size(); }
  int64_t task_cnt() const { return task_map_.size(); }
  int dump_statistic() const;

  const char* get_priority_str() const {
    const char *priority_str = "INVALID_PRIORITY";
    switch (priority_) {
      case ObDRTaskPriority::HIGH_PRI:
        priority_str = "HIGH_PRIORITY";
        break;
      case ObDRTaskPriority::LOW_PRI:
        priority_str = "LOW_PRIORITY";
        break;
      default:
        priority_str = "INVALID_PRIORITY";
        break;
    }
    return priority_str;
  };

  ObDRTaskPriority get_priority() { return priority_; }
  TaskList &get_wait_list() { return wait_list_; }
  TaskList &get_schedule_list() { return schedule_list_; }

private:
  // do push a task in wait list
  // @param [in] task_mgr, to deal with concurrency limit
  // @param [in] sibling_queue, another queue
  // @param [in] task, the task to push in
  // @param [out] has_task_in_schedule, whether task is schedule in sibling queue
  int do_push_task_in_wait_list_(
      ObDRTaskMgr &task_mgr,
      const ObDRTaskQueue &sibling_queue,
      const ObDRTask &task,
      bool &has_task_in_schedule);

  // check whether to clean this task
  // @param [in]  task, the task to check
  // @param [out] need_cleaning, whether to clean this task
  int check_task_need_cleaning_(
      const ObDRTask &task,
      bool &need_cleaning,
      ObDRTaskRetComment &ret_comment);

  // free a task
  // @param [in] task, task to free
  void free_task_(ObDRTask *&task);

  // remove task from task_map and free it
  // @param [in] task, task to clean
  void remove_task_from_map_and_free_it_(ObDRTask *&task);
private:
  bool inited_;
  common::ObServerConfig *config_;
  common::ObFIFOAllocator task_alloc_;
  TaskList wait_list_;
  TaskList schedule_list_;
  TaskMap task_map_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObDRTaskPriority priority_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDRTaskQueue);
};

class ObDRTaskMgr : public ObRsReentrantThread
{
public:
  const static int64_t TASK_QUEUE_LIMIT = 10000; //1w
  const static int64_t ONCE_ADD_TASK_CNT = TASK_QUEUE_LIMIT / 2;
  const static int64_t SAFE_DISCARD_TASK_INTERVAL = 200L * 1000L; // 200ms
  const static int64_t DATA_IN_CLEAR_INTERVAL = 20L * 60L * 1000000L; // 20min
  const static int64_t CHECK_IN_PROGRESS_INTERVAL_PER_TASK = 5L * 1000000L; // 5s
  const static int64_t CONCURRENCY_LIMIT_INTERVAL = 10L * 60L * 1000000L; // 10min
public:
  ObDRTaskMgr() : ObRsReentrantThread(true),
                  inited_(false),
                  stopped_(true),
                  loaded_(false),
                  config_(nullptr),
                  concurrency_limited_ts_(0),
                  cond_(),
                  queues_(),
                  high_task_queue_(queues_[0]),
                  low_task_queue_(queues_[1]),
                  self_(),
                  task_executor_(nullptr),
                  rpc_proxy_(nullptr),
                  sql_proxy_(nullptr),
                  schema_service_(nullptr) {}
  virtual ~ObDRTaskMgr() {}
public:

  // init a ObDRTaskMgr
  // @param [in] server, local server address
  // @param [in] config, local server config
  // @param [in] task_executor, to execute a task
  // @param [in] rpc_proxy, to send rpc for task queue
  // @param [in] sql_proxy, to send sql for updater
  // @param [in] schema_service, to get infos about objects
  int init(
      const common::ObAddr &server,
      common::ObServerConfig &config,
      ObDRTaskExecutor &task_executor,
      obrpc::ObSrvRpcProxy *rpc_proxy,
      common::ObMySQLProxy *sql_proxy,
      share::schema::ObMultiVersionSchemaService *schema_service);
  int is_inited() { return inited_; }
  int is_loaded() { return loaded_; }
  virtual void run3() override;
  virtual int blocking_run() { BLOCKING_RUN_IMPLEMENT(); }
  int start() override;
  void stop();
  void wait();
  void reuse();
public:
  // check whether a task is in schedule
  // @param [in] task_key, the task to check
  // @param [in] priority, which queue to check
  // @param [out] task_in_executing, whether task is scheduling
  virtual int check_task_in_executing(
      const ObDRTaskKey &task_key,
      const ObDRTaskPriority priority,
      bool &task_in_executing);

  // check whether a task exist
  // @param [in] task_key, the task to check
  // @param [in] priority, which queue to check
  // @param [out] task_exist, whether task is exist
  virtual int check_task_exist(
      const ObDRTaskKey &task_key,
      const ObDRTaskPriority priority,
      bool &task_exist);

  // add a task into queue
  // @param [in] task, the task to push in
  virtual int add_task(
      const ObDRTask &task);

  // to do something after receive task reply
  // param [in] reply, the execute result of this task
  virtual int deal_with_task_reply(
      const obrpc::ObDRTaskReplyResult &reply);

  // async update a cleaning task into updater
  // param [in] task_id, to identify a task
  // param [in] task_key, to locate a task quickly
  // param [in] ret_code, execute result of this task
  // param [in] need_clear_server_data_in_limit, whether clear data_in_limit
  int async_add_cleaning_task_to_updater(
      const share::ObTaskId &task_id,
      const ObDRTaskKey &task_key,
      const int ret_code,
      const bool need_record_event,
      const ObDRTaskRetComment &ret_comment,
      const bool need_clear_server_data_in_limit = true);

  // finish schedule this task and clean it
  // param [in] task_id, to identify a task
  // param [in] task_key, to locate a task quickly
  // param [in] ret_code, execute result of this task
  // param [in] need_clear_server_data_in_limit, whether clear data_in_limit
  int do_cleaning(
      const share::ObTaskId &task_id,
      const ObDRTaskKey &task_key,
      const int ret_code,
      const bool need_clear_server_data_in_limit,
      const bool need_record_event,
      const ObDRTaskRetComment &ret_comment);

  // operations of reach_concurrency_limit
  void set_reach_concurrency_limit() {
    concurrency_limited_ts_ = common::ObTimeUtility::current_time();
  }
  void clear_reach_concurrency_limit() {
    concurrency_limited_ts_ = 0;
  }
  int64_t get_reach_concurrency_limit() const {
    return concurrency_limited_ts_;
  }
  int64_t get_schedule_interval() const {
    return 1000L; // 1s
  }

  // get task count in different queue
  // @param [out] high_wait_cnt, wait task count in high priority queue
  // @param [out] high_schedule_cnt, schedule task count in high priority queue
  // @param [out] low_wait_cnt, wait task count in low priority queue
  // @param [out] low_schedule_cnt, schedule task count in low priority queue
  virtual int get_all_task_count(
      int64_t &high_wait_cnt,
      int64_t &high_schedule_cnt,
      int64_t &low_wait_cnt,
      int64_t &low_schedule_cnt);

  // log task result into rootservice event history table
  // @param [in] task, which task to log
  // @param [in] ret_code, the execute result of this task
  virtual int log_task_result(
      const ObDRTask &task,
      const int ret_code,
      const ObDRTaskRetComment &ret_comment);

private:
  ObDRTaskQueue &get_high_priority_queue_() { return high_task_queue_; }
  ObDRTaskQueue &get_low_priority_queue_() { return low_task_queue_; }
  int check_inner_stat_() const;

  // get a task by task id
  // @param [in] task_id, to identify a certain task
  // @param [in] task_key, to quickly locate a task
  // @param [out] task, the task to get
  // ATTENTION: need to lock task memory before use this function
  int get_task_by_id_(
      const share::ObTaskId &task_id,
      const ObDRTaskKey &task_key,
      ObDRTask *&task);

  // free a task
  // @param [in] allocator, allocator to use
  // @param [in] task, task to free
  void free_task_(
       common::ObIAllocator &allocator,
       ObDRTask *&task);

  // load tasks from inner table to schedule list
  int load_task_to_schedule_list_();
  int load_single_tenant_task_infos_(
      sqlclient::ObMySQLResult &res);
  int load_task_info_(
      sqlclient::ObMySQLResult &res);

  // write task info into inner table
  // @param [in] task, which task info to write
  int persist_task_info_(
      const ObDRTask &task,
      ObDRTaskRetComment &ret_comment);

  // try to log inmemory task infos according to balancer_log_interval
  // @param [in] last_dump_ts, last time do logging
  int try_dump_statistic_(
      int64_t &last_dump_ts) const;
  int inner_dump_statistic_() const;
  // try to deal with those tasks not in scheduling
  int try_clean_not_in_schedule_task_in_schedule_list_(
      int64_t &last_check_task_in_progress_ts);
  int inner_clean_not_in_schedule_task_in_schedule_list_();

  // get total wait and schedule task count in two queues
  // @param [out] wait_cnt, total wait task count in two queues
  // @param [out] in_schedule_cnt, total schedule task count in two queues
  int inner_get_task_cnt_(
      int64_t &wait_cnt,
      int64_t &in_schedule_cnt) const;

  // try to pop a task to execute
  // param [in] allocator, room to build a task
  // param [out] task, pop task
  int try_pop_task(
      common::ObIAllocator &allocator,
      ObDRTask *&task);

  // pop a task from queue and set sibling in schedule
  // @param [in] task, which task to deal with
  int pop_task(
      ObDRTask *&task);

  // try to persist and execute a task
  // @param [in] task, the task to execute
  int execute_task(
      const ObDRTask &task);

  // set sibling in schedule
  // @param [in] task, which task to deal with
  // @param [in] in_schedule, whether in schedule
  int set_sibling_in_schedule(
      const ObDRTask &task,
      const bool in_schedule);

private:
  bool inited_;
  bool stopped_;
  bool loaded_;
  common::ObServerConfig *config_;
  /* has waiting task but cannot be scheduled,
   * since mgr reaches server_data_copy_[in/out]_concurrency
   */
  volatile int64_t concurrency_limited_ts_;
  mutable common::ObThreadCond cond_;
  ObDRTaskQueue queues_[static_cast<int64_t>(ObDRTaskPriority::MAX_PRI)];
  ObDRTaskQueue &high_task_queue_; // queues_[0]
  ObDRTaskQueue &low_task_queue_;  // queues_[1]
  common::ObAddr self_;
  ObDRTaskExecutor *task_executor_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObDRTaskTableUpdater disaster_recovery_task_table_updater_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDRTaskMgr);
};
} // end namespace rootserver
} // end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_MGR_H_
