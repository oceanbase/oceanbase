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

#ifndef SRC_SHARE_SCHEDULER_OB_DAG_SCHEDULER_H_
#define SRC_SHARE_SCHEDULER_OB_DAG_SCHEDULER_H_

#include "lib/ob_define.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/lock/ob_mutex.h"
#include "lib/net/ob_addr.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/thread/thread_pool.h"

namespace oceanbase {

namespace storage {
class ObDagWarningInfo;
}
namespace share {

typedef common::ObCurTraceId::TraceId ObDagId;
class ObIDag;
class ObDagScheduler;
class ObDagWorker;

class ObITask : public common::ObDLinkBase<ObITask> {
  static const int64_t DEFAULT_CHILDREN_NUM = 8;
  friend class ObIDag;
  friend class ObDagScheduler;

public:
  enum ObITaskType {
    TASK_TYPE_UT = 0,
    TASK_TYPE_MACROMERGE = 1,
    TASK_TYPE_INDEX_FINISH = 2,
    TASK_TYPE_MAIN_FINISH = 3,
    TASK_TYPE_MINOR_MERGE = 4,
    TASK_TYPE_INDEX_PERPARE = 5,
    TASK_TYPE_INDEX_LOCAL_SORT = 6,
    TASK_TYPE_INDEX_MERGE = 7,
    TASK_TYPE_NORMAL_MINOR_MERGE = 8,
    TASK_TYPE_BUILD_INDEX_NORMAL_MERGE = 9,
    TASK_TYPE_UNIQUE_INDEX_CHECKING = 10,
    TASK_TYPE_REPORT_INDEX_STATUS = 11,
    TASK_TYPE_MERGE_PREPARE_TASK = 12,
    TASK_TYPE_INDEX_MERGE_TO_LATEST_FINISH = 13,
    TASK_TYPE_COMPACT_TO_LASTEST = 14,
    TASK_TYPE_SSTABLE_MERGE_PREPARE = 15,
    TASK_TYPE_SSTABLE_MERGE_FINISH = 16,
    TASK_TYPE_SPLIT_PREPARE_TASK = 17,
    TASK_TYPE_SPLIT_TASK = 18,
    TASK_TYPE_SPLIT_FINISH_TASK = 19,
    TASK_TYPE_UNIQUE_CHECKING_PREPARE = 20,
    TASK_TYPE_SIMPLE_UNIQUE_CHECKING = 21,
    TASK_TYPE_MIGRATE_PREPARE = 22,
    TASK_TYPE_MIGRATE_COPY_LOGIC = 23,
    TASK_TYPE_MIGRATE_FINISH_LOGIC = 24,
    TASK_TYPE_MIGRATE_COPY_PHYSICAL = 25,
    TASK_TYPE_MIGRATE_FINISH_PHYSICAL = 26,
    TASK_TYPE_MIGRATE_FINISH = 27,
    TASK_TYPE_FAKE = 28,
    TASK_TYPE_MIGRATE_ENABLE_REPLAY = 29,
    TASK_TYPE_MAJOR_MERGE_FINISH = 30,
    TASK_TYPE_GROUP_MIGRATE = 31,
    TASK_TYPE_SQL_BUILD_INDEX = 32,  // build index by sql plan.
    TASK_TYPE_SERVER_PREPROCESS = 33,
    TASK_TYPE_FAST_RECOVERY = 34,
    TASK_TYPE_MIGRATE_POST_PREPARE = 35,
    TASK_TYPE_FAST_MIGRATE_ASYNC_TASK = 36,
    TASK_TYPE_VALIDATE_BACKUP = 37,
    TASK_TYPE_VALIDATE_FINISH = 38,
    TASK_TYPE_BUILD_CHANGE_REPLICA = 39,
    TASK_TYPE_RESTORE_TAILORED_PREPARE = 40,
    TASK_TYPE_RESTORE_TAILORED_PROCESS = 41,
    TASK_TYPE_RESTORE_TAILORED_FINISH = 42,
    TASK_TYPE_MAX,
  };

  enum ObITaskStatus {
    TASK_STATUS_INITING = 0,
    TASK_STATUS_WAITING = 1,
    TASK_STATUS_RUNNING = 2,
    TASK_STATUS_FINISHED = 3,
  };

  enum ObITaskColor {
    WHITE,  // not visited
    GRAY,   // visiting, on the stack
    BLACK,  // visited, all paths have walked
  };
  explicit ObITask(const ObITaskType type);
  virtual ~ObITask();
  int do_work();
  bool is_valid() const;
  int add_child(ObITask& child);
  ObIDag* get_dag() const
  {
    return dag_;
  }
  ObITaskStatus get_status() const
  {
    return status_;
  }
  ObITaskType get_type() const
  {
    return type_;
  }
  const common::ObIArray<ObITask*>& get_child_tasks() const
  {
    return children_;
  }
  VIRTUAL_TO_STRING_KV(K_(type), K_(status), K(dag_));

private:
  virtual int generate_next_task(ObITask*& next_task)
  {
    UNUSED(next_task);
    return common::OB_ITER_END;
  }
  virtual int process() = 0;

private:
  void reset();
  int copy_dep_to(ObITask& other_task) const;
  void set_status(const ObITaskStatus status)
  {
    status_ = status;
  }
  void set_dag(ObIDag& dag)
  {
    dag_ = &dag;
  }
  int64_t get_indegree() const
  {
    return indegree_;
  }
  void inc_indegree()
  {
    ATOMIC_INC(&indegree_);
  }
  int64_t dec_indegree()
  {
    return ATOMIC_SAF(&indegree_, 1);
  }
  void prepare_check_cycle();
  ObITaskColor get_color()
  {
    return color_;
  }
  void set_color(const ObITaskColor color)
  {
    color_ = color;
  }
  int64_t get_last_visit_child()
  {
    return last_visit_child_;
  }
  void set_last_visit_child(const int64_t idx)
  {
    last_visit_child_ = idx;
  }
  int generate_next_task();

protected:
  ObIDag* dag_;

private:
  ObITaskType type_;
  ObITaskStatus status_;
  int64_t indegree_;
  int64_t last_visit_child_;
  ObITaskColor color_;
  common::ObSEArray<ObITask*, DEFAULT_CHILDREN_NUM> children_;
};

class ObFakeTask : public ObITask {
public:
  ObFakeTask() : ObITask(TASK_TYPE_FAKE)
  {}
  virtual ~ObFakeTask()
  {}
  virtual int process() override;
};

class ObIDag : public common::ObDLinkBase<ObIDag> {
public:
  // priority from high to low, high priority tasks can preempt low priority tasks util their lower bound
  enum ObIDagPriority {
    DAG_PRIO_TRANS_TABLE_MERGE = 0,
    DAG_PRIO_SSTABLE_MINI_MERGE,
    DAG_PRIO_SSTABLE_MINOR_MERGE,
    DAG_PRIO_GROUP_MIGRATE,
    DAG_PRIO_MIGRATE_HIGH,
    DAG_PRIO_MIGRATE_MID,
    DAG_PRIO_SSTABLE_MAJOR_MERGE,
    DAG_PRIO_BACKUP,
    DAG_PRIO_MIGRATE_LOW,
    DAG_PRIO_CREATE_INDEX,
    DAG_PRIO_SSTABLE_SPLIT,
    DAG_PRIO_VALIDATE,
    DAG_PRIO_MAX,
  };
  const static common::ObString ObIDagPriorityStr[DAG_PRIO_MAX]; /* = {
     "DAG_PRIO_TRANS_TABLE_MERGE",
     "DAG_PRIO_SSTABLE_MINI_MERGE",
     "DAG_PRIO_SSTABLE_MINOR_MERGE",
     "DAG_PRIO_GROUP_MIGRATE",
     "DAG_PRIO_MIGRATE_HIGH",
     "DAG_PRIO_MIGRATE_MID",
     "DAG_PRIO_SSTABLE_MAJOR_MERGE",
     "DAG_PRIO_BACKUP",
     "DAG_PRIO_MIGRATE_LOW",
     "DAG_PRIO_CREATE_INDEX",
     "DAG_PRIO_SSTABLE_SPLIT",
     "DAG_PRIO_VALIDATE"
   };*/

  // We limit the max concurrency of tasks by UpLimitType (ult for short)
  // why not simply use Priority? since several priorities may share one UpLimitType
  // e.g. the total running task number of migrate tasks should be limited, but we want
  // each priority can use up to up limit when no other priorities have tasks. If we
  // limit max concurrency by priority, we may use as many as three times threads than
  // we expected.
  enum ObIDagUpLimitType {
    DAG_ULT_MINI_MERGE = 0,
    DAG_ULT_MINOR_MERGE = 1,
    DAG_ULT_GROUP_MIGRATE = 2,
    DAG_ULT_MIGRATE = 3,
    DAG_ULT_MAJOR_MERGE = 4,
    DAG_ULT_CREATE_INDEX = 5,
    DAG_ULT_SPLIT = 6,
    DAG_ULT_BACKUP = 7,
    DAG_ULT_MAX,
  };
  const static common::ObString ObIDagUpLimitTypeStr[DAG_ULT_MAX]; /* = {
     "DAG_ULT_MINI_MERGE",
     "DAG_ULT_MINOR_MERGE",
     "DAG_ULT_GROUP_MIGRATE",
     "DAG_ULT_MIGRATE",
     "DAG_ULT_MAJOR_MERGE",
     "DAG_ULT_CREATE_INDEX",
     "DAG_ULT_SPLIT",
     "DAG_ULT_BACKUP"
   };*/
  enum ObIDagType {
    DAG_TYPE_UT = 0,
    DAG_TYPE_SSTABLE_MINOR_MERGE = 1,
    DAG_TYPE_SSTABLE_MAJOR_MERGE = 2,
    DAG_TYPE_CREATE_INDEX = 3,
    DAG_TYPE_SSTABLE_SPLIT = 4,
    DAG_TYPE_UNIQUE_CHECKING = 5,
    DAG_TYPE_MIGRATE = 6,
    DAG_TYPE_MAJOR_MERGE_FINISH = 7,
    DAG_TYPE_GROUP_MIGRATE = 8,
    DAG_TYPE_SQL_BUILD_INDEX = 9,  // build index by sql plan.
    DAG_TYPE_SSTABLE_MINI_MERGE = 10,
    DAG_TYPE_TRANS_TABLE_MERGE = 11,
    DAG_TYPE_FAST_RECOVERY_SPLIT = 12,
    DAG_TYPE_FAST_RECOVERY_RECOVER = 13,
    DAG_TYPE_BACKUP = 14,
    DAG_TYPE_SERVER_PREPROCESS = 15,
    DAG_TYPE_FAST_RECOVERY = 16,
    DAG_TYPE_VALIDATE = 17,
    DAG_TYPE_MAX,
  };

  const static char* ObIDagTypeStr[DAG_TYPE_MAX];

  enum ObDagStatus {
    DAG_STATUS_INITING = 0,
    DAG_STATUS_READY = 1,         // the dag is ready
    DAG_STATUS_NODE_RUNNING = 2,  // the dag is running,the ready dag and the running dag are both in ready_list
    DAG_STATUS_FINISH = 3,
    DAG_STATUS_NODE_FAILED = 4,
    DAG_STATUS_ABORT = 5,  // not using status 5, 6, 7
    DAG_STATUS_RM = 6,
    DAG_STATUS_HALTED = 7,
  };

  explicit ObIDag(const ObIDagType type, const ObIDagPriority priority);
  virtual ~ObIDag();
  int add_task(ObITask& task);
  template <typename T>
  int alloc_task(T*& task);
  int get_dag_ret() const
  {
    return dag_ret_;
  };
  ObDagStatus get_dag_status() const
  {
    return dag_status_;
  }
  int64_t get_priority() const
  {
    return priority_;
  }
  const ObDagId& get_dag_id() const
  {
    return id_;
  }
  int set_dag_id(const ObDagId& dag_id);
  ObIDagType get_type() const
  {
    return type_;
  }
  const char* get_name() const
  {
    return ObIDagTypeStr[type_];
  }
  bool has_set_stop()
  {
    return is_stop_;
  }
  void set_stop()
  {
    is_stop_ = true;
  }
  void set_priority(ObIDag::ObIDagPriority prio)
  {
    priority_ = prio;
  }
  int64_t get_running_task_count() const
  {
    return running_task_cnt_;
  }
  virtual void gene_basic_warning_info(storage::ObDagWarningInfo& info);
  virtual void gene_warning_info(storage::ObDagWarningInfo& info);
  virtual bool ignore_warning()
  {
    return false;
  }
  DECLARE_VIRTUAL_TO_STRING;
  DISABLE_COPY_ASSIGN(ObIDag);

public:
  virtual bool operator==(const ObIDag& other) const = 0;
  virtual int64_t hash() const = 0;
  virtual int64_t get_tenant_id() const = 0;
  virtual int fill_comment(char* buf, const int64_t buf_len) const = 0;
  virtual int64_t get_compat_mode() const = 0;

protected:
  int dag_ret_;

private:
  typedef common::ObDList<ObITask> TaskList;
  typedef lib::ObLockGuard<ObIDag> ObDagGuard;
  static const int64_t DEFAULT_TASK_NUM = 32;
  static const int64_t TOTAL_LIMIT = 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  friend class ObDagScheduler;
  friend class ObITask;
  friend class ObDagWorker;
  friend ObDagGuard;

private:
  void free_task(ObITask& task);
  int finish_task(ObITask& task, int64_t& available_cnt);
  bool is_valid();
  bool is_valid_type() const;
  int init(const int64_t total = TOTAL_LIMIT, const int64_t hold = HOLD_LIMIT, const int64_t page_size = PAGE_SIZE);
  void restart_task(ObITask& task);
  void reset();
  int check_cycle();
  int get_next_ready_task(ObITask*& task);
  bool has_finished() const;
  void set_dag_ret(const int ret)
  {
    ATOMIC_VCAS(&dag_ret_, common::OB_SUCCESS, ret);
  }
  void set_dag_status(const ObDagStatus status)
  {
    dag_status_ = status;
  }
  int lock()
  {
    return lock_.lock();
  }
  int unlock()
  {
    return lock_.unlock();
  }
  void inc_running_task_cnt()
  {
    ++running_task_cnt_;
  }
  void dec_running_task_cnt()
  {
    --running_task_cnt_;
  }

private:
  common::ObConcurrentFIFOAllocator allocator_;
  bool is_inited_;
  ObIDagPriority priority_;
  ObIDagType type_;
  ObDagId id_;
  ObDagStatus dag_status_;
  int64_t running_task_cnt_;
  int64_t start_time_;
  lib::ObMutex lock_;
  TaskList task_list_;
  bool is_stop_;
};

template <typename T, int PRIOS>
class ObPriorityList {
public:
  enum { PRIO_CNT = PRIOS };

  bool add_first(T* item, const int64_t prio)
  {
    bool bret = false;
    if (prio >= 0 && prio < PRIO_CNT) {
      bret = dlists_[prio].add_first(item);
    } else {
      COMMON_LOG(ERROR, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return bret;
  }

  bool add_last(T* item, const int64_t prio)
  {
    bool bret = false;
    if (prio >= 0 && prio < PRIO_CNT) {
      bret = dlists_[prio].add_last(item);
    } else {
      COMMON_LOG(ERROR, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return bret;
  }

  bool remove(T* item, const int64_t prio)
  {
    bool bret = false;
    if (prio >= 0 && prio < PRIO_CNT) {
      if (NULL != dlists_[prio].remove(item)) {
        bret = true;
      }
    } else {
      COMMON_LOG(ERROR, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return bret;
  }

  T* get_head(const int64_t prio)
  {
    T* result = NULL;
    if (prio >= 0 && prio < PRIO_CNT) {
      result = dlists_[prio].get_header();
    } else {
      COMMON_LOG(ERROR, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return result;
  }

  T* get_first(const int64_t prio)
  {
    T* result = NULL;
    if (prio >= 0 && prio < PRIO_CNT) {
      result = dlists_[prio].get_first();
    } else {
      COMMON_LOG(ERROR, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return result;
  }

  T* remove_first(const int64_t prio)
  {
    T* result = NULL;
    if (prio >= 0 && prio < PRIO_CNT) {
      result = dlists_[prio].remove_first();
    } else {
      COMMON_LOG(ERROR, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return result;
  }

  T* remove_last(const int64_t prio)
  {
    T* result = NULL;
    if (prio >= 0 && prio < PRIO_CNT) {
      result = dlists_[prio].remove_last();
    } else {
      COMMON_LOG(ERROR, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return result;
  }

  bool is_empty(const int64_t prio) const
  {
    bool bret = false;
    if (prio >= 0 && prio < PRIO_CNT) {
      bret = dlists_[prio].is_empty();
    } else {
      COMMON_LOG(ERROR, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return bret;
  }

  bool is_empty() const
  {
    bool bret = true;
    for (int64_t i = 0; i < PRIO_CNT; ++i) {
      bret &= dlists_[i].is_empty();
    }
    return bret;
  }

  void reset()
  {
    for (int64_t i = 0; i < PRIO_CNT; ++i) {
      dlists_[i].reset();
    }
  }

  common::ObDList<T>& get_list(const int64_t prio)
  {
    return dlists_[prio];
  }

  int64_t size(const int64_t prio)
  {
    int64_t ret = 0;
    if (prio >= 0 && prio < PRIO_CNT) {
      ret = dlists_[prio].get_size();
    } else {
      COMMON_LOG(ERROR, "invalid priority", K(prio), K(PRIO_CNT));
    }
    return ret;
  }

private:
  common::ObDList<T> dlists_[PRIOS];
};

class ObDagWorker : public lib::ThreadPool, public common::ObDLinkBase<ObDagWorker> {
public:
  typedef common::ObDLinkNode<ObDagWorker*> Node;
  typedef common::ObDList<Node> WorkerNodeList;
  enum DagWorkerStatus {
    DWS_FREE,
    DWS_RUNNABLE,
    DWS_WAITING,
    DWS_RUNNING,
    DWS_STOP,
  };

public:
  ObDagWorker();
  ~ObDagWorker();
  int init(const int64_t check_period);
  void destroy();
  void stop_worker();
  void resume();
  void run1() override;
  void yield();
  void set_task(ObITask* task)
  {
    task_ = task;
  }
  bool need_wake_up() const;
  ObITask* get_task() const
  {
    return task_;
  }
  static ObDagWorker* self()
  {
    return self_;
  }

private:
  void notify(DagWorkerStatus status);

private:
  static __thread ObDagWorker* self_;
  static const uint32_t SLEEP_TIME_MS = 100;  // 100ms
private:
  common::ObThreadCond cond_;
  ObITask* task_;
  DagWorkerStatus status_;
  int64_t check_period_;
  int64_t last_check_time_;
  bool is_inited_;
};

class ObLoadShedder {
public:
  enum ObLoadType {
    LOAD_PAST_ONE = 0,
    LOAD_PAST_FIVE = 1,
    LOAD_PAST_FIFTEEN = 2,
    LOAD_TYPE_MAX = 3,
  };
  ObLoadShedder();
  ~ObLoadShedder() = default;
  void refresh_stat();
  void reset();
  OB_INLINE int64_t get_shedding_factor() const
  {
    return load_shedding_factor_;
  }
  TO_STRING_KV(K_(cpu_cnt_online), K_(cpu_cnt_configure), K_(load_per_cpu_threshold), K_(load_shedding_factor),
      "load_past_one", load_avg_[LOAD_PAST_ONE], "load_past_five", load_avg_[LOAD_PAST_FIVE], "load_past_fifteen",
      load_avg_[LOAD_PAST_FIFTEEN]);

private:
  static const int64_t DEFAULT_LOAD_PER_CPU_THRESHOLD = 500;
  static const int64_t DEFAULT_LOAD_SHEDDING_FACTOR = 2;
  static constexpr int64_t LOAD_SHEDDING_FACTOR[LOAD_TYPE_MAX] = {
      4,  // load past one minute
      3,  // load past five minute
      2,  // load past fifteen minute
  };
  void refresh_load_shedding_factor();

private:
  int64_t cpu_cnt_online_;
  int64_t cpu_cnt_configure_;
  double load_avg_[LOAD_TYPE_MAX];
  int64_t load_per_cpu_threshold_;
  int64_t load_shedding_factor_;
};

class ObDagScheduler : public lib::ThreadPool {
  friend class ObDagWorker;

public:
  static ObDagScheduler& get_instance();
  int init(const common::ObAddr& addr, const int64_t check_period = DEFAULT_CHECK_PERIOD,
      const int32_t work_thread_num = 0, const int64_t dag_limit = DEFAULT_MAX_DAG_NUM,
      const int64_t total_mem_limit = TOTAL_LIMIT, const int64_t hold_mem_limit = HOLD_LIMIT,
      const int64_t page_size = PAGE_SIZE);
  int add_dag(ObIDag* dag, const bool emergency = false);
  template <typename T>
  int alloc_dag(T*& dag);
  void free_dag(ObIDag& dag);
  void run1() final;
  void notify();
  void destroy();
  int set_mini_merge_concurrency(const int32_t mini_merge_concurrency);
  int set_minor_merge_concurrency(const int32_t minor_merge_concurrency);
  int set_major_merge_concurrency(const int32_t major_merge_concurrency);
  int set_create_index_concurrency(const int32_t create_index_concurrency);
  int set_migrate_concurrency(const int32_t migrate_concurrency);
  int set_group_migrate_concurrency(const int32_t migrate_concurrency);
  int set_backup_concurrency(const int32_t backup_concurrency);
  int32_t get_max_thread_cnt() const;
  int32_t get_default_work_thread_cnt(const int32_t proposed_thread_num = 0) const;
  int32_t get_work_thread_num() const
  {
    return work_thread_num_;
  }
  int32_t get_thread2reserve() const
  {
    return thread2reserve_;
  }
  bool is_empty() const
  {
    return ready_dag_list_.is_empty();
  }
  int64_t get_cur_dag_cnt() const
  {
    return dag_cnt_;
  }
  int sys_task_start(ObIDag* dag);
  int64_t get_dag_count(const ObIDag::ObIDagType type) const;
  int32_t get_running_task_cnt(const ObIDag::ObIDagPriority priority);
  int32_t get_up_limit(const int64_t up_limit_type, int32_t& up_limit);
  int check_dag_exist(const ObIDag* dag, bool& exist);
  int cancel_dag(const ObIDag* dag);

private:
  struct ThreadLimit {
    int32_t min_thread_;
    int32_t max_thread_;
    ThreadLimit() : min_thread_(0), max_thread_(0)
    {}
    TO_STRING_KV(K_(min_thread), K_(max_thread));
  };
  typedef common::ObDList<ObIDag> DagList;
  typedef ObPriorityList<ObIDag, ObIDag::DAG_PRIO_MAX> PriorityDagList;
  typedef ObPriorityList<ObDagWorker, ObIDag::DAG_PRIO_MAX> PriorityWorkerList;
  typedef common::ObDList<ObDagWorker> WorkerList;
  typedef common::hash::ObHashMap<const ObIDag*, ObIDag*, common::hash::NoPthreadDefendMode,
      common::hash::hash_func<const ObIDag*>, common::hash::equal_to<const ObIDag*> >
      DagMap;
  static const uint32_t DEFAULT_TIME_SLICE_US = 500 * 1000;
  static const int64_t SCHEDULER_WAIT_TIME_MS = 1000;    // 1s
  static const int64_t WORK_THREAD_WAIT_TIME_MS = 1000;  // 1s
  static const int64_t TOTAL_LIMIT = 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t DAG_SIZE_LIMIT = 10 << 12;
  static const int64_t DEFAULT_MAX_DAG_NUM = 15000;
  static const int32_t DEFAULT_WORK_THREAD_NUM = 10;
  static const int32_t DEFAULT_MAX_CONCURRENCY = 10;
  static const int32_t DEFAULT_MINOR_MERGE_CONCURRENCY = 10;
  static const int32_t WORK_THREAD_CNT_PERCENT = 30;
  static const int32_t SPARE_THREAD_PERCENT = 10;
  static const int64_t DUMP_DAG_STATUS_INTERVAL = 10 * 1000LL * 1000LL;
  static const int64_t DEFAULT_CHECK_PERIOD = 3 * 1000 * 1000;  // 3s
  static const int32_t MAX_THREAD_LIMIT = 64;
  static const int32_t MAX_MIGRATE_THREAD_NUM = 64;
  static const int32_t MAX_GROUP_MIGRATE_THREAD_NUM = 20;
  static const int32_t MAX_VALIDATE_THREAD_NUM = 20;
  static constexpr int32_t DEFAULT_LOW_LIMIT[ObIDag::DAG_PRIO_MAX] = {1, /*trans table*/
      2,                                                                 /*Mini merge*/
      3,                                                                 /*Minor merge*/
      2,                                                                 /*Group migrate*/
      2,                                                                 /*Migrate Replica High Prio*/
      2,                                                                 /*Migrate Replica Mid Prio*/
      1,                                                                 /*Major merge*/
      5,                                                                 /*Backup task*/
      1,                                                                 /*Migrate Replica Low Prio*/
      2,                                                                 /*Create index*/
      1,                                                                 /*Split task*/
      5,
      /*Validate task*/};
  static constexpr int32_t DEFAULT_UP_LIMIT[ObIDag::DAG_ULT_MAX] = {6, /*Mini merge and trans table*/
      10,                                                              /*Minor merge*/
      2,                                                               /*Group migrate*/
      10,                                                              /*Migrate*/
      10,                                                              /*Major merge*/
      10,                                                              /*Create index*/
      10,                                                              /*Split*/
      10,
      /*Backup*/};
  // map from priority to UpLimitType
  static constexpr ObIDag::ObIDagUpLimitType UP_LIMIT_MAP[ObIDag::DAG_PRIO_MAX] = {
      ObIDag::DAG_ULT_MINI_MERGE, /*trans table*/
      ObIDag::DAG_ULT_MINI_MERGE, /*mini merge*/
      ObIDag::DAG_ULT_MINOR_MERGE,
      ObIDag::DAG_ULT_GROUP_MIGRATE,
      ObIDag::DAG_ULT_MIGRATE,
      ObIDag::DAG_ULT_MIGRATE,
      ObIDag::DAG_ULT_MAJOR_MERGE,
      ObIDag::DAG_ULT_BACKUP,
      ObIDag::DAG_ULT_MIGRATE,
      ObIDag::DAG_ULT_CREATE_INDEX,
      ObIDag::DAG_ULT_SPLIT,
      ObIDag::DAG_ULT_BACKUP};

private:
  ObDagScheduler();
  virtual ~ObDagScheduler();
  int schedule();
  int pop_task(const int64_t priority, ObITask*& task);
  int finish_task(ObITask& task, ObDagWorker& worker);
  bool has_remain_task(const int64_t priority);
  int create_worker();
  int try_reclaim_threads();
  int schedule_one(const int64_t priority);
  int dispatch_task(ObITask& task, ObDagWorker*& ret_worker);
  void destroy_all_workers();
  int set_min_thread(const int64_t priority, const int32_t concurrency);
  int set_max_thread(const int64_t priority, const int32_t concurrency);
  bool try_switch(ObDagWorker& worker);
  int try_switch(ObDagWorker& worker, const int64_t src_prio, const int64_t dest_prio, bool& need_pause);
  void pause_worker(ObDagWorker& worker, const int64_t priority);
  void dump_dag_status();
  int check_need_load_shedding(const int64_t priority, const bool for_schedule, bool& need_shedding);

private:
  bool is_inited_;
  common::ObAddr addr_;
  DagMap dag_map_;
  PriorityDagList ready_dag_list_;
  common::ObThreadCond scheduler_sync_;
  int64_t dag_cnt_;
  int64_t dag_limit_;
  int64_t check_period_;
  int32_t total_worker_cnt_;
  int32_t work_thread_num_;
  int32_t default_thread_num_;
  int32_t thread2reserve_;
  int32_t total_running_task_cnt_;
  int32_t running_task_cnts_[ObIDag::DAG_PRIO_MAX];
  int32_t running_task_cnts_per_ult_[ObIDag::DAG_ULT_MAX];  // count running task number for each UpLimitType
  int32_t low_limits_[ObIDag::DAG_PRIO_MAX];
  int32_t up_limits_[ObIDag::DAG_ULT_MAX];
  int64_t dag_cnts_[ObIDag::DAG_TYPE_MAX];
  common::ObConcurrentFIFOAllocator allocator_;
  PriorityWorkerList waiting_workers_;  // workers waiting for time slice to run
  PriorityWorkerList running_workers_;  // running workers
  WorkerList free_workers_;             // free workers who have not been assigned to any task
  ObLoadShedder load_shedder_;
};

template <typename T>
int ObIDag::alloc_task(T*& task)
{
  int ret = common::OB_SUCCESS;
  void* buf = NULL;
  task = NULL;

  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    COMMON_LOG(WARN, "dag is not inited", K(ret));
  } else if (NULL == (buf = allocator_.alloc(sizeof(T)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    task = NULL;
    COMMON_LOG(WARN, "failed to alloc task", K(ret));
  } else {
    T* ntask = new (buf) T();
    ntask->set_dag(*this);
    {
      lib::ObMutexGuard guard(lock_);
      if (!task_list_.add_last(ntask)) {
        ret = common::OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "Failed to add task", K(task), K_(id));
      }
    }
    if (OB_SUCC(ret)) {
      task = ntask;
    }
  }
  return ret;
}

template <typename T>
int ObDagScheduler::alloc_dag(T*& dag)
{
  int ret = common::OB_SUCCESS;
  void* buf = NULL;
  dag = NULL;

  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    COMMON_LOG(WARN, "scheduler is not init", K(ret));
  } else if (DAG_SIZE_LIMIT < sizeof(T)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "Dag Object is too large", K(ret), K(sizeof(T)));
  } else {
    if (NULL == (buf = allocator_.alloc(sizeof(T)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc dag", K(ret));
    } else {
      ObIDag* new_dag = new (buf) T();
      if (OB_FAIL(new_dag->init())) {
        COMMON_LOG(WARN, "failed to init allocator", K(ret));
      } else {
        dag = static_cast<T*>(new_dag);
      }
    }
  }
  return ret;
}

inline void dag_yield()
{
  ObDagWorker* worker = ObDagWorker::self();
  if (NULL != worker) {
    worker->yield();
  }
}

}  // namespace share
}  // namespace oceanbase

#endif /* SRC_SHARE_SCHEDULER_OB_DAG_SCHEDULER_H_ */
