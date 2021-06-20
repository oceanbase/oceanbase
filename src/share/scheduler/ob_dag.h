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

#ifndef SRC_SHARE_SCHEDULER_OB_DAG_H_
#define SRC_SHARE_SCHEDULER_OB_DAG_H_

#include "lib/list/ob_dlink_node.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/lock/ob_mutex.h"
#include "lib/container/ob_se_array.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "share/scheduler/ob_dag_worker.h"

namespace oceanbase {
namespace share {
typedef common::ObCurTraceId::TraceId ObDagId;
class ObIDagNew;
class ObTenantThreadPool;
class ObDagWorkerNew;

class ObITaskNew : public common::ObDLinkBase<ObITaskNew> {
public:
  enum ObITaskNewStatus {
    TASK_STATUS_INITING = 0,
    TASK_STATUS_WAITING = 1,
    TASK_STATUS_RUNNABLE = 2,
    TASK_STATUS_WAIT_TO_SWITCH = 3,
    TASK_STATUS_RUNNING = 4,
    TASK_STATUS_FINISHED = 5,
    TASK_STATUS_MAX,
  };

  enum ObITaskNewColor {
    WHITE,  // not visited
    GRAY,   // visiting, on the stack
    BLACK,  // visited, all paths have walked
  };
  enum SwitchTaskFlag {
    THIS_TASK_FINISHED = 0,
    THIS_TASK_PAUSE = 1,
  };

public:
  explicit ObITaskNew(const int64_t type_id);
  virtual ~ObITaskNew();
  int do_work();
  int add_child(ObITaskNew& child);
  ObIDagNew* get_dag() const
  {
    return dag_;
  }
  ObITaskNewStatus get_status() const
  {
    return status_;
  }
  int64_t get_type_id() const
  {
    return type_id_;
  }
  void set_type_id(const int64_t type_id)
  {
    type_id_ = type_id;
  }
  void yield();
  bool operator>=(const ObITaskNew& other) const
  {
    return type_id_ >= other.get_type_id();
  }
  bool operator>(const ObITaskNew& other) const
  {
    return type_id_ > other.get_type_id();
  }
  bool operator<=(const ObITaskNew& other) const
  {
    return type_id_ <= other.get_type_id();
  }
  bool is_equal(const ObITaskNew& other) const
  {
    return this == &other;
  }
  static bool compare_task(const ObITaskNew* lhs, const ObITaskNew* rhs)
  {
    return lhs->get_type_id() < rhs->get_type_id();
  }
  VIRTUAL_TO_STRING_KV(KP(this), K_(type_id), K_(status), K_(dag), KP_(worker), K_(next_task));

private:
  friend class ObIDagNew;
  friend class ObTenantThreadPool;
  static const int64_t DEFAULT_CHILDREN_NUM = 8;
  static const int64_t SWITCH_TIME_UPLIMIT = 50;  // 50ms
  virtual int generate_next_task(ObITaskNew*& next_task)
  {
    UNUSED(next_task);
    return common::OB_ITER_END;
  }
  virtual int process() = 0;

private:
  int generate_next_task();
  bool judge_delay_penalty();
  const common::ObIArray<ObITaskNew*>& get_child_tasks() const
  {
    return children_;
  }
  ObITaskNew* get_next_task();
  void set_next_task(ObITaskNew* task);
  ObDagWorkerNew* get_worker() const
  {
    return worker_;
  }  // if lock will produce deadlock
  void set_worker(ObDagWorkerNew* worker)
  {
    worker_ = worker;
  }
  void set_switch_start_timestamp(int64_t timestamp)
  {
    switch_start_timestamp_ = timestamp;
  }
  void set_status(const ObITaskNewStatus status);
  bool is_valid() const;
  void reset();
  int copy_dep_to(ObITaskNew& other_task) const;
  void set_dag(ObIDagNew& dag)
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
  ObITaskNewColor get_color()
  {
    return color_;
  }
  void set_color(const ObITaskNewColor color)
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
  void set_status_(const ObITaskNewStatus status)
  {
    status_ = status;
  }
  int finish_task();

protected:
  ObIDagNew* dag_;

private:
  int64_t type_id_;
  lib::ObMutex lock_;
  ObITaskNewStatus status_;
  int64_t indegree_;  // indegree of the node in dag
  // int64_t outdegree_; // not use now
  int64_t last_visit_child_;
  ObITaskNewColor color_;
  int64_t switch_start_timestamp_;  // start timestamp of set switch flag
  ObITaskNew* next_task_;
  ObDagWorkerNew* worker_;
  common::ObSEArray<ObITaskNew*, DEFAULT_CHILDREN_NUM> children_;
};
class ObIDagNew : public common::ObDLinkBase<ObIDagNew> {
public:
  // priority from high to low
  enum ObIDagNewPriority {
    DAG_PRIO_0 = 0,
    DAG_PRIO_1 = 1,
    DAG_PRIO_2 = 2,
    DAG_PRIO_3 = 3,
    DAG_PRIO_4 = 4,
    DAG_PRIO_MAX,
  };

  enum ObDagStatus {
    DAG_STATUS_INITING = 0,
    DAG_STATUS_READY = 1,         // the dag task is ready
    DAG_STATUS_NODE_RUNNING = 2,  // the dag is running,the ready dag and the running dag are both in ready_list
    DAG_STATUS_FINISH = 3,
    DAG_STATUS_NODE_FAILED = 4,
    DAG_STATUS_ABORT = 5,  // not using status 5, 6, 7
    DAG_STATUS_RM = 6,
    DAG_STATUS_HALTED = 7,
  };

  explicit ObIDagNew(const int64_t type_id, const ObIDagNewPriority priority);
  virtual ~ObIDagNew();
  int add_task(ObITaskNew& task);
  bool has_finished();
  template <typename T>
  int alloc_task(T*& task);
  int64_t get_type_id() const
  {
    return type_id_;
  }
  void set_priority(ObIDagNew::ObIDagNewPriority prio)
  {
    priority_ = prio;
  }
  int64_t get_priority() const
  {
    return priority_;
  }
  void free_task(ObITaskNew& task);
  bool operator>=(const ObIDagNew& other) const
  {
    return priority_ >= other.get_priority();
  }
  bool operator>(const ObIDagNew& other) const
  {
    return priority_ > other.get_priority();
  }
  bool operator<=(const ObIDagNew& other) const
  {
    return priority_ <= other.get_priority();
  }
  bool is_equal(const ObIDagNew& other) const
  {
    return this == &other;
  }
  VIRTUAL_TO_STRING_KV(KP(this), K_(type_id), K_(id), K_(dag_ret), K_(dag_status), K_(start_time));

public:
  virtual int64_t hash() const = 0;
  virtual int64_t get_tenant_id() const = 0;
  virtual int fill_comment(char* buf, const int64_t buf_len) const = 0;
  virtual int64_t get_compat_mode() const = 0;
  virtual bool operator==(const ObIDagNew& other) const = 0;

protected:
  int dag_ret_;

private:
  typedef common::ObDList<ObITaskNew> TaskList;  // cycled list
  static const int64_t DEFAULT_TASK_NUM = 32;
  static const int64_t TOTAL_LIMIT = 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  friend class ObDagSchedulerNew;
  friend class ObTenantThreadPool;
  friend class ObITaskNew;
  friend class ObDagWorkerNew;

  enum GetTaskFlag {
    ONLY_READY = 0,
    RUNNABLE = 1,
  };

private:
  int finish_task(ObITaskNew& task);
  bool is_valid();
  int init(const int64_t total = TOTAL_LIMIT, const int64_t hold = HOLD_LIMIT, const int64_t page_size = PAGE_SIZE);
  // void restart_task(ObITaskNew &task);
  void reset();
  int check_cycle_();
  int get_next_ready_task(const GetTaskFlag flag, ObITaskNew*& task);
  void inc_running_task_cnt()
  {
    ++running_task_cnt_;
  }
  void dec_running_task_cnt()
  {
    --running_task_cnt_;
  }
  void dec_children_indegree(ObITaskNew* task);
  ObTenantThreadPool* get_tenant_thread_pool() const
  {
    return tenant_thread_pool_;
  }
  void set_tenant_thread_pool(ObTenantThreadPool* pool)
  {
    tenant_thread_pool_ = pool;
  }
  bool is_valid_type() const;
  int get_dag_ret() const
  {
    return dag_ret_;
  };
  void set_dag_ret(const int ret)
  {
    ATOMIC_VCAS(&dag_ret_, common::OB_SUCCESS, ret);
  }
  ObDagStatus get_dag_status() const
  {
    return dag_status_;
  }
  void set_dag_status(const ObDagStatus status)
  {
    dag_status_ = status;
  }
  const ObDagId& get_dag_id() const
  {
    return id_;
  }
  int set_dag_id(const ObDagId& dag_id);

private:
  common::ObConcurrentFIFOAllocator allocator_;
  bool is_inited_;
  ObIDagNewPriority priority_;
  int64_t type_id_;
  ObDagId id_;
  ObDagStatus dag_status_;
  int64_t running_task_cnt_;
  int64_t start_time_;
  lib::ObMutex lock_;  // lock of the dag
  ObTenantThreadPool* tenant_thread_pool_;
  TaskList task_list_;
};

template <typename T>
int ObIDagNew::alloc_task(T*& task)
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
    COMMON_LOG(INFO, "alloc task", K(ntask));
    ntask->set_dag(*this);
    ntask->set_type_id(type_id_);
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

OB_INLINE void ObITaskNew::yield()
{
  if (OB_NOT_NULL(worker_)) {
    ObDagWorkerNew::yield(worker_);
  }
}

}  // namespace share
}  // namespace oceanbase

#endif
