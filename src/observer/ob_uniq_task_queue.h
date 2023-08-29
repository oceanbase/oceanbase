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

#ifndef OCEANBASE_OBSERVER_OB_UNIQ_TASK_QUEUE_H_
#define OCEANBASE_OBSERVER_OB_UNIQ_TASK_QUEUE_H_

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/list/ob_dlist.h"
#include "lib/queue/ob_dedup_queue.h"
#include "lib/lock/ob_thread_cond.h"
#include "share/ob_thread_pool.h"
#include "share/ob_debug_sync.h"
#include "share/ob_debug_sync_point.h"

namespace oceanbase
{
namespace observer
{
// implement interface of DefaultSimpleAllocerAllocator
struct ObHighPrioMemAllocator
{
public:
  explicit ObHighPrioMemAllocator(const char *label = common::ObModIds::OB_PARTITION_TABLE_TASK)
  {
    attr_.label_ = label;
    attr_.prio_ = lib::OB_HIGH_ALLOC;
  }

  void *alloc(const int64_t sz);
  void free(void *p);
  void set_label(const char *label) { attr_.label_ = label; }
  void set_attr(const common::ObMemAttr &attr) { attr_ = attr; }
private:
  common::ObMemAttr attr_;
};
template<typename T>
struct CompareT
{
public:
  bool operator()(const T &a, const T &b) const
  {
    return a.compare_without_version(b);
  }
};

// The tasks for ObUniqTaskQueue need implement the following interfaces.
template <typename Task>
class ObIUniqTaskQueueTask : public common::ObDLinkBase<Task>
{
public:
  virtual ~ObIUniqTaskQueueTask() {}
  // Barrier task is defined by is_barrier(). It should be processed alone.
  // Barrier task can be used to monitor queue's running status.
  virtual bool is_barrier() const = 0;
  // If need_process_alone() returns false, the task will be processed in batches.
  virtual bool need_process_alone() const = 0;
  // The hash() and compare_without_version(...) are used to remove duplication when
  // adding tasks into queue.
  virtual int64_t hash() const = 0;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; };
  virtual bool compare_without_version(const Task &other) const = 0;
  // The following two interfaces are used to update the waiting tasks with same key.
  virtual bool need_assign_when_equal() const = 0;
  virtual int assign_when_equal(const Task &other) = 0;
  // get_group_id() is used for classifying tasks and batch processing
  virtual uint64_t get_group_id() const = 0;
  // Basic interfaces.
  virtual void reset() = 0;
  virtual bool is_valid() const = 0;
  virtual bool operator==(const Task &other) const = 0;
};

// Unique task queue, like ObDedupQueue but tasks are unique only in queue.
// If task be poped for processing, same task can be added to queue successfully
// before process finish.
template <typename Task, typename Process>
class ObUniqTaskQueue : public share::ObThreadPool
{
public:
  friend class TestBatchProcessQueue_test_reput_Test;
  friend class TestBatchProcessQueue_test_eput2_Test;
  friend class TestBatchProcessQueue_test_update_process_Test;
  friend class TestBatchProcessQueue_test_single_update_Test;
  static const int64_t QUEUE_WAIT_INTERVAL_MS = 200; // 200ms
  ObUniqTaskQueue() : inited_(false), queue_size_(0), thread_name_(nullptr), task_set_(),
                      task_count_(0), group_map_(), processing_task_set_(), cur_group_(NULL),
                      processing_thread_count_(0), barrier_task_count_(0),
  updater_(NULL) {}
  virtual ~ObUniqTaskQueue() { }

  int init(Process *process, const int64_t thread_num, const int64_t queue_size,
           const char *thread_name = nullptr);
  // init() will trigger start(), we only want to init and start later in some cases
  int start() override;
  int init_only(
      Process *process,
      const int64_t thread_num,
      const int64_t queue_size,
      const char* thread_name = nullptr);

  // Add task to queue, never block
  // return value:
  //      OB_SUCCESS: for success
  //      OB_EAGAIN: same task exist
  //      OB_SIZE_OVERFLOW: queue size overflow
  virtual int add(const Task &task);

  virtual int check_exist(const Task &task, bool &exist);

  virtual void run1();

  virtual int64_t task_count() const
  {
    return task_count_;
  }
private:
  struct Group : public common::ObDLinkBase<Group>
  {
    Group() {}
    explicit Group(const Group &group) { UNUSED(group); }
    virtual ~Group() {}
    Group& operator=(const Group &group) { UNUSED(group); return *this; }
    common::ObDList<Task> list_;
  };
  int get_group(const uint64_t group_id, Group *&group);
  int get_next_group(Group *&group);
  int batch_process_tasks(common::ObIArray<Task> &tasks);
  int process_barrier(Task &task);
  int try_lock(const Task &tasks);
  int batch_unlock(const common::ObIArray<Task> &tasks);

  bool inited_;
  int64_t queue_size_;
  const char *thread_name_;
  common::ObThreadCond cond_;
  // FIXME baihua: half memory wasted here.
  common::hash::ObHashSet<Task,
      common::hash::NoPthreadDefendMode,
      common::hash::hash_func<Task>,
      common::hash::equal_to<Task>,
      common::hash::SimpleAllocer<typename common::hash::HashSetTypes<Task>::AllocType,
      common::hash::NodeNumTraits<Task>::NODE_NUM,
      common::hash::NoPthreadDefendMode, ObHighPrioMemAllocator> > task_set_;
  int64_t task_count_;
  common::hash::ObHashMap<uint64_t, Group,
      common::hash::NoPthreadDefendMode,
      common::hash::hash_func<uint64_t>,
      common::hash::equal_to<uint64_t>,
      common::hash::SimpleAllocer<
          typename common::hash::HashMapTypes<uint64_t, Group>::AllocType,
      common::hash::NodeNumTraits<Group>::NODE_NUM,
      common::hash::NoPthreadDefendMode, ObHighPrioMemAllocator> > group_map_;
  common::hash::ObHashSet<Task,
      common::hash::NoPthreadDefendMode,
      common::hash::hash_func<Task>,
      CompareT<Task>,
      common::hash::SimpleAllocer<typename common::hash::HashSetTypes<Task>::AllocType,
      common::hash::NodeNumTraits<Task>::NODE_NUM,
      common::hash::NoPthreadDefendMode, ObHighPrioMemAllocator> > processing_task_set_;
  common::ObDList<Group> groups_;
  Group *cur_group_;
  int64_t processing_thread_count_;
  int64_t barrier_task_count_;
  Process *updater_;
  DISALLOW_COPY_AND_ASSIGN(ObUniqTaskQueue);
};
// TODO: init should not trigger start(), have to remove start() out of init()
template <typename Task, typename Process>
int ObUniqTaskQueue<Task, Process>::init(Process *updater, const int64_t thread_num,
                                         const int64_t queue_size, const char *thread_name)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(init_only(updater, thread_num, queue_size, thread_name))) {
    SERVER_LOG(WARN, "fail to init only", K(ret), K(thread_num), K(queue_size));
  } else if (OB_FAIL(start())) {
    inited_ = false;
    SERVER_LOG(WARN, "start thread failed", K(ret), K(thread_num));
  } else {
    inited_ = true;
  }
  return ret;
}

template <typename Task, typename Process>
int ObUniqTaskQueue<Task, Process>::init_only(Process *updater, const int64_t thread_num,
                                              const int64_t queue_size, const char *thread_name)
{
  int ret = common::OB_SUCCESS;
  ObMemAttr attr(OB_SERVER_TENANT_ID, common::ObModIds::OB_PARTITION_TABLE_TASK);
  SET_USE_500(attr);
  const int64_t group_count = 128;
  if (inited_) {
    ret = common::OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (thread_num <= 0 || queue_size <= 0 || OB_ISNULL(updater)) {
    ret = common::OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(thread_num), K(queue_size), K(updater));
  } else if (OB_FAIL(cond_.init(common::ObWaitEventIds::PARTITION_TABLE_UPDATER_COND_WAIT))) {
    SERVER_LOG(WARN, "fai to init condition, ", K(ret));
  } else if (OB_FAIL(task_set_.create(queue_size, attr, attr))) {
    SERVER_LOG(WARN, "create hash map failed", K(ret), K(queue_size));
  } else if (OB_FAIL(group_map_.create(group_count,
                                       attr, attr))) {
    SERVER_LOG(WARN, "create hash map failed", K(ret), K(group_count));
  } else if (OB_FAIL(processing_task_set_.create(common::UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM * thread_num,
                                                 attr, attr))) {
    SERVER_LOG(WARN, "create hash map failed", K(ret));
  } else {
    this->set_thread_count(static_cast<int32_t>(thread_num));
    queue_size_ = queue_size;
    task_count_ = 0;
    cur_group_ = NULL;
    processing_thread_count_ = 0;
    barrier_task_count_ = 0;
    updater_ = updater;
    thread_name_ = thread_name;
    inited_ = true;
  }
  return ret;
}

template <typename Task, typename Process>
int ObUniqTaskQueue<Task, Process>::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObUniqTaskQueue is not inited", K(ret), K_(inited));
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    SERVER_LOG(WARN, "start thread failed", K(ret));
  }
  return ret;
}

template <typename Task, typename Process>
int ObUniqTaskQueue<Task, Process>::check_exist(const Task &task, bool &exist)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = common::OB_NOT_INIT;
    SERVER_LOG(WARN, "task queue not inited", K(ret));
  } else if (!task.is_valid() || nullptr != task.get_next() || nullptr != task.get_prev()) {
    ret = common::OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), K(task));
  } else {
    exist = false;
    common::ObThreadCondGuard guard(cond_);
    const Task *stored_task = nullptr;
    if (nullptr == (stored_task = task_set_.get(task))) {
      exist = false;
    } else {
      exist = true;
    }
  }
  return ret;
}

template <typename Task, typename Process>
int ObUniqTaskQueue<Task, Process>::add(const Task &task)
{
  int ret = common::OB_SUCCESS;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SERVER_LOG(WARN, "task queue not inited", K(ret));
  } else if (!task.is_valid() || NULL != task.get_next() || NULL != task.get_prev()) {
    ret = common::OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), K(task));
  } else {
    common::ObThreadCondGuard guard(cond_);
    if (task_count_ >= queue_size_) {
      ret = common::OB_SIZE_OVERFLOW;
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        SERVER_LOG(WARN, "task queue size overflow",
                   K(ret), K_(queue_size), K(common::lbt()));
      }
    } else {
      const Task *stored_task = NULL;
      if (OB_FAIL(task_set_.set_refactored(task, 0))) {
        if (common::OB_HASH_EXIST == ret) {
          if (task.need_assign_when_equal()) {
            if (NULL == (stored_task = task_set_.get(task))) {
              ret = common::OB_ERR_SYS;
              SERVER_LOG(WARN, "get inserted task failed", K(ret), K(task));
            } else if (OB_FAIL(const_cast<Task *>(stored_task)->assign_when_equal(task))) {
              SERVER_LOG(WARN, "assign task failed", K(ret), K(task));
            }
          } else {
            ret = common::OB_EAGAIN;
            SERVER_LOG(TRACE, "same task exist", K(task));
          }
        } else {
          SERVER_LOG(WARN, "insert into hash failed", K(ret), K(task));
        }
      } else if (NULL == (stored_task = task_set_.get(task))) {
        ret = common::OB_ERR_SYS;
        SERVER_LOG(WARN, "get inserted task failed", K(ret), K(task));
      } else {
        Group *group = NULL;
        const uint64_t group_id = task.get_group_id();
        if (OB_FAIL(get_group(group_id, group))) {
          SERVER_LOG(WARN, "get group failed", K(group_id), K(ret));
        } else if (NULL == group) {
          ret = common::OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "group is null", K(group_id), K(ret));
        } else if (!group->list_.add_last(const_cast<Task *>(stored_task))) {
          ret = common::OB_ERR_SYS;
          SERVER_LOG(WARN, "add task to list failed", K(ret));
        } else {
          ++task_count_;
          int tmp_ret = common::OB_SUCCESS;
          if (barrier_task_count_ > 0) {
            if (common::OB_SUCCESS != (tmp_ret = cond_.broadcast())) {
              SERVER_LOG(WARN, "condition broadcast fail", K(tmp_ret));
            }
          } else {
            if (common::OB_SUCCESS != (tmp_ret = cond_.signal())) {
              SERVER_LOG(WARN, "condition signal fail", K(tmp_ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

template <typename Task, typename Process>
void ObUniqTaskQueue<Task, Process>::run1()
{
  SERVER_LOG(INFO, "UniqTaskQueue thread start");
  int ret = common::OB_SUCCESS;
  Group *group = NULL;
  const int64_t batch_exec_cnt = common::UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM;
  common::ObArray<Task> tasks;
  if (thread_name_ != nullptr) {
    lib::set_thread_name(thread_name_, get_thread_idx());
  }
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else {
    while (!lib::Thread::current().has_set_stop()) {
      DEBUG_SYNC(common::BEFORE_UNIQ_TASK_RUN);
      Task *t = NULL;
      tasks.reuse();
      if (OB_SUCC(tasks.reserve(batch_exec_cnt))) {
        group = NULL;
        common::ObThreadCondGuard guard(cond_);
        if (task_count_ > 0) {
          if (OB_FAIL(get_next_group(group))) {
            SERVER_LOG(WARN, "get_next_next failed", K(ret));
          } else if (NULL == group || group->list_.get_size() <= 0) {
            ret = common::OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "group is null or group is empty", K(ret));
          } else {
            while (common::OB_SUCCESS == ret && tasks.count() < batch_exec_cnt
                && !group->list_.is_empty()) {
              if (tasks.count() > 0 && group->list_.get_first()->need_process_alone()) {
                break;
              } else if (OB_FAIL(try_lock(*group->list_.get_first()))) {
                ret = common::OB_SUCCESS;
                break;
              } else {
                t = group->list_.remove_first();
                if (NULL == t) {
                  ret = common::OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "remove first return null", K(ret));
                } else if (OB_FAIL(tasks.push_back(*t))) {
                  SERVER_LOG(WARN, "push_back failed", K(ret));
                } else {
                  if (OB_FAIL(task_set_.erase_refactored(*t))) {
                    SERVER_LOG(WARN, "erase task from task map failed", K(ret), "task", *t);
                  } else {
                    --task_count_;
                  }
                }
                if (OB_SUCC(ret)
                    && tasks.at(tasks.count() - 1).need_process_alone()) {
                  break;
                }
              }
            } //end while
            if (common::OB_SUCCESS == ret && tasks.count() > 0) {
              ++processing_thread_count_;
              if (group->list_.get_size() <= 0) {
                if (group->get_next() == groups_.get_header()) {
                  // bugfix: workitem/49006474
                  cur_group_ = group->get_next();
                } else {
                  cur_group_ = group->get_prev();
                }
                if (NULL == groups_.remove(group)) {
                  ret = common::OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "groups remove return null", K(ret));
                } else {
                  if (OB_FAIL(group_map_.erase_refactored(tasks.at(0).get_group_id()))) {
                    SERVER_LOG(WARN, "erase group from group_map failed",
                        K(ret), "group_id", tasks.at(0).get_group_id());
                  }
                }
              } else {
                cur_group_ = group;
              }
            }
          }
        } else {
          cond_.wait(QUEUE_WAIT_INTERVAL_MS);
        }
      } else {//end cond_
        ob_usleep(QUEUE_WAIT_INTERVAL_MS * 1000);
      }
      if (common::OB_SUCCESS == ret && tasks.count() > 0) {
        bool is_batch_execute = false;
        FOREACH_CNT_X(task, tasks, common::OB_SUCCESS == ret) {
          if (OB_ISNULL(task)) {
            ret = common::OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "get invalid task", K(ret), K(task));
          } else if (task->is_barrier()) {
            if (is_batch_execute) {
              SERVER_LOG(ERROR, "barrier task should not execute with non-barrier task", K(*task));
            }
            common::ObThreadCondGuard guard(cond_);
            ++barrier_task_count_;
            --processing_thread_count_;
            if (0 == processing_thread_count_) {
              int tmp_ret = cond_.broadcast();
              if (common::OB_SUCCESS != tmp_ret) {
                SERVER_LOG(WARN, "condition broadcast fail", K(tmp_ret));
              }
            }
            while (0 != processing_thread_count_) {
              cond_.wait(QUEUE_WAIT_INTERVAL_MS);
            }
            if (OB_FAIL(process_barrier(*task))) {
              SERVER_LOG(WARN, "process task failed", "task", *task, K(ret));
            }
            --barrier_task_count_;
            ++processing_thread_count_;
          } else {
            is_batch_execute = true;
          }
        } //end foreach
        if (OB_SUCC(ret) && is_batch_execute) {
          if (OB_FAIL(batch_process_tasks(tasks))) {
            SERVER_LOG(WARN, "fail to batch execute task", K(ret), K(tasks.count()));
          }
        }
        // always decrease $processing_thread_count_
        common::ObThreadCondGuard guard(cond_);
        --processing_thread_count_;
        if (0 == processing_thread_count_ && barrier_task_count_ > 0) {
          int tmp_ret = cond_.broadcast();
          if (common::OB_SUCCESS != tmp_ret) {
            SERVER_LOG(WARN, "condition broadcast fail", K(tmp_ret));
          }
        }
      }
      if (tasks.count() > 0) {
        common::ObThreadCondGuard guard(cond_);
        if (OB_FAIL(batch_unlock(tasks))) {
          SERVER_LOG(ERROR, "fail to batch unlock task", K(ret));
        }
      }
    }
  }
  SERVER_LOG(INFO, "UniqTaskQueue thread stop");
}

template <typename Task, typename Process>
int ObUniqTaskQueue<Task, Process>::process_barrier(Task &task)
{
  int ret = common::OB_SUCCESS;
  bool stopped = lib::Thread::current().has_set_stop();
  if (OB_ISNULL(updater_)) {
    ret = common::OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid updater", K(ret), K(updater_));
  } else if (OB_FAIL(updater_->process_barrier(task, stopped))) {
    SERVER_LOG(WARN, "fail to batch process task", K(ret));
  }
  return ret;
}

template <typename Task, typename Process>
int ObUniqTaskQueue<Task, Process>::batch_process_tasks(common::ObIArray<Task> &tasks)
{
  int ret = common::OB_SUCCESS;
  bool stopped = lib::Thread::current().has_set_stop();
  if (0 == tasks.count()) {
    //nothing todo
  } else if (OB_ISNULL(updater_)) {
    ret = common::OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid updater", K(ret), K(updater_));
  } else if (OB_FAIL(updater_->batch_process_tasks(tasks, stopped))) {
    SERVER_LOG(WARN, "fail to batch process task", K(ret));
  }
  return ret;
}



template <typename Task, typename Process>
int ObUniqTaskQueue<Task, Process>::get_group(const uint64_t group_id, Group *&group)
{
  int ret = common::OB_SUCCESS;
  group = NULL;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (common::OB_INVALID_ID == group_id) {
    ret = common::OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid group_id", K(group_id), K(ret));
  } else {
    Group new_group;
    if (OB_FAIL(group_map_.set_refactored(group_id, new_group)) && common::OB_HASH_EXIST != ret) {
      SERVER_LOG(WARN, "group_map_ set failed", K(group_id), K(ret));
    } else if (NULL == (group = const_cast<Group *>(group_map_.get(group_id)))) {
      ret = common::OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "group_map_ get failed", K(group_id), K(ret));
    } else if (common::OB_SUCCESS == ret) {
      if (!groups_.add_last(group)) {
        ret = common::OB_ERR_SYS;
        SERVER_LOG(WARN, "add group to groups failed", K(ret));
      }
    } else if (common::OB_HASH_EXIST == ret) {
      ret = common::OB_SUCCESS;
    } else {
      // do nothing
    }
  }
  return ret;
}

template <typename Task, typename Process>
int ObUniqTaskQueue<Task, Process>::try_lock(const Task &task)
{
  int ret = common::OB_SUCCESS;
  if (!task.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "get invalid task", K(ret), K(task));
  } else if (OB_FAIL(processing_task_set_.set_refactored(task, 0))) {
    if (common::OB_HASH_EXIST == ret) {
      ret = common::OB_EAGAIN;
      SERVER_LOG(TRACE, "same task exist", K(task));
    } else {
      SERVER_LOG(WARN, "fail to lock task", K(ret), K(task));
    }
  }
  return ret;
}

template <typename Task, typename Process>
int ObUniqTaskQueue<Task, Process>::batch_unlock(const common::ObIArray<Task> &tasks)
{
  int tmp_ret = common::OB_SUCCESS;
  int ret = common::OB_SUCCESS;
  FOREACH_CNT(task, tasks) {
    if (OB_ISNULL(task)) {
      tmp_ret = common::OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "get invalid task", K(tmp_ret), K(task));
    } else if (common::OB_SUCCESS != (tmp_ret = processing_task_set_.erase_refactored(*task))) {
      SERVER_LOG(ERROR, "fail to erase task", K(tmp_ret), K(*task));
    }
    if (common::OB_SUCCESS != tmp_ret && OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}


template <typename Task, typename Process>
int ObUniqTaskQueue<Task, Process>::get_next_group(Group *&next_group)
{
  int ret = common::OB_SUCCESS;
  next_group = NULL;
  bool can_execute_barrier = true;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (groups_.get_size() <= 0) {
    ret = common::OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "group is empty", K(ret));
  } else {
    Group *start_group = (NULL == cur_group_) ? groups_.get_header() : cur_group_;
    Group *group = start_group;
    Group *barrier_group = NULL;
    if (NULL == group) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "null group", K(ret), KP(group));
    }
    if (OB_SUCC(ret)) {
      do {
        group = group->get_next();
        if (NULL == group) {
          ret = common::OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "null group", K(ret), KP(group));
        } else if (groups_.get_header() == group) {
          // ignore
        } else if (group->list_.get_size() > 0) {
          if (!group->list_.get_first()->is_barrier()) {
            can_execute_barrier = false;
            next_group = group;
            break;
          } else {
            barrier_group = group;
          }
        }
      } while (OB_SUCC(ret) && group != start_group);
    }

    if (OB_SUCC(ret)) {
      if (NULL == next_group && can_execute_barrier) {
        next_group = barrier_group;
      }

      if (NULL == next_group || next_group->list_.get_size() < 0) {
        ret = common::OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "group has task not found", K(ret));
      }
    }
  }
  return ret;
}

} // end namespace observer
} // end namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_UNIQ_TASK_QUEUE_H
