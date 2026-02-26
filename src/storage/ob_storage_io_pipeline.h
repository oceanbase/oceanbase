/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_STORAGE_IO_PIPELINE_H_
#define OCEANBASE_STORAGE_OB_STORAGE_IO_PIPELINE_H_

#include "lib/hash/ob_hashmap.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "lib/signal/ob_signal_utils.h"

namespace oceanbase
{
namespace storage
{

enum TaskState
{
  TASK_INIT               = 0,
  TASK_READ_IN_PROGRESS   = 1,
  TASK_READ_DONE          = 2,
  TASK_WRITE_IN_PROGRESS  = 3,
  TASK_WRITE_DONE         = 4,
  TASK_FINISHED           = 5,
  TASK_MAX_STATE          = 6,
};

bool is_valid_state(const TaskState state);
const char *get_state_str(const TaskState state);

class ObStorageIOPipelineTaskInfo
{
public:
  ObStorageIOPipelineTaskInfo();
  virtual ~ObStorageIOPipelineTaskInfo() { ObStorageIOPipelineTaskInfo::reset(); }

  int assign(const ObStorageIOPipelineTaskInfo &other);
  virtual void reset();
  virtual bool is_valid() const;
  virtual int refresh_state(TaskState &cur_state) = 0;
  virtual void cancel() = 0;

  int64_t get_buf_size() const { return buf_size_; }
  TaskState get_state() const { return state_; }
  int set_state(const TaskState target_state);
  static int set_macro_block_id(
      blocksstable::ObStorageObjectHandle &handle, const blocksstable::MacroBlockId &macro_id);
  static int set_macro_block_id(
      blocksstable::ObMacroBlockHandle &handle, const blocksstable::MacroBlockId &macro_id);

  VIRTUAL_TO_STRING_KV(K(state_), "state", get_state_str(state_), K(buf_size_));

protected:
  TaskState state_;
  int64_t buf_size_;
};

template<
    typename TaskInfoType_,
    typename = typename std::enable_if<
        std::is_base_of<ObStorageIOPipelineTaskInfo, TaskInfoType_>::value>::type
>
class ObStorageIOPipelineTask final
{
public:
  ObStorageIOPipelineTask(common::ObIAllocator &allocator)
      : info_(),
        buf_(nullptr),
        allocator_(allocator)
  {}
  ~ObStorageIOPipelineTask() { reset(); }
  bool is_valid() const { return OB_NOT_NULL(buf_) && info_.is_valid(); }

  int init(const TaskInfoType_ &task_info)
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(buf_) || OB_UNLIKELY(info_.is_valid())) {
      ret = OB_INIT_TWICE;
      OB_LOG(WARN, "ObStorageIOPipelineTask init twice", KR(ret), KPC(this));
    } else if (OB_UNLIKELY(!task_info.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KR(ret), K(task_info));
    } else if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(task_info.get_buf_size())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to allocate memory", KR(ret), K(task_info));
    } else if (OB_FAIL(info_.assign(task_info))) {
      OB_LOG(WARN, "fail to deep copy info", KR(ret), K(task_info));
    }

    if (OB_FAIL(ret)) {
      reset();
    }
    return ret;
  }

  void reset()
  {
    info_.reset();
    if (OB_NOT_NULL(buf_)) {
      // Need to pay attention!!!
      // The allocator is used to allocate io data buffer,
      // and its memory lifecycle needs to be longer than the object handle.
      allocator_.free(buf_);
      buf_ = nullptr;
    }
  }

  int refresh_state(TaskState &cur_state)
  {
    int ret = OB_SUCCESS;
    cur_state = TASK_MAX_STATE;
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_NOT_INIT;
      OB_LOG(WARN, "ObStorageIOPipelineTask not init", KR(ret), KPC(this));
    } else if (OB_FAIL(info_.refresh_state(cur_state))) {
      OB_LOG(WARN, "fail to refresh task state", KR(ret), KPC(this));
    }
    return ret;
  }

  int set_state(const TaskState target_state)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_NOT_INIT;
      OB_LOG(WARN, "ObStorageIOPipelineTask not init", KR(ret), KPC(this));
    } else if (OB_FAIL(info_.set_state(target_state))) {
      OB_LOG(WARN, "fail to set state", KR(ret), KPC(this), K(target_state));
    }
    return ret;
  }

  void cancel()
  {
    if (is_valid()) {
        info_.cancel();
    }
  }

  TO_STRING_KV(K(info_), KP(buf_));

public:
  TaskInfoType_ info_;
  char *buf_;
  common::ObIAllocator &allocator_;
};

/**
 * @brief A pipeline class for asynchronous IO task processing with read-write-complete stages.
 * This class manages the lifecycle of IO tasks,
 * handling state transitions from read -> write -> complete.
 * It allows asynchronous execution of IO operations by offloading tasks to the IOManager.
 * The pipeline maintains internal task queues and provides thread-safe task addition (add_task).
 * Users must implement async read/write operations and completion callbacks in derived classes.
 *
 *
 * Usage Requirements:
 *     1. Inherit this class and implement all pure virtual methods:
 *         do_async_read_(): Submit async read operation using task buffer
 *         do_async_write_(): Submit async write operation using read data
 *         do_complete_task_(): Handle final task completion
 *         on_task_succeeded_(): Success callback
 *         on_task_failed_(): Failure callback
 *     2. Implement init() in derived class calling basic_init_() with proper tenant_id
 *     3. Call process() periodically
 *
 *
 * Typical Usage:
 * class MyIOPipeline : public ObStorageIOPipeline
 * {
 * public:
 *   int init(uint64_t tenant_id)
 *   {
 *       return basic_init_(tenant_id);
 *   }
 *   // Implement virtual methods...
 * };
 *
 * // In service initialization:
 * MyIOPipeline pipeline;
 * pipeline.init(MTL_ID());
 *
 * // In background thread:
 * while (running)
 * {
 *   pipeline.process();
 *   usleep(1000);
 * }
 *
 * // In worker threads:
 * MyTaskInfo task_info;
 * while (OB_EAGAIN == pipeline.add_task(task_info))
 * {
 *   // Handle full pipeline case
 *   ob_usleep(1000);
 * }
 *
 * Thread Safety:
 * - add_task() is thread-safe through internal locking
 * - process() should be called from single thread
 * - Virtual methods will be called from process() context
 *
 * @tparam TaskInfoType_ Task metadata type (must inherit from ObStorageIOPipelineTaskInfo)
 *
 */
template<
    typename TaskInfoType_,
    typename = typename std::enable_if<
        std::is_base_of<ObStorageIOPipelineTaskInfo, TaskInfoType_>::value>::type
>
class ObStorageIOPipeline
{
public:
  using TaskType = ObStorageIOPipelineTask<TaskInfoType_>;

public:
  ObStorageIOPipeline(const int64_t parallelism = DEFAULT_PARALLELISM)
      : is_inited_(false),
        is_stopped_(false),
        tenant_id_(OB_INVALID_TENANT_ID),
        parallelism_(parallelism <= 0 ? DEFAULT_PARALLELISM : parallelism),
        allocator_(),
        task_arr_(nullptr),
        free_list_(),
        async_read_list_(),
        async_write_list_()
  {}
  virtual ~ObStorageIOPipeline() { destroy(); }
  TO_STRING_KV(K(is_inited_), K(is_stopped_), K(tenant_id_), K(parallelism_),
      KP(task_arr_), K(free_list_.get_curr_total()),
      K(async_read_list_.get_curr_total()), K(async_write_list_.get_curr_total()));

  int64_t get_parallelism() const { return parallelism_; }

  void stop()
  {
    ATOMIC_STORE(&is_stopped_, true);
  }

  void cancel()
  {
    if (IS_INIT) {
      for (int64_t i = 0; i < parallelism_; ++i) {
        task_arr_[i].cancel();
      }
    }
  }

  void destroy()
  {
    cancel();
    is_inited_ = false;
    is_stopped_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    parallelism_ = DEFAULT_PARALLELISM;

    free_list_.destroy();
    async_read_list_.destroy();
    async_write_list_.destroy();
    destroy_task_arr_();
    allocator_.reset();
  }

  int add_task(const TaskInfoType_ &info)
  {
    int ret = OB_SUCCESS;
    TaskType *task = nullptr;
    ret = check_init_and_stop_();
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!info.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid task info", KR(ret), K(info));
    } else if (OB_FAIL(try_pop_free_task_(task))) {
      OB_LOG(WARN, "fail to pop free task", KR(ret), K(info));
    } else if (OB_FAIL(task->init(info))) {
      OB_LOG(WARN, "fail to init task", KR(ret), K(info));
    } else if (OB_FAIL(task->set_state(TASK_READ_IN_PROGRESS))) {
      OB_LOG(WARN, "fail to update task state", KR(ret), KPC(task), K(info));
    } else if (OB_FAIL(do_async_read_(*task))) {
      OB_LOG(WARN, "fail to do async read", KR(ret), KPC(task), K(info));
    } else if (OB_FAIL(async_read_list_.push(task))) {
      OB_LOG(ERROR, "fail to push task to async read list", KR(ret), KPC(task), K(info), KPC(this));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(task)) {
      int tmp_ret = OB_SUCCESS;
      if (task->is_valid()) {
        if (OB_TMP_FAIL(on_task_failed_(*task))) {
          OB_LOG(WARN, "fail to exec fail callback", KR(ret), KP(tmp_ret), KPC(task), K(info));
        }
      }

      if (OB_TMP_FAIL(recycle_task_(*task))) {
        OB_LOG(WARN, "fail to recycle task", KR(ret), KP(tmp_ret),
            KPC(task), K(info), K(free_list_.get_curr_total()));
      }
    }

    return ret;
  }

  int process()
  {
    int ret = OB_SUCCESS;
    ret = check_init_and_stop_();
    if (FAILEDx(schedule_async_writes_())) {
      OB_LOG(WARN, "failed to schedule async writes", KR(ret));
    } else if (OB_FAIL(complete_finished_tasks_())) {
      OB_LOG(WARN, "failed to complete finished tasks", KR(ret));
    }
    return ret;
  }

protected:
  int check_init_and_stop_() const
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      OB_LOG(WARN, "ObStorageIOPipeline not init", KR(ret));
    } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
      ret = OB_SERVICE_STOPPED;
      OB_LOG(WARN, "ObStorageIOPipeline is stopped", KR(ret));
    }
    return ret;
  }

  int try_pop_free_task_(TaskType *&task)
  {
    int ret = OB_SUCCESS;
    task = nullptr;
    ret = check_init_and_stop_();
    if (FAILEDx(free_list_.pop(task))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_EAGAIN;
      }
      OB_LOG(WARN, "fail to pop free task", KR(ret), KPC(this));
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "pop free task is null", KR(ret), KPC(this));
    }
    return ret;
  }

  int schedule_async_writes_()
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    int64_t io_cnt = 0;
    ret = check_init_and_stop_();
    if (OB_SUCC(ret)) {
      io_cnt = async_read_list_.get_curr_total();
    }

    TaskState state = TASK_MAX_STATE;
    TaskType *task = nullptr;
    bool need_recycle = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < io_cnt && !ATOMIC_LOAD(&is_stopped_); ++i) {
      state = TASK_MAX_STATE;
      task = nullptr;
      need_recycle = true;

      if (OB_FAIL(async_read_list_.pop(task))) {
        OB_LOG(WARN, "fail to pop task from async_read_list", KR(ret), K(i), K(io_cnt));
      } else if (OB_ISNULL(task) || OB_UNLIKELY(!task->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "task is invalid", KR(ret), K(i), K(io_cnt), KPC(task));
      } else if (OB_FAIL(task->refresh_state(state))) {
        OB_LOG(WARN, "fail to refresh task state", KR(ret), K(i), K(io_cnt), KPC(task));
      } else if (TASK_READ_DONE == state) {
        if (OB_FAIL(task->set_state(TASK_WRITE_IN_PROGRESS))) {
          OB_LOG(WARN, "fail to update task state", KR(ret), K(i), K(io_cnt), KPC(task));
        } else if (OB_FAIL(do_async_write_(*task))) {
          OB_LOG(WARN, "fail to do async write", KR(ret), K(i), K(io_cnt), KPC(task));
        } else if (OB_FAIL(async_write_list_.push(task))) {
          OB_LOG(ERROR, "fail to push into async_write_list",
              KR(ret), K(i), K(io_cnt), KPC(task), KPC(this));
        } else {
          need_recycle = false;
        }
      } else if (TASK_READ_IN_PROGRESS == state) {
        if (OB_FAIL(async_read_list_.push(task))) {
          OB_LOG(ERROR, "fail to push back into async_read_list",
              KR(ret), K(i), K(io_cnt), KPC(task), KPC(this));
        } else {
          need_recycle = false;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "task not in reading state, but in async_read_list_",
            KR(ret), K(i), K(io_cnt), KPC(task));
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(task) && task->is_valid()) {
        if (OB_TMP_FAIL(on_task_failed_(*task))) {
          OB_LOG(WARN, "fail to exec fail callback", KR(ret), KP(tmp_ret), K(i), K(io_cnt), KPC(task));
        }
      }
      if (need_recycle && OB_NOT_NULL(task)) {
        if (OB_TMP_FAIL(recycle_task_(*task))) {
          OB_LOG(WARN, "fail to recycle task", KR(ret), KP(tmp_ret),
            K(i), K(io_cnt), KPC(task), K(free_list_.get_curr_total()));
        }
      }

      ret = OB_SUCCESS; // ignore ret, process next task
    }

    return ret;
  }

  int complete_finished_tasks_()
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    int64_t io_cnt = 0;
    ret = check_init_and_stop_();
    if (OB_SUCC(ret)) {
      io_cnt = async_write_list_.get_curr_total();
    }

    TaskState state = TASK_MAX_STATE;
    TaskType *task = nullptr;
    bool need_recycle = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < io_cnt && !ATOMIC_LOAD(&is_stopped_); ++i) {
      state = TASK_MAX_STATE;
      task = nullptr;
      need_recycle = true;

      if (OB_FAIL(async_write_list_.pop(task))) {
        OB_LOG(WARN, "fail to pop task from async_write_list", KR(ret), K(i), K(io_cnt));
      } else if (OB_ISNULL(task) || OB_UNLIKELY(!task->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "task is invalid", KR(ret), K(i), K(io_cnt), KPC(task));
      } else if (OB_FAIL(task->refresh_state(state))) {
        OB_LOG(WARN, "fail to refresh task state", KR(ret), K(i), K(io_cnt), KPC(task));
      } else if (TASK_WRITE_DONE == state) {
        if (OB_FAIL(do_complete_task_(*task))) {
          OB_LOG(WARN, "fail to complete task", KR(ret), K(i), K(io_cnt), KPC(task));
        } else if (OB_FAIL(task->set_state(TASK_FINISHED))) {
          OB_LOG(WARN, "fail to update task state", KR(ret), K(i), K(io_cnt), KPC(task));
        } else {
          if (OB_TMP_FAIL(on_task_succeeded_(*task))) {
            OB_LOG(WARN, "fail to exec succeed callback", KR(tmp_ret), K(i), K(io_cnt), KPC(task));
          }
        }
      } else if (TASK_WRITE_IN_PROGRESS == state) {
        if (OB_FAIL(async_write_list_.push(task))) {
          OB_LOG(ERROR, "fail to push back into async_write_list",
              KR(ret), K(i), K(io_cnt), KPC(task), KPC(this));
        } else {
          need_recycle = false;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "task not in writing state, but in async_write_list",
            KR(ret), K(i), K(io_cnt), KPC(task));
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(task) && task->is_valid()) {
        if (OB_TMP_FAIL(on_task_failed_(*task))) {
          OB_LOG(WARN, "fail to exec fail callback", KR(ret), KP(tmp_ret), K(i), K(io_cnt), KPC(task));
        }
      }
      if (need_recycle && OB_NOT_NULL(task)) {
        if (OB_TMP_FAIL(recycle_task_(*task))) {
          OB_LOG(WARN, "fail to recycle task", KR(ret), KP(tmp_ret),
            K(i), K(io_cnt), KPC(task), K(free_list_.get_curr_total()));
        }
      }

      ret = OB_SUCCESS; // ignore ret, process next task
    }

    return ret;
  }

  virtual int on_task_failed_(TaskType &task) = 0;
  virtual int on_task_succeeded_(TaskType &task) = 0;
  virtual int do_async_read_(TaskType &task) = 0;
  virtual int do_async_write_(TaskType &task) = 0;
  virtual int do_complete_task_(TaskType &task) = 0;

protected:
  int basic_init_(const uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    ObMemAttr attr(tenant_id, "IOPipeline");
    SET_IGNORE_MEM_VERSION(attr);
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
      OB_LOG(WARN, "ObStorageIOPipeline init twice", KR(ret));
    } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid args", KR(ret), K(tenant_id));
    } else if (OB_FAIL(allocator_.init(
        lib::ObMallocAllocator::get_instance(), OB_MALLOC_BIG_BLOCK_SIZE, attr))) {
      OB_LOG(WARN, "fail to init allocator", KR(ret));
    } else if (OB_FAIL(free_list_.init(parallelism_, &allocator_, attr))) {
      OB_LOG(WARN, "fail to init free_list", KR(ret));
    } else if (OB_FAIL(async_write_list_.init(parallelism_, &allocator_, attr))) {
      OB_LOG(WARN, "fail to init async_write_list", KR(ret));
    } else if (OB_FAIL(async_read_list_.init(parallelism_, &allocator_, attr))) {
      OB_LOG(WARN, "fail to init async_read_list", KR(ret));
    } else if (OB_FAIL(pre_alloc_tasks_())) {
      OB_LOG(WARN, "fail to pre_alloc_tasks", KR(ret));
    } else {
      tenant_id_ = tenant_id;
      is_inited_ = true;
    }
    return ret;
  }

  int recycle_task_(TaskType &task)
  {
    int ret = OB_SUCCESS;
    task.reset();
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      OB_LOG(WARN, "ObStorageIOPipeline not init", KR(ret));
    } else if (OB_FAIL(free_list_.push(&task))) {
      OB_LOG(ERROR, "fail to push flush_entry into free_list", KR(ret),
          K(task), KP(&task), K(free_list_.get_curr_total()), KPC(this));
    }
    return ret;
  }

private:
  int pre_alloc_tasks_()
  {
    int ret = OB_SUCCESS;
    const int64_t mem_size = sizeof(TaskType) * parallelism_;
    if (OB_ISNULL(task_arr_ = static_cast<TaskType *>(allocator_.alloc(mem_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc memory", KR(ret), K(mem_size));
    } else {
      // make sure that all entries in task_arr_ are properly initialized
      for (int64_t i = 0; OB_SUCC(ret) && (i < parallelism_); ++i) {
        TaskType *task = new (task_arr_ + i) TaskType(allocator_);
      }

      for (int64_t i = 0; OB_SUCC(ret) && (i < parallelism_); ++i) {
        if (OB_FAIL(free_list_.push(task_arr_ + i))) {
          OB_LOG(WARN, "fail to push task into free_list", KR(ret),
              K(i), K(free_list_.get_curr_total()));
        }
      }
    }

    if (OB_FAIL(ret)) {
      destroy_task_arr_();
    }
    return ret;
  }

  void destroy_task_arr_()
  {
    if (OB_NOT_NULL(task_arr_)) {
      for (int64_t i = 0; i < parallelism_; ++i) {
        task_arr_[i].~TaskType();
      }
      allocator_.free(task_arr_);
      task_arr_ = nullptr;
    }
  }

protected:
  static const int64_t DEFAULT_PARALLELISM = 32;

  bool is_inited_;
  bool is_stopped_;
  uint64_t tenant_id_;
  int64_t parallelism_;
  common::ObFIFOAllocator allocator_;
  TaskType *task_arr_;
  common::ObFixedQueue<TaskType> free_list_;
  common::ObFixedQueue<TaskType> async_read_list_;
  common::ObFixedQueue<TaskType> async_write_list_;
};


class TaskInfoWithRWHandle : public ObStorageIOPipelineTaskInfo
{
public:
  TaskInfoWithRWHandle();
  virtual ~TaskInfoWithRWHandle() { TaskInfoWithRWHandle::reset(); }

  int assign(const TaskInfoWithRWHandle &other);
  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual int refresh_state(TaskState &cur_state) override;
  virtual void cancel() override;

  INHERIT_TO_STRING_KV("ObStorageIOPipelineTaskInfo", ObStorageIOPipelineTaskInfo,
      K(read_handle_), K(write_handle_));

protected:
  virtual int refresh_read_state_();
  virtual int refresh_write_state_();

public:
  blocksstable::ObStorageObjectHandle read_handle_;
  blocksstable::ObStorageObjectHandle write_handle_;
};

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_OB_STORAGE_IO_PIPELINE_H_