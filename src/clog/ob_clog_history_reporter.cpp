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

#include "lib/coro/co.h"
#include "clog/ob_clog_history_reporter.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/thread/ob_thread_name.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/config/ob_server_config.h"
#include "share/ob_thread_mgr.h"
#include "observer/ob_server_struct.h"

#define STAT(level, tag_str, args...) CLOG_LOG(level, "[CLOG_HISTORY_INFO] " tag_str, ##args)
#define _STAT(level, tag_str, args...) _CLOG_LOG(level, "[CLOG_HISTORY_INFO] " tag_str, ##args)
#define ISTAT(tag_str, args...) STAT(INFO, tag_str, ##args)
#define _ISTAT(tag_str, args...) _STAT(INFO, tag_str, ##args)
#define CLOG_DEBUG(tag_str, args...) STAT(DEBUG, tag_str, ##args)

namespace oceanbase {
using namespace lib;
using namespace common;
using namespace share;
using namespace common::sqlclient;
using namespace share::schema;
namespace clog {
int ObClogHistoryReporter::init(common::ObMySQLProxy* sql_proxy, const common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  constexpr const char* TASK_MAP_LABEL = ObModIds::OB_CLOG_HISTORY_TASK_MAP;
  constexpr uint64_t TASK_MAP_TENANT_ID = OB_SERVER_TENANT_ID;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObClogHistoryReporter init twice", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(sql_proxy) || OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(sql_proxy), K(addr));
  } else if (OB_FAIL(task_map_.init(TASK_MAP_LABEL, TASK_MAP_TENANT_ID))) {
    CLOG_LOG(WARN, "task_map_ init error", KR(ret));
  } else if (OB_FAIL(local_allocator_.init(TOTAL_LIMIT, HOLD_LIMIT, PAGE_SIZE))) {
    CLOG_LOG(WARN, "fail to init local allocator", KR(ret));
  } else if (OB_FAIL(init_thread_pool_(TASK_NUM_LIMIT))) {
    CLOG_LOG(WARN, "fail to init thread pool in ObClogHistoryReporter", KR(ret));
  } else {
    task_list_.reset();
    partition_task_count_ = 0;
    sql_proxy_ = sql_proxy;
    addr_ = addr;

    // alloc sql_str for all thread
    for (int64_t i = 0; i < THREAD_NUM && OB_SUCC(ret); i++) {
      sql_str_[i] = static_cast<char*>(local_allocator_.alloc(common::OB_MAX_SQL_LENGTH));
      if (NULL == sql_str_[i]) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        CLOG_LOG(WARN, "fail to alloc sql_str", KR(ret), K(i));
      } else {
        // do nothing
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret) && !is_inited_) {
    destroy();
  }
  return ret;
}

void ObClogHistoryReporter::stop()
{
  CLOG_LOG(INFO, "ObClogHistoryReporter stop");
  queue_cond_.signal();
  TG_STOP(lib::TGDefIDs::ClogHisRep);
}

void ObClogHistoryReporter::wait()
{
  CLOG_LOG(INFO, "ObClogHistoryReporter wait");
  TG_WAIT(lib::TGDefIDs::ClogHisRep);
}

void ObClogHistoryReporter::destroy()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    int64_t queue_task_cnt = get_task_num_();
    int64_t map_task_cnt = task_map_.count();
    CLOG_LOG(WARN, "ObClogHistoryReporter discard task count", K(queue_task_cnt), K(map_task_cnt));

    // destroy thread pool firstly, then set is_inited_ to false to avoid task execution
    destroy_thread_pool_();
    // destroy tasks in task_map_
    DestoryTaskFunctor functor(*this);
    if (OB_FAIL(task_map_.for_each(functor))) {
      CLOG_LOG(WARN, "task_map_ destroy each partition task error", KR(ret));
    }
    task_map_.destroy();
    task_list_.destroy();
    partition_task_count_ = 0;
    sql_proxy_ = NULL;
    addr_.reset();
    for (int64_t i = 0; i < THREAD_NUM; i++) {
      if (OB_ISNULL(sql_str_[i])) {
        // do nothing
      } else {
        local_allocator_.free(sql_str_[i]);
        sql_str_[i] = NULL;
      }
    }
    local_allocator_.destroy();
    is_inited_ = false;
  }
  TG_DESTROY(lib::TGDefIDs::ClogHisRep);
}

ObClogHistoryReporter& ObClogHistoryReporter::get_instance()
{
  static ObClogHistoryReporter instance;
  return instance;
}

bool ObClogHistoryReporter::is_related_table(uint64_t tenant_id, uint64_t table_id)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    bool_ret = false;
  } else if (table_id == combine_id(tenant_id, OB_ALL_CLOG_HISTORY_INFO_V2_TID)) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

void ObClogHistoryReporter::online(
    const common::ObPartitionKey& pkey, const uint64_t start_log_id, const int64_t start_log_timestamp)
{
  int ret = OB_SUCCESS;
  int64_t end_log_id = OB_INVALID_ID;
  int64_t end_log_timestamp = OB_INVALID_TIMESTAMP;

  if (has_set_stop()) {
    CLOG_LOG(INFO,
        "ObClogHistoryReporter has stopped, discard online task",
        K(pkey),
        K(start_log_id),
        K(start_log_timestamp),
        K(end_log_id),
        K(end_log_timestamp));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == start_log_id) ||
             OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_log_timestamp)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(pkey), K(start_log_id), K(start_log_timestamp));
  } else if (OB_FAIL(insert_task_tail_(
                 pkey, ONLINE_OP, start_log_id, start_log_timestamp, end_log_id, end_log_timestamp))) {
    CLOG_LOG(WARN,
        "fail to push online op",
        KR(ret),
        K(pkey),
        K(start_log_id),
        K(start_log_timestamp),
        K(end_log_id),
        K(end_log_timestamp));
  } else {
    ISTAT("submit ONLINE op succ", K(pkey), K(start_log_id), K(start_log_timestamp));
  }

  if (OB_FAIL(ret)) {
    CLOG_LOG(ERROR,
        "online fail",
        KR(ret),
        K(pkey),
        K(start_log_id),
        K(start_log_timestamp),
        K(end_log_id),
        K(end_log_timestamp));
  }
}

void ObClogHistoryReporter::offline(const common::ObPartitionKey& pkey, const uint64_t start_log_id,
    const int64_t start_log_timestamp, const uint64_t end_log_id, const int64_t end_log_timestamp)
{
  int ret = OB_SUCCESS;

  if (has_set_stop()) {
    CLOG_LOG(INFO,
        "ObClogHistoryReporter has stopped, discard offline task",
        K(pkey),
        K(start_log_id),
        K(start_log_timestamp),
        K(end_log_id),
        K(end_log_timestamp));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!pkey.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == start_log_id) ||
             OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_log_timestamp) || OB_UNLIKELY(OB_INVALID_ID == end_log_id) ||
             OB_UNLIKELY(OB_INVALID_TIMESTAMP == end_log_timestamp)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        KR(ret),
        K(pkey),
        K(start_log_id),
        K(start_log_timestamp),
        K(end_log_id),
        K(end_log_timestamp));
  } else if (OB_FAIL(insert_task_tail_(
                 pkey, OFFLINE_OP, start_log_id, start_log_timestamp, end_log_id, end_log_timestamp))) {
    CLOG_LOG(WARN,
        "fail to push offline op",
        KR(ret),
        K(pkey),
        K(start_log_id),
        K(start_log_timestamp),
        K(end_log_id),
        K(end_log_timestamp));
  } else {
    ISTAT("submit OFFLINE op succ", K(pkey), K(start_log_id), K(start_log_timestamp));
  }

  if (OB_FAIL(ret)) {
    CLOG_LOG(ERROR,
        "offline fail",
        KR(ret),
        K(pkey),
        K(start_log_id),
        K(start_log_timestamp),
        K(end_log_id),
        K(end_log_timestamp));
  }
}

void ObClogHistoryReporter::delete_server_record(const common::ObAddr& delete_addr)
{
  int ret = OB_SUCCESS;
  IQueueTask* queue_task = NULL;
  DeleteQueueTask* delete_task = NULL;

  if (has_set_stop()) {
    CLOG_LOG(INFO, "ObClogHistoryReporter has stopped, discard delete server task", K(delete_addr));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!delete_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(delete_addr));
  } else if (OB_FAIL(alloc_queue_task_(DELETE_TASK, queue_task))) {
    CLOG_LOG(WARN, "alloc queue task fail", KR(ret), KPC(queue_task));
  } else if (OB_ISNULL(queue_task)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "queue task is null", KR(ret), KPC(queue_task));
  } else if (OB_ISNULL(delete_task = dynamic_cast<DeleteQueueTask*>(queue_task))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "delete task invalid", KR(ret), KPC(delete_task));
  } else {
    delete_task->reset(delete_addr);

    if (OB_FAIL(push_work_queue_(delete_task, PUSH_TASK_TIMEOUT))) {
      CLOG_LOG(WARN, "fail to push delete task", KR(ret), KPC(delete_task));
      if (OB_FAIL(free_queue_task_(queue_task))) {
        CLOG_LOG(WARN, "free queue task fail", KR(ret), KPC(queue_task));
      }
    } else {
      ISTAT("submit DELETE task succ", KPC(delete_task));
    }
    queue_task = NULL;
    delete_task = NULL;
  }

  if (OB_FAIL(ret)) {
    CLOG_LOG(ERROR, "delete fail", KR(ret), KPC(delete_task));
  }
}

void ObClogHistoryReporter::handle(void* task)
{
  int ret = OB_SUCCESS;

  IQueueTask* queue_task = static_cast<IQueueTask*>(task);
  QueueTaskType queue_task_type = UNKNOWN_TASK;
  // is_enqueue_again = true means that when the delete task execution fails, the push work queue op succeeds.
  bool is_enqueue_again = false;
  CLOG_DEBUG("ObClogHistoryReporter:handle", KPC(queue_task));

  if (OB_ISNULL(queue_task) || OB_UNLIKELY(UNKNOWN_TASK == (queue_task_type = queue_task->task_type_))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", KR(ret), KPC(queue_task), K(queue_task_type));
  } else {
    if (PARTITION_TASK == queue_task_type) {
      PartitionQueueTask* partition_task = NULL;
      if (OB_ISNULL(partition_task = dynamic_cast<PartitionQueueTask*>(queue_task))) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "partition task is NULL", KR(ret), KPC(partition_task));
      } else if (OB_FAIL(exec_partition_task_(partition_task))) {
        CLOG_LOG(WARN, "exec partition task fail", KR(ret), KPC(partition_task));
        // when task failed, it will be inserted into task_map_.
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = insert_task_head_(partition_task))) {
          CLOG_LOG(WARN, "insert task head fail", K(tmp_ret), KPC(partition_task));
        } else {
          CLOG_LOG(INFO, "exec partition task fail, insert task head succ", K(tmp_ret), KPC(partition_task));
        }
      } else {
        CLOG_LOG(INFO, "exec partition task succ", KR(ret), KPC(partition_task));
      }
      // whether task fails or not, partition_task_count_ will always be decreased by 1.
      ATOMIC_DEC(&partition_task_count_);
      partition_task = NULL;
    } else if (DELETE_TASK == queue_task_type) {
      DeleteQueueTask* delete_task = NULL;
      if (OB_ISNULL(delete_task = dynamic_cast<DeleteQueueTask*>(queue_task))) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "delete task is NULL", KR(ret), KPC(delete_task));
      } else if (OB_FAIL(exec_delete_task_(delete_task))) {
        CLOG_LOG(WARN, "exec delete task fail", KR(ret), KPC(delete_task));
        // when task failed, it will be inserted into task_map_.
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = push_work_queue_(delete_task, PUSH_TASK_TIMEOUT))) {
          CLOG_LOG(WARN, "push delete task fail", K(tmp_ret), KPC(delete_task));
        } else {
          is_enqueue_again = true;
          CLOG_LOG(INFO, "exec delete task fail, push delete task succ", K(tmp_ret), KPC(delete_task));
        }
      } else {
        CLOG_LOG(INFO, "exec delete task succ", KR(ret), KPC(delete_task));
      }
      delete_task = NULL;
    } else {
      // do nothing
    }
  }

  // Deleting task fails, push it into work queue again, and the memory will be released except (is_enqueue_again =
  // true)
  if (!is_enqueue_again) {
    if (OB_ISNULL(queue_task)) {
      CLOG_LOG(WARN, "queue task is null", KR(ret), KPC(queue_task));
    } else if (OB_FAIL(free_queue_task_(queue_task))) {
      CLOG_LOG(WARN, "free queue task fail", KR(ret), KPC(queue_task));
    } else {
      CLOG_LOG(INFO, "free queue task succ", KR(ret));
    }
    queue_task = NULL;
  }

  if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
    print_stat();
  }
}

int ObClogHistoryReporter::init_thread_pool_(const int64_t task_num_limit)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(task_num_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(queue_.init(task_num_limit))) {
    CLOG_LOG(WARN, "task queue init failed", KR(ret), K(task_num_limit));
  } else {
    thread_counter_ = 0;
    task_num_limit_ = task_num_limit;
    if (OB_FAIL(TG_SET_RUNNABLE_AND_START(TGDefIDs::ClogHisRep, *this))) {
      CLOG_LOG(WARN, "thread pool start fail", KR(ret));
    } else {
      CLOG_LOG(INFO, "thread pool start succ", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
    stop();
    wait();
  }

  return ret;
}

void ObClogHistoryReporter::destroy_thread_pool_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(free_work_queue_task_())) {
    CLOG_LOG(WARN, "thread pool free work queue task fail", KR(ret));
  } else {
    CLOG_LOG(INFO, "thread pool free work queue task succ", KR(ret));
  }

  task_num_limit_ = 0;
  thread_counter_ = 0;
  queue_.destroy();
}

int ObClogHistoryReporter::push_work_queue_(void* task, const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret));
  } else {
    int64_t end_time = timeout + ::oceanbase::common::ObTimeUtility::current_time();
    int64_t left_time = end_time;
    bool is_terminate = false;

    while (!is_terminate) {
      is_terminate = true;
      if (OB_FAIL(queue_.push(task))) {
        if (OB_SIZE_OVERFLOW == ret) {
          left_time = end_time - ::oceanbase::common::ObTimeUtility::current_time();
          if (left_time < 0) {
            ret = OB_TIMEOUT;
          } else {
            is_terminate = false;
            queue_cond_.timedwait(left_time);
          }
        } else {
          CLOG_LOG(WARN, "fail to push clog history task into work queue", KR(ret));
        }
      } else {
        // push success
      }
    }
  }

  if (OB_SUCC(ret)) {
    queue_cond_.signal();
  }

  return ret;
}

int64_t ObClogHistoryReporter::thread_index_()
{
  // thread_counter_ is used to get index for every thread
  static RLOCAL(int64_t, index);
  if (index == 0) {
    index = ATOMIC_AAF(&thread_counter_, 1);
  }
  return index - 1;
}

void ObClogHistoryReporter::run1()
{
  int ret = OB_SUCCESS;

  const int64_t thread_index = get_thread_idx();
  lib::set_thread_name("ClogHisRep", thread_index);
  while (!has_set_stop()) {

    if (0 == thread_index) {
      if (0 == (ATOMIC_LOAD(&partition_task_count_))) {
        CLOG_DEBUG("partition task finish", K(thread_index), K(partition_task_count_));
        if (OB_FAIL(push_task_())) {
          CLOG_LOG(WARN, "push task into queue fail", KR(ret), K(partition_task_count_));
        }
      }
    }

    void* task = NULL;
    if (OB_FAIL(queue_.pop(task))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        queue_cond_.timedwait(QUEUE_WAIT_TIME);
      } else {
        CLOG_LOG(WARN, "queue_ pop error", KR(ret));
      }
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "task is null", KR(ret), KP(task));
    } else {
      if (0 != thread_index) {
        queue_cond_.signal();
      }
      handle(task);
      CLOG_DEBUG("task is not NULL", K(thread_index));
    }
  }  // while
}

int ObClogHistoryReporter::free_work_queue_task_()
{
  int ret = OB_SUCCESS;

  void* task = NULL;
  while (OB_SUCC(queue_.pop(task))) {
    IQueueTask* queue_task = static_cast<IQueueTask*>(task);
    if (OB_ISNULL(queue_task)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "queue task is null", KR(ret), KPC(queue_task));
    } else if (OB_FAIL(free_queue_task_(queue_task))) {
      CLOG_LOG(WARN, "free queue task fail", KR(ret), KPC(queue_task));
    } else {
      // do nothing
    }
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

bool ObClogHistoryReporter::InsertTaskTailFunctor::operator()(
    const common::ObPartitionKey& pkey, PartitionQueueTask& task)
{
  UNUSED(pkey);

  // add online/offline op to the tail of task
  task.add_tail(&op_);
  err_ = OB_SUCCESS;

  return true;
}

bool ObClogHistoryReporter::InsertTaskHeadFunctor::operator()(
    const common::ObPartitionKey& pkey, PartitionQueueTask& task)
{
  UNUSED(pkey);

  // add task to the head of task_map_
  task.add_task_head(&task_);
  err_ = OB_SUCCESS;

  return true;
}

bool ObClogHistoryReporter::PushTaskFunctor::operator()(const common::ObPartitionKey& pkey, PartitionQueueTask& task)
{
  int ret = OB_SUCCESS;
  bool bool_ret = true;
  UNUSED(pkey);

  if (host_.task_list_.size() < QUEUE_TASK_NUM_LIMIT) {
    if (OB_FAIL(host_.task_list_.push_back(task))) {
      CLOG_LOG(WARN, "PushTaskFunctor array_ push task fail", KR(ret), K(pkey), K(task));
    } else {
      CLOG_DEBUG("PushTaskFunctor array_ push task succ", KR(ret), K(pkey), K(task));
    }
  } else {
    ret = OB_EAGAIN;
  }
  err_ = ret;

  if (OB_SUCC(ret)) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }

  return bool_ret;
}

bool ObClogHistoryReporter::DestoryTaskFunctor::operator()(const common::ObPartitionKey& pkey, PartitionQueueTask& task)
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO, "destroy task", K(pkey), K(task), K_(count));
  UNUSED(pkey);

  if (OB_FAIL(host_.free_partition_op_list_(&task))) {
    CLOG_LOG(WARN, "task_map_ free partition op list fail", KR(ret), K(task));
  } else {
    count_++;
  }

  return true;
}

int ObClogHistoryReporter::alloc_queue_task_(const QueueTaskType queue_task_type, IQueueTask*& queue_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret));
  } else if (OB_UNLIKELY(UNKNOWN_TASK == queue_task_type)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(queue_task_type));
  } else {
    queue_task = NULL;
    void* ptr = NULL;
    int64_t malloc_retry_cnt = 0;

    if (PARTITION_TASK == queue_task_type) {
      while (OB_ISNULL(ptr = local_allocator_.alloc(sizeof(PartitionQueueTask)))) {
        if (malloc_retry_cnt > TASK_MALLOC_CNT_LIMIT) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          CLOG_LOG(ERROR, "allocate queue task instance failed", KR(ret), K(malloc_retry_cnt));
          break;
        }
        ++malloc_retry_cnt;
        CLOG_LOG(WARN, "allocate queue task instance failed, try again", K(malloc_retry_cnt));
        usleep(MALLOC_RETRY_TIME_INTERVAL);
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(ptr)) {
        queue_task = new (ptr) PartitionQueueTask();
        if (OB_ISNULL(queue_task)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          CLOG_LOG(ERROR, "construct task instance failed", KR(ret));
        } else {
          CLOG_DEBUG("alloc partition queue task succ", KR(ret), KPC(queue_task));
        }
      }
    } else if (DELETE_TASK == queue_task_type) {
      while (OB_ISNULL(ptr = local_allocator_.alloc(sizeof(DeleteQueueTask)))) {
        if (malloc_retry_cnt > TASK_MALLOC_CNT_LIMIT) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          CLOG_LOG(ERROR, "allocate queue task instance failed", KR(ret), K(malloc_retry_cnt));
          break;
        }
        ++malloc_retry_cnt;
        CLOG_LOG(WARN, "allocate queue task instance failed, try again", K(malloc_retry_cnt));
        usleep(MALLOC_RETRY_TIME_INTERVAL);
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(ptr)) {
        queue_task = new (ptr) DeleteQueueTask();
        if (OB_ISNULL(queue_task)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          CLOG_LOG(ERROR, "construct delete task instance failed", KR(ret));
        } else {
          CLOG_DEBUG("alloc delete queue task succ", KR(ret), KPC(queue_task));
        }
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObClogHistoryReporter::free_queue_task_(IQueueTask* queue_task)
{
  int ret = OB_SUCCESS;
  QueueTaskType queue_task_type = queue_task->task_type_;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret));
  } else if (OB_UNLIKELY(UNKNOWN_TASK == queue_task_type) || OB_ISNULL(queue_task)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(queue_task_type), KPC(queue_task));
  } else {
    if (PARTITION_TASK == queue_task_type) {
      PartitionQueueTask* partition_task = NULL;
      if (OB_ISNULL(partition_task = dynamic_cast<PartitionQueueTask*>(queue_task))) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "PartitionQueueTask invalid", KR(ret), KPC(partition_task));
      } else {
        if (OB_FAIL(free_partition_op_list_(partition_task))) {
          CLOG_LOG(WARN, "free partition op list fail", KR(ret), KPC(partition_task));
        } else {
          partition_task->~PartitionQueueTask();
          local_allocator_.free(partition_task);
          partition_task = NULL;

          CLOG_DEBUG("free PartitionQueueTask succ", KR(ret));
        }
      }
    } else if (DELETE_TASK == queue_task_type) {
      queue_task->~IQueueTask();
      local_allocator_.free(queue_task);
      queue_task = NULL;

      CLOG_DEBUG("free DeleteQueueTask succ", KR(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObClogHistoryReporter::alloc_partition_op_(const PartitionOpType op_type, const uint64_t start_log_id,
    const int64_t start_log_timestamp, const uint64_t end_log_id, const int64_t end_log_timestamp, PartitionOp*& op)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret));
  } else if (OB_UNLIKELY(UNKNOWN_OP == op_type)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(op_type));
  } else {
    op = NULL;
    void* ptr = NULL;
    int64_t malloc_retry_cnt = 0;

    while (NULL == (ptr = local_allocator_.alloc(sizeof(PartitionOp)))) {
      if (malloc_retry_cnt > TASK_MALLOC_CNT_LIMIT) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        CLOG_LOG(ERROR, "allocate op instance failed", KR(ret), K(malloc_retry_cnt));
        break;
      }
      ++malloc_retry_cnt;
      CLOG_LOG(WARN, "allocate op instance failed, try again", K(malloc_retry_cnt));
      usleep(MALLOC_RETRY_TIME_INTERVAL);
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(ptr)) {
      op = new (ptr) PartitionOp();
      if (OB_ISNULL(op)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        CLOG_LOG(ERROR, "construct partition op failed", KR(ret));
      } else {
        op->reset(op_type, addr_, start_log_id, start_log_timestamp, end_log_id, end_log_timestamp);
        CLOG_DEBUG("alloc op succ", KR(ret), KPC(op));
      }
    }
  }

  return ret;
}

int ObClogHistoryReporter::free_partition_op_(PartitionOp* op)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(op)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), KPC(op));
  } else {
    op->~PartitionOp();
    local_allocator_.free(op);
    op = NULL;
  }

  return ret;
}

int ObClogHistoryReporter::free_partition_op_list_(PartitionQueueTask* partition_task)
{
  int ret = OB_SUCCESS;
  PartitionOp* op = NULL;

  if (NULL == (op = partition_task->pop())) {
    // do nothing
  } else {
    while (OB_NOT_NULL(op) && OB_SUCC(ret)) {
      if (OB_FAIL(free_partition_op_(op))) {
        CLOG_LOG(WARN, "free partition op fail", KR(ret), KPC(op));
      } else {
        op = partition_task->pop();
      }
    }
  }

  return ret;
}

int ObClogHistoryReporter::insert_task_tail_(const common::ObPartitionKey& pkey, const PartitionOpType op_type,
    const uint64_t start_log_id, const int64_t start_log_timestamp, const uint64_t end_log_id,
    const int64_t end_log_timestamp)
{
  int ret = OB_SUCCESS;
  PartitionOp* op = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret));
  } else if (OB_FAIL(
                 alloc_partition_op_(op_type, start_log_id, start_log_timestamp, end_log_id, end_log_timestamp, op))) {
    CLOG_LOG(WARN,
        "alloc op fail",
        KR(ret),
        K(pkey),
        K(op_type),
        K(start_log_id),
        K(start_log_timestamp),
        K(end_log_id),
        K(end_log_timestamp),
        KPC(op));
  } else if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "op is null", KR(ret), KPC(op));
  } else {
    InsertTaskTailFunctor functor(*op);
    int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
    int64_t end_time = now + INSERT_TASK_MAP_TIMEOUT;
    bool is_terminate = false;

    while (!is_terminate && (now < end_time)) {
      is_terminate = true;
      if (OB_FAIL(task_map_.operate(pkey, functor))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          PartitionQueueTask task;
          task.reset(pkey, op);
          if (OB_FAIL(task_map_.insert(pkey, task))) {
            if (OB_ENTRY_EXIST == ret) {
              // retry
            } else {
              CLOG_LOG(WARN, "task_map_ insert task fail", KR(ret), K(task));
            }
          } else {
            CLOG_DEBUG("task_map_ insert task succ", KR(ret), K(task));
          }
        } else {
          CLOG_LOG(WARN, "task_map_ apply InsertTaskTailFunctor fail", KR(ret), KPC(op));
        }
      } else {
        CLOG_DEBUG("task_map_ insert op succ", KR(ret), KPC(op));
      }

      if (OB_ENTRY_EXIST == ret) {
        is_terminate = false;
        now = ::oceanbase::common::ObTimeUtility::current_time();
      }
    }  // while
  }

  if (OB_SUCC(ret)) {
    ISTAT("task_map_ insert task tail succ", KR(ret), KPC(op));
  } else {
    CLOG_LOG(WARN, "task_map insert task tail fail", KR(ret), KPC(op));
    if (OB_FAIL(free_partition_op_(op))) {
      CLOG_LOG(WARN, "free partition op fail", KR(ret), KPC(op));
    }
    op = NULL;
  }

  return ret;
}

int ObClogHistoryReporter::insert_task_head_(PartitionQueueTask* partition_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret));
  } else if (OB_ISNULL(partition_task)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), KPC(partition_task));
  } else {
    InsertTaskHeadFunctor functor(*partition_task);
    int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
    int64_t end_time = now + INSERT_TASK_MAP_TIMEOUT;
    bool is_terminate = false;

    while (!is_terminate && (now < end_time)) {
      is_terminate = true;
      if (OB_FAIL(task_map_.operate(partition_task->pkey_, functor))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          PartitionQueueTask task;
          task.reset(partition_task);
          if (OB_FAIL(task_map_.insert(partition_task->pkey_, task))) {
            if (OB_ENTRY_EXIST == ret) {
              // retry
            } else {
              CLOG_LOG(WARN, "task_map_ insert task fail", KR(ret), K(task));
            }
          } else {
            CLOG_DEBUG("task_map_ insert task succ", KR(ret), K(task));
          }
        } else {
          CLOG_LOG(WARN, "task_map_ apply InsertTaskHeadFunctor fail", KR(ret), KPC(partition_task));
        }
      } else {
        CLOG_DEBUG("task_map_ insert task head succ", KR(ret), KPC(partition_task));
      }

      if (OB_ENTRY_EXIST == ret) {
        is_terminate = false;
        now = ::oceanbase::common::ObTimeUtility::current_time();
      }
    }  // while
  }

  if (OB_SUCC(ret)) {
    // reset partition_task, ensure that the memory of PartitionQueueTask is released subsequently, and the memory of op
    // will not be released
    partition_task->reset();
    ISTAT("task_map_ insert task head succ", KR(ret), KPC(partition_task));
  } else {
    CLOG_LOG(WARN, "task_map insert task head fail", KR(ret), KPC(partition_task));
  }

  return ret;
}

int ObClogHistoryReporter::push_task_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret));
  } else {
    PushTaskFunctor functor(*this);
    if (OB_FAIL(task_map_.remove_if(functor))) {
      CLOG_LOG(ERROR, "task_map_ remove if error", KR(ret));
    }
  }

  while (OB_SUCC(ret)) {
    PartitionQueueTask task;
    if (OB_FAIL(task_list_.pop_front(task))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // task list empty
      } else {
        CLOG_LOG(WARN, "task_list_ pop front error", KR(ret));
      }
    } else {
      IQueueTask* queue_task = NULL;
      PartitionQueueTask* partition_task = NULL;

      if (OB_FAIL(alloc_queue_task_(PARTITION_TASK, queue_task))) {
        CLOG_LOG(WARN, "alloc queue task fail", KR(ret), KPC(queue_task));
      } else if (OB_ISNULL(queue_task)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "queue task is null", KR(ret), KPC(queue_task));
      } else if (OB_ISNULL(partition_task = dynamic_cast<PartitionQueueTask*>(queue_task))) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "partition task invalid", KR(ret), KPC(partition_task));
      } else {
        partition_task->reset(&task);

        if (OB_FAIL(push_work_queue_(partition_task, PUSH_TASK_TIMEOUT))) {
          CLOG_LOG(WARN, "fail to push partition task", KR(ret), KPC(partition_task));
          // push work queue fail, return it to task_list_
          if (OB_FAIL(task_list_.push_front(task))) {
            CLOG_LOG(WARN, "task_list_ push front fail", KR(ret), KPC(queue_task));
          }
          // release memory
          partition_task->reset();
          if (OB_FAIL(free_queue_task_(queue_task))) {
            CLOG_LOG(WARN, "free queue task fail", KR(ret), KPC(queue_task));
          }
        } else {
          // push work queue succ, partition_task_count_ increased by 1
          ATOMIC_INC(&partition_task_count_);
          ISTAT("push partition task into work queue", KR(ret), K(partition_task_count_), KPC(partition_task));
        }
      }
      queue_task = NULL;
      partition_task = NULL;
    }
  }  // while

  // task_list_ is empty or work queue space is not enough
  if (OB_ENTRY_NOT_EXIST == ret || OB_TIMEOUT == ret) {
    ret = OB_SUCCESS;
  } else {
    CLOG_LOG(WARN, "push task error", KR(ret));
  }

  return ret;
}

int ObClogHistoryReporter::exec_delete_task_(DeleteQueueTask* task)
{
  int ret = OB_SUCCESS;

  int64_t thread_index = thread_index_();
  char svr_ip[MAX_IP_ADDR_LENGTH] = "\0";
  ObSchemaGetterGuard schema_guard;
  ObArray<uint64_t> tenant_ids;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), KPC(task));
  } else if (OB_UNLIKELY(!task->delete_addr_.ip_to_string(svr_ip, sizeof(svr_ip)))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "format ip str failed", KR(ret), K(task->delete_addr_));
  } else if (OB_ISNULL(sql_str_[thread_index])) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "sql_str_ is null", KR(ret), K(thread_index));
  } else if (OB_FAIL(
                 ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    CLOG_LOG(WARN, "get schema guard failed", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    CLOG_LOG(WARN, "get tenant ids failed", KR(ret));
  } else {
    // when server A is deleted, its all records in __all_clog_history_info_v2 will be deleted.
    uint64_t tenant_id = OB_INVALID_ID;
    for (int64_t i = 0; i < tenant_ids.count() && OB_SUCC(ret); i++) {
      tenant_id = tenant_ids.at(i);
      // following code is to be compatible with versions before 2.2, in which the table of sys tenant is partitioned,
      // and user tenants not.
      int32_t partition_counts = (OB_SYS_TENANT_ID == tenant_id ? CLOG_HISTORY_INFO_PARTITION_COUNT : 1);
      bool is_commit = false;
      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(sql_proxy_))) {
        CLOG_LOG(WARN, "start transaction failed", KR(ret));
      } else {
        bool tenant_has_been_dropped = false;
        for (int32_t partition_idx = 0; partition_idx < partition_counts && OB_SUCC(ret); partition_idx++) {
          is_commit = false;
          int64_t sql_len = 0;
          int64_t affected_rows = 0;
          memset(sql_str_[thread_index], 0, OB_MAX_SQL_LENGTH);

          if (OB_SYS_TENANT_ID == tenant_id) {
            sql_len = snprintf(sql_str_[thread_index],
                OB_MAX_SQL_LENGTH,
                "DELETE from oceanbase.%s "
                "partition(p%d) "
                "WHERE svr_ip=\'%s\' "
                "AND svr_port=%d",
                OB_ALL_CLOG_HISTORY_INFO_V2_TNAME,
                partition_idx,
                svr_ip,
                task->delete_addr_.get_port());
          } else {
            sql_len = snprintf(sql_str_[thread_index],
                OB_MAX_SQL_LENGTH,
                "DELETE from oceanbase.%s "
                "WHERE svr_ip=\'%s\' "
                "AND svr_port=%d",
                OB_ALL_CLOG_HISTORY_INFO_V2_TNAME,
                svr_ip,
                task->delete_addr_.get_port());
          }

          if (sql_len <= 0 || sql_len > OB_MAX_SQL_LENGTH) {
            ret = OB_SIZE_OVERFLOW;
            CLOG_LOG(WARN, "fail to format sql clause", KR(ret), K(is_commit));
          } else if (OB_FAIL(trans.write(tenant_id, sql_str_[thread_index], affected_rows))) {
            CLOG_LOG(WARN,
                "fail to delete a clog history info record",
                KR(ret),
                "sql",
                sql_str_[thread_index],
                KPC(task),
                K(tenant_id));
          } else {
            is_commit = true;
          }
          ISTAT("handle DELETE task",
              KR(ret),
              K(tenant_id),
              K(partition_idx),
              K(is_commit),
              K(affected_rows),
              KPC(task),
              "sql",
              sql_str_[thread_index]);
        }  // for

        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(is_commit))) {
          ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
          CLOG_LOG(WARN, "end trans failed", KR(ret), K(tmp_ret), KPC(task), K(is_commit), K(tenant_id));
        }

        // skip when the tenant is dropped.
        if (OB_FAIL(ret)) {
          tenant_has_been_dropped = verify_tenant_been_dropped_(tenant_id);
          if (tenant_has_been_dropped) {
            int old_ret = ret;
            ret = OB_SUCCESS;
            CLOG_LOG(INFO,
                "tenant has been dropped",
                K(tenant_id),
                KPC(task),
                "sql",
                sql_str_[thread_index],
                K(old_ret),
                KR(ret));
          }  // if (tenant_has_been_dropped)
        }
      }
    }  // foreach tenant
  }

  return ret;
}

int ObClogHistoryReporter::exec_partition_task_(PartitionQueueTask* task)
{
  int ret = OB_SUCCESS;
  PartitionOp* op = NULL;

  if (OB_ISNULL(op = task->pop())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), KP(op));
  } else {
    while (OB_NOT_NULL(op) && OB_SUCC(ret)) {
      if (OB_FAIL(exec_partition_op_(task->pkey_, op))) {
        CLOG_LOG(WARN,
            "exec sql task failed",
            KR(ret),
            "pkey",
            task->pkey_,
            "op_type",
            op->op_type_,
            "addr",
            op->addr_,
            "start_log_id",
            op->start_log_id_,
            "start_log_timestamp",
            op->start_log_timestamp_,
            "end_log_id",
            op->end_log_id_,
            "end_log_timestamp",
            op->end_log_timestamp_);
        // taks execution fails, return it back
        task->add_head(op);
      } else if (OB_FAIL(free_partition_op_(op))) {  // free memory after execute success
        CLOG_LOG(WARN, "free partition op fail", KR(ret), KPC(op));
      } else {
        // process next op
        op = task->pop();
      }
    }
  }

  return ret;
}

int ObClogHistoryReporter::exec_partition_op_(const common::ObPartitionKey& pkey, PartitionOp* op)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  bool is_commit = false;
  bool tenant_has_been_dropped = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), KP(op));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    CLOG_LOG(WARN, "start transaction failed", KR(ret));
  } else {
    if (ONLINE_OP == op->op_type_) {
      if (OB_FAIL(handle_online_op_(pkey, op, is_commit, trans))) {
        CLOG_LOG(WARN, "handle online sql failed", KR(ret), K(is_commit));
      }
    } else if (OFFLINE_OP == op->op_type_) {
      if (OB_FAIL(handle_offline_op_(pkey, op, is_commit, trans))) {
        CLOG_LOG(WARN, "handle offline sql failed", KR(ret), K(is_commit));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "Unexpected op type", KR(ret), K(op->op_type_));
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(is_commit))) {
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      CLOG_LOG(WARN, "end trans failed", KR(ret), K(tmp_ret), K(is_commit));
    }

    // 1. if ret != OB_SUCCESS, firstly check whether tenant has been dropped.
    // 2. other failure cases, check if need retry according to is_commit.
    if (OB_FAIL(ret)) {
      tenant_has_been_dropped = verify_tenant_been_dropped_(pkey.get_tenant_id());
    }
    if (tenant_has_been_dropped) {
      int old_ret = ret;
      ret = OB_SUCCESS;
      CLOG_LOG(INFO,
          "tenant has been dropped, task not need retry",
          K(pkey),
          "tenant_id",
          pkey.get_tenant_id(),
          K(tenant_has_been_dropped),
          K(old_ret),
          KR(ret));
    } else if (!is_commit && (OFFLINE_OP == op->op_type_ || ONLINE_OP == op->op_type_)) {
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObClogHistoryReporter::handle_online_op_(
    const common::ObPartitionKey& pkey, PartitionOp* online_op, bool& is_commit, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  int64_t thread_index = thread_index_();

  // when online, end_log_id and end_log_timestamp are invalid
  char svr_ip[MAX_IP_ADDR_LENGTH] = "\0";
  is_commit = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret));
  } else if (OB_ISNULL(online_op)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), KP(online_op));
  } else if (OB_UNLIKELY(!online_op->addr_.ip_to_string(svr_ip, sizeof(svr_ip)))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "format ip str failed", KR(ret), K(is_commit), K(online_op->addr_));
  } else if (OB_ISNULL(sql_str_[thread_index])) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "sql_str_ is null", KR(ret), K(thread_index));
  } else {
    int64_t sql_len = 0;
    int64_t affected_rows = 0;
    sql_len = snprintf(sql_str_[thread_index],
        OB_MAX_SQL_LENGTH,
        "REPLACE INTO oceanbase.%s "
        "(table_id, partition_idx, partition_cnt, "
        "start_log_id, start_log_timestamp, svr_ip, svr_port, "
        "end_log_id, end_log_timestamp) "
        "VALUES(%lu, %lu, %d, %lu, %ld, \'%s\', %d, %lu, %ld) ",
        OB_ALL_CLOG_HISTORY_INFO_V2_TNAME,
        pkey.get_table_id(),
        pkey.get_partition_id(),
        pkey.get_partition_cnt(),
        online_op->start_log_id_,
        online_op->start_log_timestamp_,
        svr_ip,
        online_op->addr_.get_port(),
        online_op->end_log_id_,
        online_op->end_log_timestamp_);

    if (sql_len <= 0 || sql_len > OB_MAX_SQL_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      CLOG_LOG(WARN, "fail to format sql clause", KR(ret), K(is_commit));
    } else if (OB_SUCCESS != (ret = trans.write(pkey.get_tenant_id(), sql_str_[thread_index], affected_rows))) {
      CLOG_LOG(WARN,
          "fail to insert a clog history info record",
          KR(ret),
          K(pkey),
          "sql",
          sql_str_[thread_index],
          KPC(online_op));
    } else {
      is_commit = true;
    }

    ISTAT("handle ONLINE op",
        KR(ret),
        K(pkey),
        K(is_commit),
        K(affected_rows),
        "task",
        *online_op,
        "sql",
        sql_str_[thread_index]);
  }
  return ret;
}

int ObClogHistoryReporter::handle_offline_op_(
    const common::ObPartitionKey& pkey, PartitionOp* offline_op, bool& is_commit, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  int64_t thread_index = thread_index_();

  char svr_ip[MAX_IP_ADDR_LENGTH] = "\0";
  is_commit = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObClogHistoryReporter not init", KR(ret));
  } else if (OB_ISNULL(offline_op)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), KP(offline_op));
  } else if (OB_UNLIKELY(!offline_op->addr_.ip_to_string(svr_ip, sizeof(svr_ip)))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "format ip str failed", KR(ret), K(is_commit), K(offline_op->addr_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == offline_op->start_log_id_) ||
             OB_UNLIKELY(OB_INVALID_ID == offline_op->end_log_id_)) {
    CLOG_LOG(WARN,
        "invalid offline task, start_log_id/end_log_id is invalid",
        "start_log_id",
        offline_op->start_log_id_,
        "end_log_id",
        offline_op->end_log_id_,
        "task",
        *offline_op);
  } else if (OB_ISNULL(sql_str_[thread_index])) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "sql_str_ is null", KR(ret), K(thread_index));
  } else {
    int64_t sql_len = 0;
    int64_t affected_rows = 0;

    // Traverse all the records with start_log_id less than sql_task->end_log_id_
    // If the record's end_log_id is infinite, then its end_log_id will be overwritten as: sql_task->end_log_id_
    sql_len = snprintf(sql_str_[thread_index],
        OB_MAX_SQL_LENGTH,
        "UPDATE oceanbase.%s "
        "SET end_log_id=%lu, "
        "end_log_timestamp=%ld "
        "WHERE table_id=%lu "
        "AND partition_idx=%ld "
        "AND partition_cnt=%d "
        "AND svr_ip=\'%s\' "
        "AND svr_port=%d "
        "AND start_log_id<%lu "
        "AND end_log_id=%lu",
        OB_ALL_CLOG_HISTORY_INFO_V2_TNAME,
        offline_op->end_log_id_,
        offline_op->end_log_timestamp_,
        pkey.get_table_id(),
        pkey.get_partition_id(),
        pkey.get_partition_cnt(),
        svr_ip,
        offline_op->addr_.get_port(),
        offline_op->end_log_id_,
        OB_INVALID_ID);

    if (sql_len <= 0 || sql_len > OB_MAX_SQL_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      CLOG_LOG(WARN, "fail to format sql cause", KR(ret), K(is_commit));
    } else if (OB_FAIL(trans.write(pkey.get_tenant_id(), sql_str_[thread_index], affected_rows))) {
      CLOG_LOG(WARN, "fail to update table clog_history_info", KR(ret), K(pkey), K(sql_str_[thread_index]));
    } else {
      is_commit = true;
    }

    ISTAT("handle OFFLINE op",
        KR(ret),
        K(pkey),
        K(is_commit),
        K(affected_rows),
        KPC(offline_op),
        "sql",
        sql_str_[thread_index]);
  }
  return ret;
}

void ObClogHistoryReporter::print_stat()
{
  _ISTAT("[STAT] QUEUED_TASK_COUNT=%ld", get_task_num_());
}

// After the tenant is splitted, when the task fails, first check whether tenant has been dropped.
// If tenant exists, trigger retry.
bool ObClogHistoryReporter::verify_tenant_been_dropped_(const uint64_t tenant_id)
{
  bool tenant_has_been_dropped = false;
  int ret = OB_SUCCESS;
  share::schema::ObMultiVersionSchemaService* schema_service = GCTX.schema_service_;
  share::schema::ObSchemaGetterGuard guard;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "schema_service is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    CLOG_LOG(WARN, "fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_if_tenant_has_been_dropped(tenant_id, tenant_has_been_dropped))) {
    CLOG_LOG(WARN, "fail to check if tenant has been dropped", KR(ret), K(tenant_id));
  } else {
    CLOG_LOG(INFO, "check if tenant has been dropped succ", K(tenant_id), K(tenant_has_been_dropped), KR(ret));
  }

  return tenant_has_been_dropped;
}

}  // namespace clog
}  // namespace oceanbase
