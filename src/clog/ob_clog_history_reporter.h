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

#ifndef OCEANBASE_CLOG_OB_CLOG_HISTORY_REPORTER_
#define OCEANBASE_CLOG_OB_CLOG_HISTORY_REPORTER_

#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/net/ob_addr.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/list/ob_list.h"
#include "lib/thread/thread_mgr_interface.h"
#include "common/ob_partition_key.h"
#include "common/ob_queue_thread.h"

namespace oceanbase {
namespace common {
class ObMySQLTransaction;
class ObMySQLProxy;
}  // namespace common
namespace clog {
class ObClogHistoryReporter : public lib::TGRunnable {
public:
  static const int64_t THREAD_NUM = 3;
  static const int64_t MINI_MODE_THREAD_NUM = 1;

private:
  // ObConcurrentFIFOAllocator
  static const int64_t TOTAL_LIMIT = 512L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 512L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t TASK_MALLOC_CNT_LIMIT = 60 * 1024;       // About 10 minutes
  static const int64_t MALLOC_RETRY_TIME_INTERVAL = 10 * 1000;  // 10ms
  static const int64_t PRINT_INTERVAL = 10L * 1000L * 1000L;
  // thread pool
  static const int64_t TASK_NUM_LIMIT = 1024 * 1024;
  static const int64_t QUEUE_WAIT_TIME = 100 * 1000;
  static const int64_t MAX_THREAD_NUM = 256;
  // timeout
  static const int64_t INSERT_TASK_MAP_TIMEOUT = 10L * 1000L * 1000L;  // 10s
  static const int64_t PUSH_TASK_TIMEOUT = 1000;                       // 1ms
  // __all_clog_history_info_V2-parition num
  static const int32_t CLOG_HISTORY_INFO_PARTITION_COUNT = 16;
  // PartitionQueueTask array max number
  static const int64_t QUEUE_TASK_NUM_LIMIT = 1024;

public:
  ObClogHistoryReporter()
      : task_num_limit_(0),
        thread_counter_(0),
        queue_cond_(),
        queue_(),
        task_map_(),
        partition_task_count_(0),
        local_allocator_(),
        task_list_(local_allocator_),
        sql_proxy_(NULL),
        addr_(),
        is_inited_(false)
  {
    (void)memset(thread_conf_array_, 0, sizeof(thread_conf_array_));
    (void)memset(sql_str_, 0, sizeof(sql_str_));
  }
  virtual ~ObClogHistoryReporter()
  {
    destroy();
  }

public:
  int init(common::ObMySQLProxy* sql_proxy, const common::ObAddr& addr);
  void stop();
  void wait();
  void destroy();

public:
  static ObClogHistoryReporter& get_instance();
  static bool is_related_table(uint64_t tenant_id, uint64_t table_id);
  void online(const common::ObPartitionKey& pkey, const uint64_t start_log_id, const int64_t start_log_timestamp);
  void offline(const common::ObPartitionKey& pkey, const uint64_t start_log_id, const int64_t start_log_timestamp,
      const uint64_t end_log_id, const int64_t end_log_timestamp);
  void delete_server_record(const common::ObAddr& delete_addr);
  virtual void handle(void* task);

private:
  int init_thread_pool_(const int64_t task_num_limit);
  void destroy_thread_pool_();
  int push_work_queue_(void* task, const int64_t timeout);
  int64_t get_task_num_() const
  {
    return queue_.get_total();
  }

  static void* thread_func_(void* data);
  int64_t thread_index_();
  int free_work_queue_task_();

  void run1() final;

private:
  enum QueueTaskType {
    UNKNOWN_TASK,
    PARTITION_TASK,  // PartitionQueueTask
    DELETE_TASK      // DeleteQueueTask
  };

  // There are two task types in worker queue:
  // 1. PartitionQueueTask (partition online or offline)
  // 2. delete task(when a server is deleted, its all records in __all_clog_history_info_v2 should be deleted)
  struct IQueueTask {
    IQueueTask()
    {
      reset(UNKNOWN_TASK);
    }
    virtual ~IQueueTask()
    {
      reset(UNKNOWN_TASK);
    }

    void reset(const QueueTaskType type)
    {
      task_type_ = type;
    }

    static const char* print_type(const QueueTaskType type)
    {
      const char* str = "INVALID";
      switch (type) {
        case PARTITION_TASK:
          str = "PARTITION_TASK";
          break;
        case DELETE_TASK:
          str = "DELETE_TASK";
          break;
        case UNKNOWN_TASK:
          str = "UNKNOWN_TASK";
          break;
        default:
          str = "INVALID";
          break;
      }

      return str;
    }

    QueueTaskType task_type_;

    TO_STRING_KV("QueueTaskType", print_type(task_type_));
  };

  // partition operation type, including online, offline
  enum PartitionOpType { UNKNOWN_OP, ONLINE_OP, OFFLINE_OP };

  // PartitionOp: online and offline
  struct PartitionOp {
    PartitionOp()
    {
      reset();
    }
    ~PartitionOp()
    {
      reset();
    }

    void reset()
    {
      op_type_ = UNKNOWN_OP;
      addr_.reset();
      start_log_id_ = common::OB_INVALID_ID;
      start_log_timestamp_ = common::OB_INVALID_TIMESTAMP;
      end_log_id_ = common::OB_INVALID_ID;
      end_log_timestamp_ = common::OB_INVALID_TIMESTAMP;
      next_ = NULL;
    }

    void reset(const PartitionOpType op_type, const common::ObAddr& addr, const uint64_t start_log_id,
        const int64_t start_log_timestamp, const uint64_t end_log_id, const int64_t end_log_timestamp)
    {
      reset();
      op_type_ = op_type;
      addr_ = addr;
      start_log_id_ = start_log_id;
      ;
      start_log_timestamp_ = start_log_timestamp;
      end_log_id_ = end_log_id;
      end_log_timestamp_ = end_log_timestamp;
    }

    PartitionOpType op_type_;
    common::ObAddr addr_;
    uint64_t start_log_id_;
    int64_t start_log_timestamp_;
    uint64_t end_log_id_;
    int64_t end_log_timestamp_;
    PartitionOp* next_;

    const char* print_partition_op(const int op) const
    {
      const char* str = "INVALID";

      switch (op) {
        case ONLINE_OP:
          str = "ONLINE";
          break;
        case OFFLINE_OP:
          str = "OFFLINE";
          break;
        case UNKNOWN_OP:
          str = "UNKNOWN";
          break;
        default:
          str = "INVALID";
          break;
      }
      return str;
    }
    TO_STRING_KV("partition op str", print_partition_op(op_type_), "svr", addr_, "start_log_id", start_log_id_,
        "start_log_timestamp_", start_log_timestamp_, "end_log_id", end_log_id_, "end_log_timestamp",
        end_log_timestamp_, "next", next_);
  };

  typedef PartitionOp TaskHead;
  struct PartitionQueueTask : public IQueueTask {
    PartitionQueueTask()
    {
      reset();
    }
    ~PartitionQueueTask()
    {
      reset();
    }

    void reset()
    {
      IQueueTask::reset(PARTITION_TASK);
      pkey_.reset();
      head_ = NULL;
      tail_ = NULL;
    }

    void reset(const common::ObPartitionKey& pkey, TaskHead* task)
    {
      reset();
      pkey_ = pkey;
      add_tail(task);
    }

    void reset(PartitionQueueTask* task)
    {
      reset();
      pkey_ = task->pkey_;
      head_ = task->head_;
      tail_ = task->tail_;
    }

    // When the execution of the PartitionQueueTask task fails, re-insert it into the head of task_map to ensure order.
    void add_task_head(PartitionQueueTask* task)
    {
      if (OB_ISNULL(task)) {
        // do nothing
      } else {
        if (OB_ISNULL(head_)) {
          head_ = task->head_;
          tail_ = task->tail_;
        } else {
          if (OB_ISNULL(task->head_)) {
            // do nothing
          } else {
            task->tail_->next_ = head_;
            head_ = task->head_;
          }
        }
      }
    }

    // insert op to the head of task_map
    void add_head(TaskHead* op)
    {
      if (OB_ISNULL(op)) {
        // do nothing
      } else {
        if (NULL == head_) {
          head_ = op;
          tail_ = op;
        } else {
          op->next_ = head_;
          head_ = op;
        }
      }
    }

    // insert op to the tail of task_map
    void add_tail(TaskHead* op)
    {
      if (OB_ISNULL(op)) {
        // do nothing
      } else {
        if (NULL == head_) {
          head_ = op;
          tail_ = op;
        } else {
          tail_->next_ = op;
          tail_ = op;
        }
      }
    }

    TaskHead* pop()
    {
      TaskHead* op = NULL;
      if (OB_ISNULL(head_)) {
        // do nothing
      } else {
        op = head_;
        head_ = head_->next_;
        if (OB_ISNULL(head_)) {
          tail_ = NULL;
        }
      }

      return op;
    }

    common::ObPartitionKey pkey_;
    TaskHead* head_;
    TaskHead* tail_;

    TO_STRING_KV(K_(pkey), KPC_(head), KPC_(tail));
  };

  // whne delete server-A, its all records in __all_clog_history_reporter_info_v2 need be deleted.
  struct DeleteQueueTask : public IQueueTask {
    DeleteQueueTask()
    {
      reset();
    }
    ~DeleteQueueTask()
    {
      reset();
    }

    void reset()
    {
      IQueueTask::reset(DELETE_TASK);
      delete_addr_.reset();
    }

    void reset(const common::ObAddr& addr)
    {
      reset();
      delete_addr_ = addr;
    }

    common::ObAddr delete_addr_;

    TO_STRING_KV(K_(delete_addr));
  };

private:
  // If the task_map_ has corresponding partition's task,online_op/offline_op
  // will be inserted to the tail of the task.
  class InsertTaskTailFunctor {
  public:
    explicit InsertTaskTailFunctor(PartitionOp& op) : err_(common::OB_SUCCESS), op_(op)
    {}
    bool operator()(const common::ObPartitionKey& pkey, PartitionQueueTask& task);
    int get_err()
    {
      return err_;
    }
    TO_STRING_KV(K_(err), K_(op));

  private:
    int err_;
    PartitionOp& op_;
  };
  // If task fails, and the task_map_ has corresponding partition's other task, it will be inserted to the head of the
  // task.
  class InsertTaskHeadFunctor {
  public:
    explicit InsertTaskHeadFunctor(PartitionQueueTask& task) : err_(common::OB_SUCCESS), task_(task)
    {}
    bool operator()(const common::ObPartitionKey& pkey, PartitionQueueTask& task);
    int get_err()
    {
      return err_;
    }
    TO_STRING_KV(K_(err), K_(task));

  private:
    int err_;
    PartitionQueueTask& task_;
  };
  // for hashmap remove_if, push task into the queue of the thread pool
  class PushTaskFunctor {
  public:
    explicit PushTaskFunctor(ObClogHistoryReporter& host) : err_(common::OB_SUCCESS), host_(host)
    {}
    // return true: task_map_ will delete current item immediately, return false: task_map_ will go on traversing.
    bool operator()(const common::ObPartitionKey& pkey, PartitionQueueTask& task);
    int get_err()
    {
      return err_;
    }
    TO_STRING_KV(K(err_));

  private:
    int err_;
    ObClogHistoryReporter& host_;
  };
  // for hashmap destory
  class DestoryTaskFunctor {
  public:
    explicit DestoryTaskFunctor(ObClogHistoryReporter& host) : count_(0), host_(host)
    {}
    bool operator()(const common::ObPartitionKey& pkey, PartitionQueueTask& task);

  private:
    int count_;
    ObClogHistoryReporter& host_;
  };

private:
  int alloc_queue_task_(const QueueTaskType queue_task_type, IQueueTask*& queue_task);
  int free_queue_task_(IQueueTask* queue_task);

  // alloc memory according to operation type
  int alloc_partition_op_(const PartitionOpType op_type, const uint64_t start_log_id, const int64_t start_log_timestamp,
      const uint64_t end_log_id, const int64_t end_log_timestamp, PartitionOp*& op);
  int free_partition_op_(PartitionOp* op);
  // free memory for partition task
  int free_partition_op_list_(PartitionQueueTask* op);

  // insert online_op/offline_op into task_map_,there are two cases:
  // 1. If task_map_ has corresponding partition task, online_op/offline_op will be inserted to the tail of the existing
  // task.
  // 2. If task_map_ does not have corresponding partition task, new task will be created and inserted.
  int insert_task_tail_(const common::ObPartitionKey& pkey, const PartitionOpType op_type, const uint64_t start_log_id,
      const int64_t start_log_timestamp, const uint64_t end_log_id, const int64_t end_log_timestamp);

  // insert PartitionQueueTask into task_map_, there are two cases:
  // 1. If task_map_ has corresponding partition task, new task will be inserted to the head of the existing task.
  // 2. If task_map_ does not have corresponding partition task, new task will be directly inserted.
  int insert_task_head_(PartitionQueueTask* task);

  // insert PartitionQueueTask of hashmap into worker queue
  int push_task_();

  int exec_delete_task_(DeleteQueueTask* task);
  int exec_partition_task_(PartitionQueueTask* task);
  int exec_partition_op_(const common::ObPartitionKey& pkey, PartitionOp* op);
  int handle_online_op_(
      const common::ObPartitionKey& pkey, PartitionOp* online_op, bool& is_commit, common::ObMySQLTransaction& trans);
  int handle_offline_op_(
      const common::ObPartitionKey& pkey, PartitionOp* offline_op, bool& is_commit, common::ObMySQLTransaction& trans);
  bool verify_tenant_been_dropped_(const uint64_t tenant_id);

  void print_stat();

private:
  // thread pool
  int64_t task_num_limit_;
  int64_t thread_counter_;

  common::ObCond queue_cond_;
  common::ObFixedQueue<void> queue_;
  struct ThreadConf {
    pthread_t pd;
    ObClogHistoryReporter* host;
    ThreadConf() : pd(0), host(NULL)
    {}
  };
  ThreadConf thread_conf_array_[MAX_THREAD_NUM];

private:
  typedef common::ObList<PartitionQueueTask, common::ObConcurrentFIFOAllocator> TaskList;

private:
  common::ObLinearHashMap<common::ObPartitionKey, PartitionQueueTask> task_map_;
  volatile int64_t partition_task_count_ CACHE_ALIGNED;
  common::ObConcurrentFIFOAllocator local_allocator_;
  TaskList task_list_;
  common::ObMySQLProxy* sql_proxy_;
  common::ObAddr addr_;
  char* sql_str_[MAX_THREAD_NUM];
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObClogHistoryReporter);
};

}  // namespace clog
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_CLOG_HISTORY_REPORTER_
