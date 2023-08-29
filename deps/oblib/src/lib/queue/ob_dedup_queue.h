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

#ifndef OCEANBASE_COMMON_DEDUP_QUEUE_
#define OCEANBASE_COMMON_DEDUP_QUEUE_
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/ob_define.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/utility/utility.h"
#include "lib/thread/thread_pool.h"

namespace oceanbase
{
namespace common
{
enum ObDedupTaskType
{
  T_BLOOMFILTER = 0,
  T_SCHEMA = 1,
  T_BF_WARMUP = 2,
  T_PT_MAINTENANCE = 3, // partition table maintenance
  T_PL_UPDATE = 4, // partition location update
  T_PT_CHECK = 5,
  T_PT_MERGE = 6,
  T_PL_FETCH = 7, // obproxy partition location fetch
  T_PT_MIGRATE = 8,
  T_PT_LOCAL_INDEX_BUILD = 9,
  T_CONN_ID_FETCH = 10, // obproxy conn id fetch
  T_VIP_TENANT_FETCH = 11,  // obproxy vip--->tenant fetch
  T_CLUSTER_RESOURCE_INIT = 12, // obproxy cluster resource init
  T_SS_FETCH = 13, // obproxy server state fetch
  T_RS_ET_UPDATE = 14, // rootservice event history table update
  T_SYS_VAR_FETCH = 15, // obproxy renew system variable
  T_PT_FREEZE = 16,
  T_ELECTION_ET_UPDATE = 17,
  T_WARM_UP_TASK = 18,
  T_MAIN_ST_MERGE = 19,
  T_INDEX_ST_MERGE = 20,
  T_MAIN_MB_MERGE = 21,
  T_INDEX_MB_MERGE = 22,
  T_REFRESH_LOCALITY = 23,
  T_TRANSFER_COPY_SSTORE = 24,
  T_DANGLING_REPLICA_CHECK = 25,
  T_PL_LEADER_UPDATE = 26,  //partition location leader update
  T_REFRESH_OPT_STAT = 27,
  T_SCHEMA_RELEASE = 28,
  T_BLOOMFILTER_LOAD = 29,
  T_SCHEMA_ASYNC_REFRESH = 30,
  T_CHECK_PG_RECOVERY_FINISHED = 31,
  T_UPDATE_FILE_RECOVERY_STATUS = 32,
  T_UPDATE_FILE_RECOVERY_STATUS_V2 = 33,
};

class ObDedupQueue;
class IObDedupTask
{
  friend class ObDedupQueue;
public:
  explicit IObDedupTask(const int type) : type_(type),
                                          stat_(ST_WAITING),
                                          sync_(common::ObLatchIds::DEDUP_QUEUE_LOCK),
                                          memory_(NULL),
                                          prev_(NULL),
                                          next_(NULL)
  {
  }
  virtual ~IObDedupTask() {}
public:
  virtual int64_t hash() const = 0;
  virtual int hash(uint64_t &hash_val) const{ hash_val = hash(); return OB_SUCCESS; }
  virtual bool operator ==(const IObDedupTask &other) const = 0;
  virtual int64_t get_deep_copy_size() const = 0;
  virtual IObDedupTask *deep_copy(char *buffer, const int64_t buf_size) const = 0;
  virtual int64_t get_abs_expired_time() const = 0;
  virtual int process() = 0;
  inline int get_type() const {return type_;}
private:
  static const int ST_WAITING = 0;
  static const int ST_DONE = 1;
private:
  void set_prev(IObDedupTask *prev) {prev_ = prev;}
  void set_next(IObDedupTask *next) {next_ = next;}
  IObDedupTask *get_prev() const {return prev_;}
  IObDedupTask *get_next() const {return next_;}
  bool is_process_done() const {return ST_DONE == stat_;}
  void set_process_done() {stat_ = ST_DONE;}
  void lock() {sync_.lock();}
  int trylock() {return sync_.trylock();}
  void unlock() {sync_.unlock();}
  char *get_memory_ptr() const {return memory_;}
  void set_memory_ptr(char *memory) {memory_ = memory;}
private:
  const int type_;
  int stat_;
  ObSpinLock sync_;
  char *memory_;
  IObDedupTask *prev_;
  IObDedupTask *next_;
private:
  DISALLOW_COPY_AND_ASSIGN(IObDedupTask);
};

template <class T, class Host>
class AllocatorWrapper
{
public:
  AllocatorWrapper() : allocator_(NULL) {}
  explicit AllocatorWrapper(Host &allocator) : allocator_(&allocator) {}
  ~AllocatorWrapper() {}
public:
  T *alloc()
  {
    T *ret = nullptr;
    void *ptr = nullptr;
    if (OB_NOT_NULL(allocator_) && OB_NOT_NULL(ptr = allocator_->alloc(sizeof(T)))) {
      ret = new (ptr) T();
    }
    return ret;
  }
  void free(T *ptr)
  {
    if (NULL != ptr
        && NULL != allocator_) {
      ptr->~T();
      allocator_->free(ptr);
      ptr = NULL;
    }
  }
  void set_attr(const ObMemAttr &attr) { UNUSED(attr); }
  void inc_ref() {}
  void dec_ref() {}
  void clear() {}
private:
  Host *allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(AllocatorWrapper);
};

class ObDedupQueue : public lib::ThreadPool
{
public:
  static const int64_t TOTAL_LIMIT = 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 512L * 1024L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_MIDDLE_BLOCK_SIZE;
  static const int64_t TASK_MAP_SIZE = 20L * 1000;
  static const int64_t TASK_QUEUE_SIZE = 20L * 1000;
public:
  ObDedupQueue();
  virtual ~ObDedupQueue();
public:
  int init(const int64_t thread_num = DEFAULT_THREAD_NUM,
           const char* thread_name = nullptr,
           const int64_t queue_size = TASK_QUEUE_SIZE,
           const int64_t task_map_size = TASK_MAP_SIZE,
           const int64_t total_mem_limit = TOTAL_LIMIT,
           const int64_t hold_mem_limit = HOLD_LIMIT,
           const int64_t page_size = PAGE_SIZE,
           const uint64_t tenant_id = OB_SERVER_TENANT_ID,
           const lib::ObLabel &label = "DedupQueue");
  void destroy();
public:
  int add_task(const IObDedupTask &task);
  int64_t task_count() const { return task_queue_.get_total(); }
  void set_label(const lib::ObLabel &label) { allocator_.set_label(label); }
  void set_attr(const lib::ObMemAttr &attr) { allocator_.set_attr(attr); }
  int set_thread_dead_threshold(const int64_t thread_dead_threshold);
public:
  void run1() override;
private:
  typedef ObFixedQueue<IObDedupTask> TaskQueue;
  typedef AllocatorWrapper<hash::HashMapTypes<const IObDedupTask *, IObDedupTask *>::AllocType, ObConcurrentFIFOAllocator>
  HashAllocator;
  typedef hash::ObHashMap<const IObDedupTask *,
                          IObDedupTask *,
                          hash::LatchReadWriteDefendMode,
                          hash::hash_func<const IObDedupTask *>,
                          hash::equal_to<const IObDedupTask *>,
                          HashAllocator,
                          common::hash::NormalPointer,
                          common::ObWrapperAllocator> TaskMap;
  typedef hash::HashMapTypes<const IObDedupTask *, IObDedupTask *>::pair_type TaskMapKVPair;
  static const int32_t DEFAULT_THREAD_NUM = 4;
  static const int32_t MAX_THREAD_NUM = 64;
  static const int32_t QUEUE_WAIT_TIME_MS = 50; //50ms
  static const int32_t MAX_QUEUE_WAIT_TIME_MS = 500; //500ms
  static const int64_t GC_BATCH_NUM = 512;
  static const int64_t DEFALT_THREAD_DEAD_THRESHOLD = 30000000L; //30s
  static const int64_t THREAD_CHECK_INTERVAL = 10000000L; //10s

  struct MapFunction
  {
    ObDedupQueue &host_;
    const IObDedupTask &task_;
    int result_code_;
    MapFunction(ObDedupQueue &host, const IObDedupTask &task) :
    host_(host), task_(task), result_code_(OB_SUCCESS) {};
    void operator()(TaskMapKVPair &kvpair)
    {
      result_code_ = host_.map_callback_(task_, kvpair);
    }
  };
  enum
  {
    TH_IDEL = 0,
    TH_RUN = 1,
    TH_GC = 2,
  };
  struct ThreadMeta
  {
  public:
    int stat_;
    int task_type_;
    const IObDedupTask *running_task_;
    int64_t busy_start_time_;
    pthread_t pthread_id_;
    int64_t tid_;

    ThreadMeta()
      : stat_(TH_IDEL),
        task_type_(-1),
        running_task_(NULL),
        busy_start_time_(0),
        pthread_id_(),
        tid_(0)
    {
    }

    void init()
    {
      stat_ = TH_IDEL;
      task_type_ = -1;
      running_task_ = NULL;
      busy_start_time_ = 0;
      pthread_id_ = pthread_self();
      tid_ = GETTID();
    }
    void on_process_start(const IObDedupTask *task)
    {
      busy_start_time_ = ::oceanbase::common::ObTimeUtility::current_time();
      stat_ = TH_RUN;
      if (NULL != task) {
        task_type_ = task->get_type();
      }
      running_task_ = task;
    };
    void on_process_end()
    {
      stat_ = TH_IDEL;
    };
    void on_gc_start()
    {
      busy_start_time_ = ::oceanbase::common::ObTimeUtility::current_time();
      stat_ = TH_GC;
    };
    void on_gc_end()
    {
      stat_ = TH_IDEL;
    };
    bool check_dead(const int64_t thread_dead_threshold) const
    {
      bool bret = false;
      if (TH_IDEL != stat_
          && (busy_start_time_ + thread_dead_threshold)
          < ::oceanbase::common::ObTimeUtility::current_time()) {
        bret = true;
      }
      return bret;
    };
    int64_t to_string(char *buffer, const int64_t length) const
    {
      int64_t pos = 0;
      databuff_printf(buffer, length, pos,
                      "stat=%d task_type=%d running_task=%p busy_start_time=%ld pthread_id=%ld tid=%ld",
                      stat_, task_type_, running_task_, busy_start_time_, pthread_id_, tid_);
      return pos;
    };
  };
private:
  int map_callback_(const IObDedupTask &task, TaskMapKVPair &kvpair);
  int add_task_(const IObDedupTask &task);
  IObDedupTask *copy_task_(const IObDedupTask &task);
  void destroy_task_(IObDedupTask *task);
  bool gc_();
private:
  bool is_inited_;
  ThreadMeta thread_metas_[MAX_THREAD_NUM];
  int64_t thread_num_;
  int64_t work_thread_num_;
  int64_t thread_dead_threshold_;
  ObConcurrentFIFOAllocator allocator_;
  HashAllocator hash_allocator_;
  common::ObWrapperAllocator bucket_allocator_;
  TaskMap task_map_;
  TaskQueue task_queue_;
  ObThreadCond task_queue_sync_;
  IObDedupTask *gc_queue_head_;
  IObDedupTask *gc_queue_tail_;
  lib::ObMutex gc_queue_sync_;
  ObThreadCond work_thread_sync_;
  const char* thread_name_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDedupQueue);
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_DEDUP_QUEUE_
