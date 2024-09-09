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

#ifndef SRC_LIBRARY_SRC_LIB_THREAD_OB_DYNAMIC_THREAD_POOL_H_
#define SRC_LIBRARY_SRC_LIB_THREAD_OB_DYNAMIC_THREAD_POOL_H_

#include "lib/utility/utility.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/thread/thread_pool.h"
#include "lib/container/ob_se_array.h"
#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase
{
namespace common
{
class ObDynamicThreadPool;

struct ObDynamicThreadInfo
{
  ObDynamicThreadInfo();
  void *tid_;
  int64_t idx_;
  ObDynamicThreadPool *pool_;
  bool is_stop_;
  bool is_alive_;
  bool error_thread_; // only record error during start thread

  TO_STRING_KV(K_(tid), K_(idx), KP_(pool), K_(is_stop), K_(is_alive), K_(error_thread));
};

class ObDynamicThreadTask
{
public:
  virtual ~ObDynamicThreadTask() {}
  virtual int process(const bool &is_stop) = 0;
};

class ObDynamicThreadPool: public lib::ThreadPool
{
public:
  static const int64_t MAX_THREAD_NUM = 512;
  static const int64_t MAX_TASK_NUM = 1024 * 1024;
  static const int64_t DEFAULT_CHECK_TIME_MS = 1000; // 1s

  ObDynamicThreadPool();
  ~ObDynamicThreadPool();
  int init(const char* thread_name = nullptr);
  int set_task_thread_num(const int64_t thread_num);
  int add_task(ObDynamicThreadTask *task);

  void run1() override;
  void stop();
  void destroy();
  void task_thread_idle();
  int64_t get_task_count() const { return task_queue_.get_total(); }
  TO_STRING_KV(K_(is_inited), K_(is_stop), K_(thread_num),
      K_(need_idle), K_(start_thread_num), K_(stop_thread_num), "left_task", task_queue_.get_total());
private:
  int check_thread_status();
  int stop_all_threads();
  void wakeup();

  int start_thread(ObDynamicThreadInfo &thread_info);
  int stop_thread(ObDynamicThreadInfo &thread_info);
  int pop_task(ObDynamicThreadTask *&task);
  static void *task_thread_func(void *data);
private:
  bool is_inited_;
  volatile bool is_stop_;
  int64_t thread_num_;
  ObFixedQueue<ObDynamicThreadTask> task_queue_;
  ObDynamicThreadInfo thread_infos_[MAX_THREAD_NUM];
  ObThreadCond task_thread_cond_;
  ObThreadCond cond_;
  bool need_idle_;
  int64_t start_thread_num_;
  int64_t stop_thread_num_;
  const char* thread_name_;
  DISALLOW_COPY_AND_ASSIGN(ObDynamicThreadPool);
};
class ObSimpleDynamicThreadPool
    : public lib::ThreadPool
{
public:
  static const int64_t MAX_THREAD_NUM = 1024;
  ObSimpleDynamicThreadPool()
    : min_thread_cnt_(OB_INVALID_COUNT), max_thread_cnt_(OB_INVALID_COUNT),
      running_thread_cnt_(0), threads_idle_time_(0), update_threads_lock_(), ref_cnt_(0), name_("unknown"), tenant_id_(OB_SERVER_TENANT_ID)
  {}
  virtual ~ObSimpleDynamicThreadPool();
  int init(const int64_t thread_num, const char* name, const int64_t tenant_id);
  void destroy();
  int set_adaptive_thread(int64_t min_thread_num, int64_t max_thread_num);
  virtual int64_t get_queue_num() const = 0;
  void try_expand_thread_count();
  void try_inc_thread_count(int64_t cnt = 1);
  int64_t get_threads_idle_time() const { return ATOMIC_LOAD(&threads_idle_time_); }
  int64_t get_min_thread_cnt() const { return min_thread_cnt_; }
  int64_t get_max_thread_cnt() const { return max_thread_cnt_; }
  int64_t get_running_thread_cnt() const { return running_thread_cnt_; }
  int set_thread_count(int64_t n_threads);
  int set_max_thread_count(int64_t max_thread_cnt);
  void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  void dec_ref() { ATOMIC_SAF(&ref_cnt_, 1); }
  int64_t get_ref_cnt() { return ATOMIC_LOAD(&ref_cnt_); }
  TO_STRING_KV(KCSTRING_(name), KP(this), K_(min_thread_cnt), K_(max_thread_cnt), K_(running_thread_cnt), K_(threads_idle_time), K_(tenant_id));
protected:
  int64_t inc_thread_idle_time(int64_t time_us)
  {
    return ATOMIC_AAF(&threads_idle_time_, time_us);
  }
  int64_t inc_running_thread_cnt(int64_t cnt)
  {
    return ATOMIC_AAF(&running_thread_cnt_, cnt);
  }
private:
  int64_t min_thread_cnt_;
  int64_t max_thread_cnt_;
  int64_t running_thread_cnt_;
  int64_t threads_idle_time_;
  lib::ObMutex update_threads_lock_;
  int64_t ref_cnt_;
  const char* name_;
  int64_t tenant_id_;
};

class ObSimpleThreadPoolDynamicMgr : public lib::TGRunnable {
public:
  static const int64_t SHRINK_INTERVAL_US = 1 * 1000 * 1000;
  static const int64_t CHECK_INTERVAL_US = 200 * 1000;
  ObSimpleThreadPoolDynamicMgr() : simple_thread_pool_list_(), simple_thread_pool_list_lock_(), is_inited_(false) {}
  virtual ~ObSimpleThreadPoolDynamicMgr();
  int init();
  void stop();
  void wait();
  void destroy();
  void run1();
  int bind(ObSimpleDynamicThreadPool *pool);
  int unbind(ObSimpleDynamicThreadPool *pool);
  int64_t get_pool_num() const;
  static ObSimpleThreadPoolDynamicMgr &get_instance();
  struct ObSimpleThreadPoolStat {
    ObSimpleThreadPoolStat()
     : pool_(NULL),
       last_check_time_(OB_INVALID_TIMESTAMP),
       last_idle_time_(OB_INVALID_TIMESTAMP),
       last_thread_count_(OB_INVALID_COUNT),
       last_shrink_time_(OB_INVALID_TIMESTAMP) {}
    ObSimpleThreadPoolStat(ObSimpleDynamicThreadPool *pool)
      : pool_(pool),
        last_check_time_(OB_INVALID_TIMESTAMP),
        last_idle_time_(OB_INVALID_TIMESTAMP),
        last_thread_count_(OB_INVALID_COUNT),
        last_shrink_time_(OB_INVALID_TIMESTAMP) {}
    int is_valid()
    {
      return last_check_time_ >= 0 && last_idle_time_ >= 0 && last_thread_count_ > 0;
    }
    TO_STRING_KV(KP_(pool), K_(last_check_time), K_(last_idle_time), K_(last_thread_count));
    ObSimpleDynamicThreadPool *pool_;
    int64_t last_check_time_;
    int64_t last_idle_time_;
    int64_t last_thread_count_;
    int64_t last_shrink_time_;
  };
private:
  ObArray<ObSimpleThreadPoolStat> simple_thread_pool_list_;
  common::SpinRWLock simple_thread_pool_list_lock_;
  int is_inited_;
};

class ObResetThreadTenantIdGuard {
public:
  DISABLE_COPY_ASSIGN(ObResetThreadTenantIdGuard);
  ObResetThreadTenantIdGuard() {
    tenant_id_ = ob_get_tenant_id();
    ob_get_tenant_id() = 0;
  }
  ~ObResetThreadTenantIdGuard() {
    ob_get_tenant_id() = tenant_id_;
  }
private:
  int64_t tenant_id_;
};
}
}

#endif /* SRC_LIBRARY_SRC_LIB_THREAD_OB_DYNAMIC_THREAD_POOL_H_ */
