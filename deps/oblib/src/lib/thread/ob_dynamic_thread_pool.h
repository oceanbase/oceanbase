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

}
}

#endif /* SRC_LIBRARY_SRC_LIB_THREAD_OB_DYNAMIC_THREAD_POOL_H_ */
