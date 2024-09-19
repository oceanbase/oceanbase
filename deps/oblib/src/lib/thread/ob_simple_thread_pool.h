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

#ifndef OCEANBASE_COMMON_OB_SIMPLE_THREAD_POOL_H_
#define OCEANBASE_COMMON_OB_SIMPLE_THREAD_POOL_H_

#include "lib/queue/ob_lighty_queue.h"
#include "lib/thread/thread_pool.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "common/ob_queue_thread.h"

namespace oceanbase
{
namespace common
{
template <class T = ObLightyQueue>
class ObSimpleThreadPoolBase
    : public ObSimpleDynamicThreadPool
{
  static const int64_t QUEUE_WAIT_TIME = 100 * 1000;
public:
  ObSimpleThreadPoolBase();
  virtual ~ObSimpleThreadPoolBase();

  int init(const int64_t thread_num, const int64_t task_num_limit, const char *name = "unknown", const uint64_t tenant_id = OB_SERVER_TENANT_ID);
  void destroy();
  int push(void *task);
  int push(ObLink *task, const int priority); // designed for ObPriorityQueue
  virtual int64_t get_queue_num() const override
  {
    return queue_.size();
  }
  int set_thread_count(int64_t n_threads)
  {
    return set_max_thread_count(n_threads);
  }
private:
  virtual void handle(void *task) = 0;
  virtual void handle_drop(void *task) {
    // when thread set stop left task will be process by handle_drop (default impl is handle)
    // users should define it's behaviour to manage task memory or some what
    handle(task);
  }
protected:
  void run1();

private:
  const char* name_;
  bool is_inited_;
  T queue_;
};

using ObSimpleThreadPool = ObSimpleThreadPoolBase<ObLightyQueue>;

} // namespace common
} // namespace oceanbase

#include "ob_simple_thread_pool.ipp"

#endif // OCEANBASE_COMMON_OB_SIMPLE_THREAD_POOL_H_
