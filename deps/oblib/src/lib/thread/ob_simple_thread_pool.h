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
#include "common/ob_queue_thread.h"

namespace oceanbase
{
namespace common
{

class ObAdaptiveStrategy
{
public:
  ObAdaptiveStrategy()
    : least_thread_num_(0),
      estimate_ts_(0),
      expand_rate_(0),
      shrink_rate_(0) {}
  ObAdaptiveStrategy(const int64_t least_thread_num,
                     const int64_t estimate_ts,
                     const int64_t expand_rate,
                     const int64_t shrink_rate)
    : least_thread_num_(least_thread_num),
      estimate_ts_(estimate_ts),
      expand_rate_(expand_rate),
      shrink_rate_(shrink_rate) {}
  ~ObAdaptiveStrategy() {}
public:
  int64_t get_least_thread_num() const { return least_thread_num_; }
  int64_t get_estimate_ts() const { return estimate_ts_; }
  int64_t get_expand_rate() const { return expand_rate_; }
  int64_t get_shrink_rate() const { return shrink_rate_; }
  bool is_valid() const
  {
    return least_thread_num_ > 0 &&
           estimate_ts_ > 0 &&
           expand_rate_ > 0 &&
           shrink_rate_ > 0 &&
           expand_rate_ > shrink_rate_;
  }
public:
  TO_STRING_KV(K_(least_thread_num), K_(estimate_ts), K_(expand_rate), K_(shrink_rate));
private:
  int64_t least_thread_num_;
  int64_t estimate_ts_;
  int64_t expand_rate_;
  int64_t shrink_rate_;
};

class ObSimpleThreadPool
    : public lib::ThreadPool
{
  static const int64_t QUEUE_WAIT_TIME = 100 * 1000;
  static const int64_t MAX_THREAD_NUM = 256;
public:
  ObSimpleThreadPool();
  virtual ~ObSimpleThreadPool();

  int init(const int64_t thread_num, const int64_t task_num_limit, const char *name = "unknown", const uint64_t tenant_id = OB_SERVER_TENANT_ID);
  void destroy();
  int push(void *task);
  int64_t get_queue_num() const { return queue_.size(); }
  int set_adaptive_strategy(const ObAdaptiveStrategy &strategy);
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
  ObLightyQueue queue_;
  int64_t total_thread_num_;
  int64_t active_thread_num_;
  ObAdaptiveStrategy adaptive_strategy_;
  int64_t last_adjust_ts_;
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_SIMPLE_THREAD_POOL_H_
