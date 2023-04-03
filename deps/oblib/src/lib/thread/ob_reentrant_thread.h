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

#ifndef OCEANBASE_SHARE_OB_REENTRANT_THREAD_H_
#define OCEANBASE_SHARE_OB_REENTRANT_THREAD_H_

#include <pthread.h>
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/thread/thread_pool.h"

namespace oceanbase
{
namespace share
{

class ObReentrantThread
    : public lib::ThreadPool
{
public:
  ObReentrantThread();
  virtual ~ObReentrantThread();

  // create thread, task will not run before start() called.
  int create(const int64_t thread_cnt, const char* thread_name = nullptr);

  // start and stop run task, can be called repeatedly, if created.
  int start() override;
  void stop() override;
  int logical_start();
  void logical_stop();
  void logical_wait();
  void wait() override; // wait running task stoped
  bool has_set_stop() const override
  {
    IGNORE_RETURN lib::Thread::update_loop_ts();
    return ATOMIC_LOAD(&stop_);
  }

  // destroy thread
  int destroy();

  const char* get_thread_name() const
  { return thread_name_; }

protected:
  // run thread interface, return void
  virtual void run2() = 0;

  //do things at new thread, before blocking run
  virtual int before_blocking_run()
  { return common::OB_SUCCESS; }

  // Wait start(), and call run().
  // To make detecting thread ownership easier (by pstack), we force subclass reimplement
  // this method by calling baseclass's implement.
  virtual int blocking_run() = 0;

  //do things at new thread, after blocking run
  virtual int after_blocking_run()
  { return common::OB_SUCCESS; }

  // do nothing, used to avoid blocking_run been omitted in g++ O2 optimize.
  void nothing();
  common::ObThreadCond &get_cond()
  { return cond_; }
private:
  void run1() final;

protected:
  volatile bool stop_;
private:
  volatile bool created_;
  int64_t running_cnt_;
  common::ObThreadCond cond_;
  const char* thread_name_;
};

#define BLOCKING_RUN_IMPLEMENT() nothing(); return ObReentrantThread::blocking_run();


} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_REENTRANT_THREAD_H_
