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

#define USING_LOG_PREFIX LIB
#include "threads.h"
#include "common/ob_common_utility.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_hang_fatal_error.h"
#include "lib/thread/protected_stack_allocator.h"
#include "lib/oblog/ob_log.h"
#include "lib/signal/ob_signal_struct.h"
#include "lib/worker.h"
using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;

int64_t global_thread_stack_size = (1L << 19) - SIG_STACK_SIZE - ACHUNK_PRESERVE_SIZE;
thread_local uint64_t ThreadPool::thread_idx_ = 0;

// 获取线程局部的租户上下文，为线程池启动时检查使用
IRunWrapper *&Threads::get_expect_run_wrapper()
{
  static thread_local IRunWrapper *instance = nullptr;
  return instance;
}

Threads::~Threads()
{
  stop();
  wait();
  destroy();
}

int Threads::do_set_thread_count(int64_t n_threads)
{
  int ret = OB_SUCCESS;
  if (!stop_) {
    if (n_threads < n_threads_) {
      for (auto i = n_threads; i < n_threads_; i++) {
        threads_[i]->stop();
      }
      for (auto i = n_threads; i < n_threads_; i++) {
        auto thread = threads_[i];
        thread->wait();
        thread->destroy();
        thread->~Thread();
        ob_free(thread);
        threads_[i] = nullptr;
      }
      n_threads_ = n_threads;
    } else if (n_threads == n_threads_) {
    } else {
      auto new_threads = reinterpret_cast<Thread**>(
          ob_malloc(sizeof (Thread*) * n_threads, ObMemAttr(0 == GET_TENANT_ID() ? OB_SERVER_TENANT_ID : GET_TENANT_ID(), "Coro", ObCtxIds::DEFAULT_CTX_ID, OB_NORMAL_ALLOC)));
      if (new_threads == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(new_threads, threads_, sizeof (Thread*) * n_threads_);
        for (auto i = n_threads_; i < n_threads; i++) {
          Thread *thread = nullptr;
          ret = create_thread(thread, i);
          if (OB_FAIL(ret)) {
            n_threads = i;
            break;
          } else {
            new_threads[i] = thread;
          }
        }
        if (OB_FAIL(ret)) {
          for (auto i = n_threads_; i < n_threads; i++) {
            if (new_threads[i] != nullptr) {
              destroy_thread(new_threads[i]);
              new_threads[i] = nullptr;
            }
          }
          ob_free(new_threads);
        } else {
          ob_free(threads_);
          threads_ = new_threads;
          n_threads_ = n_threads;
        }
      }
    }
  } else { // modify init_threads_ before start
    init_threads_ = n_threads;
  }
  return ret;
}

int Threads::set_thread_count(int64_t n_threads)
{
  common::SpinWLockGuard g(lock_);
  return do_set_thread_count(n_threads);
}

int Threads::inc_thread_count(int64_t inc)
{
  common::SpinWLockGuard g(lock_);
  int64_t n_threads = n_threads_ + inc;
  return do_set_thread_count(n_threads);
}

int Threads::thread_recycle()
{
  // check if any idle threads and notify them to exit
  // idle defination: not working for more than N minutes
  common::SpinWLockGuard g(lock_);
  // int target = 10; // leave at most 10 threads as cached thread
  return do_thread_recycle();
}

int Threads::do_thread_recycle()
{
  int ret = OB_SUCCESS;
  int n_threads = n_threads_;
  // destroy all stopped threads
  // px threads mark itself as stopped when it is idle for more than 10 minutes.
  for (int i = 0; i < n_threads_; i++) {
    if (nullptr != threads_[i]) {
      if (threads_[i]->has_set_stop()) {
        destroy_thread(threads_[i]);
        threads_[i] = nullptr;
        n_threads--;
        LOG_INFO("recycle one thread", "total", n_threads_, "remain", n_threads);
      }
    }
  }
  // for simplicity, don't free threads_ buffer, only reduce n_threads_ size
  if (n_threads != n_threads_) {
    int from = 0;
    int to = 0;
    // find non-empty slot, set it to threads_[i]
    while (from < n_threads_ && to < n_threads_) {
      if (nullptr != threads_[from]) {
        threads_[to] = threads_[from];
        to++;
      }
      from++;
    }
    n_threads_ = n_threads;
  }
  return ret;
}

int Threads::init()
{
  return OB_SUCCESS;
}

int Threads::start()
{
  int ret = OB_SUCCESS;
  // 检查租户上下文
  IRunWrapper *expect_wrapper = get_expect_run_wrapper();
  n_threads_ = init_threads_;
  if (expect_wrapper != nullptr && expect_wrapper != run_wrapper_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Threads::start tenant ctx not match", KP(expect_wrapper), KP(run_wrapper_));
    ob_abort();
  } else {
    threads_ = reinterpret_cast<Thread**>(
      ob_malloc(sizeof (Thread*) * n_threads_, ObMemAttr(0 == GET_TENANT_ID() ? OB_SERVER_TENANT_ID : GET_TENANT_ID(), "Coro", ObCtxIds::DEFAULT_CTX_ID, OB_NORMAL_ALLOC)));
    if (threads_ == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  if (OB_SUCC(ret)) {
    stop_ = false;
    MEMSET(threads_, 0, sizeof (Thread*) * n_threads_);
    for (int i = 0; i < n_threads_; i++) {
      Thread *thread = nullptr;
      ret = create_thread(thread, i);
      if (OB_FAIL(ret)) {
        break;
      } else {
        threads_[i] = thread;
      }
    }
    if (OB_FAIL(ret)) {
      Threads::stop();
      Threads::wait();
      Threads::destroy();
    }
  }
  return ret;
}

void Threads::run(int64_t idx)
{
  ObTLTaGuard ta_guard(GET_TENANT_ID() ?:OB_SERVER_TENANT_ID);
  thread_idx_ = static_cast<uint64_t>(idx);
  Worker worker;
  Worker::set_worker_to_thread_local(&worker);
  run1();
}

int Threads::create_thread(Thread *&thread, int64_t idx)
{
  int ret = OB_SUCCESS;
  thread = nullptr;
  const auto buf = ob_malloc(sizeof (Thread), ObMemAttr(0 == GET_TENANT_ID() ? OB_SERVER_TENANT_ID : GET_TENANT_ID(), "Coro", ObCtxIds::DEFAULT_CTX_ID, OB_NORMAL_ALLOC));
  if (buf == nullptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    thread = new (buf) Thread(this, idx, stack_size_);
    if (OB_FAIL(thread->start())) {
      thread->~Thread();
      ob_free(thread);
      thread = nullptr;
    }
  }
  return ret;
}

void Threads::destroy_thread(Thread *thread)
{
  thread->stop();
  thread->wait();
  thread->destroy();
  thread->~Thread();
  ob_free(thread);
}

void Threads::wait()
{
  if (threads_ != nullptr) {
    for (int i = 0; i < n_threads_; i++) {
      if (threads_[i] != nullptr) {
        threads_[i]->wait();
      }
    }
  }
}

void Threads::stop()
{
  common::SpinRLockGuard g(lock_);
  stop_ = true;
  if (OB_NOT_NULL(threads_)) {
    for (int i = 0; i < n_threads_; i++) {
      if (threads_[i] != nullptr) {
        threads_[i]->stop();
      }
    }
  }
}

void Threads::destroy()
{
  if (threads_ != nullptr) {
    for (int i = 0; i < n_threads_; i++) {
      if (threads_[i] != nullptr) {
        threads_[i]->destroy();
        threads_[i]->~Thread();
        ob_free(threads_[i]);
        threads_[i] = nullptr;
      }
    }
    ob_free(threads_);
    threads_ = nullptr;
  }
}

int ObPThread::try_wait()
{
  int ret = OB_SUCCESS;
  if (nullptr != threads_[0]) {
    if (OB_FAIL(threads_[0]->try_wait())) {
      LOG_WARN("ObPThread try_wait failed", K(ret));
    }
  }
  return ret;
}