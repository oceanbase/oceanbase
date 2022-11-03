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

#ifndef __OB_COMMON_TEST_BASE_H__
#define __OB_COMMON_TEST_BASE_H__

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/time.h>
#include <unistd.h>

inline uint64_t rand64(uint64_t h)
{
  if (0 == h) return 1;
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccd;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53;
  h ^= h >> 33;
  //h &= ~(1ULL<<63);
  return h;
}

int64_t get_us()
{
  struct timeval time_val;
  gettimeofday(&time_val, NULL);
  return time_val.tv_sec * 1000000 + time_val.tv_usec;
}

#define profile(expr, n) { \
    int64_t start = get_usec(); \
    expr;\
    int64_t end = get_usec();\
    printf("%s: 1000000*%ld/%ld=%ld\n", #expr, n, end - start, 1000000 * n / (end - start)); \
  }

struct Callable
{
  Callable(): stop_(false) {}
  virtual ~Callable() {}
  virtual int call(pthread_t thread, int64_t idx) = 0;
  volatile bool stop_;
};

typedef void *(*pthread_handler_t)(void *);
class BaseWorker
{
public:
  static const int64_t MAX_N_THREAD = 16;
  struct WorkContext
  {
    WorkContext() : callable_(NULL), idx_(0) {}
    ~WorkContext() {}
    WorkContext &set(Callable *callable, int64_t idx)
    {
      callable_ = callable;
      idx_ = idx;
      return *this;
    }
    Callable *callable_;
    pthread_t thread_;
    int64_t idx_;
  };
public:
  BaseWorker(): n_thread_(0), thread_running_(false) {}
  ~BaseWorker()
  {
    wait();
  }
public:
  BaseWorker &set_thread_num(int64_t n) { n_thread_ = n; return *this; }
  int start(Callable *callable, int64_t idx = -1)
  {
    int err = 0;
    for (int64_t i = 0; i < n_thread_; i++) {
      if (idx > 0 && idx != i) { continue; }
      fprintf(stderr, "worker[%ld] start.\n", i);
      pthread_create(&ctx_[i].thread_, NULL, (pthread_handler_t)do_work, (void *)(&ctx_[i].set(callable,
                                                                                               i)));
    }
    thread_running_ = true;
    return err;
  }

  int wait(int64_t idx = -1)
  {
    int err = 0;
    int64_t ret = 0;
    for (int64_t i = 0; thread_running_ && i < n_thread_; i++) {
      if (idx > 0 && idx != i) { continue; }
      pthread_join(ctx_[i].thread_, (void **)&ret);
      if (ret != 0) {
        fprintf(stderr, "thread[%ld] => %ld\n", i, ret);
      } else {
        fprintf(stderr, "thread[%ld] => OK.\n", i);
      }
    }
    thread_running_ = false;
    return err;
  }

  static int do_work(WorkContext *ctx)
  {
    int err = 0;
    if (NULL == ctx || NULL == ctx->callable_) {
      err = -EINVAL;
    } else {
      err = ctx->callable_->call(ctx->thread_, ctx->idx_);
    }
    return err;
  }
  int par_do(Callable *callable, int64_t duration)
  {
    int err = 0;
    if (0 != (err = start(callable))) {
      fprintf(stderr, "start()=>%d\n", err);
    } else {
      usleep(static_cast<__useconds_t>(duration));
      callable->stop_ = true;
    }
    if (0 != (err = wait())) {
      fprintf(stderr, "wait()=>%d\n", err);
    }
    return err;
  }
protected:
  int64_t n_thread_;
  bool thread_running_;
  WorkContext ctx_[MAX_N_THREAD];
};

int PARDO(int64_t thread_num, Callable *call, int64_t duration)
{
  BaseWorker worker;
  fprintf(stderr, "thread_num=%ld\n", thread_num);
  return worker.set_thread_num(thread_num).par_do(call, duration);
}

#if 0
struct SimpleCallable: public Callable
{
  int64_t n_items_;
  SimpleCallable &set(int64_t n_items)
  {
    n_items_ = n_items;
    return *this;
  }
  int call(pthread_t thread, int64_t idx)
  {
    int err = 0;
    fprintf(stdout, "worker[%ld] run\n", idx);
    if (idx % 2) {
      err = -EPERM;
    }
    return err;
  }
};

int main(int argc, char **argv)
{
  int err = 0;
  BaseWorker worker;
  SimpleCallable callable;
  int64_t n_thread = 0;
  int64_t n_items = 0;
  if (argc != 3) {
    err = -EINVAL;
    fprintf(stderr, "%s n_thread n_item\n", argv[0]);
  } else {
    n_thread = atoll(argv[1]);
    n_items = atoll(argv[2]);
    profile(worker.set_thread_num(n_thread).par_do(&callable.set(n_items), 10000000), n_items);
  }
}
#endif


class RWT: public Callable
{
  typedef void *(*pthread_handler_t)(void *);
  struct Thread
  {
    int set(RWT *self, int64_t idx) { self_ = self, idx_ = idx; return 0; }
    pthread_t thread_;
    RWT *self_;
    int64_t idx_;
  };
public:
  RWT(): n_read_thread_(0), n_write_thread_(0), n_admin_thread_(0) {}
  virtual ~RWT() {}
public:
  int64_t get_thread_num() { return 1 + n_read_thread_ + n_write_thread_ + n_admin_thread_; }
  RWT &set(const int64_t n_read, const int64_t n_write, const int64_t n_admin = 0)
  {
    n_read_thread_ = n_read;
    n_write_thread_ = n_write;
    n_admin_thread_ = n_admin;
    return *this;
  }
  int report_loop()
  {
    int err = 0;
    int64_t report_interval = 1000 * 1000;
    while (!stop_ && 0 == err) {
      usleep(static_cast<__useconds_t>(report_interval));
      err = report();
    }
    return err;
  }
  virtual int call(pthread_t thread, const int64_t idx_)
  {
    int err = 0;
    int64_t idx = idx_;
    (void)(thread);
    fprintf(stderr, "rwt.start(idx=%ld)\n", idx_);
    if (idx < 0) {
      err = -EINVAL;
    }
    if (0 == err && idx >= 0) {
      if (idx == 0) {
        err = report_loop();
      }
      idx -= 1;
    }
    if (0 == err && idx >= 0) {
      if (idx < n_read_thread_) {
        err = read(idx);
      }
      idx -= n_read_thread_;
    }
    if (0 == err && idx >= 0) {
      if (idx < n_write_thread_) {
        err = write(idx);
      }
      idx -= n_write_thread_;
    }
    if (0 == err && idx >= 0) {
      if (idx < n_admin_thread_) {
        err = admin(idx);
      }
      idx -= n_admin_thread_;
    }
    if (0 == err && idx >= 0) {
      err = -EINVAL;
    }
    if (0 != err) {
      stop_ = true;
    }
    fprintf(stderr, "rwt.start(idx=%ld)=>%d\n", idx_, err);
    return err;
  }
  virtual int report() { return 0; }
  virtual int read(const int64_t idx) = 0;
  virtual int write(const int64_t idx) = 0;
  virtual int admin(const int64_t idx) { (void)(idx);  return 0; }
protected:
  int64_t n_read_thread_;
  int64_t n_write_thread_;
  int64_t n_admin_thread_;
};

#define _cfg(k, v) getenv(k)?:v
#define _cfgi(k, v) atoll(getenv(k)?:v)

inline int64_t rand_range(int64_t s, int64_t e)
{
  return s + random() % (e - s);
}

#define RWT_def(base) \
  TEST_F(base, Rand){ \
    ASSERT_EQ(0, PARDO(get_thread_num(), this, duration));      \
    ASSERT_EQ(0, check_error());                                \
  }

#include "gtest/gtest.h"
#include "common/data_buffer.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/regex/regex/utils.h"

using namespace oceanbase::common;

struct BufHolder
{
  BufHolder(int64_t limit) { buf_ = (char *)ob_malloc(limit, ObModIds::TEST); }
  ~BufHolder() { ob_free((void *)buf_); }
  char *buf_;
};

struct BaseConfig
{
  static const int64_t buf_limit = 1 << 21;
  BufHolder buf_holder;
  ObDataBuffer buf;
  int64_t duration;
  const char *schema;
  int64_t table_id;
  BaseConfig(): buf_holder(buf_limit)
  {
    buf.set_data(buf_holder.buf_, buf_limit);
    duration = _cfgi("duration", "3000000");
    schema = "./test.schema";
    table_id = 1002;
  }
};

class FixedAllocator: public ObIAllocator
{
public:
  FixedAllocator(char *buf, int64_t limit): buf_(buf), limit_(limit), pos_(0) {}
  virtual ~FixedAllocator() {}
public:
  void reset() { pos_ = 0; }
  virtual void *alloc(const int64_t sz)
  {
    void *ptr = NULL;
    int64_t pos = 0;
    if ((pos = __sync_add_and_fetch(&pos_, sz)) > limit_) {
      __sync_add_and_fetch(&pos_, -sz);
    } else {
      ptr = buf_ + pos;
    }
    return ptr;
  }
  virtual void free(void *ptr) { UNUSED(ptr); }
private:
  char *buf_;
  int64_t limit_;
  int64_t pos_;
};

#endif /* __OB_COMMON_TEST_BASE_H__ */
