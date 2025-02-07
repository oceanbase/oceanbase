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
#include <stdio.h>
#define info(format,...) fprintf(stderr, format "\n", ## __VA_ARGS__)
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include "ob_tc.h"
#include "deps/deps.h"
#include "deps/fifo_alloc.h"
#include <new>

#define cfg(k, v) (getenv(k)?:v)
#define cfgi(k, v) atoi(cfg(k,v))
#define ROOT(id) int id = qdisc_create(QDISC_ROOT, -1, #id)
#define FIFO(id, parent, weight) int id = qdisc_create(QDISC_BUFFER_QUEUE, parent, #id); qdisc_set_weight(id, weight);
#define SHARED(id, parent, weight) int id = qdisc_create(QDISC_WEIGHTED_QUEUE, parent, #id); qdisc_set_weight(id, weight);
#define DEF_LIMIT(id, x) int id = tclimit_create(TCLIMIT_BYTES, #id); tclimit_set_limit(id, x);
#define DEF_COUNT_LIMIT(id, x) int id = tclimit_create(TCLIMIT_COUNT, #id); tclimit_set_limit(id, x);
#define LIMIT(id, limiter_id) qdisc_add_limit(id, limiter_id)
#define RESERVE(id, limiter_id) qdisc_add_reserve(id, limiter_id)
#define SCHED() qsched_set_handler(root, &g_handler); qsched_start(root, n_sched_thread);
#define FILL(id, ...) FillThread fill_ ## id; fill_##id.init(root, id); fill_ ## id.start();

class FillThread;
class TestRequest: public TCRequest
{
public:
  TestRequest(int qid, int64_t bytes, FillThread* fill, int64_t seq): TCRequest(qid, bytes), fill_(fill), seq_(seq) {}
  ~TestRequest() {}
  FillThread* fill_;
  int64_t seq_;
};

class FillThread
{
public:
  friend class TestHandler;
  enum { N_ALLOC = MAX_N_CHAN };
  FillThread(): root_(-1), grp_(-1), gen_cnt_(0), sleep_interval_(0) {
    for(int i = 0; i < N_ALLOC; i++) {
      alloc_[i].set_limit(1<<21);
    }
  }
  ~FillThread() {}
  void init(int root, int grp) {
    root_ = root;
    grp_ = grp;
  }
  static void* do_fill_work(FillThread* self) { self->do_fill_loop(); return NULL; }
  void do_fill_loop() {
    tc_format_thread_name("fill_%d", grp_);
    while(1) {
      TestRequest* r = gen_request(100, gen_cnt_);
      if (r) {
        qsched_submit(root_, r, gen_cnt_);
        gen_cnt_++;
      } else {
        usleep(100);
      }
      int64_t sleep_us = sleep_interval_;
      if (sleep_us > 0) {
        usleep(sleep_us);
      }
    }
  }
  int start() {
    return pthread_create(&pd_, NULL, (void* (*)(void*))do_fill_work, this);
  }
private:
  TestRequest* gen_request(int bytes, int64_t idx) {
    TestRequest* r = (typeof(r))alloc_[idx % N_ALLOC].alloc(sizeof(TestRequest));
    if (r) {
      new(r)TestRequest(grp_, bytes, this, idx);
    }
    return r;
  }

  void free_request(TestRequest* r)
  {
    alloc_[r->seq_ % N_ALLOC].free(r);
  }
private:
  pthread_t pd_;
  int root_;
  int grp_;
  int64_t gen_cnt_;
  FifoAlloc alloc_[N_ALLOC];
  int sleep_interval_;
};

class TestHandler: public ITCHandler
{
public:
  TestHandler() {}
  virtual ~TestHandler() {}
  int handle(TCRequest* r1) {
    TestRequest* r = (typeof(r))r1;
    r->fill_->free_request(r);
    return 0;
  }
} g_handler;

void multi_fifo(int* array, int n, int parent) {
  for(int i = 0; i < n; i++) {
    char name[8];
    snprintf(name, sizeof(name), "a%d", i);
    array[i] = qdisc_create(QDISC_BUFFER_QUEUE, parent, name);
    qdisc_set_weight(array[i], 1);
  }
}
void multi_fill(FillThread* threads, int* array, int n, int root) {
  for(int i = 0; i < n; i++) {
    threads[i].init(root, array[i]);
    threads[i].start();
  }
}
#define MFIFO(array, n, parent) int array[32]; multi_fifo(array, n, parent);
#define MFILL(array, n) FillThread fill_## array[32]; multi_fill(fill_##array, array, n, root);

#define STR(x) XSTR(x)
#define XSTR(x) #x
int main()
{
  int n_sched_thread = cfgi("scheds", "2");
#include STR(SRC)
  pause();
  return 0;
}

#include "ob_tc.cpp"
