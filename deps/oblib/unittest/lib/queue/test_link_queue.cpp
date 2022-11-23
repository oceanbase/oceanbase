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

#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/time/ob_time_utility.h"
#include "lib/metrics/ob_counter.h"
using namespace oceanbase;
using namespace oceanbase::common;

ObTCCounter push_count, pop_count;

volatile bool g_stop CACHE_ALIGNED;
#define MAGIC 12345678
void before_push(int64_t* p) {
  *p = MAGIC;
}
void after_pop(int64_t* p) {
  if (MAGIC != *p) {
    fprintf(stderr, "pop error\n");
    abort();
  }
  *p = 0;
}

struct QI
{
  virtual int push(void* p) = 0;
  virtual void* pop(int64_t timeout) = 0;
};

struct SLinkQueueWrapper: public QI
{
  int push(void* p) { q_.push((ObLink*)p); return 0; }
  void* pop(int64_t timeout) { UNUSED(timeout); return (void*)q_.pop(); }
  ObSpScLinkQueue q_;
};

struct ObLightyQueueWrapper: public QI
{
  ObLightyQueueWrapper() {
    q_.init(1<<20);
  }
  int push(void* p) { return q_.push(p); }
  void* pop(int64_t timeout) {
    void* p = NULL;
    q_.pop(p, timeout);
    return p;
  }
  ObLightyQueue  q_;
};
void do_push(QI* q) {
  void* p = malloc(16);
  before_push((int64_t*)p + 1);
  while(0 != q->push(p))
    ;
}

void do_pop(QI* q) {
  void* p = NULL;
  int64_t timeout = 100 * 1000;
  while(NULL == (p = q->pop(timeout)) && !g_stop)
    ;
  if (p) {
    after_pop((int64_t*)p + 1);
    free(p);
  }
}

void* pop_loop(QI* q) {
  while(!g_stop) {
    do_pop(q);
    pop_count.inc(1);
  }
  return NULL;
}
void* push_loop(QI* q) {
  while(!g_stop) {
    do_push(q);
    usleep(100);
    push_count.inc(1);
  }
  return NULL;
}

#define cfgi(k, d) atoi(getenv(k)?:#d)
typedef void* handler_t(void*);
void do_perf(QI* qi) {
  int n_sec = cfgi("n_sec", 1);
  int n_thread = cfgi("n_thread", 2);
  pthread_t thread[256];
  g_stop = false;
  for(int i = 0; i < n_thread; i++) {
    handler_t* handler = (handler_t*)((i % 2)? push_loop: pop_loop);
    pthread_create(thread + i, NULL, handler, qi);
  }
  sleep(1);
  int64_t last_push = push_count.value();
  int64_t last_pop = pop_count.value();
  while(n_sec--) {
    sleep(1);
    int64_t cur_push = push_count.value();
    int64_t cur_pop = pop_count.value();
    fprintf(stderr, "push=%'ld pop=%'ld\n", cur_push - last_push, cur_pop - last_pop);
    last_push = cur_push;
    last_pop = cur_pop;
  }
  g_stop = true;
  for(int i = 0; i < n_thread; i++) {
    pthread_join(thread[i], NULL);
  }
}

//int64_t get_us() { return ObTimeUtility::current_time(); }
#define PERF(x) {fprintf(stderr, #x "\n"); x ## Wrapper qi; do_perf(&qi); }

#include <locale.h>
int main()
{
  setlocale(LC_ALL, "");
  //PERF(SLinkQueue);
  PERF(ObLightyQueue);
  return 0;
}
