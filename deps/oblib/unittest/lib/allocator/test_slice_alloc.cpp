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

#include "lib/allocator/ob_slice_alloc.h"
#include "lib/allocator/ob_vslice_alloc.h"
#include "lib/allocator/ob_small_allocator.h"
#include "lib/allocator/ob_simple_fifo_alloc.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/time/ob_time_utility.h"
#include "lib/metrics/ob_counter.h"

using namespace oceanbase;
using namespace oceanbase::common;

#define ISIZE 64
struct AllocInterface
{
public:
  virtual void* alloc() = 0;
  virtual void free(void* p) = 0;
  virtual int64_t hold() { return 0; }
  virtual void set_nway(int nway) { UNUSED(nway); }
};

struct MallocWrapper: public AllocInterface
{
public:
  void* alloc() { return ::malloc(ISIZE); }
  void free(void* p) { ::free(p); }
};

struct ObMallocWrapper: public AllocInterface
{
public:
  void* alloc() { return common::ob_malloc(ISIZE, ObNewModIds::TEST); }
  void free(void* p) { common::ob_free(p); }
};

struct SmallAllocWrapper: public AllocInterface, public ObSmallAllocator
{
public:
  SmallAllocWrapper() { alloc_.init(ISIZE); }
  void* alloc() { return alloc_.alloc(); }
  void free(void* p) { alloc_.free(p); }
private:
  ObSmallAllocator alloc_;
};

struct OpAllocWrapper: public AllocInterface
{
public:
  struct Item {
    char data[ISIZE];
  };
  void* alloc() { return (void*)op_alloc(Item);}
  void free(void* p) { return op_free((Item*)p);}
};

struct OpReclaimAllocWrapper: public AllocInterface
{
public:
  struct Item {
    char data[ISIZE];
  };
  void* alloc() { return (void*)op_reclaim_alloc(Item);}
  void free(void* p) { return op_reclaim_free((Item*)p);}
};

ObMemAttr mem_attr;
struct SliceAllocWrapper: public AllocInterface
{
public:
  SliceAllocWrapper(): alloc_(ISIZE, mem_attr, OB_MALLOC_BIG_BLOCK_SIZE) { alloc_.set_nway(64); }
  void* alloc() { return alloc_.alloc(); }
  void free(void* p) { return alloc_.free(p); }
  void set_nway(int nway) { alloc_.set_nway(nway); }
  int64_t hold() { return alloc_.hold(); }
private:
  ObSliceAlloc alloc_;
};

struct VSliceAllocWrapper: public AllocInterface
{
public:
  VSliceAllocWrapper(): alloc_(mem_attr, OB_MALLOC_BIG_BLOCK_SIZE) { alloc_.set_nway(64); }
  void* alloc() { return alloc_.alloc(ISIZE); }
  void free(void* p) { return alloc_.free(p); }
  void set_nway(int nway) { alloc_.set_nway(nway); }
  int64_t hold() { return alloc_.hold(); }
private:
  ObVSliceAlloc alloc_;
};

struct SimpleFifoAllocWrapper: public AllocInterface
{
public:
  SimpleFifoAllocWrapper(): alloc_(mem_attr, OB_MALLOC_BIG_BLOCK_SIZE) { alloc_.set_nway(64); }
  void* alloc() { return alloc_.alloc(ISIZE); }
  void free(void* p) { return alloc_.free(p); }
  void set_nway(int nway) { alloc_.set_nway(nway); }
  int64_t hold() { return alloc_.hold(); }
private:
  ObSimpleFifoAlloc alloc_;
};

ObTCCounter gcounter;
class FixedStack
{
public:
  FixedStack(): top_(base_) {}
  void push(void* p) {
    if (NULL == p) abort();
    *top_++ = p;
  }
  void* pop() { return *--top_; }
private:
  void** top_;
  void* base_[4096];
};

FixedStack gstack[65];
FixedStack& get_stack() { return gstack[get_itid()]; }

inline uint64_t rand64(uint64_t h)
{
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccd;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53;
  h ^= h >> 33;
  return h;
}

int64_t next_seq() {
  static int64_t init_seq = 0;
  static __thread int64_t seq = 0;
  if (seq <= 0) {
    seq = ATOMIC_AAF(&init_seq, 1);
  }
  return seq += 1024;
}

int64_t rand_gen() { return rand64(next_seq()); }

inline void do_alloc(AllocInterface* ga, int64_t count = 1) {
  for(int i = 0; i < count; i++) {
    get_stack().push(ga->alloc());
  }
}

inline void do_free(AllocInterface* ga, int64_t count = 1) {
  for(int i = 0; i < count; i++) {
    ga->free(get_stack().pop());
  }
}

volatile bool g_stop CACHE_ALIGNED;
void* thread_func(AllocInterface* ga) {
  while(!g_stop) {
    int64_t x = 1 + (rand_gen() & 1023);
    do_alloc(ga, x);
    do_free(ga, x);
    gcounter.inc(x);
  }
  return NULL;
}

#define cfgi(k, d) atoi(getenv(k)?:#d)
void do_perf(AllocInterface* ga) {
  int n_sec = cfgi("n_sec", 1);
  int n_way = cfgi("n_way", 1);
  int n_thread = cfgi("n_thread", 8);
  pthread_t thread[128];
  g_stop = false;
  ga->set_nway(n_way);
  for(int i = 0; i < n_thread; i++) {
    pthread_create(thread + i, NULL, (void* (*)(void*))thread_func, ga);
  }
  sleep(1);
  int64_t last = gcounter.value();
  while(n_sec--) {
    sleep(1);
    int64_t cur = gcounter.value();
    fprintf(stderr, "tps=%'ld hold=%ld\n", cur - last, ga->hold());
    last = cur;
  }
  g_stop = true;
  for(int i = 0; i < n_thread; i++) {
    pthread_join(thread[i], NULL);
  }
}
//int64_t get_us() { return ObTimeUtility::current_time(); }
#define PERF(x) {fprintf(stderr, #x "\n"); x ## Wrapper ga; do_perf(&ga); }

#include <locale.h>
int main()
{
  setlocale(LC_ALL, "");
  PERF(SliceAlloc);
  PERF(VSliceAlloc);
  PERF(Malloc);
  PERF(ObMalloc);
  PERF(SmallAlloc);
  PERF(OpAlloc);
  PERF(OpReclaimAlloc);
  PERF(SimpleFifoAlloc);
  return 0;
}
