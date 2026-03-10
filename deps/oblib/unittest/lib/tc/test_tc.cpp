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

#include <gtest/gtest.h>
#include "lib/tc/ob_tc.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;

static void config_qdisc(int qid, int64_t weight, int64_t max_bw, int64_t min_bw)
{
  qdisc_set_weight(qid, weight? weight: 100);
  qdisc_set_limit(qid, max_bw);
  qdisc_set_reserve(qid, min_bw);
}
class TestFillThread;
class TestTenantIOScheduler;
class TestHandler;

struct TestTCRequest
{
  TestTCRequest(int qid, int64_t bytes, TestFillThread *fill_thread)
    : enqueue_ts_(0), dequeue_ts_(0), fill_thread_(fill_thread), req_(qid, bytes) {}
  int64_t enqueue_ts_;
  int64_t dequeue_ts_;
  TestFillThread *fill_thread_;
  TCRequest req_;
};

struct TestIOStat
{
public:
  TestIOStat(int64_t tenant_id) : tenant_id_(tenant_id), qid_(-1), name_(NULL), sched_time_(0), bw_(0), count_(0) {}
  void accumulate(TestTCRequest *req) {
    lock_.lock();
    sched_time_ += req->dequeue_ts_ - req->enqueue_ts_;
    bw_ += req->req_.bytes_;
    count_ += 1;
    lock_.unlock();
  }
  void print_and_reset() {
    if (qid_ < 0) {
      return;
    }
    lock_.lock();
    int64_t sched_time = sched_time_;
    int64_t bw = bw_;
    int64_t count = count_;
    sched_time_ = 0;
    bw_ = 0;
    count_ = 0;
    lock_.unlock();
    int64_t avg_sched_time = count > 0 ? sched_time / count : 0;
    _OB_LOG(INFO, "tenant_id: %lld, name: %s, qid: %lld, avg_sched_time: %lld, bw: %lld, count: %lld", tenant_id_, name_, qid_, avg_sched_time, bw, count);
  }
  ObSimpleLock lock_;
  int64_t tenant_id_;
  int64_t qid_;
  const char *name_;
  int64_t sched_time_;
  int64_t bw_;
  int64_t count_;
};

class TestIOScheduler
{
public:
  TestIOScheduler()
    : root_id_(-1), dump_stat_pth_(-1), is_dump_stop_(false) {}

  ~TestIOScheduler()
  {
    destroy();
  }
  static TestIOScheduler &get_instance() {
    static TestIOScheduler instance;
    return instance;
  }
  void init(int n_sched_thread, ITCHandler *handler) {
    root_id_ = qdisc_create(QD_TYPE::QDISC_ROOT, -1, "root");
    config_qdisc(root_id_, 1, INT64_MAX, INT64_MAX);
    qsched_set_handler(root_id_, handler);
    qsched_start(root_id_, n_sched_thread);
    pthread_create(&dump_stat_pth_, NULL, dump_io_stat, (void*)this);
  }

  void destroy() {
    is_dump_stop_ = true;
    pthread_join(dump_stat_pth_, NULL);
    if (root_id_ >= 0) {
      qsched_stop(root_id_);
      qsched_wait(root_id_);
      qdisc_destroy(root_id_);
      root_id_ = -1;
    }
  }
  void add_tenant_ioscheduler(TestTenantIOScheduler *tenant_ioscheduler) {
    for (int i = 0; i<1024; i++) {
      if (tenant_ioschedulers_[i] == NULL) {
        tenant_ioschedulers_[i] = tenant_ioscheduler;
        return;
      }
    }
  }
  void remove_tenant_ioscheduler(TestTenantIOScheduler *tenant_ioscheduler) {
    for (int i = 0; i<1024; i++) {
      if (tenant_ioschedulers_[i] == tenant_ioscheduler) {
        tenant_ioschedulers_[i] = NULL;
        return;
      }
    }
  }
  static void* dump_io_stat(void *arg);
  int get_root_id() { return root_id_; }
private:
  int root_id_;
  pthread_t dump_stat_pth_;
  bool is_dump_stop_;
  TestTenantIOScheduler *tenant_ioschedulers_[16];
};
class TestFillThread
{
public:
  TestFillThread(int64_t tenant_id)
    : tenant_id_(tenant_id), parent_qid_(-1), qid_(-1), stop_(false), pth_(-1), gen_cnt_(0), sleep_interval_(INT64_MAX), size_(0), io_stat_(tenant_id) {}
  static void* do_fill_work(void* arg)
  {
    TestFillThread *fill_thread = (TestFillThread*)arg;
    fill_thread->do_fill_loop();
    return NULL;
  }
  void do_fill_loop() {
    while(!stop_) {
      void *ptr = ob_malloc(sizeof(TestTCRequest), "IORequest");
      if (ptr) {
        TestTCRequest *req = new(ptr) TestTCRequest(qid_, size_, this);
        req->enqueue_ts_ = ObTimeUtility::fast_current_time();
        int64_t root_id = TestIOScheduler::get_instance().get_root_id();
        qsched_submit(root_id, &(req->req_), gen_cnt_);
        gen_cnt_++;
      }
      usleep(sleep_interval_);
    }
  }
  void init(const char *group_name, int parent_qid, int64_t iops, int64_t size, int64_t weight, int64_t max_bw, int64_t min_bw)
  {
    parent_qid_ = parent_qid;
    sleep_interval_ = 1000000 / iops;
    size_ = size;
    qid_ = qdisc_create(QD_TYPE::QDISC_BUFFER_QUEUE, parent_qid_, group_name);
    io_stat_.qid_ = qid_;
    io_stat_.name_ = group_name;
    config_qdisc(qid_, weight, max_bw, min_bw);
    pthread_create(&pth_, NULL, TestFillThread::do_fill_work, (void*)this);
  }
  void destroy() {
    stop_ = true;
    pthread_join(pth_, NULL);
    qdisc_destroy(qid_);
  }
  void print_stat() {
    io_stat_.print_and_reset();
  }
private:
  int64_t tenant_id_;
  int64_t parent_qid_;
  int64_t qid_;
  int stop_;
  pthread_t pth_;
  int64_t gen_cnt_;
  int64_t sleep_interval_;
  int64_t size_;
public:
  TestIOStat io_stat_;
};
class TestHandler: public ITCHandler
{
public:
  TestHandler() {}
  virtual ~TestHandler() {}
  int handle(TCRequest* r1) {
    TestTCRequest *req = (TestTCRequest*)((char*)r1 - offsetof(TestTCRequest, req_));
    req->dequeue_ts_ = ObTimeUtility::fast_current_time();
    req->fill_thread_->io_stat_.accumulate(req);
    ob_free(req);
    return 0;
  }
};

/*
租户资源调度器，租户下有挂载一个weighted queue，
weighted queue下有挂载多个资源组，不同资源组按需设置不同的weight、limit、reserve
*/
class TestTenantIOScheduler
{
public:
  TestTenantIOScheduler(int64_t tenant_id)
    : tenant_id_(tenant_id), top_qid_(-1),
      fill_thread_A_(tenant_id), fill_thread_B_(tenant_id) {}
  void init(int root_id, int64_t max_bw, int64_t min_bw,
            int64_t iopsA, int64_t sizeA, int64_t weightA,
            int64_t iopsB, int64_t sizeB, int64_t weightB) {
    top_qid_ = qdisc_create(QD_TYPE::QDISC_WEIGHTED_QUEUE, root_id, "top");
    config_qdisc(top_qid_, 3, max_bw, min_bw);

    fill_thread_A_.init("group_A", top_qid_, iopsA, sizeA, weightA, max_bw, min_bw * 0.3);
    fill_thread_B_.init("group_B", top_qid_, iopsB, sizeB, weightB, max_bw, min_bw * 0.2);
    TestIOScheduler::get_instance().add_tenant_ioscheduler(this);
  }
  void destroy() {
    TestIOScheduler::get_instance().remove_tenant_ioscheduler(this);
    fill_thread_A_.destroy();
    fill_thread_B_.destroy();
  }
  void print_stat() {
    fill_thread_A_.print_stat();
    fill_thread_B_.print_stat();
  }

private:
  int64_t tenant_id_;
  int top_qid_;
  TestFillThread fill_thread_A_;
  TestFillThread fill_thread_B_;
};



void *TestIOScheduler::dump_io_stat(void *arg) {
  TestIOScheduler *ioscheduler = (TestIOScheduler*)arg;
  TestTenantIOScheduler **tenant_ioschedulers = ioscheduler->tenant_ioschedulers_;
  while (!ioscheduler->is_dump_stop_) {
    for (int i = 0; i<16; i++) {
      if (tenant_ioschedulers[i] != NULL) {
        tenant_ioschedulers[i]->print_stat();
      }
    }
    sleep(1);
  }
  return NULL;
}

TEST(TestTC, basic)
{
  const int64_t root_id = TestIOScheduler::get_instance().get_root_id();
  const int64_t max_bw = 128LL<<20;
  const int64_t min_bw = 128LL<<20;
  {
    // 测试删租户时回收qdisc后，不会有use-after-free的问题
    TestTenantIOScheduler t1(1001);
    //小字节高iops 与大字节低iops
    t1.init(root_id, max_bw, min_bw, 10000, 64LL<<10, 3, 10, 2LL<<20, 1);
    TestTenantIOScheduler t2(1002);
    //小字节低iops 与小字节高iops
    t2.init(root_id, max_bw, min_bw, 10000, 64LL<<10, 3, 100, 64LL<<10, 1);

    sleep(5);
    t1.destroy();
    sleep(5);
    t2.destroy();
  }
  {
    //测试流控算法
    TestTenantIOScheduler t3(1003);
    t3.init(root_id, max_bw, min_bw, 2000, 32<<10, 3, 32, 2LL<<20, 1);
    sleep(5);
    t3.destroy();
  }

}

int main(int argc, char **argv)
{
  system("rm -rf test_tc.log*");
  testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_tc.log", true);
  TestHandler handler;
  TestIOScheduler::get_instance().init(1, &handler);
  return RUN_ALL_TESTS();
}