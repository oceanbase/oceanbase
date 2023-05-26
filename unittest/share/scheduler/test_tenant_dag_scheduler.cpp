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

#include <getopt.h>
#include <unistd.h>
#include <gtest/gtest.h>
#define protected public
#define private public
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scheduler/ob_worker_obj_pool.h"
#include "lib/atomic/ob_atomic.h"
#include "observer/omt/ob_tenant_node_balancer.h"

int64_t dag_cnt = 1;
int64_t stress_time= 5; // 500ms
char log_level[20] = "INFO";
uint32_t time_slice = 20 * 1000; // 5ms
uint32_t sleep_slice = 2 * time_slice;
const int64_t CHECK_TIMEOUT = 1 * 1000 * 1000;

#define CHECK_EQ_UTIL_TIMEOUT(expected, expr) \
  { \
    int64_t start_time = oceanbase::common::ObTimeUtility::current_time(); \
    auto expr_result = (expr); \
    do { \
      if ((expected) == (expr_result)) { \
        break; \
      } else { \
        expr_result = (expr); \
      }\
    } while(oceanbase::common::ObTimeUtility::current_time() - start_time < CHECK_TIMEOUT); \
    EXPECT_EQ((expected), (expr_result)); \
  }

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace omt;
namespace unittest
{
static const int64_t SLEEP_SLICE = 100;

class TypeIdTask : public ObITaskNew
{
public:
  TypeIdTask() : ObITaskNew(0), cnt_(0), seq_(0) {}
  ~TypeIdTask() {}
  void init(int seq, int cnt)
  {
    seq_ = seq;
    cnt_ = cnt;
  }
  virtual int generate_next_task(ObITaskNew *&next_task)
  {
    int ret = OB_SUCCESS;
    if (seq_ >= cnt_ - 1) {
      ret = OB_ITER_END;
      COMMON_LOG(INFO, "generate task end", K_(seq), K_(cnt));
    } else {
      ObIDagNew *dag = get_dag();
      TypeIdTask *ntask = NULL;
      if (NULL == dag) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "dag is null", K(ret));
      } else if (OB_FAIL(dag->alloc_task(ntask))) {
        COMMON_LOG(WARN, "failed to alloc task", K(ret));
      } else if (NULL == ntask) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "task is null", K(ret));
      } else {
        ntask->init(seq_ + 1, cnt_);
        if (OB_FAIL(ret)) {
          dag->free_task(*ntask);
        } else {
          next_task = ntask;
        }
      }
    }
    return ret;
  }
  virtual int process()
  {
    ::usleep(SLEEP_SLICE);
    return OB_SUCCESS;
  }
private:
  int32_t cnt_;
  int32_t seq_;
};

class BasicTask : public ObITaskNew
{
public:
  BasicTask() : ObITaskNew(0), basic_id_(0) {}
  ~BasicTask() {}
  int process() { return OB_SUCCESS; }
  int basic_id_;
};

class TestAddTask : public ObITaskNew
{
public:
  TestAddTask()
    : ObITaskNew(0), counter_(NULL),
      adder_(), seq_(0), task_cnt_(0), sleep_us_(0)
  {}
  ~TestAddTask() {}
  int init(int64_t *counter, int64_t adder, int64_t seq, int64_t task_cnt, int sleep_us = 0)
  {
    int ret = OB_SUCCESS;
    counter_ = counter;
    adder_ = adder;
    seq_ = seq;
    task_cnt_ = task_cnt;
    sleep_us_ = sleep_us;
    return ret;
  }
  virtual int generate_next_task(ObITaskNew *&next_task)
  {
    int ret = OB_SUCCESS;
    if (seq_ >= task_cnt_ - 1) {
      ret = OB_ITER_END;
      COMMON_LOG(INFO, "generate task end", K_(seq), K_(task_cnt));
    } else {
      ObIDagNew *dag = get_dag();
      TestAddTask *ntask = NULL;
      if (NULL == dag) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "dag is null", K(ret));
      } else if (OB_FAIL(dag->alloc_task(ntask))) {
        COMMON_LOG(WARN, "failed to alloc task", K(ret));
      } else if (NULL == ntask) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "task is null", K(ret));
      } else {
        if (OB_FAIL(ntask->init(counter_, adder_, seq_ + 1, task_cnt_, sleep_us_))) {
          COMMON_LOG(WARN, "failed to init addtask", K(ret));
        }
        if (OB_FAIL(ret)) {
          dag->free_task(*ntask);
        } else {
          next_task = ntask;
        }
      }
    }
    return ret;
  }
  virtual int process()
  {
    ::usleep(sleep_us_);
    (void)ATOMIC_AAF(counter_, adder_);
    return OB_SUCCESS;
  }
  VIRTUAL_TO_STRING_KV(KP_(counter), K_(seq), K_(task_cnt));
private:
  int64_t *counter_;
  int64_t adder_;
  int64_t seq_;
  int64_t task_cnt_;
  int sleep_us_;
private:
  DISALLOW_COPY_AND_ASSIGN(TestAddTask);
};

class TestMulTask : public ObITaskNew
{
public:
  TestMulTask()
    : ObITaskNew(0), counter_(NULL), sleep_us_(0)
  {}
  ~TestMulTask() {}

  int init(int64_t *counter, int sleep_us = 0)
  {
    int ret = OB_SUCCESS;
    counter_ = counter;
    sleep_us_ = sleep_us;
    return ret;
  }
  virtual int process() { ::usleep(sleep_us_); *counter_ = *counter_ * 2; return OB_SUCCESS;}
  VIRTUAL_TO_STRING_KV(KP_(counter));
private:
  int64_t *counter_;
  int sleep_us_;
private:
  DISALLOW_COPY_AND_ASSIGN(TestMulTask);
};

class AtomicOperator
{
public:
  AtomicOperator() : v_(0) {}
  AtomicOperator(int64_t v) : v_(v) {}
  void inc()
  {
    lib::ObMutexGuard guard(lock_);
    ++v_;
  }

  void mul(int64_t m)
  {
    lib::ObMutexGuard guard(lock_);
    v_ *= m;
  }

  int64_t value()
  {
    return v_;
  }
  void reset()
  {
    v_ = 0;
  }
private:
  lib::ObMutex lock_;
  int64_t v_;
};

class AtomicMulTask : public ObITaskNew
{
public:
  AtomicMulTask() :
    ObITaskNew(0), seq_(0), cnt_(0), error_seq_(-1), op_(NULL), sleep_us_(0)
  {}
  int init(int64_t seq, int64_t cnt, AtomicOperator &op, int sleep_us = 0, int64_t error_seq = -1)
  {
    seq_ = seq;
    cnt_ = cnt;
    error_seq_ = error_seq;
    op_ = &op;
    sleep_us_ = sleep_us;
    return OB_SUCCESS;
  }
  virtual int generate_next_task(ObITaskNew *&task)
  {
    int ret = OB_SUCCESS;
    ObIDagNew *dag = NULL;
    AtomicMulTask *ntask = NULL;
    if (seq_ >= cnt_) {
      return OB_ITER_END;
    } else if (OB_ISNULL(dag = get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is NULL", K(ret));
    } else if (OB_FAIL(dag->alloc_task(ntask))){
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (OB_ISNULL(ntask)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "ntask is NULL", K(ret));
    } else {
      COMMON_LOG(INFO, "a task is generated", K(seq_));
      ntask->init(seq_ + 1, cnt_, *op_, sleep_us_, error_seq_);
      task = ntask;
    }
    return ret;
  }
  virtual int process()
  {
    int ret = OB_SUCCESS;
    int64_t cnt = sleep_us_ / SLEEP_SLICE;
    for (int64_t i = 0; i < cnt; ++i) {
      yield();
      ::usleep(SLEEP_SLICE);
    }
    if (seq_ == error_seq_) {
      COMMON_LOG(WARN, "process task meet an error", K(seq_), K(error_seq_));
      ret = OB_ERR_UNEXPECTED;
    } else {
      op_->mul(2);
    }
    return ret;
  }
  VIRTUAL_TO_STRING_KV("type", "AtomicMul", K(*dag_), K_(seq), K_(cnt), KP_(op), K_(error_seq), K_(sleep_us));
private:
  int64_t seq_;
  int64_t cnt_;
  int64_t error_seq_;
  AtomicOperator *op_;
  int sleep_us_;
};

class AtomicIncTask : public ObITaskNew
{
public:
  AtomicIncTask() :
    ObITaskNew(0), seq_(0), cnt_(0), error_seq_(-1), op_(NULL), sleep_us_(0)
  {}
  int init(int64_t seq, int64_t cnt, AtomicOperator &op, int sleep_us = 0, int64_t error_seq = -1)
  {
    seq_ = seq;
    cnt_ = cnt;
    error_seq_ = error_seq;
    op_ = &op;
    sleep_us_ = sleep_us;
    return OB_SUCCESS;
  }
  virtual int generate_next_task(ObITaskNew *&task)
  {
    int ret = OB_SUCCESS;
    ObIDagNew *dag = NULL;
    AtomicIncTask *ntask = NULL;
    if (seq_ >= cnt_) {
      return OB_ITER_END;
    } else if (OB_ISNULL(dag = get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is NULL", K(ret));
    } else if (OB_FAIL(dag->alloc_task(ntask))){
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (OB_ISNULL(ntask)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "ntask is NULL", K(ret));
    } else {
      COMMON_LOG(INFO, "a task is generated", K(seq_));
      ntask->init(seq_ + 1, cnt_, *op_, sleep_us_, error_seq_);
      task = ntask;
    }
    return ret;
  }
  virtual int process()
  {
    int ret = OB_SUCCESS;
    int time_slice = 20;
    int64_t cnt = sleep_us_ / time_slice;
    for (int64_t i = 0; i < cnt; ++i) {
      yield();
      ::usleep(time_slice);
    }
    if (seq_ == error_seq_) {
      COMMON_LOG(WARN, "process task meet an error", K(seq_), K(error_seq_));
      return OB_ERR_UNEXPECTED;
    } else {
      op_->inc();
    }
    return ret;
  }
  VIRTUAL_TO_STRING_KV("type", "AtomicInc", K(dag_), K_(seq), K_(cnt), KP_(op), K_(error_seq), K_(sleep_us));
private:
  int64_t seq_;
  int64_t cnt_;
  int64_t error_seq_;
  AtomicOperator *op_;
  int sleep_us_;
};

template<class T>
int alloc_task(ObIDagNew &dag, T *&task) {
  int ret = OB_SUCCESS;
  task = NULL;
  if (OB_FAIL(dag.alloc_task(task))) {
    COMMON_LOG(WARN, "failed to alloc task", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "task is NULL", K(ret));
  }
  return ret;
}

void wait_scheduler(ObDagSchedulerNew &scheduler) {
  while (!scheduler.is_empty()) {
    ::usleep(1000);
  }
}

class BasicDag : public ObIDagNew
{
public:
  BasicDag()
  : ObIDagNew(0, DAG_PRIO_MAX)
  {
    ObAddr addr(1683068975, 9999);
    if (OB_SUCCESS != (ObSysTaskStatMgr::get_instance().set_self_addr(addr))) {
      COMMON_LOG(WARN, "failed to add sys task", K(addr));
    }
  }
  ~BasicDag()
  {
  }
  void set_type_id(int type_id) { type_id_ = type_id; }
  int64_t hash() const { return murmurhash(&type_id_, sizeof(type_id_), 0); }
  int fill_comment(char *buf, const int64_t size) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || size < 0) {
      COMMON_LOG(INFO, "buf is NULL", K(ret), K(size));
    }
    return ret;
  }
  int64_t get_tenant_id() const { return 0 ;}
  int64_t get_compat_mode() const override { return static_cast<int64_t>(lib::Worker::CompatMode::MYSQL); }
  virtual bool operator == (const ObIDagNew &other) const
  {
    bool bret = false;
    if (get_type_id() == other.get_type_id() && this == &other) {
      bret = true;
    }
    return bret;
  }
};

class TestDag : public ObIDagNew
{
public:
  TestDag() :
      ObIDagNew(0, DAG_PRIO_4), set_id_(0), expect_(-1), expect_ret_(0), running_(false), tester_(NULL) { }
  explicit TestDag(int64_t type_id, ObIDagNewPriority prio) :
      ObIDagNew(type_id, prio), set_id_(0), expect_(-1), expect_ret_(0), running_(false), tester_(NULL) { }
  virtual ~TestDag()
  {
    if (get_dag_status() == ObIDagNew::DAG_STATUS_FINISH
        || get_dag_status() == ObIDagNew::DAG_STATUS_NODE_FAILED) {
      if (running_ && -1 != expect_) {
        if (op_.value() != expect_
            || get_dag_ret() != expect_ret_) {
          if (OB_ALLOCATE_MEMORY_FAILED != get_dag_ret()) {
            if (NULL != tester_) {
              tester_->stop();
            }
            COMMON_LOG(ERROR, "FATAL ERROR!!!", K_(expect), K(op_.value()), K_(expect_ret),
                K(get_dag_ret()), K_(id));
            common::right_to_die_or_duty_to_live();
          }
        }
      }
    }
  }
  int init(int64_t id, int expect_ret = 0, int64_t expect = -1, lib::ThreadPool *tester = NULL)
  {
    set_id_ = id;
    expect_ret_ = expect_ret;
    expect_ = expect;
    tester_ = tester;
    ObAddr addr(1683068975,9999);
    if (OB_SUCCESS != (ObSysTaskStatMgr::get_instance().set_self_addr(addr))) {
      COMMON_LOG(WARN, "failed to add sys task", K(addr));
    }
    return OB_SUCCESS;
  }
  void set_type_id(int type_id) { type_id_ = type_id; }
  virtual int64_t hash() const { return murmurhash(&set_id_, sizeof(set_id_), 0);}
  virtual bool operator == (const ObIDagNew &other) const
  {
    bool bret = false;
    if (get_type_id() == other.get_type_id()) {
      const TestDag &dag = static_cast<const TestDag &>(other);
      bret = set_id_ == dag.set_id_;
    }
    return bret;
  }
  int64_t get_set_id() { return set_id_; }
  AtomicOperator &get_op() { return op_; }
  void set_running() { running_ = true; }
  int64_t get_tenant_id() const { return 0 ;}
  int fill_comment(char *buf,const int64_t size) const {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || size <0) {
      COMMON_LOG(INFO,"buf is NULL",K(ret),K(size));
    }
    return ret;
  }
  int64_t get_compat_mode() const override { return static_cast<int64_t>(lib::Worker::CompatMode::MYSQL); }
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(type_id), K_(id), K(task_list_.get_size()));
  int64_t set_id_;
protected:
  int64_t expect_;
  int expect_ret_;
  AtomicOperator op_;
  bool running_;
  lib::ThreadPool *tester_;
private:
  DISALLOW_COPY_AND_ASSIGN(TestDag);
};

class TestPrepareTask : public ObITaskNew
{
  static const int64_t inc_task_cnt = 8;
  static const int64_t mul_task_cnt = 6;
public:
  TestPrepareTask() : ObITaskNew(0), dag_id_(0), is_error_(false), sleep_us_(0), op_(NULL)
  {}

  int init(int64_t dag_id, AtomicOperator *op = NULL, bool is_error = false, int sleep_us = 0)
  {
    int ret = OB_SUCCESS;
    dag_id_ = dag_id;
    is_error_ = is_error;
    sleep_us_ = sleep_us;
    if (NULL != op) {
      op_ = op;
    } else {
      TestDag *dag = static_cast<TestDag*>(get_dag());
      if (OB_ISNULL(dag)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "dag is null", K(ret));
      } else {
        op_ = &dag->get_op();
      }
    }
    return OB_SUCCESS;
  }

  int process()
  {
    int ret = OB_SUCCESS;
    TestDag*dag = static_cast<TestDag*>(get_dag());
    AtomicIncTask *inc_task = NULL;
    AtomicMulTask *mul_task = NULL;
    AtomicMulTask *mul_task1 = NULL;
    if (OB_ISNULL(dag)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is null", K(ret));
    } else if (OB_FAIL(alloc_task(*dag, inc_task))) {
      COMMON_LOG(WARN, "failed to alloc inc_task", K(ret));
    } else if (OB_FAIL(inc_task->init(1, inc_task_cnt, *op_))) {
    } else if (OB_FAIL(alloc_task(*dag, mul_task))){
      COMMON_LOG(WARN, "failed to alloc mul task", K(ret));
    } else if (OB_FAIL(mul_task->init(1, mul_task_cnt, *op_, 0,
        is_error_ ? 1 + (dag_id_ % mul_task_cnt) : -1))){
    } else if (OB_FAIL(alloc_task(*dag, mul_task1))){
      COMMON_LOG(WARN, "failed to alloc mul task", K(ret));
    } else if (OB_FAIL(mul_task1->init(1, mul_task_cnt, *op_))){
    } else if (OB_FAIL(mul_task->add_child(*inc_task))) {
      COMMON_LOG(WARN, "failed to add child", K(ret));
    } else if (OB_FAIL(mul_task1->add_child(*inc_task))) {
      COMMON_LOG(WARN, "failed to add child", K(ret));
    } else if (OB_FAIL(add_child(*mul_task))) {
      COMMON_LOG(WARN, "failed to add child to self", K(ret));
    } else if (OB_FAIL(add_child(*mul_task1))) {
      COMMON_LOG(WARN, "failed to add child to self", K(ret));
    } else if (OB_FAIL(dag->add_task(*inc_task))) {
      COMMON_LOG(WARN, "failed to add_task", K(ret));
    } else if (OB_FAIL(dag->add_task(*mul_task1))) {
      COMMON_LOG(WARN, "failed to add_task", K(ret));
    } else if (OB_FAIL(dag->add_task(*mul_task))) {
      COMMON_LOG(WARN, "failed to add_task", K(ret));
    } else {
      dag->set_running();
    }
    if (sleep_us_ > 0) {
      ::usleep(sleep_us_);
      if (is_error_) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
    return ret;
  }
private:
  int64_t dag_id_;
  bool is_error_;
  int sleep_us_;
  AtomicOperator *op_;
};

class TestCyclePrepare : public ObITaskNew
{
public:
  TestCyclePrepare()
    : ObITaskNew(0), op_(NULL) {}
  int init(AtomicOperator *op = NULL)
  {
    int ret = OB_SUCCESS;
    if (NULL != op) {
      op_ = op;
    } else {
      TestDag *dag = static_cast<TestDag*>(get_dag());
      if (OB_ISNULL(dag)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "dag is null", K(ret));
      } else {
        op_ = &dag->get_op();
      }
    }
    return OB_SUCCESS;
  }

  int process()
  {
    int ret = OB_SUCCESS;
    TestDag*dag = static_cast<TestDag*>(get_dag());
    AtomicIncTask *inc_task = NULL;
    AtomicMulTask *mul_task = NULL;
    AtomicMulTask *mul_task1 = NULL;
    if (OB_ISNULL(dag)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is null", K(ret));
    } else if (OB_FAIL(alloc_task(*dag, inc_task))) {
      COMMON_LOG(WARN, "failed to alloc inc_task", K(ret));
    } else if (OB_FAIL(inc_task->init(1, 5, *op_))) {
    } else if (OB_FAIL(alloc_task(*dag, mul_task))){
      COMMON_LOG(WARN, "failed to alloc mul task", K(ret));
    } else if (OB_FAIL(mul_task->init(1, 5, *op_))){
    } else if (OB_FAIL(alloc_task(*dag, mul_task1))){
      COMMON_LOG(WARN, "failed to alloc mul task", K(ret));
    } else if (OB_FAIL(mul_task1->init(1, 5, *op_))){
    } else if (OB_FAIL(mul_task->add_child(*inc_task))) {
      COMMON_LOG(WARN, "failed to add child", K(ret));
    } else if (OB_FAIL(mul_task1->add_child(*inc_task))) {
      COMMON_LOG(WARN, "failed to add child", K(ret));
    } else if (OB_FAIL(inc_task->add_child(*mul_task))) {
      COMMON_LOG(WARN, "failed to add child", K(ret));
    } else if (OB_FAIL(add_child(*mul_task))) {
      COMMON_LOG(WARN, "failed to add child to self", K(ret));
    } else if (OB_FAIL(add_child(*mul_task1))) {
      COMMON_LOG(WARN, "failed to add child to self", K(ret));
    } else if (OB_FAIL(dag->add_task(*inc_task))) {
      COMMON_LOG(WARN, "failed to add_task", K(ret));
    } else if (OB_FAIL(dag->add_task(*mul_task1))) {
      COMMON_LOG(WARN, "failed to add_task", K(ret));
    } else if (OB_FAIL(dag->add_task(*mul_task))) {
      //COMMON_LOG(WARN, "KKKKKK", K(*mul_task), K(*mul_task1), K(*inc_task));
      dag->free_task(*mul_task);
      //COMMON_LOG(WARN, "failed to add_task", K(ret));
    }
    return ret;
  }
private:
  AtomicOperator *op_;
};

// for test_switch_task
class SwitchFromTask : public ObITaskNew
{
public:
  SwitchFromTask() : ObITaskNew(0), flag_(NULL) {}
  ~SwitchFromTask() {}
  void init(int *flag) { flag_ = flag; }
  int process()
  {
    int ret = OB_SUCCESS;
    while (flag_ && 1 != *flag_) {
      yield();
      ::usleep(SLEEP_SLICE * 3);
    }
    return ret;
  }
private:
  int *flag_;
};
// for test_switch_task
class SwitchToTask : public ObITaskNew
{
public:
  SwitchToTask() : ObITaskNew(0), flag_(NULL) {}
  ~SwitchToTask() {}
  void init(int *flag) { flag_ = flag; }
  int process()
  {
    int ret = OB_SUCCESS;
    if (flag_) {
      *flag_ = 1;
    }
    return ret;
  }
private:
  int *flag_;
};

class DagSchedulerStressTester : public lib::ThreadPool
{
  static const int64_t STRESS_THREAD_NUM = 16;
public:
  DagSchedulerStressTester()
  : dag_cnt_(0),
    test_time_(0),
    tenant_id_(0),
    max_type_id_(0)
  {
  }

  int init(int64_t test_time)
  {
    test_time_ = test_time * 1000;
    return OB_SUCCESS;
  }

  int do_stress()
  {
    int ret = OB_SUCCESS;
    set_thread_count(STRESS_THREAD_NUM);
    int64_t start_time = ObTimeUtility::current_time();
    start();
    ObDagSchedulerNew::get_instance().start_run();
    wait();
    int64_t elapsed_time = ObTimeUtility::current_time() - start_time;
    COMMON_LOG(INFO, "stress test finished", K(elapsed_time / 1000));
   /* int ret_code = system("grep ERROR test_tenant_dag_scheduler.log -q");
    ret_code = WEXITSTATUS(ret_code);
    if (ret_code == 0)
      ret = OB_ERR_UNEXPECTED;
      */
    return ret;
  }

  void run1()
  {
    int64_t start_time = ObTimeUtility::current_time();
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
    while (!has_set_stop() && OB_SUCC(ret)
        && (ObTimeUtility::current_time() - start_time < test_time_)) {
      const int64_t dag_id = get_dag_id();
      TestDag *dag = NULL;
      TestPrepareTask *task = NULL;
      int expect_ret = (dag_id % 10 == 0 ? OB_ERR_UNEXPECTED : OB_SUCCESS);
      int64_t expect_value = (dag_id % 10 == 0 ? 0 : 8);

      if (OB_SUCCESS != (tmp_ret = scheduler.alloc_dag(tenant_id_, dag))) {
        if (OB_ALLOCATE_MEMORY_FAILED != tmp_ret) {
          ret = tmp_ret;
          COMMON_LOG(ERROR, "failed to allocate dag", K(ret));
        } else {
          COMMON_LOG(WARN, "out of memory");
        }
      } else {
        dag->set_priority((ObIDagNew::ObIDagNewPriority)(dag_id % ObIDagNew::DAG_PRIO_MAX));
        dag->set_type_id(dag_id % max_type_id_);
      }

      if (OB_SUCCESS != tmp_ret) {
        continue;
      }

      if (OB_FAIL(dag->init(dag_id, expect_ret, expect_value, this))) {
        COMMON_LOG(WARN, "failed to init dag", K(ret));
      } else if (OB_SUCCESS != (tmp_ret = alloc_task(*dag, task))) {
        COMMON_LOG(WARN, "failed to alloc task", K(tmp_ret));
      } else if (OB_FAIL(task->init(dag_id, NULL, expect_ret != OB_SUCCESS))) {
        COMMON_LOG(WARN, "failed to init task", K(ret));
      } else if (OB_FAIL(dag->add_task(*task))) {
        COMMON_LOG(WARN, "failed to add task", K(ret));
      } else {
        if (OB_SUCCESS != (tmp_ret = scheduler.add_dag_in_tenant(tenant_id_, dag))) {
          if (OB_SIZE_OVERFLOW != tmp_ret) {
            COMMON_LOG(ERROR, "failed to add dag", K(tmp_ret), K(*dag));
          }
          scheduler.free_dag(tenant_id_, dag);
        } else {
          ++dag_cnt_;
        }
      }
    }
  }

  int64_t get_dag_id()
  {
    return ATOMIC_FAA(&counter_, 1);
  }
  void set_tenant_id(const int64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_max_type_id(const int64_t type_id) { max_type_id_ = type_id; }
  int64_t get_dag_cnt() const { return dag_cnt_; }
private:
  static int64_t counter_;
  int64_t dag_cnt_;
  int64_t test_time_;
  int64_t tenant_id_;
  int64_t max_type_id_;
};

int64_t DagSchedulerStressTester::counter_ = 0;

class TestDagScheduler : public ::testing::Test
{
public:
  TestDagScheduler() {}
  ~TestDagScheduler() {}
  void SetUp()
  {
    ObUnitInfoGetter::ObTenantConfig unit_config;
    unit_config.mode_ = lib::Worker::CompatMode::MYSQL;
    unit_config.tenant_id_ = 0;
    TenantUnits units;
    ASSERT_EQ(OB_SUCCESS, units.push_back(unit_config));
    //ASSERT_EQ(OB_SUCCESS, ObTenantNodeBalancer::get_instance().load_tenant(units));
  }
  void TearDown() {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestDagScheduler);
};

/*
TEST_F(TestDagScheduler, test_init)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  int64_t schedule_period = 10000;
  // invalid thread cnt
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.init(schedule_period, -1));
  // invalid dag_limit
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.init(schedule_period, 0, 0));
  // invalid total_mem_limit
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.init(schedule_period, 0, 10,0));
  // invalid hold_mem_limit
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.init(schedule_period, 10,1024, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.init(schedule_period, 10,1024, 2048));

  EXPECT_EQ(OB_SUCCESS, scheduler.init(schedule_period, 100, 50));
  EXPECT_EQ(OB_INIT_TWICE, scheduler.init());
  scheduler.destroy();
}

TEST_F(TestDagScheduler, test_sort_list_task)
{
  ObSortList<ObITaskNew> task_list;
  EXPECT_EQ(true, task_list.is_empty());

  int size = 5;
  TypeIdTask * task[size];
  const int type_ids[] = {4, 2, 5, 1, 3};
  const int64_t sort_ids[] = {5,4,3,2,1};
  for (int i = 0; i < size; ++i) {
    task[i] = new TypeIdTask();
    task[i]->set_type_id(type_ids[i]);
  }
  for (int i = 0; i < size; ++i) {
    task_list.insert(task[i]);
  }
  // get list to check sort list
  common::ObDList<ObITaskNew>& list = task_list.get_list();
  ObITaskNew * cur = list.get_first();
  ObITaskNew * head = list.get_header();
  int index = 0;
  while (NULL != cur && head != cur){
    EXPECT_EQ(sort_ids[index++], cur->get_type_id());
    cur = cur->get_next();
  }
  EXPECT_EQ(true, task_list.find(task[2]));

  // new task
  TypeIdTask * other_task = new TypeIdTask();
  other_task->set_type_id(888);
  EXPECT_EQ(false, task_list.find(other_task));
  EXPECT_EQ(size, task_list.get_size());
  task_list.insert(other_task);
  EXPECT_EQ(size + 1, task_list.get_size());
  EXPECT_EQ(true, task_list.find(other_task));
  EXPECT_EQ(other_task->get_type_id(), task_list.get_first()->get_type_id());
  // test remove
  EXPECT_EQ(true, task_list.remove(other_task));
  EXPECT_EQ(sort_ids[0], task_list.get_first()->get_type_id());
  EXPECT_EQ(false, task_list.remove(other_task));
  delete(other_task);

  task_list.reset();
  for (int i = 0; i < size; ++i) {
    delete(task[i]);
  }
}

TEST_F(TestDagScheduler, test_sort_list_dynamic_score)
{
  typedef ObTenantThreadPool::DynamicScore DynamicScore;
  ObSortList<DynamicScore> dynamic_score_list;
  int size = 5;
  DynamicScore *score_list[size];
  const int scores[] = {4, 2, 5, 1, 3};
  const int64_t sort_scores[] = {5, 4, 3, 2, 1};
  for (int i = 0; i < size; ++i) {
    score_list[i] = new DynamicScore();
    score_list[i]->d_score_ = scores[i] * 1.8;
  }
  for (int i = 0; i < size; ++i) {
    dynamic_score_list.insert(score_list[i]);
  }
  common::ObDList<DynamicScore>& list = dynamic_score_list.get_list();
  DynamicScore * cur = list.get_first();
  DynamicScore * head = list.get_header();
  int index = 0;
  while (NULL != cur && head != cur){
    EXPECT_EQ(sort_scores[index++] * 1.8, cur->d_score_);
    cur = cur->get_next();
  }
  EXPECT_EQ(true, dynamic_score_list.find(score_list[1]));
  EXPECT_EQ(true, dynamic_score_list.find(score_list[3]));
  // test adjust_last
  dynamic_score_list.get_last()->d_score_ = 999;
  dynamic_score_list.adjust(dynamic_score_list.get_last());
  EXPECT_EQ(999, dynamic_score_list.get_first()->d_score_);

  dynamic_score_list.reset();
  for (int i = 0; i < size; ++i) {
    delete(score_list[i]);
  }
}

TEST_F(TestDagScheduler, test_worker_obj_pool)
{
  ObWorkerObjPool &obj_pool = ObWorkerObjPool::get_instance();
  obj_pool.destroy();
  // test init
  obj_pool.init();
  EXPECT_EQ(0, obj_pool.get_init_worker_cnt());
  EXPECT_EQ(0, obj_pool.get_worker_cnt());
  // empty pool to get worker obj
  ObDagWorkerNew * worker = NULL;
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, obj_pool.get_worker_obj(worker));

  int size = 5;
  int32_t cnt[] = {1, 5, 15, 9, 10};
  int32_t tmp = 0;
  for (int i = 0; i < size; ++i) {
    EXPECT_EQ(OB_SUCCESS, obj_pool.set_init_worker_cnt_inc(cnt[i]));
    tmp += cnt[i];
    EXPECT_EQ(tmp, obj_pool.get_init_worker_cnt());
    EXPECT_EQ(OB_SUCCESS, obj_pool.adjust_worker_cnt());
    EXPECT_EQ(static_cast<int>(tmp * ObWorkerObjPool::OBJ_CREATE_PERCENT), obj_pool.get_worker_cnt());
  }

  int32_t neg_cnt[] = {-3, -2, -8, -7, -15};
  int last_worker_cnt = obj_pool.get_init_worker_cnt() * ObWorkerObjPool::OBJ_CREATE_PERCENT;
  for (int i = 0; i < size; ++i) {
    EXPECT_EQ(OB_SUCCESS, obj_pool.set_init_worker_cnt_inc(neg_cnt[i]));
    tmp = tmp + neg_cnt[i];
    EXPECT_EQ(tmp, obj_pool.get_init_worker_cnt());
    EXPECT_EQ(OB_SUCCESS, obj_pool.adjust_worker_cnt());
    if (tmp * ObWorkerObjPool::OBJ_RELEASE_PERCENT < last_worker_cnt) {
      EXPECT_EQ(static_cast<int>(tmp * ObWorkerObjPool::OBJ_RELEASE_PERCENT), obj_pool.get_worker_cnt());
      last_worker_cnt = tmp * ObWorkerObjPool::OBJ_RELEASE_PERCENT;
    } else {
      EXPECT_EQ(last_worker_cnt, obj_pool.get_worker_cnt());
    }
  }
  // test get_worker_obj & release_worker_obj
  EXPECT_EQ(OB_SUCCESS, obj_pool.set_init_worker_cnt_inc(1 - tmp));
  EXPECT_EQ(1, obj_pool.get_init_worker_cnt());
  EXPECT_EQ(OB_SUCCESS, obj_pool.adjust_worker_cnt());
  EXPECT_EQ(OB_SUCCESS, obj_pool.get_worker_obj(worker));
  EXPECT_EQ(true, !OB_ISNULL(worker));
  EXPECT_EQ(OB_SUCCESS, obj_pool.release_worker_obj(worker));
  int uplimit = ObWorkerObjPool::OBJ_UP_LIMIT_PERCENT;
  ObDagWorkerNew * workerList[uplimit];
  for (int i = 0; i < uplimit; ++i) {
    EXPECT_EQ(OB_SUCCESS, obj_pool.get_worker_obj(workerList[i]));
  }
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, obj_pool.get_worker_obj(worker));
  for (int i = 0; i < uplimit; ++i) {
    EXPECT_EQ(OB_SUCCESS, obj_pool.release_worker_obj(workerList[i]));
  }
  obj_pool.destroy();
}

TEST_F(TestDagScheduler, add_tenant)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init());
  ObWorkerObjPool::get_instance().destroy();
  ObWorkerObjPool::get_instance().init();
  int size = 5;
  int64_t tenant_id_list[] = {111, 222, 333, 444, 555};
  int64_t other_tenant = 999;
  // test add tenant
  for (int i = 0; i < size; ++i) {
    EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id_list[i]));
  }
  EXPECT_EQ(OB_ENTRY_EXIST, scheduler.add_tenant_thread_pool(tenant_id_list[2]));
  // test set max thread number
  int max_thread_num = 8;
  common::ObArray<ObTenantSetting> tenant_settings;
  tenant_settings.push_back(ObTenantSetting(tenant_id_list[0], max_thread_num));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_settings));
  EXPECT_EQ(ObWorkerObjPool::get_instance().get_init_worker_cnt(), max_thread_num);
  max_thread_num = 3;
  tenant_settings.reset();
  tenant_settings.push_back(ObTenantSetting(tenant_id_list[0], max_thread_num));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_settings));
  EXPECT_EQ(ObWorkerObjPool::get_instance().get_init_worker_cnt(), max_thread_num);
  // non-existent tenant
  tenant_settings.reset();
  tenant_settings.push_back(ObTenantSetting(other_tenant, max_thread_num));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, scheduler.set_tenant_setting(tenant_settings));
  scheduler.destroy();
  ObWorkerObjPool::get_instance().destroy();
}

TEST_F(TestDagScheduler, del_tenant)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init());
  ObWorkerObjPool::get_instance().destroy();
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 8127;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int max_thread_num = 2;
  common::ObArray<ObTenantSetting> tenant_settings;
  tenant_settings.push_back(ObTenantSetting(tenant_id, max_thread_num));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_settings));

  BasicDag * dag = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag)); // alloc dag
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.add_dag_in_tenant(tenant_id, dag)); // empty dag
  AtomicMulTask * mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, dag->alloc_task(mul_task));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag));
  EXPECT_EQ(OB_SUCCESS, scheduler.del_tenant_thread_pool(tenant_id));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, scheduler.alloc_dag(tenant_id, dag));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, scheduler.set_tenant_setting(tenant_settings));

  scheduler.destroy();
  ObWorkerObjPool::get_instance().destroy();
}

TEST_F(TestDagScheduler, add_dag)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init());
  ObWorkerObjPool::get_instance().destroy();
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 123;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int max_thread_num = 2;
  common::ObArray<ObTenantSetting> tenant_settings;
  tenant_settings.push_back(ObTenantSetting(tenant_id, max_thread_num));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_settings));
  int size = 1;
  BasicDag *dag[size];
  BasicTask * task[size];
  int ret = OB_SUCCESS;
  for (int i = 0; i < size; ++i) {
    if (OB_FAIL(scheduler.alloc_dag(tenant_id, dag[i]))) { // alloc dag
      COMMON_LOG(WARN, "failed to alloc dag");
    } else if (NULL == dag[i]) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is null", K(ret));
    } else {
      if (OB_FAIL(dag[i]->alloc_task(task[i]))) {
        COMMON_LOG(WARN, "failed to alloc task", K(ret));
      } else if (NULL == task[i]) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "task is null", K(ret));
      } else {
        dag[i]->add_task(*task[i]); // task_list_ of dag can't be empty
        dag[i]->set_priority((ObIDagNew::ObIDagNewPriority)i);
        EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag[i]));
        EXPECT_EQ(i + 1, scheduler.get_dag_count_in_tenant(tenant_id, 0));
      }
    }
  }


  BasicDag * invalid_dag = NULL;
  if (OB_FAIL(scheduler.alloc_dag(tenant_id, invalid_dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else {
    invalid_dag->set_type_id(888);
    EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.add_dag_in_tenant(tenant_id, invalid_dag)); // invalid type id
    scheduler.free_dag(tenant_id, invalid_dag);
  }
  EXPECT_EQ(OB_SUCCESS, scheduler.start_run());
  wait_scheduler(scheduler);
  scheduler.destroy();
  ObWorkerObjPool::get_instance().destroy();
}
*/

/*
TEST_F(TestDagScheduler, test_add_type)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init(ObAddr(2, 2)));
  ObWorkerObjPool::get_instance().destroy();
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 123;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int type_id = 0;
  common::ObArray<ObDagSchedulerNew::TypeBasicSetting> type_array;
  type_array.push_back(ObDagSchedulerNew::TypeBasicSetting(type_id, 2));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_types(type_array));

  EXPECT_EQ(OB_EAGAIN, scheduler.add_types(type_array)); // same type id

  int invalid_type_id = -1;
  type_array.reset();
  type_array.push_back(ObDagSchedulerNew::TypeBasicSetting(invalid_type_id, 2));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.add_types(type_array));

  type_array.reset();
  type_array.push_back(ObDagSchedulerNew::TypeBasicSetting(type_id + 1, -2));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.add_types(type_array));

  type_array.reset();
  type_array.push_back(ObDagSchedulerNew::TypeBasicSetting(type_id + 2, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.add_types(type_array));

  type_array.reset();
  type_array.push_back(ObDagSchedulerNew::TypeBasicSetting(type_id + 3, 2));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_types(type_array));

  scheduler.destroy();
}

TEST_F(TestDagScheduler, test_set_type_score)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init(ObAddr(2, 2)));
  ObWorkerObjPool::get_instance().destroy();
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 123;
  int64_t score = 0;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int64_t type_id = 0;
  int64_t invalid_type_id = 999;

  EXPECT_EQ(OB_SUCCESS, scheduler.get_type_score_in_tenant(tenant_id, type_id, score));
  EXPECT_EQ(2, score);
  EXPECT_EQ(OB_SUCCESS, scheduler.set_type_score_in_tenant(tenant_id, type_id, 8));
  EXPECT_EQ(OB_SUCCESS, scheduler.get_type_score_in_tenant(tenant_id, type_id, score));
  EXPECT_EQ(8, score);
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.set_type_score_in_tenant(tenant_id, type_id, -1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.set_type_score_in_tenant(tenant_id, invalid_type_id, 6));
  int invalid_tenant_id = 999;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, scheduler.set_type_score_in_tenant(invalid_tenant_id, type_id, 8));
  scheduler.destroy();
}

TEST_F(TestDagScheduler, test_set_type_uplimit)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init(ObAddr(2, 2)));
  ObWorkerObjPool::get_instance().destroy();
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 123;
  int64_t uplimit = 0;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int64_t type_id = 0;
  int64_t invalid_type_id = 999;
  EXPECT_EQ(OB_SUCCESS, scheduler.get_type_uplimit_in_tenant(tenant_id, type_id, uplimit));
  EXPECT_EQ(INT_MAX, uplimit);
  EXPECT_EQ(OB_SUCCESS, scheduler.set_type_uplimit_in_tenant(tenant_id, type_id, 8));
  EXPECT_EQ(OB_SUCCESS, scheduler.get_type_uplimit_in_tenant(tenant_id, type_id, uplimit));
  EXPECT_EQ(8, uplimit);
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.set_type_uplimit_in_tenant(tenant_id, type_id, -1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.set_type_uplimit_in_tenant(tenant_id, invalid_type_id, 6));
  int invalid_tenant_id = 999;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, scheduler.set_type_uplimit_in_tenant(invalid_tenant_id, type_id, 8));
  scheduler.destroy();
  ObWorkerObjPool::get_instance().destroy();
}

TEST_F(TestDagScheduler, test_set_type_setting)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init(ObAddr(2, 2)));
  ObWorkerObjPool::get_instance().destroy();
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 123;
  int64_t uplimit = 0;
  int64_t score = 0;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int64_t type_id = 0;
  int64_t invalid_type_id = 999;

  common::ObArray<ObDagSchedulerNew::TypeTenantSetting> tenant_setting_array;
  // set score
  int set_score = 88;
  tenant_setting_array.push_back(ObDagSchedulerNew::TypeTenantSetting(tenant_id, type_id, set_score));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_type_setting(tenant_setting_array));

  EXPECT_EQ(OB_SUCCESS, scheduler.get_type_uplimit_in_tenant(tenant_id, type_id, uplimit));
  EXPECT_EQ(INT_MAX, uplimit);
  EXPECT_EQ(OB_SUCCESS, scheduler.get_type_score_in_tenant(tenant_id, type_id, score));
  EXPECT_EQ(set_score, score);
  // set score and uplimit
  int set_up_limit = 10;
  tenant_setting_array.push_back(ObDagSchedulerNew::TypeTenantSetting(tenant_id, type_id, set_score, set_up_limit));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_type_setting(tenant_setting_array));
  EXPECT_EQ(OB_SUCCESS, scheduler.get_type_uplimit_in_tenant(tenant_id, type_id, uplimit));
  EXPECT_EQ(set_up_limit, uplimit);
  EXPECT_EQ(OB_SUCCESS, scheduler.get_type_score_in_tenant(tenant_id, type_id, score));
  EXPECT_EQ(set_score, score);
  // two tenant
  int64_t tenant_id2 = 8878;
  tenant_setting_array.reset();
  tenant_setting_array.push_back(ObDagSchedulerNew::TypeTenantSetting(tenant_id, type_id, set_score, set_up_limit));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_type_setting(tenant_setting_array));
  tenant_setting_array.reset();
  tenant_setting_array.push_back(ObDagSchedulerNew::TypeTenantSetting(tenant_id2, type_id, set_score, set_up_limit));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, scheduler.set_type_setting(tenant_setting_array));
  // add tenant
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id2));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_type_setting(tenant_setting_array));
  // invalid score
  tenant_setting_array.reset();
  int invalid_score = -1;
  tenant_setting_array.push_back(ObDagSchedulerNew::TypeTenantSetting(tenant_id, type_id, invalid_score, set_up_limit));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.set_type_setting(tenant_setting_array));
  // invalid uplimit
  int invalid_uplimit = 0;
  tenant_setting_array.reset();
  tenant_setting_array.push_back(ObDagSchedulerNew::TypeTenantSetting(tenant_id, type_id, set_score, invalid_uplimit));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.set_type_setting(tenant_setting_array));

  scheduler.destroy();
}
*/

/*
TEST_F(TestDagScheduler, test_set_tenant_setting)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init());
  ObWorkerObjPool::get_instance().destroy();
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 123;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int64_t type_id = 0;
  int64_t invalid_type_id = 999;

  common::ObArray<ObTenantSetting> tenant_setting_array;
  int max_thread_num = 3;
  // set score
  int set_score = 88;
  ObTenantSetting tenant_setting(tenant_id, max_thread_num);
  tenant_setting.type_settings_.push_back(
    ObTenantTypeSetting(type_id, set_score));
  tenant_setting_array.push_back(tenant_setting);

  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_setting_array));

  ObTenantSetting get_tenant_setting;
  EXPECT_EQ(OB_SUCCESS, scheduler.get_tenant_setting_by_id(tenant_id, get_tenant_setting));
  EXPECT_EQ(ObDagTypeIds::TYPE_SETTING_END, get_tenant_setting.type_settings_.size());

  EXPECT_EQ(INT_MAX, get_tenant_setting.type_settings_.at(type_id).up_limit_);
  EXPECT_EQ(set_score, get_tenant_setting.type_settings_.at(type_id).score_);
  // set score and uplimit
  int set_up_limit = 10;
  tenant_setting_array.reset();
  tenant_setting.type_settings_.clear();
  tenant_setting.type_settings_.push_back(
      ObTenantTypeSetting(type_id, set_score, set_up_limit));
  tenant_setting_array.push_back(tenant_setting);
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_setting_array));

  get_tenant_setting.type_settings_.clear();
  EXPECT_EQ(OB_SUCCESS, scheduler.get_tenant_setting_by_id(tenant_id, get_tenant_setting));
  EXPECT_EQ(ObDagTypeIds::TYPE_SETTING_END, get_tenant_setting.type_settings_.size());

  EXPECT_EQ(set_up_limit, get_tenant_setting.type_settings_.at(type_id).up_limit_);
  EXPECT_EQ(set_score, get_tenant_setting.type_settings_.at(type_id).score_);
  // two tenant
  int64_t tenant_id2 = 8878;
  tenant_setting_array.reset();
  tenant_setting.tenant_id_ = tenant_id2;
  tenant_setting_array.push_back(tenant_setting);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, scheduler.set_tenant_setting(tenant_setting_array));

  // add tenant
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id2));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_setting_array));
  // invalid score
  int invalid_score = -1;
  tenant_setting_array.reset();
  tenant_setting.type_settings_.clear();
  tenant_setting.type_settings_.push_back(
    ObTenantTypeSetting(type_id, invalid_score, set_up_limit));
  tenant_setting_array.push_back(tenant_setting);
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.set_tenant_setting(tenant_setting_array));
  // invalid uplimit
  int invalid_uplimit = 0;
  tenant_setting_array.reset();
  tenant_setting.type_settings_.clear();
  tenant_setting.type_settings_.push_back(
    ObTenantTypeSetting(type_id, set_score, invalid_uplimit));
  tenant_setting_array.push_back(tenant_setting);
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.set_tenant_setting(tenant_setting_array));
  // invalid max_thread_num
  tenant_setting_array.reset();
  tenant_setting.max_thread_num_ = 0;
  tenant_setting.type_settings_.clear();
  tenant_setting_array.push_back(tenant_setting);
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.set_tenant_setting(tenant_setting_array));
  scheduler.destroy();
}

TEST_F(TestDagScheduler, test_cycle)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init());
  ObWorkerObjPool::get_instance().destroy();
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 111;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int ret = OB_SUCCESS;
  TestDag *dag = NULL;
  int64_t counter = 0;
  if (OB_FAIL(scheduler.alloc_dag(tenant_id, dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else {
    TestAddTask *add_task = NULL;
    TestMulTask *mul_task = NULL;
    if (OB_FAIL(dag->alloc_task(mul_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == mul_task) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else {
      if (OB_FAIL(mul_task->init(&counter))) {
        COMMON_LOG(WARN, "failed to init add task", K(ret));
      }
      EXPECT_EQ(OB_INVALID_ARGUMENT, mul_task->add_child(*mul_task)); // test self loop
      if (OB_SUCC(ret)) {
        if (OB_FAIL(dag->alloc_task(add_task))) {
          COMMON_LOG(WARN, "failed to alloc task", K(ret));
        } else if (NULL == add_task) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "task is null", K(ret));
        } else {
          add_task->init(&counter, 1, 0, 3);
          add_task->add_child(*mul_task);
          mul_task->add_child(*add_task); // mul->add  add->mul cycle
        }
        EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
        COMMON_LOG(INFO, "mul_task", K(mul_task));
        COMMON_LOG(INFO, "add_task", K(add_task));
        EXPECT_EQ(OB_INVALID_ARGUMENT, dag->add_task(*add_task));
        dag->free_task(*add_task);
        scheduler.free_dag(tenant_id, dag);
      }
    }
  }
  TestCyclePrepare *prepare_task = NULL;
  AtomicOperator op;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag)); // dag:TestDag type_id=0
  int64_t type_id = 0;
  int max_thread_num = 1;
  common::ObArray<ObTenantSetting> tenant_settings;
  tenant_settings.push_back(ObTenantSetting(tenant_id, max_thread_num));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_settings));
  // init dag
  EXPECT_EQ(OB_SUCCESS, dag->init(1, OB_INVALID_ARGUMENT, 0));
  EXPECT_EQ(OB_SUCCESS, dag->alloc_task(prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(&op));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag));
  EXPECT_EQ(OB_SUCCESS, scheduler.start_run());
  wait_scheduler(scheduler);
  EXPECT_EQ(0, op.value());
  scheduler.destroy();
  ObWorkerObjPool::get_instance().destroy();
}

TEST_F(TestDagScheduler, test_task_list_op)
{
  ObTenantThreadPool::TaskList list(0, NULL, ObModIds::OB_SCHEDULER);
  ObTenantThreadPool pool(888);
  pool.init();
  int size = 10;
  BasicTask * task[size];
  for (int i = 0; i < size; ++i) {
    task[i] = new BasicTask();
    task[i]->basic_id_ = i;
    list.push_back(task[i]);
  }
  ObTenantThreadPool::TaskListIterator found_iter = NULL;
  EXPECT_EQ(OB_SUCCESS, pool.find_in_task_list_(task[2], &list, found_iter));
  EXPECT_EQ(2, static_cast<BasicTask *>(*found_iter)->basic_id_);
  EXPECT_EQ(OB_SUCCESS, pool.find_in_task_list_(task[1], &list, found_iter));
  EXPECT_EQ(1, static_cast<BasicTask *>(*found_iter)->basic_id_);
  EXPECT_EQ(size, list.count());
  EXPECT_EQ(OB_SUCCESS, pool.remove_from_task_list_(task[2], &list));
  EXPECT_EQ(size - 1, list.count());
  EXPECT_EQ(OB_SUCCESS, pool.remove_from_task_list_(task[0], &list));
  EXPECT_EQ(size - 2, list.count());
  found_iter = NULL;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, pool.find_in_task_list_(task[2], &list, found_iter));
  for (int i = 0; i < size; ++i) {
    delete(task[i]);
  }
  pool.destroy();
}

TEST_F(TestDagScheduler, basic_test)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init(500));
  ObWorkerObjPool::get_instance().destroy();
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 879;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int64_t type_id = 0;
  int max_thread_num = 10;
  common::ObArray<ObTenantSetting> tenant_settings;
  tenant_settings.push_back(ObTenantSetting(tenant_id, max_thread_num));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_settings));
  int ret = OB_SUCCESS;
  TestDag *dag = NULL;
  TestDag *dup_dag = NULL;
  int64_t counter = 0;
  //simple two-level dag
  if (OB_FAIL(scheduler.alloc_dag(tenant_id, dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag->init(8))) {
    COMMON_LOG(WARN, "failed to init dag", K(ret));
  } else {
    TestAddTask *add_task = NULL;
    TestMulTask *mul_task = NULL;
    if (OB_FAIL(dag->alloc_task(mul_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == mul_task) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else {
      if (OB_FAIL(mul_task->init(&counter))) {
        COMMON_LOG(WARN, "failed to init add task", K(ret));
      } else if (OB_FAIL(dag->add_task(*mul_task))) {
        COMMON_LOG(WARN, "failed to add task", K(ret));
      }
      if (OB_FAIL(ret)) {
        dag->free_task(*mul_task);
      } else {
        if (OB_FAIL(dag->alloc_task(add_task))) {
          COMMON_LOG(WARN, "failed to alloc task", K(ret));
        } else if (NULL == add_task) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "task is null", K(ret));
        } else {
          if (OB_FAIL(add_task->init(&counter, 1, 0, 10, 200 * 1000))) {
            COMMON_LOG(WARN, "failed to init add task", K(ret));
          } else if (OB_FAIL(add_task->add_child(*mul_task))) {
            COMMON_LOG(WARN, "failed to add child", K(ret));
          } else if (OB_FAIL(dag->add_task(*add_task))) {
            COMMON_LOG(WARN, "failed to add task");
          }
          if (OB_FAIL(ret)) {
            dag->free_task(*add_task);
          }
        }
      }
    }
  }
  // check deduplication functionality
  if (OB_FAIL(scheduler.alloc_dag(tenant_id, dup_dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dup_dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dup_dag->init(8))) {
    COMMON_LOG(WARN, "failed to init dag", K(ret));
  } else {
    TestMulTask *mul_task = NULL;
    if (OB_FAIL(dup_dag->alloc_task(mul_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == mul_task) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else {
      if (OB_FAIL(mul_task->init(&counter))) {
        COMMON_LOG(WARN, "failed to init add task", K(ret));
      } else if (OB_FAIL(dup_dag->add_task(*mul_task))) {
        COMMON_LOG(WARN, "failed to add task", K(ret));
      }
      if (OB_FAIL(ret)) {
        dag->free_task(*mul_task);
      }
    }
  }
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag));
  EXPECT_EQ(OB_EAGAIN, scheduler.add_dag_in_tenant(tenant_id, dup_dag)); // same hash value and same id_
  scheduler.free_dag(tenant_id, dup_dag);
  EXPECT_EQ(OB_SUCCESS, scheduler.start_run());
  wait_scheduler(scheduler);
  EXPECT_EQ(counter, 20);

  // three level dag that each level would generate dynamic tasks
  AtomicOperator op(0);
  TestDag *dag1 = NULL;
  AtomicIncTask *inc_task = NULL;
  AtomicIncTask *inc_task1 = NULL;
  AtomicMulTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 4, op));
  EXPECT_EQ(OB_SUCCESS, mul_task->add_child(*inc_task));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task1));
  EXPECT_EQ(OB_SUCCESS, inc_task1->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, inc_task1->add_child(*mul_task));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task1));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag1));
  wait_scheduler(scheduler);
  EXPECT_EQ(170, op.value());

  // two-level dag with 2 tasks on the first level
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  //add mul task
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 4, op));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*mul_task));
  // add inc task
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task1));
  EXPECT_EQ(OB_SUCCESS, inc_task1->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, inc_task1->add_child(*mul_task));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task1));
  // add another inc task
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task1));
  EXPECT_EQ(OB_SUCCESS, inc_task1->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, inc_task1->add_child(*mul_task));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task1));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag1));
  wait_scheduler(scheduler);
  EXPECT_EQ(320, op.value());

  // a dag with single task which generate all other tasks while processing
  TestPrepareTask *prepare_task = NULL;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag1));
  wait_scheduler(scheduler);
  EXPECT_EQ(8, op.value());

  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, false, 1000*1000));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag1));
  wait_scheduler(scheduler);
  EXPECT_EQ(8, op.value());

  scheduler.destroy();
  ObWorkerObjPool::get_instance().destroy();
}

TEST_F(TestDagScheduler, test_switch_task)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init());
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 221;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int ret = OB_SUCCESS;
  int64_t type_id = 0;
  int max_thread_num = 1;
  common::ObArray<ObTenantSetting> tenant_settings;
  tenant_settings.push_back(ObTenantSetting(tenant_id, max_thread_num));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_settings));
  BasicDag *dag = NULL;
  int flag = 0;
  if (OB_FAIL(scheduler.alloc_dag(tenant_id, dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else {
    dag->set_priority(ObIDagNew::DAG_PRIO_1);
    SwitchFromTask *from_task = NULL;
    if (OB_FAIL(dag->alloc_task(from_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == from_task) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else if (OB_FAIL(dag->add_task(*from_task))){
      COMMON_LOG(WARN, "add task failed", K(ret));
    } else {
      from_task->init(&flag);
    }
  }
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag));
  EXPECT_EQ(OB_SUCCESS, scheduler.start_run());
  ::usleep(10000); // make sure the SwitchFromTask is running
  BasicDag *dag2 = NULL;
  if (OB_FAIL(scheduler.alloc_dag(tenant_id, dag2))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag2) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else {
    dag2->set_priority(ObIDagNew::DAG_PRIO_2);
    SwitchToTask *to_task = NULL;
    if (OB_FAIL(dag2->alloc_task(to_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == to_task) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else if (OB_FAIL(dag2->add_task(*to_task))){
      COMMON_LOG(WARN, "add task failed", K(ret));
    } else {
      to_task->init(&flag);
    }
  }
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag2));
  wait_scheduler(scheduler);
  EXPECT_EQ(flag, 1);
  scheduler.destroy();
  ObWorkerObjPool::get_instance().destroy();
}

TEST_F(TestDagScheduler, test_score_penalty)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init());
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 221;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int ret = OB_SUCCESS;
  // set type setting
  common::ObArray<ObTenantSetting> tenant_setting_array;
  int max_thread_num = 10;
  ObTenantSetting tenant_setting(tenant_id, max_thread_num);

  tenant_setting.type_settings_.push_back(ObTenantTypeSetting(0, 2));
  tenant_setting.type_settings_.push_back(ObTenantTypeSetting(1, 3));
  tenant_setting.type_settings_.push_back(ObTenantTypeSetting(2, 5));

  tenant_setting_array.push_back(tenant_setting);
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_setting_array));

  BasicDag *dag_a = NULL;
  int generate_size = 3;
  if (OB_FAIL(scheduler.alloc_dag(tenant_id, dag_a))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag_a) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else {
    dag_a->set_priority(ObIDagNew::DAG_PRIO_1);
    dag_a->set_type_id(0); // test_score_penalty_a score = 2
    TypeIdTask *task_a = NULL;
    if (OB_FAIL(dag_a->alloc_task(task_a))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == task_a) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else if (OB_FAIL(dag_a->add_task(*task_a))) {
      COMMON_LOG(WARN, "add task failed", K(ret));
    } else {
      task_a->init(0, generate_size);
    }
  }
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag_a));
  BasicDag *dag_b = NULL;
  if (OB_FAIL(scheduler.alloc_dag(tenant_id, dag_b))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag_b) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else {
    dag_b->set_priority(ObIDagNew::DAG_PRIO_1);
    dag_b->set_type_id(1); // test_score_penalty_b score = 3
    TypeIdTask *task_b = NULL;
    if (OB_FAIL(dag_b->alloc_task(task_b))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == task_b) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else if (OB_FAIL(dag_b->add_task(*task_b))) {
      COMMON_LOG(WARN, "add task failed", K(ret));
    } else {
      task_b->init(0, generate_size);
    }
  }
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag_b));
  BasicDag *dag_c = NULL;
  if (OB_FAIL(scheduler.alloc_dag(tenant_id, dag_c))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag_c) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else {
    dag_c->set_priority(ObIDagNew::DAG_PRIO_1);
    dag_c->set_type_id(2); // test_score_penalty_c score = 5
    TypeIdTask *task_c = NULL;
    if (OB_FAIL(dag_c->alloc_task(task_c))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == task_c) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else if (OB_FAIL(dag_c->add_task(*task_c))) {
      COMMON_LOG(WARN, "add task failed", K(ret));
    } else {
      task_c->init(0, generate_size);
    }
  }
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag_c));
  GetScheduleInfo schedule_info;
  scheduler.set_sche_info(tenant_id, &schedule_info);
  EXPECT_EQ(OB_SUCCESS, scheduler.start_run());
  wait_scheduler(scheduler);
  // check
  int count = schedule_info.choose_type_id_list_.size();
  int64_t exp[] = {2, 2, 1, 0, 2,
                   1, 2, 2, 0, 1};
  for(int i = 0; i < 10 && i < count; ++i) {
    EXPECT_EQ(exp[i], schedule_info.choose_type_id_list_[i]);
  }
  scheduler.destroy();
  ObWorkerObjPool::get_instance().destroy();
}

TEST_F(TestDagScheduler, test_error_handling)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init());
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 221;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int ret = OB_SUCCESS;
  int max_thread_num = 3;
  common::ObArray<ObTenantSetting> tenant_settings;
  tenant_settings.push_back(ObTenantSetting(tenant_id, max_thread_num));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_settings));
  AtomicOperator op(0);
  TestDag *dag = NULL;
  AtomicMulTask *mul_task = NULL;
  AtomicIncTask *inc_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 10, op, 0, 8));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, mul_task->add_child(*inc_task));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag));
  EXPECT_EQ(OB_SUCCESS, scheduler.start_run());
  wait_scheduler(scheduler);
  EXPECT_EQ(0, op.value());

  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 1, op, 0, 1));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, inc_task->add_child(*mul_task));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, mul_task->add_child(*inc_task));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag));
  wait_scheduler(scheduler);
  EXPECT_EQ(10, op.value());

  TestDag *dag1 = NULL;
  TestPrepareTask *prepare_task = NULL;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, true));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag1));
  wait_scheduler(scheduler);
  EXPECT_EQ(0, op.value());

  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, true, 1000*1000));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag1));
  wait_scheduler(scheduler);
  EXPECT_EQ(0, op.value());

  scheduler.destroy();
  ObWorkerObjPool::get_instance().destroy();
}

TEST_F(TestDagScheduler, stress_test)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init());
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id1 = 221;
  int64_t tenant_id2 = 334;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id1));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id2));
  int ret = OB_SUCCESS;
  // set type setting
  common::ObArray<ObTenantSetting> tenant_setting_array;
  int max_thread_num = 16;
  ObTenantSetting tenant_setting(tenant_id1, max_thread_num);

  tenant_setting.type_settings_.push_back(ObTenantTypeSetting(0, 2));
  tenant_setting.type_settings_.push_back(ObTenantTypeSetting(1, 3));
  tenant_setting.type_settings_.push_back(ObTenantTypeSetting(2, 5));

  tenant_setting_array.push_back(tenant_setting);
  tenant_setting.tenant_id_ = tenant_id2;
  tenant_setting_array.push_back(tenant_setting);
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_setting_array));

  DagSchedulerStressTester tester1;
  tester1.init(stress_time);
  tester1.set_max_type_id(3);
  tester1.set_tenant_id(tenant_id1);
  EXPECT_EQ(OB_SUCCESS, tester1.do_stress());
  DagSchedulerStressTester tester2;
  tester2.init(stress_time);
  tester2.set_max_type_id(3);
  tester2.set_tenant_id(tenant_id2);
  EXPECT_EQ(OB_SUCCESS, tester2.do_stress());
  wait_scheduler(ObDagSchedulerNew::get_instance());
  COMMON_LOG(INFO, "test finished", "stress dag create dag count:",tester1.get_dag_cnt());
  COMMON_LOG(INFO, "test finished", "stress dag create dag count:",tester2.get_dag_cnt());
  ObDagSchedulerNew::get_instance().destroy();
  ObWorkerObjPool::get_instance().destroy();
}

TEST_F(TestDagScheduler, stress_test_with_error)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init());
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id1 = 221;
  int64_t tenant_id2 = 334;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id1));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id2));
  int ret = OB_SUCCESS;
  // set type setting
  common::ObArray<ObTenantSetting> tenant_setting_array;
  int max_thread_num = 16;
  ObTenantSetting tenant_setting(tenant_id1, max_thread_num);

  tenant_setting.type_settings_.push_back(ObTenantTypeSetting(0, 2));
  tenant_setting.type_settings_.push_back(ObTenantTypeSetting(1, 3));
  tenant_setting.type_settings_.push_back(ObTenantTypeSetting(2, 5));

  tenant_setting_array.push_back(tenant_setting);
  tenant_setting.tenant_id_ = tenant_id2;
  tenant_setting_array.push_back(tenant_setting);
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_setting_array));

  DagSchedulerStressTester tester1;
  tester1.init(stress_time);
  tester1.set_max_type_id(6); // can generate dag with unregistered type
  tester1.set_tenant_id(tenant_id1);
  EXPECT_EQ(OB_SUCCESS, tester1.do_stress());
  DagSchedulerStressTester tester2;
  tester2.init(stress_time);
  tester2.set_max_type_id(4); // can generate dag with unregistered type
  tester2.set_tenant_id(tenant_id2);
  EXPECT_EQ(OB_SUCCESS, tester2.do_stress());
  wait_scheduler(ObDagSchedulerNew::get_instance());
  COMMON_LOG(INFO, "test finished", "stress dag create dag count:",tester1.get_dag_cnt());
  COMMON_LOG(INFO, "test finished", "stress dag create dag count:",tester2.get_dag_cnt());
  ObDagSchedulerNew::get_instance().destroy();
  ObWorkerObjPool::get_instance().destroy();
}


TEST_F(TestDagScheduler, test_destroy_when_running)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init());
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 221;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int ret = OB_SUCCESS;

  AtomicOperator op(0);

  TestDag *dag1 = NULL;
  AtomicIncTask *inc_task = NULL;
  int cnt = 20;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, cnt, op, 4*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  TestDag *dag2 = NULL;
  AtomicMulTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, cnt, op, 3*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));
  TestDag *dag3 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, cnt, op, 2*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*inc_task));

  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag1));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag2));
  // high priority preempt quotas from low priority, low priority run at min thread
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag3));

  // set type setting
  common::ObArray<ObTenantSetting> tenant_setting_array;
  int max_thread_num = 16;
  ObTenantSetting tenant_setting(tenant_id, max_thread_num);
  tenant_setting_array.push_back(tenant_setting);
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_setting_array));

  EXPECT_EQ(OB_SUCCESS, scheduler.start_run());
  ::usleep(50000);
  scheduler.destroy();
  ObWorkerObjPool::get_instance().destroy();
}

TEST_F(TestDagScheduler, test_max_thread_num)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  int check_period = 100; // schdule period
  EXPECT_EQ(OB_SUCCESS, scheduler.init(check_period));
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 221;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int ret = OB_SUCCESS;
  int max_thread_num[] = {5, 16, 2, 32};
  common::ObArray<ObTenantSetting> tenant_settings;
  tenant_settings.push_back(ObTenantSetting(tenant_id, max_thread_num[0]));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_settings));

  TestDag *dag1 = NULL;
  AtomicOperator op(0);
  op.reset();
  AtomicIncTask *inc_task = NULL;
  AtomicIncTask *inc_task1 = NULL;
  AtomicMulTask *mul_task = NULL;
  int cnt = 100;
  int sleep_us = 20000;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, cnt, op, sleep_us));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, cnt, op, sleep_us));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task1));
  EXPECT_EQ(OB_SUCCESS, inc_task1->init(1, cnt, op, sleep_us));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task1));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag1));

  GetScheduleInfo schedule_info;
  scheduler.set_sche_info(tenant_id, &schedule_info);

  EXPECT_EQ(OB_SUCCESS, scheduler.start_run());
  ::usleep(300 * 1000);
  tenant_settings.reset();
  tenant_settings.push_back(ObTenantSetting(tenant_id, max_thread_num[1]));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_settings));

  ::usleep(400 * 1000);
  tenant_settings.reset();
  tenant_settings.push_back(ObTenantSetting(tenant_id, max_thread_num[2]));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_settings));

  ::usleep(300 * 1000);
  tenant_settings.reset();
  tenant_settings.push_back(ObTenantSetting(tenant_id, max_thread_num[3]));
  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_settings));
  wait_scheduler(scheduler);
  for (int i = 0; i < schedule_info.running_tasks_cnt_.size(); ++i) {
    COMMON_LOG(INFO, "schedule_info", "cnt", schedule_info.running_tasks_cnt_[i]);
  }
  int before = 0;
  for (int i = 0; i < schedule_info.running_tasks_cnt_.size(); ++i) { // 5
    if (max_thread_num[0] == schedule_info.running_tasks_cnt_[i]) {
      while (i < schedule_info.running_tasks_cnt_.size()
          && schedule_info.running_tasks_cnt_[i] < max_thread_num[1]) {
        EXPECT_EQ(max_thread_num[0], schedule_info.running_tasks_cnt_[i]);
        ++i;
      }
    } else if (max_thread_num[1] == schedule_info.running_tasks_cnt_[i]) { // monotonically decreasing
      before = max_thread_num[1];
      while (i < schedule_info.running_tasks_cnt_.size()
          && schedule_info.running_tasks_cnt_[i] != max_thread_num[2]) {
        EXPECT_EQ(true, before >= schedule_info.running_tasks_cnt_[i]);
        before = schedule_info.running_tasks_cnt_[i];
        ++i;
      }
    } else if (max_thread_num[2] == schedule_info.running_tasks_cnt_[i]) { // 2
      while (i < schedule_info.running_tasks_cnt_.size()
          && schedule_info.running_tasks_cnt_[i] != max_thread_num[3]) {
        EXPECT_EQ(max_thread_num[2], schedule_info.running_tasks_cnt_[i]);
        ++i;
      }
    } else if (max_thread_num[3] == schedule_info.running_tasks_cnt_[i]) { // monotonically decreasing
      before = max_thread_num[3];
      while (i < schedule_info.running_tasks_cnt_.size()
          && schedule_info.running_tasks_cnt_[i] != 0) {
        EXPECT_EQ(true, before >= schedule_info.running_tasks_cnt_[i]);
        before = schedule_info.running_tasks_cnt_[i];
        ++i;
      }
    }
  }
  scheduler.destroy();
  ObWorkerObjPool::get_instance().destroy();
}

TEST_F(TestDagScheduler, test_set_uplimit_when_running)
{
  ObDagSchedulerNew &scheduler = ObDagSchedulerNew::get_instance();
  scheduler.destroy();
  int check_period = 100; // schdule period
  EXPECT_EQ(OB_SUCCESS, scheduler.init(check_period));
  ObWorkerObjPool::get_instance().init();
  int64_t tenant_id = 221;
  EXPECT_EQ(OB_SUCCESS, scheduler.add_tenant_thread_pool(tenant_id));
  int ret = OB_SUCCESS;
  int type_id = 0;
  int max_thread_num = 16;
  int uplimit[] = {10, 5};
  common::ObArray<ObTenantSetting> tenant_setting_array;

  ObTenantSetting tenant_setting(tenant_id, max_thread_num);
  tenant_setting.type_settings_.push_back(
  ObTenantTypeSetting(type_id, ObTenantSetting::NOT_SET, uplimit[0]));
  tenant_setting_array.push_back(tenant_setting);

  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_setting_array));

  TestDag *dag1 = NULL;
  AtomicOperator op(0);
  op.reset();
  AtomicIncTask *inc_task = NULL;
  AtomicIncTask *inc_task1 = NULL;
  AtomicMulTask *mul_task = NULL;
  int cnt = 150;
  int sleep_us = 20000;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(tenant_id, dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, cnt, op, sleep_us));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, cnt, op, sleep_us));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task1));
  EXPECT_EQ(OB_SUCCESS, inc_task1->init(1, cnt, op, sleep_us));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task1));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag_in_tenant(tenant_id, dag1));

  GetScheduleInfo schedule_info;
  scheduler.set_sche_info(tenant_id, &schedule_info);
  EXPECT_EQ(OB_SUCCESS, scheduler.start_run());
  ::usleep(800 * 1000); // wait
  COMMON_LOG(INFO, "!!!!!!~~~~~~~~~~s");
  tenant_setting_array.reset();
  tenant_setting.type_settings_.clear();
  tenant_setting.type_settings_.push_back(
  ObTenantTypeSetting(type_id, ObTenantSetting::NOT_SET, uplimit[1]));
  tenant_setting_array.push_back(tenant_setting);

  EXPECT_EQ(OB_SUCCESS, scheduler.set_tenant_setting(tenant_setting_array));
  COMMON_LOG(INFO, "!!!!!!~~~~~~~~~~s");
  wait_scheduler(scheduler);
  for (int i = 0; i < schedule_info.running_tasks_cnt_.size(); ++i) {
    COMMON_LOG(INFO, "schedule_info", "cnt",
        schedule_info.running_tasks_cnt_[i]);
  }
  for (int i = 0; i < schedule_info.running_tasks_cnt_.size(); ++i) {
    if (uplimit[0] == schedule_info.running_tasks_cnt_[i]) {
      while (i < schedule_info.running_tasks_cnt_.size()
          && schedule_info.running_tasks_cnt_[i] > uplimit[1]) {
        EXPECT_EQ(uplimit[0], schedule_info.running_tasks_cnt_[i]);
        ++i;
      }
    } else if (uplimit[1] == schedule_info.running_tasks_cnt_[i]) { // monotonically decreasing
      while (i < schedule_info.running_tasks_cnt_.size()) {
        EXPECT_EQ(true, schedule_info.running_tasks_cnt_[i] <= uplimit[1]);
        ++i;
      }
    }
  }
  scheduler.destroy();
  ObWorkerObjPool::get_instance().destroy();
}
*/

/*
TEST_F(TestDagScheduler, test_priority)
{
  ObDagScheduler &scheduler = ObDagScheduler::get_instance();
  scheduler.destroy();
  scheduler.init(ObAddr(1,1), time_slice);
  AtomicOperator op(0);

  TestLPDag *dag1 = NULL;
  AtomicIncTask *inc_task = NULL;
  int32_t thread_cnt = scheduler.get_work_thread_num();
  for (int64_t i = 0; i < ObIDag::DAG_ULT_MAX; ++i) {
    scheduler.set_max_thread(i, thread_cnt);
  }
  EXPECT_EQ(thread_cnt, scheduler.get_work_thread_num());

  int32_t lp_min = scheduler.DEFAULT_LOW_LIMIT[ObIDag::DAG_PRIO_DDL];
  int32_t mp_min = scheduler.DEFAULT_LOW_LIMIT[ObIDag::DAG_PRIO_SSTABLE_MAJOR_MERGE];
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, thread_cnt, op, 8*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  TestHPDag *dag2 = NULL;
  AtomicMulTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, thread_cnt, op, 6*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));
  TestMPDag *dag3 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, thread_cnt, op, 4*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*inc_task));

  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_DDL));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag2));
  // high priority preempt quotas from low priority, low priority run at min thread
  CHECK_EQ_UTIL_TIMEOUT(lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_DDL));
  EXPECT_EQ(thread_cnt - lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_SSTABLE_MINOR_MERGE));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag3));
  // medium priority takes the min_thread quotas belong to him
  CHECK_EQ_UTIL_TIMEOUT(lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_DDL));
  CHECK_EQ_UTIL_TIMEOUT(mp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_SSTABLE_MAJOR_MERGE));
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt - lp_min -mp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_SSTABLE_MINOR_MERGE));
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt * 2 - lp_min + mp_min, scheduler.total_worker_cnt_);
  wait_scheduler(scheduler);
  // check if excessive threads are reclaimed
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.total_worker_cnt_);

  COMMON_LOG(INFO, "start test priority case 2");

  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, thread_cnt, op, 10*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, thread_cnt - lp_min, op, 5*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));

  // low priority run at max speed
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_DDL));

  // high priority preempt quotas from low priority, low priority run at min thread
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag2));
  CHECK_EQ_UTIL_TIMEOUT(lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_DDL));
  EXPECT_EQ(thread_cnt - lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_SSTABLE_MINOR_MERGE));

  // when high priority finishes, low priority will bounce back to full speed
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_DDL));
  wait_scheduler(scheduler);
}

void print_state(int64_t idx)
{
  ObDagScheduler &scheduler = ObDagScheduler::get_instance();
  COMMON_LOG(INFO, "scheduler state: ", K(scheduler.total_running_task_cnt_), K(scheduler.work_thread_num_),
      K(scheduler.total_worker_cnt_), K(scheduler.low_limits_[idx]), K(scheduler.up_limits_[idx]), K(scheduler.running_task_cnts_[idx]));
}

TEST_F(TestDagScheduler, test_set_concurrency)
{
  ObDagScheduler &scheduler = ObDagScheduler::get_instance();
  scheduler.destroy();
  int32_t thread_cnt = ObDagScheduler::DEFAULT_UP_LIMIT[ObIDag::DAG_ULT_MINOR_MERGE];
  EXPECT_EQ(OB_SUCCESS, scheduler.init(ObAddr(1,1), thread_cnt));
  TestHPDag *dag = NULL;
  AtomicIncTask *inc_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, thread_cnt * 2, dag->get_op(), 10 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag));
  int64_t idx = ObIDag::DAG_PRIO_SSTABLE_MINOR_MERGE;
  CHECK_EQ_UTIL_TIMEOUT(scheduler.DEFAULT_LOW_LIMIT[idx], scheduler.low_limits_[idx]);
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.up_limits_[ObDagScheduler::UP_LIMIT_MAP[idx]]);
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.total_running_task_cnt_);
  EXPECT_TRUE(thread_cnt <= scheduler.work_thread_num_);
  // set max to 20
  EXPECT_EQ(OB_SUCCESS, scheduler.set_max_thread(ObDagScheduler::UP_LIMIT_MAP[idx], 20));
  EXPECT_EQ(scheduler.DEFAULT_LOW_LIMIT[idx], scheduler.low_limits_[idx]);
  EXPECT_EQ(20, scheduler.up_limits_[ObDagScheduler::UP_LIMIT_MAP[idx]]);
  CHECK_EQ_UTIL_TIMEOUT(20, scheduler.total_running_task_cnt_);
  EXPECT_EQ(20, scheduler.work_thread_num_);
  // set to 1
  EXPECT_EQ(OB_SUCCESS, scheduler.set_max_thread(ObDagScheduler::UP_LIMIT_MAP[idx], 1));
  print_state(idx);
  EXPECT_EQ(1, scheduler.low_limits_[idx]);
  EXPECT_EQ(1, scheduler.up_limits_[ObDagScheduler::UP_LIMIT_MAP[idx]]);
  CHECK_EQ_UTIL_TIMEOUT(1, scheduler.total_running_task_cnt_);
  EXPECT_TRUE(thread_cnt <= scheduler.work_thread_num_);
  // set to 2
  EXPECT_EQ(OB_SUCCESS, scheduler.set_max_thread(ObDagScheduler::UP_LIMIT_MAP[idx], 2));
  print_state(idx);
  EXPECT_EQ(1, scheduler.low_limits_[idx]);
  EXPECT_EQ(2, scheduler.up_limits_[ObDagScheduler::UP_LIMIT_MAP[idx]]);
  CHECK_EQ_UTIL_TIMEOUT(2, scheduler.total_running_task_cnt_);
  EXPECT_TRUE(thread_cnt <= scheduler.work_thread_num_);
  // set to 5
  EXPECT_EQ(OB_SUCCESS, scheduler.set_max_thread(ObDagScheduler::UP_LIMIT_MAP[idx], 5));
  print_state(idx);
  EXPECT_EQ(1, scheduler.low_limits_[idx]);
  EXPECT_EQ(5, scheduler.up_limits_[ObDagScheduler::UP_LIMIT_MAP[idx]]);
  CHECK_EQ_UTIL_TIMEOUT(5, scheduler.total_running_task_cnt_);
  EXPECT_TRUE(thread_cnt <= scheduler.work_thread_num_);
  // set to 0
  EXPECT_EQ(OB_SUCCESS, scheduler.set_max_thread(ObDagScheduler::UP_LIMIT_MAP[idx], 0));
  print_state(idx);
  EXPECT_EQ(1, scheduler.low_limits_[idx]);
  EXPECT_EQ(scheduler.up_limits_[ObDagScheduler::UP_LIMIT_MAP[idx]], ObDagScheduler::DEFAULT_UP_LIMIT[ObDagScheduler::UP_LIMIT_MAP[idx]]);
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.total_running_task_cnt_);
  EXPECT_TRUE(thread_cnt <= scheduler.work_thread_num_);
  wait_scheduler(scheduler);
}

TEST_F(TestDagScheduler, test_get_dag_count)
{
  ObDagScheduler &scheduler = ObDagScheduler::get_instance();
  TestMPDag *dag = NULL;
  TestMPDag *dag2 = NULL;
  TestMulTask *mul_task = NULL;
  TestMulTask *mul_task2 = NULL;
  int64_t counter = 1;


  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init(ObAddr(1,1), time_slice));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_UT));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_SSTABLE_MAJOR_MERGE));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_DDL));
  EXPECT_EQ(-1, scheduler.get_dag_count(ObIDag::DAG_TYPE_MAX));

  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, dag->alloc_task(mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(&counter));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag));
  sleep(1);
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_UT));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_SSTABLE_MAJOR_MERGE));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_DDL));
  EXPECT_EQ(-1, scheduler.get_dag_count(ObIDag::DAG_TYPE_MAX));
  scheduler.stop();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, dag->alloc_task(mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(&counter));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_UT));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE));
  EXPECT_EQ(1, scheduler.get_dag_count(ObIDag::DAG_TYPE_SSTABLE_MAJOR_MERGE));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_DDL));
  EXPECT_EQ(-1, scheduler.get_dag_count(ObIDag::DAG_TYPE_MAX));
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, dag2->alloc_task(mul_task2));
  EXPECT_EQ(OB_SUCCESS, mul_task2->init(&counter));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task2));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag2));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_UT));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE));
  EXPECT_EQ(2, scheduler.get_dag_count(ObIDag::DAG_TYPE_SSTABLE_MAJOR_MERGE));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_DDL));
  EXPECT_EQ(-1, scheduler.get_dag_count(ObIDag::DAG_TYPE_MAX));
  scheduler.destroy();
}


TEST_F(TestDagScheduler, test_up_limit)
{
  ObDagScheduler &scheduler = ObDagScheduler::get_instance();
  scheduler.destroy();
  scheduler.init(ObAddr(1,1), 64);
  AtomicOperator op(0);

  TestLPDag *dag1 = NULL;
  AtomicIncTask *inc_task = NULL;
  const int32_t lp_min = scheduler.low_limits_[ObIDag::DAG_PRIO_MIGRATE_LOW];
  const int32_t mp_min = scheduler.low_limits_[ObIDag::DAG_PRIO_MIGRATE_MID];
  const int32_t up_limit = scheduler.up_limits_[ObIDag::DAG_ULT_MIGRATE];
  scheduler.dump_dag_status();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  dag1->set_priority(ObIDag::DAG_PRIO_MIGRATE_LOW);
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, up_limit, op, 4*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  TestHPDag *dag2 = NULL;
  AtomicMulTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  dag2->set_priority(ObIDag::DAG_PRIO_MIGRATE_HIGH);
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, up_limit, op, 3*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));
  TestMPDag *dag3 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  dag3->set_priority(ObIDag::DAG_PRIO_MIGRATE_MID);
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, up_limit, op, 2*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*inc_task));

  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(up_limit, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_MIGRATE_LOW));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag2));
  // high priority preempt quotas from low priority, low priority run at min thread
  CHECK_EQ_UTIL_TIMEOUT(lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_MIGRATE_LOW));
  EXPECT_EQ(up_limit- lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_MIGRATE_HIGH));
  EXPECT_EQ(up_limit, scheduler.total_running_task_cnt_);
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag3));

  CHECK_EQ_UTIL_TIMEOUT(lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_MIGRATE_LOW));
  CHECK_EQ_UTIL_TIMEOUT(mp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_MIGRATE_MID));
  EXPECT_EQ(up_limit - lp_min - mp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_MIGRATE_HIGH));
  EXPECT_EQ(up_limit, scheduler.total_running_task_cnt_);
  wait_scheduler(scheduler);
}*/

}
}

void parse_cmd_arg(int argc, char **argv)
{
  int opt = 0;
  const char *opt_string = "p:s:l:";

  struct option longopts[] = {
      {"dag cnt for performance test", 1, NULL, 'p'},
      {"stress test time", 1, NULL, 's'},
      {"log level", 1, NULL, 'l'},
      {0,0,0,0} };

  while (-1 != (opt = getopt_long(argc, argv, opt_string, longopts, NULL))) {
    switch(opt) {
    case 'p':
      dag_cnt = strtoll(optarg, NULL, 10);
      break;
    case 's':
      stress_time = strtoll(optarg, NULL, 10);
      break;
    case 'l':
      snprintf(log_level, 20, "%s", optarg);
      break;
    default:
      break;
    }
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  parse_cmd_arg(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  OB_LOGGER.set_max_file_size(256*1024*1024);
 // char filename[128];
 // snprintf(filename, 128, "test_dag_scheduler.log");
  system("rm -f test_tenant_dag_scheduler.log*");
  OB_LOGGER.set_file_name("test_tenant_dag_scheduler.log");
  return RUN_ALL_TESTS();
}
