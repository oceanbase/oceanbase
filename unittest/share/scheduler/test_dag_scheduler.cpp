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
#include "share/scheduler/ob_dag_scheduler.h"
#include "lib/atomic/ob_atomic.h"
#include "observer/omt/ob_tenant_node_balancer.h"

int64_t dag_cnt = 1;
int64_t stress_time = 1;  // 100ms
char log_level[20] = "INFO";
uint32_t time_slice = 20 * 1000;  // 5ms
uint32_t sleep_slice = 2 * time_slice;
const int64_t CHECK_TIMEOUT = 1 * 1000 * 1000;

#define CHECK_EQ_UTIL_TIMEOUT(expected, expr)                                                \
  {                                                                                          \
    int64_t start_time = oceanbase::common::ObTimeUtility::current_time();                   \
    auto expr_result = (expr);                                                               \
    do {                                                                                     \
      if ((expected) == (expr_result)) {                                                     \
        break;                                                                               \
      } else {                                                                               \
        expr_result = (expr);                                                                \
      }                                                                                      \
    } while (oceanbase::common::ObTimeUtility::current_time() - start_time < CHECK_TIMEOUT); \
    EXPECT_EQ((expected), (expr_result));                                                    \
  }

namespace oceanbase {
using namespace common;
using namespace share;
using namespace omt;
namespace unittest {

class TestAddTask : public ObITask {
public:
  TestAddTask() : ObITask(ObITaskType::TASK_TYPE_UT), counter_(NULL), adder_(), seq_(0), task_cnt_(0), sleep_us_(0)
  {}
  ~TestAddTask()
  {}
  int init(int64_t* counter, int64_t adder, int64_t seq, int64_t task_cnt, int sleep_us = 0)
  {
    int ret = OB_SUCCESS;
    counter_ = counter;
    adder_ = adder;
    seq_ = seq;
    task_cnt_ = task_cnt;
    sleep_us_ = sleep_us;
    return ret;
  }
  virtual int generate_next_task(ObITask*& next_task)
  {
    int ret = OB_SUCCESS;
    if (seq_ >= task_cnt_ - 1) {
      ret = OB_ITER_END;
      COMMON_LOG(INFO, "generate task end", K_(seq), K_(task_cnt));
    } else {
      ObIDag* dag = get_dag();
      TestAddTask* ntask = NULL;
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
    this_routine::usleep(sleep_us_);
    (void)ATOMIC_AAF(counter_, adder_);
    return OB_SUCCESS;
  }
  VIRTUAL_TO_STRING_KV(KP_(counter), K_(seq), K_(task_cnt));

private:
  int64_t* counter_;
  int64_t adder_;
  int64_t seq_;
  int64_t task_cnt_;
  int sleep_us_;

private:
  DISALLOW_COPY_AND_ASSIGN(TestAddTask);
};

class TestMulTask : public ObITask {
public:
  TestMulTask() : ObITask(ObITaskType::TASK_TYPE_UT), counter_(NULL), sleep_us_(0)
  {}
  ~TestMulTask()
  {}

  int init(int64_t* counter, int sleep_us = 0)
  {
    int ret = OB_SUCCESS;
    counter_ = counter;
    sleep_us_ = sleep_us;
    return ret;
  }
  virtual int process()
  {
    this_routine::usleep(sleep_us_);
    *counter_ = *counter_ * 2;
    return OB_SUCCESS;
  }
  VIRTUAL_TO_STRING_KV(KP_(counter));

private:
  int64_t* counter_;
  int sleep_us_;

private:
  DISALLOW_COPY_AND_ASSIGN(TestMulTask);
};

class AtomicOperator {
public:
  AtomicOperator() : v_(0)
  {}
  AtomicOperator(int64_t v) : v_(v)
  {}
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

static const int64_t SLEEP_SLICE = 100;

class AtomicMulTask : public ObITask {
public:
  AtomicMulTask() : ObITask(ObITask::TASK_TYPE_UT), seq_(0), cnt_(0), error_seq_(-1), op_(NULL), sleep_us_(0)
  {}
  int init(int64_t seq, int64_t cnt, AtomicOperator& op, int sleep_us = 0, int64_t error_seq = -1)
  {
    seq_ = seq;
    cnt_ = cnt;
    error_seq_ = error_seq;
    op_ = &op;
    sleep_us_ = sleep_us;
    return OB_SUCCESS;
  }
  virtual int generate_next_task(ObITask*& task)
  {
    int ret = OB_SUCCESS;
    ObIDag* dag = NULL;
    AtomicMulTask* ntask = NULL;
    if (seq_ >= cnt_) {
      return OB_ITER_END;
    } else if (OB_ISNULL(dag = get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is NULL", K(ret));
    } else if (OB_FAIL(dag->alloc_task(ntask))) {
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
      dag_yield();
      this_routine::usleep(SLEEP_SLICE);
    }
    if (seq_ == error_seq_) {
      return OB_ERR_UNEXPECTED;
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
  AtomicOperator* op_;
  int sleep_us_;
};

class AtomicIncTask : public ObITask {
public:
  AtomicIncTask() : ObITask(ObITask::TASK_TYPE_UT), seq_(0), cnt_(0), error_seq_(-1), op_(NULL), sleep_us_(0)
  {}
  int init(int64_t seq, int64_t cnt, AtomicOperator& op, int sleep_us = 0, int64_t error_seq = -1)
  {
    seq_ = seq;
    cnt_ = cnt;
    error_seq_ = error_seq;
    op_ = &op;
    sleep_us_ = sleep_us;
    return OB_SUCCESS;
  }
  virtual int generate_next_task(ObITask*& task)
  {
    int ret = OB_SUCCESS;
    ObIDag* dag = NULL;
    AtomicIncTask* ntask = NULL;
    if (seq_ >= cnt_) {
      return OB_ITER_END;
    } else if (OB_ISNULL(dag = get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is NULL", K(ret));
    } else if (OB_FAIL(dag->alloc_task(ntask))) {
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
      dag_yield();
      this_routine::usleep(SLEEP_SLICE);
    }
    if (seq_ == error_seq_) {
      return OB_ERR_UNEXPECTED;
    } else {
      op_->inc();
    }
    return ret;
  }
  VIRTUAL_TO_STRING_KV("type", "AtomicInc", K(*dag_), K_(seq), K_(cnt), KP_(op), K_(error_seq), K_(sleep_us));

private:
  int64_t seq_;
  int64_t cnt_;
  int64_t error_seq_;
  AtomicOperator* op_;
  int sleep_us_;
};

template <class T>
int alloc_task(ObIDag& dag, T*& task)
{
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

void wait_scheduler(ObDagScheduler& scheduler)
{
  while (!scheduler.is_empty()) {
    this_routine::usleep(100000);
  }
}

class TestDag : public ObIDag {
public:
  TestDag()
      : ObIDag(ObIDagType::DAG_TYPE_UT, ObIDag::DAG_PRIO_SSTABLE_MINOR_MERGE),
        id_(0),
        expect_(-1),
        expect_ret_(0),
        running_(false),
        tester_(NULL)
  {}
  explicit TestDag(ObIDagType type, ObIDagPriority prio)
      : ObIDag(type, prio), id_(0), expect_(-1), expect_ret_(0), running_(false), tester_(NULL)
  {}
  virtual ~TestDag()
  {
    if (get_dag_status() == ObIDag::DAG_STATUS_FINISH || get_dag_status() == ObIDag::DAG_STATUS_NODE_FAILED) {
      if (running_ && -1 != expect_) {
        if (op_.value() != expect_ || get_dag_ret() != expect_ret_) {
          if (OB_ALLOCATE_MEMORY_FAILED != get_dag_ret()) {
            if (NULL != tester_) {
              tester_->stop();
            }
            COMMON_LOG(ERROR, "FATAL ERROR!!!", K_(expect), K(op_.value()), K_(expect_ret), K(get_dag_ret()), K_(id));
            common::right_to_die_or_duty_to_live();
          }
        }
      }
    }
  }
  int init(int64_t id, int expect_ret = 0, int64_t expect = -1, lib::ThreadPool* tester = NULL)
  {
    id_ = id;
    expect_ret_ = expect_ret;
    expect_ = expect;
    tester_ = tester;
    ObAddr addr(1683068975, 9999);
    if (OB_SUCCESS != (ObSysTaskStatMgr::get_instance().set_self_addr(addr))) {
      COMMON_LOG(WARN, "failed to add sys task", K(addr));
    }
    return OB_SUCCESS;
  }
  virtual int64_t hash() const
  {
    return murmurhash(&id_, sizeof(id_), 0);
  }
  virtual bool operator==(const ObIDag& other) const
  {
    bool bret = false;
    if (get_type() == other.get_type()) {
      const TestDag& dag = static_cast<const TestDag&>(other);
      bret = dag.id_ == id_;
    }
    return bret;
  }
  void set_id(int64_t id)
  {
    id_ = id;
  }
  AtomicOperator& get_op()
  {
    return op_;
  }
  void set_running()
  {
    running_ = true;
  }
  int64_t get_tenant_id() const
  {
    return 0;
  }
  int fill_comment(char* buf, const int64_t size) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || size < 0) {
      COMMON_LOG(INFO, "buf is NULL", K(ret), K(size));
    }
    return ret;
  }
  int64_t get_compat_mode() const override
  {
    return static_cast<int64_t>(ObWorker::CompatMode::MYSQL);
  }
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(type), K_(id), K(task_list_.get_size()));

protected:
  int64_t id_;
  int64_t expect_;
  int expect_ret_;
  AtomicOperator op_;
  bool running_;
  lib::ThreadPool* tester_;

private:
  DISALLOW_COPY_AND_ASSIGN(TestDag);
};

class TestLPDag : public TestDag {
public:
  TestLPDag() : TestDag(ObIDagType::DAG_TYPE_CREATE_INDEX, ObIDag::DAG_PRIO_CREATE_INDEX)
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(TestLPDag);
};

class TestMPDag : public TestDag {
public:
  TestMPDag() : TestDag(ObIDagType::DAG_TYPE_SSTABLE_MAJOR_MERGE, ObIDag::DAG_PRIO_SSTABLE_MAJOR_MERGE)
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(TestMPDag);
};

class TestHPDag : public TestDag {
public:
  TestHPDag() : TestDag(ObIDagType::DAG_TYPE_SSTABLE_MINOR_MERGE, ObIDag::DAG_PRIO_SSTABLE_MINOR_MERGE)
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(TestHPDag);
};

class TestPrepareTask : public ObITask {
  static const int64_t inc_task_cnt = 8;
  static const int64_t mul_task_cnt = 6;

public:
  TestPrepareTask() : ObITask(ObITask::TASK_TYPE_UT), dag_id_(0), is_error_(false), sleep_us_(0), op_(NULL)
  {}

  int init(int64_t dag_id, AtomicOperator* op = NULL, bool is_error = false, int sleep_us = 0)
  {
    int ret = OB_SUCCESS;
    dag_id_ = dag_id;
    is_error_ = is_error;
    sleep_us_ = sleep_us;
    if (NULL != op) {
      op_ = op;
    } else {
      TestDag* dag = static_cast<TestDag*>(get_dag());
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
    TestDag* dag = static_cast<TestDag*>(get_dag());
    AtomicIncTask* inc_task = NULL;
    AtomicMulTask* mul_task = NULL;
    AtomicMulTask* mul_task1 = NULL;
    if (OB_ISNULL(dag)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is null", K(ret));
    } else if (OB_FAIL(alloc_task(*dag, inc_task))) {
      COMMON_LOG(WARN, "failed to alloc inc_task", K(ret));
    } else if (OB_FAIL(inc_task->init(1, inc_task_cnt, *op_))) {
    } else if (OB_FAIL(alloc_task(*dag, mul_task))) {
      COMMON_LOG(WARN, "failed to alloc mul task", K(ret));
    } else if (OB_FAIL(mul_task->init(1, mul_task_cnt, *op_, 0, is_error_ ? 1 + (dag_id_ % mul_task_cnt) : -1))) {
    } else if (OB_FAIL(alloc_task(*dag, mul_task1))) {
      COMMON_LOG(WARN, "failed to alloc mul task", K(ret));
    } else if (OB_FAIL(mul_task1->init(1, mul_task_cnt, *op_))) {
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
      this_routine::usleep(sleep_us_);
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
  AtomicOperator* op_;
};

class TestCyclePrepare : public ObITask {
public:
  TestCyclePrepare() : ObITask(ObITask::TASK_TYPE_UT), op_(NULL)
  {}
  int init(AtomicOperator* op = NULL)
  {
    int ret = OB_SUCCESS;
    if (NULL != op) {
      op_ = op;
    } else {
      TestDag* dag = static_cast<TestDag*>(get_dag());
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
    TestDag* dag = static_cast<TestDag*>(get_dag());
    AtomicIncTask* inc_task = NULL;
    AtomicMulTask* mul_task = NULL;
    AtomicMulTask* mul_task1 = NULL;
    if (OB_ISNULL(dag)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is null", K(ret));
    } else if (OB_FAIL(alloc_task(*dag, inc_task))) {
      COMMON_LOG(WARN, "failed to alloc inc_task", K(ret));
    } else if (OB_FAIL(inc_task->init(1, 5, *op_))) {
    } else if (OB_FAIL(alloc_task(*dag, mul_task))) {
      COMMON_LOG(WARN, "failed to alloc mul task", K(ret));
    } else if (OB_FAIL(mul_task->init(1, 5, *op_))) {
    } else if (OB_FAIL(alloc_task(*dag, mul_task1))) {
      COMMON_LOG(WARN, "failed to alloc mul task", K(ret));
    } else if (OB_FAIL(mul_task1->init(1, 5, *op_))) {
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
      dag->free_task(*mul_task);
      COMMON_LOG(WARN, "failed to add_task", K(ret));
    }
    return ret;
  }

private:
  AtomicOperator* op_;
};

class DagSchedulerStressTester : public lib::ThreadPool {
  static const int64_t STRESS_THREAD_NUM = 16;

public:
  DagSchedulerStressTester() : test_time_(0)
  {}

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
    wait();
    int64_t elapsed_time = ObTimeUtility::current_time() - start_time;
    COMMON_LOG(INFO, "stress test finished", K(elapsed_time / 1000));
    ObDagScheduler::get_instance().destroy();
    int ret_code = system("grep ERROR test_dag_scheduler.log -q");
    ret_code = WEXITSTATUS(ret_code);
    if (ret_code == 0)
      ret = OB_ERR_UNEXPECTED;
    return ret;
  }

  void run1()
  {
    int64_t start_time = ObTimeUtility::current_time();
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    ObDagScheduler& scheduler = ObDagScheduler::get_instance();
    while (!has_set_stop() && OB_SUCC(ret) && (ObTimeUtility::current_time() - start_time < test_time_)) {
      const int64_t dag_id = get_dag_id();
      TestDag* dag = NULL;
      TestPrepareTask* task = NULL;
      int expect_ret = (dag_id % 10 == 0 ? OB_ERR_UNEXPECTED : OB_SUCCESS);
      int64_t expect_value = (dag_id % 10 == 0 ? 0 : 8);

      switch (dag_id % ObIDag::DAG_PRIO_MAX) {
        case ObIDag::DAG_PRIO_CREATE_INDEX: {
          TestLPDag* lp_dag = NULL;
          if (OB_SUCCESS != (tmp_ret = scheduler.alloc_dag(lp_dag))) {
            if (OB_ALLOCATE_MEMORY_FAILED != tmp_ret) {
              ret = tmp_ret;
              COMMON_LOG(ERROR, "failed to allocate dag", K(ret));
            } else {
              COMMON_LOG(WARN, "out of memory", K(scheduler.get_cur_dag_cnt()));
            }
          } else {
            dag = lp_dag;
          }
          break;
        }
        case ObIDag::DAG_PRIO_SSTABLE_MAJOR_MERGE: {
          TestMPDag* mp_dag = NULL;
          if (OB_SUCCESS != (tmp_ret = scheduler.alloc_dag(mp_dag))) {
            if (OB_ALLOCATE_MEMORY_FAILED != tmp_ret) {
              ret = tmp_ret;
              COMMON_LOG(ERROR, "failed to allocate dag", K(ret));
            } else {
              COMMON_LOG(WARN, "out of memory", K(scheduler.get_cur_dag_cnt()));
            }
          } else {
            dag = mp_dag;
          }
          break;
        }
        default: {
          TestHPDag* hp_dag = NULL;
          if (OB_SUCCESS != (tmp_ret = scheduler.alloc_dag(hp_dag))) {
            if (OB_ALLOCATE_MEMORY_FAILED != tmp_ret) {
              ret = tmp_ret;
              COMMON_LOG(ERROR, "failed to allocate dag", K(ret));
            } else {
              COMMON_LOG(WARN, "out of memory", K(scheduler.get_cur_dag_cnt()));
            }
          } else {
            dag = hp_dag;
          }
          break;
        }
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
        if (OB_SUCCESS != (tmp_ret = scheduler.add_dag(dag))) {
          if (OB_SIZE_OVERFLOW != tmp_ret) {
            COMMON_LOG(ERROR, "failed to add dag", K(tmp_ret), K(*dag));
          }
          scheduler.free_dag(*dag);
        }
      }
    }
  }

  int64_t get_dag_id()
  {
    return ATOMIC_FAA(&counter_, 1);
  }

private:
  static int64_t counter_;
  int64_t test_time_;
};

int64_t DagSchedulerStressTester::counter_ = 0;

class TestDagScheduler : public ::testing::Test {
public:
  TestDagScheduler()
  {}
  ~TestDagScheduler()
  {}
  void SetUp()
  {
    ObUnitInfoGetter::ObTenantConfig unit_config;
    unit_config.mode_ = ObWorker::CompatMode::MYSQL;
    unit_config.tenant_id_ = 0;
    TenantUnits units;
    ASSERT_EQ(OB_SUCCESS, units.push_back(unit_config));
    // ASSERT_EQ(OB_SUCCESS, ObTenantNodeBalancer::get_instance().load_tenant(units));
  }
  void TearDown()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(TestDagScheduler);
};

TEST_F(TestDagScheduler, test_init)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  ObAddr addr(1, 1);
  // invalid thread cnt
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.init(addr, time_slice, -1));
  // invalid dag_limit
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.init(addr, time_slice, 0, 0));
  // invalid total_mem_limit
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.init(addr, time_slice, 0, 10, 0));
  // invalid hold_mem_limit
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.init(addr, time_slice, 0, 10, 1024, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler.init(addr, time_slice, 0, 10, 1024, 2048));

  EXPECT_EQ(OB_SUCCESS, scheduler.init(addr, time_slice, 100));
  EXPECT_EQ(OB_INIT_TWICE, scheduler.init(addr));
}

TEST_F(TestDagScheduler, baisc_test)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  scheduler.destroy();
  scheduler.init(ObAddr(1, 1), time_slice);
  int ret = OB_SUCCESS;
  TestDag* dag = NULL;
  TestDag* dup_dag = NULL;
  int64_t counter = 0;
  // simple two-level dag
  if (OB_FAIL(scheduler.alloc_dag(dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag->init(1))) {
    COMMON_LOG(WARN, "failed to init dag", K(ret));
  } else {
    TestAddTask* add_task = NULL;
    TestMulTask* mul_task = NULL;
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
  if (OB_FAIL(scheduler.alloc_dag(dup_dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dup_dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dup_dag->init(1))) {
    COMMON_LOG(WARN, "failed to init dag", K(ret));
  } else {
    TestMulTask* mul_task = NULL;
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
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag));
  EXPECT_EQ(OB_EAGAIN, scheduler.add_dag(dup_dag));
  scheduler.free_dag(*dup_dag);
  wait_scheduler(scheduler);
  EXPECT_EQ(counter, 20);

  // three level dag that each level would generate dynamic tasks
  AtomicOperator op(0);
  TestDag* dag1 = NULL;
  AtomicIncTask* inc_task = NULL;
  AtomicIncTask* inc_task1 = NULL;
  AtomicMulTask* mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
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
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  wait_scheduler(scheduler);
  EXPECT_EQ(170, op.value());

  // two-level dag with 2 tasks on the first level
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  // add mul task
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
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  wait_scheduler(scheduler);
  EXPECT_EQ(320, op.value());

  // a dag with single task which generate all other tasks while processing
  TestPrepareTask* prepare_task = NULL;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  wait_scheduler(scheduler);
  EXPECT_EQ(8, op.value());

  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, false, 1000 * 1000));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  wait_scheduler(scheduler);
  EXPECT_EQ(8, op.value());
}

TEST_F(TestDagScheduler, test_cycle)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  scheduler.destroy();
  scheduler.init(ObAddr(1, 1), time_slice);
  int ret = OB_SUCCESS;
  TestDag* dag = NULL;
  int64_t counter = 0;
  if (OB_FAIL(scheduler.alloc_dag(dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag->init(1))) {
    COMMON_LOG(WARN, "failed to init dag", K(ret));
  } else {
    TestAddTask* add_task = NULL;
    TestMulTask* mul_task = NULL;
    if (OB_FAIL(dag->alloc_task(mul_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == mul_task) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else {
      if (OB_FAIL(mul_task->init(&counter))) {
        COMMON_LOG(WARN, "failed to init add task", K(ret));
      }
      EXPECT_EQ(OB_INVALID_ARGUMENT, mul_task->add_child(*mul_task));
      if (OB_SUCC(ret)) {
        if (OB_FAIL(dag->alloc_task(add_task))) {
          COMMON_LOG(WARN, "failed to alloc task", K(ret));
        } else if (NULL == add_task) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "task is null", K(ret));
        } else {
          add_task->init(&counter, 1, 0, 3);
          add_task->add_child(*mul_task);
          mul_task->add_child(*add_task);
        }
        EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
        EXPECT_EQ(OB_INVALID_ARGUMENT, dag->add_task(*add_task));
        dag->free_task(*add_task);
        scheduler.free_dag(*dag);
      }
    }
  }
  TestCyclePrepare* prepare_task = NULL;
  AtomicOperator op;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1, OB_INVALID_ARGUMENT, 0));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(&op));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag));
  wait_scheduler(scheduler);
  EXPECT_EQ(0, op.value());
}

TEST_F(TestDagScheduler, test_priority)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  scheduler.destroy();
  scheduler.init(ObAddr(1, 1), time_slice);
  AtomicOperator op(0);

  TestLPDag* dag1 = NULL;
  AtomicIncTask* inc_task = NULL;
  int32_t thread_cnt = scheduler.get_work_thread_num();
  for (int64_t i = 0; i < ObIDag::DAG_ULT_MAX; ++i) {
    scheduler.set_max_thread(i, thread_cnt);
  }
  EXPECT_EQ(thread_cnt, scheduler.get_work_thread_num());

  int32_t lp_min = scheduler.DEFAULT_LOW_LIMIT[ObIDag::DAG_PRIO_CREATE_INDEX];
  int32_t mp_min = scheduler.DEFAULT_LOW_LIMIT[ObIDag::DAG_PRIO_SSTABLE_MAJOR_MERGE];
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, thread_cnt, op, 8 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  TestHPDag* dag2 = NULL;
  AtomicMulTask* mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, thread_cnt, op, 6 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));
  TestMPDag* dag3 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, thread_cnt, op, 4 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*inc_task));

  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_CREATE_INDEX));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag2));
  // high priority preempt quotas from low priority, low priority run at min thread
  CHECK_EQ_UTIL_TIMEOUT(lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_CREATE_INDEX));
  EXPECT_EQ(thread_cnt - lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_SSTABLE_MINOR_MERGE));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag3));
  // medium priority takes the min_thread quotas belong to him
  CHECK_EQ_UTIL_TIMEOUT(lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_CREATE_INDEX));
  CHECK_EQ_UTIL_TIMEOUT(mp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_SSTABLE_MAJOR_MERGE));
  CHECK_EQ_UTIL_TIMEOUT(
      thread_cnt - lp_min - mp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_SSTABLE_MINOR_MERGE));
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt * 2 - lp_min + mp_min, scheduler.total_worker_cnt_);
  wait_scheduler(scheduler);
  // check if excessive threads are reclaimed
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.total_worker_cnt_);

  COMMON_LOG(INFO, "start test priority case 2");

  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, thread_cnt, op, 10 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, thread_cnt - lp_min, op, 5 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));

  // low priority run at max speed
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_CREATE_INDEX));

  // high priority preempt quotas from low priority, low priority run at min thread
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag2));
  CHECK_EQ_UTIL_TIMEOUT(lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_CREATE_INDEX));
  EXPECT_EQ(thread_cnt - lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_SSTABLE_MINOR_MERGE));

  // when high priority finishes, low priority will bounce back to full speed
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_CREATE_INDEX));
  wait_scheduler(scheduler);
}

TEST_F(TestDagScheduler, test_error_handling)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  scheduler.destroy();
  scheduler.init(ObAddr(1, 1), time_slice);
  AtomicOperator op(0);
  TestDag* dag = NULL;
  AtomicMulTask* mul_task = NULL;
  AtomicIncTask* inc_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 10, op, 0, 8));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, mul_task->add_child(*inc_task));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag));
  wait_scheduler(scheduler);
  EXPECT_EQ(0, op.value());

  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag));
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
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag));
  wait_scheduler(scheduler);
  EXPECT_EQ(10, op.value());

  TestDag* dag1 = NULL;
  TestPrepareTask* prepare_task = NULL;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, true));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  wait_scheduler(scheduler);
  EXPECT_EQ(0, op.value());

  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, true, 1000 * 1000));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  wait_scheduler(scheduler);
  EXPECT_EQ(0, op.value());
}

void print_state(int64_t idx)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  COMMON_LOG(INFO,
      "scheduler state: ",
      K(scheduler.total_running_task_cnt_),
      K(scheduler.work_thread_num_),
      K(scheduler.total_worker_cnt_),
      K(scheduler.low_limits_[idx]),
      K(scheduler.up_limits_[idx]),
      K(scheduler.running_task_cnts_[idx]));
}

TEST_F(TestDagScheduler, test_set_concurrency)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  scheduler.destroy();
  int32_t thread_cnt = ObDagScheduler::DEFAULT_UP_LIMIT[ObIDag::DAG_ULT_MINOR_MERGE];
  EXPECT_EQ(OB_SUCCESS, scheduler.init(ObAddr(1, 1), time_slice, thread_cnt));
  TestHPDag* dag = NULL;
  AtomicIncTask* inc_task = NULL;
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
  EXPECT_EQ(scheduler.up_limits_[ObDagScheduler::UP_LIMIT_MAP[idx]],
      ObDagScheduler::DEFAULT_UP_LIMIT[ObDagScheduler::UP_LIMIT_MAP[idx]]);
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.total_running_task_cnt_);
  EXPECT_TRUE(thread_cnt <= scheduler.work_thread_num_);
  wait_scheduler(scheduler);
}

TEST_F(TestDagScheduler, stress_test)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init(ObAddr(1, 1), time_slice, 64));
  DagSchedulerStressTester tester;
  tester.init(stress_time);
  EXPECT_EQ(OB_SUCCESS, tester.do_stress());
}

TEST_F(TestDagScheduler, test_get_dag_count)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  TestMPDag* dag = NULL;
  TestMPDag* dag2 = NULL;
  TestMulTask* mul_task = NULL;
  TestMulTask* mul_task2 = NULL;
  int64_t counter = 1;

  scheduler.destroy();
  EXPECT_EQ(OB_SUCCESS, scheduler.init(ObAddr(1, 1), time_slice));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_UT));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_SSTABLE_MAJOR_MERGE));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_SSTABLE_MINOR_MERGE));
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_CREATE_INDEX));
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
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_CREATE_INDEX));
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
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_CREATE_INDEX));
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
  EXPECT_EQ(0, scheduler.get_dag_count(ObIDag::DAG_TYPE_CREATE_INDEX));
  EXPECT_EQ(-1, scheduler.get_dag_count(ObIDag::DAG_TYPE_MAX));
  scheduler.destroy();
}

TEST_F(TestDagScheduler, test_destroy_when_running)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  scheduler.destroy();
  scheduler.init(ObAddr(1, 1), time_slice);
  AtomicOperator op(0);

  TestLPDag* dag1 = NULL;
  AtomicIncTask* inc_task = NULL;
  int32_t thread_cnt = scheduler.get_work_thread_num();
  for (int64_t i = 0; i < ObIDag::DAG_ULT_MAX; ++i) {
    scheduler.set_max_thread(i, thread_cnt);
  }
  EXPECT_EQ(thread_cnt, scheduler.get_work_thread_num());
  int32_t lp_min = scheduler.DEFAULT_LOW_LIMIT[ObIDag::DAG_PRIO_CREATE_INDEX];
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, thread_cnt, op, 4 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  TestHPDag* dag2 = NULL;
  AtomicMulTask* mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, thread_cnt, op, 3 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));
  TestMPDag* dag3 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, thread_cnt, op, 2 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*inc_task));

  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_CREATE_INDEX));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag2));
  // high priority preempt quotas from low priority, low priority run at min thread
  CHECK_EQ_UTIL_TIMEOUT(lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_CREATE_INDEX));
  CHECK_EQ_UTIL_TIMEOUT(thread_cnt - lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_SSTABLE_MINOR_MERGE));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag3));
  scheduler.destroy();
}

TEST_F(TestDagScheduler, test_up_limit)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  scheduler.destroy();
  scheduler.init(ObAddr(1, 1), time_slice, 64);
  AtomicOperator op(0);

  TestLPDag* dag1 = NULL;
  AtomicIncTask* inc_task = NULL;
  const int32_t lp_min = scheduler.low_limits_[ObIDag::DAG_PRIO_MIGRATE_LOW];
  const int32_t mp_min = scheduler.low_limits_[ObIDag::DAG_PRIO_MIGRATE_MID];
  const int32_t up_limit = scheduler.up_limits_[ObIDag::DAG_ULT_MIGRATE];
  scheduler.dump_dag_status();
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  dag1->set_priority(ObIDag::DAG_PRIO_MIGRATE_LOW);
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, up_limit, op, 4 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  TestHPDag* dag2 = NULL;
  AtomicMulTask* mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  dag2->set_priority(ObIDag::DAG_PRIO_MIGRATE_HIGH);
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, up_limit, op, 3 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));
  TestMPDag* dag3 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  dag3->set_priority(ObIDag::DAG_PRIO_MIGRATE_MID);
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, up_limit, op, 2 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*inc_task));

  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(up_limit, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_MIGRATE_LOW));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag2));
  // high priority preempt quotas from low priority, low priority run at min thread
  CHECK_EQ_UTIL_TIMEOUT(lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_MIGRATE_LOW));
  EXPECT_EQ(up_limit - lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_MIGRATE_HIGH));
  EXPECT_EQ(up_limit, scheduler.total_running_task_cnt_);
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag3));

  CHECK_EQ_UTIL_TIMEOUT(lp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_MIGRATE_LOW));
  CHECK_EQ_UTIL_TIMEOUT(mp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_MIGRATE_MID));
  EXPECT_EQ(up_limit - lp_min - mp_min, scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_MIGRATE_HIGH));
  EXPECT_EQ(up_limit, scheduler.total_running_task_cnt_);
  wait_scheduler(scheduler);
}

TEST_F(TestDagScheduler, test_emergency_task)
{
  ObDagScheduler& scheduler = ObDagScheduler::get_instance();
  scheduler.destroy();
  scheduler.init(ObAddr(1, 1), time_slice, 64);
  AtomicOperator op(0);

  EXPECT_EQ(OB_SUCCESS, scheduler.set_create_index_concurrency(1));
  EXPECT_EQ(1, scheduler.up_limits_[ObIDag::DAG_ULT_CREATE_INDEX]);
  TestLPDag* dag1 = NULL;
  AtomicIncTask* inc_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 1, op, 4 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));

  TestLPDag* dag2 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 1, op, sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*inc_task));

  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag2));

  while (scheduler.get_running_task_cnt(ObIDag::DAG_PRIO_CREATE_INDEX) == 0) {
    usleep(10);
  }

  TestLPDag* dag3 = NULL;
  AtomicMulTask* mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler.alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 1, op, 0));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, scheduler.add_dag(dag3, true));

  wait_scheduler(scheduler);

  EXPECT_EQ(3, op.value());
}

}  // namespace unittest
}  // namespace oceanbase

void parse_cmd_arg(int argc, char** argv)
{
  int opt = 0;
  const char* opt_string = "p:s:l:";

  struct option longopts[] = {{"dag cnt for performance test", 1, NULL, 'p'},
      {"stress test time", 1, NULL, 's'},
      {"log level", 1, NULL, 'l'},
      {0, 0, 0, 0}};

  while (-1 != (opt = getopt_long(argc, argv, opt_string, longopts, NULL))) {
    switch (opt) {
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

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  parse_cmd_arg(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  OB_LOGGER.set_max_file_size(256 * 1024 * 1024);
  // char filename[128];
  // snprintf(filename, 128, "test_dag_scheduler.log");
  system("rm -f test_dag_scheduler.log*");
  OB_LOGGER.set_file_name("test_dag_scheduler.log");
  return RUN_ALL_TESTS();
}
